//! Native kdb+ binary IPC v3 protocol:
//! - Lossless `K` payload encoder/decoder (`encode_k_payload` / `decode_k_payload`, used by `-8!` / `-9!`, flat/splayed files, and TCP frames)
//! - TCP handshake (`user:password\x03\x00` -> `\x03`) with optional authentication
//! - Multi-client `Listener` (`kix-serve`, `kix -p`, `kix-gui`, and `kix-cluster`) and synchronous/async `IpcClient` (`hopen`)
use crate::error::{KError, KResult};
use crate::parser::parse_script;
use crate::sym::{intern, resolve};
use crate::types::*;
use crate::Server;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;

pub const MSG_ASYNC: u8 = 0;
pub const MSG_SYNC: u8 = 1;
pub const MSG_RESPONSE: u8 = 2;

// ----- Payload Serialization (kdb+ IPC v3) ----------------------------------

pub fn encode_k_payload(val: &K) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    write_k(&mut buf, val);
    buf
}

fn write_nul_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(s.as_bytes());
    buf.push(0);
}

fn write_k(buf: &mut Vec<u8>, val: &K) {
    match val {
        K::Null => {
            buf.push(101u8);
            buf.push(0);
        }
        K::Bool(b) => {
            buf.push((-1i8) as u8);
            buf.push(u8::from(*b));
        }
        K::Guid(g) => {
            buf.push((-2i8) as u8);
            buf.extend_from_slice(g);
        }
        K::Byte(b) => {
            buf.push((-4i8) as u8);
            buf.push(*b);
        }
        K::Short(v) => {
            buf.push((-5i8) as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        K::Int(v) => {
            buf.push((-6i8) as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        K::Long(v) => {
            buf.push((-7i8) as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        K::Real(v) => {
            buf.push((-8i8) as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        K::Float(v) => {
            buf.push((-9i8) as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        K::Char(c) => {
            buf.push((-10i8) as u8);
            buf.push(*c);
        }
        K::Sym(s) => {
            buf.push((-11i8) as u8);
            write_nul_str(buf, &resolve(*s));
        }
        K::Timestamp(v) => {
            buf.push((-12i8) as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        K::Month(v) => {
            buf.push((-13i8) as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        K::Date(v) => {
            buf.push((-14i8) as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        K::Datetime(v) => {
            buf.push((-15i8) as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        K::Timespan(v) => {
            buf.push((-16i8) as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        K::Minute(v) => {
            buf.push((-17i8) as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        K::Second(v) => {
            buf.push((-18i8) as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        K::Time(v) => {
            buf.push((-19i8) as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        K::Vector(cv) => write_colvec(buf, cv),
        K::Table(t) => {
            // kdb+ IPC table (`98`): `[98, 0(attr), 99(dict), sym_vec(cols), general_list(vals)]`
            buf.push(98);
            buf.push(0);
            buf.push(99);
            write_colvec(buf, &ColVec::Sym(Attr::None, Arc::new(t.cols.clone())));
            buf.push(0);
            buf.push(0);
            buf.extend_from_slice(&(t.vals.len() as u32).to_le_bytes());
            for v in &t.vals {
                write_colvec(buf, v);
            }
        }
        K::KeyedTable(kt) => {
            // kdb+ IPC keyed table (`99`): dictionary mapping `Table(keys) -> Table(vals)`
            buf.push(99);
            write_k(buf, &K::Table(Arc::new(kt.keys.clone())));
            write_k(buf, &K::Table(Arc::new(kt.vals.clone())));
        }
        K::Dict(d) => {
            buf.push(99);
            write_k(buf, &d.keys);
            write_k(buf, &d.vals);
        }
        K::Func(f) => {
            buf.push(100);
            if let Func::Lambda(lam) = f {
                write_nul_str(buf, &lam.ns);
            } else {
                write_nul_str(buf, "");
            }
            let src = f.source_text();
            write_colvec(buf, &ColVec::Char(Attr::None, Arc::new(src.into_bytes())));
        }
    }
}

fn write_colvec(buf: &mut Vec<u8>, cv: &ColVec) {
    buf.push(cv.type_num() as u8);
    buf.push(cv.attr().ipc_byte());
    buf.extend_from_slice(&(cv.len() as u32).to_le_bytes());
    match cv {
        ColVec::General(_, xs) => {
            for x in xs.iter() {
                write_k(buf, x);
            }
        }
        ColVec::Bool(_, xs) => {
            for &b in xs.iter() {
                buf.push(u8::from(b));
            }
        }
        ColVec::Guid(_, xs) => {
            for g in xs.iter() {
                buf.extend_from_slice(g);
            }
        }
        ColVec::Byte(_, xs) | ColVec::Char(_, xs) => {
            buf.extend_from_slice(xs);
        }
        ColVec::Short(_, xs) => {
            for &v in xs.iter() {
                buf.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColVec::Int(_, xs)
        | ColVec::Month(_, xs)
        | ColVec::Date(_, xs)
        | ColVec::Minute(_, xs)
        | ColVec::Second(_, xs)
        | ColVec::Time(_, xs) => {
            for &v in xs.iter() {
                buf.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColVec::Long(_, xs) | ColVec::Timestamp(_, xs) | ColVec::Timespan(_, xs) => {
            for &v in xs.iter() {
                buf.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColVec::Real(_, xs) => {
            for &v in xs.iter() {
                buf.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColVec::Float(_, xs) | ColVec::Datetime(_, xs) => {
            for &v in xs.iter() {
                buf.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColVec::Sym(_, xs) => {
            for &s in xs.iter() {
                write_nul_str(buf, &resolve(s));
            }
        }
    }
}

pub fn decode_k_payload(data: &[u8], start: usize) -> KResult<(K, usize)> {
    decode_k_endian(data, start, true)
}

fn decode_k_endian(data: &[u8], mut p: usize, le: bool) -> KResult<(K, usize)> {
    if p >= data.len() {
        return Err(KError::ipc("truncated IPC payload"));
    }
    let tag = data[p] as i8;
    p += 1;

    let read_i16 = |b: &[u8], o: usize| -> KResult<i16> {
        let arr: [u8; 2] = b.get(o..o + 2).ok_or_else(|| KError::ipc("truncated i16"))?.try_into().unwrap();
        Ok(if le { i16::from_le_bytes(arr) } else { i16::from_be_bytes(arr) })
    };
    let read_i32 = |b: &[u8], o: usize| -> KResult<i32> {
        let arr: [u8; 4] = b.get(o..o + 4).ok_or_else(|| KError::ipc("truncated i32"))?.try_into().unwrap();
        Ok(if le { i32::from_le_bytes(arr) } else { i32::from_be_bytes(arr) })
    };
    let read_u32 = |b: &[u8], o: usize| -> KResult<u32> {
        let arr: [u8; 4] = b.get(o..o + 4).ok_or_else(|| KError::ipc("truncated u32"))?.try_into().unwrap();
        Ok(if le { u32::from_le_bytes(arr) } else { u32::from_be_bytes(arr) })
    };
    let read_i64 = |b: &[u8], o: usize| -> KResult<i64> {
        let arr: [u8; 8] = b.get(o..o + 8).ok_or_else(|| KError::ipc("truncated i64"))?.try_into().unwrap();
        Ok(if le { i64::from_le_bytes(arr) } else { i64::from_be_bytes(arr) })
    };
    let read_f32 = |b: &[u8], o: usize| -> KResult<f32> {
        let arr: [u8; 4] = b.get(o..o + 4).ok_or_else(|| KError::ipc("truncated f32"))?.try_into().unwrap();
        Ok(if le { f32::from_le_bytes(arr) } else { f32::from_be_bytes(arr) })
    };
    let read_f64 = |b: &[u8], o: usize| -> KResult<f64> {
        let arr: [u8; 8] = b.get(o..o + 8).ok_or_else(|| KError::ipc("truncated f64"))?.try_into().unwrap();
        Ok(if le { f64::from_le_bytes(arr) } else { f64::from_be_bytes(arr) })
    };

    match tag {
        -128 => {
            let (err_s, next) = read_nul_str(data, p)?;
            let _ = next;
            if let Some((sig, det)) = err_s.split_once(": ") {
                Err(KError::new(sig.trim_start_matches('\''), det))
            } else {
                Err(KError::signal(err_s.trim_start_matches('\'')))
            }
        }
        101 => {
            if p < data.len() {
                p += 1;
            }
            Ok((K::Null, p))
        }
        -1 => Ok((K::Bool(*data.get(p).ok_or_else(|| KError::ipc("truncated bool"))? != 0), p + 1)),
        -2 => {
            let g: [u8; 16] = data.get(p..p + 16).ok_or_else(|| KError::ipc("truncated guid"))?.try_into().unwrap();
            Ok((K::Guid(g), p + 16))
        }
        -4 => Ok((K::Byte(*data.get(p).ok_or_else(|| KError::ipc("truncated byte"))?), p + 1)),
        -5 => Ok((K::Short(read_i16(data, p)?), p + 2)),
        -6 => Ok((K::Int(read_i32(data, p)?), p + 4)),
        -7 => Ok((K::Long(read_i64(data, p)?), p + 8)),
        -8 => Ok((K::Real(read_f32(data, p)?), p + 4)),
        -9 => Ok((K::Float(read_f64(data, p)?), p + 8)),
        -10 => Ok((K::Char(*data.get(p).ok_or_else(|| KError::ipc("truncated char"))?), p + 1)),
        -11 => {
            let (s, next) = read_nul_str(data, p)?;
            Ok((K::Sym(intern(&s)), next))
        }
        -12 => Ok((K::Timestamp(read_i64(data, p)?), p + 8)),
        -13 => Ok((K::Month(read_i32(data, p)?), p + 4)),
        -14 => Ok((K::Date(read_i32(data, p)?), p + 4)),
        -15 => Ok((K::Datetime(read_f64(data, p)?), p + 8)),
        -16 => Ok((K::Timespan(read_i64(data, p)?), p + 8)),
        -17 => Ok((K::Minute(read_i32(data, p)?), p + 4)),
        -18 => Ok((K::Second(read_i32(data, p)?), p + 4)),
        -19 => Ok((K::Time(read_i32(data, p)?), p + 4)),

        0..=19 => {
            let attr = Attr::from_ipc_byte(*data.get(p).ok_or_else(|| KError::ipc("truncated vec attr"))?);
            let n = read_u32(data, p + 1)? as usize;
            p += 5;
            let cv = match tag {
                0 => {
                    let mut xs = Vec::with_capacity(n);
                    for _ in 0..n {
                        let (item, next) = decode_k_endian(data, p, le)?;
                        xs.push(item);
                        p = next;
                    }
                    ColVec::General(attr, Arc::new(xs))
                }
                1 => {
                    let slice = data.get(p..p + n).ok_or_else(|| KError::ipc("truncated bool vec"))?;
                    p += n;
                    ColVec::Bool(attr, Arc::new(slice.iter().map(|&b| b != 0).collect()))
                }
                2 => {
                    let mut xs = Vec::with_capacity(n);
                    for _ in 0..n {
                        let g: [u8; 16] = data.get(p..p + 16).ok_or_else(|| KError::ipc("truncated guid vec"))?.try_into().unwrap();
                        xs.push(g);
                        p += 16;
                    }
                    ColVec::Guid(attr, Arc::new(xs))
                }
                4 => {
                    let slice = data.get(p..p + n).ok_or_else(|| KError::ipc("truncated byte vec"))?;
                    p += n;
                    ColVec::Byte(attr, Arc::new(slice.to_vec()))
                }
                5 => {
                    let mut xs = Vec::with_capacity(n);
                    for _ in 0..n {
                        xs.push(read_i16(data, p)?);
                        p += 2;
                    }
                    ColVec::Short(attr, Arc::new(xs))
                }
                6 | 13 | 14 | 17 | 18 | 19 => {
                    let mut xs = Vec::with_capacity(n);
                    for _ in 0..n {
                        xs.push(read_i32(data, p)?);
                        p += 4;
                    }
                    match tag {
                        6 => ColVec::Int(attr, Arc::new(xs)),
                        13 => ColVec::Month(attr, Arc::new(xs)),
                        14 => ColVec::Date(attr, Arc::new(xs)),
                        17 => ColVec::Minute(attr, Arc::new(xs)),
                        18 => ColVec::Second(attr, Arc::new(xs)),
                        _ => ColVec::Time(attr, Arc::new(xs)),
                    }
                }
                7 | 12 | 16 => {
                    let mut xs = Vec::with_capacity(n);
                    for _ in 0..n {
                        xs.push(read_i64(data, p)?);
                        p += 8;
                    }
                    match tag {
                        7 => ColVec::Long(attr, Arc::new(xs)),
                        12 => ColVec::Timestamp(attr, Arc::new(xs)),
                        _ => ColVec::Timespan(attr, Arc::new(xs)),
                    }
                }
                8 => {
                    let mut xs = Vec::with_capacity(n);
                    for _ in 0..n {
                        xs.push(read_f32(data, p)?);
                        p += 4;
                    }
                    ColVec::Real(attr, Arc::new(xs))
                }
                9 | 15 => {
                    let mut xs = Vec::with_capacity(n);
                    for _ in 0..n {
                        xs.push(read_f64(data, p)?);
                        p += 8;
                    }
                    if tag == 9 {
                        ColVec::Float(attr, Arc::new(xs))
                    } else {
                        ColVec::Datetime(attr, Arc::new(xs))
                    }
                }
                10 => {
                    let slice = data.get(p..p + n).ok_or_else(|| KError::ipc("truncated char vec"))?;
                    p += n;
                    ColVec::Char(attr, Arc::new(slice.to_vec()))
                }
                11 => {
                    let mut xs = Vec::with_capacity(n);
                    for _ in 0..n {
                        let (s, next) = read_nul_str(data, p)?;
                        xs.push(intern(&s));
                        p = next;
                    }
                    ColVec::Sym(attr, Arc::new(xs))
                }
                _ => return Err(KError::ipc(format!("unsupported vector tag {tag}"))),
            };
            Ok((K::Vector(cv), p))
        }

        98 => {
            let _attr = *data.get(p).ok_or_else(|| KError::ipc("truncated table attr"))?;
            p += 1;
            let (dict_k, next) = decode_k_endian(data, p, le)?;
            let K::Dict(d) = dict_k else {
                return Err(KError::ipc("table payload must contain a column dictionary"));
            };
            let K::Vector(ColVec::Sym(_, cols)) = &*d.keys else {
                return Err(KError::ipc("table dictionary keys must be symbols"));
            };
            let K::Vector(ColVec::General(_, raw_cols)) = &*d.vals else {
                return Err(KError::ipc("table dictionary values must be column vectors"));
            };
            let mut vals = Vec::with_capacity(raw_cols.len());
            for rc in raw_cols.iter() {
                let K::Vector(cv) = rc else {
                    return Err(KError::ipc("table column is not a vector"));
                };
                vals.push(cv.clone());
            }
            Ok((K::Table(Arc::new(Flip::new((**cols).clone(), vals)?)), next))
        }

        99 => {
            let (kk, p1) = decode_k_endian(data, p, le)?;
            let (vk, p2) = decode_k_endian(data, p1, le)?;
            if let (K::Table(kt), K::Table(vt)) = (&kk, &vk) {
                return Ok((K::KeyedTable(Arc::new(KeyedTable::new((**kt).clone(), (**vt).clone())?)), p2));
            }
            Ok((K::Dict(Arc::new(Dict { keys: Box::new(kk), vals: Box::new(vk) })), p2))
        }

        100 => {
            let (ns, p1) = read_nul_str(data, p)?;
            let (src_k, p2) = decode_k_endian(data, p1, le)?;
            let src = src_k.as_str().unwrap_or_else(|| "{x}".into());
            if let Ok(mut exprs) = parse_script(&src) {
                if let Some(crate::ast::Expr::Lambda { params, implicit, body, source }) = exprs.pop() {
                    return Ok((K::Func(Func::Lambda(Arc::new(Lambda { params, implicit, body, source, ns }))), p2));
                }
            }
            Ok((K::Func(Func::Prim(intern(&src))), p2))
        }

        other => Err(KError::ipc(format!("unsupported IPC tag {other}"))),
    }
}

fn read_nul_str(data: &[u8], start: usize) -> KResult<(String, usize)> {
    let rel = data
        .get(start..)
        .and_then(|s| s.iter().position(|&b| b == 0))
        .ok_or_else(|| KError::ipc("unterminated NUL string in IPC payload"))?;
    let s = String::from_utf8_lossy(&data[start..start + rel]).into_owned();
    Ok((s, start + rel + 1))
}

fn encode_error_payload(err: &KError) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push((-128i8) as u8);
    let text = if err.detail.is_empty() {
        err.signal.clone()
    } else {
        format!("{}: {}", err.signal, err.detail)
    };
    write_nul_str(&mut buf, &text);
    buf
}

fn write_ipc_frame(stream: &mut TcpStream, msg_type: u8, payload: &[u8]) -> KResult<()> {
    let total = 8 + payload.len();
    let mut hdr = [0u8; 8];
    hdr[0] = 1; // little-endian
    hdr[1] = msg_type;
    hdr[2] = 0; // uncompressed
    hdr[3] = 0;
    hdr[4..8].copy_from_slice(&(total as u32).to_le_bytes());
    stream.write_all(&hdr)?;
    stream.write_all(payload)?;
    stream.flush()?;
    Ok(())
}

fn read_ipc_frame(stream: &mut TcpStream) -> KResult<(u8, bool, Vec<u8>)> {
    let mut hdr = [0u8; 8];
    stream.read_exact(&mut hdr)?;
    let le = hdr[0] != 0;
    let msg_type = hdr[1];
    let compressed = hdr[2] != 0;
    let len_bytes: [u8; 4] = hdr[4..8].try_into().unwrap();
    let total_len = if le { u32::from_le_bytes(len_bytes) } else { u32::from_be_bytes(len_bytes) } as usize;
    if total_len < 8 {
        return Err(KError::ipc("invalid IPC frame length"));
    }
    let mut body = vec![0u8; total_len - 8];
    stream.read_exact(&mut body)?;
    if compressed {
        body = uncompress_kdb(&body, le)?;
    }
    Ok((msg_type, le, body))
}

/// Decompress kdb+ IPC compressed payload (`-19!` algorithm).
fn uncompress_kdb(comp: &[u8], le: bool) -> KResult<Vec<u8>> {
    if comp.len() < 4 {
        return Err(KError::ipc("truncated compressed IPC payload"));
    }
    let arr: [u8; 4] = comp[0..4].try_into().unwrap();
    let uncomp_total = (if le { u32::from_le_bytes(arr) } else { u32::from_be_bytes(arr) }) as usize;
    let out_len = uncomp_total.saturating_sub(8);
    let mut dst = vec![0u8; out_len];
    let mut ptrs = [0usize; 256];
    let mut s = 4usize;
    let mut d = 0usize;
    let mut f = 0u8;
    let mut i = 0u8;
    let mut p = 0usize;

    while d < out_len && s < comp.len() {
        if i == 0 {
            f = comp[s];
            s += 1;
            i = 1;
        }
        if (f & i) != 0 {
            let r = ptrs[comp[s] as usize];
            s += 1;
            let cnt = (comp[s] as usize) + 2;
            s += 1;
            for k in 0..cnt {
                if d + k < out_len && r + k < d {
                    dst[d + k] = dst[r + k];
                }
            }
            d += cnt;
        } else {
            dst[d] = comp[s];
            d += 1;
            s += 1;
        }
        while p + 1 < d {
            ptrs[(dst[p] ^ dst[p + 1]) as usize] = p;
            p += 1;
        }
        i = i.wrapping_shl(1);
    }
    Ok(dst)
}

// ----- IPC Client (`IpcClient`) ---------------------------------------------

pub struct IpcClient {
    stream: TcpStream,
}

impl IpcClient {
    pub fn connect_addr(addr: &str, user: &str, password: &str) -> KResult<Self> {
        let norm = if addr.starts_with(':') { format!("127.0.0.1{addr}") } else if !addr.contains(':') { format!("{addr}:5000") } else { addr.to_string() };
        let sock = norm
            .to_socket_addrs()
            .map_err(|e| KError::ipc(format!("cannot resolve '{norm}': {e}")))?
            .next()
            .ok_or_else(|| KError::ipc(format!("no address for '{norm}'")))?;
        let mut stream = TcpStream::connect(sock).map_err(|e| KError::ipc(format!("connect {norm}: {e}")))?;
        let _ = stream.set_nodelay(true);

        // kdb+ v3 handshake: `"user:password\x03\x00"`
        let creds = if password.is_empty() {
            format!("{user}\x03\x00")
        } else {
            format!("{user}:{password}\x03\x00")
        };
        stream.write_all(creds.as_bytes())?;
        stream.flush()?;

        let mut cap = [0u8; 1];
        match stream.read_exact(&mut cap) {
            Ok(()) if cap[0] == 0xff => return Err(KError::new("access", "authentication failed")),
            Ok(()) => {}
            Err(_) => return Err(KError::new("access", "authentication refused by server")),
        }

        Ok(Self { stream })
    }

    pub fn query_str(&mut self, q_code: &str) -> KResult<K> {
        self.query_k(&K::string(q_code))
    }

    pub fn query_k(&mut self, msg: &K) -> KResult<K> {
        let payload = encode_k_payload(msg);
        write_ipc_frame(&mut self.stream, MSG_SYNC, &payload)?;
        let (_mtype, le, resp) = read_ipc_frame(&mut self.stream)?;
        let (k, _) = decode_k_endian(&resp, 0, le)?;
        Ok(k)
    }

    pub fn send_async_k(&mut self, msg: &K) -> KResult<()> {
        let payload = encode_k_payload(msg);
        write_ipc_frame(&mut self.stream, MSG_ASYNC, &payload)
    }
}

// ----- TCP IPC Server (`Listener`) ------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct IpcConfig {
    /// Optional `username -> password` map (`None` allows any user, matching kdb+ without `-u`).
    pub users: Option<HashMap<String, String>>,
}

impl IpcConfig {
    pub fn with_single_user(user: impl Into<String>, password: impl Into<String>) -> Self {
        let mut m = HashMap::new();
        m.insert(user.into(), password.into());
        Self { users: Some(m) }
    }
}

pub struct Listener {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    accept_thread: Option<JoinHandle<()>>,
}

impl Listener {
    pub fn bind(server: Server, bind_addr: impl ToSocketAddrs, cfg: IpcConfig) -> KResult<Self> {
        let tcp = TcpListener::bind(bind_addr)?;
        let addr = tcp.local_addr()?;
        let stop = Arc::new(AtomicBool::new(false));
        let stop_bg = stop.clone();
        let cfg = Arc::new(RwLock::new(cfg));

        let accept_thread = std::thread::spawn(move || {
            for incoming in tcp.incoming() {
                if stop_bg.load(Ordering::SeqCst) {
                    break;
                }
                let Ok(stream) = incoming else { continue };
                let srv = server.clone();
                let cfg_c = cfg.clone();
                let stop_c = stop_bg.clone();
                std::thread::spawn(move || {
                    let _ = handle_client(srv, stream, cfg_c, stop_c);
                });
            }
        });

        Ok(Self {
            addr,
            stop,
            accept_thread: Some(accept_thread),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn wait(mut self) {
        if let Some(h) = self.accept_thread.take() {
            let _ = h.join();
        }
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.addr);
    }
}

fn handle_client(
    server: Server,
    mut stream: TcpStream,
    cfg: Arc<RwLock<IpcConfig>>,
    stop: Arc<AtomicBool>,
) -> KResult<()> {
    let _ = stream.set_nodelay(true);

    // Read NUL-terminated handshake `"user[:pass][cap]\x00"`
    let mut hs = Vec::with_capacity(64);
    let mut b = [0u8; 1];
    loop {
        if stream.read(&mut b)? == 0 {
            return Ok(());
        }
        if b[0] == 0 {
            break;
        }
        hs.push(b[0]);
        if hs.len() > 1024 {
            return Err(KError::ipc("handshake too long"));
        }
    }

    let cap = if hs.last().is_some_and(|&c| c <= 6) { hs.pop().unwrap().max(1) } else { 3 };
    let cred_str = String::from_utf8_lossy(&hs);
    let (user, pass) = match cred_str.split_once(':') {
        Some((u, p)) => (u, p),
        None => (cred_str.as_ref(), ""),
    };

    {
        let g = cfg.read().unwrap_or_else(|e| e.into_inner());
        if let Some(users) = &g.users {
            let ok = users.get(user).is_some_and(|pw| pw == pass);
            if !ok {
                let _ = stream.write_all(&[0xff]);
                let _ = stream.flush();
                return Ok(());
            }
        }
    }

    stream.write_all(&[cap.min(3)])?;
    stream.flush()?;

    let mut conn = server.connect_as_trusted(if user.is_empty() { "anonymous" } else { user });

    while !stop.load(Ordering::SeqCst) {
        let (msg_type, le, raw_body) = match read_ipc_frame(&mut stream) {
            Ok(frame) => frame,
            Err(_) => break,
        };
        let (req_k, _) = match decode_k_endian(&raw_body, 0, le) {
            Ok(v) => v,
            Err(e) => {
                if msg_type == MSG_SYNC {
                    let ep = encode_error_payload(&e);
                    let _ = write_ipc_frame(&mut stream, MSG_RESPONSE, &ep);
                }
                continue;
            }
        };

        let res = conn.eval_ipc_message(&req_k);
        if msg_type == MSG_SYNC {
            match res {
                Ok(val) => {
                    let payload = encode_k_payload(&val);
                    write_ipc_frame(&mut stream, MSG_RESPONSE, &payload)?;
                }
                Err(err) => {
                    let ep = encode_error_payload(&err);
                    write_ipc_frame(&mut stream, MSG_RESPONSE, &ep)?;
                }
            }
        }
    }
    Ok(())
}
