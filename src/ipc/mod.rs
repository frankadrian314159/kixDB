//! Native kdb+ binary IPC v3 protocol:
//! - Lossless `K` payload encoder/decoder (`encode_k_payload` / `decode_k_payload`, used by `-8!` / `-9!`, flat/splayed files, and TCP frames)
//! - TCP handshake (`user:password\x03\x00` -> `\x03`) with optional authentication
//! - Multi-client `Listener` (`kix-serve`, `kix -p`, `kix-gui`, and `kix-cluster`) and synchronous/async `IpcClient` (`hopen`)
use crate::engine::tree::func_of;
use crate::error::{KError, KResult};
use crate::parser::parse_script;
use crate::sym::{intern, resolve};
use crate::types::*;
use crate::{Connection, Server};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
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
        K::Enum(tag, v) => {
            write_tag(buf, -20, tag);
            write_k(buf, v);
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

/// An enumeration, as saved files and the nodes of a cluster keep it (kixdb's own form: other programs are sent plain
/// values): type 20, what the domain is, its name and the values.
fn write_tag(buf: &mut Vec<u8>, type_num: i8, tag: &Tag) {
    buf.push(type_num as u8);
    buf.push(tag.source as u8);
    write_nul_str(buf, &resolve(tag.domain));
}

fn write_colvec(buf: &mut Vec<u8>, cv: &ColVec) {
    if let ColVec::Enum(tag, values) = cv {
        write_tag(buf, 20, tag);
        return write_colvec(buf, values);
    }
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
        // (written above)
        ColVec::Enum(..) => {}
    }
}

pub fn decode_k_payload(data: &[u8], start: usize) -> KResult<(K, usize)> {
    decode_k_endian(data, start, true)
}

fn decode_k_endian(data: &[u8], mut p: usize, le: bool) -> KResult<(K, usize)> {
    if p >= data.len() {
        return Err(KError::ipc("truncated IPC payload"));
    }
    let tag_byte = data[p] as i8;
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

    match tag_byte {
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
            let cv = match tag_byte {
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
                    match tag_byte {
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
                    match tag_byte {
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
                    if tag_byte == 9 {
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
                _ => return Err(KError::ipc(format!("unsupported vector tag {tag_byte}"))),
            };
            Ok((K::Vector(cv), p))
        }

        20 | -20 => {
            let source = match data.get(p) {
                Some(0) => Source::List,
                Some(1) => Source::Keys,
                Some(2) => Source::Rows,
                _ => return Err(KError::ipc("bad enumeration")),
            };
            let (domain, next) = read_nul_str(data, p + 1)?;
            let tag = Tag { domain: intern(&domain), source };
            match (tag_byte, decode_k_endian(data, next, le)?) {
                (20, (K::Vector(values), next)) => Ok((K::Vector(ColVec::Enum(tag, Box::new(values))), next)),
                (-20, (value, next)) => Ok((K::Enum(tag, Box::new(value)), next)),
                _ => Err(KError::ipc("bad enumeration")),
            }
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
            let parsed = parse_script(&src).ok().and_then(|exprs| match exprs.as_slice() {
                [e] => func_of(e, &ns),
                _ => None,
            });
            Ok((K::Func(parsed.unwrap_or_else(|| Func::Prim(intern(&src)))), p2))
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

/// Messages longer than this are compressed when the peer is on another machine (as kdb+ does).
const COMPRESS_OVER: usize = 2000;

fn write_ipc_frame(stream: &mut dyn Wire, msg_type: u8, payload: &[u8], compress: bool) -> KResult<()> {
    let packed = if compress && payload.len() > COMPRESS_OVER { compress_kdb(payload) } else { None };
    let compressed = packed.is_some();
    let payload = packed.as_deref().unwrap_or(payload);
    let total = 8 + payload.len();
    let mut hdr = [0u8; 8];
    hdr[0] = 1; // little-endian
    hdr[1] = msg_type;
    hdr[2] = u8::from(compressed);
    hdr[3] = 0;
    hdr[4..8].copy_from_slice(&(total as u32).to_le_bytes());
    stream.write_all(&hdr)?;
    stream.write_all(payload)?;
    stream.flush()?;
    Ok(())
}

fn read_ipc_frame(stream: &mut dyn Wire) -> KResult<(u8, bool, Vec<u8>)> {
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

/// Compress an IPC payload with kdb+'s IPC compression (the result starts with the uncompressed frame length), or `None`
/// when that does not at least halve it.
fn compress_kdb(src: &[u8]) -> Option<Vec<u8>> {
    let limit = src.len() / 2;
    let mut out = Vec::with_capacity(limit);
    out.extend_from_slice(&((src.len() + 8) as u32).to_le_bytes());
    let mut seen = [usize::MAX; 256];
    let (mut s, mut p) = (0usize, 0usize);
    let (mut flags, mut flag_at, mut bit) = (0u8, 0usize, 0u8);
    let mut literal: Option<(u8, usize)> = None;
    while s < src.len() {
        if bit == 0 {
            if out.len() + 17 > limit {
                return None;
            }
            if flag_at != 0 {
                out[flag_at] = flags;
            }
            (flag_at, flags, bit) = (out.len(), 0, 1);
            out.push(0);
        }
        let mut h = 0u8;
        let mut is_literal = s + 3 > src.len();
        if !is_literal {
            h = src[s] ^ src[s + 1];
            p = seen[h as usize];
            is_literal = p == usize::MAX || src[s] != src[p];
        }
        // a literal's position is remembered one step late, as the decompressor learns it
        if let Some((h0, s0)) = literal.take() {
            seen[h0 as usize] = s0;
        }
        if is_literal {
            literal = Some((h, s));
            out.push(src[s]);
            s += 1;
        } else {
            seen[h as usize] = s;
            flags |= bit;
            p += 2;
            s += 2;
            let start = s;
            let end = (s + 255).min(src.len());
            while s < end && src[p] == src[s] {
                s += 1;
                p += 1;
            }
            out.push(h);
            out.push((s - start) as u8);
        }
        bit = bit.wrapping_shl(1);
    }
    if flag_at != 0 {
        out[flag_at] = flags;
    }
    (out.len() < limit).then_some(out)
}

/// Decompress a kdb+ IPC compressed payload.
fn uncompress_kdb(comp: &[u8], le: bool) -> KResult<Vec<u8>> {
    if comp.len() < 4 {
        return Err(KError::ipc("truncated compressed IPC payload"));
    }
    let arr: [u8; 4] = comp[0..4].try_into().unwrap();
    let uncomp_total = (if le { u32::from_le_bytes(arr) } else { u32::from_be_bytes(arr) }) as usize;
    let out_len = uncomp_total.saturating_sub(8);
    let bad = || KError::ipc("corrupt compressed IPC payload");
    // one control byte covers at most 8 runs of 257 bytes
    if out_len / 2056 > comp.len() {
        return Err(bad());
    }
    let mut dst = vec![0u8; out_len];
    let mut ptrs = [0usize; 256];
    let (mut s, mut d, mut p) = (4usize, 0usize, 0usize);
    let (mut f, mut bit) = (0u8, 0u8);
    let next = |s: &mut usize| -> KResult<u8> {
        let b = *comp.get(*s).ok_or_else(bad)?;
        *s += 1;
        Ok(b)
    };

    while d < out_len {
        if bit == 0 {
            f = next(&mut s)?;
            bit = 1;
        }
        let mut run = 0;
        let back = (f & bit) != 0;
        if back {
            let r = ptrs[next(&mut s)? as usize];
            run = next(&mut s)? as usize;
            if r >= d || d + 2 + run > out_len {
                return Err(bad());
            }
            dst[d] = dst[r];
            dst[d + 1] = dst[r + 1];
            d += 2;
            for k in 0..run {
                dst[d + k] = dst[r + 2 + k];
            }
        } else {
            dst[d] = next(&mut s)?;
            d += 1;
        }
        while p + 1 < d {
            ptrs[(dst[p] ^ dst[p + 1]) as usize] = p;
            p += 1;
        }
        if back {
            d += run;
            p = d;
        }
        bit = bit.wrapping_shl(1);
    }
    Ok(dst)
}

// ----- IPC Client (`IpcClient`) ---------------------------------------------

/// A connection's byte stream: plain TCP, or TLS over TCP.
trait Wire: Read + Write + Send {}
impl<T: Read + Write + Send> Wire for T {}

fn tls_error(e: impl std::fmt::Display) -> KError {
    KError::ipc(format!("TLS: {e}"))
}

/// TLS settings of a listener from a PEM certificate chain and private key.
pub fn tls_server_config(cert_pem: &[u8], key_pem: &[u8]) -> KResult<Arc<rustls::ServerConfig>> {
    let certs = CertificateDer::pem_slice_iter(cert_pem).collect::<Result<Vec<_>, _>>().map_err(tls_error)?;
    let key = PrivateKeyDer::from_pem_slice(key_pem).map_err(tls_error)?;
    let cfg = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(tls_error)?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(tls_error)?;
    Ok(Arc::new(cfg))
}

/// A new self-signed certificate for `host` and the loopback names: `(certificate PEM, key PEM)`.
pub fn self_signed_cert(host: &str) -> KResult<(String, String)> {
    let names = vec![host.to_string(), "localhost".into(), "127.0.0.1".into()];
    let c = rcgen::generate_simple_self_signed(names).map_err(tls_error)?;
    Ok((c.cert.pem(), c.key_pair.serialize_pem()))
}

/// A TLS client stream to `host` that trusts the certificates in the PEM file `ca_file`.
fn tls_client(tcp: TcpStream, host: &str, ca_file: &str) -> KResult<Box<dyn Wire>> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(ca_file).map_err(tls_error)? {
        roots.add(cert.map_err(tls_error)?).map_err(tls_error)?;
    }
    let cfg = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(tls_error)?
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = ServerName::try_from(host.to_string()).map_err(tls_error)?;
    let conn = rustls::ClientConnection::new(Arc::new(cfg), name).map_err(tls_error)?;
    Ok(Box::new(rustls::StreamOwned::new(conn, tcp)))
}

pub struct IpcClient {
    stream: Box<dyn Wire>,
    compress: bool,
}

impl IpcClient {
    /// Connect to `host[:port]`. `tcps://host:port` connects with TLS, trusting the certificates in the file that the
    /// environment variable `KIX_SSL_CA_CERT_FILE` names.
    pub fn connect_addr(addr: &str, user: &str, password: &str) -> KResult<Self> {
        match addr.strip_prefix("tcps://") {
            Some(a) => {
                let ca = std::env::var("KIX_SSL_CA_CERT_FILE")
                    .map_err(|_| tls_error("set KIX_SSL_CA_CERT_FILE to the server's CA certificate"))?;
                Self::connect_tls(a, user, password, &ca)
            }
            None => Self::connect(addr, user, password, None),
        }
    }

    pub fn connect_tls(addr: &str, user: &str, password: &str, ca_file: &str) -> KResult<Self> {
        Self::connect(addr, user, password, Some(ca_file))
    }

    fn connect(addr: &str, user: &str, password: &str, ca_file: Option<&str>) -> KResult<Self> {
        let norm = if addr.starts_with(':') { format!("127.0.0.1{addr}") } else if !addr.contains(':') { format!("{addr}:5000") } else { addr.to_string() };
        let sock = norm
            .to_socket_addrs()
            .map_err(|e| KError::ipc(format!("cannot resolve '{norm}': {e}")))?
            .next()
            .ok_or_else(|| KError::ipc(format!("no address for '{norm}'")))?;
        let tcp = TcpStream::connect(sock).map_err(|e| KError::ipc(format!("connect {norm}: {e}")))?;
        let _ = tcp.set_nodelay(true);
        let compress = !sock.ip().is_loopback();
        let mut stream: Box<dyn Wire> = match ca_file {
            Some(ca) => tls_client(tcp, norm.rsplit_once(':').map_or(norm.as_str(), |(h, _)| h), ca)?,
            None => Box::new(tcp),
        };

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

        Ok(Self { stream, compress })
    }

    pub fn query_str(&mut self, q_code: &str) -> KResult<K> {
        self.query_k(&K::string(q_code))
    }

    pub fn query_k(&mut self, msg: &K) -> KResult<K> {
        let payload = encode_k_payload(&msg.untagged());
        write_ipc_frame(&mut *self.stream, MSG_SYNC, &payload, self.compress)?;
        let (_mtype, le, resp) = read_ipc_frame(&mut *self.stream)?;
        let (k, _) = decode_k_endian(&resp, 0, le)?;
        Ok(k)
    }

    pub fn send_async_k(&mut self, msg: &K) -> KResult<()> {
        let payload = encode_k_payload(&msg.untagged());
        write_ipc_frame(&mut *self.stream, MSG_ASYNC, &payload, self.compress)
    }
}

// ----- TCP IPC Server (`Listener`) ------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct IpcConfig {
    /// Optional `username -> password` map (`None` allows any user, matching kdb+ without `-u`).
    pub users: Option<HashMap<String, String>>,
    /// Accept TLS connections too (on the same port; plain connections are still accepted unless `force_tls` is true).
    pub tls: Option<Arc<rustls::ServerConfig>>,
    /// Require TLS (`tcps://`) and reject unencrypted connections.
    pub force_tls: bool,
}

impl IpcConfig {
    pub fn with_single_user(user: impl Into<String>, password: impl Into<String>) -> Self {
        let mut m = HashMap::new();
        m.insert(user.into(), password.into());
        Self { users: Some(m), tls: None, force_tls: false }
    }

    /// Whether it is safe to listen on `bind`. Beyond this machine every client must give a password, over TLS: else
    /// whoever reaches the port can run any `q`, or read passwords off the network.
    pub fn check_exposure(&self, bind: &str) -> Result<(), String> {
        let local = (bind, 0).to_socket_addrs().is_ok_and(|found| {
            let found: Vec<_> = found.collect();
            !found.is_empty() && found.iter().all(|a| a.ip().is_loopback())
        });
        let passwords = self.users.as_ref().is_some_and(|u| !u.is_empty() && u.values().all(|p| !p.is_empty()));
        if local || (passwords && self.tls.is_some() && self.force_tls) {
            return Ok(());
        }
        Err(format!(
            "listening on {bind}, beyond this machine, needs a password (-u USER:PASSWORD) and TLS for every client \
             (--force-tls with --tls or --tls-cert and --tls-key)"
        ))
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
    tcp: TcpStream,
    cfg: Arc<RwLock<IpcConfig>>,
    stop: Arc<AtomicBool>,
) -> KResult<()> {
    let _ = tcp.set_nodelay(true);
    let compress = tcp.peer_addr().is_ok_and(|a| !a.ip().is_loopback());
    let address = tcp.peer_addr().ok().and_then(|a| match a.ip() {
        std::net::IpAddr::V4(ip) => Some(u32::from(ip)),
        std::net::IpAddr::V6(ip) => ip.to_ipv4().map(u32::from),
    });
    let (tls, force_tls) = {
        let g = cfg.read().unwrap_or_else(|e| e.into_inner());
        (g.tls.clone(), g.force_tls)
    };
    // a TLS connection starts with a handshake record (0x16), which no IPC handshake does
    let mut first = [0u8; 1];
    let is_tls = tcp.peek(&mut first)? == 1 && first[0] == 0x16;
    if force_tls && !is_tls {
        return Err(KError::ipc("plaintext connection rejected (--force-tls)"));
    }
    let mut stream: Box<dyn Wire> = match tls {
        Some(t) if is_tls => {
            Box::new(rustls::StreamOwned::new(rustls::ServerConnection::new(t).map_err(tls_error)?, tcp))
        }
        _ => Box::new(tcp),
    };

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

    let mut conn = server.connect_as_trusted(if user.is_empty() { "anonymous" } else { user });
    conn.connected_from(address.map_or(0, |a| a as i32));
    if !conn.authorized(user, pass) {
        conn.closed(false);
        let _ = stream.write_all(&[0xff]);
        let _ = stream.flush();
        return Ok(());
    }

    let served = serve_client(&mut *stream, &mut conn, cap, compress, &stop);
    conn.closed(true);
    served
}

/// Accept a client that has been let in, and answer its messages until it leaves.
fn serve_client(stream: &mut dyn Wire, conn: &mut Connection, cap: u8, compress: bool, stop: &AtomicBool) -> KResult<()> {
    stream.write_all(&[cap.min(3)])?;
    stream.flush()?;
    conn.opened();

    while !stop.load(Ordering::SeqCst) {
        let (msg_type, le, raw_body) = match read_ipc_frame(stream) {
            Ok(frame) => frame,
            Err(_) => break,
        };
        let (req_k, _) = match decode_k_endian(&raw_body, 0, le) {
            Ok(v) => v,
            Err(e) => {
                if conn.bad_message(&raw_body) {
                    break;
                }
                if msg_type == MSG_SYNC {
                    let ep = encode_error_payload(&e);
                    let _ = write_ipc_frame(stream, MSG_RESPONSE, &ep, compress);
                }
                continue;
            }
        };

        let res = conn.answer(&req_k, msg_type == MSG_SYNC);
        if msg_type == MSG_SYNC {
            match res {
                Ok(val) => {
                    let payload = encode_k_payload(&val.untagged());
                    write_ipc_frame(stream, MSG_RESPONSE, &payload, compress)?;
                }
                Err(err) => {
                    let ep = encode_error_payload(&err);
                    write_ipc_frame(stream, MSG_RESPONSE, &ep, compress)?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_round_trips_and_rejects_damage() {
        let mut x = 7u32;
        let mut rand = move || {
            x = x.wrapping_mul(1_103_515_245).wrapping_add(12_345);
            x >> 16
        };
        for len in [2001usize, 4096, 100_000] {
            // long runs, a repeating pattern, and a little noise
            let src: Vec<u8> = (0..len)
                .map(|i| match (i / 700) % 3 {
                    0 => 0,
                    1 => (i % 11) as u8,
                    _ if rand() % 50 == 0 => rand() as u8,
                    _ => b'a',
                })
                .collect();
            let packed = compress_kdb(&src).expect("compressible");
            assert!(packed.len() < len / 2);
            assert_eq!(uncompress_kdb(&packed, true).unwrap(), src);
            assert!(uncompress_kdb(&packed[..packed.len() / 2], true).is_err());
        }
        // random bytes do not compress
        let noise: Vec<u8> = (0..5000).map(|_| rand() as u8).collect();
        assert!(compress_kdb(&noise).is_none());
    }
}
