//! The functions and constants of the `.Q` (utilities), `.j` (JSON) and `.h` (text, in `markup`) namespaces, as KX
//! documents them. A variable assigned the same name takes their place.
use crate::engine::ops::{self, index_k};
use crate::engine::{json, markup, Engine, Event};
use crate::error::{KError, KResult};
use crate::parser::is_builtin_verb;
use crate::sym::{intern, resolve, sym_count, SymId};
use crate::types::*;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use sha1::{Digest, Sha1};
use std::io::Read;
use std::net::{Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Instant;

/// Each function and the number of its arguments.
const FUNCTIONS: [(&str, usize); 56] = [
    (".Q.addmonths", 2), (".Q.addr", 1), (".Q.atob", 1), (".Q.bt", 1), (".Q.btoa", 1), (".Q.chk", 1), (".Q.dd", 2), (".Q.def", 2),
    (".Q.cn", 1), (".Q.dpft", 4), (".Q.dpfts", 5), (".Q.dpt", 3), (".Q.dpts", 4), (".Q.hdpf", 4), (".Q.MAP", 1), (".Q.par", 3), (".Q.en", 2), (".Q.ens", 3), (".Q.ind", 2), (".Q.view", 1), (".Q.f", 2), (".Q.fc", 2), (".Q.ff", 2), (".Q.fk", 1), (".Q.fmt", 3), (".Q.fs", 2),
    (".Q.fsn", 3), (".Q.ft", 2), (".Q.fu", 2), (".Q.gc", 1), (".Q.gz", 1), (".Q.id", 1), (".Q.j10", 1), (".Q.j12", 1), (".Q.opt", 1), (".Q.qt", 1),
    (".Q.s", 1), (".Q.s1", 1), (".Q.sbt", 1), (".Q.sha1", 1), (".Q.trp", 3), (".Q.trpd", 3), (".Q.ts", 2),
    (".Q.ty", 1), (".Q.v", 1), (".Q.V", 1), (".Q.w", 1), (".Q.x10", 1), (".Q.x12", 1), (".j.j", 1), (".j.jd", 1),
    (".j.k", 1), (".kix.def", 1), (".kix.dist", 2), (".kix.firsts", 2), (".kix.objects", 1),
];

/// Those that change the workspace or write files.
const WRITERS: [&str; 11] = [".Q.chk", ".Q.dpft", ".Q.dpfts", ".Q.dpt", ".Q.dpts", ".Q.hdpf", ".Q.en", ".Q.ens", ".Q.opt", ".Q.view", ".kix.dist"];

/// q's control words and the keywords of k.
const RESERVED: &str = "abs acos asin atan avg bin binr by cor cos cov delete dev div do enlist exec exit exp from getenv \
    hopen if in insert last like log max min prd select setenv sin sqrt ss sum tan update var wavg while within wsum xexp";

const BASE64_DIGITS: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const BASE36_DIGITS: &str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

/// The most that `.Q.gz` unpacks.
const LARGEST_UNPACKED: u64 = 1 << 31;

/// Ask the allocator to give the memory that it holds free back to the system.
fn release_memory() {
    #[cfg(feature = "fast-alloc")]
    {
        use libmimalloc_sys as _;
        extern "C" {
            fn mi_collect(force: bool);
        }
        // SAFETY: it takes no pointers and may be called on any thread; each has memory of its own to give back
        let collect = || unsafe { mi_collect(true) };
        rayon::broadcast(|_| collect());
        collect();
    }
}

pub fn arity(name: &str) -> Option<usize> {
    // (they are all in namespaces, so the other names, as of the primitives, are not looked for)
    if !name.starts_with('.') {
        return None;
    }
    FUNCTIONS.iter().chain(&markup::FUNCTIONS).find(|f| f.0 == name).map(|f| f.1)
}

/// The name that the global `name` has within `namespace` (`""` for the root), if it is one of that one's own.
pub fn member(namespace: &str, name: SymId) -> Option<SymId> {
    let full = resolve(name);
    let own = match namespace {
        "" => Some(full),
        _ => full.strip_prefix(namespace).and_then(|rest| rest.strip_prefix('.')),
    };
    own.filter(|n| !n.contains('.')).map(intern)
}

pub fn constant(name: &str) -> Option<K> {
    let upper = &BASE36_DIGITS[10..];
    let lower = &BASE64_DIGITS[26..52];
    Some(match name {
        ".Q.A" => K::string(upper),
        ".Q.a" => K::string(lower),
        ".Q.an" => K::string(&format!("{lower}{upper}_0123456789")),
        ".Q.b6" => K::string(BASE64_DIGITS),
        ".Q.n" => K::string(&BASE36_DIGITS[..10]),
        ".Q.nA" => K::string(BASE36_DIGITS),
        ".Q.t" => K::string(" bg xhijefcspmdznuvts"),
        ".Q.k" => K::Float(4.1),
        ".Q.res" => syms(RESERVED.split(' ').map(intern).collect()),
        _ => return markup::constant(name),
    })
}

/// Whether a statement that only reads may use `name`.
pub fn reads(name: &str) -> bool {
    (arity(name).is_some() || constant(name).is_some()) && !WRITERS.contains(&name)
}

fn syms(s: Vec<SymId>) -> K {
    K::Vector(ColVec::Sym(Attr::None, Arc::new(s)))
}

fn list(items: Vec<K>) -> K {
    K::Vector(ColVec::from_atoms(items))
}

fn dict(keys: K, vals: K) -> K {
    K::Dict(Arc::new(Dict { keys: Box::new(keys), vals: Box::new(vals) }))
}

fn items(x: &K) -> Vec<K> {
    match x {
        K::Vector(v) => (0..v.len()).map(|i| v.get_atom(i)).collect(),
        one => vec![one.clone()],
    }
}

fn text(x: &K) -> KResult<String> {
    x.as_str().ok_or_else(|| KError::ty("expected a string or symbol"))
}

fn bytes(x: &K) -> KResult<Vec<u8>> {
    match x {
        K::Vector(ColVec::Char(_, b) | ColVec::Byte(_, b)) => Ok((**b).clone()),
        K::Char(b) | K::Byte(b) => Ok(vec![*b]),
        _ => Err(KError::ty("expected characters or bytes")),
    }
}

fn byte_list(b: Vec<u8>) -> K {
    K::Vector(ColVec::Byte(Attr::None, Arc::new(b)))
}

/// `.Q.f`: `y` with `places` decimals.
fn fixed(places: &K, y: &K) -> KResult<String> {
    let (places, y) = (places.as_i64()?.clamp(0, 17) as usize, y.as_f64()?);
    // (beyond a float's precision, as the console writes it)
    Ok(match y.is_finite() && y.abs() * 10f64.powi(places as i32) < 1e17 {
        true => format!("{y:.places$}"),
        false => crate::display::float(y),
    })
}

/// `.Q.j10` and `.Q.j12`: a string of up to `width` digits as a number.
fn encode_digits(x: &K, digits: &str, width: usize) -> KResult<K> {
    let s = text(x)?;
    if s.len() > width {
        return Err(KError::length(format!("at most {width} characters")));
    }
    let digit = |c| digits.find(c).ok_or_else(|| KError::domain(format!("'{c}' is not a digit here")));
    Ok(K::Long(s.chars().try_fold(0i64, |n, c| Ok::<_, KError>(n * digits.len() as i64 + digit(c)? as i64))?))
}

fn decode_digits(x: &K, digits: &str, width: usize) -> KResult<K> {
    let (mut n, base) = (x.as_i64()?, digits.len() as i64);
    let mut out = vec![0u8; width];
    for slot in out.iter_mut().rev() {
        *slot = digits.as_bytes()[n.rem_euclid(base) as usize];
        n = n.div_euclid(base);
    }
    Ok(K::string(&String::from_utf8_lossy(&out)))
}

/// The IPv4 address of a host, by name or dotted number, as an int (-1 if it has none).
pub(crate) fn address(host: &str) -> i32 {
    let found = host.parse::<Ipv4Addr>().ok().or_else(|| {
        (host, 0).to_socket_addrs().ok()?.find_map(|a| if let SocketAddr::V4(a) = a { Some(*a.ip()) } else { None })
    });
    found.map_or(-1, |ip| u32::from(ip) as i32)
}

/// `.Q.id`: names made into valid ones that differ from each other and from q's words.
fn identifiers(names: &[SymId]) -> Vec<SymId> {
    let mut made: Vec<SymId> = vec![];
    for &name in names {
        let mut s: String = resolve(name).chars().filter(|c| c.is_ascii_alphanumeric() || *c == '_').collect();
        if s.is_empty() {
            s = "a".into();
        } else if s.starts_with(|c: char| c.is_ascii_digit() || c == '_') {
            s.insert(0, 'a');
        }
        if is_builtin_verb(&s) || RESERVED.split(' ').any(|w| w == s) {
            s.push('1');
        }
        let (base, mut n) = (s.clone(), 0);
        while made.contains(&intern(&s)) {
            n += 1;
            s = format!("{base}{n}");
        }
        made.push(intern(&s));
    }
    made
}

fn identify(x: &K) -> KResult<K> {
    let flip = |f: &Flip| Flip { cols: identifiers(&f.cols), vals: f.vals.clone() };
    Ok(match x {
        K::Sym(s) => K::Sym(identifiers(&[*s])[0]),
        K::Table(t) => K::Table(Arc::new(flip(t))),
        K::KeyedTable(t) => {
            let all = flip(&t.unkey());
            KeyedTable::from_flip_nkeys(&all, t.keys.ncols())?
        }
        K::Dict(d) => match &*d.keys {
            K::Vector(ColVec::Sym(_, names)) => dict(syms(identifiers(names)), (*d.vals).clone()),
            _ => return Err(KError::ty(".Q.id takes a dictionary with names for keys")),
        },
        _ => return Err(KError::ty(".Q.id takes a symbol, table or dictionary")),
    })
}

/// `.Q.gz`: `::` asks whether gzip is there, `(level; data)` packs and `data` unpacks.
fn gzip(x: &K) -> KResult<K> {
    let packed = match x {
        K::Null => return Ok(K::Bool(true)),
        K::Vector(ColVec::General(_, pair)) if pair.len() == 2 => {
            let level = flate2::Compression::new(pair[0].as_i64()?.clamp(0, 9) as u32);
            let mut out = vec![];
            flate2::read::GzEncoder::new(bytes(&pair[1])?.as_slice(), level).read_to_end(&mut out)?;
            out
        }
        data => {
            let mut out = vec![];
            let unpacker = flate2::read::GzDecoder::new(std::io::Cursor::new(bytes(data)?));
            unpacker.take(LARGEST_UNPACKED + 1).read_to_end(&mut out).map_err(|e| KError::domain(format!("not gzip data: {e}")))?;
            if out.len() as u64 > LARGEST_UNPACKED {
                return Err(KError::new("limit", "too large to unpack"));
            }
            out
        }
    };
    Ok(K::Vector(ColVec::Char(Attr::None, Arc::new(packed))))
}

/// `.Q.sbt`: the functions that were running, the innermost first.
fn show_backtrace(frames: &[K]) -> String {
    let line = |(i, f): (usize, &K)| format!("  [{}]  {}\n", frames.len() - 1 - i, f.cell_text());
    frames.iter().enumerate().map(line).collect()
}

/// A dictionary of names to values (lists of strings) from command-line arguments, and those that come before any name.
fn options(args: &[K]) -> (K, Vec<K>) {
    let name = |a: &K| a.as_str().and_then(|s| s.strip_prefix('-').filter(|n| n.starts_with(|c: char| c.is_alphabetic())).map(intern));
    let (mut others, mut found): (Vec<K>, Vec<(SymId, Vec<K>)>) = (vec![], vec![]);
    for a in args {
        match (name(a), found.last_mut()) {
            (Some(n), _) => found.push((n, vec![])),
            (None, Some((_, values))) => values.push(a.clone()),
            (None, None) => others.push(a.clone()),
        }
    }
    let (names, values): (Vec<_>, Vec<_>) = found.into_iter().map(|(n, v)| (n, K::Vector(ColVec::General(Attr::None, Arc::new(v))))).unzip();
    (dict(syms(names), K::Vector(ColVec::General(Attr::None, Arc::new(values)))), others)
}

/// `.Q.def`: the defaults, with those that `given` (from `.Q.opt`) names read as their types.
fn defaults(x: &K, given: &K) -> KResult<K> {
    let (K::Dict(x), K::Dict(given)) = (x, given) else { return Err(KError::ty(".Q.def takes two dictionaries")) };
    let mut values = vec![];
    for (i, key) in items(&x.keys).iter().enumerate() {
        let default = index_k(&x.vals, &[Some(K::Long(i as i64))])?;
        let argument = items(&given.keys).iter().position(|k| k == key);
        values.push(match argument {
            None => default,
            Some(at) => {
                let words = index_k(&given.vals, &[Some(K::Long(at as i64))])?;
                let word = items(&words).first().cloned().unwrap_or_else(|| K::string(""));
                let letter = " bg xhijefcspmdznuvt".as_bytes().get(default.type_num().unsigned_abs() as usize).copied();
                ops::cast_op(&K::Char(letter.unwrap_or(b'c').to_ascii_uppercase()), &word)?
            }
        });
    }
    Ok(dict((*x.keys).clone(), list(values)))
}

impl Engine<'_> {
    /// The functions that are running, the innermost first.
    pub(crate) fn frames(&self) -> Vec<K> {
        self.running.iter().rev().map(|lam| K::string(&lam.source)).collect()
    }

    /// `.Q.trp` and `.Q.trpd`: as `@[f;x;g]` and `.[f;x;g]`, with `g` given the backtrace too.
    fn trap_with_backtrace(&mut self, f: &K, args: &[Option<K>], handler: &K) -> KResult<K> {
        self.backtrace.clear();
        match self.apply_value(f, args) {
            Ok(value) => Ok(value),
            Err(e) => {
                let trace = list(std::mem::take(&mut self.backtrace));
                match handler {
                    K::Func(g) => self.apply_func(g, &[K::string(&e.signal), trace]),
                    other => Ok(other.clone()),
                }
            }
        }
    }

    /// Call a function of `FUNCTIONS`.
    pub(crate) fn namespace_call(&mut self, name: &str, args: &[K]) -> KResult<K> {
        if name == ".kix.dist" {
            return self.eval_kix_dist(args);
        }
        if arity(name) != Some(args.len()) {
            return Err(KError::rank(format!("wrong number of arguments ({}) to {name}", args.len())));
        }
        let x = &args[0];
        let y = args.get(1).unwrap_or(&K::Null);
        let list_args = |y: &K| items(y).into_iter().map(Some).collect::<Vec<_>>();
        Ok(match name {
            ".Q.fs" => self.in_chunks(x, y, 131_000)?,
            ".Q.fsn" => self.in_chunks(x, y, usize::try_from(args[2].as_i64()?).map_err(|_| KError::domain(".Q.fsn takes a positive size"))?)?,
            ".Q.addmonths" => ops::broadcast_atoms(x, y, |date, months| {
                let (K::Date(d), months) = (date, months.as_i64()?) else { return Err(KError::ty(".Q.addmonths takes a date")) };
                let Some((year, month, day)) = ymd_from_date(*d).filter(|_| *d != NULL_INT) else { return Ok(date.clone()) };
                let total = i64::from(year) * 12 + i64::from(month) - 1 + months;
                let first = i32::try_from(total.div_euclid(12)).ok().and_then(|y| date_from_ymd(y, total.rem_euclid(12) as u32 + 1, 1));
                Ok(K::Date(first.map_or(NULL_INT, |first| first + day as i32 - 1)))
            })?,
            ".Q.addr" => K::Int(address(&text(x)?)),
            ".Q.atob" => byte_list(BASE64.decode(bytes(x)?).map_err(|e| KError::domain(format!("not base64: {e}")))?),
            ".Q.btoa" => K::string(&BASE64.encode(bytes(x)?)),
            ".Q.sha1" => byte_list(Sha1::digest(bytes(x)?).to_vec()),
            ".Q.gz" => gzip(x)?,
            ".Q.j10" => encode_digits(x, BASE64_DIGITS, 10)?,
            ".Q.j12" => encode_digits(x, BASE36_DIGITS, 12)?,
            ".Q.x10" => decode_digits(x, BASE64_DIGITS, 10)?,
            ".Q.x12" => decode_digits(x, BASE36_DIGITS, 12)?,
            ".Q.dd" => {
                let (head, tail) = (text(x)?, y.as_str().unwrap_or_else(|| y.cell_text()));
                let between = if head.starts_with(':') { '/' } else { '.' };
                K::sym(&format!("{head}{between}{tail}"))
            }
            ".Q.f" => K::string(&fixed(x, y)?),
            ".Q.fmt" => {
                let (width, formatted) = (x.as_i64()?.max(0) as usize, fixed(y, &args[2])?);
                match formatted.len() > width {
                    true => K::string(&"*".repeat(width)),
                    false => K::string(&format!("{formatted:>width$}")),
                }
            }
            ".Q.id" => identify(x)?,
            ".Q.s" => K::string(&format!("{}\n", crate::display::console(x))),
            ".Q.s1" => K::string(&x.q_repr()),
            ".Q.ty" => K::Char(match x {
                K::Vector(v) => ops::column_letter(v) as u8,
                atom => ColVec::from_atoms(vec![atom.clone()]).meta_type_char() as u8,
            }),
            ".Q.qt" => K::Bool(matches!(x, K::Table(_) | K::KeyedTable(_))),
            ".Q.fk" => K::Sym(x.tag().filter(|t| t.source == Source::Keys).map_or(SymId::EMPTY, |t| t.domain)),
            ".Q.ff" => {
                let (K::Table(t), K::Table(more)) = (x, y) else { return Err(KError::ty(".Q.ff takes two tables")) };
                let mut out = (**t).clone();
                for (&column, values) in more.cols.iter().zip(&more.vals) {
                    if t.col(column).is_none() {
                        out.cols.push(column);
                        out.vals.push(values.gather(&vec![usize::MAX; t.nrows()]));
                    }
                }
                K::Table(Arc::new(out))
            }
            ".Q.V" => {
                let table = match x {
                    K::Table(t) => (**t).clone(),
                    K::KeyedTable(t) => t.unkey(),
                    _ => return Err(KError::ty(".Q.V takes a table")),
                };
                dict(syms(table.cols), list(table.vals.into_iter().map(K::Vector).collect()))
            }
            ".Q.v" => match x {
                K::Sym(s) if resolve(*s).starts_with(':') => self.eval_get(x)?,
                K::Sym(s) => self.get_var(*s)?,
                other => other.clone(),
            },
            ".Q.fc" => self.apply_value(x, &[Some(y.clone())])?,
            ".Q.fu" => match y {
                K::Vector(_) => {
                    let unique = ops::distinct_op(y)?;
                    let results = self.apply_value(x, &[Some(unique.clone())])?;
                    index_k(&results, &[Some(ops::eval_binary_prim("?", &unique, y)?)])?
                }
                _ => self.apply_value(x, &[Some(y.clone())])?,
            },
            ".Q.ft" => match y {
                K::KeyedTable(t) => match self.apply_value(x, &[Some(K::Table(Arc::new(t.unkey())))])? {
                    K::Table(made) => {
                        let keys: Vec<SymId> = t.keys.cols.iter().copied().filter(|&c| made.col(c).is_some()).collect();
                        KeyedTable::from_flip_key_names(&made, &keys)?
                    }
                    other => other,
                },
                _ => self.apply_value(x, &[Some(y.clone())])?,
            },
            ".Q.ts" => {
                let started = Instant::now();
                let value = self.apply_value(x, &list_args(y))?;
                let cost = vec![started.elapsed().as_millis() as i64, value.approx_bytes() as i64];
                list(vec![K::Vector(ColVec::Long(Attr::None, Arc::new(cost))), value])
            }
            ".Q.trp" => self.trap_with_backtrace(x, &[Some(y.clone())], &args[2])?,
            ".Q.trpd" => self.trap_with_backtrace(x, &list_args(y), &args[2])?,
            ".Q.sbt" => K::string(&show_backtrace(&items(x))),
            ".Q.bt" => {
                let shown = show_backtrace(&self.frames());
                self.print(Event::Message(shown.trim_end().to_string()));
                K::Null
            }
            ".Q.opt" => {
                let (named, others) = options(&items(x));
                self.set_globals(vec![(intern(".Q.x"), Some(K::Vector(ColVec::General(Attr::None, Arc::new(others)))))])?;
                named
            }
            ".Q.def" => defaults(x, y)?,
            // (values are freed as they are dropped, but the allocator keeps their memory for the next ones; how much
            // it then gives back is not measured)
            ".Q.gc" => {
                release_memory();
                K::Long(0)
            }
            ".Q.w" => {
                let names = ["used", "heap", "peak", "wmax", "mmap", "mphy", "syms", "symw"].map(intern).to_vec();
                // (nulls: not measured)
                let used = self.ws.approx_memory_bytes() as i64;
                let values = vec![used, NULL_LONG, NULL_LONG, 0, 0, NULL_LONG, sym_count() as i64, NULL_LONG];
                dict(syms(names), K::Vector(ColVec::Long(Attr::None, Arc::new(values))))
            }
            ".Q.en" => self.enumerate_symbols(x, y, intern("sym"))?,
            ".Q.ens" => {
                let K::Sym(domain) = &args[2] else { return Err(KError::ty(".Q.ens takes the domain's name")) };
                self.enumerate_symbols(x, y, *domain)?
            }
            ".Q.dpfts" => {
                let K::Sym(domain) = &args[4] else { return Err(KError::ty(".Q.dpfts takes the domain's name")) };
                self.write_partition(x, y, Some(&args[2]), &args[3], *domain)?
            }
            ".Q.dpts" => {
                let K::Sym(domain) = &args[3] else { return Err(KError::ty(".Q.dpts takes the domain's name")) };
                self.write_partition(x, y, None, &args[2], *domain)?
            }
            ".Q.dpt" => self.write_partition(x, y, None, &args[2], intern("sym"))?,
            ".Q.hdpf" => self.save_day(x, y, &args[2], &args[3])?,
            ".Q.par" => self.partition_path(x, y, &args[2])?,
            ".Q.cn" => self.partition_counts(x)?,
            // (nothing is held mapped: a query reads what it needs)
            ".Q.MAP" => K::Null,
            ".Q.chk" => self.check_partitions(x)?,
            ".Q.view" => self.view_partitions(x)?,
            // (a partitioned table is loaded whole when it is named, so its rows are indexed like any table's)
            ".Q.ind" => index_k(x, &[Some(y.clone())])?,
            ".Q.dpft" => self.write_partition(x, y, Some(&args[2]), &args[3], intern("sym"))?,
            ".j.j" => K::string(&json::write(x, false)),
            ".j.jd" => {
                let pair = items(x);
                let [value, K::Dict(settings)] = pair.as_slice() else { return Err(KError::ty(".j.jd takes (value; settings)")) };
                let null_infinities = ops::dict_lookup(settings, &K::sym("null0w")).is_ok_and(|v| v.as_bool().unwrap_or(false));
                K::string(&json::write(value, null_infinities))
            }
            ".j.k" => json::read(&text(x)?)?,
            ".kix.objects" => crate::inspect::workspace_objects_table(self.ws)?,
            // the first of each of the table's rows that agree in every column but the one named (a shard's `distinct`)
            ".kix.firsts" => {
                let (K::Table(t), K::Sym(aside)) = (x, y) else { return Err(KError::ty(".kix.firsts takes a table and a column's name")) };
                let compared: Vec<ColVec> = t.cols.iter().zip(&t.vals).filter(|(c, _)| *c != aside).map(|(_, v)| v.clone()).collect();
                K::Table(Arc::new(t.gather(&ops::first_rows(&ops::group_ids(&compared, t.nrows())))))
            }
            ".kix.def" => {
                let target = x.as_str().unwrap_or_else(|| x.cell_text());
                crate::inspect::workspace_definition(self.ws, &target)?
            }
            _ => self.markup_call(name, args)?,
        })
    }
}
