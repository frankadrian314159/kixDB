//! kdb+-style on-disk storage:
//! - Flat serialized `K` files (`` `:path set val `` / `` get `:path ``)
//! - Splayed tables (`` `:dir/table/ set t `` -> `.d` column order + one binary column file per column + `sym` enumeration)
//! - Logs: lists appended to item by item, as a tickerplant's log of messages is
//! - Partitioned HDB directories (`dir/2026.11.24/trade/` via `.Q.dpft` and `\l dir`, with predicate partition pruning!)
//! - `0:` delimited CSV parsing & formatting and `1:` binary reads
use crate::ast::Expr;
use crate::engine::Workspace;
use crate::error::{KError, KResult};
use crate::ipc::{decode_k_payload, encode_k_stored};
use crate::sym::{intern, resolve, SymId};
use crate::types::*;
use std::fs;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const FLAT_MAGIC: [u8; 4] = [0xfe, 0x20, b'K', b'X'];
const LOG_MAGIC: [u8; 4] = [0xfe, 0x20, b'K', b'L'];

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartKind {
    Date,
    Month,
    Year,
    Int,
}

#[derive(Clone, Debug)]
pub struct PartitionSlice {
    pub part_val: K,
    pub path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct PartitionedTable {
    pub name: SymId,
    pub part_col: SymId,
    pub kind: PartKind,
    /// The database's directory.
    pub root: PathBuf,
    /// The partitions that queries see, in order, and those that `.Q.view` hides.
    pub slices: Vec<PartitionSlice>,
    pub hidden: Vec<PartitionSlice>,
    /// The partition column, then the columns of the last partition.
    pub cols: Vec<SymId>,
}

/// A partition's kind, column, value and directory.
type Partition = (PartKind, SymId, K, PathBuf);

/// The partition directories of the database in `dir`, in order of their values: those in it, and those in the
/// directories that its `par.txt` lists, if it has one.
pub fn partition_dirs(ws: &Workspace, dir: &Path) -> KResult<Vec<Partition>> {
    let mut roots = vec![dir.to_path_buf()];
    roots.extend(segments(dir).iter().map(|s| resolve_fs_path(ws, s)));
    let mut found = Vec::new();
    for root in roots {
        let missing = |e: std::io::Error| KError::io(format!("cannot read the partitions in '{}': {e}", root.display()));
        for entry in fs::read_dir(&root).map_err(missing)? {
            let path = entry?.path();
            let name = path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
            if let Some((kind, column, value)) = parse_partition_dir_name(&name).filter(|_| path.is_dir()) {
                found.push((kind, column, value, path));
            }
        }
    }
    found.sort_by(|a, b| a.2.k_cmp(&b.2));
    Ok(found)
}

/// The directories that the `par.txt` of the database in `dir` lists, as they are written there.
pub fn segments(dir: &Path) -> Vec<String> {
    let listed = fs::read_to_string(dir.join("par.txt")).unwrap_or_default();
    listed.lines().map(|l| l.trim().trim_start_matches(':').to_string()).filter(|l| !l.is_empty()).collect()
}

fn resolve_fs_path(ws: &Workspace, rel: &str) -> PathBuf {
    match base_directory(ws) {
        // (joining an absolute path gives that path)
        Some(base) => base.join(rel),
        None => PathBuf::from(rel),
    }
}

/// The directory that relative paths are taken from, if it is not the process's: that of `\cd`, else the workspace's.
pub(crate) fn base_directory(ws: &Workspace) -> Option<PathBuf> {
    ws.host.cwd.lock().unwrap_or_else(|e| e.into_inner()).clone().or_else(|| ws.data_dir.clone())
}

/// The path that a file symbol or string (`` `:path ``) stands for.
pub fn path_of(ws: &Workspace, x: &K) -> KResult<PathBuf> {
    let name = x.as_str().ok_or_else(|| KError::ty("expected a file symbol (`:path)"))?;
    Ok(resolve_fs_path(ws, name.strip_prefix(':').unwrap_or(&name)))
}

/// Make the directories that a file at `path` is to be in.
pub fn create_parent(path: &Path) -> KResult<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

/// The first bytes of the file, which say what it holds.
fn magic(path: &Path) -> Option<[u8; 4]> {
    let mut start = [0u8; 4];
    fs::File::open(path).and_then(|mut f| f.read_exact(&mut start)).ok().map(|_| start)
}

/// Whether the file holds a `q` value, as `set` writes one, or appending to one does.
pub fn holds_value(path: &Path) -> bool {
    matches!(magic(path), Some(FLAT_MAGIC | LOG_MAGIC))
}

/// The items of a log that `x` adds, each as its length, its checksum and its value.
fn log_records<'a>(items: impl IntoIterator<Item = &'a K>) -> Vec<u8> {
    let mut bytes = Vec::new();
    for item in items {
        let payload = encode_k_stored(item);
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        bytes.extend_from_slice(&payload);
    }
    bytes
}

/// Join `x` to the value that the file holds. A general list that is added to becomes a log, so that what is added
/// later costs only itself.
pub fn append_value(path: &Path, x: &K) -> KResult<()> {
    let items: Option<Vec<K>> = match x {
        K::Vector(v) => Some((0..v.len()).map(|i| v.get_atom(i)).collect()),
        K::Dict(_) | K::Table(_) | K::KeyedTable(_) => None,
        atom => Some(vec![atom.clone()]),
    };
    if let (Some(items), Some(LOG_MAGIC)) = (&items, magic(path)) {
        return Ok(fs::OpenOptions::new().append(true).open(path)?.write_all(&log_records(items))?);
    }
    match (read_flat_k(path)?, items) {
        (K::Vector(ColVec::General(_, held)), Some(items)) => {
            Ok(fs::write(path, [LOG_MAGIC.to_vec(), log_records(held.iter().chain(&items))].concat())?)
        }
        (held, _) => write_flat_k(path, &crate::engine::ops::eval_binary_prim(",", &held, x)?),
    }
}

/// The next item of a log, of which `left` bytes remain, unless what follows is cut short or damaged.
fn next_record(file: &mut impl Read, left: u64, record: &mut Vec<u8>) -> Option<K> {
    let mut head = [0u8; 8];
    file.read_exact(&mut head).ok()?;
    let length = u32::from_le_bytes([head[0], head[1], head[2], head[3]]);
    if u64::from(length) + 8 > left {
        return None;
    }
    record.resize(length as usize, 0);
    file.read_exact(record).ok()?;
    if crc32fast::hash(record).to_le_bytes() != head[4..] {
        return None;
    }
    decode_k_payload(record, 0).ok().filter(|(_, used)| *used == record.len()).map(|(item, _)| item)
}

/// Give `each` the first `limit` items of the log, or of the list, that the file holds. Returns how many it was given
/// and, if what follows them in the log is damaged, how many of its bytes are good.
pub fn read_log(path: &Path, limit: u64, mut each: impl FnMut(K) -> KResult<()>) -> KResult<(u64, Option<u64>)> {
    if magic(path) != Some(LOG_MAGIC) {
        let K::Vector(list) = read_flat_k(path)? else { return Err(KError::ty("the file does not hold a list")) };
        let count = limit.min(list.len() as u64);
        (0..count as usize).try_for_each(|i| each(list.get_atom(i)))?;
        return Ok((count, None));
    }
    let mut file = BufReader::new(fs::File::open(path)?);
    let size = file.get_ref().metadata()?.len();
    file.read_exact(&mut [0u8; 4])?;
    let (mut good, mut count, mut record) = (4, 0, Vec::new());
    while count < limit && good < size {
        let Some(item) = next_record(&mut file, size - good, &mut record) else { return Ok((count, Some(good))) };
        each(item)?;
        good += 8 + record.len() as u64;
        count += 1;
    }
    Ok((count, None))
}

/// The bytes of `` `:file ``, or the part of them that ``(`:file; offset; length)`` names.
pub fn read_bytes(ws: &Workspace, x: &K) -> KResult<Vec<u8>> {
    let (file, from, length) = match x {
        K::Vector(ColVec::General(_, part)) if matches!(part.len(), 2 | 3) => {
            let number = |k: &K| k.as_i64().and_then(|n| u64::try_from(n).map_err(|_| KError::domain("offsets and lengths are not negative")));
            (&part[0], number(&part[1])?, part.get(2).map(number).transpose()?)
        }
        file => (file, 0, None),
    };
    let mut opened = fs::File::open(path_of(ws, file)?)?;
    opened.seek(SeekFrom::Start(from))?;
    let mut bytes = Vec::new();
    opened.take(length.unwrap_or(u64::MAX)).read_to_end(&mut bytes)?;
    Ok(bytes)
}

pub fn write_flat_k(path: &Path, val: &K) -> KResult<()> {
    create_parent(path)?;
    let payload = encode_k_stored(val);
    let mut buf = Vec::with_capacity(8 + payload.len());
    buf.extend_from_slice(&FLAT_MAGIC);
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&payload);
    fs::write(path, buf)?;
    Ok(())
}

pub fn read_flat_k(path: &Path) -> KResult<K> {
    if magic(path) == Some(LOG_MAGIC) {
        let mut items = Vec::new();
        let read = read_log(path, u64::MAX, |item| {
            items.push(item);
            Ok(())
        })?;
        if read.1.is_some() {
            return Err(KError::io(format!("damaged log '{}' (see -11!(-2;file))", path.display())));
        }
        let mut list = K::Vector(ColVec::from_atoms(items));
        list.pack();
        return Ok(list);
    }
    let buf = fs::read(path)?;
    if buf.len() < 8 || buf[..4] != FLAT_MAGIC {
        return Err(KError::io(format!("invalid kixdb binary file '{}'", path.display())));
    }
    let (mut k, _) = decode_k_payload(&buf[8..], 0)?;
    k.pack();
    Ok(k)
}

/// Save a `K` value to `:path`: splayed if `rel` ends with `/` or a splayed table is there already.
pub fn save_path(ws: &Workspace, rel: &str, val: &K) -> KResult<()> {
    let path = resolve_fs_path(ws, rel.trim_end_matches(['/', '\\']));
    let is_splay = rel.ends_with('/') || rel.ends_with('\\') || path.join(".d").exists();
    if is_splay {
        let flip = match val {
            K::Table(t) => (**t).clone(),
            K::KeyedTable(kt) => kt.unkey(),
            _ => return Err(KError::ty("splayed path (trailing '/') requires a table")),
        };
        write_splayed_table(&path, &flip)
    } else {
        write_flat_k(&path, val)
    }
}

pub fn load_path(ws: &Workspace, rel: &str) -> KResult<K> {
    load_file(&resolve_fs_path(ws, rel.trim_end_matches(['/', '\\'])))
}

/// The splayed table or the value at `path`.
pub fn load_file(path: &Path) -> KResult<K> {
    if path.is_dir() && path.join(".d").exists() {
        let flip = read_splayed_table(path, None)?;
        Ok(K::Table(Arc::new(flip)))
    } else {
        read_flat_k(path)
    }
}

pub fn write_splayed_table(dir: &Path, flip: &Flip) -> KResult<()> {
    fs::create_dir_all(dir)?;
    let d_vec = K::Vector(ColVec::Sym(Attr::None, Arc::new(flip.cols.clone())));
    write_flat_k(&dir.join(".d"), &d_vec)?;
    for (col_sym, col_vec) in flip.cols.iter().zip(&flip.vals) {
        let col_name = resolve(*col_sym);
        write_flat_k(&dir.join(col_name), &K::Vector(col_vec.clone()))?;
    }
    Ok(())
}

pub fn read_splayed_table(dir: &Path, needed_cols: Option<&[SymId]>) -> KResult<Flip> {
    let d_k = read_flat_k(&dir.join(".d"))?;
    let K::Vector(ColVec::Sym(_, all_cols)) = d_k else {
        return Err(KError::io("corrupt splayed .d file"));
    };
    let cols_to_read: Vec<SymId> = match needed_cols {
        Some(req) => all_cols.iter().copied().filter(|c| req.contains(c)).collect(),
        None => (*all_cols).clone(),
    };
    let mut vals = Vec::with_capacity(cols_to_read.len());
    for &c in &cols_to_read {
        let ck = read_flat_k(&dir.join(resolve(c)))?;
        let K::Vector(mut cv) = ck else {
            return Err(KError::io(format!("corrupt splayed column '{}'", resolve(c))));
        };
        cv.pack();
        vals.push(cv);
    }
    Flip::new(cols_to_read, vals)
}

/// Mount an HDB directory (`\l dir`): loads flat files, splayed tables, and partitioned (`YYYY.MM.DD/...`) tables!
/// Gives the names of the partitioned tables.
pub fn mount_hdb_dir(ws: &mut Workspace, dir: &Path) -> KResult<Vec<SymId>> {
    if !dir.exists() {
        return Ok(vec![]);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let fname = entry.file_name().to_string_lossy().into_owned();
        if fname.starts_with('.') || fname == "par.txt" || fname.starts_with("kix.") {
            continue;
        }
        if path.is_dir() {
            if parse_partition_dir_name(&fname).is_none() && path.join(".d").exists() {
                // Top-level splayed table!
                let flip = read_splayed_table(&path, None)?;
                ws.vars.insert(intern(&fname), K::Table(Arc::new(flip)));
                ws.unlogged.insert(intern(&fname));
            }
        } else if fname.ends_with(".q") {
            // Skip auto-running scripts unless `\l script.q` was invoked directly
        } else if let Ok(k) = read_flat_k(&path) {
            ws.vars.insert(intern(&fname), k);
            ws.unlogged.insert(intern(&fname));
        }
    }

    // Group partition slices by table name
    let mut by_table: std::collections::HashMap<SymId, PartitionedTable> = std::collections::HashMap::new();
    for (kind, pcol, pval, pdir) in partition_dirs(ws, dir)? {
        for sub in fs::read_dir(&pdir)? {
            let sub = sub?;
            let spath = sub.path();
            if spath.is_dir() && spath.join(".d").exists() {
                let tname = intern(&sub.file_name().to_string_lossy());
                let d_k = read_flat_k(&spath.join(".d"))?;
                let K::Vector(ColVec::Sym(_, s_cols)) = d_k else { continue };
                let pt = by_table.entry(tname).or_insert_with(|| PartitionedTable {
                    name: tname,
                    part_col: pcol,
                    kind: kind.clone(),
                    root: dir.to_path_buf(),
                    slices: Vec::new(),
                    hidden: Vec::new(),
                    cols: Vec::new(),
                });
                // (the last partition has the say on the columns, as in q)
                pt.cols = [pt.part_col].into_iter().chain(s_cols.iter().copied()).collect();
                pt.slices.push(PartitionSlice { part_val: pval.clone(), path: spath });
            }
        }
    }
    let mounted = by_table.keys().copied().collect();
    for (tname, pt) in by_table {
        ws.partitioned.insert(tname, pt);
    }
    Ok(mounted)
}

fn parse_partition_dir_name(name: &str) -> Option<(PartKind, SymId, K)> {
    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() == 3 && parts[0].len() == 4 {
        let y: i32 = parts[0].parse().ok()?;
        let m: u32 = parts[1].parse().ok()?;
        let d: u32 = parts[2].parse().ok()?;
        let days = date_from_ymd(y, m, d)?;
        return Some((PartKind::Date, intern("date"), K::Date(days)));
    }
    if parts.len() == 2 && parts[0].len() == 4 {
        let y: i32 = parts[0].parse().ok()?;
        let m: i32 = parts[1].parse().ok()?;
        if (1..=12).contains(&m) {
            return Some((PartKind::Month, intern("month"), K::Month((y - 2000) * 12 + (m - 1))));
        }
    }
    if name.len() == 4 && name.bytes().all(|b| b.is_ascii_digit()) {
        let y: i32 = name.parse().ok()?;
        if (1800..=2999).contains(&y) {
            return Some((PartKind::Year, intern("year"), K::Int(y)));
        }
    }
    if !name.is_empty() && name.bytes().all(|b| b.is_ascii_digit()) {
        let n: i64 = name.parse().ok()?;
        return Some((PartKind::Int, intern("int"), K::Long(n)));
    }
    None
}

/// Load a partitioned HDB table, pruning partitions across all `where` clauses that constrain the partition column!
pub fn load_partitioned_filtered(
    pt: &PartitionedTable,
    where_clauses: &[Expr],
    needed: Option<&[SymId]>,
    mut eval_rhs: impl FnMut(&Expr) -> KResult<K>,
) -> KResult<Flip> {
    // only the referenced columns are read from disk (at least one, to know the number of rows)
    let on_disk = pt.cols.get(1..).unwrap_or_default();
    let mut wanted: Vec<SymId> = on_disk.iter().copied().filter(|c| needed.is_none_or(|n| n.contains(c))).collect();
    if wanted.is_empty() {
        wanted.extend(on_disk.first());
    }
    let mut selected_slices: Vec<&PartitionSlice> = pt.slices.iter().collect();
    for wc in where_clauses {
        // Stop pruning if an earlier clause references virtual row index `i` (whose numbering depends on prior clauses).
        if wc.any(&mut |x| matches!(x, Expr::Var(s) if resolve(*s) == "i")) {
            break;
        }
        if let Some(pred) = match_partition_predicate(pt.part_col, &pt.cols, wc, &mut eval_rhs)? {
            selected_slices.retain(|s| pred(&s.part_val));
        }
    }

    if selected_slices.is_empty() {
        // Return an empty schema-preserving Flip!
        let mut vals = Vec::with_capacity(pt.cols.len());
        let p_empty = match pt.kind {
            PartKind::Date => ColVec::Date(Attr::Sorted, Arc::new(vec![])),
            PartKind::Month => ColVec::Month(Attr::Sorted, Arc::new(vec![])),
            PartKind::Year => ColVec::Int(Attr::Sorted, Arc::new(vec![])),
            PartKind::Int => ColVec::Long(Attr::Sorted, Arc::new(vec![])),
        };
        vals.push(p_empty);
        if let Some(sample_slice) = pt.slices.last().or(pt.hidden.last()) {
            let sf = read_slice(&sample_slice.path, &wanted)?;
            let mut cols = vec![pt.part_col];
            cols.extend(&sf.cols);
            vals.extend(sf.vals.iter().map(|v| v.take(0)));
            return Flip::new(cols, vals);
        }
        return Flip::new(vec![pt.part_col], vals);
    }

    let mut acc: Option<Flip> = None;
    for slice in selected_slices {
        let sf = read_slice(&slice.path, &wanted)?;
        let nrows = sf.nrows();
        // (typed even for a partition with no rows)
        let mut pcol = ColVec::from_atoms(vec![slice.part_val.clone()]).gather(&vec![0; nrows]);
        pcol.set_attr_unchecked(Attr::Sorted);
        let mut cols = vec![pt.part_col];
        cols.extend_from_slice(&sf.cols);
        let mut vals = vec![pcol];
        vals.extend(sf.vals);
        let part_flip = Flip::new(cols, vals)?;
        acc = Some(match acc {
            None => part_flip,
            Some(prev) => prev.concat(&part_flip)?,
        });
    }
    Ok(acc.unwrap())
}

/// The columns `wanted` of the partition of a table in `dir`, in that order.
fn read_slice(dir: &Path, wanted: &[SymId]) -> KResult<Flip> {
    let held = read_splayed_table(dir, Some(wanted))?;
    let lacking = |c| KError::new("mismatch", format!("{} has no column {}", dir.display(), resolve(c)));
    let vals = wanted.iter().map(|&c| held.col(c).cloned().ok_or_else(|| lacking(c))).collect::<KResult<Vec<_>>>()?;
    Flip::new(wanted.to_vec(), vals)
}

type PartPred = Box<dyn Fn(&K) -> bool>;

fn match_partition_predicate(
    part_col: SymId,
    table_cols: &[SymId],
    wc: &Expr,
    eval_rhs: &mut impl FnMut(&Expr) -> KResult<K>,
) -> KResult<Option<PartPred>> {
    let Expr::Binary { op, lhs, rhs } = wc else {
        return Ok(None);
    };
    let op_s = resolve(*op);
    let no_cols = |e: &Expr| !e.any(&mut |x| matches!(x, Expr::Var(s) | Expr::Field(_, s) if table_cols.contains(s)));
    let rhs_expr = match (&**lhs, &**rhs, op_s) {
        (Expr::Var(c), r, _) if *c == part_col && no_cols(r) => r,
        (l, Expr::Var(c), "=") if *c == part_col && no_cols(l) => l,
        _ => return Ok(None),
    };
    match op_s {
        "=" => {
            let v = eval_rhs(rhs_expr)?;
            Ok(Some(Box::new(move |pv| pv.k_cmp(&v) == std::cmp::Ordering::Equal)))
        }
        "<" => {
            let v = eval_rhs(rhs_expr)?;
            Ok(Some(Box::new(move |pv| pv.k_cmp(&v) == std::cmp::Ordering::Less)))
        }
        "<=" => {
            let v = eval_rhs(rhs_expr)?;
            Ok(Some(Box::new(move |pv| pv.k_cmp(&v) != std::cmp::Ordering::Greater)))
        }
        ">" => {
            let v = eval_rhs(rhs_expr)?;
            Ok(Some(Box::new(move |pv| pv.k_cmp(&v) == std::cmp::Ordering::Greater)))
        }
        ">=" => {
            let v = eval_rhs(rhs_expr)?;
            Ok(Some(Box::new(move |pv| pv.k_cmp(&v) != std::cmp::Ordering::Less)))
        }
        "in" => {
            let v = eval_rhs(rhs_expr)?;
            let items: Vec<K> = match v {
                K::Vector(cv) => (0..cv.len()).map(|i| cv.get_atom(i)).collect(),
                atom => vec![atom],
            };
            Ok(Some(Box::new(move |pv| items.iter().any(|it| pv.k_cmp(it) == std::cmp::Ordering::Equal))))
        }
        "within" => {
            let v = eval_rhs(rhs_expr)?;
            if let K::Vector(cv) = v {
                if cv.len() == 2 {
                    let lo = cv.get_atom(0);
                    let hi = cv.get_atom(1);
                    return Ok(Some(Box::new(move |pv| {
                        pv.k_cmp(&lo) != std::cmp::Ordering::Less && pv.k_cmp(&hi) != std::cmp::Ordering::Greater
                    })));
                }
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

// ----- `0:`, `1:`, `hcount` -------------------------------------------------

pub fn hcount(ws: &Workspace, x: &K) -> KResult<K> {
    Ok(K::Long(fs::metadata(path_of(ws, x)?)?.len() as i64))
}

fn write_file(path: &Path, bytes: &[u8]) -> KResult<()> {
    create_parent(path)?;
    Ok(fs::write(path, bytes)?)
}

pub fn one_colon(ws: &Workspace, lhs: &K, rhs: &K) -> KResult<K> {
    // `` `:file 1: bytes `` writes raw bytes, as `read1` reads them
    if let (K::Sym(_), K::Vector(ColVec::Byte(_, bs)) | K::Vector(ColVec::Char(_, bs))) = (lhs, rhs) {
        write_file(&path_of(ws, lhs)?, bs)?;
        return Ok(lhs.clone());
    }
    Err(KError::ty("1: expects `:file and byte/char vector"))
}

/// Table `flip` as lines of text with `delimiter` between its fields, the first line naming the columns.
pub fn delimited_lines(delimiter: char, flip: &Flip) -> K {
    let field = |s: String| match s.contains([delimiter, '"', '\n', '\r']) {
        true => format!("\"{}\"", s.replace('"', "\"\"")),
        false => s,
    };
    let line = |cells: Vec<String>| K::string(&cells.join(&delimiter.to_string()));
    let mut lines = vec![line(flip.cols.iter().map(|&c| field(resolve(c).into())).collect())];
    lines.extend((0..flip.nrows()).map(|r| line(flip.vals.iter().map(|c| field(c.get_atom(r).data_text())).collect())));
    K::Vector(ColVec::General(Attr::None, Arc::new(lines)))
}

/// Typed single-pass column builder for `0:` text parsing and streaming CSV imports.
pub(crate) enum ColBuilder {
    Skip,
    Bool(crate::bools::Bools),
    Guid(Vec<[u8; 16]>),
    Byte(Vec<u8>),
    Short(Vec<i16>),
    Int(char, Vec<i32>),
    Long(char, Vec<i64>),
    Real(Vec<f32>),
    Float(char, Vec<f64>),
    Char(Vec<u8>),
    Sym(Vec<SymId>),
    PackedStrings(crate::strings::Strings),
    Strings(Vec<K>),
}

impl ColBuilder {
    pub(crate) fn with_capacity(tc: char, cap: usize, pack_strings: bool) -> Self {
        if tc == ' ' {
            return Self::Skip;
        }
        if tc == '*' || tc == 'C' {
            return if pack_strings {
                Self::PackedStrings(crate::strings::Strings::with_capacity(cap, cap * 16))
            } else {
                Self::Strings(Vec::with_capacity(cap))
            };
        }
        let kind = tc.to_ascii_lowercase();
        match kind {
            'b' => Self::Bool(crate::bools::Bools::with_capacity(cap)),
            'g' => Self::Guid(Vec::with_capacity(cap)),
            'x' => Self::Byte(Vec::with_capacity(cap)),
            'h' => Self::Short(Vec::with_capacity(cap)),
            'i' | 'd' | 'm' | 'u' | 'v' | 't' => Self::Int(kind, Vec::with_capacity(cap)),
            'j' | 'p' | 'n' => Self::Long(kind, Vec::with_capacity(cap)),
            'e' => Self::Real(Vec::with_capacity(cap)),
            'f' | 'z' => Self::Float(kind, Vec::with_capacity(cap)),
            'c' => Self::Char(Vec::with_capacity(cap)),
            's' => Self::Sym(Vec::with_capacity(cap)),
            _ => {
                if pack_strings {
                    Self::PackedStrings(crate::strings::Strings::with_capacity(cap, cap * 16))
                } else {
                    Self::Strings(Vec::with_capacity(cap))
                }
            }
        }
    }

    #[inline]
    pub(crate) fn push(&mut self, s: &str) {
        match self {
            Self::Skip => {}
            Self::Bool(b) => b.push(matches!(s.trim(), "1" | "1b" | "true" | "TRUE" | "t" | "T")),
            Self::Guid(v) => v.push(parse_guid(s.trim()).unwrap_or(NULL_GUID)),
            Self::Byte(v) => v.push(u8::from_str_radix(s.trim().trim_start_matches("0x"), 16).unwrap_or(0)),
            Self::Short(v) => v.push(s.trim().trim_end_matches('h').parse().unwrap_or(NULL_SHORT)),
            Self::Int('i', v) => v.push(s.trim().trim_end_matches('i').parse().unwrap_or(NULL_INT)),
            Self::Int(kind, v) => {
                let n = match crate::engine::ops::parse_atom_from_str(*kind, s) {
                    K::Date(n) | K::Month(n) | K::Minute(n) | K::Second(n) | K::Time(n) | K::Int(n) => n,
                    _ => NULL_INT,
                };
                v.push(n);
            }
            Self::Long('j', v) => v.push(s.trim().trim_end_matches('j').parse().unwrap_or(NULL_LONG)),
            Self::Long('n', v) => {
                let n = match crate::lexer::parse_scalar_token(s.trim()) {
                    Ok((K::Timespan(ns), None)) => ns,
                    _ => NULL_LONG,
                };
                v.push(n);
            }
            Self::Long(kind, v) => {
                let n = match crate::engine::ops::parse_atom_from_str(*kind, s) {
                    K::Timestamp(n) | K::Timespan(n) | K::Long(n) => n,
                    _ => NULL_LONG,
                };
                v.push(n);
            }
            Self::Real(v) => v.push(s.trim().trim_end_matches('e').parse().unwrap_or(f32::NAN)),
            Self::Float('f', v) => v.push(s.trim().trim_end_matches('f').parse().unwrap_or(f64::NAN)),
            Self::Float(_, v) => {
                let f = match crate::lexer::parse_scalar_token(s.trim()) {
                    Ok((K::Datetime(dt), None)) => dt,
                    _ => f64::NAN,
                };
                v.push(f);
            }
            Self::Char(v) => v.push(s.as_bytes().first().copied().unwrap_or(b' ')),
            Self::Sym(v) => v.push(intern(s.trim())),
            Self::PackedStrings(v) => v.push(s.as_bytes()),
            Self::Strings(v) => v.push(K::string(s)),
        }
    }

    pub(crate) fn finish(self) -> Option<ColVec> {
        Some(match self {
            Self::Skip => return None,
            Self::Bool(b) => ColVec::Bool(Attr::None, Arc::new(b)),
            Self::Guid(v) => ColVec::Guid(Attr::None, Arc::new(v)),
            Self::Byte(v) => ColVec::Byte(Attr::None, Arc::new(v)),
            Self::Short(v) => ColVec::Short(Attr::None, Arc::new(v)),
            Self::Int('d', v) => ColVec::Date(Attr::None, Arc::new(v)),
            Self::Int('m', v) => ColVec::Month(Attr::None, Arc::new(v)),
            Self::Int('u', v) => ColVec::Minute(Attr::None, Arc::new(v)),
            Self::Int('v', v) => ColVec::Second(Attr::None, Arc::new(v)),
            Self::Int('t', v) => ColVec::Time(Attr::None, Arc::new(v)),
            Self::Int(_, v) => ColVec::Int(Attr::None, Arc::new(v)),
            Self::Long('p', v) => ColVec::Timestamp(Attr::None, Arc::new(v)),
            Self::Long('n', v) => ColVec::Timespan(Attr::None, Arc::new(v)),
            Self::Long(_, v) => ColVec::Long(Attr::None, Arc::new(v)),
            Self::Real(v) => ColVec::Real(Attr::None, Arc::new(v)),
            Self::Float('z', v) => ColVec::Datetime(Attr::None, Arc::new(v)),
            Self::Float(_, v) => ColVec::Float(Attr::None, Arc::new(v)),
            Self::Char(v) => ColVec::Char(Attr::None, Arc::new(v)),
            Self::Sym(v) => ColVec::Sym(Attr::None, Arc::new(v)),
            Self::PackedStrings(v) => ColVec::of_strings(Attr::None, v),
            Self::Strings(v) => ColVec::General(Attr::None, Arc::new(v)),
        })
    }
}

/// The text to parse: a file or a part of one, a string, or a list of strings.
fn source_text(ws: &Workspace, x: &K) -> KResult<String> {
    let file = |k: &K| matches!(k, K::Sym(s) if resolve(*s).starts_with(':'));
    Ok(match x {
        K::Sym(_) => String::from_utf8_lossy(&read_bytes(ws, x)?).into_owned(),
        K::Vector(ColVec::General(_, part)) if part.first().is_some_and(file) => String::from_utf8_lossy(&read_bytes(ws, x)?).into_owned(),
        K::Vector(ColVec::Char(_, bs)) => String::from_utf8_lossy(bs).into_owned(),
        K::Vector(cv) => (0..cv.len()).map(|i| cv.get_atom(i).as_str().unwrap_or_default()).collect::<Vec<_>>().join("\n"),
        _ => return Err(KError::ty("0: RHS must be `:file, string, or list of strings")),
    })
}

/// `(types; widths) 0: text`: each line holds records of fields of those widths. A blank type skips its field.
fn fixed_width(types: &str, widths: &[i64], text: &str) -> KResult<K> {
    if types.chars().count() != widths.len() || widths.iter().any(|&w| w <= 0) {
        return Err(KError::length("0: needs a positive width for each type"));
    }
    let record: usize = widths.iter().map(|&w| w as usize).sum();
    let mut builders: Vec<ColBuilder> = types.chars().map(|tc| ColBuilder::with_capacity(tc, 0, false)).collect();
    for line in text.lines() {
        let mut rest = line;
        while !rest.is_empty() {
            for (builder, &width) in builders.iter_mut().zip(widths) {
                let cut = rest.char_indices().nth(width as usize).map_or(rest.len(), |c| c.0);
                builder.push(&rest[..cut]);
                rest = &rest[cut..];
            }
            if line.len() < record {
                break;
            }
        }
    }
    let columns: Vec<K> = builders.into_iter().filter_map(ColBuilder::finish).map(K::Vector).collect();
    Ok(K::Vector(ColVec::General(Attr::None, Arc::new(columns))))
}

/// `"S=;" 0: text`: the keys (of the first type) and the values (strings) of pairs such as `a=1;b=2`.
fn key_values(spec: &[u8], text: &str) -> KResult<K> {
    let &[kind, within, between, ..] = spec else { return Err(KError::length("0: needs a key type and two delimiters")) };
    let mut keys = ColBuilder::with_capacity(kind as char, 0, false);
    let mut values = ColBuilder::with_capacity('*', 0, false);
    for p in text.split(between as char).filter(|p| !p.is_empty()) {
        let (k, v) = p.split_once(within as char).unwrap_or((p, ""));
        keys.push(k);
        values.push(v);
    }
    let sides = [keys.finish().unwrap(), values.finish().unwrap()];
    Ok(K::Vector(ColVec::General(Attr::None, Arc::new(sides.into_iter().map(K::Vector).collect()))))
}

/// kdb+'s `0:` CSV / text operator:
/// - `"," 0: table` -> list of CSV lines (header + rows)
/// - `` `:out.csv 0: lines `` -> writes lines to file
/// - `("SFI"; enlist ",") 0: `:file.csv` (or string/lines) -> parses CSV with header into `Table`!
/// - `("SFI"; ",") 0: lines` -> parses CSV without header into list of `ColVec`s!
/// - `("SFI"; 4 8 3) 0: lines` -> parses fields of fixed widths; `"S=;" 0: string` parses keys and values
pub fn zero_colon(ws: &Workspace, lhs: &K, rhs: &K) -> KResult<K> {
    // 1. Write text lines to file: `` `:path 0: ("line1"; "line2") ``
    if matches!(lhs, K::Sym(s) if resolve(*s).starts_with(':')) {
        let K::Vector(cv) = rhs else {
            return Err(KError::ty("`:file 0: expects list of strings"));
        };
        let out: String = (0..cv.len()).map(|i| cv.get_atom(i).as_str().unwrap_or_default() + "\n").collect();
        write_file(&path_of(ws, lhs)?, out.as_bytes())?;
        return Ok(lhs.clone());
    }

    // 2. Format table as CSV lines: `"," 0: table`
    if matches!(lhs, K::Char(_) | K::Vector(ColVec::Char(_, _))) && matches!(rhs, K::Table(_) | K::KeyedTable(_)) {
        let delimiter = lhs.as_str().and_then(|d| d.chars().next()).unwrap_or(',');
        return Ok(match rhs {
            K::Table(t) => delimited_lines(delimiter, t),
            K::KeyedTable(kt) => delimited_lines(delimiter, &kt.unkey()),
            _ => unreachable!(),
        });
    }

    if let K::Vector(ColVec::Char(_, spec)) = lhs {
        return key_values(spec, &source_text(ws, rhs)?);
    }

    // 3. Parse delimited text in one pass directly into typed columns: `(types; delim) 0: source`
    let K::Vector(spec) = lhs else {
        return Err(KError::ty("0: expects (types; delimiter) on LHS"));
    };
    if spec.len() != 2 {
        return Err(KError::length("0: spec must have 2 elements (types; delimiter)"));
    }
    let types_str = spec.get_atom(0).as_str().ok_or_else(|| KError::ty("0: types must be char/string"))?;
    let delim_k = spec.get_atom(1);
    let (delim_ch, has_header) = match &delim_k {
        K::Char(b) => (*b as char, false),
        K::Vector(ColVec::Char(_, bs)) if !bs.is_empty() => (bs[0] as char, true),
        K::Vector(widths) if widths.as_i64_vec().is_some() => {
            return fixed_width(&types_str, &widths.as_i64_vec().unwrap_or_default(), &source_text(ws, rhs)?);
        }
        _ => return Err(KError::ty("0: delimiter must be char or enlisted char, or the widths of the fields")),
    };

    let text = source_text(ws, rhs)?;
    let tchars: Vec<char> = types_str.chars().collect();
    let mut builders: Vec<ColBuilder> = tchars.iter().map(|&tc| ColBuilder::with_capacity(tc, 0, has_header)).collect();
    let mut out_cols: Vec<SymId> = Vec::new();
    let mut saw_any = false;

    crate::tools::scan_delimited_rows(&text, delim_ch, |fields| {
        if !saw_any && has_header {
            saw_any = true;
            for (ci, &tc) in tchars.iter().enumerate() {
                if tc != ' ' {
                    let cname = fields.get(ci).map(|s| s.trim()).unwrap_or("");
                    let chosen = if cname.is_empty() { intern(&format!("x{ci}")) } else { intern(cname) };
                    out_cols.push(chosen);
                }
            }
            return Ok(());
        }
        saw_any = true;
        for (ci, builder) in builders.iter_mut().enumerate() {
            builder.push(fields.get(ci).unwrap_or(""));
        }
        Ok(())
    })?;

    if !saw_any {
        return Ok(K::Vector(ColVec::General(Attr::None, Arc::new(vec![]))));
    }
    let out_vecs: Vec<ColVec> = builders.into_iter().filter_map(ColBuilder::finish).collect();
    if has_header {
        Ok(K::Table(Arc::new(Flip::new(out_cols, out_vecs)?)))
    } else {
        Ok(K::Vector(ColVec::General(
            Attr::None,
            Arc::new(out_vecs.into_iter().map(K::Vector).collect()),
        )))
    }
}
