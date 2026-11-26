//! kdb+-style on-disk storage:
//! - Flat serialized `K` files (`` `:path set val `` / `` get `:path ``)
//! - Splayed tables (`` `:dir/table/ set t `` -> `.d` column order + one binary column file per column + `sym` enumeration)
//! - Partitioned HDB directories (`dir/2026.11.24/trade/` via `.Q.dpft` and `\l dir`, with predicate partition pruning!)
//! - `0:` delimited CSV parsing & formatting and `1:` binary reads
use crate::ast::Expr;
use crate::engine::Workspace;
use crate::error::{KError, KResult};
use crate::ipc::{decode_k_payload, encode_k_payload};
use crate::sym::{intern, resolve, SymId};
use crate::types::*;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const FLAT_MAGIC: [u8; 4] = [0xfe, 0x20, b'K', b'X'];

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
    if let Ok(listed) = fs::read_to_string(dir.join("par.txt")) {
        roots.extend(listed.lines().map(str::trim).filter(|l| !l.is_empty()).map(|l| resolve_fs_path(ws, l.trim_start_matches(':'))));
    }
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

fn resolve_fs_path(ws: &Workspace, rel: &str) -> PathBuf {
    let p = Path::new(rel);
    if p.is_absolute() {
        p.to_path_buf()
    } else if let Some(base) = &ws.data_dir {
        base.join(p)
    } else {
        p.to_path_buf()
    }
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

/// Whether the file holds a `q` value, as `set` writes one.
pub fn holds_value(path: &Path) -> bool {
    let mut start = [0u8; 4];
    fs::File::open(path).and_then(|mut f| f.read_exact(&mut start)).is_ok() && start == FLAT_MAGIC
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
    let payload = encode_k_payload(val);
    let mut buf = Vec::with_capacity(8 + payload.len());
    buf.extend_from_slice(&FLAT_MAGIC);
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&payload);
    fs::write(path, buf)?;
    Ok(())
}

pub fn read_flat_k(path: &Path) -> KResult<K> {
    let buf = fs::read(path)?;
    if buf.len() < 8 || buf[..4] != FLAT_MAGIC {
        return Err(KError::io(format!("invalid kixdb binary file '{}'", path.display())));
    }
    let (k, _) = decode_k_payload(&buf[8..], 0)?;
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
        write_flat_k(&dir.join(&col_name), &K::Vector(col_vec.clone()))?;
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
        let K::Vector(cv) = ck else {
            return Err(KError::io(format!("corrupt splayed column '{}'", resolve(c))));
        };
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
    let rhs_expr = match (&**lhs, &**rhs, op_s.as_str()) {
        (Expr::Var(c), r, _) if *c == part_col && no_cols(r) => r,
        (l, Expr::Var(c), "=") if *c == part_col && no_cols(l) => l,
        _ => return Ok(None),
    };
    match op_s.as_str() {
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
    let mut lines = vec![line(flip.cols.iter().map(|&c| field(resolve(c))).collect())];
    lines.extend((0..flip.nrows()).map(|r| line(flip.vals.iter().map(|c| field(c.get_atom(r).data_text())).collect())));
    K::Vector(ColVec::General(Attr::None, Arc::new(lines)))
}

/// The column that the text `fields` make for the type `tc` (`*` and `C`: strings).
fn parsed_column(tc: char, fields: &[&str]) -> ColVec {
    let kind = tc.to_ascii_lowercase();
    if tc == '*' || tc == 'C' {
        ColVec::General(Attr::None, Arc::new(fields.iter().map(|f| K::string(f)).collect()))
    } else if fields.is_empty() {
        ColVec::empty_of_type_char(kind).unwrap_or_else(|| ColVec::General(Attr::None, Arc::new(vec![])))
    } else {
        ColVec::from_atoms(fields.iter().map(|f| crate::engine::ops::parse_atom_from_str(kind, f)).collect())
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
    let mut fields: Vec<Vec<&str>> = vec![vec![]; widths.len()];
    for line in text.lines() {
        let mut rest = line;
        while !rest.is_empty() {
            for (field, &width) in fields.iter_mut().zip(widths) {
                let cut = rest.char_indices().nth(width as usize).map_or(rest.len(), |c| c.0);
                field.push(&rest[..cut]);
                rest = &rest[cut..];
            }
            if line.len() < record {
                break;
            }
        }
    }
    let columns = types.chars().zip(&fields).filter(|(tc, _)| *tc != ' ').map(|(tc, f)| K::Vector(parsed_column(tc, f)));
    Ok(K::Vector(ColVec::General(Attr::None, Arc::new(columns.collect()))))
}

/// `"S=;" 0: text`: the keys (of the first type) and the values (strings) of pairs such as `a=1;b=2`.
fn key_values(spec: &[u8], text: &str) -> KResult<K> {
    let &[kind, within, between, ..] = spec else { return Err(KError::length("0: needs a key type and two delimiters")) };
    let pairs: Vec<(&str, &str)> = text.split(between as char).filter(|p| !p.is_empty()).map(|p| p.split_once(within as char).unwrap_or((p, ""))).collect();
    let (keys, values): (Vec<&str>, Vec<&str>) = pairs.into_iter().unzip();
    let sides = [parsed_column(kind as char, &keys), parsed_column('*', &values)];
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

    // 3. Parse delimited text: `(types; delim) 0: source`
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
        K::Vector(ColVec::Char(_, bs)) if !bs.is_empty() => (bs[0] as char, true), // `enlist ","` -> 1-element Char vector!
        K::Vector(widths) if widths.as_i64_vec().is_some() => {
            return fixed_width(&types_str, &widths.as_i64_vec().unwrap_or_default(), &source_text(ws, rhs)?);
        }
        _ => return Err(KError::ty("0: delimiter must be char or enlisted char, or the widths of the fields")),
    };

    let rows = crate::tools::parse_delimited(&source_text(ws, rhs)?, delim_ch)?;
    if rows.is_empty() {
        return Ok(K::Vector(ColVec::General(Attr::None, Arc::new(vec![]))));
    }
    let start_row = if has_header { 1 } else { 0 };
    let data_rows = &rows[start_row..];

    let mut out_cols: Vec<SymId> = Vec::new();
    let mut out_vecs: Vec<ColVec> = Vec::new();

    for (ci, tc) in types_str.chars().enumerate() {
        if tc == ' ' {
            // `' '` skips column in kdb+ `0:`!
            continue;
        }
        if has_header {
            let cname = rows[0].get(ci).map(|s| s.trim()).unwrap_or("");
            let chosen = if cname.is_empty() { format!("x{ci}") } else { cname.to_string() };
            out_cols.push(intern(&chosen));
        }
        let fields: Vec<&str> = data_rows.iter().map(|r| r.get(ci).map(String::as_str).unwrap_or("")).collect();
        out_vecs.push(parsed_column(tc, &fields));
    }

    if has_header {
        Ok(K::Table(Arc::new(Flip::new(out_cols, out_vecs)?)))
    } else {
        Ok(K::Vector(ColVec::General(
            Attr::None,
            Arc::new(out_vecs.into_iter().map(K::Vector).collect()),
        )))
    }
}
