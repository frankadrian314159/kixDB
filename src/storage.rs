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
use std::collections::HashSet;
use std::fs;
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
    pub slices: Vec<PartitionSlice>,
    pub cols: Vec<SymId>,
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

pub fn write_flat_k(path: &Path, val: &K) -> KResult<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
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

/// Save a `K` value to `:path` — if `rel` ends with `/` (or `val` is a `Table` and path is a directory), writes a **splayed** table!
pub fn save_path(ws: &Workspace, rel: &str, val: &K) -> KResult<()> {
    let is_splay = rel.ends_with('/') || rel.ends_with('\\');
    let path = resolve_fs_path(ws, rel.trim_end_matches(['/', '\\']));
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
    let path = resolve_fs_path(ws, rel.trim_end_matches(['/', '\\']));
    if path.is_dir() && path.join(".d").exists() {
        let flip = read_splayed_table(&path, None)?;
        Ok(K::Table(Arc::new(flip)))
    } else {
        read_flat_k(&path)
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

/// `.Q.en[`:dir; table]` — enumerates symbol columns against `dir/sym` and persists updated `sym` domain.
pub fn q_en(dir_k: &K, tbl_k: &K) -> KResult<K> {
    let K::Sym(dir_s) = dir_k else {
        return Err(KError::ty(".Q.en first argument must be `:dir"));
    };
    let raw = resolve(*dir_s);
    let dir_path = Path::new(raw.strip_prefix(':').unwrap_or(&raw));
    fs::create_dir_all(dir_path)?;
    let sym_file = dir_path.join("sym");

    let mut domain: Vec<SymId> = if sym_file.exists() {
        if let Ok(K::Vector(ColVec::Sym(_, existing))) = read_flat_k(&sym_file) {
            (*existing).clone()
        } else {
            vec![]
        }
    } else {
        vec![]
    };
    let mut seen: HashSet<SymId> = domain.iter().copied().collect();

    let flip = match tbl_k {
        K::Table(t) => (**t).clone(),
        K::KeyedTable(kt) => kt.unkey(),
        _ => return Err(KError::ty(".Q.en second argument must be a table")),
    };
    for cv in &flip.vals {
        if let ColVec::Sym(_, xs) = cv {
            for &s in xs.iter() {
                if seen.insert(s) {
                    domain.push(s);
                }
            }
        }
    }
    write_flat_k(&sym_file, &K::Vector(ColVec::Sym(Attr::Unique, Arc::new(domain))))?;
    Ok(tbl_k.clone())
}

/// `.Q.dpft[`:dir; partition; `field; `tablename]` — sorts `tablename` by `field`, applies `` `p# ``, splays to `dir/partition/tablename/`, and refreshes HDB mount!
pub fn q_dpft(ws: &mut Workspace, dir_k: &K, part_k: &K, field_k: &K, tbl_name_k: &K) -> KResult<K> {
    let K::Sym(dir_s) = dir_k else {
        return Err(KError::ty(".Q.dpft[dir; part; field; table] expects `:dir"));
    };
    let K::Sym(field_s) = field_k else {
        return Err(KError::ty(".Q.dpft field must be a symbol"));
    };
    let K::Sym(tbl_s) = tbl_name_k else {
        return Err(KError::ty(".Q.dpft table must be a symbol (`table)"));
    };
    let tbl_val = ws.vars.get(tbl_s).cloned().ok_or_else(|| KError::value(resolve(*tbl_s)))?;
    let sorted = crate::engine::ops::table_sort_op(&K::Sym(*field_s), &tbl_val, false)?;
    let K::Table(sorted_flip) = sorted else {
        return Err(KError::ty(".Q.dpft requires an unkeyed table"));
    };
    let mut cols = vec![*field_s];
    for &c in &sorted_flip.cols {
        if c != *field_s {
            cols.push(c);
        }
    }
    let mut vals = Vec::with_capacity(cols.len());
    for (i, &c) in cols.iter().enumerate() {
        let mut cv = sorted_flip.col(c).cloned().ok_or_else(|| KError::value(resolve(c)))?;
        if i == 0 {
            cv = cv.with_attr(Attr::Parted)?;
        }
        vals.push(cv);
    }
    let final_flip = Flip::new(cols, vals)?;
    let raw_dir = resolve(*dir_s);
    let base = resolve_fs_path(ws, raw_dir.strip_prefix(':').unwrap_or(&raw_dir));
    let _ = q_en(&K::Sym(intern(&format!(":{}", base.display()))), &K::Table(Arc::new(final_flip.clone())))?;

    let part_str = match part_k {
        K::Date(d) => format_date(*d),
        K::Month(m) => format_month(*m).trim_end_matches('m').to_string(),
        other => other.as_i64()?.to_string(),
    };
    let target_dir = base.join(&part_str).join(resolve(*tbl_s));
    write_splayed_table(&target_dir, &final_flip)?;
    mount_hdb_dir(ws, &base)?;
    Ok(K::Sym(*tbl_s))
}

/// Mount an HDB directory (`\l dir`): loads flat files, splayed tables, and partitioned (`YYYY.MM.DD/...`) tables!
pub fn mount_hdb_dir(ws: &mut Workspace, dir: &Path) -> KResult<()> {
    if !dir.exists() {
        return Ok(());
    }
    let mut part_entries: Vec<(PartKind, SymId, K, PathBuf)> = Vec::new();

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let fname = entry.file_name().to_string_lossy().into_owned();
        if fname.starts_with('.') || fname == "sym" || fname.starts_with("kix.") {
            continue;
        }
        if path.is_dir() {
            if let Some((kind, pcol, pval)) = parse_partition_dir_name(&fname) {
                part_entries.push((kind, pcol, pval, path));
            } else if path.join(".d").exists() {
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

    // Group partition slices by table name and sort chronologically
    part_entries.sort_by(|a, b| a.2.k_cmp(&b.2));
    let mut by_table: std::collections::HashMap<SymId, PartitionedTable> = std::collections::HashMap::new();
    for (kind, pcol, pval, pdir) in part_entries {
        for sub in fs::read_dir(&pdir)? {
            let sub = sub?;
            let spath = sub.path();
            if spath.is_dir() && spath.join(".d").exists() {
                let tname = intern(&sub.file_name().to_string_lossy());
                let d_k = read_flat_k(&spath.join(".d"))?;
                let K::Vector(ColVec::Sym(_, s_cols)) = d_k else { continue };
                let pt = by_table.entry(tname).or_insert_with(|| {
                    let mut cols = vec![pcol];
                    cols.extend(s_cols.iter().copied());
                    PartitionedTable {
                        name: tname,
                        part_col: pcol,
                        kind: kind.clone(),
                        slices: Vec::new(),
                        cols,
                    }
                });
                pt.slices.push(PartitionSlice { part_val: pval.clone(), path: spath });
            }
        }
    }
    for (tname, pt) in by_table {
        ws.partitioned.insert(tname, pt);
    }
    Ok(())
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
    let needed: Option<Vec<SymId>> = needed.map(|n| {
        let mut on_disk: Vec<SymId> = pt.cols[1..].iter().copied().filter(|c| n.contains(c)).collect();
        if on_disk.is_empty() {
            on_disk.extend(pt.cols.get(1));
        }
        on_disk
    });
    let needed = needed.as_deref();
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
        if let Some(sample_slice) = pt.slices.first() {
            let sf = read_splayed_table(&sample_slice.path, needed)?;
            let mut cols = vec![pt.part_col];
            cols.extend(&sf.cols);
            vals.extend(sf.vals.iter().map(|v| v.take(0)));
            return Flip::new(cols, vals);
        }
        return Flip::new(vec![pt.part_col], vals);
    }

    let mut acc: Option<Flip> = None;
    for slice in selected_slices {
        let sf = read_splayed_table(&slice.path, needed)?;
        let nrows = sf.nrows();
        let mut pcol = ColVec::from_atoms(vec![slice.part_val.clone(); nrows]);
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

pub fn hcount(x: &K) -> KResult<K> {
    let K::Sym(s) = x else {
        return Err(KError::ty("hcount expects `:file symbol"));
    };
    let raw = resolve(*s);
    let path = Path::new(raw.strip_prefix(':').unwrap_or(&raw));
    let meta = fs::metadata(path)?;
    Ok(K::Long(meta.len() as i64))
}

pub fn one_colon(lhs: &K, rhs: &K) -> KResult<K> {
    // `` `:file 1: bytes `` writes raw bytes; `read1` / `` 1: `:file `` reads raw bytes
    if let K::Sym(s) = lhs {
        let raw = resolve(*s);
        let path = Path::new(raw.strip_prefix(':').unwrap_or(&raw));
        if let K::Vector(ColVec::Byte(_, bs)) | K::Vector(ColVec::Char(_, bs)) = rhs {
            fs::write(path, bs.as_slice())?;
            return Ok(lhs.clone());
        }
    }
    Err(KError::ty("1: expects `:file and byte/char vector"))
}

/// kdb+'s `0:` CSV / text operator:
/// - `"," 0: table` -> list of CSV lines (header + rows)
/// - `` `:out.csv 0: lines `` -> writes lines to file
/// - `("SFI"; enlist ",") 0: `:file.csv` (or string/lines) -> parses CSV with header into `Table`!
/// - `("SFI"; ",") 0: lines` -> parses CSV without header into list of `ColVec`s!
pub fn zero_colon(lhs: &K, rhs: &K) -> KResult<K> {
    // 1. Write text lines to file: `` `:path 0: ("line1"; "line2") ``
    if let K::Sym(s) = lhs {
        let raw = resolve(*s);
        if let Some(p) = raw.strip_prefix(':') {
            let K::Vector(cv) = rhs else {
                return Err(KError::ty("`:file 0: expects list of strings"));
            };
            let mut out = String::new();
            for i in 0..cv.len() {
                out.push_str(&cv.get_atom(i).as_str().unwrap_or_default());
                out.push('\n');
            }
            fs::write(p, out)?;
            return Ok(lhs.clone());
        }
    }

    // 2. Format table as CSV lines: `"," 0: table`
    if matches!(lhs, K::Char(_) | K::Vector(ColVec::Char(_, _))) && matches!(rhs, K::Table(_) | K::KeyedTable(_)) {
        let delim = lhs.as_str().unwrap_or_else(|| ",".into());
        let dch = delim.chars().next().unwrap_or(',');
        let flip = match rhs {
            K::Table(t) => (**t).clone(),
            K::KeyedTable(kt) => kt.unkey(),
            _ => unreachable!(),
        };
        let mut lines = Vec::with_capacity(flip.nrows() + 1);
        let hdr: Vec<String> = flip.cols.iter().map(|&c| resolve(c)).collect();
        lines.push(K::string(&hdr.join(&dch.to_string())));
        for r in 0..flip.nrows() {
            let cells: Vec<String> = flip.vals.iter().map(|c| c.get_atom(r).cell_text()).collect();
            lines.push(K::string(&cells.join(&dch.to_string())));
        }
        return Ok(K::Vector(ColVec::General(Attr::None, Arc::new(lines))));
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
        K::Vector(ColVec::Char(_, bs)) if bs.len() == 1 => (bs[0] as char, true), // `enlist ","` -> 1-element Char vector!
        K::Vector(ColVec::Char(_, bs)) if !bs.is_empty() => (bs[0] as char, true),
        _ => return Err(KError::ty("0: delimiter must be char or enlisted char")),
    };

    let raw_text = match rhs {
        K::Sym(s) => {
            let r = resolve(*s);
            let path = r.strip_prefix(':').unwrap_or(&r);
            fs::read_to_string(path)?
        }
        K::Vector(ColVec::Char(_, bs)) => String::from_utf8_lossy(bs).into_owned(),
        K::Vector(cv) => {
            let mut joined = String::new();
            for i in 0..cv.len() {
                if i > 0 {
                    joined.push('\n');
                }
                joined.push_str(&cv.get_atom(i).as_str().unwrap_or_default());
            }
            joined
        }
        _ => return Err(KError::ty("0: RHS must be `:file, string, or list of strings")),
    };

    let rows = crate::tools::parse_delimited(&raw_text, delim_ch)?;
    if rows.is_empty() {
        return Ok(K::Vector(ColVec::General(Attr::None, Arc::new(vec![]))));
    }
    let type_chars: Vec<char> = types_str.chars().collect();
    let start_row = if has_header { 1 } else { 0 };
    let data_rows = &rows[start_row..];

    let mut out_cols: Vec<SymId> = Vec::new();
    let mut out_vecs: Vec<ColVec> = Vec::new();

    for (ci, &tc) in type_chars.iter().enumerate() {
        if tc == ' ' {
            // `' '` skips column in kdb+ `0:`!
            continue;
        }
        if has_header {
            let cname = rows[0].get(ci).map(|s| s.trim()).unwrap_or("");
            let chosen = if cname.is_empty() { format!("x{ci}") } else { cname.to_string() };
            out_cols.push(intern(&chosen));
        }
        let tl = tc.to_ascii_lowercase();
        if tc == '*' || tc == 'C' {
            let strs: Vec<K> = data_rows
                .iter()
                .map(|r| K::string(r.get(ci).map(String::as_str).unwrap_or("")))
                .collect();
            out_vecs.push(ColVec::General(Attr::None, Arc::new(strs)));
        } else if data_rows.is_empty() {
            out_vecs.push(ColVec::empty_of_type_char(tl).unwrap_or_else(|| ColVec::General(Attr::None, Arc::new(vec![]))));
        } else {
            let atoms: Vec<K> = data_rows
                .iter()
                .map(|r| crate::engine::ops::parse_atom_from_str(tl, r.get(ci).map(String::as_str).unwrap_or("")))
                .collect();
            out_vecs.push(ColVec::from_atoms(atoms));
        }
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
