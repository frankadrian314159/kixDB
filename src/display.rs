//! How values print: as q's console shows them (`console`), with the numbers and lines that `\P` and `\c` shape.
use crate::engine::ops::index_k;
use crate::engine::ResultSet;
use crate::sym::resolve;
use crate::types::*;
use std::cell::Cell;
use std::sync::Arc;

/// `\P` (significant digits of floats, 0 for all) and `\c` (lines and characters of the console).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Settings {
    pub precision: u8,
    pub rows: u16,
    pub cols: u16,
}

impl Default for Settings {
    fn default() -> Self {
        Settings { precision: 7, rows: 25, cols: 80 }
    }
}

thread_local! {
    static SETTINGS: Cell<Settings> = Cell::new(Settings::default());
}

/// Print as `settings` say, on this thread.
pub fn apply(settings: Settings) {
    SETTINGS.set(settings);
}

/// A float to `\P` significant digits, as C's `%g` writes it, with q's names for nulls and infinities.
pub fn float(v: f64) -> String {
    if !v.is_finite() {
        return if v.is_nan() { "0n" } else if v > 0.0 { "0w" } else { "-0w" }.into();
    }
    if v == 0.0 {
        return "0".into();
    }
    let digits = match SETTINGS.get().precision {
        0 => 17,
        p => usize::from(p),
    };
    let trim = |s: &str| if s.contains('.') { s.trim_end_matches('0').trim_end_matches('.').to_string() } else { s.to_string() };
    let scientific = format!("{v:.*e}", digits - 1);
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    match exponent < -4 || exponent >= digits as i32 {
        true => format!("{}e{}{:02}", trim(mantissa), if exponent < 0 { '-' } else { '+' }, exponent.abs()),
        false => trim(&format!("{v:.*}", (digits as i32 - 1 - exponent) as usize)),
    }
}

/// A value as a cell of a table or the side of a dictionary: without backticks and type letters, nulls left blank.
pub fn cell(k: &K) -> String {
    match k {
        K::Vector(_) | K::Dict(_) | K::Table(_) | K::KeyedTable(_) | K::Func(_) => k.q_repr(),
        atom => atom.cell_text(),
    }
}

/// The cell of a column's row; a column of mixed values shows each as `-3!` does.
pub fn column_cell(column: &ColVec, row: usize) -> String {
    match column {
        ColVec::General(_, items) => items[row].q_repr(),
        ColVec::Strings(..) => column.get_atom(row).q_repr(),
        typed => cell(&typed.get_atom(row)),
    }
}

/// Rows of cells with each column as wide as its widest cell.
fn aligned(rows: &[Vec<String>]) -> Vec<String> {
    let width = |c: usize| rows.iter().map(|r| r.get(c).map_or(0, |s| s.chars().count())).max().unwrap_or(0);
    let widths: Vec<usize> = (0..rows.first().map_or(0, Vec::len)).map(width).collect();
    let line = |row: &Vec<String>| row.iter().zip(&widths).map(|(c, &w)| format!("{c:<w$}")).collect::<Vec<_>>().join(" ").trim_end().to_string();
    rows.iter().map(line).collect()
}

fn item(k: &K, i: usize) -> K {
    index_k(k, &[Some(K::Long(i as i64))]).unwrap_or(K::Null)
}

fn cells(v: &ColVec) -> Vec<String> {
    (0..v.len()).map(|i| cell(&v.get_atom(i))).collect()
}

/// The lines of a dictionary, at most `most` of its entries.
fn dict(d: &Dict, most: usize) -> Vec<String> {
    let n = d.keys.count().min(most);
    let key = |i| match item(&d.keys, i) {
        K::Vector(ColVec::Sym(_, names)) => names.iter().map(|&s| resolve(s)).collect::<Vec<_>>().join(" "),
        other => cell(&other),
    };
    let keys: Vec<String> = (0..n).map(key).collect();
    let values: Vec<K> = (0..n).map(|i| item(&d.vals, i)).collect();
    // lists that are all as long as each other line up in columns, as do the values of a simple list
    let length = |v: &K| match v {
        K::Vector(v) if !matches!(v, ColVec::Char(..)) && !v.is_list() && v.len() > 1 => Some(v.len()),
        _ => None,
    };
    let columns = values.first().and_then(length).filter(|&n| values.iter().all(|v| length(v) == Some(n)));
    let shown: Vec<String> = match (&*d.vals, columns) {
        (_, Some(_)) => aligned(&values.iter().filter_map(|v| if let K::Vector(v) = v { Some(cells(v)) } else { None }).collect::<Vec<_>>()),
        (K::Vector(v), None) if v.is_list() => values.iter().map(K::q_repr).collect(),
        _ => values.iter().map(cell).collect(),
    };
    let width = keys.iter().map(|k| k.chars().count()).max().unwrap_or(0);
    keys.iter().zip(shown).map(|(k, v)| format!("{k:<width$}| {v}")).collect()
}

/// The lines of a list of mixed values: one an item, a matrix of floats in columns.
fn list(items: &[K]) -> Vec<String> {
    let row = |k: &K| match k {
        K::Vector(v @ ColVec::Float(..)) if !v.is_empty() => Some(cells(v)),
        _ => None,
    };
    match items.iter().map(row).collect::<Option<Vec<_>>>() {
        Some(rows) if rows.iter().all(|r| r.len() == rows[0].len()) => aligned(&rows),
        _ => items.iter().map(K::q_repr).collect(),
    }
}

/// What the console shows for `k`: nothing for `::`, and no more lines or characters than `\c` allows.
pub fn console(k: &K) -> String {
    let s = SETTINGS.get();
    let (most, wide) = (usize::from(s.rows.max(8)) - 2, usize::from(s.cols.max(10)));
    // (only what can be shown is formatted)
    let lines: Vec<String> = match k {
        K::Null => return String::new(),
        K::Vector(ColVec::General(_, items)) => list(&items[..items.len().min(most + 1)]),
        K::Vector(v @ ColVec::Strings(..)) => list(&(0..v.len().min(most + 1)).map(|i| v.get_atom(i)).collect::<Vec<_>>()),
        K::Vector(v) if v.len() > wide => vec![K::Vector(v.take(wide as i64)).q_repr()],
        K::Dict(d) => dict(d, most + 1),
        K::Table(t) => crate::tools::format_flip(t, most - 3).split('\n').map(String::from).collect(),
        K::KeyedTable(t) => crate::tools::format_keyed_table(t, most - 3).split('\n').map(String::from).collect(),
        other => other.q_repr().split('\n').map(String::from).collect(),
    };
    let clip = |line: &String| match line.chars().count() < wide {
        true => line.clone(),
        false => format!("{}..", line.chars().take(wide - 3).collect::<String>().trim_end()),
    };
    let cut = lines.len() > most;
    let mut shown: Vec<String> = lines.iter().take(if cut { most - 1 } else { most }).map(clip).collect();
    if cut {
        shown.push("..".into());
    }
    shown.join("\n")
}

/// The console's view of a result given as rows.
pub fn grid(rs: &ResultSet) -> String {
    let column = |c: usize| ColVec::from_atoms(rs.rows.iter().map(|r| r[c].clone()).collect());
    let table = Flip { cols: rs.columns.iter().map(|n| crate::sym::intern(n)).collect(), vals: (0..rs.columns.len()).map(column).collect() };
    match rs.key_count {
        0 => console(&K::Table(Arc::new(table))),
        n => console(&KeyedTable::from_flip_nkeys(&table, n).unwrap_or_else(|_| K::Table(Arc::new(table)))),
    }
}
