//! What changed between two values of a variable, so that the log and the replicas need only that: the columns of a
//! table (or a vector) that changed, of those that only grew only the new items, and of rows only removed their numbers.
use crate::sym::SymId;
use crate::types::{Attr, ColVec, K};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ColChange {
    /// Items added to the end of a column that held `at` items.
    Append { at: u64, tail: ColVec },
    Replace(ColVec),
    /// The items at some rows set, in a column that keeps its length; `values` carries the column's attribute afterwards.
    Set { rows: Vec<u64>, values: ColVec },
}

/// A column's position (keyed tables count their key columns first) and what became of it.
pub type Changes = Vec<(u32, ColChange)>;

type Columns<'a> = (Vec<&'a ColVec>, [&'a [SymId]; 2]);

/// Rows removed from a vector or table whose other rows stayed as they were.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Removed {
    /// The rows there were.
    pub at: u64,
    /// The numbers of those removed, rising.
    pub rows: Vec<u64>,
    /// The attributes the columns are left with.
    pub attrs: Vec<Attr>,
}

/// The columns of a vector (just itself), a dictionary of lists (keys and values) or a table, with the column names.
fn columns(v: &K) -> Option<Columns<'_>> {
    match v {
        K::Vector(c) => Some((vec![c], [&[], &[]])),
        K::Dict(d) => match (&*d.keys, &*d.vals) {
            (K::Vector(keys), K::Vector(vals)) => Some((vec![keys, vals], [&[], &[]])),
            _ => None,
        },
        K::Table(f) => Some((f.vals.iter().collect(), [&f.cols, &[]])),
        K::KeyedTable(t) => Some((t.keys.vals.iter().chain(&t.vals.vals).collect(), [&t.keys.cols, &t.vals.cols])),
        _ => None,
    }
}

/// Whether a value is what it was, so that nothing need be written.
pub(crate) fn unchanged(old: &K, new: &K) -> bool {
    // (functions, which `identical` is in doubt about, are compared as they would be written)
    identical(old, new) || postcard::to_allocvec(old).ok() == postcard::to_allocvec(new).ok()
}

/// Whether two values are the same in every bit, attributes included (`false` when in doubt).
fn identical(a: &K, b: &K) -> bool {
    match (a, b) {
        (K::Dict(x), K::Dict(y)) => identical(&x.keys, &y.keys) && identical(&x.vals, &y.vals),
        (K::Func(_), _) => false,
        _ => match (columns(a), columns(b)) {
            (Some((x, m)), Some((y, n))) => std::mem::discriminant(a) == std::mem::discriminant(b) && m == n && x.iter().zip(y).all(|(x, y)| x.len() == y.len() && x.attr() == y.attr() && begins(x, y)),
            _ => a == b,
        },
    }
}

/// Whether the columns are of one type, enumerations being of one domain too. (Strings, packed or not, are lists.)
fn alike(a: &ColVec, b: &ColVec) -> bool {
    match (a, b) {
        (ColVec::Enum(t, x), ColVec::Enum(u, y)) => t == u && alike(x, y),
        (ColVec::Strings(..), other) => other.packed().is_some(),
        (ColVec::General(..), ColVec::Strings(..)) => true,
        _ => std::mem::discriminant(a) == std::mem::discriminant(b),
    }
}

/// Whether `b` starts with exactly the items of `a`, in a column of the same type.
fn begins(a: &ColVec, b: &ColVec) -> bool {
    macro_rules! on {
        ($($same:ident)*; $($float:ident)*) => {
            match (a, b) {
                $((ColVec::$same(_, x), ColVec::$same(_, y)) => Arc::ptr_eq(x, y) || y.starts_with(x),)*
                $((ColVec::$float(_, x), ColVec::$float(_, y)) => x.len() <= y.len() && x.iter().zip(y.iter()).all(|(x, y)| x.to_bits() == y.to_bits()),)*
                (ColVec::General(_, x), ColVec::General(_, y)) => x.len() <= y.len() && (Arc::ptr_eq(x, y) || x.iter().zip(y.iter()).all(|(x, y)| identical(x, y))),
                (ColVec::Enum(t, x), ColVec::Enum(u, y)) => t == u && begins(x, y),
                (ColVec::Strings(_, x), ColVec::Strings(_, y)) => x.len() <= y.len() && (Arc::ptr_eq(x, y) || x.iter().zip(y.iter()).all(|(x, y)| x == y)),
                _ => false,
            }
        };
    }
    on!(Bool Guid Byte Short Int Long Char Sym Timestamp Month Date Timespan Minute Second Time; Real Float Datetime)
}

/// Whether row `i` of `a` and row `j` of `b`, a column of the same type, hold the same bits.
fn same_item(a: &ColVec, i: usize, b: &ColVec, j: usize) -> bool {
    macro_rules! on {
        ($($same:ident)*; $($float:ident)*) => {
            match (a, b) {
                $((ColVec::$same(_, x), ColVec::$same(_, y)) => x[i] == y[j],)*
                $((ColVec::$float(_, x), ColVec::$float(_, y)) => x[i].to_bits() == y[j].to_bits(),)*
                (ColVec::General(_, x), ColVec::General(_, y)) => identical(&x[i], &y[j]),
                (ColVec::Enum(t, x), ColVec::Enum(u, y)) => t == u && same_item(x, i, y, j),
                (ColVec::Strings(_, x), ColVec::Strings(_, y)) => x.get(i) == y.get(j),
                _ => false,
            }
        };
    }
    on!(Bool Guid Byte Short Int Long Char Sym Timestamp Month Date Timespan Minute Second Time; Real Float Datetime)
}

/// The rows whose removal turns `old` into `new`, when that is how they differ and fewer rows go than stay.
pub fn removed(old: &K, new: &K) -> Option<Removed> {
    let ((a, a_names), (b, b_names)) = (columns(old)?, columns(new)?);
    let (n, m) = (a.first()?.len(), b.first()?.len());
    if std::mem::discriminant(old) != std::mem::discriminant(new) || a_names != b_names || n <= m || n - m >= m {
        return None;
    }
    // each row of `old` is either the next row of `new` or one that was removed
    let (mut rows, mut kept) = (vec![], 0);
    for i in 0..n {
        if kept < m && a.iter().zip(&b).all(|(a, b)| same_item(a, i, b, kept)) {
            kept += 1;
        } else if rows.len() < n - m {
            rows.push(i as u64);
        } else {
            return None;
        }
    }
    (kept == m).then(|| Removed { at: n as u64, rows, attrs: b.iter().map(|c| c.attr()).collect() })
}

/// The rows of `a` that differ in `b`, which is no shorter and of its type, if there are at most `limit` of them.
fn differing(a: &ColVec, b: &ColVec, limit: usize) -> Option<Vec<u64>> {
    if !alike(a, b) || b.len() < a.len() {
        return None;
    }
    let mut rows = vec![];
    for i in 0..a.len() {
        if !same_item(a, i, b, i) {
            if rows.len() == limit {
                return None;
            }
            rows.push(i as u64);
        }
    }
    Some(rows)
}

/// The changes that turn `old` into `new`, or `None` when `new` is as well written whole: it is not a vector or table
/// of the same columns, or no column kept anything.
pub fn diff(old: &K, new: &K) -> Option<Changes> {
    let ((a, a_names), (b, b_names)) = (columns(old)?, columns(new)?);
    if std::mem::discriminant(old) != std::mem::discriminant(new) || a_names != b_names {
        return None;
    }
    let (mut changes, mut kept_any) = (vec![], false);
    for (i, (a, b)) in a.into_iter().zip(b).enumerate() {
        // the rows already there that are to be set, when they are few; `None`: the column goes whole
        let set = if begins(a, b) { Some(vec![]) } else { differing(a, b, a.len() / 8) };
        let Some(rows) = set else {
            changes.push((i as u32, ColChange::Replace(b.clone())));
            continue;
        };
        kept_any = true;
        let (grew, few) = (b.len() > a.len(), !rows.is_empty());
        if few || (!grew && a.attr() != b.attr()) {
            let mut values = b.gather(&rows.iter().map(|&r| r as usize).collect::<Vec<_>>());
            values.set_attr_unchecked(b.attr());
            changes.push((i as u32, ColChange::Set { rows, values }));
        }
        if grew {
            let mut tail = b.drop_n(a.len() as i64);
            tail.set_attr_unchecked(b.attr());
            changes.push((i as u32, ColChange::Append { at: a.len() as u64, tail }));
        }
    }
    kept_any.then_some(changes)
}

/// Add `tail` to a column of its type that holds `at` items.
fn extend(col: &mut ColVec, at: u64, tail: &ColVec) -> bool {
    if let ColVec::Enum(t, x) = col {
        return matches!(tail, ColVec::Enum(u, y) if t == u && extend(x, at, y));
    }
    match (&mut *col, tail) {
        // strings stay packed when what is added is strings, and otherwise become a list
        (ColVec::Strings(attr, x), _) if x.len() as u64 == at => {
            return match tail.packed().as_deref() {
                Some(ColVec::Strings(new, y)) => {
                    Arc::make_mut(x).extend_from(y);
                    *attr = *new;
                    true
                }
                _ => {
                    *col = col.listed().into_owned();
                    extend(col, at, tail)
                }
            };
        }
        (ColVec::General(..), ColVec::Strings(..)) => return extend(col, at, &tail.listed()),
        _ => {}
    }
    macro_rules! on {
        ($($v:ident)*) => {
            match (col, tail) {
                $((ColVec::$v(attr, x), ColVec::$v(new, y)) if x.len() as u64 == at => {
                    // (an empty column takes the tail itself, unless that holds much room to spare)
                    if x.is_empty() && y.capacity() <= y.len() + y.len() / 8 {
                        *x = y.clone();
                    } else {
                        Arc::make_mut(x).extend_from_slice(y);
                    }
                    *attr = *new;
                    true
                })*
                _ => false,
            }
        };
    }
    on!(General Bool Guid Byte Short Int Long Real Float Char Sym Timestamp Month Date Datetime Timespan Minute Second Time)
}

/// The columns of a vector, dictionary of lists or table, to change.
fn columns_mut(value: &mut K) -> Option<Vec<&mut ColVec>> {
    Some(match value {
        K::Vector(c) => vec![c],
        K::Dict(d) => {
            let d = Arc::make_mut(d);
            match (&mut *d.keys, &mut *d.vals) {
                (K::Vector(keys), K::Vector(vals)) => vec![keys, vals],
                _ => return None,
            }
        }
        K::Table(f) => Arc::make_mut(f).vals.iter_mut().collect(),
        K::KeyedTable(t) => {
            let t = Arc::make_mut(t);
            t.keys.vals.iter_mut().chain(&mut t.vals.vals).collect()
        }
        _ => return None,
    })
}

/// Remove rows from `value`; `false` if they were not found in a value like it.
pub fn remove(value: &mut K, removed: &Removed) -> bool {
    let Some(mut cols) = columns_mut(value) else { return false };
    if cols.len() != removed.attrs.len() || cols.iter().any(|c| c.len() as u64 != removed.at) {
        return false;
    }
    for (col, &attr) in cols.iter_mut().zip(&removed.attrs) {
        if let ColVec::Strings(..) = col {
            let kept: Vec<usize> = (0..removed.at).filter(|r| removed.rows.binary_search(r).is_err()).map(|r| r as usize).collect();
            **col = col.gather(&kept);
            col.set_attr_unchecked(attr);
            continue;
        }
        macro_rules! on {
            ($($v:ident)*) => {
                match match &mut **col {
                    ColVec::Enum(_, values) => &mut **values,
                    other => other,
                } {
                    $(ColVec::$v(_, x) => {
                        let (mut row, mut gone) = (0, removed.rows.iter().peekable());
                        Arc::make_mut(x).retain(|_| {
                            row += 1;
                            gone.next_if_eq(&&(row - 1)).is_none()
                        });
                    })*
                    ColVec::Enum(..) | ColVec::Strings(..) => return false,
                }
            };
        }
        on!(General Bool Guid Byte Short Int Long Real Float Char Sym Timestamp Month Date Datetime Timespan Minute Second Time);
        col.set_attr_unchecked(attr);
    }
    true
}

/// Set rows of a column of the type of `values`.
pub(crate) fn set(col: &mut ColVec, rows: &[u64], values: &ColVec) -> bool {
    if rows.len() != values.len() || rows.iter().any(|&r| r >= col.len() as u64) {
        return false;
    }
    if let ColVec::Enum(t, x) = col {
        return matches!(values, ColVec::Enum(u, y) if t == u && set(x, rows, y));
    }
    // strings set among packed ones: the text is put together anew, each row's from the last value set in it
    if let (ColVec::Strings(attr, x), Some(ColVec::Strings(new, y))) = (&mut *col, values.packed().as_deref()) {
        let mut set: Vec<Option<&[u8]>> = vec![None; x.len()];
        rows.iter().zip(y.iter()).for_each(|(&r, v)| set[r as usize] = Some(v));
        *x = Arc::new(set.into_iter().enumerate().map(|(i, v)| v.unwrap_or_else(|| x.get(i))).collect());
        *attr = *new;
        return true;
    }
    if let ColVec::Strings(..) = values {
        return set(col, rows, &values.listed());
    }
    // other values are set in the list that packed strings stand for, which is packed again if only strings remain
    if let ColVec::Strings(..) = col {
        *col = col.listed().into_owned();
        let done = set(col, rows, values);
        col.pack();
        return done;
    }
    macro_rules! on {
        ($($v:ident)*) => {
            match (col, values) {
                $((ColVec::$v(attr, x), ColVec::$v(new, y)) => {
                    if !rows.is_empty() {
                        let x = Arc::make_mut(x);
                        rows.iter().zip(y.iter()).for_each(|(&r, v)| x[r as usize] = v.clone());
                    }
                    *attr = *new;
                    true
                })*
                _ => false,
            }
        };
    }
    on!(General Bool Guid Byte Short Int Long Real Float Char Sym Timestamp Month Date Datetime Timespan Minute Second Time)
}

/// Whether `changes` were made from a value like `value`: `patch` will then make all of them.
pub fn fits(value: &K, changes: &Changes) -> bool {
    let Some((cols, _)) = columns(value) else { return false };
    let mut lengths: Vec<u64> = cols.iter().map(|c| c.len() as u64).collect();
    changes.iter().all(|(i, change)| {
        let Some((col, length)) = cols.get(*i as usize).zip(lengths.get_mut(*i as usize)) else { return false };
        let like = |other: &ColVec| alike(col, other);
        match change {
            ColChange::Replace(new) => {
                *length = new.len() as u64;
                true
            }
            ColChange::Append { at, tail } => {
                let fits = like(tail) && *length == *at;
                *length += tail.len() as u64;
                fits
            }
            ColChange::Set { rows, values } => like(values) && rows.len() == values.len() && rows.iter().all(|&r| r < *length),
        }
    })
}

/// Make `changes` to `value`; `false` if they were not made from a value like it.
pub fn patch(value: &mut K, changes: &Changes) -> bool {
    // the index of a keyed table's keys goes with them where rows are only added to them
    let taken = match value {
        K::KeyedTable(t) => {
            let mut keys = changes.iter().filter(|c| (c.0 as usize) < t.keys.ncols()).peekable();
            (keys.peek().is_some() && keys.all(|c| matches!(c.1, ColChange::Append { .. }))).then(|| crate::index::take_keys(&t.keys.vals)).flatten()
        }
        _ => None,
    };
    let Some(mut cols) = columns_mut(value) else { return false };
    let made = changes.iter().all(|(i, change)| match (cols.get_mut(*i as usize), change) {
        (Some(col), ColChange::Append { at, tail }) => extend(col, *at, tail),
        (Some(col), ColChange::Set { rows, values }) => set(col, rows, values),
        (Some(col), ColChange::Replace(new)) => {
            **col = new.clone();
            true
        }
        (None, _) => false,
    });
    value.pack();
    if let (true, Some(taken), K::KeyedTable(t)) = (made, taken, &*value) {
        crate::index::keep_keys(&t.keys.vals, taken);
    }
    made
}
