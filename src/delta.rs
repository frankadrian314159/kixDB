//! What changed between two values of a variable, so that the log and the replicas need only that: the columns of a
//! table (or a vector) that changed, and of those that only grew, only the new items.
use crate::sym::SymId;
use crate::types::{ColVec, K};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ColChange {
    /// Items added to the end of a column that held `at` items.
    Append { at: u64, tail: ColVec },
    Replace(ColVec),
}

/// A column's position (keyed tables count their key columns first) and what became of it.
pub type Changes = Vec<(u32, ColChange)>;

type Columns<'a> = (Vec<&'a ColVec>, [&'a [SymId]; 2]);

/// The columns of a vector (just itself) or a table, with the column names.
fn columns(v: &K) -> Option<Columns<'_>> {
    match v {
        K::Vector(c) => Some((vec![c], [&[], &[]])),
        K::Table(f) => Some((f.vals.iter().collect(), [&f.cols, &[]])),
        K::KeyedTable(t) => Some((t.keys.vals.iter().chain(&t.vals.vals).collect(), [&t.keys.cols, &t.vals.cols])),
        _ => None,
    }
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

/// Whether `b` starts with exactly the items of `a`, in a column of the same type.
fn begins(a: &ColVec, b: &ColVec) -> bool {
    macro_rules! on {
        ($($same:ident)*; $($float:ident)*) => {
            match (a, b) {
                $((ColVec::$same(_, x), ColVec::$same(_, y)) => Arc::ptr_eq(x, y) || y.starts_with(x),)*
                $((ColVec::$float(_, x), ColVec::$float(_, y)) => x.len() <= y.len() && x.iter().zip(y.iter()).all(|(x, y)| x.to_bits() == y.to_bits()),)*
                (ColVec::General(_, x), ColVec::General(_, y)) => x.len() <= y.len() && (Arc::ptr_eq(x, y) || x.iter().zip(y.iter()).all(|(x, y)| identical(x, y))),
                _ => false,
            }
        };
    }
    on!(Bool Guid Byte Short Int Long Char Sym Timestamp Month Date Timespan Minute Second Time; Real Float Datetime)
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
        let kept = begins(a, b);
        kept_any |= kept;
        if kept && b.len() > a.len() {
            let mut tail = b.drop_n(a.len() as i64);
            tail.set_attr_unchecked(b.attr());
            changes.push((i as u32, ColChange::Append { at: a.len() as u64, tail }));
        } else if !kept || a.attr() != b.attr() {
            changes.push((i as u32, ColChange::Replace(b.clone())));
        }
    }
    kept_any.then_some(changes)
}

/// Add `tail` to a column of its type that holds `at` items.
fn extend(col: &mut ColVec, at: u64, tail: &ColVec) -> bool {
    macro_rules! on {
        ($($v:ident)*) => {
            match (col, tail) {
                $((ColVec::$v(attr, x), ColVec::$v(new, y)) if x.len() as u64 == at => {
                    Arc::make_mut(x).extend_from_slice(y);
                    *attr = *new;
                    true
                })*
                _ => false,
            }
        };
    }
    on!(General Bool Guid Byte Short Int Long Real Float Char Sym Timestamp Month Date Datetime Timespan Minute Second Time)
}

/// Make `changes` to `value`; `false` if they were not made from a value like it.
pub fn patch(value: &mut K, changes: &Changes) -> bool {
    let mut cols: Vec<&mut ColVec> = match value {
        K::Vector(c) => vec![c],
        K::Table(f) => Arc::make_mut(f).vals.iter_mut().collect(),
        K::KeyedTable(t) => {
            let t = Arc::make_mut(t);
            t.keys.vals.iter_mut().chain(&mut t.vals.vals).collect()
        }
        _ => return false,
    };
    changes.iter().all(|(i, change)| match (cols.get_mut(*i as usize), change) {
        (Some(col), ColChange::Append { at, tail }) => extend(col, *at, tail),
        (Some(col), ColChange::Replace(new)) => {
            **col = new.clone();
            true
        }
        (None, _) => false,
    })
}
