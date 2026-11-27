//! `insert` and `upsert` as changes to the table that is there: new rows are added to the ends of its columns and
//! matched rows of a keyed table are set where they are, so a write costs the rows it touches and a look through the
//! keys, not a copy of the table.
use crate::delta::{ColChange, Changes};
use crate::engine::joins::match_keys;
use crate::engine::ops::group_ids;
use crate::error::{KError, KResult};
use crate::types::*;

/// The changes that add rows to a table, and the incoming rows that become its new rows, in order.
pub struct RowChanges {
    pub changes: Changes,
    pub added: Vec<usize>,
}

/// `column` with the type of `target`, as `insert` converts it; `None` where the column would have to change type.
fn fitted(target: &ColVec, column: &ColVec) -> KResult<Option<ColVec>> {
    // strings are added to strings where they are; other lists are made anew
    if let ColVec::Strings(..) = target {
        let packed = column.packed().map(|c| c.into_owned());
        return Ok(packed.map(|mut c| {
            c.set_attr_unchecked(Attr::None);
            c
        }));
    }
    if matches!(target, ColVec::General(..)) {
        return Ok(None);
    }
    // (a column of the target's type is taken as it is)
    let same = target.type_num() == column.type_num() && !matches!(target, ColVec::Enum(..)) && !matches!(column, ColVec::Enum(..));
    let mut fitted = if same { column.clone() } else { target.take(0).append_strict(column)? };
    fitted.set_attr_unchecked(Attr::None);
    Ok((fitted.type_num() == target.type_num() && fitted.len() == column.len()).then_some(fitted))
}

/// How the `rows` (the table's columns, keys first) change `table`. A keyed table takes them as `upsert` does, setting
/// the rows whose keys it has (nulls leave values alone), unless `insert`, which refuses those. `None`: the table must be
/// rebuilt (a column of mixed values, or one that changes type).
pub fn row_changes(table: &K, rows: &Flip, insert: bool) -> KResult<Option<RowChanges>> {
    let (columns, keys): (Vec<&ColVec>, usize) = match table {
        K::Table(t) => (t.vals.iter().collect(), 0),
        K::KeyedTable(t) => (t.keys.vals.iter().chain(&t.vals.vals).collect(), t.keys.ncols()),
        _ => return Err(KError::ty("insert/upsert target must be a table")),
    };
    if columns.len() != rows.ncols() {
        return Err(KError::new("mismatch", "table columns do not match"));
    }
    let mut incoming = Vec::with_capacity(columns.len());
    for (target, column) in columns.iter().zip(&rows.vals) {
        match fitted(target, column)? {
            Some(c) => incoming.push(c),
            None => return Ok(None),
        }
    }
    let n = rows.nrows();
    let len = columns.first().map_or(0, |c| c.len());
    // for each column, the rows already there that change and the incoming row that gives each its value
    let mut set: Vec<Vec<(u64, usize)>> = vec![vec![]; columns.len()];
    let K::KeyedTable(table) = table else {
        // every incoming row is a new one: the columns are added as they are
        let append = |(c, tail)| (c as u32, ColChange::Append { at: len as u64, tail });
        let changes = if n == 0 { vec![] } else { incoming.into_iter().enumerate().map(append).collect() };
        return Ok(Some(RowChanges { changes, added: (0..n).collect() }));
    };
    // for each column, the incoming row that gives each new row its value
    let mut new: Vec<Vec<usize>> = vec![vec![]; columns.len()];
    let found = match_keys(table, &incoming[..keys].iter().collect::<Vec<_>>(), n);
    // incoming rows with one key make one new row
    let same = group_ids(&incoming[..keys], n);
    let mut made: Vec<Option<usize>> = vec![None; n];
    for (r, at) in found.into_iter().enumerate() {
        let given = (keys..columns.len()).filter(|&c| !incoming[c].get_atom(r).is_null());
        match (at, made[same[r]]) {
            (Some(_), _) if insert => return Err(KError::new("insert", "duplicate key in keyed table insert")),
            (Some(at), _) => given.for_each(|c| set[c].push((at as u64, r))),
            (None, Some(row)) => given.for_each(|c| new[c][row] = r),
            (None, None) => {
                made[same[r]] = Some(new[0].len());
                new.iter_mut().for_each(|from| from.push(r));
            }
        }
    }
    let mut changes = vec![];
    for (c, mut of_column) in set.into_iter().enumerate() {
        if !of_column.is_empty() {
            // the last of the rows that set a value wins
            of_column.sort_by_key(|&(at, _)| at);
            of_column.reverse();
            of_column.dedup_by_key(|&mut (at, _)| at);
            let (at, from): (Vec<u64>, Vec<usize>) = of_column.into_iter().rev().unzip();
            changes.push((c as u32, ColChange::Set { rows: at, values: incoming[c].gather(&from) }));
        }
    }
    let added = new[0].clone();
    if !added.is_empty() {
        for (c, (column, from)) in incoming.iter().zip(new).enumerate() {
            // (rows that all arrive, in order, need no copying)
            let tail = if from.iter().enumerate().all(|(i, &r)| i == r) && from.len() == n { column.clone() } else { column.gather(&from) };
            changes.push((c as u32, ColChange::Append { at: len as u64, tail }));
        }
    }
    Ok(Some(RowChanges { changes, added }))
}
