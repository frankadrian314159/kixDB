//! Verbs that treat a table as a list of rows (and a keyed table as a dictionary from key rows to value rows).
use crate::engine::ops::{self, index_k};
use crate::error::{KError, KResult};
use crate::types::*;
use foldhash::{HashMap, HashSet, HashSetExt};
use std::sync::Arc;

/// A key standing for each row of `t`, equal for equal rows.
fn row_keys(t: &Flip) -> Vec<Vec<u8>> {
    (0..t.nrows()).map(|r| t.vals.iter().flat_map(|c| c.get_atom(r).hash_key()).collect()).collect()
}

/// The columns of `t` in the order of `like`'s.
fn ordered(t: &Flip, like: &Flip) -> KResult<Flip> {
    let col = |&name| t.col(name).cloned().ok_or_else(|| KError::new("mismatch", "table columns do not match"));
    match t.ncols() == like.ncols() {
        true => Flip::new(like.cols.clone(), like.cols.iter().map(col).collect::<KResult<_>>()?),
        false => Err(KError::new("mismatch", "table columns do not match")),
    }
}

/// `a , b` of tables: the rows of both, matching columns by name; a column of two types becomes a list of mixed values.
pub fn append(a: &Flip, b: &Flip) -> KResult<Flip> {
    let b = ordered(b, a)?;
    Ok(Flip { cols: a.cols.clone(), vals: a.vals.iter().zip(&b.vals).map(|(x, y)| x.concat(y)).collect() })
}

/// A list of values: dictionaries with the same names for keys make a table.
pub fn collapse(items: Vec<K>) -> K {
    let names = |k: &K| match k {
        K::Dict(d) => match &*d.keys {
            K::Vector(ColVec::Sym(_, names)) if !names.is_empty() => Some(names.clone()),
            _ => None,
        },
        _ => None,
    };
    if let Some(first) = items.first().and_then(names) {
        if items.iter().all(|i| names(i).is_some_and(|n| n == first)) {
            let columns: Vec<ColVec> = (0..first.len()).map(|c| ColVec::from_atoms(items.iter().map(|i| match i {
                K::Dict(d) => index_k(&d.vals, &[Some(K::Long(c as i64))]).unwrap_or(K::Null),
                _ => K::Null,
            }).collect())).collect();
            return K::Table(Arc::new(Flip { cols: (*first).clone(), vals: columns }));
        }
    }
    K::Vector(ColVec::from_atoms(items))
}

/// `a ,' b` of tables with as many rows: the columns of both, those of `b` winning.
pub fn beside(a: &K, b: &K) -> Option<KResult<K>> {
    let join = |a: &Flip, b: &Flip| -> KResult<Flip> {
        if a.nrows() != b.nrows() {
            return Err(KError::length("the tables must have as many rows"));
        }
        let (mut cols, mut vals) = (a.cols.clone(), a.vals.clone());
        for (&name, v) in b.cols.iter().zip(&b.vals) {
            match cols.iter().position(|&c| c == name) {
                Some(i) => vals[i] = v.clone(),
                None => {
                    cols.push(name);
                    vals.push(v.clone());
                }
            }
        }
        Flip::new(cols, vals)
    };
    Some(match (a, b) {
        (K::Table(a), K::Table(b)) => join(a, b).map(|t| K::Table(Arc::new(t))),
        (K::KeyedTable(a), K::KeyedTable(b)) if a.keys == b.keys => {
            join(&a.vals, &b.vals).and_then(|vals| Ok(K::KeyedTable(Arc::new(KeyedTable::new(a.keys.clone(), vals)?))))
        }
        _ => return None,
    })
}

/// The row of `t` that `row` names: a dictionary or a list of the values of the columns.
fn find_row(keys: &[Vec<u8>], t: &Flip, row: &K) -> KResult<i64> {
    let values: Vec<K> = match row {
        K::Dict(d) => t.cols.iter().map(|&c| ops::dict_lookup(d, &K::Sym(c))).collect::<KResult<_>>()?,
        K::Vector(v) if v.len() == t.ncols() => (0..v.len()).map(|i| v.get_atom(i)).collect(),
        atom if t.ncols() == 1 => vec![atom.clone()],
        _ => return Err(KError::length("a row needs a value for each column")),
    };
    let wanted: Vec<u8> = values.iter().flat_map(K::hash_key).collect();
    Ok(keys.iter().position(|k| *k == wanted).unwrap_or(keys.len()) as i64)
}

/// `t ? rows`: the number of each row; for a keyed table, the key of the row with those values.
pub fn find(lhs: &K, rhs: &K) -> Option<KResult<K>> {
    let (t, kt) = match lhs {
        K::Table(t) => (&**t, None),
        K::KeyedTable(kt) => (&kt.vals, Some(kt)),
        _ => return None,
    };
    let keys = row_keys(t);
    let one = |row: &K| -> KResult<i64> { find_row(&keys, t, row) };
    let found: KResult<(Vec<i64>, bool)> = match rhs {
        K::Table(wanted) => (0..wanted.nrows()).map(|r| one(&K::Dict(Arc::new(wanted.row_dict(r))))).collect::<KResult<_>>().map(|v| (v, false)),
        // a list of rows, as opposed to one row
        K::Vector(ColVec::General(_, rows)) if rows.iter().all(|r| matches!(r, K::Vector(v) if v.len() == t.ncols())) && !rows.is_empty() => {
            rows.iter().map(one).collect::<KResult<_>>().map(|v| (v, false))
        }
        row => one(row).map(|i| (vec![i], true)),
    };
    Some(found.and_then(|(rows, single)| {
        let at = match single {
            true => K::Long(rows[0]),
            false => K::Vector(ColVec::Long(Attr::None, Arc::new(rows))),
        };
        match kt {
            Some(kt) => index_k(&K::Table(Arc::new(kt.keys.clone())), &[Some(at)]),
            None => Ok(at),
        }
    }))
}

/// The rows of the keyed table `kt` whose keys are (or, `keep` false, are not) among the rows of `keys`.
fn by_keys(kt: &KeyedTable, keys: &Flip, keep: bool) -> KResult<K> {
    let wanted: HashSet<Vec<u8>> = row_keys(&ordered(keys, &kt.keys)?).into_iter().collect();
    let rows: Vec<usize> = row_keys(&kt.keys).iter().enumerate().filter(|(_, k)| wanted.contains(*k) == keep).map(|(i, _)| i).collect();
    Ok(K::KeyedTable(Arc::new(KeyedTable { keys: kt.keys.gather(&rows), vals: kt.vals.gather(&rows) })))
}

/// `keys # kt` (the rows with those keys, in their order) and `keys _ kt` (the others).
pub fn take_or_drop(lhs: &K, rhs: &K, take: bool) -> Option<KResult<K>> {
    let K::KeyedTable(kt) = rhs else { return None };
    let keys = match lhs {
        K::Table(keys) => (**keys).clone(),
        // the keys given as a list of rows
        K::Vector(ColVec::General(_, items)) => from_rows(&kt.keys.cols, &kt.keys.vals, items)?.ok()?,
        _ => return None,
    };
    if !take {
        return Some(by_keys(kt, &keys, false));
    }
    Some(ordered(&keys, &kt.keys).map(|keys| {
        let held: HashMap<Vec<u8>, usize> = row_keys(&kt.keys).into_iter().enumerate().rev().map(|(i, k)| (k, i)).collect();
        let rows: Vec<usize> = row_keys(&keys).iter().filter_map(|k| held.get(k).copied()).collect();
        K::KeyedTable(Arc::new(KeyedTable { keys: kt.keys.gather(&rows), vals: kt.vals.gather(&rows) }))
    }))
}

/// `x _ i`: the list, table or keyed table without the item at `i` (for a keyed table, the row with that key).
pub fn delete(lhs: &K, rhs: &K) -> Option<KResult<K>> {
    let others = |n: usize, at: i64| -> Vec<usize> { (0..n).filter(|&i| i as i64 != at).collect() };
    Some(match (lhs, rhs) {
        (K::Vector(v), at @ (K::Long(_) | K::Int(_) | K::Short(_))) => at.as_i64().map(|at| K::Vector(v.gather(&others(v.len(), at)))),
        (K::Table(t), at @ (K::Long(_) | K::Int(_) | K::Short(_))) => at.as_i64().map(|at| K::Table(Arc::new(t.gather(&others(t.nrows(), at))))),
        (K::KeyedTable(kt), key) if key.is_atom() && kt.keys.ncols() == 1 => {
            Flip::new(kt.keys.cols.clone(), vec![ColVec::from_atoms(vec![key.clone()])]).and_then(|keys| by_keys(kt, &keys, false))
        }
        _ => return None,
    })
}

#[derive(Clone, Copy, PartialEq)]
pub enum Set {
    Inter,
    Union,
    Except,
}

/// `inter`, `union` and `except` of the rows of two tables.
pub fn set(a: &Flip, b: &Flip, kind: Set) -> KResult<K> {
    let b = ordered(b, a)?;
    let theirs: HashSet<Vec<u8>> = row_keys(&b).into_iter().collect();
    let mine = row_keys(a);
    if kind == Set::Union {
        let all = append(a, &b)?;
        let mut seen = HashSet::new();
        let first: Vec<usize> = row_keys(&all).into_iter().enumerate().filter_map(|(i, k)| seen.insert(k).then_some(i)).collect();
        return Ok(K::Table(Arc::new(all.gather(&first))));
    }
    let rows: Vec<usize> = mine.iter().enumerate().filter_map(|(i, k)| (theirs.contains(k) == (kind == Set::Inter)).then_some(i)).collect();
    Ok(K::Table(Arc::new(a.gather(&rows))))
}

/// A table made of rows given as dictionaries of its columns, or as lists of values that fit them.
pub fn from_rows(cols: &[crate::sym::SymId], proto: &[ColVec], rows: &[K]) -> Option<KResult<Flip>> {
    let fits = |value: &K, column: &ColVec| {
        let (value, column) = (value.plain(), column.plain());
        matches!(column, ColVec::General(..)) || value.type_num().abs() == column.type_num() || column.is_numeric() && matches!(*value, K::Long(_) | K::Int(_))
    };
    let row = |r: &K| -> Option<Vec<K>> {
        match r {
            K::Dict(d) => cols.iter().map(|&c| ops::dict_lookup(d, &K::Sym(c)).ok()).collect(),
            K::Vector(ColVec::General(_, v)) if v.len() == cols.len() && v.iter().zip(proto).all(|(x, c)| fits(x, c)) => Some((**v).clone()),
            _ => None,
        }
    };
    let rows: Vec<Vec<K>> = rows.iter().map(row).collect::<Option<_>>().filter(|r: &Vec<_>| !r.is_empty())?;
    let column = |c: usize| ColVec::from_atoms(rows.iter().map(|r| r[c].clone()).collect());
    Some(Flip::new(cols.to_vec(), (0..cols.len()).map(column).collect()))
}
