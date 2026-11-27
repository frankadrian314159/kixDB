//! `x,:y` and `x[i]:y` as changes to the variable that is there (see `delta.rs`), so that they cost the items they touch
//! and not a copy of the value. What cannot be done so, as when a value changes its type, is done by making a new value.
//! And assignments at depth, `x[i;j]:y`.
use crate::delta::{ColChange, Changes};
use crate::engine::writes::row_changes;
use crate::engine::{ops, Engine, Write};
use crate::error::{KError, KResult};
use crate::sym::{resolve, SymId};
use crate::types::*;
use std::sync::Arc;

/// Whether the items of a column are all of the type that its name says.
fn typed(column: &ColVec) -> bool {
    !column.is_list() && !matches!(column, ColVec::Enum(..))
}

/// What `list,more` adds to the end of `list`, where it keeps its type.
fn tail(list: &ColVec, more: &K) -> Option<ColVec> {
    let mut tail = match (list, more) {
        (ColVec::Strings(..), K::Vector(added)) if added.packed().is_some() => added.clone(),
        // a general list that holds a list stays general whatever is added
        (ColVec::General(_, items), K::Vector(added)) if items.first().is_some_and(|i| !i.is_atom()) => {
            ColVec::General(Attr::None, Arc::new((0..added.len()).map(|i| added.get_atom(i)).collect()))
        }
        (ColVec::General(_, items), atom) if items.first().is_some_and(|i| !i.is_atom()) && atom.is_atom() => ColVec::General(Attr::None, Arc::new(vec![atom.clone()])),
        (_, K::Vector(added)) if typed(list) && typed(added) && added.type_num() == list.type_num() => added.clone(),
        (_, atom) if typed(list) && atom.type_num() == -list.type_num() => ColVec::from_atoms(vec![atom.clone()]),
        _ => return None,
    };
    tail.set_attr_unchecked(Attr::None);
    Some(tail)
}

/// The keys that a step of an assignment at depth names, where it names several: all of them (`::`, or none given), or
/// those of a list that is not itself a key.
pub(super) fn several(target: &K, step: Option<&K>) -> KResult<Option<Vec<K>>> {
    let items = |k: &K| (0..k.count()).map(|i| ops::index_k(k, &[Some(K::Long(i as i64))])).collect::<KResult<Vec<_>>>();
    Ok(match (target, step) {
        (K::Dict(d), None | Some(K::Null)) => Some(items(&d.keys)?),
        (_, None | Some(K::Null)) => Some(items(&ops::til_op(target.count() as i64)?)?),
        (K::Dict(d), Some(key @ K::Vector(v))) if matches!(v, ColVec::Char(..)) || items(&d.keys)?.contains(key) => None,
        (_, Some(keys @ K::Vector(_))) => Some(items(keys)?),
        _ => None,
    })
}

/// What the `n`th of `count` items assigned to is given: `y` itself, unless it is a list, which has one for each.
pub(super) fn part(y: &K, n: usize, count: usize) -> KResult<K> {
    match y {
        K::Vector(v) if v.len() == count => Ok(v.get_atom(n)),
        K::Vector(_) => Err(KError::length("a list assigned to several items needs one for each")),
        all => Ok(all.clone()),
    }
}

impl Engine<'_> {
    /// `x[i;j;...]:y`, or with `op` `x[i;j;...]+:y`: the first index chooses what the rest assign to.
    pub(super) fn amend_deep(&mut self, mut target: K, indices: &[Option<K>], op: Option<SymId>, rhs: &K) -> KResult<K> {
        let Some((first, rest)) = indices.split_first() else { return Ok(rhs.clone()) };
        let Some(keys) = several(&target, first.as_ref())? else {
            let step = [first.clone()];
            let new = self.amend_in_place(ops::index_k(&target, &step)?, rest, op, rhs)?;
            return self.amend_in_place(target, &step, None, &new);
        };
        let count = keys.len();
        // the items of a list are changed in turn and the list made once
        if let K::Vector(list) = &target {
            let mut items: Vec<K> = (0..list.len()).map(|i| list.get_atom(i)).collect();
            for (n, key) in keys.iter().enumerate() {
                let item = usize::try_from(key.as_i64()?).ok().and_then(|i| items.get_mut(i)).ok_or_else(|| KError::domain("index out of bounds"))?;
                *item = self.amend_in_place(std::mem::replace(item, K::Null), rest, op, &part(rhs, n, count)?)?;
            }
            return Ok(K::Vector(ColVec::from_atoms(items)));
        }
        for (n, key) in keys.into_iter().enumerate() {
            let full: Vec<Option<K>> = std::iter::once(Some(key)).chain(rest.iter().cloned()).collect();
            target = self.amend_deep(target, &full, op, &part(rhs, n, count)?)?;
        }
        Ok(target)
    }

    /// The value that an assignment to `name` changes (`global`: a `::` or an indexed assignment, which reaches a
    /// global from inside a lambda), with the global's full name where it is one.
    fn changeable(&self, name: SymId, global: bool) -> Option<(Option<SymId>, &K)> {
        if !self.table_scopes.is_empty() {
            return None;
        }
        match self.locals.last() {
            Some(frame) if frame.contains_key(&name) => Some((None, &frame[&name])),
            Some(_) if !global => None,
            _ => self.global_to_change(name).map(|full| (Some(full), &self.ws.vars[&full])),
        }
    }

    fn change(&mut self, name: SymId, global: Option<SymId>, changes: Changes) -> KResult<()> {
        if let Some(full) = global {
            return self.write_globals(vec![(full, Write::Change(changes))]);
        }
        let local = self.locals.last_mut().and_then(|frame| frame.get_mut(&name));
        match local.is_some_and(|value| crate::delta::patch(value, &changes)) {
            true => Ok(()),
            false => Err(KError::new("stale", format!("'{}' could not be changed", resolve(name)))),
        }
    }

    /// `name,:more` where it only adds to the end of a list or a table: the value afterwards, or `None` if it does not.
    pub(crate) fn append_where_it_is(&mut self, name: SymId, global: bool, more: &K) -> KResult<Option<K>> {
        let Some((full, value)) = self.changeable(name, global) else { return Ok(None) };
        let changes = match value {
            K::Vector(list) => tail(list, more).map(|tail| vec![(0, ColChange::Append { at: list.len() as u64, tail })]),
            K::Table(_) => {
                let table = value.clone();
                let (incoming, _) = self.incoming_rows(&table, more)?;
                row_changes(&table, &incoming, true)?.map(|rows| rows.changes)
            }
            _ => None,
        };
        let Some(changes) = changes else { return Ok(None) };
        self.change(name, full, changes)?;
        Ok(Some(self.get_var(name)?))
    }

    /// `name[i]:y` or `name[i]+:y` for one item of a list, or one key of a dictionary: `false` if it is not that.
    pub(crate) fn amend_where_it_is(&mut self, name: SymId, indices: &[Option<K>], op: Option<SymId>, rhs: &K) -> KResult<bool> {
        let [Some(index)] = indices else { return Ok(false) };
        let Some((full, value)) = self.changeable(name, true) else { return Ok(false) };
        if !index.is_atom() {
            return Ok(false);
        }
        // the column that holds the item, its row (`None`: a new key) and the item as it is
        let (column, row, held) = match value {
            K::Vector(list) if typed(list) => match index.as_i64().ok().and_then(|i| usize::try_from(i).ok()).filter(|&i| i < list.len()) {
                Some(i) => (list.clone(), Some(i), list.get_atom(i)),
                None => return Ok(false),
            },
            K::Dict(d) => match (&*d.keys, &*d.vals) {
                (K::Vector(keys), K::Vector(vals)) if typed(keys) && typed(vals) && index.type_num() == -keys.type_num() => match keys.position(index) {
                    Some(i) => (vals.clone(), Some(i), vals.get_atom(i)),
                    None => (vals.clone(), None, ops::dict_lookup(d, index)?),
                },
                _ => return Ok(false),
            },
            _ => return Ok(false),
        };
        let dict = matches!(value, K::Dict(_));
        let new = match op {
            Some(op) => self.eval_binary_dispatch(resolve(op), &held, rhs)?,
            None => rhs.clone(),
        };
        // a dictionary whose values change their type is made anew; a list takes what converts to its type, or nothing
        if dict && new.type_num() != -column.type_num() {
            return Ok(false);
        }
        let mut item = column.take(1);
        if item.len() != 1 {
            return Ok(false);
        }
        item.set_at(0, new)?;
        let (rows_of, len) = (u32::from(dict), column.len() as u64);
        let changes = match row {
            Some(i) => vec![(rows_of, ColChange::Set { rows: vec![i as u64], values: item })],
            None => vec![(0, ColChange::Append { at: len, tail: ColVec::from_atoms(vec![index.clone()]) }), (1, ColChange::Append { at: len, tail: item })],
        };
        // (the column is let go of, so that it can be changed where it is)
        drop((column, held));
        self.change(name, full, changes)?;
        Ok(true)
    }
}
