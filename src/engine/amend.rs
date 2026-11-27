//! `x,:y` and `x[i]:y` as changes to the variable that is there (see `delta.rs`), so that they cost the items they touch
//! and not a copy of the value. What cannot be done so, as when a value changes its type, is done by making a new value.
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

impl Engine<'_> {
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
