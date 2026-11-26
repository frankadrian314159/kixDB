//! Enumerations, foreign keys and linked columns: values tagged with the global they are drawn from (their domain),
//! which the engine checks as they are made and follows for `key.column`. Verbs that work on values see them untagged.
use crate::engine::joins::match_keys;
use crate::engine::{ops, Engine};
use crate::error::{KError, KResult};
use crate::sym::{resolve, SymId};
use crate::types::*;
use std::collections::HashSet;
use std::sync::Arc;

fn long_column(values: Vec<i64>) -> ColVec {
    ColVec::Long(Attr::None, Arc::new(values))
}

/// `x` as a list of values of a domain with `width` key columns, and whether it is just one.
fn as_list(width: usize, x: &K) -> (ColVec, bool) {
    match x {
        // (one compound key is a list of atoms, a value for each key column)
        K::Vector(v) if width > 1 && v.len() == width && (0..width).all(|i| v.get_atom(i).is_atom()) => {
            (ColVec::General(Attr::None, Arc::new(vec![x.clone()])), true)
        }
        K::Vector(ColVec::General(_, items)) if width == 1 => (ColVec::from_atoms((**items).clone()), false),
        K::Vector(v) => (v.clone(), false),
        one => (ColVec::from_atoms(vec![one.clone()]), true),
    }
}

/// `values` as columns of the types of the domain's `keys`, a row for each value.
fn key_values(keys: &Flip, values: &ColVec) -> KResult<Vec<ColVec>> {
    let fitted = |key: &ColVec, column: &ColVec| {
        key.take(0).append_strict(column).map_err(|_| KError::new("cast", "values of another type than the domain's"))
    };
    if keys.ncols() == 1 {
        return Ok(vec![fitted(&keys.vals[0], values)?]);
    }
    let part = |c: usize, r: usize| match values.get_atom(r) {
        K::Vector(key) if key.len() == keys.ncols() => Ok(key.get_atom(c)),
        _ => Err(KError::length("a compound key takes a value for each key column")),
    };
    let column = |c: usize| fitted(&keys.vals[c], &ColVec::from_atoms((0..values.len()).map(|r| part(c, r)).collect::<KResult<_>>()?));
    (0..keys.ncols()).map(column).collect()
}

/// The place of each value (a row of `columns`) among the domain's `keys`, `None` for those not there.
fn places(keys: Flip, columns: &[ColVec]) -> Vec<Option<usize>> {
    let table = KeyedTable { keys, vals: Flip { cols: vec![], vals: vec![] } };
    match_keys(&table, &columns.iter().collect::<Vec<_>>(), columns.first().map_or(0, ColVec::len))
}

impl Engine<'_> {
    /// The global that enumerations called `name` are drawn from, and its full name.
    fn domain(&self, name: SymId) -> Option<(SymId, &K)> {
        [self.qualify(name), name].into_iter().find_map(|n| self.ws.vars.get(&n).map(|v| (n, v)))
    }

    /// The columns whose values an enumeration of `tag` takes: the list itself, or the keys of the keyed table.
    fn domain_keys(&self, tag: Tag) -> KResult<Flip> {
        let (cols, vals) = match self.domain(tag.domain) {
            Some((_, K::KeyedTable(t))) if tag.source == Source::Keys => (t.keys.cols.clone(), t.keys.vals.clone()),
            Some((_, K::Vector(v))) if tag.source == Source::List => (vec![tag.domain], vec![v.clone()]),
            _ => return Err(KError::new("cast", format!("`{} is not there to take values from", resolve(tag.domain)))),
        };
        Ok(Flip { cols, vals: vals.iter().map(|c| c.plain().clone()).collect() })
    }

    /// `x` (an atom, a list, or one enumerated already) as an enumeration of `tag`: `'cast` for a value that the domain
    /// lacks (nulls need not be there).
    pub(crate) fn enumerate(&mut self, tag: Tag, x: &K) -> KResult<K> {
        if x.tag() == Some(tag) {
            return Ok(x.clone());
        }
        // row numbers need no looking up
        let keys = match tag.source {
            Source::Rows => None,
            _ => Some(self.domain_keys(tag)?),
        };
        let (list, atom) = as_list(keys.as_ref().map_or(1, Flip::ncols), &x.plain());
        let columns = match keys {
            None => vec![long_column(list.as_i64_vec().ok_or_else(|| KError::ty("a linked column takes row numbers"))?)],
            Some(keys) => {
                let columns = key_values(&keys, &list)?;
                let missing = |r: usize| columns.iter().any(|c| !c.get_atom(r).is_null());
                if places(keys, &columns).iter().enumerate().any(|(r, at)| at.is_none() && missing(r)) {
                    return Err(KError::new("cast", format!("not values of `{}", resolve(tag.domain))));
                }
                columns
            }
        };
        let values = match columns.as_slice() {
            [one] => one.clone(),
            many => {
                let key = |r| K::Vector(ColVec::from_atoms(many.iter().map(|c| c.get_atom(r)).collect()));
                ColVec::General(Attr::None, Arc::new((0..list.len()).map(key).collect()))
            }
        };
        let tagged = ColVec::Enum(tag, Box::new(values));
        Ok(if atom { tagged.get_atom(0) } else { K::Vector(tagged) })
    }

    /// `` `name?x ``: add to the list `name` the values of `x` that it lacks, then enumerate `x`.
    fn extend_domain(&mut self, name: SymId, x: &K) -> KResult<K> {
        let (full, held) = match self.domain(name) {
            Some((full, K::Vector(v))) => (full, Some(v.plain().clone())),
            _ => (self.qualify(name), None),
        };
        let new_domain = held.is_none();
        let held = held.unwrap_or_else(|| ColVec::General(Attr::None, Arc::new(vec![])));
        let (wanted, _) = as_list(1, &x.plain());
        let mut seen: HashSet<Vec<u8>> = (0..held.len()).map(|i| held.get_atom(i).hash_key()).collect();
        let new: Vec<usize> = (0..wanted.len()).filter(|&i| seen.insert(wanted.get_atom(i).hash_key())).collect();
        if new_domain || !new.is_empty() {
            let grown = held.append_strict(&wanted.gather(&new))?;
            self.set_globals(vec![(full, Some(K::Vector(grown)))])?;
        }
        self.enumerate(Tag { domain: name, source: Source::List }, x)
    }

    /// The row of the domain that each value of the enumeration `x` refers to (-1 for none), and whether `x` is one value.
    fn rows_referred(&self, tag: Tag, x: &K) -> KResult<(Vec<i64>, bool)> {
        if tag.source == Source::Rows {
            let (list, atom) = as_list(1, &x.plain());
            return Ok((list.as_i64_vec().ok_or_else(|| KError::ty("a linked column takes row numbers"))?, atom));
        }
        let keys = self.domain_keys(tag)?;
        let (list, atom) = as_list(keys.ncols(), &x.plain());
        let columns = key_values(&keys, &list)?;
        Ok((places(keys, &columns).into_iter().map(|at| at.map_or(-1, |r| r as i64)).collect(), atom))
    }

    /// `x.column`: the column of the table that a foreign key or link refers to, at the rows it refers to.
    pub(crate) fn follow(&mut self, tag: Tag, x: &K, column: SymId) -> KResult<K> {
        let (rows, atom) = self.rows_referred(tag, x)?;
        let table = match self.domain(tag.domain) {
            Some((_, K::Table(t))) => (**t).clone(),
            Some((_, K::KeyedTable(t))) => t.unkey(),
            _ => return Err(KError::value(resolve(tag.domain))),
        };
        let found = table.col(column).ok_or_else(|| KError::value(resolve(column)))?.gather_signed(&rows);
        Ok(if atom { found.get_atom(0) } else { K::Vector(found) })
    }

    /// `rows` for the table `target`: values for its enumerated columns enumerated too, which checks them.
    pub(crate) fn enumerate_rows(&mut self, target: &Flip, mut rows: Flip) -> KResult<Flip> {
        for (name, column) in rows.cols.iter().zip(&mut rows.vals) {
            if let Some(&ColVec::Enum(tag, _)) = target.col(*name) {
                if let K::Vector(made) = self.enumerate(tag, &K::Vector(column.clone()))? {
                    *column = made;
                }
            }
        }
        Ok(rows)
    }

    /// The right side of `table , more`, with values for enumerated columns of `table` enumerated.
    fn enumerate_added(&mut self, table: &Flip, more: &K) -> KResult<K> {
        Ok(match more {
            K::Table(t) => K::Table(Arc::new(self.enumerate_rows(table, (**t).clone())?)),
            K::KeyedTable(t) => {
                let (keys, vals) = (self.enumerate_rows(table, t.keys.clone())?, self.enumerate_rows(table, t.vals.clone())?);
                K::KeyedTable(Arc::new(KeyedTable { keys, vals }))
            }
            K::Dict(d) => {
                let (K::Vector(ColVec::Sym(_, names)), mut values) = (&*d.keys, Vec::new()) else { return Ok(more.clone()) };
                for (i, &name) in names.iter().enumerate() {
                    let value = ops::index_k(&d.vals, &[Some(K::Long(i as i64))])?;
                    values.push(match table.col(name) {
                        Some(&ColVec::Enum(tag, _)) => self.enumerate(tag, &value)?,
                        _ => value,
                    });
                }
                K::Dict(Arc::new(Dict { keys: d.keys.clone(), vals: Box::new(K::Vector(ColVec::from_atoms(values))) }))
            }
            other => other.clone(),
        })
    }

    /// The forms of verbs that make or take apart enumerations, which need the domains: `` `name$x ``, `` `name?x ``,
    /// `` `name!rows ``, `` `long$e `` and adding rows to a table with enumerated columns. `None` for anything else.
    pub(crate) fn enum_verb(&mut self, op: &str, lhs: &K, rhs: &K) -> Option<KResult<K>> {
        if let (",", K::Table(_) | K::KeyedTable(_)) = (op, lhs) {
            let flip = match lhs {
                K::Table(t) => (**t).clone(),
                K::KeyedTable(t) => t.unkey(),
                _ => return None,
            };
            let tagged = flip.vals.iter().any(|c| matches!(c, ColVec::Enum(..)));
            return tagged.then(|| ops::eval_binary_prim(",", lhs, &self.enumerate_added(&flip, rhs)?));
        }
        let K::Sym(name) = lhs else { return None };
        let domain = self.domain(*name).map(|(_, v)| v);
        match op {
            "$" if resolve(*name) == "long" => {
                let tag = rhs.tag()?;
                Some(self.rows_referred(tag, rhs).map(|(rows, atom)| if atom { K::Long(rows[0]) } else { K::Vector(long_column(rows)) }))
            }
            "$" if !ops::is_type_name(&resolve(*name)) => {
                let source = match domain? {
                    K::KeyedTable(_) => Source::Keys,
                    K::Vector(_) => Source::List,
                    _ => return None,
                };
                Some(self.enumerate(Tag { domain: *name, source }, rhs))
            }
            "?" if matches!(domain, None | Some(K::Vector(_))) => Some(self.extend_domain(*name, rhs)),
            // (the table need not be there yet: a table may link to itself)
            "!" if matches!(domain, None | Some(K::Table(_) | K::KeyedTable(_))) && matches!(rhs.type_num().abs(), 5..=7) => {
                Some(self.enumerate(Tag { domain: *name, source: Source::Rows }, rhs))
            }
            _ => None,
        }
    }
}
