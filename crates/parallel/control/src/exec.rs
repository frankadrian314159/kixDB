//! Running a pushed-down query: move the tables it needs, send the shards their part, put their answers together.
use crate::catalog::{rekey, unkey};
use crate::client::ticket;
use crate::merge::Aggs;
use crate::plan::{Kind, Move, Push, Rel, DRIVER};
use crate::Inner;
use futures::future::try_join_all;
use kix_shared_ces::wire::{ord_var, LoadMode, LoadReq, MutateReq, ShuffleReq, Work, ORD_COL};
use kixdb::ast::{ColSpec, Expr, QSql, QSqlKind};
use kixdb::engine::ops::{distinct_op, group_rows, sort_order, sublist_op};
use kixdb::engine::qsql::{exec_by_result, resolve_col_names};
use kixdb::parser::parse_script;
use kixdb::sym::{intern, resolve, SymId};
use kixdb::{Attr, ColVec, Connection, Dict, Flip, KError, KResult, KeyedTable, K};
use std::sync::Arc;

/// Replace every mention of `name` in `e` by `with`.
fn place(e: &mut Expr, name: SymId, with: &Expr) {
    if matches!(e, Expr::Var(s) if *s == name) {
        *e = with.clone();
        return;
    }
    for c in e.children_mut() {
        place(c, name, with);
    }
}

/// `update kix__ord: .kix.ord.<table> from <table>`: a shard's table with its row ordinals as a column.
pub fn augmented(table: SymId) -> Expr {
    Expr::Query(Box::new(QSql {
        kind: QSqlKind::Update,
        distinct: false,
        limit: None,
        cols: vec![ColSpec { name: Some(intern(ORD_COL)), expr: Expr::Var(intern(&ord_var(&resolve(table)))) }],
        by: None,
        from: Box::new(Expr::Var(table)),
        where_clauses: vec![],
    }))
}

/// The relation's source, reading the driving table plainly or with its ordinals.
fn source(rel: &Rel, with_ordinals: bool) -> Expr {
    let mut e = rel.expr.clone();
    let driver = if with_ordinals { augmented(rel.driver) } else { Expr::Var(rel.driver) };
    place(&mut e, intern(DRIVER), &driver);
    e
}

/// The rows of several shards' tables, one after the other (an empty table contributes nothing, not even its types).
pub fn concat(parts: Vec<Flip>) -> KResult<Flip> {
    let mut full = parts.iter().filter(|p| p.nrows() > 0).cloned();
    let Some(mut acc) = full.next() else {
        return parts.into_iter().next().ok_or_else(|| KError::new("cluster", "no shard answered"));
    };
    for next in full {
        if next.cols != acc.cols {
            return Err(KError::new("mismatch", "the shards returned different columns"));
        }
        for (a, b) in acc.vals.iter_mut().zip(&next.vals) {
            *a = a.concat(b);
        }
    }
    Ok(acc)
}

/// Put rows that carry ordinals back in order and remove the ordinal column.
pub fn in_order(parts: Vec<Flip>) -> KResult<Flip> {
    Ok(with_ordinals(parts)?.0)
}

/// Rows that carry ordinals, in order, without the ordinal column, and their ordinals.
pub fn with_ordinals(parts: Vec<Flip>) -> KResult<(Flip, Vec<i64>)> {
    let all = concat(parts)?;
    let at = all.col_idx(intern(ORD_COL)).ok_or_else(|| KError::new("cluster", "a shard returned no row ordinals"))?;
    let ords = match &all.vals[at] {
        ColVec::Long(_, v) => v.clone(),
        _ => Arc::new(vec![]),
    };
    let (mut out, ords) = if ords.windows(2).all(|w| w[0] <= w[1]) {
        (all, (*ords).clone())
    } else {
        let mut idx: Vec<usize> = (0..all.nrows()).collect();
        idx.sort_by_key(|&r| ords[r]);
        (all.gather(&idx), idx.iter().map(|&r| ords[r]).collect())
    };
    out.cols.remove(at);
    out.vals.remove(at);
    Ok((out, ords))
}

fn flip_of(k: K) -> KResult<(Flip, usize)> {
    unkey(&k).ok_or_else(|| KError::new("cluster", "a shard did not return a table"))
}

fn names(f: &Flip) -> K {
    K::Vector(ColVec::Sym(Attr::None, Arc::new(f.cols.clone())))
}

fn dict(keys: K, vals: K) -> K {
    K::Dict(Arc::new(Dict { keys: Box::new(keys), vals: Box::new(vals) }))
}

impl Inner {
    /// Evaluate the same work on every shard.
    pub(crate) fn all(&self, work: Work) -> KResult<Vec<K>> {
        let (ticket, shards) = (ticket(work)?, self.shards());
        self.wait(try_join_all(shards.iter().map(|s| s.run(ticket.clone()))))
    }

    pub(crate) fn one(&self, shard: usize, e: Expr) -> KResult<K> {
        self.wait(self.shard(shard)?.query(Work::Eval(Box::new(e))))
    }

    pub(crate) fn load_all(&self, req: &LoadReq) -> KResult<()> {
        let shards = self.shards();
        self.wait(try_join_all(shards.iter().map(|s| s.load(req)))).map(|_| ())
    }

    pub(crate) fn mutate_all(&self, req: &MutateReq) -> KResult<Vec<K>> {
        let shards = self.shards();
        self.wait(try_join_all(shards.iter().map(|s| s.mutate(req))))
    }

    fn move_table(&self, m: &Move) -> KResult<()> {
        let temp = resolve(m.temp);
        let mut empty = m.source.schema.clone();
        empty.cols.push(intern(ORD_COL));
        empty.vals.push(ColVec::Long(Attr::None, Arc::new(vec![])));
        self.load_all(&LoadReq::new(&temp, LoadMode::Replace, &K::Table(Arc::new(empty)), None))?;
        let (shards, slots) = (self.shards(), self.slots());
        let peers: Vec<String> = shards.iter().map(|s| s.addresses()).collect();
        let sends = shards.iter().enumerate().map(|(me, shard)| {
            let req = ShuffleReq {
                scan: augmented(m.source.name),
                key_cols: m.keys.iter().map(|&k| resolve(k)).collect(),
                target_table: temp.clone(),
                slots: (*slots).clone(),
                peers: peers.clone(),
                me,
            };
            async move { shard.shuffle(&req).await }
        });
        self.wait(try_join_all(sends))?;
        // rows arrive in any order: put them back in the order one engine would hold them
        let sorted = format!("`{ORD_COL} xasc {temp}");
        let finish = if m.driver {
            sorted
        } else if m.source.nkeys > 0 {
            let keys: String = m.source.schema.cols[..m.source.nkeys].iter().map(|&c| format!("`{}", resolve(c))).collect();
            format!("{keys} xkey delete {ORD_COL} from {sorted}")
        } else {
            format!("delete {ORD_COL} from {sorted}")
        };
        let query = parse_script(&finish)?.remove(0);
        self.mutate_all(&MutateReq { table: temp, query }).map(|_| ())
    }

    /// Run `push`; `mirror` evaluates what the control node computes itself.
    pub(crate) fn run_push(&self, mirror: &mut Connection, push: &Push) -> KResult<K> {
        let result = push.rel.moves.iter().try_for_each(|m| self.move_table(m)).and_then(|_| self.run_kind(mirror, push));
        // temporary tables go whether or not the query worked
        let none = K::Null;
        for m in &push.rel.moves {
            let dropped = self.load_all(&LoadReq::new(resolve(m.temp), LoadMode::Drop, &none, None));
            if result.is_ok() {
                dropped?;
            }
        }
        result
    }

    fn run_kind(&self, mirror: &mut Connection, push: &Push) -> KResult<K> {
        let q = &push.query;
        let with = |change: &dyn Fn(&mut QSql)| {
            let mut sent = q.clone();
            change(&mut sent);
            Expr::Query(Box::new(sent))
        };
        // the query as one shard answers it whole
        let whole = || {
            with(&|s| {
                *s.from = source(&push.rel, false);
                if push.exec_single {
                    s.cols[0].name = None;
                }
            })
        };
        match &push.kind {
            Kind::Replicated => self.one(0, whole()),
            Kind::Route(shard) => self.one(*shard, whole()),
            Kind::ByKey => self.by_key(push, with(&|s| {
                *s.from = source(&push.rel, false);
                s.kind = QSqlKind::Select;
                s.limit = None;
            })),
            Kind::Ordered => self.ordered(push, with(&|s| {
                *s.from = source(&push.rel, true);
                s.kind = QSqlKind::Select;
                if !s.cols.is_empty() {
                    s.cols.push(ColSpec { name: Some(intern(ORD_COL)), expr: Expr::Var(intern(ORD_COL)) });
                }
                // `distinct` applies to all rows together; a limit taken of each shard's rows still holds the answer
                if s.distinct {
                    s.distinct = false;
                    s.limit = None;
                }
            })),
            Kind::TwoPhase(aggs) => self.two_phase(mirror, push, aggs, with(&|s| {
                *s.from = source(&push.rel, false);
                s.kind = QSqlKind::Select;
                s.cols = aggs.partials.clone();
                s.limit = None;
            })),
        }
    }

    fn limit(&self, push: &Push, rows: K) -> KResult<K> {
        match push.query.limit.as_deref() {
            Some(Expr::Lit(n)) => sublist_op(&K::Long(n.as_i64()?), &rows),
            _ => Ok(rows),
        }
    }

    fn by_key(&self, push: &Push, sent: Expr) -> KResult<K> {
        let answers = self.all(Work::Eval(Box::new(sent)))?;
        let flips = answers.into_iter().map(flip_of).collect::<KResult<Vec<_>>>()?;
        let nkeys = flips[0].1;
        let all = concat(flips.into_iter().map(|f| f.0).collect())?;
        let mut sorted = all.gather(&sort_order(&all.vals[..nkeys], all.nrows()));
        if nkeys == 1 {
            sorted.vals[0].set_attr_unchecked(Attr::Sorted);
        }
        let result = rekey(sorted, nkeys);
        if push.query.kind == QSqlKind::Exec {
            return Ok(match result {
                K::KeyedTable(kt) => exec_by_result(&kt),
                other => other,
            });
        }
        self.limit(push, result)
    }

    fn ordered(&self, push: &Push, sent: Expr) -> KResult<K> {
        let answers = self.all(Work::Eval(Box::new(sent)))?;
        let flips = answers.into_iter().map(|k| flip_of(k).map(|f| f.0)).collect::<KResult<Vec<_>>>()?;
        let rows = in_order(flips)?;
        let q = &push.query;
        if q.kind == QSqlKind::Exec {
            return Ok(if push.exec_single {
                K::Vector(rows.vals[0].clone())
            } else {
                dict(names(&rows), K::Vector(ColVec::from_atoms(rows.vals.into_iter().map(K::Vector).collect())))
            });
        }
        // `select from t` keeps the keys of a keyed table
        let nkeys = if q.cols.is_empty() { push.rel.nkeys } else { 0 };
        let mut result = rekey(rows, nkeys);
        if q.distinct {
            if let K::Table(_) = result {
                result = distinct_op(&result)?;
            }
        }
        self.limit(push, result)
    }

    fn two_phase(&self, mirror: &mut Connection, push: &Push, aggs: &Aggs, sent: Expr) -> KResult<K> {
        let answers = self.all(Work::Eval(Box::new(sent)))?;
        let flips = answers.into_iter().map(flip_of).collect::<KResult<Vec<_>>>()?;
        let nkeys = flips[0].1;
        let empty_keys = Flip { cols: flips[0].0.cols[..nkeys].to_vec(), vals: flips[0].0.vals[..nkeys].to_vec() };
        let all = concat(flips.into_iter().map(|f| f.0).collect())?;
        // no group anywhere: the answer has the shape the query has on the empty tables
        if all.nrows() == 0 {
            return mirror.eval_expr(&push.original);
        }
        // the rows of each group, the groups in key order
        let (first_rows, mut groups) = group_rows(&all.vals[..nkeys], all.nrows());
        let first_keys: Vec<ColVec> = all.vals[..nkeys].iter().map(|c| c.gather(&first_rows)).collect();
        let order = sort_order(&first_keys, first_rows.len());
        let groups: Vec<Vec<usize>> = order.iter().map(|&g| std::mem::take(&mut groups[g])).collect();
        let merged = (0..aggs.leaf_count())
            .map(|leaf| aggs.merge(leaf, &all.vals[nkeys..], &groups))
            .collect::<KResult<Vec<_>>>()?;
        let grouped = push.query.by.is_some();
        let leaves: Vec<K> = merged
            .into_iter()
            .map(|mut v| if grouped { K::Vector(ColVec::from_atoms(v)) } else { v.pop().unwrap_or(K::Null) })
            .collect();
        let mut values = Vec::with_capacity(aggs.trees.len());
        for t in &aggs.trees {
            values.push(mirror.eval_expr(&aggs.rebuild(t, &leaves))?);
        }

        let mut val_names = resolve_col_names(&push.query.cols);
        if !grouped {
            if push.query.kind == QSqlKind::Exec {
                return Ok(match values.as_slice() {
                    [v] if push.exec_single => v.clone(),
                    _ => dict(K::Vector(ColVec::Sym(Attr::None, Arc::new(val_names))), K::Vector(ColVec::from_atoms(values))),
                });
            }
            let cols = values.into_iter().map(|v| ColVec::from_atoms(vec![v])).collect();
            return self.limit(push, K::Table(Arc::new(Flip::new(val_names, cols)?)));
        }
        let mut keys = Flip { cols: empty_keys.cols, vals: first_keys.iter().map(|c| c.gather(&order)).collect() };
        if nkeys == 1 {
            keys.vals[0].set_attr_unchecked(Attr::Sorted);
        }
        for n in &mut val_names {
            if keys.cols.contains(n) {
                *n = intern(&format!("{}_1", resolve(*n)));
            }
        }
        let cols = values
            .into_iter()
            .map(|v| match v {
                K::Vector(cv) => cv,
                atom => ColVec::repeat(atom, order.len()),
            })
            .collect();
        let vals = Flip::new(val_names, cols)?;
        if push.query.kind == QSqlKind::Exec {
            return Ok(exec_by_result(&KeyedTable { keys, vals }));
        }
        let mut cols = keys.cols;
        cols.extend(vals.cols);
        let mut columns = keys.vals;
        columns.extend(vals.vals);
        self.limit(push, rekey(Flip::new(cols, columns)?, nkeys))
    }
}
