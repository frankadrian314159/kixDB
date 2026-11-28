//! Running a pushed-down query: move the tables it needs, send the shards their part, put their answers together.
use crate::catalog::{rekey, unkey};
use crate::client::ticket;
use crate::merge::{Aggs, Merger};
use crate::plan::{Kind, Move, Push, Rel, DRIVER};
use crate::Inner;
use futures::future::try_join_all;
use kix_shared_ces::wire::{ord_var, LoadMode, LoadReq, MutateReq, ShuffleReq, Work, ORD_COL};
use kixdb::ast::{ColSpec, Expr, QSql, QSqlKind};
use kixdb::engine::ops::{distinct_op, merge_sorted_flips, sublist_op};
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
        cols: vec![ColSpec { name: Some(intern(ORD_COL)), expr: Expr::Var(intern(&ord_var(resolve(table)))) }],
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

/// Put rows that carry ordinals back in order via `O(N log k)` k-way merge and remove the ordinal column.
pub fn in_order(parts: Vec<Flip>) -> KResult<Flip> {
    in_order_limit(parts, None)
}

/// `in_order`, stopping early after `limit` rows when given.
pub fn in_order_limit(parts: Vec<Flip>, limit: Option<usize>) -> KResult<Flip> {
    let at = ord_col_idx(&parts)?;
    let mut out = merge_sorted_flips(parts, &[(at, false)], limit)?;
    out.cols.remove(at);
    out.vals.remove(at);
    Ok(out)
}

/// Rows that carry ordinals, in order, without the ordinal column, and their ordinals.
pub fn with_ordinals(parts: Vec<Flip>) -> KResult<(Flip, Vec<i64>)> {
    let at = ord_col_idx(&parts)?;
    let mut out = merge_sorted_flips(parts, &[(at, false)], None)?;
    out.cols.remove(at);
    let ords = match out.vals.remove(at) {
        ColVec::Long(_, v) => Arc::unwrap_or_clone(v),
        _ => vec![],
    };
    Ok((out, ords))
}

fn ord_col_idx(parts: &[Flip]) -> KResult<usize> {
    parts
        .iter()
        .find(|p| p.nrows() > 0)
        .or_else(|| parts.first())
        .ok_or_else(|| KError::new("cluster", "no shard answered"))?
        .col_idx(intern(ORD_COL))
        .ok_or_else(|| KError::new("cluster", "a shard returned no row ordinals"))
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

fn wrap_sort_stages(mut expr: Expr, stages: &[(Vec<SymId>, bool)], top_k: Option<usize>) -> Expr {
    for (cols, desc) in stages.iter().rev() {
        let lhs = Expr::Lit(K::Vector(ColVec::Sym(Attr::None, Arc::new(cols.clone()))));
        let op = intern(if *desc { "xdesc" } else { "xasc" });
        expr = Expr::Binary { op, lhs: Box::new(lhs), rhs: Box::new(expr) };
    }
    if let Some(k) = top_k {
        expr = Expr::Binary {
            op: intern("sublist"),
            lhs: Box::new(Expr::Lit(K::Long(k as i64))),
            rhs: Box::new(expr),
        };
    }
    expr
}

impl Inner {
    /// Evaluate the same work on every shard.
    pub(crate) fn all(&self, work: Work) -> KResult<Vec<K>> {
        let (ticket, shards) = (ticket(work)?, self.shards());
        self.wait(try_join_all(shards.iter().map(|s| s.run(ticket.clone()))))
    }

    /// Stream `(Flip, key_count)` batches from all shards concurrently as they arrive.
    pub(crate) fn stream_all_flips(&self, work: Work, mut on_part: impl FnMut(Flip, usize) -> KResult<()>) -> KResult<()> {
        let (ticket, shards) = (ticket(work)?, self.shards());
        self.wait(async {
            let (tx, mut rx) = tokio::sync::mpsc::channel(shards.len().max(1) * 2);
            let producers = async {
                let res = try_join_all(shards.iter().map(|s| s.stream_flips(ticket.clone(), tx.clone()))).await;
                drop(tx);
                res
            };
            let consumer = async {
                while let Some((part, nkeys)) = rx.recv().await {
                    on_part(part, nkeys)?;
                }
                Ok::<(), KError>(())
            };
            tokio::try_join!(producers, consumer).map(|_| ())
        })
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
        let temp = resolve(m.temp).to_string();
        let mut empty = m.source.schema.clone();
        empty.cols.push(intern(ORD_COL));
        empty.vals.push(ColVec::Long(Attr::None, Arc::new(vec![])));
        self.load_all(&LoadReq::new(&temp, LoadMode::Replace, &K::Table(Arc::new(empty)), None))?;
        let (shards, slots) = (self.shards(), self.slots());
        let peers: Vec<String> = shards.iter().map(|s| s.addresses()).collect();
        let sends = shards.iter().enumerate().map(|(me, shard)| {
            let req = ShuffleReq {
                scan: augmented(m.source.name),
                key_cols: m.keys.iter().map(|&k| resolve(k).into()).collect(),
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

    fn with_moves<R>(&self, moves: &[Move], f: impl FnOnce() -> KResult<R>) -> KResult<R> {
        let result = moves.iter().try_for_each(|m| self.move_table(m)).and_then(|_| f());
        // temporary tables go whether or not the query worked
        let none = K::Null;
        for m in moves {
            let dropped = self.load_all(&LoadReq::new(resolve(m.temp), LoadMode::Drop, &none, None));
            if result.is_ok() {
                dropped?;
            }
        }
        result
    }

    /// Run `push`; `mirror` evaluates what the control node computes itself.
    pub(crate) fn run_push(&self, mirror: &mut Connection, push: &Push) -> KResult<K> {
        self.with_moves(&push.rel.moves, || self.run_kind(mirror, push))
    }

    /// Push `stages` (`xasc`/`xdesc`) and optional `top_k` (`sublist`) down to the shards and merge via `O(N log k)` k-way merge.
    pub(crate) fn run_sorted_push(&self, push: &Push, stages: &[(Vec<SymId>, bool)], top_k: Option<usize>) -> KResult<K> {
        self.with_moves(&push.rel.moves, || {
            let q = &push.query;
            match &push.kind {
                Kind::Replicated => {
                    let mut sent = q.clone();
                    *sent.from = source(&push.rel, false);
                    self.one(0, wrap_sort_stages(Expr::Query(Box::new(sent)), stages, top_k))
                }
                Kind::Route(shard) => {
                    let mut sent = q.clone();
                    *sent.from = source(&push.rel, false);
                    self.one(*shard, wrap_sort_stages(Expr::Query(Box::new(sent)), stages, top_k))
                }
                Kind::Ordered | Kind::ByKey => {
                    let is_ordered = matches!(push.kind, Kind::Ordered);
                    let mut sent = q.clone();
                    *sent.from = source(&push.rel, is_ordered);
                    sent.kind = QSqlKind::Select;
                    sent.limit = None;
                    if is_ordered && !sent.cols.is_empty() {
                        sent.cols.push(ColSpec { name: Some(intern(ORD_COL)), expr: Expr::Var(intern(ORD_COL)) });
                    }
                    let mut base = Expr::Query(Box::new(sent));
                    if is_ordered {
                        base = Expr::Binary {
                            op: intern("xasc"),
                            lhs: Box::new(Expr::Lit(K::Sym(intern(ORD_COL)))),
                            rhs: Box::new(base),
                        };
                    }
                    let shard_expr = wrap_sort_stages(base, stages, top_k);
                    let answers = self.all(Work::Eval(Box::new(shard_expr)))?;
                    let flips = answers.into_iter().map(flip_of).collect::<KResult<Vec<_>>>()?;
                    let nkeys = if is_ordered {
                        if q.cols.is_empty() { push.rel.nkeys } else { 0 }
                    } else {
                        flips[0].1
                    };
                    let parts: Vec<Flip> = flips.into_iter().map(|f| f.0).collect();
                    let ref_flip = parts.iter().find(|p| p.nrows() > 0).unwrap_or(&parts[0]);

                    let mut specs_idx: Vec<(usize, bool)> = Vec::new();
                    let mut first_asc_sym: Option<SymId> = None;
                    let mut saw_nonempty = false;
                    for (cols, desc) in stages {
                        if cols.is_empty() {
                            continue;
                        }
                        for (j, &s) in cols.iter().enumerate() {
                            let ci = ref_flip.col_idx(s).ok_or_else(|| KError::value(resolve(s)))?;
                            if !saw_nonempty && j == 0 && !*desc {
                                first_asc_sym = Some(s);
                            }
                            if !specs_idx.iter().any(|&(c, _)| c == ci) {
                                specs_idx.push((ci, *desc));
                            }
                        }
                        saw_nonempty = true;
                    }
                    if is_ordered {
                        let ord_ci = ref_flip
                            .col_idx(intern(ORD_COL))
                            .ok_or_else(|| KError::new("cluster", "a shard returned no row ordinals"))?;
                        specs_idx.push((ord_ci, false));
                        let mut merged = merge_sorted_flips(parts, &specs_idx, top_k)?;
                        merged.cols.remove(ord_ci);
                        merged.vals.remove(ord_ci);
                        if top_k.is_none() {
                            if let Some(ci) = first_asc_sym.and_then(|s| merged.col_idx(s)) {
                                merged.vals[ci].set_attr_unchecked(Attr::Sorted);
                            }
                        }
                        Ok(rekey(merged, nkeys))
                    } else {
                        for ki in 0..nkeys {
                            if !specs_idx.iter().any(|&(c, _)| c == ki) {
                                specs_idx.push((ki, false));
                            }
                        }
                        let mut merged = merge_sorted_flips(parts, &specs_idx, top_k)?;
                        if top_k.is_none() {
                            if let Some(ci) = first_asc_sym.and_then(|s| merged.col_idx(s)) {
                                merged.vals[ci].set_attr_unchecked(Attr::Sorted);
                            } else if !saw_nonempty && nkeys == 1 {
                                merged.vals[0].set_attr_unchecked(Attr::Sorted);
                            }
                        }
                        Ok(rekey(merged, nkeys))
                    }
                }
                Kind::TwoPhase(_) => unreachable!(),
            }
        })
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
            let sent = with(&|s| {
                *s.from = source(&push.rel, false);
                if push.exec_single {
                    s.cols[0].name = None;
                }
            });
            // (`exec` takes no notice of the mark)
            match q.distinct && q.kind == QSqlKind::Exec {
                true => Expr::Unary { op: intern("distinct"), arg: Box::new(sent) },
                false => sent,
            }
        };
        match &push.kind {
            Kind::Replicated => self.one(0, whole()),
            Kind::Route(shard) => self.one(*shard, whole()),
            Kind::ByKey => self.by_key(push, with(&|s| {
                *s.from = source(&push.rel, false);
                s.kind = QSqlKind::Select;
                s.limit = None;
            })),
            Kind::Ordered => {
                let ord = intern(ORD_COL);
                let sent = with(&|s| {
                    *s.from = source(&push.rel, true);
                    s.kind = QSqlKind::Select;
                    if !s.cols.is_empty() {
                        s.cols.push(ColSpec { name: Some(ord), expr: Expr::Var(ord) });
                    }
                    // `distinct` applies to all rows together; a limit taken of each shard's rows still holds the answer
                    if s.distinct {
                        s.distinct = false;
                        s.limit = None;
                    }
                });
                // each shard sends only the first of its own equal rows, compared without their ordinals
                let sent = match q.distinct && !q.cols.is_empty() {
                    true => Expr::Apply {
                        target: Box::new(Expr::Var(intern(".kix.firsts"))),
                        args: vec![Some(sent), Some(Expr::Lit(K::Sym(ord)))],
                    },
                    false => sent,
                };
                self.ordered(push, sent)
            }
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

    fn pos_limit(&self, push: &Push) -> Option<usize> {
        match push.query.limit.as_deref() {
            Some(Expr::Lit(n)) => n.as_i64().ok().filter(|&m| m >= 0).map(|m| m as usize),
            _ => None,
        }
    }

    fn by_key(&self, push: &Push, sent: Expr) -> KResult<K> {
        let answers = self.all(Work::Eval(Box::new(sent)))?;
        let flips = answers.into_iter().map(flip_of).collect::<KResult<Vec<_>>>()?;
        let nkeys = flips[0].1;
        let specs: Vec<(usize, bool)> = (0..nkeys).map(|i| (i, false)).collect();
        let limit = (push.query.kind != QSqlKind::Exec).then(|| self.pos_limit(push)).flatten();
        let mut sorted = merge_sorted_flips(flips.into_iter().map(|f| f.0).collect(), &specs, limit)?;
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
        let q = &push.query;
        let limit = (!q.distinct && q.kind != QSqlKind::Exec).then(|| self.pos_limit(push)).flatten();
        let rows = in_order_limit(flips, limit)?;
        if q.kind == QSqlKind::Exec {
            return Ok(if push.exec_single {
                let values = K::Vector(rows.vals[0].clone());
                if q.distinct { distinct_op(&values)? } else { values }
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
        let grouped = push.query.by.is_some();
        let mut merger = Merger::new(aggs);
        self.stream_all_flips(Work::Eval(Box::new(sent)), |part, nkeys| merger.push(part, nkeys))?;
        let Some((keys, leaves)) = merger.finish(grouped) else {
            // no group anywhere: the answer has the shape the query has on the empty tables
            return mirror.eval_expr(&push.original);
        };
        let nkeys = keys.ncols();
        let ngroups = keys.nrows();
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
        for n in &mut val_names {
            if keys.cols.contains(n) {
                *n = intern(&format!("{}_1", resolve(*n)));
            }
        }
        let cols = values
            .into_iter()
            .map(|v| match v {
                K::Vector(cv) => cv,
                atom => ColVec::repeat(atom, ngroups),
            })
            .collect();
        let vals = Flip::new(val_names, cols)?;
        if push.query.kind == QSqlKind::Exec {
            return Ok(exec_by_result(&KeyedTable::from_flips(keys, vals)));
        }
        let mut cols = keys.cols;
        cols.extend(vals.cols);
        let mut columns = keys.vals;
        columns.extend(vals.vals);
        self.limit(push, rekey(Flip::new(cols, columns)?, nkeys))
    }
}
