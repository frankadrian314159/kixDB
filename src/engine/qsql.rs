//! `qSQL` execution (`select`, `exec`, `update`, `delete`), `fby` filter-by-group, and `insert`/`upsert`.
use crate::ast::{ColSpec, Expr, QSql, QSqlKind};
use crate::engine::ops::{
    aggregate_by_ids, aggregate_rows, count_by_ids, distinct_op, eval_binary_prim, eval_unary_prim, first_rows,
    group_ids, groups_from_ids, sort_order, weighted_by_ids,
};
use crate::engine::joins::match_keys;
use crate::engine::par::{self, true_rows};
use crate::engine::writes::row_changes;
use crate::engine::{Engine, RowNumbers, Write};
use crate::error::{KError, KResult};
use crate::parser::infer_col_name;
use crate::sym::{intern, resolve, SymId};
use crate::types::*;
use rayon::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

/// Whether the expression applies something item by item (`f each x`, `f'[x]`).
fn applies_each(e: &Expr) -> bool {
    e.any(&mut |x| match x {
        Expr::Adverb { kind: AdverbKind::Each, .. } => true,
        Expr::Binary { op, .. } => matches!(resolve(*op), "each" | "peach"),
        _ => false,
    })
}

/// Verbs whose argument names things only at run time.
pub const DYNAMIC: [&str; 6] = ["value", "get", "eval", "reval", "system", ".Q.v"];

/// Verbs that give one value for the values of a group.
const AGGREGATES: [&str; 13] =
    ["sum", "count", "avg", "min", "max", "first", "last", "med", "prd", "var", "dev", "svar", "sdev"];
/// Verbs that work item by item, with one argument and with two.
const ITEMWISE_UNARY: [&str; 12] =
    ["-", "neg", "abs", "sqrt", "exp", "log", "floor", "ceiling", "reciprocal", "signum", "null", "not"];
const ITEMWISE_BINARY: [&str; 17] =
    ["+", "-", "*", "%", "=", "<>", "<", "<=", ">", ">=", "&", "|", "and", "or", "div", "mod", "^"];

fn is_one_of(op: SymId, verbs: &[&str]) -> bool {
    verbs.contains(&resolve(op))
}

/// Whether row `r` of `e` depends only on row `r` of `flip`'s columns.
fn row_by_row(flip: &Flip, e: &Expr) -> bool {
    let rows = |e: &Expr| row_by_row(flip, e);
    match e {
        Expr::Lit(k) => k.is_atom(),
        Expr::Var(s) => flip.col(*s).is_some() || resolve(*s) == "i",
        Expr::Field(base, _) => matches!(&**base, Expr::Var(s) if flip.col(*s).is_some()),
        Expr::Unary { op, arg } => is_one_of(*op, &ITEMWISE_UNARY) && rows(arg),
        // the set that `in` looks in is the same for every row
        Expr::Binary { op, lhs, rhs } if resolve(*op) == "in" => {
            rows(lhs) && matches!(&**rhs, Expr::Lit(_) | Expr::Var(_)) && !matches!(&**rhs, Expr::Var(s) if flip.col(*s).is_some())
        }
        Expr::Binary { op, lhs, rhs } if resolve(*op) == "within" => rows(lhs) && matches!(&**rhs, Expr::Lit(_)),
        Expr::Binary { op, lhs, rhs } => is_one_of(*op, &ITEMWISE_BINARY) && rows(lhs) && rows(rhs),
        _ => false,
    }
}

impl<'a> Engine<'a> {
    /// The names that `exprs` can read, or `None` when they reach names at run time (through a `DYNAMIC` verb or a
    /// function variable) and so may read any column.
    fn names_read<'e>(&self,exprs: impl IntoIterator<Item = &'e Expr>) -> Option<Vec<SymId>> {
        let mut names = Vec::new();
        for e in exprs {
            e.any(&mut |x| {
                if let Expr::Var(s)
                | Expr::Field(_, s)
                | Expr::Unary { op: s, .. }
                | Expr::Binary { op: s, .. }
                | Expr::Assign { name: s, .. }
                | Expr::IndexAssign { name: s, .. }
                | Expr::Lit(K::Func(Func::Prim(s))) = x
                {
                    names.push(*s);
                }
                false
            });
        }
        let dynamic = names.iter().any(|&n| DYNAMIC.contains(&resolve(n)) || self.reaches_names(n));
        (!dynamic).then_some(names)
    }

    /// The columns of `flip` that `exprs` can read (shared, not copied); at least one, which gives the row count.
    fn columns_read<'e>(&self, flip: &Flip, exprs: impl IntoIterator<Item = &'e Expr>) -> Flip {
        let Some(names) = self.names_read(exprs) else { return flip.clone() };
        let mut keep: Vec<usize> = (0..flip.ncols()).filter(|&c| names.contains(&flip.cols[c])).collect();
        if keep.is_empty() && flip.ncols() > 0 {
            keep.push((0..flip.ncols()).min_by_key(|&c| flip.vals[c].approx_bytes()).unwrap_or(0));
        }
        Flip { cols: keep.iter().map(|&c| flip.cols[c]).collect(), vals: keep.iter().map(|&c| flip.vals[c].clone()).collect() }
    }

    /// Evaluate `e` over `flip` (`rows`), reusing pure row-wise subexpressions already in `cache`.
    fn eval_cached(&mut self, flip: &Flip, rows: &RowNumbers, e: &Expr, cache: &mut Vec<(Expr, K)>) -> KResult<K> {
        let cacheable = row_by_row(flip, e) && !matches!(e, Expr::Lit(_) | Expr::Var(_));
        if cacheable {
            if let Some((_, v)) = cache.iter().find(|(k, _)| k == e) {
                return Ok(v.clone());
            }
        }
        let val = match e {
            Expr::Unary { op, arg } if is_one_of(*op, &ITEMWISE_UNARY) || is_one_of(*op, &AGGREGATES) => {
                let x = self.eval_cached(flip, rows, arg, cache)?;
                self.eval_unary_dispatch(*op, &x)?
            }
            Expr::Binary { op, lhs, rhs } if is_one_of(*op, &ITEMWISE_BINARY) || is_one_of(*op, &["wsum", "wavg"]) => {
                let r = self.eval_cached(flip, rows, rhs, cache)?;
                let l = self.eval_cached(flip, rows, lhs, cache)?;
                self.eval_binary_dispatch(resolve(*op), &l, &r)?
            }
            _ => self.eval_in_rows(flip, rows.clone(), e)?,
        };
        if cacheable && cache.len() < 32 {
            cache.push((e.clone(), val.clone()));
        }
        Ok(val)
    }

    /// The values of `e` on all rows of `flip` (numbered `rows`), when each depends on its own row only.
    fn row_values(&mut self, flip: &Flip, rows: &RowNumbers, e: &Expr, cache: &mut Vec<(Expr, K)>) -> Option<ColVec> {
        if !row_by_row(flip, e) {
            return None;
        }
        match self.eval_cached(flip, rows, e, cache).ok()? {
            // a computed general list may not have the type that a group's own values would
            K::Vector(v) if v.len() == flip.nrows() && (matches!(e, Expr::Var(_)) || !v.is_list()) => Some(v),
            _ => None,
        }
    }

    /// `e` for all `ngroups` groups of `flip`'s rows at once: standard aggregates stream over `gids` (`u32`) in one pass,
    /// and group row lists are built only if an aggregate (`distinct`, `med`, `var`/`dev`, `General`) needs them.
    #[allow(clippy::too_many_arguments)]
    fn by_group_ids(
        &mut self,
        flip: &Flip,
        rows: &RowNumbers,
        gids: &[u32],
        ngroups: usize,
        buckets: usize,
        first_in_flip: &[usize],
        groups_cell: &mut Option<Vec<Vec<usize>>>,
        cache: &mut Vec<(Expr, K)>,
        e: &Expr,
    ) -> Option<K> {
        let atoms = |k: K| (!matches!(&k, K::Vector(v) if v.is_list())).then_some(k);
        match e {
            Expr::Lit(k) if k.is_atom() => Some(k.clone()),
            Expr::Unary { op, arg } if is_one_of(*op, &AGGREGATES) => {
                let (arg, distinct) = match &**arg {
                    Expr::Unary { op, arg } if resolve(*op) == "distinct" => (&**arg, true),
                    other => (other, false),
                };
                let verb = resolve(*op);
                if verb == "count" && !distinct && matches!(arg, Expr::Var(s) if resolve(*s) == "i" || flip.col(*s).is_some()) {
                    return Some(K::Vector(count_by_ids(gids, ngroups, buckets)));
                }
                let values = self.row_values(flip, rows, arg, cache)?;
                if !distinct {
                    if let Some(cv) = aggregate_by_ids(verb, &values, gids, ngroups, buckets, first_in_flip) {
                        return Some(K::Vector(cv));
                    }
                }
                let groups = groups_cell.get_or_insert_with(|| groups_from_ids(gids, ngroups));
                let eval_grp = |g: &[usize]| match distinct {
                    true => eval_unary_prim(verb, &distinct_op(&K::Vector(values.gather(g)))?),
                    false => aggregate_rows(verb, &values, g),
                };
                let items: KResult<Vec<K>> = match par::split(flip.nrows()) {
                    false => groups.iter().map(|g| eval_grp(g)).collect(),
                    true => groups.par_iter().map(|g| eval_grp(g)).collect(),
                };
                Some(K::Vector(ColVec::from_atoms(items.ok()?)))
            }
            Expr::Binary { op, lhs, rhs } if is_one_of(*op, &["wsum", "wavg"]) => {
                let (r, l) = (self.row_values(flip, rows, rhs, cache)?, self.row_values(flip, rows, lhs, cache)?);
                let verb = resolve(*op);
                if let Some(cv) = weighted_by_ids(verb == "wavg", &l, &r, gids, ngroups, buckets) {
                    return Some(K::Vector(cv));
                }
                let groups = groups_cell.get_or_insert_with(|| groups_from_ids(gids, ngroups));
                let eval_grp = |g: &[usize]| eval_binary_prim(verb, &K::Vector(l.gather(g)), &K::Vector(r.gather(g)));
                let items: KResult<Vec<K>> = match par::split(flip.nrows()) {
                    false => groups.iter().map(|g| eval_grp(g)).collect(),
                    true => groups.par_iter().map(|g| eval_grp(g)).collect(),
                };
                Some(K::Vector(ColVec::from_atoms(items.ok()?)))
            }
            Expr::Unary { op, arg } if is_one_of(*op, &ITEMWISE_UNARY) => {
                let x = atoms(self.by_group_ids(flip, rows, gids, ngroups, buckets, first_in_flip, groups_cell, cache, arg)?)?;
                let verb = resolve(*op);
                eval_unary_prim(verb, &x).ok()
            }
            Expr::Binary { op, lhs, rhs } if is_one_of(*op, &ITEMWISE_BINARY) => {
                let r = atoms(self.by_group_ids(flip, rows, gids, ngroups, buckets, first_in_flip, groups_cell, cache, rhs)?)?;
                let l = atoms(self.by_group_ids(flip, rows, gids, ngroups, buckets, first_in_flip, groups_cell, cache, lhs)?)?;
                let verb = resolve(*op);
                eval_binary_prim(verb, &l, &r).ok()
            }
            _ => None,
        }
    }

    pub fn eval_qsql(&mut self, q: &QSql) -> KResult<K> {
        // `delete a, b from `.` removes names from a namespace
        if let (QSqlKind::DeleteCols, Expr::Lit(K::Sym(namespace))) = (&q.kind, &*q.from) {
            let name = resolve(*namespace);
            let is_namespace = name == "." || name.starts_with('.') && !self.ws.vars.contains_key(namespace) && self.namespace(name).is_some();
            if is_namespace {
                let prefix = name.trim_end_matches('.');
                let full = |n: SymId| if prefix.is_empty() { n } else { intern(&format!("{prefix}.{}", resolve(n))) };
                let named = q.cols.iter().filter_map(|c| c.name.or_else(|| infer_col_name(&c.expr))).map(full);
                let held: Vec<SymId> = named.filter(|n| self.ws.vars.contains_key(n) || self.ws.views.contains_key(n)).collect();
                self.set_globals(held.into_iter().map(|n| (n, None)).collect())?;
                return Ok(K::Sym(*namespace));
            }
        }
        // Check if `from` is a symbol `` `table `` (in-place `update` / `delete`, or reading a named table)
        let mut inplace_target: Option<SymId> = None;
        // the names a `select`/`exec` with an explicit column list mentions: all a partitioned table has to load
        let outputs = || q.cols.iter().chain(q.by.iter().flatten()).map(|c| &c.expr);
        let reads_listed = matches!(q.kind, QSqlKind::Select | QSqlKind::Exec) && !q.cols.is_empty();
        let needed = if reads_listed { self.names_read(outputs().chain(&q.where_clauses)) } else { None };
        let can_prefilter = matches!(q.kind, QSqlKind::Select | QSqlKind::Exec) && !query_has_bare_i(q);
        let source_k = match &*q.from {
            Expr::Lit(K::Sym(tname)) => {
                if matches!(q.kind, QSqlKind::Update | QSqlKind::DeleteRows | QSqlKind::DeleteCols) {
                    inplace_target = Some(*tname);
                }
                self.resolve_table_for_query(*tname, &q.where_clauses, needed.as_deref(), can_prefilter)?
            }
            Expr::Var(tname) => {
                // If `tname` is a mounted partitioned HDB table, prune partitions using `where_clauses`!
                if self.has_partitioned(*tname) {
                    self.resolve_table_for_query(*tname, &q.where_clauses, needed.as_deref(), can_prefilter)?
                } else {
                    self.eval_expr(&q.from)?
                }
            }
            other => self.eval_expr(other)?,
        };
        // a symbol held in a variable names the table too
        let source_k = match source_k {
            K::Sym(name) => {
                if matches!(q.kind, QSqlKind::Update | QSqlKind::DeleteRows | QSqlKind::DeleteCols) {
                    inplace_target = Some(name);
                }
                // (the name is a global's, whatever locals are called)
                let locals = std::mem::take(&mut self.locals);
                let table = self.resolve_table_for_query(name, &q.where_clauses, needed.as_deref(), can_prefilter);
                self.locals = locals;
                table?
            }
            table => table,
        };

        let (base_flip, orig_key_cols) = match &source_k {
            K::Table(t) => ((**t).clone(), None),
            K::KeyedTable(kt) => (kt.unkey(), Some(kt.keys.cols.clone())),
            _ => return Err(KError::ty("qSQL 'from' target must evaluate to a table")),
        };

        // `delete col1, col2 from t`
        if q.kind == QSqlKind::DeleteCols {
            let mut drop_names = Vec::new();
            for cs in &q.cols {
                if let Some(n) = cs.name.or_else(|| infer_col_name(&cs.expr)) {
                    drop_names.push(n);
                }
            }
            let mut cols = Vec::new();
            let mut vals = Vec::new();
            for (c, v) in base_flip.cols.iter().zip(&base_flip.vals) {
                if !drop_names.contains(c) {
                    cols.push(*c);
                    vals.push(v.clone());
                }
            }
            let res_flip = Flip { cols, vals };
            let out_k = match orig_key_cols {
                Some(ks) => {
                    let kept_ks: Vec<SymId> = ks.into_iter().filter(|k| !drop_names.contains(k)).collect();
                    KeyedTable::from_flip_key_names(&res_flip, &kept_ks)?
                }
                None => K::Table(Arc::new(res_flip)),
            };
            if let Some(sym) = inplace_target {
                self.set_named(sym, out_k)?;
                return Ok(K::Sym(sym));
            }
            return Ok(out_k);
        }

        // Sequential `where` filtering: each sub-clause sees only rows surviving prior sub-clauses!
        // `active_rows == None` represents all `0..total_nrows` rows without cloning `base_flip` up front.
        let total_nrows = base_flip.nrows();
        let mut active_rows: RowNumbers = None;
        let single_key_kt = match &source_k {
            K::KeyedTable(kt) if kt.keys.ncols() == 1 => Some(&**kt),
            _ => None,
        };

        for wc in &q.where_clauses {
            if total_nrows == 0 || active_rows.as_ref().is_some_and(|r| r.is_empty()) {
                break;
            }
            let active = active_rows.as_ref().map(|r| r.as_slice());
            if let Some(indexed) = crate::index::try_filter_where(&base_flip, single_key_kt, active, wc, &mut |e| self.eval_expr(e))? {
                active_rows = Some(Arc::new(indexed));
                continue;
            }
            // while most rows are kept, testing every row costs less than copying out the ones that are
            if let Some(cur) = active_rows.as_ref().filter(|cur| cur.len() * 2 >= total_nrows && row_by_row(&base_flip, wc)) {
                if let Ok(K::Vector(ColVec::Bool(_, test))) = self.eval_in_table_scope(&base_flip, wc) {
                    if test.len() == total_nrows {
                        active_rows = Some(Arc::new(cur.iter().copied().filter(|&r| test.get(r)).collect()));
                        continue;
                    }
                }
            }
            match &active_rows {
                None => {
                    let pred_k = self.eval_in_table_scope(&base_flip, wc)?;
                    active_rows = Some(Arc::new(extract_where_indices(&pred_k, total_nrows)?));
                }
                Some(cur) => {
                    let sub_flip = self.columns_read(&base_flip, [wc]).gather(cur);
                    let pred_k = self.eval_in_rows(&sub_flip, active_rows.clone(), wc)?;
                    let keep_local = extract_where_indices(&pred_k, sub_flip.nrows())?;
                    active_rows = Some(Arc::new(keep_local.into_iter().map(|li| cur[li]).collect()));
                }
            }
        }

        match q.kind {
            QSqlKind::DeleteRows => {
                let res_flip = match &active_rows {
                    None => base_flip.take(0),
                    Some(rows) => {
                        let mut drop_mask = vec![false; total_nrows];
                        for &r in rows.iter() {
                            drop_mask[r] = true;
                        }
                        let keep_idx: Vec<usize> = (0..total_nrows).filter(|&r| !drop_mask[r]).collect();
                        base_flip.gather(&keep_idx)
                    }
                };
                let out_k = restore_keys(res_flip, orig_key_cols)?;
                if let Some(sym) = inplace_target {
                    self.set_named(sym, out_k)?;
                    return Ok(K::Sym(sym));
                }
                Ok(out_k)
            }

            QSqlKind::Update => {
                let selected = active_rows.unwrap_or_else(|| Arc::new((0..total_nrows).collect()));
                let out_flip = self.exec_update(&base_flip, &selected, &q.cols, q.by.as_deref())?;
                let out_k = restore_keys(out_flip, orig_key_cols)?;
                if let Some(sym) = inplace_target {
                    self.set_named(sym, out_k)?;
                    return Ok(K::Sym(sym));
                }
                Ok(out_k)
            }

            QSqlKind::Select | QSqlKind::Exec => {
                // without a column list the result has every column
                let read = if q.cols.is_empty() { base_flip.clone() } else { self.columns_read(&base_flip, outputs()) };
                // A query that groups most of the rows by columns of theirs reads them where they are.
                let plain = |b: &ColSpec| matches!(&b.expr, Expr::Var(v) if read.col(*v).is_some() && b.name.is_none_or(|name| name == *v));
                let kept = active_rows.as_ref().filter(|rows| {
                    let grouped = q.by.as_ref().is_some_and(|by| !by.is_empty() && by.iter().all(plain));
                    q.kind == QSqlKind::Select && grouped && !q.cols.is_empty() && !rows.is_empty() && rows.len() * 2 >= total_nrows
                });
                let filtered = match &active_rows {
                    Some(rows) if kept.is_none() => read.gather(rows),
                    _ => read,
                };
                if q.kind == QSqlKind::Exec {
                    return self.exec_query_exec(&filtered, &active_rows, &q.cols, q.by.as_deref());
                }
                let numbers = if kept.is_some() { &None } else { &active_rows };
                let kept = kept.map(|rows| rows.as_slice());
                let mut res = self.exec_select(&filtered, numbers, kept, &q.cols, q.by.as_deref(), orig_key_cols)?;
                if q.distinct {
                    if let K::Table(_) = &res {
                        res = distinct_op(&res)?;
                    }
                }
                if let Some(lim_expr) = &q.limit {
                    let n = self.eval_expr(lim_expr)?.as_i64()?;
                    res = crate::engine::ops::sublist_op(&K::Long(n), &res)?;
                }
                Ok(res)
            }

            QSqlKind::DeleteCols => unreachable!(),
        }
    }

    fn exec_select(
        &mut self,
        filtered: &Flip,
        rows: &RowNumbers,
        // the rows of `filtered` to group, where it holds others too (`by` then names columns of it)
        kept: Option<&[usize]>,
        cols: &[ColSpec],
        by: Option<&[ColSpec]>,
        orig_keys: Option<Vec<SymId>>,
    ) -> KResult<K> {
        match by {
            None => {
                if cols.is_empty() {
                    return restore_keys(filtered.clone(), orig_keys);
                }
                let mut col_names = resolve_col_names(cols);
                let _ = &mut col_names;
                let mut cache = Vec::new();
                let mut evaluated = Vec::with_capacity(cols.len());
                for cs in cols {
                    evaluated.push(self.eval_cached(filtered, rows, &cs.expr, &mut cache)?);
                }

                // Determine if any column evaluated to a vector matching `filtered.nrows()` (projection) vs all atoms (1-row summary)!
                let any_vec = evaluated.iter().any(|k| matches!(k, K::Vector(cv) if !matches!(cv, ColVec::Char(_, _)) || filtered.nrows() > 1 && cv.len() == filtered.nrows()));
                // Wait: a char column `ColVec::Char` of length `nrows` IS a vector column! Check more carefully:
                let is_col_vec = |k: &K| -> bool {
                    match k {
                        K::Vector(ColVec::Char(_, _)) => false, // wait: a string literal `"abc"` is atom-like in qSQL (`select tag: "abc" from t`? No, in q `"abc"` is 3 chars! What if `c` is a `Char` column in `filtered`? Check if `k` came from a column or has len == nrows!)
                        K::Vector(_) => true,
                        _ => false,
                    }
                };
                // A list of characters that a verb made one row at a time (`first each name`) is a column too
                let one_a_row = |chars: usize, e: &Expr| chars == filtered.nrows() && applies_each(e);
                let has_vec = evaluated.iter().zip(cols).any(|(k, cs)| {
                    if is_col_vec(k) {
                        true
                    } else if let K::Vector(ColVec::Char(_, bs)) = k {
                        // Treat as column vector if expression referenced a char column of `filtered`
                        infer_col_name(&cs.expr)
                            .and_then(|n| filtered.col(n))
                            .is_some_and(|c| matches!(c, ColVec::Char(_, _)) && bs.len() == filtered.nrows())
                            || one_a_row(bs.len(), &cs.expr)
                    } else {
                        false
                    }
                });
                let _ = any_vec;

                let mut out_vals = Vec::with_capacity(cols.len());
                if !has_vec {
                    // Every expression produced an atom (or string): 1-row summary table!
                    for k in evaluated {
                        out_vals.push(ColVec::from_atoms(vec![k]));
                    }
                } else {
                    let target_len = filtered.nrows();
                    for (k, cs) in evaluated.into_iter().zip(cols) {
                        match k {
                            K::Vector(ColVec::Char(_, ref bs))
                                if !infer_col_name(&cs.expr)
                                    .and_then(|n| filtered.col(n))
                                    .is_some_and(|c| matches!(c, ColVec::Char(_, _)))
                                    && !one_a_row(bs.len(), &cs.expr) =>
                            {
                                out_vals.push(ColVec::General(Attr::None, Arc::new(vec![k; target_len])));
                            }
                            K::Vector(cv) => {
                                if cv.len() == target_len {
                                    out_vals.push(cv);
                                } else {
                                    return Err(KError::length(format!(
                                        "select column length {} does not match table length {target_len}",
                                        cv.len()
                                    )));
                                }
                            }
                            atom => {
                                out_vals.push(ColVec::repeat(atom, target_len));
                            }
                        }
                    }
                }
                Ok(K::Table(Arc::new(Flip::new(col_names, out_vals)?)))
            }

            Some(by_specs) => {
                // `select [cols] by [by_specs] from filtered`
                // Special case in kdb+: `select by sym from t` (empty `cols`) takes the `last` of every non-key column per group!
                let effective_cols: Vec<ColSpec>;
                let cols_ref: &[ColSpec] = if cols.is_empty() {
                    let by_names: Vec<SymId> = resolve_col_names(by_specs);
                    effective_cols = filtered
                        .cols
                        .iter()
                        .filter(|c| !by_names.contains(c))
                        .map(|&c| ColSpec {
                            name: Some(c),
                            expr: Expr::Unary { op: intern("last"), arg: Box::new(Expr::Var(c)) },
                        })
                        .collect();
                    &effective_cols
                } else {
                    cols
                };

                let by_names = resolve_col_names(by_specs);
                let mut val_names = resolve_col_names(cols_ref);
                // Ensure value column names do not collide with key column names
                for vn in &mut val_names {
                    if by_names.contains(vn) {
                        *vn = intern(&format!("{}_1", resolve(*vn)));
                    }
                }

                // Evaluate `by` expressions over `filtered`
                let mut by_vecs = Vec::with_capacity(by_specs.len());
                for bs in by_specs {
                    let v = self.eval_in_rows(filtered, rows.clone(), &bs.expr)?;
                    match v {
                        K::Vector(cv) if cv.len() == filtered.nrows() => by_vecs.push(match kept {
                            Some(kept) => cv.gather(kept),
                            None => cv,
                        }),
                        atom if atom.is_atom() => by_vecs.push(ColVec::repeat(atom, filtered.nrows())),
                        _ => return Err(KError::length("by expression length must match table row count")),
                    }
                }

                if by_specs.is_empty() {
                    // `select ... by from t` (single group over whole table)
                    let mut cache = Vec::new();
                    let mut out_vals = Vec::with_capacity(cols_ref.len());
                    for cs in cols_ref {
                        let k = self.eval_cached(filtered, rows, &cs.expr, &mut cache)?;
                        out_vals.push(ColVec::from_atoms(vec![k]));
                    }
                    return Ok(K::Table(Arc::new(Flip::new(val_names, out_vals)?)));
                }

                let nrows_in = kept.map_or(filtered.nrows(), <[usize]>::len);
                if nrows_in == 0 {
                    // Empty input with `by`: probe empty sub-flip to infer result types!
                    let mut key_vals: Vec<ColVec> = by_vecs.iter().map(|bv| bv.take(0)).collect();
                    if by_vecs.len() == 1 && !key_vals.is_empty() {
                        key_vals[0].set_attr_unchecked(Attr::Sorted);
                    }
                    let mut empty_vals = Vec::with_capacity(cols_ref.len());
                    for cs in cols_ref {
                        let sample = self.eval_in_rows(filtered, rows.clone(), &cs.expr)?;
                        empty_vals.push(ColVec::from_atoms(vec![sample]).take(0));
                    }
                    let keys_flip = Flip::new(by_names, key_vals)?;
                    let vals_flip = Flip::new(val_names, empty_vals)?;
                    return Ok(K::KeyedTable(Arc::new(KeyedTable::from_flips(keys_flip, vals_flip))));
                }

                // Group row indices by composite `by` key without materializing per-group row lists up front
                let ids = group_ids(&by_vecs, nrows_in);
                let first = first_rows(&ids);
                let ngroups = first.len();

                // Sort groups ascending by `by_vecs` keys (matching kdb+ `select ... by k from t`!)
                let keys: Vec<ColVec> = by_vecs.iter().map(|bv| bv.gather(&first)).collect();
                let group_order = sort_order(&keys, ngroups);
                let mut key_vals: Vec<ColVec> = keys.iter().map(|k| k.gather(&group_order)).collect();
                if by_vecs.len() == 1 && !key_vals.is_empty() {
                    key_vals[0].set_attr_unchecked(Attr::Sorted);
                }

                let needs_ids = cols_ref.iter().any(|cs| !matches!(cs.expr, Expr::Lit(ref k) if k.is_atom()));
                let mut gids: Vec<u32> = Vec::new();
                let mut sorted_first = Vec::new();
                let buckets = ngroups + usize::from(kept.is_some());
                if needs_ids {
                    let mut remap = vec![0u32; ngroups];
                    for (sorted_g, &orig_g) in group_order.iter().enumerate() {
                        remap[orig_g] = sorted_g as u32;
                    }
                    gids = match kept {
                        None => par::map(&ids, |&g| remap[g]),
                        Some(k) => {
                            let mut full = vec![ngroups as u32; filtered.nrows()];
                            for (j, &r) in k.iter().enumerate() {
                                full[r] = remap[ids[j]];
                            }
                            full
                        }
                    };
                    drop(ids);
                    sorted_first = group_order.iter().map(|&g| kept.map_or(first[g], |k| k[first[g]])).collect();
                }

                // bind the `by` names too, so `select ... by bar: 5 xbar t` can reference `bar`
                let mut scope = filtered.clone();
                // (where the rows grouped are among others, the `by` columns are there already)
                for (bn, bv) in by_names.iter().zip(by_vecs).filter(|_| kept.is_none()) {
                    match scope.col_idx(*bn) {
                        Some(pos) => scope.vals[pos] = bv,
                        None => {
                            scope.cols.push(*bn);
                            scope.vals.push(bv);
                        }
                    }
                }
                // columns made of aggregates are computed for all groups at once, the others group by group
                let mut groups_cell: Option<Vec<Vec<usize>>> = None;
                let mut cache: Vec<(Expr, K)> = Vec::new();
                let at_once: Vec<Option<ColVec>> = cols_ref
                    .iter()
                    .map(|cs| {
                        match self.by_group_ids(
                            &scope,
                            rows,
                            &gids,
                            ngroups,
                            buckets,
                            &sorted_first,
                            &mut groups_cell,
                            &mut cache,
                            &cs.expr,
                        ) {
                            Some(K::Vector(v)) if v.len() == ngroups => Some(v),
                            // the same for every group, as in `select ok: 1b by ...`
                            Some(atom) if atom.is_atom() => Some(ColVec::repeat(atom, ngroups)),
                            _ => None,
                        }
                    })
                    .collect();
                let mut cells: Vec<Vec<K>> = vec![Vec::new(); cols_ref.len()];
                if at_once.iter().any(Option::is_none) {
                    let sorted_groups = groups_cell.get_or_insert_with(|| groups_from_ids(&gids, ngroups));
                    for grp_idx in sorted_groups.iter() {
                        let (sub, numbers) = (scope.gather(grp_idx), numbers_of(rows, grp_idx));
                        for (ci, cs) in cols_ref.iter().enumerate().filter(|(ci, _)| at_once[*ci].is_none()) {
                            cells[ci].push(self.eval_in_rows(&sub, numbers.clone(), &cs.expr)?);
                        }
                    }
                }

                let out_vals: Vec<ColVec> =
                    at_once.into_iter().zip(cells).map(|(v, c)| v.unwrap_or_else(|| ColVec::from_atoms(c))).collect();
                let keys_flip = Flip::new(by_names, key_vals)?;
                let vals_flip = Flip::new(val_names, out_vals)?;
                Ok(K::KeyedTable(Arc::new(KeyedTable::from_flips(keys_flip, vals_flip))))
            }
        }
    }

    fn exec_query_exec(&mut self, filtered: &Flip, rows: &RowNumbers, cols: &[ColSpec], by: Option<&[ColSpec]>) -> KResult<K> {
        if let Some(by_specs) = by {
            // `exec val by key from t` -> dictionary `keys ! vals`
            let tbl = self.exec_select(filtered, rows, None, cols, Some(by_specs), None)?;
            if let K::KeyedTable(kt) = &tbl {
                return Ok(exec_by_result(kt));
            }
            return Ok(tbl);
        }

        if cols.is_empty() {
            return Ok(crate::engine::ops::first_op(&K::Table(Arc::new(filtered.clone()))));
        }
        if cols.len() == 1 && cols[0].name.is_none() {
            return self.eval_in_rows(filtered, rows.clone(), &cols[0].expr);
        }
        let names = resolve_col_names(cols);
        let mut vals = Vec::with_capacity(cols.len());
        for cs in cols {
            vals.push(self.eval_in_rows(filtered, rows.clone(), &cs.expr)?);
        }
        Ok(K::Dict(Arc::new(Dict {
            keys: Box::new(K::Vector(ColVec::Sym(Attr::None, Arc::new(names)))),
            vals: Box::new(K::Vector(ColVec::from_atoms(vals))),
        })))
    }

    fn exec_update(
        &mut self,
        base_flip: &Flip,
        active_rows: &Arc<Vec<usize>>,
        cols: &[ColSpec],
        by: Option<&[ColSpec]>,
    ) -> KResult<Flip> {
        let mut work = base_flip.clone();
        if cols.is_empty() {
            return Ok(work);
        }
        let col_names = resolve_col_names(cols);
        let total = work.nrows();
        let all_selected = active_rows.len() == total;
        let rows = (!all_selected).then(|| active_rows.clone());

        if let Some(by_specs) = by {
            // Grouped `update` (`update vwap: size wavg price by sym from t`): stream over groups where possible, else per group!
            let read = self.columns_read(&work, cols.iter().chain(by_specs).map(|c| &c.expr));
            let sub = if all_selected { read } else { read.gather(active_rows) };
            let mut by_vecs = Vec::with_capacity(by_specs.len());
            for bs in by_specs {
                match self.eval_in_rows(&sub, rows.clone(), &bs.expr)? {
                    K::Vector(cv) if cv.len() == sub.nrows() => by_vecs.push(cv),
                    atom if atom.is_atom() => by_vecs.push(ColVec::repeat(atom, sub.nrows())),
                    _ => return Err(KError::length("update by expression length mismatch")),
                }
            }
            let ids = group_ids(&by_vecs, sub.nrows());
            let first = first_rows(&ids);
            let ngroups = first.len();
            let gids: Vec<u32> = par::map(&ids, |&g| g as u32);
            let mut groups_cell: Option<Vec<Vec<usize>>> = None;
            let mut cache: Vec<(Expr, K)> = Vec::new();
            let row_u64s: Option<Vec<u64>> = (!all_selected).then(|| active_rows.iter().map(|&r| r as u64).collect());

            for (cname, cs) in col_names.into_iter().zip(cols) {
                let existing = work.col(cname).cloned();
                let tag = match existing {
                    Some(ColVec::Enum(tag, _)) => Some(tag),
                    _ => None,
                };
                // Try vectorized whole-column grouped computation when `sub` is non-empty
                let mut fast_sub_cv: Option<ColVec> = None;
                if ngroups > 0 {
                    if let Some(res) =
                        self.by_group_ids(&sub, &rows, &gids, ngroups, ngroups, &first, &mut groups_cell, &mut cache, &cs.expr)
                    {
                        fast_sub_cv = match res {
                            K::Vector(per_grp) if per_grp.len() == ngroups => Some(per_grp.gather(&ids)),
                            atom if atom.is_atom() => Some(ColVec::repeat(atom, sub.nrows())),
                            _ => None,
                        };
                    } else if let Expr::Unary { op, arg } = &cs.expr {
                        if resolve(*op) == "sums" {
                            if let Some(vals) = self.row_values(&sub, &rows, arg, &mut cache) {
                                if matches!(vals.plain(), ColVec::Float(..) | ColVec::Real(..)) {
                                    let fs = vals.as_f64_vec().unwrap();
                                    let mut acc = vec![0.0f64; ngroups];
                                    let out: Vec<f64> = ids
                                        .iter()
                                        .zip(fs)
                                        .map(|(&g, v)| {
                                            if !v.is_nan() {
                                                acc[g] += v;
                                            }
                                            acc[g]
                                        })
                                        .collect();
                                    fast_sub_cv = Some(ColVec::Float(Attr::None, Arc::new(out)));
                                } else if let Some(is) = vals.as_i64_vec() {
                                    let mut acc = vec![0i64; ngroups];
                                    let out: Vec<i64> = ids
                                        .iter()
                                        .zip(is)
                                        .map(|(&g, v)| {
                                            if v != NULL_LONG {
                                                acc[g] = acc[g].wrapping_add(v);
                                            }
                                            acc[g]
                                        })
                                        .collect();
                                    fast_sub_cv = Some(ColVec::Long(Attr::None, Arc::new(out)));
                                }
                            }
                        }
                    }
                }
                if let Some(mut sub_cv) = fast_sub_cv {
                    if let Some(tag) = tag {
                        if let K::Vector(ecv) = self.enumerate(tag, &K::Vector(sub_cv))? {
                            sub_cv = ecv;
                        } else {
                            unreachable!()
                        }
                    }
                    let new_cv = if all_selected {
                        sub_cv
                    } else {
                        let mut base = existing.clone().unwrap_or_else(|| ColVec::repeat(sub_cv.null_atom(), total));
                        let r_u64 = row_u64s.as_ref().unwrap();
                        if matches!(base, ColVec::General(..)) || !crate::delta::set(&mut base, r_u64, &sub_cv) {
                            let mut atoms: Vec<K> = (0..total).map(|r| base.get_atom(r)).collect();
                            for (j, &orig_r) in active_rows.iter().enumerate() {
                                atoms[orig_r] = sub_cv.get_atom(j);
                            }
                            base = ColVec::from_atoms(atoms);
                        }
                        base.set_attr_unchecked(Attr::None);
                        base
                    };
                    if let Some(pos) = work.col_idx(cname) {
                        work.vals[pos] = new_cv;
                    } else {
                        work.cols.push(cname);
                        work.vals.push(new_cv);
                    }
                    continue;
                }

                let groups = groups_cell.get_or_insert_with(|| groups_from_ids(&gids, ngroups));
                let mut full_atoms: Vec<K> = if let Some(ref ex) = existing {
                    (0..total).map(|r| ex.get_atom(r)).collect()
                } else {
                    vec![K::Null; total]
                };
                let mut sample_atom: Option<K> = None;

                for grp in groups.iter() {
                    let gsub = sub.gather(grp);
                    let mut gval = self.eval_in_rows(&gsub, numbers_of(&rows, grp), &cs.expr)?;
                    if let Some(tag) = tag {
                        gval = self.enumerate(tag, &gval)?;
                    }
                    match gval {
                        K::Vector(cv) if cv.len() == grp.len() => {
                            for (j, &local_r) in grp.iter().enumerate() {
                                let a = cv.get_atom(j);
                                if sample_atom.is_none() {
                                    sample_atom = Some(a.clone());
                                }
                                full_atoms[active_rows[local_r]] = a;
                            }
                        }
                        atom => {
                            if sample_atom.is_none() {
                                sample_atom = Some(atom.clone());
                            }
                            for &local_r in grp {
                                full_atoms[active_rows[local_r]] = atom.clone();
                            }
                        }
                    }
                }
                if existing.is_none() {
                    if let Some(ref sa) = sample_atom {
                        let typed_null = ColVec::from_atoms(vec![sa.clone()]).null_atom();
                        for slot in &mut full_atoms {
                            if matches!(slot, K::Null) {
                                *slot = typed_null.clone();
                            }
                        }
                    }
                }
                let new_cv = ColVec::from_atoms(full_atoms);
                if let Some(pos) = work.col_idx(cname) {
                    work.vals[pos] = new_cv;
                } else {
                    work.cols.push(cname);
                    work.vals.push(new_cv);
                }
            }
            return Ok(work);
        }

        // Ungrouped `update`
        for (cname, cs) in col_names.into_iter().zip(cols) {
            let read = self.columns_read(&work, [&cs.expr]);
            let sub_cur = if all_selected { read } else { read.gather(active_rows) };
            let val = self.eval_in_rows(&sub_cur, rows.clone(), &cs.expr)?;
            if all_selected {
                let new_cv = match val {
                    K::Vector(cv) if cv.len() == total => cv,
                    atom => ColVec::repeat(atom, total),
                };
                if let Some(pos) = work.col_idx(cname) {
                    work.vals[pos] = new_cv;
                } else {
                    work.cols.push(cname);
                    work.vals.push(new_cv);
                }
            } else {
                let val = match work.col(cname) {
                    Some(&ColVec::Enum(tag, _)) => self.enumerate(tag, &val)?,
                    _ => val,
                };
                let sub_cv = match val {
                    K::Vector(cv) if cv.len() == active_rows.len() => cv,
                    K::Vector(cv) if !matches!(cv, ColVec::Char(..)) => {
                        return Err(KError::length(format!("{} values for {} rows", cv.len(), active_rows.len())));
                    }
                    atom => ColVec::repeat(atom, active_rows.len()),
                };
                if let Some(held) = work.col(cname) {
                    if !held.is_list() && held.type_num() != sub_cv.type_num() && !active_rows.is_empty() {
                        return Err(KError::ty(format!("a {} value in a {} column", sub_cv.type_name(), held.type_name())));
                    }
                }
                let mut new_cv = work.col(cname).cloned().unwrap_or_else(|| ColVec::repeat(sub_cv.null_atom(), total));
                let rows: Vec<u64> = active_rows.iter().map(|&r| r as u64).collect();
                if matches!(new_cv, ColVec::General(..)) || !crate::delta::set(&mut new_cv, &rows, &sub_cv) {
                    let mut atoms: Vec<K> = (0..total).map(|r| new_cv.get_atom(r)).collect();
                    for (j, &orig_r) in active_rows.iter().enumerate() {
                        atoms[orig_r] = sub_cv.get_atom(j);
                    }
                    new_cv = ColVec::from_atoms(atoms);
                }
                new_cv.set_attr_unchecked(Attr::None);
                if let Some(pos) = work.col_idx(cname) {
                    work.vals[pos] = new_cv;
                } else {
                    work.cols.push(cname);
                    work.vals.push(new_cv);
                }
            }
        }
        Ok(work)
    }

    /// `(agg_fn; data_vec) fby group_vec_or_table`
    pub fn eval_fby(&mut self, lhs: &K, rhs: &K) -> KResult<K> {
        let K::Vector(pair) = lhs else {
            return Err(KError::ty("fby LHS must be a 2-element list (agg_func; data_vec)"));
        };
        if pair.len() != 2 {
            return Err(KError::length("fby LHS must have 2 elements (agg_func; data_vec)"));
        }
        let K::Func(func) = pair.get_atom(0) else {
            return Err(KError::ty("fby LHS[0] must be a function"));
        };
        let K::Vector(data) = pair.get_atom(1) else {
            return Err(KError::ty("fby LHS[1] must be a vector"));
        };
        let nrows = data.len();

        let grp_cols: Vec<ColVec> = match rhs {
            K::Vector(cv) => {
                if cv.len() != nrows {
                    return Err(KError::length("fby data and group lengths differ"));
                }
                vec![cv.clone()]
            }
            K::Table(t) => {
                if t.nrows() != nrows {
                    return Err(KError::length("fby data and group table lengths differ"));
                }
                t.vals.clone()
            }
            _ => return Err(KError::ty("fby RHS must be a vector or table")),
        };

        if nrows == 0 {
            return Ok(K::Vector(ColVec::from_atoms(Vec::new())));
        }
        let ids = group_ids(&grp_cols, nrows);
        let first = first_rows(&ids);
        let ngroups = first.len();
        let gids: Vec<u32> = par::map(&ids, |&g| g as u32);
        if let Func::Prim(op) = &func {
            let verb = resolve(*op);
            if verb == "count" || !matches!(data, ColVec::General(..)) {
                if let Some(per_grp) = aggregate_by_ids(verb, &data, &gids, ngroups, ngroups, &first) {
                    return Ok(K::Vector(per_grp.gather(&ids)));
                }
            }
        }
        let groups = groups_from_ids(&gids, ngroups);
        let mut out = vec![K::Null; nrows];
        for grp in groups {
            let slice = K::Vector(data.gather(&grp));
            let res = self.apply_func(&func, &[slice])?;
            match res {
                K::Bool(b) => {
                    for r in grp {
                        out[r] = K::Bool(b);
                    }
                }
                K::Vector(cv) if cv.len() == grp.len() => {
                    for (j, r) in grp.into_iter().enumerate() {
                        out[r] = cv.get_atom(j);
                    }
                }
                scalar => {
                    for r in grp {
                        out[r] = scalar.clone();
                    }
                }
            }
        }
        Ok(K::Vector(ColVec::from_atoms(out)))
    }

    /// Add rows to the table named `name` (`upsert`: setting those whose keys it has). The rows it had and how many came.
    fn add_rows(&mut self, name: SymId, rows: &K, upsert: bool) -> KResult<(usize, usize)> {
        let is_list = |r: &K| matches!(r, K::Vector(v) if v.is_list());
        if !upsert && matches!(rows, K::Vector(ColVec::General(_, items)) if !items.is_empty() && items.iter().all(is_list)) {
            return Err(KError::ty("insert takes columns, a table or dictionaries, not a list of rows"));
        }
        if self.names_the_roots(name) {
            return self.in_root(|engine| engine.add_rows(name, rows, upsert));
        }
        let current = match self.get_var(name) {
            // a table that is not there yet is made of the rows
            Err(e) if e.signal != "stop" && matches!(rows, K::Table(_) | K::KeyedTable(_)) => {
                self.set_global(name, rows.clone())?;
                return Ok((0, rows.count()));
            }
            held => held?,
        };
        // a global is changed where it is: only the rows touched are logged and copied
        if let Some(global) = self.global_to_change(name) {
            let (incoming, start) = self.incoming_rows(&current, rows)?;
            if let Some(change) = row_changes(&current, &incoming, !upsert)? {
                drop(current);
                self.write_globals(vec![(global, Write::Change(change.changes))])?;
                return Ok((start, incoming.nrows()));
            }
        }
        let table = self.get_var(name)?;
        let (table, start, added) = self.append_to_table(&table, rows, upsert)?;
        self.set_global(name, table)?;
        Ok((start, added))
    }

    /// `` `t insert rows ``
    pub fn eval_insert(&mut self, lhs: &K, rhs: &K) -> KResult<K> {
        let K::Sym(tname) = lhs else {
            return Err(KError::ty("insert LHS must be a table symbol (`table)"));
        };
        let (start_idx, added) = self.add_rows(*tname, rhs, false)?;
        let indices: Vec<i64> = (start_idx as i64..(start_idx + added) as i64).collect();
        Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(indices))))
    }

    /// `` `t upsert rows `` or `t upsert rows`
    pub fn eval_upsert(&mut self, lhs: &K, rhs: &K) -> KResult<K> {
        if let K::Sym(tname) = lhs {
            self.add_rows(*tname, rhs, true)?;
            Ok(K::Sym(*tname))
        } else {
            let (updated, _, _) = self.append_to_table(lhs, rhs, true)?;
            Ok(updated)
        }
    }

    /// The incoming rows as a flip of the table's columns (keys first), and the rows the table has.
    pub(crate) fn incoming_rows(&mut self, target: &K, rows: &K) -> KResult<(Flip, usize)> {
        let full = match target {
            K::Table(t) => (**t).clone(),
            K::KeyedTable(kt) => kt.unkey(),
            _ => return Err(KError::ty("insert/upsert target must be a table")),
        };
        let incoming = coerce_rows_to_flip(&full.cols, &full.vals, rows)?;
        Ok((self.enumerate_rows(&full, incoming)?, full.nrows()))
    }

    /// The table `rows` make of `target`, the rows it had and how many came: by changes where they can be made to a
    /// copy, otherwise rebuilt.
    pub(crate) fn append_to_table(&mut self, target: &K, rows: &K, allow_key_overwrite: bool) -> KResult<(K, usize, usize)> {
        let (incoming, start) = self.incoming_rows(target, rows)?;
        let added = incoming.nrows();
        if let Some(change) = row_changes(target, &incoming, !allow_key_overwrite)? {
            let mut table = target.clone();
            if !crate::delta::patch(&mut table, &change.changes) {
                return Err(KError::new("stale", "the table could not be changed"));
            }
            return Ok((table, start, added));
        }
        match target {
            K::Table(t) => Ok((K::Table(Arc::new(t.concat(&incoming)?)), start, added)),
            K::KeyedTable(kt) => {
                let rows = KeyedTable::from_flip_nkeys(&incoming, kt.keys.ncols())?;
                let K::KeyedTable(new) = &rows else { return Err(KError::ty("rows do not form a keyed table")) };
                if !allow_key_overwrite && match_keys(kt, &new.keys.vals.iter().collect::<Vec<_>>(), added).iter().any(Option::is_some) {
                    return Err(KError::new("insert", "duplicate key in keyed table insert"));
                }
                Ok((crate::engine::joins::union_join(target, &rows)?, start, added))
            }
            _ => Err(KError::ty("insert/upsert target must be a table")),
        }
    }
}

fn coerce_rows_to_flip(cols: &[SymId], proto: &[ColVec], rows: &K) -> KResult<Flip> {
    match rows {
        K::Table(rt) => {
            if rt.cols == cols {
                Ok((**rt).clone())
            } else {
                let mut vals = Vec::with_capacity(cols.len());
                for (i, &c) in cols.iter().enumerate() {
                    vals.push(rt.col(c).cloned().unwrap_or_else(|| proto[i].gather(&vec![usize::MAX; rt.nrows()])));
                }
                Flip::new(cols.to_vec(), vals)
            }
        }
        K::KeyedTable(rkt) => coerce_rows_to_flip(cols, proto, &K::Table(Arc::new(rkt.unkey()))),
        K::Dict(d) => {
            let one = crate::engine::ops::enlist_op(&K::Dict(d.clone()));
            coerce_rows_to_flip(cols, proto, &one)
        }
        K::Vector(cv) => {
            if let ColVec::General(_, items) = cv {
                if let Some(rows) = crate::engine::rows::from_rows(cols, proto, items) {
                    return rows;
                }
            }
            if cv.len() != cols.len() {
                return Err(KError::length("insert list length must equal column count"));
            }
            let first = cv.get_atom(0);
            let is_bulk = matches!(&first, K::Vector(inner) if !matches!(inner, ColVec::Char(_, _)) || matches!(proto[0], ColVec::Char(_, _)));
            if is_bulk {
                let mut vals = Vec::with_capacity(cols.len());
                for i in 0..cols.len() {
                    let K::Vector(col_v) = cv.get_atom(i) else {
                        return Err(KError::ty("mixed scalar and vector in bulk insert"));
                    };
                    vals.push(col_v);
                }
                Flip::new(cols.to_vec(), vals)
            } else {
                let vals: Vec<ColVec> = (0..cols.len()).map(|i| ColVec::from_atoms(vec![cv.get_atom(i)])).collect();
                Flip::new(cols.to_vec(), vals)
            }
        }
        _ => Err(KError::ty("insert/upsert RHS must be table, dict, or row list")),
    }
}

/// What `exec ... by` gives for the keyed table the same `select` gives: a dictionary whose sides are lists where there
/// is one column. With several on both sides it is that keyed table (in q a dictionary of tables is one).
pub fn exec_by_result(kt: &KeyedTable) -> K {
    if kt.keys.ncols() > 1 && kt.vals.ncols() > 1 {
        return K::KeyedTable(Arc::new(kt.clone()));
    }
    let side = |f: &Flip| {
        if f.ncols() == 1 {
            K::Vector(f.vals[0].clone())
        } else {
            K::Table(Arc::new(f.clone()))
        }
    };
    let values = match side(&kt.vals) {
        // (dictionaries with the same keys make a table, as in a pivot)
        K::Vector(ColVec::General(_, items)) => crate::engine::rows::collapse((*items).clone()),
        other => other,
    };
    if let K::Table(table) = &values {
        // (keys with a table of values are a keyed table)
        if let Ok(keyed) = KeyedTable::new(kt.keys.clone(), (**table).clone()) {
            return K::KeyedTable(Arc::new(keyed));
        }
    }
    K::Dict(Arc::new(Dict { keys: Box::new(side(&kt.keys)), vals: Box::new(values) }))
}

fn restore_keys(flip: Flip, orig_keys: Option<Vec<SymId>>) -> KResult<K> {
    match orig_keys {
        Some(ks) if ks.iter().all(|k| flip.col_idx(*k).is_some()) => KeyedTable::from_flip_key_names(&flip, &ks),
        _ => Ok(K::Table(Arc::new(flip))),
    }
}

/// The result column names of a qSQL column list (inferred, with suffixes for repeats).
fn query_has_bare_i(q: &QSql) -> bool {
    fn bare_i(e: &Expr) -> bool {
        match e {
            Expr::Query(_) => false,
            Expr::Unary { op, arg } if resolve(*op) == "count" && matches!(&**arg, Expr::Var(s) if resolve(*s) == "i") => false,
            Expr::Var(s) if resolve(*s) == "i" => true,
            other => other.children().into_iter().any(bare_i),
        }
    }
    q.where_clauses
        .iter()
        .chain(q.cols.iter().map(|c| &c.expr))
        .chain(q.by.iter().flatten().map(|c| &c.expr))
        .any(bare_i)
}

pub fn resolve_col_names(specs: &[ColSpec]) -> Vec<SymId> {
    let mut used: HashMap<SymId, usize> = HashMap::new();
    let mut out = Vec::with_capacity(specs.len());
    for cs in specs {
        let base = cs.name.or_else(|| infer_col_name(&cs.expr)).unwrap_or_else(|| intern("x"));
        let cnt = used.entry(base).or_insert(0);
        let chosen = if *cnt == 0 {
            base
        } else {
            intern(&format!("{}{}", resolve(base), *cnt))
        };
        *cnt += 1;
        out.push(chosen);
    }
    out
}

/// The numbers in the whole table of the rows at `positions` among the rows numbered `rows`.
fn numbers_of(rows: &RowNumbers, positions: &[usize]) -> RowNumbers {
    Some(Arc::new(positions.iter().map(|&p| rows.as_ref().map_or(p, |rows| rows[p])).collect()))
}

fn extract_where_indices(pred: &K, nrows: usize) -> KResult<Vec<usize>> {
    match pred {
        K::Bool(true) => Ok((0..nrows).collect()),
        K::Bool(false) => Ok(vec![]),
        K::Vector(ColVec::Bool(_, bs)) => {
            if bs.len() != nrows {
                return Err(KError::length("where predicate boolean vector length mismatch"));
            }
            Ok(true_rows(bs))
        }
        K::Vector(cv) => {
            // In kdb+, an integer vector in `where` (e.g. from `f` or `where`) directly selects row indices or repetition counts
            if let Some(is) = cv.as_i64_vec() {
                if is.len() == nrows && is.iter().all(|&v| v == 0 || v == 1) {
                    Ok(is.into_iter().enumerate().filter_map(|(i, v)| if v != 0 { Some(i) } else { None }).collect())
                } else {
                    Ok(is.into_iter().filter_map(|i| if i >= 0 && (i as usize) < nrows { Some(i as usize) } else { None }).collect())
                }
            } else {
                Err(KError::ty("where clause must evaluate to boolean vector"))
            }
        }
        _ => Err(KError::ty("where clause must evaluate to boolean vector")),
    }
}
