//! `qSQL` execution (`select`, `exec`, `update`, `delete`), `fby` filter-by-group, and `insert`/`upsert`.
use crate::ast::{ColSpec, Expr, QSql, QSqlKind};
use crate::engine::ops::group_rows;
use crate::engine::Engine;
use crate::error::{KError, KResult};
use crate::parser::infer_col_name;
use crate::sym::{intern, resolve, SymId};
use crate::types::*;
use std::collections::HashMap;
use std::sync::Arc;

/// Verbs whose argument names things only at run time.
pub const DYNAMIC: [&str; 5] = ["value", "get", "eval", "reval", "system"];

impl<'a> Engine<'a> {
    /// The names that `exprs` can read, or `None` when they reach names at run time (through a `DYNAMIC` verb, a view
    /// or a function variable) and so may read any column.
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
        let dynamic = names.iter().any(|&n| DYNAMIC.contains(&resolve(n).as_str()) || self.reaches_names(n));
        (!dynamic).then_some(names)
    }

    /// The columns of `flip` that `exprs` can read (shared, not copied); at least one, which gives the row count.
    fn columns_read<'e>(&self,flip: &Flip, exprs: impl IntoIterator<Item = &'e Expr>) -> Flip {
        let Some(names) = self.names_read(exprs) else { return flip.clone() };
        let mut keep: Vec<usize> = (0..flip.ncols()).filter(|&c| names.contains(&flip.cols[c])).collect();
        if keep.is_empty() && flip.ncols() > 0 {
            keep.push(0);
        }
        Flip { cols: keep.iter().map(|&c| flip.cols[c]).collect(), vals: keep.iter().map(|&c| flip.vals[c].clone()).collect() }
    }

    pub fn eval_qsql(&mut self, q: &QSql) -> KResult<K> {
        // Check if `from` is a symbol `` `table `` (in-place `update` / `delete`, or reading a named table)
        let mut inplace_target: Option<SymId> = None;
        // the names a `select`/`exec` with an explicit column list mentions: all a partitioned table has to load
        let outputs = || q.cols.iter().chain(q.by.iter().flatten()).map(|c| &c.expr);
        let reads_listed = matches!(q.kind, QSqlKind::Select | QSqlKind::Exec) && !q.cols.is_empty();
        let needed = if reads_listed { self.names_read(outputs().chain(&q.where_clauses)) } else { None };
        let source_k = match &*q.from {
            Expr::Lit(K::Sym(tname)) => {
                if matches!(q.kind, QSqlKind::Update | QSqlKind::DeleteRows | QSqlKind::DeleteCols) {
                    inplace_target = Some(*tname);
                }
                self.resolve_table_for_query(*tname, &q.where_clauses, needed.as_deref())?
            }
            Expr::Var(tname) => {
                // If `tname` is a mounted partitioned HDB table, prune partitions using `where_clauses`!
                if self.has_partitioned(*tname) {
                    self.resolve_table_for_query(*tname, &q.where_clauses, needed.as_deref())?
                } else {
                    self.eval_expr(&q.from)?
                }
            }
            other => self.eval_expr(other)?,
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
                self.set_global(sym, out_k)?;
                return Ok(K::Sym(sym));
            }
            return Ok(out_k);
        }

        // Sequential `where` filtering: each sub-clause sees only rows surviving prior sub-clauses!
        // `active_rows == None` represents all `0..total_nrows` rows without cloning `base_flip` up front.
        let total_nrows = base_flip.nrows();
        let mut active_rows: Option<Vec<usize>> = None;

        for wc in &q.where_clauses {
            if total_nrows == 0 || active_rows.as_ref().is_some_and(|r| r.is_empty()) {
                break;
            }
            if let Some(indexed) = crate::index::try_filter_where(&base_flip, active_rows.as_deref(), wc, &mut |e| self.eval_expr(e))? {
                active_rows = Some(indexed);
                continue;
            }
            match &active_rows {
                None => {
                    let pred_k = self.eval_in_table_scope(&base_flip, wc)?;
                    active_rows = Some(extract_where_indices(&pred_k, total_nrows)?);
                }
                Some(cur) => {
                    let sub_flip = self.columns_read(&base_flip, [wc]).gather(cur);
                    let pred_k = self.eval_in_table_scope(&sub_flip, wc)?;
                    let keep_local = extract_where_indices(&pred_k, sub_flip.nrows())?;
                    active_rows = Some(keep_local.into_iter().map(|li| cur[li]).collect());
                }
            }
        }

        match q.kind {
            QSqlKind::DeleteRows => {
                let res_flip = match &active_rows {
                    None => base_flip.take(0),
                    Some(rows) => {
                        let mut drop_mask = vec![false; total_nrows];
                        for &r in rows {
                            drop_mask[r] = true;
                        }
                        let keep_idx: Vec<usize> = (0..total_nrows).filter(|&r| !drop_mask[r]).collect();
                        base_flip.gather(&keep_idx)
                    }
                };
                let out_k = restore_keys(res_flip, orig_key_cols)?;
                if let Some(sym) = inplace_target {
                    self.set_global(sym, out_k)?;
                    return Ok(K::Sym(sym));
                }
                Ok(out_k)
            }

            QSqlKind::Update => {
                let all_idx: Vec<usize>;
                let rows_slice: &[usize] = match &active_rows {
                    Some(r) => r.as_slice(),
                    None => {
                        all_idx = (0..total_nrows).collect();
                        &all_idx
                    }
                };
                let out_flip = self.exec_update(&base_flip, rows_slice, &q.cols, q.by.as_deref())?;
                let out_k = restore_keys(out_flip, orig_key_cols)?;
                if let Some(sym) = inplace_target {
                    self.set_global(sym, out_k)?;
                    return Ok(K::Sym(sym));
                }
                Ok(out_k)
            }

            QSqlKind::Select | QSqlKind::Exec => {
                // without a column list the result has every column
                let read = if q.cols.is_empty() { base_flip.clone() } else { self.columns_read(&base_flip, outputs()) };
                let filtered = match &active_rows {
                    None => read,
                    Some(rows) => read.gather(rows),
                };
                if q.kind == QSqlKind::Exec {
                    return self.exec_query_exec(&filtered, &q.cols, q.by.as_deref());
                }
                let mut res = self.exec_select(&filtered, &q.cols, q.by.as_deref(), orig_key_cols)?;
                if q.distinct {
                    if let K::Table(_) = &res {
                        res = crate::engine::ops::distinct_op(&res)?;
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
                let mut evaluated = Vec::with_capacity(cols.len());
                for cs in cols {
                    evaluated.push(self.eval_in_table_scope(filtered, &cs.expr)?);
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
                let has_vec = evaluated.iter().zip(cols).any(|(k, cs)| {
                    if is_col_vec(k) {
                        true
                    } else if let K::Vector(ColVec::Char(_, bs)) = k {
                        // Treat as column vector if expression referenced a char column of `filtered`
                        infer_col_name(&cs.expr)
                            .and_then(|n| filtered.col(n))
                            .is_some_and(|c| matches!(c, ColVec::Char(_, _)) && bs.len() == filtered.nrows())
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
                                    .is_some_and(|c| matches!(c, ColVec::Char(_, _))) =>
                            {
                                let _ = bs;
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
                    let v = self.eval_in_table_scope(filtered, &bs.expr)?;
                    match v {
                        K::Vector(cv) if cv.len() == filtered.nrows() => by_vecs.push(cv),
                        atom if atom.is_atom() => by_vecs.push(ColVec::repeat(atom, filtered.nrows())),
                        _ => return Err(KError::length("by expression length must match table row count")),
                    }
                }

                if by_specs.is_empty() {
                    // `select ... by from t` (single group over whole table)
                    let mut out_vals = Vec::with_capacity(cols_ref.len());
                    for cs in cols_ref {
                        let k = self.eval_in_table_scope(filtered, &cs.expr)?;
                        out_vals.push(ColVec::from_atoms(vec![k]));
                    }
                    return Ok(K::Table(Arc::new(Flip::new(val_names, out_vals)?)));
                }

                // Group row indices by composite `by` key
                let (first_rows, mut groups) = group_rows(&by_vecs, filtered.nrows());

                // Sort groups ascending by `by_vecs` keys (matching kdb+ `select ... by k from t`!)
                let mut group_order: Vec<usize> = (0..first_rows.len()).collect();
                group_order.sort_by(|&ga, &gb| {
                    let ra = first_rows[ga];
                    let rb = first_rows[gb];
                    for bv in &by_vecs {
                        let ord = bv.get_atom(ra).k_cmp(&bv.get_atom(rb));
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                    ra.cmp(&rb)
                });
                let sorted_first_rows: Vec<usize> = group_order.iter().map(|&g| first_rows[g]).collect();
                let sorted_groups: Vec<Vec<usize>> = group_order.into_iter().map(|g| std::mem::take(&mut groups[g])).collect();

                let mut key_vals: Vec<ColVec> = by_vecs.iter().map(|bv| bv.gather(&sorted_first_rows)).collect();
                if by_vecs.len() == 1 && !key_vals.is_empty() {
                    key_vals[0].set_attr_unchecked(Attr::Sorted);
                }

                let ngroups = sorted_groups.len();
                let mut agg_cols: Vec<Vec<K>> = vec![Vec::with_capacity(ngroups); cols_ref.len()];

                if ngroups == 0 {
                    // Empty input with `by`: probe empty sub-flip to infer result types!
                    let mut empty_vals = Vec::with_capacity(cols_ref.len());
                    for cs in cols_ref {
                        let sample = self.eval_in_table_scope(filtered, &cs.expr)?;
                        empty_vals.push(ColVec::from_atoms(vec![sample]).take(0));
                    }
                    let keys_flip = Flip::new(by_names, key_vals)?;
                    let vals_flip = Flip::new(val_names, empty_vals)?;
                    return Ok(K::KeyedTable(Arc::new(KeyedTable { keys: keys_flip, vals: vals_flip })));
                }

                for grp_idx in &sorted_groups {
                    // Build group sub-table, also binding any newly computed `by` aliases so `select ... by bar: 5 xbar t` can reference `bar`!
                    let mut sub = filtered.gather(grp_idx);
                    for (bn, bv) in by_names.iter().zip(&by_vecs) {
                        let gcol = bv.gather(grp_idx);
                        if let Some(pos) = sub.col_idx(*bn) {
                            sub.vals[pos] = gcol;
                        } else {
                            sub.cols.push(*bn);
                            sub.vals.push(gcol);
                        }
                    }
                    for (ci, cs) in cols_ref.iter().enumerate() {
                        let cell = self.eval_in_table_scope_with_orig_i(&sub, grp_idx, &cs.expr)?;
                        agg_cols[ci].push(cell);
                    }
                }

                let out_vals: Vec<ColVec> = agg_cols.into_iter().map(ColVec::from_atoms).collect();
                let keys_flip = Flip::new(by_names, key_vals)?;
                let vals_flip = Flip::new(val_names, out_vals)?;
                Ok(K::KeyedTable(Arc::new(KeyedTable { keys: keys_flip, vals: vals_flip })))
            }
        }
    }

    fn exec_query_exec(&mut self, filtered: &Flip, cols: &[ColSpec], by: Option<&[ColSpec]>) -> KResult<K> {
        if let Some(by_specs) = by {
            // `exec val by key from t` -> dictionary `keys ! vals`
            let tbl = self.exec_select(filtered, cols, Some(by_specs), None)?;
            if let K::KeyedTable(kt) = &tbl {
                return Ok(exec_by_result(kt));
            }
            return Ok(tbl);
        }

        if cols.is_empty() {
            return Ok(crate::engine::ops::first_op(&K::Table(Arc::new(filtered.clone()))));
        }
        if cols.len() == 1 && cols[0].name.is_none() {
            return self.eval_in_table_scope(filtered, &cols[0].expr);
        }
        let names = resolve_col_names(cols);
        let mut vals = Vec::with_capacity(cols.len());
        for cs in cols {
            vals.push(self.eval_in_table_scope(filtered, &cs.expr)?);
        }
        Ok(K::Dict(Arc::new(Dict {
            keys: Box::new(K::Vector(ColVec::Sym(Attr::None, Arc::new(names)))),
            vals: Box::new(K::Vector(ColVec::from_atoms(vals))),
        })))
    }

    fn exec_update(
        &mut self,
        base_flip: &Flip,
        active_rows: &[usize],
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

        if let Some(by_specs) = by {
            // Grouped `update` (`update vwap: size wavg price by sym from t`): evaluate per group and scatter back!
            let read = self.columns_read(&work, cols.iter().chain(by_specs).map(|c| &c.expr));
            let sub = if all_selected { read } else { read.gather(active_rows) };
            let mut by_vecs = Vec::with_capacity(by_specs.len());
            for bs in by_specs {
                match self.eval_in_table_scope(&sub, &bs.expr)? {
                    K::Vector(cv) if cv.len() == sub.nrows() => by_vecs.push(cv),
                    atom if atom.is_atom() => by_vecs.push(ColVec::repeat(atom, sub.nrows())),
                    _ => return Err(KError::length("update by expression length mismatch")),
                }
            }
            let (_first, groups) = group_rows(&by_vecs, sub.nrows());
            for (cname, cs) in col_names.into_iter().zip(cols) {
                let existing = work.col(cname).cloned();
                let mut full_atoms: Vec<K> = if let Some(ref ex) = existing {
                    (0..total).map(|r| ex.get_atom(r)).collect()
                } else {
                    vec![K::Null; total]
                };
                let mut sample_atom: Option<K> = None;

                for grp in &groups {
                    let gsub = sub.gather(grp);
                    let gval = self.eval_in_table_scope(&gsub, &cs.expr)?;
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
            let val = self.eval_in_table_scope_with_orig_i(&sub_cur, active_rows, &cs.expr)?;
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
                let sub_cv = match val {
                    K::Vector(cv) if cv.len() == active_rows.len() => cv,
                    atom => ColVec::repeat(atom, active_rows.len()),
                };
                let typed_null = sub_cv.null_atom();
                let mut atoms: Vec<K> = if let Some(ex) = work.col(cname) {
                    (0..total).map(|r| ex.get_atom(r)).collect()
                } else {
                    vec![typed_null; total]
                };
                for (j, &orig_r) in active_rows.iter().enumerate() {
                    atoms[orig_r] = sub_cv.get_atom(j);
                }
                let new_cv = ColVec::from_atoms(atoms);
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

        let (_first, groups) = group_rows(&grp_cols, nrows);
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

    /// `` `t insert rows ``
    pub fn eval_insert(&mut self, lhs: &K, rhs: &K) -> KResult<K> {
        let K::Sym(tname) = lhs else {
            return Err(KError::ty("insert LHS must be a table symbol (`table)"));
        };
        let cur = self.get_var(*tname)?;
        let (updated, start_idx, added) = append_to_table(&cur, rhs, false)?;
        self.set_global(*tname, updated)?;
        let indices: Vec<i64> = (start_idx as i64..(start_idx + added) as i64).collect();
        Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(indices))))
    }

    /// `` `t upsert rows `` or `t upsert rows`
    pub fn eval_upsert(&mut self, lhs: &K, rhs: &K) -> KResult<K> {
        if let K::Sym(tname) = lhs {
            let cur = self.get_var(*tname)?;
            let (updated, _, _) = append_to_table(&cur, rhs, true)?;
            self.set_global(*tname, updated)?;
            Ok(K::Sym(*tname))
        } else {
            let (updated, _, _) = append_to_table(lhs, rhs, true)?;
            Ok(updated)
        }
    }
}

fn append_to_table(target: &K, rows: &K, allow_key_overwrite: bool) -> KResult<(K, usize, usize)> {
    match target {
        K::Table(t) => {
            let rhs_flip = coerce_rows_to_flip(&t.cols, &t.vals, rows)?;
            let start = t.nrows();
            let added = rhs_flip.nrows();
            let merged = t.concat(&rhs_flip)?;
            Ok((K::Table(Arc::new(merged)), start, added))
        }
        K::KeyedTable(kt) => {
            let full = kt.unkey();
            let rhs_flip = coerce_rows_to_flip(&full.cols, &full.vals, rows)?;
            let added = rhs_flip.nrows();
            let start = kt.nrows();
            let rhs_kt = KeyedTable::from_flip_nkeys(&rhs_flip, kt.keys.ncols())?;
            if allow_key_overwrite {
                let merged = crate::engine::joins::union_join(target, &rhs_kt)?;
                Ok((merged, start, added))
            } else {
                // `insert` on keyed table fails if key already exists (`'insert`)
                let K::KeyedTable(rkt) = &rhs_kt else { unreachable!() };
                let existing_keys: std::collections::HashSet<Vec<u8>> = (0..kt.nrows())
                    .map(|r| {
                        let mut h = Vec::new();
                        for c in &kt.keys.vals {
                            h.extend_from_slice(&c.get_atom(r).hash_key());
                        }
                        h
                    })
                    .collect();
                for r in 0..rkt.nrows() {
                    let mut h = Vec::new();
                    for c in &rkt.keys.vals {
                        h.extend_from_slice(&c.get_atom(r).hash_key());
                    }
                    if existing_keys.contains(&h) {
                        return Err(KError::new("insert", "duplicate key in keyed table insert"));
                    }
                }
                let merged = crate::engine::joins::union_join(target, &rhs_kt)?;
                Ok((merged, start, added))
            }
        }
        _ => Err(KError::ty("insert/upsert target must be a table")),
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
    K::Dict(Arc::new(Dict { keys: Box::new(side(&kt.keys)), vals: Box::new(side(&kt.vals)) }))
}

fn restore_keys(flip: Flip, orig_keys: Option<Vec<SymId>>) -> KResult<K> {
    match orig_keys {
        Some(ks) if ks.iter().all(|k| flip.col_idx(*k).is_some()) => KeyedTable::from_flip_key_names(&flip, &ks),
        _ => Ok(K::Table(Arc::new(flip))),
    }
}

/// The result column names of a qSQL column list (inferred, with suffixes for repeats).
pub fn resolve_col_names(specs: &[ColSpec]) -> Vec<SymId> {
    let mut used: HashMap<SymId, usize> = HashMap::new();
    let mut out = Vec::with_capacity(specs.len());
    for (idx, cs) in specs.iter().enumerate() {
        let base = cs.name.or_else(|| infer_col_name(&cs.expr)).unwrap_or_else(|| intern(&format!("x{idx}")));
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

fn extract_where_indices(pred: &K, nrows: usize) -> KResult<Vec<usize>> {
    match pred {
        K::Bool(true) => Ok((0..nrows).collect()),
        K::Bool(false) => Ok(vec![]),
        K::Vector(ColVec::Bool(_, bs)) => {
            if bs.len() != nrows {
                return Err(KError::length("where predicate boolean vector length mismatch"));
            }
            Ok(bs.iter().enumerate().filter_map(|(i, &b)| if b { Some(i) } else { None }).collect())
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
