//! Relational and time-series joins matching kdb+:
//! - `ij` (inner join), `lj` (left join), `uj` (union join), `ej` (equi-join), `pj` (plus join)
//! - `aj` / `aj0` (as-of join on symbol/entity keys + monotonic time column, binary-searched per entity!)
//! - `wj` / `wj1` (window join aggregating quote windows `[lo; hi]` per trade row!)
use crate::error::{KError, KResult};
use crate::sym::{resolve, SymId};
use crate::types::*;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

fn row_hash(cols: &[&ColVec], row: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(cols.len() * 10);
    for c in cols {
        out.extend_from_slice(&c.get_atom(row).hash_key());
    }
    out
}

/// Probe row index in `kt` for each row of `left_key_cols` (`None` on miss), using attribute indexes on 1-key tables when present.
fn probe_keyed_table(kt: &KeyedTable, left_key_cols: &[&ColVec], nrows: usize) -> Vec<Option<usize>> {
    if kt.keys.ncols() == 1 && kt.keys.vals[0].attr() != Attr::None && -kt.keys.vals[0].type_num() == -left_key_cols[0].type_num() {
        let kcol = &kt.keys.vals[0];
        let lcol = left_key_cols[0];
        let klen = kcol.len();
        return (0..nrows)
            .map(|r| crate::index::find_first(kcol, &lcol.get_atom(r)).filter(|&idx| idx < klen))
            .collect();
    }
    let right_key_cols: Vec<&ColVec> = kt.keys.vals.iter().collect();
    let mut right_map: HashMap<Vec<u8>, usize> = HashMap::with_capacity(kt.nrows());
    for r in 0..kt.nrows() {
        right_map.entry(row_hash(&right_key_cols, r)).or_insert(r);
    }
    (0..nrows).map(|r| right_map.get(&row_hash(left_key_cols, r)).copied()).collect()
}

/// Look up value rows in `kt` for each row of `probe_keys` (returns `kt.vals`-shaped `Flip` with nulls for misses).
pub fn lookup_keyed_rows(kt: &KeyedTable, probe_keys: &Flip) -> KResult<Flip> {
    if kt.keys.ncols() != probe_keys.ncols() {
        return Err(KError::length("keyed table lookup column count mismatch"));
    }
    let pcols: Vec<&ColVec> = probe_keys.vals.iter().collect();
    let gather_idx: Vec<usize> = probe_keyed_table(kt, &pcols, probe_keys.nrows())
        .into_iter()
        .map(|m| m.unwrap_or(usize::MAX))
        .collect();
    Ok(kt.vals.gather(&gather_idx))
}

/// `t1 lj kt2` — Left join of unkeyed/keyed table `lhs` with keyed table `rhs` on `rhs`'s key columns.
pub fn left_join(lhs: &K, rhs: &K) -> KResult<K> {
    let K::KeyedTable(kt) = rhs else {
        return Err(KError::ty("lj RHS must be a keyed table"));
    };
    let (left_flip, left_key_len) = match lhs {
        K::Table(t) => ((**t).clone(), None),
        K::KeyedTable(lkt) => (lkt.unkey(), Some(lkt.keys.ncols())),
        _ => return Err(KError::ty("lj LHS must be a table")),
    };

    let mut left_key_cols = Vec::with_capacity(kt.keys.ncols());
    for &kc in &kt.keys.cols {
        let idx = left_flip
            .col_idx(kc)
            .ok_or_else(|| KError::new("mismatch", format!("lj key column `{}` missing on LHS", resolve(kc))))?;
        left_key_cols.push(&left_flip.vals[idx]);
    }
    let nrows = left_flip.nrows();
    let matched = probe_keyed_table(kt, &left_key_cols, nrows);

    let mut out_cols = left_flip.cols.clone();
    let mut out_vals = left_flip.vals.clone();

    for (rc, rv) in kt.vals.cols.iter().zip(&kt.vals.vals) {
        if let Some(existing_pos) = out_cols.iter().position(|c| c == rc) {
            // kdb+ `lj` semantics: non-null RHS value overwrites LHS value; on miss (or null RHS?), keep LHS on miss!
            let mut merged = Vec::with_capacity(nrows);
            for (r, m) in matched.iter().enumerate() {
                match m {
                    Some(rr) => {
                        let rhs_v = rv.get_atom(*rr);
                        if rhs_v.is_null() {
                            merged.push(out_vals[existing_pos].get_atom(r));
                        } else {
                            merged.push(rhs_v);
                        }
                    }
                    None => merged.push(out_vals[existing_pos].get_atom(r)),
                }
            }
            out_vals[existing_pos] = ColVec::from_atoms(merged);
        } else {
            let gather_idx: Vec<usize> = matched.iter().map(|m| m.unwrap_or(usize::MAX)).collect();
            out_cols.push(*rc);
            out_vals.push(rv.gather(&gather_idx));
        }
    }

    let res_flip = Flip { cols: out_cols, vals: out_vals };
    match left_key_len {
        Some(nk) => KeyedTable::from_flip_nkeys(&res_flip, nk),
        None => Ok(K::Table(Arc::new(res_flip))),
    }
}

/// `t1 ij kt2` — Inner join of table `lhs` with keyed table `rhs`.
pub fn inner_join(lhs: &K, rhs: &K) -> KResult<K> {
    let K::KeyedTable(kt) = rhs else {
        return Err(KError::ty("ij RHS must be a keyed table"));
    };
    let (left_flip, left_key_len) = match lhs {
        K::Table(t) => ((**t).clone(), None),
        K::KeyedTable(lkt) => (lkt.unkey(), Some(lkt.keys.ncols())),
        _ => return Err(KError::ty("ij LHS must be a table")),
    };

    let mut left_key_cols = Vec::with_capacity(kt.keys.ncols());
    for &kc in &kt.keys.cols {
        let idx = left_flip
            .col_idx(kc)
            .ok_or_else(|| KError::new("mismatch", format!("ij key column `{}` missing on LHS", resolve(kc))))?;
        left_key_cols.push(&left_flip.vals[idx]);
    }
    let mut l_keep = Vec::new();
    let mut r_keep = Vec::new();
    for (r, m) in probe_keyed_table(kt, &left_key_cols, left_flip.nrows()).into_iter().enumerate() {
        if let Some(rr) = m {
            l_keep.push(r);
            r_keep.push(rr);
        }
    }

    let lf = left_flip.gather(&l_keep);
    let rf = kt.vals.gather(&r_keep);
    let mut out_cols = lf.cols;
    let mut out_vals = lf.vals;
    for (rc, rv) in rf.cols.into_iter().zip(rf.vals) {
        if let Some(pos) = out_cols.iter().position(|&c| c == rc) {
            out_vals[pos] = rv;
        } else {
            out_cols.push(rc);
            out_vals.push(rv);
        }
    }
    let res_flip = Flip { cols: out_cols, vals: out_vals };
    match left_key_len {
        Some(nk) => KeyedTable::from_flip_nkeys(&res_flip, nk),
        None => Ok(K::Table(Arc::new(res_flip))),
    }
}

/// `ej[cols; t1; t2]` — Equi-join of two tables (many-to-many hash join) on `cols`.
pub fn equi_join(cols_k: &K, lhs: &K, rhs: &K) -> KResult<K> {
    let join_cols: Vec<SymId> = match cols_k {
        K::Sym(s) => vec![*s],
        K::Vector(ColVec::Sym(_, xs)) => (**xs).clone(),
        _ => return Err(KError::ty("ej first argument must be symbol or symbol vector")),
    };
    let lf = match lhs {
        K::Table(t) => (**t).clone(),
        K::KeyedTable(kt) => kt.unkey(),
        _ => return Err(KError::ty("ej second argument must be a table")),
    };
    let rf = match rhs {
        K::Table(t) => (**t).clone(),
        K::KeyedTable(kt) => kt.unkey(),
        _ => return Err(KError::ty("ej third argument must be a table")),
    };

    let mut lk = Vec::with_capacity(join_cols.len());
    let mut rk = Vec::with_capacity(join_cols.len());
    for &c in &join_cols {
        lk.push(&lf.vals[lf.col_idx(c).ok_or_else(|| KError::value(resolve(c)))?]);
        rk.push(&rf.vals[rf.col_idx(c).ok_or_else(|| KError::value(resolve(c)))?]);
    }

    let mut right_buckets: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
    for r in 0..rf.nrows() {
        right_buckets.entry(row_hash(&rk, r)).or_default().push(r);
    }

    let mut l_idx = Vec::new();
    let mut r_idx = Vec::new();
    for r in 0..lf.nrows() {
        if let Some(matches) = right_buckets.get(&row_hash(&lk, r)) {
            for &rr in matches {
                l_idx.push(r);
                r_idx.push(rr);
            }
        }
    }

    let lg = lf.gather(&l_idx);
    let rg = rf.gather(&r_idx);
    let mut out_cols = lg.cols;
    let mut out_vals = lg.vals;
    for (rc, rv) in rg.cols.into_iter().zip(rg.vals) {
        if join_cols.contains(&rc) {
            continue;
        }
        if let Some(pos) = out_cols.iter().position(|&c| c == rc) {
            out_vals[pos] = rv;
        } else {
            out_cols.push(rc);
            out_vals.push(rv);
        }
    }
    Ok(K::Table(Arc::new(Flip { cols: out_cols, vals: out_vals })))
}

/// `t1 pj kt2` — Plus join: adds matching numeric columns of keyed table `kt2` into `t1` (0 for missing keys).
pub fn plus_join(lhs: &K, rhs: &K) -> KResult<K> {
    let K::KeyedTable(kt) = rhs else {
        return Err(KError::ty("pj RHS must be a keyed table"));
    };
    let (left_flip, left_key_len) = match lhs {
        K::Table(t) => ((**t).clone(), None),
        K::KeyedTable(lkt) => (lkt.unkey(), Some(lkt.keys.ncols())),
        _ => return Err(KError::ty("pj LHS must be a table")),
    };

    let mut left_key_cols = Vec::with_capacity(kt.keys.ncols());
    for &kc in &kt.keys.cols {
        let idx = left_flip.col_idx(kc).ok_or_else(|| KError::value(resolve(kc)))?;
        left_key_cols.push(&left_flip.vals[idx]);
    }
    let nrows = left_flip.nrows();
    let matched = probe_keyed_table(kt, &left_key_cols, nrows);

    let mut out_cols = left_flip.cols;
    let mut out_vals = left_flip.vals;
    for (rc, rv) in kt.vals.cols.iter().zip(&kt.vals.vals) {
        let zero = if matches!(rv, ColVec::Float(_, _) | ColVec::Real(_, _)) { K::Float(0.0) } else { K::Long(0) };
        let addend_atoms: Vec<K> = matched
            .iter()
            .map(|m| match m {
                Some(rr) => {
                    let v = rv.get_atom(*rr);
                    if v.is_null() { zero.clone() } else { v }
                }
                None => zero.clone(),
            })
            .collect();
        let addend = K::Vector(ColVec::from_atoms(addend_atoms));
        if let Some(pos) = out_cols.iter().position(|c| c == rc) {
            if let K::Vector(summed) = crate::engine::ops::eval_binary_prim("+", &K::Vector(out_vals[pos].clone()), &addend)? {
                out_vals[pos] = summed;
            }
        } else if let K::Vector(added) = addend {
            out_cols.push(*rc);
            out_vals.push(added);
        }
    }
    let res_flip = Flip { cols: out_cols, vals: out_vals };
    match left_key_len {
        Some(nk) => KeyedTable::from_flip_nkeys(&res_flip, nk),
        None => Ok(K::Table(Arc::new(res_flip))),
    }
}

/// `t1 uj t2` — Union join of two unkeyed or keyed tables (fills missing columns with typed nulls; keyed tables upsert by key).
pub fn union_join(lhs: &K, rhs: &K) -> KResult<K> {
    match (lhs, rhs) {
        (K::Table(ta), K::Table(tb)) => {
            let mut all_cols = ta.cols.clone();
            let mut proto: Vec<ColVec> = ta.vals.iter().map(|v| v.take(0)).collect();
            for (c, v) in tb.cols.iter().zip(&tb.vals) {
                if !all_cols.contains(c) {
                    all_cols.push(*c);
                    proto.push(v.take(0));
                }
            }
            let total = ta.nrows() + tb.nrows();
            let mut out_vals = Vec::with_capacity(all_cols.len());
            for (ci, &col) in all_cols.iter().enumerate() {
                let ca = ta.col(col).cloned().unwrap_or_else(|| proto[ci].gather(&vec![usize::MAX; ta.nrows()]));
                let cb = tb.col(col).cloned().unwrap_or_else(|| proto[ci].gather(&vec![usize::MAX; tb.nrows()]));
                let merged = ca.concat(&cb);
                debug_assert_eq!(merged.len(), total);
                out_vals.push(merged);
            }
            Ok(K::Table(Arc::new(Flip { cols: all_cols, vals: out_vals })))
        }
        (K::KeyedTable(ka), K::KeyedTable(kb)) => {
            if ka.keys.cols != kb.keys.cols {
                return Err(KError::new("mismatch", "uj keyed tables must have matching key columns"));
            }
            let mut all_vcols = ka.vals.cols.clone();
            let mut proto_v: Vec<ColVec> = ka.vals.vals.iter().map(|v| v.take(0)).collect();
            for (c, v) in kb.vals.cols.iter().zip(&kb.vals.vals) {
                if !all_vcols.contains(c) {
                    all_vcols.push(*c);
                    proto_v.push(v.take(0));
                }
            }
            let mut key_rows: Vec<Vec<K>> = Vec::new();
            let mut val_rows: Vec<Vec<K>> = Vec::new();
            let mut pos_map: HashMap<Vec<u8>, usize> = HashMap::new();

            for src in [ka.as_ref(), kb.as_ref()] {
                let kcols: Vec<&ColVec> = src.keys.vals.iter().collect();
                for r in 0..src.nrows() {
                    let hk = row_hash(&kcols, r);
                    if let Some(&idx) = pos_map.get(&hk) {
                        for (vi, &vc) in all_vcols.iter().enumerate() {
                            if let Some(sc) = src.vals.col(vc) {
                                let v = sc.get_atom(r);
                                if !v.is_null() {
                                    val_rows[idx][vi] = v;
                                }
                            }
                        }
                    } else {
                        let idx = key_rows.len();
                        pos_map.insert(hk, idx);
                        key_rows.push(src.keys.vals.iter().map(|c| c.get_atom(r)).collect());
                        val_rows.push(
                            all_vcols
                                .iter()
                                .enumerate()
                                .map(|(vi, &vc)| src.vals.col(vc).map(|c| c.get_atom(r)).unwrap_or_else(|| proto_v[vi].null_atom()))
                                .collect(),
                        );
                    }
                }
            }

            let nrows = key_rows.len();
            let kvals: Vec<ColVec> = (0..ka.keys.ncols())
                .map(|c| {
                    if nrows == 0 {
                        ka.keys.vals[c].take(0)
                    } else {
                        ColVec::from_atoms(key_rows.iter().map(|r| r[c].clone()).collect())
                    }
                })
                .collect();
            let vvals: Vec<ColVec> = (0..all_vcols.len())
                .map(|c| {
                    if nrows == 0 {
                        proto_v[c].clone()
                    } else {
                        ColVec::from_atoms(val_rows.iter().map(|r| r[c].clone()).collect())
                    }
                })
                .collect();
            Ok(K::KeyedTable(Arc::new(KeyedTable {
                keys: Flip { cols: ka.keys.cols.clone(), vals: kvals },
                vals: Flip { cols: all_vcols, vals: vvals },
            })))
        }
        _ => Err(KError::ty("uj requires two unkeyed tables or two keyed tables")),
    }
}

// ----- Time-Series As-Of Join (`aj`, `aj0`, `asof`) -------------------------

/// `aj[cols; t1; t2]` (`preserve_lhs_time = true`) and `aj0[cols; t1; t2]` (`preserve_lhs_time = false`, takes quote's time!).
///
/// The first `k - 1` symbols of `cols` are exact equality keys (typically `` `sym ``), and the last symbol of `cols`
/// is the monotonic time/timestamp column (`time`). For each row of `t1`, finds the last row of `t2` with matching
/// equality keys whose `t2.time <= t1.time` using `O(log N)` binary search per entity bucket!
pub fn asof_join(cols_k: &K, lhs: &K, rhs: &K, preserve_lhs_time: bool) -> KResult<K> {
    let cols: Vec<SymId> = match cols_k {
        K::Sym(s) => vec![*s],
        K::Vector(ColVec::Sym(_, xs)) => (**xs).clone(),
        _ => return Err(KError::ty("aj first argument must be symbol or symbol vector")),
    };
    if cols.is_empty() {
        return Err(KError::domain("aj requires at least a time column"));
    }
    let lf = match lhs {
        K::Table(t) => (**t).clone(),
        K::KeyedTable(kt) => kt.unkey(),
        _ => return Err(KError::ty("aj second argument must be a table")),
    };
    let rf = match rhs {
        K::Table(t) => (**t).clone(),
        K::KeyedTable(kt) => kt.unkey(),
        _ => return Err(KError::ty("aj third argument must be a table")),
    };

    let eq_cols = &cols[..cols.len() - 1];
    let time_col = cols[cols.len() - 1];

    let mut l_eq = Vec::with_capacity(eq_cols.len());
    let mut r_eq = Vec::with_capacity(eq_cols.len());
    for &c in eq_cols {
        l_eq.push(&lf.vals[lf.col_idx(c).ok_or_else(|| KError::value(resolve(c)))?]);
        r_eq.push(&rf.vals[rf.col_idx(c).ok_or_else(|| KError::value(resolve(c)))?]);
    }
    let l_time = &lf.vals[lf.col_idx(time_col).ok_or_else(|| KError::value(resolve(time_col)))?];
    let r_time = &rf.vals[rf.col_idx(time_col).ok_or_else(|| KError::value(resolve(time_col)))?];

    // Partition RHS row indices by equality key (preserving RHS order; sort each bucket by time if not already sorted)
    let mut buckets: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
    for r in 0..rf.nrows() {
        buckets.entry(row_hash(&r_eq, r)).or_default().push(r);
    }
    for rows in buckets.values_mut() {
        let sorted = rows.windows(2).all(|w| r_time.get_atom(w[0]).k_cmp(&r_time.get_atom(w[1])) != Ordering::Greater);
        if !sorted {
            rows.sort_by(|&a, &b| r_time.get_atom(a).k_cmp(&r_time.get_atom(b)));
        }
    }

    let nrows = lf.nrows();
    let mut matched: Vec<usize> = Vec::with_capacity(nrows);
    for r in 0..nrows {
        let t_probe = l_time.get_atom(r);
        if let Some(b) = buckets.get(&row_hash(&l_eq, r)) {
            // Binary search: last index `m` in `b` where `r_time[b[m]] <= t_probe`
            let mut lo = 0usize;
            let mut hi = b.len();
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                if r_time.get_atom(b[mid]).k_cmp(&t_probe) != Ordering::Greater {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            if lo == 0 {
                matched.push(usize::MAX);
            } else {
                matched.push(b[lo - 1]);
            }
        } else {
            matched.push(usize::MAX);
        }
    }

    let mut out_cols = lf.cols.clone();
    let mut out_vals = lf.vals.clone();
    for (rc, rv) in rf.cols.iter().zip(&rf.vals) {
        if eq_cols.contains(rc) {
            continue;
        }
        if *rc == time_col && preserve_lhs_time {
            continue;
        }
        let gathered = rv.gather(&matched);
        if let Some(pos) = out_cols.iter().position(|c| c == rc) {
            out_vals[pos] = gathered;
        } else {
            out_cols.push(*rc);
            out_vals.push(gathered);
        }
    }

    Ok(K::Table(Arc::new(Flip { cols: out_cols, vals: out_vals })))
}

pub fn asof_op(lhs: &K, rhs: &K) -> KResult<K> {
    let lf = match lhs {
        K::Table(t) => (**t).clone(),
        K::KeyedTable(kt) => kt.unkey(),
        _ => return Err(KError::ty("asof LHS must be a table")),
    };
    let cols = match rhs {
        K::Dict(d) => {
            let K::Vector(ColVec::Sym(_, cs)) = &*d.keys else {
                return Err(KError::ty("asof RHS dictionary must have symbol keys"));
            };
            let probe_t = crate::engine::ops::enlist_op(rhs);
            let joined = asof_join(&K::Vector(ColVec::Sym(Attr::None, cs.clone())), &probe_t, lhs, true)?;
            return Ok(crate::engine::ops::first_op(&joined));
        }
        K::Table(rt) => rt.cols.clone(),
        _ => return Err(KError::ty("asof RHS must be dict or table")),
    };
    let _ = lf;
    asof_join(&K::Vector(ColVec::Sym(Attr::None, Arc::new(cols))), rhs, lhs, true)
}

// ----- Window Join (`wj`, `wj1`) --------------------------------------------

/// `wj[w; cols; t; (q; (agg1; `col1); (agg2; `col2); ...)]`
/// `w` is a 2-element list `(lo_times; hi_times)` parallel to `t`'s rows.
/// `wj` (`prevailing = true`) includes the prevailing quote at or just before `lo` in each window;
/// `wj1` (`prevailing = false`) considers strictly quotes inside `[lo, hi]`.
pub fn window_join(
    w_k: &K,
    cols_k: &K,
    lhs: &K,
    spec_k: &K,
    prevailing: bool,
    mut apply_fn: impl FnMut(&Func, &[K]) -> KResult<K>,
) -> KResult<K> {
    let K::Vector(w_pair) = w_k else {
        return Err(KError::ty("wj first argument must be a 2-element window list (lo; hi)"));
    };
    if w_pair.len() != 2 {
        return Err(KError::length("wj window list must have 2 elements (lo; hi)"));
    }
    let (K::Vector(lo_vec), K::Vector(hi_vec)) = (w_pair.get_atom(0), w_pair.get_atom(1)) else {
        return Err(KError::ty("wj window bounds must be vectors"));
    };

    let cols: Vec<SymId> = match cols_k {
        K::Sym(s) => vec![*s],
        K::Vector(ColVec::Sym(_, xs)) => (**xs).clone(),
        _ => return Err(KError::ty("wj second argument must be symbol or symbol vector")),
    };
    if cols.is_empty() {
        return Err(KError::domain("wj requires at least a time column"));
    }
    let lf = match lhs {
        K::Table(t) => (**t).clone(),
        K::KeyedTable(kt) => kt.unkey(),
        _ => return Err(KError::ty("wj third argument must be a table")),
    };
    if lo_vec.len() != lf.nrows() || hi_vec.len() != lf.nrows() {
        return Err(KError::length("wj window vectors must match LHS row count"));
    }

    let K::Vector(spec_list) = spec_k else {
        return Err(KError::ty("wj fourth argument must be (quote_table; (agg; `col); ...)"));
    };
    if spec_list.is_empty() {
        return Err(KError::length("wj spec cannot be empty"));
    }
    let q_val = spec_list.get_atom(0);
    let rf = match &q_val {
        K::Table(t) => (**t).clone(),
        K::KeyedTable(kt) => kt.unkey(),
        _ => return Err(KError::ty("wj spec[0] must be a quote table")),
    };

    let mut aggs: Vec<(Func, SymId, usize)> = Vec::new();
    for i in 1..spec_list.len() {
        let item = spec_list.get_atom(i);
        let K::Vector(pair) = item else {
            return Err(KError::ty("wj agg spec must be (func; `col)"));
        };
        if pair.len() != 2 {
            return Err(KError::length("wj agg spec must have 2 elements (func; `col)"));
        }
        let K::Func(func) = pair.get_atom(0) else {
            return Err(KError::ty("wj agg spec[0] must be a function"));
        };
        let K::Sym(col_sym) = pair.get_atom(1) else {
            return Err(KError::ty("wj agg spec[1] must be a column symbol"));
        };
        let ci = rf.col_idx(col_sym).ok_or_else(|| KError::value(resolve(col_sym)))?;
        aggs.push((func, col_sym, ci));
    }

    let eq_cols = &cols[..cols.len() - 1];
    let time_col = cols[cols.len() - 1];
    let mut l_eq = Vec::with_capacity(eq_cols.len());
    let mut r_eq = Vec::with_capacity(eq_cols.len());
    for &c in eq_cols {
        l_eq.push(&lf.vals[lf.col_idx(c).ok_or_else(|| KError::value(resolve(c)))?]);
        r_eq.push(&rf.vals[rf.col_idx(c).ok_or_else(|| KError::value(resolve(c)))?]);
    }
    let r_time = &rf.vals[rf.col_idx(time_col).ok_or_else(|| KError::value(resolve(time_col)))?];

    let mut buckets: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
    for r in 0..rf.nrows() {
        buckets.entry(row_hash(&r_eq, r)).or_default().push(r);
    }
    for rows in buckets.values_mut() {
        rows.sort_by(|&a, &b| r_time.get_atom(a).k_cmp(&r_time.get_atom(b)));
    }

    let nrows = lf.nrows();
    let mut agg_results: Vec<Vec<K>> = vec![Vec::with_capacity(nrows); aggs.len()];

    for r in 0..nrows {
        let lo_t = lo_vec.get_atom(r);
        let hi_t = hi_vec.get_atom(r);
        let win_rows: Vec<usize> = if let Some(b) = buckets.get(&row_hash(&l_eq, r)) {
            // First index `s` where `time >= lo_t`
            let mut l0 = 0usize;
            let mut h0 = b.len();
            while l0 < h0 {
                let m = l0 + (h0 - l0) / 2;
                if r_time.get_atom(b[m]).k_cmp(&lo_t) == Ordering::Less {
                    l0 = m + 1;
                } else {
                    h0 = m;
                }
            }
            // In `wj` (`prevailing = true`), if no quote is right at `lo_t` and `l0 > 0`, include `l0 - 1`!
            let start = if prevailing {
                if l0 < b.len() && r_time.get_atom(b[l0]).k_cmp(&lo_t) == Ordering::Equal {
                    l0
                } else {
                    l0.saturating_sub(1)
                }
            } else {
                l0
            };
            // Last index `< e` where `time <= hi_t`
            let mut l1 = 0usize;
            let mut h1 = b.len();
            while l1 < h1 {
                let m = l1 + (h1 - l1) / 2;
                if r_time.get_atom(b[m]).k_cmp(&hi_t) != Ordering::Greater {
                    l1 = m + 1;
                } else {
                    h1 = m;
                }
            }
            if start < l1 {
                b[start..l1].to_vec()
            } else if prevailing && l0 > 0 && l1 == l0 {
                vec![b[l0 - 1]]
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        for (ai, (func, _csym, ci)) in aggs.iter().enumerate() {
            let slice = K::Vector(rf.vals[*ci].gather(&win_rows));
            let val = apply_fn(func, &[slice])?;
            agg_results[ai].push(val);
        }
    }

    let mut out_cols = lf.cols;
    let mut out_vals = lf.vals;
    let mut name_counts: HashMap<SymId, usize> = HashMap::new();
    for ((_func, csym, _ci), col_atoms) in aggs.into_iter().zip(agg_results) {
        let cnt = name_counts.entry(csym).or_insert(0);
        let final_name = if *cnt == 0 && !out_cols.contains(&csym) {
            csym
        } else {
            crate::sym::intern(&format!("{}{}", resolve(csym), *cnt))
        };
        *cnt += 1;
        out_cols.push(final_name);
        out_vals.push(ColVec::from_atoms(col_atoms));
    }

    Ok(K::Table(Arc::new(Flip { cols: out_cols, vals: out_vals })))
}
