//! Attribute-accelerated lookups for kdb+ column attributes:
//! - `` `s# `` (`Attr::Sorted`): `O(log N)` binary search on `=`, `<`, `<=`, `>`, `>=`, `in`, `within`, `?`, and joins.
//! - `` `u# `` (`Attr::Unique`): cached `O(1)` hash index (`key -> row`).
//! - `` `p# `` (`Attr::Parted`): cached `O(1)` run-range index (`key -> start..end`, e.g. HDB `sym`).
//! - `` `g# `` (`Attr::Grouped`): cached `O(1)` inverted index (`key -> Vec<row>`).
use crate::ast::Expr;
use crate::error::KResult;
use crate::sym::{resolve, SymId};
use crate::types::{Attr, ColVec, Flip, K};
use std::any::Any;
use std::cmp::Ordering;
use foldhash::{HashMap, HashMapExt};
use std::ops::Range;
use std::sync::{Arc, LazyLock, Mutex, Weak};

enum AttrIndex {
    Unique(HashMap<Vec<u8>, usize>),
    Parted(HashMap<Vec<u8>, Range<usize>>),
    Grouped(HashMap<Vec<u8>, Vec<usize>>),
}

type CacheKey = (usize, usize, u8);
type CacheEntry = (Weak<dyn Any + Send + Sync>, Arc<AttrIndex>);

static INDEX_CACHE: LazyLock<Mutex<HashMap<CacheKey, CacheEntry>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn backing_identity(cv: &ColVec) -> (CacheKey, Arc<dyn Any + Send + Sync>) {
    let ab = cv.attr().ipc_byte();
    macro_rules! id {
        ($v:expr) => {{
            let ptr = Arc::as_ptr($v) as usize;
            let len = $v.len();
            let any: Arc<dyn Any + Send + Sync> = $v.clone();
            ((ptr, len, ab), any)
        }};
    }
    match cv {
        ColVec::General(_, v) => id!(v),
        ColVec::Strings(_, v) => id!(v),
        ColVec::Bool(_, v) => id!(v),
        ColVec::Guid(_, v) => id!(v),
        ColVec::Byte(_, v) => id!(v),
        ColVec::Short(_, v) => id!(v),
        ColVec::Int(_, v) => id!(v),
        ColVec::Long(_, v) => id!(v),
        ColVec::Real(_, v) => id!(v),
        ColVec::Float(_, v) => id!(v),
        ColVec::Char(_, v) => id!(v),
        ColVec::Sym(_, v) => id!(v),
        ColVec::Timestamp(_, v) => id!(v),
        ColVec::Month(_, v) => id!(v),
        ColVec::Date(_, v) => id!(v),
        ColVec::Datetime(_, v) => id!(v),
        ColVec::Timespan(_, v) => id!(v),
        ColVec::Minute(_, v) => id!(v),
        ColVec::Second(_, v) => id!(v),
        ColVec::Time(_, v) => id!(v),
        ColVec::Enum(_, v) => backing_identity(v),
    }
}

fn get_or_build_index(cv: &ColVec) -> Option<Arc<AttrIndex>> {
    let attr = cv.attr();
    if !matches!(attr, Attr::Unique | Attr::Parted | Attr::Grouped) {
        return None;
    }
    let (key, backing) = backing_identity(cv);
    {
        let cache = INDEX_CACHE.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((weak, idx)) = cache.get(&key) {
            if weak.upgrade().is_some() {
                return Some(idx.clone());
            }
        }
    }
    let n = cv.len();
    let built = match attr {
        Attr::Unique => {
            let mut m = HashMap::with_capacity(n);
            for i in 0..n {
                m.entry(cv.get_atom(i).hash_key()).or_insert(i);
            }
            AttrIndex::Unique(m)
        }
        Attr::Parted => {
            let mut m = HashMap::new();
            let mut i = 0;
            while i < n {
                let hk = cv.get_atom(i).hash_key();
                let mut j = i + 1;
                while j < n && cv.get_atom(j).hash_key() == hk {
                    j += 1;
                }
                m.entry(hk).or_insert(i..j);
                i = j;
            }
            AttrIndex::Parted(m)
        }
        Attr::Grouped => {
            let mut m: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
            for i in 0..n {
                m.entry(cv.get_atom(i).hash_key()).or_default().push(i);
            }
            AttrIndex::Grouped(m)
        }
        _ => return None,
    };
    let arc_idx = Arc::new(built);
    let mut cache = INDEX_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    if cache.len() >= 256 {
        cache.retain(|_, (w, _)| w.strong_count() > 0);
        if cache.len() >= 256 {
            cache.clear();
        }
    }
    cache.insert(key, (Arc::downgrade(&backing), arc_idx.clone()));
    Some(arc_idx)
}

/// Binary-search lower bound: first index `i` where `cv[i] >= target`.
fn lower_bound(cv: &ColVec, target: &K) -> usize {
    match (cv, target) {
        (ColVec::Sym(_, xs), K::Sym(t)) => xs.partition_point(|x| x < t),
        (ColVec::Long(_, xs), K::Long(t)) => xs.partition_point(|x| x < t),
        (ColVec::Int(_, xs), K::Int(t)) => xs.partition_point(|x| x < t),
        (ColVec::Date(_, xs), K::Date(t)) => xs.partition_point(|x| x < t),
        (ColVec::Timestamp(_, xs), K::Timestamp(t)) => xs.partition_point(|x| x < t),
        _ => {
            let mut lo = 0usize;
            let mut hi = cv.len();
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                if cv.get_atom(mid).k_cmp(target) == Ordering::Less {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            lo
        }
    }
}

/// Binary-search upper bound: first index `i` where `cv[i] > target`.
fn upper_bound(cv: &ColVec, target: &K) -> usize {
    match (cv, target) {
        (ColVec::Sym(_, xs), K::Sym(t)) => xs.partition_point(|x| x <= t),
        (ColVec::Long(_, xs), K::Long(t)) => xs.partition_point(|x| x <= t),
        (ColVec::Int(_, xs), K::Int(t)) => xs.partition_point(|x| x <= t),
        (ColVec::Date(_, xs), K::Date(t)) => xs.partition_point(|x| x <= t),
        (ColVec::Timestamp(_, xs), K::Timestamp(t)) => xs.partition_point(|x| x <= t),
        _ => {
            let mut lo = 0usize;
            let mut hi = cv.len();
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                if cv.get_atom(mid).k_cmp(target) != Ordering::Greater {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            lo
        }
    }
}

pub fn sorted_eq_range(cv: &ColVec, needle: &K) -> Range<usize> {
    let lo = lower_bound(cv, needle);
    if lo >= cv.len() || cv.get_atom(lo).k_cmp(needle) != Ordering::Equal {
        lo..lo
    } else {
        lo..upper_bound(cv, needle)
    }
}

/// Whether `find_first` can look `hay` up: it has an attribute, other than `s#` on a general list, whose order compares
/// numbers of unlike types by value where a search without it tells them apart.
pub fn finds(hay: &ColVec) -> bool {
    match hay.attr() {
        Attr::None => false,
        Attr::Sorted => !hay.is_list(),
        _ => true,
    }
}

/// Accelerated first-match lookup (`vec ? atom`, `lj`, `ij`, keyed table probe). Returns `Some(hay.len())` on miss when accelerated.
pub fn find_first(hay: &ColVec, needle: &K) -> Option<usize> {
    if !finds(hay) {
        return None;
    }
    match hay.attr() {
        Attr::None => None,
        Attr::Sorted => {
            let r = sorted_eq_range(hay, needle);
            Some(if r.start < r.end { r.start } else { hay.len() })
        }
        Attr::Unique | Attr::Parted | Attr::Grouped => {
            let idx = get_or_build_index(hay)?;
            let hk = needle.hash_key();
            let found = match &*idx {
                AttrIndex::Unique(m) => m.get(&hk).copied(),
                AttrIndex::Parted(m) => m.get(&hk).map(|r| r.start),
                AttrIndex::Grouped(m) => m.get(&hk).and_then(|v| v.first().copied()),
            };
            Some(found.unwrap_or_else(|| hay.len()))
        }
    }
}

fn types_compatible(cv: &ColVec, atom: &K) -> bool {
    if !atom.is_atom() {
        return false;
    }
    let ct = -cv.type_num();
    let at = atom.type_num();
    if ct == at {
        return true;
    }
    // Numeric cross-type comparisons (`int` vs `long` vs `float`, etc.) are valid in q
    cv.is_numeric() && atom.as_f64().is_ok()
}

fn append_eq_rows(cv: &ColVec, needle: &K, out: &mut Vec<usize>) -> bool {
    // Coerce numeric needle into column's exact atom representation if needed for hash_key lookup
    let coerced;
    let target = if -cv.type_num() == needle.type_num() || cv.attr() == Attr::Sorted {
        needle
    } else if cv.is_numeric() {
        coerced = coerce_numeric_to_col(cv, needle);
        match &coerced {
            Some(k) => k,
            None => return true, // e.g. `int_col = 3.5` matches 0 rows!
        }
    } else {
        return false;
    };

    match cv.attr() {
        Attr::None => return false,
        Attr::Sorted => out.extend(sorted_eq_range(cv, target)),
        Attr::Unique | Attr::Parted | Attr::Grouped => {
            let Some(idx) = get_or_build_index(cv) else { return false };
            let hk = target.hash_key();
            match &*idx {
                AttrIndex::Unique(m) => {
                    if let Some(&r) = m.get(&hk) {
                        out.push(r);
                    }
                }
                AttrIndex::Parted(m) => {
                    if let Some(rng) = m.get(&hk) {
                        out.extend(rng.clone());
                    }
                }
                AttrIndex::Grouped(m) => {
                    if let Some(rows) = m.get(&hk) {
                        out.extend_from_slice(rows);
                    }
                }
            }
        }
    }
    true
}

fn coerce_numeric_to_col(cv: &ColVec, atom: &K) -> Option<K> {
    let f = atom.as_f64().ok()?;
    Some(match cv {
        ColVec::Float(_, _) => K::Float(f),
        ColVec::Real(_, _) => K::Real(f as f32),
        ColVec::Long(_, _) => {
            let i = atom.as_i64().ok().or_else(|| (f.fract() == 0.0).then_some(f as i64))?;
            K::Long(i)
        }
        ColVec::Int(_, _) => {
            let i = atom.as_i64().ok().or_else(|| (f.fract() == 0.0).then_some(f as i64))?;
            K::Int(i32::try_from(i).ok()?)
        }
        ColVec::Short(_, _) => {
            let i = atom.as_i64().ok().or_else(|| (f.fract() == 0.0).then_some(f as i64))?;
            K::Short(i16::try_from(i).ok()?)
        }
        ColVec::Byte(_, _) => {
            let i = atom.as_i64().ok().or_else(|| (f.fract() == 0.0).then_some(f as i64))?;
            K::Byte(u8::try_from(i).ok()?)
        }
        ColVec::Bool(_, _) => {
            let i = atom.as_i64().ok()?;
            if i == 0 || i == 1 { K::Bool(i == 1) } else { return None }
        }
        _ => return None,
    })
}

fn intersect_sorted(a: &[usize], mut b: Vec<usize>) -> Vec<usize> {
    b.sort_unstable();
    b.dedup();
    let mut out = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            Ordering::Less => i += 1,
            Ordering::Greater => j += 1,
            Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

fn expr_uses_table_cols(e: &Expr, cols: &[SymId]) -> bool {
    e.any(&mut |x| match x {
        Expr::Var(s) | Expr::Field(_, s) => cols.contains(s) || resolve(*s) == "i",
        _ => false,
    })
}

/// Attempt to evaluate a `qSQL` `where` predicate directly against an attributed column of `flip`.
pub fn try_filter_where(
    flip: &Flip,
    active: Option<&[usize]>,
    wc: &Expr,
    eval_rhs: &mut impl FnMut(&Expr) -> KResult<K>,
) -> KResult<Option<Vec<usize>>> {
    let Expr::Binary { op, lhs, rhs } = wc else { return Ok(None) };
    let op_str = resolve(*op);

    // Support both `col op expr` and `expr = col`
    let (col_sym, val_expr, eff_op) = match (&**lhs, &**rhs, op_str) {
        (Expr::Var(c), r, _) if flip.col(*c).is_some() && !expr_uses_table_cols(r, &flip.cols) => (*c, r, op_str),
        (l, Expr::Var(c), "=") if flip.col(*c).is_some() && !expr_uses_table_cols(l, &flip.cols) => (*c, l, "="),
        _ => return Ok(None),
    };

    let cv = flip.col(col_sym).unwrap();
    if cv.attr() == Attr::None {
        return Ok(None);
    }

    let mut matched = Vec::new();
    match eff_op {
        "=" => {
            let v = eval_rhs(val_expr)?;
            if !types_compatible(cv, &v) || !append_eq_rows(cv, &v, &mut matched) {
                return Ok(None);
            }
        }
        "in" => {
            let v = eval_rhs(val_expr)?;
            match v {
                K::Vector(needles) => {
                    for i in 0..needles.len() {
                        let item = needles.get_atom(i);
                        if !types_compatible(cv, &item) || !append_eq_rows(cv, &item, &mut matched) {
                            return Ok(None);
                        }
                    }
                    matched.sort_unstable();
                    matched.dedup();
                }
                atom => {
                    if !types_compatible(cv, &atom) || !append_eq_rows(cv, &atom, &mut matched) {
                        return Ok(None);
                    }
                }
            }
        }
        "<" | "<=" | ">" | ">=" if cv.attr() == Attr::Sorted => {
            let v = eval_rhs(val_expr)?;
            if !types_compatible(cv, &v) {
                return Ok(None);
            }
            let n = cv.len();
            let rng = match eff_op {
                "<" => 0..lower_bound(cv, &v),
                "<=" => 0..upper_bound(cv, &v),
                ">" => upper_bound(cv, &v)..n,
                ">=" => lower_bound(cv, &v)..n,
                _ => unreachable!(),
            };
            matched.extend(rng);
        }
        "within" if cv.attr() == Attr::Sorted => {
            let v = eval_rhs(val_expr)?;
            let K::Vector(bounds) = v else { return Ok(None) };
            if bounds.len() != 2 {
                return Ok(None);
            }
            let lo = bounds.get_atom(0);
            let hi = bounds.get_atom(1);
            if !types_compatible(cv, &lo) || !types_compatible(cv, &hi) {
                return Ok(None);
            }
            let start = lower_bound(cv, &lo);
            let end = upper_bound(cv, &hi);
            if start < end {
                matched.extend(start..end);
            }
        }
        _ => return Ok(None),
    }

    Ok(Some(match active {
        None => matched,
        Some(cur) => intersect_sorted(cur, matched),
    }))
}
