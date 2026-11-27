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
use std::borrow::Cow;
use std::cmp::Ordering;
use foldhash::{HashMap, HashMapExt};
use std::hash::{BuildHasher, Hasher};
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

// ----- The keys of keyed tables -------------------------------------------------------------------------------------

/// A keyed table with fewer rows is quick enough to read through.
const MANY_ROWS: usize = 1_000;

/// Where each key of a large keyed table is, so that a few are found without reading them all. It is built the second
/// time keys are looked for in the same columns, goes with them when `delta::patch` only adds rows, and is dropped
/// when they change otherwise.
#[derive(Clone)]
pub struct KeyIndex {
    /// The first row that holds each key, by the key's code: its bits for one column, a hash of them for several.
    rows: HashMap<u64, u32>,
    /// How many rows are in it.
    len: usize,
    /// Whether no key is there twice.
    unique: bool,
}

impl KeyIndex {
    /// The code of the key in row `r` of the columns whose bits these are.
    fn code(bits: &[Cow<'_, [u64]>], r: usize) -> u64 {
        match bits {
            [one] => one[r],
            several => {
                let mut hasher = foldhash::fast::FixedState::default().build_hasher();
                several.iter().for_each(|column| hasher.write_u64(column[r]));
                hasher.finish()
            }
        }
    }

    /// Add the rows of `columns` from the first that is not in it. `None`: two keys share a code, so there is no index.
    fn extend(&mut self, columns: &[ColVec]) -> Option<()> {
        let end = columns.first()?.len();
        let from = self.len;
        let added = |c: &ColVec| c.drop_n(from as i64).key_bits().map(|bits| Cow::Owned(bits.into_owned()));
        let bits: Vec<Cow<'_, [u64]>> = columns.iter().map(|c| if from == 0 { c.key_bits() } else { added(c) }).collect::<Option<_>>()?;
        for r in 0..end - from {
            let row = u32::try_from(from + r).ok()?;
            let first = *self.rows.entry(Self::code(&bits, r)).or_insert(row);
            if first != row {
                if columns.len() > 1 && columns.iter().any(|c| c.get_atom(first as usize) != c.get_atom(from + r)) {
                    return None;
                }
                self.unique = false;
            }
        }
        self.len = end;
        Some(())
    }

    /// The row of `columns` with the key that row `r` of the wanted columns holds, whose bits are `bits`.
    fn row(&self, columns: &[ColVec], wanted: &[&ColVec], bits: &[Cow<'_, [u64]>], r: usize) -> Option<usize> {
        let row = *self.rows.get(&Self::code(bits, r))? as usize;
        // (of several columns, another key may have the same code)
        (columns.len() == 1 || columns.iter().zip(wanted).all(|(c, w)| c.get_atom(row) == w.get_atom(r))).then_some(row)
    }
}

/// The columns of an index, to learn when one is gone.
type Held = Vec<Weak<dyn Any + Send + Sync>>;
/// The columns of an index and what is known of them: `None` while they have been asked for only once. The cache knows
/// them by where each is held and how long it is.
type KeyEntry = (Held, Option<Arc<KeyIndex>>);
static KEY_CACHE: LazyLock<Mutex<HashMap<Vec<CacheKey>, KeyEntry>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// What stands for `columns` in the cache, if they are of kinds that are indexed: keys that are equal as `=` has them
/// when their bits are, which floats are not.
fn identity(columns: &[ColVec]) -> Option<(Vec<CacheKey>, Held)> {
    let kind = |c: &ColVec| matches!(c.type_num(), 1 | 4..=7 | 10..=14 | 16..=19) && !matches!(c, ColVec::Enum(..));
    if columns.first()?.len() < MANY_ROWS || !columns.iter().all(kind) {
        return None;
    }
    Some(columns.iter().map(|c| {
        let (key, held) = backing_identity(c);
        (key, Arc::downgrade(&held))
    }).unzip())
}

fn remember(id: Vec<CacheKey>, entry: KeyEntry) {
    let mut cache = KEY_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    if cache.len() >= 256 {
        cache.retain(|_, (held, _)| held.iter().all(|w| w.strong_count() > 0));
        if cache.len() >= 256 {
            cache.clear();
        }
    }
    cache.insert(id, entry);
}

/// The index of the key `columns`, if they have been asked for before.
fn key_index(columns: &[ColVec]) -> Option<Arc<KeyIndex>> {
    let (id, held) = identity(columns)?;
    let known = {
        let cache = KEY_CACHE.lock().unwrap_or_else(|e| e.into_inner());
        cache.get(&id).filter(|(held, _)| held.iter().all(|w| w.strong_count() > 0)).map(|(_, index)| index.clone())
    };
    match known {
        Some(Some(index)) => Some(index),
        None => {
            remember(id, (held, None));
            None
        }
        Some(None) => {
            let mut index = KeyIndex { rows: HashMap::with_capacity(columns[0].len()), len: 0, unique: true };
            index.extend(columns)?;
            let index = Arc::new(index);
            remember(id, (held, Some(index.clone())));
            Some(index)
        }
    }
}

/// The first row of the key `columns` that holds each of the `n` keys in `wanted`. `None`: look some other way (the
/// keys are too few or not indexed, there are too many to look up for an index to help, or this is the first time).
pub fn key_rows(columns: &[ColVec], wanted: &[&ColVec], n: usize) -> Option<Vec<Option<usize>>> {
    let alike = columns.len() == wanted.len() && columns.iter().zip(wanted).all(|(c, w)| c.type_num() == w.type_num() && w.len() == n);
    if !alike || n.saturating_mul(4) >= columns.first()?.len() {
        return None;
    }
    let index = key_index(columns)?;
    let bits: Vec<Cow<'_, [u64]>> = wanted.iter().map(|w| w.key_bits()).collect::<Option<_>>()?;
    Some((0..n).map(|r| index.row(columns, wanted, &bits, r)).collect())
}

/// Take the index of `columns` out of the cache, to be given back by `keep_keys` once rows have been added to them. (Left
/// there, it would hold on to the columns, which would then be copied to be changed.)
pub(crate) fn take_keys(columns: &[ColVec]) -> Option<KeyEntry> {
    let (id, _) = identity(columns)?;
    KEY_CACHE.lock().unwrap_or_else(|e| e.into_inner()).remove(&id)
}

/// The index that `take_keys` gave, for the columns as they are with rows added.
pub(crate) fn keep_keys(columns: &[ColVec], taken: KeyEntry) {
    let Some((id, held)) = identity(columns) else { return };
    let index = taken.1.and_then(|mut index| {
        Arc::make_mut(&mut index).extend(columns)?;
        Some(index)
    });
    remember(id, (held, index));
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
    key: Option<SymId>,
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
        // the one key of a keyed table is found by the index of its keys, where no key is there twice
        if key == Some(col_sym) && eff_op == "=" && active.is_none() {
            if let Some(index) = key_index(std::slice::from_ref(cv)).filter(|index| index.unique) {
                let v = eval_rhs(val_expr)?;
                if v.type_num() == -cv.type_num() {
                    let wanted = ColVec::from_atoms(vec![v]);
                    return Ok(wanted.key_bits().map(|bits| index.row(std::slice::from_ref(cv), &[&wanted], &[bits], 0).into_iter().collect()));
                }
            }
        }
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
