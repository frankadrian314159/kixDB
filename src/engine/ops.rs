//! Vectorized unary and binary `q` primitives, casts, aggregations, moving windows, and temporal extractors.
use crate::error::{KError, KResult};
use crate::sym::{intern, resolve, with_str, SymId};
use crate::types::*;
use chrono::Datelike;
use regex::Regex;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

// ----- Temporal / Dict / Table `.field` Extraction --------------------------

pub fn eval_field(val: &K, field: SymId) -> KResult<K> {
    let fname = resolve(field);
    // Temporal component extraction (`date.year`, `date.mm`, `date.dd`, `time.hh`, `time.uu`, `time.ss`, `time.minute`, `time.second`, `ts.date`, `ts.time`, `ts.minute`)
    if let Some(extracted) = try_temporal_field(val, &fname) {
        return Ok(extracted);
    }
    match val {
        K::Table(t) => t
            .col(field)
            .map(|c| K::Vector(c.clone()))
            .ok_or_else(|| KError::value(fname)),
        K::KeyedTable(kt) => {
            let f = kt.unkey();
            f.col(field)
                .map(|c| K::Vector(c.clone()))
                .ok_or_else(|| KError::value(fname))
        }
        K::Dict(d) => dict_lookup(d, &K::Sym(field)),
        _ => Err(KError::ty(format!("cannot access .{fname} on type {}h", val.type_num()))),
    }
}

fn try_temporal_field(val: &K, field: &str) -> Option<K> {
    match val {
        K::Date(d) => date_part(*d, field),
        K::Month(m) => month_part(*m, field),
        K::Timestamp(ns) => timestamp_part(*ns, field),
        K::Timespan(ns) => timespan_part(*ns, field),
        K::Time(ms) => time_part(*ms, field),
        K::Second(s) => second_part(*s, field),
        K::Minute(m) => minute_part(*m, field),
        K::Vector(ColVec::Date(_, xs)) => vec_map_temporal(xs, |&d| date_part(d, field)),
        K::Vector(ColVec::Month(_, xs)) => vec_map_temporal(xs, |&m| month_part(m, field)),
        K::Vector(ColVec::Timestamp(_, xs)) => vec_map_temporal(xs, |&ns| timestamp_part(ns, field)),
        K::Vector(ColVec::Timespan(_, xs)) => vec_map_temporal(xs, |&ns| timespan_part(ns, field)),
        K::Vector(ColVec::Time(_, xs)) => vec_map_temporal(xs, |&ms| time_part(ms, field)),
        K::Vector(ColVec::Second(_, xs)) => vec_map_temporal(xs, |&s| second_part(s, field)),
        K::Vector(ColVec::Minute(_, xs)) => vec_map_temporal(xs, |&m| minute_part(m, field)),
        _ => None,
    }
}

fn vec_map_temporal<T>(xs: &[T], f: impl Fn(&T) -> Option<K>) -> Option<K> {
    if xs.is_empty() {
        // Probe with zero to get output vector type, then take 0
        let sample = unsafe { std::mem::zeroed::<T>() };
        let atom = f(&sample)?;
        return Some(K::Vector(ColVec::from_atoms(vec![atom]).take(0)));
    }
    let mut out = Vec::with_capacity(xs.len());
    for x in xs {
        out.push(f(x)?);
    }
    Some(K::Vector(ColVec::from_atoms(out)))
}

fn date_part(days: i32, field: &str) -> Option<K> {
    if days == NULL_INT {
        return match field {
            "year" | "mm" | "dd" => Some(K::Int(NULL_INT)),
            "month" => Some(K::Month(NULL_INT)),
            "date" => Some(K::Date(NULL_INT)),
            _ => None,
        };
    }
    let (y, m, d) = ymd_from_date(days)?;
    match field {
        "year" => Some(K::Int(y)),
        "mm" => Some(K::Int(m as i32)),
        "dd" => Some(K::Int(d as i32)),
        "month" => Some(K::Month((y - 2000) * 12 + (m as i32 - 1))),
        "date" => Some(K::Date(days)),
        "week" => {
            // Monday of week in kdb+
            let epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1)?;
            let nd = epoch.checked_add_signed(chrono::Duration::days(days as i64))?;
            let wd = nd.weekday().num_days_from_monday() as i32;
            Some(K::Date(days - wd))
        }
        _ => None,
    }
}

fn month_part(months: i32, field: &str) -> Option<K> {
    if months == NULL_INT {
        return match field {
            "year" | "mm" => Some(K::Int(NULL_INT)),
            "month" => Some(K::Month(NULL_INT)),
            _ => None,
        };
    }
    let total = 2000 * 12 + months;
    let y = total.div_euclid(12);
    let m = total.rem_euclid(12) + 1;
    match field {
        "year" => Some(K::Int(y)),
        "mm" => Some(K::Int(m)),
        "month" => Some(K::Month(months)),
        _ => None,
    }
}

fn timestamp_part(ns: i64, field: &str) -> Option<K> {
    if ns == NULL_LONG {
        return match field {
            "date" => Some(K::Date(NULL_INT)),
            "month" => Some(K::Month(NULL_INT)),
            "year" | "mm" | "dd" | "hh" | "uu" | "ss" => Some(K::Int(NULL_INT)),
            "time" => Some(K::Time(NULL_INT)),
            "minute" => Some(K::Minute(NULL_INT)),
            "second" => Some(K::Second(NULL_INT)),
            "timespan" => Some(K::Timespan(NULL_LONG)),
            _ => None,
        };
    }
    let days = ns.div_euclid(NANOS_PER_DAY) as i32;
    let rem = ns.rem_euclid(NANOS_PER_DAY);
    match field {
        "date" | "year" | "mm" | "dd" | "month" | "week" => date_part(days, field),
        "timespan" => Some(K::Timespan(rem)),
        "time" => Some(K::Time((rem / NANOS_PER_MILLI) as i32)),
        "second" => Some(K::Second((rem / NANOS_PER_SEC) as i32)),
        "minute" => Some(K::Minute((rem / NANOS_PER_MIN) as i32)),
        "hh" => Some(K::Int((rem / NANOS_PER_HOUR) as i32)),
        "uu" => Some(K::Int(((rem / NANOS_PER_MIN) % 60) as i32)),
        "ss" => Some(K::Int(((rem / NANOS_PER_SEC) % 60) as i32)),
        _ => None,
    }
}

fn timespan_part(ns: i64, field: &str) -> Option<K> {
    if ns == NULL_LONG {
        return timestamp_part(NULL_LONG, field);
    }
    let rem = ns.rem_euclid(NANOS_PER_DAY);
    match field {
        "time" => Some(K::Time((rem / NANOS_PER_MILLI) as i32)),
        "second" => Some(K::Second((rem / NANOS_PER_SEC) as i32)),
        "minute" => Some(K::Minute((rem / NANOS_PER_MIN) as i32)),
        "hh" => Some(K::Int((rem / NANOS_PER_HOUR) as i32)),
        "uu" => Some(K::Int(((rem / NANOS_PER_MIN) % 60) as i32)),
        "ss" => Some(K::Int(((rem / NANOS_PER_SEC) % 60) as i32)),
        _ => None,
    }
}

fn time_part(ms: i32, field: &str) -> Option<K> {
    if ms == NULL_INT {
        return match field {
            "hh" | "uu" | "ss" => Some(K::Int(NULL_INT)),
            "minute" => Some(K::Minute(NULL_INT)),
            "second" => Some(K::Second(NULL_INT)),
            "time" => Some(K::Time(NULL_INT)),
            _ => None,
        };
    }
    let secs = ms.div_euclid(1000);
    match field {
        "hh" => Some(K::Int(secs / 3600)),
        "uu" => Some(K::Int((secs / 60) % 60)),
        "ss" => Some(K::Int(secs % 60)),
        "minute" => Some(K::Minute(secs / 60)),
        "second" => Some(K::Second(secs)),
        "time" => Some(K::Time(ms)),
        _ => None,
    }
}

fn second_part(secs: i32, field: &str) -> Option<K> {
    if secs == NULL_INT {
        return time_part(NULL_INT, field);
    }
    match field {
        "hh" => Some(K::Int(secs / 3600)),
        "uu" => Some(K::Int((secs / 60) % 60)),
        "ss" => Some(K::Int(secs % 60)),
        "minute" => Some(K::Minute(secs / 60)),
        "second" => Some(K::Second(secs)),
        _ => None,
    }
}

fn minute_part(mins: i32, field: &str) -> Option<K> {
    if mins == NULL_INT {
        return match field {
            "hh" | "uu" => Some(K::Int(NULL_INT)),
            "minute" => Some(K::Minute(NULL_INT)),
            _ => None,
        };
    }
    match field {
        "hh" => Some(K::Int(mins / 60)),
        "uu" => Some(K::Int(mins % 60)),
        "minute" => Some(K::Minute(mins)),
        _ => None,
    }
}

/// `md5 "text"`: the 16-byte digest of a string.
fn md5_op(x: &K) -> KResult<K> {
    use md5::{Digest, Md5};
    let text = match x {
        K::Char(c) => std::slice::from_ref(c),
        K::Vector(ColVec::Char(_, bytes)) => bytes.as_slice(),
        _ => return Err(KError::ty("md5 expects a string")),
    };
    Ok(K::Vector(ColVec::Byte(Attr::None, Arc::new(Md5::digest(text).to_vec()))))
}

// ----- Unary Primitives -----------------------------------------------------

pub fn eval_unary_prim(op: &str, x: &K) -> KResult<K> {
    match op {
        "-" | "neg" => unary_num(x, &|i| if i == NULL_LONG { NULL_LONG } else { i.wrapping_neg() }, &|f| -f),
        "~" | "not" => unary_not(x),
        "null" => unary_null(x),
        "abs" => unary_num(x, &|i| if i == NULL_LONG { NULL_LONG } else { i.abs() }, &f64::abs),
        "signum" => signum_op(x),
        "floor" | "_" => floor_op(x, true),
        "ceiling" => floor_op(x, false),
        "reciprocal" => unary_float(x, |f| 1.0 / f),
        "sqrt" => unary_float(x, |f| if f < 0.0 { f64::NAN } else { f.sqrt() }),
        "exp" => unary_float(x, f64::exp),
        "log" => unary_float(x, |f| if f <= 0.0 { f64::NAN } else { f.ln() }),
        "sin" => unary_float(x, f64::sin),
        "cos" => unary_float(x, f64::cos),
        "tan" => unary_float(x, f64::tan),
        "asin" => unary_float(x, f64::asin),
        "acos" => unary_float(x, f64::acos),
        "atan" => unary_float(x, f64::atan),
        "," | "enlist" => Ok(enlist_op(x)),
        "#" | "count" => Ok(K::Long(x.count() as i64)),
        "*" | "first" => Ok(first_op(x)),
        "last" => Ok(last_op(x)),
        "!" => match x {
            K::Long(n) => til_op(*n),
            K::Int(n) => til_op(*n as i64),
            K::Short(n) => til_op(*n as i64),
            _ => key_op(x),
        },
        "til" => til_op(x.as_i64()?),
        "key" => key_op(x),
        "keys" => keys_op(x),
        "cols" => cols_op(x),
        "value" => value_op(x),
        "@" | "type" => Ok(K::Short(x.type_num())),
        "+" | "flip" => flip_op(x),
        "|" | "reverse" => reverse_op(x),
        "?" | "distinct" => distinct_op(x),
        "&" | "where" => where_op(x),
        "=" | "group" => group_op(x),
        "<" | "iasc" => iasc_op(x, false),
        ">" | "idesc" => iasc_op(x, true),
        "asc" | "sorts" => sort_op(x, false),
        "desc" => sort_op(x, true),
        "raze" => raze_op(x),
        "$" | "string" => string_op(x),
        "upper" => str_transform(x, &|s| s.to_ascii_uppercase()),
        "lower" => str_transform(x, &|s| s.to_ascii_lowercase()),
        "trim" => str_transform(x, &|s| s.trim().to_string()),
        "ltrim" => str_transform(x, &|s| s.trim_start().to_string()),
        "rtrim" => str_transform(x, &|s| s.trim_end().to_string()),
        "sum" => Ok(sum_op(x)?),
        "prd" => Ok(prd_op(x)?),
        "min" => Ok(minmax_op(x, true)?),
        "max" => Ok(minmax_op(x, false)?),
        "avg" => Ok(avg_op(x)?),
        "med" => Ok(med_op(x)?),
        "var" => Ok(var_op(x, false, false)?),
        "svar" => Ok(var_op(x, true, false)?),
        "dev" => Ok(var_op(x, false, true)?),
        "sdev" => Ok(var_op(x, true, true)?),
        "sums" => running_sum(x),
        "prds" => running_prd(x),
        "mins" => running_minmax(x, true),
        "maxs" => running_minmax(x, false),
        "avgs" => running_avg(x),
        "deltas" => deltas_op(x),
        "ratios" => ratios_op(x),
        "differ" => differ_op(x),
        "prev" => shift_op(x, -1),
        "next" => shift_op(x, 1),
        "fills" => fills_op(x),
        "meta" => meta_op(x),
        "ungroup" => ungroup_op(x),
        "md5" => md5_op(x),
        _ => Err(KError::nyi(format!("unknown unary verb '{op}'"))),
    }
}

fn unary_num(x: &K, fi: &dyn Fn(i64) -> i64, ff: &dyn Fn(f64) -> f64) -> KResult<K> {
    match x {
        K::Bool(b) => Ok(K::Long(fi(i64::from(*b)))),
        K::Byte(v) => Ok(K::Long(fi(*v as i64))),
        K::Short(v) => Ok(K::Short(if *v == NULL_SHORT { NULL_SHORT } else { fi(*v as i64) as i16 })),
        K::Int(v) => Ok(K::Int(if *v == NULL_INT { NULL_INT } else { fi(*v as i64) as i32 })),
        K::Long(v) => Ok(K::Long(fi(*v))),
        K::Timespan(v) => Ok(K::Timespan(fi(*v))),
        K::Time(v) => Ok(K::Time(if *v == NULL_INT { NULL_INT } else { fi(*v as i64) as i32 })),
        K::Second(v) => Ok(K::Second(if *v == NULL_INT { NULL_INT } else { fi(*v as i64) as i32 })),
        K::Minute(v) => Ok(K::Minute(if *v == NULL_INT { NULL_INT } else { fi(*v as i64) as i32 })),
        K::Real(v) => Ok(K::Real(if v.is_nan() { f32::NAN } else { ff(*v as f64) as f32 })),
        K::Float(v) => Ok(K::Float(if v.is_nan() { f64::NAN } else { ff(*v) })),
        K::Vector(v) => match v {
            ColVec::Bool(_, xs) => Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(xs.iter().map(|&b| fi(i64::from(b))).collect())))),
            ColVec::Short(_, xs) => Ok(K::Vector(ColVec::Short(Attr::None, Arc::new(xs.iter().map(|&n| if n == NULL_SHORT { NULL_SHORT } else { fi(n as i64) as i16 }).collect())))),
            ColVec::Int(_, xs) => Ok(K::Vector(ColVec::Int(Attr::None, Arc::new(xs.iter().map(|&n| if n == NULL_INT { NULL_INT } else { fi(n as i64) as i32 }).collect())))),
            ColVec::Long(_, xs) => Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(xs.iter().map(|&n| fi(n)).collect())))),
            ColVec::Real(_, xs) => Ok(K::Vector(ColVec::Real(Attr::None, Arc::new(xs.iter().map(|&n| if n.is_nan() { f32::NAN } else { ff(n as f64) as f32 }).collect())))),
            ColVec::Float(_, xs) => Ok(K::Vector(ColVec::Float(Attr::None, Arc::new(xs.iter().map(|&n| if n.is_nan() { f64::NAN } else { ff(n) }).collect())))),
            ColVec::General(_, xs) => {
                let items: KResult<Vec<K>> = xs.iter().map(|e| unary_num(e, fi, ff)).collect();
                Ok(K::Vector(ColVec::from_atoms(items?)))
            }
            _ => Err(KError::ty("numeric unary operation requires numbers")),
        },
        K::Dict(d) => Ok(K::Dict(Arc::new(Dict { keys: d.keys.clone(), vals: Box::new(unary_num(&d.vals, fi, ff)?) }))),
        _ => Err(KError::ty("numeric unary operation requires numbers")),
    }
}

fn unary_float(x: &K, ff: impl Fn(f64) -> f64) -> KResult<K> {
    if x.is_atom() {
        let v = x.as_f64()?;
        Ok(K::Float(if v.is_nan() { f64::NAN } else { ff(v) }))
    } else if let K::Vector(cv) = x {
        let fs = cv.as_f64_vec().ok_or_else(|| KError::ty("expected numeric vector"))?;
        Ok(K::Vector(ColVec::Float(Attr::None, Arc::new(fs.into_iter().map(|v| if v.is_nan() { f64::NAN } else { ff(v) }).collect()))))
    } else {
        Err(KError::ty("expected numeric argument"))
    }
}

fn unary_not(x: &K) -> KResult<K> {
    if x.is_atom() {
        let z = match x {
            K::Bool(b) => !*b,
            K::Byte(b) => *b == 0,
            K::Short(n) => *n == 0,
            K::Int(n) => *n == 0,
            K::Long(n) => *n == 0,
            K::Real(f) => *f == 0.0,
            K::Float(f) => *f == 0.0,
            _ => return Err(KError::ty("not requires numeric or boolean")),
        };
        Ok(K::Bool(z))
    } else if let K::Vector(cv) = x {
        match cv {
            ColVec::Bool(_, xs) => Ok(K::Vector(ColVec::Bool(Attr::None, Arc::new(xs.iter().map(|b| !*b).collect())))),
            _ => {
                let fs = cv.as_f64_vec().ok_or_else(|| KError::ty("not requires numeric or boolean"))?;
                Ok(K::Vector(ColVec::Bool(Attr::None, Arc::new(fs.into_iter().map(|v| v == 0.0).collect()))))
            }
        }
    } else {
        Err(KError::ty("not requires numeric or boolean"))
    }
}

fn unary_null(x: &K) -> KResult<K> {
    if let K::Vector(cv) = x {
        let flags: Vec<bool> = (0..cv.len()).map(|i| cv.get_atom(i).is_null()).collect();
        Ok(K::Vector(ColVec::Bool(Attr::None, Arc::new(flags))))
    } else {
        Ok(K::Bool(x.is_null()))
    }
}

fn signum_op(x: &K) -> KResult<K> {
    let sgn = |v: f64| -> i32 {
        if v.is_nan() { NULL_INT } else if v > 0.0 { 1 } else if v < 0.0 { -1 } else { 0 }
    };
    if x.is_atom() {
        Ok(K::Int(sgn(x.as_f64()?)))
    } else if let K::Vector(cv) = x {
        let fs = cv.as_f64_vec().ok_or_else(|| KError::ty("signum requires numbers"))?;
        Ok(K::Vector(ColVec::Int(Attr::None, Arc::new(fs.into_iter().map(sgn).collect()))))
    } else {
        Err(KError::ty("signum requires numbers"))
    }
}

fn floor_op(x: &K, is_floor: bool) -> KResult<K> {
    // In kdb+, `_ "ABC"` lowercases chars/strings!
    if is_floor {
        match x {
            K::Char(c) => return Ok(K::Char(c.to_ascii_lowercase())),
            K::Vector(ColVec::Char(_, bs)) => {
                return Ok(K::Vector(ColVec::Char(Attr::None, Arc::new(bs.iter().map(|b| b.to_ascii_lowercase()).collect()))));
            }
            _ => {}
        }
    }
    let conv = |v: f64| -> i64 {
        if v.is_nan() {
            NULL_LONG
        } else if v == f64::INFINITY {
            INF_LONG
        } else if v == f64::NEG_INFINITY {
            NINF_LONG
        } else if is_floor {
            v.floor() as i64
        } else {
            v.ceil() as i64
        }
    };
    if x.is_atom() {
        if matches!(x, K::Long(_) | K::Int(_) | K::Short(_) | K::Date(_)) {
            return Ok(x.clone());
        }
        Ok(K::Long(conv(x.as_f64()?)))
    } else if let K::Vector(cv) = x {
        if matches!(cv, ColVec::Long(_, _) | ColVec::Int(_, _) | ColVec::Short(_, _) | ColVec::Date(_, _)) {
            return Ok(x.clone());
        }
        let fs = cv.as_f64_vec().ok_or_else(|| KError::ty("floor/ceiling requires numbers"))?;
        Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(fs.into_iter().map(conv).collect()))))
    } else {
        Err(KError::ty("floor/ceiling requires numbers"))
    }
}

pub fn enlist_op(x: &K) -> K {
    if x.is_atom() {
        K::Vector(ColVec::from_atoms(vec![x.clone()]))
    } else if let K::Dict(d) = x {
        // `enlist `a`b!1 2` creates a 1-row Table!
        if let K::Vector(ColVec::Sym(_, cols)) = &*d.keys {
            let mut vals = Vec::with_capacity(cols.len());
            for i in 0..cols.len() {
                let cell = index_k(&d.vals, &[Some(K::Long(i as i64))]).unwrap_or(K::Null);
                vals.push(ColVec::from_atoms(vec![cell]));
            }
            if let Ok(f) = Flip::new((**cols).clone(), vals) {
                return K::Table(Arc::new(f));
            }
        }
        K::Vector(ColVec::General(Attr::None, Arc::new(vec![x.clone()])))
    } else {
        K::Vector(ColVec::General(Attr::None, Arc::new(vec![x.clone()])))
    }
}

pub fn first_op(x: &K) -> K {
    match x {
        K::Vector(v) => v.get_atom(0),
        K::Table(t) => {
            if t.nrows() == 0 {
                let keys = K::Vector(ColVec::Sym(Attr::None, Arc::new(t.cols.clone())));
                let vals = K::Vector(ColVec::from_atoms(t.vals.iter().map(|c| c.null_atom()).collect()));
                K::Dict(Arc::new(Dict { keys: Box::new(keys), vals: Box::new(vals) }))
            } else {
                K::Dict(Arc::new(t.row_dict(0)))
            }
        }
        K::KeyedTable(kt) => first_op(&K::Table(Arc::new(kt.vals.clone()))),
        K::Dict(d) => first_op(&d.vals),
        other => other.clone(),
    }
}

pub fn last_op(x: &K) -> K {
    match x {
        K::Vector(v) => {
            if v.is_empty() {
                v.null_atom()
            } else {
                v.get_atom(v.len() - 1)
            }
        }
        K::Table(t) => {
            if t.nrows() == 0 {
                first_op(x)
            } else {
                K::Dict(Arc::new(t.row_dict(t.nrows() - 1)))
            }
        }
        K::Dict(d) => last_op(&d.vals),
        other => other.clone(),
    }
}

pub fn til_op(n: i64) -> KResult<K> {
    if n < 0 {
        return Err(KError::domain("til requires non-negative integer"));
    }
    let v: Vec<i64> = (0..n).collect();
    Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(v))))
}

fn key_op(x: &K) -> KResult<K> {
    match x {
        K::Dict(d) => Ok((*d.keys).clone()),
        K::KeyedTable(kt) => Ok(K::Table(Arc::new(kt.keys.clone()))),
        K::Table(_) => Ok(K::Vector(ColVec::General(Attr::None, Arc::new(vec![])))),
        _ => Err(KError::ty("key expects dictionary or keyed table")),
    }
}

fn keys_op(x: &K) -> KResult<K> {
    match x {
        K::KeyedTable(kt) => Ok(K::Vector(ColVec::Sym(Attr::None, Arc::new(kt.keys.cols.clone())))),
        K::Table(_) => Ok(K::Vector(ColVec::Sym(Attr::None, Arc::new(vec![])))),
        K::Dict(d) => Ok((*d.keys).clone()),
        _ => Err(KError::ty("keys expects table")),
    }
}

pub fn cols_op(x: &K) -> KResult<K> {
    match x {
        K::Table(t) => Ok(K::Vector(ColVec::Sym(Attr::None, Arc::new(t.cols.clone())))),
        K::KeyedTable(kt) => {
            let mut all = kt.keys.cols.clone();
            all.extend_from_slice(&kt.vals.cols);
            Ok(K::Vector(ColVec::Sym(Attr::None, Arc::new(all))))
        }
        K::Dict(d) => Ok((*d.keys).clone()),
        _ => Err(KError::ty("cols expects table")),
    }
}

fn value_op(x: &K) -> KResult<K> {
    match x {
        K::Dict(d) => Ok((*d.vals).clone()),
        K::KeyedTable(kt) => Ok(K::Table(Arc::new(kt.vals.clone()))),
        other => Ok(other.clone()),
    }
}

pub fn flip_op(x: &K) -> KResult<K> {
    match x {
        K::Dict(d) => {
            // `flip sym_vec ! list_of_cols` -> `Table`!
            let K::Vector(ColVec::Sym(_, cols)) = &*d.keys else {
                return Err(KError::ty("flip of dict requires symbol keys"));
            };
            let K::Vector(vals_vec) = &*d.vals else {
                return Err(KError::ty("flip of dict requires column list"));
            };
            if cols.len() != vals_vec.len() {
                return Err(KError::length("flip column count mismatch"));
            }
            // Determine row count (scalar columns broadcast to max vector length, as in kdb+!)
            let mut max_len = 0usize;
            let mut any_vec = false;
            for i in 0..vals_vec.len() {
                let item = vals_vec.get_atom(i);
                if let K::Vector(cv) = &item {
                    any_vec = true;
                    max_len = max_len.max(cv.len());
                }
            }
            let target_len = if any_vec { max_len } else { 1 };
            let mut col_vecs = Vec::with_capacity(cols.len());
            for i in 0..vals_vec.len() {
                let item = vals_vec.get_atom(i);
                match item {
                    K::Vector(cv) => {
                        if cv.len() != target_len {
                            return Err(KError::length("table columns must have equal length"));
                        }
                        col_vecs.push(cv);
                    }
                    atom => {
                        col_vecs.push(ColVec::repeat(atom, target_len));
                    }
                }
            }
            Ok(K::Table(Arc::new(Flip::new((**cols).clone(), col_vecs)?)))
        }
        K::Table(t) => {
            // `flip table` -> `Dict` of `cols ! column_vectors`!
            let keys = K::Vector(ColVec::Sym(Attr::None, Arc::new(t.cols.clone())));
            let vals = K::Vector(ColVec::General(
                Attr::None,
                Arc::new(t.vals.iter().cloned().map(K::Vector).collect()),
            ));
            Ok(K::Dict(Arc::new(Dict { keys: Box::new(keys), vals: Box::new(vals) })))
        }
        K::Vector(ColVec::General(_, rows)) if !rows.is_empty() => {
            let nrows = rows.len();
            let ncols = rows[0].count();
            let mut out = Vec::with_capacity(ncols);
            for c in 0..ncols {
                let mut col = Vec::with_capacity(nrows);
                for r in rows.iter() {
                    if let K::Vector(rv) = r {
                        if rv.len() != ncols {
                            return Err(KError::length("matrix rows differ in length"));
                        }
                        col.push(rv.get_atom(c));
                    } else {
                        return Err(KError::ty("flip requires list of lists"));
                    }
                }
                out.push(K::Vector(ColVec::from_atoms(col)));
            }
            Ok(K::Vector(ColVec::General(Attr::None, Arc::new(out))))
        }
        _ => Err(KError::ty("flip expects dictionary, table, or matrix")),
    }
}

fn reverse_op(x: &K) -> KResult<K> {
    match x {
        K::Vector(v) => Ok(K::Vector(v.reverse())),
        K::Table(t) => {
            let idx: Vec<usize> = (0..t.nrows()).rev().collect();
            Ok(K::Table(Arc::new(t.gather(&idx))))
        }
        K::KeyedTable(kt) => {
            let idx: Vec<usize> = (0..kt.nrows()).rev().collect();
            Ok(K::KeyedTable(Arc::new(KeyedTable { keys: kt.keys.gather(&idx), vals: kt.vals.gather(&idx) })))
        }
        K::Dict(d) => Ok(K::Dict(Arc::new(Dict { keys: Box::new(reverse_op(&d.keys)?), vals: Box::new(reverse_op(&d.vals)?) }))),
        other => Ok(other.clone()),
    }
}

/// The most slots a table that finds keys for `rows` rows may have: beyond that, filling it costs more than hashing.
/// (The minimum is small because `distinct` and `in` may be called once per group, on a few rows each time.)
fn table_room(rows: usize) -> usize {
    rows.saturating_mul(4).saturating_add(256)
}

/// The lowest of `keys` (read as signed) and how many values reach from it to the highest, when a table with a slot for
/// each of those has at most `room` slots.
fn narrow(keys: &[u64], room: usize) -> Option<(u64, usize)> {
    let (lo, hi) = keys.iter().fold((i64::MAX, i64::MIN), |(lo, hi), &k| (lo.min(k as i64), hi.max(k as i64)));
    let span = usize::try_from(hi.checked_sub(lo)?).ok()?.checked_add(1)?;
    (span <= room).then_some((lo as u64, span))
}

/// A group number for every row: rows alike in all of `cols` share one, numbered in order of first appearance.
pub(crate) fn group_ids(cols: &[ColVec], nrows: usize) -> Vec<usize> {
    // split the groups made so far by one more column, and count the groups there are then
    fn refine<T: Hash + Eq>(ids: &mut [usize], keys: impl Iterator<Item = T>) -> usize {
        let mut seen = HashMap::new();
        for (id, key) in ids.iter_mut().zip(keys) {
            let next = seen.len();
            *id = *seen.entry((*id, key)).or_insert(next);
        }
        seen.len()
    }
    // the same with a slot for every pair of a group so far and a key; a slot holds the new group's number plus one
    fn refine_narrow(ids: &mut [usize], keys: &[u64], groups: usize, lo: u64, span: usize) -> usize {
        let (mut slots, mut count) = (vec![0usize; groups * span], 0);
        for (id, key) in ids.iter_mut().zip(keys) {
            let slot = &mut slots[*id * span + key.wrapping_sub(lo) as usize];
            if *slot == 0 {
                count += 1;
                *slot = count;
            }
            *id = *slot - 1;
        }
        count
    }
    let mut ids = vec![0; nrows];
    let mut groups = 1;
    for c in cols {
        groups = match c.key_bits() {
            Some(keys) => match narrow(&keys, table_room(nrows) / groups.max(1)) {
                Some((lo, span)) => refine_narrow(&mut ids, &keys, groups, lo, span),
                None => refine(&mut ids, keys.into_iter()),
            },
            None => refine(&mut ids, (0..nrows).map(|r| c.get_atom(r).hash_key())),
        };
    }
    ids
}

/// The first row of every group that `group_ids` numbered.
pub(crate) fn first_rows(ids: &[usize]) -> Vec<usize> {
    let mut found = Vec::new();
    for (r, &id) in ids.iter().enumerate() {
        if id == found.len() {
            found.push(r);
        }
    }
    found
}

/// Group rows by `cols`: the first row of each group and all of its rows, groups in order of first appearance.
pub fn group_rows(cols: &[ColVec], nrows: usize) -> (Vec<usize>, Vec<Vec<usize>>) {
    let ids = group_ids(cols, nrows);
    let first = first_rows(&ids);
    // size each group's list before filling it: with many small groups, growing them costs more than counting
    let mut sizes = vec![0; first.len()];
    ids.iter().for_each(|&id| sizes[id] += 1);
    let mut groups: Vec<Vec<usize>> = sizes.into_iter().map(Vec::with_capacity).collect();
    ids.into_iter().enumerate().for_each(|(r, id)| groups[id].push(r));
    (first, groups)
}

/// The row numbers of `cols` in ascending order of their values, the first column deciding first; ties keep their order.
pub fn sort_order(cols: &[ColVec], nrows: usize) -> Vec<usize> {
    // stable sorts, from the last column to the first
    cols.iter().rev().fold((0..nrows).collect(), |order: Vec<usize>, c| {
        iasc_indices(&c.gather(&order), false).into_iter().map(|i| order[i]).collect()
    })
}

/// An aggregate of the values in one group's `rows`; the commonest read them where they are, without a copy.
pub fn aggregate_rows(verb: &str, values: &ColVec, rows: &[usize]) -> KResult<K> {
    Ok(match (verb, values) {
        ("count", _) => K::Long(rows.len() as i64),
        ("first", _) => values.get_atom(rows[0]),
        ("sum", ColVec::Float(_, xs)) => K::Float(rows.iter().map(|&r| xs[r]).filter(|v| !v.is_nan()).sum()),
        ("sum", ColVec::Long(_, xs)) => K::Long(rows.iter().map(|&r| xs[r]).filter(|&v| v != NULL_LONG).fold(0, i64::wrapping_add)),
        _ => eval_unary_prim(verb, &K::Vector(values.gather(rows)))?,
    })
}

pub fn distinct_op(x: &K) -> KResult<K> {
    match x {
        K::Vector(v) => Ok(K::Vector(v.gather(&first_rows(&group_ids(std::slice::from_ref(v), v.len()))))),
        K::Table(t) => Ok(K::Table(Arc::new(t.gather(&first_rows(&group_ids(&t.vals, t.nrows())))))),
        _ => Err(KError::ty("distinct requires list or table")),
    }
}

pub fn where_op(x: &K) -> KResult<K> {
    match x {
        K::Bool(b) => Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(if *b { vec![0] } else { vec![] })))),
        K::Vector(ColVec::Bool(_, bs)) => {
            let mut idx = Vec::new();
            for (i, &b) in bs.iter().enumerate() {
                if b {
                    idx.push(i as i64);
                }
            }
            Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(idx))))
        }
        K::Vector(cv) => {
            let counts = cv.as_i64_vec().ok_or_else(|| KError::ty("where requires boolean or integer vector"))?;
            let mut idx = Vec::new();
            for (i, c) in counts.into_iter().enumerate() {
                for _ in 0..c.max(0) {
                    idx.push(i as i64);
                }
            }
            Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(idx))))
        }
        K::Dict(d) => {
            let idx_k = where_op(&d.vals)?;
            index_k(&d.keys, &[Some(idx_k)])
        }
        _ => Err(KError::ty("where requires boolean or integer vector")),
    }
}

pub fn group_op(x: &K) -> KResult<K> {
    let K::Vector(cv) = x else {
        return Err(KError::ty("group requires a vector"));
    };
    let (ukeys, groups) = group_rows(std::slice::from_ref(cv), cv.len());
    let key_vec = cv.gather(&ukeys);
    let val_vecs: Vec<K> = groups
        .into_iter()
        .map(|g| K::Vector(ColVec::Long(Attr::None, Arc::new(g.into_iter().map(|r| r as i64).collect()))))
        .collect();
    Ok(K::Dict(Arc::new(Dict {
        keys: Box::new(K::Vector(key_vec)),
        vals: Box::new(K::Vector(ColVec::General(Attr::None, Arc::new(val_vecs)))),
    })))
}

pub fn iasc_indices(cv: &ColVec, desc: bool) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..cv.len()).collect();
    // Fast paths for common vector types
    match cv {
        ColVec::Long(_, xs) | ColVec::Timestamp(_, xs) | ColVec::Timespan(_, xs) => {
            idx.sort_by(|&a, &b| if desc { xs[b].cmp(&xs[a]) } else { xs[a].cmp(&xs[b]) });
        }
        ColVec::Int(_, xs)
        | ColVec::Date(_, xs)
        | ColVec::Month(_, xs)
        | ColVec::Time(_, xs)
        | ColVec::Second(_, xs)
        | ColVec::Minute(_, xs) => {
            idx.sort_by(|&a, &b| if desc { xs[b].cmp(&xs[a]) } else { xs[a].cmp(&xs[b]) });
        }
        ColVec::Float(_, xs) | ColVec::Datetime(_, xs) => {
            idx.sort_by(|&a, &b| {
                let ord = K::Float(xs[a]).k_cmp(&K::Float(xs[b]));
                if desc { ord.reverse() } else { ord }
            });
        }
        ColVec::Sym(_, xs) => {
            idx.sort_by(|&a, &b| if desc { xs[b].cmp(&xs[a]) } else { xs[a].cmp(&xs[b]) });
        }
        _ => {
            idx.sort_by(|&a, &b| {
                let ord = cv.get_atom(a).k_cmp(&cv.get_atom(b));
                if desc { ord.reverse() } else { ord }
            });
        }
    }
    idx
}

fn iasc_op(x: &K, desc: bool) -> KResult<K> {
    match x {
        K::Vector(cv) => {
            let idx: Vec<i64> = iasc_indices(cv, desc).into_iter().map(|i| i as i64).collect();
            Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(idx))))
        }
        K::Dict(d) => {
            let K::Vector(v) = &*d.vals else {
                return Err(KError::ty("iasc/idesc on dict requires vector values"));
            };
            let idx = iasc_indices(v, desc);
            let K::Vector(kv) = &*d.keys else {
                return Err(KError::ty("iasc/idesc on dict requires vector keys"));
            };
            Ok(K::Vector(kv.gather(&idx)))
        }
        _ => Err(KError::ty("iasc/idesc requires vector or dictionary")),
    }
}

fn sort_op(x: &K, desc: bool) -> KResult<K> {
    match x {
        K::Vector(cv) => {
            let idx = iasc_indices(cv, desc);
            let mut sorted = cv.gather(&idx);
            if !desc {
                sorted.set_attr_unchecked(Attr::Sorted);
            }
            Ok(K::Vector(sorted))
        }
        K::Dict(d) => {
            let K::Vector(v) = &*d.vals else {
                return Err(KError::ty("asc/desc on dict requires vector values"));
            };
            let idx = iasc_indices(v, desc);
            let K::Vector(kv) = &*d.keys else {
                return Err(KError::ty("asc/desc on dict requires vector keys"));
            };
            let mut sv = v.gather(&idx);
            if !desc {
                sv.set_attr_unchecked(Attr::Sorted);
            }
            Ok(K::Dict(Arc::new(Dict {
                keys: Box::new(K::Vector(kv.gather(&idx))),
                vals: Box::new(K::Vector(sv)),
            })))
        }
        _ => Err(KError::ty("asc/desc requires vector or dictionary")),
    }
}

pub fn raze_op(x: &K) -> KResult<K> {
    let K::Vector(ColVec::General(_, items)) = x else {
        return Ok(x.clone());
    };
    if items.is_empty() {
        return Ok(x.clone());
    }
    let mut acc: Option<K> = None;
    for item in items.iter() {
        acc = Some(match acc {
            None => {
                if item.is_atom() {
                    enlist_op(item)
                } else {
                    item.clone()
                }
            }
            Some(prev) => concat_k(&prev, item)?,
        });
    }
    Ok(acc.unwrap_or_else(|| K::Vector(ColVec::General(Attr::None, Arc::new(vec![])))))
}

fn string_op(x: &K) -> KResult<K> {
    if x.is_atom() {
        let s = x.cell_text();
        Ok(K::string(&s))
    } else if let K::Vector(ColVec::Char(_, _)) = x {
        // In kdb+, `string "abc"` applies per character -> `("a";"b";"c")` (or general list of 1-char strings)
        if let K::Vector(ColVec::Char(_, bs)) = x {
            let items: Vec<K> = bs
                .iter()
                .map(|&b| K::Vector(ColVec::Char(Attr::None, Arc::new(vec![b]))))
                .collect();
            Ok(K::Vector(ColVec::General(Attr::None, Arc::new(items))))
        } else {
            unreachable!()
        }
    } else if let K::Vector(cv) = x {
        let items: Vec<K> = (0..cv.len()).map(|i| K::string(&cv.get_atom(i).cell_text())).collect();
        Ok(K::Vector(ColVec::General(Attr::None, Arc::new(items))))
    } else {
        Ok(K::string(&x.q_repr()))
    }
}

fn str_transform(x: &K, f: &dyn Fn(&str) -> String) -> KResult<K> {
    match x {
        K::Char(c) => {
            let s = f(&(*c as char).to_string());
            Ok(K::Char(s.as_bytes().first().copied().unwrap_or(b' ')))
        }
        K::Sym(s) => Ok(K::Sym(intern(&f(&resolve(*s))))),
        K::Vector(ColVec::Char(_, bs)) => {
            let s = f(&String::from_utf8_lossy(bs));
            Ok(K::string(&s))
        }
        K::Vector(ColVec::Sym(_, xs)) => {
            let out: Vec<SymId> = xs.iter().map(|&s| intern(&f(&resolve(s)))).collect();
            Ok(K::Vector(ColVec::Sym(Attr::None, Arc::new(out))))
        }
        K::Vector(ColVec::General(_, xs)) => {
            let items: KResult<Vec<K>> = xs.iter().map(|e| str_transform(e, f)).collect();
            Ok(K::Vector(ColVec::General(Attr::None, Arc::new(items?))))
        }
        _ => Err(KError::ty("string case/trim operation requires char, string, or symbol")),
    }
}

// ----- Vector Aggregations & Running / Moving Windows -----------------------

/// The sum of the non-null values; integer overflow wraps, as in q.
fn wrapping_total(xs: &[i64]) -> i64 {
    xs.iter().filter(|&&v| v != NULL_LONG).fold(0, |a, &v| a.wrapping_add(v))
}

pub fn sum_op(x: &K) -> KResult<K> {
    if x.is_atom() {
        return Ok(x.clone());
    }
    let K::Vector(cv) = x else {
        return Err(KError::ty("sum requires vector"));
    };
    match cv {
        ColVec::Bool(_, xs) => Ok(K::Long(xs.iter().map(|&b| i64::from(b)).sum())),
        ColVec::Byte(_, xs) => Ok(K::Long(xs.iter().map(|&v| v as i64).sum())),
        ColVec::Short(_, xs) => Ok(K::Long(xs.iter().filter(|&&v| v != NULL_SHORT).map(|&v| v as i64).sum())),
        ColVec::Int(_, xs) => Ok(K::Long(xs.iter().filter(|&&v| v != NULL_INT).map(|&v| v as i64).sum())),
        ColVec::Long(_, xs) => Ok(K::Long(wrapping_total(xs))),
        ColVec::Timespan(_, xs) => Ok(K::Timespan(wrapping_total(xs))),
        ColVec::Real(_, xs) => Ok(K::Float(xs.iter().filter(|v| !v.is_nan()).map(|&v| v as f64).sum())),
        ColVec::Float(_, xs) => Ok(K::Float(xs.iter().filter(|v| !v.is_nan()).copied().sum())),
        ColVec::General(_, xs) if xs.is_empty() => Ok(K::Long(0)),
        ColVec::General(_, xs) => {
            let mut acc = xs[0].clone();
            for e in &xs[1..] {
                acc = eval_binary_prim("+", &acc, e)?;
            }
            Ok(acc)
        }
        _ => Err(KError::ty(format!("sum not supported on {}", cv.type_name()))),
    }
}

fn prd_op(x: &K) -> KResult<K> {
    if x.is_atom() {
        return Ok(x.clone());
    }
    let K::Vector(cv) = x else {
        return Err(KError::ty("prd requires vector"));
    };
    if let Some(is) = cv.as_i64_vec() {
        Ok(K::Long(is.into_iter().filter(|&v| v != NULL_LONG).fold(1, i64::wrapping_mul)))
    } else if let Some(fs) = cv.as_f64_vec() {
        Ok(K::Float(fs.into_iter().filter(|v| !v.is_nan()).product()))
    } else {
        Err(KError::ty("prd requires numeric vector"))
    }
}

pub fn minmax_op(x: &K, is_min: bool) -> KResult<K> {
    if x.is_atom() {
        return Ok(x.clone());
    }
    let K::Vector(cv) = x else {
        return Err(KError::ty("min/max requires vector"));
    };
    let mut best: Option<K> = None;
    for i in 0..cv.len() {
        let a = cv.get_atom(i);
        if a.is_null() {
            continue;
        }
        best = Some(match best {
            None => a,
            Some(cur) => {
                let cmp = a.k_cmp(&cur);
                if (is_min && cmp == Ordering::Less) || (!is_min && cmp == Ordering::Greater) {
                    a
                } else {
                    cur
                }
            }
        });
    }
    Ok(best.unwrap_or_else(|| match cv {
        ColVec::Long(_, _) => K::Long(if is_min { INF_LONG } else { NINF_LONG }),
        ColVec::Int(_, _) => K::Int(if is_min { INF_INT } else { NINF_INT }),
        ColVec::Float(_, _) => K::Float(if is_min { f64::INFINITY } else { f64::NEG_INFINITY }),
        _ => cv.null_atom(),
    }))
}

pub fn avg_op(x: &K) -> KResult<K> {
    if x.is_atom() {
        return Ok(K::Float(x.as_f64()?));
    }
    let K::Vector(cv) = x else {
        return Err(KError::ty("avg requires vector"));
    };
    if cv.is_empty() && matches!(cv, ColVec::General(_, _)) {
        return Ok(K::Float(f64::NAN));
    }
    let fs = cv.as_f64_vec().ok_or_else(|| KError::ty("avg requires numeric vector"))?;
    let mut s = 0.0;
    let mut n = 0usize;
    for v in fs {
        if !v.is_nan() {
            s += v;
            n += 1;
        }
    }
    Ok(K::Float(if n == 0 { f64::NAN } else { s / (n as f64) }))
}

fn med_op(x: &K) -> KResult<K> {
    if x.is_atom() {
        return Ok(K::Float(x.as_f64()?));
    }
    let K::Vector(cv) = x else {
        return Err(KError::ty("med requires vector"));
    };
    let mut fs: Vec<f64> = cv
        .as_f64_vec()
        .ok_or_else(|| KError::ty("med requires numeric vector"))?
        .into_iter()
        .filter(|v| !v.is_nan())
        .collect();
    if fs.is_empty() {
        return Ok(K::Float(f64::NAN));
    }
    fs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let m = fs.len() / 2;
    let med = if fs.len() % 2 == 1 { fs[m] } else { 0.5 * (fs[m - 1] + fs[m]) };
    Ok(K::Float(med))
}

fn var_op(x: &K, sample: bool, sqrt_out: bool) -> KResult<K> {
    if x.is_atom() {
        return Ok(K::Float(if sample { f64::NAN } else { 0.0 }));
    }
    let K::Vector(cv) = x else {
        return Err(KError::ty("var/dev requires vector"));
    };
    let vals: Vec<f64> = cv
        .as_f64_vec()
        .ok_or_else(|| KError::ty("var/dev requires numeric vector"))?
        .into_iter()
        .filter(|v| !v.is_nan())
        .collect();
    let n = vals.len();
    if n == 0 || (sample && n < 2) {
        return Ok(K::Float(f64::NAN));
    }
    let mean = vals.iter().sum::<f64>() / (n as f64);
    let ss: f64 = vals.iter().map(|v| (v - mean) * (v - mean)).sum();
    let denom = if sample { (n - 1) as f64 } else { n as f64 };
    let v = ss / denom;
    Ok(K::Float(if sqrt_out { v.sqrt() } else { v }))
}

fn running_sum(x: &K) -> KResult<K> {
    let K::Vector(cv) = x else { return Ok(x.clone()) };
    if matches!(cv, ColVec::Float(_, _) | ColVec::Real(_, _)) {
        let fs = cv.as_f64_vec().unwrap();
        let mut s = 0.0;
        let out: Vec<f64> = fs.into_iter().map(|v| { if !v.is_nan() { s += v; } s }).collect();
        Ok(K::Vector(ColVec::Float(Attr::None, Arc::new(out))))
    } else if let Some(is) = cv.as_i64_vec() {
        let mut s = 0i64;
        let out: Vec<i64> = is.into_iter().map(|v| { if v != NULL_LONG { s = s.wrapping_add(v); } s }).collect();
        Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(out))))
    } else {
        Err(KError::ty("sums requires numeric vector"))
    }
}

fn running_prd(x: &K) -> KResult<K> {
    let K::Vector(cv) = x else { return Ok(x.clone()) };
    if matches!(cv, ColVec::Float(_, _) | ColVec::Real(_, _)) {
        let fs = cv.as_f64_vec().unwrap();
        let mut p = 1.0;
        let out: Vec<f64> = fs.into_iter().map(|v| { if !v.is_nan() { p *= v; } p }).collect();
        Ok(K::Vector(ColVec::Float(Attr::None, Arc::new(out))))
    } else if let Some(is) = cv.as_i64_vec() {
        let mut p = 1i64;
        let out: Vec<i64> = is.into_iter().map(|v| { if v != NULL_LONG { p = p.wrapping_mul(v); } p }).collect();
        Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(out))))
    } else {
        Err(KError::ty("prds requires numeric vector"))
    }
}

fn running_minmax(x: &K, is_min: bool) -> KResult<K> {
    let K::Vector(cv) = x else { return Ok(x.clone()) };
    let mut out = Vec::with_capacity(cv.len());
    let mut cur: Option<K> = None;
    for i in 0..cv.len() {
        let a = cv.get_atom(i);
        if !a.is_null() {
            cur = Some(match cur {
                None => a,
                Some(prev) => {
                    let cmp = a.k_cmp(&prev);
                    if (is_min && cmp == Ordering::Less) || (!is_min && cmp == Ordering::Greater) { a } else { prev }
                }
            });
        }
        out.push(cur.clone().unwrap_or_else(|| cv.null_atom()));
    }
    Ok(K::Vector(ColVec::from_atoms(out)))
}

fn running_avg(x: &K) -> KResult<K> {
    let K::Vector(cv) = x else { return avg_op(x) };
    let fs = cv.as_f64_vec().ok_or_else(|| KError::ty("avgs requires numeric vector"))?;
    let mut s = 0.0;
    let mut n = 0usize;
    let out: Vec<f64> = fs
        .into_iter()
        .map(|v| {
            if !v.is_nan() {
                s += v;
                n += 1;
            }
            if n == 0 { f64::NAN } else { s / (n as f64) }
        })
        .collect();
    Ok(K::Vector(ColVec::Float(Attr::None, Arc::new(out))))
}

fn deltas_op(x: &K) -> KResult<K> {
    let K::Vector(cv) = x else { return Ok(x.clone()) };
    if matches!(cv, ColVec::Float(_, _) | ColVec::Real(_, _)) {
        let fs = cv.as_f64_vec().unwrap();
        let mut out = Vec::with_capacity(fs.len());
        for i in 0..fs.len() {
            out.push(if i == 0 { fs[0] } else { fs[i] - fs[i - 1] });
        }
        Ok(K::Vector(ColVec::Float(Attr::None, Arc::new(out))))
    } else if let Some(is) = cv.as_i64_vec() {
        let mut out = Vec::with_capacity(is.len());
        for i in 0..is.len() {
            let v = if i == 0 {
                is[0]
            } else if is[i] == NULL_LONG || is[i - 1] == NULL_LONG {
                NULL_LONG
            } else {
                is[i].wrapping_sub(is[i - 1])
            };
            out.push(v);
        }
        Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(out))))
    } else {
        Err(KError::ty("deltas requires numeric vector"))
    }
}

fn ratios_op(x: &K) -> KResult<K> {
    let K::Vector(cv) = x else { return Ok(x.clone()) };
    let fs = cv.as_f64_vec().ok_or_else(|| KError::ty("ratios requires numeric vector"))?;
    let mut out = Vec::with_capacity(fs.len());
    for i in 0..fs.len() {
        out.push(if i == 0 { fs[0] } else { fs[i] / fs[i - 1] });
    }
    Ok(K::Vector(ColVec::Float(Attr::None, Arc::new(out))))
}

fn differ_op(x: &K) -> KResult<K> {
    let K::Vector(cv) = x else { return Ok(K::Bool(true)) };
    let mut out = Vec::with_capacity(cv.len());
    for i in 0..cv.len() {
        out.push(if i == 0 { true } else { cv.get_atom(i) != cv.get_atom(i - 1) });
    }
    Ok(K::Vector(ColVec::Bool(Attr::None, Arc::new(out))))
}

fn shift_op(x: &K, delta: isize) -> KResult<K> {
    let K::Vector(cv) = x else { return Ok(K::Null) };
    let len = cv.len() as isize;
    let idx: Vec<i64> = (0..len)
        .map(|i| {
            let j = i + delta;
            if j < 0 || j >= len { -1 } else { j as i64 }
        })
        .collect();
    Ok(K::Vector(cv.gather_signed(&idx)))
}

fn fills_op(x: &K) -> KResult<K> {
    let K::Vector(cv) = x else { return Ok(x.clone()) };
    let mut out = Vec::with_capacity(cv.len());
    let mut last = cv.null_atom();
    for i in 0..cv.len() {
        let a = cv.get_atom(i);
        if !a.is_null() {
            last = a.clone();
            out.push(a);
        } else {
            out.push(last.clone());
        }
    }
    Ok(K::Vector(ColVec::from_atoms(out)))
}

pub fn meta_op(x: &K) -> KResult<K> {
    let (all_cols, all_vals, key_count) = match x {
        K::Table(t) => (t.cols.clone(), t.vals.clone(), 0usize),
        K::KeyedTable(kt) => {
            let f = kt.unkey();
            (f.cols, f.vals, kt.keys.ncols())
        }
        _ => return Err(KError::ty("meta requires a table")),
    };
    let mut t_chars = Vec::with_capacity(all_cols.len());
    let mut f_syms = Vec::with_capacity(all_cols.len());
    let mut a_syms = Vec::with_capacity(all_cols.len());
    for cv in &all_vals {
        t_chars.push(cv.meta_type_char() as u8);
        f_syms.push(SymId::EMPTY);
        a_syms.push(cv.attr().char().map(|c| intern(&c.to_string())).unwrap_or(SymId::EMPTY));
    }
    let _ = key_count;
    let keys = Flip {
        cols: vec![intern("c")],
        vals: vec![ColVec::Sym(Attr::None, Arc::new(all_cols))],
    };
    let vals = Flip {
        cols: vec![intern("t"), intern("f"), intern("a")],
        vals: vec![
            ColVec::Char(Attr::None, Arc::new(t_chars)),
            ColVec::Sym(Attr::None, Arc::new(f_syms)),
            ColVec::Sym(Attr::None, Arc::new(a_syms)),
        ],
    };
    Ok(K::KeyedTable(Arc::new(KeyedTable { keys, vals })))
}

fn ungroup_op(x: &K) -> KResult<K> {
    let f = match x {
        K::Table(t) => (**t).clone(),
        K::KeyedTable(kt) => kt.unkey(),
        _ => return Err(KError::ty("ungroup expects table")),
    };
    let mut total_rows = 0usize;
    let mut row_counts = Vec::with_capacity(f.nrows());
    for r in 0..f.nrows() {
        let mut rlen = 1usize;
        for c in &f.vals {
            if let K::Vector(v) = c.get_atom(r) {
                if !matches!(v, ColVec::Char(_, _)) {
                    rlen = v.len();
                    break;
                }
            }
        }
        row_counts.push(rlen);
        total_rows += rlen;
    }
    let mut new_vals = Vec::with_capacity(f.ncols());
    for c in &f.vals {
        let mut atoms = Vec::with_capacity(total_rows);
        for (r, &cnt) in row_counts.iter().enumerate() {
            let cell = c.get_atom(r);
            match cell {
                K::Vector(v) if !matches!(v, ColVec::Char(_, _)) => {
                    for j in 0..cnt {
                        atoms.push(v.get_atom(j));
                    }
                }
                scalar => {
                    for _ in 0..cnt {
                        atoms.push(scalar.clone());
                    }
                }
            }
        }
        new_vals.push(ColVec::from_atoms(atoms));
    }
    Ok(K::Table(Arc::new(Flip::new(f.cols, new_vals)?)))
}

// ----- Binary Primitives ----------------------------------------------------

pub fn eval_binary_prim(op: &str, lhs: &K, rhs: &K) -> KResult<K> {
    match op {
        "+" => arith_op(lhs, rhs, ArithOp::Add),
        "-" => arith_op(lhs, rhs, ArithOp::Sub),
        "*" => arith_op(lhs, rhs, ArithOp::Mul),
        "%" => float_arith_op(lhs, rhs, |a, b| a / b),
        "div" => int_arith_op(lhs, rhs, |a, b| if b == 0 { NULL_LONG } else { a.div_euclid(b) }),
        "mod" => int_arith_op(lhs, rhs, |a, b| if b == 0 { NULL_LONG } else { a.rem_euclid(b) }),
        "xexp" => float_arith_op(lhs, rhs, f64::powf),
        "xlog" => float_arith_op(lhs, rhs, |base, v| v.ln() / base.ln()),
        "&" | "and" => minmax_bin_op(lhs, rhs, true),
        "|" | "or" => minmax_bin_op(lhs, rhs, false),
        "^" => fill_op(lhs, rhs),
        "=" => cmp_op(lhs, rhs, CmpKind::Eq),
        "<>" => cmp_op(lhs, rhs, CmpKind::Ne),
        "<" => cmp_op(lhs, rhs, CmpKind::Lt),
        "<=" => cmp_op(lhs, rhs, CmpKind::Le),
        ">" => cmp_op(lhs, rhs, CmpKind::Gt),
        ">=" => cmp_op(lhs, rhs, CmpKind::Ge),
        "~" => Ok(K::Bool(lhs == rhs)),
        "," => concat_k(lhs, rhs),
        "!" => bang_op(lhs, rhs),
        "#" => take_or_attr_op(lhs, rhs),
        "_" => drop_or_cut_op(lhs, rhs),
        "sublist" => sublist_op(lhs, rhs),
        "cut" => cut_op(lhs, rhs),
        "$" => cast_op(lhs, rhs),
        "?" => find_or_roll_op(lhs, rhs),
        "in" => in_op(lhs, rhs),
        "within" => within_op(lhs, rhs),
        "like" => like_op(lhs, rhs),
        "ss" => ss_op(lhs, rhs),
        "vs" => vs_op(lhs, rhs),
        "sv" => sv_op(lhs, rhs),
        "xbar" => xbar_op(lhs, rhs),
        "xasc" => table_sort_op(lhs, rhs, false),
        "xdesc" => table_sort_op(lhs, rhs, true),
        "xkey" => xkey_op(lhs, rhs),
        "xcol" => xcol_op(lhs, rhs),
        "xcols" => xcols_op(lhs, rhs),
        "xgroup" => xgroup_op(lhs, rhs),
        "mavg" => moving_op(lhs, rhs, MovingKind::Avg),
        "msum" => moving_op(lhs, rhs, MovingKind::Sum),
        "mcount" => moving_op(lhs, rhs, MovingKind::Count),
        "mmin" => moving_op(lhs, rhs, MovingKind::Min),
        "mmax" => moving_op(lhs, rhs, MovingKind::Max),
        "mdev" => moving_op(lhs, rhs, MovingKind::Dev),
        "ema" => ema_op(lhs, rhs),
        "wavg" => weighted_op(lhs, rhs, true),
        "wsum" => weighted_op(lhs, rhs, false),
        "cov" => cov_cor_op(lhs, rhs, false),
        "cor" => cov_cor_op(lhs, rhs, true),
        "bin" => bin_search_op(lhs, rhs, false),
        "binr" => bin_search_op(lhs, rhs, true),
        "inter" => set_op(lhs, rhs, SetOp::Inter),
        "union" => set_op(lhs, rhs, SetOp::Union),
        "except" => set_op(lhs, rhs, SetOp::Except),
        "cross" => cross_op(lhs, rhs),
        "ij" => crate::engine::joins::inner_join(lhs, rhs),
        "lj" => crate::engine::joins::left_join(lhs, rhs),
        "uj" => crate::engine::joins::union_join(lhs, rhs),
        "pj" => crate::engine::joins::plus_join(lhs, rhs),
        "asof" => crate::engine::joins::asof_op(lhs, rhs),
        _ => Err(KError::nyi(format!("unknown binary verb '{op}'"))),
    }
}

#[derive(Clone, Copy)]
enum ArithOp {
    Add,
    Sub,
    Mul,
}

fn arith_atom(a: &K, b: &K, op: ArithOp) -> KResult<K> {
    // Temporal arithmetic (e.g. `date + 5`, `date1 - date2`, `timestamp + timespan`, `time + 1000`)
    match (op, a, b) {
        (ArithOp::Add, K::Date(d), K::Long(n)) | (ArithOp::Add, K::Long(n), K::Date(d)) => {
            return Ok(K::Date(if *d == NULL_INT || *n == NULL_LONG { NULL_INT } else { d.saturating_add(*n as i32) }));
        }
        (ArithOp::Add, K::Date(d), K::Int(n)) | (ArithOp::Add, K::Int(n), K::Date(d)) => {
            return Ok(K::Date(if *d == NULL_INT || *n == NULL_INT { NULL_INT } else { d.saturating_add(*n) }));
        }
        (ArithOp::Sub, K::Date(d), K::Long(n)) => {
            return Ok(K::Date(if *d == NULL_INT || *n == NULL_LONG { NULL_INT } else { d.saturating_sub(*n as i32) }));
        }
        (ArithOp::Sub, K::Date(d), K::Int(n)) => {
            return Ok(K::Date(if *d == NULL_INT || *n == NULL_INT { NULL_INT } else { d.saturating_sub(*n) }));
        }
        (ArithOp::Sub, K::Date(d1), K::Date(d2)) => {
            return Ok(K::Int(if *d1 == NULL_INT || *d2 == NULL_INT { NULL_INT } else { d1 - d2 }));
        }
        (ArithOp::Add, K::Timestamp(p), K::Timespan(n)) | (ArithOp::Add, K::Timespan(n), K::Timestamp(p))
        | (ArithOp::Add, K::Timestamp(p), K::Long(n)) | (ArithOp::Add, K::Long(n), K::Timestamp(p)) => {
            return Ok(K::Timestamp(if *p == NULL_LONG || *n == NULL_LONG { NULL_LONG } else { p.saturating_add(*n) }));
        }
        (ArithOp::Sub, K::Timestamp(p1), K::Timestamp(p2)) => {
            return Ok(K::Timespan(if *p1 == NULL_LONG || *p2 == NULL_LONG { NULL_LONG } else { p1 - p2 }));
        }
        (ArithOp::Sub, K::Timestamp(p), K::Timespan(n)) | (ArithOp::Sub, K::Timestamp(p), K::Long(n)) => {
            return Ok(K::Timestamp(if *p == NULL_LONG || *n == NULL_LONG { NULL_LONG } else { p.saturating_sub(*n) }));
        }
        (ArithOp::Add, K::Time(t1), K::Time(t2)) => {
            return Ok(K::Time(if *t1 == NULL_INT || *t2 == NULL_INT { NULL_INT } else { t1 + t2 }));
        }
        (ArithOp::Sub, K::Time(t1), K::Time(t2)) => {
            return Ok(K::Time(if *t1 == NULL_INT || *t2 == NULL_INT { NULL_INT } else { t1 - t2 }));
        }
        (ArithOp::Add, K::Time(t), K::Long(n)) | (ArithOp::Add, K::Long(n), K::Time(t)) => {
            return Ok(K::Time(if *t == NULL_INT || *n == NULL_LONG { NULL_INT } else { t + (*n as i32) }));
        }
        (ArithOp::Sub, K::Time(t), K::Long(n)) => {
            return Ok(K::Time(if *t == NULL_INT || *n == NULL_LONG { NULL_INT } else { t - (*n as i32) }));
        }
        (ArithOp::Add, K::Month(m), K::Long(n)) | (ArithOp::Add, K::Long(n), K::Month(m)) => {
            return Ok(K::Month(if *m == NULL_INT || *n == NULL_LONG { NULL_INT } else { m + (*n as i32) }));
        }
        (ArithOp::Sub, K::Month(m), K::Long(n)) => {
            return Ok(K::Month(if *m == NULL_INT || *n == NULL_LONG { NULL_INT } else { m - (*n as i32) }));
        }
        _ => {}
    }

    if matches!(a, K::Float(_) | K::Real(_) | K::Datetime(_)) || matches!(b, K::Float(_) | K::Real(_) | K::Datetime(_)) {
        let fa = a.as_f64()?;
        let fb = b.as_f64()?;
        let r = match op {
            ArithOp::Add => fa + fb,
            ArithOp::Sub => fa - fb,
            ArithOp::Mul => fa * fb,
        };
        Ok(K::Float(r))
    } else {
        let ia = a.as_i64()?;
        let ib = b.as_i64()?;
        if ia == NULL_LONG || ib == NULL_LONG {
            return Ok(K::Long(NULL_LONG));
        }
        let r = match op {
            ArithOp::Add => ia.wrapping_add(ib),
            ArithOp::Sub => ia.wrapping_sub(ib),
            ArithOp::Mul => ia.wrapping_mul(ib),
        };
        Ok(K::Long(r))
    }
}

fn arith_op(lhs: &K, rhs: &K, op: ArithOp) -> KResult<K> {
    // Typed loops: floats with any number, and longs with longs
    match operands(lhs, rhs) {
        Some((a, b)) if is_float(&a) || is_float(&b) => {
            if let (Some(x), Some(y)) = (floats(&a), floats(&b)) {
                return Ok(float_vec(zip(&x, &y, |&x, &y| match op {
                    ArithOp::Add => x + y,
                    ArithOp::Sub => x - y,
                    ArithOp::Mul => x * y,
                })));
            }
        }
        Some((ColVec::Long(_, a), ColVec::Long(_, b))) => {
            let out = zip(&a, &b, |&x, &y| {
                if x == NULL_LONG || y == NULL_LONG {
                    NULL_LONG
                } else {
                    match op {
                        ArithOp::Add => x.wrapping_add(y),
                        ArithOp::Sub => x.wrapping_sub(y),
                        ArithOp::Mul => x.wrapping_mul(y),
                    }
                }
            });
            return Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(out))));
        }
        _ => {}
    }
    broadcast_atoms(lhs, rhs, |a, b| arith_atom(a, b, op))
}

fn float_arith_op(lhs: &K, rhs: &K, f: impl Fn(f64, f64) -> f64 + Copy) -> KResult<K> {
    if let Some((a, b)) = operands(lhs, rhs) {
        if let (Some(x), Some(y)) = (floats(&a), floats(&b)) {
            return Ok(float_vec(zip(&x, &y, |&x, &y| f(x, y))));
        }
    }
    broadcast_atoms(lhs, rhs, |a, b| Ok(K::Float(f(a.as_f64()?, b.as_f64()?))))
}

fn int_arith_op(lhs: &K, rhs: &K, f: impl Fn(i64, i64) -> i64 + Copy) -> KResult<K> {
    broadcast_atoms(lhs, rhs, |a, b| {
        let ia = a.as_i64()?;
        let ib = b.as_i64()?;
        Ok(K::Long(if ia == NULL_LONG || ib == NULL_LONG { NULL_LONG } else { f(ia, ib) }))
    })
}

fn minmax_bin_op(lhs: &K, rhs: &K, is_min: bool) -> KResult<K> {
    if let (K::Vector(ColVec::Bool(_, a)), K::Vector(ColVec::Bool(_, b))) = (lhs, rhs) {
        if a.len() != b.len() {
            return Err(KError::length("vector length mismatch"));
        }
        let out: Vec<bool> = a
            .iter()
            .zip(b.iter())
            .map(|(&x, &y)| if is_min { x && y } else { x || y })
            .collect();
        return Ok(K::Vector(ColVec::Bool(Attr::None, Arc::new(out))));
    }
    broadcast_atoms(lhs, rhs, |a, b| {
        if let (K::Bool(x), K::Bool(y)) = (a, b) {
            return Ok(K::Bool(if is_min { *x && *y } else { *x || *y }));
        }
        let cmp = a.k_cmp(b);
        if (is_min && cmp != Ordering::Greater) || (!is_min && cmp != Ordering::Less) {
            Ok(a.clone())
        } else {
            Ok(b.clone())
        }
    })
}

fn fill_op(lhs: &K, rhs: &K) -> KResult<K> {
    // `lhs ^ rhs`: replace nulls in `rhs` with `lhs`!
    broadcast_atoms(lhs, rhs, |fill_val, v| {
        if v.is_null() {
            Ok(fill_val.clone())
        } else {
            Ok(v.clone())
        }
    })
}

#[derive(Clone, Copy)]
enum CmpKind {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

fn cmp_atom(a: &K, b: &K, kind: CmpKind) -> KResult<bool> {
    let res = match kind {
        CmpKind::Eq => match (a, b) {
            (K::Sym(x), K::Sym(y)) => x == y,
            (K::Char(x), K::Char(y)) => x == y,
            (K::Bool(x), K::Bool(y)) => x == y,
            (K::Guid(x), K::Guid(y)) => x == y,
            (K::Date(x), K::Date(y))
            | (K::Month(x), K::Month(y))
            | (K::Time(x), K::Time(y))
            | (K::Second(x), K::Second(y))
            | (K::Minute(x), K::Minute(y)) => x == y,
            (K::Timestamp(x), K::Timestamp(y)) | (K::Timespan(x), K::Timespan(y)) => x == y,
            _ if matches!(a, K::Float(_) | K::Real(_)) || matches!(b, K::Float(_) | K::Real(_)) => {
                let fa = a.as_f64()?;
                let fb = b.as_f64()?;
                (fa.is_nan() && fb.is_nan()) || fa == fb
            }
            _ => {
                if let (Ok(ia), Ok(ib)) = (a.as_i64(), b.as_i64()) {
                    ia == ib
                } else {
                    a == b
                }
            }
        },
        CmpKind::Ne => !cmp_atom(a, b, CmpKind::Eq)?,
        CmpKind::Lt => a.k_cmp(b) == Ordering::Less,
        CmpKind::Le => a.k_cmp(b) != Ordering::Greater,
        CmpKind::Gt => a.k_cmp(b) == Ordering::Greater,
        CmpKind::Ge => a.k_cmp(b) != Ordering::Less,
    };
    Ok(res)
}

/// Compare vectors of one type, or numbers of any, in a typed loop; `None` leaves the operands to the general path.
fn cmp_typed(lhs: &K, rhs: &K, kind: CmpKind) -> Option<Vec<bool>> {
    let (a, b) = operands(lhs, rhs)?;
    // whether the comparison holds when the left side is less than, equal to, or greater than the right
    let accept = match kind {
        CmpKind::Eq => [false, true, false],
        CmpKind::Ne => [true, false, true],
        CmpKind::Lt => [true, false, false],
        CmpKind::Le => [true, true, false],
        CmpKind::Gt => [false, false, true],
        CmpKind::Ge => [false, true, true],
    };
    let holds = |o: Ordering| accept[(o as i8 + 1) as usize];
    macro_rules! same_type {
        ($($t:ident)*) => {
            match (&a, &b) {
                $((ColVec::$t(_, x), ColVec::$t(_, y)) => return Some(zip(x, y, |x, y| holds(x.cmp(y)))),)*
                _ => {}
            }
        };
    }
    same_type!(Bool Byte Short Int Long Char Timestamp Month Date Timespan Minute Second Time);
    if let (ColVec::Sym(_, x), ColVec::Sym(_, y)) = (&a, &b) {
        // symbols are ordered by their names, but equality needs no look at them
        return Some(match kind {
            CmpKind::Eq => zip(x, y, |x, y| x == y),
            CmpKind::Ne => zip(x, y, |x, y| x != y),
            _ => zip(x, y, |x, y| holds(x.cmp(y))),
        });
    }
    Some(if is_float(&a) || is_float(&b) {
        // a null sorts before every number; computed without branches, which random data would mispredict
        zip(&floats(&a)?, &floats(&b)?, |x, y| {
            let order = i8::from(x > y) - i8::from(x < y) + i8::from(y.is_nan()) - i8::from(x.is_nan());
            accept[(order + 1) as usize]
        })
    } else {
        zip(&a.as_i64_vec()?, &b.as_i64_vec()?, |x, y| holds(x.cmp(y)))
    })
}

fn cmp_op(lhs: &K, rhs: &K, kind: CmpKind) -> KResult<K> {
    if let Some(out) = cmp_typed(lhs, rhs, kind) {
        return Ok(bool_vec(out));
    }
    // Notice in kdb+: `"abc" = "abd"` compares elementwise (`110b`), NOT string atom equality!
    broadcast_atoms(lhs, rhs, |a, b| Ok(K::Bool(cmp_atom(a, b, kind)?)))
}

// ----- Typed loops over whole vectors ---------------------------------------

/// Both operands as vectors, an atom as one of length 1; `None` for two atoms, other shapes or different lengths.
fn operands(lhs: &K, rhs: &K) -> Option<(ColVec, ColVec)> {
    let one = |k: &K| k.is_atom().then(|| ColVec::from_atoms(vec![k.clone()]));
    match (lhs, rhs) {
        (K::Vector(a), K::Vector(b)) => (a.len() == b.len()).then(|| (a.clone(), b.clone())),
        (K::Vector(a), b) => Some((a.clone(), one(b)?)),
        (a, K::Vector(b)) => Some((one(a)?, b.clone())),
        _ => None,
    }
}

/// `f` of two operands element by element, one of length 1 serving every element of the other.
fn zip<A, B, T>(x: &[A], y: &[B], f: impl Fn(&A, &B) -> T) -> Vec<T> {
    if x.len() == y.len() {
        x.iter().zip(y).map(|(x, y)| f(x, y)).collect()
    } else if y.len() == 1 {
        x.iter().map(|x| f(x, &y[0])).collect()
    } else {
        y.iter().map(|y| f(&x[0], y)).collect()
    }
}

fn is_float(c: &ColVec) -> bool {
    matches!(c, ColVec::Real(..) | ColVec::Float(..) | ColVec::Datetime(..))
}

/// A numeric or temporal vector as floats, nulls as NaN.
fn floats(c: &ColVec) -> Option<Cow<'_, [f64]>> {
    match c {
        ColVec::Float(_, v) => Some(Cow::Borrowed(v.as_slice())),
        other => other.as_f64_vec().map(Cow::Owned),
    }
}

fn float_vec(v: Vec<f64>) -> K {
    K::Vector(ColVec::Float(Attr::None, Arc::new(v)))
}

fn bool_vec(v: Vec<bool>) -> K {
    K::Vector(ColVec::Bool(Attr::None, Arc::new(v)))
}

/// The vector of elementwise results. An empty one keeps the type the verb would give (probed with nulls), so that
/// aggregates of no rows are typed like aggregates of some.
fn results(out: Vec<K>, probe: impl FnOnce() -> KResult<K>) -> K {
    match out.is_empty().then(probe) {
        Some(Ok(sample)) if sample.is_atom() => K::Vector(ColVec::from_atoms(vec![sample]).take(0)),
        _ => K::Vector(ColVec::from_atoms(out)),
    }
}

/// Elementwise binary broadcast over atoms, vectors, and dicts (matching kdb+'s conformability rules).
pub fn broadcast_atoms(lhs: &K, rhs: &K, f: impl Fn(&K, &K) -> KResult<K> + Copy) -> KResult<K> {
    match (lhs, rhs) {
        (a, b) if a.is_atom() && b.is_atom() => f(a, b),
        (a, K::Vector(vb)) if a.is_atom() => {
            let mut out = Vec::with_capacity(vb.len());
            for i in 0..vb.len() {
                let bi = vb.get_atom(i);
                out.push(if bi.is_atom() { f(a, &bi)? } else { broadcast_atoms(a, &bi, f)? });
            }
            Ok(results(out, || f(a, &vb.null_atom())))
        }
        (K::Vector(va), b) if b.is_atom() => {
            let mut out = Vec::with_capacity(va.len());
            for i in 0..va.len() {
                let ai = va.get_atom(i);
                out.push(if ai.is_atom() { f(&ai, b)? } else { broadcast_atoms(&ai, b, f)? });
            }
            Ok(results(out, || f(&va.null_atom(), b)))
        }
        (K::Vector(va), K::Vector(vb)) => {
            if va.len() != vb.len() {
                return Err(KError::length(format!("length mismatch ({} vs {})", va.len(), vb.len())));
            }
            let mut out = Vec::with_capacity(va.len());
            for i in 0..va.len() {
                let ai = va.get_atom(i);
                let bi = vb.get_atom(i);
                out.push(broadcast_atoms(&ai, &bi, f)?);
            }
            Ok(results(out, || f(&va.null_atom(), &vb.null_atom())))
        }
        (K::Dict(da), K::Dict(db)) if da.keys == db.keys => {
            Ok(K::Dict(Arc::new(Dict { keys: da.keys.clone(), vals: Box::new(broadcast_atoms(&da.vals, &db.vals, f)?) })))
        }
        (a, K::Dict(db)) if a.is_atom() => {
            Ok(K::Dict(Arc::new(Dict { keys: db.keys.clone(), vals: Box::new(broadcast_atoms(a, &db.vals, f)?) })))
        }
        (K::Dict(da), b) if b.is_atom() => {
            Ok(K::Dict(Arc::new(Dict { keys: da.keys.clone(), vals: Box::new(broadcast_atoms(&da.vals, b, f)?) })))
        }
        _ => Err(KError::ty("incompatible operand shapes")),
    }
}

// ----- Structural Binary Verbs (`,`, `!`, `#`, `_`, `$`, `?`, ...) ----------

pub fn concat_k(lhs: &K, rhs: &K) -> KResult<K> {
    match (lhs, rhs) {
        (K::Vector(a), K::Vector(b)) => Ok(K::Vector(a.concat(b))),
        (K::Vector(a), b) if b.is_atom() => Ok(K::Vector(a.concat(&ColVec::from_atoms(vec![b.clone()])))),
        (a, K::Vector(b)) if a.is_atom() => Ok(K::Vector(ColVec::from_atoms(vec![a.clone()]).concat(b))),
        (a, b) if a.is_atom() && b.is_atom() => Ok(K::Vector(ColVec::from_atoms(vec![a.clone(), b.clone()]))),
        (K::Table(ta), K::Table(tb)) => Ok(K::Table(Arc::new(ta.concat(tb)?))),
        (K::Table(ta), K::Dict(d)) => {
            let one = enlist_op(&K::Dict(d.clone()));
            if let K::Table(tb) = one {
                Ok(K::Table(Arc::new(ta.concat(&tb)?)))
            } else {
                Err(KError::ty("cannot append non-row dictionary to table"))
            }
        }
        (K::KeyedTable(ka), K::KeyedTable(kb)) => crate::engine::joins::union_join(&K::KeyedTable(ka.clone()), &K::KeyedTable(kb.clone())),
        (K::Dict(da), K::Dict(db)) => dict_merge(da, db),
        (K::Vector(ColVec::General(_, a)), other) => {
            let mut v = (**a).clone();
            v.push(other.clone());
            Ok(K::Vector(ColVec::General(Attr::None, Arc::new(v))))
        }
        (other, K::Vector(ColVec::General(_, b))) => {
            let mut v = Vec::with_capacity(1 + b.len());
            v.push(other.clone());
            v.extend_from_slice(b);
            Ok(K::Vector(ColVec::General(Attr::None, Arc::new(v))))
        }
        _ => Err(KError::ty("cannot join incompatible values")),
    }
}

fn dict_merge(da: &Dict, db: &Dict) -> KResult<K> {
    let mut keys = Vec::new();
    let mut vals = Vec::new();
    let mut pos: HashMap<Vec<u8>, usize> = HashMap::new();
    for src in [da, db] {
        let n = src.keys.count();
        for i in 0..n {
            let k = index_k(&src.keys, &[Some(K::Long(i as i64))])?;
            let v = index_k(&src.vals, &[Some(K::Long(i as i64))])?;
            let hk = k.hash_key();
            if let Some(&idx) = pos.get(&hk) {
                vals[idx] = v;
            } else {
                pos.insert(hk, keys.len());
                keys.push(k);
                vals.push(v);
            }
        }
    }
    Ok(K::Dict(Arc::new(Dict {
        keys: Box::new(K::Vector(ColVec::from_atoms(keys))),
        vals: Box::new(K::Vector(ColVec::from_atoms(vals))),
    })))
}

fn bang_op(lhs: &K, rhs: &K) -> KResult<K> {
    // 1. `n ! table` -> key first `n` columns (`0 ! kt` unkeys a keyed table!)
    match (lhs, rhs) {
        (K::Long(n), K::Table(t)) => return KeyedTable::from_flip_nkeys(t, (*n).max(0) as usize),
        (K::Int(n), K::Table(t)) => return KeyedTable::from_flip_nkeys(t, (*n).max(0) as usize),
        (K::Long(n), K::KeyedTable(kt)) => return KeyedTable::from_flip_nkeys(&kt.unkey(), (*n).max(0) as usize),
        (K::Int(n), K::KeyedTable(kt)) => return KeyedTable::from_flip_nkeys(&kt.unkey(), (*n).max(0) as usize),
        // 2. `KeyedTable` from `Table ! Table` (`([k:...] ...)`)
        (K::Table(kt), K::Table(vt)) => return Ok(K::KeyedTable(Arc::new(KeyedTable::new((**kt).clone(), (**vt).clone())?))),
        _ => {}
    }
    // 3. Dictionary `keys ! vals` (if atoms, auto-enlist to 1-element vectors as in kdb+!)
    let k = if lhs.is_atom() { enlist_op(lhs) } else { lhs.clone() };
    let v = if rhs.is_atom() { enlist_op(rhs) } else { rhs.clone() };
    if k.count() != v.count() {
        return Err(KError::length("dictionary keys and values must have equal count"));
    }
    Ok(K::Dict(Arc::new(Dict { keys: Box::new(k), vals: Box::new(v) })))
}

fn take_or_attr_op(lhs: &K, rhs: &K) -> KResult<K> {
    // Attribute application: `` `s#x ``, `` `u#x ``, `` `p#x ``, `` `g#x ``, `` `#x ``
    if let K::Sym(s) = lhs {
        let s_str = resolve(*s);
        if let Some(attr) = Attr::from_sym(&s_str) {
            return match rhs {
                K::Vector(cv) => Ok(K::Vector(cv.with_attr(attr)?)),
                K::Table(t) => {
                    if t.vals.is_empty() {
                        Ok(rhs.clone())
                    } else {
                        let mut vals = t.vals.clone();
                        vals[0] = vals[0].with_attr(attr)?;
                        Ok(K::Table(Arc::new(Flip { cols: t.cols.clone(), vals })))
                    }
                }
                other => Ok(other.clone()),
            };
        }
        // Column subset take: `` `a`b # table `` or `` (enlist `a) # table ``
        let syms = vec![*s];
        return subset_table_or_dict(&syms, rhs);
    }
    if let K::Vector(ColVec::Sym(_, syms)) = lhs {
        return subset_table_or_dict(syms, rhs);
    }
    // Reshape `r c # vec` (2-element integer list on LHS)
    if let K::Vector(cv_shape) = lhs {
        if let Some(dims) = cv_shape.as_i64_vec() {
            if dims.len() == 2 && dims[0] >= 0 && dims[1] >= 0 {
                let r = dims[0] as usize;
                let c = dims[1] as usize;
                let flat = take_or_attr_op(&K::Long((r * c) as i64), rhs)?;
                if let K::Vector(fcv) = flat {
                    let mut rows = Vec::with_capacity(r);
                    for i in 0..r {
                        let idx: Vec<usize> = (i * c..(i + 1) * c).collect();
                        rows.push(K::Vector(fcv.gather(&idx)));
                    }
                    return Ok(K::Vector(ColVec::General(Attr::None, Arc::new(rows))));
                }
            }
        }
    }

    let n = lhs.as_i64()?;
    match rhs {
        K::Vector(cv) => Ok(K::Vector(cv.take(n))),
        K::Table(t) => Ok(K::Table(Arc::new(t.take(n)))),
        K::KeyedTable(kt) => Ok(K::KeyedTable(Arc::new(KeyedTable { keys: kt.keys.take(n), vals: kt.vals.take(n) }))),
        K::Dict(d) => Ok(K::Dict(Arc::new(Dict {
            keys: Box::new(take_or_attr_op(lhs, &d.keys)?),
            vals: Box::new(take_or_attr_op(lhs, &d.vals)?),
        }))),
        atom => Ok(K::Vector(ColVec::from_atoms(vec![atom.clone()]).take(n))),
    }
}

fn subset_table_or_dict(syms: &[SymId], rhs: &K) -> KResult<K> {
    match rhs {
        K::Table(t) => {
            let mut cols = Vec::with_capacity(syms.len());
            let mut vals = Vec::with_capacity(syms.len());
            for &s in syms {
                let idx = t.col_idx(s).ok_or_else(|| KError::value(resolve(s)))?;
                cols.push(s);
                vals.push(t.vals[idx].clone());
            }
            Ok(K::Table(Arc::new(Flip { cols, vals })))
        }
        K::Dict(d) => {
            let mut vals = Vec::with_capacity(syms.len());
            for &s in syms {
                vals.push(dict_lookup(d, &K::Sym(s))?);
            }
            Ok(K::Dict(Arc::new(Dict {
                keys: Box::new(K::Vector(ColVec::Sym(Attr::None, Arc::new(syms.to_vec())))),
                vals: Box::new(K::Vector(ColVec::from_atoms(vals))),
            })))
        }
        _ => Err(KError::ty("symbol # expects table or dictionary")),
    }
}

fn drop_or_cut_op(lhs: &K, rhs: &K) -> KResult<K> {
    // Drop columns/keys by symbol: `` `col _ table `` or `` `a`b _ dict ``
    if let K::Sym(s) = lhs {
        return drop_syms(&[*s], rhs);
    }
    if let K::Vector(ColVec::Sym(_, syms)) = lhs {
        return drop_syms(syms, rhs);
    }
    // Cut at index list `0 2 5 _ vec`
    if matches!(lhs, K::Vector(_)) {
        return cut_op(lhs, rhs);
    }
    let n = lhs.as_i64()?;
    match rhs {
        K::Vector(cv) => Ok(K::Vector(cv.drop_n(n))),
        K::Table(t) => Ok(K::Table(Arc::new(t.drop_n(n)))),
        K::KeyedTable(kt) => Ok(K::KeyedTable(Arc::new(KeyedTable { keys: kt.keys.drop_n(n), vals: kt.vals.drop_n(n) }))),
        K::Dict(d) => Ok(K::Dict(Arc::new(Dict {
            keys: Box::new(drop_or_cut_op(lhs, &d.keys)?),
            vals: Box::new(drop_or_cut_op(lhs, &d.vals)?),
        }))),
        _ => Err(KError::ty("drop expects vector, table, or dictionary")),
    }
}

fn drop_syms(syms: &[SymId], rhs: &K) -> KResult<K> {
    match rhs {
        K::Table(t) => {
            let mut cols = Vec::new();
            let mut vals = Vec::new();
            for (c, v) in t.cols.iter().zip(&t.vals) {
                if !syms.contains(c) {
                    cols.push(*c);
                    vals.push(v.clone());
                }
            }
            Ok(K::Table(Arc::new(Flip { cols, vals })))
        }
        K::Dict(d) => {
            let mut keys = Vec::new();
            let mut vals = Vec::new();
            for i in 0..d.keys.count() {
                let k = index_k(&d.keys, &[Some(K::Long(i as i64))])?;
                if let K::Sym(ks) = k {
                    if syms.contains(&ks) {
                        continue;
                    }
                }
                keys.push(k);
                vals.push(index_k(&d.vals, &[Some(K::Long(i as i64))])?);
            }
            Ok(K::Dict(Arc::new(Dict {
                keys: Box::new(K::Vector(ColVec::from_atoms(keys))),
                vals: Box::new(K::Vector(ColVec::from_atoms(vals))),
            })))
        }
        _ => Err(KError::ty("symbol _ expects table or dictionary")),
    }
}

pub fn sublist_op(lhs: &K, rhs: &K) -> KResult<K> {
    if let K::Vector(cv) = lhs {
        if let Some(pair) = cv.as_i64_vec() {
            if pair.len() == 2 {
                let dropped = drop_or_cut_op(&K::Long(pair[0].max(0)), rhs)?;
                let avail = dropped.count() as i64;
                return take_or_attr_op(&K::Long(pair[1].min(avail).max(0)), &dropped);
            }
        }
    }
    let n = lhs.as_i64()?;
    let avail = rhs.count() as i64;
    let clamped = if n >= 0 { n.min(avail) } else { n.max(-avail) };
    take_or_attr_op(&K::Long(clamped), rhs)
}

fn cut_op(lhs: &K, rhs: &K) -> KResult<K> {
    if lhs.is_atom() {
        let step = lhs.as_i64()?;
        if step <= 0 {
            return Err(KError::domain("cut size must be positive"));
        }
        let len = rhs.count() as i64;
        let starts: Vec<i64> = (0..len).step_by(step as usize).collect();
        return cut_op(&K::Vector(ColVec::Long(Attr::None, Arc::new(starts))), rhs);
    }
    let K::Vector(idx_cv) = lhs else {
        return Err(KError::ty("cut expects integer or index vector"));
    };
    let idx = idx_cv.as_i64_vec().ok_or_else(|| KError::ty("cut expects integer indices"))?;
    let K::Vector(src) = rhs else {
        return Err(KError::ty("cut RHS must be a vector"));
    };
    let mut parts = Vec::with_capacity(idx.len());
    for i in 0..idx.len() {
        let s = idx[i].max(0) as usize;
        let e = if i + 1 < idx.len() { (idx[i + 1].max(0) as usize).min(src.len()) } else { src.len() };
        let range: Vec<usize> = if s < e { (s..e).collect() } else { vec![] };
        parts.push(K::Vector(src.gather(&range)));
    }
    Ok(K::Vector(ColVec::General(Attr::None, Arc::new(parts))))
}

// ----- Cast / Parse / Pad (`$`) ---------------------------------------------

pub fn cast_op(lhs: &K, rhs: &K) -> KResult<K> {
    // 1. Padding `width $ string` (`10$"abc"`, `-10$"abc"`)
    if let Ok(width) = lhs.as_i64() {
        let s = rhs.as_str().unwrap_or_else(|| rhs.cell_text());
        let w = width.unsigned_abs() as usize;
        let padded = if s.len() >= w {
            if width >= 0 { s[..w].to_string() } else { s[s.len() - w..].to_string() }
        } else if width >= 0 {
            format!("{s}{}", " ".repeat(w - s.len()))
        } else {
            format!("{}{s}", " ".repeat(w - s.len()))
        };
        return Ok(K::string(&padded));
    }

    // 2. Symbol cast `` `int$x ``, `` `float$() ``, `` `$("a";"b") ``, `` `year$date ``
    if let K::Sym(tsym) = lhs {
        let tname = resolve(*tsym);
        // Temporal component cast `` `year$d ``, `` `mm$d ``, `` `minute$t ``
        if let Some(part) = try_temporal_field(rhs, &tname) {
            return Ok(part);
        }
        return cast_by_name(&tname, rhs);
    }

    // 3. Char cast `"I"$"42"` (uppercase = parse string!), `"i"$3.5` (lowercase = binary/value cast!)
    if let K::Char(ch) = lhs {
        let c = *ch as char;
        if c.is_ascii_uppercase() {
            return parse_from_string_type(c.to_ascii_lowercase(), rhs);
        } else {
            let tname = type_name_from_char(c).ok_or_else(|| KError::domain(format!("unknown cast char '{c}'")))?;
            return cast_by_name(tname, rhs);
        }
    }

    Err(KError::ty("unsupported cast LHS"))
}

fn type_name_from_char(c: char) -> Option<&'static str> {
    Some(match c {
        'b' => "boolean",
        'g' => "guid",
        'x' => "byte",
        'h' => "short",
        'i' => "int",
        'j' => "long",
        'e' => "real",
        'f' => "float",
        'c' => "char",
        's' => "symbol",
        'p' => "timestamp",
        'm' => "month",
        'd' => "date",
        'z' => "datetime",
        'n' => "timespan",
        'u' => "minute",
        'v' => "second",
        't' => "time",
        _ => return None,
    })
}

fn cast_by_name(tname: &str, rhs: &K) -> KResult<K> {
    // Empty general list `()` cast to typed empty vector: `` `float$() ``, `` `symbol$() ``!
    if let K::Vector(ColVec::General(_, xs)) = rhs {
        if xs.is_empty() {
            let ch = match tname {
                "" | "symbol" => 's',
                "boolean" | "bool" => 'b',
                "guid" => 'g',
                "byte" => 'x',
                "short" => 'h',
                "int" => 'i',
                "long" => 'j',
                "real" => 'e',
                "float" => 'f',
                "char" => 'c',
                "timestamp" => 'p',
                "month" => 'm',
                "date" => 'd',
                "datetime" => 'z',
                "timespan" => 'n',
                "minute" => 'u',
                "second" => 'v',
                "time" => 't',
                _ => return Err(KError::domain(format!("unknown type `{tname}"))),
            };
            return Ok(K::Vector(ColVec::empty_of_type_char(ch).unwrap()));
        }
    }

    // `` `$"hello" `` casts a char vector (string) into a SINGLE `Sym` atom! And `` `$("a";"b") `` casts a list of strings into a `Sym` vector!
    if tname.is_empty() || tname == "symbol" {
        return match rhs {
            K::Sym(_) => Ok(rhs.clone()),
            K::Char(c) => Ok(K::Sym(intern(&(*c as char).to_string()))),
            K::Vector(ColVec::Char(_, bs)) => Ok(K::Sym(intern(&String::from_utf8_lossy(bs)))),
            K::Vector(ColVec::Sym(_, _)) => Ok(rhs.clone()),
            K::Vector(ColVec::General(_, xs)) => {
                let mut syms = Vec::with_capacity(xs.len());
                for x in xs.iter() {
                    let s = x.as_str().ok_or_else(|| KError::ty("`$ expects string"))?;
                    syms.push(intern(&s));
                }
                Ok(K::Vector(ColVec::Sym(Attr::None, Arc::new(syms))))
            }
            _ => Err(KError::ty("`$ expects string or char")),
        };
    }

    if let K::Vector(cv) = rhs {
        let mut out = Vec::with_capacity(cv.len());
        for i in 0..cv.len() {
            out.push(cast_atom_by_name(tname, &cv.get_atom(i))?);
        }
        return Ok(K::Vector(ColVec::from_atoms(out)));
    }
    cast_atom_by_name(tname, rhs)
}

fn cast_atom_by_name(tname: &str, v: &K) -> KResult<K> {
    Ok(match tname {
        "boolean" | "bool" => K::Bool(v.as_i64().map(|n| n != 0 && n != NULL_LONG).unwrap_or(false)),
        "byte" => K::Byte(v.as_i64()? as u8),
        "short" => {
            let n = v.as_f64()?;
            K::Short(if n.is_nan() { NULL_SHORT } else { n as i16 })
        }
        "int" => {
            let n = v.as_f64()?;
            K::Int(if n.is_nan() { NULL_INT } else { n as i32 })
        }
        "long" => {
            let n = v.as_f64()?;
            K::Long(if n.is_nan() { NULL_LONG } else { n as i64 })
        }
        "real" => K::Real(v.as_f64()? as f32),
        "float" => K::Float(v.as_f64()?),
        "char" => match v {
            K::Char(c) => K::Char(*c),
            n => K::Char(n.as_i64()? as u8),
        },
        "date" => {
            if let K::Timestamp(ns) = v {
                K::Date(if *ns == NULL_LONG { NULL_INT } else { ns.div_euclid(NANOS_PER_DAY) as i32 })
            } else {
                let n = v.as_i64()?;
                K::Date(if n == NULL_LONG { NULL_INT } else { n as i32 })
            }
        }
        "month" => {
            if let K::Date(d) = v {
                date_part(*d, "month").unwrap_or(K::Month(NULL_INT))
            } else {
                let n = v.as_i64()?;
                K::Month(if n == NULL_LONG { NULL_INT } else { n as i32 })
            }
        }
        "timestamp" => match v {
            K::Date(d) => K::Timestamp(if *d == NULL_INT { NULL_LONG } else { (*d as i64) * NANOS_PER_DAY }),
            other => K::Timestamp(other.as_i64()?),
        },
        "timespan" => match v {
            K::Time(ms) => K::Timespan(if *ms == NULL_INT { NULL_LONG } else { (*ms as i64) * NANOS_PER_MILLI }),
            K::Second(s) => K::Timespan(if *s == NULL_INT { NULL_LONG } else { (*s as i64) * NANOS_PER_SEC }),
            K::Minute(m) => K::Timespan(if *m == NULL_INT { NULL_LONG } else { (*m as i64) * NANOS_PER_MIN }),
            other => K::Timespan(other.as_i64()?),
        },
        "time" => match v {
            K::Timestamp(ns) | K::Timespan(ns) => K::Time(if *ns == NULL_LONG { NULL_INT } else { (ns.rem_euclid(NANOS_PER_DAY) / NANOS_PER_MILLI) as i32 }),
            K::Second(s) => K::Time(if *s == NULL_INT { NULL_INT } else { s * 1000 }),
            K::Minute(m) => K::Time(if *m == NULL_INT { NULL_INT } else { m * 60_000 }),
            other => {
                let n = other.as_i64()?;
                K::Time(if n == NULL_LONG { NULL_INT } else { n as i32 })
            }
        },
        "second" => match v {
            K::Time(ms) => K::Second(if *ms == NULL_INT { NULL_INT } else { ms / 1000 }),
            K::Minute(m) => K::Second(if *m == NULL_INT { NULL_INT } else { m * 60 }),
            other => K::Second(other.as_i64()? as i32),
        },
        "minute" => match v {
            K::Time(ms) => K::Minute(if *ms == NULL_INT { NULL_INT } else { ms / 60_000 }),
            K::Second(s) => K::Minute(if *s == NULL_INT { NULL_INT } else { s / 60 }),
            other => K::Minute(other.as_i64()? as i32),
        },
        _ => return Err(KError::domain(format!("unsupported cast target `{tname}"))),
    })
}

pub fn parse_atom_from_str(type_ch: char, s: &str) -> K {
    let t = s.trim();
    match type_ch {
        'b' => K::Bool(matches!(t, "1" | "1b" | "true" | "TRUE" | "t" | "T")),
        'g' => K::Guid(parse_guid(t).unwrap_or(NULL_GUID)),
        'x' => K::Byte(u8::from_str_radix(t.trim_start_matches("0x"), 16).unwrap_or(0)),
        'h' => K::Short(t.trim_end_matches('h').parse().unwrap_or(NULL_SHORT)),
        'i' => K::Int(t.trim_end_matches('i').parse().unwrap_or(NULL_INT)),
        'j' => K::Long(t.trim_end_matches('j').parse().unwrap_or(NULL_LONG)),
        'e' => K::Real(t.trim_end_matches('e').parse().unwrap_or(f32::NAN)),
        'f' => K::Float(t.trim_end_matches('f').parse().unwrap_or(f64::NAN)),
        'c' => K::Char(s.as_bytes().first().copied().unwrap_or(b' ')),
        's' => K::Sym(intern(t)),
        'd' => {
            let norm = t.replace('-', ".");
            let parts: Vec<&str> = norm.split('.').collect();
            if parts.len() == 3 {
                if let (Ok(y), Ok(m), Ok(d)) = (parts[0].parse(), parts[1].parse(), parts[2].parse()) {
                    if let Some(days) = date_from_ymd(y, m, d) {
                        return K::Date(days);
                    }
                }
            }
            K::Date(NULL_INT)
        }
        'm' => {
            let norm = t.trim_end_matches('m').replace('-', ".");
            let parts: Vec<&str> = norm.split('.').collect();
            if parts.len() >= 2 {
                if let (Ok(y), Ok(m)) = (parts[0].parse::<i32>(), parts[1].parse::<i32>()) {
                    return K::Month((y - 2000) * 12 + (m - 1));
                }
            }
            K::Month(NULL_INT)
        }
        'p' => {
            let norm = t.replace('-', ".").replace(['T', ' '], "D");
            if let Ok(mut toks) = crate::lexer::tokenize(&norm) {
                if let Some(crate::lexer::Tok::Lit(K::Timestamp(ns))) = toks.drain(..1).next() {
                    return K::Timestamp(ns);
                }
            }
            K::Timestamp(NULL_LONG)
        }
        't' => {
            if let Ok(mut toks) = crate::lexer::tokenize(t) {
                if let Some(crate::lexer::Tok::Lit(k)) = toks.drain(..1).next() {
                    if let Ok(K::Time(ms)) = cast_atom_by_name("time", &k) {
                        return K::Time(ms);
                    }
                }
            }
            K::Time(NULL_INT)
        }
        'u' => {
            if let Ok(mut toks) = crate::lexer::tokenize(t) {
                if let Some(crate::lexer::Tok::Lit(K::Minute(m))) = toks.drain(..1).next() {
                    return K::Minute(m);
                }
            }
            K::Minute(NULL_INT)
        }
        'v' => {
            if let Ok(mut toks) = crate::lexer::tokenize(t) {
                if let Some(crate::lexer::Tok::Lit(K::Second(sec))) = toks.drain(..1).next() {
                    return K::Second(sec);
                }
            }
            K::Second(NULL_INT)
        }
        _ => K::string(s),
    }
}

fn parse_from_string_type(type_ch: char, rhs: &K) -> KResult<K> {
    match rhs {
        K::Char(c) => Ok(parse_atom_from_str(type_ch, &(*c as char).to_string())),
        K::Vector(ColVec::Char(_, bs)) => Ok(parse_atom_from_str(type_ch, &String::from_utf8_lossy(bs))),
        K::Vector(ColVec::General(_, xs)) => {
            let mut out = Vec::with_capacity(xs.len());
            for x in xs.iter() {
                let s = x.as_str().unwrap_or_default();
                out.push(parse_atom_from_str(type_ch, &s));
            }
            Ok(K::Vector(ColVec::from_atoms(out)))
        }
        _ => Err(KError::ty("uppercase parse cast expects string or list of strings")),
    }
}

// ----- Search, Membership, Pattern & String Verbs ---------------------------

fn find_or_roll_op(lhs: &K, rhs: &K) -> KResult<K> {
    // 1. Roll / Deal / Sample (`5 ? 100` -> 5 random longs in `0..100`; `-5 ? 100` -> without replacement; `5 ? \`3` -> random 3-letter syms; `5 ? vec` -> sample from vec!)
    if matches!(lhs, K::Long(_) | K::Int(_)) {
        let n = lhs.as_i64()?;
        // Wait: if LHS is a vector (`10 20 30 ? 20`), `lhs` is `K::Vector`, NOT `K::Long`! So scalar integer LHS is ALWAYS roll/deal in kdb+!
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let un = n.unsigned_abs() as usize;
        if let K::Vector(cv) = rhs {
            if cv.is_empty() {
                return Err(KError::domain("cannot sample from empty vector"));
            }
            let idx: Vec<usize> = (0..un).map(|_| rng.gen_range(0..cv.len())).collect();
            return Ok(K::Vector(cv.gather(&idx)));
        }
        if let K::Sym(s) = rhs {
            let slen: usize = resolve(*s).parse().unwrap_or(4);
            let syms: Vec<SymId> = (0..un)
                .map(|_| {
                    let word: String = (0..slen).map(|_| (b'a' + rng.gen_range(0..26)) as char).collect();
                    intern(&word)
                })
                .collect();
            return Ok(K::Vector(ColVec::Sym(Attr::None, Arc::new(syms))));
        }
        if let K::Float(bound) = rhs {
            let out: Vec<f64> = (0..un).map(|_| rng.gen_range(0.0..*bound)).collect();
            return Ok(K::Vector(ColVec::Float(Attr::None, Arc::new(out))));
        }
        let bound = rhs.as_i64()?;
        if bound <= 0 {
            return Err(KError::domain("roll bound must be positive"));
        }
        if n < 0 {
            if (un as i64) > bound {
                return Err(KError::length("deal count exceeds bound"));
            }
            let mut pool: Vec<i64> = (0..bound).collect();
            for i in 0..un {
                let j = rng.gen_range(i..pool.len());
                pool.swap(i, j);
            }
            pool.truncate(un);
            return Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(pool))));
        } else {
            let out: Vec<i64> = (0..un).map(|_| rng.gen_range(0..bound)).collect();
            return Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(out))));
        }
    }

    // 2. Find (`vec ? x`): first index of each item of `rhs` in `lhs` (`count lhs` if not found, as in kdb+!)
    if let K::Vector(hay) = lhs {
        if crate::index::finds(hay) {
            if let K::Vector(needles) = rhs {
                let out: Vec<i64> = (0..needles.len())
                    .map(|i| crate::index::find_first(hay, &needles.get_atom(i)).unwrap_or_else(|| hay.len()) as i64)
                    .collect();
                return Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(out))));
            } else if let Some(idx) = crate::index::find_first(hay, rhs) {
                return Ok(K::Long(idx as i64));
            }
        }
        let mut first_idx: HashMap<Vec<u8>, i64> = HashMap::with_capacity(hay.len());
        for i in 0..hay.len() {
            first_idx.entry(hay.get_atom(i).hash_key()).or_insert(i as i64);
        }
        let miss = hay.len() as i64;
        if let K::Vector(needles) = rhs {
            let out: Vec<i64> = (0..needles.len())
                .map(|i| *first_idx.get(&needles.get_atom(i).hash_key()).unwrap_or(&miss))
                .collect();
            return Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(out))));
        } else {
            return Ok(K::Long(*first_idx.get(&rhs.hash_key()).unwrap_or(&miss)));
        }
    }
    if let K::Dict(d) = lhs {
        let pos = find_or_roll_op(&d.vals, rhs)?;
        return index_k(&d.keys, &[Some(pos)]);
    }
    Err(KError::ty("? requires integer LHS (roll) or vector/dict LHS (find)"))
}

fn in_op(lhs: &K, rhs: &K) -> KResult<K> {
    // fixed-width values of one type compare as bits, without a key per element
    if let K::Vector(have) = lhs {
        let want = match rhs {
            K::Vector(v) => v.clone(),
            atom => ColVec::from_atoms(vec![atom.clone()]),
        };
        if have.type_num() == want.type_num() {
            if let (Some(have), Some(want)) = (have.key_bits(), want.key_bits()) {
                // a short list is quick to scan; a narrow range of values has a slot for each; others are hashed
                return Ok(bool_vec(if want.len() <= 8 {
                    have.iter().map(|x| want.contains(x)).collect()
                } else if let Some((lo, span)) = narrow(&want, table_room(have.len() + want.len())) {
                    let mut slots = vec![false; span];
                    want.iter().for_each(|x| slots[x.wrapping_sub(lo) as usize] = true);
                    have.iter().map(|x| slots.get(x.wrapping_sub(lo) as usize) == Some(&true)).collect()
                } else {
                    let set: HashSet<u64> = want.into_iter().collect();
                    have.iter().map(|x| set.contains(x)).collect()
                }));
            }
        }
    }
    let set: HashSet<Vec<u8>> = match rhs {
        K::Vector(cv) => (0..cv.len()).map(|i| cv.get_atom(i).hash_key()).collect(),
        atom => [atom.hash_key()].into_iter().collect(),
    };
    if let K::Vector(cv) = lhs {
        Ok(bool_vec((0..cv.len()).map(|i| set.contains(&cv.get_atom(i).hash_key())).collect()))
    } else {
        Ok(K::Bool(set.contains(&lhs.hash_key())))
    }
}

fn within_op(lhs: &K, rhs: &K) -> KResult<K> {
    let K::Vector(bounds) = rhs else {
        return Err(KError::ty("within requires 2-element range on RHS"));
    };
    if bounds.len() != 2 {
        return Err(KError::length("within requires 2-element range [lo; hi]"));
    }
    let lo = bounds.get_atom(0);
    let hi = bounds.get_atom(1);
    // a null sorts before a lower bound that is not null, so such a range leaves nulls out by itself
    if !lo.is_null() && !matches!(lo, K::Char(_)) {
        if let (Some(from), Some(to)) = (cmp_typed(lhs, &lo, CmpKind::Ge), cmp_typed(lhs, &hi, CmpKind::Le)) {
            return Ok(bool_vec(zip(&from, &to, |&from, &to| from && to)));
        }
    }
    if let K::Vector(cv) = lhs {
        Ok(bool_vec(
            (0..cv.len())
                .map(|i| {
                    let a = cv.get_atom(i);
                    !a.is_null() && a.k_cmp(&lo) != Ordering::Less && a.k_cmp(&hi) != Ordering::Greater
                })
                .collect(),
        ))
    } else {
        Ok(K::Bool(!lhs.is_null() && lhs.k_cmp(&lo) != Ordering::Less && lhs.k_cmp(&hi) != Ordering::Greater))
    }
}

pub fn compile_kdb_like(pat: &str) -> KResult<Regex> {
    // kdb+ `like` wildcards: `*` (any sequence), `?` (single char), `[abc]` / `[^abc]` character classes!
    let mut rx = String::from("^");
    let chars: Vec<char> = pat.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '*' => rx.push_str(".*"),
            '?' => rx.push('.'),
            '[' => {
                rx.push('[');
                i += 1;
                if i < chars.len() && chars[i] == '^' {
                    rx.push('^');
                    i += 1;
                }
                while i < chars.len() && chars[i] != ']' {
                    if matches!(chars[i], '\\') {
                        rx.push('\\');
                    }
                    rx.push(chars[i]);
                    i += 1;
                }
                rx.push(']');
            }
            c if ".+()|{}^$\\".contains(c) => {
                rx.push('\\');
                rx.push(c);
            }
            c => rx.push(c),
        }
        i += 1;
    }
    rx.push('$');
    Regex::new(&rx).map_err(|e| KError::domain(format!("invalid like pattern '{pat}': {e}")))
}

fn like_op(lhs: &K, rhs: &K) -> KResult<K> {
    let pat = rhs.as_str().ok_or_else(|| KError::ty("like pattern must be string or symbol"))?;
    let rx = compile_kdb_like(&pat)?;
    // On text of one line a `*` at either end says nothing, and without it the engine can search for the rest instead of
    // running through the whole text. (Text of several lines needs the whole pattern: `*` does not cross lines.)
    let inner = rx.as_str().trim_start_matches("^.*").trim_end_matches(".*$");
    let inner = Regex::new(inner).map_err(|e| KError::domain(e.to_string()))?;
    let found = |s: &str| if s.contains('\n') { rx.is_match(s) } else { inner.is_match(s) };
    // match the text where it lies, without making a string of it for every item
    let symbol = |s: SymId| with_str(s, found);
    let text = |bs: &[u8]| found(&String::from_utf8_lossy(bs));
    match lhs {
        K::Sym(s) => Ok(K::Bool(symbol(*s))),
        K::Char(c) => Ok(K::Bool(found(&(*c as char).to_string()))),
        K::Vector(ColVec::Char(_, bs)) => Ok(K::Bool(text(bs))),
        K::Vector(ColVec::Sym(_, xs)) => Ok(bool_vec(xs.iter().map(|&s| symbol(s)).collect())),
        K::Vector(ColVec::General(_, xs)) => Ok(bool_vec(
            xs.iter()
                .map(|x| match x {
                    K::Vector(ColVec::Char(_, bs)) => text(bs),
                    other => other.as_str().is_some_and(|s| found(&s)),
                })
                .collect(),
        )),
        _ => Err(KError::ty("like LHS must be string, symbol, or list of strings/symbols")),
    }
}

fn ss_op(lhs: &K, rhs: &K) -> KResult<K> {
    let hay = lhs.as_str().ok_or_else(|| KError::ty("ss expects string"))?;
    let needle = rhs.as_str().ok_or_else(|| KError::ty("ss pattern must be string"))?;
    if needle.is_empty() {
        return Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(vec![]))));
    }
    let idx: Vec<i64> = hay.match_indices(&needle).map(|(i, _)| i as i64).collect();
    Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(idx))))
}

pub fn ssr_op(s: &K, pat: &K, rep: &K) -> KResult<K> {
    let hay = s.as_str().ok_or_else(|| KError::ty("ssr expects string"))?;
    let p = pat.as_str().ok_or_else(|| KError::ty("ssr pattern must be string"))?;
    let r = rep.as_str().ok_or_else(|| KError::ty("ssr replacement must be string"))?;
    Ok(K::string(&hay.replace(&p, &r)))
}

fn vs_op(lhs: &K, rhs: &K) -> KResult<K> {
    // `"," vs "a,b,c"` -> `("a";"b";"c")`; `` ` vs `a.b.c `` -> `` `a`b`c ``
    if let (K::Sym(sep), K::Sym(target)) = (lhs, rhs) {
        let d = if sep.is_null() { "." } else { &resolve(*sep) };
        let s = resolve(*target);
        let parts: Vec<SymId> = s.split(d).map(intern).collect();
        return Ok(K::Vector(ColVec::Sym(Attr::None, Arc::new(parts))));
    }
    let delim = lhs.as_str().ok_or_else(|| KError::ty("vs expects char/string/sym delimiter"))?;
    let text = rhs.as_str().ok_or_else(|| KError::ty("vs expects string RHS"))?;
    let parts: Vec<K> = if delim.is_empty() {
        text.lines().map(K::string).collect()
    } else {
        text.split(&delim).map(K::string).collect()
    };
    Ok(K::Vector(ColVec::General(Attr::None, Arc::new(parts))))
}

fn sv_op(lhs: &K, rhs: &K) -> KResult<K> {
    if let (K::Sym(sep), K::Vector(ColVec::Sym(_, syms))) = (lhs, rhs) {
        let d = if sep.is_null() { "." } else { &resolve(*sep) };
        let joined: Vec<String> = syms.iter().map(|&s| resolve(s)).collect();
        return Ok(K::Sym(intern(&joined.join(d))));
    }
    let delim = lhs.as_str().ok_or_else(|| KError::ty("sv expects char/string/sym delimiter"))?;
    let K::Vector(cv) = rhs else {
        return Err(KError::ty("sv expects list of strings/symbols"));
    };
    let mut parts = Vec::with_capacity(cv.len());
    for i in 0..cv.len() {
        parts.push(cv.get_atom(i).as_str().unwrap_or_else(|| cv.get_atom(i).cell_text()));
    }
    Ok(K::string(&parts.join(&delim)))
}

// ----- Time Bucketing (`xbar`), Moving Windows, Weighted Aggs ---------------

fn xbar_op(lhs: &K, rhs: &K) -> KResult<K> {
    let k = lhs.as_i64()?;
    if k <= 0 {
        return Err(KError::domain("xbar interval must be positive"));
    }
    let bar_i64 = |v: i64| if v == NULL_LONG { NULL_LONG } else { k * v.div_euclid(k) };
    let bar_i32 = |v: i32| if v == NULL_INT { NULL_INT } else { (k as i32) * v.div_euclid(k as i32) };

    match rhs {
        K::Long(v) => Ok(K::Long(bar_i64(*v))),
        K::Int(v) => Ok(K::Int(bar_i32(*v))),
        K::Date(v) => Ok(K::Date(bar_i32(*v))),
        K::Month(v) => Ok(K::Month(bar_i32(*v))),
        K::Minute(v) => Ok(K::Minute(bar_i32(*v))),
        K::Second(v) => Ok(K::Second(bar_i32(*v))),
        K::Time(v) => Ok(K::Time(bar_i32(*v))),
        K::Timestamp(v) => Ok(K::Timestamp(bar_i64(*v))),
        K::Timespan(v) => Ok(K::Timespan(bar_i64(*v))),
        K::Float(v) => {
            let kf = k as f64;
            Ok(K::Float(if v.is_nan() { f64::NAN } else { kf * (v / kf).floor() }))
        }
        K::Vector(cv) => match cv {
            ColVec::Long(_, xs) => Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(xs.iter().map(|&v| bar_i64(v)).collect())))),
            ColVec::Int(_, xs) => Ok(K::Vector(ColVec::Int(Attr::None, Arc::new(xs.iter().map(|&v| bar_i32(v)).collect())))),
            ColVec::Date(_, xs) => Ok(K::Vector(ColVec::Date(Attr::None, Arc::new(xs.iter().map(|&v| bar_i32(v)).collect())))),
            ColVec::Month(_, xs) => Ok(K::Vector(ColVec::Month(Attr::None, Arc::new(xs.iter().map(|&v| bar_i32(v)).collect())))),
            ColVec::Minute(_, xs) => Ok(K::Vector(ColVec::Minute(Attr::None, Arc::new(xs.iter().map(|&v| bar_i32(v)).collect())))),
            ColVec::Second(_, xs) => Ok(K::Vector(ColVec::Second(Attr::None, Arc::new(xs.iter().map(|&v| bar_i32(v)).collect())))),
            ColVec::Time(_, xs) => Ok(K::Vector(ColVec::Time(Attr::None, Arc::new(xs.iter().map(|&v| bar_i32(v)).collect())))),
            ColVec::Timestamp(_, xs) => Ok(K::Vector(ColVec::Timestamp(Attr::None, Arc::new(xs.iter().map(|&v| bar_i64(v)).collect())))),
            ColVec::Timespan(_, xs) => Ok(K::Vector(ColVec::Timespan(Attr::None, Arc::new(xs.iter().map(|&v| bar_i64(v)).collect())))),
            ColVec::Float(_, xs) => {
                let kf = k as f64;
                Ok(K::Vector(ColVec::Float(Attr::None, Arc::new(xs.iter().map(|&v| if v.is_nan() { f64::NAN } else { kf * (v / kf).floor() }).collect()))))
            }
            _ => Err(KError::ty("xbar requires numeric or temporal vector")),
        },
        _ => Err(KError::ty("xbar requires numeric or temporal value")),
    }
}

#[derive(Clone, Copy)]
enum MovingKind {
    Avg,
    Sum,
    Count,
    Min,
    Max,
    Dev,
}

fn moving_op(lhs: &K, rhs: &K, kind: MovingKind) -> KResult<K> {
    let w = lhs.as_i64()?.max(1) as usize;
    let K::Vector(cv) = rhs else {
        return Err(KError::ty("moving window requires vector RHS"));
    };
    let fs = cv.as_f64_vec().ok_or_else(|| KError::ty("moving window requires numeric vector"))?;
    let n = fs.len();
    match kind {
        MovingKind::Count => {
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let s = i.saturating_sub(w - 1);
                out.push(fs[s..=i].iter().filter(|v| !v.is_nan()).count() as i64);
            }
            Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(out))))
        }
        MovingKind::Sum => {
            if let Some(is) = cv.as_i64_vec() {
                let mut out = Vec::with_capacity(n);
                for i in 0..n {
                    let s = i.saturating_sub(w - 1);
                    out.push(wrapping_total(&is[s..=i]));
                }
                return Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(out))));
            }
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let s = i.saturating_sub(w - 1);
                out.push(fs[s..=i].iter().filter(|v| !v.is_nan()).sum());
            }
            Ok(K::Vector(ColVec::Float(Attr::None, Arc::new(out))))
        }
        MovingKind::Avg => {
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let s = i.saturating_sub(w - 1);
                let mut sum = 0.0;
                let mut cnt = 0usize;
                for &v in &fs[s..=i] {
                    if !v.is_nan() {
                        sum += v;
                        cnt += 1;
                    }
                }
                out.push(if cnt == 0 { f64::NAN } else { sum / (cnt as f64) });
            }
            Ok(K::Vector(ColVec::Float(Attr::None, Arc::new(out))))
        }
        MovingKind::Min | MovingKind::Max => {
            let is_min = matches!(kind, MovingKind::Min);
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let s = i.saturating_sub(w - 1);
                let mut best: Option<K> = None;
                for j in s..=i {
                    let a = cv.get_atom(j);
                    if a.is_null() {
                        continue;
                    }
                    best = Some(match best {
                        None => a,
                        Some(cur) => {
                            let cmp = a.k_cmp(&cur);
                            if (is_min && cmp == Ordering::Less) || (!is_min && cmp == Ordering::Greater) { a } else { cur }
                        }
                    });
                }
                out.push(best.unwrap_or_else(|| cv.null_atom()));
            }
            Ok(K::Vector(ColVec::from_atoms(out)))
        }
        MovingKind::Dev => {
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let s = i.saturating_sub(w - 1);
                let vals: Vec<f64> = fs[s..=i].iter().copied().filter(|v| !v.is_nan()).collect();
                if vals.is_empty() {
                    out.push(f64::NAN);
                } else {
                    let m = vals.iter().sum::<f64>() / (vals.len() as f64);
                    let var = vals.iter().map(|v| (v - m) * (v - m)).sum::<f64>() / (vals.len() as f64);
                    out.push(var.sqrt());
                }
            }
            Ok(K::Vector(ColVec::Float(Attr::None, Arc::new(out))))
        }
    }
}

fn ema_op(lhs: &K, rhs: &K) -> KResult<K> {
    let alpha = lhs.as_f64()?;
    let K::Vector(cv) = rhs else {
        return Err(KError::ty("ema requires numeric vector RHS"));
    };
    let fs = cv.as_f64_vec().ok_or_else(|| KError::ty("ema requires numeric vector"))?;
    let mut out = Vec::with_capacity(fs.len());
    let mut acc = f64::NAN;
    for v in fs {
        if v.is_nan() {
            out.push(acc);
        } else if acc.is_nan() {
            acc = v;
            out.push(acc);
        } else {
            acc = alpha * v + (1.0 - alpha) * acc;
            out.push(acc);
        }
    }
    Ok(K::Vector(ColVec::Float(Attr::None, Arc::new(out))))
}

fn weighted_op(lhs: &K, rhs: &K, divide_by_weights: bool) -> KResult<K> {
    if lhs.is_atom() && rhs.is_atom() {
        let w = lhs.as_f64()?;
        let x = rhs.as_f64()?;
        return Ok(K::Float(if divide_by_weights { if w == 0.0 { f64::NAN } else { x } } else { w * x }));
    }
    let ws = match lhs {
        K::Vector(cv) => cv.as_f64_vec().ok_or_else(|| KError::ty("wavg/wsum weights must be numeric"))?,
        a => vec![a.as_f64()?; rhs.count()],
    };
    let xs = match rhs {
        K::Vector(cv) => cv.as_f64_vec().ok_or_else(|| KError::ty("wavg/wsum values must be numeric"))?,
        b => vec![b.as_f64()?; lhs.count()],
    };
    if ws.len() != xs.len() {
        return Err(KError::length("wavg/wsum length mismatch"));
    }
    let mut num = 0.0;
    let mut den = 0.0;
    for (w, x) in ws.into_iter().zip(xs) {
        if !w.is_nan() && !x.is_nan() {
            num += w * x;
            den += w;
        }
    }
    Ok(K::Float(if divide_by_weights { if den == 0.0 { f64::NAN } else { num / den } } else { num }))
}

fn cov_cor_op(lhs: &K, rhs: &K, corr: bool) -> KResult<K> {
    let (K::Vector(ca), K::Vector(cb)) = (lhs, rhs) else {
        return Err(KError::ty("cov/cor requires two numeric vectors"));
    };
    let fa = ca.as_f64_vec().ok_or_else(|| KError::ty("cov/cor requires numeric vectors"))?;
    let fb = cb.as_f64_vec().ok_or_else(|| KError::ty("cov/cor requires numeric vectors"))?;
    if fa.len() != fb.len() {
        return Err(KError::length("cov/cor length mismatch"));
    }
    let pairs: Vec<(f64, f64)> = fa.into_iter().zip(fb).filter(|(a, b)| !a.is_nan() && !b.is_nan()).collect();
    if pairs.is_empty() {
        return Ok(K::Float(f64::NAN));
    }
    let n = pairs.len() as f64;
    let mx = pairs.iter().map(|p| p.0).sum::<f64>() / n;
    let my = pairs.iter().map(|p| p.1).sum::<f64>() / n;
    let cov = pairs.iter().map(|(x, y)| (x - mx) * (y - my)).sum::<f64>() / n;
    if !corr {
        return Ok(K::Float(cov));
    }
    let vx = pairs.iter().map(|(x, _)| (x - mx) * (x - mx)).sum::<f64>() / n;
    let vy = pairs.iter().map(|(_, y)| (y - my) * (y - my)).sum::<f64>() / n;
    let denom = (vx * vy).sqrt();
    Ok(K::Float(if denom == 0.0 { f64::NAN } else { cov / denom }))
}

// ----- Binary Search (`bin` / `binr`), Table Verbs, Set Ops -----------------

fn bin_one(cv: &ColVec, target: &K, right: bool) -> i64 {
    if cv.is_empty() {
        return if right { 0 } else { -1 };
    }
    if right {
        // `binr`: first index `i` where `cv[i] >= target` (`len` if none)
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
        lo as i64
    } else {
        // `bin`: last index `i` where `cv[i] <= target` (`-1` if `target < cv[0]`)
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
        (lo as i64) - 1
    }
}

fn bin_search_op(lhs: &K, rhs: &K, right: bool) -> KResult<K> {
    let K::Vector(sorted) = lhs else {
        return Err(KError::ty("bin/binr LHS must be a sorted vector"));
    };
    if let K::Vector(probes) = rhs {
        let out: Vec<i64> = (0..probes.len()).map(|i| bin_one(sorted, &probes.get_atom(i), right)).collect();
        Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(out))))
    } else {
        Ok(K::Long(bin_one(sorted, rhs, right)))
    }
}

fn extract_sym_list(k: &K) -> KResult<Vec<SymId>> {
    match k {
        K::Sym(s) => Ok(if s.is_null() { vec![] } else { vec![*s] }),
        K::Vector(ColVec::Sym(_, xs)) => Ok((**xs).clone()),
        K::Vector(ColVec::General(_, xs)) if xs.is_empty() => Ok(vec![]),
        _ => Err(KError::ty("expected symbol or symbol vector")),
    }
}

pub fn table_sort_op(lhs: &K, rhs: &K, desc: bool) -> KResult<K> {
    let sort_cols = extract_sym_list(lhs)?;
    let (flip, key_len) = match rhs {
        K::Table(t) => ((**t).clone(), None),
        K::KeyedTable(kt) => (kt.unkey(), Some(kt.keys.ncols())),
        _ => return Err(KError::ty("xasc/xdesc RHS must be a table")),
    };
    if sort_cols.is_empty() {
        return Ok(rhs.clone());
    }
    let mut col_indices = Vec::with_capacity(sort_cols.len());
    for s in &sort_cols {
        col_indices.push(flip.col_idx(*s).ok_or_else(|| KError::value(resolve(*s)))?);
    }
    let mut row_idx: Vec<usize> = (0..flip.nrows()).collect();
    row_idx.sort_by(|&a, &b| {
        for &ci in &col_indices {
            let ord = flip.vals[ci].get_atom(a).k_cmp(&flip.vals[ci].get_atom(b));
            if ord != Ordering::Equal {
                return if desc { ord.reverse() } else { ord };
            }
        }
        a.cmp(&b)
    });
    let mut sorted = flip.gather(&row_idx);
    if !desc {
        if let Some(&first_ci) = col_indices.first() {
            sorted.vals[first_ci].set_attr_unchecked(Attr::Sorted);
        }
    }
    match key_len {
        Some(nk) => KeyedTable::from_flip_nkeys(&sorted, nk),
        None => Ok(K::Table(Arc::new(sorted))),
    }
}

pub fn xkey_op(lhs: &K, rhs: &K) -> KResult<K> {
    let key_cols = extract_sym_list(lhs)?;
    let flip = match rhs {
        K::Table(t) => (**t).clone(),
        K::KeyedTable(kt) => kt.unkey(),
        _ => return Err(KError::ty("xkey RHS must be a table")),
    };
    KeyedTable::from_flip_key_names(&flip, &key_cols)
}

fn xcol_op(lhs: &K, rhs: &K) -> KResult<K> {
    let new_names = extract_sym_list(lhs)?;
    let (mut flip, key_len) = match rhs {
        K::Table(t) => ((**t).clone(), None),
        K::KeyedTable(kt) => (kt.unkey(), Some(kt.keys.ncols())),
        _ => return Err(KError::ty("xcol RHS must be a table")),
    };
    if new_names.len() > flip.cols.len() {
        return Err(KError::length("more column names given to xcol than table has"));
    }
    for (i, &n) in new_names.iter().enumerate() {
        flip.cols[i] = n;
    }
    match key_len {
        Some(nk) => KeyedTable::from_flip_nkeys(&flip, nk),
        None => Ok(K::Table(Arc::new(flip))),
    }
}

fn xcols_op(lhs: &K, rhs: &K) -> KResult<K> {
    let lead = extract_sym_list(lhs)?;
    let K::Table(t) = rhs else {
        return Err(KError::ty("xcols RHS must be an unkeyed table"));
    };
    let mut cols = Vec::with_capacity(t.ncols());
    let mut vals = Vec::with_capacity(t.ncols());
    for &s in &lead {
        let idx = t.col_idx(s).ok_or_else(|| KError::value(resolve(s)))?;
        cols.push(s);
        vals.push(t.vals[idx].clone());
    }
    for (c, v) in t.cols.iter().zip(&t.vals) {
        if !lead.contains(c) {
            cols.push(*c);
            vals.push(v.clone());
        }
    }
    Ok(K::Table(Arc::new(Flip { cols, vals })))
}

fn xgroup_op(lhs: &K, rhs: &K) -> KResult<K> {
    let key_cols = extract_sym_list(lhs)?;
    let flip = match rhs {
        K::Table(t) => (**t).clone(),
        K::KeyedTable(kt) => kt.unkey(),
        _ => return Err(KError::ty("xgroup RHS must be a table")),
    };
    let mut k_idx = Vec::with_capacity(key_cols.len());
    for &k in &key_cols {
        k_idx.push(flip.col_idx(k).ok_or_else(|| KError::value(resolve(k)))?);
    }
    let v_idx: Vec<usize> = (0..flip.ncols()).filter(|i| !k_idx.contains(i)).collect();

    let mut groups: Vec<Vec<usize>> = Vec::new();
    let mut first_rows: Vec<usize> = Vec::new();
    let mut map: HashMap<Vec<u8>, usize> = HashMap::new();
    for r in 0..flip.nrows() {
        let mut hk = Vec::new();
        for &ki in &k_idx {
            hk.extend_from_slice(&flip.vals[ki].get_atom(r).hash_key());
        }
        if let Some(&gid) = map.get(&hk) {
            groups[gid].push(r);
        } else {
            let gid = groups.len();
            map.insert(hk, gid);
            first_rows.push(r);
            groups.push(vec![r]);
        }
    }
    let keys_flip = Flip {
        cols: key_cols,
        vals: k_idx.iter().map(|&ki| flip.vals[ki].gather(&first_rows)).collect(),
    };
    let vals_flip = Flip {
        cols: v_idx.iter().map(|&vi| flip.cols[vi]).collect(),
        vals: v_idx
            .iter()
            .map(|&vi| {
                let col_lists: Vec<K> = groups.iter().map(|g| K::Vector(flip.vals[vi].gather(g))).collect();
                ColVec::General(Attr::None, Arc::new(col_lists))
            })
            .collect(),
    };
    Ok(K::KeyedTable(Arc::new(KeyedTable { keys: keys_flip, vals: vals_flip })))
}

#[derive(Clone, Copy)]
enum SetOp {
    Inter,
    Union,
    Except,
}

fn set_op(lhs: &K, rhs: &K, kind: SetOp) -> KResult<K> {
    let a = if lhs.is_atom() { enlist_op(lhs) } else { lhs.clone() };
    let b = if rhs.is_atom() { enlist_op(rhs) } else { rhs.clone() };
    let (K::Vector(va), K::Vector(vb)) = (&a, &b) else {
        return Err(KError::ty("inter/union/except expects vectors"));
    };
    match kind {
        SetOp::Union => distinct_op(&K::Vector(va.concat(vb))),
        SetOp::Inter => {
            let bset: HashSet<Vec<u8>> = (0..vb.len()).map(|i| vb.get_atom(i).hash_key()).collect();
            let mut seen = HashSet::new();
            let mut keep = Vec::new();
            for i in 0..va.len() {
                let hk = va.get_atom(i).hash_key();
                if bset.contains(&hk) && seen.insert(hk) {
                    keep.push(i);
                }
            }
            Ok(K::Vector(va.gather(&keep)))
        }
        SetOp::Except => {
            let bset: HashSet<Vec<u8>> = (0..vb.len()).map(|i| vb.get_atom(i).hash_key()).collect();
            let keep: Vec<usize> = (0..va.len()).filter(|&i| !bset.contains(&va.get_atom(i).hash_key())).collect();
            Ok(K::Vector(va.gather(&keep)))
        }
    }
}

fn cross_op(lhs: &K, rhs: &K) -> KResult<K> {
    match (lhs, rhs) {
        (K::Table(ta), K::Table(tb)) => {
            let na = ta.nrows();
            let nb = tb.nrows();
            let mut ia = Vec::with_capacity(na * nb);
            let mut ib = Vec::with_capacity(na * nb);
            for r1 in 0..na {
                for r2 in 0..nb {
                    ia.push(r1);
                    ib.push(r2);
                }
            }
            let fa = ta.gather(&ia);
            let fb = tb.gather(&ib);
            let mut cols = fa.cols;
            let mut vals = fa.vals;
            for (c, v) in fb.cols.into_iter().zip(fb.vals) {
                if let Some(pos) = cols.iter().position(|&x| x == c) {
                    vals[pos] = v;
                } else {
                    cols.push(c);
                    vals.push(v);
                }
            }
            Ok(K::Table(Arc::new(Flip { cols, vals })))
        }
        _ => {
            let a = if lhs.is_atom() { enlist_op(lhs) } else { lhs.clone() };
            let b = if rhs.is_atom() { enlist_op(rhs) } else { rhs.clone() };
            let (K::Vector(va), K::Vector(vb)) = (&a, &b) else {
                return Err(KError::ty("cross expects lists or tables"));
            };
            let mut out = Vec::with_capacity(va.len() * vb.len());
            for i in 0..va.len() {
                for j in 0..vb.len() {
                    out.push(concat_k(&va.get_atom(i), &vb.get_atom(j))?);
                }
            }
            Ok(K::Vector(ColVec::General(Attr::None, Arc::new(out))))
        }
    }
}

// ----- Indexing (`x[i; j]`) & Dictionary Lookup -----------------------------

pub fn dict_lookup(d: &Dict, key: &K) -> KResult<K> {
    if matches!(key, K::Null) {
        return Ok((*d.vals).clone());
    }
    if let K::Vector(kv) = key {
        if !matches!(kv, ColVec::Char(_, _)) {
            let mut out = Vec::with_capacity(kv.len());
            for i in 0..kv.len() {
                out.push(dict_lookup(d, &kv.get_atom(i))?);
            }
            return Ok(K::Vector(ColVec::from_atoms(out)));
        }
    }
    let K::Vector(keys_cv) = &*d.keys else {
        return Err(KError::ty("corrupt dictionary keys"));
    };
    let target_hk = key.hash_key();
    for i in 0..keys_cv.len() {
        if keys_cv.get_atom(i).hash_key() == target_hk {
            return index_k(&d.vals, &[Some(K::Long(i as i64))]);
        }
    }
    // Missing key in kdb+ returns a typed null of the value vector!
    if let K::Vector(vals_cv) = &*d.vals {
        Ok(vals_cv.null_atom())
    } else {
        Ok(K::Null)
    }
}

pub fn index_k(target: &K, args: &[Option<K>]) -> KResult<K> {
    if args.is_empty() {
        return Ok(target.clone());
    }
    let first = args[0].as_ref();
    let rest = &args[1..];

    let step = match target {
        K::Vector(cv) => match first {
            None | Some(K::Null) => K::Vector(cv.clone()),
            Some(idx_k) if idx_k.is_atom() => cv.get_signed(idx_k.as_i64()?),
            Some(K::Vector(idx_cv)) => {
                let is = idx_cv.as_i64_vec().ok_or_else(|| KError::ty("vector index must be integer"))?;
                K::Vector(cv.gather_signed(&is))
            }
            _ => return Err(KError::ty("invalid vector index")),
        },
        K::Dict(d) => match first {
            None | Some(K::Null) => (*d.vals).clone(),
            Some(k) => dict_lookup(d, k)?,
        },
        K::Table(t) => {
            // `t[row_spec]` or `t[row_spec; col_spec]` or `t[`col]`!
            if rest.is_empty() {
                match first {
                    None | Some(K::Null) => K::Table(t.clone()),
                    Some(K::Sym(col)) => t.col(*col).cloned().map(K::Vector).ok_or_else(|| KError::value(resolve(*col)))?,
                    Some(row_k) if row_k.is_atom() => {
                        let r = row_k.as_i64()?;
                        if r < 0 || (r as usize) >= t.nrows() {
                            first_op(&K::Table(Arc::new(t.gather(&[]))))
                        } else {
                            K::Dict(Arc::new(t.row_dict(r as usize)))
                        }
                    }
                    Some(K::Vector(idx_cv)) => {
                        let is = idx_cv.as_i64_vec().ok_or_else(|| KError::ty("table row index must be integer or symbol"))?;
                        let u: Vec<usize> = is.into_iter().map(|i| if i >= 0 && (i as usize) < t.nrows() { i as usize } else { usize::MAX }).collect();
                        K::Table(Arc::new(t.gather(&u)))
                    }
                    _ => return Err(KError::ty("invalid table index")),
                }
            } else {
                // 2D table indexing `t[rows; cols]`
                let sub_rows = match first {
                    None | Some(K::Null) => (**t).clone(),
                    Some(rk) if rk.is_atom() => {
                        let r = rk.as_i64()?;
                        let idx = if r >= 0 && (r as usize) < t.nrows() { r as usize } else { usize::MAX };
                        let row_d = K::Dict(Arc::new(t.gather(&[idx]).row_dict(0)));
                        return index_k(&row_d, rest);
                    }
                    Some(K::Vector(icv)) => {
                        let is = icv.as_i64_vec().ok_or_else(|| KError::ty("table row index must be integer"))?;
                        let u: Vec<usize> = is.into_iter().map(|i| if i >= 0 && (i as usize) < t.nrows() { i as usize } else { usize::MAX }).collect();
                        t.gather(&u)
                    }
                    _ => return Err(KError::ty("invalid table row index")),
                };
                return match rest[0].as_ref() {
                    None | Some(K::Null) => Ok(K::Table(Arc::new(sub_rows))),
                    Some(K::Sym(col)) => sub_rows.col(*col).cloned().map(K::Vector).ok_or_else(|| KError::value(resolve(*col))),
                    Some(K::Vector(ColVec::Sym(_, cols))) => subset_table_or_dict(cols, &K::Table(Arc::new(sub_rows))),
                    _ => Err(KError::ty("table column index must be symbol or symbol vector")),
                };
            }
        }
        K::KeyedTable(kt) => {
            // Indexing a KeyedTable looks up by key (`kt[\`AAPL]` or `kt[([] sym: \`AAPL\`MSFT)]`)!
            match first {
                None | Some(K::Null) => K::Table(Arc::new(kt.vals.clone())),
                Some(K::Table(probe_keys)) => {
                    let joined = crate::engine::joins::lookup_keyed_rows(kt, probe_keys)?;
                    K::Table(Arc::new(joined))
                }
                Some(k) => {
                    if kt.keys.ncols() == 1 {
                        if k.is_atom() {
                            let kv = &kt.keys.vals[0];
                            let hk = k.hash_key();
                            let found = (0..kv.len()).find(|&i| kv.get_atom(i).hash_key() == hk);
                            match found {
                                Some(r) => K::Dict(Arc::new(kt.vals.row_dict(r))),
                                None => first_op(&K::Table(Arc::new(kt.vals.gather(&[])))),
                            }
                        } else if let K::Vector(probe_cv) = k {
                            let probe_flip = Flip { cols: vec![kt.keys.cols[0]], vals: vec![probe_cv.clone()] };
                            K::Table(Arc::new(crate::engine::joins::lookup_keyed_rows(kt, &probe_flip)?))
                        } else {
                            return Err(KError::ty("invalid keyed table lookup"));
                        }
                    } else if let K::Vector(tuple_cv) = k {
                        if tuple_cv.len() == kt.keys.ncols() {
                            let found = (0..kt.nrows()).find(|&r| {
                                (0..kt.keys.ncols()).all(|c| kt.keys.vals[c].get_atom(r).hash_key() == tuple_cv.get_atom(c).hash_key())
                            });
                            match found {
                                Some(r) => K::Dict(Arc::new(kt.vals.row_dict(r))),
                                None => first_op(&K::Table(Arc::new(kt.vals.gather(&[])))),
                            }
                        } else {
                            return Err(KError::length("composite key lookup length mismatch"));
                        }
                    } else {
                        return Err(KError::ty("composite keyed table requires tuple or table key"));
                    }
                }
            }
        }
        _ => return Err(KError::ty(format!("cannot index value of type {}h", target.type_num()))),
    };

    if rest.is_empty() {
        Ok(step)
    } else if matches!(&step, K::Vector(v) if !matches!(v, ColVec::Char(_, _))) && first.is_none_or(|f| matches!(f, K::Null) || matches!(f, K::Vector(_))) {
        // Cross-section indexing `m[; 1]` or `m[0 1; 2]`!
        if let K::Vector(rows_cv) = &step {
            let mut out = Vec::with_capacity(rows_cv.len());
            for i in 0..rows_cv.len() {
                out.push(index_k(&rows_cv.get_atom(i), rest)?);
            }
            Ok(K::Vector(ColVec::from_atoms(out)))
        } else {
            unreachable!()
        }
    } else {
        index_k(&step, rest)
    }
}
