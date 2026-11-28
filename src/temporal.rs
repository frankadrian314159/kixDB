//! Conversion between temporal types. Two times of day, or two points on the calendar, compare and join in the finer of
//! their types.
use crate::types::*;

/// The types of one family, coarsest first: calendar (month, date, datetime, timestamp), then times of day (minute,
/// second, time, timespan).
const FAMILIES: [[i16; 4]; 2] = [[13, 14, 15, 12], [17, 18, 19, 16]];
const MINUTE: i64 = 60 * NANOS_PER_SEC;

/// A temporal atom as nanoseconds since 2000.01.01 or since midnight; `None` for its null and for other values. An
/// infinity of any type is the long one.
pub fn nanos(k: &K) -> Option<i64> {
    if k.is_null() {
        return None;
    }
    Some(match k {
        K::Month(INF_INT) | K::Date(INF_INT) | K::Minute(INF_INT) | K::Second(INF_INT) | K::Time(INF_INT) => INF_LONG,
        K::Month(NINF_INT) | K::Date(NINF_INT) | K::Minute(NINF_INT) | K::Second(NINF_INT) | K::Time(NINF_INT) => NINF_LONG,
        K::Month(m) => i64::from(date_from_ymd(2000 + m.div_euclid(12), m.rem_euclid(12) as u32 + 1, 1)?).saturating_mul(NANOS_PER_DAY),
        K::Date(d) => i64::from(*d).saturating_mul(NANOS_PER_DAY),
        K::Datetime(d) => ((d * NANOS_PER_DAY as f64).round() as i64).max(NINF_LONG),
        K::Minute(m) => i64::from(*m) * MINUTE,
        K::Second(s) => i64::from(*s) * NANOS_PER_SEC,
        K::Time(t) => i64::from(*t) * 1_000_000,
        K::Timestamp(n) | K::Timespan(n) => *n,
        _ => return None,
    })
}

/// The value of type `kind` (not month) at `ns` nanoseconds; its null for `None`.
fn at(ns: Option<i64>, kind: i16) -> K {
    let int = |unit: i64| {
        ns.map_or(NULL_INT, |n| match n {
            INF_LONG => INF_INT,
            NINF_LONG => NINF_INT,
            n => n.div_euclid(unit) as i32,
        })
    };
    let days = |n| match n {
        INF_LONG => f64::INFINITY,
        NINF_LONG => f64::NEG_INFINITY,
        n => n as f64 / NANOS_PER_DAY as f64,
    };
    match kind {
        14 => K::Date(int(NANOS_PER_DAY)),
        15 => K::Datetime(ns.map_or(f64::NAN, days)),
        17 => K::Minute(int(MINUTE)),
        18 => K::Second(int(NANOS_PER_SEC)),
        19 => K::Time(int(1_000_000)),
        16 => K::Timespan(ns.unwrap_or(NULL_LONG)),
        _ => K::Timestamp(ns.unwrap_or(NULL_LONG)),
    }
}

/// An atom or vector of one temporal type as the (finer) type `kind` of its family.
pub fn convert(k: &K, kind: i16) -> K {
    match k {
        K::Vector(v) if v.is_empty() => K::Vector(ColVec::from_atoms(vec![at(None, kind)]).take(0)),
        K::Vector(v) => K::Vector(ColVec::from_atoms((0..v.len()).map(|i| at(nanos(&v.get_atom(i)), kind)).collect())),
        atom => at(nanos(atom), kind),
    }
}

/// The finest of the temporal types `kinds`, if they all belong to one family.
pub fn finest(kinds: &[i16]) -> Option<i16> {
    let family = FAMILIES.iter().find(|f| kinds.iter().all(|k| f.contains(&k.abs())))?;
    family.iter().rev().find(|&&f| kinds.iter().any(|k| k.abs() == f)).copied()
}

/// `a` and `b` in the finer of their types, if they are different temporal types of one family.
pub fn align(a: &K, b: &K) -> Option<(K, K)> {
    let (x, y) = (a.type_num().abs(), b.type_num().abs());
    let kind = finest(&[x, y]).filter(|_| x != y)?;
    let to = |k: &K, from: i16| if from == kind { k.clone() } else { convert(k, kind) };
    Some((to(a, x), to(b, y)))
}
