//! Core `K` value model, typed columnar vectors (`ColVec`), attributes (`Attr`), tables (`Flip`, `KeyedTable`),
//! dictionaries (`Dict`), callables (`Func`), and temporal/null sentinels matching kdb+.
use crate::ast::Expr;
use crate::error::{KError, KResult};
use crate::sym::{intern, resolve, SymId};
use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

// ----- Null & infinity sentinels (matching kdb+) ----------------------------

pub const NULL_SHORT: i16 = i16::MIN;
pub const NINF_SHORT: i16 = i16::MIN + 1;
pub const INF_SHORT: i16 = i16::MAX;

pub const NULL_INT: i32 = i32::MIN;
pub const NINF_INT: i32 = i32::MIN + 1;
pub const INF_INT: i32 = i32::MAX;

pub const NULL_LONG: i64 = i64::MIN;
pub const NINF_LONG: i64 = i64::MIN + 1;
pub const INF_LONG: i64 = i64::MAX;

pub const NULL_GUID: [u8; 16] = [0u8; 16];
pub const NULL_CHAR: u8 = b' ';

/// Days between 1970-01-01 and kdb+'s epoch 2000-01-01.
pub const KDB_EPOCH_UNIX_DAYS: i64 = 10_957;
pub const NANOS_PER_SEC: i64 = 1_000_000_000;
pub const NANOS_PER_MILLI: i64 = 1_000_000;
pub const NANOS_PER_MIN: i64 = 60 * NANOS_PER_SEC;
pub const NANOS_PER_HOUR: i64 = 3600 * NANOS_PER_SEC;
pub const NANOS_PER_DAY: i64 = 86_400 * NANOS_PER_SEC;

#[inline]
pub fn is_null_f32(v: f32) -> bool {
    v.is_nan()
}

#[inline]
pub fn is_null_f64(v: f64) -> bool {
    v.is_nan()
}

// ----- Temporal conversion & formatting helpers -----------------------------

fn kdb_epoch() -> NaiveDate {
    NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()
}

pub fn date_from_ymd(year: i32, month: u32, day: u32) -> Option<i32> {
    let d = NaiveDate::from_ymd_opt(year, month, day)?;
    Some((d - kdb_epoch()).num_days() as i32)
}

pub fn ymd_from_date(days: i32) -> Option<(i32, u32, u32)> {
    if days == NULL_INT || days == INF_INT || days == NINF_INT {
        return None;
    }
    let d = kdb_epoch().checked_add_signed(chrono::Duration::days(days as i64))?;
    Some((d.year(), d.month(), d.day()))
}

pub fn format_date(days: i32) -> String {
    if days == NULL_INT {
        "0Nd".into()
    } else if days == INF_INT {
        "0Wd".into()
    } else if days == NINF_INT {
        "-0Wd".into()
    } else if let Some((y, m, d)) = ymd_from_date(days) {
        format!("{y:04}.{m:02}.{d:02}")
    } else {
        "0Nd".into()
    }
}

pub fn format_month(months: i32) -> String {
    if months == NULL_INT {
        "0Nm".into()
    } else if months == INF_INT {
        "0Wm".into()
    } else if months == NINF_INT {
        "-0Wm".into()
    } else {
        let total = 2000 * 12 + months;
        let y = total.div_euclid(12);
        let m = total.rem_euclid(12) + 1;
        format!("{y:04}.{m:02}m")
    }
}

pub fn format_minute(mins: i32) -> String {
    if mins == NULL_INT {
        "0Nu".into()
    } else if mins == INF_INT {
        "0Wu".into()
    } else if mins == NINF_INT {
        "-0Wu".into()
    } else {
        let neg = if mins < 0 { "-" } else { "" };
        let a = mins.unsigned_abs();
        format!("{neg}{:02}:{:02}", a / 60, a % 60)
    }
}

pub fn format_second(secs: i32) -> String {
    if secs == NULL_INT {
        "0Nv".into()
    } else if secs == INF_INT {
        "0Wv".into()
    } else if secs == NINF_INT {
        "-0Wv".into()
    } else {
        let neg = if secs < 0 { "-" } else { "" };
        let a = secs.unsigned_abs();
        format!("{neg}{:02}:{:02}:{:02}", a / 3600, (a / 60) % 60, a % 60)
    }
}

pub fn format_time(ms: i32) -> String {
    if ms == NULL_INT {
        "0Nt".into()
    } else if ms == INF_INT {
        "0Wt".into()
    } else if ms == NINF_INT {
        "-0Wt".into()
    } else {
        let neg = if ms < 0 { "-" } else { "" };
        let a = ms.unsigned_abs();
        let s = a / 1000;
        format!("{neg}{:02}:{:02}:{:02}.{:03}", s / 3600, (s / 60) % 60, s % 60, a % 1000)
    }
}

pub fn format_timespan(ns: i64) -> String {
    if ns == NULL_LONG {
        "0Nn".into()
    } else if ns == INF_LONG {
        "0Wn".into()
    } else if ns == NINF_LONG {
        "-0Wn".into()
    } else {
        let neg = if ns < 0 { "-" } else { "" };
        let a = ns.unsigned_abs();
        let days = a / (NANOS_PER_DAY as u64);
        let rem = a % (NANOS_PER_DAY as u64);
        let secs = rem / 1_000_000_000;
        let sub = rem % 1_000_000_000;
        format!(
            "{neg}{days}D{:02}:{:02}:{:02}.{sub:09}",
            secs / 3600,
            (secs / 60) % 60,
            secs % 60
        )
    }
}

pub fn format_timestamp(ns: i64) -> String {
    if ns == NULL_LONG {
        "0Np".into()
    } else if ns == INF_LONG {
        "0Wp".into()
    } else if ns == NINF_LONG {
        "-0Wp".into()
    } else {
        let days = ns.div_euclid(NANOS_PER_DAY) as i32;
        let rem = ns.rem_euclid(NANOS_PER_DAY) as u64;
        let secs = rem / 1_000_000_000;
        let sub = rem % 1_000_000_000;
        let date_str = format_date(days);
        format!(
            "{date_str}D{:02}:{:02}:{:02}.{sub:09}",
            secs / 3600,
            (secs / 60) % 60,
            secs % 60
        )
    }
}

pub fn format_guid(g: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        g[0], g[1], g[2], g[3], g[4], g[5], g[6], g[7], g[8], g[9], g[10], g[11], g[12], g[13], g[14], g[15]
    )
}

pub fn parse_guid(s: &str) -> Option<[u8; 16]> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for i in 0..16 {
        out[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

// ----- Column Attributes (`s#, `u#, `p#, `g#) -------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum Attr {
    #[default]
    None,
    /// `` `s# `` — sorted non-decreasing (enables binary search).
    Sorted,
    /// `` `u# `` — unique elements (hash lookup).
    Unique,
    /// `` `p# `` — parted / contiguous runs of identical values (HDB `sym`).
    Parted,
    /// `` `g# `` — grouped / dynamic inverted index.
    Grouped,
}

impl Attr {
    pub fn char(self) -> Option<char> {
        match self {
            Attr::None => None,
            Attr::Sorted => Some('s'),
            Attr::Unique => Some('u'),
            Attr::Parted => Some('p'),
            Attr::Grouped => Some('g'),
        }
    }

    pub fn prefix(self) -> &'static str {
        match self {
            Attr::None => "",
            Attr::Sorted => "`s#",
            Attr::Unique => "`u#",
            Attr::Parted => "`p#",
            Attr::Grouped => "`g#",
        }
    }

    pub fn from_sym(s: &str) -> Option<Self> {
        match s {
            "" => Some(Attr::None),
            "s" => Some(Attr::Sorted),
            "u" => Some(Attr::Unique),
            "p" => Some(Attr::Parted),
            "g" => Some(Attr::Grouped),
            _ => None,
        }
    }

    pub fn ipc_byte(self) -> u8 {
        match self {
            Attr::None => 0,
            Attr::Sorted => 1,
            Attr::Unique => 2,
            Attr::Parted => 3,
            Attr::Grouped => 4,
        }
    }

    pub fn from_ipc_byte(b: u8) -> Self {
        match b {
            1 => Attr::Sorted,
            2 => Attr::Unique,
            3 => Attr::Parted,
            4 => Attr::Grouped,
            _ => Attr::None,
        }
    }
}

// ----- Typed Column Vectors (`ColVec`) --------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ColVec {
    General(Attr, Arc<Vec<K>>),
    Bool(Attr, Arc<Vec<bool>>),
    Guid(Attr, Arc<Vec<[u8; 16]>>),
    Byte(Attr, #[serde(with = "crate::bytes")] Arc<Vec<u8>>),
    Short(Attr, Arc<Vec<i16>>),
    Int(Attr, Arc<Vec<i32>>),
    Long(Attr, Arc<Vec<i64>>),
    Real(Attr, Arc<Vec<f32>>),
    Float(Attr, Arc<Vec<f64>>),
    Char(Attr, #[serde(with = "crate::bytes")] Arc<Vec<u8>>),
    Sym(Attr, Arc<Vec<SymId>>),
    Timestamp(Attr, Arc<Vec<i64>>),
    Month(Attr, Arc<Vec<i32>>),
    Date(Attr, Arc<Vec<i32>>),
    Datetime(Attr, Arc<Vec<f64>>),
    Timespan(Attr, Arc<Vec<i64>>),
    Minute(Attr, Arc<Vec<i32>>),
    Second(Attr, Arc<Vec<i32>>),
    Time(Attr, Arc<Vec<i32>>),
}

impl PartialEq for ColVec {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ColVec::General(_, a), ColVec::General(_, b)) => a == b,
            (ColVec::Bool(_, a), ColVec::Bool(_, b)) => a == b,
            (ColVec::Guid(_, a), ColVec::Guid(_, b)) => a == b,
            (ColVec::Byte(_, a), ColVec::Byte(_, b)) => a == b,
            (ColVec::Short(_, a), ColVec::Short(_, b)) => a == b,
            (ColVec::Int(_, a), ColVec::Int(_, b)) => a == b,
            (ColVec::Long(_, a), ColVec::Long(_, b)) => a == b,
            (ColVec::Real(_, a), ColVec::Real(_, b)) => {
                a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan()))
            }
            (ColVec::Float(_, a), ColVec::Float(_, b)) => {
                a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan()))
            }
            (ColVec::Char(_, a), ColVec::Char(_, b)) => a == b,
            (ColVec::Sym(_, a), ColVec::Sym(_, b)) => a == b,
            (ColVec::Timestamp(_, a), ColVec::Timestamp(_, b)) => a == b,
            (ColVec::Month(_, a), ColVec::Month(_, b)) => a == b,
            (ColVec::Date(_, a), ColVec::Date(_, b)) => a == b,
            (ColVec::Datetime(_, a), ColVec::Datetime(_, b)) => {
                a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan()))
            }
            (ColVec::Timespan(_, a), ColVec::Timespan(_, b)) => a == b,
            (ColVec::Minute(_, a), ColVec::Minute(_, b)) => a == b,
            (ColVec::Second(_, a), ColVec::Second(_, b)) => a == b,
            (ColVec::Time(_, a), ColVec::Time(_, b)) => a == b,
            _ => false,
        }
    }
}

impl ColVec {
    pub fn len(&self) -> usize {
        match self {
            ColVec::General(_, v) => v.len(),
            ColVec::Bool(_, v) => v.len(),
            ColVec::Guid(_, v) => v.len(),
            ColVec::Byte(_, v) => v.len(),
            ColVec::Short(_, v) => v.len(),
            ColVec::Int(_, v) => v.len(),
            ColVec::Long(_, v) => v.len(),
            ColVec::Real(_, v) => v.len(),
            ColVec::Float(_, v) => v.len(),
            ColVec::Char(_, v) => v.len(),
            ColVec::Sym(_, v) => v.len(),
            ColVec::Timestamp(_, v) => v.len(),
            ColVec::Month(_, v) => v.len(),
            ColVec::Date(_, v) => v.len(),
            ColVec::Datetime(_, v) => v.len(),
            ColVec::Timespan(_, v) => v.len(),
            ColVec::Minute(_, v) => v.len(),
            ColVec::Second(_, v) => v.len(),
            ColVec::Time(_, v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn attr(&self) -> Attr {
        match self {
            ColVec::General(a, _)
            | ColVec::Bool(a, _)
            | ColVec::Guid(a, _)
            | ColVec::Byte(a, _)
            | ColVec::Short(a, _)
            | ColVec::Int(a, _)
            | ColVec::Long(a, _)
            | ColVec::Real(a, _)
            | ColVec::Float(a, _)
            | ColVec::Char(a, _)
            | ColVec::Sym(a, _)
            | ColVec::Timestamp(a, _)
            | ColVec::Month(a, _)
            | ColVec::Date(a, _)
            | ColVec::Datetime(a, _)
            | ColVec::Timespan(a, _)
            | ColVec::Minute(a, _)
            | ColVec::Second(a, _)
            | ColVec::Time(a, _) => *a,
        }
    }

    pub fn set_attr_unchecked(&mut self, na: Attr) {
        match self {
            ColVec::General(a, _)
            | ColVec::Bool(a, _)
            | ColVec::Guid(a, _)
            | ColVec::Byte(a, _)
            | ColVec::Short(a, _)
            | ColVec::Int(a, _)
            | ColVec::Long(a, _)
            | ColVec::Real(a, _)
            | ColVec::Float(a, _)
            | ColVec::Char(a, _)
            | ColVec::Sym(a, _)
            | ColVec::Timestamp(a, _)
            | ColVec::Month(a, _)
            | ColVec::Date(a, _)
            | ColVec::Datetime(a, _)
            | ColVec::Timespan(a, _)
            | ColVec::Minute(a, _)
            | ColVec::Second(a, _)
            | ColVec::Time(a, _) => *a = na,
        }
    }

    /// Apply and validate a kdb+ attribute (`` `s# ``, `` `u# ``, `` `p# ``, `` `g# ``).
    pub fn with_attr(&self, na: Attr) -> KResult<ColVec> {
        match na {
            Attr::None | Attr::Grouped => {}
            Attr::Sorted => {
                for i in 1..self.len() {
                    if self.get_atom(i - 1).k_cmp(&self.get_atom(i)) == std::cmp::Ordering::Greater {
                        return Err(KError::new("s-fail", "vector is not sorted ascending"));
                    }
                }
            }
            Attr::Unique => {
                let mut seen = HashSet::with_capacity(self.len());
                for i in 0..self.len() {
                    let key = self.get_atom(i).hash_key();
                    if !seen.insert(key) {
                        return Err(KError::new("u-fail", "vector contains duplicate elements"));
                    }
                }
            }
            Attr::Parted => {
                let mut seen = HashSet::new();
                let mut prev: Option<Vec<u8>> = None;
                for i in 0..self.len() {
                    let key = self.get_atom(i).hash_key();
                    if prev.as_ref() != Some(&key) {
                        if !seen.insert(key.clone()) {
                            return Err(KError::new("u-fail", "vector values are not contiguous for `p#"));
                        }
                        prev = Some(key);
                    }
                }
            }
        }
        let mut out = self.clone();
        out.set_attr_unchecked(na);
        Ok(out)
    }

    pub fn type_num(&self) -> i16 {
        match self {
            ColVec::General(_, _) => 0,
            ColVec::Bool(_, _) => 1,
            ColVec::Guid(_, _) => 2,
            ColVec::Byte(_, _) => 4,
            ColVec::Short(_, _) => 5,
            ColVec::Int(_, _) => 6,
            ColVec::Long(_, _) => 7,
            ColVec::Real(_, _) => 8,
            ColVec::Float(_, _) => 9,
            ColVec::Char(_, _) => 10,
            ColVec::Sym(_, _) => 11,
            ColVec::Timestamp(_, _) => 12,
            ColVec::Month(_, _) => 13,
            ColVec::Date(_, _) => 14,
            ColVec::Datetime(_, _) => 15,
            ColVec::Timespan(_, _) => 16,
            ColVec::Minute(_, _) => 17,
            ColVec::Second(_, _) => 18,
            ColVec::Time(_, _) => 19,
        }
    }

    /// Character shown in `meta t` (`c` column). For general lists of strings `("ab";"cd")`, returns `'C'`.
    pub fn meta_type_char(&self) -> char {
        match self {
            ColVec::General(_, v) => {
                if !v.is_empty() && v.iter().all(|x| matches!(x, K::Vector(ColVec::Char(_, _)))) {
                    'C'
                } else {
                    ' '
                }
            }
            ColVec::Bool(_, _) => 'b',
            ColVec::Guid(_, _) => 'g',
            ColVec::Byte(_, _) => 'x',
            ColVec::Short(_, _) => 'h',
            ColVec::Int(_, _) => 'i',
            ColVec::Long(_, _) => 'j',
            ColVec::Real(_, _) => 'e',
            ColVec::Float(_, _) => 'f',
            ColVec::Char(_, _) => 'c',
            ColVec::Sym(_, _) => 's',
            ColVec::Timestamp(_, _) => 'p',
            ColVec::Month(_, _) => 'm',
            ColVec::Date(_, _) => 'd',
            ColVec::Datetime(_, _) => 'z',
            ColVec::Timespan(_, _) => 'n',
            ColVec::Minute(_, _) => 'u',
            ColVec::Second(_, _) => 'v',
            ColVec::Time(_, _) => 't',
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            ColVec::General(_, v) if !v.is_empty() && v.iter().all(|x| matches!(x, K::Vector(ColVec::Char(_, _)))) => "string",
            ColVec::General(_, _) => "list",
            ColVec::Bool(_, _) => "boolean",
            ColVec::Guid(_, _) => "guid",
            ColVec::Byte(_, _) => "byte",
            ColVec::Short(_, _) => "short",
            ColVec::Int(_, _) => "int",
            ColVec::Long(_, _) => "long",
            ColVec::Real(_, _) => "real",
            ColVec::Float(_, _) => "float",
            ColVec::Char(_, _) => "char",
            ColVec::Sym(_, _) => "symbol",
            ColVec::Timestamp(_, _) => "timestamp",
            ColVec::Month(_, _) => "month",
            ColVec::Date(_, _) => "date",
            ColVec::Datetime(_, _) => "datetime",
            ColVec::Timespan(_, _) => "timespan",
            ColVec::Minute(_, _) => "minute",
            ColVec::Second(_, _) => "second",
            ColVec::Time(_, _) => "time",
        }
    }

    pub fn null_atom(&self) -> K {
        match self {
            ColVec::General(_, _) => K::Null,
            ColVec::Bool(_, _) => K::Bool(false),
            ColVec::Guid(_, _) => K::Guid(NULL_GUID),
            ColVec::Byte(_, _) => K::Byte(0),
            ColVec::Short(_, _) => K::Short(NULL_SHORT),
            ColVec::Int(_, _) => K::Int(NULL_INT),
            ColVec::Long(_, _) => K::Long(NULL_LONG),
            ColVec::Real(_, _) => K::Real(f32::NAN),
            ColVec::Float(_, _) => K::Float(f64::NAN),
            ColVec::Char(_, _) => K::Char(NULL_CHAR),
            ColVec::Sym(_, _) => K::Sym(SymId::EMPTY),
            ColVec::Timestamp(_, _) => K::Timestamp(NULL_LONG),
            ColVec::Month(_, _) => K::Month(NULL_INT),
            ColVec::Date(_, _) => K::Date(NULL_INT),
            ColVec::Datetime(_, _) => K::Datetime(f64::NAN),
            ColVec::Timespan(_, _) => K::Timespan(NULL_LONG),
            ColVec::Minute(_, _) => K::Minute(NULL_INT),
            ColVec::Second(_, _) => K::Second(NULL_INT),
            ColVec::Time(_, _) => K::Time(NULL_INT),
        }
    }

    pub fn empty_of_type_char(ch: char) -> Option<ColVec> {
        Some(match ch {
            ' ' | '*' => ColVec::General(Attr::None, Arc::new(vec![])),
            'b' => ColVec::Bool(Attr::None, Arc::new(vec![])),
            'g' => ColVec::Guid(Attr::None, Arc::new(vec![])),
            'x' => ColVec::Byte(Attr::None, Arc::new(vec![])),
            'h' => ColVec::Short(Attr::None, Arc::new(vec![])),
            'i' => ColVec::Int(Attr::None, Arc::new(vec![])),
            'j' => ColVec::Long(Attr::None, Arc::new(vec![])),
            'e' => ColVec::Real(Attr::None, Arc::new(vec![])),
            'f' => ColVec::Float(Attr::None, Arc::new(vec![])),
            'c' => ColVec::Char(Attr::None, Arc::new(vec![])),
            's' => ColVec::Sym(Attr::None, Arc::new(vec![])),
            'p' => ColVec::Timestamp(Attr::None, Arc::new(vec![])),
            'm' => ColVec::Month(Attr::None, Arc::new(vec![])),
            'd' => ColVec::Date(Attr::None, Arc::new(vec![])),
            'z' => ColVec::Datetime(Attr::None, Arc::new(vec![])),
            'n' => ColVec::Timespan(Attr::None, Arc::new(vec![])),
            'u' => ColVec::Minute(Attr::None, Arc::new(vec![])),
            'v' => ColVec::Second(Attr::None, Arc::new(vec![])),
            't' => ColVec::Time(Attr::None, Arc::new(vec![])),
            _ => return None,
        })
    }

    pub fn get_atom(&self, idx: usize) -> K {
        if idx >= self.len() {
            return self.null_atom();
        }
        match self {
            ColVec::General(_, v) => v[idx].clone(),
            ColVec::Bool(_, v) => K::Bool(v[idx]),
            ColVec::Guid(_, v) => K::Guid(v[idx]),
            ColVec::Byte(_, v) => K::Byte(v[idx]),
            ColVec::Short(_, v) => K::Short(v[idx]),
            ColVec::Int(_, v) => K::Int(v[idx]),
            ColVec::Long(_, v) => K::Long(v[idx]),
            ColVec::Real(_, v) => K::Real(v[idx]),
            ColVec::Float(_, v) => K::Float(v[idx]),
            ColVec::Char(_, v) => K::Char(v[idx]),
            ColVec::Sym(_, v) => K::Sym(v[idx]),
            ColVec::Timestamp(_, v) => K::Timestamp(v[idx]),
            ColVec::Month(_, v) => K::Month(v[idx]),
            ColVec::Date(_, v) => K::Date(v[idx]),
            ColVec::Datetime(_, v) => K::Datetime(v[idx]),
            ColVec::Timespan(_, v) => K::Timespan(v[idx]),
            ColVec::Minute(_, v) => K::Minute(v[idx]),
            ColVec::Second(_, v) => K::Second(v[idx]),
            ColVec::Time(_, v) => K::Time(v[idx]),
        }
    }

    /// Index by signed integer (out of bounds yields typed null, as in kdb+ `v[999]`).
    pub fn get_signed(&self, idx: i64) -> K {
        if idx < 0 || (idx as usize) >= self.len() {
            // past the end of a list of mixed values: a null like its first item
            if let ColVec::General(_, items) = self {
                return match items.first() {
                    Some(K::Vector(v)) => K::Vector(v.take(0)),
                    Some(atom) => ColVec::from_atoms(vec![atom.clone()]).null_atom(),
                    None => self.null_atom(),
                };
            }
            self.null_atom()
        } else {
            self.get_atom(idx as usize)
        }
    }

    pub fn gather(&self, indices: &[usize]) -> ColVec {
        macro_rules! pick {
            ($variant:ident, $v:expr, $null:expr) => {{
                let out = crate::engine::par::map(indices, |&i| if i < $v.len() { $v[i].clone() } else { $null });
                ColVec::$variant(Attr::None, Arc::new(out))
            }};
        }
        match self {
            // (a missing item of a list of lists is an empty list like its first)
            ColVec::General(_, v) => pick!(General, v, match v.first() {
                Some(K::Vector(first)) => K::Vector(first.take(0)),
                _ => K::Null,
            }),
            ColVec::Bool(_, v) => pick!(Bool, v, false),
            ColVec::Guid(_, v) => pick!(Guid, v, NULL_GUID),
            ColVec::Byte(_, v) => pick!(Byte, v, 0),
            ColVec::Short(_, v) => pick!(Short, v, NULL_SHORT),
            ColVec::Int(_, v) => pick!(Int, v, NULL_INT),
            ColVec::Long(_, v) => pick!(Long, v, NULL_LONG),
            ColVec::Real(_, v) => pick!(Real, v, f32::NAN),
            ColVec::Float(_, v) => pick!(Float, v, f64::NAN),
            ColVec::Char(_, v) => pick!(Char, v, NULL_CHAR),
            ColVec::Sym(_, v) => pick!(Sym, v, SymId::EMPTY),
            ColVec::Timestamp(_, v) => pick!(Timestamp, v, NULL_LONG),
            ColVec::Month(_, v) => pick!(Month, v, NULL_INT),
            ColVec::Date(_, v) => pick!(Date, v, NULL_INT),
            ColVec::Datetime(_, v) => pick!(Datetime, v, f64::NAN),
            ColVec::Timespan(_, v) => pick!(Timespan, v, NULL_LONG),
            ColVec::Minute(_, v) => pick!(Minute, v, NULL_INT),
            ColVec::Second(_, v) => pick!(Second, v, NULL_INT),
            ColVec::Time(_, v) => pick!(Time, v, NULL_INT),
        }
    }

    pub fn gather_signed(&self, indices: &[i64]) -> ColVec {
        macro_rules! pick {
            ($variant:ident, $v:expr, $null:expr) => {{
                let mut out = Vec::with_capacity(indices.len());
                for &i in indices {
                    if i >= 0 && (i as usize) < $v.len() {
                        out.push($v[i as usize].clone());
                    } else {
                        out.push($null);
                    }
                }
                ColVec::$variant(Attr::None, Arc::new(out))
            }};
        }
        match self {
            // (a missing item of a list of lists is an empty list like its first)
            ColVec::General(_, v) => pick!(General, v, match v.first() {
                Some(K::Vector(first)) => K::Vector(first.take(0)),
                _ => K::Null,
            }),
            ColVec::Bool(_, v) => pick!(Bool, v, false),
            ColVec::Guid(_, v) => pick!(Guid, v, NULL_GUID),
            ColVec::Byte(_, v) => pick!(Byte, v, 0),
            ColVec::Short(_, v) => pick!(Short, v, NULL_SHORT),
            ColVec::Int(_, v) => pick!(Int, v, NULL_INT),
            ColVec::Long(_, v) => pick!(Long, v, NULL_LONG),
            ColVec::Real(_, v) => pick!(Real, v, f32::NAN),
            ColVec::Float(_, v) => pick!(Float, v, f64::NAN),
            ColVec::Char(_, v) => pick!(Char, v, NULL_CHAR),
            ColVec::Sym(_, v) => pick!(Sym, v, SymId::EMPTY),
            ColVec::Timestamp(_, v) => pick!(Timestamp, v, NULL_LONG),
            ColVec::Month(_, v) => pick!(Month, v, NULL_INT),
            ColVec::Date(_, v) => pick!(Date, v, NULL_INT),
            ColVec::Datetime(_, v) => pick!(Datetime, v, f64::NAN),
            ColVec::Timespan(_, v) => pick!(Timespan, v, NULL_LONG),
            ColVec::Minute(_, v) => pick!(Minute, v, NULL_INT),
            ColVec::Second(_, v) => pick!(Second, v, NULL_INT),
            ColVec::Time(_, v) => pick!(Time, v, NULL_INT),
        }
    }

    pub fn filter_mask(&self, mask: &[bool]) -> ColVec {
        let idx: Vec<usize> = mask.iter().enumerate().filter_map(|(i, &b)| if b { Some(i) } else { None }).collect();
        let mut out = self.gather(&idx);
        if self.attr() == Attr::Sorted {
            out.set_attr_unchecked(Attr::Sorted);
        }
        out
    }

    pub fn take(&self, n: i64) -> ColVec {
        let len = self.len();
        if n == 0 {
            return self.gather(&[]);
        }
        let un = n.unsigned_abs() as usize;
        if len == 0 {
            // kdb+: taking from an empty vector fills with typed nulls
            return self.gather(&vec![usize::MAX; un]);
        }
        let mut idx = Vec::with_capacity(un);
        if n > 0 {
            for i in 0..un {
                idx.push(i % len);
            }
        } else {
            // negative take wraps from the tail
            let start = (len - (un % len)) % len;
            for i in 0..un {
                idx.push((start + i) % len);
            }
        }
        self.gather(&idx)
    }

    pub fn drop_n(&self, n: i64) -> ColVec {
        let len = self.len();
        if n == 0 {
            return self.clone();
        }
        let un = n.unsigned_abs() as usize;
        if un >= len {
            return self.gather(&[]);
        }
        let range: Vec<usize> = if n > 0 { (un..len).collect() } else { (0..len - un).collect() };
        let mut out = self.gather(&range);
        if self.attr() == Attr::Sorted {
            out.set_attr_unchecked(Attr::Sorted);
        }
        out
    }

    pub fn reverse(&self) -> ColVec {
        let idx: Vec<usize> = (0..self.len()).rev().collect();
        self.gather(&idx)
    }

    /// `n` copies of an atom as a vector (of the atom's type even when `n` is 0).
    pub fn repeat(atom: K, n: usize) -> ColVec {
        if n == 0 {
            return ColVec::from_atoms(vec![atom]).take(0);
        }
        ColVec::from_atoms(vec![atom; n])
    }

    /// Normalize a general list of uniform atoms into a typed vector (kdb+'s auto-collapse rule).
    pub fn from_atoms(items: Vec<K>) -> ColVec {
        if items.is_empty() {
            return ColVec::General(Attr::None, Arc::new(items));
        }
        let t0 = items[0].type_num();
        if t0 < 0 && items.iter().all(|x| x.type_num() == t0) {
            match t0 {
                -1 => return ColVec::Bool(Attr::None, Arc::new(items.into_iter().map(|k| if let K::Bool(v) = k { v } else { unreachable!() }).collect())),
                -2 => return ColVec::Guid(Attr::None, Arc::new(items.into_iter().map(|k| if let K::Guid(v) = k { v } else { unreachable!() }).collect())),
                -4 => return ColVec::Byte(Attr::None, Arc::new(items.into_iter().map(|k| if let K::Byte(v) = k { v } else { unreachable!() }).collect())),
                -5 => return ColVec::Short(Attr::None, Arc::new(items.into_iter().map(|k| if let K::Short(v) = k { v } else { unreachable!() }).collect())),
                -6 => return ColVec::Int(Attr::None, Arc::new(items.into_iter().map(|k| if let K::Int(v) = k { v } else { unreachable!() }).collect())),
                -7 => return ColVec::Long(Attr::None, Arc::new(items.into_iter().map(|k| if let K::Long(v) = k { v } else { unreachable!() }).collect())),
                -8 => return ColVec::Real(Attr::None, Arc::new(items.into_iter().map(|k| if let K::Real(v) = k { v } else { unreachable!() }).collect())),
                -9 => return ColVec::Float(Attr::None, Arc::new(items.into_iter().map(|k| if let K::Float(v) = k { v } else { unreachable!() }).collect())),
                -10 => return ColVec::Char(Attr::None, Arc::new(items.into_iter().map(|k| if let K::Char(v) = k { v } else { unreachable!() }).collect())),
                -11 => return ColVec::Sym(Attr::None, Arc::new(items.into_iter().map(|k| if let K::Sym(v) = k { v } else { unreachable!() }).collect())),
                -12 => return ColVec::Timestamp(Attr::None, Arc::new(items.into_iter().map(|k| if let K::Timestamp(v) = k { v } else { unreachable!() }).collect())),
                -13 => return ColVec::Month(Attr::None, Arc::new(items.into_iter().map(|k| if let K::Month(v) = k { v } else { unreachable!() }).collect())),
                -14 => return ColVec::Date(Attr::None, Arc::new(items.into_iter().map(|k| if let K::Date(v) = k { v } else { unreachable!() }).collect())),
                -15 => return ColVec::Datetime(Attr::None, Arc::new(items.into_iter().map(|k| if let K::Datetime(v) = k { v } else { unreachable!() }).collect())),
                -16 => return ColVec::Timespan(Attr::None, Arc::new(items.into_iter().map(|k| if let K::Timespan(v) = k { v } else { unreachable!() }).collect())),
                -17 => return ColVec::Minute(Attr::None, Arc::new(items.into_iter().map(|k| if let K::Minute(v) = k { v } else { unreachable!() }).collect())),
                -18 => return ColVec::Second(Attr::None, Arc::new(items.into_iter().map(|k| if let K::Second(v) = k { v } else { unreachable!() }).collect())),
                -19 => return ColVec::Time(Attr::None, Arc::new(items.into_iter().map(|k| if let K::Time(v) = k { v } else { unreachable!() }).collect())),
                _ => {}
            }
        }
        ColVec::General(Attr::None, Arc::new(items))
    }

    /// Concatenate two vectors; uniform types stay typed, mismatched types widen to a general list (matching `,` in kdb+).
    pub fn concat(&self, other: &ColVec) -> ColVec {
        macro_rules! cat {
            ($variant:ident, $a:expr, $b:expr) => {{
                let mut out = Vec::with_capacity($a.len() + $b.len());
                out.extend_from_slice($a);
                out.extend_from_slice($b);
                ColVec::$variant(Attr::None, Arc::new(out))
            }};
        }
        match (self, other) {
            (ColVec::Bool(_, a), ColVec::Bool(_, b)) => cat!(Bool, a, b),
            (ColVec::Guid(_, a), ColVec::Guid(_, b)) => cat!(Guid, a, b),
            (ColVec::Byte(_, a), ColVec::Byte(_, b)) => cat!(Byte, a, b),
            (ColVec::Short(_, a), ColVec::Short(_, b)) => cat!(Short, a, b),
            (ColVec::Int(_, a), ColVec::Int(_, b)) => cat!(Int, a, b),
            (ColVec::Long(_, a), ColVec::Long(_, b)) => cat!(Long, a, b),
            (ColVec::Real(_, a), ColVec::Real(_, b)) => cat!(Real, a, b),
            (ColVec::Float(_, a), ColVec::Float(_, b)) => cat!(Float, a, b),
            (ColVec::Char(_, a), ColVec::Char(_, b)) => cat!(Char, a, b),
            (ColVec::Sym(_, a), ColVec::Sym(_, b)) => cat!(Sym, a, b),
            (ColVec::Timestamp(_, a), ColVec::Timestamp(_, b)) => cat!(Timestamp, a, b),
            (ColVec::Month(_, a), ColVec::Month(_, b)) => cat!(Month, a, b),
            (ColVec::Date(_, a), ColVec::Date(_, b)) => cat!(Date, a, b),
            (ColVec::Datetime(_, a), ColVec::Datetime(_, b)) => cat!(Datetime, a, b),
            (ColVec::Timespan(_, a), ColVec::Timespan(_, b)) => cat!(Timespan, a, b),
            (ColVec::Minute(_, a), ColVec::Minute(_, b)) => cat!(Minute, a, b),
            (ColVec::Second(_, a), ColVec::Second(_, b)) => cat!(Second, a, b),
            (ColVec::Time(_, a), ColVec::Time(_, b)) => cat!(Time, a, b),
            _ if self.is_empty() && matches!(self, ColVec::General(_, _)) => other.clone(),
            _ if other.is_empty() && matches!(other, ColVec::General(_, _)) => self.clone(),
            _ => {
                let mut items = Vec::with_capacity(self.len() + other.len());
                for i in 0..self.len() {
                    items.push(self.get_atom(i));
                }
                for i in 0..other.len() {
                    items.push(other.get_atom(i));
                }
                ColVec::from_atoms(items)
            }
        }
    }

    /// Strict typed append for table `insert`/`upsert` (coerces int->long/float where safe, errors on incompatible types).
    pub fn append_strict(&self, other: &ColVec) -> KResult<ColVec> {
        if self.type_num() == other.type_num() || matches!(self, ColVec::General(_, _)) {
            if matches!(self, ColVec::General(_, _)) && self.is_empty() && !matches!(other, ColVec::General(_, _)) {
                return Ok(other.clone());
            }
            return Ok(self.concat(other));
        }
        // Empty typed target vs empty general source
        if other.is_empty() {
            return Ok(self.clone());
        }
        // Safe numeric widening into float or long
        match (self, other) {
            (ColVec::Float(_, a), _) if other.is_numeric() => {
                let mut out = (**a).clone();
                for i in 0..other.len() {
                    out.push(other.get_atom(i).as_f64().unwrap_or(f64::NAN));
                }
                Ok(ColVec::Float(Attr::None, Arc::new(out)))
            }
            (ColVec::Long(_, a), ColVec::Int(_, b)) => {
                let mut out = (**a).clone();
                out.extend(b.iter().map(|&v| if v == NULL_INT { NULL_LONG } else { v as i64 }));
                Ok(ColVec::Long(Attr::None, Arc::new(out)))
            }
            (ColVec::Long(_, a), ColVec::Short(_, b)) => {
                let mut out = (**a).clone();
                out.extend(b.iter().map(|&v| if v == NULL_SHORT { NULL_LONG } else { v as i64 }));
                Ok(ColVec::Long(Attr::None, Arc::new(out)))
            }
            _ => Err(KError::ty(format!(
                "cannot append {} column into {} column",
                other.type_name(),
                self.type_name()
            ))),
        }
    }

    pub fn set_at(&mut self, idx: usize, val: K) -> KResult<()> {
        if idx >= self.len() {
            return Err(KError::domain("index out of bounds"));
        }
        self.set_attr_unchecked(Attr::None);
        match (self, val) {
            (ColVec::General(_, v), x) => Arc::make_mut(v)[idx] = x,
            (ColVec::Bool(_, v), K::Bool(b)) => Arc::make_mut(v)[idx] = b,
            (ColVec::Guid(_, v), K::Guid(x)) => Arc::make_mut(v)[idx] = x,
            (ColVec::Byte(_, v), K::Byte(x)) => Arc::make_mut(v)[idx] = x,
            (ColVec::Short(_, v), K::Short(x)) => Arc::make_mut(v)[idx] = x,
            (ColVec::Int(_, v), K::Int(x)) => Arc::make_mut(v)[idx] = x,
            (ColVec::Long(_, v), K::Long(x)) => Arc::make_mut(v)[idx] = x,
            (ColVec::Long(_, v), K::Int(x)) => Arc::make_mut(v)[idx] = if x == NULL_INT { NULL_LONG } else { x as i64 },
            (ColVec::Real(_, v), K::Real(x)) => Arc::make_mut(v)[idx] = x,
            (ColVec::Float(_, v), K::Float(x)) => Arc::make_mut(v)[idx] = x,
            (ColVec::Float(_, v), K::Long(x)) => Arc::make_mut(v)[idx] = if x == NULL_LONG { f64::NAN } else { x as f64 },
            (ColVec::Float(_, v), K::Int(x)) => Arc::make_mut(v)[idx] = if x == NULL_INT { f64::NAN } else { x as f64 },
            (ColVec::Char(_, v), K::Char(x)) => Arc::make_mut(v)[idx] = x,
            (ColVec::Sym(_, v), K::Sym(x)) => Arc::make_mut(v)[idx] = x,
            (ColVec::Timestamp(_, v), K::Timestamp(x)) => Arc::make_mut(v)[idx] = x,
            (ColVec::Month(_, v), K::Month(x)) => Arc::make_mut(v)[idx] = x,
            (ColVec::Date(_, v), K::Date(x)) => Arc::make_mut(v)[idx] = x,
            (ColVec::Datetime(_, v), K::Datetime(x)) => Arc::make_mut(v)[idx] = x,
            (ColVec::Timespan(_, v), K::Timespan(x)) => Arc::make_mut(v)[idx] = x,
            (ColVec::Minute(_, v), K::Minute(x)) => Arc::make_mut(v)[idx] = x,
            (ColVec::Second(_, v), K::Second(x)) => Arc::make_mut(v)[idx] = x,
            (ColVec::Time(_, v), K::Time(x)) => Arc::make_mut(v)[idx] = x,
            _ => return Err(KError::ty("incompatible element type for vector assignment")),
        }
        Ok(())
    }

    /// Each element of a fixed-width vector as 64 bits, two being equal exactly when the elements' `hash_key`s are
    /// (so all NaNs are one value, as are both zeros).
    pub fn key_bits(&self) -> Option<Vec<u64>> {
        fn float(v: f64) -> u64 {
            if v.is_nan() { f64::NAN.to_bits() } else if v == 0.0 { 0 } else { v.to_bits() }
        }
        use crate::engine::par::map as par;
        Some(match self {
            ColVec::Bool(_, v) => par(v, |&x| u64::from(x)),
            ColVec::Byte(_, v) | ColVec::Char(_, v) => par(v, |&x| u64::from(x)),
            ColVec::Short(_, v) => par(v, |&x| x as u64),
            ColVec::Int(_, v)
            | ColVec::Month(_, v)
            | ColVec::Date(_, v)
            | ColVec::Minute(_, v)
            | ColVec::Second(_, v)
            | ColVec::Time(_, v) => par(v, |&x| x as u64),
            ColVec::Long(_, v) | ColVec::Timestamp(_, v) | ColVec::Timespan(_, v) => par(v, |&x| x as u64),
            ColVec::Real(_, v) => par(v, |&x| float(f64::from(x))),
            ColVec::Float(_, v) | ColVec::Datetime(_, v) => par(v, |&x| float(x)),
            ColVec::Sym(_, v) => par(v, |&x| u64::from(x.0)),
            ColVec::General(..) | ColVec::Guid(..) => return None,
        })
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            ColVec::Bool(_, _)
                | ColVec::Byte(_, _)
                | ColVec::Short(_, _)
                | ColVec::Int(_, _)
                | ColVec::Long(_, _)
                | ColVec::Real(_, _)
                | ColVec::Float(_, _)
        )
    }

    pub fn as_f64_vec(&self) -> Option<Vec<f64>> {
        match self {
            ColVec::Bool(_, v) => Some(v.iter().map(|&b| if b { 1.0 } else { 0.0 }).collect()),
            ColVec::Byte(_, v) => Some(v.iter().map(|&x| x as f64).collect()),
            ColVec::Short(_, v) => Some(v.iter().map(|&x| if x == NULL_SHORT { f64::NAN } else { x as f64 }).collect()),
            ColVec::Int(_, v)
            | ColVec::Month(_, v)
            | ColVec::Date(_, v)
            | ColVec::Minute(_, v)
            | ColVec::Second(_, v)
            | ColVec::Time(_, v) => Some(v.iter().map(|&x| if x == NULL_INT { f64::NAN } else { x as f64 }).collect()),
            ColVec::Long(_, v) | ColVec::Timestamp(_, v) | ColVec::Timespan(_, v) => {
                Some(v.iter().map(|&x| if x == NULL_LONG { f64::NAN } else { x as f64 }).collect())
            }
            ColVec::Real(_, v) => Some(v.iter().map(|&x| x as f64).collect()),
            ColVec::Float(_, v) | ColVec::Datetime(_, v) => Some((**v).clone()),
            _ => None,
        }
    }

    pub fn as_i64_vec(&self) -> Option<Vec<i64>> {
        match self {
            ColVec::Bool(_, v) => Some(v.iter().map(|&b| i64::from(b)).collect()),
            ColVec::Byte(_, v) => Some(v.iter().map(|&x| x as i64).collect()),
            ColVec::Short(_, v) => Some(v.iter().map(|&x| if x == NULL_SHORT { NULL_LONG } else { x as i64 }).collect()),
            ColVec::Int(_, v)
            | ColVec::Month(_, v)
            | ColVec::Date(_, v)
            | ColVec::Minute(_, v)
            | ColVec::Second(_, v)
            | ColVec::Time(_, v) => Some(v.iter().map(|&x| if x == NULL_INT { NULL_LONG } else { x as i64 }).collect()),
            ColVec::Long(_, v) | ColVec::Timestamp(_, v) | ColVec::Timespan(_, v) => Some((**v).clone()),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<String> {
        if let ColVec::Char(_, bytes) = self {
            Some(String::from_utf8_lossy(bytes).into_owned())
        } else {
            None
        }
    }

    /// Approximate heap memory bytes occupied by this vector (for `\w` stats).
    pub fn approx_bytes(&self) -> usize {
        let n = self.len();
        match self {
            ColVec::General(_, v) => v.iter().map(|k| k.approx_bytes()).sum::<usize>() + n * std::mem::size_of::<K>(),
            ColVec::Bool(_, _) | ColVec::Byte(_, _) | ColVec::Char(_, _) => n,
            ColVec::Short(_, _) => n * 2,
            ColVec::Int(_, _)
            | ColVec::Real(_, _)
            | ColVec::Sym(_, _)
            | ColVec::Month(_, _)
            | ColVec::Date(_, _)
            | ColVec::Minute(_, _)
            | ColVec::Second(_, _)
            | ColVec::Time(_, _) => n * 4,
            ColVec::Long(_, _)
            | ColVec::Float(_, _)
            | ColVec::Timestamp(_, _)
            | ColVec::Datetime(_, _)
            | ColVec::Timespan(_, _) => n * 8,
            ColVec::Guid(_, _) => n * 16,
        }
    }
}

// ----- Tables (`Flip` and `KeyedTable`) and `Dict` --------------------------

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Flip {
    pub cols: Vec<SymId>,
    pub vals: Vec<ColVec>,
}

impl Flip {
    pub fn new(cols: Vec<SymId>, vals: Vec<ColVec>) -> KResult<Self> {
        if cols.len() != vals.len() {
            return Err(KError::length("table column names and vectors differ in count"));
        }
        let nrows = vals.first().map(|v| v.len()).unwrap_or(0);
        for v in &vals {
            if v.len() != nrows {
                return Err(KError::length("table columns must have equal length"));
            }
        }
        Ok(Self { cols, vals })
    }

    pub fn nrows(&self) -> usize {
        self.vals.first().map(|v| v.len()).unwrap_or(0)
    }

    pub fn ncols(&self) -> usize {
        self.cols.len()
    }

    pub fn col_idx(&self, name: SymId) -> Option<usize> {
        self.cols.iter().position(|&c| c == name)
    }

    pub fn col(&self, name: SymId) -> Option<&ColVec> {
        self.col_idx(name).map(|i| &self.vals[i])
    }

    pub fn gather(&self, indices: &[usize]) -> Flip {
        Flip {
            cols: self.cols.clone(),
            vals: self.vals.iter().map(|v| v.gather(indices)).collect(),
        }
    }

    pub fn filter_mask(&self, mask: &[bool]) -> Flip {
        Flip {
            cols: self.cols.clone(),
            vals: self.vals.iter().map(|v| v.filter_mask(mask)).collect(),
        }
    }

    pub fn take(&self, n: i64) -> Flip {
        if n >= 0 && (n as usize) <= self.nrows() {
            let idx: Vec<usize> = (0..n as usize).collect();
            return self.gather(&idx);
        }
        if n < 0 && (n.unsigned_abs() as usize) <= self.nrows() {
            let un = n.unsigned_abs() as usize;
            let idx: Vec<usize> = (self.nrows() - un..self.nrows()).collect();
            return self.gather(&idx);
        }
        Flip {
            cols: self.cols.clone(),
            vals: self.vals.iter().map(|v| v.take(n)).collect(),
        }
    }

    pub fn drop_n(&self, n: i64) -> Flip {
        Flip {
            cols: self.cols.clone(),
            vals: self.vals.iter().map(|v| v.drop_n(n)).collect(),
        }
    }

    /// Extract row `r` as a dictionary `cols ! row_atoms` (what `t[0]` returns in kdb+).
    pub fn row_dict(&self, r: usize) -> Dict {
        let keys = K::Vector(ColVec::Sym(Attr::None, Arc::new(self.cols.clone())));
        let atoms: Vec<K> = self.vals.iter().map(|c| c.get_atom(r)).collect();
        let vals = K::Vector(ColVec::from_atoms(atoms));
        Dict { keys: Box::new(keys), vals: Box::new(vals) }
    }

    pub fn concat(&self, other: &Flip) -> KResult<Flip> {
        if self.cols != other.cols {
            return Err(KError::new("mismatch", "table columns do not match"));
        }
        let mut vals = Vec::with_capacity(self.vals.len());
        for (a, b) in self.vals.iter().zip(&other.vals) {
            vals.push(a.append_strict(b)?);
        }
        Ok(Flip { cols: self.cols.clone(), vals })
    }

    pub fn approx_bytes(&self) -> usize {
        self.vals.iter().map(|v| v.approx_bytes()).sum::<usize>() + self.cols.len() * 4
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KeyedTable {
    pub keys: Flip,
    pub vals: Flip,
}

impl KeyedTable {
    pub fn new(keys: Flip, vals: Flip) -> KResult<Self> {
        if !keys.cols.is_empty() && !vals.cols.is_empty() && keys.nrows() != vals.nrows() {
            return Err(KError::length("keyed table key and value row counts differ"));
        }
        Ok(Self { keys, vals })
    }

    pub fn nrows(&self) -> usize {
        if !self.keys.cols.is_empty() {
            self.keys.nrows()
        } else {
            self.vals.nrows()
        }
    }

    pub fn unkey(&self) -> Flip {
        let mut cols = Vec::with_capacity(self.keys.cols.len() + self.vals.cols.len());
        let mut vals = Vec::with_capacity(self.keys.vals.len() + self.vals.vals.len());
        cols.extend_from_slice(&self.keys.cols);
        vals.extend_from_slice(&self.keys.vals);
        cols.extend_from_slice(&self.vals.cols);
        vals.extend_from_slice(&self.vals.vals);
        Flip { cols, vals }
    }

    pub fn from_flip_nkeys(f: &Flip, nkeys: usize) -> KResult<K> {
        if nkeys == 0 {
            return Ok(K::Table(Arc::new(f.clone())));
        }
        if nkeys > f.ncols() {
            return Err(KError::domain("more key columns requested than table has"));
        }
        let keys = Flip { cols: f.cols[..nkeys].to_vec(), vals: f.vals[..nkeys].to_vec() };
        let vals = Flip { cols: f.cols[nkeys..].to_vec(), vals: f.vals[nkeys..].to_vec() };
        Ok(K::KeyedTable(Arc::new(KeyedTable { keys, vals })))
    }

    pub fn from_flip_key_names(f: &Flip, key_names: &[SymId]) -> KResult<K> {
        if key_names.is_empty() {
            return Ok(K::Table(Arc::new(f.clone())));
        }
        let mut kcols = Vec::with_capacity(key_names.len());
        let mut kvals = Vec::with_capacity(key_names.len());
        for &kn in key_names {
            let idx = f.col_idx(kn).ok_or_else(|| KError::domain(format!("unknown key column `{kn}")))?;
            kcols.push(kn);
            kvals.push(f.vals[idx].clone());
        }
        let mut vcols = Vec::new();
        let mut vvals = Vec::new();
        for (c, v) in f.cols.iter().zip(&f.vals) {
            if !key_names.contains(c) {
                vcols.push(*c);
                vvals.push(v.clone());
            }
        }
        Ok(K::KeyedTable(Arc::new(KeyedTable {
            keys: Flip { cols: kcols, vals: kvals },
            vals: Flip { cols: vcols, vals: vvals },
        })))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Dict {
    pub keys: Box<K>,
    pub vals: Box<K>,
}

// ----- Callables (`Lambda`, `AdverbKind`, `Func`) ---------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdverbKind {
    Each,
    Over,
    Scan,
    EachPrior,
    EachRight,
    EachLeft,
}

impl AdverbKind {
    pub fn symbol(self) -> &'static str {
        match self {
            AdverbKind::Each => "'",
            AdverbKind::Over => "/",
            AdverbKind::Scan => "\\",
            AdverbKind::EachPrior => "':",
            AdverbKind::EachRight => "/:",
            AdverbKind::EachLeft => "\\:",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Lambda {
    pub params: Vec<SymId>,
    pub implicit: bool,
    pub body: Vec<Expr>,
    pub source: String,
    pub ns: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Func {
    Prim(SymId),
    Lambda(Arc<Lambda>),
    Adverb { kind: AdverbKind, inner: Box<Func> },
    Proj { func: Box<Func>, args: Vec<Option<K>>, total_arity: usize },
    Comp(Box<Func>, Box<Func>),
}

impl Func {
    pub fn source_text(&self) -> String {
        match self {
            Func::Prim(s) => resolve(*s),
            Func::Lambda(l) => l.source.clone(),
            Func::Adverb { kind, inner } => format!("{}{}", inner.source_text(), kind.symbol()),
            Func::Proj { func, args, .. } => {
                let parts: Vec<String> = args
                    .iter()
                    .map(|a| a.as_ref().map(|k| k.q_repr()).unwrap_or_default())
                    .collect();
                format!("{}[{}]", func.source_text(), parts.join(";"))
            }
            Func::Comp(f, g) => format!("'[{};{}]", f.source_text(), g.source_text()),
        }
    }
}

// ----- `K` Value Enum -------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum K {
    /// Generic null `(::)` / omitted argument slot (`101h`).
    Null,
    Bool(bool),
    Guid([u8; 16]),
    Byte(u8),
    Short(i16),
    Int(i32),
    Long(i64),
    Real(f32),
    Float(f64),
    Char(u8),
    Sym(SymId),
    Timestamp(i64),
    Month(i32),
    Date(i32),
    Datetime(f64),
    Timespan(i64),
    Minute(i32),
    Second(i32),
    Time(i32),
    Vector(ColVec),
    Table(Arc<Flip>),
    KeyedTable(Arc<KeyedTable>),
    Dict(Arc<Dict>),
    Func(Func),
}

impl PartialEq for K {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (K::Null, K::Null) => true,
            (K::Bool(a), K::Bool(b)) => a == b,
            (K::Guid(a), K::Guid(b)) => a == b,
            (K::Byte(a), K::Byte(b)) => a == b,
            (K::Short(a), K::Short(b)) => a == b,
            (K::Int(a), K::Int(b)) => a == b,
            (K::Long(a), K::Long(b)) => a == b,
            (K::Real(a), K::Real(b)) => a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan()),
            (K::Float(a), K::Float(b)) => a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan()),
            (K::Char(a), K::Char(b)) => a == b,
            (K::Sym(a), K::Sym(b)) => a == b,
            (K::Timestamp(a), K::Timestamp(b)) => a == b,
            (K::Month(a), K::Month(b)) => a == b,
            (K::Date(a), K::Date(b)) => a == b,
            (K::Datetime(a), K::Datetime(b)) => a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan()),
            (K::Timespan(a), K::Timespan(b)) => a == b,
            (K::Minute(a), K::Minute(b)) => a == b,
            (K::Second(a), K::Second(b)) => a == b,
            (K::Time(a), K::Time(b)) => a == b,
            (K::Vector(a), K::Vector(b)) => a == b,
            (K::Table(a), K::Table(b)) => a == b,
            (K::KeyedTable(a), K::KeyedTable(b)) => a == b,
            (K::Dict(a), K::Dict(b)) => a == b,
            (K::Func(a), K::Func(b)) => a == b,
            _ => false,
        }
    }
}

impl K {
    pub fn string(s: &str) -> K {
        K::Vector(ColVec::Char(Attr::None, Arc::new(s.as_bytes().to_vec())))
    }

    pub fn sym(s: &str) -> K {
        K::Sym(intern(s))
    }

    pub fn type_num(&self) -> i16 {
        match self {
            K::Null => 101,
            K::Bool(_) => -1,
            K::Guid(_) => -2,
            K::Byte(_) => -4,
            K::Short(_) => -5,
            K::Int(_) => -6,
            K::Long(_) => -7,
            K::Real(_) => -8,
            K::Float(_) => -9,
            K::Char(_) => -10,
            K::Sym(_) => -11,
            K::Timestamp(_) => -12,
            K::Month(_) => -13,
            K::Date(_) => -14,
            K::Datetime(_) => -15,
            K::Timespan(_) => -16,
            K::Minute(_) => -17,
            K::Second(_) => -18,
            K::Time(_) => -19,
            K::Vector(v) => v.type_num(),
            K::Table(_) => 98,
            K::KeyedTable(_) | K::Dict(_) => 99,
            K::Func(Func::Lambda(_)) => 100,
            K::Func(Func::Prim(_)) => 102,
            K::Func(Func::Proj { .. }) => 104,
            K::Func(Func::Comp(_, _)) => 105,
            K::Func(Func::Adverb { kind, .. }) => match kind {
                AdverbKind::Each => 106,
                AdverbKind::Over => 107,
                AdverbKind::Scan => 108,
                AdverbKind::EachPrior => 109,
                AdverbKind::EachRight => 110,
                AdverbKind::EachLeft => 111,
            },
        }
    }

    pub fn is_atom(&self) -> bool {
        self.type_num() < 0
    }

    pub fn is_null(&self) -> bool {
        match self {
            K::Null => true,
            K::Guid(g) => *g == NULL_GUID,
            K::Short(v) => *v == NULL_SHORT,
            K::Int(v) | K::Month(v) | K::Date(v) | K::Minute(v) | K::Second(v) | K::Time(v) => *v == NULL_INT,
            K::Long(v) | K::Timestamp(v) | K::Timespan(v) => *v == NULL_LONG,
            K::Real(v) => v.is_nan(),
            K::Float(v) | K::Datetime(v) => v.is_nan(),
            K::Char(c) => *c == NULL_CHAR,
            K::Sym(s) => s.is_null(),
            _ => false,
        }
    }

    pub fn as_bool(&self) -> KResult<bool> {
        match self {
            K::Bool(b) => Ok(*b),
            K::Byte(v) => Ok(*v != 0),
            K::Short(v) => Ok(*v != 0),
            K::Int(v) => Ok(*v != 0),
            K::Long(v) => Ok(*v != 0),
            _ => Err(KError::ty("expected boolean condition")),
        }
    }

    pub fn as_i64(&self) -> KResult<i64> {
        match self {
            K::Bool(b) => Ok(i64::from(*b)),
            K::Byte(v) => Ok(*v as i64),
            K::Short(v) => Ok(if *v == NULL_SHORT { NULL_LONG } else { *v as i64 }),
            K::Int(v) | K::Month(v) | K::Date(v) | K::Minute(v) | K::Second(v) | K::Time(v) => {
                Ok(if *v == NULL_INT { NULL_LONG } else { *v as i64 })
            }
            K::Long(v) | K::Timestamp(v) | K::Timespan(v) => Ok(*v),
            _ => Err(KError::ty("expected integer")),
        }
    }

    pub fn as_f64(&self) -> KResult<f64> {
        match self {
            K::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
            K::Byte(v) => Ok(*v as f64),
            K::Short(v) => Ok(if *v == NULL_SHORT { f64::NAN } else { *v as f64 }),
            K::Int(v) | K::Month(v) | K::Date(v) | K::Minute(v) | K::Second(v) | K::Time(v) => {
                Ok(if *v == NULL_INT { f64::NAN } else { *v as f64 })
            }
            K::Long(v) | K::Timestamp(v) | K::Timespan(v) => Ok(if *v == NULL_LONG { f64::NAN } else { *v as f64 }),
            K::Real(v) => Ok(*v as f64),
            K::Float(v) | K::Datetime(v) => Ok(*v),
            _ => Err(KError::ty("expected number")),
        }
    }

    pub fn as_str(&self) -> Option<String> {
        match self {
            K::Char(c) => Some((*c as char).to_string()),
            K::Sym(s) => Some(resolve(*s)),
            K::Vector(v) => v.as_str(),
            _ => None,
        }
    }

    pub fn count(&self) -> usize {
        match self {
            K::Vector(v) => v.len(),
            K::Table(t) => t.nrows(),
            K::KeyedTable(kt) => kt.nrows(),
            K::Dict(d) => d.keys.count(),
            _ => 1,
        }
    }

    /// Total ordering across `K` values (nulls sort first, matching kdb+'s `iasc`).
    pub fn k_cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            (K::Sym(a), K::Sym(b)) => a.cmp(b),
            (K::Char(a), K::Char(b)) => a.cmp(b),
            (K::Guid(a), K::Guid(b)) => a.cmp(b),
            (K::Bool(a), K::Bool(b)) => a.cmp(b),
            (K::Byte(a), K::Byte(b)) => a.cmp(b),
            (K::Short(a), K::Short(b)) => a.cmp(b),
            (K::Int(a), K::Int(b))
            | (K::Month(a), K::Month(b))
            | (K::Date(a), K::Date(b))
            | (K::Minute(a), K::Minute(b))
            | (K::Second(a), K::Second(b))
            | (K::Time(a), K::Time(b)) => a.cmp(b),
            (K::Long(a), K::Long(b)) | (K::Timestamp(a), K::Timestamp(b)) | (K::Timespan(a), K::Timespan(b)) => a.cmp(b),
            (K::Float(a), K::Float(b)) | (K::Datetime(a), K::Datetime(b)) => match (a.is_nan(), b.is_nan()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                (false, false) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            },
            (K::Real(a), K::Real(b)) => match (a.is_nan(), b.is_nan()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                (false, false) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            },
            (K::Vector(ColVec::Char(_, a)), K::Vector(ColVec::Char(_, b))) => a.cmp(b),
            _ => {
                if let (Ok(fa), Ok(fb)) = (self.as_f64(), other.as_f64()) {
                    match (fa.is_nan(), fb.is_nan()) {
                        (true, true) => Ordering::Equal,
                        (true, false) => Ordering::Less,
                        (false, true) => Ordering::Greater,
                        (false, false) => fa.partial_cmp(&fb).unwrap_or(Ordering::Equal),
                    }
                } else {
                    self.type_num().cmp(&other.type_num()).then_with(|| self.hash_key().cmp(&other.hash_key()))
                }
            }
        }
    }

    /// Canonical byte key for hashing/grouping/distinct (`NaN` canonicalized so nulls group together).
    pub fn hash_key(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(12);
        out.extend_from_slice(&self.type_num().to_le_bytes());
        match self {
            K::Null => {}
            K::Bool(b) => out.push(u8::from(*b)),
            K::Guid(g) => out.extend_from_slice(g),
            K::Byte(b) | K::Char(b) => out.push(*b),
            K::Short(v) => out.extend_from_slice(&v.to_le_bytes()),
            K::Int(v) | K::Month(v) | K::Date(v) | K::Minute(v) | K::Second(v) | K::Time(v) => {
                out.extend_from_slice(&v.to_le_bytes())
            }
            K::Long(v) | K::Timestamp(v) | K::Timespan(v) => out.extend_from_slice(&v.to_le_bytes()),
            K::Real(v) => {
                let bits = if v.is_nan() { 0x7fc0_0000u32 } else if *v == 0.0 { 0 } else { v.to_bits() };
                out.extend_from_slice(&bits.to_le_bytes());
            }
            K::Float(v) | K::Datetime(v) => {
                let bits = if v.is_nan() { 0x7ff8_0000_0000_0000u64 } else if *v == 0.0 { 0 } else { v.to_bits() };
                out.extend_from_slice(&bits.to_le_bytes());
            }
            K::Sym(s) => out.extend_from_slice(&s.0.to_le_bytes()),
            K::Vector(ColVec::Char(_, bytes)) => out.extend_from_slice(bytes),
            K::Vector(v) => {
                out.extend_from_slice(&(v.len() as u32).to_le_bytes());
                for i in 0..v.len() {
                    out.extend_from_slice(&v.get_atom(i).hash_key());
                }
            }
            _ => out.extend_from_slice(self.q_repr().as_bytes()),
        }
        out
    }

    pub fn approx_bytes(&self) -> usize {
        match self {
            K::Vector(v) => v.approx_bytes(),
            K::Table(t) => t.approx_bytes(),
            K::KeyedTable(kt) => kt.keys.approx_bytes() + kt.vals.approx_bytes(),
            K::Dict(d) => d.keys.approx_bytes() + d.vals.approx_bytes(),
            _ => 16,
        }
    }

    /// Plain cell text for grids and CSV export (no quotes around strings or backticks on symbols).
    pub fn cell_text(&self) -> String {
        match self {
            K::Null => String::new(),
            K::Bool(b) => if *b { "1b".into() } else { "0b".into() },
            K::Guid(g) => if *g == NULL_GUID { String::new() } else { format_guid(g) },
            K::Byte(b) => format!("0x{b:02x}"),
            K::Short(v) => if *v == NULL_SHORT { String::new() } else if *v == INF_SHORT { "0Wh".into() } else if *v == NINF_SHORT { "-0Wh".into() } else { v.to_string() },
            K::Int(v) => if *v == NULL_INT { String::new() } else if *v == INF_INT { "0Wi".into() } else if *v == NINF_INT { "-0Wi".into() } else { v.to_string() },
            K::Long(v) => if *v == NULL_LONG { String::new() } else if *v == INF_LONG { "0W".into() } else if *v == NINF_LONG { "-0W".into() } else { v.to_string() },
            K::Real(v) => format_float_cell(*v as f64),
            K::Float(v) | K::Datetime(v) => format_float_cell(*v),
            K::Char(c) => (*c as char).to_string(),
            K::Sym(s) => resolve(*s),
            K::Timestamp(v) => if *v == NULL_LONG { String::new() } else { format_timestamp(*v) },
            K::Month(v) => if *v == NULL_INT { String::new() } else { format_month(*v) },
            K::Date(v) => if *v == NULL_INT { String::new() } else { format_date(*v) },
            K::Timespan(v) => if *v == NULL_LONG { String::new() } else { format_timespan(*v) },
            K::Minute(v) => if *v == NULL_INT { String::new() } else { format_minute(*v) },
            K::Second(v) => if *v == NULL_INT { String::new() } else { format_second(*v) },
            K::Time(v) => if *v == NULL_INT { String::new() } else { format_time(*v) },
            K::Vector(ColVec::Char(_, bytes)) => String::from_utf8_lossy(bytes).into_owned(),
            other => other.q_repr(),
        }
    }

    /// Authentic `q` console representation of a `K` value.
    pub fn q_repr(&self) -> String {
        match self {
            K::Null => "::".into(),
            K::Bool(b) => if *b { "1b".into() } else { "0b".into() },
            K::Guid(g) => format_guid(g),
            K::Byte(b) => format!("0x{b:02x}"),
            K::Short(v) => match *v {
                NULL_SHORT => "0Nh".into(),
                INF_SHORT => "0Wh".into(),
                NINF_SHORT => "-0Wh".into(),
                n => format!("{n}h"),
            },
            K::Int(v) => match *v {
                NULL_INT => "0Ni".into(),
                INF_INT => "0Wi".into(),
                NINF_INT => "-0Wi".into(),
                n => format!("{n}i"),
            },
            K::Long(v) => match *v {
                NULL_LONG => "0N".into(),
                INF_LONG => "0W".into(),
                NINF_LONG => "-0W".into(),
                n => n.to_string(),
            },
            K::Real(v) => {
                if v.is_nan() {
                    "0Ne".into()
                } else if *v == f32::INFINITY {
                    "0We".into()
                } else if *v == f32::NEG_INFINITY {
                    "-0We".into()
                } else {
                    format!("{v}e")
                }
            }
            K::Float(v) | K::Datetime(v) => format_float_q(*v),
            K::Char(c) => format!("\"{}\"", escape_q_char(*c)),
            K::Sym(s) => format!("`{}", resolve(*s)),
            K::Timestamp(v) => format_timestamp(*v),
            K::Month(v) => format_month(*v),
            K::Date(v) => format_date(*v),
            K::Timespan(v) => format_timespan(*v),
            K::Minute(v) => format_minute(*v),
            K::Second(v) => format_second(*v),
            K::Time(v) => format_time(*v),
            K::Vector(v) => format_vector_q(v),
            K::Table(t) => crate::tools::format_flip(t, 40),
            K::KeyedTable(kt) => crate::tools::format_keyed_table(kt, 40),
            K::Dict(d) => crate::tools::format_dict(d),
            K::Func(f) => f.source_text(),
        }
    }
}

fn format_float_cell(v: f64) -> String {
    if v.is_nan() {
        String::new()
    } else if v == f64::INFINITY {
        "0w".into()
    } else if v == f64::NEG_INFINITY {
        "-0w".into()
    } else {
        format_float_num(v, false)
    }
}

fn format_float_q(v: f64) -> String {
    if v.is_nan() {
        "0n".into()
    } else if v == f64::INFINITY {
        "0w".into()
    } else if v == f64::NEG_INFINITY {
        "-0w".into()
    } else {
        format_float_num(v, true)
    }
}

fn format_float_num(v: f64, suffix_if_int: bool) -> String {
    let v = if v == 0.0 { 0.0 } else { v };
    // Round to 7 significant-ish decimal places like kdb+'s `\P 7` default to avoid 0.30000000000000004 noise
    let s = if v.abs() >= 1e9 || (v != 0.0 && v.abs() < 1e-5) {
        format!("{v:.7e}")
    } else {
        let rounded = (v * 1e7).round() / 1e7;
        let raw = format!("{rounded}");
        raw
    };
    if suffix_if_int && !s.contains('.') && !s.contains('e') {
        format!("{s}f")
    } else {
        s
    }
}

fn escape_q_char(b: u8) -> String {
    match b {
        b'"' => "\\\"".into(),
        b'\\' => "\\\\".into(),
        b'\n' => "\\n".into(),
        b'\r' => "\\r".into(),
        b'\t' => "\\t".into(),
        0x20..=0x7e => (b as char).to_string(),
        _ => format!("\\{b:03o}"),
    }
}

fn format_vector_q(v: &ColVec) -> String {
    let pfx = v.attr().prefix();
    let single = v.len() == 1;
    let enlist = if single && !matches!(v, ColVec::Char(_, _)) { "," } else { "" };
    match v {
        ColVec::Bool(_, xs) => {
            if xs.is_empty() {
                format!("{pfx}`boolean$()")
            } else {
                let bits: String = xs.iter().map(|&b| if b { '1' } else { '0' }).collect();
                format!("{pfx}{enlist}{bits}b")
            }
        }
        ColVec::Byte(_, xs) => {
            if xs.is_empty() {
                format!("{pfx}`byte$()")
            } else {
                let hex: String = xs.iter().map(|b| format!("{b:02x}")).collect();
                format!("{pfx}{enlist}0x{hex}")
            }
        }
        ColVec::Char(_, xs) => {
            let body: String = xs.iter().map(|&b| escape_q_char(b)).collect();
            if single {
                format!("{pfx},\"{body}\"")
            } else {
                format!("{pfx}\"{body}\"")
            }
        }
        ColVec::Sym(_, xs) => {
            if xs.is_empty() {
                format!("{pfx}`symbol$()")
            } else {
                let syms: String = xs.iter().map(|s| format!("`{}", resolve(*s))).collect();
                format!("{pfx}{enlist}{syms}")
            }
        }
        ColVec::Short(_, xs) => {
            if xs.is_empty() {
                format!("{pfx}`short$()")
            } else {
                let parts: Vec<String> = xs
                    .iter()
                    .map(|&n| match n {
                        NULL_SHORT => "0N".into(),
                        INF_SHORT => "0W".into(),
                        NINF_SHORT => "-0W".into(),
                        x => x.to_string(),
                    })
                    .collect();
                format!("{pfx}{enlist}{}h", parts.join(" "))
            }
        }
        ColVec::Int(_, xs) => {
            if xs.is_empty() {
                format!("{pfx}`int$()")
            } else {
                let parts: Vec<String> = xs
                    .iter()
                    .map(|&n| match n {
                        NULL_INT => "0N".into(),
                        INF_INT => "0W".into(),
                        NINF_INT => "-0W".into(),
                        x => x.to_string(),
                    })
                    .collect();
                format!("{pfx}{enlist}{}i", parts.join(" "))
            }
        }
        ColVec::Long(_, xs) => {
            if xs.is_empty() {
                format!("{pfx}`long$()")
            } else {
                let parts: Vec<String> = xs
                    .iter()
                    .map(|&n| match n {
                        NULL_LONG => "0N".into(),
                        INF_LONG => "0W".into(),
                        NINF_LONG => "-0W".into(),
                        x => x.to_string(),
                    })
                    .collect();
                format!("{pfx}{enlist}{}", parts.join(" "))
            }
        }
        ColVec::Float(_, xs) | ColVec::Datetime(_, xs) => {
            if xs.is_empty() {
                format!("{pfx}`float$()")
            } else {
                let any_dot = xs.iter().any(|x| x.is_nan() || x.is_infinite() || x.fract() != 0.0);
                let parts: Vec<String> = xs.iter().map(|&x| format_float_num_vec(x)).collect();
                let tail = if any_dot { "" } else { "f" };
                format!("{pfx}{enlist}{}{tail}", parts.join(" "))
            }
        }
        ColVec::Real(_, xs) => {
            if xs.is_empty() {
                format!("{pfx}`real$()")
            } else {
                let parts: Vec<String> = xs.iter().map(|&x| format_float_num_vec(x as f64)).collect();
                format!("{pfx}{enlist}{}e", parts.join(" "))
            }
        }
        ColVec::General(_, xs) => {
            if xs.is_empty() {
                "()".into()
            } else if xs.len() == 1 {
                format!(",{}", xs[0].q_repr())
            } else {
                let parts: Vec<String> = xs.iter().map(|x| x.q_repr()).collect();
                format!("({})", parts.join(";"))
            }
        }
        _ => {
            if v.is_empty() {
                format!("{pfx}`{}$()", v.type_name())
            } else {
                let parts: Vec<String> = (0..v.len()).map(|i| v.get_atom(i).q_repr()).collect();
                format!("{pfx}{enlist}{}", parts.join(" "))
            }
        }
    }
}

fn format_float_num_vec(v: f64) -> String {
    if v.is_nan() {
        "0n".into()
    } else if v == f64::INFINITY {
        "0w".into()
    } else if v == f64::NEG_INFINITY {
        "-0w".into()
    } else {
        format_float_num(v, false)
    }
}

impl fmt::Display for K {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.q_repr())
    }
}
