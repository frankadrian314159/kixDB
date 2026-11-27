//! `.j.j`, `.j.jd` and `.j.k`: q values as JSON text and back.
use crate::engine::rows;
use crate::error::{KError, KResult};
use crate::sym::resolve;
use crate::types::*;
use serde_json::Value;
use std::sync::Arc;

fn quoted(s: &str) -> String {
    Value::String(s.into()).to_string()
}

/// `x` as JSON, floats to `\P` digits. Infinities are written `inf` and `-inf`, as kdb+ does, or with `null_infinities`
/// as `null`.
pub fn write(x: &K, null_infinities: bool) -> String {
    let item = |x: &K| write(x, null_infinities);
    let infinity = |negative: bool| match null_infinities {
        true => "null".to_string(),
        false => if negative { "-inf" } else { "inf" }.to_string(),
    };
    let whole = |v: i64, top: i64| match v {
        v if v == top => infinity(false),
        v if v == -top => infinity(true),
        v if v < -top => "null".into(),
        v => v.to_string(),
    };
    let float = |v: f64| match v {
        v if v.is_nan() => "null".into(),
        v if v.is_infinite() => infinity(v < 0.0),
        v => crate::display::float(v),
    };
    // dates are written with dashes, and a null time is empty
    let time = |x: &K, dashes: usize| match x.is_null() {
        true => quoted(""),
        false => quoted(&x.cell_text().trim_end_matches('m').replacen('.', "-", dashes)),
    };
    let object = |pairs: Vec<(String, String)>| {
        let pairs: Vec<String> = pairs.into_iter().map(|(k, v)| format!("{}:{v}", quoted(&k))).collect();
        format!("{{{}}}", pairs.join(","))
    };
    let array = |items: Vec<String>| format!("[{}]", items.join(","));
    let table = |t: &Flip| {
        let names: Vec<String> = t.cols.iter().map(|&c| resolve(c).into()).collect();
        let row = |r| object(names.iter().cloned().zip(t.vals.iter().map(|c| item(&c.get_atom(r)))).collect());
        array((0..t.nrows()).map(row).collect())
    };
    match x {
        K::Null => "null".into(),
        K::Bool(b) => b.to_string(),
        K::Byte(b) => quoted(&format!("{b:02x}")),
        K::Short(v) => whole(i64::from(*v), i64::from(INF_SHORT)),
        K::Int(v) => whole(i64::from(*v), i64::from(INF_INT)),
        K::Long(v) => whole(*v, INF_LONG),
        K::Real(v) => float(f64::from(*v)),
        K::Float(v) => float(*v),
        K::Char(c) => quoted(&(*c as char).to_string()),
        K::Sym(s) => quoted(resolve(*s)),
        K::Guid(_) | K::Minute(_) | K::Second(_) | K::Time(_) | K::Timespan(_) => time(x, 0),
        K::Month(_) => time(x, 1),
        K::Date(_) | K::Timestamp(_) | K::Datetime(_) => time(x, 2),
        K::Enum(_, v) => item(v),
        K::Vector(v @ ColVec::Char(..)) => quoted(&v.as_str().unwrap_or_default()),
        K::Vector(v) => array((0..v.len()).map(|i| item(&v.get_atom(i))).collect()),
        K::Table(t) => table(t),
        K::KeyedTable(t) => table(&t.unkey()),
        K::Dict(d) => {
            let nth = |side: &K, i: usize| crate::engine::ops::index_k(side, &[Some(K::Long(i as i64))]).unwrap_or(K::Null);
            let key = |i| {
                let key = nth(&d.keys, i);
                key.as_str().unwrap_or_else(|| key.cell_text())
            };
            object((0..d.keys.count()).map(|i| (key(i), item(&nth(&d.vals, i)))).collect())
        }
        K::Func(f) => quoted(&f.source_text()),
    }
}

/// The q value of JSON: numbers are floats, strings char lists, objects dictionaries, and objects alike a table.
fn value(json: Value) -> K {
    match json {
        Value::Null => K::Float(f64::NAN),
        Value::Bool(b) => K::Bool(b),
        Value::Number(n) => K::Float(n.as_f64().unwrap_or(f64::NAN)),
        Value::String(s) => K::string(&s),
        Value::Array(items) => rows::collapse(items.into_iter().map(value).collect()),
        Value::Object(fields) => {
            let (names, values): (Vec<_>, Vec<_>) = fields.into_iter().map(|(k, v)| (crate::sym::intern(&k), value(v))).unzip();
            let keys = Box::new(K::Vector(ColVec::Sym(Attr::None, Arc::new(names))));
            K::Dict(Arc::new(Dict { keys, vals: Box::new(rows::collapse(values)) }))
        }
    }
}

pub fn read(text: &str) -> KResult<K> {
    serde_json::from_str(text).map(value).map_err(|e| KError::new("parse", format!("not JSON: {e}")))
}
