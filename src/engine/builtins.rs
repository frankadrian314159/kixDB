//! Further built-in verbs: ranks and shifts, matrices, time zones and the environment.
use crate::engine::ops::{self, eval_binary_prim, eval_unary_prim};
use crate::error::{KError, KResult};
use crate::sym::{intern, resolve};
use crate::types::*;
use chrono::{Local, Offset, TimeZone};
use std::sync::Arc;

fn longs(items: Vec<i64>) -> K {
    K::Vector(ColVec::Long(Attr::None, Arc::new(items)))
}

/// Each item's place in the sorted list.
fn rank(x: &K) -> KResult<K> {
    eval_unary_prim("iasc", &eval_unary_prim("iasc", x)?)
}

/// The numbers of a matrix, row by row.
fn matrix(x: &K) -> KResult<Vec<Vec<f64>>> {
    let K::Vector(rows) = x else { return Err(KError::ty("expected a matrix")) };
    let numbers = |i| match rows.get_atom(i) {
        K::Vector(row) => row.as_f64_vec().ok_or_else(|| KError::ty("expected a matrix of numbers")),
        _ => Err(KError::ty("expected a matrix")),
    };
    let m: Vec<Vec<f64>> = (0..rows.len()).map(numbers).collect::<KResult<_>>()?;
    match m.windows(2).all(|w| w[0].len() == w[1].len()) {
        true => Ok(m),
        false => Err(KError::length("the rows of a matrix must be equally long")),
    }
}

fn from_matrix(m: Vec<Vec<f64>>) -> K {
    let rows = m.into_iter().map(|r| K::Vector(ColVec::Float(Attr::None, Arc::new(r)))).collect();
    K::Vector(ColVec::General(Attr::None, Arc::new(rows)))
}

fn multiply(a: &[Vec<f64>], b: &[Vec<f64>]) -> KResult<Vec<Vec<f64>>> {
    if a.first().is_some_and(|row| row.len() != b.len()) {
        return Err(KError::length("the matrices do not fit each other"));
    }
    let width = b.first().map_or(0, Vec::len);
    Ok(a.iter().map(|row| (0..width).map(|j| row.iter().zip(b).map(|(x, r)| x * r[j]).sum()).collect()).collect())
}

fn transpose(m: &[Vec<f64>]) -> Vec<Vec<f64>> {
    (0..m.first().map_or(0, Vec::len)).map(|j| m.iter().map(|row| row[j]).collect()).collect()
}

/// The inverse of a square matrix, by Gauss-Jordan elimination with the largest pivot.
fn invert(mut m: Vec<Vec<f64>>) -> KResult<Vec<Vec<f64>>> {
    let n = m.len();
    if m.iter().any(|row| row.len() != n) {
        return Err(KError::length("only a square matrix has an inverse"));
    }
    for (i, row) in m.iter_mut().enumerate() {
        row.extend((0..n).map(|j| if i == j { 1.0 } else { 0.0 }));
    }
    for c in 0..n {
        let pivot = (c..n).max_by(|&a, &b| m[a][c].abs().total_cmp(&m[b][c].abs())).unwrap_or(c);
        if m[pivot][c] == 0.0 {
            return Err(KError::domain("the matrix has no inverse"));
        }
        m.swap(c, pivot);
        let scale = m[c][c];
        m[c].iter_mut().for_each(|v| *v /= scale);
        let by = m[c].clone();
        for (r, row) in m.iter_mut().enumerate() {
            let factor = row[c];
            if r != c {
                row.iter_mut().zip(&by).for_each(|(v, p)| *v -= factor * p);
            }
        }
    }
    Ok(m.into_iter().map(|row| row[n..].to_vec()).collect())
}

/// A point in time moved between local time and UTC.
fn zone(x: &K, to_local: bool) -> KResult<K> {
    let K::Timestamp(ns) = x else { return Err(KError::ty("expected a timestamp")) };
    // (kdb+ times count from 2000.01.01, which is this many seconds after 1970)
    let utc = chrono::DateTime::from_timestamp(ns.div_euclid(NANOS_PER_SEC) + 946_684_800, 0).ok_or_else(|| KError::domain("time out of range"))?;
    let offset = i64::from(Local.offset_from_utc_datetime(&utc.naive_utc()).fix().local_minus_utc()) * NANOS_PER_SEC;
    Ok(K::Timestamp(if to_local { ns + offset } else { ns - offset }))
}

/// `all` (the least) or `any` (the greatest) of the items as booleans, so `1b` or `0b` of none: of a dictionary, its
/// values', and of a table, column by column.
fn truth(x: &K, all: bool) -> KResult<K> {
    match x {
        K::Dict(d) => truth(&d.vals, all),
        K::KeyedTable(t) => truth(&K::Table(Arc::new(t.vals.clone())), all),
        K::Table(t) => {
            let vals = t.vals.iter().map(|c| truth(&K::Vector(c.clone()), all)).collect::<KResult<_>>()?;
            let keys = Box::new(K::Vector(ColVec::Sym(Attr::None, Arc::new(t.cols.clone()))));
            Ok(K::Dict(Arc::new(Dict { keys, vals: Box::new(K::Vector(ColVec::from_atoms(vals))) })))
        }
        _ => ops::cast_op(&K::Sym(intern("boolean")), x).and_then(|b| ops::minmax_op(&b, all)),
    }
}

pub fn unary(op: &str, x: &K) -> Option<KResult<K>> {
    Some(match op {
        "all" | "any" => truth(x, op == "all"),
        "rank" => rank(x),
        "attr" => Ok(K::Sym(match x {
            K::Vector(v) => intern(v.attr().prefix().trim_matches(|c| c == '`' || c == '#')),
            _ => intern(""),
        })),
        "inv" => matrix(x).and_then(invert).map(from_matrix),
        "ltime" => zone(x, true),
        "gtime" => zone(x, false),
        "getenv" => match x {
            K::Sym(name) => Ok(K::string(&std::env::var(resolve(*name)).unwrap_or_default())),
            _ => Err(KError::ty("getenv takes a symbol")),
        },
        _ => return None,
    })
}

pub fn binary(op: &str, x: &K, y: &K) -> Option<KResult<K>> {
    Some(match op {
        "rotate" => x.as_i64().and_then(|n| {
            let by = longs(vec![n.rem_euclid(y.count().max(1) as i64)]);
            let by = ops::first_op(&by);
            ops::concat_k(&eval_binary_prim("_", &by, y)?, &eval_binary_prim("#", &by, y)?)
        }),
        "xprev" => x.as_i64().and_then(|n| ops::index_k(y, &[Some(longs((0..y.count() as i64).map(|i| i - n).collect()))])),
        "xrank" => x.as_i64().and_then(|n| {
            let K::Vector(ranks) = rank(y)? else { return Err(KError::ty("xrank takes a list")) };
            let len = ranks.len() as i64;
            Ok(longs(ranks.as_i64_vec().unwrap_or_default().into_iter().map(|r| r * n / len.max(1)).collect()))
        }),
        "scov" => eval_binary_prim("cov", x, y).and_then(|c| {
            let n = x.count() as f64;
            Ok(K::Float(c.as_f64()? * n / (n - 1.0)))
        }),
        "mmu" => match (x, y) {
            // of two vectors, their dot product
            (K::Vector(a), K::Vector(b)) if a.is_numeric() && b.is_numeric() => eval_binary_prim("wsum", x, y),
            _ => matrix(x).and_then(|a| multiply(&a, &matrix(y)?)).map(from_matrix),
        },
        // the `w` with `w mmu y` closest to `x`
        "lsq" => matrix(x).and_then(|a| {
            let b = matrix(y)?;
            let t = transpose(&b);
            multiply(&multiply(&a, &t)?, &invert(multiply(&b, &t)?)?).map(from_matrix)
        }),
        "setenv" => match (x, y.as_str()) {
            (K::Sym(name), Some(value)) if !resolve(*name).is_empty() && !value.contains('\0') => {
                std::env::set_var(resolve(*name), value);
                Ok(K::Null)
            }
            _ => Err(KError::ty("`name setenv \"value\"")),
        },
        _ => return None,
    })
}
