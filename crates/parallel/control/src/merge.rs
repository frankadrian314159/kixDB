//! Two-phase aggregation: what each shard computes for an aggregate, and how the control node streams and merges the
//! shards' partial states directly into typed columns without concatenating partial tables or boxing per-group atoms.
use kixdb::ast::{ColSpec, Expr};
use kixdb::engine::ops::sort_order;
use kixdb::strings::Strings;
use kixdb::sym::{intern, resolve, SymId};
use kixdb::types::{Tag, INF_INT, INF_LONG, NINF_INT, NINF_LONG, NULL_CHAR, NULL_INT, NULL_LONG, NULL_SHORT};
use kixdb::{Attr, Bools, ColVec, Flip, KError, KResult, K};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Leaf {
    Sum,
    Count,
    Min,
    Max,
    Prd,
    Avg,
    /// `var`, `svar` (sample), `dev`, `sdev` (square root).
    Var { sample: bool, root: bool },
    Wsum,
    Wavg,
}

/// A result column as an expression over merged aggregates.
#[derive(Debug)]
pub enum Tree {
    Leaf(usize),
    Unary(SymId, Box<Tree>),
    Binary(SymId, Box<Tree>, Box<Tree>),
    Const(Expr),
}

pub struct Aggs {
    /// Each aggregate and where its partial columns start.
    leaves: Vec<(Leaf, usize)>,
    /// The columns the shards compute.
    pub partials: Vec<ColSpec>,
    pub trees: Vec<Tree>,
}

/// What the planner knows about argument expressions.
pub struct Checks<'a> {
    /// Computed row by row from at least one column.
    pub row: &'a dyn Fn(&Expr) -> bool,
    pub constant: &'a dyn Fn(&Expr) -> bool,
    /// The type of the column the expression gives.
    pub type_of: &'a dyn Fn(&Expr) -> Option<i16>,
}

fn unary(op: &str, arg: Expr) -> Expr {
    Expr::Unary { op: intern(op), arg: Box::new(arg) }
}

fn binary(op: &str, lhs: Expr, rhs: Expr) -> Expr {
    Expr::Binary { op: intern(op), lhs: Box::new(lhs), rhs: Box::new(rhs) }
}

fn numeric(c: &Checks, e: &Expr) -> bool {
    matches!((c.type_of)(e), Some(1 | 4..=9))
}

/// The `n` such that `w wsum n` adds up the weights of the rows where `x` is not null.
fn present(x: &Expr) -> Expr {
    unary("not", unary("null", x.clone()))
}

fn leaf(out: &mut Aggs, kind: Leaf, partials: Vec<Expr>) -> Tree {
    out.leaves.push((kind, out.partials.len()));
    for expr in partials {
        let name = intern(&format!("kix__p{}", out.partials.len()));
        out.partials.push(ColSpec { name: Some(name), expr });
    }
    Tree::Leaf(out.leaves.len() - 1)
}

fn single(c: &Checks, out: &mut Aggs, verb: &str, x: &Expr) -> Option<Tree> {
    let is_row_count = verb == "count" && matches!(x, Expr::Var(s) if resolve(*s) == "i");
    if !(c.row)(x) && !is_row_count {
        return None;
    }
    let kind = match verb {
        "sum" if numeric(c, x) || (c.type_of)(x) == Some(16) => Leaf::Sum,
        "count" => Leaf::Count,
        "min" => Leaf::Min,
        "max" => Leaf::Max,
        "prd" if numeric(c, x) => Leaf::Prd,
        "avg" if numeric(c, x) => Leaf::Avg,
        "var" | "svar" | "dev" | "sdev" if numeric(c, x) => Leaf::Var { sample: verb.starts_with('s'), root: verb.ends_with("dev") },
        _ => return None,
    };
    let count_present = || unary("sum", present(x));
    let partials = match kind {
        Leaf::Avg => vec![unary("sum", x.clone()), count_present()],
        Leaf::Var { .. } => vec![count_present(), unary("avg", x.clone()), unary("var", x.clone())],
        _ => vec![unary(verb, x.clone())],
    };
    Some(leaf(out, kind, partials))
}

fn weighted(c: &Checks, out: &mut Aggs, verb: &str, w: &Expr, x: &Expr) -> Option<Tree> {
    let argument = |e: &Expr| ((c.row)(e) || (c.constant)(e)) && numeric(c, e);
    if !(argument(w) && argument(x) && (c.row)(w) && (c.row)(x)) {
        return None;
    }
    let total = binary("wsum", w.clone(), x.clone());
    Some(match verb {
        "wsum" => leaf(out, Leaf::Wsum, vec![total]),
        _ => leaf(out, Leaf::Wavg, vec![total, binary("wsum", w.clone(), present(x))]),
    })
}

fn tree(c: &Checks, out: &mut Aggs, e: &Expr) -> Option<Tree> {
    const UNARY: [&str; 9] = ["-", "neg", "abs", "sqrt", "exp", "log", "floor", "ceiling", "reciprocal"];
    const BINARY: [&str; 8] = ["+", "-", "*", "%", "&", "|", "^", "div"];
    if (c.constant)(e) {
        // a constant beside an aggregate; it may not hide one
        let aggregates = e.any(&mut |x| matches!(x, Expr::Unary { .. } | Expr::Apply { .. } | Expr::Query(_)));
        return (!aggregates).then(|| Tree::Const(e.clone()));
    }
    match e {
        Expr::Unary { op, arg } if UNARY.contains(&resolve(*op)) => Some(Tree::Unary(*op, Box::new(tree(c, out, arg)?))),
        Expr::Unary { op, arg } => single(c, out, resolve(*op), arg),
        Expr::Apply { target, args } => match (&**target, args.as_slice()) {
            (Expr::Var(f), [Some(a)]) => single(c, out, resolve(*f), a),
            (Expr::Var(f), [Some(a), Some(b)]) if matches!(resolve(*f), "wsum" | "wavg") => weighted(c, out, resolve(*f), a, b),
            _ => None,
        },
        Expr::Binary { op, lhs, rhs } => match resolve(*op) {
            verb @ ("wsum" | "wavg") => weighted(c, out, verb, lhs, rhs),
            o if BINARY.contains(&o) => {
                // right to left, as q evaluates
                let r = tree(c, out, rhs)?;
                let l = tree(c, out, lhs)?;
                Some(Tree::Binary(*op, Box::new(l), Box::new(r)))
            }
            _ => None,
        },
        _ => None,
    }
}

fn has_leaf(t: &Tree) -> bool {
    match t {
        Tree::Leaf(_) => true,
        Tree::Unary(_, a) => has_leaf(a),
        Tree::Binary(_, a, b) => has_leaf(a) || has_leaf(b),
        Tree::Const(_) => false,
    }
}

/// The plan for a column list made only of aggregates (and arithmetic on them).
pub fn analyze(specs: &[ColSpec], checks: &Checks) -> Option<Aggs> {
    let mut out = Aggs { leaves: vec![], partials: vec![], trees: vec![] };
    for s in specs {
        let t = tree(checks, &mut out, &s.expr)?;
        if !has_leaf(&t) {
            return None;
        }
        out.trees.push(t);
    }
    Some(out)
}

impl Aggs {
    /// A result column as an expression over the merged aggregates (`leaves[i]` is aggregate `i`'s value or values).
    pub fn rebuild(&self, t: &Tree, leaves: &[K]) -> Expr {
        match t {
            Tree::Leaf(i) => Expr::Lit(leaves[*i].clone()),
            Tree::Unary(op, a) => Expr::Unary { op: *op, arg: Box::new(self.rebuild(a, leaves)) },
            Tree::Binary(op, a, b) => {
                Expr::Binary { op: *op, lhs: Box::new(self.rebuild(a, leaves)), rhs: Box::new(self.rebuild(b, leaves)) }
            }
            Tree::Const(e) => e.clone(),
        }
    }
}

enum NumSlice<'a> {
    F64(&'a [f64]),
    F32(&'a [f32]),
    I64(&'a [i64]),
    I32(&'a [i32]),
    I16(&'a [i16]),
    U8(&'a [u8]),
    Bool(&'a Bools),
    Other(&'a ColVec),
}

impl<'a> NumSlice<'a> {
    fn new(cv: &'a ColVec) -> Self {
        match cv.plain() {
            ColVec::Float(_, xs) | ColVec::Datetime(_, xs) => Self::F64(xs),
            ColVec::Real(_, xs) => Self::F32(xs),
            ColVec::Long(_, xs) | ColVec::Timestamp(_, xs) | ColVec::Timespan(_, xs) => Self::I64(xs),
            ColVec::Int(_, xs)
            | ColVec::Month(_, xs)
            | ColVec::Date(_, xs)
            | ColVec::Minute(_, xs)
            | ColVec::Second(_, xs)
            | ColVec::Time(_, xs) => Self::I32(xs),
            ColVec::Short(_, xs) => Self::I16(xs),
            ColVec::Byte(_, xs) => Self::U8(xs),
            ColVec::Bool(_, xs) => Self::Bool(xs),
            other => Self::Other(other),
        }
    }

    #[inline]
    fn f64_at(&self, r: usize) -> f64 {
        match self {
            Self::F64(xs) => xs[r],
            Self::F32(xs) => xs[r] as f64,
            Self::I64(xs) => if xs[r] == NULL_LONG { f64::NAN } else { xs[r] as f64 },
            Self::I32(xs) => if xs[r] == NULL_INT { f64::NAN } else { xs[r] as f64 },
            Self::I16(xs) => if xs[r] == NULL_SHORT { f64::NAN } else { xs[r] as f64 },
            Self::U8(xs) => xs[r] as f64,
            Self::Bool(xs) => if xs.get(r) { 1.0 } else { 0.0 },
            Self::Other(cv) => cv.get_atom(r).as_f64().unwrap_or(f64::NAN),
        }
    }

    #[inline]
    fn i64_at(&self, r: usize) -> i64 {
        match self {
            Self::I64(xs) => xs[r],
            Self::I32(xs) => if xs[r] == NULL_INT { NULL_LONG } else { xs[r] as i64 },
            Self::I16(xs) => if xs[r] == NULL_SHORT { NULL_LONG } else { xs[r] as i64 },
            Self::U8(xs) => xs[r] as i64,
            Self::Bool(xs) => i64::from(xs.get(r)),
            _ => self.f64_at(r) as i64,
        }
    }
}

enum KeyColAcc {
    Bool(Bools),
    Guid(Vec<[u8; 16]>),
    Byte(Vec<u8>),
    Short(Vec<i16>),
    Int(i16, Vec<i32>),
    Long(i16, Vec<i64>),
    Real(Vec<f32>),
    Float(i16, Vec<f64>),
    Char(Vec<u8>),
    Sym(Vec<SymId>),
    Strings(Strings),
    Enum(Tag, Box<KeyColAcc>),
    General(Vec<K>),
}

impl KeyColAcc {
    fn from_proto(cv: &ColVec) -> Self {
        match cv {
            ColVec::Enum(t, inner) => Self::Enum(*t, Box::new(Self::from_proto(inner))),
            ColVec::Bool(..) => Self::Bool(Bools::new()),
            ColVec::Guid(..) => Self::Guid(Vec::new()),
            ColVec::Byte(..) => Self::Byte(Vec::new()),
            ColVec::Short(..) => Self::Short(Vec::new()),
            ColVec::Int(..) | ColVec::Month(..) | ColVec::Date(..) | ColVec::Minute(..) | ColVec::Second(..) | ColVec::Time(..) => {
                Self::Int(cv.type_num(), Vec::new())
            }
            ColVec::Long(..) | ColVec::Timestamp(..) | ColVec::Timespan(..) => Self::Long(cv.type_num(), Vec::new()),
            ColVec::Real(..) => Self::Real(Vec::new()),
            ColVec::Float(..) | ColVec::Datetime(..) => Self::Float(cv.type_num(), Vec::new()),
            ColVec::Char(..) => Self::Char(Vec::new()),
            ColVec::Sym(..) => Self::Sym(Vec::new()),
            ColVec::Strings(..) => Self::Strings(Strings::default()),
            ColVec::General(..) => Self::General(Vec::new()),
        }
    }

    fn push_rows(&mut self, cv: &ColVec, rows: &[usize]) {
        if rows.is_empty() {
            return;
        }
        match (self, cv) {
            (Self::Enum(_, inner), ColVec::Enum(_, src)) => inner.push_rows(src, rows),
            (Self::Bool(out), ColVec::Bool(_, xs)) => rows.iter().for_each(|&r| out.push(xs.get(r))),
            (Self::Guid(out), ColVec::Guid(_, xs)) => rows.iter().for_each(|&r| out.push(xs[r])),
            (Self::Byte(out), ColVec::Byte(_, xs)) => rows.iter().for_each(|&r| out.push(xs[r])),
            (Self::Short(out), ColVec::Short(_, xs)) => rows.iter().for_each(|&r| out.push(xs[r])),
            (Self::Int(_, out), ColVec::Int(_, xs) | ColVec::Month(_, xs) | ColVec::Date(_, xs) | ColVec::Minute(_, xs) | ColVec::Second(_, xs) | ColVec::Time(_, xs)) => {
                rows.iter().for_each(|&r| out.push(xs[r]));
            }
            (Self::Long(_, out), ColVec::Long(_, xs) | ColVec::Timestamp(_, xs) | ColVec::Timespan(_, xs)) => {
                rows.iter().for_each(|&r| out.push(xs[r]));
            }
            (Self::Real(out), ColVec::Real(_, xs)) => rows.iter().for_each(|&r| out.push(xs[r])),
            (Self::Float(_, out), ColVec::Float(_, xs) | ColVec::Datetime(_, xs)) => rows.iter().for_each(|&r| out.push(xs[r])),
            (Self::Char(out), ColVec::Char(_, xs)) => rows.iter().for_each(|&r| out.push(xs[r])),
            (Self::Sym(out), ColVec::Sym(_, xs)) => rows.iter().for_each(|&r| out.push(xs[r])),
            (Self::Strings(out), _) if cv.packed().is_some() => {
                let packed = cv.packed().unwrap();
                let ColVec::Strings(_, s) = &*packed else { unreachable!() };
                rows.iter().for_each(|&r| out.push(s.get(r)));
            }
            (Self::General(out), src) => rows.iter().for_each(|&r| out.push(src.get_atom(r))),
            _ => {}
        }
    }

    fn finish(self) -> ColVec {
        match self {
            Self::Enum(t, inner) => ColVec::Enum(t, Box::new(inner.finish())),
            Self::Bool(v) => ColVec::Bool(Attr::None, Arc::new(v)),
            Self::Guid(v) => ColVec::Guid(Attr::None, Arc::new(v)),
            Self::Byte(v) => ColVec::Byte(Attr::None, Arc::new(v)),
            Self::Short(v) => ColVec::Short(Attr::None, Arc::new(v)),
            Self::Int(13, v) => ColVec::Month(Attr::None, Arc::new(v)),
            Self::Int(14, v) => ColVec::Date(Attr::None, Arc::new(v)),
            Self::Int(17, v) => ColVec::Minute(Attr::None, Arc::new(v)),
            Self::Int(18, v) => ColVec::Second(Attr::None, Arc::new(v)),
            Self::Int(19, v) => ColVec::Time(Attr::None, Arc::new(v)),
            Self::Int(_, v) => ColVec::Int(Attr::None, Arc::new(v)),
            Self::Long(12, v) => ColVec::Timestamp(Attr::None, Arc::new(v)),
            Self::Long(16, v) => ColVec::Timespan(Attr::None, Arc::new(v)),
            Self::Long(_, v) => ColVec::Long(Attr::None, Arc::new(v)),
            Self::Real(v) => ColVec::Real(Attr::None, Arc::new(v)),
            Self::Float(15, v) => ColVec::Datetime(Attr::None, Arc::new(v)),
            Self::Float(_, v) => ColVec::Float(Attr::None, Arc::new(v)),
            Self::Char(v) => ColVec::Char(Attr::None, Arc::new(v)),
            Self::Sym(v) => ColVec::Sym(Attr::None, Arc::new(v)),
            Self::Strings(v) => ColVec::of_strings(Attr::None, v),
            Self::General(v) => ColVec::General(Attr::None, Arc::new(v)),
        }
    }
}

enum LeafAcc {
    SumLong { timespan: bool, vals: Vec<i64> },
    SumFloat(Vec<f64>),
    PrdLong(Vec<i64>),
    PrdFloat(Vec<f64>),
    Wsum(Vec<f64>),
    Ratio(Vec<(f64, f64)>),
    Var { sample: bool, root: bool, states: Vec<(f64, f64, f64)> },
    ExtBool { is_min: bool, vals: Vec<bool> },
    ExtU8 { is_char: bool, is_min: bool, vals: Vec<(bool, u8)> },
    ExtI16 { is_min: bool, vals: Vec<(bool, i16)> },
    ExtI32 { tnum: i16, is_min: bool, vals: Vec<(bool, i32)> },
    ExtI64 { tnum: i16, is_min: bool, vals: Vec<(bool, i64)> },
    ExtF32 { is_min: bool, vals: Vec<(bool, f32)> },
    ExtF64 { tnum: i16, is_min: bool, vals: Vec<(bool, f64)> },
    ExtSym { is_min: bool, vals: Vec<(bool, SymId)> },
    ExtAtom { proto_null: K, is_min: bool, vals: Vec<Option<K>> },
}

impl LeafAcc {
    fn new(kind: Leaf, at: usize, partials: &[ColVec]) -> Self {
        let c0 = partials[at].plain();
        match kind {
            Leaf::Count => Self::SumLong { timespan: false, vals: Vec::new() },
            Leaf::Sum => match c0 {
                ColVec::Float(..) | ColVec::Real(..) => Self::SumFloat(Vec::new()),
                ColVec::Timespan(..) => Self::SumLong { timespan: true, vals: Vec::new() },
                _ => Self::SumLong { timespan: false, vals: Vec::new() },
            },
            Leaf::Prd => match c0 {
                ColVec::Float(..) | ColVec::Real(..) => Self::PrdFloat(Vec::new()),
                _ => Self::PrdLong(Vec::new()),
            },
            Leaf::Wsum => Self::Wsum(Vec::new()),
            Leaf::Avg | Leaf::Wavg => Self::Ratio(Vec::new()),
            Leaf::Var { sample, root } => Self::Var { sample, root, states: Vec::new() },
            Leaf::Min | Leaf::Max => {
                let is_min = kind == Leaf::Min;
                match c0 {
                    ColVec::Bool(..) => Self::ExtBool { is_min, vals: Vec::new() },
                    ColVec::Byte(..) => Self::ExtU8 { is_char: false, is_min, vals: Vec::new() },
                    ColVec::Char(..) => Self::ExtU8 { is_char: true, is_min, vals: Vec::new() },
                    ColVec::Short(..) => Self::ExtI16 { is_min, vals: Vec::new() },
                    ColVec::Int(..) | ColVec::Month(..) | ColVec::Date(..) | ColVec::Minute(..) | ColVec::Second(..) | ColVec::Time(..) => {
                        Self::ExtI32 { tnum: c0.type_num(), is_min, vals: Vec::new() }
                    }
                    ColVec::Long(..) | ColVec::Timestamp(..) | ColVec::Timespan(..) => {
                        Self::ExtI64 { tnum: c0.type_num(), is_min, vals: Vec::new() }
                    }
                    ColVec::Real(..) => Self::ExtF32 { is_min, vals: Vec::new() },
                    ColVec::Float(..) | ColVec::Datetime(..) => Self::ExtF64 { tnum: c0.type_num(), is_min, vals: Vec::new() },
                    ColVec::Sym(..) => Self::ExtSym { is_min, vals: Vec::new() },
                    other => Self::ExtAtom { proto_null: other.null_atom(), is_min, vals: Vec::new() },
                }
            }
        }
    }

    fn resize(&mut self, ngroups: usize) {
        match self {
            Self::SumLong { vals, .. } => vals.resize(ngroups, 0),
            Self::SumFloat(vals) | Self::Wsum(vals) => vals.resize(ngroups, 0.0),
            Self::PrdLong(vals) => vals.resize(ngroups, 1),
            Self::PrdFloat(vals) => vals.resize(ngroups, 1.0),
            Self::Ratio(vals) => vals.resize(ngroups, (0.0, 0.0)),
            Self::Var { states, .. } => states.resize(ngroups, (0.0, 0.0, 0.0)),
            Self::ExtBool { is_min, vals } => vals.resize(ngroups, *is_min),
            Self::ExtU8 { vals, .. } => vals.resize(ngroups, (false, 0)),
            Self::ExtI16 { vals, .. } => vals.resize(ngroups, (false, NULL_SHORT)),
            Self::ExtI32 { vals, .. } => vals.resize(ngroups, (false, NULL_INT)),
            Self::ExtI64 { vals, .. } => vals.resize(ngroups, (false, NULL_LONG)),
            Self::ExtF32 { vals, .. } => vals.resize(ngroups, (false, f32::NAN)),
            Self::ExtF64 { vals, .. } => vals.resize(ngroups, (false, f64::NAN)),
            Self::ExtSym { vals, .. } => vals.resize(ngroups, (false, SymId::EMPTY)),
            Self::ExtAtom { vals, .. } => vals.resize(ngroups, None),
        }
    }

    fn update(&mut self, at: usize, partials: &[ColVec], gids: &[u32]) {
        match self {
            Self::SumLong { vals, .. } => {
                let ns = NumSlice::new(&partials[at]);
                for (r, &g) in gids.iter().enumerate() {
                    let v = ns.i64_at(r);
                    if v != NULL_LONG {
                        vals[g as usize] = vals[g as usize].wrapping_add(v);
                    }
                }
            }
            Self::SumFloat(vals) | Self::Wsum(vals) => {
                let ns = NumSlice::new(&partials[at]);
                for (r, &g) in gids.iter().enumerate() {
                    let v = ns.f64_at(r);
                    if !v.is_nan() {
                        vals[g as usize] += v;
                    }
                }
            }
            Self::PrdLong(vals) => {
                let ns = NumSlice::new(&partials[at]);
                for (r, &g) in gids.iter().enumerate() {
                    let v = ns.i64_at(r);
                    if v != NULL_LONG {
                        vals[g as usize] = vals[g as usize].wrapping_mul(v);
                    }
                }
            }
            Self::PrdFloat(vals) => {
                let ns = NumSlice::new(&partials[at]);
                for (r, &g) in gids.iter().enumerate() {
                    let v = ns.f64_at(r);
                    if !v.is_nan() {
                        vals[g as usize] *= v;
                    }
                }
            }
            Self::Ratio(vals) => {
                let (num_s, den_s) = (NumSlice::new(&partials[at]), NumSlice::new(&partials[at + 1]));
                for (r, &g) in gids.iter().enumerate() {
                    let (num, den) = (num_s.f64_at(r), den_s.f64_at(r));
                    let slot = &mut vals[g as usize];
                    if !num.is_nan() {
                        slot.0 += num;
                    }
                    if !den.is_nan() {
                        slot.1 += den;
                    }
                }
            }
            Self::Var { states, .. } => {
                let (cnt_s, avg_s, var_s) = (
                    NumSlice::new(&partials[at]),
                    NumSlice::new(&partials[at + 1]),
                    NumSlice::new(&partials[at + 2]),
                );
                for (r, &g) in gids.iter().enumerate() {
                    let ni = cnt_s.f64_at(r);
                    if ni <= 0.0 || ni.is_nan() {
                        continue;
                    }
                    let (mi, vi) = (avg_s.f64_at(r), var_s.f64_at(r));
                    let (n, mean, m2) = &mut states[g as usize];
                    let (all, delta) = (*n + ni, mi - *mean);
                    *m2 += vi * ni + delta * delta * *n * ni / all;
                    *mean += delta * ni / all;
                    *n = all;
                }
            }
            Self::ExtBool { is_min, vals } => {
                if let ColVec::Bool(_, xs) = partials[at].plain() {
                    for (r, &g) in gids.iter().enumerate() {
                        if *is_min {
                            vals[g as usize] &= xs.get(r);
                        } else {
                            vals[g as usize] |= xs.get(r);
                        }
                    }
                }
            }
            Self::ExtU8 { is_char, is_min, vals } => {
                if let ColVec::Byte(_, xs) | ColVec::Char(_, xs) = partials[at].plain() {
                    for (r, &g) in gids.iter().enumerate() {
                        let v = xs[r];
                        if *is_char && v == NULL_CHAR {
                            continue;
                        }
                        let (seen, cur) = &mut vals[g as usize];
                        if !*seen || (*is_min && v < *cur) || (!*is_min && v > *cur) {
                            *cur = v;
                            *seen = true;
                        }
                    }
                }
            }
            Self::ExtI16 { is_min, vals } => {
                if let ColVec::Short(_, xs) = partials[at].plain() {
                    for (r, &g) in gids.iter().enumerate() {
                        let v = xs[r];
                        if v != NULL_SHORT {
                            let (seen, cur) = &mut vals[g as usize];
                            if !*seen || (*is_min && v < *cur) || (!*is_min && v > *cur) {
                                *cur = v;
                                *seen = true;
                            }
                        }
                    }
                }
            }
            Self::ExtI32 { is_min, vals, .. } => {
                if let ColVec::Int(_, xs)
                | ColVec::Month(_, xs)
                | ColVec::Date(_, xs)
                | ColVec::Minute(_, xs)
                | ColVec::Second(_, xs)
                | ColVec::Time(_, xs) = partials[at].plain()
                {
                    for (r, &g) in gids.iter().enumerate() {
                        let v = xs[r];
                        if v != NULL_INT {
                            let (seen, cur) = &mut vals[g as usize];
                            if !*seen || (*is_min && v < *cur) || (!*is_min && v > *cur) {
                                *cur = v;
                                *seen = true;
                            }
                        }
                    }
                }
            }
            Self::ExtI64 { is_min, vals, .. } => {
                if let ColVec::Long(_, xs) | ColVec::Timestamp(_, xs) | ColVec::Timespan(_, xs) = partials[at].plain() {
                    for (r, &g) in gids.iter().enumerate() {
                        let v = xs[r];
                        if v != NULL_LONG {
                            let (seen, cur) = &mut vals[g as usize];
                            if !*seen || (*is_min && v < *cur) || (!*is_min && v > *cur) {
                                *cur = v;
                                *seen = true;
                            }
                        }
                    }
                }
            }
            Self::ExtF32 { is_min, vals } => {
                if let ColVec::Real(_, xs) = partials[at].plain() {
                    for (r, &g) in gids.iter().enumerate() {
                        let v = xs[r];
                        if !v.is_nan() {
                            let (seen, cur) = &mut vals[g as usize];
                            if !*seen || (*is_min && v < *cur) || (!*is_min && v > *cur) {
                                *cur = v;
                                *seen = true;
                            }
                        }
                    }
                }
            }
            Self::ExtF64 { is_min, vals, .. } => {
                if let ColVec::Float(_, xs) | ColVec::Datetime(_, xs) = partials[at].plain() {
                    for (r, &g) in gids.iter().enumerate() {
                        let v = xs[r];
                        if !v.is_nan() {
                            let (seen, cur) = &mut vals[g as usize];
                            if !*seen || (*is_min && v < *cur) || (!*is_min && v > *cur) {
                                *cur = v;
                                *seen = true;
                            }
                        }
                    }
                }
            }
            Self::ExtSym { is_min, vals } => {
                if let ColVec::Sym(_, xs) = partials[at].plain() {
                    for (r, &g) in gids.iter().enumerate() {
                        let v = xs[r];
                        if !v.is_null() {
                            let (seen, cur) = &mut vals[g as usize];
                            if !*seen || (*is_min && v < *cur) || (!*is_min && v > *cur) {
                                *cur = v;
                                *seen = true;
                            }
                        }
                    }
                }
            }
            Self::ExtAtom { is_min, vals, .. } => {
                let cv = partials[at].plain();
                for (r, &g) in gids.iter().enumerate() {
                    let a = cv.get_atom(r);
                    if !a.is_null() {
                        let slot = &mut vals[g as usize];
                        let replace = match slot {
                            None => true,
                            Some(cur) => {
                                let ord = a.k_cmp(cur);
                                (*is_min && ord == Ordering::Less) || (!*is_min && ord == Ordering::Greater)
                            }
                        };
                        if replace {
                            *slot = Some(a);
                        }
                    }
                }
            }
        }
    }

    fn finish(self, order: &[usize]) -> ColVec {
        match self {
            Self::SumLong { timespan: true, vals } => ColVec::Timespan(Attr::None, Arc::new(order.iter().map(|&g| vals[g]).collect())),
            Self::SumLong { timespan: false, vals } | Self::PrdLong(vals) => {
                ColVec::Long(Attr::None, Arc::new(order.iter().map(|&g| vals[g]).collect()))
            }
            Self::SumFloat(vals) | Self::PrdFloat(vals) | Self::Wsum(vals) => {
                ColVec::Float(Attr::None, Arc::new(order.iter().map(|&g| vals[g]).collect()))
            }
            Self::Ratio(vals) => ColVec::Float(
                Attr::None,
                Arc::new(
                    order
                        .iter()
                        .map(|&g| {
                            let (num, den) = vals[g];
                            if den == 0.0 { f64::NAN } else { num / den }
                        })
                        .collect(),
                ),
            ),
            Self::Var { sample, root, states } => ColVec::Float(
                Attr::None,
                Arc::new(
                    order
                        .iter()
                        .map(|&g| {
                            let (n, _, m2) = states[g];
                            let v = if n == 0.0 || (sample && n < 2.0) { f64::NAN } else { m2 / if sample { n - 1.0 } else { n } };
                            if root { v.sqrt() } else { v }
                        })
                        .collect(),
                ),
            ),
            Self::ExtBool { vals, .. } => {
                let out: Bools = order.iter().map(|&g| vals[g]).collect();
                ColVec::Bool(Attr::None, Arc::new(out))
            }
            Self::ExtU8 { is_char, vals, .. } => {
                let empty = if is_char { NULL_CHAR } else { 0u8 };
                let out: Vec<u8> = order.iter().map(|&g| if vals[g].0 { vals[g].1 } else { empty }).collect();
                if is_char {
                    ColVec::Char(Attr::None, Arc::new(out))
                } else {
                    ColVec::Byte(Attr::None, Arc::new(out))
                }
            }
            Self::ExtI16 { vals, .. } => {
                ColVec::Short(Attr::None, Arc::new(order.iter().map(|&g| if vals[g].0 { vals[g].1 } else { NULL_SHORT }).collect()))
            }
            Self::ExtI32 { tnum, is_min, vals } => {
                let empty = if tnum == 6 { if is_min { INF_INT } else { NINF_INT } } else { NULL_INT };
                let out: Vec<i32> = order.iter().map(|&g| if vals[g].0 { vals[g].1 } else { empty }).collect();
                match tnum {
                    13 => ColVec::Month(Attr::None, Arc::new(out)),
                    14 => ColVec::Date(Attr::None, Arc::new(out)),
                    17 => ColVec::Minute(Attr::None, Arc::new(out)),
                    18 => ColVec::Second(Attr::None, Arc::new(out)),
                    19 => ColVec::Time(Attr::None, Arc::new(out)),
                    _ => ColVec::Int(Attr::None, Arc::new(out)),
                }
            }
            Self::ExtI64 { tnum, is_min, vals } => {
                let empty = if tnum == 7 { if is_min { INF_LONG } else { NINF_LONG } } else { NULL_LONG };
                let out: Vec<i64> = order.iter().map(|&g| if vals[g].0 { vals[g].1 } else { empty }).collect();
                match tnum {
                    12 => ColVec::Timestamp(Attr::None, Arc::new(out)),
                    16 => ColVec::Timespan(Attr::None, Arc::new(out)),
                    _ => ColVec::Long(Attr::None, Arc::new(out)),
                }
            }
            Self::ExtF32 { vals, .. } => {
                ColVec::Real(Attr::None, Arc::new(order.iter().map(|&g| if vals[g].0 { vals[g].1 } else { f32::NAN }).collect()))
            }
            Self::ExtF64 { tnum, is_min, vals } => {
                let empty = if tnum == 9 { if is_min { f64::INFINITY } else { f64::NEG_INFINITY } } else { f64::NAN };
                let out: Vec<f64> = order.iter().map(|&g| if vals[g].0 { vals[g].1 } else { empty }).collect();
                if tnum == 15 {
                    ColVec::Datetime(Attr::None, Arc::new(out))
                } else {
                    ColVec::Float(Attr::None, Arc::new(out))
                }
            }
            Self::ExtSym { vals, .. } => {
                ColVec::Sym(Attr::None, Arc::new(order.iter().map(|&g| if vals[g].0 { vals[g].1 } else { SymId::EMPTY }).collect()))
            }
            Self::ExtAtom { proto_null, vals, .. } => {
                ColVec::from_atoms(order.iter().map(|&g| vals[g].clone().unwrap_or_else(|| proto_null.clone())).collect())
            }
        }
    }
}

#[derive(Default)]
enum GroupIndex {
    #[default]
    Uninit,
    One(HashMap<u64, u32>),
    Two(HashMap<(u64, u64), u32>),
    Bytes(HashMap<Vec<u8>, u32>),
}

/// Incremental columnar merger for two-phase shard partial states.
pub struct Merger<'a> {
    aggs: &'a Aggs,
    seen_schema: bool,
    nkeys: usize,
    key_cols: Vec<SymId>,
    part_cols: Vec<SymId>,
    ngroups: usize,
    index: GroupIndex,
    keys: Vec<KeyColAcc>,
    leaves: Vec<LeafAcc>,
    gids: Vec<u32>,
    new_rows: Vec<usize>,
}

impl<'a> Merger<'a> {
    pub fn new(aggs: &'a Aggs) -> Self {
        Self {
            aggs,
            seen_schema: false,
            nkeys: 0,
            key_cols: Vec::new(),
            part_cols: Vec::new(),
            ngroups: 0,
            index: GroupIndex::Uninit,
            keys: Vec::new(),
            leaves: Vec::new(),
            gids: Vec::new(),
            new_rows: Vec::new(),
        }
    }

    pub fn push(&mut self, part: Flip, nkeys: usize) -> KResult<()> {
        if !self.seen_schema {
            self.seen_schema = true;
            self.nkeys = nkeys;
            self.key_cols = part.cols[..nkeys].to_vec();
            self.part_cols = part.cols[nkeys..].to_vec();
        } else if nkeys != self.nkeys || part.cols[..nkeys] != self.key_cols || part.cols[nkeys..] != self.part_cols {
            return Err(KError::new("mismatch", "the shards returned different columns"));
        }

        let nrows = part.nrows();
        if nrows == 0 {
            return Ok(());
        }

        let key_vals = &part.vals[..nkeys];
        let part_vals = &part.vals[nkeys..];

        if self.leaves.is_empty() && !self.aggs.leaves.is_empty() {
            self.keys = key_vals.iter().map(KeyColAcc::from_proto).collect();
            self.leaves = self.aggs.leaves.iter().map(|&(kind, at)| LeafAcc::new(kind, at, part_vals)).collect();
            if nkeys == 1 && key_vals[0].key_bits().is_some() {
                self.index = GroupIndex::One(HashMap::new());
            } else if nkeys == 2 && key_vals[0].key_bits().is_some() && key_vals[1].key_bits().is_some() {
                self.index = GroupIndex::Two(HashMap::new());
            } else if nkeys > 0 {
                self.index = GroupIndex::Bytes(HashMap::new());
            }
        }

        self.gids.clear();
        self.gids.reserve(nrows);
        self.new_rows.clear();

        if nkeys == 0 {
            if self.ngroups == 0 {
                self.ngroups = 1;
            }
            self.gids.resize(nrows, 0);
        } else {
            match &mut self.index {
                GroupIndex::One(map) => {
                    let bits = key_vals[0].key_bits().unwrap();
                    for (r, &k) in bits.iter().enumerate() {
                        let next = self.ngroups as u32;
                        let gid = *map.entry(k).or_insert_with(|| {
                            self.ngroups += 1;
                            self.new_rows.push(r);
                            next
                        });
                        self.gids.push(gid);
                    }
                }
                GroupIndex::Two(map) => {
                    let (b0, b1) = (key_vals[0].key_bits().unwrap(), key_vals[1].key_bits().unwrap());
                    for (r, (&k0, &k1)) in b0.iter().zip(b1.iter()).enumerate() {
                        let next = self.ngroups as u32;
                        let gid = *map.entry((k0, k1)).or_insert_with(|| {
                            self.ngroups += 1;
                            self.new_rows.push(r);
                            next
                        });
                        self.gids.push(gid);
                    }
                }
                GroupIndex::Bytes(map) => {
                    let bit_cols: Vec<_> = key_vals.iter().map(ColVec::key_bits).collect();
                    for r in 0..nrows {
                        let mut key = Vec::with_capacity(nkeys * 8);
                        for (ci, cv) in key_vals.iter().enumerate() {
                            if let Some(bits) = &bit_cols[ci] {
                                key.extend_from_slice(&bits[r].to_le_bytes());
                            } else if let ColVec::Strings(_, s) = cv.plain() {
                                let bs = s.get(r);
                                key.extend_from_slice(&(bs.len() as u32).to_le_bytes());
                                key.extend_from_slice(bs);
                            } else {
                                let hk = cv.get_atom(r).hash_key();
                                key.extend_from_slice(&(hk.len() as u32).to_le_bytes());
                                key.extend_from_slice(&hk);
                            }
                        }
                        let next = self.ngroups as u32;
                        let gid = *map.entry(key).or_insert_with(|| {
                            self.ngroups += 1;
                            self.new_rows.push(r);
                            next
                        });
                        self.gids.push(gid);
                    }
                }
                GroupIndex::Uninit => unreachable!(),
            }
            for (kc, cv) in self.keys.iter_mut().zip(key_vals) {
                kc.push_rows(cv, &self.new_rows);
            }
        }

        for ((_, at), acc) in self.aggs.leaves.iter().zip(&mut self.leaves) {
            acc.resize(self.ngroups);
            acc.update(*at, part_vals, &self.gids);
        }
        Ok(())
    }

    pub fn finish(self, grouped: bool) -> Option<(Flip, Vec<K>)> {
        if !self.seen_schema || self.ngroups == 0 {
            return None;
        }
        if !grouped {
            let leaves = self.leaves.into_iter().map(|l| l.finish(&[0]).get_atom(0)).collect();
            return Some((Flip { cols: vec![], vals: vec![] }, leaves));
        }
        let first_keys: Vec<ColVec> = self.keys.into_iter().map(KeyColAcc::finish).collect();
        let order = sort_order(&first_keys, self.ngroups);
        let mut keys = Flip {
            cols: self.key_cols,
            vals: first_keys.iter().map(|c| c.gather(&order)).collect(),
        };
        if self.nkeys == 1 {
            keys.vals[0].set_attr_unchecked(Attr::Sorted);
        }
        let leaves = self.leaves.into_iter().map(|l| K::Vector(l.finish(&order))).collect();
        Some((keys, leaves))
    }
}
