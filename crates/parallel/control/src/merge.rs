//! Two-phase aggregation: what each shard computes for an aggregate, and how the control node puts the shards' partial
//! states together.
use kixdb::ast::{ColSpec, Expr};
use kixdb::engine::ops::aggregate_rows;
use kixdb::sym::{intern, resolve, SymId};
use kixdb::{ColVec, KResult, K};

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
        Expr::Unary { op, arg } if UNARY.contains(&resolve(*op).as_str()) => Some(Tree::Unary(*op, Box::new(tree(c, out, arg)?))),
        Expr::Unary { op, arg } => single(c, out, &resolve(*op), arg),
        Expr::Apply { target, args } => match (&**target, args.as_slice()) {
            (Expr::Var(f), [Some(a)]) => single(c, out, &resolve(*f), a),
            (Expr::Var(f), [Some(a), Some(b)]) if matches!(resolve(*f).as_str(), "wsum" | "wavg") => weighted(c, out, &resolve(*f), a, b),
            _ => None,
        },
        Expr::Binary { op, lhs, rhs } => match resolve(*op).as_str() {
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

fn floats(values: &ColVec, rows: &[usize]) -> Vec<f64> {
    rows.iter().map(|&r| values.get_atom(r).as_f64().unwrap_or(f64::NAN)).collect()
}

fn total(values: &ColVec, rows: &[usize]) -> f64 {
    floats(values, rows).into_iter().filter(|v| !v.is_nan()).sum()
}

impl Aggs {
    pub fn leaf_count(&self) -> usize {
        self.leaves.len()
    }

    /// Aggregate `leaf` of every group, in the order given: `partial[c]` holds partial column `c` of all shards' answers,
    /// and a group lists its rows there.
    pub fn merge(&self, leaf: usize, partial: &[ColVec], groups: &[Vec<usize>]) -> KResult<Vec<K>> {
        groups.iter().map(|g| self.merge_group(leaf, partial, g)).collect()
    }

    fn merge_group(&self, leaf: usize, partial: &[ColVec], rows: &[usize]) -> KResult<K> {
        let (kind, at) = self.leaves[leaf];
        let col = |i: usize| &partial[at + i];
        let combine = |verb: &str| aggregate_rows(verb, col(0), rows);
        Ok(match kind {
            Leaf::Sum | Leaf::Count => combine("sum")?,
            Leaf::Min => combine("min")?,
            Leaf::Max => combine("max")?,
            Leaf::Prd => combine("prd")?,
            Leaf::Wsum => K::Float(total(col(0), rows)),
            Leaf::Avg | Leaf::Wavg => {
                let (num, den) = (total(col(0), rows), total(col(1), rows));
                K::Float(if den == 0.0 { f64::NAN } else { num / den })
            }
            Leaf::Var { sample, root } => {
                // Chan's method for combining counts, means and sums of squared deviations
                let (mut n, mut mean, mut m2) = (0.0f64, 0.0f64, 0.0f64);
                let [counts, means, variances] = [0, 1, 2].map(|i| floats(col(i), rows));
                for ((ni, mi), vi) in counts.into_iter().zip(means).zip(variances) {
                    if ni == 0.0 {
                        continue;
                    }
                    let (all, delta) = (n + ni, mi - mean);
                    m2 += vi * ni + delta * delta * n * ni / all;
                    mean += delta * ni / all;
                    n = all;
                }
                let v = if n == 0.0 || (sample && n < 2.0) { f64::NAN } else { m2 / if sample { n - 1.0 } else { n } };
                K::Float(if root { v.sqrt() } else { v })
            }
        })
    }

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
