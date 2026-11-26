//! q parse trees: the value `parse` gives for source text and `eval` (and the functional forms `?[t;c;b;a]` and
//! `![t;c;b;a]`) read back.
//!
//! A tree is a general list `(verb; arg; ...)` whose items are trees. A symbol names a variable, so a symbol constant is
//! enlisted (`` ,`a ``), as is a constant symbol vector or general list. Any other value stands for itself. The `by`
//! and column arguments of a functional query are the exception: they are trees taken as they are.
//! Operators applied to one argument carry a `:` (``(-:;`x)`` negates; ``(-;`x)`` is a projection). A table literal is
//! `` (`.kix.table; keynames; valuenames; column...) ``.
use crate::ast::{ColSpec, Expr, QSql, QSqlKind};
use crate::error::{KError, KResult};
use crate::parser::{infer_col_name, is_builtin_verb, is_unary_keyword};
use crate::sym::{intern, resolve, SymId};
use crate::types::*;
use std::sync::Arc;

use super::qsql::resolve_col_names;

const OPERATORS: [&str; 22] = [
    "+", "-", "*", "%", "&", "|", "^", "=", "<", ">", "<=", ">=", "<>", "~", ",", "!", "#", "_", "$", "?", "@", ".",
];
const TABLE: &str = ".kix.table";

fn is_operator(s: &str) -> bool {
    OPERATORS.contains(&s)
}

fn adverb_word(kind: AdverbKind) -> &'static str {
    match kind {
        // (`'` signals, so the adverb goes by its word)
        AdverbKind::Each => "each",
        other => other.symbol(),
    }
}

fn adverb_named(s: &str) -> Option<AdverbKind> {
    [AdverbKind::Each, AdverbKind::Over, AdverbKind::Scan, AdverbKind::EachPrior, AdverbKind::EachRight, AdverbKind::EachLeft]
        .into_iter()
        .find(|&k| adverb_word(k) == s)
}

fn prim(name: &str) -> K {
    K::Func(Func::Prim(intern(name)))
}

/// A general list (never collapsed to a typed vector, so that a list of names stays an application).
fn list(items: Vec<K>) -> K {
    K::Vector(ColVec::General(Attr::None, Arc::new(items)))
}

fn symbols(names: Vec<SymId>) -> K {
    K::Vector(ColVec::Sym(Attr::None, Arc::new(names)))
}

/// A constant as it appears in a tree.
fn constant(k: &K) -> K {
    match k {
        K::Sym(s) => symbols(vec![*s]),
        K::Vector(ColVec::Sym(..)) => list(vec![k.clone()]),
        K::Vector(ColVec::General(_, items)) if !items.is_empty() => list(vec![k.clone()]),
        other => other.clone(),
    }
}

/// The function a source expression denotes by itself (`+`, `count`, `+/`, `{x}`).
pub fn func_of(e: &Expr, ns: &str) -> Option<Func> {
    match e {
        Expr::Lit(K::Func(f)) => Some(f.clone()),
        Expr::Var(s) if is_builtin_verb(resolve(*s)) => Some(Func::Prim(*s)),
        Expr::Adverb { kind, func } => Some(Func::Adverb { kind: *kind, inner: Box::new(func_of(func, ns)?) }),
        Expr::Lambda { params, implicit, body, source } => Some(Func::Lambda(Arc::new(Lambda {
            params: params.clone(),
            implicit: *implicit,
            body: body.clone(),
            source: source.clone(),
            ns: ns.to_string(),
        }))),
        _ => None,
    }
}

fn dict(specs: &[ColSpec], ns: &str) -> KResult<K> {
    let trees = specs.iter().map(|s| to_tree(&s.expr, ns)).collect::<KResult<Vec<_>>>()?;
    Ok(K::Dict(Arc::new(Dict { keys: Box::new(symbols(resolve_col_names(specs))), vals: Box::new(list(trees)) })))
}

/// The functional form of a qSQL statement.
fn query_tree(q: &QSql, ns: &str) -> KResult<K> {
    let from = to_tree(&q.from, ns)?;
    let nothing = || list(vec![]);
    let wheres = if q.where_clauses.is_empty() {
        nothing()
    } else {
        list(vec![list(q.where_clauses.iter().map(|w| to_tree(w, ns)).collect::<KResult<_>>()?)])
    };
    // `exec` of one unnamed expression takes the expression itself rather than a dictionary
    let alone = |specs: &[ColSpec]| match specs {
        [one] if one.name.is_none() => Some(to_tree(&one.expr, ns)),
        _ => None,
    };
    Ok(match q.kind {
        QSqlKind::Select => {
            let by = match &q.by {
                Some(by) => dict(by, ns)?,
                None => K::Bool(q.distinct),
            };
            let cols = if q.cols.is_empty() { nothing() } else { dict(&q.cols, ns)? };
            let mut items = vec![prim("?"), from, wheres, by, cols];
            if let Some(limit) = &q.limit {
                items.push(to_tree(limit, ns)?);
            }
            list(items)
        }
        QSqlKind::Exec => {
            let by = match &q.by {
                Some(by) => alone(by).unwrap_or_else(|| dict(by, ns))?,
                None => nothing(),
            };
            let cols = match alone(&q.cols) {
                Some(one) => one?,
                None if q.cols.is_empty() => nothing(),
                None => dict(&q.cols, ns)?,
            };
            list(vec![prim("?"), from, wheres, by, cols])
        }
        QSqlKind::Update => {
            let by = match &q.by {
                Some(by) => dict(by, ns)?,
                None => K::Bool(false),
            };
            list(vec![prim("!"), from, wheres, by, dict(&q.cols, ns)?])
        }
        QSqlKind::DeleteRows => list(vec![prim("!"), from, wheres, K::Bool(false), symbols(vec![])]),
        QSqlKind::DeleteCols => {
            let names = q.cols.iter().filter_map(|c| c.name.or_else(|| infer_col_name(&c.expr))).collect();
            list(vec![prim("!"), from, wheres, K::Bool(false), symbols(names)])
        }
    })
}

/// The parse tree of an expression, in namespace `ns`.
pub fn to_tree(e: &Expr, ns: &str) -> KResult<K> {
    let tree = |x: &Expr| to_tree(x, ns);
    let apply = |head: K, rest: &[Expr]| -> KResult<K> {
        let mut items = vec![head];
        for x in rest {
            items.push(tree(x)?);
        }
        Ok(list(items))
    };
    Ok(match e {
        Expr::Nop => K::Null,
        Expr::Lit(k) => constant(k),
        Expr::Var(s) if is_builtin_verb(resolve(*s)) => K::Func(Func::Prim(*s)),
        Expr::Var(s) => K::Sym(*s),
        Expr::Field(base, field) => match tree(base)? {
            K::Sym(b) => K::sym(&format!("{b}.{field}")),
            other => list(vec![prim("@"), other, symbols(vec![*field])]),
        },
        Expr::List(items) => apply(prim("enlist"), items)?,
        Expr::TableLit { keys, vals } => {
            let names = |side: &[(SymId, Expr)]| symbols(side.iter().map(|c| c.0).collect());
            let mut items = vec![prim(TABLE), list(vec![names(keys)]), list(vec![names(vals)])];
            for (_, x) in keys.iter().chain(vals) {
                items.push(tree(x)?);
            }
            list(items)
        }
        Expr::Unary { op, arg } => {
            let name = resolve(*op);
            let head = if is_operator(name) { prim(&format!("{name}:")) } else { K::Func(Func::Prim(*op)) };
            list(vec![head, tree(arg)?])
        }
        Expr::Binary { op, lhs, rhs } => list(vec![K::Func(Func::Prim(*op)), tree(lhs)?, tree(rhs)?]),
        Expr::Apply { target, args } => {
            let mut items = vec![tree(target)?];
            for a in args {
                items.push(match a {
                    Some(a) => tree(a)?,
                    None => K::Null,
                });
            }
            list(items)
        }
        Expr::Juxt { func, arg } => list(vec![tree(func)?, tree(arg)?]),
        Expr::Adverb { kind, func } => match func_of(e, ns) {
            Some(f) => K::Func(f),
            None => list(vec![prim(adverb_word(*kind)), tree(func)?]),
        },
        Expr::Lambda { .. } => K::Func(func_of(e, ns).ok_or_else(|| KError::nyi("lambda"))?),
        Expr::Assign { name, global, op, val } => {
            let head = match op {
                Some(op) => format!("{op}:"),
                None if *global => "::".into(),
                None => ":".into(),
            };
            list(vec![prim(&head), K::Sym(*name), tree(val)?])
        }
        Expr::IndexAssign { name, indices, op, val } => {
            let mut target = vec![K::Sym(*name)];
            for i in indices {
                target.push(match i {
                    Some(i) => tree(i)?,
                    None => K::Null,
                });
            }
            let head = op.map(|op| format!("{op}:")).unwrap_or_else(|| ":".into());
            list(vec![prim(&head), list(target), tree(val)?])
        }
        Expr::Cond(parts) => apply(prim("$"), parts)?,
        Expr::If { cond, body } | Expr::While { cond, body } | Expr::Do { count: cond, body } => {
            let word = match e {
                Expr::If { .. } => "if",
                Expr::While { .. } => "while",
                _ => "do",
            };
            let mut items = vec![prim(word), tree(cond)?];
            for b in body {
                items.push(tree(b)?);
            }
            list(items)
        }
        Expr::Return(x) => list(vec![prim(":"), tree(x)?]),
        Expr::Signal(x) => list(vec![prim("'"), tree(x)?]),
        Expr::Query(q) => query_tree(q, ns)?,
        Expr::SysCmd { cmd, arg } => list(vec![prim("system"), K::string(format!("{cmd} {arg}").trim_end())]),
    })
}

/// The expression a parse tree stands for.
pub fn to_expr(k: &K) -> KResult<Expr> {
    let items = match k {
        K::Sym(s) => {
            let name = resolve(*s);
            // `a.b` is field `b` of `a`; a leading dot is a namespace
            return Ok(match name.split_once('.') {
                Some((base, fields)) if !base.is_empty() => {
                    fields.split('.').fold(Expr::Var(intern(base)), |e, f| Expr::Field(Box::new(e), intern(f)))
                }
                _ => Expr::Var(*s),
            });
        }
        K::Vector(ColVec::Sym(_, xs)) if xs.len() == 1 => return Ok(Expr::Lit(K::Sym(xs[0]))),
        K::Vector(ColVec::General(_, items)) if items.len() == 1 && matches!(items[0], K::Vector(_)) => {
            return Ok(Expr::Lit(items[0].clone()));
        }
        K::Vector(ColVec::General(_, items)) if items.len() > 1 => items,
        other => return Ok(Expr::Lit(other.clone())),
    };
    let (head, rest) = (&items[0], &items[1..]);
    let all = |xs: &[K]| xs.iter().map(to_expr).collect::<KResult<Vec<_>>>();
    let boxed = |x: &K| to_expr(x).map(Box::new);
    let applied = |target: Expr| -> KResult<Expr> {
        Ok(Expr::Apply { target: Box::new(target), args: all(rest)?.into_iter().map(Some).collect() })
    };
    let K::Func(Func::Prim(op)) = head else { return applied(to_expr(head)?) };
    let name = resolve(*op);
    // the groupings and columns of a functional query are trees given as they are, not to be evaluated first
    if matches!(name, "?" | "!") && rest.len() >= 4 {
        let args = rest.iter().enumerate().map(|(i, x)| match i {
            2 | 3 => Ok(Some(Expr::Lit(x.clone()))),
            _ => to_expr(x).map(Some),
        });
        return Ok(Expr::Apply { target: Box::new(Expr::Lit(head.clone())), args: args.collect::<KResult<_>>()? });
    }
    // `+:` and friends: an operator before a colon
    let base = name.strip_suffix(':').filter(|b| is_operator(b));
    Ok(match (name, rest) {
        (":", [x]) => Expr::Return(boxed(x)?),
        ("'", [x]) => Expr::Signal(boxed(x)?),
        (":" | "::", [K::Sym(target), val]) => {
            Expr::Assign { name: *target, global: name == "::", op: None, val: boxed(val)? }
        }
        (_, [K::Sym(target), val]) if base.is_some() => {
            Expr::Assign { name: *target, global: false, op: base.map(intern), val: boxed(val)? }
        }
        (_, [K::Vector(ColVec::General(_, target)), val]) if (name == ":" || base.is_some()) && matches!(target.first(), Some(K::Sym(_))) => {
            let Some(K::Sym(target_name)) = target.first() else { return Err(KError::ty("bad assignment tree")) };
            let indices = all(&target[1..])?.into_iter().map(Some).collect();
            Expr::IndexAssign { name: *target_name, indices, op: base.map(intern), val: boxed(val)? }
        }
        ("$", [_, _, _, ..]) => Expr::Cond(all(rest)?),
        ("if", [c, body @ ..]) => Expr::If { cond: boxed(c)?, body: all(body)? },
        ("while", [c, body @ ..]) => Expr::While { cond: boxed(c)?, body: all(body)? },
        ("do", [c, body @ ..]) => Expr::Do { count: boxed(c)?, body: all(body)? },
        ("enlist", [_, _, ..]) => Expr::List(all(rest)?),
        (TABLE, [keys, vals, columns @ ..]) => {
            let names = |side: &K| match to_expr(side)? {
                Expr::Lit(K::Vector(ColVec::Sym(_, xs))) => Ok((*xs).clone()),
                _ => Err(KError::ty("bad table tree")),
            };
            let (keys, vals) = (names(keys)?, names(vals)?);
            if keys.len() + vals.len() != columns.len() {
                return Err(KError::length("table tree names and columns differ in count"));
            }
            let mut columns = all(columns)?.into_iter();
            let keys = keys.into_iter().zip(columns.by_ref()).collect();
            Expr::TableLit { keys, vals: vals.into_iter().zip(columns).collect() }
        }
        (word, [x]) if adverb_named(word).is_some() => {
            Expr::Adverb { kind: adverb_named(word).unwrap_or(AdverbKind::Each), func: boxed(x)? }
        }
        (_, [x]) if base.is_some() => Expr::Unary { op: intern(base.unwrap_or_default()), arg: boxed(x)? },
        (_, [x]) if is_unary_keyword(name) => Expr::Unary { op: *op, arg: boxed(x)? },
        (_, [x, y]) => Expr::Binary { op: *op, lhs: boxed(x)?, rhs: boxed(y)? },
        _ => applied(Expr::Lit(head.clone()))?,
    })
}

/// The `where` argument of a functional query: a list of constraint trees.
pub fn where_clauses(c: &K) -> KResult<Vec<Expr>> {
    match c {
        K::Null => Ok(vec![]),
        K::Vector(cv) => (0..cv.len()).map(|i| to_expr(&cv.get_atom(i))).collect(),
        _ => Ok(vec![to_expr(c)?]),
    }
}

/// A `by` or column argument: a dictionary of names to trees, one tree (an unnamed column), or nothing.
pub fn col_specs(k: &K) -> KResult<Vec<ColSpec>> {
    match k {
        K::Dict(d) => {
            let names: Vec<SymId> = match &*d.keys {
                K::Vector(ColVec::Sym(_, names)) => (**names).clone(),
                K::Vector(cv) if cv.is_empty() => vec![],
                _ => return Err(KError::ty("functional query column dict keys must be symbols")),
            };
            names
                .into_iter()
                .enumerate()
                .map(|(i, name)| {
                    let value = super::ops::index_k(&d.vals, &[Some(K::Long(i as i64))])?;
                    Ok(ColSpec { name: Some(name), expr: to_expr(&value)? })
                })
                .collect()
        }
        K::Null => Ok(vec![]),
        K::Vector(cv) if cv.is_empty() => Ok(vec![]),
        one => Ok(vec![ColSpec { name: None, expr: to_expr(one)? }]),
    }
}
