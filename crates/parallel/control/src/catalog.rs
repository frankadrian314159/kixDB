//! What the control node knows about names: which are distributed tables (every table of the mirror is one), how each
//! is distributed, and what an expression refers to.
use kixdb::ast::{ColSpec, Expr, QSql, QSqlKind};
use kixdb::engine::{DistMode, Workspace};
use kixdb::parser::is_builtin_verb;
use kixdb::sym::{intern, resolve, SymId};
use kixdb::{ColVec, Connection, Flip, Func, KeyedTable, Server, K};
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

/// Verbs whose argument names things only at run time.
const DYNAMIC: [&str; 5] = ["value", "get", "eval", "reval", "system"];
/// Verbs with effects: an expression that uses one is never pushed down.
pub const EFFECTS: [&str; 12] = ["insert", "upsert", "set", "system", "value", "get", "eval", "hopen", "hclose", "show", ".Q.dpft", ".kix.dist"];
/// Verbs that need only a table's schema.
const SCHEMA_VERBS: [&str; 4] = ["cols", "keys", "meta", "type"];

#[derive(Clone)]
pub struct Table {
    pub name: SymId,
    /// The table with no rows, its attributes kept.
    pub schema: Flip,
    pub nkeys: usize,
    pub mode: DistMode,
}

impl Table {
    /// The columns whose values together decide a row's shard (none for a replicated table). Several are listed in name
    /// order, so that tables declared in either order are placed alike.
    pub fn dist_cols(&self) -> Vec<SymId> {
        match &self.mode {
            DistMode::Sharded { key } => vec![*key],
            DistMode::Partitioned { col } => vec![*col],
            DistMode::CoPartitioned { cols } => {
                let mut cols = cols.clone();
                cols.sort();
                cols
            }
            DistMode::Replicated => vec![],
        }
    }

    pub fn schema_k(&self) -> K {
        rekey(self.schema.clone(), self.nkeys)
    }
}

/// A flip as a table (`nkeys == 0`) or keyed table.
pub fn rekey(flip: Flip, nkeys: usize) -> K {
    if nkeys == 0 || nkeys > flip.ncols() {
        return K::Table(Arc::new(flip));
    }
    let keys = Flip { cols: flip.cols[..nkeys].to_vec(), vals: flip.vals[..nkeys].to_vec() };
    let vals = Flip { cols: flip.cols[nkeys..].to_vec(), vals: flip.vals[nkeys..].to_vec() };
    K::KeyedTable(Arc::new(KeyedTable { keys, vals }))
}

/// A table or keyed table as `(columns, number of key columns)`.
pub fn unkey(k: &K) -> Option<(Flip, usize)> {
    match k {
        K::Table(t) => Some(((**t).clone(), 0)),
        K::KeyedTable(kt) => Some((kt.unkey(), kt.keys.ncols())),
        _ => None,
    }
}

/// `rows` with the attributes of `like`, where they still hold.
pub fn with_attrs(mut rows: Flip, like: &Flip) -> Flip {
    for (c, v) in rows.cols.iter().zip(rows.vals.iter_mut()) {
        if let Some(a) = like.col(*c).map(ColVec::attr) {
            if let Ok(with) = v.with_attr(a) {
                *v = with;
            }
        }
    }
    rows
}

/// The type of the column `e` gives over a table with the columns of `schema`.
pub fn column_type(mirror: &mut Connection, schema: &Flip, e: &Expr) -> Option<i16> {
    // probed with one row of nulls: verbs given empty vectors may not keep the type
    let probe = Expr::Query(Box::new(QSql {
        kind: QSqlKind::Exec,
        distinct: false,
        limit: None,
        cols: vec![ColSpec { name: None, expr: e.clone() }],
        by: None,
        from: Box::new(Expr::Lit(K::Table(Arc::new(schema.gather(&[usize::MAX]))))),
        where_clauses: vec![],
    }));
    mirror.eval_expr(&probe).ok().map(|k| k.type_num().abs())
}

/// The empty version of a table, keeping column attributes.
pub fn schema_of(flip: &Flip) -> Flip {
    let mut empty = flip.take(0);
    for (v, full) in empty.vals.iter_mut().zip(&flip.vals) {
        v.set_attr_unchecked(full.attr());
    }
    empty
}

fn is_table(v: &K) -> bool {
    matches!(v, K::Table(_) | K::KeyedTable(_))
}

/// A variable that holds a distributed table. (One named like a built-in verb stays on the control node: the name means
/// the verb.)
fn is_distributed(ws: &Workspace, name: SymId) -> bool {
    ws.vars.get(&name).is_some_and(is_table) && !is_builtin_verb(&resolve(name))
}

/// Names as one session sees them (`ns` is its current namespace).
pub struct Names<'a> {
    pub mirror: &'a Server,
    pub ns: String,
}

/// What an expression refers to.
#[derive(Default)]
pub struct Refs {
    pub tables: BTreeSet<SymId>,
    /// Names are computed at run time: any table may be meant.
    pub dynamic: bool,
}

impl Refs {
    pub fn none(&self) -> bool {
        self.tables.is_empty() && !self.dynamic
    }
}

impl Names<'_> {
    /// The full name of a global that `name` means here, if one satisfies `want`.
    fn find(&self, ws: &Workspace, name: SymId, want: impl Fn(&Workspace, SymId) -> bool) -> Option<SymId> {
        let s = resolve(name);
        if !s.starts_with('.') && self.ns != "." && !self.ns.is_empty() {
            let q = intern(&format!("{}.{s}", self.ns));
            if want(ws, q) {
                return Some(q);
            }
        }
        want(ws, name).then_some(name)
    }

    /// The distributed table `name` means here.
    pub fn table(&self, name: SymId) -> Option<Table> {
        self.mirror.read_workspace(|ws| {
            let name = self.find(ws, name, is_distributed)?;
            let (schema, nkeys) = unkey(&ws.vars[&name])?;
            let mode = ws.dist.get(&name).cloned().unwrap_or(DistMode::Replicated);
            Some(Table { name, schema, nkeys, mode })
        })
    }

    pub fn all_tables(&self) -> Vec<SymId> {
        self.mirror.read_workspace(|ws| ws.vars.keys().copied().filter(|&n| is_distributed(ws, n)).collect())
    }

    /// The value of a global variable that is not a table or view.
    pub fn global(&self, name: SymId) -> Option<K> {
        self.mirror.read_workspace(|ws| {
            let name = self.find(ws, name, |ws, n| ws.vars.contains_key(&n))?;
            ws.vars.get(&name).filter(|v| !is_table(v)).cloned()
        })
    }

    pub fn refs(&self, e: &Expr) -> Refs {
        let mut out = Refs::default();
        self.walk(e, &mut out, &mut HashSet::new());
        out
    }

    fn mention(&self, name: SymId, out: &mut Refs, seen: &mut HashSet<SymId>) {
        if DYNAMIC.contains(&resolve(name).as_str()) {
            out.dynamic = true;
        }
        if let Some(t) = self.table(name) {
            out.tables.insert(t.name);
            return;
        }
        if !seen.insert(name) {
            return;
        }
        // a function or view brings what its body refers to
        let (value, view) = self.mirror.read_workspace(|ws| {
            let value = self.find(ws, name, |ws, n| ws.vars.contains_key(&n)).and_then(|n| ws.vars.get(&n).cloned());
            let view = self.find(ws, name, |ws, n| ws.views.contains_key(&n)).and_then(|n| ws.views.get(&n).map(|v| v.expr.clone()));
            (value, view)
        });
        if let Some(v) = value {
            self.walk_value(&v, out, seen);
        }
        if let Some(expr) = view {
            self.walk(&expr, out, seen);
        }
    }

    fn walk_value(&self, k: &K, out: &mut Refs, seen: &mut HashSet<SymId>) {
        match k {
            K::Func(f) => self.walk_func(f, out, seen),
            K::Vector(ColVec::General(_, xs)) => xs.iter().for_each(|x| self.walk_value(x, out, seen)),
            _ => {}
        }
    }

    fn walk_func(&self, f: &Func, out: &mut Refs, seen: &mut HashSet<SymId>) {
        match f {
            Func::Prim(s) => {
                if DYNAMIC.contains(&resolve(*s).as_str()) {
                    out.dynamic = true;
                }
            }
            Func::Lambda(l) => l.body.iter().for_each(|e| self.walk(e, out, seen)),
            Func::Adverb { inner, .. } => self.walk_func(inner, out, seen),
            Func::Proj { func, args, .. } => {
                self.walk_func(func, out, seen);
                args.iter().flatten().for_each(|a| self.walk_literal(a, out, seen));
            }
            Func::Comp(a, b) => {
                self.walk_func(a, out, seen);
                self.walk_func(b, out, seen);
            }
        }
    }

    /// A literal in source: a symbol there may name a table (`` `trade insert ... ``, functional queries).
    fn walk_literal(&self, k: &K, out: &mut Refs, seen: &mut HashSet<SymId>) {
        match k {
            K::Sym(s) => out.tables.extend(self.table(*s).map(|t| t.name)),
            K::Vector(ColVec::Sym(_, xs)) => out.tables.extend(xs.iter().filter_map(|s| self.table(*s)).map(|t| t.name)),
            K::Vector(ColVec::General(_, xs)) => xs.iter().for_each(|x| self.walk_literal(x, out, seen)),
            other => self.walk_value(other, out, seen),
        }
    }

    fn walk(&self, e: &Expr, out: &mut Refs, seen: &mut HashSet<SymId>) {
        match e {
            Expr::Var(s) => self.mention(*s, out, seen),
            Expr::Lit(k) => self.walk_literal(k, out, seen),
            Expr::Assign { name, .. } | Expr::IndexAssign { name, .. } => out.tables.extend(self.table(*name).map(|t| t.name)),
            Expr::Unary { op, .. } | Expr::Binary { op, .. } => out.dynamic |= DYNAMIC.contains(&resolve(*op).as_str()),
            Expr::Field(base, field) => {
                if let Expr::Var(b) = &**base {
                    self.mention(intern(&format!("{b}.{field}")), out, seen);
                }
            }
            Expr::SysCmd { .. } => out.dynamic = true,
            _ => {}
        }
        for c in e.children() {
            self.walk(c, out, seen);
        }
    }

    /// Whether the expression has no effects of its own (it assigns nothing and calls no verb with effects).
    pub fn is_pure(&self, e: &Expr) -> bool {
        let effect = |s: &SymId| EFFECTS.contains(&resolve(*s).as_str());
        !e.any(&mut |x| match x {
            Expr::Assign { .. } | Expr::IndexAssign { .. } | Expr::SysCmd { .. } | Expr::Do { .. } | Expr::While { .. } | Expr::If { .. } => true,
            Expr::Var(s) | Expr::Unary { op: s, .. } | Expr::Binary { op: s, .. } => effect(s),
            Expr::Lit(K::Func(Func::Prim(s))) => effect(s),
            _ => false,
        })
    }

    /// Whether the expression uses tables, and only their schemas.
    pub fn schema_only(&self, e: &Expr) -> bool {
        fn walk(n: &Names, e: &Expr, used: &mut bool) -> bool {
            if let Expr::Unary { op, arg } = e {
                if let Expr::Var(t) = &**arg {
                    let verb = resolve(*op);
                    // (`meta` reads the type of a general column from its items, which the mirror does not hold)
                    let answers = |t: &Table| verb != "meta" || t.schema.vals.iter().all(|v| v.type_num() != 0);
                    if SCHEMA_VERBS.contains(&verb.as_str()) && n.table(*t).is_some_and(|t| answers(&t)) {
                        *used = true;
                        return true;
                    }
                }
            }
            match e {
                Expr::Var(s) => n.table(*s).is_none() && n.global(*s).is_none(),
                Expr::Lit(K::Sym(_) | K::Func(_) | K::Vector(ColVec::Sym(..))) => false,
                _ => e.children().into_iter().all(|c| walk(n, c, used)),
            }
        }
        let mut used = false;
        self.is_pure(e) && walk(self, e, &mut used) && used
    }
}
