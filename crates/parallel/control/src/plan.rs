//! Deciding how a `select` / `exec` can run on the shards and still give exactly the one-engine answer.
//!
//! Rows of one value of a table's distribution column are all on one shard, in their original relative order, and every
//! row of a sharded table carries a global ordinal. So a query can be pushed down when the shards can answer parts that
//! the control node can put together without looking at the rows again: see [`Kind`].
use crate::catalog::{column_type, Names, Table, EFFECTS};
use crate::merge::{self, Aggs};
use kix_shared_ces::route::shard_of;
use kixdb::ast::{ColSpec, Expr, QSql, QSqlKind};
use kixdb::engine::qsql::resolve_col_names;
use kixdb::engine::DistMode;
use kixdb::parser::is_builtin_verb;
use kixdb::sym::{intern, resolve, SymId};
use kixdb::types::AdverbKind;
use kixdb::{ColVec, Connection, Flip, Func, K};
use std::cell::Cell;
use std::collections::{HashMap, HashSet};

/// Stands for the driving table in a relation's source until it is known how the shards are to read it.
pub const DRIVER: &str = "kix__driver";

/// How rows are placed: in the slot of the hash of one or more columns, or in that of a partition value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Layout {
    Hash(Vec<SymId>),
    Part(SymId),
}

impl Layout {
    pub fn of(t: &Table) -> Option<Layout> {
        match t.mode {
            DistMode::Partitioned { col } => Some(Layout::Part(col)),
            DistMode::Replicated => None,
            DistMode::Sharded { .. } | DistMode::CoPartitioned { .. } => Some(Layout::Hash(t.dist_cols())),
        }
    }

    /// The distribution columns.
    pub fn cols(&self) -> &[SymId] {
        match self {
            Layout::Hash(cols) => cols,
            Layout::Part(c) => std::slice::from_ref(c),
        }
    }

    /// The shard of a row with these values of the distribution columns.
    pub fn place(&self, values: &[K], slots: &[u32]) -> usize {
        shard_of(values, matches!(self, Layout::Part(_)), slots)
    }
}

/// A table copied into a temporary table on the shards before the query: to every shard (no `keys`) or to the shards
/// that own the hash of the `keys`.
#[derive(Clone)]
pub struct Move {
    pub source: Table,
    pub keys: Vec<SymId>,
    pub temp: SymId,
    /// The temporary table drives the result's order, so it keeps its ordinals.
    pub driver: bool,
}

/// The source of a query as the shards read it.
#[derive(Clone)]
pub struct Rel {
    /// Mentions the driving table as [`DRIVER`].
    pub expr: Expr,
    /// The shard table (possibly temporary) whose rows order the result.
    pub driver: SymId,
    /// `None`: every shard has everything.
    pub layout: Option<Layout>,
    pub cols: Vec<SymId>,
    pub nkeys: usize,
    pub moves: Vec<Move>,
}

pub enum Kind {
    /// Every table is replicated: one shard answers.
    Replicated,
    /// `where` clauses fix the distribution columns to values on one shard, which answers the whole query.
    Route(usize),
    /// Grouped by the distribution columns (those the filters do not fix to one value): no group spans shards, so the
    /// shards' answers are concatenated.
    ByKey,
    /// Row by row: the shards' rows are interleaved by ordinal.
    Ordered,
    /// Combinable aggregates: each shard computes partial states that the control node merges.
    TwoPhase(Aggs),
}

pub struct Push {
    pub kind: Kind,
    /// The query with free names replaced by their values and every result column named.
    pub query: QSql,
    pub rel: Rel,
    /// `exec` of one unnamed column gives that column, not a dictionary.
    pub exec_single: bool,
    /// The statement as written, for results that only depend on the schema.
    pub original: Expr,
}

impl Push {
    pub fn describe(&self) -> String {
        let moves: String = self
            .rel
            .moves
            .iter()
            .map(|m| {
                if m.keys.is_empty() {
                    return format!("broadcast {}; ", m.source.name);
                }
                let keys: Vec<String> = m.keys.iter().map(|&k| resolve(k).into()).collect();
                format!("shuffle {} by {}; ", m.source.name, keys.join(", "))
            })
            .collect();
        let how = match &self.kind {
            Kind::Replicated => "replicated: one shard".to_string(),
            Kind::Route(s) => format!("route: shard {s}"),
            Kind::ByKey => "scatter-gather: grouped by the distribution column".to_string(),
            Kind::Ordered if self.query.distinct => "scatter-gather: each shard's distinct rows merged in order".to_string(),
            Kind::Ordered => "scatter-gather: rows merged in order".to_string(),
            Kind::TwoPhase(_) => "scatter-gather: two-phase aggregation".to_string(),
        };
        moves + &how
    }
}

/// What names mean inside a query that is being pushed down.
struct Scope<'a> {
    cols: &'a HashSet<SymId>,
    /// The virtual row number `i` means the same on a shard (only where one shard holds all of a table).
    row_numbers: bool,
}

pub struct Planner<'a> {
    pub names: Names<'a>,
    pub mirror: &'a mut Connection,
    pub slots: &'a [u32],
    pub broadcast_limit: usize,
    /// The number of rows of a distributed table.
    pub rows: &'a dyn Fn(&Table) -> usize,
    pub next_temp: &'a dyn Fn() -> u64,
}

const ELEMENTWISE_UNARY: [&str; 22] = [
    "-", "neg", "abs", "sqrt", "exp", "log", "sin", "cos", "tan", "asin", "acos", "atan", "floor", "ceiling", "reciprocal",
    "signum", "null", "not", "upper", "lower", "trim", "string",
];
const ELEMENTWISE_BINARY: [&str; 16] = ["+", "-", "*", "%", "=", "<>", "<", "<=", ">", ">=", "&", "|", "and", "or", "div", "mod"];
const EACH: [&str; 2] = ["each", "peach"];
const PREDICATES: [&str; 13] = ["=", "<>", "<", "<=", ">", ">=", "&", "|", "and", "or", "in", "within", "like"];

fn is_one_of(s: SymId, set: &[&str]) -> bool {
    set.contains(&resolve(s))
}

/// Whether the expression's value does not depend on any row.
pub fn is_constant(e: &Expr, cols: &HashSet<SymId>) -> bool {
    let i = intern("i");
    match e {
        // a subquery has its own columns
        Expr::Query(_) => true,
        Expr::Var(s) => !cols.contains(s) && *s != i,
        other => other.children().into_iter().all(|c| is_constant(c, cols)),
    }
}

pub fn has_query(e: &Expr) -> bool {
    e.any(&mut |x| matches!(x, Expr::Query(_)))
}

/// Whether a query gives a single value (`exec max price from t`).
fn one_value(q: &QSql) -> bool {
    const AGGREGATES: [&str; 8] = ["sum", "count", "min", "max", "avg", "first", "last", "med"];
    q.kind == QSqlKind::Exec
        && q.by.is_none()
        && matches!(q.cols.as_slice(), [c] if c.name.is_none() && matches!(&c.expr, Expr::Unary { op, .. } if is_one_of(*op, &AGGREGATES)))
}

/// `distinct select ...`, `distinct exec c from t` or `exec distinct c from t` as a query marked `distinct`.
pub fn distinct_query(e: &Expr) -> Option<Expr> {
    let distinct = |op: &SymId| resolve(*op) == "distinct";
    let mut q = match e {
        Expr::Unary { op, arg } if distinct(op) => match &**arg {
            Expr::Query(q) if !q.cols.is_empty() => (**q).clone(),
            _ => return None,
        },
        Expr::Query(q) if q.kind == QSqlKind::Exec => match q.cols.as_slice() {
            [ColSpec { name: None, expr: Expr::Unary { op, arg } }] if distinct(op) => {
                let mut inner = (**q).clone();
                inner.cols[0].expr = (**arg).clone();
                inner
            }
            _ => return None,
        },
        _ => return None,
    };
    // rows, or the values of one column; groups and limits would come before `distinct`
    let listed = q.kind == QSqlKind::Select || q.kind == QSqlKind::Exec && matches!(q.cols.as_slice(), [c] if c.name.is_none());
    if !listed || q.by.is_some() || q.limit.is_some() {
        return None;
    }
    q.distinct = true;
    Some(Expr::Query(Box::new(q)))
}

/// The name of a built-in verb written as one.
fn verb_name(f: &Expr) -> Option<SymId> {
    match f {
        Expr::Lit(K::Func(Func::Prim(s))) => Some(*s),
        Expr::Var(s) => is_builtin_verb(resolve(*s)).then_some(*s),
        _ => None,
    }
}

/// A built-in verb, perhaps with adverbs (`#`, `,\:`). (A user's function is not: the types of its results could differ
/// from shard to shard.)
fn is_verb(f: &Expr) -> bool {
    match f {
        Expr::Adverb { func, .. } => is_verb(func),
        other => verb_name(other).is_some(),
    }
}

/// Whether the expression applies a verb item by item (`f each x`, `x f'y`).
pub fn has_adverb(e: &Expr) -> bool {
    e.any(&mut |x| matches!(x, Expr::Adverb { .. }) || matches!(x, Expr::Binary { op, .. } if is_one_of(*op, &EACH)))
}

/// Whether row `r` of the result depends only on row `r` of the columns (so each shard can compute its own rows). A
/// `fby` is allowed when its groups include all of `keys`: the distribution columns that differ between the rows.
pub fn rowwise(e: &Expr, cols: &HashSet<SymId>, keys: &[SymId]) -> bool {
    let rw = |x: &Expr| rowwise(x, cols, keys);
    match e {
        Expr::Lit(k) => k.is_atom(),
        Expr::Var(s) => cols.contains(s),
        Expr::Field(base, _) => matches!(&**base, Expr::Var(s) if cols.contains(s)),
        Expr::Unary { op, arg } => is_one_of(*op, &ELEMENTWISE_UNARY) && rw(arg),
        Expr::Apply { target, args } => {
            // applied to each row's item of a column
            let items = |x: &Expr| rw(x) && uses_column(x, cols);
            match (&**target, args.as_slice()) {
                (Expr::Var(f), [Some(a)]) => is_one_of(*f, &ELEMENTWISE_UNARY) && rw(a),
                (Expr::Adverb { kind, func }, [Some(a), rest @ ..]) if is_verb(func) => match (kind, rest) {
                    (AdverbKind::Each, []) => items(a),
                    // paired item by item, so what is not a column must be an atom
                    (AdverbKind::Each, [Some(b)]) => rw(a) && rw(b) && (uses_column(a, cols) || uses_column(b, cols)),
                    // the other argument is used whole for every row
                    (AdverbKind::EachLeft, [Some(b)]) => items(a) && is_constant(b, cols),
                    (AdverbKind::EachRight, [Some(b)]) => is_constant(a, cols) && items(b),
                    _ => false,
                },
                _ => false,
            }
        }
        Expr::Binary { op, lhs, rhs } => match resolve(*op) {
            "each" | "peach" => is_verb(lhs) && rw(rhs) && uses_column(rhs, cols),
            "in" | "within" | "like" => rw(lhs) && is_constant(rhs, cols),
            "xbar" | "$" => is_constant(lhs, cols) && rw(rhs),
            "^" | "xexp" | "xlog" => rw(lhs) && rw(rhs),
            "fby" => {
                let by_key = match &**rhs {
                    Expr::Var(s) => keys.iter().all(|k| k == s),
                    Expr::TableLit { keys: k, vals } => {
                        keys.iter().all(|key| k.iter().chain(vals).any(|(_, v)| matches!(v, Expr::Var(s) if s == key)))
                    }
                    _ => false,
                };
                by_key && matches!(&**lhs, Expr::List(pair) if pair.len() == 2 && is_constant(&pair[0], cols) && rw(&pair[1]))
            }
            _ => is_one_of(*op, &ELEMENTWISE_BINARY) && rw(lhs) && rw(rhs),
        },
        // a subquery stands for a constant that is filled in before the query is sent; elsewhere than in a set it must
        // be one value
        Expr::Query(q) => one_value(q),
        _ => false,
    }
}

fn uses_column(e: &Expr, cols: &HashSet<SymId>) -> bool {
    !is_constant(e, cols)
}

/// A `where` clause that keeps a row or not by looking at that row only.
pub fn filters_rows(e: &Expr, cols: &HashSet<SymId>, keys: &[SymId]) -> bool {
    let predicate = match e {
        Expr::Binary { op, .. } => is_one_of(*op, &PREDICATES),
        Expr::Unary { op, .. } => is_one_of(*op, &["not", "null", "~"]),
        Expr::Var(_) => true,
        // `c ~\: "text"`, `"text" ~/: c`: a comparison for each row
        Expr::Apply { target, .. } => {
            matches!(&**target, Expr::Adverb { func, .. } if verb_name(func).is_some_and(|v| resolve(v) == "~" || is_one_of(v, &PREDICATES)))
        }
        _ => false,
    };
    predicate && rowwise(e, cols, keys)
}

/// What the leading `where` clauses of a statement say about where its rows are.
pub struct Filtered {
    /// How many of them keep rows one by one (or by groups that a shard holds whole).
    pub leading: usize,
    /// The distribution columns that those clauses do not fix to one value.
    pub unpinned: Vec<SymId>,
    /// The one shard that holds every row those clauses can keep.
    pub route: Option<usize>,
}

/// The values a clause `column = value` or `column in values` allows a distribution column.
fn restriction(wc: &Expr, cols: &[SymId], schema: &Flip) -> Option<(SymId, Vec<K>)> {
    let Expr::Binary { op, lhs, rhs } = wc else { return None };
    let (column, value, set) = match (resolve(*op), &**lhs, &**rhs) {
        ("=", Expr::Var(c), Expr::Lit(v)) | ("=", Expr::Lit(v), Expr::Var(c)) if v.is_atom() => (*c, v, false),
        ("in", Expr::Var(c), Expr::Lit(v)) => (*c, v, true),
        _ => return None,
    };
    if !cols.contains(&column) {
        return None;
    }
    let values: Vec<K> = match value {
        K::Vector(cv) if set => (0..cv.len()).map(|i| cv.get_atom(i)).collect(),
        atom => vec![atom.clone()],
    };
    // only values that hash like the column's own say where its rows are
    let column_type = schema.col(column)?.type_num();
    values.iter().all(|v| v.is_atom() && same_family(-v.type_num(), column_type)).then_some((column, values))
}

/// The single shard of every combination of the columns' values, if there is one.
fn one_shard(layout: &Layout, fixed: &HashMap<SymId, Vec<K>>, slots: &[u32]) -> Option<usize> {
    let choices = layout.cols().iter().map(|c| fixed.get(c)).collect::<Option<Vec<_>>>()?;
    let combinations = choices.iter().try_fold(1usize, |n, v| n.checked_mul(v.len()))?;
    let mut found = None;
    for mut n in 0..combinations {
        let values: Vec<K> = choices
            .iter()
            .map(|v| {
                let value = v[n % v.len()].clone();
                n /= v.len();
                value
            })
            .collect();
        let shard = layout.place(&values, slots);
        if found.replace(shard).is_some_and(|other| other != shard) {
            return None;
        }
    }
    found
}

/// Follow the `where` clauses while each keeps rows without looking across shards, noting the distribution columns they
/// fix and the shard they route to.
pub fn scan_filters(
    clauses: &[Expr],
    cols: &HashSet<SymId>,
    layout: &Layout,
    schema: &Flip,
    slots: &[u32],
) -> Filtered {
    let mut out = Filtered { leading: 0, unpinned: layout.cols().to_vec(), route: None };
    let mut fixed: HashMap<SymId, Vec<K>> = HashMap::new();
    for w in clauses {
        if !filters_rows(w, cols, &out.unpinned) {
            break;
        }
        out.leading += 1;
        let Some((column, values)) = restriction(w, layout.cols(), schema) else { continue };
        if values.len() == 1 {
            out.unpinned.retain(|&c| c != column);
        }
        // every clause narrows the rows, so the shortest list of values for a column holds for what follows
        if fixed.get(&column).is_none_or(|known| values.len() < known.len()) {
            fixed.insert(column, values);
        }
        if out.route.is_none() {
            out.route = one_shard(layout, &fixed, slots);
        }
    }
    out
}

fn symbols(e: &Expr) -> Option<Vec<SymId>> {
    match e {
        Expr::Lit(K::Sym(s)) => Some(vec![*s]),
        Expr::Lit(K::Vector(ColVec::Sym(_, xs))) => Some((**xs).clone()),
        _ => None,
    }
}

/// Values of these types hash and compare alike.
fn same_family(a: i16, b: i16) -> bool {
    let integer = |t: i16| matches!(t, 5..=7);
    a == b || (integer(a) && integer(b))
}

fn col_type(t: &Table, c: SymId) -> Option<i16> {
    t.schema.col(c).map(ColVec::type_num)
}

impl Planner<'_> {
    /// A relation made of tables and the joins the shards can do, moving a table where they cannot do it as it lies.
    fn relation(&mut self, e: &Expr) -> Option<Rel> {
        match e {
            Expr::Var(s) | Expr::Lit(K::Sym(s)) => {
                let t = self.names.table(*s)?;
                Some(Rel {
                    expr: Expr::Var(intern(DRIVER)),
                    driver: t.name,
                    layout: Layout::of(&t),
                    cols: t.schema.cols.clone(),
                    nkeys: t.nkeys,
                    moves: vec![],
                })
            }
            Expr::Binary { op, lhs, rhs } if is_one_of(*op, &["lj", "ij", "pj"]) => {
                let left = self.relation(lhs)?;
                let Expr::Var(r) = &**rhs else { return None };
                let right = self.names.table(*r)?;
                if right.nkeys == 0 {
                    return None;
                }
                let on = right.schema.cols[..right.nkeys].to_vec();
                let (mut rel, name) = self.join(left, &right, &on)?;
                rel.expr = Expr::Binary { op: *op, lhs: Box::new(rel.expr), rhs: Box::new(Expr::Var(name)) };
                Some(rel)
            }
            Expr::Apply { target, args } => {
                let Expr::Var(f) = &**target else { return None };
                let verb = resolve(*f);
                let args: Vec<&Expr> = args.iter().map(Option::as_ref).collect::<Option<_>>()?;
                let (cols_at, left_at) = match (verb, args.len()) {
                    ("aj" | "aj0" | "ej", 3) => (0, 1),
                    ("wj" | "wj1", 4) => (1, 2),
                    _ => return None,
                };
                let mut on = symbols(args[cols_at])?;
                if verb != "ej" {
                    on.pop()?; // the last column is the time, compared by order
                }
                let left = self.relation(args[left_at])?;
                let spec = match args.get(3) {
                    Some(Expr::List(items)) => Some(items),
                    Some(_) => return None,
                    None => None,
                };
                let Expr::Var(r) = spec.map_or(args[2], |s| &s[0]) else { return None };
                let right = self.names.table(*r)?;
                // the windows of `wj` must be computed row by row from the left table
                let window = match spec {
                    Some(_) => {
                        let Expr::Var(l) = args[2] else { return None };
                        Some(self.window(args[0], *l)?)
                    }
                    None => None,
                };
                let (mut rel, name) = self.join(left, &right, &on)?;
                let mut out: Vec<Option<Expr>> = args.iter().map(|a| Some((*a).clone())).collect();
                out[left_at] = Some(rel.expr.clone());
                match spec {
                    Some(items) => {
                        let mut items = items.clone();
                        items[0] = Expr::Var(name);
                        let allowed = HashSet::new();
                        let scope = Scope { cols: &allowed, row_numbers: false };
                        for item in items.iter_mut().skip(1) {
                            *item = self.subst(item, &scope)?;
                        }
                        out[0] = window;
                        out[3] = Some(Expr::List(items));
                    }
                    None => out[2] = Some(Expr::Var(name)),
                }
                rel.expr = Expr::Apply { target: target.clone(), args: out };
                Some(rel)
            }
            _ => None,
        }
    }

    /// The window bounds of a `wj` over table `left`, rewritten to read the driving table.
    fn window(&mut self, w: &Expr, left: SymId) -> Option<Expr> {
        match w {
            Expr::Field(base, f) if matches!(&**base, Expr::Var(s) if *s == left) => {
                Some(Expr::Field(Box::new(Expr::Var(intern(DRIVER))), *f))
            }
            Expr::Lit(k) if !matches!(k, K::Sym(_)) => Some(w.clone()),
            Expr::List(items) => items.iter().map(|x| self.window(x, left)).collect::<Option<_>>().map(Expr::List),
            Expr::Binary { op, lhs, rhs } if is_one_of(*op, &["+", "-"]) => Some(Expr::Binary {
                op: *op,
                lhs: Box::new(self.window(lhs, left)?),
                rhs: Box::new(self.window(rhs, left)?),
            }),
            // `lo +\: left.time`
            Expr::Apply { target, args } if matches!(&**target, Expr::Adverb { .. }) => {
                let args = args.iter().map(|a| a.as_ref().and_then(|a| self.window(a, left)).map(Some)).collect::<Option<_>>()?;
                Some(Expr::Apply { target: target.clone(), args })
            }
            Expr::Var(s) => self.names.global(*s).filter(K::is_atom).map(Expr::Lit),
            _ => None,
        }
    }

    fn temp(&mut self, source: &Table, keys: Vec<SymId>, driver: bool) -> Move {
        let temp = intern(&format!(".kix.tmp.t{}", (self.next_temp)()));
        Move { source: source.clone(), keys, temp, driver }
    }

    /// Make `right` joinable on columns `on` where the rows of `left` are; returns the name the shards join with.
    fn join(&mut self, mut left: Rel, right: &Table, on: &[SymId]) -> Option<(Rel, SymId)> {
        // a column of the right table replaces a left column of its name: rows would no longer lie where the
        // distribution columns say
        let replaces = |l: &Layout| l.cols().iter().any(|&c| !on.contains(&c) && right.schema.col(c).is_some());
        if left.layout.as_ref().is_some_and(replaces) {
            return None;
        }
        let Some(r) = Layout::of(right) else { return Some((left, right.name)) };
        let joined = |l: &Layout| l.cols().iter().all(|c| on.contains(c));
        if left.layout.as_ref() == Some(&r) && joined(&r) {
            return Some((left, right.name));
        }
        if (self.rows)(right) <= self.broadcast_limit {
            let m = self.temp(right, vec![], false);
            let name = m.temp;
            left.moves.push(m);
            return Some((left, name));
        }
        // shuffle: both sides by the same join columns. All of the left side's, when the join compares them (then only
        // the right table may have to move), else one join column, preferably `sym`
        let l = left.layout.clone()?;
        let by = match &l {
            Layout::Hash(cols) if joined(&l) => cols.clone(),
            _ => {
                let sym = intern("sym");
                let own = l.cols().iter().copied().filter(|c| on.contains(c)).max_by_key(|&c| c == sym);
                vec![own.or_else(|| on.first().copied())?]
            }
        };
        let target = Layout::Hash(by.clone());
        if l != target {
            // only a plain table can be moved: moving the result of a join would separate rows already brought together
            if !matches!(left.expr, Expr::Var(_)) {
                return None;
            }
            let t = self.names.table(left.driver)?;
            for &c in &by {
                if !same_family(col_type(&t, c)?, col_type(right, c)?) {
                    return None;
                }
            }
            let m = self.temp(&t, by.clone(), true);
            left.driver = m.temp;
            left.layout = Some(target.clone());
            left.moves.push(m);
        }
        if r == target {
            return Some((left, right.name));
        }
        let m = self.temp(right, by, false);
        let name = m.temp;
        left.moves.push(m);
        Some((left, name))
    }

    /// Whether a function can be sent to a shard: it refers to nothing but its arguments, locals and built-in verbs.
    fn closed(&self, f: &Func) -> bool {
        match f {
            Func::Prim(s) => !is_one_of(*s, &EFFECTS),
            Func::Lambda(l) => {
                let mut bound: HashSet<SymId> = l.params.iter().copied().collect();
                for e in &l.body {
                    e.any(&mut |x| {
                        if let Expr::Assign { name, global: false, .. } = x {
                            bound.insert(*name);
                        }
                        false
                    });
                }
                l.body.iter().all(|e| self.closed_expr(e, &bound))
            }
            Func::Adverb { inner, .. } => self.closed(inner),
            Func::Proj { func, args, .. } => self.closed(func) && args.iter().flatten().all(|a| !matches!(a, K::Func(f) if !self.closed(f))),
            Func::Comp(a, b) => self.closed(a) && self.closed(b),
        }
    }

    fn closed_expr(&self, e: &Expr, bound: &HashSet<SymId>) -> bool {
        let here = match e {
            Expr::Var(s) => bound.contains(s) || (is_builtin_verb(resolve(*s)) && !is_one_of(*s, &EFFECTS)),
            Expr::Assign { global, .. } => !*global,
            Expr::IndexAssign { name, .. } => bound.contains(name),
            Expr::Unary { op, .. } | Expr::Binary { op, .. } => !is_one_of(*op, &EFFECTS),
            Expr::Lit(K::Func(f)) => return self.closed(f),
            Expr::Query(_) | Expr::SysCmd { .. } => false,
            Expr::Lambda { params, body, .. } => {
                let inner: HashSet<SymId> = params.iter().chain(bound).copied().collect();
                return body.iter().all(|b| self.closed_expr(b, &inner));
            }
            _ => true,
        };
        here && e.children().into_iter().all(|c| self.closed_expr(c, bound))
    }

    /// Replace the names that are not columns by their values, so that a shard can evaluate the expression; `None` if it
    /// cannot.
    fn subst(&mut self, e: &Expr, scope: &Scope) -> Option<Expr> {
        match e {
            Expr::Var(s) => {
                if scope.cols.contains(s) {
                    return Some(e.clone());
                }
                let name = resolve(*s);
                if name == "i" {
                    return scope.row_numbers.then(|| e.clone());
                }
                if EFFECTS.contains(&name) || self.names.table(*s).is_some() {
                    return None;
                }
                if is_builtin_verb(name) {
                    return Some(e.clone());
                }
                if name.starts_with(".z.") {
                    return self.mirror.eval_expr(e).ok().map(Expr::Lit);
                }
                match self.names.global(*s) {
                    Some(K::Func(f)) => self.closed(&f).then_some(Expr::Lit(K::Func(f))),
                    Some(v) => Some(Expr::Lit(v)),
                    // a view needs the mirror; an unknown name fails on the shard as it would here
                    None => (!self.names.mirror.read_workspace(|ws| ws.views.contains_key(s))).then(|| e.clone()),
                }
            }
            Expr::Field(base, field) => {
                if let Expr::Var(b) = &**base {
                    if !scope.cols.contains(b) {
                        if let Some(v) = self.names.global(intern(&format!("{b}.{field}"))) {
                            return Some(Expr::Lit(v));
                        }
                    }
                }
                Some(Expr::Field(Box::new(self.subst(base, scope)?), *field))
            }
            // `count i` counts rows wherever they are
            Expr::Unary { op, arg } if resolve(*op) == "count" && matches!(&**arg, Expr::Var(s) if resolve(*s) == "i") => Some(e.clone()),
            Expr::Query(q) => {
                let marked = distinct_query(e);
                let q = match &marked {
                    Some(Expr::Query(marked)) => marked,
                    _ => q,
                };
                self.analyze(q).map(|_| e.clone())
            }
            Expr::Lambda { .. } => self.closed_expr(e, &HashSet::new()).then(|| e.clone()),
            Expr::Lit(K::Func(f)) => self.closed(f).then(|| e.clone()),
            Expr::Assign { .. } | Expr::IndexAssign { .. } | Expr::SysCmd { .. } | Expr::If { .. } | Expr::Do { .. } | Expr::While { .. } | Expr::Return(_) => None,
            Expr::Unary { op, .. } | Expr::Binary { op, .. } if is_one_of(*op, &EFFECTS) => None,
            other => {
                let mut out = other.clone();
                for child in out.children_mut() {
                    let replaced = self.subst(child, scope)?;
                    *child = replaced;
                }
                // a cast of constants (`` `$"text" ``) is a constant: computed here, it filters and routes like a literal
                if let Expr::Binary { op, lhs, rhs } = &out {
                    if resolve(*op) == "$" && matches!((&**lhs, &**rhs), (Expr::Lit(_), Expr::Lit(_))) {
                        return self.mirror.eval_expr(&out).ok().map(Expr::Lit);
                    }
                }
                Some(out)
            }
        }
    }

    pub fn substitute(&mut self, e: &Expr, cols: &HashSet<SymId>) -> Option<Expr> {
        self.subst(e, &Scope { cols, row_numbers: false })
    }

    /// The empty driving table of a relation, as the mirror knows it.
    fn driver_schema(&mut self, rel: &Rel) -> Option<Flip> {
        match rel.moves.iter().find(|m| m.temp == rel.driver) {
            Some(m) => Some(m.source.schema.clone()),
            None => self.names.table(rel.driver).map(|t| t.schema),
        }
    }

    /// How to push a query down, or `None` if the tables have to come to the control node.
    pub fn analyze(&mut self, q: &QSql) -> Option<Push> {
        if !matches!(q.kind, QSqlKind::Select | QSqlKind::Exec) {
            return None;
        }
        let original = Expr::Query(Box::new(q.clone()));
        if !self.names.is_pure(&original) || self.names.refs(&original).dynamic {
            return None;
        }
        let mut rel = self.relation(&q.from)?;
        // the columns of a joined relation are those of the join of the empty tables
        if !matches!(rel.expr, Expr::Var(_)) {
            let (flip, nkeys) = crate::catalog::unkey(&self.mirror.eval_expr(&q.from).ok()?)?;
            (rel.cols, rel.nkeys) = (flip.cols, nkeys);
        }
        let replicated = rel.layout.is_none();
        let cols: HashSet<SymId> = rel.cols.iter().copied().collect();
        let exec_single = q.kind == QSqlKind::Exec && q.cols.len() == 1 && q.cols[0].name.is_none() && q.by.is_none();

        let mut query = q.clone();
        let names = resolve_col_names(&query.cols);
        query.cols.iter_mut().zip(names).for_each(|(c, n)| c.name = Some(n));
        if let Some(by) = &mut query.by {
            let names = resolve_col_names(by);
            by.iter_mut().zip(names).for_each(|(c, n)| c.name = Some(n));
        }
        let row_numbers = replicated;
        let scope = Scope { cols: &cols, row_numbers };
        for w in &mut query.where_clauses {
            *w = self.subst(w, &scope)?;
        }
        for c in query.by.iter_mut().flatten() {
            c.expr = self.subst(&c.expr, &scope)?;
        }
        let mut with_aliases = cols.clone();
        with_aliases.extend(query.by.iter().flatten().filter_map(|c| c.name));
        let scope_cols = Scope { cols: &with_aliases, row_numbers };
        for c in &mut query.cols {
            c.expr = self.subst(&c.expr, &scope_cols)?;
        }
        if let Some(limit) = &mut query.limit {
            if !self.names.refs(limit).none() {
                return None;
            }
            **limit = Expr::Lit(self.mirror.eval_expr(limit).ok()?);
        }

        let push = |kind, query, rel| Some(Push { kind, query, rel, exec_single, original: original.clone() });
        let Some(layout) = rel.layout.clone() else { return push(Kind::Replicated, query, rel) };
        // `key = expression` and `key in expression` compare with a constant: compute it here, so that it can route
        for w in &mut query.where_clauses {
            if let Expr::Binary { op, lhs, rhs } = w {
                let on_key = matches!(&**lhs, Expr::Var(s) if layout.cols().contains(s));
                if on_key && is_one_of(*op, &["=", "in"]) && !matches!(**rhs, Expr::Lit(_)) && is_constant(rhs, &cols) && !has_query(rhs) {
                    if let Ok(v) = self.mirror.eval_expr(rhs) {
                        **rhs = Expr::Lit(v);
                    }
                }
            }
        }
        // clauses on the distribution columns route the query, if nothing before them looks at other rows
        let schema = self.driver_schema(&rel)?;
        let filtered = scan_filters(&query.where_clauses, &cols, &layout, &schema, self.slots);
        if let Some(shard) = filtered.route {
            return push(Kind::Route(shard), query, rel);
        }
        if filtered.leading < query.where_clauses.len() {
            return None;
        }
        let keys = filtered.unpinned;
        match &query.by {
            Some(by) => {
                if query.distinct || by.is_empty() || !by.iter().all(|c| rowwise(&c.expr, &cols, &keys)) {
                    return None;
                }
                if keys.iter().all(|k| by.iter().any(|c| matches!(c.expr, Expr::Var(s) if s == *k))) {
                    return push(Kind::ByKey, query, rel);
                }
            }
            None => {
                let rows = if query.cols.is_empty() {
                    query.kind == QSqlKind::Select
                } else {
                    let uses = |c: &ColSpec| uses_column(&c.expr, &cols);
                    // `select` repeats a constant on every row; `exec` returns it once
                    let enough = match query.kind {
                        QSqlKind::Exec => query.cols.iter().all(uses),
                        _ => query.cols.iter().any(uses),
                    };
                    query.cols.iter().all(|c| rowwise(&c.expr, &cols, &keys)) && enough
                };
                if rows {
                    return push(Kind::Ordered, query, rel);
                }
                if query.distinct {
                    return None;
                }
            }
        }
        let aggs = self.aggregates(&query.cols, &rel, &cols, &keys)?;
        push(Kind::TwoPhase(aggs), query, rel)
    }

    fn aggregates(&mut self, specs: &[ColSpec], rel: &Rel, cols: &HashSet<SymId>, keys: &[SymId]) -> Option<Aggs> {
        if specs.is_empty() {
            return None;
        }
        // argument types are those the expression has on the empty table
        let source = self.mirror.get_var_direct(resolve(rel.driver)).ok();
        let schema = match (&rel.expr, source) {
            (Expr::Var(_), Some(t)) => crate::catalog::unkey(&t)?.0,
            (expr, _) => {
                let mut from = expr.clone();
                let original = rel.moves.iter().fold(rel.driver, |d, m| if m.temp == d { m.source.name } else { d });
                restore(&mut from, original, &rel.moves);
                crate::catalog::unkey(&self.mirror.eval_expr(&from).ok()?)?.0
            }
        };
        let mirror = Cell::new(Some(&mut *self.mirror));
        let type_of = |e: &Expr| -> Option<i16> {
            let m = mirror.take()?;
            let t = column_type(m, &schema, e);
            mirror.set(Some(m));
            t
        };
        let row = |e: &Expr| rowwise(e, cols, keys) && uses_column(e, cols);
        let constant = |e: &Expr| is_constant(e, cols);
        merge::analyze(specs, &merge::Checks { row: &row, constant: &constant, type_of: &type_of })
    }
}

/// Put the real table names back into a relation's source, so that the mirror can evaluate it.
fn restore(e: &mut Expr, driver: SymId, moves: &[Move]) {
    if let Expr::Var(s) = e {
        if resolve(*s) == DRIVER {
            *s = driver;
        } else if let Some(m) = moves.iter().find(|m| m.temp == *s) {
            *s = m.source.name;
        }
    }
    for c in e.children_mut() {
        restore(c, driver, moves);
    }
}
