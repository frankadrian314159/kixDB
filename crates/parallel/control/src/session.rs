//! A client session of the control node: how each statement is carried out.
//!
//! A statement is first tried as a **read** that shares the cluster with other reads (pushed to the shards, or computed
//! on the control node from pushed parts). Otherwise it takes the cluster to itself: inserts, upserts, updates and
//! deletes are split over the shards where that is exact, and everything else runs on the mirror engine over the tables
//! it names, gathered from the shards; tables that changed are distributed again.
use crate::catalog::{column_type, rekey, schema_of, unkey, with_attrs, Names, Table};
use crate::exec::{augmented, in_order};
use crate::plan::{has_query, is_constant, rowwise, scan_filters, Layout, Planner, Push};
use crate::Inner;
use kix_shared_ces::wire::{ForwardReply, ForwardReq, Forwarded, LoadMode, LoadReq, MemberReq, MutateReq, TxnOp, Work, ORD_COL};
use kixdb::ast::{ColSpec, Expr, QSql, QSqlKind};
use kixdb::engine::qsql::resolve_col_names;
use kixdb::engine::{changes, DistMode, Workspace};
use kixdb::ipc::{decode_k_payload, encode_k_payload};
use kixdb::persist::LogOp;
use kixdb::parser::parse_script;
use kixdb::sym::{intern, resolve, SymId};
use kixdb::{k_to_resultset, Attr, BatchResult, ColVec, Connection, Event, Flip, Func, Interceptor, KError, KResult, K};
use std::collections::{BTreeSet, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

pub struct ClusterSession {
    inner: Arc<Inner>,
    /// This session's connection to the mirror engine (it holds the current namespace and IPC handles).
    conn: Connection,
    plan: Vec<String>,
    /// The shard operations of the change being made, committed together.
    changes: Vec<(usize, TxnOp)>,
    /// The shards did not take a change of the statement being run, so nothing it made here is kept.
    refused: bool,
    /// While a statement runs on a private copy of the mirror: the copy as the mirror last took it.
    before: Option<Workspace>,
}

fn cluster_error(message: impl Into<String>) -> KError {
    KError::new("cluster", message)
}

fn strings(v: Vec<String>) -> ColVec {
    ColVec::General(Attr::None, Arc::new(v.iter().map(|s| K::string(s)).collect()))
}

fn longs(v: Vec<i64>) -> ColVec {
    ColVec::Long(Attr::None, Arc::new(v))
}

fn table(columns: Vec<(&str, ColVec)>) -> KResult<K> {
    let (names, vals): (Vec<SymId>, Vec<ColVec>) = columns.into_iter().map(|(n, v)| (intern(n), v)).unzip();
    Ok(K::Table(Arc::new(Flip::new(names, vals)?)))
}

/// Whether a table's rows can be placed the way `mode` says.
fn fits(mode: &DistMode, flip: &Flip, nkeys: usize) -> bool {
    let cols = match mode {
        DistMode::Replicated => return true,
        DistMode::Sharded { key } => std::slice::from_ref(key),
        DistMode::Partitioned { col } => std::slice::from_ref(col),
        DistMode::CoPartitioned { cols } => cols,
    };
    // a keyed table must keep all rows of a key together
    cols.iter().all(|c| flip.col_idx(*c).is_some_and(|i| nkeys == 0 || i < nkeys))
}

/// The distribution of a table nobody declared one for: by `sym` (or the first key column), else replicated.
fn default_mode(flip: &Flip, nkeys: usize) -> DistMode {
    let sym = intern("sym");
    let usable = |c: SymId| fits(&DistMode::Sharded { key: c }, flip, nkeys);
    if usable(sym) {
        DistMode::Sharded { key: sym }
    } else if nkeys > 0 {
        DistMode::Sharded { key: flip.cols[0] }
    } else {
        DistMode::Replicated
    }
}

/// A query for the shards or the mirror, with the same shape for `select` over a table or a join.
fn as_query(e: &Expr) -> Option<QSql> {
    let source = match e {
        Expr::Query(q) => return Some((**q).clone()),
        Expr::Var(_) => true,
        Expr::Binary { op, .. } => matches!(resolve(*op), "lj" | "ij" | "pj"),
        Expr::Apply { target, .. } => matches!(&**target, Expr::Var(f) if matches!(resolve(*f), "aj" | "aj0" | "ej" | "wj" | "wj1")),
        _ => false,
    };
    source.then(|| QSql {
        kind: QSqlKind::Select,
        distinct: false,
        limit: None,
        cols: vec![],
        by: None,
        from: Box::new(e.clone()),
        where_clauses: vec![],
    })
}

/// An `update` or `delete` that every shard can run on its own rows.
struct Mutation {
    table: Table,
    query: QSql,
}

impl ClusterSession {
    pub(crate) fn new(inner: Arc<Inner>) -> ClusterSession {
        let conn = inner.mirror.connect_internal();
        ClusterSession { inner, conn, plan: vec![], changes: vec![], refused: false, before: None }
    }

    /// How the statements of the last request ran.
    pub fn last_plan(&self) -> &[String] {
        &self.plan
    }

    /// Run `code` here, under a permit of the master's, if another node of this node's group is the master and every
    /// statement reads tables and changes nothing. `None`: it is to be run some other way.
    fn read_here(&mut self, code: &str, cached: &mut Option<KResult<Vec<Expr>>>) -> Option<KResult<K>> {
        if self.inner.is_master() {
            return None;
        }
        let statements = cached.get_or_insert_with(|| parse_script(code)).as_ref().ok()?.clone();
        let names = self.names();
        if statements.is_empty() || !statements.iter().all(|e| !names.refs(e).none() && names.is_pure(e)) {
            return None;
        }
        let inner = self.inner.clone();
        inner.with_permit(|| {
            let mut last = K::Null;
            for e in &statements {
                match self.read(e) {
                    Ok(Some(value)) => last = value,
                    // one that only the master can run, or a shard that would not answer this node: the master's part
                    Ok(None) => return None,
                    Err(e) if e.signal == "cluster" => return None,
                    Err(e) => return Some(Err(e)),
                }
            }
            Some(Ok(last))
        })?
    }

    /// The answer to `code` if this node does not run it as master: read here, or given by the master.
    fn elsewhere(&mut self, code: &str, cached: &mut Option<KResult<Vec<Expr>>>) -> Option<KResult<K>> {
        self.read_here(code, cached).or_else(|| {
            self.plan.clear();
            self.passed_on(|| Forwarded::Eval(code.to_string()))
        })
    }

    /// What the master of this node's group answers to `what`, if another node is the master.
    fn passed_on(&mut self, what: impl FnOnce() -> Forwarded) -> Option<KResult<K>> {
        let reply = self.inner.pass_on(self.conn.current_namespace(), what)?;
        Some(reply.and_then(|reply| {
            self.conn.eval(&format!("\\d {}", reply.namespace))?;
            match reply.answer {
                Ok(value) => decode_k_payload(&value, 0).map(|v| v.0),
                Err((signal, detail)) => Err(KError::new(&signal, detail)),
            }
        }))
    }

    /// Run what another control node passed to this one.
    pub(crate) fn run_passed_on(&mut self, req: ForwardReq) -> ForwardReply {
        let result = self.conn.eval(&format!("\\d {}", req.namespace)).and_then(|_| match req.what {
            Forwarded::Eval(code) => self.eval(&code),
            Forwarded::Message(bytes) => decode_k_payload(&bytes, 0).and_then(|(msg, _)| self.message(&msg)),
            Forwarded::Explain(code) => self.explain(&code).map(|lines| K::Vector(strings(lines))),
        });
        let answer = result.map(|value| encode_k_payload(&value)).map_err(|e| (e.signal, e.detail));
        ForwardReply { namespace: self.conn.current_namespace(), answer }
    }

    /// Run `q` source (or a cluster command) and return the value of the last statement.
    pub fn eval(&mut self, code: &str) -> KResult<K> {
        self.plan.clear();
        let mut cached = None;
        if let Some(r) = self.elsewhere(code, &mut cached) {
            return r;
        }
        if let Some(r) = self.command(code) {
            return r;
        }
        let mut last = K::Null;
        for s in cached.unwrap_or_else(|| parse_script(code))? {
            last = self.statement(&s)?;
        }
        Ok(last)
    }

    /// Run a script and collect what a shell shows for each statement.
    pub fn execute(&mut self, code: &str) -> BatchResult {
        self.plan.clear();
        let show = |v: K| match k_to_resultset(&v) {
            Some(rs) => Event::Rows(rs),
            None => Event::Value(v),
        };
        let mut cached = None;
        // (a node that is not the master gives the value of the last statement)
        if let Some(r) = self.elsewhere(code, &mut cached).or_else(|| self.command(code)) {
            return BatchResult { events: vec![r.map(show).unwrap_or_else(Event::Error)] };
        }
        let mut events = vec![];
        match cached.unwrap_or_else(|| parse_script(code)) {
            Ok(statements) => {
                for s in statements {
                    match self.statement(&s) {
                        Ok(v) if matches!(v, K::Null) || matches!(s, Expr::Assign { .. }) => {}
                        Ok(v) => events.push(show(v)),
                        Err(e) => {
                            events.push(Event::Error(e));
                            break;
                        }
                    }
                }
            }
            Err(e) => events.push(Event::Error(e)),
        }
        BatchResult { events }
    }

    /// How each statement would run, without running it.
    pub fn explain(&mut self, code: &str) -> KResult<Vec<String>> {
        if let Some(r) = self.passed_on(|| Forwarded::Explain(code.to_string())) {
            let K::Vector(ColVec::General(_, lines)) = r? else { return Ok(vec![]) };
            return Ok(lines.iter().filter_map(K::as_str).collect());
        }
        let inner = self.inner.clone();
        let _shared = inner.gate.read().unwrap_or_else(|e| e.into_inner());
        let mut out = vec![];
        for (n, e) in parse_script(code)?.iter().enumerate() {
            let refs = self.names().refs(e);
            let how = if refs.none() {
                "control node".to_string()
            } else if self.names().schema_only(e) {
                "control node: schema only".into()
            } else if self.counted(e).is_some() {
                "scatter-gather: row counts summed".into()
            } else if let Some(push) = self.analyze(e) {
                push.describe()
            } else if self.as_insert(e).is_some() {
                "rows split over the shards".into()
            } else if self.mutation(e)?.is_some() {
                "update / delete run on every shard".into()
            } else {
                let tables: Vec<String> = refs.tables.iter().map(|&t| resolve(t).into()).collect();
                let what = if refs.dynamic { "every table".to_string() } else { tables.join(", ") };
                format!("control node, after gathering {what} (unless its parts can be pushed down)")
            };
            out.push(format!("Statement {}: {how}", n + 1));
        }
        Ok(out)
    }

    fn names(&self) -> Names<'_> {
        Names { mirror: self.conn.server(), ns: self.conn.current_namespace() }
    }

    fn note(&mut self, how: impl Into<String>) {
        self.plan.push(how.into());
    }

    // ----- Cluster commands --------------------------------------------------

    /// `EXPLAIN`, `DISTRIBUTE`, `SHARDS`, `ADD SHARD`, `REMOVE SHARD`, `REPLICAS`, `ADD REPLICA`, `REMOVE REPLICA`,
    /// `CONTROLS` and `REMOVE CONTROL`.
    fn command(&mut self, code: &str) -> Option<KResult<K>> {
        if let Some(query) = code.trim().strip_prefix("EXPLAIN ") {
            let lines = self.explain(query);
            return Some(lines.map(|l| K::Vector(strings(l))));
        }
        let words: Vec<&str> = code.trim().trim_end_matches(';').split_whitespace().collect();
        let number = |w: &str| w.parse::<u64>().map_err(|_| KError::domain(format!("'{w}' is not a number")));
        Some(match words.as_slice() {
            ["DISTRIBUTE", t, rest @ ..] => {
                let args = match rest {
                    ["REPLICATED"] => format!("`{t};`replicated"),
                    ["BY", c] => format!("`{t};`sharded;`{c}"),
                    ["PARTITIONED", "BY", cols @ ..] if !cols.is_empty() => {
                        let names = cols.concat();
                        let list: String = names.split(',').filter(|c| !c.is_empty()).map(|c| format!("`{c}")).collect();
                        format!("`{t};`partitioned;{list}")
                    }
                    _ => return Some(Err(KError::syntax("DISTRIBUTE table BY col | PARTITIONED BY col[, col] | REPLICATED"))),
                };
                parse_script(&format!(".kix.dist[{args}]")).and_then(|s| self.statement(&s[0]))
            }
            // (also when a change cannot be finished: it shows which shard does not answer)
            ["SHARDS"] => {
                let _ = self.inner.settle();
                self.shards()
            }
            ["ADD", "SHARD", addr] => self.inner.resize(Some(addr)).and_then(|_| self.shards()),
            ["REMOVE", "SHARD"] => self.inner.resize(None).and_then(|_| self.shards()),
            ["REPLICAS", s] => number(s).and_then(|s| self.members(s, MemberReq::default())),
            ["CONTROLS"] => self.controls(None),
            ["REMOVE", "CONTROL", id] => number(id).and_then(|id| self.controls(Some(id))),
            ["ADD", "REPLICA", s, id, addr] => number(s).and_then(|s| {
                let add = Some((number(id)?, addr.to_string()));
                self.members(s, MemberReq { add, remove: None })
            }),
            ["REMOVE", "REPLICA", s, id] => {
                number(s).and_then(|s| self.members(s, MemberReq { add: None, remove: Some(number(id)?) }))
            }
            _ => return None,
        })
    }

    fn shards(&mut self) -> KResult<K> {
        let shards = self.inner.shards();
        let answers = self.inner.wait(futures::future::join_all(shards.iter().map(|s| s.status())));
        let (mut roles, mut terms, mut applied, mut memory) = (vec![], vec![], vec![], vec![]);
        for a in &answers {
            let (role, term, ts, bytes) = match a {
                Ok(s) => (s.role.as_str(), s.term as i64, s.applied_ts as i64, s.memory_bytes as i64),
                Err(_) => ("down", 0, 0, 0),
            };
            roles.push(intern(role));
            terms.push(term);
            applied.push(ts);
            memory.push(bytes);
        }
        table(vec![
            ("shard", longs((0..answers.len() as i64).collect())),
            ("addr", strings(shards.iter().map(|s| s.addresses()).collect())),
            ("role", ColVec::Sym(Attr::None, Arc::new(roles))),
            ("term", longs(terms)),
            ("applied", longs(applied)),
            ("memory", longs(memory)),
        ])
    }

    fn controls(&mut self, remove: Option<u64>) -> KResult<K> {
        let (members, master) = self.inner.controls(remove)?;
        table(vec![
            ("id", longs(members.iter().map(|m| m.0 as i64).collect())),
            ("addr", strings(members.iter().map(|m| m.1.clone()).collect())),
            ("voter", ColVec::Bool(Attr::None, Arc::new(members.iter().map(|m| m.2).collect()))),
            ("master", ColVec::Bool(Attr::None, Arc::new(members.iter().map(|m| m.0 == master).collect()))),
        ])
    }

    fn members(&mut self, shard: u64, req: MemberReq) -> KResult<K> {
        let members = self.inner.members(shard as usize, &req)?;
        table(vec![
            ("id", longs(members.iter().map(|m| m.0 as i64).collect())),
            ("addr", strings(members.iter().map(|m| m.1.clone()).collect())),
            ("voter", ColVec::Bool(Attr::None, Arc::new(members.iter().map(|m| m.2).collect()))),
        ])
    }

    // ----- Statements ---------------------------------------------------------

    fn statement(&mut self, e: &Expr) -> KResult<K> {
        let inner = self.inner.clone();
        inner.settle()?;
        {
            let _shared = inner.gate.read().unwrap_or_else(|e| e.into_inner());
            if let Some(v) = self.read(e)? {
                return Ok(v);
            }
        }
        let _alone = inner.gate.write().unwrap_or_else(|e| e.into_inner());
        let result = self.on_a_copy(e);
        let saved = inner.save_state();
        let v = result?;
        saved?;
        Ok(v)
    }

    /// The value of a read of distributed tables that needs no table brought to the control node.
    fn read(&mut self, e: &Expr) -> KResult<Option<K>> {
        if self.names().refs(e).none() {
            return Ok(None);
        }
        if self.names().schema_only(e) {
            self.note("control: schema");
            return self.conn.eval_expr(e).map(Some);
        }
        let follows = self.names().follows_keys(e)?;
        if let Some(t) = self.counted(e) {
            self.note(format!("scatter-gather: count of {}", t.name));
            return Ok(Some(K::Long(self.inner.count(&t)? as i64)));
        }
        if let Some(mut push) = self.analyze(e) {
            self.note(push.describe());
            let q = &mut push.query;
            for x in q.where_clauses.iter_mut().chain(q.cols.iter_mut().map(|c| &mut c.expr)).chain(q.by.iter_mut().flatten().map(|c| &mut c.expr)) {
                self.fill(x)?;
            }
            return self.inner.run_push(&mut self.conn, &push).map(Some);
        }
        // (a key can be followed on the shards, or here with the table it refers to, but not into rows gathered alone)
        if follows {
            return Ok(None);
        }
        if let Some(v) = self.read_parts(e)? {
            return Ok(Some(v));
        }
        self.slice(e)
    }

    /// Replace the subqueries of a pushed-down expression by their values.
    fn fill(&mut self, e: &mut Expr) -> KResult<()> {
        if matches!(e, Expr::Query(_)) {
            let v = self.read(e)?.ok_or_else(|| cluster_error("a subquery could not be run on the shards"))?;
            *e = Expr::Lit(v);
            return Ok(());
        }
        for c in e.children_mut() {
            self.fill(c)?;
        }
        Ok(())
    }

    /// An expression built by verbs out of reads: run the reads, then the verbs on the control node.
    fn read_parts(&mut self, e: &Expr) -> KResult<Option<K>> {
        if let Expr::Query(q) = e {
            return self.query_over_read(e, q);
        }
        let verb = match e {
            Expr::Unary { .. } | Expr::Binary { .. } | Expr::Field(..) => true,
            // a built-in verb or the indexing of a table; a user function could do anything
            Expr::Apply { target, .. } => match &**target {
                Expr::Var(f) => kixdb::parser::is_builtin_verb(resolve(*f)) || self.names().table(*f).is_some(),
                _ => false,
            },
            _ => false,
        };
        if !verb || !self.names().is_pure(e) {
            return Ok(None);
        }
        let mut local = e.clone();
        let mut replaced = false;
        for child in local.children_mut() {
            if self.names().refs(child).none() {
                continue;
            }
            match self.read(child)? {
                Some(v) => *child = Expr::Lit(v),
                None => return Ok(None),
            }
            replaced = true;
        }
        if !replaced || !self.names().refs(&local).none() {
            return Ok(None);
        }
        self.conn.eval_expr(&local).map(Some)
    }

    /// A query whose source is itself a read (`select ... from select ... from t`) and that mentions no other table:
    /// read the source, then run the query on the control node.
    fn query_over_read(&mut self, whole: &Expr, q: &QSql) -> KResult<Option<K>> {
        if matches!(*q.from, Expr::Var(_) | Expr::Lit(_)) || !self.names().is_pure(whole) {
            return Ok(None);
        }
        let mut local = q.clone();
        *local.from = Expr::Lit(K::Null);
        if !self.names().refs(&Expr::Query(Box::new(local.clone()))).none() {
            return Ok(None);
        }
        let Some(source) = self.read(&q.from)? else { return Ok(None) };
        *local.from = Expr::Lit(source);
        self.conn.eval_expr(&Expr::Query(Box::new(local))).map(Some)
    }

    /// The table of `count table`.
    fn counted(&self, e: &Expr) -> Option<Table> {
        match e {
            Expr::Unary { op, arg } if resolve(*op) == "count" => match &**arg {
                Expr::Var(t) => self.names().table(*t),
                _ => None,
            },
            _ => None,
        }
    }

    fn with_planner<R>(&mut self, f: impl FnOnce(&mut Planner) -> R) -> R {
        let inner = self.inner.clone();
        let (ns, mirror) = (self.conn.current_namespace(), self.conn.server().clone());
        let rows = |t: &Table| inner.rows_of(t).unwrap_or(usize::MAX);
        let next_temp = || inner.next_temp.fetch_add(1, Ordering::SeqCst);
        let slots = inner.slots();
        f(&mut Planner {
            names: Names { mirror: &mirror, ns },
            mirror: &mut self.conn,
            slots: &slots,
            broadcast_limit: inner.broadcast_limit.load(Ordering::SeqCst),
            rows: &rows,
            next_temp: &next_temp,
        })
    }

    fn analyze(&mut self, e: &Expr) -> Option<Push> {
        let q = as_query(e)?;
        self.with_planner(|p| p.analyze(&q))
    }

    /// Run `e` against a private copy of the mirror, then make in the mirror what changed there, unless the shards did
    /// not take a change. (So nothing is written there that is only held for the statement, such as a gathered table.)
    fn on_a_copy(&mut self, e: &Expr) -> KResult<K> {
        let copy = self.inner.mirror.private_copy();
        self.before = Some(copy.read_workspace(Workspace::snapshot));
        let mirror = self.conn.retarget(copy);
        self.refused = false;
        let result = self.exclusive(e);
        let kept = if self.refused { Ok(()) } else { self.publish(&[]) };
        self.conn.retarget(mirror);
        self.before = None;
        kept?;
        result
    }

    /// Write to the mirror what has changed in the private copy since it was last written, and remove `drop`.
    fn publish(&mut self, drop: &[SymId]) -> KResult<()> {
        let Some(before) = &self.before else { return Ok(()) };
        let now = self.conn.server().read_workspace(Workspace::snapshot);
        self.inner.mirror.connect_internal().adopt(before, &now, drop)?;
        self.before = Some(now);
        Ok(())
    }

    /// What `publish` would write.
    fn unpublished(&self) -> Vec<LogOp> {
        let Some(before) = &self.before else { return vec![] };
        let now = self.conn.server().read_workspace(Workspace::snapshot);
        self.inner.mirror.read_workspace(|mirror| changes(mirror, before, &now))
    }

    fn exclusive(&mut self, e: &Expr) -> KResult<K> {
        match e {
            Expr::SysCmd { cmd, arg } => return self.system(e, cmd, arg),
            Expr::Apply { target, args } if matches!(&**target, Expr::Var(f) if resolve(*f) == ".kix.dist") => {
                return self.redistribute(target, args);
            }
            _ => {}
        }
        if let Some((verb, t, rows)) = self.as_insert(e) {
            return self.insert(&verb, t, &rows);
        }
        if let Some(m) = self.mutation(e)? {
            return self.mutate(m);
        }
        // `x: select ...` reads on the shards, then assigns on the control node
        if let Expr::Assign { name, global: false, op: None, val } = e {
            if self.names().is_pure(val) {
                if let Some(v) = self.read(val)? {
                    let assign = Expr::Assign { name: *name, global: false, op: None, val: Box::new(Expr::Lit(v)) };
                    return self.on_mirror(&assign);
                }
            }
        }
        if let Some(v) = self.read(e)? {
            return Ok(v);
        }
        self.on_mirror(e)
    }

    // ----- Gather -------------------------------------------------------------

    /// Run `e` on the mirror engine over the tables it refers to.
    fn on_mirror(&mut self, e: &Expr) -> KResult<K> {
        let refs = self.names().refs(e);
        let before: BTreeSet<SymId> = self.names().all_tables().into_iter().collect();
        let mut wanted = if refs.dynamic { before.clone() } else { refs.tables };
        // (and the tables that their foreign keys and links refer to)
        let mut look: Vec<SymId> = wanted.iter().copied().collect();
        while let Some(t) = look.pop() {
            for d in self.names().table(t).map(|t| self.names().domains(&t)).unwrap_or_default() {
                if wanted.insert(d.name) {
                    look.push(d.name);
                }
            }
        }
        let mut originals = vec![];
        for &name in &wanted {
            let t = self.names().table(name).ok_or_else(|| cluster_error(format!("table {name} disappeared")))?;
            let full = self.inner.gather(&t)?;
            originals.push((t, full));
        }
        if originals.is_empty() {
            self.note("control");
        } else {
            let list: Vec<String> = originals.iter().map(|(t, _)| resolve(t.name).into()).collect();
            self.note(format!("gather {}", list.join(", ")));
            self.conn.set_vars(originals.iter().map(|(t, full)| (t.name, Some(full.clone()))).collect())?;
        }
        let result = self.conn.eval_expr(e);
        // whatever changed is distributed again, also when the statement failed part way
        let written = self.write_back(originals, before);
        let v = result?;
        written?;
        Ok(v)
    }

    /// Distribute the tables that changed, on every shard or none.
    fn write_back(&mut self, originals: Vec<(Table, K)>, before: BTreeSet<SymId>) -> KResult<()> {
        let mut first_error = None;
        let mut names: BTreeSet<SymId> = self.names().all_tables().into_iter().collect();
        names.extend(originals.iter().map(|(t, _)| t.name));
        names.extend(&before);
        for name in names {
            let now = match self.names().table(name) {
                Some(_) => self.conn.server().read_workspace(|ws| ws.vars.get(&name).cloned()),
                None => None,
            };
            let original = originals.iter().find(|(t, _)| t.name == name);
            let done = match (original, now) {
                (Some((t, full)), Some(now)) if same_table(full, &now) => self.conn.set_vars(vec![(name, Some(t.schema_k()))]),
                (_, Some(now)) => {
                    let rows = unkey(&now).map_or(0, |f| f.0.nrows());
                    if original.is_none() && before.contains(&name) && rows == 0 {
                        Ok(())
                    } else {
                        self.distribute(name, &now, None)
                    }
                }
                (original, None) if original.is_some() || before.contains(&name) => {
                    let drop = LoadReq::new(resolve(name), LoadMode::Drop, &K::Null, None);
                    self.changes.extend((0..self.inner.shards().len()).map(|s| (s, TxnOp::Load(drop.clone()))));
                    Ok(())
                }
                _ => Ok(()),
            };
            if let Err(e) = done {
                first_error.get_or_insert(e);
            }
        }
        let done = match first_error {
            Some(e) => Err(e),
            None => self.commit(false, |_, _| Ok(())).map(|_| ())
        };
        self.unless_refused(done)
    }

    /// Commit the operations gathered. `learned` then makes in the copy of the mirror what the change makes of the
    /// control node's state, from the shards' answers; `alters`: it will.
    ///
    /// In a group of control nodes such a change is noted before the shards are told to make it, so that a master that
    /// takes over finishes it, and written to the mirror as soon as it is made.
    fn commit(&mut self, alters: bool, learned: impl FnOnce(&mut Self, &[K]) -> KResult<()>) -> KResult<Vec<K>> {
        let ops = std::mem::take(&mut self.changes);
        let inner = self.inner.clone();
        let grouped = inner.group.get().is_some() && !ops.is_empty() && self.before.is_some();
        if !(grouped && (alters || !self.unpublished().is_empty())) {
            let answers = inner.commit(ops)?;
            learned(self, &answers)?;
            return Ok(answers);
        }
        let (mut learned, mut noted) = (Some(learned), false);
        let done = inner.commit_noting(
            ops,
            Some(&mut |txid, primary, answers| {
                learned.take().map_or(Ok(()), |learned| learned(self, answers))?;
                let ops = self.unpublished();
                noted = !ops.is_empty();
                match noted {
                    true => self.inner.note_pending(txid, primary, &ops),
                    false => Ok(()),
                }
            }),
        );
        match done {
            Ok(answers) if !noted => Ok(answers),
            done => done.and_then(|answers| self.publish(&[Inner::pending_name()]).map(|_| answers)).inspect_err(|_| {
                // whether it was made, and written, is seen to before anything else is done
                if noted {
                    inner.doubt();
                    self.refused = true;
                }
            }),
        }
    }

    /// Note a change that the shards did not take: what the statement made on the control node is then thrown away.
    fn unless_refused(&mut self, done: KResult<()>) -> KResult<()> {
        if done.is_err() {
            self.changes.clear();
            self.refused = true;
        }
        done
    }

    /// A query over one partitioned table that cannot be pushed down: gather only the rows its leading filters keep and
    /// the columns it names.
    fn slice(&mut self, e: &Expr) -> KResult<Option<K>> {
        let Expr::Query(q) = e else { return Ok(None) };
        let (QSqlKind::Select | QSqlKind::Exec, Expr::Var(from)) = (q.kind, &*q.from) else { return Ok(None) };
        let Some(t) = self.names().table(*from) else { return Ok(None) };
        let Some(layout) = Layout::of(&t) else { return Ok(None) };
        let mut rest = (**q).clone();
        *rest.from = Expr::Lit(K::Null);
        if !self.names().is_pure(e) || !self.names().refs(&Expr::Query(Box::new(rest))).none() {
            return Ok(None);
        }
        let cols: HashSet<SymId> = t.schema.cols.iter().copied().collect();
        // `i` numbers the rows of the whole table
        let numbered = e.any(&mut |x| matches!(x, Expr::Var(s) if resolve(*s) == "i"));
        let mut pushed = vec![];
        if !numbered {
            for w in &q.where_clauses {
                match self.with_planner(|p| p.substitute(w, &cols)) {
                    Some(w) if !has_query(&w) => pushed.push(w),
                    _ => break,
                }
            }
        }
        let leading = scan_filters(&pushed, &cols, &layout, &t.schema, &self.inner.slots()).leading;
        pushed.truncate(leading);
        let mut needed: Vec<SymId> = vec![];
        if !q.cols.is_empty() {
            let used = q.cols.iter().chain(q.by.iter().flatten()).map(|c| &c.expr).chain(&q.where_clauses[pushed.len()..]);
            for x in used {
                x.any(&mut |x| {
                    if let Expr::Var(s) | Expr::Field(_, s) = x {
                        if cols.contains(s) && !needed.contains(s) {
                            needed.push(*s);
                        }
                    }
                    false
                });
            }
            if needed.is_empty() {
                needed.push(t.schema.cols[0]);
            }
        }
        if pushed.is_empty() && (needed.is_empty() || needed.len() == t.schema.cols.len()) {
            return Ok(None);
        }
        self.note(format!(
            "gather {} ({} column(s), {} filter(s) on the shards)",
            t.name,
            if needed.is_empty() { t.schema.cols.len() } else { needed.len() },
            pushed.len()
        ));
        let mut columns: Vec<ColSpec> = needed.iter().map(|&c| ColSpec { name: Some(c), expr: Expr::Var(c) }).collect();
        if !columns.is_empty() {
            columns.push(ColSpec { name: Some(intern(ORD_COL)), expr: Expr::Var(intern(ORD_COL)) });
        }
        let fetched = pushed.len();
        let fetch = Expr::Query(Box::new(QSql {
            kind: QSqlKind::Select,
            distinct: false,
            limit: None,
            cols: columns,
            by: None,
            from: Box::new(augmented(t.name)),
            where_clauses: pushed,
        }));
        let parts = self.inner.all(Work::Eval(Box::new(fetch)))?;
        let rows = in_order(parts.iter().filter_map(|k| unkey(k).map(|f| f.0)).collect())?;
        let nkeys = if needed.is_empty() { t.nkeys } else { 0 };
        let mut local = (**q).clone();
        *local.from = Expr::Lit(rekey(rows, nkeys));
        local.where_clauses.drain(..fetched);
        self.conn.eval_expr(&Expr::Query(Box::new(local))).map(Some)
    }

    // ----- Distribution -------------------------------------------------------

    /// Place a table's rows on the shards and keep its schema on the mirror.
    fn distribute(&mut self, name: SymId, value: &K, mode: Option<DistMode>) -> KResult<()> {
        let (flip, nkeys) = unkey(value).ok_or_else(|| KError::ty("only tables are distributed"))?;
        let declared = mode.or_else(|| self.conn.server().read_workspace(|ws| ws.dist.get(&name).cloned()));
        let mode = declared.filter(|m| fits(m, &flip, nkeys)).unwrap_or_else(|| default_mode(&flip, nkeys));
        let t = Table { name, schema: schema_of(&flip), nkeys, mode: mode.clone() };
        let parts = self.inner.split(&t, &flip)?;
        let loads: Vec<LoadReq> = parts
            .into_iter()
            .map(|(part, ords)| LoadReq::new(resolve(name), LoadMode::Replace, &rekey(with_attrs(part, &flip), nkeys), ords))
            .collect();
        self.changes.extend(loads.into_iter().map(TxnOp::Load).enumerate());
        let (kind, by) = match &mode {
            DistMode::Replicated => ("replicated", None),
            DistMode::Sharded { key } => ("sharded", Some(K::Sym(*key))),
            DistMode::Partitioned { col } => ("partitioned", Some(K::Sym(*col))),
            DistMode::CoPartitioned { cols } => ("partitioned", Some(K::Vector(ColVec::Sym(Attr::None, Arc::new(cols.clone()))))),
        };
        let mut args = vec![K::Sym(name), K::sym(kind)];
        args.extend(by);
        let declare = Expr::Apply {
            target: Box::new(Expr::Var(intern(".kix.dist"))),
            args: args.into_iter().map(|a| Some(Expr::Lit(a))).collect(),
        };
        self.conn.eval_expr(&declare)?;
        self.conn.set_vars(vec![(name, Some(t.schema_k()))])
    }

    /// `.kix.dist[table; mode; col]`: declare a distribution, moving the rows of a table that exists.
    fn redistribute(&mut self, target: &Expr, args: &[Option<Expr>]) -> KResult<K> {
        let mut values = vec![];
        for a in args.iter().flatten() {
            values.push(Some(Expr::Lit(self.exclusive(a)?)));
        }
        let Some(Some(Expr::Lit(K::Sym(name)))) = values.first() else {
            return Err(KError::ty(".kix.dist[table; mode; col] expects the table's name as a symbol"));
        };
        let name = *name;
        let declare = Expr::Apply { target: Box::new(target.clone()), args: values };
        let Some(t) = self.names().table(name) else {
            return self.conn.eval_expr(&declare);
        };
        let full = self.inner.gather(&t)?;
        self.conn.eval_expr(&declare)?;
        let mode = self.conn.server().read_workspace(|ws| ws.dist.get(&name).cloned()).unwrap_or(DistMode::Replicated);
        let (flip, nkeys) = unkey(&full).ok_or_else(|| cluster_error("not a table"))?;
        if !fits(&mode, &flip, nkeys) {
            let wrong = KError::domain("distribution columns must be columns of the table, and key columns of a keyed table");
            return self.unless_refused(Err(wrong)).map(|_| K::Null);
        }
        self.note(format!("redistribute {}", t.name));
        self.place_rows(t.name, &full, mode)?;
        Ok(K::Sym(name))
    }

    /// Distribute a table's rows as `mode` says, on every shard or none.
    fn place_rows(&mut self, name: SymId, rows: &K, mode: DistMode) -> KResult<()> {
        let done = self.distribute(name, rows, Some(mode)).and_then(|_| self.commit(false, |_, _| Ok(())).map(|_| ()));
        self.unless_refused(done)
    }

    /// Gather a distributed table and distribute it again as it is declared.
    pub(crate) fn place_again(&mut self, name: SymId) -> KResult<()> {
        let Some(t) = self.names().table(name) else { return Ok(()) };
        let full = self.inner.gather(&t)?;
        self.place_rows(name, &full, t.mode)
    }

    // ----- Writes -------------------------------------------------------------

    /// `` `t insert rows ``, `upsert[`t; rows]` and aliases of the two verbs, on a distributed table.
    fn as_insert(&self, e: &Expr) -> Option<(String, Table, Expr)> {
        let (verb, target, rows) = match e {
            Expr::Binary { op, lhs, rhs } => (Expr::Var(*op), &**lhs, &**rhs),
            Expr::Apply { target, args } => match args.as_slice() {
                [Some(a), Some(b)] => ((**target).clone(), a, b),
                _ => return None,
            },
            _ => return None,
        };
        let verb = match verb {
            Expr::Var(s) => match self.names().global(s) {
                Some(K::Func(Func::Prim(p))) => resolve(p),
                Some(_) => return None,
                None => resolve(s),
            },
            Expr::Lit(K::Func(Func::Prim(p))) => resolve(p),
            _ => return None,
        };
        let Expr::Lit(K::Sym(t)) = target else { return None };
        if !matches!(verb, "insert" | "upsert") {
            return None;
        }
        Some((verb.into(), self.names().table(*t)?, rows.clone()))
    }

    fn insert(&mut self, verb: &str, t: Table, rows: &Expr) -> KResult<K> {
        let rows = self.exclusive(rows)?;
        self.names().replicated_domains(&t)?;
        // the mirror turns the rows into columns of the table's types, as one engine would
        let plain = K::Table(Arc::new(t.schema.clone())).untagged().into_owned();
        let K::Table(plain) = plain else { return Err(KError::ty("not a table")) };
        let empty = K::Table(plain.clone());
        let coerce = Expr::Binary { op: intern("upsert"), lhs: Box::new(Expr::Lit(empty)), rhs: Box::new(Expr::Lit(rows)) };
        let K::Table(coerced) = self.conn.eval_expr(&coerce)? else { return Err(KError::ty("rows do not form a table")) };
        // (this node has the lists that enumerations take values from; the shards check foreign keys)
        let coerced = self.conn.enumerate_rows(&t.schema, (*coerced).clone(), true)?;
        let schema = t.schema.concat(&coerced.take(0))?;
        let before = match verb {
            "insert" => self.inner.rows_of(&t)? as i64,
            _ => 0,
        };
        let parts = self.inner.split(&t, &coerced)?;
        let mut modes = vec![if t.nkeys > 0 { LoadMode::Upsert } else { LoadMode::Append }];
        if t.nkeys > 0 && verb == "insert" {
            // every shard is asked about duplicate keys before any shard changes
            modes.insert(0, LoadMode::CheckInsert);
        }
        for mode in modes {
            let some = parts.iter().enumerate().filter(|(_, p)| p.0.nrows() > 0);
            let load = |(s, (part, ords)): (usize, &(Flip, Option<Vec<i64>>))| (s, TxnOp::Load(LoadReq::new(resolve(t.name), mode, &K::Table(Arc::new(part.clone())), ords.clone())));
            self.changes.extend(some.map(load));
        }
        // (the first rows may give columns of the schema their types)
        let (name, now) = (t.name, rekey(schema, t.nkeys));
        let alters = !same_table(&now, &t.schema_k());
        self.commit(alters, move |session, _| match alters {
            true => session.conn.set_vars(vec![(name, Some(now))]),
            false => Ok(()),
        })?;
        if verb == "insert" {
            // (an insert that is made adds every row)
            self.inner.rows.lock().unwrap_or_else(|e| e.into_inner()).insert(t.name, before as usize + coerced.nrows());
        }
        self.note(format!("{verb}: {} row(s) split over the shards", coerced.nrows()));
        Ok(match verb {
            "insert" => K::Vector(longs((before..before + coerced.nrows() as i64).collect())),
            _ => K::Sym(t.name),
        })
    }

    /// An in-place `update` / `delete` of a partitioned table that gives the same result run shard by shard.
    fn mutation(&mut self, e: &Expr) -> KResult<Option<Mutation>> {
        let Expr::Query(q) = e else { return Ok(None) };
        let Expr::Lit(K::Sym(from)) = &*q.from else { return Ok(None) };
        if matches!(q.kind, QSqlKind::Select | QSqlKind::Exec) || !self.names().is_pure(e) {
            return Ok(None);
        }
        let Some(t) = self.names().table(*from) else { return Ok(None) };
        let Some(layout) = Layout::of(&t) else { return Ok(None) };
        let cols: HashSet<SymId> = t.schema.cols.iter().copied().collect();
        let mut query = (**q).clone();
        for w in &mut query.where_clauses {
            match self.with_planner(|p| p.substitute(w, &cols)) {
                Some(s) => *w = s,
                None => return Ok(None),
            }
        }
        let filtered = scan_filters(&query.where_clauses, &cols, &layout, &t.schema, &self.inner.slots());
        if filtered.leading < query.where_clauses.len() {
            return Ok(None);
        }
        // the distribution columns that differ between the rows kept, which groups must include
        let keys = filtered.unpinned;
        let moves_rows = |names: &[SymId]| names.iter().any(|n| layout.cols().contains(n));
        let names = resolve_col_names(&query.cols);
        match q.kind {
            QSqlKind::DeleteCols if !query.where_clauses.is_empty() || moves_rows(&names) => return Ok(None),
            QSqlKind::Update => {
                if moves_rows(&names) {
                    return Ok(None);
                }
                let grouped_by_key = match &mut query.by {
                    Some(by) => {
                        for b in by.iter_mut() {
                            match self.with_planner(|p| p.substitute(&b.expr, &cols)) {
                                Some(s) if rowwise(&s, &cols, &keys) => b.expr = s,
                                _ => return Ok(None),
                            }
                        }
                        if !keys.iter().all(|k| by.iter().any(|b| matches!(b.expr, Expr::Var(s) if s == *k))) {
                            return Ok(None);
                        }
                        true
                    }
                    None => false,
                };
                for (c, name) in query.cols.iter_mut().zip(names) {
                    c.name = Some(name);
                    let Some(s) = self.with_planner(|p| p.substitute(&c.expr, &cols)) else { return Ok(None) };
                    let each_row = grouped_by_key || rowwise(&s, &cols, &keys) || (is_constant(&s, &cols) && matches!(s, Expr::Lit(ref k) if k.is_atom()));
                    // shards whose rows do not match must end up with columns of the same type as the others
                    let same_type = match t.schema.col(name) {
                        Some(old) => old.type_num() == 0 || column_type(&mut self.conn, &t.schema, &s) == Some(old.type_num()),
                        None => query.where_clauses.is_empty(),
                    };
                    if !each_row || !same_type {
                        return Ok(None);
                    }
                    c.expr = s;
                }
            }
            _ => {}
        }
        if query.where_clauses.iter().chain(query.cols.iter().map(|c| &c.expr)).any(has_query) {
            return Ok(None);
        }
        Ok(Some(Mutation { table: t, query }))
    }

    fn mutate(&mut self, m: Mutation) -> KResult<K> {
        let Mutation { table: t, mut query } = m;
        let (kind, assigned) = (query.kind, resolve_col_names(&query.cols));
        *query.from = augmented(t.name);
        let req = MutateReq { table: resolve(t.name).into(), query: Expr::Query(Box::new(query)) };
        self.changes.extend((0..self.inner.shards().len()).map(|s| (s, TxnOp::Mutate(req.clone()))));
        let name = t.name;
        self.commit(true, move |session, results| session.take_schema(&t, kind, &assigned, results))?;
        self.note("update / delete run on every shard");
        Ok(K::Sym(name))
    }

    /// Give the mirror the columns that the shards answered an `update` / `delete` of `t` with.
    fn take_schema(&mut self, t: &Table, kind: QSqlKind, assigned: &[SymId], results: &[K]) -> KResult<()> {
        let mut schemas = results.iter().filter_map(unkey);
        let (mut schema, nkeys) = schemas.next().ok_or_else(|| cluster_error("no shard answered"))?;
        for (other, _) in schemas {
            for (c, v) in schema.cols.iter().zip(schema.vals.iter_mut()) {
                if v.type_num() == 0 {
                    if let Some(typed) = other.col(*c) {
                        *v = typed.clone();
                    }
                }
            }
        }
        // columns the statement left alone keep their attributes, as on one engine (a delete of rows drops them)
        if kind != QSqlKind::DeleteRows {
            for (c, v) in schema.cols.iter().zip(schema.vals.iter_mut()) {
                if let Some(old) = t.schema.col(*c).filter(|_| !assigned.contains(c)) {
                    v.set_attr_unchecked(old.attr());
                }
            }
        }
        self.conn.set_vars(vec![(t.name, Some(rekey(schema, nkeys)))])
    }

    // ----- System commands ----------------------------------------------------

    fn system(&mut self, whole: &Expr, cmd: &str, arg: &str) -> KResult<K> {
        let (cmd, count) = match cmd.split_once(':') {
            Some((c, n)) => (c, n.parse::<usize>().ok()),
            None => (cmd, None),
        };
        match cmd {
            "a" | "b" | "v" | "f" | "d" | "w" | "\\" => self.conn.eval_expr(whole),
            "l" if std::path::Path::new(arg).is_dir() => {
                // splayed tables become distributed tables; partitioned ones stay mounted on the control node
                let before: BTreeSet<SymId> = self.names().all_tables().into_iter().collect();
                let result = self.conn.eval_expr(whole);
                self.write_back(vec![], before)?;
                result
            }
            "l" => {
                let mut last = K::Null;
                for s in parse_script(&std::fs::read_to_string(arg)?)? {
                    last = self.exclusive(&s)?;
                }
                Ok(last)
            }
            "t" | "ts" => {
                let (count, code) = match (count, arg.strip_prefix(':').and_then(|r| r.split_once(char::is_whitespace))) {
                    (None, Some((n, code))) => (n.parse().unwrap_or(1), code),
                    (n, _) => (n.unwrap_or(1), arg),
                };
                let statements = parse_script(code)?;
                let t0 = Instant::now();
                let mut bytes = 0;
                for _ in 0..count {
                    for s in &statements {
                        bytes = self.exclusive(s)?.approx_bytes();
                    }
                }
                let ms = t0.elapsed().as_millis() as i64;
                Ok(match cmd {
                    "t" => K::Long(ms),
                    _ => K::Vector(longs(vec![ms, bytes as i64])),
                })
            }
            _ => self.on_mirror(whole),
        }
    }

    /// A functional IPC message ``(`f; arg; ...)`` as an expression.
    fn message(&mut self, msg: &K) -> KResult<K> {
        let K::Vector(ColVec::General(_, items)) = msg else { return Err(KError::ty("not a functional message")) };
        let head = match &items[0] {
            K::Sym(s) => Expr::Var(*s),
            K::Vector(ColVec::Char(_, code)) => {
                let mut parsed = parse_script(&String::from_utf8_lossy(code))?;
                parsed.pop().ok_or_else(|| KError::syntax("empty function"))?
            }
            other => Expr::Lit(other.clone()),
        };
        let args = items[1..].iter().map(|a| Some(Expr::Lit(a.clone()))).collect();
        self.plan.clear();
        self.statement(&Expr::Apply { target: Box::new(head), args })
    }
}

/// Equal rows, columns and attributes.
fn same_table(a: &K, b: &K) -> bool {
    let attrs = |k: &K| unkey(k).map(|(f, n)| (f.vals.iter().map(ColVec::attr).collect::<Vec<_>>(), n));
    a == b && attrs(a) == attrs(b)
}

/// Ordinals of a slice's rows (`None` for a replicated table).
type Part = (Flip, Option<Vec<i64>>);

impl Inner {
    /// The rows of a table as last known, for choices that need not ask the shards again.
    pub(crate) fn rows_of(&self, t: &Table) -> KResult<usize> {
        if let Some(&n) = self.rows.lock().unwrap_or_else(|e| e.into_inner()).get(&t.name) {
            return Ok(n);
        }
        let n = self.count(t)?;
        self.rows.lock().unwrap_or_else(|e| e.into_inner()).insert(t.name, n);
        Ok(n)
    }

    pub(crate) fn count(&self, t: &Table) -> KResult<usize> {
        let query = Work::Q(format!("count {}", resolve(t.name)));
        if t.mode == DistMode::Replicated {
            return Ok(self.wait(self.shard(0)?.query(query))?.as_i64()? as usize);
        }
        let mut n = 0;
        for k in self.all(query)? {
            n += k.as_i64()? as usize;
        }
        Ok(n)
    }

    /// The whole table, its rows in the order one engine would hold them.
    pub(crate) fn gather(&self, t: &Table) -> KResult<K> {
        let flip = if t.mode == DistMode::Replicated {
            let k = self.wait(self.shard(0)?.query(Work::Eval(Box::new(Expr::Var(t.name)))))?;
            unkey(&k).ok_or_else(|| cluster_error("a shard did not return a table"))?.0
        } else {
            let parts = self.all(Work::Eval(Box::new(augmented(t.name))))?;
            in_order(parts.iter().filter_map(|k| unkey(k).map(|f| f.0)).collect())?
        };
        Ok(rekey(with_attrs(flip, &t.schema), t.nkeys))
    }

    /// Which rows go to which shard, with new ordinals for them.
    fn split(&self, t: &Table, rows: &Flip) -> KResult<Vec<Part>> {
        let (n, slots) = (self.shards().len(), self.slots());
        let Some(layout) = Layout::of(t) else { return Ok(vec![(rows.clone(), None); n]) };
        let keys = layout
            .cols()
            .iter()
            .map(|&c| rows.col(c).ok_or_else(|| KError::value(resolve(c))))
            .collect::<KResult<Vec<_>>>()?;
        let base = self.next_ord.fetch_add(rows.nrows() as i64, Ordering::SeqCst);
        let mut owned = vec![Vec::new(); n];
        for r in 0..rows.nrows() {
            let values: Vec<K> = keys.iter().map(|k| k.get_atom(r)).collect();
            owned[layout.place(&values, &slots)].push(r);
        }
        Ok(owned
            .into_iter()
            .map(|idx| (rows.gather(&idx), Some(idx.iter().map(|&r| base + r as i64).collect())))
            .collect())
    }
}

impl Interceptor for ClusterSession {
    fn eval_query(&mut self, code: &str) -> Option<KResult<K>> {
        Some(self.eval(code))
    }
    fn eval_message(&mut self, msg: &K) -> Option<KResult<K>> {
        let passed = self.passed_on(|| Forwarded::Message(encode_k_payload(msg)));
        Some(passed.unwrap_or_else(|| self.message(msg)))
    }
    fn explain_query(&mut self, code: &str) -> Option<KResult<Vec<String>>> {
        Some(self.explain(code))
    }
}
