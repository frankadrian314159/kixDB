//! `q` workspace state, session state, expression evaluator, lambdas, projections, adverbs,
//! protected evaluation (`@[f;x;h]` / `.[f;args;h]`), views (`::`), and system commands (`\...`).
pub mod joins;
pub mod ops;
pub mod qsql;
pub mod tree;

use crate::ast::Expr;
use crate::error::{KError, KResult};
use crate::parser::{is_binary_keyword, is_nary_keyword, is_unary_keyword, parse_script};
use crate::persist::{LogOp, Persist, Replicator};
use crate::storage::{self, PartitionedTable};
use crate::sym::{intern, resolve, sym_count, SymId};
use crate::types::*;
use chrono::{Datelike, Local, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

// ----- Result & Event types for CLI, GUI, and Cluster -----------------------

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub types: Vec<String>,
    pub key_count: usize,
    pub rows: Vec<Vec<K>>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    Rows(ResultSet),
    Value(K),
    Message(String),
    Error(KError),
}

#[derive(Clone, Debug, Default)]
pub struct BatchResult {
    pub events: Vec<Event>,
}

impl BatchResult {
    pub fn first_error(&self) -> Option<&KError> {
        self.events.iter().find_map(|e| if let Event::Error(err) = e { Some(err) } else { None })
    }

    pub fn resultsets(&self) -> Vec<&ResultSet> {
        self.events.iter().filter_map(|e| if let Event::Rows(rs) = e { Some(rs) } else { None }).collect()
    }

    pub fn values(&self) -> Vec<&K> {
        self.events.iter().filter_map(|e| if let Event::Value(v) = e { Some(v) } else { None }).collect()
    }
}

pub fn k_to_resultset(k: &K) -> Option<ResultSet> {
    match k {
        K::Table(t) => Some(flip_to_resultset(t, 0)),
        K::KeyedTable(kt) => Some(flip_to_resultset(&kt.unkey(), kt.keys.ncols())),
        _ => None,
    }
}

pub fn flip_to_resultset(f: &Flip, key_count: usize) -> ResultSet {
    let columns: Vec<String> = f.cols.iter().map(|&s| resolve(s)).collect();
    let types: Vec<String> = f.vals.iter().map(|v| v.type_name().to_string()).collect();
    let nrows = f.nrows();
    let mut rows = Vec::with_capacity(nrows);
    for r in 0..nrows {
        rows.push(f.vals.iter().map(|c| c.get_atom(r)).collect());
    }
    ResultSet { columns, types, key_count, rows }
}

// ----- Views & Distribution Metadata ----------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ViewDef {
    pub name: SymId,
    pub source: String,
    pub expr: Expr,
    #[serde(skip)]
    pub cached: Option<K>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DistMode {
    Sharded { key: SymId },
    Partitioned { col: SymId },
    Replicated,
    /// Placed by the values of several columns together, such as `date` and `sym`. (Last, so that saved snapshots
    /// written before it existed still load.)
    CoPartitioned { cols: Vec<SymId> },
}

// ----- Server / Workspace State ---------------------------------------------

pub struct Workspace {
    pub vars: HashMap<SymId, K>,
    pub views: HashMap<SymId, ViewDef>,
    pub dist: HashMap<SymId, DistMode>,
    pub partitioned: HashMap<SymId, PartitionedTable>,
    pub data_dir: Option<PathBuf>,
    pub clock: u64,
    pub epoch: u64,
    pub persist: Mutex<Option<Persist>>,
    pub replicator: Mutex<Option<Arc<dyn Replicator>>>,
}

impl Default for Workspace {
    fn default() -> Self {
        Self::new()
    }
}

impl Workspace {
    pub fn new() -> Self {
        Self {
            vars: HashMap::new(),
            views: HashMap::new(),
            dist: HashMap::new(),
            partitioned: HashMap::new(),
            data_dir: None,
            clock: 0,
            epoch: 0,
            persist: Mutex::new(None),
            replicator: Mutex::new(None),
        }
    }

    pub fn invalidate_views(&mut self) {
        for v in self.views.values_mut() {
            v.cached = None;
        }
    }

    pub fn approx_memory_bytes(&self) -> usize {
        self.vars.values().map(|v| v.approx_bytes()).sum()
    }
}

pub struct SessionState {
    pub id: u64,
    pub login: String,
    pub current_ns: String,
    pub handles: HashMap<i32, crate::ipc::IpcClient>,
    pub next_handle: i32,
}

impl SessionState {
    pub fn new(id: u64, login: impl Into<String>) -> Self {
        Self {
            id,
            login: login.into(),
            current_ns: String::new(),
            handles: HashMap::new(),
            next_handle: 3,
        }
    }
}

// ----- Execution Engine -----------------------------------------------------

struct TableScope {
    flip: Flip,
    orig_i: Option<Vec<usize>>,
}

struct ReturnSignal(K);

pub struct Engine<'a> {
    pub ws: &'a mut Workspace,
    pub session: &'a mut SessionState,
    locals: Vec<HashMap<SymId, K>>,
    table_scopes: Vec<TableScope>,
    pub messages: Vec<String>,
    /// Suppress WAL/replication recording while replaying a log or installing a snapshot.
    pub replaying: bool,
}

enum StepErr {
    Err(KError),
    Ret(ReturnSignal),
}

impl From<KError> for StepErr {
    fn from(e: KError) -> Self {
        StepErr::Err(e)
    }
}

impl<'a> Engine<'a> {
    pub fn new(ws: &'a mut Workspace, session: &'a mut SessionState) -> Self {
        Self {
            ws,
            session,
            locals: Vec::new(),
            table_scopes: Vec::new(),
            messages: Vec::new(),
            replaying: false,
        }
    }

    pub fn eval_str(&mut self, src: &str) -> KResult<K> {
        if let Some(q) = src.trim().strip_prefix("EXPLAIN ") {
            let stmts = parse_script(q)?;
            let lines: Vec<K> = stmts
                .iter()
                .enumerate()
                .map(|(idx, s)| K::string(&format!("Statement {}: {}", idx + 1, crate::describe_expr_plan(s, self.ws))))
                .collect();
            return Ok(K::Vector(ColVec::General(Attr::None, Arc::new(lines))));
        }
        let stmts = parse_script(src)?;
        if stmts.is_empty() {
            return Ok(K::Null);
        }
        let mut last = K::Null;
        for s in &stmts {
            last = self.eval_expr(s)?;
        }
        Ok(last)
    }

    pub fn run_batch(&mut self, src: &str) -> Vec<Event> {
        let mut events = Vec::new();
        let stmts = match parse_script(src) {
            Ok(s) => s,
            Err(e) => {
                events.push(Event::Error(e));
                return events;
            }
        };
        for s in &stmts {
            match self.eval_expr(s) {
                Ok(val) => {
                    for m in self.messages.drain(..) {
                        events.push(Event::Message(m));
                    }
                    // kdb+ prints nothing for a top-level assignment
                    if !matches!(val, K::Null) && !matches!(s, Expr::Assign { .. }) {
                        if let Some(rs) = k_to_resultset(&val) {
                            events.push(Event::Rows(rs));
                        } else {
                            events.push(Event::Value(val));
                        }
                    }
                }
                Err(e) => {
                    for m in self.messages.drain(..) {
                        events.push(Event::Message(m));
                    }
                    events.push(Event::Error(e));
                    break;
                }
            }
        }
        events
    }

    pub fn has_partitioned(&self, name: SymId) -> bool {
        self.ws.partitioned.contains_key(&name)
    }

    pub fn resolve_table_for_query(&mut self, name: SymId, where_clauses: &[Expr], needed: Option<&[SymId]>) -> KResult<K> {
        if let Some(pt) = self.ws.partitioned.get(&name).cloned() {
            let flip = storage::load_partitioned_filtered(&pt, where_clauses, needed, |e| self.eval_expr(e))?;
            return Ok(K::Table(Arc::new(flip)));
        }
        self.get_var(name)
    }

    pub fn eval_in_table_scope(&mut self, flip: &Flip, expr: &Expr) -> KResult<K> {
        self.table_scopes.push(TableScope { flip: flip.clone(), orig_i: None });
        let r = self.eval_expr(expr);
        self.table_scopes.pop();
        r
    }

    pub fn eval_in_table_scope_with_orig_i(&mut self, flip: &Flip, orig_i: &[usize], expr: &Expr) -> KResult<K> {
        self.table_scopes.push(TableScope { flip: flip.clone(), orig_i: Some(orig_i.to_vec()) });
        let r = self.eval_expr(expr);
        self.table_scopes.pop();
        r
    }

    /// Whether using `name` can reach other names at run time: it is a view, or a variable holding a function that is
    /// not a lambda (such as an alias of `value`).
    fn reaches_names(&self, name: SymId) -> bool {
        let keys = [self.qualify(name), name];
        let held = self.locals.last().and_then(|f| f.get(&name)).or_else(|| keys.iter().find_map(|k| self.ws.vars.get(k)));
        matches!(held, Some(K::Func(f)) if !matches!(f, Func::Lambda(_))) || keys.iter().any(|k| self.ws.views.contains_key(k))
    }

    fn qualify(&self, name: SymId) -> SymId {
        let s = resolve(name);
        if s.starts_with('.') || self.session.current_ns.is_empty() {
            name
        } else {
            intern(&format!("{}.{s}", self.session.current_ns))
        }
    }

    pub fn set_global(&mut self, name: SymId, val: K) -> KResult<()> {
        let qname = self.qualify(name);
        self.set_globals(vec![(qname, Some(val))])
    }

    /// Set (`Some`) or drop (`None`) several fully qualified globals as one log record, so that a crash or a replica sees
    /// all of the changes or none. Ephemeral `.kix.tmp.*` shuffle/broadcast tables stay in memory only.
    pub fn set_globals(&mut self, changes: Vec<(SymId, Option<K>)>) -> KResult<()> {
        let ops: Vec<LogOp> = changes
            .iter()
            .filter(|(name, _)| !is_ephemeral_var(*name))
            .map(|(name, val)| match val {
                Some(val) => LogOp::SetVar { name: *name, val: val.clone() },
                None => LogOp::DropVar { name: *name },
            })
            .collect();
        if !ops.is_empty() {
            self.record_ops(ops)?;
            self.ws.invalidate_views();
        }
        for (name, val) in changes {
            self.ws.views.remove(&name);
            match val {
                Some(val) => self.ws.vars.insert(name, val),
                None => self.ws.vars.remove(&name),
            };
        }
        Ok(())
    }

    /// Log (and replicate) a change that the caller makes only if this succeeds.
    fn record_ops(&mut self, ops: Vec<LogOp>) -> KResult<()> {
        if self.replaying {
            return Ok(());
        }
        let rec = crate::persist::WalRecord { ts: self.ws.clock + 1, ops };
        let rep = self.ws.replicator.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let mut persist = self.ws.persist.lock().unwrap_or_else(|e| e.into_inner());
        // checkpoint before appending: the snapshot holds exactly what the truncated log held
        if let Some(p) = persist.as_mut() {
            if p.opts.checkpoint_every > 0 && p.wal_records >= p.opts.checkpoint_every as u64 {
                p.checkpoint(self.ws)?;
            }
        }
        if let Some(r) = &rep {
            r.propose(&rec)?;
        }
        if let Some(p) = persist.as_mut() {
            p.append_record(&rec, rep.is_none())?;
        }
        drop(persist);
        self.ws.clock = rec.ts;
        self.ws.epoch += 1;
        Ok(())
    }

    pub fn get_var(&mut self, name: SymId) -> KResult<K> {
        // 1. Active table scope (for qSQL `where`, `select`, `by`, `update`)
        if let Some(ts) = self.table_scopes.last() {
            let s = resolve(name);
            if s == "i" {
                let idx: Vec<i64> = match &ts.orig_i {
                    Some(orig) => orig.iter().map(|&r| r as i64).collect(),
                    None => (0..ts.flip.nrows() as i64).collect(),
                };
                return Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(idx))));
            }
            if let Some(col) = ts.flip.col(name) {
                return Ok(K::Vector(col.clone()));
            }
        }

        // 2. Lambda local scope
        if let Some(frame) = self.locals.last() {
            if let Some(v) = frame.get(&name) {
                return Ok(v.clone());
            }
        }

        let s = resolve(name);

        // 3. Built-in keywords & `.z` / `.Q` / `.kix` namespaces
        if is_unary_keyword(&s) || is_binary_keyword(&s) || is_nary_keyword(&s) {
            return Ok(K::Func(Func::Prim(name)));
        }
        if let Some(sys_val) = self.eval_builtin_ns(&s)? {
            return Ok(sys_val);
        }

        // 4. Current namespace or root global variable / view / partitioned table
        let qname = self.qualify(name);
        if let Some(v) = self.ws.vars.get(&qname) {
            return Ok(v.clone());
        }
        if qname != name {
            if let Some(v) = self.ws.vars.get(&name) {
                return Ok(v.clone());
            }
        }
        if let Some(vdef) = self.ws.views.get(&qname).or_else(|| self.ws.views.get(&name)).cloned() {
            if let Some(cached) = vdef.cached {
                return Ok(cached);
            }
            let computed = self.eval_expr(&vdef.expr)?;
            if let Some(slot) = self.ws.views.get_mut(&vdef.name) {
                slot.cached = Some(computed.clone());
            }
            return Ok(computed);
        }
        if let Some(pt) = self.ws.partitioned.get(&name).cloned() {
            let flip = storage::load_partitioned_filtered(&pt, &[], None, |e| self.eval_expr(e))?;
            return Ok(K::Table(Arc::new(flip)));
        }

        Err(KError::value(s))
    }

    fn eval_builtin_ns(&self, s: &str) -> KResult<Option<K>> {
        match s {
            ".z.p" => {
                let now = Utc::now();
                let nd = now.date_naive();
                let days = date_from_ymd(nd.year(), nd.month(), nd.day()).unwrap_or(0) as i64;
                let ns_day = (now.num_seconds_from_midnight() as i64) * NANOS_PER_SEC + (now.nanosecond() as i64);
                Ok(Some(K::Timestamp(days * NANOS_PER_DAY + ns_day)))
            }
            ".z.P" => {
                let now = Local::now();
                let nd = now.date_naive();
                let days = date_from_ymd(nd.year(), nd.month(), nd.day()).unwrap_or(0) as i64;
                let ns_day = (now.num_seconds_from_midnight() as i64) * NANOS_PER_SEC + (now.nanosecond() as i64);
                Ok(Some(K::Timestamp(days * NANOS_PER_DAY + ns_day)))
            }
            ".z.d" => {
                let d = Utc::now().date_naive();
                Ok(Some(K::Date(date_from_ymd(d.year(), d.month(), d.day()).unwrap_or(0))))
            }
            ".z.D" => {
                let d = Local::now().date_naive();
                Ok(Some(K::Date(date_from_ymd(d.year(), d.month(), d.day()).unwrap_or(0))))
            }
            ".z.t" => {
                let now = Utc::now();
                let ms = (now.num_seconds_from_midnight() * 1000 + now.nanosecond() / 1_000_000) as i32;
                Ok(Some(K::Time(ms)))
            }
            ".z.T" => {
                let now = Local::now();
                let ms = (now.num_seconds_from_midnight() * 1000 + now.nanosecond() / 1_000_000) as i32;
                Ok(Some(K::Time(ms)))
            }
            ".z.K" => Ok(Some(K::Float(4.1))),
            ".z.i" => Ok(Some(K::Int(std::process::id() as i32))),
            ".z.u" => Ok(Some(K::Sym(intern(&self.session.login)))),
            ".z.o" => Ok(Some(K::Sym(intern(if cfg!(windows) { "w64" } else if cfg!(target_os = "macos") { "m64" } else { "l64" })))),
            ".Q.en" | ".Q.dpft" | ".Q.s" | ".kix.dist" | ".kix.objects" | ".kix.def" => Ok(Some(K::Func(Func::Prim(intern(s))))),
            _ => Ok(None),
        }
    }

    pub fn eval_expr(&mut self, expr: &Expr) -> KResult<K> {
        match self.eval_step(expr) {
            Ok(k) => Ok(k),
            Err(StepErr::Err(e)) => Err(e),
            Err(StepErr::Ret(r)) => Ok(r.0),
        }
    }

    fn eval_step(&mut self, expr: &Expr) -> Result<K, StepErr> {
        match expr {
            Expr::Nop => Ok(K::Null),
            Expr::Lit(k) => Ok(k.clone()),
            Expr::Var(name) => Ok(self.get_var(*name)?),

            Expr::Field(base, field) => {
                // Check if `base.field` is a dotted global variable name when `base` is a Var (`ns.foo`)!
                if let Expr::Var(b) = &**base {
                    let b_str = resolve(*b);
                    let f_str = resolve(*field);
                    let full = intern(&format!("{b_str}.{f_str}"));
                    if self.ws.vars.contains_key(&full) {
                        return Ok(self.ws.vars[&full].clone());
                    }
                }
                let v = self.eval_step(base)?;
                Ok(ops::eval_field(&v, *field)?)
            }

            Expr::List(items) => {
                // In `q`, list elements evaluate right-to-left!
                let mut vals = vec![K::Null; items.len()];
                for (i, item) in items.iter().enumerate().rev() {
                    vals[i] = self.eval_step(item)?;
                }
                Ok(K::Vector(ColVec::from_atoms(vals)))
            }

            Expr::TableLit { keys, vals } => {
                let k_flip = self.eval_table_cols(keys)?;
                let v_flip = self.eval_table_cols(vals)?;
                if keys.is_empty() {
                    Ok(K::Table(Arc::new(v_flip)))
                } else {
                    Ok(K::KeyedTable(Arc::new(KeyedTable::new(k_flip, v_flip)?)))
                }
            }

            Expr::Lambda { params, implicit, body, source } => Ok(K::Func(Func::Lambda(Arc::new(Lambda {
                params: params.clone(),
                implicit: *implicit,
                body: body.clone(),
                source: source.clone(),
                ns: self.session.current_ns.clone(),
            })))),

            Expr::Adverb { kind, func } => {
                let fval = self.eval_step(func)?;
                let K::Func(inner) = fval else {
                    return Err(KError::ty("adverb requires a function or operator").into());
                };
                Ok(K::Func(Func::Adverb { kind: *kind, inner: Box::new(inner) }))
            }

            Expr::Unary { op, arg } => {
                let x = self.eval_step(arg)?;
                Ok(self.eval_unary_dispatch(*op, &x)?)
            }

            Expr::Binary { op, lhs, rhs } => {
                let op_name = resolve(*op);
                // Protected evaluation / functional amend when `@` or `.` is used with bracketed multi-args is `Expr::Apply`,
                // while infix `f @ x` and `f . args` evaluate `rhs` first (right-to-left!), then `lhs`!
                let rval = self.eval_step(rhs)?;
                // Special case: `0N ! x` prints `x` and returns `x`!
                if op_name == "!" && matches!(&**lhs, Expr::Lit(K::Long(NULL_LONG))) {
                    self.messages.push(rval.q_repr());
                    return Ok(rval);
                }
                let lval = self.eval_step(lhs)?;
                Ok(self.eval_binary_dispatch(&op_name, &lval, &rval)?)
            }

            Expr::Juxt { func, arg } => {
                let rval = self.eval_step(arg)?;
                let fval = self.eval_step(func)?;
                Ok(self.apply_value(&fval, &[Some(rval)])?)
            }

            Expr::Apply { target, args } => {
                // Special forms: `@[target; i; f; ...]` (protected eval / functional amend) and `.[target; path; f; ...]`!
                if let Expr::Lit(K::Func(Func::Prim(op_sym))) = &**target {
                    let op_s = resolve(*op_sym);
                    if (op_s == "@" || op_s == ".") && args.len() >= 3 {
                        return self.eval_trap_or_amend(&op_s, args);
                    }
                    if op_s == "?" && (args.len() == 4 || args.len() == 5 || args.len() == 6) {
                        return Ok(self.eval_functional_select(args)?);
                    }
                    if op_s == "!" && args.len() == 4 {
                        return Ok(self.eval_functional_update(args)?);
                    }
                }
                // Evaluate arguments right-to-left (preserving `None` for projection slots!)
                let mut evaled: Vec<Option<K>> = vec![None; args.len()];
                for (i, a) in args.iter().enumerate().rev() {
                    if let Some(ex) = a {
                        evaled[i] = Some(self.eval_step(ex)?);
                    }
                }
                let tval = self.eval_step(target)?;
                Ok(self.apply_value(&tval, &evaled)?)
            }

            Expr::Assign { name, global, op, val } => {
                // Top-level `name :: expr` (outside any lambda, when `op.is_none()`) defines a lazy View in kdb+!
                if *global && self.locals.is_empty() && op.is_none() {
                    let qname = self.qualify(*name);
                    let src_str = format!("{qname}:: ...");
                    let vdef = ViewDef {
                        name: qname,
                        source: src_str,
                        expr: (**val).clone(),
                        cached: None,
                    };
                    self.record_ops(vec![LogOp::SetView { def: vdef.clone() }])?;
                    self.ws.vars.remove(&qname);
                    self.ws.views.insert(qname, vdef);
                    return Ok(K::Null);
                }

                let rhs = self.eval_step(val)?;
                let final_val = if let Some(op_sym) = op {
                    let cur = self.get_var(*name)?;
                    self.eval_binary_dispatch(&resolve(*op_sym), &cur, &rhs)?
                } else {
                    rhs
                };

                if !*global && !self.locals.is_empty() {
                    self.locals.last_mut().unwrap().insert(*name, final_val.clone());
                } else {
                    self.set_global(*name, final_val.clone())?;
                }
                Ok(final_val)
            }

            Expr::IndexAssign { name, indices, op, val } => {
                let rhs = self.eval_step(val)?;
                let mut idx_vals: Vec<Option<K>> = vec![None; indices.len()];
                for (i, ix) in indices.iter().enumerate().rev() {
                    if let Some(e) = ix {
                        idx_vals[i] = Some(self.eval_step(e)?);
                    }
                }
                let is_local = self.locals.last().is_some_and(|f| f.contains_key(name));
                let mut target = self.get_var(*name)?;
                target = self.amend_in_place(target, &idx_vals, *op, &rhs)?;
                if is_local {
                    self.locals.last_mut().unwrap().insert(*name, target);
                } else {
                    self.set_global(*name, target)?;
                }
                Ok(K::Null)
            }

            Expr::Cond(parts) => {
                let mut i = 0;
                while i + 1 < parts.len() {
                    let c = self.eval_step(&parts[i])?;
                    if c.as_bool()? {
                        return self.eval_step(&parts[i + 1]);
                    }
                    i += 2;
                }
                self.eval_step(parts.last().unwrap())
            }

            Expr::If { cond, body } => {
                if self.eval_step(cond)?.as_bool()? {
                    for b in body {
                        self.eval_step(b)?;
                    }
                }
                Ok(K::Null)
            }

            Expr::Do { count, body } => {
                let n = self.eval_step(count)?.as_i64()?.max(0);
                for _ in 0..n {
                    for b in body {
                        self.eval_step(b)?;
                    }
                }
                Ok(K::Null)
            }

            Expr::While { cond, body } => {
                while self.eval_step(cond)?.as_bool()? {
                    for b in body {
                        self.eval_step(b)?;
                    }
                }
                Ok(K::Null)
            }

            Expr::Return(inner) => {
                let v = self.eval_step(inner)?;
                Err(StepErr::Ret(ReturnSignal(v)))
            }

            Expr::Signal(inner) => {
                let v = self.eval_step(inner)?;
                let sig = v.as_str().unwrap_or_else(|| v.cell_text());
                Err(StepErr::Err(KError::signal(sig)))
            }

            Expr::Query(q) => Ok(self.eval_qsql(q)?),

            Expr::SysCmd { cmd, arg } => Ok(self.eval_sys_cmd(cmd, arg)?),
        }
    }

    fn eval_table_cols(&mut self, defs: &[(SymId, Expr)]) -> Result<Flip, StepErr> {
        if defs.is_empty() {
            return Ok(Flip { cols: vec![], vals: vec![] });
        }
        let mut names = Vec::with_capacity(defs.len());
        let mut raw_vals = vec![K::Null; defs.len()];
        for (i, (n, _)) in defs.iter().enumerate() {
            names.push(*n);
            let _ = i;
        }
        for (i, (_, ex)) in defs.iter().enumerate().rev() {
            raw_vals[i] = self.eval_step(ex)?;
        }
        // Broadcast scalar column values across vector length!
        let mut max_len = 0usize;
        let mut any_vec = false;
        for v in &raw_vals {
            if let K::Vector(cv) = v {
                if !matches!(cv, ColVec::Char(_, _)) || cv.is_empty() || raw_vals.len() == 1 {
                    any_vec = true;
                    max_len = max_len.max(cv.len());
                }
            }
        }
        let target_len = if any_vec { max_len } else { 1 };
        let mut col_vecs = Vec::with_capacity(defs.len());
        for v in raw_vals {
            match v {
                K::Vector(cv) if !matches!(cv, ColVec::Char(_, _)) || (any_vec && cv.len() == target_len) => {
                    if cv.len() != target_len {
                        return Err(KError::length("table literal column lengths differ").into());
                    }
                    col_vecs.push(cv);
                }
                atom => col_vecs.push(ColVec::repeat(atom, target_len)),
            }
        }
        Ok(Flip::new(names, col_vecs)?)
    }

    fn eval_unary_dispatch(&mut self, op: SymId, x: &K) -> KResult<K> {
        let name = resolve(op);
        match name.as_str() {
            "tables" => {
                let mut syms: Vec<SymId> = self
                    .ws
                    .vars
                    .iter()
                    .filter_map(|(&k, v)| if matches!(v, K::Table(_) | K::KeyedTable(_)) { Some(k) } else { None })
                    .chain(self.ws.partitioned.keys().copied())
                    .collect();
                syms.sort();
                syms.dedup();
                Ok(K::Vector(ColVec::Sym(Attr::Sorted, Arc::new(syms))))
            }
            "views" => {
                let mut syms: Vec<SymId> = self.ws.views.keys().copied().collect();
                syms.sort();
                Ok(K::Vector(ColVec::Sym(Attr::Sorted, Arc::new(syms))))
            }
            "show" => {
                self.messages.push(x.q_repr());
                Ok(K::Null)
            }
            "get" => self.eval_get(x),
            "parse" => self.eval_parse(x),
            "eval" => self.eval_tree(x),
            "hopen" => self.eval_hopen(x),
            "hclose" => self.eval_hclose(x),
            "hcount" => storage::hcount(x),
            "system" => {
                let s = x.as_str().ok_or_else(|| KError::ty("system expects string"))?;
                let (cmd, arg) = match s.find(char::is_whitespace) {
                    Some(p) => (&s[..p], s[p..].trim()),
                    None => (s.as_str(), ""),
                };
                self.eval_sys_cmd(cmd, arg)
            }
            "value" => {
                if let K::Vector(ColVec::Char(_, bs)) = x {
                    let code = String::from_utf8_lossy(bs).into_owned();
                    self.eval_str(&code)
                } else if let K::Sym(s) = x {
                    if resolve(*s).starts_with(':') {
                        self.eval_get(x)
                    } else {
                        self.get_var(*s)
                    }
                } else {
                    ops::eval_unary_prim("value", x)
                }
            }
            ".Q.s" => Ok(K::string(&x.q_repr())),
            ".kix.objects" => crate::inspect::workspace_objects_table(self.ws),
            ".kix.def" => {
                let target = x.as_str().unwrap_or_else(|| x.cell_text());
                crate::inspect::workspace_definition(self.ws, &target)
            }
            _ => ops::eval_unary_prim(&name, x),
        }
    }

    fn eval_binary_dispatch(&mut self, op: &str, lhs: &K, rhs: &K) -> KResult<K> {
        match op {
            "@" => self.apply_value(lhs, &[Some(rhs.clone())]),
            "each" | "over" | "scan" | "prior" => {
                let K::Func(f) = lhs else {
                    return Err(KError::ty(format!("{op} requires a function on its left")));
                };
                let kind = match op {
                    "each" => AdverbKind::Each,
                    "over" => AdverbKind::Over,
                    "scan" => AdverbKind::Scan,
                    _ => AdverbKind::EachPrior,
                };
                self.eval_adverb(kind, f, std::slice::from_ref(rhs))
            }
            "." => {
                let K::Vector(cv) = rhs else {
                    return self.apply_value(lhs, &[Some(rhs.clone())]);
                };
                let args: Vec<Option<K>> = (0..cv.len()).map(|i| Some(cv.get_atom(i))).collect();
                self.apply_value(lhs, &args)
            }
            "set" => self.eval_set(lhs, rhs),
            "insert" => self.eval_insert(lhs, rhs),
            "upsert" => self.eval_upsert(lhs, rhs),
            "fby" => self.eval_fby(lhs, rhs),
            "0:" => storage::zero_colon(lhs, rhs),
            "1:" => storage::one_colon(lhs, rhs),
            "!" => {
                // `-8!x` (IPC serialize to byte vector), `-9!bytes` (IPC deserialize), `-11!logfile`!
                if matches!(lhs, K::Long(_) | K::Int(_)) {
                    match lhs.as_i64()? {
                        -8 => {
                            let bytes = crate::ipc::encode_k_payload(rhs);
                            return Ok(K::Vector(ColVec::Byte(Attr::None, Arc::new(bytes))));
                        }
                        -9 => {
                            let K::Vector(ColVec::Byte(_, bs)) = rhs else {
                                return Err(KError::ty("-9! expects byte vector"));
                            };
                            let (k, _) = crate::ipc::decode_k_payload(bs, 0)?;
                            return Ok(k);
                        }
                        _ => {}
                    }
                }
                ops::eval_binary_prim("!", lhs, rhs)
            }
            ".Q.en" => storage::q_en(lhs, rhs),
            _ => ops::eval_binary_prim(op, lhs, rhs),
        }
    }

    pub fn apply_value(&mut self, target: &K, args: &[Option<K>]) -> KResult<K> {
        match target {
            K::Func(f) => self.apply_func_opt(f, args),
            // IPC handle call: `h "select from trade"` (sync) or `(neg h) "upd..."` (async)!
            K::Int(h) if self.session.handles.contains_key(&h.abs()) => {
                let arg = args.first().and_then(|a| a.clone()).unwrap_or(K::Null);
                let hid = h.abs();
                let sync = *h > 0;
                let client = self.session.handles.get_mut(&hid).ok_or_else(|| KError::ipc("closed handle"))?;
                if sync {
                    client.query_k(&arg)
                } else {
                    client.send_async_k(&arg)?;
                    Ok(K::Null)
                }
            }
            K::Long(h) if (*h as i32 != 0) && self.session.handles.contains_key(&((*h as i32).abs())) => {
                let hi = *h as i32;
                self.apply_value(&K::Int(hi), args)
            }
            // Symbol handle one-shot IPC `` `:localhost:5000 "1+1" ``
            K::Sym(s) if resolve(*s).starts_with("::") || (resolve(*s).starts_with(':') && resolve(*s)[1..].contains(':')) => {
                let spec = resolve(*s);
                let addr = spec.trim_start_matches(':');
                let mut client = crate::ipc::IpcClient::connect_addr(addr, &self.session.login, "")?;
                let arg = args.first().and_then(|a| a.clone()).unwrap_or(K::Null);
                client.query_k(&arg)
            }
            other => ops::index_k(other, args),
        }
    }

    pub fn apply_func(&mut self, func: &Func, concrete: &[K]) -> KResult<K> {
        let opt: Vec<Option<K>> = concrete.iter().cloned().map(Some).collect();
        self.apply_func_opt(func, &opt)
    }

    pub fn apply_func_opt(&mut self, func: &Func, args: &[Option<K>]) -> KResult<K> {
        // `f[]` calls with the generic null rather than making a projection
        let niladic = [Some(K::Null)];
        let args = if matches!(args, [None]) { &niladic[..] } else { args };
        let arity = self.func_arity(func);
        // Check for omitted projection slots (`f[1;]` or partial application `+[1]`)!
        let has_missing = args.iter().any(|a| a.is_none() || matches!(a, Some(K::Null)) && args.len() > 1);
        if (has_missing || (args.len() < arity && !(arity == 2 && args.len() == 1 && is_ambivalent_prim(func)))) && arity > 0 {
            let mut full = vec![None; arity.max(args.len())];
            for (i, a) in args.iter().enumerate() {
                if !matches!(a, None | Some(K::Null)) {
                    full[i] = a.clone();
                }
            }
            return Ok(K::Func(Func::Proj {
                func: Box::new(func.clone()),
                args: full,
                total_arity: arity.max(args.len()),
            }));
        }

        let vals: Vec<K> = args.iter().map(|a| a.clone().unwrap_or(K::Null)).collect();
        match func {
            Func::Prim(sym) => self.call_prim(*sym, &vals),
            Func::Lambda(lam) => self.call_lambda(lam, &vals),
            Func::Proj { func: inner, args: bound, .. } => {
                let mut merged: Vec<Option<K>> = Vec::with_capacity(bound.len());
                let mut vi = 0usize;
                for slot in bound {
                    if slot.is_some() {
                        merged.push(slot.clone());
                    } else if vi < vals.len() {
                        merged.push(Some(vals[vi].clone()));
                        vi += 1;
                    } else {
                        merged.push(None);
                    }
                }
                while vi < vals.len() {
                    merged.push(Some(vals[vi].clone()));
                    vi += 1;
                }
                self.apply_func_opt(inner, &merged)
            }
            Func::Comp(f, g) => {
                let mid = self.apply_func(g, &vals)?;
                self.apply_func(f, &[mid])
            }
            Func::Adverb { kind, inner } => self.eval_adverb(*kind, inner, &vals),
        }
    }

    fn func_arity(&self, func: &Func) -> usize {
        match func {
            Func::Lambda(l) => l.params.len(),
            Func::Proj { args, .. } => args.iter().filter(|a| a.is_none()).count(),
            Func::Comp(_, g) => self.func_arity(g),
            Func::Adverb { kind, inner } => match kind {
                AdverbKind::EachRight | AdverbKind::EachLeft => 2,
                _ => self.func_arity(inner).min(2),
            },
            Func::Prim(s) => {
                let n = resolve(*s);
                match n.as_str() {
                    "aj" | "aj0" | "ej" | "ssr" => 3,
                    "wj" | "wj1" | ".Q.dpft" => 4,
                    ".Q.s" | ".kix.objects" | ".kix.def" => 1,
                    ".kix.dist" => 2,
                    _ if is_unary_keyword(&n) => 1,
                    _ => 2,
                }
            }
        }
    }

    fn call_prim(&mut self, sym: SymId, vals: &[K]) -> KResult<K> {
        let name = resolve(sym);
        match (name.as_str(), vals.len()) {
            ("aj", 3) => joins::asof_join(&vals[0], &vals[1], &vals[2], true),
            ("aj0", 3) => joins::asof_join(&vals[0], &vals[1], &vals[2], false),
            ("ej", 3) => joins::equi_join(&vals[0], &vals[1], &vals[2]),
            ("ssr", 3) => ops::ssr_op(&vals[0], &vals[1], &vals[2]),
            ("wj", 4) => joins::window_join(&vals[0], &vals[1], &vals[2], &vals[3], true, |f, a| self.apply_func(f, a)),
            ("wj1", 4) => joins::window_join(&vals[0], &vals[1], &vals[2], &vals[3], false, |f, a| self.apply_func(f, a)),
            (".Q.dpft", 4) => storage::q_dpft(self.ws, &vals[0], &vals[1], &vals[2], &vals[3]),
            (".kix.dist", 2 | 3) => self.eval_kix_dist(vals),
            ("enlist", 2..) => Ok(K::Vector(ColVec::from_atoms(vals.to_vec()))),
            (_, 1) => self.eval_unary_dispatch(sym, &vals[0]),
            (_, 2) => self.eval_binary_dispatch(&name, &vals[0], &vals[1]),
            _ => Err(KError::rank(format!("wrong number of arguments ({}) to {name}", vals.len()))),
        }
    }

    fn eval_kix_dist(&mut self, vals: &[K]) -> KResult<K> {
        let K::Sym(tbl) = vals[0] else {
            return Err(KError::ty(".kix.dist[table; mode; [col]] expects symbol table name"));
        };
        let K::Sym(mode_s) = vals[1] else {
            return Err(KError::ty(".kix.dist mode must be `sharded, `partitioned, or `replicated"));
        };
        let mode = match resolve(mode_s).as_str() {
            "replicated" => DistMode::Replicated,
            "sharded" => match vals.get(2) {
                None => DistMode::Sharded { key: intern("sym") },
                Some(K::Sym(key)) => DistMode::Sharded { key: *key },
                Some(_) => return Err(KError::ty(".kix.dist `sharded takes one column; use `partitioned for several")),
            },
            "partitioned" => match vals.get(2) {
                None => DistMode::Partitioned { col: intern("date") },
                Some(K::Sym(col)) => DistMode::Partitioned { col: *col },
                Some(K::Vector(ColVec::Sym(_, cols))) if !cols.is_empty() => {
                    if cols.iter().enumerate().any(|(i, c)| cols[..i].contains(c)) {
                        return Err(KError::domain(".kix.dist names a column twice"));
                    }
                    match cols.as_slice() {
                        [col] => DistMode::Partitioned { col: *col },
                        _ => DistMode::CoPartitioned { cols: (**cols).clone() },
                    }
                }
                Some(_) => return Err(KError::ty(".kix.dist partition columns must be symbols")),
            },
            other => return Err(KError::domain(format!("unknown distribution mode `{other}"))),
        };
        self.record_ops(vec![LogOp::SetDist { table: tbl, mode: mode.clone() }])?;
        self.ws.dist.insert(tbl, mode);
        Ok(K::Sym(tbl))
    }

    fn call_lambda(&mut self, lam: &Lambda, vals: &[K]) -> KResult<K> {
        if vals.len() > lam.params.len() && !(lam.params.is_empty() && vals.len() == 1 && matches!(vals[0], K::Null)) {
            return Err(KError::rank(format!(
                "lambda takes {} argument(s), got {}",
                lam.params.len(),
                vals.len()
            )));
        }
        let mut frame = HashMap::with_capacity(lam.params.len() + 4);
        for (p, v) in lam.params.iter().zip(vals.iter()) {
            frame.insert(*p, v.clone());
        }
        let saved_ns = std::mem::replace(&mut self.session.current_ns, lam.ns.clone());
        let saved_tscope = std::mem::take(&mut self.table_scopes);
        self.locals.push(frame);

        let mut last = K::Null;
        let mut err: Option<KError> = None;
        for expr in &lam.body {
            match self.eval_step(expr) {
                Ok(v) => last = v,
                Err(StepErr::Ret(r)) => {
                    last = r.0;
                    break;
                }
                Err(StepErr::Err(e)) => {
                    err = Some(e);
                    break;
                }
            }
        }

        self.locals.pop();
        self.table_scopes = saved_tscope;
        self.session.current_ns = saved_ns;
        match err {
            Some(e) => Err(e),
            None => Ok(last),
        }
    }

    fn eval_adverb(&mut self, kind: AdverbKind, inner: &Func, vals: &[K]) -> KResult<K> {
        match kind {
            AdverbKind::Each => match vals.len() {
                1 => {
                    // over a dictionary, `each` maps the values and keeps the keys
                    if let K::Dict(d) = &vals[0] {
                        let mapped = self.eval_adverb(kind, inner, std::slice::from_ref(&d.vals))?;
                        return Ok(K::Dict(Arc::new(Dict { keys: d.keys.clone(), vals: Box::new(mapped) })));
                    }
                    let K::Vector(cv) = &vals[0] else {
                        return self.apply_func(inner, vals);
                    };
                    let mut out = Vec::with_capacity(cv.len());
                    for i in 0..cv.len() {
                        out.push(self.apply_func(inner, &[cv.get_atom(i)])?);
                    }
                    Ok(K::Vector(ColVec::from_atoms(out)))
                }
                2 => {
                    let (a, b) = (&vals[0], &vals[1]);
                    match (a, b) {
                        (K::Vector(va), K::Vector(vb)) => {
                            if va.len() != vb.len() {
                                return Err(KError::length("each length mismatch"));
                            }
                            let mut out = Vec::with_capacity(va.len());
                            for i in 0..va.len() {
                                out.push(self.apply_func(inner, &[va.get_atom(i), vb.get_atom(i)])?);
                            }
                            Ok(K::Vector(ColVec::from_atoms(out)))
                        }
                        (x, K::Vector(vb)) => {
                            let mut out = Vec::with_capacity(vb.len());
                            for i in 0..vb.len() {
                                out.push(self.apply_func(inner, &[x.clone(), vb.get_atom(i)])?);
                            }
                            Ok(K::Vector(ColVec::from_atoms(out)))
                        }
                        (K::Vector(va), y) => {
                            let mut out = Vec::with_capacity(va.len());
                            for i in 0..va.len() {
                                out.push(self.apply_func(inner, &[va.get_atom(i), y.clone()])?);
                            }
                            Ok(K::Vector(ColVec::from_atoms(out)))
                        }
                        _ => self.apply_func(inner, vals),
                    }
                }
                _ => Err(KError::rank("each takes 1 or 2 arguments")),
            },

            AdverbKind::Over => match vals.len() {
                1 => {
                    // Fast paths for `+/`, `*/`, `&/`, `|/`, `,/`:
                    if let Func::Prim(s) = inner {
                        match resolve(*s).as_str() {
                            "+" => return ops::sum_op(&vals[0]),
                            "*" => return ops::eval_unary_prim("prd", &vals[0]),
                            "&" => return ops::minmax_op(&vals[0], true),
                            "|" => return ops::minmax_op(&vals[0], false),
                            "," => return ops::raze_op(&vals[0]),
                            _ => {}
                        }
                    }
                    // If `inner` is unary -> converge to fixed point! Otherwise binary fold over vector!
                    if self.func_arity(inner) == 1 {
                        let init = vals[0].clone();
                        let mut cur = init.clone();
                        for _ in 0..100_000 {
                            let next = self.apply_func(inner, &[cur.clone()])?;
                            if next == cur || next == init {
                                return Ok(next);
                            }
                            cur = next;
                        }
                        return Ok(cur);
                    }
                    let K::Vector(cv) = &vals[0] else {
                        return Ok(vals[0].clone());
                    };
                    if cv.is_empty() {
                        return Ok(K::Long(0));
                    }
                    let mut acc = cv.get_atom(0);
                    for i in 1..cv.len() {
                        acc = self.apply_func(inner, &[acc, cv.get_atom(i)])?;
                    }
                    Ok(acc)
                }
                2 => {
                    // If `inner` is unary: `n f/ x` (do `n` iterations) or `cond_fn f/ x` (while `cond_fn[x]`)!
                    if self.func_arity(inner) == 1 {
                        if let K::Func(cond_fn) = &vals[0] {
                            let mut cur = vals[1].clone();
                            while self.apply_func(cond_fn, &[cur.clone()])?.as_bool()? {
                                cur = self.apply_func(inner, &[cur])?;
                            }
                            return Ok(cur);
                        }
                        let n = vals[0].as_i64()?.max(0);
                        let mut cur = vals[1].clone();
                        for _ in 0..n {
                            cur = self.apply_func(inner, &[cur])?;
                        }
                        return Ok(cur);
                    }
                    // Seeded fold `seed f/ list`
                    let mut acc = vals[0].clone();
                    if let K::Vector(cv) = &vals[1] {
                        for i in 0..cv.len() {
                            acc = self.apply_func(inner, &[acc, cv.get_atom(i)])?;
                        }
                        Ok(acc)
                    } else {
                        self.apply_func(inner, &[acc, vals[1].clone()])
                    }
                }
                _ => Err(KError::rank("over takes 1 or 2 arguments")),
            },

            AdverbKind::Scan => match vals.len() {
                1 => {
                    if let Func::Prim(s) = inner {
                        match resolve(*s).as_str() {
                            "+" => return ops::eval_unary_prim("sums", &vals[0]),
                            "*" => return ops::eval_unary_prim("prds", &vals[0]),
                            "&" => return ops::eval_unary_prim("mins", &vals[0]),
                            "|" => return ops::eval_unary_prim("maxs", &vals[0]),
                            _ => {}
                        }
                    }
                    if self.func_arity(inner) == 1 {
                        let init = vals[0].clone();
                        let mut out = vec![init.clone()];
                        let mut cur = init.clone();
                        for _ in 0..100_000 {
                            let next = self.apply_func(inner, &[cur.clone()])?;
                            if next == cur || next == init {
                                break;
                            }
                            out.push(next.clone());
                            cur = next;
                        }
                        return Ok(K::Vector(ColVec::from_atoms(out)));
                    }
                    let K::Vector(cv) = &vals[0] else {
                        return Ok(vals[0].clone());
                    };
                    if cv.is_empty() {
                        return Ok(vals[0].clone());
                    }
                    let mut out = Vec::with_capacity(cv.len());
                    let mut acc = cv.get_atom(0);
                    out.push(acc.clone());
                    for i in 1..cv.len() {
                        acc = self.apply_func(inner, &[acc, cv.get_atom(i)])?;
                        out.push(acc.clone());
                    }
                    Ok(K::Vector(ColVec::from_atoms(out)))
                }
                2 => {
                    if self.func_arity(inner) == 1 {
                        let n = vals[0].as_i64()?.max(0) as usize;
                        let mut out = Vec::with_capacity(n + 1);
                        let mut cur = vals[1].clone();
                        out.push(cur.clone());
                        for _ in 0..n {
                            cur = self.apply_func(inner, &[cur])?;
                            out.push(cur.clone());
                        }
                        return Ok(K::Vector(ColVec::from_atoms(out)));
                    }
                    let mut acc = vals[0].clone();
                    let K::Vector(cv) = &vals[1] else {
                        return self.apply_func(inner, vals);
                    };
                    let mut out = Vec::with_capacity(cv.len());
                    for i in 0..cv.len() {
                        acc = self.apply_func(inner, &[acc, cv.get_atom(i)])?;
                        out.push(acc.clone());
                    }
                    Ok(K::Vector(ColVec::from_atoms(out)))
                }
                _ => Err(KError::rank("scan takes 1 or 2 arguments")),
            },

            AdverbKind::EachPrior => match vals.len() {
                1 => {
                    let K::Vector(cv) = &vals[0] else {
                        return Ok(vals[0].clone());
                    };
                    if cv.is_empty() {
                        return Ok(vals[0].clone());
                    }
                    let mut out = Vec::with_capacity(cv.len());
                    out.push(cv.get_atom(0));
                    for i in 1..cv.len() {
                        out.push(self.apply_func(inner, &[cv.get_atom(i), cv.get_atom(i - 1)])?);
                    }
                    Ok(K::Vector(ColVec::from_atoms(out)))
                }
                2 => {
                    let seed = &vals[0];
                    let K::Vector(cv) = &vals[1] else {
                        return self.apply_func(inner, &[vals[1].clone(), seed.clone()]);
                    };
                    let mut out = Vec::with_capacity(cv.len());
                    for i in 0..cv.len() {
                        let prev = if i == 0 { seed.clone() } else { cv.get_atom(i - 1) };
                        out.push(self.apply_func(inner, &[cv.get_atom(i), prev])?);
                    }
                    Ok(K::Vector(ColVec::from_atoms(out)))
                }
                _ => Err(KError::rank("each-prior takes 1 or 2 arguments")),
            },

            AdverbKind::EachRight => {
                if vals.len() != 2 {
                    return Err(KError::rank("/: requires 2 arguments"));
                }
                let lhs = &vals[0];
                let K::Vector(cv) = &vals[1] else {
                    return self.apply_func(inner, vals);
                };
                let mut out = Vec::with_capacity(cv.len());
                for i in 0..cv.len() {
                    out.push(self.apply_func(inner, &[lhs.clone(), cv.get_atom(i)])?);
                }
                Ok(K::Vector(ColVec::from_atoms(out)))
            }

            AdverbKind::EachLeft => {
                if vals.len() != 2 {
                    return Err(KError::rank("\\: requires 2 arguments"));
                }
                let K::Vector(cv) = &vals[0] else {
                    return self.apply_func(inner, vals);
                };
                let rhs = &vals[1];
                let mut out = Vec::with_capacity(cv.len());
                for i in 0..cv.len() {
                    out.push(self.apply_func(inner, &[cv.get_atom(i), rhs.clone()])?);
                }
                Ok(K::Vector(ColVec::from_atoms(out)))
            }
        }
    }

    /// `@[f; x; err]` (protected evaluation) and `@[d; i; f; [y]]` (functional amend).
    fn eval_trap_or_amend(&mut self, op: &str, args: &[Option<Expr>]) -> Result<K, StepErr> {
        let mut v = Vec::with_capacity(args.len());
        for a in args {
            v.push(match a {
                Some(e) => self.eval_step(e)?,
                None => K::Null,
            });
        }
        // If 3 arguments and `v[0]` is a `Func`, this is protected evaluation `@[f; x; handler]` or `.[f; args_list; handler]`!
        if v.len() == 3 && matches!(v[0], K::Func(_)) {
            let call_res = if op == "@" {
                self.apply_value(&v[0], &[Some(v[1].clone())])
            } else if let K::Vector(cv) = &v[1] {
                let unpacked: Vec<Option<K>> = (0..cv.len()).map(|i| Some(cv.get_atom(i))).collect();
                self.apply_value(&v[0], &unpacked)
            } else {
                self.apply_value(&v[0], &[Some(v[1].clone())])
            };
            return match call_res {
                Ok(ok) => Ok(ok),
                Err(kerr) => {
                    if let K::Func(handler) = &v[2] {
                        Ok(self.apply_func(handler, &[K::string(&kerr.signal)])?)
                    } else {
                        Ok(v[2].clone())
                    }
                }
            };
        }

        // Functional amend `@[d; i; f]` / `@[d; i; f; y]`!
        if v.len() >= 3 {
            let mut inplace_sym = None;
            let base = if let K::Sym(s) = &v[0] {
                inplace_sym = Some(*s);
                self.get_var(*s)?
            } else {
                v[0].clone()
            };
            let K::Func(f) = &v[2] else {
                return Err(KError::ty("functional amend third argument must be a function").into());
            };
            if op == "." {
                if let K::Vector(path) = &v[1] {
                    let path: Vec<K> = (0..path.len()).map(|i| path.get_atom(i)).collect();
                    let updated = self.amend_path(base, &path, f, v.get(3))?;
                    if let Some(s) = inplace_sym {
                        self.set_global(s, updated)?;
                        return Ok(K::Sym(s));
                    }
                    return Ok(updated);
                }
            }
            let idx_list = vec![Some(v[1].clone())];
            let cur_sub = ops::index_k(&base, &idx_list)?;
            let new_sub = if v.len() >= 4 {
                self.apply_func(f, &[cur_sub, v[3].clone()])?
            } else {
                self.apply_func(f, &[cur_sub])?
            };
            let updated = self.amend_in_place(base, &idx_list, None, &new_sub)?;
            if let Some(s) = inplace_sym {
                self.set_global(s, updated)?;
                return Ok(K::Sym(s));
            }
            return Ok(updated);
        }
        Err(KError::rank("invalid @ / . arguments").into())
    }

    /// Amend the element reached by following `path` into nested lists and dictionaries.
    fn amend_path(&mut self, target: K, path: &[K], f: &Func, y: Option<&K>) -> KResult<K> {
        let Some((head, rest)) = path.split_first() else {
            return match y {
                Some(y) => self.apply_func(f, &[target, y.clone()]),
                None => self.apply_func(f, &[target]),
            };
        };
        let idx = [Some(head.clone())];
        let sub = ops::index_k(&target, &idx)?;
        let new_sub = self.amend_path(sub, rest, f, y)?;
        self.amend_in_place(target, &idx, None, &new_sub)
    }

    fn amend_in_place(&mut self, target: K, indices: &[Option<K>], op: Option<SymId>, rhs: &K) -> KResult<K> {
        let Some(first) = indices.first() else {
            return Ok(rhs.clone());
        };
        let apply_op = |cur: &K, v: &K, eng: &mut Self| -> KResult<K> {
            if let Some(op_s) = op {
                eng.eval_binary_dispatch(&resolve(op_s), cur, v)
            } else {
                Ok(v.clone())
            }
        };
        match target {
            K::Vector(mut cv) => {
                match first {
                    None | Some(K::Null) => {
                        for i in 0..cv.len() {
                            let cur = cv.get_atom(i);
                            let nv = apply_op(&cur, rhs, self)?;
                            cv.set_at(i, nv)?;
                        }
                    }
                    Some(idx_k) if idx_k.is_atom() => {
                        let idx = idx_k.as_i64()? as usize;
                        let cur = cv.get_atom(idx);
                        let nv = apply_op(&cur, rhs, self)?;
                        cv.set_at(idx, nv)?;
                    }
                    Some(K::Vector(icv)) => {
                        let is = icv.as_i64_vec().ok_or_else(|| KError::ty("index must be integer"))?;
                        for (pos, &idx) in is.iter().enumerate() {
                            let item_v = if let K::Vector(rv) = rhs { rv.get_atom(pos) } else { rhs.clone() };
                            let cur = cv.get_atom(idx as usize);
                            let nv = apply_op(&cur, &item_v, self)?;
                            cv.set_at(idx as usize, nv)?;
                        }
                    }
                    _ => return Err(KError::ty("invalid vector index in assignment")),
                }
                Ok(K::Vector(cv))
            }
            K::Dict(d) => {
                let Some(k) = first.clone() else { return Ok(K::Dict(d)) };
                let addon = ops::eval_binary_prim("!", &k, rhs)?;
                if let Some(op_s) = op {
                    let cur = ops::dict_lookup(&d, &k)?;
                    let nv = self.eval_binary_dispatch(&resolve(op_s), &cur, rhs)?;
                    let addon2 = ops::eval_binary_prim("!", &k, &nv)?;
                    ops::concat_k(&K::Dict(d), &addon2)
                } else {
                    ops::concat_k(&K::Dict(d), &addon)
                }
            }
            K::Table(t) => {
                let mut f = (*t).clone();
                if let Some(K::Sym(col)) = first {
                    let nv = if let Some(op_s) = op {
                        let cur = f.col(*col).cloned().map(K::Vector).unwrap_or(K::Null);
                        self.eval_binary_dispatch(&resolve(op_s), &cur, rhs)?
                    } else {
                        rhs.clone()
                    };
                    let new_cv = match nv {
                        K::Vector(cv) if cv.len() == f.nrows() => cv,
                        atom => ColVec::repeat(atom, f.nrows()),
                    };
                    if let Some(ci) = f.col_idx(*col) {
                        f.vals[ci] = new_cv;
                    } else {
                        f.cols.push(*col);
                        f.vals.push(new_cv);
                    }
                    return Ok(K::Table(Arc::new(f)));
                }
                Err(KError::nyi("row-indexed table assignment (use update ... from `t)"))
            }
            _ => Err(KError::ty("cannot index-assign target")),
        }
    }

    /// Functional select and exec `?[t; c; b; a]` / `?[t; c; b; a; n]`. `b` is `0b` (no grouping), `1b` (distinct), a
    /// dictionary of groupings, `()` (exec) or one tree (exec by it); `a` is `()` (all columns), a dictionary of columns
    /// or one tree (exec of it).
    fn eval_functional_select(&mut self, args: &[Option<Expr>]) -> KResult<K> {
        let [c_val, b_val, a_val] = self.functional_args(args)?;
        let limit = args.get(4).and_then(|a| a.as_ref()).map(|e| self.eval_expr(e)).transpose()?;
        let one_column = !matches!(a_val, K::Dict(_) | K::Null) && !matches!(&a_val, K::Vector(cv) if cv.is_empty());
        let (distinct, by, exec) = match &b_val {
            K::Bool(b) => (*b, None, one_column),
            K::Dict(_) => (false, Some(tree::col_specs(&b_val)?), one_column),
            K::Null => (false, None, true),
            K::Vector(cv) if cv.is_empty() => (false, None, true),
            _ => (false, Some(tree::col_specs(&b_val)?), true),
        };
        let q = crate::ast::QSql {
            kind: if exec { crate::ast::QSqlKind::Exec } else { crate::ast::QSqlKind::Select },
            distinct,
            limit: limit.map(|n| Box::new(Expr::Lit(n))),
            cols: tree::col_specs(&a_val)?,
            by,
            from: Box::new(args[0].clone().unwrap_or(Expr::Lit(K::Null))),
            where_clauses: tree::where_clauses(&c_val)?,
        };
        self.eval_qsql(&q)
    }

    /// Functional update and delete `![t; c; b; a]`: `a` is a dictionary of new columns, the names of columns to delete,
    /// or no names (delete the rows `c` keeps).
    fn eval_functional_update(&mut self, args: &[Option<Expr>]) -> KResult<K> {
        use crate::ast::QSqlKind;
        let [c_val, b_val, a_val] = self.functional_args(args)?;
        let (kind, cols) = match &a_val {
            K::Vector(cv) if cv.is_empty() => (QSqlKind::DeleteRows, vec![]),
            K::Sym(_) | K::Vector(ColVec::Sym(..)) => {
                let names = match &a_val {
                    K::Sym(s) => vec![*s],
                    K::Vector(ColVec::Sym(_, xs)) => (**xs).clone(),
                    _ => vec![],
                };
                let spec = |n| crate::ast::ColSpec { name: Some(n), expr: Expr::Var(n) };
                (QSqlKind::DeleteCols, names.into_iter().map(spec).collect())
            }
            _ => (QSqlKind::Update, tree::col_specs(&a_val)?),
        };
        let q = crate::ast::QSql {
            kind,
            distinct: false,
            limit: None,
            cols,
            by: if let K::Dict(_) = &b_val { Some(tree::col_specs(&b_val)?) } else { None },
            from: Box::new(args[0].clone().unwrap_or(Expr::Lit(K::Null))),
            where_clauses: tree::where_clauses(&c_val)?,
        };
        self.eval_qsql(&q)
    }

    /// The values of the `c`, `b` and `a` arguments of a functional query.
    fn functional_args(&mut self, args: &[Option<Expr>]) -> KResult<[K; 3]> {
        let mut out = [K::Null, K::Bool(false), K::Null];
        for (slot, arg) in out.iter_mut().zip(args.iter().skip(1)) {
            if let Some(e) = arg {
                *slot = self.eval_expr(e)?;
            }
        }
        Ok(out)
    }

    // ----- Parse trees --------------------------------------------------------

    /// `parse "..."`: the parse tree of the source; several statements are joined by `;`.
    fn eval_parse(&mut self, x: &K) -> KResult<K> {
        let source = match x {
            K::Char(_) | K::Vector(ColVec::Char(..)) => x.as_str().unwrap_or_default(),
            _ => return Err(KError::ty("parse expects a string")),
        };
        let ns = self.session.current_ns.clone();
        let mut trees = parse_script(&source)?.iter().map(|e| tree::to_tree(e, &ns)).collect::<KResult<Vec<_>>>()?;
        Ok(match trees.len() {
            0 => K::Null,
            1 => trees.remove(0),
            _ => {
                trees.insert(0, K::Func(Func::Prim(intern(";"))));
                K::Vector(ColVec::General(Attr::None, Arc::new(trees)))
            }
        })
    }

    /// `eval tree`
    fn eval_tree(&mut self, x: &K) -> KResult<K> {
        if let K::Vector(ColVec::General(_, items)) = x {
            if matches!(items.first(), Some(K::Func(Func::Prim(s))) if resolve(*s) == ";") {
                let mut last = K::Null;
                for statement in &items[1..] {
                    last = self.eval_tree(statement)?;
                }
                return Ok(last);
            }
        }
        let e = tree::to_expr(x)?;
        self.eval_expr(&e)
    }

    // ----- Persistence (`set` / `get`) & IPC handles (`hopen` / `hclose`) ---

    fn eval_set(&mut self, lhs: &K, rhs: &K) -> KResult<K> {
        let K::Sym(s) = lhs else {
            return Err(KError::ty("set LHS must be a symbol (`var or `:path)"));
        };
        let target = resolve(*s);
        if let Some(rel_path) = target.strip_prefix(':') {
            storage::save_path(self.ws, rel_path, rhs)?;
            Ok(lhs.clone())
        } else {
            self.set_global(*s, rhs.clone())?;
            Ok(lhs.clone())
        }
    }

    fn eval_get(&mut self, x: &K) -> KResult<K> {
        let K::Sym(s) = x else {
            return Err(KError::ty("get expects file symbol (`:path) or variable symbol"));
        };
        let target = resolve(*s);
        if let Some(rel_path) = target.strip_prefix(':') {
            storage::load_path(self.ws, rel_path)
        } else {
            self.get_var(*s)
        }
    }

    fn eval_hopen(&mut self, x: &K) -> KResult<K> {
        let spec = x.as_str().ok_or_else(|| KError::ty("hopen expects `:host:port symbol or string"))?;
        let addr = spec.trim_start_matches(':');
        let client = crate::ipc::IpcClient::connect_addr(addr, &self.session.login, "")?;
        let h = self.session.next_handle;
        self.session.next_handle += 1;
        self.session.handles.insert(h, client);
        Ok(K::Int(h))
    }

    fn eval_hclose(&mut self, x: &K) -> KResult<K> {
        let h = x.as_i64()? as i32;
        self.session.handles.remove(&h.abs());
        Ok(K::Null)
    }

    // ----- System Commands (`\a`, `\b`, `\v`, `\f`, `\d`, `\l`, `\t`, `\ts`, `\w`)

    fn eval_sys_cmd(&mut self, cmd: &str, arg: &str) -> KResult<K> {
        // `\t:100 expr` arrives as command `t:100`
        if let Some((c @ ("t" | "ts"), n)) = cmd.split_once(':') {
            return self.eval_sys_cmd(c, &format!(":{n} {arg}"));
        }
        match cmd {
            "a" => self.eval_unary_dispatch(intern("tables"), &K::Null),
            "b" => self.eval_unary_dispatch(intern("views"), &K::Null),
            "v" => {
                let mut syms: Vec<SymId> = self
                    .ws
                    .vars
                    .iter()
                    .filter_map(|(&k, v)| {
                        if !matches!(v, K::Func(_)) && !resolve(k).starts_with('.') {
                            Some(k)
                        } else {
                            None
                        }
                    })
                    .collect();
                syms.sort();
                Ok(K::Vector(ColVec::Sym(Attr::Sorted, Arc::new(syms))))
            }
            "f" => {
                let mut syms: Vec<SymId> = self
                    .ws
                    .vars
                    .iter()
                    .filter_map(|(&k, v)| if matches!(v, K::Func(Func::Lambda(_))) { Some(k) } else { None })
                    .collect();
                syms.sort();
                Ok(K::Vector(ColVec::Sym(Attr::Sorted, Arc::new(syms))))
            }
            "d" => {
                if arg.is_empty() {
                    let ns = if self.session.current_ns.is_empty() { "." } else { &self.session.current_ns };
                    Ok(K::Sym(intern(ns)))
                } else {
                    self.session.current_ns = if arg == "." {
                        String::new()
                    } else if arg.starts_with('.') {
                        arg.to_string()
                    } else {
                        format!(".{arg}")
                    };
                    Ok(K::Null)
                }
            }
            "t" => {
                if arg.is_empty() {
                    return Ok(K::Int(0));
                }
                // `\t:100 expr` runs 100 times!
                let (iters, code) = if let Some(rest) = arg.strip_prefix(':') {
                    match rest.find(char::is_whitespace) {
                        Some(p) => (rest[..p].parse::<usize>().unwrap_or(1), rest[p..].trim()),
                        None => (1, rest),
                    }
                } else {
                    (1, arg)
                };
                let t0 = Instant::now();
                for _ in 0..iters {
                    self.eval_str(code)?;
                }
                Ok(K::Long(t0.elapsed().as_millis() as i64))
            }
            "ts" => {
                let (iters, code) = if let Some(rest) = arg.strip_prefix(':') {
                    match rest.find(char::is_whitespace) {
                        Some(p) => (rest[..p].parse::<usize>().unwrap_or(1), rest[p..].trim()),
                        None => (1, rest),
                    }
                } else {
                    (1, arg)
                };
                let t0 = Instant::now();
                let mut last_bytes = 0usize;
                for _ in 0..iters {
                    let v = self.eval_str(code)?;
                    last_bytes = v.approx_bytes();
                }
                let ms = t0.elapsed().as_millis() as i64;
                Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(vec![ms, last_bytes as i64]))))
            }
            "w" => {
                let used = self.ws.approx_memory_bytes() as i64;
                let syms = sym_count() as i64;
                Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(vec![used, used * 2, used, 0, 0, syms]))))
            }
            "l" => {
                if arg.is_empty() {
                    return Err(KError::domain("\\l requires a file or directory path"));
                }
                let p = std::path::Path::new(arg);
                if p.is_dir() {
                    storage::mount_hdb_dir(self.ws, p)?;
                    Ok(K::Null)
                } else {
                    let src = std::fs::read_to_string(p)?;
                    self.eval_str(&src)
                }
            }
            "\\" => Ok(K::Null),
            _ => Err(KError::nyi(format!("unknown system command \\{cmd}"))),
        }
    }
}

pub(crate) fn is_ephemeral_var(name: SymId) -> bool {
    let s = resolve(name);
    s.starts_with(".kix.tmp.") || s.starts_with(".kix.ord..kix.tmp.")
}

fn is_ambivalent_prim(func: &Func) -> bool {
    match func {
        Func::Prim(s) => matches!(
            resolve(*s).as_str(),
            "+" | "-" | "*" | "&" | "|" | "<" | ">" | "=" | "~" | "," | "!" | "#" | "_" | "$" | "?" | "@" | "."
        ),
        Func::Adverb { kind, .. } => matches!(
            kind,
            AdverbKind::Each | AdverbKind::Over | AdverbKind::Scan | AdverbKind::EachPrior
        ),
        _ => false,
    }
}
