//! `kixdb` — a clean-room clone of KX's kdb+ columnstore database and the `q` query language.
//!
//! ```
//! use kixdb::Server;
//! let server = Server::new();
//! let mut conn = server.connect();
//! let v = conn.eval("sum 1 + til 10").unwrap();
//! assert_eq!(v.to_string(), "55");
//! ```
pub mod ast;
pub mod delta;
pub mod engine;
pub mod error;
pub mod index;
pub mod inspect;
pub mod ipc;
pub mod lexer;
pub mod parser;
pub mod persist;
pub mod storage;
pub mod sym;
pub mod tools;
pub mod types;

use engine::{Engine, SessionState, Workspace};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

pub use engine::{flip_to_resultset, k_to_resultset, BatchResult, DistMode, Event, ResultSet};
pub use error::{KError, KResult};
pub use inspect::StoreStats;
pub use persist::StorageOptions;
pub use types::{Attr, ColVec, Dict, Flip, Func, KeyedTable, K};

/// Hook used by `crates/parallel/control` (`kix-cluster`) to intercept queries on a gateway `Server` and
/// route/scatter/gather them across distributed shard nodes while presenting a standard kdb+ IPC server to clients.
pub trait Interceptor: Send {
    fn eval_query(&mut self, code: &str) -> Option<KResult<K>>;
    /// A functional IPC message such as ``(`upsert; `trade; rows)``.
    fn eval_message(&mut self, msg: &K) -> Option<KResult<K>> {
        let _ = msg;
        None
    }
    fn explain_query(&mut self, code: &str) -> Option<KResult<Vec<String>>> {
        let _ = code;
        None
    }
}

pub type InterceptorFactory = dyn Fn() -> Box<dyn Interceptor> + Send + Sync;

pub(crate) struct Shared {
    pub(crate) ws: RwLock<Workspace>,
    next_session_id: AtomicU64,
    crashed: AtomicBool,
    interceptor: Mutex<Option<Arc<InterceptorFactory>>>,
}

impl Drop for Shared {
    fn drop(&mut self) {
        if !self.crashed.load(Ordering::SeqCst) {
            if let Ok(ws) = self.ws.write() {
                if let Ok(mut p_guard) = ws.persist.lock() {
                    if let Some(p) = p_guard.as_mut() {
                        let _ = p.checkpoint(&ws);
                    }
                }
            }
        }
    }
}

/// Shared workspace handle (cheap to `Clone`, thread-safe).
#[derive(Clone)]
pub struct Server {
    pub(crate) shared: Arc<Shared>,
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

impl Server {
    /// Create a transient in-memory workspace (nothing written to disk).
    pub fn new() -> Self {
        Self {
            shared: Arc::new(Shared {
                ws: RwLock::new(Workspace::new()),
                next_session_id: AtomicU64::new(1),
                crashed: AtomicBool::new(false),
                interceptor: Mutex::new(None),
            }),
        }
    }

    /// Open (or create) a durable workspace in `dir` backed by a checksummed WAL (`kix.wal`), atomic snapshots (`kix.snap`),
    /// and any splayed or partitioned HDB tables inside `dir`.
    pub fn open(dir: impl AsRef<Path>) -> KResult<Self> {
        Self::open_with(dir, StorageOptions::default())
    }

    pub fn open_with(dir: impl AsRef<Path>, opts: StorageOptions) -> KResult<Self> {
        let (p, ws) = persist::Persist::open(dir.as_ref(), opts)?;
        *ws.persist.lock().unwrap_or_else(|e| e.into_inner()) = Some(p);
        Ok(Self {
            shared: Arc::new(Shared {
                ws: RwLock::new(ws),
                next_session_id: AtomicU64::new(1),
                crashed: AtomicBool::new(false),
                interceptor: Mutex::new(None),
            }),
        })
    }

    pub fn connect(&self) -> Connection {
        let mut c = self.connect_internal();
        c.interceptor = self.new_interceptor();
        c
    }

    /// Connect without installing the cluster interceptor (used by the cluster coordinator's own mirror workspace).
    pub fn connect_internal(&self) -> Connection {
        let id = self.shared.next_session_id.fetch_add(1, Ordering::SeqCst);
        Connection {
            server: self.clone(),
            session: SessionState::new(id, "admin"),
            interceptor: None,
        }
    }

    pub fn connect_as_trusted(&self, login: &str) -> Connection {
        let id = self.shared.next_session_id.fetch_add(1, Ordering::SeqCst);
        Connection {
            server: self.clone(),
            session: SessionState::new(id, login),
            interceptor: self.new_interceptor(),
        }
    }

    pub fn set_interceptor(&self, f: Option<Arc<InterceptorFactory>>) {
        *self.shared.interceptor.lock().unwrap_or_else(|e| e.into_inner()) = f;
    }

    fn new_interceptor(&self) -> Option<Box<dyn Interceptor>> {
        self.shared.interceptor.lock().unwrap_or_else(|e| e.into_inner()).as_ref().map(|f| f())
    }

    // ----- Replication, Snapshot & Lifecycle Hooks --------------------------

    pub fn set_replicator(&self, r: Option<Arc<dyn persist::Replicator>>) {
        let g = self.shared.ws.read().unwrap_or_else(|e| e.into_inner());
        *g.replicator.lock().unwrap_or_else(|e| e.into_inner()) = r;
    }

    pub fn applied_ts(&self) -> u64 {
        self.shared.ws.read().unwrap_or_else(|e| e.into_inner()).clock
    }

    pub fn apply_replicated(&self, rec: &persist::WalRecord) -> KResult<bool> {
        let mut g = self.shared.ws.write().unwrap_or_else(|e| e.into_inner());
        if rec.ts <= g.clock {
            return Ok(false);
        }
        if let Some(p) = g.persist.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
            p.append_record(rec, false)?;
        }
        persist::apply_record(&mut g, rec)?;
        Ok(true)
    }

    /// Log the next change to each variable the engine now holds as its whole new value: for a new member of a group,
    /// whose readers may not have the values it read from its own storage.
    pub fn log_values_whole(&self) {
        let mut g = self.shared.ws.write().unwrap_or_else(|e| e.into_inner());
        let names: Vec<_> = g.vars.keys().copied().collect();
        g.unlogged.extend(names);
    }

    /// Install a snapshot that holds later changes than the engine does: a replica that lost its latest changes (it does
    /// not force each to disk) starts again from there, as records that change a value need the value they were made from.
    pub fn restore_state(&self, data: &[u8]) -> KResult<()> {
        let snap: persist::SnapshotData = postcard::from_bytes(data).map_err(|e| KError::io(format!("bad snapshot: {e}")))?;
        if snap.ts > self.applied_ts() {
            self.install_state(data)?;
        }
        Ok(())
    }

    pub fn export_state_at(&self, ts: u64) -> KResult<Vec<u8>> {
        let g = self.shared.ws.read().unwrap_or_else(|e| e.into_inner());
        let snap = persist::export_snapshot(&g, ts);
        postcard::to_allocvec(&snap).map_err(|e| KError::io(format!("snapshot export failed: {e}")))
    }

    pub fn export_state(&self) -> KResult<Vec<u8>> {
        self.export_state_at(self.applied_ts())
    }

    pub fn install_state(&self, data: &[u8]) -> KResult<()> {
        let snap: persist::SnapshotData = postcard::from_bytes(data).map_err(|e| KError::io(format!("bad snapshot: {e}")))?;
        let mut g = self.shared.ws.write().unwrap_or_else(|e| e.into_inner());
        persist::install_snapshot_into(&mut g, snap);
        let p_opt = g.persist.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(mut p) = p_opt {
            let _ = p.checkpoint(&g);
            *g.persist.lock().unwrap_or_else(|e| e.into_inner()) = Some(p);
        }
        Ok(())
    }

    /// Look at the workspace (variables, views, mounted tables) under the read lock.
    pub fn read_workspace<R>(&self, f: impl FnOnce(&engine::Workspace) -> R) -> R {
        f(&self.shared.ws.read().unwrap_or_else(|e| e.into_inner()))
    }

    pub fn checkpoint(&self) -> KResult<()> {
        let g = self.shared.ws.write().unwrap_or_else(|e| e.into_inner());
        let mut p_guard = g.persist.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(p) = p_guard.as_mut() {
            p.checkpoint(&g)?;
        }
        Ok(())
    }

    pub fn flush_log(&self) -> KResult<()> {
        let g = self.shared.ws.read().unwrap_or_else(|e| e.into_inner());
        let mut p_guard = g.persist.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(p) = p_guard.as_mut() {
            p.flush()?;
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn simulate_crash(self) {
        self.shared.crashed.store(true, Ordering::SeqCst);
    }

    pub fn storage_stats(&self) -> StoreStats {
        let g = self.shared.ws.read().unwrap_or_else(|e| e.into_inner());
        let table_count = g.vars.values().filter(|v| matches!(v, K::Table(_) | K::KeyedTable(_))).count() + g.partitioned.len();
        let (wal_records, wal_bytes, snap_bytes, log_forces) = match g.persist.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
            Some(p) => (p.wal_records, p.wal_bytes, p.snap_bytes, p.log_forces),
            None => (0, 0, 0, 0),
        };
        StoreStats {
            memory_bytes: g.approx_memory_bytes(),
            sym_count: sym::sym_count(),
            table_count,
            view_count: g.views.len(),
            wal_records,
            wal_bytes,
            snap_bytes,
            log_forces,
            data_dir: g.data_dir.as_ref().map(|d| d.display().to_string()),
        }
    }
}

/// A session against a [`Server`].
pub struct Connection {
    pub(crate) server: Server,
    pub(crate) session: SessionState,
    pub(crate) interceptor: Option<Box<dyn Interceptor>>,
}

impl Connection {
    /// Evaluate `q` source code and return the resulting `K` value directly.
    pub fn eval(&mut self, q_code: &str) -> KResult<K> {
        if let Some(int) = self.interceptor.as_mut() {
            if let Some(res) = int.eval_query(q_code) {
                return res;
            }
        }
        let mut ws = self.server.shared.ws.write().unwrap_or_else(|e| e.into_inner());
        let mut eng = Engine::new(&mut ws, &mut self.session);
        eng.eval_str(q_code)
    }

    /// Evaluate an incoming IPC `K` message (string query OR functional list `("fn"; arg1; arg2)`).
    pub fn eval_ipc_message(&mut self, msg: &K) -> KResult<K> {
        match msg {
            K::Char(c) => self.eval(&(*c as char).to_string()),
            K::Vector(ColVec::Char(_, bs)) => {
                let code = String::from_utf8_lossy(bs);
                self.eval(&code)
            }
            K::Vector(ColVec::General(_, items)) if !items.is_empty() => {
                if let Some(res) = self.interceptor.as_mut().and_then(|i| i.eval_message(msg)) {
                    return res;
                }
                let mut ws = self.server.shared.ws.write().unwrap_or_else(|e| e.into_inner());
                let mut eng = Engine::new(&mut ws, &mut self.session);
                let head = &items[0];
                let func_k = match head {
                    K::Func(_) => head.clone(),
                    K::Sym(s) => eng.get_var(*s)?,
                    K::Vector(ColVec::Char(_, bs)) => eng.eval_str(&String::from_utf8_lossy(bs))?,
                    other => other.clone(),
                };
                let args: Vec<Option<K>> = items[1..].iter().cloned().map(Some).collect();
                eng.apply_value(&func_k, &args)
            }
            other => Ok(other.clone()),
        }
    }

    /// Execute a `q` script and collect displayable [`Event`]s (`Rows` for tables, `Value` for scalars/vectors/dicts, `Message`, `Error`).
    pub fn execute(&mut self, q_code: &str) -> BatchResult {
        if let Some(int) = self.interceptor.as_mut() {
            if let Some(res) = int.eval_query(q_code) {
                return match res {
                    Ok(K::Null) => BatchResult { events: vec![] },
                    Ok(val) => {
                        if let Some(rs) = k_to_resultset(&val) {
                            BatchResult { events: vec![Event::Rows(rs)] }
                        } else {
                            BatchResult { events: vec![Event::Value(val)] }
                        }
                    }
                    Err(e) => BatchResult { events: vec![Event::Error(e)] },
                };
            }
        }
        let mut ws = self.server.shared.ws.write().unwrap_or_else(|e| e.into_inner());
        let mut eng = Engine::new(&mut ws, &mut self.session);
        BatchResult { events: eng.run_batch(q_code) }
    }

    /// Run a query returning a tabular `ResultSet` rows vector, or an error.
    pub fn query(&mut self, q_code: &str) -> KResult<Vec<Vec<K>>> {
        let val = self.eval(q_code)?;
        if let Some(rs) = k_to_resultset(&val) {
            Ok(rs.rows)
        } else {
            Ok(vec![vec![val]])
        }
    }

    /// Evaluate an already parsed expression (the cluster sends shards expressions, not source text).
    pub fn eval_expr(&mut self, expr: &ast::Expr) -> KResult<K> {
        let mut ws = self.server.shared.ws.write().unwrap_or_else(|e| e.into_inner());
        Engine::new(&mut ws, &mut self.session).eval_expr(expr)
    }

    /// Set (`Some`) or drop (`None`) fully qualified globals atomically: one log record.
    pub fn set_vars(&mut self, changes: Vec<(sym::SymId, Option<K>)>) -> KResult<()> {
        let mut ws = self.server.shared.ws.write().unwrap_or_else(|e| e.into_inner());
        Engine::new(&mut ws, &mut self.session).set_globals(changes)
    }

    /// Directly store or replace a table/value in the workspace without serializing through `q` text (fast bulk load for shards).
    pub fn set_var_direct(&mut self, name: &str, val: K) -> KResult<()> {
        let mut ws = self.server.shared.ws.write().unwrap_or_else(|e| e.into_inner());
        let mut eng = Engine::new(&mut ws, &mut self.session);
        eng.set_global(sym::intern(name), val)
    }

    pub fn get_var_direct(&mut self, name: &str) -> KResult<K> {
        let mut ws = self.server.shared.ws.write().unwrap_or_else(|e| e.into_inner());
        let mut eng = Engine::new(&mut ws, &mut self.session);
        eng.get_var(sym::intern(name))
    }

    /// Explain how `q_code` will be parsed and executed (including partition pruning or distributed cluster routing).
    pub fn explain(&mut self, q_code: &str) -> KResult<Vec<String>> {
        if let Some(int) = self.interceptor.as_mut() {
            if let Some(plan) = int.explain_query(q_code) {
                return plan;
            }
        }
        let stmts = parser::parse_script(q_code)?;
        let ws = self.server.shared.ws.read().unwrap_or_else(|e| e.into_inner());
        let mut lines = Vec::new();
        for (idx, s) in stmts.iter().enumerate() {
            lines.push(format!("Statement {}: {}", idx + 1, describe_expr_plan(s, &ws)));
        }
        Ok(lines)
    }

    pub fn current_namespace(&self) -> String {
        if self.session.current_ns.is_empty() {
            ".".into()
        } else {
            self.session.current_ns.clone()
        }
    }

    pub fn login(&self) -> String {
        self.session.login.clone()
    }
}

pub(crate) fn describe_expr_plan(e: &ast::Expr, ws: &Workspace) -> String {
    match e {
        ast::Expr::Query(q) => {
            let kind = match q.kind {
                ast::QSqlKind::Select => "qSQL SELECT",
                ast::QSqlKind::Exec => "qSQL EXEC",
                ast::QSqlKind::Update => "qSQL UPDATE",
                ast::QSqlKind::DeleteRows => "qSQL DELETE ROWS",
                ast::QSqlKind::DeleteCols => "qSQL DELETE COLS",
            };
            let src_info = match &*q.from {
                ast::Expr::Var(s) | ast::Expr::Lit(K::Sym(s)) => {
                    let n = sym::resolve(*s);
                    if let Some(pt) = ws.partitioned.get(s) {
                        format!(
                            "partitioned HDB table `{n}` ({} slices on `{}`)",
                            pt.slices.len(),
                            sym::resolve(pt.part_col)
                        )
                    } else if let Some(K::Table(t)) = ws.vars.get(s) {
                        let a0 = t.vals.first().and_then(|v| v.attr().char()).map(|c| format!(" [`{c}#]")).unwrap_or_default();
                        format!("columnar table `{n}` ({} rows, {} cols{a0})", t.nrows(), t.ncols())
                    } else if let Some(K::KeyedTable(kt)) = ws.vars.get(s) {
                        format!("keyed table `{n}` ({} rows, {} key cols)", kt.nrows(), kt.keys.ncols())
                    } else {
                        format!("`{n}`")
                    }
                }
                _ => "derived table expression".into(),
            };
            let wh = if q.where_clauses.is_empty() {
                String::new()
            } else {
                format!(" -> sequential vector filter ({} predicate(s))", q.where_clauses.len())
            };
            let grp = match &q.by {
                Some(bs) => format!(" -> hash group by {} key(s)", bs.len()),
                None => String::new(),
            };
            format!("{kind} on {src_info}{wh}{grp}")
        }
        other => format!("Vector/AST evaluation: {other:?}"),
    }
}

impl tools::QSession for Connection {
    fn run_q(&mut self, code: &str) -> KResult<()> {
        self.eval(code).map(|_| ())
    }
    fn describe_table(&mut self, name: &str) -> Option<inspect::TableInfo> {
        self.table_info(name)
    }
}

impl tools::QSession for ipc::IpcClient {
    fn run_q(&mut self, code: &str) -> KResult<()> {
        self.query_str(code).map(|_| ())
    }
    fn describe_table(&mut self, name: &str) -> Option<inspect::TableInfo> {
        inspect::remote_table_info(self, name)
    }
}
