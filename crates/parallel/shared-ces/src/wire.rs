//! Messages carried over Arrow Flight tickets and actions (`postcard` encoded), and errors carried in `tonic::Status`.
//!
//! A query is a `DoGet` whose ticket is a [`Fragment`]; its result streams back as Arrow batches. Everything else is an
//! action.
use kixdb::ast::Expr;
use kixdb::ipc::{decode_k_payload, encode_k_payload};
use kixdb::{KError, K};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tonic::{Code, Status};

pub const ACTION_LOAD: &str = "load";
pub const ACTION_MUTATE: &str = "mutate";
pub const ACTION_SHUFFLE: &str = "shuffle";
pub const ACTION_STATUS: &str = "status";
pub const ACTION_METRICS: &str = "metrics";
pub const ACTION_MEMBERS: &str = "members";
pub const ACTION_RAFT: &str = "raft";
pub const ACTION_TXN: &str = "txn";
/// Have a shard refuse, from now on, any control node elected before the one that asks.
pub const ACTION_FENCE: &str = "fence";
/// A statement that a control node passes to the master of its group.
pub const ACTION_FORWARD: &str = "forward";

/// A control node of a group asks the master to let it read, or ends that.
pub const ACTION_PERMIT: &str = "permit";
/// How long a permit to read lasts unless it is renewed.
pub const PERMIT_LIMIT: Duration = Duration::from_secs(5);

/// The master's leave to read: while it lasts, no statement changes anything.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Permit {
    /// The term the master that gave it was elected for.
    pub term: u64,
    pub id: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum PermitReq {
    Take,
    Renew(Permit),
    Release(Permit),
}

/// The permit asked about and, for one taken, how far the master's mirror had come: the reader's must be no older.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct PermitReply {
    pub permit: Permit,
    pub position: u64,
}

/// The request header in which the master of a group of control nodes gives the term it was elected for.
pub const TERM_HEADER: &str = "kix-term";
/// The shard variable that holds the latest term the shard was fenced with.
pub const TERM_VAR: &str = ".kix.term";

/// A statement that a control node that is not the master passes to the one that is, with its session's namespace.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForwardReq {
    pub namespace: String,
    pub what: Forwarded,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Forwarded {
    Eval(String),
    /// A functional IPC message, IPC-encoded.
    Message(Vec<u8>),
    Explain(String),
}

/// What the master answers: the namespace the session is then in, and the value (IPC-encoded) or the signal.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForwardReply {
    pub namespace: String,
    pub answer: Result<Vec<u8>, (String, String)>,
}

/// Message prefix of the status with which a Raft follower redirects to the current leader (followed by its address).
pub const NOT_LEADER_PREFIX: &str = "NOTLEADER|";
/// Message prefix of the status with which a draining node refuses work.
pub const SHUTTING_DOWN_PREFIX: &str = "SHUTTING_DOWN|";
const ERROR_PREFIX: &str = "KIX|";

/// A column the control node adds to a table slice on its way through a join: the row's global ordinal.
pub const ORD_COL: &str = "kix__ord";

/// The shard variable of shard 0 that holds the cluster's layout, once the number of shards has changed.
pub const LAYOUT_VAR: &str = ".kix.layout";
/// The shard variable that marks a shard that is being added: what it holds may be thrown away.
pub const JOINING_VAR: &str = ".kix.joining";

/// Where a shard that stays holds the copies of rows that are moving to it, until they are merged into `table`.
pub fn moving_var(table: &str) -> String {
    format!(".kix.move.{table}")
}

/// The shard variable that holds the global row ordinals of a sharded table, parallel to its rows.
pub fn ord_var(table: &str) -> String {
    format!(".kix.ord.{table}")
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Work {
    /// Evaluate `q` source.
    Q(String),
    /// Evaluate a parsed expression (free variables already replaced by their values).
    Eval(Box<Expr>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Fragment {
    pub work: Work,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LoadMode {
    /// Make the table exactly these rows.
    Replace,
    /// Append the rows to an unkeyed table.
    Append,
    /// Upsert the rows into a keyed table (its key columns come first).
    Upsert,
    /// Fail with `'insert` if a row's key is already in the keyed table; change nothing.
    CheckInsert,
    /// Remove the table.
    Drop,
}

/// Change a table slice on a shard.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoadReq {
    pub table: String,
    pub mode: LoadMode,
    /// The IPC-encoded table.
    #[serde(with = "kixdb::bytes")]
    pub ipc_k: Vec<u8>,
    /// Global ordinals of those rows (sharded tables only).
    pub ords: Option<Vec<i64>>,
}

impl LoadReq {
    pub fn new(table: impl Into<String>, mode: LoadMode, rows: &K, ords: Option<Vec<i64>>) -> Self {
        Self { table: table.into(), mode, ipc_k: encode_k_payload(rows), ords }
    }

    pub fn rows(&self) -> Result<K, KError> {
        decode_k_payload(&self.ipc_k, 0).map(|(k, _)| k)
    }
}

/// One shard's part of a change that spans shards.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TxnOp {
    Load(LoadReq),
    Mutate(MutateReq),
    Move(MoveReq),
}

/// A shard's part in moving rows as the number of shards changes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MoveReq {
    /// Keep only the rows of `table` that the shard, `me`, owns under `slots`, by the values of `cols`.
    Keep { table: String, cols: Vec<String>, partitioned: bool, slots: Vec<u32>, me: usize },
    /// Merge the rows copied beside `table` into it, in the order of their ordinals.
    Merge { table: String },
}

impl TxnOp {
    /// The table it is about.
    pub fn table(&self) -> &str {
        match self {
            TxnOp::Load(req) => &req.table,
            TxnOp::Mutate(req) => &req.table,
            TxnOp::Move(MoveReq::Keep { table, .. } | MoveReq::Merge { table }) => table,
        }
    }

    /// Whether it changes anything (a duplicate check only looks).
    pub fn changes(&self) -> bool {
        !matches!(self, TxnOp::Load(LoadReq { mode: LoadMode::CheckInsert, .. }))
    }
}

/// A step of a two-phase commit (`txid` names the change).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TxnReq {
    /// Work the operations out and hold them. `primary` is the shard whose commit decides for all.
    Prepare { txid: u64, primary: u32, ops: Vec<TxnOp> },
    /// Make the change held; `primary` when this shard is the one that decides. `forget` names changes every shard has
    /// committed, which a primary need not remember (told along with the next commit, to spare a request).
    Commit { txid: u64, primary: bool, forget: Vec<u64> },
    /// Drop the change held, unless this shard, as primary, committed it.
    Abort { txid: u64 },
    /// Which change is held.
    Pending,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TxnReply {
    Done,
    /// The answers of the mutations among the operations (IPC-encoded), in order.
    Prepared(Vec<Vec<u8>>),
    Outcome { committed: bool },
    /// The change held, and its primary.
    Pending(Option<(u64, u32)>),
}

/// Run an `update` / `delete` over a table slice. `query` reads the slice (with its [`ORD_COL`] column when the table
/// has ordinals) and returns the new one.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MutateReq {
    pub table: String,
    pub query: Expr,
}

/// Shard-to-shard exchange: evaluate `scan` (a table), partition its rows by the hash of the `key_cols` (none: every
/// row to every shard), and append each part to `target_table` on its shard.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShuffleReq {
    pub scan: Expr,
    pub key_cols: Vec<String>,
    pub target_table: String,
    pub slots: Vec<u32>,
    /// Addresses of all shards, by shard index.
    pub peers: Vec<String>,
    pub me: usize,
}

/// Change (or just list, when both are `None`) the members of a replicated shard.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MemberReq {
    pub add: Option<(u64, String)>,
    pub remove: Option<u64>,
}

/// `(node id, address, voter)`.
pub type MemberInfo = (u64, String, bool);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardStatus {
    pub shard_id: u32,
    pub role: String,
    pub term: u64,
    pub leader_addr: Option<String>,
    pub applied_ts: u64,
    pub memory_bytes: usize,
    pub draining: bool,
}

/// The status that carries a `q` signal back to the caller.
pub fn status_of(e: &KError) -> Status {
    Status::new(Code::Aborted, format!("{ERROR_PREFIX}{}\u{1f}{}", e.signal, e.detail))
}

/// The error a status stands for: the shard's `q` signal, or `'cluster` for a transport failure.
pub fn error_of(s: &Status) -> KError {
    match s.message().strip_prefix(ERROR_PREFIX).and_then(|m| m.split_once('\u{1f}')) {
        Some((signal, detail)) => KError::new(signal, detail),
        None => KError::new("cluster", s.message()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signals_survive_the_trip_through_a_status() {
        let e = KError::new("length", "vectors differ");
        assert_eq!(error_of(&status_of(&e)), e);
        assert_eq!(error_of(&Status::unavailable("connection refused")).signal, "cluster");
    }

    #[test]
    fn load_requests_and_expressions_round_trip() {
        let rows = K::string("abc");
        let req = LoadReq::new("t", LoadMode::Append, &rows, Some(vec![1, 2]));
        let back: LoadReq = postcard::from_bytes(&postcard::to_allocvec(&req).unwrap()).unwrap();
        assert_eq!(back.rows().unwrap(), rows);
        let expr = kixdb::parser::parse_script("select max px by sym from t where sym in `a`b").unwrap().remove(0);
        let frag = Fragment { work: Work::Eval(Box::new(expr.clone())) };
        let back: Fragment = postcard::from_bytes(&postcard::to_allocvec(&frag).unwrap()).unwrap();
        assert!(matches!(back.work, Work::Eval(e) if *e == expr));
    }
}
