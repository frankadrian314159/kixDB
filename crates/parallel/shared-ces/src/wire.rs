//! Control-plane messages carried over Arrow Flight actions and tickets (`postcard` encoded).
use kixdb::KError;
use serde::{Deserialize, Serialize};

pub const ACTION_RUN: &str = "run";
pub const ACTION_LOAD: &str = "load";
pub const ACTION_SHUFFLE: &str = "shuffle";
pub const ACTION_STATUS: &str = "status";
pub const ACTION_METRICS: &str = "metrics";
pub const ACTION_RAFT: &str = "raft";
pub const ACTION_CLOSE_SESSION: &str = "close_session";

/// Error code prefix returned in `tonic::Status` when a Raft follower redirects to the current leader.
pub const NOT_LEADER_PREFIX: &str = "NOTLEADER|";
pub const SHUTTING_DOWN_PREFIX: &str = "SHUTTING_DOWN|";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Work {
    Q(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Fragment {
    pub session: u64,
    pub work: Work,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireError {
    pub signal: String,
    pub detail: String,
}

impl From<&KError> for WireError {
    fn from(e: &KError) -> Self {
        Self {
            signal: e.signal.clone(),
            detail: e.detail.clone(),
        }
    }
}

impl From<WireError> for KError {
    fn from(w: WireError) -> Self {
        KError::new(w.signal, w.detail)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunReply {
    /// IPC-encoded `K` result when small/non-table, or `None` when callers use `DoGet` for streaming batches.
    pub ipc_result: Option<Vec<u8>>,
    pub error: Option<WireError>,
}

/// Load a serialized `K` table slice directly into `table` on a shard (`replace = true` overwrites, `false` appends/upserts).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoadReq {
    pub table: String,
    pub replace: bool,
    pub ipc_k: Vec<u8>,
}

/// Shard-to-shard exchange: run `scan_q` locally, hash-partition rows by `key_col` (`None` = broadcast all rows to all shards!),
/// and send each partition directly to its destination shard via `ACTION_LOAD` into `target_table`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShuffleReq {
    pub scan_q: String,
    pub key_col: Option<String>,
    pub target_table: String,
    pub slots: Vec<u32>,
    pub peers: Vec<String>,
    pub me: usize,
}

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
