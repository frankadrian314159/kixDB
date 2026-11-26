//! Types shared across control and shard nodes in the distributed `kixdb` cluster:
//! - [`config`]: static cluster topology (`cluster.json`)
//! - [`route`]: which of 1,024 slots a row is in (by the hash of its key, or by its partition value) and which shard
//!   owns each
//! - [`arrowconv`]: lossless columnar `Flip` <-> Apache Arrow `RecordBatch` conversion
//! - [`wire`]: Flight action and ticket messages encoded with `postcard`
//! - [`metrics`]: node resource & engine telemetry with Prometheus exposition
//! - [`tls`]: mutual TLS between nodes, and the certificates for it
//! - [`pieces`]: actions too large for one message
pub mod arrowconv;
pub mod config;
pub mod metrics;
pub mod pieces;
pub mod route;
pub mod tls;
pub mod wire;

pub use config::{ClusterConfig, ShardInfo};
pub use tls::NodeTls;
pub use wire::{Fragment, LoadMode, LoadReq, MemberReq, MutateReq, ShardStatus, ShuffleReq, Work};
