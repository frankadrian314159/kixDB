//! Types shared across control and shard nodes in the distributed `kixdb` cluster:
//! - [`config`]: static cluster topology (`cluster.json`)
//! - [`route`]: deterministic 1024-slot entity/symbol hash & date/time partition routing
//! - [`arrowconv`]: lossless columnar `Flip` <-> Apache Arrow `RecordBatch` conversion
//! - [`wire`]: Flight action and ticket messages encoded with `postcard`
//! - [`metrics`]: node resource & engine telemetry with Prometheus exposition
pub mod arrowconv;
pub mod config;
pub mod metrics;
pub mod route;
pub mod wire;

pub use config::{ClusterConfig, ShardInfo};
pub use wire::{Fragment, LoadMode, LoadReq, MemberReq, MutateReq, ShardStatus, ShuffleReq, Work};
