//! The control node of distributed kixdb.
//!
//! The control node keeps a **mirror** engine that holds the schema of every distributed table (the table with no rows)
//! and everything that is not a table: variables, functions, views. The rows live on the shards. A statement is run by
//! the cheapest way that gives the answer one engine would give:
//! * **pushed down** to the shards ([`plan`], [`exec`]): routed to one shard, run on every shard and concatenated or
//!   merged, with joins made co-located by broadcasting or shuffling a table; inserts, upserts, updates and deletes are
//!   split over the shards;
//! * **gathered** ([`session`]): anything else gets the tables it names copied into the mirror, runs there, and the
//!   tables it changed are distributed again.
mod catalog;
pub mod client;
mod commit;
mod exec;
mod merge;
mod plan;
mod session;

pub use session::ClusterSession;

use client::ShardClient;
use futures::future::{join_all, try_join_all};
use kix_shared_ces::NodeTls;
use kix_shared_ces::metrics::{render_prometheus, NodeMetrics};
use kix_shared_ces::route::even_slots;
use kix_shared_ces::wire::{ord_var, MemberInfo, MemberReq, ShardStatus, Work};
use kix_shared_ces::{ClusterConfig, ShardInfo};
use kixdb::engine::DistMode;
use kixdb::sym::{resolve, SymId};
use kixdb::{Interceptor, KError, KResult, Server, K};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

/// A table with at most this many rows is copied to every shard for a join, rather than shuffled.
const BROADCAST_LIMIT: usize = 100_000;
const STATE_MAGIC: [u8; 4] = *b"KXCS";

/// What `cluster.state` holds.
#[derive(Serialize, Deserialize)]
struct State {
    shards: Vec<ShardInfo>,
    slots: Vec<u32>,
    /// The mirror engine's exported state: table schemas and distribution modes, variables, functions, views.
    mirror: Vec<u8>,
}

struct Inner {
    runtime: tokio::runtime::Runtime,
    shards: Vec<ShardClient>,
    /// Which shard owns each of the 1024 key hash slots.
    slots: Vec<u32>,
    mirror: Server,
    /// Pushed-down reads share the cluster; every other statement has it to itself.
    gate: RwLock<()>,
    /// The next global row ordinal (it orders the rows of sharded tables as one engine would hold them).
    next_ord: AtomicI64,
    next_temp: AtomicU64,
    broadcast_limit: AtomicUsize,
    /// How many rows tables had when last counted or written (only this node changes them).
    rows: Mutex<HashMap<SymId, usize>>,
    state_path: Option<PathBuf>,
    /// Since when some shard may hold a change waiting to be resolved (and when to try).
    unsettled: Mutex<Option<Instant>>,
    /// Changes every shard committed, which their primaries have yet to be told of: `(primary, txid)`.
    finished: Mutex<Vec<(usize, u64)>>,
    _state_lock: Option<std::fs::File>,
}

impl Inner {
    /// Wait for work on the shards (never call from inside a tokio runtime).
    fn wait<T>(&self, f: impl Future<Output = T>) -> T {
        self.runtime.block_on(f)
    }

    fn members(&self, shard: usize, req: &MemberReq) -> KResult<Vec<MemberInfo>> {
        let s = self.shards.get(shard).ok_or_else(|| KError::domain(format!("no shard {shard}")))?;
        let _alone = self.gate.write().unwrap_or_else(|e| e.into_inner());
        let members = self.wait(s.members(req))?;
        self.save_state()?;
        Ok(members)
    }

    fn save_state(&self) -> KResult<()> {
        let Some(path) = &self.state_path else { return Ok(()) };
        let shards = self.shards.iter().map(|s| ShardInfo { id: s.id, addr: s.addresses() }).collect();
        let state = State { shards, slots: self.slots.clone(), mirror: self.mirror.export_state()? };
        let payload = postcard::to_allocvec(&state).map_err(|e| KError::io(e.to_string()))?;
        let crc = crc32fast::hash(&payload);
        let mut out = Vec::with_capacity(8 + payload.len());
        out.extend_from_slice(&STATE_MAGIC);
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&payload);
        let tmp = path.with_extension("state.tmp");
        std::fs::write(&tmp, out)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }
}

/// A distributed kixdb cluster, seen from its control node. Cloning shares it.
#[derive(Clone)]
pub struct Cluster {
    inner: Arc<Inner>,
}

impl Cluster {
    /// Start a control node for the shards of `config`, with no state file, over unprotected connections.
    pub fn connect(config: &ClusterConfig) -> KResult<Cluster> {
        Self::connect_with(config, None)
    }

    /// `connect`, proving who the control node is with `tls` and accepting only shards that can do the same.
    pub fn connect_with(config: &ClusterConfig, tls: Option<NodeTls>) -> KResult<Cluster> {
        Self::build(config.shards.clone(), even_slots(config.shards.len()), tls, None, None, None)
    }

    /// Start a control node that keeps its state in `path`. An existing file picks the cluster up where it was: it
    /// decides the number of shards and their addresses (which follow membership changes), to which a `config` with as
    /// many shards adds any other replica addresses.
    pub fn open(path: impl AsRef<Path>, config: &ClusterConfig) -> KResult<Cluster> {
        Self::open_with(path, config, None)
    }

    /// `open`, over connections protected by `tls` (as for `connect_with`).
    pub fn open_with(path: impl AsRef<Path>, config: &ClusterConfig, tls: Option<NodeTls>) -> KResult<Cluster> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let lock_path = path.with_extension("state.lock");
        let lock_file = std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(&lock_path)?;
        if lock_file.try_lock().is_err() {
            return Err(KError::io(format!("cluster state '{}' is in use by another process", path.display())));
        }
        if !path.exists() {
            let c = Self::build(config.shards.clone(), even_slots(config.shards.len()), tls, None, Some(path), Some(lock_file))?;
            c.inner.save_state()?;
            return Ok(c);
        }
        let raw = std::fs::read(&path)?;
        let payload: &[u8] = if raw.len() >= 8 && raw[..4] == STATE_MAGIC {
            let expected = u32::from_le_bytes(raw[4..8].try_into().unwrap());
            let actual = crc32fast::hash(&raw[8..]);
            if expected != actual {
                return Err(KError::io(format!("{}: state checksum mismatch", path.display())));
            }
            &raw[8..]
        } else {
            return Err(KError::io(format!("{}: invalid or corrupt cluster state header", path.display())));
        };
        let state: State = postcard::from_bytes(payload).map_err(|e| KError::io(format!("{}: {e}", path.display())))?;
        let mut shards = state.shards;
        if !config.shards.is_empty() {
            if config.shards.len() != shards.len() {
                let message = format!("the configuration has {} shard(s), the state file {}", config.shards.len(), shards.len());
                return Err(KError::new("cluster", message));
            }
            for (known, given) in shards.iter_mut().zip(&config.shards) {
                for addr in given.endpoints() {
                    if !known.endpoints().contains(&addr) {
                        known.addr = format!("{},{addr}", known.addr);
                    }
                }
            }
        }
        Self::build(shards, state.slots, tls, Some(state.mirror), Some(path), Some(lock_file))
    }

    fn build(
        info: Vec<ShardInfo>,
        slots: Vec<u32>,
        tls: Option<NodeTls>,
        mirror_state: Option<Vec<u8>>,
        state_path: Option<PathBuf>,
        state_lock: Option<std::fs::File>,
    ) -> KResult<Cluster> {
        if info.is_empty() {
            return Err(KError::new("cluster", "the cluster has no shards"));
        }
        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build()?;
        let shards = {
            let _in_runtime = runtime.enter();
            info.iter().map(|s| ShardClient::new(s.id, &s.addr, tls.clone())).collect::<KResult<Vec<_>>>()?
        };
        let mirror = Server::new();
        if let Some(state) = &mirror_state {
            mirror.install_state(state)?;
        }
        let inner = Inner {
            runtime,
            shards,
            slots,
            mirror,
            gate: RwLock::new(()),
            next_ord: AtomicI64::new(0),
            next_temp: AtomicU64::new(0),
            broadcast_limit: AtomicUsize::new(BROADCAST_LIMIT),
            rows: Mutex::default(),
            state_path,
            // (a control node before this one may have left changes half made)
            unsettled: Mutex::new(Some(Instant::now())),
            finished: Mutex::default(),
            _state_lock: state_lock,
        };
        let cluster = Cluster { inner: Arc::new(inner) };
        if mirror_state.is_some() {
            cluster.recover_ordinals()?;
        }
        Ok(cluster)
    }

    /// Continue numbering rows after the largest ordinal any shard holds.
    fn recover_ordinals(&self) -> KResult<()> {
        let partitioned: Vec<String> = self.inner.mirror.read_workspace(|ws| {
            ws.dist
                .iter()
                .filter(|(name, mode)| !matches!(mode, DistMode::Replicated) && ws.vars.contains_key(name))
                .map(|(&name, _)| resolve(name))
                .collect()
        });
        for table in partitioned {
            let query = Work::Q(format!("max {}", ord_var(&table)));
            let answers = self.inner.wait(try_join_all(self.inner.shards.iter().map(|s| s.query(query.clone()))))?;
            for a in answers {
                if let K::Long(max) = a {
                    self.inner.next_ord.fetch_max(max.saturating_add(1).max(0), Ordering::SeqCst);
                }
            }
        }
        Ok(())
    }

    pub fn session(&self) -> ClusterSession {
        ClusterSession::new(self.inner.clone())
    }

    pub fn shard_count(&self) -> usize {
        self.inner.shards.len()
    }

    /// The largest table (in rows) that a join copies to every shard instead of shuffling.
    pub fn set_broadcast_limit(&self, rows: usize) {
        self.inner.broadcast_limit.store(rows, Ordering::SeqCst);
    }

    /// Fail a request that a shard has not answered after `limit` (300 s to begin with; zero: never). A change that times
    /// out may still be made.
    pub fn set_request_timeout(&self, limit: Duration) {
        self.inner.shards.iter().for_each(|s| s.set_limit(limit));
    }

    /// Wait until every shard answers; the ids of those that do not within `limit`.
    pub fn wait_for_shards(&self, limit: Duration) -> Result<(), Vec<u32>> {
        let deadline = Instant::now() + limit;
        loop {
            let answers = |s: &ShardClient| {
                let left = deadline.saturating_duration_since(Instant::now()).max(Duration::from_secs(1));
                self.inner.wait(async { matches!(tokio::time::timeout(left, s.query(Work::Q("::".into()))).await, Ok(Ok(_))) })
            };
            let down: Vec<u32> = self.inner.shards.iter().filter(|s| !answers(s)).map(|s| s.id).collect();
            if down.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(down);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn status(&self) -> Vec<KResult<ShardStatus>> {
        self.inner.wait(join_all(self.inner.shards.iter().map(|s| s.status())))
    }

    /// The members of a replicated shard, after making the change `req` asks for.
    pub fn members(&self, shard: usize, req: &MemberReq) -> KResult<Vec<MemberInfo>> {
        self.inner.members(shard, req)
    }

    /// Prometheus text for every shard that answers.
    pub fn prometheus(&self) -> String {
        let answers = self.inner.wait(join_all(self.inner.shards.iter().map(|s| s.metrics())));
        let up: Vec<NodeMetrics> = answers.into_iter().filter_map(Result::ok).collect();
        let mut out = format!("# TYPE kixdb_shards_up gauge\nkixdb_shards_up {}\n", up.len());
        out.push_str(&render_prometheus(&up));
        out
    }

    /// A server whose connections run on the cluster: bind a `kixdb::ipc::Listener` to it to serve kdb+ IPC clients
    /// (`kix`, `kix-gui`, `hopen`) as one database.
    pub fn gateway(&self) -> Server {
        let inner = Arc::downgrade(&self.inner);
        let cluster_gone = || -> Box<dyn Interceptor> { Box::new(Closed) };
        self.inner.mirror.set_interceptor(Some(Arc::new(move || match inner.upgrade() {
            Some(inner) => Box::new(ClusterSession::new(inner)) as Box<dyn Interceptor>,
            None => cluster_gone(),
        })));
        self.inner.mirror.clone()
    }

    /// Stop serving new gateway connections and write the state.
    pub fn shutdown(&self) -> KResult<()> {
        self.inner.mirror.set_interceptor(None);
        self.inner.save_state()
    }
}

/// What a gateway connection gets once the cluster has been dropped.
struct Closed;

impl Interceptor for Closed {
    fn eval_query(&mut self, _code: &str) -> Option<KResult<K>> {
        Some(Err(KError::new("cluster", "the cluster is shut down")))
    }
}
