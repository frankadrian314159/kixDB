//! A shard node of distributed kixdb.
//!
//! The node is a complete single-node engine (`kixdb::Server`: storage, write-ahead log, query engine) behind an Arrow
//! Flight service; the cluster adds only routing, planning and merging above it (see the control crate). The service:
//! * `DoGet(ticket = Fragment)` evaluates an expression and streams its value as Arrow record batches;
//! * action `load` replaces, appends to, upserts into or drops a table slice (and its row ordinals);
//! * action `mutate` runs an `update` / `delete` over a table slice;
//! * action `shuffle` sends the rows of a scan straight to the shards that own them;
//! * actions `status`, `metrics`, `members` and `raft` serve operations and replication.
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use kix_shared_ces::arrowconv;
use kix_shared_ces::metrics::NodeMetrics;
use kix_shared_ces::pieces::{Assembly, ACTION_PIECE};
use kix_shared_ces::route::shard_for_keys;
use kix_shared_ces::NodeTls;
use kix_shared_ces::wire::{self, ord_var, Fragment, LoadMode, LoadReq, MemberReq, MutateReq, ShardStatus, ShuffleReq, TxnReq, Work};
use kixdb::delta::{self, ColChange};
use kixdb::engine::joins::{match_keys, union_join};
use kixdb::engine::writes::row_changes;
use kixdb::engine::Write;
use kixdb::ipc::encode_k_payload;
use kixdb::sym::{intern, SymId};
use kixdb::{Attr, ColVec, Connection, Flip, KError, KResult, KeyedTable, Server, K};
use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tonic::{Request, Response, Status, Streaming};

pub mod replica;
mod txn;
mod transport;
pub use replica::{Replica, ReplicaConfig};
use transport::MAX_MESSAGE;

/// Rows per Arrow batch.
const BATCH_ROWS: usize = 8192;

/// What a node's requests and its operator share.
struct Core {
    shard_id: u32,
    server: Server,
    /// Set when this node is one replica of a replicated shard.
    replica: Option<Arc<Replica>>,
    /// One change of a table slice at a time: each reads the slice, then writes it.
    writes: Mutex<()>,
    peers: transport::Peers,
    /// The change prepared and not yet committed, worked out.
    prepared: txn::Prepared,
    /// Actions arriving in pieces.
    pieces: Assembly,
    started: Instant,
    in_flight: AtomicU64,
    statements: AtomicU64,
    draining: AtomicBool,
    /// When each piece of engine work now running began, oldest first.
    running: Mutex<BTreeMap<u64, Instant>>,
    next_work: AtomicU64,
}

/// A piece of engine work, listed in `Core::running` while it runs.
struct Running<'a>(&'a Core, u64);

impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.0.running.lock().unwrap_or_else(|e| e.into_inner()).remove(&self.1);
    }
}

/// Stops the engine work of a request that is dropped, as one is when its caller gives up.
struct Cancel(Arc<AtomicBool>);

impl Drop for Cancel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// A request being served (counted until it ends).
struct InFlight(Arc<Core>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// What a drain did.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DrainReport {
    /// Requests still running when the grace period ended.
    pub abandoned: u64,
    /// A checkpoint was written (the next start replays no log).
    pub checkpointed: bool,
    pub waited_ms: u64,
}

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}

fn decode<'a, T: serde::Deserialize<'a>>(bytes: &'a [u8]) -> Result<T, Status> {
    postcard::from_bytes(bytes).map_err(|e| Status::invalid_argument(format!("bad request: {e}")))
}

fn long_vec(v: Vec<i64>) -> K {
    K::Vector(ColVec::Long(Attr::None, Arc::new(v)))
}

fn unkeyed(k: &K) -> KResult<Flip> {
    match k {
        K::Table(t) => Ok((**t).clone()),
        K::KeyedTable(kt) => Ok(kt.unkey()),
        _ => Err(KError::ty("expected a table")),
    }
}

/// The identity of each row's key (the first `nkeys` columns).
fn row_keys(f: &Flip, nkeys: usize) -> Vec<Vec<u8>> {
    (0..f.nrows()).map(|r| f.vals[..nkeys].iter().flat_map(|c| c.get_atom(r).hash_key()).collect()).collect()
}

/// Writes to the variables of the engine.
type Changes = Vec<(SymId, Write)>;

/// The value `name` has once `staged` is made.
fn staged_value(conn: &mut Connection, staged: &Changes, name: &str) -> KResult<K> {
    let mut value = None;
    for (_, write) in staged.iter().filter(|s| s.0 == intern(name)) {
        value = Some(match write {
            Write::Value(v) => v.clone(),
            Write::Change(changes) => {
                let mut v = match value.take() {
                    Some(v) => v,
                    None => Some(conn.get_var_direct(name)?),
                };
                if !v.as_mut().is_some_and(|v| delta::patch(v, changes)) {
                    return Err(KError::new("cluster", format!("`{name} is not the table the change was made from")));
                }
                v
            }
        });
    }
    match value {
        Some(v) => v.ok_or_else(|| KError::value(name.to_string())),
        None => conn.get_var_direct(name),
    }
}

/// What a load would change on the engine behind `conn`, were `staged` made first.
fn load_changes(conn: &mut Connection, staged: &Changes, req: &LoadReq) -> KResult<Changes> {
    let (table, ords) = (intern(&req.table), intern(&ord_var(&req.table)));
    if req.mode == LoadMode::Drop {
        return Ok(vec![(table, Write::Value(None)), (ords, Write::Value(None))]);
    }
    let rows = req.rows()?;
    if req.mode == LoadMode::Replace {
        let ordinals = req.ords.clone().map(long_vec);
        return Ok(vec![(table, Write::Value(Some(rows))), (ords, Write::Value(ordinals))]);
    }
    let incoming = unkeyed(&rows)?;
    let current = staged_value(conn, staged, &req.table)?;
    // values for foreign keys are checked against the keyed table, which every shard has whole
    let incoming = conn.enumerate_rows(&unkeyed(&current)?, incoming, false)?;
    let keyed = match (&current, req.mode) {
        (K::Table(_), LoadMode::Append) => None,
        (K::KeyedTable(kt), LoadMode::Upsert | LoadMode::CheckInsert) => Some(kt),
        _ => return Err(KError::ty(format!("table `{} does not accept this load", req.table))),
    };
    let key_columns = |kt: &KeyedTable| incoming.vals[..kt.keys.ncols()].iter().collect::<Vec<_>>();
    if req.mode == LoadMode::CheckInsert {
        let found = keyed.map(|kt| match_keys(kt, &key_columns(kt), incoming.nrows()));
        return match found.is_some_and(|f| f.iter().any(Option::is_some)) {
            true => Err(KError::new("insert", "duplicate key in keyed table insert")),
            false => Ok(vec![]),
        };
    }
    // the table is changed where it can be, and otherwise rebuilt; `added`: the incoming rows that become new rows
    let (write, added) = match row_changes(&current, &incoming, false)? {
        Some(rows) => (Write::Change(rows.changes), rows.added),
        None => match (&current, keyed) {
            (K::Table(t), _) => (Write::Value(Some(K::Table(Arc::new(t.concat(&incoming)?)))), (0..incoming.nrows()).collect()),
            (_, Some(kt)) => {
                let nkeys = kt.keys.ncols();
                let mut seen: HashSet<Vec<u8>> = row_keys(&kt.keys, nkeys).into_iter().collect();
                let added = row_keys(&incoming, nkeys).into_iter().enumerate().filter_map(|(i, k)| seen.insert(k).then_some(i)).collect();
                (Write::Value(Some(union_join(&current, &KeyedTable::from_flip_nkeys(&incoming, nkeys)?)?)), added)
            }
            _ => return Err(KError::ty(format!("table `{} does not accept this load", req.table))),
        },
    };
    // (so that the table is not held twice while it changes)
    drop(current);
    let mut changes = vec![(table, write)];
    if let Some(new) = &req.ords {
        let held = match staged_value(conn, staged, &ord_var(&req.table))? {
            K::Vector(ColVec::Long(_, v)) => v.len(),
            _ => return Err(KError::new("cluster", "row ordinals are missing")),
        };
        let tail = ColVec::Long(Attr::None, Arc::new(added.iter().map(|&i| new[i]).collect()));
        changes.push((ords, Write::Change(vec![(0, ColChange::Append { at: held as u64, tail })])));
    }
    Ok(changes)
}

/// Work out an `update` / `delete` over a slice: the empty table with the resulting columns, and what would change.
fn mutation_changes(conn: &mut Connection, req: &MutateReq) -> KResult<(K, Changes)> {
    let result = conn.eval_expr(&req.query)?;
    let nkeys = match &result {
        K::KeyedTable(kt) => kt.keys.ncols(),
        _ => 0,
    };
    let mut flip = unkeyed(&result)?;
    let ordinals = flip.col_idx(intern(wire::ORD_COL)).map(|i| {
        flip.cols.remove(i);
        K::Vector(flip.vals.remove(i))
    });
    let schema = KeyedTable::from_flip_nkeys(&flip.take(0), nkeys)?;
    let mut changes = vec![(intern(&req.table), Write::Value(Some(KeyedTable::from_flip_nkeys(&flip, nkeys)?)))];
    if let Some(o) = ordinals {
        changes.push((intern(&ord_var(&req.table)), Write::Value(Some(o))));
    }
    Ok((schema, changes))
}

fn evaluate(conn: &mut Connection, work: &Work) -> KResult<K> {
    match work {
        Work::Q(code) => conn.eval(code),
        Work::Eval(expr) => conn.eval_expr(expr),
    }
}

impl Core {
    fn metrics(&self) -> NodeMetrics {
        let st = self.server.storage_stats();
        let (cpu_percent, rss_bytes) = NodeMetrics::sample_host();
        NodeMetrics {
            shard_id: self.shard_id,
            cpu_percent,
            rss_bytes,
            workspace_bytes: st.memory_bytes,
            sym_count: st.sym_count,
            table_count: st.table_count,
            statements: self.statements.load(Ordering::SeqCst),
            in_flight: self.in_flight.load(Ordering::SeqCst),
            uptime_secs: self.started.elapsed().as_secs(),
            draining: self.draining.load(Ordering::SeqCst),
        }
    }

    /// A replicated shard serves requests on its leader only; any other replica says where the leader is.
    fn check_leader(&self) -> Result<(), Status> {
        match &self.replica {
            Some(r) if !r.can_serve() => Err(Status::unavailable(format!("{}{}", wire::NOT_LEADER_PREFIX, r.leader_addr().unwrap_or_default()))),
            _ => Ok(()),
        }
    }

    /// Admit a request that uses the engine.
    fn admit(self: &Arc<Self>) -> Result<InFlight, Status> {
        if self.draining.load(Ordering::SeqCst) {
            return Err(Status::unavailable(format!("{}shard {} takes no new work", wire::SHUTTING_DOWN_PREFIX, self.shard_id)));
        }
        self.check_leader()?;
        self.statements.fetch_add(1, Ordering::SeqCst);
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        Ok(InFlight(self.clone()))
    }

    /// Run engine work on a blocking thread.
    async fn blocking<T: Send + 'static>(self: &Arc<Self>, work: impl FnOnce(&Core, &mut Connection) -> KResult<T> + Send + 'static) -> Result<T, Status> {
        let conn = self.server.connect_internal();
        let _stop_if_dropped = Cancel(conn.cancel_flag());
        self.to_the_end(conn, work).await
    }

    /// Run engine work on a blocking thread, whether or not its caller waits for it.
    async fn to_the_end<T: Send + 'static>(self: &Arc<Self>, mut conn: Connection, work: impl FnOnce(&Core, &mut Connection) -> KResult<T> + Send + 'static) -> Result<T, Status> {
        let core = self.clone();
        tokio::task::spawn_blocking(move || {
            let id = core.next_work.fetch_add(1, Ordering::Relaxed);
            core.running.lock().unwrap_or_else(|e| e.into_inner()).insert(id, Instant::now());
            let _listed = Running(&core, id);
            work(&core, &mut conn)
        })
            .await
            .map_err(|e| internal(format!("the request panicked: {e}")))?
            .map_err(|e| wire::status_of(&e))
    }

    async fn load(self: &Arc<Self>, req: LoadReq) -> Result<(), Status> {
        self.blocking(move |core, conn| {
            let _one = core.writes.lock().unwrap_or_else(|e| e.into_inner());
            txn::check_free(&core.server, &req.table)?;
            let changes = load_changes(conn, &vec![], &req)?;
            conn.write_vars(changes)
        })
        .await
    }

    /// Run the scan and send every row to the shard that owns it (or to all of them), straight to that shard.
    async fn shuffle(self: &Arc<Self>, req: ShuffleReq) -> Result<u64, Status> {
        let ShuffleReq { scan, key_cols, target_table, slots, peers, me } = req;
        let rows = self.blocking(move |_, conn| unkeyed(&conn.eval_expr(&scan)?)).await?;
        let total = rows.nrows() as u64;
        let parts: Vec<Flip> = if key_cols.is_empty() {
            vec![rows; peers.len()]
        } else {
            let keys = key_cols
                .iter()
                .map(|c| rows.col(intern(c)).ok_or_else(|| wire::status_of(&KError::value(c.clone()))))
                .collect::<Result<Vec<_>, _>>()?;
            let mut owned = vec![Vec::new(); peers.len()];
            for r in 0..rows.nrows() {
                let values: Vec<K> = keys.iter().map(|k| k.get_atom(r)).collect();
                let shard = shard_for_keys(&values, &slots);
                owned.get_mut(shard).ok_or_else(|| Status::invalid_argument("bad slot map"))?.push(r);
            }
            owned.iter().map(|idx| rows.gather(idx)).collect()
        };
        let sends = parts.into_iter().enumerate().filter(|(_, p)| p.nrows() > 0).map(|(i, part)| {
            let req = LoadReq::new(&target_table, LoadMode::Append, &K::Table(Arc::new(part)), None);
            let addr = peers[i].clone();
            async move {
                if i == me {
                    self.load(req).await
                } else {
                    self.peers.send_load(&addr, &req).await
                }
            }
        });
        for sent in futures::future::join_all(sends).await {
            sent?;
        }
        Ok(total)
    }

    fn status(&self) -> ShardStatus {
        let (role, term) = match &self.replica {
            Some(r) => {
                let (role, term) = r.role();
                (replica::role_name(role), term)
            }
            None => ("single", 0),
        };
        ShardStatus {
            shard_id: self.shard_id,
            role: role.into(),
            term,
            leader_addr: self.replica.as_ref().and_then(|r| r.leader_addr()),
            applied_ts: self.server.applied_ts(),
            memory_bytes: self.server.storage_stats().memory_bytes,
            draining: self.draining.load(Ordering::SeqCst),
        }
    }
}

pub struct ShardNode {
    core: Arc<Core>,
}

/// The handle with which a process watches, drains and stops a node, kept after the node is handed to the server.
#[derive(Clone)]
pub struct NodeControl {
    core: Arc<Core>,
}

impl NodeControl {
    pub fn is_draining(&self) -> bool {
        self.core.draining.load(Ordering::SeqCst)
    }

    pub fn metrics(&self) -> NodeMetrics {
        self.core.metrics()
    }

    /// Stop taking new work, let the requests in progress finish (for at most `grace`), force the log and checkpoint.
    pub async fn drain(&self, grace: Duration) -> DrainReport {
        let t0 = Instant::now();
        self.core.draining.store(true, Ordering::SeqCst);
        while self.core.in_flight.load(Ordering::SeqCst) > 0 && t0.elapsed() < grace {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let abandoned = self.core.in_flight.load(Ordering::SeqCst);
        let server = self.core.server.clone();
        let checkpointed = tokio::task::spawn_blocking(move || server.flush_log().and_then(|_| server.checkpoint()).is_ok()).await.unwrap_or(false);
        DrainReport { abandoned, checkpointed, waited_ms: t0.elapsed().as_millis() as u64 }
    }
}

impl ShardNode {
    pub fn new(shard_id: u32, server: Server) -> ShardNode {
        ShardNode::build(shard_id, server, None)
    }

    /// A node that is one replica of a replicated shard: `server` must be a persistent engine; the replica keeps its Raft
    /// log in `cfg.dir`. Must be called inside a tokio runtime.
    pub fn replicated(shard_id: u32, server: Server, cfg: ReplicaConfig) -> std::io::Result<ShardNode> {
        let tls = cfg.tls.clone();
        let limit = cfg.stall_limit;
        let replica = Replica::start(cfg, server.clone())?;
        let node = ShardNode::build(shard_id, server, Some(replica.clone())).with_tls(tls);
        if !limit.is_zero() {
            // (it ends with the node, which it does not keep alive)
            let core = Arc::downgrade(&node.core);
            tokio::spawn(async move {
                let mut stuck = false;
                while let Some(core) = core.upgrade() {
                    let oldest = core.running.lock().unwrap_or_else(|e| e.into_inner()).values().next().copied();
                    let now = oldest.is_some_and(|since| since.elapsed() >= limit);
                    if now != stuck {
                        stuck = now;
                        replica.set_unfit(stuck);
                    }
                    drop(core);
                    tokio::time::sleep(limit.min(Duration::from_secs(1)) / 5).await;
                }
            });
        }
        Ok(node)
    }

    /// Require mutual TLS of every caller and use it towards the other shards (`None`: unprotected connections). A
    /// replica takes this from its configuration. Call before the node serves.
    pub fn with_tls(mut self, tls: Option<NodeTls>) -> ShardNode {
        Arc::get_mut(&mut self.core).expect("a node is configured before it serves").peers.tls = tls;
        self
    }

    fn build(shard_id: u32, server: Server, replica: Option<Arc<Replica>>) -> ShardNode {
        let core = Core {
            shard_id,
            server,
            replica,
            writes: Mutex::default(),
            peers: Default::default(),
            prepared: Default::default(),
            pieces: Assembly::default(),
            started: Instant::now(),
            in_flight: AtomicU64::new(0),
            statements: AtomicU64::new(0),
            draining: AtomicBool::new(false),
            running: Mutex::default(),
            next_work: AtomicU64::new(0),
        };
        ShardNode { core: Arc::new(core) }
    }

    pub fn control(&self) -> NodeControl {
        NodeControl { core: self.core.clone() }
    }

    pub fn into_service(self) -> FlightServiceServer<ShardNode> {
        FlightServiceServer::new(self).max_decoding_message_size(MAX_MESSAGE).max_encoding_message_size(MAX_MESSAGE)
    }
}

type FlightStream<T> = BoxStream<'static, Result<T, Status>>;

#[tonic::async_trait]
impl FlightService for ShardNode {
    type HandshakeStream = FlightStream<HandshakeResponse>;
    type ListFlightsStream = FlightStream<FlightInfo>;
    type DoGetStream = FlightStream<FlightData>;
    type DoPutStream = FlightStream<PutResult>;
    type DoActionStream = FlightStream<arrow_flight::Result>;
    type ListActionsStream = FlightStream<ActionType>;
    type DoExchangeStream = FlightStream<FlightData>;

    async fn do_get(&self, request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let frag: Fragment = decode(&request.into_inner().ticket)?;
        let _busy = self.core.admit()?;
        let (schema, batches) = self
            .core
            .blocking(move |_, conn| {
                let value = evaluate(conn, &frag.work)?;
                arrowconv::k_to_batches(&value, BATCH_ROWS).map_err(|e| KError::new("cluster", e))
            })
            .await?;
        let out = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::iter(batches.into_iter().map(Ok)))
            .map_err(internal);
        Ok(Response::new(out.boxed()))
    }

    async fn do_action(&self, request: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> {
        let mut action = request.into_inner();
        let core = &self.core;
        let reply = |body: Vec<u8>| Response::new(stream::iter(vec![Ok(arrow_flight::Result { body: body.into() })]).boxed());
        if action.r#type == ACTION_PIECE {
            let Some((kind, body)) = core.pieces.add(&action.body).map_err(Status::invalid_argument)? else { return Ok(reply(vec![])) };
            action = Action::new(kind, body);
        }
        let encode = |r: Result<Vec<u8>, postcard::Error>| r.map_err(internal);
        let body: Vec<u8> = match action.r#type.as_str() {
            wire::ACTION_LOAD => {
                let req: LoadReq = decode(&action.body)?;
                let _busy = core.admit()?;
                core.load(req).await?;
                vec![]
            }
            wire::ACTION_MUTATE => {
                let req: MutateReq = decode(&action.body)?;
                let _busy = core.admit()?;
                core.blocking(move |core, conn| {
                    let _one = core.writes.lock().unwrap_or_else(|e| e.into_inner());
                    txn::check_free(&core.server, &req.table)?;
                    let (schema, changes) = mutation_changes(conn, &req)?;
                    conn.write_vars(changes)?;
                    Ok(encode_k_payload(&schema))
                })
                .await?
            }
            wire::ACTION_TXN => {
                let req: TxnReq = decode(&action.body)?;
                let _busy = core.admit()?;
                // a commit goes on without its caller: it must not stop half decided
                let commits = matches!(req, TxnReq::Commit { .. });
                let work = move |core: &Core, conn: &mut Connection| {
                    let _one = core.writes.lock().unwrap_or_else(|e| e.into_inner());
                    txn::run(&core.server, conn, &core.prepared, req)
                };
                let reply = match commits {
                    true => core.to_the_end(core.server.connect_internal(), work).await?,
                    false => core.blocking(work).await?,
                };
                encode(postcard::to_allocvec(&reply))?
            }
            wire::ACTION_SHUFFLE => {
                let req: ShuffleReq = decode(&action.body)?;
                let _busy = core.admit()?;
                let sent = core.shuffle(req).await?;
                encode(postcard::to_allocvec(&sent))?
            }
            wire::ACTION_STATUS => encode(postcard::to_allocvec(&core.status()))?,
            wire::ACTION_METRICS => {
                let c = core.clone();
                let m = tokio::task::spawn_blocking(move || c.metrics()).await.map_err(internal)?;
                encode(postcard::to_allocvec(&m))?
            }
            wire::ACTION_MEMBERS => {
                let req: MemberReq = decode(&action.body)?;
                let Some(r) = core.replica.clone() else {
                    return Err(wire::status_of(&KError::new("cluster", "this shard is not replicated")));
                };
                // listing too: a follower may be an entry behind
                core.check_leader()?;
                let members = core
                    .blocking(move |_, _| {
                        if let Some((id, addr)) = &req.add {
                            r.add_replica(*id, addr).map_err(|e| KError::new("cluster", e))?;
                        }
                        if let Some(id) = req.remove {
                            r.remove_replica(id).map_err(|e| KError::new("cluster", e))?;
                        }
                        Ok(r.members())
                    })
                    .await?;
                encode(postcard::to_allocvec(&members))?
            }
            wire::ACTION_RAFT => {
                let env: replica::Envelope = decode(&action.body)?;
                if let Some(r) = &core.replica {
                    r.deliver(env.from, env.msg);
                }
                vec![]
            }
            other => return Err(Status::unimplemented(format!("unknown action {other}"))),
        };
        Ok(reply(body))
    }

    async fn list_actions(&self, _r: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        let names = [wire::ACTION_LOAD, wire::ACTION_MUTATE, wire::ACTION_SHUFFLE, wire::ACTION_STATUS, wire::ACTION_METRICS, wire::ACTION_MEMBERS, wire::ACTION_RAFT, wire::ACTION_TXN];
        let acts = names.map(|n| Ok(ActionType { r#type: n.to_string(), description: String::new() }));
        Ok(Response::new(stream::iter(acts).boxed()))
    }
    async fn handshake(&self, _r: Request<Streaming<HandshakeRequest>>) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }
    async fn list_flights(&self, _r: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }
    async fn get_flight_info(&self, _r: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }
    async fn poll_flight_info(&self, _r: Request<FlightDescriptor>) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }
    async fn get_schema(&self, _r: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }
    async fn do_put(&self, _r: Request<Streaming<FlightData>>) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }
    async fn do_exchange(&self, _r: Request<Streaming<FlightData>>) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
}

/// A shard node that is listening (for tests and for the `kix-shard` binary).
pub struct RunningShard {
    pub addr: SocketAddr,
    /// The node's engine (for tests and tools that look at a shard directly).
    pub server: Server,
    pub replica: Option<Arc<Replica>>,
    control: NodeControl,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<()>>,
}

impl RunningShard {
    pub fn control(&self) -> &NodeControl {
        &self.control
    }

    /// Drain, then stop the listener (and the replica: the others elect a new leader).
    pub async fn graceful_shutdown(self, grace: Duration) -> DrainReport {
        let report = self.control.drain(grace).await;
        self.shutdown().await;
        report
    }

    /// Stop the listener at once, as when a node dies.
    pub async fn shutdown(mut self) {
        if let Some(r) = &self.replica {
            r.stop();
        }
        if let Some(s) = self.stop.take() {
            let _ = s.send(());
        }
        if let Some(j) = self.join.take() {
            // open connections would keep a graceful stop waiting
            let abort = j.abort_handle();
            if tokio::time::timeout(Duration::from_millis(500), j).await.is_err() {
                abort.abort();
            }
        }
    }
}

/// Bind `addr` (port 0 picks a free one) and serve the node on a background task.
pub async fn spawn(node: ShardNode, addr: SocketAddr) -> std::io::Result<RunningShard> {
    spawn_on(node, tokio::net::TcpListener::bind(addr).await?).await
}

/// Serve the node on a listener that is already bound (the replicas of a group must know each other's addresses before
/// any of them starts, so they bind first).
pub async fn spawn_on(node: ShardNode, listener: tokio::net::TcpListener) -> std::io::Result<RunningShard> {
    let addr = listener.local_addr()?;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let incoming = tonic::transport::server::TcpIncoming::from(listener).with_nodelay(Some(true));
    let mut builder = tonic::transport::Server::builder();
    if let Some(tls) = &node.core.peers.tls {
        builder = builder.tls_config(tls.server()).map_err(std::io::Error::other)?;
    }
    let control = node.control();
    let (server, replica) = (node.core.server.clone(), node.core.replica.clone());
    let service = node.into_service();
    let join = tokio::spawn(async move {
        let _ = builder
            .max_concurrent_streams(Some(1024))
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            })
            .await;
    });
    Ok(RunningShard { addr, server, replica, control, stop: Some(stop), join: Some(join) })
}
