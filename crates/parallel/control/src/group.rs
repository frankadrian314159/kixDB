//! Several control nodes. They form a group that elects one of them the **master** (Raft, with the replicas of a
//! replicated shard): what is replicated is the mirror, with a record of the shards. The master alone changes anything.
//! The others answer reads themselves, each under a **permit** of the master's, and pass the other statements of their
//! clients to it. A new master first has every shard refuse the masters before it (they send the term they were elected
//! for), then finishes what was left half done.
use crate::client::ShardClient;
use crate::{cluster_error, read_state, Cluster, ClusterSession, Inner, State};
use futures::future::try_join_all;
use kix_shared_ces::wire::{self, ForwardReply, ForwardReq, Forwarded, MemberInfo, MemberReq, Permit, PermitReply, PermitReq, TxnReply, TxnReq, PERMIT_LIMIT};
use kix_shared_ces::{ClusterConfig, NodeTls};
use kix_raft::Role;
use kix_storage::{spawn_on, Replica, ReplicaConfig, RunningShard, ShardNode};
use kixdb::persist::LogOp;
use kixdb::sym::intern;
use kixdb::{Attr, ColVec, KError, KResult, Server, K};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};
use tonic::Status;

/// How long a new node tries to be taken into a group.
const JOIN_LIMIT: Duration = Duration::from_secs(60);
/// How long a statement waits for there to be a master.
const NO_MASTER: Duration = Duration::from_secs(10);
/// How long a statement waits for a leader that has not heard from a majority lately, as when a disk is slow.
const HEARING: Duration = Duration::from_secs(1);
/// A master answers only while a majority has answered it this recently, by the clock.
const LEASE: Duration = Duration::from_millis(300);
/// How long a new master waits before it takes over: longer than the lease of the one before it can still run, as that
/// one was last answered before the others began to wait for it (at least `LEASE` under their election timeout), which
/// leaves 300 ms for an answer to be on its way.
const OUTLAST: Duration = Duration::from_millis(400);
/// The mirror variable that records the shards and what each owns.
const RECORD: &str = ".kix.cluster";
/// The mirror variable that holds a change of the control node's that goes with a change on the shards which is not yet
/// known to be made: `(txid, primary shard, what to write)`.
const PENDING: &str = ".kix.pending";

/// How a control node takes part in a group.
#[derive(Clone, Debug)]
pub struct GroupConfig {
    /// Its number in the group.
    pub id: u64,
    /// Where it listens for the other control nodes.
    pub listen: SocketAddr,
    /// How they reach it, if not at the address it listens on.
    pub advertise: Option<String>,
    /// Where it keeps its copy of the mirror and the group's log.
    pub data: PathBuf,
    /// A control node of the group to join. `None`: the node starts a group, or is in one already.
    pub join: Option<String>,
    /// The state file of a control node that was alone, which a new group takes over.
    pub import: Option<PathBuf>,
}

pub(crate) struct Group {
    replica: Arc<Replica>,
    node: Mutex<Option<RunningShard>>,
    address: String,
    /// The term this node has taken over for.
    settled: AtomicU64,
    /// The connection to the master, for the statements passed to it.
    master: Mutex<Option<(String, Arc<ShardClient>)>>,
    /// The permits to read that this node, as master, has out: each is held while its sender is here.
    permits: Mutex<HashMap<u64, mpsc::Sender<()>>>,
    next_permit: AtomicU64,
    halted: AtomicBool,
}

impl Drop for Group {
    fn drop(&mut self) {
        self.replica.leave();
    }
}

impl Group {
    pub(crate) fn leads(&self) -> bool {
        self.term().is_some()
    }

    /// The term for which this node is the master now. A leader that is not ready to serve this moment (it has a change
    /// to apply, or has not heard from a majority lately) is waited for.
    fn term(&self) -> Option<u64> {
        let deadline = Instant::now() + HEARING;
        loop {
            let (role, term) = self.replica.role();
            if self.halted.load(Ordering::SeqCst) || role != Role::Leader {
                return None;
            }
            if self.replica.can_serve() && self.replica.heard_lately(LEASE) {
                return Some(term);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

fn not_master() -> KError {
    cluster_error("this control node is not the master")
}

fn bytes(of: &impl serde::Serialize) -> KResult<K> {
    let bytes = postcard::to_allocvec(of).map_err(|e| cluster_error(e.to_string()))?;
    Ok(K::Vector(ColVec::Byte(Attr::None, Arc::new(bytes))))
}

impl Cluster {
    /// Start a control node of a group. The first of a group takes the shards from `config` (or from the state file it
    /// imports); one that joins learns them, and everything else, from the group.
    pub fn open_group(group: GroupConfig, config: &ClusterConfig, tls: Option<NodeTls>) -> KResult<Cluster> {
        std::fs::create_dir_all(&group.data)?;
        let (state, first) = (group.data.join("state"), group.data.join("first"));
        let known = state.exists();
        let import = match &group.import {
            // (also when the first node stopped before it had written what it took over)
            Some(path) if (!known || first.exists()) && path.exists() => match read_state(path)? {
                (_, true) => return Err(cluster_error("start a single control node on this old state file once before a group takes it over")),
                (state, false) => Some(state),
            },
            _ => None,
        };
        if !known && group.join.is_none() {
            if config.shards.is_empty() && import.is_none() {
                return Err(cluster_error("the first control node of a group needs the shards: a configuration or a state file"));
            }
            std::fs::write(&first, "")?;
        }
        let mirror = Server::open(&state)?;
        let cluster = Self::build(Self::new_state(config), mirror.clone(), tls.clone(), None, None)?;
        let inner = &cluster.inner;
        let listener = inner.wait(tokio::net::TcpListener::bind(group.listen))?;
        let address = match &group.advertise {
            Some(address) => address.clone(),
            None => listener.local_addr()?.to_string(),
        };
        let master = match &group.join {
            Some(at) => {
                let _in_runtime = inner.runtime.enter();
                Some(ShardClient::new(0, at, tls.clone())?)
            }
            None => None,
        };
        let mut peers = vec![];
        if let Some(master) = &master {
            match inner.until_answered(|| inner.wait(master.members(&MemberReq::default())), if known { Duration::ZERO } else { JOIN_LIMIT }) {
                Ok(members) => peers = members.into_iter().filter(|m| m.0 != group.id).map(|m| (m.0, m.1)).collect(),
                // (a node that is in the group already knows the others)
                Err(e) if known => eprintln!("kix-cluster: cannot ask {:?} about the group: {e}", group.join),
                Err(e) => return Err(e),
            }
        }
        let mut replica = ReplicaConfig::new(group.id, peers, group.data.join("raft"));
        (replica.addr, replica.tls, replica.join) = (address.clone(), tls, !first.exists());
        let node = {
            let _in_runtime = inner.runtime.enter();
            ShardNode::replicated(group.id as u32, mirror, replica)?
        };
        let weak = Arc::downgrade(inner);
        let node = node.with_extension(Box::new(move |kind, body| {
            [wire::ACTION_FORWARD, wire::ACTION_PERMIT].contains(&kind).then(|| weak.upgrade().ok_or_else(|| Status::unavailable("the control node is gone"))?.answer(kind, body))
        }));
        // (the temporary tables of each node's reads have names of their own on the shards)
        inner.next_temp.store(group.id << 40, Ordering::SeqCst);
        let replica = node.replica().ok_or_else(|| cluster_error("the control node has no replica"))?;
        // a new group's first node writes the shards it was given, and what it takes over, before any node can join
        if first.exists() && inner.held::<State>(RECORD)?.is_none() {
            inner.until_answered(|| replica.can_serve().then_some(()).ok_or_else(not_master), JOIN_LIMIT)?;
            if let Some(state) = import {
                let held = Server::new();
                held.install_state(&state.mirror)?;
                let blank = Server::new().read_workspace(kixdb::engine::Workspace::snapshot);
                held.read_workspace(|ws| inner.mirror.connect_internal().adopt(&blank, ws, &[]))?;
                inner.set_topology(inner.topology_of(&state.shards, state.slots, state.resizes)?);
            }
            inner.record()?;
        }
        let running = inner.wait(spawn_on(node, listener))?;
        let joined = Group {
            replica,
            node: Mutex::new(Some(running)),
            address: address.clone(),
            settled: AtomicU64::new(0),
            master: Mutex::default(),
            permits: Mutex::default(),
            next_permit: AtomicU64::new(0),
            halted: AtomicBool::new(false),
        };
        let _ = inner.group.set(joined);
        if let Some(master) = &master {
            // (a node that has run before is in the group, unless an earlier join was cut short: it asks once)
            let add = MemberReq { add: Some((group.id, address)), remove: None };
            if let Err(e) = inner.until_answered(|| inner.wait(master.members(&add)), if known { Duration::ZERO } else { JOIN_LIMIT }) {
                if !known {
                    cluster.halt();
                    return Err(cluster_error(format!("the group did not take this node in: {}", e.detail)));
                }
                eprintln!("kix-cluster: cannot join through {:?}: {e}", group.join);
            }
        }
        // a node that is elected takes over at once, not at the first statement it is given
        let weak = Arc::downgrade(inner);
        std::thread::spawn(move || {
            while let Some(inner) = weak.upgrade() {
                match inner.group.get() {
                    Some(group) if !group.halted.load(Ordering::SeqCst) => drop(group.term().map(|_| inner.settle())),
                    _ => break,
                }
                drop(inner);
                std::thread::sleep(Duration::from_millis(100));
            }
        });
        Ok(cluster)
    }

    /// Whether this node answers statements itself: it is alone, or the master of its group.
    pub fn is_master(&self) -> bool {
        self.inner.is_master()
    }

    /// The address of this node in its group.
    pub fn control_address(&self) -> Option<String> {
        self.inner.group.get().map(|g| g.address.clone())
    }

    /// Leave off taking part in the group at once, as a node that dies does. (`shutdown` hands over first.)
    pub fn halt(&self) {
        let Some(group) = self.inner.group.get() else { return };
        group.halted.store(true, Ordering::SeqCst);
        group.replica.leave();
        let node = group.node.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(node) = node {
            self.inner.wait(node.shutdown());
        }
    }

    /// For tests: cut this node off from the other control nodes (it still reaches the shards), or end that.
    #[doc(hidden)]
    pub fn cut_off(&self, cut: bool) {
        if let Some(group) = self.inner.group.get() {
            group.replica.cut_off(cut);
        }
    }

    /// Have another node lead, if there is one that can, and leave.
    pub(crate) fn leave_group(&self) {
        if let Some(group) = self.inner.group.get() {
            if group.replica.members().len() > 1 {
                group.replica.set_unfit(true);
                let deadline = Instant::now() + Duration::from_secs(3);
                while group.replica.leader_addr().is_none() && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
            self.halt();
        }
    }
}

impl Inner {
    /// Ask until there is an answer, for at most `limit` (a group may be electing its master).
    fn until_answered<T>(&self, ask: impl Fn() -> KResult<T>, limit: Duration) -> KResult<T> {
        let deadline = Instant::now() + limit;
        loop {
            match ask() {
                Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(100)),
                answer => return answer,
            }
        }
    }

    /// The value of a mirror variable that holds bytes, decoded.
    fn held<T: serde::de::DeserializeOwned>(&self, name: &str) -> KResult<Option<T>> {
        match self.mirror.read_workspace(|ws| ws.vars.get(&intern(name)).cloned()) {
            Some(K::Vector(ColVec::Byte(_, bytes))) => postcard::from_bytes(&bytes).map(Some).map_err(|e| cluster_error(format!("{name} is damaged: {e}"))),
            _ => Ok(None),
        }
    }

    /// Whether the node is in a group and is its master up to date: an error if it is in one and is not the master.
    pub(crate) fn current(&self) -> KResult<bool> {
        match self.group.get() {
            Some(group) => Ok(group.term().ok_or_else(not_master)? == group.settled.load(Ordering::SeqCst)),
            None => Ok(true),
        }
    }

    /// A node that is not the master takes the shards to be those the master recorded.
    pub(crate) fn follow(&self) {
        if self.group.get().is_some_and(|g| g.term().is_none()) {
            if let Ok(Some(state)) = self.held::<State>(RECORD) {
                if let Ok(topology) = self.topology_of(&state.shards, state.slots, state.resizes) {
                    self.set_topology(topology);
                }
            }
        }
    }

    /// Write the record of the shards in the mirror, if it is not there as it is.
    pub(crate) fn record(&self) -> KResult<()> {
        let held = self.topology();
        let record = bytes(&State { shards: held.info(), slots: (*held.slots).clone(), mirror: vec![], resizes: held.resizes })?;
        if self.mirror.read_workspace(|ws| ws.vars.get(&intern(RECORD)) != Some(&record)) {
            self.mirror.connect_internal().set_var_direct(RECORD, record)?;
        }
        Ok(())
    }

    /// Before a master of a group does anything: have every shard refuse earlier masters, finish what was left half
    /// done, and learn what only the shards know. Nothing happens unless the node was elected since it last did this.
    /// The caller has the cluster to itself.
    pub(crate) fn take_over(&self) -> KResult<()> {
        let Some(group) = self.group.get() else { return Ok(()) };
        let term = group.term().ok_or_else(not_master)?;
        if group.settled.load(Ordering::SeqCst) == term {
            return Ok(());
        }
        // an earlier master, cut off, may still take itself to lead and answer from its copy of the mirror
        std::thread::sleep(OUTLAST);
        self.term.store(term, Ordering::SeqCst);
        let state = self.held::<State>(RECORD)?.ok_or_else(|| cluster_error("the group has no record of its shards"))?;
        self.set_topology(self.topology_of(&state.shards, state.slots, state.resizes)?);
        // shard 0 first: its layout, if it holds one, says which the other shards are
        self.wait(self.shard(0)?.fence())?;
        if let Some(held) = self.fetch_layout()? {
            self.set_topology(self.topology_of(&held.shards, held.slots, held.resizes)?);
        }
        let shards = self.shards();
        self.wait(try_join_all(shards.iter().map(|s| s.fence())))?;
        self.forget_all();
        self.finish_pending()?;
        self.adopt_layout()?;
        self.resolve();
        if self.in_doubt() {
            return Err(cluster_error("a change left half made cannot be finished: a shard does not answer"));
        }
        self.recover_ordinals()?;
        group.settled.store(term, Ordering::SeqCst);
        self.record()
    }

    /// Note, before a change is made on the shards, what the control node is to write if it is.
    pub(crate) fn note_pending(&self, txid: u64, primary: usize, ops: &[LogOp]) -> KResult<()> {
        let note = bytes(&(txid, primary as u32, ops))?;
        self.mirror.connect_internal().set_var_direct(PENDING, note)
    }

    /// The name of the variable that `note_pending` writes.
    pub(crate) fn pending_name() -> kixdb::sym::SymId {
        intern(PENDING)
    }

    /// Write what a change that was left noted calls for: the primary shard of the change says whether it was made.
    pub(crate) fn finish_pending(&self) -> KResult<()> {
        let Some((txid, primary, mut ops)) = self.held::<(u64, u32, Vec<LogOp>)>(PENDING)? else { return Ok(()) };
        let TxnReply::Outcome { committed } = self.wait(self.shard(primary as usize)?.txn(&TxnReq::Abort { txid }))? else {
            return Err(cluster_error("a shard did not say whether a change was made"));
        };
        if !committed {
            ops.clear();
        }
        ops.push(LogOp::DropVar { name: Self::pending_name() });
        self.mirror.connect_internal().replay(ops)
    }

    /// What the master answers to `what`, if another control node is the master. `None`: this node answers itself.
    pub(crate) fn pass_on(&self, namespace: String, what: impl FnOnce() -> Forwarded) -> Option<KResult<ForwardReply>> {
        let group = self.group.get()?;
        let deadline = Instant::now() + NO_MASTER;
        let master = loop {
            if group.term().is_some() {
                return None;
            }
            // (the node taken to be the master is first asked whether it is, which is safe to ask of one that has just
            // died: a statement sent to such a node could not safely be sent again)
            if let Some(client) = self.known_master(group) {
                if self.wait(client.status()).is_ok_and(|s| s.role == "leader") {
                    break client;
                }
            }
            if Instant::now() >= deadline || group.halted.load(Ordering::SeqCst) {
                return Some(Err(cluster_error("no control node is the master (a majority of them must be up)")));
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        let answer = self.wait(master.forward(&ForwardReq { namespace, what: what() }));
        Some(answer.map_err(|e| cluster_error(format!("the master did not answer; {}", e.detail.replacen("shard 0", "control node", 1)))))
    }

    /// A connection to the node this one takes to be the master, if it knows one and can reach it.
    fn known_master(&self, group: &Group) -> Option<Arc<ShardClient>> {
        let address = group.replica.leader_addr().filter(|_| !group.replica.is_cut_off())?;
        self.client_of(group, address).ok()
    }

    /// Run `read` under a permit of the master's, if another node is the master: with the mirror as new as the master's
    /// was when it gave the permit, the shards told for which term, and the permit renewed for as long as it takes.
    /// `None`: it could not be done so, or the permit may have lapsed meanwhile, and what was read is not to be used.
    pub(crate) fn with_permit<T>(&self, read: impl FnOnce() -> T) -> Option<T> {
        let group = self.group.get().filter(|g| g.term().is_none())?;
        let master = self.known_master(group)?;
        let asked = Instant::now();
        let given = self.wait(master.permit(&PermitReq::Take)).ok()?;
        let permit = given.permit;
        // when the latest request that kept the permit was sent: it lasts at least its limit from then
        let kept = Mutex::new(asked);
        let value = loop {
            // all that this node asks of shards at one time carries one term, which changes only while nothing does
            let shared = self.gate.read().unwrap_or_else(|e| e.into_inner());
            if self.term.load(Ordering::SeqCst) != permit.term {
                drop(shared);
                let _alone = self.gate.write().unwrap_or_else(|e| e.into_inner());
                if self.term.fetch_max(permit.term, Ordering::SeqCst) > permit.term {
                    break None;
                }
                continue;
            }
            while self.mirror.applied_ts() < given.position && asked.elapsed() < PERMIT_LIMIT / 2 {
                std::thread::sleep(Duration::from_millis(1));
            }
            if self.mirror.applied_ts() < given.position {
                break None;
            }
            self.follow();
            break std::thread::scope(|scope| {
                let (done, finished) = mpsc::channel::<()>();
                let (kept, master) = (&kept, &master);
                scope.spawn(move || {
                    while finished.recv_timeout(PERMIT_LIMIT / 3) == Err(mpsc::RecvTimeoutError::Timeout) {
                        let sent = Instant::now();
                        if self.wait(master.permit(&PermitReq::Renew(permit))).is_err() {
                            break;
                        }
                        *kept.lock().unwrap_or_else(|e| e.into_inner()) = sent;
                    }
                });
                let value = read();
                let lasted = kept.lock().unwrap_or_else(|e| e.into_inner()).elapsed() < PERMIT_LIMIT;
                drop(done);
                lasted.then_some(value)
            });
        };
        // (without waiting: a permit not released lapses)
        self.runtime.spawn(async move {
            let _ = master.permit(&PermitReq::Release(permit)).await;
        });
        value
    }

    /// The master's side of a permit to read: the shared side of the cluster's lock, held by a thread of its own until
    /// the permit is released or goes `PERMIT_LIMIT` without being renewed.
    fn permit(self: Arc<Self>, req: PermitReq) -> KResult<PermitReply> {
        let group = self.group.get().ok_or_else(not_master)?;
        let gone = || cluster_error("the permit to read has lapsed");
        match req {
            PermitReq::Take => {
                self.settle()?;
                let permit = Permit { term: group.term().ok_or_else(not_master)?, id: group.next_permit.fetch_add(1, Ordering::SeqCst) };
                let ((keep, renewed), (given, position)) = (mpsc::channel(), mpsc::channel());
                group.permits.lock().unwrap_or_else(|e| e.into_inner()).insert(permit.id, keep);
                let inner = self.clone();
                std::thread::spawn(move || {
                    let _shared = inner.gate.read().unwrap_or_else(|e| e.into_inner());
                    let _ = given.send(inner.mirror.applied_ts());
                    while renewed.recv_timeout(PERMIT_LIMIT).is_ok() {}
                    if let Some(group) = inner.group.get() {
                        group.permits.lock().unwrap_or_else(|e| e.into_inner()).remove(&permit.id);
                    }
                });
                Ok(PermitReply { permit, position: position.recv().map_err(|_| gone())? })
            }
            PermitReq::Renew(permit) | PermitReq::Release(permit) => {
                // (a permit of another term is none of this master's)
                if group.term() != Some(permit.term) {
                    return Err(gone());
                }
                let mut permits = group.permits.lock().unwrap_or_else(|e| e.into_inner());
                match req {
                    PermitReq::Release(_) => drop(permits.remove(&permit.id)),
                    _ => permits.get(&permit.id).and_then(|keep| keep.send(()).ok()).ok_or_else(gone)?,
                }
                Ok(PermitReply { permit, position: 0 })
            }
        }
    }

    /// The connection to the control node at `address`, made once while it is the one asked for.
    fn client_of(&self, group: &Group, address: String) -> KResult<Arc<ShardClient>> {
        let mut held = group.master.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((_, client)) = held.as_ref().filter(|h| h.0 == address) {
            return Ok(client.clone());
        }
        let _in_runtime = self.runtime.enter();
        let client = Arc::new(ShardClient::new(0, &address, self.tls.clone())?);
        client.set_limit(self.request_limit());
        *held = Some((address, client.clone()));
        Ok(client)
    }

    /// Answer another control node: run what it passed on, or see to a permit of its to read.
    fn answer(self: Arc<Self>, kind: &str, body: &[u8]) -> Result<Vec<u8>, Status> {
        if self.group.get().is_some_and(|g| g.replica.is_cut_off()) {
            return Err(Status::unavailable("the control node cannot be reached"));
        }
        if let Some(group) = self.group.get().filter(|g| g.term().is_none()) {
            let master = group.replica.leader_addr().unwrap_or_default();
            return Err(Status::unavailable(format!("{}{master}", wire::NOT_LEADER_PREFIX)));
        }
        let bad = |e: postcard::Error| Status::invalid_argument(format!("bad request: {e}"));
        let reply = match kind {
            wire::ACTION_PERMIT => {
                let req = postcard::from_bytes(body).map_err(bad)?;
                postcard::to_allocvec(&self.permit(req).map_err(|e| wire::status_of(&e))?)
            }
            _ => {
                let req: ForwardReq = postcard::from_bytes(body).map_err(bad)?;
                postcard::to_allocvec(&ClusterSession::new(self).run_passed_on(req))
            }
        };
        reply.map_err(|e| Status::internal(e.to_string()))
    }

    /// The members of the group of control nodes, after removing `remove`, and which of them is the master.
    pub(crate) fn controls(&self, remove: Option<u64>) -> KResult<(Vec<MemberInfo>, u64)> {
        self.settle()?;
        let group = self.group.get().ok_or_else(|| cluster_error("this control node is in no group"))?;
        if let Some(id) = remove {
            group.replica.remove_replica(id).map_err(cluster_error)?;
        }
        Ok((group.replica.members(), group.replica.node_id))
    }
}
