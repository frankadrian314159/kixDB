//! A replicated shard: several nodes hold the same workspace and elect a leader (Raft). A change on the leader takes
//! effect only once a majority of the replicas have its record.
//!
//! The unit of replication is the engine's own redo record (`WalRecord`). The leader's engine hands each record to this
//! replica (`Replicator::propose`) before it makes the change, and goes on when the group has committed it; the other
//! replicas apply the records of the log to their engines in order (`Server::apply_replicated`). Nothing is re-executed,
//! so replicas cannot diverge.
//!
//! Two threads run the replica. The protocol thread never touches the engine, because a proposing statement holds the
//! engine's lock while it waits for that thread to report the commit; the applier thread does everything that needs the
//! engine, including **snapshots** (every `snapshot_every` entries the engine's state is exported and the log cut; a
//! replica that fell behind the log is sent the snapshot). **Membership** can change while serving: a new replica joins
//! as a learner, is caught up, then promoted.
//!
//! The leader serves requests only while a majority has recently heard from it and it has applied everything committed.
use crate::transport::{self, AddressBook};
use kix_raft::{Conf, Config, Entry, FileStorage, Member, Msg, NodeId, Raft, Role, Snapshot};
use kix_shared_ces::wire::MemberInfo;
use kixdb::persist::{Replicator, WalRecord};
use kixdb::{KError, KResult, Server};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// A Raft message on the wire.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Envelope {
    pub from: NodeId,
    pub msg: Msg,
}

#[derive(Clone, Debug)]
pub struct ReplicaConfig {
    pub node_id: NodeId,
    /// The other replicas: id and `host:port` of their Flight service.
    pub peers: Vec<(NodeId, String)>,
    /// Where this replica keeps its Raft log (the engine's data directory is given separately).
    pub dir: PathBuf,
    pub tick_ms: u64,
    pub election_ticks: u32,
    pub heartbeat_ticks: u32,
    /// Force the Raft log to disk at every write (tests may turn it off).
    pub sync: bool,
    /// This replica's own `host:port`, which membership changes give the others.
    pub addr: String,
    /// Join an existing group: take no part in elections until the group has added this replica.
    pub join: bool,
    /// Take a snapshot and cut the log after this many applied entries (0: never).
    pub snapshot_every: u64,
    /// How long a change waits for a majority.
    pub commit_timeout: Duration,
}

impl ReplicaConfig {
    pub fn new(node_id: NodeId, peers: Vec<(NodeId, String)>, dir: impl Into<PathBuf>) -> Self {
        Self {
            node_id,
            peers,
            dir: dir.into(),
            tick_ms: 20,
            election_ticks: 10,
            heartbeat_ticks: 2,
            sync: true,
            addr: String::new(),
            join: false,
            snapshot_every: 5000,
            commit_timeout: Duration::from_secs(10),
        }
    }
}

enum Apply {
    /// An entry, and whether this node's own engine proposed it and so makes the change itself.
    Entry(Entry, bool),
    Snapshot(Snapshot),
}

#[derive(Default)]
struct Shared {
    /// The proposal a statement is waiting on: `(index, term)`.
    waiting: Option<(u64, u64)>,
    outcome: Option<bool>,
    /// A proposal given up on whose fate is not known: nothing else is proposed until it has been applied or replaced.
    unresolved: u64,
    applied_index: u64,
}

pub struct Replica {
    pub node_id: NodeId,
    raft: Mutex<Raft<FileStorage>>,
    shared: Mutex<Shared>,
    cv: Condvar,
    /// Messages from peers, for the protocol thread (`None` wakes it).
    inbox: mpsc::Sender<Option<(NodeId, Msg)>>,
    apply: Mutex<Option<mpsc::Sender<Apply>>>,
    outbox: tokio::sync::mpsc::UnboundedSender<(NodeId, Msg)>,
    server: Server,
    book: AddressBook,
    addr: String,
    commit_timeout: Duration,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Replica {
    /// Open the Raft log, make `server` a replica of the group and run the protocol. Must be called inside a tokio runtime
    /// (the transport uses it).
    pub fn start(cfg: ReplicaConfig, server: Server) -> std::io::Result<Arc<Replica>> {
        let storage = FileStorage::open(&cfg.dir, cfg.sync).map_err(std::io::Error::other)?;
        let raft_cfg = Config {
            id: cfg.node_id,
            peers: if cfg.join { vec![] } else { cfg.peers.iter().map(|p| p.0).collect() },
            election_ticks: cfg.election_ticks,
            heartbeat_ticks: cfg.heartbeat_ticks,
            seed: cfg.node_id.wrapping_mul(2_654_435_761) ^ std::process::id() as u64,
            join: cfg.join,
        };
        let raft = Raft::new(raft_cfg, storage);
        let (otx, orx) = tokio::sync::mpsc::unbounded_channel();
        let (itx, irx) = mpsc::channel();
        let (atx, arx) = mpsc::channel();
        let book: AddressBook = Arc::new(Mutex::new(cfg.peers.iter().cloned().collect()));
        let replica = Arc::new(Replica {
            node_id: cfg.node_id,
            raft: Mutex::new(raft),
            shared: Mutex::default(),
            cv: Condvar::new(),
            inbox: itx,
            apply: Mutex::new(Some(atx)),
            outbox: otx,
            server: server.clone(),
            book: book.clone(),
            addr: cfg.addr.clone(),
            commit_timeout: cfg.commit_timeout,
        });
        server.set_replicator(Some(Arc::new(Handle(replica.clone()))));
        transport::spawn_sender(cfg.node_id, book, orx);
        let (me, tick) = (replica.clone(), Duration::from_millis(cfg.tick_ms.max(1)));
        std::thread::Builder::new().name(format!("raft-{}", cfg.node_id)).spawn(move || me.run(irx, tick))?;
        let (me, every) = (replica.clone(), cfg.snapshot_every);
        std::thread::Builder::new().name(format!("raft-apply-{}", cfg.node_id)).spawn(move || me.applier(arx, every))?;
        Ok(replica)
    }

    /// Stop taking part in the group.
    pub fn stop(&self) {
        lock(&self.apply).take();
        let _ = self.inbox.send(None);
        self.cv.notify_all();
        self.server.set_replicator(None);
    }

    fn stopped(&self) -> bool {
        lock(&self.apply).is_none()
    }

    /// A message from a peer.
    pub fn deliver(&self, from: NodeId, msg: Msg) {
        let _ = self.inbox.send(Some((from, msg)));
    }

    pub fn role(&self) -> (Role, u64) {
        let r = lock(&self.raft);
        (r.role, r.term)
    }

    /// The address of the leader this replica knows of, if it is another node.
    pub fn leader_addr(&self) -> Option<String> {
        let leader = lock(&self.raft).leader?;
        (leader != self.node_id).then(|| lock(&self.book).get(&leader).cloned()).flatten()
    }

    /// The members of the group as this replica knows them.
    pub fn members(&self) -> Vec<MemberInfo> {
        let conf = self.conf_with_addrs(&lock(&self.raft));
        let info = |voter| move |m: &Member| (m.id, m.addr.clone(), voter);
        conf.voters.iter().map(info(true)).chain(conf.learners.iter().map(info(false))).collect()
    }

    /// The index of the last snapshot (0: none).
    pub fn snapshot_index(&self) -> u64 {
        lock(&self.raft).snapshot_index()
    }

    /// Whether requests can be served: this is the leader, a majority has answered it recently, and it has applied
    /// everything the group has committed, including an entry of its own term.
    pub fn can_serve(&self) -> bool {
        let r = lock(&self.raft);
        let commit = r.commit_index;
        if r.role != Role::Leader || !r.has_lease() || r.term_at(commit) != Some(r.term) {
            return false;
        }
        drop(r);
        lock(&self.shared).applied_index >= commit
    }

    /// Take what Raft has to send and to apply (under the raft lock, so the order of the entries is kept).
    fn pump(&self, raft: &mut Raft<FileStorage>) {
        let ready = raft.ready();
        {
            // the transport reaches members through this book
            let mut book = lock(&self.book);
            for m in raft.conf.voters.iter().chain(&raft.conf.learners) {
                if !m.addr.is_empty() && m.id != self.node_id {
                    book.insert(m.id, m.addr.clone());
                }
            }
        }
        for m in ready.messages {
            let _ = self.outbox.send(m);
        }
        let Some(apply) = lock(&self.apply).clone() else { return };
        if let Some(s) = ready.snapshot {
            let _ = apply.send(Apply::Snapshot(s));
        }
        for e in ready.committed {
            let mut sh = lock(&self.shared);
            let mine = match sh.waiting {
                Some((index, term)) if index == e.index => {
                    sh.waiting = None;
                    sh.outcome = Some(term == e.term);
                    term == e.term
                }
                _ => false,
            };
            drop(sh);
            self.cv.notify_all();
            let _ = apply.send(Apply::Entry(e, mine));
        }
    }

    fn run(self: Arc<Self>, inbox: mpsc::Receiver<Option<(NodeId, Msg)>>, tick: Duration) {
        let mut last_tick = Instant::now();
        while !self.stopped() {
            let first = inbox.recv_timeout(tick.saturating_sub(last_tick.elapsed()));
            let mut raft = lock(&self.raft);
            for (from, msg) in first.ok().into_iter().chain(inbox.try_iter()).flatten() {
                raft.step(from, msg);
            }
            // at most one tick per turn: a thread that was held up must not make up the lost time at once
            if last_tick.elapsed() >= tick {
                last_tick = Instant::now();
                raft.tick();
            }
            self.pump(&mut raft);
        }
    }

    fn applier(self: Arc<Self>, entries: mpsc::Receiver<Apply>, snapshot_every: u64) {
        while let Ok(item) = entries.recv() {
            let index = match item {
                Apply::Snapshot(s) => {
                    if let Err(e) = self.server.install_state(&s.data) {
                        eprintln!("replica {}: installing the snapshot of index {} failed: {e}", self.node_id, s.index);
                    }
                    s.index
                }
                Apply::Entry(e, mine) => {
                    if !mine && !e.data.is_empty() {
                        let applied = postcard::from_bytes::<WalRecord>(&e.data)
                            .map_err(|err| KError::io(err.to_string()))
                            .and_then(|rec| self.server.apply_replicated(&rec));
                        if let Err(err) = applied {
                            eprintln!("replica {}: applying the record at index {} failed: {err}", self.node_id, e.index);
                        }
                    }
                    e.index
                }
            };
            lock(&self.shared).applied_index = index;
            self.cv.notify_all();
            if snapshot_every > 0 && index >= self.snapshot_index() + snapshot_every {
                // the export waits for statements in progress, so the state holds everything up to `index`
                match self.server.export_state() {
                    Ok(data) => lock(&self.raft).compact(index, data),
                    Err(e) => eprintln!("replica {}: snapshot failed: {e}", self.node_id),
                }
            }
        }
    }

    /// The configuration, with every member's address filled in (a group started from the command line has none in its
    /// log until its first change).
    fn conf_with_addrs(&self, raft: &Raft<FileStorage>) -> Conf {
        let mut c = raft.conf.clone();
        let book = lock(&self.book);
        for m in c.voters.iter_mut().chain(c.learners.iter_mut()) {
            if m.addr.is_empty() {
                m.addr = if m.id == self.node_id { self.addr.clone() } else { book.get(&m.id).cloned().unwrap_or_default() };
            }
        }
        c
    }

    fn not_leader(raft: &Raft<FileStorage>) -> String {
        match raft.leader {
            Some(l) if l != raft.id => format!("this replica is not the leader (node {l} is)"),
            _ => "this replica is not the leader".into(),
        }
    }

    /// Change the membership by one step and wait until the group has committed it. Leader only.
    fn change_conf(&self, deadline: Instant, change: impl Fn(Conf) -> Conf) -> Result<(), String> {
        let mut proposed = None;
        loop {
            {
                let mut raft = lock(&self.raft);
                match proposed {
                    None if raft.role != Role::Leader => return Err(Self::not_leader(&raft)),
                    None if !raft.has_pending_conf() => {
                        let conf = change(self.conf_with_addrs(&raft));
                        proposed = Some(raft.propose_conf(conf).map_err(|_| Self::not_leader(&raft))?);
                        self.pump(&mut raft);
                    }
                    Some(index) if raft.commit_index >= index => return Ok(()),
                    _ => {}
                }
            }
            if Instant::now() >= deadline {
                return Err("timed out waiting for the group to commit the membership change".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Add the replica `id` (a `kix-shard --join` that is up at `addr`): it joins as a learner, is caught up, and is made
    /// a voter. Leader only.
    pub fn add_replica(&self, id: NodeId, addr: &str) -> Result<(), String> {
        if self.addr.is_empty() {
            return Err("this replica was not told its own address".into());
        }
        let deadline = Instant::now() + self.commit_timeout;
        lock(&self.book).insert(id, addr.to_string());
        let member = Member { id, addr: addr.to_string() };
        self.change_conf(deadline, |c| c.with_learner(member.clone()))?;
        let target = lock(&self.raft).commit_index;
        loop {
            let caught_up = lock(&self.raft).matched(id) >= target;
            if caught_up {
                break;
            }
            if Instant::now() >= deadline {
                return Err(format!("replica {id} did not catch up in time (it stays a learner)"));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        self.change_conf(deadline, |c| c.with_promoted(id))
    }

    /// Remove the replica `id`; the leader may remove itself, and then hands over.
    pub fn remove_replica(&self, id: NodeId) -> Result<(), String> {
        if !self.members().iter().any(|m| m.0 == id) {
            return Ok(());
        }
        self.change_conf(Instant::now() + self.commit_timeout, |c| c.without(id))
    }
}

/// What the engine calls before it makes a change.
struct Handle(Arc<Replica>);

impl Replicator for Handle {
    fn propose(&self, rec: &WalRecord) -> KResult<()> {
        let r = &self.0;
        let fail = |m: String| KError::new("cluster", m);
        let data = postcard::to_allocvec(rec).map_err(|e| fail(e.to_string()))?;
        {
            let mut raft = lock(&r.raft);
            {
                let sh = lock(&r.shared);
                if sh.unresolved > sh.applied_index {
                    return Err(fail("an earlier change is still waiting for a majority of the replicas".into()));
                }
            }
            if raft.role != Role::Leader {
                return Err(fail(Replica::not_leader(&raft)));
            }
            let term = raft.term;
            // registered before the entry can be reported committed: that needs the raft lock, which is held
            let index = raft.last_index() + 1;
            let mut sh = lock(&r.shared);
            sh.waiting = Some((index, term));
            sh.outcome = None;
            drop(sh);
            raft.propose(data).map_err(|_| fail(Replica::not_leader(&raft)))?;
            r.pump(&mut raft);
        }
        let _ = r.inbox.send(None);
        let deadline = Instant::now() + r.commit_timeout;
        let mut sh = lock(&r.shared);
        loop {
            if let Some(ok) = sh.outcome.take() {
                return if ok { Ok(()) } else { Err(fail("the change was replaced by another leader's".into())) };
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() || r.stopped() {
                // if the group commits it later, this node applies it like any other record
                if let Some((index, _)) = sh.waiting.take() {
                    sh.unresolved = index;
                }
                return Err(fail("timed out waiting for a majority of the replicas".into()));
            }
            sh = r.cv.wait_timeout(sh, left.min(Duration::from_millis(100))).unwrap_or_else(|e| e.into_inner()).0;
        }
    }
}

/// Role names for status reports.
pub fn role_name(role: Role) -> &'static str {
    match role {
        Role::Follower => "follower",
        Role::Candidate => "candidate",
        Role::Leader => "leader",
    }
}
