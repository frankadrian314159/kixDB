//! Pure state-machine Raft consensus core (`Raft<S>`), modeled on `../tsql-server/crates/parallel/raft`:
//! - Randomized leader election with pre-disruption lease check
//! - Log replication with fast conflict back-off and current-term majority commitment, one batch at a time to each replica
//! - Leader read lease (`has_lease`)
//! - Log compaction & snapshot installation (`compact` / `Msg::Snapshot`), one copy at a time too
//! - Single-server learner-to-voter membership changes
pub mod storage;

pub use storage::{FileStorage, HardState, MemStorage, Snapshot, Storage};

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

pub type NodeId = u64;

/// A batch of entries takes no further entry once it holds this many bytes.
pub const MAX_BATCH: usize = 4 << 20;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Member {
    pub id: NodeId,
    pub addr: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Conf {
    pub voters: Vec<Member>,
    pub learners: Vec<Member>,
}

impl Conf {
    pub fn is_voter(&self, id: NodeId) -> bool {
        self.voters.iter().any(|m| m.id == id)
    }

    pub fn is_learner(&self, id: NodeId) -> bool {
        self.learners.iter().any(|m| m.id == id)
    }

    pub fn voter_ids(&self) -> Vec<NodeId> {
        self.voters.iter().map(|m| m.id).collect()
    }

    pub fn ids(&self) -> Vec<NodeId> {
        self.voters.iter().chain(self.learners.iter()).map(|m| m.id).collect()
    }

    pub fn addr_of(&self, id: NodeId) -> Option<&str> {
        self.voters.iter().chain(self.learners.iter()).find(|m| m.id == id).map(|m| m.addr.as_str())
    }

    pub fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    pub fn with_learner(&self, m: Member) -> Conf {
        let mut c = self.clone();
        if !c.is_voter(m.id) && !c.is_learner(m.id) {
            c.learners.push(m);
        }
        c
    }

    pub fn with_promoted(&self, id: NodeId) -> Conf {
        let mut c = self.clone();
        if let Some(pos) = c.learners.iter().position(|m| m.id == id) {
            let m = c.learners.remove(pos);
            c.voters.push(m);
        }
        c
    }

    pub fn without(&self, id: NodeId) -> Conf {
        let mut c = self.clone();
        c.voters.retain(|m| m.id != id);
        c.learners.retain(|m| m.id != id);
        c
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub term: u64,
    pub index: u64,
    pub data: Vec<u8>,
    pub conf: Option<Conf>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Msg {
    Vote { term: u64, last_index: u64, last_term: u64 },
    VoteReply { term: u64, granted: bool },
    Append {
        term: u64,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<Entry>,
        commit: u64,
    },
    AppendReply {
        term: u64,
        success: bool,
        match_index: u64,
    },
    Snapshot {
        term: u64,
        snap: Snapshot,
    },
    SnapshotReply {
        term: u64,
        index: u64,
    },
}

impl Msg {
    /// For a message that carries state (a snapshot, or entries): the last index it carries. The transport reports what
    /// became of every such message with `Raft::sent`.
    pub fn carries(&self) -> Option<u64> {
        match self {
            Msg::Snapshot { snap, .. } => Some(snap.index),
            Msg::Append { entries, .. } => entries.last().map(|e| e.index),
            _ => None,
        }
    }
}

/// A snapshot or batch of entries on its way to a replica.
#[derive(Debug)]
struct Sending {
    /// The last index it carries.
    upto: u64,
    /// `None` while the transport has it, then the ticks left before another may be sent.
    wait: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub election_ticks: u32,
    pub heartbeat_ticks: u32,
    pub seed: u64,
    pub join: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub struct NotLeader {
    pub leader: Option<NodeId>,
}

#[derive(Default)]
pub struct Ready {
    pub messages: Vec<(NodeId, Msg)>,
    pub committed: Vec<Entry>,
    pub snapshot: Option<Snapshot>,
}

pub struct Raft<S: Storage> {
    pub id: NodeId,
    pub role: Role,
    pub term: u64,
    pub voted_for: Option<NodeId>,
    pub leader: Option<NodeId>,
    pub conf: Conf,
    storage: S,
    snap_index: u64,
    snap_term: u64,
    /// The entry the log starts after: the snapshot's, or one a little before it.
    base_index: u64,
    base_term: u64,
    log: Vec<Entry>,
    /// How many entries a cut keeps behind its snapshot (within a batch's size), for replicas a little way behind.
    pub trailing: u64,
    pub commit_index: u64,
    applied_to_ready: u64,
    election_ticks: u32,
    heartbeat_ticks: u32,
    ticks_since_leader: u32,
    heartbeat_elapsed: u32,
    randomized_timeout: u32,
    rng_state: u64,
    votes: HashSet<NodeId>,
    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,
    recent_ack: HashSet<NodeId>,
    /// A majority answered during the last complete election window (or elected us).
    lease_held: bool,
    /// This node should not lead for now.
    unfit: bool,
    outbox: Vec<(NodeId, Msg)>,
    pending_snap: Option<Snapshot>,
    sending: HashMap<NodeId, Sending>,
}

impl<S: Storage> Raft<S> {
    pub fn new(cfg: Config, mut storage: S) -> Self {
        let (hs, mut entries, snap) = storage.load();
        let (snap_index, snap_term, mut conf) = match &snap {
            Some(sn) => (sn.index, sn.term, sn.conf.clone()),
            None => {
                let mut voters = Vec::new();
                if !cfg.join {
                    let mut all = vec![cfg.id];
                    for p in &cfg.peers {
                        if !all.contains(p) {
                            all.push(*p);
                        }
                    }
                    all.sort_unstable();
                    for nid in all {
                        voters.push(Member { id: nid, addr: String::new() });
                    }
                }
                (0, 0, Conf { voters, learners: vec![] })
            }
        };
        // entries from before the snapshot are of use only if they lead up to it (the first marks where the log starts)
        if !entries.iter().any(|e| e.index == snap_index && e.term == snap_term) {
            entries.retain(|e| e.index > snap_index);
        }
        let (base_index, base_term) = match entries.first() {
            Some(first) if first.index <= snap_index => {
                let base = (first.index, first.term);
                entries.remove(0);
                base
            }
            _ => (snap_index, snap_term),
        };
        for e in entries.iter().filter(|e| e.index > snap_index) {
            if let Some(c) = &e.conf {
                conf = c.clone();
            }
        }
        let mut r = Self {
            id: cfg.id,
            role: Role::Follower,
            term: hs.term,
            voted_for: hs.voted_for,
            leader: None,
            conf,
            storage,
            snap_index,
            snap_term,
            base_index,
            base_term,
            log: entries,
            trailing: 0,
            commit_index: snap_index,
            applied_to_ready: snap_index,
            election_ticks: cfg.election_ticks.max(4),
            heartbeat_ticks: cfg.heartbeat_ticks.max(1),
            ticks_since_leader: 0,
            heartbeat_elapsed: 0,
            randomized_timeout: cfg.election_ticks.max(4),
            rng_state: cfg.seed ^ cfg.id.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 1,
            votes: HashSet::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            recent_ack: HashSet::new(),
            lease_held: false,
            unfit: false,
            outbox: Vec::new(),
            pending_snap: None,
            sending: HashMap::new(),
        };
        r.reset_election_timeout();
        // Single-voter group elects itself immediately on first tick
        r
    }

    fn next_rand(&mut self) -> u32 {
        let mut x = self.rng_state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng_state = x;
        (x >> 16) as u32
    }

    fn reset_election_timeout(&mut self) {
        let span = self.election_ticks;
        self.randomized_timeout = span + (self.next_rand() % span);
        self.ticks_since_leader = 0;
    }

    pub fn last_index(&self) -> u64 {
        self.log.last().map(|e| e.index).unwrap_or(self.base_index)
    }

    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        if index == self.base_index {
            return Some(self.base_term);
        }
        self.entry(index).map(|e| e.term)
    }

    pub fn last_term(&self) -> u64 {
        self.term_at(self.last_index()).unwrap_or(0)
    }

    pub fn has_lease(&self) -> bool {
        if self.role != Role::Leader {
            return false;
        }
        if self.conf.voters.len() <= 1 && self.conf.is_voter(self.id) {
            return true;
        }
        self.lease_held || self.acked_voters() >= self.conf.quorum()
    }

    fn acked_voters(&self) -> usize {
        self.conf.voters.iter().filter(|m| m.id == self.id || self.recent_ack.contains(&m.id)).count()
    }

    /// Whether a membership change is appended but not yet committed (the next one must wait for it).
    pub fn has_pending_conf(&self) -> bool {
        self.log.iter().any(|e| e.index > self.commit_index && e.conf.is_some())
    }

    /// How far the log of the replica `id` is known to match (leader only).
    pub fn matched(&self, id: NodeId) -> u64 {
        self.match_index.get(&id).copied().unwrap_or(0)
    }

    pub fn snapshot_index(&self) -> u64 {
        self.snap_index
    }

    /// The log entry at `index`, if it has not been compacted away.
    pub fn entry(&self, index: u64) -> Option<&Entry> {
        self.log.get(index.checked_sub(self.base_index + 1)? as usize)
    }

    /// While a node is unfit it hands over any leadership and calls no election; it still votes and takes the log.
    pub fn set_unfit(&mut self, unfit: bool) {
        self.unfit = unfit;
        if unfit && self.role != Role::Follower {
            self.become_follower(self.term, None);
        }
    }

    /// The size of the entries the latest snapshot does not stand for.
    pub fn bytes_since_snapshot(&self) -> u64 {
        self.log.iter().rev().take_while(|e| e.index > self.snap_index).map(|e| e.data.len() as u64).sum()
    }

    /// The transport's report on a message that `carries` state up to `upto`: delivered, or given up on. Every one must
    /// be reported: nothing more is sent to that replica until then, nor, without its reply, for a while after.
    pub fn sent(&mut self, peer: NodeId, upto: u64, delivered: bool) {
        let ticks = self.election_ticks * if delivered { 10 } else { 1 };
        if let Some(s) = self.sending.get_mut(&peer).filter(|s| s.upto == upto) {
            s.wait = Some(ticks);
        }
    }

    pub fn tick(&mut self) {
        self.sending.retain(|_, s| {
            s.wait = s.wait.map(|t| t.saturating_sub(1));
            s.wait != Some(0)
        });
        match self.role {
            Role::Leader => {
                self.ticks_since_leader += 1;
                if self.ticks_since_leader >= self.election_ticks {
                    // Check lease renewal; if isolated from majority for a full election window, step down!
                    if self.acked_voters() < self.conf.quorum() {
                        self.become_follower(self.term, None);
                        return;
                    }
                    self.lease_held = true;
                    self.recent_ack.clear();
                    self.recent_ack.insert(self.id);
                    self.ticks_since_leader = 0;
                }
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.heartbeat_ticks {
                    self.heartbeat_elapsed = 0;
                    self.broadcast_append();
                }
            }
            Role::Follower | Role::Candidate => {
                if self.unfit || self.conf.voters.is_empty() || !self.conf.is_voter(self.id) {
                    return;
                }
                if self.conf.voters.len() == 1 && self.role != Role::Leader {
                    self.start_election();
                    return;
                }
                self.ticks_since_leader += 1;
                if self.ticks_since_leader >= self.randomized_timeout {
                    self.start_election();
                }
            }
        }
    }

    fn persist_hs(&mut self) {
        self.storage.save_hard_state(&HardState {
            term: self.term,
            voted_for: self.voted_for,
        });
    }

    fn become_follower(&mut self, term: u64, leader: Option<NodeId>) {
        let changed = term != self.term;
        self.lease_held = false;
        if term > self.term {
            self.term = term;
            self.voted_for = None;
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.votes.clear();
        self.reset_election_timeout();
        if changed {
            self.persist_hs();
        }
    }

    fn start_election(&mut self) {
        self.term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.leader = None;
        self.votes.clear();
        self.votes.insert(self.id);
        self.reset_election_timeout();
        self.persist_hs();

        if self.votes.len() >= self.conf.quorum() {
            self.become_leader();
            return;
        }

        let li = self.last_index();
        let lt = self.last_term();
        for peer in self.conf.voter_ids() {
            if peer != self.id {
                self.outbox.push((
                    peer,
                    Msg::Vote {
                        term: self.term,
                        last_index: li,
                        last_term: lt,
                    },
                ));
            }
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader = Some(self.id);
        self.heartbeat_elapsed = 0;
        self.ticks_since_leader = 0;
        self.recent_ack.clear();
        self.recent_ack.insert(self.id);
        self.lease_held = true;
        self.sending.clear();

        let next = self.last_index() + 1;
        self.next_index.clear();
        self.match_index.clear();
        for id in self.conf.ids() {
            self.next_index.insert(id, next);
            self.match_index.insert(id, if id == self.id { self.last_index() } else { 0 });
        }

        // Append empty no-op entry in new term (Raft §5.4.2)
        let noop = Entry {
            term: self.term,
            index: next,
            data: vec![],
            conf: None,
        };
        self.log.push(noop.clone());
        self.storage.append_entries(next, &[noop]);
        self.match_index.insert(self.id, next);
        self.advance_commit();
        self.broadcast_append();
    }

    pub fn propose(&mut self, data: Vec<u8>) -> Result<u64, NotLeader> {
        if self.role != Role::Leader {
            return Err(NotLeader { leader: self.leader });
        }
        let idx = self.last_index() + 1;
        let entry = Entry {
            term: self.term,
            index: idx,
            data,
            conf: None,
        };
        self.log.push(entry.clone());
        self.storage.append_entries(idx, &[entry]);
        self.match_index.insert(self.id, idx);
        self.advance_commit();
        self.broadcast_append();
        Ok(idx)
    }

    pub fn propose_conf(&mut self, new_conf: Conf) -> Result<u64, NotLeader> {
        if self.role != Role::Leader {
            return Err(NotLeader { leader: self.leader });
        }
        let idx = self.last_index() + 1;
        let entry = Entry {
            term: self.term,
            index: idx,
            data: vec![],
            conf: Some(new_conf.clone()),
        };
        self.conf = new_conf;
        for id in self.conf.ids() {
            self.next_index.entry(id).or_insert(idx);
            self.match_index.entry(id).or_insert(0);
        }
        self.log.push(entry.clone());
        self.storage.append_entries(idx, &[entry]);
        self.match_index.insert(self.id, idx);
        self.advance_commit();
        self.broadcast_append();
        Ok(idx)
    }

    pub fn compact(&mut self, index: u64, data: Vec<u8>) {
        if index <= self.snap_index || index > self.commit_index {
            return;
        }
        let Some(term) = self.term_at(index) else { return };
        let snap = Snapshot {
            index,
            term,
            conf: self.conf.clone(),
            data,
        };
        self.snap_index = index;
        self.snap_term = term;
        // the log now starts after `base`
        let (mut base, mut size) = (index, 0);
        while base > self.base_index + 1 && index - base < self.trailing && size < MAX_BATCH {
            size += self.entry(base).map_or(0, |e| e.data.len());
            base -= 1;
        }
        let mut kept = self.log.split_off((base - self.base_index - 1) as usize);
        (self.base_index, self.base_term) = (base, kept[0].term);
        // (storage is given the entry at `base` too: it is how a restart knows the term there)
        self.storage.save_snapshot(&snap, &kept);
        kept.remove(0);
        self.log = kept;
    }

    fn broadcast_append(&mut self) {
        let peers: Vec<NodeId> = self.conf.ids().into_iter().filter(|&id| id != self.id).collect();
        for peer in peers {
            self.send_append(peer);
        }
    }

    fn send_append(&mut self, peer: NodeId) {
        let next = self.next_index.get(&peer).copied().unwrap_or(self.last_index() + 1);
        let msg = if self.sending.contains_key(&peer) {
            // an empty append every log matches: it keeps the replica from calling an election while it waits
            Msg::Append { term: self.term, prev_index: 0, prev_term: 0, entries: vec![], commit: self.commit_index }
        } else if next <= self.base_index {
            let Some(snap) = self.storage.snapshot() else { return };
            Msg::Snapshot { term: self.term, snap }
        } else {
            let prev_index = next - 1;
            let prev_term = self.term_at(prev_index).unwrap_or(0);
            let offset = (next - self.base_index - 1) as usize;
            let mut size = 0;
            let fits = |e: &&Entry| {
                let room = size < MAX_BATCH;
                size += e.data.len();
                room
            };
            let entries = self.log.get(offset..).unwrap_or(&[]).iter().take_while(fits).cloned().collect();
            Msg::Append { term: self.term, prev_index, prev_term, entries, commit: self.commit_index }
        };
        if let Some(upto) = msg.carries() {
            self.sending.insert(peer, Sending { upto, wait: None });
        }
        self.outbox.push((peer, msg));
    }

    fn advance_commit(&mut self) {
        let last = self.last_index();
        for idx in ((self.commit_index + 1)..=last).rev() {
            if self.term_at(idx) != Some(self.term) {
                continue;
            }
            let replicated = self
                .conf
                .voters
                .iter()
                .filter(|m| m.id == self.id || self.match_index.get(&m.id).copied().unwrap_or(0) >= idx)
                .count();
            if replicated >= self.conf.quorum() {
                self.commit_index = idx;
                break;
            }
        }
        if self.role == Role::Leader && !self.conf.is_voter(self.id) && !self.has_pending_conf() {
            // tell the others the change is committed, then hand over
            self.broadcast_append();
            self.become_follower(self.term, None);
        }
    }

    pub fn step(&mut self, from: NodeId, msg: Msg) {
        match msg {
            Msg::Vote { term, last_index, last_term } => {
                // a node that is hearing from a live leader ignores elections, so that a removed or cut-off node cannot
                // disrupt the group
                let led = match self.role {
                    Role::Leader => self.has_lease(),
                    _ => self.leader.is_some_and(|l| l != from) && self.ticks_since_leader < self.election_ticks,
                };
                if led {
                    return;
                }
                if term > self.term {
                    self.become_follower(term, None);
                }
                let up_to_date = (last_term, last_index) >= (self.last_term(), self.last_index());
                let can_vote = term == self.term
                    && self.conf.is_voter(from)
                    && (self.voted_for.is_none() || self.voted_for == Some(from))
                    && up_to_date;
                if can_vote {
                    self.voted_for = Some(from);
                    self.reset_election_timeout();
                    self.persist_hs();
                }
                self.outbox.push((from, Msg::VoteReply { term: self.term, granted: can_vote }));
            }

            Msg::VoteReply { term, granted } => {
                if term > self.term {
                    self.become_follower(term, None);
                    return;
                }
                if self.role == Role::Candidate && term == self.term && granted && self.conf.is_voter(from) {
                    self.votes.insert(from);
                    if self.votes.len() >= self.conf.quorum() {
                        self.become_leader();
                    }
                }
            }

            Msg::Append { term, prev_index, prev_term, entries, commit } => {
                if term < self.term {
                    self.outbox.push((from, Msg::AppendReply { term: self.term, success: false, match_index: self.last_index() }));
                    return;
                }
                self.become_follower(term, Some(from));
                if prev_index < self.snap_index {
                    self.outbox.push((from, Msg::AppendReply { term: self.term, success: true, match_index: self.snap_index }));
                    return;
                }
                if self.term_at(prev_index) != Some(prev_term) {
                    self.outbox.push((from, Msg::AppendReply { term: self.term, success: false, match_index: self.last_index().min(prev_index.saturating_sub(1)) }));
                    return;
                }

                // Append new or conflicting entries
                let mut changed_from: Option<u64> = None;
                for e in &entries {
                    if self.term_at(e.index) != Some(e.term) {
                        self.log.retain(|x| x.index < e.index);
                        if changed_from.is_none() {
                            changed_from = Some(e.index);
                        }
                        if let Some(c) = &e.conf {
                            self.conf = c.clone();
                        }
                        self.log.push(e.clone());
                    }
                }
                if let Some(first_new) = changed_from {
                    let pos = (first_new - self.base_index - 1) as usize;
                    let tail = self.log[pos..].to_vec();
                    self.storage.append_entries(first_new, &tail);
                }

                let match_idx = entries.last().map(|e| e.index).unwrap_or(prev_index);
                // entries past `match_idx` may be left over from an old term: only what matched is known to be the leader's
                if commit.min(match_idx) > self.commit_index {
                    self.commit_index = commit.min(match_idx);
                }
                self.outbox.push((from, Msg::AppendReply { term: self.term, success: true, match_index: match_idx }));
            }

            Msg::AppendReply { term, success, match_index } => {
                if term > self.term {
                    self.become_follower(term, None);
                    return;
                }
                if self.role != Role::Leader || term != self.term {
                    return;
                }
                self.recent_ack.insert(from);
                if success {
                    if self.sending.get(&from).is_some_and(|s| match_index >= s.upto) {
                        self.sending.remove(&from);
                    }
                    let prev_m = self.match_index.get(&from).copied().unwrap_or(0);
                    if match_index > prev_m {
                        self.match_index.insert(from, match_index);
                        self.next_index.insert(from, match_index + 1);
                        let old_c = self.commit_index;
                        self.advance_commit();
                        if self.commit_index > old_c {
                            self.broadcast_append();
                        } else if self.role == Role::Leader {
                            // the next batch, if there is one, and the commit index this replica may have missed
                            self.send_append(from);
                        }
                    }
                } else {
                    let fallback = (match_index + 1).max(1);
                    // one that moves nothing is old news: what is on its way is left to arrive
                    if self.next_index.insert(from, fallback) != Some(fallback) {
                        self.sending.remove(&from);
                    }
                    self.send_append(from);
                }
            }

            Msg::Snapshot { term, snap } => {
                if term < self.term {
                    // tell a stale leader about the newer term
                    self.outbox.push((from, Msg::SnapshotReply { term: self.term, index: 0 }));
                    return;
                }
                self.become_follower(term, Some(from));
                if snap.index > self.commit_index {
                    self.log.retain(|e| e.index > snap.index);
                    self.snap_index = snap.index;
                    self.snap_term = snap.term;
                    (self.base_index, self.base_term) = (snap.index, snap.term);
                    self.conf = snap.conf.clone();
                    self.commit_index = snap.index;
                    self.applied_to_ready = snap.index;
                    self.storage.save_snapshot(&snap, &self.log);
                    self.pending_snap = Some(snap.clone());
                }
                // everything up to the commit index is the leader's, whether or not the snapshot was needed
                self.outbox.push((from, Msg::SnapshotReply { term: self.term, index: self.commit_index }));
            }

            Msg::SnapshotReply { term, index } => {
                if term > self.term {
                    self.become_follower(term, None);
                    return;
                }
                if self.role == Role::Leader && term == self.term {
                    self.recent_ack.insert(from);
                    self.sending.remove(&from);
                    self.match_index.insert(from, index);
                    self.next_index.insert(from, index + 1);
                    self.advance_commit();
                    if self.role == Role::Leader && index < self.last_index() {
                        self.send_append(from);
                    }
                }
            }
        }
    }

    pub fn ready(&mut self) -> Ready {
        let mut committed = Vec::new();
        if self.commit_index > self.applied_to_ready {
            for idx in (self.applied_to_ready + 1)..=self.commit_index {
                committed.extend(self.entry(idx).cloned());
            }
            self.applied_to_ready = self.commit_index;
        }
        Ready {
            messages: std::mem::take(&mut self.outbox),
            committed,
            snapshot: self.pending_snap.take(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_snapshot_at_a_time_goes_to_a_replica_that_is_behind() {
        let node = |id, peers| Raft::new(Config { id, peers, election_ticks: 8, heartbeat_ticks: 2, seed: id, join: false }, MemStorage::new());
        // a leader of three whose third replica has answered nothing, with the log cut past it
        let mut leader = node(1, vec![2, 3]);
        while leader.role != Role::Candidate {
            leader.tick();
        }
        let term = leader.term;
        leader.step(2, Msg::VoteReply { term, granted: true });
        let index = leader.propose(vec![7]).unwrap();
        let acknowledge = |leader: &mut Raft<MemStorage>| leader.step(2, Msg::AppendReply { term, success: true, match_index: index });
        acknowledge(&mut leader);
        leader.compact(index, b"state".to_vec());
        leader.sent(3, index - 1, false); // what went to it before the log was cut is lost
        leader.ready();
        // what the third replica is sent over some ticks: (snapshots, heartbeats)
        let run = |leader: &mut Raft<MemStorage>, ticks| {
            let (mut snapshots, mut heartbeats) = (0, 0);
            for _ in 0..ticks {
                acknowledge(leader);
                leader.tick();
                for (to, m) in leader.ready().messages {
                    match m {
                        Msg::Snapshot { .. } if to == 3 => snapshots += 1,
                        Msg::Append { prev_index: 0, .. } if to == 3 => heartbeats += 1,
                        _ => {}
                    }
                }
            }
            (snapshots, heartbeats)
        };
        // one copy is on its way, however long that takes
        let (snapshots, heartbeats) = run(&mut leader, 200);
        assert_eq!(snapshots, 1);
        assert!(heartbeats >= 90, "{heartbeats}");
        // one that was given up on is sent again after a pause; a report about something else changes nothing
        leader.sent(3, index + 1, false);
        assert_eq!(run(&mut leader, 20).0, 0);
        leader.sent(3, index, false);
        assert_eq!(run(&mut leader, 6).0, 0);
        assert_eq!(run(&mut leader, 6).0, 1);
        // so is one that was delivered but never answered
        leader.sent(3, index, true);
        assert_eq!(run(&mut leader, 70).0, 0);
        assert_eq!(run(&mut leader, 30).0, 1);
        // a replica that gets a heartbeat accepts it, and once its reply arrives it is sent entries
        let mut behind = node(3, vec![1, 2]);
        behind.step(1, Msg::Append { term, prev_index: 0, prev_term: 0, entries: vec![], commit: index });
        assert!(matches!(behind.ready().messages[..], [(1, Msg::AppendReply { success: true, match_index: 0, .. })]));
        leader.step(3, Msg::SnapshotReply { term, index });
        assert_eq!(run(&mut leader, 20), (0, 0));
        assert_eq!(leader.matched(3), index);
    }

    #[test]
    fn one_batch_of_entries_at_a_time_goes_to_a_replica() {
        let mut leader = Raft::new(Config { id: 1, peers: vec![2], election_ticks: 8, heartbeat_ticks: 2, seed: 1, join: false }, MemStorage::new());
        while leader.role != Role::Candidate {
            leader.tick();
        }
        let term = leader.term;
        leader.step(2, Msg::VoteReply { term, granted: true });
        let reply = |leader: &mut Raft<MemStorage>, match_index| leader.step(2, Msg::AppendReply { term, success: true, match_index });
        // the entries sent since the last call, batch by batch
        let batches = |leader: &mut Raft<MemStorage>| -> Vec<Vec<u64>> {
            let all = leader.ready().messages.into_iter();
            all.filter_map(|(_, m)| match m {
                Msg::Append { entries, .. } if !entries.is_empty() => Some(entries.iter().map(|e| e.index).collect()),
                _ => None,
            })
            .collect()
        };
        assert_eq!(batches(&mut leader), [[1]]);
        // nothing more goes while that batch is on its way
        for _ in 0..5 {
            leader.propose(vec![0; MAX_BATCH / 2]).unwrap();
            leader.tick();
        }
        assert!(batches(&mut leader).is_empty());
        // its answer brings the next at once, as many entries as make up a batch, and then the rest
        reply(&mut leader, 1);
        assert_eq!(batches(&mut leader), [[2, 3]]);
        reply(&mut leader, 3);
        assert_eq!(batches(&mut leader), [[4, 5]]);
        // a refusal that says nothing new does not send the batch twice
        leader.step(2, Msg::AppendReply { term, success: false, match_index: 3 });
        assert!(batches(&mut leader).is_empty());
        reply(&mut leader, 5);
        assert_eq!(batches(&mut leader), [[6]]);
    }

    #[test]
    fn a_cut_keeps_entries_for_a_replica_a_little_way_behind() {
        let config = || Config { id: 1, peers: vec![2], election_ticks: 8, heartbeat_ticks: 2, seed: 1, join: false };
        let storage = std::rc::Rc::new(std::cell::RefCell::new(MemStorage::new()));
        let mut leader = Raft::new(config(), Shared(storage.clone()));
        while leader.role != Role::Candidate {
            leader.tick();
        }
        let term = leader.term;
        leader.step(2, Msg::VoteReply { term, granted: true });
        for k in 0..9 {
            leader.propose(vec![k; 10]).unwrap();
        }
        let reply = |leader: &mut Raft<Shared>, success, match_index| leader.step(2, Msg::AppendReply { term, success, match_index });
        reply(&mut leader, true, 10);
        leader.trailing = 3;
        leader.compact(10, b"state".to_vec());
        assert_eq!((leader.snapshot_index(), leader.bytes_since_snapshot()), (10, 0));
        assert!(leader.entry(7).is_none() && leader.entry(8).is_some() && leader.term_at(7) == Some(term));
        // what a replica is sent when its log ends at `last`
        let mut sent = |last| {
            leader.sent(2, 10, false);
            leader.ready();
            reply(&mut leader, false, last);
            leader.ready().messages.pop().unwrap().1
        };
        assert!(matches!(sent(7), Msg::Append { prev_index: 7, .. }));
        assert!(matches!(sent(6), Msg::Snapshot { .. }));
        // a restart finds the same log
        let again = Raft::new(config(), Shared(storage.clone()));
        assert!(again.entry(7).is_none() && again.entry(8).is_some() && again.term_at(7) == Some(term));
        assert_eq!((again.last_index(), again.commit_index), (10, 10));
    }

    #[derive(Clone)]
    struct Shared(std::rc::Rc<std::cell::RefCell<MemStorage>>);

    impl Storage for Shared {
        fn load(&mut self) -> (HardState, Vec<Entry>, Option<Snapshot>) {
            self.0.borrow_mut().load()
        }
        fn save_hard_state(&mut self, hs: &HardState) {
            self.0.borrow_mut().save_hard_state(hs)
        }
        fn append_entries(&mut self, from: u64, entries: &[Entry]) {
            self.0.borrow_mut().append_entries(from, entries)
        }
        fn save_snapshot(&mut self, snap: &Snapshot, rest: &[Entry]) {
            self.0.borrow_mut().save_snapshot(snap, rest)
        }
        fn snapshot(&self) -> Option<Snapshot> {
            self.0.borrow().snapshot()
        }
    }

    #[test]
    fn three_node_election_replication_partition_and_snapshot() {
        let ids = [1u64, 2, 3];
        let mut nodes: HashMap<NodeId, Raft<MemStorage>> = ids
            .iter()
            .map(|&id| {
                let peers: Vec<NodeId> = ids.iter().copied().filter(|&x| x != id).collect();
                let cfg = Config { id, peers, election_ticks: 8, heartbeat_ticks: 2, seed: id * 17, join: false };
                (id, Raft::new(cfg, MemStorage::new()))
            })
            .collect();

        let deliver = |nodes: &mut HashMap<NodeId, Raft<MemStorage>>, blocked: &HashSet<(NodeId, NodeId)>| -> HashMap<NodeId, Vec<Vec<u8>>> {
            let mut applied: HashMap<NodeId, Vec<Vec<u8>>> = HashMap::new();
            loop {
                let mut msgs = Vec::new();
                for (&id, n) in nodes.iter_mut() {
                    let rdy = n.ready();
                    for e in rdy.committed {
                        if !e.data.is_empty() {
                            applied.entry(id).or_default().push(e.data);
                        }
                    }
                    msgs.extend(rdy.messages.into_iter().map(|(to, m)| (id, to, m)));
                }
                if msgs.is_empty() {
                    break;
                }
                for (from, to, m) in msgs {
                    let lost = blocked.contains(&(from, to)) || blocked.contains(&(to, from));
                    if let Some(upto) = m.carries() {
                        nodes.get_mut(&from).unwrap().sent(to, upto, !lost);
                    }
                    if let Some(dst) = nodes.get_mut(&to).filter(|_| !lost) {
                        dst.step(from, m);
                    }
                }
            }
            applied
        };

        let no_block = HashSet::new();
        for _ in 0..30 {
            for n in nodes.values_mut() {
                n.tick();
            }
            deliver(&mut nodes, &no_block);
        }

        let leader_id = *nodes.iter().find(|(_, n)| n.role == Role::Leader).expect("a leader elected").0;
        assert!(nodes[&leader_id].has_lease());

        // Propose 3 entries and verify quorum commit on all 3 nodes
        for k in 0..3u8 {
            nodes.get_mut(&leader_id).unwrap().propose(vec![10 + k]).unwrap();
        }
        let committed = deliver(&mut nodes, &no_block);
        for id in ids {
            assert_eq!(committed.get(&id).map(Vec::len), Some(3));
        }

        // Isolate node 3, compact log on leader, then heal partition -> node 3 catches up via Snapshot!
        let mut isolated = HashSet::new();
        let victim = ids.into_iter().find(|&x| x != leader_id).unwrap();
        for other in ids {
            if other != victim {
                isolated.insert((victim, other));
            }
        }
        for k in 0..5u8 {
            nodes.get_mut(&leader_id).unwrap().propose(vec![50 + k]).unwrap();
            deliver(&mut nodes, &isolated);
        }
        let cidx = nodes[&leader_id].commit_index;
        nodes.get_mut(&leader_id).unwrap().compact(cidx, b"full-kix-snap".to_vec());

        // Heal network and tick heartbeats
        for _ in 0..10 {
            for n in nodes.values_mut() {
                n.tick();
            }
            deliver(&mut nodes, &no_block);
        }
        assert_eq!(nodes[&victim].commit_index, cidx);
    }
}
