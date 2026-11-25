//! Raft under a simulated hostile network: messages are lost, delayed and reordered, nodes are partitioned off, crash and
//! restart from their storage, and take snapshots. Safety is checked at every step, liveness after the network heals.
use kix_raft::{Config, FileStorage, Member, MemStorage, Msg, NodeId, Raft, Role, Storage};
use std::collections::{HashMap, HashSet};

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

/// A node's storage survives its crashes.
#[derive(Clone, Default)]
struct Disk(std::rc::Rc<std::cell::RefCell<MemStorage>>);

impl Storage for Disk {
    fn load(&self) -> (kix_raft::HardState, Vec<kix_raft::Entry>, Option<kix_raft::Snapshot>) {
        self.0.borrow().load()
    }
    fn save_hard_state(&mut self, hs: &kix_raft::HardState) {
        self.0.borrow_mut().save_hard_state(hs)
    }
    fn append_entries(&mut self, from: u64, entries: &[kix_raft::Entry]) {
        self.0.borrow_mut().append_entries(from, entries)
    }
    fn save_snapshot(&mut self, snap: &kix_raft::Snapshot, rest: &[kix_raft::Entry]) {
        self.0.borrow_mut().save_snapshot(snap, rest)
    }
}

fn config(id: NodeId, ids: &[NodeId], seed: u64, join: bool) -> Config {
    let peers = ids.iter().copied().filter(|&p| p != id).collect();
    Config { id, peers, election_ticks: 10, heartbeat_ticks: 2, seed: seed.wrapping_mul(31) + id, join }
}

struct Sim {
    rng: Rng,
    ids: Vec<NodeId>,
    nodes: HashMap<NodeId, Raft<Disk>>,
    disks: HashMap<NodeId, Disk>,
    /// the state machine of each node: the commands it applied, in order (a snapshot carries all of them)
    applied: HashMap<NodeId, Vec<u32>>,
    wire: Vec<(NodeId, NodeId, Msg)>,
    cut: HashSet<NodeId>,
    leaders: HashMap<u64, NodeId>,
    /// the longest sequence of commands any node has applied: every other node's must be a prefix of it
    truth: Vec<u32>,
    next_cmd: u32,
    loss: u64,
}

impl Sim {
    fn new(n: u64, seed: u64) -> Sim {
        let ids: Vec<NodeId> = (1..=n).collect();
        let mut sim = Sim {
            rng: Rng(seed * 2 + 1),
            ids: ids.clone(),
            nodes: HashMap::new(),
            disks: HashMap::new(),
            applied: HashMap::new(),
            wire: vec![],
            cut: HashSet::new(),
            leaders: HashMap::new(),
            truth: vec![],
            next_cmd: 0,
            loss: 0,
        };
        for &id in &ids {
            let disk = Disk::default();
            sim.nodes.insert(id, Raft::new(config(id, &ids, seed, false), disk.clone()));
            sim.disks.insert(id, disk);
            sim.applied.insert(id, vec![]);
        }
        sim
    }

    fn restart(&mut self, id: NodeId) {
        let disk = self.disks[&id].clone();
        // the state machine is rebuilt from the snapshot and the entries committed again
        let state = disk.load().2.map(|s| decode(&s.data)).unwrap_or_default();
        self.applied.insert(id, state);
        let seed = self.rng.below(1 << 30);
        let ids = self.ids.clone();
        self.nodes.insert(id, Raft::new(config(id, &ids, seed, false), disk));
    }

    fn step(&mut self) {
        let mut order: Vec<NodeId> = self.nodes.keys().copied().collect();
        order.sort_unstable();
        for &id in &order {
            if self.rng.chance(80) {
                self.nodes.get_mut(&id).unwrap().tick();
            }
        }
        // deliver a random selection of what is on the wire, out of order
        let mut later = vec![];
        for (from, to, m) in std::mem::take(&mut self.wire) {
            let carries = m.carries();
            let lost = self.cut.contains(&from) != self.cut.contains(&to) || self.rng.chance(self.loss);
            if !lost && self.rng.chance(self.loss) {
                later.push((from, to, m));
                continue;
            }
            if !lost {
                if let Some(n) = self.nodes.get_mut(&to) {
                    n.step(from, m);
                }
            }
            // as a transport must: tell the sender what became of what it sent
            if let (Some(upto), Some(n)) = (carries, self.nodes.get_mut(&from)) {
                n.sent(to, upto, !lost);
            }
        }
        self.wire = later;
        for &id in &order {
            self.drain(id);
        }
    }

    fn drain(&mut self, id: NodeId) {
        let node = self.nodes.get_mut(&id).unwrap();
        let ready = node.ready();
        if node.role == Role::Leader {
            let first = *self.leaders.entry(node.term).or_insert(id);
            assert_eq!(first, id, "two leaders in term {}", node.term);
        }
        self.wire.extend(ready.messages.into_iter().map(|(to, m)| (id, to, m)));
        let state = self.applied.get_mut(&id).unwrap();
        if let Some(s) = ready.snapshot {
            *state = decode(&s.data);
        }
        state.extend(ready.committed.iter().filter(|e| !e.data.is_empty()).map(|e| u32::from_le_bytes(e.data[..4].try_into().unwrap())));
        let common = state.len().min(self.truth.len());
        assert_eq!(state[..common], self.truth[..common], "node {id} applied something else than the group");
        if state.len() > self.truth.len() {
            self.truth = state.clone();
        }
    }

    fn propose(&mut self) {
        let cmd = self.next_cmd;
        let leaders: Vec<NodeId> = self.nodes.iter().filter(|(_, n)| n.role == Role::Leader).map(|(&i, _)| i).collect();
        for id in leaders {
            if self.nodes.get_mut(&id).unwrap().propose(cmd.to_le_bytes().to_vec()).is_ok() {
                self.next_cmd += 1;
                break;
            }
        }
    }

    fn leader(&self) -> Option<NodeId> {
        self.nodes.iter().filter(|(i, n)| n.role == Role::Leader && !self.cut.contains(i)).max_by_key(|(_, n)| n.term).map(|(&i, _)| i)
    }

    /// Heal everything and check that the group makes progress and every node catches up.
    fn settle(&mut self) {
        self.cut.clear();
        self.loss = 0;
        for _ in 0..200 {
            self.step();
        }
        let before = self.truth.len();
        for _ in 0..5 {
            self.propose();
        }
        for _ in 0..100 {
            self.step();
        }
        assert!(self.truth.len() >= before + 5, "no progress after healing");
        for (id, n) in &self.nodes {
            if n.conf.is_voter(*id) {
                assert_eq!(self.applied[id], self.truth, "node {id} did not catch up");
            }
        }
    }
}

fn decode(data: &[u8]) -> Vec<u32> {
    data.chunks(4).map(|c| u32::from_le_bytes(c.try_into().unwrap())).collect()
}

#[test]
fn random_faults_never_break_safety() {
    for seed in 0..60 {
        let size = if seed % 2 == 0 { 3 } else { 5 };
        let mut sim = Sim::new(size, seed);
        for round in 0..1500 {
            if round % 100 == 0 {
                sim.loss = sim.rng.below(30);
                sim.cut.clear();
                for id in 1..=size {
                    if sim.rng.chance(20) {
                        sim.cut.insert(id);
                    }
                }
            }
            if sim.rng.chance(1) {
                let id = 1 + sim.rng.below(size);
                sim.restart(id);
            }
            if sim.rng.chance(2) {
                // snapshot a node's state and cut its log
                let id = 1 + sim.rng.below(size);
                let data: Vec<u8> = sim.applied[&id].iter().flat_map(|c| c.to_le_bytes()).collect();
                let n = sim.nodes.get_mut(&id).unwrap();
                let upto = n.commit_index;
                n.compact(upto, data);
            }
            if sim.rng.chance(30) {
                sim.propose();
            }
            sim.step();
        }
        sim.settle();
    }
}

#[test]
fn membership_changes_add_a_learner_promote_it_and_remove_the_leader() {
    let mut sim = Sim::new(3, 7);
    sim.settle();
    let leader = sim.leader().unwrap();
    let disk = Disk::default();
    sim.nodes.insert(4, Raft::new(config(4, &[], 99, true), disk.clone()));
    sim.disks.insert(4, disk);
    sim.applied.insert(4, vec![]);

    // add as a learner: it follows the log but does not vote
    let conf = sim.nodes[&leader].conf.with_learner(Member { id: 4, addr: "four".into() });
    sim.nodes.get_mut(&leader).unwrap().propose_conf(conf).unwrap();
    assert!(sim.nodes[&leader].has_pending_conf());
    for _ in 0..50 {
        sim.step();
    }
    assert!(!sim.nodes[&leader].has_pending_conf());
    assert!(sim.nodes[&4].conf.is_learner(4));
    assert_eq!(sim.applied[&4], sim.truth);

    let conf = sim.nodes[&leader].conf.with_promoted(4);
    sim.nodes.get_mut(&leader).unwrap().propose_conf(conf).unwrap();
    for _ in 0..50 {
        sim.step();
    }
    assert!(sim.nodes[&4].conf.is_voter(4));

    // the leader removes itself: it hands over, and the removed node cannot disturb the others
    let conf = sim.nodes[&leader].conf.without(leader);
    sim.nodes.get_mut(&leader).unwrap().propose_conf(conf).unwrap();
    for _ in 0..200 {
        sim.step();
    }
    let new_leader = sim.leader().expect("a new leader");
    assert_ne!(new_leader, leader);
    assert_eq!(sim.nodes[&new_leader].conf.voters.len(), 3);
    sim.settle();
}

#[test]
fn a_cut_off_node_does_not_depose_a_healthy_leader_when_it_returns() {
    let mut sim = Sim::new(3, 3);
    sim.settle();
    let leader = sim.leader().unwrap();
    let term = sim.nodes[&leader].term;
    let victim = sim.ids.iter().copied().find(|&i| i != leader).unwrap();
    sim.cut.insert(victim);
    for _ in 0..200 {
        sim.step();
        assert!(sim.nodes[&leader].has_lease());
    }
    assert!(sim.nodes[&victim].term > term);
    sim.settle();
}

#[test]
fn file_storage_survives_reopening_and_refuses_damage() {
    let dir = tempfile::tempdir().unwrap();
    let open = || FileStorage::open(dir.path(), true);
    {
        let mut n = Raft::new(config(1, &[1], 1, false), open().unwrap());
        n.tick();
        for c in 0..3u8 {
            n.propose(vec![c]).unwrap();
        }
        let upto = n.commit_index - 1;
        n.compact(upto, b"state".to_vec());
    }
    let mut n = Raft::new(config(1, &[1], 1, false), open().unwrap());
    assert!(n.term >= 1 && n.last_index() == 4 && n.snapshot_index() == 3);
    n.tick();
    assert_eq!(n.role, Role::Leader);

    let path = dir.path().join("raft.snap");
    let mut bytes = std::fs::read(&path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    std::fs::write(&path, bytes).unwrap();
    assert!(open().is_err());
}
