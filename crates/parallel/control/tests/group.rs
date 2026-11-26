//! Several control nodes: they join a group, any of them takes statements, and when the master stops another takes over
//! with nothing that was acknowledged lost.
mod common;
use common::{same, Shards, MARKET};
use kix_control::client::ShardClient;
use kix_control::{Cluster, ClusterSession, GroupConfig};
use kix_shared_ces::wire::{LoadMode, LoadReq, TxnOp, TxnReq, Work};
use kix_shared_ces::ClusterConfig;
use kixdb::ipc::{IpcClient, IpcConfig, Listener};
use kixdb::persist::LogOp;
use kixdb::sym::intern;
use kixdb::{Connection, Server, K};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A group of control nodes over some shards, and one engine to compare with.
struct Group {
    shards: Shards,
    dir: tempfile::TempDir,
    /// The nodes that run, by number, and where each listens (also after it stops).
    nodes: BTreeMap<u64, Cluster>,
    addresses: BTreeMap<u64, String>,
    single: Connection,
}

fn node(dir: &Path, id: u64, listen: &str, join: Option<String>, config: &ClusterConfig) -> Cluster {
    let data = dir.join(format!("control{id}"));
    let group = GroupConfig { id, listen: listen.parse().unwrap(), advertise: None, data, join, import: None };
    // (a node that was stopped in this process lets go of its directory a moment later)
    wait(&format!("control node {id}"), || match Cluster::open_group(group.clone(), config, None) {
        Err(e) if e.detail.contains("in use") => None,
        started => Some(started.unwrap_or_else(|e| panic!("control node {id}: {e}"))),
    })
}

fn wait<T>(what: &str, mut check: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(found) = check() {
            return found;
        }
        assert!(Instant::now() < deadline, "gave up waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

impl Group {
    /// `shards` shards and `controls` control nodes, numbered from 1.
    fn start(shards: u32, controls: u64) -> Group {
        let shards = Shards::start(shards);
        let dir = tempfile::tempdir().unwrap();
        let first = node(dir.path(), 1, "127.0.0.1:0", None, &shards.config());
        let mut group = Group { addresses: BTreeMap::from([(1, first.control_address().unwrap())]), nodes: BTreeMap::from([(1, first)]), shards, dir, single: Server::new().connect() };
        group.master();
        for id in 2..=controls {
            group.join(id);
        }
        group
    }

    /// Start node `id`, joining through the node started last (which need not be the master).
    fn join(&mut self, id: u64) {
        let through = self.addresses.values().next_back().cloned();
        let started = node(self.dir.path(), id, "127.0.0.1:0", through, &ClusterConfig::default());
        self.addresses.insert(id, started.control_address().unwrap());
        self.nodes.insert(id, started);
    }

    /// Start a stopped node again as it was.
    fn restart(&mut self, id: u64) {
        let config = if id == 1 { self.shards.config() } else { ClusterConfig::default() };
        // (the second node as it was started first, naming a node to join through that may be gone)
        let through = (id == 2).then(|| "127.0.0.1:1".to_string());
        let started = node(self.dir.path(), id, &self.addresses[&id], through, &config);
        self.nodes.insert(id, started);
    }

    fn stop(&mut self, id: u64) {
        self.nodes.remove(&id).expect("a running node").halt();
    }

    fn master(&self) -> u64 {
        wait("a master", || self.nodes.iter().find(|n| n.1.is_master()).map(|n| *n.0))
    }

    fn follower(&self) -> u64 {
        let master = self.master();
        *self.nodes.keys().find(|&&id| id != master).expect("another node")
    }

    fn session(&self, id: u64) -> ClusterSession {
        self.nodes[&id].session()
    }

    /// Run `code` on one engine and through node `id`, which must agree.
    fn check(&mut self, id: u64, code: &str) {
        let expected = self.single.eval(code);
        let actual = self.session(id).eval(code);
        match (&expected, &actual) {
            (Ok(e), Ok(a)) => assert!(same(e, a), "node {id}: {code}\n  one engine: {e}\n  cluster:    {a}"),
            (Err(e), Err(a)) => assert_eq!(e.signal, a.signal, "node {id}: {code}\n  one engine: {e}\n  cluster: {a}"),
            _ => panic!("node {id}: {code}\n  one engine: {expected:?}\n  cluster:    {actual:?}"),
        }
    }

    /// The same answers through every node.
    fn check_all(&mut self) {
        let ids: Vec<u64> = self.nodes.keys().copied().collect();
        for id in ids {
            for query in ["select from trade", "select sum size by sym from trade", "aj[`sym`time; trade; quote]", "select from trade lj acct", "inst", "x", "v", ".extra.t"] {
                self.check(id, query);
            }
        }
    }

    fn load(&mut self) {
        let mut session = self.session(self.master());
        for statement in MARKET.iter().chain(&["x: 1; v:: x + count trade", "\\d .extra", "t: ([] a: 1 2 3)", "\\d ."]) {
            self.single.eval(statement).unwrap_or_else(|e| panic!("{statement}: {e}"));
            session.eval(statement).unwrap_or_else(|e| panic!("{statement}: {e}"));
        }
    }

    /// The latest term shard 0 has been fenced with.
    fn term(&self) -> u64 {
        self.shards.nodes[0].server.connect().eval("value `.kix.term").unwrap().as_i64().unwrap() as u64
    }
}

#[test]
fn every_node_takes_statements_and_the_group_outlives_its_master() {
    let mut g = Group::start(2, 1);
    g.load();
    g.join(2);
    g.join(3);
    let first = g.master();
    let members = g.session(g.follower()).eval("CONTROLS").unwrap().to_string();
    for address in g.addresses.values() {
        assert!(members.contains(address), "{members}");
    }
    g.check_all();
    // changes through a node that is not the master, in a namespace of its session's own
    let through = g.follower();
    let mut session = g.session(through);
    for statement in ["`trade insert (16:00:00.000; `IBM; 1.5; 3; `N; `ann)", "x: 2", "\\d .extra", "`t insert enlist 4", "u: count t", "\\d .", "update size: size + 1 from `trade where sym = `IBM"] {
        let (expected, actual) = (g.single.eval(statement), session.eval(statement));
        assert_eq!(expected.map_err(|e| e.signal), actual.map_err(|e| e.signal), "{statement}");
    }
    g.check(through, ".extra.u");
    g.check(through, "`nothing insert 1");
    assert!(session.explain("select from trade").unwrap()[0].contains("merged in order"));
    // kdb+ IPC clients of a node that is not the master see the same database
    let gateway = Listener::bind(g.nodes[&through].gateway(), "127.0.0.1:0", IpcConfig::default()).unwrap();
    let mut client = IpcClient::connect_addr(&gateway.local_addr().to_string(), "u", "").unwrap();
    assert!(same(&client.query_str("select from trade").unwrap(), &g.single.eval("select from trade").unwrap()));
    let message = g.single.eval("(\"count\"; 1 2 3)").unwrap();
    assert_eq!(client.query_k(&message).unwrap(), K::Long(3));
    drop((client, gateway, session));

    // the master dies: another is elected, and nothing is lost
    let term = g.term();
    g.stop(first);
    let second = g.master();
    assert_ne!(first, second);
    g.check_all();
    g.check(second, "`trade insert (16:00:01.000; `AMD; 2.5; 4; `N; `bob)");
    g.check(g.follower(), "w: ([] sym: `a`b; n: 1 2)");
    g.check_all();
    assert!(g.term() > term, "{} then {}", term, g.term());
    // shards refuse the master that is gone, and a control node that is in no group
    let stale = g.shards.runtime.block_on(async {
        let client = ShardClient::new(0, &g.shards.nodes[0].addr.to_string(), None).unwrap().fenced_by(Arc::new(AtomicU64::new(term)));
        client.query(Work::Q("count trade".into())).await
    });
    assert!(stale.unwrap_err().detail.contains("another control node"));
    let outsider = Cluster::connect(&g.shards.config()).unwrap();
    let e = outsider.session().eval("anything: ([] a: 1 2)").unwrap_err();
    assert!(e.detail.contains("another control node"), "{e}");

    // it comes back as a follower and catches up
    g.restart(first);
    wait("the node to follow", || g.session(first).eval("count w").ok());
    assert!(!g.nodes[&first].is_master());
    g.check_all();
    g.check(first, "w");

    // without a majority nothing is answered; with one again all is as it was
    let master = g.master();
    let other = g.follower();
    g.stop(master);
    g.stop(other);
    let last = *g.nodes.keys().next().unwrap();
    let e = g.session(last).eval("count trade").unwrap_err();
    assert!(e.detail.contains("no control node is the master"), "{e}");
    g.restart(other);
    g.master();
    g.check(last, "count trade");
    g.check_all();
}

#[test]
fn what_was_acknowledged_survives_a_master_that_dies_while_writing() {
    let mut g = Group::start(2, 3);
    let master = g.master();
    g.session(master).eval("events: ([] n: `long$(); sym: `symbol$())").unwrap();
    for round in 0..3 {
        let master = g.master();
        let mut session = g.session(g.follower());
        let writer = std::thread::spawn(move || {
            let mut acknowledged = vec![];
            for n in round * 1000..round * 1000 + 150 {
                // (one row goes to one shard, two rows may go to both)
                let statement = match n % 3 {
                    0 => format!("`events insert ({n}; `a)"),
                    1 => format!("`events insert ({n} {}; `b`c)", n + 500),
                    _ => format!("t{n}: ([] a: enlist {n}); `events insert ({n}; `d)"),
                };
                match session.eval(&statement) {
                    Ok(_) => acknowledged.push(n),
                    Err(e) => eprintln!("{n}: {e}"),
                }
            }
            acknowledged
        });
        std::thread::sleep(Duration::from_millis(150));
        g.stop(master);
        let acknowledged = writer.join().unwrap();
        let now = g.master();
        let mut check = g.session(now);
        assert!(acknowledged.len() > 3, "{acknowledged:?}");
        for n in &acknowledged {
            assert_eq!(check.eval(&format!("count select from events where n = {n}")).unwrap(), K::Long(1), "row {n}");
            if n % 3 == 2 {
                assert_eq!(check.eval(&format!("exec sum a from t{n}")).unwrap(), K::Long(*n), "table t{n}");
            }
        }
        assert_eq!(check.eval("(count events) = count distinct events`n").unwrap(), K::Bool(true));
        // every row is where one engine would hold it: in the order it arrived
        assert_eq!(check.eval("{x ~ asc x} exec n from events where sym = `a").unwrap(), K::Bool(true));
        g.restart(master);
    }
    let rows = g.shards.rows("events");
    let counted = g.session(g.master()).eval("count events").unwrap();
    assert_eq!(K::Long(rows.iter().sum::<usize>() as i64), counted, "{rows:?}");
}

#[test]
fn a_change_noted_but_not_written_is_finished_by_what_the_shards_did() {
    let mut g = Group::start(2, 3);
    g.load();
    for (txid, committed) in [(11u64, true), (12, false)] {
        let master = g.master();
        // what a master does up to the commit: have the shard prepare, note its own change, perhaps commit
        let term = Arc::new(AtomicU64::new(g.term()));
        let address = g.shards.nodes[0].addr.to_string();
        let rows = g.single.eval("([] a: 1 2)").unwrap();
        g.shards.runtime.block_on(async {
            let client = ShardClient::new(0, &address, None).unwrap().fenced_by(term);
            let ops = vec![TxnOp::Load(LoadReq::new(format!("late{txid}"), LoadMode::Replace, &rows, None))];
            client.txn(&TxnReq::Prepare { txid, primary: 0, ops }).await.unwrap();
            if committed {
                client.txn(&TxnReq::Commit { txid, primary: true, forget: vec![] }).await.unwrap();
            }
        });
        let ops = vec![LogOp::SetVar { name: intern(&format!("made{txid}")), val: K::Long(txid as i64) }];
        let note = postcard::to_allocvec(&(txid, 0u32, ops)).unwrap();
        let hex: String = note.iter().map(|b| format!("{b:02x}")).collect();
        g.session(master).eval(&format!(".kix.pending: 0x{hex}")).unwrap();
        // the master dies there: the next one does the rest
        g.stop(master);
        let mut session = g.session(g.master());
        let made = session.eval(&format!("made{txid}"));
        assert_eq!(made.is_ok(), committed, "{made:?}");
        assert!(session.eval(".kix.pending").is_err());
        assert_eq!(g.shards.nodes[0].server.connect().eval(&format!("@[{{count value x}}; `late{txid}; 0N]")).unwrap(), if committed { K::Long(2) } else { K::Long(i64::MIN) });
        g.restart(master);
        g.check(master, "count trade");
    }
}

#[test]
fn a_new_master_knows_the_shards_there_are() {
    let mut g = Group::start(2, 3);
    g.load();
    let third = g.shards.add(2);
    let through = g.follower();
    g.session(through).eval(&format!("ADD SHARD {third}")).unwrap();
    wait("the followers to know", || g.nodes.values().all(|n| n.shard_count() == 3).then_some(()));
    g.check_all();
    let dead = g.master();
    g.stop(dead);
    g.master();
    g.check_all();
    let through = g.follower();
    g.session(through).eval("REMOVE SHARD").unwrap();
    g.check_all();
    // a dead node that is removed from the group no longer counts towards a majority: of the two left, both are needed
    let K::Table(listed) = g.session(through).eval(&format!("REMOVE CONTROL {dead}")).unwrap() else { panic!("a table") };
    assert_eq!(listed.nrows(), 2, "{}", K::Table(listed));
    g.check(through, "count trade");
    // a fourth takes the dead one's place, and the group survives a loss again
    g.addresses.remove(&dead);
    g.join(4);
    g.stop(g.master());
    g.master();
    g.check_all();
}

#[test]
fn a_group_takes_over_the_state_of_a_node_that_was_alone() {
    let shards = Shards::start(2);
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("cluster.state");
    {
        let alone = Cluster::open(&state, &shards.config()).unwrap();
        let mut s = alone.session();
        s.eval("t: ([] sym: `a`b`c`d; v: 1 2 3 4); y: 7; w:: y + 1; f: {x * 7}").unwrap();
    }
    let group = |id: u64, join: Option<String>, import| {
        let data = dir.path().join(format!("control{id}"));
        let config = GroupConfig { id, listen: "127.0.0.1:0".parse().unwrap(), advertise: None, data, join, import };
        Cluster::open_group(config, &ClusterConfig::default(), None).unwrap()
    };
    let first = group(1, None, Some(state));
    let second = group(2, first.control_address(), None);
    let address = first.control_address().unwrap();
    first.halt();
    drop(first);
    // (of two, one is no majority: with the first back, either can lead, and both have what was imported)
    let first = node(dir.path(), 1, &address, None, &ClusterConfig::default());
    wait("a master", || (first.is_master() || second.is_master()).then_some(()));
    let mut s = second.session();
    assert_eq!(s.eval("(exec sum v from t; y; w; f 6)").unwrap().to_string(), "10 7 8 42");
    s.eval("`t insert (`e; 5)").unwrap();
    assert_eq!(s.eval("t").unwrap().count(), 5);
}
