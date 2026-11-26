//! A replicated shard behind the control node: replicas hold the same data, a new leader takes over when one dies, a
//! replica can be added and removed while serving, one that fell behind is sent a snapshot, and a shard drains.
use kix_control::Cluster;
use kix_raft::Role;
use kix_shared_ces::{ClusterConfig, LoadMode, LoadReq, ShardInfo};
use kix_storage::{spawn, spawn_on, ReplicaConfig, RunningShard, ShardNode};
use kixdb::{Server, K};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Group {
    runtime: tokio::runtime::Runtime,
    dir: tempfile::TempDir,
    replicas: Vec<Option<RunningShard>>,
    addrs: Vec<String>,
}

fn replica(runtime: &tokio::runtime::Runtime, dir: &Path, listener: tokio::net::TcpListener, config: impl FnOnce(&mut ReplicaConfig), id: u64, peers: Vec<(u64, String)>) -> RunningShard {
    let _inside = runtime.enter();
    let server = Server::open_with(dir.join(format!("data{id}")), kixdb::StorageOptions { sync_wal: false, checkpoint_every: 50 }).unwrap();
    let mut rc = ReplicaConfig::new(id, peers, dir.join(format!("raft{id}")));
    rc.sync = false;
    rc.addr = listener.local_addr().unwrap().to_string();
    rc.commit_timeout = Duration::from_secs(3);
    config(&mut rc);
    let node = ShardNode::replicated(0, server, rc).unwrap();
    runtime.block_on(spawn_on(node, listener)).unwrap()
}

impl Group {
    fn start(snapshot_every: u64) -> Group {
        Group::start_with("", |rc| rc.snapshot_every = snapshot_every)
    }

    /// A group whose replicas, set up by `configure`, all begin with the data `preload` makes (it is not in their logs).
    fn start_with(preload: &str, configure: impl Fn(&mut ReplicaConfig)) -> Group {
        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let dir = tempfile::tempdir().unwrap();
        for id in (1..=3).filter(|_| !preload.is_empty()) {
            Server::open(dir.path().join(format!("data{id}"))).unwrap().connect_internal().eval(preload).unwrap();
        }
        // every replica must know the others' addresses before any starts
        let listeners: Vec<_> = (0..3).map(|_| runtime.block_on(tokio::net::TcpListener::bind("127.0.0.1:0")).unwrap()).collect();
        let addrs: Vec<String> = listeners.iter().map(|l| l.local_addr().unwrap().to_string()).collect();
        let replicas = listeners
            .into_iter()
            .enumerate()
            .map(|(i, l)| {
                let peers = (0..3).filter(|&p| p != i).map(|p| (p as u64 + 1, addrs[p].clone())).collect();
                Some(replica(&runtime, dir.path(), l, &configure, i as u64 + 1, peers))
            })
            .collect();
        Group { runtime, dir, replicas, addrs }
    }

    fn address(&self) -> String {
        self.addrs.join(",")
    }

    fn leader(&self) -> usize {
        wait("a leader", || {
            self.replicas.iter().position(|r| r.as_ref().is_some_and(|r| r.replica.as_ref().unwrap().can_serve()))
        })
    }

    fn kill(&mut self, i: usize) {
        let node = self.replicas[i].take().unwrap();
        self.runtime.block_on(node.shutdown());
    }

    /// The number of rows of `table` the leader holds.
    fn leader_rows(&self, table: &str) -> i64 {
        let leader = self.replicas[self.leader()].as_ref().unwrap();
        leader.server.connect_internal().eval(&format!("count {table}")).unwrap().as_i64().unwrap()
    }

    /// Every running replica comes to hold `count` rows of `table`.
    fn all_hold(&self, table: &str, count: i64) {
        for r in self.replicas.iter().flatten() {
            wait(&format!("{count} rows of {table} on {}", r.addr), || {
                let n = r.server.connect_internal().eval(&format!("count {table}")).ok()?;
                (n == K::Long(count)).then_some(())
            });
        }
    }
}

fn wait<T>(what: &str, mut check: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(v) = check() {
            return v;
        }
        assert!(Instant::now() < deadline, "gave up waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn config(shards: &[String]) -> ClusterConfig {
    ClusterConfig { shards: shards.iter().enumerate().map(|(i, a)| ShardInfo { id: i as u32, addr: a.clone() }).collect() }
}

#[test]
fn a_replicated_shard_survives_its_leader_and_changes_members() {
    let mut group = Group::start(20);
    group.leader();
    // a second, unreplicated shard
    let other = group.runtime.block_on(spawn(ShardNode::new(1, Server::new()), "127.0.0.1:0".parse::<SocketAddr>().unwrap())).unwrap();
    let cluster = Cluster::connect(&config(&[group.address(), other.addr.to_string()])).unwrap();
    cluster.wait_for_shards(Duration::from_secs(10)).unwrap();
    let mut s = cluster.session();
    s.eval("trade: ([] sym: `a`b`c`d`e`f; qty: 1 2 3 4 5 6)").unwrap();
    for i in 0..30 {
        s.eval(&format!("`trade insert (`a`b`c`d`e`f; 6#{i})")).unwrap();
    }
    let mut rows: i64 = 6 + 30 * 6;
    assert_eq!(s.eval("count trade").unwrap(), K::Long(rows));
    assert!(s.eval("SHARDS").unwrap().to_string().contains("leader"));
    let on_group = group.leader_rows("trade");
    assert!(on_group > 0 && on_group < rows);
    group.all_hold("trade", on_group);
    // the log was cut: a snapshot stands for its start
    wait("a snapshot", || group.replicas.iter().flatten().all(|r| r.replica.as_ref().unwrap().snapshot_index() > 0).then_some(()));

    // the leader dies: the others elect one, and the cluster goes on
    let dead = group.leader();
    group.kill(dead);
    assert_eq!(s.eval("count trade").unwrap(), K::Long(rows));
    s.eval("`trade insert (`a`b`c`d`e`f; 6#100)").unwrap();
    s.eval("update qty: qty + 1 from `trade where sym in `a`b`c`d`e`f").unwrap();
    rows += 6;
    assert_eq!(s.eval("exec count i from trade where qty = 101").unwrap(), K::Long(6));
    assert_ne!(group.leader(), dead);

    // it comes back, having missed entries the log no longer holds, and catches up
    let listener = wait("its port", || group.runtime.block_on(tokio::net::TcpListener::bind(&group.addrs[dead])).ok());
    for i in 0..30 {
        s.eval(&format!("small{i}: {i}; `trade insert (`a`b`c`d`e`f; 6#{i})")).unwrap();
    }
    rows += 30 * 6;
    let peers = (0..3).filter(|&p| p != dead).map(|p| (p as u64 + 1, group.addrs[p].clone())).collect();
    let back = replica(&group.runtime, group.dir.path(), listener, |rc| rc.snapshot_every = 20, dead as u64 + 1, peers);
    group.replicas[dead] = Some(back);
    let on_group = group.leader_rows("trade");
    group.all_hold("trade", on_group);
    assert_eq!(s.eval("count trade").unwrap(), K::Long(rows));

    // a fourth replica joins and a member is removed, while the shard serves
    let listener = group.runtime.block_on(tokio::net::TcpListener::bind("127.0.0.1:0")).unwrap();
    let fourth = listener.local_addr().unwrap().to_string();
    let peers = (0..3).map(|p| (p as u64 + 1, group.addrs[p].clone())).collect();
    let joined = replica(&group.runtime, group.dir.path(), listener, |rc| rc.join = true, 4, peers);
    let members = s.eval(&format!("ADD REPLICA 0 4 {fourth}")).unwrap().to_string();
    assert!(members.contains(&fourth), "{members}");
    group.replicas.push(Some(joined));
    group.addrs.push(fourth.clone());
    // the control node now knows the new replica
    let K::Table(shards) = s.eval("SHARDS").unwrap() else { panic!("SHARDS is a table") };
    let known = shards.col(kixdb::sym::intern("addr")).unwrap().get_atom(0).cell_text();
    assert!(known.contains(&fourth), "{known}");
    group.all_hold("trade", on_group);
    let leader = group.leader() as u64 + 1;
    let victim = (1..=3).find(|&id| id != leader).unwrap();
    s.eval(&format!("REMOVE REPLICA 0 {victim}")).unwrap();
    group.kill(victim as usize - 1);
    assert_eq!(s.eval("REPLICAS 0").unwrap().count(), 3, "three members");
    s.eval("`trade insert (`a`b`c`d`e`f; 6#7)").unwrap();
    assert_eq!(s.eval("count trade").unwrap(), K::Long(rows + 6));
    assert!(s.eval("REPLICAS 1").unwrap_err().detail.contains("not replicated"));

    // leadership can move to the new replica, which the control node was not started with
    let others: Vec<usize> = (0..3).filter(|&i| group.replicas[i].is_some()).collect();
    group.kill(others[0]);
    assert_eq!(s.eval("count trade").unwrap(), K::Long(rows + 6));

    for i in 0..group.replicas.len() {
        if group.replicas[i].is_some() {
            group.kill(i);
        }
    }
    group.runtime.block_on(other.shutdown());
}

#[test]
fn without_a_majority_nothing_is_written() {
    let mut group = Group::start(0);
    group.leader();
    let cluster = Cluster::connect(&config(&[group.address()])).unwrap();
    let mut s = cluster.session();
    s.eval("t: ([] sym: `a`b; v: 1 2)").unwrap();
    let leader = group.leader();
    for i in (0..3).filter(|&i| i != leader) {
        group.kill(i);
    }
    assert_eq!(s.eval("`t insert (`c; 3)").unwrap_err().signal, "cluster");
    // the engine of the old leader did not change
    let engine = &group.replicas[leader].as_ref().unwrap().server;
    assert_eq!(engine.connect_internal().eval("count t").unwrap(), K::Long(2));
    // a replica that is not the leader refuses changes made on its engine directly
    wait("the leader to step down", || (group.replicas[leader].as_ref().unwrap().replica.as_ref().unwrap().role().0 != Role::Leader).then_some(()));
    assert_eq!(engine.connect_internal().eval("x: 1").unwrap_err().signal, "cluster");
    group.kill(leader);
}

/// Forward connections to `to`, passing about `rate` bytes a second towards it and counting them in `sent`.
async fn slow_link(listener: tokio::net::TcpListener, to: String, rate: usize, sent: Arc<AtomicUsize>) {
    // when the link is next free
    let free = Arc::new(tokio::sync::Mutex::new(Instant::now()));
    while let Ok((from, _)) = listener.accept().await {
        let (to, free, sent) = (to.clone(), free.clone(), sent.clone());
        tokio::spawn(async move {
            let Ok(out) = tokio::net::TcpStream::connect(to).await else { return };
            let ((mut read, mut write), (mut back, mut forth)) = (from.into_split(), out.into_split());
            tokio::spawn(async move { tokio::io::copy(&mut back, &mut write).await });
            let mut buf = vec![0u8; 16 * 1024];
            while let Ok(n @ 1..) = read.read(&mut buf).await {
                let mut at = free.lock().await;
                *at = (*at).max(Instant::now() - Duration::from_millis(50)) + Duration::from_secs_f64(n as f64 / rate as f64);
                tokio::time::sleep_until((*at).into()).await;
                sent.fetch_add(n, Relaxed);
                if forth.write_all(&buf[..n]).await.is_err() {
                    break;
                }
            }
        });
    }
}

#[test]
fn a_replica_behind_a_slow_link_is_sent_its_snapshot_once() {
    // four megabytes on each replica, and then small changes until the log is cut: only a snapshot can pass the data on
    // (the snapshot is sent as several pieces)
    kix_shared_ces::pieces::PIECE.store(1 << 20, Relaxed);
    let mut group = Group::start_with("big: ([] v: 0.5 + til 524288)", |rc| rc.snapshot_every = 10);
    let leader = group.leader();
    let mut engine = group.replicas[leader].as_ref().unwrap().server.connect_internal();
    for i in 0..12 {
        engine.eval(&format!("x{i}: {i}")).unwrap();
    }
    wait("a snapshot", || (group.replicas[leader].as_ref().unwrap().replica.as_ref().unwrap().snapshot_index() >= 10).then_some(()));
    // the new replica is reached at 900 kB a second: the snapshot takes longer than other messages are given
    let bind = || group.runtime.block_on(tokio::net::TcpListener::bind("127.0.0.1:0")).unwrap();
    let (listener, front) = (bind(), bind());
    let (direct, public) = (listener.local_addr().unwrap().to_string(), front.local_addr().unwrap().to_string());
    let sent = Arc::new(AtomicUsize::new(0));
    group.runtime.spawn(slow_link(front, direct, 900_000, sent.clone()));
    let peers = (0..3).map(|p| (p as u64 + 1, group.addrs[p].clone())).collect();
    let reached_at = public.clone();
    let joined = replica(&group.runtime, group.dir.path(), listener, |rc| (rc.join, rc.addr) = (true, reached_at), 4, peers);
    // (adding it waits only as long as the leader's own commit timeout, 3 s: it may give up before the replica is there)
    let _ = group.replicas[leader].as_ref().unwrap().replica.as_ref().unwrap().add_replica(4, &public);
    group.replicas.push(Some(joined));
    group.all_hold("big", 524288);
    let sent = sent.load(Relaxed);
    assert!(sent < 6 << 20, "{sent} bytes sent for a snapshot of about 4 MB");
    // asking again finishes the job: the replica becomes a voter
    let leading = group.replicas[leader].as_ref().unwrap().replica.as_ref().unwrap();
    wait("the replica to be added", || leading.add_replica(4, &public).ok());
    assert!(leading.members().iter().any(|m| m.0 == 4 && m.2));
    for i in 0..group.replicas.len() {
        group.kill(i);
    }
}

#[test]
fn a_leader_whose_engine_is_stuck_hands_over() {
    let mut group = Group::start_with("", |rc| rc.stall_limit = Duration::from_millis(500));
    let stuck = group.leader();
    let cluster = Cluster::connect(&config(&[group.address()])).unwrap();
    let mut s = cluster.session();
    s.eval("t: ([] a: 1 2 3)").unwrap();
    // something takes the leader's engine and keeps it, with a change waiting for it
    let engine = group.replicas[stuck].as_ref().unwrap().server.clone();
    let (release, held) = std::sync::mpsc::channel::<()>();
    let (holding, now_held) = std::sync::mpsc::channel();
    let hold = std::thread::spawn(move || {
        engine.read_workspace(|_| {
            holding.send(()).unwrap();
            held.recv().ok();
        })
    });
    now_held.recv().unwrap();
    // (sent to that replica itself, so that the cluster's own statements go on)
    let rows = Server::new().connect().eval("([] a: enlist 4)").unwrap();
    let load = LoadReq::new("t", LoadMode::Append, &rows, None);
    let addr = group.addrs[stuck].clone();
    let waiting = group.runtime.spawn(async move { kix_control::client::ShardClient::new(0, &addr, None).unwrap().load(&load).await });
    wait("another leader", || (group.leader() != stuck).then_some(()));
    s.eval("`t insert enlist 5").unwrap();
    assert_eq!(s.eval("count t").unwrap(), K::Long(4));
    release.send(()).unwrap();
    hold.join().unwrap();
    // the change that waited is refused: its replica no longer leads
    assert!(group.runtime.block_on(waiting).unwrap().is_err());
    for i in 0..group.replicas.len() {
        group.kill(i);
    }
}

#[test]
fn a_draining_shard_finishes_and_refuses_new_work() {
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let server = Server::open(dir.path()).unwrap();
    let node = runtime.block_on(spawn(ShardNode::new(0, server), "127.0.0.1:0".parse().unwrap())).unwrap();
    let cluster = Cluster::connect(&config(&[node.addr.to_string()])).unwrap();
    let mut s = cluster.session();
    s.eval("t: ([] sym: `a`b; v: 1 2)").unwrap();
    let report = runtime.block_on(node.control().drain(Duration::from_secs(5)));
    assert!(report.checkpointed && report.abandoned == 0);
    let refused = s.eval("count t").unwrap_err();
    assert!(refused.detail.contains("SHUTTING_DOWN"), "{refused}");
    assert!(node.control().metrics().draining);
    runtime.block_on(node.shutdown());
    // the checkpoint holds the data
    drop(cluster);
    let reopened = wait("the directory to be released", || Server::open(dir.path()).ok());
    assert_eq!(reopened.connect().eval("count t").unwrap(), K::Long(2));
}
