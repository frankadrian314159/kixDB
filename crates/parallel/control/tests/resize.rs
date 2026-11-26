//! Adding and removing shards while the cluster holds data: every answer stays that of one engine, each shard keeps its
//! rows in order, and a change that cannot be completed is not made at all.
mod common;
use common::{load_market, Pair, Shards};
use kix_control::client::ShardClient;
use kix_control::Cluster;
use kix_shared_ces::route::{even_slots, rebalanced, shard_of};
use kix_shared_ces::wire::{moving_var, LoadMode, LoadReq, MoveReq, TxnOp, TxnReply, TxnReq, JOINING_VAR, LAYOUT_VAR};
use kix_shared_ces::{ClusterConfig, ShardInfo};
use kix_storage::{spawn, ShardNode};
use kixdb::sym::intern;
use kixdb::{Attr, ColVec, Server, K};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

/// Tables of every kind, beside those of the market.
const MORE: [&str; 14] = [
    ".kix.dist[`daily; `partitioned; `date]",
    "daily: ([] date: 2026.01.01 + (til 400) div 8; sym: 400#`a`b`c`d; v: til 400)",
    ".kix.dist[`cq; `partitioned; `date`sym]",
    "cq: ([] date: 2026.01.01 + (til 300) mod 20; sym: 300#`a`b`c`d`e`f`g; v: til 300)",
    "none: ([] sym: `symbol$(); v: `long$())",
    "pos: ([sym: `AAPL`MSFT`GOOG`IBM`NVDA`AMD`ORCL; acct: 1 2 3 4 5 6 7] qty: 10 20 30 40 50 60 70)",
    ".kix.dist[`ref; `replicated]",
    "ref: ([id: 1 2 3] name: `x`y`z)",
    ".kix.dist[`note; `replicated]",
    "note: ([] about: `ref$3 1 2 1; text: `p`q`r`s)",
    "ev: `msft`aapl`goog",
    "orders: ([] sym: `AAPL`MSFT`GOOG`IBM`NVDA; r: `ref$1 2 3 1 2; e: `ev$`msft`aapl`goog`msft`aapl)",
    ".ns.t: ([] sym: `a`b`c`d`e`f; v: til 6)",
    "blank: ([] sym: 5#`; v: til 5)",
];

const QUERIES: [&str; 24] = [
    "select from trade",
    "select from quote",
    "select sum size, hi: max price by sym from trade",
    "select d: deltas price, sym from trade where sym in `AAPL`MSFT`GOOG`IBM`NVDA`AMD`ORCL",
    "select from trade where sym = `AAPL",
    "aj[`sym`time; trade; quote]",
    "select from trade lj inst",
    "select from trade lj acct",
    "select sum v by date from daily",
    "select from daily where date = 2026.01.07",
    "select d: deltas v from daily where date in 2026.01.03 2026.01.04 2026.01.09",
    "select from cq where date = 2026.01.07, sym = `a",
    "select sum v by date, sym from cq",
    "none",
    "pos",
    "select from pos where sym = `AAPL",
    "select r.name, e, sym from orders",
    "select about.name, text from note",
    ".ns.t",
    "blank",
    "inst",
    "acct",
    "count each (trade; daily; cq; pos; orders)",
    "select med price by sym from trade",
];

const SHARDED: [&str; 7] = ["trade", "quote", "daily", "cq", "pos", "orders", ".ns.t"];

fn cluster_of(shards: u32) -> Pair {
    let mut p = load_market(Pair::new(shards));
    for statement in MORE {
        p.single.eval(statement).unwrap_or_else(|e| panic!("{statement}: {e}"));
        p.session.eval(statement).unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    p
}

/// Every answer is one engine's, every shard of the first `n` holds its rows in order, and any beyond them holds none.
fn check_all(p: &mut Pair, n: usize) {
    QUERIES.iter().for_each(|q| drop(p.check(q)));
    for (s, node) in p.shards.nodes.iter().enumerate() {
        let mut c = node.server.connect();
        if s >= n {
            assert_eq!(c.eval("count tables[]").unwrap(), K::Long(0), "shard {s} is not empty");
            continue;
        }
        for table in SHARDED {
            let ords = format!("value `$\".kix.ord.{table}\"");
            let sound = c.eval(&format!("({{x ~ asc x}} {ords}; (count {table}) = count {ords})")).unwrap();
            assert_eq!(sound.to_string(), "11b", "shard {s}, {table}: in order, with an ordinal for each row");
        }
    }
    assert_eq!(p.cluster.shard_count(), n);
}

/// Each of the first `n` shards holds some rows of the larger tables, and together they hold them all once.
fn rows_are_spread(p: &mut Pair, n: usize) {
    for table in ["trade", "quote", "daily", "cq"] {
        let rows = &p.shards.rows(table)[..n];
        assert!(rows.iter().all(|&r| r > 0), "{table}: {rows:?}");
        assert_eq!(K::Long(rows.iter().sum::<usize>() as i64), p.single.eval(&format!("count {table}")).unwrap(), "{table}: {rows:?}");
    }
}

#[test]
fn shards_are_added_and_removed() {
    let mut p = cluster_of(2);
    p.cluster.set_move_batch(7);
    check_all(&mut p, 2);

    let third = p.shards.add(2);
    let K::Table(listed) = p.session.eval(&format!("ADD SHARD {third}")).unwrap() else { panic!("SHARDS is a table") };
    assert_eq!(listed.nrows(), 3);
    check_all(&mut p, 3);
    rows_are_spread(&mut p, 3);
    p.plan("select from trade where sym = `IBM", "route");
    for write in [
        "`trade insert (16:00:00.000; `IBM; 1.5; 3; `N; `ann)",
        "`pos upsert (`AAPL`TSLA; 1 9; 11 99)",
        "`pos insert (`AAPL; 1; 0)",
        "update v: v + 1 from `daily where date < 2026.01.10",
        "delete from `cq where sym = `b",
        "`none insert (`a`b`c; 1 2 3)",
        "`daily insert (2026.03.01 2026.03.02; `a`b; 1 2)",
    ] {
        p.check(write);
        check_all(&mut p, 3);
    }

    let fourth = p.shards.add(3);
    p.cluster.add_shard(&fourth).unwrap();
    check_all(&mut p, 4);
    rows_are_spread(&mut p, 4);

    p.session.eval("REMOVE SHARD").unwrap();
    check_all(&mut p, 3);
    p.check("`trade insert (16:00:01.000; `AMD; 2.5; 4; `N; `bob)");
    // a shard that was removed is empty, and can come back
    p.cluster.add_shard(&fourth).unwrap();
    check_all(&mut p, 4);
    p.cluster.set_move_batch(100_000);
    for n in [3, 2, 1] {
        p.cluster.remove_shard().unwrap();
        check_all(&mut p, n);
        p.check(&format!("update qty: qty + {n} from `pos"));
    }
    assert_eq!(p.cluster.remove_shard().unwrap_err().signal, "domain");
    p.cluster.add_shard(&p.shards.nodes[1].addr.to_string()).unwrap();
    check_all(&mut p, 2);
    rows_are_spread(&mut p, 2);
}

#[test]
fn a_change_that_cannot_be_made_leaves_the_cluster_as_it_was() {
    let mut p = cluster_of(2);
    let refused = |p: &mut Pair, address: &str, why: &str| {
        let e = p.cluster.add_shard(address).unwrap_err();
        assert!(e.detail.contains(why), "{address}: {e}");
        QUERIES.iter().for_each(|q| drop(p.check(q)));
        assert_eq!(p.cluster.shard_count(), 2);
    };
    let first = p.shards.nodes[0].addr.to_string();
    refused(&mut p, &first, "already a shard");
    refused(&mut p, "127.0.0.1:1", "shard 2");
    let wrong = p.shards.add(5);
    refused(&mut p, &wrong, "--id 2");
    let used = p.shards.add(2);
    p.shards.nodes[3].server.connect().eval("mine: ([] a: 1 2)").unwrap();
    refused(&mut p, &used, "not empty");
    assert_eq!(p.shards.nodes[3].server.connect().eval("count mine").unwrap(), K::Long(2));

    // a shard that stays does not take the change: the copy is made, the commit is not
    let new = p.shards.add(2);
    p.cluster.set_request_timeout(Duration::from_secs(1));
    let engine = p.shards.nodes[1].server.clone();
    let (release, held) = mpsc::channel::<()>();
    let (holding, now_held) = mpsc::channel();
    let hold = std::thread::spawn(move || {
        engine.read_workspace(|_| {
            holding.send(()).unwrap();
            held.recv().ok();
        })
    });
    now_held.recv().unwrap();
    let e = p.cluster.add_shard(&new).unwrap_err();
    assert!(e.detail.contains("no shard made the change"), "{e}");
    release.send(()).unwrap();
    hold.join().unwrap();
    std::thread::sleep(Duration::from_millis(300));
    p.cluster.set_request_timeout(Duration::from_secs(300));
    assert_eq!(p.cluster.shard_count(), 2);
    for q in QUERIES {
        p.check(q);
    }
    // what the attempt left on the new shard is thrown away by the next
    let leftover = p.shards.nodes.last().unwrap().server.clone();
    assert_eq!(leftover.connect().eval(&format!("value `{JOINING_VAR}")).unwrap(), K::Bool(true));
    leftover.connect().eval("junk: ([] a: 1 2)").unwrap();
    p.cluster.add_shard(&new).unwrap();
    let kept = p.shards.nodes.remove(p.shards.nodes.len() - 1);
    p.shards.nodes.truncate(2);
    p.shards.nodes.push(kept);
    check_all(&mut p, 3);

    // a removal that is not made leaves no copies beside the tables of the shards that stay
    p.cluster.set_request_timeout(Duration::from_secs(1));
    let engine = p.shards.nodes[0].server.clone();
    let (release, held) = mpsc::channel::<()>();
    let (holding, now_held) = mpsc::channel();
    let hold = std::thread::spawn(move || {
        engine.read_workspace(|_| {
            holding.send(()).unwrap();
            held.recv().ok();
        })
    });
    now_held.recv().unwrap();
    assert_eq!(p.cluster.remove_shard().unwrap_err().signal, "cluster");
    release.send(()).unwrap();
    hold.join().unwrap();
    std::thread::sleep(Duration::from_millis(300));
    p.cluster.set_request_timeout(Duration::from_secs(300));
    check_all(&mut p, 3);
    let copies = p.shards.nodes[1].server.connect().eval(&format!("@[{{count value x}}; `$\"{}\"; `none]", moving_var("trade"))).unwrap();
    assert_eq!(copies.to_string(), "`none");
}

#[test]
fn the_shards_say_how_many_there_are() {
    let mut p = Pair::new(2);
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("cluster.state");
    let config = p.shards.config();
    p.cluster = Cluster::open(&state, &config).unwrap();
    p.session = p.cluster.session();
    let mut p = load_market(p);
    let before = std::fs::read(&state).unwrap();
    let third = p.shards.add(2);
    p.session.eval(&format!("ADD SHARD {third}")).unwrap();
    // (the control node goes, and with it its hold on the state file)
    let close = |p: &mut Pair| {
        p.cluster = Cluster::connect(&config).unwrap();
        p.session = p.cluster.session();
    };
    let reopen = |p: &mut Pair| {
        close(p);
        p.cluster = Cluster::open(&state, &config).unwrap();
        p.session = p.cluster.session();
    };
    // the state file, and a configuration that still names two shards
    reopen(&mut p);
    assert_eq!(p.cluster.shard_count(), 3);
    p.check("select sum size by sym from trade");
    p.check("select from trade");
    // a state file written before the shard was added, as when the control node stops just after the change
    close(&mut p);
    std::fs::write(&state, before).unwrap();
    p.cluster = Cluster::open(&state, &config).unwrap();
    p.session = p.cluster.session();
    assert_eq!(p.cluster.shard_count(), 3);
    p.check("select from trade");
    p.check("`trade insert (16:00:00.000; `IBM; 1.5; 3; `N; `ann)");
    p.check("select from trade");
    // a control node without a state file learns it at its first statement
    let fresh = Cluster::connect(&config).unwrap();
    assert!(fresh.session().eval("SHARDS").is_ok());
    assert_eq!(fresh.shard_count(), 3);
}

/// Do by hand what a control node does to add shard 2 to a cluster of two that holds `r`, up to the commit of shard 0:
/// then it stops.
fn die_while_adding(shards: &Shards) {
    let mut all = shards.nodes[0].server.connect_internal();
    let slots = rebalanced(&even_slots(2), 3);
    let clients: Vec<ShardClient> = shards.runtime.block_on(async {
        shards.nodes.iter().enumerate().map(|(i, n)| ShardClient::new(i as u32, &n.addr.to_string(), None).unwrap()).collect()
    });
    // copy the rows that move to the new shard
    let mut moving: Vec<(i64, i64)> = vec![];
    for node in &shards.nodes[..2] {
        let K::Table(held) = node.server.connect().eval("update o: .kix.ord.r from r").unwrap() else { panic!("a table") };
        for row in 0..held.nrows() {
            let value = held.col(intern("a")).unwrap().get_atom(row);
            if shard_of(std::slice::from_ref(&value), false, &slots) == 2 {
                moving.push((held.col(intern("o")).unwrap().get_atom(row).as_i64().unwrap(), value.as_i64().unwrap()));
            }
        }
    }
    moving.sort_unstable();
    assert!(!moving.is_empty());
    let list = |v: Vec<i64>| v.iter().map(i64::to_string).collect::<Vec<_>>().join(" ");
    let rows = all.eval(&format!("([] a: (),{})", list(moving.iter().map(|m| m.1).collect()))).unwrap();
    let load = LoadReq::new("r", LoadMode::Replace, &rows, Some(moving.iter().map(|m| m.0).collect()));
    // the layout, as shard 0 holds it
    let info: Vec<ShardInfo> = shards.config().shards;
    let layout = postcard::to_allocvec(&(1u64, slots.clone(), info, false)).unwrap();
    let layout = LoadReq::new(LAYOUT_VAR, LoadMode::Replace, &K::Vector(ColVec::Byte(Attr::None, Arc::new(layout))), None);
    let keep = |me| TxnOp::Move(MoveReq::Keep { table: "r".into(), cols: vec!["a".into()], partitioned: false, slots: slots.clone(), me });
    shards.runtime.block_on(async {
        clients[2].load(&load).await.unwrap();
        let parts = [vec![keep(0), TxnOp::Load(layout.clone())], vec![keep(1)], vec![TxnOp::Load(LoadReq::new(JOINING_VAR, LoadMode::Drop, &K::Null, None))]];
        for (client, ops) in clients.iter().zip(parts) {
            let reply = client.txn(&TxnReq::Prepare { txid: 9, primary: 0, ops }).await;
            assert!(matches!(reply, Ok(TxnReply::Prepared(_))), "{reply:?}");
        }
        clients[0].txn(&TxnReq::Commit { txid: 9, primary: true, forget: vec![] }).await.unwrap();
    });
}

#[test]
fn a_change_left_half_made_is_finished_before_anything_is_read() {
    let mut shards = Shards::start(3);
    let two = ClusterConfig { shards: shards.config().shards[..2].to_vec() };
    Cluster::connect(&two).unwrap().session().eval("r: ([] a: til 100); .kix.dist[`r; `sharded; `a]").unwrap();
    die_while_adding(&shards);
    // shard 1 has not heard, and cannot be told while something holds its engine
    let next = Cluster::connect(&two).unwrap();
    next.set_request_timeout(Duration::from_secs(1));
    let mut s = next.session();
    let engine = shards.nodes[1].server.clone();
    let (release, held) = mpsc::channel::<()>();
    let (holding, now_held) = mpsc::channel();
    let hold = std::thread::spawn(move || {
        engine.read_workspace(|_| {
            holding.send(()).unwrap();
            held.recv().ok();
        })
    });
    now_held.recv().unwrap();
    let e = s.eval("1+1").unwrap_err();
    assert!(e.detail.contains("not finished"), "{e}");
    release.send(()).unwrap();
    hold.join().unwrap();
    std::thread::sleep(Duration::from_millis(300));
    next.set_request_timeout(Duration::from_secs(300));
    assert_eq!(next.shard_count(), 3);
    // `r` was made without the mirror of this node: ask the shards
    s.eval("::").unwrap();
    let held = shards.rows("r");
    assert_eq!(held.iter().sum::<usize>(), 100, "{held:?}");
    assert!(held[2] > 0, "{held:?}");
    shards.nodes.clear();
}

#[test]
fn partitioned_tables_of_an_old_cluster_move_to_the_slot_map() {
    let mut p = Pair::new(3);
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("cluster.state");
    let config = p.shards.config();
    p.cluster = Cluster::open(&state, &config).unwrap();
    p.session = p.cluster.session();
    // (days for which the old placement and the new one differ: those whose number over 1,024 is not a multiple of 3)
    for statement in [MORE[0], "daily: ([] date: 2029.01.01 + (til 400) div 8; sym: 400#`a`b`c`d; v: til 400)"] {
        p.single.eval(statement).unwrap();
        p.session.eval(statement).unwrap();
    }
    // place the rows as an earlier version did, by the value modulo the number of shards
    let mut whole = Server::new().connect();
    for (s, node) in p.shards.nodes.iter().enumerate() {
        let part = node.server.connect().eval("update o: .kix.ord.daily from daily").unwrap();
        whole.set_var_direct(&format!("p{s}"), part).unwrap();
    }
    whole.eval("whole: `o xasc p0, p1, p2").unwrap();
    for (s, node) in p.shards.nodes.iter().enumerate() {
        let ords = whole.eval(&format!("exec o from whole where {s} = (`long$date) mod 3")).unwrap();
        let rows = whole.eval(&format!("delete o from select from whole where {s} = (`long$date) mod 3")).unwrap();
        let mut c = node.server.connect();
        c.set_var_direct("daily", rows).unwrap();
        c.set_var_direct(".kix.ord.daily", ords).unwrap();
    }
    let by_mod = p.shards.rows("daily");
    drop(std::mem::replace(&mut p.cluster, Cluster::connect(&config).unwrap()));
    p.session = p.cluster.session();
    // and write the state file as that version did
    let raw = std::fs::read(&state).unwrap();
    assert_eq!(&raw[..4], b"KXC2");
    let (shards, slots, mirror, _): (Vec<ShardInfo>, Vec<u32>, Vec<u8>, u64) = postcard::from_bytes(&raw[8..]).unwrap();
    let payload = postcard::to_allocvec(&(shards, slots, mirror)).unwrap();
    let old = [&b"KXCS"[..], &crc32fast::hash(&payload).to_le_bytes(), &payload].concat();
    std::fs::write(&state, old).unwrap();

    p.cluster = Cluster::open(&state, &config).unwrap();
    p.session = p.cluster.session();
    assert_ne!(p.shards.rows("daily"), by_mod);
    assert_eq!(&std::fs::read(&state).unwrap()[..4], b"KXC2");
    for day in 1..29 {
        p.check(&format!("select from daily where date = 2029.01.{day:02}"));
    }
    p.check("select from daily");
}

#[test]
fn a_durable_shard_replays_the_moves() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let dirs: Vec<tempfile::TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let start = |id: u32, addr: &str| {
        let server = Server::open(dirs[id as usize].path()).unwrap();
        runtime.block_on(spawn(ShardNode::new(id, server), addr.parse().unwrap())).unwrap()
    };
    let nodes: Vec<_> = (0..3).map(|id| start(id, "127.0.0.1:0")).collect();
    let addresses: Vec<String> = nodes.iter().map(|n| n.addr.to_string()).collect();
    let two = ClusterConfig { shards: (0..2).map(|id| ShardInfo { id, addr: addresses[id as usize].clone() }).collect() };
    let cluster = Cluster::connect(&two).unwrap();
    cluster.set_move_batch(11);
    let mut s = cluster.session();
    s.eval("t: ([] sym: 100#`a`b`c`d`e`f`g; v: til 100); k: ([sym: `a`b`c`d`e`f`g] v: til 7)").unwrap();
    let held = |servers: &[Server]| -> Vec<String> {
        servers.iter().map(|n| n.connect().eval("(t; .kix.ord.t; k; .kix.ord.k)").unwrap().to_string()).collect()
    };
    cluster.add_shard(&addresses[2]).unwrap();
    cluster.remove_shard().unwrap();
    cluster.add_shard(&addresses[2]).unwrap();
    s.eval("`t insert (`a`b; 100 101)").unwrap();
    let before = held(&nodes.iter().map(|n| n.server.clone()).collect::<Vec<_>>());
    for node in nodes {
        runtime.block_on(node.shutdown());
    }
    // what each shard reads back from its log
    let servers: Vec<Server> = dirs.iter().map(|d| wait_to_open(d.path())).collect();
    assert_eq!(held(&servers), before);
}

fn wait_to_open(dir: &std::path::Path) -> Server {
    for _ in 0..500 {
        if let Ok(server) = Server::open(dir) {
            return server;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("{} was not released", dir.display());
}
