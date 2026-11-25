//! A change that spans shards is made on all of them or on none, whatever fails part way.
mod common;
use kix_control::client::ShardClient;
use kix_control::Cluster;
use kix_shared_ces::wire::{LoadMode, LoadReq, TxnOp, TxnReply, TxnReq};
use kixdb::{KResult, K};
use std::sync::mpsc;
use std::time::Duration;

#[test]
fn a_change_one_shard_does_not_take_is_made_on_none() {
    let shards = common::Shards::start(3);
    let cluster = Cluster::connect(&shards.config()).unwrap();
    cluster.set_request_timeout(Duration::from_secs(1));
    let mut s = cluster.session();
    s.eval("t: ([] sym: 60#`a`b`c`d`e`f`g`h`i`j; v: til 60)").unwrap();
    let before = shards.rows("t");
    assert!(before.iter().all(|&n| n > 0), "{before:?}");
    // something holds the last shard's engine: it cannot take its part
    let server = shards.nodes[2].server.clone();
    for statement in ["`t upsert ([] sym: `a`b`c`d`e`f`g`h`i`j; v: 10#0)", "update v: 0 - v from `t"] {
        let engine = server.clone();
        let (release, held) = mpsc::channel::<()>();
        let (holding, now_held) = mpsc::channel();
        let hold = std::thread::spawn(move || {
            engine.read_workspace(|_| {
                holding.send(()).unwrap();
                held.recv().ok();
            })
        });
        now_held.recv().unwrap();
        let e = s.eval(statement).unwrap_err();
        assert!(e.detail.contains("no answer within 1s; no shard made the change"), "{statement}: {e}");
        release.send(()).unwrap();
        hold.join().unwrap();
        // (what was waiting for the engine now runs)
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(s.eval("exec (count i; sum v) from t").unwrap().to_string(), "60 1770", "{statement}");
        assert_eq!(shards.rows("t"), before, "{statement}");
    }
    // and the cluster takes changes again
    s.eval("`t upsert ([] sym: `a`b`c`d`e`f`g`h`i`j; v: 10#1)").unwrap();
    assert_eq!(s.eval("exec (count i; sum v) from t").unwrap().to_string(), "70 1780");
    // an error on one shard leaves them all as they were
    s.eval("k: ([sym: `a`b`c`d] v: 1 2 3 4)").unwrap();
    assert_eq!(s.eval("`k insert (`e`f`g`a; 5 6 7 8)").unwrap_err().signal, "insert");
    assert_eq!(s.eval("count k").unwrap(), K::Long(4));
}

/// What a control node does before it dies: tell every shard of a change to take `rows` more rows of `r`, and (when
/// `committed`) tell the primary to commit it.
fn die_half_way(shards: &common::Shards, rows: &str, committed: bool) {
    let rows = shards.nodes[0].server.connect_internal().eval(rows).unwrap();
    let load = LoadReq::new("r", LoadMode::Append, &rows, None);
    shards.runtime.block_on(async {
        let clients: Vec<ShardClient> = shards.nodes.iter().enumerate().map(|(i, n)| ShardClient::new(i as u32, &n.addr.to_string(), None).unwrap()).collect();
        for c in &clients {
            let reply = c.txn(&TxnReq::Prepare { txid: 7, primary: 0, ops: vec![TxnOp::Load(load.clone())] }).await;
            assert!(matches!(reply, Ok(TxnReply::Prepared(_))), "{reply:?}");
        }
        // a shard holding a change takes no other
        let refused: KResult<()> = clients[1].load(&load).await;
        assert!(refused.unwrap_err().detail.contains("waiting to be committed"));
        let other = clients[1].txn(&TxnReq::Prepare { txid: 8, primary: 0, ops: vec![] }).await;
        assert!(other.unwrap_err().detail.contains("waiting to be committed"));
        if committed {
            clients[0].txn(&TxnReq::Commit { txid: 7, primary: true, forget: vec![] }).await.unwrap();
        }
    });
}

#[test]
fn the_next_control_node_finishes_or_abandons_what_was_half_made() {
    let shards = common::Shards::start(3);
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("cluster.state");
    let open = || Cluster::open(&state, &shards.config()).unwrap();
    open().session().eval("r: ([] a: 1 2 3)").unwrap();
    assert_eq!(shards.rows("r"), [3, 3, 3]);

    // no shard had committed: all abandon it
    die_half_way(&shards, "([] a: 4 5)", false);
    let cluster = open();
    let mut s = cluster.session();
    assert_eq!(s.eval("count r").unwrap(), K::Long(3));
    assert_eq!(shards.rows("r"), [3, 3, 3]);
    s.eval("`r insert enlist 4").unwrap();
    assert_eq!(shards.rows("r"), [4, 4, 4]);
    drop((s, cluster));

    // the primary had committed: the others are told to
    die_half_way(&shards, "([] a: 5 6)", true);
    assert_eq!(shards.rows("r"), [6, 4, 4]);
    let cluster = open();
    let mut s = cluster.session();
    assert_eq!(s.eval("count r").unwrap(), K::Long(6));
    assert_eq!(shards.rows("r"), [6, 6, 6]);
    s.eval("delete from `r where a > 4").unwrap();
    assert_eq!(shards.rows("r"), [4, 4, 4]);
}
