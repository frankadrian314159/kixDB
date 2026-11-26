//! Log records hold what changed in a value: applying them gives exactly the value the writer has.
use kixdb::persist::{LogOp, Replicator, SnapshotData, WalRecord};
use kixdb::{KResult, Server, StorageOptions};
use std::sync::{Arc, Mutex};

/// Keeps the records an engine makes, as the replicas of a shard receive them.
#[derive(Default)]
struct Records(Mutex<Vec<Vec<u8>>>);

impl Replicator for Records {
    fn propose(&self, rec: &WalRecord) -> KResult<()> {
        self.0.lock().unwrap().push(postcard::to_allocvec(rec).unwrap());
        Ok(())
    }
}

/// Every variable of an engine, by name, as the bytes it is stored as.
fn state(server: &Server) -> Vec<(String, Vec<u8>)> {
    let snap: SnapshotData = postcard::from_bytes(&server.export_state().unwrap()).unwrap();
    let mut vars: Vec<_> = snap.vars.iter().map(|(name, v)| (kixdb::sym::resolve(*name).to_string(), postcard::to_allocvec(v).unwrap())).collect();
    vars.sort();
    vars
}

struct Pair {
    leader: Server,
    records: Arc<Records>,
    follower: Server,
}

impl Pair {
    fn new(leader: Server, follower: Server) -> Pair {
        let records = Arc::new(Records::default());
        leader.set_replicator(Some(records.clone()));
        Pair { leader, records, follower }
    }

    /// Run `q` on the leader and its records on the follower; the two must then agree. The size of the records.
    fn run(&self, q: &str) -> usize {
        self.leader.connect_internal().eval(q).unwrap_or_else(|e| panic!("{q}: {e}"));
        let records = std::mem::take(&mut *self.records.0.lock().unwrap());
        for bytes in &records {
            self.follower.apply_replicated(&postcard::from_bytes(bytes).unwrap()).unwrap_or_else(|e| panic!("{q}: {e}"));
        }
        assert_eq!(state(&self.leader), state(&self.follower), "after {q}");
        records.iter().map(Vec::len).sum()
    }
}

#[test]
fn a_follower_given_the_changes_holds_the_same_values() {
    let pair = Pair::new(Server::new(), Server::new());
    for q in [
        "t: ([] s: `a`b`c; n: 1 2 3; txt: (\"xx\"; \"yy\"; \"zzz\"); f: 1.5 -0.0 0n)",
        "`t insert (`d; 4; \"dd\"; 0.0)",
        "t,: ([] s: enlist `e; n: enlist 5; txt: enlist \"ee\"; f: enlist 5.0)",
        "update f: 0.0 from `t where n = 2",
        "update n: n * 10 from `t",
        "update m: n from `t",
        "delete m from `t",
        "delete from `t where s = `b",
        "w: ([] g: (1; \"a\"; `x; 2.5; 3); n: `s#til 5)",
        "delete from `w where n = 2",
        "kd: ([k: `a`b`c`d] v: 1 2 3 4)",
        "delete from `kd where k = `b",
        "dv: `g#`a`b`a`c`a",
        "dv: dv where dv <> `c",
        "dup: 1 1 1 2 1",
        "dup: dup where dup <> 2",
        "t: `s xasc t",
        "t: t",
        "k: ([sym: `a`b] v: 1 2)",
        "`k upsert (`c; 3)",
        "`k upsert (`a; 9)",
        "`k upsert ([] sym: `z`z`a; v: 1 0N 7)",
        "m: ([a: 1 2; b: `x`y] v: 10 20; w: 1.5 2.5)",
        "`m upsert ([] a: 2 3 3 1; b: `y`z`z`x; v: 0N 30 31 11; w: 9 0n 8.0 0n)",
        "`m insert (7; `x; 1; 1.0)",
        "m,: ([a: enlist 1; b: enlist `x] v: enlist 5; w: enlist 5.0)",
        "v: `s#til 10",
        "v,: 10 11",
        "v,: 0",
        "g: `g#`a`b`a",
        "g,: `b",
        "e: ()",
        "e,: 1",
        "e: e, `x",
        "l: (1 2; \"ab\")",
        "l,: enlist `g#`x`y`x",
        "l: (`s#1 2; \"ab\"; `x`y`x)",
        "d: `a`b!1 2",
        "d,: (enlist `c)!enlist 3",
        "empty: 0#0",
        "empty,: 7 8",
        "empty: 0#0",
        "n: 5",
        "n: 6",
        "f: {x + 1}",
        "f: {x + 2}",
        "t: 5",
        "a:: n + 1",
        "a: 1 2",
        "a,: 3",
    ] {
        pair.run(q);
    }
}

#[test]
fn appends_and_column_updates_log_only_what_changed() {
    let pair = Pair::new(Server::new(), Server::new());
    // 8 bytes a value: each column is about 800 kB, as is the vector
    let whole = pair.run("big: ([] a: 0.5 + til 100000; b: 1.5 + til 100000; c: 2.5 + til 100000); v: 0.5 + til 100000");
    assert!(whole > 3_000_000, "{whole}");
    for q in ["`big insert (1.0; 2.0; 3.0)", "v,: 4.0", "big,: ([] a: 1 2.0; b: 3 4.0; c: 5 6.0)"] {
        let size = pair.run(q);
        assert!(size < 200, "{q}: {size} bytes");
    }
    for q in ["delete from `big where a in 10.5 5000.5 99999.5", "v: v where v <> 7.5"] {
        let size = pair.run(q);
        assert!(size < 200, "{q}: {size} bytes");
    }
    // a few values of a column are replicated by themselves, as are the rows of a keyed table that are set
    let few = pair.run("update b: 0.0 from `big where a < 50");
    assert!((300..1500).contains(&few), "{few}");
    pair.run("keyed: ([id: til 100000] x: til 100000; y: 0.5 + til 100000)");
    let one_key = pair.run("`keyed upsert (7; 1; 2.0)");
    assert!(one_key < 200, "{one_key}");
    let one_column = pair.run("update b: b + 1 from `big");
    assert!((700_000..900_000).contains(&one_column), "{one_column}");
}

#[test]
fn a_value_read_from_a_file_is_logged_whole_before_it_is_changed() {
    let dir = tempfile::tempdir().unwrap();
    Server::new().connect_internal().eval(&format!("`:{}/prices set 1 2 3", dir.path().display().to_string().replace('\\', "/"))).unwrap();
    // the follower does not have the file
    let pair = Pair::new(Server::open(dir.path()).unwrap(), Server::new());
    assert_eq!(pair.leader.connect_internal().eval("count prices").unwrap(), kixdb::K::Long(3));
    pair.leader.connect_internal().eval("prices,: 4").unwrap();
    let records = std::mem::take(&mut *pair.records.0.lock().unwrap());
    let rec: WalRecord = postcard::from_bytes(&records[0]).unwrap();
    assert!(matches!(rec.ops[..], [LogOp::SetVar { .. }]), "{rec:?}");
    pair.follower.apply_replicated(&rec).unwrap();
    assert!(pair.run("prices,: 5") > 0);
}

#[test]
fn values_a_new_member_holds_are_logged_whole_once() {
    let leader = Server::new();
    leader.connect_internal().eval("x: 1 2").unwrap();
    // the follower starts without it
    let pair = Pair::new(leader, Server::new());
    pair.leader.log_values_whole();
    let (first, second) = (pair.run("x,: til 1000"), pair.run("x,: 3"));
    assert!(first > 1000 && second < 100, "{first} {second}");
}

#[test]
fn the_log_replays_changes_and_refuses_them_without_their_value() {
    let dir = tempfile::tempdir().unwrap();
    let options = StorageOptions { sync_wal: false, checkpoint_every: 0 };
    let server = Server::open_with(dir.path(), options).unwrap();
    let mut c = server.connect_internal();
    for q in ["t: ([] s: `a`b; n: 1 2)", "`t insert (`c; 3)", "update n: 2 * n from `t", "x: 1 2", "x,: 3"] {
        c.eval(q).unwrap();
    }
    // as after a crash: the log, before the server closes and folds it into a snapshot
    let copy = tempfile::tempdir().unwrap();
    std::fs::copy(dir.path().join("kix.wal"), copy.path().join("kix.wal")).unwrap();
    assert_eq!(state(&server), state(&Server::open(copy.path()).unwrap()));

    // a change to a value the engine does not have is an error, not a wrong value
    let pair = Pair::new(Server::new(), Server::new());
    pair.run("x: 1 2");
    pair.leader.connect_internal().eval("x,: 3").unwrap();
    let rec: WalRecord = postcard::from_bytes(&pair.records.0.lock().unwrap()[0]).unwrap();
    assert!(Server::new().apply_replicated(&rec).is_err());
    let stale = Server::new();
    stale.connect_internal().eval("x: enlist 1").unwrap();
    assert!(stale.apply_replicated(&WalRecord { ts: 100, ops: rec.ops }).is_err());
}
