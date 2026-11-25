//! Single engine versus cluster: every statement must give the same answer, by the plan expected for it.
mod common;
use common::{market, same, Pair};
use kix_control::Cluster;
use kixdb::ipc::{IpcClient, IpcConfig, Listener};
use kixdb::K;

#[test]
fn tables_are_spread_over_the_shards_as_declared() {
    let mut p = market(3);
    for table in ["trade", "quote"] {
        let rows = p.shards.rows(table);
        assert_eq!(rows.iter().sum::<usize>() as i64, p.single.eval(&format!("count {table}")).unwrap().as_i64().unwrap());
        assert!(rows.iter().filter(|&&r| r > 0).count() > 1, "{table} is on one shard only: {rows:?}");
    }
    assert_eq!(p.shards.rows("inst"), vec![5, 5, 5]);
    p.plan("count trade", "count of trade");
    p.plan("cols trade", "schema");
    p.plan("keys acct", "schema");
    p.plan("0 ! meta inst", "schema");
    p.plan("type trade", "schema");
    p.check("tables[]");
    p.check("n");
}

#[test]
fn queries_are_pushed_to_the_shards() {
    let mut p = market(3);
    // routed to one shard, where even order-dependent functions are exact
    p.plan("select from trade where sym = `AAPL", "route");
    p.plan("select time, d: deltas price, m: 5 mavg price from trade where sym = `MSFT", "route");
    p.plan("select o: first price, c: last price by 5 xbar time.minute from trade where sym = `IBM", "route");
    p.plan("exec price from trade where sym = `GOOG, price > avg price", "route");
    p.plan("s: `AMD; select from trade where sym in enlist s", "route");

    // row by row, merged in the original order
    p.plan("select from trade", "merged in order");
    p.plan("trade", "merged in order");
    p.plan("select time, price from trade where price > 110, size < 150", "merged in order");
    p.plan("select time, notional: price * size from trade where sym in `AAPL`MSFT`GOOG`IBM", "merged in order");
    p.plan("exec price from trade where size > 50", "merged in order");
    p.plan("exec price, size from trade where size > 150", "merged in order");
    p.plan("select distinct sym, ex from trade", "merged in order");
    p.plan("select[5] from trade", "merged in order");
    p.plan("select[-3] price from trade where size > 20", "merged in order");
    p.plan("select from trade where price = (max; price) fby sym", "merged in order");
    p.plan("select from trade where price > 1000", "merged in order");
    p.plan("select from acct", "merged in order");
    p.plan("select from trade where time within 09:31:00.000 09:32:00.000", "merged in order");

    // a cast of a constant string is a constant: it filters on the shards and routes like a literal
    p.plan("select from trade where sym = `$\"AAPL\"", "route");
    p.plan("exec price from trade where ex in (`N;`$\"B\"), ex <> `$\"Q\"", "merged in order");
    p.plan("select sum size, avg price from trade where ex = `$\"N\"", "two-phase");
    p.plan("select time from trade where price > \"F\"$\"110\"", "merged in order");

    // grouped by the distribution column: no group spans shards
    p.plan("select vwap: size wavg price, hi: max price, n: count i by sym from trade", "grouped by the distribution column");
    p.plan("select o: first price, h: max price, l: min price, c: last price by sym, bar: 5 xbar time.minute from trade", "grouped by");
    p.plan("select by sym from trade", "grouped by");
    p.plan("select price, med size by sym from trade where size > 10", "grouped by");
    p.plan("exec max price by sym from trade", "grouped by");
    p.plan("exec hi: max price, lo: min price by sym from trade", "grouped by");
    p.plan("update r: 1 from select last price by sym from trade", "grouped by");
    p.plan("exec max price from select last price by sym from trade", "grouped by");
    p.plan("select[2] sum size by sym from trade", "grouped by");
    p.plan("select sum size by sym from trade where price > 1000", "grouped by");

    // partial aggregates merged on the control node
    p.plan("select sum size, avg price by ex from trade", "two-phase");
    p.plan(
        "select s: sum size, a: avg price, mn: min price, mx: max price, d: dev price, v: var price, sv: svar price, sd: sdev size, \
         w: size wavg price, ws: size wsum price, c: count i, p: prd 1 + size mod 2 from trade",
        "two-phase",
    );
    p.plan("select spread: (max price) - min price, mid: 0.5 * (max price) + min price by ex from trade", "two-phase");
    p.plan("select sum size by 5 xbar time.minute from trade", "two-phase");
    p.plan("select n: count i, max time by ex, trader from trade where size > 100", "two-phase");
    p.plan("exec sum size from trade", "two-phase");
    p.plan("exec sum size by ex from trade", "two-phase");
    p.plan("exec s: sum size, a: avg price from trade", "two-phase");
    p.plan("select max sym, min ex from trade", "two-phase");
    p.plan("select max price, avg size from trade where price > 1000", "two-phase");
    p.plan("select sum size by ex from trade where price > 1000", "two-phase");
    p.plan("exec avg price from trade where sym in `AAPL`MSFT`GOOG`IBM`NVDA", "two-phase");

    // only replicated tables: any one shard
    p.plan("select from inst where lot > 20", "replicated");
    p.plan("select sum lot by sector from inst", "replicated");
    p.plan("exec i from inst where sector = `tech", "replicated");
}

#[test]
fn joins_run_where_the_rows_are() {
    let mut p = market(3);
    p.plan("trade lj inst", "merged in order");
    p.plan("trade ij inst", "merged in order");
    p.plan("select sum size by sector from trade lj inst", "two-phase");
    p.plan("select sum size by sym, sector from trade lj inst", "grouped by");
    p.plan("ej[`sym; trade; ([] sym: `AAPL`AAPL`MSFT; tag: 1 2 3)]", "gather");
    p.check("tags: ([] sym: `AAPL`AAPL`MSFT; tag: 1 2 3)");
    p.check(".kix.dist[`tags; `replicated]");
    p.plan("ej[`sym; trade; tags]", "merged in order");

    // both sharded by `sym`: as-of and window joins run shard by shard
    let plan = p.check("aj[`sym`time; trade; quote]");
    assert!(plan.contains("merged in order") && !plan.contains("shuffle") && !plan.contains("broadcast"), "{plan}");
    p.check("aj0[`sym`time; trade; quote]");
    p.plan("select sym, price, bid from aj[`sym`time; trade; quote] where price > bid", "merged in order");
    p.plan("select max ask, min bid by sym from aj[`sym`time; trade; quote]", "grouped by");
    p.plan("wj[(-5000 0) +\\: trade.time; `sym`time; trade; (quote; (max; `bid); (min; `ask))]", "merged in order");
    p.plan("wj1[(trade.time - 2000; trade.time); `sym`time; trade; (quote; (avg; `bid))]", "merged in order");
    p.plan("select from aj[`sym`time; trade; quote] where sym = `NVDA", "route");

    // keyed by another column: a small table is broadcast, a large one shuffled
    p.plan("trade lj acct", "broadcast acct");
    p.plan("select sum size by desk from trade ij acct", "broadcast acct");
    p.cluster.set_broadcast_limit(0);
    p.plan("trade lj acct", "shuffle trade by trader");
    p.plan("trade ij acct", "shuffle trade by trader");
    p.plan("select sum size by desk from trade lj acct", "shuffle trade by trader");
    p.plan("select count i by trader from trade lj acct", "grouped by");

    // quotes spread by time: they are shuffled to meet the trades
    p.check(".kix.dist[`quote; `sharded; `time]");
    p.plan("aj[`sym`time; trade; quote]", "shuffle quote by sym");
    p.plan("wj[(-5000 0) +\\: trade.time; `sym`time; trade; (quote; (max; `bid))]", "shuffle quote by sym");
    p.cluster.set_broadcast_limit(10_000);
    p.plan("aj[`sym`time; trade; quote]", "broadcast quote");
    // temporary tables are gone
    for node in &p.shards.nodes {
        let left = node.server.read_workspace(|ws| ws.vars.keys().filter(|k| k.to_string().starts_with(".kix.tmp")).count());
        assert_eq!(left, 0);
    }
}

#[test]
fn everything_else_is_gathered_and_still_exact() {
    let mut p = market(3);
    p.plan("select med price from trade", "gather trade");
    p.plan("select med price by ex from trade", "gather trade (2 column(s)");
    p.plan("select from trade where price > avg price", "gather trade");
    p.plan("select d: deltas price from trade where size > 10", "gather trade (1 column(s), 1 filter(s)");
    p.plan("select i, price from trade where sym = `IBM", "gather");
    p.plan("update m: 3 mavg price by sym from trade", "gather trade");
    p.check("select first price, last size by ex from trade");
    p.check("exec first price from trade");
    p.check("select distinct sum size by ex from trade");
    p.check("first trade");
    p.check("trade 0 1 2");
    p.check("last exec price from trade");
    p.check("exec distinct ex from trade");
    p.check("exec last price, first size from trade");
    p.check("max trade.price");
    p.check("`price xasc trade");
    p.check("select from `size xdesc trade where sym = `AAPL");
    p.check("value \"count trade\"");
    p.check("f: {[s] select from trade where sym = s}; f `AAPL");
    p.check("{count trade} each 1 2");
    p.check("v:: select sum size by sym from trade");
    p.check("v");
    p.check("count select from trade where size > 10");
    p.check("1 + exec sum size from trade");
    p.check("(count trade; count quote)");
    p.check("0 ! select sum size by sym from trade");
    p.check("exec price from trade where size = max size");
    p.check("x: select from trade where sym = `AAPL; count x");
    p.check("x");
    // subqueries
    p.plan("select from trade where sym in exec sym from inst where sector = `tech", "merged in order");
    p.plan("select from trade where price > exec avg price from trade", "merged in order");
    // errors are those of one engine
    p.check("select nosuch from trade");
    p.check("select price + `a from trade");
    p.check("trade lj trade");
    p.check("sum exec sym from trade");
}

#[test]
fn writes_change_the_cluster_as_they_change_one_engine() {
    let mut p = market(3);
    let all = |p: &mut Pair, t: &str| {
        p.check(t);
        p.check(&format!("meta {t}"));
        p.check(&format!("count {t}"));
    };
    p.plan("`trade insert (09:40:00.000; `AAPL; 150.5; 10; `N; `ann)", "insert: 1 row(s)");
    p.plan("`trade insert (09:41:00.000 09:41:01.000; `MSFT`TSLA; 150.5 151.5; 10 20; `N`Q; `bob`dan)", "insert: 2 row(s)");
    p.plan("`trade upsert ([] time: enlist 09:42:00.000; sym: enlist `IBM; price: enlist 99.5; size: enlist 5; ex: enlist `B; trader: enlist `cat)", "upsert");
    p.plan("insert[`trade; (09:43:00.000; `AMD; 98.0; 7; `B; `cat)]", "insert");
    p.check("upd: insert");
    p.plan("upd[`trade; (09:44:00.000; `AMD; 97.0; 8; `Q; `ann)]", "insert");
    all(&mut p, "trade");
    // wrong types and shapes are refused as on one engine, and change nothing
    p.check("`trade insert (1; 2)");
    p.check("`trade insert (09:45:00.000; `AMD; `high; 8; `Q; `ann)");
    all(&mut p, "trade");

    // a sharded keyed table: new keys are appended in order, old ones updated where they are
    p.plan("`acct upsert ([] trader: `dan`ann`eve; desk: `west`north`south)", "upsert");
    all(&mut p, "acct");
    p.check("`acct insert (`ann; `east)");
    p.plan("`acct insert (`fay; `east)", "insert");
    all(&mut p, "acct");
    // a replicated keyed table
    p.check("`inst upsert (`TSLA; `auto; 100)");
    all(&mut p, "inst");

    p.plan("update size: size * 2 from `trade where sym = `AAPL", "run on every shard");
    p.plan("update price: price + 1.0, size: size + 1 from `trade where size > 100", "run on every shard");
    p.plan("update m: 2 mavg price by sym from `trade", "run on every shard");
    p.plan("update flag: 1b from `trade", "run on every shard");
    all(&mut p, "trade");
    p.plan("delete from `trade where size < 20", "run on every shard");
    p.plan("delete flag, m from `trade", "run on every shard");
    all(&mut p, "trade");
    // these need all rows at once
    p.plan("update price: avg price from `trade where ex = `N", "gather trade");
    p.plan("delete from `trade where price = max price", "gather trade");
    p.plan("update big: 1b from `trade where size > 100", "gather trade");
    p.check("update sym: `XXX from `trade where sym = `AMD");
    all(&mut p, "trade");
    p.check("update lot: 2 * lot from `inst");
    all(&mut p, "inst");
    // an error leaves every shard unchanged
    p.check("update price: price + `a from `trade");
    all(&mut p, "trade");

    p.check("trade: select from trade where sym <> `IBM");
    all(&mut p, "trade");
    p.check("trade,: ([] time: enlist 10:00:00.000; sym: enlist `IBM; price: enlist 1.5; size: enlist 1; ex: enlist `N; trader: enlist `ann; big: enlist 0b)");
    p.check("`price xasc `trade");
    p.check("trade: `time xasc trade");
    all(&mut p, "trade");
    p.check("trade: 0#trade");
    all(&mut p, "trade");
    p.check("`trade insert (10:01:00.000; `AAPL; 2.5; 3; `N; `ann; 1b)");
    all(&mut p, "trade");
    // a table created empty takes its types from the first rows
    p.check("journal: ([] sym: `symbol$(); note: ())");
    p.check("`journal insert (`a; \"first\")");
    p.check("`journal insert (`b; \"second\")");
    all(&mut p, "journal");
    // a table that becomes something else leaves the shards
    p.check("journal: 42");
    p.check("journal");
    assert_eq!(p.shards.rows("journal"), vec![0, 0, 0]);
}

#[test]
fn distribution_modes_and_namespaces() {
    let mut p = market(3);
    p.session.eval("DISTRIBUTE trade PARTITIONED BY time").unwrap();
    p.check("trade");
    p.plan("select sum size by sym from trade", "two-phase");
    p.plan("select from trade where time = 09:30:00.000", "route");
    p.session.eval("DISTRIBUTE trade REPLICATED").unwrap();
    assert_eq!(p.shards.rows("trade"), vec![240, 240, 240]);
    p.plan("select med price from trade", "replicated");
    p.session.eval("DISTRIBUTE trade BY ex").unwrap();
    p.plan("select sum size by ex from trade", "grouped by");
    p.check("trade");
    // a column that does not exist, and a keyed table by a value column
    assert_eq!(p.session.eval(".kix.dist[`trade; `sharded; `nosuch]").unwrap_err().signal, "domain");
    assert_eq!(p.session.eval("DISTRIBUTE acct BY desk").unwrap_err().signal, "domain");
    p.check("trade");

    // partitioned by date, as an on-disk history would be
    for s in [
        ".kix.dist[`daily; `partitioned; `date]",
        "daily: ([] date: 2026.11.20 + (til 30) mod 5; sym: `a`b`c ((til 30) mod 3); px: 0.5 * til 30)",
    ] {
        p.check(s);
    }
    p.plan("select from daily where date = 2026.11.22", "route");
    p.plan("select max px by date from daily", "grouped by");
    p.plan("select max px by sym from daily where date within 2026.11.21 2026.11.23", "two-phase");

    p.check("\\d .ref");
    p.check("book: ([] sym: `a`b`c`d; q: 1 2 3 4)");
    p.plan("select sum q by sym from book", "grouped by");
    p.check("\\d .");
    p.check("count .ref.book");
    p.check("select from .ref.book where q > 1");
}

#[test]
fn explain_describes_without_running() {
    let mut p = market(2);
    let lines = p.session.explain("select from trade where sym = `AAPL; `trade insert 1; select med price from trade; update size: 1 from `trade; 1+1").unwrap();
    let wanted = ["route", "split over the shards", "gathering trade", "every shard", "control node"];
    for (line, how) in lines.iter().zip(wanted) {
        assert!(line.contains(how), "{lines:?}");
    }
    p.check("count trade");
    assert!(p.session.eval("SHARDS").unwrap().to_string().contains("single"));
    assert!(p.session.eval("EXPLAIN select from trade").unwrap().to_string().contains("merged in order"));
    assert!(p.cluster.prometheus().contains("kixdb_shards_up 2"));
}

#[test]
fn clients_see_one_database_over_ipc() {
    let p = market(3);
    let gateway = Listener::bind(p.cluster.gateway(), ("127.0.0.1", 0), IpcConfig::default()).unwrap();
    let mut client = IpcClient::connect_addr(&gateway.local_addr().to_string(), "alice", "").unwrap();
    assert_eq!(client.query_str("count trade").unwrap(), K::Long(240));
    // functional messages, as a tickerplant sends them
    let msg = kixdb::Server::new().connect().eval("(`insert; `trade; (10:00:00.000; `AAPL; 1.5; 3; `N; `ann))").unwrap();
    client.query_k(&msg).unwrap();
    assert_eq!(client.query_str("count trade").unwrap(), K::Long(241));
    assert_eq!(client.query_str("exec last price from trade where sym = `AAPL").unwrap(), K::Float(1.5));
    assert_eq!(client.query_str("nosuch").unwrap_err().signal, "nosuch");
    // another q process
    let q = format!("h: hopen `:{}; r: h \"select sum size by ex from trade\"; hclose h; r", gateway.local_addr());
    let remote = kixdb::Server::new().connect().eval(&q).unwrap();
    assert!(same(&remote, &client.query_str("select sum size by ex from trade").unwrap()));
}

#[test]
fn concurrent_sessions() {
    let p = market(3);
    std::thread::scope(|scope| {
        for w in 0..4 {
            let cluster = p.cluster.clone();
            scope.spawn(move || {
                let mut s = cluster.session();
                for i in 0..15 {
                    s.eval(&format!("`trade insert (10:00:00.000; `W{w}; {i}.0; 1; `N; `ann)")).unwrap();
                    assert_eq!(s.eval(&format!("count select from trade where sym = `W{w}")).unwrap(), K::Long(i + 1));
                    // a gathering statement among the reads and writes
                    s.eval("select med price from trade").unwrap();
                    s.eval("trade lj acct").unwrap();
                }
            });
        }
    });
    assert_eq!(p.cluster.session().eval("count trade").unwrap(), K::Long(300));
}

#[test]
fn state_file_picks_the_cluster_up_again() {
    let shards = common::Shards::start(2);
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("cluster.state");
    {
        let cluster = Cluster::open(&state, &shards.config()).unwrap();
        let mut s = cluster.session();
        s.eval("limit: 10; f: {x * limit}").unwrap();
        s.eval("t: ([] sym: `a`b`c`d`e; v: 1 2 3 4 5)").unwrap();
        s.eval("v:: exec sum v from t").unwrap();
    }
    let cluster = Cluster::open(&state, &kix_shared_ces::ClusterConfig::default()).unwrap();
    // a second control node cannot open the same locked state file simultaneously
    assert!(Cluster::open(&state, &kix_shared_ces::ClusterConfig::default()).is_err());
    let mut s = cluster.session();
    assert_eq!(s.eval("f 3").unwrap(), K::Long(30));
    // new rows are numbered after the old ones
    s.eval("`t insert (`f`a; 6 7)").unwrap();
    assert_eq!(s.eval("exec v from t").unwrap().to_string(), "1 2 3 4 5 6 7");
    assert_eq!(s.eval("v").unwrap(), K::Long(28));
    drop(s);
    drop(cluster);

    // corrupting `cluster.state` is caught by its CRC32 checksum
    let mut bytes = std::fs::read(&state).unwrap();
    *bytes.last_mut().unwrap() ^= 0xff;
    std::fs::write(&state, bytes).unwrap();
    assert!(Cluster::open(&state, &kix_shared_ces::ClusterConfig::default()).is_err());
}

#[test]
fn broadcast_and_shuffle_joins_do_not_write_to_shard_wal() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let d0 = tempfile::tempdir().unwrap();
    let d1 = tempfile::tempdir().unwrap();
    let s0_srv = kixdb::Server::open(d0.path()).unwrap();
    let s1_srv = kixdb::Server::open(d1.path()).unwrap();
    let sh0 = rt.block_on(kix_storage::spawn(kix_storage::ShardNode::new(0, s0_srv.clone()), "127.0.0.1:0".parse().unwrap())).unwrap();
    let sh1 = rt.block_on(kix_storage::spawn(kix_storage::ShardNode::new(1, s1_srv.clone()), "127.0.0.1:0".parse().unwrap())).unwrap();
    let cfg = kix_shared_ces::ClusterConfig {
        shards: vec![
            kix_shared_ces::ShardInfo { id: 0, addr: sh0.addr.to_string() },
            kix_shared_ces::ShardInfo { id: 1, addr: sh1.addr.to_string() },
        ],
    };
    let cluster = Cluster::connect(&cfg).unwrap();
    let mut s = cluster.session();
    s.eval("orders: ([] oid: 1 2 3 4; cid: 10 20 10 30; amt: 50 60 70 80)").unwrap();
    s.eval("cust: ([cid: 10 20 30] tier: `gold`silver`gold)").unwrap();

    let wal_before = (s0_srv.storage_stats().wal_records, s1_srv.storage_stats().wal_records);
    // Broadcast join (`orders` sharded by `oid`, `cust` sharded by `cid` -> `cust` moved to `.kix.tmp.*`)
    assert_eq!(s.eval("exec sum amt from (orders lj cust) where tier = `gold").unwrap(), K::Long(200));
    // Force shuffle by setting broadcast limit to 1 row
    cluster.set_broadcast_limit(1);
    assert_eq!(s.eval("exec sum amt from (orders lj cust) where tier = `silver").unwrap(), K::Long(60));
    let wal_after = (s0_srv.storage_stats().wal_records, s1_srv.storage_stats().wal_records);
    assert_eq!(wal_before, wal_after, "read-only broadcast/shuffle joins must not append to shard WAL");
}

#[test]
fn a_join_that_overwrites_the_distribution_column_is_not_pushed_down() {
    let mut p = market(3);
    p.check("trade: update sector: ex from trade");
    p.check(".kix.dist[`trade; `sharded; `sector]");
    p.plan("select sum size by sector from trade", "grouped by");
    // `inst` has a `sector` of its own, which replaces the one trades are placed by
    p.check("select sum size by sector from trade lj inst");
    p.check("select sum size by sector from trade ij inst");
    p.check("select from trade lj inst where sector = `tech");
    p.check("select sum size by sector from ej[`sym; trade; 0 ! inst]");
}
