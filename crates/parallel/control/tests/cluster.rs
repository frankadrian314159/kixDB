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
    // every kind of merge over many groups of two keys
    p.plan(
        "select s: sum size, a: avg price, d: dev price, v: svar price, w: size wavg price, ws: size wsum price, c: count i, \
         p: prd 1 + size mod 2, hi: max sym by trader, ex from trade",
        "two-phase",
    );
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
    // replies large enough to be unpacked away from the thread that gathers them
    p.check(".kix.dist[`wide; `sharded; `a]");
    p.check("wide: ([] a: til 60000; b: reverse til 60000)");
    p.plan("select from wide", "merged in order");
    p.plan("select sum b by a from wide where b > 10", "grouped by the distribution column");
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
    // `i` numbers the rows of the whole table, also after a filter
    p.check("exec i from trade where size > 100");
    p.check("exec i from trade where size > 100, i < 50");
    p.check("select i, price from trade where size > 100, ex = `N");
    p.check("select f: first i, s: sum i, n: count i by ex from trade where size > 100");
    p.check("exec last i by sym from trade where price > 110");
    p.check("exec i from inst where lot > 20");
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
fn tables_named_by_symbols_are_changed_on_the_shards() {
    let mut p = market(3);
    for statement in [
        "`price xasc `trade",
        "select from trade",
        "`sym xkey `inst",
        "inst",
        "bump: {[t] update size: size + 1 from t}",
        "bump `trade",
        "exec sum size from trade",
        "`time xasc `trade",
        "trade",
        "quote: 0#quote",
        "`quote insert ([] time: 2#09:30:00.000; sym: `IBM`AMD; bid: 1 2f; ask: 3 4f)",
        "trade,: `time`sym`price`size`ex`trader!(09:45:00.000; `IBM; 1.5; 3; `N; `ann)",
        "select from trade where sym = `IBM",
        "quote",
    ] {
        p.check(statement);
    }
}

#[test]
fn foreign_keys_and_enumerations_work_across_the_shards() {
    let mut p = market(3);
    p.check("kt: ([eid: 1001 1002 1003] name: `Dent`Beeblebrox`Prefect; iq: 98 42 126)");
    p.session.eval("DISTRIBUTE kt REPLICATED").unwrap();
    for statement in [
        "d: ([] sym: 30#`a`b`c`d`e; eid: `kt$30#1003 1001 1002; sc: til 30)",
        "select from d",
        "meta d",
        "select eid.name, sc from d",
        "select sum sc by eid.name from d",
        "select sc from d where eid.iq > 90",
        "select sum eid.iq by sym from d",
        "`d insert (`a; 1002; 100)",
        "`d insert (`a; 1009; 101)",
        "d ,: `sym`eid`sc!(`b; 1001; 102)",
        "update eid: 1001 from `d where sc = 100",
        "update eid: 1009 from `d where sc = 100",
        "select eid.name, sc from d where sc >= 28",
        // an enumeration of a list, which the shards do not have
        "venue: `N`Q`L",
        "e: ([] sym: 6#`a`b`c; x: `venue$6#`N`Q)",
        "`e insert (`a; `L)",
        "`e insert (`a; `Z)",
        "update x: `Q from `e where sym = `a",
        "select from e",
        "meta e",
    ] {
        p.check(statement);
    }
    assert!(p.shards.rows("d").iter().filter(|&&r| r > 0).count() > 1, "d is on one shard only");
    // (the changes that should be made were)
    assert_eq!(p.session.eval("(count d; count e; exec sum eid from d where sc = 100)").unwrap().to_string(), "32 7 1001");
    // a shard cannot follow a key into a table that it has only part of
    p.session.eval("DISTRIBUTE kt BY eid").unwrap();
    assert_eq!(p.session.eval("select eid.name from d").unwrap_err().signal, "nyi");
    assert_eq!(p.session.eval("`d insert (`a; 1002; 103)").unwrap_err().signal, "nyi");
    p.check("select eid, sc from d");
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
fn callbacks_a_cluster_cannot_make_are_refused() {
    let p = market(2);
    let listener = Listener::bind(p.cluster.gateway(), "127.0.0.1:0", IpcConfig::default()).unwrap();
    let address = listener.local_addr().to_string();
    let mut client = IpcClient::connect_addr(&address, "alice", "").unwrap();
    // none is left in force for a client to rely on, however it is set
    for statement in [".z.pg: {'denied}", ".z.ps: {x}", "`.z.ts set {x}", ".z.vs: {[n; i] n}"] {
        assert_eq!(client.query_str(statement).err().map(|e| e.signal), Some("nyi".into()), "{statement}");
    }
    // nor does a node run shell commands
    assert_eq!(client.query_str("system \"echo hi\"").err().map(|e| e.signal), Some("access".into()));
    // the console's handles are accepted, though the node prints nothing for a client
    assert_eq!(client.query_str("-1 \"to the console\"").unwrap(), K::Int(-1));
    // nor is anything sent to a client
    assert_eq!(client.query_str("(neg .z.w) 1").err().map(|e| e.signal), Some("nyi".into()));
    assert_eq!(client.query_str("count trade").unwrap(), K::Long(240));
    // who connects is still checked
    client.query_str(".z.pw: {[user; password] user = `alice}").unwrap();
    assert!(IpcClient::connect_addr(&address, "alice", "").is_ok());
    assert!(IpcClient::connect_addr(&address, "mallory", "").is_err());
}

#[test]
fn what_a_statement_makes_on_the_control_node_is_kept_as_one_engine_keeps_it() {
    let mut p = market(2);
    for statement in [
        "x: 1; v:: x + count trade",
        "v",
        // what was set before a failure stays, in a function too
        "f: {y:: x; 'oops}; f 5",
        "y",
        // (a gathered table is held only while the statement runs)
        "z: med exec price from trade; z",
        "m: 0n; {m:: med trade`price}[]; m",
        "system \"x y\"; y",
        "x: 10; v",
        "`trade insert (`oops)",
        "update bad: price + sym from `trade",
        "cols trade",
    ] {
        p.check(statement);
    }
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

#[test]
fn streamed_two_phase_and_k_way_sorted_merges_match_one_engine() {
    let mut p = market(3);
    // Multi-batch (>8,192 groups per shard) two-phase aggregation across 1, 2, and string keys
    p.check(".kix.dist[`big; `sharded; `sid]");
    p.check(
        "big: ([] sid: til 30000; g1: (til 30000) mod 20000; g2: `a`b`c`d ((til 30000) mod 4); \
         v: 1.5 * 1 + (til 30000) mod 97; w: 1 + (til 30000) mod 13; tag: string (til 30000) mod 500)",
    );
    p.plan(
        "select s: sum w, vf: sum v, a: avg v, dv: dev v, sv: svar v, wv: w wavg v, mn: min v, mx: max g2, n: count i by g1 from big",
        "two-phase",
    );
    p.plan("select sum w, avg v, min sid, max sid by g1, g2 from big where w > 3", "two-phase");
    p.plan("select sum w, avg v by tag from big", "two-phase");

    // K-way sorted merge and pushed-down top-K (`sublist` + `xasc`/`xdesc`) on Ordered and ByKey queries
    p.plan("`price xasc trade", "k-way sorted merge");
    p.plan("`size xdesc `price xasc trade", "k-way sorted merge");
    p.plan("15 sublist `price xdesc `time xasc select sym, time, price, size from trade where size > 20", "k-way sorted merge");
    p.plan("5 sublist `s xdesc select s: sum size, hi: max price by sym from trade", "k-way sorted merge");
    p.plan("5 sublist `s xdesc 0! select s: sum size by sym from trade", "k-way sorted merge");
    p.plan("25 sublist `v xdesc `sid xasc big", "k-way sorted merge");
}

#[test]
fn verbs_applied_item_by_item_run_on_the_shards() {
    let mut p = Pair::new(3);
    for statement in [
        ".kix.dist[`cust; `sharded; `id]",
        "n: 60",
        "cust: ([] id: til n; grp: `a`b`c (til n) mod 3; name: n#(\"ann\";\"bo\";\"carla\";\"\"); \
         phone: (n#(\"13\";\"31\";\"23\";\"99\")) ,' \"-555-\" ,/: string til n; bal: -50 + (37 * til n) mod 101)",
        ".kix.dist[`none; `sharded; `id]",
        "none: 0 # cust",
    ] {
        p.single.eval(statement).unwrap_or_else(|e| panic!("{statement}: {e}"));
        p.session.eval(statement).unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    for table in ["cust", "none"] {
        for (columns, how) in [
            ("cc: `$2#'phone", "merged in order"),
            ("a: 2#'phone, b: 2_'phone, c: -3#'phone", "merged in order"),
            ("a: (phone ,\\: \"x\"), b: (\"+\" ,/: phone), c: (name ,' phone)", "merged in order"),
            ("n: count each phone, f: first each name, u: upper each name, r: reverse peach phone", "merged in order"),
            ("n: count'[phone], m: 2 + count each name, s: `$\"-\" vs/: phone", "merged in order"),
            ("n: sum count each phone, m: max count each name by grp", "two-phase"),
            ("n: sum count each phone", "two-phase"),
            ("n: count i by cc: `$2#'phone", "two-phase"),
            ("n: count each phone by id", "grouped by"),
            ("f: first each phone, g: upper first each name", "merged in order"),
            ("n: count i by f: first each phone", "two-phase"),
            ("m: max first each phone by grp", "two-phase"),
            ("distinct f: 2#'phone", "merged in order"),
        ] {
            p.plan(&format!("select {columns} from {table}"), how);
        }
        p.plan(&format!("exec `$2#'phone from {table}"), "merged in order");
        // (the first clause leaves most shards, or all, without rows for those that follow)
        for first in ["bal > -100", "id < 1", "id < 0"] {
            for then in [
                "(`$2#'phone) in `13`31",
                "(`$2#'phone) like \"1*\"",
                "0 < count each phone",
                "(5 xbar count each phone) within 5 10",
                "\"1\" = first each phone",
                "\"13\" ~/: 2#'phone",
                "name ~\\: \"ann\"",
                "\"5\" in/: phone",
            ] {
                p.plan(&format!("select id, cc: 2#'phone from {table} where {first}, {then}"), "merged in order");
                p.check(&format!("select n: sum count each phone, hi: max count each name from {table} where {first}, {then}"));
            }
        }
    }
    // TPC-H Q22's shape: no table comes to the control node
    for statement in [
        "avg_bal: exec avg bal from (select bal, cc: `$2#'phone from cust where bal > 0) where cc in `13`31",
        "select numcust: count i, total: sum bal by cc from (select bal, cc: `$2#'phone from cust where bal > avg_bal, not id in 1 2 3) where cc in `13`31",
    ] {
        let plan = p.check(statement);
        assert!(plan.contains("merged in order") && !plan.contains("gather cust"), "{statement}: {plan}");
    }
    // what needs other rows, or a function of the user's, is computed on the control node
    for columns in ["(name ,\\: phone)", "(name ,/: phone)", "{count x} each phone", "(phone ,' name 0)", "(,':) name", "count each distinct name"] {
        p.plan(&format!("select {columns} from cust"), "gather cust");
    }
    // changes: rows are chosen on the shards, new columns are made here, so that every shard gets their types
    p.plan("delete from `cust where \"99\" ~/: 2#'phone", "update / delete run on every shard");
    p.check("update len: count each phone from `cust where id < 1");
    p.check("update len: count each phone from `cust");
    p.check("update bal: 0 from `cust where name ~\\: \"bo\"");
    p.check("`cust insert (100; `a; \"zed\"; \"13-000\"; 5; 6)");
    p.check("cust");
}

#[test]
fn distinct_rows_are_dropped_on_the_shards() {
    let mut p = Pair::new(3);
    for statement in [
        ".kix.dist[`t; `sharded; `id]",
        "n: 90",
        "t: ([] id: til n; s: `c`a`b`a (til n) mod 4; v: n#3 1 2 3 1; f: n#0n 1.5 -0.0 0.0 2.5; d: 2026.01.01 + (til n) mod 3; \
         flag: n#10b; name: n#(\"bo\";\"ann\";\"\";\"x\"); g: n#(1; `a; \"xy\"; 1 2; ()); sorted: `s#(til n) div 30)",
        ".kix.dist[`none; `sharded; `id]",
        "none: 0 # t",
        ".kix.dist[`kt; `sharded; `id]",
        "kt: ([id: til 20] s: 20#`b`a)",
        ".kix.dist[`inst; `replicated]",
        "inst: ([] s: `c`a`b`a; w: 1 2 3 2)",
    ] {
        p.single.eval(statement).unwrap_or_else(|e| panic!("{statement}: {e}"));
        p.session.eval(statement).unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    let pushed = "distinct rows merged in order";
    for table in ["t", "none"] {
        // (the last two filters leave most shards, or all, without rows)
        for filter in ["", " where v > 1", " where id < 1", " where id < 0"] {
            for column in ["s", "v", "f", "d", "flag", "name", "g", "sorted", "2#'name", "v * 10", "count each name", "first each name"] {
                p.plan(&format!("distinct exec {column} from {table}{filter}"), pushed);
                p.plan(&format!("exec distinct {column} from {table}{filter}"), pushed);
                p.plan(&format!("select distinct c: {column} from {table}{filter}"), pushed);
                p.check(&format!("attr distinct exec {column} from {table}{filter}"));
                p.check(&format!("meta select distinct c: {column} from {table}{filter}"));
            }
            for columns in ["s, v", "s, f, d", "name, g", "s, c: v, d", "s, s", "sorted, flag"] {
                p.plan(&format!("select distinct {columns} from {table}{filter}"), pushed);
                p.plan(&format!("distinct select {columns} from {table}{filter}"), pushed);
            }
        }
    }
    p.plan("select[3] distinct s from t", pushed);
    p.plan("select[-2] distinct s, v from t", pushed);
    p.plan("select distinct from t where v > 1", "merged in order");
    p.plan("select distinct s from t where v = (max; v) fby id", pushed);
    p.plan("distinct exec s from kt", pushed);
    p.plan("select distinct s from kt", pushed);
    p.check("distinct select from kt");
    p.check("select distinct from kt");
    // one shard answers whole
    p.plan("distinct exec s from t where id = 7", "route");
    p.plan("exec distinct s from t where id = 7", "route");
    p.plan("exec distinct s from inst", "replicated");
    p.plan("distinct select s, w from inst", "replicated");
    // as part of a larger statement
    p.plan("x: distinct exec s from t; count x", "control");
    p.plan("count distinct exec s from t", pushed);
    p.plan("select from inst where s in distinct exec s from t", "replicated");
    p.plan("select id from t where s in exec distinct s from t where v > 2", "merged in order");
    // what is not the distinct rows of one query is done as before
    p.check("distinct exec s, v from t");
    p.check("exec distinct s, v from t");
    p.check("distinct select[3] s from t");
    p.check("exec distinct s by v from t");
    p.check("distinct select s by v from t");
    p.check("exec distinct sum v from t");
    p.check("distinct exec distinct s from t");
    p.check("exec distinct nosuch from t");
}

#[test]
fn general_columns_stay_general_where_rows_share_a_type() {
    let mut p = Pair::new(3);
    for statement in [
        ".kix.dist[`t; `sharded; `id]",
        "t: ([] id: til 12; s: 12#`a`b; g: 12#(1; `a; \"xy\"))",
        // (each of these rows is on a shard of its own, which so holds one type)
        ".kix.dist[`u; `sharded; `id]",
        "u: ([] id: 0 1; g: (1; `a))",
    ] {
        p.single.eval(statement).unwrap_or_else(|e| panic!("{statement}: {e}"));
        p.session.eval(statement).unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    for code in [
        "exec g from t where id < 1",
        "select g from t where id in 0 3 6 9",
        "exec g from `id xdesc select from t where id in 0 3",
        "x: select from t where id in 0 3; x`g",
        "exec g by s from t where id in 0 6",
        "select count i by g from t where id in 0 3 6 9",
        "select count i by g from u",
        "exec g from u where id = 1",
    ] {
        p.check(code);
    }
}

#[test]
fn rows_of_one_type_can_be_added_to_mixed_columns() {
    let mut p = Pair::new(3);
    for code in [
        ".kix.dist[`u; `sharded; `id]",
        // (two shards hold one type each, and one holds no row)
        "u: ([] id: 0 1; g: (1; `a))",
        "`u insert (0; 2)",
        "`u insert (0; `b)",
        "`u insert (3 4 5; 6 7 8)",
        "`u insert (3 4 5; `c`d`e)",
        "u,: ([] id: 6 7; g: 1 2)",
        "u",
        "delete from `u where id > 1",
        "`u insert (2; 9)",
        "`u insert (2; `f)",
        "u",
        ".kix.dist[`kt; `sharded; `id]",
        "kt: ([id: 0 1] g: (1; `a))",
        "`kt upsert (0; 2)",
        "`kt upsert (0; `b)",
        "`kt upsert (3 4 5; 6 7 8)",
        "`kt upsert (3 4 5; `c`d`e)",
        "kt",
        ".kix.dist[`st; `sharded; `id]",
        "st: ([] id: 0 1; g: (\"ab\"; \"cd\"))",
        "`st insert (3 4 5; 6 7 8)",
        "`st insert (3 4 5; `c`d`e)",
        "st",
        // an empty column takes the type of what is added
        ".kix.dist[`e; `sharded; `id]",
        "e: ([] id: `long$(); g: ())",
        "`e insert (0 1; 2 3)",
        "`e insert (2; `a)",
        "meta e",
        "e",
    ] {
        p.check(code);
    }
}
