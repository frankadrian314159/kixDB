//! Tables placed by `date` and `sym` together.
mod common;
use common::{market, same, Pair};

const DATE: &str = "2026.11.23";

/// The market data spread over four days, `trade` and `quote` co-partitioned.
fn days(shards: u32) -> Pair {
    let mut p = market(shards);
    for s in [
        "trade: update date: 2026.11.23 + (til count trade) mod 4 from trade",
        "quote: update date: 2026.11.23 + (til count quote) mod 4 from quote",
        ".kix.dist[`trade; `partitioned; `date`sym]",
        ".kix.dist[`quote; `partitioned; `date`sym]",
    ] {
        p.check(s);
    }
    p
}

#[test]
fn a_day_and_a_symbol_are_each_spread_but_their_pairs_stay_together() {
    let mut p = days(4);
    for filter in [format!("date = {DATE}"), "sym = `AAPL".into()] {
        let holding = p
            .shards
            .nodes
            .iter()
            .filter(|n| n.server.connect().eval(&format!("count select from trade where {filter}")).unwrap().as_i64().unwrap() > 0)
            .count();
        assert!(holding > 1, "{filter} is on one shard only");
    }
    // every (date, sym) pair is on exactly one shard
    let mut pairs = 0;
    for n in &p.shards.nodes {
        pairs += n.server.connect().eval("count select by date, sym from trade").unwrap().as_i64().unwrap();
    }
    assert_eq!(pairs, p.single.eval("count select by date, sym from trade").unwrap().as_i64().unwrap());
    p.check("trade");
    p.check("meta trade");
}

#[test]
fn queries_use_both_distribution_columns() {
    let mut p = days(4);
    let d = DATE;
    // both fixed: one shard, where order-dependent functions are exact
    p.plan(&format!("select from trade where date = {d}, sym = `AAPL"), "route");
    p.plan(&format!("select d: deltas price from trade where sym = `AAPL, date = {d}, price > avg price"), "route");
    p.plan(&format!("exec last price from trade where date in enlist {d}, sym in enlist `MSFT"), "route");
    // one fixed: not enough to route
    p.plan(&format!("select from trade where date = {d}"), "merged in order");
    p.plan("select from trade where sym = `AAPL", "merged in order");
    // groups that hold both columns, counting those the filters fix, stay on a shard
    p.plan("select o: first price, c: last price by date, sym from trade", "grouped by");
    p.plan("select last price by sym, date, ex from trade where size > 10", "grouped by");
    p.plan(&format!("select m: med price, last size by sym from trade where date = {d}"), "grouped by");
    p.plan("select last price by date from trade where sym = `IBM", "grouped by");
    p.plan(&format!("select from trade where date = {d}, size = (max; size) fby sym"), "merged in order");
    p.plan("select from trade where size = (max; size) fby ([] date; sym)", "merged in order");
    // groups that span shards
    p.plan("select sum size by sym from trade", "two-phase");
    p.plan("select avg price by date from trade", "two-phase");
    p.plan("select last price by sym from trade", "gather");
    p.plan(&format!("select last price by sym from trade where date in {d} 2026.11.24"), "gather");
    p.plan("select from trade where size = (max; size) fby sym", "gather");
    p.plan("select d: deltas price from trade where sym = `AAPL", "gather");
}

#[test]
fn joins_move_only_what_they_must() {
    let mut p = days(4);
    let plan = p.check("aj[`sym`date`time; trade; quote]");
    assert!(!plan.contains("shuffle") && !plan.contains("broadcast"), "{plan}");
    p.plan("select max bid by date, sym from aj[`date`sym`time; trade; quote]", "grouped by");
    // both tables have a `date` and the join does not compare it: the quote's date replaces the trade's, so rows no
    // longer lie where their columns say, and the join runs on the control node
    let plan = p.check("aj[`sym`time; trade; quote]");
    assert!(!plan.contains("shuffle") && !plan.contains("broadcast"), "{plan}");
    p.check("select max bid by date, sym from aj[`sym`time; trade; quote]");
    // quotes without dates, placed by `sym`: they are copied to the trades, or the trades are brought to them
    p.check("last_quote: delete date from quote");
    p.plan("aj[`sym`time; trade; last_quote]", "broadcast last_quote");
    p.cluster.set_broadcast_limit(0);
    p.plan("aj[`sym`time; trade; last_quote]", "shuffle trade by sym; scatter-gather");
    // the other table is placed by `sym` alone: only it moves, to where the pairs are
    p.check(".kix.dist[`quote; `sharded; `sym]");
    p.plan("aj[`sym`date`time; trade; quote]", "shuffle quote by date, sym");
    p.check("daily: select hi: max price by date, sym from trade");
    p.check(".kix.dist[`daily; `partitioned; `sym`date]");
    let plan = p.check("trade lj daily");
    assert!(!plan.contains("shuffle") && !plan.contains("broadcast"), "declared in either order: {plan}");
    p.check(".kix.dist[`daily; `sharded; `sym]");
    p.plan("trade lj daily", "shuffle daily by date, sym");
    // `acct` is already placed by `trader`
    p.plan("trade lj acct", "shuffle trade by trader; scatter-gather");
    // trades by `sym` joined to pairs: the pairs move to the symbols
    p.check(".kix.dist[`trade; `sharded; `sym]");
    p.check(".kix.dist[`quote; `partitioned; `date`sym]");
    p.plan("aj[`sym`date`time; trade; quote]", "shuffle quote by sym");
}

#[test]
fn writes_and_declarations() {
    let mut p = days(3);
    let d = DATE;
    p.plan("`trade insert (10:00:00.000; `AAPL; 1.5; 3; `N; `ann; 2026.11.30)", "insert");
    p.plan("update size: size + 1 from `trade where sym = `AAPL", "run on every shard");
    p.plan("update m: avg price by date, sym from `trade", "run on every shard");
    p.plan(&format!("update m: sums price by sym from `trade where date = {d}"), "run on every shard");
    p.check("trade");
    p.plan("delete from `trade where size < 20", "run on every shard");
    // groups by one column span shards, and neither distribution column can change in place
    p.plan("update n: sums price by sym from `trade", "gather");
    p.check("update date: date + 1 from `trade where size > 150");
    p.check("update sym: `XXX from `trade where size > 190");
    p.check("trade");
    p.plan(&format!("select from trade where date = {d}, sym = `AAPL"), "route");

    assert_eq!(p.session.eval(".kix.dist[`trade; `partitioned; `date`nosuch]").unwrap_err().signal, "domain");
    assert_eq!(p.session.eval(".kix.dist[`trade; `partitioned; `date`date]").unwrap_err().signal, "domain");
    assert_eq!(p.session.eval(".kix.dist[`trade; `sharded; `date`sym]").unwrap_err().signal, "type");
    assert_eq!(p.session.eval("DISTRIBUTE acct PARTITIONED BY trader, desk").unwrap_err().signal, "domain");
    p.check("trade");
    p.session.eval("DISTRIBUTE trade BY sym").unwrap();
    p.session.eval("DISTRIBUTE trade PARTITIONED BY date, sym").unwrap();
    p.plan(&format!("select from trade where date = {d}, sym = `AAPL"), "route");
    let (a, b) = (p.single.eval("trade").unwrap(), p.session.eval("trade").unwrap());
    assert!(same(&a, &b));
}

#[test]
fn parse_and_eval_on_the_cluster() {
    let mut p = days(3);
    p.check("parse \"select sum size by sym from trade\"");
    p.check("eval parse \"select sum size by sym from trade where date = 2026.11.23\"");
    p.check("q: parse \"exec max price from trade\"; eval q");
    p.check("eval parse \"update size: 2 * size from `trade where sym = `AAPL\"");
    p.check("eval parse \"delete from `trade where size < 10\"");
    p.check("trade");
    p.check("?[trade; (); (); (max; `price)]");
    p.check("md5 \"abc\"");
    p.check("select h: md5 each string sym from trade where size > 100");
}

#[test]
fn generated_statements_agree() {
    const COLUMNS: [&str; 9] = ["", "date, sym, price", "sum size", "a: avg price, n: count i", "last price", "d: deltas price", "m: med size", "price, v: size * 2", "mx: max price, v: var size"];
    const GROUPS: [&str; 6] = ["", " by sym", " by date", " by date, sym", " by ex", " by sym, 5 xbar time.minute"];
    const FILTERS: [&str; 10] = [
        "",
        " where date = 2026.11.23",
        " where sym = `AAPL",
        " where date = 2026.11.24, sym = `MSFT",
        " where sym in `AAPL`GOOG, date = 2026.11.25",
        " where date within 2026.11.23 2026.11.24, size > 50",
        " where date = 2026.11.26, size = (max; size) fby sym",
        " where size = (max; size) fby ([] sym; date)",
        " where date = 2026.11.23, price > avg price",
        " where date = 2027.01.01",
    ];
    const SOURCES: [&str; 3] = ["trade", "aj[`sym`date`time; trade; quote]", "trade lj acct"];
    const WRITES: [&str; 6] = [
        "update size: size + 1 from `trade where date = 2026.11.24",
        "update m: max price by sym from `trade where date = 2026.11.23",
        "update m: max price by date, sym from `trade",
        "delete from `trade where date = 2026.11.25, size < 30",
        "`trade insert (10:00:00.000; `GOOG; 2.5; 4; `Q; `bob; 2026.11.25; 0n)",
        "delete from `trade where sym = `IBM, size = max size",
    ];
    let mut seed = 99u64;
    let mut pick = |from: &[&'static str]| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        from[(seed % from.len() as u64) as usize]
    };
    for (shards, limit) in [(3, 0), (5, 100_000)] {
        let mut p = days(shards);
        p.cluster.set_broadcast_limit(limit);
        p.check("update m: 0n from `trade");
        for step in 0..900 {
            if step % 60 == 59 {
                p.check(pick(&WRITES));
                p.check("trade");
            }
            let verb = pick(&["select", "select", "exec"]);
            let query = format!("{verb} {}{} from {}{}", pick(&COLUMNS), pick(&GROUPS), pick(&SOURCES), pick(&FILTERS));
            p.check(&query);
        }
    }
}
