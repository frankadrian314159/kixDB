use kixdb::Server;

fn setup_market(c: &mut kixdb::Connection) {
    c.eval(
        "inst: ([sym: `AAPL`MSFT`GOOG] name: (\"Apple\"; \"Microsoft\"; \"Alphabet\"); lot: 100 100 50);\
         trade: ([] time: 09:30:01.000 09:30:02.000 09:31:10.000 09:31:20.000 09:35:05.000;\
                    sym: `AAPL`MSFT`AAPL`AAPL`MSFT;\
                    price: 180.0 400.0 182.0 181.0 404.0;\
                    size: 100 200 300 100 200);\
         quote: ([] time: 09:30:00.000 09:30:01.500 09:31:00.000 09:31:15.000;\
                    sym: `AAPL`MSFT`AAPL`AAPL;\
                    bid: 179.8 399.5 181.8 180.8;\
                    ask: 180.1 400.2 182.1 181.2)",
    )
    .unwrap();
}

#[test]
fn qsql_select_by_sequential_where_and_fby() {
    let srv = Server::new();
    let mut c = srv.connect();
    setup_market(&mut c);

    // Sequential `where`: second clause `price > avg price` runs ONLY over AAPL rows (avg of 180, 182, 181 = 181.0 -> only 182.0 passes!)
    let r = c.eval("exec price from trade where sym = `AAPL, price > avg price").unwrap();
    assert_eq!(r.to_string(), ",182f");

    // Grouped aggregation with `wavg` (VWAP) and `sum`
    let vwap = c.eval("exec vwap from (0 ! select vwap: size wavg price, vol: sum size by sym from trade) where sym = `AAPL").unwrap();
    // (100*180 + 300*182 + 100*181) / 500 = (18000 + 54600 + 18100) / 500 = 90700 / 500 = 181.4
    assert_eq!(vwap.to_string(), ",181.4");

    // `fby` filter-by-group: trades whose size >= max size for their symbol
    let big = c.eval("select sym, price, size from trade where size = (max; size) fby sym").unwrap();
    assert_eq!(big.count(), 3); // AAPL 300, MSFT 200 (2 rows)

    // `xbar` 5-minute OHLC bars
    let bars = c.eval("0 ! select open: first price, high: max price, low: min price, close: last price by sym, bar: 5 xbar time.minute from trade").unwrap();
    assert_eq!(bars.count(), 3); // (AAPL, 09:30), (MSFT, 09:30), (MSFT, 09:35)
}

#[test]
fn qsql_update_delete_insert_upsert() {
    let srv = Server::new();
    let mut c = srv.connect();
    setup_market(&mut c);

    // Value update (does not mutate `trade`)
    let u = c.eval("exec notional from update notional: price * size from trade").unwrap();
    assert_eq!(c.eval("first `long$18000 80000 54600 18100 80800").unwrap().to_string(), "18000");
    assert_eq!(u.count(), 5);
    assert_eq!(c.eval("cols trade").unwrap().to_string(), "`time`sym`price`size");

    // Grouped `update ... by sym`
    let rel = c.eval("exec diff from update diff: price - avg price by sym from trade where sym = `MSFT").unwrap();
    assert_eq!(rel.to_string(), "0n -2 0n 0n 2");

    // In-place update, insert, upsert, and delete
    c.eval("update size: size * 2 from `trade where sym = `MSFT").unwrap();
    assert_eq!(c.eval("exec sum size from trade where sym = `MSFT").unwrap().to_string(), "800");

    c.eval("`trade insert (09:36:00.000; `GOOG; 150.5; 50)").unwrap();
    assert_eq!(c.eval("count trade").unwrap().to_string(), "6");

    c.eval("delete from `trade where sym = `GOOG").unwrap();
    assert_eq!(c.eval("count trade").unwrap().to_string(), "5");
}

#[test]
fn relational_and_timeseries_joins_aj_wj() {
    let srv = Server::new();
    let mut c = srv.connect();
    setup_market(&mut c);

    // `lj` and `ij` with keyed `inst`
    assert_eq!(c.eval("exec lot from (trade lj inst)").unwrap().to_string(), "100 100 100 100 100");
    c.eval("`trade insert (09:40:00.000; `NVDA; 900.0; 10)").unwrap();
    assert_eq!(c.eval("count trade ij inst").unwrap().to_string(), "5");
    assert_eq!(c.eval("count trade lj inst").unwrap().to_string(), "6");

    // `aj` as-of join:
    // AAPL @ 09:30:01 -> quote @ 09:30:00 (bid 179.8)
    // MSFT @ 09:30:02 -> quote @ 09:30:01.500 (bid 399.5)
    // AAPL @ 09:31:10 -> quote @ 09:31:00 (bid 181.8)
    // AAPL @ 09:31:20 -> quote @ 09:31:15 (bid 180.8)
    // MSFT @ 09:35:05 -> quote @ 09:30:01.500 (bid 399.5)
    // NVDA @ 09:40:00 -> no quote (0n)
    let bids = c.eval("exec bid from aj[`sym`time; trade; quote]").unwrap();
    assert_eq!(bids.to_string(), "179.8 399.5 181.8 180.8 399.5 0n");

    // `wj1` window join: 20-second lookback `(-20000 0) +\: trade.time`
    let wj_res = c
        .eval("t2: select from trade where sym=`AAPL; exec bid from wj1[(t2.time - 20000; t2.time); `sym`time; t2; (quote; (max; `bid))]")
        .unwrap();
    assert_eq!(wj_res.to_string(), "179.8 181.8 181.8");
}

#[test]
fn attribute_accelerated_filters_find_and_joins() {
    let srv = Server::new();
    let mut c = srv.connect();

    // Build identical unattributed and attributed tables (`s#, `u#, `p#, `g#)
    c.eval(
        "plain: ([] ts: 10 20 30 40 50 60 70 80; id: 100 101 102 103 104 105 106 107; sym: `A`A`A`B`B`C`C`C; grp: `x`y`x`z`y`x`z`y; v: 1 2 3 4 5 6 7 8);\
         idxed: ([] ts: `s#10 20 30 40 50 60 70 80; id: `u#100 101 102 103 104 105 106 107; sym: `p#`A`A`A`B`B`C`C`C; grp: `g#`x`y`x`z`y`x`z`y; v: 1 2 3 4 5 6 7 8)",
    )
    .unwrap();

    for q in [
        "exec v from T where ts = 40",
        "exec v from T where 40 = ts",
        "exec v from T where ts < 35",
        "exec v from T where ts <= 30",
        "exec v from T where ts > 60",
        "exec v from T where ts >= 60",
        "exec v from T where ts within 25 65",
        "exec v from T where id = 105",
        "exec v from T where id in 101 106 999",
        "exec v from T where sym = `B",
        "exec v from T where sym in `A`C",
        "exec v from T where grp = `y",
        "exec v from T where grp in `x`z, ts >= 30, sym = `C",
    ] {
        let a = c.eval(&q.replace('T', "plain")).unwrap();
        let b = c.eval(&q.replace('T', "idxed")).unwrap();
        assert_eq!(a, b, "mismatch on {q}");
    }

    // `?` (find) on `s#, `u#, `p#, `g#
    assert_eq!(c.eval("(`s#10 20 30 40) ? 30 99 10").unwrap().to_string(), "2 4 0");
    assert_eq!(c.eval("(`u#`a`b`c) ? `c`x`a").unwrap().to_string(), "2 3 0");
    assert_eq!(c.eval("(`p#`a`a`b`b`c) ? `b`z`a").unwrap().to_string(), "2 5 0");
    assert_eq!(c.eval("(`g#`x`y`x`z) ? `z`x`w").unwrap().to_string(), "3 0 4");

    // `lj`, `ij`, `pj` against `u# and `s# keyed tables
    c.eval("ku: ([id: `u#101 104 107] w: 10 20 30); ks: ([ts: `s#20 50 80] bonus: 5 6 7)").unwrap();
    assert_eq!(c.eval("exec w from (idxed lj ku)").unwrap().to_string(), "0N 10 0N 0N 20 0N 0N 30");
    assert_eq!(c.eval("exec v from (idxed ij ks)").unwrap().to_string(), "2 5 8");
    assert_eq!(c.eval("exec v from (idxed pj ([id: `u#100 107] v: 100 200))").unwrap().to_string(), "101 2 3 4 5 6 7 208");
}

#[test]
fn joins_match_keys_by_type_and_value_however_they_are_looked_up() {
    let mut c = Server::new().connect();
    // keys 0 to 29 and again 0 to 9; `w` and `lw` hold the same keys spread far apart, so that they are hashed
    c.eval("r: ([] k: (til 30), til 10; s: 40#`a`b`c; v: til 40); l: ([] k: 3 1 35 0N 29 3; s: `a`b`c`a`a`c)").unwrap();
    c.eval("w: update k: k * 1000000000000 from r; lw: update k: k * 1000000000000 from l").unwrap();
    // an indexed table so large that a few rows are looked up in its index, many by numbering both sides' keys
    c.eval("big: `k xkey ([] k: `s#til 1000; v: til 1000)").unwrap();
    for (code, expected) in [
        // the first row with a key wins, and a missing key gives nulls
        ("exec v from l lj `k xkey r", "3 1 0N 0N 29 3"),
        ("exec v from lw lj `k xkey w", "3 1 0N 0N 29 3"),
        ("exec v from l lj `k`s xkey r", "3 1 0N 0N 0N 0N"),
        ("exec v from lw lj `k`s xkey w", "3 1 0N 0N 0N 0N"),
        ("exec v from l ij `k xkey r", "3 1 29 3"),
        // an equi-join keeps every match, in the order of the right table
        ("exec v from ej[`k; l; r]", "3 33 1 31 29 3 33"),
        ("exec v from ej[`k`s; lw; w]", "3 33 1 31"),
        // keys of unlike types differ
        ("exec v from ([] k: 3 1i) lj `k xkey r", "0N 0N"),
        ("exec v from l lj big", "3 1 35 0N 29 3"),
        ("(600 # exec v from l lj big) ~ exec v from (600 # l) lj big", "1b"),
        ("exec v from ([k: 1 2] v: 10 20) uj ([k: 2 3] v: 21 31)", "10 21 31"),
        ("exec p from aj[`s`t; ([] s: `a`b`a`c; t: 5 5 9 9); ([] s: `a`a`b; t: 1 7 3; p: 10 20 30)]", "10 30 20 0N"),
    ] {
        assert_eq!(c.eval(code).unwrap().to_string(), expected, "{code}");
    }
}

#[test]
fn i_is_the_row_number_in_the_whole_table() {
    let mut c = Server::new().connect();
    c.eval("t: ([] g: 10#`a`b; v: til 10); s: ([] v: `s#til 10)").unwrap();
    for (code, expected) in [
        ("exec i from t where v > 5", "6 7 8 9"),
        // a later clause, and a clause on `i` itself, after a filter
        ("exec i from t where v > 2, i < 6", "3 4 5"),
        ("exec v from t where v > 3, i in 5 7", "5 7"),
        ("exec i from s where v > 5, i < 8", "6 7"),
        // columns computed from it, with and without a column list of aggregates
        ("exec i - v from t where v > 7", "0 0"),
        // within groups: computed for all groups at once, and group by group (the lambda)
        ("value exec first i by g from t where v > 3", "4 5"),
        ("value exec sum i by g from t where v > 3", "18 21"),
        ("raze value exec {x} i by g from t where v > 6", "8 7 9"),
        ("key exec count v by 3 xbar i from t where v > 4", "`s#3 6 9"),
        // updates and deletes
        ("exec j from update j: i from t where v > 7", "0N 0N 0N 0N 0N 0N 0N 0N 8 9"),
        ("exec j from update j: i by g from t where v > 7", "0N 0N 0N 0N 0N 0N 0N 0N 8 9"),
        ("exec j from update j: sum i by g from t where v > 5", "0N 0N 0N 0N 0N 0N 14 16 14 16"),
        ("exec v from delete from t where v > 2, i > 5", "0 1 2 3 4 5"),
    ] {
        assert_eq!(c.eval(code).unwrap().to_string(), expected, "{code}");
    }
}

#[test]
fn a_view_is_computed_from_global_names_whoever_reads_it_first() {
    let mut c = Server::new().connect();
    c.eval("t: ([] a: 1 2 3 4 5; b: 10 20 30 40 50); a: 100").unwrap();
    // read first inside a query, which has a column `a`, and inside a function, which has a local `a`
    c.eval("v:: a + 1; w:: sum a; f: {[] a: 1; v}").unwrap();
    for (code, expected) in [
        ("exec w from t where b > 30", "100"),
        ("w", "100"),
        ("f[]", "101"),
        ("v", "101"),
        // a changed global still reaches it
        ("a: 7; (v; w; exec v from t)", "8 7 8"),
    ] {
        assert_eq!(c.eval(code).unwrap().to_string(), expected, "{code}");
    }
    // a view reads the names of its own namespace first, wherever it is read from
    for statement in ["\\d .ns", "k: 5", "u:: k * 2", "\\d .", "k: 1000"] {
        c.execute(statement);
    }
    assert_eq!(c.eval(".ns.u").unwrap().to_string(), "10");
    // a column is not a name a view can read
    c.eval("x:: nowhere + 1").unwrap();
    assert!(c.eval("select x from ([] nowhere: 1 2)").is_err());
}

#[test]
fn an_attribute_does_not_change_what_a_general_list_matches() {
    // a sorted general list once matched numbers of unlike types by value
    let mut c = Server::new().connect();
    c.eval("p: ([a: (1; 2.5; 3)] v: 10 20 30); s: ([a: `s#(1; 2.5; 3)] v: 10 20 30); l: ([] a: (1i; 2.5e; 3; 1.0))").unwrap();
    for code in ["l lj {}", "l ij {}", "(key {})[`a] ? (1i; 3)", "(key {})[`a] ? 1i"] {
        let (plain, sorted) = (code.replace("{}", "p"), code.replace("{}", "s"));
        assert_eq!(c.eval(&sorted).unwrap(), c.eval(&plain).unwrap(), "{code}");
    }
    assert_eq!(c.eval("exec v from l ij s").unwrap().to_string(), ",30");
}
