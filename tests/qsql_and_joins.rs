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
