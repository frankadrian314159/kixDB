use kixdb::Server;
use std::fs::OpenOptions;
use std::io::Write;

#[test]
fn flat_splayed_and_partitioned_hdb_pruning() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();

    let srv = Server::open(dir).unwrap();
    let mut c = srv.connect();

    // 1. Flat file `set` / `get`
    c.eval("`:vec.bin set 10 20 30 40").unwrap();
    assert_eq!(c.eval("get `:vec.bin").unwrap().to_string(), "10 20 30 40");

    // 2. Splayed table `set` / `get`
    c.eval("`:ref_splay/ set ([] sym: `AAPL`MSFT; sector: `Tech`Tech; weight: 0.6 0.4)").unwrap();
    assert_eq!(c.eval("exec sum weight from get `:ref_splay/").unwrap().to_string(), "1f");

    // 3. Partitioned HDB via `.Q.dpft` across two dates
    c.eval("t_d: ([] sym: `MSFT`AAPL`AAPL; price: 400.0 180.0 182.0; size: 50 100 200)").unwrap();
    let hdb_sym = format!("`:{}", dir.display().to_string().replace('\\', "/"));
    c.eval(&format!(".Q.dpft[{hdb_sym}; 2026.11.23; `sym; `t_d]")).unwrap();
    c.eval("t_d: ([] sym: `GOOG`AAPL; price: 150.0 185.0; size: 300 400)").unwrap();
    c.eval(&format!(".Q.dpft[{hdb_sym}; 2026.11.24; `sym; `t_d]")).unwrap();
    c.eval("delete t_d from `.").unwrap_or(kixdb::K::Null);
    drop(c);
    drop(srv);

    // Reopen HDB and verify partition pruning on `date = 2026.11.24`
    let srv2 = Server::open(dir).unwrap();
    let mut c2 = srv2.connect();
    assert_eq!(c2.eval("count select from t_d").unwrap().to_string(), "5");
    assert_eq!(c2.eval("exec sum size from t_d where date = 2026.11.24").unwrap().to_string(), "700");
    assert_eq!(
        c2.eval("exec sum size from t_d where date = 2026.11.23, sym = `AAPL").unwrap().to_string(),
        "300"
    );
}

#[test]
fn wal_crash_recovery_and_torn_tail_handling() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();

    {
        let srv = Server::open(dir).unwrap();
        let mut c = srv.connect();
        c.eval("ledger: ([] id: 1 2 3; amt: 10.5 20.0 30.25)").unwrap();
        c.eval("`ledger insert (4; 99.25)").unwrap();
        // Simulate abrupt process crash (no shutdown checkpoint!)
        srv.simulate_crash();
    }

    // Append torn garbage bytes to the end of `kix.wal` to simulate mid-write power loss
    {
        let mut f = OpenOptions::new().append(true).open(dir.join("kix.wal")).unwrap();
        f.write_all(b"KXWL\x99\x00\x00\x00partial_torn_tail").unwrap();
    }

    // Reopen and verify WAL replay recovered all committed mutations and discarded the torn tail
    let srv2 = Server::open(dir).unwrap();
    let mut c2 = srv2.connect();
    assert_eq!(c2.eval("exec sum amt from ledger").unwrap().to_string(), "160f");
    srv2.checkpoint().unwrap();
    assert_eq!(srv2.storage_stats().wal_bytes, 0);
}

#[test]
fn csv_zero_colon_and_bulk_import() {
    let tmp = tempfile::tempdir().unwrap();
    let csv_path = tmp.path().join("ticks.csv");

    let srv = Server::new();
    let mut c = srv.connect();
    c.eval("orig: ([] sym: `AAPL`MSFT`NVDA; px: 180.5 410.25 920.0; qty: 10 20 30)").unwrap();
    let p_str = csv_path.display().to_string().replace('\\', "/");
    c.eval(&format!("`:{p_str} 0: \",\" 0: orig")).unwrap();

    let loaded = c.eval(&format!("(\"SFJ\"; enlist \",\") 0: `:{p_str}")).unwrap();
    assert_eq!(loaded, c.eval("orig").unwrap());

    let count = kixdb::tools::bulk_import(&mut c, &csv_path, "bulk_tbl", ',', true, true, 2, None, None).unwrap();
    assert_eq!(count, 3);
    assert_eq!(c.eval("exec sum qty from bulk_tbl").unwrap().to_string(), "60");
}
