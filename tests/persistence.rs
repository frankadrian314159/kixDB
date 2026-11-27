use kixdb::{Server, StorageOptions};
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
    // a column named only inside a string is loaded too
    assert_eq!(c2.eval("exec value \"sum size\" from t_d where date = 2026.11.24").unwrap().to_string(), "700");
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

    // Quoted commas, escaped quotes, embedded newlines, booleans, dates, and packed strings in one pass
    let rich_csv = "id,ok,dt,note\n1,true,2026-11-27,\"hello, \"\"world\"\"\"\n2,false,2026-11-28,\"line1\nline2\"\n";
    let rich_path = tmp.path().join("rich.csv");
    std::fs::write(&rich_path, rich_csv).unwrap();
    let rich_str = rich_path.display().to_string().replace('\\', "/");
    assert_eq!(kixdb::tools::import_csv(&mut c, rich_csv, "imp_tbl", true).unwrap(), 2);
    assert_eq!(kixdb::tools::bulk_import(&mut c, &rich_path, "bulk_rich", ',', true, true, 1, None, None).unwrap(), 2);
    assert_eq!(c.eval("imp_tbl ~ bulk_rich").unwrap().to_string(), "1b");
    assert_eq!(
        c.eval(&format!("((\"JBDC\"; enlist \",\") 0: `:{rich_str}) ~ imp_tbl")).unwrap().to_string(),
        "1b"
    );
}

#[test]
fn a_data_directory_is_locked_and_the_log_checkpoints_itself() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = StorageOptions { sync_wal: false, checkpoint_every: 3 };
    let srv = Server::open_with(tmp.path(), opts).unwrap();
    assert!(Server::open(tmp.path()).is_err(), "a second process must not open the same directory");
    let mut c = srv.connect();
    for i in 0..10 {
        c.eval(&format!("x{i}: {i}")).unwrap();
    }
    let stats = srv.storage_stats();
    assert!(stats.wal_records <= 3 && stats.snap_bytes > 0, "{} records", stats.wal_records);
    srv.simulate_crash();
    drop(c);
    let again = Server::open(tmp.path()).unwrap();
    assert_eq!(again.connect().eval("x0 + x9").unwrap().to_string(), "9");
}

#[test]
fn partitioned_queries_load_only_the_columns_they_name() {
    let tmp = tempfile::tempdir().unwrap();
    let hdb = tmp.path().display().to_string().replace('\\', "/");
    {
        let srv = Server::new();
        let mut c = srv.connect();
        c.eval("t: ([] sym: `a`b; px: 1.5 2.5; size: 10 20)").unwrap();
        c.eval(&format!(".Q.dpft[`:{hdb}; 2026.11.24; `sym; `t]")).unwrap();
    }
    // damage a column: queries that do not name it still work, those that do fail
    std::fs::write(tmp.path().join("2026.11.24/t/px"), b"damaged").unwrap();
    let srv = Server::new();
    let mut c = srv.connect();
    c.eval(&format!("\\l {hdb}")).unwrap();
    assert_eq!(c.eval("exec sum size from t where date = 2026.11.24").unwrap().to_string(), "30");
    assert_eq!(c.eval("count select i from t").unwrap().to_string(), "2");
    assert!(c.eval("exec px from t").is_err());
    assert!(c.eval("select from t").is_err());
}

#[test]
fn year_partitions_and_non_leading_where_pruning() {
    let tmp = tempfile::tempdir().unwrap();
    let hdb = tmp.path().display().to_string().replace('\\', "/");
    {
        let srv = Server::new();
        let mut c = srv.connect();
        c.eval("yt: ([] sym: `a`b; v: 10 20)").unwrap();
        c.eval(&format!(".Q.dpft[`:{hdb}; 2025i; `sym; `yt]")).unwrap();
        c.eval("yt: ([] sym: `a`c; v: 30 40)").unwrap();
        c.eval(&format!(".Q.dpft[`:{hdb}; 2026i; `sym; `yt]")).unwrap();
    }
    // Corrupt 2025 partition's `v` column: a query whose SECOND `where` clause prunes to `year = 2026` must not touch 2025!
    std::fs::write(tmp.path().join("2025/yt/v"), b"corrupt").unwrap();
    let srv = Server::new();
    let mut c = srv.connect();
    c.eval(&format!("\\l {hdb}")).unwrap();
    assert_eq!(c.eval("exec sum v from yt where sym = `a, year = 2026").unwrap().to_string(), "30");
    assert_eq!(c.eval("exec sum v from yt where year > 2025, year <= 2026").unwrap().to_string(), "70");
    assert!(c.eval("exec sum v from yt where year = 2025").is_err());
}
