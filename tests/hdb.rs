//! Databases on disk: enumerating symbols, splayed tables, and partitioned and segmented databases.
use kixdb::{Connection, Server};

/// A connection to a durable workspace in a new directory, so that relative paths stay inside it.
fn workspace() -> (tempfile::TempDir, Connection) {
    let dir = tempfile::tempdir().unwrap();
    let connection = Server::open(dir.path()).unwrap().connect();
    (dir, connection)
}

fn check(c: &mut Connection, cases: &[(&str, &str)]) {
    for (code, expected) in cases {
        let got = c.eval(code).map(|k| k.to_string()).unwrap_or_else(|e| format!("'{}", e.signal));
        assert_eq!(&got, expected, "{code}");
    }
}

#[test]
fn symbols_are_enumerated() {
    let (_dir, mut c) = workspace();
    check(&mut c, &[
        ("t: .Q.en[`:db] ([] s:`a`b`a; v:1 2 3; u:`x`y`z); type t`s", "20h"),
        ("(sym; get `:db/sym; t[`v] where t[`s]=`a)", "(`a`b`x`y`z;`a`b`x`y`z;1 3)"),
        // a symbol that the domain lacks goes in only through `.Q.en`, which adds it
        ("`t insert (`c; 4; `x)", "'cast"),
        ("`t insert .Q.en[`:db] ([] s:enlist `c; v:enlist 4; u:enlist `x); (count t; last sym)", "(4;`c)"),
        ("e: .Q.ens[`:db; ([] s:`q`r); `other]; (other; type e`s; count sym)", "(`q`r;20h;6)"),
        ("`:db/t/ set t", "`:db/t/"),
        // `\l` of a directory loads the domains and tables in it; of a splayed table, that table
        ("system \"x t\"; system \"x sym\"; system \"l db\"; (exec s from t; sym 0 1)", "(`sym$`a`b`a`c;`a`b)"),
        ("system \"x t\"; system \"l db/t\"", "`t"),
        ("(count t; iasc `:db/t/v)", "(4;0 1 2 3)"),
        ("count () , t", "4"),
    ]);
}

#[test]
fn partitions_are_viewed_and_filled() {
    let (_dir, mut c) = workspace();
    check(&mut c, &[
        ("t: ([] sym:`b`a; v:1 2); q: ([] sym:enlist `a; b:enlist 9.5)", "+`sym`b!(,`a;,9.5)"),
        (".Q.dpft[`:db; 2026.01.01; `sym; `t]; .Q.dpft[`:db; 2026.01.02; `sym; `t]; .Q.dpft[`:db; 2026.01.02; `sym; `q]", "`q"),
        ("system \"x t\"; system \"x q\"; system \"l db\"; (.Q.pf; .Q.pt; date)", "(`date;`q`t;2026.01.01 2026.01.02)"),
        ("exec v from t where date=last date, sym=`a", ",2"),
        ("type exec sym from t", "20h"),
        // the first partition lacks `q` until it is filled
        ("count select from q", "1"),
        ("r: .Q.chk `:db; count each r", "1 0"),
        ("(key `:db/2026.01.01; system \"l db\"; count select from q)", "(`q`t;::;1)"),
        ("count select from q where date=2026.01.01", "0"),
        (".Q.view 2026.01.02; (.Q.pv; .Q.PV; date; count select from t)", "(,2026.01.02;2026.01.01 2026.01.02;,2026.01.02;2)"),
        (".Q.view[]; count select from t", "4"),
        ("exec v from .Q.ind[t; 1 2]", "1 2"),
    ]);
}

#[test]
fn a_variable_and_a_mounted_table_replace_each_other() {
    let (_dir, mut c) = workspace();
    check(&mut c, &[
        ("`:db/2026.01.01/t/ set ([] v:1 2); system \"l db\"; exec v from t", "1 2"),
        // a value assigned to the table's name is what queries then see, until the database is mounted again
        ("t: ([] v:enlist 7); exec v from t", ",7"),
        ("{[t] exec v from t} ([] v:enlist 8)", ",8"),
        ("system \"l db\"; exec v from t", "1 2"),
    ]);
}

#[test]
fn segments_and_columns_that_differ() {
    let (_dir, mut c) = workspace();
    check(&mut c, &[
        ("`:seg/a/2026.01.01/t/ set ([] v:1 2); `:seg/b/2026.01.02/t/ set ([] v:3 4; w:5 6)", "`:seg/b/2026.01.02/t/"),
        ("`:root/par.txt 0: (\"seg/a\"; \"seg/b\"); system \"l root\"; date", "2026.01.01 2026.01.02"),
        // the last partition says which columns there are
        ("exec w from t where date=2026.01.02", "5 6"),
        ("select from t", "'mismatch"),
        ("exec v from t", "1 2 3 4"),
    ]);
}

#[test]
fn mapped_partitions_schema_evolution_and_qp() {
    let (_dir, mut c) = workspace();
    check(&mut c, &[
        // Day 1 has `t` (without `w` and `tag`) and splayed `sp`; Day 2 adds columns `w` and `tag` to `t` and adds table `q`
        ("sym: `$(); `:db/sp/ set .Q.en[`:db] ([] s:`x`y; a:10 20)", "`:db/sp/"),
        ("t: ([] sym:`b`a`b; v:10 20 30; px:1.5 2.5 3.5); .Q.dpft[`:db; 2026.01.01; `sym; `t]", "`t"),
        ("t: ([] sym:`a`c; v:40 50; px:4.5 5.5; w:7 8; tag:(\"hi\";\"yo\")); .Q.dpft[`:db; 2026.01.02; `sym; `t]", "`t"),
        ("q: ([] sym:enlist `a; b:enlist 9.5); .Q.dpft[`:db; 2026.01.02; `sym; `q]", "`q"),
        ("mem: ([] k: 1 2); system \"l db\"", "::"),
        // `.Q.qp` distinguishes partitioned (1b), splayed (0b), and in-memory / non-table values (0) without loading partitions
        ("(.Q.qp t; .Q.qp[q]; .Q.qp sp; .Q.qp get `:db/sp; .Q.qp mem; .Q.qp `t; .Q.qp 42)", "(1b;1b;0b;0b;0;0;0)"),
        // Header-only `cols`, `count`, `meta`, `.Q.cn`, `.Q.pn`, and `select count i by date`
        ("(cols t; count t; .Q.cn t; .Q.pn`t)", "(`date`sym`v`px`w`tag;5;3 2;3 2)"),
        ("(exec t from meta t; exec a from meta t)", "(\"dsjfjC\";`s`p````)"),
        ("exec x from select count i by date from t", "3 2"),
        // `.Q.MAP[]` maps partition files and reports mapped bytes in `.Q.w[]`, while still allowing partition overwrites on Windows
        ("[a: .Q.w[][`mmap]; .Q.MAP[]; (a = 0; .Q.w[][`mmap] > 0)]", "11b"),
        // Per-partition `p#sym` filtering + mapped row subset extraction across multiple partitions
        ("exec v from t where sym = `a", "20 40"),
        ("exec px from t where sym = `b, v > 15", ",3.5"),
        // Missing columns signal `'mismatch` until `.Q.bv[]` builds `.Q.vp`
        ("select from t", "'mismatch"),
        (".Q.bv[]; key .Q.vp", ",`t"),
        ("exec w from t", "0N 0N 0N 7 8"),
        ("exec tag from t", "(\"\";\"\";\"\";\"hi\";\"yo\")"),
        ("exec w from t where sym = `b", "0N 0N"),
        // `.Q.bv[`]` also adds virtual empty slices for tables absent in older partitions (`q` on 2026.01.01)
        (".Q.bv[`]; (.Q.cn q; count select from q where date = 2026.01.01)", "(0 1;0)"),
        // Overwriting an already-mapped partition succeeds cleanly
        ("t: ([] sym:enlist `z; v:enlist 99; px:enlist 9.9; w:enlist 1; tag:enlist \"ok\"); .Q.dpft[`:db; 2026.01.01; `sym; `t]", "`t"),
        ("system \"l db\"; .Q.MAP[]; (count t; exec v from t)", "(3;99 40 50)"),
    ]);
}
