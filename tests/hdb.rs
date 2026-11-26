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
