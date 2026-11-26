//! Files: the handles of `hopen`, reading and deleting, amends of what a file holds, `save` and `load`, and `0:`.
use kixdb::{Connection, Server};

/// A connection to a durable workspace in a new directory, so that relative paths stay inside it.
fn workspace() -> (tempfile::TempDir, Connection) {
    let dir = tempfile::tempdir().unwrap();
    let connection = Server::open(dir.path()).unwrap().connect();
    (dir, connection)
}

fn check(c: &mut Connection, cases: &[(&str, &str)]) {
    for (code, expected) in cases {
        let got = c.eval(code).map(|k| k.to_string()).unwrap_or_else(|e| format!("'{} {}", e.signal, e.detail));
        assert_eq!(&got, expected, "{code}");
    }
}

#[test]
fn names_reading_and_deleting() {
    let (dir, mut c) = workspace();
    check(&mut c, &[
        ("hsym `$\"sub/a b.txt\"", "`:sub/a b.txt"),
        ("hsym hsym `a`:b", "`:a`:b"),
        ("`:sub/f.txt 0: (\"Life\"; \"The Universe\")", "`:sub/f.txt"),
        ("read0 `:sub/f.txt", "(\"Life\";\"The Universe\")"),
        ("read0 (`:sub/f.txt; 5; 3)", "\"The\""),
        ("read1 (`:sub/f.txt; 0; 4)", "0x4c696665"),
        ("hcount `:sub/f.txt", "18"),
        ("`:sub/b 1: 0x06072a", "`:sub/b"),
        ("read1 `:sub/b", "0x06072a"),
        ("key `:sub", "`b`f.txt"),
        ("key `:sub/b", "`:sub/b"),
        ("hdel `:sub/b", "`:sub/b"),
        ("(count key `:sub/b; key `:sub)", "(0;,`f.txt)"),
    ]);
    assert!(c.eval("hdel `:sub").is_err(), "a directory that holds a file stays");
    assert!(dir.path().join("sub/f.txt").exists(), "relative paths are the workspace's");
}

#[test]
fn file_handles_append() {
    let (_dir, mut c) = workspace();
    check(&mut c, &[
        // a file holding a list takes items
        ("`:L set 10 20 30", "`:L"),
        ("h: hopen `:L", "3i"),
        ("h 42; h 100 200", "3i"),
        ("hclose h; get `:L", "10 20 30 42 100 200"),
        // text: a line for each string
        ("h: hopen `:t.txt; (neg h) \"one\"; (neg h) (\"two\"; \"three\")", "-3i"),
        ("neg[h] enlist \"four\"; hclose h; read0 `:t.txt", "(\"one\";\"two\";\"three\";\"four\")"),
        // any other file takes bytes
        ("h: hopen `:raw; h 0x0102; h \"A\"; h 7h; hclose h; read1 `:raw", "0x0102410700"),
        ("h: hopen `:raw; @[h; `a; {x}]", "\"type\""),
        // handles to files are not connections, and a number is used again once its handle is closed
        ("count .z.H", "0"),
        ("hclose h; hopen `:L", "3i"),
        (".[`:raw; (); :; 1 2]; get `:raw", "1 2"),
    ]);
}

#[test]
fn files_are_changed_by_name() {
    let (_dir, mut c) = workspace();
    check(&mut c, &[
        (".[`:raw; (); :; 1001 1002 1003]", "`:raw"),
        (".[`:raw; (); ,; 42]; get `:raw", "1001 1002 1003 42"),
        ("@[`:raw; 0; +; 5]; first get `:raw", "1006"),
        (".[`:new; (); ,; 1 2]; get `:new", "1 2"),
        ("`:t set ([] s:`a`b; v:1 2)", "`:t"),
        ("`:t upsert (`c; 3)", "`:t"),
        ("exec v from `:t where s in `a`c", "1 3"),
        ("`v xdesc `:t; exec s from `:t", "`c`b`a"),
        ("(cols `:t; count meta `:t)", "(`s`v;2)"),
        // a splayed table stays splayed
        ("`:d/ set ([] s:`a`b; v:1 2)", "`:d/"),
        (".[`:d/; (); ,; ([] s:enlist `c; v:enlist 3)]; exec v from `:d", "1 2 3"),
        ("@[`:d; `v; neg]; (get `:d/v; key `:d)", "(-1 -2 -3;`.d`s`v)"),
        // each column of a table named in an amend is changed by itself
        ("t: ([] a:1 2; b:3 4); (@[t; `a`b; reverse])`b", "4 3"),
        ("(@[t; `a`b; +; 10 100])`b", "103 104"),
        ("s: `symbol$(); u: @[([] p:`x`y; q:`y`z); `p`q; `s?]; (s; value u `q)", "(`x`y`z;`y`z)"),
    ]);
}

#[test]
fn save_and_load() {
    let (_dir, mut c) = workspace();
    check(&mut c, &[
        ("t: ([] c1:`a`b; c2:10 20); count t", "2"),
        ("save `:out/t", "`:out/t"),
        ("system \"x t\"; load `:out/t", "`t"),
        ("t`c2", "10 20"),
        ("save `:out/t.csv; read0 `:out/t.csv", "(\"c1,c2\";\"a,10\";\"b,20\")"),
        ("save `:out/t.txt; first read0 `:out/t.txt", "\"c1\\tc2\""),
        ("save `:out/t.xml; (read0 `:out/t.xml) 1", "\"<r><c1>a</c1><c2>10</c2></r>\""),
        ("save `:out/t.xls; 2 < count read0 `:out/t.xls", "1b"),
    ]);
    check(&mut c, &[
        ("rsave `:splayed/t/", "`:splayed/t/"),
        ("system \"x t\"; rload `:splayed/t; t`c1", "`a`b"),
        // `\l` of a file that holds a value sets the variable named after it
        ("u: 5; save `:u; system \"x u\"; system \"l u\"; u", "5"),
    ]);
}

#[test]
fn delimited_fixed_width_and_key_value_text() {
    let (_dir, mut c) = workspace();
    check(&mut c, &[
        ("`:f.txt 0: (\"1001 98.5ABC\"; \"1002 42.0XYZ\")", "`:f.txt"),
        ("(\"JFS\"; 4 5 3) 0: `:f.txt", "(1001 1002;98.5 42;`ABC`XYZ)"),
        ("(\"J S\"; 4 5 3) 0: (`:f.txt; 13; 13)", "(,1002;,`XYZ)"),
        ("(\"J*\"; 4 5) 0: enlist \"12   abc\"", "(,12;,\" abc\")"),
        ("(\"JS\"; \",\") 0: read0 `:f.txt", "(0N 0N;``)"),
        ("(\"SJ\"; enlist csv) 0: (\"a,b\"; \"x,1\")", "+`a`b!(,`x;,1)"),
        ("\"S=;\" 0: \"one=1;two=22\"", "(`one`two;(,\"1\";\"22\"))"),
        ("\"I=;\" 0: \"1=a;2=bc\"", "(1 2i;(,\"a\";\"bc\"))"),
        ("n: 0; .Q.fsn[{n:: n + count x}; `:f.txt; 1]", "26"),
        ("n", "2"),
        ("n: 0; .Q.fs[{n:: count x}; `:f.txt]", "26"),
        ("n", "2"),
    ]);
}
