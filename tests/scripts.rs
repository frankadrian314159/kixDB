//! What existing q scripts use: composition by a trailing verb, projections of amends, globals changed from
//! functions, `$` products, `delete` from a namespace, and the settings `\S`, `\W` and `\z`.
use kixdb::{Connection, Server};

fn check(c: &mut Connection, cases: &[(&str, &str)]) {
    for (code, expected) in cases {
        let got = c.eval(code).map(|k| k.to_string()).unwrap_or_else(|e| format!("'{}", e.signal));
        assert_eq!(&got, expected, "{code}");
    }
}

fn fresh() -> Connection {
    Server::new().connect()
}

#[test]
fn verbs_left_waiting_are_composed() {
    check(&mut fresh(), &[
        ("t: ([] time: 1 2; sym: `a`b); (2 # key flip value @) `t", "`time`sym"),
        ("(`time`sym ~ 2 # key flip value @) each enlist `t", ",1b"),
        ("f: (count reverse @); f 1 2 3", "3"),
        ("g: (neg 1 +); g 5", "-6"),
        // in brackets, it is a value like any other
        ("count (1 +)", "1"),
        // an amend with a place left empty is a projection
        ("d: 1 2 3; f: @[; 1; neg]; f d", "1 -2 3"),
        ("(.[; (); :; ] .) (`made; 5); made", "5"),
        ("s: ([] sym: `b`a); @[; `sym; `g#] each enlist `s; attr s`sym", "`g"),
        ("w: `a`b!(1 2; 3 4); neg union/[w]", "-1 -2 -3 -4"),
        ("(count 3#(); union/[`a`b!(enlist 5; ())])", "(3;,5)"),
        // after `]`, a name of q's own namespaces is applied to
        ("f: {[a; b] b}; g: {f[x].z.K}; g 0", "4.1"),
        ("0 \"1+1\"", "2"),
        ("0 (+; 1; 2)", "3"),
    ]);
}

#[test]
fn functions_change_globals() {
    check(&mut fresh(), &[
        ("n: 0; f: {n+: 1; 0}; f 0; f 0; n", "2"),
        ("s: {.a.b: x; 0}; s 5; .a.b", "5"),
        ("g: {n: 10; n+: 1; n}; (g 0; n)", "11 2"),
        ("t: `a`b`c; t@: 2 0; t", "`c`a"),
        ("h: {t: 10 20 30; t@: where 101b; t}; (h 0; t)", "(10 30;`c`a)"),
        // from a namespace's function, a table given by its name is the root's if the namespace has none
        ("q: ([] s: `g#`symbol$()); .n.add: {`q insert enlist x}; .n.add `a; (count q; attr q`s; key `.n)", "(1;`g;,`add)"),
        (".n.mark: {@[`q; `s; `p#]; 0}; .n.mark 0; (attr q`s; key `.n)", "(`p;`add`mark)"),
        ("v: 1 2; .n.clear: {@[`.; `v`q; 0#]}; .n.clear 0; (count v; count q)", "0 0"),
        // a query changes the root's table, and still sees the namespace's variables
        ("w: ([] v: 1 2 3); system \"d .n\"", "::"),
        ("least: 2; cut: {delete from `w where v < least}; system \"d .\"", "::"),
        (".n.cut 0; (w`v; `w in key `.n)", "(2 3;0b)"),
        ("(tables `.; tables `.n)", "(`s#`q`w;`s#`symbol$())"),
        // a time of day from a timestamp
        ("p: 2026.11.26D10:30:15; (\"n\"$p; \"u\"$p; \"v\"$p)", "(0D10:30:15.000000000;10:30;10:30:15)"),
    ]);
}

#[test]
fn dollar_multiplies_floats() {
    check(&mut fresh(), &[
        ("1 2 3f $ 4 5 6f", "32f"),
        ("(1 2 3f; 4 5 6f) $ (7 8f; 9 10f; 11 12f)", "(58 64f;139 154f)"),
        ("(1 2f; 3 4f) $ 5 6f", "17 39f"),
        ("5 6f $ (1 2f; 3 4f)", "23 34f"),
        ("1 2f mmu (1 2f; 3 4f)", "7 10f"),
        ("1 2f $ 1 2 3f", "'length"),
        ("1 2 $ 3 4", "'type"),
        ("(\"f\"$3; 4$\"ab\")", "(3f;\"ab  \")"),
    ]);
}

#[test]
fn names_are_deleted() {
    check(&mut fresh(), &[
        ("a: 1; b: 2; c: 3; delete a from `.", "`."),
        ("(key `.) except `", "`b`c"),
        ("delete b, absent from `.; ![`.; (); 0b; enlist `c]; (key `.) except `", "`symbol$()"),
        (".n.u: 1; .n.v: 2; delete u from `.n; key `.n", ",`v"),
        ("t: ([] a: 1 2; b: 3 4); delete a from `t; cols t", ",`b"),
    ]);
}

#[test]
fn seeds_weeks_and_dates_are_set() {
    check(&mut fresh(), &[
        ("system \"S\"", "0Ni"),
        ("system \"S 42\"; x: 5?100; y: 5?`3; 0", "0"),
        ("system \"S 42\"; x ~ 5?100", "1b"),
        ("y ~ 5?`3", "1b"),
        ("system \"S\"", "42i"),
        ("@[system; \"S 0\"; {x}]", "\"domain\""),
        ("(system \"W\"; `week$2026.11.26)", "(2i;2026.11.23)"),
        // weeks that start on Saturday
        ("system \"W 0\"; (`week$2026.11.26; `week$2026.11.28 2026.11.29)", "(2026.11.21;2026.11.28 2026.11.28)"),
        ("system \"W 2\"; (system \"z\"; \"D\"$\"06/01/2010\")", "(0i;2010.06.01)"),
        ("system \"z 1\"; (\"D\"$\"06/01/2010\"; \"D\"$\"2010.06.01\")", "2010.01.06 2010.06.01"),
        ("@[system; \"W 7\"; {x}]", "\"domain\""),
        (".z.k", "2024.02.13"),
    ]);
    // another workspace has its own settings
    check(&mut fresh(), &[("(system \"z\"; system \"W\")", "0 2i")]);
}
