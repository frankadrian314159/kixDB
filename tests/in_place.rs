//! Assignments that change a variable where it is give what making a new value would, and leave copies of it alone.
use kixdb::Server;

fn check(setup: &str, cases: &[(&str, &str)]) {
    let server = Server::new();
    let mut c = server.connect();
    c.eval(setup).unwrap();
    for (code, expected) in cases {
        assert_eq!(c.eval(code).map(|k| k.to_string()).unwrap_or_else(|e| format!("'{}", e.signal)), *expected, "{code}");
    }
}

#[test]
fn appending_to_a_list() {
    check("x: 1 2; y: x; gl: 1 2; e: (); w: (\"ab\"; \"cd\"); s: `s#1 2", &[
        ("x,: 3", "1 2 3"),
        ("x,: 4 5; (x; y)", "(1 2 3 4 5;1 2)"),
        ("x,: `long$(); count x", "5"),
        ("x,: `a", "'type"),
        ("x,: 1.5 2.5", "'type"),
        ("x", "1 2 3 4 5"),
        ("e,: 1; e,: 2; e", "1 2"),
        ("w,: enlist \"ef\"; w,: \"g\"; (count w; w 2; w 3)", "(4;\"ef\";\"g\")"),
        ("s,: 0; attr s", "`"),
        ("f: {x,: 9; x}; (f y; y)", "(1 2 9;1 2)"),
        ("g: {r: 0#0; do[3; r,: x]; r}; g 7", "7 7 7"),
        ("h: {r: y; r,: 9; (r; y)}; h[0; y]", "(1 2 9;1 2)"),
        // without `::` a lambda makes a local of a global's value
        ("m: {gl,: 100; gl}; (count m 0; count gl)", "3 2"),
    ]);
}

#[test]
fn a_second_colon_changes_the_global() {
    check("gl: 1 2; n: 5; d: `a`b!1 2", &[
        ("f: {gl,:: 8; count gl}; r: f 0; (r; gl)", "(3;1 2 8)"),
        ("g: {gl,:: x; n+:: 1; n-:: 3; 0}; g 9; (gl; n)", "(1 2 8 9;3)"),
        ("gl+:: 10; gl", "11 12 18 19"),
        ("h: {d[`a]:: 5; d[`b]+:: 5; gl[0]:: 0; gl[1]+:: 1; 0}; h 0; (value d; gl)", "(5 7;0 13 18 19)"),
        // as `x::y` does, it assigns a local that has the name
        ("k: {gl: 0 0; gl,:: 1; count gl}; (k 0; count gl)", "3 4"),
        ("parse \"n+::1\"", "(+::;`n;1)"),
        ("eval parse \"n+::1\"; n", "4"),
        // a colon of its own still returns, and `::` is still the generic null
        ("{:x; 0} 7", "7"),
        ("e: (); e,: (::); count e", "1"),
    ]);
}

#[test]
fn assigning_at_depth() {
    check("o: (1 2 3; 4 5 6); n: `a`b!(`x`y!1 2; `x`y!3 4); L: (1 2; (3 4; 5 6)); t: ([] a: 1 2 3)", &[
        ("m: o; m[1;2]: 99; m", "(1 2 3;4 5 99)"),
        ("m: o; m[0;1]+: 100; m", "(1 102 3;4 5 6)"),
        // a place left empty stands for all of them, and a list of values gives one to each
        ("m: o; m[;0]: 7 8; m", "(7 2 3;8 5 6)"),
        ("m: o; m[;0]: 9; m", "(9 2 3;9 5 6)"),
        ("m: o; m[1;]: 10 11 12; m", "(1 2 3;10 11 12)"),
        ("m: o; m[0 1; 2]: 30 60; m", "(1 2 30;4 5 60)"),
        ("m: o; m[0 1; 0 1]: (10 20; 30 40); m", "(10 20 3;30 40 6)"),
        ("m: o; m[; 1 2]+: 100; m", "(1 102 103;4 105 106)"),
        // what cannot be done changes nothing
        ("m: o; m[5;0]: 1", "'domain"),
        ("m[1;2]: `a", "'type"),
        ("m[;0]: 1 2 3", "'length"),
        ("m[0; 0 1]: 7 8 9", "'length"),
        ("m", "(1 2 3;4 5 6)"),
        ("m[1][2]: 9", "'assign"),
        ("m[1][2]+: 9", "'assign"),
        ("(m[1][2]; m[1;2])", "6 6"),
        ("n[`a;`y]: 50; (value n`a; value n`b)", "(1 50;3 4)"),
        ("n[`a`b; `x]: 0 7; (value n`a; value n`b)", "(0 50;7 4)"),
        ("n[; `y]+: 1; (value n`a; value n`b)", "(0 51;7 5)"),
        ("L[1;1;0]: 42; L", "(1 2;(3 4;42 6))"),
        ("s: (\"ab\"; \"cd\")!(1 2; 3 4); s[\"cd\"; 1]: 9; s \"cd\"", "3 9"),
        ("t[`a; 1]: 20; t`a", "1 20 3"),
        ("t[1; `a]: 5", "'nyi"),
        ("t`a", "1 20 3"),
        ("f: {[m] m[0;0]: 99; m}; (f o; o)", "((99 2 3;4 5 6);(1 2 3;4 5 6))"),
        ("g: {o[0;0]: 50; 0}; g 0; o", "(50 2 3;4 5 6)"),
        // the functional form takes lists of indices as the brackets do
        (".[(1 2 3; 4 5 6); (0 1; 2); :; 10 20]", "(1 2 10;4 5 20)"),
        (".[(1 2 3; 4 5 6); (::; 0); +; 100]", "(101 2 3;104 5 6)"),
        ("x: 1 2 3; x[0 1]: 7 8 9", "'length"),
        ("x[0 1]: 7 8; x", "7 8 3"),
    ]);
}

#[test]
fn appending_rows_to_a_table() {
    check("t: ([] c: `a`b; n: 1 2); u: t", &[
        ("t,: ([] c: enlist `c; n: enlist 3); (t`c; u`c)", "(`a`b`c;`a`b)"),
        ("t,: `c`n!(`d; 4); t`n", "1 2 3 4"),
        ("t,: ([] c: enlist `e; n: enlist `bad)", "'type"),
        ("count t", "4"),
    ]);
}

#[test]
fn amending_an_item_of_a_list() {
    check("x: 10 20 30; y: x; gl: x; f: 1.5 2.5", &[
        ("x[1]: 21; (x; y)", "(10 21 30;10 20 30)"),
        ("x[2]+: 5; x", "10 21 35"),
        ("x[0]: `a", "'type"),
        ("x[3]: 1", "'domain"),
        ("x", "10 21 35"),
        ("f[0]: 7; f", "7 2.5"),
        ("g: {x[0]: 99; x}; (g y; y)", "(99 20 30;10 20 30)"),
        ("h: {gl[0]: 11; 0}; h 0; (gl; y)", "(11 20 30;10 20 30)"),
        ("z: `s#1 2 3; z[0]: 5; attr z", "`"),
    ]);
}

#[test]
fn amending_a_key_of_a_dictionary() {
    check("d: `a`b!1 2; e: d", &[
        ("d[`a]: 10; (value d; value e)", "(10 2;1 2)"),
        ("d[`c]: 3; key d", "`a`b`c"),
        ("d[`c]+: 4; d`c", "7"),
        ("d[`z]+: 4; d`z", "0N"),
        ("d[`a]: `x; (d`a; d`b)", "(`x;2)"),
        ("n: (`long$())!`long$(); do[100; n[count n]: count n]; (count n; sum value n)", "100 4950"),
        ("n[5]: 50; n 5 6", "50 6"),
        ("f: {m: x; m[`b]: 0; (m`b; x`b)}; f e", "0 2"),
        ("count e", "2"),
    ]);
}

#[test]
fn sorting_by_several_columns_keeps_the_order_of_equals() {
    check("t: ([] s: `b`a`b`a`b; n: 2 9 1 9 2; i0: til 5)", &[
        ("(`s xasc t)`i0", "1 3 0 2 4"),
        ("(`s`n xasc t)`i0", "1 3 2 0 4"),
        ("(`s xdesc t)`i0", "0 2 4 1 3"),
        ("(`s`n xdesc t)`i0", "0 4 2 1 3"),
        ("attr (`s`n xasc t)`s", "`s"),
    ]);
}

#[test]
fn updates_of_some_rows_and_with_one_value() {
    check("t: ([] c: `a`b`c; n: 1 2 3)", &[
        ("(update n: 0 from t)`n", "0 0 0"),
        ("(update n: 10 * n from t where c <> `b)`n", "10 2 30"),
        ("(update m: n from t where n > 1)`m", "0N 2 3"),
        ("(update n: 1.5 from t where c = `a)", "'type"),
        ("t`n", "1 2 3"),
    ]);
}

#[test]
fn bulk_rows_into_keyed_and_unkeyed_tables() {
    check("u: ([] k: 0#0; v: 0#0); k: ([k: 1 2] v: 10 20; w: 1 2)", &[
        ("`u insert (1 2 3; 4 5 6); u`v", "4 5 6"),
        ("`u upsert ([] k: 7 8; v: 9 9); count u", "5"),
        ("`k upsert ([] k: 3 1 3 4; v: 30 11 31 40; w: 3 0N 0N 4); exec k from k", "1 2 3 4"),
        ("exec v from k", "11 20 31 40"),
        ("exec w from k", "1 2 3 4"),
    ]);
}
