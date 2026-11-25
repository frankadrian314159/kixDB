//! The typed loops over whole vectors give what applying the verb atom by atom gives, and `where` reads only the
//! columns a query names without changing what it can see.
use kixdb::{Connection, Server};

fn eval(c: &mut Connection, code: &str) -> String {
    c.eval(code).map(|k| k.to_string()).unwrap_or_else(|e| format!("'{}", e.signal))
}

/// Numbers and temporals of each width, with nulls.
const NUMBERS: [&str; 8] = [
    "1 2 0N 4 5",
    "5 2 3 0N 1i",
    "1 0N 3 7 5h",
    "1 0n 3.5 -0.0 5",
    "0n 2 3.5 0 4e",
    "10101b",
    "2000.01.02 0Nd 2000.01.04 2000.01.05 2000.01.03",
    "00:00:00.001 00:00:00.002 0Nt 00:00:00.005 00:00:00.004",
];
const OTHERS: [&str; 4] = ["\"abc d\"", "\"dcb a\"", "`b``a`c`b", "`c`b`a``b"];

fn agree(c: &mut Connection, whole: &str, by_atom: &str) {
    assert_eq!(eval(c, &format!("({whole}) ~ {by_atom}")), "1b", "{whole}  against  {by_atom}");
}

#[test]
fn comparisons_and_arithmetic_match_the_atom_by_atom_results() {
    let mut c = Server::new().connect();
    let numbers = NUMBERS.iter().flat_map(|a| NUMBERS.iter().map(move |b| (a, b)));
    let others = [(0, 0), (0, 1), (2, 2), (2, 3)].map(|(a, b)| (&OTHERS[a], &OTHERS[b]));
    for (a, b) in numbers.clone().chain(others) {
        c.eval(&format!("a: {a}; b: {b}")).unwrap();
        for op in ["=", "<>", "<", "<=", ">", ">="] {
            agree(&mut c, &format!("a {op} b"), &format!("a {op}' b"));
            agree(&mut c, &format!("a {op} b 1"), &format!("a {op}\\: b 1"));
            agree(&mut c, &format!("(a 2) {op} b"), &format!("(a 2) {op}/: b"));
        }
    }
    // arithmetic with a float on either side, and long with long
    let floats = |s: &str| s.contains('.') && !s.contains(':') && !s.starts_with("2000") || s.ends_with('e');
    for (a, b) in numbers.filter(|(a, b)| floats(a) || floats(b) || (**a == NUMBERS[0] && **b == NUMBERS[0])) {
        c.eval(&format!("a: {a}; b: {b}")).unwrap();
        for op in ["+", "-", "*", "%"] {
            agree(&mut c, &format!("a {op} b"), &format!("a {op}' b"));
            agree(&mut c, &format!("a {op} b 3"), &format!("a {op}\\: b 3"));
            agree(&mut c, &format!("(a 3) {op} b"), &format!("(a 3) {op}/: b"));
        }
    }
    assert_eq!(eval(&mut c, "1 2 3 = 1 2"), "'length");
    assert_eq!(eval(&mut c, "1 2 3f + 1 2f"), "'length");
    assert_eq!(eval(&mut c, "type each ((`float$()) = 1; 1 - `float$(); (`long$()) + `long$())"), "1 9 7h");
}

#[test]
fn in_and_within_match_the_atom_by_atom_results() {
    let mut c = Server::new().connect();
    for a in NUMBERS.iter().chain(&OTHERS) {
        c.eval(&format!("a: {a}")).unwrap();
        // short and long lists take different paths; so does a list of another type
        for list in ["2#a", "1_a", "12#reverse a", "a 1", "1 4", "`a`zz", "(1;`a;2.5)"] {
            agree(&mut c, &format!("a in {list}"), &format!("{{x in {list}}} each a"));
        }
        for range in ["(a 0; a 3)", "(a 3; a 0)", "(a 1; a 4)", "1 4", "0.5 4.5"] {
            if !(a.starts_with(['"', '`']) && range.contains('.')) {
                agree(&mut c, &format!("a within {range}"), &format!("{{x within {range}}} each a"));
            }
        }
    }
}

#[test]
fn grouping_and_distinct_keep_the_order_of_first_appearance() {
    let mut c = Server::new().connect();
    for (code, expected) in [
        ("distinct 3 1 3 0N 1 0N", "3 1 0N"),
        ("count distinct 0n 0n -0.0 0.0", "2"),
        ("distinct `b`a``b`", "`b`a`"),
        ("count distinct (\"ab\";\"cd\";\"ab\")", "2"),
        ("count distinct (1;1i;1)", "2"),
        ("g: group \"abab\"; (key g; value g) ~ (\"ab\"; (0 2; 1 3))", "1b"),
        ("t: ([] a: 1 1 2 2 1; b: `x`y`x`x`x; v: 1 2 3 4 5); exec v from 0!select sum v by a, b from t", "6 2 7"),
        ("count distinct select a, b from t", "3"),
        ("exec m from update m: sum v by a, b from t", "6 2 7 7 6"),
        ("exec v from t where v = (max; v) fby ([] a; b)", "2 4 5"),
    ] {
        assert_eq!(eval(&mut c, code), expected, "{code}");
    }
}

#[test]
fn queries_see_the_columns_they_name_however_they_name_them() {
    let mut c = Server::new().connect();
    c.eval("t: ([] a: 1 2 3 4; b: 10 20 30 40; c: `w`x`y`z; s: (\"p\"; \"qq\"; \"r\"; \"ss\"))").unwrap();
    c.eval("alias: value").unwrap();
    for (code, expected) in [
        ("exec c from t where a > 1, b < 40", "`x`y"),
        ("exec alias \"a + b\" from t where a > 3", ",44"),
        ("count select i from t where a > 1, b < 40", "2"),
        ("exec count i from t where a > 1, i > 0", "2"),
        ("cols select from t where a > 1, b < 40", "`a`b`c`s"),
        ("exec s from t where c in `x`z, a > 2", ",\"ss\""),
        // names reached at run time
        ("exec value \"a + b\" from t where a > 1, value \"b < 40\"", "22 33"),
        ("exec get `b from t where a > 3", ",40"),
        ("exec eval (+;`a;`b) from t where a > 3", ",44"),
        ("exec d from update d: a + b from t where b > 10, c <> `z", "0N 22 33 0N"),
        ("exec m from update m: sum b by c from t where a > 2", "0N 0N 30 40"),
    ] {
        assert_eq!(eval(&mut c, code), expected, "{code}");
    }
}
