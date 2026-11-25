//! The typed loops over whole vectors give what applying the verb atom by atom gives, aggregates computed for all groups
//! at once give what each group gives alone, and `where` reads only the columns a query names without changing what it
//! can see.
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
fn narrow_and_wide_ranges_of_values_look_up_and_group_alike() {
    let mut c = Server::new().connect();
    // `a` spans few values (found in a table with a slot for each), `w` the same ones spread far apart (hashed)
    c.eval("a: -3 + 2000?7; b: 2000?`x`y`z; d: 1999.12.29 + a; w: a * 1000000000000").unwrap();
    c.eval("t: ([] a; b; d; w); p: 2000?100000; q: 2000?100000").unwrap();
    // long lists, with values below, between and above theirs, and nulls and infinities on either side
    for list in ["-3 -1 1 2 3 5 7 9 11 13", "til 100", "-100 + til 98", "0N 0W -0W 1 2 3 4 5 6 7", "(til 10), 1000000000000"] {
        for values in ["a", "a, 0N 0W -0W"] {
            agree(&mut c, &format!("({values}) in {list}"), &format!("{{x in {list}}} each ({values})"));
        }
    }
    agree(&mut c, "d in 1999.12.29 + til 10", "a in til 10");
    for (narrow, wide) in [
        ("value group a", "value group w"),
        ("value group d", "value group w"),
        ("1000000000000 * distinct a", "distinct w"),
        ("value select i by a, b from t", "value select i by w, b from t"),
        ("value select i by b, a from t", "value select i by b, w from t"),
        // two columns whose pairs are too many for a table
        ("count distinct ([] p; q)", "count distinct flip (p; q)"),
    ] {
        agree(&mut c, narrow, wide);
    }
    assert_eq!(eval(&mut c, "(count distinct `long$(); count select by a, b from 0#t; (`long$()) in til 20)"), "(0;0;`boolean$())");
}

#[test]
fn aggregates_over_all_groups_at_once_match_the_group_by_group_results() {
    let mut c = Server::new().connect();
    c.eval(
        "t: ([] k: 2 1 2 1 3 2; j: 1 0N 3 4 5 6; f: 1.5 2 0n 4 5 6; s: `a`b`a``b`a; d: 2000.01.01 + 5 0N 3 1 2 4; \
         w: (\"p\"; \"qq\"; \"\"; \"r\"; \"s\"; \"t\"))",
    )
    .unwrap();
    c.eval("some: 1 3 5").unwrap();
    let aggregates = ["sum", "count", "avg", "min", "max", "first", "last", "med", "prd", "var", "dev", "svar", "sdev"];
    let mut exprs: Vec<String> = aggregates.iter().flat_map(|a| ["j", "f", "s", "d", "w", "i"].map(|x| format!("{a} {x}"))).collect();
    exprs.extend(
        [
            "count distinct s", "last distinct j", "sum f * 1 - j", "sum j in some", "sum not s in `a`c", "sum j within 2 5",
            "(sum j * s = `a) % sum j", "100.0 * (sum f) % count i", "0 ^ min j", "neg max f", "(first d) + count i",
            "j wsum f", "j wavg f", "not min d", "null first w", "count first w", "sum 1", "first some", "sum j + some",
            // the same for every group
            "1b", "2 + 3", "neg 2.5", "`x", "0n",
        ]
        .map(String::from),
    );
    for e in &exprs {
        for tail in ["by k from t", "by s, k from t where i <> 2", "by k: 2 xbar k from t"] {
            // a lambda around the expression has it evaluated group by group
            let (whole, each) = (format!("select r: {e} {tail}"), format!("select r: {{x}} {e} {tail}"));
            match eval(&mut c, &whole) {
                error if error.starts_with('\'') => assert_eq!(error, eval(&mut c, &each), "{whole}"),
                _ => agree(&mut c, &whole, &each),
            }
        }
    }
    for (code, expected) in [
        ("value exec sum j by k from t", "4 10 5"),
        ("(select ok: 1b, n: count i by k from t) ~ ([k: 1 2 3] ok: 111b; n: 2 3 1)", "1b"),
        // keys sort by every column, the first one first
        ("(key select count i by s, k from t) ~ ([] s: ``a`b`b; k: 1 2 1 3)", "1b"),
        ("exec r from select r: (sum j) % count i, q: sums j by k from t", "2 3.3333333 5"),
    ] {
        assert_eq!(eval(&mut c, code), expected, "{code}");
    }
}

#[test]
fn patterns_and_filters_match_what_they_should() {
    let mut c = Server::new().connect();
    // a `*` does not cross lines, so the fourth text, of two lines, matches none of these
    c.eval("s: (\"ab\"; \"abc\"; \"cab\"; \"a\nb\"; \"\"; \"ba\")").unwrap();
    for (pattern, expected) in [
        ("a*", "110000b"),
        ("*b", "101000b"),
        ("*b*", "111001b"),
        ("a*c", "010000b"),
        ("*", "111011b"),
        ("**", "111011b"),
        ("", "000010b"),
        ("?b", "100000b"),
        ("[bc]a*", "001001b"),
    ] {
        for text in ["s", "(`$s)"] {
            assert_eq!(eval(&mut c, &format!("{text} like \"{pattern}\"")), expected, "{text} like {pattern}");
        }
    }
    // characters that mean something to a regular expression stand for themselves
    assert_eq!(eval(&mut c, "(\"a.b\"; \"axb\"; \"a+(b\"; \"a$\") like \"a.*\""), "1000b");
    assert_eq!(eval(&mut c, "(\"a+(b\" like \"*+(*\"; \"a$\" like \"*$\"; `ab like \"*b\")"), "111b");
    // clause by clause, keeping many rows, few, all or none, gives the rows that all the conditions together give
    c.eval("t: ([] id: til 997; a: (til 997) mod 10; b: (til 997) mod 7)").unwrap();
    for (a, b) in [(-1, 7), (4, 3), (8, 1), (9, 7), (-1, 0)] {
        agree(&mut c, &format!("exec id from t where a > {a}, b < {b}"), &format!("where (t[`a] > {a}) & t[`b] < {b}"));
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
        ("exec count i from t where a > 1, i > 0", "3"),
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

#[test]
fn large_vectors_split_across_threads_give_the_same_answers_in_the_same_order() {
    // (long enough for every filter, aggregate and grouping to be shared out)
    let mut c = kixdb::Server::new().connect();
    c.eval("n: 300000; t: ([] g: (til n) mod 7; h: (til n) mod 100003; s: `a`bb`c ((til n) mod 3); x: til n; f: 0.5 * til n)").unwrap();
    let n = 300_000i64;
    let by_group: Vec<i64> = (0..7).map(|g| (0..n).filter(|x| x % 7 == g).sum()).collect();
    let by_group = by_group.iter().map(i64::to_string).collect::<Vec<_>>().join(" ");
    for (code, expected) in [
        ("exec (sum x; sum x + x; min x; max x) from t", format!("({}; {}; 0; {})", n * (n - 1) / 2, n * (n - 1), n - 1)),
        ("exec (sum f; avg f; min f; max f) from t", format!("({:?}; {:?}; 0.0; {:?})", (n * (n - 1) / 2) as f64 / 2.0, (n - 1) as f64 / 4.0, (n - 1) as f64 / 2.0)),
        ("(max 0n , 0.5 * til n; min 0N , 5 + til n)", format!("({:?}; 5)", (n - 1) as f64 / 2.0)),
        ("exec x from t where 5 = x mod 100000", "5 100005 200005".into()),
        ("count select from t where x >= 1000, x < 201000, s like \"b*\"", "66667".into()),
        ("exec sum x by g from t", format!("(til 7)!{by_group}")),
        ("(distinct exec h from t) ~ til 100003", "1b".into()),
        ("exec (count i; first x; last x) from t where h = 100002", "2 100002 200005".into()),
        ("count select count i by h from t", "100003".into()),
        ("exec count i by s from t", "`a`bb`c!100000 100000 100000".into()),
        ("count select count i by g, s from t", "21".into()),
        ("(t[`x] iasc exec g from t) 0 1 2", "0 7 14".into()),
    ] {
        let got = c.eval(code).unwrap_or_else(|e| panic!("{code}: {e}"));
        assert_eq!(got, c.eval(&expected).unwrap(), "{code}: {got}");
    }
}
