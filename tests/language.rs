use kixdb::{Event, Server};

fn eval(code: &str) -> String {
    Server::new().connect().eval(code).map(|k| k.to_string()).unwrap_or_else(|e| format!("'{}", e.signal))
}

fn check(cases: &[(&str, &str)]) {
    for (code, expected) in cases {
        assert_eq!(eval(code), *expected, "{code}");
    }
}

#[test]
fn each_over_scan_and_adverbs_after_operators() {
    check(&[
        ("{x*2} each 1 2 3", "2 4 6"),
        ("first each (1 2;3 4)", "1 3"),
        ("+ over 1 2 3", "6"),
        ("(count each `a`b!(1 2;3 4 5))`b", "3"),
        ("(neg;abs)@'1 -2", "-1 2"),
        ("-2_til 5", "0 1 2"),
        ("f: {42}; f[]", "42"),
        (".Q.s 1 2", "\"1 2\\n\""),
    ]);
}

#[test]
fn early_return_deep_amend_and_functional_constants() {
    check(&[
        ("f: {[a] if[a>0; :`pos]; `neg}; (f 1; f -1)", "`pos`neg"),
        (".[(1 2 3;4 5 6); 1 2; +; 10]", "(1 2 3;4 5 16)"),
        ("t: ([] s:`a`b; v:1 2); exec v from ?[t; enlist (=;`s;enlist `b); 0b; ()]", ",2"),
        ("t: ([] s:`a`b`c; v:1 2 3); exec v from ?[t; enlist (in;`s;enlist `a`c); 0b; ()]", "1 3"),
        ("\\t:3 sum til 10", "0"),
    ]);
}

#[test]
fn empty_vectors_keep_their_types_and_integers_wrap() {
    check(&[
        ("type (`float$()) + 1", "9h"),
        ("type (`long$()) mod 2", "7h"),
        ("type exec a from update a: 1b from 0#([] x: 1 2)", "1h"),
        ("prd 64#2", "0"),
        ("0 < sum 0W 1", "0b"),
        ("t: ([] k: 1 2 3; j: 4 5 6; v: 7 8 9); type exec k, j by v, k from t", "99h"),
    ]);
}

#[test]
fn assignments_print_nothing() {
    let srv = Server::new();
    let events = srv.connect().execute("x: 1 2 3\nsum x").events;
    assert!(matches!(events.as_slice(), [Event::Value(v)] if v.to_string() == "6"), "{events:?}");
}
