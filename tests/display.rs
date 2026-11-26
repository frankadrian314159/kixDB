//! What the console prints, and the one-line form that `-3!` gives.
use kixdb::{display, Server};

fn check(setup: &str, show: impl Fn(&kixdb::K) -> String, cases: &[(&str, &str)]) {
    let server = Server::new();
    let mut c = server.connect();
    c.eval(setup).unwrap();
    for (code, expected) in cases {
        assert_eq!(show(&c.eval(code).unwrap()), *expected, "{code}");
    }
}

#[test]
fn one_line_form() {
    check("", |k| k.q_repr(), &[
        ("1%3", "0.3333333"),
        ("1e10 1.5", "1e+10 1.5"),
        ("(1234567.0; 123456789.0; 0.00001)", "1234567 1.234568e+08 1e-05"),
        ("2 3f", "2 3f"),
        ("2000.01.02T12:00:00.000", "2000.01.02T12:00:00.000"),
        ("`a`b!1 2", "`a`b!1 2"),
        ("(enlist `a)!enlist 1", "(,`a)!,1"),
        ("([] a: 1 2; b: `x`y)", "+`a`b!(1 2;`x`y)"),
        ("([k: 1 2] v: 3 4)", "(+(,`k)!,1 2)!+(,`v)!,3 4"),
        ("{x+y+z}[1]", "{x+y+z}[1]"),
        ("-3!(1;`a;\"b\")", "\"(1;`a;\\\"b\\\")\""),
    ]);
}

#[test]
fn console_form() {
    check("", display::console, &[
        ("(1; 1.5; `a; \"bc\")", "1\n1.5\n`a\n\"bc\""),
        ("(1 2 3; 10 20)", "1 2 3\n10 20"),
        ("(1 2 3f; 10 200 3000f)", "1  2   3\n10 200 3000"),
        ("::", ""),
        ("`a`bb!10 20", "a | 10\nbb| 20"),
        ("`name`iq!(`Dent; 98)", "name| `Dent\niq  | 98"),
        ("`c1`c2!(`a`bbbb; 100 2)", "c1| a   bbbb\nc2| 100 2"),
        ("`x`y!10b", "x| 1\ny| 0"),
        ("([] b: 10b; f: 1.5 0n; s: (\"ab\"; \"c\"))", "b f   s\n----------\n1 1.5 \"ab\"\n0     \"c\""),
        ("til 100", "0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28.."),
    ]);
}

#[test]
fn precision_and_console_size_are_settings() {
    let server = Server::new();
    let mut c = server.connect();
    let mut shown = |code: &str| display::console(&c.eval(code).unwrap());
    assert_eq!(shown("\\P"), "7i");
    shown("\\P 0");
    assert_eq!(shown("1%3"), "0.33333333333333331");
    shown("\\P 3");
    assert_eq!((shown("1%3"), shown("string 2%3")), ("0.333".into(), "\"0.667\"".into()));
    shown("\\P 7");
    assert_eq!(shown("\\c"), "25 80i");
    shown("\\c 8 20");
    assert_eq!(shown("([] a: til 9)"), "a\n-\n0\n1\n2\n..");
    assert_eq!(shown("til 30"), "0 1 2 3 4 5 6 7 8..");
}
