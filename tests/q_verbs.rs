//! Further verbs, forms of application and table operations of q.
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
fn application_amends_and_adverbs() {
    check("f: {[] 42}; g: {`a;}; h: {b:1; b::x; b}", &[
        ("(f[]; f[0])", "42 42"),
        ("(::) 42", "42"),
        ("g[]", "::"),
        ("(h 7; (::) 1)", "7 1"),
        ("(abs; neg)[1; 5]", "-5"),
        ("value +[2]", "(+;2;::)"),
        ("(+[2]) 3", "5"),
        ("@[10 20 30; 0 2; :; 42 43]", "42 20 43"),
        ("d: `a`b!(1 2; 3 4); .[d; (`a; 1); neg] `a", "1 -2"),
        (".[(1 2; 3 4); (::; 0); neg]", "(-1 2;-3 4)"),
        ("(+/)[0; 1 2; 10 20]", "'rank"),
        ("{x+y+z}/[0; 1 2; 10 20]", "33"),
        ("(~':) 1 1 2", "010b"),
        ("{[x] 1-x}/[0]", "1"),
        ("value (+; 6; 7)", "13"),
    ]);
}

#[test]
fn blocks_choices_comments_and_self_reference() {
    check("f: {$[x<2; 1; x * .z.s x - 1]}", &[
        ("$[1b; [a: 6; b: 7; a * b]; 0]", "42"),
        ("?[101b; 1 2 3; 0]", "1 0 3"),
        ("exec ?[c > 1; c; 0] from ([] c: 1 2 3)", "0 2 3"),
        ("f 5", "120"),
        ("$[0N; `yes; `no]", "`yes"),
        ("a: 1\n/\na: 2\n\\\na", "1"),
    ]);
}

#[test]
fn built_in_verbs() {
    check("", &[
        ("(all 101b; any 001b)", "01b"),
        ("rank 30 10 20", "2 0 1"),
        ("2 rotate 1 2 3 4", "3 4 1 2"),
        ("2 xprev 10 20 30", "0N 0N 10"),
        ("2 xrank 30 10 40 20", "1 0 1 0"),
        ("(2 0f; 0 4f) mmu inv (2 0f; 0 4f)", "(1 0f;0 1f)"),
        ("1 2 3f mmu 1 2 3f", "14f"),
        ("(`long$2.7; `int$-2.2; 7h$\"a\")", "(3;-2i;97)"),
        ("\"ijf\"$10 20 30", "(10i;20;30f)"),
        ("(\"D\"$\"12.31.2014\"; \"D\"$\"20141231\")", "2014.12.31 2014.12.31"),
        ("`$\" abc \"", "`abc"),
        ("\"fun\" ss \"f?n\"", ",0"),
        ("` sv `:root`a`b", "`:root/a/b"),
        ("avg (1 2; 3 4)", "2 3f"),
        ("1.5 xbar 1 2 4", "0 1.5 3"),
        ("key 3", "0 1 2"),
        (".jab.a: 1; .jab.b: 2; key `.jab", "`a`b"),
        ("(get `.jab)`b", "2"),
    ]);
}

#[test]
fn tables_as_lists_of_rows() {
    check("t: ([] c: `a`b`c; n: 10 20 30); kt: ([k: 1 2 3] v: `x`y`z)", &[
        ("(t _ 1)`c", "`a`c"),
        ("(([] k: 1 3) # kt)[3] `v", "`z"),
        ("count ([] k: 1 3) _ kt", "1"),
        ("t ? `c`n!(`b; 20)", "1"),
        ("(kt ? `y)`k", "2"),
        ("count t union ([] c: `c`d; n: 30 40)", "4"),
        ("(t except 1#t)`c", "`b`c"),
        ("cols t ,' ([] m: 1 2 3)", "`c`n`m"),
        ("(0^([] a: 1 0N))`a", "1 0"),
        ("t[`n`c] 0", "10 20 30"),
        ("(t 1)`n", "20"),
        ("(t[1;]) `n", "20"),
        ("type (`a`b!1 2; `a`b!3 4)", "98h"),
        ("`n xdesc `t; t`n", "30 20 10"),
        ("`t insert (`d; 40); count t", "4"),
        ("`t insert ((`e; 1); (`f; 2))", "'type"),
        ("`fresh insert ([] a: 1 2); count fresh", "2"),
        ("p: {[t] update n: 0 from t}; p `t; sum t`n", "0"),
        ("update n: 1 2 from t where c in `a`b`d", "'length"),
        ("update n: 1.5 from t where c = `a", "'type"),
        ("cols ([k: 1 2]; v: 3 4)", "`k`v"),
        ("cols ([] 1 2; 3 4)", "`x`x1"),
        ("cols select c, c, 2*n, n+1, 100 xbar n from t", "`c`c1`x`n`n1"),
        ("t,: `c`n!(`g; 1.5)", "'type"),
        ("t, ([] n: 1#1; c: 1#`g)", "'mismatch"),
    ]);
}

#[test]
fn find_gives_the_first_place_of_each() {
    check("x: 5 7 5 9 0N; f: 1.5 -0.0 0n 1.5; s: `b`a`b; m: (1; `a; \"cd\"; 2.5); t: ([] c: (\"ab\"; \"cd\"; \"ab\")); long: (til 1000), til 1000", &[
        ("(x ? 5; x ? 9; x ? 4; x ? 0N)", "0 3 5 4"),
        ("(x ? 5i; x ? 5.0; x ? `a)", "5 5 5"),
        ("(f ? 1.5; f ? 0.0; f ? 0n; f ? 2.5)", "0 1 2 4"),
        ("(s ? `b; s ? `z)", "0 3"),
        ("(m ? `a; m ? \"cd\"; m ? 2.5; m ? 9)", "1 2 3 4"),
        ("x ? 9 5 4", "3 0 5"),
        ("x ? 0#0", "`long$()"),
        // many at once, of the list's type and of others
        ("long ? 999 0 5 5 1000 -1 7 8 9 10", "999 0 5 5 2000 2000 7 8 9 10"),
        ("(f ? 10#1.5 0n; s ? 10#`a`z)", "(0 2 0 2 0 2 0 2 0 2;1 3 1 3 1 3 1 3 1 3)"),
        ("x ? 10#5.0", "5 5 5 5 5 5 5 5 5 5"),
        ("(t`c) ? \"cd\"", "1"),
        ("(t`c) ? 10#(\"cd\"; \"zz\")", "1 3 1 3 1 3 1 3 1 3"),
        ("(t`c) ? (\"ab\"; \"zz\")", "0 3"),
        // (a string that is not there is looked for as its characters, none of which is a string)
        ("(t`c) ? \"zz\"", "3 3"),
    ]);
}

#[test]
fn all_and_any_give_booleans_even_of_nothing() {
    check("t: ([] c: 1 2; n: 1 0); e: 0#t", &[
        ("(all `boolean$(); any 0#0b)", "10b"),
        ("(all (); any ())", "10b"),
        ("(all `long$(); any `float$())", "10b"),
        ("(all \"\"; any `date$())", "10b"),
        ("(min `boolean$(); (|/) `boolean$())", "10b"),
        ("type `boolean$`long$()", "1h"),
        ("`$\"\"", "`"),
        ("(all `a`b!11b; any `a`b!01b)", "11b"),
        ("all (`symbol$())!()", "1b"),
        ("(value all t; value any t)", "(10b;11b)"),
        ("(value all e; value any e)", "(11b;00b)"),
        ("value all ([k: 1 2] v: 1 1)", ",1b"),
        ("value exec all n, any n from e", "10b"),
    ]);
}
