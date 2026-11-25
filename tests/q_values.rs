//! How values of different types combine, and what verbs do with lists and dictionaries.
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
fn number_types_widen_and_floats_compare_within_a_tolerance() {
    check("", &[
        ("1b+1b", "2i"),
        ("5i*0x03", "15i"),
        ("5i*3", "15"),
        ("3&2.5 8", "2.5 3"),
        ("(7 div -2; 7 mod -2; -7 div 2)", "-4 -1 -4"),
        ("(7 div 2.5; 7 mod 2.5)", "(2;2f)"),
        ("r: 1%6; 1=r+r+r+r+r+r", "1b"),
        ("(1 2f+1e-15 0.1)=1 2f", "10b"),
        ("`a=\"a\"", "'type"),
        ("not (\"a\"; 2000.01.01; 00:00:00; 0)", "0111b"),
        ("(.5; 1 .25)", "(0.5;1 0.25)"),
    ]);
}

#[test]
fn temporal_types_compare_join_and_count_in_the_finer_type() {
    check("", &[
        ("2015.07m=2015.07.01", "1b"),
        ("2015.01.01<2015.02m", "1b"),
        ("12:00=12:00:00.000", "1b"),
        ("2000.01.02=2000.01.02D00:00:00", "1b"),
        ("12:34 01:02:03", "12:34:00 01:02:03"),
        ("01:02:03 12:34 11:59:59.999u", "01:02 12:34 11:59"),
        ("2015.01.01+12:00:00.000000000", "2015.01.01D12:00:00.000000000"),
        ("(12:00+30; 12:30-12:00; 00:00:10+5)", "(12:30;30i;00:00:15)"),
        ("(`date$2000.01.02T12:00:00.000; 2000.01.02T12:00:00.000=1.5)", "(2000.01.02;1b)"),
        ("(12:34:56).mm", "34i"),
        ("type 3?12:00", "17h"),
    ]);
}

#[test]
fn lists_reshape_index_and_keep_their_type() {
    check("L: 1 2 3 4", &[
        ("0N 3#til 7", "(0 1 2;3 4 5;,6)"),
        ("2 0N#til 5", "(0 1;2 3 4)"),
        ("(100 200 300 400)(0 1;2 3)", "(100 200;300 400)"),
        ("(2+)[3]", "5"),
        ("L[3]:`a", "'type"),
        ("L,:`a", "'type"),
        ("M: (1;2;`c); M[2]:3; M", "1 2 3"),
        ("t: ([] a: 1 2; b: `x`y); t,:(`z;3)", "'type"),
        ("t,:(3;`z); count t", "3"),
        ("count 3?0Ng", "3"),
        ("`bad `news", "'bad"),
    ]);
}

#[test]
fn bytes_bits_and_digits_convert() {
    check("", &[
        ("0x0 vs 258i", "0x00000102"),
        ("0x0 sv 0x00000102", "258i"),
        ("0b sv 00000101b", "0x05"),
        ("10 sv 1 2 3", "123"),
        ("10 vs 123", "1 2 3"),
        ("24 60 60 vs 3723", "1 2 3"),
        ("24 60 60 sv 1 2 3", "3723"),
    ]);
}

#[test]
fn dictionaries_combine_by_key() {
    check("a: `a`b`c!10 20 30; b: `b`c`d!20 300 400", &[
        ("(a+b)`a`b`c`d", "10 40 330 400"),
        ("(a^b)`a`b`c`d", "10 20 300 400"),
        ("value a=b", "0100b"),
        ("value a<b", "0011b"),
        ("key a _ `b", "`a`c"),
        ("key `a`c cut a", ",`b"),
        ("d: (1 2; 3 4 5)!10 20; (d 3 4 5; d?20)", "(20;3 4 5)"),
        ("v: (1 2; 3 4 5); (`x`y!v)?3 4 5", "`y"),
    ]);
}

#[test]
fn views_know_what_they_read_and_refuse_loops() {
    check("x: 1; y: 2; w:: x+y; u:: 2*w", &[
        (".z.b `x", ",`w"),
        (".z.b `w", ",`u"),
        ("u", "6"),
        ("x:: u; x", "'loop"),
        (".Q.s1 1 2", "\"1 2\""),
    ]);
}
