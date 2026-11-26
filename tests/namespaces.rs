//! The `.Q` utilities and `.j` (JSON) functions, with the examples of KX's reference.
use kixdb::{Connection, Server};

fn check(c: &mut Connection, cases: &[(&str, &str)]) {
    for (code, expected) in cases {
        assert_eq!(c.eval(code).map(|k| k.to_string()).unwrap_or_else(|e| format!("'{}", e.signal)), *expected, "{code}");
    }
}

fn session() -> Connection {
    Server::new().connect()
}

#[test]
fn constants_dates_and_text() {
    check(&mut session(), &[
        (".Q.A", "\"ABCDEFGHIJKLMNOPQRSTUVWXYZ\""),
        (".Q.an", "\"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789\""),
        (".Q.b6", "\"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/\""),
        ("(.Q.n; .Q.t ? \"j\"; .Q.k)", "(\"0123456789\";7;4.1)"),
        (".Q.addmonths[2007.10.16; 6 7]", "2008.04.16 2008.05.16"),
        (".Q.addmonths[2006.10.29; 4]", "2007.03.01"),
        (".Q.f[2;] each 9.996 34.3445 -.02", "(\"10.00\";\"34.34\";\"-0.02\")"),
        (".Q.fmt[6;2] each 1 234", "(\"  1.00\";\"234.00\")"),
        (".Q.fmt[3;2; 234]", "\"***\""),
        (".Q.dd[`:dir; `file]", "`:dir/file"),
        (".Q.dd[`AAPL; \"O\"]", "`AAPL.O"),
        (".Q.id each `$(\"ab\"; \"a/b\"; \"two words\"; \"2drifters\"; \"2+2\"; \"_\"; \"+\")", "`ab`ab`twowords`a2drifters`a22`a_`a"),
        ("cols .Q.id (`$(\"count+\"; \"count*\"; \"count1\")) xcol ([] 1 2; 3 4; 5 6)", "`count1`count11`count12"),
        ("cols .Q.id (`$(\"aa\"; \"=\"; \"+\")) xcol ([] 1 2; 3 4; 5 6)", "`aa`a`a1"),
        (".Q.s1 `a`b!1 2", "\"`a`b!1 2\""),
    ]);
}

#[test]
fn encodings_addresses_and_compression() {
    check(&mut session(), &[
        (".Q.btoa \"Hello World!\"", "\"SGVsbG8gV29ybGQh\""),
        ("10h$.Q.atob \"aGVsbG8=\"", "\"hello\""),
        (".Q.atob \"aGVsbG8\"", "'domain"),
        (".Q.sha1 \"Hello World!\"", "0x2ef7bde608ce5404e97d5f042f95f89f1c232871"),
        (".Q.x10 12345", "\"AAAAAAADA5\""),
        (".Q.j10 each .Q.x10 each 12346 12347", "12346 12347"),
        (".Q.x12 12345", "\"0000000009IX\""),
        (".Q.j12 .Q.x12 12345", "12345"),
        (".Q.addr `$\"127.0.0.1\"", "2130706433i"),
        ("s: 10000#\"helloworld\"; z: .Q.gz (9; s); (100 > count z; s ~ .Q.gz z; .Q.gz[::])", "111b"),
        (".Q.gz \"not packed\"", "'domain"),
    ]);
}

#[test]
fn tables_and_functions() {
    let mut c = session();
    for statement in ["t: ([] a: 3 4 5; b: \"abc\"; c: (3; \"xy\"; `ab); d: (3 4; 5 3; 4 5); e: (\"abc\"; \"de\"; \"fg\"))", "k: ([s: `a`b`c] v: 1 2 3)"] {
        c.eval(statement).unwrap();
    }
    check(&mut c, &[
        (".Q.ty each t `a`b`c`d`e", "\"jc JC\""),
        ("(.Q.qt t; .Q.qt k; .Q.qt 1)", "110b"),
        ("cols .Q.ff[([] a: 1 2); ([] a: 3 4; z: `x`y)]", "`a`z"),
        (".Q.ff[([] a: 1 2); ([] z: enlist 7)] `z", "0N 0N"),
        ("exec v from .Q.ft[{1 _ x}; k]", "2 3"),
        ("keys .Q.ft[{1 _ x}; k]", ",`s"),
        (".Q.fu[{x * x}; 3 1 3 3]", "9 1 9 9"),
        (".Q.fc[{2 * x}; 1 2 3]", "2 4 6"),
        (".Q.V[k] `v", "1 2 3"),
        ("(.Q.v `k) ~ k", "1b"),
        (".Q.v 42", "42"),
        ("kt: ([e: 1 2] n: `x`y); (.Q.fk `kt$2 1; .Q.fk 1 2)", "`kt`"),
        (".Q.ts[+; 2 3] 1", "5"),
        (".Q.trp[{x + 1}; 2; {[e; bt] e}]", "3"),
        (".Q.trp[{`hello + x}; 2; {[e; bt] e}]", "\"type\""),
        (".Q.trpd[{x + y}; (1; `b); {[e; bt] e}]", "\"type\""),
        // the backtrace holds the functions that were running, the innermost first
        ("inner: {`a + x}; outer: {inner x}; .Q.trp[outer; 1; {[e; bt] bt}]", "(\"{`a + x}\";\"{inner x}\")"),
        (".Q.sbt .Q.trp[outer; 1; {[e; bt] bt}]", "\"  [1]  {`a + x}\\n  [0]  {inner x}\\n\""),
        ("(key .Q.w[]) 0 6", "`used`syms"),
        (".Q.gc[]", "0"),
        // a variable takes the place of a built-in
        (".Q.f: {[x; y] `mine}; .Q.f[2; 1]", "`mine"),
    ]);
}

#[test]
fn command_line_options() {
    check(&mut session(), &[
        ("o: .Q.opt (\"path\"; \"-abc\"; \"123\"; \"-flag\"; \"-xyz\"; \"-7\"); key o", "`abc`flag`xyz"),
        ("o `xyz", ",\"-7\""),
        (".Q.x", ",\"path\""),
        ("d: .Q.def[`abc`xyz`efg!(1; 2.; `a)] .Q.opt (\"-abc\"; \"123\"; \"-xyz\"; \"321\"); value d", "(123;321f;`a)"),
    ]);
}

#[test]
fn json_is_written_and_read() {
    check(&mut session(), &[
        (".j.j `a`b!(0 1; (\"hello\"; \"world\"))", "\"{\\\"a\\\":[0,1],\\\"b\\\":[\\\"hello\\\",\\\"world\\\"]}\""),
        (".j.j ([] a: 1 2; b: `Greetings`Earthlings)", "\"[{\\\"a\\\":1,\\\"b\\\":\\\"Greetings\\\"},{\\\"a\\\":2,\\\"b\\\":\\\"Earthlings\\\"}]\""),
        (".j.j -0w 0 1 2 3 0w", "\"[-inf,0,1,2,3,inf]\""),
        (".j.jd (-0w 0 1 0w; (enlist `null0w)!enlist 1b)", "\"[null,0,1,null]\""),
        (".j.j (1b; 0N; 2026.11.25; 2026.11m; 12:34:56.789; 0Nd; \"a\\\"b\")", "\"[true,null,\\\"2026-11-25\\\",\\\"2026-11\\\",\\\"12:34:56.789\\\",\\\"\\\",\\\"a\\\\\\\"b\\\"]\""),
        ("r: .j.k .j.j `a`b!(0 1; (\"hello\"; \"world\")); r `a", "0 1f"),
        ("r `b", "(\"hello\";\"world\")"),
        ("t: .j.k .j.j ([] a: 1 2; b: `x`y); (type t; cols t; t `a)", "(98h;`a`b;1 2f)"),
        (".j.k \"{\\\"code\\\": 3, \\\"ok\\\": true, \\\"none\\\": null}\"", "`code`ok`none!(3f;1b;0n)"),
        (".j.k \"{oops\"", "'parse"),
    ]);
}

#[test]
fn delimited_text_xml_and_spreadsheets() {
    let mut c = session();
    c.eval("t: ([] a: 1 2 3; b: `x`y`z)").unwrap();
    check(&mut c, &[
        (".h.cd t", "(\"a,b\";\"1,x\";\"2,y\";\"3,z\")"),
        (".h.cd (`a`b`c; 1 2 3; \"xyz\")", "(\"a,1,x\";\"b,2,y\";\"c,3,z\")"),
        (".h.td 1 # t", "(\"a\\tb\";\"1\\tx\")"),
        // fields that need it are quoted, and read back
        (".h.cd ([] s: (\"a,b\"; \"say \\\"hi\\\"\"))", "(,\"s\";\"\\\"a,b\\\"\";\"\\\"say \\\"\\\"hi\\\"\\\"\\\"\")"),
        ("((enlist \"*\"; enlist \",\") 0: \",\" 0: ([] s: (\"a,b\"; \"c\"))) `s", "(\"a,b\";,\"c\")"),
        ("n: ([] c: (1 2; 3 4)); .h.cd n", "(,\"c\";\"1 2\";\"3 4\")"),
        (".h.d: \"*\"; .h.cd n", "(,\"c\";\"1*2\";\"3*4\")"),
        (".h.xd 1 # t", "(\"<R>\";\"<r><a>1</a><b>x</b></r>\";\"</R>\")"),
        ("x: .h.ed t; (count x; x 4)", "(9;\"<Row><Cell><Data ss:Type=\\\"Number\\\">1</Data></Cell><Cell><Data ss:Type=\\\"String\\\">x</Data></Cell></Row>\")"),
        ("count .h.edsn `p`q!(t; t)", "15"),
        (".h.tx[`csv] 1 # t", "(\"a,b\";\"1,x\")"),
        (".h.tx[`json] 2 # t", "(\"{\\\"a\\\":1,\\\"b\\\":\\\"x\\\"}\";\"{\\\"a\\\":2,\\\"b\\\":\\\"y\\\"}\")"),
        ("(.h.xt[`json; (\"{\\\"foo\\\":\\\"bar\\\"}\"; \"{\\\"this\\\":\\\"that\\\"}\")] 0) `foo", "\"bar\""),
    ]);
}

#[test]
fn markup_and_escapes() {
    check(&mut session(), &[
        (".h.xs \"Arthur & Co.\"", "\"Arthur &amp; Co.\""),
        (".h.hc \"<foo>\"", "\"&lt;foo>\""),
        (".h.hr \"foo\"", "\"---\""),
        (".h.hu \"http://www.kx.com\"", "\"http%3a%2f%2fwww.kx.com\""),
        (".h.uh \"http%3a%2f%2fwww.kx.com\"", "\"http://www.kx.com\""),
        ("(.h.hug \"a\") \"a /\"", "(,\"a\";\"%20\";\"%2f\")"),
        ("d: \"abc\"!10 20 30; d \"cab\"", "30 10 20"),
        (".h.iso8601 100", "\"2000-01-01T00:00:00.000000100\""),
        (".h.code \"foo\\tbar\"", "\"foo <code><nobr>bar</nobr></code>\""),
        (".h.ha[`http://www.example.com; \"Main\"]", "\"<a href=http://www.example.com>Main</a>\""),
        (".h.hb[\"u\"; \"Main\"]", "\"<a target=v href=\\\"u\\\">Main</a>\""),
        (".h.htc[`tag; \"value\"]", "\"<tag>value</tag>\""),
        (".h.htac[`a; (`href`target)!(\"u\"; \"_blank\"); \"Main\"]", "\"<a href=\\\"u\\\" target=\\\"_blank\\\">Main</a>\""),
        (".h.http \"see http://www.example.com now\"", "\"see <a href=\\\"http://www.example.com\\\">http://www.example.com</a> now\""),
        (".h.nbr \"foo bar\"", "\"<nobr>foo bar</nobr>\""),
        (".h.pre (\"foo\"; \"bar\")", "\"<pre>foo\\nbar\\n</pre>\""),
        (".h.text (\"foo\"; \"bar\")", "\"<p>foo</p>\\n<p>bar</p>\\n\""),
        (".h.ty `json", "\"application/json\""),
        ("(19 # .h.html \"<p>hi</p>\"; -23 # .h.html \"<p>hi</p>\")", "(\"<html><head><style>\";\"<p>hi</p></body></html>\")"),
    ]);
}
