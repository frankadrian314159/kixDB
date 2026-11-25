//! `parse`, `eval` and `md5`.
use kixdb::{Connection, Server, K};

const SETUP: &str = "t: ([] s:`a`b`a`c; v:1 2 3 4; px: 1.5 2.5 3.5 4.5; tm: 09:30:00 09:31:00 09:36:00 09:40:00);\
    k: ([s:`a`b] name: `A`B); q: ([] s:`a`a`b; tm: 09:00:00 09:35:00 09:00:00; bid: 1 2 3);\
    x: 10; y: 1 2 3; d: (1 2 3;4 5 6); f: {[a;b] a+b}; add: +";

/// Every kind of statement the parser knows.
const STATEMENTS: &[&str] = &[
    "1+2", "2*3+4", "(2*3)+4", "-x", "neg y", "sum y", "count[y]", "y 1", "y[0 2]", "d[1;2]", "x", "`a", "`a`b", "\"abc\"",
    "1 2 3", "1.5", "()", "(1;`a;\"c\")", "(x;y)", "(`a;`b)", "enlist 1", "enlist `a", "enlist `a`b", "`a`b!1 2", "(`a`b;`c`d)",
    "f[1;2]", "f[1;] 5", "add[1;2]", "{x*x} 4", "{[a] a} each y", "{x+y}/[0;y]", "+/ y", "(+/) y", "0 +/ y", "y +' y",
    "y ,/: 7 8", "y ,\\: 7 8", "-': y", "(+\\) y", "count each d", "+ over y", "f'[y;y]", "f/[y]", "x in y", "y within 1 2",
    "2 xbar y", "3 mavg y", "`s like \"s*\"", "x ~ x", "y,y", "2#y", "-1_y", "\"J\"$\"42\"", "`$\"abc\"", "10$\"ab\"",
    "$[x>5;`big;`small]", "$[x<5;`small;x<20;`mid;`big]", "if[x>5; x: 1]; x", "i:0; do[3;i+:1]; i", "i:0; while[i<4;i+:2]; i",
    "z: 5", "z: x+1; z", "x+: 5; x", "y[1]: 20; y", "y[0 2]+: 100; y", "d[1;0]: 40; d", "g: {[a] if[a>0; :`pos]; `neg}; (g 1; g -1)",
    "@[{x+1}; `a; {x}]", ".[f; (1;2); {x}]", "@[y; 0; +; 5]", "@[`y; 1; *; 2]; y", ".[d; 1 2; +; 10]", "'oops",
    "([] a: 1 2; b: `p`q)", "([k: 1 2] v: 3 4)", "([] a: y; b: x)", "t.px", "tm: t.tm; tm.minute",
    "select from t", "select s, v from t", "select s, n: v*2 from t where v>1", "select from t where s=`a, v>1",
    "select from t where s in `a`b", "select from t where s like \"a\"", "select distinct s from t", "select[2] from t",
    "select[-2] v from t", "select sum v by s from t", "select n: count i, m: max px by s from t where v>1",
    "select vwap: v wavg px by s, 5 xbar tm.minute from t", "select by s from t", "select max v, min px from t",
    "exec v from t", "exec v from t where s=`a", "exec v, px from t", "exec n: v from t", "exec max v from t",
    "exec v by s from t", "exec sum v by s from t", "exec v, px by s from t", "exec a: sum v by s from t",
    "exec v by s, tm from t", "exec v, px by s, tm from t", "exec from t",
    "update v: v*2 from t", "update w: v+px from t where s=`a", "update m: avg px by s from t", "t",
    "update v: 0 from `t where s=`c; t", "delete from t where v>2", "delete px, tm from t", "delete from `t where s=`b; t",
    "delete px from `t; t", "delete from t", "t lj k", "t ij k", "count t uj t", "aj[`s`tm; t; q]", "ej[`s; t; 0!k]",
    "wj[(t.tm - 60; t.tm); `s`tm; t; (q; (max; `bid))]", "select from t where v = (max; v) fby s",
    "select from t where s in exec s from k", "`t insert (`d; 5; 5.5; 09:50:00); count t", "`k upsert (`c; `C); k",
    "\\a", "w:: x*2; x: 4; w", "count \"a;b\"", "x; y", "1;",
];

fn fresh() -> Connection {
    let mut c = Server::new().connect();
    c.eval(SETUP).unwrap();
    c
}

fn outcome(r: Result<K, kixdb::KError>) -> String {
    match r {
        Ok(v) => format!("{} {}", v.type_num(), v),
        Err(e) => format!("'{}", e.signal),
    }
}

#[test]
fn evaluating_a_parse_tree_is_evaluating_the_source() {
    for code in STATEMENTS {
        let (mut direct, mut via) = (fresh(), fresh());
        let expected = outcome(direct.eval(code));
        via.set_vars(vec![(kixdb::sym::intern("src"), Some(K::string(code)))]).unwrap();
        let actual = outcome(via.eval("eval parse src"));
        assert_eq!(actual, expected, "{code}: tree {:?}", via.eval("parse src").map(|k| k.to_string()));
        // the same variables are left behind
        for name in ["t", "k", "x", "y", "d"] {
            assert_eq!(via.eval(name), direct.eval(name), "{code} left a different {name}");
        }
    }
}

#[test]
fn a_tree_survives_ipc_serialization() {
    for code in STATEMENTS {
        let (mut direct, mut via) = (fresh(), fresh());
        let expected = outcome(direct.eval(code));
        via.set_vars(vec![(kixdb::sym::intern("src"), Some(K::string(code)))]).unwrap();
        let actual = outcome(via.eval("eval -9! -8! parse src"));
        assert_eq!(actual, expected, "{code}");
    }
}

#[test]
fn trees_have_the_shape_q_gives_them() {
    let mut c = fresh();
    let mut tree = |code: &str, same_as: &str| {
        let parsed = c.eval(&format!("parse \"{code}\"")).unwrap();
        let built = c.eval(same_as).unwrap();
        assert_eq!(parsed, built, "{code}: {parsed}");
    };
    tree("1+2", "(+;1;2)");
    tree("a+b*2", "(+;`a;(*;`b;2))");
    tree("sum x", "(sum;`x)");
    tree("f[x;1]", "(`f;`x;1)");
    tree("s=`b", "(=;`s;enlist `b)");
    tree("1 2 3", "1 2 3");
}

#[test]
fn trees_can_be_built_and_changed_by_hand() {
    let mut c = fresh();
    let mut check = |code: &str, expected: &str| assert_eq!(c.eval(code).unwrap().to_string(), expected, "{code}");
    check("eval (+;1;2)", "3");
    check("eval (sum;`y)", "6");
    check("p: parse \"exec sum v from t where s=`a\"; eval p", "4");
    check("p: parse \"y*2\"; p[0]: (+); eval p", "3 4 5");
}

#[test]
fn functional_forms_take_the_arguments_parse_gives() {
    let mut c = fresh();
    let mut check = |functional: &str, qsql: &str| assert_eq!(c.eval(functional).unwrap(), c.eval(qsql).unwrap(), "{functional}");
    check("?[t; (); (); `v]", "exec v from t");
    check("?[t; (); (); (max;`v)]", "exec max v from t");
    check("?[t; (); `s; (sum;`v)]", "exec sum v by s from t");
    check("?[t; (); 0b; (); 2]", "select[2] from t");
    check("![t; (); 0b; `px`tm]", "delete px, tm from t");
    check("![t; enlist (>;`v;2); 0b; `symbol$()]", "delete from t where v>2");
}

#[test]
fn md5_gives_the_standard_digest() {
    let mut c = fresh();
    let mut check = |code: &str, expected: &str| assert_eq!(c.eval(code).unwrap().to_string(), expected, "{code}");
    check("md5 \"abc\"", "0x900150983cd24fb0d6963f7d28e17f72");
    check("md5 \"\"", "0xd41d8cd98f00b204e9800998ecf8427e");
    check("count md5 each (\"a\"; \"b\")", "2");
    assert_eq!(c.eval("md5 1").unwrap_err().signal, "type");
    assert_eq!(c.eval("parse 1").unwrap_err().signal, "type");
    assert_eq!(c.eval("parse \"(\"").unwrap_err().signal, "syntax");
}
