//! A table keeps a column of strings packed (`ColVec::Strings`). Nothing that reads it may tell: every expression here
//! gives, for the column `p` of a stored table, what it gives for the general list `l` of the same strings.
use kixdb::ipc::{decode_k_payload, encode_k_payload, encode_k_stored};
use kixdb::{ColVec, Connection, Server, StorageOptions, K};

const ITEMS: &str = "(\"apple\"; \"banana\"; \"\"; \"cherry\"; \"apple\"; \" 42 \")";

/// What `code` gives, as `-3!` shows it, or the signal it raises.
fn shown(c: &mut Connection, code: &str) -> String {
    match c.eval(code) {
        Ok(K::Null) => "::".into(),
        Ok(value) => format!("{} {}", value.type_num(), value.q_repr()),
        Err(e) => format!("'{}", e.signal),
    }
}

fn is_packed(value: &K) -> bool {
    matches!(value, K::Vector(ColVec::Strings(..)))
}

fn setup() -> Server {
    let server = Server::new();
    let mut c = server.connect();
    for q in [format!("l: {ITEMS}"), format!("held: ([] s: {ITEMS}; n: til 6)"), "p: held`s".into()] {
        c.eval(&q).unwrap();
    }
    assert!(is_packed(&c.eval("p").unwrap()), "a stored column of strings is packed");
    assert!(!is_packed(&c.eval("l").unwrap()), "a list of strings that is not a column is left as it is");
    server
}

/// Every template, with `X` standing for the strings, gives the same for the packed column and the list.
fn same(templates: &[&str]) {
    let server = setup();
    let mut c = server.connect();
    let differ: Vec<String> = templates
        .iter()
        .filter_map(|t| {
            let (packed, listed) = (shown(&mut c, &t.replace('X', "p")), shown(&mut c, &t.replace('X', "l")));
            (packed != listed).then(|| format!("{t}\n    packed: {packed}\n    listed: {listed}"))
        })
        .collect();
    assert!(differ.is_empty(), "{}", differ.join("\n"));
}

#[test]
fn verbs_of_one_argument() {
    same(&[
        "X", "count X", "first X", "last X", "reverse X", "distinct X", "group X", "iasc X", "idesc X", "asc X", "desc X", "type X", "enlist X", "upper X", "lower X",
        "trim X", "ltrim X", "rtrim X", "string X", "null X", "raze X", "value X", "key X", "-3!X", "-9!-8!X", "-8!X", "flip enlist X", "rank X", "sum X", "max X", "min X",
        "prev X", "next X", "differ X", "not X", "neg X", "til count X", "attr X", "where X", "avg X", "prd X", "deltas X", "md5 first X", ".Q.s1 X", ".j.j X",
        "show X", "meta X", "all X", "any X", "`$X", "`$ X 0", "\"J\"$X", "\"S\"$X", "\"*\"$X", "10$X", "-4$X", "`int$X", "`long$X", "(),X", "count each X",
        "first each X", "last each X", "reverse each X", "upper each X", "count X 0", "{x} each X", "type each X", "string count X",
    ]);
}

#[test]
fn verbs_of_two_arguments_adverbs_and_indexing() {
    same(&[
        "X like \"a*\"", "X like \"*an*\"", "X like \"\"", "X = X", "X <> X", "X ~ X", "X ~ l", "l ~ X", "X ~\\: \"apple\"", "\"apple\" ~/: X", "X ,\\: \"!\"",
        "\"!\" ,/: X", "X ,' X", "X in (\"apple\"; \"\")", "X in X", "X in l", "(\"apple\"; \"pear\") in X", "\"apple\" in X", "X in \"apple\"", "X ? \"cherry\"",
        "X ? \"pear\"", "X ? (\"cherry\"; \"pear\")", "l ? X", "X, X", "X, l", "l, X", "X, \"z\"", "X, enlist \"z\"", "(enlist \"z\"), X", "X, 1", "1, X", "X, `a", "X, ()",
        "(), X", "X, `a`b!1 2", "3 # X", "-2 # X", "10 # X", "0 # X", "2 _ X", "-2 _ X", "X _ 1", "1 3 sublist X", "2 sublist X", "X 2", "X 1 4", "X 10", "X[;0]", "X[0;1]",
        "X @ 3", "@[X; 1; :; \"zz\"]", "@[X; 0; upper]", ".[X; (0; 0); :; \"A\"]", "3 rotate X", "1 xprev X", "X except (\"apple\"; \"\")", "X inter l", "X union l",
        "X ? X", "2 cut X", "0 2 _ X", "\",\" sv X", "` sv X", "\" \" vs/: X", "ss[;\"a\"] each X", "ssr[;\"a\"; \"_\"] each X", "X where X like \"a*\"", "X cross 1 2",
        "(X; X)", "`a`b!(X; X)", "(`a`b!(X; X)) `a", "first (X; 1)", "max count each X", "`x`y`z`u`v`w ! X", "X ! til 6", "(X ! til 6) \"cherry\"", "10 20 30 ! 3 # X",
        "{x, y}/[X]", ",/[X]", "(,\\) X", "~': X", "X {x, y}' X", "X xbar 1", "X < X", "X + 1", "X & X", "X | X", "X ^ X", "X $ 1", "X @' 0", "X sublist 1",
    ]);
}

#[test]
fn tables_over_the_column() {
    // a table that is local to a function is not stored, so its columns stay lists
    same(&[
        "([] s: X)", "meta ([] s: X)", "asc ([] s: X)", "distinct ([] s: X)", "([k: X] v: til 6)", "([k: X] v: til 6) enlist \"cherry\"", "(([k: X] v: til 6) `cherry)",
        "select count i by s from ([] s: X)", "select s from ([] s: X) where s like \"a*\"", "select from ([] s: X; n: til 6) where s ~\\: \"apple\"",
        "select from ([] s: X; n: til 6) where s in (\"apple\"; \"cherry\")", "exec distinct s from ([] s: X)", "exec s from ([] s: X) where i > 2",
        "update s: upper s from ([] s: X)", "update s: enlist \"q\" from ([] s: X) where i = 1", "update s: 5 from ([] s: X) where i = 1", "update s: 5 from ([] s: X)",
        "update t: s from ([] s: X) where i > 3", "delete from ([] s: X) where s like \"a*\"", "`s xasc ([] s: X; n: til 6)", "`s`n xdesc ([] s: X; n: til 6)",
        "([] s: X; n: til 6) lj ([s: X] v: 10 * til 6)", "([] s: X) ij ([s: 2 # X] v: 1 2)", "([] s: X; n: til 6) uj ([] s: X; m: til 6)",
        "([s: X] n: til 6) uj ([s: reverse X] m: til 6)", "([] s: X) , ([] s: X)", "([] s: X; n: til 6) pj ([s: X] n: til 6)", "ej[`s; ([] s: X); ([] s: X; m: til 6)]",
        "select from ([] s: X; n: til 6) where n = (max; n) fby s", "select n by s from ([] s: X; n: til 6)", "ungroup select n by s from ([] s: X; n: til 6)",
        "select first s, last s, distinct s by n mod 2 from ([] s: X; n: til 6)", "count ([] s: X) except ([] s: 2 # X)", "([] s: X) ? `s ! enlist \"cherry\"",
        "([] s: X; n: til 6) 3", "value flip ([] s: X)", "flip (enlist `s) ! enlist X", "1 2 # ([] s: X)", "2 _ ([] s: X)", "(1 xkey ([] s: X; n: til 6)) \"banana\"",
        "?[([] s: X); enlist (like; `s; \"a*\"); 0b; ()]", "?[([] s: X); enlist (in; `s; enlist 2 # X); 0b; ()]", "\",\" 0: ([] s: X)", ".j.j ([] s: X)", "-9!-8!([] s: X)",
    ]);
}

/// The packed column of a stored table is changed as the list would be, and stays packed while it holds only strings.
#[test]
fn a_stored_column_takes_writes() {
    let server = setup();
    let mut c = server.connect();
    let packed = |c: &mut Connection| is_packed(&c.eval("held`s").unwrap());
    for (write, then) in [
        ("`held insert (\"date\"; 6)", "(\"date\"; 7)"),
        ("held,: ([] s: enlist \"elder\"; n: enlist 7)", "(\"elder\"; 8)"),
        ("`held insert ([] s: (\"fig\"; \"\"); n: 8 9)", "(\"\"; 10)"),
        ("update s: upper s from `held where n < 3", "(\"\"; 10)"),
        ("update s: enlist \"one\" from `held where n = 1", "(\"\"; 10)"),
        ("delete from `held where n = 2", "(\"\"; 9)"),
        ("update s: {x, \"!\"} each s from `held", "(enlist \"!\"; 9)"),
    ] {
        c.eval(write).unwrap_or_else(|e| panic!("{write}: {e}"));
        assert_eq!(shown(&mut c, "(last held`s; count held)"), shown(&mut c, then), "{write}");
        assert!(packed(&mut c), "{write}");
    }
    // a value that is not a string makes a list of the column; strings alone pack it again
    c.eval("update s: 5 from `held where n = 0").unwrap();
    assert!(!packed(&mut c));
    c.eval("update s: enlist \"five\" from `held where n = 0").unwrap();
    assert!(packed(&mut c));
    c.eval("k: ([id: 1 2] s: (enlist \"a\"; \"bb\")); `k upsert (3; \"ccc\"); `k upsert (1; \"zz\")").unwrap();
    assert_eq!(shown(&mut c, "exec s from k"), shown(&mut c, "(\"zz\"; \"bb\"; \"ccc\")"));
    assert!(is_packed(&c.eval("exec s from k").unwrap()));
    // a variable that holds the column is amended and appended to as a list is
    c.eval("p,: enlist \"new\"; p[0]: \"zero\"; l,: enlist \"new\"; l[0]: \"zero\"").unwrap();
    assert_eq!(shown(&mut c, "p"), shown(&mut c, "l"));
    c.eval("empty: ([] s: ()); `empty insert ([] s: enlist \"ab\"); empty,: ([] s: enlist \"cd\")").unwrap();
    assert!(is_packed(&c.eval("empty`s").unwrap()));
}

#[test]
fn files_logs_and_other_programs() {
    let server = setup();
    let packed = server.connect().eval("held").unwrap();
    let column = server.connect().eval("p").unwrap();
    let listed = server.connect().eval("l").unwrap();
    // other programs are sent exactly the bytes that the list would be
    assert_eq!(encode_k_payload(&column), encode_k_payload(&listed));
    // files and cluster nodes are given the packed form, which is smaller and reads back as it was
    let stored = encode_k_stored(&packed);
    assert!(stored.len() < encode_k_payload(&packed).len());
    assert_eq!(decode_k_payload(&stored, 0).unwrap().0, packed);
    assert!(decode_k_payload(&stored[..stored.len() - 1], 0).is_err());

    // a durable engine gives back what it held, from its log and from a snapshot
    let dir = tempfile::tempdir().unwrap();
    let options = StorageOptions { sync_wal: false, checkpoint_every: 0 };
    let held = {
        let durable = Server::open_with(dir.path(), options).unwrap();
        let mut c = durable.connect();
        c.eval(&format!("t: ([] s: {ITEMS}; n: til 6); `t insert (\"added\"; 6); update s: upper s from `t where n = 0")).unwrap();
        let copy = tempfile::tempdir().unwrap();
        std::fs::copy(dir.path().join("kix.wal"), copy.path().join("kix.wal")).unwrap();
        let replayed = Server::open(copy.path()).unwrap().connect().eval("t").unwrap();
        let held = c.eval("t").unwrap();
        assert_eq!(replayed, held);
        held
    };
    let reopened = Server::open(dir.path()).unwrap().connect().eval("t").unwrap();
    assert_eq!(reopened, held);
    assert!(matches!(&reopened, K::Table(t) if matches!(t.vals[0], ColVec::Strings(..))));

    // saved values and splayed tables come back packed
    let files = tempfile::tempdir().unwrap();
    let root = files.path().display().to_string().replace('\\', "/");
    let mut c = server.connect_internal();
    c.eval(&format!("`:{root}/flat set held; `:{root}/splayed/ set held")).unwrap();
    for name in ["flat", "splayed"] {
        let back = c.eval(&format!("get `:{root}/{name}")).unwrap();
        assert!(matches!(&back, K::Table(t) if matches!(t.vals[0], ColVec::Strings(..))), "{name}");
        assert_eq!(shown(&mut c, &format!("(get `:{root}/{name}) ~ held")), "-1 1b", "{name}");
    }
}

/// Queries of a stored table, whose column is packed, against those of the same table as an argument, whose is not.
#[test]
fn queries_of_a_stored_table() {
    let server = Server::new();
    let mut c = server.connect();
    for q in [format!("g: ([] s: {ITEMS}; n: til 6)"), "k: ([s: (\"apple\"; \"cherry\"; \"fig\")] v: 1 2 3)".into()] {
        c.eval(&q).unwrap();
    }
    assert!(matches!(c.eval("g`s").unwrap(), K::Vector(ColVec::Strings(..))));
    let listed = format!("([] s: {ITEMS}; n: til 6)");
    let templates = [
        "T", "meta T", "count T", "T 2", "T`s", "T `s`n", "2 # T", "-2 _ T", "T`s`n", "select from T where s like \"a*\"", "select from T where s ~\\: \"apple\"",
        "select from T where s in (\"apple\"; \"fig\")", "select from T where not s like \"a*\"", "select from T where 5 < count each s", "select count i by s from T",
        "select n by s from T", "select last s by n mod 2 from T", "exec distinct s from T", "exec s!n from T", "update u: upper s from T", "update s: reverse s from T",
        "select from T where s like \"a*\", n > 1", "`s xasc T", "`n`s xdesc T", "distinct T", "select distinct s from T", "T lj k", "T ij k", "T uj T", "T, T",
        "T except 2 # T", "T inter 2 # T", "1 xkey T", "`s xgroup T", "value flip T", "flip T", "T ? `s`n ! (\"cherry\"; 3)", "first T", "last T", "reverse T",
        "iasc T`s", "group T`s", "(T`s) ? \"cherry\"", "`$T`s", "(T`s) ,\\: \"?\"", "\" \" sv T`s", "count each T`s", "select syms: `$s from T", "csv 0: T",
        ".j.j T", "-9!-8!T", "-8!T", "select s from T where i in 1 3", "T where T[`s] like \"*an*\"", "update s: enlist \"z\" from T where n = 0", "delete s from T",
        "ungroup select n by s from T", "select from T where n = (max; n) fby s", "?[T; enlist (like; `s; \"a*\"); 0b; ()]", "T @ 1 3", "T[1; `s]", "T[`s; 1]",
        "1!T", "0!2!T", "-8!1!T", "-3!T", "(T`s) ~/: T`s", "\"fig\" ~/: T`s", "(T`s) ~\\: \"\"", "\"fig\" ~\\: T`s",
    ];
    let differ: Vec<String> = templates
        .iter()
        .filter_map(|t| {
            let run = |c: &mut Connection, table: &str| shown(c, &format!("{{[T] {t}}}[{table}]"));
            let (packed, plain) = (run(&mut c, "g"), run(&mut c, &listed));
            (packed != plain).then(|| format!("{t}\n    packed: {packed}\n    listed: {plain}"))
        })
        .collect();
    assert!(differ.is_empty(), "{}", differ.join("\n"));
}
