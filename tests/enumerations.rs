//! Enumerations, foreign keys and linked columns; and longs in int columns.
use kixdb::ipc::{IpcClient, IpcConfig, Listener};
use kixdb::{Connection, Server, K};

fn check(c: &mut Connection, cases: &[(&str, &str)]) {
    for (code, expected) in cases {
        assert_eq!(c.eval(code).map(|k| k.to_string()).unwrap_or_else(|e| format!("'{}", e.signal)), *expected, "{code}");
    }
}

fn session(setup: &[&str]) -> Connection {
    let mut c = Server::new().connect();
    for statement in setup {
        c.eval(statement).unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    c
}

const KEYED: &str = "kt: ([eid: 1001 1002 1003] name: `Dent`Beeblebrox`Prefect; iq: 98 42 126)";

#[test]
fn enumerations_take_only_values_of_their_list() {
    let mut c = session(&["sym: `g`aapl`msft`ibm", "ev: `sym$`aapl`ibm`g`aapl"]);
    check(&mut c, &[
        ("ev", "`sym$`aapl`ibm`g`aapl"),
        ("ev 3", "`sym$`aapl"),
        ("(type ev; type first ev)", "20 -20h"),
        ("value ev", "`aapl`ibm`g`aapl"),
        ("`long$ev", "1 3 0 1"),
        ("key ev", "`sym"),
        ("ev = `aapl", "1001b"),
        ("ev ? `ibm", "1"),
        ("ev in `ibm`aapl", "1101b"),
        ("ev ~ `aapl`ibm`g`aapl", "0b"),
        ("distinct ev", "`sym$`aapl`ibm`g"),
        ("2 # reverse ev", "`sym$`aapl`g"),
        ("ev , ev 1", "`sym$`aapl`ibm`g`aapl`ibm"),
        ("ev , `x", "`aapl`ibm`g`aapl`x"),
        ("`sym$`zz", "'cast"),
        ("ev ,: `twtr", "'cast"),
        ("ev[0]: `nope", "'cast"),
        ("ev ,: `sym?`twtr; ev", "`sym$`aapl`ibm`g`aapl`twtr"),
        ("sym", "`g`aapl`msft`ibm`twtr"),
        ("ev[0]: `msft; ev", "`sym$`msft`ibm`g`aapl`twtr"),
        // `?` makes the list if there is none
        ("`other?`b`a`b", "`other$`b`a`b"),
        ("other", "`b`a"),
        // a type's name casts to it, whatever variables there are
        ("long: 1 2; `long$2.5", "3"),
    ]);
}

#[test]
fn foreign_keys_are_checked_and_followed() {
    let mut c = session(&[KEYED, "d: ([] eid: `kt$1003 1001 1002 1001; sc: 126 36 92 39)"]);
    check(&mut c, &[
        ("d `eid", "`kt$1003 1001 1002 1001"),
        ("`long$d `eid", "2 0 1 0"),
        ("`kt$1004", "'cast"),
        ("exec t from meta d", "\"jj\""),
        ("exec f from meta d", "`kt`"),
        ("fkeys d", "(,`eid)!,`kt"),
        ("fkeys `d", "(,`eid)!,`kt"),
        ("exec eid.name from d", "`Prefect`Dent`Beeblebrox`Dent"),
        ("cols select eid.name, sc from d", "`name`sc"),
        ("exec sum sc by eid.name from d", "`s#`Beeblebrox`Dent`Prefect!92 75 126"),
        ("exec sc from d where eid = 1001", "36 39"),
        ("exec sc from d where eid.iq > 90", "126 36 39"),
        // a change to the table referred to shows through the key
        ("update iq: 100 from `kt where eid = 1002; exec eid.iq from d", "126 98 100 98"),
        // new rows and new values are checked
        ("`d insert (1002; 7)", ",4"),
        ("`d insert (1009; 7)", "'cast"),
        ("d ,: `eid`sc!(1009; 8)", "'cast"),
        ("update eid: 1009 from `d where sc = 7", "'cast"),
        ("update eid: 1003 from `d where sc = 7; exec eid.name from d where sc = 7", ",`Prefect"),
        ("count d , `eid`sc!(1001; 1)", "6"),
        ("e: ([] eid: `kt$(); sc: `long$()); `e insert (1001; 1); exec eid.name from e", ",`Dent"),
    ]);
}

#[test]
fn compound_keys_and_linked_columns() {
    let mut c = session(&[
        "ktc: ([lname: `Dent`Beeblebrox; fname: `Arthur`Zaphod] iq: 98 42)",
        "tf: ([] name: `ktc$(`Beeblebrox`Zaphod; `Dent`Arthur); sc: 36 126)",
        "tree: ([] id: 0 1 2; pid: `tree!0N 0 1; v: 100 200 300)",
    ]);
    check(&mut c, &[
        ("exec name.iq from tf", "42 98"),
        ("tf ,: (`ktc$`Dent`Arthur; 5); exec name.lname from tf", "`Beeblebrox`Dent`Dent"),
        ("`ktc$`Dent`Zaphod", "'cast"),
        ("exec f from meta tree", "``tree`"),
        ("tree `pid", "`tree!0N 0 1"),
        ("exec pid.v from tree", "0N 100 200"),
        ("exec pid.pid.v from tree", "0N 0N 100"),
        ("`long$tree `pid", "0N 0 1"),
        ("value tree `pid", "0N 0 1"),
    ]);
}

#[test]
fn enumerations_are_saved_but_not_sent_to_other_programs() {
    let dir = tempfile::tempdir().unwrap();
    {
        let server = Server::open(dir.path()).unwrap();
        let mut c = server.connect();
        for statement in [KEYED, "d: ([] eid: `kt$1001 1002; sc: 1 2)", "`d insert (1003; 3)", "update eid: 1003 from `d where sc = 1"] {
            c.eval(statement).unwrap();
        }
        c.eval("`:flat set d; `:splay/ set d").unwrap();
        server.simulate_crash();
    }
    let server = Server::open(dir.path()).unwrap();
    let mut c = server.connect();
    check(&mut c, &[
        ("d `eid", "`kt$1003 1002 1003"),
        ("exec eid.name from d", "`Prefect`Beeblebrox`Prefect"),
        ("(get `:flat) ~ d", "1b"),
        ("(get `:splay/) ~ d", "1b"),
        ("type (-9!-8!d) `eid", "7h"),
    ]);
    let listener = Listener::bind(server.clone(), "127.0.0.1:0", IpcConfig::default()).unwrap();
    let mut client = IpcClient::connect_addr(&listener.local_addr().to_string(), "u", "").unwrap();
    assert_eq!(client.query_str("d `eid").unwrap(), c.eval("1003 1002 1003").unwrap());
    let K::Table(sent) = client.query_str("d").unwrap() else { panic!("not a table") };
    assert_eq!(sent.vals[0].type_num(), 7);
}

#[test]
fn longs_that_fit_go_into_int_columns() {
    let mut c = session(&["t: ([] name: `symbol$(); iq: `int$())"]);
    check(&mut c, &[
        ("t ,: `name`iq!(`Beeblebrox; 42); t `iq", ",42i"),
        ("`t insert (`Dent; 0N); t `iq", "42 0Ni"),
        ("`t upsert ([] name: `a`b; iq: 0W -0W); t `iq", "42 0N 0W -0Wi"),
        ("`t insert (`c; 3000000000)", "'type"),
        ("`t insert (`c; 98.0)", "'type"),
        ("count t", "4"),
    ]);
}
