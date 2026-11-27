//! What a tickerplant system needs: logs and their replay (`-11!`), answers that are put off (`-30!`), ports and the
//! directory from q, and the functions of the end of a day.
use kixdb::ipc::{IpcClient, IpcConfig, Listener};
use kixdb::{Connection, Server, K};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

/// A connection to a durable workspace in a new directory, so that relative paths stay inside it.
fn workspace() -> (tempfile::TempDir, Connection) {
    let dir = tempfile::tempdir().unwrap();
    let connection = Server::open(dir.path()).unwrap().connect();
    (dir, connection)
}

fn text(result: kixdb::KResult<K>) -> String {
    result.map(|k| k.to_string()).unwrap_or_else(|e| format!("'{}", e.signal))
}

fn check(c: &mut Connection, cases: &[(&str, &str)]) {
    for (code, expected) in cases {
        assert_eq!(&text(c.eval(code)), expected, "{code}");
    }
}

#[test]
fn a_log_is_appended_to_and_replayed() {
    let (dir, mut c) = workspace();
    check(&mut c, &[
        ("n: 0; upd: {[t; x] n+: x; t}; n", "0"),
        (".[`:log; (); :; ()]; -11!(-2; `:log)", "0"),
        ("h: hopen `:log; h enlist (`upd; `a; 1); h ((`upd; `b; 2); (`upd; `c; 3)); hclose h; count get `:log", "3"),
        ("r: -11!`:log; (r; n)", "3 6"),
        ("n: 0; r: -11!(2; `:log); (r; n)", "2 3"),
        ("n: 0; r: -11!(-1; `:log); (r; n; -11!(-2; `:log))", "3 6 3"),
        // from a function of a namespace, the messages see the globals
        ("n: 0; .ns.run: {[upd] -11!`:log}; .ns.run 0; n", "6"),
        // `.z.ps` takes the messages, as it takes those of clients
        ("seen: (); .z.ps: {seen,: enlist x 1}; -11!`:log; seen", "`a`b`c"),
        ("system \"x .z.ps\"; @[{-11!x}; (-3; `:log); {x}]", "\"domain\""),
        // a list that `set` wrote is replayed too, and becomes a log when it is added to
        ("`:set set enlist (`upd; `a; 5); n: 0; r: -11!`:set; (r; n)", "1 5"),
        ("h: hopen `:set; h enlist (`upd; `b; 6); hclose h; n: 0; r: -11!`:set; (r; n)", "2 11"),
        // atoms that are added are items, and what is read back is the list that joining them makes
        (".[`:nums; (); :; ()]; h: hopen `:nums; h 1 2; h 3; hclose h; get `:nums", "1 2 3"),
    ]);
    // a log cut short in the middle of a message: the good ones are counted and replayed
    let path = dir.path().join("log");
    let whole = std::fs::read(&path).unwrap();
    std::fs::write(&path, &whole[..whole.len() - 3]).unwrap();
    let good = c.eval("-11!(-2; `:log)").unwrap().to_string();
    assert!(good.starts_with("2 "), "{good}");
    check(&mut c, &[("n: 0; @[{-11!x}; `:log; {x}]", "\"badtail\""), ("n", "3"), ("n: 0; r: -11!(2; `:log); (r; n)", "2 3")]);
    assert!(c.eval("get `:log").is_err());
    // and one whose bytes were changed
    let mut damaged = whole.clone();
    *damaged.last_mut().unwrap() ^= 1;
    std::fs::write(&path, damaged).unwrap();
    assert!(c.eval("-11!(-2; `:log)").unwrap().to_string().starts_with("2 "));
}

#[test]
fn what_a_replay_makes_is_kept() {
    let (dir, mut c) = workspace();
    check(&mut c, &[
        ("t: ([] s: `g#`symbol$(); v: `long$()); upd: insert", "insert"),
        (".[`:log; (); :; ()]; h: hopen `:log; h enlist (`upd; `t; (`a`b; 1 2)); hclose h; -11!`:log", "1"),
    ]);
    drop(c);
    let mut again = Server::open(dir.path()).unwrap().connect();
    check(&mut again, &[("(t`v; attr t`s)", "(1 2;`g)")]);
}

#[test]
fn an_answer_is_put_off() {
    let server = Server::new();
    let mut local = server.connect();
    for code in [
        "hold: {waiting:: .z.w; -30!(::)}",
        "fail: {-30!(::); 'oops}",
        ".z.pg: {$[x ~ \"wait\"; hold[]; x ~ \"fail\"; fail[]; value x]}",
    ] {
        local.eval(code).unwrap();
    }
    let listener = Listener::bind(server, "127.0.0.1:0", IpcConfig::default()).unwrap();
    let address = listener.local_addr().to_string();
    let connect = || IpcClient::connect_addr(&address, "u", "").unwrap();
    let (mut asking, mut answering) = (connect(), connect());
    for (given, expected) in [("0b; 42", "42"), ("1b; \"later\"", "'later")] {
        local.eval("waiting: 0Ni").unwrap();
        let mut client = asking;
        let waiting = std::thread::spawn(move || (text(client.query_str("wait")), client));
        while text(local.eval("null waiting")) == "1b" {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        answering.query_str(&format!("-30!(waiting; {given})")).unwrap();
        let (answer, client) = waiting.join().unwrap();
        assert_eq!(answer, expected);
        asking = client;
    }
    // one answer for each request, and an error is not put off
    assert_eq!(text(answering.query_str("-30!(waiting; 0b; 1)")), "'domain");
    assert_eq!(text(asking.query_str("fail")), "'oops");
    assert_eq!(text(asking.query_str("1+1")), "2");
    assert_eq!(local.eval("-30!(::)").unwrap_err().signal, "domain");
}

#[test]
fn ports_and_directories_are_set_from_q() {
    let (dir, mut c) = workspace();
    assert_eq!(c.eval("system \"p 5000\"").unwrap_err().signal, "nyi", "a process that cannot open one says so");
    let asked = Arc::new(Mutex::new(vec![]));
    let server = Server::new();
    let seen = asked.clone();
    server.when_asked_to_listen(move |_, port| {
        seen.lock().unwrap().push(port);
        Ok(())
    });
    let mut q = server.connect();
    check(&mut q, &[("system \"p\"", "0i"), ("system \"p 5000\"", "::"), ("system \"p 0\"", "::")]);
    assert_eq!(*asked.lock().unwrap(), [5000, 0]);
    let listener = Listener::bind(server.clone(), "127.0.0.1:0", IpcConfig::default()).unwrap();
    assert_eq!(text(q.eval("system \"p\"")), format!("{}i", listener.local_addr().port()));
    drop(listener);
    check(&mut q, &[("system \"p\"", "0i")]);
    // relative paths are taken from the directory of `\cd`
    check(&mut c, &[
        ("`:sub/a set 1; system \"cd sub\"; get `:a", "1"),
        ("`:b set 2; system \"cd ..\"; get `:sub/b", "2"),
        ("@[system; \"cd nowhere\"; {x}]", "\"os\""),
    ]);
    assert!(dir.path().join("sub/b").exists());
}

#[test]
fn a_days_end_saves_the_tables() {
    let (_dir, mut c) = workspace();
    check(&mut c, &[
        ("t: ([] sym: `g#`b`a`b; v: 1 2 3); q: ([] sym: enlist `a; b: enlist 9.5); n: 5", "5"),
        ("\\d .u\nend: {.Q.hdpf[`; `:db; x; `sym]}\n\\d .\n.u.end 2026.01.01", "::"),
        // they are empty, and nothing is mounted in their place
        ("(count t; count q; @[value; \".Q.pv\"; {x}])", "(0;0;\".Q.pv\")"),
        ("`t insert (`c; 4); .Q.dpt[`:db; 2026.01.02; `t]; .Q.dpts[`:db; 2026.01.02; `q; `other]", "`q"),
        ("system \"x t\"; system \"x q\"; system \"l db\"; .Q.pv", "2026.01.01 2026.01.02"),
        ("exec v from t where date=2026.01.01", "2 1 3"),
        (".Q.pn", "`q`t!(1 0;3 1)"),
        (".Q.cn select from t", "3 1"),
        (".Q.par[`:db; 2026.01.03; `t]", "`:db/2026.01.03/t"),
        ("(count .Q.pd; count .Q.P; .Q.MAP[])", "(2;0;::)"),
        (".Q.view 2026.01.02; (count .Q.pd; count .Q.PD)", "1 2"),
    ]);
}

#[test]
fn segments_take_partitions_in_turn() {
    let (dir, mut c) = workspace();
    for sub in ["db", "one", "two"] {
        std::fs::create_dir(dir.path().join(sub)).unwrap();
    }
    std::fs::write(dir.path().join("db/par.txt"), "one\ntwo\n").unwrap();
    check(&mut c, &[
        (".Q.par[`:db; 2000.01.01; `t]", "`:one/2000.01.01/t"),
        (".Q.par[`:db; 2000.01.02; `t]", "`:two/2000.01.02/t"),
        ("t: ([] sym: `a`b; v: 1 2); .Q.dpft[`:db; 2000.01.02; `sym; `t]", "`t"),
        (".Q.P", "`:one`:two"),
    ]);
    assert!(dir.path().join("two/2000.01.02/t").is_dir());
}

/// A tickerplant and a subscriber to it, written for this test in the way that q's are.
#[test]
fn a_tickerplant_publishes_logs_and_is_replayed() {
    let dir = tempfile::tempdir().unwrap();
    let plant = Server::open(dir.path()).unwrap();
    let mut local = plant.connect();
    let script = "trade: ([] sym: `g#`symbol$(); price: `float$())
\\d .u
t: tables `.
w: t!(count t)#()
sub: {w[x],: enlist .z.w; (x; value x)}
L: `:day
.[L; (); :; ()]
l: hopen L
i: 0
upd: {[t; x] l enlist (`upd; t; x); i+: 1; (neg union/[w]) @\\: (`upd; t; x);}
\\d .
.z.pc: {.u.w: .u.w except\\: x}";
    for line in script.split('\n') {
        let line = if let Some(ns) = line.strip_prefix("\\d ") { format!("system \"d {ns}\"") } else { line.to_string() };
        local.eval(&line).unwrap_or_else(|e| panic!("{line}: {e}"));
    }
    let listener = Listener::bind(plant, "127.0.0.1:0", IpcConfig::default()).unwrap();
    let address = listener.local_addr().to_string();
    let subscriber = Server::new();
    let mut c = subscriber.connect();
    let _background = subscriber.start_background();
    for code in ["upd: insert", &format!("h: hopen `:{address}"), "(.[; (); :; ] .) h \".u.sub `trade\""] {
        c.eval(code).unwrap_or_else(|e| panic!("{code}: {e}"));
    }
    c.eval("neg[h] (`.u.upd; `trade; (`a`b; 1 2f)); h \"\"").unwrap();
    let started = std::time::Instant::now();
    while text(c.eval("count trade")) != "2" {
        assert!(started.elapsed().as_secs() < 10, "nothing was published");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    check(&mut c, &[("attr trade`sym", "`g")]);
    // a second one starts from the log
    let mut late = Server::open(dir.path().join("late")).unwrap().connect();
    let log = dir.path().join("day").display().to_string().replace('\\', "/");
    check(&mut late, &[("trade: ([] sym: `symbol$(); price: `float$()); upd: insert", "insert")]);
    check(&mut late, &[(&format!("-11!`$\":{log}\""), "1"), ("trade`price", "1 2f")]);
}

#[test]
fn the_shell_serves_on_when_its_input_ends() {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kix"));
    command.args(["-p", "0", "-t", "20"]).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut shell = command.spawn().unwrap();
    let setup = b"ticks: 0\n.z.ts: {ticks+: 1}\n-1 string system \"p\";\n";
    shell.stdin.take().unwrap().write_all(setup).unwrap();
    let mut lines = BufReader::new(shell.stdout.take().unwrap()).lines();
    let port = lines.find_map(|line| line.unwrap().parse::<u16>().ok()).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(300));
    let mut client = IpcClient::connect_addr(&format!("127.0.0.1:{port}"), "u", "").unwrap();
    assert_eq!(text(client.query_str("ticks > 0")), "1b");
    shell.kill().unwrap();
    shell.wait().unwrap();
}
