//! A process that runs what the servers it is connected to send, while it does nothing else (a subscriber's `upd`),
//! and its handles, which all its sessions share.
use kixdb::ipc::{self_signed_cert, tls_server_config, IpcConfig, Listener};
use kixdb::{Connection, Server, K};
use std::time::{Duration, Instant};

fn text(result: kixdb::KResult<K>) -> String {
    result.map(|k| k.to_string()).unwrap_or_else(|e| format!("'{}", e.signal))
}

fn run(c: &mut Connection, statements: &[&str]) {
    for statement in statements {
        c.eval(statement).unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
}

/// Wait for `code` to give `expected`: messages are run on the subscriber's own thread.
fn comes_to(c: &mut Connection, code: &str, expected: &str) {
    let started = Instant::now();
    while text(c.eval(code)) != expected {
        assert!(started.elapsed() < Duration::from_secs(10), "{code} is {}, not {expected}", text(c.eval(code)));
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// A publisher whose subscribers get `(`upd; table; rows)`, and its address.
fn publisher(config: IpcConfig) -> (Connection, Listener, String) {
    let server = Server::new();
    let mut local = server.connect();
    run(&mut local, &[
        "subs: `int$()",
        "acks: `long$()",
        "ack: {acks:: acks, x}",
        "sub: {subs:: distinct subs, .z.w; `ok}",
        ".z.pc: {subs:: subs except x}",
        "pub: {[t; rows] {[h; t; rows] neg[h] (`upd; t; rows)}[; t; rows] each subs;}",
    ]);
    let listener = Listener::bind(server, "127.0.0.1:0", config).unwrap();
    let address = listener.local_addr().to_string();
    (local, listener, address)
}

#[test]
fn a_subscriber_runs_what_is_published() {
    let (mut publisher, listener, address) = publisher(IpcConfig::default());
    let server = Server::new();
    let mut c = server.connect();
    let _background = server.start_background();
    run(&mut c, &[
        "trade: ([] sym:`symbol$(); price:`float$())",
        "lost: `int$()",
        ".z.pc: {lost:: lost, x}",
        // the handle on which a message came is `.z.w`
        "upd: {[t; rows] t insert rows; neg[.z.w] (`ack; count rows)}",
        &format!("h: hopen `:{address}"),
        "h \"sub[]\"",
    ]);
    // many messages, in order
    run(&mut publisher, &["do[200; pub[`trade; ([] sym:`a`b; price:1 2f)]]", "pub[`trade; ([] sym:enlist `z; price:enlist 9f)]"]);
    comes_to(&mut c, "(count trade; last trade`sym)", "(401;`z)");
    comes_to(&mut publisher, "(count acks; sum acks)", "201 401");
    // a synchronous call from within a message, and a message sent while an answer was awaited
    run(&mut c, &["upd: {[t; rows] seen:: h \"count subs\"}"]);
    publisher.eval("pub[`trade; ()]").unwrap();
    comes_to(&mut c, "@[value; `seen; 0]", "1");
    assert_eq!(text(c.eval("h \"neg[.z.w] \\\"later: 5\\\"; 2\"")), "2");
    comes_to(&mut c, "@[value; `later; 0]", "5");
    // `.z.ps` takes messages in place of evaluating them, and one that fails does not end the connection
    run(&mut c, &[".z.ps: {got:: x}"]);
    publisher.eval("{neg[x] \"1+`a\"} each subs").unwrap();
    comes_to(&mut c, "@[value; `got; 0]", "\"1+`a\"");
    run(&mut c, &["\\x .z.ps"]);
    publisher.eval("{neg[x] \"1+`a\"; neg[x] \"after: 1\"} each subs").unwrap();
    comes_to(&mut c, "@[value; `after; 0]", "1");
    // every session of the process may use the handle
    let mut other = server.connect();
    assert_eq!(text(other.eval("(.z.H; h \"6*7\")")), "(`s#,3i;42)");
    // the publisher closes: `.z.pc` hears, and the handle is no more
    publisher.eval("hclose first subs").unwrap();
    comes_to(&mut c, "lost", ",3i");
    assert_eq!(text(c.eval("(count .z.H; @[{h x}; \"1\"; {`closed}])")), "(0;`closed)");
    drop(listener);
}

#[test]
fn a_subscriber_closes_its_end() {
    let (mut publisher, _listener, address) = publisher(IpcConfig::default());
    let server = Server::new();
    let mut c = server.connect();
    let _background = server.start_background();
    run(&mut c, &["lost: 0", ".z.pc: {lost:: x}", &format!("h: hopen `:{address}"), "h \"sub[]\""]);
    comes_to(&mut publisher, "count subs", "1");
    run(&mut c, &["neg[h] \"told: 1\"; hclose h"]);
    // what was sent arrives, the publisher sees the end, and `.z.pc` is not for what we close ourselves
    comes_to(&mut publisher, "(@[value; `told; 0]; count subs)", "1 0");
    assert_eq!(text(c.eval("(lost; count .z.H)")), "0 0");
}

#[test]
fn two_connections_over_tls_take_turns() {
    let (cert, key) = self_signed_cert("localhost").unwrap();
    let ca = std::env::temp_dir().join(format!("kixdb-subscriber-{}.pem", std::process::id()));
    std::fs::write(&ca, &cert).unwrap();
    std::env::set_var("KIX_SSL_CA_CERT_FILE", &ca);
    let tls = tls_server_config(cert.as_bytes(), key.as_bytes()).unwrap();
    let config = IpcConfig { tls: Some(tls), force_tls: true, ..IpcConfig::default() };
    let (mut first, _a, one) = publisher(config.clone());
    let (mut second, _b, two) = publisher(config);
    let server = Server::new();
    let mut c = server.connect();
    let _background = server.start_background();
    run(&mut c, &["n: 3 4i!0 0", "upd: {[t; rows] n[.z.w]+: count rows}"]);
    for address in [one, two] {
        run(&mut c, &[&format!("(hopen `:tcps://{address}) \"sub[]\"")]);
    }
    // larger than a TLS record, from both at once, while the subscriber asks too
    for p in [&mut first, &mut second] {
        p.eval("do[100; pub[`t; 40000#\"p\"]]").unwrap();
    }
    for _ in 0..20 {
        assert_eq!(text(c.eval("(3i \"count subs\"; count 4i \"100000#\\\"r\\\"\")")), "1 100000");
    }
    comes_to(&mut c, "value n", "4000000 4000000");
    let _ = std::fs::remove_file(ca);
}
