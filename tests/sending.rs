//! What a server sends to its clients (`neg[.z.w] msg`, `-25!`, `.z.W`, `hclose`) and how a client receives it.
use kixdb::ipc::{self_signed_cert, tls_server_config, IpcClient, IpcConfig, Listener};
use kixdb::{Connection, Server, K};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn serve(config: IpcConfig, setup: &[&str]) -> (Server, Connection, Listener, String) {
    let server = Server::new();
    let mut local = server.connect();
    for statement in setup {
        local.eval(statement).unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    let listener = Listener::bind(server.clone(), "127.0.0.1:0", config).unwrap();
    let address = listener.local_addr().to_string();
    (server, local, listener, address)
}

fn text(result: kixdb::KResult<K>) -> String {
    result.map(|k| k.to_string()).unwrap_or_else(|e| format!("'{}", e.signal))
}

/// Wait for `code` to give `expected`: connections are served on the server's threads.
fn comes_to(c: &mut Connection, code: &str, expected: &str) {
    let started = Instant::now();
    while text(c.eval(code)) != expected {
        assert!(started.elapsed() < Duration::from_secs(10), "{code} is {}, not {expected}", text(c.eval(code)));
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn what_a_request_sends_arrives_before_its_answer() {
    let (_, mut local, _listener, address) = serve(IpcConfig::default(), &[".z.po: {neg[x] \"welcome\"}"]);
    let mut client = IpcClient::connect_addr(&address, "ann", "").unwrap();
    assert_eq!(text(client.query_str("(neg .z.w) `one; neg[.z.w] 2 3; neg[.z.w][]; `done")), "`done");
    for expected in ["\"welcome\"", "`one", "2 3"] {
        assert_eq!(text(client.receive()), expected);
    }
    // a handle taken from `.z.H`, from this session
    comes_to(&mut local, "count .z.H", "1");
    local.eval("{neg[x] (`upd; x)} each .z.H").unwrap();
    assert_eq!(text(client.receive()), "(`upd;1000i)");
    // what cannot be sent
    for (code, error) in [(".z.w \"1\"", "'nyi"), ("first[.z.H] \"1\"", "'nyi")] {
        assert_eq!(text(client.query_str(code)), error, "{code}");
    }
    assert_eq!(text(local.eval("(neg 1000) `x; (neg 4000) `x")), "'type");
    // nothing waits once it is written
    comes_to(&mut local, ".z.W", "(,1000i)!,0i");
    assert_eq!(text(local.eval("-25!(.z.H; `all)")), "::");
}

#[test]
fn publishing_to_subscribers() {
    let (server, mut local, _listener, address) = serve(
        IpcConfig::default(),
        &["subs: `int$()", "sub: {subs:: distinct subs, .z.w; `ok}", ".z.pc: {subs:: subs except x}", "n: 0"],
    );
    let mut clients: Vec<IpcClient> = (0..3).map(|_| IpcClient::connect_addr(&address, "u", "").unwrap()).collect();
    for client in &mut clients {
        assert_eq!(text(client.query_str("sub[]")), "`ok");
    }
    assert_eq!(text(local.eval("-25!(subs; (`upd; 1 2 3))")), "::");
    for client in &mut clients {
        assert_eq!(text(client.receive()), "(`upd;1 2 3)");
    }
    // nothing is sent if any handle is not open
    assert_eq!(text(local.eval("-25!(subs, 4000i; `no)")), "'ipc");
    // from the timer
    let timer = server.start_background();
    local.eval(".z.ts: {n:: n + 1; {neg[x] (`tick; n)} each subs; if[n = 3; system \"t 0\"]}").unwrap();
    local.eval("\\t 5").unwrap();
    for client in &mut clients {
        for tick in 1..=3 {
            assert_eq!(text(client.receive()), format!("(`tick;{tick})"));
        }
    }
    drop(timer);
    // a client that leaves is forgotten, and the others go on
    clients.remove(0);
    comes_to(&mut local, "count subs", "2");
    local.eval("{neg[x] `still} each subs").unwrap();
    for client in &mut clients {
        assert_eq!(text(client.receive()), "`still");
    }
}

#[test]
fn the_server_closes_a_client() {
    let (_, mut local, _listener, address) = serve(IpcConfig::default(), &["gone: `int$()", ".z.pc: {gone:: gone, x}"]);
    let mut client = IpcClient::connect_addr(&address, "u", "").unwrap();
    client.query_str("1").unwrap();
    local.eval("neg[1000i] `bye; hclose 1000i").unwrap();
    // what was queued arrives, then the connection ends
    assert_eq!(text(client.receive()), "`bye");
    assert!(client.receive().is_err());
    assert!(client.query_str("1").is_err());
    // the handle is free at once, and `.z.pc` hears only of those that leave of themselves
    assert_eq!(text(local.eval("(count .z.H; count gone)")), "0 0");
    let mut next = IpcClient::connect_addr(&address, "u", "").unwrap();
    assert_eq!(text(next.query_str(".z.w")), "1000i");
    // a client may close its own handle: it gets no answer
    assert!(next.query_str("hclose .z.w; 1").is_err());
    comes_to(&mut local, "(count .z.H; count gone)", "0 0");
    drop(IpcClient::connect_addr(&address, "u", "").unwrap());
    comes_to(&mut local, "gone", ",1000i");
}

#[test]
fn a_client_that_falls_behind_is_disconnected() {
    let limit = IpcConfig { queue_limit: Some(1 << 20), ..IpcConfig::default() };
    let (_, mut local, _listener, address) = serve(limit, &["gone: `int$()", ".z.pc: {gone:: gone, x}"]);
    let mut stalled = IpcClient::connect_addr(&address, "u", "").unwrap();
    stalled.query_str("1").unwrap();
    let mut reading = IpcClient::connect_addr(&address, "u", "").unwrap();
    // an answer larger than the limit is no reason to disconnect
    assert_eq!(text(reading.query_str("count 3000000#\"x\"")), "3000000");
    assert_eq!(reading.query_str("3000000#\"x\"").unwrap().count(), 3_000_000);
    // publishing does not wait for the client that does not read, which is dropped once too much waits
    let started = Instant::now();
    local.eval("m: 100000#\"x\"; do[500; if[1000i in .z.H; neg[1000i] m]]").unwrap();
    assert!(started.elapsed() < Duration::from_secs(5), "publishing took {:?}", started.elapsed());
    comes_to(&mut local, "gone", ",1000i");
    assert_eq!(text(reading.query_str("2")), "2");
}

#[test]
fn tls_sends_and_answers_at_once() {
    let (cert, key) = self_signed_cert("localhost").unwrap();
    let ca = std::env::temp_dir().join(format!("kixdb-sending-{}.pem", std::process::id()));
    std::fs::write(&ca, &cert).unwrap();
    let tls = tls_server_config(cert.as_bytes(), key.as_bytes()).unwrap();
    let config = IpcConfig { tls: Some(tls), force_tls: true, ..IpcConfig::default() };
    let (server, _, _listener, address) = serve(config, &[]);
    let mut client = IpcClient::connect_tls(&address, "u", "", ca.to_str().unwrap()).unwrap();
    client.query_str("1").unwrap();
    // another thread publishes while the client asks, in both directions larger than a TLS record
    let publisher = std::thread::spawn(move || {
        let mut c = server.connect();
        for i in 0..200 {
            c.eval(&format!("neg[1000i] ({i}; 40000#\"p\")")).unwrap();
        }
    });
    let big = K::string(&"abcdefg".repeat(300_000));
    for _ in 0..20 {
        let count = K::Vector(kixdb::ColVec::General(kixdb::Attr::None, Arc::new(vec![K::sym("count"), big.clone()])));
        assert_eq!(text(client.query_k(&count)), "2100000");
        assert_eq!(client.query_str("200000#\"r\"").unwrap().count(), 200_000);
    }
    publisher.join().unwrap();
    for i in 0..200 {
        let message = client.receive().unwrap();
        assert_eq!(message.count(), 2, "message {i}");
        assert!(message.to_string().starts_with(&format!("({i};\"ppp")), "message {i}");
    }
    let _ = std::fs::remove_file(ca);
}

#[test]
fn a_q_client_receives() {
    let (_, _, _listener, address) = serve(IpcConfig::default(), &["later: {neg[.z.w] (`result; x * x)}"]);
    let mut c = Server::new().connect();
    c.eval(&format!("h: hopen `:{address}")).unwrap();
    // deferred synchronous: ask asynchronously, then wait for the message sent back
    assert_eq!(text(c.eval("neg[h] (`later; 7); neg[h][]; h[]")), "(`result;49)");
    // (one sent while an answer was awaited waits its turn)
    assert_eq!(text(c.eval("h \"neg[.z.w] `first; 2\"")), "2");
    assert_eq!(text(c.eval("h[]")), "`first");
    c.eval("hclose h").unwrap();
}
