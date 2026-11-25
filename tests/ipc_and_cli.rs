use kixdb::ipc::{IpcClient, IpcConfig, Listener};
use kixdb::Server;
use std::process::Command;

#[test]
fn ipc_wire_codec_and_live_tcp_listener() {
    let srv = Server::new();
    {
        let mut c = srv.connect();
        c.eval("quotes: ([sym: `AAPL`MSFT] bid: 180.1 410.2; ask: 180.3 410.5)").unwrap();
        // Verify `-8!` / `-9!` in-process binary IPC round-trip on a keyed table
        assert_eq!(c.eval("(-9! -8! quotes) ~ quotes").unwrap().to_string(), "1b");
    }

    let listener = Listener::bind(srv.clone(), ("127.0.0.1", 0), IpcConfig::with_single_user("alice", "s3cr3t")).unwrap();
    let port = listener.local_addr().port();
    let addr = format!("127.0.0.1:{port}");

    // Bad password must be rejected with `'access`
    assert!(IpcClient::connect_addr(&addr, "alice", "wrong").is_err());

    // Valid client query + functional insert + `hopen` from another local engine!
    let mut cli = IpcClient::connect_addr(&addr, "alice", "s3cr3t").unwrap();
    let res = cli.query_str("exec bid from quotes where sym = `MSFT").unwrap();
    assert_eq!(res.to_string(), ",410.2");

    // Functional IPC message: `("upsert"; `quotes; (`GOOG; 150.0; 150.2))`
    let msg = srv.connect().eval("(`upsert; `quotes; (`GOOG; 150.0; 150.2))").unwrap();
    cli.query_k(&msg).unwrap();
    assert_eq!(cli.query_str("count quotes").unwrap().to_string(), "3");

    // Unauthenticated listener tested via `hopen` inside `q`
    let open_listener = Listener::bind(srv, ("127.0.0.1", 0), IpcConfig::default()).unwrap();
    let open_port = open_listener.local_addr().port();
    let client_srv = Server::new();
    let mut cc = client_srv.connect();
    let remote_sum = cc
        .eval(&format!("h: hopen `:127.0.0.1:{open_port}; r: h \"exec sum bid from quotes\"; hclose h; r"))
        .unwrap();
    // 180.1 + 410.2 + 150.0 = 740.3
    assert_eq!(remote_sum.to_string(), "740.3");
}

#[test]
fn cli_binary_runs_queries_and_scripts() {
    let bin = env!("CARGO_BIN_EXE_kix");
    let out = Command::new(bin)
        .args(["-Q", "t: ([] a: 1 2 3; b: 10 20 30)", "-Q", "exec sum b from t where a >= 2"])
        .output()
        .expect("run kix CLI");
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(stdout.trim(), "50");
}

#[test]
fn tls_and_plain_clients_share_a_port() {
    let (cert, key) = kixdb::ipc::self_signed_cert("127.0.0.1").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let ca = dir.path().join("ca.pem");
    std::fs::write(&ca, &cert).unwrap();
    let mut cfg = IpcConfig::with_single_user("alice", "s3cr3t");
    cfg.tls = Some(kixdb::ipc::tls_server_config(cert.as_bytes(), key.as_bytes()).unwrap());
    let listener = Listener::bind(Server::new(), ("127.0.0.1", 0), cfg).unwrap();
    let addr = listener.local_addr().to_string();
    let ca = ca.display().to_string();

    let mut secure = IpcClient::connect_tls(&addr, "alice", "s3cr3t", &ca).unwrap();
    // a large message both ways
    assert_eq!(secure.query_str("x: til 100000; sum x").unwrap().to_string(), "4999950000");
    assert_eq!(secure.query_str("x").unwrap().count(), 100_000);
    assert!(IpcClient::connect_tls(&addr, "alice", "wrong", &ca).is_err());
    let mut plain = IpcClient::connect_addr(&addr, "alice", "s3cr3t").unwrap();
    assert_eq!(plain.query_str("count x").unwrap().to_string(), "100000");
    // a certificate the client does not trust is refused
    let (other, _) = kixdb::ipc::self_signed_cert("127.0.0.1").unwrap();
    std::fs::write(dir.path().join("other.pem"), other).unwrap();
    let other = dir.path().join("other.pem").display().to_string();
    assert!(IpcClient::connect_tls(&addr, "alice", "s3cr3t", &other).is_err());

    // `--force-tls` (`IpcConfig::force_tls = true`) rejects plaintext clients while allowing TLS clients
    let mut forced = IpcConfig::with_single_user("alice", "s3cr3t");
    forced.tls = Some(kixdb::ipc::tls_server_config(cert.as_bytes(), key.as_bytes()).unwrap());
    forced.force_tls = true;
    let forced_listener = Listener::bind(Server::new(), ("127.0.0.1", 0), forced).unwrap();
    let forced_addr = forced_listener.local_addr().to_string();
    assert!(IpcClient::connect_addr(&forced_addr, "alice", "s3cr3t").is_err());
    let mut forced_tls = IpcClient::connect_tls(&forced_addr, "alice", "s3cr3t", &ca).unwrap();
    assert_eq!(forced_tls.query_str("2 + 3").unwrap().to_string(), "5");
}

#[test]
fn remote_introspection_does_not_eval_views_and_cli_connect_supports_dot_commands() {
    let srv = Server::new();
    {
        let mut c = srv.connect();
        c.eval("counter: 0; boom:: (counter:: counter + 1; 42); .analytics.factor: 7; f: {[x] x + 1}").unwrap();
    }
    let listener = Listener::bind(srv.clone(), ("127.0.0.1", 0), IpcConfig::default()).unwrap();
    let addr = listener.local_addr().to_string();

    let mut cli = IpcClient::connect_addr(&addr, "admin", "").unwrap();
    let ns = kixdb::inspect::remote_namespaces(&mut cli);
    assert!(ns.contains(&".".to_string()) && ns.contains(&".analytics".to_string()), "{ns:?}");

    // `remote_definition` must inspect `boom` WITHOUT evaluating it (`counter` stays 0!)
    let def = kixdb::inspect::remote_definition(&mut cli, "boom").unwrap();
    assert!(def.starts_with("boom::"), "got: {def}");
    assert_eq!(cli.query_str("counter").unwrap().to_string(), "0");

    let plan = kixdb::inspect::remote_explain(&mut cli, "select from ([] a: 1 2) where a = 1").unwrap();
    assert!(plan[0].contains("qSQL SELECT"), "{plan:?}");

    // Test `kix --connect` `.import`, `.schema`, `.plan`
    let tmp = tempfile::tempdir().unwrap();
    let csv = tmp.path().join("items.csv");
    std::fs::write(&csv, "sym,qty\nAAPL,10\nMSFT,25\n").unwrap();
    let bin = env!("CARGO_BIN_EXE_kix");
    let script = format!(
        ".import --new {} items\n.schema items\n.plan select from items where sym = `AAPL\nexec sum qty from items\n\\\\\n",
        csv.display()
    );
    let mut child = Command::new(bin)
        .args(["--connect", &addr])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    use std::io::Write;
    child.stdin.as_mut().unwrap().write_all(script.as_bytes()).unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("imported 2 row(s) into items"), "stdout: {stdout}");
    assert!(stdout.contains("items:"), "stdout: {stdout}");
    assert!(stdout.contains("qSQL SELECT"), "stdout: {stdout}");
    assert!(stdout.contains("35"), "stdout: {stdout}");
}

#[test]
fn listening_beyond_this_machine_needs_a_password_over_tls() {
    let (cert, key) = kixdb::ipc::self_signed_cert("db").unwrap();
    let mut safe = IpcConfig::with_single_user("alice", "s3cr3t");
    safe.tls = Some(kixdb::ipc::tls_server_config(cert.as_bytes(), key.as_bytes()).unwrap());
    safe.force_tls = true;
    // on this machine anything goes
    for local in ["127.0.0.1", "localhost", "::1"] {
        assert!(IpcConfig::default().check_exposure(local).is_ok(), "{local}");
    }
    for reachable in ["0.0.0.0", "::", "203.0.113.1", "no.such.host.invalid"] {
        assert!(safe.check_exposure(reachable).is_ok(), "{reachable}");
        assert!(IpcConfig::default().check_exposure(reachable).is_err(), "{reachable}");
    }
    // each of the three is needed
    let without = |change: &dyn Fn(&mut IpcConfig)| {
        let mut config = safe.clone();
        change(&mut config);
        config.check_exposure("0.0.0.0").is_err()
    };
    assert!(without(&|c| c.users = None));
    assert!(without(&|c| c.users = IpcConfig::with_single_user("alice", "").users));
    assert!(without(&|c| c.force_tls = false));
    assert!(without(&|c| c.tls = None));
}

#[test]
fn kix_serve_refuses_to_listen_unprotected_beyond_this_machine() {
    // (an address no machine here has, so that nothing is ever reachable)
    let run = |more: &[&str]| {
        let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_kix-serve"));
        let out = cmd.args(["--bind", "203.0.113.1", "-p", "0"]).args(more).env_remove("KIX_AUTH").output().unwrap();
        assert!(!out.status.success());
        String::from_utf8_lossy(&out.stderr).into_owned()
    };
    let refused = run(&[]);
    assert!(refused.contains("--force-tls") && !refused.contains("cannot listen"), "{refused}");
    assert!(run(&["-u", "alice:s3cr3t"]).contains("--insecure-clients"));
    // allowed to go on, it gets as far as the address
    let allowed = run(&["--insecure-clients"]);
    assert!(allowed.contains("WARNING") && allowed.contains("cannot listen"), "{allowed}");
}
