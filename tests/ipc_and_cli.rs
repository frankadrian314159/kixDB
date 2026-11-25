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
