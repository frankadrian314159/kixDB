//! The `.z` callbacks of servers and the shell, and the values of `.z`.
use kixdb::ipc::{IpcClient, IpcConfig, Listener};
use kixdb::{Connection, Server, K};
use std::time::{Duration, Instant};

fn serve(setup: &[&str]) -> (Connection, Listener, String) {
    let server = Server::new();
    let mut local = server.connect();
    for statement in setup {
        local.eval(statement).unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    let listener = Listener::bind(server, "127.0.0.1:0", IpcConfig::default()).unwrap();
    let address = listener.local_addr().to_string();
    (local, listener, address)
}

fn text(result: kixdb::KResult<K>) -> String {
    result.map(|k| k.to_string()).unwrap_or_else(|e| format!("'{}", e.signal))
}

/// Wait for `code` to give `expected`: callbacks run on the server's threads.
fn comes_to(c: &mut Connection, code: &str, expected: &str) {
    let started = Instant::now();
    while text(c.eval(code)) != expected {
        assert!(started.elapsed() < Duration::from_secs(5), "{code} is {}, not {expected}", text(c.eval(code)));
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn pw_decides_who_connects_and_fails_closed() {
    let (mut local, _listener, address) = serve(&[".z.pw: {[user; password] (user = `ann) and password ~ \"open\"}"]);
    assert!(IpcClient::connect_addr(&address, "ann", "open").is_ok());
    for (user, password) in [("ann", "shut"), ("bob", "open")] {
        assert_eq!(IpcClient::connect_addr(&address, user, password).err().map(|e| e.signal), Some("access".into()), "{user}");
    }
    // an error, or an answer that is not true, keeps everyone out
    for broken in [".z.pw: {[u; p] u + 1}", ".z.pw: {[u; p] 1}", ".z.pw: {[u] 1b}"] {
        local.eval(broken).unwrap();
        assert!(IpcClient::connect_addr(&address, "ann", "open").is_err(), "{broken}");
    }
    local.eval("\\x .z.pw").unwrap();
    assert!(IpcClient::connect_addr(&address, "bob", "any").is_ok());
    comes_to(&mut local, "count .z.H", "0");
}

#[test]
fn opening_messages_and_closing_are_heard() {
    let (mut local, _listener, address) = serve(&[
        "seen: ()",
        ".z.po: {seen:: seen, enlist (`open; x = .z.w; .z.u)}",
        ".z.pc: {seen:: seen, enlist (`close; x; .z.w)}",
        ".z.pg: {$[x ~ \"secret\"; 'denied; (`answer; value x)]}",
        ".z.ps: {told:: x}",
    ]);
    let mut client = IpcClient::connect_addr(&address, "ann", "").unwrap();
    assert_eq!(text(client.query_str("6 * 7")), "(`answer;42)");
    assert_eq!(client.query_str("secret").err().map(|e| e.signal), Some("denied".into()));
    // a list sent as a message goes to the handler too
    let message = local.eval("(\"neg\"; 3)").unwrap();
    assert_eq!(text(client.query_k(&message)), "(`answer;-3)");
    client.send_async_k(&K::string("hello")).unwrap();
    comes_to(&mut local, "@[value; `told; `none]", "\"hello\"");
    comes_to(&mut local, "seen", ",(`open;1b;`ann)");
    drop(client);
    comes_to(&mut local, "last seen", "(`close;1000i;0i)");
    // restoring the defaults
    local.eval("\\x .z.pg").unwrap();
    let mut client = IpcClient::connect_addr(&address, "ann", "").unwrap();
    assert_eq!(text(client.query_str("6 * 7")), "42");
    assert_eq!(text(client.query_str("(.z.w; .z.a; .z.H; .z.W)")), "(1000i;2130706433i;`s#,1000i;(,1000i)!,0i)");
}

#[test]
fn the_timer_and_value_set_are_called() {
    let server = Server::new();
    let mut c = server.connect();
    let _timer = server.start_background();
    for statement in ["ticks: 0", ".z.ts: {ticks:: 1 + ticks; at:: x}", "\\t 5"] {
        c.eval(statement).unwrap();
    }
    comes_to(&mut c, "ticks > 2", "1b");
    assert_eq!(text(c.eval("(system \"t\"; type at)")), "(5i;-12h)");
    c.eval("\\t 0").unwrap();
    std::thread::sleep(Duration::from_millis(60));
    let stopped = c.eval("ticks").unwrap();
    std::thread::sleep(Duration::from_millis(60));
    assert_eq!(c.eval("ticks").unwrap(), stopped);

    for statement in ["heard: `symbol$()", ".z.vs: {[name; index] heard:: heard, name; n:: count heard}", "a: 1", "b: ([] v: 1 2)", "`b insert ([] v: enlist 3)", ".c.d: 1"] {
        c.eval(statement).unwrap();
    }
    // (not itself, and not names in other namespaces)
    assert_eq!(text(c.eval("heard")), "`a`b`b");
}

#[test]
fn process_values() {
    let mut c = Server::new().connect();
    assert_eq!(text(c.eval("(.z.w; .z.q; .z.r; count .z.l; type .z.x; type .z.n; type .z.N; type .z.z; type .z.Z)")), "(0i;0b;0b;0;0h;-16h;-16h;-15h;-15h)");
    assert_eq!(text(c.eval("(0 < .z.c; type .z.h; type .z.a; type .z.f; 0 < count .z.X; 0.001 > abs .z.z - `datetime$.z.p)")), "(1b;-11h;-6h;-11h;1b;1b)");
}

#[test]
fn the_shell_calls_input_and_exit() {
    use std::io::Write;
    let script = ".z.exit: {0N!(`bye; x)}\n.z.pi: {0N!(`typed; x); value x}\n1 + 2\n\\x .z.pi\n3 + 4\n\\\\\n";
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_kix"))
        .args(["--", "-abc", "1"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.as_mut().unwrap().write_all(script.as_bytes()).unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let stdout = String::from_utf8_lossy(&out.stdout).replace('\r', "");
    assert_eq!(stdout, "(`typed;\"1 + 2\\n\")\n3\n7\n(`bye;0i)\n");
}
