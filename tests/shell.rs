//! Shell commands (`\ls`, `system "ls"`, `\cd`), which a process that serves clients runs only if told to.
use kixdb::ipc::{IpcClient, IpcConfig, Listener};
use kixdb::{Connection, Server, K};
use std::io::{Read, Write};

fn text(result: kixdb::KResult<K>) -> String {
    result.map(|k| k.to_string()).unwrap_or_else(|e| format!("'{}", e.signal))
}

fn check(c: &mut Connection, cases: &[(&str, &str)]) {
    for (code, expected) in cases {
        assert_eq!(&text(c.eval(code)), expected, "{code}");
    }
}

#[test]
fn commands_give_the_lines_they_print() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::open(dir.path()).unwrap();
    let mut c = server.connect().at_console();
    // off until allowed, in a library as anywhere
    check(&mut c, &[("system \"echo hi\"", "'access"), ("\\echo hi", "'access")]);
    server.allow_shell_commands(true);
    check(&mut c, &[
        ("system \"echo hi\"", ",\"hi\""),
        ("\\echo one two", ",\"one two\""),
        ("system \"  echo a&&echo b\"", "(,\"a\";,\"b\")"),
        ("f: {system \"echo \", string x}; f 42", ",\"42\""),
        ("system \"cd .\"", "::"),
        ("system \"exit 3\"", "'os"),
        ("@[system; \"exit 1\"; {`caught}]", "`caught"),
        // quotes reach the shell as they are
        ("count system \"echo \\\"a b\\\"\"", "1"),
        // q's own commands are never the shell's
        ("\\p", "0i"),
        ("\\o 1", "'nyi"),
        ("system \"cd elsewhere\"", "'os"),
    ]);
    // commands run where relative paths start: in the workspace's directory
    let show = if cfg!(windows) { "type" } else { "cat" };
    c.eval("`:note.txt 0: (\"first\"; \"second\")").unwrap();
    assert_eq!(text(c.eval(&format!("system \"{show} note.txt\""))), "(\"first\";\"second\")");
    let held = c.eval("system \"cd\"").unwrap().as_str().unwrap();
    assert_eq!(std::path::Path::new(&held), dir.path());
}

#[test]
fn a_process_that_serves_clients_runs_none_unless_told_to() {
    let server = Server::new();
    server.allow_shell_commands(true);
    let mut local = server.connect();
    let listener = Listener::bind(server.clone(), "127.0.0.1:0", IpcConfig::default()).unwrap();
    let mut client = IpcClient::connect_addr(&listener.local_addr().to_string(), "u", "").unwrap();
    // not for a client, however it asks, nor for what it can have the process run
    for code in ["system \"echo hi\"", "value \"\\\\echo hi\"", "\\echo hi"] {
        assert_eq!(text(client.query_str(code)), "'access", "{code}");
    }
    client.query_str("report: {system \"echo hi\"}").unwrap();
    check(&mut local, &[("report[]", "'access"), ("system \"echo hi\"", "'access")]);
    drop(listener);
    let open = IpcConfig { allow_shell: true, ..IpcConfig::default() };
    let listener = Listener::bind(server.clone(), "127.0.0.1:0", open).unwrap();
    let mut client = IpcClient::connect_addr(&listener.local_addr().to_string(), "u", "").unwrap();
    assert_eq!(text(client.query_str("report[]")), ",\"hi\"");
}

#[test]
fn an_http_request_is_not_a_client() {
    let server = Server::new();
    let mut local = server.connect();
    local.eval("x: 1").unwrap();
    let listener = Listener::bind(server.clone(), "127.0.0.1:0", IpcConfig::default()).unwrap();
    // what a web page could have a browser send: a request whose body holds a message, here `x: 2`
    let mut request = b"POST / HTTP/1.1\r\nHost: localhost\r\n\r\n\0".to_vec();
    let message = [&[10u8, 0, 4, 0, 0, 0][..], b"x: 2"].concat();
    request.extend([1, 0, 0, 0]);
    request.extend((8 + message.len() as u32).to_le_bytes());
    request.extend(message);
    let mut tcp = std::net::TcpStream::connect(listener.local_addr()).unwrap();
    tcp.write_all(&request).unwrap();
    // the connection ends with nothing answered
    let mut answer = Vec::new();
    let _ = tcp.read_to_end(&mut answer);
    assert!(answer.is_empty(), "{answer:?}");
    assert_eq!(text(local.eval("x")), "1");
}
