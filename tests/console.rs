//! The console's handles: `-1` and `-2` write lines to the output and as errors, `1` and `2` text as it is.
use kixdb::ipc::IpcClient;
use kixdb::{Event, Server, K};
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};

fn printed(code: &str) -> Vec<Event> {
    Server::new().connect().execute(code).events
}

fn console(text: &str, error: bool) -> Event {
    Event::Console { text: text.into(), error }
}

#[test]
fn the_console_is_written_to() {
    assert_eq!(printed("-1 \"hello\";"), [console("hello\n", false)]);
    assert_eq!(printed("-2 (\"a\"; \"bc\");"), [console("a\nbc\n", true)]);
    let text = [console("no ", false), console("line end", false), console("x", true)];
    assert_eq!(printed("1 \"no \";\n1 (\"line\"; \" end\");\n2 \"x\";"), text);
    // the value is the handle, shown unless the line ends with a semicolon; in order with what else is printed
    let events = printed("show 1; -1 \"two\"");
    assert_eq!(events, [Event::Message("1".into()), console("two\n", false), Event::Value(K::Int(-1))]);
    assert_eq!(printed("1+1;\n2+2"), [Event::Value(K::Long(4))]);
    let lines = [console("p\n", false), console("q\n", false)];
    assert_eq!(printed("-1 each (\"p\"; \"q\");"), lines);
    assert_eq!(printed("h: -1; f: {h x}; f each (\"p\"; \"q\");"), lines);
    assert_eq!(printed("-1@\"p\";\nneg[1] \"q\";"), lines);
    assert_eq!(printed("-1 \"n is \", string 6*7;"), [console("n is 42\n", false)]);
    // only text is written
    let mut c = Server::new().connect();
    assert_eq!(c.eval("-1 `sym").unwrap_err().signal, "type");
    assert_eq!(c.eval("@[-1; 42; {`caught}]").unwrap().to_string(), "`caught");
}

#[test]
fn a_script_prints_to_its_console() {
    let script = "-1 \"out\";\n-2 \"err\";\n1 \"a\";\n1 \"b\\n\";\n\\\\\n";
    let mut shell = Command::new(env!("CARGO_BIN_EXE_kix")).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    shell.stdin.take().unwrap().write_all(script.as_bytes()).unwrap();
    let output = shell.wait_with_output().unwrap();
    assert_eq!(String::from_utf8_lossy(&output.stdout), "out\nab\n");
    assert_eq!(String::from_utf8_lossy(&output.stderr), "err\n");
}

#[test]
fn a_server_prints_what_a_client_has_it_print() {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kix-serve"));
    let mut server = command.args(["-p", "0"]).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    let mut said = String::new();
    BufReader::new(server.stderr.take().unwrap()).read_line(&mut said).unwrap();
    let address = said.split_whitespace().find(|word| word.starts_with("127.0.0.1:")).unwrap();
    let mut client = IpcClient::connect_addr(address, "u", "").unwrap();
    assert_eq!(client.query_str("-1 \"from a client\"").unwrap(), K::Int(-1));
    client.query_str("show 42; 0N!`seen;").unwrap();
    server.kill().unwrap();
    let mut output = String::new();
    server.stdout.take().unwrap().read_to_string(&mut output).unwrap();
    server.wait().unwrap();
    assert_eq!(output, "from a client\n42\n`seen\n");
}
