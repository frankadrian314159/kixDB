//! `kix` — interactive `q` shell / REPL and script runner for `kixdb`.
//!
//! ```text
//! kix [DATA_DIR | SCRIPT.q] [-D dir] [-p port] [--connect host[:port]] [-U user -P pass]
//!     [-q|-Q expr]... [-f|-i script.q]...
//! ```
use kixdb::inspect::ObjectKind;
use kixdb::ipc::{IpcClient, IpcConfig, Listener};
use kixdb::{tools, BatchResult, Connection, Event, Server, StorageOptions, K};
use std::fs;
use std::io::{self, BufRead, IsTerminal, Write};
use std::path::Path;
use std::process::ExitCode;
use std::time::Instant;

const HELP: &str = "\
Enter q / qSQL expressions at the q) prompt. Backslash commands match kdb+, and dot commands mirror tsql:
  \\a  | .tables              list tables in workspace
  \\b  | .views               list views (lazy :: definitions)
  \\f  | .functions           list user functions
  \\v  | .vars                list global variables
  \\d [.ns]                   show or switch current namespace
  \\l PATH                    load a .q script or mount an HDB directory
  \\t [:N] EXPR               execution time in milliseconds (N iterations)
  \\ts [:N] EXPR              time (ms) and space (bytes) of EXPR
  \\p [PORT]                  start/show background IPC listener on PORT
  \\w  | .storage             workspace memory, symbol pool, and WAL statistics
  \\\\  | .quit | exit         exit shell

Dot commands:
  .meta TABLE                columns, type chars, keys, and attributes (`s# `u# `p# `g#)
  .schema [NAME]             q definition script for a table, view, or function
  .import [--new] FILE TBL   import CSV into existing or new table
  .export FILE QUERY         export a table query's result as CSV
  .read FILE                 run a .q script file
  .plan QUERY                show execution / partition / cluster plan without running
  .timer on|off              print elapsed time after every command
  .checkpoint                fold WAL into an atomic snapshot now
  .help                      show this help";

const USAGE: &str = "usage: kix [DATA_DIR | SCRIPT.q] [-D dir] [-p port] [--connect host[:port]] [-U user -P pass] [-Q expr]... [-i script.q]...

Without a data directory the workspace lives in memory only. Passing a .q file loads it before entering the REPL
(or exits if -Q/-q is also given). --connect attaches the shell to a remote kix-serve / kix-cluster over kdb+ IPC.";

enum Target {
    Local {
        conn: Connection,
        server: Server,
        listener: Option<Listener>,
    },
    Remote {
        client: IpcClient,
        addr: String,
    },
}

struct Shell {
    target: Target,
    timer: bool,
    failed: bool,
}

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let mut data_dir: Option<String> = None;
    let mut preload_scripts: Vec<String> = Vec::new();
    let mut jobs: Vec<String> = Vec::new();
    let mut listen_port: Option<u16> = None;
    let mut remote_addr: Option<String> = None;
    let mut user = String::from("admin");
    let mut pass = String::new();
    let opts = StorageOptions::default();

    let bad = |m: &str| {
        eprintln!("{m}\n{USAGE}");
        ExitCode::from(2)
    };

    while let Some(a) = args.next() {
        match a.as_str() {
            "-h" | "--help" | "-?" => {
                println!("{USAGE}\n\n{HELP}");
                return ExitCode::SUCCESS;
            }
            "-D" | "--data" => match args.next() {
                Some(d) => data_dir = Some(d),
                None => return bad("-D needs a directory"),
            },
            "-p" | "--port" => match args.next().and_then(|v| v.parse::<u16>().ok()) {
                Some(p) => listen_port = Some(p),
                None => return bad("-p needs a TCP port"),
            },
            "--connect" | "-S" => match args.next() {
                Some(s) => remote_addr = Some(s),
                None => return bad("--connect needs HOST or HOST:PORT"),
            },
            "-U" => match args.next() {
                Some(u) => user = u,
                None => return bad("-U needs a username"),
            },
            "-P" => match args.next() {
                Some(p) => pass = p,
                None => return bad("-P needs a password"),
            },
            "-Q" | "-q" | "-c" => match args.next() {
                Some(q) => jobs.push(q),
                None => return bad("-Q needs a q expression"),
            },
            "-i" | "-f" => match args.next().map(fs::read_to_string) {
                Some(Ok(s)) => jobs.push(s),
                Some(Err(e)) => return bad(&format!("cannot read script: {e}")),
                None => return bad("-i needs a .q file"),
            },
            other if !other.starts_with('-') => {
                if other.ends_with(".q") && Path::new(other).is_file() {
                    match fs::read_to_string(other) {
                        Ok(s) => preload_scripts.push(s),
                        Err(e) => return bad(&format!("cannot read '{other}': {e}")),
                    }
                } else if data_dir.is_none() {
                    data_dir = Some(other.to_string());
                } else {
                    return bad(&format!("unexpected argument '{other}'"));
                }
            }
            other => return bad(&format!("unknown argument '{other}'")),
        }
    }

    let target = if let Some(addr) = remote_addr {
        match IpcClient::connect_addr(&addr, &user, &pass) {
            Ok(client) => Target::Remote { client, addr },
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        let server = match &data_dir {
            Some(d) => match Server::open_with(d, opts) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("cannot open '{d}': {e}");
                    return ExitCode::FAILURE;
                }
            },
            None => Server::new(),
        };
        let listener = match listen_port {
            Some(p) => match Listener::bind(server.clone(), ("127.0.0.1", p), IpcConfig::default()) {
                Ok(l) => Some(l),
                Err(e) => {
                    eprintln!("cannot listen on port {p}: {e}");
                    return ExitCode::FAILURE;
                }
            },
            None => None,
        };
        let conn = server.connect_as_trusted(&user);
        Target::Local { conn, server, listener }
    };

    let mut sh = Shell { target, timer: false, failed: false };
    for s in &preload_scripts {
        sh.run_code(s);
    }

    if !jobs.is_empty() {
        for j in &jobs {
            sh.run_code(j);
        }
        return if sh.failed { ExitCode::FAILURE } else { ExitCode::SUCCESS };
    }

    let interactive = io::stdin().is_terminal();
    if interactive {
        match &sh.target {
            Target::Local { listener, .. } => {
                let port_note = listener.as_ref().map(|l| format!(", IPC {}", l.local_addr())).unwrap_or_default();
                let loc = data_dir.as_deref().unwrap_or("in-memory");
                println!("kixDB 0.1 ({loc}{port_note})   (.help for commands, \\\\ to exit)");
            }
            Target::Remote { addr, .. } => {
                println!("kixDB 0.1 connected to {addr}   (.help for commands, \\\\ to exit)");
            }
        }
    }
    sh.repl(interactive);
    ExitCode::SUCCESS
}

impl Shell {
    fn repl(&mut self, interactive: bool) {
        let stdin = io::stdin();
        let mut buf = String::new();
        loop {
            if interactive {
                if buf.is_empty() {
                    let ns_prefix = match &self.target {
                        Target::Local { conn, .. } => {
                            let ns = conn.current_namespace();
                            if ns == "." { String::new() } else { ns }
                        }
                        Target::Remote { .. } => String::new(),
                    };
                    print!("q{ns_prefix})");
                } else {
                    print!("  ");
                }
                let _ = io::stdout().flush();
            }
            let mut line = String::new();
            match stdin.lock().read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
            let trimmed = line.trim();
            if buf.trim().is_empty() {
                if matches!(trimmed, "\\\\" | "exit" | "quit" | ".quit") {
                    break;
                }
                if trimmed.is_empty() {
                    continue;
                }
                if let Some(rest) = trimmed.strip_prefix("\\p") {
                    self.handle_port_cmd(rest.trim());
                    continue;
                }
                let head = trimmed.split_whitespace().next().unwrap_or("");
                if matches!(
                    head,
                    ".help"
                        | ".quit"
                        | ".tables"
                        | ".views"
                        | ".functions"
                        | ".vars"
                        | ".meta"
                        | ".schema"
                        | ".import"
                        | ".export"
                        | ".read"
                        | ".plan"
                        | ".timer"
                        | ".storage"
                        | ".checkpoint"
                ) {
                    if !self.dot_command(trimmed) {
                        break;
                    }
                    continue;
                }
            }
            buf.push_str(&line);
            if tools::delimiters_balanced(&buf) {
                let code = std::mem::take(&mut buf);
                self.run_code(&code);
            }
        }
        if !buf.trim().is_empty() {
            self.run_code(&buf);
        }
    }

    fn handle_port_cmd(&mut self, arg: &str) {
        if let Target::Local { server, listener, .. } = &mut self.target {
            if arg.is_empty() {
                let p = listener.as_ref().map(|l| l.local_addr().port() as i32).unwrap_or(0);
                println!("{p}i");
            } else if arg == "0" {
                *listener = None;
            } else if let Ok(p) = arg.parse::<u16>() {
                match Listener::bind(server.clone(), ("127.0.0.1", p), IpcConfig::default()) {
                    Ok(l) => *listener = Some(l),
                    Err(e) => eprintln!("{e}"),
                }
            } else {
                eprintln!("'domain: invalid port '{arg}'");
            }
        }
    }

    fn run_code(&mut self, code: &str) {
        let t0 = Instant::now();
        match &mut self.target {
            Target::Local { conn, .. } => {
                let res = conn.execute(code);
                self.show_batch(&res);
            }
            Target::Remote { client, .. } => match client.query_str(code) {
                Ok(K::Null) => {}
                Ok(val) => println!("{}", val.q_repr()),
                Err(e) => {
                    self.failed = true;
                    eprintln!("{e}");
                }
            },
        }
        if self.timer {
            println!("({:.3} ms)", t0.elapsed().as_secs_f64() * 1e3);
        }
    }

    fn show_batch(&mut self, res: &BatchResult) {
        let stdout = io::stdout();
        let mut out = stdout.lock();
        for ev in &res.events {
            match ev {
                Event::Rows(rs) => {
                    let _ = writeln!(out, "{}", tools::format_grid(rs, 60));
                }
                Event::Value(v) => {
                    let _ = writeln!(out, "{}", v.q_repr());
                }
                Event::Message(m) => {
                    let _ = writeln!(out, "{m}");
                }
                Event::Error(e) => {
                    self.failed = true;
                    eprintln!("{e}");
                }
            }
        }
    }

    fn dot_command(&mut self, line: &str) -> bool {
        let (cmd, rest) = match line.find(char::is_whitespace) {
            Some(i) => (&line[..i], line[i..].trim()),
            None => (line, ""),
        };
        match cmd {
            ".help" => println!("{HELP}"),
            ".quit" => return false,
            ".timer" => match rest {
                "on" => self.timer = true,
                "off" => self.timer = false,
                _ => eprintln!("usage: .timer on|off"),
            },
            ".tables" => self.run_code("\\a"),
            ".views" => self.run_code("\\b"),
            ".functions" => self.run_code("\\f"),
            ".vars" => self.run_code("\\v"),
            ".meta" => {
                if rest.is_empty() {
                    eprintln!("usage: .meta TABLE");
                } else {
                    self.run_code(&format!("meta {rest}"));
                }
            }
            ".read" => match fs::read_to_string(rest) {
                Ok(s) => self.run_code(&s),
                Err(e) => eprintln!("cannot read '{rest}': {e}"),
            },
            ".export" => {
                let Some((file, q)) = rest.split_once(char::is_whitespace) else {
                    eprintln!("usage: .export FILE QUERY");
                    return true;
                };
                match &mut self.target {
                    Target::Local { conn, .. } => {
                        let res = conn.execute(q.trim());
                        if let Some(e) = res.first_error() {
                            eprintln!("{e}");
                        } else if let Some(rs) = res.resultsets().first() {
                            match fs::write(file, tools::to_csv(rs)) {
                                Ok(()) => println!("wrote {} row(s) to {file}", rs.rows.len()),
                                Err(e) => eprintln!("cannot write '{file}': {e}"),
                            }
                        } else {
                            eprintln!("query did not return a table");
                        }
                    }
                    Target::Remote { client, .. } => match client.query_str(q.trim()) {
                        Ok(val) => {
                            if let Some(rs) = kixdb::k_to_resultset(&val) {
                                let _ = fs::write(file, tools::to_csv(&rs));
                                println!("wrote {} row(s) to {file}", rs.rows.len());
                            }
                        }
                        Err(e) => eprintln!("{e}"),
                    },
                }
            }
            ".import" => {
                let (create, rem) = match rest.strip_prefix("--new") {
                    Some(r) => (true, r.trim()),
                    None => (false, rest),
                };
                let parts: Vec<&str> = rem.split_whitespace().collect();
                if parts.len() != 2 {
                    eprintln!("usage: .import [--new] FILE TABLE");
                    return true;
                }
                let Ok(csv_text) = fs::read_to_string(parts[0]) else {
                    eprintln!("cannot read '{}'", parts[0]);
                    return true;
                };
                let res = match &mut self.target {
                    Target::Local { conn, .. } => tools::import_csv(conn, &csv_text, parts[1], create),
                    Target::Remote { client, .. } => tools::import_csv(client, &csv_text, parts[1], create),
                };
                match res {
                    Ok(n) => println!("imported {n} row(s) into {}", parts[1]),
                    Err(e) => eprintln!("{e}"),
                }
            }
            ".schema" => match &mut self.target {
                Target::Local { conn, .. } => {
                    if rest.is_empty() {
                        for o in conn.objects().into_iter().filter(|o| matches!(o.kind, ObjectKind::Table | ObjectKind::KeyedTable | ObjectKind::PartitionedTable | ObjectKind::View | ObjectKind::Function)) {
                            if let Some(d) = conn.definition(&o.full_name()) {
                                println!("{d}");
                            }
                        }
                    } else if let Some(d) = conn.definition(rest) {
                        println!("{d}");
                    } else {
                        eprintln!("'{rest} not found");
                    }
                }
                Target::Remote { client, .. } => {
                    if rest.is_empty() {
                        for o in kixdb::inspect::remote_objects(client).into_iter().filter(|o| matches!(o.kind, ObjectKind::Table | ObjectKind::KeyedTable | ObjectKind::PartitionedTable | ObjectKind::View | ObjectKind::Function)) {
                            if let Some(d) = kixdb::inspect::remote_definition(client, &o.full_name()) {
                                println!("{d}");
                            }
                        }
                    } else if let Some(d) = kixdb::inspect::remote_definition(client, rest) {
                        println!("{d}");
                    } else {
                        eprintln!("'{rest} not found");
                    }
                }
            },
            ".plan" => {
                let res = match &mut self.target {
                    Target::Local { conn, .. } => conn.explain(rest),
                    Target::Remote { client, .. } => kixdb::inspect::remote_explain(client, rest),
                };
                match res {
                    Ok(lines) => {
                        for l in lines {
                            println!("{l}");
                        }
                    }
                    Err(e) => eprintln!("{e}"),
                }
            }
            ".storage" => {
                if let Target::Local { server, .. } = &self.target {
                    let st = server.storage_stats();
                    println!(
                        "memory: {} B | symbols: {} | tables: {} | views: {} | WAL: {} rec ({} B) | snapshot: {} B | dir: {}",
                        st.memory_bytes,
                        st.sym_count,
                        st.table_count,
                        st.view_count,
                        st.wal_records,
                        st.wal_bytes,
                        st.snap_bytes,
                        st.data_dir.as_deref().unwrap_or("in-memory")
                    );
                } else {
                    self.run_code("\\w");
                }
            }
            ".checkpoint" => {
                if let Target::Local { server, .. } = &self.target {
                    if let Err(e) = server.checkpoint() {
                        eprintln!("{e}");
                    } else {
                        println!("checkpoint written");
                    }
                }
            }
            other => eprintln!("unknown dot command '{other}' (.help for list)"),
        }
        true
    }
}
