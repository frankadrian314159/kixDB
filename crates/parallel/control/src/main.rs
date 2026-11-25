//! `kix-cluster`: the control node of a distributed kixdb cluster.
//!
//!   kix-cluster --config cluster.json [--state FILE] --listen PORT [--bind ADDR] [-u USER:PASS] [--metrics-listen HOST:PORT]
//!   kix-cluster --config cluster.json [--state FILE] [--plan] < script.q
//!
//! With `--listen` it serves kdb+ IPC clients (`kix --connect`, `kix-gui --connect`, `hopen`) as one database. Without
//! it, it runs `q` from standard input against the cluster; `--plan` shows on standard error how each statement ran.
//! Besides `q`, both accept `EXPLAIN query`, `DISTRIBUTE table BY col | PARTITIONED BY col | REPLICATED`, `SHARDS`, `REPLICAS shard`,
//! `ADD REPLICA shard id host:port` and `REMOVE REPLICA shard id`.
//!
//! `--wait-shards SECS` (30) waits for every shard to answer before starting; `--broadcast-limit ROWS` (100000) is the
//! largest table a join copies to every shard rather than shuffling.
use kix_control::Cluster;
use kix_shared_ces::metrics::serve_http;
use kix_shared_ces::ClusterConfig;
use kixdb::ipc::{self_signed_cert, tls_server_config, IpcConfig, Listener};
use kixdb::{tools, Event};
use std::io::BufRead;
use std::process::ExitCode;
use std::time::Duration;

fn usage() -> ! {
    eprintln!(
        "usage: kix-cluster --config cluster.json [--state FILE] --listen PORT [--bind ADDR] [-u USER:PASSWORD]\n                   \
         [--tls-cert FILE --tls-key FILE | --tls] [--force-tls] [--metrics-listen HOST:PORT] [--wait-shards SECS] [--broadcast-limit ROWS]\n       \
         kix-cluster --config cluster.json [--state FILE] [--plan] < script.q"
    );
    std::process::exit(2);
}

fn fail(message: String) -> ! {
    eprintln!("kix-cluster: {message}");
    std::process::exit(1);
}

fn parsed<T: std::str::FromStr>(value: Option<String>) -> T {
    value.and_then(|v| v.parse().ok()).unwrap_or_else(|| usage())
}

fn main() -> ExitCode {
    let (mut config, mut state, mut listen) = (None::<String>, None::<String>, None::<u16>);
    let (mut bind, mut auth) = ("127.0.0.1".to_string(), std::env::var("KIX_AUTH").ok());
    let (mut metrics_listen, mut broadcast_limit) = (None::<String>, None::<usize>);
    let (mut show_plan, mut wait_shards) = (false, 30u64);
    let (mut cert, mut key, mut self_signed, mut force_tls) = (None::<String>, None::<String>, false, false);
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--config" => config = args.next(),
            "--state" => state = args.next(),
            "--listen" | "-p" => listen = Some(parsed(args.next())),
            "--bind" => bind = parsed(args.next()),
            "-u" | "--auth" => auth = args.next(),
            "--tls-cert" => cert = args.next(),
            "--tls-key" => key = args.next(),
            "--tls" => self_signed = true,
            "--force-tls" => force_tls = true,
            "--metrics-listen" => metrics_listen = args.next(),
            "--broadcast-limit" => broadcast_limit = Some(parsed(args.next())),
            "--wait-shards" => wait_shards = parsed(args.next()),
            "--plan" => show_plan = true,
            _ => usage(),
        }
    }
    // an existing state file knows the shards; otherwise the configuration names them
    let known = state.as_deref().is_some_and(|s| std::path::Path::new(s).exists());
    let config = match config {
        Some(path) => ClusterConfig::from_file(&path).unwrap_or_else(|e| fail(format!("{path}: {e}"))),
        None if known => ClusterConfig::default(),
        None => usage(),
    };
    let cluster = match &state {
        Some(path) => Cluster::open(path, &config),
        None => Cluster::connect(&config),
    }
    .unwrap_or_else(|e| fail(e.to_string()));
    if let Some(rows) = broadcast_limit {
        cluster.set_broadcast_limit(rows);
    }
    if wait_shards > 0 {
        if let Err(down) = cluster.wait_for_shards(Duration::from_secs(wait_shards)) {
            fail(format!("shards {down:?} do not answer after {wait_shards}s"));
        }
    }
    if let Some(at) = &metrics_listen {
        let c = cluster.clone();
        match serve_http(at, move || c.prometheus()) {
            Ok(a) => eprintln!("kix-cluster: metrics on http://{a}/metrics"),
            Err(e) => fail(format!("cannot serve metrics on {at}: {e}")),
        }
    }
    let Some(port) = listen else { return run_input(&cluster, show_plan) };
    let mut ipc = match auth {
        Some(spec) => {
            let (user, password) = spec.split_once(':').unwrap_or((&spec, ""));
            IpcConfig::with_single_user(user, password)
        }
        None => IpcConfig::default(),
    };
    let read_pem = |p: &str| std::fs::read(p).unwrap_or_else(|e| fail(format!("cannot read {p}: {e}")));
    let pem = match (cert, key) {
        (Some(c), Some(k)) => Some((read_pem(&c), read_pem(&k))),
        (None, None) if self_signed => {
            let (c, k) = self_signed_cert(&bind).unwrap_or_else(|e| fail(e.to_string()));
            if let Err(e) = std::fs::write("kix-cluster-cert.pem", &c) {
                fail(format!("cannot write kix-cluster-cert.pem: {e}"));
            }
            Some((c.into_bytes(), k.into_bytes()))
        }
        (None, None) => None,
        _ => usage(),
    };
    if let Some((c, k)) = pem {
        ipc.tls = Some(tls_server_config(&c, &k).unwrap_or_else(|e| fail(e.to_string())));
        ipc.force_tls = force_tls;
    } else if force_tls {
        fail("--force-tls requires --tls or --tls-cert/--tls-key".into());
    }
    let listener = Listener::bind(cluster.gateway(), (bind.as_str(), port), ipc)
        .unwrap_or_else(|e| fail(format!("cannot listen on {bind}:{port}: {e}")));
    eprintln!("kix-cluster: control node listening on {} (kdb+ IPC), {} shard(s)", listener.local_addr(), cluster.shard_count());
    wait_for_stop();
    eprintln!("kix-cluster: stopping");
    drop(listener);
    match cluster.shutdown() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => fail(format!("cannot write the state: {e}")),
    }
}

/// Run `q` from standard input.
fn run_input(cluster: &Cluster, show_plan: bool) -> ExitCode {
    let mut session = cluster.session();
    let mut failed = false;
    let mut run = |code: &str| {
        for event in session.execute(code).events {
            match event {
                Event::Rows(rs) => println!("{}", tools::format_grid(&rs, 60)),
                Event::Value(v) => println!("{}", v.q_repr()),
                Event::Message(m) => println!("{m}"),
                Event::Error(e) => {
                    eprintln!("{e}");
                    failed = true;
                }
            }
        }
        if show_plan {
            session.last_plan().iter().for_each(|p| eprintln!("plan: {p}"));
        }
    };
    let mut buffer = String::new();
    for line in std::io::stdin().lock().lines().map_while(Result::ok) {
        if buffer.is_empty() && matches!(line.trim(), "\\\\" | "exit" | "quit") {
            break;
        }
        buffer.push_str(&line);
        buffer.push('\n');
        if tools::delimiters_balanced(&buffer) {
            if !buffer.trim().is_empty() {
                run(&buffer);
            }
            buffer.clear();
        }
    }
    if !buffer.trim().is_empty() {
        run(&buffer);
    }
    if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

/// Block until SIGTERM or SIGINT (Ctrl-C), where the platform has SIGTERM.
fn wait_for_stop() {
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread().enable_all().build() else { return };
    runtime.block_on(async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            if let Ok(mut term) = signal(SignalKind::terminate()) {
                tokio::select! {
                    _ = term.recv() => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
                return;
            }
        }
        let _ = tokio::signal::ctrl_c().await;
    });
}
