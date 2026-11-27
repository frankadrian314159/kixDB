//! `kix-cluster`: the control node of a distributed kixdb cluster.
//!
//!   kix-cluster --config cluster.json [--state FILE] --listen PORT [--bind ADDR] [-u USER:PASS] [--metrics-listen HOST:PORT]
//!   kix-cluster --config cluster.json [--state FILE] [--plan] < script.q
//!   kix-cluster --make-certs DIR NAME...
//!
//! Connections to the shards use mutual TLS: give `--node-ca FILE --node-cert FILE --node-key FILE`, or `--insecure` to
//! leave them unprotected. `--make-certs` writes a certificate authority and a certificate for every node into DIR.
//! Listening for clients on an address other machines can reach needs `-u` and `--force-tls`, or `--insecure-clients`.
//!
//! With `--listen` it serves kdb+ IPC clients (`kix --connect`, `kix-gui --connect`, `hopen`) as one database. Without
//! it, it runs `q` from standard input against the cluster; `--plan` shows on standard error how each statement ran.
//! Besides `q`, both accept `EXPLAIN query`, `DISTRIBUTE table BY col | PARTITIONED BY col | REPLICATED`, `SHARDS`,
//! `ADD SHARD host:port`, `REMOVE SHARD`, `REPLICAS shard`, `ADD REPLICA shard id host:port` and
//! `REMOVE REPLICA shard id`.
//!
//! With `--listen`, `--scale-command PROGRAM --scale-above-mb N --max-shards N` adds shards by itself: while a shard's
//! variables hold more than N MB, `PROGRAM ID` is run to start shard ID and print its address. `--scale-interval SECS`
//! (30), `--scale-checks N` (3 in a row), `--scale-cooldown SECS` (300) and `--scale-dry-run` tune it.
//!
//! Several control nodes form a group with one master: give each `--control-id N --control-listen HOST:PORT --data DIR`
//! (and `--control-addr HOST:PORT` if the others reach it elsewhere) with `--listen`. The first takes the shards from
//! `--config` (or an existing `--state`); the others take `--join HOST:PORT`, the control address of any node of the
//! group, and no `--config`. Clients may connect to any of them. `CONTROLS` lists them; `REMOVE CONTROL id` removes one.
//!
//! `--wait-shards SECS` (30) waits for every shard to answer before starting; `--broadcast-limit ROWS` (100000) is the
//! largest table a join copies to every shard rather than shuffling. `--request-timeout SECS` (300; 0: none) is how long
//! a shard may take over a request before the statement fails; a change that fails so may still be made.
use kix_control::{Autoscale, Cluster, GroupConfig};
use kix_shared_ces::metrics::serve_http;
use kix_shared_ces::tls::{make_certs, TlsFlags};
use kix_shared_ces::ClusterConfig;
use kixdb::ipc::{self_signed_cert, tls_server_config, IpcConfig, Listener};
use kixdb::{tools, Event};
use std::io::BufRead;
use std::process::ExitCode;
use std::time::Duration;

/// mimalloc runs the engine markedly faster than the system allocator.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn usage() -> ! {
    eprintln!(
        "usage: kix-cluster --config cluster.json [--state FILE] --listen PORT [--bind ADDR] [-u USER:PASSWORD]\n                   \
         [--tls-cert FILE --tls-key FILE | --tls] [--force-tls] [--insecure-clients] [--metrics-listen HOST:PORT]\n                   \
         [--wait-shards SECS] [--broadcast-limit ROWS] [--request-timeout SECS]\n                   \
         [--scale-command PROGRAM --scale-above-mb N --max-shards N [--scale-interval SECS] [--scale-checks N]\n                   \
         [--scale-cooldown SECS] [--scale-dry-run]]\n                   \
         [--control-id N --control-listen HOST:PORT --data DIR [--control-addr HOST:PORT] [--join HOST:PORT]]\n       \
         kix-cluster --config cluster.json [--state FILE] [--plan] < script.q\n       \
         kix-cluster --make-certs DIR NAME...\n\
         connections to the shards need --node-ca FILE --node-cert FILE --node-key FILE, or --insecure"
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
    kixdb::engine::par::one_thread_a_core();
    let (mut config, mut state, mut listen) = (None::<String>, None::<String>, None::<u16>);
    let (mut bind, mut auth) = ("127.0.0.1".to_string(), std::env::var("KIX_AUTH").ok());
    let (mut metrics_listen, mut broadcast_limit, mut request_timeout) = (None::<String>, None::<usize>, None::<u64>);
    let (mut show_plan, mut wait_shards, mut insecure_clients) = (false, 30u64, false);
    let (mut cert, mut key, mut self_signed, mut force_tls) = (None::<String>, None::<String>, false, false);
    let (mut scale_command, mut scale_above, mut max_shards) = (None::<String>, None::<usize>, None::<usize>);
    let (mut scale_interval, mut scale_checks, mut scale_cooldown, mut dry_run) = (None::<u64>, None::<u32>, None::<u64>, false);
    let (mut control_id, mut control_listen, mut control_addr, mut data, mut join) = (None, None, None::<String>, None::<String>, None::<String>);
    let mut tls = TlsFlags::default();
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        if tls.take(&a, &mut args) {
            continue;
        }
        match a.as_str() {
            "--make-certs" => {
                let Some(dir) = args.next() else { usage() };
                let names: Vec<String> = args.collect();
                make_certs(&dir, &names).unwrap_or_else(|e| fail(e));
                eprintln!("kix-cluster: certificates in {dir}; keep ca.key away from the nodes");
                return ExitCode::SUCCESS;
            }
            "--config" => config = args.next(),
            "--state" => state = args.next(),
            "--listen" | "-p" => listen = Some(parsed(args.next())),
            "--bind" => bind = parsed(args.next()),
            "-u" | "--auth" => auth = args.next(),
            "--tls-cert" => cert = args.next(),
            "--tls-key" => key = args.next(),
            "--tls" => self_signed = true,
            "--force-tls" => force_tls = true,
            "--insecure-clients" => insecure_clients = true,
            "--metrics-listen" => metrics_listen = args.next(),
            "--broadcast-limit" => broadcast_limit = Some(parsed(args.next())),
            "--request-timeout" => request_timeout = Some(parsed(args.next())),
            "--wait-shards" => wait_shards = parsed(args.next()),
            "--plan" => show_plan = true,
            "--scale-command" => scale_command = args.next(),
            "--scale-above-mb" => scale_above = Some(parsed(args.next())),
            "--max-shards" => max_shards = Some(parsed(args.next())),
            "--scale-interval" => scale_interval = Some(parsed(args.next())),
            "--scale-checks" => scale_checks = Some(parsed(args.next())),
            "--scale-cooldown" => scale_cooldown = Some(parsed(args.next())),
            "--scale-dry-run" => dry_run = true,
            "--control-id" => control_id = Some(parsed(args.next())),
            "--control-listen" => control_listen = Some(parsed(args.next())),
            "--control-addr" => control_addr = args.next(),
            "--data" => data = args.next(),
            "--join" => join = args.next(),
            _ => usage(),
        }
    }
    let tuned = scale_interval.is_some() || scale_checks.is_some() || scale_cooldown.is_some() || dry_run;
    let scaling = match (scale_command, scale_above, max_shards) {
        (Some(command), Some(above), Some(most)) if listen.is_some() => {
            let mut policy = Autoscale::new(command, above.saturating_mul(1 << 20), most);
            policy.interval = scale_interval.map_or(policy.interval, |s| Duration::from_secs(s.max(1)));
            policy.checks = scale_checks.unwrap_or(policy.checks);
            policy.cooldown = scale_cooldown.map_or(policy.cooldown, Duration::from_secs);
            policy.dry_run = dry_run;
            Some(policy)
        }
        (None, None, None) if !tuned => None,
        _ => fail("adding shards by itself needs --listen, --scale-command, --scale-above-mb and --max-shards".into()),
    };
    // an existing state file knows the shards; otherwise the configuration names them
    let known = state.as_deref().is_some_and(|s| std::path::Path::new(s).exists());
    let group = match (control_id, control_listen, data) {
        (Some(id), Some(control), Some(data)) if listen.is_some() => {
            Some(GroupConfig { id, listen: control, advertise: control_addr, data: data.into(), join, import: state.clone().map(Into::into) })
        }
        (None, None, None) if control_addr.is_none() && join.is_none() => None,
        _ => fail("a control node of a group needs --listen, --control-id, --control-listen and --data".into()),
    };
    let config = match config {
        Some(path) => ClusterConfig::from_file(&path).unwrap_or_else(|e| fail(format!("{path}: {e}"))),
        // (a node of a group may have the shards already, or learn them)
        None if known || group.is_some() => ClusterConfig::default(),
        None => usage(),
    };
    let tls = tls.load().unwrap_or_else(|e| fail(e));
    if tls.is_none() {
        eprintln!("kix-cluster: WARNING: connections to the shards are neither encrypted nor authenticated (--insecure)");
    }
    let grouped = group.is_some();
    let cluster = match (group, &state) {
        (Some(group), _) => Cluster::open_group(group, &config, tls),
        (None, Some(path)) => Cluster::open_with(path, &config, tls),
        (None, None) => Cluster::connect_with(&config, tls),
    }
    .unwrap_or_else(|e| fail(e.to_string()));
    if let Some(rows) = broadcast_limit {
        cluster.set_broadcast_limit(rows);
    }
    if let Some(secs) = request_timeout {
        cluster.set_request_timeout(Duration::from_secs(secs));
    }
    // (a group's master waits for the shards when it takes over)
    if wait_shards > 0 && !grouped {
        if let Err(down) = cluster.wait_for_shards(Duration::from_secs(wait_shards)) {
            let why: String = cluster.status().into_iter().filter_map(Result::err).map(|e| format!("
  {e}")).collect();
            fail(format!("shards {down:?} do not answer after {wait_shards}s{why}"));
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
    match ipc.check_exposure(&bind) {
        Err(why) if insecure_clients => eprintln!("kix-cluster: WARNING: {why}; continuing (--insecure-clients)"),
        Err(why) => fail(format!("{why}, or --insecure-clients to listen unprotected")),
        Ok(()) => {}
    }
    let listener = Listener::bind(cluster.gateway(), (bind.as_str(), port), ipc)
        .unwrap_or_else(|e| fail(format!("cannot listen on {bind}:{port}: {e}")));
    match cluster.control_address() {
        Some(control) => eprintln!("kix-cluster: control node listening on {} (kdb+ IPC) and {control} (its group)", listener.local_addr()),
        None => eprintln!("kix-cluster: control node listening on {} (kdb+ IPC), {} shard(s)", listener.local_addr(), cluster.shard_count()),
    }
    let scaler = scaling.map(|policy| cluster.autoscale(policy));
    wait_for_stop();
    eprintln!("kix-cluster: stopping");
    drop(listener);
    drop(scaler);
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
                Event::Rows(rs) => println!("{}", kixdb::display::grid(&rs)),
                Event::Value(v) => println!("{}", kixdb::display::console(&v)),
                printed @ (Event::Message(_) | Event::Console { .. }) => printed.print(),
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
