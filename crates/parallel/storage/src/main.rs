//! `kix-shard`: one shard node of a distributed kixdb cluster.
//!
//!   kix-shard --id 0 --listen 127.0.0.1:50051 [--data DIR]
//!   kix-shard --id 0 --listen HOST:PORT --data DIR --replica 1 --peers 2=HOST:PORT,3=HOST:PORT [--raft-dir DIR]
//!
//! The second form makes the node one replica of a replicated shard: `--replica` is its number in the group, `--peers` the
//! other replicas (number=address). Every replica of a shard is given the same `--id` and its own `--data`. The control
//! node's cluster file lists all replicas of the shard in one address, separated by commas.
//!
//! Other flags: `--join` (a new replica that waits for `ADD REPLICA`), `--addr HOST:PORT` (how the others reach this
//! replica, when not the listen address), `--snapshot-every N` (applied entries between Raft snapshots; 5000),
//! `--drain-secs N` (how long requests may take to finish when the node is told to stop; 15) and `--metrics-listen
//! HOST:PORT` (serve `/metrics` in the Prometheus text format and `/health`). The node stops gracefully on SIGTERM and
//! Ctrl-C: it refuses new work, lets requests finish, forces the log and writes a checkpoint, then closes its listener.
//!
//! Without `--data` the shard keeps its data in memory; with it the shard is durable (write-ahead log and checkpoints in
//! DIR, exactly like `kix-serve DIR`).
use kix_shared_ces::metrics::{render_prometheus, serve_http};
use kix_storage::{spawn, ReplicaConfig, ShardNode};
use kixdb::Server;
use std::net::SocketAddr;
use std::time::Duration;

fn usage() -> ! {
    eprintln!(
        "usage: kix-shard --id N --listen HOST:PORT [--data DIR] [--metrics-listen HOST:PORT] [--drain-secs N]\n       \
         kix-shard --id N --listen HOST:PORT --data DIR --replica R --peers R2=HOST:PORT,R3=HOST:PORT\n                 \
         [--raft-dir DIR] [--join] [--addr HOST:PORT] [--snapshot-every N]"
    );
    std::process::exit(2);
}

fn fail(message: String) -> ! {
    eprintln!("kix-shard: {message}");
    std::process::exit(1);
}

fn parsed<T: std::str::FromStr>(value: Option<String>) -> T {
    value.and_then(|v| v.parse().ok()).unwrap_or_else(|| usage())
}

#[tokio::main]
async fn main() {
    let (mut id, mut listen, mut data, mut replica) = (None::<u32>, None::<SocketAddr>, None::<String>, None::<u64>);
    let mut peers: Vec<(u64, String)> = vec![];
    let (mut raft_dir, mut addr, mut metrics_listen) = (None::<String>, None::<String>, None::<String>);
    let (mut join, mut snapshot_every, mut drain_secs) = (false, None::<u64>, 15u64);
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--id" => id = Some(parsed(args.next())),
            "--listen" => listen = Some(parsed(args.next())),
            "--data" => data = args.next(),
            "--replica" => replica = Some(parsed(args.next())),
            "--raft-dir" => raft_dir = args.next(),
            "--join" => join = true,
            "--addr" => addr = args.next(),
            "--drain-secs" => drain_secs = parsed(args.next()),
            "--metrics-listen" => metrics_listen = args.next(),
            "--snapshot-every" => snapshot_every = Some(parsed(args.next())),
            "--peers" => {
                for p in args.next().unwrap_or_default().split(',').filter(|p| !p.is_empty()) {
                    let Some((n, a)) = p.split_once('=') else { usage() };
                    peers.push((parsed(Some(n.to_string())), a.to_string()));
                }
            }
            _ => usage(),
        }
    }
    let (Some(id), Some(listen)) = (id, listen) else { usage() };
    let server = match &data {
        Some(dir) => Server::open(dir).unwrap_or_else(|e| fail(format!("cannot open {dir}: {e}"))),
        None => Server::new(),
    };
    let node = match replica {
        Some(r) => {
            let Some(d) = &data else { fail("a replica needs --data (its engine must be durable)".into()) };
            let dir = raft_dir.unwrap_or_else(|| format!("{d}.raft"));
            let mut rc = ReplicaConfig::new(r, peers, dir);
            rc.join = join;
            rc.addr = addr.unwrap_or_else(|| listen.to_string());
            if let Some(n) = snapshot_every {
                rc.snapshot_every = n;
            }
            ShardNode::replicated(id, server, rc).unwrap_or_else(|e| fail(format!("cannot start the replica: {e}")))
        }
        None => ShardNode::new(id, server),
    };
    if let Some(at) = &metrics_listen {
        let control = node.control();
        match serve_http(at, move || render_prometheus(&[control.metrics()])) {
            Ok(a) => eprintln!("kix-shard {id}: metrics on http://{a}/metrics"),
            Err(e) => fail(format!("cannot serve metrics on {at}: {e}")),
        }
    }
    let running = spawn(node, listen).await.unwrap_or_else(|e| fail(format!("cannot listen on {listen}: {e}")));
    let place = data.map(|d| format!("data in {d}")).unwrap_or_else(|| "in memory".into());
    eprintln!("kix-shard {id} listening on {} ({place})", running.addr);
    wait_for_stop().await;
    eprintln!("kix-shard {id}: draining (up to {drain_secs}s for requests in progress)");
    let r = running.graceful_shutdown(Duration::from_secs(drain_secs)).await;
    eprintln!(
        "kix-shard {id}: stopped ({} request(s) abandoned; checkpoint {}; {} ms)",
        r.abandoned,
        if r.checkpointed { "written" } else { "not written" },
        r.waited_ms
    );
}

/// Wait for the signal to stop: SIGTERM or SIGINT (Ctrl-C), where the platform has SIGTERM.
async fn wait_for_stop() {
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
}
