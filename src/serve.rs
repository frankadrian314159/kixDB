//! `kix-serve` — standalone TCP server speaking native kdb+ binary IPC v3.
//!
//! ```text
//! kix-serve [DATA_DIR] [-p|--port N] [--bind ADDR] [-u USER:PASS] [--no-sync]
//! ```
use kixdb::ipc::{IpcConfig, Listener};
use kixdb::{Server, StorageOptions};

fn usage() -> ! {
    eprintln!(
        "usage: kix-serve [DATA_DIR] [-p|--port N] [--bind ADDR] [-u USER:PASSWORD] [--no-sync]\n\n\
         DATA_DIR        directory for durable WAL/snapshot and HDB partitions (omitted: in-memory)\n\
         -p, --port N    TCP port for kdb+ binary IPC (default 5000)\n\
         --bind ADDR     interface address to listen on (default 127.0.0.1)\n\
         -u USER:PASS    require credentials during IPC handshake (or set KIX_AUTH=user:pass)\n\
         --no-sync       disable per-write fsync on WAL appends"
    );
    std::process::exit(2)
}

fn main() {
    let mut dir: Option<String> = None;
    let mut port: u16 = 5000;
    let mut bind = "127.0.0.1".to_string();
    let mut auth = std::env::var("KIX_AUTH").ok();
    let mut opts = StorageOptions::default();

    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "-p" | "--port" => port = args.next().and_then(|v| v.parse().ok()).unwrap_or_else(|| usage()),
            "--bind" => bind = args.next().unwrap_or_else(|| usage()),
            "-u" | "--auth" => auth = Some(args.next().unwrap_or_else(|| usage())),
            "--no-sync" => opts.sync_wal = false,
            "-h" | "--help" => usage(),
            other if !other.starts_with('-') && dir.is_none() => dir = Some(other.to_string()),
            _ => usage(),
        }
    }

    let server = match &dir {
        Some(d) => match Server::open_with(d, opts) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("cannot open '{d}': {e}");
                std::process::exit(1);
            }
        },
        None => Server::new(),
    };

    let ipc_cfg = match auth {
        Some(spec) => match spec.split_once(':') {
            Some((u, p)) => IpcConfig::with_single_user(u, p),
            None => IpcConfig::with_single_user(spec, ""),
        },
        None => IpcConfig::default(),
    };

    let listener = match Listener::bind(server, (bind.as_str(), port), ipc_cfg) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("cannot listen on {bind}:{port}: {e}");
            std::process::exit(1);
        }
    };

    eprintln!(
        "kixdb listening on {} (kdb+ IPC v3); workspace: {}",
        listener.local_addr(),
        dir.as_deref().unwrap_or("in memory")
    );
    listener.wait();
}

#[cfg(feature = "fast-alloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
