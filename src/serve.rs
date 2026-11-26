//! `kix-serve` — standalone TCP server speaking native kdb+ binary IPC v3.
//!
//! ```text
//! kix-serve [DATA_DIR] [-p|--port N] [--bind ADDR] [-u USER:PASS] [--tls-cert FILE --tls-key FILE | --tls] [--no-sync]
//!           [--max-queue MB] [-- ARGS]
//! ```
//!
//! An address other machines can reach needs `-u` and `--force-tls`, or `--insecure-clients` to listen unprotected.
use kixdb::ipc::{self_signed_cert, tls_server_config, IpcConfig, Listener};
use kixdb::{Server, StorageOptions};

fn usage() -> ! {
    eprintln!(
        "usage: kix-serve [DATA_DIR] [-p|--port N] [--bind ADDR] [-u USER:PASSWORD] [--tls-cert FILE --tls-key FILE | --tls] [--no-sync] [--max-queue MB]\n\n\
         DATA_DIR        directory for durable WAL/snapshot and HDB partitions (omitted: in-memory)\n\
         -p, --port N    TCP port for kdb+ binary IPC (default 5000)\n\
         --bind ADDR     interface address to listen on (default 127.0.0.1)\n\
         -u USER:PASS    require credentials during IPC handshake (or set KIX_AUTH=user:pass)\n\
         --tls-cert FILE  PEM certificate chain: also accept TLS connections (`tcps://`)\n\
         --tls-key FILE   PEM private key of that certificate\n\
         --tls           accept TLS with a new self-signed certificate, written to kix-cert.pem\n\
         --force-tls     reject plaintext connections (requires --tls or --tls-cert/--tls-key)\n\
         --insecure-clients  listen beyond this machine without both -u and --force-tls\n\
         --max-queue MB  disconnect a client when more than this waits to be sent to it (default 256; 0: no limit)\n\
         --no-sync       disable per-write fsync on WAL appends"
    );
    std::process::exit(2)
}

fn fail(message: &str) -> ! {
    eprintln!("kix-serve: {message}");
    std::process::exit(1)
}

fn read_pem(path: &str) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|e| fail(&format!("cannot read {path}: {e}")))
}

fn main() {
    let mut dir: Option<String> = None;
    let mut port: u16 = 5000;
    let mut bind = "127.0.0.1".to_string();
    let mut auth = std::env::var("KIX_AUTH").ok();
    let mut opts = StorageOptions::default();
    let (mut cert, mut key, mut self_signed, mut force_tls) = (None::<String>, None::<String>, false, false);
    let mut insecure_clients = false;
    let mut queue_limit = None::<usize>;

    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "-p" | "--port" => port = args.next().and_then(|v| v.parse().ok()).unwrap_or_else(|| usage()),
            "--bind" => bind = args.next().unwrap_or_else(|| usage()),
            "-u" | "--auth" => auth = Some(args.next().unwrap_or_else(|| usage())),
            "--no-sync" => opts.sync_wal = false,
            "--tls-cert" => cert = Some(args.next().unwrap_or_else(|| usage())),
            "--tls-key" => key = Some(args.next().unwrap_or_else(|| usage())),
            "--tls" => self_signed = true,
            "--force-tls" => force_tls = true,
            "--insecure-clients" => insecure_clients = true,
            "--max-queue" => {
                let megabytes: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or_else(|| usage());
                queue_limit = Some(if megabytes == 0 { usize::MAX } else { megabytes.saturating_mul(1 << 20) });
            }
            "-h" | "--help" => usage(),
            // (what follows is the workspace's own, in `.z.x`)
            "--" => break,
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

    let mut ipc_cfg = match auth {
        Some(spec) => match spec.split_once(':') {
            Some((u, p)) => IpcConfig::with_single_user(u, p),
            None => IpcConfig::with_single_user(spec, ""),
        },
        None => IpcConfig::default(),
    };

    ipc_cfg.queue_limit = queue_limit;

    let pem = match (cert, key) {
        (Some(c), Some(k)) => Some((read_pem(&c), read_pem(&k))),
        (None, None) if self_signed => {
            let (c, k) = self_signed_cert(&bind).unwrap_or_else(|e| fail(&e.to_string()));
            let path = std::path::Path::new(dir.as_deref().unwrap_or(".")).join("kix-cert.pem");
            if let Err(e) = std::fs::write(&path, &c) {
                fail(&format!("cannot write {}: {e}", path.display()));
            }
            eprintln!("self-signed certificate written to {}", path.display());
            Some((c.into_bytes(), k.into_bytes()))
        }
        (None, None) => None,
        _ => usage(),
    };
    if let Some((c, k)) = pem {
        ipc_cfg.tls = Some(tls_server_config(&c, &k).unwrap_or_else(|e| fail(&e.to_string())));
        ipc_cfg.force_tls = force_tls;
    } else if force_tls {
        fail("--force-tls requires --tls or --tls-cert/--tls-key");
    }

    match ipc_cfg.check_exposure(&bind) {
        Err(why) if insecure_clients => eprintln!("kix-serve: WARNING: {why}; continuing (--insecure-clients)"),
        Err(why) => fail(&format!("{why}, or --insecure-clients to listen unprotected")),
        Ok(()) => {}
    }

    let _timer = server.start_timer();
    let listener = match Listener::bind(server.clone(), (bind.as_str(), port), ipc_cfg) {
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
