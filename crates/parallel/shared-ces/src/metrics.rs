//! Resource & workspace metrics of a shard or control node, with Prometheus text exposition and the HTTP endpoint.
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::time::Duration;
use sysinfo::System;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NodeMetrics {
    pub shard_id: u32,
    pub cpu_percent: f32,
    pub rss_bytes: u64,
    pub workspace_bytes: usize,
    pub sym_count: usize,
    pub table_count: usize,
    pub statements: u64,
    pub in_flight: u64,
    pub uptime_secs: u64,
    pub draining: bool,
}

type Gauge = (&'static str, &'static str, fn(&NodeMetrics) -> f64);

const SERIES: [Gauge; 9] = [
    ("kixdb_workspace_bytes", "gauge", |m| m.workspace_bytes as f64),
    ("kixdb_rss_bytes", "gauge", |m| m.rss_bytes as f64),
    ("kixdb_cpu_percent", "gauge", |m| m.cpu_percent as f64),
    ("kixdb_symbols", "gauge", |m| m.sym_count as f64),
    ("kixdb_tables", "gauge", |m| m.table_count as f64),
    ("kixdb_statements_total", "counter", |m| m.statements as f64),
    ("kixdb_in_flight", "gauge", |m| m.in_flight as f64),
    ("kixdb_uptime_seconds", "gauge", |m| m.uptime_secs as f64),
    ("kixdb_draining", "gauge", |m| u8::from(m.draining) as f64),
];

impl NodeMetrics {
    /// CPU use of the machine and resident memory of this process.
    pub fn sample_host() -> (f32, u64) {
        let mut sys = System::new();
        sys.refresh_cpu();
        let rss = match sysinfo::get_current_pid() {
            Ok(pid) => {
                sys.refresh_process(pid);
                sys.process(pid).map_or(0, |p| p.memory())
            }
            Err(_) => 0,
        };
        // CPU use is a difference between two samples
        std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        sys.refresh_cpu();
        (sys.global_cpu_info().cpu_usage(), rss)
    }
}

/// Prometheus text exposition of several nodes' metrics, labelled by shard.
pub fn render_prometheus(nodes: &[NodeMetrics]) -> String {
    let mut out = String::new();
    for (name, kind, value) in SERIES {
        out.push_str(&format!("# TYPE {name} {kind}\n"));
        for m in nodes {
            out.push_str(&format!("{name}{{shard=\"{}\"}} {}\n", m.shard_id, value(m)));
        }
    }
    out
}

/// Serve `/metrics` (what `render` returns) and `/health` on a background thread; returns the bound address.
pub fn serve_http(addr: &str, render: impl Fn() -> String + Send + 'static) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { continue };
            let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
            let mut buf = [0u8; 1024];
            let n = s.read(&mut buf).unwrap_or(0);
            let request = String::from_utf8_lossy(&buf[..n]);
            let (status, body) = match request.split_whitespace().nth(1) {
                Some("/metrics") => ("200 OK", render()),
                Some("/health") => ("200 OK", "ok\n".to_string()),
                _ => ("404 Not Found", String::new()),
            };
            let head = format!(
                "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = s.write_all(head.as_bytes()).and_then(|_| s.write_all(body.as_bytes()));
        }
    });
    Ok(local)
}
