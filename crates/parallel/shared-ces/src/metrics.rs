//! Resource & workspace metrics of a shard or control node, with Prometheus text exposition.
use serde::{Deserialize, Serialize};
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

impl NodeMetrics {
    pub fn sample_host() -> (f32, u64) {
        let mut sys = System::new();
        sys.refresh_cpu_usage();
        let pid = sysinfo::get_current_pid().ok();
        let rss = if let Some(p) = pid {
            sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[p]), true);
            sys.process(p).map(|pr| pr.memory()).unwrap_or(0)
        } else {
            0
        };
        let cpu = sys.global_cpu_usage();
        (cpu, rss)
    }

    pub fn to_prometheus(&self) -> String {
        format!(
            "# TYPE kixdb_workspace_bytes gauge\n\
             kixdb_workspace_bytes{{shard=\"{0}\"}} {1}\n\
             # TYPE kixdb_rss_bytes gauge\n\
             kixdb_rss_bytes{{shard=\"{0}\"}} {2}\n\
             # TYPE kixdb_cpu_percent gauge\n\
             kixdb_cpu_percent{{shard=\"{0}\"}} {3:.2}\n\
             # TYPE kixdb_statements_total counter\n\
             kixdb_statements_total{{shard=\"{0}\"}} {4}\n\
             # TYPE kixdb_in_flight gauge\n\
             kixdb_in_flight{{shard=\"{0}\"}} {5}\n\
             # TYPE kixdb_uptime_seconds gauge\n\
             kixdb_uptime_seconds{{shard=\"{0}\"}} {6}\n",
            self.shard_id,
            self.workspace_bytes,
            self.rss_bytes,
            self.cpu_percent,
            self.statements,
            self.in_flight,
            self.uptime_secs
        )
    }
}
