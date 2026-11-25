//! The `kix-cluster` binary, given a cluster file and `q` on standard input.
mod common;
use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn kix_cluster_runs_a_script_and_keeps_its_state() {
    let shards = common::Shards::start(2);
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("cluster.json");
    let state = dir.path().join("cluster.state");
    let list: Vec<String> = shards.config().shards.iter().map(|s| format!("{{\"id\":{},\"addr\":\"{}\"}}", s.id, s.addr)).collect();
    std::fs::write(&config, format!("{{\"shards\":[{}]}}", list.join(","))).unwrap();
    let run = |with_config: bool, script: &str| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_kix-cluster"));
        if with_config {
            cmd.arg("--config").arg(&config);
        }
        cmd.arg("--state").arg(&state).arg("--plan").stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = cmd.spawn().unwrap();
        child.stdin.take().unwrap().write_all(script.as_bytes()).unwrap();
        child.wait_with_output().unwrap()
    };
    let first = run(true, "t: ([] sym: `a`b`c;\n  v: 1 2 3)\nDISTRIBUTE t BY sym\nexec sum v from t\n");
    assert!(first.status.success(), "{}", String::from_utf8_lossy(&first.stderr));
    assert_eq!(String::from_utf8_lossy(&first.stdout).trim().lines().last(), Some("6"));
    assert!(String::from_utf8_lossy(&first.stderr).contains("plan: scatter-gather: two-phase aggregation"));
    // the state file alone is enough the second time
    let second = run(false, "count t\nnosuch\n");
    assert!(!second.status.success());
    assert_eq!(String::from_utf8_lossy(&second.stdout).trim(), "3");
    assert!(String::from_utf8_lossy(&second.stderr).contains("'nosuch"));
}
