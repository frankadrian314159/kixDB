//! The `kix-cluster` binary, given a cluster file and `q` on standard input.
mod common;
use kix_shared_ces::tls::NodeTls;
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
        cmd.arg("--state").arg(&state).args(["--plan", "--insecure"]);
        cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
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

#[test]
fn kix_cluster_makes_certificates_and_insists_on_them() {
    let dir = tempfile::tempdir().unwrap();
    let run = |args: &[&str], script: &str| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_kix-cluster"));
        cmd.current_dir(dir.path()).args(args).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = cmd.spawn().unwrap();
        // a program that refuses to start may have closed its input already
        let _ = child.stdin.take().unwrap().write_all(script.as_bytes());
        let out = child.wait_with_output().unwrap();
        let text = |b: &[u8]| String::from_utf8_lossy(b).into_owned();
        (out.status.success(), text(&out.stdout), text(&out.stderr))
    };
    let (made, _, why) = run(&["--make-certs", "certs", "control", "shard"], "");
    assert!(made, "{why}");
    let load = |node: &str| {
        let certs = dir.path().join("certs");
        NodeTls::from_files(certs.join("ca.pem"), certs.join(format!("{node}.pem")), certs.join(format!("{node}.key"))).unwrap()
    };
    let shards = common::Shards::start_with(1, Some(load("shard")));
    std::fs::write(dir.path().join("cluster.json"), format!("{{\"shards\":[{{\"id\":0,\"addr\":\"{}\"}}]}}", shards.nodes[0].addr)).unwrap();

    let secured = ["--config", "cluster.json", "--node-ca", "certs/ca.pem", "--node-cert", "certs/control.pem", "--node-key", "certs/control.key"];
    let (ok, out, why) = run(&secured, "t: ([] a: 1 2 3)
exec sum a from t
");
    assert!(ok, "{why}");
    assert_eq!(out.trim().lines().last(), Some("6"));
    assert!(!why.contains("WARNING"), "{why}");

    let (ok, _, why) = run(&["--config", "cluster.json"], "");
    assert!(!ok && why.contains("--insecure"), "{why}");
    let (ok, _, why) = run(&["--config", "cluster.json", "--insecure", "--wait-shards", "1"], "");
    assert!(!ok && why.contains("WARNING") && why.contains("do not answer"), "{why}");
}
