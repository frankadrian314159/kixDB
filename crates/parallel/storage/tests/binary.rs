//! The `kix-shard` binary refuses to start unprotected unless told to, or to join a group it cannot answer.
use std::process::Command;

#[test]
fn kix_shard_insists_on_certificates() {
    let refused = |flags: &[&str], why: &str| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_kix-shard"));
        let out = cmd.args(["--id", "0", "--listen", "127.0.0.1:0"]).args(flags).output().unwrap();
        let said = String::from_utf8_lossy(&out.stderr);
        assert!(!out.status.success() && said.contains(why), "{flags:?}: {said}");
    };
    refused(&[], "--insecure");
    // a replica that joins must know the others
    refused(&["--join", "--insecure"], "--join needs --replica and --peers");
    refused(&["--join", "--insecure", "--data", "d", "--replica", "4"], "--join needs --replica and --peers");
    refused(&["--join", "--insecure", "--peers", "1=127.0.0.1:1"], "--join needs --replica and --peers");
    refused(&["--node-ca", "ca.pem"], "go together");
    refused(&["--insecure", "--node-key", "node.key"], "cannot be combined");
    refused(&["--node-ca", "none.pem", "--node-cert", "none.pem", "--node-key", "none.key"], "cannot read none.pem");
}
