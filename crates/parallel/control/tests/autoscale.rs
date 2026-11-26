//! Shards added without being asked, by a program that names them.
mod common;
use common::{load_market, Pair};
use kix_control::{Autoscale, Cluster};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// A program that notes the id it is given in `asked`, then runs `then`.
fn program(dir: &Path, then: &str) -> PathBuf {
    let asked = dir.join("asked");
    let (name, script) = match cfg!(windows) {
        true => ("start.cmd", format!("@echo off\r\n>>\"{}\" echo %1\r\n{then}\r\n", asked.display())),
        false => ("start.sh", format!("#!/bin/sh\necho $1 >> '{}'\n{then}\n", asked.display())),
    };
    let path = dir.join(name);
    std::fs::write(&path, script).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    path
}

/// The ids the program was run with.
fn asked(dir: &Path) -> Vec<String> {
    std::fs::read_to_string(dir.join("asked")).unwrap_or_default().split_whitespace().map(String::from).collect()
}

/// Checks that come quickly, one after another with no wait between attempts.
fn eager(command: PathBuf, above_bytes: usize, max_shards: usize) -> Autoscale {
    let mut policy = Autoscale::new(command, above_bytes, max_shards);
    (policy.interval, policy.cooldown, policy.checks) = (Duration::from_millis(20), Duration::ZERO, 2);
    policy.start_limit = Duration::from_secs(10);
    policy
}

fn metric(cluster: &Cluster, name: &str) -> u64 {
    let text = cluster.prometheus();
    let line = text.lines().find(|l| l.starts_with(&format!("kixdb_autoscale_{name} "))).unwrap_or_else(|| panic!("{text}"));
    line.rsplit(' ').next().unwrap().parse().unwrap()
}

fn comes_true(what: &str, mut check: impl FnMut() -> bool) {
    let started = Instant::now();
    while !check() {
        assert!(started.elapsed() < Duration::from_secs(30), "{what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// A moment in which the shards are checked many times.
fn a_while() {
    std::thread::sleep(Duration::from_millis(600));
}

#[test]
fn shards_are_added_while_one_holds_too_much() {
    let mut p = load_market(Pair::new(2));
    let dir = tempfile::tempdir().unwrap();
    let third = p.shards.add(2);
    let command = program(dir.path(), &format!("echo starting shard\necho {third}"));
    // nothing is above a mark this high
    let scaler = p.cluster.autoscale(eager(command.clone(), 1 << 30, 3));
    a_while();
    drop(scaler);
    assert_eq!((p.cluster.shard_count(), asked(dir.path()).len()), (2, 0));
    // a dry run says what it would do
    let mut policy = eager(command.clone(), 1, 3);
    policy.dry_run = true;
    let scaler = p.cluster.autoscale(policy);
    a_while();
    drop(scaler);
    assert_eq!((p.cluster.shard_count(), asked(dir.path()).len()), (2, 0));
    // above the mark: one shard is added, and no more than are allowed
    let _scaler = p.cluster.autoscale(eager(command, 1, 3));
    comes_true("a third shard", || p.cluster.shard_count() == 3);
    a_while();
    assert_eq!(asked(dir.path()), ["2"]);
    assert_eq!((metric(&p.cluster, "shards_added_total"), metric(&p.cluster, "failures_total")), (1, 0));
    for query in ["select from trade", "select sum size by sym from trade", "aj[`sym`time; trade; quote]"] {
        p.check(query);
    }
    assert!(p.shards.rows("trade")[2] > 0);
}

#[test]
fn a_shard_that_was_started_is_tried_again_before_another_is() {
    let mut p = load_market(Pair::new(1));
    let dir = tempfile::tempdir().unwrap();
    // a program that fails starts nothing
    let scaler = p.cluster.autoscale(eager(program(dir.path(), "exit 1"), 1, 2));
    comes_true("two failures", || metric(&p.cluster, "failures_total") >= 2);
    drop(scaler);
    assert_eq!(p.cluster.shard_count(), 1);
    std::fs::remove_file(dir.path().join("asked")).unwrap();
    // one that names a shard which does not answer is run once; the address is then tried again
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    let mut policy = eager(program(dir.path(), &format!("echo {silent}")), 1, 2);
    policy.start_limit = Duration::from_secs(1);
    let scaler = p.cluster.autoscale(policy);
    let failed = metric(&p.cluster, "failures_total");
    comes_true("two more failures", || metric(&p.cluster, "failures_total") >= failed + 2);
    drop(scaler);
    assert_eq!(asked(dir.path()), ["1"]);
    assert_eq!(p.cluster.shard_count(), 1);
    p.check("select from trade");
}

#[test]
fn adding_stops_when_it_frees_nothing() {
    let mut p = Pair::new(1);
    // every row has one symbol, so no number of shards splits them
    let statement = "big: ([] sym: 20000#`only; v: til 20000)";
    p.single.eval(statement).unwrap();
    p.session.eval(statement).unwrap();
    let dir = tempfile::tempdir().unwrap();
    // two shards, of which the one that `only` goes to may be either
    let added: Vec<String> = (1..5).map(|id| p.shards.add(id)).collect();
    let script = match cfg!(windows) {
        true => (1..5).map(|id| format!("if \"%1\"==\"{id}\" echo {}", added[id - 1])).collect::<Vec<_>>().join("\r\n"),
        false => format!("case $1 in {} esac", (1..5).map(|id| format!("{id}) echo {};;", added[id - 1])).collect::<Vec<_>>().join(" ")),
    };
    let _scaler = p.cluster.autoscale(eager(program(dir.path(), &script), 1000, 5));
    comes_true("a pause", || metric(&p.cluster, "paused") == 1);
    let shards = p.cluster.shard_count();
    a_while();
    // (a first shard may take the symbol's slot and so seem to help; the next cannot)
    assert!(shards < 5 && p.cluster.shard_count() == shards, "{shards} then {}", p.cluster.shard_count());
    p.check("select sum v from big");
}
