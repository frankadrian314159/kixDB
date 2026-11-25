//! TPC: the decision-support benchmarks, TPC-H and TPC-DS, run against `kixdb` in `q` / `qSQL`.
//!
//! `cargo run --release --example tpc [h|ds] [--sf 0.05] [--durable] [--plans] [--query Q5] [--q "..."]`
//!
//! Each benchmark loads generated columnar data, runs every query once (timing it), and checks what it can: queries
//! that fail are reported, and selected answers are recomputed in Rust from the generated rows. The exit status is
//! non-zero when any query errors or any check fails.
mod ds;
mod h;
mod util;

use kixdb::{Connection, Server};
use std::time::Instant;
use util::*;

fn run_adhoc(c: &mut Connection, adhoc: &[String]) {
    for q in adhoc {
        let (res, dt) = timed(c, q);
        println!("{:.3}s {}", dt, if res.is_ok() { "ok" } else { "ERROR" });
        match &res {
            Ok(rows) => {
                println!("   {} row(s)", rows.len());
                for r in rows.iter().take(12) {
                    println!("   | {}", r.join(" | "));
                }
            }
            Err(e) => println!("   {e}"),
        }
        if let Ok(lines) = c.explain(q) {
            for l in lines {
                println!("      plan: {l}");
            }
        }
    }
}

fn matches_query(name: &str, wanted: &Option<String>) -> bool {
    match wanted {
        None => true,
        Some(only) => name.split_whitespace().next().is_some_and(|n| n.eq_ignore_ascii_case(only.trim())),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let flag = |name: &str| args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).and_then(|v| v.parse::<f64>().ok());
    let flag_s = |name: &str| args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned();
    let sf = flag("--sf").unwrap_or(0.05);
    let durable = args.iter().any(|a| a == "--durable");
    let show_plans = args.iter().any(|a| a == "--plans");
    let only_q = flag_s("--query");
    let only_h = args.iter().any(|a| a == "h");
    let only_ds = args.iter().any(|a| a == "ds");
    let adhoc: Vec<String> = args
        .iter()
        .enumerate()
        .filter(|(_, a)| *a == "--q" || *a == "--sql")
        .filter_map(|(i, _)| args.get(i + 1).cloned())
        .collect();
    let dir = tempfile::tempdir().unwrap();
    let mut failures = 0;
    let mut total = 0.0;

    if !only_ds {
        println!("== TPC-H (scale factor {sf}) ==");
        let server = if durable { Server::open(dir.path().join("h")).unwrap() } else { Server::new() };
        let mut c = server.connect();
        let t = Instant::now();
        let data = h::load(&mut c, sf);
        println!("loaded in {:.2}s", t.elapsed().as_secs_f64());
        if !adhoc.is_empty() {
            run_adhoc(&mut c, &adhoc);
            return;
        }
        let mut report = Report::default();
        for (name, q) in h::queries(sf) {
            if !matches_query(&name, &only_q) {
                continue;
            }
            let (res, dt) = timed(&mut c, &q);
            if dt > 0.5 || show_plans {
                if let Ok(lines) = c.explain(&q) {
                    for l in lines {
                        println!("      plan: {l}");
                    }
                }
            }
            match res {
                Err(e) => {
                    failures += 1;
                    report.add(&name, dt, 0, &format!("ERROR: {e}"));
                }
                Ok(rows) => match h::check(&data, &name, &rows) {
                    Some(Err(why)) => {
                        failures += 1;
                        report.add(&name, dt, rows.len(), &format!("WRONG: {why}"));
                    }
                    Some(Ok(())) => report.add(&name, dt, rows.len(), "verified"),
                    None => report.add(&name, dt, rows.len(), ""),
                },
            }
        }
        println!("TPC-H total {:.2}s", report.total());
        total += report.total();
    }

    if !only_h {
        println!("== TPC-DS (scale {sf}) ==");
        let server = if durable { Server::open(dir.path().join("ds")).unwrap() } else { Server::new() };
        let mut c = server.connect();
        let t = Instant::now();
        let data = ds::load(&mut c, sf);
        println!("loaded in {:.2}s", t.elapsed().as_secs_f64());
        if !adhoc.is_empty() {
            run_adhoc(&mut c, &adhoc);
            return;
        }
        let mut report = Report::default();
        for (name, q) in ds::queries() {
            if !matches_query(&name, &only_q) {
                continue;
            }
            let (res, dt) = timed(&mut c, &q);
            if dt > 0.5 || show_plans {
                if let Ok(lines) = c.explain(&q) {
                    for l in lines {
                        println!("      plan: {l}");
                    }
                }
            }
            match res {
                Err(e) => {
                    failures += 1;
                    report.add(&name, dt, 0, &format!("ERROR: {e}"));
                }
                Ok(rows) => match ds::check(&data, &name, &rows) {
                    Some(Err(why)) => {
                        failures += 1;
                        report.add(&name, dt, rows.len(), &format!("WRONG: {why}"));
                    }
                    Some(Ok(())) => report.add(&name, dt, rows.len(), "verified"),
                    None => report.add(&name, dt, rows.len(), ""),
                },
            }
        }
        println!("TPC-DS total {:.2}s", report.total());
        total += report.total();
    }

    println!("total {total:.2}s, {failures} problem(s)");
    if failures > 0 {
        std::process::exit(1);
    }
}

/// mimalloc runs the engine markedly faster than the system allocator.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
