//! Helpers shared by the TPC benchmarks: a seeded random source, columnar bulk loading and result reporting.
#![allow(dead_code)]
use chrono::{Datelike, NaiveDate};
use kixdb::sym::intern;
use kixdb::types::date_from_ymd;
use kixdb::{Attr, BatchResult, ColVec, Connection, Event, Flip, K};
use rand::{rngs::StdRng, Rng as _, SeedableRng};
use std::sync::Arc;
use std::time::Instant;

/// Where a benchmark's `q` statements go: the in-process engine (`Connection`), or a cluster session.
pub trait QDb {
    /// Run a `q` script: the rows of its last result set (as text; nulls as `NULL`), or its first error.
    fn exec(&mut self, q: &str) -> Result<Out, String>;
    /// Bulk-load a table in one columnar batch via `upsert`.
    fn bulk_load(&mut self, table: &str, rows: K) -> Result<(), String>;
}

#[derive(Default, Clone, Debug)]
pub struct Out {
    pub rows: Vec<Vec<String>>,
}

pub fn k_str(v: &K) -> String {
    if v.is_null() {
        return "NULL".to_string();
    }
    match v {
        K::Float(f) | K::Datetime(f) => if *f == 0.0 { "0".to_string() } else { f.to_string() },
        K::Real(f) => if *f == 0.0 { "0".to_string() } else { f.to_string() },
        _ => v.cell_text(),
    }
}

pub fn out_of(r: &BatchResult) -> Result<Out, String> {
    if let Some(e) = r.first_error() {
        return Err(e.to_string());
    }
    let rows = match r.events.last() {
        Some(Event::Rows(rs)) => rs.rows.iter().map(|row| row.iter().map(k_str).collect()).collect(),
        Some(Event::Value(v)) => vec![vec![k_str(v)]],
        _ => r
            .resultsets()
            .last()
            .map(|rs| rs.rows.iter().map(|row| row.iter().map(k_str).collect()).collect())
            .unwrap_or_default(),
    };
    Ok(Out { rows })
}

impl QDb for Connection {
    fn exec(&mut self, q: &str) -> Result<Out, String> {
        out_of(&self.execute(q))
    }

    fn bulk_load(&mut self, table: &str, rows: K) -> Result<(), String> {
        let msg = K::Vector(ColVec::General(Attr::None, Arc::new(vec![K::Sym(intern("upsert")), K::Sym(intern(table)), rows])));
        self.eval_ipc_message(&msg).map(|_| ()).map_err(|e| e.to_string())
    }
}

pub fn longs(v: Vec<i64>) -> ColVec {
    ColVec::Long(Attr::None, Arc::new(v))
}

pub fn floats(v: Vec<f64>) -> ColVec {
    ColVec::Float(Attr::None, Arc::new(v))
}

pub fn dates(v: Vec<i32>) -> ColVec {
    ColVec::Date(Attr::None, Arc::new(v))
}

pub fn chars(v: Vec<u8>) -> ColVec {
    ColVec::Char(Attr::None, Arc::new(v))
}

pub fn syms<S: AsRef<str>>(v: Vec<S>) -> ColVec {
    ColVec::Sym(Attr::None, Arc::new(v.into_iter().map(|s| intern(s.as_ref())).collect()))
}

pub fn strings(v: Vec<String>) -> ColVec {
    ColVec::General(Attr::None, Arc::new(v.into_iter().map(|s| K::string(&s)).collect()))
}

pub fn k_date(d: NaiveDate) -> i32 {
    date_from_ymd(d.year(), d.month(), d.day()).unwrap()
}

pub fn load_cols<D: QDb + ?Sized>(c: &mut D, table: &str, cols: Vec<(&str, ColVec)>) {
    let (names, vals): (Vec<_>, Vec<_>) = cols.into_iter().map(|(n, v)| (intern(n), v)).unzip();
    let flip = Flip::new(names, vals).unwrap_or_else(|e| panic!("{table}: {e}"));
    if let Err(e) = c.bulk_load(table, K::Table(Arc::new(flip))) {
        panic!("bulk_load {table}: {e}");
    }
}

pub struct Rng(StdRng);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(StdRng::seed_from_u64(seed))
    }
    /// Uniform in `lo..=hi`.
    pub fn int(&mut self, lo: i64, hi: i64) -> i64 {
        self.0.gen_range(lo..=hi)
    }
    pub fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.0.gen_range(0..xs.len())]
    }
    pub fn chance(&mut self, p: f64) -> bool {
        self.0.gen_bool(p)
    }
    pub fn float(&mut self) -> f64 {
        self.0.gen::<f64>()
    }
    /// Words joined by spaces, from a small vocabulary.
    pub fn text(&mut self, words: usize) -> String {
        const W: &[&str] = &[
            "the", "quick", "furiously", "carefully", "regular", "final", "pending", "ironic", "bold", "express",
            "deposits", "accounts", "packages", "theodolites", "foxes", "pinto", "beans", "requests", "platelets",
            "instructions", "dependencies", "excuses",
        ];
        (0..words).map(|_| *self.pick(W)).collect::<Vec<_>>().join(" ")
    }
    pub fn digits(&mut self, n: usize) -> String {
        (0..n).map(|_| char::from(b'0' + self.0.gen_range(0..10u8))).collect()
    }
}

/// Run a script, panicking with the head of the script on error.
pub fn must<D: QDb + ?Sized>(c: &mut D, q: &str) {
    if let Err(e) = c.exec(q) {
        let head: String = q.chars().take(200).collect();
        panic!("{head}\n=> {e}");
    }
}

/// One line of a report.
pub struct Line {
    pub name: String,
    pub secs: f64,
    pub rows: usize,
    pub note: String,
}

#[derive(Default)]
pub struct Report {
    pub lines: Vec<Line>,
}

impl Report {
    pub fn add(&mut self, name: &str, secs: f64, rows: usize, note: &str) {
        println!("{:<34} {:>9.3}s {:>9} rows  {}", name, secs, rows, note);
        self.lines.push(Line { name: name.to_string(), secs, rows, note: note.to_string() });
    }
    pub fn total(&self) -> f64 {
        self.lines.iter().map(|l| l.secs).sum()
    }
}

/// Run a query, returning its rows as strings (or the error text) and the time it took.
pub fn timed<D: QDb + ?Sized>(c: &mut D, q: &str) -> (Result<Vec<Vec<String>>, String>, f64) {
    let t = Instant::now();
    let r = c.exec(q);
    let dt = t.elapsed().as_secs_f64();
    (r.map(|o| o.rows), dt)
}

pub fn num(s: &str) -> f64 {
    s.parse::<f64>().unwrap_or(f64::NAN)
}

/// Are two numbers equal to within rounding?
pub fn close(a: f64, b: f64) -> bool {
    (a - b).abs() <= 0.011 + 1e-9 * a.abs().max(b.abs())
}

/// Compare two result sets ignoring row order (ties under `xasc`/`xdesc` may come out in either order); numbers are
/// compared to within rounding. Returns a description of the first difference.
pub fn compare_rows(a: &[Vec<String>], b: &[Vec<String>]) -> Option<String> {
    if a.len() != b.len() {
        return Some(format!("{} rows against {}", a.len(), b.len()));
    }
    let key = |r: &Vec<String>| r.join("\u{1}");
    let (mut a, mut b) = (a.to_vec(), b.to_vec());
    a.sort_by_key(key);
    b.sort_by_key(key);
    for (x, y) in a.iter().zip(b.iter()) {
        if x.len() != y.len() {
            return Some(format!("row widths {} and {}", x.len(), y.len()));
        }
        for (u, v) in x.iter().zip(y.iter()) {
            let same = u == v || (u.parse::<f64>().is_ok() && v.parse::<f64>().is_ok() && close(num(u), num(v)));
            if !same {
                return Some(format!("{:?} against {:?}", x, y));
            }
        }
    }
    None
}
