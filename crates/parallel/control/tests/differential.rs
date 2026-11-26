//! Generated statements, one engine versus cluster: every combination of source, column list, grouping and filter, then a
//! long run of random writes. The generator is seeded, so a failure repeats.
mod common;
use common::{market, same, Pair};

const SOURCES: [&str; 4] = ["trade", "trade lj inst", "trade lj acct", "aj[`sym`time; trade; quote]"];

const COLUMNS: [&str; 22] = [
    "",
    "sym, price",
    "price, v: size * price",
    "m: neg price, b: price > 110, s: string sym",
    "t: 5 xbar time.minute, size",
    "sum size",
    "a: avg size, n: count i",
    "mn: min price, mx: max time",
    "v: var price, d: dev size",
    "sv: svar price, sd: sdev price",
    "w: size wavg price, t: size wsum price",
    "r: (sum size) % count i, q: sqrt sum price * price",
    "hi: max sym, lo: min ex",
    "p: prd 1 + size mod 2",
    "first price, last size",
    "med price",
    "d: deltas price",
    "m: 3 mavg price",
    "distinct sym",
    "price - avg price",
    "x: 1, price",
    "x: 1",
];

const GROUPS: [&str; 6] = ["", " by sym", " by ex", " by sym, ex", " by 5 xbar time.minute", " by trader, up: price > 112"];

const FILTERS: [&str; 14] = [
    "",
    " where sym = `AAPL",
    " where sym in `MSFT`GOOG",
    " where sym = `NOSUCH",
    " where size > 100",
    " where size > 100000",
    " where price > 105, price < 120, ex <> `Q",
    " where ex = `N, sym = `IBM",
    " where price > avg price",
    " where sym = `AMD, price > avg price",
    " where size = (max; size) fby sym",
    " where time within 09:31:00.000 09:33:00.000",
    " where sym in exec sym from inst where lot > 20",
    " where (size > 150) or sym like \"A*\"",
];

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % n as u64) as usize
    }
    fn pick<'a>(&mut self, from: &[&'a str]) -> &'a str {
        from[self.below(from.len())]
    }
}

fn query(rng: &mut Rng) -> String {
    let verb = rng.pick(&["select", "select", "select", "exec", "select distinct", "select[7]", "select[-4]"]);
    let (cols, by, from, filter) = (rng.pick(&COLUMNS), rng.pick(&GROUPS), rng.pick(&SOURCES), rng.pick(&FILTERS));
    format!("{verb} {cols}{by} from {from}{filter}")
}

fn queries(p: &mut Pair, seed: u64, count: usize) {
    let mut rng = Rng(seed);
    for _ in 0..count {
        p.check(&query(&mut rng));
    }
}

#[test]
fn generated_queries_agree() {
    for shards in [1, 3, 5] {
        queries(&mut market(shards), 0x9e37 + shards as u64, 700);
    }
}

#[test]
fn generated_queries_agree_when_tables_must_move() {
    let mut p = market(4);
    p.cluster.set_broadcast_limit(0);
    p.check(".kix.dist[`quote; `partitioned; `time]");
    queries(&mut p, 77, 500);
    p.check(".kix.dist[`trade; `partitioned; `time]");
    queries(&mut p, 78, 500);
    p.cluster.set_broadcast_limit(1_000_000);
    p.check(".kix.dist[`trade; `sharded; `trader]");
    queries(&mut p, 79, 500);
}

#[test]
fn generated_queries_agree_with_nulls_empty_tables_and_keyed_tables() {
    let mut p = market(3);
    for s in [
        "update price: 0n from `trade where 0 = size mod 7",
        "update size: 0N from `trade where 0 = size mod 11",
        "update ex: ` from `trade where 0 = size mod 13",
        "delete from `trade where sym = `IBM",
    ] {
        p.check(s);
    }
    queries(&mut p, 4242, 700);
    // a keyed table spread over the shards
    p.check("trade: `time`sym xkey trade");
    queries(&mut p, 4243, 400);
    p.check("trade: 0#0!trade");
    queries(&mut p, 4244, 400);
    p.check("`trade insert (10:00:00.000; `AAPL; 1.5; 3; `N; `ann)");
    queries(&mut p, 4245, 400);
}

fn write(rng: &mut Rng, step: usize) -> String {
    let sym = rng.pick(&["AAPL", "MSFT", "GOOG", "IBM", "NVDA", "TSLA"]);
    let trader = rng.pick(&["ann", "bob", "cat", "eve"]);
    let n = rng.below(200);
    match rng.below(14) {
        0 => format!("`trade insert (10:00:00.000 + {step}; `{sym}; {n}.5; {n}; `N; `{trader})"),
        1 => format!("`trade insert (2#10:00:00.000 + {step}; `{sym}`ORCL; 1.5 2.5; {n} 7; `Q`B; `ann`bob)"),
        2 => format!("update size: size + {n} from `trade where sym = `{sym}"),
        3 => format!("update price: price + 0.5 from `trade where size > {n}"),
        4 => "update c: sums size by sym from `trade".to_string(),
        5 => format!("delete from `trade where size = {n}"),
        6 => format!("delete from `trade where sym = `{sym}, size < {n}"),
        7 => format!("`acct upsert (`{trader}; `d{})", n % 4),
        8 => format!("`acct insert (`{trader}; `d{})", n % 4),
        9 => format!("update desk: `d{} from `acct where trader = `{trader}", n % 4),
        10 => format!("delete from `acct where trader = `{trader}"),
        11 => "update price: price % 2 from `trade where price > (avg; price) fby sym".to_string(),
        12 => format!("update size: max size from `trade where ex = `Q, sym = `{sym}"),
        _ => format!("trade: select from trade where size <> {n}"),
    }
}

#[test]
fn generated_statements_agree_as_shards_come_and_go() {
    let mut p = market(2);
    p.cluster.set_move_batch(13);
    let mut rng = Rng(77);
    let added: Vec<String> = (2..5).map(|id| p.shards.add(id)).collect();
    // up to five shards and down to one
    let steps: [Option<usize>; 7] = [Some(0), Some(1), None, Some(1), Some(2), None, None];
    for (turn, step) in steps.into_iter().chain([None, None]).enumerate() {
        match step {
            Some(address) => p.cluster.add_shard(&added[address]).unwrap(),
            None => p.cluster.remove_shard().unwrap(),
        }
        for write_step in 0..25 {
            p.check(&write(&mut rng, write_step));
        }
        for table in ["trade", "quote", "acct", "inst"] {
            let (a, b) = (p.single.eval(table).unwrap(), p.session.eval(table).unwrap());
            assert!(same(&a, &b), "after change {turn}, {table} differs
{a}
{b}");
        }
        queries(&mut p, 900 + turn as u64, 60);
    }
    assert_eq!(p.cluster.shard_count(), 1);
}

#[test]
fn generated_writes_leave_the_same_tables() {
    for shards in [2, 4] {
        let mut p = market(shards);
        let mut rng = Rng(1000 + shards as u64);
        for step in 0..600 {
            let statement = write(&mut rng, step);
            p.check(&statement);
            if step % 10 == 9 {
                for table in ["trade", "acct"] {
                    let (a, b) = (p.single.eval(table).unwrap(), p.session.eval(table).unwrap());
                    assert!(same(&a, &b), "after step {step} ({statement}) {table} differs\n{a}\n{b}");
                }
                queries(&mut p, step as u64 + 5, 3);
            }
        }
    }
}
