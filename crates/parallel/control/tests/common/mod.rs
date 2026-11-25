//! A cluster of in-process shard nodes, and comparison of its answers with one engine's.
#![allow(dead_code)]
use kix_control::{Cluster, ClusterSession};
use kix_shared_ces::tls::{make_certs, NodeTls};
use kix_shared_ces::{ClusterConfig, ShardInfo};
use kix_storage::{spawn, RunningShard, ShardNode};
use kixdb::{ColVec, Connection, Server, K};

pub struct Shards {
    pub runtime: tokio::runtime::Runtime,
    pub nodes: Vec<RunningShard>,
}

impl Shards {
    pub fn start(n: u32) -> Shards {
        Shards::start_with(n, None)
    }

    /// Shards that require mutual TLS when `tls` holds certificates.
    pub fn start_with(n: u32, tls: Option<NodeTls>) -> Shards {
        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
        let nodes = (0..n)
            .map(|id| {
                let node = ShardNode::new(id, Server::new()).with_tls(tls.clone());
                runtime.block_on(spawn(node, "127.0.0.1:0".parse().unwrap())).unwrap()
            })
            .collect();
        Shards { runtime, nodes }
    }

    pub fn config(&self) -> ClusterConfig {
        let shards = self.nodes.iter().enumerate().map(|(i, n)| ShardInfo { id: i as u32, addr: n.addr.to_string() }).collect();
        ClusterConfig { shards }
    }

    /// The number of rows of `table` on each shard.
    pub fn rows(&self, table: &str) -> Vec<usize> {
        self.nodes.iter().map(|n| n.server.connect().eval(&format!("count {table}")).map_or(0, |k| k.as_i64().unwrap() as usize)).collect()
    }
}

/// Equal values of equal types; floating point numbers may differ in the last digits (sums are added in another order).
pub fn same(a: &K, b: &K) -> bool {
    let close = |x: f64, y: f64| (x.is_nan() && y.is_nan()) || x == y || (x - y).abs() <= 1e-9 * x.abs().max(y.abs()).max(1.0);
    let vecs = |x: &ColVec, y: &ColVec| {
        x.type_num() == y.type_num() && x.len() == y.len() && (0..x.len()).all(|i| same(&x.get_atom(i), &y.get_atom(i)))
    };
    let flips = |x: &kixdb::Flip, y: &kixdb::Flip| x.cols == y.cols && x.vals.iter().zip(&y.vals).all(|(p, q)| vecs(p, q));
    match (a, b) {
        (K::Float(x), K::Float(y)) => close(*x, *y),
        (K::Vector(x), K::Vector(y)) => vecs(x, y),
        (K::Table(x), K::Table(y)) => flips(x, y),
        (K::KeyedTable(x), K::KeyedTable(y)) => flips(&x.keys, &y.keys) && flips(&x.vals, &y.vals),
        (K::Dict(x), K::Dict(y)) => same(&x.keys, &y.keys) && same(&x.vals, &y.vals),
        _ => a == b,
    }
}

/// One engine and a cluster given the same statements.
pub struct Pair {
    pub single: Connection,
    pub cluster: Cluster,
    pub session: ClusterSession,
    pub shards: Shards,
}

impl Pair {
    pub fn new(shards: u32) -> Pair {
        Pair::with_tls(shards, None)
    }

    pub fn with_tls(shards: u32, tls: Option<NodeTls>) -> Pair {
        let shards = Shards::start_with(shards, tls.clone());
        let cluster = Cluster::connect_with(&shards.config(), tls).unwrap();
        let session = cluster.session();
        Pair { single: Server::new().connect(), cluster, session, shards }
    }

    /// Run `code` on both and require the same value or the same signal; returns how the cluster ran it.
    pub fn check(&mut self, code: &str) -> String {
        let expected = self.single.eval(code);
        let actual = self.session.eval(code);
        let plan = self.session.last_plan().join(" | ");
        match (&expected, &actual) {
            (Ok(e), Ok(a)) => assert!(same(e, a), "{code}\n  plan: {plan}\n  one engine: {e}\n  cluster:    {a}"),
            (Err(e), Err(a)) => assert_eq!(e.signal, a.signal, "{code}\n  plan: {plan}\n  one engine: {e}\n  cluster: {a}"),
            _ => panic!("{code}\n  plan: {plan}\n  one engine: {expected:?}\n  cluster:    {actual:?}"),
        }
        plan
    }

    /// `check`, and require the plan to mention `how`.
    pub fn plan(&mut self, code: &str, how: &str) {
        let plan = self.check(code);
        assert!(plan.contains(how), "{code}\n  expected a plan with '{how}', got '{plan}'");
    }
}

/// Trades, quotes and reference data that do not depend on chance.
pub const MARKET: [&str; 6] = [
    "n: 240",
    "trade: ([] time: 09:30:00.000 + 1500 * til n; sym: `AAPL`MSFT`GOOG`IBM`NVDA`AMD`ORCL ((13 * til n) mod 7); \
     price: 100 + 0.25 * (37 * til n) mod 101; size: 1 + (53 * til n) mod 200; ex: `N`Q`B ((til n) mod 3); trader: `ann`bob`cat`dan ((5 * til n) mod 4))",
    "quote: ([] time: 09:29:59.000 + 700 * til 600; sym: `AAPL`MSFT`GOOG`IBM`NVDA`AMD`ORCL ((11 * til 600) mod 7); \
     bid: 99 + 0.25 * (29 * til 600) mod 103; ask: 101 + 0.25 * (31 * til 600) mod 97)",
    ".kix.dist[`inst; `replicated]",
    "inst: ([sym: `AAPL`MSFT`GOOG`IBM`NVDA] sector: `tech`tech`media`services`chips; lot: 100 100 50 10 25)",
    "acct: ([trader: `ann`bob`cat] desk: `east`west`east)",
];

pub fn market(shards: u32) -> Pair {
    load_market(Pair::new(shards))
}

pub fn load_market(mut p: Pair) -> Pair {
    // `acct` is keyed by a column that is not what trades are sharded by
    for statement in MARKET {
        p.single.eval(statement).unwrap_or_else(|e| panic!("{statement}: {e}"));
        p.session.eval(statement).unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    p
}

/// A new certificate authority in `dir` with a certificate for each name, and what each of those nodes would load.
pub fn certificates<const N: usize>(dir: &std::path::Path, names: [&str; N]) -> [NodeTls; N] {
    make_certs(dir, &names.map(String::from)).unwrap();
    names.map(|n| NodeTls::from_files(dir.join("ca.pem"), dir.join(format!("{n}.pem")), dir.join(format!("{n}.key"))).unwrap())
}
