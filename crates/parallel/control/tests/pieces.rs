//! Tables and rows too large for one message reach the shards in pieces.
mod common;
use kix_shared_ces::pieces::PIECE;
use std::sync::atomic::Ordering::Relaxed;

#[test]
fn large_loads_and_transfers_between_shards_arrive_whole() {
    PIECE.store(1000, Relaxed);
    let mut p = common::Pair::new(3);
    for statement in [
        "t: ([] sym: `a`b`c`d`e (til 5000) mod 5; v: til 5000)",
        "`t insert (5000#`d`e; 5000 + til 5000)",
        "k: ([sym: `a`b`c`d`e] w: 1 2 3 4 5)",
    ] {
        p.single.eval(statement).unwrap_or_else(|e| panic!("{statement}: {e}"));
        p.session.eval(statement).unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    p.check("t");
    p.check("k");
    // rows shuffled between the shards travel the same way
    p.cluster.set_broadcast_limit(0);
    p.session.eval("DISTRIBUTE t BY v").unwrap();
    p.plan("select v, w from t lj k", "shuffle");
}
