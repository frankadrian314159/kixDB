//! The index of a large keyed table's keys finds what reading through them finds, whatever is written to the table.
use kixdb::{Connection, Server};

fn holds(c: &mut Connection, check: &str, after: &str) {
    let got = c.eval(check).unwrap_or_else(|e| panic!("{check} (after {after}): {e}")).to_string();
    assert_eq!(got, "1b", "{check} (after {after})");
}

/// `k`, keyed by `id`, against the same rows unkeyed, which are searched by reading them.
fn lookups_agree(c: &mut Connection, after: &str) {
    for x in [0, 3, 17, 2999, 3000, 3001, 3002, 7000, 13000, -1] {
        // (asked twice: the index is built the second time)
        for _ in 0..2 {
            holds(c, &format!("(k {x}) ~ first (select v, w from (0!k) where id = {x}), enlist `v`w!(0N; `)"), after);
            holds(c, &format!("(0!select from k where id = {x}) ~ select from (0!k) where id = {x}"), after);
            holds(c, &format!("(0!select from k where {x} = id, v > 5) ~ select from (0!k) where id = {x}, v > 5"), after);
            holds(c, &format!("(k ([] id: ({x}; 17))) ~ {{first (select v, w from (0!k) where id = x), enlist `v`w!(0N; `)}} each ({x}; 17)"), after);
        }
    }
}

#[test]
fn one_key_through_writes() {
    let server = Server::new();
    let mut c = server.connect();
    c.eval("k: ([id: til 3000] v: til 3000; w: 3000#`a`b`c); held: k").unwrap();
    lookups_agree(&mut c, "making it");
    for write in [
        "`k upsert (17; 100; `z)",
        "`k upsert (3000; 101; `y)",
        "`k upsert ([] id: 3001 17 3002 3001; v: 1 2 3 4; w: `p`q`r`s)",
        "`k insert (7000; 5; `t)",
        "k,: ([id: enlist 13000] v: enlist 9; w: enlist `u)",
        "update v: 0 from `k where id = 3",
        "update id: id + 20000 from `k where id = 0",
        "delete from `k where id = 17",
        "`k upsert (17; 8; `a)",
        "k: `v xdesc k",
        "k: 1 ! (0!k), ([] id: 3 3; v: 70 71; w: `d`e)",
        "`k upsert (3; 72; `f)",
        "`k upsert (5000; 1; `g)",
    ] {
        c.eval(write).unwrap_or_else(|e| panic!("{write}: {e}"));
        lookups_agree(&mut c, write);
    }
    // a copy made before the writes is as it was
    holds(&mut c, "(3000 = count held) and 17 = held[17; `v]", "the writes");
    c.eval("f: {[t] t,: ([id: enlist 9000] v: enlist 1; w: enlist `h); (t 9000) `v}").unwrap();
    holds(&mut c, "(1 = f k) and (null (k 9000) `v) and 1 = f k", "appending to a local");
}

#[test]
fn several_keys_symbols_and_floats() {
    let server = Server::new();
    let mut c = server.connect();
    c.eval("m: ([a: 3000#1 2 3; b: til 3000] v: til 3000); s: ([sym: `$string til 3000] v: til 3000); f: ([x: 0.5 + til 3000] v: til 3000)").unwrap();
    for write in ["0", "`m upsert (1; 0; 50); `s upsert (`7; 50)", "`m upsert (9; 9000; 51); `s upsert (`new; 51)", "delete from `m where b = 5; delete from `s where sym = `5"] {
        c.eval(write).unwrap();
        for _ in 0..2 {
            for (a, b) in [(1, 0), (2, 1), (9, 9000), (1, 5), (3, 2999), (2, 2999)] {
                holds(&mut c, &format!("((m ({a}; {b})) `v) ~ first (exec v from (0!m) where a = {a}, b = {b}), 0N"), write);
                holds(&mut c, &format!("((m ([] a: enlist {a}; b: enlist {b}))`v) ~ enlist (m ({a}; {b})) `v"), write);
            }
            for name in ["0", "7", "5", "new", "none"] {
                holds(&mut c, &format!("((s `$\"{name}\") `v) ~ first (exec v from (0!s) where sym = `$\"{name}\"), 0N"), write);
                holds(&mut c, &format!("(0!select from s where sym = `$\"{name}\") ~ select from (0!s) where sym = `$\"{name}\""), write);
            }
            // (`=` on floats allows a little difference, which an index would not)
            holds(&mut c, "1 = count select from f where x = 10.5 + 1e-15", write);
        }
    }
}
