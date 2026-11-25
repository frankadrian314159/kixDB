//! Deterministic 1024-slot key hash and date/time partition routing for `kixdb` tables.
//!
//! Symbol values (`K::Sym`) hash by their resolved UTF-8 symbol text (never process-local `u32` IDs), and integers/dates
//! hash by their canonical `i64` representation, so every process routes `` `AAPL `` or `42` to the exact same shard while
//! preserving intra-shard time order.
use kixdb::sym::resolve;
use kixdb::K;

pub const SLOTS: usize = 1024;

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

pub fn key_hash(val: &K) -> u64 {
    if val.is_null() {
        return 0;
    }
    match val {
        K::Sym(s) => fnv1a64(resolve(*s).as_bytes()),
        K::Char(c) => fnv1a64(&[*c]),
        K::Guid(g) => fnv1a64(g),
        K::Vector(kixdb::ColVec::Char(_, bs)) => fnv1a64(bs),
        _ => {
            if let Ok(n) = val.as_i64() {
                fnv1a64(&n.to_le_bytes())
            } else if let Ok(f) = val.as_f64() {
                let bits = if f == 0.0 { 0 } else { f.to_bits() };
                fnv1a64(&bits.to_le_bytes())
            } else {
                fnv1a64(&val.hash_key())
            }
        }
    }
}

pub fn slot_of(hash: u64) -> usize {
    let mut x = hash;
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    (x % (SLOTS as u64)) as usize
}

pub fn even_slots(nshards: usize) -> Vec<u32> {
    let n = nshards.max(1);
    (0..SLOTS).map(|s| (s % n) as u32).collect()
}

pub fn shard_for_key(val: &K, slots: &[u32]) -> usize {
    let sl = slot_of(key_hash(val));
    slots.get(sl).copied().unwrap_or(0) as usize
}

/// The shard of a row placed by several columns together (such as `date` and `sym`): the hash of all their values, in
/// the order given. One value routes as [`shard_for_key`].
pub fn shard_for_keys(values: &[K], slots: &[u32]) -> usize {
    match values {
        [one] => shard_for_key(one, slots),
        _ => {
            let hashes: Vec<u8> = values.iter().flat_map(|v| key_hash(v).to_le_bytes()).collect();
            slots.get(slot_of(fnv1a64(&hashes))).copied().unwrap_or(0) as usize
        }
    }
}

/// Route a temporal/integer partition value (`date`, `month`, `int`) to a shard index (`val mod nshards`).
pub fn shard_for_partition(val: &K, nshards: usize) -> usize {
    let n = nshards.max(1) as i64;
    let v = val.as_i64().unwrap_or(0);
    v.rem_euclid(n) as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use kixdb::sym::intern;

    #[test]
    fn symbols_and_integers_route_deterministically_and_evenly() {
        let slots = even_slots(4);
        let s1 = shard_for_key(&K::Sym(intern("AAPL")), &slots);
        let s2 = shard_for_key(&K::Sym(intern("AAPL")), &slots);
        assert_eq!(s1, s2);

        // Int(42) and Long(42) must hash identically
        assert_eq!(key_hash(&K::Int(42)), key_hash(&K::Long(42)));

        let mut counts = [0usize; 4];
        for i in 0..4000 {
            counts[shard_for_key(&K::Long(i), &slots)] += 1;
        }
        for c in counts {
            assert!(c > 800 && c < 1200, "skewed distribution: {counts:?}");
        }
    }

    #[test]
    fn composite_keys_spread_each_day_and_each_symbol() {
        let slots = even_slots(4);
        let at = |day: i32, sym: &str| shard_for_keys(&[K::Date(day), K::Sym(intern(sym))], &slots);
        assert_eq!(at(1, "AAPL"), at(1, "AAPL"));
        assert_eq!(shard_for_keys(&[K::Long(42)], &slots), shard_for_key(&K::Long(42), &slots));
        let mut counts = [0usize; 4];
        for day in 0..40 {
            for s in 0..100 {
                counts[at(day, &format!("S{s}"))] += 1;
            }
        }
        for c in counts {
            assert!(c > 800 && c < 1200, "skewed distribution: {counts:?}");
        }
        // one day lands on every shard, and so does one symbol
        let spread = |shards: std::collections::HashSet<usize>| shards.len() == 4;
        assert!(spread((0..100).map(|s| at(7, &format!("S{s}"))).collect()));
        assert!(spread((0..100).map(|d| at(d, "AAPL")).collect()));
    }
}
