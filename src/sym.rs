//! Global symbol interner (`SymId`), matching kdb+'s interned symbol pool.
//!
//! `SymId(0)` is always the empty symbol `` ` `` (the symbol null). Serialization writes the symbol's string and
//! deserialization re-interns it, so snapshots, WAL records, and RPC messages stay valid across processes and restarts.
//!
//! Names are never freed, so each is held once, for good, and read without a lock.
use foldhash::HashMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::fmt;
use std::sync::{OnceLock, RwLock};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct SymId(pub u32);

impl SymId {
    pub const EMPTY: SymId = SymId(0);

    pub fn is_null(self) -> bool {
        self.0 == 0
    }
}

/// The ids of the names interned so far.
fn pool() -> &'static RwLock<HashMap<&'static str, SymId>> {
    static POOL: OnceLock<RwLock<HashMap<&'static str, SymId>>> = OnceLock::new();
    POOL.get_or_init(|| RwLock::new(HashMap::from_iter([("", SymId::EMPTY)])))
}

/// The names by id, in blocks that are made as they are needed, each twice as long as the one before.
type Block = Box<[OnceLock<&'static str>]>;
static NAMES: [OnceLock<Block>; 23] = [const { OnceLock::new() }; 23];
const FIRST: u64 = 1 << 10;

/// The block that holds the name of symbol `id`, and its place there.
fn place(id: u32) -> (usize, usize) {
    let n = u64::from(id) + FIRST;
    let block = n.ilog2() - FIRST.ilog2();
    (block as usize, (n - (FIRST << block)) as usize)
}

pub fn intern(s: &str) -> SymId {
    if s.is_empty() {
        return SymId::EMPTY;
    }
    if let Some(&id) = pool().read().unwrap_or_else(|e| e.into_inner()).get(s) {
        return id;
    }
    let mut ids = pool().write().unwrap_or_else(|e| e.into_inner());
    if let Some(&id) = ids.get(s) {
        return id;
    }
    let id = SymId(ids.len() as u32);
    let (block, at) = place(id.0);
    let name: &'static str = Box::leak(s.into());
    // (the name is there before anyone can learn the id)
    let _ = NAMES[block].get_or_init(|| (0..FIRST << block).map(|_| OnceLock::new()).collect())[at].set(name);
    ids.insert(name, id);
    id
}

pub fn resolve(id: SymId) -> &'static str {
    let (block, at) = place(id.0);
    NAMES[block].get().and_then(|names| names[at].get()).copied().unwrap_or_default()
}

/// Number of distinct symbols currently interned (used by `\w` workspace stats).
pub fn sym_count() -> usize {
    pool().read().unwrap_or_else(|e| e.into_inner()).len()
}

impl Ord for SymId {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.0 == other.0 {
            return Ordering::Equal;
        }
        resolve(*self).cmp(resolve(*other))
    }
}

impl PartialOrd for SymId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for SymId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "`{}", resolve(*self))
    }
}

impl fmt::Display for SymId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(resolve(*self))
    }
}

impl Serialize for SymId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(resolve(*self))
    }
}

impl<'de> Deserialize<'de> for SymId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Ok(intern(&s))
    }
}
