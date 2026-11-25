//! Global symbol interner (`SymId`), matching kdb+'s interned symbol pool.
//!
//! `SymId(0)` is always the empty symbol `` ` `` (the symbol null). Serialization writes the symbol's string and
//! deserialization re-interns it, so snapshots, WAL records, and RPC messages stay valid across processes and restarts.
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::sync::{OnceLock, RwLock};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct SymId(pub u32);

impl SymId {
    pub const EMPTY: SymId = SymId(0);

    pub fn is_null(self) -> bool {
        self.0 == 0
    }

    pub fn as_str_owned(self) -> String {
        resolve(self)
    }
}

struct Pool {
    to_id: HashMap<String, SymId>,
    from_id: Vec<String>,
}

impl Pool {
    fn new() -> Self {
        let mut to_id = HashMap::new();
        to_id.insert(String::new(), SymId::EMPTY);
        Self { to_id, from_id: vec![String::new()] }
    }
}

fn pool() -> &'static RwLock<Pool> {
    static POOL: OnceLock<RwLock<Pool>> = OnceLock::new();
    POOL.get_or_init(|| RwLock::new(Pool::new()))
}

pub fn intern(s: &str) -> SymId {
    if s.is_empty() {
        return SymId::EMPTY;
    }
    {
        let r = pool().read().unwrap_or_else(|e| e.into_inner());
        if let Some(&id) = r.to_id.get(s) {
            return id;
        }
    }
    let mut w = pool().write().unwrap_or_else(|e| e.into_inner());
    if let Some(&id) = w.to_id.get(s) {
        return id;
    }
    let id = SymId(w.from_id.len() as u32);
    let owned = s.to_string();
    w.from_id.push(owned.clone());
    w.to_id.insert(owned, id);
    id
}

pub fn resolve(id: SymId) -> String {
    if id.0 == 0 {
        return String::new();
    }
    let r = pool().read().unwrap_or_else(|e| e.into_inner());
    r.from_id.get(id.0 as usize).cloned().unwrap_or_default()
}

pub fn with_str<R>(id: SymId, f: impl FnOnce(&str) -> R) -> R {
    if id.0 == 0 {
        return f("");
    }
    let r = pool().read().unwrap_or_else(|e| e.into_inner());
    let s = r.from_id.get(id.0 as usize).map(String::as_str).unwrap_or("");
    f(s)
}

/// Number of distinct symbols currently interned (used by `\w` workspace stats).
pub fn sym_count() -> usize {
    let r = pool().read().unwrap_or_else(|e| e.into_inner());
    r.from_id.len()
}

impl Ord for SymId {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.0 == other.0 {
            return Ordering::Equal;
        }
        let r = pool().read().unwrap_or_else(|e| e.into_inner());
        let a = r.from_id.get(self.0 as usize).map(String::as_str).unwrap_or("");
        let b = r.from_id.get(other.0 as usize).map(String::as_str).unwrap_or("");
        a.cmp(b)
    }
}

impl PartialOrd for SymId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for SymId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        with_str(*self, |s| write!(f, "`{s}"))
    }
}

impl fmt::Display for SymId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        with_str(*self, |s| f.write_str(s))
    }
}

impl Serialize for SymId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        with_str(*self, |s| serializer.serialize_str(s))
    }
}

impl<'de> Deserialize<'de> for SymId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Ok(intern(&s))
    }
}
