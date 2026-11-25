//! Durable (`FileStorage`) and in-memory (`MemStorage`) Raft log, hard-state, and snapshot persistence.
use crate::{Conf, Entry, NodeId};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardState {
    pub term: u64,
    pub voted_for: Option<NodeId>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub index: u64,
    pub term: u64,
    pub conf: Conf,
    pub data: Vec<u8>,
}

pub trait Storage {
    fn load(&self) -> (HardState, Vec<Entry>, Option<Snapshot>);
    fn save_hard_state(&mut self, hs: &HardState);
    fn append_entries(&mut self, from_index: u64, entries: &[Entry]);
    fn save_snapshot(&mut self, snap: &Snapshot, remaining_entries: &[Entry]);
}

#[derive(Clone, Default)]
pub struct MemStorage {
    pub hs: HardState,
    pub entries: Vec<Entry>,
    pub snapshot: Option<Snapshot>,
}

impl MemStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Storage for MemStorage {
    fn load(&self) -> (HardState, Vec<Entry>, Option<Snapshot>) {
        (self.hs.clone(), self.entries.clone(), self.snapshot.clone())
    }

    fn save_hard_state(&mut self, hs: &HardState) {
        self.hs = hs.clone();
    }

    fn append_entries(&mut self, from_index: u64, entries: &[Entry]) {
        self.entries.retain(|e| e.index < from_index);
        self.entries.extend_from_slice(entries);
    }

    fn save_snapshot(&mut self, snap: &Snapshot, remaining_entries: &[Entry]) {
        self.snapshot = Some(snap.clone());
        self.entries = remaining_entries.to_vec();
    }
}

#[derive(Serialize, Deserialize, Default)]
struct PersistedRaft {
    hs: HardState,
    entries: Vec<Entry>,
    snapshot: Option<Snapshot>,
}

pub struct FileStorage {
    dir: PathBuf,
    mem: MemStorage,
    sync: bool,
}

impl FileStorage {
    pub fn open(dir: impl AsRef<Path>, sync: bool) -> Result<Self, String> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        let state_path = dir.join("raft.bin");
        let mem = if state_path.exists() {
            let bytes = fs::read(&state_path).map_err(|e| e.to_string())?;
            let damaged = || format!("{} is damaged", state_path.display());
            if bytes.len() < 8 || &bytes[..4] != b"KXRF" || crc32fast::hash(&bytes[8..]).to_le_bytes() != bytes[4..8] {
                return Err(damaged());
            }
            let p: PersistedRaft = postcard::from_bytes(&bytes[8..]).map_err(|_| damaged())?;
            MemStorage { hs: p.hs, entries: p.entries, snapshot: p.snapshot }
        } else {
            MemStorage::new()
        };
        Ok(Self { dir, mem, sync })
    }

    fn flush(&self) {
        let p = PersistedRaft {
            hs: self.mem.hs.clone(),
            entries: self.mem.entries.clone(),
            snapshot: self.mem.snapshot.clone(),
        };
        let Ok(payload) = postcard::to_allocvec(&p) else { return };
        let crc = crc32fast::hash(&payload);
        let mut out = Vec::with_capacity(8 + payload.len());
        out.extend_from_slice(b"KXRF");
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&payload);
        let tmp = self.dir.join("raft.bin.tmp");
        let dst = self.dir.join("raft.bin");
        if let Ok(mut f) = fs::File::create(&tmp) {
            use std::io::Write;
            let _ = f.write_all(&out);
            if self.sync {
                let _ = f.sync_data();
            }
            let _ = fs::rename(&tmp, &dst);
        }
    }
}

impl Storage for FileStorage {
    fn load(&self) -> (HardState, Vec<Entry>, Option<Snapshot>) {
        self.mem.load()
    }

    fn save_hard_state(&mut self, hs: &HardState) {
        self.mem.save_hard_state(hs);
        self.flush();
    }

    fn append_entries(&mut self, from_index: u64, entries: &[Entry]) {
        self.mem.append_entries(from_index, entries);
        self.flush();
    }

    fn save_snapshot(&mut self, snap: &Snapshot, remaining_entries: &[Entry]) {
        self.mem.save_snapshot(snap, remaining_entries);
        self.flush();
    }
}
