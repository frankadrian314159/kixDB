//! Durable (`FileStorage`) and in-memory (`MemStorage`) Raft log, hard-state, and snapshot persistence.
//!
//! `FileStorage` keeps two files. `raft.log` is a sequence of checksummed records, appended to as the hard state or the
//! log changes and written afresh only when the log is cut. `raft.snap` holds the snapshot and is written only then.
use crate::{Conf, Entry, NodeId};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const LOG: &str = "raft.log";
const SNAP: &str = "raft.snap";
/// Where earlier versions kept everything. It now says where that went, which they refuse as damage rather than start
/// as a node that has never voted.
const OLD: &str = "raft.bin";
const MOVED: &[u8] = b"KXR2: the Raft state is in raft.log and raft.snap";

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
    #[serde(with = "crate::bytes")]
    pub data: Vec<u8>,
}

pub trait Storage {
    /// Everything stored, asked for once, when a node starts.
    fn load(&mut self) -> (HardState, Vec<Entry>, Option<Snapshot>);
    /// The snapshot alone.
    fn snapshot(&self) -> Option<Snapshot>;
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
    fn load(&mut self) -> (HardState, Vec<Entry>, Option<Snapshot>) {
        (self.hs.clone(), self.entries.clone(), self.snapshot.clone())
    }

    fn snapshot(&self) -> Option<Snapshot> {
        self.snapshot.clone()
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

/// A record of `raft.log`.
#[derive(Serialize, Deserialize)]
enum Record<'a> {
    Hard(HardState),
    /// Entries from an index on, replacing any the log held from there.
    Entries(u64, Cow<'a, [Entry]>),
}

/// The contents of the single file of earlier versions.
#[derive(Serialize, Deserialize)]
struct Old {
    hs: HardState,
    entries: Vec<Entry>,
    snapshot: Option<Snapshot>,
}

/// A value as a record: its length, its checksum, its bytes.
fn frame<T: Serialize>(value: &T) -> Vec<u8> {
    let payload = postcard::to_allocvec(value).expect("Raft state serializes");
    let mut out = Vec::with_capacity(8 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

/// The payload of the record `bytes` begins with, if it is whole and sound.
fn unframe(bytes: &[u8]) -> Option<&[u8]> {
    let len = u32::from_le_bytes(bytes.get(..4)?.try_into().ok()?) as usize;
    let payload = bytes.get(8..8usize.checked_add(len)?)?;
    (len > 0 && crc32fast::hash(payload).to_le_bytes() == bytes[4..8]).then_some(payload)
}

/// The log lives in its file: only `Raft` holds the entries in memory. The snapshot is kept here too, as reading it back
/// would hold a node up for too long each time one is sent.
pub struct FileStorage {
    dir: PathBuf,
    hs: HardState,
    sync: bool,
    /// `raft.log`, open for writing, and the length of its sound records.
    log: Option<File>,
    len: u64,
    /// Records a failed write left to be written.
    pending: Vec<u8>,
    /// The entries read when opening, until `load` takes them.
    loaded: Vec<Entry>,
    snapshot: Option<Snapshot>,
}

impl FileStorage {
    pub fn open(dir: impl AsRef<Path>, sync: bool) -> Result<Self, String> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        let mut s = Self { dir, hs: HardState::default(), sync, log: None, len: 0, pending: vec![], loaded: vec![], snapshot: None };
        let io = |e: std::io::Error| e.to_string();
        let old = s.read(OLD)?;
        if old.as_deref() != Some(MOVED) {
            // a new directory, or one of an earlier version
            let mut mem = MemStorage::new();
            if let Some(bytes) = old {
                let sound = bytes.len() >= 8 && &bytes[..4] == b"KXRF" && crc32fast::hash(&bytes[8..]).to_le_bytes() == bytes[4..8];
                let p: Old = sound.then(|| postcard::from_bytes(&bytes[8..]).ok()).flatten().ok_or_else(|| s.damaged(OLD))?;
                mem = MemStorage { hs: p.hs, entries: p.entries, snapshot: p.snapshot };
            }
            if let Some(snap) = &mem.snapshot {
                s.replace(SNAP, &frame(snap)).map_err(io)?;
            }
            s.hs = mem.hs;
            s.rewrite_log(&mem.entries).map_err(io)?;
            s.replace(OLD, MOVED).map_err(io)?;
            (s.loaded, s.snapshot) = (mem.entries, mem.snapshot);
            return Ok(s);
        }
        s.snapshot = s.read_snapshot()?;
        let bytes = s.read(LOG)?.ok_or_else(|| s.damaged(LOG))?;
        let (mut at, mut mem) = (0, MemStorage::new());
        while at < bytes.len() {
            let rest = &bytes[at..];
            let Some((payload, record)) = unframe(rest).and_then(|p| Some((p, postcard::from_bytes::<Record>(p).ok()?))) else {
                // what a crash can leave is an unfinished last record (or zeros); anything else is damage
                let len = rest.get(..4).map_or(0, |l| u32::from_le_bytes(l.try_into().unwrap()) as usize);
                if len.saturating_add(8) >= rest.len() || rest.iter().all(|&b| b == 0) {
                    break;
                }
                return Err(s.damaged(LOG));
            };
            at += 8 + payload.len();
            match record {
                Record::Hard(hs) => mem.hs = hs,
                Record::Entries(from, entries) => mem.append_entries(from, &entries),
            }
        }
        (s.hs, s.loaded, s.len) = (mem.hs, mem.entries, at as u64);
        s.log = Some(s.open_log().map_err(io)?);
        // cut off what is not sound
        s.write_pending().map_err(io)?;
        Ok(s)
    }

    fn damaged(&self, name: &str) -> String {
        format!("{} is damaged", self.dir.join(name).display())
    }

    fn read(&self, name: &str) -> Result<Option<Vec<u8>>, String> {
        match fs::read(self.dir.join(name)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(format!("{}: {e}", self.dir.join(name).display())),
        }
    }

    fn read_snapshot(&self) -> Result<Option<Snapshot>, String> {
        let Some(bytes) = self.read(SNAP)? else { return Ok(None) };
        let whole = unframe(&bytes).filter(|p| p.len() + 8 == bytes.len());
        whole.and_then(|p| postcard::from_bytes(p).ok()).map(Some).ok_or_else(|| self.damaged(SNAP))
    }

    /// Write a file whole: it holds either what it held or `bytes`.
    fn replace(&self, name: &str, bytes: &[u8]) -> std::io::Result<()> {
        let tmp = self.dir.join(format!("{name}.tmp"));
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        if self.sync {
            f.sync_data()?;
        }
        drop(f);
        fs::rename(&tmp, self.dir.join(name))
    }

    fn open_log(&self) -> std::io::Result<File> {
        OpenOptions::new().write(true).open(self.dir.join(LOG))
    }

    fn rewrite_log(&mut self, entries: &[Entry]) -> std::io::Result<()> {
        let mut bytes = frame(&Record::Hard(self.hs.clone()));
        if let Some(first) = entries.first() {
            bytes.extend(frame(&Record::Entries(first.index, Cow::Borrowed(entries))));
        }
        // (closed first: Windows does not replace a file that is open)
        self.log = None;
        self.replace(LOG, &bytes)?;
        self.log = Some(self.open_log()?);
        self.len = bytes.len() as u64;
        self.pending.clear();
        Ok(())
    }

    /// Put the records not yet written after the sound ones, dropping anything else there.
    fn write_pending(&mut self) -> std::io::Result<()> {
        let Some(f) = self.log.as_mut() else { return Err(std::io::Error::other("the log is not open")) };
        f.set_len(self.len)?;
        f.seek(SeekFrom::Start(self.len))?;
        f.write_all(&self.pending)?;
        if self.sync {
            f.sync_data()?;
        }
        self.len += self.pending.len() as u64;
        self.pending.clear();
        Ok(())
    }

    fn append(&mut self, record: &Record) {
        self.pending.extend(frame(record));
        // a record that cannot be written is kept, to go before the next one
        if let Err(e) = self.write_pending() {
            eprintln!("raft: writing {} failed: {e}", self.dir.join(LOG).display());
        }
    }
}

impl Storage for FileStorage {
    fn load(&mut self) -> (HardState, Vec<Entry>, Option<Snapshot>) {
        (self.hs.clone(), std::mem::take(&mut self.loaded), self.snapshot())
    }

    fn snapshot(&self) -> Option<Snapshot> {
        self.snapshot.clone()
    }

    fn save_hard_state(&mut self, hs: &HardState) {
        self.hs = hs.clone();
        self.append(&Record::Hard(hs.clone()));
    }

    fn append_entries(&mut self, from_index: u64, entries: &[Entry]) {
        self.append(&Record::Entries(from_index, Cow::Borrowed(entries)));
    }

    fn save_snapshot(&mut self, snap: &Snapshot, remaining_entries: &[Entry]) {
        self.snapshot = Some(snap.clone());
        // the snapshot first: until the log is cut it still holds what the snapshot stands for
        if let Err(e) = self.replace(SNAP, &frame(snap)).and_then(|_| self.rewrite_log(remaining_entries)) {
            eprintln!("raft: saving the snapshot in {} failed: {e}", self.dir.display());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: u64, data: u8) -> Entry {
        Entry { term: 1, index, data: vec![data; 100], conf: None }
    }

    fn snapshot(index: u64) -> Snapshot {
        Snapshot { index, term: 1, conf: Conf::default(), data: vec![9; 10_000] }
    }

    #[test]
    fn records_are_appended_and_the_snapshot_is_written_only_when_the_log_is_cut() {
        let dir = tempfile::tempdir().unwrap();
        let open = || FileStorage::open(dir.path(), true);
        let size = |name: &str| fs::metadata(dir.path().join(name)).unwrap().len();
        let mut s = open().unwrap();
        s.append_entries(1, &[entry(1, 1), entry(2, 2), entry(3, 3)]);
        s.save_snapshot(&snapshot(2), &[entry(3, 3)]);
        let (log, snap) = (size(LOG), fs::read(dir.path().join(SNAP)).unwrap());
        assert!(log < 200 && snap.len() > 10_000);
        // changes cost their own size, and leave the snapshot alone
        s.save_hard_state(&HardState { term: 4, voted_for: Some(2) });
        s.append_entries(4, &[entry(4, 4), entry(5, 5)]);
        s.append_entries(5, &[entry(5, 55)]);
        assert!(size(LOG) < log + 450);
        assert_eq!(fs::read(dir.path().join(SNAP)).unwrap(), snap);
        drop(s);
        let kept = open().unwrap().load();
        assert_eq!(kept.0, HardState { term: 4, voted_for: Some(2) });
        assert_eq!(kept.1, [entry(3, 3), entry(4, 4), entry(5, 55)]);

        // an unfinished last record is dropped, and writing goes on after the sound ones
        let whole = fs::read(dir.path().join(LOG)).unwrap();
        for tail in [&whole[..9], &[0; 64][..], &[7, 0, 0][..]] {
            fs::write(dir.path().join(LOG), [&whole[..], tail].concat()).unwrap();
            assert_eq!(open().unwrap().load(), kept);
            assert_eq!(size(LOG), whole.len() as u64);
        }
        // damage before the end, or in the snapshot, is refused
        for (name, at) in [(LOG, 9), (SNAP, snap.len() - 1)] {
            let sound = fs::read(dir.path().join(name)).unwrap();
            let mut bad = sound.clone();
            bad[at] ^= 1;
            fs::write(dir.path().join(name), bad).unwrap();
            assert!(open().is_err(), "{name}");
            fs::write(dir.path().join(name), sound).unwrap();
        }
    }

    #[test]
    fn the_state_of_an_earlier_version_is_taken_over() {
        let dir = tempfile::tempdir().unwrap();
        let old = Old { hs: HardState { term: 3, voted_for: Some(1) }, entries: vec![entry(8, 8)], snapshot: Some(snapshot(7)) };
        let payload = postcard::to_allocvec(&old).unwrap();
        fs::write(dir.path().join(OLD), [&b"KXRF"[..], &crc32fast::hash(&payload).to_le_bytes(), &payload].concat()).unwrap();
        for _ in 0..2 {
            let loaded = FileStorage::open(dir.path(), true).unwrap().load();
            assert_eq!(loaded, (old.hs.clone(), old.entries.clone(), old.snapshot.clone()));
        }
        assert_eq!(fs::read(dir.path().join(OLD)).unwrap(), MOVED);
    }
}
