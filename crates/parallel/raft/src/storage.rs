//! Durable (`FileStorage`) and in-memory (`MemStorage`) Raft log, hard-state, and snapshot persistence.
//!
//! `FileStorage` keeps two files. `raft.log` is a sequence of checksummed records, appended to as the hard state or the
//! log changes and written afresh only when the log is cut. `raft.snap` holds the snapshot and is written only then.
use crate::{Conf, Entry, NodeId};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
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
    pub data: Vec<u8>,
}

pub trait Storage {
    fn load(&self) -> (HardState, Vec<Entry>, Option<Snapshot>);
    /// The snapshot alone.
    fn snapshot(&self) -> Option<Snapshot> {
        self.load().2
    }
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

pub struct FileStorage {
    dir: PathBuf,
    mem: MemStorage,
    sync: bool,
    /// `raft.log`, open for appending.
    log: Option<File>,
}

impl FileStorage {
    pub fn open(dir: impl AsRef<Path>, sync: bool) -> Result<Self, String> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        let mut s = Self { dir, mem: MemStorage::new(), sync, log: None };
        let read = |name: &str| match fs::read(s.dir.join(name)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(format!("{}: {e}", s.dir.join(name).display())),
        };
        let damaged = |name: &str| format!("{} is damaged", s.dir.join(name).display());
        let old = read(OLD)?;
        if old.as_deref() != Some(MOVED) {
            // a new directory, or one of an earlier version
            if let Some(bytes) = old {
                let sound = bytes.len() >= 8 && &bytes[..4] == b"KXRF" && crc32fast::hash(&bytes[8..]).to_le_bytes() == bytes[4..8];
                let p: Old = sound.then(|| postcard::from_bytes(&bytes[8..]).ok()).flatten().ok_or_else(|| damaged(OLD))?;
                s.mem = MemStorage { hs: p.hs, entries: p.entries, snapshot: p.snapshot };
            }
            let io = |e: std::io::Error| e.to_string();
            if let Some(snap) = &s.mem.snapshot {
                s.replace(SNAP, &frame(snap)).map_err(io)?;
            }
            s.rewrite_log().map_err(io)?;
            s.replace(OLD, MOVED).map_err(io)?;
            return Ok(s);
        }
        if let Some(bytes) = read(SNAP)? {
            let whole = unframe(&bytes).filter(|p| p.len() + 8 == bytes.len());
            s.mem.snapshot = Some(whole.and_then(|p| postcard::from_bytes(p).ok()).ok_or_else(|| damaged(SNAP))?);
        }
        let bytes = read(LOG)?.ok_or_else(|| damaged(LOG))?;
        let mut at = 0;
        while at < bytes.len() {
            let rest = &bytes[at..];
            let Some((payload, record)) = unframe(rest).and_then(|p| Some((p, postcard::from_bytes::<Record>(p).ok()?))) else {
                // what a crash can leave is an unfinished last record (or zeros); anything else is damage
                let len = rest.get(..4).map_or(0, |l| u32::from_le_bytes(l.try_into().unwrap()) as usize);
                if len.saturating_add(8) >= rest.len() || rest.iter().all(|&b| b == 0) {
                    break;
                }
                return Err(damaged(LOG));
            };
            at += 8 + payload.len();
            match record {
                Record::Hard(hs) => s.mem.hs = hs,
                Record::Entries(from, entries) => s.mem.append_entries(from, &entries),
            }
        }
        // a crash between writing a snapshot and cutting the log leaves entries the snapshot stands for
        let covered = s.mem.snapshot.as_ref().map_or(0, |snap| snap.index);
        s.mem.entries.retain(|e| e.index > covered);
        let io = |e: std::io::Error| e.to_string();
        if at < bytes.len() {
            // (a handle that only appends may not shorten a file on Windows)
            OpenOptions::new().write(true).open(s.dir.join(LOG)).and_then(|f| f.set_len(at as u64)).map_err(io)?;
        }
        s.log = Some(s.append_to_log().map_err(io)?);
        Ok(s)
    }

    fn append_to_log(&self) -> std::io::Result<File> {
        OpenOptions::new().append(true).open(self.dir.join(LOG))
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

    fn rewrite_log(&mut self) -> std::io::Result<()> {
        let mut bytes = frame(&Record::Hard(self.mem.hs.clone()));
        if let Some(first) = self.mem.entries.first() {
            bytes.extend(frame(&Record::Entries(first.index, Cow::Borrowed(&self.mem.entries))));
        }
        // (closed first: Windows does not replace a file that is open)
        self.log = None;
        self.replace(LOG, &bytes)?;
        self.log = Some(self.append_to_log()?);
        Ok(())
    }

    fn append(&mut self, record: &Record) {
        let sync = self.sync;
        let written = self.log.as_mut().is_some_and(|f| f.write_all(&frame(record)).and_then(|_| if sync { f.sync_data() } else { Ok(()) }).is_ok());
        // after a failed write the end of the file is unknown: write it afresh
        if !written {
            if let Err(e) = self.rewrite_log() {
                eprintln!("raft: writing {} failed: {e}", self.dir.join(LOG).display());
            }
        }
    }
}

impl Storage for FileStorage {
    fn load(&self) -> (HardState, Vec<Entry>, Option<Snapshot>) {
        self.mem.load()
    }

    fn snapshot(&self) -> Option<Snapshot> {
        self.mem.snapshot()
    }

    fn save_hard_state(&mut self, hs: &HardState) {
        self.mem.save_hard_state(hs);
        self.append(&Record::Hard(hs.clone()));
    }

    fn append_entries(&mut self, from_index: u64, entries: &[Entry]) {
        self.mem.append_entries(from_index, entries);
        self.append(&Record::Entries(from_index, Cow::Borrowed(entries)));
    }

    fn save_snapshot(&mut self, snap: &Snapshot, remaining_entries: &[Entry]) {
        self.mem.save_snapshot(snap, remaining_entries);
        // the snapshot first: until the log is cut it still holds what the snapshot stands for
        if let Err(e) = self.replace(SNAP, &frame(snap)).and_then(|_| self.rewrite_log()) {
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
        let kept = s.load();
        drop(s);
        assert_eq!(open().unwrap().load(), kept);
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
        // a snapshot written just before a crash stands for the entries the log still holds
        fs::write(dir.path().join(SNAP), frame(&snapshot(4))).unwrap();
        assert_eq!(open().unwrap().load().1, [entry(5, 55)]);
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
