//! Durable workspace persistence: checksummed write-ahead log (`kix.wal`), atomic snapshots (`kix.snap`),
//! crash recovery (discarding torn log tails), and replication hooks for `crates/parallel/`.
use crate::delta;
use crate::engine::{DistMode, ViewDef, Workspace};
use crate::error::{KError, KResult};
use crate::storage;
use crate::sym::{resolve, SymId};
use crate::types::K;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const WAL_MAGIC: [u8; 4] = *b"KXWL";
const SNAP_MAGIC: [u8; 4] = *b"KXSN";

#[derive(Clone, Copy, Debug)]
pub struct StorageOptions {
    /// Sync every WAL append to disk before returning.
    pub sync_wal: bool,
    /// Automatically fold the WAL into a snapshot after this many records (`0` = manual/shutdown only).
    pub checkpoint_every: usize,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self { sync_wal: true, checkpoint_every: 1000 }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LogOp {
    SetVar { name: SymId, val: K },
    DropVar { name: SymId },
    SetView { def: ViewDef },
    SetDist { table: SymId, mode: DistMode },
    /// A new value of a vector or table as what changed in it (see `delta.rs`).
    ChangeVar { name: SymId, changes: delta::Changes },
    /// A vector or table with some of its rows removed.
    RemoveRows { name: SymId, removed: delta::Removed },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalRecord {
    pub ts: u64,
    pub ops: Vec<LogOp>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotData {
    pub ts: u64,
    pub vars: Vec<(SymId, K)>,
    pub views: Vec<ViewDef>,
    pub dist: Vec<(SymId, DistMode)>,
}

/// Consensus replication hook used by `crates/parallel/storage` (`kix-shard` Raft groups).
pub trait Replicator: Send + Sync {
    fn propose(&self, rec: &WalRecord) -> KResult<()>;
}

pub struct Persist {
    dir: PathBuf,
    /// Exclusive lock on `kix.lock`, held while the workspace is open.
    _lock: File,
    wal_file: File,
    pub wal_records: u64,
    pub wal_bytes: u64,
    pub snap_bytes: u64,
    pub log_forces: u64,
    pub opts: StorageOptions,
}

impl Persist {
    pub fn open(dir: &Path, opts: StorageOptions) -> KResult<(Persist, Workspace)> {
        fs::create_dir_all(dir)?;
        let lock = OpenOptions::new().create(true).truncate(false).write(true).open(dir.join("kix.lock"))?;
        if lock.try_lock().is_err() {
            return Err(KError::io(format!("data directory '{}' is in use by another kixdb process", dir.display())));
        }
        let mut ws = Workspace::new();
        ws.data_dir = Some(dir.to_path_buf());

        // 1. Mount any splayed/partitioned HDB directories in `dir`
        storage::mount_hdb_dir(&mut ws, dir)?;

        // 2. Load latest atomic snapshot `kix.snap` if present
        let snap_path = dir.join("kix.snap");
        let mut snap_bytes = 0u64;
        if snap_path.exists() {
            let bytes = fs::read(&snap_path)?;
            snap_bytes = bytes.len() as u64;
            if bytes.len() >= 8 && bytes[..4] == SNAP_MAGIC {
                let expected_crc = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
                let actual_crc = crc32fast::hash(&bytes[8..]);
                if expected_crc != actual_crc {
                    return Err(KError::io("snapshot checksum mismatch in kix.snap"));
                }
                let snap: SnapshotData = postcard::from_bytes(&bytes[8..])
                    .map_err(|e| KError::io(format!("corrupt snapshot: {e}")))?;
                install_snapshot_into(&mut ws, snap);
            }
        }

        // 3. Replay `kix.wal`, discarding any torn trailing frame from an unclean crash
        let wal_path = dir.join("kix.wal");
        let mut wal_file = OpenOptions::new().create(true).read(true).write(true).open(&wal_path)?;
        let mut raw = Vec::new();
        wal_file.read_to_end(&mut raw)?;

        let mut offset = 0usize;
        let mut replayed = 0u64;
        while offset + 12 <= raw.len() {
            if raw[offset..offset + 4] != WAL_MAGIC {
                // Torn / corrupt tail — truncate here
                break;
            }
            let frame_len = u32::from_le_bytes(raw[offset + 4..offset + 8].try_into().unwrap()) as usize;
            let expected_crc = u32::from_le_bytes(raw[offset + 8..offset + 12].try_into().unwrap());
            if offset + 12 + frame_len > raw.len() {
                // Incomplete tail write — truncate cleanly
                break;
            }
            let payload = &raw[offset + 12..offset + 12 + frame_len];
            if crc32fast::hash(payload) != expected_crc {
                break;
            }
            if let Ok(rec) = postcard::from_bytes::<WalRecord>(payload) {
                if rec.ts > ws.clock {
                    apply_record(&mut ws, &rec)?;
                }
                replayed += 1;
                offset += 12 + frame_len;
            } else {
                break;
            }
        }

        if offset < raw.len() {
            wal_file.set_len(offset as u64)?;
            wal_file.seek(SeekFrom::End(0))?;
            wal_file.sync_all()?;
        } else {
            wal_file.seek(SeekFrom::End(0))?;
        }

        let p = Persist {
            dir: dir.to_path_buf(),
            _lock: lock,
            wal_file,
            wal_records: replayed,
            wal_bytes: offset as u64,
            snap_bytes,
            log_forces: 0,
            opts,
        };
        Ok((p, ws))
    }

    pub fn append_record(&mut self, rec: &WalRecord, force_sync: bool) -> KResult<()> {
        let payload = postcard::to_allocvec(rec).map_err(|e| KError::io(format!("WAL serialize failed: {e}")))?;
        let crc = crc32fast::hash(&payload);
        let mut frame = Vec::with_capacity(12 + payload.len());
        frame.extend_from_slice(&WAL_MAGIC);
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc.to_le_bytes());
        frame.extend_from_slice(&payload);

        self.wal_file.write_all(&frame)?;
        if force_sync && self.opts.sync_wal {
            self.wal_file.sync_data()?;
            self.log_forces += 1;
        }
        self.wal_records += 1;
        self.wal_bytes += frame.len() as u64;
        Ok(())
    }

    pub fn flush(&mut self) -> KResult<()> {
        self.wal_file.sync_data()?;
        self.log_forces += 1;
        Ok(())
    }

    pub fn checkpoint(&mut self, ws: &Workspace) -> KResult<()> {
        let snap = export_snapshot(ws, ws.clock);
        let payload = postcard::to_allocvec(&snap).map_err(|e| KError::io(format!("snapshot serialize failed: {e}")))?;
        let crc = crc32fast::hash(&payload);
        let mut out = Vec::with_capacity(8 + payload.len());
        out.extend_from_slice(&SNAP_MAGIC);
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&payload);

        let tmp_path = self.dir.join("kix.snap.tmp");
        let final_path = self.dir.join("kix.snap");
        {
            let mut f = File::create(&tmp_path)?;
            f.write_all(&out)?;
            f.sync_all()?;
        }
        fs::rename(&tmp_path, &final_path)?;

        // Truncate WAL now that snapshot is durable
        self.wal_file.set_len(0)?;
        self.wal_file.seek(SeekFrom::Start(0))?;
        self.wal_file.sync_all()?;
        self.wal_records = 0;
        self.wal_bytes = 0;
        self.snap_bytes = out.len() as u64;
        self.log_forces += 1;
        Ok(())
    }
}

fn mismatch(name: SymId) -> KError {
    KError::io(format!("the log changes '{}', whose value is not the one it was made from", resolve(name)))
}

pub fn apply_record(ws: &mut Workspace, rec: &WalRecord) -> KResult<()> {
    for op in &rec.ops {
        match op {
            LogOp::SetVar { name, val } => {
                ws.views.remove(name);
                ws.vars.insert(*name, val.clone());
            }
            LogOp::DropVar { name } => {
                ws.vars.remove(name);
                ws.views.remove(name);
            }
            LogOp::SetView { def } => {
                ws.vars.remove(&def.name);
                ws.views.insert(def.name, def.clone());
            }
            LogOp::SetDist { table, mode } => {
                ws.dist.insert(*table, mode.clone());
            }
            LogOp::ChangeVar { name, changes } => {
                ws.views.remove(name);
                if !ws.vars.get_mut(name).is_some_and(|v| delta::patch(v, changes)) {
                    return Err(mismatch(*name));
                }
            }
            LogOp::RemoveRows { name, removed } => {
                ws.views.remove(name);
                if !ws.vars.get_mut(name).is_some_and(|v| delta::remove(v, removed)) {
                    return Err(mismatch(*name));
                }
            }
        }
    }
    ws.invalidate_views();
    ws.clock = ws.clock.max(rec.ts);
    ws.epoch += 1;
    Ok(())
}

pub fn export_snapshot(ws: &Workspace, ts: u64) -> SnapshotData {
    SnapshotData {
        ts,
        vars: ws
            .vars
            .iter()
            .filter(|(&k, _)| !crate::engine::is_ephemeral_var(k))
            .map(|(&k, v)| (k, v.clone()))
            .collect(),
        views: ws.views.values().cloned().collect(),
        dist: ws.dist.iter().map(|(&k, m)| (k, m.clone())).collect(),
    }
}

pub fn install_snapshot_into(ws: &mut Workspace, snap: SnapshotData) {
    ws.vars.clear();
    ws.unlogged.clear();
    ws.views.clear();
    ws.dist.clear();
    for (k, v) in snap.vars {
        ws.vars.insert(k, v);
    }
    for v in snap.views {
        ws.views.insert(v.name, v);
    }
    for (k, m) in snap.dist {
        ws.dist.insert(k, m);
    }
    ws.clock = snap.ts;
    ws.epoch += 1;
}
