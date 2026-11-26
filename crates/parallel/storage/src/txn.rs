//! A shard's part in a change that spans shards (two-phase commit).
//!
//! **Prepare** works the change out, to find any error in it, and keeps the request in a variable: logged and replicated
//! like any other, so a restart or a new leader still holds it. Until it is committed or abandoned the shard takes no
//! other change. **Commit** makes the change and drops the request in one log record. One shard of the change, its
//! **primary**, also remembers that it committed: it is committed first, so whether it did decides for all of them.
use crate::{load_changes, move_changes, mutation_changes, staged_value, Changes};
use kixdb::engine::Write;
use kix_shared_ces::wire::{TxnOp, TxnReply, TxnReq};
use kixdb::ipc::encode_k_payload;
use kixdb::sym::intern;
use kixdb::{Attr, ColVec, Connection, KError, KResult, Server, K};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// The request a shard holds between prepare and commit.
const PENDING: &str = ".kix.txn.pending";
/// On a primary, the changes it committed that other shards may still ask about.
const DONE: &str = ".kix.txn.done";

#[derive(Serialize, Deserialize)]
struct Pending {
    txid: u64,
    primary: u32,
    ops: Vec<TxnOp>,
}

fn variable(server: &Server, name: &str) -> Option<K> {
    server.read_workspace(|ws| ws.vars.get(&intern(name)).cloned())
}

fn held(server: &Server) -> KResult<Option<Pending>> {
    match variable(server, PENDING) {
        Some(K::Vector(ColVec::Byte(_, bytes))) => postcard::from_bytes(&bytes).map(Some).map_err(|e| KError::new("cluster", format!("the prepared change is damaged: {e}"))),
        _ => Ok(None),
    }
}

fn busy() -> KError {
    KError::new("cluster", "the shard holds a change that is waiting to be committed or abandoned")
}

/// Refuse a change made on its own while one is prepared (temporary tables are not part of any).
pub fn check_free(server: &Server, table: &str) -> KResult<()> {
    match held(server)? {
        Some(_) if !table.starts_with(".kix.tmp.") => Err(busy()),
        _ => Ok(()),
    }
}

/// What preparing worked out, kept for the commit: the change's name and its writes. (A shard takes no other change in
/// between; one that did not prepare, as after a restart or a change of leader, works them out again.)
pub type Prepared = std::sync::Mutex<Option<(u64, Changes)>>;

/// What the operations would change, each seeing those before it, and the answers of the mutations.
fn work_out(conn: &mut Connection, ops: &[TxnOp]) -> KResult<(Changes, Vec<Vec<u8>>)> {
    let (mut changes, mut answers): (Changes, _) = (vec![], vec![]);
    for op in ops {
        let new = match op {
            TxnOp::Load(req) => load_changes(conn, &changes, req)?,
            TxnOp::Mutate(req) => {
                let (schema, new) = mutation_changes(conn, req)?;
                answers.push(encode_k_payload(&schema));
                new
            }
            TxnOp::Move(req) => move_changes(conn, &changes, req)?,
        };
        for (name, write) in new {
            if changes.iter().any(|c| c.0 == name) {
                // one write a name: the two make its new value
                let dropped = matches!(write, Write::Value(None));
                changes.push((name, write));
                let value = if dropped { None } else { Some(staged_value(conn, &changes, kixdb::sym::resolve(name))?) };
                changes.retain(|c| c.0 != name);
                changes.push((name, Write::Value(value)));
            } else {
                changes.push((name, write));
            }
        }
    }
    Ok((changes, answers))
}

pub fn run(server: &Server, conn: &mut Connection, prepared: &Prepared, req: TxnReq) -> KResult<TxnReply> {
    let keep = |value| *prepared.lock().unwrap_or_else(|e| e.into_inner()) = value;
    let pending = held(server)?;
    let done: Vec<i64> = match variable(server, DONE) {
        Some(K::Vector(ColVec::Long(_, ids))) => (*ids).clone(),
        _ => vec![],
    };
    let longs = |ids: Vec<i64>| Some(K::Vector(ColVec::Long(Attr::None, Arc::new(ids))));
    Ok(match req {
        TxnReq::Prepare { txid, primary, ops } => {
            if pending.is_some_and(|p| p.txid != txid) {
                return Err(busy());
            }
            let (changes, answers) = work_out(conn, &ops)?;
            let bytes = postcard::to_allocvec(&Pending { txid, primary, ops }).map_err(|e| KError::new("cluster", e.to_string()))?;
            conn.set_vars(vec![(intern(PENDING), Some(K::Vector(ColVec::Byte(Attr::None, Arc::new(bytes)))))])?;
            keep(Some((txid, changes)));
            TxnReply::Prepared(answers)
        }
        TxnReq::Commit { txid, primary, forget } => match pending.filter(|p| p.txid == txid) {
            Some(p) => {
                let kept = prepared.lock().unwrap_or_else(|e| e.into_inner()).take().filter(|k| k.0 == txid);
                let mut changes = match kept {
                    Some((_, changes)) => changes,
                    None => work_out(conn, &p.ops)?.0,
                };
                changes.push((intern(PENDING), Write::Value(None)));
                if primary {
                    let remembered = done.into_iter().filter(|&id| !forget.contains(&(id as u64)));
                    changes.push((intern(DONE), Write::Value(longs(remembered.chain([txid as i64]).collect()))));
                }
                conn.write_vars(changes)?;
                TxnReply::Done
            }
            // a primary asked to commit what it neither holds nor did has abandoned it
            None if primary && !done.contains(&(txid as i64)) => return Err(KError::new("cluster", "the change was abandoned")),
            None => TxnReply::Done,
        },
        TxnReq::Abort { txid } => {
            if pending.is_some_and(|p| p.txid == txid) {
                conn.set_vars(vec![(intern(PENDING), None)])?;
                keep(None);
            }
            TxnReply::Outcome { committed: done.contains(&(txid as i64)) }
        }
        TxnReq::Pending => TxnReply::Pending(pending.map(|p| (p.txid, p.primary))),
    })
}
