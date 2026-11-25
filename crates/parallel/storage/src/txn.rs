//! A shard's part in a change that spans shards (two-phase commit).
//!
//! **Prepare** works the change out, to find any error in it, and keeps the request in a variable: logged and replicated
//! like any other, so a restart or a new leader still holds it. Until it is committed or abandoned the shard takes no
//! other change. **Commit** makes the change and drops the request in one log record. One shard of the change, its
//! **primary**, also remembers that it committed: it is committed first, so whether it did decides for all of them.
use crate::{load_changes, mutation_changes, Changes};
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
        };
        changes.retain(|(name, _)| !new.iter().any(|n| n.0 == *name));
        changes.extend(new);
    }
    Ok((changes, answers))
}

pub fn run(server: &Server, conn: &mut Connection, req: TxnReq) -> KResult<TxnReply> {
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
            let (_, answers) = work_out(conn, &ops)?;
            let bytes = postcard::to_allocvec(&Pending { txid, primary, ops }).map_err(|e| KError::new("cluster", e.to_string()))?;
            conn.set_vars(vec![(intern(PENDING), Some(K::Vector(ColVec::Byte(Attr::None, Arc::new(bytes)))))])?;
            TxnReply::Prepared(answers)
        }
        TxnReq::Commit { txid, primary, forget } => match pending.filter(|p| p.txid == txid) {
            Some(p) => {
                let (mut changes, _) = work_out(conn, &p.ops)?;
                changes.push((intern(PENDING), None));
                if primary {
                    let remembered = done.into_iter().filter(|&id| !forget.contains(&(id as u64)));
                    changes.push((intern(DONE), longs(remembered.chain([txid as i64]).collect())));
                }
                conn.set_vars(changes)?;
                TxnReply::Done
            }
            // a primary asked to commit what it neither holds nor did has abandoned it
            None if primary && !done.contains(&(txid as i64)) => return Err(KError::new("cluster", "the change was abandoned")),
            None => TxnReply::Done,
        },
        TxnReq::Abort { txid } => {
            if pending.is_some_and(|p| p.txid == txid) {
                conn.set_vars(vec![(intern(PENDING), None)])?;
            }
            TxnReply::Outcome { committed: done.contains(&(txid as i64)) }
        }
        TxnReq::Pending => TxnReply::Pending(pending.map(|p| (p.txid, p.primary))),
    })
}
