//! Changes that are made on every shard they touch or on none (two-phase commit).
//!
//! Each shard first **prepares** its part: it works it out and stores it. When all have, the **primary** (the first of
//! them) is told to commit, and then the others. Whether the primary committed is the whole answer: if the control node
//! or a shard fails part way, [`Inner::resolve`] asks the primary to abandon the change; a primary that committed says so
//! and the others are committed, otherwise all abandon it. Nothing need be kept on the control node.
use crate::client::UNKNOWN;
use crate::Inner;
use futures::future::join_all;
use kix_shared_ces::wire::{TxnOp, TxnReply, TxnReq};
use kixdb::ipc::decode_k_payload;
use kixdb::sym::intern;
use kixdb::{KError, KResult, K};
use std::collections::BTreeMap;
use std::hash::BuildHasher;
use std::time::{Duration, Instant};

/// How long after a failed attempt to resolve before statements try again.
const RETRY: Duration = Duration::from_secs(5);

fn answer(bytes: &[u8]) -> KResult<K> {
    decode_k_payload(bytes, 0).map(|(k, _)| k)
}

impl Inner {
    /// Note that some shard may hold a change waiting to be resolved.
    fn doubt(&self) {
        *self.unsettled.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
    }

    /// Whether some change may be waiting for a shard.
    pub(crate) fn in_doubt(&self) -> bool {
        self.unsettled.lock().unwrap_or_else(|e| e.into_inner()).is_some()
    }

    /// Whether it is time to resolve changes that may be waiting.
    pub(crate) fn unsettled(&self) -> bool {
        self.unsettled.lock().unwrap_or_else(|e| e.into_inner()).is_some_and(|due| due <= Instant::now())
    }

    fn txn(&self, shard: usize, req: &TxnReq) -> KResult<TxnReply> {
        self.wait(self.shard(shard)?.txn(req))
    }

    /// Every shard of a change committed it: its primary is told with its next commit.
    fn finished(&self, primary: usize, txid: u64) {
        self.finished.lock().unwrap_or_else(|e| e.into_inner()).push((primary, txid));
    }

    /// Finish or abandon every change the shards hold. The caller has the cluster to itself.
    pub(crate) fn resolve(&self) {
        self.forget_counts(None);
        let shards = self.shards();
        let held = self.wait(join_all(shards.iter().map(|s| s.txn(&TxnReq::Pending))));
        let mut settled = held.iter().all(Result::is_ok);
        // the shards holding each change
        let mut changes: BTreeMap<(u64, u32), Vec<usize>> = BTreeMap::new();
        for (shard, reply) in held.iter().enumerate() {
            if let Ok(TxnReply::Pending(Some(change))) = reply {
                changes.entry(*change).or_default().push(shard);
            }
        }
        for ((txid, primary), holders) in changes {
            let Ok(TxnReply::Outcome { committed }) = self.txn(primary as usize, &TxnReq::Abort { txid }) else {
                settled = false;
                continue;
            };
            let end = if committed { TxnReq::Commit { txid, primary: false, forget: vec![] } } else { TxnReq::Abort { txid } };
            // (every holder is told, also after one could not be)
            let ended = holders.iter().filter(|&&s| s != primary as usize && self.txn(s, &end).is_err()).count() == 0;
            settled &= ended;
            if committed && ended {
                self.finished(primary as usize, txid);
            }
        }
        *self.unsettled.lock().unwrap_or_else(|e| e.into_inner()) = (!settled).then(|| Instant::now() + RETRY);
    }

    /// Drop the row counts of the tables the operations change (of all, without any).
    fn forget_counts(&self, ops: Option<&[(usize, TxnOp)]>) {
        let mut rows = self.rows.lock().unwrap_or_else(|e| e.into_inner());
        match ops {
            Some(ops) => ops.iter().filter(|o| o.1.changes()).for_each(|(_, op)| {
                rows.remove(&intern(op.table()));
            }),
            None => rows.clear(),
        }
    }

    /// Make the operations, each on its shard: all of them or none. Returns the answers of the mutations, in order. The
    /// caller has the cluster to itself.
    pub(crate) fn commit(&self, ops: Vec<(usize, TxnOp)>) -> KResult<Vec<K>> {
        if ops.is_empty() {
            return Ok(vec![]);
        }
        if self.unsettled() {
            self.resolve();
        }
        self.forget_counts(Some(&ops));
        let mut shards: BTreeMap<usize, Vec<(usize, TxnOp)>> = BTreeMap::new();
        let changing = ops.iter().filter(|(_, op)| op.changes()).count();
        for (n, (shard, op)) in ops.into_iter().enumerate() {
            shards.entry(shard).or_default().push((n, op));
        }
        // one change on one shard is all or nothing by itself
        if shards.len() == 1 && changing <= 1 {
            let mut answers = vec![];
            for (shard, ops) in shards {
                for (_, op) in ops {
                    let done = match &op {
                        TxnOp::Load(req) => self.wait(self.shard(shard)?.load(req)),
                        TxnOp::Mutate(req) => self.wait(self.shard(shard)?.mutate(req)).map(|k| answers.push(k)),
                        // (a change of the number of shards writes the layout too, so it is never one alone)
                        TxnOp::Move(_) => Err(KError::new("cluster", "rows move only as part of a larger change")),
                    };
                    done.inspect_err(|_| self.doubt())?;
                }
            }
            return Ok(answers);
        }
        let txid = std::collections::hash_map::RandomState::new().hash_one(Instant::now());
        let primary = *shards.keys().next().expect("there are operations");
        // phase one: every shard stores its part
        let prepares: Vec<(usize, TxnReq)> = shards
            .iter()
            .map(|(&s, ops)| (s, TxnReq::Prepare { txid, primary: primary as u32, ops: ops.iter().map(|o| o.1.clone()).collect() }))
            .collect();
        let held = self.shards();
        let replies = self.wait(join_all(prepares.iter().map(|(s, req)| held[*s].txn(req))));
        // the mutations' answers, by the number of their operation
        let mut answers: BTreeMap<usize, Vec<u8>> = BTreeMap::new();
        let mut failed = None;
        for ((_, ops), reply) in shards.iter().zip(replies) {
            match reply {
                Ok(TxnReply::Prepared(of_shard)) => {
                    let mutations = ops.iter().filter(|o| matches!(o.1, TxnOp::Mutate(_)));
                    answers.extend(mutations.map(|o| o.0).zip(of_shard));
                }
                Ok(_) => failed = failed.or(Some(KError::new("cluster", "a shard did not prepare the change"))),
                Err(e) => failed = failed.or(Some(e)),
            }
        }
        let abandon = |from: usize| {
            for (&s, _) in shards.iter().skip(from) {
                if self.txn(s, &TxnReq::Abort { txid }).is_err() {
                    self.doubt();
                }
            }
        };
        if let Some(e) = failed {
            abandon(0);
            // whatever became of the request, no shard will commit
            return Err(KError::new(&e.signal, e.detail.replace(UNKNOWN, "; no shard made the change")));
        }
        // phase two: the primary decides
        let forget: Vec<u64> = self.finished.lock().unwrap_or_else(|e| e.into_inner()).iter().filter(|f| f.0 == primary).map(|f| f.1).collect();
        if let Err(e) = self.txn(primary, &TxnReq::Commit { txid, primary: true, forget: forget.clone() }) {
            match self.txn(primary, &TxnReq::Abort { txid }) {
                Ok(TxnReply::Outcome { committed: true }) => {}
                Ok(_) => {
                    abandon(1);
                    return Err(e);
                }
                Err(_) => {
                    self.doubt();
                    return Err(KError::new(&e.signal, format!("{}; the change may or may not be made", e.detail)));
                }
            }
        }
        self.finished.lock().unwrap_or_else(|e| e.into_inner()).retain(|f| f.0 != primary || !forget.contains(&f.1));
        let commit = TxnReq::Commit { txid, primary: false, forget: vec![] };
        let others = self.wait(join_all(shards.keys().skip(1).map(|&s| held[s].txn(&commit))));
        if others.iter().all(Result::is_ok) {
            self.finished(primary, txid);
        } else {
            // the change is made: the shards that have not heard are told when they can be
            self.doubt();
        }
        answers.values().map(|b| answer(b)).collect()
    }
}
