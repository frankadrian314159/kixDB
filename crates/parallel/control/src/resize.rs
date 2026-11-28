//! Changing the number of shards (`ADD SHARD`, `REMOVE SHARD`): rows move to where the new slot map puts them while
//! statements wait.
//!
//! Which shards there are and which of the 1,024 slots each owns, the **layout**, is kept on shard 0 from the first
//! change on, where any control node can find it. A change first copies the rows that move (into the tables of a new
//! shard, or beside those of a shard that stays), which alters nothing that is read. Then one commit across the shards
//! removes the moved rows where they were, merges the copies in and writes the new layout. So the number of shards
//! changes whole or not at all.
use crate::catalog::{rekey, unkey, Names, Table};
use crate::client::ShardClient;
use crate::exec::{augmented, with_ordinals};
use crate::plan::Layout;
use crate::{cluster_error, Inner, Topology};
use kix_shared_ces::route::{rebalanced, SLOTS};
use kix_shared_ces::wire::{moving_var, LoadMode, LoadReq, MoveReq, TxnOp, Work, JOINING_VAR, LAYOUT_VAR, ORD_COL};
use kix_shared_ces::ShardInfo;
use kixdb::ast::{ColSpec, Expr, QSql, QSqlKind};
use kixdb::engine::DistMode;
use kixdb::parser::parse_script;
use kixdb::sym::{intern, resolve, SymId};
use futures::future::join_all;
use kixdb::{Attr, ColVec, KError, KResult, K};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// The layout as shard 0 holds it.
#[derive(Serialize, Deserialize)]
pub(crate) struct Held {
    /// How many times the number of shards has changed.
    pub(crate) resizes: u64,
    pub(crate) slots: Vec<u32>,
    pub(crate) shards: Vec<ShardInfo>,
    /// Every shard is known to have made the change that brought this layout.
    done: bool,
}

fn longs(v: Vec<i64>) -> K {
    K::Vector(ColVec::Long(Attr::None, Arc::new(v)))
}

/// The write of the layout of `topology` on shard 0.
fn layout_load(topology: &Topology, done: bool) -> KResult<LoadReq> {
    let held = Held { resizes: topology.resizes, slots: (*topology.slots).clone(), shards: topology.info(), done };
    let bytes = postcard::to_allocvec(&held).map_err(|e| cluster_error(e.to_string()))?;
    Ok(LoadReq::new(LAYOUT_VAR, LoadMode::Replace, &K::Vector(ColVec::Byte(Attr::None, Arc::new(bytes))), None))
}

/// `select cols from <table with its ordinals> where ...`: all columns if none are named.
fn select(table: SymId, cols: &[SymId], where_clauses: Vec<Expr>) -> Work {
    let cols = cols.iter().map(|&c| ColSpec { name: Some(c), expr: Expr::Var(c) }).collect();
    let from = Box::new(augmented(table));
    Work::Eval(Box::new(Expr::Query(Box::new(QSql { kind: QSqlKind::Select, distinct: false, limit: None, cols, by: None, from, where_clauses }))))
}

/// How rows are added to a slice of the table.
fn adding(t: &Table) -> LoadMode {
    if t.nkeys > 0 {
        LoadMode::Upsert
    } else {
        LoadMode::Append
    }
}

impl Inner {
    /// Before a statement: have the layout that shard 0 holds, and finish or abandon changes left half made.
    pub(crate) fn settle(&self) -> KResult<()> {
        if self.current()? && !self.layout_unsure.load(Ordering::SeqCst) && !self.unsettled() {
            return Ok(());
        }
        let _alone = self.gate.write().unwrap_or_else(|e| e.into_inner());
        self.settle_alone()
    }

    /// `settle`, for a caller that has the cluster to itself.
    fn settle_alone(&self) -> KResult<()> {
        self.take_over()?;
        if self.group.get().is_some() {
            self.finish_pending()?;
        }
        if self.layout_unsure.load(Ordering::SeqCst) {
            self.adopt_layout()?;
        }
        if self.unsettled() {
            self.resolve();
        }
        // (nodes that read under a permit learn where the rows are from the record)
        if self.group.get().is_some() {
            self.record()?;
        }
        Ok(())
    }

    /// The layout that shard 0 holds, if it holds one.
    pub(crate) fn fetch_layout(&self) -> KResult<Option<Held>> {
        let query = Work::Q(format!("@[value; `{LAYOUT_VAR}; 0#0x00]"));
        match self.wait(self.shard(0)?.query(query))? {
            K::Vector(ColVec::Byte(_, bytes)) if !bytes.is_empty() => {
                postcard::from_bytes(&bytes).map(Some).map_err(|e| cluster_error(format!("the layout on shard 0 is damaged: {e}")))
            }
            _ => Ok(None),
        }
    }

    /// Take up the layout that shard 0 holds, if it holds one. A change of the number of shards that not every shard is
    /// known to have made is finished first: until then nothing is read, as rows could be seen in two places.
    pub(crate) fn adopt_layout(&self) -> KResult<()> {
        if let Some(held) = self.fetch_layout()? {
            self.set_topology(self.topology_of(&held.shards, held.slots, held.resizes)?);
            if !held.done {
                self.resolve();
                if self.in_doubt() {
                    return Err(cluster_error("a change of the number of shards is not finished: a shard does not answer"));
                }
                self.wait(self.shard(0)?.load(&layout_load(&self.topology(), true)?))?;
            }
        }
        self.layout_unsure.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// A connection to the shard at the comma-separated `addresses`.
    pub(crate) fn new_shard(&self, id: u32, addresses: &str) -> KResult<Arc<ShardClient>> {
        let _in_runtime = self.runtime.enter();
        let shard = ShardClient::new(id, addresses, self.tls.clone())?.fenced_by(self.term.clone());
        shard.set_limit(self.request_limit());
        Ok(Arc::new(shard))
    }

    /// Add the shard at `add`, or remove the last one.
    pub(crate) fn resize(&self, add: Option<&str>) -> KResult<()> {
        let _alone = self.gate.write().unwrap_or_else(|e| e.into_inner());
        self.settle_alone()?;
        if self.in_doubt() {
            return Err(cluster_error("a change is waiting for a shard: change the number of shards when every shard answers"));
        }
        let before = self.topology();
        let n = before.shards.len();
        let mut after = Topology { resizes: before.resizes + 1, ..before.clone() };
        match add {
            Some(addresses) if before.shards.iter().any(|s| s.shares(addresses)) => {
                return Err(KError::domain(format!("{addresses} is already a shard of this cluster")));
            }
            Some(addresses) if n < SLOTS => after.shards.push(self.new_shard(n as u32, addresses)?),
            None if n > 1 => drop(after.shards.pop()),
            _ => return Err(KError::domain(format!("a cluster has from 1 to {SLOTS} shards"))),
        }
        after.slots = Arc::new(rebalanced(&before.slots, after.shards.len()));
        // while rows move, every shard that takes part can be reached
        if add.is_some() {
            self.set_topology(Topology { shards: after.shards.clone(), ..before.clone() });
        }
        let committed = self.copy_rows(&before, &after).and_then(|ops| self.commit(ops));
        // what shard 0 now holds says whether the change was made
        self.set_topology(before);
        self.layout_unsure.store(true, Ordering::SeqCst);
        let adopted = self.adopt_layout();
        if self.topology().resizes != after.resizes {
            // (copies made beside the tables of shards that stay are thrown away, where they can be)
            // (all at once, so that a shard that does not answer is waited for once)
            let drop = |t: &Table| LoadReq::new(moving_var(resolve(t.name)), LoadMode::Drop, &K::Null, None);
            let (shards, drops): (_, Vec<LoadReq>) = (self.shards(), self.tables_to_copy().iter().map(drop).collect());
            self.wait(join_all(shards.iter().flat_map(|s| drops.iter().map(|d| s.load(d)))));
            committed?;
            adopted?;
            return Err(cluster_error("the number of shards did not change"));
        }
        self.save_state()?;
        adopted
    }

    /// The distributed tables, replicated ones first, each after those its columns refer to.
    fn tables_to_copy(&self) -> Vec<Table> {
        fn add(names: &Names, t: Table, seen: &mut BTreeSet<SymId>, ordered: &mut Vec<Table>) {
            if seen.insert(t.name) {
                names.domains(&t).into_iter().for_each(|d| add(names, d, seen, ordered));
                ordered.push(t);
            }
        }
        let names = Names { mirror: &self.mirror, ns: ".".into(), local: &[] };
        let (mut seen, mut ordered) = (BTreeSet::new(), vec![]);
        names.all_tables().into_iter().filter_map(|n| names.table(n)).for_each(|t| add(&names, t, &mut seen, &mut ordered));
        ordered.sort_by_key(|t| t.mode != DistMode::Replicated);
        ordered
    }

    /// Make shard `new` ready to be filled: it must be an empty shard started with that `--id`, or what is left of an
    /// earlier attempt to add it.
    fn prepare_new_shard(&self, new: usize) -> KResult<()> {
        let shard = self.shard(new)?;
        let id = self.wait(shard.status())?.shard_id;
        if id as usize != new {
            return Err(KError::domain(format!("the new shard must be started with --id {new}, not --id {id}")));
        }
        let ask = |code: String| self.wait(shard.query(Work::Q(code)));
        let tables = match ask("tables[]".into())? {
            K::Vector(ColVec::Sym(_, names)) => (*names).clone(),
            _ => vec![],
        };
        let joining = ask(format!("@[value; `{JOINING_VAR}; 0b]"))? == K::Bool(true);
        let leads = matches!(ask(format!("@[value; `{LAYOUT_VAR}; 0#0x00]"))?, K::Vector(held) if !held.is_empty());
        if leads || (!joining && !tables.is_empty()) {
            return Err(KError::domain("the new shard is not empty"));
        }
        if self.group.get().is_some() {
            self.wait(shard.fence())?;
        }
        // (a change that an earlier attempt left prepared would refuse what is loaded)
        self.resolve();
        self.wait(shard.load(&LoadReq::new(JOINING_VAR, LoadMode::Replace, &K::Bool(true), None)))?;
        for name in tables {
            self.wait(shard.load(&LoadReq::new(resolve(name), LoadMode::Drop, &K::Null, None)))?;
        }
        Ok(())
    }

    /// Copy the rows that move as the cluster goes from `before` to `after`, and give the operations that then make the
    /// change.
    fn copy_rows(&self, before: &Topology, after: &Topology) -> KResult<Vec<(usize, TxnOp)>> {
        let (old, new) = (before.shards.len(), after.shards.len());
        let batch = self.move_batch.load(Ordering::SeqCst).max(1);
        let tables = self.tables_to_copy();
        let mut ops = vec![];
        if new > old {
            self.prepare_new_shard(old)?;
        }
        for t in &tables {
            let name = resolve(t.name).to_string();
            let Some(layout) = Layout::of(t) else {
                // a replicated table: the new shard is given all of it
                if new > old {
                    let shard = self.shard(old)?;
                    self.wait(shard.load(&LoadReq::new(&name, LoadMode::Replace, &t.schema_k(), None)))?;
                    for start in (0..self.count(t)?).step_by(batch) {
                        let part = parse_script(&format!("({start}; {batch}) sublist {name}"))?.remove(0);
                        self.wait(shard.load(&LoadReq::new(&name, adding(t), &self.one(0, part)?, None)))?;
                    }
                }
                ops.extend((new..old).map(|s| (s, TxnOp::Load(LoadReq::new(&name, LoadMode::Drop, &K::Null, None)))));
                continue;
            };
            // the rows that move: their ordinals, where they are and where they go
            let mut moving: Vec<(i64, usize, usize)> = vec![];
            let mut cols = layout.cols().to_vec();
            cols.push(intern(ORD_COL));
            for from in 0..old {
                let held = self.wait(self.shard(from)?.query(select(t.name, &cols, vec![])))?;
                let (keys, _) = unkey(&held).ok_or_else(|| cluster_error("a shard did not return a table"))?;
                let (keys, ords) = with_ordinals(vec![keys])?;
                for (r, ord) in ords.into_iter().enumerate() {
                    let values: Vec<K> = keys.vals.iter().map(|c| c.get_atom(r)).collect();
                    let to = layout.place(&values, &after.slots);
                    if to != from {
                        moving.push((ord, from, to));
                    }
                }
            }
            moving.sort_unstable();
            // a new shard takes the rows into the table itself; a shard that stays, beside it
            let target = |to: usize| if to >= old { name.clone() } else { moving_var(&name) };
            let mut destinations: BTreeSet<usize> = moving.iter().map(|m| m.2).collect();
            destinations.extend(old..new);
            for &to in &destinations {
                self.wait(self.shard(to)?.load(&LoadReq::new(target(to), LoadMode::Replace, &t.schema_k(), Some(vec![]))))?;
            }
            // (in the order of the ordinals, so that a new shard holds its rows in order)
            for rows in moving.chunks(batch) {
                let mut wanted: BTreeMap<usize, Vec<i64>> = BTreeMap::new();
                rows.iter().for_each(|&(ord, from, _)| wanted.entry(from).or_default().push(ord));
                let mut parts = vec![];
                for (from, ords) in wanted {
                    let among = Expr::Binary { op: intern("in"), lhs: Box::new(Expr::Var(intern(ORD_COL))), rhs: Box::new(Expr::Lit(longs(ords))) };
                    let held = self.wait(self.shard(from)?.query(select(t.name, &[], vec![among])))?;
                    parts.push(unkey(&held).ok_or_else(|| cluster_error("a shard did not return a table"))?.0);
                }
                let (fetched, ords) = with_ordinals(parts)?;
                let goes: HashMap<i64, usize> = rows.iter().map(|&(ord, _, to)| (ord, to)).collect();
                for &to in &destinations {
                    let some: Vec<usize> = (0..ords.len()).filter(|&r| goes.get(&ords[r]) == Some(&to)).collect();
                    if !some.is_empty() {
                        let part = rekey(fetched.gather(&some), t.nkeys);
                        let numbers = some.iter().map(|&r| ords[r]).collect();
                        self.wait(self.shard(to)?.load(&LoadReq::new(target(to), adding(t), &part, Some(numbers))))?;
                    }
                }
            }
            // in the commit: shards that stay give up what left them and take in what was copied to them
            let losing: BTreeSet<usize> = moving.iter().map(|m| m.1).filter(|&from| from < new).collect();
            ops.extend(losing.into_iter().map(|me| {
                let cols = layout.cols().iter().map(|&c| resolve(c).into()).collect();
                let partitioned = matches!(layout, Layout::Part(_));
                (me, TxnOp::Move(MoveReq::Keep { table: name.clone(), cols, partitioned, slots: (*after.slots).clone(), me }))
            }));
            ops.extend(destinations.into_iter().filter(|&to| to < old).map(|to| (to, TxnOp::Move(MoveReq::Merge { table: name.clone() }))));
            ops.extend((new..old).map(|s| (s, TxnOp::Load(LoadReq::new(&name, LoadMode::Drop, &K::Null, None)))));
        }
        ops.extend((old..new).map(|s| (s, TxnOp::Load(LoadReq::new(JOINING_VAR, LoadMode::Drop, &K::Null, None)))));
        ops.push((0, TxnOp::Load(layout_load(after, false)?)));
        Ok(ops)
    }
}
