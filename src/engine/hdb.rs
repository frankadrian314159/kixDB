//! Databases on disk: enumerating a table's symbols (`.Q.en`, `.Q.ens`), writing partitions (`.Q.dpft`, `.Q.dpfts`,
//! `.Q.chk`) and what is mounted of them (`.Q.view`, `.Q.pv`, `.Q.pf`, `.Q.pt`).
use crate::engine::{ops, Engine};
use crate::error::{KError, KResult};
use crate::storage::{self, PartitionSlice};
use crate::sym::{intern, resolve, SymId};
use crate::types::*;
use std::collections::HashSet;
use std::fs;
use std::sync::Arc;

fn symbols(names: Vec<SymId>) -> K {
    K::Vector(ColVec::Sym(Attr::None, Arc::new(names)))
}

fn general(items: Vec<K>) -> K {
    K::Vector(ColVec::General(Attr::None, Arc::new(items)))
}

/// The symbol of the table or other name `x` gives.
fn name_of(x: &K) -> KResult<SymId> {
    match x {
        K::Sym(s) => Ok(*s),
        _ => Err(KError::ty("expected a name (a symbol)")),
    }
}

impl Engine<'_> {
    /// `.Q.en[dir; table]` (`.Q.ens` names the domain, which is `sym` otherwise): the table with its symbol columns
    /// enumerated from the list in `dir/domain`, to which the symbols it lacks are added. The global of that name is
    /// set to the list.
    pub(crate) fn enumerate_symbols(&mut self, dir: &K, table: &K, domain: SymId) -> KResult<K> {
        let directory = storage::path_of(self.ws, dir)?;
        fs::create_dir_all(&directory)?;
        let file = directory.join(resolve(domain));
        let mut list: Vec<SymId> = match file.exists().then(|| storage::read_flat_k(&file)).transpose()? {
            Some(K::Vector(held)) => match held.plain() {
                ColVec::Sym(_, names) => (**names).clone(),
                _ if held.is_empty() => vec![],
                _ => return Err(KError::ty(format!("{} does not hold a list of symbols", file.display()))),
            },
            Some(_) => return Err(KError::ty(format!("{} does not hold a list of symbols", file.display()))),
            None => vec![],
        };
        let mut seen: HashSet<SymId> = list.iter().copied().collect();
        let tag = Tag { domain, source: Source::List };
        let mut enumerated = |flip: &Flip| {
            let vals = flip.vals.iter().map(|column| match column {
                ColVec::Sym(_, names) => {
                    list.extend(names.iter().copied().filter(|&s| seen.insert(s)));
                    ColVec::Enum(tag, Box::new(column.clone()))
                }
                other => other.clone(),
            });
            Flip { cols: flip.cols.clone(), vals: vals.collect() }
        };
        let made = match table {
            K::Table(t) => K::Table(Arc::new(enumerated(t))),
            K::KeyedTable(t) => K::KeyedTable(Arc::new(KeyedTable { keys: enumerated(&t.keys), vals: enumerated(&t.vals) })),
            _ => return Err(KError::ty(".Q.en takes a table")),
        };
        let list = symbols(list);
        storage::write_flat_k(&file, &list)?;
        self.set_globals(vec![(domain, Some(list))])?;
        Ok(made)
    }

    /// `.Q.dpft[dir; partition; field; table]` (`.Q.dpfts` names the domain): write the global `table`, sorted and
    /// parted by `field` and enumerated, to `dir/partition/table/`, and mount `dir` again.
    pub(crate) fn write_partition(&mut self, dir: &K, partition: &K, field: &K, table: &K, domain: SymId) -> KResult<K> {
        let (field, name) = (name_of(field)?, name_of(table)?);
        let K::Table(sorted) = ops::table_sort_op(&K::Sym(field), &self.get_var(name)?, false)? else {
            return Err(KError::ty(".Q.dpft requires an unkeyed table"));
        };
        let cols: Vec<SymId> = [field].into_iter().chain(sorted.cols.iter().copied().filter(|&c| c != field)).collect();
        let mut vals = cols.iter().map(|&c| sorted.col(c).cloned().ok_or_else(|| KError::value(resolve(c)))).collect::<KResult<Vec<_>>>()?;
        vals[0] = vals[0].with_attr(Attr::Parted)?;
        let K::Table(written) = self.enumerate_symbols(dir, &K::Table(Arc::new(Flip::new(cols, vals)?)), domain)? else {
            return Err(KError::ty(".Q.dpft requires a table"));
        };
        let directory = match partition {
            K::Date(d) => format_date(*d),
            K::Month(m) => format_month(*m).trim_end_matches('m').to_string(),
            other => other.as_i64()?.to_string(),
        };
        let base = storage::path_of(self.ws, dir)?;
        storage::write_splayed_table(&base.join(directory).join(resolve(name)), &written)?;
        storage::mount_hdb_dir(self.ws, &base)?;
        Ok(K::Sym(name))
    }

    /// `.Q.chk dir`: give each partition the tables that the last one has and it lacks, empty. For each partition, the
    /// result holds its directory if it was filled, else `()`.
    pub(crate) fn check_partitions(&mut self, dir: &K) -> KResult<K> {
        let partitions = storage::partition_dirs(self.ws, &storage::path_of(self.ws, dir)?)?;
        let Some(last) = partitions.last() else { return Ok(general(vec![])) };
        let mut templates = Vec::new();
        for entry in fs::read_dir(&last.3)? {
            let path = entry?.path();
            if path.join(".d").exists() {
                let table = storage::read_splayed_table(&path, None)?;
                let empty = Flip { cols: table.cols, vals: table.vals.iter().map(|v| v.take(0)).collect() };
                templates.push((path.file_name().map(|n| n.to_os_string()).unwrap_or_default(), empty));
            }
        }
        let mut report = Vec::with_capacity(partitions.len());
        for (_, _, _, partition) in &partitions {
            let lacking: Vec<_> = templates.iter().filter(|t| !partition.join(&t.0).join(".d").exists()).collect();
            for (name, empty) in &lacking {
                storage::write_splayed_table(&partition.join(name), empty)?;
            }
            let filled = intern(&format!(":{}", partition.display()));
            report.push(if lacking.is_empty() { general(vec![]) } else { symbols(vec![filled]) });
        }
        Ok(general(report))
    }

    /// `.Q.view values`: queries see only those partitions; `.Q.view[]` shows them all again.
    pub(crate) fn view_partitions(&mut self, wanted: &K) -> KResult<K> {
        let shown = |s: &PartitionSlice| match wanted {
            K::Null => true,
            K::Vector(values) => (0..values.len()).any(|i| values.get_atom(i).k_cmp(&s.part_val).is_eq()),
            one => one.k_cmp(&s.part_val).is_eq(),
        };
        self.ws.writable()?;
        self.ws.invalidate_views();
        for table in self.ws.partitioned.values_mut() {
            let mut all = std::mem::take(&mut table.slices);
            all.append(&mut table.hidden);
            all.sort_by(|a, b| a.part_val.k_cmp(&b.part_val));
            (table.slices, table.hidden) = all.into_iter().partition(&shown);
        }
        Ok(K::Null)
    }

    /// The values of the partitions mounted, sorted: those shown, or (`all`) every one.
    pub(crate) fn partition_values(&self, column: Option<SymId>, all: bool) -> Option<K> {
        let tables: Vec<_> = self.ws.partitioned.values().filter(|t| column.is_none_or(|c| t.part_col == c)).collect();
        if tables.is_empty() {
            return None;
        }
        let hidden = |t: &&storage::PartitionedTable| if all { t.hidden.clone() } else { vec![] };
        let mut values: Vec<K> = tables.iter().flat_map(|t| t.slices.iter().cloned().chain(hidden(t))).map(|s| s.part_val).collect();
        values.sort_by(K::k_cmp);
        values.dedup_by(|a, b| a.k_cmp(b).is_eq());
        Some(K::Vector(ColVec::from_atoms(values)))
    }

    /// `.Q.pv`, `.Q.PV` (partition values: shown, all), `.Q.pf` (their column) and `.Q.pt` (the partitioned tables).
    pub(crate) fn partition_info(&self, name: &str) -> Option<K> {
        match name {
            ".Q.pv" | ".Q.PV" => self.partition_values(None, name == ".Q.PV"),
            ".Q.pf" => self.ws.partitioned.values().next().map(|t| K::Sym(t.part_col)),
            ".Q.pt" => {
                let mut names: Vec<SymId> = self.ws.partitioned.keys().copied().collect();
                names.sort_by_key(|&n| resolve(n));
                Some(symbols(names))
            }
            _ => None,
        }
    }
}
