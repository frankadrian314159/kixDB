//! Databases on disk: enumerating a table's symbols (`.Q.en`, `.Q.ens`), writing partitions (`.Q.dpft`, `.Q.dpt`,
//! `.Q.hdpf`, `.Q.chk`), where they go (`.Q.par`) and what is mounted of them (`.Q.view`, `.Q.pv`, `.Q.cn` and so on).
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

/// The name of the directory of the partition with the value `x`.
fn partition_name(x: &K) -> KResult<String> {
    Ok(match x {
        K::Date(d) => format_date(*d),
        K::Month(m) => format_month(*m).trim_end_matches('m').to_string(),
        other => other.as_i64()?.to_string(),
    })
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

    /// `.Q.par[dir; partition; table]`: where the table is, or is to be, in that partition: in `dir`, or in the segment
    /// that `dir/par.txt` gives the partition to (each in turn).
    pub(crate) fn partition_path(&self, dir: &K, partition: &K, table: &K) -> KResult<K> {
        let segments = storage::segments(&storage::path_of(self.ws, dir)?);
        let root = match segments.len() {
            0 => dir.as_str().unwrap_or_default().trim_start_matches(':').trim_end_matches('/').to_string(),
            n => segments[partition.as_i64()?.rem_euclid(n as i64) as usize].clone(),
        };
        Ok(K::sym(&format!(":{root}/{}/{}", partition_name(partition)?, resolve(name_of(table)?))))
    }

    /// `.Q.dpft[dir; partition; field; table]` (`.Q.dpfts` names the domain): write the table to its partition and
    /// mount `dir` again. `.Q.dpt` and `.Q.dpts` give no field.
    pub(crate) fn write_partition(&mut self, dir: &K, partition: &K, field: Option<&K>, table: &K, domain: SymId) -> KResult<K> {
        let name = self.save_partition(dir, partition, field, table, domain)?;
        storage::mount_hdb_dir(self.ws, &storage::path_of(self.ws, dir)?)?;
        Ok(K::Sym(name))
    }

    /// Write the global `table` to its place in the partition, enumerated and, if there is a `field`, sorted and parted
    /// by that.
    fn save_partition(&mut self, dir: &K, partition: &K, field: Option<&K>, table: &K, domain: SymId) -> KResult<SymId> {
        let name = name_of(table)?;
        let mut written = self.get_var(name)?;
        if let Some(field) = field {
            let field = name_of(field)?;
            let K::Table(sorted) = ops::table_sort_op(&K::Sym(field), &written, false)? else {
                return Err(KError::ty(".Q.dpft requires an unkeyed table"));
            };
            let cols: Vec<SymId> = [field].into_iter().chain(sorted.cols.iter().copied().filter(|&c| c != field)).collect();
            let mut vals = cols.iter().map(|&c| sorted.col(c).cloned().ok_or_else(|| KError::value(resolve(c)))).collect::<KResult<Vec<_>>>()?;
            vals[0] = vals[0].with_attr(Attr::Parted)?;
            written = K::Table(Arc::new(Flip::new(cols, vals)?));
        }
        let K::Table(_) = written else { return Err(KError::ty("a partition holds unkeyed tables")) };
        let K::Table(written) = self.enumerate_symbols(dir, &written, domain)? else {
            return Err(KError::ty("a partition holds unkeyed tables"));
        };
        let place = storage::path_of(self.ws, &self.partition_path(dir, partition, table)?)?;
        storage::write_splayed_table(&place, &written)?;
        Ok(name)
    }

    /// `.Q.hdpf[database; dir; partition; field]`, a day's end: write every table of the root namespace to the
    /// partition, empty it, and tell the process at `database`, if there is one, to load what is new.
    pub(crate) fn save_day(&mut self, database: &K, dir: &K, partition: &K, field: &K) -> KResult<K> {
        let namespace = std::mem::take(&mut self.session.current_ns);
        let saved = (|| -> KResult<()> {
            let K::Vector(tables) = self.eval_unary_dispatch(intern("tables"), &K::sym("."))? else { return Ok(()) };
            for table in (0..tables.len()).map(|i| tables.get_atom(i)) {
                let name = name_of(&table)?;
                // (a table that is mounted and no more is not written again)
                if !self.ws.vars.contains_key(&name) {
                    continue;
                }
                self.save_partition(dir, partition, Some(field), &table, intern("sym"))?;
                let empty = ops::eval_binary_prim("#", &K::Long(0), &self.get_var(name)?)?;
                self.set_globals(vec![(name, Some(empty))])?;
            }
            Ok(())
        })();
        self.session.current_ns = namespace;
        saved?;
        let address = match database {
            K::Int(_) | K::Long(_) => Some(format!("::{}", database.as_i64()?)),
            other => other.as_str(),
        };
        if let Some(Ok(mut database)) = address.and_then(|a| self.connect_to(&a)) {
            database.query_k(&K::string("\\l ."))?;
        }
        Ok(K::Null)
    }

    /// `.Q.cn table`: the rows that each partition shown has of a partitioned table.
    pub(crate) fn partition_counts(&self, table: &K) -> KResult<K> {
        let column = self.ws.partitioned.values().next().map(|t| t.part_col);
        let (K::Table(table), Some(column), Some(shown)) = (table, column, self.partition_values(None, false)) else {
            return Err(KError::ty(".Q.cn takes a partitioned table"));
        };
        let column = table.col(column).ok_or_else(|| KError::ty(".Q.cn takes a partitioned table"))?;
        let mut counts = vec![0i64; shown.count()];
        let places = ops::eval_binary_prim("?", &shown, &K::Vector(column.clone()))?;
        for place in (0..column.len()).filter_map(|i| ops::index_k(&places, &[Some(K::Long(i as i64))]).ok()?.as_i64().ok()) {
            counts.get_mut(place as usize).into_iter().for_each(|n| *n += 1);
        }
        Ok(K::Vector(ColVec::Long(Attr::None, Arc::new(counts))))
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

    /// The directory that holds each partition shown, or (`all`) every one.
    fn partition_places(&self, all: bool) -> Option<K> {
        let K::Vector(values) = self.partition_values(None, all)? else { return None };
        let place = |value: K| {
            let mut slices = self.ws.partitioned.values().flat_map(|t| t.slices.iter().chain(&t.hidden));
            let root = slices.find(|s| s.part_val == value)?.path.parent()?.parent()?;
            Some(intern(&format!(":{}", root.display())))
        };
        Some(symbols((0..values.len()).filter_map(|i| place(values.get_atom(i))).collect()))
    }

    /// `.Q.pn`: for each partitioned table, the rows that it has in each partition shown.
    fn partition_sizes(&self) -> KResult<Option<K>> {
        let Some(K::Vector(shown)) = self.partition_values(None, false) else { return Ok(None) };
        let mut tables: Vec<_> = self.ws.partitioned.values().collect();
        tables.sort_by_key(|t| resolve(t.name));
        let mut counts = Vec::with_capacity(tables.len());
        for table in &tables {
            let mut each = vec![0i64; shown.len()];
            for (n, rows) in each.iter_mut().enumerate() {
                if let (Some(slice), Some(&column)) = (table.slices.iter().find(|s| s.part_val == shown.get_atom(n)), table.cols.get(1)) {
                    *rows = storage::read_flat_k(&slice.path.join(resolve(column)))?.count() as i64;
                }
            }
            counts.push(K::Vector(ColVec::Long(Attr::None, Arc::new(each))));
        }
        let names = symbols(tables.iter().map(|t| t.name).collect());
        Ok(Some(K::Dict(Arc::new(Dict { keys: Box::new(names), vals: Box::new(general(counts)) }))))
    }

    /// `.Q.pv`, `.Q.PV` (partition values: shown, all), `.Q.pd`, `.Q.PD` (their directories), `.Q.pn` (their rows),
    /// `.Q.pf` (their column), `.Q.pt` (the partitioned tables) and `.Q.P` (the segments that `par.txt` lists).
    pub(crate) fn partition_info(&self, name: &str) -> KResult<Option<K>> {
        let first = self.ws.partitioned.values().next();
        Ok(match name {
            ".Q.pd" | ".Q.PD" => self.partition_places(name == ".Q.PD"),
            ".Q.pn" => self.partition_sizes()?,
            ".Q.P" => first.map(|t| symbols(storage::segments(&t.root).iter().map(|s| intern(&format!(":{s}"))).collect())),
            ".Q.pv" | ".Q.PV" => self.partition_values(None, name == ".Q.PV"),
            ".Q.pf" => first.map(|t| K::Sym(t.part_col)),
            ".Q.pt" => {
                let mut names: Vec<SymId> = self.ws.partitioned.keys().copied().collect();
                names.sort_by_key(|&n| resolve(n));
                Some(symbols(names))
            }
            _ => None,
        })
    }
}
