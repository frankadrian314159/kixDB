//! Workspace introspection shared by the `kix` CLI shell and the `kix-gui` desktop application.
use crate::sym::{intern, resolve};
use crate::types::{Func, K};
use crate::Connection;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectKind {
    Table,
    KeyedTable,
    PartitionedTable,
    View,
    Function,
    Dictionary,
    Variable,
}

impl ObjectKind {
    pub fn label(self) -> &'static str {
        match self {
            ObjectKind::Table => "Table",
            ObjectKind::KeyedTable => "Keyed Table",
            ObjectKind::PartitionedTable => "Partitioned Table",
            ObjectKind::View => "View",
            ObjectKind::Function => "Function",
            ObjectKind::Dictionary => "Dictionary",
            ObjectKind::Variable => "Variable",
        }
    }

    pub fn is_tabular(self) -> bool {
        matches!(self, ObjectKind::Table | ObjectKind::KeyedTable | ObjectKind::PartitionedTable)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectEntry {
    pub namespace: String,
    pub name: String,
    pub kind: ObjectKind,
}

impl ObjectEntry {
    pub fn full_name(&self) -> String {
        if self.namespace.is_empty() || self.namespace == "." {
            self.name.clone()
        } else {
            format!("{}.{}", self.namespace, self.name)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub type_char: char,
    pub type_name: String,
    pub attr: Option<char>,
    pub is_key: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub kind: ObjectKind,
    pub columns: Vec<ColumnInfo>,
    pub nrows: usize,
    pub approx_bytes: usize,
}

impl TableInfo {
    pub fn key_columns(&self) -> Vec<&str> {
        self.columns.iter().filter(|c| c.is_key).map(|c| c.name.as_str()).collect()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreStats {
    pub memory_bytes: usize,
    pub sym_count: usize,
    pub table_count: usize,
    pub view_count: usize,
    pub wal_records: u64,
    pub wal_bytes: u64,
    pub snap_bytes: u64,
    pub log_forces: u64,
    pub data_dir: Option<String>,
}

impl Connection {
    /// List all workspace objects (tables, keyed tables, partitioned tables, views, functions, dicts, variables).
    pub fn objects(&self) -> Vec<ObjectEntry> {
        let g = self.server.shared.ws.read().unwrap_or_else(|e| e.into_inner());
        let mut out = Vec::new();

        for (&sym, val) in &g.vars {
            let full = resolve(sym);
            let (ns, short) = split_ns(&full);
            let kind = match val {
                K::Table(_) => ObjectKind::Table,
                K::KeyedTable(_) => ObjectKind::KeyedTable,
                K::Func(Func::Lambda(_)) => ObjectKind::Function,
                K::Dict(_) => ObjectKind::Dictionary,
                _ => ObjectKind::Variable,
            };
            out.push(ObjectEntry { namespace: ns, name: short, kind });
        }

        for &sym in g.partitioned.keys() {
            let full = resolve(sym);
            if !g.vars.contains_key(&sym) {
                let (ns, short) = split_ns(&full);
                out.push(ObjectEntry { namespace: ns, name: short, kind: ObjectKind::PartitionedTable });
            }
        }

        for &sym in g.views.keys() {
            let full = resolve(sym);
            let (ns, short) = split_ns(&full);
            out.push(ObjectEntry { namespace: ns, name: short, kind: ObjectKind::View });
        }

        out.sort_by(|a, b| (kind_rank(a.kind), &a.namespace, &a.name).cmp(&(kind_rank(b.kind), &b.namespace, &b.name)));
        out
    }

    pub fn namespaces(&self) -> Vec<String> {
        let g = self.server.shared.ws.read().unwrap_or_else(|e| e.into_inner());
        let mut set = std::collections::BTreeSet::new();
        set.insert(".".to_string());
        for &sym in g.vars.keys().chain(g.views.keys()) {
            let full = resolve(sym);
            let (ns, _) = split_ns(&full);
            if !ns.is_empty() {
                set.insert(ns);
            }
        }
        set.into_iter().collect()
    }

    /// Detailed structure of a table (unkeyed, keyed, or partitioned).
    pub fn table_info(&mut self, name: &str) -> Option<TableInfo> {
        let sym = intern(name);
        let g = self.server.shared.ws.read().unwrap_or_else(|e| e.into_inner());
        if let Some(val) = g.vars.get(&sym) {
            return table_info_from_k(name, val);
        }
        if let Some(pt) = g.partitioned.get(&sym).cloned() {
            drop(g);
            let flip = crate::storage::load_partitioned_filtered(&pt, &[], None, |_| Ok(K::Null)).ok()?;
            let columns = flip
                .cols
                .iter()
                .zip(&flip.vals)
                .map(|(&c, v)| ColumnInfo {
                    name: resolve(c),
                    type_char: v.meta_type_char(),
                    type_name: v.type_name().to_string(),
                    attr: v.attr().char(),
                    is_key: false,
                })
                .collect();
            return Some(TableInfo {
                name: name.to_string(),
                kind: ObjectKind::PartitionedTable,
                columns,
                nrows: flip.nrows(),
                approx_bytes: flip.approx_bytes(),
            });
        }
        None
    }

    /// Source definition of a function or view, or schema creation script of a table.
    pub fn definition(&mut self, name: &str) -> Option<String> {
        let sym = intern(name);
        {
            let g = self.server.shared.ws.read().unwrap_or_else(|e| e.into_inner());
            if let Some(vdef) = g.views.get(&sym) {
                return Some(format!("{}:: {}", name, vdef.source.trim_start_matches(&format!("{name}:: "))));
            }
            if let Some(K::Func(f)) = g.vars.get(&sym) {
                return Some(format!("{name}: {}", f.source_text()));
            }
            if let Some(other) = g.vars.get(&sym) {
                if !matches!(other, K::Table(_) | K::KeyedTable(_)) {
                    return Some(format!("{name}: {}", other.q_repr()));
                }
            }
        }
        self.table_info(name).map(|ti| crate::tools::table_script(&ti))
    }
}

fn table_info_from_k(name: &str, val: &K) -> Option<TableInfo> {
    match val {
        K::Table(t) => {
            let columns = t
                .cols
                .iter()
                .zip(&t.vals)
                .map(|(&c, v)| ColumnInfo {
                    name: resolve(c),
                    type_char: v.meta_type_char(),
                    type_name: v.type_name().to_string(),
                    attr: v.attr().char(),
                    is_key: false,
                })
                .collect();
            Some(TableInfo {
                name: name.to_string(),
                kind: ObjectKind::Table,
                columns,
                nrows: t.nrows(),
                approx_bytes: t.approx_bytes(),
            })
        }
        K::KeyedTable(kt) => {
            let mut columns = Vec::with_capacity(kt.keys.ncols() + kt.vals.ncols());
            for (&c, v) in kt.keys.cols.iter().zip(&kt.keys.vals) {
                columns.push(ColumnInfo {
                    name: resolve(c),
                    type_char: v.meta_type_char(),
                    type_name: v.type_name().to_string(),
                    attr: v.attr().char(),
                    is_key: true,
                });
            }
            for (&c, v) in kt.vals.cols.iter().zip(&kt.vals.vals) {
                columns.push(ColumnInfo {
                    name: resolve(c),
                    type_char: v.meta_type_char(),
                    type_name: v.type_name().to_string(),
                    attr: v.attr().char(),
                    is_key: false,
                });
            }
            Some(TableInfo {
                name: name.to_string(),
                kind: ObjectKind::KeyedTable,
                columns,
                nrows: kt.nrows(),
                approx_bytes: kt.keys.approx_bytes() + kt.vals.approx_bytes(),
            })
        }
        _ => None,
    }
}

fn split_ns(full: &str) -> (String, String) {
    if let Some(stripped) = full.strip_prefix('.') {
        if let Some((ns_part, item)) = stripped.rsplit_once('.') {
            return (format!(".{ns_part}"), item.to_string());
        }
        return (".".to_string(), full.to_string());
    }
    (".".to_string(), full.to_string())
}

fn kind_rank(k: ObjectKind) -> u8 {
    match k {
        ObjectKind::Table => 0,
        ObjectKind::KeyedTable => 1,
        ObjectKind::PartitionedTable => 2,
        ObjectKind::View => 3,
        ObjectKind::Function => 4,
        ObjectKind::Dictionary => 5,
        ObjectKind::Variable => 6,
    }
}
