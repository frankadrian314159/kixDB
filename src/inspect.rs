//! Workspace introspection shared by the `kix` CLI shell and the `kix-gui` desktop application.
use crate::engine::Workspace;
use crate::error::{KError, KResult};
use crate::ipc::IpcClient;
use crate::sym::{intern, resolve};
use crate::types::{Attr, ColVec, Flip, Func, K};
use crate::Connection;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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
        collect_objects(&g)
    }

    pub fn namespaces(&self) -> Vec<String> {
        let g = self.server.shared.ws.read().unwrap_or_else(|e| e.into_inner());
        let mut set = std::collections::BTreeSet::new();
        set.insert(".".to_string());
        for (&sym, _) in g.vars.iter().filter(|(&s, _)| !resolve(s).starts_with(".kix.")) {
            let full = resolve(sym);
            let (ns, _) = split_ns(full);
            if !ns.is_empty() {
                set.insert(ns);
            }
        }
        for &sym in g.views.keys() {
            let full = resolve(sym);
            let (ns, _) = split_ns(full);
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
            let flip = crate::storage::partitioned_schema_flip(&pt, None, true).ok()?;
            let nrows: usize = pt.slices.iter().map(|s| crate::storage::slice_row_count(&pt, s).unwrap_or(0)).sum();
            let columns = flip
                .cols
                .iter()
                .zip(&flip.vals)
                .map(|(&c, v)| ColumnInfo {
                    name: resolve(c).into(),
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
                nrows,
                approx_bytes: pt.mapped_bytes(),
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
                    name: resolve(c).into(),
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
                    name: resolve(c).into(),
                    type_char: v.meta_type_char(),
                    type_name: v.type_name().to_string(),
                    attr: v.attr().char(),
                    is_key: true,
                });
            }
            for (&c, v) in kt.vals.cols.iter().zip(&kt.vals.vals) {
                columns.push(ColumnInfo {
                    name: resolve(c).into(),
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

fn kind_sym(k: ObjectKind) -> &'static str {
    match k {
        ObjectKind::Table => "table",
        ObjectKind::KeyedTable => "keyed",
        ObjectKind::PartitionedTable => "partitioned",
        ObjectKind::View => "view",
        ObjectKind::Function => "function",
        ObjectKind::Dictionary => "dict",
        ObjectKind::Variable => "variable",
    }
}

fn kind_from_sym(s: &str) -> ObjectKind {
    match s {
        "table" => ObjectKind::Table,
        "keyed" => ObjectKind::KeyedTable,
        "partitioned" => ObjectKind::PartitionedTable,
        "view" => ObjectKind::View,
        "function" => ObjectKind::Function,
        "dict" => ObjectKind::Dictionary,
        _ => ObjectKind::Variable,
    }
}

pub(crate) fn collect_objects(ws: &Workspace) -> Vec<ObjectEntry> {
    let mut out = Vec::new();
    for (&sym, val) in ws.vars.iter() {
        let full = resolve(sym);
        if full.starts_with(".kix.") {
            continue;
        }
        let (ns, short) = split_ns(full);
        let kind = match val {
            K::Table(_) => ObjectKind::Table,
            K::KeyedTable(_) => ObjectKind::KeyedTable,
            K::Func(Func::Lambda(_)) => ObjectKind::Function,
            K::Dict(_) => ObjectKind::Dictionary,
            _ => ObjectKind::Variable,
        };
        out.push(ObjectEntry { namespace: ns, name: short, kind });
    }
    for &sym in ws.partitioned.keys() {
        if !ws.vars.contains_key(&sym) {
            let full = resolve(sym);
            let (ns, short) = split_ns(full);
            out.push(ObjectEntry { namespace: ns, name: short, kind: ObjectKind::PartitionedTable });
        }
    }
    for &sym in ws.views.keys() {
        let full = resolve(sym);
        let (ns, short) = split_ns(full);
        out.push(ObjectEntry { namespace: ns, name: short, kind: ObjectKind::View });
    }
    out.sort_by(|a, b| (kind_rank(a.kind), &a.namespace, &a.name).cmp(&(kind_rank(b.kind), &b.namespace, &b.name)));
    out
}

/// `.kix.objects[]` — single-table workspace catalog for remote CLI/GUI clients.
pub(crate) fn workspace_objects_table(ws: &Workspace) -> KResult<K> {
    let entries = collect_objects(ws);
    let ns = ColVec::Sym(Attr::None, Arc::new(entries.iter().map(|e| intern(&e.namespace)).collect()));
    let name = ColVec::Sym(Attr::None, Arc::new(entries.iter().map(|e| intern(&e.name)).collect()));
    let kind = ColVec::Sym(Attr::None, Arc::new(entries.iter().map(|e| intern(kind_sym(e.kind))).collect()));
    Ok(K::Table(Arc::new(Flip::new(vec![intern("ns"), intern("name"), intern("kind")], vec![ns, name, kind])?)))
}

/// `.kix.def[`name]` — return definition script without evaluating views.
pub(crate) fn workspace_definition(ws: &Workspace, name: &str) -> KResult<K> {
    let sym = intern(name);
    if let Some(vdef) = ws.views.get(&sym) {
        return Ok(K::string(&format!("{name}:: {}", vdef.source.trim_start_matches(&format!("{name}:: ")))));
    }
    if let Some(K::Func(f)) = ws.vars.get(&sym) {
        return Ok(K::string(&format!("{name}: {}", f.source_text())));
    }
    if let Some(val) = ws.vars.get(&sym) {
        if let Some(ti) = table_info_from_k(name, val) {
            return Ok(K::string(&crate::tools::table_script(&ti)));
        }
        return Ok(K::string(&format!("{name}: {}", val.q_repr())));
    }
    if let Some(pt) = ws.partitioned.get(&sym) {
        if let Ok(flip) = crate::storage::partitioned_schema_flip(pt, None, true) {
            let nrows: usize = pt.slices.iter().map(|s| crate::storage::slice_row_count(pt, s).unwrap_or(0)).sum();
            let columns = flip
                .cols
                .iter()
                .zip(&flip.vals)
                .map(|(&c, v)| ColumnInfo {
                    name: resolve(c).into(),
                    type_char: v.meta_type_char(),
                    type_name: v.type_name().to_string(),
                    attr: v.attr().char(),
                    is_key: false,
                })
                .collect();
            let ti = TableInfo {
                name: name.to_string(),
                kind: ObjectKind::PartitionedTable,
                columns,
                nrows,
                approx_bytes: pt.mapped_bytes(),
            };
            return Ok(K::string(&crate::tools::table_script(&ti)));
        }
    }
    Err(KError::value(name.to_string()))
}

// ----- Remote IPC Introspection Helpers (shared by `kix --connect` and `kix-gui`) -----

pub fn remote_objects(client: &mut IpcClient) -> Vec<ObjectEntry> {
    if let Ok(K::Table(f)) = client.query_str(".kix.objects[]") {
        if let (Some(ns_c), Some(nm_c), Some(kd_c)) = (f.col(intern("ns")), f.col(intern("name")), f.col(intern("kind"))) {
            return (0..f.nrows())
                .map(|r| ObjectEntry {
                    namespace: ns_c.get_atom(r).cell_text(),
                    name: nm_c.get_atom(r).cell_text(),
                    kind: kind_from_sym(&kd_c.get_atom(r).cell_text()),
                })
                .collect();
        }
    }
    // Fallback for external real kdb+ servers without `.kix.objects`
    let sym_list = |c: &mut IpcClient, cmd: &str| -> Vec<String> {
        match c.query_str(cmd) {
            Ok(K::Vector(ColVec::Sym(_, syms))) => syms.iter().map(|&s| resolve(s).into()).collect(),
            _ => vec![],
        }
    };
    let mut out = Vec::new();
    let tables = sym_list(client, "\\a");
    let views = sym_list(client, "\\b");
    let funcs = sym_list(client, "\\f");
    let vars = sym_list(client, "\\v");
    for t in &tables {
        let is_keyed = client.query_str(&format!("type {t}")).ok().and_then(|k| k.as_i64().ok()) == Some(99);
        out.push(ObjectEntry {
            namespace: ".".into(),
            name: t.clone(),
            kind: if is_keyed { ObjectKind::KeyedTable } else { ObjectKind::Table },
        });
    }
    for v in &views {
        out.push(ObjectEntry { namespace: ".".into(), name: v.clone(), kind: ObjectKind::View });
    }
    for f in &funcs {
        out.push(ObjectEntry { namespace: ".".into(), name: f.clone(), kind: ObjectKind::Function });
    }
    for v in &vars {
        if !tables.contains(v) && !views.contains(v) {
            let is_dict = client.query_str(&format!("type {v}")).ok().and_then(|k| k.as_i64().ok()) == Some(99);
            out.push(ObjectEntry {
                namespace: ".".into(),
                name: v.clone(),
                kind: if is_dict { ObjectKind::Dictionary } else { ObjectKind::Variable },
            });
        }
    }
    out
}

pub fn remote_namespaces(client: &mut IpcClient) -> Vec<String> {
    let mut set = std::collections::BTreeSet::new();
    set.insert(".".to_string());
    for o in remote_objects(client) {
        if !o.namespace.is_empty() {
            set.insert(o.namespace);
        }
    }
    set.into_iter().collect()
}

pub fn remote_table_info(client: &mut IpcClient, name: &str) -> Option<TableInfo> {
    let meta_k = client.query_str(&format!("0 ! meta {name}")).ok()?;
    let K::Table(mf) = meta_k else { return None };
    let keys_vec: Vec<String> = match client.query_str(&format!("keys {name}")).ok()? {
        K::Vector(ColVec::Sym(_, ks)) => ks.iter().map(|&s| resolve(s).into()).collect(),
        _ => vec![],
    };
    let nrows = client
        .query_str(&format!("count {name}"))
        .ok()
        .and_then(|k| k.as_i64().ok())
        .unwrap_or(0) as usize;

    let c_col = mf.col(intern("c"))?;
    let t_col = mf.col(intern("t"))?;
    let a_col = mf.col(intern("a"))?;

    let mut columns = Vec::with_capacity(mf.nrows());
    for r in 0..mf.nrows() {
        let cname = c_col.get_atom(r).cell_text();
        let tc = t_col.get_atom(r).cell_text().chars().next().unwrap_or(' ');
        let attr = a_col.get_atom(r).cell_text().chars().next();
        let tname = ColVec::empty_of_type_char(tc)
            .map(|v| v.type_name().to_string())
            .unwrap_or_else(|| "list".into());
        let is_key = keys_vec.contains(&cname);
        columns.push(ColumnInfo { name: cname, type_char: tc, type_name: tname, attr, is_key });
    }
    let kind = if keys_vec.is_empty() { ObjectKind::Table } else { ObjectKind::KeyedTable };
    Some(TableInfo { name: name.to_string(), kind, columns, nrows, approx_bytes: 0 })
}

pub fn remote_definition(client: &mut IpcClient, name: &str) -> Option<String> {
    let msg = K::Vector(ColVec::General(Attr::None, Arc::new(vec![K::sym(".kix.def"), K::sym(name)])));
    if let Ok(k) = client.query_k(&msg) {
        if let Some(s) = k.as_str() {
            return Some(s);
        }
    }
    if let Some(ti) = remote_table_info(client, name) {
        return Some(crate::tools::table_script(&ti));
    }
    None
}

pub fn remote_explain(client: &mut IpcClient, code: &str) -> KResult<Vec<String>> {
    match client.query_str(&format!("EXPLAIN {code}")) {
        Ok(K::Vector(cv)) => Ok((0..cv.len()).map(|i| cv.get_atom(i).cell_text()).collect()),
        Ok(other) => Ok(vec![other.cell_text()]),
        Err(e) => Err(e),
    }
}
