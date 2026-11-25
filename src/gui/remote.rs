//! Remote `Backend` over native kdb+ binary IPC (`kix-serve`, `kix -p`, `kix-cluster`, or real kdb+).
use crate::backend::Backend;
use kixdb::inspect::{ColumnInfo, ObjectEntry, ObjectKind, TableInfo};
use kixdb::ipc::IpcClient;
use kixdb::sym::resolve;
use kixdb::{k_to_resultset, BatchResult, ColVec, Event, KError, KResult, K};

#[derive(Clone, Debug)]
pub struct Target {
    pub host: String,
    pub port: u16,
    pub login: String,
    pub password: String,
}

impl Default for Target {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 5000,
            login: "admin".into(),
            password: String::new(),
        }
    }
}

impl Target {
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

pub struct RemoteBackend {
    client: IpcClient,
    login: String,
    ns: String,
}

impl RemoteBackend {
    pub fn connect(target: &Target) -> KResult<Self> {
        let client = IpcClient::connect_addr(&target.addr(), &target.login, &target.password)?;
        Ok(Self {
            client,
            login: target.login.clone(),
            ns: ".".into(),
        })
    }

    fn sym_list(&mut self, cmd: &str) -> Vec<String> {
        match self.client.query_str(cmd) {
            Ok(K::Vector(ColVec::Sym(_, syms))) => syms.iter().map(|&s| resolve(s)).collect(),
            _ => vec![],
        }
    }
}

impl Backend for RemoteBackend {
    fn eval(&mut self, code: &str) -> KResult<K> {
        self.client.query_str(code)
    }

    fn execute(&mut self, code: &str) -> BatchResult {
        match self.client.query_str(code) {
            Ok(K::Null) => BatchResult { events: vec![] },
            Ok(val) => {
                if let Some(rs) = k_to_resultset(&val) {
                    BatchResult { events: vec![Event::Rows(rs)] }
                } else {
                    BatchResult { events: vec![Event::Value(val)] }
                }
            }
            Err(e) => BatchResult { events: vec![Event::Error(e)] },
        }
    }

    fn objects(&mut self) -> Vec<ObjectEntry> {
        let mut out = Vec::new();
        let tables = self.sym_list("\\a");
        let views = self.sym_list("\\b");
        let funcs = self.sym_list("\\f");
        let vars = self.sym_list("\\v");

        for t in &tables {
            let is_keyed = self
                .client
                .query_str(&format!("type {t}"))
                .map(|k| k.as_i64().unwrap_or(98) == 99)
                .unwrap_or(false);
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
                let is_dict = self
                    .client
                    .query_str(&format!("type {v}"))
                    .map(|k| k.as_i64().unwrap_or(0) == 99)
                    .unwrap_or(false);
                out.push(ObjectEntry {
                    namespace: ".".into(),
                    name: v.clone(),
                    kind: if is_dict { ObjectKind::Dictionary } else { ObjectKind::Variable },
                });
            }
        }
        out
    }

    fn table_info(&mut self, name: &str) -> Option<TableInfo> {
        let meta_k = self.client.query_str(&format!("0 ! meta {name}")).ok()?;
        let K::Table(mf) = meta_k else { return None };
        let keys_vec: Vec<String> = match self.client.query_str(&format!("keys {name}")).ok()? {
            K::Vector(ColVec::Sym(_, ks)) => ks.iter().map(|&s| resolve(s)).collect(),
            _ => vec![],
        };
        let nrows = self
            .client
            .query_str(&format!("count {name}"))
            .ok()
            .and_then(|k| k.as_i64().ok())
            .unwrap_or(0) as usize;

        let c_col = mf.col(kixdb::sym::intern("c"))?;
        let t_col = mf.col(kixdb::sym::intern("t"))?;
        let a_col = mf.col(kixdb::sym::intern("a"))?;

        let mut columns = Vec::with_capacity(mf.nrows());
        for r in 0..mf.nrows() {
            let cname = c_col.get_atom(r).cell_text();
            let tc = t_col.get_atom(r).cell_text().chars().next().unwrap_or(' ');
            let attr_s = a_col.get_atom(r).cell_text();
            let attr = attr_s.chars().next();
            let tname = kixdb::ColVec::empty_of_type_char(tc)
                .map(|v| v.type_name().to_string())
                .unwrap_or_else(|| "list".into());
            let is_key = keys_vec.contains(&cname);
            columns.push(ColumnInfo {
                name: cname,
                type_char: tc,
                type_name: tname,
                attr,
                is_key,
            });
        }
        let kind = if keys_vec.is_empty() { ObjectKind::Table } else { ObjectKind::KeyedTable };
        Some(TableInfo {
            name: name.to_string(),
            kind,
            columns,
            nrows,
            approx_bytes: 0,
        })
    }

    fn definition(&mut self, name: &str) -> Option<String> {
        if let Some(ti) = self.table_info(name) {
            return Some(kixdb::tools::table_script(&ti));
        }
        self.client.query_str(name).ok().map(|k| format!("{name}: {}", k.q_repr()))
    }

    fn namespaces(&mut self) -> Vec<String> {
        vec![".".into()]
    }

    fn current_namespace(&mut self) -> String {
        if let Ok(K::Sym(s)) = self.client.query_str("\\d") {
            self.ns = resolve(s);
        }
        self.ns.clone()
    }

    fn login(&self) -> String {
        self.login.clone()
    }

    fn explain(&mut self, code: &str) -> KResult<Vec<String>> {
        // Ask remote engine / cluster or parse locally
        let stmts = kixdb::parser::parse_script(code).map_err(|e| KError::syntax(e.to_string()))?;
        Ok(stmts.iter().enumerate().map(|(i, s)| format!("Remote IPC statement {}: {s:?}", i + 1)).collect())
    }
}
