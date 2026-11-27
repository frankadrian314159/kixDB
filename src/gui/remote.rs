//! Remote `Backend` over native kdb+ binary IPC (`kix-serve`, `kix -p`, `kix-cluster`, or real kdb+), including TLS (`tcps://`).
use crate::backend::Backend;
use kixdb::inspect::{
    remote_definition, remote_explain, remote_namespaces, remote_objects, remote_table_info, ObjectEntry, TableInfo,
};
use kixdb::ipc::IpcClient;
use kixdb::sym::resolve;
use kixdb::{k_to_resultset, BatchResult, Event, KResult, K};

#[derive(Clone, Debug)]
pub struct Target {
    pub host: String,
    pub port: u16,
    pub login: String,
    pub password: String,
    pub tls: bool,
    pub ca_cert: String,
}

impl Default for Target {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 5000,
            login: "admin".into(),
            password: String::new(),
            tls: false,
            ca_cert: std::env::var("KIX_SSL_CA_CERT_FILE").unwrap_or_default(),
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
        let raw_addr = target.addr();
        let client = if target.tls {
            let ca = if target.ca_cert.trim().is_empty() { "kix-cert.pem" } else { target.ca_cert.trim() };
            IpcClient::connect_tls(&raw_addr, &target.login, &target.password, ca)?
        } else {
            IpcClient::connect_addr(&raw_addr, &target.login, &target.password)?
        };
        Ok(Self {
            client,
            login: target.login.clone(),
            ns: ".".into(),
        })
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
        remote_objects(&mut self.client)
    }

    fn table_info(&mut self, name: &str) -> Option<TableInfo> {
        remote_table_info(&mut self.client, name)
    }

    fn definition(&mut self, name: &str) -> Option<String> {
        remote_definition(&mut self.client, name)
    }

    fn namespaces(&mut self) -> Vec<String> {
        remote_namespaces(&mut self.client)
    }

    fn current_namespace(&mut self) -> String {
        if let Ok(K::Sym(s)) = self.client.query_str("\\d") {
            self.ns = resolve(s).into();
        }
        self.ns.clone()
    }

    fn login(&self) -> String {
        self.login.clone()
    }

    fn explain(&mut self, code: &str) -> KResult<Vec<String>> {
        remote_explain(&mut self.client, code)
    }
}
