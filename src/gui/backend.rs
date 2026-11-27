//! Abstraction over an embedded `kixdb::Connection` or a remote kdb+ IPC server (`RemoteBackend`).
use kixdb::inspect::{ObjectEntry, TableInfo};
use kixdb::{Attr, BatchResult, ColVec, Connection, Flip, KResult, K};
use std::sync::Arc;

pub trait Backend: Send {
    fn execute(&mut self, code: &str) -> BatchResult;
    fn eval(&mut self, code: &str) -> KResult<K>;
    fn send_message(&mut self, msg: &K) -> KResult<K>;
    fn table_info(&mut self, name: &str) -> Option<TableInfo>;
    fn objects(&mut self) -> Vec<ObjectEntry>;
    fn definition(&mut self, name: &str) -> Option<String>;
    fn namespaces(&mut self) -> Vec<String>;
    fn current_namespace(&mut self) -> String;
    fn login(&self) -> String;
    fn explain(&mut self, code: &str) -> KResult<Vec<String>>;
}

impl Backend for Connection {
    fn execute(&mut self, code: &str) -> BatchResult {
        Connection::execute(self, code)
    }
    fn eval(&mut self, code: &str) -> KResult<K> {
        Connection::eval(self, code)
    }
    fn send_message(&mut self, msg: &K) -> KResult<K> {
        Connection::eval_ipc_message(self, msg)
    }
    fn table_info(&mut self, name: &str) -> Option<TableInfo> {
        Connection::table_info(self, name)
    }
    fn objects(&mut self) -> Vec<ObjectEntry> {
        Connection::objects(self)
    }
    fn definition(&mut self, name: &str) -> Option<String> {
        Connection::definition(self, name)
    }
    fn namespaces(&mut self) -> Vec<String> {
        Connection::namespaces(self)
    }
    fn current_namespace(&mut self) -> String {
        Connection::current_namespace(self)
    }
    fn login(&self) -> String {
        Connection::login(self)
    }
    fn explain(&mut self, code: &str) -> KResult<Vec<String>> {
        Connection::explain(self, code)
    }
}

impl<'a> kixdb::tools::QSession for dyn Backend + 'a {
    fn run_q(&mut self, code: &str) -> KResult<()> {
        self.eval(code).map(|_| ())
    }
    fn describe_table(&mut self, name: &str) -> Option<TableInfo> {
        self.table_info(name)
    }
    fn upsert_batch(&mut self, table_name: &str, batch: Flip) -> KResult<()> {
        let msg = K::Vector(ColVec::General(
            Attr::None,
            Arc::new(vec![
                K::string("upsert"),
                K::Sym(kixdb::sym::intern(table_name)),
                K::Table(Arc::new(batch)),
            ]),
        ));
        self.send_message(&msg).map(|_| ())
    }
}
