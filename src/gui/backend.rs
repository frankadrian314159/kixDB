//! Abstraction over an embedded `kixdb::Connection` or a remote kdb+ IPC server (`RemoteBackend`).
use kixdb::inspect::{ObjectEntry, TableInfo};
use kixdb::{BatchResult, Connection, KResult, K};

pub trait Backend: Send {
    fn execute(&mut self, code: &str) -> BatchResult;
    fn eval(&mut self, code: &str) -> KResult<K>;
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
}
