use crate::{write_ast::RootWriteQuery, result_ast::WriteQueryResult};
use serde_json::Value;
use futures::future::BoxFuture;

/// Methods for writing data.
pub trait UnmanagedDatabaseWriter {
    /// Execute raw SQL string without any safety guarantees, returning the result as JSON.
    fn execute_raw<'a>(&'a self, db_name: String, query: String) -> BoxFuture<'a, crate::Result<Value>>;

    /// Executes the write query and all nested write queries, returning the result
    /// of the topmost write.
    fn execute<'a>(&'a self, db_name: String, write_query: RootWriteQuery) -> BoxFuture<'a, crate::Result<WriteQueryResult>>;
}
