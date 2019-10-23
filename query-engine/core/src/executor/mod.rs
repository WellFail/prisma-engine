mod read;
mod write;

pub use read::ReadQueryExecutor;
pub use write::WriteQueryExecutor;

use crate::{
    query_builders::QueryBuilder,
    query_document::QueryDocument,
    response_ir::{Response, ResultIrBuilder},
    CoreError, CoreResult, QueryPair, QuerySchemaRef, ResultPair, ResultResolutionStrategy,
};
use connector::{ModelExtractor, Query, ReadQuery, WriteQuery};
use futures::future::{BoxFuture, FutureExt};
use prisma_models::ModelRef;

/// Central query executor and main entry point into the query core.
pub struct QueryExecutor {
    primary_connector: &'static str,
    read_executor: ReadQueryExecutor,
    write_executor: WriteQueryExecutor,
}

// Todo:
// - Partial execution semantics?
// - Do we need a clearer separation of queries coming from different query blocks? (e.g. 2 query { ... } in GQL)
// - ReadQueryResult should probably just be QueryResult
// - This is all temporary code until the larger query execution overhaul.
impl QueryExecutor {
    pub fn new(
        primary_connector: &'static str,
        read_executor: ReadQueryExecutor,
        write_executor: WriteQueryExecutor,
    ) -> Self {
        QueryExecutor {
            primary_connector,
            read_executor,
            write_executor,
        }
    }

    pub fn primary_connector(&self) -> &'static str {
        self.primary_connector
    }

    /// Executes a query document, which involves parsing & validating the document,
    /// building queries and a query execution plan, and finally calling the connector APIs to
    /// resolve the queries and build reponses.
    pub async fn execute(&self, query_doc: QueryDocument, query_schema: QuerySchemaRef) -> CoreResult<Vec<Response>> {
        // 1. Parse and validate query document (building)
        let queries = QueryBuilder::new(query_schema).build(query_doc)?;

        // 2. Build query plan
        // ...

        // 3. Execute query plan
        let results: Vec<ResultPair> = self.execute_queries(queries).await?;

        // 4. Build IR response / Parse results into IR response
        Ok(results
            .into_iter()
            .fold(ResultIrBuilder::new(), |builder, result| builder.push(result))
            .build())
    }

    async fn execute_queries(&self, queries: Vec<QueryPair>) -> CoreResult<Vec<ResultPair>> {
        let mut result = Vec::new();

        for query in queries.into_iter() {
            result.push(self.execute_query(query).await?)
        }

        Ok(result)
    }

    fn execute_query<'a>(&'a self, query: QueryPair) -> BoxFuture<'a, CoreResult<ResultPair>> {
        let (query, strategy) = query;
        let model_opt = query.extract_model();

        match query {
            Query::Read(read) => self.execute_read(read, strategy).boxed(),
            Query::Write(write) => self.execute_write(write, strategy, model_opt),
        }
    }

    async fn execute_read(&self, read: ReadQuery, strategy: ResultResolutionStrategy) -> CoreResult<ResultPair> {
        let query_result = self.read_executor.execute(read, &[]).await?;

        Ok(match strategy {
            ResultResolutionStrategy::Serialize(typ) => ResultPair::Read(query_result, typ),
            ResultResolutionStrategy::Dependent(_) => unimplemented!(), // Dependent query exec. from read is not supported in this execution model.
        })
    }

    fn execute_write<'a>(
        &'a self,
        write: WriteQuery,
        strategy: ResultResolutionStrategy,
        model_opt: Option<ModelRef>,
    ) -> BoxFuture<'a, CoreResult<ResultPair>> {
        async move {
            match strategy {
                ResultResolutionStrategy::Serialize(typ) => {
                    let result = self.write_executor.execute(write).await?;

                    Ok(ResultPair::Write(result, typ))
                }
                ResultResolutionStrategy::Dependent(dependent_pair) => match model_opt {
                    Some(model) => {
                        let result = self.execute_dependent(write, *dependent_pair, model).await?;

                        Ok(result)
                    }
                    None => Err(CoreError::ConversionError(
                        "Model required for dependent query execution".into(),
                    )),
                },
            }
        }
            .boxed()
    }

    fn execute_dependent<'a>(
        &'a self,
        write: WriteQuery,
        dependent_pair: QueryPair,
        model: ModelRef,
    ) -> BoxFuture<'a, CoreResult<ResultPair>> {
        match dependent_pair {
            (Query::Read(ReadQuery::RecordQuery(mut rq)), strategy) => async move {
                let result = self.write_executor.execute(write).await?;
                rq.record_finder = Some(result.result.to_record_finder(model)?);

                let dependent_pair = (Query::Read(ReadQuery::RecordQuery(rq)), strategy);
                Ok(self.execute_query(dependent_pair).await?)
            }
                .boxed(),
            _ => unreachable!(), // Invariant for now
        }
    }

    /// Returns db name used in the executor.
    pub fn db_name(&self) -> String {
        self.write_executor.db_name.clone()
    }
}
