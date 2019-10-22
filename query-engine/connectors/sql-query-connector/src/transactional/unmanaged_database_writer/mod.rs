mod create;
mod delete;
mod delete_many;
mod nested;
mod relation;
mod update;
mod update_many;

use crate::{
    database::{SqlCapabilities, SqlDatabase},
    error::SqlError,
    query_builder::WriteQueryBuilder,
    RawQuery, Transaction, Transactional,
};
use connector_interface::{self, result_ast::*, write_ast::*, UnmanagedDatabaseWriter};
use serde_json::Value;
use std::sync::Arc;
use futures::future::{BoxFuture, FutureExt};

impl<T> UnmanagedDatabaseWriter for SqlDatabase<T>
where
    T: Transactional + SqlCapabilities + Send + Sync + 'static,
{
    fn execute<'a>(&'a self, db_name: String, write_query: RootWriteQuery) -> BoxFuture<'a, connector_interface::Result<WriteQueryResult>> {
        async fn create(conn: &dyn Transaction, cn: &CreateRecord) -> crate::Result<WriteQueryResult> {
            let parent_id = create::execute(conn, Arc::clone(&cn.model), &cn.non_list_args, &cn.list_args).await?;
            nested::execute(conn, &cn.nested_writes, &parent_id).await?;

            Ok(WriteQueryResult {
                identifier: Identifier::Id(parent_id),
                typ: WriteQueryResultType::Create,
            })
        }

        async fn update(conn: &dyn Transaction, un: &UpdateRecord) -> crate::Result<WriteQueryResult> {
            let parent_id = update::execute(conn, &un.where_, &un.non_list_args, &un.list_args).await?;
            nested::execute(conn, &un.nested_writes, &parent_id).await?;

            Ok(WriteQueryResult {
                identifier: Identifier::Id(parent_id),
                typ: WriteQueryResultType::Update,
            })
        }

        async fn execute(tx: &dyn Transaction, write_query: RootWriteQuery) -> crate::Result<WriteQueryResult> {
            match write_query {
                RootWriteQuery::CreateRecord(ref cn) => Ok(create(tx, cn).await?),
                RootWriteQuery::UpdateRecord(ref un) => Ok(update(tx, un).await?),
                RootWriteQuery::UpsertRecord(ref ups) => match tx.find_id(&ups.where_).await {
                    Err(_e @ SqlError::RecordNotFoundForWhere { .. }) => Ok(create(tx, &ups.create).await?),
                    Err(e) => Err(e.into()),
                    Ok(_) => Ok(update(tx, &ups.update).await?),
                },
                RootWriteQuery::UpdateManyRecords(ref uns) => {
                    let count = update_many::execute(
                        tx,
                        Arc::clone(&uns.model),
                        &uns.filter,
                        &uns.non_list_args,
                        &uns.list_args,
                    ).await?;

                    Ok(WriteQueryResult {
                        identifier: Identifier::Count(count),
                        typ: WriteQueryResultType::Many,
                    })
                }
                RootWriteQuery::DeleteRecord(ref dn) => {
                    let record = delete::execute(tx, &dn.where_).await?;

                    Ok(WriteQueryResult {
                        identifier: Identifier::Record(record),
                        typ: WriteQueryResultType::Delete,
                    })
                }
                RootWriteQuery::DeleteManyRecords(ref dns) => {
                    let count = delete_many::execute(tx, Arc::clone(&dns.model), &dns.filter).await?;

                    Ok(WriteQueryResult {
                        identifier: Identifier::Count(count),
                        typ: WriteQueryResultType::Many,
                    })
                }
                RootWriteQuery::ResetData(ref rd) => {
                    let tables = WriteQueryBuilder::truncate_tables(Arc::clone(&rd.internal_data_model));
                    tx.empty_tables(tables).await?;

                    Ok(WriteQueryResult {
                        identifier: Identifier::None,
                        typ: WriteQueryResultType::Unit,
                    })
                }
            }
        }

        let fut = async move {
            let conn = self.executor.get_connection(&db_name).await?;
            let mut tx = conn.start_transaction().await?;
            let res = execute(&tx, write_query).await;

            if res.is_ok() {
                tx.commit().await?;
            } else {
                tx.rollback().await?;
            }

            res
        };

        async move { Ok(fut.await?) }.boxed()
    }

    fn execute_raw<'a>(&'a self, db_name: String, query: String) -> BoxFuture<'a, connector_interface::Result<Value>> {
        let fut = async move {
            let conn = self.executor.get_connection(&db_name).await?;
            let tx = conn.start_transaction().await?;

            tx.raw_json(RawQuery::from(query)).await
        };

        async move { Ok(fut.await?) }.boxed()
    }
}
