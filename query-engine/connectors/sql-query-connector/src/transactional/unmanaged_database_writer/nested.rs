use super::{create, delete, delete_many, relation, update, update_many};
use crate::{error::SqlError, Transaction};
use connector_interface::write_ast::*;
use futures::future::{BoxFuture, FutureExt};
use prisma_models::GraphqlId;
use std::sync::Arc;

pub fn execute<'a>(
    conn: &'a dyn Transaction,
    nested_write_writes: &'a NestedWriteQueries,
    parent_id: &'a GraphqlId,
) -> BoxFuture<'a, crate::Result<()>> {
    async fn create(conn: &dyn Transaction, parent_id: &GraphqlId, cn: &NestedCreateRecord) -> crate::Result<()> {
        let parent_id = create::execute_nested(
            conn,
            parent_id,
            cn,
            Arc::clone(&cn.relation_field),
            &cn.non_list_args,
            &cn.list_args,
        )
        .await?;

        execute(conn, &cn.nested_writes, &parent_id).await?;

        Ok(())
    }

    async fn update(conn: &dyn Transaction, parent_id: &GraphqlId, un: &NestedUpdateRecord) -> crate::Result<()> {
        let parent_id = update::execute_nested(
            conn,
            parent_id,
            &un.where_,
            Arc::clone(&un.relation_field),
            &un.non_list_args,
            &un.list_args,
        )
        .await?;

        execute(conn, &un.nested_writes, &parent_id).await?;

        Ok(())
    }

    async move {
        for create_record in nested_write_writes.creates.iter() {
            create(conn, parent_id, create_record).await?;
        }

        for update_record in nested_write_writes.updates.iter() {
            update(conn, parent_id, update_record).await?;
        }

        for upsert_record in nested_write_writes.upserts.iter() {
            let id_opt = conn
                .find_id_by_parent(
                    Arc::clone(&upsert_record.relation_field),
                    parent_id,
                    &upsert_record.where_,
                )
                .await;

            match id_opt {
                Ok(_) => update(conn, parent_id, &upsert_record.update).await?,
                Err(_e @ SqlError::RecordsNotConnected { .. }) => {
                    create(conn, parent_id, &upsert_record.create).await?
                }
                Err(e) => return Err(e),
            }
        }

        for delete_record in nested_write_writes.deletes.iter() {
            delete::execute_nested(
                conn,
                parent_id,
                delete_record,
                &delete_record.where_,
                Arc::clone(&delete_record.relation_field),
            )
            .await?;
        }

        for connect in nested_write_writes.connects.iter() {
            relation::connect(
                conn,
                &parent_id,
                connect,
                &connect.where_,
                Arc::clone(&connect.relation_field),
            )
            .await?;
        }

        for set in nested_write_writes.sets.iter() {
            relation::set(conn, &parent_id, set, &set.wheres, Arc::clone(&set.relation_field)).await?;
        }

        for disconnect in nested_write_writes.disconnects.iter() {
            relation::disconnect(conn, &parent_id, disconnect, &disconnect.where_).await?;
        }

        for update_many in nested_write_writes.update_manys.iter() {
            update_many::execute_nested(
                conn,
                &parent_id,
                &update_many.filter,
                Arc::clone(&update_many.relation_field),
                &update_many.non_list_args,
                &update_many.list_args,
            )
            .await?;
        }

        for delete_many in nested_write_writes.delete_manys.iter() {
            delete_many::execute_nested(
                conn,
                &parent_id,
                &delete_many.filter,
                Arc::clone(&delete_many.relation_field),
            )
            .await?;
        }

        Ok(())
    }
        .boxed()
}
