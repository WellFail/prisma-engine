use crate::{
    error::SqlError,
    query_builder::{DeleteActions, NestedActions, WriteQueryBuilder},
    Transaction,
};
use connector_interface::{error::RecordFinderInfo, filter::RecordFinder};
use prisma_models::{GraphqlId, RelationFieldRef, SingleRecord};
use std::sync::Arc;

/// A top level delete that removes one record. Violating any relations or a
/// non-existing record will cause an error.
///
/// Will return the deleted record if the delete was successful.
pub async fn execute(conn: &dyn Transaction, record_finder: &RecordFinder) -> crate::Result<SingleRecord> {
    let model = record_finder.field.model();
    let record = conn.find_record(record_finder).await?;
    let id = record.collect_id(&model.fields().id().name).unwrap();

    DeleteActions::check_relation_violations(Arc::clone(&model), &[&id], |select| {
        async move {
            let ids = conn.select_ids(select).await?;
            Ok(ids.into_iter().next())
        }
    })
    .await?;

    for delete in WriteQueryBuilder::delete_many(model, &[&id]) {
        conn.delete(delete).await?;
    }

    Ok(record)
}

/// A nested delete that removes one item related to the given `parent_id`.
/// If no `RecordFinder` is given, will delete the first item from the
/// table.
///
/// Errors thrown from domain violations:
///
/// - Violating any relations where the deleted record is required
/// - If the deleted record is not connected to the parent
/// - The record does not exist
pub async fn execute_nested(
    conn: &dyn Transaction,
    parent_id: &GraphqlId,
    actions: &dyn NestedActions,
    record_finder: &Option<RecordFinder>,
    relation_field: RelationFieldRef,
) -> crate::Result<()> {
    if let Some(ref record_finder) = record_finder {
        conn.find_id(record_finder).await?;
    };

    let find = conn
        .find_id_by_parent(Arc::clone(&relation_field), parent_id, record_finder)
        .await;

    let child_id = find.map_err(|e| match e {
        SqlError::RecordsNotConnected {
            relation_name,
            parent_name,
            child_name,
            child_where,
            ..
        } => {
            let model = Arc::clone(&relation_field.model());

            SqlError::RecordsNotConnected {
                relation_name,
                parent_name,
                parent_where: Some(Box::new(RecordFinderInfo::for_id(model, parent_id))),
                child_name,
                child_where,
            }
        }
        e => e,
    })?;

    {
        let (select, check) = actions.ensure_connected(parent_id, &child_id);
        let ids = conn.select_ids(select).await?;
        check(ids.into_iter().next().is_some())?;
    }

    let related_model = relation_field.related_model();

    DeleteActions::check_relation_violations(related_model, &[&child_id; 1], |select| {
        async move {
            let ids = conn.select_ids(select).await?;
            Ok(ids.into_iter().next())
        }
    })
    .await?;

    for delete in WriteQueryBuilder::delete_many(relation_field.related_model(), &[&child_id]) {
        conn.delete(delete).await?;
    }

    Ok(())
}
