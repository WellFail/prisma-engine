use crate::{
    query_builder::{DeleteActions, WriteQueryBuilder},
    Transaction,
};
use connector_interface::filter::Filter;
use prisma_models::{GraphqlId, ModelRef, RelationFieldRef};
use std::sync::Arc;

/// A top level delete that removes records matching the `Filter`. Violating
/// any relations will cause an error.
///
/// Will return the number records deleted.
pub async fn execute(conn: &dyn Transaction, model: ModelRef, filter: &Filter) -> crate::Result<usize> {
    let ids = conn.filter_ids(Arc::clone(&model), filter.clone()).await?;
    let ids: Vec<&GraphqlId> = ids.iter().map(|id| &*id).collect();
    let count = ids.len();

    if count == 0 {
        return Ok(count);
    }

    DeleteActions::check_relation_violations(Arc::clone(&model), ids.as_slice(), |select| {
        async move {
            let ids = conn.select_ids(select).await?;
            Ok(ids.into_iter().next())
        }
    })
    .await?;

    for delete in WriteQueryBuilder::delete_many(model, ids.as_slice()) {
        conn.delete(delete).await?;
    }

    Ok(count)
}

/// Removes nested items matching to filter, or if no filter is given, all
/// nested items related to the given `parent_id`. An error will be thrown
/// if any deleted record is required in a model.
pub async fn execute_nested(
    conn: &dyn Transaction,
    parent_id: &GraphqlId,
    filter: &Option<Filter>,
    relation_field: RelationFieldRef,
) -> crate::Result<usize> {
    let ids = conn
        .filter_ids_by_parents(Arc::clone(&relation_field), vec![parent_id], filter.clone())
        .await?;
    let count = ids.len();

    if count == 0 {
        return Ok(count);
    }

    let ids: Vec<&GraphqlId> = ids.iter().map(|id| &*id).collect();
    let model = relation_field.model();

    DeleteActions::check_relation_violations(model, ids.as_slice(), |select| {
        async move {
            let ids = conn.select_ids(select).await?;
            Ok(ids.into_iter().next())
        }
    })
    .await?;

    for delete in WriteQueryBuilder::delete_many(relation_field.related_model(), ids.as_slice()) {
        conn.delete(delete).await?;
    }

    Ok(count)
}
