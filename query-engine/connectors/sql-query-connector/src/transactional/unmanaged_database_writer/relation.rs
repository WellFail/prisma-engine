use crate::{
    query_builder::{NestedActions, WriteQueryBuilder},
    Transaction,
};
use connector_interface::filter::RecordFinder;
use prisma_models::{GraphqlId, RelationFieldRef};
use std::sync::Arc;

/// Connect a record to the parent.
///
/// When nested with a create, will have special behaviour in some cases:
///
/// | action                               | p is a list | p is required | c is list | c is required |
/// | ------------------------------------ | ----------- | ------------- | --------- | ------------- |
/// | relation violation                   | false       | true          | false     | true          |
/// | check if connected to another parent | false       | true          | false     | false         |
///
/// When nesting to an action that is not a create:
///
/// | action                               | p is a list | p is required | c is list | c is required |
/// | ------------------------------------ | ----------- | ------------- | --------- | ------------- |
/// | relation violation                   | false       | true          | false     | true          |
/// | check if connected to another parent | false       | true          | false     | false         |
/// | check if parent has another child    | false       | true          | false     | false         |
///
/// If none of the checks fail, the record will be disconnected to the
/// previous relation before connecting to the given parent.
pub async fn connect(
    conn: &dyn Transaction,
    parent_id: &GraphqlId,
    actions: &dyn NestedActions,
    record_finder: &RecordFinder,
    relation_field: RelationFieldRef,
) -> crate::Result<()> {
    if let Some((select, check)) = actions.required_check(parent_id)? {
        let ids = conn.select_ids(select).await?;
        check(ids.into_iter().next().is_some())?
    }

    let child_id = conn.find_id(record_finder).await?;

    if let Some(query) = actions.parent_removal(parent_id) {
        conn.execute(query).await?;
    }

    if let Some(query) = actions.child_removal(&child_id) {
        conn.execute(query).await?;
    }

    let relation_query = WriteQueryBuilder::create_relation(relation_field, parent_id, &child_id);
    conn.execute(relation_query).await?;

    Ok(())
}

/// Disconnect a record from the parent.
///
/// The following cases will lead to a relation violation error:
///
/// | p is a list | p is required | c is list | c is required |
/// | ----------- | ------------- | --------- | ------------- |
/// | false       | true          | false     | true          |
/// | false       | true          | false     | false         |
/// | false       | false         | false     | true          |
/// | true        | false         | false     | true          |
/// | false       | true          | true      | false         |
pub async fn disconnect(
    conn: &dyn Transaction,
    parent_id: &GraphqlId,
    actions: &dyn NestedActions,
    record_finder: &Option<RecordFinder>,
) -> crate::Result<()> {
    if let Some((select, check)) = actions.required_check(parent_id)? {
        let ids = conn.select_ids(select).await?;
        check(ids.into_iter().next().is_some())?
    }

    match record_finder {
        None => {
            let (select, check) = actions.ensure_parent_is_connected(parent_id);

            let ids = conn.select_ids(select).await?;
            check(ids.into_iter().next().is_some())?;

            conn.execute(actions.removal_by_parent(parent_id)).await?;
        }
        Some(ref selector) => {
            let child_id = conn.find_id(selector).await?;
            let (select, check) = actions.ensure_connected(parent_id, &child_id);

            let ids = conn.select_ids(select).await?;
            check(ids.into_iter().next().is_some())?;

            conn.execute(actions.removal_by_parent_and_child(parent_id, &child_id))
                .await?;
        }
    }

    Ok(())
}

/// Connects multiple records into the parent. Rules from `execute_connect`
/// apply.
pub async fn set(
    conn: &dyn Transaction,
    parent_id: &GraphqlId,
    actions: &dyn NestedActions,
    record_finders: &Vec<RecordFinder>,
    relation_field: RelationFieldRef,
) -> crate::Result<()> {
    if let Some((select, check)) = actions.required_check(parent_id)? {
        let ids = conn.select_ids(select).await?;
        check(ids.into_iter().next().is_some())?
    }

    conn.execute(actions.removal_by_parent(parent_id)).await?;

    for selector in record_finders {
        let child_id = conn.find_id(selector).await?;

        if !relation_field.is_list {
            conn.execute(actions.removal_by_child(&child_id)).await?;
        }

        let relation_query = WriteQueryBuilder::create_relation(Arc::clone(&relation_field), parent_id, &child_id);
        conn.execute(relation_query).await?;
    }

    Ok(())
}
