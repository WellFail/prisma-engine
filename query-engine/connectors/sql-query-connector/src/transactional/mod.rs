mod managed_database_reader;
mod unmanaged_database_writer;

pub use managed_database_reader::*;
pub use unmanaged_database_writer::*;

use crate::{error::*, query_builder::ReadQueryBuilder, AliasedCondition, RawQuery, SqlRow, ToSqlRow, SQLIO};
use connector_interface::{
    error::RecordFinderInfo,
    filter::{Filter, RecordFinder},
};
use prisma_models::*;
use prisma_query::{
    ast::*,
    connector::{self, Queryable},
};
use serde_json::{Map, Number, Value};
use std::{convert::TryFrom, sync::Arc};

pub trait Transactional {
    fn get_connection<'a>(&'a self, db: &'a str) -> SQLIO<'a, Box<dyn Transaction>>;
}

impl<'t> Transaction for connector::Transaction<'t> {}

pub trait Transaction: Queryable + Send {
    fn filter<'a>(&'a self, q: Query<'a>, idents: &'a [TypeIdentifier]) -> SQLIO<'a, Vec<SqlRow>> {
        SQLIO::new(async move {
            let result_set = self.query(q).await?;
            let mut sql_rows = Vec::new();

            for row in result_set {
                sql_rows.push(row.to_sql_row(idents)?);
            }

            Ok(sql_rows)
        })
    }

    fn raw_json<'a>(&'a self, q: RawQuery) -> SQLIO<'a, Value> {
        SQLIO::new(async move {
            if q.is_select() {
                let result_set = self.query_raw(q.0.as_str(), &[]).await?;
                let columns: Vec<String> = result_set.columns().map(ToString::to_string).collect();
                let mut result = Vec::new();

                for row in result_set.into_iter() {
                    let mut object = Map::new();

                    for (idx, p_value) in row.into_iter().enumerate() {
                        let column_name: String = columns[idx].clone();
                        object.insert(column_name, Value::from(p_value));
                    }

                    result.push(Value::Object(object));
                }

                Ok(Value::Array(result))
            } else {
                let changes = self.execute_raw(q.0.as_str(), &[]).await?;
                Ok(Value::Number(Number::from(changes)))
            }
        })
    }

    /// Find one full record selecting all scalar fields.
    fn find_record<'a>(&'a self, record_finder: &'a RecordFinder) -> SQLIO<'a, SingleRecord> {
        use SqlError::*;

        SQLIO::new(async move {
            let model = record_finder.field.model();
            let selected_fields = SelectedFields::from(Arc::clone(&model));
            let select = ReadQueryBuilder::get_records(model, &selected_fields, record_finder);
            let idents = selected_fields.type_identifiers();

            let row = self.find(select, idents.as_slice()).await.map_err(|e| match e {
                RecordDoesNotExist => RecordNotFoundForWhere(RecordFinderInfo::from(record_finder)),
                e => e,
            })?;

            let record = Record::from(row);

            Ok(SingleRecord::new(record, selected_fields.names()))
        })
    }

    /// Select one row from the database.
    fn find<'a>(&'a self, q: Select<'a>, idents: &'a [TypeIdentifier]) -> SQLIO<'a, SqlRow> {
        SQLIO::new(async move {
            self.filter(q.limit(1).into(), idents)
                .await?
                .into_iter()
                .next()
                .ok_or(SqlError::RecordDoesNotExist)
        })
    }

    /// Read the first column from the first row as an integer.
    fn find_int<'a>(&'a self, q: Select<'a>) -> SQLIO<'a, i64> {
        SQLIO::new(async move {
            // UNWRAP: A dataset will always have at least one column, even if it contains no data.
            let id = self.find(q, &[TypeIdentifier::Int]).await?.values.into_iter().next().unwrap();

            Ok(i64::try_from(id)?)
        })
    }

    /// Read the first column from the first row as an `GraphqlId`.
    fn find_id<'a>(&'a self, record_finder: &'a RecordFinder) -> SQLIO<'a, GraphqlId> {
        SQLIO::new(async move {
            let model = record_finder.field.model();
            let filter = Filter::from(record_finder.clone());

            let id = self
                .filter_ids(model, filter)
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| SqlError::RecordNotFoundForWhere(RecordFinderInfo::from(record_finder)))?;

            Ok(id)
        })
    }

    /// Read the all columns as an `GraphqlId`
    fn filter_ids<'a>(&'a self, model: ModelRef, filter: Filter) -> SQLIO<'a, Vec<GraphqlId>> {
        let select = Select::from_table(model.table())
            .column(model.fields().id().as_column())
            .so_that(filter.aliased_cond(None));

        self.select_ids(select)
    }

    fn select_ids<'a>(&'a self, select: Select<'a>) -> SQLIO<'a, Vec<GraphqlId>> {
        SQLIO::new(async move {
            let mut rows = self.filter(select.into(), &[TypeIdentifier::GraphQLID]).await?;
            let mut result = Vec::new();

            for mut row in rows.drain(0..) {
                for value in row.values.drain(0..) {
                    result.push(GraphqlId::try_from(value)?)
                }
            }

            Ok(result)
        })
    }

    /// Find a child of a parent. Will return an error if no child found with
    /// the given parameters. A more restrictive version of `get_ids_by_parents`.
    fn find_id_by_parent<'a>(
        &'a self,
        parent_field: RelationFieldRef,
        parent_id: &'a GraphqlId,
        selector: &'a Option<RecordFinder>,
    ) -> SQLIO<'a, GraphqlId> {
        SQLIO::new(async move {
            let ids = self.filter_ids_by_parents(
                Arc::clone(&parent_field),
                vec![parent_id],
                selector.clone().map(Filter::from),
            ).await?;

            let id = ids.into_iter().next().ok_or_else(|| SqlError::RecordsNotConnected {
                relation_name: parent_field.relation().name.clone(),
                parent_name: parent_field.model().name.clone(),
                parent_where: None,
                child_name: parent_field.related_model().name.clone(),
                child_where: selector.as_ref().map(RecordFinderInfo::from).map(Box::new),
            })?;

            Ok(id)
        })
    }

    /// Find all children record id's with the given parent id's, optionally given
    /// a `Filter` for extra filtering.
    fn filter_ids_by_parents<'a>(
        &'a self,
        parent_field: RelationFieldRef,
        parent_ids: Vec<&'a GraphqlId>,
        selector: Option<Filter>,
    ) -> SQLIO<'a, Vec<GraphqlId>> {
        let related_model = parent_field.related_model();
        let relation = parent_field.relation();
        let child_id_field = relation.column_for_relation_side(parent_field.relation_side.opposite());
        let parent_id_field = relation.column_for_relation_side(parent_field.relation_side);

        let subselect = Select::from_table(relation.relation_table())
            .column(child_id_field)
            .so_that(parent_id_field.in_selection(parent_ids));

        let conditions = related_model
            .fields()
            .id()
            .db_name()
            .to_string()
            .in_selection(subselect);

        let conditions = match selector {
            Some(into_cond) => {
                let filter: Filter = into_cond.into();
                conditions.and(filter.aliased_cond(None))
            }
            None => conditions.into(),
        };

        let select = Select::from_table(related_model.table())
            .column(related_model.fields().id().as_column())
            .so_that(conditions);

        self.select_ids(select)
    }
}
