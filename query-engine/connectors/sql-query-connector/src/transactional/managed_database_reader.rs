use crate::{
    database::{SqlCapabilities, SqlDatabase},
    error::SqlError,
    query_builder::read::{ManyRelatedRecordsBaseQuery, ManyRelatedRecordsQueryBuilder, ReadQueryBuilder},
    Transactional,
};

use connector_interface::{self as iface, error::ConnectorError, filter::RecordFinder, *};
use itertools::Itertools;
use prisma_models::*;
use std::convert::TryFrom;

struct ScalarListElement {
    record_id: GraphqlId,
    value: PrismaValue,
}

impl<T> ManagedDatabaseReader for SqlDatabase<T>
where
    T: Transactional + SqlCapabilities + Send + Sync + 'static,
{
    fn get_single_record<'a>(
        &'a self,
        record_finder: &'a RecordFinder,
        selected_fields: &'a SelectedFields,
    ) -> iface::IO<'a, Option<SingleRecord>> {
        iface::IO::new(async move {
            let db_name = &record_finder.field.model().internal_data_model().db_name;
            let query = ReadQueryBuilder::get_records(record_finder.field.model(), selected_fields, record_finder);

            let field_names = selected_fields.names();
            let idents = selected_fields.type_identifiers();
            let conn = self.executor.get_connection(db_name).await?;

            let result = match conn.find(query, idents.as_slice()).await {
                Ok(result) => Ok(Some(result)),
                Err(_e @ SqlError::RecordNotFoundForWhere(_)) => Ok(None),
                Err(e) => Err(e),
            }?;

            Ok(result.map(|r| SingleRecord {
                record: Record::from(r),
                field_names,
            }))
        })
    }

    fn get_many_records<'a>(
        &'a self,
        model: ModelRef,
        query_arguments: QueryArguments,
        selected_fields: &'a SelectedFields,
    ) -> iface::IO<'a, ManyRecords> {
        iface::IO::new(async move {
            let db_name = &model.internal_data_model().db_name;
            let field_names = selected_fields.names();

            let idents = selected_fields.type_identifiers();
            let query = ReadQueryBuilder::get_records(model, selected_fields, query_arguments);

            let conn = self.executor.get_connection(db_name).await?;
            let records = conn.filter(query.into(), idents.as_slice()).await?;
            let records = records.into_iter().map(Record::from).collect();

            Ok(ManyRecords { records, field_names })
        })
    }

    fn get_related_records<'a>(
        &'a self,
        from_field: RelationFieldRef,
        from_record_ids: &'a [GraphqlId],
        query_arguments: QueryArguments,
        selected_fields: &'a SelectedFields,
    ) -> iface::IO<'a, ManyRecords> {
        iface::IO::new(async move {
            let db_name = &from_field.model().internal_data_model().db_name;
            let idents = selected_fields.type_identifiers();
            let field_names = selected_fields.names();

            let query = {
                let is_with_pagination = query_arguments.is_with_pagination();
                let base = ManyRelatedRecordsBaseQuery::new(
                    from_field,
                    from_record_ids.to_vec(),
                    query_arguments,
                    selected_fields.clone(),
                );

                if is_with_pagination {
                    T::ManyRelatedRecordsBuilder::with_pagination(base)
                } else {
                    T::ManyRelatedRecordsBuilder::without_pagination(base)
                }
            };

            let conn = self.executor.get_connection(db_name).await?;
            let records = conn.filter(query, idents.as_slice()).await?;

            let records: connector_interface::Result<Vec<Record>> = records
                .into_iter()
                .map(|mut row| {
                    let parent_id = row.values.pop().ok_or(ConnectorError::ColumnDoesNotExist)?;

                    // Relation id is always the second last value. We don't need it
                    // here and we don't need it in the record.
                    let _ = row.values.pop();

                    let mut record = Record::from(row);
                    record.add_parent_id(GraphqlId::try_from(parent_id)?);

                    Ok(record)
                })
                .collect();

            Ok(ManyRecords {
                records: records?,
                field_names,
            })
        })
    }

    fn count_by_model(&self, model: ModelRef, query_arguments: QueryArguments) -> iface::IO<usize> {
        iface::IO::new(async move {
            let db_name = &model.internal_data_model().db_name;
            let query = ReadQueryBuilder::count_by_model(model, query_arguments);
            let conn = self.executor.get_connection(db_name).await?;

            Ok(conn.find_int(query).await? as usize)
        })
    }

    fn count_by_table<'a>(&'a self, database: &'a str, table: &'a str) -> iface::IO<'a, usize> {
        iface::IO::new(async move {
            let query = ReadQueryBuilder::count_by_table(database, table);
            let conn = self.executor.get_connection(database).await?;

            Ok(conn.find_int(query).await? as usize)
        })
    }

    fn get_scalar_list_values_by_record_ids(
        &self,
        list_field: ScalarFieldRef,
        record_ids: Vec<GraphqlId>,
    ) -> iface::IO<Vec<ScalarListValues>> {
        iface::IO::new(async move {
            let db_name = &list_field.model().internal_data_model().db_name;
            let type_identifier = list_field.type_identifier;

            let query = ReadQueryBuilder::get_scalar_list_values_by_record_ids(list_field, record_ids);
            let conn = self.executor.get_connection(db_name).await?;

            let rows = conn
                .filter(query.into(), &[TypeIdentifier::GraphQLID, type_identifier])
                .await?;

            let results: connector_interface::Result<Vec<ScalarListElement>> = rows
                .into_iter()
                .map(|row| {
                    let mut iter = row.values.into_iter();

                    let record_id = iter.next().ok_or(SqlError::ColumnDoesNotExist)?;
                    let value = iter.next().ok_or(SqlError::ColumnDoesNotExist)?;

                    Ok(ScalarListElement {
                        record_id: GraphqlId::try_from(record_id)?,
                        value,
                    })
                })
                .collect();

            let mut list_values = Vec::new();

            for (record_id, elements) in &results?.into_iter().group_by(|ele| ele.record_id.clone()) {
                let values = ScalarListValues {
                    record_id,
                    values: elements.into_iter().map(|e| e.value).collect(),
                };

                list_values.push(values);
            }

            Ok(list_values)
        })
    }
}
