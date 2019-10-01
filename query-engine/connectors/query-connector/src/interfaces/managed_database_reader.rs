use crate::{filter::RecordFinder, query_arguments::QueryArguments};
use prisma_models::prelude::*;
use prisma_models::ScalarFieldRef;
use futures::future::BoxFuture;

/// Managed interface for fetching data.
pub trait ManagedDatabaseReader {
    /// Find one record.
    fn get_single_record<'a>(
        &'a self,
        record_finder: &'a RecordFinder,
        selected_fields: &'a SelectedFields,
    ) -> BoxFuture<'a, crate::Result<Option<SingleRecord>>>;

    /// Filter many records.
    fn get_many_records(
        &self,
        model: ModelRef,
        query_arguments: QueryArguments,
        selected_fields: &SelectedFields,
    ) -> BoxFuture<'static, crate::Result<ManyRecords>>;

    /// Filter records related to the parent.
    fn get_related_records(
        &self,
        from_field: RelationFieldRef,
        from_record_ids: &[GraphqlId],
        query_arguments: QueryArguments,
        selected_fields: &SelectedFields,
    ) -> BoxFuture<'static, crate::Result<ManyRecords>>;

    /// Fetch scalar list values for the parent.
    fn get_scalar_list_values_by_record_ids(
        &self,
        list_field: ScalarFieldRef,
        record_ids: Vec<GraphqlId>,
    ) -> BoxFuture<'static, crate::Result<Vec<ScalarListValues>>>;

    /// Count the items in the model with the given arguments.
    fn count_by_model(&self, model: ModelRef, query_arguments: QueryArguments) -> BoxFuture<'static, crate::Result<usize>>;

    /// Count the items in the table.
    fn count_by_table(&self, database: &str, table: &str) -> BoxFuture<'static, crate::Result<usize>>;
}

#[derive(Debug)]
pub struct ScalarListValues {
    pub record_id: GraphqlId,
    pub values: Vec<PrismaValue>,
}
