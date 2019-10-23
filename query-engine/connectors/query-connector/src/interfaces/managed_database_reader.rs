use crate::{filter::RecordFinder, query_arguments::QueryArguments};
use prisma_models::prelude::*;
use prisma_models::ScalarFieldRef;

/// Managed interface for fetching data.
pub trait ManagedDatabaseReader {
    /// Find one record.
    fn get_single_record<'a>(
        &'a self,
        record_finder: &'a RecordFinder,
        selected_fields: &'a SelectedFields,
    ) -> crate::IO<'a, Option<SingleRecord>>;

    /// Filter many records.
    fn get_many_records<'a>(
        &'a self,
        model: ModelRef,
        query_arguments: QueryArguments,
        selected_fields: &'a SelectedFields,
    ) -> crate::IO<'a, ManyRecords>;

    /// Filter records related to the parent.
    fn get_related_records<'a>(
        &'a self,
        from_field: RelationFieldRef,
        from_record_ids: &'a [GraphqlId],
        query_arguments: QueryArguments,
        selected_fields: &'a SelectedFields,
    ) -> crate::IO<'a, ManyRecords>;

    /// Fetch scalar list values for the parent.
    fn get_scalar_list_values_by_record_ids(
        &self,
        list_field: ScalarFieldRef,
        record_ids: Vec<GraphqlId>,
    ) -> crate::IO<Vec<ScalarListValues>>;

    /// Count the items in the model with the given arguments.
    fn count_by_model(&self, model: ModelRef, query_arguments: QueryArguments) -> crate::IO<usize>;

    /// Count the items in the table.
    fn count_by_table<'a>(&'a self, database: &'a str, table: &'a str) -> crate::IO<'a, usize>;
}

#[derive(Debug)]
pub struct ScalarListValues {
    pub record_id: GraphqlId,
    pub values: Vec<PrismaValue>,
}
