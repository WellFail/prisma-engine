use crate::error::SqlError;
use prisma_models::prelude::*;
use prisma_query::ast::*;
use std::future::Future;

/// Checks to be executed when deleting data.
pub struct DeleteActions;

impl DeleteActions {
    /// A model can be required in another model, preventing the deletion.
    /// Therefore we must check if any other model in the data model sets
    /// `is_required` for this record.
    ///
    /// The closure is called with a `SELECT` statement to be executed in the
    /// connector, giving the connector the possibility to return an optional
    /// `GraphqlID` from the database, such as trying to read a row from the
    /// `SELECT`.
    pub async fn check_relation_violations<'a, F, U>(model: ModelRef, ids: &'a [&'a GraphqlId], f: F) -> crate::Result<()>
    where
        F: Fn(Select<'a>) -> U + Send + 'a,
        U: Future<Output = crate::Result<Option<GraphqlId>>> + Send,
    {
        for rf in model.internal_data_model().fields_requiring_model(model) {
            let relation = rf.relation();

            let condition = rf
                .opposite_column()
                .in_selection(ids.to_vec())
                .and(rf.relation_column().is_not_null());

            let select = Select::from_table(relation.relation_table())
                .column(rf.opposite_column())
                .so_that(condition);

            if let Some(_) = f(select).await? {
                return Err(SqlError::RelationViolation {
                    relation_name: relation.name.clone(),
                    model_a_name: relation.model_a().name.clone(),
                    model_b_name: relation.model_b().name.clone(),
                });
            }
        }

        Ok(())
    }
}
