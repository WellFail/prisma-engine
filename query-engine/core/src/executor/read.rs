use crate::CoreResult;
use connector::{self, query_ast::*, result_ast::*, ManagedDatabaseReader, QueryArguments, ScalarListValues};
use futures::future::{BoxFuture, FutureExt};
use prisma_models::{GraphqlId, ScalarField, SelectedFields};
use std::sync::Arc;

pub struct ReadQueryExecutor {
    pub data_resolver: Arc<dyn ManagedDatabaseReader + Send + Sync + 'static>,
}

impl ReadQueryExecutor {
    pub fn execute<'a>(
        &'a self,
        query: ReadQuery,
        parent_ids: &'a [GraphqlId],
    ) -> BoxFuture<'a, CoreResult<ReadQueryResult>> {
        async move {
            match query {
                ReadQuery::RecordQuery(q) => self.read_one(q).await,
                ReadQuery::ManyRecordsQuery(q) => self.read_many(q).await,
                ReadQuery::RelatedRecordsQuery(q) => self.read_related(q, parent_ids).await,
                ReadQuery::AggregateRecordsQuery(q) => self.aggregate(q).await,
            }
        }
            .boxed()
    }

    /// Queries a single record.
    pub fn read_one<'a>(&'a self, query: RecordQuery) -> BoxFuture<'a, CoreResult<ReadQueryResult>> {
        async move {
            let selected_fields = Self::inject_required_fields(query.selected_fields.clone());

            let scalars = self
                .data_resolver
                .get_single_record(query.record_finder.as_ref().unwrap(), &selected_fields)
                .await?;

            let model = Arc::clone(&query.record_finder.unwrap().field.model());
            let id_field = model.fields().id().name.clone();

            match scalars {
                Some(record) => {
                    let ids = vec![record.collect_id(&id_field)?];
                    let list_fields = selected_fields.scalar_lists();

                    let lists = self.resolve_scalar_list_fields(ids.clone(), list_fields).await?;
                    let mut nested = Vec::new();

                    for q in query.nested.into_iter() {
                        nested.push(self.execute(q, &ids).await?)
                    }

                    Ok(ReadQueryResult {
                        name: query.name,
                        alias: query.alias,
                        content: ResultContent::RecordSelection(RecordSelection {
                            fields: query.selection_order,
                            scalars: record.into(),
                            nested,
                            lists,
                            id_field,
                            ..Default::default()
                        }),
                    })
                }
                None => Ok(ReadQueryResult {
                    name: query.name,
                    alias: query.alias,
                    content: ResultContent::RecordSelection(RecordSelection {
                        fields: query.selection_order,
                        id_field,
                        ..Default::default()
                    }),
                }),
            }
        }
            .boxed()
    }

    /// Queries a set of records.
    pub fn read_many<'a>(&'a self, query: ManyRecordsQuery) -> BoxFuture<'a, CoreResult<ReadQueryResult>> {
        async move {
            let selected_fields = Self::inject_required_fields(query.selected_fields.clone());

            let scalars = self
                .data_resolver
                .get_many_records(Arc::clone(&query.model), query.args.clone(), &selected_fields)
                .await?;

            let model = Arc::clone(&query.model);
            let id_field = model.fields().id().name.clone();
            let ids = scalars.collect_ids(&id_field)?;

            let list_fields = selected_fields.scalar_lists();
            let lists = self.resolve_scalar_list_fields(ids.clone(), list_fields).await?;
            let mut nested = Vec::new();

            for q in query.nested.into_iter() {
                nested.push(self.execute(q, &ids).await?)
            }

            Ok(ReadQueryResult {
                name: query.name,
                alias: query.alias,
                content: ResultContent::RecordSelection(RecordSelection {
                    fields: query.selection_order,
                    query_arguments: query.args,
                    scalars,
                    nested,
                    lists,
                    id_field,
                }),
            })
        }
            .boxed()
    }

    /// Queries related records for a set of parent IDs.
    pub fn read_related<'a>(
        &'a self,
        query: RelatedRecordsQuery,
        parent_ids: &'a [GraphqlId],
    ) -> BoxFuture<'a, CoreResult<ReadQueryResult>> {
        async move {
            let selected_fields = Self::inject_required_fields(query.selected_fields.clone());

            let scalars = self
                .data_resolver
                .get_related_records(
                    Arc::clone(&query.parent_field),
                    parent_ids,
                    query.args.clone(),
                    &selected_fields,
                )
                .await?;

            let model = Arc::clone(&query.parent_field.related_model());
            let id_field = model.fields().id().name.clone();
            let ids = scalars.collect_ids(&id_field)?;

            let list_fields = selected_fields.scalar_lists();
            let lists = self.resolve_scalar_list_fields(ids.clone(), list_fields).await?;

            let mut nested = Vec::new();
            for q in query.nested.into_iter() {
                nested.push(self.execute(q, &ids).await?)
            }

            Ok(ReadQueryResult {
                name: query.name,
                alias: query.alias,
                content: ResultContent::RecordSelection(RecordSelection {
                    fields: query.selection_order,
                    query_arguments: query.args,
                    scalars,
                    nested,
                    lists,
                    id_field,
                }),
            })
        }
            .boxed()
    }

    pub async fn aggregate(&self, query: AggregateRecordsQuery) -> CoreResult<ReadQueryResult> {
        let result = self
            .data_resolver
            .count_by_model(query.model, QueryArguments::default())
            .await?;

        Ok(ReadQueryResult {
            name: query.name,
            alias: query.alias,
            content: ResultContent::Count(result),
        })
    }

    /// Resolves scalar lists for a list field for a set of parent IDs.
    async fn resolve_scalar_list_fields(
        &self,
        record_ids: Vec<GraphqlId>,
        list_fields: Vec<Arc<ScalarField>>,
    ) -> connector::Result<Vec<(String, Vec<ScalarListValues>)>> {
        let mut results = Vec::new();

        for list_field in list_fields.into_iter() {
            let name = list_field.name.clone();

            let values = self
                .data_resolver
                .get_scalar_list_values_by_record_ids(list_field, record_ids.clone())
                .await?;

            results.push((name, values))
        }

        Ok(results)
    }

    /// Injects fields required for querying, if they're not already in the selection set.
    /// Currently, required fields for every query are:
    /// - ID field
    fn inject_required_fields(mut selected_fields: SelectedFields) -> SelectedFields {
        let id_field = selected_fields.model().fields().id();

        if selected_fields
            .scalar
            .iter()
            .find(|f| f.field.name == id_field.name)
            .is_none()
        {
            selected_fields.add_scalar(id_field);
        }

        selected_fields
    }
}
