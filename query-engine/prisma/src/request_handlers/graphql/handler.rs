use super::protocol_adapter::GraphQLProtocolAdapter;
use crate::{context::PrismaContext, serializers::json, PrismaRequest, PrismaResult, RequestHandler};
use core::response_ir;
use graphql_parser as gql;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use futures::future::{BoxFuture, FutureExt};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphQlBody {
    query: String,
    operation_name: Option<String>,
    variables: HashMap<String, String>,
}

#[derive(Clone, Copy)]
pub struct GraphQlRequestHandler;

#[allow(unused_variables)]
impl RequestHandler for GraphQlRequestHandler {
    type Body = GraphQlBody;

    fn handle<'a, S>(&'a self, req: S, ctx: &'a PrismaContext) -> BoxFuture<'a, serde_json::Value>
    where
        S: Into<PrismaRequest<Self::Body>> + Send + Sync + 'static
    {
        async move {
            let responses = match handle_graphql_query(req.into(), ctx).await {
                Ok(responses) => responses,
                Err(err) => vec![err.into()],
            };

            json::serialize(responses)
        }.boxed()
    }
}

async fn handle_graphql_query(
    req: PrismaRequest<GraphQlBody>,
    ctx: &PrismaContext,
) -> PrismaResult<Vec<response_ir::Response>> {
    debug!("Incoming GQL query: {:?}", &req.body.query);

    let gql_doc = gql::parse_query(&req.body.query)?;
    let query_doc = GraphQLProtocolAdapter::convert(gql_doc, req.body.operation_name)?;
    let schema = Arc::clone(ctx.query_schema());

    ctx.executor()
        .execute(query_doc, schema)
        .await
        .map_err(|err| {
            debug!("{}", err);
            err.into()
        })
}
