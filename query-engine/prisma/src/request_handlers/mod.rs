pub mod graphql;

pub use core::QuerySchemaRenderer;
pub use graphql::{GraphQlBody, GraphQlRequestHandler};

use crate::{context::PrismaContext};
use serde_json;
use std::collections::HashMap;
use futures::future::BoxFuture;

pub trait RequestHandler {
    type Body;

    fn handle<'a, S>(&'a self, req: S, ctx: &'a PrismaContext) -> BoxFuture<'a, serde_json::Value>
    where
        S: Into<PrismaRequest<Self::Body>> + Send + Sync + 'static;
}

pub struct PrismaRequest<T> {
    pub body: T,
    pub headers: HashMap<String, String>,
    pub path: String,
}
