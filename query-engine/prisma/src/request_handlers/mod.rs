pub mod graphql;

pub use core::QuerySchemaRenderer;
pub use graphql::{GraphQlBody, GraphQlRequestHandler};

use crate::{context::PrismaContext};
use serde_json;
use std::{collections::HashMap, sync::Arc, fmt::Debug};
use futures::future::BoxFuture;

pub trait RequestHandler {
    type Body: Debug;

    fn handle<'a, S>(&'a self, req: S, ctx: &'a PrismaContext) -> BoxFuture<'a, serde_json::Value>
    where
        S: Into<PrismaRequest<Self::Body>> + Send + Sync + 'static;
}

#[derive(Debug)]
pub struct PrismaRequest<T: Debug> {
    pub body: T,
    pub headers: HashMap<String, String>,
    pub path: String,
}
