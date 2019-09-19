#[cfg(test)]
mod tests;

use super::dmmf;
use crate::{
    context::PrismaContext,
    request_handlers::{
        graphql::{GraphQLSchemaRenderer, GraphQlBody, GraphQlRequestHandler},
        PrismaRequest, RequestHandler,
    },
    PrismaResult,
};
use serde_json::json;
use tide::{error::ResultExt, response, App, Context, EndpointResult};
use http::response::Response;
use core::schema::QuerySchemaRenderer;
use std::{sync::Arc, time::Instant};

#[derive(RustEmbed)]
#[folder = "query-engine/prisma/static_files"]
struct StaticFiles;

#[derive(DebugStub)]
pub(crate) struct RequestContext {
    context: PrismaContext,
    #[debug_stub = "#GraphQlRequestHandler#"]
    graphql_request_handler: GraphQlRequestHandler,
}

pub struct HttpServer;

impl HttpServer {
    pub async fn run(address: (&'static str, u16), legacy_mode: bool) -> PrismaResult<()>
    {
        let now = Instant::now();

        let request_context = RequestContext {
            context: PrismaContext::new(legacy_mode)?,
            graphql_request_handler: GraphQlRequestHandler,

        let mut app = App::with_state(request_context);
        app.at("/").get(Self::playground_handler).post(Self::http_handler);
        app.at("/sdl").get(Self::sdl_handler);
        app.at("/dmmf").get(Self::dmmf_handler);
        app.at("/status").get(Self::status_handler);
        app.at("/server_info").get(Self::status_handler);

        trace!("Initialized in {}ms", now.elapsed().as_millis());
        info!("Started http server on {}:{}", address.0, address.1);

        app.serve(address).await?;

        Ok(())
    }

    async fn http_handler(mut cx: Context<RequestContext>) -> EndpointResult {
        let body: GraphQlBody = cx.body_json().await.client_err()?;

        let req = PrismaRequest {
            body,
            path: cx.uri().path().into(),
            headers: cx
                .headers()
                .iter()
                .map(|(k, v)| (format!("{}", k), v.to_str().unwrap().into()))
                .collect(),
        };

        let handler = cx.state().graphql_request_handler;
        let result = handler.handle(req, &cx.state().context);

        Ok(response::json(result))
    }

    async fn playground_handler(_: Context<RequestContext>) -> Response<Vec<u8>> {
        let index_html = StaticFiles::get("playground.html").unwrap();

        Response::builder()
            .status(200)
            .header("Content-Type", "text/html")
            .body(index_html.into_owned())
            .unwrap()
    }

    /// Handler for the playground to work with the SDL-rendered query schema.
    /// Serves a raw SDL string created from the query schema.
    async fn sdl_handler(cx: Context<RequestContext>) -> Response<String> {
        let request_context = cx.state();
        let rendered = GraphQLSchemaRenderer::render(Arc::clone(&request_context.context().query_schema()));

        Response::builder()
            .status(200)
            .header("Content-Type", "application/text")
            .body(rendered)
            .unwrap()
    }

    /// Renders the Data Model Meta Format.
    /// Only callable if prisma was initialized using a v2 data model.
    async fn dmmf_handler(cx: Context<RequestContext>) -> EndpointResult {
        let request_context = cx.state();

        let dmmf = dmmf::render_dmmf(
            request_context.context().datamodel(),
            Arc::clone(request_context.context().query_schema()),
        );

        Ok(response::json(dmmf))
    }

    /// Simple status endpoint
    async fn server_info_handler(cx: Context<RequestContext>) -> EndpointResult {
        let request_context = cx.state();

        let response = json!({
            "commit": env!("GIT_HASH"),
            "version": env!("CARGO_PKG_VERSION"),
            "primary_connector": request_context.context().primary_connector(),
        });

        Ok(response::json(response))
    }

    async fn status_handler(_: Context<RequestContext>) -> EndpointResult {
        Ok(response::json(json!({"status": "ok"})))
    }
}

