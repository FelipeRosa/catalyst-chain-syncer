use std::{future::Future, sync::Arc, time::Duration};

use axum::{extract::MatchedPath, http::Request, Router};
use tokio::{net::ToSocketAddrs, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tower_http::{
    timeout::TimeoutLayer,
    trace::{DefaultOnFailure, DefaultOnRequest, DefaultOnResponse, TraceLayer},
};
use tracing::{info, info_span, Level};

pub mod routes;

pub struct ServerHandle {
    _cancellation_token: tokio_util::sync::DropGuard,
    join_handle: JoinHandle<()>,
}

impl ServerHandle {
    pub fn shutdown(self) -> impl Future<Output = <JoinHandle<()> as Future>::Output> {
        self.join_handle
    }
}

impl Future for ServerHandle {
    type Output = <JoinHandle<()> as Future>::Output;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let join_handle = &mut self.join_handle;
        tokio::pin!(join_handle);

        join_handle.poll(cx)
    }
}

pub async fn start<A>(listen_address: A, database_url: &str) -> anyhow::Result<ServerHandle>
where
    A: ToSocketAddrs + std::fmt::Debug + Send + 'static,
{
    let db_pool = Arc::new(postgres_store::ConnectionPool::new(database_url).await?);
    let cancellation_token = CancellationToken::new();

    let join_handle = tokio::spawn({
        let server_cancellation_token = cancellation_token.child_token();

        async move {
            let app = Router::new()
                .route(
                    "/api/v1/registrations",
                    axum::routing::get(routes::get_registrations),
                )
                .with_state(db_pool)
                .layer(
                    TraceLayer::new_for_http()
                        .make_span_with(|req: &Request<_>| {
                            // TODO: Add request ID?

                            let path = req
                                .extensions()
                                .get::<MatchedPath>()
                                .map(MatchedPath::as_str);

                            info_span!("http_request", method = ?req.method(), path)
                        })
                        .on_request(DefaultOnRequest::new().level(Level::INFO))
                        .on_response(DefaultOnResponse::new().level(Level::INFO))
                        .on_failure(DefaultOnFailure::new().level(Level::ERROR)),
                )
                .layer(TimeoutLayer::new(Duration::from_secs(30)));

            info!(listen_address = ?listen_address, "Starting API server");

            let listener = tokio::net::TcpListener::bind(listen_address)
                .await
                .expect("Listener");

            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    server_cancellation_token.cancelled().await;
                    info!("Stopping server");
                })
                .await
                .expect("Server running");
        }
    });

    Ok(ServerHandle {
        _cancellation_token: cancellation_token.drop_guard(),
        join_handle,
    })
}
