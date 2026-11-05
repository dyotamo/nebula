use std::{io, sync::Arc};

use axum::{
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use serde::Deserialize;
use tokio::net::TcpListener;
use tracing::info;

use crate::engine::CacheStore;

pub async fn run(listener: TcpListener, store: Arc<CacheStore>) -> io::Result<()> {
    info!(address = %listener.local_addr()?, "NebulaCache REST API listening");
    axum::serve(listener, router(store)).await
}

fn router(store: Arc<CacheStore>) -> Router {
    Router::new()
        .route("/v1/ping", get(ping))
        .route(
            "/v1/keys/{key}",
            get(get_key).put(set_key).delete(delete_key),
        )
        .with_state(store)
}

async fn ping() -> &'static str {
    "PONG"
}

async fn get_key(State(store): State<Arc<CacheStore>>, Path(key): Path<String>) -> Response {
    match store.get(key.as_bytes()) {
        Some(value) => (StatusCode::OK, Body::from(value)).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn set_key(
    State(store): State<Arc<CacheStore>>,
    Path(key): Path<String>,
    Query(options): Query<SetOptions>,
    body: Bytes,
) -> StatusCode {
    store.set(key.into_bytes(), body.to_vec(), options.ttl_secs);
    StatusCode::NO_CONTENT
}

async fn delete_key(State(store): State<Arc<CacheStore>>, Path(key): Path<String>) -> StatusCode {
    if store.delete(key.as_bytes()) {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct SetOptions {
    ttl_secs: u64,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    use super::router;
    use crate::engine::CacheStore;

    async fn request(address: std::net::SocketAddr, request: &str) -> String {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();

        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8_lossy(&response).into_owned()
    }

    #[tokio::test]
    async fn rest_api_supports_ping_and_key_operations() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let store = Arc::new(CacheStore::new());
        let server = tokio::spawn(async move {
            axum::serve(listener, router(store)).await.unwrap();
        });

        let ping = request(
            address,
            "GET /v1/ping HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(ping.starts_with("HTTP/1.1 200"));
        assert!(ping.ends_with("PONG"));

        let set = request(
            address,
            "PUT /v1/keys/greeting?ttl_secs=0 HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
        )
        .await;
        assert!(set.starts_with("HTTP/1.1 204"));

        let get = request(
            address,
            "GET /v1/keys/greeting HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(get.starts_with("HTTP/1.1 200"));
        assert!(get.ends_with("hello"));

        let delete = request(
            address,
            "DELETE /v1/keys/greeting HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(delete.starts_with("HTTP/1.1 204"));

        let missing = request(
            address,
            "GET /v1/keys/greeting HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(missing.starts_with("HTTP/1.1 404"));

        server.abort();
    }
}
