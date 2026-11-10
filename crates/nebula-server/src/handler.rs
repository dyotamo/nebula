//! Implements the protocol-level behavior for cache requests, independent of socket handling.
//!
//! The server delegates each decoded request here so request validation, store operations, and
//! response status mapping stay in one place.

use std::{sync::Arc, time::Instant};

use nebula_protocol::{Opcode, Request, Response};
use tracing::debug;

use nebula_core::CacheStore;

use crate::metrics::Metrics;

/// Applies a request to the cache without recording request metrics.
pub async fn handle(request: Request, store: Arc<CacheStore>) -> Response {
    handle_observed(request, store, None).await
}

/// Applies a request and records its operation, outcome, and elapsed time.
pub async fn handle_with_metrics(
    request: Request,
    store: Arc<CacheStore>,
    metrics: Arc<Metrics>,
) -> Response {
    handle_observed(request, store, Some(metrics)).await
}

async fn handle_observed(
    request: Request,
    store: Arc<CacheStore>,
    metrics: Option<Arc<Metrics>>,
) -> Response {
    let started = Instant::now();
    let operation = match request.opcode {
        Opcode::Ping => "ping",
        Opcode::Set => "set",
        Opcode::Get => "get",
        Opcode::Delete => "delete",
        Opcode::Auth => "auth",
    };
    let response = match request.opcode {
        Opcode::Ping => Response::ok(b"PONG".to_vec()),

        Opcode::Set => {
            if request.key.is_empty() {
                Response::invalid_argument()
            } else {
                if store.set(request.key, request.value, request.ttl_secs) {
                    Response::empty_ok()
                } else {
                    Response::internal_error(b"cache size limit exceeded")
                }
            }
        }

        Opcode::Get => match store.get(&request.key) {
            Some(value) => Response::ok(value),
            None => Response::not_found(),
        },

        Opcode::Delete => {
            if store.delete(&request.key) {
                Response::empty_ok()
            } else {
                Response::not_found()
            }
        }
        Opcode::Auth => Response::invalid_argument(),
    };
    if let Some(metrics) = metrics {
        metrics.record_request(
            operation,
            response.status == nebula_protocol::Status::Ok,
            started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        );
    }
    response
}

/// Emits a debug-level connection event for the remote peer.
pub fn log_connection(peer: std::net::SocketAddr) {
    debug!(%peer, "client connected");
}
