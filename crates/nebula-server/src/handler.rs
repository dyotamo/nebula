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
///
/// The caller must enforce readiness, authentication, and authorization before dispatching here.
pub async fn handle(request: Request, store: Arc<CacheStore>) -> Response {
    handle_observed(request, store, None).await
}

/// Applies a request and records its operation, outcome, and elapsed time.
///
/// Like [`handle`], this does not enforce connection-level access controls. Only `Ok` responses
/// count as successful requests; cache misses are recorded as non-successes.
pub async fn handle_with_metrics(
    request: Request,
    store: Arc<CacheStore>,
    metrics: Arc<Metrics>,
) -> Response {
    handle_observed(request, store, Some(metrics)).await
}

/// Shares dispatch between observed and unobserved callers so status mapping cannot diverge.
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
            // Empty keys are a request error; capacity rejection is reported separately below.
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

        // Store reads apply expiration semantics, so expired values surface as ordinary misses.
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
        // AUTH is consumed by the connection handshake, not by cache operation dispatch.
        Opcode::Auth => Response::invalid_argument(),
    };
    // Measure validation and store work only, excluding frame I/O and response serialization.
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
