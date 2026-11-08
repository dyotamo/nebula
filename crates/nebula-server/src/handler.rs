use std::{sync::Arc, time::Instant};

use nebula_protocol::{Opcode, Request, Response};
use tracing::debug;

use nebula_core::CacheStore;

use crate::metrics::Metrics;

pub async fn handle(request: Request, store: Arc<CacheStore>) -> Response {
    handle_observed(request, store, None).await
}

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

pub fn log_connection(peer: std::net::SocketAddr) {
    debug!(%peer, "client connected");
}
