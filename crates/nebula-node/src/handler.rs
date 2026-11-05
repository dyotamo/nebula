use std::sync::Arc;

use nebula_protocol::{Opcode, Request, Response};
use tracing::debug;

use crate::engine::CacheStore;

pub async fn handle(request: Request, store: Arc<CacheStore>) -> Response {
    match request.opcode {
        Opcode::Ping => Response::ok(b"PONG".to_vec()),
        Opcode::Set => {
            if request.key.is_empty() {
                return Response::invalid_argument();
            }
            store.set(request.key, request.value, request.ttl_secs);
            Response::empty_ok()
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
    }
}

pub fn log_connection(peer: std::net::SocketAddr) {
    debug!(%peer, "client connected");
}
