use std::{sync::Arc, time::Duration};

use nebula_core::CacheStore;
use nebula_protocol::{Opcode, Request, Status};
use nebula_server::handler;

// Exercise the public handler boundary without socket or authentication setup.
#[tokio::test]
async fn set_then_get_returns_value() {
    let store = Arc::new(CacheStore::new());

    let set = Request {
        opcode: Opcode::Set,
        key: b"hello".to_vec(),
        value: b"world".to_vec(),
        ttl_secs: 0,
    };
    let response = handler::handle(set, Arc::clone(&store)).await;
    assert_eq!(response.status, Status::Ok);

    let get = Request {
        opcode: Opcode::Get,
        key: b"hello".to_vec(),
        value: Vec::new(),
        ttl_secs: 0,
    };
    let response = handler::handle(get, store).await;
    assert_eq!(response.value, b"world");
}

// Verify that a live cache entry becomes unreadable after its real-time TTL elapses.
#[tokio::test]
async fn ttl_expires_entry() {
    let store = Arc::new(CacheStore::new());
    store.set(b"temporary".to_vec(), b"value".to_vec(), 1);
    // Expiry uses std::time::Instant, so advancing only Tokio's clock would not expire this key.
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert_eq!(store.get(b"temporary"), None);
}
