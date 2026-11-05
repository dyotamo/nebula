use std::{sync::Arc, time::Duration};

use nebula_node::{engine::CacheStore, handler};
use nebula_protocol::{Opcode, Request, Status};

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

#[tokio::test]
async fn ttl_expires_entry() {
    let store = Arc::new(CacheStore::new());
    store.set(b"temporary".to_vec(), b"value".to_vec(), 1);
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert_eq!(store.get(b"temporary"), None);
}
