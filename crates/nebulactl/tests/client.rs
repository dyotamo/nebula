use nebula_protocol::{read_request, write_response, Opcode, Response, Status};
use nebulactl::Client;
use tokio::net::TcpListener;

#[tokio::test]
async fn client_sends_cache_operations_over_the_protocol() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for _ in 0..4 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await.unwrap();
            let response = match request.opcode {
                Opcode::Ping => Response::ok(b"PONG".to_vec()),
                Opcode::Set => {
                    assert_eq!(request.key, b"key");
                    assert_eq!(request.value, b"value");
                    assert_eq!(request.ttl_secs, 60);
                    Response::empty_ok()
                }
                Opcode::Get => {
                    assert_eq!(request.key, b"key");
                    Response::ok(b"value".to_vec())
                }
                Opcode::Delete => {
                    assert_eq!(request.key, b"missing");
                    Response::not_found()
                }
            };
            write_response(&mut socket, &response).await.unwrap();
        }
    });

    let client = Client::new(address.to_string());
    client.ping().await.unwrap();
    client.set(b"key", b"value", 60).await.unwrap();
    assert_eq!(client.get(b"key").await.unwrap(), Some(b"value".to_vec()));
    assert!(!client.delete(b"missing").await.unwrap());
    server.await.unwrap();
}

#[tokio::test]
async fn get_returns_none_for_missing_keys() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await.unwrap();
        write_response(&mut socket, &Response::not_found())
            .await
            .unwrap();
    });

    assert_eq!(
        Client::new(address.to_string())
            .get("missing")
            .await
            .unwrap(),
        None
    );
    server.await.unwrap();
}

#[tokio::test]
async fn client_surfaces_server_errors() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await.unwrap();
        let response = Response {
            status: Status::InvalidArgument,
            value: b"invalid key".to_vec(),
        };
        write_response(&mut socket, &response).await.unwrap();
    });

    let error = Client::new(address.to_string())
        .set("key", "value", 0)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("invalid key"));
    server.await.unwrap();
}
