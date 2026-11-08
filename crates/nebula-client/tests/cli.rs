use std::process::Output;

use nebula_protocol::{read_request, write_response, Opcode, Response, Status};
use tokio::{net::TcpListener, process::Command};

async fn run_cli(address: &str, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_nebula"))
        .arg("--address")
        .arg(address)
        .args(args)
        .output()
        .await
        .unwrap()
}

#[tokio::test]
async fn cli_sends_cache_operations_over_the_protocol() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
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

    let ping = run_cli(&address, &["ping"]).await;
    assert!(ping.status.success());
    assert_eq!(ping.stdout, b"PONG\n");

    let set = run_cli(&address, &["set", "key", "value", "60"]).await;
    assert!(set.status.success());

    let get = run_cli(&address, &["get", "key"]).await;
    assert!(get.status.success());
    assert_eq!(get.stdout, b"value\n");

    let delete = run_cli(&address, &["delete", "missing"]).await;
    assert!(delete.status.success());
    assert_eq!(delete.stdout, b"not found\n");

    server.await.unwrap();
}

#[tokio::test]
async fn get_reports_missing_keys_as_a_cli_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await.unwrap();
        write_response(&mut socket, &Response::not_found())
            .await
            .unwrap();
    });

    let output = run_cli(&address, &["get", "missing"]).await;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("key not found"));
    server.await.unwrap();
}

#[tokio::test]
async fn cli_surfaces_server_errors() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await.unwrap();
        let response = Response {
            status: Status::InvalidArgument,
            value: b"invalid key".to_vec(),
        };
        write_response(&mut socket, &response).await.unwrap();
    });

    let output = run_cli(&address, &["set", "key", "value"]).await;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid key"));
    server.await.unwrap();
}
