use std::{io, sync::Arc};

use nebula_protocol::{read_request_with_limits, write_response_with_limits, FrameLimits};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::{oneshot, watch},
    task::JoinSet,
};
use tracing::{error, info, warn};

use nebula_core::CacheStore;

use crate::{handler, lifecycle::ApplicationState};

pub async fn run(
    listener: TcpListener,
    store: Arc<CacheStore>,
    limits: FrameLimits,
    mut lifecycle: watch::Receiver<ApplicationState>,
    started: oneshot::Sender<()>,
) -> io::Result<()> {
    info!(address = %listener.local_addr()?, "NebulaCache node listening");
    let _ = started.send(());
    let mut connections = JoinSet::new();

    loop {
        let state = *lifecycle.borrow();
        match state {
            ApplicationState::Initializing => {
                if lifecycle.changed().await.is_err() {
                    break;
                }
                continue;
            }
            ApplicationState::Ready => {}
            _ => break,
        }

        tokio::select! {
            changed = lifecycle.changed() => {
                if changed.is_err() || *lifecycle.borrow() != ApplicationState::Ready {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (socket, peer) = accepted?;
                if *lifecycle.borrow() != ApplicationState::Ready {
                    drop(socket);
                    break;
                }
                handler::log_connection(peer);
                connections.spawn(serve_connection(
                    socket,
                    Arc::clone(&store),
                    limits,
                    lifecycle.clone(),
                ));
            }
            completed = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(err)) = completed {
                    warn!(error = %err, "connection task failed");
                }
            }
        }
    }

    while let Some(result) = connections.join_next().await {
        if let Err(err) = result {
            warn!(error = %err, "connection task failed during shutdown");
        }
    }
    Ok(())
}

async fn serve_connection(
    mut socket: TcpStream,
    store: Arc<CacheStore>,
    limits: FrameLimits,
    mut lifecycle: watch::Receiver<ApplicationState>,
) -> io::Result<()> {
    loop {
        if *lifecycle.borrow() != ApplicationState::Ready {
            return Ok(());
        }

        let request = tokio::select! {
            result = read_request_with_limits(&mut socket, limits) => match result {
                Ok(request) => request,
                Err(nebula_protocol::ProtocolError::Io(err))
                    if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(err) => {
                    error!(error = %err, "invalid request frame");
                    let _ = socket.shutdown().await;
                    return Ok(());
                }
            },
            changed = lifecycle.changed() => {
                if changed.is_err() || *lifecycle.borrow() != ApplicationState::Ready {
                    return Ok(());
                }
                continue;
            }
        };

        if *lifecycle.borrow() != ApplicationState::Ready {
            return Ok(());
        }

        let response = handler::handle(request, Arc::clone(&store)).await;
        if let Err(err) = write_response_with_limits(&mut socket, &response, limits).await {
            return Err(io::Error::other(err));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nebula_core::CacheStore;
    use nebula_protocol::{read_response, write_request, Opcode, Request, Status};
    use tokio::{net::TcpListener, sync::oneshot};

    use crate::lifecycle::{ApplicationState, Lifecycle};

    use super::run;

    #[tokio::test]
    async fn tcp_requests_are_not_processed_until_application_is_ready() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let store = Arc::new(CacheStore::new());
        let lifecycle = Lifecycle::new();
        let (started_tx, started_rx) = oneshot::channel();
        let server = tokio::spawn(run(
            listener,
            Arc::clone(&store),
            Default::default(),
            lifecycle.subscribe(),
            started_tx,
        ));
        started_rx.await.unwrap();

        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        write_request(
            &mut client,
            &Request {
                opcode: Opcode::Set,
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                ttl_secs: 0,
            },
        )
        .await
        .unwrap();
        assert_eq!(store.get(b"key"), None);

        lifecycle.transition(ApplicationState::Ready).unwrap();
        let response = read_response(&mut client).await.unwrap();
        assert_eq!(response.status, Status::Ok);
        assert_eq!(store.get(b"key"), Some(b"value".to_vec()));

        lifecycle.transition(ApplicationState::Stopping).unwrap();
        server.await.unwrap().unwrap();
    }
}
