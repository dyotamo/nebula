use std::{io, sync::Arc, time::Duration};

use nebula_protocol::{read_request_with_limits, write_response_with_limits, FrameLimits};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::{oneshot, watch},
    task::JoinSet,
    time::{interval, MissedTickBehavior},
};
use tracing::{error, info, warn};

use nebula_core::CacheStore;
use nebula_health::lifecycle::ApplicationState;
use nebula_watchdog::WorkerHeartbeat;

use crate::handler;

pub(crate) async fn run(
    listener: TcpListener,
    store: Arc<CacheStore>,
    limits: FrameLimits,
    mut lifecycle: watch::Receiver<ApplicationState>,
    heartbeat: WorkerHeartbeat,
    started: oneshot::Sender<()>,
) -> io::Result<()> {
    info!(address = %listener.local_addr()?, "NebulaCache node listening");
    let _ = started.send(());
    let mut connections = JoinSet::new();
    let mut heartbeat_interval = interval(Duration::from_secs(1));
    heartbeat_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        let state = *lifecycle.borrow();
        match state {
            ApplicationState::Initializing => {
                if lifecycle.changed().await.is_err() {
                    break;
                }
                continue;
            }
            ApplicationState::Ready | ApplicationState::Degraded => {}
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed => {
                break;
            }
        }

        tokio::select! {
            changed = lifecycle.changed() => {
                if changed.is_err() {
                    break;
                }
            }
            accepted = listener.accept(), if state == ApplicationState::Ready => {
                let (socket, peer) = accepted?;
                if *lifecycle.borrow() != ApplicationState::Ready {
                    drop(socket);
                    continue;
                }
                handler::log_connection(peer);
                connections.spawn(serve_connection(
                    socket,
                    Arc::clone(&store),
                    limits,
                    lifecycle.clone(),
                ));
            }
            _ = heartbeat_interval.tick() => heartbeat.heartbeat(),
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
    use nebula_health::{
        lifecycle::{ApplicationState, Lifecycle},
        HealthComponent, HealthManager,
    };
    use nebula_protocol::{read_response, write_request, Opcode, Request, Status};
    use nebula_watchdog::Watchdog;
    use tokio::{net::TcpListener, sync::oneshot};

    use super::run;

    #[tokio::test]
    async fn tcp_requests_are_not_processed_until_application_is_ready() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let store = Arc::new(CacheStore::new());
        let lifecycle = Lifecycle::new();
        let health = HealthManager::new(lifecycle.clone(), false);
        let watchdog = Watchdog::new(health);
        let heartbeat = watchdog.register_worker(
            "tcp-server-test",
            HealthComponent::TcpServer,
            std::time::Duration::from_secs(3),
        );
        let (started_tx, started_rx) = oneshot::channel();
        let server = tokio::spawn(run(
            listener,
            Arc::clone(&store),
            Default::default(),
            lifecycle.subscribe(),
            heartbeat,
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
