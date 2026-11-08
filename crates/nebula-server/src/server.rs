use std::{io, sync::Arc, time::Duration};

use nebula_protocol::{read_request_with_limits, write_response_with_limits, FrameLimits};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::{oneshot, watch, Semaphore},
    task::JoinSet,
    time::{interval, MissedTickBehavior},
};
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};

use nebula_core::CacheStore;
use nebula_health::lifecycle::ApplicationState;
use nebula_watchdog::WorkerHeartbeat;

use crate::{handler, metrics::Metrics};

pub(crate) struct ServerLimits {
    pub frame: FrameLimits,
    pub max_connections: usize,
}

pub(crate) async fn run(
    listener: TcpListener,
    store: Arc<CacheStore>,
    limits: ServerLimits,
    tls_acceptor: Option<TlsAcceptor>,
    mut lifecycle: watch::Receiver<ApplicationState>,
    heartbeat: WorkerHeartbeat,
    metrics: Arc<Metrics>,
    started: oneshot::Sender<()>,
) -> io::Result<()> {
    info!(address = %listener.local_addr()?, "NebulaCache node listening");
    let _ = started.send(());
    let ServerLimits {
        frame,
        max_connections,
    } = limits;
    let mut connections = JoinSet::new();
    let connection_slots = Arc::new(Semaphore::new(max_connections));
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
                let connection_permit = match Arc::clone(&connection_slots).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        warn!(%peer, max_connections, "connection limit reached; rejecting client");
                        drop(socket);
                        continue;
                    }
                };
                handler::log_connection(peer);
                metrics.connection_opened();
                let metrics = Arc::clone(&metrics);
                let store = Arc::clone(&store);
                let lifecycle = lifecycle.clone();
                let tls_acceptor = tls_acceptor.clone();
                connections.spawn(async move {
                    let _connection_permit = connection_permit;
                    let _active_connection = ActiveConnection(metrics.clone());
                    if let Some(acceptor) = tls_acceptor {
                        match acceptor.accept(socket).await {
                            Ok(stream) => serve_connection(stream, store, frame, lifecycle, metrics).await,
                            Err(err) => {
                                warn!(error = %err, "TLS handshake failed");
                                Ok(())
                            }
                        }
                    } else {
                        serve_connection(socket, store, frame, lifecycle, metrics).await
                    }
                });
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

struct ActiveConnection(Arc<Metrics>);

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.0.connection_closed();
    }
}

async fn serve_connection<S>(
    mut socket: S,
    store: Arc<CacheStore>,
    limits: FrameLimits,
    mut lifecycle: watch::Receiver<ApplicationState>,
    metrics: Arc<Metrics>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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

        let response =
            handler::handle_with_metrics(request, Arc::clone(&store), Arc::clone(&metrics)).await;
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

    use crate::metrics::Metrics;

    use super::{run, ServerLimits};

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
            ServerLimits {
                frame: Default::default(),
                max_connections: 1,
            },
            None,
            lifecycle.subscribe(),
            heartbeat,
            Metrics::new(),
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

        let mut rejected = tokio::net::TcpStream::connect(address).await.unwrap();
        let _ = write_request(
            &mut rejected,
            &Request {
                opcode: Opcode::Ping,
                key: Vec::new(),
                value: Vec::new(),
                ttl_secs: 0,
            },
        )
        .await;
        assert!(tokio::time::timeout(
            std::time::Duration::from_secs(1),
            read_response(&mut rejected)
        )
        .await
        .unwrap()
        .is_err());

        lifecycle.transition(ApplicationState::Stopping).unwrap();
        server.await.unwrap().unwrap();
    }
}
