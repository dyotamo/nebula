//! Accepts TCP connections and serves NebulaCache protocol frames.
//!
//! Connection admission is bounded by a semaphore, and each connection observes the shared
//! application lifecycle so requests stop being served as soon as readiness is withdrawn.

use std::{io, sync::Arc, time::Duration};

use nebula_protocol::{
    read_request_with_limits, write_response_with_limits, FrameLimits, Response,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::{oneshot, watch, Semaphore},
    task::JoinSet,
    time::{interval, MissedTickBehavior},
};
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};

use nebula_auth::AuthStore;
use nebula_core::CacheStore;
use nebula_health::lifecycle::ApplicationState;
use nebula_watchdog::WorkerHeartbeat;

use crate::{handler, metrics::Metrics};

/// Limits applied to incoming protocol frames and concurrently accepted connections.
pub(crate) struct ServerLimits {
    /// Maximum lengths accepted by the protocol frame decoder and encoder.
    pub frame: FrameLimits,
    /// Number of concurrent connections permitted by the listener.
    pub max_connections: usize,
}

/// Runs the listener until shutdown, then waits for all accepted connection tasks to finish.
///
/// Authentication is rejected unless TLS is enabled, preventing bearer tokens from crossing a
/// plaintext connection.
pub(crate) async fn run(
    listener: TcpListener,
    store: Arc<CacheStore>,
    limits: ServerLimits,
    tls_acceptor: Option<TlsAcceptor>,
    auth_store: Option<Arc<AuthStore>>,
    mut lifecycle: watch::Receiver<ApplicationState>,
    heartbeat: WorkerHeartbeat,
    metrics: Arc<Metrics>,
    started: oneshot::Sender<()>,
) -> io::Result<()> {
    if auth_store.is_some() && tls_acceptor.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "authentication requires TLS",
        ));
    }
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
            // Stop admitting new sockets while degraded; already accepted connections are
            // separately told to stop when their lifecycle receiver changes.
            accepted = listener.accept(), if state == ApplicationState::Ready => {
                let (socket, peer) = accepted?;
                if *lifecycle.borrow() != ApplicationState::Ready {
                    drop(socket);
                    continue;
                }
                // Recheck after accept because readiness may have changed while accept completed.
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
                let auth_store = auth_store.clone();
                connections.spawn(async move {
                    // These guards keep admission capacity and connection metrics balanced across
                    // every return path, including TLS and protocol errors.
                    let _connection_permit = connection_permit;
                    let _active_connection = ActiveConnection(metrics.clone());
                    if let Some(acceptor) = tls_acceptor {
                        match acceptor.accept(socket).await {
                            Ok(stream) => serve_connection(stream, store, frame, lifecycle, metrics, auth_store).await,
                            Err(err) => {
                                warn!(error = %err, "TLS handshake failed");
                                Ok(())
                            }
                        }
                    } else {
                        serve_connection(socket, store, frame, lifecycle, metrics, auth_store).await
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

struct AuthenticatedConnection(Arc<Metrics>);

impl Drop for AuthenticatedConnection {
    fn drop(&mut self) {
        self.0.authenticated_connection_closed();
    }
}

struct ActiveConnection(Arc<Metrics>);

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.0.connection_closed();
    }
}

/// Reads and responds to requests on one stream until it closes or the application stops.
///
/// With authentication enabled, the first frame must authenticate successfully; the resulting
/// identity is then checked against each operation's required role.
async fn serve_connection<S>(
    mut socket: S,
    store: Arc<CacheStore>,
    limits: FrameLimits,
    mut lifecycle: watch::Receiver<ApplicationState>,
    metrics: Arc<Metrics>,
    auth_store: Option<Arc<AuthStore>>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut identity = None;
    let mut _authenticated_connection = None;
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

        // Authentication is a one-time connection handshake; once identity is set, later AUTH
        // frames are denied and each operation is checked against that identity's role.
        let (response, close_after_response) = if let Some(auth_store) = auth_store.as_ref() {
            match identity.as_ref() {
                None => {
                    if request.opcode != nebula_protocol::Opcode::Auth {
                        let _ = socket.shutdown().await;
                        return Ok(());
                    }
                    let authentication_started = std::time::Instant::now();
                    let authentication_result = std::str::from_utf8(&request.value)
                        .map_err(|_| ())
                        .and_then(|token| {
                            auth_store.authenticate(token).map_err(|err| {
                                warn!(error = %err, "authentication storage lookup failed");
                            })
                        });
                    let authentication_elapsed = authentication_started.elapsed().as_nanos() as u64;
                    match authentication_result {
                        Ok(Some(authenticated)) => {
                            metrics.record_authentication(true, authentication_elapsed);
                            metrics.authenticated_connection_opened();
                            _authenticated_connection =
                                Some(AuthenticatedConnection(Arc::clone(&metrics)));
                            identity = Some(authenticated);
                            (Response::ok(b"AUTH_RESULT".to_vec()), false)
                        }
                        Ok(None) | Err(()) => {
                            metrics.record_authentication(false, authentication_elapsed);
                            warn!("connection authentication failed");
                            (Response::unauthorized(), true)
                        }
                    }
                }
                Some(identity) => {
                    let allowed = match request.opcode {
                        nebula_protocol::Opcode::Ping => true,
                        nebula_protocol::Opcode::Get => identity.role.can_read(),
                        nebula_protocol::Opcode::Set | nebula_protocol::Opcode::Delete => {
                            identity.role.can_write()
                        }
                        nebula_protocol::Opcode::Auth => false,
                    };
                    metrics.authorization_check(allowed);
                    if allowed {
                        (
                            handler::handle_with_metrics(
                                request,
                                Arc::clone(&store),
                                Arc::clone(&metrics),
                            )
                            .await,
                            false,
                        )
                    } else {
                        (Response::forbidden(), false)
                    }
                }
            }
        } else {
            (
                handler::handle_with_metrics(request, Arc::clone(&store), Arc::clone(&metrics))
                    .await,
                false,
            )
        };
        if let Err(err) = write_response_with_limits(&mut socket, &response, limits).await {
            return Err(io::Error::other(err));
        }
        if close_after_response {
            let _ = socket.shutdown().await;
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nebula_auth::{AuthStore, Role};
    use nebula_core::CacheStore;
    use nebula_health::{
        lifecycle::{ApplicationState, Lifecycle},
        HealthComponent, HealthManager,
    };
    use nebula_protocol::{read_response, write_request, Opcode, Request, Status};
    use nebula_watchdog::Watchdog;
    use tokio::{net::TcpListener, sync::oneshot};

    use crate::metrics::Metrics;

    use super::{run, serve_connection, ServerLimits};

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

    #[tokio::test]
    async fn authentication_is_required_and_readonly_roles_cannot_write() {
        let auth_store = Arc::new(AuthStore::open(":memory:").unwrap());
        auth_store.create_user("viewer", Role::ReadOnly).unwrap();
        let token = auth_store.create_token("viewer", None).unwrap();
        let lifecycle = Lifecycle::new();
        lifecycle.transition(ApplicationState::Ready).unwrap();
        let store = Arc::new(CacheStore::new());
        let metrics = Metrics::new();

        let (mut unauthenticated_client, unauthenticated_server) = tokio::io::duplex(4096);
        let unauthenticated_connection = tokio::spawn(serve_connection(
            unauthenticated_server,
            Arc::clone(&store),
            Default::default(),
            lifecycle.subscribe(),
            Arc::clone(&metrics),
            Some(Arc::clone(&auth_store)),
        ));
        write_request(&mut unauthenticated_client, &Request::ping())
            .await
            .unwrap();
        assert!(read_response(&mut unauthenticated_client).await.is_err());
        unauthenticated_connection.await.unwrap().unwrap();

        let (mut client, server) = tokio::io::duplex(4096);
        let connection = tokio::spawn(serve_connection(
            server,
            Arc::clone(&store),
            Default::default(),
            lifecycle.subscribe(),
            Arc::clone(&metrics),
            Some(auth_store),
        ));
        write_request(&mut client, &Request::auth(&token.token))
            .await
            .unwrap();
        let auth_response = read_response(&mut client).await.unwrap();
        assert_eq!(auth_response.status, Status::Ok);
        assert_eq!(auth_response.value, b"AUTH_RESULT");

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
        assert_eq!(
            read_response(&mut client).await.unwrap().status,
            Status::Forbidden
        );
        assert_eq!(store.get(b"key"), None);

        lifecycle.transition(ApplicationState::Stopping).unwrap();
        connection.await.unwrap().unwrap();
    }
}
