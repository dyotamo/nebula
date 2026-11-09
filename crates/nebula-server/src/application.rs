use std::{error::Error, fs::File, io, io::BufReader, sync::Arc, time::Duration};

use nebula_auth::AuthStore;
use nebula_core::CacheStore;
use nebula_health::{
    lifecycle::{ApplicationState, Lifecycle},
    ComponentHealth, HealthComponent, HealthManager,
};
use nebula_watchdog::Watchdog;
use tokio::{
    net::TcpListener,
    sync::oneshot,
    task::{JoinError, JoinSet},
    time::timeout,
};
use tokio_rustls::{rustls, TlsAcceptor};
use tracing::{error, info, warn};

use crate::{
    admin,
    config::{Config, PersistenceMode, TlsOptions},
    metrics::{self, Metrics},
    persistence::PersistenceManager,
    server, systemd, ttl_cleanup,
};

pub struct Application {
    config: Config,
    store: std::sync::Arc<CacheStore>,
    lifecycle: Lifecycle,
    health: HealthManager,
    watchdog: Watchdog,
    metrics: Arc<Metrics>,
    persistence: Option<PersistenceManager>,
    services: JoinSet<io::Result<()>>,
    shutdown_timeout: Duration,
}

impl Application {
    pub fn new(config: Config) -> Self {
        let lifecycle = Lifecycle::new();
        let health = HealthManager::new(lifecycle.clone(), config.admin.enabled);
        let watchdog = Watchdog::new(health.clone());
        let metrics = Metrics::new();
        let store = Arc::new(CacheStore::with_limits(
            config.resource_limits.max_cache_size_bytes,
            config.resource_limits.eviction_policy,
        ));
        let persistence = if config.persistence.mode == PersistenceMode::Snapshot {
            health.report(
                HealthComponent::Persistence("snapshots".to_owned()),
                ComponentHealth::Healthy,
            );
            Some(PersistenceManager::new(
                config.persistence.snapshot_dir.clone(),
                Duration::from_secs(config.persistence.snapshot_interval_secs),
                config.persistence.snapshot_retention,
                health.clone(),
                Arc::clone(&metrics),
            ))
        } else {
            None
        };
        Self {
            shutdown_timeout: Duration::from_secs(config.shutdown_timeout_secs),
            config,
            store,
            lifecycle,
            health,
            watchdog,
            metrics,
            persistence,
            services: JoinSet::new(),
        }
    }

    pub fn state(&self) -> ApplicationState {
        self.lifecycle.state()
    }

    pub fn health(&self) -> HealthManager {
        self.health.clone()
    }

    pub async fn run(mut self) -> Result<(), Box<dyn Error>> {
        let mut signals = ShutdownSignals::new()?;
        self.start().await?;

        let exited_service = tokio::select! {
            _ = signals.wait() => None,
            result = self.services.join_next() => Some(result),
        };

        if let Some(result) = exited_service {
            return self.handle_unexpected_exit(result).await;
        }

        self.shutdown().await
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn Error>> {
        if self.lifecycle.state() != ApplicationState::Initializing {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "application startup can only be run once",
            )
            .into());
        }

        if self.config.ttl_cleanup_interval_secs == 0 {
            return Err(self
                .fail_startup(
                    "configure TTL cleanup",
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "TTL cleanup interval must be greater than zero",
                    ),
                )
                .await);
        }

        if self.config.persistence.mode == PersistenceMode::Snapshot
            && (self.config.persistence.snapshot_interval_secs == 0
                || self.config.persistence.snapshot_retention == 0
                || self.config.persistence.snapshot_dir.as_os_str().is_empty())
        {
            return Err(self
                .fail_startup(
                    "initialize persistence",
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "snapshot directory must not be empty, and snapshot interval and retention must be greater than zero",
                    ),
                )
                .await);
        }

        if self.config.admin.enabled && !self.config.security.authentication {
            return Err(self
                .fail_startup(
                    "configure admin component",
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "security.authentication must be enabled when admin.enabled is true",
                    ),
                )
                .await);
        }

        if self.config.security.authentication && !self.config.tls.enabled {
            return Err(self
                .fail_startup(
                    "configure authentication",
                    io::Error::new(io::ErrorKind::InvalidInput, "authentication requires TLS"),
                )
                .await);
        }

        let auth_store = if self.config.security.authentication {
            let store = match AuthStore::open(&self.config.security.auth.database_path) {
                Ok(store) => Arc::new(store),
                Err(err) => {
                    return Err(self
                        .fail_startup("initialize authentication storage", io::Error::other(err))
                        .await);
                }
            };
            match store.ensure_bootstrap_credential() {
                Ok(Some(credential)) => {
                    eprintln!(
                        "NebulaCache one-time administrator bootstrap credential: {credential}"
                    );
                }
                Ok(None) => {}
                Err(err) => {
                    return Err(self
                        .fail_startup("initialize administrator bootstrap", io::Error::other(err))
                        .await);
                }
            }
            Some(store)
        } else {
            None
        };

        let tls_acceptor = match load_tls_acceptor(&self.config.tls) {
            Ok(acceptor) => acceptor,
            Err(err) => return Err(self.fail_startup("configure TCP TLS", err).await),
        };

        if let Some(persistence) = &self.persistence {
            if let Err(err) = persistence.restore(Arc::clone(&self.store)).await {
                return Err(self.fail_startup("restore cache snapshot", err).await);
            }
        }

        let listener = match TcpListener::bind(self.config.listen_addr).await {
            Ok(listener) => listener,
            Err(err) => return Err(self.fail_startup("bind TCP listener", err).await),
        };

        let admin_listener = if self.config.admin.enabled {
            match TcpListener::bind(self.config.admin.listen_addr).await {
                Ok(listener) => Some(listener),
                Err(err) => return Err(self.fail_startup("bind admin listener", err).await),
            }
        } else {
            None
        };
        let metrics_listener = if self.config.metrics.enabled {
            match TcpListener::bind(self.config.metrics.listen_addr).await {
                Ok(listener) => Some(listener),
                Err(err) => return Err(self.fail_startup("bind metrics listener", err).await),
            }
        } else {
            None
        };

        let frame_limits = self.config.frame_limits();
        let limits = server::ServerLimits {
            frame: frame_limits,
            max_connections: self.config.resource_limits.max_connections,
        };
        let admin_tls_acceptor = tls_acceptor.clone();
        let (tcp_started_tx, tcp_started_rx) = oneshot::channel();
        let tcp_store = self.store.clone();
        let tcp_auth_store = auth_store.clone();
        let tcp_lifecycle = self.lifecycle.subscribe();
        let tcp_heartbeat = self.watchdog.register_worker(
            "tcp-server",
            HealthComponent::TcpServer,
            Duration::from_secs(3),
        );
        let tcp_metrics = Arc::clone(&self.metrics);
        let tcp_health = self.health.clone();
        let tcp_lifecycle_state = self.lifecycle.clone();
        self.services.spawn(async move {
            let result = server::run(
                listener,
                tcp_store,
                limits,
                tls_acceptor,
                tcp_auth_store,
                tcp_lifecycle,
                tcp_heartbeat,
                tcp_metrics,
                tcp_started_tx,
            )
            .await;
            if matches!(
                tcp_lifecycle_state.state(),
                ApplicationState::Ready | ApplicationState::Degraded
            ) {
                tcp_health.report(
                    HealthComponent::TcpServer,
                    ComponentHealth::Unhealthy(
                        result
                            .as_ref()
                            .err()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "TCP service exited unexpectedly".to_owned()),
                    ),
                );
                let _ = tcp_lifecycle_state.transition(ApplicationState::Failed);
            }
            result
        });

        let admin_started = if let Some(listener) = admin_listener {
            let admin_lifecycle = self.lifecycle.subscribe();
            let admin_lifecycle_state = self.lifecycle.clone();
            let admin_health = self.health.clone();
            let admin_metrics = Arc::clone(&self.metrics);
            let dashboard_enabled = self.config.admin.metrics_dashboard;
            let admin_auth_store = auth_store.clone();
            let admin_heartbeat = self.watchdog.register_worker(
                "admin",
                HealthComponent::Admin,
                Duration::from_secs(3),
            );
            let store = self.store.clone();
            let (started_tx, started_rx) = oneshot::channel();
            self.services.spawn(async move {
                let result = admin::run(
                    listener,
                    store,
                    frame_limits,
                    admin_auth_store,
                    admin_tls_acceptor,
                    admin_lifecycle,
                    admin_heartbeat,
                    admin_metrics,
                    admin_health.clone(),
                    dashboard_enabled,
                    started_tx,
                )
                .await;
                if matches!(
                    admin_lifecycle_state.state(),
                    ApplicationState::Ready | ApplicationState::Degraded
                ) {
                    admin_health.report(
                        HealthComponent::Admin,
                        ComponentHealth::Unhealthy(
                            result
                                .as_ref()
                                .err()
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "admin console exited unexpectedly".to_owned()),
                        ),
                    );
                    let _ = admin_lifecycle_state.transition(ApplicationState::Failed);
                }
                result
            });
            Some(started_rx)
        } else {
            None
        };

        let metrics_started = if let Some(listener) = metrics_listener {
            let lifecycle = self.lifecycle.subscribe();
            let lifecycle_state = self.lifecycle.clone();
            let health = self.health.clone();
            let heartbeat = self.watchdog.register_worker(
                "metrics-endpoint",
                HealthComponent::MetricsEndpoint,
                Duration::from_secs(3),
            );
            let metrics = Arc::clone(&self.metrics);
            let store = Arc::clone(&self.store);
            let persistence_directory = self
                .persistence
                .as_ref()
                .map(|_| self.config.persistence.snapshot_dir.clone());
            let (started_tx, started_rx) = oneshot::channel();
            self.services.spawn(async move {
                let result = metrics::run_exporter(
                    listener,
                    metrics,
                    store,
                    lifecycle,
                    health.clone(),
                    persistence_directory,
                    heartbeat,
                    started_tx,
                )
                .await;
                if matches!(
                    lifecycle_state.state(),
                    ApplicationState::Ready | ApplicationState::Degraded
                ) {
                    health.report(
                        HealthComponent::MetricsEndpoint,
                        ComponentHealth::Unhealthy(
                            result
                                .as_ref()
                                .err()
                                .map(ToString::to_string)
                                .unwrap_or_else(|| {
                                    "metrics endpoint exited unexpectedly".to_owned()
                                }),
                        ),
                    );
                    let _ = lifecycle_state.transition(ApplicationState::Failed);
                }
                result
            });
            Some(started_rx)
        } else {
            None
        };

        if let Err(err) = self
            .wait_for_start(tcp_started_rx, admin_started, metrics_started)
            .await
        {
            return Err(self.fail_startup("start application services", err).await);
        }

        if let Some(result) = self.services.try_join_next() {
            return Err(self
                .fail_startup(
                    "start application services",
                    join_result_error(Some(result)),
                )
                .await);
        }

        if let Err(err) = self.lifecycle.transition(ApplicationState::Ready) {
            return Err(Box::new(err));
        }
        if let Some(persistence) = self.persistence.clone() {
            let persistence_store = Arc::clone(&self.store);
            let persistence_lifecycle = self.lifecycle.subscribe();
            self.services.spawn(async move {
                persistence
                    .run(persistence_store, persistence_lifecycle)
                    .await;
                Ok(())
            });
        }
        let cleanup_store = Arc::clone(&self.store);
        let cleanup_lifecycle = self.lifecycle.subscribe();
        let cleanup_interval = Duration::from_secs(self.config.ttl_cleanup_interval_secs);
        self.services.spawn(async move {
            ttl_cleanup::run(cleanup_store, cleanup_interval, cleanup_lifecycle).await;
            Ok(())
        });
        let watchdog = self.watchdog.clone();
        let watchdog_lifecycle = self.lifecycle.subscribe();
        self.services.spawn(async move {
            watchdog.run(watchdog_lifecycle).await;
            Ok(())
        });
        if let Some(watchdog_interval) = systemd::watchdog_interval() {
            let watchdog_lifecycle = self.lifecycle.subscribe();
            let watchdog_state = self.lifecycle.clone();
            let watchdog_health = self.health.clone();
            self.services.spawn(async move {
                systemd::run_watchdog(
                    watchdog_state,
                    watchdog_health,
                    watchdog_lifecycle,
                    watchdog_interval,
                )
                .await;
                Ok(())
            });
        }
        if let Err(err) = systemd::notify("READY=1") {
            return Err(self.fail_startup("notify systemd readiness", err).await);
        }
        info!(address = %self.config.listen_addr, "NebulaCache application ready");
        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<(), Box<dyn Error>> {
        match self.lifecycle.state() {
            ApplicationState::Stopped | ApplicationState::Stopping => return Ok(()),
            ApplicationState::Ready | ApplicationState::Degraded => {
                if let Err(err) = systemd::notify("STOPPING=1") {
                    warn!(error = %err, "systemd stopping notification failed");
                }
                self.lifecycle.transition(ApplicationState::Stopping)?
            }
            ApplicationState::Failed => {
                self.stop_services().await;
                return Ok(());
            }
            ApplicationState::Initializing => {
                self.lifecycle.transition(ApplicationState::Failed)?;
                self.stop_services().await;
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot shut down an application that has not started",
                )
                .into());
            }
        }

        if let Some(err) = self.stop_services().await {
            let _ = self.lifecycle.transition(ApplicationState::Failed);
            error!(error = %err, "application shutdown failed");
            return Err(err.into());
        }

        if let Some(persistence) = &self.persistence {
            if let Err(err) = persistence.snapshot(Arc::clone(&self.store)).await {
                let _ = self.lifecycle.transition(ApplicationState::Failed);
                error!(error = %err, "final cache snapshot failed during shutdown");
                return Err(Box::new(err));
            }
        }

        self.lifecycle.transition(ApplicationState::Stopped)?;
        info!("NebulaCache application stopped");
        Ok(())
    }

    async fn wait_for_start(
        &mut self,
        tcp_started: oneshot::Receiver<()>,
        admin_started: Option<oneshot::Receiver<()>>,
        metrics_started: Option<oneshot::Receiver<()>>,
    ) -> io::Result<()> {
        let mut started = vec![tcp_started];
        if let Some(admin_started) = admin_started {
            started.push(admin_started);
        }
        if let Some(metrics_started) = metrics_started {
            started.push(metrics_started);
        }

        for receiver in started {
            tokio::select! {
                result = receiver => {
                    result.map_err(|_| io::Error::other("service exited before startup completed"))?;
                }
                result = self.services.join_next() => {
                    return Err(join_result_error(result));
                }
            }
        }
        Ok(())
    }

    async fn fail_startup(&mut self, context: &str, err: io::Error) -> Box<dyn Error> {
        let component = if context.contains("persistence") || context.contains("snapshot") {
            HealthComponent::Persistence("snapshots".to_owned())
        } else if context.contains("admin") {
            HealthComponent::Admin
        } else {
            HealthComponent::TcpServer
        };
        self.health.report(
            component,
            ComponentHealth::Unhealthy(format!("{context}: {err}")),
        );
        let _ = self.lifecycle.transition(ApplicationState::Failed);
        let error = io::Error::new(err.kind(), format!("{context}: {err}"));
        error!(error = %error, "application startup failed");
        self.stop_services().await;
        Box::new(error)
    }

    async fn handle_unexpected_exit(
        &mut self,
        result: Option<Result<io::Result<()>, JoinError>>,
    ) -> Result<(), Box<dyn Error>> {
        let err = join_result_error(result);
        self.health.report(
            HealthComponent::BackgroundWorker("application-service-supervisor".to_owned()),
            ComponentHealth::Unhealthy(err.to_string()),
        );
        let _ = self.lifecycle.transition(ApplicationState::Failed);
        error!(error = %err, "mandatory application service exited unexpectedly");
        self.stop_services().await;
        Err(Box::new(err))
    }

    async fn stop_services(&mut self) -> Option<io::Error> {
        let drained = timeout(self.shutdown_timeout, async {
            while let Some(result) = self.services.join_next().await {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => return Err(err),
                    Err(err) => return Err(io::Error::other(err)),
                }
            }
            Ok(())
        })
        .await;

        match drained {
            Ok(Ok(())) => None,
            Ok(Err(err)) => {
                error!(error = %err, "service failed while shutting down");
                self.services.abort_all();
                while self.services.join_next().await.is_some() {}
                Some(err)
            }
            Err(_) => {
                warn!("graceful shutdown timed out; forcing service shutdown");
                self.services.abort_all();
                while self.services.join_next().await.is_some() {}
                None
            }
        }
    }
}

fn load_tls_acceptor(options: &TlsOptions) -> io::Result<Option<TlsAcceptor>> {
    if !options.enabled {
        return Ok(None);
    }

    let certificate_path = options.certificate.as_ref().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "tls.certificate is required")
    })?;
    let private_key_path = options.private_key.as_ref().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "tls.private_key is required")
    })?;
    let certificates = rustls_pemfile::certs(&mut BufReader::new(File::open(certificate_path)?))
        .collect::<Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "TLS certificate file contains no certificates",
        ));
    }
    let private_key =
        rustls_pemfile::private_key(&mut BufReader::new(File::open(private_key_path)?))?
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "TLS private key file is empty")
            })?;
    let server_config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?
    .with_no_client_auth()
    .with_single_cert(certificates, private_key)
    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    Ok(Some(TlsAcceptor::from(Arc::new(server_config))))
}

fn join_result_error(result: Option<Result<io::Result<()>, JoinError>>) -> io::Error {
    match result {
        Some(Ok(Ok(()))) => io::Error::other("application service stopped unexpectedly"),
        Some(Ok(Err(err))) => err,
        Some(Err(err)) => io::Error::other(err),
        None => io::Error::other("all application services stopped unexpectedly"),
    }
}

#[cfg(unix)]
struct ShutdownSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl ShutdownSignals {
    fn new() -> io::Result<Self> {
        use tokio::signal::unix::{signal, SignalKind};
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    async fn wait(&mut self) {
        tokio::select! {
            _ = self.interrupt.recv() => {}
            _ = self.terminate.recv() => {}
        }
    }
}

#[cfg(not(unix))]
struct ShutdownSignals;

#[cfg(not(unix))]
impl ShutdownSignals {
    fn new() -> io::Result<Self> {
        Ok(Self)
    }

    async fn wait(&mut self) {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use std::{io, net::SocketAddr, time::Duration};

    use nebula_health::{
        lifecycle::ApplicationState, ApplicationHealth, ComponentHealth, HealthComponent,
    };
    use tokio::net::TcpListener;

    use crate::config::{Config, PersistenceMode};

    use super::Application;

    fn test_config(address: SocketAddr) -> Config {
        Config {
            listen_addr: address,
            admin: Default::default(),
            tls: Default::default(),
            security: Default::default(),
            shutdown_timeout_secs: 1,
            ttl_cleanup_interval_secs: 3_600,
            persistence: Default::default(),
            metrics: {
                let mut metrics = crate::config::MetricsOptions::default();
                metrics.enabled = false;
                metrics
            },
            resource_limits: Default::default(),
            log_level: "info".to_owned(),
        }
    }

    #[tokio::test]
    async fn starts_ready_and_shutdown_is_idempotent() {
        let mut app = Application::new(test_config("127.0.0.1:0".parse().unwrap()));
        app.start().await.unwrap();
        assert_eq!(app.state(), ApplicationState::Ready);

        app.shutdown().await.unwrap();
        app.shutdown().await.unwrap();
        assert_eq!(app.state(), ApplicationState::Stopped);
    }

    #[tokio::test]
    async fn graceful_shutdown_persists_cache_for_the_next_startup() {
        let directory = std::env::temp_dir().join(format!(
            "nebula-application-snapshot-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut config = test_config("127.0.0.1:0".parse().unwrap());
        config.persistence.mode = PersistenceMode::Snapshot;
        config.persistence.snapshot_dir = directory.clone();
        config.persistence.snapshot_interval_secs = 1;
        let mut app = Application::new(config.clone());
        app.start().await.unwrap();
        app.store
            .set(b"restart-key".to_vec(), b"restart-value".to_vec(), 0);
        let active_snapshot = app
            .persistence
            .as_ref()
            .unwrap()
            .lock_snapshot_operation()
            .await;
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        let release_snapshot = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(active_snapshot);
        });
        app.shutdown().await.unwrap();
        release_snapshot.await.unwrap();

        let mut restarted = Application::new(config);
        restarted.start().await.unwrap();
        assert_eq!(
            restarted.store.get(b"restart-key"),
            Some(b"restart-value".to_vec())
        );
        restarted.shutdown().await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn invalid_snapshot_fails_startup_instead_of_using_an_empty_cache() {
        let directory = std::env::temp_dir().join(format!(
            "nebula-invalid-snapshot-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join("snapshot-corrupt.nbs"), b"corrupt").unwrap();
        let mut config = test_config("127.0.0.1:0".parse().unwrap());
        config.persistence.mode = PersistenceMode::Snapshot;
        config.persistence.snapshot_dir = directory.clone();
        let mut app = Application::new(config);

        assert!(app.start().await.is_err());
        assert_eq!(app.state(), ApplicationState::Failed);
        assert!(matches!(
            app.health()
                .snapshot()
                .components
                .get(&HealthComponent::Persistence("snapshots".to_owned())),
            Some(ComponentHealth::Unhealthy(_))
        ));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn health_degradation_blocks_readiness_until_recovery() {
        let mut app = Application::new(test_config("127.0.0.1:0".parse().unwrap()));
        app.start().await.unwrap();
        let health = app.health();

        health.report(
            HealthComponent::CacheEngine,
            ComponentHealth::Degraded("temporary cache issue".to_owned()),
        );
        assert_eq!(health.snapshot().overall, ApplicationHealth::Degraded);
        assert_eq!(app.state(), ApplicationState::Degraded);

        health.report(HealthComponent::CacheEngine, ComponentHealth::Healthy);
        assert_eq!(health.snapshot().overall, ApplicationHealth::Healthy);
        assert_eq!(app.state(), ApplicationState::Ready);
        app.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn startup_failure_releases_already_bound_listener() {
        let occupied_admin = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let admin_address = occupied_admin.local_addr().unwrap();
        let tcp_reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tcp_address = tcp_reservation.local_addr().unwrap();
        drop(tcp_reservation);

        let mut config = test_config(tcp_address);
        config.admin.enabled = true;
        config.admin.listen_addr = admin_address;
        config.security.authentication = true;
        config.security.auth.database_path = ":memory:".into();
        config.tls.enabled = true;
        config.tls.certificate =
            Some(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../server.pem"));
        config.tls.private_key =
            Some(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../server-key.pem"));
        let mut app = Application::new(config);

        assert!(app.start().await.is_err());
        assert_eq!(app.state(), ApplicationState::Failed);
        assert!(TcpListener::bind(tcp_address).await.is_ok());
        drop(occupied_admin);
    }

    #[tokio::test]
    async fn shutdown_timeout_forces_service_tasks_to_stop() {
        let mut app = Application::new(test_config("127.0.0.1:0".parse().unwrap()));
        app.lifecycle.transition(ApplicationState::Ready).unwrap();
        app.shutdown_timeout = Duration::ZERO;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        app.services.spawn(async move {
            let _ = started_tx.send(());
            std::future::pending::<io::Result<()>>().await
        });
        started_rx.await.unwrap();

        app.shutdown().await.unwrap();
        assert_eq!(app.state(), ApplicationState::Stopped);
        assert!(app.services.is_empty());
    }
}
