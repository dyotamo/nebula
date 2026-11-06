use std::{error::Error, io, sync::Arc, time::Duration};

use nebula_core::CacheStore;
use tokio::{
    net::TcpListener,
    sync::oneshot,
    task::{JoinError, JoinSet},
    time::timeout,
};
use tracing::{error, info, warn};

use crate::{
    admin_console,
    config::{Config, PersistenceMode},
    health::{ComponentHealth, HealthComponent, HealthManager},
    lifecycle::{ApplicationState, Lifecycle},
    persistence::PersistenceManager,
    server,
};

pub struct Application {
    config: Config,
    store: std::sync::Arc<CacheStore>,
    lifecycle: Lifecycle,
    health: HealthManager,
    persistence: Option<PersistenceManager>,
    services: JoinSet<io::Result<()>>,
    shutdown_timeout: Duration,
}

impl Application {
    pub fn new(config: Config) -> Self {
        let lifecycle = Lifecycle::new();
        let health = HealthManager::new(lifecycle.clone(), config.admin_console.enabled);
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
            ))
        } else {
            None
        };
        Self {
            shutdown_timeout: Duration::from_secs(config.shutdown_timeout_secs),
            config,
            store: std::sync::Arc::new(CacheStore::new()),
            lifecycle,
            health,
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

        if let Some(persistence) = &self.persistence {
            if let Err(err) = persistence.restore(Arc::clone(&self.store)).await {
                return Err(self.fail_startup("restore cache snapshot", err).await);
            }
        }

        let listener = match TcpListener::bind(self.config.listen_addr).await {
            Ok(listener) => listener,
            Err(err) => return Err(self.fail_startup("bind TCP listener", err).await),
        };

        let admin_listener = if self.config.admin_console.enabled {
            match TcpListener::bind(self.config.admin_console.listen_addr).await {
                Ok(listener) => Some(listener),
                Err(err) => return Err(self.fail_startup("bind admin listener", err).await),
            }
        } else {
            None
        };

        let limits = self.config.frame_limits();
        let (tcp_started_tx, tcp_started_rx) = oneshot::channel();
        let tcp_store = self.store.clone();
        let tcp_lifecycle = self.lifecycle.subscribe();
        let tcp_heartbeat = self.health.register_worker(
            "tcp-server",
            HealthComponent::TcpServer,
            Duration::from_secs(3),
        );
        let tcp_health = self.health.clone();
        let tcp_lifecycle_state = self.lifecycle.clone();
        self.services.spawn(async move {
            let result = server::run(
                listener,
                tcp_store,
                limits,
                tcp_lifecycle,
                tcp_heartbeat,
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
            let Some(password) = self.config.admin_console.password.clone() else {
                return Err(self
                    .fail_startup(
                        "start admin console",
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "admin console password is required when the console is enabled",
                        ),
                    )
                    .await);
            };
            let admin_lifecycle = self.lifecycle.subscribe();
            let admin_lifecycle_state = self.lifecycle.clone();
            let admin_health = self.health.clone();
            let admin_heartbeat = self.health.register_worker(
                "admin-console",
                HealthComponent::AdminConsole,
                Duration::from_secs(3),
            );
            let store = self.store.clone();
            let (started_tx, started_rx) = oneshot::channel();
            self.services.spawn(async move {
                let result = admin_console::run(
                    listener,
                    store,
                    limits,
                    password,
                    admin_lifecycle,
                    admin_heartbeat,
                    started_tx,
                )
                .await;
                if matches!(
                    admin_lifecycle_state.state(),
                    ApplicationState::Ready | ApplicationState::Degraded
                ) {
                    admin_health.report(
                        HealthComponent::AdminConsole,
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

        if let Err(err) = self.wait_for_start(tcp_started_rx, admin_started).await {
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
        let watchdog_health = self.health.clone();
        let watchdog_lifecycle = self.lifecycle.subscribe();
        self.services.spawn(async move {
            watchdog_health.run_watchdog(watchdog_lifecycle).await;
            Ok(())
        });
        info!(address = %self.config.listen_addr, "NebulaCache application ready");
        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<(), Box<dyn Error>> {
        match self.lifecycle.state() {
            ApplicationState::Stopped | ApplicationState::Stopping => return Ok(()),
            ApplicationState::Ready | ApplicationState::Degraded => {
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
    ) -> io::Result<()> {
        let mut started = vec![tcp_started];
        if let Some(admin_started) = admin_started {
            started.push(admin_started);
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
            HealthComponent::AdminConsole
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

    use tokio::net::TcpListener;

    use crate::{
        config::{Config, PersistenceMode},
        health::{ApplicationHealth, ComponentHealth, HealthComponent},
        lifecycle::ApplicationState,
    };

    use super::Application;

    fn test_config(address: SocketAddr) -> Config {
        Config {
            listen_addr: address,
            admin_console: Default::default(),
            shutdown_timeout_secs: 1,
            persistence: Default::default(),
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
        config.admin_console.enabled = true;
        config.admin_console.password = Some("secret".to_owned());
        config.admin_console.listen_addr = admin_address;
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
