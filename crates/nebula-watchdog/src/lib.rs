use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use nebula_health::{lifecycle::ApplicationState, ComponentHealth, HealthComponent, HealthManager};
use tokio::{sync::watch, time::MissedTickBehavior};

const WATCHDOG_INTERVAL: Duration = Duration::from_millis(250);
const WATCHDOG_FAILURE: &str = "worker heartbeat deadline exceeded";

#[derive(Clone)]
pub struct Watchdog {
    health: HealthManager,
    workers: Arc<Mutex<BTreeMap<String, WorkerState>>>,
}

struct WorkerState {
    component: HealthComponent,
    last_heartbeat: Instant,
    timeout: Duration,
}

#[derive(Clone)]
pub struct WorkerHeartbeat {
    watchdog: Watchdog,
    worker_id: String,
}

impl Watchdog {
    pub fn new(health: HealthManager) -> Self {
        Self {
            health,
            workers: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn register_worker(
        &self,
        worker_id: impl Into<String>,
        component: HealthComponent,
        timeout: Duration,
    ) -> WorkerHeartbeat {
        let worker_id = worker_id.into();
        self.health
            .report(component.clone(), ComponentHealth::Healthy);
        self.workers
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(
                worker_id.clone(),
                WorkerState {
                    component,
                    last_heartbeat: Instant::now(),
                    timeout,
                },
            );
        WorkerHeartbeat {
            watchdog: self.clone(),
            worker_id,
        }
    }

    pub async fn run(&self, mut lifecycle: watch::Receiver<ApplicationState>) {
        let mut interval = tokio::time::interval(WATCHDOG_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = interval.tick() => self.check_workers(Instant::now()),
                changed = lifecycle.changed() => {
                    if changed.is_err() || matches!(
                        *lifecycle.borrow(),
                        ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed
                    ) {
                        return;
                    }
                }
            }
        }
    }

    fn check_workers(&self, now: Instant) {
        let updates: Vec<_> = self
            .workers
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .map(|worker| {
                let expired = now.saturating_duration_since(worker.last_heartbeat) > worker.timeout;
                (worker.component.clone(), expired)
            })
            .collect();

        for (component, expired) in updates {
            let current = self.health.snapshot().components.get(&component).cloned();
            let replacement = if expired {
                Some(ComponentHealth::Unhealthy(WATCHDOG_FAILURE.to_owned()))
            } else if current == Some(ComponentHealth::Unhealthy(WATCHDOG_FAILURE.to_owned())) {
                Some(ComponentHealth::Healthy)
            } else {
                None
            };
            if let Some(replacement) = replacement {
                self.health.report(component, replacement);
            }
        }
    }

    fn heartbeat(&self, worker_id: &str) {
        let component = {
            let mut workers = self
                .workers
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let Some(worker) = workers.get_mut(worker_id) else {
                return;
            };
            worker.last_heartbeat = Instant::now();
            worker.component.clone()
        };

        if self.health.snapshot().components.get(&component)
            == Some(&ComponentHealth::Unhealthy(WATCHDOG_FAILURE.to_owned()))
        {
            self.health.report(component, ComponentHealth::Healthy);
        }
    }
}

impl WorkerHeartbeat {
    pub fn heartbeat(&self) {
        self.watchdog.heartbeat(&self.worker_id);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use nebula_health::{
        lifecycle::{ApplicationState, Lifecycle},
        ApplicationHealth, ComponentHealth, HealthComponent, HealthManager,
    };

    use super::{Watchdog, WorkerHeartbeat};

    #[test]
    fn idle_heartbeat_remains_healthy_and_missed_heartbeat_recovers() {
        let lifecycle = Lifecycle::new();
        let health = HealthManager::new(lifecycle.clone(), false);
        let watchdog = Watchdog::new(health.clone());
        let worker = watchdog.register_worker(
            "idle-worker",
            HealthComponent::BackgroundWorker("idle-worker".to_owned()),
            Duration::from_secs(3),
        );
        lifecycle.transition(ApplicationState::Ready).unwrap();

        let idle_heartbeat_at = std::time::Instant::now();
        worker.heartbeat();
        watchdog.check_workers(idle_heartbeat_at + Duration::from_secs(2));
        assert_eq!(health.snapshot().overall, ApplicationHealth::Healthy);

        let expired_at = idle_heartbeat_at + Duration::from_secs(4);
        watchdog.check_workers(expired_at);
        assert_eq!(health.snapshot().overall, ApplicationHealth::Unhealthy);
        assert_eq!(lifecycle.state(), ApplicationState::Degraded);

        worker.heartbeat();
        assert_eq!(health.snapshot().overall, ApplicationHealth::Healthy);
        assert_eq!(lifecycle.state(), ApplicationState::Ready);
    }

    #[test]
    fn fatal_component_health_does_not_self_recover_on_worker_heartbeat() {
        let lifecycle = Lifecycle::new();
        let health = HealthManager::new(lifecycle, false);
        let watchdog = Watchdog::new(health.clone());
        let _worker: WorkerHeartbeat =
            watchdog.register_worker("server", HealthComponent::TcpServer, Duration::from_secs(3));
        health.report(
            HealthComponent::TcpServer,
            ComponentHealth::Unhealthy("service exited".to_owned()),
        );
        assert_eq!(health.snapshot().overall, ApplicationHealth::Unhealthy);
    }
}
