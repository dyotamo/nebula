use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::{sync::watch, time::MissedTickBehavior};

use crate::lifecycle::{ApplicationState, Lifecycle};

const WATCHDOG_INTERVAL: Duration = Duration::from_millis(250);
const WATCHDOG_FAILURE: &str = "worker heartbeat deadline exceeded";

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum HealthComponent {
    TcpServer,
    CacheEngine,
    AdminConsole,
    BackgroundWorker(String),
    Persistence(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ComponentHealth {
    Healthy,
    Degraded(String),
    Unhealthy(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationHealth {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HealthSnapshot {
    pub overall: ApplicationHealth,
    pub components: BTreeMap<HealthComponent, ComponentHealth>,
}

#[derive(Clone)]
pub struct HealthManager {
    inner: Arc<Mutex<HealthState>>,
    lifecycle: Lifecycle,
}

struct HealthState {
    components: BTreeMap<HealthComponent, ComponentHealth>,
    workers: BTreeMap<String, WorkerState>,
    updates: watch::Sender<HealthSnapshot>,
}

struct WorkerState {
    component: HealthComponent,
    last_heartbeat: Instant,
    timeout: Duration,
}

#[derive(Clone)]
pub(crate) struct WorkerHeartbeat {
    manager: HealthManager,
    worker_id: String,
}

impl HealthManager {
    pub(crate) fn new(lifecycle: Lifecycle, admin_enabled: bool) -> Self {
        let mut components = BTreeMap::from([
            (HealthComponent::TcpServer, ComponentHealth::Healthy),
            (HealthComponent::CacheEngine, ComponentHealth::Healthy),
        ]);
        if admin_enabled {
            components.insert(HealthComponent::AdminConsole, ComponentHealth::Healthy);
        }
        let initial = snapshot(&components);
        let (updates, _) = watch::channel(initial);
        Self {
            inner: Arc::new(Mutex::new(HealthState {
                components,
                workers: BTreeMap::new(),
                updates,
            })),
            lifecycle,
        }
    }

    pub fn snapshot(&self) -> HealthSnapshot {
        let state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let current = state.updates.borrow().clone();
        current
    }

    pub fn subscribe(&self) -> watch::Receiver<HealthSnapshot> {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .updates
            .subscribe()
    }

    pub fn report(&self, component: HealthComponent, health: ComponentHealth) {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if state.components.get(&component) == Some(&health) {
            return;
        }
        state.components.insert(component, health);
        let current = snapshot(&state.components);
        state.updates.send_replace(current.clone());
        drop(state);
        self.synchronize_lifecycle(current.overall);
    }

    pub(crate) fn register_worker(
        &self,
        worker_id: impl Into<String>,
        component: HealthComponent,
        timeout: Duration,
    ) -> WorkerHeartbeat {
        let worker_id = worker_id.into();
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        state
            .components
            .insert(component.clone(), ComponentHealth::Healthy);
        state.workers.insert(
            worker_id.clone(),
            WorkerState {
                component,
                last_heartbeat: Instant::now(),
                timeout,
            },
        );
        let current = snapshot(&state.components);
        state.updates.send_replace(current.clone());
        drop(state);
        self.synchronize_lifecycle(current.overall);
        WorkerHeartbeat {
            manager: self.clone(),
            worker_id,
        }
    }

    pub(crate) async fn run_watchdog(&self, mut lifecycle: watch::Receiver<ApplicationState>) {
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
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let updates: Vec<_> = state
            .workers
            .values()
            .map(|worker| {
                let expired = now.saturating_duration_since(worker.last_heartbeat) > worker.timeout;
                (worker.component.clone(), expired)
            })
            .collect();
        let mut changed = false;
        for (component, expired) in updates {
            let current = state
                .components
                .get(&component)
                .cloned()
                .unwrap_or(ComponentHealth::Healthy);
            let replacement = if expired {
                ComponentHealth::Unhealthy(WATCHDOG_FAILURE.to_owned())
            } else if current == ComponentHealth::Unhealthy(WATCHDOG_FAILURE.to_owned()) {
                ComponentHealth::Healthy
            } else {
                current
            };
            if state.components.get(&component) != Some(&replacement) {
                state.components.insert(component, replacement);
                changed = true;
            }
        }
        if changed {
            let current = snapshot(&state.components);
            state.updates.send_replace(current.clone());
            drop(state);
            self.synchronize_lifecycle(current.overall);
        }
    }

    fn heartbeat(&self, worker_id: &str) {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let Some(worker) = state.workers.get_mut(worker_id) else {
            return;
        };
        worker.last_heartbeat = Instant::now();
        let component = worker.component.clone();
        let recovered = state.components.get(&component)
            == Some(&ComponentHealth::Unhealthy(WATCHDOG_FAILURE.to_owned()));
        if recovered {
            state.components.insert(component, ComponentHealth::Healthy);
            let current = snapshot(&state.components);
            state.updates.send_replace(current.clone());
            drop(state);
            self.synchronize_lifecycle(current.overall);
        }
    }

    fn synchronize_lifecycle(&self, health: ApplicationHealth) {
        match (self.lifecycle.state(), health) {
            (
                ApplicationState::Ready,
                ApplicationHealth::Degraded | ApplicationHealth::Unhealthy,
            ) => {
                let _ = self.lifecycle.transition(ApplicationState::Degraded);
            }
            (ApplicationState::Degraded, ApplicationHealth::Healthy) => {
                let _ = self.lifecycle.transition(ApplicationState::Ready);
            }
            _ => {}
        }
    }
}

impl WorkerHeartbeat {
    pub(crate) fn heartbeat(&self) {
        self.manager.heartbeat(&self.worker_id);
    }
}

fn snapshot(components: &BTreeMap<HealthComponent, ComponentHealth>) -> HealthSnapshot {
    let overall = if components
        .values()
        .any(|health| matches!(health, ComponentHealth::Unhealthy(_)))
    {
        ApplicationHealth::Unhealthy
    } else if components
        .values()
        .any(|health| matches!(health, ComponentHealth::Degraded(_)))
    {
        ApplicationHealth::Degraded
    } else {
        ApplicationHealth::Healthy
    };
    HealthSnapshot {
        overall,
        components: components.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        ApplicationHealth, ComponentHealth, HealthComponent, HealthManager, WorkerHeartbeat,
    };
    use crate::lifecycle::{ApplicationState, Lifecycle};

    #[test]
    fn reports_component_and_aggregate_health() {
        let lifecycle = Lifecycle::new();
        let health = HealthManager::new(lifecycle.clone(), true);
        lifecycle.transition(ApplicationState::Ready).unwrap();

        health.report(
            HealthComponent::CacheEngine,
            ComponentHealth::Degraded("test degradation".to_owned()),
        );
        let snapshot = health.snapshot();
        assert_eq!(snapshot.overall, ApplicationHealth::Degraded);
        assert_eq!(lifecycle.state(), ApplicationState::Degraded);

        health.report(HealthComponent::CacheEngine, ComponentHealth::Healthy);
        assert_eq!(health.snapshot().overall, ApplicationHealth::Healthy);
        assert_eq!(lifecycle.state(), ApplicationState::Ready);
    }

    #[test]
    fn idle_heartbeat_remains_healthy_and_missed_heartbeat_recovers() {
        let lifecycle = Lifecycle::new();
        let health = HealthManager::new(lifecycle.clone(), false);
        let worker = health.register_worker(
            "idle-worker",
            HealthComponent::BackgroundWorker("idle-worker".to_owned()),
            Duration::from_secs(3),
        );
        lifecycle.transition(ApplicationState::Ready).unwrap();

        let idle_heartbeat_at = std::time::Instant::now();
        worker.heartbeat();
        health.check_workers(idle_heartbeat_at + Duration::from_secs(2));
        assert_eq!(health.snapshot().overall, ApplicationHealth::Healthy);

        let expired_at = idle_heartbeat_at + Duration::from_secs(4);
        health.check_workers(expired_at);
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
        let _worker: WorkerHeartbeat =
            health.register_worker("server", HealthComponent::TcpServer, Duration::from_secs(3));
        health.report(
            HealthComponent::TcpServer,
            ComponentHealth::Unhealthy("service exited".to_owned()),
        );
        assert_eq!(health.snapshot().overall, ApplicationHealth::Unhealthy);
    }
}
