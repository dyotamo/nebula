use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use tokio::sync::watch;

use crate::lifecycle::{ApplicationState, Lifecycle};

pub mod lifecycle;

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
    updates: watch::Sender<HealthSnapshot>,
}

impl HealthManager {
    pub fn new(lifecycle: Lifecycle, admin_enabled: bool) -> Self {
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
    use super::{ApplicationHealth, ComponentHealth, HealthComponent, HealthManager};
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
}
