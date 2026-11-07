use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::sync::watch;

use crate::lifecycle::{ApplicationState, Lifecycle};

pub mod lifecycle;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum HealthComponent {
    TcpServer,
    CacheEngine,
    AdminConsole,
    MetricsEndpoint,
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
    pub recent_failures: Vec<String>,
}

#[derive(Clone)]
pub struct HealthManager {
    inner: Arc<Mutex<HealthState>>,
    lifecycle: Lifecycle,
}

struct HealthState {
    components: BTreeMap<HealthComponent, ComponentHealth>,
    recent_failures: VecDeque<String>,
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
        let initial = snapshot(&components, &VecDeque::new());
        let (updates, _) = watch::channel(initial);
        Self {
            inner: Arc::new(Mutex::new(HealthState {
                components,
                recent_failures: VecDeque::new(),
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
        let previous = state.components.get(&component).cloned();
        if let ComponentHealth::Unhealthy(reason) = &health {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            state
                .recent_failures
                .push_back(format!("{timestamp}: {component:?} unhealthy: {reason}"));
            if state.recent_failures.len() > 20 {
                state.recent_failures.pop_front();
            }
        }
        state.components.insert(component.clone(), health.clone());
        let current = snapshot(&state.components, &state.recent_failures);
        state.updates.send_replace(current.clone());
        drop(state);
        tracing::info!(component = ?component, previous = ?previous.as_ref().map(health_level), current = health_level(&health), "component health changed");
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

fn snapshot(
    components: &BTreeMap<HealthComponent, ComponentHealth>,
    recent_failures: &VecDeque<String>,
) -> HealthSnapshot {
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
        recent_failures: recent_failures.iter().cloned().collect(),
    }
}

fn health_level(health: &ComponentHealth) -> &'static str {
    match health {
        ComponentHealth::Healthy => "healthy",
        ComponentHealth::Degraded(_) => "degraded",
        ComponentHealth::Unhealthy(_) => "unhealthy",
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
