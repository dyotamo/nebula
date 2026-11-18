//! Shared component health, bounded diagnostics, and readiness synchronization.
//!
//! Health describes reported subsystem conditions, not whether startup or shutdown
//! has completed. The supervisor owns those lifecycle decisions independently.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::sync::watch;

use crate::lifecycle::{ApplicationState, Lifecycle};

/// Validated application states and shared transition notifications.
pub mod lifecycle;

/// Identity of a service or named subsystem tracked independently for health reporting.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum HealthComponent {
    /// Cache protocol listener and its serving loop.
    TcpServer,
    /// In-memory cache subsystem, independently of any listener.
    CacheEngine,
    /// Optional management HTTP service.
    Admin,
    /// Metrics-serving subsystem, which need not share the cache listener.
    MetricsEndpoint,
    /// Independently named worker; equal names identify the same health slot.
    BackgroundWorker(String),
    /// Named persistence subsystem or operation tracked by the supervisor.
    Persistence(String),
}

/// Component status, with a diagnostic reason when service is impaired or unavailable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ComponentHealth {
    /// No current impairment is reported; prior failures remain in history.
    Healthy,
    /// Impaired service with a diagnostic reason; readiness is withdrawn.
    Degraded(String),
    /// Unavailable or failed component; this alone does not make lifecycle failure terminal.
    Unhealthy(String),
}

/// Worst reported component status, distinct from the process lifecycle state.
/// A healthy aggregate does not itself mean startup has completed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationHealth {
    Healthy,
    Degraded,
    Unhealthy,
}

/// Consistent, owned view of component health and bounded failure history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HealthSnapshot {
    /// Unhealthy takes precedence over degraded, which takes precedence over healthy.
    pub overall: ApplicationHealth,
    /// Latest status for each registered component.
    pub components: BTreeMap<HealthComponent, ComponentHealth>,
    /// Up to 20 timestamped unhealthy reports, oldest first; retained after recovery.
    pub recent_failures: Vec<String>,
}

/// Shared health registry that publishes snapshots and updates readiness on health changes.
///
/// Reports replace a single status per component, including its reason; they are not
/// independent failure leases. Callers sharing a component must coordinate ownership.
#[derive(Clone)]
pub struct HealthManager {
    inner: Arc<Mutex<HealthState>>,
    lifecycle: Lifecycle,
}

// The mutex makes registry, failure history, and published snapshot one update.
// Lifecycle synchronization happens separately after releasing this registry lock.
struct HealthState {
    components: BTreeMap<HealthComponent, ComponentHealth>,
    recent_failures: VecDeque<String>,
    updates: watch::Sender<HealthSnapshot>,
}

impl HealthManager {
    /// Seeds healthy TCP/cache components and optionally admin, without changing the lifecycle.
    /// Other components are registered by their first report, including a healthy report.
    pub fn new(lifecycle: Lifecycle, admin_enabled: bool) -> Self {
        let mut components = BTreeMap::from([
            (HealthComponent::TcpServer, ComponentHealth::Healthy),
            (HealthComponent::CacheEngine, ComponentHealth::Healthy),
        ]);
        if admin_enabled {
            components.insert(HealthComponent::Admin, ComponentHealth::Healthy);
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

    /// Returns an owned copy of the latest published snapshot.
    /// The copy stays stable after later reports and never borrows the registry mutex.
    pub fn snapshot(&self) -> HealthSnapshot {
        let state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let current = state.updates.borrow().clone();
        current
    }

    /// Watches the latest snapshot; this is not a lossless stream of individual reports.
    /// Borrow the initial value explicitly before waiting for later changes.
    pub fn subscribe(&self) -> watch::Receiver<HealthSnapshot> {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .updates
            .subscribe()
    }

    /// Inserts or replaces a component status, publishes a snapshot, and synchronizes readiness.
    ///
    /// Identical reports are ignored; changed unhealthy reports append to the failure history.
    /// A changed reason is a changed report. Recovery replaces current status without
    /// erasing history, and health never forces a terminal lifecycle back to readiness.
    pub fn report(&self, component: HealthComponent, health: ComponentHealth) {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        // Heartbeats may report repeatedly; suppress duplicate notifications and
        // repeated history entries so polling frequency does not dominate diagnostics.
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
            // Bound diagnostic memory even when failure reasons keep changing.
            if state.recent_failures.len() > 20 {
                state.recent_failures.pop_front();
            }
        }
        state.components.insert(component.clone(), health.clone());
        let current = snapshot(&state.components, &state.recent_failures);
        // Retain the latest snapshot even when there are no watch receivers.
        state.updates.send_replace(current.clone());
        // Logging and lifecycle transitions do not need exclusive registry access.
        drop(state);
        tracing::info!(component = ?component, previous = ?previous.as_ref().map(health_level), current = health_level(&health), "component health changed");
        self.synchronize_lifecycle(current.overall);
    }

    // These are best-effort transitions: the supervisor may have advanced the
    // lifecycle since the observation, in which case validation rejects the stale edge.
    fn synchronize_lifecycle(&self, health: ApplicationHealth) {
        // Health controls readiness only; startup, shutdown, and fatal failure stay supervisor-owned.
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

// Compute severity from current reports, not failure history, so a recovered
// component can restore a healthy aggregate while its diagnostic record remains.
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

// Log stable severity labels separately from potentially variable diagnostic reasons.
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
