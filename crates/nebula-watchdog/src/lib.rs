//! Cooperative worker monitoring based on elapsed monotonic time.
//!
//! Workers must call their heartbeat handles even when idle. Monitoring reports
//! health changes; it neither restarts workers nor interrupts overdue work.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use nebula_health::{lifecycle::ApplicationState, ComponentHealth, HealthComponent, HealthManager};
use tokio::{sync::watch, time::MissedTickBehavior};

// Detection is polling-based: a missed deadline is noticed on a later tick, not
// by a dedicated timer for each worker. Scheduling delays can add further latency.
const WATCHDOG_INTERVAL: Duration = Duration::from_millis(250);
// Recovery recognizes this exact reason so it does not clear unrelated component failures.
const WATCHDOG_FAILURE: &str = "worker heartbeat deadline exceeded";

/// Shared worker deadlines that translate missed heartbeats into component health failures.
///
/// Clones share registrations. Monitoring is cooperative: a heartbeat proves the caller
/// reached that point, not that all work associated with the component is progressing.
#[derive(Clone)]
pub struct Watchdog {
    health: HealthManager,
    workers: Arc<Mutex<BTreeMap<String, WorkerState>>>,
}

// The registry lock protects each deadline and its component association together;
// a reused ID replaces the entire registration rather than retaining an old deadline.
struct WorkerState {
    component: HealthComponent,
    last_heartbeat: Instant,
    timeout: Duration,
}

/// Clonable heartbeat handle; dropping it does not unregister the worker.
/// Handles identify a registration by name, so any clone refreshes the shared deadline.
#[derive(Clone)]
pub struct WorkerHeartbeat {
    watchdog: Watchdog,
    worker_id: String,
}

impl Watchdog {
    /// Creates an empty registry; deadline checks start only when `run` is polled.
    /// Heartbeat refresh and timeout recovery can still run without the monitoring loop.
    pub fn new(health: HealthManager) -> Self {
        Self {
            health,
            workers: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Starts a deadline at registration time and marks the component healthy.
    ///
    /// Reusing an ID replaces its deadline/component; existing handles also refer to that new entry.
    /// Registration reports healthy even if the component had another failure reason.
    /// Use distinct components when worker health must be tracked independently: reports
    /// are keyed by component, not aggregated across all workers sharing that component.
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

    /// Checks deadlines every 250 ms until shutdown, failure, or closure is observed on `lifecycle`.
    /// The loop does not remove registrations or change their last reported health on exit.
    pub async fn run(&self, mut lifecycle: watch::Receiver<ApplicationState>) {
        let mut interval = tokio::time::interval(WATCHDOG_INTERVAL);
        // After scheduler stalls, resume at the normal cadence instead of issuing
        // a burst of catch-up scans against nearly identical heartbeat state.
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

    // One time sample gives the scan a common cutoff. Reports use a snapshot of
    // registrations, so a heartbeat racing the scan may be reflected on the next tick.
    fn check_workers(&self, now: Instant) {
        // Release the registry lock before health reporting so heartbeats need not wait for it.
        let updates: Vec<_> = self
            .workers
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .map(|worker| {
                // A heartbeat can be newer than the scan time; saturation treats
                // that race as zero elapsed time rather than an overdue worker.
                let expired = now.saturating_duration_since(worker.last_heartbeat) > worker.timeout;
                (worker.component.clone(), expired)
            })
            .collect();

        for (component, expired) in updates {
            let current = self.health.snapshot().components.get(&component).cloned();
            // Timeouts replace current component health, but automatic recovery
            // clears only our exact timeout reason, not a later unrelated failure.
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

    // Refresh under the registry lock, then report recovery without holding it;
    // health reporting can publish snapshots and trigger lifecycle transitions.
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
    /// Refreshes the deadline and clears a watchdog timeout, but not an unrelated failure reason.
    /// Call periodically while idle too; completing a unit of work is not required.
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
