use std::{
    collections::VecDeque,
    fmt::Write as _,
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use axum::{extract::State, http::header, response::IntoResponse, routing::get, Router};
use nebula_core::CacheStore;
use nebula_health::{
    lifecycle::ApplicationState, ApplicationHealth, ComponentHealth, HealthComponent, HealthManager,
};
use nebula_watchdog::WorkerHeartbeat;
use serde::Serialize;
use tokio::{
    net::TcpListener,
    sync::{oneshot, watch},
    time::{interval, MissedTickBehavior},
};

const OPERATIONS: [&str; 4] = ["ping", "set", "get", "delete"];
const LATENCY_BUCKETS: [f64; 8] = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1.0];
const HISTORY_LIMIT: usize = 360;
const HISTORY_INTERVAL_SECS: u64 = 5;

#[derive(Default)]
struct OperationMetrics {
    requests: AtomicU64,
    errors: AtomicU64,
    latency_sum_ns: AtomicU64,
    latency_buckets: [AtomicU64; 8],
}

#[derive(Default)]
struct PersistenceMetrics {
    snapshots: AtomicU64,
    snapshot_failures: AtomicU64,
    snapshot_duration_ns: AtomicU64,
    snapshot_duration_buckets: [AtomicU64; 8],
    last_snapshot_success_timestamp_secs: AtomicU64,
    recoveries: AtomicU64,
    recovery_failures: AtomicU64,
    recovery_duration_ns: AtomicU64,
    recovery_duration_buckets: [AtomicU64; 8],
}

#[derive(Clone, Serialize)]
pub struct HistoryPoint {
    pub elapsed_secs: u64,
    pub requests_total: u64,
    pub errors_total: u64,
    pub active_connections: u64,
    pub cache_entries: usize,
    pub latency_ms: f64,
}

#[derive(Clone, Serialize)]
pub struct ComponentView {
    pub name: String,
    pub status: String,
}

#[derive(Clone, Serialize)]
pub struct DashboardSnapshot {
    pub available: bool,
    pub uptime_secs: u64,
    pub state: String,
    pub health: String,
    pub requests_total: u64,
    pub errors_total: u64,
    pub requests_per_second: f64,
    pub errors_per_second: f64,
    pub avg_latency_ms: f64,
    pub active_connections: u64,
    pub cache_entries: usize,
    pub cache_memory_bytes: usize,
    pub snapshot_count: u64,
    pub snapshot_failures: u64,
    pub snapshot_duration_ms: f64,
    pub last_snapshot_success_timestamp_secs: Option<u64>,
    pub recovery_count: u64,
    pub recovery_failures: u64,
    pub recovery_duration_ms: f64,
    pub components: Vec<ComponentView>,
    pub recent_failures: Vec<String>,
    pub history: Vec<HistoryPoint>,
}

pub struct Metrics {
    started: Instant,
    operations: [OperationMetrics; 4],
    active_connections: AtomicU64,
    auth_success: AtomicU64,
    auth_failure: AtomicU64,
    auth_duration_ns: AtomicU64,
    auth_duration_count: AtomicU64,
    auth_duration_buckets: [AtomicU64; 8],
    authz_checks: AtomicU64,
    authz_denied: AtomicU64,
    auth_active_connections: AtomicU64,
    auth_revocations: AtomicU64,
    persistence: PersistenceMetrics,
    history: Mutex<VecDeque<HistoryPoint>>,
    last_history_sample: Mutex<Option<Instant>>,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            started: Instant::now(),
            operations: std::array::from_fn(|_| OperationMetrics::default()),
            active_connections: AtomicU64::new(0),
            auth_success: AtomicU64::new(0),
            auth_failure: AtomicU64::new(0),
            auth_duration_ns: AtomicU64::new(0),
            auth_duration_count: AtomicU64::new(0),
            auth_duration_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            authz_checks: AtomicU64::new(0),
            authz_denied: AtomicU64::new(0),
            auth_active_connections: AtomicU64::new(0),
            auth_revocations: AtomicU64::new(0),
            persistence: PersistenceMetrics::default(),
            history: Mutex::new(VecDeque::new()),
            last_history_sample: Mutex::new(None),
        })
    }

    pub fn record_request(&self, operation: &str, success: bool, elapsed_ns: u64) {
        let Some(index) = OPERATIONS
            .iter()
            .position(|candidate| *candidate == operation)
        else {
            return;
        };
        let metrics = &self.operations[index];
        metrics.requests.fetch_add(1, Ordering::Relaxed);
        if !success {
            metrics.errors.fetch_add(1, Ordering::Relaxed);
        }
        metrics
            .latency_sum_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        let elapsed_secs = elapsed_ns as f64 / 1_000_000_000.0;
        for (bucket, bound) in metrics.latency_buckets.iter().zip(LATENCY_BUCKETS) {
            if elapsed_secs <= bound {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn connection_opened(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connection_closed(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn record_authentication(&self, success: bool, elapsed_ns: u64) {
        if success {
            self.auth_success.fetch_add(1, Ordering::Relaxed);
        } else {
            self.auth_failure.fetch_add(1, Ordering::Relaxed);
        }
        self.auth_duration_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        self.auth_duration_count.fetch_add(1, Ordering::Relaxed);
        record_buckets(&self.auth_duration_buckets, elapsed_ns);
    }

    pub fn authorization_check(&self, allowed: bool) {
        self.authz_checks.fetch_add(1, Ordering::Relaxed);
        if !allowed {
            self.authz_denied.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn authenticated_connection_opened(&self) {
        self.auth_active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn authenticated_connection_closed(&self) {
        self.auth_active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn record_revocation(&self) {
        self.auth_revocations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_snapshot(&self, elapsed_ns: u64, success: bool) {
        self.persistence.snapshots.fetch_add(1, Ordering::Relaxed);
        self.persistence
            .snapshot_duration_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        record_buckets(&self.persistence.snapshot_duration_buckets, elapsed_ns);
        if success {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            self.persistence
                .last_snapshot_success_timestamp_secs
                .store(timestamp, Ordering::Relaxed);
        } else {
            self.persistence
                .snapshot_failures
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_recovery(&self, elapsed_ns: u64, success: bool) {
        self.persistence.recoveries.fetch_add(1, Ordering::Relaxed);
        self.persistence
            .recovery_duration_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        record_buckets(&self.persistence.recovery_duration_buckets, elapsed_ns);
        if !success {
            self.persistence
                .recovery_failures
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn dashboard_snapshot(
        &self,
        store: &CacheStore,
        lifecycle: ApplicationState,
        health: &HealthManager,
    ) -> DashboardSnapshot {
        let (requests_total, errors_total, latency_sum_ns, request_count) = self.request_totals();
        let uptime_secs = self.started.elapsed().as_secs();
        let active_connections = self.active_connections.load(Ordering::Relaxed);
        let cache_entries = store.len();
        let avg_latency_ms = if request_count == 0 {
            0.0
        } else {
            latency_sum_ns as f64 / request_count as f64 / 1_000_000.0
        };
        let now = Instant::now();
        let mut history_guard = self
            .history
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut last = self
            .last_history_sample
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if last
            .is_none_or(|previous| now.duration_since(previous).as_secs() >= HISTORY_INTERVAL_SECS)
        {
            history_guard.push_back(HistoryPoint {
                elapsed_secs: uptime_secs,
                requests_total,
                errors_total,
                active_connections,
                cache_entries,
                latency_ms: avg_latency_ms,
            });
            if history_guard.len() > HISTORY_LIMIT {
                history_guard.pop_front();
            }
            *last = Some(now);
        }
        let history = history_guard.iter().cloned().collect::<Vec<_>>();
        drop(last);
        drop(history_guard);

        let health_snapshot = health.snapshot();
        DashboardSnapshot {
            available: true,
            uptime_secs,
            state: lifecycle_name(lifecycle).to_owned(),
            health: match health_snapshot.overall {
                ApplicationHealth::Healthy => "healthy",
                ApplicationHealth::Degraded => "degraded",
                ApplicationHealth::Unhealthy => "unhealthy",
            }
            .to_owned(),
            requests_total,
            errors_total,
            requests_per_second: rate(&history, |point| point.requests_total),
            errors_per_second: rate(&history, |point| point.errors_total),
            avg_latency_ms,
            active_connections,
            cache_entries,
            cache_memory_bytes: store.memory_usage(),
            snapshot_count: self.persistence.snapshots.load(Ordering::Relaxed),
            snapshot_failures: self.persistence.snapshot_failures.load(Ordering::Relaxed),
            snapshot_duration_ms: average_millis(
                self.persistence
                    .snapshot_duration_ns
                    .load(Ordering::Relaxed),
                self.persistence.snapshots.load(Ordering::Relaxed),
            ),
            last_snapshot_success_timestamp_secs: match self
                .persistence
                .last_snapshot_success_timestamp_secs
                .load(Ordering::Relaxed)
            {
                0 => None,
                timestamp => Some(timestamp),
            },
            recovery_count: self.persistence.recoveries.load(Ordering::Relaxed),
            recovery_failures: self.persistence.recovery_failures.load(Ordering::Relaxed),
            recovery_duration_ms: average_millis(
                self.persistence
                    .recovery_duration_ns
                    .load(Ordering::Relaxed),
                self.persistence.recoveries.load(Ordering::Relaxed),
            ),
            components: health_snapshot
                .components
                .iter()
                .map(|(component, status)| ComponentView {
                    name: component_name(component).to_owned(),
                    status: health_name(status).to_owned(),
                })
                .collect(),
            recent_failures: health_snapshot.recent_failures,
            history,
        }
    }

    pub fn render_prometheus(
        &self,
        store: &CacheStore,
        lifecycle: ApplicationState,
        health: &HealthManager,
        persistence_directory: Option<&Path>,
    ) -> String {
        let mut output = String::with_capacity(4096);
        let uptime = self.started.elapsed().as_secs_f64();
        metric(
            &mut output,
            "nebula_uptime_seconds",
            "Application uptime in seconds.",
            "gauge",
            &uptime.to_string(),
        );
        metric(
            &mut output,
            "nebula_lifecycle_state",
            "Whether the application is in each lifecycle state.",
            "gauge",
            "",
        );
        for state in [
            "initializing",
            "ready",
            "degraded",
            "stopping",
            "stopped",
            "failed",
        ] {
            let value = if lifecycle_name(lifecycle) == state {
                1.0
            } else {
                0.0
            };
            let _ = writeln!(
                output,
                "nebula_lifecycle_state{{state=\"{state}\"}} {value}"
            );
        }

        let _ = writeln!(output, "# HELP nebula_requests_total Total protocol requests by operation.\n# TYPE nebula_requests_total counter\n# HELP nebula_request_errors_total Protocol requests returning a non-success status.\n# TYPE nebula_request_errors_total counter\n# HELP nebula_request_duration_seconds Time spent processing protocol requests.\n# TYPE nebula_request_duration_seconds histogram");
        for (index, operation) in OPERATIONS.iter().enumerate() {
            let metrics = &self.operations[index];
            let requests = metrics.requests.load(Ordering::Relaxed);
            let errors = metrics.errors.load(Ordering::Relaxed);
            let sum = metrics.latency_sum_ns.load(Ordering::Relaxed) as f64 / 1_000_000_000.0;
            let _ = writeln!(
                output,
                "nebula_requests_total{{operation=\"{operation}\"}} {requests}"
            );
            let _ = writeln!(
                output,
                "nebula_request_errors_total{{operation=\"{operation}\"}} {errors}"
            );
            let _ = writeln!(
                output,
                "nebula_request_duration_seconds_sum{{operation=\"{operation}\"}} {sum}"
            );
            let _ = writeln!(
                output,
                "nebula_request_duration_seconds_count{{operation=\"{operation}\"}} {requests}"
            );
            for (bucket, bound) in metrics.latency_buckets.iter().zip(LATENCY_BUCKETS) {
                let count = bucket.load(Ordering::Relaxed);
                let _ = writeln!(output, "nebula_request_duration_seconds_bucket{{operation=\"{operation}\",le=\"{bound}\"}} {count}");
            }
            let _ = writeln!(output, "nebula_request_duration_seconds_bucket{{operation=\"{operation}\",le=\"+Inf\"}} {requests}");
        }
        let active = self.active_connections.load(Ordering::Relaxed);
        let _ = writeln!(output, "# HELP nebula_active_connections Current active TCP connections.\n# TYPE nebula_active_connections gauge\nnebula_active_connections {active}");
        let auth_success = self.auth_success.load(Ordering::Relaxed);
        let auth_failure = self.auth_failure.load(Ordering::Relaxed);
        let auth_duration_sum =
            self.auth_duration_ns.load(Ordering::Relaxed) as f64 / 1_000_000_000.0;
        let auth_duration_count = self.auth_duration_count.load(Ordering::Relaxed);
        let authz_checks = self.authz_checks.load(Ordering::Relaxed);
        let authz_denied = self.authz_denied.load(Ordering::Relaxed);
        let auth_active = self.auth_active_connections.load(Ordering::Relaxed);
        let auth_revocations = self.auth_revocations.load(Ordering::Relaxed);
        let _ = writeln!(output, "# HELP nebula_auth_success_total Successful authentications.\n# TYPE nebula_auth_success_total counter\nnebula_auth_success_total {auth_success}");
        let _ = writeln!(output, "# HELP nebula_auth_failure_total Failed authentications.\n# TYPE nebula_auth_failure_total counter\nnebula_auth_failure_total {auth_failure}");
        let _ = writeln!(output, "# HELP nebula_auth_duration_seconds Authentication duration in seconds.\n# TYPE nebula_auth_duration_seconds histogram");
        let _ = writeln!(output, "nebula_auth_duration_seconds_sum {auth_duration_sum}\nnebula_auth_duration_seconds_count {auth_duration_count}");
        for (bucket, bound) in self.auth_duration_buckets.iter().zip(LATENCY_BUCKETS) {
            let count = bucket.load(Ordering::Relaxed);
            let _ = writeln!(
                output,
                "nebula_auth_duration_seconds_bucket{{le=\"{bound}\"}} {count}"
            );
        }
        let _ = writeln!(
            output,
            "nebula_auth_duration_seconds_bucket{{le=\"+Inf\"}} {auth_duration_count}"
        );
        let _ = writeln!(output, "# HELP nebula_authz_checks_total Authorization checks.\n# TYPE nebula_authz_checks_total counter\nnebula_authz_checks_total {authz_checks}");
        let _ = writeln!(output, "# HELP nebula_authz_denied_total Denied authorization checks.\n# TYPE nebula_authz_denied_total counter\nnebula_authz_denied_total {authz_denied}");
        let _ = writeln!(output, "# HELP nebula_auth_active_connections Authenticated TCP connections.\n# TYPE nebula_auth_active_connections gauge\nnebula_auth_active_connections {auth_active}");
        let _ = writeln!(output, "# HELP nebula_auth_revocations_total Token revocations.\n# TYPE nebula_auth_revocations_total counter\nnebula_auth_revocations_total {auth_revocations}");
        let entries = store.len();
        let memory = store.memory_usage();
        let _ = writeln!(output, "# HELP nebula_cache_entries Current cache entry count.\n# TYPE nebula_cache_entries gauge\nnebula_cache_entries {entries}");
        let _ = writeln!(output, "# HELP nebula_cache_memory_bytes Approximate bytes held by cache keys and values.\n# TYPE nebula_cache_memory_bytes gauge\nnebula_cache_memory_bytes {memory}");

        let snapshots = self.persistence.snapshots.load(Ordering::Relaxed);
        let snapshot_failures = self.persistence.snapshot_failures.load(Ordering::Relaxed);
        let snapshot_seconds = self
            .persistence
            .snapshot_duration_ns
            .load(Ordering::Relaxed) as f64
            / 1_000_000_000.0;
        let recoveries = self.persistence.recoveries.load(Ordering::Relaxed);
        let recovery_failures = self.persistence.recovery_failures.load(Ordering::Relaxed);
        let recovery_seconds = self
            .persistence
            .recovery_duration_ns
            .load(Ordering::Relaxed) as f64
            / 1_000_000_000.0;
        let _ = writeln!(output, "# HELP nebula_snapshots_total Snapshot attempts.\n# TYPE nebula_snapshots_total counter\nnebula_snapshots_total {snapshots}");
        let _ = writeln!(output, "# HELP nebula_snapshot_failures_total Failed snapshot attempts.\n# TYPE nebula_snapshot_failures_total counter\nnebula_snapshot_failures_total {snapshot_failures}");
        let last_snapshot_timestamp = self
            .persistence
            .last_snapshot_success_timestamp_secs
            .load(Ordering::Relaxed);
        let _ = writeln!(output, "# HELP nebula_snapshot_last_success_timestamp_seconds Unix timestamp of the last successful snapshot, or zero if none has succeeded.\n# TYPE nebula_snapshot_last_success_timestamp_seconds gauge\nnebula_snapshot_last_success_timestamp_seconds {last_snapshot_timestamp}");
        let _ = writeln!(output, "# HELP nebula_recoveries_total Recovery attempts.\n# TYPE nebula_recoveries_total counter\nnebula_recoveries_total {recoveries}");
        let _ = writeln!(output, "# HELP nebula_recovery_failures_total Failed recovery attempts.\n# TYPE nebula_recovery_failures_total counter\nnebula_recovery_failures_total {recovery_failures}");
        render_histogram(
            &mut output,
            "nebula_snapshot_duration_seconds",
            "Duration of snapshot attempts in seconds.",
            snapshot_seconds,
            snapshots,
            &self.persistence.snapshot_duration_buckets,
        );
        render_histogram(
            &mut output,
            "nebula_recovery_duration_seconds",
            "Duration of recovery attempts in seconds.",
            recovery_seconds,
            recoveries,
            &self.persistence.recovery_duration_buckets,
        );

        let snapshot = health.snapshot();
        let _ = writeln!(output, "# HELP nebula_health_status Whether the application or component is healthy.\n# TYPE nebula_health_status gauge");
        let overall = if snapshot.overall == ApplicationHealth::Healthy {
            1.0
        } else {
            0.0
        };
        let _ = writeln!(
            output,
            "nebula_health_status{{component=\"application\"}} {overall}"
        );
        for (component, status) in &snapshot.components {
            let component_label = stable_component_label(component);
            let healthy = if *status == ComponentHealth::Healthy {
                1.0
            } else {
                0.0
            };
            let _ = writeln!(
                output,
                "nebula_health_status{{component=\"{component_label}\"}} {healthy}"
            );
        }
        let persistence_healthy = if snapshot.components.iter().all(|(component, status)| {
            !matches!(component, HealthComponent::Persistence(_))
                || *status == ComponentHealth::Healthy
        }) {
            1.0
        } else {
            0.0
        };
        let _ = writeln!(output, "# HELP nebula_persistence_healthy Whether persistence is healthy.\n# TYPE nebula_persistence_healthy gauge\nnebula_persistence_healthy {persistence_healthy}");
        if let Some(directory) = persistence_directory {
            let files = std::fs::read_dir(directory)
                .map(|entries| {
                    entries
                        .filter_map(Result::ok)
                        .filter(|entry| {
                            entry
                                .path()
                                .extension()
                                .is_some_and(|extension| extension == "nbs")
                        })
                        .count()
                })
                .unwrap_or(0);
            let _ = writeln!(output, "# HELP nebula_snapshot_files Current retained snapshot file count.\n# TYPE nebula_snapshot_files gauge\nnebula_snapshot_files {files}");
            if let Some(free_bytes) = available_disk_bytes(directory) {
                let _ = writeln!(output, "# HELP nebula_persistence_available_bytes Available bytes on the persistence filesystem.\n# TYPE nebula_persistence_available_bytes gauge\nnebula_persistence_available_bytes {free_bytes}");
            }
        }
        output
    }

    fn request_totals(&self) -> (u64, u64, u64, u64) {
        self.operations
            .iter()
            .fold((0, 0, 0, 0), |mut totals, operation| {
                totals.0 += operation.requests.load(Ordering::Relaxed);
                totals.1 += operation.errors.load(Ordering::Relaxed);
                totals.2 += operation.latency_sum_ns.load(Ordering::Relaxed);
                totals.3 += operation.requests.load(Ordering::Relaxed);
                totals
            })
    }
}

fn record_buckets(buckets: &[AtomicU64; 8], elapsed_ns: u64) {
    let elapsed_secs = elapsed_ns as f64 / 1_000_000_000.0;
    for (bucket, bound) in buckets.iter().zip(LATENCY_BUCKETS) {
        if elapsed_secs <= bound {
            bucket.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn render_histogram(
    output: &mut String,
    name: &str,
    description: &str,
    sum: f64,
    count: u64,
    buckets: &[AtomicU64; 8],
) {
    let _ = writeln!(
        output,
        "# HELP {name} {description}\n# TYPE {name} histogram"
    );
    for (bucket, bound) in buckets.iter().zip(LATENCY_BUCKETS) {
        let count = bucket.load(Ordering::Relaxed);
        let _ = writeln!(output, "{name}_bucket{{le=\"{bound}\"}} {count}");
    }
    let _ = writeln!(
        output,
        "{name}_bucket{{le=\"+Inf\"}} {count}\n{name}_sum {sum}\n{name}_count {count}"
    );
}

struct ExporterState {
    metrics: Arc<Metrics>,
    store: Arc<CacheStore>,
    lifecycle: watch::Receiver<ApplicationState>,
    health: HealthManager,
    persistence_directory: Option<PathBuf>,
}

pub(crate) async fn run_exporter(
    listener: TcpListener,
    metrics: Arc<Metrics>,
    store: Arc<CacheStore>,
    lifecycle: watch::Receiver<ApplicationState>,
    health: HealthManager,
    persistence_directory: Option<PathBuf>,
    heartbeat: WorkerHeartbeat,
    started: oneshot::Sender<()>,
) -> io::Result<()> {
    let address = listener.local_addr()?;
    let state = ExporterState {
        metrics,
        store,
        lifecycle: lifecycle.clone(),
        health,
        persistence_directory,
    };
    let app = Router::new()
        .route("/metrics", get(scrape))
        .with_state(Arc::new(state));
    let server = axum::serve(listener, app).with_graceful_shutdown(wait_for_shutdown(lifecycle));
    let mut server = Box::pin(async move { server.await });
    let _ = started.send(());
    tracing::info!(%address, "Prometheus metrics endpoint listening");
    let mut heartbeat_interval = interval(std::time::Duration::from_secs(1));
    heartbeat_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            result = &mut server => return result,
            _ = heartbeat_interval.tick() => heartbeat.heartbeat(),
        }
    }
}

async fn scrape(State(state): State<Arc<ExporterState>>) -> impl IntoResponse {
    let body = state.metrics.render_prometheus(
        &state.store,
        *state.lifecycle.borrow(),
        &state.health,
        state.persistence_directory.as_deref(),
    );
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}

async fn wait_for_shutdown(mut lifecycle: watch::Receiver<ApplicationState>) {
    loop {
        if matches!(
            *lifecycle.borrow(),
            ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed
        ) {
            return;
        }
        if lifecycle.changed().await.is_err() {
            return;
        }
    }
}

fn average_millis(total_ns: u64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        total_ns as f64 / count as f64 / 1_000_000.0
    }
}

fn rate(history: &[HistoryPoint], value: impl Fn(&HistoryPoint) -> u64) -> f64 {
    if history.len() < 2 {
        return 0.0;
    }
    let first = &history[history.len() - 2];
    let last = &history[history.len() - 1];
    let elapsed = last.elapsed_secs.saturating_sub(first.elapsed_secs).max(1);
    value(last).saturating_sub(value(first)) as f64 / elapsed as f64
}

fn lifecycle_name(state: ApplicationState) -> &'static str {
    match state {
        ApplicationState::Initializing => "initializing",
        ApplicationState::Ready => "ready",
        ApplicationState::Degraded => "degraded",
        ApplicationState::Stopping => "stopping",
        ApplicationState::Stopped => "stopped",
        ApplicationState::Failed => "failed",
    }
}

fn component_name(component: &HealthComponent) -> &'static str {
    match component {
        HealthComponent::TcpServer => "tcp_server",
        HealthComponent::CacheEngine => "cache_engine",
        HealthComponent::AdminConsole => "admin_console",
        HealthComponent::MetricsEndpoint => "metrics_endpoint",
        HealthComponent::BackgroundWorker(_) => "background_worker",
        HealthComponent::Persistence(_) => "persistence",
    }
}

fn stable_component_label(component: &HealthComponent) -> &'static str {
    component_name(component)
}

fn health_name(status: &ComponentHealth) -> &'static str {
    match status {
        ComponentHealth::Healthy => "healthy",
        ComponentHealth::Degraded(_) => "degraded",
        ComponentHealth::Unhealthy(_) => "unhealthy",
    }
}

fn metric(output: &mut String, name: &str, help: &str, kind: &str, value: &str) {
    let _ = writeln!(output, "# HELP {name} {help}\n# TYPE {name} {kind}");
    if !value.is_empty() {
        let _ = writeln!(output, "{name} {value}");
    }
}

#[cfg(unix)]
fn available_disk_bytes(path: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let path = path.ancestors().find(|candidate| candidate.is_dir())?;
    let path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let result = unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) };
    if result != 0 {
        return None;
    }
    let stats = unsafe { stats.assume_init() };
    Some((stats.f_bavail as u64).saturating_mul(stats.f_frsize as u64))
}

#[cfg(not(unix))]
fn available_disk_bytes(_: &Path) -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use nebula_core::CacheStore;
    use nebula_health::{
        lifecycle::{ApplicationState, Lifecycle},
        HealthManager,
    };

    use super::Metrics;

    #[test]
    fn exports_operation_metrics_with_bounded_labels_and_histograms() {
        let metrics = Metrics::new();
        metrics.record_request("get", true, 2_000_000);
        metrics.record_request("get", false, 8_000_000);
        metrics.record_snapshot(10_000_000, true);
        metrics.record_authentication(true, 2_000_000);
        metrics.record_authentication(false, 8_000_000);
        metrics.authorization_check(false);
        metrics.authenticated_connection_opened();
        metrics.record_revocation();
        let lifecycle = Lifecycle::new();
        let health = HealthManager::new(lifecycle.clone(), false);
        let output =
            metrics.render_prometheus(&CacheStore::new(), lifecycle.state(), &health, None);
        assert!(output.contains("nebula_requests_total{operation=\"get\"} 2"));
        assert!(output.contains("nebula_request_errors_total{operation=\"get\"} 1"));
        assert!(output
            .contains("nebula_request_duration_seconds_bucket{operation=\"get\",le=\"0.01\"} 2"));
        assert!(output.contains("nebula_lifecycle_state{state=\"initializing\"} 1"));
        assert!(output.contains("nebula_snapshot_last_success_timestamp_seconds "));
        assert!(output.contains("nebula_auth_success_total 1"));
        assert!(output.contains("nebula_auth_failure_total 1"));
        assert!(output.contains("nebula_authz_checks_total 1"));
        assert!(output.contains("nebula_authz_denied_total 1"));
        assert!(output.contains("nebula_auth_active_connections 1"));
        assert!(output.contains("nebula_auth_revocations_total 1"));
        assert!(!output.contains("cache_key"));
    }

    #[test]
    fn records_bounded_runtime_history_for_dashboard() {
        let metrics = Metrics::new();
        let lifecycle = Lifecycle::new();
        lifecycle.transition(ApplicationState::Ready).unwrap();
        let health = HealthManager::new(lifecycle.clone(), false);
        metrics.record_request("ping", true, 4_000_000);
        metrics.record_snapshot(10_000_000, true);
        let snapshot = metrics.dashboard_snapshot(&CacheStore::new(), lifecycle.state(), &health);
        assert_eq!(snapshot.requests_total, 1);
        assert!(snapshot.last_snapshot_success_timestamp_secs.is_some());
        assert_eq!(snapshot.state, "ready");
        assert_eq!(snapshot.history.len(), 1);
    }

    #[tokio::test]
    async fn prometheus_endpoint_serves_metrics_and_shuts_down_gracefully() {
        use std::sync::Arc;

        use nebula_health::HealthComponent;
        use nebula_watchdog::Watchdog;
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::{TcpListener, TcpStream},
            sync::oneshot,
        };

        use super::run_exporter;

        let lifecycle = Lifecycle::new();
        lifecycle.transition(ApplicationState::Ready).unwrap();
        let health = HealthManager::new(lifecycle.clone(), false);
        let watchdog = Watchdog::new(health.clone());
        let heartbeat = watchdog.register_worker(
            "metrics-endpoint-test",
            HealthComponent::MetricsEndpoint,
            std::time::Duration::from_secs(3),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (started_tx, started_rx) = oneshot::channel();
        let exporter = tokio::spawn(run_exporter(
            listener,
            Metrics::new(),
            Arc::new(CacheStore::new()),
            lifecycle.subscribe(),
            health,
            None,
            heartbeat,
            started_tx,
        ));
        started_rx.await.unwrap();

        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200"));
        assert!(response.contains("nebula_uptime_seconds"));
        assert!(response.contains("nebula_request_duration_seconds_bucket"));

        lifecycle.transition(ApplicationState::Stopping).unwrap();
        exporter.await.unwrap().unwrap();
    }
}
