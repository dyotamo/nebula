//! Periodically removes expired entries while the application is active.

use std::{sync::Arc, time::Duration};

use nebula_core::CacheStore;
use nebula_health::lifecycle::ApplicationState;
use tokio::time::{interval_at, Instant, MissedTickBehavior};
use tracing::info;

/// Runs expiration cleanup on a fixed interval until lifecycle shutdown is signaled.
///
/// Cleanup continues while degraded so expired entries do not accumulate when readiness is lost.
pub(crate) async fn run(
    store: Arc<CacheStore>,
    interval: Duration,
    mut lifecycle: tokio::sync::watch::Receiver<ApplicationState>,
) {
    // Defer the first scan for a full interval and avoid catch-up bursts after a slow cleanup.
    let mut cleanup_ticks = interval_at(Instant::now() + interval, interval);
    cleanup_ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = cleanup_ticks.tick() => {
                let removed = store.cleanup_expired();
                if removed > 0 {
                    info!(entries = removed, "expired cache entries cleaned up");
                }
            }
            // Nonterminal changes keep maintenance running; sender loss also ends the worker.
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
