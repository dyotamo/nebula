use std::fmt;

use thiserror::Error;
use tokio::sync::watch;
use tracing::info;

/// Process lifecycle used to gate readiness and coordinate service shutdown.
///
/// A component health failure can withdraw readiness without killing the process.
/// `Failed`, unlike `Degraded`, cannot recover within the same lifecycle instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationState {
    /// Startup is in progress; cache requests must not be served yet.
    Initializing,
    /// Startup completed and cache requests may be served.
    Ready,
    /// Readiness is withdrawn, but recovery to `Ready` is allowed.
    Degraded,
    /// Shutdown is in progress; returning to a serving state is forbidden.
    Stopping,
    /// Graceful shutdown completed; this state is terminal.
    Stopped,
    /// An unrecoverable failure occurred; this state is terminal.
    Failed,
}

// Use explicit operational labels rather than Debug formatting in logs and probes.
impl fmt::Display for ApplicationState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Initializing => "INITIALIZING",
            Self::Ready => "READY",
            Self::Degraded => "DEGRADED",
            Self::Stopping => "STOPPING",
            Self::Stopped => "STOPPED",
            Self::Failed => "FAILED",
        };
        formatter.write_str(name)
    }
}

/// A rejected transition; the lifecycle remains unchanged.
#[derive(Debug, Error)]
#[error("invalid application lifecycle transition from {from} to {to}")]
pub struct LifecycleError {
    /// State observed while validating the transition.
    pub from: ApplicationState,
    /// Requested destination state.
    pub to: ApplicationState,
}

/// Validated lifecycle shared by all clones, with latest-state watch notifications.
///
/// Cloning does not create an independent state machine. Transitions remain recorded
/// even when no receiver is subscribed; watchers are observers, not state owners.
#[derive(Clone)]
pub struct Lifecycle {
    state: watch::Sender<ApplicationState>,
}

impl Lifecycle {
    /// Creates a shared lifecycle in the non-serving `Initializing` state.
    /// Startup must explicitly transition to `Ready`; construction does not imply readiness.
    pub fn new() -> Self {
        let (state, _) = watch::channel(ApplicationState::Initializing);
        Self { state }
    }

    /// Returns the current state without marking any subscriber's value as seen.
    /// This is an observation only, not a guard against a concurrent transition.
    pub fn state(&self) -> ApplicationState {
        *self.state.borrow()
    }

    /// Subscribes to future changes; rapid transitions may coalesce into the latest state.
    /// The current value starts as seen, so borrow it before waiting if it matters immediately.
    pub fn subscribe(&self) -> watch::Receiver<ApplicationState> {
        self.state.subscribe()
    }

    /// Validates and publishes a transition, rejecting repeats and exits from terminal states.
    ///
    /// Returns [`LifecycleError`] without notifying subscribers when the transition is invalid.
    /// Validation and replacement are atomic with respect to other transitions; an earlier
    /// call to `state` cannot be used to reserve a particular source state.
    pub fn transition(&self, to: ApplicationState) -> Result<(), LifecycleError> {
        let mut from = self.state();
        let mut accepted = false;
        // Validate under the watch write lock so concurrent transitions cannot use a stale state.
        self.state.send_if_modified(|state| {
            from = *state;
            if valid_transition(*state, to) {
                *state = to;
                accepted = true;
                true
            } else {
                // Returning false suppresses a notification for repeats and rejected edges.
                false
            }
        });

        if accepted {
            info!(from = %from, to = %to, "application lifecycle transition");
            Ok(())
        } else {
            Err(LifecycleError { from, to })
        }
    }
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self::new()
    }
}

// Keep every permitted edge explicit: health can toggle Ready/Degraded, but startup
// cannot skip straight to shutdown, and neither terminal state has an outgoing edge.
fn valid_transition(from: ApplicationState, to: ApplicationState) -> bool {
    matches!(
        (from, to),
        (ApplicationState::Initializing, ApplicationState::Ready)
            | (ApplicationState::Initializing, ApplicationState::Failed)
            | (ApplicationState::Ready, ApplicationState::Degraded)
            | (ApplicationState::Ready, ApplicationState::Stopping)
            | (ApplicationState::Ready, ApplicationState::Failed)
            | (ApplicationState::Degraded, ApplicationState::Ready)
            | (ApplicationState::Degraded, ApplicationState::Stopping)
            | (ApplicationState::Degraded, ApplicationState::Failed)
            | (ApplicationState::Stopping, ApplicationState::Stopped)
            | (ApplicationState::Stopping, ApplicationState::Failed)
    )
}

#[cfg(test)]
mod tests {
    use super::{ApplicationState, Lifecycle};

    #[test]
    fn accepts_supported_lifecycle_transitions() {
        let lifecycle = Lifecycle::new();
        assert_eq!(lifecycle.state(), ApplicationState::Initializing);
        lifecycle.transition(ApplicationState::Ready).unwrap();
        lifecycle.transition(ApplicationState::Stopping).unwrap();
        lifecycle.transition(ApplicationState::Stopped).unwrap();
        assert_eq!(lifecycle.state(), ApplicationState::Stopped);
    }

    #[test]
    fn rejects_invalid_lifecycle_transitions() {
        let lifecycle = Lifecycle::new();
        assert!(lifecycle.transition(ApplicationState::Stopped).is_err());
        assert!(lifecycle.transition(ApplicationState::Degraded).is_err());
        lifecycle.transition(ApplicationState::Ready).unwrap();
        assert!(lifecycle
            .transition(ApplicationState::Initializing)
            .is_err());
    }
}
