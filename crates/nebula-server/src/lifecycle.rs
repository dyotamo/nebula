use std::fmt;

use thiserror::Error;
use tokio::sync::watch;
use tracing::info;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationState {
    Initializing,
    Ready,
    Degraded,
    Stopping,
    Stopped,
    Failed,
}

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

#[derive(Debug, Error)]
#[error("invalid application lifecycle transition from {from} to {to}")]
pub struct LifecycleError {
    pub from: ApplicationState,
    pub to: ApplicationState,
}

#[derive(Clone)]
pub(crate) struct Lifecycle {
    state: watch::Sender<ApplicationState>,
}

impl Lifecycle {
    pub(crate) fn new() -> Self {
        let (state, _) = watch::channel(ApplicationState::Initializing);
        Self { state }
    }

    pub fn state(&self) -> ApplicationState {
        *self.state.borrow()
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<ApplicationState> {
        self.state.subscribe()
    }

    pub(crate) fn transition(&self, to: ApplicationState) -> Result<(), LifecycleError> {
        let mut from = self.state();
        let mut accepted = false;
        self.state.send_if_modified(|state| {
            from = *state;
            if valid_transition(*state, to) {
                *state = to;
                accepted = true;
                true
            } else {
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

fn valid_transition(from: ApplicationState, to: ApplicationState) -> bool {
    matches!(
        (from, to),
        (ApplicationState::Initializing, ApplicationState::Ready)
            | (ApplicationState::Initializing, ApplicationState::Failed)
            | (ApplicationState::Ready, ApplicationState::Stopping)
            | (ApplicationState::Ready, ApplicationState::Failed)
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
