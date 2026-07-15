//! Shared cooperative cancellation and deadline control.

use crate::{MongrelError, Result};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CancellationReason {
    None = 0,
    ClientRequest = 1,
    Deadline = 2,
    ClientDisconnected = 3,
    SessionClosed = 4,
    ServerShutdown = 5,
}

impl CancellationReason {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::ClientRequest,
            2 => Self::Deadline,
            3 => Self::ClientDisconnected,
            4 => Self::SessionClosed,
            5 => Self::ServerShutdown,
            _ => Self::None,
        }
    }
}

#[derive(Debug)]
struct CancellationState {
    reason: AtomicU8,
}

/// Cloneable cooperative control shared through one execution.
///
/// Child controls inherit every ancestor cancellation state and the tightest
/// deadline. Cancelling a child does not cancel its parent.
#[derive(Debug, Clone)]
pub struct ExecutionControl {
    states: Arc<Vec<Arc<CancellationState>>>,
    own: Arc<CancellationState>,
    wake: Arc<tokio::sync::Notify>,
    deadline: Option<Instant>,
}

impl ExecutionControl {
    pub fn new(deadline: Option<Instant>) -> Self {
        let own = Arc::new(CancellationState {
            reason: AtomicU8::new(CancellationReason::None as u8),
        });
        Self {
            states: Arc::new(vec![Arc::clone(&own)]),
            own,
            wake: Arc::new(tokio::sync::Notify::new()),
            deadline,
        }
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        Self::new(Some(Instant::now() + timeout))
    }

    pub fn child_with_deadline(&self, deadline: Option<Instant>) -> Self {
        let deadline = match (self.deadline, deadline) {
            (Some(parent), Some(child)) => Some(parent.min(child)),
            (Some(parent), None) => Some(parent),
            (None, child) => child,
        };
        let own = Arc::new(CancellationState {
            reason: AtomicU8::new(CancellationReason::None as u8),
        });
        let mut states = self.states.as_ref().clone();
        states.push(Arc::clone(&own));
        Self {
            states: Arc::new(states),
            own,
            wake: Arc::clone(&self.wake),
            deadline,
        }
    }

    pub fn child_with_timeout(&self, timeout: Duration) -> Self {
        self.child_with_deadline(Some(Instant::now() + timeout))
    }

    pub fn cancel(&self, reason: CancellationReason) {
        if reason == CancellationReason::None {
            return;
        }
        let _ = self.own.reason.compare_exchange(
            CancellationReason::None as u8,
            reason as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.wake.notify_waiters();
    }

    pub fn checkpoint(&self) -> Result<()> {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            self.cancel(CancellationReason::Deadline);
        }
        match self.reason() {
            CancellationReason::None => Ok(()),
            CancellationReason::Deadline => Err(MongrelError::DeadlineExceeded),
            _ => Err(MongrelError::Cancelled),
        }
    }

    pub async fn cancelled(&self) {
        loop {
            if self.checkpoint().is_err() {
                return;
            }
            let notified = self.wake.notified();
            if self.checkpoint().is_err() {
                return;
            }
            if let Some(remaining) = self.remaining_duration() {
                if tokio::time::timeout(remaining, notified).await.is_err() {
                    self.cancel(CancellationReason::Deadline);
                    return;
                }
            } else {
                notified.await;
            }
        }
    }

    pub fn remaining_duration(&self) -> Option<Duration> {
        self.deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn reason(&self) -> CancellationReason {
        self.states
            .iter()
            .map(|state| CancellationReason::from_u8(state.reason.load(Ordering::Acquire)))
            .find(|reason| *reason != CancellationReason::None)
            .unwrap_or(CancellationReason::None)
    }

    pub fn is_cancelled(&self) -> bool {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            self.cancel(CancellationReason::Deadline);
        }
        self.reason() != CancellationReason::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_reason_wins_and_unknown_values_are_checked() {
        let control = ExecutionControl::new(None);
        control.cancel(CancellationReason::ClientRequest);
        control.cancel(CancellationReason::ServerShutdown);
        assert_eq!(control.reason(), CancellationReason::ClientRequest);
        assert!(matches!(control.checkpoint(), Err(MongrelError::Cancelled)));
        assert_eq!(CancellationReason::from_u8(255), CancellationReason::None);
    }

    #[test]
    fn deadline_returns_structured_error() {
        let control = ExecutionControl::new(Some(Instant::now()));
        assert!(matches!(
            control.checkpoint(),
            Err(MongrelError::DeadlineExceeded)
        ));
        assert_eq!(control.reason(), CancellationReason::Deadline);
    }

    #[test]
    fn children_inherit_parent_but_do_not_cancel_it() {
        let parent = ExecutionControl::new(None);
        let child = parent.child_with_deadline(None);
        child.cancel(CancellationReason::ClientRequest);
        assert!(parent.checkpoint().is_ok());
        assert!(child.checkpoint().is_err());

        let sibling = parent.child_with_deadline(None);
        parent.cancel(CancellationReason::SessionClosed);
        assert_eq!(sibling.reason(), CancellationReason::SessionClosed);
        assert!(sibling.checkpoint().is_err());
    }

    #[test]
    fn child_deadline_cannot_weaken_parent() {
        let parent_deadline = Instant::now() + Duration::from_secs(1);
        let parent = ExecutionControl::new(Some(parent_deadline));
        let child = parent.child_with_timeout(Duration::from_secs(60));
        assert_eq!(child.deadline(), Some(parent_deadline));
    }
}
