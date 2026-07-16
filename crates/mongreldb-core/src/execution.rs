//! Shared cooperative cancellation and deadline control.

use crate::{MongrelError, Result};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
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
    /// Stable protocol spelling shared by daemon and language bindings.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ClientRequest => "client_request",
            Self::Deadline => "deadline",
            Self::ClientDisconnected => "client_disconnected",
            Self::SessionClosed => "session_closed",
            Self::ServerShutdown => "server_shutdown",
        }
    }

    pub fn from_protocol_str(value: &str) -> Option<Self> {
        match value {
            "none" => Some(Self::None),
            "client_request" => Some(Self::ClientRequest),
            "deadline" => Some(Self::Deadline),
            "client_disconnected" => Some(Self::ClientDisconnected),
            "session_closed" => Some(Self::SessionClosed),
            "server_shutdown" => Some(Self::ServerShutdown),
            _ => None,
        }
    }

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
    sequence: AtomicU64,
    reason: AtomicU8,
}

#[derive(Debug, Default)]
struct CancellationOrder {
    next_sequence: u64,
}

/// Cloneable cooperative control shared through one execution.
///
/// Child controls inherit every ancestor cancellation state and the tightest
/// deadline. Cancelling a child does not cancel its parent. Cancellation events
/// are serialized across the hierarchy, so the first event visible to a control
/// remains its reason permanently.
#[derive(Debug, Clone)]
pub struct ExecutionControl {
    states: Arc<Vec<Arc<CancellationState>>>,
    own: Arc<CancellationState>,
    order: Arc<parking_lot::Mutex<CancellationOrder>>,
    wake: Arc<tokio::sync::Notify>,
    deadline: Option<Instant>,
}

impl ExecutionControl {
    pub fn new(deadline: Option<Instant>) -> Self {
        let own = Arc::new(CancellationState {
            sequence: AtomicU64::new(0),
            reason: AtomicU8::new(CancellationReason::None as u8),
        });
        Self {
            states: Arc::new(vec![Arc::clone(&own)]),
            own,
            order: Arc::new(parking_lot::Mutex::new(CancellationOrder::default())),
            wake: Arc::new(tokio::sync::Notify::new()),
            deadline,
        }
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        let now = Instant::now();
        Self::new(Some(now.checked_add(timeout).unwrap_or(now)))
    }

    pub fn child_with_deadline(&self, deadline: Option<Instant>) -> Self {
        let deadline = match (self.deadline, deadline) {
            (Some(parent), Some(child)) => Some(parent.min(child)),
            (Some(parent), None) => Some(parent),
            (None, child) => child,
        };
        let own = Arc::new(CancellationState {
            sequence: AtomicU64::new(0),
            reason: AtomicU8::new(CancellationReason::None as u8),
        });
        let mut states = self.states.as_ref().clone();
        states.push(Arc::clone(&own));
        Self {
            states: Arc::new(states),
            own,
            order: Arc::clone(&self.order),
            wake: Arc::clone(&self.wake),
            deadline,
        }
    }

    pub fn child_with_timeout(&self, timeout: Duration) -> Self {
        let now = Instant::now();
        self.child_with_deadline(Some(now.checked_add(timeout).unwrap_or(now)))
    }

    pub fn cancel(&self, reason: CancellationReason) {
        if reason == CancellationReason::None {
            return;
        }
        let mut order = self.order.lock();
        if self.own.reason.load(Ordering::Relaxed) == CancellationReason::None as u8 {
            self.own
                .sequence
                .store(order.next_sequence, Ordering::Relaxed);
            order.next_sequence = order.next_sequence.saturating_add(1);
            self.own.reason.store(reason as u8, Ordering::Release);
        }
        drop(order);
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
            .filter_map(|state| {
                let reason = CancellationReason::from_u8(state.reason.load(Ordering::Acquire));
                (reason != CancellationReason::None)
                    .then(|| (state.sequence.load(Ordering::Relaxed), reason))
            })
            .min_by_key(|(sequence, _)| *sequence)
            .map_or(CancellationReason::None, |(_, reason)| reason)
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
    fn first_reason_wins() {
        let control = ExecutionControl::new(None);
        control.cancel(CancellationReason::ClientRequest);
        control.cancel(CancellationReason::ServerShutdown);
        assert_eq!(control.reason(), CancellationReason::ClientRequest);
        assert!(matches!(control.checkpoint(), Err(MongrelError::Cancelled)));
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
    fn child_reason_uses_first_event_without_prior_observation() {
        let parent = ExecutionControl::new(None);
        let child = parent.child_with_deadline(None);

        child.cancel(CancellationReason::ClientRequest);
        parent.cancel(CancellationReason::ServerShutdown);

        assert_eq!(child.reason(), CancellationReason::ClientRequest);
        assert_eq!(parent.reason(), CancellationReason::ServerShutdown);
    }

    #[test]
    fn child_reason_uses_first_parent_event_without_prior_observation() {
        let parent = ExecutionControl::new(None);
        let child = parent.child_with_deadline(None);

        parent.cancel(CancellationReason::SessionClosed);
        child.cancel(CancellationReason::ClientRequest);

        assert_eq!(child.reason(), CancellationReason::SessionClosed);
    }

    #[test]
    fn cross_thread_parent_child_order_is_stable() {
        let parent = ExecutionControl::new(None);
        let child = parent.child_with_deadline(None);
        let (cancelled_tx, cancelled_rx) = std::sync::mpsc::sync_channel(0);
        let child_task = {
            let child = child.clone();
            std::thread::spawn(move || {
                child.cancel(CancellationReason::ClientRequest);
                cancelled_tx.send(()).unwrap();
            })
        };

        cancelled_rx.recv().unwrap();
        parent.cancel(CancellationReason::ServerShutdown);
        child_task.join().unwrap();

        assert_eq!(child.reason(), CancellationReason::ClientRequest);
    }

    #[test]
    fn concurrent_parent_child_cancellation_stays_stable() {
        let parent = ExecutionControl::new(None);
        let child = parent.child_with_deadline(None);
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let parent_task = {
            let parent = parent.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                parent.cancel(CancellationReason::ServerShutdown);
            })
        };
        let child_task = {
            let child = child.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                child.cancel(CancellationReason::ClientRequest);
            })
        };
        barrier.wait();
        parent_task.join().unwrap();
        child_task.join().unwrap();

        let observed = child.reason();
        assert_ne!(observed, CancellationReason::None);
        parent.cancel(CancellationReason::SessionClosed);
        child.cancel(CancellationReason::ClientDisconnected);
        assert_eq!(child.reason(), observed);
    }

    #[test]
    fn child_deadline_cannot_weaken_parent() {
        let parent_deadline = Instant::now() + Duration::from_secs(1);
        let parent = ExecutionControl::new(Some(parent_deadline));
        let child = parent.child_with_timeout(Duration::from_secs(60));
        assert_eq!(child.deadline(), Some(parent_deadline));
    }

    #[test]
    fn overflowing_timeouts_fail_closed_without_panicking() {
        let control = ExecutionControl::with_timeout(Duration::MAX);
        assert!(matches!(
            control.checkpoint(),
            Err(MongrelError::DeadlineExceeded)
        ));

        let parent = ExecutionControl::new(None);
        let child = parent.child_with_timeout(Duration::MAX);
        assert!(matches!(
            child.checkpoint(),
            Err(MongrelError::DeadlineExceeded)
        ));
    }
}
