//! Shared storage-core plumbing (spec §10.1, S1A-003/S1A-004).
//!
//! This module hosts the two cross-cutting pieces of the Stage 1A "one shared
//! storage core" work that are independent of the [`crate::database`] internals:
//!
//! - [`DatabaseFileIdentity`] — the stable, directory-handle-derived identity
//!   of a database root (S1A-003). The canonical path is kept for diagnostics
//!   only; identity is the device + inode (Unix) or volume serial + file index
//!   (Windows) of the pinned directory descriptor, so renames and symlink
//!   aliases of the same root collapse onto one identity.
//! - [`LifecycleState`] / [`LifecycleController`] / [`OperationGuard`] — the
//!   core lifecycle state machine (S1A-004). Every operation on a shared core
//!   holds an [`OperationGuard`]; `shutdown()` transitions through
//!   `Draining`/`Closing` to `Closed` while new operations are rejected.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use crate::error::{MongrelError, Result};

/// Stable identity of a database root directory (spec §10.1, S1A-003).
///
/// Equality and hashing use only the durable directory-handle identity, never
/// the path text: two paths that resolve to the same directory (symlinks,
/// `..` segments, renamed parents) map to one identity. `canonical_path` is
/// retained for diagnostics and error messages.
#[derive(Clone, Debug)]
pub struct DatabaseFileIdentity {
    stable: crate::durable_file::DurableFileIdentity,
    canonical_path: PathBuf,
}

impl DatabaseFileIdentity {
    /// Resolve the stable identity of an existing database root.
    ///
    /// The path is canonicalized and pinned through a [`crate::durable_file::DurableRoot`]
    /// descriptor so the identity is read from the directory handle itself.
    pub fn for_path(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        let canonical_path = root.canonicalize().map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                MongrelError::NotFound(format!("database root {}: {error}", root.display()))
            } else {
                MongrelError::Io(error)
            }
        })?;
        let durable_root = crate::durable_file::DurableRoot::open(&canonical_path)?;
        let stable = durable_root.file_identity()?;
        Ok(Self {
            stable,
            canonical_path,
        })
    }

    /// Build an identity from an already-pinned durable root.
    pub fn from_durable_root(root: &crate::durable_file::DurableRoot) -> Result<Self> {
        Ok(Self {
            stable: root.file_identity()?,
            canonical_path: root.canonical_path().to_path_buf(),
        })
    }

    /// Canonical path of the root at identity resolution time. Diagnostics
    /// only — never use this as the identity itself.
    pub fn canonical_path(&self) -> &Path {
        &self.canonical_path
    }
}

impl PartialEq for DatabaseFileIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.stable == other.stable
    }
}

impl Eq for DatabaseFileIdentity {}

impl std::hash::Hash for DatabaseFileIdentity {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.stable.hash(state);
    }
}

impl std::fmt::Display for DatabaseFileIdentity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.canonical_path.display())
    }
}

/// Lifecycle states of one storage core (spec §10.1, S1A-004).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleState {
    /// Recovery, WAL opening, open-generation advancement, and table mounting
    /// are in progress. No operations are admitted.
    Opening,
    /// The core admits operations.
    Open,
    /// `shutdown()` has begun: new sessions and writes are rejected while
    /// in-flight operations drain.
    Draining,
    /// Operations have drained; durable state is being synced, workers are
    /// stopping, and the file lock is being released.
    Closing,
    /// The core is fully shut down. Every operation fails.
    Closed,
    /// An unrecoverable internal error (e.g. a durability poison) left the
    /// core unable to continue. Every operation fails.
    Poisoned,
}

impl LifecycleState {
    /// Whether the core currently admits new operations.
    pub fn admits_operations(self) -> bool {
        matches!(self, LifecycleState::Open)
    }
}

impl std::fmt::Display for LifecycleState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            LifecycleState::Opening => "opening",
            LifecycleState::Open => "open",
            LifecycleState::Draining => "draining",
            LifecycleState::Closing => "closing",
            LifecycleState::Closed => "closed",
            LifecycleState::Poisoned => "poisoned",
        };
        formatter.write_str(name)
    }
}

/// The lifecycle controller embedded in every `DatabaseCore` (spec §10.1,
/// S1A-004). It owns the state machine plus the in-flight operation counter
/// that `shutdown()` drains against.
///
/// The controller is shared through an `Arc` so [`OperationGuard`]s can be
/// `'static` and cross threads freely (a query worker may hold a guard long
/// after the initiating handle returned).
#[derive(Debug)]
pub struct LifecycleController {
    inner: Mutex<LifecycleInner>,
    drained: Condvar,
}

#[derive(Debug)]
struct LifecycleInner {
    state: LifecycleState,
    active_operations: u64,
}

impl Default for LifecycleController {
    fn default() -> Self {
        Self::new()
    }
}

impl LifecycleController {
    /// A controller in the `Opening` state: the core admits no operations
    /// until [`Self::mark_open`] completes initialization.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(LifecycleInner {
                state: LifecycleState::Opening,
                active_operations: 0,
            }),
            drained: Condvar::new(),
        }
    }

    /// The current lifecycle state.
    pub fn state(&self) -> LifecycleState {
        self.inner.lock().state
    }

    /// Whether the core currently admits new operations.
    pub fn is_open(&self) -> bool {
        self.state().admits_operations()
    }

    /// `Opening` → `Open`. Called exactly once when core initialization
    /// finishes (recovery, WAL opening, open-generation advancement, and
    /// table mounting are done).
    pub fn mark_open(&self) {
        let mut inner = self.inner.lock();
        if matches!(inner.state, LifecycleState::Opening) {
            inner.state = LifecycleState::Open;
        }
    }

    /// `Open` → `Draining`: the first `shutdown()` step. Returns `true` when
    /// this call initiated the drain; `false` when the core was already past
    /// `Open` (a concurrent or repeated shutdown), in which case the caller
    /// must not drive the remaining shutdown steps.
    pub fn begin_shutdown(&self) -> bool {
        let mut inner = self.inner.lock();
        if matches!(inner.state, LifecycleState::Open) {
            inner.state = LifecycleState::Draining;
            return true;
        }
        false
    }

    /// Wait until every in-flight operation has drained or `deadline`
    /// elapses. The core stays in `Draining` either way; a timeout leaves the
    /// shutdown to the caller to retry or abandon.
    pub fn wait_drained(&self, deadline: Duration) -> Result<()> {
        let started = Instant::now();
        let mut inner = self.inner.lock();
        loop {
            if inner.active_operations == 0 {
                return Ok(());
            }
            let elapsed = started.elapsed();
            if elapsed >= deadline {
                return Err(MongrelError::DatabaseBusy {
                    strong_handles: inner.active_operations as usize,
                });
            }
            let remaining = deadline - elapsed;
            if self
                .drained
                .wait_for(&mut inner, remaining.min(Duration::from_millis(50)))
                .timed_out()
                && started.elapsed() >= deadline
            {
                return Err(MongrelError::DatabaseBusy {
                    strong_handles: inner.active_operations as usize,
                });
            }
        }
    }

    /// `Draining` → `Closing`: operations have drained; durable sync, worker
    /// stop, and file-lock release happen in this state.
    pub fn mark_closing(&self) {
        let mut inner = self.inner.lock();
        if matches!(inner.state, LifecycleState::Draining) {
            inner.state = LifecycleState::Closing;
        }
    }

    /// `Closing` → `Closed`: the final shutdown step.
    pub fn mark_closed(&self) {
        let mut inner = self.inner.lock();
        if matches!(
            inner.state,
            LifecycleState::Closing | LifecycleState::Draining
        ) {
            inner.state = LifecycleState::Closed;
        }
        self.drained.notify_all();
    }

    /// Any state → `Poisoned`: an unrecoverable internal error. Poison is
    /// terminal and sticky — a poisoned core never returns to `Open`.
    pub fn poison(&self) {
        let mut inner = self.inner.lock();
        inner.state = LifecycleState::Poisoned;
        drop(inner);
        self.drained.notify_all();
    }

    /// Admit one operation (S1A-004: "every operation holds an
    /// `OperationGuard`"). New operations are rejected unless the core is
    /// `Open`; the returned guard releases its slot on drop.
    pub fn begin_operation(self: &Arc<Self>) -> Result<OperationGuard> {
        let mut inner = self.inner.lock();
        if !inner.state.admits_operations() {
            return Err(MongrelError::Conflict(format!(
                "database core is not open (lifecycle state: {})",
                inner.state
            )));
        }
        inner.active_operations = inner
            .active_operations
            .checked_add(1)
            .ok_or_else(|| MongrelError::Full("operation counter exhausted".into()))?;
        Ok(OperationGuard {
            lifecycle: Arc::clone(self),
        })
    }

    /// Number of operations currently holding a guard.
    pub fn active_operations(&self) -> u64 {
        self.inner.lock().active_operations
    }

    fn end_operation(&self) {
        let mut inner = self.inner.lock();
        inner.active_operations = inner.active_operations.saturating_sub(1);
        if inner.active_operations == 0 {
            drop(inner);
            self.drained.notify_all();
        }
    }
}

/// RAII proof that one operation holds the core open (spec §10.1, S1A-004).
/// Dropping the guard releases the operation slot; `shutdown()` waits for all
/// outstanding guards before closing.
#[derive(Debug)]
pub struct OperationGuard {
    lifecycle: Arc<LifecycleController>,
}

impl OperationGuard {
    /// The lifecycle state the guard was taken under (always
    /// [`LifecycleState::Open`]).
    pub fn lifecycle(&self) -> &Arc<LifecycleController> {
        &self.lifecycle
    }
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        self.lifecycle.end_operation();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_identity_ignores_path_spelling() {
        let dir = std::env::temp_dir().join(format!(
            "mongreldb-core-identity-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(dir.join("sub")).unwrap();
        let direct = DatabaseFileIdentity::for_path(dir.join("sub")).unwrap();
        let aliased =
            DatabaseFileIdentity::for_path(dir.join("sub").join("..").join("sub")).unwrap();
        assert_eq!(direct, aliased);
        let other = DatabaseFileIdentity::for_path(&dir).unwrap();
        assert_ne!(direct, other);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn lifecycle_rejects_operations_unless_open() {
        let lifecycle = Arc::new(LifecycleController::new());
        assert_eq!(lifecycle.state(), LifecycleState::Opening);
        assert!(lifecycle.begin_operation().is_err());
        lifecycle.mark_open();
        let guard = lifecycle.begin_operation().unwrap();
        assert_eq!(lifecycle.active_operations(), 1);
        drop(guard);
        assert_eq!(lifecycle.active_operations(), 0);
    }

    #[test]
    fn shutdown_drains_then_closes() {
        let lifecycle = Arc::new(LifecycleController::new());
        lifecycle.mark_open();
        let guard = lifecycle.begin_operation().unwrap();
        assert!(lifecycle.begin_shutdown());
        // New operations are rejected while draining.
        assert!(lifecycle.begin_operation().is_err());
        // A second shutdown attempt does not take ownership of the drain.
        assert!(!lifecycle.begin_shutdown());
        assert!(
            lifecycle.wait_drained(Duration::from_millis(10)).is_err(),
            "drain must time out while a guard is held"
        );
        drop(guard);
        lifecycle.wait_drained(Duration::from_secs(1)).unwrap();
        lifecycle.mark_closing();
        assert_eq!(lifecycle.state(), LifecycleState::Closing);
        lifecycle.mark_closed();
        assert_eq!(lifecycle.state(), LifecycleState::Closed);
        assert!(lifecycle.begin_operation().is_err());
    }

    #[test]
    fn poison_is_terminal() {
        let lifecycle = Arc::new(LifecycleController::new());
        lifecycle.mark_open();
        lifecycle.poison();
        assert_eq!(lifecycle.state(), LifecycleState::Poisoned);
        assert!(lifecycle.begin_operation().is_err());
        lifecycle.mark_open();
        assert_eq!(lifecycle.state(), LifecycleState::Poisoned);
        assert!(!lifecycle.begin_shutdown());
    }
}
