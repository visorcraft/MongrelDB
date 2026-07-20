//! Named fault-injection hooks (spec section 9.6, FND-006).
//!
//! Production code calls [`inject`] at durable boundaries. Hooks are disabled
//! by default and cost one atomic load when disarmed; tests arm named actions
//! through the registry and synchronize on hook hits with barriers, never
//! with arbitrary sleeps.
//!
//! # Hook naming
//!
//! Hook names are dot-namespaced: `<component>.<boundary>.<phase>`. The phase
//! is `before` or `after`. `before` fires immediately before the durable
//! boundary is crossed, so a failure aborts the operation before publication;
//! `after` fires immediately after publication, so the operation is durable
//! even when the hook reports a failure.
//!
//! # Canonical hook catalog
//!
//! WAL and commit publication (instrumented by the commit-log wave, not by
//! the storage-metadata wave):
//!
//! - `wal.append.before` / `wal.append.after`
//! - `wal.fsync.before` / `wal.fsync.after`
//! - `commit.publish.before` / `commit.publish.after`
//!
//! Storage metadata (instrumented in `mongreldb-core`):
//!
//! - `catalog.publish.before` / `catalog.publish.after` — atomic `CATALOG`
//!   checkpoint replacement.
//! - `snapshot.install.before` / `snapshot.install.after` — replication
//!   snapshot install and PITR restore publication.
//! - `index.publish.before` / `index.publish.after` — secondary-index
//!   checkpoint/generation publication.
//!
//! Tablet split and merge (instrumented in `mongreldb-cluster`):
//!
//! - `tablet.split.before` / `tablet.split.after` — the atomic routing
//!   publication of a split (children `Active` + source `Retiring` in one
//!   meta command; spec section 12.5 step 8).
//! - `tablet.split.phase.1` … `tablet.split.phase.7` — fired after each
//!   split phase's progress record is durable (`split.json`), in
//!   `SplitPhase` declaration order; crash-resume tests arm these.
//! - `tablet.merge.before` / `tablet.merge.after` — the atomic routing
//!   publication of a merge (replacement `Active` + sources `Retiring`;
//!   spec section 12.6).
//! - `tablet.merge.phase.1` … `tablet.merge.phase.7` — fired after each
//!   merge phase's progress record is durable (`merge.json`), in
//!   `MergePhase` declaration order.
//!
//! Cluster backup (instrumented in `mongreldb-cluster`):
//!
//! - `cluster.backup.before` / `cluster.backup.after` — bracket a full run.
//! - `cluster.backup.pin` — after the meta version is pinned.
//! - `cluster.backup.tablet` — after each tablet snapshot is written.
//! - `cluster.backup.validate` — after full validation, before publish.
//! - `cluster.backup.publish.before` / `cluster.backup.publish.after` —
//!   bracket atomic manifest publication (spec §12.12 step 6: publish last).
//!
//! KMS root-key rotation (instrumented in `mongreldb-core`):
//!
//! - `kms.rotation.phase.1` … `kms.rotation.phase.7` — fired after each
//!   rotation phase is durable, in `KeyRotationPhase` declaration order;
//!   crash-resume tests arm these.
//!
//! Transaction prepare and decision (instrumented in `mongreldb-core` single-
//! node commit and `mongreldb-cluster` distributed 2PC):
//!
//! - `txn.prepare.before` / `txn.prepare.after` — bracket entry into the
//!   prepare state (single-node `Preparing`, or durable participant prepare
//!   in 2PC). `before` aborts before prepare becomes public/durable;
//!   `after` fires once prepare has been entered.
//! - `txn.decision.before` / `txn.decision.after` — bracket the durable
//!   commit/abort decision (single-node enter `CommitCritical` → published
//!   `Committed` receipt, or 2PC coordinator `Commit`/`Abort` proposal).
//!   `before` aborts before the decision can become durable; `after` fires
//!   only after the decision is durable and must not undo it.
//!
//! # Test coordination
//!
//! - [`Action::Barrier`] records one arrival at a named barrier per hook hit;
//!   [`wait_barrier`] blocks the test thread until the expected number of
//!   arrivals is observed (or a timeout expires).
//! - [`ScopedGuard`] arms a hook for the duration of a scope and clears the
//!   whole registry on drop, even while unwinding.
//! - [`activate_limited`] arms a hook that fires a bounded number of times
//!   and then passes through.
//! - [`hits`] reports how often an armed hook was evaluated, for direct
//!   test assertions.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// The failure surface returned by an injected hook.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Fault {
    /// A configured fault fired at the named hook.
    #[error("injected fault at `{0}`")]
    Injected(&'static str),
}

/// The failure surface of a [`wait_barrier`] call that did not observe the
/// expected arrivals in time.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BarrierError {
    /// The barrier did not reach the expected arrival count before the
    /// timeout expired.
    #[error(
        "timed out after {timeout:?} waiting for {expected} arrivals at barrier `{name}` ({arrived} arrived)"
    )]
    Timeout {
        /// The barrier that timed out.
        name: &'static str,
        /// The arrival count the caller was waiting for.
        expected: usize,
        /// The arrival count observed when the timeout expired.
        arrived: usize,
        /// The timeout that expired.
        timeout: Duration,
    },
}

/// Barrier coordination for a hook: each hit records one arrival at the
/// named barrier and returns `Ok(())`. Test threads observe the arrivals
/// with [`wait_barrier`]. The firing thread never blocks; use
/// [`Action::Callback`] when a test must pause the firing thread.
#[derive(Debug, Clone)]
pub struct BarrierAction {
    name: &'static str,
}

impl BarrierAction {
    /// Signal the named barrier on every hit of the armed hook.
    pub fn new(name: &'static str) -> Self {
        Self { name }
    }

    /// The barrier this action signals.
    pub fn name(&self) -> &'static str {
        self.name
    }
}

/// What happens when an armed hook is hit.
#[derive(Clone)]
pub enum Action {
    /// Return [`Fault::Injected`] to the caller.
    Fail,
    /// Panic (useful for crash-style tests that catch unwinding).
    Panic,
    /// Sleep for a fixed duration (prefer barriers in new tests).
    Sleep(Duration),
    /// Invoke an arbitrary observer with the hook name, then return `Ok(())`.
    Callback(Arc<dyn Fn(&'static str) + Send + Sync>),
    /// Record one arrival at a named barrier, then return `Ok(())`.
    Barrier(BarrierAction),
}

impl std::fmt::Debug for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Action::Fail => f.write_str("Fail"),
            Action::Panic => f.write_str("Panic"),
            Action::Sleep(duration) => f.debug_tuple("Sleep").field(duration).finish(),
            Action::Callback(_) => f.write_str("Callback(..)"),
            Action::Barrier(action) => f.debug_tuple("Barrier").field(action).finish(),
        }
    }
}

struct HookState {
    action: Action,
    /// Remaining firings before the hook passes through (`None` = unlimited).
    remaining: Option<u64>,
    /// Evaluations since the hook was last (re)armed, including pass-throughs
    /// after the firing budget was exhausted.
    hits: u64,
}

#[derive(Default)]
struct BarrierState {
    arrivals: Mutex<usize>,
    released: Condvar,
}

#[derive(Default)]
struct Registry {
    armed: AtomicBool,
    hooks: Mutex<HashMap<&'static str, HookState>>,
    barriers: Mutex<HashMap<&'static str, Arc<BarrierState>>>,
}

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(Registry::default)
}

/// Evaluates the named hook. Returns `Ok(())` when no action is configured
/// or the firing budget armed by [`activate_limited`] is exhausted.
///
/// Call sites propagate the error, e.g. `mongreldb_fault::inject("wal.fsync.before")?`.
pub fn inject(name: &'static str) -> Result<(), Fault> {
    let registry = registry();
    if !registry.armed.load(Ordering::Acquire) {
        return Ok(());
    }
    let action = {
        let mut hooks = registry.hooks.lock().expect("fault registry");
        let Some(hook) = hooks.get_mut(name) else {
            return Ok(());
        };
        hook.hits += 1;
        if hook.remaining == Some(0) {
            return Ok(());
        }
        if let Some(remaining) = &mut hook.remaining {
            *remaining -= 1;
        }
        hook.action.clone()
    };
    match action {
        Action::Fail => Err(Fault::Injected(name)),
        Action::Panic => panic!("injected fault at `{name}`"),
        Action::Sleep(duration) => {
            std::thread::sleep(duration);
            Ok(())
        }
        Action::Callback(callback) => {
            callback(name);
            Ok(())
        }
        Action::Barrier(barrier) => {
            arrive(registry, barrier.name());
            Ok(())
        }
    }
}

/// Arms one hook with an action. Re-arming a hook resets its hit count.
pub fn activate(name: &'static str, action: Action) {
    activate_inner(name, action, None);
}

/// Arms one hook with an action that fires at most `limit` times; later
/// evaluations pass through with `Ok(())` and still count as hits.
pub fn activate_limited(name: &'static str, action: Action, limit: u64) {
    activate_inner(name, action, Some(limit));
}

fn activate_inner(name: &'static str, action: Action, remaining: Option<u64>) {
    let registry = registry();
    registry.hooks.lock().expect("fault registry").insert(
        name,
        HookState {
            action,
            remaining,
            hits: 0,
        },
    );
    registry.armed.store(true, Ordering::Release);
}

/// Disarms one hook and forgets its hit count.
pub fn deactivate(name: &'static str) {
    let registry = registry();
    let mut hooks = registry.hooks.lock().expect("fault registry");
    hooks.remove(name);
    if hooks.is_empty() {
        registry.armed.store(false, Ordering::Release);
    }
}

/// Disarms every hook and resets every hit count and barrier. A barrier wait
/// already in flight is not released; it keeps waiting on its own timeout.
pub fn clear() {
    let registry = registry();
    registry.hooks.lock().expect("fault registry").clear();
    registry.barriers.lock().expect("fault registry").clear();
    registry.armed.store(false, Ordering::Release);
}

/// Number of times the named hook was evaluated since it was last (re)armed,
/// including pass-throughs after an [`activate_limited`] budget ran out.
/// Counts reset on re-arm, [`deactivate`], and [`clear`]; an unknown hook
/// reports zero.
pub fn hits(name: &'static str) -> u64 {
    registry()
        .hooks
        .lock()
        .expect("fault registry")
        .get(name)
        .map_or(0, |hook| hook.hits)
}

/// Blocks until the named barrier records `expected_arrivals` hook hits, or
/// the timeout expires. Arrivals are cumulative since the last [`clear`], so
/// a wait started after the hits landed returns immediately.
pub fn wait_barrier(
    name: &'static str,
    expected_arrivals: usize,
    timeout: Duration,
) -> Result<(), BarrierError> {
    let barrier = barrier_state(name);
    let deadline = Instant::now() + timeout;
    let mut arrivals = barrier.arrivals.lock().expect("fault barrier");
    while *arrivals < expected_arrivals {
        let now = Instant::now();
        if now >= deadline {
            return Err(BarrierError::Timeout {
                name,
                expected: expected_arrivals,
                arrived: *arrivals,
                timeout,
            });
        }
        let (guard, elapsed) = barrier
            .released
            .wait_timeout(arrivals, deadline - now)
            .expect("fault barrier");
        arrivals = guard;
        if elapsed.timed_out() && *arrivals < expected_arrivals {
            return Err(BarrierError::Timeout {
                name,
                expected: expected_arrivals,
                arrived: *arrivals,
                timeout,
            });
        }
    }
    Ok(())
}

fn barrier_state(name: &'static str) -> Arc<BarrierState> {
    registry()
        .barriers
        .lock()
        .expect("fault registry")
        .entry(name)
        .or_default()
        .clone()
}

fn arrive(registry: &Registry, name: &'static str) {
    let barrier = {
        let mut barriers = registry.barriers.lock().expect("fault registry");
        barriers.entry(name).or_default().clone()
    };
    let mut arrivals = barrier.arrivals.lock().expect("fault barrier");
    *arrivals += 1;
    barrier.released.notify_all();
}

/// RAII test guard: arms one hook on construction and calls [`clear`] on
/// drop. Dropping runs during panic unwinding, so an injected [`Action::Panic`]
/// cannot leak an armed hook into the next test.
#[must_use]
pub struct ScopedGuard {
    name: &'static str,
}

impl ScopedGuard {
    /// Arm `name` with `action` until the guard is dropped.
    pub fn new(name: &'static str, action: Action) -> Self {
        activate(name, action);
        Self { name }
    }

    /// Arm `name` with a count-limited action (see [`activate_limited`])
    /// until the guard is dropped.
    pub fn limited(name: &'static str, action: Action, limit: u64) -> Self {
        activate_limited(name, action, limit);
        Self { name }
    }

    /// The hook this guard armed.
    pub fn name(&self) -> &'static str {
        self.name
    }
}

impl std::fmt::Debug for ScopedGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopedGuard")
            .field("name", &self.name)
            .finish()
    }
}

impl Drop for ScopedGuard {
    fn drop(&mut self) {
        clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The registry is process-global; these tests must run serialized so
    // concurrent `clear()` calls do not disarm another test's hook.
    #[test]
    fn registry_lifecycle() {
        assert!(inject("test.inactive").is_ok());

        activate("test.fail", Action::Fail);
        assert_eq!(inject("test.fail"), Err(Fault::Injected("test.fail")));
        deactivate("test.fail");
        assert!(inject("test.fail").is_ok());

        activate("test.clear", Action::Fail);
        clear();
        assert!(inject("test.clear").is_ok());
    }
}
