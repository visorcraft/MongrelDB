//! Key and predicate lock manager with deadlock detection (spec section 10.2,
//! S1B-003). Implemented in the Stage 1 wave.
//!
//! This wave builds the manager and its tests as a self-contained unit.
//! Wiring lock acquisition into the transaction, constraint, sequence, and DDL
//! paths is a documented follow-up owned by the next Stage 1B wave; nothing
//! outside this module calls into it yet.
//!
//! ## Modes
//!
//! Two modes, [`LockMode::Shared`] and [`LockMode::Exclusive`], cover every
//! use case spec section 10.2 lists: `SELECT ... FOR UPDATE` takes row locks
//! in Exclusive mode, unique-constraint probes and global sequence allocation
//! take Exclusive key/barrier locks, foreign-key parent protection takes a
//! Shared lock on the parent row (blocking a concurrent Exclusive delete or
//! key update of the parent), and DDL takes the schema barrier in Exclusive
//! mode while DML transactions hold it in Shared mode. A third Update mode is
//! deliberately omitted: with only Shared/Exclusive the classic read-then-write
//! conversion is a Shared → Exclusive upgrade, and the conversion deadlocks an
//! Update mode exists to prevent are instead resolved deterministically by the
//! deadlock detector below.
//!
//! ## Wait queues
//!
//! Per spec section 10.2, each queued [`Waiter`] carries exactly four things:
//! the transaction ID, the deadline, the cancellation control, and the lock
//! mode. Victim-selection priority and wait outcomes are bookkeeping and live
//! in side tables ([`Inner::priorities`], [`Inner::fates`]) so the queue shape
//! matches the spec verbatim.
//!
//! Transaction IDs are `u64` today: core allocates them monotonically
//! (`txn::allocate_txn_id`), so the numerically largest ID is the youngest
//! transaction. The later migration to the 128-bit
//! [`mongreldb_types::ids::TransactionId`] (spec section 7) swaps the ID type;
//! because random 128-bit IDs do not encode age, that migration must also give
//! victim selection a separate age source.
//!
//! ## Fairness
//!
//! Grants are strict FIFO per key where modes allow: a waiter is granted only
//! when every waiter ahead of it has been granted and its mode is compatible
//! with every current holder. A reader arriving behind a queued writer never
//! barges ahead of it, so a writer waits for at most one bounded window: the
//! holder generation present when it enqueued plus the readers already queued
//! ahead of it. Readers arriving later queue behind the writer and cannot
//! extend its wait.
//!
//! ## Deadlock detection and victim selection
//!
//! The wait-for graph is rebuilt from queue state on every enqueue and on
//! every grant (spec section 10.2). A waiter has an edge to every incompatible
//! holder of its key and to every incompatible waiter ahead of it; a
//! transaction never has an edge to itself. Cycle search iterates nodes and
//! edges in ascending ID order, so the cycle found for a given state is
//! deterministic. The victim is chosen deterministically: lowest priority
//! first (priority is optional; all transactions default to
//! [`DEFAULT_PRIORITY`], in which case the rule collapses to the spec's
//! default), then the youngest transaction (largest `u64` ID). The victim's
//! blocked `acquire` returns [`LockError::Deadlock`]; everyone else in the
//! cycle keeps waiting and proceeds once the victim aborts and releases.
//!
//! ## Error mapping
//!
//! [`From<LockError> for MongrelError`] maps a deadlock onto the dedicated
//! `MongrelError::Deadlock` variant (victim and wait-for cycle preserved), so
//! the precise taxonomy category of spec section 9.7 —
//! [`ErrorCategory::Deadlock`] — survives the bridge, alongside the same
//! retry-the-whole-transaction discipline as `MongrelError::Conflict`
//! ([`ErrorCategory::retry_class`]). Deadline and cancellation map onto the
//! matching `MongrelError` variants; [`LockError::category`] exposes the
//! taxonomy category of every failure directly.
//!
//! ## Blocking waits
//!
//! Thread-safe and synchronous only: one `parking_lot` mutex plus one
//! condition variable. Waiters sleep in `wait_timeout` for the smaller of
//! their remaining deadline and [`CANCELLATION_POLL_INTERVAL`], so
//! cancellation through [`ExecutionControl`] (which offers no synchronous
//! wait handle) is observed within that interval and deadlines within timer
//! resolution.
//!
//! ## Usage contract
//!
//! A transaction has at most one `acquire` in flight at a time (its statements
//! execute sequentially), and calls [`LockManager::release_all`] exactly once
//! when the transaction ends — including when it is a deadlock victim, whose
//! held locks are *not* released implicitly. Re-acquisition is re-entrant:
//! requesting a mode already covered by the transaction's hold is a no-op, and
//! a sole holder's Shared → Exclusive upgrade succeeds immediately. Re-issued
//! acquisitions therefore can never deadlock a transaction against itself.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::Bound;
use std::time::{Duration, Instant};

use mongreldb_types::errors::ErrorCategory;
use parking_lot::{Condvar, Mutex, MutexGuard};

use crate::{ExecutionControl, MongrelError, RowId};

/// How often a blocked waiter wakes to re-check cancellation when neither a
/// notify nor its deadline comes first. This bounds the latency between an
/// [`ExecutionControl::cancel`] and the waiter's [`LockError::Cancelled`].
const CANCELLATION_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Priority of a transaction that never stated one. Choosing zero as the
/// default makes "lowest priority, then youngest" collapse to plain
/// "youngest" — the spec's default rule — whenever no transaction in a cycle
/// carries an explicit priority.
const DEFAULT_PRIORITY: u64 = 0;

/// The mode of a lock acquisition or hold.
///
/// Shared holds coexist with other Shared holds; Exclusive excludes
/// everything. See the module docs for why there is deliberately no Update
/// mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockMode {
    /// Coexists with other Shared holds (foreign-key parent protection, DML
    /// transactions under the schema barrier).
    Shared,
    /// Excludes every other mode (`SELECT ... FOR UPDATE` row locks,
    /// unique-constraint probes, sequence allocation, DDL barriers).
    Exclusive,
}

impl LockMode {
    /// Whether two modes can be held on the same key at the same time.
    pub const fn compatible(self, other: Self) -> bool {
        matches!((self, other), (Self::Shared, Self::Shared))
    }
}

/// A typed, lockable key.
///
/// The families cover the three key spaces of spec section 10.2: row keys
/// (physical `RowId` or primary-key bytes), predicate/range keys, and named
/// barriers for schema/DDL and sequence allocation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LockKey {
    /// One physical row of a table (`SELECT ... FOR UPDATE`).
    Row { table_id: u64, row_id: RowId },
    /// One primary-key or unique-constraint key of a table. Callers with
    /// several key spaces per table (multiple unique indexes) MUST fold the
    /// index identity into `key`, since the manager compares bytes only.
    Key { table_id: u64, key: Vec<u8> },
    /// One predicate/range over a table's key space (serializable predicate
    /// protection).
    Range {
        table_id: u64,
        low: Bound<Vec<u8>>,
        high: Bound<Vec<u8>>,
    },
    /// A named barrier (schema/DDL, sequence allocation). Names are compared
    /// as strings; constructors below define the engine's namespaces.
    Barrier { name: String },
}

impl LockKey {
    /// A physical-row key (spec section 10.2 `SELECT ... FOR UPDATE`).
    pub fn row(table_id: u64, row_id: RowId) -> Self {
        Self::Row { table_id, row_id }
    }

    /// A primary-key or unique-constraint key from encoded key bytes.
    pub fn key(table_id: u64, key: impl Into<Vec<u8>>) -> Self {
        Self::Key {
            table_id,
            key: key.into(),
        }
    }

    /// A predicate/range key over a table's key space.
    pub fn range(table_id: u64, low: Bound<Vec<u8>>, high: Bound<Vec<u8>>) -> Self {
        Self::Range {
            table_id,
            low,
            high,
        }
    }

    /// A named barrier.
    pub fn barrier(name: impl Into<String>) -> Self {
        Self::Barrier { name: name.into() }
    }

    /// The schema/DDL barrier: DDL takes it in Exclusive mode, DML
    /// transactions in Shared mode, so schema changes exclude concurrent
    /// DML and one another.
    pub fn schema_barrier() -> Self {
        Self::barrier("schema")
    }

    /// The barrier guarding allocation of one named sequence; always taken
    /// in Exclusive mode so concurrent allocations serialize.
    pub fn sequence_barrier(sequence: &str) -> Self {
        Self::barrier(format!("sequence:{sequence}"))
    }
}

/// One lock acquisition request.
///
/// The four fields the spec's wait queue carries — transaction ID, deadline,
/// cancellation control, lock mode — are all here; `priority` is
/// victim-selection input and never enters the queue itself.
#[derive(Debug, Clone)]
pub struct LockRequest {
    /// The requesting transaction. `u64` today; see the module docs for the
    /// 128-bit [`mongreldb_types::ids::TransactionId`] migration note.
    pub txn_id: u64,
    /// The requested mode.
    pub mode: LockMode,
    /// Optional lock-wait deadline, independent of any deadline inside
    /// `control`. Expiry fails the wait with [`LockError::DeadlineExceeded`].
    pub deadline: Option<Instant>,
    /// Cooperative cancellation control; cancelling it fails the wait with
    /// [`LockError::Cancelled`] (or [`LockError::DeadlineExceeded`] when the
    /// control's own deadline fired first), observed within
    /// [`CANCELLATION_POLL_INTERVAL`].
    pub control: ExecutionControl,
    /// Optional deadlock-victim priority: within one wait-for cycle the
    /// lowest priority value dies first, ties break by youngest transaction.
    /// `None` is [`DEFAULT_PRIORITY`].
    pub priority: Option<u64>,
}

impl LockRequest {
    /// A request with no lock-wait deadline and no explicit priority.
    pub fn new(txn_id: u64, mode: LockMode, control: ExecutionControl) -> Self {
        Self {
            txn_id,
            mode,
            deadline: None,
            control,
            priority: None,
        }
    }

    /// Sets the lock-wait deadline.
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Sets the lock-wait deadline relative to now. Overflowing timeouts
    /// collapse to an already-expired deadline (fail closed), matching
    /// [`ExecutionControl::with_timeout`].
    pub fn with_timeout(self, timeout: Duration) -> Self {
        let now = Instant::now();
        self.with_deadline(now.checked_add(timeout).unwrap_or(now))
    }

    /// Sets the deadlock-victim priority (lower values die first).
    pub fn with_priority(mut self, priority: u64) -> Self {
        self.priority = Some(priority);
        self
    }
}

/// A failed lock acquisition.
///
/// The taxonomy category of each variant is available through
/// [`Self::category`]; conversion to [`MongrelError`] maps onto the closest
/// existing variant (see the module docs for the deadlock mapping).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LockError {
    /// The transaction was chosen as the deterministic deadlock victim; retry
    /// the whole transaction.
    #[error(
        "deadlock: transaction {victim} was chosen as the deadlock victim (wait-for cycle {cycle}); retry the whole transaction"
    )]
    Deadlock {
        /// The doomed transaction (always the receiver of this error).
        victim: u64,
        /// The wait-for cycle, `a → b → …`, each waiting on the next.
        cycle: String,
    },
    /// The lock-wait deadline (or the control's own deadline) expired.
    #[error("lock wait deadline exceeded")]
    DeadlineExceeded,
    /// The wait was cancelled through its [`ExecutionControl`].
    #[error("lock wait cancelled")]
    Cancelled,
    /// The request violates the usage contract (e.g. a second in-flight wait
    /// for the same transaction on the same key).
    #[error("invalid lock request: {0}")]
    InvalidRequest(String),
}

impl LockError {
    /// The stable cross-language taxonomy category of this failure (spec
    /// section 9.7); identical to the category of the [`MongrelError`]
    /// produced by the bridge below.
    pub fn category(&self) -> ErrorCategory {
        match self {
            Self::Deadlock { .. } => ErrorCategory::Deadlock,
            Self::DeadlineExceeded => ErrorCategory::DeadlineExceeded,
            Self::Cancelled => ErrorCategory::Cancelled,
            // Mirrors the MongrelError::InvalidArgument bridge.
            Self::InvalidRequest(_) => ErrorCategory::ClusterVersionMismatch,
        }
    }
}

impl From<LockError> for MongrelError {
    /// Maps onto the matching [`MongrelError`] variant. A deadlock becomes
    /// `MongrelError::Deadlock` — victim and cycle preserved — so callers see
    /// the precise [`ErrorCategory::Deadlock`] with the same
    /// retry-the-whole-transaction discipline as `Conflict`.
    fn from(error: LockError) -> Self {
        match error {
            LockError::Deadlock { victim, cycle } => MongrelError::Deadlock { victim, cycle },
            LockError::DeadlineExceeded => MongrelError::DeadlineExceeded,
            LockError::Cancelled => MongrelError::Cancelled,
            LockError::InvalidRequest(message) => MongrelError::InvalidArgument(message),
        }
    }
}

/// One granted hold.
#[derive(Debug, Clone, Copy)]
struct Holder {
    txn_id: u64,
    mode: LockMode,
}

/// One queued acquisition. Carries exactly the four fields spec section 10.2
/// lists: transaction ID, deadline, cancellation control, and lock mode.
#[derive(Debug)]
struct Waiter {
    txn_id: u64,
    deadline: Option<Instant>,
    control: ExecutionControl,
    mode: LockMode,
}

/// Per-key state: granted holds plus the strict-FIFO wait queue.
#[derive(Debug, Default)]
struct LockState {
    holders: Vec<Holder>,
    queue: VecDeque<Waiter>,
}

/// Why a waiter's entry left its queue without a grant; consumed by the
/// blocked thread, which returns the matching [`LockError`].
#[derive(Debug)]
enum Fate {
    Deadlock { victim: u64, cycle: String },
    DeadlineExceeded,
    Cancelled,
}

impl Fate {
    fn into_error(self) -> LockError {
        match self {
            Self::Deadlock { victim, cycle } => LockError::Deadlock { victim, cycle },
            Self::DeadlineExceeded => LockError::DeadlineExceeded,
            Self::Cancelled => LockError::Cancelled,
        }
    }
}

#[derive(Debug, Default)]
struct Inner {
    locks: HashMap<LockKey, LockState>,
    /// Wait outcomes for entries already removed from a queue (deadlock
    /// victims, waiters purged by a grant pass). Keyed by transaction: a
    /// transaction waits on at most one key at a time.
    fates: HashMap<u64, Fate>,
    /// Latest stated deadlock-victim priority per transaction; registered on
    /// every acquire so holders that later join a cycle are covered too.
    priorities: HashMap<u64, u64>,
}

/// The key and predicate lock manager. Cheap to share: all state lives
/// behind one mutex and one condition variable.
#[derive(Debug)]
pub struct LockManager {
    inner: Mutex<Inner>,
    wake: Condvar,
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            wake: Condvar::new(),
        }
    }

    /// Acquires `key` for `request.txn_id`, blocking until granted, doomed,
    /// cancelled, or past the deadline. See the module docs for the
    /// re-entrancy, upgrade, fairness, and deadlock rules.
    pub fn acquire(&self, key: LockKey, request: LockRequest) -> Result<(), LockError> {
        let LockRequest {
            txn_id,
            mode,
            deadline,
            control,
            priority,
        } = request;

        let mut inner = self.inner.lock();
        check_request_live(&control, deadline)?;
        if let Some(priority) = priority {
            inner.priorities.insert(txn_id, priority);
        }

        {
            let state = inner.locks.entry(key.clone()).or_default();
            match state.holders.iter().position(|h| h.txn_id == txn_id) {
                Some(position) => {
                    let held = state.holders[position].mode;
                    if held == LockMode::Exclusive || mode == LockMode::Shared {
                        // Re-entrant no-op: an Exclusive hold covers any
                        // re-request, a Shared hold covers a Shared one. A
                        // transaction never deadlocks against itself here.
                        return Ok(());
                    }
                    if state.holders.len() == 1 && state.queue.is_empty() {
                        // Sole-holder Shared → Exclusive upgrade is immediate.
                        state.holders[position].mode = LockMode::Exclusive;
                        return Ok(());
                    }
                    // Otherwise the upgrade queues like any other request;
                    // the grant pass completes it once only its own Shared
                    // hold remains.
                }
                None => {
                    let blocked = !state.queue.is_empty()
                        || state.holders.iter().any(|h| !mode.compatible(h.mode));
                    if !blocked {
                        state.holders.push(Holder { txn_id, mode });
                        return Ok(());
                    }
                }
            }
            if state.queue.iter().any(|w| w.txn_id == txn_id) {
                return Err(LockError::InvalidRequest(format!(
                    "transaction {txn_id} already has a pending wait on {key:?}"
                )));
            }
            state.queue.push_back(Waiter {
                txn_id,
                deadline,
                control: control.clone(),
                mode,
            });
        }

        // Spec section 10.2: check the wait-for graph on enqueue. If this
        // requester is the victim its fate is already recorded and the first
        // loop iteration below returns it synchronously.
        detect_deadlocks(&mut inner);
        self.wake.notify_all();

        let result = self.wait_for_grant(&mut inner, &key, txn_id, &control, deadline);
        if result.is_err() {
            // A departed waiter may unblock the queue behind it.
            process_key(&mut inner, &key);
            detect_deadlocks(&mut inner);
            self.wake.notify_all();
        }
        prune_if_idle(&mut inner, &key);
        result
    }

    /// Releases every hold `txn_id` has on `key`, granting queued waiters.
    /// Releasing a key the transaction does not hold is a no-op.
    pub fn release(&self, txn_id: u64, key: &LockKey) {
        let mut inner = self.inner.lock();
        if inner
            .locks
            .get(key)
            .is_some_and(|s| s.holders.iter().any(|h| h.txn_id == txn_id))
        {
            if let Some(state) = inner.locks.get_mut(key) {
                state.holders.retain(|h| h.txn_id != txn_id);
            }
            process_key(&mut inner, key);
            // Spec section 10.2: check the wait-for graph on grant.
            detect_deadlocks(&mut inner);
            prune_if_idle(&mut inner, key);
        }
        drop(inner);
        self.wake.notify_all();
    }

    /// Releases every hold and pending wait of `txn_id`; called exactly once
    /// when the transaction ends, including on abort as a deadlock victim.
    pub fn release_all(&self, txn_id: u64) {
        let mut inner = self.inner.lock();
        inner.fates.remove(&txn_id);
        inner.priorities.remove(&txn_id);
        let mut affected = Vec::new();
        for (key, state) in inner.locks.iter_mut() {
            let touched = state.holders.iter().any(|h| h.txn_id == txn_id)
                || state.queue.iter().any(|w| w.txn_id == txn_id);
            if touched {
                state.holders.retain(|h| h.txn_id != txn_id);
                state.queue.retain(|w| w.txn_id != txn_id);
                affected.push(key.clone());
            }
        }
        for key in &affected {
            process_key(&mut inner, key);
        }
        detect_deadlocks(&mut inner);
        inner
            .locks
            .retain(|_, state| !(state.holders.is_empty() && state.queue.is_empty()));
        drop(inner);
        self.wake.notify_all();
    }

    /// Whether `txn_id` currently holds any mode on `key`.
    pub fn holds(&self, txn_id: u64, key: &LockKey) -> bool {
        self.inner
            .lock()
            .locks
            .get(key)
            .is_some_and(|state| state.holders.iter().any(|h| h.txn_id == txn_id))
    }

    /// Number of queued waiters on `key`; test-only introspection used to
    /// sequence multi-threaded tests deterministically.
    #[cfg(test)]
    fn queued_waiters(&self, key: &LockKey) -> usize {
        self.inner
            .lock()
            .locks
            .get(key)
            .map_or(0, |state| state.queue.len())
    }

    /// Blocks until this waiter is granted or leaves the queue with an error.
    /// The queue invariant: an entry leaves only by grant (it becomes a
    /// holder) or by fate (an error recorded in `fates`).
    fn wait_for_grant(
        &self,
        inner: &mut MutexGuard<'_, Inner>,
        key: &LockKey,
        txn_id: u64,
        control: &ExecutionControl,
        deadline: Option<Instant>,
    ) -> Result<(), LockError> {
        loop {
            if let Some(fate) = inner.fates.remove(&txn_id) {
                return Err(fate.into_error());
            }
            let (queued, held) = match inner.locks.get(key) {
                Some(state) => (
                    state.queue.iter().any(|w| w.txn_id == txn_id),
                    state.holders.iter().any(|h| h.txn_id == txn_id),
                ),
                None => (false, false),
            };
            if !queued {
                if held {
                    return Ok(());
                }
                // The entry vanished without a grant and without a fate: only
                // a release_all racing this wait (a usage-contract violation)
                // can do that. Fail closed instead of hanging.
                return Err(LockError::Cancelled);
            }
            if let Err(error) = check_request_live(control, deadline) {
                remove_waiter(inner, key, txn_id);
                return Err(error);
            }
            // Wake at the earlier of the lock-wait deadline and the next
            // cancellation poll.
            let wake_at = deadline
                .unwrap_or_else(|| Instant::now() + CANCELLATION_POLL_INTERVAL)
                .min(Instant::now() + CANCELLATION_POLL_INTERVAL);
            let _ = self.wake.wait_until(inner, wake_at);
        }
    }
}

/// Fails a request whose cancellation already fired or whose lock-wait
/// deadline already passed. [`ExecutionControl::checkpoint`] self-cancels an
/// expired control with the Deadline reason, so the control's own deadline is
/// honored here too.
fn check_request_live(
    control: &ExecutionControl,
    deadline: Option<Instant>,
) -> Result<(), LockError> {
    match control.checkpoint() {
        Ok(()) => {}
        Err(MongrelError::DeadlineExceeded) => return Err(LockError::DeadlineExceeded),
        Err(_) => return Err(LockError::Cancelled),
    }
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(LockError::DeadlineExceeded);
    }
    Ok(())
}

fn remove_waiter(inner: &mut Inner, key: &LockKey, txn_id: u64) {
    if let Some(state) = inner.locks.get_mut(key) {
        state.queue.retain(|w| w.txn_id != txn_id);
    }
}

fn prune_if_idle(inner: &mut Inner, key: &LockKey) {
    if inner
        .locks
        .get(key)
        .is_some_and(|state| state.holders.is_empty() && state.queue.is_empty())
    {
        inner.locks.remove(key);
    }
}

/// Purges waiters that can never proceed (cancelled or past deadline) and
/// grants the maximal compatible FIFO prefix of the queue. Granting stops at
/// the first waiter whose mode conflicts with any holder — no barging, no
/// skipping.
fn process_key(inner: &mut Inner, key: &LockKey) {
    let now = Instant::now();
    let dead: Vec<u64> = match inner.locks.get(key) {
        Some(state) => state
            .queue
            .iter()
            .filter(|w| w.control.is_cancelled() || w.deadline.is_some_and(|d| now >= d))
            .map(|w| w.txn_id)
            .collect(),
        None => return,
    };
    for txn_id in dead {
        if let Some(state) = inner.locks.get_mut(key) {
            if let Some(position) = state.queue.iter().position(|w| w.txn_id == txn_id) {
                let waiter = state.queue.remove(position).expect("position found above");
                let fate = match waiter.control.checkpoint() {
                    Err(MongrelError::DeadlineExceeded) => Fate::DeadlineExceeded,
                    Err(_) => Fate::Cancelled,
                    // Not cancelled: the lock-wait deadline fired.
                    Ok(()) => Fate::DeadlineExceeded,
                };
                inner.fates.entry(txn_id).or_insert(fate);
            }
        }
    }

    loop {
        let Some(state) = inner.locks.get_mut(key) else {
            return;
        };
        let Some(front) = state.queue.front() else {
            return;
        };
        // The waiter's own existing hold never blocks it: that is what lets a
        // queued Shared → Exclusive upgrade complete once other holds drain.
        let grantable = state
            .holders
            .iter()
            .all(|h| h.txn_id == front.txn_id || front.mode.compatible(h.mode));
        if !grantable {
            return;
        }
        let waiter = state.queue.pop_front().expect("front checked above");
        match state.holders.iter_mut().find(|h| h.txn_id == waiter.txn_id) {
            Some(holder) => holder.mode = LockMode::Exclusive,
            None => state.holders.push(Holder {
                txn_id: waiter.txn_id,
                mode: waiter.mode,
            }),
        }
    }
}

/// Builds the wait-for graph: an edge `waiter → other` means `waiter` cannot
/// be granted until `other` leaves. Targets are incompatible holders of the
/// waiter's key and incompatible waiters ahead of it in the FIFO queue. A
/// transaction never waits on itself (re-entrancy and upgrade rules above),
/// so self-edges are excluded. Nodes and adjacency lists are sorted, making
/// the subsequent cycle search deterministic.
fn build_wait_for_graph(inner: &Inner) -> BTreeMap<u64, Vec<u64>> {
    let mut graph: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for state in inner.locks.values() {
        for (index, waiter) in state.queue.iter().enumerate() {
            let edges = graph.entry(waiter.txn_id).or_default();
            for holder in &state.holders {
                if holder.txn_id != waiter.txn_id && !waiter.mode.compatible(holder.mode) {
                    edges.push(holder.txn_id);
                }
            }
            for ahead in state.queue.iter().take(index) {
                if ahead.txn_id != waiter.txn_id && !waiter.mode.compatible(ahead.mode) {
                    edges.push(ahead.txn_id);
                }
            }
        }
    }
    for edges in graph.values_mut() {
        edges.sort_unstable();
        edges.dedup();
    }
    graph
}

/// Finds one cycle with a deterministic depth-first search: roots and
/// adjacency lists are visited in ascending transaction-ID order, so equal
/// states always yield the same cycle. Returns the cycle members in wait
/// order (each waits on the next, the last on the first).
fn find_cycle(graph: &BTreeMap<u64, Vec<u64>>) -> Option<Vec<u64>> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Mark {
        Visiting,
        Done,
    }

    let mut marks: HashMap<u64, Mark> = HashMap::new();
    let mut stack: Vec<u64> = Vec::new();
    for &start in graph.keys() {
        if marks.contains_key(&start) {
            continue;
        }
        marks.insert(start, Mark::Visiting);
        stack.push(start);
        let mut frames: Vec<(u64, usize)> = vec![(start, 0)];
        while let Some((node, index)) = frames.last().copied() {
            let edges = graph.get(&node).map(Vec::as_slice).unwrap_or(&[]);
            if index < edges.len() {
                frames.last_mut().expect("last copied above").1 += 1;
                let next = edges[index];
                match marks.get(&next) {
                    None => {
                        marks.insert(next, Mark::Visiting);
                        stack.push(next);
                        frames.push((next, 0));
                    }
                    Some(Mark::Visiting) => {
                        let position = stack
                            .iter()
                            .position(|member| *member == next)
                            .expect("a visiting node is on the stack");
                        return Some(stack[position..].to_vec());
                    }
                    Some(Mark::Done) => {}
                }
            } else {
                marks.insert(node, Mark::Done);
                stack.pop();
                frames.pop();
            }
        }
    }
    None
}

/// Deterministic victim selection (spec section 10.2): the lowest priority
/// value dies first — transactions that never stated one share
/// [`DEFAULT_PRIORITY`] — and ties go to the youngest transaction, the largest
/// `u64` ID under core's monotonic allocator.
fn choose_victim(cycle: &[u64], priorities: &HashMap<u64, u64>) -> u64 {
    *cycle
        .iter()
        .min_by(|a, b| {
            let priority_a = priorities.get(a).copied().unwrap_or(DEFAULT_PRIORITY);
            let priority_b = priorities.get(b).copied().unwrap_or(DEFAULT_PRIORITY);
            priority_a.cmp(&priority_b).then_with(|| b.cmp(a))
        })
        .expect("a deadlock cycle is never empty")
}

/// Detects and resolves deadlocks until none remain: each doomed victim is
/// removed from its queue and its [`Fate::Deadlock`] recorded, so every
/// iteration strictly shrinks the waiter set and the loop terminates.
fn detect_deadlocks(inner: &mut Inner) {
    loop {
        let graph = build_wait_for_graph(inner);
        let Some(cycle) = find_cycle(&graph) else {
            return;
        };
        let victim = choose_victim(&cycle, &inner.priorities);
        // Every cycle member waits on the next, so the victim is always a
        // queued waiter of exactly one key.
        let victim_key = inner.locks.iter().find_map(|(key, state)| {
            state
                .queue
                .iter()
                .any(|w| w.txn_id == victim)
                .then(|| key.clone())
        });
        let Some(victim_key) = victim_key else {
            // Defensive: the victim left the queue between graph build and
            // doom. Nothing to kill; re-detect on the next state change.
            return;
        };
        if let Some(state) = inner.locks.get_mut(&victim_key) {
            state.queue.retain(|w| w.txn_id != victim);
        }
        let cycle = cycle
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(" → ");
        inner.fates.insert(victim, Fate::Deadlock { victim, cycle });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CancellationReason;
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::thread;

    use LockMode::{Exclusive, Shared};

    fn manager() -> Arc<LockManager> {
        Arc::new(LockManager::new())
    }

    fn request(txn_id: u64, mode: LockMode) -> LockRequest {
        LockRequest::new(txn_id, mode, ExecutionControl::new(None))
    }

    fn timed_request(txn_id: u64, mode: LockMode, timeout: Duration) -> LockRequest {
        request(txn_id, mode).with_timeout(timeout)
    }

    fn row_key(n: u64) -> LockKey {
        LockKey::row(1, RowId(n))
    }

    /// Runs one acquire on its own thread and delivers the result.
    fn spawn_acquire(
        manager: &Arc<LockManager>,
        key: LockKey,
        request: LockRequest,
    ) -> mpsc::Receiver<Result<(), LockError>> {
        let (sender, receiver) = mpsc::channel();
        let manager = Arc::clone(manager);
        thread::spawn(move || {
            let result = manager.acquire(key, request);
            sender.send(result).expect("receiver alive");
        });
        receiver
    }

    /// Waits until `key` has at least `n` queued waiters, so multi-threaded
    /// tests sequence enqueue order deterministically instead of relying on
    /// sleeps. Setup-only; assertions never depend on this timing.
    fn wait_for_queue(manager: &LockManager, key: &LockKey, n: usize) {
        for _ in 0..200 {
            if manager.queued_waiters(key) >= n {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("queue did not reach {n} waiters on {key:?}");
    }

    #[test]
    fn acquire_and_release_round_trip() {
        let manager = LockManager::new();
        let key = row_key(1);
        manager.acquire(key.clone(), request(1, Exclusive)).unwrap();
        assert!(manager.holds(1, &key));
        manager.release(1, &key);
        assert!(!manager.holds(1, &key));

        manager.acquire(key.clone(), request(2, Shared)).unwrap();
        assert!(manager.holds(2, &key));
        manager.release_all(2);
        assert!(!manager.holds(2, &key));
    }

    #[test]
    fn mode_compatibility_matrix() {
        assert!(Shared.compatible(Shared));
        assert!(!Shared.compatible(Exclusive));
        assert!(!Exclusive.compatible(Shared));
        assert!(!Exclusive.compatible(Exclusive));
    }

    #[test]
    fn shared_locks_coexist() {
        let manager = LockManager::new();
        let key = row_key(7);
        manager.acquire(key.clone(), request(1, Shared)).unwrap();
        manager.acquire(key.clone(), request(2, Shared)).unwrap();
        assert!(manager.holds(1, &key));
        assert!(manager.holds(2, &key));
    }

    #[test]
    fn exclusive_blocks_shared_and_exclusive_until_release() {
        let manager = LockManager::new();
        let key = row_key(8);
        manager.acquire(key.clone(), request(1, Exclusive)).unwrap();

        let shared = manager.acquire(
            key.clone(),
            timed_request(2, Shared, Duration::from_millis(40)),
        );
        assert_eq!(shared, Err(LockError::DeadlineExceeded));
        let exclusive = manager.acquire(
            key.clone(),
            timed_request(3, Exclusive, Duration::from_millis(40)),
        );
        assert_eq!(exclusive, Err(LockError::DeadlineExceeded));

        manager.release(1, &key);
        manager.acquire(key.clone(), request(2, Shared)).unwrap();
        let still_blocked = manager.acquire(
            key.clone(),
            timed_request(3, Exclusive, Duration::from_millis(40)),
        );
        assert_eq!(still_blocked, Err(LockError::DeadlineExceeded));
    }

    #[test]
    fn wait_respects_the_lock_deadline() {
        let manager = LockManager::new();
        let key = row_key(9);
        manager.acquire(key.clone(), request(1, Exclusive)).unwrap();

        let started = Instant::now();
        let result = manager.acquire(key, timed_request(2, Exclusive, Duration::from_millis(80)));
        let elapsed = started.elapsed();
        assert_eq!(result, Err(LockError::DeadlineExceeded));
        assert_eq!(
            result.unwrap_err().category(),
            ErrorCategory::DeadlineExceeded
        );
        assert!(
            elapsed >= Duration::from_millis(70),
            "returned early: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "returned late: {elapsed:?}"
        );
    }

    #[test]
    fn expired_deadline_fails_fast_even_when_unlocked() {
        let manager = LockManager::new();
        let result = manager.acquire(row_key(10), timed_request(1, Exclusive, Duration::ZERO));
        assert_eq!(result, Err(LockError::DeadlineExceeded));
    }

    #[test]
    fn cancellation_wakes_the_waiter_and_clears_its_queue_entry() {
        let manager = manager();
        let key = row_key(11);
        manager.acquire(key.clone(), request(1, Exclusive)).unwrap();

        let control = ExecutionControl::new(None);
        let cancelled_rx = spawn_acquire(
            &manager,
            key.clone(),
            LockRequest::new(2, Exclusive, control.clone()),
        );
        wait_for_queue(&manager, &key, 1);
        control.cancel(CancellationReason::ClientRequest);
        let result = cancelled_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(result, Err(LockError::Cancelled));
        assert!(!manager.holds(2, &key));

        // The cancelled waiter's entry is gone: the next requester is granted
        // as soon as the holder releases.
        let next_rx = spawn_acquire(&manager, key.clone(), request(3, Exclusive));
        wait_for_queue(&manager, &key, 1);
        manager.release(1, &key);
        assert_eq!(
            next_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            Ok(())
        );
    }

    #[test]
    fn cancelled_control_fails_fast_even_when_unlocked() {
        let manager = LockManager::new();
        let control = ExecutionControl::new(None);
        control.cancel(CancellationReason::ClientRequest);
        let result = manager.acquire(row_key(12), LockRequest::new(1, Exclusive, control));
        assert_eq!(result, Err(LockError::Cancelled));
    }

    #[test]
    fn control_deadline_maps_to_deadline_exceeded() {
        let manager = LockManager::new();
        let key = row_key(13);
        manager.acquire(key.clone(), request(1, Exclusive)).unwrap();
        let control = ExecutionControl::with_timeout(Duration::from_millis(50));
        let result = manager.acquire(key, LockRequest::new(2, Exclusive, control));
        assert_eq!(result, Err(LockError::DeadlineExceeded));
    }

    #[test]
    fn fifo_grant_order_blocks_barging_readers() {
        let manager = manager();
        let key = row_key(14);
        manager.acquire(key.clone(), request(1, Shared)).unwrap();

        // The writer queues behind the shared holder.
        let writer_rx = spawn_acquire(&manager, key.clone(), request(2, Exclusive));
        wait_for_queue(&manager, &key, 1);
        // A reader arriving after the writer must not barge ahead of it, even
        // though Shared is compatible with the current holder: this is the
        // bounded writer-wait window (module docs).
        let reader_rx = spawn_acquire(&manager, key.clone(), request(3, Shared));
        wait_for_queue(&manager, &key, 2);

        manager.release(1, &key);
        assert_eq!(
            writer_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            Ok(())
        );
        assert!(
            reader_rx.recv_timeout(Duration::from_millis(150)).is_err(),
            "reader barged ahead of the queued writer"
        );
        manager.release(2, &key);
        assert_eq!(
            reader_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            Ok(())
        );
    }

    #[test]
    fn release_all_grants_blocked_waiters() {
        let manager = manager();
        let key = row_key(24);
        manager.acquire(key.clone(), request(1, Exclusive)).unwrap();
        let waiter_rx = spawn_acquire(&manager, key.clone(), request(2, Exclusive));
        wait_for_queue(&manager, &key, 1);
        manager.release_all(1);
        assert_eq!(
            waiter_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            Ok(())
        );
        assert!(manager.holds(2, &key));
    }

    #[test]
    fn ab_ba_deadlock_kills_requester_when_it_is_youngest() {
        let manager = manager();
        let key_a = row_key(20);
        let key_b = row_key(21);
        manager
            .acquire(key_a.clone(), request(1, Exclusive))
            .unwrap();
        manager
            .acquire(key_b.clone(), request(2, Exclusive))
            .unwrap();

        // t1 blocks on B (held by t2).
        let t1_rx = spawn_acquire(&manager, key_b.clone(), request(1, Exclusive));
        wait_for_queue(&manager, &key_b, 1);
        // t2 closes the cycle and is the youngest transaction, so t2 — the
        // requester — is the deterministic victim and fails synchronously.
        let error = manager
            .acquire(key_a.clone(), request(2, Exclusive))
            .unwrap_err();
        match &error {
            LockError::Deadlock { victim, cycle } => {
                assert_eq!(*victim, 2, "youngest transaction must be the victim");
                assert!(
                    cycle.contains('1') && cycle.contains('2'),
                    "cycle names both: {cycle}"
                );
            }
            other => panic!("expected t2 to be the deadlock victim, got {other:?}"),
        }
        assert_eq!(error.category(), ErrorCategory::Deadlock);

        // The victim aborts: the survivor's wait completes.
        manager.release_all(2);
        assert_eq!(t1_rx.recv_timeout(Duration::from_secs(5)).unwrap(), Ok(()));
        assert!(manager.holds(1, &key_b));
    }

    #[test]
    fn ab_ba_deadlock_kills_youngest_even_when_it_is_not_the_requester() {
        // IDs chosen so the requester closing the cycle is the OLDER
        // transaction: victim selection, not request order, decides who dies.
        let manager = manager();
        let key_a = row_key(22);
        let key_b = row_key(23);
        manager
            .acquire(key_a.clone(), request(100, Exclusive))
            .unwrap();
        manager
            .acquire(key_b.clone(), request(200, Exclusive))
            .unwrap();

        // t200 blocks on A (held by t100), then aborts if doomed.
        let t200 = {
            let manager = Arc::clone(&manager);
            let key_a = key_a.clone();
            thread::spawn(move || {
                let result = manager.acquire(key_a, request(200, Exclusive));
                if result.is_err() {
                    manager.release_all(200);
                }
                result
            })
        };
        wait_for_queue(&manager, &key_a, 1);

        // t100 closes the cycle and SURVIVES: it waits until the victim
        // aborts, then is granted B.
        manager
            .acquire(key_b.clone(), request(100, Exclusive))
            .unwrap();
        assert!(manager.holds(100, &key_b));

        let victim = t200.join().unwrap();
        assert!(
            matches!(victim, Err(LockError::Deadlock { victim: 200, .. })),
            "youngest transaction must be the victim: {victim:?}"
        );
    }

    #[test]
    fn three_transaction_cycle_kills_youngest() {
        let manager = manager();
        let key_a = row_key(30);
        let key_b = row_key(31);
        let key_c = row_key(32);
        manager
            .acquire(key_a.clone(), request(1, Exclusive))
            .unwrap();
        manager
            .acquire(key_b.clone(), request(2, Exclusive))
            .unwrap();
        manager
            .acquire(key_c.clone(), request(3, Exclusive))
            .unwrap();

        // t1 waits on B, t2 waits on C.
        let t1_rx = spawn_acquire(&manager, key_b.clone(), request(1, Exclusive));
        wait_for_queue(&manager, &key_b, 1);
        let t2_rx = spawn_acquire(&manager, key_c.clone(), request(2, Exclusive));
        wait_for_queue(&manager, &key_c, 1);
        // t3 waits on A, closing t1 → t2 → t3 → t1; t3 is youngest and dies.
        let error = manager
            .acquire(key_a.clone(), request(3, Exclusive))
            .unwrap_err();
        assert!(
            matches!(error, LockError::Deadlock { victim: 3, .. }),
            "youngest of the 3-cycle must die: {error:?}"
        );

        // Unwind: t3's abort grants C to t2; t2's abort grants B to t1.
        manager.release_all(3);
        assert_eq!(t2_rx.recv_timeout(Duration::from_secs(5)).unwrap(), Ok(()));
        manager.release_all(2);
        assert_eq!(t1_rx.recv_timeout(Duration::from_secs(5)).unwrap(), Ok(()));
    }

    #[test]
    fn lowest_priority_dies_before_age_is_considered() {
        let manager = manager();
        let key_a = row_key(40);
        let key_b = row_key(41);
        let key_c = row_key(42);
        manager
            .acquire(key_a.clone(), request(1, Exclusive))
            .unwrap();
        manager
            .acquire(key_b.clone(), request(2, Exclusive))
            .unwrap();
        manager
            .acquire(key_c.clone(), request(3, Exclusive))
            .unwrap();

        // Priorities t1 = 10, t2 = 20, t3 = 30: t1 is the victim even though
        // t3 is the youngest transaction and t3's enqueue closes the cycle.
        let t1 = {
            let manager = Arc::clone(&manager);
            let key_b = key_b.clone();
            thread::spawn(move || {
                let result = manager.acquire(key_b, request(1, Exclusive).with_priority(10));
                if result.is_err() {
                    manager.release_all(1);
                }
                result
            })
        };
        wait_for_queue(&manager, &key_b, 1);
        let t2_rx = spawn_acquire(
            &manager,
            key_c.clone(),
            request(2, Exclusive).with_priority(20),
        );
        wait_for_queue(&manager, &key_c, 1);

        // t3 closes the cycle, survives, and is granted A once t1 aborts.
        manager
            .acquire(key_a.clone(), request(3, Exclusive).with_priority(30))
            .unwrap();
        assert!(manager.holds(3, &key_a));

        let victim = t1.join().unwrap();
        assert!(
            matches!(victim, Err(LockError::Deadlock { victim: 1, .. })),
            "lowest priority must die: {victim:?}"
        );

        manager.release_all(3);
        assert_eq!(t2_rx.recv_timeout(Duration::from_secs(5)).unwrap(), Ok(()));
    }

    #[test]
    fn equal_priorities_fall_back_to_youngest() {
        let manager = manager();
        let key_a = row_key(50);
        let key_b = row_key(51);
        manager
            .acquire(key_a.clone(), request(1, Exclusive).with_priority(5))
            .unwrap();
        manager
            .acquire(key_b.clone(), request(2, Exclusive).with_priority(5))
            .unwrap();

        let t1_rx = spawn_acquire(
            &manager,
            key_b.clone(),
            request(1, Exclusive).with_priority(5),
        );
        wait_for_queue(&manager, &key_b, 1);
        let error = manager
            .acquire(key_a.clone(), request(2, Exclusive).with_priority(5))
            .unwrap_err();
        assert!(
            matches!(error, LockError::Deadlock { victim: 2, .. }),
            "equal priorities must fall back to youngest: {error:?}"
        );
        manager.release_all(2);
        assert_eq!(t1_rx.recv_timeout(Duration::from_secs(5)).unwrap(), Ok(()));
    }

    #[test]
    fn same_transaction_requests_never_self_deadlock() {
        let manager = LockManager::new();
        let key = row_key(60);
        // Re-entrant requests are no-ops, so re-issued acquisitions cannot
        // deadlock a transaction against itself.
        manager.acquire(key.clone(), request(1, Exclusive)).unwrap();
        manager.acquire(key.clone(), request(1, Exclusive)).unwrap();
        manager.acquire(key.clone(), request(1, Shared)).unwrap();
        assert!(manager.holds(1, &key));

        // A sole-holder Shared → Exclusive upgrade succeeds immediately and
        // really excludes others afterwards.
        let other = row_key(61);
        manager.acquire(other.clone(), request(2, Shared)).unwrap();
        manager
            .acquire(other.clone(), request(2, Exclusive))
            .unwrap();
        let blocked = manager.acquire(
            other,
            timed_request(3, Exclusive, Duration::from_millis(40)),
        );
        assert_eq!(blocked, Err(LockError::DeadlineExceeded));
    }

    #[test]
    fn detector_kills_a_self_loop_if_one_is_ever_constructed() {
        // build_wait_for_graph never emits self-edges, so no real state can
        // produce a single-transaction cycle; if a self-loop ever appears the
        // detector must still doom the looping transaction instead of hanging.
        let graph = BTreeMap::from([(7_u64, vec![7_u64])]);
        let cycle = find_cycle(&graph).expect("a self loop is a cycle");
        assert_eq!(cycle, vec![7]);
        assert_eq!(choose_victim(&cycle, &HashMap::new()), 7);
    }

    #[test]
    fn shared_to_exclusive_upgrade_deadlock_kills_youngest() {
        let manager = manager();
        let key = row_key(70);
        manager.acquire(key.clone(), request(1, Shared)).unwrap();
        manager.acquire(key.clone(), request(2, Shared)).unwrap();

        // t1 converts S → X and queues behind t2's Shared hold.
        let t1_rx = spawn_acquire(&manager, key.clone(), request(1, Exclusive));
        wait_for_queue(&manager, &key, 1);
        // t2 converts S → X, closing the classic conversion cycle; t2, the
        // youngest, is the victim.
        let error = manager
            .acquire(key.clone(), request(2, Exclusive))
            .unwrap_err();
        assert!(
            matches!(error, LockError::Deadlock { victim: 2, .. }),
            "youngest upgrader must die: {error:?}"
        );

        // t2 aborts; t1's queued upgrade completes over its own Shared hold.
        manager.release_all(2);
        assert_eq!(t1_rx.recv_timeout(Duration::from_secs(5)).unwrap(), Ok(()));
        let blocked = manager.acquire(
            key.clone(),
            timed_request(3, Shared, Duration::from_millis(40)),
        );
        assert_eq!(
            blocked,
            Err(LockError::DeadlineExceeded),
            "t1 must now hold Exclusive"
        );
    }

    #[test]
    fn sequence_barrier_is_exclusive_across_transactions() {
        let manager = LockManager::new();
        let sequence = LockKey::sequence_barrier("order_id");
        manager
            .acquire(sequence.clone(), request(1, Exclusive))
            .unwrap();

        // A concurrent allocation on the same sequence blocks ...
        let blocked = manager.acquire(
            sequence.clone(),
            timed_request(2, Exclusive, Duration::from_millis(40)),
        );
        assert_eq!(blocked, Err(LockError::DeadlineExceeded));
        // ... while a different sequence is unaffected.
        manager
            .acquire(
                LockKey::sequence_barrier("shipment_id"),
                request(2, Exclusive),
            )
            .unwrap();

        manager.release_all(1);
        manager.acquire(sequence, request(2, Exclusive)).unwrap();
    }

    #[test]
    fn ddl_barrier_excludes_dml_and_concurrent_ddl() {
        let manager = LockManager::new();
        let barrier = LockKey::schema_barrier();
        // DDL takes the barrier exclusively.
        manager
            .acquire(barrier.clone(), request(1, Exclusive))
            .unwrap();
        // DML sharing the barrier blocks for the DDL's duration ...
        let dml = manager.acquire(
            barrier.clone(),
            timed_request(2, Shared, Duration::from_millis(40)),
        );
        assert_eq!(dml, Err(LockError::DeadlineExceeded));
        // ... and so does concurrent DDL.
        let ddl = manager.acquire(
            barrier.clone(),
            timed_request(3, Exclusive, Duration::from_millis(40)),
        );
        assert_eq!(ddl, Err(LockError::DeadlineExceeded));

        manager.release_all(1);
        // With no DDL in flight, many DML transactions share the barrier.
        manager
            .acquire(barrier.clone(), request(2, Shared))
            .unwrap();
        manager.acquire(barrier, request(3, Shared)).unwrap();
    }

    #[test]
    fn key_families_do_not_false_share() {
        let manager = LockManager::new();
        manager
            .acquire(LockKey::row(1, RowId(5)), request(1, Exclusive))
            .unwrap();
        // Same table, same numeric identity, different key family: no conflict.
        manager
            .acquire(
                LockKey::key(1, 5_u64.to_be_bytes().to_vec()),
                request(2, Exclusive),
            )
            .unwrap();
        manager
            .acquire(
                LockKey::range(
                    1,
                    Bound::Included(1_u64.to_be_bytes().to_vec()),
                    Bound::Excluded(6_u64.to_be_bytes().to_vec()),
                ),
                request(3, Exclusive),
            )
            .unwrap();
    }

    #[test]
    fn lock_errors_bridge_to_mongrel_error_and_the_taxonomy() {
        let deadlock = LockError::Deadlock {
            victim: 9,
            cycle: "9 → 4 → 9".to_string(),
        };
        assert_eq!(deadlock.category(), ErrorCategory::Deadlock);
        let mapped = MongrelError::from(deadlock);
        assert!(
            matches!(
                &mapped,
                MongrelError::Deadlock { victim: 9, cycle } if cycle == "9 → 4 → 9"
            ),
            "deadlock maps to the dedicated variant, victim and cycle preserved: {mapped:?}"
        );
        assert_eq!(mapped.category(), ErrorCategory::Deadlock);

        assert!(matches!(
            MongrelError::from(LockError::DeadlineExceeded),
            MongrelError::DeadlineExceeded
        ));
        assert!(matches!(
            MongrelError::from(LockError::Cancelled),
            MongrelError::Cancelled
        ));
        assert_eq!(LockError::Cancelled.category(), ErrorCategory::Cancelled);
        assert!(matches!(
            MongrelError::from(LockError::InvalidRequest("x".to_string())),
            MongrelError::InvalidArgument(_)
        ));
    }

    #[test]
    fn wait_for_graph_cycle_search_is_deterministic() {
        // No cycle: a plain wait chain.
        let graph = BTreeMap::from([(1, vec![2]), (2, vec![3])]);
        assert_eq!(find_cycle(&graph), None);

        // 1 → 2 → 3 → 2: the cycle is {2, 3}, found deterministically.
        let graph = BTreeMap::from([(1, vec![2]), (2, vec![3]), (3, vec![2])]);
        let cycle = find_cycle(&graph).expect("cycle");
        assert_eq!(cycle, vec![2, 3]);

        // Victim selection: no priorities → youngest; explicit priorities →
        // lowest value dies; ties fall back to youngest.
        assert_eq!(choose_victim(&cycle, &HashMap::new()), 3);
        assert_eq!(choose_victim(&cycle, &HashMap::from([(2, 5), (3, 1)])), 3);
        assert_eq!(choose_victim(&cycle, &HashMap::from([(2, 1), (3, 5)])), 2);
    }
}
