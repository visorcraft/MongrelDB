//! Cross-table transactions on the shared WAL (spec §8.2, single-applier subset
//! — parallelism arrives in P3).
//!
//! A [`Transaction`] stages puts/deletes keyed by table; [`Transaction::commit`]
//! reserves a commit epoch from the shared authority, appends the staged data
//! records + a `TxnCommit` marker to the shared WAL, group-fsyncs, applies the
//! staging to each table's memtable + indexes at the commit epoch, persists the
//! per-table manifests, and publishes the visible watermark. Rollback (or a
//! dropped transaction) discards the staging and appends nothing durable.
//!
//! ## Stage 1B (spec §10.2)
//!
//! - S1B-001: every transaction carries a formal [`TransactionState`]
//!   (`Active` → `Preparing` → `CommitCritical` → `Committed`/`Aborted`),
//!   its [`IsolationLevel`], its HLC read timestamp, and its read/write/
//!   predicate sets.
//! - S1B-002: `ReadCommitted` re-pins its read snapshot per statement,
//!   `RepeatableRead` keeps the fixed-at-begin snapshot (the historical
//!   `Snapshot` name is a deprecated alias), and `Serializable` adds
//!   SSI-style read/predicate-set certification at commit: when a concurrent
//!   commit invalidated anything the transaction read (a dangerous
//!   structure), the commit aborts with a serialization failure instead of
//!   allowing a non-serializable interleaving.
//! - S1B-005: [`Transaction::commit_idempotent`] accepts an idempotency key
//!   plus owner plus request fingerprint plus expiry; a repeated key with an
//!   identical request replays the original commit receipt, a repeated key
//!   with a different request conflicts, and records persist durably in a
//!   sibling `TXN_IDEMPOTENCY` file (mirroring `jobs.rs`'s `JOBS` pattern).

use crate::database::{Database, ExternalTriggerBridge, TableHandle};
use crate::epoch::{Epoch, Snapshot};
use crate::error::{MongrelError, Result};
use crate::memtable::Value;
use crate::rowid::RowId;
use crate::wal::SharedWal;
use mongreldb_types::hlc::HlcTimestamp;
use parking_lot::{Condvar, Mutex as PlMutex};
use std::sync::Arc;

pub(crate) fn allocate_txn_id(allocator: &PlMutex<u64>) -> Result<u64> {
    let mut next = allocator.lock();
    let id = *next;
    if id == crate::wal::SYSTEM_TXN_ID || id & u32::MAX as u64 == 0 {
        return Err(MongrelError::Full(
            "per-open transaction id namespace exhausted; reopen the database".into(),
        ));
    }
    *next = id.checked_add(1).ok_or_else(|| {
        MongrelError::Full(
            "per-open transaction id namespace exhausted; reopen the database".into(),
        )
    })?;
    Ok(id)
}

/// One staged mutation against a named table.
pub(crate) enum Staged {
    Put(Vec<(u16, Value)>),
    Delete(RowId),
    /// Full post-update row image plus the logical columns changed by this
    /// operation. Authorization uses `changed_columns`; constraints, RLS,
    /// triggers, WAL publication, and index maintenance use `new_row`.
    Update {
        row_id: RowId,
        new_row: Vec<(u16, Value)>,
        changed_columns: Vec<u16>,
    },
    Truncate,
}

#[derive(Debug, Clone)]
pub struct OwnedRow {
    pub columns: Vec<(u16, Value)>,
}

#[derive(Debug, Clone)]
pub struct PutResult {
    pub auto_inc: Option<i64>,
    pub row: OwnedRow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpsertActionKind {
    Inserted,
    Updated,
    Unchanged,
}

#[derive(Debug, Clone)]
pub enum UpsertAction {
    DoNothing,
    DoUpdate(Vec<(u16, Value)>),
}

#[derive(Debug, Clone)]
pub struct UpsertResult {
    pub action: UpsertActionKind,
    pub row: OwnedRow,
    pub auto_inc: Option<i64>,
}

// ── S1B-001: formal transaction state (spec §10.2) ───────────────────────

/// Why a transaction ended without a commit receipt (spec §10.2, S1B-001).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AbortReason {
    /// Explicit rollback, or the transaction was dropped while still active.
    RolledBack,
    /// Write/write conflict (first-committer-wins) or an SSI serialization
    /// failure detected at commit.
    Conflict(String),
    /// Constraint, authorization, or catalog validation failed before the
    /// commit fence.
    Validation(String),
    /// Cancellation or deadline before the commit fence.
    Cancelled(String),
    /// Any other pre-fence failure.
    Error(String),
}

/// Formal transaction state (spec §10.2, S1B-001).
///
/// Once a commit is `CommitCritical` its outcome may be durable; from there
/// the only honest transition is `Committed(receipt)`. A post-fence failure
/// (unknown outcome) leaves the state `CommitCritical` rather than reporting
/// an abort that may be false (spec §4.7).
#[derive(Debug, Clone)]
pub enum TransactionState {
    /// Staging writes; no commit attempted.
    Active,
    /// Commit entered: preparation and validation are underway, nothing can
    /// be durable yet.
    Preparing,
    /// Commit timestamp assigned and the commit proposal in flight; the
    /// outcome may already be durable.
    CommitCritical,
    /// Durable and published; carries the commit log's irrevocable receipt.
    Committed(mongreldb_log::CommitReceipt),
    /// Ended before the commit fence with nothing durable.
    Aborted(AbortReason),
}

/// Shared, inspectable handle to a transaction's formal state (S1B-001).
///
/// Transitions are enforced: `Active → Preparing → CommitCritical →
/// Committed`, and `Aborted` is reachable only from `Active`/`Preparing`.
/// A transaction outlives its `commit()` call through this handle, so tests
/// and higher layers can inspect the final state after the `Transaction`
/// value itself is consumed.
#[derive(Debug, Clone)]
pub struct TxnStateHandle {
    inner: std::sync::Arc<PlMutex<TransactionState>>,
}

impl TxnStateHandle {
    fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(PlMutex::new(TransactionState::Active)),
        }
    }

    /// The current state (cloned).
    pub fn state(&self) -> TransactionState {
        self.inner.lock().clone()
    }

    /// Transition to `next` when legal; returns whether the transition
    /// happened. Illegal transitions are rejected (the state is unchanged).
    fn transition(&self, next: TransactionState) -> bool {
        let mut current = self.inner.lock();
        let legal = matches!(
            (&*current, &next),
            (TransactionState::Active, TransactionState::Preparing)
                | (TransactionState::Active, TransactionState::Aborted(_))
                | (
                    TransactionState::Preparing,
                    TransactionState::CommitCritical
                )
                | (TransactionState::Preparing, TransactionState::Aborted(_))
                // Idempotent replay (S1B-005): the commit resolved to a
                // pre-existing receipt without entering commit-critical.
                | (TransactionState::Preparing, TransactionState::Committed(_))
                | (
                    TransactionState::CommitCritical,
                    TransactionState::Committed(_)
                )
        );
        if legal {
            *current = next;
        }
        legal
    }

    pub(crate) fn begin_prepare(&self) -> bool {
        self.transition(TransactionState::Preparing)
    }

    pub(crate) fn enter_commit_critical(&self) -> bool {
        self.transition(TransactionState::CommitCritical)
    }

    pub(crate) fn committed(&self, receipt: mongreldb_log::CommitReceipt) -> bool {
        self.transition(TransactionState::Committed(receipt))
    }

    /// Abort from `Active`/`Preparing`. From `CommitCritical` this is a no-op
    /// returning `false`: a possibly-durable commit is never reported aborted.
    pub fn abort(&self, reason: AbortReason) -> bool {
        self.transition(TransactionState::Aborted(reason))
    }
}

/// Map a commit-path error onto the transaction state: pre-fence failures
/// abort; post-fence unknown-outcome errors leave `CommitCritical` intact.
pub(crate) fn classify_commit_error(state: &TxnStateHandle, error: &MongrelError) {
    let reason = match error {
        MongrelError::Conflict(message) => AbortReason::Conflict(message.clone()),
        // SSI certification abort: the same retry-the-whole-transaction
        // discipline as a write/write conflict (taxonomy category 8 keeps it
        // precise for bindings). The recorded message keeps the variant's
        // display prefix so state inspection reads exactly like the error.
        MongrelError::SerializationFailure { message } => {
            AbortReason::Conflict(format!("serialization failure: {message}"))
        }
        MongrelError::Cancelled => AbortReason::Cancelled("cancelled".into()),
        MongrelError::DeadlineExceeded => AbortReason::Cancelled("deadline exceeded".into()),
        // Post-fence: the commit may be durable (spec §4.7). Do not abort.
        MongrelError::CommitOutcomeUnknown { .. } | MongrelError::DurableCommit { .. } => return,
        MongrelError::InvalidArgument(message)
        | MongrelError::Schema(message)
        | MongrelError::TriggerValidation(message) => AbortReason::Validation(message.clone()),
        other => AbortReason::Error(other.to_string()),
    };
    state.abort(reason);
}

// ── S1B-001/002: read, write, and predicate sets ─────────────────────────

/// Point reads a transaction performed, in shared `(table_id, row_id)` space
/// (spec §10.2, S1B-001). Recorded at every isolation level; certified
/// against the conflict index at commit time for `Serializable`.
#[derive(Debug, Clone, Default)]
pub struct ReadSet {
    rows: std::collections::BTreeSet<(u64, u64)>,
}

impl ReadSet {
    pub(crate) fn record_row(&mut self, table_id: u64, row_id: RowId) {
        self.rows.insert((table_id, row_id.0));
    }

    /// The recorded `(table_id, row_id)` point reads.
    pub fn rows(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.rows.iter().copied()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Predicate/range reads at table granularity (spec §10.2, S1B-002). A scan
/// or range lookup registers the table here; `Serializable` certification
/// treats any concurrent write on a registered table as a phantom
/// invalidation. Table granularity is deliberately conservative — the
/// single-node commit path trades some false-positive aborts for a simple,
/// sound phantom check.
#[derive(Debug, Clone, Default)]
pub struct PredicateSet {
    tables: std::collections::BTreeSet<u64>,
}

impl PredicateSet {
    pub(crate) fn record_table(&mut self, table_id: u64) {
        self.tables.insert(table_id);
    }

    /// The tables with registered predicate/range reads.
    pub fn tables(&self) -> impl Iterator<Item = u64> + '_ {
        self.tables.iter().copied()
    }

    pub fn len(&self) -> usize {
        self.tables.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }
}

/// The SSI certification keys for a commit (S1B-002): every point read as a
/// row key, every predicate read as a table key, probed against the
/// [`ConflictIndex`] exactly like first-committer-wins write keys.
pub(crate) fn ssi_validation_keys(
    read_set: &ReadSet,
    predicate_set: &PredicateSet,
) -> Vec<WriteKey> {
    let mut keys = Vec::with_capacity(read_set.len() + predicate_set.len());
    for (table_id, row_id) in read_set.rows() {
        keys.push(WriteKey::Row { table_id, row_id });
    }
    for table_id in predicate_set.tables() {
        keys.push(WriteKey::Table { table_id });
    }
    keys
}

// ── S1B-004/005: per-commit context and idempotency ──────────────────────

/// Per-commit context threaded from a [`Transaction`] into the commit
/// sequencer (spec §10.2, S1B-001/002/004/005).
pub(crate) struct TxnCommitContext {
    /// Isolation level the transaction ran at.
    pub isolation: IsolationLevel,
    /// HLC read timestamp captured at `begin` (spec §8.2 participant
    /// timestamp). `None` when clock-skew rejection deferred allocation to
    /// the commit path, or for internal commits with no transaction.
    pub read_ts: Option<HlcTimestamp>,
    /// Tracked point reads (SSI certification input).
    pub read_set: ReadSet,
    /// Tracked predicate/range reads (SSI certification input).
    pub predicate_set: PredicateSet,
    /// Formal state handle driven through the commit protocol.
    pub state: Option<TxnStateHandle>,
    /// Optional idempotency parameters (S1B-005).
    pub idempotency: Option<IdempotencyRequest>,
}

impl TxnCommitContext {
    /// Internal commit paths (catalog backfills, external-table state) that do
    /// not originate from a user `Transaction`: repeatable-read semantics, no
    /// tracked reads, no state handle, no idempotency.
    pub(crate) fn internal() -> Self {
        Self {
            isolation: IsolationLevel::RepeatableRead,
            read_ts: None,
            read_set: ReadSet::default(),
            predicate_set: PredicateSet::default(),
            state: None,
            idempotency: None,
        }
    }
}

/// An in-flight cross-table transaction. Holds a read snapshot taken at `begin`
/// and stages writes; nothing is durable or visible until [`Self::commit`].
///
/// S1B-001 state: the transaction carries its [`IsolationLevel`], the HLC read
/// timestamp captured at `begin`, its read/predicate sets, and a formal
/// [`TransactionState`] inspectable through [`Self::state`]/[`Self::state_handle`].
pub struct Transaction<'db> {
    db: &'db Database,
    txn_id: u64,
    allocation_error: Option<String>,
    isolation: IsolationLevel,
    read: Snapshot,
    read_ts: Option<HlcTimestamp>,
    read_set: ReadSet,
    predicate_set: PredicateSet,
    state: TxnStateHandle,
    staging: Vec<(u64 /*table_id*/, Staged)>,
    external_states: Vec<(String, Vec<u8>)>,
    materialized_view_updates: Vec<crate::catalog::MaterializedViewEntry>,
    principal: Option<crate::auth::Principal>,
    principal_catalog_bound: bool,
    external_trigger_bridge: Option<&'db dyn ExternalTriggerBridge>,
    _active: Option<ActiveTxnGuard<'db>>,
}

impl<'db> Transaction<'db> {
    pub(crate) fn new(
        db: &'db Database,
        txn_id: Result<u64>,
        read: Snapshot,
        isolation: IsolationLevel,
    ) -> Self {
        let guard = db.register_active(read.epoch);
        // §8.2: the transaction's read timestamp is a commit-timestamp
        // participant. Skew rejection defers allocation to the commit path,
        // which fails closed there instead.
        let read_ts = db.hlc_clock().now().ok();
        let (txn_id, allocation_error) = match txn_id {
            Ok(txn_id) => (txn_id, None),
            Err(MongrelError::Full(message)) => (crate::wal::SYSTEM_TXN_ID, Some(message)),
            Err(error) => (crate::wal::SYSTEM_TXN_ID, Some(error.to_string())),
        };
        Self {
            db,
            txn_id,
            allocation_error,
            isolation,
            read,
            read_ts,
            read_set: ReadSet::default(),
            predicate_set: PredicateSet::default(),
            state: TxnStateHandle::new(),
            staging: Vec::new(),
            external_states: Vec::new(),
            materialized_view_updates: Vec::new(),
            principal: None,
            principal_catalog_bound: false,
            external_trigger_bridge: None,
            _active: Some(guard),
        }
    }

    pub(crate) fn with_external_trigger_bridge(
        mut self,
        bridge: &'db dyn ExternalTriggerBridge,
    ) -> Self {
        self.external_trigger_bridge = Some(bridge);
        self
    }

    pub(crate) fn with_principal(
        mut self,
        principal: Option<crate::auth::Principal>,
        catalog_bound: bool,
    ) -> Self {
        self.principal = principal;
        self.principal_catalog_bound = catalog_bound;
        self
    }

    pub fn read_snapshot(&self) -> Snapshot {
        self.read
    }

    /// The transaction's id (generation-scoped: high 32 bits = open generation,
    /// low 32 = per-open counter). Mainly diagnostic / test-facing.
    pub fn txn_id(&self) -> u64 {
        self.txn_id
    }

    /// The isolation level this transaction was begun with (S1B-001/002).
    pub fn isolation(&self) -> IsolationLevel {
        self.isolation
    }

    /// The HLC read timestamp captured at `begin` (spec §8.2). `None` when
    /// clock-skew rejection deferred allocation to the commit path.
    pub fn read_ts(&self) -> Option<HlcTimestamp> {
        self.read_ts
    }

    /// The current formal state (S1B-001).
    pub fn state(&self) -> TransactionState {
        self.state.state()
    }

    /// A cloneable handle to the formal state, inspectable after this value
    /// is consumed by `commit`/`rollback` (S1B-001).
    pub fn state_handle(&self) -> TxnStateHandle {
        self.state.clone()
    }

    /// Point reads recorded so far (S1B-001; certified at `Serializable`).
    pub fn read_set(&self) -> &ReadSet {
        &self.read_set
    }

    /// Predicate/range reads recorded so far (S1B-001/002).
    pub fn predicate_set(&self) -> &PredicateSet {
        &self.predicate_set
    }

    /// The snapshot a statement reads at (S1B-002): `ReadCommitted` re-pins
    /// at the latest visible epoch per statement; every other level reads
    /// the fixed begin snapshot.
    fn statement_snapshot(&self) -> Snapshot {
        match self.isolation.canonical() {
            IsolationLevel::ReadCommitted => self.db.visible_snapshot(),
            _ => self.read,
        }
    }

    /// Read one row at this transaction's current statement snapshot and
    /// record the point read in the transaction's read set. `ReadCommitted`
    /// statements observe commits that landed since the previous statement;
    /// `RepeatableRead`/`Serializable` observe the fixed begin snapshot.
    pub fn get(&mut self, table: &str, row_id: RowId) -> Result<Option<OwnedRow>> {
        let snap = self.statement_snapshot();
        let id = self.db.table_id(table)?;
        let handle = self.db.table(table)?;
        let row = handle.lock().get(row_id, snap);
        Ok(row.map(|row| {
            self.read_set.record_row(id, row_id);
            owned_row_from_map(row.columns)
        }))
    }

    /// Register a table-granularity predicate/range read (S1B-002). Scan and
    /// range-lookup paths report here so `Serializable` certification detects
    /// phantoms: any concurrent write on the table then invalidates the read.
    pub fn track_predicate_read(&mut self, table: &str) -> Result<()> {
        let id = self.db.table_id(table)?;
        self.predicate_set.record_table(id);
        Ok(())
    }

    /// Stage a put on `table`. The row id is allocated at commit so an aborted
    /// transaction never consumes ids. If the table has an `AUTO_INCREMENT`
    /// primary key and the column is omitted or null, the engine fills it now
    /// and returns the assigned value; explicit ids are honored and advance the
    /// counter. The value is staged in `cells`, so the commit path writes the
    /// same id into the row.
    /// S1B-003: serialize auto-increment allocation for `table_id` on its
    /// sequence barrier BEFORE the engine allocates, so concurrently
    /// committing transactions' assigned values map monotonically onto commit
    /// order. Held until the transaction ends (the commit-path guard, or the
    /// drop release on rollback/abort). Acquired before any table lock —
    /// never the reverse — to keep the lock order consistent with the commit
    /// path, and only when this staging actually allocates (an explicit
    /// auto-inc value advances the counter without allocation).
    fn lock_auto_inc_barrier(&self, table_id: u64, cells: &[(u16, Value)]) -> Result<()> {
        if self.db.table_auto_inc_would_allocate(table_id, cells) {
            self.db.acquire_txn_lock(
                self.txn_id,
                crate::locks::LockKey::sequence_barrier(format!("auto_inc:{table_id}").as_str()),
                crate::locks::LockMode::Exclusive,
                None,
            )?;
        }
        Ok(())
    }

    pub fn put(&mut self, table: &str, mut cells: Vec<(u16, Value)>) -> Result<Option<i64>> {
        self.require_columns(table, crate::auth::ColumnOperation::Insert, &cells)?;
        let id = self.db.table_id(table)?;
        self.lock_auto_inc_barrier(id, &cells)?;
        let handle = self.db.table(table)?;
        let mut t = handle.lock();
        let assigned = t.fill_auto_inc(&mut cells)?;
        t.apply_defaults(&mut cells)?;
        drop(t);
        self.staging.push((id, Staged::Put(cells)));
        Ok(assigned)
    }

    /// Stage a row in a hidden CTAS build table.
    #[doc(hidden)]
    pub fn put_building(
        &mut self,
        table: &str,
        mut cells: Vec<(u16, Value)>,
    ) -> Result<Option<i64>> {
        self.db
            .require_for(self.principal.as_ref(), &crate::auth::Permission::Ddl)?;
        let id = self.db.building_table_id(table)?;
        self.lock_auto_inc_barrier(id, &cells)?;
        let handle = self.db.table_by_id(id)?;
        let mut target = handle.lock();
        let assigned = target.fill_auto_inc(&mut cells)?;
        target.apply_defaults(&mut cells)?;
        let primary_key_column = target
            .schema()
            .primary_key()
            .map(|column| column.id)
            .ok_or_else(|| MongrelError::Schema("CTAS build table has no primary key".into()))?;
        let primary_key = cells
            .iter()
            .find(|(column, _)| *column == primary_key_column)
            .map(|(_, value)| value)
            .ok_or_else(|| MongrelError::InvalidArgument("CTAS primary key is missing".into()))?;
        if matches!(primary_key, Value::Null) {
            return Err(MongrelError::InvalidArgument(
                "CTAS primary key cannot be NULL".into(),
            ));
        }
        let primary_key = primary_key.encode_key();
        let replacing = self
            .staging
            .iter()
            .any(|(table_id, staged)| *table_id == id && matches!(staged, Staged::Truncate));
        if !replacing && target.lookup_pk(&primary_key).is_some() {
            return Err(MongrelError::InvalidArgument(
                "duplicate CTAS primary key".into(),
            ));
        }
        drop(target);
        if self.staging.iter().any(|(staged_table, staged)| {
            if *staged_table != id {
                return false;
            }
            let Staged::Put(staged_cells) = staged else {
                return false;
            };
            staged_cells
                .iter()
                .find(|(column, _)| *column == primary_key_column)
                .is_some_and(|(_, value)| value.encode_key() == primary_key)
        }) {
            return Err(MongrelError::InvalidArgument(
                "duplicate CTAS primary key".into(),
            ));
        }
        self.staging.push((id, Staged::Put(cells)));
        Ok(assigned)
    }

    /// Stage a truncate against an unpublished building table.
    #[doc(hidden)]
    pub fn truncate_building(&mut self, table: &str) -> Result<()> {
        self.db
            .require_for(self.principal.as_ref(), &crate::auth::Permission::Ddl)?;
        let id = self.db.building_table_id(table)?;
        if self.staging.iter().any(|(table_id, _)| *table_id == id) {
            return Err(MongrelError::InvalidArgument(
                "building-table truncate must be staged before replacement rows".into(),
            ));
        }
        self.staging.push((id, Staged::Truncate));
        Ok(())
    }

    pub fn put_returning(
        &mut self,
        table: &str,
        mut cells: Vec<(u16, Value)>,
    ) -> Result<PutResult> {
        self.require_columns(table, crate::auth::ColumnOperation::Insert, &cells)?;
        let id = self.db.table_id(table)?;
        self.lock_auto_inc_barrier(id, &cells)?;
        let handle = self.db.table(table)?;
        let mut t = handle.lock();
        let assigned = t.fill_auto_inc(&mut cells)?;
        t.apply_defaults(&mut cells)?;
        drop(t);
        let row = owned_row_from_cells(&cells);
        self.staging.push((id, Staged::Put(cells)));
        Ok(PutResult {
            auto_inc: assigned,
            row,
        })
    }

    /// Stage a returning put only if `table` still names the exact catalog
    /// resource checked by the caller.
    #[doc(hidden)]
    pub fn put_returning_bound(
        &mut self,
        table: &str,
        expected_table_id: u64,
        expected_schema_id: u64,
        mut cells: Vec<(u16, Value)>,
    ) -> Result<PutResult> {
        self.require_columns(table, crate::auth::ColumnOperation::Insert, &cells)?;
        self.lock_auto_inc_barrier(expected_table_id, &cells)?;
        let handle = self.bound_table(table, expected_table_id, expected_schema_id)?;
        let mut target = handle.lock();
        let assigned = target.fill_auto_inc(&mut cells)?;
        target.apply_defaults(&mut cells)?;
        drop(target);
        let row = owned_row_from_cells(&cells);
        self.staging.push((expected_table_id, Staged::Put(cells)));
        Ok(PutResult {
            auto_inc: assigned,
            row,
        })
    }

    /// Stage many puts on the same `table` with one table-id lookup + one
    /// auto-inc lock pass. Each row is staged individually (same as repeated
    /// `put`); the savings are the amortized lookups/locks for bulk guard-row
    /// writes and batched application-row inserts. Returns the assigned
    /// auto-increment values (`Some` only where the engine filled the column).
    pub fn put_batch(
        &mut self,
        table: &str,
        rows: Vec<Vec<(u16, Value)>>,
    ) -> Result<Vec<Option<i64>>> {
        if !rows.is_empty() {
            let mut columns = rows
                .iter()
                .flat_map(|cells| cells.iter().map(|(column, _)| *column))
                .collect::<Vec<_>>();
            columns.sort_unstable();
            columns.dedup();
            self.db.require_columns_for(
                table,
                crate::auth::ColumnOperation::Insert,
                &columns,
                self.principal.as_ref(),
            )?;
        }
        let id = self.db.table_id(table)?;
        for cells in &rows {
            self.lock_auto_inc_barrier(id, cells)?;
        }
        let handle = self.db.table(table)?;
        let mut t = handle.lock();
        let mut assigned = Vec::with_capacity(rows.len());
        for mut cells in rows {
            let a = t.fill_auto_inc(&mut cells)?;
            t.apply_defaults(&mut cells)?;
            assigned.push(a);
            self.staging.push((id, Staged::Put(cells)));
        }
        drop(t);
        Ok(assigned)
    }

    /// Stage a delete of `row_id` on `table`.
    pub fn delete(&mut self, table: &str, row_id: RowId) -> Result<()> {
        self.delete_batch(table, vec![row_id])
    }

    /// Stage a delete only against the exact table generation checked by the
    /// caller.
    #[doc(hidden)]
    pub fn delete_bound(
        &mut self,
        table: &str,
        expected_table_id: u64,
        expected_schema_id: u64,
        row_id: RowId,
    ) -> Result<()> {
        self.require_delete(table)?;
        self.bound_table(table, expected_table_id, expected_schema_id)?;
        self.reject_after_truncate(expected_table_id)?;
        self.staging
            .push((expected_table_id, Staged::Delete(row_id)));
        Ok(())
    }

    /// Resolve and delete a primary key only on the exact table generation
    /// checked by the caller. Returns `false` when the key is absent.
    #[doc(hidden)]
    pub fn delete_by_pk_bound(
        &mut self,
        table: &str,
        expected_table_id: u64,
        expected_schema_id: u64,
        key: &Value,
    ) -> Result<bool> {
        self.require_delete(table)?;
        let handle = self.bound_table(table, expected_table_id, expected_schema_id)?;
        self.reject_after_truncate(expected_table_id)?;
        let row_id = {
            let mut target = handle.lock();
            target.ensure_indexes_complete()?;
            target.lookup_pk(&key.encode_key())
        };
        let Some(row_id) = row_id else {
            return Ok(false);
        };
        self.read_set.record_row(expected_table_id, row_id);
        self.staging
            .push((expected_table_id, Staged::Delete(row_id)));
        Ok(true)
    }

    /// Stage deletes without materializing pre-images.
    pub fn delete_batch(&mut self, table: &str, row_ids: Vec<RowId>) -> Result<()> {
        self.require_delete(table)?;
        let id = self.db.table_id(table)?;
        self.reject_after_truncate(id)?;
        self.staging.extend(
            row_ids
                .into_iter()
                .map(|row_id| (id, Staged::Delete(row_id))),
        );
        Ok(())
    }

    /// Stage opaque external-table module state. The payload is committed under
    /// the same WAL `TxnCommit` as ordinary table writes.
    pub fn put_external_state(&mut self, table: &str, state: Vec<u8>) -> Result<()> {
        if self.db.external_table(table).is_none() {
            return Err(MongrelError::NotFound(format!(
                "external table {table:?} not found"
            )));
        }
        self.external_states.push((table.to_string(), state));
        Ok(())
    }

    /// Stage a materialized-view checkpoint in the same durable commit as its
    /// row deltas. This makes incremental refresh replay idempotent after a
    /// crash: data and the Last-Event-ID watermark advance together.
    pub fn set_materialized_view_definition(
        &mut self,
        definition: crate::catalog::MaterializedViewEntry,
    ) -> Result<()> {
        self.db
            .require_for(self.principal.as_ref(), &crate::auth::Permission::Ddl)?;
        if self.db.table_id(&definition.name).is_err() {
            return Err(MongrelError::NotFound(format!(
                "materialized view table {:?} not found",
                definition.name
            )));
        }
        self.materialized_view_updates
            .retain(|current| current.name != definition.name);
        self.materialized_view_updates.push(definition);
        Ok(())
    }

    pub fn delete_many(&mut self, table: &str, row_ids: Vec<RowId>) -> Result<Vec<OwnedRow>> {
        self.require_delete(table)?;
        let id = self.db.table_id(table)?;
        self.reject_after_truncate(id)?;
        let snap = self.statement_snapshot();
        let handle = self.db.table(table)?;
        let t = handle.lock();
        let mut pre_images = Vec::with_capacity(row_ids.len());
        for row_id in &row_ids {
            if let Some(row) = t.get(*row_id, snap) {
                pre_images.push(owned_row_from_map(row.columns));
                self.read_set.record_row(id, *row_id);
            }
        }
        drop(t);
        for row_id in row_ids {
            self.staging.push((id, Staged::Delete(row_id)));
        }
        Ok(pre_images)
    }

    pub fn update_many(
        &mut self,
        table: &str,
        updates: Vec<(RowId, Vec<(u16, Value)>)>,
    ) -> Result<Vec<OwnedRow>> {
        if !updates.is_empty() {
            let mut columns = updates
                .iter()
                .flat_map(|(_, cells)| cells.iter().map(|(column, _)| *column))
                .collect::<Vec<_>>();
            columns.sort_unstable();
            columns.dedup();
            self.db.require_columns_for(
                table,
                crate::auth::ColumnOperation::Update,
                &columns,
                self.principal.as_ref(),
            )?;
        }
        let id = self.db.table_id(table)?;
        self.reject_after_truncate(id)?;
        let snap = self.statement_snapshot();
        let handle = self.db.table(table)?;
        let t = handle.lock();
        let mut post_images = Vec::with_capacity(updates.len());
        let mut staged = Vec::with_capacity(updates.len());
        for (old_id, new_cells) in updates {
            let changed_columns = changed_columns(&new_cells);
            let old_row = t
                .get(old_id, snap)
                .ok_or_else(|| MongrelError::NotFound(format!("row {old_id:?} not found")))?;
            self.read_set.record_row(id, old_id);
            let merged = merge_cells(old_row.columns.into_iter().collect(), new_cells);
            post_images.push(owned_row_from_cells(&merged));
            staged.push((
                id,
                Staged::Update {
                    row_id: old_id,
                    new_row: merged,
                    changed_columns,
                },
            ));
        }
        drop(t);
        self.staging.extend(staged);
        Ok(post_images)
    }

    pub fn upsert(
        &mut self,
        table: &str,
        mut insert_cells: Vec<(u16, Value)>,
        action: UpsertAction,
    ) -> Result<UpsertResult> {
        // Upsert may insert or update. Check Insert up front (the common
        // path); the DoUpdate branch additionally checks Update before
        // mutating an existing row.
        self.require_columns(table, crate::auth::ColumnOperation::Insert, &insert_cells)?;
        let id = self.db.table_id(table)?;
        self.lock_auto_inc_barrier(id, &insert_cells)?;
        self.reject_after_truncate(id)?;
        match (self.existing_pk_row(table, &insert_cells)?, action) {
            (None, _) => {
                let handle = self.db.table(table)?;
                let mut t = handle.lock();
                let assigned = t.fill_auto_inc(&mut insert_cells)?;
                t.apply_defaults(&mut insert_cells)?;
                drop(t);
                let row = owned_row_from_cells(&insert_cells);
                self.staging.push((id, Staged::Put(insert_cells)));
                Ok(UpsertResult {
                    action: UpsertActionKind::Inserted,
                    row,
                    auto_inc: assigned,
                })
            }
            (Some((_old_id, old_row)), UpsertAction::DoNothing) => Ok(UpsertResult {
                action: UpsertActionKind::Unchanged,
                row: old_row,
                auto_inc: None,
            }),
            (Some((old_id, old_row)), UpsertAction::DoUpdate(update_cells)) => {
                // The update branch requires Update permission.
                self.require_columns(table, crate::auth::ColumnOperation::Update, &update_cells)?;
                let changed_columns = changed_columns(&update_cells);
                let merged = merge_cells(old_row.columns.clone(), update_cells);
                if columns_equal(&old_row.columns, &merged) {
                    return Ok(UpsertResult {
                        action: UpsertActionKind::Unchanged,
                        row: old_row,
                        auto_inc: None,
                    });
                }
                let row = owned_row_from_cells(&merged);
                self.staging.push((
                    id,
                    Staged::Update {
                        row_id: old_id,
                        new_row: merged,
                        changed_columns,
                    },
                ));
                Ok(UpsertResult {
                    action: UpsertActionKind::Updated,
                    row,
                    auto_inc: None,
                })
            }
        }
    }

    /// Stage an upsert only if `table` still names the exact catalog resource
    /// checked by the caller.
    #[doc(hidden)]
    pub fn upsert_bound(
        &mut self,
        table: &str,
        expected_table_id: u64,
        expected_schema_id: u64,
        mut insert_cells: Vec<(u16, Value)>,
        action: UpsertAction,
    ) -> Result<UpsertResult> {
        self.require_columns(table, crate::auth::ColumnOperation::Insert, &insert_cells)?;
        self.lock_auto_inc_barrier(expected_table_id, &insert_cells)?;
        let handle = self.bound_table(table, expected_table_id, expected_schema_id)?;
        self.reject_after_truncate(expected_table_id)?;
        match (
            self.existing_pk_row_in(&handle, &insert_cells, expected_table_id)?,
            action,
        ) {
            (None, _) => {
                let mut target = handle.lock();
                let assigned = target.fill_auto_inc(&mut insert_cells)?;
                target.apply_defaults(&mut insert_cells)?;
                drop(target);
                let row = owned_row_from_cells(&insert_cells);
                self.staging
                    .push((expected_table_id, Staged::Put(insert_cells)));
                Ok(UpsertResult {
                    action: UpsertActionKind::Inserted,
                    row,
                    auto_inc: assigned,
                })
            }
            (Some((_old_id, old_row)), UpsertAction::DoNothing) => Ok(UpsertResult {
                action: UpsertActionKind::Unchanged,
                row: old_row,
                auto_inc: None,
            }),
            (Some((old_id, old_row)), UpsertAction::DoUpdate(update_cells)) => {
                self.require_columns(table, crate::auth::ColumnOperation::Update, &update_cells)?;
                let changed_columns = changed_columns(&update_cells);
                let merged = merge_cells(old_row.columns.clone(), update_cells);
                if columns_equal(&old_row.columns, &merged) {
                    return Ok(UpsertResult {
                        action: UpsertActionKind::Unchanged,
                        row: old_row,
                        auto_inc: None,
                    });
                }
                let row = owned_row_from_cells(&merged);
                self.staging.push((
                    expected_table_id,
                    Staged::Update {
                        row_id: old_id,
                        new_row: merged,
                        changed_columns,
                    },
                ));
                Ok(UpsertResult {
                    action: UpsertActionKind::Updated,
                    row,
                    auto_inc: None,
                })
            }
        }
    }

    pub fn truncate(&mut self, table: &str) -> Result<()> {
        self.db
            .require_for(self.principal.as_ref(), &crate::auth::Permission::Admin)?;
        let id = self.db.table_id(table)?;
        for (table_id, op) in &self.staging {
            if *table_id == id && !matches!(op, Staged::Truncate) {
                return Err(MongrelError::InvalidArgument(
                    "truncate cannot be combined with other writes on the same table".into(),
                ));
            }
        }
        self.staging.push((id, Staged::Truncate));
        Ok(())
    }

    fn reject_after_truncate(&self, table_id: u64) -> Result<()> {
        if self
            .staging
            .iter()
            .any(|(tid, op)| *tid == table_id && matches!(op, Staged::Truncate))
        {
            return Err(MongrelError::InvalidArgument(
                "truncate cannot be combined with other writes on the same table".into(),
            ));
        }
        Ok(())
    }

    fn require_columns(
        &self,
        table: &str,
        operation: crate::auth::ColumnOperation,
        cells: &[(u16, Value)],
    ) -> Result<()> {
        let columns = cells.iter().map(|(column, _)| *column).collect::<Vec<_>>();
        self.db
            .require_columns_for(table, operation, &columns, self.principal.as_ref())
    }

    fn require_delete(&self, table: &str) -> Result<()> {
        self.db.require_for(
            self.principal.as_ref(),
            &crate::auth::Permission::Delete {
                table: table.to_string(),
            },
        )
    }

    fn bound_table(
        &self,
        table: &str,
        expected_table_id: u64,
        expected_schema_id: u64,
    ) -> Result<TableHandle> {
        let current = self.db.table_identity(table)?;
        if current != (expected_table_id, expected_schema_id) {
            return Err(MongrelError::Conflict(format!(
                "table {table:?} changed after request authorization"
            )));
        }
        self.db.table_by_id(expected_table_id)
    }

    fn existing_pk_row(
        &mut self,
        table: &str,
        cells: &[(u16, Value)],
    ) -> Result<Option<(RowId, OwnedRow)>> {
        let id = self.db.table_id(table)?;
        let handle = self.db.table(table)?;
        self.existing_pk_row_in(&handle, cells, id)
    }

    fn existing_pk_row_in(
        &mut self,
        handle: &TableHandle,
        cells: &[(u16, Value)],
        table_id: u64,
    ) -> Result<Option<(RowId, OwnedRow)>> {
        let snap = self.statement_snapshot();
        let target = handle.lock();
        let Some(pk_col) = target.schema().primary_key() else {
            return Ok(None);
        };
        let Some((_, pk_value)) = cells.iter().find(|(id, _)| *id == pk_col.id) else {
            return Ok(None);
        };
        if matches!(pk_value, Value::Null) {
            return Ok(None);
        }
        let Some(row_id) = target.lookup_pk(&pk_value.encode_key()) else {
            return Ok(None);
        };
        let found = target
            .get(row_id, snap)
            .map(|row| (row_id, owned_row_from_map(row.columns)));
        if found.is_some() {
            self.read_set.record_row(table_id, row_id);
        }
        Ok(found)
    }

    /// Commit: durably seal the staging under one epoch and publish it.
    pub fn commit(self) -> Result<Epoch> {
        self.commit_full(None, None, None).map(|(epoch, _)| epoch)
    }

    /// Commit with idempotency parameters (spec §10.2, S1B-005): a repeated
    /// key with an identical request fingerprint returns the original commit
    /// epoch (the original receipt is visible through the state handle); a
    /// repeated key with a different fingerprint returns `Conflict`.
    pub fn commit_idempotent(self, request: IdempotencyRequest) -> Result<Epoch> {
        self.commit_full(None, None, Some(request))
            .map(|(epoch, _)| epoch)
    }

    pub fn commit_with_row_ids(self) -> Result<(Epoch, Vec<RowId>)> {
        self.commit_full(None, None, None)
    }

    /// Cooperatively prepare this transaction, then invoke `before_commit`
    /// immediately before the first WAL append can occur. If cancellation or
    /// the callback wins, no commit epoch or WAL record is produced.
    pub fn commit_controlled<F>(
        self,
        control: &crate::ExecutionControl,
        mut before_commit: F,
    ) -> Result<Epoch>
    where
        F: FnMut() -> Result<()>,
    {
        self.commit_full(Some(control), Some(&mut before_commit), None)
            .map(|(epoch, _)| epoch)
    }

    pub fn commit_controlled_with_row_ids<F>(
        self,
        control: &crate::ExecutionControl,
        mut before_commit: F,
    ) -> Result<(Epoch, Vec<RowId>)>
    where
        F: FnMut() -> Result<()>,
    {
        self.commit_full(Some(control), Some(&mut before_commit), None)
    }

    /// The single commit entry point (S1B-004): validates state, drives the
    /// formal state machine (`Active → Preparing → …`), threads the S1B
    /// commit context into the sequencer, and classifies the outcome onto
    /// the state handle (`Committed` is set by the sequencer once published;
    /// pre-fence errors abort here; post-fence unknown outcomes leave
    /// `CommitCritical` intact).
    ///
    /// FND-006: `txn.prepare.before`/`txn.prepare.after` bracket the
    /// transition into `Preparing`. A `before` failure never enters prepare;
    /// an `after` failure leaves `Preparing` and is classified as a pre-fence
    /// abort (nothing durable yet).
    fn commit_full(
        mut self,
        control: Option<&crate::ExecutionControl>,
        before_commit: Option<&mut dyn FnMut() -> Result<()>>,
        idempotency: Option<IdempotencyRequest>,
    ) -> Result<(Epoch, Vec<RowId>)> {
        if let Some(message) = self.allocation_error.take() {
            self.state.abort(AbortReason::Error(message.clone()));
            return Err(MongrelError::Full(message));
        }
        // FND-006: arm before the formal prepare transition so a Fail aborts
        // while still Active (no Preparing/Committed observation).
        if let Err(fault) = mongreldb_fault::inject("txn.prepare.before") {
            let error = crate::commit_log::fault_as_io(fault);
            classify_commit_error(&self.state, &error);
            return Err(error);
        }
        self.state.begin_prepare();
        if let Err(fault) = mongreldb_fault::inject("txn.prepare.after") {
            let error = crate::commit_log::fault_as_io(fault);
            classify_commit_error(&self.state, &error);
            return Err(error);
        }
        let context = TxnCommitContext {
            isolation: self.isolation,
            read_ts: self.read_ts,
            read_set: std::mem::take(&mut self.read_set),
            predicate_set: std::mem::take(&mut self.predicate_set),
            state: Some(self.state.clone()),
            idempotency,
        };
        let staging = std::mem::take(&mut self.staging);
        let external_states = std::mem::take(&mut self.external_states);
        let materialized_view_updates = std::mem::take(&mut self.materialized_view_updates);
        let principal = self.principal.take();
        let result = match (control, before_commit) {
            (Some(control), Some(before_commit)) => {
                self.db.commit_transaction_with_external_states_controlled(
                    self.txn_id,
                    self.read.epoch,
                    staging,
                    external_states,
                    materialized_view_updates,
                    principal,
                    self.principal_catalog_bound,
                    self.external_trigger_bridge,
                    context,
                    control,
                    before_commit,
                )
            }
            _ => self.db.commit_transaction_with_external_states(
                self.txn_id,
                self.read.epoch,
                staging,
                external_states,
                materialized_view_updates,
                principal,
                self.principal_catalog_bound,
                self.external_trigger_bridge,
                context,
            ),
        };
        if let Err(error) = &result {
            classify_commit_error(&self.state, error);
        }
        result
    }

    /// Rollback: discard staging. Nothing is appended to the WAL.
    pub fn rollback(self) {
        self.state.abort(AbortReason::RolledBack);
        // Dropping `self` discards the staging — it lives only in memory.
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        // A transaction dropped without commit is aborted (nothing durable
        // was appended). `Committed`/`CommitCritical` states set by a
        // completed or in-flight commit are left untouched.
        self.state.abort(AbortReason::RolledBack);
        // S1B-004 step 12: a transaction never outlives its lock holds. The
        // commit path already released them through its guard, so this only
        // fires for rollback/abort/drop paths — and is a no-op otherwise.
        if self.txn_id != crate::wal::SYSTEM_TXN_ID {
            self.db.release_txn_locks(self.txn_id);
        }
    }
}

fn owned_row_from_cells(cells: &[(u16, Value)]) -> OwnedRow {
    let mut columns = cells.to_vec();
    columns.sort_by_key(|(id, _)| *id);
    OwnedRow { columns }
}

fn owned_row_from_map(columns: HashMap<u16, Value>) -> OwnedRow {
    let mut columns: Vec<(u16, Value)> = columns.into_iter().collect();
    columns.sort_by_key(|(id, _)| *id);
    OwnedRow { columns }
}

fn merge_cells(mut base: Vec<(u16, Value)>, updates: Vec<(u16, Value)>) -> Vec<(u16, Value)> {
    for (id, value) in updates {
        base.retain(|(existing, _)| *existing != id);
        base.push((id, value));
    }
    base.sort_by_key(|(id, _)| *id);
    base
}

fn changed_columns(cells: &[(u16, Value)]) -> Vec<u16> {
    let mut columns = cells.iter().map(|(column, _)| *column).collect::<Vec<_>>();
    columns.sort_unstable();
    columns.dedup();
    columns
}

fn columns_equal(a: &[(u16, Value)], b: &[(u16, Value)]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut a: Vec<_> = a.iter().collect();
    let mut b: Vec<_> = b.iter().collect();
    a.sort_by_key(|(id, _)| *id);
    b.sort_by_key(|(id, _)| *id);
    a.iter()
        .zip(b.iter())
        .all(|((id_a, v_a), (id_b, v_b))| id_a == id_b && v_a == v_b)
}

/// Staged operation produced after row-id allocation (internal to commit).
pub(crate) enum StagedOp {
    Put(Vec<crate::memtable::Row>),
    Delete(Vec<RowId>),
    Truncate,
}

// ── P3.1: conflict index + active-txn registry (spec §8.3, §9.2) ─────────

use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

/// A write-set key broad enough to detect all write–write conflicts under
/// snapshot isolation (spec §8.3, review fix #13).
#[derive(Clone, Debug)]
pub enum WriteKey {
    /// Row-version key for updates/deletes of existing rows.
    Row { table_id: u64, row_id: u64 },
    /// Unique/PK key for inserts/updates touching a UNIQUE column.
    Unique {
        table_id: u64,
        index_id: u16,
        key_hash: u64,
    },
    /// Table-scope key for TRUNCATE/DROP/ALTER and any txn writing that table.
    Table { table_id: u64 },
}

impl Hash for WriteKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            WriteKey::Row { table_id, row_id } => {
                0u8.hash(state);
                table_id.hash(state);
                row_id.hash(state);
            }
            WriteKey::Unique {
                table_id,
                index_id,
                key_hash,
            } => {
                1u8.hash(state);
                table_id.hash(state);
                index_id.hash(state);
                key_hash.hash(state);
            }
            WriteKey::Table { table_id } => {
                2u8.hash(state);
                table_id.hash(state);
            }
        }
    }
}

impl PartialEq for WriteKey {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                WriteKey::Row {
                    table_id: a,
                    row_id: b,
                },
                WriteKey::Row {
                    table_id: c,
                    row_id: d,
                },
            ) => a == c && b == d,
            (
                WriteKey::Unique {
                    table_id: a,
                    index_id: b,
                    key_hash: c,
                },
                WriteKey::Unique {
                    table_id: d,
                    index_id: e,
                    key_hash: f,
                },
            ) => a == d && b == e && c == f,
            (WriteKey::Table { table_id: a }, WriteKey::Table { table_id: b }) => a == b,
            _ => false,
        }
    }
}

impl Eq for WriteKey {}

const CONFLICT_SHARDS: usize = 16;

/// A sharded concurrent map of `WriteKey → commit_epoch` recording recent
/// committed writes (spec §9.2). Validation probes per write-set key; pruning
/// drops entries below `min(active read_epoch)`.
pub struct ConflictIndex {
    shards: [parking_lot::Mutex<HashMap<WriteKey, u64>>; CONFLICT_SHARDS],
    table_truncate_epochs: parking_lot::Mutex<HashMap<u64, u64>>,
    table_write_epochs: parking_lot::Mutex<HashMap<u64, u64>>,
    /// Bumped on every `record()` so pre-validation can detect whether new
    /// commits arrived between the pre-check and the sequencer (spec §8.5,
    /// review fix #17).
    version: std::sync::atomic::AtomicU64,
}

impl ConflictIndex {
    pub fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| parking_lot::Mutex::new(HashMap::new())),
            table_truncate_epochs: parking_lot::Mutex::new(HashMap::new()),
            table_write_epochs: parking_lot::Mutex::new(HashMap::new()),
            version: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Current version (incremented on every `record`). Used by the two-phase
    /// validation: pre-validate + snapshot version → sequencer re-checks only
    /// if the version advanced.
    pub fn version(&self) -> u64 {
        self.version.load(std::sync::atomic::Ordering::Acquire)
    }

    fn shard(&self, key: &WriteKey) -> &parking_lot::Mutex<HashMap<WriteKey, u64>> {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut h);
        let idx = (h.finish() as usize) & (CONFLICT_SHARDS - 1);
        &self.shards[idx]
    }

    /// Returns `true` if any key was committed at an epoch strictly greater
    /// than `read_epoch` (write–write conflict under SI; first-committer-wins).
    pub fn conflicts(&self, keys: &[WriteKey], read_epoch: Epoch) -> bool {
        for k in keys {
            let s = self.shard(k);
            if let Some(&ce) = s.lock().get(k) {
                if ce > read_epoch.0 {
                    return true;
                }
            }
        }
        let truncates = self.table_truncate_epochs.lock();
        let writes = self.table_write_epochs.lock();
        for k in keys {
            match k {
                WriteKey::Row { table_id, .. } | WriteKey::Unique { table_id, .. } => {
                    if truncates.get(table_id).is_some_and(|&ce| ce > read_epoch.0) {
                        return true;
                    }
                }
                WriteKey::Table { table_id } => {
                    if writes.get(table_id).is_some_and(|&ce| ce > read_epoch.0) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Record every write-set key at `commit_epoch`.
    pub fn record(&self, keys: &[WriteKey], commit_epoch: Epoch) {
        for k in keys {
            let s = self.shard(k);
            s.lock().insert(k.clone(), commit_epoch.0);
        }
        let mut truncates = self.table_truncate_epochs.lock();
        let mut writes = self.table_write_epochs.lock();
        for k in keys {
            match k {
                WriteKey::Table { table_id } => {
                    truncates
                        .entry(*table_id)
                        .and_modify(|ce| *ce = (*ce).max(commit_epoch.0))
                        .or_insert(commit_epoch.0);
                }
                WriteKey::Row { table_id, .. } | WriteKey::Unique { table_id, .. } => {
                    writes
                        .entry(*table_id)
                        .and_modify(|ce| *ce = (*ce).max(commit_epoch.0))
                        .or_insert(commit_epoch.0);
                }
            }
        }
        self.version
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }

    /// Drop entries whose `commit_epoch < min_active` (they can never cause a
    /// future conflict once no live txn reads below `min_active`).
    pub fn prune_below(&self, min_active: Epoch) {
        for s in &self.shards {
            s.lock().retain(|_, ce| *ce >= min_active.0);
        }
        self.table_truncate_epochs
            .lock()
            .retain(|_, ce| *ce >= min_active.0);
        self.table_write_epochs
            .lock()
            .retain(|_, ce| *ce >= min_active.0);
    }
}

impl Default for ConflictIndex {
    fn default() -> Self {
        Self::new()
    }
}

// ── P3.2: real group commit (spec §9.3) ─────────────────────────────────

/// Group-commit coordinator (spec §9.3). The commit sequencer appends a txn's
/// records under the WAL mutex but does **not** fsync there; instead each
/// committer calls [`Self::await_durable`] with its commit record's WAL seq.
/// Exactly one waiter becomes the *leader* and issues a single `group_sync`
/// (fsync), which makes durable every record appended up to that point; the
/// others are *followers* that simply wait until `durable_seq` reaches their
/// commit seq. One fsync therefore covers a whole batch of concurrent commits.
pub struct GroupCommit {
    inner: PlMutex<GroupState>,
    cv: Condvar,
    /// S1A-004: the owning core's lifecycle, poisoned on fsync error so every
    /// later operation is rejected at admission. `None` for standalone tables
    /// that have no core lifecycle.
    lifecycle: Option<Arc<crate::core::LifecycleController>>,
}

struct GroupState {
    durable_seq: u64,
    syncing: bool,
    poisoned: bool,
}

impl GroupCommit {
    pub fn new(durable_seq: u64) -> Self {
        Self {
            inner: PlMutex::new(GroupState {
                durable_seq,
                syncing: false,
                poisoned: false,
            }),
            cv: Condvar::new(),
            lifecycle: None,
        }
    }

    /// Attach the owning core's lifecycle controller (S1A-004): an fsync
    /// error poisons it, transitioning the core to
    /// [`crate::core::LifecycleState::Poisoned`].
    pub fn with_lifecycle(mut self, lifecycle: Arc<crate::core::LifecycleController>) -> Self {
        self.lifecycle = Some(lifecycle);
        self
    }

    /// Block until `commit_seq` is durable. The first eligible caller fsyncs on
    /// behalf of the batch; the rest wait on the condvar. On fsync error the
    /// coordinator is poisoned and every waiter (current and future) returns
    /// `Err` (spec §9.3e). `wal` is the same `SharedWal` the sequencer appended
    /// to — locked here only for the brief fsync, never across the wait.
    pub fn await_durable(&self, wal: &PlMutex<SharedWal>, commit_seq: u64) -> Result<()> {
        let mut st = self.inner.lock();
        loop {
            if st.poisoned {
                return Err(MongrelError::Other(
                    "database poisoned by fsync error".into(),
                ));
            }
            if st.durable_seq >= commit_seq {
                return Ok(());
            }
            if st.syncing {
                // Another thread is the leader; wait for it to advance durability.
                self.cv.wait(&mut st);
                continue;
            }
            // Become the leader: fsync outside the coordinator lock (but under
            // the WAL lock) so followers can queue up behind us.
            st.syncing = true;
            drop(st);
            // ponytail: fixed 50 µs batch window; make adaptive if isolated commit latency matters.
            std::thread::sleep(std::time::Duration::from_micros(50));
            let res = wal.lock().group_sync();
            st = self.inner.lock();
            st.syncing = false;
            match res {
                Ok(durable) => {
                    if durable > st.durable_seq {
                        st.durable_seq = durable;
                    }
                    self.cv.notify_all();
                    // Loop re-checks: our commit_seq <= durable (group_sync makes
                    // everything appended-so-far durable), so we return Ok next.
                }
                Err(e) => {
                    st.poisoned = true;
                    // S1A-004: the fsync poison is unrecoverable — transition
                    // the core lifecycle so every later operation is rejected
                    // at admission, not just at the next WAL append.
                    if let Some(lifecycle) = &self.lifecycle {
                        lifecycle.poison();
                    }
                    self.cv.notify_all();
                    return Err(e);
                }
            }
        }
    }
}

/// Tracks the `read_epoch` of every in-flight transaction (spec §9.2, review
/// fix #12). `begin` registers **before** the first read; `min_read_epoch`
/// drives conflict-index pruning.
pub struct ActiveTxns {
    inner: parking_lot::Mutex<BTreeMap<u64, u64>>,
}

impl ActiveTxns {
    pub fn new() -> Self {
        Self {
            inner: parking_lot::Mutex::new(BTreeMap::new()),
        }
    }

    /// Register a transaction's read epoch. Returns a guard that deregisters
    /// on drop.
    pub fn register(&self, read_epoch: Epoch) -> ActiveTxnGuard<'_> {
        let mut g = self.inner.lock();
        *g.entry(read_epoch.0).or_insert(0) += 1;
        ActiveTxnGuard {
            active: self,
            epoch: read_epoch.0,
        }
    }

    /// The lowest live `read_epoch`, or `u64::MAX` when no txn is active.
    pub fn min_read_epoch(&self) -> u64 {
        self.inner.lock().keys().next().copied().unwrap_or(u64::MAX)
    }
}

impl Default for ActiveTxns {
    fn default() -> Self {
        Self::new()
    }
}

/// Guard for an active transaction's read-epoch registration.
pub struct ActiveTxnGuard<'a> {
    active: &'a ActiveTxns,
    epoch: u64,
}

impl Drop for ActiveTxnGuard<'_> {
    fn drop(&mut self) {
        let mut g = self.active.inner.lock();
        if let Some(count) = g.get_mut(&self.epoch) {
            *count -= 1;
            if *count == 0 {
                g.remove(&self.epoch);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_transaction_allocator_never_crosses_open_generation() {
        let allocator = PlMutex::new((7_u64 << 32) | u32::MAX as u64);
        assert_eq!(
            allocate_txn_id(&allocator).unwrap(),
            (7_u64 << 32) | u32::MAX as u64
        );
        assert!(matches!(
            allocate_txn_id(&allocator),
            Err(MongrelError::Full(_))
        ));
    }

    #[test]
    fn conflict_index_first_committer_wins_and_prunes_safely() {
        let ci = ConflictIndex::new();
        let k = vec![WriteKey::Row {
            table_id: 1,
            row_id: 7,
        }];
        assert!(!ci.conflicts(&k, Epoch(5)));
        ci.record(&k, Epoch(6));
        assert!(ci.conflicts(&k, Epoch(5)));
        assert!(!ci.conflicts(&k, Epoch(6)));
        ci.prune_below(Epoch(7));
        assert!(!ci.conflicts(&k, Epoch(5)));
    }

    #[test]
    fn conflict_index_table_scope_conflicts_both_directions() {
        let ci = ConflictIndex::new();
        ci.record(&[WriteKey::Table { table_id: 1 }], Epoch(6));
        assert!(ci.conflicts(
            &[WriteKey::Row {
                table_id: 1,
                row_id: 7,
            }],
            Epoch(5)
        ));
        assert!(ci.conflicts(
            &[WriteKey::Unique {
                table_id: 1,
                index_id: 0,
                key_hash: 42,
            }],
            Epoch(5)
        ));
        assert!(!ci.conflicts(
            &[WriteKey::Row {
                table_id: 2,
                row_id: 7,
            }],
            Epoch(5)
        ));

        let ci = ConflictIndex::new();
        ci.record(
            &[WriteKey::Row {
                table_id: 1,
                row_id: 7,
            }],
            Epoch(6),
        );
        assert!(ci.conflicts(&[WriteKey::Table { table_id: 1 }], Epoch(5)));
        assert!(!ci.conflicts(&[WriteKey::Table { table_id: 2 }], Epoch(5)));
    }

    #[test]
    fn writekey_eq_across_variants() {
        let r1 = WriteKey::Row {
            table_id: 1,
            row_id: 2,
        };
        let r2 = WriteKey::Row {
            table_id: 1,
            row_id: 2,
        };
        let r3 = WriteKey::Row {
            table_id: 1,
            row_id: 3,
        };
        assert_eq!(r1, r2);
        assert_ne!(r1, r3);

        let u1 = WriteKey::Unique {
            table_id: 1,
            index_id: 0,
            key_hash: 42,
        };
        let u2 = WriteKey::Unique {
            table_id: 1,
            index_id: 0,
            key_hash: 42,
        };
        assert_eq!(u1, u2);
        assert_ne!(r1, u1);

        let t1 = WriteKey::Table { table_id: 5 };
        let t2 = WriteKey::Table { table_id: 5 };
        assert_eq!(t1, t2);
        assert_ne!(t1, r1);
    }

    #[test]
    fn active_txns_tracks_min_read_epoch() {
        let at = ActiveTxns::new();
        assert_eq!(at.min_read_epoch(), u64::MAX);
        let g1 = at.register(Epoch(5));
        assert_eq!(at.min_read_epoch(), 5);
        let g2 = at.register(Epoch(3));
        assert_eq!(at.min_read_epoch(), 3);
        drop(g2);
        assert_eq!(at.min_read_epoch(), 5);
        drop(g1);
        assert_eq!(at.min_read_epoch(), u64::MAX);
    }

    #[test]
    fn active_txns_dedups_same_epoch() {
        let at = ActiveTxns::new();
        let g1 = at.register(Epoch(7));
        let g2 = at.register(Epoch(7));
        assert_eq!(at.min_read_epoch(), 7);
        drop(g1);
        assert_eq!(at.min_read_epoch(), 7);
        drop(g2);
        assert_eq!(at.min_read_epoch(), u64::MAX);
    }

    #[test]
    fn isolation_level_snapshot_aliases_repeatable_read() {
        #[allow(deprecated)]
        let snapshot = IsolationLevel::Snapshot;
        assert_eq!(snapshot.canonical(), IsolationLevel::RepeatableRead);
        assert_eq!(IsolationLevel::default(), IsolationLevel::RepeatableRead);
        assert_eq!(
            IsolationLevel::ReadCommitted.canonical(),
            IsolationLevel::ReadCommitted
        );
        assert_eq!(
            IsolationLevel::Serializable.canonical(),
            IsolationLevel::Serializable
        );
    }

    #[test]
    fn transaction_state_transitions_are_enforced() {
        let handle = TxnStateHandle::new();
        assert!(matches!(handle.state(), TransactionState::Active));
        // Illegal: Active cannot become Committed or CommitCritical directly.
        assert!(!handle.enter_commit_critical());
        assert!(matches!(handle.state(), TransactionState::Active));
        // Illegal: Committed is unreachable from Active.
        let receipt = mongreldb_log::CommitReceipt {
            transaction_id: mongreldb_types::ids::TransactionId::from_bytes([0; 16]),
            commit_ts: HlcTimestamp::ZERO,
            log_position: mongreldb_log::LogPosition::ZERO,
            durability: mongreldb_log::DurabilityLevel::GroupCommit,
        };
        assert!(!handle.committed(receipt.clone()));

        assert!(handle.begin_prepare());
        assert!(matches!(handle.state(), TransactionState::Preparing));
        assert!(handle.enter_commit_critical());
        assert!(matches!(handle.state(), TransactionState::CommitCritical));
        // CommitCritical never reports aborted (spec §4.7).
        assert!(!handle.abort(AbortReason::Conflict("late".into())));
        assert!(matches!(handle.state(), TransactionState::CommitCritical));
        assert!(handle.committed(receipt));
        assert!(matches!(handle.state(), TransactionState::Committed(_)));
        // Terminal: no further transitions.
        assert!(!handle.abort(AbortReason::RolledBack));
        assert!(!handle.begin_prepare());
    }

    #[test]
    fn abort_from_preparing_is_terminal() {
        let handle = TxnStateHandle::new();
        assert!(handle.abort(AbortReason::RolledBack));
        assert!(matches!(
            handle.state(),
            TransactionState::Aborted(AbortReason::RolledBack)
        ));
        assert!(!handle.begin_prepare());

        let handle = TxnStateHandle::new();
        handle.begin_prepare();
        assert!(handle.abort(AbortReason::Validation("bad row".into())));
        match handle.state() {
            TransactionState::Aborted(AbortReason::Validation(message)) => {
                assert_eq!(message, "bad row")
            }
            other => panic!("expected validation abort, got {other:?}"),
        }
    }

    #[test]
    fn classify_commit_error_leaves_post_fence_states_untouched() {
        let handle = TxnStateHandle::new();
        handle.begin_prepare();
        classify_commit_error(&handle, &MongrelError::Conflict("ww".into()));
        assert!(matches!(
            handle.state(),
            TransactionState::Aborted(AbortReason::Conflict(_))
        ));

        let handle = TxnStateHandle::new();
        handle.begin_prepare();
        handle.enter_commit_critical();
        classify_commit_error(
            &handle,
            &MongrelError::CommitOutcomeUnknown {
                epoch: 7,
                message: "fsync".into(),
            },
        );
        assert!(matches!(handle.state(), TransactionState::CommitCritical));
        classify_commit_error(
            &handle,
            &MongrelError::DurableCommit {
                epoch: 7,
                message: "publish".into(),
            },
        );
        assert!(matches!(handle.state(), TransactionState::CommitCritical));
    }

    #[test]
    fn classify_commit_error_maps_serialization_failure_to_conflict_abort() {
        // The native SSI abort variant aborts pre-fence with the same
        // retry-the-whole-transaction reason as a write/write conflict, and
        // the recorded message keeps the variant's display prefix.
        let handle = TxnStateHandle::new();
        handle.begin_prepare();
        classify_commit_error(
            &handle,
            &MongrelError::SerializationFailure {
                message: "a concurrent commit invalidated this transaction's reads".into(),
            },
        );
        match handle.state() {
            TransactionState::Aborted(AbortReason::Conflict(message)) => {
                assert_eq!(
                    message,
                    "serialization failure: a concurrent commit invalidated this transaction's reads"
                );
            }
            other => panic!("expected conflict abort, got {other:?}"),
        }
    }

    #[test]
    fn ssi_validation_keys_cover_rows_and_predicates() {
        let mut reads = ReadSet::default();
        reads.record_row(3, RowId(9));
        reads.record_row(3, RowId(9));
        reads.record_row(4, RowId(1));
        let mut predicates = PredicateSet::default();
        predicates.record_table(5);
        let keys = ssi_validation_keys(&reads, &predicates);
        assert_eq!(keys.len(), 3);
        assert!(keys.iter().any(|key| matches!(
            key,
            WriteKey::Row {
                table_id: 3,
                row_id: 9
            }
        )));
        assert!(keys.iter().any(|key| matches!(
            key,
            WriteKey::Row {
                table_id: 4,
                row_id: 1
            }
        )));
        assert!(keys
            .iter()
            .any(|key| matches!(key, WriteKey::Table { table_id: 5 })));
    }

    fn test_request(key: &str) -> IdempotencyRequest {
        IdempotencyRequest {
            key: key.to_string(),
            owner: "alice".to_string(),
            fingerprint: 42,
            ttl: None,
        }
    }

    fn test_commit_ts() -> HlcTimestamp {
        HlcTimestamp {
            physical_micros: 1_700_000_000_000_000,
            logical: 3,
            node_tiebreaker: 0,
        }
    }

    #[test]
    fn idempotency_ledger_replay_conflict_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        let root = std::sync::Arc::new(crate::durable_file::DurableRoot::open(dir.path()).unwrap());
        let ledger = IdempotencyLedger::open(std::sync::Arc::clone(&root), None).unwrap();

        let request = test_request("k1");
        assert!(matches!(
            ledger.check_and_reserve(&request).unwrap(),
            IdempotencyCheck::Reserved
        ));
        // In-flight duplicate conflicts.
        assert!(matches!(
            ledger.check_and_reserve(&request),
            Err(MongrelError::Conflict(_))
        ));
        ledger
            .complete(&request, 7, Epoch(11), test_commit_ts())
            .unwrap();

        // Identical request replays the original receipt.
        let replay = ledger.check_and_reserve(&request).unwrap();
        let IdempotencyCheck::Replay(receipt) = replay else {
            panic!("expected replay");
        };
        assert_eq!(receipt.log_position.index, 11);
        assert_eq!(receipt.commit_ts, test_commit_ts());
        assert_eq!(
            receipt.durability,
            mongreldb_log::DurabilityLevel::GroupCommit
        );

        // Same key, different fingerprint conflicts.
        let mut other = test_request("k1");
        other.fingerprint = 43;
        assert!(matches!(
            ledger.check_and_reserve(&other),
            Err(MongrelError::Conflict(_))
        ));
        // Same key, different owner is an independent key.
        let mut foreign = test_request("k1");
        foreign.owner = "bob".to_string();
        assert!(matches!(
            ledger.check_and_reserve(&foreign).unwrap(),
            IdempotencyCheck::Reserved
        ));
        drop(ledger);

        // Restart: records survive, replay still returns the original receipt.
        let reopened = IdempotencyLedger::open(root, None).unwrap();
        let replay = reopened.check_and_reserve(&request).unwrap();
        let IdempotencyCheck::Replay(receipt) = replay else {
            panic!("expected replay after restart");
        };
        assert_eq!(receipt.log_position.index, 11);
        assert_eq!(receipt.commit_ts, test_commit_ts());
        // The interrupted reservation also survived and fails closed.
        assert!(matches!(
            reopened.check_and_reserve(&foreign),
            Err(MongrelError::Conflict(_))
        ));
    }

    #[test]
    fn idempotency_ledger_release_and_expiry() {
        let dir = tempfile::tempdir().unwrap();
        let root = std::sync::Arc::new(crate::durable_file::DurableRoot::open(dir.path()).unwrap());
        let ledger = IdempotencyLedger::open(std::sync::Arc::clone(&root), None).unwrap();

        // A released reservation frees the key for a fresh attempt.
        let request = test_request("k-release");
        assert!(matches!(
            ledger.check_and_reserve(&request).unwrap(),
            IdempotencyCheck::Reserved
        ));
        ledger.release(&request);
        assert!(matches!(
            ledger.check_and_reserve(&request).unwrap(),
            IdempotencyCheck::Reserved
        ));
        ledger
            .complete(&request, 9, Epoch(12), test_commit_ts())
            .unwrap();

        // An already-expired record is swept on the next check, so the key
        // can be reused.
        let mut expired = test_request("k-expired");
        expired.ttl = Some(Duration::from_nanos(1));
        assert!(matches!(
            ledger.check_and_reserve(&expired).unwrap(),
            IdempotencyCheck::Reserved
        ));
        ledger
            .complete(&expired, 10, Epoch(13), test_commit_ts())
            .unwrap();
        std::thread::sleep(Duration::from_millis(2));
        assert!(matches!(
            ledger.check_and_reserve(&expired).unwrap(),
            IdempotencyCheck::Reserved
        ));

        // Restart sweeps the expired record too.
        drop(ledger);
        let reopened = IdempotencyLedger::open(root, None).unwrap();
        assert!(matches!(
            reopened.check_and_reserve(&expired).unwrap(),
            IdempotencyCheck::Reserved
        ));
    }

    #[test]
    fn idempotency_ledger_rejects_empty_key_or_owner() {
        let dir = tempfile::tempdir().unwrap();
        let root = std::sync::Arc::new(crate::durable_file::DurableRoot::open(dir.path()).unwrap());
        let ledger = IdempotencyLedger::open(root, None).unwrap();
        let mut request = test_request("");
        assert!(matches!(
            ledger.check_and_reserve(&request),
            Err(MongrelError::InvalidArgument(_))
        ));
        request.key = "k".to_string();
        request.owner.clear();
        assert!(matches!(
            ledger.check_and_reserve(&request),
            Err(MongrelError::InvalidArgument(_))
        ));
    }

    #[test]
    fn idempotency_ledger_enforces_bounded_size() {
        let mut records: Vec<StoredIdempotencyRecord> = (0..MAX_IDEMPOTENCY_RECORDS + 4)
            .map(|index| StoredIdempotencyRecord {
                owner: "o".to_string(),
                key: format!("k{index}"),
                fingerprint: 1,
                expires_at_micros: None,
                outcome: StoredIdempotencyOutcome::Committed {
                    txn_id: index as u64,
                    epoch: index as u64,
                    commit_ts: test_commit_ts(),
                },
            })
            .collect();
        enforce_bounds(&mut records).unwrap();
        assert_eq!(records.len(), MAX_IDEMPOTENCY_RECORDS);
        // Oldest (lowest epoch) evicted first.
        assert!(records.iter().all(|record| match &record.outcome {
            StoredIdempotencyOutcome::Committed { epoch, .. } => *epoch >= 4,
            StoredIdempotencyOutcome::Reserved => false,
        }));

        // A ledger of only in-flight reservations cannot be bounded by
        // eviction: fail closed.
        let mut reserved: Vec<StoredIdempotencyRecord> = (0..MAX_IDEMPOTENCY_RECORDS + 1)
            .map(|index| StoredIdempotencyRecord {
                owner: "o".to_string(),
                key: format!("r{index}"),
                fingerprint: 1,
                expires_at_micros: None,
                outcome: StoredIdempotencyOutcome::Reserved,
            })
            .collect();
        assert!(matches!(
            enforce_bounds(&mut reserved),
            Err(MongrelError::ResourceLimitExceeded { .. })
        ));
    }

    #[test]
    fn idempotency_ledger_tampered_file_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let root = std::sync::Arc::new(crate::durable_file::DurableRoot::open(dir.path()).unwrap());
        let ledger = IdempotencyLedger::open(std::sync::Arc::clone(&root), None).unwrap();
        let request = test_request("k1");
        ledger.check_and_reserve(&request).unwrap();
        drop(ledger);

        let path = dir.path().join(IDEMPOTENCY_FILENAME);
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&path, bytes).unwrap();
        assert!(IdempotencyLedger::open(root, None).is_err());
    }
}

/// Transaction isolation level (spec §10.2, S1B-002). MongrelDB defaults to
/// `RepeatableRead` (snapshot isolation).
///
/// - `RepeatableRead`: one snapshot fixed at `begin`; own staged writes are
///   visible to the transaction; write/write conflicts abort one transaction
///   (first-committer-wins). This is the engine's historical `Snapshot` (SI)
///   semantics under its SQL-standard name.
/// - `Snapshot`: deprecated alias of `RepeatableRead`, kept so existing call
///   sites compile unchanged. The two are interchangeable everywhere via
///   [`Self::canonical`].
/// - `ReadCommitted`: each statement obtains a new snapshot at the latest
///   visible epoch, so committed concurrent changes may appear between
///   statements. Commit-time conflict validation still uses the begin epoch
///   (conservative first-committer-wins).
/// - `Serializable`: repeatable-read snapshot plus SSI-style certification —
///   the commit sequencer tracks read dependencies (point reads), predicate/
///   range reads (table granularity), and write dependencies, and aborts
///   with a serialization failure when a concurrent commit invalidated a
///   tracked read (the rw-antidependency dangerous structure).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    #[default]
    RepeatableRead,
    #[deprecated(
        note = "renamed to `RepeatableRead` (spec §10.2 S1B-002); identical semantics, kept for compatibility"
    )]
    Snapshot,
    ReadCommitted,
    Serializable,
}

impl IsolationLevel {
    /// Collapse aliases onto their canonical variant: `Snapshot` behaves
    /// exactly as `RepeatableRead`.
    pub fn canonical(self) -> Self {
        match self {
            #[allow(deprecated)]
            Self::Snapshot => Self::RepeatableRead,
            other => other,
        }
    }
}

// ── S1B-005: durable transaction idempotency ─────────────────────────────

use std::time::Duration;

/// Client-supplied idempotency parameters for one commit (spec §10.2,
/// S1B-005).
#[derive(Debug, Clone)]
pub struct IdempotencyRequest {
    /// The idempotency key, unique per owner.
    pub key: String,
    /// Owning principal; keys from different owners never alias.
    pub owner: String,
    /// Fingerprint of the request payload (caller-computed hash). A repeated
    /// key with the same fingerprint replays the original receipt; a
    /// different fingerprint conflicts.
    pub fingerprint: u64,
    /// How long the record is retained after the commit completes. `None`
    /// keeps the record until the ledger's bounded-size eviction.
    pub ttl: Option<Duration>,
}

/// Sibling ledger file next to `CATALOG`/`JOBS` (mirroring the `JOBS`
/// pattern): checksum-framed JSON, atomically renamed, optionally sealed
/// with the metadata DEK.
pub const IDEMPOTENCY_FILENAME: &str = "TXN_IDEMPOTENCY";

const IDEMPOTENCY_FORMAT_VERSION: u16 = 1;
const IDEMPOTENCY_MAGIC: &[u8; 8] = b"MONGRTXI";
/// Bounded ledger: at most this many records are retained (oldest committed
/// records are evicted first; in-flight reservations are never evicted).
const MAX_IDEMPOTENCY_RECORDS: usize = 65_536;
/// Hard cap on the ledger file size (fail closed beyond it).
const MAX_IDEMPOTENCY_BYTES: u64 = 64 * 1024 * 1024;

/// The durable outcome of one idempotency key.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum StoredIdempotencyOutcome {
    /// A commit was attempted but its receipt was never recorded (crash
    /// window). Retries fail closed with a conflict.
    Reserved,
    /// The commit is durable; this is the receipt to replay.
    Committed {
        txn_id: u64,
        epoch: u64,
        commit_ts: HlcTimestamp,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoredIdempotencyRecord {
    owner: String,
    key: String,
    fingerprint: u64,
    expires_at_micros: Option<u64>,
    outcome: StoredIdempotencyOutcome,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct IdempotencyEnvelope {
    format_version: u16,
    records: Vec<StoredIdempotencyRecord>,
}

/// Result of the pre-propose idempotency check.
pub(crate) enum IdempotencyCheck {
    /// First attempt under this key: it is now reserved (durably) and the
    /// commit must be completed or released.
    Reserved,
    /// An identical request already committed: replay the original receipt
    /// without re-executing.
    Replay(mongreldb_log::CommitReceipt),
}

/// The durable idempotency ledger (S1B-005). In-memory slots mirror the
/// sibling file; every mutation is persisted through
/// [`crate::durable_file::DurableRoot::write_atomic`] before it is relied
/// upon, so a response never acknowledges an idempotency record that a
/// restart cannot see.
pub(crate) struct IdempotencyLedger {
    root: std::sync::Arc<crate::durable_file::DurableRoot>,
    meta_dek: Option<[u8; crate::catalog::META_DEK_LEN]>,
    inner: PlMutex<Vec<StoredIdempotencyRecord>>,
}

impl IdempotencyLedger {
    /// Open the ledger, loading any persisted records. Missing file means an
    /// empty ledger; present-but-unverifiable content fails closed (like the
    /// job registry). Expired records are swept in memory on open; the file
    /// is rewritten lazily on the next mutation.
    pub(crate) fn open(
        root: std::sync::Arc<crate::durable_file::DurableRoot>,
        meta_dek: Option<[u8; crate::catalog::META_DEK_LEN]>,
    ) -> Result<Self> {
        let mut inner = read_idempotency_file(&root, meta_dek.as_ref())?.unwrap_or_default();
        sweep_expired(&mut inner, wall_micros());
        Ok(Self {
            root,
            meta_dek,
            inner: PlMutex::new(inner),
        })
    }

    /// Pre-propose check (S1B-005): sweep expired records, then classify the
    /// request. A new key is reserved and the reservation persisted BEFORE
    /// the commit proposal, so a crash between the durable commit and the
    /// receipt record leaves a `Reserved` slot that fails closed on retry.
    pub(crate) fn check_and_reserve(
        &self,
        request: &IdempotencyRequest,
    ) -> Result<IdempotencyCheck> {
        validate_request(request)?;
        let now = wall_micros();
        let mut inner = self.inner.lock();
        sweep_expired(&mut inner, now);
        if let Some(existing) = inner
            .iter()
            .find(|record| record.owner == request.owner && record.key == request.key)
        {
            if existing.fingerprint != request.fingerprint {
                return Err(MongrelError::Conflict(format!(
                    "idempotency key {:?} was already used with a different request",
                    request.key
                )));
            }
            return match &existing.outcome {
                StoredIdempotencyOutcome::Committed {
                    txn_id,
                    epoch,
                    commit_ts,
                } => Ok(IdempotencyCheck::Replay(mongreldb_log::CommitReceipt {
                    transaction_id: crate::commit_log::transaction_id_from_txn(*txn_id),
                    commit_ts: *commit_ts,
                    log_position: mongreldb_log::LogPosition {
                        term: 0,
                        index: *epoch,
                    },
                    durability: mongreldb_log::DurabilityLevel::GroupCommit,
                })),
                StoredIdempotencyOutcome::Reserved => Err(MongrelError::Conflict(format!(
                    "idempotency key {:?} has an in-flight or interrupted commit; retry to resolve",
                    request.key
                ))),
            };
        }
        inner.push(StoredIdempotencyRecord {
            owner: request.owner.clone(),
            key: request.key.clone(),
            fingerprint: request.fingerprint,
            expires_at_micros: expiry_micros(request.ttl, now),
            outcome: StoredIdempotencyOutcome::Reserved,
        });
        persist_locked(&self.root, self.meta_dek.as_ref(), &mut inner)?;
        Ok(IdempotencyCheck::Reserved)
    }

    /// Record the original commit receipt against a reserved key (after the
    /// receipt exists, before the caller is told success). Never sweeps: the
    /// reservation being completed belongs to an in-flight commit and must
    /// survive even an absurdly short TTL (expiry applies once the commit
    /// has completed, measured from completion).
    pub(crate) fn complete(
        &self,
        request: &IdempotencyRequest,
        txn_id: u64,
        epoch: Epoch,
        commit_ts: HlcTimestamp,
    ) -> Result<()> {
        let mut inner = self.inner.lock();
        let now = wall_micros();
        let Some(record) = inner
            .iter_mut()
            .find(|record| record.owner == request.owner && record.key == request.key)
        else {
            return Err(MongrelError::Other(format!(
                "idempotency reservation for key {:?} vanished during commit",
                request.key
            )));
        };
        record.expires_at_micros = expiry_micros(request.ttl, now);
        record.outcome = StoredIdempotencyOutcome::Committed {
            txn_id,
            epoch: epoch.0,
            commit_ts,
        };
        persist_locked(&self.root, self.meta_dek.as_ref(), &mut inner)
    }

    /// Drop a reservation whose commit never became durable (pre-fence
    /// failure). Best-effort: the caller is already handling the commit
    /// error, and a leaked reservation expires by TTL or conflicts safely.
    pub(crate) fn release(&self, request: &IdempotencyRequest) {
        let mut inner = self.inner.lock();
        inner.retain(|record| {
            !(record.owner == request.owner
                && record.key == request.key
                && matches!(record.outcome, StoredIdempotencyOutcome::Reserved))
        });
        let _ = persist_locked(&self.root, self.meta_dek.as_ref(), &mut inner);
    }
}

/// Guard releasing an idempotency reservation on drop unless disarmed (the
/// commit reached its receipt).
pub(crate) struct IdempotencyReservationGuard<'a> {
    ledger: &'a IdempotencyLedger,
    request: IdempotencyRequest,
    armed: bool,
}

impl<'a> IdempotencyReservationGuard<'a> {
    pub(crate) fn new(ledger: &'a IdempotencyLedger, request: IdempotencyRequest) -> Self {
        Self {
            ledger,
            request,
            armed: true,
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for IdempotencyReservationGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.ledger.release(&self.request);
        }
    }
}

fn validate_request(request: &IdempotencyRequest) -> Result<()> {
    if request.key.is_empty() {
        return Err(MongrelError::InvalidArgument(
            "idempotency key must not be empty".into(),
        ));
    }
    if request.owner.is_empty() {
        return Err(MongrelError::InvalidArgument(
            "idempotency owner must not be empty".into(),
        ));
    }
    Ok(())
}

fn expiry_micros(ttl: Option<Duration>, now_micros: u64) -> Option<u64> {
    ttl.map(|ttl| now_micros.saturating_add(u64::try_from(ttl.as_micros()).unwrap_or(u64::MAX)))
}

fn sweep_expired(records: &mut Vec<StoredIdempotencyRecord>, now_micros: u64) {
    records.retain(|record| {
        record
            .expires_at_micros
            .is_none_or(|expires_at| expires_at > now_micros)
    });
}

/// Evict oldest committed records beyond the bounded size. Reservations in
/// flight are never evicted; a ledger that still exceeds the bound fails
/// closed with `ResourceLimitExceeded`.
fn enforce_bounds(records: &mut Vec<StoredIdempotencyRecord>) -> Result<()> {
    while records.len() > MAX_IDEMPOTENCY_RECORDS {
        let Some((oldest, _)) = records
            .iter()
            .enumerate()
            .filter(|(_, record)| {
                matches!(record.outcome, StoredIdempotencyOutcome::Committed { .. })
            })
            .min_by_key(|(_, record)| match &record.outcome {
                StoredIdempotencyOutcome::Committed { epoch, .. } => *epoch,
                StoredIdempotencyOutcome::Reserved => unreachable!(),
            })
        else {
            return Err(MongrelError::ResourceLimitExceeded {
                resource: "idempotency records",
                requested: records.len(),
                limit: MAX_IDEMPOTENCY_RECORDS,
            });
        };
        records.remove(oldest);
    }
    Ok(())
}

fn persist_locked(
    root: &crate::durable_file::DurableRoot,
    meta_dek: Option<&[u8; crate::catalog::META_DEK_LEN]>,
    records: &mut Vec<StoredIdempotencyRecord>,
) -> Result<()> {
    enforce_bounds(records)?;
    let body = serde_json::to_vec(&IdempotencyEnvelope {
        format_version: IDEMPOTENCY_FORMAT_VERSION,
        records: records.clone(),
    })
    .map_err(|error| MongrelError::Other(format!("idempotency ledger serialize: {error}")))?;
    let payload = seal_idempotency(&body, meta_dek)?;
    root.write_atomic(IDEMPOTENCY_FILENAME, &payload)?;
    Ok(())
}

fn read_idempotency_file(
    root: &crate::durable_file::DurableRoot,
    meta_dek: Option<&[u8; crate::catalog::META_DEK_LEN]>,
) -> Result<Option<Vec<StoredIdempotencyRecord>>> {
    use std::io::Read as _;
    let file = match root.open_regular(IDEMPOTENCY_FILENAME) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let length = file.metadata()?.len();
    if length > MAX_IDEMPOTENCY_BYTES {
        return Err(MongrelError::Other(format!(
            "idempotency ledger of {length} bytes exceeds the {MAX_IDEMPOTENCY_BYTES}-byte limit"
        )));
    }
    let mut bytes = Vec::with_capacity(length as usize);
    file.take(MAX_IDEMPOTENCY_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length {
        return Err(MongrelError::Other(
            "idempotency ledger length changed while reading".into(),
        ));
    }
    open_idempotency_payload(&bytes, meta_dek).map(Some)
}

fn decode_idempotency(body: &[u8]) -> Result<Vec<StoredIdempotencyRecord>> {
    let envelope: IdempotencyEnvelope = serde_json::from_slice(body)
        .map_err(|error| MongrelError::Other(format!("idempotency ledger deserialize: {error}")))?;
    if envelope.format_version != IDEMPOTENCY_FORMAT_VERSION {
        return Err(MongrelError::Other(format!(
            "unsupported idempotency ledger format version {}",
            envelope.format_version
        )));
    }
    Ok(envelope.records)
}

fn plaintext_idempotency_frame(body: &[u8]) -> Vec<u8> {
    use sha2::Digest as _;
    let hash = sha2::Sha256::digest(body);
    let mut out = Vec::with_capacity(body.len() + 8 + 32);
    out.extend_from_slice(IDEMPOTENCY_MAGIC);
    out.extend_from_slice(&hash);
    out.extend_from_slice(body);
    out
}

fn parse_idempotency_plaintext(bytes: &[u8]) -> Result<Vec<StoredIdempotencyRecord>> {
    use sha2::Digest as _;
    if bytes.len() < 8 + 32 || &bytes[..8] != IDEMPOTENCY_MAGIC {
        return Err(MongrelError::Other(
            "idempotency ledger magic mismatch (corrupt or sealed with a key)".into(),
        ));
    }
    let (tag, body) = bytes[8..].split_at(32);
    let calc = sha2::Sha256::digest(body);
    if tag != calc.as_slice() {
        return Err(MongrelError::Other(
            "idempotency ledger checksum mismatch (tampered or torn)".into(),
        ));
    }
    decode_idempotency(body)
}

fn seal_idempotency(
    body: &[u8],
    meta_dek: Option<&[u8; crate::catalog::META_DEK_LEN]>,
) -> Result<Vec<u8>> {
    match meta_dek {
        Some(dek) => crate::encryption::encrypt_blob(dek, body),
        None => Ok(plaintext_idempotency_frame(body)),
    }
}

fn open_idempotency_payload(
    bytes: &[u8],
    meta_dek: Option<&[u8; crate::catalog::META_DEK_LEN]>,
) -> Result<Vec<StoredIdempotencyRecord>> {
    match meta_dek {
        // Fail closed: an unauthenticated ledger is an error, never "no
        // records" (mirroring the job registry).
        Some(dek) => {
            let body = crate::encryption::decrypt_blob(dek, bytes).map_err(|_| {
                MongrelError::Decryption(
                    "idempotency ledger authentication failed (wrong key or tampered)".into(),
                )
            })?;
            decode_idempotency(&body)
        }
        None => parse_idempotency_plaintext(bytes),
    }
}

/// Wall-clock microseconds since the Unix epoch (saturating), for expiry.
fn wall_micros() -> u64 {
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_micros())
        .unwrap_or(0);
    u64::try_from(micros).unwrap_or(u64::MAX)
}
