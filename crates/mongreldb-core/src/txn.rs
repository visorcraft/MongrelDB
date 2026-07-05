//! Cross-table transactions on the shared WAL (spec §8.2, single-applier subset
//! — parallelism arrives in P3).
//!
//! A [`Transaction`] stages puts/deletes keyed by table; [`Transaction::commit`]
//! reserves a commit epoch from the shared authority, appends the staged data
//! records + a `TxnCommit` marker to the shared WAL, group-fsyncs, applies the
//! staging to each table's memtable + indexes at the commit epoch, persists the
//! per-table manifests, and publishes the visible watermark. Rollback (or a
//! dropped transaction) discards the staging and appends nothing durable.

use crate::database::{Database, ExternalTriggerBridge};
use crate::epoch::{Epoch, Snapshot};
use crate::error::{MongrelError, Result};
use crate::memtable::Value;
use crate::rowid::RowId;
use crate::wal::SharedWal;
use parking_lot::{Condvar, Mutex as PlMutex};

/// One staged mutation against a named table.
pub(crate) enum Staged {
    Put(Vec<(u16, Value)>),
    Delete(RowId),
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

/// An in-flight cross-table transaction. Holds a read snapshot taken at `begin`
/// and stages writes; nothing is durable or visible until [`Self::commit`].
pub struct Transaction<'db> {
    db: &'db Database,
    txn_id: u64,
    read: Snapshot,
    staging: Vec<(u64 /*table_id*/, Staged)>,
    external_states: Vec<(String, Vec<u8>)>,
    external_trigger_bridge: Option<&'db dyn ExternalTriggerBridge>,
    _active: Option<ActiveTxnGuard<'db>>,
}

impl<'db> Transaction<'db> {
    pub(crate) fn new(db: &'db Database, txn_id: u64, read: Snapshot) -> Self {
        let guard = db.register_active(read.epoch);
        Self {
            db,
            txn_id,
            read,
            staging: Vec::new(),
            external_states: Vec::new(),
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

    pub fn read_snapshot(&self) -> Snapshot {
        self.read
    }

    /// The transaction's id (generation-scoped: high 32 bits = open generation,
    /// low 32 = per-open counter). Mainly diagnostic / test-facing.
    pub fn txn_id(&self) -> u64 {
        self.txn_id
    }

    /// Stage a put on `table`. The row id is allocated at commit so an aborted
    /// transaction never consumes ids. If the table has an `AUTO_INCREMENT`
    /// primary key and the column is omitted or null, the engine fills it now
    /// and returns the assigned value; explicit ids are honored and advance the
    /// counter. The value is staged in `cells`, so the commit path writes the
    /// same id into the row.
    pub fn put(&mut self, table: &str, mut cells: Vec<(u16, Value)>) -> Result<Option<i64>> {
        let id = self.db.table_id(table)?;
        self.reject_after_truncate(id)?;
        let handle = self.db.table(table)?;
        let mut t = handle.lock();
        let assigned = t.fill_auto_inc(&mut cells)?;
        drop(t);
        self.staging.push((id, Staged::Put(cells)));
        Ok(assigned)
    }

    pub fn put_returning(
        &mut self,
        table: &str,
        mut cells: Vec<(u16, Value)>,
    ) -> Result<PutResult> {
        let id = self.db.table_id(table)?;
        self.reject_after_truncate(id)?;
        let handle = self.db.table(table)?;
        let assigned = handle.lock().fill_auto_inc(&mut cells)?;
        let row = owned_row_from_cells(&cells);
        self.staging.push((id, Staged::Put(cells)));
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
        let id = self.db.table_id(table)?;
        self.reject_after_truncate(id)?;
        let handle = self.db.table(table)?;
        let mut t = handle.lock();
        let mut assigned = Vec::with_capacity(rows.len());
        for mut cells in rows {
            let a = t.fill_auto_inc(&mut cells)?;
            assigned.push(a);
            self.staging.push((id, Staged::Put(cells)));
        }
        drop(t);
        Ok(assigned)
    }

    /// Stage a delete of `row_id` on `table`.
    pub fn delete(&mut self, table: &str, row_id: RowId) -> Result<()> {
        let id = self.db.table_id(table)?;
        self.reject_after_truncate(id)?;
        self.staging.push((id, Staged::Delete(row_id)));
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

    pub fn delete_many(&mut self, table: &str, row_ids: Vec<RowId>) -> Result<Vec<OwnedRow>> {
        let id = self.db.table_id(table)?;
        self.reject_after_truncate(id)?;
        let snap = self.read;
        let handle = self.db.table(table)?;
        let t = handle.lock();
        let mut pre_images = Vec::with_capacity(row_ids.len());
        for row_id in &row_ids {
            if let Some(row) = t.get(*row_id, snap) {
                pre_images.push(owned_row_from_map(row.columns));
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
        let id = self.db.table_id(table)?;
        self.reject_after_truncate(id)?;
        let snap = self.read;
        let handle = self.db.table(table)?;
        let t = handle.lock();
        let mut post_images = Vec::with_capacity(updates.len());
        let mut staged = Vec::with_capacity(updates.len() * 2);
        for (old_id, new_cells) in updates {
            let old_row = t
                .get(old_id, snap)
                .ok_or_else(|| MongrelError::NotFound(format!("row {old_id:?} not found")))?;
            let merged = merge_cells(old_row.columns.into_iter().collect(), new_cells);
            post_images.push(owned_row_from_cells(&merged));
            staged.push((id, Staged::Delete(old_id)));
            staged.push((id, Staged::Put(merged)));
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
        let id = self.db.table_id(table)?;
        self.reject_after_truncate(id)?;
        match (self.existing_pk_row(table, &insert_cells)?, action) {
            (None, _) => {
                let assigned = self
                    .db
                    .table(table)?
                    .lock()
                    .fill_auto_inc(&mut insert_cells)?;
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
                let merged = merge_cells(old_row.columns.clone(), update_cells);
                if columns_equal(&old_row.columns, &merged) {
                    return Ok(UpsertResult {
                        action: UpsertActionKind::Unchanged,
                        row: old_row,
                        auto_inc: None,
                    });
                }
                let row = owned_row_from_cells(&merged);
                self.staging.push((id, Staged::Delete(old_id)));
                self.staging.push((id, Staged::Put(merged)));
                Ok(UpsertResult {
                    action: UpsertActionKind::Updated,
                    row,
                    auto_inc: None,
                })
            }
        }
    }

    pub fn truncate(&mut self, table: &str) -> Result<()> {
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

    fn existing_pk_row(
        &self,
        table: &str,
        cells: &[(u16, Value)],
    ) -> Result<Option<(RowId, OwnedRow)>> {
        let handle = self.db.table(table)?;
        let t = handle.lock();
        let Some(pk_col) = t.schema().primary_key() else {
            return Ok(None);
        };
        let Some((_, pk_value)) = cells.iter().find(|(id, _)| *id == pk_col.id) else {
            return Ok(None);
        };
        if matches!(pk_value, Value::Null) {
            return Ok(None);
        }
        let Some(row_id) = t.lookup_pk(&pk_value.encode_key()) else {
            return Ok(None);
        };
        Ok(t.get(row_id, self.read)
            .map(|row| (row_id, owned_row_from_map(row.columns))))
    }

    /// Commit: durably seal the staging under one epoch and publish it.
    pub fn commit(self) -> Result<Epoch> {
        self.db.commit_transaction_with_external_states(
            self.txn_id,
            self.read.epoch,
            self.staging,
            self.external_states,
            self.external_trigger_bridge,
        )
    }

    /// Rollback: discard staging. Nothing is appended to the WAL.
    pub fn rollback(self) {
        // Dropping `self` is enough — staging lives only in memory.
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
    Put(crate::memtable::Row),
    Delete(RowId),
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
        }
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
}

/// Transaction isolation level. MongrelDB defaults to `Snapshot` (SI).
///
/// - `Snapshot`: reads see a consistent snapshot taken at `begin`; writes
///   conflict on first-committer-wins for overlapping keys.
/// - `ReadCommitted`: each read sees the latest committed epoch (no stale
///   reads within a long transaction). Weaker than Snapshot but avoids
///   aborts from read-write conflicts.
/// - `Serializable`: same as Snapshot under MongrelDB's optimistic model —
///   the conflict index already detects write-skew. Explicitly marked so
///   callers can request the strongest level without behavioral surprise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    #[default]
    Snapshot,
    ReadCommitted,
    Serializable,
}
