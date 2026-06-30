//! Cross-table transactions on the shared WAL (spec §8.2, single-applier subset
//! — parallelism arrives in P3).
//!
//! A [`Transaction`] stages puts/deletes keyed by table; [`Transaction::commit`]
//! reserves a commit epoch from the shared authority, appends the staged data
//! records + a `TxnCommit` marker to the shared WAL, group-fsyncs, applies the
//! staging to each table's memtable + indexes at the commit epoch, persists the
//! per-table manifests, and publishes the visible watermark. Rollback (or a
//! dropped transaction) discards the staging and appends nothing durable.

use crate::database::Database;
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

/// An in-flight cross-table transaction. Holds a read snapshot taken at `begin`
/// and stages writes; nothing is durable or visible until [`Self::commit`].
pub struct Transaction<'db> {
    db: &'db Database,
    txn_id: u64,
    read: Snapshot,
    staging: Vec<(u64 /*table_id*/, Staged)>,
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
            _active: Some(guard),
        }
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

    /// Stage a delete of `row_id` on `table`.
    pub fn delete(&mut self, table: &str, row_id: RowId) -> Result<()> {
        let id = self.db.table_id(table)?;
        self.reject_after_truncate(id)?;
        self.staging.push((id, Staged::Delete(row_id)));
        Ok(())
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

    /// Commit: durably seal the staging under one epoch and publish it.
    pub fn commit(self) -> Result<Epoch> {
        self.db
            .commit_transaction(self.txn_id, self.read.epoch, self.staging)
    }

    /// Rollback: discard staging. Nothing is appended to the WAL.
    pub fn rollback(self) {
        // Dropping `self` is enough — staging lives only in memory.
    }
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
