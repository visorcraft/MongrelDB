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
use crate::epoch::Snapshot;
use crate::error::Result;
use crate::memtable::Value;
use crate::rowid::RowId;

/// One staged mutation against a named table.
pub(crate) enum Staged {
    Put(Vec<(u16, Value)>),
    Delete(RowId),
}

/// An in-flight cross-table transaction. Holds a read snapshot taken at `begin`
/// and stages writes; nothing is durable or visible until [`Self::commit`].
pub struct Transaction<'db> {
    db: &'db Database,
    txn_id: u64,
    read: Snapshot,
    staging: Vec<(u64 /*table_id*/, Staged)>,
}

impl<'db> Transaction<'db> {
    pub(crate) fn new(db: &'db Database, txn_id: u64, read: Snapshot) -> Self {
        Self {
            db,
            txn_id,
            read,
            staging: Vec::new(),
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
    /// transaction never consumes ids.
    pub fn put(&mut self, table: &str, cells: Vec<(u16, Value)>) -> Result<()> {
        let id = self.db.table_id(table)?;
        self.staging.push((id, Staged::Put(cells)));
        Ok(())
    }

    /// Stage a delete of `row_id` on `table`.
    pub fn delete(&mut self, table: &str, row_id: RowId) -> Result<()> {
        let id = self.db.table_id(table)?;
        self.staging.push((id, Staged::Delete(row_id)));
        Ok(())
    }

    /// Commit: durably seal the staging under one epoch and publish it.
    pub fn commit(self) -> Result<crate::epoch::Epoch> {
        self.db.commit_transaction(self.txn_id, self.staging)
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
}
