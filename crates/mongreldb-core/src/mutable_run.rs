//! Mutable run tier — the LSM layer between the skip-list memtable and the
//! immutable `.sr` sorted runs (Phase 11.1).
//!
//! A flush drains the live memtable into this in-memory tier instead of
//! immediately writing a new sorted run. The tier is a [`crate::pma::Pma`] keyed
//! by the composite `(RowId, Epoch)` version key, so it stays sorted (the
//! natural order `RunWriter` consumes) and absorbs further flushes in place with
//! amortized `O(log² n)` inserts — exactly the "cache-oblivious mutable sorted
//! run" described in §2. Only once the tier crosses a byte watermark does it
//! spill to an immutable sorted run on disk, coalescing many small flushes into
//! one larger run (fewer runs ⇒ fewer reader merges ⇒ faster scans).
//!
//! MVCC semantics mirror [`crate::memtable::Memtable`]: every version is kept,
//! keyed by `(RowId, Epoch)`; a snapshot read returns the newest version with
//! `epoch <= snapshot`. The tier is purely in-memory and rebuilds from WAL
//! replay on reopen, so it carries no on-disk state of its own.

use crate::epoch::Epoch;
use crate::memtable::Row;
use crate::pma::Pma;
use crate::rowid::RowId;
use std::collections::BTreeMap;

/// Composite version key — identical to the memtable's, so all versions of one
/// `RowId` sort contiguously in ascending-epoch order.
type VersionKey = (RowId, Epoch);

/// The PMA-backed mutable run tier. Holds flushed-but-not-yet-spilled rows in
/// sorted `(RowId, Epoch)` order.
pub struct MutableRun {
    pma: Pma<VersionKey, Row>,
    byte_size: u64,
}

impl Default for MutableRun {
    fn default() -> Self {
        Self::new()
    }
}

impl MutableRun {
    pub fn new() -> Self {
        Self {
            pma: Pma::new(),
            byte_size: 0,
        }
    }

    /// Fold drained memtable rows (already ascending by `(RowId, Epoch)`) into
    /// the tier via one bulk merge + re-spread — far cheaper than per-element
    /// inserts, which would cluster at the tail on sorted input.
    pub fn insert_many(&mut self, rows: Vec<Row>) {
        let batch: Vec<(VersionKey, Row)> = rows
            .into_iter()
            .map(|r| {
                self.byte_size = self.byte_size.saturating_add(r.estimated_bytes());
                ((r.row_id, r.committed_epoch), r)
            })
            .collect();
        self.pma.extend_sorted(batch);
    }

    /// Number of stored versions.
    pub fn len(&self) -> usize {
        self.pma.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pma.is_empty()
    }

    /// Approximate bytes held — the spill-threshold signal.
    pub fn approx_bytes(&self) -> u64 {
        self.byte_size
    }

    /// Newest version of `row_id` with `epoch <= snapshot` (including
    /// tombstones), mirroring `Memtable::get_version`. Returns `None` if no
    /// version is visible. Seeks straight to `row_id`'s versions via the
    /// PMA's gappy binary search instead of scanning from the front.
    pub fn get_version(&self, row_id: RowId, snapshot_epoch: Epoch) -> Option<(Epoch, Row)> {
        let mut best: Option<(Epoch, Row)> = None;
        for ((rid, ep), row) in self.pma.iter_from(&(row_id, Epoch::ZERO)) {
            if *rid != row_id {
                break;
            }
            if *ep <= snapshot_epoch {
                best = Some((*ep, row.clone()));
            }
        }
        best
    }

    /// Newest visible version per `RowId` at `snapshot` (including tombstones),
    /// ascending by `RowId` — mirroring `Memtable::visible_versions`.
    pub fn visible_versions(&self, snapshot_epoch: Epoch) -> Vec<Row> {
        let mut by_row: BTreeMap<RowId, Row> = BTreeMap::new();
        for ((rid, ep), row) in self.pma.iter() {
            if *ep <= snapshot_epoch {
                by_row.insert(*rid, row.clone()); // ascending ⇒ newest wins
            }
        }
        by_row.into_values().collect()
    }

    /// Drain every version in ascending `(RowId, Epoch)` order — the order
    /// `RunWriter::write` requires when spilling to an immutable run.
    pub fn drain_sorted(&mut self) -> Vec<Row> {
        let out: Vec<Row> = self
            .pma
            .drain_sorted()
            .into_iter()
            .map(|(_, r)| r)
            .collect();
        self.byte_size = 0;
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Value;

    fn row(id: u64, epoch: u64, v: i64) -> Row {
        Row::new(RowId(id), Epoch(epoch)).with_column(1, Value::Int64(v))
    }

    fn tomb(id: u64, epoch: u64) -> Row {
        Row {
            row_id: RowId(id),
            committed_epoch: Epoch(epoch),
            columns: std::collections::HashMap::new(),
            deleted: true,
        }
    }

    fn int_of(r: &Row) -> i64 {
        match r.columns.get(&1) {
            Some(Value::Int64(x)) => *x,
            _ => panic!("expected Int64 column"),
        }
    }

    #[test]
    fn get_version_returns_newest_visible() {
        let mut mr = MutableRun::new();
        mr.insert_many(vec![row(1, 1, 10), row(1, 3, 30), row(1, 9, 90)]);
        // Snapshot before the 9 version sees the 3 version.
        assert_eq!(int_of(&mr.get_version(RowId(1), Epoch(5)).unwrap().1), 30);
        // Latest snapshot sees the newest.
        assert_eq!(int_of(&mr.get_version(RowId(1), Epoch(9)).unwrap().1), 90);
        // No version at/before epoch 0.
        assert!(mr.get_version(RowId(1), Epoch(0)).is_none());
        // Missing row.
        assert!(mr.get_version(RowId(2), Epoch(100)).is_none());
    }

    #[test]
    fn tombstone_is_returned_as_a_version() {
        let mut mr = MutableRun::new();
        mr.insert_many(vec![row(1, 1, 10), tomb(1, 2)]);
        let v = mr.get_version(RowId(1), Epoch(5)).unwrap().1;
        assert!(v.deleted);
        // Before the tombstone the live version is visible.
        let v0 = mr.get_version(RowId(1), Epoch(1)).unwrap().1;
        assert!(!v0.deleted);
    }

    #[test]
    fn visible_versions_dedups_to_newest_ascending() {
        let mut mr = MutableRun::new();
        mr.insert_many(vec![
            row(3, 1, 30),
            row(1, 1, 10),
            row(2, 9, 20), // future relative to snapshot 5
            row(1, 3, 11), // newer version of row 1
            row(3, 2, 31),
        ]);
        let out = mr.visible_versions(Epoch(5));
        let got: Vec<(u64, i64)> = out.iter().map(|r| (r.row_id.0, int_of(r))).collect();
        assert_eq!(got, vec![(1, 11), (3, 31)], "row 2 hidden, newest wins");
    }

    #[test]
    fn drain_sorted_is_ascending_version_order_and_empties() {
        let mut mr = MutableRun::new();
        mr.insert_many(vec![row(3, 1, 0), row(1, 2, 0), row(1, 1, 0), row(2, 1, 0)]);
        let out = mr.drain_sorted();
        let keys: Vec<(u64, u64)> = out
            .iter()
            .map(|r| (r.row_id.0, r.committed_epoch.0))
            .collect();
        assert_eq!(keys, vec![(1, 1), (1, 2), (2, 1), (3, 1)]);
        assert!(mr.is_empty());
        assert_eq!(mr.approx_bytes(), 0);
    }

    #[test]
    fn many_inserts_stay_queryable() {
        let mut mr = MutableRun::new();
        let mut rows = Vec::new();
        for i in 0..500u64 {
            rows.push(row(i, 1, i as i64));
        }
        mr.insert_many(rows);
        assert_eq!(mr.len(), 500);
        for i in 0..500u64 {
            assert_eq!(
                int_of(&mr.get_version(RowId(i), Epoch(1)).unwrap().1),
                i as i64
            );
        }
        assert!(mr.approx_bytes() > 0);
    }
}
