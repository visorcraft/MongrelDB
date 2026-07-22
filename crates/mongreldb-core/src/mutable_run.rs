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
//! keyed by `(RowId, Epoch)`. Product visibility prefers HLC via
//! [`crate::epoch::Snapshot::observes_row`] / [`crate::epoch::Snapshot::version_is_newer`]
//! when versions carry `commit_ts` (P0.5-T3). Epoch-only APIs remain for
//! legacy dual-model call sites. The tier is purely in-memory and rebuilds
//! from WAL replay on reopen, so it carries no on-disk state of its own.

use crate::epoch::{Epoch, Snapshot};
use crate::memtable::Row;
use crate::pma::Pma;
use crate::rowid::RowId;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Composite version key — identical to the memtable's, so all versions of one
/// `RowId` sort contiguously in ascending-epoch order.
type VersionKey = (RowId, Epoch);

/// The PMA-backed mutable run tier. Holds flushed-but-not-yet-spilled rows in
/// sorted `(RowId, Epoch)` order.
#[derive(Clone)]
struct MutableRunSegment {
    pma: Pma<VersionKey, Row>,
    byte_size: u64,
}

#[derive(Clone)]
pub struct MutableRun {
    frozen: Arc<Vec<Arc<MutableRunSegment>>>,
    active: MutableRunSegment,
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
            frozen: Arc::new(Vec::new()),
            active: MutableRunSegment {
                pma: Pma::new(),
                byte_size: 0,
            },
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
                let bytes = r.estimated_bytes();
                self.byte_size = self.byte_size.saturating_add(bytes);
                self.active.byte_size = self.active.byte_size.saturating_add(bytes);
                ((r.row_id, r.committed_epoch), r)
            })
            .collect();
        self.active.pma.extend_sorted(batch);
    }

    /// Number of stored versions.
    pub fn len(&self) -> usize {
        self.active.pma.len()
            + self
                .frozen
                .iter()
                .map(|segment| segment.pma.len())
                .sum::<usize>()
    }

    pub fn is_empty(&self) -> bool {
        self.active.pma.is_empty() && self.frozen.is_empty()
    }

    /// Approximate bytes held — the spill-threshold signal.
    pub fn approx_bytes(&self) -> u64 {
        self.byte_size
    }

    /// Newest version of `row_id` with `epoch <= snapshot` (including
    /// tombstones). Legacy epoch-only entry point; prefer
    /// [`Self::get_version_at`] when the caller holds a full [`Snapshot`].
    pub fn get_version(&self, row_id: RowId, snapshot_epoch: Epoch) -> Option<(Epoch, Row)> {
        self.get_version_at(row_id, Snapshot::at(snapshot_epoch))
    }

    /// Newest version of `row_id` visible under `snapshot` (including
    /// tombstones), using HLC authority when stamps are present (P0.5-T3).
    /// Seeks to `row_id`'s versions via the PMA's gappy binary search.
    pub fn get_version_at(&self, row_id: RowId, snapshot: Snapshot) -> Option<(Epoch, Row)> {
        let mut best: Option<Row> = None;
        for pma in self
            .frozen
            .iter()
            .map(|segment| &segment.pma)
            .chain(std::iter::once(&self.active.pma))
        {
            // Under HLC authority, stamped rows may be visible regardless of
            // local epoch order — scan every version of this row_id.
            let end_epoch = if snapshot.uses_hlc_authority() {
                Epoch(u64::MAX)
            } else {
                snapshot.epoch
            };
            for ((rid, _epoch), row) in pma.iter_from(&(row_id, Epoch::ZERO)) {
                if *rid != row_id {
                    break;
                }
                // Stop early on pure-legacy scans once past the epoch pin.
                if !snapshot.uses_hlc_authority() && row.committed_epoch > end_epoch {
                    break;
                }
                if !snapshot.observes_row(row.committed_epoch, row.commit_ts) {
                    continue;
                }
                if best.as_ref().is_none_or(|current| {
                    Snapshot::version_is_newer(
                        row.committed_epoch,
                        row.commit_ts,
                        current.committed_epoch,
                        current.commit_ts,
                    )
                }) {
                    best = Some(row.clone());
                }
            }
        }
        best.map(|row| (row.committed_epoch, row))
    }

    /// Newest visible version per `RowId` at `snapshot` (including tombstones),
    /// ascending by `RowId`. Legacy epoch-only entry point; prefer
    /// [`Self::visible_versions_at`].
    pub fn visible_versions(&self, snapshot_epoch: Epoch) -> Vec<Row> {
        self.visible_versions_at(Snapshot::at(snapshot_epoch))
    }

    /// Newest visible version per `RowId` under a full [`Snapshot`], including
    /// tombstones. HLC-stamped versions use HLC order (P0.5-T3).
    pub fn visible_versions_at(&self, snapshot: Snapshot) -> Vec<Row> {
        let mut by_row: BTreeMap<RowId, Row> = BTreeMap::new();
        for pma in self
            .frozen
            .iter()
            .map(|segment| &segment.pma)
            .chain(std::iter::once(&self.active.pma))
        {
            for ((_rid, _epoch), row) in pma.iter() {
                if !snapshot.observes_row(row.committed_epoch, row.commit_ts) {
                    continue;
                }
                by_row
                    .entry(row.row_id)
                    .and_modify(|existing| {
                        if Snapshot::version_is_newer(
                            row.committed_epoch,
                            row.commit_ts,
                            existing.committed_epoch,
                            existing.commit_ts,
                        ) {
                            *existing = row.clone();
                        }
                    })
                    .or_insert_with(|| row.clone());
            }
        }
        by_row.into_values().collect()
    }

    pub(crate) fn seal(&mut self) {
        if self.active.pma.is_empty() {
            return;
        }
        let active = std::mem::replace(
            &mut self.active,
            MutableRunSegment {
                pma: Pma::new(),
                byte_size: 0,
            },
        );
        Arc::make_mut(&mut self.frozen).push(Arc::new(active));
        if self.frozen.len() >= crate::MAX_READ_GENERATION_LAYERS {
            self.consolidate();
        }
    }

    fn consolidate(&mut self) {
        let mut rows = self
            .frozen
            .iter()
            .flat_map(|segment| segment.pma.iter().map(|(_, row)| row.clone()))
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| (row.row_id, row.committed_epoch));
        let mut pma = Pma::new();
        pma.extend_sorted(
            rows.into_iter()
                .map(|row| ((row.row_id, row.committed_epoch), row))
                .collect(),
        );
        self.frozen = Arc::new(vec![Arc::new(MutableRunSegment {
            pma,
            byte_size: self.byte_size,
        })]);
    }

    #[cfg(test)]
    pub(crate) fn frozen_layer_count(&self) -> usize {
        self.frozen.len()
    }

    /// Drain every version in ascending `(RowId, Epoch)` order — the order
    /// `RunWriter::write` requires when spilling to an immutable run.
    pub fn drain_sorted(&mut self) -> Vec<Row> {
        let mut out = self
            .frozen
            .iter()
            .flat_map(|segment| segment.pma.iter().map(|(_, row)| row.clone()))
            .chain(self.active.pma.iter().map(|(_, row)| row.clone()))
            .collect::<Vec<_>>();
        out.sort_by_key(|row| (row.row_id, row.committed_epoch));
        self.frozen = Arc::new(Vec::new());
        self.active = MutableRunSegment {
            pma: Pma::new(),
            byte_size: 0,
        };
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
            commit_ts: None,
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
    fn sealed_generations_share_rows_and_consolidate() {
        let mut writer = MutableRun::new();
        for id in 0..crate::MAX_READ_GENERATION_LAYERS as u64 + 2 {
            writer.insert_many(vec![row(id, id + 1, id as i64)]);
            writer.seal();
        }
        assert!(writer.frozen_layer_count() < crate::MAX_READ_GENERATION_LAYERS);
        let generation = writer.clone();
        writer.insert_many(vec![row(99, 99, 99)]);
        assert!(generation.get_version(RowId(99), Epoch(99)).is_none());
        assert!(writer.get_version(RowId(99), Epoch(99)).is_some());
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

    fn hlc(physical_micros: u64) -> mongreldb_types::hlc::HlcTimestamp {
        mongreldb_types::hlc::HlcTimestamp {
            physical_micros,
            logical: 0,
            node_tiebreaker: 1,
        }
    }

    fn hlc_row(id: u64, epoch: u64, ts: mongreldb_types::hlc::HlcTimestamp, v: i64) -> Row {
        Row::new_with_hlc(RowId(id), Epoch(epoch), ts).with_column(1, Value::Int64(v))
    }

    #[test]
    fn hlc_visibility_is_authoritative_when_stamped() {
        let mut mr = MutableRun::new();
        let early = hlc(100);
        let late = hlc(200);
        mr.insert_many(vec![
            hlc_row(1, 1, early, 1),
            hlc_row(1, 2, late, 2),
        ]);
        let snap = Snapshot::at_hlc(Epoch(99), early);
        let versions = mr.visible_versions_at(snap);
        assert_eq!(versions.len(), 1);
        assert_eq!(int_of(&versions[0]), 1);
        assert_eq!(
            int_of(&mr.get_version_at(RowId(1), snap).unwrap().1),
            1
        );
        let snap2 = Snapshot::at_hlc(Epoch(1), late);
        assert_eq!(int_of(&mr.visible_versions_at(snap2)[0]), 2);
        assert_eq!(
            int_of(&mr.get_version_at(RowId(1), snap2).unwrap().1),
            2
        );
    }

    #[test]
    fn snapshot_hlc_hides_later_commit_ts_even_if_epoch_higher() {
        let mut mr = MutableRun::new();
        let early = hlc(100);
        let late = hlc(200);
        // Lower epoch, later HLC would win under epoch-only newest-of-visible;
        // HLC authority must hide it under an early pin.
        mr.insert_many(vec![
            hlc_row(1, 1, late, 99),
            hlc_row(1, 50, early, 1),
        ]);
        let snap = Snapshot::at_hlc(Epoch(99), early);
        let versions = mr.visible_versions_at(snap);
        assert_eq!(versions.len(), 1);
        assert_eq!(int_of(&versions[0]), 1);
        assert_eq!(versions[0].commit_ts, Some(early));
        assert_eq!(
            int_of(&mr.get_version_at(RowId(1), snap).unwrap().1),
            1
        );
    }

    #[test]
    fn epoch_only_snapshot_does_not_observe_hlc_stamped_rows() {
        let mut mr = MutableRun::new();
        mr.insert_many(vec![
            hlc_row(1, 1, hlc(50), 1),
            row(2, 1, 2),
        ]);
        let legacy = Snapshot::at(Epoch(99));
        let versions = mr.visible_versions_at(legacy);
        assert_eq!(versions.len(), 1, "only pure-legacy row is visible");
        assert_eq!(versions[0].row_id, RowId(2));
        assert!(versions[0].commit_ts.is_none());
        assert!(mr.get_version_at(RowId(1), legacy).is_none());
        assert!(mr.get_version_at(RowId(2), legacy).is_some());
    }
}
