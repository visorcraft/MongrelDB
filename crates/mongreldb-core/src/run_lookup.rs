//! RowId → run-set lookup directory (TODO §1.2).
//!
//! Derived, rebuildable, **never** authoritative. Missing, stale, corrupt, or
//! incomplete directory state falls back to the existing range-scan path in
//! `Table::get`. The directory is rebuilt from sorted-run system columns and
//! published atomically alongside the manifest, so a torn publication never
//! leaves an inconsistent directory visible.
//!
//! On-disk shape (planned):
//! - delta-encoded `RowId` keys (varint);
//! - for each key, a newest-first list of `RunLocator` ordinals;
//! - a fingerprint of the exact active run-set + schema/index generation that
//!   produced the directory; rejected on reopen if the active manifest
//!   diverges.
//!
//! The current file is a skeleton that defines the data layout; the rebuild,
//! checkpoint, and tombstone-stripping code lands in PR B.

use std::collections::BTreeMap;

use crate::epoch::Epoch;
use crate::rowid::RowId;
use mongreldb_types::hlc::HlcTimestamp;

/// What one run-level posting says about a `RowId`: enough metadata to
/// conservatively decide whether the run can contain a version visible to a
/// supplied snapshot, without opening the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunLocator {
    /// Stable identifier of the run, matched against the manifest's `RunRef`.
    pub run_id: u128,
    pub min_epoch: Epoch,
    pub max_epoch: Epoch,
    /// `Some` when the run carries HLC-stamped rows.
    pub min_hlc: Option<HlcTimestamp>,
    pub max_hlc: Option<HlcTimestamp>,
    /// `true` when the run also contains legacy unstamped (epoch-only) rows.
    pub contains_unstamped_versions: bool,
}

/// A complete, directly-consultable view of the directory for a single
/// `RowId`. Returned by [`RunLookupDirectory::locate`].
#[derive(Debug, Clone, Default)]
pub struct RunLocatorList {
    pub locators: Vec<RunLocator>,
}

impl RunLocatorList {
    pub fn is_empty(&self) -> bool {
        self.locators.is_empty()
    }

    pub fn len(&self) -> usize {
        self.locators.len()
    }
}

/// Per-`RowId` posting list. Backed by a `BTreeMap` for the in-memory
/// representation; the on-disk checkpoint uses delta-encoded `RowId` keys.
#[derive(Debug, Default, Clone)]
pub struct RunLookupDirectory {
    /// `(RowId -> newest-first RunLocator list)`.
    postings: BTreeMap<RowId, RunLocatorList>,
    /// Fingerprint of the active run-set + schema/index generation that
    /// produced this directory. `None` for an in-memory rebuild not yet
    /// checkpointed.
    fingerprint: Option<u64>,
}

impl RunLookupDirectory {
    /// Construct an empty directory with no fingerprint.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Look up the candidate run locators for `row_id`. Returns an empty list
    /// when the row has no postings (caller may treat this as a miss).
    pub fn locate(&self, row_id: RowId) -> &RunLocatorList {
        static EMPTY: RunLocatorList = RunLocatorList {
            locators: Vec::new(),
        };
        self.postings.get(&row_id).unwrap_or(&EMPTY)
    }

    /// Insert or replace a `RunLocator` for `row_id`. Locators are kept
    /// newest-first; the most recent write wins for ties.
    pub fn insert(&mut self, row_id: RowId, locator: RunLocator) {
        let entry = self.postings.entry(row_id).or_default();
        // Newest-first: locate the first locator whose `max_epoch`/`max_hlc`
        // is older than the new one and insert before it. Falls back to
        // append when the new locator is the oldest.
        let pos = entry
            .locators
            .iter()
            .position(|existing| locator_is_newer(locator, *existing));
        match pos {
            Some(idx) => entry.locators.insert(idx, locator),
            None => entry.locators.push(locator),
        }
    }

    /// Drop every locator whose `run_id` no longer exists in the active
    /// manifest. Returns the number of removed postings.
    pub fn retain_active_runs(&mut self, active_runs: &[u128]) -> usize {
        let mut removed = 0;
        for list in self.postings.values_mut() {
            let before = list.locators.len();
            list.locators.retain(|l| active_runs.contains(&l.run_id));
            removed += before - list.locators.len();
        }
        removed
    }

    /// Set the fingerprint of the active run-set + schema/index generation.
    pub fn set_fingerprint(&mut self, fp: u64) {
        self.fingerprint = Some(fp);
    }

    pub fn fingerprint(&self) -> Option<u64> {
        self.fingerprint
    }

    /// Number of `(RowId, posting)` entries. Used for memory/footprint metrics.
    pub fn total_locators(&self) -> usize {
        self.postings.values().map(|l| l.locators.len()).sum()
    }

    /// Number of distinct `RowId` keys.
    pub fn key_count(&self) -> usize {
        self.postings.len()
    }

    /// Maximum number of locators for any single `RowId`.
    pub fn max_locators_per_row(&self) -> usize {
        self.postings
            .values()
            .map(|l| l.locators.len())
            .max()
            .unwrap_or(0)
    }

    /// Average number of locators per `RowId` (0 when the directory is empty).
    pub fn avg_locators_per_row(&self) -> f64 {
        if self.postings.is_empty() {
            return 0.0;
        }
        self.total_locators() as f64 / self.postings.len() as f64
    }
}

/// `true` when `newer` is strictly newer than `older` by either epoch or
/// HLC. Ties compare as `false` (preserve existing order).
fn locator_is_newer(newer: RunLocator, older: RunLocator) -> bool {
    if newer.max_epoch > older.max_epoch {
        return true;
    }
    if newer.max_epoch < older.max_epoch {
        return false;
    }
    match (newer.max_hlc, older.max_hlc) {
        (Some(a), Some(b)) => a > b,
        (Some(_), None) => true,
        (None, Some(_)) => false,
        (None, None) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hlc_from_phys(physical_micros: u64) -> HlcTimestamp {
        HlcTimestamp {
            physical_micros,
            logical: 0,
            node_tiebreaker: 0,
        }
    }

    fn loc(run_id: u128, epoch: u64, hlc: Option<u64>) -> RunLocator {
        RunLocator {
            run_id,
            min_epoch: Epoch(epoch),
            max_epoch: Epoch(epoch),
            min_hlc: hlc.map(hlc_from_phys),
            max_hlc: hlc.map(hlc_from_phys),
            contains_unstamped_versions: hlc.is_none(),
        }
    }

    #[test]
    fn empty_directory_returns_empty_list() {
        let dir = RunLookupDirectory::empty();
        assert!(dir.locate(RowId(42)).is_empty());
        assert_eq!(dir.key_count(), 0);
    }

    #[test]
    fn insert_orders_locators_newest_first() {
        let mut dir = RunLookupDirectory::empty();
        dir.insert(RowId(1), loc(0xA, 1, None));
        dir.insert(RowId(1), loc(0xB, 3, None));
        dir.insert(RowId(1), loc(0xC, 2, None));
        let list = dir.locate(RowId(1));
        let ids: Vec<u128> = list.locators.iter().map(|l| l.run_id).collect();
        assert_eq!(ids, vec![0xB, 0xC, 0xA]);
    }

    #[test]
    fn retain_active_runs_drops_stale_locators() {
        let mut dir = RunLookupDirectory::empty();
        dir.insert(RowId(1), loc(10, 1, None));
        dir.insert(RowId(1), loc(20, 2, None));
        dir.insert(RowId(2), loc(20, 1, None));
        let removed = dir.retain_active_runs(&[10]);
        assert_eq!(removed, 2);
        assert!(dir.locate(RowId(1)).locators.iter().all(|l| l.run_id == 10));
        assert!(dir.locate(RowId(2)).is_empty());
    }

    #[test]
    fn hlc_tie_break_resolves_above_epoch_only() {
        let mut dir = RunLookupDirectory::empty();
        dir.insert(RowId(1), loc(0xA, 5, None));
        dir.insert(RowId(1), loc(0xB, 5, Some(100)));
        let list = dir.locate(RowId(1));
        assert_eq!(list.locators[0].run_id, 0xB, "HLC newer wins on tie");
    }

    #[test]
    fn memory_metrics_report_postings() {
        let mut dir = RunLookupDirectory::empty();
        dir.insert(RowId(1), loc(1, 1, None));
        dir.insert(RowId(1), loc(2, 2, None));
        dir.insert(RowId(2), loc(1, 1, None));
        assert_eq!(dir.total_locators(), 3);
        assert_eq!(dir.key_count(), 2);
        assert_eq!(dir.max_locators_per_row(), 2);
        assert!((dir.avg_locators_per_row() - 1.5).abs() < 1e-9);
    }
}
