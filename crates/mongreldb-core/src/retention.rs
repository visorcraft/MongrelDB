//! Global snapshot-retention registry for a multi-table `Database`.
//!
//! Readers register the epoch they pin via [`SnapshotRegistry::register`] and
//! the returned [`SnapshotGuard`] deregisters on drop. Garbage collection of
//! superseded runs, dropped tables, and recycled WAL segments is gated on
//! [`SnapshotRegistry::min_active`]: nothing whose retire epoch is still
//! observable by an open reader may be physically deleted.

use crate::epoch::Epoch;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Set of transaction ids that are currently spilling into `_txn/<txn_id>/`
/// (spec §8.5, review fix #14). A large transaction registers its id before
/// writing its pending run and holds the [`SpillGuard`] through publish; GC
/// consults [`ActiveSpills::is_active`] and never deletes a live txn's pending
/// dir (deleting it would lose the spill run / fail the commit).
#[derive(Default)]
pub struct ActiveSpills {
    inner: Mutex<HashSet<u64>>,
}

impl ActiveSpills {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashSet::new()),
        }
    }

    /// Register `txn_id` as actively spilling. The id stays protected from GC
    /// until the returned guard is dropped.
    pub fn register(self: &Arc<Self>, txn_id: u64) -> SpillGuard {
        self.inner.lock().insert(txn_id);
        SpillGuard {
            registry: Arc::clone(self),
            txn_id,
        }
    }

    /// Whether `txn_id`'s pending `_txn/` dir is currently in use.
    pub fn is_active(&self, txn_id: u64) -> bool {
        self.inner.lock().contains(&txn_id)
    }

    /// Whether no transaction is currently spilling. Used to gate WAL-segment GC.
    pub fn is_idle(&self) -> bool {
        self.inner.lock().is_empty()
    }

    fn release(&self, txn_id: u64) {
        self.inner.lock().remove(&txn_id);
    }
}

/// RAII handle that deregisters its txn id from [`ActiveSpills`] on drop.
pub struct SpillGuard {
    registry: Arc<ActiveSpills>,
    txn_id: u64,
}

impl Drop for SpillGuard {
    fn drop(&mut self) {
        self.registry.release(self.txn_id);
    }
}

/// Refcounted multiset of pinned reader epochs. Tracks the lowest live snapshot
/// so the reaper can decide what is safe to reclaim.
#[derive(Default)]
pub struct SnapshotRegistry {
    /// `epoch -> count` of currently-pinned reader snapshots.
    live: Mutex<BTreeMap<u64, u64>>,
    /// Number of prior commit epochs compaction must keep queryable. Zero
    /// preserves the current-state-only behavior.
    history_epochs: AtomicU64,
    /// Earliest epoch known to have been protected since history was enabled.
    history_start: AtomicU64,
}

impl SnapshotRegistry {
    pub fn new() -> Self {
        Self {
            live: Mutex::new(BTreeMap::new()),
            history_epochs: AtomicU64::new(0),
            history_start: AtomicU64::new(0),
        }
    }

    pub fn configure_history(&self, epochs: u64, start_epoch: Epoch) {
        self.history_start.store(start_epoch.0, Ordering::Release);
        self.history_epochs.store(epochs, Ordering::Release);
    }

    pub fn history_config(&self) -> (u64, Epoch) {
        (
            self.history_epochs.load(Ordering::Acquire),
            Epoch(self.history_start.load(Ordering::Acquire)),
        )
    }

    /// Earliest epoch guaranteed available by the rolling history window.
    /// Returns `None` when historical retention is disabled.
    pub fn history_floor(&self, visible: Epoch) -> Option<Epoch> {
        let (epochs, start) = self.history_config();
        (epochs > 0).then(|| Epoch(start.0.max(visible.0.saturating_sub(epochs))))
    }

    /// Register a pinned reader at `epoch`. The snapshot stays retained until
    /// the returned guard is dropped.
    pub fn register(&self, epoch: Epoch) -> SnapshotGuard<'_> {
        let mut live = self.live.lock();
        *live.entry(epoch.0).or_insert(0) += 1;
        SnapshotGuard {
            registry: self,
            epoch,
        }
    }

    /// The lowest currently-live pinned epoch. If no reader is active, returns
    /// `visible` (nothing older than the reader watermark is retained, so GC is
    /// free to reclaim anything strictly below it).
    pub fn min_active(&self, visible: Epoch) -> Epoch {
        match self.live.lock().keys().next().copied() {
            Some(min) => Epoch(min),
            None => visible,
        }
    }

    /// The lowest currently-pinned epoch, or `None` when no reader is active.
    /// Unlike [`Self::min_active`] this distinguishes "no readers" from "a reader
    /// pinned at `visible`", which compaction needs: with no readers it may drop
    /// superseded versions/tombstones freely, but a pin (even at `visible`) must
    /// preserve the version that reader can still see.
    pub fn min_pinned(&self) -> Option<Epoch> {
        self.live.lock().keys().next().copied().map(Epoch)
    }

    fn release(&self, epoch: Epoch) {
        let mut live = self.live.lock();
        if let Some(count) = live.get_mut(&epoch.0) {
            *count -= 1;
            if *count == 0 {
                live.remove(&epoch.0);
            }
        }
    }
}

/// RAII handle that deregisters its epoch from the registry on drop.
pub struct SnapshotGuard<'r> {
    registry: &'r SnapshotRegistry,
    epoch: Epoch,
}

impl Drop for SnapshotGuard<'_> {
    fn drop(&mut self) {
        self.registry.release(self.epoch);
    }
}

/// An owned, shareable guard (across threads / `Arc`) for snapshots that must
/// outlive a borrow of the registry, e.g. inside an `Arc<Database>`.
pub struct OwnedSnapshotGuard {
    registry: Arc<SnapshotRegistry>,
    epoch: Epoch,
}

impl OwnedSnapshotGuard {
    /// The epoch this guard pins.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }
}

impl Drop for OwnedSnapshotGuard {
    fn drop(&mut self) {
        self.registry.release(self.epoch);
    }
}

impl SnapshotRegistry {
    /// Register a pinned reader and return an owned (clonable-handle) guard
    /// that does not borrow the registry.
    pub fn register_owned(self: &Arc<Self>, epoch: Epoch) -> OwnedSnapshotGuard {
        {
            let mut live = self.live.lock();
            *live.entry(epoch.0).or_insert(0) += 1;
        }
        OwnedSnapshotGuard {
            registry: Arc::clone(self),
            epoch,
        }
    }
}

/// The version-retention pin sources of spec §10.3 (S1C-004). A version may be
/// reclaimed only when it is older than the oldest pin of **every** source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PinSource {
    /// Oldest active transaction/read snapshot (MVCC readers). Projected from
    /// [`SnapshotRegistry`] and the table-local pin set in diagnostics.
    TransactionSnapshot,
    /// Configured rolling history-retention window. Projected from
    /// [`SnapshotRegistry::history_floor`] in diagnostics.
    HistoryRetention,
    /// Oldest backup / point-in-time-recovery pin.
    BackupPitr,
    /// Oldest replication-follower requirement.
    Replication,
    /// Oldest cursor / immutable read-generation pin.
    ReadGeneration,
    /// Oldest online index build still reading historical versions.
    OnlineIndexBuild,
}

impl PinSource {
    /// Every source, in stable declaration order (diagnostics iteration).
    pub const ALL: [PinSource; 6] = [
        PinSource::TransactionSnapshot,
        PinSource::HistoryRetention,
        PinSource::BackupPitr,
        PinSource::Replication,
        PinSource::ReadGeneration,
        PinSource::OnlineIndexBuild,
    ];

    /// Stable lowercase label for logs and diagnostics output.
    pub fn label(self) -> &'static str {
        match self {
            PinSource::TransactionSnapshot => "transaction_snapshot",
            PinSource::HistoryRetention => "history_retention",
            PinSource::BackupPitr => "backup_pitr",
            PinSource::Replication => "replication",
            PinSource::ReadGeneration => "read_generation",
            PinSource::OnlineIndexBuild => "online_index_build",
        }
    }
}

/// Diagnostics view of one active pin source (S1C-004): the oldest epoch it
/// holds, when the oldest of its pins was taken, and how many pins are live.
/// `held_since` is `None` for projected sources ([`PinSource::TransactionSnapshot`]
/// and [`PinSource::HistoryRetention`]) whose epochs come from the
/// [`SnapshotRegistry`] rather than from registered [`PinGuard`]s.
#[derive(Debug, Clone)]
pub struct PinInfo {
    pub source: PinSource,
    pub oldest_epoch: Epoch,
    pub held_since: Option<Instant>,
    pub pin_count: usize,
}

/// Every currently-active pin source, one entry per source (S1C-004
/// diagnostics). Empty when nothing pins version reclamation.
#[derive(Debug, Clone, Default)]
pub struct PinsReport {
    pub pins: Vec<PinInfo>,
}

impl PinsReport {
    /// The oldest epoch held by any source — the version-reclamation floor.
    pub fn oldest_epoch(&self) -> Option<Epoch> {
        self.pins.iter().map(|pin| pin.oldest_epoch).min()
    }

    /// The entry for `source`, if that source currently holds a pin.
    pub fn get(&self, source: PinSource) -> Option<&PinInfo> {
        self.pins.iter().find(|pin| pin.source == source)
    }

    pub fn is_empty(&self) -> bool {
        self.pins.is_empty()
    }

    pub fn len(&self) -> usize {
        self.pins.len()
    }

    /// Merge a projected epoch for `source` (a floor derived from another
    /// retention mechanism rather than a registered guard). The reported
    /// oldest epoch only moves down; `held_since`/`pin_count` keep describing
    /// the registered guards, if any.
    pub fn record_projection(&mut self, source: PinSource, epoch: Epoch) {
        match self.pins.iter_mut().find(|pin| pin.source == source) {
            Some(info) => info.oldest_epoch = info.oldest_epoch.min(epoch),
            None => self.pins.push(PinInfo {
                source,
                oldest_epoch: epoch,
                held_since: None,
                pin_count: 0,
            }),
        }
    }
}

struct PinEntry {
    source: PinSource,
    epoch: Epoch,
    held_since: Instant,
}

/// Unified registry of version-retention pins (S1C-004).
///
/// Every subsystem that needs historical versions to survive reclamation —
/// backup/PITR, replication, cursors/read generations, online index builds —
/// registers a guard here via [`PinRegistry::pin`]. GC consults
/// [`PinRegistry::oldest_pinned`] in addition to the [`SnapshotRegistry`]
/// (transaction snapshots) and the configured history window, and
/// [`PinRegistry::report`] exposes every active source for diagnostics.
/// Guards are cheap (one map insertion) and deregister on drop.
#[derive(Default)]
pub struct PinRegistry {
    /// `pin id -> entry`; ids are monotonic so equal epochs stay distinguishable.
    pins: Mutex<BTreeMap<u64, PinEntry>>,
    next_id: AtomicU64,
}

impl PinRegistry {
    pub fn new() -> Self {
        Self {
            pins: Mutex::new(BTreeMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    /// Register a pin of `source` at `epoch`. Versions at or below `epoch`
    /// stay retained until the returned guard (and every clone of it) drops.
    pub fn pin(self: &Arc<Self>, source: PinSource, epoch: Epoch) -> PinGuard {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.pins.lock().insert(
            id,
            PinEntry {
                source,
                epoch,
                held_since: Instant::now(),
            },
        );
        PinGuard {
            registry: Arc::clone(self),
            id,
            source,
            epoch,
        }
    }

    /// The oldest epoch held by any registered pin, or `None` when no pin is
    /// active (GC is then gated only by snapshots/history).
    pub fn oldest_pinned(&self) -> Option<Epoch> {
        self.pins.lock().values().map(|entry| entry.epoch).min()
    }

    /// The oldest epoch held by pins of `source`.
    pub fn oldest_for(&self, source: PinSource) -> Option<Epoch> {
        self.pins
            .lock()
            .values()
            .filter(|entry| entry.source == source)
            .map(|entry| entry.epoch)
            .min()
    }

    /// Diagnostics: one entry per source with at least one live pin.
    pub fn report(&self) -> PinsReport {
        let pins = self.pins.lock();
        let mut by_source: BTreeMap<PinSource, PinInfo> = BTreeMap::new();
        for entry in pins.values() {
            by_source
                .entry(entry.source)
                .and_modify(|info| {
                    info.oldest_epoch = info.oldest_epoch.min(entry.epoch);
                    info.held_since = match info.held_since {
                        Some(since) => Some(since.min(entry.held_since)),
                        None => Some(entry.held_since),
                    };
                    info.pin_count += 1;
                })
                .or_insert(PinInfo {
                    source: entry.source,
                    oldest_epoch: entry.epoch,
                    held_since: Some(entry.held_since),
                    pin_count: 1,
                });
        }
        PinsReport {
            pins: by_source.into_values().collect(),
        }
    }

    fn release(&self, id: u64) {
        self.pins.lock().remove(&id);
    }
}

/// RAII handle that deregisters its pin from the [`PinRegistry`] on drop.
/// Not [`Clone`]: share it behind an `Arc` when several owners must keep the
/// same pin alive (the pin releases when the last `Arc` drops).
pub struct PinGuard {
    registry: Arc<PinRegistry>,
    id: u64,
    source: PinSource,
    epoch: Epoch,
}

impl PinGuard {
    pub fn source(&self) -> PinSource {
        self.source
    }

    pub fn epoch(&self) -> Epoch {
        self.epoch
    }
}

impl Drop for PinGuard {
    fn drop(&mut self) {
        self.registry.release(self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_tracks_min_active_snapshot() {
        let r = SnapshotRegistry::new();
        assert_eq!(r.min_active(Epoch(10)), Epoch(10));
        let g1 = r.register(Epoch(5));
        let g2 = r.register(Epoch(8));
        assert_eq!(r.min_active(Epoch(10)), Epoch(5));
        drop(g1);
        assert_eq!(r.min_active(Epoch(10)), Epoch(8));
        drop(g2);
        assert_eq!(r.min_active(Epoch(10)), Epoch(10));
    }

    #[test]
    fn retention_refcounts_duplicate_epochs() {
        let r = SnapshotRegistry::new();
        let a = r.register(Epoch(3));
        let b = r.register(Epoch(3));
        assert_eq!(r.min_active(Epoch(9)), Epoch(3));
        drop(a);
        assert_eq!(r.min_active(Epoch(9)), Epoch(3));
        drop(b);
        assert_eq!(r.min_active(Epoch(9)), Epoch(9));
    }

    #[test]
    fn pin_registry_tracks_oldest_epoch_per_source() {
        let registry = Arc::new(PinRegistry::new());
        assert_eq!(registry.oldest_pinned(), None);
        let backup = registry.pin(PinSource::BackupPitr, Epoch(7));
        let replication = registry.pin(PinSource::Replication, Epoch(4));
        assert_eq!(registry.oldest_pinned(), Some(Epoch(4)));
        assert_eq!(registry.oldest_for(PinSource::BackupPitr), Some(Epoch(7)));
        assert_eq!(registry.oldest_for(PinSource::ReadGeneration), None);
        drop(replication);
        assert_eq!(registry.oldest_pinned(), Some(Epoch(7)));
        drop(backup);
        assert_eq!(registry.oldest_pinned(), None);
    }

    #[test]
    fn pin_registry_report_lists_every_active_source_once() {
        let registry = Arc::new(PinRegistry::new());
        let mut guards = Vec::new();
        for (offset, source) in PinSource::ALL.into_iter().enumerate() {
            guards.push(registry.pin(source, Epoch(offset as u64 + 2)));
        }
        // A second, newer pin of one source must not duplicate its entry.
        guards.push(registry.pin(PinSource::BackupPitr, Epoch(50)));

        let report = registry.report();
        assert_eq!(report.len(), PinSource::ALL.len());
        for (offset, source) in PinSource::ALL.into_iter().enumerate() {
            let info = report.get(source).expect("source listed");
            assert_eq!(info.oldest_epoch, Epoch(offset as u64 + 2));
            assert!(
                info.held_since.is_some(),
                "registered pins carry a timestamp"
            );
        }
        assert_eq!(report.get(PinSource::BackupPitr).unwrap().pin_count, 2);
        assert_eq!(report.oldest_epoch(), Some(Epoch(2)));

        drop(guards);
        assert!(registry.report().is_empty());
    }

    #[test]
    fn pins_report_projection_only_lowers_the_floor() {
        let registry = Arc::new(PinRegistry::new());
        let guard = registry.pin(PinSource::ReadGeneration, Epoch(9));
        let mut report = registry.report();
        // A projection older than the registered pin lowers the source floor;
        // a newer one is ignored. Projections carry no timestamp or count.
        report.record_projection(PinSource::ReadGeneration, Epoch(4));
        report.record_projection(PinSource::ReadGeneration, Epoch(12));
        report.record_projection(PinSource::TransactionSnapshot, Epoch(6));
        let info = report.get(PinSource::ReadGeneration).unwrap();
        assert_eq!(info.oldest_epoch, Epoch(4));
        assert_eq!(info.pin_count, 1, "projection is not a registered guard");
        assert!(info.held_since.is_some());
        let projected = report.get(PinSource::TransactionSnapshot).unwrap();
        assert_eq!(projected.oldest_epoch, Epoch(6));
        assert_eq!(projected.pin_count, 0);
        assert!(projected.held_since.is_none());
        assert_eq!(report.oldest_epoch(), Some(Epoch(4)));
        drop(guard);
    }
}
