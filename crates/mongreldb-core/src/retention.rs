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
use std::sync::Arc;

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
}

impl SnapshotRegistry {
    pub fn new() -> Self {
        Self {
            live: Mutex::new(BTreeMap::new()),
        }
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
}
