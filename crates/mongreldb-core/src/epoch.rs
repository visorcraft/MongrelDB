use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};

/// A monotonically increasing commit number. Every successful commit bumps the
/// epoch. Readers pin a [`Snapshot`] and only observe data with
/// `committed_epoch <= snapshot.epoch`. This is the value that tags every cache
/// entry and every WAL record, giving correctness-by-construction cache
/// invalidation.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct Epoch(pub u64);

impl Epoch {
    pub const ZERO: Epoch = Epoch(0);

    #[inline]
    pub fn next(self) -> Epoch {
        Epoch(self.0.wrapping_add(1))
    }
}

/// A point-in-time read view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    pub epoch: Epoch,
}

impl Snapshot {
    #[inline]
    pub fn at(epoch: Epoch) -> Self {
        Self { epoch }
    }

    /// A cache page tagged with `page_epoch` is visible to this snapshot iff
    /// the page was committed at or before the snapshot.
    #[inline]
    pub fn observes(&self, page_epoch: Epoch) -> bool {
        page_epoch <= self.epoch
    }
}

/// Atomic source of commit epochs. Shared by the writer and all readers.
#[derive(Debug, Default)]
pub struct EpochClock {
    current: AtomicU64,
}

impl EpochClock {
    pub fn new(start: u64) -> Self {
        Self {
            current: AtomicU64::new(start),
        }
    }

    #[inline]
    pub fn now(&self) -> Epoch {
        Epoch(self.current.load(Ordering::Acquire))
    }

    #[inline]
    pub fn snapshot(&self) -> Snapshot {
        Snapshot::at(self.now())
    }

    /// Advance to the next epoch and return it. Called once per committed txn.
    #[inline]
    pub fn bump(&self) -> Epoch {
        Epoch(self.current.fetch_add(1, Ordering::AcqRel) + 1)
    }

    /// Move the clock forward to at least `e` (used during recovery). Never
    /// moves backward.
    pub fn advance_to(&self, e: Epoch) {
        loop {
            let cur = self.current.load(Ordering::Acquire);
            if e.0 <= cur {
                return;
            }
            let _ = self
                .current
                .compare_exchange(cur, e.0, Ordering::AcqRel, Ordering::Acquire);
        }
    }
}

/// Dual-counter epoch authority for a multi-table `Database`. `assigned` is the
/// commit-order ticket (advanced the instant a txn is sequenced); `visible` is
/// the in-order reader watermark, advanced only once a committed txn has been
/// fully published. Readers pin `visible`; writers reserve `assigned`. The two
/// counters decouple "what order commits happened" from "what is safe to read".
///
/// ## Epoch abandonment
///
/// An assigned epoch that will never be published (because the operation that
/// reserved it failed before applying any writes) can be **abandoned** via
/// [`Self::abandon`]. The in-order watermark advances past abandoned epochs
/// just as it does for published ones — readers never observe data at an
/// abandoned epoch because no data was committed there.
#[derive(Debug)]
pub struct EpochAuthority {
    assigned: AtomicU64,
    visible: AtomicU64,
    /// Epochs that have finished publishing but cannot yet be absorbed into the
    /// `visible` watermark because an earlier assigned epoch is still in flight.
    /// Shared across every commit path (cross-table transactions, single-table
    /// `Table::commit`, and DDL) so the watermark only ever advances in assigned
    /// order regardless of which path or thread completes first.
    pending: Mutex<BTreeSet<u64>>,
    /// Epochs that were assigned but will never be published (the operation
    /// failed after `bump_assigned` but before any writes were applied). The
    /// watermark skips these just as it would a published epoch.
    abandoned: Mutex<BTreeSet<u64>>,
}

impl EpochAuthority {
    pub fn new(start: u64) -> Self {
        Self {
            assigned: AtomicU64::new(start),
            visible: AtomicU64::new(start),
            pending: Mutex::new(BTreeSet::new()),
            abandoned: Mutex::new(BTreeSet::new()),
        }
    }

    /// The reader watermark: the highest epoch fully published and visible.
    #[inline]
    pub fn visible(&self) -> Epoch {
        Epoch(self.visible.load(Ordering::Acquire))
    }

    /// Reserve the next commit-order ticket. Returns the assigned epoch.
    #[inline]
    pub fn bump_assigned(&self) -> Epoch {
        Epoch(self.assigned.fetch_add(1, Ordering::AcqRel) + 1)
    }

    /// Advance the reader watermark to `e`, monotonically. Stale (lower) values
    /// are ignored so out-of-order publish completions never regress visibility.
    pub fn publish_visible(&self, e: Epoch) {
        let mut cur = self.visible.load(Ordering::Acquire);
        while e.0 > cur {
            match self
                .visible
                .compare_exchange_weak(cur, e.0, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }

    /// Publish a fully-committed `e` and advance `visible` in assigned order:
    /// `e` becomes visible only once it and every prior assigned epoch have
    /// also been published (or abandoned). Because the pending set lives on the
    /// shared authority, interleaved commits from different paths/threads can
    /// never make the watermark jump past an epoch whose writes are not yet
    /// applied. Each assigned epoch must call either this or [`Self::abandon`]
    /// exactly once.
    pub fn publish_in_order(&self, e: Epoch) {
        let mut pending = self.pending.lock();
        let mut abandoned = self.abandoned.lock();
        pending.insert(e.0);
        let mut vis = self.visible.load(Ordering::Acquire);
        // Advance past both published and abandoned epochs. An abandoned epoch
        // has no committed data, so readers correctly skip it.
        loop {
            let next = vis + 1;
            if pending.remove(&next) {
                vis = next;
            } else if abandoned.remove(&next) {
                vis = next;
            } else {
                break;
            }
        }
        // `vis` only ever moves forward here; `publish_visible` is monotonic.
        drop(pending);
        drop(abandoned);
        self.publish_visible(Epoch(vis));
    }

    /// Abandon an assigned epoch that will never be published (the operation
    /// failed after `bump_assigned` but before any writes were applied). The
    /// in-order watermark advances past it just as for a published epoch — no
    /// data exists at an abandoned epoch, so readers correctly skip it.
    pub fn abandon(&self, e: Epoch) {
        let mut pending = self.pending.lock();
        let mut abandoned = self.abandoned.lock();
        // Remove from pending if it was already published (idempotent).
        pending.remove(&e.0);
        // Mark as abandoned so publish_in_order can skip it.
        abandoned.insert(e.0);
        // Try to advance the watermark past any now-resolvable holes.
        let mut vis = self.visible.load(Ordering::Acquire);
        loop {
            let next = vis + 1;
            if pending.remove(&next) {
                vis = next;
            } else if abandoned.remove(&next) {
                vis = next;
            } else {
                break;
            }
        }
        drop(pending);
        drop(abandoned);
        self.publish_visible(Epoch(vis));
    }

    /// Recovery: set both counters to `e` (e.g. the max committed epoch on open).
    pub fn set_recovered(&self, e: Epoch) {
        self.assigned.store(e.0, Ordering::Release);
        self.visible.store(e.0, Ordering::Release);
    }

    /// Monotonically raise both counters to at least `e` (used while opening
    /// tables that share one authority — each advances the shared clock to its
    /// own manifest epoch; the max wins).
    pub fn advance_recovered(&self, e: Epoch) {
        raise_to(&self.assigned, e.0);
        raise_to(&self.visible, e.0);
    }

    /// The current `assigned` counter (test/diagnostic use).
    #[inline]
    pub fn assigned(&self) -> Epoch {
        Epoch(self.assigned.load(Ordering::Acquire))
    }
}

fn raise_to(cell: &AtomicU64, target: u64) {
    let mut cur = cell.load(Ordering::Acquire);
    while target > cur {
        match cell.compare_exchange_weak(cur, target, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => break,
            Err(actual) => cur = actual,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_visibility_is_monotonic() {
        let clock = EpochClock::new(10);
        assert_eq!(clock.now(), Epoch(10));
        let s = clock.snapshot();
        assert!(s.observes(Epoch(10)));
        assert!(!s.observes(Epoch(11)));
        let next = clock.bump();
        assert_eq!(next, Epoch(11));
        assert!(clock.snapshot().observes(Epoch(11)));
    }

    #[test]
    fn epoch_authority_assigned_and_visible_advance_in_order() {
        let a = EpochAuthority::new(0);
        assert_eq!(a.visible(), Epoch(0));
        let e1 = a.bump_assigned();
        let e2 = a.bump_assigned();
        assert_eq!((e1, e2), (Epoch(1), Epoch(2)));
        a.publish_visible(Epoch(2));
        assert_eq!(a.visible(), Epoch(2));
        a.publish_visible(Epoch(1));
        assert_eq!(a.visible(), Epoch(2));
    }

    #[test]
    fn publish_in_order_gates_until_gap_filled() {
        let a = EpochAuthority::new(0);
        let e1 = a.bump_assigned();
        let e2 = a.bump_assigned();
        let e3 = a.bump_assigned();
        assert_eq!((e1, e2, e3), (Epoch(1), Epoch(2), Epoch(3)));

        // A later epoch finishing first must NOT advance the watermark past the
        // still-in-flight earlier epochs.
        a.publish_in_order(e3);
        assert_eq!(a.visible(), Epoch(0), "e3 cannot be visible before e1/e2");
        a.publish_in_order(e2);
        assert_eq!(a.visible(), Epoch(0), "e2 still gated on e1");

        // Filling the gap drains everything consecutively in one shot.
        a.publish_in_order(e1);
        assert_eq!(a.visible(), Epoch(3));
    }

    #[test]
    fn abandon_unblocks_watermark() {
        let a = EpochAuthority::new(0);
        let e1 = a.bump_assigned();
        let e2 = a.bump_assigned(); // this one will be abandoned (operation failed)
        let e3 = a.bump_assigned();

        // e3 is published but can't advance past the e2 hole.
        a.publish_in_order(e3);
        assert_eq!(a.visible(), Epoch(0), "e3 gated on e1 and e2");

        // e1 is published — advances to 1, but still gated on e2.
        a.publish_in_order(e1);
        assert_eq!(a.visible(), Epoch(1), "e1 visible but e2 hole remains");

        // e2 is abandoned (operation failed) — watermark should advance past it
        // and drain e3.
        a.abandon(e2);
        assert_eq!(a.visible(), Epoch(3), "abandoning e2 drains e3");
    }

    #[test]
    fn abandon_before_publish_of_later_epochs() {
        let a = EpochAuthority::new(0);
        let e1 = a.bump_assigned();
        let e2 = a.bump_assigned();
        let e3 = a.bump_assigned();

        // Abandon e1 first (before e2/e3 are published). The watermark advances
        // past e1 (no data there), but stops at e2 (not yet published/abandoned).
        a.abandon(e1);
        assert_eq!(a.visible(), Epoch(1), "e1 abandoned, watermark at 1, e2 pending");

        // Now publish e2 — should advance to 2.
        a.publish_in_order(e2);
        assert_eq!(a.visible(), Epoch(2));

        // Publish e3 — advances to 3.
        a.publish_in_order(e3);
        assert_eq!(a.visible(), Epoch(3));
    }
}
