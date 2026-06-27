use serde::{Deserialize, Serialize};
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
#[derive(Debug)]
pub struct EpochAuthority {
    assigned: AtomicU64,
    visible: AtomicU64,
}

impl EpochAuthority {
    pub fn new(start: u64) -> Self {
        Self {
            assigned: AtomicU64::new(start),
            visible: AtomicU64::new(start),
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
}
