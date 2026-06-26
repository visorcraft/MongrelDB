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
}
