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

/// Exact database epoch captured for maintenance that does not create a data
/// commit. This lets callers report the snapshot a maintenance operation used
/// without inventing a commit epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceReceipt {
    pub epoch: Epoch,
}

/// A point-in-time read view.
///
/// # Authority model (P0.5-T7)
///
/// **[`HlcTimestamp`](mongreldb_types::hlc::HlcTimestamp) is the sole
/// cluster-wide visibility authority** when present on a snapshot or row
/// version. Local [`Epoch`] is a per-core sequencing aid only — it must never
/// be treated as an equal authority for cross-replica / HLC-stamped data.
///
/// | Snapshot `commit_ts` | Row `commit_ts` | Visibility rule |
/// |----------------------|-----------------|-----------------|
/// | non-`ZERO` (`at_hlc`) | `Some(ts)`      | HLC: `ts <= snap.commit_ts` |
/// | `ZERO` (legacy)      | `Some(_)`       | **not visible** (no epoch fallback) |
/// | any                  | `None` (legacy) | epoch: `row_epoch <= snap.epoch` |
///
/// Prefer [`Self::at_hlc`] / [`Self::unbounded`] for product reads. Prefer
/// [`Self::at`] only for pure-legacy paths that never see HLC-stamped rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    /// Local sequencing watermark (aid only when HLC is active).
    pub epoch: Epoch,
    /// Sole cluster-wide visibility timestamp (HLC). `ZERO` means a legacy
    /// epoch-only snapshot for dual-model compatibility — not an equal
    /// authority for HLC-stamped row versions.
    pub commit_ts: mongreldb_types::hlc::HlcTimestamp,
}

impl Snapshot {
    #[inline]
    pub fn at(epoch: Epoch) -> Self {
        Self {
            epoch,
            commit_ts: mongreldb_types::hlc::HlcTimestamp::ZERO,
        }
    }

    /// Pin a snapshot with an explicit HLC commit timestamp (authoritative).
    #[inline]
    pub fn at_hlc(epoch: Epoch, commit_ts: mongreldb_types::hlc::HlcTimestamp) -> Self {
        Self { epoch, commit_ts }
    }

    /// Unbounded product/recovery snapshot: observes every epoch and every HLC.
    ///
    /// Prefer this over [`Self::at`]`(`[`Epoch`]`(u64::MAX))` once rows may carry
    /// HLC stamps — epoch-only snapshots intentionally hide HLC-stamped versions.
    #[inline]
    pub fn unbounded() -> Self {
        Self::at_hlc(Epoch(u64::MAX), mongreldb_types::hlc::HlcTimestamp::MAX)
    }

    /// Whether this snapshot uses HLC as the cluster-wide visibility authority.
    ///
    /// `false` means legacy epoch-only (`commit_ts == ZERO`): HLC-stamped rows
    /// are intentionally not observed.
    #[inline]
    pub fn uses_hlc_authority(&self) -> bool {
        self.commit_ts != mongreldb_types::hlc::HlcTimestamp::ZERO
    }

    /// A cache page tagged with `page_epoch` is visible to this snapshot iff
    /// the page was committed at or before the snapshot.
    ///
    /// Cache pages remain epoch-tagged during dual-model migration; row-version
    /// visibility must use [`Self::observes_version`] / [`Self::observes_row`].
    #[inline]
    pub fn observes(&self, page_epoch: Epoch) -> bool {
        page_epoch <= self.epoch
    }

    /// Row visibility under HLC-authoritative MVCC (P0.5-T7).
    ///
    /// See the type-level authority table. Alias of the same rule used by every
    /// product visibility path that has access to both stamps.
    #[inline]
    pub fn observes_version(
        &self,
        row_epoch: Epoch,
        row_commit_ts: Option<mongreldb_types::hlc::HlcTimestamp>,
    ) -> bool {
        match (self.uses_hlc_authority(), row_commit_ts) {
            (true, Some(ts)) => ts <= self.commit_ts,
            (false, Some(_)) => false,
            (_, None) => row_epoch <= self.epoch,
        }
    }

    /// Same as [`Self::observes_version`] — preferred name at call sites that
    /// reason about full row versions rather than cache pages.
    #[inline]
    pub fn observes_row(
        &self,
        row_epoch: Epoch,
        row_commit_ts: Option<mongreldb_types::hlc::HlcTimestamp>,
    ) -> bool {
        self.observes_version(row_epoch, row_commit_ts)
    }

    /// Whether version `a` is strictly newer than version `b` under HLC
    /// authority. When **both** carry HLC, HLC order wins; otherwise local
    /// epoch order is used (legacy / mixed).
    #[inline]
    pub fn version_is_newer(
        a_epoch: Epoch,
        a_commit_ts: Option<mongreldb_types::hlc::HlcTimestamp>,
        b_epoch: Epoch,
        b_commit_ts: Option<mongreldb_types::hlc::HlcTimestamp>,
    ) -> bool {
        match (a_commit_ts, b_commit_ts) {
            (Some(a), Some(b)) => a > b,
            _ => a_epoch > b_epoch,
        }
    }
}

/// Named HLC pin sources that form the version-GC floor (P0.5-T6).
///
/// Each field is the oldest HLC still required by that pin source. Sources that
/// are not currently pinning, or that only pin a local epoch without a durable
/// HLC projection, report [`HlcTimestamp::ZERO`](mongreldb_types::hlc::HlcTimestamp::ZERO).
///
/// The epoch-based floor ([`crate::engine::Table::version_gc_floor`]) remains
/// the physical reclamation gate until every pin source carries HLC; this
/// struct exposes the HLC projection for diagnostics and progressive cutover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcFloor {
    pub transaction_snapshot: mongreldb_types::hlc::HlcTimestamp,
    pub history_retention: mongreldb_types::hlc::HlcTimestamp,
    pub backup_pitr: mongreldb_types::hlc::HlcTimestamp,
    pub replication: mongreldb_types::hlc::HlcTimestamp,
    pub read_generation: mongreldb_types::hlc::HlcTimestamp,
    pub online_index_build: mongreldb_types::hlc::HlcTimestamp,
}

impl Default for GcFloor {
    fn default() -> Self {
        Self::ZERO
    }
}

impl GcFloor {
    /// No HLC pins reported by any source.
    pub const ZERO: Self = Self {
        transaction_snapshot: mongreldb_types::hlc::HlcTimestamp::ZERO,
        history_retention: mongreldb_types::hlc::HlcTimestamp::ZERO,
        backup_pitr: mongreldb_types::hlc::HlcTimestamp::ZERO,
        replication: mongreldb_types::hlc::HlcTimestamp::ZERO,
        read_generation: mongreldb_types::hlc::HlcTimestamp::ZERO,
        online_index_build: mongreldb_types::hlc::HlcTimestamp::ZERO,
    };

    /// Minimum non-`ZERO` HLC across all named sources, or `ZERO` when none
    /// report an HLC pin.
    #[inline]
    pub fn floor(&self) -> mongreldb_types::hlc::HlcTimestamp {
        [
            self.transaction_snapshot,
            self.history_retention,
            self.backup_pitr,
            self.replication,
            self.read_generation,
            self.online_index_build,
        ]
        .into_iter()
        .filter(|ts| *ts != mongreldb_types::hlc::HlcTimestamp::ZERO)
        .min()
        .unwrap_or(mongreldb_types::hlc::HlcTimestamp::ZERO)
    }

    /// Stable `(source_label, hlc)` pairs for diagnostics.
    pub fn sources(&self) -> [(&'static str, mongreldb_types::hlc::HlcTimestamp); 6] {
        [
            ("transaction_snapshot", self.transaction_snapshot),
            ("history_retention", self.history_retention),
            ("backup_pitr", self.backup_pitr),
            ("replication", self.replication),
            ("read_generation", self.read_generation),
            ("online_index_build", self.online_index_build),
        ]
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
    /// Highest epoch backed by a successfully published durable commit.
    /// Unlike `visible`, this never advances for an abandoned ticket.
    committed: AtomicU64,
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

/// Resolves an assigned epoch on every exit path. Successful publishers call
/// [`Self::disarm`] after [`EpochAuthority::publish_in_order`]; failed paths
/// abandon the ticket so later commits cannot stall behind an epoch hole.
pub(crate) struct EpochGuard<'a> {
    authority: &'a EpochAuthority,
    epoch: Epoch,
    armed: bool,
}

impl<'a> EpochGuard<'a> {
    pub(crate) fn new(authority: &'a EpochAuthority, epoch: Epoch) -> Self {
        Self {
            authority,
            epoch,
            armed: true,
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for EpochGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.authority.abandon(self.epoch);
        }
    }
}

impl EpochAuthority {
    pub fn new(start: u64) -> Self {
        Self {
            assigned: AtomicU64::new(start),
            visible: AtomicU64::new(start),
            committed: AtomicU64::new(start),
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
        raise_to(&self.committed, e.0);
        let mut pending = self.pending.lock();
        let mut abandoned = self.abandoned.lock();
        pending.insert(e.0);
        let mut vis = self.visible.load(Ordering::Acquire);
        // Advance past both published and abandoned epochs. An abandoned epoch
        // has no committed data, so readers correctly skip it.
        loop {
            let next = vis + 1;
            if pending.remove(&next) || abandoned.remove(&next) {
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
            if pending.remove(&next) || abandoned.remove(&next) {
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
        self.committed.store(e.0, Ordering::Release);
    }

    /// Monotonically raise both counters to at least `e` (used while opening
    /// tables that share one authority — each advances the shared clock to its
    /// own manifest epoch; the max wins).
    pub fn advance_recovered(&self, e: Epoch) {
        raise_to(&self.assigned, e.0);
        raise_to(&self.visible, e.0);
        raise_to(&self.committed, e.0);
    }

    /// The current `assigned` counter (test/diagnostic use).
    #[inline]
    pub fn assigned(&self) -> Epoch {
        Epoch(self.assigned.load(Ordering::Acquire))
    }

    /// Highest durable commit epoch. Abandoned assignment tickets are absent.
    #[inline]
    pub fn committed(&self) -> Epoch {
        Epoch(self.committed.load(Ordering::Acquire))
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
    use mongreldb_types::hlc::HlcTimestamp;

    fn hlc(physical_micros: u64) -> HlcTimestamp {
        HlcTimestamp {
            physical_micros,
            logical: 0,
            node_tiebreaker: 1,
        }
    }

    #[test]
    fn hlc_visibility_is_authoritative_when_stamped() {
        let early = hlc(100);
        let late = hlc(200);
        // High epoch, early HLC: only early-stamped versions are visible.
        let snap = Snapshot::at_hlc(Epoch(99), early);
        assert!(snap.uses_hlc_authority());
        assert!(snap.observes_version(Epoch(1), Some(early)));
        assert!(!snap.observes_version(Epoch(2), Some(late)));
        assert!(snap.observes_row(Epoch(1), Some(early)));
        assert!(!snap.observes_row(Epoch(2), Some(late)));
        // Low epoch, late HLC: late-stamped versions win even if epoch is lower
        // than a concurrent higher-epoch stamp that is still later in HLC order.
        let snap2 = Snapshot::at_hlc(Epoch(1), late);
        assert!(snap2.observes_version(Epoch(1), Some(early)));
        assert!(snap2.observes_version(Epoch(2), Some(late)));
    }

    #[test]
    fn snapshot_hlc_hides_later_commit_ts_even_if_epoch_higher() {
        let early = hlc(100);
        let late = hlc(200);
        // Snapshot pinned at early HLC with a *low* epoch budget still hides a
        // later-commit_ts row even when that row's epoch is below the pin.
        let snap = Snapshot::at_hlc(Epoch(10), early);
        assert!(!snap.observes_version(Epoch(5), Some(late)));
        assert!(snap.observes_version(Epoch(5), Some(early)));
        // Legacy epoch-only snapshot must not reclaim HLC-stamped rows via epoch.
        let legacy = Snapshot::at(Epoch(99));
        assert!(!legacy.uses_hlc_authority());
        assert!(!legacy.observes_version(Epoch(1), Some(early)));
        assert!(legacy.observes_version(Epoch(1), None));
    }

    #[test]
    fn version_is_newer_prefers_hlc_when_both_stamped() {
        let early = hlc(100);
        let late = hlc(200);
        // Later HLC is newer even with a lower local epoch.
        assert!(Snapshot::version_is_newer(
            Epoch(1),
            Some(late),
            Epoch(50),
            Some(early)
        ));
        assert!(!Snapshot::version_is_newer(
            Epoch(50),
            Some(early),
            Epoch(1),
            Some(late)
        ));
        // Mixed / legacy falls back to epoch order.
        assert!(Snapshot::version_is_newer(Epoch(3), None, Epoch(2), None));
        assert!(Snapshot::version_is_newer(
            Epoch(3),
            Some(early),
            Epoch(2),
            None
        ));
    }

    #[test]
    fn hlc_clock_order_matches_visibility_order() {
        let a = hlc(100);
        let b = HlcTimestamp {
            physical_micros: 100,
            logical: 1,
            node_tiebreaker: 1,
        };
        let c = HlcTimestamp {
            physical_micros: 100,
            logical: 1,
            node_tiebreaker: 2,
        };
        assert!(a < b && b < c);
        let snap = Snapshot::at_hlc(Epoch(1), b);
        assert!(snap.observes_row(Epoch(1), Some(a)));
        assert!(snap.observes_row(Epoch(1), Some(b)));
        assert!(!snap.observes_row(Epoch(1), Some(c)));
    }

    #[test]
    fn gc_floor_min_skips_zero_sources() {
        let mut floor = GcFloor::ZERO;
        assert_eq!(floor.floor(), HlcTimestamp::ZERO);
        floor.transaction_snapshot = hlc(300);
        floor.backup_pitr = hlc(100);
        floor.replication = hlc(200);
        assert_eq!(floor.floor(), hlc(100));
        assert_eq!(floor.sources().len(), 6);
    }

    /// P0.5-X5: excessive skew rejects timestamp allocation on the shared
    /// [`mongreldb_types::hlc::HlcClock`] API used by Database/Table commits.
    #[test]
    fn clock_skew_rejects_timestamp_allocation() {
        use mongreldb_types::hlc::HlcClock;
        use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
        use std::sync::Arc;
        use std::time::Duration;

        let wall = Arc::new(AtomicU64::new(10_000));
        let wall_src = {
            let wall = Arc::clone(&wall);
            Arc::new(move || wall.load(AtomicOrdering::Relaxed))
                as mongreldb_types::hlc::WallClockSource
        };
        let clock = HlcClock::with_time_source(1, Duration::from_micros(1_000), wall_src);
        let remote = HlcTimestamp {
            physical_micros: 15_000,
            logical: 0,
            node_tiebreaker: 2,
        };
        assert!(
            clock.observe(remote).is_err(),
            "5ms skew must exceed 1ms bound"
        );
        assert!(
            clock.now().is_err(),
            "further allocation stays rejected after skew trip"
        );
        // next_after is not skew-gated (recovery path); product commits use now().
        let _ = clock.next_after(HlcTimestamp::ZERO);
    }

    // P0.5-X7: bounded-staleness rejection of lagging replicas is tested in
    // mongreldb-consensus (`read::tests` + cluster integration), not here.

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
        assert_eq!(a.committed(), Epoch(3));
    }

    #[test]
    fn abandoned_latest_ticket_does_not_advance_commit_watermark() {
        let a = EpochAuthority::new(4);
        let abandoned = a.bump_assigned();
        a.abandon(abandoned);
        assert_eq!(a.visible(), Epoch(5));
        assert_eq!(a.committed(), Epoch(4));
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
        assert_eq!(
            a.visible(),
            Epoch(1),
            "e1 abandoned, watermark at 1, e2 pending"
        );

        // Now publish e2 — should advance to 2.
        a.publish_in_order(e2);
        assert_eq!(a.visible(), Epoch(2));

        // Publish e3 — advances to 3.
        a.publish_in_order(e3);
        assert_eq!(a.visible(), Epoch(3));
    }
}
