//! Hybrid Logical Clock timestamps, the node clock, and the legacy-to-HLC
//! migration model (spec section 8).
//!
//! [`HlcTimestamp`] is the commit/visibility timestamp of the target MVCC
//! model. Ordering is lexicographic by
//! `(physical_micros, logical, node_tiebreaker)`, which the derived
//! `PartialOrd`/`Ord` implement via field declaration order.
//!
//! [`HlcClock`] implements the section 8.2 clock rules: physical time may
//! move backward but returned timestamps never do, a received timestamp
//! advances the local clock, and clock skew is monitored. Once the maximum
//! observed skew exceeds the configured limit, timestamp allocation through
//! [`HlcClock::now`] and [`HlcClock::observe`] fails closed with
//! [`ClockSkewError`].
//!
//! [`MigrationWatermark`] carries the section 8.4 migration state. The stored
//! format is always explicit (`mvcc_format_version` plus the watermark); it
//! is never inferred from byte length.

use core::cmp::Ordering;
use core::fmt;
use core::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A hybrid-logical-clock timestamp (spec section 8.1).
#[derive(
    Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct HlcTimestamp {
    /// Physical wall-clock component in microseconds since the Unix epoch.
    pub physical_micros: u64,
    /// Logical counter, bumped when physical time does not advance.
    pub logical: u32,
    /// Node tiebreaker so equal physical+logical values order deterministically.
    pub node_tiebreaker: u32,
}

impl HlcTimestamp {
    /// The smallest possible timestamp.
    pub const ZERO: Self = Self {
        physical_micros: 0,
        logical: 0,
        node_tiebreaker: 0,
    };
}

impl fmt::Display for HlcTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}.{}",
            self.physical_micros, self.logical, self.node_tiebreaker
        )
    }
}

impl fmt::Debug for HlcTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// Error returned when parsing a textual [`HlcTimestamp`] fails.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HlcParseError {
    /// The text was not `<physical_micros>.<logical>.<node_tiebreaker>`.
    #[error(
        "invalid HLC timestamp `{0}`: expected `<physical_micros>.<logical>.<node_tiebreaker>`"
    )]
    InvalidFormat(String),
    /// One of the three dot-separated components was not an unsigned integer.
    #[error("invalid HLC timestamp `{0}`: component `{1}` is not an unsigned integer")]
    InvalidComponent(String, String),
}

impl FromStr for HlcTimestamp {
    type Err = HlcParseError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        fn parse_component<T: FromStr>(text: &str, component: &str) -> Result<T, HlcParseError> {
            component
                .parse::<T>()
                .map_err(|_| HlcParseError::InvalidComponent(text.to_owned(), component.to_owned()))
        }

        let mut parts = text.split('.');
        let (Some(physical), Some(logical), Some(node), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(HlcParseError::InvalidFormat(text.to_owned()));
        };
        Ok(Self {
            physical_micros: parse_component::<u64>(text, physical)?,
            logical: parse_component::<u32>(text, logical)?,
            node_tiebreaker: parse_component::<u32>(text, node)?,
        })
    }
}

/// Error returned when clock skew exceeds the configured maximum (spec
/// section 8.2): excessive skew rejects timestamp allocation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("clock skew of {observed:?} exceeds the configured maximum {maximum:?}")]
pub struct ClockSkewError {
    /// Largest skew observed so far (high-water mark, never decreases).
    pub observed: Duration,
    /// Configured maximum acceptable skew.
    pub maximum: Duration,
}

/// Injectable wall-clock source: microseconds since the Unix epoch.
pub type WallClockSource = Arc<dyn Fn() -> u64 + Send + Sync>;

/// Mutable state guarded by the [`HlcClock`] mutex.
#[derive(Debug, Clone, Copy)]
struct ClockState {
    /// Physical component of the last timestamp the clock produced.
    physical_micros: u64,
    /// Logical component of the last timestamp the clock produced.
    logical: u32,
    /// High-water mark of `|remote.physical_micros - local physical|` in
    /// microseconds. Never decreases; while it exceeds the configured
    /// `max_skew`, timestamp allocation is rejected.
    max_observed_skew_micros: u64,
}

/// A thread-safe hybrid logical clock (spec section 8.2).
///
/// Every timestamp handed out by [`Self::now`], [`Self::observe`], and
/// [`Self::next_after`] is strictly greater than every timestamp the same
/// clock handed out before, even when the physical wall clock regresses:
/// when physical time does not advance, the logical counter is bumped
/// instead. The logical counter saturates at `u32::MAX` rather than moving
/// backward; physical time advancing again resets it.
///
/// The wall-clock source is injected at construction so tests fully control
/// time; `node_tiebreaker` is attached to every returned timestamp.
pub struct HlcClock {
    state: Mutex<ClockState>,
    wall: WallClockSource,
    node_tiebreaker: u32,
    max_skew: Duration,
}

impl HlcClock {
    /// Creates a clock reading the system wall clock.
    pub fn new(node_tiebreaker: u32, max_skew: Duration) -> Self {
        Self::with_time_source(node_tiebreaker, max_skew, Arc::new(system_time_micros))
    }

    /// Creates a clock with an injected wall-clock source.
    pub fn with_time_source(
        node_tiebreaker: u32,
        max_skew: Duration,
        wall: WallClockSource,
    ) -> Self {
        Self {
            state: Mutex::new(ClockState {
                physical_micros: 0,
                logical: 0,
                max_observed_skew_micros: 0,
            }),
            wall,
            node_tiebreaker,
            max_skew,
        }
    }

    /// The node tiebreaker attached to every timestamp this clock returns.
    pub fn node_tiebreaker(&self) -> u32 {
        self.node_tiebreaker
    }

    /// The configured maximum acceptable clock skew.
    pub fn max_skew(&self) -> Duration {
        self.max_skew
    }

    /// High-water mark of the skew between received timestamps and the local
    /// physical clock (spec section 8.2 skew monitoring).
    pub fn max_observed_skew(&self) -> Duration {
        let state = self.state.lock().expect("HLC clock state poisoned");
        Duration::from_micros(state.max_observed_skew_micros)
    }

    /// Allocates a fresh timestamp.
    ///
    /// Never moves backward: if the physical wall clock has not advanced past
    /// the last produced timestamp, the logical counter is bumped instead.
    ///
    /// # Errors
    ///
    /// Returns [`ClockSkewError`] once excessive skew has been observed.
    pub fn now(&self) -> Result<HlcTimestamp, ClockSkewError> {
        let wall = (self.wall)();
        let mut state = self.state.lock().expect("HLC clock state poisoned");
        self.reject_excessive_skew(&state)?;
        Self::advance(&mut state, wall);
        Ok(self.stamp(&state))
    }

    /// Advances the local clock past `remote` and returns the new timestamp
    /// (spec section 8.2: a received timestamp advances the local clock).
    ///
    /// The skew between `remote.physical_micros` and the local physical clock
    /// is folded into [`Self::max_observed_skew`].
    ///
    /// # Errors
    ///
    /// Returns [`ClockSkewError`] when the skew of `remote` (or of any earlier
    /// observation) exceeds the configured maximum; the local clock is left
    /// unchanged in that case.
    pub fn observe(&self, remote: HlcTimestamp) -> Result<HlcTimestamp, ClockSkewError> {
        let wall = (self.wall)();
        let mut state = self.state.lock().expect("HLC clock state poisoned");

        let skew_micros = remote.physical_micros.abs_diff(wall);
        state.max_observed_skew_micros = state.max_observed_skew_micros.max(skew_micros);
        self.reject_excessive_skew(&state)?;

        let local = (state.physical_micros, state.logical);
        let physical = wall.max(local.0).max(remote.physical_micros);
        let logical = if physical == local.0 && physical == remote.physical_micros {
            local.1.max(remote.logical).saturating_add(1)
        } else if physical == local.0 {
            local.1.saturating_add(1)
        } else if physical == remote.physical_micros {
            remote.logical.saturating_add(1)
        } else {
            0
        };
        state.physical_micros = physical;
        state.logical = logical;
        Ok(self.stamp(&state))
    }

    /// Returns a timestamp strictly greater than `minimum`, advancing the
    /// local clock when necessary.
    ///
    /// Does not consult skew state: [`Self::now`] and [`Self::observe`] are
    /// the fail-closed allocation paths.
    pub fn next_after(&self, minimum: HlcTimestamp) -> HlcTimestamp {
        let wall = (self.wall)();
        let mut state = self.state.lock().expect("HLC clock state poisoned");
        Self::advance(&mut state, wall);
        let mut candidate = self.stamp(&state);
        if candidate <= minimum {
            let (physical, logical) = if minimum.logical < u32::MAX {
                (minimum.physical_micros, minimum.logical + 1)
            } else {
                (minimum.physical_micros.saturating_add(1), 0)
            };
            if physical > state.physical_micros {
                state.physical_micros = physical;
                state.logical = logical;
            } else {
                state.logical = state.logical.max(logical);
            }
            candidate = self.stamp(&state);
        }
        candidate
    }

    /// Commit timestamp strictly greater than every participant read/write
    /// timestamp (spec section 8.2).
    pub fn commit_timestamp(
        &self,
        participants: impl IntoIterator<Item = HlcTimestamp>,
    ) -> HlcTimestamp {
        let maximum = participants.into_iter().max().unwrap_or(HlcTimestamp::ZERO);
        self.next_after(maximum)
    }

    /// The standard HLC tick: adopt the wall clock when it is ahead,
    /// otherwise bump the logical counter.
    fn advance(state: &mut ClockState, wall: u64) {
        if wall > state.physical_micros {
            state.physical_micros = wall;
            state.logical = 0;
        } else {
            state.logical = state.logical.saturating_add(1);
        }
    }

    fn reject_excessive_skew(&self, state: &ClockState) -> Result<(), ClockSkewError> {
        let observed = Duration::from_micros(state.max_observed_skew_micros);
        if observed > self.max_skew {
            Err(ClockSkewError {
                observed,
                maximum: self.max_skew,
            })
        } else {
            Ok(())
        }
    }

    fn stamp(&self, state: &ClockState) -> HlcTimestamp {
        HlcTimestamp {
            physical_micros: state.physical_micros,
            logical: state.logical,
            node_tiebreaker: self.node_tiebreaker,
        }
    }
}

/// Microseconds since the Unix epoch according to the system wall clock.
/// Saturates at `u64::MAX`; a clock before the epoch reads as zero.
fn system_time_micros() -> u64 {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros())
        .unwrap_or(0);
    u64::try_from(micros).unwrap_or(u64::MAX)
}

/// Version stamp stored on row versions during the legacy-to-HLC migration
/// (spec section 8.4).
///
/// Comparison semantics are defined relative to an explicit migration
/// watermark: all `LegacyEpoch` values sort before the watermark and new HLC
/// values sort at or after it. The format is always explicit; it is never
/// inferred from byte length.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub enum StoredVersionStamp {
    /// Pre-migration logical epoch counter.
    LegacyEpoch(u64),
    /// Post-migration hybrid logical clock timestamp.
    Hlc(HlcTimestamp),
}

/// Durable migration state for the legacy-to-HLC cut-over (spec section 8.4).
///
/// The database stores both fields. The format is always explicit; it is
/// never inferred from byte length.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct MigrationWatermark {
    /// Explicit MVCC storage format version (see [`Self::FORMAT_LEGACY_EPOCH`]
    /// and [`Self::FORMAT_HLC`]).
    pub mvcc_format_version: u32,
    /// Cut-over timestamp: all legacy epochs sort before it, all new HLC
    /// timestamps sort at or after it.
    pub watermark: HlcTimestamp,
}

impl MigrationWatermark {
    /// `mvcc_format_version` while row versions carry
    /// [`StoredVersionStamp::LegacyEpoch`].
    pub const FORMAT_LEGACY_EPOCH: u32 = 1;
    /// `mvcc_format_version` once row versions carry
    /// [`StoredVersionStamp::Hlc`].
    pub const FORMAT_HLC: u32 = 2;

    /// Total order over stored version stamps during migration: legacy epochs
    /// order numerically among themselves, every legacy epoch sorts before
    /// every HLC timestamp, and HLC timestamps keep their natural order.
    pub fn cmp_stamps(a: &StoredVersionStamp, b: &StoredVersionStamp) -> Ordering {
        match (a, b) {
            (StoredVersionStamp::LegacyEpoch(x), StoredVersionStamp::LegacyEpoch(y)) => x.cmp(y),
            (StoredVersionStamp::LegacyEpoch(_), StoredVersionStamp::Hlc(_)) => Ordering::Less,
            (StoredVersionStamp::Hlc(_), StoredVersionStamp::LegacyEpoch(_)) => Ordering::Greater,
            (StoredVersionStamp::Hlc(x), StoredVersionStamp::Hlc(y)) => x.cmp(y),
        }
    }

    /// How a stamp orders against the watermark: every legacy epoch sorts
    /// before it; HLC timestamps sort at or after it (a pre-watermark HLC
    /// timestamp cannot occur once the watermark is enforced, so it clamps to
    /// [`Ordering::Equal`]).
    pub fn cmp_stamp_to_watermark(&self, stamp: &StoredVersionStamp) -> Ordering {
        match stamp {
            StoredVersionStamp::LegacyEpoch(_) => Ordering::Less,
            StoredVersionStamp::Hlc(ts) => ts.cmp(&self.watermark).max(Ordering::Equal),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    /// Deterministic, manually advanced wall-clock source.
    struct ManualWall {
        micros: Arc<AtomicU64>,
    }

    impl ManualWall {
        fn new(micros: u64) -> Self {
            Self {
                micros: Arc::new(AtomicU64::new(micros)),
            }
        }

        fn source(&self) -> WallClockSource {
            let micros = Arc::clone(&self.micros);
            Arc::new(move || micros.load(AtomicOrdering::Relaxed))
        }

        fn set(&self, micros: u64) {
            self.micros.store(micros, AtomicOrdering::Relaxed);
        }
    }

    fn ts(physical_micros: u64, logical: u32, node_tiebreaker: u32) -> HlcTimestamp {
        HlcTimestamp {
            physical_micros,
            logical,
            node_tiebreaker,
        }
    }

    #[test]
    fn lexicographic_ordering() {
        assert!(ts(1, 0, 0) < ts(2, 0, 0));
        assert!(ts(1, 0, 0) < ts(1, 1, 0));
        assert!(ts(1, 0, 0) < ts(1, 0, 1));
        assert!(ts(1, 1, 0) < ts(1, 1, 1));
        assert!(HlcTimestamp::ZERO < ts(0, 0, 1));
    }

    #[test]
    fn now_never_moves_backward_when_physical_time_regresses() {
        let wall = ManualWall::new(1_000_000);
        let clock = HlcClock::with_time_source(7, Duration::from_secs(60), wall.source());

        let first = clock.now().unwrap();
        assert_eq!(first, ts(1_000_000, 0, 7));

        wall.set(500_000); // physical time jumps backward
        let second = clock.now().unwrap();
        assert!(second > first);
        assert_eq!(second, ts(1_000_000, 1, 7));

        wall.set(100_000); // and further backward
        let third = clock.now().unwrap();
        assert!(third > second);
        assert_eq!(third, ts(1_000_000, 2, 7));

        wall.set(2_000_000); // physical time advances again
        let fourth = clock.now().unwrap();
        assert!(fourth > third);
        assert_eq!(fourth, ts(2_000_000, 0, 7));
    }

    #[test]
    fn concurrent_now_is_unique_and_ordered() {
        let wall = ManualWall::new(1_000_000);
        let clock = Arc::new(HlcClock::with_time_source(
            7,
            Duration::from_secs(60),
            wall.source(),
        ));

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let clock = Arc::clone(&clock);
                std::thread::spawn(move || {
                    (0..250).map(|_| clock.now().unwrap()).collect::<Vec<_>>()
                })
            })
            .collect();

        let mut all = Vec::with_capacity(8 * 250);
        for handle in handles {
            all.extend(handle.join().unwrap());
        }
        assert_eq!(all.len(), 8 * 250);

        all.sort();
        all.dedup();
        assert_eq!(all.len(), 8 * 250, "every timestamp must be unique");

        // The wall clock never moved, so the logical counter covers 0..2000.
        for (index, stamp) in all.iter().enumerate() {
            assert_eq!(stamp.physical_micros, 1_000_000);
            assert_eq!(stamp.logical as usize, index);
            assert_eq!(stamp.node_tiebreaker, 7);
        }
    }

    #[test]
    fn observe_advances_local_clock_past_remote() {
        let wall = ManualWall::new(1_000);
        let clock = HlcClock::with_time_source(3, Duration::from_secs(600), wall.source());

        // A remote ahead of local physical time advances the local clock.
        let remote = ts(5_000, 7, 9);
        let observed = clock.observe(remote).unwrap();
        assert!(observed > remote);
        assert_eq!(observed, ts(5_000, 8, 3));

        // The local clock stays ahead of the remote afterwards.
        let next = clock.now().unwrap();
        assert!(next > remote);
        assert!(next > observed);
        assert_eq!(next, ts(5_000, 9, 3));

        // Skew monitoring folded the observation in.
        assert_eq!(clock.max_observed_skew(), Duration::from_micros(4_000));

        // Same physical component, higher logical: still advances.
        let remote = ts(5_000, 20, 9);
        let observed = clock.observe(remote).unwrap();
        assert!(observed > remote);
        assert_eq!(observed, ts(5_000, 21, 3));

        // A stale remote does not move the clock backward.
        let stale = ts(2_000, 0, 9);
        let observed = clock.observe(stale).unwrap();
        assert!(observed > stale);
        assert_eq!(observed, ts(5_000, 22, 3));
    }

    #[test]
    fn observe_rejects_excessive_skew_and_rejects_allocation() {
        let wall = ManualWall::new(10_000);
        let clock = HlcClock::with_time_source(1, Duration::from_micros(1_000), wall.source());

        let remote = ts(15_000, 0, 2);
        let err = clock.observe(remote).unwrap_err();
        assert_eq!(
            err,
            ClockSkewError {
                observed: Duration::from_micros(5_000),
                maximum: Duration::from_micros(1_000),
            }
        );
        assert_eq!(clock.max_observed_skew(), Duration::from_micros(5_000));

        // The failed observation did not advance the clock.
        let next = clock.next_after(HlcTimestamp::ZERO);
        assert_eq!(next, ts(10_000, 0, 1));

        // Excessive skew rejects further timestamp allocation.
        assert!(clock.now().is_err());
        assert!(clock.observe(ts(10_500, 0, 2)).is_err());
    }

    #[test]
    fn next_after_is_strictly_greater() {
        let wall = ManualWall::new(1_000);
        let clock = HlcClock::with_time_source(5, Duration::from_secs(60), wall.source());

        // A fresh tick already exceeds a small minimum.
        assert_eq!(clock.next_after(ts(1, 1, 1)), ts(1_000, 0, 5)); // state (1000, 0)

        // Same tick components but a higher tiebreaker on the minimum forces
        // the strict-successor path.
        let minimum = ts(1_000, 1, u32::MAX);
        let next = clock.next_after(minimum); // tick -> (1000, 1); candidate <= minimum
        assert!(next > minimum);
        assert_eq!(next, ts(1_000, 2, 5)); // state (1000, 2)

        // A minimum ahead of the wall clock jumps the physical component.
        let minimum = ts(9_999, 42, 0);
        let next = clock.next_after(minimum);
        assert!(next > minimum);
        assert_eq!(next, ts(9_999, 43, 5)); // state (9999, 43)

        // Logical overflow rolls into the physical component.
        let minimum = ts(9_999, u32::MAX, u32::MAX);
        let next = clock.next_after(minimum);
        assert!(next > minimum);
        assert_eq!(next, ts(10_000, 0, 5)); // state (10000, 0)

        // Consecutive calls stay strictly increasing.
        let mut previous = next;
        for _ in 0..100 {
            let current = clock.next_after(HlcTimestamp::ZERO);
            assert!(current > previous);
            previous = current;
        }
    }

    #[test]
    fn commit_timestamp_exceeds_every_participant() {
        let wall = ManualWall::new(5_000);
        let clock = HlcClock::with_time_source(4, Duration::from_secs(60), wall.source());

        let participants = [ts(5_000, 3, 9), ts(7_000, 0, 1), ts(6_999, 12, 12)];
        let commit = clock.commit_timestamp(participants);
        for participant in participants {
            assert!(commit > participant);
        }
        assert_eq!(commit, ts(7_000, 1, 4));

        // No participants: still a valid fresh timestamp.
        let commit = clock.commit_timestamp(Vec::new());
        assert!(commit > HlcTimestamp::ZERO);
        assert_eq!(commit, ts(7_000, 2, 4));
    }

    #[test]
    fn legacy_stamps_sort_before_watermark_and_hlc_at_or_after() {
        let watermark = MigrationWatermark {
            mvcc_format_version: MigrationWatermark::FORMAT_HLC,
            watermark: ts(1_000, 0, 0),
        };

        // Every legacy epoch sorts before the watermark, however large.
        assert_eq!(
            watermark.cmp_stamp_to_watermark(&StoredVersionStamp::LegacyEpoch(u64::MAX)),
            Ordering::Less
        );
        // HLC timestamps sort at or after the watermark.
        assert_eq!(
            watermark.cmp_stamp_to_watermark(&StoredVersionStamp::Hlc(ts(1_000, 0, 0))),
            Ordering::Equal
        );
        assert_eq!(
            watermark.cmp_stamp_to_watermark(&StoredVersionStamp::Hlc(ts(1_000, 0, 1))),
            Ordering::Greater
        );
        // A pre-watermark HLC timestamp cannot occur once the watermark is
        // enforced; the comparison clamps it to "at the watermark".
        assert_eq!(
            watermark.cmp_stamp_to_watermark(&StoredVersionStamp::Hlc(ts(999, 99, 99))),
            Ordering::Equal
        );

        // Total order during migration.
        assert_eq!(
            MigrationWatermark::cmp_stamps(
                &StoredVersionStamp::LegacyEpoch(u64::MAX),
                &StoredVersionStamp::Hlc(HlcTimestamp::ZERO),
            ),
            Ordering::Less
        );
        assert_eq!(
            MigrationWatermark::cmp_stamps(
                &StoredVersionStamp::Hlc(HlcTimestamp::ZERO),
                &StoredVersionStamp::LegacyEpoch(0),
            ),
            Ordering::Greater
        );
        assert_eq!(
            MigrationWatermark::cmp_stamps(
                &StoredVersionStamp::LegacyEpoch(7),
                &StoredVersionStamp::LegacyEpoch(9),
            ),
            Ordering::Less
        );
        assert_eq!(
            MigrationWatermark::cmp_stamps(
                &StoredVersionStamp::Hlc(ts(1, 0, 0)),
                &StoredVersionStamp::Hlc(ts(1, 0, 1)),
            ),
            Ordering::Less
        );
    }

    #[test]
    fn display_and_from_str_round_trip() {
        for stamp in [
            HlcTimestamp::ZERO,
            ts(1, 2, 3),
            ts(1_756_000_000_000_000, 42, 9),
            ts(u64::MAX, u32::MAX, u32::MAX),
        ] {
            let text = stamp.to_string();
            let parsed: HlcTimestamp = text.parse().unwrap();
            assert_eq!(parsed, stamp);
        }
        assert_eq!(ts(1, 2, 3).to_string(), "1.2.3");
        assert_eq!(format!("{:?}", ts(1, 2, 3)), "1.2.3");
    }

    #[test]
    fn from_str_rejects_malformed_text() {
        assert_eq!(
            "".parse::<HlcTimestamp>(),
            Err(HlcParseError::InvalidFormat(String::new()))
        );
        assert_eq!(
            "1.2".parse::<HlcTimestamp>(),
            Err(HlcParseError::InvalidFormat("1.2".to_owned()))
        );
        assert_eq!(
            "1.2.3.4".parse::<HlcTimestamp>(),
            Err(HlcParseError::InvalidFormat("1.2.3.4".to_owned()))
        );
        assert_eq!(
            "1.2.x".parse::<HlcTimestamp>(),
            Err(HlcParseError::InvalidComponent(
                "1.2.x".to_owned(),
                "x".to_owned()
            ))
        );
        assert_eq!(
            "18446744073709551616.0.0".parse::<HlcTimestamp>(),
            Err(HlcParseError::InvalidComponent(
                "18446744073709551616.0.0".to_owned(),
                "18446744073709551616".to_owned()
            ))
        );
        assert!("1.2.-3".parse::<HlcTimestamp>().is_err());
        assert!("1.2.3 ".parse::<HlcTimestamp>().is_err());
    }

    #[test]
    fn serde_round_trip() {
        let stamp = ts(1_756_000_000_000_000, 42, 9);
        assert_eq!(
            tokens::to_tokens(&stamp),
            tokens::Token::Struct(
                "HlcTimestamp",
                vec![
                    tokens::Token::U64(1_756_000_000_000_000),
                    tokens::Token::U32(42),
                    tokens::Token::U32(9),
                ],
            )
        );
        assert_eq!(
            tokens::from_tokens::<HlcTimestamp>(tokens::to_tokens(&stamp)),
            stamp
        );

        for stored in [
            StoredVersionStamp::LegacyEpoch(123_456),
            StoredVersionStamp::Hlc(stamp),
        ] {
            assert_eq!(
                tokens::from_tokens::<StoredVersionStamp>(tokens::to_tokens(&stored)),
                stored
            );
        }

        let watermark = MigrationWatermark {
            mvcc_format_version: MigrationWatermark::FORMAT_HLC,
            watermark: stamp,
        };
        assert_eq!(
            tokens::from_tokens::<MigrationWatermark>(tokens::to_tokens(&watermark)),
            watermark
        );
    }

    #[test]
    fn clock_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<HlcClock>();
    }

    #[test]
    fn system_clock_smoke() {
        let clock = HlcClock::new(1, Duration::from_secs(60));
        let stamp = clock.now().unwrap();
        assert!(stamp.physical_micros > 0);
        assert_eq!(stamp.node_tiebreaker, 1);
    }

    /// Minimal serde round-trip support: the crate intentionally has no
    /// serialization-format dependency, so tests record values into a token
    /// tree and deserialize back from it.
    mod tokens {
        use core::fmt;
        use serde::de::{self, EnumAccess, SeqAccess, VariantAccess, Visitor};
        use serde::ser::{Impossible, SerializeStruct};
        use serde::{Deserialize, Deserializer, Serialize, Serializer};

        #[derive(Debug, Clone, PartialEq, Eq)]
        pub enum Token {
            U32(u32),
            U64(u64),
            Struct(&'static str, Vec<Token>),
            NewtypeVariant(&'static str, &'static str, Box<Token>),
        }

        pub fn to_tokens<T: Serialize>(value: &T) -> Token {
            value
                .serialize(TokenSerializer)
                .expect("serialize to tokens")
        }

        pub fn from_tokens<T: for<'de> Deserialize<'de>>(token: Token) -> T {
            T::deserialize(TokenDeserializer(token)).expect("deserialize from tokens")
        }

        #[derive(Debug)]
        pub struct TokenError(&'static str);

        impl fmt::Display for TokenError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.0)
            }
        }

        impl std::error::Error for TokenError {}

        impl serde::ser::Error for TokenError {
            fn custom<T: fmt::Display>(_msg: T) -> Self {
                TokenError("custom serialization error")
            }
        }

        impl serde::de::Error for TokenError {
            fn custom<T: fmt::Display>(_msg: T) -> Self {
                TokenError("custom deserialization error")
            }
        }

        struct TokenSerializer;

        struct TokenStructSerializer {
            name: &'static str,
            fields: Vec<Token>,
        }

        impl SerializeStruct for TokenStructSerializer {
            type Ok = Token;
            type Error = TokenError;

            fn serialize_field<T: ?Sized + Serialize>(
                &mut self,
                _key: &'static str,
                value: &T,
            ) -> Result<(), TokenError> {
                self.fields.push(value.serialize(TokenSerializer)?);
                Ok(())
            }

            fn end(self) -> Result<Token, TokenError> {
                Ok(Token::Struct(self.name, self.fields))
            }
        }

        impl Serializer for TokenSerializer {
            type Ok = Token;
            type Error = TokenError;
            type SerializeSeq = Impossible<Token, TokenError>;
            type SerializeTuple = Impossible<Token, TokenError>;
            type SerializeTupleStruct = Impossible<Token, TokenError>;
            type SerializeTupleVariant = Impossible<Token, TokenError>;
            type SerializeMap = Impossible<Token, TokenError>;
            type SerializeStruct = TokenStructSerializer;
            type SerializeStructVariant = Impossible<Token, TokenError>;

            fn serialize_u32(self, v: u32) -> Result<Token, TokenError> {
                Ok(Token::U32(v))
            }

            fn serialize_u64(self, v: u64) -> Result<Token, TokenError> {
                Ok(Token::U64(v))
            }

            fn serialize_struct(
                self,
                name: &'static str,
                _len: usize,
            ) -> Result<TokenStructSerializer, TokenError> {
                Ok(TokenStructSerializer {
                    name,
                    fields: Vec::new(),
                })
            }

            fn serialize_newtype_variant<T: ?Sized + Serialize>(
                self,
                name: &'static str,
                _variant_index: u32,
                variant: &'static str,
                value: &T,
            ) -> Result<Token, TokenError> {
                Ok(Token::NewtypeVariant(
                    name,
                    variant,
                    Box::new(value.serialize(TokenSerializer)?),
                ))
            }

            fn serialize_bool(self, _v: bool) -> Result<Token, TokenError> {
                Err(TokenError("bool unsupported"))
            }

            fn serialize_i8(self, _v: i8) -> Result<Token, TokenError> {
                Err(TokenError("i8 unsupported"))
            }

            fn serialize_i16(self, _v: i16) -> Result<Token, TokenError> {
                Err(TokenError("i16 unsupported"))
            }

            fn serialize_i32(self, _v: i32) -> Result<Token, TokenError> {
                Err(TokenError("i32 unsupported"))
            }

            fn serialize_i64(self, _v: i64) -> Result<Token, TokenError> {
                Err(TokenError("i64 unsupported"))
            }

            fn serialize_u8(self, _v: u8) -> Result<Token, TokenError> {
                Err(TokenError("u8 unsupported"))
            }

            fn serialize_u16(self, _v: u16) -> Result<Token, TokenError> {
                Err(TokenError("u16 unsupported"))
            }

            fn serialize_f32(self, _v: f32) -> Result<Token, TokenError> {
                Err(TokenError("f32 unsupported"))
            }

            fn serialize_f64(self, _v: f64) -> Result<Token, TokenError> {
                Err(TokenError("f64 unsupported"))
            }

            fn serialize_char(self, _v: char) -> Result<Token, TokenError> {
                Err(TokenError("char unsupported"))
            }

            fn serialize_str(self, _v: &str) -> Result<Token, TokenError> {
                Err(TokenError("str unsupported"))
            }

            fn serialize_bytes(self, _v: &[u8]) -> Result<Token, TokenError> {
                Err(TokenError("bytes unsupported"))
            }

            fn serialize_none(self) -> Result<Token, TokenError> {
                Err(TokenError("none unsupported"))
            }

            fn serialize_some<T: ?Sized + Serialize>(
                self,
                _value: &T,
            ) -> Result<Token, TokenError> {
                Err(TokenError("some unsupported"))
            }

            fn serialize_unit(self) -> Result<Token, TokenError> {
                Err(TokenError("unit unsupported"))
            }

            fn serialize_unit_struct(self, _name: &'static str) -> Result<Token, TokenError> {
                Err(TokenError("unit struct unsupported"))
            }

            fn serialize_unit_variant(
                self,
                _name: &'static str,
                _variant_index: u32,
                _variant: &'static str,
            ) -> Result<Token, TokenError> {
                Err(TokenError("unit variant unsupported"))
            }

            fn serialize_newtype_struct<T: ?Sized + Serialize>(
                self,
                _name: &'static str,
                _value: &T,
            ) -> Result<Token, TokenError> {
                Err(TokenError("newtype struct unsupported"))
            }

            fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, TokenError> {
                Err(TokenError("seq unsupported"))
            }

            fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, TokenError> {
                Err(TokenError("tuple unsupported"))
            }

            fn serialize_tuple_struct(
                self,
                _name: &'static str,
                _len: usize,
            ) -> Result<Self::SerializeTupleStruct, TokenError> {
                Err(TokenError("tuple struct unsupported"))
            }

            fn serialize_tuple_variant(
                self,
                _name: &'static str,
                _variant_index: u32,
                _variant: &'static str,
                _len: usize,
            ) -> Result<Self::SerializeTupleVariant, TokenError> {
                Err(TokenError("tuple variant unsupported"))
            }

            fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, TokenError> {
                Err(TokenError("map unsupported"))
            }

            fn serialize_struct_variant(
                self,
                _name: &'static str,
                _variant_index: u32,
                _variant: &'static str,
                _len: usize,
            ) -> Result<Self::SerializeStructVariant, TokenError> {
                Err(TokenError("struct variant unsupported"))
            }
        }

        struct TokenDeserializer(Token);

        struct TokenSeqAccess {
            iter: std::vec::IntoIter<Token>,
        }

        impl<'de> SeqAccess<'de> for TokenSeqAccess {
            type Error = TokenError;

            fn next_element_seed<T: de::DeserializeSeed<'de>>(
                &mut self,
                seed: T,
            ) -> Result<Option<T::Value>, TokenError> {
                match self.iter.next() {
                    Some(token) => seed.deserialize(TokenDeserializer(token)).map(Some),
                    None => Ok(None),
                }
            }
        }

        struct TokenEnumAccess {
            variant: &'static str,
            value: Token,
        }

        impl<'de> EnumAccess<'de> for TokenEnumAccess {
            type Error = TokenError;
            type Variant = TokenVariantAccess;

            fn variant_seed<V: de::DeserializeSeed<'de>>(
                self,
                seed: V,
            ) -> Result<(V::Value, TokenVariantAccess), TokenError> {
                let value =
                    seed.deserialize(de::value::BorrowedStrDeserializer::new(self.variant))?;
                Ok((value, TokenVariantAccess { value: self.value }))
            }
        }

        struct TokenVariantAccess {
            value: Token,
        }

        impl<'de> VariantAccess<'de> for TokenVariantAccess {
            type Error = TokenError;

            fn unit_variant(self) -> Result<(), TokenError> {
                Err(TokenError("unit variants unsupported"))
            }

            fn newtype_variant_seed<T: de::DeserializeSeed<'de>>(
                self,
                seed: T,
            ) -> Result<T::Value, TokenError> {
                seed.deserialize(TokenDeserializer(self.value))
            }

            fn tuple_variant<V: Visitor<'de>>(
                self,
                _len: usize,
                _visitor: V,
            ) -> Result<V::Value, TokenError> {
                Err(TokenError("tuple variants unsupported"))
            }

            fn struct_variant<V: Visitor<'de>>(
                self,
                _fields: &'static [&'static str],
                _visitor: V,
            ) -> Result<V::Value, TokenError> {
                Err(TokenError("struct variants unsupported"))
            }
        }

        impl<'de> Deserializer<'de> for TokenDeserializer {
            type Error = TokenError;

            fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TokenError> {
                match self.0 {
                    Token::U32(v) => visitor.visit_u32(v),
                    Token::U64(v) => visitor.visit_u64(v),
                    Token::Struct(_, fields) => visitor.visit_seq(TokenSeqAccess {
                        iter: fields.into_iter(),
                    }),
                    Token::NewtypeVariant(_, variant, value) => {
                        visitor.visit_enum(TokenEnumAccess {
                            variant,
                            value: *value,
                        })
                    }
                }
            }

            fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TokenError> {
                match self.0 {
                    Token::U32(v) => visitor.visit_u32(v),
                    _ => Err(TokenError("expected u32 token")),
                }
            }

            fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, TokenError> {
                match self.0 {
                    Token::U64(v) => visitor.visit_u64(v),
                    _ => Err(TokenError("expected u64 token")),
                }
            }

            fn deserialize_struct<V: Visitor<'de>>(
                self,
                _name: &'static str,
                _fields: &'static [&'static str],
                visitor: V,
            ) -> Result<V::Value, TokenError> {
                match self.0 {
                    Token::Struct(_, fields) => visitor.visit_seq(TokenSeqAccess {
                        iter: fields.into_iter(),
                    }),
                    _ => Err(TokenError("expected struct token")),
                }
            }

            fn deserialize_enum<V: Visitor<'de>>(
                self,
                _name: &'static str,
                _variants: &'static [&'static str],
                visitor: V,
            ) -> Result<V::Value, TokenError> {
                match self.0 {
                    Token::NewtypeVariant(_, variant, value) => {
                        visitor.visit_enum(TokenEnumAccess {
                            variant,
                            value: *value,
                        })
                    }
                    _ => Err(TokenError("expected enum token")),
                }
            }

            serde::forward_to_deserialize_any! {
                bool i8 i16 i32 i64 i128 u8 u16 u128 f32 f64 char str string
                bytes byte_buf option unit unit_struct newtype_struct seq tuple
                tuple_struct map identifier ignored_any
            }
        }
    }
}
