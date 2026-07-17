//! Virtual time in microseconds with per-node skew schedules (spec
//! section 9.5, FND-005).
//!
//! The simulated clock never reads the wall clock. It moves only when
//! the scenario runner advances it — normally when every task is idle
//! ("advance-on-idle"), jumping straight to the next timer or message
//! delivery. Per-node [`SkewSchedule`]s model unsynchronized hardware
//! clocks without affecting the global event order.

use crate::network::NodeId;
use std::collections::BTreeMap;

/// Virtual time unit: microseconds since scenario start.
pub type Micros = u64;

/// Clock misuse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClockError {
    /// Attempted to move virtual time backwards.
    #[error("cannot move virtual clock backwards from {now} to {target}")]
    Backwards {
        /// Current virtual time.
        now: Micros,
        /// Requested target time.
        target: Micros,
    },
}

/// A piecewise-constant offset schedule for one node's hardware clock.
///
/// Each segment `(start, skew)` applies `skew` micros from `start`
/// until the next segment begins. Times before the first segment have
/// zero skew.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SkewSchedule {
    segments: Vec<(Micros, i64)>,
}

impl SkewSchedule {
    /// A fixed offset applied from t=0.
    pub fn constant(skew_micros: i64) -> Self {
        Self {
            segments: vec![(0, skew_micros)],
        }
    }

    /// A schedule from `(start, skew)` segments. Panics unless segment
    /// starts are strictly increasing.
    pub fn piecewise(segments: Vec<(Micros, i64)>) -> Self {
        assert!(
            segments.windows(2).all(|w| w[0].0 < w[1].0),
            "skew schedule segments must have strictly increasing start times"
        );
        Self { segments }
    }

    /// The offset in effect at `at`.
    pub fn skew_at(&self, at: Micros) -> i64 {
        let mut skew = 0;
        for &(start, value) in &self.segments {
            if start > at {
                break;
            }
            skew = value;
        }
        skew
    }
}

/// The global virtual clock plus per-node skew schedules.
#[derive(Debug, Default)]
pub struct Clock {
    now: Micros,
    skews: BTreeMap<NodeId, SkewSchedule>,
}

impl Clock {
    /// A clock at t=0 with no skew.
    pub fn new() -> Self {
        Self::default()
    }

    /// Current global virtual time.
    pub fn now(&self) -> Micros {
        self.now
    }

    /// Moves the clock to `target`. Moving backwards is an error.
    pub fn advance_to(&mut self, target: Micros) -> Result<(), ClockError> {
        if target < self.now {
            return Err(ClockError::Backwards {
                now: self.now,
                target,
            });
        }
        self.now = target;
        Ok(())
    }

    /// Moves the clock forward by `delta`, returning the new time.
    pub fn advance_by(&mut self, delta: Micros) -> Micros {
        self.now += delta;
        self.now
    }

    /// Installs a skew schedule for a node.
    pub fn set_skew(&mut self, node: NodeId, schedule: SkewSchedule) {
        self.skews.insert(node, schedule);
    }

    /// Node-local time: global now plus the node's current skew.
    pub fn node_now(&self, node: NodeId) -> i64 {
        let skew = self.skews.get(&node).map_or(0, |s| s.skew_at(self.now));
        self.now as i64 + skew
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_only_moves_forward() {
        let mut clock = Clock::new();
        assert_eq!(clock.now(), 0);
        assert_eq!(clock.advance_by(100), 100);
        clock.advance_to(250).unwrap();
        clock.advance_to(250).unwrap();
        assert_eq!(
            clock.advance_to(100),
            Err(ClockError::Backwards {
                now: 250,
                target: 100
            })
        );
    }

    #[test]
    fn piecewise_skew_switches_at_segment_starts() {
        let schedule = SkewSchedule::piecewise(vec![(100, -50), (500, 200)]);
        assert_eq!(schedule.skew_at(0), 0);
        assert_eq!(schedule.skew_at(99), 0);
        assert_eq!(schedule.skew_at(100), -50);
        assert_eq!(schedule.skew_at(499), -50);
        assert_eq!(schedule.skew_at(500), 200);
    }

    #[test]
    #[should_panic(expected = "strictly increasing")]
    fn piecewise_rejects_unsorted_segments() {
        let _ = SkewSchedule::piecewise(vec![(100, 1), (50, 2)]);
    }

    #[test]
    fn node_now_applies_constant_skew() {
        let node = NodeId(7);
        let mut clock = Clock::new();
        clock.set_skew(node, SkewSchedule::constant(250));
        assert_eq!(clock.node_now(node), 250);
        clock.advance_by(100);
        assert_eq!(clock.node_now(node), 350);
        assert_eq!(clock.node_now(NodeId(8)), 100);
    }
}
