//! Learned index — the on-disk "cold" primary path.
//!
//! Two implementations of the [`LearnedIndex`] trait:
//! - [`LinearLearnedIndex`]: binary-search baseline over a sorted sample.
//! - [`PgmIndex`]: a real compressed **PGM-index** (piecewise-linear model with
//!   guaranteed error `epsilon`), ~10–100× smaller than a B-tree and
//!   `O(log(segments) + 1)` lookup. Built greedily by the shrinking-cone
//!   algorithm at flush time and stored in the run's index trailer.

use serde::{Deserialize, Serialize};

/// `key -> approximate (lo, hi) offset range`; a tiny final scan corrects the
/// prediction.
pub trait LearnedIndex {
    fn predict(&self, key: u64) -> (usize, usize);
}

/// Baseline: sorted `(key, offset)` samples, binary-searched.
pub struct LinearLearnedIndex {
    samples: Vec<(u64, usize)>,
}

impl LinearLearnedIndex {
    pub fn from_sorted(samples: Vec<(u64, usize)>) -> Self {
        debug_assert!(
            samples.windows(2).all(|w| w[0].0 <= w[1].0),
            "samples must be sorted"
        );
        Self { samples }
    }
}

impl LearnedIndex for LinearLearnedIndex {
    fn predict(&self, key: u64) -> (usize, usize) {
        let idx = self
            .samples
            .partition_point(|(k, _)| *k < key)
            .saturating_sub(1);
        let lo = self.samples.get(idx).map(|(_, o)| *o).unwrap_or(0);
        let hi = self
            .samples
            .get(idx + 1)
            .map(|(_, o)| *o)
            .unwrap_or(usize::MAX);
        (lo, hi)
    }
}

/// One piecewise-linear segment: `pos ≈ intercept + slope * (key - first_key)`,
/// accurate to within `epsilon` for every key in the segment's range.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct PgmSegment {
    pub key: u64, // first key covered by this segment
    pub slope: f64,
    pub intercept: f64,
}

/// Compressed PGM-index over `(key -> position)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgmIndex {
    segments: Vec<PgmSegment>,
    epsilon: usize,
}

impl PgmIndex {
    pub fn segments(&self) -> &[PgmSegment] {
        &self.segments
    }

    /// Build a PGM-index from points sorted ascending by key, guaranteeing every
    /// point is predicted within `epsilon` positions. Greedy shrinking-cone
    /// segmentation (the PGM-index core construction).
    pub fn build(points: &[(u64, usize)], epsilon: usize) -> Self {
        let mut out = Vec::new();
        let n = points.len();
        let mut i = 0;
        while i < n {
            let (x0, y0) = points[i];
            if i + 1 >= n {
                // Singleton tail segment: a flat line at y0.
                out.push(PgmSegment {
                    key: x0,
                    slope: 0.0,
                    intercept: y0 as f64,
                });
                break;
            }
            let (x1, y1) = points[i + 1];
            let dx = (x1 - x0) as f64;
            // Feasible slope interval after the second point.
            let mut lo = ((y1.saturating_sub(epsilon)) as f64 - y0 as f64) / dx;
            let mut hi = ((y1 + epsilon) as f64 - y0 as f64) / dx;
            let mut j = i + 1;
            while j + 1 < n {
                let (xj, yj) = points[j + 1];
                let dxj = (xj - x0) as f64;
                let lo_j = ((yj.saturating_sub(epsilon)) as f64 - y0 as f64) / dxj;
                let hi_j = ((yj + epsilon) as f64 - y0 as f64) / dxj;
                if lo_j > hi || hi_j < lo {
                    break; // cone empty → cannot extend this segment
                }
                if lo_j > lo {
                    lo = lo_j;
                }
                if hi_j < hi {
                    hi = hi_j;
                }
                j += 1;
            }
            let slope = (lo + hi) / 2.0;
            out.push(PgmSegment {
                key: x0,
                slope,
                intercept: y0 as f64,
            });
            i = j + 1; // next segment starts after the last covered point (no overlap)
        }
        Self {
            segments: out,
            epsilon,
        }
    }
}

impl LearnedIndex for PgmIndex {
    fn predict(&self, key: u64) -> (usize, usize) {
        if self.segments.is_empty() {
            return (0, usize::MAX);
        }
        // Find the last segment whose key <= the query key.
        let idx = self
            .segments
            .partition_point(|s| s.key <= key)
            .saturating_sub(1)
            .min(self.segments.len() - 1);
        let s = &self.segments[idx];
        let pos = s.intercept + s.slope * (key as f64 - s.key as f64);
        let lo = (pos as usize).saturating_sub(self.epsilon);
        let hi = pos as usize + self.epsilon;
        (lo, hi)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linear_baseline_predicts() {
        let idx = LinearLearnedIndex::from_sorted(vec![(0, 0), (100, 1000), (200, 2000)]);
        assert_eq!(idx.predict(0), (0, 1000));
        assert_eq!(idx.predict(150), (1000, 2000));
        assert_eq!(idx.predict(250), (2000, usize::MAX));
    }

    #[test]
    fn pgm_within_epsilon_on_linear_data() {
        // y = x exactly → one segment, zero error.
        let pts: Vec<(u64, usize)> = (0..1000u64).map(|i| (i, i as usize)).collect();
        let pgm = PgmIndex::build(&pts, 4);
        assert_eq!(pgm.segments().len(), 1, "linear data is a single segment");
        for (k, v) in &pts {
            let (lo, hi) = pgm.predict(*k);
            assert!(*v >= lo && *v <= hi, "key {k}: {} not in [{lo},{hi}]", v);
        }
    }

    #[test]
    fn pgm_within_epsilon_on_step_data() {
        // y grows in steps of 5 → several segments, each within epsilon.
        let pts: Vec<(u64, usize)> = (0..200u64).map(|i| (i, (i / 10) as usize * 5)).collect();
        let eps = 8;
        let pgm = PgmIndex::build(&pts, eps);
        for (k, v) in &pts {
            let (lo, hi) = pgm.predict(*k);
            assert!(lo <= *v && *v <= hi, "key {k} val {v} outside [{lo},{hi}]");
        }
    }

    #[test]
    fn pgm_is_far_smaller_than_linear_for_monotone_keys() {
        // row_id == position (perfectly linear) → 1 segment vs N samples.
        let pts: Vec<(u64, usize)> = (0..100_000u64).map(|i| (i, i as usize)).collect();
        let pgm = PgmIndex::build(&pts, 8);
        assert!(
            pgm.segments().len() <= 2,
            "got {} segments",
            pgm.segments().len()
        );
    }
}
