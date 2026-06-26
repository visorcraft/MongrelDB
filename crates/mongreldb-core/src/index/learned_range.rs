//! Per-column learned (PGM) range index — serves `Condition::Range` and
//! `Condition::RangeF64` sub-linearly for numeric columns declared
//! `IndexKind::LearnedRange`.
//!
//! The run is sorted by `RowId`, not by column value, so at flush we collect
//! `(value, row_id)`, sort by value, and build a PGM over `value → position`
//! (parallel to the sorted `row_id` array). A range query uses the PGM to land
//! in an ε-window, then a local binary search finds the exact `[lo,hi]` slice —
//! `O(log segments + log ε)` instead of a full column scan.
//!
//! Both `i64` and `f64` columns are supported via order-preserving key encodings.

use super::pgm::{LearnedIndex, PgmIndex};
use std::collections::HashSet;

/// Order-preserving encoding of `i64` into `u64` (flip the sign bit), so PGM
/// key order matches numeric order including negatives: `MIN→0`, `-1→2⁶³-1`,
/// `0→2⁶³`, `MAX→u64::MAX`.
#[inline]
fn i64_key(v: i64) -> u64 {
    (v as u64) ^ (1u64 << 63)
}

/// Order-preserving encoding of `f64` into `u64`: positive floats map to
/// `2⁶³..u64::MAX` (sign bit flipped), negative floats map to `0..2⁶³-1`
/// (all bits flipped) so the total order matches IEEE-754 totalOrder.
#[inline]
fn f64_key(v: f64) -> u64 {
    let bits = v.to_bits();
    if bits & (1u64 << 63) != 0 {
        !bits
    } else {
        bits ^ (1u64 << 63)
    }
}

#[derive(Debug, Clone)]
pub struct ColumnLearnedRange {
    keys: Vec<u64>,    // order-preserving value keys, ascending
    row_ids: Vec<u64>, // parallel row ids (sorted by value)
    pgm: PgmIndex,
}

impl ColumnLearnedRange {
    /// Build from `(value, row_id)` pairs in any order.
    pub fn build_i64(pairs: &[(i64, u64)]) -> Self {
        let mut sorted: Vec<(u64, u64)> = pairs.iter().map(|(v, r)| (i64_key(*v), *r)).collect();
        sorted.sort_unstable_by_key(|(k, _)| *k);
        let keys: Vec<u64> = sorted.iter().map(|(k, _)| *k).collect();
        let row_ids: Vec<u64> = sorted.iter().map(|(_, r)| *r).collect();
        let points: Vec<(u64, usize)> = keys.iter().enumerate().map(|(i, k)| (*k, i)).collect();
        let pgm = if points.is_empty() {
            PgmIndex::build(&[], 16)
        } else {
            PgmIndex::build(&points, 16)
        };
        Self { keys, row_ids, pgm }
    }

    /// First position whose key `>= key`. Seeded by the PGM ε-window, then
    /// gallops outward — the window alone only brackets duplicate runs up to
    /// 2·ε, so longer runs need expansion (rare; keeps lookups sub-linear).
    fn lower_bound(&self, key: u64) -> usize {
        let n = self.keys.len();
        if n == 0 {
            return 0;
        }
        let (lo, hi) = self.pgm.predict(key);
        let lo = lo.min(n);
        let hi = hi.min(n).max(lo);
        let mut idx = lo + self.keys[lo..hi].partition_point(|k| *k < key);
        // Gallop left if the window began inside a run of keys >= search key.
        if idx > 0 && self.keys[idx - 1] >= key {
            let mut step = 1usize;
            while idx >= step && self.keys[idx - step] >= key {
                step <<= 1;
            }
            let start = idx - step.min(idx);
            idx = start + self.keys[start..idx].partition_point(|k| *k < key);
        }
        // Gallop right if the window ended before the true lower bound.
        if idx < n && self.keys[idx] < key {
            let mut step = 1usize;
            while idx + step <= n && self.keys[(idx + step).min(n) - 1] < key {
                step <<= 1;
            }
            let end = (idx + step).min(n);
            idx = idx + self.keys[idx..end].partition_point(|k| *k < key);
        }
        idx
    }

    /// First position whose key `> key` (galloping, same rationale).
    fn upper_bound(&self, key: u64) -> usize {
        let n = self.keys.len();
        if n == 0 {
            return 0;
        }
        let (lo, hi) = self.pgm.predict(key);
        let lo = lo.min(n);
        let hi = hi.min(n).max(lo);
        let mut idx = lo + self.keys[lo..hi].partition_point(|k| *k <= key);
        if idx > 0 && self.keys[idx - 1] > key {
            let mut step = 1usize;
            while idx >= step && self.keys[idx - step] > key {
                step <<= 1;
            }
            let start = idx - step.min(idx);
            idx = start + self.keys[start..idx].partition_point(|k| *k <= key);
        }
        if idx < n && self.keys[idx] <= key {
            let mut step = 1usize;
            while idx + step <= n && self.keys[(idx + step).min(n) - 1] <= key {
                step <<= 1;
            }
            let end = (idx + step).min(n);
            idx = idx + self.keys[idx..end].partition_point(|k| *k <= key);
        }
        idx
    }

    /// Row ids whose value is in `[lo, hi]` (inclusive).
    pub fn range(&self, lo: i64, hi: i64) -> HashSet<u64> {
        if hi < lo || self.keys.is_empty() {
            return HashSet::new();
        }
        let start = self.lower_bound(i64_key(lo));
        let end = self.upper_bound(i64_key(hi));
        self.row_ids[start..end].iter().copied().collect()
    }

    /// Build from `(f64_value, row_id)` pairs (Phase 13.3).
    pub fn build_f64(pairs: &[(f64, u64)]) -> Self {
        let mut sorted: Vec<(u64, u64)> = pairs.iter().map(|(v, r)| (f64_key(*v), *r)).collect();
        sorted.sort_unstable_by_key(|(k, _)| *k);
        let keys: Vec<u64> = sorted.iter().map(|(k, _)| *k).collect();
        let row_ids: Vec<u64> = sorted.iter().map(|(_, r)| *r).collect();
        let points: Vec<(u64, usize)> = keys.iter().enumerate().map(|(i, k)| (*k, i)).collect();
        let pgm = if points.is_empty() {
            PgmIndex::build(&[], 16)
        } else {
            PgmIndex::build(&points, 16)
        };
        Self { keys, row_ids, pgm }
    }

    /// Row ids whose f64 value is in `[lo, hi]` with per-bound inclusivity
    /// (Phase 13.3).
    pub fn range_f64(
        &self,
        lo: f64,
        lo_inclusive: bool,
        hi: f64,
        hi_inclusive: bool,
    ) -> HashSet<u64> {
        if self.keys.is_empty() {
            return HashSet::new();
        }
        // Convert to key space. For inclusive bounds, use the value's key
        // directly (it maps to the exact position). For exclusive bounds,
        // nudge inward by ±1 in key space (which is the next representable
        // value in the order-preserving encoding).
        let lo_key = f64_key(lo);
        let hi_key = f64_key(hi);
        let (start, end) = if hi < lo {
            return HashSet::new();
        } else if lo_inclusive && hi_inclusive {
            (self.lower_bound(lo_key), self.upper_bound(hi_key))
        } else if lo_inclusive {
            // hi exclusive
            (self.lower_bound(lo_key), self.lower_bound(hi_key))
        } else if hi_inclusive {
            // lo exclusive
            (self.upper_bound(lo_key), self.upper_bound(hi_key))
        } else {
            (self.upper_bound(lo_key), self.lower_bound(hi_key))
        };
        self.row_ids[start..end].iter().copied().collect()
    }

    /// Snapshot `(value_key, row_id, pgm segments/epsilon)` for checkpointing.
    pub fn snapshot(&self) -> ColumnLearnedRangeSnapshot {
        ColumnLearnedRangeSnapshot {
            keys: self.keys.clone(),
            row_ids: self.row_ids.clone(),
            pgm: self.pgm.clone(),
        }
    }

    /// Rebuild from a snapshot produced by [`ColumnLearnedRange::snapshot`].
    pub fn from_snapshot(snap: ColumnLearnedRangeSnapshot) -> Self {
        Self {
            keys: snap.keys,
            row_ids: snap.row_ids,
            pgm: snap.pgm,
        }
    }
}

/// Serializable snapshot of a [`ColumnLearnedRange`] (PGM is already serde).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnLearnedRangeSnapshot {
    pub keys: Vec<u64>,
    pub row_ids: Vec<u64>,
    pub pgm: PgmIndex,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_returns_exact_slice() {
        // values out of row_id order; duplicates present.
        let pairs = vec![
            (100i64, 0u64),
            (-5, 1),
            (50, 2),
            (100, 3),
            (1000, 4),
            (-5, 5),
        ];
        let idx = ColumnLearnedRange::build_i64(&pairs);
        // sorted by value: -5(r1,r5), 50(r2), 100(r0,r3), 1000(r4)
        let r = idx.range(50, 100);
        assert_eq!(r, [2, 0, 3].into_iter().collect::<HashSet<_>>());
        let all = idx.range(i64::MIN, i64::MAX);
        assert_eq!(all.len(), 6);
        let none = idx.range(200, 300);
        assert!(none.is_empty());
        let negs = idx.range(i64::MIN, -5);
        assert_eq!(negs, [1, 5].into_iter().collect::<HashSet<_>>());
    }
}
