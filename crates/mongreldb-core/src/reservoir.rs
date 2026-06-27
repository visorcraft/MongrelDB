//! Reservoir sampling for approximate analytics (Phase 8.2).
//!
//! A fixed-capacity uniform sample of row ids is maintained incrementally as
//! rows are inserted (Vitter's Algorithm R). It lets
//! [`crate::Table::approx_aggregate`] answer `COUNT / SUM / AVG … WHERE` over a
//! ~1 % sample in O(k) instead of a full scan, with normal-theory confidence
//! intervals. When the table has `≤ k` live rows the sample is the whole table
//! and the estimate is exact (zero-width interval).
//!
//! The sample lives in memory; it is repopulated from the visible rows during
//! [`crate::Table::open`] (which already scans for index rebuild), so a reopened
//! table has a sample immediately. Persistence is deferred to Phase 9.

/// A reservoir sample of row ids over a live table, plus the count of rows ever
/// offered (for the sampling algorithm's inclusion probability).
#[derive(Debug, Clone)]
pub struct Reservoir {
    row_ids: Vec<u64>,
    k: usize,
    seen: u64,
    state: u64,
}

impl Reservoir {
    /// A reservoir of capacity `k`, seeded by `seed`.
    pub fn new(k: usize, seed: u64) -> Self {
        Self {
            row_ids: Vec::with_capacity(k),
            k,
            seen: 0,
            state: seed,
        }
    }

    /// The configured sample capacity.
    pub fn capacity(&self) -> usize {
        self.k
    }

    /// The sampled row ids (live and tombstoned alike; callers filter by
    /// visibility at read time).
    pub fn row_ids(&self) -> &[u64] {
        &self.row_ids
    }

    /// Total rows ever offered (never decreased by deletes).
    pub fn seen(&self) -> u64 {
        self.seen
    }

    /// Offer one row id to the sample (call once per inserted row, in insert
    /// order). Implements Vitter's Algorithm R.
    pub fn offer(&mut self, rid: u64) {
        self.seen += 1;
        if (self.row_ids.len() as u64) < self.k as u64 {
            self.row_ids.push(rid);
        } else {
            // j uniform in [0, seen); replace slot j with probability k/seen.
            let j = self.gen_range_u64(0, self.seen);
            if (j as usize) < self.k {
                self.row_ids[j as usize] = rid;
            }
        }
    }

    /// Clear the sample (e.g. before a full rebuild on open).
    pub fn reset(&mut self) {
        self.row_ids.clear();
        self.seen = 0;
    }

    /// Uniform u64 in `[lo, hi)`.
    fn gen_range_u64(&mut self, lo: u64, hi: u64) -> u64 {
        let n = hi.saturating_sub(lo);
        if n == 0 {
            return lo;
        }
        lo + (self.next_u64() % n)
    }

    /// SplitMix64 step.
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

impl Default for Reservoir {
    fn default() -> Self {
        Self::new(8192, 0xC0FF_BEEF_1234_5678)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fills_then_reservoirs() {
        let mut r = Reservoir::new(8, 1);
        for i in 0..10_000u64 {
            r.offer(i);
        }
        // Capacity never exceeded; every slot in range.
        assert_eq!(r.row_ids.len(), 8);
        assert!(r.row_ids.iter().all(|&x| x < 10_000));
        assert_eq!(r.seen, 10_000);
    }

    #[test]
    fn small_table_is_exact() {
        // Fewer rows than capacity ⇒ the sample holds every row id.
        let mut r = Reservoir::new(8192, 2);
        for i in 0..50u64 {
            r.offer(i);
        }
        let mut sorted = r.row_ids.to_vec();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..50).collect::<Vec<_>>());
    }

    #[test]
    fn is_uniform_ish() {
        // Over many offers the mean of sampled ids ≈ population mean (100k/2).
        let mut r = Reservoir::new(4096, 7);
        for i in 0..100_000u64 {
            r.offer(i);
        }
        let mean: f64 = r.row_ids.iter().map(|&x| x as f64).sum::<f64>() / r.row_ids.len() as f64;
        // Expected 49_999.5; allow ±2k (sample noise).
        assert!((mean - 49_999.5).abs() < 2_000.0, "mean {mean} biased");
    }
}
