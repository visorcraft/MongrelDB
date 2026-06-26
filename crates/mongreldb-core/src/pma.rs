//! Packed Memory Array — a cache-oblivious sorted array with amortized
//! `O(log² n)` insert and **no full rewrite per insert**.
//!
//! The array is a power-of-two-sized buffer holding the elements in sorted
//! order with uniform gaps. On insert, the smallest enclosing power-of-two,
//! aligned window whose density is below its upper threshold is rebalanced
//! (its elements gathered and re-spread evenly); only when the whole array
//! passes the high watermark does the buffer grow and redistribute globally.
//! This is the "mutable run" tier that sits between the memtable and the
//! immutable sorted runs, absorbing updates in place.
//!
//! Lookup uses gappy binary search (each probe walks past `O(gaps)` empties,
//! bounded by the density invariant).

const LEAF_WINDOW: usize = 8; // smallest rebalance window
const HIGH_WATERMARK: f64 = 0.8; // global density that triggers a grow+redistribute

/// A packed-memory array of `(K, V)` pairs sorted by `K`.
pub struct Pma<K: Ord + Clone, V: Clone> {
    buf: Vec<Option<(K, V)>>,
    capacity: usize, // power of two
    count: usize,
}

impl<K: Ord + Clone, V: Clone> Pma<K, V> {
    pub fn new() -> Self {
        Self::with_capacity(LEAF_WINDOW)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.next_power_of_two().max(LEAF_WINDOW);
        Self {
            buf: vec![None; capacity],
            capacity,
            count: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Insert `(key, val)` in sorted order (duplicates allowed).
    pub fn insert(&mut self, key: K, val: V) {
        if (self.count + 1) as f64 / self.capacity as f64 > HIGH_WATERMARK {
            self.grow();
        }
        let pos = self.find_insert_index(&key).min(self.capacity - 1);
        if self.buf[pos].is_none() {
            self.buf[pos] = Some((key, val));
            self.count += 1;
            return;
        }
        // Slot occupied (or we're at the tail) → rebalance a window, then place.
        let (start, size) = self.rebalance_window_for(pos);
        self.redistribute(start, size, Some((key, val)));
    }

    /// Bulk-insert a pre-sorted batch (ascending by `K`) in one efficient pass:
    /// merge with the present elements and re-spread evenly. This is the path
    /// the mutable-run tier uses for a memtable drain, where one-at-a-time
    /// inserts would cluster at the tail and thrash. O(count + capacity).
    pub fn extend_sorted(&mut self, batch: Vec<(K, V)>) {
        if batch.is_empty() {
            return;
        }
        // Present elements are in ascending order by the PMA invariant.
        let present: Vec<(K, V)> = self.buf.iter().flatten().cloned().collect();
        let merged = merge_sorted(present, batch);
        self.count = merged.len();
        // Grow until the post-insert density has headroom (≤ half-full) so the
        // next drain doesn't immediately re-grow.
        let target_density = 0.5;
        while self.capacity < 2 || (self.count as f64 / self.capacity as f64) > target_density {
            self.capacity *= 2;
        }
        self.buf = vec![None; self.capacity];
        spread_evenly(&merged, &mut self.buf);
    }

    /// Read the value for the first entry with key == `key`, if present.
    pub fn get(&self, key: &K) -> Option<&V> {
        let idx = self.lower_bound_present(key)?;
        let (k, v) = self.buf[idx].as_ref().unwrap();
        if k.cmp(key) == std::cmp::Ordering::Equal {
            Some(v)
        } else {
            None
        }
    }

    /// Iterate present elements in ascending key order.
    pub fn iter(&self) -> impl Iterator<Item = &(K, V)> {
        self.buf.iter().flatten()
    }

    /// Drain all elements in ascending key order.
    pub fn drain_sorted(&mut self) -> Vec<(K, V)> {
        let mut out: Vec<(K, V)> = self.buf.iter().flatten().cloned().collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        for slot in self.buf.iter_mut() {
            *slot = None;
        }
        self.count = 0;
        out
    }

    // ---- internals -----------------------------------------------------

    /// Gappy binary search: the buffer index of the first present slot whose
    /// key is `>= *key`, or `None` when every present key is `< *key`. The PMA
    /// keeps present elements in ascending order, so binary search lands on a
    /// (possibly empty) slot; each probe walks past the bounded gap to the
    /// nearest present element — O(log² n) thanks to the density invariant.
    fn lower_bound_present(&self, key: &K) -> Option<usize> {
        let mut lo: usize = 0;
        let mut hi: usize = self.capacity;
        let mut best: Option<usize> = None;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.first_present_in(mid, hi) {
                Some(idx) => {
                    let (k, _) = self.buf[idx].as_ref().unwrap();
                    match k.cmp(key) {
                        std::cmp::Ordering::Less => lo = idx + 1,
                        std::cmp::Ordering::Equal => return Some(idx),
                        std::cmp::Ordering::Greater => {
                            best = Some(idx);
                            hi = idx;
                        }
                    }
                }
                None => hi = mid, // no present element in [mid, hi) → search left
            }
        }
        best
    }

    /// First present slot in `[from, until)`, scanning forward. The density
    /// invariant bounds the run of empties, so this is O(gap) = O(log n).
    fn first_present_in(&self, from: usize, until: usize) -> Option<usize> {
        self.buf[from..until]
            .iter()
            .position(|s| s.is_some())
            .map(|p| from + p)
    }

    /// First present index with key `>= *key`, or `capacity` if all present are
    /// `< *key`. (The gappy `O(log² n)` find — was linear.)
    fn find_insert_index(&self, key: &K) -> usize {
        self.lower_bound_present(key).unwrap_or(self.capacity)
    }

    /// Smallest power-of-two aligned window `[start, start+size)` (size ≥
    /// LEAF_WINDOW) containing `pos` whose density is below its threshold.
    fn rebalance_window_for(&self, pos: usize) -> (usize, usize) {
        let mut size = LEAF_WINDOW;
        loop {
            let start = (pos / size) * size;
            let present = self.buf[start..start + size]
                .iter()
                .filter(|s| s.is_some())
                .count();
            let density = (present + 1) as f64 / size as f64;
            let threshold = window_threshold(size, self.capacity);
            if density <= threshold || start + size > self.capacity {
                return (start, size.min(self.capacity - start));
            }
            size *= 2;
            if size > self.capacity {
                return (0, self.capacity);
            }
        }
    }

    /// Gather present elements of `[start, start+size)`, clear the window, and
    /// re-spread them (plus the optional `new` element) evenly with gaps.
    fn redistribute(&mut self, start: usize, size: usize, new: Option<(K, V)>) {
        let mut elems: Vec<(K, V)> = self.buf[start..start + size]
            .iter()
            .filter_map(|s| s.clone())
            .collect();
        if let Some(n) = new {
            let i = elems.partition_point(|(k, _)| k < &n.0);
            elems.insert(i, n);
        }
        for slot in self.buf[start..start + size].iter_mut() {
            *slot = None;
        }
        let m = elems.len();
        let gap = size as f64 / m as f64;
        for (rank, entry) in elems.into_iter().enumerate() {
            let idx = start + ((rank as f64 * gap).floor() as usize).min(size - 1);
            // If a collision lands two elements on the same slot, nudge right.
            let mut idx = idx;
            while idx < start + size && self.buf[idx].is_some() {
                idx += 1;
            }
            if idx < start + size {
                self.buf[idx] = Some(entry);
            }
        }
        self.count = self.buf.iter().filter(|s| s.is_some()).count();
    }

    fn grow(&mut self) {
        let mut elems: Vec<(K, V)> = self.buf.iter().filter_map(|s| s.clone()).collect();
        elems.sort_by(|a, b| a.0.cmp(&b.0));
        let new_cap = self.capacity * 2;
        let mut new_buf = vec![None; new_cap];
        let m = elems.len();
        if m > 0 {
            let gap = new_cap as f64 / m as f64;
            for (rank, entry) in elems.into_iter().enumerate() {
                let mut idx = (rank as f64 * gap).floor() as usize;
                while idx < new_cap && new_buf[idx].is_some() {
                    idx += 1;
                }
                if idx < new_cap {
                    new_buf[idx] = Some(entry);
                }
            }
        }
        self.buf = new_buf;
        self.capacity = new_cap;
        // count unchanged
    }
}

impl<K: Ord + Clone, V: Clone> Default for Pma<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

fn window_threshold(size: usize, capacity: usize) -> f64 {
    // Larger (higher-level) windows get looser thresholds so rebalances stay
    // local when possible. Root (size == capacity) is the global watermark.
    let frac = size as f64 / capacity as f64;
    0.55 + 0.35 * frac
}

/// Merge two ascending-sorted vectors into one ascending-sorted vector.
fn merge_sorted<K: Ord + Clone, V: Clone>(a: Vec<(K, V)>, b: Vec<(K, V)>) -> Vec<(K, V)> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut ai, mut bi) = (0, 0);
    while ai < a.len() && bi < b.len() {
        if a[ai].0 <= b[bi].0 {
            out.push(a[ai].clone());
            ai += 1;
        } else {
            out.push(b[bi].clone());
            bi += 1;
        }
    }
    while ai < a.len() {
        out.push(a[ai].clone());
        ai += 1;
    }
    while bi < b.len() {
        out.push(b[bi].clone());
        bi += 1;
    }
    out
}

/// Place `elems` (already ascending) into `buf` at evenly-spaced indices with
/// gaps, preserving order. Leaves every other slot `None`.
fn spread_evenly<K, V>(elems: &[(K, V)], buf: &mut [Option<(K, V)>])
where
    K: Clone,
    V: Clone,
{
    let n = elems.len();
    if n == 0 {
        return;
    }
    let cap = buf.len();
    let gap = cap as f64 / n as f64;
    let mut last_idx = 0;
    for (rank, entry) in elems.iter().enumerate() {
        let mut idx = (rank as f64 * gap).floor() as usize;
        // Nudge right past any collision (bounded by density).
        while idx < cap && buf[idx].is_some() {
            idx += 1;
        }
        if idx < cap {
            buf[idx] = Some(entry.clone());
            last_idx = idx;
        } else {
            // Ran out of room at the tail — place at the last free slot.
            buf[last_idx] = Some(entry.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserts_keep_sorted_and_complete() {
        let mut p: Pma<u64, &'static str> = Pma::new();
        let mut keys: Vec<u64> = (0..1000).collect();
        let n = keys.len();
        // Shuffle with a fixed pattern.
        for i in 0..n {
            keys.swap(i, (i * 7 + 3) % n);
        }
        for k in &keys {
            p.insert(*k, "v");
        }
        assert_eq!(p.len(), 1000);
        let collected: Vec<u64> = p.iter().map(|(k, _)| *k).collect();
        let mut sorted = collected.clone();
        sorted.sort();
        assert_eq!(collected, sorted, "elements must remain sorted");
        // All keys present and queryable.
        for k in &keys {
            assert_eq!(p.get(k), Some(&"v"));
        }
        assert_eq!(p.get(&500_000), None);
    }

    #[test]
    fn duplicates_allowed() {
        let mut p: Pma<u64, u64> = Pma::new();
        for _ in 0..5 {
            p.insert(7, 1);
        }
        assert_eq!(p.len(), 5);
    }

    #[test]
    fn drain_sorted_returns_all_ascending() {
        let mut p: Pma<u64, u64> = Pma::new();
        for k in [30u64, 10, 20, 5, 25] {
            p.insert(k, k * 2);
        }
        let out = p.drain_sorted();
        let keys: Vec<u64> = out.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![5, 10, 20, 25, 30]);
        assert_eq!(out[0].1, 10);
        assert!(p.is_empty());
    }

    #[test]
    fn extend_sorted_merges_and_preserves_order() {
        let mut p: Pma<u64, u64> = Pma::new();
        p.extend_sorted(vec![(10, 100), (30, 300)]);
        p.extend_sorted(vec![(20, 200), (40, 400)]);
        assert_eq!(p.len(), 4);
        let keys: Vec<u64> = p.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![10, 20, 30, 40]);
        for k in [10, 20, 30, 40] {
            assert_eq!(p.get(&k), Some(&(k * 10)));
        }
    }

    #[test]
    fn extend_sorted_handles_large_sorted_batch() {
        // The mutable-run tier drains tens of thousands of sorted rows at once;
        // this must stay fast (merge + re-spread, not per-element inserts).
        let mut p: Pma<u64, u64> = Pma::new();
        let batch: Vec<(u64, u64)> = (0..70_000).map(|i| (i, i)).collect();
        p.extend_sorted(batch);
        assert_eq!(p.len(), 70_000);
        let keys: Vec<u64> = p.iter().map(|(k, _)| *k).collect();
        let mut expect = keys.clone();
        expect.sort();
        assert_eq!(keys, expect);
        assert_eq!(p.get(&0), Some(&0));
        assert_eq!(p.get(&69_999), Some(&69_999));
        assert_eq!(p.get(&70_000), None);
    }
}
