//! MVCC-tagged, content-addressed page cache.
//!
//! Correctness by construction: every entry carries the [`Epoch`] at which its
//! page was committed, and the key is the page's content/identity hash. A query
//! at [`Snapshot`] only reads entries with
//! `committed_epoch <= snapshot.epoch`; a rewritten page gets a new hash and the
//! old entry ages out by capacity — so **no invalidation sweep ever runs**.
//!
//! Eviction is a frequency-aware CLOCK (a.k.a. second-chance): each entry has a
//! small access counter; the clock hand sweeps and either evicts a cold
//! (counter == 0) entry or decrements a hot one (giving it a second chance).
//! This blends recency (the hand sweep) with frequency (the counter) — an
//! LRU/LFU hybrid — in O(1) amortized with no linked-list moves.
//!
//! An optional persistent cache under `_cache/` (raw on-disk bytes — ciphertext
//! when the table is encrypted, so no plaintext ever persists) survives restart.

use crate::epoch::{Epoch, Snapshot};
use crate::page::CachedPage;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative page-cache access counters (Priority 14: hit visibility). A *hit*
/// is a lookup that returned a page visible to the snapshot; a *miss* is a
/// lookup that found nothing or an entry too new for the snapshot (the caller
/// then reads from disk).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
}

impl CacheStats {
    /// Fraction of lookups served from cache in `[0, 1]` (`0` when never used).
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// Bounded, MVCC-safe page cache with frequency-aware CLOCK eviction and an
/// optional persistent backing directory.
pub struct PageCache {
    map: HashMap<[u8; 32], Entry>,
    /// Clock hand indexes into `ring`; `ring` holds every live key once.
    ring: Vec<[u8; 32]>,
    hand: usize,
    capacity_bytes: u64,
    used_bytes: u64,
    /// Backing directory for the persistent cache (`_cache/`). Entries are
    /// spilled/loaded as `<hex(key)>` files (raw on-disk bytes).
    dir: Option<PathBuf>,
    persistent: bool,
    /// Lookups served from cache (visible to the snapshot).
    hits: AtomicU64,
    /// Lookups that found nothing visible (caller falls through to disk).
    misses: AtomicU64,
}

struct Entry {
    bytes: bytes::Bytes,
    epoch: Epoch,
    freq: u8,
}

const FREQ_MAX: u8 = 3;

impl PageCache {
    pub fn new(capacity_bytes: u64) -> Self {
        Self {
            map: HashMap::new(),
            ring: Vec::new(),
            hand: 0,
            capacity_bytes,
            used_bytes: 0,
            dir: None,
            persistent: false,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Cumulative hit/miss counts since construction (or the last
    /// [`reset_stats`](Self::reset_stats)). Cheap (`Relaxed` atomic loads).
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }

    /// Zero the hit/miss counters (e.g. to measure a single query's locality).
    pub fn reset_stats(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
    }

    /// Enable persistence: load any cached pages from `<dir>` and spill future
    /// evictions/inserts there. Files are raw page bytes (ciphertext when the
    /// table is encrypted).
    pub fn with_persistence(mut self, dir: PathBuf) -> Self {
        self.persistent = true;
        self.dir = Some(dir.clone());
        self.load_from_disk();
        self
    }

    /// Insert a page (replaces any entry with the same key). Spills to the
    /// persistent backing directory when enabled.
    pub fn insert(&mut self, page: CachedPage) {
        let key = page.content_hash;
        let size = page.bytes.len() as u64;
        let epoch = page.committed_epoch;
        let was_new = !self.map.contains_key(&key);
        if let Some(old) = self.map.insert(
            key,
            Entry {
                bytes: page.bytes.clone(),
                epoch,
                freq: 1,
            },
        ) {
            self.used_bytes = self.used_bytes.saturating_sub(old.bytes.len() as u64);
        } else if was_new {
            self.ring.push(key);
        }
        self.used_bytes += size;
        self.evict_if_needed();
    }

    /// Fetch the page visible to `snapshot` (the entry's committed epoch must be
    /// `<= snapshot.epoch`), promoting its frequency. O(1).
    pub fn get(&mut self, content_hash: &[u8; 32], snapshot: Snapshot) -> Option<bytes::Bytes> {
        let hit = self
            .map
            .get_mut(content_hash)
            .filter(|e| e.epoch <= snapshot.epoch)
            .map(|entry| {
                if entry.freq < FREQ_MAX {
                    entry.freq += 1;
                }
                entry.bytes.clone()
            });
        let counter = if hit.is_some() {
            &self.hits
        } else {
            &self.misses
        };
        counter.fetch_add(1, Ordering::Relaxed);
        hit
    }

    /// Non-blocking probe used by the parallel (rayon) read path: returns a hit
    /// without contending on a write lock when one is held. `Some` on hit;
    /// `None` on miss (or if the value is not visible to `snapshot`). The
    /// caller treats any `None` as "fall through to disk".
    pub fn try_get(&self, content_hash: &[u8; 32], snapshot: Snapshot) -> Option<bytes::Bytes> {
        let hit = self.map.get(content_hash).and_then(|e| {
            if e.epoch <= snapshot.epoch {
                Some(e.bytes.clone())
            } else {
                None
            }
        });
        let counter = if hit.is_some() {
            &self.hits
        } else {
            &self.misses
        };
        counter.fetch_add(1, Ordering::Relaxed);
        hit
    }

    pub fn used_bytes(&self) -> u64 {
        self.used_bytes
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Flush all live entries to the persistent backing dir (best-effort).
    pub fn flush_to_disk(&self) {
        if !self.persistent {
            return;
        }
        for (key, entry) in &self.map {
            self.spill(key, entry.epoch, &entry.bytes);
        }
    }

    fn spill(&self, key: &[u8; 32], epoch: Epoch, bytes: &[u8]) {
        if !self.persistent {
            return;
        }
        let Some(dir) = &self.dir else { return };
        let _ = std::fs::create_dir_all(dir);
        let hex = hex_key(key);
        // Embed the committed epoch in the filename (`<hex>.<epoch>`) so it can
        // be restored on reload instead of defaulting to a maximal epoch —
        // otherwise reloaded pages are invisible to ordinary snapshots and the
        // persistent tier is effectively dead after reopen. The page bytes stay
        // raw (ciphertext when the table is encrypted).
        let path = dir.join(format!("{hex}.{}", epoch.0));
        let tmp = dir.join(format!("{hex}.{}.tmp", epoch.0));
        if std::fs::write(&tmp, bytes).is_ok() {
            let _ = std::fs::rename(&tmp, &path);
        }
    }

    fn load_from_disk(&mut self) {
        let Some(dir) = &self.dir else { return };
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(s) = name.to_str() else { continue };
            // Skip in-progress temp files.
            if s.ends_with(".tmp") {
                continue;
            }
            // New format: "<64hex>.<epoch>". Legacy raw files ("<64hex>" with
            // no dot) stay visible to every snapshot.
            let (key, epoch) = match s.rsplit_once('.') {
                Some((hex, suffix)) => {
                    let key = match decode_hex_key(hex) {
                        Some(k) => k,
                        None => continue,
                    };
                    let epoch = match suffix.parse::<u64>() {
                        Ok(e) => Epoch(e),
                        Err(_) => continue,
                    };
                    (key, epoch)
                }
                None => {
                    let key = match decode_hex_key(s) {
                        Some(k) => k,
                        None => continue,
                    };
                    (key, Epoch(u64::MAX))
                }
            };
            if self.map.contains_key(&key) {
                continue;
            }
            let Ok(bytes) = std::fs::read(entry.path()) else {
                continue;
            };
            let size = bytes.len() as u64;
            if self.used_bytes.saturating_add(size) > self.capacity_bytes {
                break; // don't exceed capacity on load
            }
            self.map.insert(
                key,
                Entry {
                    bytes: bytes::Bytes::from(bytes),
                    epoch,
                    freq: 0,
                },
            );
            self.ring.push(key);
            self.used_bytes += size;
        }
    }

    /// Frequency-aware CLOCK eviction: advance the hand, evicting the first cold
    /// (freq == 0) entry; decrement hot entries as they're passed (second chance).
    fn evict_if_needed(&mut self) {
        if self.ring.is_empty() {
            return;
        }
        while self.used_bytes > self.capacity_bytes {
            // Find a cold slot. Bounded by ~2× live entries per eviction storm.
            let mut scanned = 0;
            let len = self.ring.len();
            loop {
                let key = self.ring[self.hand];
                let Some(entry) = self.map.get_mut(&key) else {
                    // Stale ring slot (key removed); reclaim it.
                    self.ring.swap_remove(self.hand);
                    if self.hand >= self.ring.len() && !self.ring.is_empty() {
                        self.hand %= self.ring.len();
                    }
                    break;
                };
                if entry.freq == 0 {
                    // Evict this cold entry.
                    let removed = self.map.remove(&key).expect("entry present");
                    self.spill(&key, removed.epoch, &removed.bytes);
                    self.used_bytes = self.used_bytes.saturating_sub(removed.bytes.len() as u64);
                    self.ring.swap_remove(self.hand);
                    if !self.ring.is_empty() {
                        self.hand %= self.ring.len();
                    }
                    break;
                } else {
                    entry.freq -= 1;
                    scanned += 1;
                    if scanned > len * 2 {
                        // Everything is hot; evict the current slot outright. The
                        // hand has NOT been advanced yet, so this removes the slot
                        // we just inspected (not an innocent neighbor).
                        let removed = self.map.remove(&key).expect("entry present");
                        self.spill(&key, removed.epoch, &removed.bytes);
                        self.used_bytes =
                            self.used_bytes.saturating_sub(removed.bytes.len() as u64);
                        self.ring.swap_remove(self.hand);
                        if !self.ring.is_empty() {
                            self.hand %= self.ring.len();
                        }
                        break;
                    }
                    self.hand = (self.hand + 1) % len;
                }
            }
            if self.ring.is_empty() {
                break;
            }
        }
    }
}

fn hex_key(key: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in key {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn decode_hex_key(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// Bounded LRU/CLOCK cache of **decoded** columnar pages (Phase 15.4). The
/// [`PageCache`] above holds raw (ciphertext) page bytes; this second layer
/// caches the post-decompress, post-decrypt typed page so a repeat scan skips
/// decode entirely. Pages are immutable per `(run_id, column_id, page_seq)`
/// identity (keyed via [`crate::sorted_run::page_cache_key`]), so there is no
/// MVCC/invalidation concern — runs never change, and a rewritten page lives in
/// a different run (different id) and simply misses here.
pub struct DecodedPageCache {
    map: HashMap<[u8; 32], std::sync::Arc<crate::columnar::NativeColumn>>,
    ring: Vec<[u8; 32]>,
    hand: usize,
    capacity_bytes: u64,
    used_bytes: u64,
}

impl DecodedPageCache {
    pub fn new(capacity_bytes: u64) -> Self {
        Self {
            map: HashMap::new(),
            ring: Vec::new(),
            hand: 0,
            capacity_bytes,
            used_bytes: 0,
        }
    }

    /// Non-blocking probe used by the parallel scan path (`&self`, no write
    /// lock) — returns the cached decoded page on hit, `None` on miss.
    pub fn try_get(&self, key: &[u8; 32]) -> Option<std::sync::Arc<crate::columnar::NativeColumn>> {
        self.map.get(key).cloned()
    }

    /// Insert a decoded page (replaces any entry with the same key). Evicts
    /// cold entries (CLOCK) when over capacity.
    pub fn insert(&mut self, key: [u8; 32], col: std::sync::Arc<crate::columnar::NativeColumn>) {
        let size = col.approx_bytes();
        let was_new = !self.map.contains_key(&key);
        if let Some(old) = self.map.insert(key, col) {
            self.used_bytes = self.used_bytes.saturating_sub(old.approx_bytes());
        } else if was_new {
            self.ring.push(key);
        }
        self.used_bytes += size;
        self.evict_if_needed();
    }

    fn evict_if_needed(&mut self) {
        // Simple CLOCK eviction: while over capacity, evict the slot under the
        // hand and advance. Decoded pages are cheap to rebuild (decode is fast,
        // especially under LZ4), so a crude size-bounded policy is sufficient —
        // the goal is just to cap memory for very hot repeat-scan workloads.
        while self.used_bytes > self.capacity_bytes && !self.ring.is_empty() {
            let idx = self.hand.min(self.ring.len() - 1);
            let key = self.ring.swap_remove(idx);
            if let Some(col) = self.map.remove(&key) {
                self.used_bytes = self.used_bytes.saturating_sub(col.approx_bytes());
            }
            if self.ring.is_empty() {
                self.hand = 0;
            } else {
                self.hand %= self.ring.len();
            }
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn used_bytes(&self) -> u64 {
        self.used_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn page(hash: [u8; 32], epoch: u64, data: &[u8]) -> CachedPage {
        CachedPage {
            committed_epoch: Epoch(epoch),
            content_hash: hash,
            bytes: bytes::Bytes::copy_from_slice(data),
        }
    }

    #[test]
    fn mvcc_visibility() {
        let mut cache = PageCache::new(1 << 20);
        let hash = [1u8; 32];
        cache.insert(page(hash, 3, b"v3"));
        // A snapshot at epoch 2 must not see the epoch-3 page.
        assert!(cache.get(&hash, Snapshot::at(Epoch(2))).is_none());
        // A snapshot at epoch 3 does.
        assert_eq!(
            cache.get(&hash, Snapshot::at(Epoch(3))),
            Some(bytes::Bytes::copy_from_slice(b"v3"))
        );
    }

    #[test]
    fn hit_miss_counters_track_lookups() {
        let mut cache = PageCache::new(1 << 20);
        let hash = [7u8; 32];
        cache.insert(page(hash, 1, b"v"));
        assert_eq!(cache.stats(), CacheStats { hits: 0, misses: 0 });

        // Visible hit (get + try_get).
        assert!(cache.get(&hash, Snapshot::at(Epoch(1))).is_some());
        assert!(cache.try_get(&hash, Snapshot::at(Epoch(1))).is_some());
        // Absent key ⇒ miss; present-but-too-new entry ⇒ miss.
        assert!(cache.get(&[9u8; 32], Snapshot::at(Epoch(1))).is_none());
        assert!(cache.get(&hash, Snapshot::at(Epoch(0))).is_none());

        let s = cache.stats();
        assert_eq!(s, CacheStats { hits: 2, misses: 2 });
        assert!((s.hit_rate() - 0.5).abs() < 1e-9);

        cache.reset_stats();
        assert_eq!(cache.stats(), CacheStats { hits: 0, misses: 0 });
        assert_eq!(cache.stats().hit_rate(), 0.0);
    }

    #[test]
    fn content_addressed_replacement_ages_out_old() {
        let mut cache = PageCache::new(1 << 20);
        let hash_a = [1u8; 32];
        let hash_b = [2u8; 32];
        cache.insert(page(hash_a, 1, b"old"));
        cache.insert(page(hash_b, 1, b"new"));
        assert_eq!(cache.len(), 2);
        assert!(cache.get(&hash_a, Snapshot::at(Epoch(1))).is_some());
    }

    #[test]
    fn capacity_eviction_removes_cold_first() {
        let mut cache = PageCache::new(10);
        cache.insert(page([1u8; 32], 1, b"0123456789")); // exactly 10 bytes
        assert_eq!(cache.used_bytes(), 10);
        cache.insert(page([2u8; 32], 1, b"x")); // over → evicts the cold first page
        assert_eq!(cache.used_bytes(), 1);
        assert!(cache.get(&[1u8; 32], Snapshot::at(Epoch(1))).is_none());
        assert!(cache.get(&[2u8; 32], Snapshot::at(Epoch(1))).is_some());
    }

    #[test]
    fn frequency_keeps_hot_pages_alive() {
        // Capacity for ~2 entries. Access A repeatedly; it should survive the
        // insertion of B and C while the cold ones are evicted.
        let mut cache = PageCache::new(2);
        let a = [10u8; 32];
        cache.insert(page(a, 1, b"A"));
        // Bump A's frequency so the clock hand gives it second chances.
        for _ in 0..5 {
            let _ = cache.get(&a, Snapshot::at(Epoch(1)));
        }
        cache.insert(page([20u8; 32], 1, b"B"));
        cache.insert(page([30u8; 32], 1, b"C"));
        // A is hot → should still be resident.
        assert!(
            cache.get(&a, Snapshot::at(Epoch(1))).is_some(),
            "hot page should survive eviction"
        );
    }

    #[test]
    fn persistent_cache_survives_restart() {
        let dir = tempdir().unwrap();
        let data = b"page-payload-1234";
        let hash = [42u8; 32];

        // First cache: insert + spill.
        {
            let mut cache = PageCache::new(1 << 20).with_persistence(dir.path().to_path_buf());
            cache.insert(page(hash, 5, data));
            cache.flush_to_disk();
        }
        // The backing file exists (named `<hex>.<epoch>`).
        let backing = dir.path().join(format!("{}.{}", hex_key(&hash), 5));
        assert!(backing.exists(), "cache file should be spilled");

        // Second cache (simulating reopen): loads the spilled page.
        let mut cache = PageCache::new(1 << 20).with_persistence(dir.path().to_path_buf());
        let got = cache.get(&hash, Snapshot::at(Epoch(u64::MAX)));
        assert_eq!(got, Some(bytes::Bytes::copy_from_slice(data)));
    }

    #[test]
    fn try_get_does_not_block() {
        let mut cache = PageCache::new(1 << 20);
        let hash = [7u8; 32];
        cache.insert(page(hash, 1, b"hi"));
        let got = cache.try_get(&hash, Snapshot::at(Epoch(1)));
        assert!(got.is_some());
        let miss = cache.try_get(&[8u8; 32], Snapshot::at(Epoch(1)));
        assert!(miss.is_none());
    }

    #[test]
    fn hot_eviction_storm_does_not_orphan_entries() {
        // Regression: when every entry is hot, the CLOCK fallback must evict the
        // slot it actually inspected (not an innocent neighbor), otherwise orphaned
        // entries accumulate and used_bytes drifts permanently above capacity.
        let mut cache = PageCache::new(6); // room for ~3 two-byte pages
        let mut next = 1u8;
        for _ in 0..200 {
            let key = [next; 32];
            cache.insert(page(key, 1, b"aa")); // 2 bytes each
                                               // Access the page repeatedly so it becomes hot (forces the fallback
                                               // path as the clock hand sweeps only-hot entries on later evictions).
            for _ in 0..4 {
                let _ = cache.get(&key, Snapshot::at(Epoch(1)));
            }
            next = next.wrapping_add(1);
        }
        // After many hot evictions, used_bytes must respect capacity (no orphan
        // drift) and every live ring slot must resolve to a map entry.
        assert!(
            cache.used_bytes() <= cache.capacity_bytes + 2, // +one page in flight
            "used_bytes {} leaked past capacity {}",
            cache.used_bytes(),
            cache.capacity_bytes
        );
        // Consistency: ring and map agree (no orphans either way).
        let mut ok = true;
        for k in &cache.ring {
            if !cache.map.contains_key(k) {
                ok = false;
            }
        }
        assert!(
            ok,
            "ring references a key absent from the map (orphan slot)"
        );
        assert_eq!(
            cache.ring.len(),
            cache.map.len(),
            "ring/map size mismatch (orphaned entries or slots)"
        );
    }
}
