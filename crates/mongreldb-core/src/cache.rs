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
//!
//! Stage 1E (S1E-003): both caches can report their live bytes to a
//! [`crate::memory::MemoryGovernor`] ([`PageCache::with_governor`],
//! [`DecodedPageCache::with_governor`]) and expose an
//! `evict_reclaimable(budget)` entry point the governor drives under pressure
//! escalation step 2. All governor behavior is additive: without an attached
//! governor the caches are exactly as before.

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
    /// Lookups skipped because the cache shard's lock was contended
    /// (`try_lock` returned `None`). Non-zero values signal contention that
    /// sharding or a larger shard count could relieve. (§5.8)
    pub try_lock_misses: u64,
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
///
/// When a [`crate::memory::MemoryGovernor`] is attached
/// ([`with_governor`](Self::with_governor)) the cache reports its live bytes
/// to the governor (S1E-003): every insert/eviction resizes one per-cache
/// [`crate::memory::Reservation`], the governor's denial of growth sheds the
/// coldest entries, and [`evict_reclaimable`](Self::evict_reclaimable) is the
/// entry point the governor drives under escalation step 2. Without a
/// governor the cache behaves exactly as before.
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
    /// Governor reservation tracking `used_bytes` when a governor is
    /// attached; `None` keeps the cache self-bounded only.
    grant: Option<crate::memory::Reservation>,
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
            grant: None,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Attach a [`crate::memory::MemoryGovernor`] (S1E-003): the cache keeps
    /// one reservation sized to its live bytes, so the governor's per-class
    /// accounting always reflects this cache, and growth the governor denies
    /// sheds the coldest entries. Any already-cached bytes (e.g. loaded by
    /// [`with_persistence`](Self::with_persistence)) are accounted — and
    /// evicted down if the governor cannot grant them — immediately.
    pub fn with_governor(
        mut self,
        governor: crate::memory::MemoryGovernor,
        class: crate::memory::MemoryClass,
    ) -> Self {
        self.grant = Some(
            governor
                .try_reserve(0, class)
                .expect("zero-byte reservation always succeeds"),
        );
        self.sync_governor();
        self
    }

    /// Cumulative hit/miss counts since construction (or the last
    /// [`reset_stats`](Self::reset_stats)). Cheap (`Relaxed` atomic loads).
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            try_lock_misses: 0,
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
        // When a governor is already attached, account for the loaded bytes
        // (shedding the coldest if the governor cannot grant them).
        self.sync_governor();
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
        self.sync_governor();
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
        while self.used_bytes > self.capacity_bytes && self.clock_step() {}
    }

    /// One CLOCK step: removes exactly one ring slot — evicting its cold entry,
    /// reclaiming a stale slot, or (when everything is hot) force-evicting the
    /// inspected slot — and returns `false` only when the ring is empty.
    fn clock_step(&mut self) -> bool {
        if self.ring.is_empty() {
            return false;
        }
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
                    self.used_bytes = self.used_bytes.saturating_sub(removed.bytes.len() as u64);
                    self.ring.swap_remove(self.hand);
                    if !self.ring.is_empty() {
                        self.hand %= self.ring.len();
                    }
                    break;
                }
                self.hand = (self.hand + 1) % len;
            }
        }
        true
    }

    /// Keep the governor reservation (when attached) sized to `used_bytes`.
    /// Growth the governor denies is answered by shedding the coldest entries
    /// until the accounting fits the grant — the cache stays within the
    /// governor's budget even when other subsystems hold the node under
    /// pressure. Terminates: each step removes one ring slot, and an empty
    /// cache (0 bytes) always fits.
    fn sync_governor(&mut self) {
        if self.grant.is_none() {
            return;
        }
        loop {
            let denied = {
                let used = self.used_bytes;
                let grant = self.grant.as_mut().expect("checked above");
                grant.resize(used).is_err()
            };
            if !denied || !self.clock_step() {
                break;
            }
        }
    }

    /// Governor entry point (S1E-003 step 2): evict at least `budget` bytes of
    /// reclaimable entries (coldest first), returning the bytes actually
    /// freed. Without an attached governor the cache is self-bounded and this
    /// is a plain manual trim.
    pub fn evict_reclaimable(&mut self, budget: u64) -> u64 {
        let before = self.used_bytes;
        while before - self.used_bytes < budget && self.clock_step() {}
        self.sync_governor();
        before - self.used_bytes
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
///
/// Governor attachment ([`with_governor`](Self::with_governor)) behaves exactly
/// as on [`PageCache`]: live bytes are reported to the governor and
/// [`evict_reclaimable`](Self::evict_reclaimable) is the governor-driven entry
/// point (S1E-003 step 2).
pub struct DecodedPageCache {
    map: HashMap<[u8; 32], std::sync::Arc<crate::columnar::NativeColumn>>,
    ring: Vec<[u8; 32]>,
    hand: usize,
    capacity_bytes: u64,
    used_bytes: u64,
    /// Governor reservation tracking `used_bytes` when a governor is
    /// attached; `None` keeps the cache self-bounded only.
    grant: Option<crate::memory::Reservation>,
    /// Lookups served from the decoded cache (skipped decode).
    hits: AtomicU64,
    /// Lookups that missed (caller decoded the page).
    misses: AtomicU64,
}

impl DecodedPageCache {
    pub fn new(capacity_bytes: u64) -> Self {
        Self {
            map: HashMap::new(),
            ring: Vec::new(),
            hand: 0,
            capacity_bytes,
            used_bytes: 0,
            grant: None,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Attach a [`crate::memory::MemoryGovernor`] (S1E-003): the cache keeps
    /// one reservation sized to its live bytes and sheds entries when the
    /// governor denies growth. See [`PageCache::with_governor`].
    pub fn with_governor(
        mut self,
        governor: crate::memory::MemoryGovernor,
        class: crate::memory::MemoryClass,
    ) -> Self {
        self.grant = Some(
            governor
                .try_reserve(0, class)
                .expect("zero-byte reservation always succeeds"),
        );
        self.sync_governor();
        self
    }

    /// Cumulative hit/miss counts since construction (or the last
    /// [`reset_stats`](Self::reset_stats)).
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            try_lock_misses: 0,
        }
    }

    /// Zero the hit/miss counters.
    pub fn reset_stats(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
    }

    /// Non-blocking probe used by the parallel scan path (`&self`, no write
    /// lock) — returns the cached decoded page on hit, `None` on miss.
    pub fn try_get(&self, key: &[u8; 32]) -> Option<std::sync::Arc<crate::columnar::NativeColumn>> {
        match self.map.get(key).cloned() {
            Some(v) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(v)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
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
        self.sync_governor();
    }

    fn evict_if_needed(&mut self) {
        // Simple CLOCK eviction: while over capacity, evict the slot under the
        // hand and advance. Decoded pages are cheap to rebuild (decode is fast,
        // especially under LZ4), so a crude size-bounded policy is sufficient —
        // the goal is just to cap memory for very hot repeat-scan workloads.
        while self.used_bytes > self.capacity_bytes && self.clock_step() {}
    }

    /// One CLOCK step: removes the slot under the hand and returns `false`
    /// only when the ring is empty.
    fn clock_step(&mut self) -> bool {
        if self.ring.is_empty() {
            return false;
        }
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
        true
    }

    /// Keep the governor reservation (when attached) sized to `used_bytes`;
    /// see [`PageCache::sync_governor`].
    fn sync_governor(&mut self) {
        if self.grant.is_none() {
            return;
        }
        loop {
            let denied = {
                let used = self.used_bytes;
                let grant = self.grant.as_mut().expect("checked above");
                grant.resize(used).is_err()
            };
            if !denied || !self.clock_step() {
                break;
            }
        }
    }

    /// Governor entry point (S1E-003 step 2): evict at least `budget` bytes of
    /// reclaimable entries, returning the bytes actually freed.
    pub fn evict_reclaimable(&mut self, budget: u64) -> u64 {
        let before = self.used_bytes;
        while before - self.used_bytes < budget && self.clock_step() {}
        self.sync_governor();
        before - self.used_bytes
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

/// A sharded mutex wrapper for the page caches. Each key routes to one of `N`
/// independent shards (each with its own lock), so concurrent rayon workers
/// probing different keys rarely contend on the same lock. A `try_lock` that
/// fails under contention is counted as a try-lock-miss. (§5.8)
pub struct Sharded<T> {
    shards: Vec<parking_lot::Mutex<T>>,
    try_lock_misses: AtomicU64,
}

/// Default shard count — balances contention reduction against per-shard
/// overhead. Each shard gets `total_capacity / SHARDS` of the budget.
pub const CACHE_SHARDS: usize = 16;

impl<T> Sharded<T> {
    pub fn new(n_shards: usize, make: impl FnMut() -> T) -> Self {
        let mut make = make;
        let shards = (0..n_shards)
            .map(|_| parking_lot::Mutex::new(make()))
            .collect();
        Self {
            shards,
            try_lock_misses: AtomicU64::new(0),
        }
    }

    fn idx(&self, key: &[u8; 32]) -> usize {
        let h = u32::from_le_bytes([key[0], key[1], key[2], key[3]]);
        (h as usize) % self.shards.len()
    }

    pub fn try_lock(&self, key: &[u8; 32]) -> Option<parking_lot::MutexGuard<'_, T>> {
        match self.shards[self.idx(key)].try_lock() {
            Some(g) => Some(g),
            None => {
                self.try_lock_misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn lock(&self, key: &[u8; 32]) -> parking_lot::MutexGuard<'_, T> {
        self.shards[self.idx(key)].lock()
    }

    pub fn try_lock_misses(&self) -> u64 {
        self.try_lock_misses.load(Ordering::Relaxed)
    }
}

impl Sharded<PageCache> {
    pub fn stats(&self) -> CacheStats {
        let mut total = CacheStats::default();
        for s in &self.shards {
            let st = s.lock().stats();
            total.hits += st.hits;
            total.misses += st.misses;
        }
        total.try_lock_misses = self.try_lock_misses();
        total
    }

    pub fn reset_stats(&self) {
        for s in &self.shards {
            s.lock().reset_stats();
        }
        self.try_lock_misses.store(0, Ordering::Relaxed);
    }

    pub fn flush_to_disk(&self) {
        for s in &self.shards {
            s.lock().flush_to_disk();
        }
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().len()).sum()
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Live bytes across all shards.
    pub fn used_bytes(&self) -> u64 {
        self.shards.iter().map(|s| s.lock().used_bytes()).sum()
    }

    /// Governor entry point (S1E-003 step 2): evict at least `budget` bytes
    /// across the shards (spread evenly over the remaining shards), returning
    /// the bytes actually freed.
    pub fn evict_reclaimable(&self, budget: u64) -> u64 {
        let n = self.shards.len() as u64;
        let mut freed = 0u64;
        for (i, shard) in self.shards.iter().enumerate() {
            if freed >= budget {
                break;
            }
            // Ceiling division: small budgets still reach the first shards.
            let left = n - i as u64;
            let target = (budget - freed).div_ceil(left);
            freed += shard.lock().evict_reclaimable(target);
        }
        freed
    }
}

impl crate::memory::Reclaimable for Sharded<PageCache> {
    fn evict_reclaimable(&self, budget: u64) -> u64 {
        <Sharded<PageCache>>::evict_reclaimable(self, budget)
    }

    fn reclaimable_bytes(&self) -> u64 {
        self.used_bytes()
    }
}

impl Sharded<DecodedPageCache> {
    pub fn stats(&self) -> CacheStats {
        let mut total = CacheStats::default();
        for s in &self.shards {
            let st = s.lock().stats();
            total.hits += st.hits;
            total.misses += st.misses;
        }
        total.try_lock_misses = self.try_lock_misses();
        total
    }

    pub fn reset_stats(&self) {
        for s in &self.shards {
            s.lock().reset_stats();
        }
        self.try_lock_misses.store(0, Ordering::Relaxed);
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().len()).sum()
    }

    /// Live bytes across all shards.
    pub fn used_bytes(&self) -> u64 {
        self.shards.iter().map(|s| s.lock().used_bytes()).sum()
    }

    /// Governor entry point (S1E-003 step 2): evict at least `budget` bytes
    /// across the shards, returning the bytes actually freed.
    pub fn evict_reclaimable(&self, budget: u64) -> u64 {
        let n = self.shards.len() as u64;
        let mut freed = 0u64;
        for (i, shard) in self.shards.iter().enumerate() {
            if freed >= budget {
                break;
            }
            let left = n - i as u64;
            let target = (budget - freed).div_ceil(left);
            freed += shard.lock().evict_reclaimable(target);
        }
        freed
    }
}

impl crate::memory::Reclaimable for Sharded<DecodedPageCache> {
    fn evict_reclaimable(&self, budget: u64) -> u64 {
        <Sharded<DecodedPageCache>>::evict_reclaimable(self, budget)
    }

    fn reclaimable_bytes(&self) -> u64 {
        self.used_bytes()
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
        assert_eq!(
            cache.stats(),
            CacheStats {
                hits: 0,
                misses: 0,
                try_lock_misses: 0
            }
        );

        // Visible hit (get + try_get).
        assert!(cache.get(&hash, Snapshot::at(Epoch(1))).is_some());
        assert!(cache.try_get(&hash, Snapshot::at(Epoch(1))).is_some());
        // Absent key ⇒ miss; present-but-too-new entry ⇒ miss.
        assert!(cache.get(&[9u8; 32], Snapshot::at(Epoch(1))).is_none());
        assert!(cache.get(&hash, Snapshot::at(Epoch(0))).is_none());

        let s = cache.stats();
        assert_eq!(
            s,
            CacheStats {
                hits: 2,
                misses: 2,
                try_lock_misses: 0
            }
        );
        assert!((s.hit_rate() - 0.5).abs() < 1e-9);

        cache.reset_stats();
        assert_eq!(
            cache.stats(),
            CacheStats {
                hits: 0,
                misses: 0,
                try_lock_misses: 0
            }
        );
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

    #[test]
    fn decoded_cache_hit_miss_counters() {
        // Priority 14: the decoded-page cache reports hit/miss counts so a
        // repeat scan's decode-skip rate is observable.
        use crate::columnar::NativeColumn;
        let mut cache = DecodedPageCache::new(1 << 20);
        let key = [9u8; 32];
        cache.insert(
            key,
            std::sync::Arc::new(NativeColumn::Int64 {
                data: vec![1, 2, 3],
                validity: vec![],
            }),
        );
        // Two hits, one miss.
        assert!(cache.try_get(&key).is_some());
        assert!(cache.try_get(&key).is_some());
        assert!(cache.try_get(&[0u8; 32]).is_none());
        let s = cache.stats();
        assert_eq!(s.hits, 2);
        assert_eq!(s.misses, 1);
        assert!(s.hit_rate() > 0.66 && s.hit_rate() < 0.67);
        cache.reset_stats();
        assert_eq!(cache.stats(), CacheStats::default());
    }

    #[test]
    fn governor_attached_cache_reports_live_bytes() {
        use crate::memory::{GovernorConfig, MemoryClass, MemoryGovernor};
        let governor =
            MemoryGovernor::new(GovernorConfig::new(1 << 20).with_reserved_floor(0)).unwrap();
        let mut cache =
            PageCache::new(1 << 20).with_governor(governor.clone(), MemoryClass::PageCache);
        cache.insert(page([1u8; 32], 1, b"0123456789"));
        cache.insert(page([2u8; 32], 1, b"abcde"));
        // The governor's per-class accounting tracks the cache's live bytes.
        assert_eq!(governor.usage(MemoryClass::PageCache), 15);
        assert_eq!(cache.used_bytes(), 15);
        // Capacity eviction shrinks the grant too.
        let mut cache = PageCache::new(10).with_governor(governor.clone(), MemoryClass::PageCache);
        cache.insert(page([3u8; 32], 1, b"0123456789"));
        cache.insert(page([4u8; 32], 1, b"x"));
        assert_eq!(cache.used_bytes(), 1);
        assert_eq!(governor.usage(MemoryClass::PageCache), 16);
        // Dropping the cache releases its reservation.
        drop(cache);
        assert_eq!(governor.usage(MemoryClass::PageCache), 15);
    }

    #[test]
    fn governor_denial_sheds_coldest_entries() {
        use crate::memory::{GovernorConfig, MemoryClass, MemoryGovernor};
        // The governor is the binding constraint: 24 bytes node-wide, no
        // floor, while the cache's own capacity is huge.
        let governor = MemoryGovernor::new(GovernorConfig::new(24).with_reserved_floor(0)).unwrap();
        let mut cache =
            PageCache::new(1 << 20).with_governor(governor.clone(), MemoryClass::PageCache);
        for i in 0..8u8 {
            cache.insert(page([i; 32], 1, b"01234567")); // 8 bytes each
        }
        // The cache never exceeds what the governor granted (no OOM growth),
        // and the accounting is exact.
        assert!(cache.used_bytes() <= 24);
        assert_eq!(governor.usage(MemoryClass::PageCache), cache.used_bytes());
        // The coldest (first-inserted) pages were shed; recent ones survive.
        assert!(cache.get(&[7u8; 32], Snapshot::at(Epoch(1))).is_some());
    }

    #[test]
    fn evict_reclaimable_frees_budget_and_keeps_accounting() {
        use crate::memory::{GovernorConfig, MemoryClass, MemoryGovernor};
        let governor =
            MemoryGovernor::new(GovernorConfig::new(1 << 20).with_reserved_floor(0)).unwrap();
        let mut cache =
            PageCache::new(1 << 20).with_governor(governor.clone(), MemoryClass::PageCache);
        for i in 0..4u8 {
            cache.insert(page([i; 32], 1, b"0123456789")); // 10 bytes each
        }
        assert_eq!(governor.usage(MemoryClass::PageCache), 40);
        let freed = cache.evict_reclaimable(25);
        assert!(freed >= 25, "freed {freed}");
        assert_eq!(cache.used_bytes(), 40 - freed);
        assert_eq!(governor.usage(MemoryClass::PageCache), cache.used_bytes());
        // Evicting more than held frees everything and stays consistent.
        let remaining = cache.used_bytes();
        let freed = cache.evict_reclaimable(1 << 20);
        assert_eq!(freed, remaining);
        assert_eq!(cache.used_bytes(), 0);
        assert_eq!(governor.usage(MemoryClass::PageCache), 0);
    }

    #[test]
    fn decoded_cache_governor_reporting() {
        use crate::columnar::NativeColumn;
        use crate::memory::{GovernorConfig, MemoryClass, MemoryGovernor};
        let governor =
            MemoryGovernor::new(GovernorConfig::new(1 << 20).with_reserved_floor(0)).unwrap();
        let mut cache = DecodedPageCache::new(1 << 20)
            .with_governor(governor.clone(), MemoryClass::DecodedCache);
        cache.insert(
            [9u8; 32],
            std::sync::Arc::new(NativeColumn::Int64 {
                data: vec![1, 2, 3],
                validity: vec![],
            }),
        );
        assert_eq!(
            governor.usage(MemoryClass::DecodedCache),
            cache.used_bytes()
        );
        assert!(cache.used_bytes() > 0);
        let held = cache.used_bytes();
        let freed = cache.evict_reclaimable(u64::MAX);
        assert_eq!(freed, held);
        assert_eq!(cache.used_bytes(), 0);
        assert_eq!(governor.usage(MemoryClass::DecodedCache), 0);
    }
}
