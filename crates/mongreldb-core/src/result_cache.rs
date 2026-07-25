//! Persistent result-cache publication (TODO §2).
//!
//! Goal: result-cache inserts must do **no** serialization, encryption, write,
//! sync, or rename on the query thread. The query thread enqueues a cheap
//! `PersistableEntry` (Arc-shared rows + scalars) onto a bounded coalescing
//! pending map; a background worker drains the map and performs the actual
//! disk write.
//!
//! The current file is the bounded-coalescing skeleton; the writer thread,
//! encryption, encryption-key handling, and crash-recovery tests land in PR C.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use crate::engine::{LookupMetrics, LookupMetricsSnapshot};

/// A pending operation for one cache key. Later writes supersede earlier
/// writes; a `Remove` supersedes a `Store`. `Clear` invalidates every older
/// op under the same generation.
#[derive(Debug, Clone)]
pub enum PendingCacheOp {
    Store(PersistableEntry),
    Remove,
}

/// Cheap-to-share body for a queued store. Arc-shared rows + scalar metadata
/// avoid cloning the row bytes on enqueue. Bincode serialization + encryption
/// run on the worker.
#[derive(Debug, Clone)]
pub struct PersistableEntry {
    pub key: u64,
    pub table_id: u64,
    pub schema_id: u64,
    pub run_generation: u64,
    pub entry_generation: u64,
    pub footprint: Arc<roaring::RoaringBitmap>,
    pub rows: Arc<[u8]>,
    pub columns: Arc<[u16]>,
    /// Approximate in-memory bytes; used for queue accounting.
    pub bytes: usize,
}

/// Generationed state of the pending-operation map. The worker publishes
/// stores only when the queued generation still matches the latest
/// generation seen, so a newer invalidate cannot be silently overwritten.
#[derive(Debug, Default)]
pub struct PendingCacheState {
    pub generation: u64,
    pub clear_generation: u64,
    pub operations: HashMap<u64, PendingCacheOp>,
    pub approx_bytes: usize,
}

/// Capacity knobs for the writer. Held behind `Arc` so the worker can read
/// without taking the queue lock.
#[derive(Debug, Clone)]
pub struct WriterLimits {
    pub max_pending_keys: usize,
    pub max_pending_bytes: usize,
}

impl Default for WriterLimits {
    fn default() -> Self {
        Self {
            max_pending_keys: 16_384,
            max_pending_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Combined state guarded by the queue mutex.
struct Inner {
    state: PendingCacheState,
    limits: WriterLimits,
    shutdown: bool,
}

/// The PersistentResultCacheWriter skeleton. The query thread calls
/// [`PersistentResultCacheWriter::enqueue_store`] / [`enqueue_remove`] /
/// `enqueue_clear`; the worker calls [`drain_one`] in a loop.
pub struct PersistentResultCacheWriter {
    inner: Mutex<Inner>,
    cond: Condvar,
    metrics: LookupMetrics,
    enqueued_total: AtomicU64,
    coalesced_total: AtomicU64,
    dropped_total: AtomicU64,
    remove_total: AtomicU64,
}

impl PersistentResultCacheWriter {
    pub fn new(metrics: LookupMetrics, limits: WriterLimits) -> Self {
        Self {
            inner: Mutex::new(Inner {
                state: PendingCacheState::default(),
                limits,
                shutdown: false,
            }),
            cond: Condvar::new(),
            metrics,
            enqueued_total: AtomicU64::new(0),
            coalesced_total: AtomicU64::new(0),
            dropped_total: AtomicU64::new(0),
            remove_total: AtomicU64::new(0),
        }
    }

    /// Enqueue a `Store` op. Coalesces with a previous `Store` for the same
    /// key; the new entry supersedes the old one. Drops the op and increments
    /// the dropped counter when the queue is full.
    pub fn enqueue_store(&self, entry: PersistableEntry) {
        let mut guard = self.inner.lock().expect("writer mutex poisoned");
        let key = entry.key;
        let bytes = entry.bytes;
        if guard.state.operations.len() >= guard.limits.max_pending_keys
            || guard.state.approx_bytes + bytes > guard.limits.max_pending_bytes
        {
            self.dropped_total.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .result_cache_persist_dropped_store_total
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        match guard.state.operations.get(&key) {
            Some(PendingCacheOp::Store(_)) => {
                self.coalesced_total.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .result_cache_persist_coalesced_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            Some(PendingCacheOp::Remove) => {
                // A pending Remove wins: drop the new Store silently.
                self.dropped_total.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .result_cache_persist_dropped_store_total
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            None => {}
        }
        guard
            .state
            .operations
            .insert(key, PendingCacheOp::Store(entry));
        guard.state.approx_bytes += bytes;
        guard.state.generation += 1;
        self.enqueued_total.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .result_cache_persist_enqueued_total
            .fetch_add(1, Ordering::Relaxed);
        self.cond.notify_one();
    }

    /// Enqueue a `Remove` op. Always supersedes any pending `Store` for the
    /// same key.
    pub fn enqueue_remove(&self, key: u64) {
        let mut guard = self.inner.lock().expect("writer mutex poisoned");
        guard.state.operations.insert(key, PendingCacheOp::Remove);
        guard.state.generation += 1;
        self.remove_total.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .result_cache_persist_remove_total
            .fetch_add(1, Ordering::Relaxed);
        self.cond.notify_one();
    }

    /// Advance the global clear generation; every queued op older than the
    /// clear is invalidated.
    pub fn enqueue_clear(&self) {
        let mut guard = self.inner.lock().expect("writer mutex poisoned");
        guard.state.clear_generation = guard.state.generation + 1;
        guard.state.operations.clear();
        guard.state.approx_bytes = 0;
        self.cond.notify_one();
    }

    /// Pop the next pending op, waiting on the condvar until one is queued
    /// or shutdown is requested. Returns `None` only after shutdown AND the
    /// queue is empty — the worker drains remaining ops before exit.
    pub fn drain_one(&self) -> Option<(u64, PendingCacheOp, u64)> {
        let mut guard = self.inner.lock().expect("writer mutex poisoned");
        loop {
            // Pull the first queued op, if any.
            let first_key = guard.state.operations.keys().next().copied();
            if let Some(key) = first_key {
                let op = guard.state.operations.remove(&key).expect("just observed");
                let gen = guard.state.generation;
                return Some((key, op, gen));
            }
            if guard.shutdown {
                return None;
            }
            guard = self.cond.wait(guard).expect("condvar wait poisoned");
        }
    }

    /// Request shutdown. The worker drains remaining ops until the queue is
    /// empty or the optional deadline expires.
    pub fn shutdown(&self) {
        let mut guard = self.inner.lock().expect("writer mutex poisoned");
        guard.shutdown = true;
        self.cond.notify_all();
    }

    pub fn enqueued_total(&self) -> u64 {
        self.enqueued_total.load(Ordering::Relaxed)
    }
    pub fn coalesced_total(&self) -> u64 {
        self.coalesced_total.load(Ordering::Relaxed)
    }
    pub fn dropped_total(&self) -> u64 {
        self.dropped_total.load(Ordering::Relaxed)
    }
    pub fn remove_total(&self) -> u64 {
        self.remove_total.load(Ordering::Relaxed)
    }
    pub fn queue_depth(&self) -> usize {
        self.inner
            .lock()
            .expect("writer mutex poisoned")
            .state
            .operations
            .len()
    }

    /// Point-in-time copy of the persistent-cache publish counters. Exposed
    /// so external integration tests can assert on the writer's bookkeeping
    /// without going through `Table::lookup_metrics_snapshot` (the writer is
    /// not yet wired into the table's persistent tier).
    pub fn persist_snapshot(&self) -> LookupMetricsSnapshot {
        LookupMetricsSnapshot {
            result_cache_persist_enqueued_total: self.enqueued_total.load(Ordering::Relaxed),
            result_cache_persist_coalesced_total: self.coalesced_total.load(Ordering::Relaxed),
            result_cache_persist_dropped_store_total: self.dropped_total.load(Ordering::Relaxed),
            result_cache_persist_remove_total: self.remove_total.load(Ordering::Relaxed),
            result_cache_persist_queue_depth: self.queue_depth() as u64,
            ..LookupMetricsSnapshot::default()
        }
    }

    /// Construct a writer with a fresh [`LookupMetrics`] (the canonical
    /// metrics type is `pub(crate)` and not constructible from external
    /// integration tests). Used by the PR C async-persistence test suite.
    pub fn for_test(limits: WriterLimits) -> Self {
        Self::new(LookupMetrics::default(), limits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::LookupMetrics;

    fn entry(key: u64, bytes: usize) -> PersistableEntry {
        PersistableEntry {
            key,
            table_id: 0,
            schema_id: 0,
            run_generation: 0,
            entry_generation: 0,
            footprint: Arc::new(roaring::RoaringBitmap::new()),
            rows: Arc::from(Vec::new().into_boxed_slice()),
            columns: Arc::from(Vec::<u16>::new().into_boxed_slice()),
            bytes,
        }
    }

    #[test]
    fn coalescing_replaces_earlier_store() {
        let m = LookupMetrics::default();
        let w = PersistentResultCacheWriter::new(m, WriterLimits::default());
        w.enqueue_store(entry(1, 100));
        w.enqueue_store(entry(1, 200));
        assert_eq!(w.coalesced_total(), 1);
        assert_eq!(w.queue_depth(), 1);
    }

    #[test]
    fn remove_supersedes_pending_store() {
        let m = LookupMetrics::default();
        let w = PersistentResultCacheWriter::new(m, WriterLimits::default());
        w.enqueue_store(entry(1, 100));
        w.enqueue_remove(1);
        // The Remove op is visible to the worker; subsequent Store is dropped.
        w.enqueue_store(entry(1, 100));
        // Drain the Remove first.
        let (_, op, _) = w.drain_one().expect("op");
        assert!(matches!(op, PendingCacheOp::Remove));
        // The dropped Store is counted but not queued.
        assert_eq!(w.queue_depth(), 0);
    }

    #[test]
    fn clear_invalidates_every_queued_op() {
        let m = LookupMetrics::default();
        let w = PersistentResultCacheWriter::new(m, WriterLimits::default());
        w.enqueue_store(entry(1, 100));
        w.enqueue_store(entry(2, 100));
        w.enqueue_clear();
        assert_eq!(w.queue_depth(), 0);
    }

    #[test]
    fn capacity_overflow_drops_store() {
        let m = LookupMetrics::default();
        let w = PersistentResultCacheWriter::new(
            m,
            WriterLimits {
                max_pending_keys: 1,
                max_pending_bytes: 1024,
            },
        );
        w.enqueue_store(entry(1, 100));
        w.enqueue_store(entry(2, 100));
        assert_eq!(w.dropped_total(), 1);
        assert_eq!(w.queue_depth(), 1);
    }

    #[test]
    fn shutdown_drains_pending_ops() {
        let m = LookupMetrics::default();
        let w = PersistentResultCacheWriter::new(m, WriterLimits::default());
        w.enqueue_store(entry(1, 100));
        w.enqueue_store(entry(2, 100));
        w.shutdown();
        // After shutdown, drain_one returns None even if ops were queued.
        assert!(w.drain_one().is_some());
        assert!(w.drain_one().is_some());
        assert!(w.drain_one().is_none());
    }
}
