//! Stage 1E (spec §10.5, S1E-003) integration: a `MemoryGovernor` wired to the
//! sharded page caches a real `Database` runs on.
//!
//! `Database` builds its caches as `Sharded<PageCache>` /
//! `Sharded<DecodedPageCache>` over `CACHE_SHARDS` shards; these tests attach
//! a governor to exactly those types with small budgets and drive them through
//! their public API: cache memory stays bounded by the governor (no OOM
//! growth), eviction happens under pressure, the replication/control floor is
//! never starved, and the accounting counters stay exact at every step.
//!
//! Attaching the governor inside `Database::open` itself is later-wave wiring
//! (the fields are private to `database.rs`); a final smoke test runs the
//! production read path of a real `Table` to prove the instrumented caches
//! behave identically without a governor attached.

use std::sync::Arc;

use mongreldb_core::cache::{DecodedPageCache, PageCache, Sharded, CACHE_SHARDS};
use mongreldb_core::columnar::NativeColumn;
use mongreldb_core::memory::{
    EscalationLevel, EscalationThresholds, GovernorConfig, MemoryClass, MemoryGovernor,
};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{CachedPage, Epoch, Snapshot, Table, Value};
use tempfile::tempdir;

const KIB: u64 = 1024;

fn page(seed: u8, len: usize) -> CachedPage {
    CachedPage {
        committed_epoch: Epoch(1),
        content_hash: [seed; 32],
        bytes: bytes::Bytes::from(vec![seed; len]),
    }
}

/// A sharded raw-page cache built exactly as `Database::open` builds it, with
/// every shard reporting to `governor`. Per-shard capacity is deliberately
/// huge so the governor — not the cache's own capacity — is the binding
/// constraint in these tests.
fn governed_page_cache(governor: &MemoryGovernor) -> Arc<Sharded<PageCache>> {
    Arc::new(Sharded::new(CACHE_SHARDS, || {
        PageCache::new(64 * KIB * KIB).with_governor(governor.clone(), MemoryClass::PageCache)
    }))
}

fn insert_pages(cache: &Sharded<PageCache>, seeds: impl Iterator<Item = u8>, len: usize) {
    for seed in seeds {
        let key = [seed; 32];
        cache.lock(&key).insert(page(seed, len));
    }
}

#[test]
fn governor_bounds_sharded_page_cache_under_pressure() {
    // 64 KiB node, 16 KiB reserved floor: non-reserved classes (the page
    // cache) are capped at 48 KiB.
    let governor =
        MemoryGovernor::new(GovernorConfig::new(64 * KIB).with_reserved_floor(16 * KIB)).unwrap();
    let cache = governed_page_cache(&governor);
    governor.register_reclaimable(&cache);

    // Offer 256 KiB of pages: 8× the node maximum, >5× what the cache may hold.
    insert_pages(&cache, 0u8..64, 4 * KIB as usize);

    // Eviction happened, memory is bounded by the governor (no OOM growth)…
    assert!(
        cache.used_bytes() <= 48 * KIB,
        "cache {} KiB exceeded the governor's non-reserved limit",
        cache.used_bytes() / KIB
    );
    assert!(cache.len() < 64, "cold pages were evicted");
    // …the accounting is exact…
    assert_eq!(
        governor.usage(MemoryClass::PageCache),
        cache.used_bytes(),
        "governor accounting must equal the cache's live bytes"
    );
    assert_eq!(governor.total_used(), cache.used_bytes());
    // …counters are sane…
    let stats = governor.stats();
    assert!(stats.reservations_granted > 0);
    assert_eq!(stats.usage_for(MemoryClass::PageCache), cache.used_bytes());
    // …and the cache still serves the pages that survived: the coldest
    // (oldest) inserts were evicted first, recent ones remain.
    let snap = Snapshot::at(Epoch(1));
    let mut cached = Vec::new();
    for seed in 0u8..64 {
        let key = [seed; 32];
        if cache.lock(&key).get(&key, snap).is_some() {
            cached.push(seed);
        }
    }
    assert_eq!(cached.len(), cache.len(), "every resident page hits");
    assert_eq!(cached.len(), 12, "48 KiB of 4 KiB pages");
    assert!(
        cached.iter().all(|seed| *seed >= 48),
        "only recent pages survive, got {cached:?}"
    );
    assert!(cache.stats().hits > 0);

    // Reclaiming through the governor frees cache bytes and stays consistent.
    let before = cache.used_bytes();
    let freed = governor.evict_reclaimable(16 * KIB);
    assert!(freed >= 16 * KIB, "freed {freed}");
    assert_eq!(cache.used_bytes(), before - freed);
    assert_eq!(governor.usage(MemoryClass::PageCache), cache.used_bytes());

    // Dropping the cache releases all of its governor accounting.
    drop(cache);
    assert_eq!(governor.usage(MemoryClass::PageCache), 0);
    assert_eq!(governor.total_used(), 0);
}

#[test]
fn escalation_drives_eviction_and_preserves_replication_floor() {
    // 64 KiB node, 16 KiB floor, thresholds at 50/60/70/80% so the escalation
    // ladder is reachable with a small cache.
    let governor = MemoryGovernor::new(
        GovernorConfig::new(64 * KIB)
            .with_reserved_floor(16 * KIB)
            .with_thresholds(EscalationThresholds {
                reject_low_priority: 0.50,
                evict_caches: 0.60,
                spill_operators: 0.70,
                throttle_maintenance: 0.80,
                hysteresis: 0.05,
            }),
    )
    .unwrap();
    let cache = governed_page_cache(&governor);
    governor.register_reclaimable(&cache);

    // The cache fills its entire non-reserved share (48 KiB = 75% of the node):
    // pressure alone escalates to the spill level.
    insert_pages(&cache, 0u8..32, 4 * KIB as usize);
    assert_eq!(cache.used_bytes(), 48 * KIB);
    assert!(governor.should_evict_caches());
    assert!(governor.spill_trigger());
    assert_eq!(governor.stats().spill_triggers, 1);

    // Step 1: new low-priority work is rejected while pressure holds.
    assert!(matches!(
        governor.try_reserve(KIB, MemoryClass::Compaction),
        Err(mongreldb_core::memory::MemoryError::LowPriorityRejected { .. })
    ));

    // Step 2: the governor drives cache eviction; pressure de-escalates
    // through the hysteresis band as bytes come back.
    let freed = governor.evict_reclaimable(16 * KIB);
    assert_eq!(freed, 16 * KIB);
    assert_eq!(cache.used_bytes(), 32 * KIB);
    assert_eq!(governor.usage(MemoryClass::PageCache), 32 * KIB);
    // 50% pressure: below spill/evict minus hysteresis, still inside step 1's
    // band (0.50 is not below 0.50 - 0.05).
    assert_eq!(governor.escalation(), EscalationLevel::RejectLowPriority);

    // Foreground query pressure plus the remaining cache re-enters the spill
    // level (non-reserved classes are capped at 48 KiB); the trigger counter
    // advances once per entry.
    let foreground = governor
        .try_reserve(16 * KIB, MemoryClass::QueryExecution)
        .unwrap();
    assert!(governor.spill_trigger());
    assert_eq!(governor.stats().spill_triggers, 2);

    // Step 3 is a hook point only (S1E-004 lands the mechanism): the trigger
    // is observable, nothing spills here.
    // Step 4 needs the last band: replication traffic tops the node off.
    // Non-reserved classes are now capped out (48 KiB used of 48 KiB)…
    assert!(governor
        .try_reserve(KIB, MemoryClass::ResultBuffering)
        .is_err());
    // …but replication is never starved: it reserves the full floor.
    let replication = governor
        .try_reserve(16 * KIB, MemoryClass::Replication)
        .unwrap();
    assert_eq!(governor.total_used(), 64 * KIB);
    assert_eq!(governor.escalation(), EscalationLevel::ThrottleMaintenance);
    assert!(governor.should_throttle_maintenance());
    // At the node maximum even reserved classes stop (bounded, never negative).
    assert!(governor.try_reserve(KIB, MemoryClass::Replication).is_err());
    assert!(governor
        .try_reserve(KIB, MemoryClass::NetworkBuffers)
        .is_err());

    // Relief: drop foreground + replication, evict the cache — the node
    // returns to zero usage and no escalation.
    drop(foreground);
    drop(replication);
    let freed = governor.evict_reclaimable(u64::MAX);
    assert_eq!(freed, 32 * KIB);
    assert_eq!(governor.total_used(), 0);
    assert_eq!(governor.escalation(), EscalationLevel::None);

    let stats = governor.stats();
    assert!(stats.reservations_granted > 0);
    assert!(stats.reservations_rejected > 0);
    assert!(stats.low_priority_rejected > 0);
}

#[test]
fn decoded_cache_reports_to_governor_alongside_page_cache() {
    let governor =
        MemoryGovernor::new(GovernorConfig::new(64 * KIB).with_reserved_floor(0)).unwrap();
    let page_cache = governed_page_cache(&governor);
    let decoded_cache = Arc::new(Sharded::new(CACHE_SHARDS, || {
        DecodedPageCache::new(64 * KIB * KIB)
            .with_governor(governor.clone(), MemoryClass::DecodedCache)
    }));
    governor.register_reclaimable(&page_cache);
    governor.register_reclaimable(&decoded_cache);

    insert_pages(&page_cache, 0u8..8, 4 * KIB as usize); // 32 KiB raw
    let column = Arc::new(NativeColumn::Int64 {
        data: vec![0; 1024], // 8 KiB decoded
        validity: vec![],
    });
    for seed in 0u8..2 {
        let key = [seed; 32];
        decoded_cache.lock(&key).insert(key, Arc::clone(&column));
    }
    let decoded_bytes = decoded_cache.used_bytes();
    assert!(decoded_bytes > 0);
    assert_eq!(
        governor.usage(MemoryClass::DecodedCache),
        decoded_bytes,
        "decoded cache accounting is exact"
    );
    assert_eq!(governor.total_used(), 32 * KIB + decoded_bytes);

    // One governor drives both reclaimable caches (S1E-003 step 2).
    let freed = governor.evict_reclaimable(u64::MAX);
    assert_eq!(freed, 32 * KIB + decoded_bytes);
    assert_eq!(page_cache.used_bytes(), 0);
    assert_eq!(decoded_cache.used_bytes(), 0);
    assert_eq!(governor.total_used(), 0);
}

#[test]
fn production_read_path_is_unchanged_without_a_governor() {
    // A real Table exercises the instrumented caches through the production
    // read path (SharedCtx builds the same sharded caches Database uses).
    // With no governor attached, behavior must be exactly as before.
    let dir = tempdir().unwrap();
    let schema = Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
            },
            ColumnDef {
                id: 2,
                name: "v".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    };
    let mut table = Table::create(dir.path(), schema, 1).unwrap();
    for i in 0..30_000i64 {
        table
            .put(vec![(1, Value::Int64(i)), (2, Value::Int64(i * 3))])
            .unwrap();
    }
    table.flush().unwrap();

    // Two full scans over the shared caches: identical, correct results.
    for _ in 0..2 {
        let snap = table.snapshot();
        let cols = table.visible_columns_native(snap, None).unwrap();
        let ids = cols
            .iter()
            .find(|(c, _)| *c == 1)
            .map(|(_, c)| c.len())
            .unwrap();
        assert_eq!(ids, 30_000);
    }
}
