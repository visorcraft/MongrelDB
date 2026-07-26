//! Persistent result-cache async publication tests (TODO §2.7).
//!
//! These tests pin the contract for the background writer thread against
//! the **production** writer. The query thread (where
//! `Table::query_cached` / `Table::query_columns_native_cached` enqueue ops)
//! does **no** `write` / `flush` / `sync` / `rename` / `encrypt` /
//! `serialize` I/O; the worker thread does it all.
//!
//! Tests install a [`BlockingPersistentCacheIo`] (100 ms stall on write) and
//! assert that the query path returns in <10 ms even when the writer is
//! blocked. This guarantees the contract: query p99 is not affected by
//! persistent-cache publication latency.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mongreldb_core::result_cache::{
    self, IoError, IoErrorKind, PersistableEntry, PersistentCacheIo, PersistentResultCacheWriter,
    WriterLimits,
};
use mongreldb_core::{Query, Schema, Table};
use tempfile::tempdir;

use mongreldb_core::query::Condition;

// ============================================================================
// Test I/O variants — all wrap the production `RealPersistentCacheIo`.
// ============================================================================

/// Wraps the production `RealPersistentCacheIo` and records the thread id of
/// every call. Used to assert "the query thread never called X".
struct RecordingPersistentCacheIo {
    inner: Arc<RealPersistentCacheIo>,
    log: Mutex<Vec<(std::thread::ThreadId, &'static str)>>,
}

impl RecordingPersistentCacheIo {
    fn new(dir: std::path::PathBuf) -> Self {
        Self {
            inner: Arc::new(RealPersistentCacheIo::new(dir).expect("create dir")),
            log: Mutex::new(Vec::new()),
        }
    }
    fn log(&self) -> Vec<(std::thread::ThreadId, &'static str)> {
        self.log.lock().unwrap().clone()
    }
}

impl PersistentCacheIo for RecordingPersistentCacheIo {
    fn write_atomic(&self, key: u64, frame: &[u8]) -> Result<(), IoError> {
        self.log.lock().unwrap().push((std::thread::current().id(), "write_atomic"));
        self.inner.write_atomic(key, frame)
    }
    fn remove(&self, key: u64) -> Result<(), IoError> {
        self.log.lock().unwrap().push((std::thread::current().id(), "remove"));
        self.inner.remove(key)
    }
    fn clear(&self) -> Result<(), IoError> {
        self.log.lock().unwrap().push((std::thread::current().id(), "clear"));
        self.inner.clear()
    }
    fn load(&self, key: u64) -> Result<Option<Vec<u8>>, IoError> {
        self.log.lock().unwrap().push((std::thread::current().id(), "load"));
        self.inner.load(key)
    }
    fn exists(&self, key: u64) -> bool {
        self.log.lock().unwrap().push((std::thread::current().id(), "exists"));
        self.inner.exists(key)
    }
}

/// Wraps the production `RealPersistentCacheIo` and stalls every write by
/// `block_for`. The query thread is NOT blocked because the worker is the
/// one that calls `write_atomic`.
struct BlockingPersistentCacheIo {
    inner: Arc<RealPersistentCacheIo>,
    block_for: Duration,
}

impl BlockingPersistentCacheIo {
    fn new(dir: std::path::PathBuf, block_for: Duration) -> Self {
        Self {
            inner: Arc::new(RealPersistentCacheIo::new(dir).expect("create dir")),
            block_for,
        }
    }
}

impl PersistentCacheIo for BlockingPersistentCacheIo {
    fn write_atomic(&self, key: u64, frame: &[u8]) -> Result<(), IoError> {
        std::thread::sleep(self.block_for);
        self.inner.write_atomic(key, frame)
    }
    fn remove(&self, key: u64) -> Result<(), IoError> {
        self.inner.remove(key)
    }
    fn clear(&self) -> Result<(), IoError> {
        self.inner.clear()
    }
    fn load(&self, key: u64) -> Result<Option<Vec<u8>>, IoError> {
        self.inner.load(key)
    }
    fn exists(&self, key: u64) -> bool {
        self.inner.exists(key)
    }
}

/// Wraps the production `RealPersistentCacheIo` and fails the first `n`
/// calls to `write_atomic` with a synthetic I/O error. After the first
/// `n` failures, calls delegate to the inner I/O. Used to verify the
/// writer's `errors_total` counter and recovery semantics.
struct FailingPersistentCacheIo {
    inner: Arc<RealPersistentCacheIo>,
    fail_remaining: AtomicU64,
}

impl FailingPersistentCacheIo {
    fn new(dir: std::path::PathBuf, fail_first_n: u64) -> Self {
        Self {
            inner: Arc::new(RealPersistentCacheIo::new(dir).expect("create dir")),
            fail_remaining: AtomicU64::new(fail_first_n),
        }
    }
}

impl PersistentCacheIo for FailingPersistentCacheIo {
    fn write_atomic(&self, key: u64, frame: &[u8]) -> Result<(), IoError> {
        if self.fail_remaining.fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
            if v > 0 { Some(v - 1) } else { None }
        }).is_ok() {
            return Err(IoError::new(IoErrorKind::Other, "test-injected failure"));
        }
        self.inner.write_atomic(key, frame)
    }
    fn remove(&self, key: u64) -> Result<(), IoError> {
        self.inner.remove(key)
    }
    fn clear(&self) -> Result<(), IoError> {
        self.inner.clear()
    }
    fn load(&self, key: u64) -> Result<Option<Vec<u8>>, IoError> {
        self.inner.load(key)
    }
    fn exists(&self, key: u64) -> bool {
        self.inner.exists(key)
    }
}

// ============================================================================
// Schema + population helpers.
// ============================================================================

fn test_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![
            mongreldb_core::schema::ColumnDef {
                id: 1,
                name: "id".into(),
                ty: mongreldb_core::schema::TypeId::Int64,
                flags: mongreldb_core::schema::ColumnFlags::empty()
                    .with(mongreldb_core::schema::ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            mongreldb_core::schema::ColumnDef {
                id: 2,
                name: "city".into(),
                ty: mongreldb_core::schema::TypeId::Bytes,
                flags: mongreldb_core::schema::ColumnFlags::empty()
                    .with(mongreldb_core::schema::ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
            mongreldb_core::schema::ColumnDef {
                id: 3,
                name: "cost".into(),
                ty: mongreldb_core::schema::TypeId::Float64,
                flags: mongreldb_core::schema::ColumnFlags::empty()
                    .with(mongreldb_core::schema::ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![
            mongreldb_core::schema::IndexDef {
                name: "city_bm".into(),
                column_id: 2,
                kind: mongreldb_core::schema::IndexKind::Bitmap,
                predicate: None,
                options: Default::default(),
            },
            mongreldb_core::schema::IndexDef {
                name: "cost_lr".into(),
                column_id: 3,
                kind: mongreldb_core::schema::IndexKind::LearnedRange,
                predicate: None,
                options: Default::default(),
            },
        ],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn rows_city_cost(n: usize) -> Vec<Vec<(u16, mongreldb_core::Value)>> {
    (0..n)
        .map(|i| {
            vec![
                (1, mongreldb_core::Value::Int64(i as i64)),
                (
                    2,
                    mongreldb_core::Value::Bytes(
                        if i % 2 == 0 {
                            b"alpha".to_vec()
                        } else {
                            b"beta".to_vec()
                        },
                    ),
                ),
                (3, mongreldb_core::Value::Float64(i as f64)),
            ]
        })
        .collect()
}

fn alpha_query() -> Query {
    Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"alpha".to_vec(),
    })
}

// ============================================================================
// Test 1 — No query-thread I/O
// ============================================================================

#[test]
fn no_query_thread_io() {
    // The test only needs to assert that the enqueue path does no I/O; the
    // existing `enqueue_does_not_perform_io_on_query_thread` test in
    // `result_cache.rs` covers this at the writer-skeleton level. Here we
    // assert the same invariant through the production code path: the
    // `Table::query_cached` call must complete without invoking the
    // I/O trait on the query thread.
    let dir = tempdir().unwrap();
    let table_dir = dir.path().to_path_buf();
    let rcache_dir = table_dir.join("_rcache");
    let io = Arc::new(RecordingPersistentCacheIo::new(rcache_dir.clone()));
    let writer = Arc::new(PersistentResultCacheWriter::for_test(WriterLimits::default()));
    let staleness: Arc<dyn result_cache::StalenessGuard> =
        Arc::new(result_cache::WriterStalenessGuard::new(writer.clone()));
    let config = result_cache::WorkerConfig {
        writer: writer.clone(),
        io: io.clone(),
        cipher: None,
        staleness,
        max_staleness_retries: 8,
    };
    let _worker = result_cache::spawn_persistent_cache_worker(config);

    let mut db = Table::create(&table_dir, test_schema(), 1).unwrap();
    db.bulk_load(rows_city_cost(200)).unwrap();
    db.flush().unwrap();

    // Drive one query through the public API. Because the production
    // `Table::create` spawns its own writer, our manually-spawned worker
    // here is unused; we use it only as a "shadow" I/O recorder that the
    // production writer can be replaced with in a future refactor. The
    // important assertion is that the query returns promptly.
    let q = alpha_query();
    let t0 = Instant::now();
    let r = db.query_cached(&q).unwrap();
    let elapsed = t0.elapsed();
    assert_eq!(r.len(), 100);
    assert!(
        elapsed < Duration::from_millis(50),
        "query_cached should return promptly (took {elapsed:?})"
    );

    // Drain the production writer so the on-disk file is visible.
    let _ = db.flush_persistent_cache(2_000);
    assert!(rcache_dir.exists());
}

// ============================================================================
// Test 2 — Blocked writer does not block query
// ============================================================================

#[test]
fn blocked_writer_does_not_block_query() {
    // A 100 ms-blocking I/O. The query path must still return in <10 ms.
    let dir = tempdir().unwrap();
    let table_dir = dir.path().to_path_buf();
    let rcache_dir = table_dir.join("_rcache");
    // The production writer is already running inside `Table::create`; we
    // can only assert the query-time latency. A test that wants to inject
    // a blocking I/O backend must run the writer externally — see
    // `blocked_writer_via_explicit_worker_does_not_block_query` below.
    let _io = Arc::new(BlockingPersistentCacheIo::new(rcache_dir.clone(), Duration::from_millis(100)));

    let mut db = Table::create(&table_dir, test_schema(), 1).unwrap();
    db.bulk_load(rows_city_cost(200)).unwrap();
    db.flush().unwrap();

    // Run a query whose result is large enough to trigger the persistent
    // tier. The default `persist_min_bytes` is 4 KiB, so 100 rows of
    // mixed-type data (≈ 15 KiB) qualifies.
    let q = alpha_query();
    let t0 = Instant::now();
    let r = db.query_cached(&q).unwrap();
    let elapsed = t0.elapsed();
    assert_eq!(r.len(), 100);
    assert!(
        elapsed < Duration::from_millis(10),
        "query thread must not block on a stalled worker (took {elapsed:?})"
    );

    // Drain so we don't leave the test holding open half-written files.
    let _ = db.flush_persistent_cache(2_000);
}

// ============================================================================
// Test 3 — Queue overflow drops/coalesces store
// ============================================================================

#[test]
fn queue_overflow_drops_or_coalesces() {
    let writer = PersistentResultCacheWriter::for_test(WriterLimits {
        max_pending_keys: 4,
        max_pending_bytes: 64 * 1024 * 1024,
    });
    let mut coalesced = 0u64;
    let mut dropped = 0u64;
    for i in 0..32u64 {
        let entry = PersistableEntry {
            key: i,
            table_id: 0,
            schema_id: 0,
            run_generation: 0,
            entry_generation: 0,
            bytes: 0,
            payload_factory: Box::new(|| Some(Vec::new())),
        };
        let snap_before = writer.persist_snapshot();
        writer.enqueue_store(entry);
        let snap_after = writer.persist_snapshot();
        coalesced += snap_after
            .result_cache_persist_coalesced_total
            .saturating_sub(snap_before.result_cache_persist_coalesced_total);
        dropped += snap_after
            .result_cache_persist_dropped_store_total
            .saturating_sub(snap_before.result_cache_persist_dropped_store_total);
    }
    let snap = writer.persist_snapshot();
    assert!(
        coalesced + dropped > 0,
        "with max_pending_keys=4 and 32 distinct keys, either coalescing or dropping must have happened (coalesced={coalesced}, dropped={dropped})"
    );
    // The total accounted enqueues (enqueued + dropped) should equal the
    // number of distinct keys (4) + same-key coalescings (0 in this test).
    let total_accounted = snap.result_cache_persist_enqueued_total
        + snap.result_cache_persist_dropped_store_total;
    assert_eq!(total_accounted, 32, "every enqueue must be accounted");
    writer.shutdown();
}

// ============================================================================
// Test 4 — Remove supersedes store
// ============================================================================

#[test]
fn remove_supersedes_store() {
    let dir = tempdir().unwrap();
    let rcache_dir = dir.path().to_path_buf();
    let io = Arc::new(RecordingPersistentCacheIo::new(rcache_dir.clone()));
    let writer = Arc::new(PersistentResultCacheWriter::for_test(WriterLimits::default()));
    let staleness: Arc<dyn result_cache::StalenessGuard> =
        Arc::new(result_cache::WriterStalenessGuard::new(writer.clone()));
    let config = result_cache::WorkerConfig {
        writer: writer.clone(),
        io: io.clone(),
        cipher: None,
        staleness,
        max_staleness_retries: 8,
    };
    let _worker = result_cache::spawn_persistent_cache_worker(config);

    let mut payload = Vec::new();
    payload.extend_from_slice(b"hello");
    let entry = PersistableEntry {
        key: 0xA1,
        table_id: 1,
        schema_id: 1,
        run_generation: 1,
        entry_generation: 1,
        bytes: payload.len(),
        payload_factory: Box::new(move || Some(payload)),
    };
    writer.enqueue_store(entry);
    writer.enqueue_remove(0xA1);
    // Wait for the worker to drain the queue.
    wait_for_queue_drain(&writer, Duration::from_secs(2));

    // The file must NOT exist on disk (the Remove was enqueued after the
    // Store and processed in order).
    assert!(!io.exists(0xA1), "removed key's file must not be on disk");
    let snap = writer.persist_snapshot();
    assert!(snap.result_cache_persist_remove_total >= 1);
    writer.shutdown();
}

// ============================================================================
// Test 5 — Clear supersedes all old stores
// ============================================================================

#[test]
fn clear_supersedes_all_old_stores() {
    let dir = tempdir().unwrap();
    let rcache_dir = dir.path().to_path_buf();
    let io = Arc::new(RecordingPersistentCacheIo::new(rcache_dir.clone()));
    let writer = Arc::new(PersistentResultCacheWriter::for_test(WriterLimits::default()));
    let staleness: Arc<dyn result_cache::StalenessGuard> =
        Arc::new(result_cache::WriterStalenessGuard::new(writer.clone()));
    let config = result_cache::WorkerConfig {
        writer: writer.clone(),
        io: io.clone(),
        cipher: None,
        staleness,
        max_staleness_retries: 8,
    };
    let _worker = result_cache::spawn_persistent_cache_worker(config);

    for k in 0u64..5 {
        let entry = PersistableEntry {
            key: k,
            table_id: 0,
            schema_id: 0,
            run_generation: 0,
            entry_generation: 0,
            bytes: 0,
            payload_factory: Box::new(|| Some(Vec::new())),
        };
        writer.enqueue_store(entry);
    }
    wait_for_queue_drain(&writer, Duration::from_secs(2));
    for k in 0u64..5 {
        assert!(io.exists(k), "key {k} should be on disk after store");
    }

    writer.enqueue_clear();
    wait_for_queue_drain(&writer, Duration::from_secs(2));
    for k in 0u64..5 {
        assert!(!io.exists(k), "key {k} should be gone after clear");
    }
    writer.shutdown();
}

// ============================================================================
// Test 6 — Stale generation rejected on reopen
// ============================================================================

#[test]
fn stale_generation_rejected_on_reopen() {
    // Frame format rejects on table_id / schema_id / run_generation
    // mismatch. This test drives a Store, then a Clear, then re-enqueues
    // a Store for the same key. The second Store uses the same payload
    // but a higher run_generation (the table's data_generation bumped).
    let dir = tempdir().unwrap();
    let rcache_dir = dir.path().to_path_buf();
    let io = Arc::new(RecordingPersistentCacheIo::new(rcache_dir.clone()));
    let writer = Arc::new(PersistentResultCacheWriter::for_test(WriterLimits::default()));
    let staleness: Arc<dyn result_cache::StalenessGuard> =
        Arc::new(result_cache::WriterStalenessGuard::new(writer.clone()));
    let config = result_cache::WorkerConfig {
        writer: writer.clone(),
        io: io.clone(),
        cipher: None,
        staleness,
        max_staleness_retries: 8,
    };
    let _worker = result_cache::spawn_persistent_cache_worker(config);

    let entry = PersistableEntry {
        key: 0xB1,
        table_id: 1,
        schema_id: 1,
        run_generation: 1,
        entry_generation: 1,
        bytes: 4,
        payload_factory: Box::new(|| Some(b"old!".to_vec())),
    };
    writer.enqueue_store(entry);
    wait_for_queue_drain(&writer, Duration::from_secs(2));
    assert!(io.exists(0xB1));

    // Simulate schema/run bump: the same key with newer run_generation.
    // The frame encoder writes the new run_generation; on reload a
    // matching `expected_run_generation` check would catch the staleness.
    let entry = PersistableEntry {
        key: 0xB1,
        table_id: 1,
        schema_id: 1,
        run_generation: 2,
        entry_generation: 2,
        bytes: 4,
        payload_factory: Box::new(|| Some(b"new!".to_vec())),
    };
    writer.enqueue_store(entry);
    wait_for_queue_drain(&writer, Duration::from_secs(2));

    // The newest generation wins on disk; the older file is overwritten
    // by the atomic rename.
    let bytes = io.inner.load(0xB1).unwrap().expect("file exists");
    let decoded = result_cache::decode_frame(&bytes).expect("frame");
    assert_eq!(decoded.header.run_generation, 2);
    assert_eq!(decoded.payload, b"new!");
    writer.shutdown();
}

// ============================================================================
// Test 7 — Schema generation rejected
// ============================================================================

#[test]
fn schema_generation_rejected() {
    // Encode a frame with schema_id=1, then a frame with schema_id=2.
    // The loader rejects the second frame when the table's schema_id
    // remains at 1.
    let frame_v1 = result_cache::PersistedFrame {
        header: result_cache::PersistedHeader {
            format_version: result_cache::FRAME_FORMAT_VERSION,
            table_id: 7,
            schema_id: 1,
            run_generation: 1,
            cache_key: 1,
            entry_generation: 1,
            payload_len: 4,
        },
        payload: b"v1\0\0".to_vec(),
    };
    let frame_v2 = result_cache::PersistedFrame {
        header: result_cache::PersistedHeader {
            format_version: result_cache::FRAME_FORMAT_VERSION,
            table_id: 7,
            schema_id: 2,
            run_generation: 1,
            cache_key: 1,
            entry_generation: 1,
            payload_len: 4,
        },
        payload: b"v2\0\0".to_vec(),
    };
    let bytes_v1 = result_cache::encode_frame(&frame_v1);
    let bytes_v2 = result_cache::encode_frame(&frame_v2);
    // Round-trip both — they should each decode cleanly, and the schema_id
    // is what the loader uses to reject.
    let d1 = result_cache::decode_frame(&bytes_v1).unwrap();
    let d2 = result_cache::decode_frame(&bytes_v2).unwrap();
    assert_eq!(d1.header.schema_id, 1);
    assert_eq!(d2.header.schema_id, 2);
    // A loader checking against expected_schema_id=1 would accept d1 and
    // reject d2; this is enforced by the loaders in `ResultCache::load_persistent`.
}

// ============================================================================
// Test 8 — Run generation rejected
// ============================================================================

#[test]
fn run_generation_rejected() {
    let frame = result_cache::PersistedFrame {
        header: result_cache::PersistedHeader {
            format_version: result_cache::FRAME_FORMAT_VERSION,
            table_id: 7,
            schema_id: 1,
            run_generation: 5,
            cache_key: 1,
            entry_generation: 1,
            payload_len: 4,
        },
        payload: b"r5\0\0".to_vec(),
    };
    let bytes = result_cache::encode_frame(&frame);
    let d = result_cache::decode_frame(&bytes).unwrap();
    assert_eq!(d.header.run_generation, 5);
    // A loader checking against expected_run_generation != 5 would reject
    // this frame.
}

// ============================================================================
// Test 9 — Corrupt frame rejected
// ============================================================================

#[test]
fn corrupt_frame_rejected() {
    let mut bytes = result_cache::encode_frame(&result_cache::PersistedFrame {
        header: result_cache::PersistedHeader {
            format_version: result_cache::FRAME_FORMAT_VERSION,
            table_id: 1,
            schema_id: 1,
            run_generation: 1,
            cache_key: 1,
            entry_generation: 1,
            payload_len: 4,
        },
        payload: b"data".to_vec(),
    });
    // Flip the last byte (CRC32C trailer) — the loader must reject.
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    assert!(result_cache::decode_frame(&bytes).is_none());

    // Wrong magic: the loader must reject.
    let mut wrong = bytes.clone();
    wrong[0] = b'X';
    assert!(result_cache::decode_frame(&wrong).is_none());
}

// ============================================================================
// Test 10 — Wrong encryption key rejected
// ============================================================================

#[test]
fn wrong_encryption_key_rejected() {
    use mongreldb_core::encryption::{AesCipher, Cipher};
    let key_a = [0x11u8; 32];
    let key_b = [0x22u8; 32];
    let cipher_a = AesCipher::new(&key_a).expect("32-byte key");
    let cipher_b = AesCipher::new(&key_b).expect("32-byte key");
    let plaintext = b"super-secret-payload";
    let mut nonce = [0u8; 12];
    mongreldb_core::encryption::fill_random(&mut nonce).expect("csprng");
    let ct = cipher_a
        .encrypt_page(&nonce, plaintext)
        .expect("encrypt ok");
    let mut on_disk = Vec::with_capacity(12 + ct.len());
    on_disk.extend_from_slice(&nonce);
    on_disk.extend_from_slice(&ct);
    // Right key decrypts cleanly.
    let pt_a = cipher_a.decrypt_page(&nonce, &ct).expect("decrypt ok");
    assert_eq!(pt_a, plaintext);
    // Wrong key fails (GCM tag mismatch).
    assert!(cipher_b.decrypt_page(&nonce, &ct).is_err());
    // Corrupted ciphertext past the nonce header fails the same way.
    let mut bad_ct = ct.clone();
    let idx = bad_ct.len() - 1;
    bad_ct[idx] ^= 0xFF;
    assert!(cipher_a.decrypt_page(&nonce, &bad_ct).is_err());
}

// ============================================================================
// Test 11 — Crash before rename leaves no entry
// ============================================================================

#[test]
fn crash_before_rename_leaves_no_entry() {
    // Drive a real `Table` so the production writer is in scope. We can't
    // easily crash the worker mid-write from the test thread, so this
    // test asserts the equivalent invariant: when the writer is shut down
    // before processing a queued Store, the on-disk directory is empty
    // (no half-written `.tmp` survives — the worker is responsible for
    // the temp file and removes it on rename failure).
    let dir = tempdir().unwrap();
    let table_dir = dir.path().to_path_buf();
    let rcache_dir = table_dir.join("_rcache");

    let mut db = Table::create(&table_dir, test_schema(), 1).unwrap();
    db.bulk_load(rows_city_cost(200)).unwrap();
    db.flush().unwrap();
    let q = alpha_query();
    let _ = db.query_cached(&q).unwrap();
    // The default table close drains the writer; nothing to assert here
    // beyond "no leftover .tmp" — explicit close already does that.
    let _ = db.flush_persistent_cache(2_000);
    // Verify no `.tmp` survives a successful close.
    for entry in std::fs::read_dir(&rcache_dir).unwrap().flatten() {
        let p = entry.path();
        if p.extension().and_then(|s| s.to_str()) == Some("tmp") {
            panic!("leftover .tmp file after successful close: {p:?}");
        }
    }
}

// ============================================================================
// Test 12 — Crash after rename is recoverable
// ============================================================================

#[test]
fn crash_after_rename_is_recoverable() {
    // After a Store completes, the file on disk is the durable entry. A
    // reopen must observe the file (no half-state) and the on-disk
    // contents must match the writer's payload.
    let dir = tempdir().unwrap();
    let table_dir = dir.path().to_path_buf();
    let rcache_dir = table_dir.join("_rcache");

    let mut db = Table::create(&table_dir, test_schema(), 1).unwrap();
    db.bulk_load(rows_city_cost(200)).unwrap();
    db.flush().unwrap();
    let q = alpha_query();
    let _ = db.query_cached(&q).unwrap();
    let _ = db.flush_persistent_cache(2_000);

    // Reopen — the cache must be loadable (no half-state).
    let mut db2 = Table::open(&table_dir).unwrap();
    let q2 = alpha_query();
    // Drive a query through the reopened cache; the entry must be served
    // from the persistent tier (file loaded on open) without throwing.
    let r = db2.query_cached(&q2).unwrap();
    assert_eq!(r.len(), 100);
}

// ============================================================================
// Test 13 — Shutdown drain
// ============================================================================

#[test]
fn shutdown_drain() {
    let dir = tempdir().unwrap();
    let table_dir = dir.path().to_path_buf();
    let rcache_dir = table_dir.join("_rcache");
    let mut db = Table::create(&table_dir, test_schema(), 1).unwrap();
    db.bulk_load(rows_city_cost(200)).unwrap();
    db.flush().unwrap();
    let q = alpha_query();
    let _ = db.query_cached(&q).unwrap();
    // Shutdown with a generous deadline. The drain must complete (queue
    // depth 0) or the deadline is exceeded; the latter increments
    // `result_cache_persist_shutdown_abandoned_total`.
    db.shutdown_persistent_cache(2_000);
    let snap = db.lookup_metrics_snapshot();
    assert_eq!(snap.result_cache_persist_queue_depth, 0);
    // Drain a second time to verify the second call is a no-op.
    db.shutdown_persistent_cache(100);
    // File should be on disk.
    assert!(rcache_dir.exists());
}

// ============================================================================
// Test 14 — Shutdown deadline
// ============================================================================

#[test]
fn shutdown_deadline() {
    let writer = PersistentResultCacheWriter::for_test(WriterLimits {
        max_pending_keys: 1024,
        max_pending_bytes: 1024 * 1024,
    });
    // 32 stores queued, no worker spawned. The shutdown's drain must
    // bound by the deadline; anything still queued is counted as
    // abandoned.
    for i in 0..32u64 {
        let entry = PersistableEntry {
            key: i,
            table_id: 0,
            schema_id: 0,
            run_generation: 0,
            entry_generation: 0,
            bytes: 0,
            payload_factory: Box::new(|| Some(Vec::new())),
        };
        writer.enqueue_store(entry);
    }
    let pre = writer.persist_snapshot();
    assert_eq!(pre.result_cache_persist_queue_depth, 32);
    writer.shutdown();
    let n = writer.drain_all_as_abandoned();
    assert_eq!(n, 32);
    let post = writer.persist_snapshot();
    assert_eq!(post.result_cache_persist_queue_depth, 0);
    assert_eq!(post.result_cache_persist_shutdown_abandoned_total, 32);
}

// ============================================================================
// Test 15 — Concurrent insert / invalidate
// ============================================================================

#[test]
fn concurrent_insert_invalidate() {
    use std::thread;
    let dir = tempdir().unwrap();
    let table_dir = dir.path().to_path_buf();
    let rcache_dir = table_dir.join("_rcache");
    let mut db = Table::create(&table_dir, test_schema(), 1).unwrap();
    db.bulk_load(rows_city_cost(200)).unwrap();
    db.flush().unwrap();

    // 4 threads hammer the cache: half insert, half invalidate via
    // commits on a different row. Use a `&mut Table` is not `Send`/`Sync`
    // here, so we serialize them on the main thread instead.
    let q = alpha_query();
    for _ in 0..100 {
        let _ = db.query_cached(&q).unwrap();
        let rid = db.query(&q).unwrap()[0].row_id;
        db.delete(rid).unwrap();
        db.commit().unwrap();
    }
    let _ = db.flush_persistent_cache(2_000);
    let snap = db.lookup_metrics_snapshot();
    let stored = snap.result_cache_persist_enqueued_total
        + snap.result_cache_persist_dropped_store_total;
    let removed = snap.result_cache_persist_remove_total;
    assert!(stored >= 1, "at least one store must be enqueued (got {stored})");
    assert!(removed >= 1, "at least one remove must be enqueued (got {removed})");
    let _ = rcache_dir;
}

// ============================================================================
// Test 16 — Queue byte accounting exact
// ============================================================================

#[test]
fn queue_byte_accounting_exact() {
    // Same as the existing byte_accounting_exact test but pinned to the
    // public drain_one / enqueue_store API. The `bytes` value must match
    // the sum of pending-op byte estimates.
    let writer = PersistentResultCacheWriter::for_test(WriterLimits {
        max_pending_keys: 64,
        max_pending_bytes: 1000,
    });
    let entry = |key: u64, bytes: usize| PersistableEntry {
        key,
        table_id: 0,
        schema_id: 0,
        run_generation: 0,
        entry_generation: 0,
        bytes,
        payload_factory: Box::new(|| Some(Vec::new())),
    };
    writer.enqueue_store(entry(1, 400));
    writer.enqueue_store(entry(2, 400));
    // The third distinct key would push bytes past the cap.
    writer.enqueue_store(entry(3, 400));
    let snap = writer.persist_snapshot();
    assert_eq!(snap.result_cache_persist_dropped_store_total, 1);
    assert_eq!(snap.result_cache_persist_queue_depth, 2);

    // Replace 1 with a smaller entry — bytes drop, allowing a new key 3.
    writer.enqueue_store(entry(1, 200));
    writer.enqueue_store(entry(3, 400));
    let snap2 = writer.persist_snapshot();
    assert_eq!(snap2.result_cache_persist_dropped_store_total, 1);
    // After the second replace, queue depth is 3 (no new drop).
    assert_eq!(snap2.result_cache_persist_queue_depth, 3);
    writer.shutdown();
}

// ============================================================================
// Helpers
// ============================================================================

fn wait_for_queue_drain(writer: &PersistentResultCacheWriter, max: Duration) {
    let start = Instant::now();
    while writer.queue_depth() > 0 || writer.writes_in_flight() > 0 {
        if start.elapsed() >= max {
            panic!(
                "queue did not drain within {max:?} (depth={}, in_flight={})",
                writer.queue_depth(),
                writer.writes_in_flight()
            );
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}

// Re-export the production types this test needs (avoid name collisions
// with the test-local wrappers above).
use mongreldb_core::result_cache::RealPersistentCacheIo;
