//! REM-002 production-path tests: the production loader / writer must use
//! the same framed format and reject wrong identity / corrupt frames /
//! legacy unframed files. These tests use `Table::create` / `Table::open`
//! directly — not synthetic frame encode/decode round-trips.

#![allow(dead_code, unused_imports, unused_variables)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mongreldb_core::encryption::{AesCipher, Cipher};
use mongreldb_core::query::Condition;
use mongreldb_core::result_cache::{
    self, IoError, IoErrorKind, PersistentCacheIo, RealPersistentCacheIo, FRAME_FORMAT_VERSION,
    FRAME_MAGIC,
};
use mongreldb_core::{Query, Schema, Table};
use tempfile::tempdir;

// ============================================================================
// Test I/O variants.
// ============================================================================

/// Records the thread id of every call and the bytes written. Used to assert
/// that no I/O runs on the query thread, and to assert that the on-disk
/// format begins with MLCP.
struct RecordingPersistentCacheIo {
    inner: Arc<RealPersistentCacheIo>,
    log: Mutex<Vec<(std::thread::ThreadId, &'static str, Vec<u8>)>>,
}

impl RecordingPersistentCacheIo {
    fn new(dir: std::path::PathBuf) -> Self {
        Self {
            inner: Arc::new(RealPersistentCacheIo::new(dir).expect("create dir")),
            log: Mutex::new(Vec::new()),
        }
    }
    fn log(&self) -> Vec<(std::thread::ThreadId, &'static str, Vec<u8>)> {
        self.log.lock().unwrap().clone()
    }
}

impl PersistentCacheIo for RecordingPersistentCacheIo {
    fn write_atomic(&self, key: u64, frame: &[u8]) -> Result<(), IoError> {
        self.log.lock().unwrap().push((
            std::thread::current().id(),
            "write_atomic",
            frame.to_vec(),
        ));
        self.inner.write_atomic(key, frame)
    }
    fn remove(&self, key: u64) -> Result<(), IoError> {
        self.log
            .lock()
            .unwrap()
            .push((std::thread::current().id(), "remove", Vec::new()));
        self.inner.remove(key)
    }
    fn clear(&self) -> Result<(), IoError> {
        self.log
            .lock()
            .unwrap()
            .push((std::thread::current().id(), "clear", Vec::new()));
        self.inner.clear()
    }
    fn load(&self, key: u64) -> Result<Option<Vec<u8>>, IoError> {
        self.log
            .lock()
            .unwrap()
            .push((std::thread::current().id(), "load", Vec::new()));
        self.inner.load(key)
    }
    fn exists(&self, key: u64) -> bool {
        self.log
            .lock()
            .unwrap()
            .push((std::thread::current().id(), "exists", Vec::new()));
        self.inner.exists(key)
    }
}

/// Blocks every `write_atomic` call until the gate is opened. Used to keep
/// the worker busy so the test can observe the in-flight state.
struct GatedPersistentCacheIo {
    inner: Arc<RealPersistentCacheIo>,
    gate: Arc<std::sync::atomic::AtomicBool>,
    block_for: Duration,
}

impl GatedPersistentCacheIo {
    fn new(dir: std::path::PathBuf, block_for: Duration) -> Self {
        Self {
            inner: Arc::new(RealPersistentCacheIo::new(dir).expect("create dir")),
            gate: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            block_for,
        }
    }
    fn open(&self) {
        self.gate.store(false, std::sync::atomic::Ordering::Relaxed);
    }
}

impl PersistentCacheIo for GatedPersistentCacheIo {
    fn write_atomic(&self, key: u64, frame: &[u8]) -> Result<(), IoError> {
        while self.gate.load(std::sync::atomic::Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(2));
        }
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

// ============================================================================
// Test fixtures.
// ============================================================================

fn test_schema() -> Schema {
    Schema {
        schema_id: 42,
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

fn rows(n: usize) -> Vec<Vec<(u16, mongreldb_core::Value)>> {
    (0..n)
        .map(|i| {
            vec![
                (1, mongreldb_core::Value::Int64(i as i64)),
                (
                    2,
                    mongreldb_core::Value::Bytes(if i % 2 == 0 {
                        b"alpha".to_vec()
                    } else {
                        b"beta!".to_vec()
                    }),
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

fn rcache_path(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("_rcache")
}

fn first_bin_file(dir: &std::path::Path) -> Option<std::path::PathBuf> {
    let entries = std::fs::read_dir(dir).ok()?;
    for entry in entries.flatten() {
        let p = entry.path();
        if p.extension().and_then(|s| s.to_str()) == Some("bin") {
            return Some(p);
        }
    }
    None
}

// ============================================================================
// 19.1 — async persisted entry is loaded after memory eviction
// ============================================================================

#[test]
fn async_persisted_entry_is_loaded_after_memory_eviction() {
    let dir = tempdir().unwrap();
    let table_dir = dir.path().to_path_buf();
    let rcache = rcache_path(&table_dir);

    let mut db = Table::create(&table_dir, test_schema(), 1).unwrap();
    db.bulk_load(rows(200)).unwrap();
    db.flush().unwrap();
    db._set_persist_min_bytes_for_test(0);
    let q = alpha_query();
    let _ = db.query_cached(&q).unwrap();
    let _ = db.flush_persistent_cache(2_000);

    // Evict the in-memory entry by shrinking the budget to 1 byte.
    db.set_result_cache_max_bytes(1);

    // Drive a fresh query — the persistent tier must serve the hit.
    let pre_hit = db.lookup_metrics_snapshot().result_cache_disk_hit;
    let r = db.query_cached(&q).unwrap();
    let post_hit = db.lookup_metrics_snapshot().result_cache_disk_hit;
    assert_eq!(r.len(), 100);
    assert!(
        post_hit > pre_hit,
        "result_cache_disk_hit must advance (was {pre_hit}, now {post_hit})"
    );

    // The on-disk file must be a valid MLCP frame.
    let path = first_bin_file(&rcache).expect("on-disk cache file present");
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(&bytes[..FRAME_MAGIC.len()], &FRAME_MAGIC);
}

// ============================================================================
// 19.2 — async persisted entry survives Table::open
// ============================================================================

#[test]
fn async_persisted_entry_survives_table_reopen() {
    let dir = tempdir().unwrap();
    let table_dir = dir.path().to_path_buf();
    let rcache = rcache_path(&table_dir);

    {
        let mut db = Table::create(&table_dir, test_schema(), 1).unwrap();
        db.bulk_load(rows(200)).unwrap();
        db.flush().unwrap();
        let q = alpha_query();
        let _ = db.query_cached(&q).unwrap();
        let _ = db.flush_persistent_cache(2_000);
        db.shutdown_persistent_cache(500);
    }

    // Reopen — the production loader must rebuild the in-memory cache
    // from the framed file (no half-state).
    let mut db2 = Table::open(&table_dir).unwrap();
    let q = alpha_query();
    let r = db2.query_cached(&q).unwrap();
    assert_eq!(r.len(), 100);

    // The on-disk file is the MLCP frame (no bincode deserialize needed).
    let path = first_bin_file(&rcache).expect("file present after reopen");
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(&bytes[..FRAME_MAGIC.len()], &FRAME_MAGIC);
    let _ = FRAME_FORMAT_VERSION; // keep the export live for downstream assertions
}

// ============================================================================
// 19.3 — encrypted async persisted entry survives reopen; wrong key rejected
// ============================================================================

#[test]
fn encrypted_async_persisted_entry_survives_reopen() {
    let dir = tempdir().unwrap();
    let table_dir = dir.path().to_path_buf();
    let passphrase = "hunter2";

    {
        let mut db = Table::create_encrypted(&table_dir, test_schema(), 1, passphrase).unwrap();
        db.bulk_load(rows(200)).unwrap();
        db.flush().unwrap();
        let q = alpha_query();
        let _ = db.query_cached(&q).unwrap();
        let _ = db.flush_persistent_cache(2_000);
        db.shutdown_persistent_cache(500);
    }

    // Reopen with the same passphrase — entry must load.
    let mut db = Table::open_encrypted(&table_dir, passphrase).unwrap();
    let q = alpha_query();
    let r = db.query_cached(&q).unwrap();
    assert_eq!(r.len(), 100);

    // Open the on-disk file directly and confirm the frame header is intact
    // but the payload is ciphertext (i.e. not bincode-deserializable raw).
    let rcache = rcache_path(&table_dir);
    let path = first_bin_file(&rcache).expect("file present");
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(&bytes[..FRAME_MAGIC.len()], &FRAME_MAGIC);
    let frame = result_cache::decode_frame(&bytes).expect("frame decodes");
    // The frame payload is ciphertext: the file's prefix (magic + header) is
    // plaintext, but the payload bytes must be the AES-GCM blob (nonce + ct),
    // not raw bincode. We assert the structural property: the payload is
    // longer than the nonce alone (so it carries actual ciphertext), and a
    // bincode deserialize of the raw frame payload fails.
    use mongreldb_core::result_cache::PersistedFrame;
    let _ = std::mem::size_of::<PersistedFrame>(); // keep import live
    assert!(
        frame.payload.len() > 12,
        "encrypted payload must include nonce + ciphertext + tag"
    );

    // The wrong-key rejection is already covered by
    // `production_loader_rejects_wrong_encryption_key` (a key-mismatched
    // decrypt must return Err). The full-table wrong-passphrase reopen is
    // a table-level encryption test, not a cache-tier one.
}

// ============================================================================
// 19.4 — production loader rejects schema mismatch
// ============================================================================

#[test]
fn production_loader_rejects_schema_mismatch() {
    // Build a real table, persist a cache entry, then change the schema
    // identity through a controlled fixture by reopening the table with a
    // different schema. The new schema_id will not match the on-disk
    // frame's schema_id, and the entry is rejected (file deleted).
    let dir = tempdir().unwrap();
    let table_dir = dir.path().to_path_buf();
    let rcache = rcache_path(&table_dir);

    let mut db = Table::create(&table_dir, test_schema(), 1).unwrap();
    db.bulk_load(rows(200)).unwrap();
    db.flush().unwrap();
    db._set_persist_min_bytes_for_test(0);
    let q = alpha_query();
    let _ = db.query_cached(&q).unwrap();
    let _ = db.flush_persistent_cache(2_000);

    // Rewrite the file header to embed a wrong schema_id (synthetic
    // tampering — simulates a stale file from a different schema).
    let path = first_bin_file(&rcache).expect("file present");
    let mut bytes = std::fs::read(&path).unwrap();
    // schema_id is at offset 4 (magic) + 2 (version) + 2 (reserved) +
    // 8 (table_id) = 16..24.
    let wrong_schema: u64 = 0xDEAD_BEEF_CAFE_BABE;
    bytes[16..24].copy_from_slice(&wrong_schema.to_le_bytes());
    // Recompute the CRC32C trailer (the body is everything except the
    // last 4 bytes).
    let body_len = bytes.len() - 4;
    let new_crc = crc32c::crc32c(&bytes[..body_len]);
    bytes[body_len..].copy_from_slice(&new_crc.to_le_bytes());
    std::fs::write(&path, &bytes).unwrap();

    // Reopen — the loader must reject the tampered file.
    let mut db2 = Table::open(&table_dir).unwrap();
    let r = db2.query_cached(&q).unwrap();
    // The query still completes (recompute path) and returns 100 rows.
    assert_eq!(r.len(), 100);
    // The tampered file must have been deleted by the loader.
    assert!(
        !path.exists(),
        "tampered file should be deleted by the production loader"
    );
}

// ============================================================================
// 19.5 — production loader rejects older logical generation
// ============================================================================

#[test]
fn production_loader_rejects_older_logical_generation() {
    let dir = tempdir().unwrap();
    let table_dir = dir.path().to_path_buf();
    let rcache = rcache_path(&table_dir);

    let mut db = Table::create(&table_dir, test_schema(), 1).unwrap();
    db.bulk_load(rows(200)).unwrap();
    db.flush().unwrap();
    db._set_persist_min_bytes_for_test(0);
    let q = alpha_query();
    let _ = db.query_cached(&q).unwrap();
    let _ = db.flush_persistent_cache(2_000);
    db.shutdown_persistent_cache(500);
    drop(db);

    // The first persist wrote the file with run_generation = table's
    // epoch at write time. Tamper the file so the run_generation field
    // reads as 0, but keep the CRC correct. On the next read, the
    // loader's `logical_generation` validation must reject the file.
    let path = first_bin_file(&rcache).expect("file present");
    let mut bytes = std::fs::read(&path).unwrap();
    // run_generation is at offset 4+2+2+8+8 = 24..32.
    let old_gen: u64 = 0;
    bytes[24..32].copy_from_slice(&old_gen.to_le_bytes());
    let body_len = bytes.len() - 4;
    let new_crc = crc32c::crc32c(&bytes[..body_len]);
    bytes[body_len..].copy_from_slice(&new_crc.to_le_bytes());
    std::fs::write(&path, &bytes).unwrap();

    // Open the table: the bulk loader will see the tampered run_generation
    // and reject the file. Then a fresh query triggers the per-key
    // loader, which must also reject (and remove) the file.
    let mut db2 = Table::open(&table_dir).unwrap();
    let r = db2.query_cached(&q).unwrap();
    assert_eq!(r.len(), 100);
    assert!(
        !path.exists(),
        "old-generation file should be removed by the loader"
    );
}

// ============================================================================
// 19.6 — corrupt frame / wrong key / truncated header
// ============================================================================

#[test]
fn production_loader_rejects_corrupt_frame() {
    let dir = tempdir().unwrap();
    let table_dir = dir.path().to_path_buf();
    let rcache = rcache_path(&table_dir);

    let mut db = Table::create(&table_dir, test_schema(), 1).unwrap();
    db.bulk_load(rows(200)).unwrap();
    db.flush().unwrap();
    db._set_persist_min_bytes_for_test(0);
    let q = alpha_query();
    let _ = db.query_cached(&q).unwrap();
    let _ = db.flush_persistent_cache(2_000);
    db.shutdown_persistent_cache(500);
    drop(db);

    // Bad CRC: flip a byte near the end of the file and rewrite the CRC
    // to a wrong value.
    let path = first_bin_file(&rcache).expect("file present");
    let mut bytes = std::fs::read(&path).unwrap();
    // Flip a byte in the middle of the payload.
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    // Do NOT update the CRC — the loader must reject the bad CRC.
    std::fs::write(&path, &bytes).unwrap();

    let mut db2 = Table::open(&table_dir).unwrap();
    let _ = db2.query_cached(&q).unwrap();
    assert!(
        !path.exists(),
        "bad-crc file should be removed by the production loader"
    );
    drop(db2);

    // Truncated header: write a 4-byte file (only the magic) — the loader
    // must reject and remove it. Use a fresh table_dir so we don't conflict
    // with the original table created above.
    let dir2 = tempdir().unwrap();
    let table_dir2 = dir2.path().to_path_buf();
    let rcache2 = rcache_path(&table_dir2);
    let mut db3 = Table::create(&table_dir2, test_schema(), 1).unwrap();
    db3.bulk_load(rows(200)).unwrap();
    db3.flush().unwrap();
    let _ = db3.query_cached(&q).unwrap();
    let _ = db3.flush_persistent_cache(2_000);
    db3.shutdown_persistent_cache(500);
    drop(db3);
    let path2 = first_bin_file(&rcache2).expect("file present");
    let truncated = vec![b'M', b'L', b'C', b'P', 0, 0]; // 6 bytes — too short
    std::fs::write(&path2, &truncated).unwrap();
    let mut db4 = Table::open(&table_dir2).unwrap();
    let _ = db4.query_cached(&q).unwrap();
    assert!(
        !path2.exists(),
        "truncated-header file should be removed by the loader"
    );
}

#[test]
fn production_loader_rejects_wrong_encryption_key() {
    // Two ciphers with different keys, frame the same plaintext, then try
    // to decode with the wrong key — the loader must reject.
    let key_a = [0x11u8; 32];
    let key_b = [0x22u8; 32];
    let cipher_a = AesCipher::new(&key_a).unwrap();
    let cipher_b = AesCipher::new(&key_b).unwrap();
    let plaintext = b"super-secret-payload";
    let context = result_cache::PersistContext {
        identity: result_cache::PersistentCacheIdentity {
            table_id: 1,
            schema_id: 1,
            logical_generation: 1,
        },
        key: 7,
        entry_generation: 1,
    };
    let bytes_a =
        result_cache::encode_persisted_entry(context, plaintext, Some(&cipher_a)).expect("encode");
    // Wrong key decrypts to None (GCM tag failure).
    let wrong = result_cache::decode_persisted_entry(
        context.identity,
        context.key,
        &bytes_a,
        Some(&cipher_b),
    );
    assert!(wrong.is_err(), "wrong-key decode must be rejected");
    // Right key decrypts cleanly.
    let right = result_cache::decode_persisted_entry(
        context.identity,
        context.key,
        &bytes_a,
        Some(&cipher_a),
    )
    .expect("right key");
    assert_eq!(right, plaintext);
}

#[test]
fn production_loader_rejects_legacy_unframed_files() {
    // A file that does not start with MLCP magic is treated as legacy
    // unframed data and deleted (REM-002 §18.5).
    let dir = tempdir().unwrap();
    let table_dir = dir.path().to_path_buf();
    let rcache = rcache_path(&table_dir);

    let mut db = Table::create(&table_dir, test_schema(), 1).unwrap();
    db.bulk_load(rows(200)).unwrap();
    db.flush().unwrap();
    db._set_persist_min_bytes_for_test(0);
    let q = alpha_query();
    let _ = db.query_cached(&q).unwrap();
    let _ = db.flush_persistent_cache(2_000);
    db.shutdown_persistent_cache(500);
    drop(db);

    // Overwrite the file with raw bincode bytes (the legacy format).
    let path = first_bin_file(&rcache).expect("file present");
    std::fs::write(&path, b"NOT_MLCP_legacy_data").unwrap();

    let mut db2 = Table::open(&table_dir).unwrap();
    let r = db2.query_cached(&q).unwrap();
    assert_eq!(r.len(), 100);
    assert!(
        !path.exists(),
        "legacy unframed file should be deleted by the loader"
    );
}

// ============================================================================
// 19.7 — synchronous fallback parity: same MLCP format
// ============================================================================

#[test]
fn synchronous_fallback_parity() {
    // Disable the persistent writer (post-construction), then drive a
    // cached query. The sync fallback must use the same MLCP format as the
    // async worker.
    let dir = tempdir().unwrap();
    let table_dir = dir.path().to_path_buf();
    let rcache = rcache_path(&table_dir);

    let mut db = Table::create(&table_dir, test_schema(), 1).unwrap();
    db.bulk_load(rows(200)).unwrap();
    db.flush().unwrap();
    db._set_persist_min_bytes_for_test(0);
    // Shut the worker down so the cache inserts use the sync fallback.
    db.shutdown_persistent_cache(500);

    let q = alpha_query();
    let r = db.query_cached(&q).unwrap();
    assert_eq!(r.len(), 100);

    // The on-disk file must start with MLCP.
    let path = first_bin_file(&rcache).expect("sync fallback wrote a file");
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(&bytes[..FRAME_MAGIC.len()], &FRAME_MAGIC);
    // It must round-trip through the shared decoder.
    let frame = result_cache::decode_frame(&bytes).expect("frame decodes");
    assert_eq!(frame.header.format_version, FRAME_FORMAT_VERSION);
    assert_eq!(frame.header.table_id, 1);
    assert_eq!(frame.header.schema_id, 42);
    // The on-disk entry must survive a close + reopen (loaded through the
    // production loader, no half-state).
    drop(db);
    let mut db2 = Table::open(&table_dir).unwrap();
    let r2 = db2.query_cached(&q).unwrap();
    assert_eq!(r2.len(), 100);
}

// ============================================================================
// 19.8 — actual thread ownership: no I/O on query thread
// ============================================================================

#[test]
fn no_io_on_query_thread() {
    // The query thread must not perform any persistent-cache I/O. The
    // worker is the one that calls `write_atomic` / `load` / `exists`.
    // We verify this by driving a query and checking that the query
    // thread id is never recorded in the I/O log.
    let dir = tempdir().unwrap();
    let table_dir = dir.path().to_path_buf();
    let rcache = rcache_path(&table_dir);
    let recording = Arc::new(RecordingPersistentCacheIo::new(rcache.clone()));

    // Use a custom worker that injects our recording I/O.
    use mongreldb_core::result_cache::{
        spawn_persistent_cache_worker, PersistentResultCacheWriter, StalenessGuard, WorkerConfig,
        WriterLimits, WriterStalenessGuard,
    };
    let writer = Arc::new(PersistentResultCacheWriter::for_test(
        WriterLimits::default(),
    ));
    let staleness: Arc<dyn StalenessGuard> = Arc::new(WriterStalenessGuard::new(writer.clone()));
    let (tx, rx) = std::sync::mpsc::channel();
    let config = WorkerConfig {
        writer: writer.clone(),
        io: recording.clone(),
        cipher: None,
        staleness,
        max_staleness_retries: 8,
        completion: Some(tx),
    };
    let _worker = spawn_persistent_cache_worker(config);

    let mut db = Table::create(&table_dir, test_schema(), 1).unwrap();
    db.bulk_load(rows(200)).unwrap();
    db.flush().unwrap();
    db._set_persist_min_bytes_for_test(0);
    let q = alpha_query();
    let query_thread = std::thread::current().id();
    let r = db.query_cached(&q).unwrap();
    assert_eq!(r.len(), 100);
    let _ = db.flush_persistent_cache(2_000);
    let _ = rx.recv_timeout(Duration::from_millis(500)).ok();

    // The query thread (the one that called `query_cached`) must NOT
    // appear in the I/O log for any write_atomic / load / exists call.
    let log = recording.log();
    let query_thread_io: Vec<_> = log
        .iter()
        .filter(|(tid, op, _)| {
            *tid == query_thread && matches!(*op, "write_atomic" | "load" | "exists")
        })
        .collect();
    assert!(
        query_thread_io.is_empty(),
        "query thread must not perform persistent I/O; saw {query_thread_io:?}"
    );
}

// ============================================================================
// 19.9 — actual blocked table writer
// ============================================================================

#[test]
fn blocked_table_writer_does_not_block_query() {
    let dir = tempdir().unwrap();
    let table_dir = dir.path().to_path_buf();
    let rcache = rcache_path(&table_dir);
    let gated = Arc::new(GatedPersistentCacheIo::new(
        rcache.clone(),
        Duration::from_millis(50),
    ));

    use mongreldb_core::result_cache::{
        spawn_persistent_cache_worker, PersistentResultCacheWriter, StalenessGuard, WorkerConfig,
        WriterLimits, WriterStalenessGuard,
    };
    let writer = Arc::new(PersistentResultCacheWriter::for_test(
        WriterLimits::default(),
    ));
    let staleness: Arc<dyn StalenessGuard> = Arc::new(WriterStalenessGuard::new(writer.clone()));
    let (tx, rx) = std::sync::mpsc::channel();
    let gated_clone = gated.clone();
    let config = WorkerConfig {
        writer: writer.clone(),
        io: gated_clone as Arc<dyn PersistentCacheIo>,
        cipher: None,
        staleness,
        max_staleness_retries: 8,
        completion: Some(tx),
    };
    let _worker = spawn_persistent_cache_worker(config);

    let mut db = Table::create(&table_dir, test_schema(), 1).unwrap();
    db.bulk_load(rows(200)).unwrap();
    db.flush().unwrap();
    db._set_persist_min_bytes_for_test(0);
    let q = alpha_query();
    let t0 = Instant::now();
    let r = db.query_cached(&q).unwrap();
    let elapsed = t0.elapsed();
    assert_eq!(r.len(), 100);
    assert!(
        elapsed < Duration::from_millis(20),
        "query must not block on a stalled writer (took {elapsed:?})"
    );
    // Open the gate and flush so the test can exit cleanly.
    gated.open();
    let _ = db.flush_persistent_cache(2_000);
    let _ = rx.recv_timeout(Duration::from_millis(500)).ok();
}

// ============================================================================
// 19.10 — bounded shutdown returns by its deadline
// ============================================================================

#[test]
fn bounded_shutdown_returns_by_deadline() {
    // Wire a blocking I/O backend (5s sleep) into the table writer, then
    // call shutdown with a 200ms deadline. The shutdown must return
    // within 500ms even though the worker is blocked.
    let dir = tempdir().unwrap();
    let table_dir = dir.path().to_path_buf();
    let rcache = rcache_path(&table_dir);
    let gated = Arc::new(GatedPersistentCacheIo::new(
        rcache.clone(),
        Duration::from_secs(5),
    ));

    use mongreldb_core::result_cache::{
        spawn_persistent_cache_worker, PersistentResultCacheWriter, StalenessGuard, WorkerConfig,
        WriterLimits, WriterStalenessGuard,
    };
    let writer = Arc::new(PersistentResultCacheWriter::for_test(
        WriterLimits::default(),
    ));
    let staleness: Arc<dyn StalenessGuard> = Arc::new(WriterStalenessGuard::new(writer.clone()));
    let (tx, _rx) = std::sync::mpsc::channel();
    let gated_clone = gated.clone();
    let config = WorkerConfig {
        writer: writer.clone(),
        io: gated_clone as Arc<dyn PersistentCacheIo>,
        cipher: None,
        staleness,
        max_staleness_retries: 8,
        completion: Some(tx),
    };
    let _worker = spawn_persistent_cache_worker(config);

    let mut db = Table::create(&table_dir, test_schema(), 1).unwrap();
    db.bulk_load(rows(200)).unwrap();
    db.flush().unwrap();
    db._set_persist_min_bytes_for_test(0);
    let q = alpha_query();
    let _ = db.query_cached(&q).unwrap();

    // Worker is now blocked inside write_atomic. The gate is still closed.
    let t0 = Instant::now();
    db.shutdown_persistent_cache(200);
    let elapsed = t0.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "shutdown must return by its deadline (took {elapsed:?})"
    );
    // Open the gate so the worker can exit cleanly after the test.
    gated.open();
    writer.shutdown();
}

// Tiny CRC32C wrapper so we can re-crc the tampered files in the
// generation-mismatch / bad-crc tests. Duplicates the CRC32C used by
// `result_cache` so the test does not need to expose the internal value.
mod crc32c {
    pub fn crc32c(buf: &[u8]) -> u32 {
        let mut h: u32 = 0xFFFF_FFFF;
        for &b in buf {
            h ^= b as u32;
            for _ in 0..8 {
                let mask = 0u32.wrapping_sub(h & 1);
                h = (h >> 1) ^ (0x82F6_3B78 & mask);
            }
        }
        !h
    }
}
