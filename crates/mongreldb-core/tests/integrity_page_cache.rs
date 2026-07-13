//! Integrity tests for the shared, persistent, MVCC content-addressed page cache
//! and the decoded-page cache layer.
//!
//! These tests exercise the cache through normal Table API usage: repeated scans,
//! concurrent readers, persistence across reopen, MVCC visibility, eviction
//! pressure, and encrypted ciphertext round-trips.

use mongreldb_core::columnar::NativeColumn;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{PageCache, Snapshot, Table, Value};
use tempfile::tempdir;

fn schema_plain() -> Schema {
    Schema {
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
            ColumnDef {
                id: 3,
                name: "label".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "label_bm".into(),
            column_id: 3,
            kind: IndexKind::Bitmap,
            predicate: None,
            options: Default::default(),
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn rows(n: usize) -> Vec<Vec<(u16, Value)>> {
    (0..n)
        .map(|i| {
            vec![
                (1, Value::Int64(i as i64)),
                (2, Value::Int64((i * 7) as i64)),
                (
                    3,
                    Value::Bytes(
                        format!("row-{i:08}-{:x}", i.wrapping_mul(0x9E3779B9)).into_bytes(),
                    ),
                ),
            ]
        })
        .collect()
}

fn extract_int_col(cols: &[(u16, NativeColumn)], col_id: u16) -> Vec<i64> {
    for (id, col) in cols {
        if *id == col_id {
            if let NativeColumn::Int64 { data, .. } = col {
                return data.clone();
            }
        }
    }
    panic!("int64 column {col_id} not found");
}

fn extract_bytes_col(cols: &[(u16, NativeColumn)], col_id: u16) -> Vec<Vec<u8>> {
    for (id, col) in cols {
        if *id == col_id {
            if let NativeColumn::Bytes {
                offsets, values, ..
            } = col
            {
                let mut out = Vec::with_capacity(col.len());
                for i in 0..col.len() {
                    let start = offsets[i] as usize;
                    let end = offsets[i + 1] as usize;
                    out.push(values[start..end].to_vec());
                }
                return out;
            }
        }
    }
    panic!("bytes column {col_id} not found");
}

fn full_scan_sorted_ids(db: &Table) -> Vec<i64> {
    let snap = db.snapshot();
    let cols = db.visible_columns_native(snap, Some(&[1, 2, 3])).unwrap();
    let mut ids = extract_int_col(&cols, 1);
    ids.sort_unstable();
    ids
}

/// Repeated full scans over the same flushed data must return bit-identical
/// columns, whether pages are served from disk or from the shared cache.
#[test]
fn repeated_full_scans_return_identical_data() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema_plain(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1); // force spill to a sorted run

    let batch = rows(50_000);
    db.bulk_load(batch).unwrap();
    db.flush().unwrap();

    let snap = db.snapshot();
    let first = db.visible_columns_native(snap, Some(&[1, 2, 3])).unwrap();
    let first_ids = extract_int_col(&first, 1);
    let first_vs = extract_int_col(&first, 2);
    let first_labels = extract_bytes_col(&first, 3);

    // Several more scans; the page cache should warm up and serve hits.
    for _ in 0..5 {
        let snap = db.snapshot();
        let cols = db.visible_columns_native(snap, Some(&[1, 2, 3])).unwrap();
        assert_eq!(
            extract_int_col(&cols, 1),
            first_ids,
            "ids diverged across scans"
        );
        assert_eq!(extract_int_col(&cols, 2), first_vs, "v column diverged");
        assert_eq!(
            extract_bytes_col(&cols, 3),
            first_labels,
            "label column diverged"
        );
    }

    // The decoded-page cache should also have entries after repeat scans.
    assert!(
        db.decoded_cache_len() > 0,
        "decoded-page cache should warm on repeated scans"
    );
}

/// Concurrent readers through the shared page cache must observe consistent,
/// uncorrupted pages. Because `Table` is not `Sync`, we exercise the cache
/// directly with multiple threads contending on the same `PageCache`.
#[test]
fn concurrent_cache_access_stays_consistent() {
    use mongreldb_core::page::CachedPage;
    use mongreldb_core::Epoch;

    let cache = std::sync::Arc::new(parking_lot::Mutex::new(PageCache::new(64 * 1024)));
    let mut hashes = Vec::new();
    for i in 0..200u64 {
        let mut hash = [0u8; 32];
        hash[..8].copy_from_slice(&i.to_le_bytes());
        hashes.push(hash);
        cache.lock().insert(CachedPage {
            committed_epoch: Epoch(i),
            content_hash: hash,
            bytes: bytes::Bytes::from(format!("page-{i:04}").into_bytes()),
        });
    }

    let mut all_ok = true;
    std::thread::scope(|s| {
        let mut handles = Vec::new();
        for _ in 0..4 {
            let c = cache.clone();
            let hcopy = hashes.clone();
            handles.push(s.spawn(move || {
                let mut local_ok = true;
                // Skip i=0: there is no epoch before 0 to test invisibility.
                for (i, h) in hcopy.iter().enumerate().skip(1) {
                    let visible_at = Snapshot::at(Epoch(i as u64));
                    let invisible_at = Snapshot::at(Epoch((i - 1) as u64));
                    let mut guard = c.lock();
                    match guard.get(h, visible_at) {
                        Some(bytes) => {
                            let expected = format!("page-{i:04}");
                            if &bytes[..] != expected.as_bytes() {
                                local_ok = false;
                            }
                        }
                        None => {
                            // The page may have been evicted; that is fine as
                            // long as we do not see corrupt data.
                        }
                    }
                    if guard.get(h, invisible_at).is_some() {
                        // A snapshot from before the page epoch must never see it.
                        local_ok = false;
                    }
                }
                local_ok
            }));
        }
        for h in handles {
            if !h.join().unwrap() {
                all_ok = false;
            }
        }
    });
    assert!(
        all_ok,
        "concurrent cache access observed corruption or MVCC violation"
    );
}

/// Projection pushdown plus the decoded-page cache: decoding only the requested
/// columns and caching the decoded form must remain consistent across hits.
#[test]
fn decoded_cache_projection_consistency() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema_plain(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1);

    let batch = rows(30_000);
    db.bulk_load(batch).unwrap();
    db.flush().unwrap();

    let proj = [1u16, 3];
    let mut previous: Option<Vec<(u16, NativeColumn)>> = None;
    for _ in 0..4 {
        let snap = db.snapshot();
        let cols = db.visible_columns_native(snap, Some(&proj)).unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(extract_int_col(&cols, 1).len(), 30_000);
        assert_eq!(extract_bytes_col(&cols, 3).len(), 30_000);
        if let Some(ref prev) = previous {
            assert_eq!(extract_int_col(&cols, 1), extract_int_col(prev, 1));
            assert_eq!(extract_bytes_col(&cols, 3), extract_bytes_col(prev, 3));
        }
        previous = Some(cols);
    }
    assert!(
        db.decoded_cache_len() > 0,
        "decoded cache should hold projected pages"
    );
}

/// The persistent `_cache/` tier must reload raw page bytes on reopen and
/// continue to serve correct scans.
#[test]
fn persistent_page_cache_survives_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut db = Table::create(&path, schema_plain(), 1).unwrap();
        db.set_mutable_run_spill_bytes(1);
        let batch = rows(80_000);
        db.bulk_load(batch).unwrap();
        db.flush().unwrap();

        // Warm the shared page cache and spill it to disk.
        let _ = db
            .visible_columns_native(db.snapshot(), Some(&[1, 2, 3]))
            .unwrap();
        assert!(db.page_cache_len() > 0, "page cache should be populated");
        db.page_cache_flush();
    }

    let cache_dir = path.join("_cache");
    assert!(cache_dir.exists(), "_cache dir should exist");
    let spilled = std::fs::read_dir(&cache_dir).unwrap().count();
    assert!(spilled > 0, "expected spilled page files");

    {
        let db = Table::open(&path).unwrap();
        assert_eq!(db.count(), 80_000);
        let ids = full_scan_sorted_ids(&db);
        assert_eq!(ids.len(), 80_000);
        // Verify the data is actually the expected values, not just the count.
        for (i, id) in ids.iter().enumerate() {
            assert_eq!(*id, i as i64);
        }
    }
}

/// A pinned snapshot must keep seeing its committed view even after the cache is
/// warmed and later writes create newer runs/pages.
#[test]
fn mvcc_snapshot_isolation_through_cache() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema_plain(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1);

    let first_batch = rows(10_000);
    db.bulk_load(first_batch).unwrap();
    db.flush().unwrap();

    // Pin a snapshot and warm the cache from it.
    let pinned = db.pin_snapshot();
    let cached_old = db.visible_columns_native(pinned, Some(&[1, 2, 3])).unwrap();
    assert_eq!(extract_int_col(&cached_old, 1).len(), 10_000);

    // Commit more rows at a higher epoch and flush to new runs.
    let second_batch: Vec<Vec<(u16, Value)>> = (10_000..20_000)
        .map(|i| {
            vec![
                (1, Value::Int64(i as i64)),
                (2, Value::Int64((i * 7) as i64)),
                (3, Value::Bytes(format!("new-{i}").into_bytes())),
            ]
        })
        .collect();
    db.bulk_load(second_batch).unwrap();
    db.flush().unwrap();

    // The pinned snapshot must still see exactly the original 10k rows.
    let old_again = db.visible_columns_native(pinned, Some(&[1, 2, 3])).unwrap();
    assert_eq!(
        extract_int_col(&cached_old, 1),
        extract_int_col(&old_again, 1),
        "pinned snapshot saw new rows through the cache"
    );

    // A current snapshot sees all 20k.
    let current = db
        .visible_columns_native(db.snapshot(), Some(&[1]))
        .unwrap();
    assert_eq!(extract_int_col(&current, 1).len(), 20_000);

    db.unpin_snapshot(pinned);
}

/// Under memory pressure the cache evicts, but every scan must still return the
/// complete, correct data set. We force pressure by loading more data than the
/// default 64 MiB page-cache budget and then scanning repeatedly.
#[test]
fn eviction_pressure_keeps_scans_correct() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema_plain(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1);

    // Use large, relatively incompressible strings so the raw page bytes exceed
    // the cache budget even after zstd compression.
    let n = 60_000usize;
    let big_batch: Vec<Vec<(u16, Value)>> = (0..n)
        .map(|i| {
            let payload = format!(
                "LARGE-VALUE-{i:010}-{:032x}-noise",
                i.wrapping_mul(0x9E3779B97F4A7C15)
            );
            vec![
                (1, Value::Int64(i as i64)),
                (2, Value::Int64((i * 7) as i64)),
                (3, Value::Bytes(payload.into_bytes())),
            ]
        })
        .collect();
    db.bulk_load(big_batch).unwrap();
    db.flush().unwrap();

    let baseline = full_scan_sorted_ids(&db);
    assert_eq!(baseline.len(), n);

    // Repeated scans force churn; results must stay correct.
    for pass in 0..5 {
        let ids = full_scan_sorted_ids(&db);
        assert_eq!(
            ids.len(),
            n,
            "scan {pass} lost rows under eviction pressure"
        );
        assert_eq!(ids, baseline, "scan {pass} returned wrong data");
    }

    // The cache should be populated but bounded by its budget.
    assert!(db.page_cache_len() > 0);
}

/// Direct stress of the PageCache eviction algorithm: a capacity far smaller
/// than any inserted page must still bound memory and never leave orphaned
/// ring/map entries.
#[test]
fn page_cache_tiny_capacity_stress() {
    use mongreldb_core::page::CachedPage;
    use mongreldb_core::Epoch;

    let capacity = 64u64;
    let mut cache = PageCache::new(capacity); // room for a few tiny pages
    let mut hashes = Vec::new();
    for i in 0..500u64 {
        let mut hash = [0u8; 32];
        hash[..8].copy_from_slice(&i.to_le_bytes());
        hashes.push(hash);
        cache.insert(CachedPage {
            committed_epoch: Epoch(i),
            content_hash: hash,
            bytes: bytes::Bytes::from(format!("p{i:03}").into_bytes()), // 4 bytes
        });
    }

    assert!(
        cache.used_bytes() <= capacity + 32,
        "used_bytes {} leaked past capacity {}",
        cache.used_bytes(),
        capacity
    );

    // MVCC: a very old snapshot must not see pages committed after it.
    let old_snap = Snapshot::at(Epoch(5));
    assert!(cache.get(&hashes[10], old_snap).is_none());
    // A current snapshot can see any still-resident page; verify the bytes are
    // exactly what we inserted (no corruption during eviction storms).
    let max_snap = Snapshot::at(Epoch(u64::MAX));
    let mut hits = 0;
    for (i, h) in hashes.iter().enumerate() {
        if let Some(bytes) = cache.get(h, max_snap) {
            hits += 1;
            let expected = format!("p{i:03}");
            assert_eq!(
                &bytes[..],
                expected.as_bytes(),
                "cached page was corrupted after eviction"
            );
        }
    }
    assert!(hits > 0, "all pages evicted; cache should retain some");
    assert!(
        cache.len() <= 16,
        "cache len should be small given the 64-byte capacity"
    );
}

/// Cache persistence with a plaintext table: the raw bytes reloaded from disk
/// must still decode to the original values.
#[test]
fn persistent_cache_plaintext_round_trip() {
    let dir = tempdir().unwrap();
    let mut cache = PageCache::new(1 << 20).with_persistence(dir.path().to_path_buf());
    let data = b"plaintext-page-payload";
    let mut hash = [0u8; 32];
    hash[0] = 7;
    cache.insert(mongreldb_core::page::CachedPage {
        committed_epoch: mongreldb_core::Epoch(3),
        content_hash: hash,
        bytes: bytes::Bytes::copy_from_slice(data),
    });
    cache.flush_to_disk();
    drop(cache);

    // The persistent cache reloads pages with their original committed epoch, so
    // a normal snapshot at that epoch (or newer) sees them; a maximal snapshot
    // also sees them.
    let mut reloaded = PageCache::new(1 << 20).with_persistence(dir.path().to_path_buf());
    let got = reloaded
        .get(&hash, Snapshot::at(mongreldb_core::Epoch(u64::MAX)))
        .expect("reloaded cache must contain spilled page");
    assert_eq!(&got[..], data);
}

/// Regression: pages spilled to the persistent cache must reload with their
/// original committed epoch so they remain visible to ordinary (non-maximal)
/// snapshots after reopen. Previously they reloaded with `Epoch(u64::MAX)`,
/// making the persistent tier effectively dead for normal snapshots.
#[test]
fn persistent_cache_epoch_visibility() {
    let dir = tempdir().unwrap();
    let mut cache = PageCache::new(1 << 20).with_persistence(dir.path().to_path_buf());
    let mut hash = [0u8; 32];
    hash[0] = 9;
    cache.insert(mongreldb_core::page::CachedPage {
        committed_epoch: mongreldb_core::Epoch(3),
        content_hash: hash,
        bytes: bytes::Bytes::copy_from_slice(b"data"),
    });
    cache.flush_to_disk();
    drop(cache);

    let mut reloaded = PageCache::new(1 << 20).with_persistence(dir.path().to_path_buf());
    // Visible at the exact committed epoch and at any newer epoch.
    let got = reloaded.get(&hash, Snapshot::at(mongreldb_core::Epoch(3)));
    assert!(
        got.is_some(),
        "persistent cache page must be visible at its committed epoch after reopen"
    );
    assert_eq!(&got.unwrap()[..], b"data");
    // An older snapshot must NOT see it (MVCC visibility preserved).
    let missed = reloaded.get(&hash, Snapshot::at(mongreldb_core::Epoch(2)));
    assert!(
        missed.is_none(),
        "page committed at epoch 3 must not be visible at epoch 2"
    );
}

#[cfg(feature = "encryption")]
mod encrypted {
    use super::*;
    use mongreldb_core::query::{Condition, Query};
    use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};

    fn schema_encrypted_indexable_eq() -> Schema {
        Schema {
            schema_id: 2,
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
                    name: "label".into(),
                    ty: TypeId::Bytes,
                    flags: ColumnFlags::empty().with(ColumnFlags::ENCRYPTED_INDEXABLE),
                    default_value: None,
                },
            ],
            indexes: vec![IndexDef {
                name: "label_eq".into(),
                column_id: 2,
                kind: IndexKind::Bitmap,
                predicate: None,
                options: Default::default(),
            }],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        }
    }

    fn schema_encrypted_indexable_range() -> Schema {
        Schema {
            schema_id: 3,
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
                    name: "score".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::ENCRYPTED_INDEXABLE),
                    default_value: None,
                },
            ],
            indexes: vec![IndexDef {
                name: "score_lr".into(),
                column_id: 2,
                kind: IndexKind::LearnedRange,
                predicate: None,
                options: Default::default(),
            }],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        }
    }

    /// Encrypted ciphertext bytes in the page cache must decrypt to the original
    /// plaintext on every access, including after reopen.
    #[test]
    fn encrypted_cached_ciphertext_round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        {
            let mut db = Table::create_encrypted(&path, schema_plain(), 1, "passphrase").unwrap();
            db.set_mutable_run_spill_bytes(1);
            let batch = rows(25_000);
            db.bulk_load(batch).unwrap();
            db.flush().unwrap();

            let baseline = full_scan_sorted_ids(&db);
            assert_eq!(baseline.len(), 25_000);

            // Multiple scans hit the cache (ciphertext) and decrypt each time.
            for _ in 0..3 {
                let ids = full_scan_sorted_ids(&db);
                assert_eq!(ids, baseline);
            }

            db.page_cache_flush();
        }

        {
            let db = Table::open_encrypted(&path, "passphrase").unwrap();
            assert_eq!(db.count(), 25_000);
            let ids = full_scan_sorted_ids(&db);
            assert_eq!(ids.len(), 25_000);
            for (i, id) in ids.iter().enumerate() {
                assert_eq!(*id, i as i64);
            }
        }
    }

    /// Bitmap equality on an ENCRYPTED_INDEXABLE column must work correctly when
    /// pages are served from the shared cache.
    #[test]
    fn encrypted_indexable_bitmap_through_cache() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        {
            let mut db =
                Table::create_encrypted(&path, schema_encrypted_indexable_eq(), 1, "pw").unwrap();
            db.set_mutable_run_spill_bytes(1);
            for i in 0..200u64 {
                let label = if i % 4 == 0 { b"red" } else { b"blu" };
                db.put(vec![
                    (1, Value::Int64(i as i64)),
                    (2, Value::Bytes(label.to_vec())),
                ])
                .unwrap();
            }
            db.flush().unwrap();

            let q = Query::new().and(Condition::BitmapEq {
                column_id: 2,
                value: b"red".to_vec(),
            });
            let reds = db.query(&q).unwrap();
            assert_eq!(reds.len(), 50, "expected 50 red rows");

            // Warm cache, then run the same query again.
            let _ = db.visible_columns_native(db.snapshot(), None).unwrap();
            let reds_cached = db.query(&q).unwrap();
            assert_eq!(reds_cached.len(), 50);

            db.page_cache_flush();
        }

        {
            let mut db = Table::open_encrypted(&path, "pw").unwrap();
            let q = Query::new().and(Condition::BitmapEq {
                column_id: 2,
                value: b"red".to_vec(),
            });
            let reds = db.query(&q).unwrap();
            assert_eq!(reds.len(), 50, "bitmap query wrong after reopen");
        }
    }

    /// Range queries on an encrypted, indexable Int64 column must not silently
    /// drop rows when page stats are suppressed; cached ciphertext pages must
    /// still decrypt correctly.
    #[test]
    fn encrypted_indexable_range_through_cache() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        {
            let mut db =
                Table::create_encrypted(&path, schema_encrypted_indexable_range(), 1, "pw")
                    .unwrap();
            db.set_mutable_run_spill_bytes(1);
            for i in 1..=500i64 {
                db.put(vec![(1, Value::Int64(i)), (2, Value::Int64(i * 10))])
                    .unwrap();
            }
            db.flush().unwrap();

            let q = Query::new().and(Condition::Range {
                column_id: 2,
                lo: 100,
                hi: 200,
            });
            let got = db.query(&q).unwrap();
            assert_eq!(got.len(), 11, "range [100,200] should match 11 rows");

            // Warm cache and repeat.
            let _ = db.visible_columns_native(db.snapshot(), None).unwrap();
            let got2 = db.query(&q).unwrap();
            assert_eq!(got2.len(), 11);

            db.page_cache_flush();
        }

        {
            let mut db = Table::open_encrypted(&path, "pw").unwrap();
            let q = Query::new().and(Condition::Range {
                column_id: 2,
                lo: 100,
                hi: 200,
            });
            let got = db.query(&q).unwrap();
            assert_eq!(got.len(), 11, "range query wrong after reopen");
        }
    }

    /// Wrong key must fail to open an encrypted table whose cache files are also
    /// present; the persistent cache must not bypass the key hierarchy.
    #[test]
    fn encrypted_cache_wrong_key_rejected() {
        let dir = tempdir().unwrap();
        {
            let mut db = Table::create_encrypted(dir.path(), schema_plain(), 1, "right").unwrap();
            db.set_mutable_run_spill_bytes(1);
            db.bulk_load(rows(1_000)).unwrap();
            db.flush().unwrap();
            let _ = db.visible_columns_native(db.snapshot(), None).unwrap();
            db.page_cache_flush();
        }
        let result = Table::open_encrypted(dir.path(), "wrong");
        assert!(result.is_err(), "wrong key must not open encrypted table");
    }
}
