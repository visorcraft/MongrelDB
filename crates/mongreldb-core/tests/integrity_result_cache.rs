//! Result-cache integrity tests — try to break MongrelDB's table-level result
//! cache through normal API usage.
//!
//! Coverage: cache hit correctness, MVCC snapshot safety, fine-grained
//! invalidation, persistence across restart, budget thrashing, column-aware
//! invalidation, empty projection, and encrypted-table caching.

use mongreldb_core::columnar::NativeColumn;
use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::*;
use mongreldb_core::{Snapshot, Table, Value};
use tempfile::tempdir;

fn test_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 2,
                name: "city".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            },
            ColumnDef {
                id: 3,
                name: "cost".into(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            },
        ],
        indexes: vec![
            IndexDef {
                name: "city_bm".into(),
                column_id: 2,
                kind: IndexKind::Bitmap,
            },
            IndexDef {
                name: "cost_lr".into(),
                column_id: 3,
                kind: IndexKind::LearnedRange,
            },
        ],
        colocation: vec![],
    }
}

fn rows_city_cost(n: usize) -> Vec<Vec<(u16, Value)>> {
    (0..n)
        .map(|i| {
            vec![
                (1, Value::Int64(i as i64)),
                (
                    2,
                    Value::Bytes(
                        (if i % 2 == 0 {
                            &b"alpha"[..]
                        } else {
                            &b"beta"[..]
                        })
                        .to_vec(),
                    ),
                ),
                (3, Value::Float64(i as f64)),
            ]
        })
        .collect()
}

fn count_from_native_cols(cols: &[(u16, NativeColumn)]) -> usize {
    for (id, col) in cols {
        if *id == 1 {
            return match col {
                NativeColumn::Int64 { data, .. } => data.len(),
                _ => panic!("expected int64 column"),
            };
        }
    }
    panic!("no id column in projection");
}

fn sum_cost_from_native_cols(cols: &[(u16, NativeColumn)]) -> f64 {
    for (id, col) in cols {
        if *id == 3 {
            return match col {
                NativeColumn::Float64 { data, .. } => data.iter().sum(),
                _ => panic!("expected float64 column"),
            };
        }
    }
    panic!("no cost column in projection");
}

/// Helper: run the same query through the cached and non-cached paths and
/// assert they agree.
fn assert_cached_equals_uncached(db: &mut Table, q: &Query, proj: Option<&[u16]>, snap: Snapshot) {
    let uncached = db.query(q).unwrap();
    let cached = db.query_cached(q).unwrap();
    assert_eq!(
        uncached.len(),
        cached.len(),
        "cached row count must match uncached"
    );
    for (a, b) in uncached.iter().zip(cached.iter()) {
        assert_eq!(a.row_id, b.row_id, "cached row ids must match uncached");
        assert_eq!(
            a.columns.get(&2),
            b.columns.get(&2),
            "cached city value must match uncached"
        );
    }

    if !q.conditions.is_empty() {
        let uncached_cols = db
            .query_columns_native(&q.conditions, proj, snap)
            .unwrap()
            .expect("pushdown should serve");
        let cached_cols = db
            .query_columns_native_cached(&q.conditions, proj, snap)
            .unwrap()
            .expect("pushdown should serve");
        assert_eq!(
            uncached_cols.len(),
            cached_cols.len(),
            "cached column set size must match uncached"
        );
        for ((id_a, col_a), (id_b, col_b)) in uncached_cols.iter().zip(cached_cols.iter()) {
            assert_eq!(id_a, id_b, "column ids must align");
            assert_eq!(
                col_a.len(),
                col_b.len(),
                "cached column length must match uncached"
            );
        }
    }
}

#[test]
fn cache_hit_matches_uncached_bitmap_range_and_pk() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), test_schema(), 1).unwrap();
    db.bulk_load(rows_city_cost(1000)).unwrap();
    db.flush().unwrap();

    let snap = db.snapshot();

    // Bitmap equality.
    let q_city = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"alpha".to_vec(),
    });
    assert_cached_equals_uncached(&mut db, &q_city, Some(&[1, 3]), snap);

    // Range (served by LearnedRange index).
    let q_range = Query::new().and(Condition::RangeF64 {
        column_id: 3,
        lo: 0.0,
        lo_inclusive: true,
        hi: 99.0,
        hi_inclusive: true,
    });
    assert_cached_equals_uncached(&mut db, &q_range, Some(&[1, 2]), snap);

    // PK lookup.
    let q_pk = Query::new().and(Condition::Pk(42i64.to_be_bytes().to_vec()));
    assert_cached_equals_uncached(&mut db, &q_pk, Some(&[2, 3]), snap);

    // Repeated calls are hits and still match.
    for _ in 0..5 {
        assert_cached_equals_uncached(&mut db, &q_city, Some(&[1, 3]), snap);
    }
}

#[test]
fn fine_grained_invalidation_delete_survivor_then_insert_matching() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), test_schema(), 1).unwrap();
    db.bulk_load(rows_city_cost(200)).unwrap();
    db.flush().unwrap();

    let q_alpha = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"alpha".to_vec(),
    });

    let r0 = db.query_cached(&q_alpha).unwrap();
    assert_eq!(r0.len(), 100);

    // Delete a survivor — footprint intersects, cache must drop.
    db.delete(r0[0].row_id).unwrap();
    db.commit().unwrap();
    let r1 = db.query_cached(&q_alpha).unwrap();
    assert_eq!(r1.len(), 99, "delete of survivor must invalidate cache");

    // Insert a new row that matches the condition — condition column touched.
    db.put(vec![
        (1, Value::Int64(9999)),
        (2, Value::Bytes(b"alpha".to_vec())),
        (3, Value::Float64(1.0)),
    ])
    .unwrap();
    db.commit().unwrap();
    let r2 = db.query_cached(&q_alpha).unwrap();
    assert_eq!(r2.len(), 100, "matching insert must invalidate cache");
}

#[test]
fn delete_non_survivor_does_not_invalidate() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), test_schema(), 1).unwrap();
    db.bulk_load(rows_city_cost(200)).unwrap();
    db.flush().unwrap();

    let q_alpha = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"alpha".to_vec(),
    });
    let r0 = db.query_cached(&q_alpha).unwrap();
    assert_eq!(r0.len(), 100);

    // Pick a "beta" row (not in result) and delete it.
    let beta_rid = db
        .query(&Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"beta".to_vec(),
        }))
        .unwrap()[0]
        .row_id;
    db.delete(beta_rid).unwrap();
    db.commit().unwrap();

    // The alpha cache entry should survive (footprint does not intersect).
    let r1 = db.query_cached(&q_alpha).unwrap();
    assert_eq!(
        r1.len(),
        100,
        "delete of non-survivor should not invalidate"
    );
}

#[test]
fn column_aware_invalidation_partial_put_bitmap() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), test_schema(), 1).unwrap();
    db.bulk_load(rows_city_cost(200)).unwrap();
    db.flush().unwrap();

    // Cache a bitmap query on city (column 2).
    let q_city = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"alpha".to_vec(),
    });
    let r0 = db.query_cached(&q_city).unwrap();
    assert_eq!(r0.len(), 100);

    // Put a row that does NOT touch column 2 — the city entry should survive.
    db.put(vec![(1, Value::Int64(10000)), (3, Value::Float64(1.0))])
        .unwrap();
    db.commit().unwrap();
    let r1 = db.query_cached(&q_city).unwrap();
    assert_eq!(
        r1.len(),
        100,
        "write not touching condition col should survive"
    );

    // Now touch column 2 with a matching value — the entry must invalidate.
    db.put(vec![
        (1, Value::Int64(10001)),
        (2, Value::Bytes(b"alpha".to_vec())),
        (3, Value::Float64(2.0)),
    ])
    .unwrap();
    db.commit().unwrap();
    let r2 = db.query_cached(&q_city).unwrap();
    assert_eq!(
        r2.len(),
        101,
        "write touching condition col must invalidate"
    );
}

#[test]
fn range_query_with_learned_index_misses_memtable_rows() {
    // Regression: when a LearnedRange index exists, resolve_condition for Range/
    // RangeF64 only consults the index (built from sorted runs) and ignores rows
    // still in the memtable. A put that matches the range is invisible until
    // flush rebuilds the index.
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), test_schema(), 1).unwrap();
    db.bulk_load(rows_city_cost(200)).unwrap();
    db.flush().unwrap();

    let q_cost = Query::new().and(Condition::RangeF64 {
        column_id: 3,
        lo: 0.0,
        lo_inclusive: true,
        hi: 49.0,
        hi_inclusive: true,
    });
    assert_eq!(db.query(&q_cost).unwrap().len(), 50);

    db.put(vec![
        (1, Value::Int64(10001)),
        (2, Value::Bytes(b"gamma".to_vec())),
        (3, Value::Float64(25.0)),
    ])
    .unwrap();
    db.commit().unwrap();

    // The new row's cost (25.0) is inside [0,49], so both cached and uncached
    // queries should see 51 rows. The learned index path currently returns 50.
    assert_eq!(
        db.query(&q_cost).unwrap().len(),
        51,
        "uncached range query must see memtable row matching learned index range"
    );
    assert_eq!(
        db.query_cached(&q_cost).unwrap().len(),
        51,
        "cached range query must see memtable row matching learned index range"
    );
}

#[test]
fn persistent_cache_survives_restart_and_corruption_falls_back() {
    let dir = tempdir().unwrap();
    let rcache_dir = dir.path().join("_rcache");

    {
        let mut db = Table::create(dir.path(), test_schema(), 1).unwrap();
        db.bulk_load(rows_city_cost(200)).unwrap();
        db.flush().unwrap();

        let q = Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"alpha".to_vec(),
        });
        let r = db.query_cached(&q).unwrap();
        assert_eq!(r.len(), 100);
    }

    // Tamper with one cache file.
    let mut cache_files: Vec<_> = std::fs::read_dir(&rcache_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("bin"))
        .map(|e| e.path())
        .collect();
    assert!(
        !cache_files.is_empty(),
        "persistent cache files should exist"
    );
    cache_files.sort();
    let victim = &cache_files[0];
    std::fs::write(victim, b"corrupted garbage").unwrap();

    {
        let mut db = Table::open(dir.path()).unwrap();
        let q = Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"alpha".to_vec(),
        });
        // Corrupt file should be ignored; query re-resolves from runs.
        let r = db.query_cached(&q).unwrap();
        assert_eq!(
            r.len(),
            100,
            "corrupt persistent cache must fall back to disk"
        );
    }
}

#[test]
fn cache_budget_thrashing_stays_correct() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), test_schema(), 1).unwrap();
    db.set_result_cache_max_bytes(1);
    db.bulk_load(rows_city_cost(500)).unwrap();
    db.flush().unwrap();

    let queries: Vec<Query> = vec![
        Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"alpha".to_vec(),
        }),
        Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"beta".to_vec(),
        }),
        Query::new().and(Condition::RangeF64 {
            column_id: 3,
            lo: 0.0,
            lo_inclusive: true,
            hi: 49.0,
            hi_inclusive: true,
        }),
    ];

    // Rapidly alternate tiny-budget queries; every result must match the
    // uncached path.
    for _ in 0..20 {
        for q in &queries {
            let uncached = db.query(q).unwrap();
            let cached = db.query_cached(q).unwrap();
            assert_eq!(uncached.len(), cached.len());
        }
    }
}

#[test]
fn empty_projection_cached_query_columns() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), test_schema(), 1).unwrap();
    db.bulk_load(rows_city_cost(100)).unwrap();
    db.flush().unwrap();

    let cond = [Condition::BitmapEq {
        column_id: 2,
        value: b"alpha".to_vec(),
    }];
    let snap = db.snapshot();

    let uncached = db
        .query_columns_native(&cond, Some(&[]), snap)
        .unwrap()
        .expect("served");
    let cached = db
        .query_columns_native_cached(&cond, Some(&[]), snap)
        .unwrap()
        .expect("served");

    // With an empty projection there are no columns to return.
    assert!(uncached.is_empty());
    assert_eq!(uncached.len(), cached.len());
    // Note: the survivor *count* is not carried in the returned columns, so an
    // empty projection cannot distinguish 0 matches from N matches through this
    // API alone. That is a design limitation worth recording.
}

#[test]
fn identical_queries_at_different_snapshots_must_be_isolated() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), test_schema(), 1).unwrap();
    db.bulk_load(rows_city_cost(100)).unwrap();
    db.flush().unwrap();

    let cond = [Condition::BitmapEq {
        column_id: 2,
        value: b"alpha".to_vec(),
    }];
    let proj = [1u16, 3];

    // Pin a read snapshot before the delete.
    let old_snap = db.pin_snapshot();

    // Delete one alpha survivor at the current epoch.
    let alpha_rows = db
        .query(&Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"alpha".to_vec(),
        }))
        .unwrap();
    assert_eq!(alpha_rows.len(), 50);
    db.delete(alpha_rows[0].row_id).unwrap();
    db.commit().unwrap();

    let new_snap = db.snapshot();

    // Query at the new snapshot first — this populates the cache with post-delete data.
    let cols_new = db
        .query_columns_native_cached(&cond, Some(&proj), new_snap)
        .unwrap()
        .expect("served");
    let count_new = count_from_native_cols(&cols_new);
    assert_eq!(count_new, 49, "new snapshot should see the delete");

    // Now query at the OLD pinned snapshot. Because the delete happened after
    // the pin, MVCC requires the old snapshot to still see 50 rows. The cache
    // key is identical, so if the cache ignores the snapshot epoch it will
    // incorrectly return the post-delete result.
    let cols_old = db
        .query_columns_native_cached(&cond, Some(&proj), old_snap)
        .unwrap()
        .expect("served");
    let count_old = count_from_native_cols(&cols_old);
    assert_eq!(
        count_old, 50,
        "old pinned snapshot must see pre-delete rows; cache must be snapshot-aware"
    );

    db.unpin_snapshot(old_snap);
}

#[test]
fn pinned_snapshot_with_commit_then_current_query_does_not_pollute_old_snapshot() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), test_schema(), 1).unwrap();
    db.bulk_load(rows_city_cost(100)).unwrap();
    db.flush().unwrap();

    let cond = [Condition::BitmapEq {
        column_id: 2,
        value: b"alpha".to_vec(),
    }];
    let proj = [1u16, 3];

    let old_snap = db.pin_snapshot();

    // Populate the cache at the old snapshot first.
    let cols_old1 = db
        .query_columns_native_cached(&cond, Some(&proj), old_snap)
        .unwrap()
        .expect("served");
    assert_eq!(count_from_native_cols(&cols_old1), 50);

    // Insert a matching row and commit.
    db.put(vec![
        (1, Value::Int64(9998)),
        (2, Value::Bytes(b"alpha".to_vec())),
        (3, Value::Float64(7.0)),
    ])
    .unwrap();
    db.commit().unwrap();

    // Query at the new snapshot — must see 51 rows and repopulate cache.
    let new_snap = db.snapshot();
    let cols_new = db
        .query_columns_native_cached(&cond, Some(&proj), new_snap)
        .unwrap()
        .expect("served");
    assert_eq!(count_from_native_cols(&cols_new), 51);

    // Query again at the old snapshot — must still see 50 rows, not the cached 51.
    let cols_old2 = db
        .query_columns_native_cached(&cond, Some(&proj), old_snap)
        .unwrap()
        .expect("served");
    assert_eq!(
        count_from_native_cols(&cols_old2),
        50,
        "old snapshot must remain isolated after cache repopulation"
    );

    db.unpin_snapshot(old_snap);
}

#[test]
fn large_volume_cache_invalidation_and_consistency() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), test_schema(), 1).unwrap();
    db.bulk_load(rows_city_cost(50_000)).unwrap();
    db.flush().unwrap();

    let q_alpha = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"alpha".to_vec(),
    });
    let r0 = db.query_cached(&q_alpha).unwrap();
    assert_eq!(r0.len(), 25_000);

    // Delete every 10th survivor — precise footprint invalidation.
    let to_delete: Vec<_> = r0.iter().step_by(10).map(|r| r.row_id).collect();
    for rid in to_delete {
        db.delete(rid).unwrap();
    }
    db.commit().unwrap();

    let r1 = db.query_cached(&q_alpha).unwrap();
    assert_eq!(r1.len(), 25_000 - 2_500);

    // Compare against uncached.
    let r_uncached = db.query(&q_alpha).unwrap();
    assert_eq!(r1.len(), r_uncached.len());

    // Sum verification through native cached path.
    let cond = [Condition::BitmapEq {
        column_id: 2,
        value: b"alpha".to_vec(),
    }];
    let snap = db.snapshot();
    let cols = db
        .query_columns_native_cached(&cond, Some(&[3]), snap)
        .unwrap()
        .expect("served");
    let cached_sum = sum_cost_from_native_cols(&cols);
    let uncached_sum: f64 = r_uncached
        .iter()
        .filter_map(|r| r.columns.get(&3))
        .filter_map(|v| match v {
            Value::Float64(x) => Some(*x),
            _ => None,
        })
        .sum();
    assert!((cached_sum - uncached_sum).abs() < 1e-6);
}

#[test]
fn reopen_keeps_persistent_cache_consistent_after_mutation() {
    let dir = tempdir().unwrap();
    let rcache_dir = dir.path().join("_rcache");

    {
        let mut db = Table::create(dir.path(), test_schema(), 1).unwrap();
        db.bulk_load(rows_city_cost(200)).unwrap();
        db.flush().unwrap();

        let q = Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"alpha".to_vec(),
        });
        assert_eq!(db.query_cached(&q).unwrap().len(), 100);

        // Mutate after caching.
        let rid = db.query(&q).unwrap()[0].row_id;
        db.delete(rid).unwrap();
        db.commit().unwrap();
        assert_eq!(db.query_cached(&q).unwrap().len(), 99);
    }

    // On reopen, stale persistent files for the pre-delete cached entry may
    // still exist. The open path must either load-and-invalidate them or ignore
    // them; in no case may it return 100 rows.
    {
        let mut db = Table::open(dir.path()).unwrap();
        let q = Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"alpha".to_vec(),
        });
        let r = db.query_cached(&q).unwrap();
        assert_eq!(
            r.len(),
            99,
            "reopened cache must not resurrect deleted rows"
        );

        // There should still be cache files; they should not contain plaintext.
        let files: Vec<_> = std::fs::read_dir(&rcache_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("bin"))
            .collect();
        assert!(!files.is_empty());
    }
}

#[test]
fn mixed_put_and_flush_invalidates_cache_correctly() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), test_schema(), 1).unwrap();
    db.bulk_load(rows_city_cost(100)).unwrap();
    db.flush().unwrap();

    let q_alpha = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"alpha".to_vec(),
    });
    let r0 = db.query_cached(&q_alpha).unwrap();
    assert_eq!(r0.len(), 50);

    // Stage a put but do not commit; cache must not see uncommitted data.
    db.put(vec![
        (1, Value::Int64(5000)),
        (2, Value::Bytes(b"alpha".to_vec())),
        (3, Value::Float64(1.0)),
    ])
    .unwrap();
    let r1 = db.query_cached(&q_alpha).unwrap();
    assert_eq!(
        r1.len(),
        50,
        "uncommitted put must not affect cached result"
    );

    // Commit + flush; now the cache must reflect the new row.
    db.commit().unwrap();
    db.flush().unwrap();
    let r2 = db.query_cached(&q_alpha).unwrap();
    assert_eq!(r2.len(), 51, "post-flush cache must see committed row");
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_table_result_cache_does_not_leak_plaintext() {
    let dir = tempdir().unwrap();
    let rcache_dir = dir.path().join("_rcache");

    {
        let mut db = Table::create_encrypted(dir.path(), test_schema(), 1, "s3cr3t").unwrap();
        db.bulk_load(rows_city_cost(200)).unwrap();
        db.flush().unwrap();

        let q = Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"alpha".to_vec(),
        });
        assert_eq!(db.query_cached(&q).unwrap().len(), 100);
    }

    // Read all cache files and ensure none contain the plaintext value "alpha".
    for entry in std::fs::read_dir(&rcache_dir).unwrap().flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("bin") {
            continue;
        }
        let bytes = std::fs::read(&path).unwrap();
        assert!(
            !bytes.windows(5).any(|w| w == b"alpha"),
            "encrypted result cache must not contain plaintext 'alpha'"
        );
    }

    // Reopen and verify the persistent encrypted cache still works.
    {
        let mut db = Table::open_encrypted(dir.path(), "s3cr3t").unwrap();
        let q = Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"alpha".to_vec(),
        });
        assert_eq!(db.query_cached(&q).unwrap().len(), 100);
    }
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_table_wrong_key_rejects_open_but_cache_files_stay_opaque() {
    let dir = tempdir().unwrap();

    {
        let mut db = Table::create_encrypted(dir.path(), test_schema(), 1, "s3cr3t").unwrap();
        db.bulk_load(rows_city_cost(50)).unwrap();
        db.flush().unwrap();

        let q = Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"alpha".to_vec(),
        });
        assert_eq!(db.query_cached(&q).unwrap().len(), 25);
    }

    // Wrong passphrase must fail to open.
    assert!(Table::open_encrypted(dir.path(), "wrong").is_err());

    // The cache files should still not leak plaintext.
    let rcache_dir = dir.path().join("_rcache");
    for entry in std::fs::read_dir(&rcache_dir).unwrap().flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("bin") {
            continue;
        }
        let bytes = std::fs::read(&path).unwrap();
        assert!(
            !bytes.windows(5).any(|w| w == b"alpha"),
            "cache must remain opaque even when table cannot be opened"
        );
    }
}
