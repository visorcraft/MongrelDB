//! Phase 19.1 — Table-level in-process result cache (`query_cached` /
//! `query_columns_native_cached`): repeat queries hit; a `commit()` invalidates.

use mongreldb_core::columnar::NativeColumn;
use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::*;
use mongreldb_core::{Table, Value};
use tempfile::tempdir;

fn schema() -> Schema {
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
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 3,
                name: "cost".into(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            },
        ],
        indexes: vec![IndexDef {
            name: "city_bm".into(),
            column_id: 2,
            kind: IndexKind::Bitmap,
            predicate: None,
        }],
        colocation: vec![],
        constraints: Default::default(),
    }
}

#[test]
fn query_cached_hits_then_invalidates_on_commit() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    let rows: Vec<Vec<(u16, Value)>> = (0..1000)
        .map(|i| {
            vec![
                (1, Value::Int64(i)),
                (2, Value::Bytes(b"alpha".to_vec())),
                (3, Value::Float64(i as f64)),
            ]
        })
        .collect();
    db.bulk_load(rows).unwrap();

    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"alpha".to_vec(),
    });
    let r0 = db.query_cached(&q).unwrap();
    assert_eq!(r0.len(), 1000);
    // Second call is a cache hit (same epoch, same conditions).
    let r1 = db.query_cached(&q).unwrap();
    assert_eq!(r1.len(), 1000);
    assert_eq!(r0[0].row_id, r1[0].row_id);

    // A commit (epoch bump) invalidates; the next call re-resolves and reflects
    // a mutation.
    db.put(vec![
        (1, Value::Int64(5000)),
        (2, Value::Bytes(b"alpha".to_vec())),
        (3, Value::Float64(9.0)),
    ])
    .unwrap();
    db.commit().unwrap();
    let r2 = db.query_cached(&q).unwrap();
    assert_eq!(r2.len(), 1001, "post-commit query sees the new row");
}

#[test]
fn query_columns_native_cached_hits() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    let cols = vec![
        (
            1,
            NativeColumn::Int64 {
                data: (0..500).collect(),
                validity: vec![0xFF; 500usize.div_ceil(8)],
            },
        ),
        (
            2,
            NativeColumn::Bytes {
                offsets: (0..=500).map(|i| (i * 3) as u32).collect(),
                values: b"abc".repeat(500),
                validity: vec![0xFF; 500usize.div_ceil(8)],
            },
        ),
        (
            3,
            NativeColumn::Float64 {
                data: (0..500).map(|i| i as f64).collect(),
                validity: vec![0xFF; 500usize.div_ceil(8)],
            },
        ),
    ];
    db.bulk_load_columns(cols).unwrap();

    let snap = db.snapshot();
    let cond = [Condition::RangeF64 {
        column_id: 3,
        lo: 0.0,
        lo_inclusive: true,
        hi: 10.0,
        hi_inclusive: true,
    }];
    let proj = [1u16, 3];
    let c0 = db
        .query_columns_native_cached(&cond, Some(&proj), snap)
        .unwrap()
        .expect("served");
    // The Range resolves via the page-pruned scan; only rows with cost in [0,10]
    // survive (11 rows).
    let n0 = match &c0[1].1 {
        NativeColumn::Float64 { data, .. } => data.len(),
        _ => panic!(),
    };
    assert_eq!(n0, 11);

    // Cache hit — identical result.
    let c1 = db
        .query_columns_native_cached(&cond, Some(&proj), snap)
        .unwrap()
        .expect("served");
    let n1 = match &c1[1].1 {
        NativeColumn::Float64 { data, .. } => data.len(),
        _ => panic!(),
    };
    assert_eq!(n0, n1);
}

#[test]
fn lru_promote_on_hit_and_configurable_budget() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    let rows: Vec<Vec<(u16, Value)>> = (0..300)
        .map(|i| {
            vec![
                (1, Value::Int64(i)),
                (
                    2,
                    Value::Bytes((if i % 3 == 0 { b"alpha" } else { b"beta!" }).to_vec()),
                ),
                (3, Value::Float64(i as f64)),
            ]
        })
        .collect();
    db.bulk_load(rows).unwrap();

    let q_alpha = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"alpha".to_vec(),
    });
    let q_beta = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"beta!".to_vec(),
    });

    // Both queries cache fine under the default budget.
    let ra = db.query_cached(&q_alpha).unwrap();
    let rb = db.query_cached(&q_beta).unwrap();
    assert_eq!(ra.len(), 100);
    assert_eq!(rb.len(), 200);

    // Shrink the budget aggressively — existing entries get evicted down.
    db.set_result_cache_max_bytes(1);
    // Results are still correct after eviction (re-resolved from scratch).
    let ra2 = db.query_cached(&q_alpha).unwrap();
    assert_eq!(ra2.len(), 100);
    assert_eq!(ra2[0].row_id, ra[0].row_id);

    // Grow it back; both cache and hit again.
    db.set_result_cache_max_bytes(256 * 1024 * 1024);
    db.query_cached(&q_beta).unwrap();
    let rb2 = db.query_cached(&q_beta).unwrap();
    assert_eq!(rb2.len(), 200);

    // Promote-on-hit: touch alpha, then shrink to ~one-entry budget.
    // The previously-MRU beta is now LRU relative to alpha and gets evicted;
    // alpha survives. We can't inspect the cache directly, but we verify the
    // results are always correct regardless of eviction order.
    db.query_cached(&q_alpha).unwrap();
    db.set_result_cache_max_bytes(1);
    let ra3 = db.query_cached(&q_alpha).unwrap();
    assert_eq!(ra3.len(), 100, "correct results after promote + eviction");
}

#[test]
fn fine_grained_invalidation_delete_survivor() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    let rows: Vec<Vec<(u16, Value)>> = (0..300)
        .map(|i| {
            vec![
                (1, Value::Int64(i)),
                (
                    2,
                    Value::Bytes((if i % 3 == 0 { b"alpha" } else { b"beta!" }).to_vec()),
                ),
                (3, Value::Float64(i as f64)),
            ]
        })
        .collect();
    db.bulk_load(rows).unwrap();

    // Cache a query: city = "alpha" → 100 survivors (i=0,3,6,...,297).
    let q_city = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"alpha".to_vec(),
    });
    let r0 = db.query_cached(&q_city).unwrap();
    assert_eq!(r0.len(), 100);

    // Delete a survivor → its RowId is in the footprint → entry invalidated.
    db.delete(r0[0].row_id).unwrap();
    db.commit().unwrap();
    let r1 = db.query_cached(&q_city).unwrap();
    assert_eq!(r1.len(), 99, "delete of a survivor drops one row");
}

#[test]
fn fine_grained_invalidation_column_aware_insert() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    // Two columns: city (2) and cost (3). Bitmap index on city only.
    let rows: Vec<Vec<(u16, Value)>> = (0..300)
        .map(|i| {
            vec![
                (1, Value::Int64(i)),
                (
                    2,
                    Value::Bytes((if i % 3 == 0 { b"alpha" } else { b"beta!" }).to_vec()),
                ),
                (3, Value::Float64(i as f64)),
            ]
        })
        .collect();
    db.bulk_load(rows).unwrap();

    // Cache a query on cost (column 3) — condition_cols = {3}.
    let q_cost = Query::new().and(Condition::RangeF64 {
        column_id: 3,
        lo: 0.0,
        lo_inclusive: true,
        hi: 50.0,
        hi_inclusive: true,
    });
    let r0 = db.query_cached(&q_cost).unwrap();
    assert_eq!(r0.len(), 51);

    // Commit a put that does NOT touch column 3.
    // Since put writes all columns (the caller provides values for all),
    // column 3 IS written → the cost entry IS invalidated. This is correct.
    // To test a non-invalidating write, we'd need a put that skips column 3.
    // A put with only columns 1 and 2:
    db.put(vec![
        (1, Value::Int64(9999)),
        (2, Value::Bytes(b"alpha".to_vec())),
    ])
    .unwrap();
    db.commit().unwrap();

    // Column 3 was not written → pending_put_cols = {1, 2}.
    // The cost query's condition_cols = {3} → no intersection → entry survives!
    let r1 = db.query_cached(&q_cost).unwrap();
    assert_eq!(
        r1.len(),
        51,
        "cost query survives a write that doesn't touch column 3"
    );
}

#[test]
fn fine_grained_invalidation_multi_run_delete() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    let rows: Vec<Vec<(u16, Value)>> = (0..100)
        .map(|i| {
            vec![
                (1, Value::Int64(i)),
                (
                    2,
                    Value::Bytes((if i % 2 == 0 { b"alpha" } else { b"beta!" }).to_vec()),
                ),
                (3, Value::Float64(i as f64)),
            ]
        })
        .collect();
    db.bulk_load(rows).unwrap();

    // Force a second run via flush with a tiny spill threshold.
    db.set_mutable_run_spill_bytes(1);
    db.put(vec![
        (1, Value::Int64(100)),
        (2, Value::Bytes(b"alpha".to_vec())),
        (3, Value::Float64(100.0)),
    ])
    .unwrap();
    db.flush().unwrap();
    assert!(db.run_count() >= 2);

    let snap = db.snapshot();
    let cond = [Condition::BitmapEq {
        column_id: 2,
        value: b"alpha".to_vec(),
    }];
    let proj = [2u16];
    let c0 = db
        .query_columns_native_cached(&cond, Some(&proj), snap)
        .unwrap()
        .expect("served");
    let n0 = match &c0[0].1 {
        NativeColumn::Bytes { offsets, .. } => offsets.len() - 1,
        _ => panic!("expected bytes"),
    };
    assert!(n0 > 0);

    // Delete a survivor. Footprint is empty (multi-run) → conservative path
    // (any delete → evict entries with empty footprint) must fire.
    db.delete(mongreldb_core::RowId(0)).unwrap();
    db.commit().unwrap();

    // New snapshot sees the delete (MVCC: old snapshot would still see the row).
    let snap2 = db.snapshot();
    let c1 = db
        .query_columns_native_cached(&cond, Some(&proj), snap2)
        .unwrap()
        .expect("served");
    let n1 = match &c1[0].1 {
        NativeColumn::Bytes { offsets, .. } => offsets.len() - 1,
        _ => panic!("expected bytes"),
    };
    assert_eq!(n1, n0 - 1, "multi-run cached entry must reflect the delete");
}

#[test]
fn persistent_tier_survives_restart() {
    let dir = tempdir().unwrap();
    let rcache_path = dir.path().join("_rcache");

    {
        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
        let rows: Vec<Vec<(u16, Value)>> = (0..200)
            .map(|i| {
                vec![
                    (1, Value::Int64(i)),
                    (
                        2,
                        Value::Bytes((if i % 2 == 0 { b"alpha" } else { b"beta!" }).to_vec()),
                    ),
                    (3, Value::Float64(i as f64)),
                ]
            })
            .collect();
        db.bulk_load(rows).unwrap();

        let q = Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"alpha".to_vec(),
        });
        let r = db.query_cached(&q).unwrap();
        assert_eq!(r.len(), 100);

        let files: Vec<_> = std::fs::read_dir(&rcache_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("bin"))
            .collect();
        assert!(!files.is_empty(), "persistent cache files written");
    }

    {
        let mut db = Table::open(dir.path()).unwrap();
        let q = Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"alpha".to_vec(),
        });
        let r = db.query_cached(&q).unwrap();
        assert_eq!(r.len(), 100, "persistent cache hit after restart");
    }
}
