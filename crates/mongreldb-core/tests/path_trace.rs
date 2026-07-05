//! Query path instrumentation tests (OPTIMIZATIONS.md Priority 0 / 16).
//!
//! Verifies that [`mongreldb_core::trace::QueryTrace`] correctly records the
//! physical path each query takes: the fast index-pushdown gather, the lazy
//! page cursor, the multi-run k-way merge, the count-survivor shortcut, the
//! materialized fallback, the result-cache hit, and the index-rebuild stall
//! detector. These are path-sensitive correctness tests — they assert *which*
//! path ran, not just *that* results are correct.

use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::trace::{IndexRebuild, ScanMode};
use mongreldb_core::{Condition, IndexBuildPolicy, Query, Table, Value};
use tempfile::{tempdir, TempDir};

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
                name: "cat".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 3,
                name: "cost".into(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![IndexDef {
            name: "cat_bitmap".into(),
            column_id: 2,
            kind: IndexKind::Bitmap,
            predicate: None,
        }],
        colocation: vec![],
        constraints: Default::default(),
    }
}

fn rows(n: u64) -> Vec<Vec<(u16, Value)>> {
    (0..n)
        .map(|i| {
            vec![
                (1, Value::Int64(i as i64)),
                (2, Value::Bytes(format!("cat{}", i % 20).into_bytes())),
                (3, Value::Float64(199.99 + i as f64)),
            ]
        })
        .collect()
}

/// Steady-state fixture: these tests assert *which query path* runs once the
/// indexes are live, so build them eagerly (the deferred default is covered by
/// `trace_index_rebuild_after_bulk_load`).
fn build_db(n: u64) -> (TempDir, Table) {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.set_index_build_policy(IndexBuildPolicy::Eager);
    db.bulk_load(rows(n)).unwrap();
    (dir, db)
}

#[test]
fn trace_bitmap_pushdown_uses_fast_path() {
    let (_dir, mut db) = build_db(10_000);
    let snap = db.snapshot();
    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"cat5".to_vec(),
    });
    let proj = [1u16, 3];
    let (cols, trace) = db
        .query_columns_native_traced(&q.conditions, Some(&proj), snap)
        .unwrap();
    let cols = cols.expect("bitmap pushdown should be served");
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].1.len(), 500); // 10000 / 20
    trace
        .assert_mode(ScanMode::NativePushdown)
        .assert_no_index_rebuild()
        .assert_fast_row_id_map()
        .assert_not_materialized();
    assert_eq!(trace.conditions_pushed, 1);
    assert_eq!(trace.survivor_count, Some(500));
    assert_eq!(trace.run_count, 1);
    assert!(!trace.result_cache_hit);
}

#[test]
fn trace_count_conditions_uses_survivor_shortcut() {
    let (_dir, mut db) = build_db(10_000);
    let snap = db.snapshot();
    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"cat5".to_vec(),
    });
    let (count, trace) = db.count_conditions_traced(&q.conditions, snap).unwrap();
    assert_eq!(count, Some(500));
    trace
        .assert_mode(ScanMode::CountSurvivors)
        .assert_no_index_rebuild()
        .assert_not_materialized();
    assert_eq!(trace.survivor_count, Some(500));
}

#[test]
fn trace_native_page_cursor_records_cursor_mode() {
    let (_dir, db) = build_db(10_000);
    let snap = db.snapshot();
    let q: Vec<Condition> = vec![Condition::BitmapEq {
        column_id: 2,
        value: b"cat5".to_vec(),
    }];
    let (cursor_opt, trace) = db
        .native_page_cursor_traced(snap, vec![(1, TypeId::Int64), (3, TypeId::Float64)], &q)
        .unwrap();
    let mut cursor = cursor_opt.expect("single-run cursor should build");
    let batch = cursor.next_batch().unwrap().unwrap();
    assert_eq!(batch.len(), 2);
    trace
        .assert_mode(ScanMode::NativePageCursor)
        .assert_no_index_rebuild();
    assert_eq!(trace.run_count, 1);
    assert_eq!(trace.conditions_pushed, 1);
}

#[test]
fn trace_result_cache_hit_on_second_query() {
    let (_dir, mut db) = build_db(10_000);
    let snap = db.snapshot();
    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"cat5".to_vec(),
    });
    let proj = [1u16];
    // First call — miss, populates cache.
    let (_, trace_miss) = db
        .query_columns_native_cached_traced(&q.conditions, Some(&proj), snap)
        .unwrap();
    assert!(!trace_miss.result_cache_hit);
    // Second call — hit.
    let (_, trace_hit) = db
        .query_columns_native_cached_traced(&q.conditions, Some(&proj), snap)
        .unwrap();
    trace_hit
        .assert_cache_hit()
        .assert_mode(ScanMode::NativePushdown);
}

#[test]
fn trace_overlay_uses_cursor_not_materialization() {
    let (_dir, mut db) = build_db(10_000);
    // Put a row into the memtable (non-empty overlay) → fast gather unavailable,
    // but the cursor path (native_page_cursor) handles the overlay columnar.
    db.put(vec![
        (1, Value::Int64(99_999)),
        (2, Value::Bytes(b"cat5".to_vec())),
        (3, Value::Float64(42.0)),
    ])
    .unwrap();
    let snap = db.snapshot();
    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"cat5".to_vec(),
    });
    let proj = [1u16, 3];
    let (_, trace) = db
        .query_columns_native_traced(&q.conditions, Some(&proj), snap)
        .unwrap();
    // The non-fast-path now routes through the columnar cursor instead of
    // row materialization (Priority 1 + 4 fix): single-run-with-overlay →
    // NativePageCursor, not Materialized.
    assert_eq!(
        trace.scan_mode,
        ScanMode::NativePageCursor,
        "overlay should use cursor path, not materialization: {trace}"
    );
    assert!(
        !trace.row_materialized,
        "overlay should not materialize rows: {trace}"
    );
    assert!(trace.memtable_rows > 0);
}

#[test]
fn trace_index_rebuild_after_bulk_load() {
    // A fresh table with deferred indexes: the first traced query that needs
    // indexes triggers `ensure_indexes_complete` → IndexRebuild::Rebuilt.
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.bulk_load(rows(5_000)).unwrap();
    let snap = db.snapshot();
    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"cat5".to_vec(),
    });
    let (_, trace) = db
        .query_columns_native_traced(&q.conditions, Some(&[1u16]), snap)
        .unwrap();
    // Default `IndexBuildPolicy::Deferred`: the bulk load leaves indexes
    // incomplete, so the first query pays the one-time lazy rebuild…
    assert_eq!(
        trace.index_rebuild,
        IndexRebuild::Rebuilt,
        "first query after a deferred bulk load must rebuild indexes: {trace}"
    );
    // …and the second query is served from the now-complete indexes.
    let (_, trace) = db
        .query_columns_native_traced(&q.conditions, Some(&[1u16]), snap)
        .unwrap();
    assert_eq!(
        trace.index_rebuild,
        IndexRebuild::AlreadyComplete,
        "second query must not rebuild again: {trace}"
    );
}

#[test]
fn trace_query_method_records_materialization() {
    let (_dir, mut db) = build_db(1_000);
    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"cat5".to_vec(),
    });
    let (rows, trace) = db.query_traced(&q).unwrap();
    assert!(!rows.is_empty());
    // query() always returns Vec<Row> (HashMap-backed) — this IS the slow path
    // that optimizations try to avoid. The trace should flag it as materialized.
    trace.assert_mode(ScanMode::Materialized);
    assert!(
        trace.row_materialized,
        "query() should set row_materialized: {trace}"
    );
}

#[test]
fn trace_multi_run_cursor_records_run_count() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1); // each flush spills a fresh run
                                       // Run 1.
    db.bulk_load(rows(2_000)).unwrap();
    db.flush().unwrap();
    // Run 2: a few more rows force a second sorted run.
    for i in 0..10i64 {
        db.put(vec![
            (1, Value::Int64(100_000 + i)),
            (2, Value::Bytes(b"cat5".to_vec())),
            (3, Value::Float64(1.0)),
        ])
        .unwrap();
    }
    db.flush().unwrap();
    let snap = db.snapshot();
    assert!(
        db.run_count() >= 2,
        "expected multi-run layout, got {} runs",
        db.run_count()
    );
    let q: Vec<Condition> = vec![Condition::BitmapEq {
        column_id: 2,
        value: b"cat5".to_vec(),
    }];
    let (cursor_opt, trace) = db
        .native_multi_run_cursor_traced(snap, vec![(1, TypeId::Int64), (3, TypeId::Float64)], &q)
        .unwrap();
    let _cursor = cursor_opt.expect("multi-run cursor should build");
    trace.assert_mode(ScanMode::MultiRunCursor);
    assert!(
        trace.run_count >= 2,
        "multi-run should record >= 2 runs: {trace}"
    );
}

#[test]
fn trace_is_fast_for_good_paths() {
    let (_dir, mut db) = build_db(10_000);
    let snap = db.snapshot();
    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"cat5".to_vec(),
    });
    let (_, trace) = db
        .query_columns_native_traced(&q.conditions, Some(&[1u16]), snap)
        .unwrap();
    assert!(trace.is_fast(), "pushdown path should be fast: {trace}");
}
