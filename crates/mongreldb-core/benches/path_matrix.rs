//! Path-aware query benchmark matrix (OPTIMIZATIONS.md Priority 0 / 16).
//!
//! Complements `filtered_query` by exercising **multiple table layouts** and
//! printing the [`QueryTrace`] path each case takes, so timing regressions can
//! be attributed to a path change (not just wall-clock noise).
//!
//! Layouts: fresh single-run (clean bulk load), dirty single-run (non-empty
//! memtable overlay), multi-run (2+ sorted runs). Predicates: none, PK point
//! lookup, bitmap equality, int range, bitmap∩range, BitmapIn, COUNT survivors.
//!
//! Run: `cargo bench -p mongreldb-core --bench path_matrix`

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use mongreldb_core::query::Query;
use mongreldb_core::schema::*;
use mongreldb_core::trace::ScanMode;
use mongreldb_core::{Condition as C, Table, Value};
use std::time::Duration;
use tempfile::tempdir;

const N: u64 = 1_000_000;
const CATS: u64 = 20;

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
            ColumnDef {
                id: 4,
                name: "ts".into(),
                ty: TypeId::Int64,
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
                (2, Value::Bytes(format!("cat{}", i % CATS).into_bytes())),
                (3, Value::Float64(199.99 + i as f64)),
                (4, Value::Int64(1_700_000_000 + i as i64)),
            ]
        })
        .collect()
}

/// Print the trace once so the benchmark output shows which path each case takes.
fn show_trace(label: &str, trace: &mongreldb_core::trace::QueryTrace) {
    println!("[path_matrix] {label:32} {trace}");
}

/// Fresh clean single-run table (bulk loaded, indexes eager).
struct FreshLayout {
    _dir: tempfile::TempDir,
    db: Table,
    snap: mongreldb_core::Snapshot,
}

impl FreshLayout {
    fn build() -> Self {
        let dir = tempdir().unwrap();
        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
        db.bulk_load(rows(N)).unwrap();
        let snap = db.snapshot();
        Self {
            _dir: dir,
            db,
            snap,
        }
    }
}

/// Dirty single-run: clean bulk load + an unflushed memtable overlay.
struct DirtyLayout {
    _dir: tempfile::TempDir,
    db: Table,
    snap: mongreldb_core::Snapshot,
}

impl DirtyLayout {
    fn build() -> Self {
        let dir = tempdir().unwrap();
        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
        db.bulk_load(rows(N)).unwrap();
        // Inject a handful of unflushed updates to make the memtable non-empty.
        for i in 0..50i64 {
            db.put(vec![
                (1, Value::Int64(i)),
                (2, Value::Bytes(b"cat5".to_vec())),
                (3, Value::Float64(999.0)),
                (4, Value::Int64(1_700_000_000)),
            ])
            .unwrap();
        }
        let snap = db.snapshot();
        Self {
            _dir: dir,
            db,
            snap,
        }
    }
}

/// Multi-run: two sorted runs via spill-each-flush.
struct MultiRunLayout {
    _dir: tempfile::TempDir,
    db: Table,
    snap: mongreldb_core::Snapshot,
}

impl MultiRunLayout {
    fn build() -> Self {
        let dir = tempdir().unwrap();
        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
        db.set_mutable_run_spill_bytes(1);
        db.bulk_load(rows(N)).unwrap();
        db.flush().unwrap();
        // Second run: a small batch forces a fresh sorted run.
        for i in 0..100i64 {
            db.put(vec![
                (1, Value::Int64(N as i64 + i)),
                (2, Value::Bytes(b"cat5".to_vec())),
                (3, Value::Float64(1.0)),
                (4, Value::Int64(1_700_000_000 + N as i64 + i)),
            ])
            .unwrap();
        }
        db.flush().unwrap();
        let snap = db.snapshot();
        assert!(db.run_count() >= 2);
        Self {
            _dir: dir,
            db,
            snap,
        }
    }
}

fn bench_path_matrix(c: &mut Criterion) {
    let mut g = c.benchmark_group("path_matrix");
    g.measurement_time(Duration::from_secs(4));
    g.sample_size(10);
    g.throughput(Throughput::Elements(N));

    let proj: [u16; 2] = [1, 3];

    // -----------------------------------------------------------------------
    // Fresh single-run layout.
    // -----------------------------------------------------------------------
    {
        let mut fresh = FreshLayout::build();
        let snap = fresh.snap;

        // PK point lookup: single survivor.
        let pk_q = Query::pk(Value::Int64(500_000).encode_key());
        let (_, t) = fresh
            .db
            .query_columns_native_traced(&pk_q.conditions, Some(&proj), snap)
            .unwrap();
        show_trace("fresh/pk_lookup", &t);

        g.bench_function("fresh/pk_lookup", |b| {
            b.iter(|| {
                let q = Query::pk(Value::Int64(500_000).encode_key());
                let cols = fresh
                    .db
                    .query_columns_native(&q.conditions, Some(&proj), snap)
                    .unwrap()
                    .unwrap();
                black_box(cols);
            })
        });

        // Bitmap equality: N/CATS survivors.
        let bm_q = Query::new().and(C::BitmapEq {
            column_id: 2,
            value: b"cat5".to_vec(),
        });
        let (_, t) = fresh
            .db
            .query_columns_native_traced(&bm_q.conditions, Some(&proj), snap)
            .unwrap();
        show_trace("fresh/bitmap_eq", &t);

        g.bench_function("fresh/bitmap_eq", |b| {
            b.iter(|| {
                let cols = fresh
                    .db
                    .query_columns_native(&bm_q.conditions, Some(&proj), snap)
                    .unwrap()
                    .unwrap();
                black_box(cols);
            })
        });

        // Int range: 5% window.
        let lo = 1_700_000_000i64 + (N as i64) / 20;
        let hi = 1_700_000_000i64 + (N as i64) / 10;
        let rg_q = Query::new().and(C::Range {
            column_id: 4,
            lo,
            hi,
        });
        let (_, t) = fresh
            .db
            .query_columns_native_traced(&rg_q.conditions, Some(&proj), snap)
            .unwrap();
        show_trace("fresh/range_int", &t);

        g.bench_function("fresh/range_int", |b| {
            b.iter(|| {
                let cols = fresh
                    .db
                    .query_columns_native(&rg_q.conditions, Some(&proj), snap)
                    .unwrap()
                    .unwrap();
                black_box(cols);
            })
        });

        // Bitmap ∩ range.
        let both_q = Query::new()
            .and(C::BitmapEq {
                column_id: 2,
                value: b"cat5".to_vec(),
            })
            .and(C::Range {
                column_id: 4,
                lo: 1_700_000_000i64,
                hi: 1_700_000_000i64 + (N as i64) / 2,
            });
        let (_, t) = fresh
            .db
            .query_columns_native_traced(&both_q.conditions, Some(&proj), snap)
            .unwrap();
        show_trace("fresh/bitmap_intersect_range", &t);

        g.bench_function("fresh/bitmap_intersect_range", |b| {
            b.iter(|| {
                let cols = fresh
                    .db
                    .query_columns_native(&both_q.conditions, Some(&proj), snap)
                    .unwrap()
                    .unwrap();
                black_box(cols);
            })
        });

        // BitmapIn (IN list of 5 values).
        let in_q = Query::new().and(C::BitmapIn {
            column_id: 2,
            values: (0..5).map(|i| format!("cat{i}").into_bytes()).collect(),
        });
        let (_, t) = fresh
            .db
            .query_columns_native_traced(&in_q.conditions, Some(&proj), snap)
            .unwrap();
        show_trace("fresh/bitmap_in", &t);

        g.bench_function("fresh/bitmap_in", |b| {
            b.iter(|| {
                let cols = fresh
                    .db
                    .query_columns_native(&in_q.conditions, Some(&proj), snap)
                    .unwrap()
                    .unwrap();
                black_box(cols);
            })
        });

        // COUNT survivors (no column decode).
        let (_, t) = fresh
            .db
            .count_conditions_traced(&bm_q.conditions, snap)
            .unwrap();
        show_trace("fresh/count_survivors", &t);

        g.bench_function("fresh/count_survivors", |b| {
            b.iter(|| {
                let n = fresh
                    .db
                    .count_conditions(&bm_q.conditions, snap)
                    .unwrap()
                    .unwrap();
                black_box(n);
            })
        });
    }

    // -----------------------------------------------------------------------
    // Dirty single-run (non-empty memtable overlay).
    // -----------------------------------------------------------------------
    {
        let mut dirty = DirtyLayout::build();
        let snap = dirty.snap;
        let bm_q = Query::new().and(C::BitmapEq {
            column_id: 2,
            value: b"cat5".to_vec(),
        });
        let (_, t) = dirty
            .db
            .query_columns_native_traced(&bm_q.conditions, Some(&proj), snap)
            .unwrap();
        show_trace("dirty/bitmap_eq", &t);

        g.bench_function("dirty/bitmap_eq", |b| {
            b.iter(|| {
                let cols = dirty
                    .db
                    .query_columns_native(&bm_q.conditions, Some(&proj), snap)
                    .unwrap()
                    .unwrap();
                black_box(cols);
            })
        });
    }

    // -----------------------------------------------------------------------
    // Multi-run layout.
    // -----------------------------------------------------------------------
    {
        let mut multi = MultiRunLayout::build();
        let snap = multi.snap;
        let bm_q = Query::new().and(C::BitmapEq {
            column_id: 2,
            value: b"cat5".to_vec(),
        });
        let proj_pairs = vec![(1, TypeId::Int64), (3, TypeId::Float64)];

        // Trace the multi-run cursor path — the streaming fast path for
        // multi-run tables.
        let (_, t) = multi
            .db
            .native_multi_run_cursor_traced(snap, proj_pairs.clone(), &bm_q.conditions)
            .unwrap();
        show_trace("multi/bitmap_eq_cursor", &t);

        g.bench_function("multi/bitmap_eq_cursor", |b| {
            b.iter(|| {
                use mongreldb_core::Cursor;
                let mut cur = multi
                    .db
                    .native_multi_run_cursor(snap, proj_pairs.clone(), &bm_q.conditions)
                    .unwrap()
                    .unwrap();
                while let Some(batch) = cur.next_batch().unwrap() {
                    black_box(batch);
                }
            })
        });

        // Also benchmark query_columns_native directly on multi-run — it now
        // routes through the cursor internally (Priority 1 + 4 fix), so this
        // measures the full columnar pushdown path end-to-end. Previously this
        // was the ~191 s per-rid rows_for_rids materialized fallback.
        g.bench_function("multi/bitmap_eq_native", |b| {
            b.iter(|| {
                let cols = multi
                    .db
                    .query_columns_native(&bm_q.conditions, Some(&proj), snap)
                    .unwrap()
                    .unwrap();
                black_box(cols);
            })
        });

        // Verify the multi-run path now uses the columnar cursor (Priority 1+4 fix)
        // instead of the old per-rid rows_for_rids materialization.
        let (_, t) = multi
            .db
            .query_columns_native_traced(&bm_q.conditions, Some(&proj), snap)
            .unwrap();
        show_trace("multi/bitmap_eq_native[columnar]", &t);
        debug_assert_eq!(
            t.scan_mode,
            ScanMode::MultiRunCursor,
            "multi-run query_columns_native should now use cursor path"
        );
    }

    g.finish();
}

criterion_group!(benches, bench_path_matrix);
criterion_main!(benches);
