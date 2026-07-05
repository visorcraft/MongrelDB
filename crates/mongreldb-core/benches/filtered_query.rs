//! Filtered-query benchmark on 1M rows: full scan vs index/range pushdown.
//!
//! Compares decoding all columns (`visible_columns_native`) against
//! `query_columns_native` with bitmap-equality, int-range, and their
//! intersection, each projecting a subset of columns. The pushdown paths decode
//! only the projected columns and binary-search survivor positions, so they
//! should stay flat as the table grows while the full scan scales linearly.
//!
//! Run: `cargo bench -p mongreldb-core --bench filtered_query`

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use mongreldb_core::query::{Condition, Query};
use mongreldb_core::{schema::*, Table, Value};
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

fn bench_filtered_query(c: &mut Criterion) {
    let mut g = c.benchmark_group("filtered_query");
    g.measurement_time(Duration::from_secs(4));
    g.sample_size(10);
    g.throughput(Throughput::Elements(N));

    // Setup once: bulk-load 1M rows (indexes maintained by `index_into`).
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.bulk_load(rows(N)).unwrap();
    let snap = db.snapshot();
    let proj: [u16; 2] = [1, 3]; // id + cost

    // Baseline: full scan, decode every column.
    g.bench_function("scan_full_all_columns", |b| {
        b.iter(|| {
            let cols = db.visible_columns_native(snap, None).unwrap();
            black_box(cols);
        })
    });

    // Bitmap pushdown (cat = "cat5"): ~N/CATS survivors, 2 columns decoded.
    g.bench_function("filter_bitmap_eq", |b| {
        b.iter(|| {
            let q = Query::new().and(Condition::BitmapEq {
                column_id: 2,
                value: b"cat5".to_vec(),
            });
            let cols = db
                .query_columns_native(&q.conditions, Some(&proj), snap)
                .unwrap()
                .unwrap();
            black_box(cols);
        })
    });

    // Int range pushdown (ts in a 5% window): single-column filter, 2 decoded.
    g.bench_function("filter_range_int", |b| {
        b.iter(|| {
            let lo = 1_700_000_000i64 + (N as i64) / 20;
            let hi = 1_700_000_000i64 + (N as i64) / 10;
            let q = Query::new().and(Condition::Range {
                column_id: 4,
                lo,
                hi,
            });
            let cols = db
                .query_columns_native(&q.conditions, Some(&proj), snap)
                .unwrap()
                .unwrap();
            black_box(cols);
        })
    });

    // Intersection: bitmap ∩ range.
    g.bench_function("filter_bitmap_intersect_range", |b| {
        b.iter(|| {
            let lo = 1_700_000_000i64;
            let hi = 1_700_000_000i64 + (N as i64) / 2;
            let q = Query::new()
                .and(Condition::BitmapEq {
                    column_id: 2,
                    value: b"cat5".to_vec(),
                })
                .and(Condition::Range {
                    column_id: 4,
                    lo,
                    hi,
                });
            let cols = db
                .query_columns_native(&q.conditions, Some(&proj), snap)
                .unwrap()
                .unwrap();
            black_box(cols);
        })
    });

    // Projection knob: same bitmap filter, decode only 1 column vs 3.
    g.bench_function("filter_bitmap_project_1col", |b| {
        b.iter(|| {
            let q = Query::new().and(Condition::BitmapEq {
                column_id: 2,
                value: b"cat5".to_vec(),
            });
            let proj1: [u16; 1] = [1];
            let cols = db
                .query_columns_native(&q.conditions, Some(&proj1), snap)
                .unwrap()
                .unwrap();
            black_box(cols);
        })
    });

    let _ = db;
    drop(dir);
    g.finish();
}

criterion_group!(benches, bench_filtered_query);
criterion_main!(benches);
