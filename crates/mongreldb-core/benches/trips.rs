//! Realistic-operation benchmark for a small (100-row) `travel_trips`-style
//! table: full scan (the `select *` equivalent) and a single-record update.
//!
//! `select *` is measured two ways: with the rows still in the live memtable
//! (just inserted/committed) and after `flush()` to a sorted run on disk (the
//! more realistic "table at rest" case). The update is `put` (new version) +
//! `commit` (one fsync) — the durable-update cost.

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use mongreldb_core::{schema::*, Table, Value};
use std::time::Duration;
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
                default_value: None,
            },
            ColumnDef {
                id: 2,
                name: "destination".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
            ColumnDef {
                id: 3,
                name: "departure".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
            ColumnDef {
                id: 4,
                name: "cost".into(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
            ColumnDef {
                id: 5,
                name: "rating".into(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: Vec::new(),
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

const N: u64 = 100;

fn insert_rows(db: &mut Table) {
    for i in 0..N {
        db.put(vec![
            (1, Value::Int64(i as i64)),
            (2, Value::Bytes(format!("City{i}").into_bytes())),
            (3, Value::Int64(1_700_000_000 + i as i64 * 86_400)),
            (4, Value::Float64(199.99 + i as f64)),
            (5, Value::Float64(4.0 + (i % 2) as f64)),
        ])
        .unwrap();
    }
}

/// A table with 100 rows still in the memtable (inserted + committed, not flushed).
fn memtable_db() -> (tempfile::TempDir, Table) {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    insert_rows(&mut db);
    db.commit().unwrap();
    (dir, db)
}

/// A table with 100 rows flushed to a sorted run on disk (memtable empty).
fn flushed_db() -> (tempfile::TempDir, Table) {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    insert_rows(&mut db);
    db.flush().unwrap();
    (dir, db)
}

fn bench_trips(c: &mut Criterion) {
    let mut g = c.benchmark_group("trips");
    g.measurement_time(Duration::from_secs(2));
    g.sample_size(50);

    // --- "select *" over 100 rows, in the live memtable ----------------
    g.throughput(Throughput::Elements(N));
    g.bench_function("select_star_100_memtable", |b| {
        b.iter_batched_ref(
            memtable_db,
            |(_dir, db)| {
                let snap = db.snapshot();
                let rows = db.visible_rows(snap).unwrap();
                black_box(rows);
            },
            BatchSize::SmallInput,
        );
    });

    // --- "select *" over 100 rows, flushed to a sorted run on disk ------
    g.bench_function("select_star_100_flushed", |b| {
        b.iter_batched_ref(
            flushed_db,
            |(_dir, db)| {
                let snap = db.snapshot();
                let rows = db.visible_rows(snap).unwrap();
                black_box(rows);
            },
            BatchSize::SmallInput,
        );
    });

    // --- update one record (put new version + commit, one fsync) --------
    g.throughput(Throughput::Elements(1));
    g.bench_function("update_one_commit", |b| {
        b.iter_batched_ref(
            flushed_db,
            |(_dir, db)| {
                // Update the row whose id == 0: a new version + a durable commit.
                black_box(
                    db.put(vec![
                        (1, Value::Int64(0)),
                        (2, Value::Bytes(b"City0-updated".to_vec())),
                        (3, Value::Int64(1_700_000_000)),
                        (4, Value::Float64(209.99)),
                        (5, Value::Float64(5.0)),
                    ])
                    .unwrap(),
                );
                black_box(db.commit().unwrap());
            },
            BatchSize::SmallInput,
        );
    });

    // --- update one record WITHOUT fsync (in-process cost only) ---------
    g.bench_function("update_one_no_fsync", |b| {
        b.iter_batched_ref(
            flushed_db,
            |(_dir, db)| {
                black_box(
                    db.put(vec![
                        (1, Value::Int64(0)),
                        (2, Value::Bytes(b"City0-updated".to_vec())),
                        (3, Value::Int64(1_700_000_000)),
                        (4, Value::Float64(209.99)),
                        (5, Value::Float64(5.0)),
                    ])
                    .unwrap(),
                );
            },
            BatchSize::SmallInput,
        );
    });

    g.finish();
}

criterion_group!(benches, bench_trips);
criterion_main!(benches);
