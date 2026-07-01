//! Scale benchmark: bulk ingest (`put_batch` + flush) and a full scan over a
//! larger table, to give MongrelDB real at-scale numbers (vs the ≤100-row micro
//! bench in `trips.rs`).
//!
//! Run: `cargo bench -p mongreldb-core --bench scale`

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use mongreldb_core::{columnar::NativeColumn, schema::*, Table, Value};
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
            },
            ColumnDef {
                id: 2,
                name: "destination".into(),
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
        indexes: Vec::new(),
        colocation: vec![], constraints: Default::default(),
    }
}

fn batch(n: u64) -> Vec<Vec<(u16, Value)>> {
    (0..n)
        .map(|i| {
            vec![
                (1, Value::Int64(i as i64)),
                (2, Value::Bytes(format!("City{}", i % 50).into_bytes())), // low-cardinality ⇒ dictionary encoding
                (3, Value::Float64(199.99 + i as f64)),
            ]
        })
        .collect()
}

/// Same data as `batch`, but as typed `NativeColumn`s (no `Value` enum) — the
/// Phase 14 ingest surface.
fn native_columns(n: u64) -> Vec<(u16, NativeColumn)> {
    let ids = NativeColumn::Int64 {
        data: (0..n as i64).collect(),
        validity: vec![0xFF; (n as usize).div_ceil(8)],
    };
    let mut offsets = Vec::with_capacity(n as usize + 1);
    let mut values = Vec::new();
    offsets.push(0);
    for i in 0..n {
        values.extend_from_slice(format!("City{}", i % 50).as_bytes());
        offsets.push(values.len() as u32);
    }
    let dest = NativeColumn::Bytes {
        offsets,
        values,
        validity: vec![0xFF; (n as usize).div_ceil(8)],
    };
    let cost = NativeColumn::Float64 {
        data: (0..n).map(|i| 199.99 + i as f64).collect(),
        validity: vec![0xFF; (n as usize).div_ceil(8)],
    };
    vec![(1, ids), (2, dest), (3, cost)]
}

fn bench_scale(c: &mut Criterion) {
    let mut g = c.benchmark_group("scale");
    g.measurement_time(Duration::from_secs(3));
    g.sample_size(10);

    for &n in &[100u64, 1_000_000] {
        // Bulk ingest: put_batch + one commit (no flush).
        g.throughput(Throughput::Elements(n));
        g.bench_with_input(
            criterion::BenchmarkId::new("bulk_ingest_put_batch", n),
            &n,
            |b, &n| {
                b.iter_batched(
                    || (tempdir().unwrap(), batch(n)),
                    |(dir, rows)| {
                        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
                        let ids = db.put_batch(rows).unwrap();
                        db.commit().unwrap();
                        black_box(ids);
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Bulk ingest + flush to a compressed run on disk.
        g.bench_with_input(
            criterion::BenchmarkId::new("bulk_ingest_flush", n),
            &n,
            |b, &n| {
                b.iter_batched(
                    || (tempdir().unwrap(), batch(n)),
                    |(dir, rows)| {
                        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
                        db.put_batch(rows).unwrap();
                        db.flush().unwrap();
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Bulk-load fast path: write a run directly (skip WAL + memtable).
        g.bench_with_input(criterion::BenchmarkId::new("bulk_load", n), &n, |b, &n| {
            b.iter_batched(
                || (tempdir().unwrap(), batch(n)),
                |(dir, rows)| {
                    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
                    db.bulk_load(rows).unwrap();
                },
                BatchSize::SmallInput,
            );
        });

        // Phase 14 typed bulk load: Vec<NativeColumn> → zstd-1 run, parallel
        // encode + direct-to-mmap. The fast ingest path.
        g.bench_with_input(
            criterion::BenchmarkId::new("bulk_load_columns", n),
            &n,
            |b, &n| {
                b.iter_batched(
                    || (tempdir().unwrap(), native_columns(n)),
                    |(dir, cols)| {
                        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
                        db.bulk_load_columns(cols).unwrap();
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Phase 14.4: raw ALGO_PLAIN pages (no zstd) — maximal encode throughput.
        g.bench_with_input(
            criterion::BenchmarkId::new("bulk_load_fast", n),
            &n,
            |b, &n| {
                b.iter_batched(
                    || (tempdir().unwrap(), native_columns(n)),
                    |(dir, cols)| {
                        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
                        db.bulk_load_fast(cols).unwrap();
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Full scan via the vectorized columnar path (select *).
        g.bench_with_input(
            criterion::BenchmarkId::new("scan_columns", n),
            &n,
            |b, &n| {
                b.iter_batched_ref(
                    || {
                        let dir = tempdir().unwrap();
                        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
                        db.put_batch(batch(n)).unwrap();
                        db.flush().unwrap();
                        (dir, db)
                    },
                    |(_dir, db)| {
                        let snap = db.snapshot();
                        let cols = db.visible_columns(snap).unwrap();
                        black_box(cols);
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // Phase 15.7: full scan over a typed bulk-loaded run (little-endian
        // pages — decode is a memcpy on LE targets) via the NATIVE typed scan
        // path (`visible_columns_native`), which is what production scans use.
        // Compare against `scan_columns` (BE, Value path) for the endianness +
        // decode-path win.
        g.bench_with_input(
            criterion::BenchmarkId::new("scan_columns_le", n),
            &n,
            |b, &n| {
                b.iter_batched_ref(
                    || {
                        let dir = tempdir().unwrap();
                        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
                        db.bulk_load_columns(native_columns(n)).unwrap();
                        (dir, db)
                    },
                    |(_dir, db)| {
                        let snap = db.snapshot();
                        let cols = db.visible_columns_native(snap, None).unwrap();
                        black_box(cols);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    g.finish();
}

criterion_group!(benches, bench_scale);
criterion_main!(benches);
