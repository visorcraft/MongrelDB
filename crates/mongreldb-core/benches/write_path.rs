//! Write-path micro-benchmarks.
//!
//! - `put_no_fsync`: per-write latency of `put` with auto-sync disabled — the
//!   pure in-process cost (WAL append to buffer + skip-list memtable insert +
//!   HOT update). This is the "sub-ms write" number.
//! - `commit_fsync`: the durability floor (one `fsync` of the WAL + an atomic
//!   manifest write).
//! - `group_commit_1000`: 1000 puts + one commit, reported as amortized
//!   elements/sec — the steady-state group-commit throughput.
//!
//! Run: `cargo bench -p mongreldb-core`

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
            },
            ColumnDef {
                id: 2,
                name: "payload".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: Vec::new(),
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

const PAYLOAD: &[u8] = b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"; // 32 bytes

fn fresh_db() -> (tempfile::TempDir, Table) {
    let dir = tempdir().expect("tempdir");
    let mut db = Table::create(dir.path(), schema(), 1).expect("create");
    db.set_sync_byte_threshold(0); // manual commit only; isolate put latency
    (dir, db)
}

fn bench_write_path(c: &mut Criterion) {
    let mut g = c.benchmark_group("write_path");
    g.measurement_time(Duration::from_secs(2));
    g.sample_size(40);

    // --- pure per-write latency (no fsync) -----------------------------
    g.bench_function("put_no_fsync", |b| {
        b.iter_batched_ref(
            fresh_db,
            |(_dir, db)| {
                let i = db.memtable_len() as i64 + 1;
                black_box(
                    db.put(vec![
                        (1, Value::Int64(i)),
                        (2, Value::Bytes(PAYLOAD.to_vec())),
                    ])
                    .unwrap(),
                );
            },
            BatchSize::SmallInput,
        );
    });

    // --- durability floor: a single group commit -----------------------
    g.bench_function("commit_fsync", |b| {
        b.iter_batched_ref(
            fresh_db,
            |(_dir, db)| black_box(db.commit().unwrap()),
            BatchSize::SmallInput,
        );
    });

    // --- group-commit throughput (1000 puts + 1 commit, full cycle) ----
    const N: u64 = 1000;
    g.throughput(Throughput::Elements(N));
    g.bench_function("group_commit_1000", |b| {
        b.iter_batched_ref(
            fresh_db,
            |(_dir, db)| {
                for i in 0..N {
                    black_box(
                        db.put(vec![
                            (1, Value::Int64(i as i64)),
                            (2, Value::Bytes(PAYLOAD.to_vec())),
                        ])
                        .unwrap(),
                    );
                }
                black_box(db.commit().unwrap());
            },
            BatchSize::SmallInput,
        );
    });

    g.finish();
}

criterion_group!(benches, bench_write_path);
criterion_main!(benches);
