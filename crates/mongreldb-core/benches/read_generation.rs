//! One-million-row reader/writer characterization for immutable generations.
//!
//! Run: `cargo bench -p mongreldb-core --bench read_generation`

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use mongreldb_core::{columnar::NativeColumn, schema::*, Table, TableHandle, Value};
use std::time::Duration;

const ROWS: usize = 1_000_000;

fn million_row_handle() -> (tempfile::TempDir, TableHandle) {
    let dir = tempfile::tempdir().expect("tempdir");
    let schema = Schema {
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
        }],
        ..Schema::default()
    };
    let mut table = Table::create(dir.path(), schema, 1).expect("create");
    table
        .bulk_load_fast(vec![(
            1,
            NativeColumn::Int64 {
                data: (0..ROWS as i64).collect(),
                validity: vec![0xff; ROWS.div_ceil(8)],
            },
        )])
        .expect("bulk load");
    let handle = TableHandle::from_table(table);
    drop(
        handle
            .read_generation_with_context(None)
            .expect("warm generation"),
    );
    (dir, handle)
}

fn bench_read_generation(c: &mut Criterion) {
    let (_dir, handle) = million_row_handle();
    let mut group = c.benchmark_group("read_generation_1m");
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(20);

    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("create", |b| {
        b.iter(|| {
            let (generation, _) = handle
                .read_generation_with_context(None)
                .expect("read generation");
            black_box(generation.count());
        });
    });

    let mut next_id = ROWS as i64;
    group.throughput(Throughput::Elements(1));
    group.bench_function("writer_while_generation_live", |b| {
        b.iter(|| {
            let (generation, _) = handle
                .read_generation_with_context(None)
                .expect("read generation");
            let clones_before = handle.generation_stats().cow_clone_count;
            let mut writer = handle.lock();
            writer.put(vec![(1, Value::Int64(next_id))]).expect("put");
            writer.commit().expect("commit");
            next_id += 1;
            assert_eq!(handle.generation_stats().cow_clone_count, clones_before);
            black_box(generation.count());
        });
    });
    group.finish();

    #[cfg(target_os = "linux")]
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        if let Some(peak) = status.lines().find(|line| line.starts_with("VmHWM:")) {
            eprintln!(
                "read_generation_1m peak_rss: {}",
                peak.trim_start_matches("VmHWM:").trim()
            );
        }
    }
}

criterion_group!(benches, bench_read_generation);
criterion_main!(benches);
