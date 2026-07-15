//! One-million-row bounded cursor-generation/write characterization.

use mongreldb_core::{
    columnar::NativeColumn,
    schema::{ColumnDef, ColumnFlags, Schema, TypeId},
    Table, TableHandle, Value,
};
use std::{collections::VecDeque, time::Instant};

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn percentile(values: &mut [u128], fraction: f64) -> u128 {
    values.sort_unstable();
    values[((values.len() - 1) as f64 * fraction).round() as usize]
}

fn peak_rss_bytes() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()
        .map(|kib| kib * 1024)
}

fn main() {
    let rows = env_usize("MONGRELDB_READ_GENERATION_ROWS", 1_000_000);
    let writes = env_usize("MONGRELDB_READ_GENERATION_WRITES", 100);
    let cursor_limit = env_usize("MONGRELDB_READ_GENERATION_CURSORS", 32);
    assert!(rows > 0 && writes > 0 && cursor_limit > 0);

    let dir = tempfile::tempdir().unwrap();
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
    let mut table = Table::create(dir.path(), schema, 1).unwrap();
    table
        .bulk_load_fast(vec![(
            1,
            NativeColumn::Int64 {
                data: (0..rows as i64).collect(),
                validity: vec![0xff; rows.div_ceil(8)],
            },
        )])
        .unwrap();
    let handle = TableHandle::from_table(table);
    let mut cursors = VecDeque::with_capacity(cursor_limit);
    let started = Instant::now();
    let mut commit_us = Vec::with_capacity(writes);
    for offset in 0..writes {
        if cursors.len() == cursor_limit {
            cursors.pop_front();
        }
        let (generation, _) = handle.read_generation_with_context(None).unwrap();
        cursors.push_back(generation);
        let write_started = Instant::now();
        let mut writer = handle.lock();
        writer
            .put(vec![(1, Value::Int64((rows + offset) as i64))])
            .unwrap();
        writer.commit().unwrap();
        drop(writer);
        commit_us.push(write_started.elapsed().as_micros());
    }
    let stats_while_live = handle.generation_stats();
    drop(cursors);
    let stats_after_drop = handle.generation_stats();
    let elapsed = started.elapsed();
    let p50 = percentile(&mut commit_us.clone(), 0.50);
    let p95 = percentile(&mut commit_us.clone(), 0.95);
    let p99 = percentile(&mut commit_us, 0.99);
    println!(
        "{}",
        serde_json::json!({
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "rows": rows,
            "writes": writes,
            "cursor_limit": cursor_limit,
            "commit_latency": {"p50_us": p50, "p95_us": p95, "p99_us": p99},
            "throughput_writes_per_second": writes as f64 / elapsed.as_secs_f64(),
            "peak_rss_bytes": peak_rss_bytes(),
            "generation_stats_while_live": {
                "active_read_generations": stats_while_live.active_read_generations,
                "max_live_read_generations": stats_while_live.max_live_read_generations,
                "cow_clone_count": stats_while_live.cow_clone_count,
                "cow_clone_nanos": stats_while_live.cow_clone_nanos,
                "estimated_cow_clone_bytes": stats_while_live.estimated_cow_clone_bytes,
                "writer_wait_nanos": stats_while_live.writer_wait_nanos,
            },
            "active_read_generations_after_drop": stats_after_drop.active_read_generations,
        })
    );
}
