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

fn rss_bytes(field: &str) -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix(field))?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()
        .map(|kib| kib * 1024)
}

fn peak_rss_bytes() -> Option<u64> {
    rss_bytes("VmHWM:")
}

fn current_rss_bytes() -> Option<u64> {
    rss_bytes("VmRSS:")
}

fn main() {
    let rows = env_usize("MONGRELDB_READ_GENERATION_ROWS", 1_000_000);
    let writes = env_usize("MONGRELDB_READ_GENERATION_WRITES", 100);
    let cursor_limit = env_usize("MONGRELDB_READ_GENERATION_CURSORS", 32);
    let cursor_lifetime_seconds = env_usize("MONGRELDB_READ_GENERATION_CURSOR_LIFETIME_SECONDS", 0);
    assert!(rows > 0 && writes > 0 && cursor_limit > 0);

    let dir = tempfile::tempdir().unwrap();
    let schema = Schema {
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
            embedding_source: None,
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
    let rss_before_generations = current_rss_bytes();
    let mut cursors = VecDeque::with_capacity(cursor_limit);
    let started = Instant::now();
    let mut commit_us = Vec::with_capacity(writes);
    let mut query_us = Vec::with_capacity(writes);
    for offset in 0..writes {
        if cursors.len() == cursor_limit {
            cursors.pop_front();
        }
        let query_started = Instant::now();
        let (generation, _) = handle.read_generation_with_context(None).unwrap();
        query_us.push(query_started.elapsed().as_micros());
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
    let rss_while_generations = current_rss_bytes();
    let workload_elapsed = started.elapsed();
    if cursor_lifetime_seconds > 0 {
        std::thread::sleep(std::time::Duration::from_secs(
            cursor_lifetime_seconds as u64,
        ));
    }
    drop(cursors);
    let stats_after_drop = handle.generation_stats();
    let rss_after_drop = current_rss_bytes();
    let p50 = percentile(&mut commit_us.clone(), 0.50);
    let p95 = percentile(&mut commit_us.clone(), 0.95);
    let p99 = percentile(&mut commit_us, 0.99);
    let query_p50 = percentile(&mut query_us.clone(), 0.50);
    let query_p95 = percentile(&mut query_us.clone(), 0.95);
    let query_p99 = percentile(&mut query_us, 0.99);
    println!(
        "{}",
        serde_json::json!({
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "rows": rows,
            "writes": writes,
            "cursor_limit": cursor_limit,
            "cursor_lifetime_seconds": cursor_lifetime_seconds,
            "query_latency": {"p50_us": query_p50, "p95_us": query_p95, "p99_us": query_p99},
            "commit_latency": {"p50_us": p50, "p95_us": p95, "p99_us": p99},
            "throughput_writes_per_second": writes as f64 / workload_elapsed.as_secs_f64(),
            "clone_bytes_per_write": stats_while_live.estimated_cow_clone_bytes as f64 / writes as f64,
            "peak_rss_bytes": peak_rss_bytes(),
            "current_rss_before_generations": rss_before_generations,
            "current_rss_while_generations": rss_while_generations,
            "current_rss_after_generation_drop": rss_after_drop,
            "estimated_old_generation_retained_bytes": rss_while_generations.zip(rss_before_generations).map(|(after, before)| after.saturating_sub(before)),
            "oom_or_failure": false,
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
