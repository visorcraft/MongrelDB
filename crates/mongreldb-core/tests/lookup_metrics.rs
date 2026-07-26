//! Regression tests for lookup observability counters.
//!
//! The HOT fast path is the expected behavior for primary-key lookups. Any
//! fallback in a healthy workload is a regression to investigate — these tests
//! fail loudly when the fast path silently regresses.

use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{Table, Value};
use tempfile::tempdir;

/// Emit a structured one-line JSON record for the residual-closure script
/// (`scripts/run-residual-closure.sh`) to harvest. The script greps for
/// lines starting with `{"test":` and writes them to the corresponding
/// `<topic>-results.jsonl` artifact.
macro_rules! emit_metric {
    ($name:literal, $metric:expr, $unit:literal) => {
        println!(
            "{}",
            serde_json::json!({
                "test": $name,
                "metric": $metric,
                "unit": $unit,
            })
        )
    };
}

fn schema() -> Schema {
    let column = |id: u16, name: &str, ty: TypeId, primary_key: bool| ColumnDef {
        id,
        name: name.into(),
        ty,
        flags: if primary_key {
            ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY)
        } else {
            ColumnFlags::empty()
        },
        default_value: None,
        embedding_source: None,
    };
    Schema {
        schema_id: 1,
        columns: vec![
            column(1, "id", TypeId::Int64, true),
            column(2, "name", TypeId::Bytes, false),
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn put(table: &mut Table, id: i64, name: &str) {
    table
        .put(vec![
            (1, Value::Int64(id)),
            (2, Value::Bytes(name.as_bytes().to_vec())),
        ])
        .unwrap();
}

fn pk_bytes(id: i64) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

#[test]
fn healthy_pk_lookup_uses_hot_fast_path_with_zero_fallback() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    for i in 0..50i64 {
        put(&mut table, 1000 + i, "alice");
    }
    table.commit().unwrap();

    let before = table.lookup_metrics_snapshot();
    assert_eq!(
        before.hot_lookup_fallback, 0,
        "fresh table should not have any fallback"
    );

    // Each Condition::Pk lookup hits HOT directly.
    for i in 0..50i64 {
        let _hits = table
            .query(&Query::new().and(Condition::Pk(pk_bytes(1000 + i))))
            .unwrap();
    }

    let after = table.lookup_metrics_snapshot();
    let hit_delta = after.hot_lookup_hit - before.hot_lookup_hit;
    let fallback_delta = after.hot_lookup_fallback - before.hot_lookup_fallback;
    assert_eq!(
        hit_delta,
        50,
        "expected 50 HOT hits from Pk lookups"
    );
    assert_eq!(
        fallback_delta,
        0,
        "healthy Pk lookups must not trigger fallback"
    );
    emit_metric!(
        "lookup_metrics::healthy_pk_lookup_uses_hot_fast_path_with_zero_fallback",
        fallback_delta,
        "fallback_count"
    );
}

#[test]
fn result_cache_counters_advance_on_repeat_query() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    put(&mut table, 1, "alpha");
    put(&mut table, 2, "beta");
    table.commit().unwrap();
    table.flush().unwrap();

    let before = table.lookup_metrics_snapshot();
    // First call: miss + insert (writes to persistent tier).
    let _ = table
        .query_columns_native_cached(
            &[Condition::Pk(pk_bytes(1))],
            None,
            mongreldb_core::Snapshot::unbounded(),
        )
        .unwrap();
    // Second call: memory hit.
    let _ = table
        .query_columns_native_cached(
            &[Condition::Pk(pk_bytes(1))],
            None,
            mongreldb_core::Snapshot::unbounded(),
        )
        .unwrap();

    let after = table.lookup_metrics_snapshot();
    let mem_delta = after.result_cache_memory_hit - before.result_cache_memory_hit;
    let miss_delta = after.result_cache_miss - before.result_cache_miss;
    let write_delta =
        after.result_cache_persistent_write_us - before.result_cache_persistent_write_us;

    assert!(
        mem_delta >= 1,
        "expected at least one memory hit on repeat query, got {}",
        mem_delta
    );
    assert!(
        miss_delta >= 1,
        "expected at least one miss on first query, got {}",
        miss_delta
    );
    // Tiny one-row PK results stay below the default 4 KiB persist threshold
    // so a warm miss is not forced to pay synchronous filesystem publish.
    // Memory-tier insert still happens (proven by the hit on the second call).
    assert_eq!(
        write_delta, 0,
        "tiny one-row result must skip persistent tier (got write_us delta {write_delta})"
    );
    emit_metric!(
        "lookup_metrics::result_cache_counters_advance_on_repeat_query",
        mem_delta,
        "memory_hit_count"
    );
}

// Indices into `LookupMetricsSnapshot::hot_fallback_reasons`. They mirror the
// ordering in `engine::hot_fallback_reason_index` and must stay aligned if a
// new reason is appended. The 3 ignored tests below reference these; once
// PR F wires the engine increment sites they are used.
#[allow(dead_code)]
const REASON_MISSING_MAPPING: usize = 0;
#[allow(dead_code)]
const REASON_STALE_ROW_ID: usize = 1;
#[allow(dead_code)]
const REASON_INVISIBLE_AT_SNAPSHOT: usize = 2;
#[allow(dead_code)]
const REASON_HISTORICAL_SNAPSHOT: usize = 3;
#[allow(dead_code)]
const REASON_TOMBSTONE: usize = 4;
#[allow(dead_code)]
const REASON_TTL_EXPIRED: usize = 5;
#[allow(dead_code)]
const REASON_PRIMARY_KEY_MISMATCH: usize = 6;
#[allow(dead_code)]
const REASON_INDEX_INCOMPLETE: usize = 7;
#[allow(dead_code)]
const REASON_CHECKPOINT_REJECTED: usize = 8;

#[test]
fn healthy_pk_lookup_records_zero_fallback() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    for i in 0..32i64 {
        put(&mut table, 1000 + i, "alice");
    }
    table.commit().unwrap();

    let before = table.lookup_metrics_snapshot();

    for i in 0..32i64 {
        let _hits = table
            .query(&Query::new().and(Condition::Pk(pk_bytes(1000 + i))))
            .unwrap();
    }

    let after = table.lookup_metrics_snapshot();
    assert_eq!(
        after.hot_lookup_hit - before.hot_lookup_hit,
        32,
        "expected 32 HOT hits from Pk lookups"
    );
    assert_eq!(
        after.hot_lookup_fallback - before.hot_lookup_fallback,
        0,
        "healthy Pk lookups must not trigger fallback"
    );

    // Every per-reason counter must stay zero on the healthy path.
    let before_reasons = before.hot_fallback_reasons;
    let after_reasons = after.hot_fallback_reasons;
    for (idx, (b, a)) in before_reasons.iter().zip(after_reasons.iter()).enumerate() {
        assert_eq!(
            a - b,
            0,
            "healthy lookup must not increment reason[{idx}], got delta {}",
            a - b
        );
    }
    let before_total: u64 = before_reasons.iter().sum();
    let after_total: u64 = after_reasons.iter().sum();
    assert_eq!(after_total - before_total, 0);
    assert_eq!(
        after.hot_fallback_runs_considered_total - before.hot_fallback_runs_considered_total,
        0
    );
    assert_eq!(
        after.hot_fallback_runs_opened_total - before.hot_fallback_runs_opened_total,
        0
    );
    assert_eq!(
        after.hot_fallback_pages_decoded_total - before.hot_fallback_pages_decoded_total,
        0
    );
    assert_eq!(
        after.hot_fallback_rows_materialized_total - before.hot_fallback_rows_materialized_total,
        0
    );
    assert_eq!(
        after.hot_mapping_rebuild_total - before.hot_mapping_rebuild_total,
        0
    );
    assert_eq!(
        after.hot_checkpoint_rejected_total - before.hot_checkpoint_rejected_total,
        0
    );
    emit_metric!(
        "lookup_metrics::healthy_pk_lookup_records_zero_fallback",
        after.hot_lookup_hit - before.hot_lookup_hit,
        "hot_hit_count"
    );
}

#[test]
fn deleted_row_increments_tombstone_fallback_reason() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    let row_id = table
        .put(vec![
            (1, Value::Int64(7)),
            (2, Value::Bytes(b"alice".to_vec())),
        ])
        .unwrap();
    table.commit().unwrap();
    table.delete(row_id).unwrap();
    table.commit().unwrap();

    let before = table.lookup_metrics_snapshot();
    let _rows = table
        .query(&Query::new().and(Condition::Pk(pk_bytes(7))))
        .unwrap();
    let after = table.lookup_metrics_snapshot();

    assert_eq!(
        after.hot_fallback_reasons[REASON_TOMBSTONE]
            - before.hot_fallback_reasons[REASON_TOMBSTONE],
        1,
        "deleted-row lookup must increment the Tombstone fallback reason"
    );
    assert!(
        after.hot_fallback_runs_considered_total - before.hot_fallback_runs_considered_total >= 1,
        "deleted-row lookup must register at least one considered run"
    );
    emit_metric!(
        "lookup_metrics::deleted_row_increments_tombstone_fallback_reason",
        after.hot_fallback_reasons[REASON_TOMBSTONE]
            - before.hot_fallback_reasons[REASON_TOMBSTONE],
        "tombstone_reason_count"
    );
}

#[test]
fn historical_snapshot_records_historical_fallback_reason() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    let row_id = table
        .put(vec![
            (1, Value::Int64(11)),
            (2, Value::Bytes(b"alice".to_vec())),
        ])
        .unwrap();
    table.commit().unwrap();

    // Pin the snapshot BEFORE the delete commits so the lookup must travel
    // through the historical-snapshot fallback path.
    let snap = table.snapshot();
    table.delete(row_id).unwrap();
    table.commit().unwrap();

    let before = table.lookup_metrics_snapshot();
    let _rows = table
        .query_at_with_allowed(&Query::new().and(Condition::Pk(pk_bytes(11))), snap, None)
        .unwrap();
    let after = table.lookup_metrics_snapshot();

    assert_eq!(
        after.hot_fallback_reasons[REASON_HISTORICAL_SNAPSHOT]
            - before.hot_fallback_reasons[REASON_HISTORICAL_SNAPSHOT],
        1,
        "lookup under pre-delete snapshot must record HistoricalSnapshot reason"
    );
    emit_metric!(
        "lookup_metrics::historical_snapshot_records_historical_fallback_reason",
        after.hot_fallback_reasons[REASON_HISTORICAL_SNAPSHOT]
            - before.hot_fallback_reasons[REASON_HISTORICAL_SNAPSHOT],
        "historical_reason_count"
    );
}

#[test]
fn snapshot_to_metrics_is_consistent() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();

    // Healthy lookups (HOT hits) must not perturb the per-reason counters.
    for i in 0..8i64 {
        put(&mut table, 2000 + i, "healthy");
    }
    table.commit().unwrap();
    for i in 0..8i64 {
        let _hits = table
            .query(&Query::new().and(Condition::Pk(pk_bytes(2000 + i))))
            .unwrap();
    }

    // Delete + lookup exercises the tombstone fallback path.
    let tomb_row = table
        .put(vec![
            (1, Value::Int64(99)),
            (2, Value::Bytes(b"doomed".to_vec())),
        ])
        .unwrap();
    table.commit().unwrap();
    table.delete(tomb_row).unwrap();
    table.commit().unwrap();
    let _rows = table
        .query(&Query::new().and(Condition::Pk(pk_bytes(99))))
        .unwrap();

    // Lookup under a pre-delete snapshot exercises the historical path.
    let pinned = table.snapshot();
    let hist_row = table
        .put(vec![
            (1, Value::Int64(101)),
            (2, Value::Bytes(b"hist".to_vec())),
        ])
        .unwrap();
    table.commit().unwrap();
    table.delete(hist_row).unwrap();
    table.commit().unwrap();
    let _rows = table
        .query_at_with_allowed(
            &Query::new().and(Condition::Pk(pk_bytes(101))),
            pinned,
            None,
        )
        .unwrap();

    let metrics = table.lookup_metrics_snapshot();
    let reason_sum: u64 = metrics.hot_fallback_reasons.iter().sum();
    assert_eq!(
        reason_sum, metrics.hot_lookup_fallback,
        "sum of hot_fallback_reasons[0..9] (={reason_sum}) must equal \
         hot_lookup_fallback (={})",
        metrics.hot_lookup_fallback
    );
    emit_metric!(
        "lookup_metrics::snapshot_to_metrics_is_consistent",
        reason_sum,
        "reason_sum_equals_fallback"
    );
}
