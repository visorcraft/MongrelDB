//! Regression tests for lookup observability counters.
//!
//! The HOT fast path is the expected behavior for primary-key lookups. Any
//! fallback in a healthy workload is a regression to investigate — these tests
//! fail loudly when the fast path silently regresses.

use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{Table, Value};
use tempfile::tempdir;

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
    assert_eq!(
        after.hot_lookup_hit - before.hot_lookup_hit,
        50,
        "expected 50 HOT hits from Pk lookups"
    );
    assert_eq!(
        after.hot_lookup_fallback - before.hot_lookup_fallback,
        0,
        "healthy Pk lookups must not trigger fallback"
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
}
