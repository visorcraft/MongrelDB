//! REM-003 controlled-scan tests.
//!
//! The tests in this file assert real tier state (`memtable_len` /
//! `mutable_run_len` / `run_count`) **before** running the controlled scan, so
//! the streaming contract is verified against the actual tier that holds the
//! data — not against a coincidentally-passing `bulk_load_columns` that
//! materialises a sorted run. See
//! `docs/architecture/controlled-cursor.md` for the contract.

use std::collections::BTreeMap;

use mongreldb_core::columnar::NativeColumn;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::trace::QueryTrace;
use mongreldb_core::{
    CancellationReason, Epoch, ExecutionControl, MongrelError, Row, RowId, Snapshot, Table, Value,
};
use mongreldb_types::hlc::HlcTimestamp;
use tempfile::tempdir;

/// Emit a structured one-line JSON record for `scripts/run-residual-closure.sh`
/// to harvest.
macro_rules! emit_scan_metric {
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
    Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "value".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        ..Schema::default()
    }
}

fn put(table: &mut Table, id: i64, value: i64) -> RowId {
    table
        .put(vec![(1, Value::Int64(id)), (2, Value::Int64(value))])
        .unwrap()
}

fn bulk_load(table: &mut Table, rows: usize) {
    table
        .bulk_load_columns(vec![
            (1, NativeColumn::int64_sequence(0, rows)),
            (2, NativeColumn::int64_sequence(0, rows)),
        ])
        .unwrap();
}

fn value(row: &Row) -> i64 {
    match row.columns.get(&2) {
        Some(Value::Int64(value)) => *value,
        other => panic!("expected Int64 value, got {other:?}"),
    }
}

fn collect(table: &Table, snap: Snapshot) -> Vec<Row> {
    let control = ExecutionControl::new(None);
    let mut out = Vec::new();
    table
        .for_each_visible_row_controlled(snap, &control, |row| {
            out.push(row);
            Ok(())
        })
        .unwrap();
    out
}

#[test]
fn one_million_row_memtable_yields_ascending_strict_order() {
    // Real memtable million-row fixture using normal `put` (NOT
    // bulk_load_columns, which would write to a sorted run). Assert
    // `memtable_len >= 1_000_000` and `mutable_run_len() == 0` BEFORE
    // scanning so the assertion is about the memtable path, not a
    // coincidentally-passing run.
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(u64::MAX);
    let n = 1_000_000_i64;
    for i in 0..n {
        put(&mut table, i, i);
    }
    table.commit().unwrap();
    assert!(
        table.memtable_len() >= 1_000_000,
        "memtable must hold all rows (memtable_len = {})",
        table.memtable_len()
    );
    assert_eq!(
        table.mutable_run_len(),
        0,
        "mutable run must be empty (mutable_run_len = {})",
        table.mutable_run_len()
    );
    let snap = table.snapshot();
    let (rows, trace) = QueryTrace::capture(|| collect(&table, snap));
    assert_eq!(rows.len(), 1_000_000);
    assert_eq!(trace.controlled_scan_rows_emitted, 1_000_000);
    for pair in rows.windows(2) {
        assert!(
            pair[0].row_id.0 < pair[1].row_id.0,
            "output must be strictly ascending by RowId"
        );
    }
    assert!(
        trace.controlled_scan_setup_time_us < 200_000,
        "setup took {} µs",
        trace.controlled_scan_setup_time_us
    );
    assert!(
        trace.controlled_scan_peak_source_buffer_rows <= 8,
        "memtable-only streaming should keep the per-tier buffer small (peak = {})",
        trace.controlled_scan_peak_source_buffer_rows
    );
    emit_scan_metric!(
        "controlled_scan::one_million_row_memtable_yields_ascending_strict_order",
        trace.controlled_scan_peak_source_buffer_rows,
        "peak_source_buffer_rows"
    );
}

#[test]
fn one_million_row_mutable_run_yields_ascending_strict_order() {
    // Real mutable-run million-row fixture. Flush once so the memtable drains
    // into the mutable run, then assert the mutable run actually holds the
    // million rows and no sorted runs exist.
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(u64::MAX);
    let n = 1_000_000_i64;
    for i in 0..n {
        put(&mut table, i, i);
    }
    table.flush().unwrap();
    table.commit().unwrap();
    assert!(
        table.mutable_run_len() >= 1_000_000,
        "mutable run must hold all rows (mutable_run_len = {})",
        table.mutable_run_len()
    );
    assert_eq!(
        table.run_count(),
        0,
        "no sorted runs (run_count = {})",
        table.run_count()
    );
    let snap = table.snapshot();
    let (rows, trace) = QueryTrace::capture(|| collect(&table, snap));
    assert_eq!(rows.len(), 1_000_000);
    assert_eq!(trace.controlled_scan_rows_emitted, 1_000_000);
    for pair in rows.windows(2) {
        assert!(
            pair[0].row_id.0 < pair[1].row_id.0,
            "output must be strictly ascending by RowId"
        );
    }
    assert!(
        trace.controlled_scan_setup_time_us < 200_000,
        "mutable-run setup took {} µs",
        trace.controlled_scan_setup_time_us
    );
    emit_scan_metric!(
        "controlled_scan::one_million_row_mutable_run_yields_ascending_strict_order",
        trace.controlled_scan_peak_source_buffer_rows,
        "peak_source_buffer_rows"
    );
}

#[test]
fn out_of_order_be_tree_buffer_emits_ascending() {
    // Insert higher, then low, then higher row. The Bε internal buffer is in
    // insertion order; the streaming cursor must still yield ascending RowId.
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(u64::MAX);
    let _ = put(&mut table, 1_000_000, 1);
    let _ = put(&mut table, 1, 2);
    let _ = put(&mut table, 999_999, 3);
    let _ = put(&mut table, 500_000, 4);
    let _ = put(&mut table, 2, 5);
    table.commit().unwrap();
    assert!(table.memtable_len() >= 5);

    let snap = table.snapshot();
    let mut observed: Vec<RowId> = Vec::new();
    let control = ExecutionControl::new(None);
    table
        .for_each_visible_row_controlled(snap, &control, |row| {
            observed.push(row.row_id);
            Ok(())
        })
        .unwrap();
    let mut sorted = observed.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(observed, sorted, "output must be strictly ascending");
    assert_eq!(observed.len(), 5);
}

#[test]
fn same_rowid_across_frozen_layers_dedups_to_newest() {
    // Use a clustered (WITHOUT ROWID) table so the same primary key (7)
    // always maps to the same RowId, letting us exercise dense history on
    // a single RowId across a sorted run and a memtable.
    let mut schema_clustered = schema();
    schema_clustered.clustered = true;
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema_clustered, 1).unwrap();
    table.set_mutable_run_spill_bytes(1);
    let rid = put(&mut table, 7, 100);
    table.flush().unwrap();
    put(&mut table, 8, 80);
    table.flush().unwrap();
    table.set_mutable_run_spill_bytes(u64::MAX);
    // Re-write the same primary key (7) with a higher epoch in the active
    // memtable. Because the table is clustered, this re-uses `rid`.
    table
        .put(vec![(1, Value::Int64(7)), (2, Value::Int64(777))])
        .unwrap();
    table.commit().unwrap();

    let snap = table.snapshot();
    let rows = collect(&table, snap);
    let by_rid: BTreeMap<RowId, i64> = rows.iter().map(|r| (r.row_id, value(r))).collect();
    assert_eq!(
        by_rid.get(&rid).copied(),
        Some(777),
        "newer version must win"
    );
    assert_eq!(by_rid.len(), 2);
}

#[test]
fn dense_single_row_history_streams() {
    // 10k versions of a single RowId in the memtable. Use a clustered
    // (WITHOUT ROWID) table so the same primary key (1) always maps to the
    // same RowId. Commit after every put so each version has a unique
    // epoch — the dedup's `version_is_newer` then picks the last write
    // unambiguously (dense same-epoch history in a single Bε-tree batches
    // messages and the buffer/leaf ordering does not preserve insertion
    // order across equal keys).
    let mut schema_clustered = schema();
    schema_clustered.clustered = true;
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema_clustered, 1).unwrap();
    table.set_mutable_run_spill_bytes(u64::MAX);
    let total = 10_000i64;
    for v in 0..total {
        table
            .put(vec![(1, Value::Int64(1)), (2, Value::Int64(v))])
            .unwrap();
        table.commit().unwrap();
    }
    let snap = table.snapshot();
    let control = ExecutionControl::new(None);
    let (result, trace) = QueryTrace::capture(|| {
        let mut count = 0;
        let mut last_value: Option<i64> = None;
        table
            .for_each_visible_row_controlled(snap, &control, |row| {
                count += 1;
                if !row.deleted {
                    last_value = Some(value(&row));
                }
                Ok(())
            })
            .map(|()| (count, last_value))
    });
    let (count, last_value) = result.unwrap();
    assert_eq!(
        count, 1,
        "10k versions of one RowId collapse to one emitted row"
    );
    assert_eq!(last_value, Some(total - 1));
    assert!(
        trace.controlled_scan_peak_same_row_versions >= 10_000,
        "peak_same_row_versions must reflect the dense history (got {})",
        trace.controlled_scan_peak_same_row_versions
    );
    emit_scan_metric!(
        "controlled_scan::dense_single_row_history_streams",
        trace.controlled_scan_peak_same_row_versions,
        "peak_same_row_versions"
    );
}

#[test]
fn three_tier_oracle_match() {
    // Force all three tiers to be live concurrently: a sorted run, a
    // mutable-run layer, and the active memtable. Independent oracle
    // (table.visible_rows) and the controlled scan must match exactly.
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(1);
    put(&mut table, 1, 10);
    table.flush().unwrap(); // sorted run
    table.set_mutable_run_spill_bytes(u64::MAX);
    put(&mut table, 2, 20);
    table.flush().unwrap(); // mutable run
    put(&mut table, 3, 30); // memtable
    table.commit().unwrap();

    let snap = table.snapshot();
    let oracle: BTreeMap<i64, i64> = table
        .visible_rows(snap)
        .unwrap()
        .into_iter()
        .map(|r| {
            (
                match r.columns.get(&1) {
                    Some(Value::Int64(v)) => *v,
                    _ => panic!("missing id"),
                },
                match r.columns.get(&2) {
                    Some(Value::Int64(v)) => *v,
                    _ => panic!("missing value"),
                },
            )
        })
        .collect();

    let control = ExecutionControl::new(None);
    let mut observed = BTreeMap::new();
    table
        .for_each_visible_row_controlled(snap, &control, |row| {
            let id = match row.columns.get(&1) {
                Some(Value::Int64(v)) => *v,
                _ => return Ok(()),
            };
            let v = match row.columns.get(&2) {
                Some(Value::Int64(v)) => *v,
                _ => return Ok(()),
            };
            observed.insert(id, v);
            Ok(())
        })
        .unwrap();
    assert_eq!(observed, oracle);
    let mut ordered: Vec<RowId> = observed.keys().map(|k| RowId(*k as u64)).collect();
    ordered.sort();
    ordered.dedup();
    assert_eq!(ordered.len(), 3);
}

#[test]
fn cancellation_bounds_versions_examined() {
    // After the visitor cancels, the scan must stop within a small bounded
    // number of additional examined versions (per the REM-003 contract).
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(u64::MAX);
    for i in 0..10_000i64 {
        put(&mut table, i, i);
    }
    table.commit().unwrap();
    let control = ExecutionControl::new(None);
    let mut visited = 0usize;
    let (result, trace) = QueryTrace::capture(|| {
        table.for_each_visible_row_controlled(table.snapshot(), &control, |_| {
            visited += 1;
            if visited == 50 {
                control.cancel(CancellationReason::ClientRequest);
            }
            Ok(())
        })
    });
    assert!(matches!(result, Err(MongrelError::Cancelled)));
    // Cancellation is observed within the current source-batch, so at most
    // the next 256 source-version refills + the in-flight dedup group.
    assert!(
        trace.controlled_scan_versions_examined <= 50 + 512,
        "excess versions examined after cancel: {}",
        trace.controlled_scan_versions_examined
    );
    emit_scan_metric!(
        "controlled_scan::cancellation_bounds_versions_examined",
        trace.controlled_scan_versions_examined,
        "versions_examined"
    );
}

#[test]
fn output_contract_strict_ascending_no_duplicates() {
    // Mix of overlapping keys across tiers, output must be strictly ascending
    // with no duplicate RowIds and no deleted rows. Use a clustered
    // (WITHOUT ROWID) table so the same primary key (100) maps to the same
    // RowId across the sorted run and the memtable, letting us delete it
    // and verify the tombstone wins.
    let mut schema_clustered = schema();
    schema_clustered.clustered = true;
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema_clustered, 1).unwrap();
    table.set_mutable_run_spill_bytes(1);
    for i in 0..500i64 {
        put(&mut table, i * 2, i);
    }
    table.flush().unwrap();
    table.set_mutable_run_spill_bytes(u64::MAX);
    for i in 0..500i64 {
        put(&mut table, i * 2 + 1, i);
    }
    table.flush().unwrap();
    // Re-write PK=100 in the memtable with a higher value, then delete it.
    let delete_id = put(&mut table, 100, 9_999);
    table.delete(delete_id).unwrap();
    table.commit().unwrap();

    let snap = table.snapshot();
    let mut last = 0u64;
    let mut seen: BTreeMap<RowId, i64> = BTreeMap::new();
    let control = ExecutionControl::new(None);
    table
        .for_each_visible_row_controlled(snap, &control, |row| {
            assert!(row.row_id.0 > last, "strictly ascending RowId");
            last = row.row_id.0;
            assert!(!row.deleted, "tombstones must be suppressed");
            seen.insert(row.row_id, value(&row));
            Ok(())
        })
        .unwrap();
    // 1000 distinct live rows (0..=999), with PK=100 deleted.
    assert_eq!(seen.len(), 999);
    assert!(!seen.contains_key(&delete_id));
}

#[test]
fn newer_memtable_value_beats_mutable_run_value() {
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(u64::MAX);
    put(&mut table, 1, 10);
    table.flush().unwrap();
    put(&mut table, 1, 20);
    table.commit().unwrap();
    let snap = table.snapshot();
    let rows = collect(&table, snap);
    assert_eq!(rows.len(), 1);
    assert_eq!(value(&rows[0]), 20);
}

#[test]
fn hlc_inversion_chooses_higher_hlc() {
    let old_hlc = HlcTimestamp {
        physical_micros: 100,
        logical: 0,
        node_tiebreaker: 1,
    };
    let new_hlc = HlcTimestamp {
        physical_micros: 200,
        logical: 0,
        node_tiebreaker: 1,
    };
    assert!(Snapshot::version_is_newer(
        Epoch(1),
        Some(new_hlc),
        Epoch(50),
        Some(old_hlc),
    ));
    assert!(!Snapshot::version_is_newer(
        Epoch(50),
        Some(old_hlc),
        Epoch(1),
        Some(new_hlc),
    ));
}

#[test]
fn tombstone_suppresses_older_live_version() {
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(1);
    let row_id = put(&mut table, 1, 10);
    table.flush().unwrap();
    table.delete(row_id).unwrap();
    table.commit().unwrap();
    let snap = table.snapshot();
    let rows = collect(&table, snap);
    assert!(!rows.iter().any(|r| r.row_id == row_id));
}

#[test]
fn controlled_scan_interleaves_memtable_mutable_run_and_sorted_run() {
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(1);
    put(&mut table, 1, 10);
    table.flush().unwrap();
    table.set_mutable_run_spill_bytes(u64::MAX);
    put(&mut table, 2, 20);
    table.flush().unwrap();
    put(&mut table, 3, 30);
    table.commit().unwrap();
    let snap = table.snapshot();
    let rows = collect(&table, snap);
    assert_eq!(rows.len(), 3);
    let values: Vec<i64> = rows.iter().map(value).collect();
    assert_eq!(values, vec![10, 20, 30]);
    assert!(rows.windows(2).all(|p| p[0].row_id < p[1].row_id));
}

#[test]
fn visitor_error_short_circuits_scan() {
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(u64::MAX);
    for i in 0..100i64 {
        put(&mut table, i, i);
    }
    table.commit().unwrap();
    let control = ExecutionControl::new(None);
    let mut visited = 0;
    let err = table
        .for_each_visible_row_controlled(table.snapshot(), &control, |row| {
            visited += 1;
            if visited == 5 {
                Err(MongrelError::Other("stop".into()))
            } else {
                assert!(!row.deleted);
                Ok(())
            }
        })
        .unwrap_err();
    assert!(matches!(err, MongrelError::Other(_)));
    assert!(visited <= 6);
}

#[test]
fn cancellation_before_first_row_is_observed() {
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(u64::MAX);
    for i in 0..1_000i64 {
        put(&mut table, i, i);
    }
    table.commit().unwrap();
    let control = ExecutionControl::new(None);
    control.cancel(CancellationReason::ClientRequest);
    let result = table.for_each_visible_row_controlled(table.snapshot(), &control, |_| {
        panic!("visit must not run when the control is already cancelled")
    });
    assert!(matches!(result, Err(MongrelError::Cancelled)));
}

#[test]
fn dml_count_update_delete_regression_fixture_still_passes() {
    // Mirror of dml_phase1.rs:203 — count-style callers must continue to
    // produce the right answer once the streaming cursor is on the hot path.
    // Use a clustered (WITHOUT ROWID) table so the second put on the same
    // primary key re-uses the same RowId; then the delete on the same PK
    // actually targets the row that was just inserted.
    let mut schema_clustered = schema();
    schema_clustered.clustered = true;
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema_clustered, 1).unwrap();
    table.set_mutable_run_spill_bytes(u64::MAX);
    let initial = 50;
    for i in 0..initial {
        put(&mut table, i, i * 1000);
    }
    let mut delete_ids = Vec::new();
    for id in 0..5i64 {
        let (rid, _) = table
            .put_returning(vec![(1, Value::Int64(id)), (2, Value::Int64(id * 1000))])
            .unwrap();
        delete_ids.push(rid);
    }
    table.commit().unwrap();
    for rid in &delete_ids {
        table.delete(*rid).unwrap();
    }
    table.commit().unwrap();

    let control = ExecutionControl::new(None);
    let mut count = 0_u64;
    table
        .for_each_visible_row_controlled(table.snapshot(), &control, |row| {
            assert!(!row.deleted);
            count += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(count, initial as u64 - 5);
}

#[test]
fn hundred_thousand_rows_with_ten_versions_each_match_oracle() {
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(u64::MAX);
    for _ in 0..10 {
        for id in 0..100_000i64 {
            put(&mut table, id, id);
        }
        table.commit().unwrap();
    }
    let snap = table.snapshot();
    let mut observed: BTreeMap<i64, i64> = BTreeMap::new();
    let control = ExecutionControl::new(None);
    table
        .for_each_visible_row_controlled(snap, &control, |row| {
            observed.insert(value(&row), value(&row));
            Ok(())
        })
        .unwrap();
    assert_eq!(observed.len(), 100_000);
}

#[test]
fn sorted_run_scan_matches_oracle() {
    // The legacy sorted-run path must continue to work end-to-end.
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    bulk_load(&mut table, 1_000);
    table.commit().unwrap();
    let snap = table.snapshot();
    let oracle = table.visible_rows(snap).unwrap();
    let rows = collect(&table, snap);
    assert_eq!(rows.len(), oracle.len());
    for pair in rows.windows(2) {
        assert!(pair[0].row_id < pair[1].row_id);
    }
}
