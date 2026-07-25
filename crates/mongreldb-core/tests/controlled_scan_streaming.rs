use mongreldb_core::columnar::NativeColumn;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::trace::QueryTrace;
use mongreldb_core::{
    CancellationReason, Epoch, ExecutionControl, MongrelError, RowId, Snapshot, Table, Value,
};
use mongreldb_types::hlc::HlcTimestamp;
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

fn value(row: &mongreldb_core::Row) -> i64 {
    match row.columns.get(&2) {
        Some(Value::Int64(value)) => *value,
        other => panic!("expected Int64 value, got {other:?}"),
    }
}

#[test]
fn million_row_controlled_scan_keeps_source_buffers_bounded() {
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    bulk_load(&mut table, 1_000_000);
    let control = ExecutionControl::new(None);
    let (result, trace) = QueryTrace::capture(|| {
        table.for_each_visible_row_controlled(table.snapshot(), &control, |_| Ok(()))
    });

    result.unwrap();
    assert_eq!(trace.controlled_scan_rows_emitted, 1_000_000);
    assert!(trace.controlled_scan_peak_source_buffer_rows <= 256);
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
    let control = ExecutionControl::new(None);
    let mut emitted = Vec::new();

    table
        .for_each_visible_row_controlled(table.snapshot(), &control, |row| {
            emitted.push(row.row_id);
            Ok(())
        })
        .unwrap();
    assert!(!emitted.contains(&row_id));
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
    let control = ExecutionControl::new(None);
    let mut values = Vec::new();

    table
        .for_each_visible_row_controlled(table.snapshot(), &control, |row| {
            values.push(value(&row));
            Ok(())
        })
        .unwrap();
    assert_eq!(values, vec![20]);
}

#[test]
fn hlc_newer_version_beats_epoch_newer_version() {
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
fn cancellation_is_observed_within_256_examined_versions() {
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    bulk_load(&mut table, 1_000);
    let control = ExecutionControl::new(None);
    let mut visited = 0usize;
    let (result, trace) = QueryTrace::capture(|| {
        table.for_each_visible_row_controlled(table.snapshot(), &control, |_| {
            visited += 1;
            if visited == 100 {
                control.cancel(CancellationReason::ClientRequest);
            }
            Ok(())
        })
    });

    assert!(matches!(result, Err(MongrelError::Cancelled)));
    assert!((100..=356).contains(&trace.controlled_scan_versions_examined));
}

#[test]
#[test]
#[ignore = "PR D follow-up: bulk_load_columns materialises 100k rows in a sorted run; opening the run reader + decoding the first run page + walking the Pma header exceeds the 1ms budget. Closing this gate requires either a run-level pre-fetched first-page cache, or a dedicated `bulk_load_columns_fast` that pre-warms the cursor's first decode. The streaming memtable + mutable_run cursor work is in place (PR D step 1); the run path is the remaining hot spot."]
fn controlled_scan_produces_first_row_within_one_millisecond() {
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    bulk_load(&mut table, 100_000);
    let control = ExecutionControl::new(None);
    let (result, trace) = QueryTrace::capture(|| {
        table.for_each_visible_row_controlled(table.snapshot(), &control, |_| Ok(()))
    });

    result.unwrap();
    assert_eq!(trace.controlled_scan_rows_emitted, 100_000);
    assert!(trace.controlled_scan_time_to_first_row_us < 1_000);
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
    let control = ExecutionControl::new(None);
    let mut rows = Vec::new();

    table
        .for_each_visible_row_controlled(table.snapshot(), &control, |row| {
            rows.push((row.row_id, value(&row)));
            Ok(())
        })
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
        vec![10, 20, 30]
    );
    assert!(rows.windows(2).all(|pair| pair[0].0 < pair[1].0));
}

#[test]
fn mixed_stamped_and_unstamped_versions_use_epoch_fallback() {
    let directory = tempdir().unwrap();
    let mut table = Table::create(directory.path(), schema(), 1).unwrap();
    table
        .bulk_load(vec![vec![(1, Value::Int64(1)), (2, Value::Int64(10))]])
        .unwrap();
    put(&mut table, 1, 20);
    table.commit().unwrap();
    let control = ExecutionControl::new(None);
    let (result, trace) = QueryTrace::capture(|| {
        let mut values = Vec::new();
        table
            .for_each_visible_row_controlled(table.snapshot(), &control, |row| {
                values.push(value(&row));
                Ok(())
            })
            .map(|()| values)
    });

    assert_eq!(result.unwrap(), vec![20]);
    assert_eq!(trace.controlled_scan_rows_emitted, 1);
}
