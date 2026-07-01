//! Integration tests for the gap-closure batch: schema evolution, doctor,
//! bulk-load, vectorized columnar scan, and TSV round-trip.

use mongreldb_core::columnar::NativeColumn;
use mongreldb_core::epoch::Epoch;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{tsv, NativeAgg, NativeAggResult, Snapshot, Table, Value};
use tempfile::tempdir;

fn schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
        }],
        indexes: Vec::new(),
        colocation: vec![], constraints: Default::default(),
    }
}

#[test]
fn bulk_load_then_vectorized_scan() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    let batch: Vec<Vec<(u16, Value)>> = (0..1000).map(|i| vec![(1, Value::Int64(i))]).collect();
    db.bulk_load(batch).unwrap();
    assert_eq!(db.run_count(), 1);
    assert_eq!(db.memtable_len(), 0);

    let snap = db.snapshot();
    let cols = db.visible_columns(snap).unwrap();
    let ids = cols.iter().find(|(c, _)| *c == 1).unwrap();
    assert_eq!(ids.1.len(), 1000);
    // Spot-check the column is the vectorized path's gathered values.
    assert_eq!(ids.1[0], Value::Int64(0));
    assert_eq!(ids.1[999], Value::Int64(999));
}

#[test]
fn schema_evolution_reads_old_rows_as_null() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.put(vec![(1, Value::Int64(1))]).unwrap();
    db.flush().unwrap();

    let new_id = db
        .add_column(
            "note",
            TypeId::Bytes,
            ColumnFlags::empty().with(ColumnFlags::NULLABLE),
        )
        .unwrap();
    assert!(new_id > 1);

    // Old run reads the new column as null.
    let cols = db.visible_columns(db.snapshot()).unwrap();
    let note = cols.iter().find(|(c, _)| *c == new_id).unwrap();
    assert_eq!(note.1, vec![Value::Null]);

    // A fresh write includes the new column.
    db.put(vec![
        (1, Value::Int64(2)),
        (new_id, Value::Bytes(b"hi".to_vec())),
    ])
    .unwrap();
    db.commit().unwrap();
    let cols = db.visible_columns(db.snapshot()).unwrap();
    let note = cols.iter().find(|(c, _)| *c == new_id).unwrap();
    assert!(note
        .1
        .iter()
        .any(|v| matches!(v, Value::Bytes(b) if b == b"hi")));
}

#[test]
fn doctor_drops_a_corrupt_run() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1); // force a spill per flush (exercises doctor)
    db.put(vec![(1, Value::Int64(1))]).unwrap();
    db.flush().unwrap();
    db.put(vec![(1, Value::Int64(2))]).unwrap();
    db.flush().unwrap();
    assert_eq!(db.run_count(), 2);

    // Corrupt one run.
    let live = dir.path().join("_runs").join("r-1.sr");
    let mut bytes = std::fs::read(&live).unwrap();
    bytes[300] ^= 0xFF;
    std::fs::write(&live, bytes).unwrap();

    assert!(
        db.check().unwrap().runs_ok < 2,
        "check must flag the corrupt run"
    );
    let report = db.doctor().unwrap();
    assert!(
        !report.runs_dropped.is_empty(),
        "doctor must drop the corrupt run"
    );
    // After repair, check passes cleanly on the survivors.
    assert!(db.check().unwrap().issues.is_empty());
}

#[test]
fn native_bulk_load_and_metadata_count() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    let n = 50_000usize;
    let id_col = NativeColumn::int64_sequence(0, n);
    let cols = vec![(1u16, id_col)];
    db.bulk_load_columns(cols).unwrap();
    // Metadata COUNT(*) is instant and exact.
    assert_eq!(db.count(), n as u64);
    // Native scan returns the full column.
    let snap = db.snapshot();
    let out = db.visible_columns_native(snap, None).unwrap();
    let len = out
        .iter()
        .find(|(c, _)| *c == 1)
        .map(|(_, c)| c.len())
        .unwrap();
    assert_eq!(len, n);
}

/// Phase 6.4: `column_native` decodes pages in parallel (rayon) when the run is
/// mmap-backed and spans more than one page. Verify a multi-page column round-
/// trips exactly through that parallel path.
#[test]
fn parallel_page_decode_round_trips_multipage_column() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    // 3 full 65 536-row pages + a partial fourth → exercises the parallel branch.
    let n = 65_536 * 3 + 12_345;
    let id_col = NativeColumn::int64_sequence(0, n);
    db.bulk_load_columns(vec![(1u16, id_col)]).unwrap();
    let snap = db.snapshot();
    let out = db.visible_columns_native(snap, None).unwrap();
    let col = out
        .iter()
        .find(|(c, _)| *c == 1)
        .map(|(_, c)| c)
        .expect("column 1 present");
    assert_eq!(col.len(), n);
    // Spot-check the parallel-decoded concatenation: first, a boundary, and last.
    match col {
        NativeColumn::Int64 { data, .. } => {
            assert_eq!(data[0], 0);
            assert_eq!(data[65_536], 65_536); // first row of page 2
            assert_eq!(data[65_536 * 2], 65_536 * 2); // first row of page 3
            assert_eq!(data[n - 1], (n - 1) as i64);
        }
        _ => panic!("expected Int64 column"),
    }
}

/// Phase 7.2: the native vectorized aggregate kernel computes Count/Sum/Min/Max/
/// Avg over a (filtered) Int64 column in one pass, skipping Arrow entirely.
#[test]
fn native_aggregate_matches_expected() {
    let dir = tempdir().unwrap();
    let schema = Schema {
        schema_id: 2,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 2,
                name: "v".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![IndexDef {
            name: "v_bitmap".into(),
            column_id: 2,
            kind: IndexKind::Bitmap,
        }],
        colocation: vec![], constraints: Default::default(),
    };
    let mut db = Table::create(dir.path(), schema, 1).unwrap();
    let n = 50_000i64;
    db.bulk_load(
        (0..n)
            .map(|i| vec![(1, Value::Int64(i)), (2, Value::Int64(i * 2))])
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let snap = db.snapshot();

    // Unfiltered: v = 0,2,4,...; sum = 2*(0+1+..+n-1) = (n-1)*n.
    let r = db
        .aggregate_native(snap, Some(2), &[], NativeAgg::Sum)
        .unwrap()
        .unwrap();
    assert!(matches!(r, NativeAggResult::Int(_)));
    assert_eq!(
        (n - 1) * n,
        match r {
            NativeAggResult::Int(x) => x,
            _ => unreachable!(),
        }
    );
    let mn = db
        .aggregate_native(snap, Some(2), &[], NativeAgg::Min)
        .unwrap()
        .unwrap();
    assert_eq!(
        match mn {
            NativeAggResult::Int(x) => x,
            _ => unreachable!(),
        },
        0
    );
    let mx = db
        .aggregate_native(snap, Some(2), &[], NativeAgg::Max)
        .unwrap()
        .unwrap();
    assert_eq!(
        match mx {
            NativeAggResult::Int(x) => x,
            _ => unreachable!(),
        },
        (n - 1) * 2
    );
    let c = db
        .aggregate_native(snap, None, &[], NativeAgg::Count)
        .unwrap()
        .unwrap();
    assert_eq!(
        match c {
            NativeAggResult::Count(x) => x,
            _ => unreachable!(),
        },
        n as u64
    );

    // Filtered: v < 1000 ⇒ v ∈ {0,2,..,998} ⇒ 500 rows.
    use mongreldb_core::Condition;
    let cond = Condition::Range {
        column_id: 2,
        lo: 0,
        hi: 999,
    };
    let cf = db
        .aggregate_native(snap, None, &[cond], NativeAgg::Count)
        .unwrap()
        .unwrap();
    assert_eq!(
        match cf {
            NativeAggResult::Count(x) => x,
            _ => unreachable!(),
        },
        500
    );
}

#[test]
fn tsv_round_trips_through_the_engine() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    let original = vec![
        vec![(1, Value::Int64(1))],
        vec![(1, Value::Int64(2))],
        vec![(1, Value::Int64(3))],
    ];
    db.put_batch(original.clone()).unwrap();
    db.commit().unwrap();

    let rows = db.visible_rows(db.snapshot()).unwrap();
    let exported = tsv::export_tsv(db.schema(), &rows);
    let imported = tsv::import_tsv(db.schema(), &exported).unwrap();
    assert_eq!(imported.len(), 3);

    // Re-ingest the imported rows into a fresh db and verify.
    let dir2 = tempdir().unwrap();
    let mut db2 = Table::create(dir2.path(), schema(), 1).unwrap();
    db2.put_batch(imported).unwrap();
    db2.commit().unwrap();
    let snap = Snapshot::at(Epoch(1));
    let n = db2.visible_rows(snap).unwrap().len();
    assert_eq!(n, 3);
}
