//! Crash-recovery robustness: an orphan run (written but never added to the
//! manifest, i.e. a crash mid-flush between the run write and the manifest
//! commit) is ignored, and a torn WAL tail (crash mid-append) is truncated.

use mongreldb_core::epoch::Epoch;
use mongreldb_core::memtable::Row;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::sorted_run::RunWriter;
use mongreldb_core::{RowId, Table, Value};
use std::fs::OpenOptions;
use std::io::Write;
use tempfile::tempdir;

fn schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "v".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
        }],
        indexes: Vec::new(),
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn row(id: i64) -> Row {
    Row::new(RowId(id as u64), Epoch(0)).with_column(1, Value::Int64(id))
}

#[test]
fn orphan_run_from_crash_mid_flush_is_ignored() {
    let dir = tempdir().unwrap();
    // Durable, manifest-referenced run with rows 1 and 2.
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.put(vec![(1, Value::Int64(1))]).unwrap();
    db.put(vec![(1, Value::Int64(2))]).unwrap();
    db.flush().unwrap();

    // Simulate a crash mid-flush: a second run file (rows 3) is written to disk
    // but the manifest was never updated to reference it.
    let orphan = dir.path().join("_runs").join("r-999.sr");
    RunWriter::new(db.schema(), 999, Epoch(5), 0)
        .write(&orphan, &[row(3)])
        .unwrap();
    assert!(orphan.exists(), "orphan run written");

    // Recovery must ignore the orphan run.
    let db = Table::open(dir.path()).unwrap();
    let rows = db.visible_rows(db.snapshot()).unwrap();
    let vals: Vec<i64> = rows
        .iter()
        .filter_map(|r| match r.columns.get(&1) {
            Some(Value::Int64(v)) => Some(*v),
            _ => None,
        })
        .collect();
    assert_eq!(vals, vec![1, 2], "orphan run must not appear in reads");
}

#[test]
fn torn_wal_tail_is_truncated_on_recovery() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut db = Table::create(&path, schema(), 1).unwrap();
        db.put(vec![(1, Value::Int64(7))]).unwrap();
        db.commit().unwrap(); // Put record fsynced into seg-000000.wal
    }
    // Simulate a crash mid-append: a partial record claiming a long length but
    // with only a few payload bytes.
    let wal = path.join("_wal").join("seg-000000.wal");
    {
        let mut f = OpenOptions::new().append(true).open(&wal).unwrap();
        f.write_all(&[0xff, 0xff, 0xff, 0x00]).unwrap(); // REC_LEN = a large value
        f.write_all(&[0u8; 3]).unwrap(); // only 3 bytes of the promised record
        f.sync_all().unwrap();
    }

    // Recovery replays the valid prefix and stops at the torn tail (no panic).
    let db = Table::open(&path).unwrap();
    let rows = db.visible_rows(db.snapshot()).unwrap();
    let vals: Vec<i64> = rows
        .iter()
        .filter_map(|r| match r.columns.get(&1) {
            Some(Value::Int64(v)) => Some(*v),
            _ => None,
        })
        .collect();
    assert_eq!(vals, vec![7], "valid record before the torn tail survives");
}
