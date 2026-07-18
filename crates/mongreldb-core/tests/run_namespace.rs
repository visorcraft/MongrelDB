use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{MongrelError, Table, Value};
use tempfile::tempdir;

fn schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
            embedding_source: None,
        }],
        ..Schema::default()
    }
}

#[test]
fn reopen_reserves_canonical_orphan_run_ids() {
    let dir = tempdir().unwrap();
    drop(Table::create(dir.path(), schema(), 1).unwrap());
    std::fs::write(dir.path().join("_runs/r-99.sr"), b"orphan").unwrap();

    let mut table = Table::open(dir.path()).unwrap();
    table.set_mutable_run_spill_bytes(1);
    table.put(vec![(1, Value::Int64(1))]).unwrap();
    table.flush().unwrap();

    assert!(dir.path().join("_runs/r-100.sr").is_file());
}

#[test]
fn maximum_on_disk_run_id_exhausts_namespace_without_wrap() {
    let dir = tempdir().unwrap();
    drop(Table::create(dir.path(), schema(), 1).unwrap());
    std::fs::write(
        dir.path().join(format!("_runs/r-{}.sr", u64::MAX)),
        b"orphan",
    )
    .unwrap();

    assert!(matches!(
        Table::open(dir.path()),
        Err(MongrelError::Full(_))
    ));
}
