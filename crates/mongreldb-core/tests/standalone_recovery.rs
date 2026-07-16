use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{Table, Value};
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
        }],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

#[test]
fn committed_unflushed_rows_survive_repeated_reopen() {
    let dir = tempdir().unwrap();
    {
        let mut table = Table::create(dir.path(), schema(), 1).unwrap();
        table.put(vec![(1, Value::Int64(7))]).unwrap();
        table.commit().unwrap();
    }

    for _ in 0..3 {
        let table = Table::open(dir.path()).unwrap();
        assert_eq!(table.count(), 1);
    }
}

#[test]
fn durable_truncate_survives_repeated_reopen() {
    let dir = tempdir().unwrap();
    {
        let mut table = Table::create(dir.path(), schema(), 1).unwrap();
        table.set_mutable_run_spill_bytes(1);
        table.put(vec![(1, Value::Int64(7))]).unwrap();
        table.flush().unwrap();
        assert_eq!(table.count(), 1);
        table.truncate().unwrap();
        table.commit().unwrap();
        assert_eq!(table.count(), 0);
    }

    for _ in 0..3 {
        let table = Table::open(dir.path()).unwrap();
        assert_eq!(table.count(), 0);
    }
}
