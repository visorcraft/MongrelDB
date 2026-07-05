//! WITHOUT ROWID: clustered primary key tables derive RowId from the PK value.

use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema as CoreSchema, TypeId};
use mongreldb_core::{Table, Value};
use tempfile::tempdir;

fn clustered_schema() -> CoreSchema {
    CoreSchema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
        }],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: true,
    }
}

#[test]
fn clustered_put_is_idempotent() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), clustered_schema(), 1).unwrap();
    // Same PK value → same RowId on every put.
    let (row_id_1, _) = table.put_returning(vec![(1, Value::Int64(42))]).unwrap();
    let (row_id_2, _) = table.put_returning(vec![(1, Value::Int64(42))]).unwrap();
    assert_eq!(
        row_id_1, row_id_2,
        "clustered table: same PK must produce the same RowId (idempotent upsert)"
    );
    // Different PK value → different RowId.
    let (row_id_3, _) = table.put_returning(vec![(1, Value::Int64(99))]).unwrap();
    assert_ne!(
        row_id_1, row_id_3,
        "different PKs must produce different RowIds"
    );
}

#[test]
fn clustered_table_stores_and_retrieves_data() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), clustered_schema(), 1).unwrap();
    let (row_id, _) = table.put_returning(vec![(1, Value::Int64(7))]).unwrap();
    table.flush().unwrap();
    let snap = table.snapshot();
    let row = table.get(row_id, snap);
    assert!(row.is_some(), "clustered table row should be retrievable");
    assert_eq!(
        row.unwrap().columns.get(&1),
        Some(&Value::Int64(7)),
        "clustered table should store the correct value"
    );
}
