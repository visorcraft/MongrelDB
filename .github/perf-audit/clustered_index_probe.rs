use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Table, Value};
use tempfile::tempdir;

#[test]
fn clustered_same_pk_update_moves_bitmap_and_preserves_count() {
    let dir = tempdir().unwrap();
    let schema = Schema {
        schema_id: 55,
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
                name: "bucket".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "bucket_bitmap".into(),
            column_id: 2,
            kind: IndexKind::Bitmap,
            predicate: None,
            options: Default::default(),
        }],
        clustered: true,
        ..Schema::default()
    };
    let mut table = Table::create(dir.path(), schema, 1).unwrap();
    let first = table
        .put(vec![(1, Value::Int64(7)), (2, Value::Int64(10))])
        .unwrap();
    table.commit().unwrap();
    let second = table
        .put(vec![(1, Value::Int64(7)), (2, Value::Int64(20))])
        .unwrap();
    table.commit().unwrap();
    assert_eq!(first, second, "clustered row id must be stable for the same PK");
    assert_eq!(table.count(), 1, "same-PK clustered upsert inflated live_count");

    let old = table
        .query(&Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: Value::Int64(10).encode_key(),
        }))
        .unwrap();
    assert!(old.is_empty(), "old bitmap key retained the clustered row");

    let new = table
        .query(&Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: Value::Int64(20).encode_key(),
        }))
        .unwrap();
    assert_eq!(new.len(), 1);
    assert_eq!(new[0].columns.get(&1), Some(&Value::Int64(7)));
}
