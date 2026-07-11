use mongreldb_core::{ColumnDef, ColumnFlags, Database, Schema, TypeId, Value};
use tempfile::tempdir;

fn schema() -> Schema {
    Schema {
        schema_id: 0,
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

fn ids(db: &Database) -> Vec<i64> {
    let mut values = db
        .table("items")
        .unwrap()
        .lock()
        .visible_rows(db.snapshot().0)
        .unwrap()
        .into_iter()
        .filter_map(|row| match row.columns.get(&1) {
            Some(Value::Int64(value)) => Some(*value),
            _ => None,
        })
        .collect::<Vec<_>>();
    values.sort_unstable();
    values
}

#[test]
fn truncate_then_put_is_atomic_and_spill_recoverable() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    db.transaction(|transaction| {
        transaction.put("items", vec![(1, Value::Int64(1))])?;
        transaction.put("items", vec![(1, Value::Int64(2))])?;
        Ok(())
    })
    .unwrap();
    db.set_spill_threshold(1);

    let mut transaction = db.begin();
    transaction.truncate("items").unwrap();
    transaction
        .put("items", vec![(1, Value::Int64(3))])
        .unwrap();
    transaction
        .put("items", vec![(1, Value::Int64(4))])
        .unwrap();
    transaction.commit().unwrap();
    assert_eq!(ids(&db), vec![3, 4]);
    drop(db);

    let reopened = Database::open(dir.path()).unwrap();
    assert_eq!(ids(&reopened), vec![3, 4]);
}

#[test]
fn failed_replace_keeps_previous_rows() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    db.transaction(|transaction| transaction.put("items", vec![(1, Value::Int64(1))]))
        .unwrap();

    let mut transaction = db.begin();
    transaction.truncate("items").unwrap();
    transaction.put("items", vec![(1, Value::Null)]).unwrap();
    assert!(transaction.commit().is_err());
    assert_eq!(ids(&db), vec![1]);
}
