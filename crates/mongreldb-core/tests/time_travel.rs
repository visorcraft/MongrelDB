use mongreldb_core::{ColumnDef, ColumnFlags, Database, Epoch, Schema, TypeId, Value};
use tempfile::tempdir;

fn schema() -> Schema {
    Schema {
        schema_id: 0,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
            },
            ColumnDef {
                id: 2,
                name: "value".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

fn put(db: &Database, value: i64) -> Epoch {
    db.transaction(|transaction| {
        transaction.put(
            "items",
            vec![(1, Value::Int64(1)), (2, Value::Int64(value))],
        )?;
        Ok(())
    })
    .unwrap();
    db.visible_epoch()
}

fn value_at(db: &Database, epoch: Epoch) -> i64 {
    let (snapshot, _guard) = db.snapshot_at_owned(epoch).unwrap();
    let rows = db
        .table("items")
        .unwrap()
        .lock()
        .visible_rows(snapshot)
        .unwrap();
    assert_eq!(rows.len(), 1);
    match rows[0].columns.get(&2) {
        Some(Value::Int64(value)) => *value,
        other => panic!("unexpected value: {other:?}"),
    }
}

#[test]
fn retained_epochs_survive_compaction() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    db.set_history_retention_epochs(100).unwrap();
    db.table("items")
        .unwrap()
        .lock()
        .set_mutable_run_spill_bytes(1);

    let first = put(&db, 10);
    db.checkpoint().unwrap();
    let second = put(&db, 20);
    db.checkpoint().unwrap();
    let third = put(&db, 30);
    db.checkpoint().unwrap();

    db.compact().unwrap();
    assert_eq!(value_at(&db, first), 10);
    assert_eq!(value_at(&db, second), 20);
    assert_eq!(value_at(&db, third), 30);
    assert_eq!(db.table("items").unwrap().lock().count(), 1);
}

#[test]
fn retention_floor_rejects_expired_and_future_epochs() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    db.set_history_retention_epochs(2).unwrap();
    let old = put(&db, 1);
    put(&db, 2);
    put(&db, 3);
    put(&db, 4);

    let earliest = db.earliest_retained_epoch();
    assert!(earliest > old);
    let error = db.snapshot_at_owned(old).err().unwrap();
    assert!(error.to_string().contains("no longer retained"));
    assert!(db
        .snapshot_at_owned(Epoch(db.visible_epoch().0 + 1))
        .is_err());
}

#[test]
fn retention_configuration_persists_and_cannot_restore_lost_history() {
    let dir = tempdir().unwrap();
    let earliest = {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("items", schema()).unwrap();
        put(&db, 1);
        db.set_history_retention_epochs(5).unwrap();
        let earliest = db.earliest_retained_epoch();
        assert_eq!(earliest, db.visible_epoch());
        earliest
    };

    let db = Database::open(dir.path()).unwrap();
    assert_eq!(db.history_retention_epochs(), 5);
    assert_eq!(db.earliest_retained_epoch(), earliest);
    db.set_history_retention_epochs(50).unwrap();
    assert_eq!(db.earliest_retained_epoch(), earliest);
}
