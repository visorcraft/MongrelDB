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

#[test]
fn durable_change_events_resume_with_stable_ids() {
    let dir = tempdir().unwrap();
    let expected_ids = {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("items", schema()).unwrap();
        let mut commit_wake = db.subscribe_change_commits();
        db.transaction(|transaction| {
            transaction.put("items", vec![(1, Value::Int64(1))])?;
            transaction.put("items", vec![(1, Value::Int64(2))])?;
            Ok(())
        })
        .unwrap();
        assert!(commit_wake.try_recv().is_ok());

        let batch = db.change_events_since(None).unwrap();
        assert!(!batch.gap);
        let changes: Vec<_> = batch
            .events
            .into_iter()
            .filter(|event| event.op == "put")
            .collect();
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].table, "items");
        assert_eq!(changes[0].epoch, changes[1].epoch);
        assert!(changes[0].data.is_some());
        let ids: Vec<String> = changes
            .iter()
            .map(|event| event.id.clone().unwrap())
            .collect();

        let resumed = db.change_events_since(Some(&ids[0])).unwrap();
        let resumed_ids: Vec<_> = resumed
            .events
            .into_iter()
            .filter_map(|event| event.id)
            .collect();
        assert_eq!(resumed_ids, vec![ids[1].clone()]);
        ids
    };

    let db = Database::open(dir.path()).unwrap();
    let replayed_ids: Vec<_> = db
        .change_events_since(None)
        .unwrap()
        .events
        .into_iter()
        .filter(|event| event.op == "put")
        .filter_map(|event| event.id)
        .collect();
    assert_eq!(replayed_ids, expected_ids);
}

#[test]
fn aborted_transactions_never_enter_cdc() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    let mut transaction = db.begin();
    transaction
        .put("items", vec![(1, Value::Int64(1))])
        .unwrap();
    transaction.rollback();

    assert!(db
        .change_events_since(None)
        .unwrap()
        .events
        .iter()
        .all(|event| event.op != "put"));
}

#[test]
fn malformed_resume_id_is_rejected() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    assert!(db.change_events_since(Some("bad-id")).is_err());
}

#[test]
fn resume_before_retained_wal_reports_gap() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.set_replication_wal_retention_segments(1);
    db.create_table("items", schema()).unwrap();
    db.transaction(|transaction| {
        transaction.put("items", vec![(1, Value::Int64(1))])?;
        Ok(())
    })
    .unwrap();
    let first_id = db
        .change_events_since(None)
        .unwrap()
        .events
        .last()
        .unwrap()
        .id
        .clone()
        .unwrap();
    db.checkpoint().unwrap();

    for id in 2..=5 {
        db.transaction(|transaction| {
            transaction.put("items", vec![(1, Value::Int64(id))])?;
            Ok(())
        })
        .unwrap();
        db.checkpoint().unwrap();
    }

    let resumed = db.change_events_since(Some(&first_id)).unwrap();
    assert!(resumed.gap);
}

#[test]
fn spilled_transaction_cdc_includes_rows() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    db.set_spill_threshold(1);
    db.transaction(|transaction| {
        transaction.put("items", vec![(1, Value::Int64(7))])?;
        Ok(())
    })
    .unwrap();

    let batch = db.change_events_since(None).unwrap();
    assert!(!batch.gap);
    let event = batch
        .events
        .iter()
        .find(|event| event.op == "put_run")
        .unwrap();
    assert_eq!(
        event.data.as_ref().unwrap()["rows"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn delete_cdc_carries_durable_before_image() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    db.transaction(|transaction| transaction.put("items", vec![(1, Value::Int64(9))]))
        .unwrap();
    let row_id = db
        .table("items")
        .unwrap()
        .lock()
        .visible_rows(db.snapshot().0)
        .unwrap()[0]
        .row_id;
    db.transaction(|transaction| transaction.delete("items", row_id))
        .unwrap();

    let delete = db
        .change_events_since(None)
        .unwrap()
        .events
        .into_iter()
        .find(|event| event.op == "delete")
        .unwrap();
    let before: Vec<mongreldb_core::Row> =
        serde_json::from_value(delete.data.unwrap()["before"].clone()).unwrap();
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].columns.get(&1), Some(&Value::Int64(9)));
}
