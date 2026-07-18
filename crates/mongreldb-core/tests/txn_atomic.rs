//! P2.5 — atomic cross-table transactions on the shared WAL.

use mongreldb_core::{
    schema::*, CancellationReason, Database, ExecutionControl, MongrelError, RowId, Value,
};
use std::sync::atomic::{AtomicBool, Ordering};
use tempfile::tempdir;

fn one_int_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "v".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
            embedding_source: None,
        }],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

#[test]
fn cross_table_txn_is_all_or_nothing() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("a", one_int_schema()).unwrap();
    db.create_table("b", one_int_schema()).unwrap();

    db.transaction(|t| {
        t.put("a", vec![(1, Value::Int64(1))])?;
        t.put("b", vec![(1, Value::Int64(2))])?;
        Ok(())
    })
    .unwrap();

    assert_eq!(db.table("a").unwrap().lock().count(), 1);
    assert_eq!(db.table("b").unwrap().lock().count(), 1);

    // A rolled-back txn writes nothing.
    let _: Result<(), _> = db.transaction(|t| {
        t.put("a", vec![(1, Value::Int64(9))])?;
        Err(MongrelError::Other("boom".into()))
    });
    assert_eq!(db.table("a").unwrap().lock().count(), 1);
}

#[test]
fn manifest_failure_still_publishes_every_table_and_recovers() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let a_id = db.create_table("a", one_int_schema()).unwrap();
    db.create_table("b", one_int_schema()).unwrap();
    let manifest = dir.path().join("tables").join(a_id.to_string()).join("_mf");
    let saved_manifest = manifest.with_extension("saved");
    std::fs::rename(&manifest, &saved_manifest).unwrap();
    std::fs::create_dir(&manifest).unwrap();

    let error = db
        .transaction(|t| {
            t.put("a", vec![(1, Value::Int64(1))])?;
            t.put("b", vec![(1, Value::Int64(2))])?;
            Ok(())
        })
        .unwrap_err();
    let epoch = match error {
        MongrelError::DurableCommit { epoch, .. } => epoch,
        other => panic!("expected durable commit error, got {other:?}"),
    };
    assert_eq!(db.visible_epoch().0, epoch);
    assert_eq!(db.table("a").unwrap().lock().count(), 1);
    assert_eq!(db.table("b").unwrap().lock().count(), 1);
    assert!(db
        .ensure_consistent_read()
        .unwrap_err()
        .to_string()
        .contains("reopen required"));

    drop(db);
    std::fs::remove_dir(&manifest).unwrap();
    std::fs::rename(&saved_manifest, &manifest).unwrap();
    let reopened = Database::open(dir.path()).unwrap();
    assert_eq!(reopened.visible_epoch().0, epoch);
    assert_eq!(reopened.table("a").unwrap().lock().count(), 1);
    assert_eq!(reopened.table("b").unwrap().lock().count(), 1);
}

#[test]
fn txn_delete_and_put_in_one_commit() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("a", one_int_schema()).unwrap();
    db.transaction(|t| {
        t.put("a", vec![(1, Value::Int64(1))])?;
        t.put("a", vec![(1, Value::Int64(2))])?;
        Ok(())
    })
    .unwrap();
    assert_eq!(db.table("a").unwrap().lock().count(), 2);
    // delete the first row (RowId(0), the first allocated by a fresh table).
    db.transaction(|t| {
        t.delete("a", RowId(0))?;
        Ok(())
    })
    .unwrap();
    assert_eq!(db.table("a").unwrap().lock().count(), 1);
}

#[test]
fn controlled_commit_cancellation_wins_before_fence() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("a", one_int_schema()).unwrap();
    let initial_epoch = db.visible_epoch();
    let control = ExecutionControl::new(None);
    control.cancel(CancellationReason::ClientRequest);
    let callback_called = AtomicBool::new(false);
    let mut tx = db.begin();
    tx.put("a", vec![(1, Value::Int64(1))]).unwrap();

    let error = tx
        .commit_controlled(&control, || {
            callback_called.store(true, Ordering::Relaxed);
            Ok(())
        })
        .unwrap_err();

    assert!(matches!(error, MongrelError::Cancelled));
    assert!(!callback_called.load(Ordering::Relaxed));
    assert_eq!(db.visible_epoch(), initial_epoch);
    assert_eq!(db.table("a").unwrap().lock().count(), 0);
}

#[test]
fn controlled_commit_callback_runs_before_any_wal_append() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("a", one_int_schema()).unwrap();
    let initial_epoch = db.visible_epoch();
    let control = ExecutionControl::new(None);
    let callback_called = AtomicBool::new(false);
    let mut tx = db.begin();
    tx.put("a", vec![(1, Value::Int64(1))]).unwrap();

    let error = tx
        .commit_controlled(&control, || {
            callback_called.store(true, Ordering::Relaxed);
            Err(MongrelError::Cancelled)
        })
        .unwrap_err();

    assert!(matches!(error, MongrelError::Cancelled));
    assert!(callback_called.load(Ordering::Relaxed));
    assert_eq!(db.visible_epoch(), initial_epoch);
    assert_eq!(db.table("a").unwrap().lock().count(), 0);

    drop(db);
    let reopened = Database::open(dir.path()).unwrap();
    assert_eq!(reopened.visible_epoch(), initial_epoch);
    assert_eq!(reopened.table("a").unwrap().lock().count(), 0);
}
