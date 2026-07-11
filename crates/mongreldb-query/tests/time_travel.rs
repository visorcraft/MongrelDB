use arrow::array::Int64Array;
use futures::StreamExt;
use mongreldb_core::{Database, Epoch, Value};
use mongreldb_query::MongrelSession;
use std::sync::Arc;
use tempfile::tempdir;

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

#[tokio::test]
async fn sql_as_of_epoch_uses_retained_full_scan() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY, value BIGINT)")
        .await
        .unwrap();
    db.set_history_retention_epochs(100).unwrap();
    db.table("items")
        .unwrap()
        .lock()
        .set_mutable_run_spill_bytes(1);

    let first = put(&db, 10);
    db.checkpoint().unwrap();
    put(&db, 20);
    db.checkpoint().unwrap();
    db.compact().unwrap();

    let batches = session
        .run(&format!(
            "SELECT value FROM items AS OF EPOCH {} WHERE value = 10",
            first.0
        ))
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.value(0), 10);

    let aliased = session
        .run(&format!(
            "SELECT old.value FROM items AS OF EPOCH {} AS old",
            first.0
        ))
        .await
        .unwrap();
    assert_eq!(aliased[0].num_rows(), 1);
}

#[tokio::test]
async fn streaming_as_of_holds_snapshot_until_stream_drop() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY, value BIGINT)")
        .await
        .unwrap();
    db.set_history_retention_epochs(10).unwrap();
    let first = put(&db, 10);
    put(&db, 20);

    let mut stream = session
        .run_stream(&format!("SELECT value FROM items AS OF EPOCH {}", first.0))
        .await
        .unwrap();
    let batch = stream.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn sql_as_of_rejects_epoch_outside_window() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY, value BIGINT)")
        .await
        .unwrap();
    db.set_history_retention_epochs(1).unwrap();
    let old = put(&db, 1);
    put(&db, 2);
    put(&db, 3);

    let error = session
        .run(&format!("SELECT * FROM items AS OF EPOCH {}", old.0))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("no longer retained"));
}
