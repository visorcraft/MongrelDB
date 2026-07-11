use mongreldb_core::{Database, Value};
use mongreldb_query::MongrelSession;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::tempdir;

fn total_rows(batches: &[arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|batch| batch.num_rows()).sum()
}

#[tokio::test]
async fn create_table_ttl_clause_controls_sql_visibility() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run(
            "CREATE TABLE events (id BIGINT PRIMARY KEY, created_at TIMESTAMP) \
             TTL_COLUMN created_at TTL '7 days'",
        )
        .await
        .unwrap();

    let policy = db.table("events").unwrap().lock().ttl().unwrap();
    assert_eq!(policy.column_id, 2);
    assert_eq!(policy.duration_nanos, 604_800_000_000_000);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    {
        let handle = db.table("events").unwrap();
        let mut table = handle.lock();
        table
            .put(vec![
                (1, Value::Int64(1)),
                (2, Value::Int64(now - 8 * 86_400_000_000_000)),
            ])
            .unwrap();
        table
            .put(vec![(1, Value::Int64(2)), (2, Value::Int64(now))])
            .unwrap();
        table.commit().unwrap();
    }

    let rows = session
        .run("SELECT id FROM events ORDER BY id")
        .await
        .unwrap();
    assert_eq!(total_rows(&rows), 1);
}

#[tokio::test]
async fn invalid_ttl_clause_does_not_create_table() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    let error = session
        .run("CREATE TABLE bad (id BIGINT PRIMARY KEY) TTL_COLUMN id TTL '1 day'")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("must be TIMESTAMP"));
    assert!(db.table_names().is_empty());
}
