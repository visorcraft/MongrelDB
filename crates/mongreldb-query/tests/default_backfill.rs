use arrow::array::Int64Array;
use mongreldb_core::Database;
use mongreldb_query::MongrelSession;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn sql_set_not_null_backfills_existing_rows_from_default() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(db).unwrap();
    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY, score BIGINT)")
        .await
        .unwrap();
    session
        .run("INSERT INTO items (id) VALUES (1), (2)")
        .await
        .unwrap();
    session
        .run("ALTER TABLE items ALTER COLUMN score SET DEFAULT 7")
        .await
        .unwrap();
    session
        .run("ALTER TABLE items ALTER COLUMN score SET NOT NULL")
        .await
        .unwrap();

    let batches = session
        .run("SELECT score FROM items ORDER BY id")
        .await
        .unwrap();
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values(), &[7, 7]);
}
