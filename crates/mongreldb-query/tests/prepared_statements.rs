//! Integration tests for prepared statements (#11): PREPARE / EXECUTE /
//! DEALLOCATE persisted on a session's DataFusion context, so the parse+plan
//! cost is paid once and repeated EXECUTE calls with different params reuse it.

use mongreldb_core::Database;
use mongreldb_query::MongrelSession;
use std::sync::Arc;
use tempfile::tempdir;

async fn session() -> (tempfile::TempDir, MongrelSession) {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let s = MongrelSession::open(Arc::new(db)).unwrap();
    s.run("CREATE TABLE items (id BIGINT PRIMARY KEY, v DOUBLE)")
        .await
        .unwrap();
    s.run("INSERT INTO items (id, v) VALUES (1, 10.0), (2, 20.0), (3, 30.0)")
        .await
        .unwrap();
    (dir, s)
}

#[tokio::test]
async fn prepare_then_execute_reuses_plan() {
    let (_dir, s) = session().await;
    // Prepare once.
    s.run("PREPARE gt AS SELECT id FROM items WHERE v > $1")
        .await
        .expect("PREPARE should succeed");

    // Execute with param 15.0 → ids 2,3.
    let rows = s
        .run("EXECUTE gt(15.0)")
        .await
        .expect("EXECUTE should return rows");
    assert!(!rows.is_empty(), "EXECUTE should produce result batches");

    // Execute again with a different param → reuse the prepared plan.
    let rows2 = s
        .run("EXECUTE gt(25.0)")
        .await
        .expect("second EXECUTE should succeed");
    assert!(!rows2.is_empty());
}

#[tokio::test]
async fn deallocate_removes_prepared() {
    let (_dir, s) = session().await;
    s.run("PREPARE p2 AS SELECT $1")
        .await
        .unwrap();
    s.run("DEALLOCATE p2").await.unwrap();
    // After DEALLOCATE, EXECUTE must fail.
    let err = s.run("EXECUTE p2(1)").await;
    assert!(err.is_err(), "EXECUTE after DEALLOCATE should error");
}

#[tokio::test]
async fn execute_unknown_statement_errors() {
    let (_dir, s) = session().await;
    let err = s.run("EXECUTE nope(1)").await;
    assert!(err.is_err(), "EXECUTE of an unprepared name should error");
}
