use arrow::array::Int64Array;
use futures::StreamExt;
use mongreldb_core::{ColumnDef, ColumnFlags, Database, Schema, TypeId};
use mongreldb_query::{MongrelQueryError, MongrelSession, QueryId, SqlQueryOptions, SqlQueryPhase};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn session() -> (tempfile::TempDir, MongrelSession) {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::create(dir.path()).unwrap();
    database
        .create_table(
            "items",
            Schema {
                columns: vec![ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                    default_value: None,
                }],
                ..Schema::default()
            },
        )
        .unwrap();
    (dir, MongrelSession::open(Arc::new(database)).unwrap())
}

async fn ids(session: &MongrelSession) -> Vec<i64> {
    let batches = session
        .run("SELECT id FROM items ORDER BY id")
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..values.len()).map(|index| values.value(index))
        })
        .collect()
}

#[tokio::test]
async fn rollback_to_handles_empty_and_staged_transactions_and_retains_target() {
    let (_dir, session) = session();
    session.run("/* transaction */ BEGIN").await.unwrap();
    session
        .run("-- before savepoint\nSAVEPOINT empty")
        .await
        .unwrap();
    session.run("ROLLBACK TO empty").await.unwrap();
    session
        .run("INSERT INTO items (id) VALUES (1)")
        .await
        .unwrap();
    session.run("SAVEPOINT sp1").await.unwrap();
    session
        .run("INSERT INTO items (id) VALUES (2)")
        .await
        .unwrap();
    session.run("ROLLBACK TO sp1").await.unwrap();
    session
        .run("INSERT INTO items (id) VALUES (3)")
        .await
        .unwrap();
    session.run("ROLLBACK TO SAVEPOINT sp1").await.unwrap();
    session
        .run("INSERT INTO items (id) VALUES (4)")
        .await
        .unwrap();
    session.run("COMMIT").await.unwrap();

    assert_eq!(ids(&session).await, vec![1, 4]);
}

#[tokio::test]
async fn nested_release_removes_target_and_nested_savepoints_only() {
    let (_dir, session) = session();
    session.run("BEGIN").await.unwrap();
    session
        .run("INSERT INTO items (id) VALUES (1)")
        .await
        .unwrap();
    session.run("SAVEPOINT outer_sp").await.unwrap();
    session
        .run("INSERT INTO items (id) VALUES (2)")
        .await
        .unwrap();
    session.run("SAVEPOINT inner_sp").await.unwrap();
    session
        .run("INSERT INTO items (id) VALUES (3)")
        .await
        .unwrap();
    session.run("RELEASE SAVEPOINT outer_sp").await.unwrap();

    assert_eq!(session.staged_sql_operation_count(), Some(3));
    assert!(matches!(
        session.run("ROLLBACK TO inner_sp").await,
        Err(MongrelQueryError::SavepointNotFound { name }) if name == "inner_sp"
    ));
    session.run("ROLLBACK WORK").await.unwrap();
    assert!(ids(&session).await.is_empty());
}

#[tokio::test]
async fn savepoint_errors_are_typed_and_recoverable() {
    let (_dir, session) = session();
    for sql in ["SAVEPOINT sp", "RELEASE sp", "ROLLBACK TO sp"] {
        assert!(matches!(
            session.run(sql).await,
            Err(MongrelQueryError::NoSqlTransaction)
        ));
    }

    session.run("BEGIN").await.unwrap();
    session.run("SAVEPOINT sp").await.unwrap();
    assert!(matches!(
        session.run("RELEASE missing").await,
        Err(MongrelQueryError::SavepointNotFound { name }) if name == "missing"
    ));
    session.run("ROLLBACK TO sp").await.unwrap();
    session.run("RELEASE sp").await.unwrap();
    assert!(matches!(
        session.run("ROLLBACK TO sp").await,
        Err(MongrelQueryError::SavepointNotFound { name }) if name == "sp"
    ));
    session.run("ROLLBACK").await.unwrap();
}

#[tokio::test]
async fn rollback_aliases_distinguish_savepoint_and_full_rollback() {
    let (_dir, session) = session();
    for (base, rollback) in [
        (1, "/* alias */ ROLLBACK WORK TO SAVEPOINT sp"),
        (3, "ROLLBACK TRANSACTION TO sp"),
        (5, "ROLLBACK TRAN TO SAVEPOINT sp"),
    ] {
        session.run("BEGIN").await.unwrap();
        session
            .run(&format!("INSERT INTO items (id) VALUES ({base})"))
            .await
            .unwrap();
        session.run("SAVEPOINT sp").await.unwrap();
        session
            .run(&format!("INSERT INTO items (id) VALUES ({})", base + 1))
            .await
            .unwrap();
        session.run(rollback).await.unwrap();
        session.run("COMMIT").await.unwrap();
    }

    session.run("BEGIN").await.unwrap();
    session
        .run("INSERT INTO items (id) VALUES (7)")
        .await
        .unwrap();
    session
        .run("-- full rollback\nROLLBACK TRANSACTION")
        .await
        .unwrap();
    assert_eq!(ids(&session).await, vec![1, 3, 5]);
}

#[tokio::test]
async fn rollback_to_recovers_failed_multi_statement_transaction() {
    let (_dir, session) = session();
    session.run("BEGIN").await.unwrap();
    session
        .run("INSERT INTO items (id) VALUES (1)")
        .await
        .unwrap();
    session.run("SAVEPOINT stable").await.unwrap();

    assert!(session
        .run("INSERT INTO items (id) VALUES (2); SELECT * FROM missing_table")
        .await
        .is_err());
    assert!(matches!(
        session.run("COMMIT").await,
        Err(MongrelQueryError::TransactionAborted)
    ));
    session
        .run("ROLLBACK TO stable; INSERT INTO items (id) VALUES (3)")
        .await
        .unwrap();
    session.run("COMMIT").await.unwrap();

    assert_eq!(ids(&session).await, vec![1, 3]);
}

#[tokio::test]
async fn rollback_to_recovers_parse_and_commit_constraint_failures() {
    let (_dir, session) = session();
    session.run("BEGIN").await.unwrap();
    session
        .run("INSERT INTO items (id) VALUES (1)")
        .await
        .unwrap();
    session.run("SAVEPOINT parsed").await.unwrap();
    assert!(session.run("SELECT (").await.is_err());
    assert!(matches!(
        session.run("COMMIT").await,
        Err(MongrelQueryError::TransactionAborted)
    ));
    session.run("ROLLBACK TO parsed").await.unwrap();
    session
        .run("INSERT INTO items (id) VALUES (2)")
        .await
        .unwrap();
    session.run("COMMIT").await.unwrap();

    session
        .run("CREATE TABLE checked_items (id BIGINT PRIMARY KEY, value BIGINT CHECK (value > 0))")
        .await
        .unwrap();
    session.run("BEGIN").await.unwrap();
    session
        .run("INSERT INTO checked_items (id, value) VALUES (3, 3)")
        .await
        .unwrap();
    session.run("SAVEPOINT before_invalid").await.unwrap();
    session
        .run("INSERT INTO checked_items (id, value) VALUES (9, -1)")
        .await
        .unwrap();
    assert!(session.run("COMMIT").await.is_err());
    assert!(matches!(
        session
            .run("INSERT INTO checked_items (id, value) VALUES (99, 99)")
            .await,
        Err(MongrelQueryError::TransactionAborted)
    ));
    session.run("ROLLBACK TO before_invalid").await.unwrap();
    session
        .run("INSERT INTO checked_items (id, value) VALUES (4, 4)")
        .await
        .unwrap();
    session.run("COMMIT").await.unwrap();

    assert_eq!(ids(&session).await, vec![1, 2]);
    let batches = session
        .run("SELECT id FROM checked_items ORDER BY id")
        .await
        .unwrap();
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        (0..values.len())
            .map(|index| values.value(index))
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
}

#[tokio::test]
async fn dropped_buffered_transaction_stream_keeps_completed_state() {
    let (_dir, session) = session();
    session.run("BEGIN").await.unwrap();
    session
        .run("INSERT INTO items (id) VALUES (1)")
        .await
        .unwrap();
    session.run("SAVEPOINT sp").await.unwrap();
    session
        .run("INSERT INTO items (id) VALUES (2)")
        .await
        .unwrap();
    let stream = session.run_stream("ROLLBACK TO sp").await.unwrap();
    drop(stream);
    session.run("COMMIT").await.unwrap();
    assert_eq!(ids(&session).await, vec![1]);

    session.run("BEGIN").await.unwrap();
    session
        .run("INSERT INTO items (id) VALUES (3)")
        .await
        .unwrap();
    session.run("SAVEPOINT released").await.unwrap();
    let stream = session.run_stream("RELEASE released").await.unwrap();
    drop(stream);
    assert!(matches!(
        session.run("ROLLBACK TO released").await,
        Err(MongrelQueryError::SavepointNotFound { name }) if name == "released"
    ));
    session.run("ROLLBACK").await.unwrap();
}

#[tokio::test]
async fn streaming_commit_preserves_commit_critical_state() {
    let (_dir, session) = session();
    session.run("BEGIN").await.unwrap();
    session
        .run("INSERT INTO items (id) VALUES (1)")
        .await
        .unwrap();
    let query_id = QueryId::random().unwrap();
    let query = session
        .register_query(SqlQueryOptions {
            query_id: Some(query_id),
            ..SqlQueryOptions::default()
        })
        .unwrap();
    let mut stream = session
        .run_stream_with_query("COMMIT", query)
        .await
        .unwrap();
    while let Some(batch) = stream.next().await {
        batch.unwrap();
    }
    assert_eq!(ids(&session).await, vec![1]);
    let status = session.query_registry().status(query_id).unwrap();
    assert_eq!(status.phase, SqlQueryPhase::Completed);
    assert!(status.committed);
}

#[test]
fn rollback_to_deadlock_watchdog() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "rollback_to_deadlock_child",
            "--ignored",
            "--nocapture",
        ])
        .env("MONGRELDB_SAVEPOINT_WATCHDOG", dir.path())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(
                status.success(),
                "savepoint watchdog child failed: {status}"
            );
            return;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let _ = child.wait();
            panic!("ROLLBACK TO deadlocked");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[test]
#[ignore]
fn rollback_to_deadlock_child() {
    let Ok(path) = std::env::var("MONGRELDB_SAVEPOINT_WATCHDOG") else {
        return;
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let database = Database::create(path).unwrap();
        database
            .create_table(
                "items",
                Schema {
                    columns: vec![ColumnDef {
                        id: 1,
                        name: "id".into(),
                        ty: TypeId::Int64,
                        flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                        default_value: None,
                    }],
                    ..Schema::default()
                },
            )
            .unwrap();
        let session = MongrelSession::open(Arc::new(database)).unwrap();
        session.run("BEGIN").await.unwrap();
        session
            .run("INSERT INTO items (id) VALUES (1)")
            .await
            .unwrap();
        session.run("SAVEPOINT sp").await.unwrap();
        session
            .run("INSERT INTO items (id) VALUES (2)")
            .await
            .unwrap();
        session.run("ROLLBACK TO sp").await.unwrap();
    });
}
