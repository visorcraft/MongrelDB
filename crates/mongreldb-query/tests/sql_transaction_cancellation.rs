use arrow::array::Int64Array;
use mongreldb_core::{ColumnDef, ColumnFlags, Database, Schema, TypeId};
use mongreldb_query::{
    CancelOutcome, MongrelQueryError, MongrelSession, QueryId, SqlQueryOptions, SqlQueryPhase,
    SqlTestHookPoint,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn session() -> (tempfile::TempDir, Arc<MongrelSession>) {
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
    (
        dir,
        Arc::new(MongrelSession::open(Arc::new(database)).unwrap()),
    )
}

fn blocking_hook(
    point: SqlTestHookPoint,
) -> (
    Arc<std::sync::Barrier>,
    std::sync::mpsc::Receiver<()>,
    mongreldb_query::SqlTestHook,
) {
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let worker_barrier = Arc::clone(&barrier);
    let (sender, receiver) = std::sync::mpsc::channel();
    let fired = AtomicBool::new(false);
    let hook = Arc::new(move |observed| {
        if observed == point && !fired.swap(true, Ordering::AcqRel) {
            sender.send(()).unwrap();
            worker_barrier.wait();
        }
    });
    (barrier, receiver, hook)
}

fn count(batches: &[arrow::record_batch::RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn failed_statement_restores_savepoint_and_aborts_transaction() {
    let (_dir, session) = session();
    session.run("BEGIN").await.unwrap();
    session
        .run("INSERT INTO items (id) VALUES (1)")
        .await
        .unwrap();

    assert!(session.run("SELECT * FROM missing_table").await.is_err());
    assert!(matches!(
        session.run("SELECT count(*) FROM items").await,
        Err(MongrelQueryError::TransactionAborted)
    ));
    assert!(matches!(
        session.run("COMMIT").await,
        Err(MongrelQueryError::TransactionAborted)
    ));

    session.run("ROLLBACK").await.unwrap();
    assert_eq!(
        count(&session.run("SELECT count(*) FROM items").await.unwrap()),
        0
    );
}

#[tokio::test]
async fn explicit_commit_owns_fence_and_records_durable_outcome() {
    let (_dir, session) = session();
    session.run("BEGIN").await.unwrap();
    session
        .run("INSERT INTO items (id) VALUES (1)")
        .await
        .unwrap();
    let query_id = QueryId::random().unwrap();
    session
        .run_with_options(
            "COMMIT",
            SqlQueryOptions {
                query_id: Some(query_id),
                ..SqlQueryOptions::default()
            },
        )
        .await
        .unwrap();

    let status = session.query_registry().status(query_id).unwrap();
    assert_eq!(status.phase, SqlQueryPhase::Completed);
    assert!(status.committed);
    assert_eq!(
        count(&session.run("SELECT count(*) FROM items").await.unwrap()),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_wins_before_autocommit_fence_and_writes_nothing() {
    let (_dir, session) = session();
    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::BeforeCommitFence);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let query = session
        .register_query(SqlQueryOptions {
            query_id: Some(query_id),
            ..SqlQueryOptions::default()
        })
        .unwrap();
    let worker = {
        let session = Arc::clone(&session);
        tokio::spawn(async move {
            session
                .run_with_query("INSERT INTO items (id) VALUES (1)", query)
                .await
        })
    };
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
    barrier.wait();
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled { query_id: id, .. }) if id == query_id
    ));
    session.set_test_hook(None);
    assert_eq!(
        count(&session.run("SELECT count(*) FROM items").await.unwrap()),
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_wins_fence_and_cancel_is_too_late() {
    let (_dir, session) = session();
    session.run("BEGIN").await.unwrap();
    session
        .run("INSERT INTO items (id) VALUES (1)")
        .await
        .unwrap();
    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::InsideCommitCritical);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let query = session
        .register_query(SqlQueryOptions {
            query_id: Some(query_id),
            ..SqlQueryOptions::default()
        })
        .unwrap();
    let worker = {
        let session = Arc::clone(&session);
        tokio::spawn(async move { session.run_with_query("COMMIT", query).await })
    };
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(
        session.query_registry().status(query_id).unwrap().phase,
        SqlQueryPhase::CommitCritical
    );
    assert_eq!(session.cancel_query(query_id), CancelOutcome::TooLate);
    barrier.wait();
    worker.await.unwrap().unwrap();
    session.set_test_hook(None);
    let status = session.query_registry().status(query_id).unwrap();
    assert_eq!(status.phase, SqlQueryPhase::Completed);
    assert!(status.committed);
    assert_eq!(
        count(&session.run("SELECT count(*) FROM items").await.unwrap()),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_transaction_statement_restores_staging_and_aborts() {
    let (_dir, session) = session();
    session.run("BEGIN").await.unwrap();
    session
        .run("INSERT INTO items (id) VALUES (1)")
        .await
        .unwrap();
    assert_eq!(session.staged_sql_operation_count(), Some(1));

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::AfterStatementStaging);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let query = session
        .register_query(SqlQueryOptions {
            query_id: Some(query_id),
            ..SqlQueryOptions::default()
        })
        .unwrap();
    let worker = {
        let session = Arc::clone(&session);
        tokio::spawn(async move {
            session
                .run_with_query("INSERT INTO items (id) VALUES (2)", query)
                .await
        })
    };
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
    barrier.wait();
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled { query_id: id, .. }) if id == query_id
    ));
    session.set_test_hook(None);
    assert_eq!(session.staged_sql_operation_count(), Some(1));
    assert!(matches!(
        session.run("COMMIT").await,
        Err(MongrelQueryError::TransactionAborted)
    ));
    session.run("ROLLBACK").await.unwrap();
}
