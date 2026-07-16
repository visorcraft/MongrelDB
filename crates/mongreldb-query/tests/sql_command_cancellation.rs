use arrow::array::Int64Array;
use mongreldb_core::constraint::{FkAction, ForeignKey, TableConstraints};
use mongreldb_core::procedure::{
    ProcedureBody, ProcedureCell, ProcedureMode, ProcedureStep, ProcedureValue, StoredProcedure,
};
use mongreldb_core::{ColumnDef, ColumnFlags, Database, Schema, TypeId, Value};
use mongreldb_query::{
    CancelOutcome, MongrelQueryError, MongrelSession, QueryId, SqlQueryOptions, SqlQueryPhase,
    SqlTestHookPoint,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn session() -> (tempfile::TempDir, Arc<Database>, Arc<MongrelSession>) {
    let dir = tempfile::tempdir().unwrap();
    let database = Arc::new(Database::create(dir.path()).unwrap());
    let session = Arc::new(MongrelSession::open(Arc::clone(&database)).unwrap());
    (dir, database, session)
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

fn nth_blocking_hook(
    point: SqlTestHookPoint,
    nth: usize,
) -> (
    Arc<std::sync::Barrier>,
    std::sync::mpsc::Receiver<()>,
    mongreldb_query::SqlTestHook,
) {
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let worker_barrier = Arc::clone(&barrier);
    let (sender, receiver) = std::sync::mpsc::channel();
    let observed_count = AtomicUsize::new(0);
    let hook = Arc::new(move |observed| {
        if observed == point && observed_count.fetch_add(1, Ordering::AcqRel) + 1 == nth {
            sender.send(()).unwrap();
            worker_barrier.wait();
        }
    });
    (barrier, receiver, hook)
}

fn run_registered(
    session: Arc<MongrelSession>,
    sql: &'static str,
    query_id: QueryId,
) -> tokio::task::JoinHandle<mongreldb_query::Result<Vec<arrow::record_batch::RecordBatch>>> {
    let query = session
        .register_query(SqlQueryOptions {
            query_id: Some(query_id),
            ..SqlQueryOptions::default()
        })
        .unwrap();
    tokio::spawn(async move { session.run_with_query(sql, query).await })
}

fn first_i64(batches: &[arrow::record_batch::RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

fn install_write_procedure(database: &Database) {
    database
        .create_procedure(
            StoredProcedure::new(
                "write_item",
                ProcedureMode::ReadWrite,
                Vec::new(),
                ProcedureBody {
                    steps: vec![ProcedureStep::Put {
                        id: "write".into(),
                        table: "items".into(),
                        cells: vec![
                            ProcedureCell {
                                column_id: 1,
                                value: ProcedureValue::Literal(Value::Int64(2)),
                            },
                            ProcedureCell {
                                column_id: 2,
                                value: ProcedureValue::Literal(Value::Int64(20)),
                            },
                        ],
                        returning: false,
                    }],
                    return_value: ProcedureValue::Literal(Value::Null),
                },
                0,
            )
            .unwrap(),
        )
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctas_cancel_during_source_scan_publishes_no_target() {
    let (_dir, database, session) = session();
    session
        .run("CREATE TABLE source (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    session
        .run("INSERT INTO source VALUES (1), (2), (3)")
        .await
        .unwrap();

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::AfterScanBatch);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(
        Arc::clone(&session),
        "CREATE TABLE copied AS SELECT id FROM source",
        query_id,
    );
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    let cancel_outcome = session.cancel_query(query_id);
    barrier.wait();
    assert_eq!(cancel_outcome, CancelOutcome::Accepted);
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled { query_id: id, .. }) if id == query_id
    ));
    session.set_test_hook(None);
    assert!(database.table("copied").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctas_deadline_during_source_scan_publishes_no_target() {
    let (_dir, database, session) = session();
    session
        .run("CREATE TABLE source (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    session.run("INSERT INTO source VALUES (1)").await.unwrap();

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::AfterScanBatch);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let query = session
        .register_query(SqlQueryOptions {
            query_id: Some(query_id),
            timeout: Some(Duration::from_millis(25)),
            ..SqlQueryOptions::default()
        })
        .unwrap();
    let worker = {
        let session = Arc::clone(&session);
        tokio::spawn(async move {
            session
                .run_with_query("CREATE TABLE copied AS SELECT id FROM source", query)
                .await
        })
    };
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    std::thread::sleep(Duration::from_millis(75));
    barrier.wait();
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::DeadlineExceeded { query_id: id, .. }) if id == query_id
    ));
    session.set_test_hook(None);
    assert!(database.table("copied").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctas_cancel_during_conversion_publishes_no_target() {
    let (_dir, database, session) = session();
    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::BeforeScanBatch);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(
        Arc::clone(&session),
        "CREATE TABLE copied AS SELECT 1 AS id UNION ALL SELECT 2 AS id",
        query_id,
    );
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
    barrier.wait();
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled { query_id: id, .. }) if id == query_id
    ));
    session.set_test_hook(None);
    assert!(database.table("copied").is_err());
}

#[tokio::test]
async fn ctas_validation_failures_publish_no_target() {
    let (_dir, database, session) = session();

    assert!(session
        .run("CREATE TABLE bad_type AS SELECT CAST(1 AS DECIMAL(10, 2)) AS id")
        .await
        .is_err());
    assert!(database.table("bad_type").is_err());

    assert!(session
        .run("CREATE TABLE duplicate_pk AS SELECT 1 AS id UNION ALL SELECT 1 AS id")
        .await
        .is_err());
    assert!(database.table("duplicate_pk").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctas_publish_fence_rejects_cancel_and_records_commit() {
    let (_dir, database, session) = session();
    session
        .run("CREATE TABLE source (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    session.run("INSERT INTO source VALUES (1)").await.unwrap();

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::InsideCommitCritical);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(
        Arc::clone(&session),
        "CREATE TABLE copied AS SELECT id FROM source",
        query_id,
    );
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::TooLate);
    barrier.wait();
    worker.await.unwrap().unwrap();
    session.set_test_hook(None);

    assert!(database.table("copied").is_ok());
    let status = session.query_registry().status(query_id).unwrap();
    assert_eq!(status.phase, SqlQueryPhase::Completed);
    assert!(status.committed);
    assert!(status.durable_outcome.last_commit_epoch.is_some());
}

#[tokio::test]
async fn ctas_if_not_exists_keeps_existing_target() {
    let (_dir, _database, session) = session();
    session
        .run("CREATE TABLE copied (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    session.run("INSERT INTO copied VALUES (99)").await.unwrap();

    session
        .run("CREATE TABLE IF NOT EXISTS copied AS SELECT 1 AS id")
        .await
        .unwrap();
    assert_eq!(
        first_i64(&session.run("SELECT id FROM copied").await.unwrap()),
        99
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_ctas_to_same_name_has_one_winner() {
    let (_dir, database, first) = session();
    first
        .run("CREATE TABLE source (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    first
        .run("INSERT INTO source VALUES (1), (2), (3)")
        .await
        .unwrap();
    let second = Arc::new(MongrelSession::open(Arc::clone(&database)).unwrap());

    let (left, right) = tokio::join!(
        first.run("CREATE TABLE copied AS SELECT id FROM source"),
        second.run("CREATE TABLE copied AS SELECT id FROM source"),
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let mut names = database.table_names();
    names.sort();
    assert_eq!(names, vec!["copied", "source"]);
    assert_eq!(database.table("copied").unwrap().lock().count(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_cancel_during_source_scan_keeps_old_rows() {
    let (_dir, _database, session) = session();
    session
        .run("CREATE TABLE source (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    session.run("INSERT INTO source VALUES (1)").await.unwrap();
    session
        .run("CREATE MATERIALIZED VIEW snapshot AS SELECT id FROM source ORDER BY id")
        .await
        .unwrap();
    session.run("INSERT INTO source VALUES (2)").await.unwrap();

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::AfterScanBatch);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(
        Arc::clone(&session),
        "REFRESH MATERIALIZED VIEW snapshot",
        query_id,
    );
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
    barrier.wait();
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled { query_id: id, .. }) if id == query_id
    ));
    session.set_test_hook(None);

    assert_eq!(
        first_i64(&session.run("SELECT count(*) FROM snapshot").await.unwrap()),
        1
    );
    assert!(!session.query_registry().status(query_id).unwrap().committed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_commit_fence_rejects_cancel_and_records_commit() {
    let (_dir, database, session) = session();
    session
        .run("CREATE TABLE source (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    session.run("INSERT INTO source VALUES (1)").await.unwrap();
    session
        .run("CREATE MATERIALIZED VIEW snapshot AS SELECT id FROM source ORDER BY id")
        .await
        .unwrap();
    session.run("INSERT INTO source VALUES (2)").await.unwrap();

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::InsideCommitCritical);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(
        Arc::clone(&session),
        "REFRESH MATERIALIZED VIEW snapshot",
        query_id,
    );
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::TooLate);
    barrier.wait();
    worker.await.unwrap().unwrap();
    session.set_test_hook(None);

    assert_eq!(
        first_i64(&session.run("SELECT count(*) FROM snapshot").await.unwrap()),
        2
    );
    let status = session.query_registry().status(query_id).unwrap();
    assert!(status.committed);
    let epoch = status.durable_outcome.last_commit_epoch.unwrap();
    assert_eq!(
        database
            .materialized_view("snapshot")
            .unwrap()
            .last_refresh_epoch,
        epoch
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_disconnect_after_commit_reports_durable_epoch() {
    let (_dir, database, session) = session();
    session
        .run("CREATE TABLE source (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    session.run("INSERT INTO source VALUES (1)").await.unwrap();
    session
        .run("CREATE MATERIALIZED VIEW snapshot AS SELECT id FROM source ORDER BY id")
        .await
        .unwrap();
    session.run("INSERT INTO source VALUES (2)").await.unwrap();

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::AfterDurableCommit);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(
        Arc::clone(&session),
        "REFRESH MATERIALIZED VIEW snapshot",
        query_id,
    );
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
    barrier.wait();
    let error = worker.await.unwrap().unwrap_err();
    assert!(matches!(
        error,
        MongrelQueryError::QueryCancelled {
            committed: true,
            last_commit_epoch: Some(_),
            ..
        }
    ));
    session.set_test_hook(None);

    assert_eq!(
        first_i64(&session.run("SELECT count(*) FROM snapshot").await.unwrap()),
        2
    );
    let status = session.query_registry().status(query_id).unwrap();
    assert!(status.committed);
    assert_eq!(status.phase, SqlQueryPhase::Cancelled);
    assert_eq!(
        database
            .materialized_view("snapshot")
            .unwrap()
            .last_refresh_epoch,
        status.durable_outcome.last_commit_epoch.unwrap()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn autocommit_cancel_after_commit_finishes_external_session_sync() {
    let (_dir, _database, session) = session();
    session
        .run("CREATE VIRTUAL TABLE kv USING kv_store")
        .await
        .unwrap();

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::AfterDurableCommit);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(
        Arc::clone(&session),
        "INSERT INTO kv (key, value) VALUES ('one', '1')",
        query_id,
    );
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
    barrier.wait();
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled {
            query_id: id,
            committed: true,
            last_commit_epoch: Some(_),
            ..
        }) if id == query_id
    ));
    session.set_test_hook(None);

    assert_eq!(
        first_i64(&session.run("SELECT count(*) FROM kv").await.unwrap()),
        1
    );
    assert_eq!(
        first_i64(&session.run("SELECT changes()").await.unwrap()),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn autocommit_cancel_after_commit_records_ordinary_dml_changes() {
    let (_dir, _database, session) = session();
    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    assert_eq!(
        first_i64(&session.run("SELECT count(*) FROM items").await.unwrap()),
        0
    );

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::AfterDurableCommit);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(
        Arc::clone(&session),
        "INSERT INTO items VALUES (1), (2)",
        query_id,
    );
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
    barrier.wait();
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled {
            query_id: id,
            committed: true,
            last_commit_epoch: Some(_),
            ..
        }) if id == query_id
    ));
    session.set_test_hook(None);

    assert_eq!(
        first_i64(&session.run("SELECT changes()").await.unwrap()),
        2
    );
    assert_eq!(
        first_i64(&session.run("SELECT count(*) FROM items").await.unwrap()),
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_loop_checkpoint_cancels_before_staging() {
    let (_dir, _database, session) = session();
    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY, value BIGINT)")
        .await
        .unwrap();
    let values = (1..=1024)
        .map(|id| format!("({id}, 10)"))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!("INSERT INTO items VALUES {values}"))
        .await
        .unwrap();

    let (barrier, reached, hook) = nth_blocking_hook(SqlTestHookPoint::BeforeScanBatch, 2);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(
        Arc::clone(&session),
        "UPDATE items SET value = 11",
        query_id,
    );
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
    barrier.wait();
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled { query_id: id, .. }) if id == query_id
    ));
    session.set_test_hook(None);

    assert_eq!(
        first_i64(
            &session
                .run("SELECT count(*) FROM items WHERE value <> 10")
                .await
                .unwrap()
        ),
        0
    );
    assert_eq!(session.staged_sql_operation_count(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_loop_checkpoint_cancels_before_staging() {
    let (_dir, _database, session) = session();
    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY, value BIGINT)")
        .await
        .unwrap();
    let values = (1..=1024)
        .map(|id| format!("({id}, 10)"))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!("INSERT INTO items VALUES {values}"))
        .await
        .unwrap();

    let (barrier, reached, hook) = nth_blocking_hook(SqlTestHookPoint::BeforeScanBatch, 2);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(
        Arc::clone(&session),
        "DELETE FROM items WHERE value = 10",
        query_id,
    );
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
    barrier.wait();
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled { query_id: id, .. }) if id == query_id
    ));
    session.set_test_hook(None);

    assert_eq!(
        first_i64(&session.run("SELECT count(*) FROM items").await.unwrap()),
        1024
    );
    assert_eq!(session.staged_sql_operation_count(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordered_update_merge_checkpoint_cancels_before_staging() {
    let (_dir, _database, session) = session();
    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY, value BIGINT)")
        .await
        .unwrap();
    let values = (1..=2048)
        .map(|id| format!("({id}, 10)"))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!("INSERT INTO items VALUES {values}"))
        .await
        .unwrap();

    // Eight scan checkpoints, eight key-build checkpoints, two bounded-run
    // checkpoints, then the first merge checkpoint.
    let (barrier, reached, hook) = nth_blocking_hook(SqlTestHookPoint::BeforeScanBatch, 19);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(
        Arc::clone(&session),
        "UPDATE items SET value = 99 ORDER BY id DESC LIMIT 1",
        query_id,
    );
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
    barrier.wait();
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled { query_id: id, .. }) if id == query_id
    ));
    session.set_test_hook(None);

    assert_eq!(
        first_i64(
            &session
                .run("SELECT count(*) FROM items WHERE value <> 10")
                .await
                .unwrap()
        ),
        0
    );
    assert_eq!(session.staged_sql_operation_count(), None);

    session
        .run("UPDATE items SET value = 99 ORDER BY id DESC LIMIT 1")
        .await
        .unwrap();
    assert_eq!(
        first_i64(
            &session
                .run("SELECT id FROM items WHERE value = 99")
                .await
                .unwrap()
        ),
        2048
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordered_update_permutation_checkpoint_cancels_before_staging() {
    let (_dir, _database, session) = session();
    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY, value BIGINT)")
        .await
        .unwrap();
    let values = (1..=2048)
        .map(|id| format!("({id}, 10)"))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!("INSERT INTO items VALUES {values}"))
        .await
        .unwrap();

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::DuringOrderedDmlPermutation);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(
        Arc::clone(&session),
        "UPDATE items SET value = 99 ORDER BY id DESC LIMIT 1",
        query_id,
    );
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
    barrier.wait();
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled { query_id: id, .. }) if id == query_id
    ));
    session.set_test_hook(None);

    assert_eq!(
        first_i64(
            &session
                .run("SELECT count(*) FROM items WHERE value <> 10")
                .await
                .unwrap()
        ),
        0
    );
    assert_eq!(session.staged_sql_operation_count(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trigger_values_expansion_checkpoint_is_cancellable() {
    let (_dir, database, session) = session();
    session
        .run("CREATE TABLE source (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    session
        .run("CREATE TABLE audit (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    let values = (1..=1024)
        .map(|id| format!("({id})"))
        .collect::<Vec<_>>()
        .join(",");
    let sql: &'static str = Box::leak(
        format!(
            "CREATE TRIGGER source_ai AFTER INSERT ON source BEGIN \
             INSERT INTO audit VALUES {values}; END"
        )
        .into_boxed_str(),
    );

    // First checkpoint starts the statement. Second starts the VALUES row
    // loop. Third is inside value conversion.
    let (barrier, reached, hook) = nth_blocking_hook(SqlTestHookPoint::DuringTriggerExpansion, 3);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(Arc::clone(&session), sql, query_id);
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
    barrier.wait();
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled { query_id: id, .. }) if id == query_id
    ));
    session.set_test_hook(None);
    assert!(database.trigger("source_ai").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn foreign_key_check_stream_checkpoint_is_cancellable() {
    let dir = tempfile::tempdir().unwrap();
    let database = Arc::new(Database::create(dir.path()).unwrap());
    let parent_schema = Schema {
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
        constraints: TableConstraints::default(),
        clustered: false,
    };
    let mut child_schema = parent_schema.clone();
    child_schema.columns.push(ColumnDef {
        id: 2,
        name: "parent_id".into(),
        ty: TypeId::Int64,
        flags: ColumnFlags::empty(),
        default_value: None,
    });
    child_schema.constraints.foreign_keys.push(ForeignKey {
        id: 1,
        name: "children_parent_fk".into(),
        columns: vec![2],
        ref_table: "parents".into(),
        ref_columns: vec![1],
        on_delete: FkAction::Restrict,
        on_update: FkAction::Restrict,
    });
    database.create_table("parents", parent_schema).unwrap();
    database.create_table("children", child_schema).unwrap();
    let mut transaction = database.begin();
    for id in 1..=1024 {
        transaction
            .put("parents", vec![(1, Value::Int64(id))])
            .unwrap();
        transaction
            .put(
                "children",
                vec![(1, Value::Int64(id)), (2, Value::Int64(id))],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    let session = Arc::new(MongrelSession::open(Arc::clone(&database)).unwrap());

    let (barrier, reached, hook) = nth_blocking_hook(SqlTestHookPoint::BeforeScanBatch, 2);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(Arc::clone(&session), "PRAGMA foreign_key_check", query_id);
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
    barrier.wait();
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled { query_id: id, .. }) if id == query_id
    ));
    session.set_test_hook(None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn view_update_uses_controlled_scan_and_cancels_cleanly() {
    let (_dir, _database, session) = session();
    session
        .run("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    session
        .run("INSERT INTO users VALUES (1, 'Old')")
        .await
        .unwrap();
    session
        .run("CREATE VIEW user_names (id, name) AS SELECT id, name FROM users")
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER user_names_iou INSTEAD OF UPDATE ON user_names BEGIN \
             UPDATE users SET name = NEW.name WHERE id = OLD.id; END",
        )
        .await
        .unwrap();

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::AfterScanBatch);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(
        Arc::clone(&session),
        "UPDATE user_names SET name = 'New' WHERE id = 1",
        query_id,
    );
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
    barrier.wait();
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled { query_id: id, .. }) if id == query_id
    ));
    session.set_test_hook(None);

    let batches = session.run("SELECT name FROM users").await.unwrap();
    let value = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(value, "Old");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assignment_evaluation_observes_cancel_before_mutation() {
    let (_dir, _database, session) = session();
    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY, value TEXT)")
        .await
        .unwrap();
    session
        .run("INSERT INTO items VALUES (1, 'old')")
        .await
        .unwrap();

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::BeforeAssignmentEvaluation);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(
        Arc::clone(&session),
        "UPDATE items SET value = 'new' WHERE id = 1",
        query_id,
    );
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
    barrier.wait();
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled { query_id: id, .. }) if id == query_id
    ));
    session.set_test_hook(None);

    let batches = session.run("SELECT value FROM items").await.unwrap();
    let value = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(value, "old");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_view_delete_cancels_before_trigger_writes() {
    let (_dir, _database, session) = session();
    session
        .run("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    let values = (0..1024)
        .map(|id| format!("({id}, 'user-{id}')"))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!("INSERT INTO users VALUES {values}"))
        .await
        .unwrap();
    session
        .run("CREATE VIEW user_names (id, name) AS SELECT id, name FROM users")
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER user_names_iod INSTEAD OF DELETE ON user_names BEGIN \
             DELETE FROM users WHERE id = OLD.id; END",
        )
        .await
        .unwrap();

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::AfterScanBatch);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let query = session
        .register_query(SqlQueryOptions {
            query_id: Some(query_id),
            ..SqlQueryOptions::default()
        })
        .unwrap();
    let worker_session = Arc::clone(&session);
    let worker = tokio::spawn(async move {
        worker_session
            .run_with_query("DELETE FROM user_names", query)
            .await
    });
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
    barrier.wait();
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled { query_id: id, .. }) if id == query_id
    ));
    session.set_test_hook(None);

    assert_eq!(
        first_i64(&session.run("SELECT COUNT(*) FROM users").await.unwrap()),
        1024
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_update_loop_cancels_before_replace() {
    let (_dir, _database, session) = session();
    session
        .run("CREATE VIRTUAL TABLE kv USING kv_store")
        .await
        .unwrap();
    session
        .run("INSERT INTO kv (key, value) VALUES ('one', '1')")
        .await
        .unwrap();

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::BeforeScanBatch);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(
        Arc::clone(&session),
        "UPDATE kv SET value = 'changed' WHERE key = 'one'",
        query_id,
    );
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
    barrier.wait();
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled { query_id: id, .. }) if id == query_id
    ));
    session.set_test_hook(None);

    let batches = session.run("SELECT value FROM kv").await.unwrap();
    let value = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(value, "1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_refresh_cancels_before_commit() {
    let (_dir, _database, session) = session();
    session
        .run(
            "CREATE TABLE sales (id BIGINT PRIMARY KEY, category TEXT NOT NULL, amount BIGINT NOT NULL)",
        )
        .await
        .unwrap();
    session
        .run("INSERT INTO sales VALUES (1, 'a', 10)")
        .await
        .unwrap();
    session
        .run(
            "CREATE MATERIALIZED VIEW totals AS \
             SELECT category, COUNT(*) AS n, SUM(amount) AS total \
             FROM sales GROUP BY category",
        )
        .await
        .unwrap();
    session
        .run("INSERT INTO sales VALUES (2, 'a', 20)")
        .await
        .unwrap();

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::BeforeScanBatch);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(
        Arc::clone(&session),
        "REFRESH MATERIALIZED VIEW totals",
        query_id,
    );
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
    barrier.wait();
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled { query_id: id, .. }) if id == query_id
    ));
    session.set_test_hook(None);

    assert_eq!(
        first_i64(&session.run("SELECT n FROM totals").await.unwrap()),
        1
    );
    assert!(!session.query_registry().status(query_id).unwrap().committed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_rebuild_cancel_keeps_original_table() {
    let (_dir, database, session) = session();
    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY, value BIGINT)")
        .await
        .unwrap();
    session
        .run("INSERT INTO items VALUES (1, 10), (2, 20)")
        .await
        .unwrap();

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::BeforeScanBatch);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(
        Arc::clone(&session),
        "ALTER TABLE items ADD COLUMN note TEXT",
        query_id,
    );
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
    barrier.wait();
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled { query_id: id, .. }) if id == query_id
    ));
    session.set_test_hook(None);

    let handle = database.table("items").unwrap();
    let table = handle.lock();
    assert!(table.schema().column("note").is_none());
    assert_eq!(table.count(), 2);
    drop(table);
    assert_eq!(database.table_names(), vec!["items"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_rebuild_publish_fence_is_too_late() {
    let (_dir, database, session) = session();
    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY, value BIGINT)")
        .await
        .unwrap();
    session
        .run("INSERT INTO items VALUES (1, 10)")
        .await
        .unwrap();

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::InsideCommitCritical);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(
        Arc::clone(&session),
        "ALTER TABLE items ADD COLUMN note TEXT",
        query_id,
    );
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::TooLate);
    barrier.wait();
    worker.await.unwrap().unwrap();
    session.set_test_hook(None);

    assert!(database
        .table("items")
        .unwrap()
        .lock()
        .schema()
        .column("note")
        .is_some());
    let status = session.query_registry().status(query_id).unwrap();
    assert!(status.committed);
    assert_eq!(
        status.durable_outcome.last_commit_epoch,
        Some(database.visible_epoch().0)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_pragma_fence_records_exact_epoch() {
    let (_dir, database, session) = session();
    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::InsideCommitCritical);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(Arc::clone(&session), "PRAGMA user_version = 42", query_id);
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::TooLate);
    barrier.wait();
    assert_eq!(first_i64(&worker.await.unwrap().unwrap()), 42);
    session.set_test_hook(None);

    let status = session.query_registry().status(query_id).unwrap();
    assert!(status.committed);
    assert_eq!(
        status.durable_outcome.last_commit_epoch,
        Some(database.visible_epoch().0)
    );
    assert_eq!(database.sql_pragma_i64("user_version").unwrap(), Some(42));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vacuum_into_fence_records_backup_snapshot_epoch() {
    let (_dir, database, session) = session();
    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    session.run("INSERT INTO items VALUES (1)").await.unwrap();
    let expected_epoch = database.visible_epoch().0;
    let backup_dir = tempfile::tempdir().unwrap();
    let destination = backup_dir.path().join("snapshot");

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::InsideCommitCritical);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let sql = format!("VACUUM INTO '{}'", destination.display());
    let query = session
        .register_query(SqlQueryOptions {
            query_id: Some(query_id),
            ..SqlQueryOptions::default()
        })
        .unwrap();
    let worker = {
        let session = Arc::clone(&session);
        tokio::spawn(async move { session.run_with_query(&sql, query).await })
    };
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::TooLate);
    barrier.wait();
    worker.await.unwrap().unwrap();
    session.set_test_hook(None);

    assert!(destination.join("_meta/backup.json").is_file());
    let status = session.query_registry().status(query_id).unwrap();
    assert!(status.committed);
    assert_eq!(
        status.durable_outcome.last_commit_epoch,
        Some(expected_epoch)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn call_cancel_before_commit_rolls_back_procedure() {
    let (_dir, database, session) = session();
    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY, value BIGINT)")
        .await
        .unwrap();
    install_write_procedure(&database);

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::BeforeCommitFence);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(Arc::clone(&session), "CALL write_item(JSON '{}')", query_id);
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::Accepted);
    barrier.wait();
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled { query_id: id, .. }) if id == query_id
    ));
    session.set_test_hook(None);
    assert_eq!(
        first_i64(&session.run("SELECT count(*) FROM items").await.unwrap()),
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn call_commit_fence_is_too_late_and_records_epoch() {
    let (_dir, database, session) = session();
    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY, value BIGINT)")
        .await
        .unwrap();
    install_write_procedure(&database);

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::InsideCommitCritical);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(Arc::clone(&session), "CALL write_item(JSON '{}')", query_id);
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::TooLate);
    barrier.wait();
    worker.await.unwrap().unwrap();
    session.set_test_hook(None);
    assert_eq!(
        first_i64(&session.run("SELECT count(*) FROM items").await.unwrap()),
        1
    );
    let status = session.query_registry().status(query_id).unwrap();
    assert!(status.committed);
    assert!(status.durable_outcome.last_commit_epoch.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_publish_fence_is_too_late() {
    let (dir, database, session) = session();
    session
        .run("CREATE TABLE doomed (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    let table_id = database.table_id("doomed").unwrap();
    let table_dir = dir.path().join("tables").join(table_id.to_string());
    database.drop_table("doomed").unwrap();

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::InsideCommitCritical);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(Arc::clone(&session), "COMPACT DATABASE", query_id);
    reached.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(session.cancel_query(query_id), CancelOutcome::TooLate);
    barrier.wait();
    worker.await.unwrap().unwrap();
    session.set_test_hook(None);

    assert!(!table_dir.exists());
    let status = session.query_registry().status(query_id).unwrap();
    assert!(status.committed);
    assert_eq!(
        status.durable_outcome.last_commit_epoch,
        Some(database.visible_epoch().0)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_not_null_cancel_between_backfill_and_metadata_reports_partial_commit() {
    let (_dir, database, session) = session();
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

    let (barrier, reached, hook) = blocking_hook(SqlTestHookPoint::AfterDurableCommit);
    session.set_test_hook(Some(hook));
    let query_id = QueryId::random().unwrap();
    let worker = run_registered(
        Arc::clone(&session),
        "ALTER TABLE items ALTER COLUMN score SET NOT NULL",
        query_id,
    );
    if let Err(error) = reached.recv_timeout(Duration::from_secs(5)) {
        if worker.is_finished() {
            panic!(
                "ALTER finished before durable hook: {:?}",
                worker.await.unwrap()
            );
        }
        panic!("ALTER did not reach durable hook: {error}");
    }
    let cancel_outcome = session.cancel_query(query_id);
    barrier.wait();
    assert_eq!(cancel_outcome, CancelOutcome::Accepted);
    let error = worker.await.unwrap().unwrap_err();
    assert!(matches!(
        error,
        MongrelQueryError::QueryCancelled {
            committed: true,
            last_commit_epoch: Some(_),
            ..
        }
    ));
    session.set_test_hook(None);

    let status = session.query_registry().status(query_id).unwrap();
    assert!(status.committed);
    assert_eq!(status.phase, SqlQueryPhase::Cancelled);
    assert!(status.durable_outcome.last_commit_epoch.is_some());
    assert!(database
        .table("items")
        .unwrap()
        .lock()
        .schema()
        .column("score")
        .unwrap()
        .flags
        .contains(mongreldb_core::schema::ColumnFlags::NULLABLE));
    let values = session
        .run("SELECT score FROM items ORDER BY id")
        .await
        .unwrap();
    let values = values[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values(), &[7, 7]);
}
