use futures::StreamExt;
use mongreldb_core::{CancellationReason, ColumnDef, ColumnFlags, Database, Schema, TypeId, Value};
use mongreldb_query::{
    CancelOutcome, MongrelQueryError, MongrelSession, QueryId, SqlQueryOptions, SqlQueryPhase,
};
use std::sync::Arc;
use std::time::Duration;

fn session() -> (tempfile::TempDir, Arc<MongrelSession>) {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::create(dir.path()).unwrap();
    database
        .create_table(
            "docs",
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
    database
        .transaction(|transaction| {
            transaction.put_batch(
                "docs",
                (0..100).map(|id| vec![(1, Value::Int64(id))]).collect(),
            )?;
            Ok(())
        })
        .unwrap();
    let session = MongrelSession::open(Arc::new(database)).unwrap();
    (dir, Arc::new(session))
}

async fn wait_for_phase(session: &MongrelSession, id: QueryId, phase: SqlQueryPhase) {
    for _ in 0..100 {
        if session
            .query_registry()
            .status(id)
            .is_some_and(|status| status.phase == phase)
        {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("query {id} did not reach {phase:?}");
}

#[tokio::test]
async fn cancel_while_queued_for_session_permit_and_reuse_session() {
    let (_dir, session) = session();
    let held_id = QueryId::random().unwrap();
    let held = session
        .run_stream_with_options(
            "SELECT id FROM docs",
            SqlQueryOptions {
                query_id: Some(held_id),
                ..SqlQueryOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        session.query_registry().status(held_id).unwrap().phase,
        SqlQueryPhase::Streaming
    );

    let queued_id = QueryId::random().unwrap();
    let queued = session
        .register_query(SqlQueryOptions {
            query_id: Some(queued_id),
            ..SqlQueryOptions::default()
        })
        .unwrap();
    let worker = {
        let session = Arc::clone(&session);
        tokio::spawn(async move {
            session
                .run_with_query("SELECT count(*) FROM docs", queued)
                .await
        })
    };
    wait_for_phase(&session, queued_id, SqlQueryPhase::Queued).await;
    assert_eq!(session.cancel_query(queued_id), CancelOutcome::Accepted);
    assert!(matches!(
        worker.await.unwrap(),
        Err(MongrelQueryError::QueryCancelled { query_id, .. }) if query_id == queued_id
    ));
    drop(held);
    assert!(session.run("SELECT 1").await.is_ok());
    assert_eq!(session.query_registry().active_count(), 0);
}

#[tokio::test]
async fn deadline_includes_session_queue_wait() {
    let (_dir, session) = session();
    let held = session.run_stream("SELECT id FROM docs").await.unwrap();
    let id = QueryId::random().unwrap();
    let result = session
        .run_with_options(
            "SELECT count(*) FROM docs",
            SqlQueryOptions {
                query_id: Some(id),
                timeout: Some(Duration::from_millis(1)),
                ..SqlQueryOptions::default()
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(MongrelQueryError::DeadlineExceeded { query_id, .. }) if query_id == id
    ));
    drop(held);
}

#[tokio::test]
async fn dropping_stream_cancels_and_normal_completion_cleans_registry() {
    let (_dir, session) = session();
    let dropped_id = QueryId::random().unwrap();
    let dropped = session
        .run_stream_with_options(
            "SELECT id FROM docs",
            SqlQueryOptions {
                query_id: Some(dropped_id),
                ..SqlQueryOptions::default()
            },
        )
        .await
        .unwrap();
    drop(dropped);
    let status = session.query_registry().status(dropped_id).unwrap();
    assert_eq!(status.phase, SqlQueryPhase::Cancelled);
    assert_eq!(
        status.cancellation_reason,
        CancellationReason::ClientDisconnected
    );

    let completed_id = QueryId::random().unwrap();
    let mut completed = session
        .run_stream_with_options(
            "SELECT id FROM docs",
            SqlQueryOptions {
                query_id: Some(completed_id),
                ..SqlQueryOptions::default()
            },
        )
        .await
        .unwrap();
    while let Some(batch) = completed.next().await {
        batch.unwrap();
    }
    assert_eq!(
        session.query_registry().status(completed_id).unwrap().phase,
        SqlQueryPhase::Completed
    );
    assert_eq!(session.query_registry().active_count(), 0);
}

#[tokio::test]
async fn multi_statement_and_prepared_reuse_get_fresh_controls() {
    let (_dir, session) = session();
    let multi_id = QueryId::random().unwrap();
    let batches = session
        .run_with_options(
            "SELECT 1; SELECT 2",
            SqlQueryOptions {
                query_id: Some(multi_id),
                ..SqlQueryOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(
        session.query_registry().status(multi_id).unwrap().phase,
        SqlQueryPhase::Completed
    );

    session
        .run("PREPARE q AS SELECT id FROM docs")
        .await
        .unwrap();
    let first_id = QueryId::random().unwrap();
    let second_id = QueryId::random().unwrap();
    session
        .run_with_options(
            "EXECUTE q",
            SqlQueryOptions {
                query_id: Some(first_id),
                ..SqlQueryOptions::default()
            },
        )
        .await
        .unwrap();
    session
        .run_with_options(
            "EXECUTE q",
            SqlQueryOptions {
                query_id: Some(second_id),
                ..SqlQueryOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        session.query_registry().status(first_id).unwrap().phase,
        SqlQueryPhase::Completed
    );
    assert_eq!(
        session.query_registry().status(second_id).unwrap().phase,
        SqlQueryPhase::Completed
    );
}
