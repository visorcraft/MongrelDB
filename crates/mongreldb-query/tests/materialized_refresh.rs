use mongreldb_core::Database;
use mongreldb_query::MongrelSession;
use std::sync::Arc;
use tempfile::tempdir;

fn rows(batches: &[arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|batch| batch.num_rows()).sum()
}

fn aggregate_rows(batches: &[arrow::record_batch::RecordBatch]) -> Vec<(String, i64, i64)> {
    use arrow::array::{Int64Array, StringArray};
    let mut rows = Vec::new();
    for batch in batches {
        let groups = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sums = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((
                groups.value(index).to_string(),
                counts.value(index),
                sums.value(index),
            ));
        }
    }
    rows
}

#[tokio::test]
async fn materialized_view_definition_persists_and_full_refresh_is_atomic() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    session
        .run("CREATE TABLE source (id BIGINT PRIMARY KEY, view_key BIGINT)")
        .await
        .unwrap();
    session
        .run("INSERT INTO source VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    session
        .run(
            "CREATE MATERIALIZED VIEW current_values AS \
             SELECT view_key, id FROM source ORDER BY id",
        )
        .await
        .unwrap();

    let definition = db.materialized_view("current_values").unwrap();
    assert!(definition.query.contains("view_key"));
    let initial_refresh = definition.last_refresh_epoch;
    assert_eq!(
        rows(&session.run("SELECT * FROM current_values").await.unwrap()),
        2
    );

    session
        .run("INSERT INTO source VALUES (3, NULL)")
        .await
        .unwrap();
    assert!(session
        .run("REFRESH MATERIALIZED VIEW current_values")
        .await
        .is_err());
    assert_eq!(
        rows(&session.run("SELECT * FROM current_values").await.unwrap()),
        2,
        "failed refresh must not publish its truncate"
    );

    session
        .run("DELETE FROM source WHERE id = 3")
        .await
        .unwrap();
    session
        .run("INSERT INTO source VALUES (4, 40)")
        .await
        .unwrap();
    session
        .run("REFRESH MATERIALIZED VIEW current_values")
        .await
        .unwrap();
    assert_eq!(
        rows(&session.run("SELECT * FROM current_values").await.unwrap()),
        3
    );
    assert!(
        db.materialized_view("current_values")
            .unwrap()
            .last_refresh_epoch
            > initial_refresh
    );

    drop(session);
    drop(db);

    let reopened = Arc::new(Database::open(dir.path()).unwrap());
    assert!(reopened.materialized_view("current_values").is_some());
    let reopened_session = MongrelSession::open(Arc::clone(&reopened)).unwrap();
    reopened_session
        .run("INSERT INTO source VALUES (5, 50)")
        .await
        .unwrap();
    reopened_session
        .run("REFRESH MATERIALIZED VIEW current_values")
        .await
        .unwrap();
    assert_eq!(
        rows(
            &reopened_session
                .run("SELECT * FROM current_values")
                .await
                .unwrap()
        ),
        4
    );

    reopened_session
        .run("DROP MATERIALIZED VIEW current_values")
        .await
        .unwrap();
    assert!(reopened.materialized_view("current_values").is_none());
    assert!(reopened.table("current_values").is_err());
}

#[tokio::test]
async fn refresh_can_atomically_empty_a_materialized_view() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(db).unwrap();
    session
        .run("CREATE TABLE source (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    session.run("INSERT INTO source VALUES (1)").await.unwrap();
    session
        .run("CREATE MATERIALIZED VIEW snapshot AS SELECT id FROM source")
        .await
        .unwrap();
    session
        .run("DELETE FROM source WHERE id = 1")
        .await
        .unwrap();
    session
        .run("REFRESH MATERIALIZED VIEW snapshot")
        .await
        .unwrap();
    assert_eq!(
        rows(&session.run("SELECT * FROM snapshot").await.unwrap()),
        0
    );
}

#[tokio::test]
async fn eligible_grouped_aggregate_refreshes_from_durable_deltas() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    session
        .run(
            "CREATE TABLE sales (\
                id BIGINT PRIMARY KEY, \
                category VARCHAR NOT NULL, \
                amount BIGINT NOT NULL\
            )",
        )
        .await
        .unwrap();
    session
        .run("INSERT INTO sales VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 5)")
        .await
        .unwrap();
    session
        .run(
            "CREATE MATERIALIZED VIEW sales_totals AS \
             SELECT category, COUNT(*) AS n, SUM(amount) AS total \
             FROM sales GROUP BY category",
        )
        .await
        .unwrap();
    let definition = db.materialized_view("sales_totals").unwrap();
    assert!(definition.incremental.is_some());
    assert_eq!(
        aggregate_rows(
            &session
                .run("SELECT * FROM sales_totals ORDER BY category")
                .await
                .unwrap()
        ),
        vec![("a".into(), 2, 30), ("b".into(), 1, 5)]
    );

    session
        .run("INSERT INTO sales VALUES (4, 'a', 7)")
        .await
        .unwrap();
    session
        .run("UPDATE sales SET category = 'b', amount = 30 WHERE id = 1")
        .await
        .unwrap();
    session.run("DELETE FROM sales WHERE id = 2").await.unwrap();
    session
        .run("REFRESH MATERIALIZED VIEW sales_totals")
        .await
        .unwrap();
    assert_eq!(
        aggregate_rows(
            &session
                .run("SELECT * FROM sales_totals ORDER BY category")
                .await
                .unwrap()
        ),
        vec![("a".into(), 1, 7), ("b".into(), 2, 35)]
    );

    // A source truncate cannot be safely represented as deltas. Refresh falls
    // back to one exact snapshot rebuild and resumes incrementally afterward.
    session.run("TRUNCATE TABLE sales").await.unwrap();
    session
        .run("INSERT INTO sales VALUES (5, 'c', 9)")
        .await
        .unwrap();
    session
        .run("REFRESH MATERIALIZED VIEW sales_totals")
        .await
        .unwrap();
    assert_eq!(
        aggregate_rows(
            &session
                .run("SELECT * FROM sales_totals ORDER BY category")
                .await
                .unwrap()
        ),
        vec![("c".into(), 1, 9)]
    );

    drop(session);
    drop(db);
    let reopened = Arc::new(Database::open(dir.path()).unwrap());
    let reopened_session = MongrelSession::open(Arc::clone(&reopened)).unwrap();
    reopened_session
        .run("INSERT INTO sales VALUES (6, 'c', 11)")
        .await
        .unwrap();
    reopened_session
        .run("REFRESH MATERIALIZED VIEW sales_totals")
        .await
        .unwrap();
    assert_eq!(
        aggregate_rows(
            &reopened_session
                .run("SELECT * FROM sales_totals ORDER BY category")
                .await
                .unwrap()
        ),
        vec![("c".into(), 2, 20)]
    );
}
