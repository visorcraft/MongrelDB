//! HOT-fallback observability: verifies the `/metrics` endpoint exposes the
//! per-table `LookupMetricsSnapshot` aggregated across the database, with the
//! expected HELP/TYPE preambles and counter values for every HOT series.

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use mongreldb_core::engine::LookupMetricsSnapshot;
use mongreldb_core::{ColumnDef, ColumnFlags, Database, Schema, TypeId};
use mongreldb_server::build_app;
use serde_json::{json, Value};
use std::sync::Arc;
use tempfile::tempdir;
use tower::ServiceExt;

fn post(path: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn get(path: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(path)
        .body(Body::empty())
        .unwrap()
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn text_body(response: axum::response::Response) -> String {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn one_table_database() -> (tempfile::TempDir, Arc<Database>) {
    let directory = tempdir().unwrap();
    let database = Arc::new(Database::create(directory.path()).unwrap());
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
                    embedding_source: None,
                }],
                ..Schema::default()
            },
        )
        .unwrap();
    (directory, database)
}

async fn sql(app: axum::Router, statement: &str) -> Value {
    let response = app
        .oneshot(post("/sql", json!({ "sql": statement })))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "SQL: {statement}");
    json_body(response).await
}

#[tokio::test]
async fn metrics_export_includes_hot_fallback_block() {
    let (_directory, database) = one_table_database();
    let app = build_app(Arc::clone(&database));

    // Seed a row, then perform a healthy PK query (hot hit) and a deleted-row
    // query (hot fallback). Both paths must bump the table's per-atomic
    // counters so the /metrics export reflects the table snapshot.
    sql(app.clone(), "INSERT INTO items (id) VALUES (1)").await;
    sql(app.clone(), "SELECT id FROM items WHERE id = 1").await;
    sql(app.clone(), "DELETE FROM items WHERE id = 1").await;
    sql(app.clone(), "SELECT id FROM items WHERE id = 1").await;

    // Scrape the aggregated snapshot before reading the wire body so we know
    // the exact expected values.
    let expected: LookupMetricsSnapshot = database
        .table("items")
        .unwrap()
        .read()
        .lookup_metrics_snapshot();
    assert!(
        expected.hot_lookup_hit >= 1,
        "expected at least one hot hit; got {expected:?}"
    );
    assert!(
        expected.hot_lookup_fallback >= 1,
        "expected at least one hot fallback; got {expected:?}"
    );

    let response = app.clone().oneshot(get("/metrics")).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(
        content_type.starts_with("text/plain"),
        "metrics content-type: {content_type}"
    );
    let body = text_body(response).await;

    // Every HELP/TYPE preamble required by the spec must be present.
    for preamble in [
        "# HELP hot_lookup_total ",
        "# TYPE hot_lookup_total counter",
        "# HELP hot_fallback_total ",
        "# TYPE hot_fallback_total counter",
        "# HELP hot_fallback_overlay_versions_total ",
        "# TYPE hot_fallback_overlay_versions_total counter",
        "# HELP hot_fallback_runs_considered_total ",
        "# TYPE hot_fallback_runs_considered_total counter",
        "# TYPE hot_fallback_runs_opened_total counter",
        "# TYPE hot_fallback_pages_decoded_total counter",
        "# TYPE hot_fallback_rows_materialized_total counter",
        "# HELP hot_lookup_duration_seconds ",
        "# TYPE hot_lookup_duration_seconds counter",
        "# TYPE hot_fallback_duration_seconds counter",
        "# HELP hot_mapping_rebuild_total ",
        "# TYPE hot_mapping_rebuild_total counter",
        "# TYPE hot_checkpoint_rejected_total counter",
    ] {
        assert!(
            body.contains(preamble),
            "missing preamble {preamble:?} in:\n{body}"
        );
    }

    // The per-reason labelled series must cover every reason index 0..=8.
    for reason in [
        "missing_mapping",
        "stale_row_id",
        "invisible_at_snapshot",
        "historical_snapshot",
        "tombstone",
        "ttl_expired",
        "primary_key_mismatch",
        "index_incomplete",
        "checkpoint_rejected",
    ] {
        let needle = format!("hot_fallback_total{{reason=\"{reason}\"}}");
        assert!(
            body.contains(&needle),
            "missing series {needle:?} in:\n{body}"
        );
    }

    // Every counter value on the wire must match the aggregated snapshot.
    for (needle, value) in [
        (
            "hot_lookup_total{outcome=\"hit\"}".to_string(),
            expected.hot_lookup_hit,
        ),
        (
            "hot_lookup_total{outcome=\"fallback\"}".to_string(),
            expected.hot_lookup_fallback,
        ),
        (
            "hot_fallback_overlay_versions_total".to_string(),
            expected.hot_fallback_overlay_versions_total,
        ),
        (
            "hot_fallback_runs_considered_total".to_string(),
            expected.hot_fallback_runs_considered_total,
        ),
        (
            "hot_fallback_runs_opened_total".to_string(),
            expected.hot_fallback_runs_opened_total,
        ),
        (
            "hot_fallback_pages_decoded_total".to_string(),
            expected.hot_fallback_pages_decoded_total,
        ),
        (
            "hot_fallback_rows_materialized_total".to_string(),
            expected.hot_fallback_rows_materialized_total,
        ),
        (
            "hot_mapping_rebuild_total".to_string(),
            expected.hot_mapping_rebuild_total,
        ),
        (
            "hot_checkpoint_rejected_total".to_string(),
            expected.hot_checkpoint_rejected_total,
        ),
    ] {
        let line = format!("{needle} {value}");
        assert!(body.contains(&line), "missing line {line:?} in:\n{body}");
    }
    // Per-reason breakdown must also equal the snapshot.
    for (idx, reason) in [
        "missing_mapping",
        "stale_row_id",
        "invisible_at_snapshot",
        "historical_snapshot",
        "tombstone",
        "ttl_expired",
        "primary_key_mismatch",
        "index_incomplete",
        "checkpoint_rejected",
    ]
    .into_iter()
    .enumerate()
    {
        let line = format!(
            "hot_fallback_total{{reason=\"{reason}\"}} {}",
            expected.hot_fallback_reasons[idx]
        );
        assert!(
            body.contains(&line),
            "missing per-reason line {line:?} in:\n{body}"
        );
    }
    // Duration counters (seconds) are derived from the nanos atomic.
    let expected_lookup_seconds = format!(
        "hot_lookup_duration_seconds {}",
        expected.hot_lookup_duration_nanos as f64 / 1e9
    );
    let expected_fallback_seconds = format!(
        "hot_fallback_duration_seconds {}",
        expected.hot_fallback_duration_nanos as f64 / 1e9
    );
    assert!(
        body.contains(&expected_lookup_seconds),
        "missing line {expected_lookup_seconds:?} in:\n{body}"
    );
    assert!(
        body.contains(&expected_fallback_seconds),
        "missing line {expected_fallback_seconds:?} in:\n{body}"
    );
}
