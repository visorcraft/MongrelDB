use futures::StreamExt;
use mongreldb_core::{ColumnDef, ColumnFlags, Database, Schema, TypeId, Value};
use mongreldb_server::build_app;
use std::sync::Arc;
use tempfile::tempdir;
use tower::ServiceExt;

fn schema() -> Schema {
    Schema {
        schema_id: 0,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
            embedding_source: None,
        }],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

fn put(db: &Database, id: i64) {
    db.transaction(|transaction| {
        transaction.put("items", vec![(1, Value::Int64(id))])?;
        Ok(())
    })
    .unwrap();
}

#[tokio::test]
async fn events_is_resumable_sse() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("items", schema()).unwrap();
    put(&db, 1);
    let first_id = db
        .change_events_since(None)
        .unwrap()
        .events
        .last()
        .unwrap()
        .id
        .clone()
        .unwrap();
    put(&db, 2);
    let app = build_app(Arc::clone(&db));

    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/events")
                .header("last-event-id", &first_id)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .unwrap(),
        "text/event-stream"
    );
    let mut body = response.into_body().into_data_stream();
    let chunk = tokio::time::timeout(std::time::Duration::from_secs(1), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let text = String::from_utf8(chunk.to_vec()).unwrap();
    assert!(text.contains("event: change"));
    assert!(text.contains("\"op\":\"put\""));
    assert!(!text.contains(&format!("id: {first_id}\n")));
}

#[tokio::test]
async fn events_rejects_malformed_resume_id() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(db);
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri("/events")
                .header("last-event-id", "broken")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);

    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/events")
                .header(
                    "last-event-id",
                    axum::http::HeaderValue::from_bytes(&[0xff]).unwrap(),
                )
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
}
