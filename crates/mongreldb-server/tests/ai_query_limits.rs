use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::Database;
use mongreldb_server::build_app;
use std::sync::Arc;
use tower::ServiceExt;

fn schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "embedding".into(),
                ty: TypeId::Embedding { dim: 8 },
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "ann".into(),
            column_id: 2,
            kind: IndexKind::Ann,
            predicate: None,
            options: Default::default(),
        }],
        ..Schema::default()
    }
}

async fn post(app: axum::Router, path: &str, body: serde_json::Value) -> (u16, serde_json::Value) {
    let response = app
        .oneshot(
            axum::http::Request::post(path)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

fn assert_bad_request(status: u16, body: &serde_json::Value) {
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["status"], "aborted");
    assert_eq!(body["error"]["code"], "BAD_REQUEST");
}

#[tokio::test]
async fn kit_ai_limits_return_typed_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("docs", schema()).unwrap();
    let app = build_app(db);

    let (status, body) = post(
        app.clone(),
        "/kit/query",
        serde_json::json!({"table":"docs","conditions":[],"limit":usize::MAX}),
    )
    .await;
    assert_bad_request(status, &body);

    let retrievers = (0..=mongreldb_core::query::MAX_RETRIEVERS)
        .map(|index| {
            serde_json::json!({
                "name":format!("dense{index}"),
                "weight":1.0,
                "ann":{"column_id":2,"query":[1,1,1,1,1,1,1,1],"k":1}
            })
        })
        .collect::<Vec<_>>();
    let (status, body) = post(
        app.clone(),
        "/kit/search",
        serde_json::json!({
            "table":"docs","must":[],"retrievers":retrievers,
            "fusion":{"reciprocal_rank":{"constant":60}},"limit":1,"projection":[1]
        }),
    )
    .await;
    assert_bad_request(status, &body);

    let (status, body) = post(
        app.clone(),
        "/kit/search",
        serde_json::json!({
            "table":"docs",
            "must":[{"ann":{"column_id":2,"query":[1,1,1,1,1,1,1,1],"k":1}}],
            "retrievers":[{"name":"dense","weight":1.0,"ann":{"column_id":2,"query":[1,1,1,1,1,1,1,1],"k":1}}],
            "fusion":{"reciprocal_rank":{"constant":60}},"limit":1,"projection":[1]
        }),
    )
    .await;
    assert_bad_request(status, &body);

    let (status, body) = post(
        app,
        "/kit/ann_rerank",
        serde_json::json!({
            "table":"docs","column_id":2,"query":[1,1,1,1,1,1,1,1],
            "candidate_k":usize::MAX,"limit":1,"metric":"cosine"
        }),
    )
    .await;
    assert_bad_request(status, &body);
}
