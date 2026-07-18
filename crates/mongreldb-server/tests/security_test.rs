use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use mongreldb_core::{
    ColumnDef, ColumnFlags, ColumnMask, Database, MaskStrategy, Permission, PolicyCommand,
    Principal, RowPolicy, Schema, SecurityCatalog, SecurityExpr, TypeId, Value,
};
use mongreldb_core::{IndexDef, IndexKind};
use mongreldb_query::ExternalTableModule;
use mongreldb_server::build_app_full;
use serde_json::{json, Value as JsonValue};
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
                name: "owner".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 3,
                name: "secret".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 4,
                name: "value".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 5,
                name: "sparse".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![
            IndexDef {
                name: "sparse".into(),
                column_id: 5,
                kind: IndexKind::Sparse,
                predicate: None,
                options: Default::default(),
            },
            IndexDef {
                name: "hidden_value".into(),
                column_id: 4,
                kind: IndexKind::Bitmap,
                predicate: None,
                options: Default::default(),
            },
        ],
        clustered: true,
        ..Schema::default()
    }
}

fn request(method: &str, uri: &str, body: JsonValue) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", "Basic YWxpY2U6cHc=")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

#[tokio::test]
async fn user_principal_secures_sql_native_kit_and_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("docs", schema()).unwrap();
    db.transaction(|transaction| {
        transaction.put(
            "docs",
            vec![
                (1, Value::Int64(1)),
                (2, Value::Bytes(b"alice".to_vec())),
                (3, Value::Bytes(b"alice-secret".to_vec())),
                (4, Value::Int64(10)),
                (
                    5,
                    Value::Bytes(mongreldb_core::query::encode_sparse_vector(&[(1, 1.0)])?),
                ),
            ],
        )?;
        transaction.put(
            "docs",
            vec![
                (1, Value::Int64(2)),
                (2, Value::Bytes(b"bob".to_vec())),
                (3, Value::Bytes(b"bob-secret".to_vec())),
                (4, Value::Int64(20)),
                (
                    5,
                    Value::Bytes(mongreldb_core::query::encode_sparse_vector(&[(1, 10.0)])?),
                ),
            ],
        )?;
        Ok(())
    })
    .unwrap();
    db.create_user("alice", "pw").unwrap();
    db.create_role("tenant").unwrap();
    for permission in [
        Permission::SelectColumns {
            table: "docs".into(),
            columns: vec![
                "id".into(),
                "owner".into(),
                "secret".into(),
                "sparse".into(),
            ],
        },
        Permission::InsertColumns {
            table: "docs".into(),
            columns: vec!["id".into(), "owner".into(), "secret".into(), "value".into()],
        },
    ] {
        db.grant_permission("tenant", permission).unwrap();
    }
    db.grant_role("alice", "tenant").unwrap();
    db.set_security_catalog_as(
        SecurityCatalog {
            rls_tables: vec!["docs".into()],
            policies: vec![RowPolicy {
                name: "owner_only".into(),
                table: "docs".into(),
                command: PolicyCommand::All,
                subjects: vec!["public".into()],
                permissive: true,
                using: Some(SecurityExpr::ColumnEqCurrentUser { column: 2 }),
                with_check: Some(SecurityExpr::ColumnEqCurrentUser { column: 2 }),
            }],
            masks: vec![ColumnMask {
                name: "hide_secret".into(),
                table: "docs".into(),
                column: 3,
                strategy: MaskStrategy::Redact {
                    replacement: "***".into(),
                },
                exempt_subjects: Vec::new(),
            }],
        },
        Some(&Principal {
            user_id: 0,
            created_epoch: 0,
            username: "admin".into(),
            is_admin: true,
            roles: Vec::new(),
            permissions: Vec::new(),
        }),
    )
    .unwrap();

    let app = build_app_full(
        Arc::clone(&db),
        std::iter::empty::<Arc<dyn ExternalTableModule>>(),
        None,
        None,
        true,
    );

    for uri in [
        "/audit",
        "/metrics",
        "/events",
        "/wal/stream?since=0",
        "/history/retention",
        "/kit/ai/metrics",
    ] {
        let response = app
            .clone()
            .oneshot(request("GET", uri, json!({})))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{uri}");
    }

    let response = app
        .clone()
        .oneshot(request(
            "POST",
            "/kit/search",
            json!({
                "table":"docs",
                "must":[],
                "retrievers":[{
                    "name":"sparse",
                    "weight":1.0,
                    "sparse":{"column_id":5,"query":[[1,1.0]],"k":1}
                }],
                "fusion":{"reciprocal_rank":{"constant":60}},
                "limit":1,
                "projection":[1],
                "explain":true
            }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let body: JsonValue =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], "PERMISSION_DENIED");
    assert!(body.get("trace").is_none());

    let response = app
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({ "sql": "SELECT id, secret FROM docs ORDER BY id" }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: JsonValue =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(body, json!([{ "id": 1, "secret": "***" }]));

    let response = app
        .clone()
        .oneshot(request(
            "POST",
            "/kit/retrieve",
            json!({
                "table":"docs",
                "retriever":{"sparse":{"column_id":5,"query":[[1,1.0]],"k":2}}
            }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: JsonValue =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(body["hits"].as_array().unwrap().len(), 1);
    assert_eq!(body["hits"][0]["rank"], 1);

    let response = app
        .clone()
        .oneshot(request("GET", "/kit/schema/docs", json!({})))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: JsonValue =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert!(body["columns"]
        .as_array()
        .unwrap()
        .iter()
        .all(|column| column["id"] != 4));
    assert!(body["indexes"]
        .as_array()
        .unwrap()
        .iter()
        .all(|index| index["column_id"] != 4));

    let response = app
        .clone()
        .oneshot(request("GET", "/tables/docs/count", json!({})))
        .await
        .unwrap();
    let body: JsonValue =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(body, json!({ "count": 1 }));

    let response = app
        .clone()
        .oneshot(request(
            "POST",
            "/kit/query",
            json!({ "table": "docs", "projection": [1, 2, 3] }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: JsonValue =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);
    assert!(body["rows"][0]["cells"]
        .as_array()
        .unwrap()
        .contains(&json!("***")));

    let response = app
        .clone()
        .oneshot(request(
            "POST",
            "/tables/docs/put",
            json!({ "row": [1, 3, 2, "bob", 3, "stolen", 4, 30] }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let response = app
        .clone()
        .oneshot(request("POST", "/sessions", json!({})))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: JsonValue =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    let session_id = body["session_id"].as_str().unwrap();
    let mut session_request = request(
        "POST",
        "/sql",
        json!({ "sql": "SELECT COUNT(*) AS n FROM docs" }),
    );
    session_request
        .headers_mut()
        .insert("x-session-id", session_id.parse().unwrap());
    let response = app.oneshot(session_request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: JsonValue =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(body, json!([{ "n": 1 }]));
}

#[tokio::test]
async fn admin_principal_allows_history_retention() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_user("admin", "pw").unwrap();
    db.set_user_admin("admin", true).unwrap();

    let app = build_app_full(
        Arc::clone(&db),
        std::iter::empty::<Arc<dyn ExternalTableModule>>(),
        None,
        None,
        true,
    );

    let admin_basic = "Basic YWRtaW46cHc="; // admin:pw

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/history/retention")
                .header("authorization", admin_basic)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: JsonValue =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(body.as_object().unwrap().len(), 2);
    assert!(body["history_retention_epochs"].is_u64());
    assert!(body["earliest_retained_epoch"].is_u64());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/kit/ai/metrics")
                .header("authorization", admin_basic)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/history/retention")
                .header("authorization", admin_basic)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"history_retention_epochs": 7}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: JsonValue =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(body.as_object().unwrap().len(), 2);
    assert_eq!(body["history_retention_epochs"], 7);
    assert!(body["earliest_retained_epoch"].is_u64());
}

#[tokio::test]
async fn bearer_token_uses_current_admin_when_database_requires_auth() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Database::create_with_credentials(dir.path(), "admin", "database-password").unwrap(),
    );
    db.create_table("docs", schema()).unwrap();
    let app = build_app_full(
        db,
        std::iter::empty::<Arc<dyn ExternalTableModule>>(),
        Some("server-token".into()),
        None,
        false,
    );
    let bearer_request = |method: &str, uri: &str, body: JsonValue| {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("authorization", "Bearer server-token")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };

    let response = app
        .clone()
        .oneshot(bearer_request(
            "POST",
            "/sql",
            json!({ "sql": "SELECT COUNT(*) AS n FROM docs" }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json_body(response).await, json!([{ "n": 0 }]));

    let response = app
        .oneshot(bearer_request("POST", "/sessions", json!({})))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn require_auth_without_http_auth_fails_closed_for_all_route_groups() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Database::create_with_credentials(dir.path(), "admin", "database-password").unwrap(),
    );
    let app = build_app_full(
        db,
        std::iter::empty::<Arc<dyn ExternalTableModule>>(),
        None,
        None,
        false,
    );
    for (method, uri) in [
        ("GET", "/health"),
        ("GET", "/capabilities"),
        ("GET", "/metrics"),
        ("GET", "/audit"),
        ("GET", "/tables"),
        ("POST", "/sql"),
        ("POST", "/txn"),
        ("POST", "/sessions"),
        ("GET", "/procedures"),
        ("GET", "/triggers"),
        ("GET", "/kit/schema"),
        ("POST", "/compact"),
        ("GET", "/wal/stream"),
        ("GET", "/replication/snapshot"),
        ("GET", "/events"),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {uri}"
        );
    }
}

async fn json_body(response: axum::response::Response) -> JsonValue {
    serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap()
}
