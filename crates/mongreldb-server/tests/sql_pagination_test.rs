use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use mongreldb_core::{ColumnDef, ColumnFlags, Database, Permission, Schema, TypeId};
use mongreldb_server::{build_app, build_app_full};
use serde_json::{json, Value};
use std::sync::Arc;
use tempfile::tempdir;
use tower::ServiceExt;

fn post(path: &str, body: Value, authorization: Option<&str>) -> Request<Body> {
    let mut request = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json");
    if let Some(authorization) = authorization {
        request = request.header("authorization", authorization);
    }
    request.body(Body::from(body.to_string())).unwrap()
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn database() -> (tempfile::TempDir, Arc<Database>) {
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
                }],
                ..Schema::default()
            },
        )
        .unwrap();
    (directory, database)
}

async fn sql(app: axum::Router, statement: &str) -> Value {
    let response = app
        .oneshot(post("/sql", json!({ "sql": statement }), None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    json_body(response).await
}

async fn seed(app: axum::Router) {
    for id in 1..=5 {
        sql(
            app.clone(),
            &format!("INSERT INTO items (id) VALUES ({id})"),
        )
        .await;
    }
}

#[tokio::test]
async fn pages_are_projected_stable_and_retryable() {
    let (_directory, database) = database();
    let app = build_app(Arc::clone(&database));
    seed(app.clone()).await;
    let first = app
        .clone()
        .oneshot(post(
            "/sql",
            json!({
                "sql": "SELECT id, id * 10 AS hidden FROM items ORDER BY id",
                "max_output_rows": 10,
                "max_output_bytes": 4096,
                "pagination": {
                    "page_size_rows": 2,
                    "projection": ["id"],
                    "max_page_bytes": 1024,
                    "max_page_tokens": 256
                }
            }),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first = json_body(first).await;
    assert_eq!(first["rows"], json!([{ "id": 1 }, { "id": 2 }]));
    assert_eq!(first["page"]["projection"], json!(["id"]));
    assert_eq!(first["page"]["total_rows"], 5);
    assert_eq!(first["page"]["snapshot"], "retained_result");
    assert!(first["page"]["byte_count"].as_u64().unwrap() <= 1024);
    assert!(first["page"]["estimated_tokens"].as_u64().unwrap() <= 256);
    let cursor = first["next_cursor"].as_str().unwrap().to_owned();

    sql(app.clone(), "INSERT INTO items (id) VALUES (6)").await;
    let cancelled_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let cancel = app
        .clone()
        .oneshot(post(
            &format!("/queries/{cancelled_id}/cancel"),
            json!({}),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(cancel.status(), StatusCode::ACCEPTED);
    let cancelled = app
        .clone()
        .oneshot(post(
            "/sql/continue",
            json!({ "cursor": cursor.clone(), "operation_id": cancelled_id }),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(cancelled.status(), StatusCode::from_u16(499).unwrap());
    assert_eq!(cancelled.headers()["x-mongreldb-query-id"], cancelled_id);
    let cancelled = json_body(cancelled).await;
    assert_eq!(cancelled["query_id"], cancelled_id);
    assert_eq!(cancelled["committed"], false);

    let second_request = |operation_id| {
        post(
            "/sql/continue",
            json!({ "cursor": cursor.clone(), "operation_id": operation_id }),
            None,
        )
    };
    let second_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let second = app
        .clone()
        .oneshot(second_request(second_id))
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    assert_eq!(second.headers()["x-mongreldb-query-id"], second_id);
    let second = json_body(second).await;
    assert_eq!(second["rows"], json!([{ "id": 3 }, { "id": 4 }]));
    assert_eq!(second["page"]["total_rows"], 5);
    let retry = json_body(
        app.clone()
            .oneshot(second_request("cccccccccccccccccccccccccccccccc"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(retry, second);

    let final_cursor = second["next_cursor"].as_str().unwrap().to_owned();
    let final_page = app
        .clone()
        .oneshot(post(
            "/sql/continue",
            json!({
                "cursor": final_cursor.clone(),
                "operation_id": "dddddddddddddddddddddddddddddddd"
            }),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(final_page.status(), StatusCode::OK);
    let final_page = json_body(final_page).await;
    assert_eq!(final_page["rows"], json!([{ "id": 5 }]));
    assert!(final_page["next_cursor"].is_null());
    let final_retry = json_body(
        app.clone()
            .oneshot(post(
                "/sql/continue",
                json!({
                    "cursor": final_cursor,
                    "operation_id": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                }),
                None,
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(final_retry, final_page);

    let mut tampered = cursor.as_bytes().to_vec();
    let last = tampered.len() - 1;
    tampered[last] = if tampered[last] == b'a' { b'b' } else { b'a' };
    let tampered = app
        .clone()
        .oneshot(post(
            "/sql/continue",
            json!({ "cursor": String::from_utf8(tampered).unwrap() }),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(tampered.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        json_body(tampered).await["error"]["code"],
        "INVALID_SQL_CURSOR"
    );

    let other_server = build_app(database);
    let other = other_server
        .oneshot(post(
            "/sql/continue",
            json!({ "cursor": first["next_cursor"] }),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(other.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn pagination_rejects_writes_bad_projection_and_oversized_rows() {
    let (_directory, database) = database();
    let app = build_app(database);
    let write = app
        .clone()
        .oneshot(post(
            "/sql",
            json!({
                "sql": "INSERT INTO items (id) VALUES (1)",
                "pagination": { "page_size_rows": 1, "projection": ["id"] }
            }),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(write.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        json_body(write).await["error"]["code"],
        "PAGINATION_REQUIRES_SINGLE_READ_QUERY"
    );
    assert_eq!(
        sql(app.clone(), "SELECT count(*) AS n FROM items").await[0]["n"],
        0
    );

    sql(app.clone(), "INSERT INTO items (id) VALUES (1)").await;
    let projection = app
        .clone()
        .oneshot(post(
            "/sql",
            json!({
                "sql": "SELECT id FROM items",
                "pagination": { "page_size_rows": 1, "projection": ["missing"] }
            }),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(projection.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        json_body(projection).await["error"]["code"],
        "INVALID_SQL_PROJECTION"
    );

    let page_limit = app
        .oneshot(post(
            "/sql",
            json!({
                "sql": "SELECT id FROM items",
                "pagination": {
                    "page_size_rows": 1,
                    "projection": ["id"],
                    "max_page_bytes": 2
                }
            }),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(page_limit.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        json_body(page_limit).await["error"]["code"],
        "RESULT_LIMIT_EXCEEDED"
    );
}

#[tokio::test]
async fn continuation_is_owner_bound_under_authentication() {
    let directory = tempdir().unwrap();
    let database =
        Arc::new(Database::create_with_credentials(directory.path(), "admin", "pw").unwrap());
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
    database.create_user("alice", "pw").unwrap();
    database.create_user("bob", "pw").unwrap();
    database.create_role("reader").unwrap();
    database
        .grant_permission(
            "reader",
            Permission::Select {
                table: "items".into(),
            },
        )
        .unwrap();
    database.grant_role("alice", "reader").unwrap();
    database.grant_role("bob", "reader").unwrap();
    database
        .transaction(|transaction| {
            for id in 1..=3 {
                transaction.put("items", vec![(1, mongreldb_core::Value::Int64(id))])?;
            }
            Ok(())
        })
        .unwrap();
    let app = build_app_full(Arc::clone(&database), std::iter::empty(), None, None, true);
    let first = app
        .clone()
        .oneshot(post(
            "/sql",
            json!({
                "sql": "SELECT id FROM items ORDER BY id",
                "pagination": { "page_size_rows": 1, "projection": ["id"] }
            }),
            Some("Basic YWxpY2U6cHc="),
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let cursor = json_body(first).await["next_cursor"]
        .as_str()
        .unwrap()
        .to_owned();
    let other_owner = app
        .clone()
        .oneshot(post(
            "/sql/continue",
            json!({ "cursor": cursor.clone() }),
            Some("Basic Ym9iOnB3"),
        ))
        .await
        .unwrap();
    assert_eq!(other_owner.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        json_body(other_owner).await["error"]["code"],
        "SQL_CURSOR_NOT_FOUND"
    );

    database.drop_user("alice").unwrap();
    database.create_user("alice", "pw").unwrap();
    database.grant_role("alice", "reader").unwrap();
    let replacement = app
        .oneshot(post(
            "/sql/continue",
            json!({ "cursor": cursor }),
            Some("Basic YWxpY2U6cHc="),
        ))
        .await
        .unwrap();
    assert_eq!(replacement.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        json_body(replacement).await["error"]["code"],
        "SQL_CURSOR_NOT_FOUND"
    );
}
