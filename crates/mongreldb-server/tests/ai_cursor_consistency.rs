use mongreldb_core::Database;
use mongreldb_server::build_app;
use std::sync::Arc;
use tempfile::{tempdir, TempDir};
use tower::ServiceExt;

async fn post(app: axum::Router, path: &str, body: serde_json::Value) -> (u16, serde_json::Value) {
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(path)
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

fn search(limit: usize) -> serde_json::Value {
    serde_json::json!({
        "table": "docs",
        "must": [{"bitmap_eq": {"column_id": 2, "value": "published"}}],
        "retrievers": [{
            "name": "dense",
            "weight": 1.0,
            "ann": {
                "column_id": 5,
                "query": [1, -1, 1, -1, 1, -1, 1, -1],
                "k": 10
            }
        }],
        "fusion": {"reciprocal_rank": {"constant": 60}},
        "rerank": {"exact_vector": {
            "embedding_column": 5,
            "query": [1, -1, 1, -1, 1, -1, 1, -1],
            "metric": "cosine",
            "candidate_limit": 10,
            "weight": 1.0
        }},
        "projection": [1, 3],
        "limit": limit
    })
}

async fn setup() -> (TempDir, Arc<Database>, axum::Router) {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(Arc::clone(&db));
    let create = serde_json::json!({
        "name": "docs",
        "columns": [
            {"id": 1, "name": "id", "ty": "int64", "primary_key": true},
            {"id": 2, "name": "status", "ty": "bytes"},
            {"id": 3, "name": "text", "ty": "bytes"},
            {"id": 4, "name": "sparse", "ty": "bytes"},
            {"id": 5, "name": "embedding", "ty": "embedding(8)"},
            {"id": 6, "name": "members", "ty": "bytes"}
        ],
        "indexes": [
            {"name": "status", "column_id": 2, "kind": "bitmap"},
            {"name": "text", "column_id": 3, "kind": "fm"},
            {"name": "sparse", "column_id": 4, "kind": "sparse"},
            {"name": "embedding", "column_id": 5, "kind": "hnsw"},
            {"name": "members", "column_id": 6, "kind": "lsh"}
        ]
    });
    assert_eq!(post(app.clone(), "/kit/create_table", create).await.0, 200);
    let rows = (1..=6)
        .map(|id| {
            let embedding = if id % 2 == 0 {
                vec![1, -1, 1, -1, 1, -1, 1, -1]
            } else {
                vec![1, -1, 1, -1, 1, -1, -1, -1]
            };
            serde_json::json!({"put": {"table": "docs", "cells": [
                1, id,
                2, "published",
                3, format!("document {id}"),
                4, [[id, 1.0]],
                5, embedding,
                6, [format!("tag-{id}"), "shared"]
            ]}})
        })
        .collect::<Vec<_>>();
    assert_eq!(
        post(app.clone(), "/kit/txn", serde_json::json!({"ops": rows}))
            .await
            .0,
        200
    );
    (dir, db, app)
}

async fn first_page(app: axum::Router) -> serde_json::Value {
    let (status, body) = post(app, "/kit/search", search(2)).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["hits"].as_array().unwrap().len(), 2);
    assert!(body["next_cursor"].is_string());
    body
}

#[tokio::test]
async fn search_cursor_preserves_global_rank_order_and_exact_rerank() {
    let (_dir, db, app) = setup().await;
    let (_, full) = post(app.clone(), "/kit/search", search(10)).await;
    let expected = full["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| hit["row_id"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();

    let mut request = search(2);
    let mut actual = Vec::new();
    let mut ranks = Vec::new();
    loop {
        let (status, page) = post(app.clone(), "/kit/search", request.clone()).await;
        assert_eq!(status, 200, "{page}");
        for hit in page["hits"].as_array().unwrap() {
            actual.push(hit["row_id"].as_str().unwrap().to_string());
            ranks.push(hit["final_rank"].as_u64().unwrap());
            assert!(hit["exact_rerank_score"].is_number());
        }
        let Some(cursor) = page["next_cursor"].as_str() else {
            break;
        };
        request["cursor"] = serde_json::json!(cursor);
    }
    assert_eq!(actual, expected);
    assert_eq!(ranks, (1..=expected.len() as u64).collect::<Vec<_>>());
    let unique = actual.iter().collect::<std::collections::HashSet<_>>();
    assert_eq!(unique.len(), actual.len());

    db.checkpoint().unwrap();
}

#[tokio::test]
async fn search_cursor_fails_stale_after_any_indexed_value_changes() {
    let updates = [
        serde_json::json!([2, "draft"]),
        serde_json::json!([3, "changed text"]),
        serde_json::json!([4, [[99, 1.0]]]),
        serde_json::json!([5, [-1, 1, -1, 1, -1, 1, -1, 1]]),
        serde_json::json!([6, ["changed"]]),
    ];
    for update_cells in updates {
        let (_dir, _db, app) = setup().await;
        let first = first_page(app.clone()).await;
        let mutation = serde_json::json!({"ops": [{"upsert": {
            "table": "docs",
            "cells": [1, 4],
            "update_cells": update_cells
        }}]});
        assert_eq!(post(app.clone(), "/kit/txn", mutation).await.0, 200);
        let mut request = search(2);
        request["cursor"] = first["next_cursor"].clone();
        let (status, body) = post(app, "/kit/search", request).await;
        assert_eq!(status, 409, "{body}");
        assert_eq!(body["error"]["code"], "CURSOR_STALE");
    }
}

#[tokio::test]
async fn search_cursor_fails_stale_after_checkpoint_and_rejects_mac_or_server_change() {
    let (_dir, db, app) = setup().await;
    let first = first_page(app.clone()).await;
    db.checkpoint().unwrap();
    let _ = db.compact_table("docs").unwrap();
    let mut request = search(2);
    request["cursor"] = first["next_cursor"].clone();
    let (status, page) = post(app.clone(), "/kit/search", request.clone()).await;
    assert_eq!(status, 409, "{page}");
    assert_eq!(page["error"]["code"], "CURSOR_STALE");

    let mut tampered = first["next_cursor"].as_str().unwrap().as_bytes().to_vec();
    let index = tampered.len() / 4;
    tampered[index] = if tampered[index] == b'0' { b'1' } else { b'0' };
    request["cursor"] = serde_json::json!(String::from_utf8(tampered).unwrap());
    assert_eq!(post(app, "/kit/search", request.clone()).await.0, 400);

    request["cursor"] = first["next_cursor"].clone();
    let other_server = build_app(db);
    assert_eq!(post(other_server, "/kit/search", request).await.0, 400);
}
