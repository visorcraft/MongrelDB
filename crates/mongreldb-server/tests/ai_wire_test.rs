use mongreldb_core::Database;
use mongreldb_server::build_app;
use std::sync::Arc;
use tempfile::tempdir;
use tower::ServiceExt;

async fn request(
    app: axum::Router,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
) -> (u16, serde_json::Value) {
    let mut builder = axum::http::Request::builder().method(method).uri(uri);
    let body = match body {
        Some(body) => {
            builder = builder.header("content-type", "application/json");
            axum::body::Body::from(serde_json::to_vec(&body).unwrap())
        }
        None => axum::body::Body::empty(),
    };
    let response = app.oneshot(builder.body(body).unwrap()).await.unwrap();
    let status = response.status().as_u16();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| serde_json::json!({"text": String::from_utf8_lossy(&bytes)}));
    (status, json)
}

#[tokio::test]
async fn kit_ai_indexes_work_over_wire_and_validate_values() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(db);

    let create = serde_json::json!({
        "name": "docs",
        "columns": [
            {"id": 1, "name": "id", "ty": "int64", "primary_key": true},
            {"id": 2, "name": "status", "ty": "bytes"},
            {"id": 3, "name": "sparse", "ty": "bytes"},
            {"id": 4, "name": "embedding", "ty": "embedding(8)"},
            {"id": 5, "name": "members", "ty": "bytes"}
        ],
        "indexes": [
            {"name": "status_bm", "column_id": 2, "kind": "bitmap"},
            {"name": "sparse_idx", "column_id": 3, "kind": "sparse"},
            {"name": "embedding_ann", "column_id": 4, "kind": "hnsw"},
            {"name": "members_minhash", "column_id": 5, "kind": "lsh"}
        ]
    });
    let (status, body) = request(app.clone(), "POST", "/kit/create_table", Some(create)).await;
    assert_eq!(status, 200, "{body}");

    let (status, schema) = request(app.clone(), "GET", "/kit/schema/docs", None).await;
    assert_eq!(status, 200, "{schema}");
    assert_eq!(schema["indexes"].as_array().unwrap().len(), 4);
    assert_eq!(schema["indexes"][2]["kind"], "ann");
    assert_eq!(schema["indexes"][3]["kind"], "minhash");

    let rows = [
        serde_json::json!([
            1,
            1,
            2,
            "published",
            3,
            [[1, 2.0], [2, 1.0]],
            4,
            [1, -1, 1, -1, 1, -1, 1, -1],
            5,
            ["a", "b", "c", "d"]
        ]),
        serde_json::json!([
            1,
            2,
            2,
            "draft",
            3,
            [[1, 1.0], [3, 3.0]],
            4,
            [-1, 1, -1, 1, -1, 1, -1, 1],
            5,
            ["a", "b", "c", "x"]
        ]),
        serde_json::json!([
            1,
            3,
            2,
            "published",
            3,
            [[2, 5.0]],
            4,
            [1, -1, 1, -1, 1, -1, -1, -1],
            5,
            ["p", "q", "r", "s"]
        ]),
    ];
    for cells in rows {
        let txn = serde_json::json!({"ops": [{"put": {"table": "docs", "cells": cells}}]});
        let (status, body) = request(app.clone(), "POST", "/kit/txn", Some(txn)).await;
        assert_eq!(status, 200, "{body}");
    }

    let cases = [
        serde_json::json!({"ann":{"column_id":4,"query":[1,-1,1,-1,1,-1,1,-1],"k":1}}),
        serde_json::json!({"sparse_match":{"column_id":3,"query":[[1,2.0]],"k":1}}),
        serde_json::json!({"minhash_similar_members":{"column_id":5,"members":["a","b","c","d"],"k":1}}),
    ];
    for condition in cases {
        let query = serde_json::json!({"table":"docs","conditions":[condition],"projection":[1]});
        let (status, body) = request(app.clone(), "POST", "/kit/query", Some(query)).await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["rows"].as_array().unwrap().len(), 1, "{body}");
        assert_eq!(body["rows"][0]["cells"], serde_json::json!([1, 1]));
    }

    let retrieve = serde_json::json!({
        "table":"docs",
        "retriever":{"sparse":{"column_id":3,"query":[[1,2.0]],"k":2}}
    });
    let (status, body) = request(app.clone(), "POST", "/kit/retrieve", Some(retrieve)).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["hits"][0]["rank"], 1);
    assert_eq!(body["hits"][0]["score"]["kind"], "sparse_dot_product");
    assert_eq!(body["hits"][0]["score"]["value"], 4.0);

    let exact = serde_json::json!({
        "table":"docs", "column_id":5, "members":["a","b","c","d"],
        "candidate_k":10, "min_jaccard":0.7, "limit":10
    });
    let (status, body) = request(app.clone(), "POST", "/kit/set_similarity", Some(exact)).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["hits"].as_array().unwrap().len(), 1);
    assert_eq!(body["hits"][0]["exact_jaccard"], 1.0);

    let search = serde_json::json!({
        "table":"docs",
        "must":[{"bitmap_eq":{"column_id":2,"value":"published"}}],
        "retrievers":[
            {"name":"dense","weight":1.0,"ann":{"column_id":4,"query":[1,-1,1,-1,1,-1,1,-1],"k":1}},
            {"name":"sparse","weight":1.0,"sparse":{"column_id":3,"query":[[2,1.0]],"k":1}}
        ],
        "fusion":{"reciprocal_rank":{"constant":60}},
        "limit":10,
        "projection":[1]
    });
    let (status, body) = request(app.clone(), "POST", "/kit/search", Some(search)).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["hits"].as_array().unwrap().len(), 2, "{body}");
    assert_eq!(body["hits"][0]["cells"], serde_json::json!([1, 1]));
    assert_eq!(body["hits"][1]["cells"], serde_json::json!([1, 3]));
    assert!(body["hits"]
        .as_array()
        .unwrap()
        .iter()
        .all(|hit| hit["fused_score"].as_f64().unwrap() > 0.0));

    let invalid = serde_json::json!({"ops":[{"put":{"table":"docs","cells":[1,9,4,[1,2]]}}]});
    let (status, body) = request(app, "POST", "/kit/txn", Some(invalid)).await;
    assert_eq!(status, 400, "{body}");
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("column 4: embedding dimension must be 8"));
}
