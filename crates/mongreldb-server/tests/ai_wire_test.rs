use mongreldb_core::{
    auth::Permission,
    schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId},
    Database, Value,
};
use mongreldb_server::{build_app, build_app_full};
use std::sync::Arc;
use tempfile::tempdir;
use tower::ServiceExt;

async fn request(
    app: axum::Router,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
) -> (u16, serde_json::Value) {
    request_with_authorization(app, method, uri, body, None).await
}

async fn request_with_authorization(
    app: axum::Router,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
    authorization: Option<&str>,
) -> (u16, serde_json::Value) {
    let mut builder = axum::http::Request::builder().method(method).uri(uri);
    if let Some(authorization) = authorization {
        builder = builder.header("authorization", authorization);
    }
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
            {"name": "embedding_ann", "column_id": 4, "kind": "hnsw", "options":{"ann":{"m":8,"ef_construction":32,"ef_search":17}}},
            {"name": "members_minhash", "column_id": 5, "kind": "lsh", "options":{"minhash":{"permutations":64,"bands":16}}}
        ]
    });
    let (status, body) = request(app.clone(), "POST", "/kit/create_table", Some(create)).await;
    assert_eq!(status, 200, "{body}");

    let (status, schema) = request(app.clone(), "GET", "/kit/schema/docs", None).await;
    assert_eq!(status, 200, "{schema}");
    assert_eq!(schema["indexes"].as_array().unwrap().len(), 4);
    assert_eq!(schema["indexes"][2]["kind"], "ann");
    assert_eq!(schema["indexes"][3]["kind"], "minhash");
    assert_eq!(schema["indexes"][2]["options"]["ann"]["ef_search"], 17);
    assert_eq!(
        schema["indexes"][3]["options"]["minhash"]["permutations"],
        64
    );

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
        assert_eq!(status, 400, "{body}");
        assert!(body.to_string().contains("ranked AI conditions"));
    }

    for condition in [
        serde_json::json!({"ann":{"column_id":4,"query":[1,-1,1,-1,1,-1,1,-1],"k":3}}),
        serde_json::json!({"sparse_match":{"column_id":3,"query":[[1,2.0]],"k":3}}),
        serde_json::json!({"minhash_similar_members":{"column_id":5,"members":["a","b","c","d"],"k":3}}),
    ] {
        let query = serde_json::json!({
            "table":"docs",
            "conditions":[{"bitmap_eq":{"column_id":2,"value":"published"}}, condition],
            "projection":[1]
        });
        let (status, body) = request(app.clone(), "POST", "/kit/query", Some(query)).await;
        assert_eq!(status, 400, "{body}");
        assert!(body.to_string().contains("ranked AI conditions"));
    }

    let retrieve = serde_json::json!({
        "table":"docs",
        "retriever":{"sparse":{"column_id":3,"query":[[1,2.0]],"k":2}}
    });
    let (status, body) =
        request(app.clone(), "POST", "/kit/retrieve", Some(retrieve.clone())).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["hits"][0]["rank"], 1);
    assert_eq!(body["hits"][0]["score"]["kind"], "sparse_dot_product");
    assert_eq!(body["hits"][0]["score"]["value"], 4.0);

    let exact = serde_json::json!({
        "table":"docs", "column_id":5, "members":["a","b","c","d"],
        "candidate_k":10, "min_jaccard":0.7, "limit":10
    });
    let (status, body) = request(
        app.clone(),
        "POST",
        "/kit/set_similarity",
        Some(exact.clone()),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["hits"].as_array().unwrap().len(), 1);
    assert_eq!(body["hits"][0]["exact_jaccard"], 1.0);

    let ann_rerank = serde_json::json!({
        "table":"docs", "column_id":4,
        "query":[1,-1,1,-1,1,-1,1,-1],
        "candidate_k":10, "limit":2, "metric":"cosine"
    });
    let (status, body) = request(
        app.clone(),
        "POST",
        "/kit/ann_rerank",
        Some(ann_rerank.clone()),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(body["hits"][0]["exact_score"].as_f64().unwrap().is_finite());
    assert_eq!(body["hits"][0]["candidate_distance"]["kind"], "hamming");
    assert!(body["hits"][0]["candidate_distance"]["value"].is_number());
    assert!(body["hits"][0]["hamming_distance"].is_null());

    let search = serde_json::json!({
        "table":"docs",
        "must":[{"bitmap_eq":{"column_id":2,"value":"published"}}],
        "retrievers":[
            {"name":"dense","weight":1.0,"ann":{"column_id":4,"query":[1,-1,1,-1,1,-1,1,-1],"k":1}},
            {"name":"sparse","weight":1.0,"sparse":{"column_id":3,"query":[[2,1.0]],"k":1}}
        ],
        "fusion":{"reciprocal_rank":{"constant":60}},
        "rerank":{"exact_vector":{
            "embedding_column":4,
            "query":[1,-1,1,-1,1,-1,-1,-1],
            "metric":"cosine",
            "candidate_limit":10,
            "weight":1.0
        }},
        "limit":10,
        "projection":[1],
        "explain":true
    });
    let (status, body) = request(app.clone(), "POST", "/kit/search", Some(search.clone())).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["hits"].as_array().unwrap().len(), 2, "{body}");
    assert_eq!(body["hits"][0]["cells"], serde_json::json!([1, 3]));
    assert_eq!(body["hits"][1]["cells"], serde_json::json!([1, 1]));
    assert_eq!(body["hits"][0]["final_rank"], 1);
    assert!(body["hits"][0]["exact_rerank_score"].is_number());
    assert!(body["hits"]
        .as_array()
        .unwrap()
        .iter()
        .all(|hit| hit["fused_score"].as_f64().unwrap() > 0.0));
    assert!(body["trace"].is_object(), "{body}");

    let mut page = search.clone();
    page["limit"] = serde_json::json!(1);
    page["explain"] = serde_json::json!(false);
    let (status, first_page) =
        request(app.clone(), "POST", "/kit/search", Some(page.clone())).await;
    assert_eq!(status, 200, "{first_page}");
    let cursor = first_page["next_cursor"].as_str().unwrap().to_string();
    let first_row = first_page["hits"][0]["row_id"]
        .as_str()
        .unwrap()
        .to_string();

    let insert = serde_json::json!({"ops":[{"put":{"table":"docs","cells":[
        1,4,2,"published",3,[[2,4.0]],4,[1,-1,1,-1,1,-1,-1,-1],5,["new"]
    ]}}]});
    assert_eq!(
        request(app.clone(), "POST", "/kit/txn", Some(insert))
            .await
            .0,
        200
    );
    page["cursor"] = serde_json::json!(cursor);
    let (status, second_page) =
        request(app.clone(), "POST", "/kit/search", Some(page.clone())).await;
    assert_eq!(status, 409, "{second_page}");
    assert_eq!(second_page["error"]["code"], "CURSOR_STALE");
    assert!(!second_page.to_string().contains(&first_row));

    page["projection"] = serde_json::json!([1, 2]);
    let (status, body) = request(app.clone(), "POST", "/kit/search", Some(page)).await;
    assert_eq!(status, 400, "{body}");
    assert!(body.to_string().contains("cursor does not match"));

    let (status, body) = request(app.clone(), "GET", "/kit/ai/metrics", None).await;
    assert_eq!(status, 200, "{body}");

    for (path, mut request_body) in [
        ("/kit/retrieve", retrieve),
        ("/kit/set_similarity", exact),
        ("/kit/ann_rerank", ann_rerank),
        ("/kit/search", search),
    ] {
        request_body["max_work"] = serde_json::json!(1);
        let (status, body) = request(app.clone(), "POST", path, Some(request_body)).await;
        assert_eq!(status, 429, "{path}: {body}");
        assert_eq!(body["error"]["code"], "WORK_BUDGET_EXCEEDED", "{body}");
    }

    let invalid = serde_json::json!({"ops":[{"put":{"table":"docs","cells":[1,9,4,[1,2]]}}]});
    let (status, body) = request(app.clone(), "POST", "/kit/txn", Some(invalid)).await;
    assert_eq!(status, 400, "{body}");
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("column 4: embedding dimension must be 8"));

    for (column_id, value, message) in [
        (
            3,
            serde_json::json!("not sparse"),
            "sparse vector must be an array",
        ),
        (
            5,
            serde_json::json!([{"nested":true}]),
            "set members must be strings, numbers, or booleans",
        ),
    ] {
        let invalid =
            serde_json::json!({"ops":[{"put":{"table":"docs","cells":[1,9,column_id,value]}}]});
        let (status, body) = request(app.clone(), "POST", "/kit/txn", Some(invalid)).await;
        assert_eq!(status, 400, "{body}");
        assert!(body["error"]["message"].as_str().unwrap().contains(message));
    }

    let invalid_query = serde_json::json!({
        "table":"docs",
        "conditions":[{"ann":{"column_id":4,"query":[1,2],"k":1}}],
        "projection":[1]
    });
    let (status, body) = request(app.clone(), "POST", "/kit/query", Some(invalid_query)).await;
    assert_eq!(status, 400, "{body}");
    assert!(body.to_string().contains("ranked AI conditions"));

    drop(app);
    let reopened = Arc::new(Database::open(dir.path()).unwrap());
    let app = build_app(reopened);
    for condition in [
        serde_json::json!({"ann":{"column_id":4,"query":[1,-1,1,-1,1,-1,1,-1],"k":1}}),
        serde_json::json!({"sparse_match":{"column_id":3,"query":[[1,2.0]],"k":1}}),
        serde_json::json!({"minhash_similar_members":{"column_id":5,"members":["a","b","c","d"],"k":1}}),
    ] {
        let query = serde_json::json!({"table":"docs","conditions":[condition],"projection":[1]});
        let (status, body) = request(app.clone(), "POST", "/kit/query", Some(query)).await;
        assert_eq!(status, 400, "{body}");
        assert!(body.to_string().contains("ranked AI conditions"));
    }
}

#[tokio::test]
async fn kit_schema_round_trips_all_index_options_and_embedding_source() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let app = build_app(db);

    let create = serde_json::json!({
        "name": "search_docs",
        "columns": [
            {"id": 1, "name": "id", "ty": "int64", "primary_key": true},
            {"id": 2, "name": "embedding", "ty": "embedding(3)", "embedding_source": {
                "kind": "generated_column_spec",
                "spec": {
                    "provider_id": "tenant-embeddings",
                    "model_id": "text-model",
                    "model_version": "2026-07",
                    "source_columns": [4],
                    "input_template": "{body}",
                    "dimension": 3,
                    "normalization": "l2",
                    "failure_policy": "abort_write"
                }
            }},
            {"id": 3, "name": "status", "ty": "bytes"},
            {"id": 4, "name": "body", "ty": "bytes"},
            {"id": 5, "name": "rank", "ty": "int64"},
            {"id": 6, "name": "members", "ty": "bytes"},
            {"id": 7, "name": "sparse", "ty": "bytes"}
        ],
        "indexes": [
            {"name": "status_bm", "column_id": 3, "kind": "bitmap", "predicate": "status IS NOT NULL"},
            {"name": "body_fm", "column_id": 4, "kind": "fm_index"},
            {"name": "embedding_ann", "column_id": 2, "kind": "ann", "options": {"ann": {
                "m": 12, "ef_construction": 48, "ef_search": 24, "quantization": "dense"
            }}},
            {"name": "rank_range", "column_id": 5, "kind": "learned_range", "options": {"learned_range": {"epsilon": 8}}},
            {"name": "members_minhash", "column_id": 6, "kind": "minhash", "options": {"minhash": {"permutations": 64, "bands": 16}}},
            {"name": "sparse_idx", "column_id": 7, "kind": "sparse"}
        ]
    });
    let (status, body) = request(app.clone(), "POST", "/kit/create_table", Some(create)).await;
    assert_eq!(status, 200, "{body}");

    let (status, schema) = request(app, "GET", "/kit/schema/search_docs", None).await;
    assert_eq!(status, 200, "{schema}");
    assert_eq!(
        schema["columns"][1]["embedding_source"]["kind"],
        "generated_column_spec"
    );
    assert_eq!(
        schema["columns"][1]["embedding_source"]["spec"]["model_id"],
        "text-model"
    );
    assert_eq!(schema["indexes"].as_array().unwrap().len(), 6);
    assert_eq!(schema["indexes"][0]["predicate"], "status IS NOT NULL");
    assert_eq!(
        schema["indexes"][2]["options"]["ann"]["quantization"],
        "dense"
    );
    assert_eq!(schema["indexes"][2]["options"]["ann"]["m"], 12);
    assert_eq!(
        schema["indexes"][3]["options"]["learned_range"]["epsilon"],
        8
    );
    assert_eq!(schema["indexes"][4]["options"]["minhash"]["bands"], 16);
}

#[tokio::test]
async fn kit_ai_routes_require_every_used_column() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create_with_credentials(dir.path(), "admin", "admin-pw").unwrap());
    db.create_table(
        "docs",
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
                    name: "status".into(),
                    ty: TypeId::Bytes,
                    flags: ColumnFlags::empty(),
                    default_value: None,
                    embedding_source: None,
                },
                ColumnDef {
                    id: 3,
                    name: "sparse".into(),
                    ty: TypeId::Bytes,
                    flags: ColumnFlags::empty(),
                    default_value: None,
                    embedding_source: None,
                },
                ColumnDef {
                    id: 4,
                    name: "embedding".into(),
                    ty: TypeId::Embedding { dim: 2 },
                    flags: ColumnFlags::empty(),
                    default_value: None,
                    embedding_source: None,
                },
                ColumnDef {
                    id: 5,
                    name: "members".into(),
                    ty: TypeId::Bytes,
                    flags: ColumnFlags::empty(),
                    default_value: None,
                    embedding_source: None,
                },
                ColumnDef {
                    id: 6,
                    name: "projected".into(),
                    ty: TypeId::Bytes,
                    flags: ColumnFlags::empty(),
                    default_value: None,
                    embedding_source: None,
                },
            ],
            indexes: vec![
                IndexDef {
                    name: "status_bm".into(),
                    column_id: 2,
                    kind: IndexKind::Bitmap,
                    predicate: None,
                    options: Default::default(),
                },
                IndexDef {
                    name: "sparse_idx".into(),
                    column_id: 3,
                    kind: IndexKind::Sparse,
                    predicate: None,
                    options: Default::default(),
                },
                IndexDef {
                    name: "ann_idx".into(),
                    column_id: 4,
                    kind: IndexKind::Ann,
                    predicate: None,
                    options: Default::default(),
                },
                IndexDef {
                    name: "set_idx".into(),
                    column_id: 5,
                    kind: IndexKind::MinHash,
                    predicate: None,
                    options: Default::default(),
                },
            ],
            ..Schema::default()
        },
    )
    .unwrap();
    {
        let handle = db.table("docs").unwrap();
        let mut table = handle.lock();
        table
            .put(vec![
                (1, Value::Int64(1)),
                (2, Value::Bytes(b"published".to_vec())),
                (
                    3,
                    Value::Bytes(bincode::serialize(&vec![(1u32, 1.0f32)]).unwrap()),
                ),
                (4, Value::Embedding(vec![1.0, 0.0])),
                (5, Value::Bytes(br#"["a"]"#.to_vec())),
                (6, Value::Bytes(b"visible".to_vec())),
            ])
            .unwrap();
        table.commit().unwrap();
    }
    db.create_user("alice", "alice-pw").unwrap();
    db.create_role("reader").unwrap();
    db.grant_role("alice", "reader").unwrap();

    let retrieve = serde_json::json!({
        "table":"docs", "retriever":{"sparse":{"column_id":3,"query":[[1,1.0]],"k":1}}
    });
    let ann = serde_json::json!({
        "table":"docs", "column_id":4, "query":[1,0], "candidate_k":1, "limit":1, "metric":"cosine"
    });
    let set = serde_json::json!({
        "table":"docs", "column_id":5, "members":["a"], "candidate_k":1, "min_jaccard":0, "limit":1
    });
    let search = serde_json::json!({
        "table":"docs",
        "must":[{"bitmap_eq":{"column_id":2,"value":"published"}}],
        "retrievers":[{"name":"sparse","sparse":{"column_id":3,"query":[[1,1.0]],"k":1}}],
        "fusion":{"reciprocal_rank":{"constant":60}},
        "rerank":{"exact_vector":{"embedding_column":4,"query":[1,0],"metric":"cosine","candidate_limit":1,"weight":1}},
        "limit":1, "projection":[6]
    });
    let query = serde_json::json!({
        "table":"docs", "conditions":[{"bitmap_eq":{"column_id":2,"value":"published"}}], "projection":[1]
    });
    let cases = [
        ("/kit/retrieve", retrieve.clone(), vec![]),
        (
            "/kit/retrieve",
            retrieve,
            vec!["id", "status", "embedding", "members", "projected"],
        ),
        (
            "/kit/ann_rerank",
            ann,
            vec!["id", "status", "sparse", "members", "projected"],
        ),
        (
            "/kit/set_similarity",
            set,
            vec!["id", "status", "sparse", "embedding", "projected"],
        ),
        (
            "/kit/search",
            search.clone(),
            vec!["id", "status", "sparse", "embedding", "members"],
        ),
        (
            "/kit/search",
            search.clone(),
            vec!["id", "status", "embedding", "members", "projected"],
        ),
        (
            "/kit/search",
            search,
            vec!["id", "status", "sparse", "members", "projected"],
        ),
        (
            "/kit/query",
            query,
            vec!["id", "sparse", "embedding", "members", "projected"],
        ),
    ];
    let mut previous = None;
    for (path, body, columns) in cases {
        if let Some(permission) = previous.take() {
            db.revoke_permission("reader", permission).unwrap();
        }
        if !columns.is_empty() {
            let permission = Permission::SelectColumns {
                table: "docs".into(),
                columns: columns.into_iter().map(str::to_string).collect(),
            };
            db.grant_permission("reader", permission.clone()).unwrap();
            previous = Some(permission);
        }
        let app = build_app_full(Arc::clone(&db), std::iter::empty(), None, None, true);
        let (status, response) = request_with_authorization(
            app,
            "POST",
            path,
            Some(body),
            Some("Basic YWxpY2U6YWxpY2UtcHc="),
        )
        .await;
        assert_eq!(status, 403, "{path}: {response}");
        assert_eq!(
            response["error"]["code"], "PERMISSION_DENIED",
            "{path}: {response}"
        );
        assert!(response.get("hits").is_none(), "{path}: {response}");
        assert!(response.get("rows").is_none(), "{path}: {response}");
        assert!(response.get("trace").is_none(), "{path}: {response}");
    }
}

#[tokio::test]
async fn kit_ai_deadline_bounds_table_lock_wait_and_releases_worker() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table(
        "docs",
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
                    name: "sparse".into(),
                    ty: TypeId::Bytes,
                    flags: ColumnFlags::empty(),
                    default_value: None,
                    embedding_source: None,
                },
            ],
            indexes: vec![IndexDef {
                name: "sparse_idx".into(),
                column_id: 2,
                kind: IndexKind::Sparse,
                predicate: None,
                options: Default::default(),
            }],
            ..Schema::default()
        },
    )
    .unwrap();
    let app = build_app(Arc::clone(&db));
    let request_body = serde_json::json!({
        "table":"docs", "retriever":{"sparse":{"column_id":2,"query":[[1,1.0]],"k":1}}, "deadline_ms":10
    });

    let handle = db.table("docs").unwrap();
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let _guard = handle.lock();
        locked_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    locked_rx.recv().unwrap();

    let started = std::time::Instant::now();
    let timed = tokio::spawn(request(
        app.clone(),
        "POST",
        "/kit/retrieve",
        Some(request_body.clone()),
    ));
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let (health, _) = request(app.clone(), "GET", "/health", None).await;
    assert_eq!(health, 200);
    let (status, response) = tokio::time::timeout(std::time::Duration::from_millis(300), timed)
        .await
        .expect("deadline response blocked")
        .unwrap();
    assert_eq!(status, 504, "{response}");
    assert_eq!(response["error"]["code"], "DEADLINE_EXCEEDED");
    assert!(started.elapsed() < std::time::Duration::from_millis(300));

    release_tx.send(()).unwrap();
    holder.join().unwrap();
    let mut retry = request_body;
    retry.as_object_mut().unwrap().remove("deadline_ms");
    let (status, response) = request(app, "POST", "/kit/retrieve", Some(retry)).await;
    assert_eq!(status, 200, "{response}");
}

#[tokio::test]
async fn remote_sql_rejects_boolean_ai_in_direct_and_prepared_queries() {
    let dir = tempdir().unwrap();
    let app = build_app(Arc::new(Database::create(dir.path()).unwrap()));

    for sql in [
        "SELECT ann_search(embedding, '[1]', 1)",
        "SELECT sparse_match(sparse, '[]', 1)",
    ] {
        let (status, _) = request(
            app.clone(),
            "POST",
            "/sql",
            Some(serde_json::json!({"sql":sql})),
        )
        .await;
        assert_eq!(status, 400);
    }
    let (status, _) = request(
        app.clone(),
        "POST",
        "/sql",
        Some(serde_json::json!({"sql":"SELECT 'ann_search(x)' AS text"})),
    )
    .await;
    assert_eq!(status, 200);

    let (status, session) = request(
        app.clone(),
        "POST",
        "/sessions",
        Some(serde_json::json!({})),
    )
    .await;
    assert_eq!(status, 200, "{session}");
    let session_id = session["session_id"].as_str().unwrap();
    let (status, _) = request(
        app,
        "POST",
        &format!("/sessions/{session_id}/prepare"),
        Some(serde_json::json!({
            "name":"unsafe_ai",
            "sql":"SELECT sparse_match(sparse, '[]', 1)"
        })),
    )
    .await;
    assert_eq!(status, 400);
}
