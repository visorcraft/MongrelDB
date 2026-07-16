use mongreldb_client::{
    AsyncMongrelClient, ClientError, KitAnnRerankRequest, KitErrorCode, KitRetrieveRequest,
    KitSearchRequest, KitSetSimilarityRequest, KitTxnRequest, KitVectorMetric, MongrelClient,
};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn mount_ai_routes(server: &MockServer, authorization: &str) {
    for (route, body) in [
        ("/kit/retrieve", serde_json::json!({"hits": []})),
        ("/kit/ann_rerank", serde_json::json!({"hits": []})),
        ("/kit/set_similarity", serde_json::json!({"hits": []})),
        ("/kit/search", serde_json::json!({"hits": []})),
    ] {
        Mock::given(method("POST"))
            .and(path(route))
            .and(header("authorization", authorization))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(1)
            .mount(server)
            .await;
    }
    Mock::given(method("GET"))
        .and(path("/kit/ai/metrics"))
        .and(header("authorization", authorization))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(server)
        .await;
}

fn retrieve_request() -> KitRetrieveRequest {
    KitRetrieveRequest {
        table: "docs".into(),
        retriever: serde_json::json!({
            "ann": {"column_id": 2, "query": [1.0, 0.0], "k": 1}
        }),
    }
}

fn rerank_request() -> KitAnnRerankRequest {
    KitAnnRerankRequest {
        table: "docs".into(),
        column_id: 2,
        query: vec![1.0, 0.0],
        candidate_k: 2,
        limit: 1,
        metric: KitVectorMetric::Cosine,
    }
}

fn set_request() -> KitSetSimilarityRequest {
    KitSetSimilarityRequest {
        table: "docs".into(),
        column_id: 3,
        members: vec![serde_json::json!("a")],
        candidate_k: 2,
        min_jaccard: 0.0,
        limit: 1,
    }
}

fn search_request() -> KitSearchRequest {
    KitSearchRequest {
        table: "docs".into(),
        must: vec![],
        retrievers: vec![serde_json::json!({
            "name": "ann",
            "weight": 1.0,
            "retriever": {"ann": {"column_id": 2, "query": [1.0, 0.0], "k": 1}}
        })],
        fusion: serde_json::json!({"rrf": {"k": 60.0}}),
        rerank: None,
        limit: 1,
        projection: None,
        deadline_ms: None,
        max_work: None,
        explain: false,
        cursor: None,
    }
}

fn call_all_blocking(client: &MongrelClient) {
    client.kit_retrieve(&retrieve_request()).unwrap();
    client.kit_ann_rerank(&rerank_request()).unwrap();
    client.kit_set_similarity(&set_request()).unwrap();
    client.kit_search(&search_request()).unwrap();
    client.kit_ai_metrics().unwrap();
}

#[tokio::test]
async fn blocking_builder_sends_bearer_auth_on_every_ai_route() {
    let server = MockServer::start().await;
    mount_ai_routes(&server, "Bearer secret-token").await;
    let uri = server.uri();
    tokio::task::spawn_blocking(move || {
        let client = MongrelClient::builder(uri)
            .bearer_token("secret-token")
            .build()
            .unwrap();
        call_all_blocking(&client);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn blocking_builder_sends_basic_auth_on_every_ai_route() {
    let server = MockServer::start().await;
    mount_ai_routes(&server, "Basic YWxpY2U6c2VjcmV0").await;
    let uri = server.uri();
    tokio::task::spawn_blocking(move || {
        let client = MongrelClient::builder(uri)
            .basic_auth("alice", "secret")
            .build()
            .unwrap();
        call_all_blocking(&client);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn async_builder_sends_auth_on_every_ai_route() {
    let server = MockServer::start().await;
    mount_ai_routes(&server, "Bearer async-token").await;
    let client = AsyncMongrelClient::builder(server.uri())
        .bearer_token("async-token")
        .build()
        .unwrap();
    client.kit_retrieve(&retrieve_request()).await.unwrap();
    client.kit_ann_rerank(&rerank_request()).await.unwrap();
    client.kit_set_similarity(&set_request()).await.unwrap();
    client.kit_search(&search_request()).await.unwrap();
    client.kit_ai_metrics().await.unwrap();
}

#[tokio::test]
async fn auth_failures_decode_to_typed_errors() {
    for (status, code, expected) in [
        (401, "AUTH_REQUIRED", KitErrorCode::AuthRequired),
        (403, "PERMISSION_DENIED", KitErrorCode::PermissionDenied),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/kit/txn"))
            .respond_with(
                ResponseTemplate::new(status).set_body_json(serde_json::json!({
                    "status": "aborted",
                    "error": {"code": code, "message": "denied"}
                })),
            )
            .expect(1)
            .mount(&server)
            .await;
        let uri = server.uri();
        let error = tokio::task::spawn_blocking(move || {
            MongrelClient::new(&uri)
                .unwrap()
                .kit_txn(&KitTxnRequest::new(Vec::new()))
                .unwrap_err()
        })
        .await
        .unwrap();
        assert!(matches!(
            error,
            ClientError::Kit {
                code,
                status: actual,
                ..
            } if code == expected && actual == status
        ));
    }
}

#[tokio::test]
async fn bare_middleware_auth_failures_never_become_unknown_outcomes() {
    for (status, expected) in [
        (401, KitErrorCode::AuthRequired),
        (403, KitErrorCode::PermissionDenied),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/kit/txn"))
            .respond_with(ResponseTemplate::new(status))
            .expect(2)
            .mount(&server)
            .await;
        let request = KitTxnRequest::new(Vec::new());
        let uri = server.uri();
        let blocking_uri = uri.clone();
        let blocking_request = request.clone();
        let blocking = tokio::task::spawn_blocking(move || {
            MongrelClient::new(&blocking_uri)
                .unwrap()
                .kit_txn(&blocking_request)
                .unwrap_err()
        })
        .await
        .unwrap();
        assert!(matches!(
            blocking,
            ClientError::Kit {
                code,
                committed: Some(false),
                status: actual,
                ..
            } if code == expected && actual == status
        ));

        let asynchronous = AsyncMongrelClient::new(&uri)
            .unwrap()
            .kit_txn(&request)
            .await
            .unwrap_err();
        assert!(matches!(
            asynchronous,
            ClientError::Kit {
                code,
                committed: Some(false),
                status: actual,
                ..
            } if code == expected && actual == status
        ));
    }

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/sql"))
        .respond_with(ResponseTemplate::new(401))
        .expect(2)
        .mount(&server)
        .await;
    let uri = server.uri();
    let blocking_uri = uri.clone();
    let blocking = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&blocking_uri)
            .unwrap()
            .sql("SELECT 1")
            .unwrap_err()
    })
    .await
    .unwrap();
    assert!(matches!(blocking, ClientError::Http { status: 401, .. }));
    let asynchronous = AsyncMongrelClient::new(&uri)
        .unwrap()
        .sql("SELECT 1")
        .await
        .unwrap_err();
    assert!(matches!(
        asynchronous,
        ClientError::Http { status: 401, .. }
    ));
}

#[tokio::test]
async fn durable_idempotency_failures_decode_to_typed_errors() {
    for (status, wire_code, expected) in [
        (
            409,
            "QUERY_OUTCOME_UNKNOWN",
            KitErrorCode::QueryOutcomeUnknown,
        ),
        (
            409,
            "IDEMPOTENCY_KEY_REUSE_MISMATCH",
            KitErrorCode::IdempotencyKeyReuseMismatch,
        ),
        (
            503,
            "IDEMPOTENCY_STORE_FULL",
            KitErrorCode::IdempotencyStoreFull,
        ),
        (
            503,
            "IDEMPOTENCY_STORE_UNAVAILABLE",
            KitErrorCode::IdempotencyStoreUnavailable,
        ),
        (
            400,
            "INVALID_IDEMPOTENCY_KEY",
            KitErrorCode::InvalidIdempotencyKey,
        ),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/kit/txn"))
            .respond_with(ResponseTemplate::new(status).set_body_json(
                if wire_code == "QUERY_OUTCOME_UNKNOWN" {
                    serde_json::json!({
                        "status": "outcome_unknown",
                        "committed": null,
                        "epoch": null,
                        "epoch_text": null,
                        "retryable": false,
                        "error": {"code": wire_code, "message": "idempotency failure"}
                    })
                } else {
                    serde_json::json!({
                        "status": "aborted",
                        "committed": false,
                        "retryable": matches!(
                            wire_code,
                            "IDEMPOTENCY_STORE_FULL" | "IDEMPOTENCY_STORE_UNAVAILABLE"
                        ),
                        "error": {"code": wire_code, "message": "idempotency failure"}
                    })
                },
            ))
            .expect(1)
            .mount(&server)
            .await;
        let uri = server.uri();
        let error = tokio::task::spawn_blocking(move || {
            MongrelClient::new(&uri)
                .unwrap()
                .kit_txn(&KitTxnRequest::new(Vec::new()))
                .unwrap_err()
        })
        .await
        .unwrap();

        match error {
            ClientError::Kit {
                code,
                status: actual,
                ..
            } => {
                assert_eq!(code, expected);
                assert_eq!(code.to_string(), wire_code);
                assert_eq!(actual, status);
            }
            other => panic!("expected typed Kit error, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn durable_kit_outcomes_preserve_safe_metadata() {
    const EXACT_EPOCH: u64 = 9_007_199_254_740_993;

    let committed_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/kit/txn"))
        .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
            "status": "committed",
            "committed": true,
            "epoch": EXACT_EPOCH,
            "epoch_text": EXACT_EPOCH.to_string(),
            "results": [],
            "retryable": false,
            "error": {
                "code": "COMMIT_OUTCOME",
                "message": "commit succeeded but publication failed"
            }
        })))
        .expect(1)
        .mount(&committed_server)
        .await;
    let committed_uri = committed_server.uri();
    let committed_error = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&committed_uri)
            .unwrap()
            .kit_txn(&KitTxnRequest::new(Vec::new()))
            .unwrap_err()
    })
    .await
    .unwrap();
    match &committed_error {
        ClientError::Kit {
            code,
            status,
            committed,
            epoch,
            epoch_text,
            retryable,
            ..
        } => {
            assert_eq!(*code, KitErrorCode::CommitOutcome);
            assert_eq!(code.to_string(), "COMMIT_OUTCOME");
            assert_eq!(*status, 409);
            assert_eq!(*committed, Some(true));
            assert_eq!(*epoch, Some(EXACT_EPOCH));
            assert_eq!(epoch_text.as_deref(), Some("9007199254740993"));
            assert_eq!(*retryable, Some(false));
        }
        other => panic!("expected committed Kit outcome, got {other:?}"),
    }
    assert!(!format!("{committed_error:?}").contains("committed-result-must-not-leak"));
    assert!(!committed_error
        .to_string()
        .contains("committed-result-must-not-leak"));

    let unknown_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/kit/search"))
        .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
            "status": "outcome_unknown",
            "committed": null,
            "epoch": 23,
            "epoch_text": "23",
            "retryable": false,
            "error": {
                "code": "QUERY_OUTCOME_UNKNOWN",
                "message": "commit publication outcome is unknown"
            }
        })))
        .expect(1)
        .mount(&unknown_server)
        .await;
    let unknown_error = AsyncMongrelClient::new(&unknown_server.uri())
        .unwrap()
        .kit_search(&search_request())
        .await
        .unwrap_err();
    match &unknown_error {
        ClientError::Kit {
            code,
            status,
            committed,
            epoch,
            retryable,
            ..
        } => {
            assert_eq!(*code, KitErrorCode::QueryOutcomeUnknown);
            assert_eq!(*status, 409);
            assert_eq!(*committed, None);
            assert_eq!(*epoch, Some(23));
            assert_eq!(*retryable, Some(false));
        }
        other => panic!("expected unknown Kit outcome, got {other:?}"),
    }
    assert!(!format!("{unknown_error:?}").contains("unknown-result-must-not-leak"));
    assert!(!unknown_error
        .to_string()
        .contains("unknown-result-must-not-leak"));
}

#[tokio::test]
async fn kit_epoch_text_must_be_canonical_and_match_numeric_epoch() {
    for epoch_text in ["9007199254740992", "09007199254740993"] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/kit/txn"))
            .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
                "status": "committed",
                "committed": true,
                "epoch": 9_007_199_254_740_993_u64,
                "epoch_text": epoch_text,
                "retryable": false,
                "error": {"code": "COMMIT_OUTCOME", "message": "committed"}
            })))
            .expect(1)
            .mount(&server)
            .await;
        let uri = server.uri();
        let error = tokio::task::spawn_blocking(move || {
            MongrelClient::new(&uri)
                .unwrap()
                .kit_txn(&KitTxnRequest::new(Vec::new()))
                .unwrap_err()
        })
        .await
        .unwrap();
        assert!(matches!(
            error,
            ClientError::Kit {
                code: KitErrorCode::QueryOutcomeUnknown,
                committed: None,
                ..
            }
        ));
    }
}

#[test]
fn invalid_credentials_never_appear_in_errors() {
    let secret = "very-secret\nvalue";
    let error = MongrelClient::builder("http://127.0.0.1")
        .bearer_token(secret)
        .build()
        .err()
        .expect("invalid header must fail");
    let debug = format!("{error:?}");
    let display = error.to_string();
    assert!(!debug.contains("very-secret"));
    assert!(!display.contains("very-secret"));
}

#[test]
fn constructors_reject_invalid_urls_without_panicking() {
    for url in [
        "not a URL",
        "ftp://127.0.0.1",
        "http://user:secret@127.0.0.1",
        "http://127.0.0.1?token=secret",
        "http://127.0.0.1#fragment",
    ] {
        assert!(matches!(
            MongrelClient::new(url),
            Err(ClientError::Transport(_))
        ));
        assert!(matches!(
            AsyncMongrelClient::new(url),
            Err(ClientError::Transport(_))
        ));
    }
}

#[tokio::test]
async fn blocking_auth_rebuild_preserves_request_timeout() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/health"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(200))
                .set_body_string("ok"),
        )
        .expect(2)
        .mount(&server)
        .await;
    let uri = server.uri();
    tokio::task::spawn_blocking(move || {
        for client in [
            MongrelClient::builder(&uri)
                .request_timeout(Duration::from_millis(20))
                .build()
                .unwrap()
                .try_with_bearer_token("token")
                .unwrap(),
            MongrelClient::builder(&uri)
                .request_timeout(Duration::from_millis(20))
                .build()
                .unwrap()
                .try_with_basic_auth("alice", "secret")
                .unwrap(),
        ] {
            assert!(matches!(client.health(), Err(ClientError::Transport(_))));
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn async_auth_rebuild_preserves_request_timeout() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/health"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(200))
                .set_body_string("ok"),
        )
        .expect(2)
        .mount(&server)
        .await;
    for client in [
        AsyncMongrelClient::builder(server.uri())
            .request_timeout(Duration::from_millis(20))
            .build()
            .unwrap()
            .try_with_bearer_token("token")
            .unwrap(),
        AsyncMongrelClient::builder(server.uri())
            .request_timeout(Duration::from_millis(20))
            .build()
            .unwrap()
            .try_with_basic_auth("alice", "secret")
            .unwrap(),
    ] {
        assert!(matches!(
            client.health().await,
            Err(ClientError::Transport(_))
        ));
    }
}
use std::time::Duration;
