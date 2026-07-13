use mongreldb_client::{
    AsyncMongrelClient, ClientError, KitAnnRerankRequest, KitErrorCode, KitRetrieveRequest,
    KitSearchRequest, KitSetSimilarityRequest, KitVectorMetric, MongrelClient,
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
            .and(path("/kit/search"))
            .respond_with(ResponseTemplate::new(status).set_body_json(serde_json::json!({
                "status": "error",
                "error": {"code": code, "message": "denied"}
            })))
            .expect(1)
            .mount(&server)
            .await;
        let uri = server.uri();
        let error = tokio::task::spawn_blocking(move || {
            MongrelClient::new(&uri)
                .kit_search(&search_request())
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
