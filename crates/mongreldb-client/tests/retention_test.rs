//! mongreldb-client history-retention HTTP contract tests.
//!
//! These tests verify the exact method, path, request body, and response shape
//! the client sends for the frozen `/history/retention` contract.

use mongreldb_client::{ClientError, MongrelClient};
use wiremock::matchers::{body_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn get_history_retention_sends_exact_request_and_parses_response() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/history/retention"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "history_retention_epochs": 7,
                "earliest_retained_epoch": 3,
            })),
        )
        .mount(&server)
        .await;

    let uri = server.uri();
    let epochs = tokio::task::spawn_blocking(move || {
        let client = MongrelClient::new(&uri);
        client.history_retention_epochs().unwrap()
    })
    .await
    .unwrap();
    assert_eq!(epochs, 7);

    let uri = server.uri();
    let earliest = tokio::task::spawn_blocking(move || {
        let client = MongrelClient::new(&uri);
        client.earliest_retained_epoch().unwrap()
    })
    .await
    .unwrap();
    assert_eq!(earliest, 3);
}

#[tokio::test]
async fn set_history_retention_sends_exact_request_and_parses_response() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/history/retention"))
        .and(header("content-type", "application/json"))
        .and(body_json(serde_json::json!({"history_retention_epochs": 42})))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "history_retention_epochs": 42,
                "earliest_retained_epoch": 1,
            })),
        )
        .mount(&server)
        .await;

    let uri = server.uri();
    let resp = tokio::task::spawn_blocking(move || {
        let client = MongrelClient::new(&uri);
        client.set_history_retention_epochs(42).unwrap()
    })
    .await
    .unwrap();
    assert_eq!(resp.history_retention_epochs, 42);
    assert_eq!(resp.earliest_retained_epoch, 1);
}

#[tokio::test]
async fn propagates_non_2xx_as_http_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/history/retention"))
        .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
        .mount(&server)
        .await;

    let uri = server.uri();
    let err = tokio::task::spawn_blocking(move || {
        let client = MongrelClient::new(&uri);
        client.history_retention_epochs().unwrap_err()
    })
    .await
    .unwrap();
    match err {
        ClientError::Http { status, .. } => assert_eq!(status, 503),
        other => panic!("expected HTTP error, got {other:?}"),
    }
}
