use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use mongreldb_client::{
    ClientError, MongrelClient, RemoteAuth, RemoteCancelOutcome, RemoteOptions,
    RemoteQueryErrorCode, RemoteQueryStatus,
};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const QUERY_ID: &str = "11112222333344445555666677778888";

#[test]
fn pre_cancelled_status_is_terminal() {
    let status: RemoteQueryStatus = serde_json::from_value(serde_json::json!({
        "query_id": QUERY_ID,
        "state": "pre_cancelled",
        "server_state": "pre_cancelled",
        "committed": false,
        "terminal_error": {
            "code": "QUERY_CANCELLED",
            "category": "cancelled"
        }
    }))
    .unwrap();
    assert!(status.is_terminal());

    let finished: RemoteQueryStatus = serde_json::from_value(serde_json::json!({
        "query_id": QUERY_ID,
        "status": "finished",
        "state": "finished",
        "server_state": "finished",
        "outcome": {"serialization": "unknown"}
    }))
    .unwrap();
    assert!(finished.is_terminal());
}

#[tokio::test]
async fn blocking_and_async_query_status_reject_wrong_query_id() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
        .expect(2)
        .mount(&server)
        .await;
    let mut status = completed_without_commit_status();
    status["query_id"] = serde_json::json!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    Mock::given(method("GET"))
        .and(path(format!("/queries/{QUERY_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(status))
        .expect(2)
        .mount(&server)
        .await;

    let uri = server.uri();
    let blocking_uri = uri.clone();
    let error = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&blocking_uri)
            .unwrap()
            .query_status(QUERY_ID.parse().unwrap())
            .unwrap_err()
    })
    .await
    .unwrap();
    assert!(matches!(error, ClientError::Decode(_)));

    let error = mongreldb_client::AsyncMongrelClient::new(&uri)
        .unwrap()
        .query_status(QUERY_ID.parse().unwrap())
        .await
        .unwrap_err();
    assert!(matches!(error, ClientError::Decode(_)));
}

#[tokio::test]
async fn query_status_failure_never_cancels_the_observed_query() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/queries/{QUERY_ID}")))
        .respond_with(ResponseTemplate::new(500).set_body_string("temporary failure"))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/queries/{QUERY_ID}/cancel")))
        .respond_with(ResponseTemplate::new(202))
        .expect(0)
        .mount(&server)
        .await;

    let uri = server.uri();
    let blocking_uri = uri.clone();
    let blocking = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&blocking_uri)
            .unwrap()
            .query_status(QUERY_ID.parse().unwrap())
            .unwrap_err()
    })
    .await
    .unwrap();
    assert!(matches!(blocking, ClientError::Decode(_)));

    let asynchronous = mongreldb_client::AsyncMongrelClient::new(&uri)
        .unwrap()
        .query_status(QUERY_ID.parse().unwrap())
        .await
        .unwrap_err();
    assert!(matches!(asynchronous, ClientError::Decode(_)));
}

#[tokio::test]
async fn blocking_and_async_query_status_reject_semantic_conflicts() {
    let mut unknown_status = completed_without_commit_status();
    unknown_status["status"] = serde_json::json!("bogus");
    let mut count_mismatch = completed_without_commit_status();
    count_mismatch["outcome"]["committed_statements"] = serde_json::json!(1);
    let mut index_out_of_order = completed_without_commit_status();
    index_out_of_order["statement_index"] = serde_json::json!(2);
    index_out_of_order["outcome"]["statement_index"] = serde_json::json!(2);
    let mut empty_terminal_error = completed_without_commit_status();
    empty_terminal_error["terminal_error"] = serde_json::json!({"code": "", "category": ""});
    let mut wrong_terminal_error = completed_without_commit_status();
    wrong_terminal_error["terminal_error"] =
        serde_json::json!({"code": "QUERY_CANCELLED", "category": "execution"});
    let mut wrong_retryable = completed_without_commit_status();
    wrong_retryable["retryable"] = serde_json::json!(true);
    let mut wrong_cancel_outcome = completed_without_commit_status();
    wrong_cancel_outcome["cancel_outcome"] = serde_json::json!("accepted");
    let mut wrong_cancellation_reason = completed_without_commit_status();
    wrong_cancellation_reason["cancellation_reason"] = serde_json::json!("client_request");
    let mut missing_terminal_state = completed_without_commit_status();
    missing_terminal_state["terminal_state"] = serde_json::Value::Null;
    let mut invalid_serialization = completed_without_commit_status();
    invalid_serialization["outcome"]["serialization"] = serde_json::json!("bogus");

    for status in [
        unknown_status,
        count_mismatch,
        index_out_of_order,
        empty_terminal_error,
        wrong_terminal_error,
        wrong_retryable,
        wrong_cancel_outcome,
        wrong_cancellation_reason,
        missing_terminal_state,
        invalid_serialization,
    ] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/capabilities"))
            .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/queries/{QUERY_ID}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(status))
            .expect(2)
            .mount(&server)
            .await;

        let uri = server.uri();
        let blocking_uri = uri.clone();
        let error = tokio::task::spawn_blocking(move || {
            MongrelClient::new(&blocking_uri)
                .unwrap()
                .query_status(QUERY_ID.parse().unwrap())
                .unwrap_err()
        })
        .await
        .unwrap();
        assert!(matches!(error, ClientError::Decode(_)));

        let error = mongreldb_client::AsyncMongrelClient::new(&uri)
            .unwrap()
            .query_status(QUERY_ID.parse().unwrap())
            .await
            .unwrap_err();
        assert!(matches!(error, ClientError::Decode(_)));
    }
}

#[tokio::test]
async fn malformed_recovery_status_never_claims_commit_or_replays() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/sql"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-mongreldb-query-id", QUERY_ID)
                .set_body_bytes(b"{"),
        )
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/queries/{QUERY_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(malformed_committed_status()))
        .expect(4..)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/queries/{QUERY_ID}/cancel")))
        .respond_with(ResponseTemplate::new(404))
        .expect(2)
        .mount(&server)
        .await;

    let uri = server.uri();
    let blocking_uri = uri.clone();
    let error = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&blocking_uri)
            .unwrap()
            .sql_write_idempotent_with_options(
                "INSERT INTO jobs VALUES (1)",
                "malformed-status-blocking",
                mongreldb_client::SqlClientOptions {
                    query_id: Some(QUERY_ID.parse().unwrap()),
                    timeout: None,
                },
            )
            .unwrap_err()
    })
    .await
    .unwrap();
    assert!(matches!(error, ClientError::QueryOutcomeUnknown { .. }));

    let error = mongreldb_client::AsyncMongrelClient::new(&uri)
        .unwrap()
        .sql_write_idempotent_with_options(
            "INSERT INTO jobs VALUES (1)",
            "malformed-status-async",
            mongreldb_client::SqlClientOptions {
                query_id: Some(QUERY_ID.parse().unwrap()),
                timeout: None,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(error, ClientError::QueryOutcomeUnknown { .. }));
}

#[test]
fn remote_auth_debug_never_exposes_secrets() {
    let bearer = format!("{:?}", RemoteAuth::Bearer("bearer-secret".into()));
    assert!(!bearer.contains("bearer-secret"));
    let basic = format!(
        "{:?}",
        RemoteAuth::Basic {
            username: "alice".into(),
            password: "password-secret".into(),
        }
    );
    assert!(basic.contains("alice"));
    assert!(!basic.contains("password-secret"));
}

fn capabilities() -> serde_json::Value {
    serde_json::json!({
        "sql_cancellation": {
            "version": 2,
            "client_query_ids": true,
            "cancel_endpoint": true,
            "query_status": true,
            "pre_registration_cancel": true,
            "stream_disconnect_cancels": true
        },
        "sql_idempotency": {
            "version": 1,
            "durable_pre_execution_intent": true,
            "replay_committed_receipt": true,
            "indeterminate_never_reexecutes": true
        }
    })
}

fn malformed_committed_status() -> serde_json::Value {
    serde_json::json!({
        "query_id": QUERY_ID,
        "status": "committed",
        "terminal_state": "committed",
        "state": "completed",
        "server_state": "completed",
        "operation": "INSERT",
        "committed": true,
        "committed_statements": 1,
        "last_commit_epoch": 7,
        "last_commit_epoch_text": "8",
        "first_commit_statement_index": 0,
        "last_commit_statement_index": 0,
        "completed_statements": 1,
        "statement_index": 0,
        "outcome": {
            "committed": true,
            "committed_statements": 1,
            "last_commit_epoch": 7,
            "last_commit_epoch_text": "8",
            "first_commit_statement_index": 0,
            "last_commit_statement_index": 0,
            "completed_statements": 1,
            "statement_index": 0,
            "serialization": "succeeded"
        },
        "trace": {}
    })
}

#[test]
fn builders_reject_credentialed_or_non_http_urls_without_echoing_secrets() {
    for url in [
        "ftp://127.0.0.1:8453",
        "http://alice:secret@127.0.0.1:8453",
        "http://127.0.0.1:8453?token=secret",
        "http://127.0.0.1:8453/#secret",
    ] {
        let Err(error) = MongrelClient::builder(url).build() else {
            panic!("invalid URL was accepted");
        };
        let error = error.to_string();
        assert!(!error.contains("alice"));
        assert!(!error.contains("secret"));
        let Err(error) = mongreldb_client::AsyncMongrelClient::builder(url).build() else {
            panic!("invalid URL was accepted");
        };
        let error = error.to_string();
        assert!(!error.contains("alice"));
        assert!(!error.contains("secret"));
    }
}

#[tokio::test]
async fn shared_auth_and_v2_pre_cancel_are_structured() {
    let server = MockServer::start().await;
    for route in [
        "/capabilities",
        "/queries/11112222333344445555666677778888/cancel",
    ] {
        let body = if route == "/capabilities" {
            capabilities()
        } else {
            serde_json::json!({
                "query_id": QUERY_ID,
                "state": "pre_cancelled",
                "cancel_outcome": "pre_cancelled",
                "committed": false
            })
        };
        Mock::given(method(if route == "/capabilities" {
            "GET"
        } else {
            "POST"
        }))
        .and(path(route))
        .and(header("authorization", "Bearer secret"))
        .respond_with(ResponseTemplate::new(202).set_body_json(body))
        .expect(1)
        .mount(&server)
        .await;
    }
    let uri = server.uri();
    let outcome = tokio::task::spawn_blocking(move || {
        MongrelClient::with_options(
            uri,
            RemoteOptions {
                auth: Some(RemoteAuth::Bearer("secret".into())),
                transport_timeout: Some(Duration::from_secs(2)),
            },
        )
        .unwrap()
        .cancel_sql(QUERY_ID.parse().unwrap())
        .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(outcome, RemoteCancelOutcome::PreCancelled);
}

#[tokio::test]
async fn cancel_decodes_too_late_and_not_found() {
    for (status, body, expected) in [
        (
            409,
            serde_json::json!({
                "query_id": QUERY_ID,
                "state": "commit_critical",
                "cancel_outcome": "too_late",
                "committed": true
            }),
            RemoteCancelOutcome::TooLate,
        ),
        (
            404,
            serde_json::json!({
                "query_id": QUERY_ID,
                "state": "not_found",
                "cancel_outcome": "not_found",
                "committed": false
            }),
            RemoteCancelOutcome::NotFound,
        ),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/capabilities"))
            .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(format!("/queries/{QUERY_ID}/cancel")))
            .respond_with(ResponseTemplate::new(status).set_body_json(body))
            .expect(1)
            .mount(&server)
            .await;
        let uri = server.uri();
        let outcome = tokio::task::spawn_blocking(move || {
            MongrelClient::new(&uri)
                .unwrap()
                .cancel_sql(QUERY_ID.parse().unwrap())
                .unwrap()
        })
        .await
        .unwrap();
        assert_eq!(outcome, expected);
    }
}

#[tokio::test]
async fn cancellation_rejects_wrong_id_conflicting_fields_and_status() {
    for (status, body) in [
        (
            200,
            serde_json::json!({
                "query_id": QUERY_ID,
                "state": "commit_critical",
                "cancel_outcome": "too_late"
            }),
        ),
        (
            409,
            serde_json::json!({
                "query_id": QUERY_ID,
                "state": "cancellation_requested",
                "cancel_outcome": "accepted"
            }),
        ),
        (
            202,
            serde_json::json!({
                "query_id": QUERY_ID,
                "state": "cancellation_requested",
                "cancel_outcome": "too_late"
            }),
        ),
        (
            202,
            serde_json::json!({
                "query_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "state": "cancellation_requested",
                "cancel_outcome": "accepted"
            }),
        ),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/capabilities"))
            .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(format!("/queries/{QUERY_ID}/cancel")))
            .respond_with(ResponseTemplate::new(status).set_body_json(body))
            .expect(2)
            .mount(&server)
            .await;
        let blocking_uri = server.uri();
        let error = tokio::task::spawn_blocking(move || {
            MongrelClient::new(&blocking_uri)
                .unwrap()
                .cancel_sql(QUERY_ID.parse().unwrap())
                .unwrap_err()
        })
        .await
        .unwrap();
        assert!(matches!(error, ClientError::Decode(_)));
        let error = mongreldb_client::AsyncMongrelClient::new(&server.uri())
            .unwrap()
            .cancel_sql(QUERY_ID.parse().unwrap())
            .await
            .unwrap_err();
        assert!(matches!(error, ClientError::Decode(_)));
    }
}

#[tokio::test]
async fn query_status_not_found_is_typed() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/queries/{QUERY_ID}")))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "query_id": QUERY_ID,
            "status": "unknown",
            "terminal_state": null,
            "committed": null,
            "committed_statements": null,
            "last_commit_epoch": null,
            "last_commit_epoch_text": null,
            "first_commit_statement_index": null,
            "last_commit_statement_index": null,
            "completed_statements": null,
            "statement_index": null,
            "cancel_outcome": "not_found",
            "cancellation_reason": null,
            "retryable": false,
            "server_state": "not_found",
            "outcome": {
                "committed": null,
                "committed_statements": null,
                "last_commit_epoch": null,
                "last_commit_epoch_text": null,
                "first_commit_statement_index": null,
                "last_commit_statement_index": null,
                "completed_statements": null,
                "statement_index": null,
                "serialization": "unknown"
            },
            "error": {
                "code": "QUERY_NOT_FOUND",
                "message": "query not found",
                "query_id": QUERY_ID,
                "committed": null,
                "retryable": false
            }
        })))
        .expect(1)
        .mount(&server)
        .await;
    let uri = server.uri();
    let error = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&uri)
            .unwrap()
            .query_status(QUERY_ID.parse().unwrap())
            .unwrap_err()
    })
    .await
    .unwrap();
    assert!(matches!(
        error,
        ClientError::Query {
            code: RemoteQueryErrorCode::QueryNotFound,
            response,
            ..
        } if response.cancel_outcome == Some(RemoteCancelOutcome::NotFound)
            && response.status == "unknown"
            && response.committed.is_none()
            && response.error.committed.is_none()
    ));
}

#[tokio::test]
async fn controlled_sql_rejects_capability_v1() {
    let server = MockServer::start().await;
    let mut legacy = capabilities();
    legacy["sql_cancellation"]["version"] = serde_json::json!(1);
    legacy["sql_cancellation"]["pre_registration_cancel"] = serde_json::json!(false);
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(legacy))
        .expect(1)
        .mount(&server)
        .await;
    let uri = server.uri();
    let error = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&uri).unwrap().sql_with_options(
            "SELECT 1",
            mongreldb_client::SqlClientOptions {
                query_id: Some(QUERY_ID.parse().unwrap()),
                timeout: None,
            },
        )
    })
    .await
    .unwrap()
    .unwrap_err();
    assert!(matches!(
        error,
        ClientError::Query {
            code: RemoteQueryErrorCode::CapabilityUnsupported,
            ..
        }
    ));
}

#[tokio::test]
async fn cancel_and_status_decode_every_terminal_field() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/queries/{QUERY_ID}/cancel")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "query_id": QUERY_ID,
            "state": "finished",
            "cancel_outcome": "already_finished"
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/queries/{QUERY_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "query_id": QUERY_ID,
            "status": "cancelled_after_commit",
            "terminal_state": "cancelled_after_commit",
            "state": "cancelled",
            "server_state": "cancelled",
            "operation": "INSERT",
            "committed": true,
            "committed_statements": 1,
            "last_commit_epoch": 42,
            "last_commit_epoch_text": "42",
            "first_commit_statement_index": 0,
            "last_commit_statement_index": 0,
            "completed_statements": 1,
            "statement_index": 1,
            "cancel_outcome": "already_finished",
            "cancellation_reason": "client_request",
            "retryable": false,
            "outcome": {
                "committed": true,
                "committed_statements": 1,
                "last_commit_epoch": 42,
                "last_commit_epoch_text": "42",
                "first_commit_statement_index": 0,
                "last_commit_statement_index": 0,
                "completed_statements": 1,
                "statement_index": 1,
                "serialization": "failed"
            },
            "terminal_error": {
                "code": "QUERY_CANCELLED_AFTER_COMMIT",
                "category": "cancellation"
            },
            "trace": {}
        })))
        .expect(1)
        .mount(&server)
        .await;
    let uri = server.uri();
    tokio::task::spawn_blocking(move || {
        let client = MongrelClient::new(&uri).unwrap();
        let query_id = QUERY_ID.parse().unwrap();
        assert_eq!(
            client.cancel_sql(query_id).unwrap(),
            RemoteCancelOutcome::AlreadyFinished
        );
        let status = client.query_status(query_id).unwrap();
        assert_eq!(status.committed, Some(true));
        assert_eq!(
            status.terminal_state.as_deref(),
            Some("cancelled_after_commit")
        );
        assert_eq!(status.server_state_or_state(), "cancelled");
        assert_eq!(status.last_commit_epoch, Some(42));
        assert_eq!(status.first_commit_statement_index, Some(0));
        assert_eq!(status.last_commit_statement_index, Some(0));
        assert_eq!(status.outcome.first_commit_statement_index, Some(0));
        assert_eq!(
            status.terminal_error.unwrap().code,
            RemoteQueryErrorCode::QueryCancelledAfterCommit
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn arrow_decode_failure_recovers_committed_status() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/sql"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-mongreldb-query-id", QUERY_ID)
                .set_body_bytes(b"not-arrow-ipc"),
        )
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/queries/{QUERY_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "query_id": QUERY_ID,
            "status": "committed",
            "terminal_state": "committed",
            "state": "completed",
            "server_state": "completed",
            "operation": "INSERT",
            "committed": true,
            "committed_statements": 1,
            "last_commit_epoch": 81,
            "last_commit_epoch_text": "81",
            "first_commit_statement_index": 0,
            "last_commit_statement_index": 0,
            "completed_statements": 1,
            "statement_index": 0,
            "cancel_outcome": "already_finished",
            "cancellation_reason": "none",
            "retryable": false,
            "outcome": {
                "committed": true,
                "committed_statements": 1,
                "last_commit_epoch": 81,
                "last_commit_epoch_text": "81",
                "first_commit_statement_index": 0,
                "last_commit_statement_index": 0,
                "completed_statements": 1,
                "statement_index": 0,
                "serialization": "succeeded"
            },
            "trace": {}
        })))
        .expect(2)
        .mount(&server)
        .await;
    let uri = server.uri();
    let blocking_uri = uri.clone();
    let error = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&blocking_uri).unwrap().sql_with_options(
            "INSERT INTO jobs VALUES (1); SELECT * FROM jobs",
            mongreldb_client::SqlClientOptions {
                query_id: Some(QUERY_ID.parse().unwrap()),
                timeout: None,
            },
        )
    })
    .await
    .unwrap()
    .unwrap_err();
    assert!(matches!(
        error,
        ClientError::Query {
            code: RemoteQueryErrorCode::CommitOutcome,
            response,
            ..
        } if response.committed == Some(true)
            && response.last_commit_epoch == Some(81)
            && response.last_commit_epoch_text.as_deref() == Some("81")
            && response.first_commit_statement_index == Some(0)
            && response.last_commit_statement_index == Some(0)
    ));

    let error = mongreldb_client::AsyncMongrelClient::new(&uri)
        .unwrap()
        .sql_with_options(
            "INSERT INTO jobs VALUES (1); SELECT * FROM jobs",
            mongreldb_client::SqlClientOptions {
                query_id: Some(QUERY_ID.parse().unwrap()),
                timeout: None,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ClientError::Query {
            code: RemoteQueryErrorCode::CommitOutcome,
            response,
            ..
        } if response.committed == Some(true)
            && response.last_commit_epoch == Some(81)
            && response.last_commit_epoch_text.as_deref() == Some("81")
            && response.first_commit_statement_index == Some(0)
            && response.last_commit_statement_index == Some(0)
    ));
}

fn committed_while_serializing_status() -> serde_json::Value {
    serde_json::json!({
        "query_id": QUERY_ID,
        "status": "committed",
        "terminal_state": null,
        "state": "serializing",
        "server_state": "serializing",
        "operation": "INSERT",
        "committed": true,
        "committed_statements": 1,
        "last_commit_epoch": 91,
        "last_commit_epoch_text": "91",
        "first_commit_statement_index": 0,
        "last_commit_statement_index": 0,
        "completed_statements": 1,
        "statement_index": 0,
        "cancel_outcome": null,
        "cancellation_reason": "none",
        "retryable": false,
        "outcome": {
            "committed": true,
            "committed_statements": 1,
            "last_commit_epoch": 91,
            "last_commit_epoch_text": "91",
            "first_commit_statement_index": 0,
            "last_commit_statement_index": 0,
            "completed_statements": 1,
            "statement_index": 0,
            "serialization": "in_progress"
        },
        "terminal_error": null,
        "trace": {}
    })
}

#[tokio::test]
async fn recovery_never_cancels_after_a_durable_commit() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/sql"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-mongreldb-query-id", QUERY_ID)
                .set_body_bytes(b"not-arrow-ipc"),
        )
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/queries/{QUERY_ID}")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(committed_while_serializing_status()),
        )
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/queries/{QUERY_ID}/cancel")))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let options = mongreldb_client::SqlClientOptions {
        query_id: Some(QUERY_ID.parse().unwrap()),
        timeout: None,
    };
    let uri = server.uri();
    let blocking_uri = uri.clone();
    let blocking_options = options.clone();
    let blocking = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&blocking_uri)
            .unwrap()
            .sql_with_options("INSERT INTO jobs VALUES (1)", blocking_options)
            .unwrap_err()
    })
    .await
    .unwrap();
    assert!(matches!(
        blocking,
        ClientError::Query {
            code: RemoteQueryErrorCode::CommitOutcome,
            response,
            ..
        } if response.committed == Some(true) && response.last_commit_epoch == Some(91)
    ));

    let asynchronous = mongreldb_client::AsyncMongrelClient::new(&uri)
        .unwrap()
        .sql_with_options("INSERT INTO jobs VALUES (1)", options)
        .await
        .unwrap_err();
    assert!(matches!(
        asynchronous,
        ClientError::Query {
            code: RemoteQueryErrorCode::CommitOutcome,
            response,
            ..
        } if response.committed == Some(true) && response.last_commit_epoch == Some(91)
    ));
}

#[tokio::test]
async fn empty_arrow_success_requires_exact_query_id_header() {
    for response_query_id in [None, Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/capabilities"))
            .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
            .expect(2)
            .mount(&server)
            .await;
        let mut response = ResponseTemplate::new(200).set_body_bytes(Vec::<u8>::new());
        if let Some(response_query_id) = response_query_id {
            response = response.insert_header("x-mongreldb-query-id", response_query_id);
        }
        Mock::given(method("POST"))
            .and(path("/sql"))
            .respond_with(response)
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/queries/{QUERY_ID}")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(completed_without_commit_status()),
            )
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(format!("/queries/{QUERY_ID}/cancel")))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let options = mongreldb_client::SqlClientOptions {
            query_id: Some(QUERY_ID.parse().unwrap()),
            timeout: None,
        };
        let uri = server.uri();
        let blocking_uri = uri.clone();
        let blocking_options = options.clone();
        let blocking = tokio::task::spawn_blocking(move || {
            MongrelClient::new(&blocking_uri)
                .unwrap()
                .sql_with_options("INSERT INTO jobs VALUES (1)", blocking_options)
                .unwrap_err()
        })
        .await
        .unwrap();
        assert!(matches!(
            blocking,
            ClientError::Query {
                code: RemoteQueryErrorCode::SerializationFailed,
                response,
                ..
            } if response.committed == Some(false)
        ));

        let asynchronous = mongreldb_client::AsyncMongrelClient::new(&uri)
            .unwrap()
            .sql_with_options("INSERT INTO jobs VALUES (1)", options)
            .await
            .unwrap_err();
        assert!(matches!(
            asynchronous,
            ClientError::Query {
                code: RemoteQueryErrorCode::SerializationFailed,
                response,
                ..
            } if response.committed == Some(false)
        ));
    }
}

#[tokio::test]
async fn exact_header_accepts_empty_arrow_success() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/sql"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-mongreldb-query-id", QUERY_ID)
                .set_body_bytes(Vec::<u8>::new()),
        )
        .expect(2)
        .mount(&server)
        .await;
    let options = mongreldb_client::SqlClientOptions {
        query_id: Some(QUERY_ID.parse().unwrap()),
        timeout: None,
    };
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
        .expect(2)
        .mount(&server)
        .await;

    let uri = server.uri();
    let blocking_uri = uri.clone();
    let blocking_options = options.clone();
    let blocking = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&blocking_uri)
            .unwrap()
            .sql_with_options("INSERT INTO jobs VALUES (1)", blocking_options)
            .unwrap()
    })
    .await
    .unwrap();
    assert!(blocking.is_empty());
    let asynchronous = mongreldb_client::AsyncMongrelClient::new(&uri)
        .unwrap()
        .sql_with_options("INSERT INTO jobs VALUES (1)", options)
        .await
        .unwrap();
    assert!(asynchronous.is_empty());
}

#[tokio::test]
async fn sql_errors_keep_stable_code_and_durable_outcome() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/sql"))
        .respond_with(
            ResponseTemplate::new(409)
                .insert_header("x-mongreldb-query-id", QUERY_ID)
                .set_body_json(serde_json::json!({
                    "query_id": QUERY_ID,
                    "status": "deadline_after_commit",
                    "terminal_state": "deadline_after_commit",
                    "committed": true,
                    "committed_statements": 1,
                    "last_commit_epoch": 9,
                    "last_commit_epoch_text": "9",
                    "first_commit_statement_index": 0,
                    "last_commit_statement_index": 0,
                    "completed_statements": 1,
                    "statement_index": 1,
                    "cancel_outcome": "accepted",
                    "cancellation_reason": "deadline",
                    "retryable": false,
                    "server_state": "cancelled",
                    "outcome": {
                        "committed": true,
                        "committed_statements": 1,
                        "last_commit_epoch": 9,
                        "last_commit_epoch_text": "9",
                        "first_commit_statement_index": 0,
                        "last_commit_statement_index": 0,
                        "completed_statements": 1,
                        "statement_index": 1,
                        "serialization": "failed"
                    },
                    "error": {
                        "code": "DEADLINE_AFTER_COMMIT",
                        "message": "deadline after commit",
                        "query_id": QUERY_ID,
                        "committed": true,
                        "retryable": false
                    }
                })),
        )
        .expect(1)
        .mount(&server)
        .await;
    let uri = server.uri();
    let error = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&uri).unwrap().sql_with_options(
            "SELECT 1",
            mongreldb_client::SqlClientOptions {
                query_id: Some(QUERY_ID.parse().unwrap()),
                timeout: None,
            },
        )
    })
    .await
    .unwrap()
    .unwrap_err();
    assert!(matches!(
        error,
        ClientError::Query {
            code: RemoteQueryErrorCode::DeadlineAfterCommit,
            response,
            ..
        } if response.committed == Some(true) && response.last_commit_epoch == Some(9)
    ));
}

fn valid_after_commit_error() -> serde_json::Value {
    serde_json::json!({
        "query_id": QUERY_ID,
        "status": "committed_with_error",
        "committed": true,
        "committed_statements": 1,
        "last_commit_epoch": 9,
        "last_commit_epoch_text": "9",
        "first_commit_statement_index": 0,
        "last_commit_statement_index": 0,
        "completed_statements": 1,
        "statement_index": 0,
        "retryable": false,
        "server_state": "failed",
        "outcome": {
            "committed": true,
            "committed_statements": 1,
            "last_commit_epoch": 9,
            "last_commit_epoch_text": "9",
            "first_commit_statement_index": 0,
            "last_commit_statement_index": 0,
            "completed_statements": 1,
            "statement_index": 0,
            "serialization": "failed"
        },
        "error": {
            "code": "SERIALIZATION_FAILED_AFTER_COMMIT",
            "message": "serialization failed after commit",
            "query_id": QUERY_ID,
            "committed": true,
            "retryable": false
        }
    })
}

#[tokio::test]
async fn malformed_sql_error_envelopes_fail_closed_in_blocking_and_async_clients() {
    let mut wrong_id = valid_after_commit_error();
    wrong_id["error"]["query_id"] = serde_json::json!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let mut contradictory = valid_after_commit_error();
    contradictory["outcome"]["committed"] = serde_json::json!(false);
    let mut noncanonical = valid_after_commit_error();
    noncanonical["last_commit_epoch_text"] = serde_json::json!("09");
    let mut numeric_mismatch = valid_after_commit_error();
    numeric_mismatch["last_commit_epoch"] = serde_json::json!(10);
    let mut wrong_code = valid_after_commit_error();
    wrong_code["status"] = serde_json::json!("deadline_after_commit");

    for (body, preserves_commit) in [
        (wrong_id, false),
        (contradictory, false),
        (noncanonical, false),
        (numeric_mismatch, false),
        (wrong_code, true),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/capabilities"))
            .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/sql"))
            .respond_with(
                ResponseTemplate::new(409)
                    .insert_header("x-mongreldb-query-id", QUERY_ID)
                    .set_body_json(body),
            )
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/queries/{QUERY_ID}")))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(format!("/queries/{QUERY_ID}/cancel")))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let blocking_uri = server.uri();
        let error = tokio::task::spawn_blocking(move || {
            MongrelClient::new(&blocking_uri)
                .unwrap()
                .sql_with_options(
                    "SELECT 1",
                    mongreldb_client::SqlClientOptions {
                        query_id: Some(QUERY_ID.parse().unwrap()),
                        timeout: None,
                    },
                )
                .unwrap_err()
        })
        .await
        .unwrap();
        assert!(if preserves_commit {
            matches!(
                error,
                ClientError::Query {
                    code: RemoteQueryErrorCode::CommitOutcome,
                    ..
                }
            )
        } else {
            matches!(error, ClientError::QueryOutcomeUnknown { .. })
        });

        let error = mongreldb_client::AsyncMongrelClient::new(&server.uri())
            .unwrap()
            .sql_with_options(
                "SELECT 1",
                mongreldb_client::SqlClientOptions {
                    query_id: Some(QUERY_ID.parse().unwrap()),
                    timeout: None,
                },
            )
            .await
            .unwrap_err();
        assert!(if preserves_commit {
            matches!(
                error,
                ClientError::Query {
                    code: RemoteQueryErrorCode::CommitOutcome,
                    ..
                }
            )
        } else {
            matches!(error, ClientError::QueryOutcomeUnknown { .. })
        });
    }
}

#[tokio::test]
async fn malformed_error_body_commit_proof_survives_a_missing_header() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
        .expect(2)
        .mount(&server)
        .await;
    let mut response = valid_after_commit_error();
    response["unknown_non_durable_field"] = serde_json::json!(true);
    Mock::given(method("POST"))
        .and(path("/sql"))
        .respond_with(ResponseTemplate::new(409).set_body_json(response))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/queries/{QUERY_ID}")))
        .respond_with(ResponseTemplate::new(404))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/queries/{QUERY_ID}/cancel")))
        .respond_with(ResponseTemplate::new(404))
        .expect(0)
        .mount(&server)
        .await;

    let options = mongreldb_client::SqlClientOptions {
        query_id: Some(QUERY_ID.parse().unwrap()),
        timeout: None,
    };
    let uri = server.uri();
    let blocking_uri = uri.clone();
    let blocking_options = options.clone();
    let blocking = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&blocking_uri)
            .unwrap()
            .sql_with_options("INSERT INTO jobs VALUES (1)", blocking_options)
            .unwrap_err()
    })
    .await
    .unwrap();
    assert!(matches!(
        blocking,
        ClientError::Query {
            code: RemoteQueryErrorCode::CommitOutcome,
            response,
            ..
        } if response.committed == Some(true) && response.last_commit_epoch == Some(9)
    ));

    let asynchronous = mongreldb_client::AsyncMongrelClient::new(&uri)
        .unwrap()
        .sql_with_options("INSERT INTO jobs VALUES (1)", options)
        .await
        .unwrap_err();
    assert!(matches!(
        asynchronous,
        ClientError::Query {
            code: RemoteQueryErrorCode::CommitOutcome,
            response,
            ..
        } if response.committed == Some(true) && response.last_commit_epoch == Some(9)
    ));
}

#[tokio::test]
async fn query_registry_full_without_query_id_fails_closed() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/sql"))
        .respond_with(ResponseTemplate::new(503).set_body_json(serde_json::json!({
            "status": "failed_before_commit",
            "committed": false,
            "retryable": true,
            "error": {
                "code": "QUERY_REGISTRY_FULL",
                "message": "query registry is full",
                "committed": false,
                "retryable": true
            }
        })))
        .expect(1)
        .mount(&server)
        .await;
    let uri = server.uri();
    let error =
        tokio::task::spawn_blocking(move || MongrelClient::new(&uri).unwrap().sql("SELECT 1"))
            .await
            .unwrap()
            .unwrap_err();
    assert!(matches!(error, ClientError::QueryOutcomeUnknown { .. }));
}

fn read_request(stream: &mut TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut bytes = Vec::new();
    let mut buffer = [0; 4096];
    loop {
        let read = stream.read(&mut buffer).unwrap();
        bytes.extend_from_slice(&buffer[..read]);
        if read == 0 || bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

fn spawn_truncated_sql_error_server() -> (std::net::SocketAddr, std::thread::JoinHandle<()>) {
    spawn_truncated_sql_error_server_with_status(serde_json::json!({
        "query_id": QUERY_ID,
        "status": "committed_with_error",
        "terminal_state": "committed_with_error",
        "state": "failed",
        "server_state": "failed",
        "operation": "INSERT",
        "committed": true,
        "committed_statements": 1,
        "last_commit_epoch": 91,
        "last_commit_epoch_text": "91",
        "first_commit_statement_index": 0,
        "last_commit_statement_index": 0,
        "completed_statements": 1,
        "statement_index": 0,
        "cancel_outcome": "already_finished",
        "cancellation_reason": "none",
        "retryable": false,
        "outcome": {
            "committed": true,
            "committed_statements": 1,
            "last_commit_epoch": 91,
            "last_commit_epoch_text": "91",
            "first_commit_statement_index": 0,
            "last_commit_statement_index": 0,
            "completed_statements": 1,
            "statement_index": 0,
            "serialization": "failed"
        },
        "terminal_error": {
            "code": "SERIALIZATION_FAILED_AFTER_COMMIT",
            "category": "serialization"
        },
        "trace": {}
    }))
}

fn spawn_truncated_sql_error_server_with_status(
    status_body: serde_json::Value,
) -> (std::net::SocketAddr, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut capabilities_request, _) = listener.accept().unwrap();
        assert!(read_request(&mut capabilities_request).starts_with("GET /capabilities "));
        let body = capabilities().to_string();
        write!(
            capabilities_request,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();

        let (mut sql, _) = listener.accept().unwrap();
        assert!(read_request(&mut sql).starts_with("POST /sql "));
        write!(
            sql,
            "HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\nContent-Length: 4096\r\nConnection: close\r\n\r\n{{\"query_id\":\"{QUERY_ID}\""
        )
        .unwrap();
        drop(sql);

        let (mut status, _) = listener.accept().unwrap();
        assert!(read_request(&mut status).starts_with(&format!("GET /queries/{QUERY_ID} ")));
        let body = status_body.to_string();
        write!(
            status,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
    });
    (address, server)
}

fn completed_without_commit_status() -> serde_json::Value {
    serde_json::json!({
        "query_id": QUERY_ID,
        "status": "completed",
        "terminal_state": "completed",
        "state": "completed",
        "server_state": "completed",
        "operation": "SELECT",
        "committed": false,
        "committed_statements": 0,
        "completed_statements": 1,
        "statement_index": 0,
        "cancel_outcome": "already_finished",
        "cancellation_reason": "none",
        "retryable": false,
        "outcome": {
            "committed": false,
            "committed_statements": 0,
            "completed_statements": 1,
            "statement_index": 0,
            "serialization": "succeeded"
        },
        "trace": {}
    })
}

#[test]
fn truncated_non_success_sql_body_recovers_durable_outcome() {
    let (address, server) = spawn_truncated_sql_error_server();
    let error = MongrelClient::new(&format!("http://{address}"))
        .unwrap()
        .sql_with_options(
            "INSERT INTO jobs VALUES (1)",
            mongreldb_client::SqlClientOptions {
                query_id: Some(QUERY_ID.parse().unwrap()),
                timeout: None,
            },
        )
        .unwrap_err();
    assert!(matches!(
        error,
        ClientError::Query {
            code: RemoteQueryErrorCode::SerializationFailedAfterCommit,
            response,
            ..
        } if response.committed == Some(true) && response.last_commit_epoch == Some(91)
    ));
    server.join().unwrap();
}

#[tokio::test]
async fn async_truncated_non_success_sql_body_recovers_durable_outcome() {
    let (address, server) = spawn_truncated_sql_error_server();
    let error = mongreldb_client::AsyncMongrelClient::new(&format!("http://{address}"))
        .unwrap()
        .sql_with_options(
            "INSERT INTO jobs VALUES (1)",
            mongreldb_client::SqlClientOptions {
                query_id: Some(QUERY_ID.parse().unwrap()),
                timeout: None,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ClientError::Query {
            code: RemoteQueryErrorCode::SerializationFailedAfterCommit,
            response,
            ..
        } if response.committed == Some(true) && response.last_commit_epoch == Some(91)
    ));
    server.join().unwrap();
}

#[test]
fn malformed_response_with_known_no_commit_is_not_outcome_unknown() {
    let (address, server) =
        spawn_truncated_sql_error_server_with_status(completed_without_commit_status());
    let error = MongrelClient::new(&format!("http://{address}"))
        .unwrap()
        .sql_with_options(
            "SELECT 1",
            mongreldb_client::SqlClientOptions {
                query_id: Some(QUERY_ID.parse().unwrap()),
                timeout: None,
            },
        )
        .unwrap_err();
    assert!(matches!(
        error,
        ClientError::Query {
            code: RemoteQueryErrorCode::SerializationFailed,
            response,
            ..
        } if response.committed == Some(false)
    ));
    server.join().unwrap();
}

#[tokio::test]
async fn async_malformed_response_with_known_no_commit_is_not_outcome_unknown() {
    let (address, server) =
        spawn_truncated_sql_error_server_with_status(completed_without_commit_status());
    let error = mongreldb_client::AsyncMongrelClient::new(&format!("http://{address}"))
        .unwrap()
        .sql_with_options(
            "SELECT 1",
            mongreldb_client::SqlClientOptions {
                query_id: Some(QUERY_ID.parse().unwrap()),
                timeout: None,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ClientError::Query {
            code: RemoteQueryErrorCode::SerializationFailed,
            response,
            ..
        } if response.committed == Some(false)
    ));
    server.join().unwrap();
}

#[test]
fn transport_loss_recovers_committed_status() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut capabilities_request, _) = listener.accept().unwrap();
        assert!(read_request(&mut capabilities_request).starts_with("GET /capabilities "));
        let body = capabilities().to_string();
        write!(
            capabilities_request,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        let (mut sql, _) = listener.accept().unwrap();
        assert!(read_request(&mut sql).starts_with("POST /sql "));
        drop(sql);
        let (mut status, _) = listener.accept().unwrap();
        assert!(read_request(&mut status).starts_with(&format!("GET /queries/{QUERY_ID} ")));
        let body = serde_json::json!({
            "query_id": QUERY_ID,
            "status": "executing",
            "state": "executing",
            "operation": "INSERT",
            "committed": false,
            "outcome": {},
            "trace": {}
        })
        .to_string();
        write!(
            status,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        let (mut cancel, _) = listener.accept().unwrap();
        assert!(read_request(&mut cancel).starts_with(&format!("POST /queries/{QUERY_ID}/cancel ")));
        let body = serde_json::json!({
            "query_id": QUERY_ID,
            "state": "cancellation_requested",
            "cancel_outcome": "accepted",
            "committed": false
        })
        .to_string();
        write!(
            cancel,
            "HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        let (mut status, _) = listener.accept().unwrap();
        assert!(read_request(&mut status).starts_with(&format!("GET /queries/{QUERY_ID} ")));
        let body = serde_json::json!({
            "query_id": QUERY_ID,
            "status": "committed",
            "terminal_state": "committed",
            "state": "completed",
            "server_state": "completed",
            "operation": "INSERT",
            "committed": true,
            "committed_statements": 1,
            "last_commit_epoch": 77,
            "last_commit_epoch_text": "77",
            "first_commit_statement_index": 0,
            "last_commit_statement_index": 0,
            "completed_statements": 1,
            "statement_index": 0,
            "cancel_outcome": "already_finished",
            "cancellation_reason": "none",
            "retryable": false,
            "outcome": {
                "committed": true,
                "committed_statements": 1,
                "last_commit_epoch": 77,
                "last_commit_epoch_text": "77",
                "first_commit_statement_index": 0,
                "last_commit_statement_index": 0,
                "completed_statements": 1,
                "statement_index": 0,
                "serialization": "succeeded"
            },
            "trace": {}
        })
        .to_string();
        write!(
            status,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
    });
    let client = MongrelClient::new(&format!("http://{address}")).unwrap();
    let error = client
        .sql_with_options(
            "INSERT INTO jobs VALUES (1)",
            mongreldb_client::SqlClientOptions {
                query_id: Some(QUERY_ID.parse().unwrap()),
                timeout: None,
            },
        )
        .unwrap_err();
    assert!(matches!(
        error,
        ClientError::Query {
            code: RemoteQueryErrorCode::CommitOutcome,
            response,
            ..
        } if response.committed == Some(true) && response.last_commit_epoch == Some(77)
    ));
    server.join().unwrap();
}
