use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use mongreldb_client::{
    AsyncMongrelClient, ClientError, KitErrorCode, KitOp, KitQueryRequest, KitTxnRequest,
    MongrelClient, RemoteQueryErrorCode, ReplicationFollower, SqlClientOptions,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const QUERY_ID: &str = "11112222333344445555666677778888";

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
        },
        "sql_pagination": {
            "version": 1,
            "continuation_endpoint": "/sql/continue",
            "retained_snapshot": true,
            "projection_required": true,
            "byte_and_token_hints": true
        }
    })
}

fn completed_status() -> serde_json::Value {
    serde_json::json!({
        "query_id": QUERY_ID,
        "status": "completed",
        "terminal_state": "completed",
        "state": "completed",
        "server_state": "completed",
        "operation": "SELECT",
        "committed": false,
        "committed_statements": 0,
        "last_commit_epoch": null,
        "last_commit_epoch_text": null,
        "first_commit_statement_index": null,
        "last_commit_statement_index": null,
        "completed_statements": 1,
        "statement_index": 0,
        "cancel_outcome": "already_finished",
        "cancellation_reason": "none",
        "retryable": false,
        "outcome": {
            "committed": false,
            "committed_statements": 0,
            "last_commit_epoch": null,
            "last_commit_epoch_text": null,
            "first_commit_statement_index": null,
            "last_commit_statement_index": null,
            "completed_statements": 1,
            "statement_index": 0,
            "serialization": "succeeded"
        },
        "terminal_error": null,
        "trace": {}
    })
}

async fn assert_kit_rejected(body: String, status: u16, request: KitTxnRequest) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/kit/txn"))
        .respond_with(ResponseTemplate::new(status).set_body_raw(body, "application/json"))
        .expect(2)
        .mount(&server)
        .await;

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
            code: KitErrorCode::QueryOutcomeUnknown,
            committed: None,
            ..
        }
    ));

    let asynchronous = AsyncMongrelClient::new(&uri)
        .unwrap()
        .kit_txn(&request)
        .await
        .unwrap_err();
    assert!(matches!(
        asynchronous,
        ClientError::Kit {
            code: KitErrorCode::QueryOutcomeUnknown,
            committed: None,
            ..
        }
    ));
}

async fn assert_kit_known_commit(body: String, status: u16, request: KitTxnRequest) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/kit/txn"))
        .respond_with(ResponseTemplate::new(status).set_body_raw(body, "application/json"))
        .expect(2)
        .mount(&server)
        .await;

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
            code: KitErrorCode::CommitOutcome,
            committed: Some(true),
            epoch: Some(1),
            ..
        }
    ));

    let asynchronous = AsyncMongrelClient::new(&uri)
        .unwrap()
        .kit_txn(&request)
        .await
        .unwrap_err();
    assert!(matches!(
        asynchronous,
        ClientError::Kit {
            code: KitErrorCode::CommitOutcome,
            committed: Some(true),
            epoch: Some(1),
            ..
        }
    ));
}

#[tokio::test]
async fn kit_txn_rejects_duplicate_unknown_and_oversized_envelopes() {
    let empty = KitTxnRequest::new(Vec::new());
    assert_kit_rejected(
        r#"{"status":"aborted","status":"committed","error":{"code":"BAD_REQUEST","message":"bad"}}"#
            .into(),
        400,
        empty.clone(),
    )
    .await;
    assert_kit_known_commit(
        r#"{"status":"committed","epoch":1,"epoch_text":"1","results":[],"unknown":true}"#.into(),
        200,
        empty.clone(),
    )
    .await;
    assert_kit_rejected(
        r#"{"status":"aborted","unknown":true,"error":{"code":"BAD_REQUEST","message":"bad"}}"#
            .into(),
        400,
        empty.clone(),
    )
    .await;
    assert_kit_rejected(
        r#"{"status":"aborted","committed":false,"error":{"code":"BAD_REQUEST","message":"bad"}}"#
            .into(),
        400,
        empty.clone(),
    )
    .await;
    assert_kit_rejected(
        r#"{"status":"outcome_unknown","committed":null,"epoch":null,"retryable":false,"error":{"code":"QUERY_OUTCOME_UNKNOWN","message":"unknown"}}"#
            .into(),
        409,
        empty.clone(),
    )
    .await;
    assert_kit_known_commit(
        r#"{"status":"committed","epoch":1,"epoch_text":"1","results":{},"unknown":true}"#.into(),
        200,
        empty.clone(),
    )
    .await;
    assert_kit_known_commit(
        r#"{"status":"committed","committed":true,"epoch":1,"epoch_text":"1","retryable":false,"unknown":true,"error":{"code":"COMMIT_OUTCOME","message":"published"}}"#
            .into(),
        409,
        empty.clone(),
    )
    .await;
    assert_kit_known_commit(
        r#"{"status":"committed","committed":true,"epoch":1,"epoch_text":"1","retryable":false,"error":{"code":"COMMIT_OUTCOME","message":"published","op_index":null}}"#
            .into(),
        409,
        empty.clone(),
    )
    .await;

    let oversized = format!(
        "{{\"status\":\"committed\",\"epoch\":1,\"epoch_text\":\"1\",\"results\":[],\"unknown\":\"{}\"}}",
        "x".repeat(65 * 1024 * 1024)
    );
    assert_kit_rejected(oversized, 200, empty).await;
}

#[tokio::test]
async fn dynamic_path_segments_cannot_escape_their_route() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "committed",
            "epoch": 1,
            "epoch_text": "1"
        })))
        .expect(1)
        .mount(&server)
        .await;
    let uri = server.uri();
    tokio::task::spawn_blocking(move || {
        MongrelClient::new(&uri)
            .unwrap()
            .drop_procedure("../tables/victim")
            .unwrap();
    })
    .await
    .unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].url.path(), "/procedures/..%2Ftables%2Fvictim");
}

#[tokio::test]
async fn kit_txn_rejects_status_epoch_count_and_kind_mismatches() {
    let request = KitTxnRequest::new(vec![KitOp::put("items", Vec::new())]);
    for body in [
        serde_json::json!({
            "status": "completed",
            "epoch": 1,
            "epoch_text": "1",
            "results": [{"kind": "put", "row_id": null, "auto_inc": null}]
        }),
        serde_json::json!({
            "status": "committed",
            "epoch": 1,
            "epoch_text": "2",
            "results": [{"kind": "put", "row_id": null, "auto_inc": null}]
        }),
    ] {
        assert_kit_rejected(body.to_string(), 200, request.clone()).await;
    }
    for body in [
        serde_json::json!({
            "status": "committed",
            "epoch": 1,
            "epoch_text": "1",
            "results": []
        }),
        serde_json::json!({
            "status": "committed",
            "epoch": 1,
            "epoch_text": "1",
            "results": [{"kind": "deleted"}]
        }),
        serde_json::json!({
            "status": "committed",
            "epoch": 1,
            "epoch_text": "1",
            "results": [{"kind": "put", "row_id": "7", "auto_inc": null}]
        }),
        serde_json::json!({
            "status": "committed",
            "epoch": 1,
            "epoch_text": "1",
            "results": [{"kind": "put", "row_id": null, "auto_inc": null, "row": [1]}]
        }),
    ] {
        assert_kit_known_commit(body.to_string(), 200, request.clone()).await;
    }
    assert_kit_known_commit(
        serde_json::json!({
            "status": "committed",
            "committed": true,
            "epoch": 1,
            "epoch_text": "1",
            "results": [{"kind": "deleted"}],
            "retryable": false,
            "error": {"code": "COMMIT_OUTCOME", "message": "published late"}
        })
        .to_string(),
        409,
        request,
    )
    .await;

    assert_kit_known_commit(
        serde_json::json!({
            "status": "committed",
            "committed": true,
            "epoch": 1,
            "epoch_text": "1",
            "results": [{"kind": "future_kind"}],
            "retryable": false,
            "error": {"code": "COMMIT_OUTCOME", "message": "published late"}
        })
        .to_string(),
        409,
        KitTxnRequest::new(vec![KitOp::put("items", Vec::new())]),
    )
    .await;

    assert_kit_known_commit(
        serde_json::json!({
            "status": "committed",
            "epoch": 1,
            "epoch_text": "1",
            "results": [{"kind": "put", "row_id": null, "auto_inc": null}]
        })
        .to_string(),
        200,
        KitTxnRequest::new(vec![KitOp::put_returning("items", Vec::new())]),
    )
    .await;
}

#[tokio::test]
async fn kit_txn_accepts_exact_result_kinds_in_both_clients() {
    let request = KitTxnRequest::new(vec![
        KitOp::put("items", Vec::new()),
        KitOp::Upsert {
            table: "items".into(),
            cells: Vec::new(),
            update_cells: None,
            returning: false,
        },
        KitOp::Delete {
            table: "items".into(),
            row_id: 1,
        },
        KitOp::delete_by_pk("items", serde_json::json!(2)),
    ]);
    let response = serde_json::json!({
        "status": "committed",
        "epoch": 9_007_199_254_740_993_u64,
        "epoch_text": "9007199254740993",
        "results": [
            {"kind": "put", "row_id": null, "auto_inc": null},
            {"kind": "upsert", "action": "unchanged", "auto_inc": null},
            {"kind": "deleted"},
            {"kind": "not_found"}
        ]
    });
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/kit/txn"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response))
        .expect(2)
        .mount(&server)
        .await;

    let uri = server.uri();
    let blocking_uri = uri.clone();
    let blocking_request = request.clone();
    let blocking = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&blocking_uri)
            .unwrap()
            .kit_txn(&blocking_request)
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(blocking.results.len(), 4);

    let asynchronous = AsyncMongrelClient::new(&uri)
        .unwrap()
        .kit_txn(&request)
        .await
        .unwrap();
    assert_eq!(asynchronous.results.len(), 4);
}

#[tokio::test]
async fn kit_txn_preserves_known_commit_when_error_omits_results() {
    let body = serde_json::json!({
        "status": "committed",
        "committed": true,
        "epoch": 9,
        "epoch_text": "9",
        "retryable": false,
        "error": {"code": "COMMIT_OUTCOME", "message": "published late"}
    });
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/kit/txn"))
        .respond_with(ResponseTemplate::new(409).set_body_json(body))
        .expect(2)
        .mount(&server)
        .await;
    let request = KitTxnRequest::new(vec![KitOp::put("items", Vec::new())]);

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
            code: KitErrorCode::CommitOutcome,
            committed: Some(true),
            epoch: Some(9),
            ..
        }
    ));

    let asynchronous = AsyncMongrelClient::new(&uri)
        .unwrap()
        .kit_txn(&request)
        .await
        .unwrap_err();
    assert!(matches!(
        asynchronous,
        ClientError::Kit {
            code: KitErrorCode::CommitOutcome,
            committed: Some(true),
            epoch: Some(9),
            ..
        }
    ));
}

#[tokio::test]
async fn non_sql_durable_error_is_not_misclassified_as_query_error() {
    let body = serde_json::json!({
        "status": "committed",
        "committed": true,
        "epoch": 11,
        "epoch_text": "11",
        "retryable": false,
        "error": {"code": "COMMIT_OUTCOME", "message": "trigger drop committed"}
    });
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/triggers/example"))
        .respond_with(ResponseTemplate::new(409).set_body_json(body))
        .expect(1)
        .mount(&server)
        .await;
    let uri = server.uri();
    let error = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&uri)
            .unwrap()
            .drop_trigger("example")
            .unwrap_err()
    })
    .await
    .unwrap();
    assert!(matches!(
        error,
        ClientError::Kit {
            code: KitErrorCode::CommitOutcome,
            committed: Some(true),
            epoch: Some(11),
            ..
        }
    ));
}

#[tokio::test]
async fn kit_query_rejects_inconsistent_continuation_cursor() {
    for body in [
        serde_json::json!({"rows": [], "truncated": true, "next_cursor": ""}),
        serde_json::json!({"rows": [], "truncated": true, "next_cursor": "x".repeat(2049)}),
        serde_json::json!({"rows": [], "truncated": false, "next_cursor": "cursor"}),
        serde_json::json!({
            "rows": [{"row_id": "01", "cells": [1, "value"]}],
            "truncated": false,
            "next_cursor": null
        }),
        serde_json::json!({
            "rows": [{"row_id": "1", "cells": [1]}],
            "truncated": false,
            "next_cursor": null
        }),
        serde_json::json!({
            "rows": [{"row_id": "1", "cells": [1, "a", 1, "b"]}],
            "truncated": false,
            "next_cursor": null
        }),
        serde_json::json!({
            "rows": [
                {"row_id": "1", "cells": [1, "a"]},
                {"row_id": "1", "cells": [1, "b"]}
            ],
            "truncated": false,
            "next_cursor": null
        }),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/kit/query"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(1)
            .mount(&server)
            .await;
        let uri = server.uri();
        let error = tokio::task::spawn_blocking(move || {
            MongrelClient::new(&uri)
                .unwrap()
                .kit_query(&KitQueryRequest {
                    table: "items".into(),
                    conditions: Vec::new(),
                    projection: None,
                    limit: None,
                    offset: None,
                    cursor: None,
                })
                .unwrap_err()
        })
        .await
        .unwrap();
        assert!(matches!(error, ClientError::Decode(_)));
    }
}

#[tokio::test]
async fn kit_query_response_is_bound_to_limit_and_projection() {
    for (body, projection, limit) in [
        (
            serde_json::json!({
                "rows": [
                    {"row_id": "1", "cells": [1, "a"]},
                    {"row_id": "2", "cells": [1, "b"]}
                ],
                "truncated": false,
                "next_cursor": null
            }),
            None,
            Some(1),
        ),
        (
            serde_json::json!({
                "rows": [{"row_id": "1", "cells": [2, "secret"]}],
                "truncated": false,
                "next_cursor": null
            }),
            Some(vec![1]),
            Some(1),
        ),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/kit/query"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(1)
            .mount(&server)
            .await;
        let uri = server.uri();
        let error = tokio::task::spawn_blocking(move || {
            MongrelClient::new(&uri)
                .unwrap()
                .kit_query(&KitQueryRequest {
                    table: "items".into(),
                    conditions: Vec::new(),
                    projection,
                    limit,
                    offset: None,
                    cursor: None,
                })
                .unwrap_err()
        })
        .await
        .unwrap();
        assert!(matches!(error, ClientError::Decode(_)));
    }
}

#[tokio::test]
async fn query_status_rejects_duplicate_and_unknown_fields() {
    for body in [
        format!("{{\"query_id\":\"{QUERY_ID}\",\"query_id\":\"{QUERY_ID}\"}}"),
        {
            let mut status = completed_status();
            status["unknown"] = serde_json::json!(true);
            status.to_string()
        },
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
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
            .expect(2)
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

        let asynchronous = AsyncMongrelClient::new(&uri)
            .unwrap()
            .query_status(QUERY_ID.parse().unwrap())
            .await
            .unwrap_err();
        assert!(matches!(asynchronous, ClientError::Decode(_)));
    }
}

#[tokio::test]
async fn duplicate_sql_receipt_recovers_status_in_both_clients() {
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
                .set_body_raw(
                    format!("{{\"query_id\":\"{QUERY_ID}\",\"query_id\":\"{QUERY_ID}\"}}"),
                    "application/json",
                ),
        )
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/queries/{QUERY_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(completed_status()))
        .expect(4)
        .mount(&server)
        .await;

    let options = SqlClientOptions {
        query_id: Some(QUERY_ID.parse().unwrap()),
        timeout: None,
    };
    let uri = server.uri();
    let blocking_uri = uri.clone();
    let blocking_options = options.clone();
    let blocking = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&blocking_uri)
            .unwrap()
            .sql_write_idempotent_with_options(
                "INSERT INTO items VALUES (1)",
                "blocking-key",
                blocking_options,
            )
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

    let asynchronous = AsyncMongrelClient::new(&uri)
        .unwrap()
        .sql_write_idempotent_with_options("INSERT INTO items VALUES (1)", "async-key", options)
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

#[tokio::test]
async fn malformed_query_not_found_never_authorizes_idempotent_replay() {
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
                .set_body_raw("{}", "application/json"),
        )
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/queries/{QUERY_ID}")))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "error": {"code": "QUERY_NOT_FOUND", "message": "missing"}
        })))
        .mount(&server)
        .await;

    let options = SqlClientOptions {
        query_id: Some(QUERY_ID.parse().unwrap()),
        timeout: None,
    };
    let uri = server.uri();
    let blocking_uri = uri.clone();
    let blocking_options = options.clone();
    let blocking = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&blocking_uri)
            .unwrap()
            .sql_write_idempotent_with_options(
                "INSERT INTO items VALUES (1)",
                "blocking-key",
                blocking_options,
            )
            .unwrap_err()
    })
    .await
    .unwrap();
    assert!(matches!(blocking, ClientError::QueryOutcomeUnknown { .. }));

    let asynchronous = AsyncMongrelClient::new(&uri)
        .unwrap()
        .sql_write_idempotent_with_options("INSERT INTO items VALUES (1)", "async-key", options)
        .await
        .unwrap_err();
    assert!(matches!(
        asynchronous,
        ClientError::QueryOutcomeUnknown { .. }
    ));
}

fn read_request(stream: &mut TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 4096];
        let read = stream.read(&mut chunk).unwrap();
        assert!(read > 0);
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            line.split_once(':').and_then(|(name, value)| {
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
        })
        .unwrap_or(0);
    while bytes.len() < header_end + content_length {
        let mut chunk = [0_u8; 4096];
        let read = stream.read(&mut chunk).unwrap();
        assert!(read > 0);
        bytes.extend_from_slice(&chunk[..read]);
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

fn write_json(stream: &mut TcpStream, value: &serde_json::Value) {
    let body = value.to_string();
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
}

fn spawn_oversized_sql_server() -> (std::net::SocketAddr, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut capabilities_request, _) = listener.accept().unwrap();
        assert!(read_request(&mut capabilities_request).starts_with("GET /capabilities "));
        write_json(&mut capabilities_request, &capabilities());

        let (mut sql_request, _) = listener.accept().unwrap();
        assert!(read_request(&mut sql_request).starts_with("POST /sql "));
        write!(
            sql_request,
            "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.apache.arrow.file\r\nx-mongreldb-query-id: {QUERY_ID}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            65_u64 * 1024 * 1024 + 1
        )
        .unwrap();

        let (mut status_request, _) = listener.accept().unwrap();
        assert!(read_request(&mut status_request).starts_with(&format!("GET /queries/{QUERY_ID} ")));
        write_json(&mut status_request, &completed_status());
    });
    (address, server)
}

#[test]
fn blocking_oversized_sql_response_recovers_status() {
    let (address, server) = spawn_oversized_sql_server();
    let error = MongrelClient::new(&format!("http://{address}"))
        .unwrap()
        .sql_with_options(
            "SELECT 1",
            SqlClientOptions {
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
async fn async_oversized_sql_response_recovers_status() {
    let (address, server) = spawn_oversized_sql_server();
    let error = AsyncMongrelClient::new(&format!("http://{address}"))
        .unwrap()
        .sql_with_options(
            "SELECT 1",
            SqlClientOptions {
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

fn temporary_replica_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "mongreldb-client-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn replication_snapshot_response_is_bounded() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut request, _) = listener.accept().unwrap();
        assert!(read_request(&mut request).starts_with("GET /replication/snapshot "));
        write!(
            request,
            concat!(
                "HTTP/1.1 200 OK\r\n",
                "x-mongreldb-source-id: {}\r\n",
                "x-mongreldb-current-epoch: 0\r\n",
                "Content-Length: {}\r\n",
                "Connection: close\r\n\r\n"
            ),
            "00".repeat(32),
            512_u64 * 1024 * 1024 + 1
        )
        .unwrap();
    });
    let path = temporary_replica_path("snapshot-cap");
    let error = ReplicationFollower::new(&format!("http://{address}"), &path)
        .unwrap()
        .bootstrap()
        .unwrap_err();
    assert!(error.contains("response exceeds"));
    server.join().unwrap();
    let _ = std::fs::remove_dir_all(path);
}

#[test]
fn replication_wal_response_is_bounded() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut request, _) = listener.accept().unwrap();
        assert!(read_request(&mut request).starts_with("GET /wal/stream?since=0 "));
        write!(
            request,
            concat!(
                "HTTP/1.1 200 OK\r\n",
                "x-mongreldb-from-epoch: 0\r\n",
                "x-mongreldb-current-epoch: 0\r\n",
                "x-mongreldb-source-id: {}\r\n",
                "x-mongreldb-commit-count: 0\r\n",
                "x-mongreldb-records-sha256: {}\r\n",
                "Content-Length: {}\r\n",
                "Connection: close\r\n\r\n"
            ),
            "01".repeat(32),
            "00".repeat(32),
            256_u64 * 1024 * 1024 + 1
        )
        .unwrap();
    });
    let path = temporary_replica_path("wal-cap");
    std::fs::create_dir_all(path.join("_meta")).unwrap();
    std::fs::write(path.join("CATALOG"), []).unwrap();
    std::fs::write(path.join("_meta/replica"), b"read-only replica\n").unwrap();
    std::fs::write(path.join("_meta/replication_source_id"), [1_u8; 32]).unwrap();
    let mut follower = ReplicationFollower::new(&format!("http://{address}"), &path).unwrap();
    let error = follower.sync().unwrap_err();
    assert!(error.contains("response exceeds"));
    server.join().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}
