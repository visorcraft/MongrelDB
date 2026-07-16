use std::sync::{Arc, Mutex};

use mongreldb_client::{
    ClientError, MongrelClient, RemoteQueryErrorCode, RemoteSqlControlOptions, SqlClientOptions,
    SqlPageOptions,
};
use wiremock::matchers::{body_json, method, path, path_regex};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const QUERY_ID: &str = "11112222333344445555666677778888";

fn sql_response(status: u16) -> ResponseTemplate {
    sql_response_for(status, QUERY_ID)
}

fn sql_response_for(status: u16, query_id: &str) -> ResponseTemplate {
    ResponseTemplate::new(status).insert_header("x-mongreldb-query-id", query_id)
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

fn query_not_found(query_id: &str) -> serde_json::Value {
    serde_json::json!({
        "query_id": query_id,
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
            "query_id": query_id,
            "committed": null,
            "retryable": false
        }
    })
}

fn queryless_sql_error(code: &str) -> serde_json::Value {
    serde_json::json!({
        "status": "failed_before_commit",
        "terminal_state": "failed_before_commit",
        "server_state": "failed",
        "committed": false,
        "committed_statements": 0,
        "last_commit_epoch": null,
        "last_commit_epoch_text": null,
        "first_commit_statement_index": null,
        "last_commit_statement_index": null,
        "completed_statements": 0,
        "statement_index": 0,
        "cancel_outcome": null,
        "cancellation_reason": null,
        "retryable": false,
        "outcome": {
            "committed": false,
            "committed_statements": 0,
            "last_commit_epoch": null,
            "last_commit_epoch_text": null,
            "first_commit_statement_index": null,
            "last_commit_statement_index": null,
            "completed_statements": 0,
            "statement_index": 0,
            "serialization": "not_started"
        },
        "error": {
            "code": code,
            "message": "queryless SQL error",
            "committed": false,
            "retryable": false
        }
    })
}

fn identified_sql_error(code: &str, query_id: &str) -> serde_json::Value {
    let mut error = queryless_sql_error(code);
    error["query_id"] = serde_json::json!(query_id);
    error["error"]["query_id"] = serde_json::json!(query_id);
    error["cancel_outcome"] = serde_json::json!("already_finished");
    error["cancellation_reason"] = serde_json::json!("none");
    error
}

fn exact_query_not_found(request: &Request) -> ResponseTemplate {
    let query_id = request
        .url
        .path_segments()
        .and_then(Iterator::last)
        .unwrap_or_default();
    ResponseTemplate::new(404).set_body_json(query_not_found(query_id))
}

fn receipt() -> serde_json::Value {
    serde_json::json!({
        "query_id": QUERY_ID,
        "original_query_id": QUERY_ID,
        "status": "committed_with_error",
        "terminal_state": "committed_with_error",
        "server_state": "failed",
        "cancel_outcome": "already_finished",
        "cancellation_reason": "none",
        "committed": true,
        "committed_statements": 1,
        "last_commit_epoch": null,
        "last_commit_epoch_text": "9007199254740993",
        "first_commit_statement_index": 0,
        "last_commit_statement_index": 0,
        "completed_statements": 1,
        "statement_index": 0,
        "retryable": false,
        "idempotency_replayed": false,
        "idempotency_persisted": true,
        "idempotency_expires_at_ms": 123456,
        "outcome": {
            "committed": true,
            "committed_statements": 1,
            "last_commit_epoch": null,
            "last_commit_epoch_text": "9007199254740993",
            "first_commit_statement_index": 0,
            "last_commit_statement_index": 0,
            "completed_statements": 1,
            "statement_index": 0,
            "serialization": "failed"
        },
        "terminal_error": {
            "code": "SERIALIZATION_FAILED_AFTER_COMMIT",
            "category": "serialization"
        }
    })
}

fn malformed_receipt_cases() -> Vec<(&'static str, &'static str, serde_json::Value)> {
    let mut mirrored_commit = receipt();
    mirrored_commit["outcome"]["committed"] = serde_json::json!(false);

    let mut top_epoch_mismatch = receipt();
    top_epoch_mismatch["last_commit_epoch"] = serde_json::json!(7);
    top_epoch_mismatch["last_commit_epoch_text"] = serde_json::json!("8");
    top_epoch_mismatch["outcome"]["last_commit_epoch"] = serde_json::json!(8);
    top_epoch_mismatch["outcome"]["last_commit_epoch_text"] = serde_json::json!("8");

    let mut outcome_epoch_mismatch = receipt();
    outcome_epoch_mismatch["last_commit_epoch"] = serde_json::json!(8);
    outcome_epoch_mismatch["last_commit_epoch_text"] = serde_json::json!("8");
    outcome_epoch_mismatch["outcome"]["last_commit_epoch"] = serde_json::json!(7);
    outcome_epoch_mismatch["outcome"]["last_commit_epoch_text"] = serde_json::json!("8");

    let mut noncanonical_epoch = receipt();
    noncanonical_epoch["last_commit_epoch"] = serde_json::json!(8);
    noncanonical_epoch["last_commit_epoch_text"] = serde_json::json!("08");
    noncanonical_epoch["outcome"]["last_commit_epoch"] = serde_json::json!(8);
    noncanonical_epoch["outcome"]["last_commit_epoch_text"] = serde_json::json!("8");

    let mut missing_exact_epoch = receipt();
    missing_exact_epoch["last_commit_epoch"] = serde_json::json!(8);
    missing_exact_epoch["last_commit_epoch_text"] = serde_json::Value::Null;
    missing_exact_epoch["outcome"]["last_commit_epoch"] = serde_json::json!(8);
    missing_exact_epoch["outcome"]["last_commit_epoch_text"] = serde_json::Value::Null;

    let mut missing_indexes = receipt();
    missing_indexes["first_commit_statement_index"] = serde_json::Value::Null;
    missing_indexes["last_commit_statement_index"] = serde_json::Value::Null;
    missing_indexes["outcome"]["first_commit_statement_index"] = serde_json::Value::Null;
    missing_indexes["outcome"]["last_commit_statement_index"] = serde_json::Value::Null;

    let mut reversed_indexes = receipt();
    reversed_indexes["first_commit_statement_index"] = serde_json::json!(1);
    reversed_indexes["last_commit_statement_index"] = serde_json::json!(0);
    reversed_indexes["statement_index"] = serde_json::json!(1);
    reversed_indexes["outcome"]["first_commit_statement_index"] = serde_json::json!(1);
    reversed_indexes["outcome"]["last_commit_statement_index"] = serde_json::json!(0);
    reversed_indexes["outcome"]["statement_index"] = serde_json::json!(1);

    let mut count_exceeds_range = receipt();
    count_exceeds_range["committed_statements"] = serde_json::json!(2);
    count_exceeds_range["outcome"]["committed_statements"] = serde_json::json!(2);

    let mut last_exceeds_statement = receipt();
    last_exceeds_statement["last_commit_statement_index"] = serde_json::json!(1);
    last_exceeds_statement["outcome"]["last_commit_statement_index"] = serde_json::json!(1);

    let mut statement_exceeds_completed = receipt();
    statement_exceeds_completed["statement_index"] = serde_json::json!(2);
    statement_exceeds_completed["outcome"]["statement_index"] = serde_json::json!(2);

    let mut completed_exceeds_statement = receipt();
    completed_exceeds_statement["completed_statements"] = serde_json::json!(2);
    completed_exceeds_statement["outcome"]["completed_statements"] = serde_json::json!(2);

    vec![
        (
            "INSERT INTO items (id) VALUES (9)",
            "mirrored-commit",
            mirrored_commit,
        ),
        (
            "INSERT INTO items (id) VALUES (10)",
            "top-epoch-mismatch",
            top_epoch_mismatch,
        ),
        (
            "INSERT INTO items (id) VALUES (11)",
            "outcome-epoch-mismatch",
            outcome_epoch_mismatch,
        ),
        (
            "INSERT INTO items (id) VALUES (12)",
            "noncanonical-epoch",
            noncanonical_epoch,
        ),
        (
            "INSERT INTO items (id) VALUES (13)",
            "missing-exact-epoch",
            missing_exact_epoch,
        ),
        (
            "INSERT INTO items (id) VALUES (14)",
            "missing-indexes",
            missing_indexes,
        ),
        (
            "INSERT INTO items (id) VALUES (15)",
            "reversed-indexes",
            reversed_indexes,
        ),
        (
            "INSERT INTO items (id) VALUES (16)",
            "count-exceeds-range",
            count_exceeds_range,
        ),
        (
            "INSERT INTO items (id) VALUES (17)",
            "last-exceeds-statement",
            last_exceeds_statement,
        ),
        (
            "INSERT INTO items (id) VALUES (18)",
            "statement-exceeds-completed",
            statement_exceeds_completed,
        ),
        (
            "INSERT INTO items (id) VALUES (19)",
            "completed-exceeds-statement",
            completed_exceeds_statement,
        ),
    ]
}

fn malformed_receipt_preserves_commit(idempotency_key: &str) -> bool {
    matches!(
        idempotency_key,
        "missing-indexes"
            | "reversed-indexes"
            | "count-exceeds-range"
            | "last-exceeds-statement"
            | "statement-exceeds-completed"
            | "completed-exceeds-statement"
    )
}

fn completed_status() -> serde_json::Value {
    serde_json::json!({
        "query_id": QUERY_ID,
        "status": "completed",
        "state": "completed",
        "server_state": "completed",
        "terminal_state": "completed",
        "operation": "SELECT",
        "committed": false,
        "cancel_outcome": "already_finished",
        "cancellation_reason": "none",
        "committed_statements": 0,
        "completed_statements": 1,
        "statement_index": 0,
        "retryable": false,
        "outcome": {
            "committed": false,
            "committed_statements": 0,
            "completed_statements": 1,
            "statement_index": 0,
            "serialization": "succeeded"
        },
        "terminal_error": null,
        "trace": {}
    })
}

fn malformed_page() -> serde_json::Value {
    serde_json::json!({
        "status": "completed",
        "rows": [{"id": 1}],
        "next_cursor": null,
        "page": {
            "offset": 0,
            "row_count": 2,
            "total_rows": 1,
            "byte_count": 10,
            "estimated_tokens": 3,
            "limits": {"rows": 2, "bytes": 1024, "tokens": 256},
            "projection": ["id"],
            "expires_at_ms": 123456,
            "snapshot": "retained_result",
            "token_estimate": "ceil(projected_json_bytes/4)"
        }
    })
}

fn non_object_page() -> serde_json::Value {
    serde_json::json!({
        "status": "completed",
        "rows": [1],
        "next_cursor": null,
        "page": {
            "offset": 0,
            "row_count": 1,
            "total_rows": 1,
            "byte_count": 1,
            "estimated_tokens": 1,
            "limits": {"rows": 2, "bytes": 1024, "tokens": 256},
            "projection": ["id"],
            "expires_at_ms": 123456,
            "snapshot": "retained_result",
            "token_estimate": "ceil(projected_json_bytes/4)"
        }
    })
}

fn replay_receipt(query_id: &str) -> serde_json::Value {
    serde_json::json!({
        "query_id": query_id,
        "original_query_id": QUERY_ID,
        "status": "committed",
        "terminal_state": "committed",
        "server_state": "completed",
        "cancel_outcome": "already_finished",
        "cancellation_reason": "none",
        "committed": true,
        "committed_statements": 1,
        "last_commit_epoch": 51,
        "last_commit_epoch_text": "51",
        "first_commit_statement_index": 0,
        "last_commit_statement_index": 0,
        "completed_statements": 1,
        "statement_index": 0,
        "retryable": false,
        "idempotency_replayed": true,
        "idempotency_persisted": true,
        "idempotency_expires_at_ms": 123456,
        "outcome": {
            "committed": true,
            "committed_statements": 1,
            "last_commit_epoch": 51,
            "last_commit_epoch_text": "51",
            "first_commit_statement_index": 0,
            "last_commit_statement_index": 0,
            "completed_statements": 1,
            "statement_index": 0,
            "serialization": "succeeded"
        },
        "terminal_error": null
    })
}

fn assert_serialization_error(error: ClientError, query_id: Option<&str>) {
    assert!(matches!(
        error,
        ClientError::Query {
            code: RemoteQueryErrorCode::SerializationFailed,
            response,
            ..
        } if response.query_id.as_deref() == query_id
    ));
}

async fn mount_malformed_protocol_mocks(server: &MockServer) {
    let malformed_receipts = malformed_receipt_cases();
    let malformed_receipt_count = malformed_receipts.len() as u64;
    let unknown_receipt_count = malformed_receipts
        .iter()
        .filter(|(_, key, _)| !malformed_receipt_preserves_commit(key))
        .count() as u64;
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
        .expect(malformed_receipt_count + 2)
        .mount(server)
        .await;

    for (sql, idempotency_key, malformed_receipt) in malformed_receipts {
        Mock::given(method("POST"))
            .and(path("/sql"))
            .and(body_json(serde_json::json!({
                "sql": sql,
                "query_id": QUERY_ID,
                "idempotency_key": idempotency_key
            })))
            .respond_with(sql_response(200).set_body_json(malformed_receipt))
            .expect(1)
            .mount(server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/sql"))
        .and(body_json(serde_json::json!({
            "sql": "SELECT id FROM items",
            "query_id": QUERY_ID,
            "pagination": {
                "page_size_rows": 2,
                "projection": ["id"]
            }
        })))
        .respond_with(sql_response(200).set_body_json(non_object_page()))
        .expect(1)
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/sql/continue"))
        .and(body_json(serde_json::json!({
            "cursor": "bad-page-cursor",
            "operation_id": QUERY_ID,
            "timeout_ms": null
        })))
        .respond_with(sql_response(200).set_body_json(malformed_page()))
        .expect(1)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/queries/{QUERY_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(completed_status()))
        .expect(unknown_receipt_count * 2 + 1)
        .mount(server)
        .await;
}

async fn mount_restart_replay_mocks(server: &MockServer) -> Arc<Mutex<Vec<serde_json::Value>>> {
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
        .expect(2)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/queries/[0-9a-f]{32}$"))
        .respond_with(exact_query_not_found)
        .expect(1)
        .mount(server)
        .await;
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&requests);
    Mock::given(method("POST"))
        .and(path("/sql"))
        .respond_with(move |request: &Request| {
            let body: serde_json::Value = request.body_json().unwrap();
            let mut requests = captured.lock().unwrap();
            requests.push(body.clone());
            if requests.len() == 1 {
                sql_response_for(200, body["query_id"].as_str().unwrap()).set_body_bytes(b"{")
            } else {
                sql_response_for(200, body["query_id"].as_str().unwrap())
                    .set_body_json(replay_receipt(body["query_id"].as_str().unwrap()))
            }
        })
        .expect(2)
        .mount(server)
        .await;
    requests
}

async fn mount_first_execution_after_missing_mocks(
    server: &MockServer,
) -> Arc<Mutex<Vec<serde_json::Value>>> {
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
        .expect(2)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/queries/[0-9a-f]{32}$"))
        .respond_with(exact_query_not_found)
        .expect(1)
        .mount(server)
        .await;
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&requests);
    Mock::given(method("POST"))
        .and(path("/sql"))
        .respond_with(move |request: &Request| {
            let body: serde_json::Value = request.body_json().unwrap();
            let mut requests = captured.lock().unwrap();
            requests.push(body.clone());
            if requests.len() == 1 {
                sql_response_for(200, body["query_id"].as_str().unwrap()).set_body_bytes(b"{")
            } else {
                let query_id = body["query_id"].as_str().unwrap();
                let mut receipt = replay_receipt(query_id);
                receipt["original_query_id"] = serde_json::json!(query_id);
                receipt["idempotency_replayed"] = serde_json::json!(false);
                sql_response_for(200, query_id).set_body_json(receipt)
            }
        })
        .expect(2)
        .mount(server)
        .await;
    requests
}

async fn mount_wrong_original_replay_mocks(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
        .expect(2)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/queries/[0-9a-f]{32}$"))
        .respond_with(exact_query_not_found)
        .expect(2)
        .mount(server)
        .await;
    let attempts = Arc::new(Mutex::new(0_usize));
    Mock::given(method("POST"))
        .and(path("/sql"))
        .respond_with(move |request: &Request| {
            let mut attempts = attempts.lock().unwrap();
            *attempts += 1;
            if *attempts == 1 {
                let body: serde_json::Value = request.body_json().unwrap();
                sql_response_for(200, body["query_id"].as_str().unwrap()).set_body_bytes(b"{")
            } else {
                let body: serde_json::Value = request.body_json().unwrap();
                let mut receipt = replay_receipt(body["query_id"].as_str().unwrap());
                receipt["original_query_id"] =
                    serde_json::json!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
                sql_response_for(200, body["query_id"].as_str().unwrap()).set_body_json(receipt)
            }
        })
        .expect(2)
        .mount(server)
        .await;
}

fn assert_restart_replay(
    receipt: mongreldb_client::RemoteSqlReceipt,
    requests: &Arc<Mutex<Vec<serde_json::Value>>>,
) {
    assert!(receipt.idempotency_replayed);
    assert_eq!(receipt.original_query_id, QUERY_ID);
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["query_id"], QUERY_ID);
    assert_ne!(requests[1]["query_id"], QUERY_ID);
    assert_eq!(requests[0]["sql"], requests[1]["sql"]);
    assert_eq!(
        requests[0]["idempotency_key"],
        requests[1]["idempotency_key"]
    );
}

#[test]
fn protocol_bounds_are_rejected_before_network_io() {
    let client = MongrelClient::new("http://127.0.0.1:1").unwrap();
    assert!(matches!(
        client.sql_write_idempotent("INSERT INTO t VALUES (1)", ""),
        Err(ClientError::Decode(_))
    ));
    assert!(matches!(
        client.continue_sql_page("", RemoteSqlControlOptions::default()),
        Err(ClientError::Decode(_))
    ));
    assert!(matches!(
        client.sql_page("SELECT 1", SqlPageOptions::new(0, vec!["*".into()])),
        Err(ClientError::Decode(_))
    ));
}

#[test]
fn current_sql_protocol_codes_have_typed_variants() {
    for (code, expected) in [
        ("NO_SQL_TRANSACTION", RemoteQueryErrorCode::NoSqlTransaction),
        (
            "SAVEPOINT_NOT_FOUND",
            RemoteQueryErrorCode::SavepointNotFound,
        ),
        ("QUERY_FAILED", RemoteQueryErrorCode::QueryFailed),
        (
            "SERIALIZATION_WORKER_FAILED",
            RemoteQueryErrorCode::SerializationWorkerFailed,
        ),
        (
            "INVALID_QUERY_OPTIONS",
            RemoteQueryErrorCode::InvalidQueryOptions,
        ),
        (
            "INCOMPATIBLE_SQL_CONTROLS",
            RemoteQueryErrorCode::IncompatibleSqlControls,
        ),
        (
            "INVALID_IDEMPOTENCY_KEY",
            RemoteQueryErrorCode::InvalidIdempotencyKey,
        ),
        (
            "IDEMPOTENCY_REQUIRES_JSON",
            RemoteQueryErrorCode::IdempotencyRequiresJson,
        ),
        (
            "IDEMPOTENCY_REQUIRES_SINGLE_WRITE",
            RemoteQueryErrorCode::IdempotencyRequiresSingleWrite,
        ),
        (
            "IDEMPOTENCY_STORE_FULL",
            RemoteQueryErrorCode::IdempotencyStoreFull,
        ),
        (
            "PAGINATION_REQUIRES_JSON",
            RemoteQueryErrorCode::PaginationRequiresJson,
        ),
        (
            "PAGINATION_REQUIRES_SINGLE_READ_QUERY",
            RemoteQueryErrorCode::PaginationRequiresSingleReadQuery,
        ),
        (
            "INVALID_PAGINATION_OPTIONS",
            RemoteQueryErrorCode::InvalidPaginationOptions,
        ),
        (
            "INVALID_PAGE_OFFSET",
            RemoteQueryErrorCode::InvalidPageOffset,
        ),
        (
            "SQL_PAGE_STORE_FULL",
            RemoteQueryErrorCode::SqlPageStoreFull,
        ),
        (
            "SQL_ADMISSION_CLOSED",
            RemoteQueryErrorCode::SqlAdmissionClosed,
        ),
        (
            "ENTROPY_UNAVAILABLE",
            RemoteQueryErrorCode::EntropyUnavailable,
        ),
    ] {
        let decoded: RemoteQueryErrorCode =
            serde_json::from_value(serde_json::json!(code)).unwrap();
        assert_eq!(decoded, expected);
        assert_eq!(decoded.as_str(), code);
    }
}

#[tokio::test]
async fn blocking_client_decodes_idempotent_receipt_and_unknown_outcome() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
        .expect(3)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/sql"))
        .and(body_json(serde_json::json!({
            "sql": "INSERT INTO items (id) VALUES (1)",
            "query_id": QUERY_ID,
            "idempotency_key": "write-key"
        })))
        .respond_with(sql_response(200).set_body_json(receipt()))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/sql"))
        .and(body_json(serde_json::json!({
            "sql": "INSERT INTO items (id) VALUES (3)",
            "query_id": QUERY_ID,
            "idempotency_key": "receipt-loss-key"
        })))
        .respond_with(sql_response(200).set_body_bytes(b"{"))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/queries/{QUERY_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "query_id": QUERY_ID,
            "status": "committed",
            "state": "completed",
            "server_state": "completed",
            "terminal_state": "committed",
            "operation": "INSERT",
            "committed": true,
            "cancel_outcome": "already_finished",
            "cancellation_reason": "none",
            "committed_statements": 1,
            "last_commit_epoch": 43,
            "last_commit_epoch_text": "43",
            "first_commit_statement_index": 0,
            "last_commit_statement_index": 0,
            "completed_statements": 1,
            "statement_index": 0,
            "retryable": false,
            "outcome": {
                "committed": true,
                "committed_statements": 1,
                "last_commit_epoch": 43,
                "last_commit_epoch_text": "43",
                "first_commit_statement_index": 0,
                "last_commit_statement_index": 0,
                "completed_statements": 1,
                "statement_index": 0,
                "serialization": "succeeded"
            },
            "terminal_error": null,
            "trace": {}
        })))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/sql"))
        .and(body_json(serde_json::json!({
            "sql": "INSERT INTO items (id) VALUES (2)",
            "query_id": QUERY_ID,
            "idempotency_key": "crash-key"
        })))
        .respond_with(sql_response(409).set_body_json(serde_json::json!({
            "query_id": QUERY_ID,
            "status": "outcome_unknown",
            "terminal_state": "outcome_unknown",
            "committed": null,
            "committed_statements": null,
            "last_commit_epoch": null,
            "last_commit_epoch_text": null,
            "first_commit_statement_index": null,
            "last_commit_statement_index": null,
            "completed_statements": null,
            "statement_index": null,
            "cancel_outcome": "already_finished",
            "cancellation_reason": "none",
            "retryable": false,
            "server_state": "failed",
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
                "code": "QUERY_OUTCOME_UNKNOWN",
                "message": "durable intent has no receipt",
                "query_id": QUERY_ID,
                "committed": null,
                "retryable": false
            }
        })))
        .expect(1)
        .mount(&server)
        .await;
    let uri = server.uri();
    tokio::task::spawn_blocking(move || {
        let client = MongrelClient::new(&uri).unwrap();
        let options = SqlClientOptions {
            query_id: Some(QUERY_ID.parse().unwrap()),
            timeout: None,
        };
        let receipt = client
            .sql_write_idempotent_with_options(
                "INSERT INTO items (id) VALUES (1)",
                "write-key",
                options.clone(),
            )
            .unwrap();
        assert_eq!(receipt.last_commit_epoch, Some(9_007_199_254_740_993));
        assert_eq!(
            receipt.last_commit_epoch_text.as_deref(),
            Some("9007199254740993")
        );
        assert_eq!(receipt.first_commit_statement_index, Some(0));
        assert_eq!(receipt.last_commit_statement_index, Some(0));
        assert!(receipt.idempotency_persisted);
        assert_eq!(receipt.status, "committed_with_error");
        assert_eq!(receipt.server_state, "failed");
        assert_eq!(
            receipt.cancel_outcome,
            Some(mongreldb_client::RemoteCancelOutcome::AlreadyFinished)
        );
        assert_eq!(receipt.cancellation_reason, "none");
        assert_eq!(
            receipt.terminal_error.unwrap().code,
            RemoteQueryErrorCode::SerializationFailedAfterCommit
        );

        let error = client
            .sql_write_idempotent_with_options(
                "INSERT INTO items (id) VALUES (2)",
                "crash-key",
                options.clone(),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            ClientError::Query {
                code: RemoteQueryErrorCode::QueryOutcomeUnknown,
                response,
                ..
            } if response.status == "outcome_unknown"
                && response.committed.is_none()
                && response.committed_statements.is_none()
                && response.completed_statements.is_none()
                && response.statement_index.is_none()
                && response.error.committed.is_none()
        ));

        let error = client
            .sql_write_idempotent_with_options(
                "INSERT INTO items (id) VALUES (3)",
                "receipt-loss-key",
                options,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            ClientError::Query {
                code: RemoteQueryErrorCode::CommitOutcome,
                response,
                ..
            } if response.committed == Some(true)
                && response.last_commit_epoch == Some(43)
                && response.last_commit_epoch_text.as_deref() == Some("43")
        ));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn malformed_receipt_preserves_proven_commit_in_both_clients() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
        .expect(2)
        .mount(&server)
        .await;
    let mut malformed = receipt();
    malformed["unknown"] = serde_json::json!(true);
    Mock::given(method("POST"))
        .and(path("/sql"))
        .respond_with(sql_response(200).set_body_json(malformed))
        .expect(2)
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
                "INSERT INTO items (id) VALUES (1)",
                "malformed-receipt",
                blocking_options,
            )
            .unwrap_err()
    })
    .await
    .unwrap();
    assert!(matches!(
        &blocking,
        ClientError::Query {
            code: RemoteQueryErrorCode::CommitOutcome,
            response,
            ..
        } if response.committed == Some(true)
            && response.last_commit_epoch == Some(9_007_199_254_740_993)
    ));

    let asynchronous = mongreldb_client::AsyncMongrelClient::new(&uri)
        .unwrap()
        .sql_write_idempotent_with_options(
            "INSERT INTO items (id) VALUES (1)",
            "malformed-receipt",
            options,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        asynchronous,
        ClientError::Query {
            code: RemoteQueryErrorCode::CommitOutcome,
            response,
            ..
        } if response.committed == Some(true)
            && response.last_commit_epoch == Some(9_007_199_254_740_993)
    ));
}

#[tokio::test]
async fn blocking_client_rejects_malformed_receipts_and_pages() {
    let server = MockServer::start().await;
    mount_malformed_protocol_mocks(&server).await;
    let uri = server.uri();
    tokio::task::spawn_blocking(move || {
        let client = MongrelClient::new(&uri).unwrap();
        let options = SqlClientOptions {
            query_id: Some(QUERY_ID.parse().unwrap()),
            timeout: None,
        };
        for (sql, idempotency_key, _) in malformed_receipt_cases() {
            let error = client
                .sql_write_idempotent_with_options(sql, idempotency_key, options.clone())
                .unwrap_err();
            if malformed_receipt_preserves_commit(idempotency_key) {
                assert!(matches!(
                    error,
                    ClientError::Query {
                        code: RemoteQueryErrorCode::CommitOutcome,
                        response,
                        ..
                    } if response.committed == Some(true)
                ));
            } else {
                assert_serialization_error(error, Some(QUERY_ID));
            }
        }

        let mut options = SqlPageOptions::new(2, vec!["id".into()]);
        options.query_id = Some(QUERY_ID.parse().unwrap());
        assert_serialization_error(
            client
                .sql_page("SELECT id FROM items", options)
                .unwrap_err(),
            Some(QUERY_ID),
        );
        assert_serialization_error(
            client
                .continue_sql_page(
                    "bad-page-cursor",
                    RemoteSqlControlOptions {
                        query_id: Some(QUERY_ID.parse().unwrap()),
                        timeout: None,
                    },
                )
                .unwrap_err(),
            Some(QUERY_ID),
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn async_client_rejects_malformed_receipts_and_pages() {
    let server = MockServer::start().await;
    mount_malformed_protocol_mocks(&server).await;
    let client = mongreldb_client::AsyncMongrelClient::new(&server.uri()).unwrap();
    let options = SqlClientOptions {
        query_id: Some(QUERY_ID.parse().unwrap()),
        timeout: None,
    };
    for (sql, idempotency_key, _) in malformed_receipt_cases() {
        let error = client
            .sql_write_idempotent_with_options(sql, idempotency_key, options.clone())
            .await
            .unwrap_err();
        if malformed_receipt_preserves_commit(idempotency_key) {
            assert!(matches!(
                error,
                ClientError::Query {
                    code: RemoteQueryErrorCode::CommitOutcome,
                    response,
                    ..
                } if response.committed == Some(true)
            ));
        } else {
            assert_serialization_error(error, Some(QUERY_ID));
        }
    }

    let mut options = SqlPageOptions::new(2, vec!["id".into()]);
    options.query_id = Some(QUERY_ID.parse().unwrap());
    assert_serialization_error(
        client
            .sql_page("SELECT id FROM items", options)
            .await
            .unwrap_err(),
        Some(QUERY_ID),
    );
    assert_serialization_error(
        client
            .continue_sql_page(
                "bad-page-cursor",
                RemoteSqlControlOptions {
                    query_id: Some(QUERY_ID.parse().unwrap()),
                    timeout: None,
                },
            )
            .await
            .unwrap_err(),
        Some(QUERY_ID),
    );
}

#[tokio::test]
async fn identified_cursor_errors_remain_typed_in_both_clients() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
        .expect(2)
        .mount(&server)
        .await;
    let body = identified_sql_error("SQL_CURSOR_NOT_FOUND", QUERY_ID);
    Mock::given(method("POST"))
        .and(path("/sql/continue"))
        .and(body_json(serde_json::json!({
            "cursor": "missing-cursor",
            "operation_id": QUERY_ID,
            "timeout_ms": null
        })))
        .respond_with(sql_response(404).set_body_json(body))
        .expect(2)
        .mount(&server)
        .await;

    let uri = server.uri();
    let blocking_uri = uri.clone();
    let blocking = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&blocking_uri)
            .unwrap()
            .continue_sql_page(
                "missing-cursor",
                RemoteSqlControlOptions {
                    query_id: Some(QUERY_ID.parse().unwrap()),
                    timeout: None,
                },
            )
            .unwrap_err()
    })
    .await
    .unwrap();
    assert!(
        matches!(
            &blocking,
            ClientError::Query {
                code: RemoteQueryErrorCode::SqlCursorNotFound,
                response,
                ..
            } if response.query_id.as_deref() == Some(QUERY_ID) && response.committed == Some(false)
        ),
        "{blocking:?}"
    );

    let asynchronous = mongreldb_client::AsyncMongrelClient::new(&uri)
        .unwrap()
        .continue_sql_page(
            "missing-cursor",
            RemoteSqlControlOptions {
                query_id: Some(QUERY_ID.parse().unwrap()),
                timeout: None,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        asynchronous,
        ClientError::Query {
            code: RemoteQueryErrorCode::SqlCursorNotFound,
            response,
            ..
        } if response.query_id.as_deref() == Some(QUERY_ID) && response.committed == Some(false)
    ));
}

#[tokio::test]
async fn queryless_non_cursor_codes_fail_closed_in_both_clients() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/sql/continue"))
        .respond_with(
            ResponseTemplate::new(499).set_body_json(queryless_sql_error("QUERY_CANCELLED")),
        )
        .expect(2)
        .mount(&server)
        .await;

    let uri = server.uri();
    let blocking_uri = uri.clone();
    let blocking = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&blocking_uri)
            .unwrap()
            .continue_sql_page("missing-cursor", RemoteSqlControlOptions::default())
            .unwrap_err()
    })
    .await
    .unwrap();
    assert!(matches!(blocking, ClientError::QueryOutcomeUnknown { .. }));

    let asynchronous = mongreldb_client::AsyncMongrelClient::new(&uri)
        .unwrap()
        .continue_sql_page("missing-cursor", RemoteSqlControlOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(
        asynchronous,
        ClientError::QueryOutcomeUnknown { .. }
    ));
}

#[tokio::test]
async fn blocking_client_replays_idempotency_key_once_after_restart() {
    let server = MockServer::start().await;
    let requests = mount_restart_replay_mocks(&server).await;
    let uri = server.uri();
    let receipt = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&uri)
            .unwrap()
            .sql_write_idempotent_with_options(
                "INSERT INTO items (id) VALUES (10)",
                "restart-key",
                SqlClientOptions {
                    query_id: Some(QUERY_ID.parse().unwrap()),
                    timeout: None,
                },
            )
            .unwrap()
    })
    .await
    .unwrap();
    assert_restart_replay(receipt, &requests);
}

#[tokio::test]
async fn async_client_replays_idempotency_key_once_after_restart() {
    let server = MockServer::start().await;
    let requests = mount_restart_replay_mocks(&server).await;
    let receipt = mongreldb_client::AsyncMongrelClient::new(&server.uri())
        .unwrap()
        .sql_write_idempotent_with_options(
            "INSERT INTO items (id) VALUES (10)",
            "restart-key",
            SqlClientOptions {
                query_id: Some(QUERY_ID.parse().unwrap()),
                timeout: None,
            },
        )
        .await
        .unwrap();
    assert_restart_replay(receipt, &requests);
}

#[tokio::test]
async fn retry_can_be_the_first_idempotent_execution_in_both_clients() {
    let server = MockServer::start().await;
    let requests = mount_first_execution_after_missing_mocks(&server).await;
    let uri = server.uri();
    let receipt = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&uri)
            .unwrap()
            .sql_write_idempotent_with_options(
                "INSERT INTO items (id) VALUES (10)",
                "first-on-retry-key",
                SqlClientOptions {
                    query_id: Some(QUERY_ID.parse().unwrap()),
                    timeout: None,
                },
            )
            .unwrap()
    })
    .await
    .unwrap();
    assert!(!receipt.idempotency_replayed);
    let retry_query_id = {
        let requests = requests.lock().unwrap();
        requests[1]["query_id"].as_str().unwrap().to_owned()
    };
    assert_eq!(receipt.original_query_id, retry_query_id);

    let server = MockServer::start().await;
    let requests = mount_first_execution_after_missing_mocks(&server).await;
    let receipt = mongreldb_client::AsyncMongrelClient::new(&server.uri())
        .unwrap()
        .sql_write_idempotent_with_options(
            "INSERT INTO items (id) VALUES (10)",
            "first-on-retry-key",
            SqlClientOptions {
                query_id: Some(QUERY_ID.parse().unwrap()),
                timeout: None,
            },
        )
        .await
        .unwrap();
    assert!(!receipt.idempotency_replayed);
    let retry_query_id = {
        let requests = requests.lock().unwrap();
        requests[1]["query_id"].as_str().unwrap().to_owned()
    };
    assert_eq!(receipt.original_query_id, retry_query_id);
}

#[tokio::test]
async fn automatic_replay_rejects_unrelated_original_query_id() {
    let server = MockServer::start().await;
    mount_wrong_original_replay_mocks(&server).await;
    let uri = server.uri();
    let error = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&uri)
            .unwrap()
            .sql_write_idempotent_with_options(
                "INSERT INTO items (id) VALUES (10)",
                "restart-key",
                SqlClientOptions {
                    query_id: Some(QUERY_ID.parse().unwrap()),
                    timeout: None,
                },
            )
            .unwrap_err()
    })
    .await
    .unwrap();
    assert!(matches!(error, ClientError::QueryOutcomeUnknown { .. }));

    let server = MockServer::start().await;
    mount_wrong_original_replay_mocks(&server).await;
    let error = mongreldb_client::AsyncMongrelClient::new(&server.uri())
        .unwrap()
        .sql_write_idempotent_with_options(
            "INSERT INTO items (id) VALUES (10)",
            "restart-key",
            SqlClientOptions {
                query_id: Some(QUERY_ID.parse().unwrap()),
                timeout: None,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(error, ClientError::QueryOutcomeUnknown { .. }));
}

#[tokio::test]
async fn initial_preexisting_replay_accepts_valid_original_query_id() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
        .expect(1)
        .mount(&server)
        .await;
    let mut response = replay_receipt(QUERY_ID);
    response["original_query_id"] = serde_json::json!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    Mock::given(method("POST"))
        .and(path("/sql"))
        .respond_with(sql_response(200).set_body_json(response))
        .expect(1)
        .mount(&server)
        .await;
    let uri = server.uri();
    let receipt = tokio::task::spawn_blocking(move || {
        MongrelClient::new(&uri)
            .unwrap()
            .sql_write_idempotent_with_options(
                "INSERT INTO items (id) VALUES (10)",
                "preexisting-key",
                SqlClientOptions {
                    query_id: Some(QUERY_ID.parse().unwrap()),
                    timeout: None,
                },
            )
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(
        receipt.original_query_id,
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
}

#[tokio::test]
async fn async_client_sends_projection_and_continues_cursor() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/capabilities"))
        .respond_with(ResponseTemplate::new(200).set_body_json(capabilities()))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/sql"))
        .and(body_json(serde_json::json!({
            "sql": "SELECT id, secret FROM items ORDER BY id",
            "query_id": QUERY_ID,
            "max_output_rows": 10,
            "max_output_bytes": 4096,
            "pagination": {
                "page_size_rows": 1,
                "projection": ["id"],
                "max_page_bytes": 1024,
                "max_page_tokens": 256
            }
        })))
        .respond_with(sql_response(200).set_body_json(serde_json::json!({
            "status": "completed",
            "rows": [{"id": 1}],
            "next_cursor": "cursor-2",
            "page": {
                "offset": 0,
                "row_count": 1,
                "total_rows": 2,
                "byte_count": 10,
                "estimated_tokens": 3,
                "limits": {"rows": 1, "bytes": 1024, "tokens": 256},
                "projection": ["id"],
                "expires_at_ms": 123456,
                "snapshot": "retained_result",
                "token_estimate": "ceil(projected_json_bytes/4)"
            }
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/sql/continue"))
        .and(body_json(serde_json::json!({
            "cursor": "cursor-2",
            "operation_id": QUERY_ID,
            "timeout_ms": null
        })))
        .respond_with(sql_response(200).set_body_json(serde_json::json!({
            "status": "completed",
            "rows": [{"id": 2}],
            "next_cursor": null,
            "page": {
                "offset": 1,
                "row_count": 1,
                "total_rows": 2,
                "byte_count": 10,
                "estimated_tokens": 3,
                "limits": {"rows": 1, "bytes": 1024, "tokens": 256},
                "projection": ["id"],
                "expires_at_ms": 123456,
                "snapshot": "retained_result",
                "token_estimate": "ceil(projected_json_bytes/4)"
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = mongreldb_client::AsyncMongrelClient::new(&server.uri()).unwrap();
    let mut options = SqlPageOptions::new(1, vec!["id".into()]);
    options.query_id = Some(QUERY_ID.parse().unwrap());
    options.max_output_rows = Some(10);
    options.max_output_bytes = Some(4096);
    options.max_page_bytes = Some(1024);
    options.max_page_tokens = Some(256);
    let first = client
        .sql_page("SELECT id, secret FROM items ORDER BY id", options)
        .await
        .unwrap();
    assert_eq!(first.rows, vec![serde_json::json!({"id": 1})]);
    let second = client
        .continue_sql_page(
            first.next_cursor.as_deref().unwrap(),
            RemoteSqlControlOptions {
                query_id: Some(QUERY_ID.parse().unwrap()),
                timeout: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(second.rows, vec![serde_json::json!({"id": 2})]);
    assert!(second.next_cursor.is_none());
}
