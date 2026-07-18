//! Stage 1G operational endpoints + idempotency unification tests:
//! - /sql idempotency anchored in the core `TXN_IDEMPOTENCY` ledger (S1B-005):
//!   replay across restart returns the same core commit receipt without
//!   duplicate effects; a different fingerprint conflicts.
//! - Session read-your-writes tokens carry real commit-timestamp lineage.
//! - `POST /admin/drain` + `GET /admin/drain` (graceful drain, §10.7).
//! - `POST /admin/reload` (configuration reload, §10.7 / §16.1).
//! - Audit coverage for admin and prepared-statement events, with redaction.

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use mongreldb_core::{ColumnDef, ColumnFlags, Database, Schema, TypeId};
use mongreldb_server::{build_app, build_app_full, build_app_with_sessions, SessionStore};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tower::ServiceExt;

fn request(method: &str, uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn empty_request(method: &str, uri: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

fn authorized_request(method: &str, uri: &str, body: Value, authorization: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header("authorization", authorization)
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn session_request(path: &str, body: Value, session_id: Option<&str>) -> Request<Body> {
    let mut request = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json");
    if let Some(session_id) = session_id {
        request = request.header("x-session-id", session_id);
    }
    request.body(Body::from(body.to_string())).unwrap()
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn items_schema() -> Schema {
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
                name: "value".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
        ],
        ..Schema::default()
    }
}

fn database() -> (tempfile::TempDir, Arc<Database>) {
    let directory = tempdir().unwrap();
    let database = Arc::new(Database::create(directory.path()).unwrap());
    database.create_table("items", items_schema()).unwrap();
    (directory, database)
}

async fn count(app: axum::Router) -> i64 {
    let response = app
        .oneshot(request(
            "POST",
            "/sql",
            json!({
                "sql": "SELECT count(*) AS n FROM items",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    json_body(response).await[0]["n"].as_i64().unwrap()
}

#[tokio::test]
async fn idempotent_replay_across_restart_returns_same_core_receipt_without_duplicate_effects() {
    let (directory, database) = database();
    let app = build_app(Arc::clone(&database));
    let first = app
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({
                "sql": "INSERT INTO items (id, value) VALUES (1, 10)",
                "idempotency_key": "stage1g-key",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.headers()["idempotency-replayed"], "false");
    let first = json_body(first).await;
    assert_eq!(first["status"], "committed");
    // S1B-005: the response additively surfaces the core commit log's receipt.
    let receipt = &first["commit_receipt"];
    assert!(receipt.is_object(), "missing commit_receipt: {first}");
    assert_eq!(receipt["transaction_id"].as_str().unwrap().len(), 32);
    assert!(receipt["commit_ts_physical_micros"].as_u64().unwrap() > 0);
    assert_eq!(receipt["log_term"], 0);
    assert_eq!(receipt["durability"], "group_commit");
    // The ledger record is its own commit, strictly after the write's epoch.
    assert!(
        receipt["log_index"].as_u64().unwrap()
            > first["outcome"]["last_commit_epoch"].as_u64().unwrap(),
        "ledger record must commit after the write: {first}"
    );
    // The durable core ledger file exists (TXN_IDEMPOTENCY).
    assert!(directory.path().join("TXN_IDEMPOTENCY").exists());
    assert_eq!(count(app.clone()).await, 1);

    // "Restart" the app over the same database: replay returns the SAME core
    // receipt, without re-executing the write.
    let restarted = build_app(Arc::clone(&database));
    let replay = restarted
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({
                "sql": " INSERT INTO items (id, value) VALUES (1, 10) -- same request\n",
                "idempotency_key": "stage1g-key",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.headers()["idempotency-replayed"], "true");
    let replay = json_body(replay).await;
    assert_eq!(replay["idempotency_replayed"], true);
    assert_eq!(
        replay["commit_receipt"], first["commit_receipt"],
        "an identical replay must return the original receipt (S1B-005)"
    );
    assert_eq!(replay["outcome"], first["outcome"]);
    assert_eq!(count(restarted).await, 1, "no duplicate effects");
}

#[tokio::test]
async fn idempotency_fingerprint_conflict_is_durable_across_restart() {
    let (_directory, database) = database();
    let app = build_app(Arc::clone(&database));
    let first = app
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({
                "sql": "INSERT INTO items (id) VALUES (1)",
                "idempotency_key": "conflict-key",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    let restarted = build_app(Arc::clone(&database));
    // Same key, different request fingerprint → Conflict (S1B-005), before
    // and after "restart".
    for app in [app.clone(), restarted.clone()] {
        let mismatch = app
            .oneshot(request(
                "POST",
                "/sql",
                json!({
                    "sql": "INSERT INTO items (id) VALUES (2)",
                    "idempotency_key": "conflict-key",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(mismatch.status(), StatusCode::CONFLICT);
        assert_eq!(
            json_body(mismatch).await["error"]["code"],
            "IDEMPOTENCY_KEY_REUSE_MISMATCH"
        );
    }
    // The original request still replays cleanly.
    let replay = restarted
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({
                "sql": "INSERT INTO items (id) VALUES (1)",
                "idempotency_key": "conflict-key",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.headers()["idempotency-replayed"], "true");
    assert_eq!(count(restarted).await, 1);
}

#[tokio::test]
async fn noop_idempotent_write_has_no_commit_receipt() {
    let (_directory, database) = database();
    let app = build_app(database);
    let response = app
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({
                "sql": "UPDATE items SET value = 7 WHERE id = 9",
                "idempotency_key": "noop-key",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body["committed"], false);
    assert!(
        body.get("commit_receipt").is_none(),
        "a write that never committed records no core receipt: {body}"
    );
}

#[tokio::test]
async fn core_ledger_anchors_receipts_across_real_database_reopen() {
    let directory = tempdir().unwrap();
    {
        let database = Arc::new(Database::create(directory.path()).unwrap());
        database.create_table("items", items_schema()).unwrap();
        let app = build_app(Arc::clone(&database));
        let first = app
            .clone()
            .oneshot(request(
                "POST",
                "/sql",
                json!({
                    "sql": "INSERT INTO items (id, value) VALUES (1, 10)",
                    "idempotency_key": "reopen-key",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(count(app).await, 1);
    }
    // Real process-level reopen: WAL, HTTP receipts, and the core
    // TXN_IDEMPOTENCY ledger all reload from disk.
    let database = Arc::new(Database::open(directory.path()).unwrap());
    let app = build_app(database);
    let replay = app
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({
                "sql": "INSERT INTO items (id, value) VALUES (1, 10)",
                "idempotency_key": "reopen-key",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.headers()["idempotency-replayed"], "true");
    let replay = json_body(replay).await;
    assert!(replay["commit_receipt"].is_object());
    assert_eq!(replay["commit_receipt"]["durability"], "group_commit");
    assert_eq!(count(app).await, 1, "no duplicate effects after reopen");
}

#[tokio::test]
async fn session_read_your_writes_token_tracks_commit_timestamps() {
    let (_directory, database) = database();
    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(60)));
    let app = build_app_with_sessions(
        Arc::clone(&database),
        std::iter::empty(),
        None,
        None,
        false,
        Arc::clone(&sessions),
    );
    let opened = app
        .clone()
        .oneshot(session_request("/sessions", Value::Null, None))
        .await
        .unwrap();
    let session_id = json_body(opened).await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let entry = sessions.get(&session_id, "anonymous").unwrap();
    assert_eq!(entry.protocol_record().read_your_writes_token, None);

    // An ordinary (non-idempotent) committed write: the token is the write's
    // literal commit timestamp, sourced from core (the query-layer durable
    // outcome backed by the per-open epoch→commit-ts ledger), not a
    // fresh-begin HLC.
    let write = app
        .clone()
        .oneshot(session_request(
            "/sql",
            json!({ "sql": "INSERT INTO items (id) VALUES (1)" }),
            Some(&session_id),
        ))
        .await
        .unwrap();
    assert_eq!(write.status(), StatusCode::OK);
    let first_token = entry
        .protocol_record()
        .read_your_writes_token
        .expect("a committed session write must set the read-your-writes token");
    assert!(first_token.physical_micros > 0);
    // Literal lineage: the token is exactly the commit timestamp core sealed
    // for the write's epoch.
    let write_epoch = database.visible_epoch();
    assert_eq!(
        database.commit_ts_for_epoch(write_epoch),
        Some(first_token),
        "the token must be the literal commit receipt timestamp of the write"
    );

    // An idempotent committed write on the same session: the token is the
    // core ledger receipt's exact commit timestamp, and it advances.
    let idempotent = app
        .clone()
        .oneshot(session_request(
            "/sql",
            json!({
                "sql": "INSERT INTO items (id) VALUES (2)",
                "idempotency_key": "ryw-session-key",
            }),
            Some(&session_id),
        ))
        .await
        .unwrap();
    assert_eq!(idempotent.status(), StatusCode::OK);
    let body = json_body(idempotent).await;
    let receipt = &body["commit_receipt"];
    assert!(receipt.is_object(), "missing commit_receipt: {body}");
    let second_token = entry
        .protocol_record()
        .read_your_writes_token
        .expect("token must persist");
    assert_eq!(
        second_token.physical_micros,
        receipt["commit_ts_physical_micros"].as_u64().unwrap()
    );
    assert_eq!(
        second_token.logical,
        receipt["commit_ts_logical"].as_u64().unwrap() as u32
    );
    assert!(
        (
            second_token.physical_micros,
            second_token.logical,
            second_token.node_tiebreaker
        ) > (
            first_token.physical_micros,
            first_token.logical,
            first_token.node_tiebreaker
        ),
        "the read-your-writes token must advance with each commit"
    );
}

#[tokio::test]
async fn drain_rejects_new_writes_then_closes() {
    let (_directory, database) = database();
    let app = build_app(database);
    let seed = app
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({
                "sql": "INSERT INTO items (id) VALUES (1)",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(seed.status(), StatusCode::OK);

    // Drain with a generous deadline: an idle database drains immediately.
    let drain = app
        .clone()
        .oneshot(request(
            "POST",
            "/admin/drain",
            json!({ "drain_deadline_ms": 10_000 }),
        ))
        .await
        .unwrap();
    assert_eq!(drain.status(), StatusCode::OK);
    let status = json_body(drain).await;
    assert_eq!(status["lifecycle"], "closed");
    assert_eq!(status["accepting_sql"], false);
    assert_eq!(status["drain"]["initiated"], true);
    assert_eq!(status["drain"]["completed"], true);
    assert_eq!(status["drain"]["deadline_ms"], 10_000);

    // New writes are rejected: the SQL surface 503s...
    let write = app
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({
                "sql": "INSERT INTO items (id) VALUES (2)",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(write.status(), StatusCode::SERVICE_UNAVAILABLE);
    // ...and so do the native and Kit write surfaces.
    let put = app
        .clone()
        .oneshot(request(
            "POST",
            "/tables/items/put",
            json!({ "row": [1, 2, 2, 5] }),
        ))
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::SERVICE_UNAVAILABLE);
    let kit = app
        .clone()
        .oneshot(request("POST", "/kit/txn", json!({ "ops": [] })))
        .await
        .unwrap();
    assert_eq!(kit.status(), StatusCode::SERVICE_UNAVAILABLE);
    // Session creation is closed too.
    let session = app
        .clone()
        .oneshot(session_request("/sessions", Value::Null, None))
        .await
        .unwrap();
    assert_eq!(session.status(), StatusCode::SERVICE_UNAVAILABLE);

    // GET reports the terminal lifecycle; a repeated POST is idempotent.
    let get = app
        .clone()
        .oneshot(empty_request("GET", "/admin/drain"))
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    let get = json_body(get).await;
    assert_eq!(get["lifecycle"], "closed");
    assert_eq!(get["drain"]["completed"], true);
    let again = app
        .clone()
        .oneshot(request("POST", "/admin/drain", json!({})))
        .await
        .unwrap();
    assert_eq!(again.status(), StatusCode::OK);

    // The drain is audited with principal + outcome.
    let audit = app.oneshot(empty_request("GET", "/audit")).await.unwrap();
    assert_eq!(audit.status(), StatusCode::OK);
    let audit = json_body(audit).await;
    let events: Vec<&Value> = audit
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| {
            event["action"]
                .as_str()
                .is_some_and(|a| a.starts_with("admin.drain"))
        })
        .collect();
    assert!(
        events.iter().any(|event| event["action"] == "admin.drain"),
        "{audit}"
    );
    assert!(
        events
            .iter()
            .any(|event| event["action"] == "admin.drain.ok"),
        "{audit}"
    );
    assert!(events.iter().all(|event| event["principal"].is_string()));
}

#[tokio::test]
async fn reload_applies_mutable_config() {
    let (_directory, database) = database();
    let app = build_app(database);
    for id in 0..10 {
        let insert = app
            .clone()
            .oneshot(request(
                "POST",
                "/sql",
                json!({
                    "sql": format!("INSERT INTO items (id) VALUES ({id})"),
                }),
            ))
            .await
            .unwrap();
        assert_eq!(insert.status(), StatusCode::OK);
    }

    // Baseline: an explicit 10-row cap is honored against the default ceiling.
    let baseline = app
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({
                "sql": "SELECT id FROM items",
                "max_output_rows": 10,
            }),
        ))
        .await
        .unwrap();
    assert_eq!(baseline.status(), StatusCode::OK);

    // Reload with explicit overrides: the output ceiling drops to 5.
    let reload = app
        .clone()
        .oneshot(request(
            "POST",
            "/admin/reload",
            json!({
                "sql_max_output_rows": 5,
                "slow_query_ms": 250,
                "history_retention_epochs": 7,
            }),
        ))
        .await
        .unwrap();
    assert_eq!(reload.status(), StatusCode::OK);
    let applied = json_body(reload).await;
    assert_eq!(applied["reloaded"], true);
    assert_eq!(applied["applied"]["sql_max_output_rows"], 5);
    assert_eq!(applied["applied"]["slow_query_ms"], 250);
    assert_eq!(applied["applied"]["history_retention_epochs"], 7);

    // The new ceiling is live: the same request now exceeds the limit.
    let limited = app
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({
                "sql": "SELECT id FROM items",
                "max_output_rows": 10,
            }),
        ))
        .await
        .unwrap();
    assert_eq!(
        limited.status(),
        StatusCode::PAYLOAD_TOO_LARGE,
        "reloaded output ceiling must apply to new requests"
    );

    // Zero overrides are rejected (limits must be positive).
    let invalid = app
        .clone()
        .oneshot(request(
            "POST",
            "/admin/reload",
            json!({ "sql_max_output_rows": 0 }),
        ))
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);

    // The reload is audited with principal + outcome (field names only).
    let audit = app.oneshot(empty_request("GET", "/audit")).await.unwrap();
    let audit = json_body(audit).await;
    assert!(audit.as_array().unwrap().iter().any(|event| {
        event["action"] == "admin.reload.ok"
            && event["detail"]
                .as_str()
                .unwrap()
                .contains("sql_max_output_rows")
    }));
}

#[tokio::test]
async fn admin_endpoints_require_admin_and_never_log_secrets() {
    let directory = tempdir().unwrap();
    let database =
        Arc::new(Database::create_with_credentials(directory.path(), "admin", "admin-pw").unwrap());
    database.create_table("items", items_schema()).unwrap();
    database.create_user("alice", "alice-pw").unwrap();
    let app = build_app_full(database, std::iter::empty(), None, None, true);

    // Unauthenticated and non-admin callers are rejected before anything runs.
    let anonymous = app
        .clone()
        .oneshot(request("POST", "/admin/reload", json!({})))
        .await
        .unwrap();
    assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);
    let non_admin = app
        .clone()
        .oneshot(authorized_request(
            "POST",
            "/admin/reload",
            json!({}),
            "Basic YWxpY2U6YWxpY2UtcHc=",
        ))
        .await
        .unwrap();
    assert_eq!(non_admin.status(), StatusCode::FORBIDDEN);
    let non_admin_drain = app
        .clone()
        .oneshot(authorized_request(
            "POST",
            "/admin/drain",
            json!({}),
            "Basic YWxpY2U6YWxpY2UtcHc=",
        ))
        .await
        .unwrap();
    assert_eq!(non_admin_drain.status(), StatusCode::FORBIDDEN);

    // The authorization failures are audited — without any credential material.
    let audit = app
        .oneshot(authorized_request(
            "GET",
            "/audit",
            Value::Null,
            "Basic YWRtaW46YWRtaW4tcHc=",
        ))
        .await
        .unwrap();
    assert_eq!(audit.status(), StatusCode::OK);
    let audit = json_body(audit).await;
    let events = audit.as_array().unwrap();
    assert!(events
        .iter()
        .any(|event| event["action"] == "admin.reload.fail"));
    assert!(events
        .iter()
        .any(|event| event["action"] == "admin.drain.fail"));
    let serialized = audit.to_string();
    assert!(
        !serialized.contains("alice-pw"),
        "passwords must never reach the audit log"
    );
    assert!(
        !serialized.contains("admin-pw"),
        "passwords must never reach the audit log"
    );
}

#[tokio::test]
async fn prepared_invalidate_and_replan_are_audited_without_sql() {
    let (_directory, database) = database();
    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(60)));
    let app = build_app_with_sessions(
        database,
        std::iter::empty(),
        None,
        None,
        false,
        Arc::clone(&sessions),
    );
    let secret_literal = "s3cret-pw-literal";
    let opened = app
        .clone()
        .oneshot(session_request("/sessions", Value::Null, None))
        .await
        .unwrap();
    let session_id = json_body(opened).await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let prepare = app
        .clone()
        .oneshot(session_request(
            &format!("/sessions/{session_id}/prepare"),
            json!({
                "name": "p",
                "sql": format!("SELECT id FROM items WHERE value = 7 OR id = -1 AND '{secret_literal}' = '{secret_literal}'"),
            }),
            Some(&session_id),
        ))
        .await
        .unwrap();
    assert_eq!(prepare.status(), StatusCode::OK);

    // A cross-session schema change invalidates the binding.
    let alter = app
        .clone()
        .oneshot(request(
            "POST",
            "/sql",
            json!({
                "sql": "ALTER TABLE items ADD COLUMN extra BIGINT NULL",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(alter.status(), StatusCode::OK);

    // Execute: the stale plan is invalidated and replanned, then runs.
    let execute = app
        .clone()
        .oneshot(session_request(
            &format!("/sessions/{session_id}/execute"),
            json!({ "name": "p", "params": [] }),
            Some(&session_id),
        ))
        .await
        .unwrap();
    assert_eq!(execute.status(), StatusCode::OK);

    let audit = app.oneshot(empty_request("GET", "/audit")).await.unwrap();
    let audit = json_body(audit).await;
    let events = audit.as_array().unwrap();
    assert!(
        events
            .iter()
            .any(|event| event["action"] == "prepared.invalidate"),
        "{audit}"
    );
    assert!(
        events
            .iter()
            .any(|event| event["action"] == "prepared.replan.ok"),
        "{audit}"
    );
    // Statement SQL and its literals never reach the audit log.
    let serialized = audit.to_string();
    assert!(
        !serialized.contains(secret_literal),
        "prepared-statement SQL must never reach the audit log"
    );
    assert!(!serialized.contains("SELECT id FROM items"));
}
