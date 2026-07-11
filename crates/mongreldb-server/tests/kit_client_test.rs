//! Client ↔ server typed integration over a real TCP listener.

use std::sync::Arc;
use std::thread;

use mongreldb_client::{ClientError, KitErrorCode, KitOp, KitTxnRequest, MongrelClient};
use mongreldb_core::constraint::{CheckConstraint, CheckExpr, TableConstraints, UniqueConstraint};
use mongreldb_core::schema::*;
use mongreldb_core::{
    Database, StoredTrigger, TriggerCell, TriggerDefinition, TriggerEvent, TriggerProgram,
    TriggerRaiseAction, TriggerStep, TriggerTarget, TriggerTiming, TriggerValue, Value,
};
use mongreldb_server::{build_app, build_app_full};
use tempfile::TempDir;

fn col(id: u16, name: &str, ty: TypeId, flags: ColumnFlags) -> ColumnDef {
    ColumnDef {
        id,
        name: name.into(),
        ty,
        flags,
        default_value: None,
    }
}

fn mk_schema() -> Schema {
    let mut cons = TableConstraints::default();
    cons.uniques.push(UniqueConstraint {
        id: 1,
        name: "email_unique".into(),
        columns: vec![1],
    });
    cons.checks.push(CheckConstraint {
        id: 2,
        name: "age_nonneg".into(),
        expr: CheckExpr::Or(
            Box::new(CheckExpr::IsNull(2)),
            Box::new(CheckExpr::Ge(
                Box::new(CheckExpr::Col(2)),
                Box::new(CheckExpr::Lit(Value::Int64(0))),
            )),
        ),
    });
    Schema {
        schema_id: 0,
        columns: vec![
            col(
                0,
                "id",
                TypeId::Int64,
                ColumnFlags::empty()
                    .with(ColumnFlags::PRIMARY_KEY)
                    .with(ColumnFlags::AUTO_INCREMENT),
            ),
            col(
                1,
                "email",
                TypeId::Bytes,
                ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            ),
            col(
                2,
                "age",
                TypeId::Int64,
                ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            ),
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: cons,
        clustered: false,
    }
}

fn audit_schema() -> Schema {
    Schema {
        schema_id: 0,
        columns: vec![
            col(
                0,
                "id",
                TypeId::Int64,
                ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            ),
            col(1, "user_id", TypeId::Int64, ColumnFlags::empty()),
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn audit_trigger(name: &str) -> StoredTrigger {
    StoredTrigger::new(
        name,
        TriggerDefinition {
            target: TriggerTarget::Table("users".into()),
            timing: TriggerTiming::After,
            event: TriggerEvent::Insert,
            update_of: Vec::new(),
            target_columns: Vec::new(),
            when: None,
            program: TriggerProgram {
                steps: vec![TriggerStep::Insert {
                    table: "audit".into(),
                    cells: vec![
                        TriggerCell {
                            column_id: 0,
                            value: TriggerValue::NewColumn(0),
                        },
                        TriggerCell {
                            column_id: 1,
                            value: TriggerValue::NewColumn(0),
                        },
                    ],
                }],
            },
        },
        0,
    )
    .unwrap()
}

fn aborting_trigger(name: &str) -> StoredTrigger {
    StoredTrigger::new(
        name,
        TriggerDefinition {
            target: TriggerTarget::Table("users".into()),
            timing: TriggerTiming::After,
            event: TriggerEvent::Insert,
            update_of: Vec::new(),
            target_columns: Vec::new(),
            when: None,
            program: TriggerProgram {
                steps: vec![TriggerStep::Raise {
                    action: TriggerRaiseAction::Abort,
                    message: TriggerValue::Literal(Value::Bytes(b"blocked by trigger".to_vec())),
                }],
            },
        },
        0,
    )
    .unwrap()
}

/// Boot a real `mongreldb-server` on an ephemeral port in a background thread
/// and return a connected typed client.
struct Server {
    _dir: TempDir,
    base_url: String,
    _join: thread::JoinHandle<()>,
}

impl Server {
    fn start() -> Self {
        let dir = TempDir::new().unwrap();
        let dir_path = dir.path().to_path_buf();
        let db = Database::create(&dir_path).unwrap();
        db.create_table("users", mk_schema()).unwrap();
        db.create_table("audit", audit_schema()).unwrap();
        let app = build_app(Arc::new(db));

        // Bind an ephemeral port, then serve in a background thread with its
        // own multi-thread runtime.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let listener = rt.block_on(async {
            tokio::net::TcpListener::bind(("127.0.0.1", 0))
                .await
                .unwrap()
        });
        let addr = listener.local_addr().unwrap();
        let join = thread::spawn(move || {
            rt.block_on(async {
                axum::serve(listener, app).await.unwrap();
            });
        });
        Server {
            _dir: dir,
            base_url: format!("http://{addr}"),
            _join: join,
        }
    }

    fn client(&self) -> MongrelClient {
        MongrelClient::new(&self.base_url)
    }

    fn start_with_auth() -> Self {
        let dir = TempDir::new().unwrap();
        let dir_path = dir.path().to_path_buf();
        let db = Database::create(&dir_path).unwrap();
        db.create_table("users", mk_schema()).unwrap();
        db.create_user("alice", "pw").unwrap();
        let app = build_app_full(Arc::new(db), std::iter::empty(), None, None, true);

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let listener = rt.block_on(async {
            tokio::net::TcpListener::bind(("127.0.0.1", 0))
                .await
                .unwrap()
        });
        let addr = listener.local_addr().unwrap();
        let join = thread::spawn(move || {
            rt.block_on(async {
                axum::serve(listener, app).await.unwrap();
            });
        });
        Server {
            _dir: dir,
            base_url: format!("http://{addr}"),
            _join: join,
        }
    }
}

#[test]
fn client_health_and_schema() {
    let srv = Server::start();
    let c = srv.client();
    c.health().unwrap();
    let schema = c.kit_schema("users").unwrap();
    assert_eq!(schema.columns.len(), 3);
    assert_eq!(schema.constraints.uniques[0]["name"], "email_unique");
    assert!(schema.columns[0].auto_increment);
}

#[test]
fn client_kit_txn_commit_then_check_violation() {
    let srv = Server::start();
    let c = srv.client();

    let resp = c
        .kit_txn(&KitTxnRequest::new(vec![KitOp::put_returning(
            "users",
            vec![
                serde_json::json!(1),
                serde_json::json!("a@x"),
                serde_json::json!(2),
                serde_json::json!(30),
            ],
        )]))
        .unwrap();
    assert_eq!(resp.status, "committed");
    assert!(resp.epoch > 0);

    let err = c
        .kit_txn(&KitTxnRequest::new(vec![KitOp::put(
            "users",
            vec![
                serde_json::json!(1),
                serde_json::json!("b@x"),
                serde_json::json!(2),
                serde_json::json!(-9),
            ],
        )]))
        .unwrap_err();
    match err {
        ClientError::Kit { code, .. } => assert_eq!(code, KitErrorCode::CheckViolation),
        other => panic!("expected Kit error, got {other:?}"),
    }
}

#[test]
fn client_kit_txn_unique_violation_typed() {
    let srv = Server::start();
    let c = srv.client();
    c.kit_txn(&KitTxnRequest::new(vec![KitOp::put(
        "users",
        vec![
            serde_json::json!(1),
            serde_json::json!("dup@x"),
            serde_json::json!(2),
            serde_json::json!(1),
        ],
    )]))
    .unwrap();

    let err = c
        .kit_txn(&KitTxnRequest::new(vec![KitOp::put(
            "users",
            vec![
                serde_json::json!(0),
                serde_json::json!(2),
                serde_json::json!(1),
                serde_json::json!("dup@x"),
                serde_json::json!(2),
                serde_json::json!(2),
            ],
        )]))
        .unwrap_err();
    match err {
        ClientError::Kit { code, .. } => assert_eq!(code, KitErrorCode::UniqueViolation),
        other => panic!("expected Kit error, got {other:?}"),
    }
}

#[test]
fn client_kit_txn_trigger_failure_is_typed() {
    let srv = Server::start();
    let c = srv.client();
    c.create_trigger(aborting_trigger("users_block")).unwrap();

    let err = c
        .kit_txn(&KitTxnRequest::new(vec![KitOp::put(
            "users",
            vec![
                serde_json::json!(1),
                serde_json::json!("blocked@x"),
                serde_json::json!(2),
                serde_json::json!(42),
            ],
        )]))
        .unwrap_err();
    match err {
        ClientError::Kit { code, message, .. } => {
            assert_eq!(code, KitErrorCode::TriggerValidation);
            assert!(message.contains("blocked by trigger"), "{message}");
        }
        other => panic!("expected trigger validation Kit error, got {other:?}"),
    }
}

#[test]
fn client_kit_txn_idempotency() {
    let srv = Server::start();
    let c = srv.client();
    let req = KitTxnRequest::new(vec![KitOp::put(
        "users",
        vec![
            serde_json::json!(1),
            serde_json::json!("a@x"),
            serde_json::json!(2),
            serde_json::json!(1),
        ],
    )])
    .with_idempotency_key("k-xyz");
    let r1 = c.kit_txn(&req).unwrap();
    let r2 = c.kit_txn(&req).unwrap();
    assert_eq!(r1.epoch, r2.epoch, "idempotent replay returns cached epoch");
}

#[test]
fn client_trigger_catalog_crud_and_execution() {
    let srv = Server::start();
    let c = srv.client();

    let created = c.create_trigger(audit_trigger("users_ai")).unwrap();
    assert_eq!(created.name, "users_ai");
    assert_eq!(created.version, 1);

    let listed = c.triggers().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "users_ai");

    let fetched = c.trigger("users_ai").unwrap();
    assert_eq!(fetched.name, "users_ai");

    let replaced = c
        .replace_trigger("users_ai", audit_trigger("ignored_by_route"))
        .unwrap();
    assert_eq!(replaced.name, "users_ai");
    assert_eq!(replaced.version, 2);

    c.txn(vec![mongreldb_client::TxnOp {
        table: "users".into(),
        op: "put".into(),
        cells: Some(vec![
            serde_json::json!(0),
            serde_json::json!(42),
            serde_json::json!(1),
            serde_json::json!("trigger@x"),
            serde_json::json!(2),
            serde_json::json!(5),
        ]),
        row_id: None,
    }])
    .unwrap();
    assert_eq!(c.count("audit").unwrap(), 1);

    c.drop_trigger("users_ai").unwrap();
    assert!(c.triggers().unwrap().is_empty());

    let err = c.trigger("users_ai").unwrap_err();
    match err {
        ClientError::Kit { code, .. } => assert_eq!(code, KitErrorCode::TriggerNotFound),
        other => panic!("expected trigger not found Kit error, got {other:?}"),
    }
}

#[test]
fn client_trigger_ddl_idempotency() {
    let srv = Server::start();
    let c = srv.client();

    let created = c
        .create_trigger_with_idempotency_key(audit_trigger("users_ai"), Some("trigger-create-k"))
        .unwrap();
    let replayed = c
        .create_trigger_with_idempotency_key(audit_trigger("users_ai"), Some("trigger-create-k"))
        .unwrap();
    assert_eq!(created.version, replayed.version);

    let replaced = c
        .replace_trigger_with_idempotency_key(
            "users_ai",
            audit_trigger("ignored_by_route"),
            Some("trigger-replace-k"),
        )
        .unwrap();
    let replayed = c
        .replace_trigger_with_idempotency_key(
            "users_ai",
            audit_trigger("ignored_by_route"),
            Some("trigger-replace-k"),
        )
        .unwrap();
    assert_eq!(replaced.version, replayed.version);
    assert_eq!(replaced.version, 2);

    c.drop_trigger_with_idempotency_key("users_ai", Some("trigger-drop-k"))
        .unwrap();
    c.drop_trigger_with_idempotency_key("users_ai", Some("trigger-drop-k"))
        .unwrap();
}

#[test]
fn client_legacy_endpoints_still_checked() {
    let srv = Server::start();
    let c = srv.client();
    // count on a missing table → typed HTTP error (404 path, plain Http).
    let err = c.count("nope").unwrap_err();
    assert!(matches!(err, ClientError::Http { .. }), "got {err:?}");
}

#[test]
fn client_history_retention_round_trips() {
    let srv = Server::start();
    let c = srv.client();

    let initial = c.history_retention_epochs().unwrap();

    let resp = c.set_history_retention_epochs(7).unwrap();
    assert_eq!(resp.history_retention_epochs, 7);
    assert_eq!(c.history_retention_epochs().unwrap(), 7);

    // Earliest retained epoch is stable once history has advanced; it should not
    // move backward when retention is expanded again.
    let earliest_after_shrink = c.earliest_retained_epoch().unwrap();

    let resp = c.set_history_retention_epochs(100).unwrap();
    assert_eq!(resp.history_retention_epochs, 100);
    assert_eq!(c.history_retention_epochs().unwrap(), 100);
    assert_eq!(
        c.earliest_retained_epoch().unwrap(),
        earliest_after_shrink,
        "earliest retained epoch must not move backward"
    );

    // Setting it back to the initial value restores the original retention window.
    c.set_history_retention_epochs(initial).unwrap();
    assert_eq!(c.history_retention_epochs().unwrap(), initial);
}

#[test]
fn client_history_retention_propagates_http_errors() {
    let srv = Server::start_with_auth();
    let c = srv.client();

    let err = c.history_retention_epochs().unwrap_err();
    match err {
        ClientError::Http { status, .. } => assert_eq!(status, 401),
        other => panic!("expected HTTP 401, got {other:?}"),
    }

    let err = c.set_history_retention_epochs(7).unwrap_err();
    match err {
        ClientError::Http { status, .. } => assert_eq!(status, 401),
        other => panic!("expected HTTP 401, got {other:?}"),
    }
}
