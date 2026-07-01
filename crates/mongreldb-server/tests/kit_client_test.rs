//! Client ↔ server typed integration over a real TCP listener.

use std::sync::Arc;
use std::thread;

use mongreldb_client::{ClientError, KitErrorCode, KitOp, KitTxnRequest, MongrelClient};
use mongreldb_core::constraint::{CheckConstraint, CheckExpr, TableConstraints, UniqueConstraint};
use mongreldb_core::schema::*;
use mongreldb_core::{Database, Value};
use mongreldb_server::build_app;
use tempfile::TempDir;

fn col(id: u16, name: &str, ty: TypeId, flags: ColumnFlags) -> ColumnDef {
    ColumnDef { id, name: name.into(), ty, flags }
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
            col(0, "id", TypeId::Int64, ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY).with(ColumnFlags::AUTO_INCREMENT)),
            col(1, "email", TypeId::Bytes, ColumnFlags::empty().with(ColumnFlags::NULLABLE)),
            col(2, "age", TypeId::Int64, ColumnFlags::empty().with(ColumnFlags::NULLABLE)),
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: cons,
    }
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
        let app = build_app(Arc::new(db));

        // Bind an ephemeral port, then serve in a background thread with its
        // own multi-thread runtime.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let listener = rt.block_on(async {
            tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap()
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
fn client_legacy_endpoints_still_checked() {
    let srv = Server::start();
    let c = srv.client();
    // count on a missing table → typed HTTP error (404 path, plain Http).
    let err = c.count("nope").unwrap_err();
    assert!(matches!(err, ClientError::Http { .. }), "got {err:?}");
}
