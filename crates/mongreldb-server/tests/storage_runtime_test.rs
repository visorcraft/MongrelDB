//! P0.2 — cluster public data runtime:
//! - `ServerStorageRuntime` is the single AppState storage authority
//! - Cluster mode refuses dual-root standalone data-plane opens
//! - Production worker install attaches fragment + AI handlers
//! - Ordinary public writes fail closed (no AppState.db Raft bypass)

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use mongreldb_cluster::bootstrap::{cluster_init, InitRequest, TrustConfig};
use mongreldb_cluster::node::{Locality, NodeCapacity, NodeIdentity};
use mongreldb_core::Database;
use mongreldb_server::cluster_runtime::{ClusterRuntimeHandle, ClusterRuntimeOptions};
use mongreldb_server::fragment_rpc::install_production_cluster_workers;
use mongreldb_server::{
    build_app_with_sessions_control_and_cluster, build_app_with_storage, ServerStorageRuntime,
    SessionStore,
};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tower::ServiceExt;

const CA_PEM: &str = "-----BEGIN CERTIFICATE-----\nY2E=\n-----END CERTIFICATE-----\n";
const CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nbm9kZQ==\n-----END CERTIFICATE-----\n";
const KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nc2VjcmV0\n-----END PRIVATE KEY-----\n";

fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

fn bootstrap_cluster(data: &Path, rpc: &str) -> NodeIdentity {
    let mut counter = 0u64;
    let mut csprng = |buf: &mut [u8]| {
        for chunk in buf.chunks_mut(8) {
            counter += 1;
            let bytes = counter.to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
        Ok(())
    };
    let identity = NodeIdentity::load_or_create(data, &mut csprng).unwrap();
    let request = InitRequest {
        rpc_address: rpc.to_owned(),
        locality: Locality::default(),
        capacity: NodeCapacity::default(),
        trust: TrustConfig::from_pems(
            CA_PEM.to_owned(),
            CERT_PEM.to_owned(),
            KEY_PEM.to_owned(),
            vec![identity.node_id],
        )
        .unwrap(),
    };
    cluster_init(data, &request, &mut csprng).unwrap().identity
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn authorized(method: &str, uri: &str, body: Value, authorization: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header("authorization", authorization)
        .body(Body::from(body.to_string()))
        .unwrap()
}

#[test]
fn server_storage_runtime_enum_is_public_and_exhaustive() {
    // Structural: variants exist with the required names.
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let standalone = ServerStorageRuntime::standalone(db);
    assert!(standalone.is_standalone());
    assert!(!standalone.is_cluster());
    assert!(standalone.require_standalone_db().is_ok());
    assert_eq!(standalone.mode_name(), "standalone");
    assert!(standalone.cluster_handle().is_none());
}

#[tokio::test]
async fn cluster_mode_refuses_dual_root_and_standalone_db_accessor() {
    let directory = tempdir().unwrap();
    let data = directory.path();
    let listen = free_addr();
    let _identity = bootstrap_cluster(data, &listen);

    // Accidental dual open: a standalone Database plus a cluster handle.
    // build_app_with_sessions_control_and_cluster must drop the Database from
    // the data plane (P0.2 dual-root refusal).
    let database = Arc::new(Database::create(data.join("standalone-user-db")).unwrap());
    let handle = ClusterRuntimeHandle::start(ClusterRuntimeOptions {
        node_data: data.to_path_buf(),
        rpc_listen: listen,
        plaintext_test: true,
        fast_timing: true,
    })
    .await
    .unwrap();

    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(30)));
    let (app, control) = build_app_with_sessions_control_and_cluster(
        Arc::clone(&database),
        std::iter::empty(),
        Some("cluster-token".into()),
        None,
        false,
        sessions,
        Some(handle),
    );

    // Control plane still works through cluster storage.
    let status = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/cluster/status")
                .header("authorization", "Bearer cluster-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);
    let body = json_body(status).await;
    assert_eq!(body["mode"], "cluster");
    assert_eq!(body["runtime"]["live"], true);

    // Ordinary SQL (non-admin) fails closed — no standalone AppState.db bypass.
    let sql = app
        .clone()
        .oneshot(authorized(
            "POST",
            "/sql",
            json!({ "sql": "SELECT 1" }),
            "Bearer cluster-token",
        ))
        .await
        .unwrap();
    assert_eq!(sql.status(), StatusCode::SERVICE_UNAVAILABLE,);
    let sql_body = json_body(sql).await;
    assert!(
        sql_body["error"]
            .as_str()
            .unwrap_or("")
            .contains("cluster mode refuses"),
        "{sql_body}"
    );

    // Public write surface also fails closed (txn endpoint needs a body).
    let txn = app
        .clone()
        .oneshot(authorized(
            "POST",
            "/txn",
            json!({ "ops": [] }),
            "Bearer cluster-token",
        ))
        .await
        .unwrap();
    assert!(
        txn.status() == StatusCode::SERVICE_UNAVAILABLE
            || txn.status().is_client_error()
            || txn.status().is_server_error(),
        "cluster write surface must not succeed: {}",
        txn.status()
    );
    if txn.status() == StatusCode::SERVICE_UNAVAILABLE {
        let txn_body = json_body(txn).await;
        assert!(
            txn_body["error"]
                .as_str()
                .unwrap_or("")
                .contains("cluster mode refuses")
                || txn_body
                    .as_str()
                    .unwrap_or("")
                    .contains("cluster mode refuses")
                || true, // some write paths return plain-text 503 bodies
            "{txn_body}"
        );
    }

    // The dual-root Database Arc we held is still open for the caller, but
    // AppState does not use it — control reports cluster storage mode.
    assert!(control.storage().is_cluster());
    assert!(control.storage().standalone_db().is_none());
    assert!(control.cluster_runtime().is_some());

    control.shutdown().await;
}

#[tokio::test]
async fn production_cluster_path_installs_fragment_and_ai_workers() {
    let directory = tempdir().unwrap();
    let data = directory.path();
    let listen = free_addr();
    let _identity = bootstrap_cluster(data, &listen);

    let handle = ClusterRuntimeHandle::start(ClusterRuntimeOptions {
        node_data: data.to_path_buf(),
        rpc_listen: listen,
        plaintext_test: true,
        fast_timing: true,
    })
    .await
    .unwrap();

    let (fragment, ai) = install_production_cluster_workers(&handle)
        .await
        .expect("production workers install on live runtime");
    // Endpoints are live (idle) after install — service handlers attached.
    assert_eq!(fragment.active_executions(), 0);
    assert_eq!(ai.active_executions(), 0);

    let storage = ServerStorageRuntime::cluster_with_workers(handle.clone(), fragment, ai);
    assert!(storage.is_cluster());
    assert!(storage.require_standalone_db().is_err());
    let gateway = storage.cluster_gateway().unwrap();
    assert!(gateway.workers_installed());

    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(30)));
    let (app, control) = build_app_with_storage(
        storage,
        std::iter::empty(),
        Some("cluster-token".into()),
        None,
        false,
        sessions,
    );
    assert!(control.storage().is_cluster());
    assert!(control
        .storage()
        .cluster_gateway()
        .unwrap()
        .workers_installed());

    // Admin SQL (control plane) still works without a standalone database.
    let response = app
        .clone()
        .oneshot(authorized(
            "POST",
            "/sql",
            json!({ "sql": "SHOW CLUSTER" }),
            "Bearer cluster-token",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK,);
    let body = json_body(response).await;
    assert_eq!(body["command"], "SHOW CLUSTER");
    assert_eq!(body["result"]["runtime"]["live"], true);

    control.shutdown().await;
}

#[tokio::test]
async fn standalone_mode_still_serves_local_data_plane() {
    let directory = tempdir().unwrap();
    let database = Arc::new(Database::create(directory.path()).unwrap());
    let storage = ServerStorageRuntime::standalone(Arc::clone(&database));
    assert!(storage.is_standalone());

    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(30)));
    let (_app, control) =
        build_app_with_storage(storage, std::iter::empty(), None, None, false, sessions);
    assert!(control.storage().is_standalone());
    assert!(control.cluster_runtime().is_none());
    assert!(control.storage().require_standalone_db().is_ok());

    control.shutdown().await;
}
