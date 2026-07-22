//! Server-hosted NodeRuntime product path (Stage 2/3):
//! - After `cluster init`, start the app with a live runtime under plaintext
//!   test mode and assert status / SHOW CLUSTER report a live runtime.
//! - TRANSFER LEADER / SPLIT TABLET without a hosted tablet return structured
//!   errors (not silent `"accepted"`).
//! - Standalone mode still fails closed with `"cluster runtime not running"`.
//! - TRANSFER LEADER against a real single-replica tablet succeeds.

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use mongreldb_cluster::bootstrap::{cluster_init, InitRequest, TrustConfig};
use mongreldb_cluster::meta::{DatabaseDescriptor, DatabaseState, MetaCommand, TableSchemaRecord};
use mongreldb_cluster::node::{Locality, NodeCapacity, NodeIdentity};
use mongreldb_cluster::runtime::NodeRuntime;
use mongreldb_cluster::tablet::{
    ColumnId, ReplicaDescriptor, ReplicaRole, TablePartitioningRecord, TabletDescriptor,
    TabletState,
};
use mongreldb_core::Database;
use mongreldb_log::commit_log::ExecutionControl;
use mongreldb_server::cluster_runtime::{ClusterRuntimeHandle, ClusterRuntimeOptions};
use mongreldb_server::{
    build_app, build_app_full, build_app_with_storage, ServerStorageRuntime,
};
use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::{
    DatabaseId, MetadataVersion, NodeId, RaftGroupId, SchemaVersion, TableId, TabletId,
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
const LEADER_TIMEOUT: Duration = Duration::from_secs(15);

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

fn authorized_request(method: &str, uri: &str, body: Value, authorization: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header("authorization", authorization)
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn empty_authorized(method: &str, uri: &str, authorization: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", authorization)
        .body(Body::empty())
        .unwrap()
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn sql_json(app: &axum::Router, admin: &str, sql: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(authorized_request(
            "POST",
            "/sql",
            json!({ "sql": sql }),
            admin,
        ))
        .await
        .unwrap();
    let status = response.status();
    (status, json_body(response).await)
}

fn command_id(seq: u8) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[0] = seq;
    id
}

#[tokio::test]
async fn standalone_transfer_and_split_fail_closed() {
    let directory = tempdir().unwrap();
    let database =
        Arc::new(Database::create_with_credentials(directory.path(), "admin", "admin-pw").unwrap());
    let app = build_app_full(Arc::clone(&database), std::iter::empty(), None, None, true);
    let admin = "Basic YWRtaW46YWRtaW4tcHc=";
    let tablet = TabletId::from_bytes([0x11; 16]);
    let node = NodeId::from_bytes([0x22; 16]);

    let (status, body) =
        sql_json(&app, admin, &format!("TRANSFER LEADER {tablet} TO {node}")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["status"], "error");
    assert_eq!(body["error"], "cluster runtime not running");
    assert_ne!(body["status"], "accepted");

    let (status, body) = sql_json(&app, admin, &format!("SPLIT TABLET {tablet}")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["error"], "cluster runtime not running");

    let (status, body) = sql_json(&app, admin, &format!("MERGE TABLETS {tablet} {tablet}")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["error"], "cluster runtime not running");

    let (status, body) = sql_json(
        &app,
        admin,
        &format!("MOVE REPLICA {tablet} FROM {node} TO {node}"),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["error"], "cluster runtime not running");
}

#[tokio::test]
async fn live_runtime_status_and_missing_tablet_ops() {
    let directory = tempdir().unwrap();
    let data = directory.path();
    let listen = free_addr();
    let identity = bootstrap_cluster(data, &listen);

    let handle = ClusterRuntimeHandle::start(ClusterRuntimeOptions {
        node_data: data.to_path_buf(),
        rpc_listen: listen.clone(),
        plaintext_test: true,
        fast_timing: true,
    })
    .await
    .expect("plaintext runtime starts after cluster init");

    // P0.2: cluster AppState has no peer standalone user database. Use bearer
    // token admin for the control plane (no catalog user-auth dual-root).
    let sessions = Arc::new(mongreldb_server::SessionStore::new(
        32,
        Duration::from_secs(60),
    ));
    let (app, control) = build_app_with_storage(
        ServerStorageRuntime::cluster(handle),
        std::iter::empty(),
        Some("cluster-token".into()),
        None,
        false,
        sessions,
    );
    let admin = "Bearer cluster-token";
    assert!(control.storage().is_cluster());
    assert!(control.storage().standalone_db().is_none());

    let status_response = app
        .clone()
        .oneshot(empty_authorized("GET", "/admin/cluster/status", admin))
        .await
        .unwrap();
    assert_eq!(status_response.status(), StatusCode::OK);
    let status = json_body(status_response).await;
    assert_eq!(status["mode"], "cluster");
    assert_eq!(status["runtime"]["live"], true);
    assert_eq!(status["runtime"]["node_id"], identity.node_id.to_string());
    assert_eq!(status["runtime"]["rpc_address"], listen);
    assert_eq!(status["runtime"]["meta_present"], true);
    assert_eq!(status["runtime"]["tablet_count"], 0);

    let (code, cluster) = sql_json(&app, admin, "SHOW CLUSTER").await;
    assert_eq!(code, StatusCode::OK, "{cluster}");
    assert_eq!(cluster["result"]["runtime"]["live"], true);
    assert_eq!(
        cluster["result"]["runtime"]["node_id"],
        identity.node_id.to_string()
    );

    let missing = TabletId::from_bytes([0xCD; 16]);
    let (code, body) = sql_json(
        &app,
        admin,
        &format!("TRANSFER LEADER {missing} TO {}", identity.node_id),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["status"], "error");
    assert!(
        body["error"]
            .as_str()
            .unwrap_or("")
            .contains("hosts no live tablet"),
        "{body}"
    );
    assert_ne!(body["status"], "accepted");

    let (code, body) = sql_json(&app, admin, &format!("SPLIT TABLET {missing}")).await;
    assert_eq!(code, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["status"], "error");
    assert!(
        body["error"]
            .as_str()
            .unwrap_or("")
            .contains("hosts no live tablet"),
        "{body}"
    );

    let (code, body) = sql_json(
        &app,
        admin,
        &format!(
            "MOVE REPLICA {missing} FROM {} TO {}",
            identity.node_id, identity.node_id
        ),
    )
    .await;
    assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert!(
        body["error"]
            .as_str()
            .unwrap_or("")
            .contains("not yet live"),
        "{body}"
    );

    control.shutdown().await;
}

#[tokio::test]
async fn transfer_leader_on_live_single_replica_tablet() {
    let directory = tempdir().unwrap();
    let data = directory.path();
    let listen = free_addr();
    let identity = bootstrap_cluster(data, &listen);

    let handle = ClusterRuntimeHandle::start(ClusterRuntimeOptions {
        node_data: data.to_path_buf(),
        rpc_listen: listen,
        plaintext_test: true,
        fast_timing: true,
    })
    .await
    .unwrap();

    let table_id = TableId::new(1);
    let database_id = DatabaseId::from_bytes([0x42; 16]);
    let tablet_id = seed_single_replica_tablet(&handle, &identity, table_id, database_id).await;

    let sessions = Arc::new(mongreldb_server::SessionStore::new(
        32,
        Duration::from_secs(60),
    ));
    let (app, control) = build_app_with_storage(
        ServerStorageRuntime::cluster(handle),
        std::iter::empty(),
        Some("cluster-token".into()),
        None,
        false,
        sessions,
    );
    let admin = "Bearer cluster-token";

    // Transfer to self is a documented no-op success on a single-voter group.
    let (code, body) = sql_json(
        &app,
        admin,
        &format!("TRANSFER LEADER {tablet_id} TO {}", identity.node_id),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "ok");
    assert_eq!(body["command"], "TRANSFER LEADER");

    control.shutdown().await;
}

/// Create + bootstrap one single-replica tablet on the live runtime.
async fn seed_single_replica_tablet(
    handle: &ClusterRuntimeHandle,
    identity: &NodeIdentity,
    table_id: TableId,
    database_id: DatabaseId,
) -> TabletId {
    let mutex = handle.runtime_mutex();
    let mut guard = mutex.lock().await;
    let runtime: &mut NodeRuntime = guard.as_mut().expect("runtime live");
    let meta = runtime.meta_group().expect("meta present");
    meta.group()
        .wait_leader(LEADER_TIMEOUT)
        .await
        .expect("meta leader");
    let control = ExecutionControl::default();

    meta.propose(
        command_id(1),
        MetaCommand::CreateDatabase {
            descriptor: DatabaseDescriptor {
                database_id,
                name: "app".into(),
                created_at: HlcTimestamp::ZERO,
                state: DatabaseState::Online,
                metadata_version: MetadataVersion::ZERO,
            },
        },
        &control,
    )
    .await
    .unwrap();
    meta.propose(
        command_id(2),
        MetaCommand::SetTableSchema {
            record: TableSchemaRecord {
                table_id,
                database_id,
                schema_version: SchemaVersion::new(1),
                schema: serde_json::json!({"columns": [{"name": "pk", "type": "u64"}]}),
                metadata_version: MetadataVersion::ZERO,
            },
        },
        &control,
    )
    .await
    .unwrap();

    let raft_ids = meta.allocate_raft_node_ids(1, &control).await.unwrap();
    let tablet_id = TabletId::from_bytes([0x77; 16]);
    let raft_group_id = RaftGroupId::from_bytes([0x88; 16]);
    let descriptor = TabletDescriptor {
        tablet_id,
        table_id,
        database_id,
        raft_group_id,
        partition: mongreldb_cluster::tablet::PartitionBounds::unbounded(),
        replicas: vec![ReplicaDescriptor {
            node_id: identity.node_id,
            role: ReplicaRole::Voter,
            raft_node_id: raft_ids[0],
        }],
        leader_hint: Some(identity.node_id),
        generation: 1,
        state: TabletState::Active,
    };
    let address = runtime.rpc_address().to_owned();
    let peers = [(identity.node_id, address)];
    let partitioning =
        TablePartitioningRecord::automatic_default(table_id, vec![ColumnId::new(1)], 16);
    runtime
        .create_tablet(&descriptor, &partitioning, Some(&peers), true, &control)
        .await
        .expect("create single-replica tablet");
    runtime
        .tablet_group(tablet_id)
        .expect("tablet opened")
        .wait_leader(LEADER_TIMEOUT)
        .await
        .expect("tablet leader");
    tablet_id
}

#[tokio::test]
async fn standalone_build_app_still_healthy() {
    let directory = tempdir().unwrap();
    let database = Arc::new(Database::create(directory.path()).unwrap());
    let app = build_app(database);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
