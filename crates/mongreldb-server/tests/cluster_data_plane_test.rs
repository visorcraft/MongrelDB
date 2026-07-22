//! P0.2 — public cluster data plane routes INSERT/SELECT through Raft
//! (`write_tablet_rows` / `tablet_rows`), never through standalone AppState.db.

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
use mongreldb_log::commit_log::ExecutionControl;
use mongreldb_server::cluster_runtime::{ClusterRuntimeHandle, ClusterRuntimeOptions};
use mongreldb_server::{build_app_with_storage, ServerStorageRuntime, SessionStore};
use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::{
    DatabaseId, MetadataVersion, RaftGroupId, SchemaVersion, TableId, TabletId,
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

fn command_id(seq: u8) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[0] = seq;
    id
}

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
                schema: serde_json::json!({"columns": [{"name": "id", "type": "u64"}, {"name": "name", "type": "string"}]}),
                metadata_version: MetadataVersion::ZERO,
            },
        },
        &control,
    )
    .await
    .unwrap();

    let raft_ids = meta.allocate_raft_node_ids(1, &control).await.unwrap();
    let tablet_id = TabletId::from_bytes([0xA1; 16]);
    let raft_group_id = RaftGroupId::from_bytes([0xA2; 16]);
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

fn authorized(method: &str, uri: &str, body: Value, authorization: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header("authorization", authorization)
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn sql_json(
    app: &axum::Router,
    token: &str,
    sql: &str,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(authorized(
            "POST",
            "/sql",
            json!({ "sql": sql }),
            token,
        ))
        .await
        .unwrap();
    let status = response.status();
    (status, json_body(response).await)
}

#[tokio::test]
async fn cluster_mode_insert_and_read_via_raft() {
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
    .expect("plaintext runtime starts");

    let table_id = TableId::new(1);
    let database_id = DatabaseId::from_bytes([0x42; 16]);
    let tablet_id =
        seed_single_replica_tablet(&handle, &identity, table_id, database_id).await;

    // Production-style worker install: public DF SQL must use these workers
    // (FAC-DS-2), not a per-request ephemeral InMemoryTransport.
    let (fragment_endpoint, ai_endpoint) =
        mongreldb_server::fragment_rpc::install_production_cluster_workers(&handle)
            .await
            .expect("install fragment + AI workers");
    let storage = ServerStorageRuntime::cluster_with_workers(
        handle.clone(),
        fragment_endpoint,
        ai_endpoint,
    );
    assert!(storage.is_cluster());
    assert!(storage.require_standalone_db().is_err());
    assert!(storage.standalone_db().is_none());
    assert!(
        storage
            .cluster_gateway()
            .is_some_and(|g| g.workers_installed()),
        "gateway must expose installed fragment workers"
    );

    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(30)));
    let (app, control) = build_app_with_storage(
        storage,
        std::iter::empty(),
        Some("cluster-token".into()),
        None,
        false,
        sessions,
    );
    let token = "Bearer cluster-token";
    assert!(control.storage().require_standalone_db().is_err());
    assert!(control.storage().is_cluster());

    // Unsupported public SQL still fails closed (no standalone bypass).
    let (status, body) = sql_json(&app, token, "SELECT 1").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert!(
        body["error"]
            .as_str()
            .unwrap_or("")
            .contains("cluster mode refuses"),
        "{body}"
    );

    // INSERT is owned by consensus (typed write_tablet_ops → Raft propose).
    let (status, insert_body) = sql_json(
        &app,
        token,
        "INSERT INTO items (id, name) VALUES (1, 'alice')",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{insert_body}");
    assert_eq!(insert_body["status"], "ok");
    assert_eq!(insert_body["storage_mode"], "cluster");
    assert_eq!(insert_body["rows_affected"], 1);
    assert_eq!(insert_body["tablet_id"], tablet_id.to_string());
    assert_eq!(insert_body["write_path"], "write_tablet_ops");
    // P0.2-X5: public SQL reports the tablet Raft commit index (not a synthetic epoch).
    let commit_index = insert_body["commit_index"]
        .as_u64()
        .expect("commit_index must be a number");
    assert!(
        commit_index >= 1,
        "commit_index must come from a Raft receipt: {insert_body}"
    );
    // P0.2-X6: public write status reports the distributed commit timestamp.
    let commit_ts = insert_body["commit_ts"]
        .as_object()
        .expect("commit_ts object on insert response");
    assert!(
        commit_ts["physical_micros"].as_u64().unwrap_or(0) > 0,
        "commit_ts.physical_micros must be authoritative HLC: {insert_body}"
    );
    assert!(commit_ts["logical"].as_u64().is_some(), "{insert_body}");

    // Cross-check: tablet group applied index is at least the reported commit_index.
    let status = handle.runtime_status_json().await.expect("status");
    let tablets = status["tablets"].as_array().cloned().unwrap_or_default();
    let tablet_status = tablets.iter().find(|t| {
        t["tablet_id"].as_str() == Some(tablet_id.to_string().as_str())
    });
    if let Some(ts) = tablet_status {
        let applied = ts["applied_index"].as_u64().unwrap_or(0);
        assert!(
            applied >= commit_index,
            "tablet applied_index {applied} must cover public commit_index {commit_index}: {ts}"
        );
    }

    // Round-trip read via public SQL (tablet_typed_rows applied view).
    let (status, select_body) = sql_json(&app, token, "SELECT * FROM items").await;
    assert_eq!(status, StatusCode::OK, "{select_body}");
    let rows = select_body.as_array().expect("SELECT returns JSON array");
    assert_eq!(rows.len(), 1, "{select_body}");
    assert_eq!(rows[0]["id"], 1);
    assert_eq!(rows[0]["name"], "alice");

    // Spy: the same row is visible in the typed tablet keyspace after propose.
    let typed = handle
        .tablet_typed_rows(tablet_id)
        .await
        .expect("tablet_typed_rows after insert");
    assert!(
        !typed.is_empty(),
        "write_tablet_ops must leave applied typed rows"
    );
    let binding = handle
        .tablet_table_binding(tablet_id)
        .await
        .unwrap()
        .expect("items bound");
    assert_eq!(binding.local_table_name, "items");
    // row_id 1 from the id column.
    let cells = typed.get(&1).expect("row id 1");
    assert!(
        cells.values().any(|v| matches!(v, mongreldb_core::Value::Bytes(b) if b == b"alice")),
        "expected alice bytes cell, have {cells:?}"
    );

    // Filtered SELECT.
    let (status, filtered) = sql_json(&app, token, "SELECT name FROM items WHERE id = 1").await;
    assert_eq!(status, StatusCode::OK, "{filtered}");
    let rows = filtered.as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"], "alice");
    assert!(rows[0].get("id").is_none());

    // P0.4: complex SELECT goes through DataFusion plan + Coordinator execute
    // over the RemoteFragmentEndpoint protocol (FAC-DS-2) with correct values.
    let (status, agg) = sql_json(&app, token, "SELECT COUNT(*) FROM items").await;
    assert_eq!(status, StatusCode::OK, "{agg}");
    assert_eq!(agg["status"], "ok", "{agg}");
    assert_eq!(agg["storage_mode"], "cluster");
    assert_eq!(agg["distributed"], true);
    assert_eq!(agg["planner"], "plan_sql_distributed");
    assert_eq!(agg["executor"], "Coordinator");
    assert_eq!(
        agg["fragment_transport"], "RemoteFragmentEndpoint",
        "public SQL must use remote fragment contract, not bare InMemoryTransport: {agg}"
    );
    assert_eq!(
        agg["fragment_source"], "installed_fragment_endpoint",
        "public SQL must execute via ClusterGatewayRuntime.fragment_endpoint workers: {agg}"
    );
    let rows = agg["rows"]
        .as_array()
        .expect("Coordinator execute must return rows, not plan-only");
    assert!(!rows.is_empty(), "Coordinator execute must return rows: {agg}");
    // Correct aggregate value after 1 INSERT (AC §7.8).
    let count = rows[0]
        .as_object()
        .and_then(|o| {
            o.values().find_map(|v| {
                v.as_i64()
                    .or_else(|| v.as_u64().map(|u| u as i64))
                    .or_else(|| v.as_f64().map(|f| f as i64))
            })
        })
        .unwrap_or(-1);
    assert_eq!(
        count, 1,
        "COUNT(*) must equal inserted row count, got {count}: {agg}"
    );
    // Must not be the old plan-only response.
    assert_ne!(agg["status"], "planned", "{agg}");

    // Kit put batch also goes through Raft (same bound table).
    let kit_resp = app
        .clone()
        .oneshot(authorized(
            "POST",
            "/kit/txn",
            json!({
                "ops": [{
                    "put": {
                        "table": "items",
                        "cells": [1, 2, 2, "bob"],
                        "returning": true
                    }
                }]
            }),
            token,
        ))
        .await
        .unwrap();
    assert_eq!(kit_resp.status(), StatusCode::OK,);
    let kit_body = json_body(kit_resp).await;
    assert_eq!(kit_body["status"], "ok", "{kit_body}");
    assert_eq!(kit_body["storage_mode"], "cluster");
    assert!(kit_body["commit_index"].as_u64().unwrap_or(0) >= 1);

    // Direct handle write_tablet_ops still works (API surface for gateway).
    use mongreldb_consensus::engine_sink::TabletWriteOperation;
    let receipt = handle
        .write_tablet_ops(
            tablet_id,
            vec![TabletWriteOperation::Put {
                table_id: binding.local_table_id,
                row_id: 99,
                cells: vec![
                    (1, mongreldb_core::Value::Int64(99)),
                    (2, mongreldb_core::Value::Bytes(b"direct".to_vec())),
                ],
            }],
        )
        .await
        .expect("direct write_tablet_ops");
    assert!(receipt.position.index >= 1);

    // ID: P0.2-X9 Public Kit retrieval reads tablet data (not AppState.db).
    let kit_search = app
        .clone()
        .oneshot(authorized(
            "POST",
            "/kit/search",
            json!({
                "table": "items",
                "retrievers": [{
                    "name": "scan",
                    "weight": 1.0,
                    "sparse": { "column_id": 2, "query": [[1, 1.0]], "k": 10 }
                }],
                "fusion": { "reciprocal_rank": { "constant": 60 } },
                "limit": 10
            }),
            token,
        ))
        .await
        .unwrap();
    assert_eq!(kit_search.status(), StatusCode::OK,);
    let search_body = json_body(kit_search).await;
    assert_eq!(search_body["status"], "ok", "{search_body}");
    assert_eq!(search_body["storage_mode"], "cluster", "{search_body}");
    assert_eq!(
        search_body["tablet_id"],
        tablet_id.to_string(),
        "{search_body}"
    );
    let hits = search_body["hits"]
        .as_array()
        .expect("kit search returns hits array");
    assert!(
        !hits.is_empty(),
        "public Kit search must read tablet rows: {search_body}"
    );
    let names: Vec<&str> = hits
        .iter()
        .filter_map(|hit| {
            hit.get("row")
                .and_then(|row| row.get("name"))
                .and_then(|v| v.as_str())
                .or_else(|| hit.get("name").and_then(|v| v.as_str()))
        })
        .collect();
    assert!(
        names.iter().any(|n| *n == "alice" || *n == "bob")
            || hits.iter().any(|hit| {
                let s = hit.to_string();
                s.contains("alice") || s.contains("bob")
            }),
        "kit hits must include raft-written tablet rows: {search_body}"
    );

    // Multi-retriever path exercises production hybrid fusion over tablet data.
    let hybrid = app
        .clone()
        .oneshot(authorized(
            "POST",
            "/kit/search",
            json!({
                "table": "items",
                "retrievers": [
                    {
                        "name": "dense",
                        "weight": 1.0,
                        "ann": {
                            "column_id": 1,
                            "query": [0.0, 1.0],
                            "k": 10
                        }
                    },
                    {
                        "name": "sparse",
                        "weight": 1.0,
                        "sparse": { "column_id": 2, "query": [[1, 1.0]], "k": 10 }
                    }
                ],
                "fusion": { "reciprocal_rank": { "constant": 60 } },
                "limit": 10
            }),
            token,
        ))
        .await
        .unwrap();
    assert_eq!(hybrid.status(), StatusCode::OK);
    let hybrid_body = json_body(hybrid).await;
    assert_eq!(hybrid_body["status"], "ok", "{hybrid_body}");
    assert_eq!(hybrid_body["storage_mode"], "cluster");
    assert_eq!(
        hybrid_body["production_path"], "fuse_distributed_hits",
        "multi-retriever kit search must use production fusion: {hybrid_body}"
    );
    assert!(
        hybrid_body["hits"]
            .as_array()
            .map(|h| !h.is_empty())
            .unwrap_or(false),
        "hybrid kit search must return tablet hits: {hybrid_body}"
    );

    control.shutdown().await;
}

#[tokio::test]
async fn cluster_mode_sql_without_tablet_is_unavailable() {
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

    let sessions = Arc::new(SessionStore::new(8, Duration::from_secs(30)));
    let (app, control) = build_app_with_storage(
        ServerStorageRuntime::cluster(handle),
        std::iter::empty(),
        Some("cluster-token".into()),
        None,
        false,
        sessions,
    );
    let token = "Bearer cluster-token";

    let (status, body) = sql_json(
        &app,
        token,
        "INSERT INTO items (id, name) VALUES (1, 'alice')",
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["storage_mode"], "cluster");
    assert!(
        body["error"]
            .as_str()
            .unwrap_or("")
            .contains("no hosted tablets"),
        "{body}"
    );

    control.shutdown().await;
}
