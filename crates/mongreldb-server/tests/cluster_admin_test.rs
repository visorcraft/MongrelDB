//! Cluster administration endpoint tests (spec section 11.1, S2A-002):
//! - `GET /admin/cluster/status`: standalone reporting when no cluster
//!   identity exists, and the cluster-mode view (identity, membership,
//!   descriptors, version info) with a key-free trust summary.
//! - `POST /admin/cluster/node/drain` + `POST /admin/cluster/node/remove`:
//!   membership transitions with confirmation-token enforcement, admin
//!   authorization, and audit coverage (the token is never audited).

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use mongreldb_cluster::bootstrap::{
    cluster_init, cluster_status, removal_confirmation_token, InitRequest, TrustConfig,
};
use mongreldb_cluster::node::{Locality, NodeCapacity, NodeIdentity, NodeState};
use mongreldb_core::Database;
use mongreldb_server::{build_app, build_app_full};
use mongreldb_types::ids::NodeId;
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;
use tower::ServiceExt;

const CA_PEM: &str = "-----BEGIN CERTIFICATE-----\nY2E=\n-----END CERTIFICATE-----\n";
const CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nbm9kZQ==\n-----END CERTIFICATE-----\n";
const KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nc2VjcmV0\n-----END PRIVATE KEY-----\n";

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

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// Bootstrap a single-node cluster in `data` (the CLI's `cluster init`
/// library form) and return the provisioned identity.
fn bootstrap_cluster(data: &Path) -> NodeIdentity {
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
        rpc_address: "127.0.0.1:8453".to_owned(),
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

#[tokio::test]
async fn standalone_mode_reports_and_rejects_mutations() {
    let directory = tempdir().unwrap();
    let database = Arc::new(Database::create(directory.path()).unwrap());
    let app = build_app(database);

    // The rest of the server is unaffected by the missing cluster identity.
    let health = app
        .clone()
        .oneshot(empty_request("GET", "/health"))
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    let tables = app
        .clone()
        .oneshot(empty_request("GET", "/tables"))
        .await
        .unwrap();
    assert_eq!(tables.status(), StatusCode::OK);

    // Status reports standalone mode with this binary's version info.
    let status = app
        .clone()
        .oneshot(empty_request("GET", "/admin/cluster/status"))
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);
    let status = json_body(status).await;
    assert_eq!(status["mode"], "standalone");
    assert_eq!(
        status["version_info"]["binary_version"],
        env!("CARGO_PKG_VERSION")
    );

    // Mutations conflict with the standalone state; a missing token is a
    // plain bad request (checked before the standalone conflict).
    let drain = app
        .clone()
        .oneshot(request("POST", "/admin/cluster/node/drain", json!({})))
        .await
        .unwrap();
    assert_eq!(drain.status(), StatusCode::CONFLICT);
    let drain = json_body(drain).await;
    assert!(
        drain["error"].as_str().unwrap().contains("standalone"),
        "{drain}"
    );
    let remove = app
        .clone()
        .oneshot(request(
            "POST",
            "/admin/cluster/node/remove",
            json!({ "confirm_token": "anything" }),
        ))
        .await
        .unwrap();
    assert_eq!(remove.status(), StatusCode::CONFLICT);
    let remove = app
        .oneshot(request("POST", "/admin/cluster/node/remove", json!({})))
        .await
        .unwrap();
    assert_eq!(remove.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn cluster_mode_status_drain_and_remove() {
    let directory = tempdir().unwrap();
    let identity = bootstrap_cluster(directory.path());
    let database = Arc::new(Database::create(directory.path()).unwrap());
    let app = build_app(database);

    // Status reports the bootstrapped cluster: identity, membership,
    // descriptors, version info, and a key-free trust summary.
    let status = app
        .clone()
        .oneshot(empty_request("GET", "/admin/cluster/status"))
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);
    let status_bytes = to_bytes(status.into_body(), usize::MAX).await.unwrap();
    let status_text = String::from_utf8(status_bytes.to_vec()).unwrap();
    assert!(
        !status_text.contains("c2VjcmV0"),
        "key material leaked: {status_text}"
    );
    let status: Value = serde_json::from_str(&status_text).unwrap();
    assert_eq!(status["mode"], "cluster");
    assert_eq!(
        status["identity"]["cluster_id"],
        identity.cluster_id.to_hex()
    );
    assert_eq!(status["identity"]["node_id"], identity.node_id.to_hex());
    assert_eq!(status["membership"].as_array().unwrap().len(), 1);
    assert_eq!(status["membership"][0]["state"], "Up");
    assert_eq!(status["membership"][0]["rpc_address"], "127.0.0.1:8453");
    assert_eq!(
        status["database_group"]["raft_group_id"]
            .as_str()
            .unwrap()
            .len(),
        32
    );
    assert_eq!(status["trust"]["has_node_key"], true);
    assert_eq!(
        status["version_info"]["binary_version"],
        env!("CARGO_PKG_VERSION")
    );

    // Drain defaults to this node's own identity and persists.
    let drain = app
        .clone()
        .oneshot(request("POST", "/admin/cluster/node/drain", json!({})))
        .await
        .unwrap();
    assert_eq!(drain.status(), StatusCode::OK);
    let drain = json_body(drain).await;
    assert_eq!(drain["member"]["node_id"], identity.node_id.to_hex());
    assert_eq!(drain["member"]["state"], "Draining");
    assert_eq!(
        cluster_status(directory.path()).unwrap().membership[0].state,
        NodeState::Draining
    );
    // Draining again conflicts; an unknown member is a 404; a malformed id
    // is a 400.
    let again = app
        .clone()
        .oneshot(request("POST", "/admin/cluster/node/drain", json!({})))
        .await
        .unwrap();
    assert_eq!(again.status(), StatusCode::CONFLICT);
    let unknown = app
        .clone()
        .oneshot(request(
            "POST",
            "/admin/cluster/node/drain",
            json!({ "node_id": NodeId::new_random().to_hex() }),
        ))
        .await
        .unwrap();
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
    let malformed = app
        .clone()
        .oneshot(request(
            "POST",
            "/admin/cluster/node/drain",
            json!({ "node_id": "not-a-node-id" }),
        ))
        .await
        .unwrap();
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);

    // Remove requires the out-of-band confirmation token.
    let missing = app
        .clone()
        .oneshot(request("POST", "/admin/cluster/node/remove", json!({})))
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::BAD_REQUEST);
    let wrong = app
        .clone()
        .oneshot(request(
            "POST",
            "/admin/cluster/node/remove",
            json!({ "confirm_token": "not-the-token" }),
        ))
        .await
        .unwrap();
    assert_eq!(wrong.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        cluster_status(directory.path()).unwrap().membership[0].state,
        NodeState::Draining,
        "a rejected token must not change membership"
    );

    // The correct token decommissions the member; the token appears in no
    // response body and in no audit event.
    let token = removal_confirmation_token(identity.cluster_id, identity.node_id);
    let removed = app
        .clone()
        .oneshot(request(
            "POST",
            "/admin/cluster/node/remove",
            json!({ "confirm_token": token }),
        ))
        .await
        .unwrap();
    assert_eq!(removed.status(), StatusCode::OK);
    let removed_bytes = to_bytes(removed.into_body(), usize::MAX).await.unwrap();
    let removed_text = String::from_utf8(removed_bytes.to_vec()).unwrap();
    assert!(
        !removed_text.contains(&token),
        "token echoed: {removed_text}"
    );
    let removed: Value = serde_json::from_str(&removed_text).unwrap();
    assert_eq!(removed["member"]["state"], "Decommissioned");
    assert_eq!(
        cluster_status(directory.path()).unwrap().membership[0].state,
        NodeState::Decommissioned
    );

    // Drain and remove outcomes are audited with principal + action; the
    // confirmation token never reaches the audit log.
    let audit = app.oneshot(empty_request("GET", "/audit")).await.unwrap();
    assert_eq!(audit.status(), StatusCode::OK);
    let audit = json_body(audit).await;
    let events = audit.as_array().unwrap();
    for action in [
        "admin.cluster.drain",
        "admin.cluster.drain.ok",
        "admin.cluster.remove",
        "admin.cluster.remove.ok",
    ] {
        assert!(
            events.iter().any(|event| event["action"] == action),
            "missing audit event {action}: {audit}"
        );
    }
    for event in events {
        let detail = event["detail"].as_str().unwrap_or_default();
        assert!(!detail.contains(&token), "token leaked into audit: {event}");
    }
}

#[tokio::test]
async fn cluster_endpoints_require_admin_and_audit_authorization_failures() {
    let directory = tempdir().unwrap();
    bootstrap_cluster(directory.path());
    let database =
        Arc::new(Database::create_with_credentials(directory.path(), "admin", "admin-pw").unwrap());
    database.create_user("alice", "alice-pw").unwrap();
    let app = build_app_full(database, std::iter::empty(), None, None, true);

    // Unauthenticated and non-admin callers are rejected before anything runs.
    let anonymous = app
        .clone()
        .oneshot(empty_request("GET", "/admin/cluster/status"))
        .await
        .unwrap();
    assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);
    let non_admin_status = app
        .clone()
        .oneshot(authorized_request(
            "GET",
            "/admin/cluster/status",
            Value::Null,
            "Basic YWxpY2U6YWxpY2UtcHc=",
        ))
        .await
        .unwrap();
    assert_eq!(non_admin_status.status(), StatusCode::FORBIDDEN);
    let non_admin_drain = app
        .clone()
        .oneshot(authorized_request(
            "POST",
            "/admin/cluster/node/drain",
            json!({}),
            "Basic YWxpY2U6YWxpY2UtcHc=",
        ))
        .await
        .unwrap();
    assert_eq!(non_admin_drain.status(), StatusCode::FORBIDDEN);

    // The admin principal passes and sees the cluster view.
    let status = app
        .clone()
        .oneshot(authorized_request(
            "GET",
            "/admin/cluster/status",
            Value::Null,
            "Basic YWRtaW46YWRtaW4tcHc=",
        ))
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);
    let status = json_body(status).await;
    assert_eq!(status["mode"], "cluster");

    // The authorization failures are audited under the cluster actions.
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
    for action in ["admin.cluster.status.fail", "admin.cluster.drain.fail"] {
        assert!(
            events.iter().any(|event| event["action"] == action),
            "missing audit event {action}: {audit}"
        );
    }
}

/// Drive §15 admin SQL through the real `/sql` entry with AppState: on-disk
/// tablets surface in SHOW TABLETS, SHOW RESOURCE GROUPS exercises scheduler +
/// node governor + AI helpers, BACKUP/RESTORE submit live ops jobs, RESTORE
/// builds a plan from a published cluster backup when present.
#[tokio::test]
async fn admin_sql_show_and_backup_restore_use_live_state() {
    use mongreldb_cluster::cluster_backup::{
        run_cluster_backup, BackupSource, ClusterBackupError, ClusterBackupRequest,
        TabletSnapshotArtifact,
    };
    use mongreldb_cluster::tablet::{
        ReplicaDescriptor, ReplicaRole, TabletDescriptor, TabletLayout, TabletState,
    };
    use mongreldb_types::hlc::HlcTimestamp;
    use mongreldb_types::ids::{
        ClusterId, DatabaseId, MetadataVersion, RaftGroupId, TableId, TabletId,
    };
    use std::collections::BTreeMap;

    let directory = tempdir().unwrap();
    let database =
        Arc::new(Database::create_with_credentials(directory.path(), "admin", "admin-pw").unwrap());

    // Persist one real tablet descriptor under the database root.
    let tablet_id = TabletId::from_bytes({
        let mut b = [0u8; 16];
        b[15] = 7;
        b
    });
    let raft = RaftGroupId::from_bytes({
        let mut b = [0u8; 16];
        b[15] = 8;
        b
    });
    let node = NodeId::from_bytes({
        let mut b = [0u8; 16];
        b[15] = 1;
        b
    });
    let desc = TabletDescriptor {
        tablet_id,
        table_id: TableId::new(3),
        database_id: mongreldb_types::ids::DatabaseId::ZERO,
        raft_group_id: raft,
        partition: mongreldb_cluster::tablet::PartitionBounds::unbounded(),
        replicas: vec![ReplicaDescriptor {
            node_id: node,
            role: ReplicaRole::Voter,
            raft_node_id: 1,
        }],
        leader_hint: Some(node),
        generation: 2,
        state: TabletState::Active,
    };
    let layout = TabletLayout::new(directory.path(), tablet_id, raft);
    layout.create(&desc).unwrap();

    // Publish a real cluster backup for RESTORE plan_restore.
    struct Src;
    impl BackupSource for Src {
        fn capture_tablet(
            &self,
            tablet: &TabletDescriptor,
            _backup_ts: HlcTimestamp,
        ) -> Result<TabletSnapshotArtifact, ClusterBackupError> {
            Ok(TabletSnapshotArtifact {
                snapshot_payload: format!("snap-{}", tablet.tablet_id).into_bytes(),
                extra_files: BTreeMap::new(),
                covered_commit_ts: HlcTimestamp {
                    physical_micros: 1_000,
                    logical: 0,
                    node_tiebreaker: 1,
                },
                log_continuation_term: 1,
                log_continuation_index: 1,
                log_archive: None,
            })
        }
    }
    let backup_dir = directory.path().join("backup-out");
    run_cluster_backup(
        &ClusterBackupRequest {
            cluster_id: ClusterId::from_bytes([0xAA; 16]),
            database_id: DatabaseId::from_bytes([0xBB; 16]),
            meta_version: MetadataVersion::new(1),
            backup_ts: Some(HlcTimestamp {
                physical_micros: 1_000,
                logical: 0,
                node_tiebreaker: 1,
            }),
            tablets: vec![desc.clone()],
            destination: backup_dir.clone(),
            encryption: None,
        },
        &Src,
    )
    .unwrap();

    let app = build_app_full(Arc::clone(&database), std::iter::empty(), None, None, true);
    let admin = "Basic YWRtaW46YWRtaW4tcHc=";

    async fn sql_json(app: &axum::Router, admin: &str, sql: &str) -> Value {
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
        assert_eq!(response.status(), StatusCode::OK, "sql={sql}");
        json_body(response).await
    }

    let tablets = sql_json(&app, admin, "SHOW TABLETS").await;
    assert_eq!(tablets["command"], "SHOW TABLETS");
    assert_eq!(tablets["tablets"].as_array().unwrap().len(), 1);
    assert_eq!(
        tablets["tablets"][0]["tablet_id"].as_str().unwrap(),
        tablet_id.to_string()
    );
    assert_eq!(tablets["tablets"][0]["generation"], 2);

    let replicas = sql_json(&app, admin, "SHOW REPLICAS").await;
    assert_eq!(replicas["replicas"].as_array().unwrap().len(), 1);
    assert_eq!(replicas["replicas"][0]["counts_toward_quorum"], true);

    let resources = sql_json(&app, admin, "SHOW RESOURCE GROUPS").await;
    assert!(resources["node_governor"].is_object());
    assert!(resources["node_governor"]["actions"].is_array());
    assert!(resources["scheduler"].is_object());
    assert!(
        resources["ai"]["adaptive_local_k_example"]
            .as_u64()
            .unwrap()
            >= 1
    );

    let cluster = sql_json(&app, admin, "SHOW CLUSTER").await;
    assert_eq!(cluster["multi_region"]["multi_leader_default"], false);
    assert!(cluster["multi_region"]["total_voters"].as_u64().unwrap() >= 1);

    let backup = sql_json(&app, admin, "BACKUP DATABASE TO '/tmp/unused'").await;
    assert_eq!(backup["status"], "accepted");
    assert!(backup["job"]["job_id"].as_str().unwrap().contains("backup"));

    let restore = sql_json(
        &app,
        admin,
        &format!("RESTORE DATABASE FROM '{}'", backup_dir.display()),
    )
    .await;
    assert_eq!(restore["status"], "accepted");
    // OpsJobKind::Restore serializes as kind name "restore" (RestoreVerification aliases the same name).
    assert_eq!(restore["job"]["kind"].as_str().unwrap(), "restore");
    assert!(
        restore["restore_plan"]["tablet_count"].as_u64().unwrap() >= 1,
        "restore plan must come from published backup: {restore}"
    );
    assert!(restore["restore_plan"]["target_cluster_id"].is_string());

    let jobs = sql_json(&app, admin, "SHOW JOBS").await;
    let job_list = jobs["jobs"].as_array().unwrap();
    assert!(
        job_list.iter().any(|j| j["source"] == "ops"),
        "expected ops jobs in SHOW JOBS: {jobs}"
    );
    // P1.6-X5: progress is visible on the public SHOW JOBS surface.
    assert!(
        job_list.iter().any(|j| {
            j["source"] == "ops"
                && j["progress"].as_str().map(|p| !p.is_empty()).unwrap_or(false)
        }),
        "ops jobs must expose non-empty progress: {jobs}"
    );

    let backups = sql_json(&app, admin, "SHOW BACKUPS").await;
    assert!(
        !backups["backups"].as_array().unwrap().is_empty(),
        "SHOW BACKUPS must list submitted backup jobs: {backups}"
    );
}
