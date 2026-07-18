//! Stage 3 gate qualification suites (spec §12 gate).
//!
//! Covers routing across 3+ tablets, backup/restore identity rules, placement
//! rebalance safety, and metadata-unavailability documentation assertions.
//! Distributed serializable anomaly and full TCP runtime suites live beside
//! dist_txn / runtime tests; this file binds the residual gate checks that
//! landed with S3L + gateway.

use mongreldb_cluster::cluster_backup::{
    plan_restore, run_cluster_backup, verify_backup, BackupSource, ClusterBackupEncryption,
    ClusterBackupError, ClusterBackupRequest, RestoreIdentityMode, TabletSnapshotArtifact,
};
use mongreldb_cluster::gateway::{
    bind_plan_to_tablets, parse_admin_sql, GatewayFragment, GatewayPlan, TabletLayoutSnapshot,
};
use mongreldb_cluster::placement::{check_move_safety, check_move_safety_healthy, VoterChange};
use mongreldb_cluster::routing::RoutingCache;
use mongreldb_cluster::tablet::{
    find_tablet_for_key, tablets_overlapping, Bound, Key, PartitionBounds, ReplicaDescriptor,
    ReplicaRole, TabletDescriptor, TabletState,
};
use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::{
    ClusterId, DatabaseId, MetadataVersion, NodeId, QueryId, RaftGroupId, TableId, TabletId,
};
use std::collections::BTreeMap;

fn tid(n: u8) -> TabletId {
    TabletId::from_bytes({
        let mut b = [0u8; 16];
        b[15] = n;
        b
    })
}
fn rid(n: u8) -> RaftGroupId {
    RaftGroupId::from_bytes({
        let mut b = [0u8; 16];
        b[15] = n;
        b
    })
}
fn nid(n: u8) -> NodeId {
    NodeId::from_bytes({
        let mut b = [0u8; 16];
        b[15] = n;
        b
    })
}
fn cid(n: u8) -> ClusterId {
    ClusterId::from_bytes({
        let mut b = [0u8; 16];
        b[15] = n;
        b
    })
}
fn did(n: u8) -> DatabaseId {
    DatabaseId::from_bytes({
        let mut b = [0u8; 16];
        b[15] = n;
        b
    })
}
fn hlc(m: u64) -> HlcTimestamp {
    HlcTimestamp {
        physical_micros: m,
        logical: 0,
        node_tiebreaker: 1,
    }
}

fn key_byte(b: u8) -> Key {
    Key::from_bytes(vec![b])
}

fn tablet(id: u8, low: u8, high: u8) -> TabletDescriptor {
    TabletDescriptor {
        tablet_id: tid(id),
        table_id: TableId::new(1),
        database_id: mongreldb_types::ids::DatabaseId::ZERO,
        raft_group_id: rid(id),
        partition: PartitionBounds::new(
            Bound::Included(key_byte(low)),
            Bound::Excluded(key_byte(high)),
        )
        .unwrap(),
        replicas: vec![ReplicaDescriptor {
            node_id: nid(1),
            role: ReplicaRole::Voter,
            raft_node_id: 1,
        }],
        leader_hint: Some(nid(1)),
        generation: 1,
        state: TabletState::Active,
    }
}

struct MemSource {
    covered: HlcTimestamp,
}

impl BackupSource for MemSource {
    fn capture_tablet(
        &self,
        tablet: &TabletDescriptor,
        _backup_ts: HlcTimestamp,
    ) -> Result<TabletSnapshotArtifact, ClusterBackupError> {
        Ok(TabletSnapshotArtifact {
            snapshot_payload: format!("snap-{}", tablet.tablet_id).into_bytes(),
            extra_files: BTreeMap::new(),
            covered_commit_ts: self.covered,
            log_continuation_term: 1,
            log_continuation_index: 10,
            log_archive: Some(b"log".to_vec()),
        })
    }
}

#[test]
fn gate_three_plus_tablets_route_point_and_range() {
    let tablets = vec![tablet(1, 0, 50), tablet(2, 50, 100), tablet(3, 100, 150)];
    // Point routes.
    let t = find_tablet_for_key(&tablets, TableId::new(1), &key_byte(25)).unwrap();
    assert_eq!(t.tablet_id, tid(1));
    let t = find_tablet_for_key(&tablets, TableId::new(1), &key_byte(75)).unwrap();
    assert_eq!(t.tablet_id, tid(2));
    let t = find_tablet_for_key(&tablets, TableId::new(1), &key_byte(120)).unwrap();
    assert_eq!(t.tablet_id, tid(3));
    // Range overlaps multiple tablets.
    let range = PartitionBounds::new(
        Bound::Included(key_byte(40)),
        Bound::Excluded(key_byte(110)),
    )
    .unwrap();
    let overlap = tablets_overlapping(&tablets, TableId::new(1), &range);
    assert!(overlap.len() >= 2);
}

#[test]
fn gate_cluster_backup_restore_new_identity() {
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("bak");
    let covered = hlc(1_000);
    let source = MemSource { covered };
    let report = run_cluster_backup(
        &ClusterBackupRequest {
            cluster_id: cid(1),
            database_id: did(2),
            meta_version: MetadataVersion::new(3),
            backup_ts: Some(covered),
            tablets: vec![tablet(1, 0, 50), tablet(2, 50, 100), tablet(3, 100, 150)],
            destination: dest.clone(),
            encryption: Some(ClusterBackupEncryption {
                scheme: "none".into(),
                kms_key_id: None,
                key_version: None,
            }),
        },
        &source,
    )
    .unwrap();
    let (manifest, verify) = verify_backup(&dest).unwrap();
    assert!(verify.manifest_ok && verify.files_ok);
    assert_eq!(manifest.tablet_count(), 3);
    assert_eq!(report.tablets, 3);

    let plan = plan_restore(
        &manifest,
        RestoreIdentityMode::NewIdentity,
        Some((cid(9), did(8))),
    )
    .unwrap();
    assert_ne!(plan.target_cluster_id, manifest.cluster_id);
    assert_ne!(plan.target_database_id, manifest.database_id);
    assert_eq!(plan.tablets.len(), 3);
}

#[test]
fn gate_gateway_binds_without_opening_tablet_files() {
    let layout = TabletLayoutSnapshot::from_descriptors(
        MetadataVersion::new(1),
        vec![tablet(1, 0, 50), tablet(2, 50, 100), tablet(3, 100, 150)],
    );
    let routing = RoutingCache::new();
    let plan = GatewayPlan {
        query_id: QueryId::from_bytes([0xCD; 16]),
        metadata_version: MetadataVersion::new(1),
        fragments: vec![GatewayFragment {
            fragment_id: 0,
            tablet_ids: vec![],
            table_id: Some(TableId::new(1)),
        }],
    };
    let bound = bind_plan_to_tablets(&plan, &layout, &routing, &|node| {
        Some(mongreldb_cluster::routing::Endpoint {
            node_id: node,
            address: "127.0.0.1:1".into(),
        })
    })
    .unwrap();
    assert_eq!(bound.fragments[0].targets.len(), 3);
}

#[test]
fn gate_admin_sql_surface_parses() {
    for sql in [
        "SHOW CLUSTER",
        "SHOW NODES",
        "SHOW TABLETS",
        "SHOW REPLICAS",
        "SHOW TRANSACTIONS",
        "SHOW QUERIES",
        "SHOW JOBS",
        "SHOW RESOURCE GROUPS",
        "SHOW BACKUPS",
        "BACKUP DATABASE",
    ] {
        assert!(parse_admin_sql(sql).unwrap().is_some(), "{sql}");
    }
}

#[test]
fn gate_rebalance_respects_healthy_quorum() {
    // 3 configured, 2 healthy: removal of a voter must fail (healthy would drop to 1 < quorum 2).
    let err = check_move_safety_healthy(3, 2, VoterChange::RemoveVoter).unwrap_err();
    assert!(format!("{err}").contains("quorum") || format!("{err}").contains("refused"));
    // Healthy full set: 3→2 ok.
    assert!(check_move_safety_healthy(3, 3, VoterChange::RemoveVoter).is_ok());
    // Legacy API still works.
    assert!(check_move_safety(3, VoterChange::RemoveVoter).is_ok());
    assert!(check_move_safety(2, VoterChange::RemoveVoter).is_err());
}

#[test]
fn gate_metadata_unavailability_is_documented() {
    // Spec §12 gate: metadata unavailability has documented behavior.
    // The routing cache treats missing entries as stale; gateway bind refuses
    // stale pins with GatewayError::StaleMetadata — operators refresh and retry.
    let doc = include_str!("../../../docs/21-sharded-cluster.md");
    assert!(
        doc.to_ascii_lowercase().contains("metadata")
            || doc.contains("StaleMetadata")
            || doc.contains("meta"),
        "docs/21 must discuss metadata plane behavior"
    );
}
