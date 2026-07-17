//! Node runtime integration tests (spec sections 11.1, 12.1-12.4, 12.7).
//!
//! Real multi-node clusters over loopback TCP (plaintext
//! [`TransportSecurity::PlaintextForTesting`]): a three-node runtime cluster
//! bootstraps the meta group, joins nodes 2/3 through
//! [`MetaGroup::add_member`], creates a three-replica tablet group spanning
//! the nodes, proposes writes through the tablet leader, reads on followers
//! with read-your-writes tokens (spec section 11.4), restarts one node into
//! rejoin + catch-up, and shuts one node down gracefully while quorum writes
//! continue. A second test drives the section 12.7 replica-join workflow
//! (add learner, catch up, promote) onto a fourth node.

use std::path::{Path, PathBuf};
use std::time::Duration;

use mongreldb_cluster::bootstrap::{
    cluster_init, cluster_join, InitRequest, JoinInvite, TrustConfig,
};
use mongreldb_cluster::meta::{DatabaseDescriptor, DatabaseState, MetaCommand, TableSchemaRecord};
use mongreldb_cluster::network::{TcpTransport, TransportConfig, TransportSecurity};
use mongreldb_cluster::node::{
    BuildVersion, Locality, NodeCapacity, NodeDescriptor, NodeIdentity, NodeState, VersionInfo,
};
use mongreldb_cluster::runtime::{
    GroupTiming, MetaMembership, NodeRuntime, NodeRuntimeConfig, RuntimeError,
};
use mongreldb_cluster::tablet::{
    ColumnId, PartitionBounds, ReplicaDescriptor, ReplicaRole, TablePartitioningRecord,
    TabletDescriptor, TabletState,
};
use mongreldb_consensus::group::{ConsensusGroup, GroupCommitReceipt};
use mongreldb_consensus::identity::{CommandKind, RaftNodeId};
use mongreldb_consensus::read::{ReadConsistency, SessionToken};
use mongreldb_log::commit_log::ExecutionControl;
use mongreldb_log::envelope::CommandEnvelope;
use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::{
    ClusterId, DatabaseId, MetadataVersion, NodeId, RaftGroupId, SchemaVersion, TableId, TabletId,
};
use tempfile::TempDir;

const LEADER_TIMEOUT: Duration = Duration::from_secs(15);
/// Maintenance envelopes ride `CommandKind::Maintenance` (envelope command
/// type 3 per `mongreldb-core` `replicated_apply`); the engine apply sink
/// treats them as documented no-ops, so they exercise the quorum write path
/// (propose, replicate, commit, apply, watermark) without needing core row
/// payloads.
const COMMAND_TYPE_MAINTENANCE: u32 = 3;

// Dummy PEM armor: `cluster init` / `cluster join` validate trust material
// structurally, and these tests run plaintext transport, so the content is
// inert (mirrors `bootstrap.rs`'s own tests).
const CA_PEM: &str = "-----BEGIN CERTIFICATE-----\nY2E=\n-----END CERTIFICATE-----\n";
const CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nbm9kZQ==\n-----END CERTIFICATE-----\n";
const KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nc2VjcmV0\n-----END PRIVATE KEY-----\n";

fn trust() -> TrustConfig {
    TrustConfig::from_pems(
        CA_PEM.to_owned(),
        CERT_PEM.to_owned(),
        KEY_PEM.to_owned(),
        vec![NodeId::from_bytes([0xFF; 16])],
    )
    .unwrap()
}

fn csprng() -> impl FnMut(&mut [u8]) -> Result<(), getrandom::Error> {
    getrandom::getrandom
}

/// A free loopback address (`127.0.0.1:<port>`), pre-allocated by binding
/// and releasing port 0.
fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

fn fast_timing() -> GroupTiming {
    GroupTiming {
        heartbeat_interval: Duration::from_millis(50),
        election_timeout_min: Duration::from_millis(150),
        election_timeout_max: Duration::from_millis(300),
        install_snapshot_timeout: Duration::from_millis(2_000),
    }
}

fn transport_config() -> TransportConfig {
    TransportConfig {
        connect_timeout: Duration::from_millis(500),
        rpc_timeout: Duration::from_secs(2),
        snapshot_timeout: Duration::from_secs(5),
        connect_attempts: 5,
        reconnect_backoff: Duration::from_millis(10),
        max_frame_bytes: 16 * 1024 * 1024,
        max_connections: 64,
        handshake_timeout: Duration::from_secs(2),
        shutdown_grace: Duration::from_secs(2),
    }
}

fn descriptor(node_id: NodeId, address: &str) -> NodeDescriptor {
    NodeDescriptor {
        node_id,
        rpc_address: address.to_owned(),
        locality: Locality::default(),
        capacity: NodeCapacity::default(),
        state: NodeState::Up,
        version: BuildVersion::current(),
        version_info: VersionInfo::current(),
    }
}

fn runtime_config(
    node_data: PathBuf,
    address: &str,
    peers: &[(NodeId, String)],
    meta: Option<MetaMembership>,
) -> NodeRuntimeConfig {
    NodeRuntimeConfig {
        node_data,
        security: TransportSecurity::PlaintextForTesting,
        transport: transport_config(),
        listen_address: address.to_owned(),
        rpc_address: Some(address.to_owned()),
        peers: peers.to_vec(),
        meta,
        timing: Some(fast_timing()),
    }
}

/// Provisions one node directory: `cluster init` when `join` is `None`,
/// `cluster join` into `(cluster_id, endpoints)` otherwise.
fn provision_node(
    node_data: &Path,
    address: &str,
    join: Option<&(ClusterId, Vec<String>)>,
) -> NodeIdentity {
    match join {
        None => {
            let report = cluster_init(
                node_data,
                &InitRequest {
                    rpc_address: address.to_owned(),
                    locality: "region=test,zone=a".parse().unwrap(),
                    capacity: NodeCapacity::default(),
                    trust: trust(),
                },
                &mut csprng(),
            )
            .unwrap();
            report.identity
        }
        Some((cluster_id, endpoints)) => {
            let report = cluster_join(
                node_data,
                &JoinInvite {
                    cluster_id: *cluster_id,
                    member_endpoints: endpoints.clone(),
                    trust: trust(),
                },
                &mut csprng(),
            )
            .unwrap();
            report.identity
        }
    }
}

fn command_id(seq: u64) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&seq.to_le_bytes());
    id
}

fn tablet_descriptor(
    tablet_id: TabletId,
    raft_group_id: RaftGroupId,
    table_id: TableId,
    replicas: Vec<ReplicaDescriptor>,
    generation: u64,
) -> TabletDescriptor {
    TabletDescriptor {
        tablet_id,
        table_id,
        raft_group_id,
        partition: PartitionBounds::unbounded(),
        replicas,
        leader_hint: None,
        generation,
        state: TabletState::Active,
    }
}

fn voter(node_id: NodeId, raft_node_id: RaftNodeId) -> ReplicaDescriptor {
    ReplicaDescriptor {
        node_id,
        role: ReplicaRole::Voter,
        raft_node_id,
    }
}

async fn propose_write(group: &ConsensusGroup<TcpTransport>, seq: u64) -> GroupCommitReceipt {
    let envelope = CommandEnvelope::new(
        COMMAND_TYPE_MAINTENANCE,
        command_id(seq),
        format!("write-{seq}").into_bytes(),
    );
    group
        .propose(
            CommandKind::Maintenance,
            envelope,
            &ExecutionControl::default(),
        )
        .await
        .unwrap()
}

fn session_token(
    group: &ConsensusGroup<TcpTransport>,
    receipt: &GroupCommitReceipt,
) -> SessionToken {
    SessionToken {
        group_id: group.group_id().to_owned(),
        commit_index: receipt.position.index,
        commit_ts: receipt.commit_ts,
    }
}

/// The runtime (of `nodes`) whose tablet group replica currently leads.
async fn tablet_leader<'a>(nodes: &[&'a NodeRuntime], tablet_id: TabletId) -> &'a NodeRuntime {
    let leader = nodes
        .iter()
        .find_map(|node| node.tablet_group(tablet_id))
        .expect("a tablet group")
        .wait_leader(LEADER_TIMEOUT)
        .await
        .unwrap();
    nodes
        .iter()
        .find(|node| {
            node.tablet_group(tablet_id)
                .is_some_and(|group| group.node_id() == leader)
        })
        .copied()
        .expect("the leader is one of the runtimes")
}

/// Publishes the database and table schema the tablet descriptor references
/// (meta apply enforces the reference).
async fn publish_schema(node: &NodeRuntime, database_id: DatabaseId, table_id: TableId) {
    let control = ExecutionControl::default();
    let meta = node.meta_group().unwrap();
    meta.propose(
        command_id(90),
        MetaCommand::CreateDatabase {
            descriptor: DatabaseDescriptor {
                database_id,
                name: "app".to_owned(),
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
        command_id(91),
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
}

#[tokio::test]
async fn three_node_runtime_cluster_end_to_end() {
    let tmp = TempDir::new().unwrap();
    let dirs: Vec<PathBuf> = (1..=3)
        .map(|index| tmp.path().join(format!("node-{index}")))
        .collect();
    let addrs: Vec<String> = (1..=3).map(|_| free_addr()).collect();

    // Provision identities: node 1 inits the cluster; nodes 2/3 join it.
    let identity1 = provision_node(&dirs[0], &addrs[0], None);
    let invite = (identity1.cluster_id, addrs.clone());
    let identity2 = provision_node(&dirs[1], &addrs[1], Some(&invite));
    let identity3 = provision_node(&dirs[2], &addrs[2], Some(&invite));
    let identities = [identity1, identity2, identity3];
    let peers: Vec<(NodeId, String)> = identities
        .iter()
        .zip(&addrs)
        .map(|(identity, addr)| (identity.node_id, addr.clone()))
        .collect();

    // Start the runtimes: node 1 bootstraps the meta group; nodes 2/3 run
    // pristine meta members awaiting `add_member`.
    let meta_group_id = RaftGroupId::new_random();
    let mut node1 = NodeRuntime::start(runtime_config(
        dirs[0].clone(),
        &addrs[0],
        &peers,
        Some(MetaMembership {
            meta_group_id,
            bootstrap_voters: Some(vec![(identities[0].node_id, addrs[0].clone())]),
        }),
    ))
    .await
    .unwrap();
    let mut node2 = NodeRuntime::start(runtime_config(
        dirs[1].clone(),
        &addrs[1],
        &peers,
        Some(MetaMembership {
            meta_group_id,
            bootstrap_voters: None,
        }),
    ))
    .await
    .unwrap();
    let mut node3 = NodeRuntime::start(runtime_config(
        dirs[2].clone(),
        &addrs[2],
        &peers,
        Some(MetaMembership {
            meta_group_id,
            bootstrap_voters: None,
        }),
    ))
    .await
    .unwrap();

    let control = ExecutionControl::default();
    {
        let meta = node1.meta_group().unwrap();
        meta.group().wait_leader(LEADER_TIMEOUT).await.unwrap();
        // Node 1 registers itself; nodes 2/3 join through the membership
        // workflow (learner, catch-up, promote, descriptor registration).
        meta.propose(
            command_id(1),
            MetaCommand::RegisterNode {
                descriptor: descriptor(identities[0].node_id, &addrs[0]),
            },
            &control,
        )
        .await
        .unwrap();
        meta.add_member(&descriptor(identities[1].node_id, &addrs[1]), &control)
            .await
            .unwrap();
        meta.add_member(&descriptor(identities[2].node_id, &addrs[2]), &control)
            .await
            .unwrap();
        assert_eq!(meta.state().nodes.len(), 3);
    }
    publish_schema(&node1, DatabaseId::new_random(), TableId::new(1)).await;
    let table_id = TableId::new(1);

    // One tablet group spanning all three nodes (raft ids 1..=3).
    let tablet_id = TabletId::new_random();
    let tablet = tablet_descriptor(
        tablet_id,
        RaftGroupId::new_random(),
        table_id,
        vec![
            voter(identities[0].node_id, 1),
            voter(identities[1].node_id, 2),
            voter(identities[2].node_id, 3),
        ],
        1,
    );
    let partitioning =
        TablePartitioningRecord::automatic_default(table_id, vec![ColumnId::new(1)], 16);
    // Replicas first; the bootstrapping node last.
    node2
        .create_tablet(&tablet, &partitioning, None, false, &control)
        .await
        .unwrap();
    node3
        .create_tablet(&tablet, &partitioning, None, false, &control)
        .await
        .unwrap();
    node1
        .create_tablet(
            &tablet,
            &partitioning,
            Some(peers.as_slice()),
            true,
            &control,
        )
        .await
        .unwrap();

    // The meta descriptor reflects the tablet and its replicas.
    let published = node1
        .meta_group()
        .unwrap()
        .state()
        .tablet(tablet_id)
        .unwrap()
        .clone();
    assert_eq!(published, tablet);
    assert_eq!(published.replicas.len(), 3);

    // Status: identity, groups with roles, applied watermarks.
    let status = node1.status();
    assert_eq!(status.identity, identities[0]);
    assert_eq!(status.rpc_address, addrs[0]);
    assert!(status.meta.is_some());
    assert_eq!(status.tablets.len(), 1);
    assert_eq!(status.tablets[0].replicas.len(), 3);

    // Writes through the tablet group leader; read-your-writes on both
    // followers (spec section 11.4).
    let nodes: Vec<&NodeRuntime> = vec![&node1, &node2, &node3];
    let leader = tablet_leader(&nodes, tablet_id).await;
    let leader_group = leader.tablet_group(tablet_id).unwrap();
    let receipt = propose_write(leader_group, 1).await;
    let token = session_token(leader_group, &receipt);
    for node in &nodes {
        let group = node.tablet_group(tablet_id).unwrap();
        let watermark = group
            .consistent_read(
                &ReadConsistency::ReadYourWrites {
                    token: token.clone(),
                },
                &control,
            )
            .await
            .unwrap();
        assert!(watermark.position.index >= token.commit_index);
    }

    // Restart node 3: it rejoins from its durable state and catches up.
    node3.shutdown().await.unwrap();
    let node3 = NodeRuntime::start(runtime_config(
        dirs[2].clone(),
        &addrs[2],
        &peers,
        Some(MetaMembership {
            meta_group_id,
            bootstrap_voters: None,
        }),
    ))
    .await
    .unwrap();
    assert!(node3.meta_group().is_some());
    let restarted_group = node3.tablet_group(tablet_id).unwrap();
    restarted_group
        .wait_applied_index(receipt.position.index, LEADER_TIMEOUT)
        .await
        .unwrap();

    // A fresh write lands on the restarted node too (read-your-writes).
    let nodes: Vec<&NodeRuntime> = vec![&node1, &node2, &node3];
    let leader = tablet_leader(&nodes, tablet_id).await;
    let leader_group = leader.tablet_group(tablet_id).unwrap();
    let receipt = propose_write(leader_group, 2).await;
    let token = session_token(leader_group, &receipt);
    let watermark = restarted_group
        .consistent_read(
            &ReadConsistency::ReadYourWrites {
                token: token.clone(),
            },
            &control,
        )
        .await
        .unwrap();
    assert!(watermark.position.index >= token.commit_index);

    // Graceful shutdown of a non-leader node leaves quorum writes working.
    let nodes: Vec<&NodeRuntime> = vec![&node1, &node2, &node3];
    let leader = tablet_leader(&nodes, tablet_id).await;
    let victim_is_node2 = leader.identity().node_id != identities[1].node_id;
    let survivor = if victim_is_node2 {
        node2.shutdown().await.unwrap();
        node3
    } else {
        node3.shutdown().await.unwrap();
        node2
    };
    let survivors: Vec<&NodeRuntime> = vec![&node1, &survivor];
    let leader = tablet_leader(&survivors, tablet_id).await;
    let receipt = propose_write(leader.tablet_group(tablet_id).unwrap(), 3).await;
    assert!(receipt.position.index > 0);

    node1.shutdown().await.unwrap();
    survivor.shutdown().await.unwrap();
}

#[tokio::test]
async fn replica_join_workflow_promotes_a_new_voter() {
    let tmp = TempDir::new().unwrap();
    let dirs: Vec<PathBuf> = (1..=4)
        .map(|index| tmp.path().join(format!("node-{index}")))
        .collect();
    let addrs: Vec<String> = (1..=4).map(|_| free_addr()).collect();

    let identity1 = provision_node(&dirs[0], &addrs[0], None);
    let invite = (identity1.cluster_id, addrs[..3].to_vec());
    let identity2 = provision_node(&dirs[1], &addrs[1], Some(&invite));
    let identity3 = provision_node(&dirs[2], &addrs[2], Some(&invite));
    let identity4 = provision_node(&dirs[3], &addrs[3], Some(&invite));
    let identities = [identity1, identity2, identity3, identity4];
    let peers: Vec<(NodeId, String)> = identities
        .iter()
        .zip(&addrs)
        .map(|(identity, addr)| (identity.node_id, addr.clone()))
        .collect();

    // Nodes 1-3 run the meta group and (shortly) a three-voter tablet; node
    // 4 runs no meta group and no tablet yet.
    let meta_group_id = RaftGroupId::new_random();
    let membership = |bootstrap_voters| {
        Some(MetaMembership {
            meta_group_id,
            bootstrap_voters,
        })
    };
    let mut node1 = NodeRuntime::start(runtime_config(
        dirs[0].clone(),
        &addrs[0],
        &peers,
        membership(Some(vec![(identities[0].node_id, addrs[0].clone())])),
    ))
    .await
    .unwrap();
    let mut node2 = NodeRuntime::start(runtime_config(
        dirs[1].clone(),
        &addrs[1],
        &peers,
        membership(None),
    ))
    .await
    .unwrap();
    let mut node3 = NodeRuntime::start(runtime_config(
        dirs[2].clone(),
        &addrs[2],
        &peers,
        membership(None),
    ))
    .await
    .unwrap();
    let mut node4 = NodeRuntime::start(runtime_config(dirs[3].clone(), &addrs[3], &peers, None))
        .await
        .unwrap();

    let control = ExecutionControl::default();
    {
        let meta = node1.meta_group().unwrap();
        meta.group().wait_leader(LEADER_TIMEOUT).await.unwrap();
        meta.propose(
            command_id(1),
            MetaCommand::RegisterNode {
                descriptor: descriptor(identities[0].node_id, &addrs[0]),
            },
            &control,
        )
        .await
        .unwrap();
        for (identity, addr) in identities[1..3].iter().zip(&addrs[1..3]) {
            meta.add_member(&descriptor(identity.node_id, addr), &control)
                .await
                .unwrap();
        }
        // Register node 4's descriptor so its locality is known to placement.
        meta.propose(
            command_id(2),
            MetaCommand::RegisterNode {
                descriptor: descriptor(identities[3].node_id, &addrs[3]),
            },
            &control,
        )
        .await
        .unwrap();
    }
    publish_schema(&node1, DatabaseId::new_random(), TableId::new(1)).await;
    let table_id = TableId::new(1);

    let tablet_id = TabletId::new_random();
    let tablet = tablet_descriptor(
        tablet_id,
        RaftGroupId::new_random(),
        table_id,
        vec![
            voter(identities[0].node_id, 1),
            voter(identities[1].node_id, 2),
            voter(identities[2].node_id, 3),
        ],
        1,
    );
    let partitioning =
        TablePartitioningRecord::automatic_default(table_id, vec![ColumnId::new(1)], 16);
    node2
        .create_tablet(&tablet, &partitioning, None, false, &control)
        .await
        .unwrap();
    node3
        .create_tablet(&tablet, &partitioning, None, false, &control)
        .await
        .unwrap();
    node1
        .create_tablet(&tablet, &partitioning, Some(&peers[..3]), true, &control)
        .await
        .unwrap();

    // Node 4 creates its local replica as a learner (descriptor generation 2
    // naming it), then the tablet leader drives the section 12.7 movement
    // protocol: add learner, snapshot/catch-up, promote.
    let learner = ReplicaDescriptor {
        node_id: identities[3].node_id,
        role: ReplicaRole::Learner,
        raft_node_id: 4,
    };
    let mut joined = tablet.clone();
    joined.generation = 2;
    joined.replicas.push(learner);
    node4
        .create_tablet(&joined, &partitioning, None, false, &control)
        .await
        .unwrap();
    let nodes: Vec<&NodeRuntime> = vec![&node1, &node2, &node3];
    let leader = tablet_leader(&nodes, tablet_id).await;
    leader
        .add_tablet_replica(tablet_id, learner, &addrs[3])
        .await
        .unwrap();

    // Publish the promoted descriptor (node 4 a voter, generation 2) and
    // confirm the meta state reflects it.
    let mut promoted = joined.clone();
    promoted
        .replicas
        .iter_mut()
        .find(|replica| replica.node_id == identities[3].node_id)
        .unwrap()
        .role = ReplicaRole::Voter;
    leader
        .publish_tablet_descriptor(&promoted, &control)
        .await
        .unwrap();
    let published = node1
        .meta_group()
        .unwrap()
        .state()
        .tablet(tablet_id)
        .unwrap()
        .clone();
    assert_eq!(published, promoted);
    assert_eq!(published.replicas.len(), 4);
    assert_eq!(published.voter_count(), 4);

    // The group serves writes, and the promoted replica answers
    // read-your-writes.
    let leader_group = leader.tablet_group(tablet_id).unwrap();
    let receipt = propose_write(leader_group, 1).await;
    let token = session_token(leader_group, &receipt);
    let watermark = node4
        .tablet_group(tablet_id)
        .unwrap()
        .consistent_read(&ReadConsistency::ReadYourWrites { token }, &control)
        .await
        .unwrap();
    assert!(watermark.position.index >= receipt.position.index);

    node4.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node1.shutdown().await.unwrap();
}

#[tokio::test]
async fn runtime_failure_modes_fail_closed() {
    let tmp = TempDir::new().unwrap();
    let control = ExecutionControl::default();

    // Starting on an unprovisioned directory fails closed (S2A-001).
    let config = NodeRuntimeConfig::new(tmp.path().join("unprovisioned"), free_addr());
    assert!(matches!(
        NodeRuntime::start(config).await,
        Err(RuntimeError::Cluster(
            mongreldb_cluster::node::ClusterError::NotInitialized
        ))
    ));

    // A one-node runtime (no meta membership).
    let dir = tmp.path().join("node-1");
    let addr = free_addr();
    let identity = provision_node(&dir, &addr, None);
    let peers = vec![(identity.node_id, addr.clone())];
    let mut node = NodeRuntime::start(runtime_config(dir.clone(), &addr, &peers, None))
        .await
        .unwrap();

    let tablet_id = TabletId::new_random();
    let table_id = TableId::new(1);
    let tablet = tablet_descriptor(
        tablet_id,
        RaftGroupId::new_random(),
        table_id,
        vec![voter(identity.node_id, 1)],
        1,
    );
    let partitioning =
        TablePartitioningRecord::automatic_default(table_id, vec![ColumnId::new(1)], 16);

    // The partitioning record must name the descriptor's table.
    let wrong =
        TablePartitioningRecord::automatic_default(TableId::new(2), vec![ColumnId::new(1)], 16);
    assert!(matches!(
        node.create_tablet(&tablet, &wrong, None, false, &control)
            .await,
        Err(RuntimeError::InvalidRequest(_))
    ));
    // A descriptor that does not list this node as a replica is refused
    // (before any layout is allocated).
    let mut foreign = tablet.clone();
    foreign.replicas = vec![voter(NodeId::new_random(), 7)];
    assert!(matches!(
        node.create_tablet(&foreign, &partitioning, None, false, &control)
            .await,
        Err(RuntimeError::InvalidRequest(_))
    ));
    // Publishing without a meta group is refused.
    assert!(matches!(
        node.publish_tablet_descriptor(&tablet, &control).await,
        Err(RuntimeError::InvalidRequest(_))
    ));

    // Create the single-replica tablet; an identical repeat is a no-op, a
    // different descriptor for the same tablet id fails closed.
    let bootstrap = vec![(identity.node_id, addr.clone())];
    node.create_tablet(
        &tablet,
        &partitioning,
        Some(bootstrap.as_slice()),
        false,
        &control,
    )
    .await
    .unwrap();
    node.create_tablet(&tablet, &partitioning, None, false, &control)
        .await
        .unwrap();
    let mut changed = tablet.clone();
    changed.generation = 2;
    assert!(matches!(
        node.create_tablet(&changed, &partitioning, None, false, &control)
            .await,
        Err(RuntimeError::InvalidRequest(_))
    ));
    let status = node.status();
    assert_eq!(status.tablets.len(), 1);
    assert!(status.meta.is_none());

    // A second runtime on the same directory cannot own the same tablet
    // storage core (spec section 12.3, process-local half).
    assert!(matches!(
        NodeRuntime::start(runtime_config(dir.clone(), &free_addr(), &peers, None)).await,
        Err(RuntimeError::Tablet(
            mongreldb_cluster::tablet::TabletError::AlreadyOwned { .. }
        ))
    ));

    node.shutdown().await.unwrap();

    // After shutdown the ownership guard is released: the node reopens.
    let node = NodeRuntime::start(runtime_config(dir, &addr, &peers, None))
        .await
        .unwrap();
    assert_eq!(node.tablet_ids(), vec![tablet_id]);
    node.shutdown().await.unwrap();
}
