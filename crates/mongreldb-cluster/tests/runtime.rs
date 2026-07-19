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
use mongreldb_cluster::merge::{MergeError, MergePhase};
use mongreldb_cluster::meta::{
    DatabaseDescriptor, DatabaseState, MetaCommand, MetaError, MetaRejectionReason,
    TableSchemaRecord,
};
use mongreldb_cluster::network::{TcpTransport, TransportConfig, TransportSecurity};
use mongreldb_cluster::node::{
    BuildVersion, Locality, NodeCapacity, NodeDescriptor, NodeIdentity, NodeState, VersionInfo,
};
use mongreldb_cluster::runtime::{
    GroupTiming, MetaMembership, NodeRuntime, NodeRuntimeConfig, RuntimeError,
};
use mongreldb_cluster::split::{retry_guidance, RetryGuidance, SplitError, SplitPhase};
use mongreldb_cluster::tablet::{
    check_generation, find_tablet_for_key, Bound, ColumnId, Key, PartitionBounds,
    ReplicaDescriptor, ReplicaRole, RoutingError, TablePartitioningRecord, TabletDescriptor,
    TabletState,
};
use mongreldb_consensus::group::{ConsensusGroup, GroupCommitReceipt};
use mongreldb_consensus::identity::{raft_node_id, CommandKind, RaftNodeId};
use mongreldb_consensus::read::{ReadConsistency, SessionToken};
use mongreldb_log::commit_log::ExecutionControl;
use mongreldb_log::envelope::CommandEnvelope;
use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::{
    ClusterId, DatabaseId, MetadataVersion, NodeId, RaftGroupId, SchemaVersion, TableId, TabletId,
};
use std::collections::{BTreeMap, BTreeSet};
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
        heartbeat_interval: Duration::from_millis(100),
        election_timeout_min: Duration::from_millis(300),
        election_timeout_max: Duration::from_millis(600),
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
        database_id: DatabaseId::ZERO,
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

async fn create_bootstrapped_tablet(
    nodes: &mut [&mut NodeRuntime],
    tablet: &TabletDescriptor,
    partitioning: &TablePartitioningRecord,
    peers: &[(NodeId, String)],
    control: &ExecutionControl,
) {
    let deadline = std::time::Instant::now() + LEADER_TIMEOUT;
    loop {
        let leader = nodes[0]
            .meta_group()
            .unwrap()
            .group()
            .wait_leader(LEADER_TIMEOUT)
            .await
            .unwrap();
        let runtime = nodes
            .iter()
            .position(|node| raft_node_id(&node.identity().node_id) == leader)
            .expect("the meta leader is one of the runtimes");
        match nodes[runtime]
            .create_tablet(tablet, partitioning, Some(peers), true, control)
            .await
        {
            Ok(()) => return,
            Err(error) if is_meta_not_leader(&error) && std::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => panic!("create_tablet failed: {error}"),
        }
    }
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

async fn publish_tablet_descriptor_on_meta_leader(
    nodes: &[&NodeRuntime],
    descriptor: &TabletDescriptor,
    control: &ExecutionControl,
) {
    let deadline = std::time::Instant::now() + LEADER_TIMEOUT;
    loop {
        let leader = nodes[0]
            .meta_group()
            .unwrap()
            .group()
            .wait_leader(LEADER_TIMEOUT)
            .await
            .unwrap();
        let runtime = nodes
            .iter()
            .find(|node| raft_node_id(&node.identity().node_id) == leader)
            .expect("the meta leader is one of the runtimes");
        match runtime.publish_tablet_descriptor(descriptor, control).await {
            Ok(_) => return,
            Err(error) if is_meta_not_leader(&error) && std::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => panic!("publish_tablet_descriptor failed: {error}"),
        }
    }
}

async fn wait_for_tablet_descriptor(node: &NodeRuntime, tablet_id: TabletId) -> TabletDescriptor {
    let deadline = std::time::Instant::now() + LEADER_TIMEOUT;
    loop {
        if let Some(descriptor) = node
            .meta_group()
            .unwrap()
            .state()
            .tablet(tablet_id)
            .cloned()
        {
            return descriptor;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "tablet descriptor did not reach node {}",
            node.identity().node_id
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The heavy multi-node TCP tests run one at a time: parallel 3-node
/// clusters with sub-second election timers starve each other's heartbeats
/// under full-suite load and churn leadership, which the protocols tolerate
/// but these fixed-driver flows should not have to absorb.
static SERIAL_CLUSTER_TESTS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Acquires the multi-node-cluster test lock (see [`SERIAL_CLUSTER_TESTS`]).
async fn serial_cluster_lock() -> tokio::sync::MutexGuard<'static, ()> {
    SERIAL_CLUSTER_TESTS.lock().await
}

#[tokio::test]
async fn three_node_runtime_cluster_end_to_end() {
    let _serial = serial_cluster_lock().await;
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
    create_bootstrapped_tablet(
        &mut [&mut node1, &mut node2, &mut node3],
        &tablet,
        &partitioning,
        peers.as_slice(),
        &control,
    )
    .await;

    // The meta descriptor reflects the tablet and its replicas.
    let published = wait_for_tablet_descriptor(&node1, tablet_id).await;
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
    let _serial = serial_cluster_lock().await;
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
    create_bootstrapped_tablet(
        &mut [&mut node1, &mut node2, &mut node3],
        &tablet,
        &partitioning,
        &peers[..3],
        &control,
    )
    .await;

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
    let mut leader = tablet_leader(&nodes, tablet_id).await;
    while let Err(error) = leader
        .add_tablet_replica(tablet_id, learner, &addrs[3])
        .await
    {
        if !is_meta_not_leader(&error) {
            panic!("add_tablet_replica failed: {error}");
        }
        leader = tablet_leader(&nodes, tablet_id).await;
    }

    // Publish the promoted descriptor (node 4 a voter, generation 2) and
    // confirm the meta state reflects it.
    let mut promoted = joined.clone();
    promoted
        .replicas
        .iter_mut()
        .find(|replica| replica.node_id == identities[3].node_id)
        .unwrap()
        .role = ReplicaRole::Voter;
    publish_tablet_descriptor_on_meta_leader(&nodes, &promoted, &control).await;
    let published = wait_for_tablet_descriptor(&node1, tablet_id).await;
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
    let _serial = serial_cluster_lock().await;
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

// ---------------------------------------------------------------------------
// Split/merge integration (spec sections 12.5-12.6)
// ---------------------------------------------------------------------------

fn key(bytes: &[u8]) -> Key {
    Key::from_bytes(bytes.to_vec())
}

fn bounds(low: &[u8], high: &[u8]) -> PartitionBounds {
    PartitionBounds::new(Bound::Included(key(low)), Bound::Excluded(key(high))).unwrap()
}

/// A provisioned multi-node cluster directory set plus the wiring to start
/// and restart runtimes on it.
struct ClusterFixture {
    _tmp: TempDir,
    dirs: Vec<PathBuf>,
    addrs: Vec<String>,
    identities: Vec<NodeIdentity>,
    peers: Vec<(NodeId, String)>,
    meta_group_id: RaftGroupId,
}

impl ClusterFixture {
    fn new(count: usize) -> Self {
        let tmp = TempDir::new().unwrap();
        let dirs: Vec<PathBuf> = (1..=count)
            .map(|index| tmp.path().join(format!("node-{index}")))
            .collect();
        let addrs: Vec<String> = (1..=count).map(|_| free_addr()).collect();
        let identity1 = provision_node(&dirs[0], &addrs[0], None);
        let invite = (identity1.cluster_id, addrs.clone());
        let mut identities = vec![identity1];
        for index in 1..count {
            identities.push(provision_node(&dirs[index], &addrs[index], Some(&invite)));
        }
        let peers: Vec<(NodeId, String)> = identities
            .iter()
            .zip(&addrs)
            .map(|(identity, address)| (identity.node_id, address.clone()))
            .collect();
        Self {
            _tmp: tmp,
            dirs,
            addrs,
            identities,
            peers,
            meta_group_id: RaftGroupId::new_random(),
        }
    }

    fn config(&self, index: usize, bootstrap: bool) -> NodeRuntimeConfig {
        runtime_config(
            self.dirs[index].clone(),
            &self.addrs[index],
            &self.peers,
            Some(MetaMembership {
                meta_group_id: self.meta_group_id,
                bootstrap_voters: bootstrap
                    .then(|| vec![(self.identities[index].node_id, self.addrs[index].clone())]),
            }),
        )
    }

    /// Starts every node: node 1 bootstraps the meta group, the rest join
    /// through the membership workflow.
    async fn start_all(&self) -> Vec<NodeRuntime> {
        let mut nodes = Vec::new();
        for index in 0..self.dirs.len() {
            nodes.push(
                NodeRuntime::start(self.config(index, index == 0))
                    .await
                    .unwrap(),
            );
        }
        let control = ExecutionControl::default();
        let meta = nodes[0].meta_group().unwrap();
        meta.group().wait_leader(LEADER_TIMEOUT).await.unwrap();
        meta.propose(
            command_id(1),
            MetaCommand::RegisterNode {
                descriptor: descriptor(self.identities[0].node_id, &self.addrs[0]),
            },
            &control,
        )
        .await
        .unwrap();
        for index in 1..self.dirs.len() {
            meta.add_member(
                &descriptor(self.identities[index].node_id, &self.addrs[index]),
                &control,
            )
            .await
            .unwrap();
        }
        nodes
    }

    /// Restarts one node from its durable state (no bootstrap).
    async fn restart(&self, index: usize) -> NodeRuntime {
        NodeRuntime::start(self.config(index, false)).await.unwrap()
    }
}

/// The index of the node currently leading the meta group (all nodes agree).
async fn meta_leader_index(nodes: &[NodeRuntime]) -> usize {
    let deadline = std::time::Instant::now() + LEADER_TIMEOUT;
    loop {
        let mut votes = BTreeMap::<RaftNodeId, usize>::new();
        for node in nodes {
            if let Some(leader) = node.meta_group().unwrap().group().metrics().current_leader {
                *votes.entry(leader).or_default() += 1;
            }
        }
        if let Some((leader, count)) = votes.iter().next_back() {
            if *count == nodes.len() {
                return nodes
                    .iter()
                    .position(|node| raft_node_id(&node.identity().node_id) == *leader)
                    .expect("the meta leader is one of the runtimes");
            }
        }
        assert!(std::time::Instant::now() < deadline, "no meta leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Transfers meta leadership onto `index` when it is not already the leader
/// (a restarted split driver must lead the meta group to propose).
async fn ensure_meta_leader(nodes: &[NodeRuntime], index: usize) {
    let target = raft_node_id(&nodes[index].identity().node_id);
    let leader = nodes[index]
        .meta_group()
        .unwrap()
        .group()
        .wait_leader(LEADER_TIMEOUT)
        .await
        .unwrap();
    if leader == target {
        return;
    }
    let leader_index = nodes
        .iter()
        .position(|node| raft_node_id(&node.identity().node_id) == leader)
        .expect("the meta leader is one of the runtimes");
    nodes[leader_index]
        .meta_group()
        .unwrap()
        .group()
        .transfer_leader(target, LEADER_TIMEOUT)
        .await
        .unwrap();
}

/// Transfers a tablet group's leadership onto `index` when needed (child
/// state installs propose through the driver's local group).
async fn ensure_tablet_leader(nodes: &[NodeRuntime], tablet_id: TabletId, index: usize) {
    let target = nodes[index].tablet_group(tablet_id).unwrap().node_id();
    let leader = nodes[index]
        .tablet_group(tablet_id)
        .unwrap()
        .wait_leader(LEADER_TIMEOUT)
        .await
        .unwrap();
    if leader == target {
        return;
    }
    let leader_index = nodes
        .iter()
        .position(|node| {
            node.tablet_group(tablet_id)
                .is_some_and(|group| group.node_id() == leader)
        })
        .expect("the tablet leader is one of the runtimes");
    nodes[leader_index]
        .tablet_group(tablet_id)
        .unwrap()
        .transfer_leader(target, LEADER_TIMEOUT)
        .await
        .unwrap();
}

/// Creates one tablet spanning every node: replica raft ids from the
/// meta-owned allocator, local replicas everywhere, bootstrap + publish on
/// the meta leader.
async fn create_tablet_on_cluster(
    nodes: &mut [NodeRuntime],
    table_id: TableId,
    partition: PartitionBounds,
) -> TabletDescriptor {
    let control = ExecutionControl::default();
    let leader = meta_leader_index(nodes).await;
    let meta = nodes[leader].meta_group().unwrap();
    let raft_ids = meta.allocate_raft_node_ids(3, &control).await.unwrap();
    let tablet = TabletDescriptor {
        tablet_id: TabletId::new_random(),
        database_id: mongreldb_types::ids::DatabaseId::ZERO,
        table_id,
        raft_group_id: RaftGroupId::new_random(),
        partition,
        replicas: nodes
            .iter()
            .enumerate()
            .map(|(index, node)| ReplicaDescriptor {
                node_id: node.identity().node_id,
                role: ReplicaRole::Voter,
                raft_node_id: raft_ids[index],
            })
            .collect(),
        leader_hint: None,
        generation: 1,
        state: TabletState::Active,
    };
    let partitioning =
        TablePartitioningRecord::automatic_default(table_id, vec![ColumnId::new(1)], 16);
    // Replicas first; the bootstrapping node last.
    for node in nodes.iter_mut().skip(1) {
        node.create_tablet(&tablet, &partitioning, None, false, &control)
            .await
            .unwrap();
    }
    let peers: Vec<(NodeId, String)> = tablet
        .replicas
        .iter()
        .map(|replica| {
            let address = nodes
                .iter()
                .find(|node| node.identity().node_id == replica.node_id)
                .unwrap()
                .rpc_address()
                .to_owned();
            (replica.node_id, address)
        })
        .collect();
    let mut runtimes: Vec<&mut NodeRuntime> = nodes.iter_mut().collect();
    create_bootstrapped_tablet(
        &mut runtimes,
        &tablet,
        &partitioning,
        peers.as_slice(),
        &control,
    )
    .await;
    nodes[0]
        .tablet_group(tablet.tablet_id)
        .unwrap()
        .wait_leader(LEADER_TIMEOUT)
        .await
        .unwrap();
    tablet
}

/// Writes rows through the tablet group's current leader, returning the
/// commit receipt (its position gates replica catch-up waits).
async fn write_rows(
    nodes: &[NodeRuntime],
    tablet_id: TabletId,
    rows: &[(Key, Vec<u8>)],
) -> GroupCommitReceipt {
    let deadline = std::time::Instant::now() + LEADER_TIMEOUT;
    loop {
        let refs: Vec<&NodeRuntime> = nodes.iter().collect();
        let leader = tablet_leader(&refs, tablet_id).await;
        match leader
            .write_tablet_rows(tablet_id, rows, &ExecutionControl::default())
            .await
        {
            Ok(receipt) => return receipt,
            Err(_) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => panic!("write to tablet {tablet_id} failed: {error}"),
        }
    }
}

/// Polls until `node`'s local ledger of `tablet_id` holds exactly `expected`
/// (replication and apply are asynchronous on followers).
async fn wait_rows(node: &NodeRuntime, tablet_id: TabletId, expected: &BTreeMap<Key, Vec<u8>>) {
    let deadline = std::time::Instant::now() + LEADER_TIMEOUT;
    loop {
        let rows = node.tablet_rows(tablet_id).unwrap();
        if rows == *expected {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "tablet {tablet_id} rows did not settle: have {rows:?}, want {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The meta state's tablet descriptors, in tablet-id order.
fn meta_descriptors(node: &NodeRuntime) -> Vec<TabletDescriptor> {
    node.meta_group()
        .unwrap()
        .state()
        .tablets
        .values()
        .map(|record| record.descriptor.clone())
        .collect()
}

/// The local meta apply watermark.
fn meta_version(node: &NodeRuntime) -> MetadataVersion {
    node.meta_group().unwrap().metadata_version()
}

/// Polls until `node`'s meta apply watermark reaches `version` (meta
/// replication to a follower is asynchronous; the driver's propose returns
/// once ITS local apply landed).
async fn wait_meta_caught_up(node: &NodeRuntime, version: MetadataVersion) {
    let deadline = std::time::Instant::now() + LEADER_TIMEOUT;
    loop {
        if meta_version(node) >= version {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "meta state did not catch up to {version:?} (at {:?})",
            meta_version(node)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Sort key for a partition's low endpoint (Unbounded sorts first).
fn low_key(partition: &PartitionBounds) -> Vec<u8> {
    match &partition.low {
        Bound::Included(key) | Bound::Excluded(key) => key.as_bytes().to_vec(),
        Bound::Unbounded => Vec::new(),
    }
}

fn row_map(rows: &[(&[u8], &[u8])]) -> BTreeMap<Key, Vec<u8>> {
    rows.iter()
        .map(|(key, value)| (Key::from_bytes(key.to_vec()), value.to_vec()))
        .collect()
}

#[tokio::test]
async fn split_end_to_end_on_three_node_cluster() {
    let _serial = serial_cluster_lock().await;
    let fixture = ClusterFixture::new(3);
    let mut nodes = fixture.start_all().await;
    let control = ExecutionControl::default();
    let database_id = DatabaseId::new_random();
    let table_id = TableId::new(1);
    publish_schema(&nodes[0], database_id, table_id).await;
    let source = create_tablet_on_cluster(&mut nodes, table_id, bounds(b"a", b"z")).await;
    let g = source.generation;

    // Data written before the split.
    let pre_split = row_map(&[
        (b"apple", b"v-apple"),
        (b"fig", b"v-fig"),
        (b"mango", b"v-mango"),
        (b"yam", b"v-yam"),
    ]);
    write_rows(
        &nodes,
        source.tablet_id,
        &pre_split.clone().into_iter().collect::<Vec<_>>(),
    )
    .await;
    for node in &nodes {
        wait_rows(node, source.tablet_id, &pre_split).await;
    }

    let mut driver = meta_leader_index(&nodes).await;
    // Step 1: the source is marked Splitting at g + 1; requests holding g
    // redirect (spec section 12.5 step 9).
    let (phase, _) = split_step_retry(
        &mut nodes,
        &mut driver,
        source.tablet_id,
        Some(key(b"m")),
        &control,
    )
    .await;
    assert_eq!(phase, SplitPhase::MarkedSplitting);
    let splitting = nodes[driver]
        .meta_group()
        .unwrap()
        .state()
        .tablet(source.tablet_id)
        .unwrap()
        .clone();
    assert_eq!(splitting.state, TabletState::Splitting);
    assert_eq!(splitting.generation, g + 1);
    let error = check_generation(&splitting, g).unwrap_err();
    assert_eq!(
        error,
        RoutingError::TabletSplit {
            tablet_id: source.tablet_id,
            used_generation: g,
            current_generation: g + 1,
        }
    );
    assert_eq!(
        retry_guidance(&error),
        RetryGuidance::AwaitSplitPublish {
            tablet_id: source.tablet_id,
        }
    );

    // Step 2: the children are created — Creating, never routable.
    let (phase, _) =
        split_step_retry(&mut nodes, &mut driver, source.tablet_id, None, &control).await;
    assert_eq!(phase, SplitPhase::ChildrenCreated);
    let descriptors = meta_descriptors(&nodes[driver]);
    assert_eq!(
        find_tablet_for_key(&descriptors, table_id, &key(b"fig"))
            .unwrap()
            .tablet_id,
        source.tablet_id
    );
    // The peers adopt the child replicas; the child groups elect. (Meta
    // replication to the peers is asynchronous: wait for the children to
    // arrive before the sync asserts the exact outcome.)
    let version = meta_version(&nodes[driver]);
    for (index, node) in nodes.iter_mut().enumerate() {
        if index == driver {
            continue;
        }
        wait_meta_caught_up(node, version).await;
        let report = node.sync_hosted_tablets(&control).await.unwrap();
        assert_eq!(report.created.len(), 2);
    }
    let children: Vec<TabletDescriptor> = {
        let mut creating: Vec<TabletDescriptor> = meta_descriptors(&nodes[driver])
            .into_iter()
            .filter(|descriptor| descriptor.state == TabletState::Creating)
            .collect();
        creating.sort_by_key(|descriptor| low_key(&descriptor.partition));
        creating
    };
    assert_eq!(children.len(), 2);
    for child in &children {
        ensure_tablet_leader(&nodes, child.tablet_id, driver).await;
    }

    // Steps 3-5: pin, build, catch up. A write committed mid-split (after
    // the pin timestamp) streams to its child as a delta.
    let (phase, _) =
        split_step_retry(&mut nodes, &mut driver, source.tablet_id, None, &control).await;
    assert_eq!(phase, SplitPhase::SnapshotPinned);
    let mid_split = row_map(&[(b"banana", b"v-banana@mid")]);
    let receipt = write_rows(
        &nodes,
        source.tablet_id,
        &mid_split.clone().into_iter().collect::<Vec<_>>(),
    )
    .await;
    // The driver's local ledger must apply it before the catch-up reads deltas.
    nodes[driver]
        .tablet_group(source.tablet_id)
        .unwrap()
        .wait_applied_index(receipt.position.index, LEADER_TIMEOUT)
        .await
        .unwrap();
    let (phase, _) =
        split_step_retry(&mut nodes, &mut driver, source.tablet_id, None, &control).await;
    assert_eq!(phase, SplitPhase::ChildrenBuilt);
    let (phase, _) =
        split_step_retry(&mut nodes, &mut driver, source.tablet_id, None, &control).await;
    assert_eq!(phase, SplitPhase::CaughtUp);

    // Step 6: the atomic publication — one meta command flips the children
    // Active and the source Retiring at one generation.
    let (phase, published) =
        split_step_retry(&mut nodes, &mut driver, source.tablet_id, None, &control).await;
    assert_eq!(phase, SplitPhase::Published);
    let command = published.expect("the publish step yields its command");
    assert_eq!(command.publish_generation(), g + 2);
    let meta_state = nodes[driver].meta_group().unwrap().state();
    let retiring = meta_state.tablet(source.tablet_id).unwrap();
    assert_eq!(retiring.state, TabletState::Retiring);
    assert_eq!(retiring.generation, g + 2);
    for child in &command.children {
        let stored = meta_state.tablet(child.tablet_id).unwrap();
        assert_eq!(stored.state, TabletState::Active);
        assert_eq!(stored.generation, g + 2);
        assert!(stored
            .replicas
            .iter()
            .all(|replica| replica.role == ReplicaRole::Voter));
    }
    // Routing resolves every key to its child; stale generations redirect.
    let descriptors = meta_descriptors(&nodes[driver]);
    assert_eq!(
        find_tablet_for_key(&descriptors, table_id, &key(b"banana"))
            .unwrap()
            .tablet_id,
        command.children[0].tablet_id
    );
    assert_eq!(
        find_tablet_for_key(&descriptors, table_id, &key(b"yam"))
            .unwrap()
            .tablet_id,
        command.children[1].tablet_id
    );
    assert!(matches!(
        check_generation(retiring, g),
        Err(RoutingError::TabletMoved { .. })
    ));
    // The child replica raft ids came from the meta-owned allocator: fresh
    // and distinct from the source's (and from each other).
    let mut raft_ids: BTreeSet<u64> = source
        .replicas
        .iter()
        .map(|replica| replica.raft_node_id)
        .collect();
    for child in &command.children {
        for replica in &child.replicas {
            assert!(
                raft_ids.insert(replica.raft_node_id),
                "raft id {} reused",
                replica.raft_node_id
            );
        }
    }

    // Step 7: the source is retired and removed; peers tear their replicas
    // down on the next sync.
    let (phase, _) =
        split_step_retry(&mut nodes, &mut driver, source.tablet_id, None, &control).await;
    assert_eq!(phase, SplitPhase::SourceRetired);
    assert!(nodes[driver]
        .meta_group()
        .unwrap()
        .state()
        .tablet(source.tablet_id)
        .is_none());
    assert!(!fixture.dirs[driver]
        .join("tablets")
        .join(source.tablet_id.to_hex())
        .exists());
    // The source's removal must replicate to each peer's meta before its
    // sync tears the local replica down.
    let version = meta_version(&nodes[driver]);
    for (index, node) in nodes.iter_mut().enumerate() {
        if index == driver {
            continue;
        }
        wait_meta_caught_up(node, version).await;
        let report = node.sync_hosted_tablets(&control).await.unwrap();
        assert_eq!(report.torn_down, vec![source.tablet_id]);
    }
    for dir in &fixture.dirs {
        assert!(!dir.join("tablets").join(source.tablet_id.to_hex()).exists());
    }

    // Zero loss, zero duplication: the pre-split data and the mid-split
    // delta are fully readable from the children on EVERY replica.
    let expected_lower = row_map(&[
        (b"apple", b"v-apple"),
        (b"banana", b"v-banana@mid"),
        (b"fig", b"v-fig"),
    ]);
    let expected_upper = row_map(&[(b"mango", b"v-mango"), (b"yam", b"v-yam")]);
    let all: BTreeSet<Key> = pre_split.keys().chain(mid_split.keys()).cloned().collect();
    assert_eq!(
        expected_lower
            .keys()
            .chain(expected_upper.keys())
            .cloned()
            .collect::<BTreeSet<_>>(),
        all
    );
    for node in &nodes {
        wait_rows(node, command.children[0].tablet_id, &expected_lower).await;
        wait_rows(node, command.children[1].tablet_id, &expected_upper).await;
    }

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

/// Drives the split on the meta leader from the current persisted phase to
/// `target`, syncing the peers' hosted tablets at the children-created and
/// published boundaries (the points the protocol needs them).
///
/// True when a runtime error is the meta plane reporting this node is not the
/// leader (spec section 11.7): proposals must be retried on the current
/// leader, never unwrapped — elections churn under parallel load.
fn is_meta_not_leader(error: &RuntimeError) -> bool {
    matches!(
        error,
        RuntimeError::Split(SplitError::MetaPlane(MetaRejectionReason::NotLeader { .. }))
            | RuntimeError::Merge(MergeError::MetaPlane(MetaRejectionReason::NotLeader { .. }))
            | RuntimeError::Meta(MetaError::Consensus(
                mongreldb_consensus::error::ConsensusError::NotLeader { .. },
            ))
            | RuntimeError::Consensus(mongreldb_consensus::error::ConsensusError::NotLeader { .. })
    )
}

/// One split step with leader-stabilizing retry: the split executor's
/// progress lives on the driver node's local disk, so on `NotLeader` the test
/// re-elects leadership onto the SAME driver (never hops nodes) and resumes —
/// the executor's persisted phases make the resume exact.
async fn split_step_retry(
    nodes: &mut [NodeRuntime],
    driver: &mut usize,
    tablet_id: TabletId,
    split_key: Option<Key>,
    control: &ExecutionControl,
) -> (
    SplitPhase,
    Option<mongreldb_cluster::split::SplitPublishCommand>,
) {
    loop {
        match nodes[*driver]
            .split_step(tablet_id, split_key.clone(), control)
            .await
        {
            Ok(step) => return step,
            Err(error) if is_meta_not_leader(&error) => {
                ensure_meta_leader(nodes, *driver).await;
            }
            Err(error) => panic!("split_step failed: {error}"),
        }
    }
}

/// One merge step with leader-following retry (idempotent, like the split).
async fn merge_step_retry(
    nodes: &mut [NodeRuntime],
    driver: &mut usize,
    first: TabletId,
    second: TabletId,
    control: &ExecutionControl,
) -> (
    MergePhase,
    Option<mongreldb_cluster::merge::MergePublishCommand>,
) {
    loop {
        match nodes[*driver].merge_step(first, second, control).await {
            Ok(step) => return step,
            Err(error) if is_meta_not_leader(&error) => {
                ensure_meta_leader(nodes, *driver).await;
            }
            Err(error) => panic!("merge_step failed: {error}"),
        }
    }
}

/// One abort step with leader-following retry.
async fn abort_split_retry(
    nodes: &mut [NodeRuntime],
    driver: &mut usize,
    tablet_id: TabletId,
    control: &ExecutionControl,
) -> mongreldb_cluster::split::SplitAbortReport {
    loop {
        match nodes[*driver].abort_split(tablet_id, control).await {
            Ok(report) => return report,
            Err(error) if is_meta_not_leader(&error) => {
                ensure_meta_leader(nodes, *driver).await;
            }
            Err(error) => panic!("abort_split failed: {error}"),
        }
    }
}

async fn drive_split_to(
    nodes: &mut [NodeRuntime],
    driver: usize,
    source: &TabletDescriptor,
    split_key: Option<Key>,
    target: SplitPhase,
) -> Option<mongreldb_cluster::split::SplitPublishCommand> {
    let mut driver = driver;
    let control = ExecutionControl::default();
    let mut published = None;
    loop {
        // Child state installs and catch-up deltas propose through the
        // driver's local child groups. Pull leadership onto the driver
        // before the first step after a restart, not after that step has
        // already tried to propose.
        for child in active_or_creating(nodes, driver, source) {
            ensure_tablet_leader(nodes, child, driver).await;
        }
        let (phase, command) = split_step_retry(
            nodes,
            &mut driver,
            source.tablet_id,
            split_key.clone(),
            &control,
        )
        .await;
        if command.is_some() {
            published = command;
        }
        match phase {
            SplitPhase::ChildrenCreated | SplitPhase::Published => {
                let version = meta_version(&nodes[driver]);
                for (index, node) in nodes.iter_mut().enumerate() {
                    if index == driver {
                        continue;
                    }
                    wait_meta_caught_up(node, version).await;
                    node.sync_hosted_tablets(&control).await.unwrap();
                }
            }
            _ => {}
        }
        if phase == target {
            return published;
        }
    }
}

/// The not-yet-removed tablets derived from `source`'s table (Creating or
/// Active), as tablet ids, lower bounds first.
fn active_or_creating(
    nodes: &[NodeRuntime],
    driver: usize,
    source: &TabletDescriptor,
) -> Vec<TabletId> {
    let mut derived: Vec<TabletDescriptor> = meta_descriptors(&nodes[driver])
        .into_iter()
        .filter(|descriptor| {
            descriptor.table_id == source.table_id && descriptor.tablet_id != source.tablet_id
        })
        .collect();
    derived.sort_by_key(|descriptor| low_key(&descriptor.partition));
    derived
        .into_iter()
        .map(|descriptor| descriptor.tablet_id)
        .collect()
}

#[tokio::test]
async fn split_crash_at_each_phase_resumes_via_the_runtime() {
    let _serial = serial_cluster_lock().await;
    // Crash points: every durable phase with remaining work. The final
    // phase's teardown removes the record itself, so a "crash" there is the
    // completed split — covered by the end-to-end test.
    for crash_after in [
        SplitPhase::MarkedSplitting,
        SplitPhase::ChildrenCreated,
        SplitPhase::SnapshotPinned,
        SplitPhase::ChildrenBuilt,
        SplitPhase::CaughtUp,
        SplitPhase::Published,
    ] {
        let fixture = ClusterFixture::new(3);
        let mut nodes = fixture.start_all().await;
        let control = ExecutionControl::default();
        let database_id = DatabaseId::new_random();
        let table_id = TableId::new(1);
        publish_schema(&nodes[0], database_id, table_id).await;
        let source = create_tablet_on_cluster(&mut nodes, table_id, bounds(b"a", b"z")).await;
        let rows = row_map(&[(b"apple", b"v-apple"), (b"yam", b"v-yam")]);
        write_rows(
            &nodes,
            source.tablet_id,
            &rows.clone().into_iter().collect::<Vec<_>>(),
        )
        .await;

        let mut driver = meta_leader_index(&nodes).await;
        // Begin the split (records Started), then drive to the crash point.
        let (phase, _) = split_step_retry(
            &mut nodes,
            &mut driver,
            source.tablet_id,
            Some(key(b"m")),
            &control,
        )
        .await;
        assert_eq!(phase, SplitPhase::MarkedSplitting);
        let pre_crash = if crash_after != SplitPhase::MarkedSplitting {
            drive_split_to(&mut nodes, driver, &source, None, crash_after).await
        } else {
            None
        };

        // The crash: the driver stops WITHOUT a graceful shutdown (raft
        // tasks die in place; only fsynced state survives) and restarts
        // from its durable state.
        let crashed = nodes.remove(driver);
        crashed.crash().await;
        let restarted = fixture.restart(driver).await;
        nodes.insert(driver, restarted);
        ensure_meta_leader(&nodes, driver).await;

        // The runtime resumes the split from the persisted phase and drives
        // it to completion.
        let published =
            drive_split_to(&mut nodes, driver, &source, None, SplitPhase::SourceRetired)
                .await
                .or(pre_crash)
                .expect("the resumed split re-publishes its command");
        assert!(
            nodes[driver]
                .meta_group()
                .unwrap()
                .state()
                .tablet(source.tablet_id)
                .is_none(),
            "crash after {crash_after}: source still in meta"
        );
        // Data written before the split is fully readable from the children
        // on every replica.
        let expected_lower = row_map(&[(b"apple", b"v-apple")]);
        let expected_upper = row_map(&[(b"yam", b"v-yam")]);
        for node in nodes.iter() {
            wait_rows(node, published.children[0].tablet_id, &expected_lower).await;
            wait_rows(node, published.children[1].tablet_id, &expected_upper).await;
        }
        for node in nodes {
            node.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn split_abort_mid_split_restores_the_source() {
    let _serial = serial_cluster_lock().await;
    let fixture = ClusterFixture::new(3);
    let mut nodes = fixture.start_all().await;
    let control = ExecutionControl::default();
    let database_id = DatabaseId::new_random();
    let table_id = TableId::new(1);
    publish_schema(&nodes[0], database_id, table_id).await;
    let source = create_tablet_on_cluster(&mut nodes, table_id, bounds(b"a", b"z")).await;
    let rows = row_map(&[(b"apple", b"v-apple"), (b"yam", b"v-yam")]);
    write_rows(
        &nodes,
        source.tablet_id,
        &rows.clone().into_iter().collect::<Vec<_>>(),
    )
    .await;

    let mut driver = meta_leader_index(&nodes).await;
    // Mark and create the children, then abort mid-split.
    drive_split_to(
        &mut nodes,
        driver,
        &source,
        Some(key(b"m")),
        SplitPhase::ChildrenCreated,
    )
    .await;
    let report = abort_split_retry(&mut nodes, &mut driver, source.tablet_id, &control).await;
    assert_eq!(report.phase, Some(SplitPhase::ChildrenCreated));
    assert_eq!(report.children_removed.len(), 2);
    // The source is Active again one generation above the mark; the children
    // are gone from the meta state and the local disk.
    let meta_state = nodes[driver].meta_group().unwrap().state();
    let restored = meta_state.tablet(source.tablet_id).unwrap();
    assert_eq!(restored.state, TabletState::Active);
    assert_eq!(restored.generation, source.generation + 2);
    assert_eq!(meta_state.tablets.len(), 1);
    assert_eq!(nodes[driver].tablet_ids(), vec![source.tablet_id]);
    assert_eq!(
        nodes[driver].tablet_descriptor(source.tablet_id).unwrap(),
        restored
    );
    for child in &report.children_removed {
        assert!(!fixture.dirs[driver]
            .join("tablets")
            .join(child.to_hex())
            .exists());
    }
    // The persisted progress record is cleared.
    assert!(!fixture.dirs[driver]
        .join("tablets")
        .join(source.tablet_id.to_hex())
        .join("split.json")
        .exists());
    // The abort is idempotent: a second drive is a no-op.
    let second = abort_split_retry(&mut nodes, &mut driver, source.tablet_id, &control).await;
    assert_eq!(second.phase, None);
    assert!(second.children_removed.is_empty());
    // The source keeps serving: writes and reads work through the abort.
    let more = row_map(&[(b"fig", b"v-fig@after-abort")]);
    write_rows(
        &nodes,
        source.tablet_id,
        &more.clone().into_iter().collect::<Vec<_>>(),
    )
    .await;
    let mut expected = rows.clone();
    expected.extend(more);
    for node in &nodes {
        wait_rows(node, source.tablet_id, &expected).await;
    }
    // The peers' sync tears down the orphaned child replicas (hosted but no
    // longer in the meta state); nothing is created or refreshed. The
    // children's removal must replicate to each peer's meta first.
    let version = meta_version(&nodes[driver]);
    for (index, node) in nodes.iter_mut().enumerate() {
        if index == driver {
            continue;
        }
        wait_meta_caught_up(node, version).await;
        let mut torn_down = node.sync_hosted_tablets(&control).await.unwrap().torn_down;
        torn_down.sort();
        let mut expected_orphans = report.children_removed.clone();
        expected_orphans.sort();
        assert_eq!(torn_down, expected_orphans);
        assert!(node.tablet_ids().contains(&source.tablet_id));
    }

    // A fresh split of the restored source runs to completion.
    let published = drive_split_to(
        &mut nodes,
        driver,
        &source,
        Some(key(b"m")),
        SplitPhase::SourceRetired,
    )
    .await
    .expect("the second split publishes");
    let expected_lower = row_map(&[(b"apple", b"v-apple"), (b"fig", b"v-fig@after-abort")]);
    let expected_upper = row_map(&[(b"yam", b"v-yam")]);
    for node in &nodes {
        wait_rows(node, published.children[0].tablet_id, &expected_lower).await;
        wait_rows(node, published.children[1].tablet_id, &expected_upper).await;
    }

    // Once a split has published, aborting fails closed.
    let another = create_tablet_on_cluster(&mut nodes, table_id, bounds(b"A", b"Z")).await;
    drive_split_to(
        &mut nodes,
        driver,
        &another,
        Some(key(b"M")),
        SplitPhase::Published,
    )
    .await;
    assert!(matches!(
        nodes[driver].abort_split(another.tablet_id, &control).await,
        Err(RuntimeError::Split(
            mongreldb_cluster::split::SplitError::CannotAbort { .. }
        ))
    ));

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn merge_end_to_end_on_three_node_cluster() {
    let _serial = serial_cluster_lock().await;
    let fixture = ClusterFixture::new(3);
    let mut nodes = fixture.start_all().await;
    let control = ExecutionControl::default();
    let database_id = DatabaseId::new_random();
    let table_id = TableId::new(1);
    publish_schema(&nodes[0], database_id, table_id).await;
    // Two adjacent tablets, each spanning the cluster.
    let first = create_tablet_on_cluster(&mut nodes, table_id, bounds(b"a", b"m")).await;
    let second = create_tablet_on_cluster(&mut nodes, table_id, bounds(b"m", b"z")).await;
    let first_rows = row_map(&[(b"apple", b"v-apple"), (b"fig", b"v-fig")]);
    let second_rows = row_map(&[(b"mango", b"v-mango"), (b"yam", b"v-yam")]);
    write_rows(
        &nodes,
        first.tablet_id,
        &first_rows.clone().into_iter().collect::<Vec<_>>(),
    )
    .await;
    write_rows(
        &nodes,
        second.tablet_id,
        &second_rows.clone().into_iter().collect::<Vec<_>>(),
    )
    .await;
    for node in &nodes {
        wait_rows(node, first.tablet_id, &first_rows).await;
        wait_rows(node, second.tablet_id, &second_rows).await;
    }

    let mut driver = meta_leader_index(&nodes).await;
    // Mark both sources Merging; create the hidden replacement.
    let (phase, _) = merge_step_retry(
        &mut nodes,
        &mut driver,
        first.tablet_id,
        second.tablet_id,
        &control,
    )
    .await;
    assert_eq!(phase, MergePhase::MarkedMerging);
    let (phase, _) = merge_step_retry(
        &mut nodes,
        &mut driver,
        first.tablet_id,
        second.tablet_id,
        &control,
    )
    .await;
    assert_eq!(phase, MergePhase::ReplacementCreated);
    // The peers adopt the replacement; its group elects.
    let version = meta_version(&nodes[driver]);
    for (index, node) in nodes.iter_mut().enumerate() {
        if index == driver {
            continue;
        }
        wait_meta_caught_up(node, version).await;
        let report = node.sync_hosted_tablets(&control).await.unwrap();
        assert_eq!(report.created.len(), 1);
    }
    let replacement_id = {
        let creating: Vec<TabletDescriptor> = meta_descriptors(&nodes[driver])
            .into_iter()
            .filter(|descriptor| descriptor.state == TabletState::Creating)
            .collect();
        assert_eq!(creating.len(), 1);
        creating[0].tablet_id
    };
    ensure_tablet_leader(&nodes, replacement_id, driver).await;
    // Pin, build, catch up, publish.
    for expected in [
        MergePhase::SnapshotsPinned,
        MergePhase::ReplacementBuilt,
        MergePhase::CaughtUp,
    ] {
        let (phase, _) = merge_step_retry(
            &mut nodes,
            &mut driver,
            first.tablet_id,
            second.tablet_id,
            &control,
        )
        .await;
        assert_eq!(phase, expected);
    }
    let (phase, published) = merge_step_retry(
        &mut nodes,
        &mut driver,
        first.tablet_id,
        second.tablet_id,
        &control,
    )
    .await;
    assert_eq!(phase, MergePhase::Published);
    let command = published.expect("the publish step yields its command");
    // Both sources started at generation 1: p = max(1, 1) + 2 = 3.
    assert_eq!(command.publish_generation(), 3);
    let meta_state = nodes[driver].meta_group().unwrap().state();
    let replacement = meta_state.tablet(command.replacement.tablet_id).unwrap();
    assert_eq!(replacement.state, TabletState::Active);
    assert_eq!(replacement.generation, 3);
    assert_eq!(replacement.partition, bounds(b"a", b"z"));
    for source in &command.sources {
        let stored = meta_state.tablet(source.tablet_id).unwrap();
        assert_eq!(stored.state, TabletState::Retiring);
        assert_eq!(stored.generation, 3);
        assert!(matches!(
            check_generation(stored, 1),
            Err(RoutingError::TabletMoved { .. })
        ));
    }
    // Routing resolves every key to the replacement.
    let descriptors = meta_descriptors(&nodes[driver]);
    for probe in ["apple", "fig", "mango", "yam"] {
        assert_eq!(
            find_tablet_for_key(&descriptors, table_id, &key(probe.as_bytes()))
                .unwrap()
                .tablet_id,
            command.replacement.tablet_id
        );
    }
    // Retire the sources; the peers tear their replicas down on sync.
    let (phase, _) = merge_step_retry(
        &mut nodes,
        &mut driver,
        first.tablet_id,
        second.tablet_id,
        &control,
    )
    .await;
    assert_eq!(phase, MergePhase::SourcesRetired);
    for source in &command.sources {
        assert!(nodes[driver]
            .meta_group()
            .unwrap()
            .state()
            .tablet(source.tablet_id)
            .is_none());
    }
    let version = meta_version(&nodes[driver]);
    for (index, node) in nodes.iter_mut().enumerate() {
        if index == driver {
            continue;
        }
        wait_meta_caught_up(node, version).await;
        let mut torn_down = node.sync_hosted_tablets(&control).await.unwrap().torn_down;
        torn_down.sort();
        let mut sources: Vec<TabletId> = command
            .sources
            .iter()
            .map(|source| source.tablet_id)
            .collect();
        sources.sort();
        assert_eq!(torn_down, sources);
    }
    // No data loss: the replacement holds both sources' rows on every replica.
    let mut expected = first_rows.clone();
    expected.extend(second_rows.clone());
    for node in &nodes {
        wait_rows(node, command.replacement.tablet_id, &expected).await;
    }

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}
