//! Cluster node runtime (spec sections 11.1, 12.1-12.4, 12.7 integration).
//!
//! [`NodeRuntime`] is the library form of a running cluster node. Starting a
//! runtime:
//!
//! 1. loads the node's persisted [`NodeIdentity`] (spec section 11.1 — the
//!    node must have been provisioned by `cluster init` / `cluster join`
//!    first);
//! 2. builds the [`TcpTransport`] client and the [`TransportServer`] listener
//!    (spec section 6.7) and feeds the configured membership directory into
//!    the transport's peer table;
//! 3. starts the meta control-plane group when this node is a meta member
//!    ([`MetaMembership`]; [`MetaGroup::create`] / [`MetaGroup::bootstrap`],
//!    spec section 12.1);
//! 4. scans the local tablet layout (spec section 12.3:
//!    `tablets/<tablet-id>/tablet.json`) and reopens every tablet group this
//!    node hosts, one [`ConsensusGroup`] over the consensus engine sink per
//!    group, each registered in the transport's dispatch registry.
//!
//! # Tablet group raft ids
//!
//! A node's meta-group raft id is the projection [`raft_node_id`] of its
//! durable [`NodeId`], so tablet groups must **not** use that projection: a
//! node hosting the meta group and tablet groups would attach two raft nodes
//! under one id to the transport registry. Tablet replica raft ids come from
//! the canonical [`crate::tablet::ReplicaDescriptor::raft_node_id`] (allocated
//! by the meta control plane, spec section 12.1); opening a group fails
//! closed when a replica's raft id is already attached locally.
//!
//! # Engine binding of tablet groups (interim)
//!
//! The consensus engine sink binds each group to a `ClusterReplica` storage
//! core keyed by `(cluster_id, node_id, database_id)`. The tablet descriptor
//! names a table, not a database, and the meta-driven table-to-database
//! resolution lands with distributed DDL (spec section 12.11); until then the
//! runtime derives the sink's database id deterministically from the group's
//! raft group id ([`tablet_database_id`]), which is stable across restarts
//! and identical on every replica of the group (so snapshot validation
//! agrees).
//!
//! # Graceful shutdown
//!
//! [`NodeRuntime::shutdown`] stops accepting RPCs first (the listener drains
//! in-flight connections within its configured grace), then shuts down every
//! tablet group and the meta group (each detaches from the transport
//! registry, fsyncs, and stops its raft task), and finally releases the
//! process-local tablet ownership guards (spec section 12.3).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mongreldb_consensus::engine_sink::{open_engine_sink, EngineGroupConfig, EngineSinkError};
use mongreldb_consensus::error::ConsensusError;
use mongreldb_consensus::group::{ConsensusGroup, GroupConfig, GroupMetrics};
use mongreldb_consensus::identity::raft_node_id;
use mongreldb_consensus::state_machine::ApplySink;
use mongreldb_log::commit_log::{ExecutionControl, LogPosition};
use mongreldb_types::ids::{DatabaseId, MetadataVersion, NodeId, RaftGroupId, TabletId};
use openraft::BasicNode;

use crate::meta::{MetaCommand, MetaError, MetaGroup, MetaGroupConfig};
use crate::network::{
    PeerEndpoint, TcpTransport, TransportConfig, TransportError, TransportSecurity, TransportServer,
};
use crate::node::{ClusterError, NodeIdentity};
use crate::tablet::{
    ReplicaDescriptor, TablePartitioningRecord, TabletDescriptor, TabletError, TabletLayout,
    TabletOwnershipGuard, TabletOwnershipRegistry, TabletState, TABLETS_DIR, TABLET_META_FILENAME,
};

/// Errors of the node runtime surface.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    /// Node identity / bootstrap-layer failure.
    #[error(transparent)]
    Cluster(#[from] ClusterError),
    /// Meta control-plane failure.
    #[error(transparent)]
    Meta(#[from] MetaError),
    /// Consensus group failure.
    #[error(transparent)]
    Consensus(#[from] ConsensusError),
    /// Transport failure.
    #[error(transparent)]
    Transport(#[from] TransportError),
    /// Tablet layout / descriptor failure.
    #[error(transparent)]
    Tablet(#[from] TabletError),
    /// Engine sink (applied tablet core) failure.
    #[error(transparent)]
    EngineSink(#[from] EngineSinkError),
    /// Runtime I/O failure.
    #[error("runtime I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The caller's request was malformed for this node (identity mismatch,
    /// missing replica, raft-id collision, invalid workflow order).
    #[error("invalid runtime request: {0}")]
    InvalidRequest(String),
}

/// Optional raft timing overrides, applied to every group the runtime opens
/// (meta and tablets). `None` keeps the production defaults of
/// [`GroupConfig::new`]; tests install fast elections through this knob.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupTiming {
    /// Leader heartbeat interval.
    pub heartbeat_interval: Duration,
    /// Minimum election timeout.
    pub election_timeout_min: Duration,
    /// Maximum election timeout.
    pub election_timeout_max: Duration,
    /// Timeout for one snapshot-send/install round.
    pub install_snapshot_timeout: Duration,
}

impl GroupTiming {
    fn apply(&self, config: &mut GroupConfig) {
        config.heartbeat_interval = self.heartbeat_interval;
        config.election_timeout_min = self.election_timeout_min;
        config.election_timeout_max = self.election_timeout_max;
        config.install_snapshot_timeout = self.install_snapshot_timeout;
    }
}

/// This node's membership of the meta control-plane group (spec section
/// 12.1). Present in [`NodeRuntimeConfig::meta`] exactly when the node is a
/// meta member.
#[derive(Clone, Debug)]
pub struct MetaMembership {
    /// The dedicated meta group's durable identifier (minted at cluster
    /// bootstrap; identical on every meta member).
    pub meta_group_id: RaftGroupId,
    /// The voter set to bootstrap the pristine group with, on the one
    /// bootstrap node only; `None` on every other member (it joins through
    /// [`MetaGroup::add_member`]). Reopening an already-initialized group
    /// never re-bootstraps.
    pub bootstrap_voters: Option<Vec<(NodeId, String)>>,
}

/// Static configuration of a [`NodeRuntime`].
#[derive(Clone, Debug)]
pub struct NodeRuntimeConfig {
    /// The node's local data root (`node-data`).
    pub node_data: PathBuf,
    /// How the transport authenticates its peers (production: mTLS).
    pub security: TransportSecurity,
    /// Transport bounds (shared by the client and the listener).
    pub transport: TransportConfig,
    /// Address the [`TransportServer`] binds (`host:port`; port 0 picks a
    /// free port, reported through [`NodeRuntime::rpc_address`]).
    pub listen_address: String,
    /// Address advertised to peers and stamped into node descriptors;
    /// defaults to the bound listen address.
    pub rpc_address: Option<String>,
    /// Static membership directory: every cluster node's durable id and RPC
    /// address. Feeds the transport's peer table and the resolution of
    /// tablet replica raft ids to endpoints.
    pub peers: Vec<(NodeId, String)>,
    /// Meta group membership, when this node is a meta member.
    pub meta: Option<MetaMembership>,
    /// Raft timing overrides (tests); `None` for production defaults.
    pub timing: Option<GroupTiming>,
}

impl NodeRuntimeConfig {
    /// A configuration with production transport bounds, plaintext security,
    /// and no meta membership; callers adjust the fields they need.
    pub fn new(node_data: PathBuf, listen_address: String) -> Self {
        Self {
            node_data,
            security: TransportSecurity::PlaintextForTesting,
            transport: TransportConfig::default(),
            listen_address,
            rpc_address: None,
            peers: Vec::new(),
            meta: None,
            timing: None,
        }
    }
}

/// Status of the meta group member on this node.
#[derive(Clone, Debug)]
pub struct MetaGroupStatus {
    /// The meta group's durable id.
    pub meta_group_id: RaftGroupId,
    /// Local applied watermark of the replicated meta state.
    pub metadata_version: MetadataVersion,
    /// Raft observability metrics (spec section 14.4).
    pub metrics: GroupMetrics,
}

/// Status of one tablet group hosted on this node.
#[derive(Clone, Debug)]
pub struct TabletGroupStatus {
    /// The tablet's durable id.
    pub tablet_id: TabletId,
    /// The consensus group replicating the tablet.
    pub raft_group_id: RaftGroupId,
    /// Lifecycle state of the descriptor the group was opened with.
    pub state: TabletState,
    /// Replicas of the descriptor the group was opened with.
    pub replicas: Vec<ReplicaDescriptor>,
    /// Local applied watermark of the group.
    pub applied: LogPosition,
    /// Raft observability metrics (spec section 14.4).
    pub metrics: GroupMetrics,
}

/// Point-in-time view of a running node: identity, groups with roles, and
/// applied watermarks (spec sections 11.1, 14.4).
#[derive(Clone, Debug)]
pub struct RuntimeStatus {
    /// This node's persisted identity.
    pub identity: NodeIdentity,
    /// The advertised RPC address.
    pub rpc_address: String,
    /// Meta group status, when this node is a meta member.
    pub meta: Option<MetaGroupStatus>,
    /// Tablet groups hosted on this node, in tablet-id order.
    pub tablets: Vec<TabletGroupStatus>,
}

/// The engine sink's database binding of a tablet group: derived from the
/// raft group id so every replica of the group — and every restart of one
/// replica — binds identically. Interim until meta-driven table-to-database
/// resolution lands (spec section 12.11; see the module docs).
fn tablet_database_id(raft_group_id: RaftGroupId) -> DatabaseId {
    DatabaseId::from_bytes(*raft_group_id.as_bytes())
}

/// The text identifier of a tablet group (session tokens carry it, spec
/// section 11.4); identical on every replica of the group.
fn tablet_group_name(tablet_id: TabletId) -> String {
    format!("tablet-{}", tablet_id.to_hex())
}

/// One tablet group hosted on this node: the consensus group over the engine
/// sink, the descriptor it was opened with, and the ownership reservation
/// (spec section 12.3: one tablet storage core is owned by one node process).
struct TabletGroup {
    group: ConsensusGroup<TcpTransport>,
    descriptor: TabletDescriptor,
    _ownership: TabletOwnershipGuard<'static>,
}

/// Minimal decode probe discovering a tablet directory's raft group id ahead
/// of the authoritative [`TabletLayout::validate`] verification (which checks
/// the versioned, checksummed envelope end to end).
#[derive(serde::Deserialize)]
struct TabletFileProbe {
    tablet: TabletProbe,
}

#[derive(serde::Deserialize)]
struct TabletProbe {
    raft_group_id: RaftGroupId,
}

/// Scans `node-data/tablets/` for tablet replicas this node hosts, returning
/// each tablet's id and raft group id (sorted by tablet id). Half-created
/// tablet directories (no `tablet.json`) fail closed, exactly as
/// [`TabletLayout::validate`] does.
fn scan_tablet_layouts(node_data: &Path) -> Result<Vec<(TabletId, RaftGroupId)>, RuntimeError> {
    let root = node_data.join(TABLETS_DIR);
    if !root.is_dir() {
        return Ok(Vec::new());
    }
    let mut found = Vec::new();
    for entry in std::fs::read_dir(&root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(RuntimeError::InvalidRequest(format!(
                "tablet directory name {name:?} is not UTF-8"
            )));
        };
        let tablet_id: TabletId = name.parse().map_err(|_| {
            RuntimeError::InvalidRequest(format!("tablet directory `{name}` is not a tablet id"))
        })?;
        let probe_path = entry.path().join(TABLET_META_FILENAME);
        let Some(bytes) = crate::node::read_meta_file(&probe_path)? else {
            return Err(TabletError::MissingMetadata(probe_path).into());
        };
        let probe: TabletFileProbe = serde_json::from_slice(&bytes).map_err(|error| {
            RuntimeError::InvalidRequest(format!(
                "tablet metadata probe at {} failed: {error}",
                probe_path.display()
            ))
        })?;
        found.push((tablet_id, probe.tablet.raft_group_id));
    }
    found.sort();
    Ok(found)
}

/// The peer endpoint of one node under the runtime's security mode.
fn peer_endpoint(security: &TransportSecurity, node_id: NodeId, address: &str) -> PeerEndpoint {
    match security {
        TransportSecurity::Mtls(_) => PeerEndpoint::mtls(address, node_id),
        TransportSecurity::PlaintextForTesting => PeerEndpoint::plaintext(address),
    }
}

/// The per-node context every tablet group open needs (kept apart from the
/// per-tablet parameters of [`NodeRuntime::open_tablet_group`]).
struct TabletOpenContext<'a> {
    node_data: &'a Path,
    security: &'a TransportSecurity,
    timing: Option<GroupTiming>,
    identity: &'a NodeIdentity,
    peers: &'a BTreeMap<NodeId, String>,
}

/// A running cluster node: identity, transport, meta group, and tablet
/// groups (see the module docs).
pub struct NodeRuntime {
    identity: NodeIdentity,
    node_data: PathBuf,
    rpc_address: String,
    security: TransportSecurity,
    peers: BTreeMap<NodeId, String>,
    timing: Option<GroupTiming>,
    transport: Arc<TcpTransport>,
    server: Option<TransportServer>,
    meta: Option<MetaGroup<TcpTransport>>,
    tablets: BTreeMap<TabletId, TabletGroup>,
}

impl NodeRuntime {
    /// Starts the node (see the module docs for the ordered start-up steps).
    pub async fn start(config: NodeRuntimeConfig) -> Result<Self, RuntimeError> {
        let identity =
            NodeIdentity::load(&config.node_data)?.ok_or(ClusterError::NotInitialized)?;
        let transport = Arc::new(TcpTransport::new(
            config.transport.clone(),
            config.security.clone(),
        ));
        let mut peers = BTreeMap::new();
        for (node_id, address) in &config.peers {
            transport.upsert_peer(
                raft_node_id(node_id),
                peer_endpoint(&config.security, *node_id, address),
            );
            peers.insert(*node_id, address.clone());
        }
        let server = TransportServer::bind(
            &config.listen_address,
            config.security.clone(),
            transport.registry(),
            config.transport.clone(),
        )
        .await?;
        let rpc_address = config
            .rpc_address
            .clone()
            .unwrap_or_else(|| server.local_addr().to_string());

        let meta = match &config.meta {
            Some(membership) => {
                let group =
                    Self::start_meta_group(&config, &identity, membership, transport.clone())
                        .await?;
                Some(group)
            }
            None => None,
        };
        let tablets =
            Self::open_tablet_groups(&config, &identity, &peers, transport.clone()).await?;
        Ok(Self {
            identity,
            node_data: config.node_data.clone(),
            rpc_address,
            security: config.security.clone(),
            peers,
            timing: config.timing,
            transport,
            server: Some(server),
            meta,
            tablets,
        })
    }

    /// Opens this node's meta group member and bootstraps it when the
    /// membership names a bootstrap voter set and the group is still pristine.
    async fn start_meta_group(
        config: &NodeRuntimeConfig,
        identity: &NodeIdentity,
        membership: &MetaMembership,
        transport: Arc<TcpTransport>,
    ) -> Result<MetaGroup<TcpTransport>, RuntimeError> {
        let meta_config = MetaGroupConfig::new(
            config.node_data.clone(),
            membership.meta_group_id,
            identity.node_id,
        );
        let mut group_config = meta_config.group_config();
        if let Some(timing) = &config.timing {
            timing.apply(&mut group_config);
        }
        let group = MetaGroup::create(meta_config, group_config, transport).await?;
        if let Some(voters) = &membership.bootstrap_voters {
            if !voters
                .iter()
                .any(|(node_id, _)| *node_id == identity.node_id)
            {
                return Err(RuntimeError::InvalidRequest(
                    "meta bootstrap voter set does not include this node".to_owned(),
                ));
            }
            if !group.is_initialized().await? {
                group.bootstrap(voters).await?;
            }
        }
        Ok(group)
    }

    /// Reopens every tablet group the local layout lists (restart path).
    async fn open_tablet_groups(
        config: &NodeRuntimeConfig,
        identity: &NodeIdentity,
        peers: &BTreeMap<NodeId, String>,
        transport: Arc<TcpTransport>,
    ) -> Result<BTreeMap<TabletId, TabletGroup>, RuntimeError> {
        let mut groups = BTreeMap::new();
        let context = TabletOpenContext {
            node_data: &config.node_data,
            security: &config.security,
            timing: config.timing,
            identity,
            peers,
        };
        for (tablet_id, raft_group_id) in scan_tablet_layouts(&config.node_data)? {
            let layout = TabletLayout::new(config.node_data.clone(), tablet_id, raft_group_id);
            let descriptor = layout.validate()?;
            let group =
                Self::open_tablet_group(&context, transport.clone(), &layout, &descriptor).await?;
            groups.insert(tablet_id, group);
        }
        Ok(groups)
    }

    /// Opens one tablet group over the engine sink: validates this node's
    /// replica, reserves the tablet directory, registers replica endpoints,
    /// and starts the raft task (which attaches itself to the transport
    /// registry).
    async fn open_tablet_group(
        context: &TabletOpenContext<'_>,
        transport: Arc<TcpTransport>,
        layout: &TabletLayout,
        descriptor: &TabletDescriptor,
    ) -> Result<TabletGroup, RuntimeError> {
        let replica = *descriptor
            .replica_on(context.identity.node_id)
            .ok_or_else(|| {
                RuntimeError::InvalidRequest(format!(
                    "tablet {} descriptor does not list this node ({}) as a replica",
                    descriptor.tablet_id, context.identity.node_id
                ))
            })?;
        if transport.registry().get(replica.raft_node_id).is_some() {
            return Err(RuntimeError::InvalidRequest(format!(
                "raft id {} is already attached to this node's transport registry",
                replica.raft_node_id
            )));
        }
        // The replica raft ids route through the transport's peer table;
        // every replica must resolve to a cluster node address.
        for other in &descriptor.replicas {
            let Some(address) = context.peers.get(&other.node_id) else {
                return Err(RuntimeError::InvalidRequest(format!(
                    "no membership-directory address for replica node {} of tablet {}",
                    other.node_id, descriptor.tablet_id
                )));
            };
            transport.upsert_peer(
                other.raft_node_id,
                peer_endpoint(context.security, other.node_id, address),
            );
        }
        let ownership = TabletOwnershipRegistry::global().try_reserve(layout)?;
        let engine_config = EngineGroupConfig::new(
            context.node_data.to_path_buf(),
            descriptor.raft_group_id,
            context.identity.cluster_id,
            context.identity.node_id,
            tablet_database_id(descriptor.raft_group_id),
        );
        let sink = open_engine_sink(&engine_config)?;
        let mut group_config = GroupConfig::new(
            tablet_group_name(descriptor.tablet_id),
            replica.raft_node_id,
            engine_config.group_dir(),
        );
        group_config.storage = engine_config.storage.clone();
        group_config.idempotency_retention = engine_config.idempotency_retention;
        if let Some(timing) = &context.timing {
            timing.apply(&mut group_config);
        }
        let dyn_sink: Arc<Mutex<dyn ApplySink>> = sink;
        let group = ConsensusGroup::create(group_config, transport, dyn_sink).await?;
        Ok(TabletGroup {
            group,
            descriptor: descriptor.clone(),
            _ownership: ownership,
        })
    }

    /// This node's persisted identity.
    pub fn identity(&self) -> &NodeIdentity {
        &self.identity
    }

    /// The advertised RPC address of this node.
    pub fn rpc_address(&self) -> &str {
        &self.rpc_address
    }

    /// This node's meta group member, when it is a meta member.
    pub fn meta_group(&self) -> Option<&MetaGroup<TcpTransport>> {
        self.meta.as_ref()
    }

    /// The consensus group of one tablet hosted on this node.
    pub fn tablet_group(&self, tablet_id: TabletId) -> Option<&ConsensusGroup<TcpTransport>> {
        self.tablets.get(&tablet_id).map(|tablet| &tablet.group)
    }

    /// The descriptor a hosted tablet group was opened with.
    pub fn tablet_descriptor(&self, tablet_id: TabletId) -> Option<&TabletDescriptor> {
        self.tablets
            .get(&tablet_id)
            .map(|tablet| &tablet.descriptor)
    }

    /// Every tablet hosted on this node, in tablet-id order.
    pub fn tablet_ids(&self) -> Vec<TabletId> {
        self.tablets.keys().copied().collect()
    }

    /// One hosted tablet group, or an [`RuntimeError::InvalidRequest`] naming
    /// the tablet this node does not host.
    fn hosted_tablet(&self, tablet_id: TabletId) -> Result<&TabletGroup, RuntimeError> {
        self.tablets.get(&tablet_id).ok_or_else(|| {
            RuntimeError::InvalidRequest(format!("this node hosts no tablet {tablet_id}"))
        })
    }

    /// Creates one tablet replica on this node (spec sections 12.3, 12.7):
    /// allocates the section 12.3 layout, opens a [`ConsensusGroup`] over the
    /// engine sink, and registers the group in the transport registry.
    ///
    /// `partitioning` is the table's section 12.2 partitioning record; it is
    /// validated for structural soundness and must name the same table as the
    /// descriptor. When `bootstrap_voters` is `Some`, the pristine group is
    /// bootstrapped with that voter set (mapped to the descriptor's raft ids;
    /// must include this node) — call this on exactly one replica, after the
    /// other replicas created their local groups. When `publish_to_meta` is
    /// set, the descriptor is published to the meta group through
    /// [`NodeRuntime::publish_tablet_descriptor`] (which requires this node
    /// to host the meta group and reach its leader).
    ///
    /// Repeating the call with an identical descriptor is a no-op; a
    /// different descriptor for an already-open tablet fails closed.
    pub async fn create_tablet(
        &mut self,
        descriptor: &TabletDescriptor,
        partitioning: &TablePartitioningRecord,
        bootstrap_voters: Option<&[(NodeId, String)]>,
        publish_to_meta: bool,
        control: &ExecutionControl,
    ) -> Result<(), RuntimeError> {
        descriptor.validate()?;
        partitioning.validate()?;
        if partitioning.table_id != descriptor.table_id {
            return Err(RuntimeError::InvalidRequest(format!(
                "partitioning record names table {} but the descriptor partitions table {}",
                partitioning.table_id, descriptor.table_id
            )));
        }
        if let Some(existing) = self.tablets.get(&descriptor.tablet_id) {
            if existing.descriptor == *descriptor {
                return Ok(());
            }
            return Err(RuntimeError::InvalidRequest(format!(
                "tablet {} is already open with a different descriptor",
                descriptor.tablet_id
            )));
        }
        // Fail closed before touching the layout: this node must be a
        // replica of the tablet it is asked to create.
        if descriptor.replica_on(self.identity.node_id).is_none() {
            return Err(RuntimeError::InvalidRequest(format!(
                "tablet {} descriptor does not list this node ({}) as a replica",
                descriptor.tablet_id, self.identity.node_id
            )));
        }
        let layout = TabletLayout::new(
            self.node_data.clone(),
            descriptor.tablet_id,
            descriptor.raft_group_id,
        );
        layout.create(descriptor)?;
        let context = TabletOpenContext {
            node_data: &self.node_data,
            security: &self.security,
            timing: self.timing,
            identity: &self.identity,
            peers: &self.peers,
        };
        let group =
            Self::open_tablet_group(&context, self.transport.clone(), &layout, descriptor).await?;
        if let Some(voters) = bootstrap_voters {
            let mut members = BTreeMap::new();
            for (node_id, address) in voters {
                let Some(replica) = descriptor.replica_on(*node_id) else {
                    return Err(RuntimeError::InvalidRequest(format!(
                        "bootstrap voter {node_id} is not a replica of tablet {}",
                        descriptor.tablet_id
                    )));
                };
                members.insert(replica.raft_node_id, BasicNode::new(address.clone()));
            }
            if !members.contains_key(&group.group.node_id()) {
                return Err(RuntimeError::InvalidRequest(
                    "tablet bootstrap voter set does not include this node".to_owned(),
                ));
            }
            if !group.group.is_initialized().await? {
                group.group.bootstrap(members).await?;
            }
        }
        self.tablets.insert(descriptor.tablet_id, group);
        if publish_to_meta {
            self.publish_tablet_descriptor(descriptor, control).await?;
        }
        Ok(())
    }

    /// The section 12.7 replica-join workflow, driven on the node hosting
    /// the target group (normally its leader): add the new replica as a
    /// learner (blocking until it is line-rate — the snapshot/catch-up step),
    /// then promote it to voter through joint consensus. The learner's own
    /// node must already have created its local replica
    /// ([`NodeRuntime::create_tablet`] with the learner in the descriptor);
    /// the descriptor update itself (role flip, generation bump) is published
    /// to the meta group separately
    /// ([`NodeRuntime::publish_tablet_descriptor`]).
    pub async fn add_tablet_replica(
        &self,
        tablet_id: TabletId,
        replica: ReplicaDescriptor,
        address: &str,
    ) -> Result<(), RuntimeError> {
        let tablet = self.hosted_tablet(tablet_id)?;
        if tablet.descriptor.replicas.iter().any(|existing| {
            existing.node_id == replica.node_id || existing.raft_node_id == replica.raft_node_id
        }) {
            return Err(RuntimeError::InvalidRequest(format!(
                "replica node {} / raft id {} already belongs to tablet {}",
                replica.node_id, replica.raft_node_id, tablet_id
            )));
        }
        self.transport.upsert_peer(
            replica.raft_node_id,
            peer_endpoint(&self.security, replica.node_id, address),
        );
        // `add_learner` blocks until the learner is line-rate: the
        // snapshot/catch-up step of the movement protocol (spec section
        // 12.7).
        tablet
            .group
            .add_learner(replica.raft_node_id, BasicNode::new(address.to_owned()))
            .await?;
        tablet.group.promote(replica.raft_node_id).await?;
        Ok(())
    }

    /// Publishes a tablet descriptor to the meta group (last-writer-wins by
    /// `generation`). Requires this node to host the meta group; the proposal
    /// rides the meta leader (a follower surfaces the routed
    /// [`ConsensusError::NotLeader`] through [`RuntimeError::Meta`]).
    pub async fn publish_tablet_descriptor(
        &self,
        descriptor: &TabletDescriptor,
        control: &ExecutionControl,
    ) -> Result<MetadataVersion, RuntimeError> {
        let meta = self.meta.as_ref().ok_or_else(|| {
            RuntimeError::InvalidRequest("this node hosts no meta group".to_owned())
        })?;
        let receipt = meta
            .propose(
                crate::meta::new_command_id()?,
                MetaCommand::SetTabletDescriptor {
                    descriptor: descriptor.clone(),
                },
                control,
            )
            .await?;
        Ok(receipt.metadata_version)
    }

    /// Point-in-time node status: identity, groups with roles, and applied
    /// watermarks (spec section 14.4).
    pub fn status(&self) -> RuntimeStatus {
        RuntimeStatus {
            identity: self.identity.clone(),
            rpc_address: self.rpc_address.clone(),
            meta: self.meta.as_ref().map(|meta| MetaGroupStatus {
                meta_group_id: meta.meta_group_id(),
                metadata_version: meta.metadata_version(),
                metrics: meta.group().metrics(),
            }),
            tablets: self
                .tablets
                .values()
                .map(|tablet| TabletGroupStatus {
                    tablet_id: tablet.descriptor.tablet_id,
                    raft_group_id: tablet.descriptor.raft_group_id,
                    state: tablet.descriptor.state,
                    replicas: tablet.descriptor.replicas.clone(),
                    applied: tablet.group.applied_position(),
                    metrics: tablet.group.metrics(),
                })
                .collect(),
        }
    }

    /// Graceful shutdown (see the module docs): stop accepting RPCs, shut
    /// down every group, release the tablet ownership guards. All groups are
    /// attempted even when one fails; the first error is reported.
    pub async fn shutdown(mut self) -> Result<(), RuntimeError> {
        // 1. Stop accepting RPCs; in-flight connections drain within the
        //    listener's configured grace.
        if let Some(server) = self.server.take() {
            server.shutdown().await;
        }
        let mut first_error: Option<RuntimeError> = None;
        // 2. Shut down groups: each detaches from the transport registry,
        //    fsyncs its log, and stops its raft task.
        for (_, tablet) in std::mem::take(&mut self.tablets) {
            if let Err(error) = tablet.group.shutdown().await {
                if first_error.is_none() {
                    first_error = Some(error.into());
                }
            }
        }
        if let Some(meta) = self.meta.take() {
            if let Err(error) = meta.shutdown().await {
                if first_error.is_none() {
                    first_error = Some(error.into());
                }
            }
        }
        // 3. Tablet ownership guards release as the handles drop.
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_scan_tolerates_a_node_without_tablets() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(scan_tablet_layouts(tmp.path()).unwrap(), Vec::new());
        // An empty tablets directory scans clean too.
        std::fs::create_dir_all(tmp.path().join(TABLETS_DIR)).unwrap();
        assert_eq!(scan_tablet_layouts(tmp.path()).unwrap(), Vec::new());
    }

    #[test]
    fn layout_scan_fails_closed_on_garbage_directories() {
        let tmp = tempfile::tempdir().unwrap();
        // A directory that is not a tablet id is rejected.
        let garbage = tmp.path().join(TABLETS_DIR).join("not-a-tablet");
        std::fs::create_dir_all(&garbage).unwrap();
        assert!(matches!(
            scan_tablet_layouts(tmp.path()),
            Err(RuntimeError::InvalidRequest(_))
        ));
        std::fs::remove_dir_all(&garbage).unwrap();
        // A tablet directory missing its metadata fails closed (spec section
        // 12.3), exactly as `TabletLayout::validate` does.
        let tablet_id = TabletId::new_random();
        std::fs::create_dir_all(tmp.path().join(TABLETS_DIR).join(tablet_id.to_hex())).unwrap();
        assert!(matches!(
            scan_tablet_layouts(tmp.path()),
            Err(RuntimeError::Tablet(TabletError::MissingMetadata(_)))
        ));
    }
}
