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
//! # Split/merge integration (spec sections 12.5-12.6)
//!
//! [`NodeRuntime::split_tablet`] / [`NodeRuntime::merge_tablets`] drive the
//! full safe-split/safe-merge protocols against the real seams: the meta
//! plane is the node's [`MetaGroup`] (the atomic publications ride the
//! single [`MetaCommand::PublishSplit`] / [`MetaCommand::PublishMerge`]
//! raft commands), child tablet ids and replica raft ids are minted by the
//! meta-owned allocator ([`MetaGroup::allocate_raft_node_ids`]), and
//! [`NodeRuntime::abort_split`] unwinds an unpublished split.
//!
//! The data plane is the interim [`TabletLedger`]: the cluster crate has no
//! storage-engine dependency (the consensus crate does not re-export core
//! row types), so the engine's applied keyspace is opaque here. The ledger
//! is a partition-keyed row store the runtime owns outright, replicated
//! through the tablet's own raft group as [`COMMAND_TYPE_TABLET_DATA`]
//! catalog commands and applied on every replica by the composite
//! [`TabletGroupSink`] — real replication and durability, bound to the
//! local engine-backed groups, without naming a core type. When the engine
//! grows a tablet-aware keyspace API (Stage 3C's applied MVCC state), the
//! [`crate::split::TabletKeyspace`] / [`crate::split::ChildStateSink`]
//! seams rebind to it and the ledger retires; the split/merge protocol
//! drivers are unchanged.
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

use mongreldb_consensus::engine_sink::{
    open_engine_sink, EngineApplySink, EngineGroupConfig, EngineSinkError,
};
use mongreldb_consensus::error::ConsensusError;
use mongreldb_consensus::group::{ConsensusGroup, GroupCommitReceipt, GroupConfig, GroupMetrics};
use mongreldb_consensus::identity::{raft_node_id, CommandKind, RaftNodeId, ReplicatedCommand};
use mongreldb_consensus::state_machine::{AppliedCommand, ApplySink, StateMachineError};
use mongreldb_log::commit_log::{ExecutionControl, LogPosition};
use mongreldb_log::envelope::CommandEnvelope;
use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::{DatabaseId, MetadataVersion, NodeId, RaftGroupId, TabletId};
use openraft::BasicNode;
use serde::{Deserialize, Serialize};

use crate::merge::{
    merge_progress, MergeExecutor, MergeInputs, MergeMetaPlane, MergePhase, MergePlan,
    MergePlanner, MergePublishCommand,
};
use crate::meta::{MetaCommand, MetaError, MetaGroup, MetaGroupConfig, MetaRejectionReason};
use crate::network::{
    PeerEndpoint, TcpTransport, TransportConfig, TransportError, TransportSecurity, TransportServer,
};
use crate::node::{ClusterError, NodeIdentity};
use crate::split::{
    abort_split, split_progress, ChildAllocation, ChildStateSink, SnapshotPin, SplitAbortReport,
    SplitError, SplitExecutor, SplitKeySelection, SplitPhase, SplitPlan, SplitPublishCommand,
    TabletDataError, TabletKeyspace, TabletMetaPlane, TabletSplitPlanner,
};
use crate::tablet::{
    Key, ReplicaDescriptor, TablePartitioningRecord, TabletDescriptor, TabletError, TabletLayout,
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
    /// Split protocol failure.
    #[error(transparent)]
    Split(#[from] SplitError),
    /// Merge protocol failure.
    #[error(transparent)]
    Merge(#[from] crate::merge::MergeError),
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

// ---------------------------------------------------------------------------
// The interim tablet data plane (see the module docs)
// ---------------------------------------------------------------------------

/// `CommandEnvelope::command_type` of tablet data commands (upsert/replace
/// of partition-keyed rows). Envelope discriminants are never reused (spec
/// section 4.10): 1 is the engine transaction command, 2 the engine catalog
/// command, 3 the maintenance command, 4 the meta control-plane command.
pub const COMMAND_TYPE_TABLET_DATA: u32 = 5;

/// The format version of [`TabletDataCommandRecord`] payloads this build
/// writes.
pub const TABLET_DATA_COMMAND_FORMAT_VERSION: u32 = 1;
/// The oldest [`TabletDataCommandRecord`] format version this build accepts.
pub const MIN_SUPPORTED_TABLET_DATA_COMMAND_FORMAT_VERSION: u32 = 1;

/// One mutation of a tablet's applied keyspace (spec section 12.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TabletDataCommand {
    /// Inserts or replaces the newest version of each key.
    Upsert {
        /// The key/value pairs, in commit order.
        entries: Vec<(Key, Vec<u8>)>,
    },
    /// Atomically replaces the whole applied keyspace (the split/merge child
    /// state install: staged beside live state, then installed in one
    /// command — never a partial overwrite).
    Replace {
        /// The complete replacement contents.
        rows: Vec<(Key, Vec<u8>)>,
    },
}

/// The versioned payload of one [`COMMAND_TYPE_TABLET_DATA`] envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TabletDataCommandRecord {
    /// Format version; see [`TABLET_DATA_COMMAND_FORMAT_VERSION`].
    pub format_version: u32,
    /// The command.
    pub command: TabletDataCommand,
}

impl TabletDataCommandRecord {
    /// Wraps `command` at the current format version.
    pub fn new(command: TabletDataCommand) -> Self {
        Self {
            format_version: TABLET_DATA_COMMAND_FORMAT_VERSION,
            command,
        }
    }

    /// Serializes the record (JSON).
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("tablet data command encoding is total")
    }

    /// Parses and verifies a record produced by [`Self::encode`]; unknown
    /// versions and malformed payloads fail closed.
    pub fn decode(payload: &[u8]) -> Result<Self, StateMachineError> {
        let record: Self = serde_json::from_slice(payload)
            .map_err(|error| StateMachineError::Corrupt(format!("tablet data command: {error}")))?;
        if record.format_version < MIN_SUPPORTED_TABLET_DATA_COMMAND_FORMAT_VERSION
            || record.format_version > TABLET_DATA_COMMAND_FORMAT_VERSION
        {
            return Err(StateMachineError::Corrupt(format!(
                "tablet data command format version {} is outside \
                 {MIN_SUPPORTED_TABLET_DATA_COMMAND_FORMAT_VERSION}..=\
                 {TABLET_DATA_COMMAND_FORMAT_VERSION}",
                record.format_version
            )));
        }
        Ok(record)
    }
}

/// Name of the ledger's durable checkpoint inside `<group dir>/raft/state/`.
pub const TABLET_LEDGER_FILENAME: &str = "tablet-ledger.json";
/// The ledger checkpoint format version this build writes.
pub const TABLET_LEDGER_FORMAT_VERSION: u32 = 1;
/// The oldest ledger checkpoint format version this build accepts.
pub const MIN_SUPPORTED_TABLET_LEDGER_FORMAT_VERSION: u32 = 1;

/// The ceiling timestamp ("visible at any time"): one above every legal
/// physical-micros value.
const MAX_TIMESTAMP: HlcTimestamp = HlcTimestamp {
    physical_micros: u64::MAX,
    logical: u32::MAX,
    node_tiebreaker: u32::MAX,
};

/// The durable ledger checkpoint: the applied keyspace (per-key version
/// chains) plus the log watermark it reflects — the same crash-window
/// replay dedup idiom as [`crate::meta::MetaApplySink`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TabletLedgerCheckpoint {
    /// Checkpoint format version; see [`TABLET_LEDGER_FORMAT_VERSION`].
    format_version: u32,
    /// Log position the keyspace reflects.
    position: LogPosition,
    /// Per-key version chains, ascending commit timestamps.
    rows: BTreeMap<Key, Vec<(HlcTimestamp, Vec<u8>)>>,
}

/// The applied keyspace of one tablet replica (see the module docs): an
/// MVCC-lite, partition-keyed row store applied from the tablet group's
/// committed raft log. Versions are kept per key so the split/merge
/// executors can pin a snapshot at `split_ts`/`merge_ts` and read the
/// before/after sides of the timeline; chains compact against the oldest
/// live pin (a pin protects exactly the at-or-below view).
///
/// Persistence mirrors [`crate::meta::MetaApplySink`]: the checkpoint is
/// rewritten atomically after every applied entry, and redelivery at or
/// below the durable watermark is skipped, so applies never double-apply.
#[derive(Debug)]
pub struct TabletLedger {
    rows: BTreeMap<Key, Vec<(HlcTimestamp, Vec<u8>)>>,
    /// Registered snapshot pins (timestamp -> live pin count).
    pins: BTreeMap<HlcTimestamp, usize>,
    position: LogPosition,
    state_dir: PathBuf,
}

impl TabletLedger {
    /// Opens (creating if needed) the ledger under `group_dir`, loading the
    /// persisted checkpoint when present. A present but undecodable or
    /// unsupported-version checkpoint fails closed (spec section 4.10).
    pub fn open(group_dir: &Path) -> Result<Self, RuntimeError> {
        let state_dir = group_dir.join("raft").join("state");
        std::fs::create_dir_all(&state_dir).map_err(RuntimeError::Io)?;
        let path = state_dir.join(TABLET_LEDGER_FILENAME);
        let Some(bytes) = crate::node::read_meta_file(&path)? else {
            return Ok(Self {
                rows: BTreeMap::new(),
                pins: BTreeMap::new(),
                position: LogPosition::ZERO,
                state_dir,
            });
        };
        let checkpoint: TabletLedgerCheckpoint =
            crate::node::decode_json(TABLET_LEDGER_FILENAME, &bytes)?;
        if checkpoint.format_version < MIN_SUPPORTED_TABLET_LEDGER_FORMAT_VERSION
            || checkpoint.format_version > TABLET_LEDGER_FORMAT_VERSION
        {
            return Err(ClusterError::UnsupportedFormatVersion {
                file: TABLET_LEDGER_FILENAME,
                found: checkpoint.format_version,
                min: MIN_SUPPORTED_TABLET_LEDGER_FORMAT_VERSION,
                max: TABLET_LEDGER_FORMAT_VERSION,
            }
            .into());
        }
        Ok(Self {
            rows: checkpoint.rows,
            pins: BTreeMap::new(),
            position: checkpoint.position,
            state_dir,
        })
    }

    /// Applies one committed command at `position`. Redelivery at or below
    /// the durable watermark is skipped (the sink-first/checkpoint-second
    /// crash window); the checkpoint is persisted before returning.
    fn apply(
        &mut self,
        command: &TabletDataCommand,
        commit_ts: HlcTimestamp,
        position: LogPosition,
    ) -> Result<(), RuntimeError> {
        if position.index <= self.position.index {
            return Ok(());
        }
        match command {
            TabletDataCommand::Upsert { entries } => {
                for (key, value) in entries {
                    self.insert_version(key.clone(), commit_ts, value.clone());
                }
            }
            TabletDataCommand::Replace { rows } => {
                // A whole-keyspace install would silently destroy a pinned
                // snapshot's timeline; fail closed instead. (Child state
                // installs only ever target child ledgers, which are never
                // pinned: pins belong to split/merge sources.)
                if !self.pins.is_empty() {
                    return Err(RuntimeError::InvalidRequest(
                        "tablet ledger Replace with live snapshot pins".to_owned(),
                    ));
                }
                self.rows.clear();
                for (key, value) in rows {
                    self.insert_version(key.clone(), commit_ts, value.clone());
                }
            }
        }
        self.position = position;
        self.persist()
    }

    /// Inserts one version, compacting the chain against the oldest live
    /// pin: versions below the pin collapse to the newest-below baseline
    /// (the pin's at-or-below view); with no pins only the newest survives.
    fn insert_version(&mut self, key: Key, ts: HlcTimestamp, value: Vec<u8>) {
        let chain = self.rows.entry(key).or_default();
        chain.push((ts, value));
        chain.sort_by_key(|(version, _)| *version);
        match self.pins.keys().next() {
            None => {
                let newest = chain.pop().expect("just inserted");
                chain.clear();
                chain.push(newest);
            }
            Some(oldest_pin) => {
                let baseline = chain.partition_point(|(version, _)| *version <= *oldest_pin);
                if baseline > 1 {
                    chain.drain(..baseline - 1);
                }
            }
        }
    }

    /// Persists the checkpoint atomically (temp-write + rename + dir fsync).
    fn persist(&self) -> Result<(), RuntimeError> {
        let checkpoint = TabletLedgerCheckpoint {
            format_version: TABLET_LEDGER_FORMAT_VERSION,
            position: self.position,
            rows: self.rows.clone(),
        };
        let bytes = crate::node::encode_json(TABLET_LEDGER_FILENAME, &checkpoint)?;
        crate::node::write_meta_atomic(&self.state_dir, TABLET_LEDGER_FILENAME, &bytes)
            .map_err(ClusterError::Io)?;
        Ok(())
    }

    /// The log position the keyspace reflects.
    pub fn applied_position(&self) -> LogPosition {
        self.position
    }

    /// Registers a snapshot pin at `ts`; the matching [`LedgerPin`] guard
    /// releases it.
    fn pin(&mut self, ts: HlcTimestamp) {
        *self.pins.entry(ts).or_insert(0) += 1;
    }

    /// Releases one pin at `ts`.
    fn unpin(&mut self, ts: HlcTimestamp) {
        if let Some(count) = self.pins.get_mut(&ts) {
            *count -= 1;
            if *count == 0 {
                self.pins.remove(&ts);
            }
        }
    }

    /// Number of live snapshot pins.
    pub fn pin_count(&self) -> usize {
        self.pins.values().sum()
    }

    /// Every key's newest version at or below `ts`, in key order.
    pub fn rows_at(&self, ts: HlcTimestamp) -> BTreeMap<Key, Vec<u8>> {
        self.rows
            .iter()
            .filter_map(|(key, chain)| {
                let visible = chain.iter().rfind(|(version, _)| *version <= ts)?;
                Some((key.clone(), visible.1.clone()))
            })
            .collect()
    }

    /// The current contents (the newest version of every key).
    pub fn current_rows(&self) -> BTreeMap<Key, Vec<u8>> {
        self.rows_at(MAX_TIMESTAMP)
    }

    /// Mutations committed after `ts`, in commit order (ties broken by key
    /// for determinism); multiple versions of one key arrive oldest first.
    pub fn deltas_after(&self, ts: HlcTimestamp) -> Vec<(Key, Vec<u8>)> {
        let mut deltas: Vec<(HlcTimestamp, Key, Vec<u8>)> = Vec::new();
        for (key, chain) in &self.rows {
            for (version, value) in chain {
                if *version > ts {
                    deltas.push((*version, key.clone(), value.clone()));
                }
            }
        }
        deltas.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
        deltas
            .into_iter()
            .map(|(_, key, value)| (key, value))
            .collect()
    }

    /// The applied size in bytes (newest versions only), feeding the merge
    /// planner's combined-size check (spec section 12.6).
    pub fn size_bytes(&self) -> u64 {
        self.rows
            .iter()
            .map(|(key, chain)| {
                let newest = chain.last().expect("non-empty chain");
                (key.as_bytes().len() + newest.1.len()) as u64
            })
            .sum()
    }

    /// The checkpoint bytes, for the composite group snapshot.
    fn snapshot_bytes(&self) -> Result<Vec<u8>, StateMachineError> {
        serde_json::to_vec(&TabletLedgerCheckpoint {
            format_version: TABLET_LEDGER_FORMAT_VERSION,
            position: self.position,
            rows: self.rows.clone(),
        })
        .map_err(|error| StateMachineError::Sink(format!("tablet ledger snapshot: {error}")))
    }

    /// Installs checkpoint bytes produced by [`Self::snapshot_bytes`]
    /// (fail closed on unknown versions or malformed payloads).
    fn install_bytes(&mut self, bytes: &[u8]) -> Result<(), StateMachineError> {
        let checkpoint: TabletLedgerCheckpoint =
            serde_json::from_slice(bytes).map_err(|error| {
                StateMachineError::Corrupt(format!("tablet ledger snapshot: {error}"))
            })?;
        if checkpoint.format_version < MIN_SUPPORTED_TABLET_LEDGER_FORMAT_VERSION
            || checkpoint.format_version > TABLET_LEDGER_FORMAT_VERSION
        {
            return Err(StateMachineError::Corrupt(format!(
                "tablet ledger snapshot format version {} is outside \
                 {MIN_SUPPORTED_TABLET_LEDGER_FORMAT_VERSION}..={TABLET_LEDGER_FORMAT_VERSION}",
                checkpoint.format_version
            )));
        }
        self.rows = checkpoint.rows;
        self.position = checkpoint.position;
        self.persist()
            .map_err(|error| StateMachineError::Sink(format!("tablet ledger install: {error}")))
    }
}

/// A [`TabletLedger`] snapshot pin ([`crate::split::SnapshotPin`]); dropping
/// releases it.
struct LedgerPin {
    ts: HlcTimestamp,
    ledger: Arc<Mutex<TabletLedger>>,
}

impl SnapshotPin for LedgerPin {
    fn pinned_at(&self) -> HlcTimestamp {
        self.ts
    }
}

impl Drop for LedgerPin {
    fn drop(&mut self) {
        self.ledger
            .lock()
            .expect("tablet ledger lock poisoned")
            .unpin(self.ts);
    }
}

/// The composite apply sink of one tablet group: engine commands delegate
/// to the consensus [`EngineApplySink`]; [`COMMAND_TYPE_TABLET_DATA`]
/// commands apply to the [`TabletLedger`]. Snapshots frame both halves, so
/// raft catch-up installs engine state and ledger atomically together.
pub struct TabletGroupSink {
    engine: Arc<Mutex<EngineApplySink>>,
    ledger: Arc<Mutex<TabletLedger>>,
}

/// The composite snapshot framing version.
const TABLET_GROUP_SNAPSHOT_FORMAT_VERSION: u32 = 1;

/// `[version u32 LE][engine_len u64 LE][engine bytes][ledger bytes]`.
fn encode_group_snapshot(engine: &[u8], ledger: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + engine.len() + ledger.len());
    out.extend_from_slice(&TABLET_GROUP_SNAPSHOT_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&(engine.len() as u64).to_le_bytes());
    out.extend_from_slice(engine);
    out.extend_from_slice(ledger);
    out
}

fn decode_group_snapshot(bytes: &[u8]) -> Result<(Vec<u8>, Vec<u8>), StateMachineError> {
    if bytes.len() < 12 {
        return Err(StateMachineError::Corrupt(
            "tablet group snapshot: truncated frame".to_owned(),
        ));
    }
    let version = u32::from_le_bytes(bytes[..4].try_into().expect("4 bytes"));
    if version != TABLET_GROUP_SNAPSHOT_FORMAT_VERSION {
        return Err(StateMachineError::Corrupt(format!(
            "tablet group snapshot format version {version} is not \
             {TABLET_GROUP_SNAPSHOT_FORMAT_VERSION}"
        )));
    }
    let engine_len = u64::from_le_bytes(bytes[4..12].try_into().expect("8 bytes")) as usize;
    if bytes.len() < 12 + engine_len {
        return Err(StateMachineError::Corrupt(
            "tablet group snapshot: truncated engine payload".to_owned(),
        ));
    }
    Ok((
        bytes[12..12 + engine_len].to_vec(),
        bytes[12 + engine_len..].to_vec(),
    ))
}

impl TabletGroupSink {
    /// The ledger half (read-path inspection, the split/merge keyspace seam).
    pub fn ledger(&self) -> Arc<Mutex<TabletLedger>> {
        self.ledger.clone()
    }
}

impl ApplySink for TabletGroupSink {
    fn apply(&mut self, command: &AppliedCommand) -> Result<(), StateMachineError> {
        if let ReplicatedCommandRef::Catalog(envelope) = replicated_envelope(&command.command) {
            if envelope.command_type == COMMAND_TYPE_TABLET_DATA {
                envelope.verify().map_err(|error| {
                    StateMachineError::Corrupt(format!("tablet data envelope: {error}"))
                })?;
                let record = TabletDataCommandRecord::decode(&envelope.payload)?;
                let commit_ts = command.commit_ts().unwrap_or(HlcTimestamp::ZERO);
                return self
                    .ledger
                    .lock()
                    .map_err(|_| StateMachineError::Sink("tablet ledger lock poisoned".to_owned()))?
                    .apply(&record.command, commit_ts, command.position)
                    .map_err(|error| StateMachineError::Sink(error.to_string()));
            }
        }
        self.engine
            .lock()
            .map_err(|_| StateMachineError::Sink("engine sink lock poisoned".to_owned()))?
            .apply(command)
    }

    fn snapshot(&self) -> Result<Vec<u8>, StateMachineError> {
        let engine = self
            .engine
            .lock()
            .map_err(|_| StateMachineError::Sink("engine sink lock poisoned".to_owned()))?
            .snapshot()?;
        let ledger = self
            .ledger
            .lock()
            .map_err(|_| StateMachineError::Sink("tablet ledger lock poisoned".to_owned()))?
            .snapshot_bytes()?;
        Ok(encode_group_snapshot(&engine, &ledger))
    }

    fn install(&mut self, data: &[u8]) -> Result<(), StateMachineError> {
        let (engine, ledger) = decode_group_snapshot(data)?;
        self.engine
            .lock()
            .map_err(|_| StateMachineError::Sink("engine sink lock poisoned".to_owned()))?
            .install(&engine)?;
        self.ledger
            .lock()
            .map_err(|_| StateMachineError::Sink("tablet ledger lock poisoned".to_owned()))?
            .install_bytes(&ledger)
    }
}

/// Borrowed view of one replicated command's envelope, for command-type
/// dispatch in [`TabletGroupSink::apply`].
enum ReplicatedCommandRef<'a> {
    Catalog(&'a CommandEnvelope),
    Other,
}

fn replicated_envelope(command: &ReplicatedCommand) -> ReplicatedCommandRef<'_> {
    match command {
        ReplicatedCommand::Catalog(catalog) => ReplicatedCommandRef::Catalog(&catalog.envelope),
        _ => ReplicatedCommandRef::Other,
    }
}

/// One tablet group hosted on this node: the consensus group over the
/// composite engine+ledger sink, the descriptor it was opened with, and the
/// ownership reservation (spec section 12.3: one tablet storage core is
/// owned by one node process).
struct TabletGroup {
    group: Arc<ConsensusGroup<TcpTransport>>,
    descriptor: TabletDescriptor,
    sink: Arc<Mutex<TabletGroupSink>>,
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
    meta: Option<Arc<MetaGroup<TcpTransport>>>,
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
                Some(Arc::new(group))
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
        let ledger = TabletLedger::open(&engine_config.group_dir())?;
        let sink = Arc::new(Mutex::new(TabletGroupSink {
            engine: open_engine_sink(&engine_config)?,
            ledger: Arc::new(Mutex::new(ledger)),
        }));
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
        let dyn_sink: Arc<Mutex<dyn ApplySink>> = sink.clone();
        let group = ConsensusGroup::create(group_config, transport, dyn_sink).await?;
        Ok(TabletGroup {
            group: Arc::new(group),
            descriptor: descriptor.clone(),
            sink,
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
        self.meta.as_deref()
    }

    /// The consensus group of one tablet hosted on this node.
    pub fn tablet_group(&self, tablet_id: TabletId) -> Option<&ConsensusGroup<TcpTransport>> {
        self.tablets.get(&tablet_id).map(|tablet| &*tablet.group)
    }

    /// The applied keyspace ledger of one tablet hosted on this node (the
    /// interim data plane; see the module docs).
    pub fn tablet_ledger(&self, tablet_id: TabletId) -> Option<Arc<Mutex<TabletLedger>>> {
        self.tablets.get(&tablet_id).map(|tablet| {
            tablet
                .sink
                .lock()
                .expect("tablet sink lock poisoned")
                .ledger()
        })
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
        self.create_hosted_replica(descriptor, bootstrap_voters)
            .await?;
        if publish_to_meta {
            self.publish_tablet_descriptor(descriptor, control).await?;
        }
        Ok(())
    }

    /// Creates (or validates the existing) local replica of `descriptor`:
    /// allocates the section 12.3 layout, opens the [`ConsensusGroup`] over
    /// the composite engine+ledger sink, registers the group in the
    /// transport registry, and — when `bootstrap_voters` is `Some` and the
    /// group is still pristine — bootstraps it with that voter set (call on
    /// exactly one replica). Idempotent for an identical descriptor; a
    /// different descriptor for an already-open tablet fails closed.
    async fn create_hosted_replica(
        &mut self,
        descriptor: &TabletDescriptor,
        bootstrap_voters: Option<&[(NodeId, String)]>,
    ) -> Result<(), RuntimeError> {
        descriptor.validate()?;
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
        // Bounded retries: membership changes are serialized, so wait out
        // any in-flight change (bootstrap, an election, or a concurrent admin
        // move); a transient `MembershipInProgress` after the wait re-waits.
        // Idempotent under §11.7 retries: an earlier attempt may have died
        // between the learner add and the promote, so consult the group's
        // live membership each attempt and issue only the missing steps.
        for _ in 0..10 {
            tablet
                .group
                .wait_uniform_membership(Duration::from_secs(30))
                .await?;
            let (voters, learners) = tablet.group.members();
            if voters.contains(&replica.raft_node_id) {
                return Ok(());
            }
            if !learners.contains(&replica.raft_node_id) {
                // `add_learner` blocks until the learner is line-rate: the
                // snapshot/catch-up step of the movement protocol (spec
                // section 12.7).
                match tablet
                    .group
                    .add_learner(replica.raft_node_id, BasicNode::new(address.to_owned()))
                    .await
                {
                    Ok(()) => {}
                    Err(ConsensusError::MembershipInProgress) => continue,
                    Err(error) => return Err(error.into()),
                }
            }
            match tablet.group.promote(replica.raft_node_id).await {
                Ok(()) => return Ok(()),
                Err(ConsensusError::MembershipInProgress) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(RuntimeError::InvalidRequest(format!(
            "membership change for tablet {tablet_id} did not settle"
        )))
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

    // -----------------------------------------------------------------------
    // Split/merge seam bindings (spec sections 12.5-12.6)
    // -----------------------------------------------------------------------

    /// The meta group this node hosts, or an [`RuntimeError::InvalidRequest`]
    /// when it hosts none (the split/merge drivers and the hosted-tablet
    /// reconciler require one).
    fn meta_group_or_err(&self) -> Result<Arc<MetaGroup<TcpTransport>>, RuntimeError> {
        self.meta
            .clone()
            .ok_or_else(|| RuntimeError::InvalidRequest("this node hosts no meta group".to_owned()))
    }

    /// Plans a fresh split of `tablet_id` against the meta state (the
    /// descriptor/generation authority): the split key, two fresh child
    /// tablets on the source's replica nodes, and replica raft ids from the
    /// meta-owned allocator.
    async fn plan_split(
        &self,
        tablet_id: TabletId,
        split_key: Option<Key>,
        control: &ExecutionControl,
    ) -> Result<SplitPlan, RuntimeError> {
        let meta = self.meta_group_or_err()?;
        let source = meta.state().tablet(tablet_id).cloned().ok_or_else(|| {
            RuntimeError::InvalidRequest(format!("tablet {tablet_id} is not in the meta state"))
        })?;
        if source.state != TabletState::Active {
            return Err(RuntimeError::InvalidRequest(format!(
                "tablet {tablet_id} is in state {}, expected Active",
                source.state
            )));
        }
        if source.replica_on(self.identity.node_id).is_none() {
            return Err(RuntimeError::InvalidRequest(format!(
                "this node hosts no replica of tablet {tablet_id}"
            )));
        }
        let replica_count = source.replicas.len();
        let raft_ids = meta
            .allocate_raft_node_ids(
                2 * u32::try_from(replica_count).unwrap_or(u32::MAX),
                control,
            )
            .await?;
        let allocation = |ids: &[RaftNodeId]| ChildAllocation {
            tablet_id: TabletId::new_random(),
            raft_group_id: RaftGroupId::new_random(),
            replicas: source
                .replicas
                .iter()
                .zip(ids)
                .map(|(replica, raft_node_id)| ReplicaDescriptor {
                    node_id: replica.node_id,
                    role: replica.role,
                    raft_node_id: *raft_node_id,
                })
                .collect(),
        };
        let selection = match split_key {
            Some(key) => SplitKeySelection::Explicit(key),
            None => SplitKeySelection::Midpoint,
        };
        let plan = TabletSplitPlanner::new(self.node_data.clone()).plan(
            &source,
            selection,
            now_timestamp(),
            [
                allocation(&raft_ids[..replica_count]),
                allocation(&raft_ids[replica_count..]),
            ],
        )?;
        Ok(plan)
    }

    /// Creates the local child/replacement replicas of a split (or merge)
    /// plan when this node hosts one, bootstrapping each pristine group with
    /// its full voter set (peers adopt through
    /// [`NodeRuntime::sync_hosted_tablets`] with no bootstrap), and returns
    /// the child-state sinks bound to the live groups.
    async fn child_sinks(
        &mut self,
        children: &[TabletDescriptor; 2],
        control: &ExecutionControl,
    ) -> Result<[RuntimeChildSink; 2], RuntimeError> {
        for child in children {
            // Already-hosted groups (any generation/state) are reused as
            // they are; a resumed split re-publishes nothing here.
            if child.replica_on(self.identity.node_id).is_some()
                && !self.tablets.contains_key(&child.tablet_id)
            {
                let voters: Vec<(NodeId, String)> = child
                    .replicas
                    .iter()
                    .filter_map(|replica| {
                        self.peers
                            .get(&replica.node_id)
                            .map(|address| (replica.node_id, address.clone()))
                    })
                    .collect();
                self.create_hosted_replica(child, Some(voters.as_slice()))
                    .await?;
            }
        }
        let mut sinks = Vec::with_capacity(2);
        for child in children {
            let group = self
                .tablets
                .get(&child.tablet_id)
                .ok_or_else(|| {
                    RuntimeError::InvalidRequest(format!(
                        "child tablet {} is not hosted on this node",
                        child.tablet_id
                    ))
                })?
                .group
                .clone();
            sinks.push(RuntimeChildSink {
                group,
                handle: tokio::runtime::Handle::current(),
                control: control.clone(),
                staged: None,
            });
        }
        let [lower, upper]: [RuntimeChildSink; 2] = sinks.try_into().map_err(|_| {
            RuntimeError::InvalidRequest("expected exactly two child sinks".to_owned())
        })?;
        Ok([lower, upper])
    }

    /// Shuts down and removes one hosted tablet group (the ownership
    /// reservation releases with the drop). Absent tablets are a no-op.
    async fn drop_hosted_tablet(&mut self, tablet_id: TabletId) -> Result<(), RuntimeError> {
        if let Some(tablet) = self.tablets.remove(&tablet_id) {
            tablet.group.shutdown().await?;
        }
        Ok(())
    }

    /// Adopts the persisted `tablet.json` descriptor into the in-memory copy
    /// when its generation advanced (the split/merge executors persist every
    /// transition they publish).
    fn refresh_local_descriptor(&mut self, layout: &TabletLayout) -> Result<(), RuntimeError> {
        let descriptor = layout.load_metadata()?;
        if let Some(tablet) = self.tablets.get_mut(&layout.tablet_id()) {
            if descriptor.generation > tablet.descriptor.generation {
                tablet.descriptor = descriptor;
            }
        }
        Ok(())
    }

    /// Executes one phase of the split of `tablet_id` (spec section 12.5),
    /// resuming from the persisted progress record when one exists
    /// (`split_key` is ignored then: the recorded plan rules) and planning a
    /// fresh split otherwise (`None` selects the deterministic midpoint of
    /// the source bounds). Returns the newly completed phase and, once the
    /// atomic publication has happened, its [`SplitPublishCommand`].
    ///
    /// The driver runs on a node hosting both the meta group and a source
    /// replica. Child replicas are created locally with the plan; peers
    /// adopt them through [`NodeRuntime::sync_hosted_tablets`] (the child
    /// groups elect once a quorum of their replicas is up, so the build and
    /// catch-up phases proceed after the peers have synced).
    pub async fn split_step(
        &mut self,
        tablet_id: TabletId,
        split_key: Option<Key>,
        control: &ExecutionControl,
    ) -> Result<(SplitPhase, Option<SplitPublishCommand>), RuntimeError> {
        let meta = self.meta_group_or_err()?;
        let raft_group_id = match self.tablets.get(&tablet_id) {
            Some(tablet) => tablet.descriptor.raft_group_id,
            // After the publish step the local source group is already
            // dropped; only the retire step remains, and it works off the
            // layout and the meta plane alone.
            None => meta
                .state()
                .tablet(tablet_id)
                .map(|descriptor| descriptor.raft_group_id)
                .ok_or_else(|| {
                    RuntimeError::InvalidRequest(format!("this node hosts no tablet {tablet_id}"))
                })?,
        };
        let source_layout = TabletLayout::new(self.node_data.clone(), tablet_id, raft_group_id);
        let progress = split_progress(&source_layout)?;
        let hosted = self.tablets.contains_key(&tablet_id);
        if !hosted
            && progress
                .as_ref()
                .is_none_or(|record| record.phase < SplitPhase::Published)
        {
            return Err(RuntimeError::InvalidRequest(format!(
                "this node hosts no tablet {tablet_id}"
            )));
        }
        let keyspace = match self.tablet_ledger(tablet_id) {
            Some(ledger) => RuntimeKeyspace { ledger },
            // The group is gone but the layout remains: the on-disk ledger
            // checkpoint is written on every apply, so it is exactly as
            // fresh as the live one (and the remaining retire step never
            // reads it).
            None => RuntimeKeyspace {
                ledger: Arc::new(Mutex::new(TabletLedger::open(&source_layout.group_dir())?)),
            },
        };
        let plane = RuntimeMetaPlane {
            meta: meta.clone(),
            handle: tokio::runtime::Handle::current(),
            control: control.clone(),
        };
        let mut executor = match progress {
            Some(progress) => {
                let children = progress.plan().child_descriptors();
                let sinks = self.child_sinks(&children, control).await?;
                SplitExecutor::resume(source_layout.clone(), plane, keyspace, sinks)?.ok_or_else(
                    || {
                        RuntimeError::InvalidRequest(format!(
                            "split progress of tablet {tablet_id} vanished mid-resume"
                        ))
                    },
                )?
            }
            None => {
                let plan = self.plan_split(tablet_id, split_key, control).await?;
                let sinks = self.child_sinks(&plan.child_descriptors(), control).await?;
                SplitExecutor::begin(plan, source_layout.clone(), plane, keyspace, sinks)?
            }
        };
        // The retire step's teardown removes the source directory; the local
        // group must not be running when that happens.
        if executor.phase() == SplitPhase::Published {
            self.drop_hosted_tablet(tablet_id).await?;
        }
        let phase = tokio::task::spawn_blocking(move || executor.step())
            .await
            .map_err(|error| {
                RuntimeError::InvalidRequest(format!("split driver task failed: {error}"))
            })??;
        if matches!(phase, SplitPhase::MarkedSplitting | SplitPhase::Published) {
            self.refresh_local_descriptor(&source_layout)?;
        }
        let published = if phase >= SplitPhase::Published {
            // Deterministic from the recorded plan; recomputation after a
            // crash in the barrier yields the identical command. (At the
            // terminal phase the record is gone with the source teardown;
            // `split_tablet` kept the command from the publish step.)
            match split_progress(&source_layout)? {
                Some(record) => Some(SplitPublishCommand::from_plan(&record.plan())?),
                None => None,
            }
        } else {
            None
        };
        Ok((phase, published))
    }

    /// Drives the split of `tablet_id` to completion (spec section 12.5),
    /// returning the atomic publication. Resumes an in-progress split after
    /// a crash. A [`SplitError::SourceRetained`] surfaces with the split
    /// parked at [`SplitPhase::Published`]: drop the old-generation pins and
    /// call again.
    pub async fn split_tablet(
        &mut self,
        tablet_id: TabletId,
        split_key: Option<Key>,
        control: &ExecutionControl,
    ) -> Result<SplitPublishCommand, RuntimeError> {
        let mut published = None;
        loop {
            let (phase, command) = self
                .split_step(tablet_id, split_key.clone(), control)
                .await?;
            if command.is_some() {
                published = command;
            }
            if phase == SplitPhase::SourceRetired {
                break;
            }
        }
        published.ok_or_else(|| {
            RuntimeError::InvalidRequest(format!(
                "split of tablet {tablet_id} completed without a publication"
            ))
        })
    }

    /// Aborts the in-progress split of `tablet_id` (spec section 12.5): the
    /// children are removed from the meta state and torn down locally, the
    /// source is republished `Active`, and the persisted progress record is
    /// cleared. Idempotent — a second call is a no-op; fails closed once the
    /// split reached [`SplitPhase::Published`].
    pub async fn abort_split(
        &mut self,
        tablet_id: TabletId,
        control: &ExecutionControl,
    ) -> Result<SplitAbortReport, RuntimeError> {
        let meta = self.meta_group_or_err()?;
        let source_layout = {
            let source = self.hosted_tablet(tablet_id)?;
            TabletLayout::new(
                self.node_data.clone(),
                tablet_id,
                source.descriptor.raft_group_id,
            )
        };
        // The local child groups must not be running when the abort tears
        // their directories down.
        if let Some(progress) = split_progress(&source_layout)? {
            if progress.phase >= SplitPhase::Published {
                return Err(SplitError::CannotAbort {
                    tablet: tablet_id,
                    phase: progress.phase,
                }
                .into());
            }
            for child in &progress.children {
                self.drop_hosted_tablet(child.tablet_id).await?;
            }
        }
        let mut plane = RuntimeMetaPlane {
            meta,
            handle: tokio::runtime::Handle::current(),
            control: control.clone(),
        };
        let layout = source_layout.clone();
        let report = tokio::task::spawn_blocking(move || abort_split(&layout, &mut plane))
            .await
            .map_err(|error| {
                RuntimeError::InvalidRequest(format!("split abort task failed: {error}"))
            })??;
        // The source's local replica metadata follows the restored
        // descriptor (the abort persisted it already).
        self.refresh_local_descriptor(&source_layout)?;
        Ok(report)
    }

    /// Executes one phase of the merge of `first` and `second` (spec section
    /// 12.6), resuming from the persisted progress record when one exists
    /// and planning a fresh merge otherwise. Returns the newly completed
    /// phase and, once the atomic publication has happened, its
    /// [`MergePublishCommand`]. The driver runs on a node hosting the meta
    /// group and a replica of both sources (merge validation requires the
    /// sources to share their placement, so one node hosts both).
    pub async fn merge_step(
        &mut self,
        first: TabletId,
        second: TabletId,
        control: &ExecutionControl,
    ) -> Result<(MergePhase, Option<MergePublishCommand>), RuntimeError> {
        let meta = self.meta_group_or_err()?;
        // After the publish step the local source groups are already
        // dropped; the retire step works off the layouts and the meta plane.
        let layout_of = |tablet_id: TabletId| -> Result<TabletLayout, RuntimeError> {
            let raft_group_id = match self.tablets.get(&tablet_id) {
                Some(tablet) => tablet.descriptor.raft_group_id,
                None => meta
                    .state()
                    .tablet(tablet_id)
                    .map(|descriptor| descriptor.raft_group_id)
                    .ok_or_else(|| {
                        RuntimeError::InvalidRequest(format!(
                            "this node hosts no tablet {tablet_id}"
                        ))
                    })?,
            };
            Ok(TabletLayout::new(
                self.node_data.clone(),
                tablet_id,
                raft_group_id,
            ))
        };
        let first_layout = layout_of(first)?;
        let second_layout = layout_of(second)?;
        // Everything up to the retire step reads the local keyspaces: both
        // sources must be hosted (merge validation requires the sources to
        // share their placement, so one node hosts both). At Published only
        // the retire step remains, and it works off the layouts alone.
        let phase_in_flight = match (
            merge_progress(&first_layout)?,
            merge_progress(&second_layout)?,
        ) {
            (Some(progress), None) => Some(progress.phase),
            (None, None) => None,
            _ => {
                return Err(RuntimeError::InvalidRequest(
                    "merge progress records on both sources: corrupt state".to_owned(),
                ));
            }
        };
        let retired_only = matches!(
            phase_in_flight,
            Some(MergePhase::Published | MergePhase::SourcesRetired)
        );
        if !retired_only
            && (!self.tablets.contains_key(&first) || !self.tablets.contains_key(&second))
        {
            return Err(RuntimeError::InvalidRequest(format!(
                "this node must host both merge sources ({first}, {second})"
            )));
        }
        let keyspace_of = |layout: &TabletLayout| -> Result<RuntimeKeyspace, RuntimeError> {
            match self.tablet_ledger(layout.tablet_id()) {
                Some(ledger) => Ok(RuntimeKeyspace { ledger }),
                None => Ok(RuntimeKeyspace {
                    ledger: Arc::new(Mutex::new(TabletLedger::open(&layout.group_dir())?)),
                }),
            }
        };
        let (first_keyspace, second_keyspace) =
            (keyspace_of(&first_layout)?, keyspace_of(&second_layout)?);
        // The progress record lives in the LOWER source's directory.
        let ordered_layouts = |lower_first: bool| {
            if lower_first {
                [first_layout.clone(), second_layout.clone()]
            } else {
                [second_layout.clone(), first_layout.clone()]
            }
        };
        let plane = RuntimeMetaPlane {
            meta: meta.clone(),
            handle: tokio::runtime::Handle::current(),
            control: control.clone(),
        };
        let mut executor = match phase_in_flight {
            Some(_) => {
                let progress = merge_progress(&first_layout)?
                    .or(merge_progress(&second_layout)?)
                    .expect("phase_in_flight is Some");
                let lower_first = progress.sources[0].tablet_id == first;
                let sink = self.replacement_sink(&progress.plan(), control).await?;
                let keyspaces = if lower_first {
                    [first_keyspace, second_keyspace]
                } else {
                    [second_keyspace, first_keyspace]
                };
                MergeExecutor::resume(ordered_layouts(lower_first), plane, keyspaces, sink)?
                    .ok_or_else(|| {
                        RuntimeError::InvalidRequest(
                            "merge progress vanished mid-resume".to_owned(),
                        )
                    })?
            }
            None => {
                let plan = self.plan_merge(first, second, control).await?;
                let lower_first = plan.sources[0].tablet_id == first;
                let sink = self.replacement_sink(&plan, control).await?;
                let keyspaces = if lower_first {
                    [first_keyspace, second_keyspace]
                } else {
                    [second_keyspace, first_keyspace]
                };
                MergeExecutor::begin(plan, ordered_layouts(lower_first), plane, keyspaces, sink)?
            }
        };
        // The retire step tears both sources down; neither local group may
        // be running then.
        if executor.phase() == MergePhase::Published {
            self.drop_hosted_tablet(first).await?;
            self.drop_hosted_tablet(second).await?;
        }
        let phase = tokio::task::spawn_blocking(move || executor.step())
            .await
            .map_err(|error| {
                RuntimeError::InvalidRequest(format!("merge driver task failed: {error}"))
            })??;
        if matches!(phase, MergePhase::MarkedMerging | MergePhase::Published) {
            self.refresh_local_descriptor(&first_layout)?;
            self.refresh_local_descriptor(&second_layout)?;
        }
        let published = if phase >= MergePhase::Published {
            let progress = merge_progress(&first_layout)?.or(merge_progress(&second_layout)?);
            match progress {
                Some(record) => Some(MergePublishCommand::from_plan(&record.plan())?),
                None => None,
            }
        } else {
            None
        };
        Ok((phase, published))
    }

    /// Drives the merge of `first` and `second` to completion (spec section
    /// 12.6), returning the atomic publication. Resumes an in-progress merge
    /// after a crash.
    pub async fn merge_tablets(
        &mut self,
        first: TabletId,
        second: TabletId,
        control: &ExecutionControl,
    ) -> Result<MergePublishCommand, RuntimeError> {
        let mut published = None;
        loop {
            let (phase, command) = self.merge_step(first, second, control).await?;
            if command.is_some() {
                published = command;
            }
            if phase == MergePhase::SourcesRetired {
                break;
            }
        }
        published.ok_or_else(|| {
            RuntimeError::InvalidRequest("merge completed without a publication".to_owned())
        })
    }

    /// Plans a fresh merge of the pair against the meta state and the local
    /// ledgers (spec section 12.6's requirement list): same table/schema,
    /// adjacent ranges, identical placement, no active schema job, combined
    /// size under the threshold. The replacement lands on the sources' node
    /// set with fresh meta-allocated raft ids.
    async fn plan_merge(
        &self,
        first: TabletId,
        second: TabletId,
        control: &ExecutionControl,
    ) -> Result<MergePlan, RuntimeError> {
        let meta = self.meta_group_or_err()?;
        let state = meta.state();
        let descriptor_of = |tablet_id: TabletId| -> Result<TabletDescriptor, RuntimeError> {
            state.tablet(tablet_id).cloned().ok_or_else(|| {
                RuntimeError::InvalidRequest(format!("tablet {tablet_id} is not in the meta state"))
            })
        };
        let (first_desc, second_desc) = (descriptor_of(first)?, descriptor_of(second)?);
        let schema = state.table(first_desc.table_id).ok_or_else(|| {
            RuntimeError::InvalidRequest(format!(
                "table {} of tablet {first} is not in the meta state",
                first_desc.table_id
            ))
        })?;
        let active_schema_job = state
            .schema_jobs
            .values()
            .find(|job| job.table_id == first_desc.table_id && !job.state.is_terminal())
            .map(|job| job.job_id);
        let size_of = |tablet_id: TabletId| -> Result<u64, RuntimeError> {
            Ok(self
                .tablet_ledger(tablet_id)
                .ok_or_else(|| {
                    RuntimeError::InvalidRequest(format!("this node hosts no tablet {tablet_id}"))
                })?
                .lock()
                .expect("tablet ledger lock poisoned")
                .size_bytes())
        };
        let replica_count = first_desc.replicas.len();
        let raft_ids = meta
            .allocate_raft_node_ids(u32::try_from(replica_count).unwrap_or(u32::MAX), control)
            .await?;
        let allocation = ChildAllocation {
            tablet_id: TabletId::new_random(),
            raft_group_id: RaftGroupId::new_random(),
            replicas: first_desc
                .replicas
                .iter()
                .zip(&raft_ids)
                .map(|(replica, raft_node_id)| ReplicaDescriptor {
                    node_id: replica.node_id,
                    role: replica.role,
                    raft_node_id: *raft_node_id,
                })
                .collect(),
        };
        let plan = MergePlanner::new(self.node_data.clone()).plan(
            MergeInputs {
                first: first_desc,
                second: second_desc,
                first_schema: schema.schema_version,
                second_schema: schema.schema_version,
                active_schema_job,
                first_size_bytes: size_of(first)?,
                second_size_bytes: size_of(second)?,
                max_merged_size_bytes: DEFAULT_MAX_MERGED_SIZE_BYTES,
            },
            now_timestamp(),
            allocation,
        )?;
        Ok(plan)
    }

    /// The replacement-state sink of a merge plan, creating the local
    /// replacement replica (bootstrapped with its full voter set) when this
    /// node hosts one.
    async fn replacement_sink(
        &mut self,
        plan: &MergePlan,
        control: &ExecutionControl,
    ) -> Result<RuntimeChildSink, RuntimeError> {
        let descriptor = plan.replacement_descriptor();
        if descriptor.replica_on(self.identity.node_id).is_some()
            && !self.tablets.contains_key(&descriptor.tablet_id)
        {
            let voters: Vec<(NodeId, String)> = descriptor
                .replicas
                .iter()
                .filter_map(|replica| {
                    self.peers
                        .get(&replica.node_id)
                        .map(|address| (replica.node_id, address.clone()))
                })
                .collect();
            self.create_hosted_replica(&descriptor, Some(voters.as_slice()))
                .await?;
        }
        let group = self
            .tablets
            .get(&descriptor.tablet_id)
            .ok_or_else(|| {
                RuntimeError::InvalidRequest(format!(
                    "replacement tablet {} is not hosted on this node",
                    descriptor.tablet_id
                ))
            })?
            .group
            .clone();
        Ok(RuntimeChildSink {
            group,
            handle: tokio::runtime::Handle::current(),
            control: control.clone(),
            staged: None,
        })
    }

    /// Reconciles the hosted tablet replicas with the meta state (spec
    /// sections 12.3, 12.5-12.6): creates the local replica of every
    /// descriptor listing this node that is not yet hosted (split/merge
    /// children adopt on their replica nodes), refreshes the persisted
    /// `tablet.json` and the in-memory descriptor of every hosted tablet
    /// whose generation advanced in meta, and tears down hosted groups
    /// whose descriptor left the meta state or no longer lists this node
    /// (retired sources). Requires the local meta group; the pass is
    /// idempotent and safe to repeat.
    pub async fn sync_hosted_tablets(
        &mut self,
        _control: &ExecutionControl,
    ) -> Result<HostedSyncReport, RuntimeError> {
        let meta = self.meta_group_or_err()?;
        let state = meta.state();
        let node_id = self.identity.node_id;
        let mut report = HostedSyncReport::default();
        // Create: descriptors listing this node, not yet hosted.
        for record in state.tablets.values() {
            let descriptor = &record.descriptor;
            if self.tablets.contains_key(&descriptor.tablet_id)
                || descriptor.replica_on(node_id).is_none()
            {
                continue;
            }
            self.create_hosted_replica(descriptor, None).await?;
            report.created.push(descriptor.tablet_id);
        }
        // Refresh: meta's generation advanced past the local copy (b2: the
        // persisted tablet.json follows first, then the in-memory copy).
        for record in state.tablets.values() {
            let published = &record.descriptor;
            let Some(tablet) = self.tablets.get(&published.tablet_id) else {
                continue;
            };
            if published.generation <= tablet.descriptor.generation {
                continue;
            }
            if published.raft_group_id != tablet.descriptor.raft_group_id {
                return Err(RuntimeError::InvalidRequest(format!(
                    "meta descriptor for tablet {} names a different raft group than the \
                     hosted replica",
                    published.tablet_id
                )));
            }
            if published.replica_on(node_id).is_none() {
                continue; // torn down below, not refreshed
            }
            let layout = TabletLayout::new(
                self.node_data.clone(),
                published.tablet_id,
                published.raft_group_id,
            );
            layout.store_metadata(published)?;
            self.tablets
                .get_mut(&published.tablet_id)
                .expect("checked above")
                .descriptor = published.clone();
            report.refreshed.push(published.tablet_id);
        }
        // Teardown: hosted, but the descriptor left the meta state or no
        // longer lists this node.
        let outgoing: Vec<TabletId> = self
            .tablets
            .keys()
            .filter(|tablet_id| {
                state
                    .tablets
                    .get(tablet_id)
                    .is_none_or(|record| record.descriptor.replica_on(node_id).is_none())
            })
            .copied()
            .collect();
        for tablet_id in outgoing {
            let Some(tablet) = self.tablets.remove(&tablet_id) else {
                continue;
            };
            let layout = TabletLayout::new(
                self.node_data.clone(),
                tablet_id,
                tablet.descriptor.raft_group_id,
            );
            tablet.group.shutdown().await?;
            drop(tablet);
            layout.teardown()?;
            report.torn_down.push(tablet_id);
        }
        Ok(report)
    }

    /// Writes rows into a hosted tablet's applied keyspace (the interim data
    /// plane; see the module docs): one raft-replicated upsert, committed
    /// and applied on every replica. The proposal rides the local group, so
    /// it must be the leader — a [`ConsensusError::NotLeader`] surfaces with
    /// the leader hint for the caller to route by.
    pub async fn write_tablet_rows(
        &self,
        tablet_id: TabletId,
        entries: &[(Key, Vec<u8>)],
        control: &ExecutionControl,
    ) -> Result<GroupCommitReceipt, RuntimeError> {
        let tablet = self.hosted_tablet(tablet_id)?;
        let envelope = CommandEnvelope::new(
            COMMAND_TYPE_TABLET_DATA,
            new_data_command_id()?,
            TabletDataCommandRecord::new(TabletDataCommand::Upsert {
                entries: entries.to_vec(),
            })
            .encode(),
        );
        let receipt = tablet
            .group
            .propose(CommandKind::Catalog, envelope, control)
            .await?;
        Ok(receipt)
    }

    /// The current applied rows of a hosted tablet (the local replica's
    /// point-in-time view; run a read barrier first for linearizability).
    pub fn tablet_rows(&self, tablet_id: TabletId) -> Result<BTreeMap<Key, Vec<u8>>, RuntimeError> {
        let ledger = self.tablet_ledger(tablet_id).ok_or_else(|| {
            RuntimeError::InvalidRequest(format!("this node hosts no tablet {tablet_id}"))
        })?;
        let rows = ledger
            .lock()
            .expect("tablet ledger lock poisoned")
            .current_rows();
        Ok(rows)
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

    /// Process-free crash simulation: stops every group's raft task without
    /// the graceful storage close (see [`ConsensusGroup::crash`]) and drops
    /// the listener, so a restarted [`NodeRuntime::start`] reopens exactly
    /// the durable state a power loss would have left. Tablet ownership
    /// guards release with the dropped groups.
    pub async fn crash(mut self) {
        // Stop accepting RPCs immediately (no drain).
        drop(self.server.take());
        for (_, tablet) in std::mem::take(&mut self.tablets) {
            match Arc::try_unwrap(tablet.group) {
                Ok(group) => group.crash().await,
                Err(group) => {
                    let _ = group.shutdown().await;
                }
            }
        }
        if let Some(meta) = self.meta.take() {
            match Arc::try_unwrap(meta) {
                Ok(meta) => meta.crash().await,
                Err(meta) => {
                    let _ = meta.shutdown().await;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The split/merge seam implementations (spec sections 12.5-12.6)
// ---------------------------------------------------------------------------

/// The default merge-size threshold feeding [`MergeInputs`] when the runtime
/// plans a merge (spec section 12.6's "combined size under threshold").
/// Operator-tunable thresholds are a follow-up wave's configuration knob.
pub const DEFAULT_MAX_MERGED_SIZE_BYTES: u64 = 64 * 1024 * 1024;

/// The current wall clock as an HLC timestamp (split/merge pin timestamps;
/// the runtime is not the cluster's timestamp authority, so these only ever
/// order executor-local work).
fn now_timestamp() -> HlcTimestamp {
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_micros() as u64)
        .unwrap_or(0);
    HlcTimestamp {
        physical_micros: micros,
        logical: 0,
        node_tiebreaker: 0,
    }
}

/// Mints a tablet-data command id.
fn new_data_command_id() -> Result<[u8; 16], RuntimeError> {
    let mut id = [0u8; 16];
    getrandom::getrandom(&mut id)
        .map_err(|error| RuntimeError::InvalidRequest(format!("CSPRNG failed: {error}")))?;
    Ok(id)
}

/// The [`TabletMetaPlane`] binding over this node's meta group (spec section
/// 12.1): every descriptor write is one quorum-committed meta command, and
/// the atomic split/merge publications ride the single
/// [`MetaCommand::PublishSplit`] / [`MetaCommand::PublishMerge`] commands —
/// never a descriptor-by-descriptor sequence. Constructed per executor step;
/// its proposals block on the node's tokio runtime from the executor's
/// blocking thread.
struct RuntimeMetaPlane {
    meta: Arc<MetaGroup<TcpTransport>>,
    handle: tokio::runtime::Handle,
    control: ExecutionControl,
}

impl RuntimeMetaPlane {
    fn propose(&self, command: MetaCommand) -> Result<(), MetaRejectionReason> {
        let meta = self.meta.clone();
        let control = self.control.clone();
        self.handle
            .block_on(async move {
                meta.propose(crate::meta::new_command_id()?, command, &control)
                    .await
            })
            .map(|_| ())
            .map_err(|error| match error {
                MetaError::Rejected(reason) => reason,
                MetaError::Consensus(ConsensusError::NotLeader { leader }) => {
                    MetaRejectionReason::NotLeader { leader }
                }
                other => MetaRejectionReason::ProposalFailed {
                    reason: other.to_string(),
                },
            })
    }
}

impl TabletMetaPlane for RuntimeMetaPlane {
    fn set_tablet(&mut self, descriptor: &TabletDescriptor) -> Result<(), MetaRejectionReason> {
        self.propose(MetaCommand::SetTabletDescriptor {
            descriptor: descriptor.clone(),
        })
    }

    fn tablet(&self, tablet_id: TabletId) -> Option<TabletDescriptor> {
        self.meta.state().tablet(tablet_id).cloned()
    }

    fn remove_tablet(
        &mut self,
        tablet_id: TabletId,
        generation: u64,
    ) -> Result<(), MetaRejectionReason> {
        self.propose(MetaCommand::RemoveTabletDescriptor {
            tablet_id,
            generation,
        })
    }

    fn publish_split(&mut self, command: &SplitPublishCommand) -> Result<(), MetaRejectionReason> {
        self.propose(MetaCommand::PublishSplit {
            command: command.clone(),
        })
    }
}

impl MergeMetaPlane for RuntimeMetaPlane {
    fn publish_merge(&mut self, command: &MergePublishCommand) -> Result<(), MetaRejectionReason> {
        self.propose(MetaCommand::PublishMerge {
            command: command.clone(),
        })
    }
}

/// The [`TabletKeyspace`] binding over a hosted tablet's applied ledger
/// (the interim data plane; see the module docs).
struct RuntimeKeyspace {
    ledger: Arc<Mutex<TabletLedger>>,
}

impl TabletKeyspace for RuntimeKeyspace {
    fn pin_snapshot(&mut self, ts: HlcTimestamp) -> Result<Box<dyn SnapshotPin>, TabletDataError> {
        self.ledger
            .lock()
            .map_err(|_| TabletDataError::Keyspace("tablet ledger lock poisoned".to_owned()))?
            .pin(ts);
        Ok(Box::new(LedgerPin {
            ts,
            ledger: self.ledger.clone(),
        }))
    }

    fn snapshot_at(
        &self,
        ts: HlcTimestamp,
    ) -> Result<crate::split::RecordStream<'_>, TabletDataError> {
        let ledger = self
            .ledger
            .lock()
            .map_err(|_| TabletDataError::Keyspace("tablet ledger lock poisoned".to_owned()))?;
        Ok(Box::new(ledger.rows_at(ts).into_iter()))
    }

    fn deltas_after(
        &self,
        ts: HlcTimestamp,
    ) -> Result<crate::split::RecordStream<'_>, TabletDataError> {
        let ledger = self
            .ledger
            .lock()
            .map_err(|_| TabletDataError::Keyspace("tablet ledger lock poisoned".to_owned()))?;
        Ok(Box::new(ledger.deltas_after(ts).into_iter()))
    }
}

/// The [`ChildStateSink`] binding over a child/replacement tablet's live
/// raft group: the staged build is local and the install is one
/// [`TabletDataCommand::Replace`] committed through the group, so every
/// child replica applies the identical state atomically; catch-up deltas
/// ride [`TabletDataCommand::Upsert`] the same way.
struct RuntimeChildSink {
    group: Arc<ConsensusGroup<TcpTransport>>,
    handle: tokio::runtime::Handle,
    control: ExecutionControl,
    staged: Option<BTreeMap<Key, Vec<u8>>>,
}

impl RuntimeChildSink {
    fn propose_data(&self, command: TabletDataCommand) -> Result<(), TabletDataError> {
        let envelope = CommandEnvelope::new(
            COMMAND_TYPE_TABLET_DATA,
            new_data_command_id().map_err(|error| TabletDataError::Sink(error.to_string()))?,
            TabletDataCommandRecord::new(command).encode(),
        );
        self.handle
            .block_on(
                self.group
                    .propose(CommandKind::Catalog, envelope, &self.control),
            )
            .map(|_| ())
            .map_err(|error| TabletDataError::Sink(error.to_string()))
    }
}

impl ChildStateSink for RuntimeChildSink {
    fn begin_build(&mut self) -> Result<(), TabletDataError> {
        self.staged = Some(BTreeMap::new());
        Ok(())
    }

    fn stage(&mut self, key: &Key, value: &[u8]) -> Result<(), TabletDataError> {
        let staged = self.staged.as_mut().ok_or(TabletDataError::NoStagedBuild)?;
        staged.insert(key.clone(), value.to_vec());
        Ok(())
    }

    fn install_staged(&mut self) -> Result<(), TabletDataError> {
        let rows = self.staged.take().ok_or(TabletDataError::NoStagedBuild)?;
        self.propose_data(TabletDataCommand::Replace {
            rows: rows.into_iter().collect(),
        })
    }

    fn apply_delta(&mut self, key: &Key, value: &[u8]) -> Result<(), TabletDataError> {
        self.propose_data(TabletDataCommand::Upsert {
            entries: vec![(key.clone(), value.to_vec())],
        })
    }
}

/// The outcome of one [`NodeRuntime::sync_hosted_tablets`] pass.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HostedSyncReport {
    /// Local replicas created for meta descriptors listing this node.
    pub created: Vec<TabletId>,
    /// Hosted tablets whose persisted `tablet.json` and in-memory
    /// descriptor advanced to the meta generation.
    pub refreshed: Vec<TabletId>,
    /// Hosted groups shut down and torn down (their descriptor left the
    /// meta state or no longer lists this node).
    pub torn_down: Vec<TabletId>,
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

    // -- the interim tablet data plane -----------------------------------------

    fn ts(micros: u64) -> HlcTimestamp {
        HlcTimestamp {
            physical_micros: micros,
            logical: 0,
            node_tiebreaker: 0,
        }
    }

    fn pos(index: u64) -> LogPosition {
        LogPosition { term: 1, index }
    }

    fn upsert(key: &[u8], value: &[u8]) -> TabletDataCommand {
        TabletDataCommand::Upsert {
            entries: vec![(Key::from_bytes(key.to_vec()), value.to_vec())],
        }
    }

    #[test]
    fn tablet_ledger_applies_versions_and_partitions_the_timeline() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = TabletLedger::open(tmp.path()).unwrap();
        ledger
            .apply(&upsert(b"a", b"a@1"), ts(100), pos(1))
            .unwrap();
        ledger
            .apply(&upsert(b"b", b"b@1"), ts(100), pos(2))
            .unwrap();
        // A pin at the snapshot timestamp protects the at-or-below view
        // from compaction (the split/merge executor pattern).
        ledger.pin(ts(100));
        ledger
            .apply(&upsert(b"a", b"a@2"), ts(200), pos(3))
            .unwrap();
        // The at-or-below view splits exactly at the timestamp.
        assert_eq!(
            ledger.rows_at(ts(100)),
            BTreeMap::from([
                (Key::from_bytes(b"a".to_vec()), b"a@1".to_vec()),
                (Key::from_bytes(b"b".to_vec()), b"b@1".to_vec()),
            ])
        );
        assert_eq!(
            ledger.current_rows().get(&Key::from_bytes(b"a".to_vec())),
            Some(&b"a@2".to_vec())
        );
        // Deltas after the pin timestamp arrive in commit order.
        assert_eq!(
            ledger.deltas_after(ts(100)),
            vec![(Key::from_bytes(b"a".to_vec()), b"a@2".to_vec())]
        );
        assert!(ledger.deltas_after(ts(200)).is_empty());
        ledger.unpin(ts(100));
        // Redelivery at or below the watermark is skipped.
        ledger
            .apply(&upsert(b"z", b"z@1"), ts(300), pos(3))
            .unwrap();
        assert!(!ledger
            .current_rows()
            .contains_key(&Key::from_bytes(b"z".to_vec())));
        // The checkpoint survives a restart; the watermark dedups replay.
        let position = ledger.applied_position();
        drop(ledger);
        let reopened = TabletLedger::open(tmp.path()).unwrap();
        assert_eq!(reopened.applied_position(), position);
        assert_eq!(
            reopened.current_rows().get(&Key::from_bytes(b"a".to_vec())),
            Some(&b"a@2".to_vec())
        );
        // A corrupt checkpoint fails closed.
        std::fs::write(
            tmp.path()
                .join("raft")
                .join("state")
                .join(TABLET_LEDGER_FILENAME),
            b"junk",
        )
        .unwrap();
        assert!(TabletLedger::open(tmp.path()).is_err());
    }

    #[test]
    fn tablet_ledger_compacts_against_the_oldest_pin() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = TabletLedger::open(tmp.path()).unwrap();
        ledger
            .apply(&upsert(b"a", b"a@1"), ts(100), pos(1))
            .unwrap();
        // Pin at 150, then write newer versions: the at-or-below baseline
        // (a@1) survives compaction while the pin lives.
        ledger.pin(ts(150));
        ledger
            .apply(&upsert(b"a", b"a@2"), ts(200), pos(2))
            .unwrap();
        ledger
            .apply(&upsert(b"a", b"a@3"), ts(300), pos(3))
            .unwrap();
        assert_eq!(
            ledger.rows_at(ts(150)).get(&Key::from_bytes(b"a".to_vec())),
            Some(&b"a@1".to_vec())
        );
        assert_eq!(
            ledger.deltas_after(ts(150)),
            vec![
                (Key::from_bytes(b"a".to_vec()), b"a@2".to_vec()),
                (Key::from_bytes(b"a".to_vec()), b"a@3".to_vec()),
            ]
        );
        // Releasing the pin lets the chain collapse to the newest version.
        ledger.unpin(ts(150));
        ledger
            .apply(&upsert(b"a", b"a@4"), ts(400), pos(4))
            .unwrap();
        assert_eq!(
            ledger.rows_at(ts(150)).get(&Key::from_bytes(b"a".to_vec())),
            None
        );
        assert_eq!(
            ledger.current_rows().get(&Key::from_bytes(b"a".to_vec())),
            Some(&b"a@4".to_vec())
        );
        assert_eq!(ledger.pin_count(), 0);
    }

    #[test]
    fn tablet_ledger_replace_is_atomic_and_refused_under_a_live_pin() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ledger = TabletLedger::open(tmp.path()).unwrap();
        ledger
            .apply(&upsert(b"a", b"a@1"), ts(100), pos(1))
            .unwrap();
        ledger.pin(ts(150));
        let replace = TabletDataCommand::Replace {
            rows: vec![(Key::from_bytes(b"b".to_vec()), b"b@2".to_vec())],
        };
        assert!(ledger.apply(&replace, ts(200), pos(2)).is_err());
        ledger.unpin(ts(150));
        ledger.apply(&replace, ts(200), pos(2)).unwrap();
        assert_eq!(
            ledger.current_rows(),
            BTreeMap::from([(Key::from_bytes(b"b".to_vec()), b"b@2".to_vec())])
        );
        // The snapshot bytes install into a fresh ledger (raft catch-up).
        let bytes = ledger.snapshot_bytes().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();
        let mut follower = TabletLedger::open(follower_dir.path()).unwrap();
        follower.install_bytes(&bytes).unwrap();
        assert_eq!(follower.current_rows(), ledger.current_rows());
        assert!(follower.install_bytes(b"junk").is_err());
    }

    #[test]
    fn group_snapshot_frame_round_trips_and_fails_closed() {
        let engine = b"engine-half".to_vec();
        let ledger = b"ledger-half".to_vec();
        let frame = encode_group_snapshot(&engine, &ledger);
        let (engine_back, ledger_back) = decode_group_snapshot(&frame).unwrap();
        assert_eq!(engine_back, engine);
        assert_eq!(ledger_back, ledger);
        // Empty halves frame fine.
        let (empty_engine, empty_ledger) =
            decode_group_snapshot(&encode_group_snapshot(&[], &[])).unwrap();
        assert!(empty_engine.is_empty() && empty_ledger.is_empty());
        // Truncations and unknown versions fail closed: a short header or a
        // truncated engine half (the frame carries its length) is rejected;
        // a short ledger half is rejected at install.
        assert!(decode_group_snapshot(&frame[..6]).is_err());
        assert!(decode_group_snapshot(&frame[..12 + engine.len() - 1]).is_err());
        let mut future = frame.clone();
        future[..4].copy_from_slice(&99_u32.to_le_bytes());
        assert!(decode_group_snapshot(&future).is_err());
    }

    #[test]
    fn tablet_data_command_record_round_trips_and_fails_closed() {
        let record = TabletDataCommandRecord::new(upsert(b"k", b"v"));
        let decoded = TabletDataCommandRecord::decode(&record.encode()).unwrap();
        assert_eq!(decoded, record);
        assert!(TabletDataCommandRecord::decode(b"not json").is_err());
        let mut value: serde_json::Value = serde_json::from_slice(&record.encode()).unwrap();
        value["format_version"] = serde_json::json!(99);
        assert!(TabletDataCommandRecord::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    }
}
