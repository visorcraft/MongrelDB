//! Cluster meta (spec sections 11-12, Stages 2-3).
//!
//! Stage 2H (spec section 11.8, ADR-0010) landed the rolling-upgrade control
//! surface: the cluster feature level and feature registry, the
//! [`FeatureActivation`] record, rolling-upgrade planning
//! ([`plan_rolling_upgrade`]), and rollback assessment ([`assess_rollback`]).
//!
//! Stage 3A (spec section 12.1) lands the meta control plane itself: a
//! dedicated Raft group owning the cluster's control-plane state — never user
//! row data. [`MetaState`] is the deterministic, serde-versioned state
//! machine (membership and node descriptors, databases, table schemas,
//! tablet descriptors, replica placement and policies, schema/index jobs,
//! transaction status partitions, cluster settings, and the feature flags
//! above); [`MetaCommand`] is the versioned command enum riding
//! [`ReplicatedCommand::Catalog`] envelopes; [`MetaApplySink`] binds the
//! state to `mongreldb-consensus`'s apply path; and [`MetaGroup`] is the
//! bootstrap/membership/propose helper the node runtime drives.
//!
//! # Reconciliation notes
//!
//! - The descriptor family ([`TabletDescriptor`], [`ReplicaDescriptor`],
//!   [`ReplicaRole`], [`PartitionBounds`], [`TabletState`]) and the placement
//!   contract ([`PlacementPolicy`], [`LocalityConstraint`]) are the canonical
//!   `crate::tablet` / `crate::placement` types, re-exported here so
//!   `crate::meta::*` paths keep resolving. Meta records wrap them with the
//!   meta state's [`MetadataVersion`] ([`TabletRecord`], [`ReplicaPlacement`],
//!   [`PlacementPolicyRecord`]); a descriptor's own `generation` remains the
//!   last-writer-wins guard.
//! - [`SchemaJobKind`]/[`SchemaJobState`] minimally mirror the core job
//!   registry's concepts (`mongreldb-core` `jobs.rs`), which the cluster
//!   crate deliberately does not depend on. Distributed DDL (spec section
//!   12.11) reconciles the two registries.
//! - [`DefaultConsistency`] is a payload-free mirror of
//!   `mongreldb_consensus::read::ReadConsistency` suitable as a cluster-wide
//!   default (request-scoped token/timestamp variants carry no payload here).
//!
//! # Format v1 migration (spec sections 4.10, 17)
//!
//! The first meta control-plane build (format v1) replicated meta-local
//! minimal mirrors of the `crate::tablet` / `crate::placement` types. The
//! type reconciliation adopted the canonical types as format v2. v1 command
//! records and v1 state checkpoints remain decodable: every decode probes the
//! `format_version` field and routes v1 payloads through the [`v1`]
//! compatibility module, migrating them to the canonical shapes before apply:
//!
//! - partition bounds `{start, end}` map onto `{low, high}` with the v1
//!   semantics (start inclusive, end exclusive);
//! - tablet states map `Online -> Active`, `Offline -> Retiring` (`Creating`
//!   is unchanged);
//! - v1 replicas carried no per-group raft id; they gain
//!   `raft_node_id = raft_node_id(node_id)`, the projection the v1 group
//!   actually used;
//! - v1 voter constraints were hard requirements (`required = true`) and v1
//!   leader preferences soft ones (`required = false`);
//! - records keep their v1 `metadata_version`.
//!
//! New writes are stamped v2; v1 is accepted forever (the minimum supported
//! version constants stay at 1) so checkpoints at
//! `raft/state/meta-state.json` and log entries written by a v1 binary load
//! and replay after upgrade.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use mongreldb_consensus::error::ConsensusError;
use mongreldb_consensus::group::{ConsensusGroup, GroupCommitReceipt, GroupConfig};
use mongreldb_consensus::identity::{raft_node_id, CommandKind, RaftNodeId, ReplicatedCommand};
use mongreldb_consensus::network::RaftTransport;
use mongreldb_consensus::state_machine::{AppliedCommand, ApplySink, StateMachineError};
use mongreldb_consensus::storage::StorageConfig;
use mongreldb_log::commit_log::{ExecutionControl, LogPosition};
use mongreldb_log::envelope::CommandEnvelope;
use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::{
    DatabaseId, MetadataVersion, NodeId, RaftGroupId, SchemaVersion, TableId, TabletId,
};
use serde::{Deserialize, Serialize};

use crate::merge::MergePublishCommand;
use crate::node::{Incompatibility, NodeDescriptor, NodeState, VersionInfo};
use crate::split::SplitPublishCommand;
use crate::tablet::{Bound, Key};

/// Cluster-wide feature level (spec section 17: separate from binary
/// version; ADR-0010 decision 3).
///
/// The level never lowers: it rises only when a [`FeatureActivation`] is
/// applied at quorum, and rolling it back requires the restore-based path
/// documented in [`RollbackPath::RestoreFromBackup`].
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct ClusterFeatureLevel(pub u64);

impl ClusterFeatureLevel {
    /// The level of a cluster that has activated no features.
    pub const ZERO: Self = Self(0);
}

impl fmt::Display for ClusterFeatureLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Registry of gated features: feature name to the minimum
/// [`ClusterFeatureLevel`] at which the feature may be activated.
///
/// Declarations are append-only and levels are never reused for a different
/// feature (spec section 4.10). Stage 2H ships the activation mechanism
/// before the first gated feature — ADR-0010 requires feature work to land
/// dark at least one release before activation — so
/// [`FeatureRegistry::current`] is empty; later waves declare features there.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FeatureRegistry {
    required_level: BTreeMap<String, u64>,
}

impl FeatureRegistry {
    /// The feature registry of the running binary.
    pub fn current() -> Self {
        Self::default()
    }

    /// Declare a gated feature and the minimum level that activates it.
    pub fn declare(&mut self, feature: impl Into<String>, level: ClusterFeatureLevel) {
        self.required_level.insert(feature.into(), level.0);
    }

    /// The minimum level at which `feature` may be activated, if the feature
    /// is registered.
    pub fn required_level(&self, feature: &str) -> Option<ClusterFeatureLevel> {
        self.required_level
            .get(feature)
            .copied()
            .map(ClusterFeatureLevel)
    }

    /// Whether `feature` is active at `level` (spec section 11.8).
    pub fn feature_supported(&self, level: ClusterFeatureLevel, feature: &str) -> bool {
        self.required_level(feature)
            .is_some_and(|required| level >= required)
    }

    /// The registered feature names; a node's advertised
    /// [`VersionInfo::feature_set`] is drawn from this set.
    pub fn feature_names(&self) -> BTreeSet<String> {
        self.required_level.keys().cloned().collect()
    }
}

/// Record of one cluster feature activation (spec section 11.8).
///
/// Feature activation is a replicated catalog command (ADR-0010 decision 4):
/// the catalog-command variant that carries this record through the command
/// envelope lands with the meta-group integration, and the apply path there
/// re-runs [`FeatureActivation::validate`] at quorum. Defined here so the
/// record shape and the activation rule exist before that integration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureActivation {
    /// Registered name of the feature being activated.
    pub feature: String,
    /// Cluster feature level this activation raises the cluster to.
    pub level: ClusterFeatureLevel,
    /// Commit timestamp of the activation (assigned by the commit sequencer
    /// once the command is replicated).
    pub activated_at: HlcTimestamp,
    /// Node that proposed the activation.
    pub activated_by: NodeId,
}

/// Why a [`FeatureActivation`] may not be applied. Activation failures fail
/// closed (ADR-0010).
///
/// Serde derives exist so the meta group's rejection journal
/// ([`MetaRejection`]) can record the typed reason inside replicated state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum FeatureActivationError {
    /// The feature is not declared in this binary's registry.
    #[error("feature `{feature}` is not declared in the feature registry")]
    UnknownFeature {
        /// The feature that was to be activated.
        feature: String,
    },
    /// The activation level is below the feature's registered minimum.
    #[error(
        "feature `{feature}` requires cluster feature level {required}; \
         activation attempted at {attempted}"
    )]
    LevelBelowRequirement {
        /// The feature that was to be activated.
        feature: String,
        /// Registered minimum level for the feature.
        required: ClusterFeatureLevel,
        /// Level the activation attempted.
        attempted: ClusterFeatureLevel,
    },
    /// The activation would lower the cluster feature level; the level never
    /// regresses (ADR-0010: no in-place un-activate).
    #[error(
        "cluster feature level never lowers: current level {current}, \
         activation attempted at {attempted}"
    )]
    LevelRegression {
        /// Current cluster feature level.
        current: ClusterFeatureLevel,
        /// Level the activation attempted.
        attempted: ClusterFeatureLevel,
    },
    /// A voter's advertisement does not include the feature (spec section
    /// 11.8 step 5: enable new features only after every voter supports
    /// them).
    #[error("feature `{feature}` cannot activate: voter {node} does not support it")]
    UnsupportedByVoter {
        /// The feature that was to be activated.
        feature: String,
        /// The first voter whose [`VersionInfo::feature_set`] lacks it.
        node: NodeId,
    },
    /// Activation with no voters is meaningless; fail closed.
    #[error("feature activation requires at least one voter")]
    NoVoters,
}

impl FeatureActivation {
    /// Validate the activation against the registry, the current cluster
    /// level, and every voter's advertised [`VersionInfo`].
    ///
    /// `voters` must be exactly the current voter set of the group that will
    /// apply the command. The rule (spec section 11.8 step 5): a feature may
    /// activate only when every voter supports it, at a level that satisfies
    /// the registry minimum and never lowers the cluster level.
    pub fn validate(
        &self,
        registry: &FeatureRegistry,
        current_level: ClusterFeatureLevel,
        voters: &[NodeDescriptor],
    ) -> Result<(), FeatureActivationError> {
        let required = registry.required_level(&self.feature).ok_or_else(|| {
            FeatureActivationError::UnknownFeature {
                feature: self.feature.clone(),
            }
        })?;
        if self.level < required {
            return Err(FeatureActivationError::LevelBelowRequirement {
                feature: self.feature.clone(),
                required,
                attempted: self.level,
            });
        }
        if self.level < current_level {
            return Err(FeatureActivationError::LevelRegression {
                current: current_level,
                attempted: self.level,
            });
        }
        if voters.is_empty() {
            return Err(FeatureActivationError::NoVoters);
        }
        for voter in voters {
            if !voter.version_info.feature_set.contains(&self.feature) {
                return Err(FeatureActivationError::UnsupportedByVoter {
                    feature: self.feature.clone(),
                    node: voter.node_id,
                });
            }
        }
        Ok(())
    }
}

/// One ordered step of a rolling upgrade (spec section 11.8, ADR-0010
/// decision 6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpgradeStep {
    /// Upgrade one follower to the target binary, one at a time, waiting for
    /// it to rejoin and catch up before the next step.
    UpgradeFollower {
        /// The follower to upgrade.
        node_id: NodeId,
    },
    /// Move leadership off the current leader so its upgrade interrupts no
    /// writes.
    TransferLeadership {
        /// The leader to move leadership away from.
        from: NodeId,
    },
    /// Upgrade the former leader; it is always the last node upgraded.
    UpgradeFormerLeader {
        /// The former leader to upgrade.
        node_id: NodeId,
    },
    /// Final, explicit gate: propose [`FeatureActivation`]s for the new
    /// binary's features, only after every voter runs the target binary.
    /// Never implicit — activation is an operator decision applied at quorum
    /// (ADR-0010 decision 3).
    EnableNewFeatures,
}

/// A validated rolling-upgrade plan: the target advertisement plus the
/// ordered steps to reach it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpgradePlan {
    /// Version advertisement every node is upgraded to.
    pub target: VersionInfo,
    /// Ordered upgrade steps; see [`UpgradeStep`].
    pub steps: Vec<UpgradeStep>,
}

/// Why a rolling upgrade cannot be planned. Planning failures fail closed
/// (ADR-0010).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UpgradePlanError {
    /// No nodes were supplied.
    #[error("cannot plan a rolling upgrade for an empty membership")]
    EmptyMembership,
    /// The named leader is absent from the supplied membership.
    #[error("current leader {leader} is not present in the supplied membership")]
    LeaderNotInMembership {
        /// The leader that was looked up.
        leader: NodeId,
    },
    /// The same node appeared twice.
    #[error("node {node} appears more than once in the supplied membership")]
    DuplicateNode {
        /// The duplicated node.
        node: NodeId,
    },
    /// A node's advertisement cannot interoperate with the target binary
    /// (spec section 11.8 step 1: verify compatibility).
    #[error("node {node} is not compatible with the upgrade target: {incompatibility}")]
    IncompatibleNode {
        /// The incompatible node.
        node: NodeId,
        /// The first non-overlapping advertised range.
        incompatibility: Incompatibility,
    },
}

/// Plan a rolling upgrade of `nodes` to the `target` binary (spec section
/// 11.8).
///
/// Every node's advertised [`VersionInfo`] is verified against `target`
/// first (step 1); any mismatch fails closed with
/// [`UpgradePlanError::IncompatibleNode`]. The resulting plan upgrades
/// followers one at a time in membership order (step 2), transfers
/// leadership off `current_leader` (step 3 — omitted for a single-node
/// membership, where there is no peer to receive it), upgrades the former
/// leader last (step 4), and ends with the explicit enable-new-features gate
/// (step 5), which the operator executes via [`FeatureActivation`].
pub fn plan_rolling_upgrade(
    nodes: &[NodeDescriptor],
    current_leader: NodeId,
    target: &VersionInfo,
) -> Result<UpgradePlan, UpgradePlanError> {
    if nodes.is_empty() {
        return Err(UpgradePlanError::EmptyMembership);
    }
    for (index, node) in nodes.iter().enumerate() {
        if nodes[..index]
            .iter()
            .any(|prior| prior.node_id == node.node_id)
        {
            return Err(UpgradePlanError::DuplicateNode { node: node.node_id });
        }
    }
    if !nodes.iter().any(|node| node.node_id == current_leader) {
        return Err(UpgradePlanError::LeaderNotInMembership {
            leader: current_leader,
        });
    }
    for node in nodes {
        if let Err(incompatibility) = node.version_info.is_compatible_with(target) {
            return Err(UpgradePlanError::IncompatibleNode {
                node: node.node_id,
                incompatibility,
            });
        }
    }
    let mut steps = Vec::with_capacity(nodes.len() + 2);
    for node in nodes {
        if node.node_id != current_leader {
            steps.push(UpgradeStep::UpgradeFollower {
                node_id: node.node_id,
            });
        }
    }
    if nodes.len() > 1 {
        steps.push(UpgradeStep::TransferLeadership {
            from: current_leader,
        });
    }
    steps.push(UpgradeStep::UpgradeFormerLeader {
        node_id: current_leader,
    });
    steps.push(UpgradeStep::EnableNewFeatures);
    Ok(UpgradePlan {
        target: target.clone(),
        steps,
    })
}

/// The supported rollback path for an upgrade in flight (spec section 17;
/// ADR-0010 reversal strategy).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RollbackPath {
    /// Binary downgrade node by node, former leader last. Supported only
    /// before any feature activation: no required N-only command has been
    /// emitted and snapshots are still written in a format the previous
    /// reader accepts, so every byte of durable state remains
    /// previous-binary readable.
    BinaryDowngrade,
    /// Restore-based rollback: binary downgrade alone is insufficient once a
    /// feature has activated. Restore from a backup/snapshot taken before
    /// activation, then replay the committed log up to a pre-activation
    /// fence (spec section 17: on-disk downgrade is not implied).
    RestoreFromBackup,
}

/// Assessment of how an upgrade in flight may be abandoned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RollbackAssessment {
    /// The supported rollback path.
    pub path: RollbackPath,
    /// Features whose activation closed the binary-downgrade window; empty
    /// when [`RollbackPath::BinaryDowngrade`] is still available.
    pub activated_features: Vec<String>,
}

/// Assess the supported rollback path given the features activated so far.
///
/// Before any feature activation a node downgrade is safe
/// ([`RollbackPath::BinaryDowngrade`]); the first activation ends the
/// rollback window and leaves only the restore-based path (spec section 17).
pub fn assess_rollback(activations: &[FeatureActivation]) -> RollbackAssessment {
    let activated_features: Vec<String> = activations
        .iter()
        .map(|activation| activation.feature.clone())
        .collect();
    let path = if activated_features.is_empty() {
        RollbackPath::BinaryDowngrade
    } else {
        RollbackPath::RestoreFromBackup
    };
    RollbackAssessment {
        path,
        activated_features,
    }
}

// ---------------------------------------------------------------------------
// Stage 3A — meta control plane (spec section 12.1)
// ---------------------------------------------------------------------------
//
// The meta group is a dedicated Raft group owning control-plane state only;
// user row data never enters it (spec section 12.1). Commands ride
// `ReplicatedCommand::Catalog` envelopes stamped with
// [`COMMAND_TYPE_META_COMMAND`]; the apply path is deterministic and total:
// a refused command never faults the raft state machine — it is recorded in
// the bounded rejection journal ([`MetaState::rejections`]) and surfaced to
// the proposer by [`MetaGroup::propose`].
//
// # Serde versioning (spec sections 4.10, 17)
//
// [`MetaState`] and [`MetaCommandRecord`] carry explicit `format_version`
// fields checked on decode (fail closed outside the supported range). Unlike
// the crate's static metadata files, these types deliberately omit
// `deny_unknown_fields`: they travel in log entries and snapshots, where
// spec section 17 requires an N-1 node to ignore optional N fields during
// the rolling-upgrade window. Additive evolution lands as new optional
// fields with serde defaults; new required command variants ship dark behind
// feature activation (ADR-0010).

/// Format version of [`MetaCommandRecord`] payloads this build writes.
///
/// v2 reconciles the tablet/placement payload types onto the canonical
/// `crate::tablet` / `crate::placement` shapes; v1 payloads (meta-local
/// mirrors) remain decodable and migrate on read (see the module docs).
pub const META_COMMAND_FORMAT_VERSION: u32 = 2;
/// Oldest [`MetaCommandRecord`] format version this build accepts.
pub const MIN_SUPPORTED_META_COMMAND_FORMAT_VERSION: u32 = 1;
/// Format version of [`MetaState`] snapshots this build writes (see
/// [`META_COMMAND_FORMAT_VERSION`] for the v1 reconciliation).
pub const META_STATE_FORMAT_VERSION: u32 = 2;
/// Oldest [`MetaState`] snapshot format version this build accepts.
pub const MIN_SUPPORTED_META_STATE_FORMAT_VERSION: u32 = 1;
/// `CommandEnvelope::command_type` of meta control-plane commands.
///
/// Envelope discriminants are never reused (spec section 4.10): `1` is the
/// transaction command (`mongreldb-core` `commit_log`), `2` the engine
/// catalog command, and `3` the maintenance command (`mongreldb-core`
/// `replicated_apply`); `4` is the meta control-plane command.
pub const COMMAND_TYPE_META_COMMAND: u32 = 4;
/// Bound on [`MetaState::rejections`] (mirrors the engine catalog's
/// `COMMAND_HISTORY_LIMIT`).
pub const META_REJECTION_LIMIT: usize = 256;
/// First per-group raft node id the meta-owned allocator hands out (spec
/// section 12.1: the meta control plane owns the node-id ↔ raft-id mapping
/// for tablet groups). Id 0 is never allocated; the meta group itself uses
/// the `raft_node_id` projection of the member node ids, which the
/// allocator skips over (and [`MetaCommand::RegisterNode`] refuses a node
/// whose projection collides with an id already assigned to a replica).
pub const FIRST_RAFT_NODE_ID: u64 = 1;
/// Largest single [`MetaCommand::AllocateRaftNodeIds`] request.
pub const MAX_RAFT_NODE_ID_ALLOCATION: u32 = 4096;
/// Bound on [`MetaState::raft_id_allocations`] (the idempotent-replay
/// record of recent allocations).
pub const RAFT_ID_ALLOCATION_RECORD_LIMIT: usize = 1024;
/// Bound on the collision-skip scan of one allocation: the allocator
/// advances past at most this many already-used ids before refusing
/// (fail closed; in practice ids collide negligibly often).
pub const RAFT_ID_ALLOCATION_SCAN_LIMIT: u64 = 1 << 20;
/// Substrings (matched case-insensitively) that bar a key from the cluster
/// settings: secrets are never stored as plaintext cluster settings (spec
/// section 16.2). TLS private keys, backup credentials, and encryption-key
/// material live in static node configuration or the engine's key hierarchy,
/// never in replicated meta state. The match is deliberately conservative —
/// a legitimate key containing one of these substrings must be renamed.
pub const SECRET_SETTING_KEY_DENYLIST: &[&str] = &[
    "secret",
    "password",
    "passwd",
    "private_key",
    "api_key",
    "token",
    "credential",
];

/// The fixed cluster-setting keys ([`MetaCommand::SetClusterSetting`]).
/// `resource_groups.<name>` is the one dynamic key shape; everything else is
/// listed here.
pub const KNOWN_SETTING_KEYS: &[&str] = &[
    "history_retention_epochs",
    "backup.enabled",
    "backup.interval_seconds",
    "backup.retention_count",
    "default_consistency",
    "ai.max_concurrent_requests",
    "ai.max_memory_bytes",
    "jobs.max_concurrent",
];

/// Default for [`ClusterSettings::max_concurrent_jobs`] (mirrors the core job
/// registry's `DEFAULT_MAX_CONCURRENT_JOBS`).
pub const DEFAULT_MAX_CONCURRENT_JOBS: u32 = 2;
/// Default for [`BackupPolicy::interval_seconds`] (daily).
pub const DEFAULT_BACKUP_INTERVAL_SECONDS: u64 = 86_400;
/// Default for [`BackupPolicy::retention_count`].
pub const DEFAULT_BACKUP_RETENTION_COUNT: u32 = 7;
/// Default for [`AiLimits::max_concurrent_requests`].
pub const DEFAULT_AI_MAX_CONCURRENT_REQUESTS: u32 = 16;
/// Default for [`AiLimits::max_memory_bytes`] (512 MiB).
pub const DEFAULT_AI_MAX_MEMORY_BYTES: u64 = 512 * 1024 * 1024;

fn zero_metadata_version() -> MetadataVersion {
    MetadataVersion::ZERO
}

/// Lowercase hex of a byte string (command-id map keys; JSON map keys must
/// be strings).
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Errors of the meta control-plane surface (group factory, membership
/// workflow, proposals).
#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    /// Consensus group failure (including the routed
    /// [`ConsensusError::NotLeader`] leader hint, spec section 11.7).
    #[error(transparent)]
    Consensus(#[from] ConsensusError),
    /// Encoding a [`MetaCommandRecord`] failed.
    #[error("meta command encoding failed: {0}")]
    Encode(String),
    /// The command committed but the apply path refused it; the typed reason
    /// is also journaled in [`MetaState::rejections`].
    #[error("meta command refused at apply: {0}")]
    Rejected(MetaRejectionReason),
    /// The caller's request was malformed for this group (raft-id projection
    /// collision, mismatched group config, invalid workflow order).
    #[error("invalid meta group request: {0}")]
    InvalidRequest(String),
    /// Meta group I/O failure.
    #[error("meta group I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The caller-supplied operating-system CSPRNG failed.
    #[error("operating-system CSPRNG failed: {0}")]
    Rng(String),
    /// The sink's durable checkpoint failed verification (fails closed, spec
    /// section 4.10).
    #[error("corrupt meta state checkpoint: {0}")]
    CorruptCheckpoint(String),
}

/// Why a [`MetaCommandRecord`] payload could not be decoded. Decode failures
/// fail closed (spec section 4.10).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MetaDecodeError {
    /// The payload is not a well-formed record.
    #[error("meta command decode failed: {0}")]
    Malformed(String),
    /// The record's format version is outside the supported range.
    #[error("unsupported meta command format version {found} (supported {min}..={max})")]
    UnsupportedVersion {
        /// Version found in the payload.
        found: u32,
        /// Oldest version this build accepts.
        min: u32,
        /// Newest version this build accepts.
        max: u32,
    },
}

// ---------------------------------------------------------------------------
// Ownership records (spec section 12.1)
// ---------------------------------------------------------------------------

/// Lifecycle state of a logical database.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatabaseState {
    /// Serving traffic.
    Online,
    /// Being dropped; drained of tablets before removal.
    Dropping,
}

/// One logical database owned by the meta group (spec section 12.1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseDescriptor {
    /// The database's durable identifier.
    pub database_id: DatabaseId,
    /// Unique database name.
    pub name: String,
    /// Creation timestamp stamped by the proposer.
    pub created_at: HlcTimestamp,
    /// Lifecycle state.
    pub state: DatabaseState,
    /// Meta state's [`MetadataVersion`] of the last modification.
    #[serde(default = "zero_metadata_version")]
    pub metadata_version: MetadataVersion,
}

/// Replicated schema of one table: the opaque schema document (JSON) plus
/// its monotonic version. Last-writer-wins by `schema_version`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableSchemaRecord {
    /// The table's durable identifier.
    pub table_id: TableId,
    /// Database owning the table.
    pub database_id: DatabaseId,
    /// Monotonic schema version (never reused, never lowered).
    pub schema_version: SchemaVersion,
    /// The schema document (opaque to the meta group; the engine interprets
    /// it at DDL apply time).
    pub schema: serde_json::Value,
    /// Meta state's [`MetadataVersion`] of the last modification.
    #[serde(default = "zero_metadata_version")]
    pub metadata_version: MetadataVersion,
}

// ---------------------------------------------------------------------------
// Canonical descriptor types (reconciled with crate::tablet / crate::placement)
// ---------------------------------------------------------------------------

/// The canonical locality constraint (`crate::placement`), re-exported.
pub use crate::placement::LocalityConstraint;
/// The canonical placement policy (`crate::placement`), re-exported.
pub use crate::placement::PlacementPolicy;
/// The canonical partition bounds (`crate::tablet`), re-exported.
pub use crate::tablet::PartitionBounds;
/// The canonical replica descriptor (`crate::tablet`), re-exported.
pub use crate::tablet::ReplicaDescriptor;
/// The canonical replica role (`crate::tablet`), re-exported.
pub use crate::tablet::ReplicaRole;
/// The canonical tablet descriptor (`crate::tablet`), re-exported.
pub use crate::tablet::TabletDescriptor;
/// The canonical tablet lifecycle state (`crate::tablet`), re-exported.
pub use crate::tablet::TabletState;

/// Replica set of one raft group (spec section 12.1 "replica placement"): the
/// canonical `crate::tablet` replica descriptors wrapped with the meta state's
/// modification version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaPlacement {
    /// The group whose replicas are placed.
    pub raft_group_id: RaftGroupId,
    /// Placed replicas and their roles.
    pub replicas: Vec<ReplicaDescriptor>,
    /// Meta state's [`MetadataVersion`] of the last modification.
    #[serde(default = "zero_metadata_version")]
    pub metadata_version: MetadataVersion,
}

/// Replicated tablet record: the canonical `crate::tablet` descriptor wrapped
/// with the meta state's modification version (observability and the
/// optimistic-concurrency token of other records; the descriptor's own
/// `generation` remains the last-writer-wins guard).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TabletRecord {
    /// The tablet descriptor.
    pub descriptor: TabletDescriptor,
    /// Meta state's [`MetadataVersion`] of the last modification.
    #[serde(default = "zero_metadata_version")]
    pub metadata_version: MetadataVersion,
}

/// Replicated placement-policy record: the canonical `crate::placement`
/// policy wrapped with the meta state's modification version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementPolicyRecord {
    /// The placement policy.
    pub policy: PlacementPolicy,
    /// Meta state's [`MetadataVersion`] of the last modification.
    #[serde(default = "zero_metadata_version")]
    pub metadata_version: MetadataVersion,
}

/// Schema/index job kinds owned by the meta group. Minimal mirror of the
/// core registry's DDL-relevant `JobKind`s (see module reconciliation notes;
/// spec section 12.11 wires these to distributed DDL).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaJobKind {
    /// Online secondary-index build.
    IndexBuild,
    /// Backfill of a newly added or altered column.
    ColumnBackfill,
    /// Validation of existing rows against a new schema constraint.
    SchemaValidation,
}

/// Schema job lifecycle. Minimal mirror of the core registry's `JobState`
/// (see module reconciliation notes); the transition graph is identical.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaJobState {
    /// Submitted, waiting for admission.
    Pending,
    /// Admitted; a worker is actively driving it.
    Running,
    /// Parked; resumes from the last durable checkpoint.
    Paused,
    /// Cancellation requested; rollback has not finished.
    Cancelling,
    /// Terminal: every phase completed and published.
    Succeeded,
    /// Terminal: failed or cancelled.
    Failed,
    /// A phase failed or a cancel was observed; rollback is in progress.
    RollingBack,
}

impl SchemaJobState {
    /// Terminal states have no outgoing edges.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }

    /// Whether the `self -> next` edge exists in the job graph (mirrors the
    /// core registry's `JobState::can_transition`).
    pub fn can_transition(self, next: Self) -> bool {
        use SchemaJobState::{
            Cancelling, Failed, Paused, Pending, RollingBack, Running, Succeeded,
        };
        matches!(
            (self, next),
            (Pending, Running)
                | (Pending, Cancelling)
                | (Running, Paused)
                | (Running, Cancelling)
                | (Running, RollingBack)
                | (Running, Succeeded)
                | (Paused, Pending)
                | (Paused, Cancelling)
                | (Cancelling, RollingBack)
                | (Cancelling, Failed)
                | (RollingBack, Failed)
        )
    }
}

/// One replicated schema/index job record (spec section 12.1 "schema/index
/// jobs").
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaJobRecord {
    /// Job identifier (allocated by the proposer; never reused).
    pub job_id: u64,
    /// Database owning the job's target.
    pub database_id: DatabaseId,
    /// Table the job operates on.
    pub table_id: TableId,
    /// Job kind.
    pub kind: SchemaJobKind,
    /// Lifecycle state; submissions start [`SchemaJobState::Pending`].
    pub state: SchemaJobState,
    /// Submission timestamp stamped by the proposer.
    pub submitted_at: HlcTimestamp,
    /// Timestamp of the last state update.
    pub updated_at: HlcTimestamp,
    /// Failure detail for terminal/rolling-back states.
    pub error: Option<String>,
    /// Meta state's [`MetadataVersion`] of the last modification.
    #[serde(default = "zero_metadata_version")]
    pub metadata_version: MetadataVersion,
}

/// One transaction status partition and its home group (spec sections 12.1,
/// 12.8): the coordinator record of a distributed transaction lives on the
/// partition's home raft group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxnStatusPartition {
    /// Partition identifier (derived from the transaction id, spec section
    /// 12.8).
    pub partition_id: u32,
    /// Raft group owning the partition's transaction status records.
    pub home_raft_group: RaftGroupId,
}

// ---------------------------------------------------------------------------
// Dynamic cluster settings (spec section 16.2)
// ---------------------------------------------------------------------------

/// Per-resource-group limits stored as cluster settings. A zero field means
/// "no group-specific cap" (the node's static limits still apply).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ResourceGroupSetting {
    /// Memory cap in bytes.
    pub max_memory_bytes: u64,
    /// Concurrent query cap.
    pub max_concurrent_queries: u32,
    /// Temporary-disk (spill) budget in bytes.
    pub temp_disk_budget_bytes: u64,
}

/// Cluster-wide backup policy setting (spec section 16.2). Backup
/// destinations and their credentials are static node configuration — never
/// cluster settings (see [`SECRET_SETTING_KEY_DENYLIST`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct BackupPolicy {
    /// Whether scheduled backups run.
    pub enabled: bool,
    /// Interval between scheduled backups.
    pub interval_seconds: u64,
    /// How many completed backups are retained.
    pub retention_count: u32,
}

impl Default for BackupPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_seconds: DEFAULT_BACKUP_INTERVAL_SECONDS,
            retention_count: DEFAULT_BACKUP_RETENTION_COUNT,
        }
    }
}

/// Cluster-wide AI limits (spec section 16.2; AI limits are a security
/// boundary — the engine fails closed at or below these).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AiLimits {
    /// Concurrent AI request cap.
    pub max_concurrent_requests: u32,
    /// Total AI memory budget in bytes.
    pub max_memory_bytes: u64,
}

impl Default for AiLimits {
    fn default() -> Self {
        Self {
            max_concurrent_requests: DEFAULT_AI_MAX_CONCURRENT_REQUESTS,
            max_memory_bytes: DEFAULT_AI_MAX_MEMORY_BYTES,
        }
    }
}

/// Cluster-wide default read consistency (spec sections 11.4, 16.2).
/// Payload-free mirror of `mongreldb_consensus::read::ReadConsistency` (the
/// request-scoped token/timestamp variants carry no payload here).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DefaultConsistency {
    /// Leader read-index + wait applied (the default; strongest).
    #[default]
    Linearizable,
    /// Wait until the replica applied the session's last write.
    ReadYourWrites,
    /// Serve at a requested snapshot timestamp.
    Snapshot,
    /// Serve if the applied watermark lags by at most `max_lag_ms`.
    BoundedStaleness {
        /// Maximum tolerated lag in milliseconds.
        max_lag_ms: u64,
    },
    /// Serve the local applied watermark immediately.
    Eventual,
}

/// The replicated dynamic cluster settings (spec section 16.2). Placement
/// policies are first-class records ([`MetaState::placement_policies`], set
/// by [`MetaCommand::SetPlacementPolicy`]) rather than scalar settings, and
/// feature activation rides [`MetaCommand::ActivateFeature`]; the remaining
/// section 16.2 categories live here. Secrets are never stored as plaintext
/// settings (enforced by [`SECRET_SETTING_KEY_DENYLIST`]).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterSettings {
    /// Resource-group limits by group name.
    pub resource_groups: BTreeMap<String, ResourceGroupSetting>,
    /// MVCC history retention in epochs (0 = disabled; mirrors the core's
    /// `history_retention_epochs`).
    pub history_retention_epochs: u64,
    /// Cluster-wide backup policy.
    pub backup: BackupPolicy,
    /// Default read consistency for sessions that do not request one.
    pub default_consistency: DefaultConsistency,
    /// Cluster-wide AI limits.
    pub ai: AiLimits,
    /// Bound on concurrently active jobs.
    pub max_concurrent_jobs: u32,
}

impl ClusterSettings {
    /// Applies one key/value setting; see [`KNOWN_SETTING_KEYS`] and
    /// `resource_groups.<name>` (a `null` value removes the group). The
    /// denylist runs first, so a denied key is refused even when unknown.
    fn apply(&mut self, key: &str, value: &serde_json::Value) -> Result<(), MetaRejectionReason> {
        let lowered = key.to_ascii_lowercase();
        if SECRET_SETTING_KEY_DENYLIST
            .iter()
            .any(|needle| lowered.contains(needle))
        {
            return Err(MetaRejectionReason::SecretSettingKey {
                key: key.to_owned(),
            });
        }
        fn invalid(key: &str, reason: impl Into<String>) -> MetaRejectionReason {
            MetaRejectionReason::InvalidSettingValue {
                key: key.to_owned(),
                reason: reason.into(),
            }
        }
        match key {
            "history_retention_epochs" => {
                self.history_retention_epochs = value
                    .as_u64()
                    .ok_or_else(|| invalid(key, "expected an unsigned integer"))?;
            }
            "backup.enabled" => {
                self.backup.enabled = value
                    .as_bool()
                    .ok_or_else(|| invalid(key, "expected a boolean"))?;
            }
            "backup.interval_seconds" => {
                let interval = value
                    .as_u64()
                    .ok_or_else(|| invalid(key, "expected an unsigned integer"))?;
                if interval == 0 {
                    return Err(invalid(key, "interval must be positive"));
                }
                self.backup.interval_seconds = interval;
            }
            "backup.retention_count" => {
                let retention = value
                    .as_u64()
                    .ok_or_else(|| invalid(key, "expected an unsigned integer"))?;
                self.backup.retention_count = u32::try_from(retention)
                    .map_err(|_| invalid(key, "retention count exceeds u32"))?;
            }
            "default_consistency" => {
                self.default_consistency =
                    serde_json::from_value(value.clone()).map_err(|error| {
                        invalid(
                            key,
                            format!(
                                "expected one of \"Linearizable\", \"ReadYourWrites\", \
                                 \"Snapshot\", \"Eventual\", or \
                                 {{\"BoundedStaleness\": {{\"max_lag_ms\": ..}}}}: {error}"
                            ),
                        )
                    })?;
            }
            "ai.max_concurrent_requests" => {
                let cap = value
                    .as_u64()
                    .ok_or_else(|| invalid(key, "expected an unsigned integer"))?;
                self.ai.max_concurrent_requests =
                    u32::try_from(cap).map_err(|_| invalid(key, "cap exceeds u32"))?;
            }
            "ai.max_memory_bytes" => {
                self.ai.max_memory_bytes = value
                    .as_u64()
                    .ok_or_else(|| invalid(key, "expected an unsigned integer"))?;
            }
            "jobs.max_concurrent" => {
                let cap = value
                    .as_u64()
                    .ok_or_else(|| invalid(key, "expected an unsigned integer"))?;
                if cap == 0 {
                    return Err(invalid(key, "job concurrency must be positive"));
                }
                self.max_concurrent_jobs =
                    u32::try_from(cap).map_err(|_| invalid(key, "cap exceeds u32"))?;
            }
            _ => {
                let Some(name) = key.strip_prefix("resource_groups.") else {
                    return Err(MetaRejectionReason::UnknownSettingKey {
                        key: key.to_owned(),
                    });
                };
                if name.is_empty() {
                    return Err(MetaRejectionReason::UnknownSettingKey {
                        key: key.to_owned(),
                    });
                }
                if value.is_null() {
                    self.resource_groups.remove(name);
                } else {
                    let group: ResourceGroupSetting = serde_json::from_value(value.clone())
                        .map_err(|error| {
                            invalid(key, format!("expected a resource-group object: {error}"))
                        })?;
                    self.resource_groups.insert(name.to_owned(), group);
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Apply outcomes
// ---------------------------------------------------------------------------

/// Why a [`MetaCommand`] was refused at apply. Refusals are deterministic
/// (every replica reaches the same conclusion from the same state) and never
/// fault the raft state machine: they are journaled in
/// [`MetaState::rejections`] and reported to the proposer by
/// [`MetaGroup::propose`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum MetaRejectionReason {
    /// Feature activation failed validation (spec section 11.8 step 5;
    /// re-validated at apply, ADR-0010 decision 4).
    #[error(transparent)]
    FeatureActivation(#[from] FeatureActivationError),
    /// The setting key is on the secrets denylist (spec section 16.2).
    #[error(
        "cluster setting key `{key}` is denied: secrets are never stored as \
         plaintext cluster settings"
    )]
    SecretSettingKey {
        /// The denied key.
        key: String,
    },
    /// The setting key maps to no typed setting.
    #[error("unknown cluster setting key `{key}`")]
    UnknownSettingKey {
        /// The unknown key.
        key: String,
    },
    /// The setting value failed typed parsing or validation.
    #[error("invalid value for cluster setting `{key}`: {reason}")]
    InvalidSettingValue {
        /// The setting key.
        key: String,
        /// Why the value failed.
        reason: String,
    },
    /// A last-writer-wins guard rejected an out-of-date write.
    #[error("stale write to {resource}: current version {current}, attempted {attempted}")]
    StaleWrite {
        /// What was being written.
        resource: String,
        /// Version currently stored.
        current: MetadataVersion,
        /// Version the command carried.
        attempted: MetadataVersion,
    },
    /// The command conflicts with existing state.
    #[error("conflicting {resource}: {reason}")]
    Conflict {
        /// What conflicted.
        resource: String,
        /// Why it conflicted.
        reason: String,
    },
    /// A referenced record does not exist.
    #[error("{resource} not found")]
    NotFound {
        /// What was looked up.
        resource: String,
    },
    /// The command itself is malformed.
    #[error("invalid meta command: {reason}")]
    Invalid {
        /// Why the command is invalid.
        reason: String,
    },
    /// A meta-plane binding's proposal transport failed (leader loss,
    /// timeout, shutdown) before the command's outcome was known. Never
    /// journaled from apply — apply-time refusals are deterministic; this
    /// surfaces only through the
    /// [`crate::split::TabletMetaPlane`]/[`crate::merge::MergeMetaPlane`]
    /// seams the node runtime drives. Every split/merge descriptor write is
    /// idempotent, so retrying the failed step is safe.
    #[error("meta proposal failed: {reason}")]
    ProposalFailed {
        /// What failed.
        reason: String,
    },
    /// The node receiving the proposal is not the meta leader (spec section
    /// 11.7): the caller re-resolves the leader and retries there. Carried
    /// structured (never stringified) so gateways and split/merge drivers can
    /// pattern-match it; split/merge descriptor writes are idempotent, so the
    /// retry is safe.
    #[error("not the meta leader (current leader: {leader:?})")]
    NotLeader {
        /// The node's current belief about the meta leader's raft id, if any.
        leader: Option<u64>,
    },
}

/// One journaled refusal: the refused command's id and the typed reason.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaRejection {
    /// Leader-assigned id of the refused command (`None` when the command
    /// carried no id; always `Some` through [`MetaGroup::propose`]).
    pub command_id: Option<[u8; 16]>,
    /// Why the command was refused.
    pub reason: MetaRejectionReason,
}

// ---------------------------------------------------------------------------
// MetaCommand
// ---------------------------------------------------------------------------

/// One replicated meta control-plane command (spec section 12.1).
///
/// Every variant is deterministic and idempotent at apply: records are
/// versioned ([`MetadataVersion`], `schema_version`, or `generation` per
/// record) and last-writer-wins guards reject stale or conflicting writes
/// with a typed [`MetaRejectionReason`] instead of faulting the state
/// machine.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MetaCommand {
    /// Registers (or re-registers) a cluster node's descriptor. Identical
    /// re-registration is a no-op.
    RegisterNode {
        /// The node's advertised descriptor.
        descriptor: NodeDescriptor,
    },
    /// Updates a node's lifecycle state; `Decommissioned` is terminal.
    UpdateNodeState {
        /// The node to update.
        node_id: NodeId,
        /// Requested lifecycle state.
        state: NodeState,
        /// Optimistic-concurrency guard: when `Some`, the write applies only
        /// if the record's current [`MetadataVersion`] equals it.
        expected_version: Option<MetadataVersion>,
    },
    /// Removes a node from membership. Refused while any tablet or placement
    /// still references the node; absent nodes are a no-op.
    RemoveNode {
        /// The node to remove.
        node_id: NodeId,
    },
    /// Creates a logical database. Name and id must both be free.
    CreateDatabase {
        /// The new database's descriptor.
        descriptor: DatabaseDescriptor,
    },
    /// Drops a database. Refused while tables reference it; absent databases
    /// are a no-op.
    DropDatabase {
        /// The database to drop.
        database_id: DatabaseId,
    },
    /// Publishes a table schema (last-writer-wins by `schema_version`).
    SetTableSchema {
        /// The schema record to publish.
        record: TableSchemaRecord,
    },
    /// Publishes a tablet descriptor (last-writer-wins by `generation`).
    SetTabletDescriptor {
        /// The tablet descriptor to publish.
        descriptor: TabletDescriptor,
    },
    /// Removes a tablet descriptor at or above its stored `generation`.
    RemoveTabletDescriptor {
        /// The tablet to remove.
        tablet_id: TabletId,
        /// Removal generation; below the stored generation the command is
        /// stale.
        generation: u64,
    },
    /// Publishes the replica placement of one raft group.
    SetReplicaPlacement {
        /// The placement to publish.
        placement: ReplicaPlacement,
    },
    /// Publishes (or replaces) a named placement policy (spec section 12.7).
    SetPlacementPolicy {
        /// Policy name.
        name: String,
        /// The policy.
        policy: PlacementPolicy,
    },
    /// Submits a schema/index job (starts [`SchemaJobState::Pending`]).
    SubmitSchemaJob {
        /// The job record.
        job: SchemaJobRecord,
    },
    /// Transitions a schema job along the legal state graph.
    UpdateSchemaJob {
        /// The job to transition.
        job_id: u64,
        /// Requested state.
        state: SchemaJobState,
        /// Timestamp of the update.
        updated_at: HlcTimestamp,
        /// Failure detail to record (cleared with `None`).
        error: Option<String>,
        /// Optimistic-concurrency guard; see
        /// [`MetaCommand::UpdateNodeState::expected_version`].
        expected_version: Option<MetadataVersion>,
    },
    /// Sets one dynamic cluster setting (spec section 16.2; see
    /// [`KNOWN_SETTING_KEYS`] and [`SECRET_SETTING_KEY_DENYLIST`]).
    SetClusterSetting {
        /// Setting key.
        key: String,
        /// Setting value (typed per key).
        value: serde_json::Value,
    },
    /// Activates a cluster feature (spec section 11.8 step 5): refused at
    /// apply unless every non-decommissioned registered node supports the
    /// feature and the level satisfies the registry.
    ActivateFeature {
        /// The activation record.
        activation: FeatureActivation,
    },
    /// Publishes one transaction status partition's home group.
    SetTxnStatusPartition {
        /// The partition record.
        partition: TxnStatusPartition,
    },
    /// Publishes the atomic routing change of one tablet split (spec section
    /// 12.5 step 8): the children become `Active` and the source `Retiring`
    /// in ONE command. Refused unless the stored source is the command's
    /// `Splitting` precursor and the stored children its `Creating`
    /// precursors at exactly one generation below the publication
    /// generation, the child bounds partition the source at the split key,
    /// and no other routable tablet of the table overlaps a child. An exact
    /// re-application (the stored descriptors already carry the command's
    /// content) is a no-op, so a split resumed after a crash in the
    /// publication barrier may re-publish.
    PublishSplit {
        /// The publication (see [`crate::split::SplitPublishCommand`]).
        command: SplitPublishCommand,
    },
    /// Publishes the atomic routing change of one tablet merge (spec section
    /// 12.6): the hidden replacement becomes `Active` and both sources
    /// `Retiring` in ONE command. Refused unless the stored sources are the
    /// command's `Merging` precursors and the stored replacement its
    /// `Creating` precursor, with the command-wide generation one above the
    /// highest stored generation (a lagging source jumps to it), the
    /// replacement bounds covering exactly the source union, and no other
    /// routable tablet of the table overlapping the replacement. Exact
    /// re-application is a no-op (see [`Self::PublishSplit`]).
    PublishMerge {
        /// The publication (see [`crate::merge::MergePublishCommand`]).
        command: MergePublishCommand,
    },
    /// Allocates `count` fresh per-group raft node ids from the meta-owned
    /// allocator (spec section 12.1: the meta control plane owns the
    /// node-id ↔ raft-id mapping; tablet replica raft ids come from this
    /// allocator, never the ad-hoc node-id projection the meta group itself
    /// uses). Ids are drawn from a monotonic counter that skips ids already
    /// in use (registered-node projections, tablet and placement replicas).
    /// The allocation is recorded under the command id, so a replayed
    /// command never double-allocates; the proposer reads the base back
    /// through [`MetaGroup::allocate_raft_node_ids`].
    AllocateRaftNodeIds {
        /// Number of ids to allocate
        /// (`1..=MAX_RAFT_NODE_ID_ALLOCATION`).
        count: u32,
    },
}

/// The versioned envelope payload carrying one [`MetaCommand`] (spec section
/// 4.10). Serialized as JSON into a [`CommandEnvelope`] stamped with
/// [`COMMAND_TYPE_META_COMMAND`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetaCommandRecord {
    /// Format version; see [`META_COMMAND_FORMAT_VERSION`].
    pub format_version: u32,
    /// The command.
    pub command: MetaCommand,
}

impl MetaCommandRecord {
    /// Wraps `command` at the current format version.
    pub fn new(command: MetaCommand) -> Self {
        Self {
            format_version: META_COMMAND_FORMAT_VERSION,
            command,
        }
    }

    /// Serializes the record (JSON; human-readable so 128-bit ids take their
    /// canonical hex form).
    pub fn encode(&self) -> Result<Vec<u8>, MetaError> {
        serde_json::to_vec(self).map_err(|error| MetaError::Encode(error.to_string()))
    }

    /// Parses and verifies a record produced by [`MetaCommandRecord::encode`];
    /// v1 records migrate to the canonical shapes (see the module docs).
    /// Unknown versions and malformed payloads fail closed.
    pub fn decode(payload: &[u8]) -> Result<Self, MetaDecodeError> {
        let probe: FormatVersionProbe = serde_json::from_slice(payload)
            .map_err(|error| MetaDecodeError::Malformed(error.to_string()))?;
        if probe.format_version < MIN_SUPPORTED_META_COMMAND_FORMAT_VERSION
            || probe.format_version > META_COMMAND_FORMAT_VERSION
        {
            return Err(MetaDecodeError::UnsupportedVersion {
                found: probe.format_version,
                min: MIN_SUPPORTED_META_COMMAND_FORMAT_VERSION,
                max: META_COMMAND_FORMAT_VERSION,
            });
        }
        if probe.format_version == 1 {
            let record: v1::MetaCommandRecord = serde_json::from_slice(payload)
                .map_err(|error| MetaDecodeError::Malformed(error.to_string()))?;
            if record.format_version != probe.format_version {
                return Err(MetaDecodeError::Malformed(
                    "inconsistent format version".to_owned(),
                ));
            }
            return Ok(Self {
                format_version: META_COMMAND_FORMAT_VERSION,
                command: migrate_command(record.command),
            });
        }
        serde_json::from_slice(payload)
            .map_err(|error| MetaDecodeError::Malformed(error.to_string()))
    }
}

// ---------------------------------------------------------------------------
// MetaState
// ---------------------------------------------------------------------------

/// A registered node: the advertised descriptor plus the meta state's
/// modification version (the optimistic-concurrency token for
/// [`MetaCommand::UpdateNodeState`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    /// The node's advertised descriptor.
    pub descriptor: NodeDescriptor,
    /// Meta state's [`MetadataVersion`] of the last modification.
    #[serde(default = "zero_metadata_version")]
    pub metadata_version: MetadataVersion,
}

/// The replicated control-plane state of the meta group (spec section 12.1).
///
/// Deterministic: every replica applies the same commands in the same order
/// and reaches byte-identical state. Versioned: `format_version` gates the
/// snapshot format and `metadata_version` ticks once per applied command
/// (accepted or refused — a refusal appends to the rejection journal, which
/// is itself state), giving readers a monotonic watermark.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MetaState {
    /// Snapshot format version; see [`META_STATE_FORMAT_VERSION`].
    pub format_version: u32,
    /// Monotonic per-applied-command version.
    pub metadata_version: MetadataVersion,
    /// Cluster membership: registered node descriptors and locality.
    pub nodes: BTreeMap<NodeId, NodeRecord>,
    /// Logical databases.
    pub databases: BTreeMap<DatabaseId, DatabaseDescriptor>,
    /// Replicated table schemas.
    pub tables: BTreeMap<TableId, TableSchemaRecord>,
    /// Tablet records (canonical descriptor + modification version).
    pub tablets: BTreeMap<TabletId, TabletRecord>,
    /// Replica placements by raft group.
    pub placements: BTreeMap<RaftGroupId, ReplicaPlacement>,
    /// Named placement policy records (spec section 12.7).
    pub placement_policies: BTreeMap<String, PlacementPolicyRecord>,
    /// Schema/index jobs.
    pub schema_jobs: BTreeMap<u64, SchemaJobRecord>,
    /// Transaction status partitions (spec section 12.8).
    pub txn_status_partitions: BTreeMap<u32, TxnStatusPartition>,
    /// Dynamic cluster settings (spec section 16.2).
    pub settings: ClusterSettings,
    /// Cluster feature level (never lowers; ADR-0010).
    pub feature_level: ClusterFeatureLevel,
    /// Applied feature activations, oldest first.
    pub feature_activations: Vec<FeatureActivation>,
    /// Bounded journal of refused commands, oldest first
    /// ([`META_REJECTION_LIMIT`]).
    pub rejections: VecDeque<MetaRejection>,
    /// Next per-group raft node id the meta-owned allocator hands out
    /// ([`MetaCommand::AllocateRaftNodeIds`]); monotonic and never reused.
    /// Starts at [`FIRST_RAFT_NODE_ID`].
    pub next_raft_node_id: u64,
    /// Recent raft-node-id allocations by command id (hex-encoded, base of
    /// each allocated range), the idempotent-replay record of
    /// [`MetaCommand::AllocateRaftNodeIds`]; bounded by
    /// [`RAFT_ID_ALLOCATION_RECORD_LIMIT`]. (Hex keys: JSON map keys must be
    /// strings.)
    pub raft_id_allocations: BTreeMap<String, u64>,
    /// Eviction order of [`Self::raft_id_allocations`], oldest first.
    pub raft_id_allocation_order: VecDeque<String>,
}

impl Default for MetaState {
    fn default() -> Self {
        Self {
            format_version: META_STATE_FORMAT_VERSION,
            metadata_version: MetadataVersion::ZERO,
            nodes: BTreeMap::new(),
            databases: BTreeMap::new(),
            tables: BTreeMap::new(),
            tablets: BTreeMap::new(),
            placements: BTreeMap::new(),
            placement_policies: BTreeMap::new(),
            schema_jobs: BTreeMap::new(),
            txn_status_partitions: BTreeMap::new(),
            settings: ClusterSettings::default(),
            feature_level: ClusterFeatureLevel::ZERO,
            feature_activations: Vec::new(),
            rejections: VecDeque::new(),
            next_raft_node_id: FIRST_RAFT_NODE_ID,
            raft_id_allocations: BTreeMap::new(),
            raft_id_allocation_order: VecDeque::new(),
        }
    }
}

impl MetaState {
    /// Applies one committed command. `metadata_version` ticks first (every
    /// applied command moves the watermark); a refusal journals the reason
    /// and leaves the records untouched.
    ///
    /// `commit_ts` is the leader-assigned commit timestamp of the command's
    /// log entry: an [`FeatureActivation::activated_at`] left as
    /// [`HlcTimestamp::ZERO`] by the proposer is stamped with it here, so
    /// every replica records the identical timestamp.
    pub fn apply(
        &mut self,
        command: &MetaCommand,
        command_id: Option<[u8; 16]>,
        commit_ts: HlcTimestamp,
        registry: &FeatureRegistry,
    ) -> Result<(), MetaRejectionReason> {
        self.metadata_version = MetadataVersion(self.metadata_version.get() + 1);
        let version = self.metadata_version;
        let result = self.dispatch(command, command_id, commit_ts, registry, version);
        if let Err(reason) = &result {
            self.rejections.push_back(MetaRejection {
                command_id,
                reason: reason.clone(),
            });
            while self.rejections.len() > META_REJECTION_LIMIT {
                self.rejections.pop_front();
            }
        }
        result
    }

    fn dispatch(
        &mut self,
        command: &MetaCommand,
        command_id: Option<[u8; 16]>,
        commit_ts: HlcTimestamp,
        registry: &FeatureRegistry,
        version: MetadataVersion,
    ) -> Result<(), MetaRejectionReason> {
        match command {
            MetaCommand::RegisterNode { descriptor } => {
                if descriptor.node_id == NodeId::ZERO {
                    return Err(MetaRejectionReason::Invalid {
                        reason: "reserved all-zero node id".to_owned(),
                    });
                }
                match self.nodes.get(&descriptor.node_id) {
                    Some(existing) if existing.descriptor == *descriptor => Ok(()),
                    _ => {
                        // The meta group addresses its members by the
                        // `raft_node_id` projection of their node ids; tablet
                        // groups must never reuse a projection (the runtime
                        // attaches one raft node per id to its transport
                        // registry). Fail closed when the projection is
                        // already assigned to a tablet/placement replica.
                        let projected = raft_node_id(&descriptor.node_id);
                        let assigned_to_replica = self.tablets.values().any(|record| {
                            record
                                .descriptor
                                .replicas
                                .iter()
                                .any(|replica| replica.raft_node_id == projected)
                        }) || self.placements.values().any(|placement| {
                            placement
                                .replicas
                                .iter()
                                .any(|replica| replica.raft_node_id == projected)
                        });
                        if assigned_to_replica {
                            return Err(MetaRejectionReason::Conflict {
                                resource: format!("node {}", descriptor.node_id),
                                reason: format!(
                                    "raft id projection {projected} is already assigned to a \
                                     tablet or placement replica; re-mint the node id"
                                ),
                            });
                        }
                        self.nodes.insert(
                            descriptor.node_id,
                            NodeRecord {
                                descriptor: descriptor.clone(),
                                metadata_version: version,
                            },
                        );
                        Ok(())
                    }
                }
            }
            MetaCommand::UpdateNodeState {
                node_id,
                state,
                expected_version,
            } => {
                let Some(record) = self.nodes.get_mut(node_id) else {
                    return Err(MetaRejectionReason::NotFound {
                        resource: format!("node {node_id}"),
                    });
                };
                if let Some(expected) = expected_version {
                    if *expected != record.metadata_version {
                        return Err(MetaRejectionReason::StaleWrite {
                            resource: format!("node {node_id}"),
                            current: record.metadata_version,
                            attempted: *expected,
                        });
                    }
                }
                if record.descriptor.state == NodeState::Decommissioned {
                    return Err(MetaRejectionReason::Conflict {
                        resource: format!("node {node_id}"),
                        reason: "decommissioned is terminal".to_owned(),
                    });
                }
                if record.descriptor.state == *state {
                    return Ok(());
                }
                record.descriptor.state = *state;
                record.metadata_version = version;
                Ok(())
            }
            MetaCommand::RemoveNode { node_id } => {
                if !self.nodes.contains_key(node_id) {
                    return Ok(());
                }
                let referenced_by_placement = self
                    .placements
                    .values()
                    .any(|placement| placement.replicas.iter().any(|r| r.node_id == *node_id));
                let referenced_by_tablet = self.tablets.values().any(|record| {
                    record
                        .descriptor
                        .replicas
                        .iter()
                        .any(|r| r.node_id == *node_id)
                        || record.descriptor.leader_hint == Some(*node_id)
                });
                if referenced_by_placement || referenced_by_tablet {
                    return Err(MetaRejectionReason::Conflict {
                        resource: format!("node {node_id}"),
                        reason: "node still hosts a tablet or placement replica".to_owned(),
                    });
                }
                self.nodes.remove(node_id);
                Ok(())
            }
            MetaCommand::CreateDatabase { descriptor } => {
                if descriptor.database_id == DatabaseId::ZERO {
                    return Err(MetaRejectionReason::Invalid {
                        reason: "reserved all-zero database id".to_owned(),
                    });
                }
                if descriptor.name.trim().is_empty() {
                    return Err(MetaRejectionReason::Invalid {
                        reason: "database name is empty".to_owned(),
                    });
                }
                if let Some(existing) = self.databases.get(&descriptor.database_id) {
                    return if existing.name == descriptor.name
                        && existing.state == descriptor.state
                        && existing.created_at == descriptor.created_at
                    {
                        Ok(())
                    } else {
                        Err(MetaRejectionReason::Conflict {
                            resource: format!("database {}", descriptor.database_id),
                            reason: "database id already exists with different content".to_owned(),
                        })
                    };
                }
                if self
                    .databases
                    .values()
                    .any(|database| database.name == descriptor.name)
                {
                    return Err(MetaRejectionReason::Conflict {
                        resource: format!("database `{}`", descriptor.name),
                        reason: "database name already taken".to_owned(),
                    });
                }
                let mut descriptor = descriptor.clone();
                descriptor.metadata_version = version;
                self.databases.insert(descriptor.database_id, descriptor);
                Ok(())
            }
            MetaCommand::DropDatabase { database_id } => {
                if !self.databases.contains_key(database_id) {
                    return Ok(());
                }
                if self
                    .tables
                    .values()
                    .any(|table| table.database_id == *database_id)
                {
                    return Err(MetaRejectionReason::Conflict {
                        resource: format!("database {database_id}"),
                        reason: "database still has tables".to_owned(),
                    });
                }
                self.databases.remove(database_id);
                Ok(())
            }
            MetaCommand::SetTableSchema { record } => {
                if !self.databases.contains_key(&record.database_id) {
                    return Err(MetaRejectionReason::NotFound {
                        resource: format!("database {}", record.database_id),
                    });
                }
                if record.schema_version == SchemaVersion::ZERO {
                    return Err(MetaRejectionReason::Invalid {
                        reason: "reserved zero schema version".to_owned(),
                    });
                }
                match self.tables.get(&record.table_id) {
                    Some(existing) => {
                        if record.schema_version > existing.schema_version {
                            let mut record = record.clone();
                            record.metadata_version = version;
                            self.tables.insert(record.table_id, record);
                            Ok(())
                        } else if record.schema_version == existing.schema_version {
                            if existing.database_id == record.database_id
                                && existing.schema == record.schema
                            {
                                Ok(())
                            } else {
                                Err(MetaRejectionReason::Conflict {
                                    resource: format!("table {}", record.table_id),
                                    reason: "schema version already used for different content"
                                        .to_owned(),
                                })
                            }
                        } else {
                            Err(MetaRejectionReason::StaleWrite {
                                resource: format!("table {}", record.table_id),
                                current: MetadataVersion(existing.schema_version.get()),
                                attempted: MetadataVersion(record.schema_version.get()),
                            })
                        }
                    }
                    None => {
                        let mut record = record.clone();
                        record.metadata_version = version;
                        self.tables.insert(record.table_id, record);
                        Ok(())
                    }
                }
            }
            MetaCommand::SetTabletDescriptor { descriptor } => {
                if let Err(error) = descriptor.validate() {
                    return Err(MetaRejectionReason::Invalid {
                        reason: error.to_string(),
                    });
                }
                if !self.tables.contains_key(&descriptor.table_id) {
                    return Err(MetaRejectionReason::NotFound {
                        resource: format!("table {}", descriptor.table_id),
                    });
                }
                match self.tablets.get(&descriptor.tablet_id) {
                    Some(existing) => {
                        if descriptor.generation > existing.descriptor.generation {
                            self.tablets.insert(
                                descriptor.tablet_id,
                                TabletRecord {
                                    descriptor: descriptor.clone(),
                                    metadata_version: version,
                                },
                            );
                            Ok(())
                        } else if descriptor.generation == existing.descriptor.generation {
                            if existing.descriptor == *descriptor {
                                Ok(())
                            } else {
                                Err(MetaRejectionReason::Conflict {
                                    resource: format!("tablet {}", descriptor.tablet_id),
                                    reason: "generation already used for different content"
                                        .to_owned(),
                                })
                            }
                        } else {
                            Err(MetaRejectionReason::StaleWrite {
                                resource: format!("tablet {}", descriptor.tablet_id),
                                current: MetadataVersion(existing.descriptor.generation),
                                attempted: MetadataVersion(descriptor.generation),
                            })
                        }
                    }
                    None => {
                        self.tablets.insert(
                            descriptor.tablet_id,
                            TabletRecord {
                                descriptor: descriptor.clone(),
                                metadata_version: version,
                            },
                        );
                        Ok(())
                    }
                }
            }
            MetaCommand::RemoveTabletDescriptor {
                tablet_id,
                generation,
            } => match self.tablets.get(tablet_id) {
                None => Ok(()),
                Some(existing) => {
                    if *generation >= existing.descriptor.generation {
                        self.tablets.remove(tablet_id);
                        Ok(())
                    } else {
                        Err(MetaRejectionReason::StaleWrite {
                            resource: format!("tablet {tablet_id}"),
                            current: MetadataVersion(existing.descriptor.generation),
                            attempted: MetadataVersion(*generation),
                        })
                    }
                }
            },
            MetaCommand::SetReplicaPlacement { placement } => {
                if placement.replicas.is_empty() {
                    return Err(MetaRejectionReason::Invalid {
                        reason: "placement has no replicas".to_owned(),
                    });
                }
                for (index, replica) in placement.replicas.iter().enumerate() {
                    if !self.nodes.contains_key(&replica.node_id) {
                        return Err(MetaRejectionReason::NotFound {
                            resource: format!("node {}", replica.node_id),
                        });
                    }
                    if placement.replicas[..index]
                        .iter()
                        .any(|prior| prior.node_id == replica.node_id)
                    {
                        return Err(MetaRejectionReason::Conflict {
                            resource: format!("raft group {}", placement.raft_group_id),
                            reason: format!("node {} appears twice", replica.node_id),
                        });
                    }
                }
                match self.placements.get(&placement.raft_group_id) {
                    Some(existing) if existing.replicas == placement.replicas => Ok(()),
                    _ => {
                        let mut placement = placement.clone();
                        placement.metadata_version = version;
                        self.placements.insert(placement.raft_group_id, placement);
                        Ok(())
                    }
                }
            }
            MetaCommand::SetPlacementPolicy { name, policy } => {
                if name.trim().is_empty() {
                    return Err(MetaRejectionReason::Invalid {
                        reason: "placement policy name is empty".to_owned(),
                    });
                }
                if policy.replicas == 0 {
                    return Err(MetaRejectionReason::Invalid {
                        reason: format!("placement policy `{name}` requests zero replicas"),
                    });
                }
                match self.placement_policies.get(name) {
                    Some(existing) if existing.policy == *policy => Ok(()),
                    _ => {
                        self.placement_policies.insert(
                            name.clone(),
                            PlacementPolicyRecord {
                                policy: policy.clone(),
                                metadata_version: version,
                            },
                        );
                        Ok(())
                    }
                }
            }
            MetaCommand::SubmitSchemaJob { job } => {
                if job.job_id == 0 {
                    return Err(MetaRejectionReason::Invalid {
                        reason: "reserved zero job id".to_owned(),
                    });
                }
                if !self.databases.contains_key(&job.database_id) {
                    return Err(MetaRejectionReason::NotFound {
                        resource: format!("database {}", job.database_id),
                    });
                }
                if !self.tables.contains_key(&job.table_id) {
                    return Err(MetaRejectionReason::NotFound {
                        resource: format!("table {}", job.table_id),
                    });
                }
                if job.state != SchemaJobState::Pending {
                    return Err(MetaRejectionReason::Invalid {
                        reason: "submitted jobs start Pending".to_owned(),
                    });
                }
                match self.schema_jobs.get(&job.job_id) {
                    Some(existing) => {
                        let mut comparable = existing.clone();
                        comparable.metadata_version = job.metadata_version;
                        if comparable == *job {
                            Ok(())
                        } else {
                            Err(MetaRejectionReason::Conflict {
                                resource: format!("schema job {}", job.job_id),
                                reason: "job id already exists with different content".to_owned(),
                            })
                        }
                    }
                    None => {
                        let mut job = job.clone();
                        job.metadata_version = version;
                        self.schema_jobs.insert(job.job_id, job);
                        Ok(())
                    }
                }
            }
            MetaCommand::UpdateSchemaJob {
                job_id,
                state,
                updated_at,
                error,
                expected_version,
            } => {
                let Some(record) = self.schema_jobs.get_mut(job_id) else {
                    return Err(MetaRejectionReason::NotFound {
                        resource: format!("schema job {job_id}"),
                    });
                };
                if let Some(expected) = expected_version {
                    if *expected != record.metadata_version {
                        return Err(MetaRejectionReason::StaleWrite {
                            resource: format!("schema job {job_id}"),
                            current: record.metadata_version,
                            attempted: *expected,
                        });
                    }
                }
                if record.state.is_terminal() {
                    return Err(MetaRejectionReason::Conflict {
                        resource: format!("schema job {job_id}"),
                        reason: format!("terminal state {:?}", record.state),
                    });
                }
                if record.state != *state && !record.state.can_transition(*state) {
                    return Err(MetaRejectionReason::Conflict {
                        resource: format!("schema job {job_id}"),
                        reason: format!("illegal transition {:?} -> {:?}", record.state, state),
                    });
                }
                if record.state == *state && record.error == *error {
                    return Ok(());
                }
                record.state = *state;
                record.updated_at = *updated_at;
                record.error = error.clone();
                record.metadata_version = version;
                Ok(())
            }
            MetaCommand::SetClusterSetting { key, value } => self.settings.apply(key, value),
            MetaCommand::ActivateFeature { activation } => {
                // Re-validate at quorum (ADR-0010 decision 4): the voter set
                // is every registered, non-decommissioned node's advertised
                // descriptor — the meta group owns cluster membership, so its
                // registry is the authoritative voter view (spec section 12.1
                // "cluster membership").
                let voters: Vec<NodeDescriptor> = self
                    .nodes
                    .values()
                    .filter(|record| record.descriptor.state != NodeState::Decommissioned)
                    .map(|record| record.descriptor.clone())
                    .collect();
                activation.validate(registry, self.feature_level, &voters)?;
                let already = self.feature_activations.iter().any(|applied| {
                    applied.feature == activation.feature && applied.level == activation.level
                });
                if already {
                    return Ok(());
                }
                if activation.level > self.feature_level {
                    self.feature_level = activation.level;
                }
                let mut applied = activation.clone();
                if applied.activated_at == HlcTimestamp::ZERO {
                    applied.activated_at = commit_ts;
                }
                self.feature_activations.push(applied);
                Ok(())
            }
            MetaCommand::SetTxnStatusPartition { partition } => {
                match self.txn_status_partitions.get(&partition.partition_id) {
                    Some(existing) if existing == partition => Ok(()),
                    _ => {
                        self.txn_status_partitions
                            .insert(partition.partition_id, partition.clone());
                        Ok(())
                    }
                }
            }
            MetaCommand::PublishSplit { command } => self.apply_split_publish(command, version),
            MetaCommand::PublishMerge { command } => self.apply_merge_publish(command, version),
            MetaCommand::AllocateRaftNodeIds { count } => {
                self.apply_allocate_raft_node_ids(*count, command_id)
            }
        }
    }

    /// Validates and applies the atomic split publication (spec section 12.5
    /// step 8): one command flips the children to `Active` and the source to
    /// `Retiring` at one shared generation. See [`MetaCommand::PublishSplit`]
    /// for the acceptance contract.
    fn apply_split_publish(
        &mut self,
        command: &SplitPublishCommand,
        version: MetadataVersion,
    ) -> Result<(), MetaRejectionReason> {
        command
            .validate()
            .map_err(|error| MetaRejectionReason::Invalid {
                reason: error.to_string(),
            })?;
        if !self.tables.contains_key(&command.source.table_id) {
            return Err(MetaRejectionReason::NotFound {
                resource: format!("table {}", command.source.table_id),
            });
        }
        let publish = command.publish_generation();
        // Idempotent replay: a split resumed after a crash in the
        // publication barrier re-publishes the identical command.
        let replay = self.tablet(command.source.tablet_id) == Some(&command.source)
            && command
                .children
                .iter()
                .all(|child| self.tablet(child.tablet_id) == Some(child));
        if replay {
            return Ok(());
        }
        let precursor_generation =
            publish
                .checked_sub(1)
                .ok_or_else(|| MetaRejectionReason::Invalid {
                    reason: "publication generation is zero".to_owned(),
                })?;
        // The stored source must be the command's `Splitting` precursor at
        // exactly `publish - 1` (it was marked at `g + 1`; the publication
        // assigns `g + 2`).
        self.require_stored_precursor(
            &command.source,
            TabletState::Splitting,
            precursor_generation,
            false,
        )?;
        // Each stored child must be the command's `Creating` precursor (same
        // content with learner replicas) at exactly `publish - 1`.
        for child in &command.children {
            self.require_stored_precursor(
                child,
                TabletState::Creating,
                precursor_generation,
                true,
            )?;
        }
        // No other routable tablet of the table may overlap a child (the
        // participants are excluded; `Creating`/`Retiring` tablets are not
        // serving, so they cannot double-serve a key).
        let participants: BTreeSet<TabletId> = command
            .children
            .iter()
            .map(|child| child.tablet_id)
            .chain([command.source.tablet_id])
            .collect();
        for child in &command.children {
            self.require_no_routable_overlap(child, &participants)?;
        }
        for child in &command.children {
            self.tablets.insert(
                child.tablet_id,
                TabletRecord {
                    descriptor: child.clone(),
                    metadata_version: version,
                },
            );
        }
        self.tablets.insert(
            command.source.tablet_id,
            TabletRecord {
                descriptor: command.source.clone(),
                metadata_version: version,
            },
        );
        Ok(())
    }

    /// Validates and applies the atomic merge publication (spec section
    /// 12.6): one command flips the hidden replacement to `Active` and both
    /// sources to `Retiring` at one command-wide generation. See
    /// [`MetaCommand::PublishMerge`] for the acceptance contract.
    fn apply_merge_publish(
        &mut self,
        command: &MergePublishCommand,
        version: MetadataVersion,
    ) -> Result<(), MetaRejectionReason> {
        command
            .validate()
            .map_err(|error| MetaRejectionReason::Invalid {
                reason: error.to_string(),
            })?;
        if !self.tables.contains_key(&command.replacement.table_id) {
            return Err(MetaRejectionReason::NotFound {
                resource: format!("table {}", command.replacement.table_id),
            });
        }
        let publish = command.publish_generation();
        let replay = self.tablet(command.replacement.tablet_id) == Some(&command.replacement)
            && command
                .sources
                .iter()
                .all(|source| self.tablet(source.tablet_id) == Some(source));
        if replay {
            return Ok(());
        }
        let precursor_generation =
            publish
                .checked_sub(1)
                .ok_or_else(|| MetaRejectionReason::Invalid {
                    reason: "publication generation is zero".to_owned(),
                })?;
        // The stored replacement must be the command's `Creating` precursor
        // at exactly `publish - 1` (it was created at `max(g1, g2) + 1`);
        // that anchors the command-wide generation: both sources were marked
        // at their own `g + 1`, at or below `publish - 1`.
        self.require_stored_precursor(
            &command.replacement,
            TabletState::Creating,
            precursor_generation,
            true,
        )?;
        // Each stored source must be the command's `Merging` precursor below
        // the publication generation (a source whose generation lags jumps
        // to the command-wide generation at publish).
        for source in &command.sources {
            let stored = self.tablet(source.tablet_id).cloned().ok_or_else(|| {
                MetaRejectionReason::NotFound {
                    resource: format!("tablet {}", source.tablet_id),
                }
            })?;
            let mut precursor = stored.clone();
            precursor.state = TabletState::Retiring;
            precursor.generation = publish;
            if stored.state != TabletState::Merging
                || stored.generation >= publish
                || precursor != *source
            {
                return Err(MetaRejectionReason::Conflict {
                    resource: format!("tablet {}", source.tablet_id),
                    reason: format!(
                        "stored descriptor (state {}, generation {}) is not the Merging \
                         precursor of the publication (generation {publish})",
                        stored.state, stored.generation
                    ),
                });
            }
        }
        let participants: BTreeSet<TabletId> = command
            .sources
            .iter()
            .map(|source| source.tablet_id)
            .chain([command.replacement.tablet_id])
            .collect();
        self.require_no_routable_overlap(&command.replacement, &participants)?;
        self.tablets.insert(
            command.replacement.tablet_id,
            TabletRecord {
                descriptor: command.replacement.clone(),
                metadata_version: version,
            },
        );
        for source in &command.sources {
            self.tablets.insert(
                source.tablet_id,
                TabletRecord {
                    descriptor: source.clone(),
                    metadata_version: version,
                },
            );
        }
        Ok(())
    }

    /// The stored descriptor of `published.tablet_id`, verified as the
    /// publication's precursor: in `precursor_state` at `precursor_generation`
    /// with otherwise identical content (a published child's learner replicas
    /// promote to voters, so those compare with roles normalized).
    fn require_stored_precursor(
        &self,
        published: &TabletDescriptor,
        precursor_state: TabletState,
        precursor_generation: u64,
        learners_promoted: bool,
    ) -> Result<TabletDescriptor, MetaRejectionReason> {
        let stored = self.tablet(published.tablet_id).cloned().ok_or_else(|| {
            MetaRejectionReason::NotFound {
                resource: format!("tablet {}", published.tablet_id),
            }
        })?;
        let mut precursor = stored.clone();
        precursor.state = published.state;
        precursor.generation = published.generation;
        if learners_promoted {
            for replica in &mut precursor.replicas {
                replica.role = ReplicaRole::Voter;
            }
        }
        if stored.state != precursor_state
            || stored.generation != precursor_generation
            || precursor != *published
        {
            return Err(MetaRejectionReason::Conflict {
                resource: format!("tablet {}", published.tablet_id),
                reason: format!(
                    "stored descriptor (state {}, generation {}) is not the {} precursor of \
                     the publication (state {}, generation {})",
                    stored.state,
                    stored.generation,
                    precursor_state,
                    published.state,
                    published.generation
                ),
            });
        }
        Ok(stored)
    }

    /// Fails when any routable tablet of `published.table_id` outside
    /// `participants` overlaps `published`'s bounds (two serving tablets
    /// covering one key would double-serve reads).
    fn require_no_routable_overlap(
        &self,
        published: &TabletDescriptor,
        participants: &BTreeSet<TabletId>,
    ) -> Result<(), MetaRejectionReason> {
        let overlapping =
            self.tablets
                .values()
                .map(|record| &record.descriptor)
                .find(|descriptor| {
                    !participants.contains(&descriptor.tablet_id)
                        && descriptor.table_id == published.table_id
                        && descriptor.state.is_routable()
                        && descriptor.partition.overlaps(&published.partition)
                });
        if let Some(other) = overlapping {
            return Err(MetaRejectionReason::Conflict {
                resource: format!("tablet {}", published.tablet_id),
                reason: format!("bounds overlap routable tablet {}", other.tablet_id),
            });
        }
        Ok(())
    }

    /// Applies one raft-node-id allocation (see
    /// [`MetaCommand::AllocateRaftNodeIds`]): draws `count` ids from the
    /// monotonic counter, skipping ids already in use, and records the base
    /// under the command id for idempotent replay.
    fn apply_allocate_raft_node_ids(
        &mut self,
        count: u32,
        command_id: Option<[u8; 16]>,
    ) -> Result<(), MetaRejectionReason> {
        if count == 0 || count > MAX_RAFT_NODE_ID_ALLOCATION {
            return Err(MetaRejectionReason::Invalid {
                reason: format!(
                    "raft node id allocation count {count} is outside \
                     1..={MAX_RAFT_NODE_ID_ALLOCATION}"
                ),
            });
        }
        if let Some(id) = command_id {
            if self.raft_id_allocations.contains_key(&hex_encode(&id)) {
                // Idempotent replay of an already-applied allocation.
                return Ok(());
            }
        }
        let mut used = BTreeSet::new();
        for record in self.nodes.values() {
            used.insert(raft_node_id(&record.descriptor.node_id));
        }
        for record in self.tablets.values() {
            for replica in &record.descriptor.replicas {
                used.insert(replica.raft_node_id);
            }
        }
        for placement in self.placements.values() {
            for replica in &placement.replicas {
                used.insert(replica.raft_node_id);
            }
        }
        let count = u64::from(count);
        let mut base = self.next_raft_node_id.max(FIRST_RAFT_NODE_ID);
        let mut skips = 0u64;
        loop {
            let end = base
                .checked_add(count)
                .ok_or_else(|| MetaRejectionReason::Conflict {
                    resource: "raft node id allocator".to_owned(),
                    reason: "allocation overflows u64".to_owned(),
                })?;
            match used.range(base..end).next() {
                None => break,
                Some(conflicting) => {
                    skips += 1;
                    if skips > RAFT_ID_ALLOCATION_SCAN_LIMIT {
                        return Err(MetaRejectionReason::Conflict {
                            resource: "raft node id allocator".to_owned(),
                            reason: format!(
                                "collision skip scan exceeded {RAFT_ID_ALLOCATION_SCAN_LIMIT} \
                                 allocated ids"
                            ),
                        });
                    }
                    base = conflicting + 1;
                }
            }
        }
        let end = base + count;
        self.next_raft_node_id = end;
        if let Some(id) = command_id {
            let key = hex_encode(&id);
            self.raft_id_allocations.insert(key.clone(), base);
            self.raft_id_allocation_order.push_back(key);
            while self.raft_id_allocation_order.len() > RAFT_ID_ALLOCATION_RECORD_LIMIT {
                if let Some(oldest) = self.raft_id_allocation_order.pop_front() {
                    self.raft_id_allocations.remove(&oldest);
                }
            }
        }
        Ok(())
    }

    /// One registered node's descriptor.
    pub fn node(&self, node_id: NodeId) -> Option<&NodeDescriptor> {
        self.nodes.get(&node_id).map(|record| &record.descriptor)
    }

    /// One registered node's record (descriptor + modification version).
    pub fn node_record(&self, node_id: NodeId) -> Option<&NodeRecord> {
        self.nodes.get(&node_id)
    }

    /// Every registered node descriptor, in node-id order.
    pub fn node_descriptors(&self) -> Vec<NodeDescriptor> {
        self.nodes
            .values()
            .map(|record| record.descriptor.clone())
            .collect()
    }

    /// One database descriptor by id.
    pub fn database(&self, database_id: DatabaseId) -> Option<&DatabaseDescriptor> {
        self.databases.get(&database_id)
    }

    /// One database descriptor by name.
    pub fn database_by_name(&self, name: &str) -> Option<&DatabaseDescriptor> {
        self.databases
            .values()
            .find(|database| database.name == name)
    }

    /// One table's schema record.
    pub fn table(&self, table_id: TableId) -> Option<&TableSchemaRecord> {
        self.tables.get(&table_id)
    }

    /// One tablet descriptor (the canonical `crate::tablet` shape).
    pub fn tablet(&self, tablet_id: TabletId) -> Option<&TabletDescriptor> {
        self.tablets
            .get(&tablet_id)
            .map(|record| &record.descriptor)
    }

    /// One tablet record (descriptor + modification version).
    pub fn tablet_record(&self, tablet_id: TabletId) -> Option<&TabletRecord> {
        self.tablets.get(&tablet_id)
    }

    /// One raft group's replica placement.
    pub fn placement(&self, raft_group_id: RaftGroupId) -> Option<&ReplicaPlacement> {
        self.placements.get(&raft_group_id)
    }

    /// One named placement policy (the canonical `crate::placement` shape).
    pub fn placement_policy(&self, name: &str) -> Option<&PlacementPolicy> {
        self.placement_policies
            .get(name)
            .map(|record| &record.policy)
    }

    /// One named placement policy record (policy + modification version).
    pub fn placement_policy_record(&self, name: &str) -> Option<&PlacementPolicyRecord> {
        self.placement_policies.get(name)
    }

    /// One schema job record.
    pub fn schema_job(&self, job_id: u64) -> Option<&SchemaJobRecord> {
        self.schema_jobs.get(&job_id)
    }

    /// One transaction status partition.
    pub fn txn_status_partition(&self, partition_id: u32) -> Option<&TxnStatusPartition> {
        self.txn_status_partitions.get(&partition_id)
    }

    /// The dynamic cluster settings.
    pub fn settings(&self) -> &ClusterSettings {
        &self.settings
    }

    /// The cluster feature level.
    pub fn feature_level(&self) -> ClusterFeatureLevel {
        self.feature_level
    }

    /// Whether `feature` is active at the state's current feature level.
    pub fn feature_active(&self, registry: &FeatureRegistry, feature: &str) -> bool {
        registry.feature_supported(self.feature_level, feature)
    }

    /// The applied feature activations, oldest first.
    pub fn feature_activations(&self) -> &[FeatureActivation] {
        &self.feature_activations
    }

    /// The bounded refusal journal, oldest first.
    pub fn rejections(&self) -> &VecDeque<MetaRejection> {
        &self.rejections
    }

    /// The next id the meta-owned raft-node-id allocator hands out.
    pub fn next_raft_node_id(&self) -> u64 {
        self.next_raft_node_id
    }

    /// The base of the range allocated by the command with id `command_id`,
    /// when the allocation record is still retained
    /// ([`RAFT_ID_ALLOCATION_RECORD_LIMIT`]).
    pub fn raft_id_allocation(&self, command_id: &[u8; 16]) -> Option<u64> {
        self.raft_id_allocations
            .get(&hex_encode(command_id))
            .copied()
    }
}

// ---------------------------------------------------------------------------
// Format v1 compatibility (spec sections 4.10, 17; module docs)
// ---------------------------------------------------------------------------
//
// The serde shapes the first meta control-plane build (format v1) persisted
// and replicated, kept verbatim so v1 command records and v1 state
// checkpoints decode forever. Every field is read by the migration functions
// below, which map the v1 meta-local mirrors onto the canonical
// `crate::tablet` / `crate::placement` types (see the module docs for the
// mapping decisions).
mod v1 {
    use super::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub enum ReplicaRole {
        Voter,
        Learner,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ReplicaDescriptor {
        pub node_id: NodeId,
        pub role: ReplicaRole,
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct PartitionBounds {
        pub start: Option<Vec<u8>>,
        pub end: Option<Vec<u8>>,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub enum TabletState {
        Creating,
        Online,
        Offline,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct TabletDescriptor {
        pub tablet_id: TabletId,
        pub table_id: TableId,
        pub raft_group_id: RaftGroupId,
        pub partition: PartitionBounds,
        pub replicas: Vec<ReplicaDescriptor>,
        pub leader_hint: Option<NodeId>,
        pub generation: u64,
        pub state: TabletState,
        #[serde(default = "zero_metadata_version")]
        pub metadata_version: MetadataVersion,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ReplicaPlacement {
        pub raft_group_id: RaftGroupId,
        pub replicas: Vec<ReplicaDescriptor>,
        #[serde(default = "zero_metadata_version")]
        pub metadata_version: MetadataVersion,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct LocalityConstraint {
        pub key: String,
        pub value: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct PlacementPolicy {
        pub replicas: u8,
        pub voter_constraints: Vec<LocalityConstraint>,
        pub leader_preferences: Vec<LocalityConstraint>,
        pub prohibited_nodes: Vec<NodeId>,
        #[serde(default = "zero_metadata_version")]
        pub metadata_version: MetadataVersion,
    }

    /// The v1 command enum: identical variant set; only the tablet/placement
    /// payloads differ from the current (v2) shapes.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub enum MetaCommand {
        RegisterNode {
            descriptor: NodeDescriptor,
        },
        UpdateNodeState {
            node_id: NodeId,
            state: NodeState,
            expected_version: Option<MetadataVersion>,
        },
        RemoveNode {
            node_id: NodeId,
        },
        CreateDatabase {
            descriptor: DatabaseDescriptor,
        },
        DropDatabase {
            database_id: DatabaseId,
        },
        SetTableSchema {
            record: TableSchemaRecord,
        },
        SetTabletDescriptor {
            descriptor: TabletDescriptor,
        },
        RemoveTabletDescriptor {
            tablet_id: TabletId,
            generation: u64,
        },
        SetReplicaPlacement {
            placement: ReplicaPlacement,
        },
        SetPlacementPolicy {
            name: String,
            policy: PlacementPolicy,
        },
        SubmitSchemaJob {
            job: SchemaJobRecord,
        },
        UpdateSchemaJob {
            job_id: u64,
            state: SchemaJobState,
            updated_at: HlcTimestamp,
            error: Option<String>,
            expected_version: Option<MetadataVersion>,
        },
        SetClusterSetting {
            key: String,
            value: serde_json::Value,
        },
        ActivateFeature {
            activation: FeatureActivation,
        },
        SetTxnStatusPartition {
            partition: TxnStatusPartition,
        },
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct MetaCommandRecord {
        pub format_version: u32,
        pub command: MetaCommand,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    #[serde(default)]
    pub struct MetaState {
        pub format_version: u32,
        pub metadata_version: MetadataVersion,
        pub nodes: BTreeMap<NodeId, NodeRecord>,
        pub databases: BTreeMap<DatabaseId, DatabaseDescriptor>,
        pub tables: BTreeMap<TableId, TableSchemaRecord>,
        pub tablets: BTreeMap<TabletId, TabletDescriptor>,
        pub placements: BTreeMap<RaftGroupId, ReplicaPlacement>,
        pub placement_policies: BTreeMap<String, PlacementPolicy>,
        pub schema_jobs: BTreeMap<u64, SchemaJobRecord>,
        pub txn_status_partitions: BTreeMap<u32, TxnStatusPartition>,
        pub settings: ClusterSettings,
        pub feature_level: ClusterFeatureLevel,
        pub feature_activations: Vec<FeatureActivation>,
        pub rejections: VecDeque<MetaRejection>,
    }

    impl Default for MetaState {
        fn default() -> Self {
            Self {
                format_version: 1,
                metadata_version: MetadataVersion::ZERO,
                nodes: BTreeMap::new(),
                databases: BTreeMap::new(),
                tables: BTreeMap::new(),
                tablets: BTreeMap::new(),
                placements: BTreeMap::new(),
                placement_policies: BTreeMap::new(),
                schema_jobs: BTreeMap::new(),
                txn_status_partitions: BTreeMap::new(),
                settings: ClusterSettings::default(),
                feature_level: ClusterFeatureLevel::ZERO,
                feature_activations: Vec::new(),
                rejections: VecDeque::new(),
            }
        }
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct MetaStateCheckpoint {
        pub format_version: u32,
        pub position: LogPosition,
        pub command_id: Option<[u8; 16]>,
        pub state: MetaState,
    }
}

impl From<v1::ReplicaRole> for ReplicaRole {
    fn from(role: v1::ReplicaRole) -> Self {
        match role {
            v1::ReplicaRole::Voter => Self::Voter,
            v1::ReplicaRole::Learner => Self::Learner,
        }
    }
}

/// v1 replicas carried no per-group raft id; the projection of the node id is
/// the id the v1 raft group actually used for the replica.
fn migrate_replica(replica: v1::ReplicaDescriptor) -> ReplicaDescriptor {
    ReplicaDescriptor {
        node_id: replica.node_id,
        role: replica.role.into(),
        raft_node_id: raft_node_id(&replica.node_id),
    }
}

/// v1 bounds were `Option` endpoints (start inclusive, end exclusive); the
/// canonical bounds carry the same semantics as `Bound` endpoints.
fn migrate_bounds(bounds: v1::PartitionBounds) -> PartitionBounds {
    PartitionBounds {
        low: match bounds.start {
            None => Bound::Unbounded,
            Some(start) => Bound::Included(Key::from_bytes(start)),
        },
        high: match bounds.end {
            None => Bound::Unbounded,
            Some(end) => Bound::Excluded(Key::from_bytes(end)),
        },
    }
}

fn migrate_tablet_state(state: v1::TabletState) -> TabletState {
    match state {
        v1::TabletState::Creating => TabletState::Creating,
        v1::TabletState::Online => TabletState::Active,
        // v1 "being removed; drained before the descriptor is deleted" is the
        // canonical "retired from routing, retained until no pins remain".
        v1::TabletState::Offline => TabletState::Retiring,
    }
}

fn migrate_tablet(tablet: v1::TabletDescriptor) -> TabletRecord {
    TabletRecord {
        descriptor: TabletDescriptor {
            tablet_id: tablet.tablet_id,
            table_id: tablet.table_id,
            raft_group_id: tablet.raft_group_id,
            partition: migrate_bounds(tablet.partition),
            replicas: tablet.replicas.into_iter().map(migrate_replica).collect(),
            leader_hint: tablet.leader_hint,
            generation: tablet.generation,
            state: migrate_tablet_state(tablet.state),
        },
        metadata_version: tablet.metadata_version,
    }
}

fn migrate_placement(placement: v1::ReplicaPlacement) -> ReplicaPlacement {
    ReplicaPlacement {
        raft_group_id: placement.raft_group_id,
        replicas: placement
            .replicas
            .into_iter()
            .map(migrate_replica)
            .collect(),
        metadata_version: placement.metadata_version,
    }
}

/// v1 voter constraints were hard requirements ("every voter must satisfy");
/// v1 leader preferences were soft ("preferences, in priority order").
fn migrate_policy(policy: v1::PlacementPolicy) -> PlacementPolicyRecord {
    PlacementPolicyRecord {
        policy: PlacementPolicy {
            replicas: policy.replicas,
            voter_constraints: policy
                .voter_constraints
                .into_iter()
                .map(|constraint| LocalityConstraint {
                    key: constraint.key,
                    value: constraint.value,
                    required: true,
                })
                .collect(),
            leader_preferences: policy
                .leader_preferences
                .into_iter()
                .map(|constraint| LocalityConstraint {
                    key: constraint.key,
                    value: constraint.value,
                    required: false,
                })
                .collect(),
            prohibited_nodes: policy.prohibited_nodes,
        },
        metadata_version: policy.metadata_version,
    }
}

fn migrate_command(command: v1::MetaCommand) -> MetaCommand {
    match command {
        v1::MetaCommand::RegisterNode { descriptor } => MetaCommand::RegisterNode { descriptor },
        v1::MetaCommand::UpdateNodeState {
            node_id,
            state,
            expected_version,
        } => MetaCommand::UpdateNodeState {
            node_id,
            state,
            expected_version,
        },
        v1::MetaCommand::RemoveNode { node_id } => MetaCommand::RemoveNode { node_id },
        v1::MetaCommand::CreateDatabase { descriptor } => {
            MetaCommand::CreateDatabase { descriptor }
        }
        v1::MetaCommand::DropDatabase { database_id } => MetaCommand::DropDatabase { database_id },
        v1::MetaCommand::SetTableSchema { record } => MetaCommand::SetTableSchema { record },
        v1::MetaCommand::SetTabletDescriptor { descriptor } => MetaCommand::SetTabletDescriptor {
            descriptor: migrate_tablet(descriptor).descriptor,
        },
        v1::MetaCommand::RemoveTabletDescriptor {
            tablet_id,
            generation,
        } => MetaCommand::RemoveTabletDescriptor {
            tablet_id,
            generation,
        },
        v1::MetaCommand::SetReplicaPlacement { placement } => MetaCommand::SetReplicaPlacement {
            placement: migrate_placement(placement),
        },
        v1::MetaCommand::SetPlacementPolicy { name, policy } => MetaCommand::SetPlacementPolicy {
            name,
            policy: migrate_policy(policy).policy,
        },
        v1::MetaCommand::SubmitSchemaJob { job } => MetaCommand::SubmitSchemaJob { job },
        v1::MetaCommand::UpdateSchemaJob {
            job_id,
            state,
            updated_at,
            error,
            expected_version,
        } => MetaCommand::UpdateSchemaJob {
            job_id,
            state,
            updated_at,
            error,
            expected_version,
        },
        v1::MetaCommand::SetClusterSetting { key, value } => {
            MetaCommand::SetClusterSetting { key, value }
        }
        v1::MetaCommand::ActivateFeature { activation } => {
            MetaCommand::ActivateFeature { activation }
        }
        v1::MetaCommand::SetTxnStatusPartition { partition } => {
            MetaCommand::SetTxnStatusPartition { partition }
        }
    }
}

fn migrate_state(state: v1::MetaState) -> MetaState {
    MetaState {
        format_version: META_STATE_FORMAT_VERSION,
        metadata_version: state.metadata_version,
        nodes: state.nodes,
        databases: state.databases,
        tables: state.tables,
        tablets: state
            .tablets
            .into_iter()
            .map(|(tablet_id, tablet)| (tablet_id, migrate_tablet(tablet)))
            .collect(),
        placements: state
            .placements
            .into_iter()
            .map(|(group_id, placement)| (group_id, migrate_placement(placement)))
            .collect(),
        placement_policies: state
            .placement_policies
            .into_iter()
            .map(|(name, policy)| (name, migrate_policy(policy)))
            .collect(),
        schema_jobs: state.schema_jobs,
        txn_status_partitions: state.txn_status_partitions,
        settings: state.settings,
        feature_level: state.feature_level,
        feature_activations: state.feature_activations,
        rejections: state.rejections,
        // v1 pre-dates the meta-owned raft-id allocator; the counter starts
        // fresh (ad-hoc replica raft ids of v1 deployments stay as recorded
        // in their descriptors and are skipped by the collision scan).
        next_raft_node_id: FIRST_RAFT_NODE_ID,
        raft_id_allocations: BTreeMap::new(),
        raft_id_allocation_order: VecDeque::new(),
    }
}

fn migrate_checkpoint(checkpoint: v1::MetaStateCheckpoint) -> MetaStateCheckpoint {
    MetaStateCheckpoint {
        format_version: META_STATE_CHECKPOINT_FORMAT_VERSION,
        position: checkpoint.position,
        command_id: checkpoint.command_id,
        state: migrate_state(checkpoint.state),
    }
}

/// Decode-time probe of the `format_version` field every versioned meta
/// payload (command records, state checkpoints) carries as its first field.
#[derive(Deserialize)]
struct FormatVersionProbe {
    format_version: u32,
}

/// Parses and verifies a checkpoint produced by [`MetaStateCheckpoint`]
/// serialization; v1 checkpoints migrate (see the module docs). Unknown
/// versions and malformed payloads fail closed.
fn decode_meta_checkpoint(bytes: &[u8]) -> Result<MetaStateCheckpoint, String> {
    let probe: FormatVersionProbe =
        serde_json::from_slice(bytes).map_err(|error| format!("decode: {error}"))?;
    if probe.format_version < MIN_SUPPORTED_META_STATE_CHECKPOINT_FORMAT_VERSION
        || probe.format_version > META_STATE_CHECKPOINT_FORMAT_VERSION
    {
        return Err(format!(
            "unsupported format version {} (supported \
             {MIN_SUPPORTED_META_STATE_CHECKPOINT_FORMAT_VERSION}..=\
             {META_STATE_CHECKPOINT_FORMAT_VERSION})",
            probe.format_version
        ));
    }
    if probe.format_version == 1 {
        let checkpoint: v1::MetaStateCheckpoint =
            serde_json::from_slice(bytes).map_err(|error| format!("decode v1: {error}"))?;
        if checkpoint.format_version != probe.format_version {
            return Err("inconsistent format version".to_owned());
        }
        return Ok(migrate_checkpoint(checkpoint));
    }
    serde_json::from_slice(bytes).map_err(|error| format!("decode: {error}"))
}

// ---------------------------------------------------------------------------
// MetaApplySink
// ---------------------------------------------------------------------------

/// Name of the sink's durable checkpoint file inside `<group dir>/raft/state/`.
pub const META_STATE_CHECKPOINT_FILENAME: &str = "meta-state.json";
/// Format version of the checkpoint file this build writes (v2 carries the
/// reconciled canonical tablet/placement types; see the module docs).
pub const META_STATE_CHECKPOINT_FORMAT_VERSION: u32 = 2;
/// Oldest checkpoint format version this build accepts.
pub const MIN_SUPPORTED_META_STATE_CHECKPOINT_FORMAT_VERSION: u32 = 1;

/// The sink's durable checkpoint: the applied state plus the log watermark
/// it reflects. The same record is the sink's snapshot payload, so snapshot
/// install and restart recovery restore byte-identical state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetaStateCheckpoint {
    /// Checkpoint format version; see [`META_STATE_CHECKPOINT_FORMAT_VERSION`].
    pub format_version: u32,
    /// Log position the state reflects (the replay watermark).
    pub position: LogPosition,
    /// Last applied command id (informational; the state machine owns the
    /// idempotent-replay set).
    pub command_id: Option<[u8; 16]>,
    /// The applied meta state.
    pub state: MetaState,
}

/// The [`ApplySink`] binding the meta group's committed commands to
/// [`MetaState`] (spec section 12.1).
///
/// # Persistence
///
/// The sink checkpoints [`MetaStateCheckpoint`] to
/// `<group dir>/raft/state/meta-state.json` after every dispatched entry
/// (atomic temp-write + rename + directory fsync, the crate's metadata
/// idiom). This is required, not decorative: the consensus state machine
/// persists its apply checkpoint per batch, so after a restart openraft
/// replays only entries *after* that checkpoint — an in-memory-only sink
/// would come back empty while the log says it applied everything (the
/// engine sink's applied database root is its equivalent durable state).
///
/// The dispatch order is sink-first, checkpoint-second (see the consensus
/// state machine docs): a crash in that window can redeliver an entry the
/// sink already applied and persisted. The checkpoint's `position` is the
/// durable watermark — redelivered entries at or below it are skipped, so
/// `metadata_version` and the records never double-apply.
///
/// Apply is deterministic and total. Refused commands are journaled in
/// state, never returned as state-machine errors; genuine faults fail
/// closed: an undecodable payload, an envelope that is not a
/// [`COMMAND_TYPE_META_COMMAND`] catalog command, or — per spec section
/// 12.1's "no user row data" — a transaction command misrouted to the meta
/// group.
pub struct MetaApplySink {
    state: MetaState,
    position: LogPosition,
    command_id: Option<[u8; 16]>,
    registry: FeatureRegistry,
    /// `<group dir>/raft/state`.
    state_dir: PathBuf,
}

impl MetaApplySink {
    /// Opens (creating if needed) the sink under `group_dir`, loading the
    /// persisted checkpoint when present. A present but undecodable or
    /// unsupported-version checkpoint fails closed (spec section 4.10).
    ///
    /// `registry` is the binary's feature registry (identical on every
    /// replica of one deployment); [`FeatureRegistry::current`] in
    /// production.
    pub fn open(group_dir: &Path, registry: FeatureRegistry) -> Result<Self, MetaError> {
        let state_dir = group_dir.join("raft").join("state");
        std::fs::create_dir_all(&state_dir).map_err(MetaError::Io)?;
        let checkpoint_path = state_dir.join(META_STATE_CHECKPOINT_FILENAME);
        let Some(bytes) =
            crate::node::read_meta_file(&checkpoint_path).map_err(|error| match error {
                crate::node::ClusterError::Io(error) => MetaError::Io(error),
                other => MetaError::CorruptCheckpoint(other.to_string()),
            })?
        else {
            return Ok(MetaApplySink {
                state: MetaState::default(),
                position: LogPosition::ZERO,
                command_id: None,
                registry,
                state_dir,
            });
        };
        let checkpoint = decode_meta_checkpoint(&bytes).map_err(MetaError::CorruptCheckpoint)?;
        if checkpoint.state.format_version < MIN_SUPPORTED_META_STATE_FORMAT_VERSION
            || checkpoint.state.format_version > META_STATE_FORMAT_VERSION
        {
            return Err(MetaError::CorruptCheckpoint(format!(
                "unsupported meta state format version {} (supported \
                 {MIN_SUPPORTED_META_STATE_FORMAT_VERSION}..={META_STATE_FORMAT_VERSION})",
                checkpoint.state.format_version
            )));
        }
        Ok(MetaApplySink {
            state: checkpoint.state,
            position: checkpoint.position,
            command_id: checkpoint.command_id,
            registry,
            state_dir,
        })
    }

    /// The current replicated state.
    pub fn state(&self) -> &MetaState {
        &self.state
    }

    /// The monotonic per-applied-command version.
    pub fn metadata_version(&self) -> MetadataVersion {
        self.state.metadata_version
    }

    /// The log position the state reflects (the crash-window replay
    /// watermark).
    pub fn applied_position(&self) -> LogPosition {
        self.position
    }

    fn checkpoint(&self) -> MetaStateCheckpoint {
        MetaStateCheckpoint {
            format_version: META_STATE_CHECKPOINT_FORMAT_VERSION,
            position: self.position,
            command_id: self.command_id,
            state: self.state.clone(),
        }
    }

    fn persist(&self) -> Result<(), StateMachineError> {
        let bytes = serde_json::to_vec(&self.checkpoint())
            .map_err(|error| StateMachineError::Sink(format!("meta checkpoint encode: {error}")))?;
        crate::node::write_meta_atomic(&self.state_dir, META_STATE_CHECKPOINT_FILENAME, &bytes)
            .map_err(|error| StateMachineError::Sink(format!("meta checkpoint write: {error}")))
    }
}

impl ApplySink for MetaApplySink {
    fn apply(&mut self, command: &AppliedCommand) -> Result<(), StateMachineError> {
        // Crash-window replay: the sink persisted this entry (or a later
        // one) already; skip it so versions and records never double-apply.
        if command.position.index <= self.position.index {
            return Ok(());
        }
        match &command.command {
            ReplicatedCommand::Catalog(catalog) => {
                catalog.envelope.verify().map_err(|error| {
                    StateMachineError::Corrupt(format!("meta envelope: {error}"))
                })?;
                if catalog.envelope.command_type != COMMAND_TYPE_META_COMMAND {
                    return Err(StateMachineError::Corrupt(format!(
                        "meta command_type {} is not COMMAND_TYPE_META_COMMAND",
                        catalog.envelope.command_type
                    )));
                }
                let record = MetaCommandRecord::decode(&catalog.envelope.payload)
                    .map_err(|error| StateMachineError::Corrupt(error.to_string()))?;
                let commit_ts = command.commit_ts().unwrap_or(HlcTimestamp::ZERO);
                // A refusal is journaled state, not a state-machine error.
                let _ = self.state.apply(
                    &record.command,
                    command.command_id(),
                    commit_ts,
                    &self.registry,
                );
            }
            // Maintenance commands are node-runtime directives and Noop
            // advances the commit index; neither touches meta state.
            ReplicatedCommand::Maintenance(_) | ReplicatedCommand::Noop => {}
            // The meta group owns control-plane state only (spec section
            // 12.1): a transaction command here is misrouted — fail closed.
            ReplicatedCommand::Transaction(_) => {
                return Err(StateMachineError::Corrupt(
                    "transaction command on the meta group: the meta group owns \
                     control-plane state only (spec section 12.1)"
                        .to_owned(),
                ));
            }
        }
        self.position = command.position;
        if let Some(command_id) = command.command_id() {
            self.command_id = Some(command_id);
        }
        self.persist()
    }

    fn snapshot(&self) -> Result<Vec<u8>, StateMachineError> {
        serde_json::to_vec(&self.checkpoint())
            .map_err(|error| StateMachineError::Sink(format!("meta snapshot encode: {error}")))
    }

    fn install(&mut self, data: &[u8]) -> Result<(), StateMachineError> {
        let checkpoint = decode_meta_checkpoint(data)
            .map_err(|error| StateMachineError::Corrupt(format!("meta snapshot {error}")))?;
        if checkpoint.state.format_version < MIN_SUPPORTED_META_STATE_FORMAT_VERSION
            || checkpoint.state.format_version > META_STATE_FORMAT_VERSION
        {
            return Err(StateMachineError::Corrupt(format!(
                "unsupported meta state format version {} (supported \
                 {MIN_SUPPORTED_META_STATE_FORMAT_VERSION}..={META_STATE_FORMAT_VERSION})",
                checkpoint.state.format_version
            )));
        }
        self.state = checkpoint.state;
        self.position = checkpoint.position;
        self.command_id = checkpoint.command_id;
        self.persist()
    }
}

impl fmt::Debug for MetaApplySink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetaApplySink")
            .field("metadata_version", &self.state.metadata_version)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// MetaGroup: bootstrap and membership workflow (spec sections 12.1, 12.7)
// ---------------------------------------------------------------------------

/// Static layout and identity of one meta-group member.
///
/// # Directory layout (mirrors the engine sink's single-group layout)
///
/// ```text
/// node-data/
///   groups/
///     <meta-group-id>/
///       raft/        log segments, vote, state machine checkpoint + snapshots
///       raft/state/meta-state.json   the sink's durable MetaState checkpoint
/// ```
///
/// The applied meta state is durable in the sink's checkpoint file (see
/// [`MetaApplySink`]) and travels inside raft snapshots for catch-up.
#[derive(Debug, Clone)]
pub struct MetaGroupConfig {
    /// The node's local data root (`node-data`).
    pub node_data: PathBuf,
    /// The dedicated meta group's durable identifier (minted at cluster
    /// bootstrap; never a tablet group).
    pub meta_group_id: RaftGroupId,
    /// This node's durable id.
    pub node_id: NodeId,
    /// The binary's feature registry gating [`MetaCommand::ActivateFeature`]
    /// at apply (identical on every replica of one deployment).
    pub registry: FeatureRegistry,
    /// Durable log storage configuration.
    pub storage: StorageConfig,
    /// Bound on the apply idempotency set (S2B-004).
    pub idempotency_retention: usize,
}

impl MetaGroupConfig {
    /// Required identities; registry, storage, and retention default to
    /// production values.
    pub fn new(node_data: PathBuf, meta_group_id: RaftGroupId, node_id: NodeId) -> Self {
        MetaGroupConfig {
            node_data,
            meta_group_id,
            node_id,
            registry: FeatureRegistry::current(),
            storage: StorageConfig::default(),
            idempotency_retention:
                mongreldb_consensus::state_machine::DEFAULT_IDEMPOTENCY_RETENTION,
        }
    }

    /// `<node-data>/groups/<meta-group-id>` — the group directory handed to
    /// [`ConsensusGroup`].
    pub fn group_dir(&self) -> PathBuf {
        self.node_data
            .join("groups")
            .join(self.meta_group_id.to_hex())
    }

    /// The group's text identifier (`meta-<hex>`).
    pub fn cluster_name(&self) -> String {
        format!("meta-{}", self.meta_group_id.to_hex())
    }

    /// The default [`GroupConfig`] for this member (production timings;
    /// callers may tune it before passing it to [`MetaGroup::create`]).
    pub fn group_config(&self) -> GroupConfig {
        let mut config = GroupConfig::new(
            self.cluster_name(),
            raft_node_id(&self.node_id),
            self.group_dir(),
        );
        config.storage = self.storage.clone();
        config.idempotency_retention = self.idempotency_retention;
        config
    }
}

/// Proof that a meta command was committed and applied (and not refused).
#[derive(Debug, Clone)]
pub struct MetaCommandReceipt {
    /// The consensus commit receipt (position, commit timestamp, command id,
    /// idempotent-replay flag).
    pub receipt: GroupCommitReceipt,
    /// The meta state's watermark after applying the command.
    pub metadata_version: MetadataVersion,
}

/// One member of the dedicated meta control-plane group (spec section 12.1):
/// a [`ConsensusGroup`] whose apply sink is a [`MetaApplySink`], plus the
/// bootstrap/membership/propose workflow the node runtime drives.
pub struct MetaGroup<T: RaftTransport> {
    group: ConsensusGroup<T>,
    sink: Arc<Mutex<MetaApplySink>>,
    config: MetaGroupConfig,
}

/// Builds one openraft node value for the membership calls without naming
/// the openraft type: the cluster crate deliberately has no openraft
/// dependency (ADR-0004 confines it to `mongreldb-consensus`), so the
/// concrete node type is inferred from the consuming [`ConsensusGroup`] call
/// and constructed through its serde shape (`{"addr": ..}`), which
/// `mongreldb-consensus` enables via openraft's `serde` feature.
fn basic_node<N>(address: &str) -> Result<N, MetaError>
where
    N: for<'de> Deserialize<'de>,
{
    serde_json::from_value(serde_json::json!({ "addr": address }))
        .map_err(|error| MetaError::InvalidRequest(format!("member address `{address}`: {error}")))
}

/// Mints a command id for the workflow's internal proposals.
pub(crate) fn new_command_id() -> Result<[u8; 16], MetaError> {
    let mut id = [0u8; 16];
    getrandom::getrandom(&mut id).map_err(|error| MetaError::Rng(error.to_string()))?;
    Ok(id)
}

impl<T: RaftTransport> MetaGroup<T> {
    /// Opens the group's durable state and starts the raft task with a
    /// [`MetaApplySink`] installed. `group_config` must match
    /// [`MetaGroupConfig::group_config`] (tuned timings are fine; identity
    /// and directory are not — mismatches fail closed).
    pub async fn create(
        config: MetaGroupConfig,
        group_config: GroupConfig,
        transport: Arc<T>,
    ) -> Result<Self, MetaError> {
        if group_config.node_id != raft_node_id(&config.node_id) {
            return Err(MetaError::InvalidRequest(format!(
                "group config raft id {} does not match the node id projection {}",
                group_config.node_id,
                raft_node_id(&config.node_id)
            )));
        }
        if group_config.dir != config.group_dir() {
            return Err(MetaError::InvalidRequest(format!(
                "group config dir {:?} is not the meta group dir {:?}",
                group_config.dir,
                config.group_dir()
            )));
        }
        let sink = Arc::new(Mutex::new(MetaApplySink::open(
            &config.group_dir(),
            config.registry.clone(),
        )?));
        let group = ConsensusGroup::create(
            group_config,
            transport,
            sink.clone() as Arc<Mutex<dyn ApplySink>>,
        )
        .await?;
        Ok(MetaGroup {
            group,
            sink,
            config,
        })
    }

    /// The underlying consensus group (snapshots, membership, transfer,
    /// read barriers, shutdown).
    pub fn group(&self) -> &ConsensusGroup<T> {
        &self.group
    }

    /// This node's durable id.
    pub fn node_id(&self) -> NodeId {
        self.config.node_id
    }

    /// The meta group's durable id.
    pub fn meta_group_id(&self) -> RaftGroupId {
        self.config.meta_group_id
    }

    /// Bootstraps a pristine meta group with the given voter set of
    /// `(node_id, rpc_address)` pairs (call on one pristine member; check
    /// [`MetaGroup::is_initialized`] on reopen). The 64-bit raft-id
    /// projection of distinct node ids must not collide (ADR: collisions are
    /// rejected at cluster bootstrap by this layer — the consensus adapter
    /// treats raft ids as opaque).
    pub async fn bootstrap(&self, members: &[(NodeId, String)]) -> Result<(), MetaError> {
        let mut projected: BTreeMap<RaftNodeId, NodeId> = BTreeMap::new();
        let mut map = BTreeMap::new();
        for (node_id, address) in members {
            let raft_id = raft_node_id(node_id);
            if let Some(prior) = projected.insert(raft_id, *node_id) {
                if prior != *node_id {
                    return Err(MetaError::InvalidRequest(format!(
                        "node id projection collision: {prior} and {node_id} both project to \
                         raft id {raft_id}; re-mint one of the node ids"
                    )));
                }
            }
            map.insert(raft_id, basic_node(address)?);
        }
        self.group
            .bootstrap(map)
            .await
            .map_err(MetaError::Consensus)
    }

    /// Whether this node already holds an initialized membership.
    pub async fn is_initialized(&self) -> Result<bool, MetaError> {
        self.group
            .is_initialized()
            .await
            .map_err(MetaError::Consensus)
    }

    /// Adds one member to the meta group (spec section 12.7's movement
    /// protocol, meta-group form): add learner and wait until it is
    /// line-rate, promote it to voter through joint consensus, then register
    /// its descriptor in replicated meta state. Registration comes last so
    /// the feature-activation voter view (registered, non-decommissioned
    /// descriptors) reflects only nodes that actually vote.
    pub async fn add_member(
        &self,
        descriptor: &NodeDescriptor,
        control: &ExecutionControl,
    ) -> Result<MetaCommandReceipt, MetaError> {
        let raft_id = raft_node_id(&descriptor.node_id);
        let (voters, learners) = self.group.members();
        if voters.contains(&raft_id) || learners.contains(&raft_id) {
            return Err(MetaError::InvalidRequest(format!(
                "node {} is already a meta group member",
                descriptor.node_id
            )));
        }
        self.group
            .add_learner(raft_id, basic_node(&descriptor.rpc_address)?)
            .await?;
        self.group.promote(raft_id).await?;
        self.propose(
            new_command_id()?,
            MetaCommand::RegisterNode {
                descriptor: descriptor.clone(),
            },
            control,
        )
        .await
    }

    /// Removes one member: the replicated [`MetaCommand::RemoveNode`] first
    /// (validating the node hosts no remaining replicas), then the
    /// joint-consensus removal. Validate-then-act keeps the two membership
    /// views from diverging on a refusal, and a retry after a mid-workflow
    /// failure is idempotent (`RemoveNode` of an absent node is a no-op).
    /// Leadership must be transferred off the node first (spec section
    /// 11.6); removing the current leader fails closed.
    pub async fn remove_member(
        &self,
        node_id: NodeId,
        control: &ExecutionControl,
    ) -> Result<MetaCommandReceipt, MetaError> {
        let raft_id = raft_node_id(&node_id);
        let metrics = self.group.metrics();
        if metrics.current_leader == Some(raft_id) {
            return Err(MetaError::InvalidRequest(
                "transfer leadership off the node before removing it".to_owned(),
            ));
        }
        let receipt = self
            .propose(
                new_command_id()?,
                MetaCommand::RemoveNode { node_id },
                control,
            )
            .await?;
        self.group.remove(raft_id).await?;
        Ok(receipt)
    }

    /// Proposes one meta command (quorum durability; spec section 11.3) and
    /// waits for commit + apply. `command_id` is the caller's idempotency
    /// token: a retry with the same id and payload replays the original
    /// apply without re-dispatching (S2B-004).
    ///
    /// The command rides a [`COMMAND_TYPE_META_COMMAND`] catalog envelope.
    /// When the apply path refused the command, the typed
    /// [`MetaRejectionReason`] is returned (and journaled in state); the
    /// raft entry itself committed normally.
    pub async fn propose(
        &self,
        command_id: [u8; 16],
        command: MetaCommand,
        control: &ExecutionControl,
    ) -> Result<MetaCommandReceipt, MetaError> {
        let payload = MetaCommandRecord::new(command).encode()?;
        let envelope = CommandEnvelope::new(COMMAND_TYPE_META_COMMAND, command_id, payload);
        let receipt = self
            .group
            .propose(CommandKind::Catalog, envelope, control)
            .await?;
        // client_write returns after local apply, so the local sink's view
        // already includes this command (or its refusal).
        let (metadata_version, rejection) = {
            let sink = self
                .sink
                .lock()
                .map_err(|_| MetaError::InvalidRequest("meta sink lock poisoned".to_owned()))?;
            let rejection = sink
                .state()
                .rejections()
                .iter()
                .rev()
                .find(|entry| entry.command_id == Some(command_id))
                .map(|entry| entry.reason.clone());
            (sink.metadata_version(), rejection)
        };
        if let Some(reason) = rejection {
            return Err(MetaError::Rejected(reason));
        }
        Ok(MetaCommandReceipt {
            receipt,
            metadata_version,
        })
    }

    /// A point-in-time clone of the replicated meta state at this node's
    /// applied watermark. For a linearizable view, run
    /// [`ConsensusGroup::read_index`] (via [`MetaGroup::group`]) first.
    pub fn state(&self) -> MetaState {
        self.sink
            .lock()
            .expect("meta sink lock poisoned")
            .state()
            .clone()
    }

    /// Allocates `count` fresh per-group raft node ids from the meta-owned
    /// allocator (spec section 12.1), returning them in ascending order.
    /// Tablet replica raft ids come from this allocator — never the ad-hoc
    /// node-id projection the meta group itself uses (see
    /// [`MetaCommand::AllocateRaftNodeIds`]). The allocation is idempotent
    /// under its command id and durable in replicated state, so ids are
    /// never reused across failovers and restarts.
    pub async fn allocate_raft_node_ids(
        &self,
        count: u32,
        control: &ExecutionControl,
    ) -> Result<Vec<RaftNodeId>, MetaError> {
        let command_id = new_command_id()?;
        self.propose(
            command_id,
            MetaCommand::AllocateRaftNodeIds { count },
            control,
        )
        .await?;
        let state = self.state();
        let base = state.raft_id_allocation(&command_id).ok_or_else(|| {
            MetaError::InvalidRequest(
                "raft node id allocation record missing after commit".to_owned(),
            )
        })?;
        Ok((base..base + u64::from(count)).collect())
    }

    /// The local applied watermark (monotonic per applied command).
    pub fn metadata_version(&self) -> MetadataVersion {
        self.sink
            .lock()
            .expect("meta sink lock poisoned")
            .metadata_version()
    }

    /// Graceful shutdown of the underlying group.
    pub async fn shutdown(&self) -> Result<(), MetaError> {
        self.group.shutdown().await.map_err(MetaError::Consensus)
    }

    /// Process-free crash simulation: stops the raft task without the
    /// graceful storage close (see [`ConsensusGroup::crash`]); everything
    /// fsynced survives, which is exactly the split/merge crash-resume
    /// contract.
    pub async fn crash(self) {
        self.group.crash().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{BuildVersion, Locality, NodeCapacity, NodeState};

    fn node_id(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 16])
    }

    fn descriptor(byte: u8, features: &[&str]) -> NodeDescriptor {
        let mut version_info = VersionInfo::current();
        version_info.feature_set = features.iter().map(|feature| feature.to_string()).collect();
        NodeDescriptor {
            node_id: node_id(byte),
            rpc_address: format!("127.0.0.1:{}", 7000 + u16::from(byte)),
            locality: Locality::default(),
            capacity: NodeCapacity::default(),
            state: NodeState::Up,
            version: BuildVersion::current(),
            version_info,
        }
    }

    fn registry_with(feature: &str, level: u64) -> FeatureRegistry {
        let mut registry = FeatureRegistry::current();
        registry.declare(feature, ClusterFeatureLevel(level));
        registry
    }

    fn activation(feature: &str, level: u64) -> FeatureActivation {
        FeatureActivation {
            feature: feature.to_owned(),
            level: ClusterFeatureLevel(level),
            activated_at: HlcTimestamp::ZERO,
            activated_by: node_id(1),
        }
    }

    #[test]
    fn feature_supported_only_at_or_above_registered_level() {
        let registry = registry_with("ann-v2", 7);
        assert!(!registry.feature_supported(ClusterFeatureLevel(6), "ann-v2"));
        assert!(registry.feature_supported(ClusterFeatureLevel(7), "ann-v2"));
        assert!(registry.feature_supported(ClusterFeatureLevel(8), "ann-v2"));
        // Unknown features are never supported (fail closed).
        assert!(!registry.feature_supported(ClusterFeatureLevel(u64::MAX), "nope"));
    }

    #[test]
    fn activation_refused_until_every_voter_supports_the_feature() {
        let registry = registry_with("ann-v2", 7);
        let voters = vec![
            descriptor(1, &["ann-v2"]),
            descriptor(2, &[]),
            descriptor(3, &["ann-v2"]),
        ];
        let error = activation("ann-v2", 7)
            .validate(&registry, ClusterFeatureLevel::ZERO, &voters)
            .unwrap_err();
        assert_eq!(
            error,
            FeatureActivationError::UnsupportedByVoter {
                feature: "ann-v2".to_owned(),
                node: node_id(2),
            }
        );
        // Once the last voter advertises support, activation validates.
        let voters = vec![
            descriptor(1, &["ann-v2"]),
            descriptor(2, &["ann-v2"]),
            descriptor(3, &["ann-v2"]),
        ];
        activation("ann-v2", 7)
            .validate(&registry, ClusterFeatureLevel::ZERO, &voters)
            .unwrap();
    }

    #[test]
    fn activation_rejects_unknown_features() {
        let registry = FeatureRegistry::current();
        let voters = vec![descriptor(1, &["ann-v2"])];
        let error = activation("ann-v2", 7)
            .validate(&registry, ClusterFeatureLevel::ZERO, &voters)
            .unwrap_err();
        assert_eq!(
            error,
            FeatureActivationError::UnknownFeature {
                feature: "ann-v2".to_owned(),
            }
        );
    }

    #[test]
    fn activation_rejects_a_level_below_the_registered_minimum() {
        let registry = registry_with("ann-v2", 7);
        let voters = vec![descriptor(1, &["ann-v2"])];
        let error = activation("ann-v2", 6)
            .validate(&registry, ClusterFeatureLevel::ZERO, &voters)
            .unwrap_err();
        assert_eq!(
            error,
            FeatureActivationError::LevelBelowRequirement {
                feature: "ann-v2".to_owned(),
                required: ClusterFeatureLevel(7),
                attempted: ClusterFeatureLevel(6),
            }
        );
    }

    #[test]
    fn feature_level_never_regresses() {
        let registry = registry_with("ann-v2", 7);
        let voters = vec![descriptor(1, &["ann-v2"])];
        let error = activation("ann-v2", 7)
            .validate(&registry, ClusterFeatureLevel(9), &voters)
            .unwrap_err();
        assert_eq!(
            error,
            FeatureActivationError::LevelRegression {
                current: ClusterFeatureLevel(9),
                attempted: ClusterFeatureLevel(7),
            }
        );
        // A second feature registered at the cluster's current level may
        // still activate: the level does not lower.
        let registry = registry_with("ai-hybrid", 9);
        let voters = vec![descriptor(1, &["ai-hybrid"])];
        activation("ai-hybrid", 9)
            .validate(&registry, ClusterFeatureLevel(9), &voters)
            .unwrap();
    }

    #[test]
    fn activation_requires_at_least_one_voter() {
        let registry = registry_with("ann-v2", 7);
        let error = activation("ann-v2", 7)
            .validate(&registry, ClusterFeatureLevel::ZERO, &[])
            .unwrap_err();
        assert_eq!(error, FeatureActivationError::NoVoters);
    }

    #[test]
    fn activation_record_round_trips_serde() {
        let record = activation("ann-v2", 7);
        let json = serde_json::to_vec(&record).unwrap();
        let back: FeatureActivation = serde_json::from_slice(&json).unwrap();
        assert_eq!(back, record);
    }

    #[test]
    fn upgrade_plan_upgrades_followers_first_and_the_leader_last() {
        let target = VersionInfo::current();
        let nodes = vec![descriptor(1, &[]), descriptor(2, &[]), descriptor(3, &[])];
        let plan = plan_rolling_upgrade(&nodes, node_id(1), &target).unwrap();
        assert_eq!(plan.target, target);
        assert_eq!(
            plan.steps,
            vec![
                UpgradeStep::UpgradeFollower {
                    node_id: node_id(2)
                },
                UpgradeStep::UpgradeFollower {
                    node_id: node_id(3)
                },
                UpgradeStep::TransferLeadership { from: node_id(1) },
                UpgradeStep::UpgradeFormerLeader {
                    node_id: node_id(1)
                },
                UpgradeStep::EnableNewFeatures,
            ]
        );
    }

    #[test]
    fn upgrade_plan_for_a_single_node_skips_leadership_transfer() {
        let target = VersionInfo::current();
        let nodes = vec![descriptor(1, &[])];
        let plan = plan_rolling_upgrade(&nodes, node_id(1), &target).unwrap();
        assert_eq!(
            plan.steps,
            vec![
                UpgradeStep::UpgradeFormerLeader {
                    node_id: node_id(1)
                },
                UpgradeStep::EnableNewFeatures,
            ]
        );
    }

    #[test]
    fn upgrade_plan_verifies_compatibility_first() {
        let mut target = VersionInfo::current();
        target.protocol_min = target.protocol_max + 1;
        let nodes = vec![descriptor(1, &[]), descriptor(2, &[])];
        let error = plan_rolling_upgrade(&nodes, node_id(1), &target).unwrap_err();
        assert!(matches!(
            error,
            UpgradePlanError::IncompatibleNode {
                node,
                incompatibility: Incompatibility::ProtocolVersion { .. },
            } if node == node_id(1)
        ));
    }

    #[test]
    fn upgrade_plan_rejects_malformed_membership() {
        let target = VersionInfo::current();
        assert_eq!(
            plan_rolling_upgrade(&[], node_id(1), &target).unwrap_err(),
            UpgradePlanError::EmptyMembership,
        );
        let nodes = vec![descriptor(1, &[])];
        assert_eq!(
            plan_rolling_upgrade(&nodes, node_id(9), &target).unwrap_err(),
            UpgradePlanError::LeaderNotInMembership { leader: node_id(9) },
        );
        let nodes = vec![descriptor(1, &[]), descriptor(1, &[])];
        assert_eq!(
            plan_rolling_upgrade(&nodes, node_id(1), &target).unwrap_err(),
            UpgradePlanError::DuplicateNode { node: node_id(1) },
        );
    }

    #[test]
    fn rollback_is_a_binary_downgrade_until_the_first_feature_activates() {
        let before_activation = assess_rollback(&[]);
        assert_eq!(before_activation.path, RollbackPath::BinaryDowngrade);
        assert!(before_activation.activated_features.is_empty());

        let after_activation = assess_rollback(&[activation("ann-v2", 7)]);
        assert_eq!(after_activation.path, RollbackPath::RestoreFromBackup);
        assert_eq!(
            after_activation.activated_features,
            vec!["ann-v2".to_owned()]
        );
    }
}

// ---------------------------------------------------------------------------
// Stage 3A tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod stage3a_tests {
    use super::*;
    use crate::node::{BuildVersion, Locality, NodeCapacity};
    use mongreldb_consensus::network::InMemoryTransport;
    use mongreldb_log::commit_log::LogPosition;
    use std::path::Path;
    use std::time::{Duration, Instant};

    const LEADER_TIMEOUT: Duration = Duration::from_secs(10);
    const META_GID: RaftGroupId = RaftGroupId::from_bytes([0xAA; 16]);

    fn node_id(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 16])
    }

    fn raft_id(byte: u8) -> RaftNodeId {
        raft_node_id(&node_id(byte))
    }

    fn group_id(byte: u8) -> RaftGroupId {
        RaftGroupId::from_bytes([byte; 16])
    }

    fn ts(micros: u64) -> HlcTimestamp {
        HlcTimestamp {
            physical_micros: micros,
            logical: 0,
            node_tiebreaker: 0,
        }
    }

    fn cmd_id(byte: u8) -> [u8; 16] {
        [byte; 16]
    }

    fn descriptor(byte: u8, features: &[&str]) -> NodeDescriptor {
        let mut version_info = VersionInfo::current();
        version_info.feature_set = features.iter().map(|feature| feature.to_string()).collect();
        NodeDescriptor {
            node_id: node_id(byte),
            rpc_address: format!("127.0.0.1:{}", 7100 + u16::from(byte)),
            locality: Locality::default(),
            capacity: NodeCapacity::default(),
            state: NodeState::Up,
            version: BuildVersion::current(),
            version_info,
        }
    }

    fn registry_with(feature: &str, level: u64) -> FeatureRegistry {
        let mut registry = FeatureRegistry::current();
        registry.declare(feature, ClusterFeatureLevel(level));
        registry
    }

    fn database(byte: u8, name: &str) -> DatabaseDescriptor {
        DatabaseDescriptor {
            database_id: DatabaseId::from_bytes([byte; 16]),
            name: name.to_owned(),
            created_at: ts(1_000),
            state: DatabaseState::Online,
            metadata_version: MetadataVersion::ZERO,
        }
    }

    fn schema_record(table: u64, database_byte: u8, version: u64) -> TableSchemaRecord {
        TableSchemaRecord {
            table_id: TableId(table),
            database_id: DatabaseId::from_bytes([database_byte; 16]),
            schema_version: SchemaVersion(version),
            schema: serde_json::json!({"columns": [{"name": "pk", "type": "u64"}]}),
            metadata_version: MetadataVersion::ZERO,
        }
    }

    fn tablet(byte: u8, table: u64, generation: u64) -> TabletDescriptor {
        TabletDescriptor {
            tablet_id: TabletId::from_bytes([byte; 16]),
            table_id: TableId(table),
            raft_group_id: group_id(9),
            partition: PartitionBounds {
                low: Bound::Unbounded,
                high: Bound::Excluded(Key::from_bytes(b"m".to_vec())),
            },
            replicas: vec![ReplicaDescriptor {
                node_id: node_id(1),
                role: ReplicaRole::Voter,
                raft_node_id: raft_id(1),
            }],
            leader_hint: None,
            generation,
            state: TabletState::Active,
        }
    }

    fn placement(byte: u8, members: &[(u8, ReplicaRole)]) -> ReplicaPlacement {
        ReplicaPlacement {
            raft_group_id: group_id(byte),
            replicas: members
                .iter()
                .map(|(node, role)| ReplicaDescriptor {
                    node_id: node_id(*node),
                    role: *role,
                    raft_node_id: raft_id(*node),
                })
                .collect(),
            metadata_version: MetadataVersion::ZERO,
        }
    }

    fn policy(replicas: u8) -> PlacementPolicy {
        PlacementPolicy {
            replicas,
            voter_constraints: vec![LocalityConstraint {
                key: "region".to_owned(),
                value: "us-central".to_owned(),
                required: true,
            }],
            leader_preferences: Vec::new(),
            prohibited_nodes: Vec::new(),
        }
    }

    fn schema_job(job_id: u64, table: u64, database_byte: u8) -> SchemaJobRecord {
        SchemaJobRecord {
            job_id,
            database_id: DatabaseId::from_bytes([database_byte; 16]),
            table_id: TableId(table),
            kind: SchemaJobKind::IndexBuild,
            state: SchemaJobState::Pending,
            submitted_at: ts(1_000),
            updated_at: ts(1_000),
            error: None,
            metadata_version: MetadataVersion::ZERO,
        }
    }

    fn activation(feature: &str, level: u64) -> FeatureActivation {
        FeatureActivation {
            feature: feature.to_owned(),
            level: ClusterFeatureLevel(level),
            activated_at: HlcTimestamp::ZERO,
            activated_by: node_id(1),
        }
    }

    fn apply(
        state: &mut MetaState,
        registry: &FeatureRegistry,
        id: u8,
        command: MetaCommand,
    ) -> Result<(), MetaRejectionReason> {
        state.apply(&command, Some(cmd_id(id)), ts(1_000), registry)
    }

    fn fast_group_config(config: &MetaGroupConfig) -> GroupConfig {
        let mut group = config.group_config();
        group.heartbeat_interval = Duration::from_millis(50);
        group.election_timeout_min = Duration::from_millis(150);
        group.election_timeout_max = Duration::from_millis(300);
        group.install_snapshot_timeout = Duration::from_millis(1_000);
        group
    }

    fn meta_config(dir: &Path, node: u8, registry: FeatureRegistry) -> MetaGroupConfig {
        let mut config = MetaGroupConfig::new(dir.to_path_buf(), META_GID, node_id(node));
        config.registry = registry;
        config
    }

    /// Waits until every group in `among` agrees on one leader **that is one
    /// of `among`** — a stopped leader's id lingers in the survivors' metrics
    /// until the next election, so a bare consensus check can otherwise
    /// return a node outside the live set.
    async fn wait_consensus_leader(among: &[&MetaGroup<InMemoryTransport>]) -> RaftNodeId {
        let allowed: BTreeSet<RaftNodeId> = among
            .iter()
            .map(|group| raft_node_id(&group.node_id()))
            .collect();
        let deadline = Instant::now() + LEADER_TIMEOUT;
        loop {
            let mut leaders = BTreeSet::new();
            let mut seen = 0_usize;
            for group in among {
                if let Some(leader) = group.group().metrics().current_leader {
                    leaders.insert(leader);
                    seen += 1;
                }
            }
            if seen == among.len() && leaders.len() == 1 {
                let leader = *leaders.iter().next().expect("one leader");
                if allowed.contains(&leader) {
                    return leader;
                }
            }
            assert!(
                Instant::now() < deadline,
                "no consensus leader (saw {leaders:?})"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    // -- serde + record plumbing -------------------------------------------

    #[test]
    fn meta_command_record_round_trips_every_variant() {
        let commands = vec![
            MetaCommand::RegisterNode {
                descriptor: descriptor(1, &["ann-v2"]),
            },
            MetaCommand::UpdateNodeState {
                node_id: node_id(1),
                state: NodeState::Draining,
                expected_version: Some(MetadataVersion(3)),
            },
            MetaCommand::RemoveNode {
                node_id: node_id(1),
            },
            MetaCommand::CreateDatabase {
                descriptor: database(1, "app"),
            },
            MetaCommand::DropDatabase {
                database_id: DatabaseId::from_bytes([1; 16]),
            },
            MetaCommand::SetTableSchema {
                record: schema_record(1, 1, 1),
            },
            MetaCommand::SetTabletDescriptor {
                descriptor: tablet(1, 1, 1),
            },
            MetaCommand::RemoveTabletDescriptor {
                tablet_id: TabletId::from_bytes([1; 16]),
                generation: 1,
            },
            MetaCommand::SetReplicaPlacement {
                placement: placement(9, &[(1, ReplicaRole::Voter)]),
            },
            MetaCommand::SetPlacementPolicy {
                name: "default".to_owned(),
                policy: policy(3),
            },
            MetaCommand::SubmitSchemaJob {
                job: schema_job(7, 1, 1),
            },
            MetaCommand::UpdateSchemaJob {
                job_id: 7,
                state: SchemaJobState::Running,
                updated_at: ts(2_000),
                error: None,
                expected_version: None,
            },
            MetaCommand::SetClusterSetting {
                key: "jobs.max_concurrent".to_owned(),
                value: serde_json::json!(4),
            },
            MetaCommand::ActivateFeature {
                activation: activation("ann-v2", 7),
            },
            MetaCommand::SetTxnStatusPartition {
                partition: TxnStatusPartition {
                    partition_id: 0,
                    home_raft_group: group_id(9),
                },
            },
            MetaCommand::PublishSplit {
                command: split_publish_command(),
            },
            MetaCommand::PublishMerge {
                command: merge_publish_command(),
            },
            MetaCommand::AllocateRaftNodeIds { count: 3 },
        ];
        for command in commands {
            let record = MetaCommandRecord::new(command);
            let bytes = record.encode().unwrap();
            assert_eq!(MetaCommandRecord::decode(&bytes).unwrap(), record);
        }
        // Malformed payloads and unsupported versions fail closed.
        assert!(matches!(
            MetaCommandRecord::decode(b"not json"),
            Err(MetaDecodeError::Malformed(_))
        ));
        let future = MetaCommandRecord {
            format_version: META_COMMAND_FORMAT_VERSION + 1,
            command: MetaCommand::RemoveNode {
                node_id: node_id(1),
            },
        };
        assert_eq!(
            MetaCommandRecord::decode(&future.encode().unwrap()).unwrap_err(),
            MetaDecodeError::UnsupportedVersion {
                found: META_COMMAND_FORMAT_VERSION + 1,
                min: MIN_SUPPORTED_META_COMMAND_FORMAT_VERSION,
                max: META_COMMAND_FORMAT_VERSION,
            }
        );
    }

    // -- single-node apply round-trips (every command) ----------------------

    fn every_command_sequence() -> Vec<MetaCommand> {
        vec![
            MetaCommand::RegisterNode {
                descriptor: descriptor(1, &["ann-v2"]),
            },
            MetaCommand::UpdateNodeState {
                node_id: node_id(1),
                state: NodeState::Draining,
                expected_version: None,
            },
            MetaCommand::CreateDatabase {
                descriptor: database(1, "app"),
            },
            MetaCommand::SetTableSchema {
                record: schema_record(1, 1, 1),
            },
            MetaCommand::SetTabletDescriptor {
                descriptor: tablet(1, 1, 1),
            },
            MetaCommand::RemoveTabletDescriptor {
                tablet_id: TabletId::from_bytes([1; 16]),
                generation: 1,
            },
            MetaCommand::SetReplicaPlacement {
                placement: placement(9, &[(1, ReplicaRole::Voter)]),
            },
            MetaCommand::SetPlacementPolicy {
                name: "default".to_owned(),
                policy: policy(3),
            },
            MetaCommand::SubmitSchemaJob {
                job: schema_job(7, 1, 1),
            },
            MetaCommand::UpdateSchemaJob {
                job_id: 7,
                state: SchemaJobState::Running,
                updated_at: ts(2_000),
                error: None,
                expected_version: None,
            },
            MetaCommand::SetClusterSetting {
                key: "jobs.max_concurrent".to_owned(),
                value: serde_json::json!(4),
            },
            MetaCommand::ActivateFeature {
                activation: activation("ann-v2", 7),
            },
            MetaCommand::SetTxnStatusPartition {
                partition: TxnStatusPartition {
                    partition_id: 0,
                    home_raft_group: group_id(9),
                },
            },
            MetaCommand::RemoveNode {
                node_id: node_id(99),
            },
            MetaCommand::DropDatabase {
                database_id: DatabaseId::from_bytes([0xEE; 16]),
            },
            MetaCommand::AllocateRaftNodeIds { count: 3 },
        ]
    }

    #[test]
    fn apply_round_trips_every_command() {
        let registry = registry_with("ann-v2", 7);
        let mut state = MetaState::default();
        for (index, command) in every_command_sequence().into_iter().enumerate() {
            let id = u8::try_from(index + 1).unwrap();
            apply(&mut state, &registry, id, command).unwrap();
        }
        assert_eq!(state.metadata_version, MetadataVersion(16));
        assert!(state.rejections().is_empty());
        // The allocator command appended to the sequence handed out three ids.
        assert_eq!(state.next_raft_node_id(), FIRST_RAFT_NODE_ID + 3);

        let node = state.node_record(node_id(1)).unwrap();
        assert_eq!(node.descriptor.state, NodeState::Draining);
        assert_eq!(node.metadata_version, MetadataVersion(2));
        assert_eq!(state.database_by_name("app").unwrap().name, "app");
        assert_eq!(
            state.table(TableId(1)).unwrap().schema_version,
            SchemaVersion(1)
        );
        assert!(state.tablets.is_empty());
        assert_eq!(state.placement(group_id(9)).unwrap().replicas.len(), 1);
        assert_eq!(state.placement_policy("default").unwrap().replicas, 3);
        assert_eq!(state.schema_job(7).unwrap().state, SchemaJobState::Running);
        assert_eq!(state.settings().max_concurrent_jobs, 4);
        assert_eq!(state.feature_level(), ClusterFeatureLevel(7));
        assert!(state.feature_active(&registry, "ann-v2"));
        // A ZERO activated_at is stamped with the entry's commit timestamp.
        assert_eq!(state.feature_activations()[0].activated_at, ts(1_000));
        assert_eq!(
            state.txn_status_partition(0).unwrap().home_raft_group,
            group_id(9)
        );
    }

    // -- idempotent, deterministic apply ------------------------------------

    #[test]
    fn apply_is_idempotent_for_record_replays() {
        let registry = registry_with("ann-v2", 7);
        let mut state = MetaState::default();
        apply(
            &mut state,
            &registry,
            1,
            MetaCommand::RegisterNode {
                descriptor: descriptor(1, &["ann-v2"]),
            },
        )
        .unwrap();
        apply(
            &mut state,
            &registry,
            2,
            MetaCommand::RegisterNode {
                descriptor: descriptor(1, &["ann-v2"]),
            },
        )
        .unwrap();
        assert_eq!(state.nodes.len(), 1);
        // The replay was a record-level no-op: the record keeps the version
        // of the first write while the state watermark ticks per command.
        assert_eq!(
            state.node_record(node_id(1)).unwrap().metadata_version,
            MetadataVersion(1)
        );
        assert_eq!(state.metadata_version, MetadataVersion(2));

        apply(
            &mut state,
            &registry,
            3,
            MetaCommand::ActivateFeature {
                activation: activation("ann-v2", 7),
            },
        )
        .unwrap();
        apply(
            &mut state,
            &registry,
            4,
            MetaCommand::ActivateFeature {
                activation: activation("ann-v2", 7),
            },
        )
        .unwrap();
        assert_eq!(state.feature_activations().len(), 1);
        assert_eq!(state.feature_level(), ClusterFeatureLevel(7));
    }

    #[test]
    fn apply_is_deterministic_across_states() {
        let registry = registry_with("ann-v2", 7);
        let commands = every_command_sequence();
        let mut a = MetaState::default();
        let mut b = MetaState::default();
        for (index, command) in commands.iter().enumerate() {
            let id = u8::try_from(index + 1).unwrap();
            a.apply(command, Some(cmd_id(id)), ts(1_000), &registry)
                .unwrap();
            b.apply(command, Some(cmd_id(id)), ts(1_000), &registry)
                .unwrap();
        }
        assert_eq!(a, b);
        // Snapshots of identical states are byte-identical.
        assert_eq!(
            serde_json::to_vec(&a).unwrap(),
            serde_json::to_vec(&b).unwrap()
        );
    }

    // -- split/merge publish adoption (spec sections 12.5-12.6) --------------

    fn key(bytes: &[u8]) -> Key {
        Key::from_bytes(bytes.to_vec())
    }

    fn voter_on(node: u8, raft: u64) -> ReplicaDescriptor {
        ReplicaDescriptor {
            node_id: node_id(node),
            role: ReplicaRole::Voter,
            raft_node_id: raft,
        }
    }

    /// The pre-split source: table 1, [a, z), `Active` at generation 5.
    fn split_source() -> TabletDescriptor {
        TabletDescriptor {
            tablet_id: TabletId::from_bytes([0x51; 16]),
            table_id: TableId(1),
            raft_group_id: group_id(0x51),
            partition: PartitionBounds::new(Bound::Included(key(b"a")), Bound::Excluded(key(b"z")))
                .unwrap(),
            replicas: vec![voter_on(1, 101), voter_on(2, 102)],
            leader_hint: None,
            generation: 5,
            state: TabletState::Active,
        }
    }

    fn split_plan() -> crate::split::SplitPlan {
        crate::split::TabletSplitPlanner::new("/unused")
            .plan(
                &split_source(),
                crate::split::SplitKeySelection::Explicit(key(b"m")),
                ts(150),
                [
                    crate::split::ChildAllocation {
                        tablet_id: TabletId::from_bytes([0x52; 16]),
                        raft_group_id: group_id(0x52),
                        replicas: vec![voter_on(1, 201), voter_on(2, 202)],
                    },
                    crate::split::ChildAllocation {
                        tablet_id: TabletId::from_bytes([0x53; 16]),
                        raft_group_id: group_id(0x53),
                        replicas: vec![voter_on(1, 301), voter_on(2, 302)],
                    },
                ],
            )
            .unwrap()
    }

    fn split_publish_command() -> SplitPublishCommand {
        SplitPublishCommand::from_plan(&split_plan()).unwrap()
    }

    /// Seeds a state with the database/table, the `Splitting` source, and
    /// the `Creating` children of [`split_plan`], returning the plan.
    fn seed_split_publish(
        state: &mut MetaState,
        registry: &FeatureRegistry,
    ) -> crate::split::SplitPlan {
        apply(
            state,
            registry,
            1,
            MetaCommand::CreateDatabase {
                descriptor: database(1, "app"),
            },
        )
        .unwrap();
        apply(
            state,
            registry,
            2,
            MetaCommand::SetTableSchema {
                record: schema_record(1, 1, 1),
            },
        )
        .unwrap();
        let plan = split_plan();
        apply(
            state,
            registry,
            3,
            MetaCommand::SetTabletDescriptor {
                descriptor: plan.source.clone(),
            },
        )
        .unwrap();
        let marked = plan
            .source
            .published_transition(TabletState::Splitting)
            .unwrap();
        apply(
            state,
            registry,
            4,
            MetaCommand::SetTabletDescriptor { descriptor: marked },
        )
        .unwrap();
        for (index, child) in plan.child_descriptors().into_iter().enumerate() {
            apply(
                state,
                registry,
                5 + u8::try_from(index).unwrap(),
                MetaCommand::SetTabletDescriptor { descriptor: child },
            )
            .unwrap();
        }
        plan
    }

    /// The merge sources: table 1, adjacent [a, m) at generation 4 and
    /// [m, z) at generation 6, placed on the same two nodes.
    fn merge_sources() -> [TabletDescriptor; 2] {
        let source =
            |byte: u8, low: Bound<Key>, high: Bound<Key>, generation: u64, raft_base: u64| {
                TabletDescriptor {
                    tablet_id: TabletId::from_bytes([byte; 16]),
                    table_id: TableId(1),
                    raft_group_id: group_id(byte),
                    partition: PartitionBounds::new(low, high).unwrap(),
                    replicas: vec![voter_on(1, raft_base), voter_on(2, raft_base + 1)],
                    leader_hint: None,
                    generation,
                    state: TabletState::Active,
                }
            };
        [
            source(
                0x61,
                Bound::Included(key(b"a")),
                Bound::Excluded(key(b"m")),
                4,
                101,
            ),
            source(
                0x62,
                Bound::Included(key(b"m")),
                Bound::Excluded(key(b"z")),
                6,
                201,
            ),
        ]
    }

    fn merge_plan() -> crate::merge::MergePlan {
        let [first, second] = merge_sources();
        crate::merge::MergePlanner::new("/unused")
            .plan(
                crate::merge::MergeInputs {
                    first,
                    second,
                    first_schema: SchemaVersion(1),
                    second_schema: SchemaVersion(1),
                    active_schema_job: None,
                    first_size_bytes: 1_000,
                    second_size_bytes: 2_000,
                    max_merged_size_bytes: 1_000_000,
                },
                ts(150),
                crate::split::ChildAllocation {
                    tablet_id: TabletId::from_bytes([0x63; 16]),
                    raft_group_id: group_id(0x63),
                    replicas: vec![voter_on(1, 301), voter_on(2, 302)],
                },
            )
            .unwrap()
    }

    fn merge_publish_command() -> MergePublishCommand {
        MergePublishCommand::from_plan(&merge_plan()).unwrap()
    }

    /// Seeds a state with the database/table, the `Merging` sources, and the
    /// `Creating` replacement of [`merge_plan`], returning the plan.
    fn seed_merge_publish(
        state: &mut MetaState,
        registry: &FeatureRegistry,
    ) -> crate::merge::MergePlan {
        apply(
            state,
            registry,
            1,
            MetaCommand::CreateDatabase {
                descriptor: database(1, "app"),
            },
        )
        .unwrap();
        apply(
            state,
            registry,
            2,
            MetaCommand::SetTableSchema {
                record: schema_record(1, 1, 1),
            },
        )
        .unwrap();
        let plan = merge_plan();
        for (index, source) in plan.sources.iter().enumerate() {
            let id = 3 + 2 * u8::try_from(index).unwrap();
            apply(
                state,
                registry,
                id,
                MetaCommand::SetTabletDescriptor {
                    descriptor: source.clone(),
                },
            )
            .unwrap();
            let marked = source.published_transition(TabletState::Merging).unwrap();
            apply(
                state,
                registry,
                id + 1,
                MetaCommand::SetTabletDescriptor { descriptor: marked },
            )
            .unwrap();
        }
        apply(
            state,
            registry,
            7,
            MetaCommand::SetTabletDescriptor {
                descriptor: plan.replacement_descriptor(),
            },
        )
        .unwrap();
        plan
    }

    #[test]
    fn publish_split_applies_atomically_and_replays_idempotently() {
        let registry = registry_with("ann-v2", 7);
        let mut state = MetaState::default();
        let plan = seed_split_publish(&mut state, &registry);
        let command = SplitPublishCommand::from_plan(&plan).unwrap();
        apply(
            &mut state,
            &registry,
            10,
            MetaCommand::PublishSplit {
                command: command.clone(),
            },
        )
        .unwrap();
        // One atomic publication: children Active, source Retiring, all at
        // the publication generation (g + 2 = 7), learners promoted.
        let source = state.tablet(plan.source.tablet_id).unwrap().clone();
        assert_eq!(source.state, TabletState::Retiring);
        assert_eq!(source.generation, 7);
        for child in &command.children {
            let stored = state.tablet(child.tablet_id).unwrap();
            assert_eq!(stored, child);
            assert_eq!(stored.state, TabletState::Active);
            assert_eq!(stored.generation, 7);
            assert!(stored.replicas.iter().all(|r| r.role == ReplicaRole::Voter));
        }
        assert!(state.rejections().is_empty());
        // Idempotent replay (a resumed split re-publishes after a crash in
        // the barrier): a different command id carrying the identical
        // publication is a no-op.
        let version = state.metadata_version;
        apply(
            &mut state,
            &registry,
            11,
            MetaCommand::PublishSplit { command },
        )
        .unwrap();
        assert!(state.rejections().is_empty());
        assert_eq!(state.tablet(plan.source.tablet_id).unwrap(), &source);
        assert_eq!(
            state
                .tablet_record(plan.source.tablet_id)
                .unwrap()
                .metadata_version,
            version
        );
    }

    #[test]
    fn publish_split_rejection_matrix() {
        let registry = registry_with("ann-v2", 7);

        // The source was never marked Splitting (only the database, table,
        // and Active source are seeded).
        let mut state = MetaState::default();
        apply(
            &mut state,
            &registry,
            1,
            MetaCommand::CreateDatabase {
                descriptor: database(1, "app"),
            },
        )
        .unwrap();
        apply(
            &mut state,
            &registry,
            2,
            MetaCommand::SetTableSchema {
                record: schema_record(1, 1, 1),
            },
        )
        .unwrap();
        apply(
            &mut state,
            &registry,
            3,
            MetaCommand::SetTabletDescriptor {
                descriptor: split_source(),
            },
        )
        .unwrap();
        let error = apply(
            &mut state,
            &registry,
            21,
            MetaCommand::PublishSplit {
                command: split_publish_command(),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Conflict { .. }));
        assert_eq!(state.rejections().len(), 1);
        assert_eq!(state.rejections()[0].command_id, Some(cmd_id(21)));

        // A child descriptor is missing.
        let mut state = MetaState::default();
        let plan = seed_split_publish(&mut state, &registry);
        apply(
            &mut state,
            &registry,
            20,
            MetaCommand::RemoveTabletDescriptor {
                tablet_id: plan.child_descriptors()[1].tablet_id,
                generation: 6,
            },
        )
        .unwrap();
        let error = apply(
            &mut state,
            &registry,
            21,
            MetaCommand::PublishSplit {
                command: split_publish_command(),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::NotFound { .. }));

        // A child is not in Creating.
        let mut state = MetaState::default();
        let plan = seed_split_publish(&mut state, &registry);
        let mut rogue = plan.child_descriptors()[0].clone();
        rogue.state = TabletState::Active;
        rogue.generation = 7;
        for replica in &mut rogue.replicas {
            replica.role = ReplicaRole::Voter;
        }
        apply(
            &mut state,
            &registry,
            20,
            MetaCommand::SetTabletDescriptor { descriptor: rogue },
        )
        .unwrap();
        let error = apply(
            &mut state,
            &registry,
            21,
            MetaCommand::PublishSplit {
                command: split_publish_command(),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Conflict { .. }));

        // The publication generation does not follow the stored precursors.
        let mut state = MetaState::default();
        seed_split_publish(&mut state, &registry);
        let mut command = split_publish_command();
        command.source.generation += 1;
        for child in &mut command.children {
            child.generation += 1;
        }
        let error = apply(
            &mut state,
            &registry,
            21,
            MetaCommand::PublishSplit { command },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Conflict { .. }));

        // The child bounds do not partition the source at the split key.
        let mut state = MetaState::default();
        seed_split_publish(&mut state, &registry);
        let mut command = split_publish_command();
        command.split_key = key(b"n");
        let error = apply(
            &mut state,
            &registry,
            21,
            MetaCommand::PublishSplit { command },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Invalid { .. }));

        // Another routable tablet of the table overlaps a child.
        let mut state = MetaState::default();
        seed_split_publish(&mut state, &registry);
        let overlapping = TabletDescriptor {
            tablet_id: TabletId::from_bytes([0x70; 16]),
            table_id: TableId(1),
            raft_group_id: group_id(0x70),
            partition: PartitionBounds::new(Bound::Included(key(b"a")), Bound::Excluded(key(b"b")))
                .unwrap(),
            replicas: vec![voter_on(1, 901)],
            leader_hint: None,
            generation: 1,
            state: TabletState::Active,
        };
        apply(
            &mut state,
            &registry,
            20,
            MetaCommand::SetTabletDescriptor {
                descriptor: overlapping,
            },
        )
        .unwrap();
        let error = apply(
            &mut state,
            &registry,
            21,
            MetaCommand::PublishSplit {
                command: split_publish_command(),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Conflict { .. }));

        // The table does not exist.
        let mut state = MetaState::default();
        let error = apply(
            &mut state,
            &registry,
            21,
            MetaCommand::PublishSplit {
                command: split_publish_command(),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::NotFound { .. }));
    }

    #[test]
    fn publish_merge_applies_atomically_with_lagging_generations_and_replays() {
        let registry = registry_with("ann-v2", 7);
        let mut state = MetaState::default();
        let plan = seed_merge_publish(&mut state, &registry);
        let command = MergePublishCommand::from_plan(&plan).unwrap();
        apply(
            &mut state,
            &registry,
            10,
            MetaCommand::PublishMerge {
                command: command.clone(),
            },
        )
        .unwrap();
        // The command-wide generation is max(g1, g2) + 2 = 8: the source
        // marked at generation 5 jumps to 8 with the rest.
        let replacement = state.tablet(command.replacement.tablet_id).unwrap();
        assert_eq!(replacement.state, TabletState::Active);
        assert_eq!(replacement.generation, 8);
        for source in &command.sources {
            let stored = state.tablet(source.tablet_id).unwrap();
            assert_eq!(stored.state, TabletState::Retiring);
            assert_eq!(stored.generation, 8);
        }
        assert!(state.rejections().is_empty());
        // Idempotent replay.
        apply(
            &mut state,
            &registry,
            11,
            MetaCommand::PublishMerge { command },
        )
        .unwrap();
        assert!(state.rejections().is_empty());
    }

    #[test]
    fn publish_merge_rejection_matrix() {
        let registry = registry_with("ann-v2", 7);

        // A source was never marked Merging (it is stored Active, one
        // generation above the mark it never took).
        let mut state = MetaState::default();
        let plan = seed_merge_publish(&mut state, &registry);
        let mut active = plan.sources[0].clone();
        active.generation = 6;
        apply(
            &mut state,
            &registry,
            20,
            MetaCommand::SetTabletDescriptor { descriptor: active },
        )
        .unwrap();
        let error = apply(
            &mut state,
            &registry,
            21,
            MetaCommand::PublishMerge {
                command: merge_publish_command(),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Conflict { .. }));

        // The replacement descriptor is missing.
        let mut state = MetaState::default();
        let plan = seed_merge_publish(&mut state, &registry);
        apply(
            &mut state,
            &registry,
            20,
            MetaCommand::RemoveTabletDescriptor {
                tablet_id: plan.replacement_descriptor().tablet_id,
                generation: 7,
            },
        )
        .unwrap();
        let error = apply(
            &mut state,
            &registry,
            21,
            MetaCommand::PublishMerge {
                command: merge_publish_command(),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::NotFound { .. }));

        // The replacement is not in Creating.
        let mut state = MetaState::default();
        let plan = seed_merge_publish(&mut state, &registry);
        let mut rogue = plan.replacement_descriptor();
        rogue.state = TabletState::Active;
        rogue.generation = 8;
        for replica in &mut rogue.replicas {
            replica.role = ReplicaRole::Voter;
        }
        apply(
            &mut state,
            &registry,
            20,
            MetaCommand::SetTabletDescriptor { descriptor: rogue },
        )
        .unwrap();
        let error = apply(
            &mut state,
            &registry,
            21,
            MetaCommand::PublishMerge {
                command: merge_publish_command(),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Conflict { .. }));

        // The replacement's stored generation is off the generation math
        // (remove the seeded descriptor, re-create it one generation low).
        let mut state = MetaState::default();
        let plan = seed_merge_publish(&mut state, &registry);
        let mut rogue = plan.replacement_descriptor();
        rogue.generation -= 1;
        apply(
            &mut state,
            &registry,
            19,
            MetaCommand::RemoveTabletDescriptor {
                tablet_id: rogue.tablet_id,
                generation: 7,
            },
        )
        .unwrap();
        apply(
            &mut state,
            &registry,
            20,
            MetaCommand::SetTabletDescriptor { descriptor: rogue },
        )
        .unwrap();
        let error = apply(
            &mut state,
            &registry,
            21,
            MetaCommand::PublishMerge {
                command: merge_publish_command(),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Conflict { .. }));

        // The replacement bounds are not the union of the sources.
        let mut state = MetaState::default();
        seed_merge_publish(&mut state, &registry);
        let mut command = merge_publish_command();
        command.replacement.partition = command.sources[0].partition.clone();
        let error = apply(
            &mut state,
            &registry,
            21,
            MetaCommand::PublishMerge { command },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Invalid { .. }));

        // Another routable tablet of the table overlaps the replacement.
        let mut state = MetaState::default();
        seed_merge_publish(&mut state, &registry);
        let overlapping = TabletDescriptor {
            tablet_id: TabletId::from_bytes([0x70; 16]),
            table_id: TableId(1),
            raft_group_id: group_id(0x70),
            partition: PartitionBounds::new(Bound::Included(key(b"b")), Bound::Excluded(key(b"c")))
                .unwrap(),
            replicas: vec![voter_on(1, 901)],
            leader_hint: None,
            generation: 1,
            state: TabletState::Active,
        };
        apply(
            &mut state,
            &registry,
            20,
            MetaCommand::SetTabletDescriptor {
                descriptor: overlapping,
            },
        )
        .unwrap();
        let error = apply(
            &mut state,
            &registry,
            21,
            MetaCommand::PublishMerge {
                command: merge_publish_command(),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Conflict { .. }));
    }

    // -- meta-owned raft-node-id allocation -----------------------------------

    #[test]
    fn allocate_raft_node_ids_is_monotonic_unique_and_replay_safe() {
        let registry = registry_with("ann-v2", 7);
        let mut state = MetaState::default();
        apply(
            &mut state,
            &registry,
            1,
            MetaCommand::AllocateRaftNodeIds { count: 3 },
        )
        .unwrap();
        assert_eq!(
            state.raft_id_allocation(&cmd_id(1)),
            Some(FIRST_RAFT_NODE_ID)
        );
        assert_eq!(state.next_raft_node_id(), FIRST_RAFT_NODE_ID + 3);
        apply(
            &mut state,
            &registry,
            2,
            MetaCommand::AllocateRaftNodeIds { count: 2 },
        )
        .unwrap();
        // The ranges never overlap and the counter only advances.
        assert_eq!(
            state.raft_id_allocation(&cmd_id(2)),
            Some(FIRST_RAFT_NODE_ID + 3)
        );
        assert_eq!(state.next_raft_node_id(), FIRST_RAFT_NODE_ID + 5);
        // Replaying the first command id is a no-op: no double allocation.
        apply(
            &mut state,
            &registry,
            1,
            MetaCommand::AllocateRaftNodeIds { count: 3 },
        )
        .unwrap();
        assert_eq!(state.next_raft_node_id(), FIRST_RAFT_NODE_ID + 5);
        assert_eq!(state.raft_id_allocations.len(), 2);
        // Bounds are enforced and journaled.
        let error = apply(
            &mut state,
            &registry,
            3,
            MetaCommand::AllocateRaftNodeIds { count: 0 },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Invalid { .. }));
        let error = apply(
            &mut state,
            &registry,
            4,
            MetaCommand::AllocateRaftNodeIds {
                count: MAX_RAFT_NODE_ID_ALLOCATION + 1,
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Invalid { .. }));
        assert_eq!(state.rejections().len(), 2);
        assert_eq!(state.next_raft_node_id(), FIRST_RAFT_NODE_ID + 5);
    }

    #[test]
    fn allocator_skips_ids_in_use_and_survives_restarts() {
        let registry = registry_with("ann-v2", 7);
        let mut state = MetaState::default();
        apply(
            &mut state,
            &registry,
            1,
            MetaCommand::CreateDatabase {
                descriptor: database(1, "app"),
            },
        )
        .unwrap();
        apply(
            &mut state,
            &registry,
            2,
            MetaCommand::SetTableSchema {
                record: schema_record(1, 1, 1),
            },
        )
        .unwrap();
        // Register the replica nodes first (placements validate node
        // references); their projections are far above the ids below.
        for (id, node) in [(3, 1), (4, 2)] {
            apply(
                &mut state,
                &registry,
                id,
                MetaCommand::RegisterNode {
                    descriptor: descriptor(node, &[]),
                },
            )
            .unwrap();
        }
        // Tablet replicas hold raft ids 1 and 3; a placement holds 5.
        let mut tablet = tablet(1, 1, 1);
        tablet.replicas = vec![voter_on(1, 1), voter_on(2, 3)];
        apply(
            &mut state,
            &registry,
            5,
            MetaCommand::SetTabletDescriptor { descriptor: tablet },
        )
        .unwrap();
        apply(
            &mut state,
            &registry,
            6,
            MetaCommand::SetReplicaPlacement {
                placement: ReplicaPlacement {
                    raft_group_id: group_id(8),
                    replicas: vec![voter_on(1, 5)],
                    metadata_version: MetadataVersion::ZERO,
                },
            },
        )
        .unwrap();
        // The allocation [1, 4) collides at 1 and 3, [4, 7) at 5: the first
        // free window of three is [6, 9).
        apply(
            &mut state,
            &registry,
            7,
            MetaCommand::AllocateRaftNodeIds { count: 3 },
        )
        .unwrap();
        assert_eq!(state.raft_id_allocation(&cmd_id(7)), Some(6));
        assert_eq!(state.next_raft_node_id(), 9);
        // A registered node's projection is skipped too: force the counter
        // onto it and watch the allocation step over it.
        apply(
            &mut state,
            &registry,
            8,
            MetaCommand::RegisterNode {
                descriptor: descriptor(9, &[]),
            },
        )
        .unwrap();
        state.next_raft_node_id = raft_node_id(&node_id(9));
        apply(
            &mut state,
            &registry,
            9,
            MetaCommand::AllocateRaftNodeIds { count: 2 },
        )
        .unwrap();
        assert_eq!(
            state.raft_id_allocation(&cmd_id(9)),
            Some(raft_node_id(&node_id(9)) + 1)
        );

        // Restart durability: the counter and the replay records live in the
        // durable checkpoint.
        let tmp = tempfile::tempdir().unwrap();
        let mut sink = MetaApplySink::open(tmp.path(), registry.clone()).unwrap();
        ApplySink::apply(
            &mut sink,
            &applied(
                1,
                meta_envelope(1, MetaCommand::AllocateRaftNodeIds { count: 4 }),
            ),
        )
        .unwrap();
        drop(sink);
        let mut reopened = MetaApplySink::open(tmp.path(), registry).unwrap();
        assert_eq!(reopened.state().next_raft_node_id(), FIRST_RAFT_NODE_ID + 4);
        // The replay record survived: the same command id does not allocate
        // again, and a fresh command continues above the first range.
        ApplySink::apply(
            &mut reopened,
            &applied(
                2,
                meta_envelope(1, MetaCommand::AllocateRaftNodeIds { count: 4 }),
            ),
        )
        .unwrap();
        assert_eq!(reopened.state().next_raft_node_id(), FIRST_RAFT_NODE_ID + 4);
        ApplySink::apply(
            &mut reopened,
            &applied(
                3,
                meta_envelope(2, MetaCommand::AllocateRaftNodeIds { count: 1 }),
            ),
        )
        .unwrap();
        assert_eq!(
            reopened.state().raft_id_allocation(&cmd_id(2)),
            Some(FIRST_RAFT_NODE_ID + 4)
        );
    }

    #[test]
    fn register_node_rejects_a_projection_collision_with_replica_raft_ids() {
        let registry = registry_with("ann-v2", 7);
        let mut state = MetaState::default();
        apply(
            &mut state,
            &registry,
            1,
            MetaCommand::CreateDatabase {
                descriptor: database(1, "app"),
            },
        )
        .unwrap();
        apply(
            &mut state,
            &registry,
            2,
            MetaCommand::SetTableSchema {
                record: schema_record(1, 1, 1),
            },
        )
        .unwrap();
        let mut tablet = tablet(1, 1, 1);
        tablet.replicas = vec![voter_on(1, 4242)];
        apply(
            &mut state,
            &registry,
            3,
            MetaCommand::SetTabletDescriptor { descriptor: tablet },
        )
        .unwrap();
        // A node whose raft-id projection is already a tablet replica's id
        // is refused: tablet groups address replicas by id, and the meta
        // group addresses its members by projection — a collision would
        // attach two raft nodes under one id on a node hosting both.
        let mut colliding = descriptor(7, &[]);
        let mut bytes = [0xAB; 16];
        bytes[..8].copy_from_slice(&4242_u64.to_le_bytes());
        colliding.node_id = NodeId::from_bytes(bytes);
        let error = apply(
            &mut state,
            &registry,
            4,
            MetaCommand::RegisterNode {
                descriptor: colliding.clone(),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Conflict { .. }));
        assert_eq!(state.rejections().len(), 1);
        // A non-colliding registration still succeeds, and an identical
        // re-registration stays a no-op.
        apply(
            &mut state,
            &registry,
            5,
            MetaCommand::RegisterNode {
                descriptor: descriptor(8, &[]),
            },
        )
        .unwrap();
    }

    #[test]
    fn set_tablet_descriptor_rejects_structurally_invalid_descriptors() {
        let registry = registry_with("ann-v2", 7);
        let mut state = MetaState::default();
        apply(
            &mut state,
            &registry,
            1,
            MetaCommand::CreateDatabase {
                descriptor: database(1, "app"),
            },
        )
        .unwrap();
        apply(
            &mut state,
            &registry,
            2,
            MetaCommand::SetTableSchema {
                record: schema_record(1, 1, 1),
            },
        )
        .unwrap();
        // Duplicate raft ids within one group are refused at apply.
        let mut tablet = tablet(1, 1, 1);
        tablet.replicas = vec![voter_on(1, 7), voter_on(2, 7)];
        let error = apply(
            &mut state,
            &registry,
            3,
            MetaCommand::SetTabletDescriptor { descriptor: tablet },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Invalid { .. }));
        assert_eq!(state.rejections().len(), 1);
    }

    // -- feature activation gating at apply ----------------------------------

    #[test]
    fn activation_refused_at_apply_until_every_voter_supports_it() {
        let registry = registry_with("ann-v2", 7);
        let mut state = MetaState::default();
        for (id, byte, features) in [
            (1_u8, 1_u8, vec!["ann-v2"]),
            (2, 2, vec![]),
            (3, 3, vec!["ann-v2"]),
        ] {
            apply(
                &mut state,
                &registry,
                id,
                MetaCommand::RegisterNode {
                    descriptor: descriptor(byte, &features),
                },
            )
            .unwrap();
        }
        let error = apply(
            &mut state,
            &registry,
            4,
            MetaCommand::ActivateFeature {
                activation: activation("ann-v2", 7),
            },
        )
        .unwrap_err();
        assert_eq!(
            error,
            MetaRejectionReason::FeatureActivation(FeatureActivationError::UnsupportedByVoter {
                feature: "ann-v2".to_owned(),
                node: node_id(2),
            })
        );
        // The refusal is journaled with the command id; the level is unmoved.
        assert_eq!(state.feature_level(), ClusterFeatureLevel::ZERO);
        assert_eq!(state.rejections().len(), 1);
        assert_eq!(state.rejections()[0].command_id, Some(cmd_id(4)));
        assert_eq!(state.rejections()[0].reason, error);

        // Node 2 re-registers with support; activation now applies.
        apply(
            &mut state,
            &registry,
            5,
            MetaCommand::RegisterNode {
                descriptor: descriptor(2, &["ann-v2"]),
            },
        )
        .unwrap();
        apply(
            &mut state,
            &registry,
            6,
            MetaCommand::ActivateFeature {
                activation: activation("ann-v2", 7),
            },
        )
        .unwrap();
        assert_eq!(state.feature_level(), ClusterFeatureLevel(7));
    }

    #[test]
    fn activation_apply_rechecks_registry_level_and_voters() {
        let mut registry = registry_with("ann-v2", 7);
        registry.declare("ai-hybrid", ClusterFeatureLevel(5));
        let mut state = MetaState::default();
        // No registered nodes at all: fail closed.
        let error = apply(
            &mut state,
            &registry,
            1,
            MetaCommand::ActivateFeature {
                activation: activation("ann-v2", 7),
            },
        )
        .unwrap_err();
        assert_eq!(
            error,
            MetaRejectionReason::FeatureActivation(FeatureActivationError::NoVoters)
        );
        apply(
            &mut state,
            &registry,
            2,
            MetaCommand::RegisterNode {
                descriptor: descriptor(1, &["ann-v2", "ai-hybrid"]),
            },
        )
        .unwrap();
        // Below the registry minimum.
        let error = apply(
            &mut state,
            &registry,
            3,
            MetaCommand::ActivateFeature {
                activation: activation("ann-v2", 6),
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            MetaRejectionReason::FeatureActivation(
                FeatureActivationError::LevelBelowRequirement { .. }
            )
        ));
        // Undeclared feature.
        let error = apply(
            &mut state,
            &registry,
            4,
            MetaCommand::ActivateFeature {
                activation: activation("nope", 1),
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            MetaRejectionReason::FeatureActivation(FeatureActivationError::UnknownFeature { .. })
        ));
        // Activate, then attempt to lower the level.
        apply(
            &mut state,
            &registry,
            5,
            MetaCommand::ActivateFeature {
                activation: activation("ann-v2", 7),
            },
        )
        .unwrap();
        let error = apply(
            &mut state,
            &registry,
            6,
            MetaCommand::ActivateFeature {
                activation: activation("ai-hybrid", 5),
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            MetaRejectionReason::FeatureActivation(FeatureActivationError::LevelRegression { .. })
        ));
        assert_eq!(state.rejections().len(), 4);
    }

    // -- cluster settings ----------------------------------------------------

    #[test]
    fn settings_denylist_rejects_plaintext_secrets() {
        let registry = FeatureRegistry::current();
        let mut state = MetaState::default();
        for (id, key) in [
            (1_u8, "backup.private_key_pem"),
            (2, "ai.api_key"),
            (3, "admin.password"),
            (4, "tls.secret"),
            (5, "auth.token_endpoint"),
            (6, "service.credential"),
            // The denylist runs before the known-key check (fail closed).
            (7, "secret.unknown"),
        ] {
            let error = apply(
                &mut state,
                &registry,
                id,
                MetaCommand::SetClusterSetting {
                    key: key.to_owned(),
                    value: serde_json::json!("x"),
                },
            )
            .unwrap_err();
            assert_eq!(
                error,
                MetaRejectionReason::SecretSettingKey {
                    key: key.to_owned()
                }
            );
        }
        assert!(state.rejections().len() == 7);
        // Nothing was written.
        assert_eq!(state.settings(), &ClusterSettings::default());
    }

    #[test]
    fn settings_unknown_keys_and_bad_values_are_refused() {
        let registry = FeatureRegistry::current();
        let mut state = MetaState::default();
        let error = apply(
            &mut state,
            &registry,
            1,
            MetaCommand::SetClusterSetting {
                key: "no.such.key".to_owned(),
                value: serde_json::json!(1),
            },
        )
        .unwrap_err();
        assert_eq!(
            error,
            MetaRejectionReason::UnknownSettingKey {
                key: "no.such.key".to_owned()
            }
        );
        for (id, key, value) in [
            (2_u8, "jobs.max_concurrent", serde_json::json!("four")),
            (3, "jobs.max_concurrent", serde_json::json!(0)),
            (4, "backup.enabled", serde_json::json!(1)),
            (
                5,
                "default_consistency",
                serde_json::json!("EventuallyConsistent"),
            ),
        ] {
            let error = apply(
                &mut state,
                &registry,
                id,
                MetaCommand::SetClusterSetting {
                    key: key.to_owned(),
                    value,
                },
            )
            .unwrap_err();
            assert!(matches!(
                error,
                MetaRejectionReason::InvalidSettingValue { .. }
            ));
        }
    }

    #[test]
    fn settings_apply_typed_values() {
        let registry = FeatureRegistry::current();
        let mut state = MetaState::default();
        for (id, key, value) in [
            (1_u8, "history_retention_epochs", serde_json::json!(12)),
            (2, "backup.enabled", serde_json::json!(true)),
            (3, "backup.interval_seconds", serde_json::json!(3_600)),
            (4, "backup.retention_count", serde_json::json!(3)),
            (
                5,
                "default_consistency",
                serde_json::json!({"BoundedStaleness": {"max_lag_ms": 250}}),
            ),
            (6, "ai.max_concurrent_requests", serde_json::json!(8)),
            (7, "ai.max_memory_bytes", serde_json::json!(1 << 20)),
            (8, "jobs.max_concurrent", serde_json::json!(4)),
            (
                9,
                "resource_groups.etl",
                serde_json::json!({"max_memory_bytes": 1024, "max_concurrent_queries": 2, "temp_disk_budget_bytes": 4096}),
            ),
        ] {
            apply(
                &mut state,
                &registry,
                id,
                MetaCommand::SetClusterSetting {
                    key: key.to_owned(),
                    value,
                },
            )
            .unwrap();
        }
        let settings = state.settings();
        assert_eq!(settings.history_retention_epochs, 12);
        assert!(settings.backup.enabled);
        assert_eq!(settings.backup.interval_seconds, 3_600);
        assert_eq!(settings.backup.retention_count, 3);
        assert_eq!(
            settings.default_consistency,
            DefaultConsistency::BoundedStaleness { max_lag_ms: 250 }
        );
        assert_eq!(settings.ai.max_concurrent_requests, 8);
        assert_eq!(settings.ai.max_memory_bytes, 1 << 20);
        assert_eq!(settings.max_concurrent_jobs, 4);
        assert_eq!(settings.resource_groups["etl"].max_memory_bytes, 1_024);
        // A null value removes the group.
        apply(
            &mut state,
            &registry,
            10,
            MetaCommand::SetClusterSetting {
                key: "resource_groups.etl".to_owned(),
                value: serde_json::Value::Null,
            },
        )
        .unwrap();
        assert!(state.settings().resource_groups.is_empty());
    }

    // -- versioning, LWW guards, integrity -----------------------------------

    #[test]
    fn metadata_version_ticks_once_per_applied_command() {
        let registry = FeatureRegistry::current();
        let mut state = MetaState::default();
        assert_eq!(state.metadata_version, MetadataVersion::ZERO);
        apply(
            &mut state,
            &registry,
            1,
            MetaCommand::RegisterNode {
                descriptor: descriptor(1, &[]),
            },
        )
        .unwrap();
        assert_eq!(state.metadata_version, MetadataVersion(1));
        // A refused command still ticks: the refusal itself is journaled state.
        apply(
            &mut state,
            &registry,
            2,
            MetaCommand::UpdateNodeState {
                node_id: node_id(42),
                state: NodeState::Down,
                expected_version: None,
            },
        )
        .unwrap_err();
        assert_eq!(state.metadata_version, MetadataVersion(2));
        apply(
            &mut state,
            &registry,
            3,
            MetaCommand::RemoveNode {
                node_id: node_id(1),
            },
        )
        .unwrap();
        assert_eq!(state.metadata_version, MetadataVersion(3));
    }

    #[test]
    fn stale_and_conflicting_writes_are_refused() {
        let registry = FeatureRegistry::current();
        let mut state = MetaState::default();
        apply(
            &mut state,
            &registry,
            1,
            MetaCommand::RegisterNode {
                descriptor: descriptor(1, &[]),
            },
        )
        .unwrap();
        apply(
            &mut state,
            &registry,
            2,
            MetaCommand::CreateDatabase {
                descriptor: database(1, "app"),
            },
        )
        .unwrap();
        // Optimistic-concurrency guard on node state.
        let node_version = state.node_record(node_id(1)).unwrap().metadata_version;
        let error = apply(
            &mut state,
            &registry,
            3,
            MetaCommand::UpdateNodeState {
                node_id: node_id(1),
                state: NodeState::Down,
                expected_version: Some(MetadataVersion(node_version.get() + 9)),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::StaleWrite { .. }));
        apply(
            &mut state,
            &registry,
            4,
            MetaCommand::UpdateNodeState {
                node_id: node_id(1),
                state: NodeState::Down,
                expected_version: Some(node_version),
            },
        )
        .unwrap();

        // Schema versions move forward only.
        apply(
            &mut state,
            &registry,
            5,
            MetaCommand::SetTableSchema {
                record: schema_record(1, 1, 2),
            },
        )
        .unwrap();
        let error = apply(
            &mut state,
            &registry,
            6,
            MetaCommand::SetTableSchema {
                record: schema_record(1, 1, 1),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::StaleWrite { .. }));
        let mut conflicting = schema_record(1, 1, 2);
        conflicting.schema = serde_json::json!({"columns": []});
        let error = apply(
            &mut state,
            &registry,
            7,
            MetaCommand::SetTableSchema {
                record: conflicting,
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Conflict { .. }));
        // Identical replay is a no-op.
        apply(
            &mut state,
            &registry,
            8,
            MetaCommand::SetTableSchema {
                record: schema_record(1, 1, 2),
            },
        )
        .unwrap();

        // Tablet generations move forward only.
        apply(
            &mut state,
            &registry,
            9,
            MetaCommand::SetTabletDescriptor {
                descriptor: tablet(1, 1, 5),
            },
        )
        .unwrap();
        let mut conflicted = tablet(1, 1, 5);
        conflicted.leader_hint = Some(node_id(1));
        let error = apply(
            &mut state,
            &registry,
            10,
            MetaCommand::SetTabletDescriptor {
                descriptor: conflicted,
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Conflict { .. }));
        let error = apply(
            &mut state,
            &registry,
            11,
            MetaCommand::SetTabletDescriptor {
                descriptor: tablet(1, 1, 4),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::StaleWrite { .. }));
        let error = apply(
            &mut state,
            &registry,
            12,
            MetaCommand::RemoveTabletDescriptor {
                tablet_id: TabletId::from_bytes([1; 16]),
                generation: 4,
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::StaleWrite { .. }));
        apply(
            &mut state,
            &registry,
            13,
            MetaCommand::SetTabletDescriptor {
                descriptor: tablet(1, 1, 6),
            },
        )
        .unwrap();

        // Database id/name uniqueness.
        let error = apply(
            &mut state,
            &registry,
            14,
            MetaCommand::CreateDatabase {
                descriptor: database(2, "app"),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Conflict { .. }));
        let error = apply(
            &mut state,
            &registry,
            15,
            MetaCommand::CreateDatabase {
                descriptor: database(1, "other"),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Conflict { .. }));
        apply(
            &mut state,
            &registry,
            16,
            MetaCommand::CreateDatabase {
                descriptor: database(1, "app"),
            },
        )
        .unwrap();
    }

    #[test]
    fn referential_integrity_is_enforced() {
        let registry = FeatureRegistry::current();
        let mut state = MetaState::default();
        apply(
            &mut state,
            &registry,
            1,
            MetaCommand::RegisterNode {
                descriptor: descriptor(1, &[]),
            },
        )
        .unwrap();
        apply(
            &mut state,
            &registry,
            2,
            MetaCommand::CreateDatabase {
                descriptor: database(1, "app"),
            },
        )
        .unwrap();
        apply(
            &mut state,
            &registry,
            3,
            MetaCommand::SetTableSchema {
                record: schema_record(1, 1, 1),
            },
        )
        .unwrap();
        apply(
            &mut state,
            &registry,
            4,
            MetaCommand::SetReplicaPlacement {
                placement: placement(9, &[(1, ReplicaRole::Voter)]),
            },
        )
        .unwrap();
        // Node removal is refused while a placement references it.
        let error = apply(
            &mut state,
            &registry,
            5,
            MetaCommand::RemoveNode {
                node_id: node_id(1),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Conflict { .. }));
        // Database drop is refused while tables reference it.
        let error = apply(
            &mut state,
            &registry,
            6,
            MetaCommand::DropDatabase {
                database_id: DatabaseId::from_bytes([1; 16]),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Conflict { .. }));
        // A placement may not reference an unregistered node.
        let error = apply(
            &mut state,
            &registry,
            7,
            MetaCommand::SetReplicaPlacement {
                placement: placement(8, &[(7, ReplicaRole::Voter)]),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::NotFound { .. }));
        // Duplicate replica nodes are refused.
        let error = apply(
            &mut state,
            &registry,
            8,
            MetaCommand::SetReplicaPlacement {
                placement: placement(8, &[(1, ReplicaRole::Voter), (1, ReplicaRole::Learner)]),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Conflict { .. }));
        // A tablet's table must exist.
        let error = apply(
            &mut state,
            &registry,
            9,
            MetaCommand::SetTabletDescriptor {
                descriptor: tablet(2, 999, 1),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::NotFound { .. }));
        // A schema's database must exist.
        let error = apply(
            &mut state,
            &registry,
            10,
            MetaCommand::SetTableSchema {
                record: schema_record(2, 42, 1),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::NotFound { .. }));
        // Once the placement moves off, removal succeeds.
        apply(
            &mut state,
            &registry,
            11,
            MetaCommand::SetReplicaPlacement {
                placement: placement(9, &[(1, ReplicaRole::Learner)]),
            },
        )
        .unwrap();
        let error = apply(
            &mut state,
            &registry,
            12,
            MetaCommand::RemoveNode {
                node_id: node_id(1),
            },
        )
        .unwrap_err();
        assert!(
            matches!(error, MetaRejectionReason::Conflict { .. }),
            "learner replicas still reference the node"
        );
    }

    #[test]
    fn schema_job_graph_is_enforced() {
        let registry = FeatureRegistry::current();
        let mut state = MetaState::default();
        apply(
            &mut state,
            &registry,
            1,
            MetaCommand::RegisterNode {
                descriptor: descriptor(1, &[]),
            },
        )
        .unwrap();
        apply(
            &mut state,
            &registry,
            2,
            MetaCommand::CreateDatabase {
                descriptor: database(1, "app"),
            },
        )
        .unwrap();
        apply(
            &mut state,
            &registry,
            3,
            MetaCommand::SetTableSchema {
                record: schema_record(1, 1, 1),
            },
        )
        .unwrap();
        // Submissions must start Pending.
        let mut running = schema_job(7, 1, 1);
        running.state = SchemaJobState::Running;
        let error = apply(
            &mut state,
            &registry,
            4,
            MetaCommand::SubmitSchemaJob { job: running },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Invalid { .. }));
        apply(
            &mut state,
            &registry,
            5,
            MetaCommand::SubmitSchemaJob {
                job: schema_job(7, 1, 1),
            },
        )
        .unwrap();
        // Pending -> Succeeded is not an edge.
        let error = apply(
            &mut state,
            &registry,
            6,
            MetaCommand::UpdateSchemaJob {
                job_id: 7,
                state: SchemaJobState::Succeeded,
                updated_at: ts(2_000),
                error: None,
                expected_version: None,
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Conflict { .. }));
        // Legal walk: Pending -> Running -> Succeeded.
        for (id, job_state) in [
            (7_u8, SchemaJobState::Running),
            (8, SchemaJobState::Succeeded),
        ] {
            apply(
                &mut state,
                &registry,
                id,
                MetaCommand::UpdateSchemaJob {
                    job_id: 7,
                    state: job_state,
                    updated_at: ts(2_000),
                    error: None,
                    expected_version: None,
                },
            )
            .unwrap();
        }
        // Terminal states have no outgoing edges.
        let error = apply(
            &mut state,
            &registry,
            9,
            MetaCommand::UpdateSchemaJob {
                job_id: 7,
                state: SchemaJobState::Running,
                updated_at: ts(3_000),
                error: None,
                expected_version: None,
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::Conflict { .. }));
        // Stale optimistic-concurrency guard.
        let error = apply(
            &mut state,
            &registry,
            10,
            MetaCommand::UpdateSchemaJob {
                job_id: 7,
                state: SchemaJobState::Failed,
                updated_at: ts(3_000),
                error: None,
                expected_version: Some(MetadataVersion(1)),
            },
        )
        .unwrap_err();
        assert!(matches!(error, MetaRejectionReason::StaleWrite { .. }));
    }

    // -- sink: envelope discipline + snapshots -------------------------------

    fn applied(byte: u8, command: ReplicatedCommand) -> AppliedCommand {
        AppliedCommand {
            position: LogPosition {
                term: 1,
                index: u64::from(byte),
            },
            command,
        }
    }

    fn meta_envelope(id: u8, command: MetaCommand) -> ReplicatedCommand {
        let payload = MetaCommandRecord::new(command).encode().unwrap();
        ReplicatedCommand::new(
            CommandKind::Catalog,
            CommandEnvelope::new(COMMAND_TYPE_META_COMMAND, cmd_id(id), payload),
            ts(1_000),
        )
    }

    #[test]
    fn sink_rejects_non_meta_payloads_and_transactions() {
        let tmp = tempfile::tempdir().unwrap();
        let mut sink = MetaApplySink::open(tmp.path(), FeatureRegistry::current()).unwrap();
        // A catalog envelope with a foreign command type fails closed.
        let foreign = ReplicatedCommand::new(
            CommandKind::Catalog,
            CommandEnvelope::new(999, cmd_id(1), b"payload".to_vec()),
            ts(1_000),
        );
        assert!(ApplySink::apply(&mut sink, &applied(1, foreign)).is_err());
        // A transaction command is misrouted to the meta group: fail closed.
        let transaction = ReplicatedCommand::new(
            CommandKind::Transaction,
            CommandEnvelope::new(1, cmd_id(2), b"rows".to_vec()),
            ts(1_000),
        );
        assert!(ApplySink::apply(&mut sink, &applied(2, transaction)).is_err());
        // Maintenance and Noop are documented no-ops.
        let maintenance = ReplicatedCommand::new(
            CommandKind::Maintenance,
            CommandEnvelope::new(3, cmd_id(3), b"directive".to_vec()),
            ts(1_000),
        );
        ApplySink::apply(&mut sink, &applied(3, maintenance)).unwrap();
        ApplySink::apply(&mut sink, &applied(4, ReplicatedCommand::Noop)).unwrap();
        assert_eq!(sink.metadata_version(), MetadataVersion::ZERO);
    }

    #[test]
    fn sink_snapshot_install_preserves_state() {
        let tmp_a = tempfile::tempdir().unwrap();
        let tmp_b = tempfile::tempdir().unwrap();
        let registry = registry_with("ann-v2", 7);
        let mut sink = MetaApplySink::open(tmp_a.path(), registry.clone()).unwrap();
        for (id, command) in every_command_sequence().into_iter().enumerate() {
            let id = u8::try_from(id + 1).unwrap();
            ApplySink::apply(&mut sink, &applied(id, meta_envelope(id, command))).unwrap();
        }
        // Plus one journaled refusal, so the journal round-trips too.
        ApplySink::apply(
            &mut sink,
            &applied(
                42,
                meta_envelope(
                    42,
                    MetaCommand::SetClusterSetting {
                        key: "ai.api_key".to_owned(),
                        value: serde_json::json!("x"),
                    },
                ),
            ),
        )
        .unwrap();
        let bytes = sink.snapshot().unwrap();

        let mut restored = MetaApplySink::open(tmp_b.path(), registry).unwrap();
        restored.install(&bytes).unwrap();
        assert_eq!(restored.state(), sink.state());
        assert_eq!(restored.metadata_version(), sink.metadata_version());
        assert_eq!(restored.applied_position(), sink.applied_position());
        assert_eq!(restored.state().rejections().len(), 1);

        // Corrupt payloads and unsupported versions fail closed.
        assert!(restored.install(b"junk").is_err());
        let mut future = restored.snapshot().unwrap();
        let mut checkpoint: MetaStateCheckpoint = serde_json::from_slice(&future).unwrap();
        checkpoint.format_version = META_STATE_CHECKPOINT_FORMAT_VERSION + 1;
        future = serde_json::to_vec(&checkpoint).unwrap();
        assert!(restored.install(&future).is_err());
        let mut checkpoint: MetaStateCheckpoint =
            serde_json::from_slice(&restored.snapshot().unwrap()).unwrap();
        checkpoint.state.format_version = META_STATE_FORMAT_VERSION + 1;
        assert!(restored
            .install(&serde_json::to_vec(&checkpoint).unwrap())
            .is_err());
        // The failed installs left the state untouched.
        assert_eq!(restored.state(), sink.state());
    }

    #[test]
    fn sink_restart_recovers_from_its_checkpoint() {
        let tmp = tempfile::tempdir().unwrap();
        let registry = registry_with("ann-v2", 7);
        let mut sink = MetaApplySink::open(tmp.path(), registry.clone()).unwrap();
        for (id, command) in every_command_sequence().into_iter().enumerate() {
            let id = u8::try_from(id + 1).unwrap();
            ApplySink::apply(&mut sink, &applied(id, meta_envelope(id, command))).unwrap();
        }
        let before = sink.state().clone();
        let position = sink.applied_position();
        drop(sink);

        // Reopen: the state is recovered from the durable checkpoint without
        // replaying the log.
        let mut reopened = MetaApplySink::open(tmp.path(), registry).unwrap();
        assert_eq!(reopened.state(), &before);
        assert_eq!(reopened.metadata_version(), MetadataVersion(16));
        assert_eq!(reopened.applied_position(), position);

        // Crash-window replay: redelivering an entry at or below the
        // checkpoint watermark is skipped without double-applying.
        let replay = meta_envelope(
            15,
            MetaCommand::DropDatabase {
                database_id: DatabaseId::from_bytes([0xEE; 16]),
            },
        );
        ApplySink::apply(&mut reopened, &applied(15, replay)).unwrap();
        assert_eq!(reopened.state(), &before);
        // A new entry above the watermark applies and checkpoints.
        let next = meta_envelope(
            17,
            MetaCommand::SetClusterSetting {
                key: "jobs.max_concurrent".to_owned(),
                value: serde_json::json!(8),
            },
        );
        ApplySink::apply(&mut reopened, &applied(17, next)).unwrap();
        assert_eq!(reopened.metadata_version(), MetadataVersion(17));
        assert_eq!(reopened.state().settings().max_concurrent_jobs, 8);

        // A present-but-corrupt checkpoint fails closed.
        std::fs::write(
            tmp.path()
                .join("raft")
                .join("state")
                .join(META_STATE_CHECKPOINT_FILENAME),
            b"junk",
        )
        .unwrap();
        assert!(matches!(
            MetaApplySink::open(tmp.path(), FeatureRegistry::current()),
            Err(MetaError::CorruptCheckpoint(_))
        ));
    }

    // -- group integration ----------------------------------------------------

    async fn single_node_group(
        dir: &Path,
        node: u8,
        registry: FeatureRegistry,
        transport: Arc<InMemoryTransport>,
    ) -> MetaGroup<InMemoryTransport> {
        let config = meta_config(dir, node, registry);
        let group_config = fast_group_config(&config);
        let meta = MetaGroup::create(config, group_config, transport)
            .await
            .unwrap();
        meta.bootstrap(&[(
            node_id(node),
            format!("127.0.0.1:{}", 7100 + u16::from(node)),
        )])
        .await
        .unwrap();
        meta.group().wait_leader(LEADER_TIMEOUT).await.unwrap();
        meta
    }

    #[tokio::test]
    async fn single_node_meta_group_round_trips_every_command() {
        let tmp = tempfile::tempdir().unwrap();
        let transport = Arc::new(InMemoryTransport::new());
        let meta = single_node_group(
            &tmp.path().join("node-1"),
            1,
            registry_with("ann-v2", 7),
            transport,
        )
        .await;
        // Directory layout: node-data/groups/<meta-group-id>/raft.
        assert!(tmp
            .path()
            .join("node-1/groups")
            .join(META_GID.to_hex())
            .join("raft")
            .is_dir());

        let control = ExecutionControl::default();
        let mut last_version = MetadataVersion::ZERO;
        for (index, command) in every_command_sequence().into_iter().enumerate() {
            let id = u8::try_from(index + 1).unwrap();
            let receipt = meta.propose(cmd_id(id), command, &control).await.unwrap();
            assert!(receipt.metadata_version > last_version);
            last_version = receipt.metadata_version;
        }
        assert_eq!(last_version, MetadataVersion(16));
        assert_eq!(meta.metadata_version(), MetadataVersion(16));
        let state = meta.state();
        assert_eq!(state.feature_level(), ClusterFeatureLevel(7));
        assert_eq!(state.settings().max_concurrent_jobs, 4);
        assert_eq!(state.schema_job(7).unwrap().state, SchemaJobState::Running);
        assert!(state.rejections().is_empty());
        meta.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn idempotent_replay_through_the_group() {
        let tmp = tempfile::tempdir().unwrap();
        let transport = Arc::new(InMemoryTransport::new());
        let meta = single_node_group(
            &tmp.path().join("node-1"),
            1,
            FeatureRegistry::current(),
            transport,
        )
        .await;
        let control = ExecutionControl::default();
        let command = MetaCommand::RegisterNode {
            descriptor: descriptor(1, &[]),
        };
        let first = meta
            .propose(cmd_id(1), command.clone(), &control)
            .await
            .unwrap();
        assert_eq!(first.metadata_version, MetadataVersion(1));
        assert!(!first.receipt.response.duplicate);
        // A client retry with the same command id and payload commits a new
        // entry but is recognized as a replay at apply (S2B-004).
        let retry = meta.propose(cmd_id(1), command, &control).await.unwrap();
        assert!(retry.receipt.response.duplicate);
        assert_eq!(retry.metadata_version, MetadataVersion(1));
        assert_eq!(meta.state().nodes.len(), 1);
        meta.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn refused_commands_surface_typed_errors_through_the_group() {
        let tmp = tempfile::tempdir().unwrap();
        let transport = Arc::new(InMemoryTransport::new());
        let meta = single_node_group(
            &tmp.path().join("node-1"),
            1,
            registry_with("ann-v2", 7),
            transport,
        )
        .await;
        let control = ExecutionControl::default();
        meta.propose(
            cmd_id(1),
            MetaCommand::RegisterNode {
                descriptor: descriptor(1, &[]),
            },
            &control,
        )
        .await
        .unwrap();
        // The single voter lacks the feature: refused at apply, typed error.
        let error = meta
            .propose(
                cmd_id(2),
                MetaCommand::ActivateFeature {
                    activation: activation("ann-v2", 7),
                },
                &control,
            )
            .await
            .unwrap_err();
        let MetaError::Rejected(reason) = error else {
            panic!("expected a typed rejection, got {error}");
        };
        assert_eq!(
            reason,
            MetaRejectionReason::FeatureActivation(FeatureActivationError::UnsupportedByVoter {
                feature: "ann-v2".to_owned(),
                node: node_id(1),
            })
        );
        // Denied settings key, same path.
        let error = meta
            .propose(
                cmd_id(3),
                MetaCommand::SetClusterSetting {
                    key: "backup.private_key_pem".to_owned(),
                    value: serde_json::json!("pem"),
                },
                &control,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MetaError::Rejected(MetaRejectionReason::SecretSettingKey { .. })
        ));
        // Both refusals are journaled; the watermark still moved.
        assert_eq!(meta.metadata_version(), MetadataVersion(3));
        assert_eq!(meta.state().rejections().len(), 2);
        meta.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn add_and_remove_member_workflow() {
        let tmp = tempfile::tempdir().unwrap();
        let transport = Arc::new(InMemoryTransport::new());
        let meta1 = single_node_group(
            &tmp.path().join("node-1"),
            1,
            FeatureRegistry::current(),
            transport.clone(),
        )
        .await;
        // Node 2 runs a pristine (never initialized) meta group member.
        let config2 = meta_config(&tmp.path().join("node-2"), 2, FeatureRegistry::current());
        let group_config2 = fast_group_config(&config2);
        let meta2 = MetaGroup::create(config2, group_config2, transport.clone())
            .await
            .unwrap();
        let control = ExecutionControl::default();

        // The bootstrap member registers its descriptor (initial-membership
        // registration is part of the bootstrap workflow).
        meta1
            .propose(
                cmd_id(1),
                MetaCommand::RegisterNode {
                    descriptor: descriptor(1, &[]),
                },
                &control,
            )
            .await
            .unwrap();

        // add_member: learner, catch-up, promote, then the descriptor lands
        // in replicated state.
        let descriptor2 = descriptor(2, &[]);
        let receipt = meta1.add_member(&descriptor2, &control).await.unwrap();
        let (voters, _) = meta1.group().members();
        assert!(voters.contains(&raft_id(2)));
        meta2
            .group()
            .wait_applied_index(receipt.receipt.position.index, LEADER_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(
            meta2.state().node(node_id(2)).unwrap().rpc_address,
            descriptor2.rpc_address
        );

        // A placement referencing node 2 blocks its removal.
        meta1
            .propose(
                cmd_id(10),
                MetaCommand::SetReplicaPlacement {
                    placement: placement(9, &[(2, ReplicaRole::Voter)]),
                },
                &control,
            )
            .await
            .unwrap();
        let error = meta1.remove_member(node_id(2), &control).await.unwrap_err();
        assert!(matches!(
            error,
            MetaError::Rejected(MetaRejectionReason::Conflict { .. })
        ));
        let (voters, _) = meta1.group().members();
        assert!(
            voters.contains(&raft_id(2)),
            "a refused removal leaves raft membership untouched"
        );

        // Move the placement off, then remove: meta state first, raft second.
        meta1
            .propose(
                cmd_id(11),
                MetaCommand::SetReplicaPlacement {
                    placement: placement(9, &[(1, ReplicaRole::Voter)]),
                },
                &control,
            )
            .await
            .unwrap();
        meta1.remove_member(node_id(2), &control).await.unwrap();
        let (voters, _) = meta1.group().members();
        assert!(!voters.contains(&raft_id(2)));
        assert!(meta1.state().node(node_id(2)).is_none());

        // Removing the current leader fails closed.
        let error = meta1.remove_member(node_id(1), &control).await.unwrap_err();
        assert!(matches!(error, MetaError::InvalidRequest(_)));

        meta2.shutdown().await.unwrap();
        meta1.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn bootstrap_rejects_raft_id_projection_collisions() {
        let tmp = tempfile::tempdir().unwrap();
        let transport = Arc::new(InMemoryTransport::new());
        let config = meta_config(&tmp.path().join("node-1"), 1, FeatureRegistry::current());
        let group_config = fast_group_config(&config);
        let meta = MetaGroup::create(config, group_config, transport)
            .await
            .unwrap();
        // Distinct node ids sharing the first eight bytes project to the
        // same raft id: rejected at bootstrap by this layer.
        let colliding = NodeId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 9, 9, 9, 9, 9, 9]);
        let first = NodeId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8]);
        let error = meta
            .bootstrap(&[
                (first, "127.0.0.1:7101".to_owned()),
                (colliding, "127.0.0.1:7102".to_owned()),
            ])
            .await
            .unwrap_err();
        assert!(matches!(error, MetaError::InvalidRequest(_)));
        meta.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn create_rejects_a_mismatched_group_config() {
        let tmp = tempfile::tempdir().unwrap();
        let transport = Arc::new(InMemoryTransport::new());
        let config = meta_config(&tmp.path().join("node-1"), 1, FeatureRegistry::current());
        let mut group_config = fast_group_config(&config);
        group_config.dir = tmp.path().join("elsewhere");
        let result = MetaGroup::<InMemoryTransport>::create(config, group_config, transport).await;
        match result {
            Err(error) => assert!(matches!(error, MetaError::InvalidRequest(_))),
            Ok(meta) => {
                meta.shutdown().await.unwrap();
                panic!("expected create to reject a mismatched group config");
            }
        }
    }

    #[tokio::test]
    async fn three_node_meta_group_converges_after_leader_failover() {
        let tmp = tempfile::tempdir().unwrap();
        let transport = Arc::new(InMemoryTransport::new());
        let registry = || registry_with("ann-v2", 7);
        let mut groups: BTreeMap<u8, MetaGroup<InMemoryTransport>> = BTreeMap::new();
        for byte in [1_u8, 2, 3] {
            let config = meta_config(&tmp.path().join(format!("node-{byte}")), byte, registry());
            let group_config = fast_group_config(&config);
            groups.insert(
                byte,
                MetaGroup::create(config, group_config, transport.clone())
                    .await
                    .unwrap(),
            );
        }
        let members: Vec<(NodeId, String)> = [1_u8, 2, 3]
            .iter()
            .map(|byte| (node_id(*byte), format!("127.0.0.1:710{byte}")))
            .collect();
        groups[&1].bootstrap(&members).await.unwrap();
        let leader_byte = {
            let leader = wait_consensus_leader(&[&groups[&1], &groups[&2], &groups[&3]]).await;
            [1_u8, 2, 3]
                .into_iter()
                .find(|byte| raft_id(*byte) == leader)
                .unwrap()
        };

        let control = ExecutionControl::default();
        let mut proposals: Vec<MetaCommand> = [1_u8, 2, 3]
            .iter()
            .map(|byte| MetaCommand::RegisterNode {
                descriptor: descriptor(*byte, &["ann-v2"]),
            })
            .collect();
        proposals.extend([
            MetaCommand::CreateDatabase {
                descriptor: database(1, "app"),
            },
            MetaCommand::SetTableSchema {
                record: schema_record(1, 1, 1),
            },
            MetaCommand::SetReplicaPlacement {
                placement: placement(
                    9,
                    &[
                        (1, ReplicaRole::Voter),
                        (2, ReplicaRole::Voter),
                        (3, ReplicaRole::Voter),
                    ],
                ),
            },
            MetaCommand::SetPlacementPolicy {
                name: "default".to_owned(),
                policy: policy(3),
            },
            MetaCommand::ActivateFeature {
                activation: activation("ann-v2", 7),
            },
            MetaCommand::SetClusterSetting {
                key: "jobs.max_concurrent".to_owned(),
                value: serde_json::json!(4),
            },
            MetaCommand::SetTxnStatusPartition {
                partition: TxnStatusPartition {
                    partition_id: 0,
                    home_raft_group: group_id(9),
                },
            },
        ]);
        let mut last_index = 0_u64;
        for (seq, command) in proposals.into_iter().enumerate() {
            let id = u8::try_from(seq + 1).unwrap();
            let receipt = groups[&leader_byte]
                .propose(cmd_id(id), command, &control)
                .await
                .unwrap();
            last_index = receipt.receipt.position.index;
        }
        for byte in [1_u8, 2, 3] {
            groups[&byte]
                .group()
                .wait_applied_index(last_index, LEADER_TIMEOUT)
                .await
                .unwrap();
        }
        assert_eq!(groups[&1].state(), groups[&2].state());
        assert_eq!(groups[&2].state(), groups[&3].state());

        // Fail over: stop the leader; the survivors elect a new one and keep
        // accepting commands.
        groups[&leader_byte].shutdown().await.unwrap();
        let survivors: Vec<u8> = [1_u8, 2, 3]
            .into_iter()
            .filter(|byte| *byte != leader_byte)
            .collect();
        let new_leader_byte = {
            let leader =
                wait_consensus_leader(&[&groups[&survivors[0]], &groups[&survivors[1]]]).await;
            survivors
                .iter()
                .copied()
                .find(|byte| raft_id(*byte) == leader)
                .unwrap()
        };
        let mut new_index = last_index;
        for (seq, command) in [
            MetaCommand::SubmitSchemaJob {
                job: schema_job(7, 1, 1),
            },
            MetaCommand::UpdateSchemaJob {
                job_id: 7,
                state: SchemaJobState::Running,
                updated_at: ts(9_000),
                error: None,
                expected_version: None,
            },
        ]
        .into_iter()
        .enumerate()
        {
            let id = u8::try_from(seq + 100).unwrap();
            let receipt = groups[&new_leader_byte]
                .propose(cmd_id(id), command, &control)
                .await
                .unwrap();
            new_index = receipt.receipt.position.index;
        }
        for byte in &survivors {
            groups[byte]
                .group()
                .wait_applied_index(new_index, LEADER_TIMEOUT)
                .await
                .unwrap();
        }
        assert_eq!(groups[&survivors[0]].state(), groups[&survivors[1]].state());

        // The failed node rejoins from its durable state and converges.
        let config = meta_config(
            &tmp.path().join(format!("node-{leader_byte}")),
            leader_byte,
            registry(),
        );
        let group_config = fast_group_config(&config);
        let rejoined = MetaGroup::create(config, group_config, transport.clone())
            .await
            .unwrap();
        rejoined
            .group()
            .wait_applied_index(new_index, LEADER_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(rejoined.state(), groups[&survivors[0]].state());
        assert_eq!(
            rejoined.metadata_version(),
            groups[&survivors[0]].metadata_version()
        );
        for group in groups.values() {
            group.shutdown().await.unwrap();
        }
        rejoined.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn snapshot_install_preserves_group_state() {
        let tmp = tempfile::tempdir().unwrap();
        let transport = Arc::new(InMemoryTransport::new());
        let meta = single_node_group(
            &tmp.path().join("node-1"),
            1,
            registry_with("ann-v2", 7),
            transport.clone(),
        )
        .await;
        let control = ExecutionControl::default();
        for (index, command) in every_command_sequence().into_iter().enumerate() {
            let id = u8::try_from(index + 1).unwrap();
            meta.propose(cmd_id(id), command, &control).await.unwrap();
        }
        let snapshot = meta.group().snapshot().await.unwrap();

        // A fresh member installs the image: identical state, watermark, and
        // feature level without replaying the log.
        let config = meta_config(&tmp.path().join("node-2"), 2, registry_with("ann-v2", 7));
        let group_config = fast_group_config(&config);
        let fresh = MetaGroup::create(config, group_config, transport)
            .await
            .unwrap();
        fresh.group().install_snapshot(&snapshot).unwrap();
        assert_eq!(fresh.state(), meta.state());
        assert_eq!(fresh.metadata_version(), meta.metadata_version());
        assert_eq!(fresh.state().feature_level(), ClusterFeatureLevel(7));
        meta.shutdown().await.unwrap();
        fresh.shutdown().await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// Type reconciliation tests: v1 payloads decode and migrate (module docs)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod reconciliation_tests {
    use super::*;
    use crate::node::{BuildVersion, Locality, NodeCapacity};
    use mongreldb_log::commit_log::LogPosition;

    fn node_id(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 16])
    }

    fn group_id(byte: u8) -> RaftGroupId {
        RaftGroupId::from_bytes([byte; 16])
    }

    fn tablet_id(byte: u8) -> TabletId {
        TabletId::from_bytes([byte; 16])
    }

    fn ts(micros: u64) -> HlcTimestamp {
        HlcTimestamp {
            physical_micros: micros,
            logical: 0,
            node_tiebreaker: 0,
        }
    }

    fn descriptor(byte: u8) -> NodeDescriptor {
        NodeDescriptor {
            node_id: node_id(byte),
            rpc_address: format!("127.0.0.1:{}", 7200 + u16::from(byte)),
            locality: Locality::default(),
            capacity: NodeCapacity::default(),
            state: NodeState::Up,
            version: BuildVersion::current(),
            version_info: VersionInfo::current(),
        }
    }

    fn v1_tablet(byte: u8, state: v1::TabletState) -> v1::TabletDescriptor {
        v1::TabletDescriptor {
            tablet_id: tablet_id(byte),
            table_id: TableId(3),
            raft_group_id: group_id(9),
            partition: v1::PartitionBounds {
                start: Some(b"a".to_vec()),
                end: Some(b"m".to_vec()),
            },
            replicas: vec![
                v1::ReplicaDescriptor {
                    node_id: node_id(1),
                    role: v1::ReplicaRole::Voter,
                },
                v1::ReplicaDescriptor {
                    node_id: node_id(2),
                    role: v1::ReplicaRole::Learner,
                },
            ],
            leader_hint: Some(node_id(1)),
            generation: 4,
            state,
            metadata_version: MetadataVersion(11),
        }
    }

    fn v1_policy() -> v1::PlacementPolicy {
        v1::PlacementPolicy {
            replicas: 3,
            voter_constraints: vec![v1::LocalityConstraint {
                key: "region".to_owned(),
                value: "us-central".to_owned(),
            }],
            leader_preferences: vec![v1::LocalityConstraint {
                key: "zone".to_owned(),
                value: "a".to_owned(),
            }],
            prohibited_nodes: vec![node_id(9)],
            metadata_version: MetadataVersion(12),
        }
    }

    /// Encodes a v1 command record exactly as the v1 build did.
    fn v1_encode(command: v1::MetaCommand) -> Vec<u8> {
        serde_json::to_vec(&v1::MetaCommandRecord {
            format_version: 1,
            command,
        })
        .unwrap()
    }

    #[test]
    fn v1_tablet_command_decodes_and_migrates_to_the_canonical_shapes() {
        let decoded = MetaCommandRecord::decode(&v1_encode(v1::MetaCommand::SetTabletDescriptor {
            descriptor: v1_tablet(1, v1::TabletState::Online),
        }))
        .unwrap();
        assert_eq!(decoded.format_version, META_COMMAND_FORMAT_VERSION);
        let MetaCommand::SetTabletDescriptor { descriptor } = decoded.command else {
            panic!("unexpected command variant: {:?}", decoded.command);
        };
        // start/end map onto low/high with v1 semantics (inclusive/exclusive).
        assert_eq!(
            descriptor.partition,
            PartitionBounds {
                low: Bound::Included(Key::from_bytes(b"a".to_vec())),
                high: Bound::Excluded(Key::from_bytes(b"m".to_vec())),
            }
        );
        // Online maps onto Active; roles and the leader hint carry over.
        assert_eq!(descriptor.state, TabletState::Active);
        assert_eq!(descriptor.leader_hint, Some(node_id(1)));
        assert_eq!(descriptor.generation, 4);
        assert_eq!(descriptor.replicas.len(), 2);
        assert_eq!(descriptor.replicas[0].role, ReplicaRole::Voter);
        assert_eq!(descriptor.replicas[1].role, ReplicaRole::Learner);
        // v1 replicas carried no raft id: they gain the node-id projection
        // the v1 group actually used.
        assert_eq!(
            descriptor.replicas[0].raft_node_id,
            raft_node_id(&node_id(1))
        );
        assert_eq!(
            descriptor.replicas[1].raft_node_id,
            raft_node_id(&node_id(2))
        );
        // The command-level descriptor carries no meta modification version.
    }

    #[test]
    fn v1_placement_and_policy_commands_decode_and_migrate() {
        let decoded = MetaCommandRecord::decode(&v1_encode(v1::MetaCommand::SetReplicaPlacement {
            placement: v1::ReplicaPlacement {
                raft_group_id: group_id(9),
                replicas: vec![v1::ReplicaDescriptor {
                    node_id: node_id(3),
                    role: v1::ReplicaRole::Voter,
                }],
                metadata_version: MetadataVersion(5),
            },
        }))
        .unwrap();
        let MetaCommand::SetReplicaPlacement { placement } = decoded.command else {
            panic!("unexpected command variant: {:?}", decoded.command);
        };
        assert_eq!(
            placement.replicas[0].raft_node_id,
            raft_node_id(&node_id(3))
        );
        assert_eq!(placement.metadata_version, MetadataVersion(5));

        let decoded = MetaCommandRecord::decode(&v1_encode(v1::MetaCommand::SetPlacementPolicy {
            name: "default".to_owned(),
            policy: v1_policy(),
        }))
        .unwrap();
        let MetaCommand::SetPlacementPolicy { name, policy } = decoded.command else {
            panic!("unexpected command variant: {:?}", decoded.command);
        };
        assert_eq!(name, "default");
        assert_eq!(policy.replicas, 3);
        // Voter constraints were hard requirements; leader preferences soft.
        assert_eq!(
            policy.voter_constraints,
            vec![LocalityConstraint {
                key: "region".to_owned(),
                value: "us-central".to_owned(),
                required: true,
            }]
        );
        assert_eq!(
            policy.leader_preferences,
            vec![LocalityConstraint {
                key: "zone".to_owned(),
                value: "a".to_owned(),
                required: false,
            }]
        );
        assert_eq!(policy.prohibited_nodes, vec![node_id(9)]);
    }

    #[test]
    fn v1_unaffected_command_variants_pass_through() {
        let decoded = MetaCommandRecord::decode(&v1_encode(v1::MetaCommand::RegisterNode {
            descriptor: descriptor(1),
        }))
        .unwrap();
        assert_eq!(
            decoded.command,
            MetaCommand::RegisterNode {
                descriptor: descriptor(1)
            }
        );
        let decoded =
            MetaCommandRecord::decode(&v1_encode(v1::MetaCommand::RemoveTabletDescriptor {
                tablet_id: tablet_id(1),
                generation: 9,
            }))
            .unwrap();
        assert_eq!(
            decoded.command,
            MetaCommand::RemoveTabletDescriptor {
                tablet_id: tablet_id(1),
                generation: 9,
            }
        );
    }

    #[test]
    fn v1_checkpoint_loads_and_migrates_meta_state() {
        let tmp = tempfile::tempdir().unwrap();
        let state_dir = tmp.path().join("raft").join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        let mut state = v1::MetaState {
            metadata_version: MetadataVersion(20),
            ..v1::MetaState::default()
        };
        state
            .tablets
            .insert(tablet_id(1), v1_tablet(1, v1::TabletState::Offline));
        state.placements.insert(
            group_id(9),
            v1::ReplicaPlacement {
                raft_group_id: group_id(9),
                replicas: vec![v1::ReplicaDescriptor {
                    node_id: node_id(1),
                    role: v1::ReplicaRole::Voter,
                }],
                metadata_version: MetadataVersion(6),
            },
        );
        state
            .placement_policies
            .insert("default".to_owned(), v1_policy());
        let checkpoint = v1::MetaStateCheckpoint {
            format_version: 1,
            position: LogPosition { term: 2, index: 20 },
            command_id: Some([7; 16]),
            state,
        };
        std::fs::write(
            state_dir.join(META_STATE_CHECKPOINT_FILENAME),
            serde_json::to_vec(&checkpoint).unwrap(),
        )
        .unwrap();

        let sink = MetaApplySink::open(tmp.path(), FeatureRegistry::current()).unwrap();
        assert_eq!(sink.applied_position(), LogPosition { term: 2, index: 20 });
        assert_eq!(sink.metadata_version(), MetadataVersion(20));
        let state = sink.state().clone();
        assert_eq!(state.format_version, META_STATE_FORMAT_VERSION);
        // Offline maps onto Retiring; the record version is preserved.
        let record = state.tablet_record(tablet_id(1)).unwrap();
        assert_eq!(record.descriptor.state, TabletState::Retiring);
        assert_eq!(record.metadata_version, MetadataVersion(11));
        let placement = state.placement(group_id(9)).unwrap();
        assert_eq!(
            placement.replicas[0].raft_node_id,
            raft_node_id(&node_id(1))
        );
        assert_eq!(placement.metadata_version, MetadataVersion(6));
        let policy = state.placement_policy_record("default").unwrap();
        assert!(policy.policy.voter_constraints[0].required);
        assert!(!policy.policy.leader_preferences[0].required);
        assert_eq!(policy.metadata_version, MetadataVersion(12));

        // The migrated sink checkpoints v2: after an apply forces a persist,
        // a reopen reads its own format and the on-disk bytes are v2.
        drop(sink);
        let mut reopened = MetaApplySink::open(tmp.path(), FeatureRegistry::current()).unwrap();
        assert_eq!(reopened.state(), &state);
        ApplySink::apply(
            &mut reopened,
            &AppliedCommand {
                position: LogPosition { term: 2, index: 21 },
                command: ReplicatedCommand::Noop,
            },
        )
        .unwrap();
        let on_disk = std::fs::read(state_dir.join(META_STATE_CHECKPOINT_FILENAME)).unwrap();
        let checkpoint: MetaStateCheckpoint = serde_json::from_slice(&on_disk).unwrap();
        assert_eq!(
            checkpoint.format_version,
            META_STATE_CHECKPOINT_FORMAT_VERSION
        );
        assert_eq!(checkpoint.state, *reopened.state());
    }

    #[test]
    fn literal_v1_checkpoint_json_loads_through_serde_defaults() {
        // The exact byte shape a v1 build wrote (sparse state fields ride the
        // serde defaults): guards the v1 compatibility module against drift.
        let tablet_hex = tablet_id(2).to_hex();
        let group_hex = group_id(9).to_hex();
        let node_hex = node_id(1).to_hex();
        let json = serde_json::json!({
            "format_version": 1,
            "position": {"term": 1, "index": 9},
            "command_id": null,
            "state": {
                "format_version": 1,
                "metadata_version": 9,
                "tablets": {
                    tablet_hex.as_str(): {
                        "tablet_id": tablet_hex.as_str(),
                        "table_id": 3,
                        "raft_group_id": group_hex.as_str(),
                        "partition": {"start": null, "end": [109]},
                        "replicas": [{"node_id": node_hex.as_str(), "role": "Voter"}],
                        "leader_hint": null,
                        "generation": 4,
                        "state": "Online",
                        "metadata_version": 7
                    }
                },
                "placement_policies": {
                    "default": {
                        "replicas": 3,
                        "voter_constraints": [{"key": "region", "value": "us-central"}],
                        "leader_preferences": [],
                        "prohibited_nodes": [],
                        "metadata_version": 8
                    }
                }
            }
        });
        let tmp = tempfile::tempdir().unwrap();
        let state_dir = tmp.path().join("raft").join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        std::fs::write(
            state_dir.join(META_STATE_CHECKPOINT_FILENAME),
            serde_json::to_vec(&json).unwrap(),
        )
        .unwrap();

        let sink = MetaApplySink::open(tmp.path(), FeatureRegistry::current()).unwrap();
        let record = sink.state().tablet_record(tablet_id(2)).unwrap();
        assert_eq!(
            record.descriptor.partition,
            PartitionBounds {
                low: Bound::Unbounded,
                high: Bound::Excluded(Key::from_bytes(b"m".to_vec())),
            }
        );
        assert_eq!(record.descriptor.state, TabletState::Active);
        assert_eq!(record.metadata_version, MetadataVersion(7));
        let policy = sink.state().placement_policy_record("default").unwrap();
        assert_eq!(policy.metadata_version, MetadataVersion(8));
        assert_eq!(sink.metadata_version(), MetadataVersion(9));
    }

    #[test]
    fn unsupported_future_versions_fail_closed() {
        let future = serde_json::json!({
            "format_version": META_COMMAND_FORMAT_VERSION + 1,
            "command": {"RemoveNode": {"node_id": node_id(1)}},
        });
        assert_eq!(
            MetaCommandRecord::decode(&serde_json::to_vec(&future).unwrap()).unwrap_err(),
            MetaDecodeError::UnsupportedVersion {
                found: META_COMMAND_FORMAT_VERSION + 1,
                min: MIN_SUPPORTED_META_COMMAND_FORMAT_VERSION,
                max: META_COMMAND_FORMAT_VERSION,
            }
        );

        let tmp = tempfile::tempdir().unwrap();
        let state_dir = tmp.path().join("raft").join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        let future = serde_json::json!({
            "format_version": META_STATE_CHECKPOINT_FORMAT_VERSION + 1,
            "position": {"term": 1, "index": 1},
            "command_id": null,
            "state": {"format_version": 1},
        });
        std::fs::write(
            state_dir.join(META_STATE_CHECKPOINT_FILENAME),
            serde_json::to_vec(&future).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            MetaApplySink::open(tmp.path(), FeatureRegistry::current()),
            Err(MetaError::CorruptCheckpoint(_))
        ));
    }

    #[test]
    fn v1_command_replays_through_the_sink_apply_path() {
        let tmp = tempfile::tempdir().unwrap();
        let mut sink = MetaApplySink::open(tmp.path(), FeatureRegistry::current()).unwrap();
        // Register the table the tablet references, then the v1 tablet write.
        let register = MetaCommandRecord::new(MetaCommand::SetTableSchema {
            record: TableSchemaRecord {
                table_id: TableId(3),
                database_id: DatabaseId::from_bytes([4; 16]),
                schema_version: SchemaVersion(1),
                schema: serde_json::json!({"columns": []}),
                metadata_version: MetadataVersion::ZERO,
            },
        });
        let database = MetaCommandRecord::new(MetaCommand::CreateDatabase {
            descriptor: DatabaseDescriptor {
                database_id: DatabaseId::from_bytes([4; 16]),
                name: "app".to_owned(),
                created_at: ts(1_000),
                state: DatabaseState::Online,
                metadata_version: MetadataVersion::ZERO,
            },
        });
        let envelopes = [
            (1_u64, database.encode().unwrap()),
            (2, register.encode().unwrap()),
            (
                3,
                v1_encode(v1::MetaCommand::SetTabletDescriptor {
                    descriptor: v1_tablet(1, v1::TabletState::Online),
                }),
            ),
        ];
        for (index, payload) in envelopes {
            let command = ReplicatedCommand::new(
                CommandKind::Catalog,
                CommandEnvelope::new(COMMAND_TYPE_META_COMMAND, [index as u8; 16], payload),
                ts(1_000),
            );
            ApplySink::apply(
                &mut sink,
                &AppliedCommand {
                    position: LogPosition { term: 1, index },
                    command,
                },
            )
            .unwrap();
        }
        // The v1 payload applied as the migrated canonical descriptor.
        let record = sink.state().tablet_record(tablet_id(1)).unwrap();
        assert_eq!(record.descriptor.state, TabletState::Active);
        assert_eq!(
            record.descriptor.replicas[0].raft_node_id,
            raft_node_id(&node_id(1))
        );
        assert_eq!(record.metadata_version, MetadataVersion(3));
        // And a v2 write at a higher generation wins over the migrated record.
        let mut descriptor = record.descriptor.clone();
        descriptor.generation = 5;
        let record_v2 = MetaCommandRecord::new(MetaCommand::SetTabletDescriptor {
            descriptor: descriptor.clone(),
        });
        let command = ReplicatedCommand::new(
            CommandKind::Catalog,
            CommandEnvelope::new(
                COMMAND_TYPE_META_COMMAND,
                [9; 16],
                record_v2.encode().unwrap(),
            ),
            ts(1_000),
        );
        ApplySink::apply(
            &mut sink,
            &AppliedCommand {
                position: LogPosition { term: 1, index: 4 },
                command,
            },
        )
        .unwrap();
        assert_eq!(sink.state().tablet(tablet_id(1)).unwrap(), &descriptor);
        assert!(sink.state().rejections().is_empty());
    }

    #[test]
    fn reconciled_records_round_trip_serde() {
        let record = TabletRecord {
            descriptor: migrate_tablet(v1_tablet(1, v1::TabletState::Online)).descriptor,
            metadata_version: MetadataVersion(3),
        };
        let json = serde_json::to_vec(&record).unwrap();
        assert_eq!(
            serde_json::from_slice::<TabletRecord>(&json).unwrap(),
            record
        );

        let policy = PlacementPolicyRecord {
            policy: migrate_policy(v1_policy()).policy,
            metadata_version: MetadataVersion(4),
        };
        let json = serde_json::to_vec(&policy).unwrap();
        assert_eq!(
            serde_json::from_slice::<PlacementPolicyRecord>(&json).unwrap(),
            policy
        );

        let placement = ReplicaPlacement {
            raft_group_id: group_id(9),
            replicas: vec![ReplicaDescriptor {
                node_id: node_id(1),
                role: ReplicaRole::Voter,
                raft_node_id: raft_node_id(&node_id(1)),
            }],
            metadata_version: MetadataVersion(5),
        };
        let json = serde_json::to_vec(&placement).unwrap();
        assert_eq!(
            serde_json::from_slice::<ReplicaPlacement>(&json).unwrap(),
            placement
        );
    }
}
