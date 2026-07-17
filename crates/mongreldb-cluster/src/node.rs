//! Node identity and node descriptor types (spec section 11.1, S2A-001;
//! section 12.1 node descriptor; section 13.7 locality tiers).
//!
//! Every node persists a [`NodeIdentity`] at
//! `<node-data>/cluster-meta/identity.json` on first boot. The identity binds
//! the node to exactly one cluster for the lifetime of the directory: S2A-001
//! requires that a node cannot join two clusters without an explicit
//! wipe/reprovision, so bootstrap and join workflows fail closed with
//! [`ClusterError::ClusterIdentityMismatch`] when a persisted identity names a
//! different cluster. [`wipe_identity`] is the only reset and returns a typed
//! [`WipedMarker`] report for the caller to audit; nothing is logged from
//! this module.
//!
//! All durable writes are atomic: a unique temporary file is written and
//! fsynced, then atomically renamed into place (or hard-linked for
//! create-if-absent creation), followed by a directory fsync — the same idiom
//! the storage core uses for its catalog checkpoints, implemented locally
//! because this crate does not depend on the storage core. Loading verifies
//! the format version and the payload: unknown versions, unknown fields,
//! corrupt payloads, and reserved all-zero identifiers all fail closed (spec
//! section 4.10).

use std::collections::BTreeSet;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::{ClusterId, NodeId};
use serde::{Deserialize, Serialize};

/// The identity format version this build writes.
pub const NODE_IDENTITY_FORMAT_VERSION: u32 = 1;
/// The oldest identity format version this build accepts.
pub const MIN_SUPPORTED_NODE_IDENTITY_FORMAT_VERSION: u32 = 1;
/// Name of the per-node cluster metadata directory under the node data dir.
pub const CLUSTER_META_DIR: &str = "cluster-meta";
/// Name of the persisted identity file inside [`CLUSTER_META_DIR`].
pub const IDENTITY_FILENAME: &str = "identity.json";
/// Upper bound on a single cluster-metadata file.
pub(crate) const MAX_META_BYTES: u64 = 16 * 1024 * 1024;

/// Caller-supplied source of cryptographic randomness used to mint
/// identifiers; production passes `getrandom::getrandom`, tests pass a
/// deterministic filler.
pub type Csprng<'a> = &'a mut dyn FnMut(&mut [u8]) -> Result<(), getrandom::Error>;

/// The one error type of the cluster bootstrap surface.
#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    /// A persisted identity binds this node to a different cluster than the
    /// bootstrap/join being attempted (S2A-001). Only
    /// [`wipe_identity`] resets the binding.
    #[error(
        "cluster identity mismatch: persisted identity belongs to cluster {persisted}, \
         cannot bootstrap or join cluster {requested}; wipe the node identity to reprovision"
    )]
    ClusterIdentityMismatch {
        /// Cluster the persisted identity belongs to.
        persisted: ClusterId,
        /// Cluster the attempted operation targeted.
        requested: ClusterId,
    },
    /// A durable metadata file carries a format version outside the
    /// supported range (spec section 4.10: fail closed).
    #[error("unsupported format version {found} in {file} (supported {min}..={max})")]
    UnsupportedFormatVersion {
        /// Metadata file kind (`identity.json`, `cluster.json`, ...).
        file: &'static str,
        /// Version found in the file.
        found: u32,
        /// Oldest version this build accepts.
        min: u32,
        /// Newest version this build accepts.
        max: u32,
    },
    /// A durable metadata file failed structural verification: undecodable
    /// payload, unknown fields, or a reserved all-zero identifier.
    #[error("cluster metadata file {file} failed verification: {detail}")]
    CorruptMetadata {
        /// Metadata file kind (`identity.json`, `cluster.json`, ...).
        file: &'static str,
        /// What failed verification.
        detail: String,
    },
    /// Cluster metadata I/O failed.
    #[error("cluster metadata I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The caller-supplied CSPRNG failed.
    #[error("operating-system CSPRNG failed: {0}")]
    Rng(String),
    /// The operation is deliberately not implemented yet; fails closed.
    #[error("unsupported operation: {0}")]
    Unsupported(&'static str),
    /// Caller-supplied trust material failed validation.
    #[error("invalid trust material: {0}")]
    InvalidTrustMaterial(&'static str),
    /// A `cluster join` invite failed validation.
    #[error("invalid join invite: {0}")]
    InvalidInvite(&'static str),
    /// The node is already bootstrapped; re-running init/join is rejected.
    #[error(
        "cluster is already bootstrapped on this node (cluster {cluster_id}); \
         wipe the node identity to reprovision"
    )]
    AlreadyBootstrapped {
        /// Cluster the persisted bootstrap record belongs to.
        cluster_id: ClusterId,
    },
    /// No bootstrap record exists in this directory yet.
    #[error(
        "cluster metadata is not initialized in this directory; run cluster init or join first"
    )]
    NotInitialized,
    /// Another bootstrap workflow holds the bootstrap lock file.
    #[error("another bootstrap workflow holds the lock {0}")]
    BootstrapInProgress(PathBuf),
    /// The target node is absent from the local membership record.
    #[error("node {node} is not present in the cluster membership record")]
    NodeNotFound {
        /// The node that was looked up.
        node: NodeId,
    },
    /// The requested node state transition is not permitted.
    #[error("invalid node state transition for node {node}: {from} -> {to}")]
    InvalidNodeStateTransition {
        /// The node whose state was to change.
        node: NodeId,
        /// Current persisted state.
        from: NodeState,
        /// Requested target state.
        to: NodeState,
    },
    /// `node remove` was attempted without the matching confirmation token.
    #[error("invalid node-removal confirmation token")]
    InvalidConfirmationToken,
}

/// Persisted node identity (spec section 11.1, S2A-001).
///
/// Serialized as versioned JSON; `format_version` is part of the struct per
/// S2A-001 and is verified on every load. Unknown fields and unknown versions
/// fail closed (spec section 4.10).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeIdentity {
    /// Cluster this node is bound to for the lifetime of its data directory.
    pub cluster_id: ClusterId,
    /// This node's identifier within the cluster.
    pub node_id: NodeId,
    /// Wall-clock time the identity was minted (informational).
    pub created_at: HlcTimestamp,
    /// Durable format version; see [`NODE_IDENTITY_FORMAT_VERSION`].
    pub format_version: u32,
}

impl NodeIdentity {
    /// Load and verify the persisted identity, if present.
    ///
    /// Returns `Ok(None)` when no identity has been minted yet. A present but
    /// undecodable, unknown-version, or reserved-identifier file fails closed
    /// with a typed error; it is never silently replaced.
    pub fn load(node_data: &Path) -> Result<Option<Self>, ClusterError> {
        let path = cluster_meta_dir(node_data).join(IDENTITY_FILENAME);
        let Some(bytes) = read_meta_file(&path)? else {
            return Ok(None);
        };
        let identity: Self = decode_json(IDENTITY_FILENAME, &bytes)?;
        if identity.format_version < MIN_SUPPORTED_NODE_IDENTITY_FORMAT_VERSION
            || identity.format_version > NODE_IDENTITY_FORMAT_VERSION
        {
            return Err(ClusterError::UnsupportedFormatVersion {
                file: IDENTITY_FILENAME,
                found: identity.format_version,
                min: MIN_SUPPORTED_NODE_IDENTITY_FORMAT_VERSION,
                max: NODE_IDENTITY_FORMAT_VERSION,
            });
        }
        if identity.cluster_id == ClusterId::ZERO || identity.node_id == NodeId::ZERO {
            return Err(ClusterError::CorruptMetadata {
                file: IDENTITY_FILENAME,
                detail: "reserved all-zero identifier".to_owned(),
            });
        }
        Ok(Some(identity))
    }

    /// Load the persisted identity, or mint and persist a fresh one on first
    /// boot (`cluster_id` and `node_id` drawn from `csprng`).
    ///
    /// Concurrent first boots on one directory race on an atomic
    /// create-if-absent publish; the loser loads and returns the winner's
    /// identity, so the result is stable.
    pub fn load_or_create(node_data: &Path, csprng: Csprng<'_>) -> Result<Self, ClusterError> {
        if let Some(identity) = Self::load(node_data)? {
            return Ok(identity);
        }
        let cluster_id = ClusterId::from_bytes(mint_id(csprng)?);
        match Self::create(node_data, cluster_id, csprng)? {
            CreateOutcome::Created(identity) => Ok(identity),
            CreateOutcome::AlreadyExists => {
                Self::load(node_data)?.ok_or(ClusterError::CorruptMetadata {
                    file: IDENTITY_FILENAME,
                    detail: "identity vanished after create race".to_owned(),
                })
            }
        }
    }

    /// Provision an identity for a specific cluster (bootstrap/join path).
    ///
    /// - No persisted identity: mint a fresh `node_id` and persist.
    /// - Persisted identity for the same cluster: verified and returned
    ///   unchanged (idempotent retry).
    /// - Persisted identity for a different cluster: fails closed with
    ///   [`ClusterError::ClusterIdentityMismatch`] (S2A-001).
    pub(crate) fn provision(
        node_data: &Path,
        cluster_id: ClusterId,
        csprng: Csprng<'_>,
    ) -> Result<Self, ClusterError> {
        match Self::create(node_data, cluster_id, csprng)? {
            CreateOutcome::Created(identity) => Ok(identity),
            CreateOutcome::AlreadyExists => {
                let identity = Self::load(node_data)?.ok_or(ClusterError::CorruptMetadata {
                    file: IDENTITY_FILENAME,
                    detail: "identity vanished after create race".to_owned(),
                })?;
                if identity.cluster_id != cluster_id {
                    return Err(ClusterError::ClusterIdentityMismatch {
                        persisted: identity.cluster_id,
                        requested: cluster_id,
                    });
                }
                Ok(identity)
            }
        }
    }

    fn create(
        node_data: &Path,
        cluster_id: ClusterId,
        csprng: Csprng<'_>,
    ) -> Result<CreateOutcome, ClusterError> {
        let identity = Self {
            cluster_id,
            node_id: NodeId::from_bytes(mint_id(csprng)?),
            created_at: wall_clock_now(),
            format_version: NODE_IDENTITY_FORMAT_VERSION,
        };
        let bytes = encode_json(IDENTITY_FILENAME, &identity)?;
        let meta_dir = cluster_meta_dir(node_data);
        fs::create_dir_all(&meta_dir)?;
        Ok(
            match create_meta_file(&meta_dir, IDENTITY_FILENAME, &bytes)? {
                true => CreateOutcome::Created(identity),
                false => CreateOutcome::AlreadyExists,
            },
        )
    }
}

enum CreateOutcome {
    Created(NodeIdentity),
    AlreadyExists,
}

/// Typed audit report returned by [`wipe_identity`]; the caller decides how
/// to log it (this module never logs).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WipedMarker {
    /// Cluster the wiped identity belonged to.
    pub wiped_cluster_id: ClusterId,
    /// Node id of the wiped identity.
    pub wiped_node_id: NodeId,
    /// Wall-clock time the wipe ran (informational).
    pub wiped_at: HlcTimestamp,
    /// Durable files removed by the wipe, relative to the node data dir.
    pub removed_files: Vec<PathBuf>,
}

/// Explicitly wipe this node's cluster provisioning state: the identity plus
/// any bootstrap records under `cluster-meta/`.
///
/// This is the only reset that lets the node join a different cluster
/// (S2A-001). Returns `Ok(None)` when no identity was persisted. The returned
/// [`WipedMarker`] is the audit trail of what was destroyed.
pub fn wipe_identity(node_data: &Path) -> Result<Option<WipedMarker>, ClusterError> {
    let Some(identity) = NodeIdentity::load(node_data)? else {
        return Ok(None);
    };
    let meta_dir = cluster_meta_dir(node_data);
    let mut removed_files = Vec::new();
    for filename in [
        IDENTITY_FILENAME,
        crate::bootstrap::CLUSTER_RECORD_FILENAME,
        crate::bootstrap::TRUST_FILENAME,
        crate::bootstrap::JOIN_RECORD_FILENAME,
    ] {
        let path = meta_dir.join(filename);
        match fs::remove_file(&path) {
            Ok(()) => removed_files.push(path),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    sync_dir(&meta_dir)?;
    Ok(Some(WipedMarker {
        wiped_cluster_id: identity.cluster_id,
        wiped_node_id: identity.node_id,
        wiped_at: wall_clock_now(),
        removed_files,
    }))
}

/// Lifecycle state of a cluster node (spec section 12.1). Declaration order
/// is frozen; enum values are never reused (spec section 4.10).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeState {
    /// Joined but not yet accepted into membership.
    Bootstrapping,
    /// Full member serving traffic.
    Up,
    /// Leaving the cluster; replicas and leases are moved off first.
    Draining,
    /// Permanently removed from the cluster.
    Decommissioned,
    /// Temporarily unreachable; expected to return.
    Down,
}

impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Bootstrapping => "Bootstrapping",
            Self::Up => "Up",
            Self::Draining => "Draining",
            Self::Decommissioned => "Decommissioned",
            Self::Down => "Down",
        };
        f.write_str(name)
    }
}

/// Error returned when parsing a textual [`Locality`] fails.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LocalityParseError {
    /// A comma-separated tier was not of the form `key=value`.
    #[error("invalid locality tier `{0}`: expected `key=value`")]
    InvalidTier(String),
    /// A tier had an empty key or value.
    #[error("locality tier `{0}` has an empty key or value")]
    EmptyComponent(String),
    /// The same locality key appeared twice.
    #[error("duplicate locality key `{0}`")]
    DuplicateKey(String),
}

/// Ordered locality tiers of a node, coarsest first, following the
/// `region, availability zone, rack, node` hierarchy of spec section 13.7.
///
/// The canonical text form is comma-separated `key=value` pairs, matching the
/// node configuration file (`locality = "region=us-central,zone=a"`, spec
/// section 16.1). Parsing trims surrounding whitespace, rejects malformed,
/// empty, and duplicate tiers, and preserves tier order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Locality {
    tiers: Vec<(String, String)>,
}

impl Locality {
    /// The ordered `(key, value)` tiers, coarsest first.
    pub fn tiers(&self) -> &[(String, String)] {
        &self.tiers
    }

    /// The value of one tier key, if present.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.tiers
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

impl FromStr for Locality {
    type Err = LocalityParseError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let mut tiers = Vec::new();
        for tier in text.split(',') {
            let tier = tier.trim();
            if tier.is_empty() {
                continue;
            }
            let (key, value) = tier
                .split_once('=')
                .ok_or_else(|| LocalityParseError::InvalidTier(tier.to_owned()))?;
            let key = key.trim();
            let value = value.trim();
            if key.is_empty() || value.is_empty() {
                return Err(LocalityParseError::EmptyComponent(tier.to_owned()));
            }
            if tiers.iter().any(|(k, _): &(String, String)| k == key) {
                return Err(LocalityParseError::DuplicateKey(key.to_owned()));
            }
            tiers.push((key.to_owned(), value.to_owned()));
        }
        Ok(Self { tiers })
    }
}

impl fmt::Display for Locality {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, (key, value)) in self.tiers.iter().enumerate() {
            if index > 0 {
                f.write_str(",")?;
            }
            write!(f, "{key}={value}")?;
        }
        Ok(())
    }
}

impl Serialize for Locality {
    /// Serializes as the canonical `key=value,...` string in every format.
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Locality {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

/// Advertised capacity of one node, used by placement (spec section 12.1).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeCapacity {
    /// Logical CPU cores.
    pub cpu: u32,
    /// Usable memory in bytes.
    pub memory_bytes: u64,
    /// Usable disk in bytes.
    pub disk_bytes: u64,
}

/// Build version a node advertises (spec section 11.8).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildVersion {
    /// Engine package version (`CARGO_PKG_VERSION` at build time).
    pub version: String,
    /// Source revision, when the build recorded `MONGRELDB_GIT_SHA`.
    pub git_sha: Option<String>,
}

impl BuildVersion {
    /// The version of the running binary.
    pub fn current() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            git_sha: option_env!("MONGRELDB_GIT_SHA").map(str::to_owned),
        }
    }
}

/// Oldest wire-protocol version this build accepts (spec section 11.8).
///
/// Mirrors `MIN_SUPPORTED_PROTOCOL_VERSION` in
/// `crates/mongreldb-protocol/src/envelope.rs`; the cluster crate
/// deliberately does not depend on the protocol crate, so the value is
/// declared here and must track it (join-time overlap checks fail closed on
/// drift).
pub const PROTOCOL_VERSION_MIN: u32 = 1;
/// Newest wire-protocol version this build speaks.
///
/// Mirrors `PROTOCOL_VERSION` in
/// `crates/mongreldb-protocol/src/envelope.rs`; see [`PROTOCOL_VERSION_MIN`].
pub const PROTOCOL_VERSION_MAX: u32 = 1;
/// Oldest committed-command log format this build reads: the FND-003
/// `CommandEnvelope` minimum in `mongreldb-log`.
pub const LOG_FORMAT_VERSION_MIN: u32 = mongreldb_log::envelope::MIN_SUPPORTED_FORMAT_VERSION;
/// Newest committed-command log format this build writes: the FND-003
/// `CommandEnvelope` version in `mongreldb-log`.
pub const LOG_FORMAT_VERSION_MAX: u32 = mongreldb_log::envelope::COMMAND_ENVELOPE_FORMAT_VERSION;
/// Oldest replicated snapshot format this build reads.
///
/// Mirrors the private log-store `FORMAT_VERSION` in
/// `crates/mongreldb-consensus/src/storage.rs`, which frames the snapshot
/// records this build exchanges; the authoritative minimum lands in
/// `mongreldb-consensus` when snapshot envelopes are versioned per
/// ADR-0010.
pub const SNAPSHOT_FORMAT_VERSION_MIN: u32 = 1;
/// Newest replicated snapshot format this build writes.
///
/// Mirrors the private log-store `FORMAT_VERSION` in
/// `crates/mongreldb-consensus/src/storage.rs`; the authoritative maximum
/// lands in `mongreldb-consensus` alongside snapshot envelope versioning
/// (ADR-0010 migration step 3).
pub const SNAPSHOT_FORMAT_VERSION_MAX: u32 = 1;

/// Version and format compatibility envelope every node advertises (spec
/// section 11.8; ADR-0010 decision 5).
///
/// Ranges are inclusive. The derived [`Default`] describes a descriptor
/// written before Stage 2H that never carried an advertisement: its zeroed
/// ranges overlap only other zeroed ranges, so a missing advertisement fails
/// closed against any real one (spec section 4.10).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionInfo {
    /// Engine package version of the running binary (`CARGO_PKG_VERSION`).
    pub binary_version: String,
    /// Oldest wire-protocol version accepted.
    pub protocol_min: u32,
    /// Newest wire-protocol version spoken.
    pub protocol_max: u32,
    /// Oldest committed-command log format read.
    pub log_format_min: u32,
    /// Newest committed-command log format written.
    pub log_format_max: u32,
    /// Oldest replicated snapshot format read.
    pub snapshot_format_min: u32,
    /// Newest replicated snapshot format written.
    pub snapshot_format_max: u32,
    /// Names of cluster features this build can activate; the advertised set
    /// gates feature activation (spec section 11.8 step 5).
    pub feature_set: BTreeSet<String>,
}

impl VersionInfo {
    /// The advertisement of the running binary, sourced from the build
    /// environment and this crate's declared format ranges.
    pub fn current() -> Self {
        Self {
            binary_version: env!("CARGO_PKG_VERSION").to_owned(),
            protocol_min: PROTOCOL_VERSION_MIN,
            protocol_max: PROTOCOL_VERSION_MAX,
            log_format_min: LOG_FORMAT_VERSION_MIN,
            log_format_max: LOG_FORMAT_VERSION_MAX,
            snapshot_format_min: SNAPSHOT_FORMAT_VERSION_MIN,
            snapshot_format_max: SNAPSHOT_FORMAT_VERSION_MAX,
            feature_set: crate::meta::FeatureRegistry::current().feature_names(),
        }
    }

    /// Verify that this node and `other` can interoperate in one cluster.
    ///
    /// Compatibility requires every advertised range pair to overlap: with
    /// the N/N-1 support window (spec section 17) an N binary accepts
    /// `[N-1, N]` and an N-1 binary accepts `[N-1, N-1]`, which overlap, while
    /// a two-version skew does not. Failures fail closed with a typed
    /// [`Incompatibility`] naming the first non-overlapping range.
    pub fn is_compatible_with(&self, other: &Self) -> Result<(), Incompatibility> {
        if !ranges_overlap(
            self.protocol_min,
            self.protocol_max,
            other.protocol_min,
            other.protocol_max,
        ) {
            return Err(Incompatibility::ProtocolVersion {
                ours: (self.protocol_min, self.protocol_max),
                theirs: (other.protocol_min, other.protocol_max),
            });
        }
        if !ranges_overlap(
            self.log_format_min,
            self.log_format_max,
            other.log_format_min,
            other.log_format_max,
        ) {
            return Err(Incompatibility::LogFormat {
                ours: (self.log_format_min, self.log_format_max),
                theirs: (other.log_format_min, other.log_format_max),
            });
        }
        if !ranges_overlap(
            self.snapshot_format_min,
            self.snapshot_format_max,
            other.snapshot_format_min,
            other.snapshot_format_max,
        ) {
            return Err(Incompatibility::SnapshotFormat {
                ours: (self.snapshot_format_min, self.snapshot_format_max),
                theirs: (other.snapshot_format_min, other.snapshot_format_max),
            });
        }
        Ok(())
    }
}

/// Inclusive ranges `[a_min, a_max]` and `[b_min, b_max]` overlap. Malformed
/// ranges (`min > max`) never overlap, so they fail closed.
fn ranges_overlap(a_min: u32, a_max: u32, b_min: u32, b_max: u32) -> bool {
    a_min <= a_max && b_min <= b_max && a_min <= b_max && b_min <= a_max
}

/// One advertised range pair does not overlap; compatibility checks fail
/// closed with the first mismatch found (ADR-0010: never silently degrade).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Incompatibility {
    /// Wire-protocol ranges do not overlap.
    #[error(
        "wire-protocol version ranges do not overlap: {ours:?} vs {theirs:?} (min, max inclusive)"
    )]
    ProtocolVersion {
        /// This node's `(min, max)` protocol range.
        ours: (u32, u32),
        /// The other node's `(min, max)` protocol range.
        theirs: (u32, u32),
    },
    /// Committed-command log format ranges do not overlap.
    #[error("log format ranges do not overlap: {ours:?} vs {theirs:?} (min, max inclusive)")]
    LogFormat {
        /// This node's `(min, max)` log format range.
        ours: (u32, u32),
        /// The other node's `(min, max)` log format range.
        theirs: (u32, u32),
    },
    /// Replicated snapshot format ranges do not overlap.
    #[error("snapshot format ranges do not overlap: {ours:?} vs {theirs:?} (min, max inclusive)")]
    SnapshotFormat {
        /// This node's `(min, max)` snapshot format range.
        ours: (u32, u32),
        /// The other node's `(min, max)` snapshot format range.
        theirs: (u32, u32),
    },
}

/// One cluster member as advertised by the meta control plane (spec section
/// 12.1). Defined with the Stage 2A bootstrap workflows; the meta group takes
/// ownership of the replicated copy in Stage 3A.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeDescriptor {
    /// The member's node identifier.
    pub node_id: NodeId,
    /// Advertised RPC address (`host:port`).
    pub rpc_address: String,
    /// Ordered locality tiers (spec section 13.7).
    pub locality: Locality,
    /// Advertised capacity.
    pub capacity: NodeCapacity,
    /// Lifecycle state.
    pub state: NodeState,
    /// Advertised build version.
    pub version: BuildVersion,
    /// Advertised version/format compatibility envelope (spec section 11.8).
    ///
    /// Deserialized with [`VersionInfo::default`] when absent so descriptors
    /// written before Stage 2H still decode; the default's zeroed ranges
    /// fail closed against any real advertisement (spec section 4.10).
    #[serde(default)]
    pub version_info: VersionInfo,
}

/// `<node-data>/cluster-meta`.
pub(crate) fn cluster_meta_dir(node_data: &Path) -> PathBuf {
    node_data.join(CLUSTER_META_DIR)
}

/// Wall-clock time as an HLC timestamp (logical and tiebreaker zero); used
/// only for informational `created_at`/`wiped_at` markers, never for commit
/// timestamps.
pub(crate) fn wall_clock_now() -> HlcTimestamp {
    let physical_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_micros()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    HlcTimestamp {
        physical_micros,
        logical: 0,
        node_tiebreaker: 0,
    }
}

/// Mint one non-zero 128-bit identifier from the CSPRNG. The all-zero value
/// is reserved, so it is redrawn rather than persisted.
pub(crate) fn mint_id(csprng: Csprng<'_>) -> Result<[u8; 16], ClusterError> {
    loop {
        let mut bytes = [0u8; 16];
        csprng(&mut bytes).map_err(|error| ClusterError::Rng(error.to_string()))?;
        if bytes != [0u8; 16] {
            return Ok(bytes);
        }
    }
}

/// Serialize one metadata value as pretty JSON.
pub(crate) fn encode_json<T: Serialize>(
    file: &'static str,
    value: &T,
) -> Result<Vec<u8>, ClusterError> {
    serde_json::to_vec_pretty(value).map_err(|error| ClusterError::CorruptMetadata {
        file,
        detail: format!("encode: {error}"),
    })
}

/// Deserialize one metadata value, rejecting trailing bytes and unknown
/// fields (via `deny_unknown_fields` on the target type).
pub(crate) fn decode_json<T: for<'de> Deserialize<'de>>(
    file: &'static str,
    bytes: &[u8],
) -> Result<T, ClusterError> {
    serde_json::from_slice(bytes).map_err(|error| ClusterError::CorruptMetadata {
        file,
        detail: format!("decode: {error}"),
    })
}

/// Read one metadata file, returning `Ok(None)` when it is absent. Files
/// larger than [`MAX_META_BYTES`] fail closed.
pub(crate) fn read_meta_file(path: &Path) -> Result<Option<Vec<u8>>, ClusterError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let length = file.metadata()?.len();
    if length > MAX_META_BYTES {
        return Err(ClusterError::CorruptMetadata {
            file: "cluster-meta",
            detail: format!(
                "{} exceeds the {} byte limit",
                path.display(),
                MAX_META_BYTES
            ),
        });
    }
    let mut bytes = Vec::with_capacity(length as usize);
    file.take(MAX_META_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length {
        return Err(ClusterError::CorruptMetadata {
            file: "cluster-meta",
            detail: format!("{} changed while reading", path.display()),
        });
    }
    Ok(Some(bytes))
}

/// Atomically replace `<dir>/<filename>`: unique synced temporary file,
/// atomic rename, directory fsync (the storage core's catalog idiom).
pub(crate) fn write_meta_atomic(dir: &Path, filename: &str, bytes: &[u8]) -> io::Result<()> {
    let temporary = write_temp_file(dir, filename, bytes)?;
    let result = fs::rename(&temporary, dir.join(filename));
    match result {
        Ok(()) => sync_dir(dir),
        Err(error) => {
            let _ = fs::remove_file(&temporary);
            Err(error)
        }
    }
}

/// Atomically create `<dir>/<filename>` only if it does not exist (synced
/// temporary file published by a hard link, which fails if the destination
/// already exists). Returns `Ok(false)` when the file was already present.
pub(crate) fn create_meta_file(dir: &Path, filename: &str, bytes: &[u8]) -> io::Result<bool> {
    let temporary = write_temp_file(dir, filename, bytes)?;
    let result = fs::hard_link(&temporary, dir.join(filename));
    let _ = fs::remove_file(&temporary);
    match result {
        Ok(()) => {
            sync_dir(dir)?;
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(false),
        Err(error) => Err(error),
    }
}

/// Write `bytes` to a unique temporary file beside `filename` and fsync it.
fn write_temp_file(dir: &Path, filename: &str, bytes: &[u8]) -> io::Result<PathBuf> {
    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);
    loop {
        let unique = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let temporary = dir.join(format!(".{filename}.tmp-{}-{unique}", std::process::id()));
        let mut file = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(file) => file,
            // Another writer took this exact name; draw a fresh one.
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        };
        let result = file.write_all(bytes).and_then(|()| file.sync_all());
        drop(file);
        match result {
            Ok(()) => return Ok(temporary),
            Err(error) => {
                let _ = fs::remove_file(&temporary);
                return Err(error);
            }
        }
    }
}

/// Fsync a directory so a rename/link inside it becomes durable.
pub(crate) fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_csprng() -> impl FnMut(&mut [u8]) -> Result<(), getrandom::Error> {
        let mut counter = 0u64;
        move |buf: &mut [u8]| {
            for chunk in buf.chunks_mut(8) {
                counter += 1;
                let bytes = counter.to_le_bytes();
                chunk.copy_from_slice(&bytes[..chunk.len()]);
            }
            Ok(())
        }
    }

    fn minted_identity(node_data: &Path) -> NodeIdentity {
        NodeIdentity::load_or_create(node_data, &mut test_csprng()).expect("mint identity")
    }

    #[test]
    fn first_boot_mints_and_persists_identity() {
        let dir = tempfile::tempdir().unwrap();
        let identity = minted_identity(dir.path());
        assert_ne!(identity.cluster_id, ClusterId::ZERO);
        assert_ne!(identity.node_id, NodeId::ZERO);
        assert_eq!(identity.format_version, NODE_IDENTITY_FORMAT_VERSION);
        assert!(dir
            .path()
            .join(CLUSTER_META_DIR)
            .join(IDENTITY_FILENAME)
            .is_file());
    }

    #[test]
    fn reload_returns_the_same_identity() {
        let dir = tempfile::tempdir().unwrap();
        let first = minted_identity(dir.path());
        let second = NodeIdentity::load_or_create(dir.path(), &mut test_csprng())
            .expect("second boot loads, never re-mints");
        assert_eq!(first, second);
        assert_eq!(NodeIdentity::load(dir.path()).unwrap(), Some(first));
    }

    #[test]
    fn minted_cluster_and_node_ids_differ() {
        let dir = tempfile::tempdir().unwrap();
        let identity = minted_identity(dir.path());
        assert_ne!(identity.cluster_id.as_bytes(), identity.node_id.as_bytes());
    }

    #[test]
    fn load_on_empty_dir_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(NodeIdentity::load(dir.path()).unwrap(), None);
    }

    #[test]
    fn unknown_format_version_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        minted_identity(dir.path());
        let mut value: serde_json::Value = serde_json::from_slice(
            &std::fs::read(dir.path().join(CLUSTER_META_DIR).join(IDENTITY_FILENAME)).unwrap(),
        )
        .unwrap();
        value["format_version"] = serde_json::json!(99);
        std::fs::write(
            dir.path().join(CLUSTER_META_DIR).join(IDENTITY_FILENAME),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
        let error = NodeIdentity::load(dir.path()).unwrap_err();
        assert!(
            matches!(
                error,
                ClusterError::UnsupportedFormatVersion {
                    file: IDENTITY_FILENAME,
                    found: 99,
                    ..
                }
            ),
            "unexpected error: {error}"
        );
        // A tampered file is never silently replaced.
        assert!(NodeIdentity::load_or_create(dir.path(), &mut test_csprng()).is_err());
    }

    #[test]
    fn corrupt_payload_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let meta = dir.path().join(CLUSTER_META_DIR);
        std::fs::create_dir_all(&meta).unwrap();
        std::fs::write(meta.join(IDENTITY_FILENAME), b"{ not json").unwrap();
        let error = NodeIdentity::load(dir.path()).unwrap_err();
        assert!(matches!(
            error,
            ClusterError::CorruptMetadata {
                file: IDENTITY_FILENAME,
                ..
            }
        ));
    }

    #[test]
    fn unknown_fields_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        minted_identity(dir.path());
        let path = dir.path().join(CLUSTER_META_DIR).join(IDENTITY_FILENAME);
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        value["unexpected"] = serde_json::json!(1);
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        let error = NodeIdentity::load(dir.path()).unwrap_err();
        assert!(matches!(
            error,
            ClusterError::CorruptMetadata {
                file: IDENTITY_FILENAME,
                ..
            }
        ));
    }

    #[test]
    fn reserved_zero_identifiers_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let meta = dir.path().join(CLUSTER_META_DIR);
        std::fs::create_dir_all(&meta).unwrap();
        let identity = NodeIdentity {
            cluster_id: ClusterId::ZERO,
            node_id: NodeId::new_random(),
            created_at: HlcTimestamp::ZERO,
            format_version: NODE_IDENTITY_FORMAT_VERSION,
        };
        std::fs::write(
            meta.join(IDENTITY_FILENAME),
            serde_json::to_vec(&identity).unwrap(),
        )
        .unwrap();
        let error = NodeIdentity::load(dir.path()).unwrap_err();
        assert!(matches!(
            error,
            ClusterError::CorruptMetadata {
                file: IDENTITY_FILENAME,
                ..
            }
        ));
    }

    #[test]
    fn provision_rejects_a_different_cluster() {
        let dir = tempfile::tempdir().unwrap();
        let cluster_a = ClusterId::new_random();
        let identity = NodeIdentity::provision(dir.path(), cluster_a, &mut test_csprng()).unwrap();
        assert_eq!(identity.cluster_id, cluster_a);
        // Same cluster: idempotent, returns the persisted identity.
        let again = NodeIdentity::provision(dir.path(), cluster_a, &mut test_csprng()).unwrap();
        assert_eq!(again, identity);
        // Different cluster: fails closed (S2A-001).
        let error =
            NodeIdentity::provision(dir.path(), ClusterId::new_random(), &mut test_csprng())
                .unwrap_err();
        assert!(
            matches!(
                error,
                ClusterError::ClusterIdentityMismatch { persisted, .. } if persisted == cluster_a
            ),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn wipe_is_the_only_reset() {
        let dir = tempfile::tempdir().unwrap();
        let cluster_a = ClusterId::new_random();
        NodeIdentity::provision(dir.path(), cluster_a, &mut test_csprng()).unwrap();
        let marker = wipe_identity(dir.path())
            .unwrap()
            .expect("identity was persisted");
        assert_eq!(marker.wiped_cluster_id, cluster_a);
        assert!(marker
            .removed_files
            .iter()
            .any(|path| path.ends_with(IDENTITY_FILENAME)));
        assert_eq!(NodeIdentity::load(dir.path()).unwrap(), None);
        // Second wipe is a no-op.
        assert_eq!(wipe_identity(dir.path()).unwrap(), None);
        // After a wipe the node may join another cluster.
        let cluster_b = ClusterId::new_random();
        let identity = NodeIdentity::provision(dir.path(), cluster_b, &mut test_csprng()).unwrap();
        assert_eq!(identity.cluster_id, cluster_b);
    }

    #[test]
    fn locality_round_trips_canonical_text() {
        let locality: Locality = "region=us-central,zone=a".parse().unwrap();
        assert_eq!(locality.tiers().len(), 2);
        assert_eq!(locality.get("region"), Some("us-central"));
        assert_eq!(locality.get("zone"), Some("a"));
        assert_eq!(locality.get("rack"), None);
        assert_eq!(locality.to_string(), "region=us-central,zone=a");
        // Full section 13.7 hierarchy with lenient whitespace.
        let full: Locality = " region=r1 , zone=z , rack=rk , node=n ".parse().unwrap();
        assert_eq!(full.to_string(), "region=r1,zone=z,rack=rk,node=n");
        // Empty text is an empty locality.
        assert!("".parse::<Locality>().unwrap().tiers().is_empty());
    }

    #[test]
    fn locality_rejects_malformed_input() {
        assert!(matches!(
            "region".parse::<Locality>(),
            Err(LocalityParseError::InvalidTier(_))
        ));
        assert!(matches!(
            "region=".parse::<Locality>(),
            Err(LocalityParseError::EmptyComponent(_))
        ));
        assert!(matches!(
            "=a".parse::<Locality>(),
            Err(LocalityParseError::EmptyComponent(_))
        ));
        assert!(matches!(
            "region=a,region=b".parse::<Locality>(),
            Err(LocalityParseError::DuplicateKey(_))
        ));
    }

    #[test]
    fn locality_serializes_as_canonical_string() {
        let locality: Locality = "region=us-central,zone=a".parse().unwrap();
        let json = serde_json::to_string(&locality).unwrap();
        assert_eq!(json, "\"region=us-central,zone=a\"");
        let back: Locality = serde_json::from_str(&json).unwrap();
        assert_eq!(back, locality);
    }

    #[test]
    fn build_version_comes_from_the_build_environment() {
        let version = BuildVersion::current();
        assert_eq!(version.version, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn node_state_serializes_by_stable_variant_name() {
        assert_eq!(serde_json::to_string(&NodeState::Up).unwrap(), "\"Up\"");
        assert_eq!(
            serde_json::to_string(&NodeState::Decommissioned).unwrap(),
            "\"Decommissioned\""
        );
        let back: NodeState = serde_json::from_str("\"Draining\"").unwrap();
        assert_eq!(back, NodeState::Draining);
    }

    #[test]
    fn concurrent_first_boots_agree_on_one_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let barrier = std::sync::Barrier::new(4);
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        NodeIdentity::load_or_create(&path, &mut test_csprng())
                    })
                })
                .collect();
            let identities: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
            let first = identities[0].as_ref().unwrap().clone();
            for identity in &identities {
                assert_eq!(identity.as_ref().unwrap(), &first);
            }
        });
    }

    fn version_info(protocol: (u32, u32), log: (u32, u32), snapshot: (u32, u32)) -> VersionInfo {
        VersionInfo {
            binary_version: "test".to_owned(),
            protocol_min: protocol.0,
            protocol_max: protocol.1,
            log_format_min: log.0,
            log_format_max: log.1,
            snapshot_format_min: snapshot.0,
            snapshot_format_max: snapshot.1,
            feature_set: BTreeSet::new(),
        }
    }

    fn sample_descriptor() -> NodeDescriptor {
        NodeDescriptor {
            node_id: NodeId::new_random(),
            rpc_address: "127.0.0.1:7100".to_owned(),
            locality: "region=us-central,zone=a".parse().unwrap(),
            capacity: NodeCapacity::default(),
            state: NodeState::Up,
            version: BuildVersion::current(),
            version_info: VersionInfo::current(),
        }
    }

    #[test]
    fn current_version_info_advertises_a_self_compatible_window() {
        let current = VersionInfo::current();
        assert_eq!(current.binary_version, env!("CARGO_PKG_VERSION"));
        assert!(current.protocol_min <= current.protocol_max);
        assert!(current.log_format_min <= current.log_format_max);
        assert!(current.snapshot_format_min <= current.snapshot_format_max);
        // The declared ranges mirror the format authorities of record.
        assert_eq!(
            current.log_format_min,
            mongreldb_log::envelope::MIN_SUPPORTED_FORMAT_VERSION
        );
        assert_eq!(
            current.log_format_max,
            mongreldb_log::envelope::COMMAND_ENVELOPE_FORMAT_VERSION
        );
        current.is_compatible_with(&current).unwrap();
    }

    #[test]
    fn compatibility_is_range_overlap_in_every_dimension() {
        let base = version_info((1, 2), (1, 2), (1, 2));
        // Identical and nested ranges overlap.
        base.is_compatible_with(&base.clone()).unwrap();
        base.is_compatible_with(&version_info((2, 2), (1, 1), (1, 2)))
            .unwrap();
        // Each dimension is checked; the first mismatch names the range.
        let error = base
            .is_compatible_with(&version_info((3, 4), (1, 2), (1, 2)))
            .unwrap_err();
        assert!(matches!(error, Incompatibility::ProtocolVersion { .. }));
        let error = base
            .is_compatible_with(&version_info((1, 2), (3, 4), (1, 2)))
            .unwrap_err();
        assert!(matches!(error, Incompatibility::LogFormat { .. }));
        let error = base
            .is_compatible_with(&version_info((1, 2), (1, 2), (3, 4)))
            .unwrap_err();
        assert!(matches!(error, Incompatibility::SnapshotFormat { .. }));
        // Malformed ranges (min > max) fail closed.
        let error = base
            .is_compatible_with(&version_info((2, 1), (1, 2), (1, 2)))
            .unwrap_err();
        assert!(matches!(error, Incompatibility::ProtocolVersion { .. }));
    }

    #[test]
    fn n_and_n_minus_1_interoperate_but_n_plus_1_skew_does_not() {
        // Section 17 window: the N binary accepts [N-1, N], the N-1 binary
        // accepts [N-1, N-1]; both directions overlap.
        let n_minus_1 = version_info((1, 1), (1, 1), (1, 1));
        let n = version_info((1, 2), (1, 2), (1, 2));
        n.is_compatible_with(&n_minus_1).unwrap();
        n_minus_1.is_compatible_with(&n).unwrap();
        // A two-version skew does not overlap and fails closed.
        let n_plus_1 = version_info((2, 3), (2, 3), (2, 3));
        assert!(matches!(
            n_minus_1.is_compatible_with(&n_plus_1).unwrap_err(),
            Incompatibility::ProtocolVersion { .. }
        ));
    }

    #[test]
    fn missing_advertisement_fails_closed_against_a_real_one() {
        // A descriptor written before Stage 2H decodes with zeroed ranges,
        // which overlap no real advertisement (section 4.10).
        let legacy = VersionInfo::default();
        let error = legacy
            .is_compatible_with(&VersionInfo::current())
            .unwrap_err();
        assert!(matches!(error, Incompatibility::ProtocolVersion { .. }));
        // Two legacy descriptors are mutually compatible: range overlap is
        // symmetric and says nothing about unsent fields.
        legacy.is_compatible_with(&VersionInfo::default()).unwrap();
    }

    #[test]
    fn descriptor_without_version_info_decodes_with_a_default() {
        let descriptor = sample_descriptor();
        let mut value = serde_json::to_value(&descriptor).unwrap();
        // A Stage 2A descriptor carried no version advertisement.
        value.as_object_mut().unwrap().remove("version_info");
        let decoded: NodeDescriptor = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.version_info, VersionInfo::default());
        assert_eq!(decoded.node_id, descriptor.node_id);
    }

    #[test]
    fn descriptor_with_version_info_round_trips() {
        let descriptor = sample_descriptor();
        let json = serde_json::to_vec(&descriptor).unwrap();
        let decoded: NodeDescriptor = serde_json::from_slice(&json).unwrap();
        assert_eq!(decoded, descriptor);
    }

    #[test]
    fn descriptor_still_fails_closed_on_unknown_fields() {
        let descriptor = sample_descriptor();
        let mut value = serde_json::to_value(&descriptor).unwrap();
        value["unexpected"] = serde_json::json!(1);
        assert!(serde_json::from_value::<NodeDescriptor>(value).is_err());
    }
}
