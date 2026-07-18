//! Cluster bootstrap workflows (spec section 11.1, S2A-002).
//!
//! This module is the library form of the operator commands; the CLI wiring
//! lands with the server crate:
//!
//! ```text
//! mongreldb cluster init     -> cluster_init
//! mongreldb cluster join     -> cluster_join
//! mongreldb cluster status   -> cluster_status
//! mongreldb node drain       -> node_drain
//! mongreldb node remove      -> node_remove
//! ```
//!
//! `cluster init` creates the cluster ID (adopting an already-persisted
//! identity's cluster ID when present), the initial membership, the single
//! database Raft group descriptor, and the cluster trust configuration
//! (S2A-002). `cluster join` validates an invite (cluster ID, member
//! endpoints, trust material) and refuses to run when a persisted identity
//! names a different cluster (S2A-001, [`ClusterError::ClusterIdentityMismatch`]).
//!
//! Trust material is PEM: callers may supply PEMs via [`TrustConfig::from_pems`]
//! or mint a bootstrap CA + first node cert with [`TrustConfig::generate`].
//! The node key is never logged: [`TrustConfig`]'s `Debug` implementation
//! redacts it, and [`cluster_status`] returns a key-free [`TrustSummary`].
//!
//! Every durable write is atomic (synced temporary file, rename, directory
//! fsync; see [`crate::node`]), every durable file carries a format version
//! that fails closed when unknown (spec section 4.10), and all mutating
//! workflows serialize on a bootstrap lock file so concurrent bootstrap
//! attempts on one directory cannot interleave.

use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::{ClusterId, DatabaseId, NodeId, RaftGroupId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::node::{
    cluster_meta_dir, decode_json, encode_json, mint_id, read_meta_file, wall_clock_now,
    write_meta_atomic, BuildVersion, ClusterError, Csprng, Locality, NodeCapacity, NodeDescriptor,
    NodeIdentity, NodeState,
};

/// Name of the bootstrap record file inside `cluster-meta/`.
pub const CLUSTER_RECORD_FILENAME: &str = "cluster.json";
/// Name of the trust material file inside `cluster-meta/`.
pub const TRUST_FILENAME: &str = "trust.json";
/// Name of the join record file inside `cluster-meta/`.
pub const JOIN_RECORD_FILENAME: &str = "join.json";
/// Name of the transient bootstrap lock file inside `cluster-meta/`.
pub const BOOTSTRAP_LOCK_FILENAME: &str = "bootstrap.lock";
/// The cluster record format version this build writes.
pub const CLUSTER_RECORD_FORMAT_VERSION: u32 = 1;
/// The trust file format version this build writes.
pub const TRUST_FORMAT_VERSION: u32 = 1;
/// The join record format version this build writes.
pub const JOIN_RECORD_FORMAT_VERSION: u32 = 1;

/// Cluster CA/trust configuration (S2A-002).
///
/// Trust PEMs may be operator-supplied ([`Self::from_pems`]) or minted
/// ([`Self::generate`]). `node_key_pem` is the node's private key: it is
/// redacted from `Debug` output and must never be logged by callers.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustConfig {
    /// PEM-armored cluster CA certificate chain.
    pub ca_cert_pem: String,
    /// PEM-armored node certificate.
    pub node_cert_pem: String,
    /// PEM-armored node private key. Never log this value.
    pub node_key_pem: String,
    /// Nodes admitted to the cluster; an empty list admits no peers.
    pub allowed_node_ids: Vec<NodeId>,
}

impl TrustConfig {
    /// Build a trust configuration from existing PEM material, validating it.
    pub fn from_pems(
        ca_cert_pem: String,
        node_cert_pem: String,
        node_key_pem: String,
        allowed_node_ids: Vec<NodeId>,
    ) -> Result<Self, ClusterError> {
        let config = Self {
            ca_cert_pem,
            node_cert_pem,
            node_key_pem,
            allowed_node_ids,
        };
        config.validate()?;
        Ok(config)
    }

    /// Mint a fresh cluster CA and a node certificate for the first admitted
    /// node id (CN/SAN = [`crate::network::node_cert_name`]). Remaining
    /// `allowed_node_ids` are admitted for handshake identity checks; mint
    /// per-node certs for them via ops tooling as they join.
    ///
    /// Production deployments may still supply PEMs via [`Self::from_pems`];
    /// this path is for bootstrap and test fixtures without external PKI.
    pub fn generate(allowed_node_ids: Vec<NodeId>) -> Result<Self, ClusterError> {
        if allowed_node_ids.is_empty() {
            return Err(ClusterError::InvalidTrustMaterial(
                "allowed_node_ids is empty; an empty list admits no peers",
            ));
        }
        let ca_key = rcgen::KeyPair::generate()
            .map_err(|_| ClusterError::InvalidTrustMaterial("CA key generation failed"))?;
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new())
            .map_err(|_| ClusterError::InvalidTrustMaterial("CA certificate params invalid"))?;
        ca_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "MongrelDB Cluster CA");
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
            rcgen::KeyUsagePurpose::DigitalSignature,
        ];
        let ca_cert = ca_params
            .self_signed(&ca_key)
            .map_err(|_| ClusterError::InvalidTrustMaterial("CA self-sign failed"))?;
        let ca_cert_pem = ca_cert.pem();

        let node_id = allowed_node_ids[0];
        let node_name = crate::network::node_cert_name(&node_id);
        let node_key = rcgen::KeyPair::generate()
            .map_err(|_| ClusterError::InvalidTrustMaterial("node key generation failed"))?;
        let mut node_params = rcgen::CertificateParams::new(vec![node_name.clone()])
            .map_err(|_| ClusterError::InvalidTrustMaterial("node certificate params invalid"))?;
        node_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, node_name);
        node_params.key_usages = vec![
            rcgen::KeyUsagePurpose::DigitalSignature,
            rcgen::KeyUsagePurpose::KeyEncipherment,
        ];
        node_params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let node_cert = node_params
            .signed_by(&node_key, &ca_cert, &ca_key)
            .map_err(|_| ClusterError::InvalidTrustMaterial("node certificate signing failed"))?;
        let config = Self {
            ca_cert_pem,
            node_cert_pem: node_cert.pem(),
            node_key_pem: node_key.serialize_pem(),
            allowed_node_ids,
        };
        config.validate()?;
        Ok(config)
    }

    /// Fail-closed structural validation: every PEM must carry armor, and
    /// the allowed-node list must admit at least one peer.
    pub fn validate(&self) -> Result<(), ClusterError> {
        for (pem, what) in [
            (&self.ca_cert_pem, "ca_cert_pem is not PEM armored"),
            (&self.node_cert_pem, "node_cert_pem is not PEM armored"),
            (&self.node_key_pem, "node_key_pem is not PEM armored"),
        ] {
            if !(pem.contains("-----BEGIN ") && pem.contains("-----END ")) {
                return Err(ClusterError::InvalidTrustMaterial(what));
            }
        }
        if self.allowed_node_ids.is_empty() {
            return Err(ClusterError::InvalidTrustMaterial(
                "allowed_node_ids is empty; an empty list admits no peers",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for TrustConfig {
    /// The node private key is redacted; certificates are public material.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrustConfig")
            .field("ca_cert_pem", &self.ca_cert_pem)
            .field("node_cert_pem", &self.node_cert_pem)
            .field("node_key_pem", &"<redacted>")
            .field("allowed_node_ids", &self.allowed_node_ids)
            .finish()
    }
}

/// The key-free view of a [`TrustConfig`] safe to hand to status reporters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrustSummary {
    /// PEM-armored cluster CA certificate chain.
    pub ca_cert_pem: String,
    /// PEM-armored node certificate.
    pub node_cert_pem: String,
    /// Nodes admitted to the cluster.
    pub allowed_node_ids: Vec<NodeId>,
    /// Whether a node private key is persisted (never the key itself).
    pub has_node_key: bool,
}

impl From<&TrustConfig> for TrustSummary {
    fn from(config: &TrustConfig) -> Self {
        Self {
            ca_cert_pem: config.ca_cert_pem.clone(),
            node_cert_pem: config.node_cert_pem.clone(),
            allowed_node_ids: config.allowed_node_ids.clone(),
            has_node_key: !config.node_key_pem.is_empty(),
        }
    }
}

/// The single database Raft group created by `cluster init` (S2A-002). Stage
/// 2 replicates one logical database as exactly one consensus group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseGroupDescriptor {
    /// The logical database replicated by the group.
    pub database_id: DatabaseId,
    /// The consensus group replicating it.
    pub raft_group_id: RaftGroupId,
    /// Initial voting members.
    pub voter_ids: Vec<NodeId>,
}

/// Durable bootstrap record written by `cluster init`: cluster ID, initial
/// membership, and the single database Raft group descriptor (S2A-002).
///
/// This is the bootstrap-time view of membership; the replicated meta group
/// takes ownership in Stage 3A. Unknown fields and unknown format versions
/// fail closed (spec section 4.10).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterRecord {
    /// Durable format version; see [`CLUSTER_RECORD_FORMAT_VERSION`].
    pub format_version: u32,
    /// Cluster this record bootstraps.
    pub cluster_id: ClusterId,
    /// Wall-clock time the record was written (informational).
    pub created_at: HlcTimestamp,
    /// Initial membership, first the bootstrapping node itself.
    pub members: Vec<NodeDescriptor>,
    /// The single database Raft group.
    pub database_group: DatabaseGroupDescriptor,
}

/// Durable record written by `cluster join`: the validated invite this node
/// booted from. Membership acceptance itself is a replicated meta-group
/// operation (Stage 2F).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinRecord {
    /// Durable format version; see [`JOIN_RECORD_FORMAT_VERSION`].
    pub format_version: u32,
    /// Cluster this node joined.
    pub cluster_id: ClusterId,
    /// Validated member endpoints from the invite.
    pub member_endpoints: Vec<String>,
    /// Wall-clock time the node joined (informational).
    pub joined_at: HlcTimestamp,
}

/// Parameters of `cluster init`.
#[derive(Clone, Debug)]
pub struct InitRequest {
    /// This node's advertised RPC address (`host:port`).
    pub rpc_address: String,
    /// This node's locality tiers (spec section 13.7).
    pub locality: Locality,
    /// This node's advertised capacity.
    pub capacity: NodeCapacity,
    /// Caller-supplied trust material (validated before use).
    pub trust: TrustConfig,
}

/// Result of `cluster init`.
#[derive(Clone, Debug)]
pub struct InitReport {
    /// The node's identity (created or adopted).
    pub identity: NodeIdentity,
    /// The durable bootstrap record that was written.
    pub record: ClusterRecord,
}

/// A validated `cluster join` invite: cluster ID, member endpoints, and
/// trust material.
#[derive(Clone, Debug)]
pub struct JoinInvite {
    /// Cluster to join.
    pub cluster_id: ClusterId,
    /// Endpoints of existing members (`host:port`).
    pub member_endpoints: Vec<String>,
    /// Caller-supplied trust material (validated before use).
    pub trust: TrustConfig,
}

/// Result of `cluster join`.
#[derive(Clone, Debug)]
pub struct JoinReport {
    /// The node's identity, provisioned for the invited cluster.
    pub identity: NodeIdentity,
    /// The durable join record that was written.
    pub record: JoinRecord,
}

/// The `cluster status` view: identity, membership, and group descriptors.
#[derive(Clone, Debug)]
pub struct ClusterStatus {
    /// This node's identity.
    pub identity: NodeIdentity,
    /// Membership from the bootstrap record (empty on a joined node until the
    /// meta group lands in Stage 2F/3A).
    pub membership: Vec<NodeDescriptor>,
    /// Known member endpoints (bootstrap record or validated invite).
    pub member_endpoints: Vec<String>,
    /// The single database Raft group descriptor, when initialized here.
    pub database_group: Option<DatabaseGroupDescriptor>,
    /// Key-free view of the persisted trust material, when present.
    pub trust: Option<TrustSummary>,
}

/// `cluster init`: create the cluster on this node (S2A-002).
///
/// Creates the cluster ID (or adopts the one of an already-persisted
/// identity), the initial membership containing this node, the single
/// database Raft group descriptor, and persists the trust configuration.
/// Running init on an already-bootstrapped node fails closed with
/// [`ClusterError::AlreadyBootstrapped`]; reprovisioning requires
/// [`crate::node::wipe_identity`].
pub fn cluster_init(
    node_data: &Path,
    request: &InitRequest,
    csprng: Csprng<'_>,
) -> Result<InitReport, ClusterError> {
    if request.rpc_address.trim().is_empty() {
        return Err(ClusterError::InvalidInvite("init rpc_address is empty"));
    }
    request.trust.validate()?;
    let _lock = BootstrapLock::acquire(node_data)?;
    if load_cluster_record(node_data)?.is_some() || load_join_record(node_data)?.is_some() {
        return Err(ClusterError::AlreadyBootstrapped {
            cluster_id: NodeIdentity::load(node_data)?
                .map(|identity| identity.cluster_id)
                .unwrap_or(ClusterId::ZERO),
        });
    }
    // Adopt a persisted identity (and its cluster ID) when the node already
    // minted one; otherwise mint a fresh cluster ID. A node whose persisted
    // identity names a different cluster was never the intent of init: the
    // operator reprovisions with an explicit wipe.
    let identity = match NodeIdentity::load(node_data)? {
        Some(identity) => identity,
        None => {
            let cluster_id = ClusterId::from_bytes(mint_id(csprng)?);
            NodeIdentity::provision(node_data, cluster_id, csprng)?
        }
    };
    let self_descriptor = NodeDescriptor {
        node_id: identity.node_id,
        rpc_address: request.rpc_address.clone(),
        locality: request.locality.clone(),
        capacity: request.capacity,
        state: NodeState::Up,
        version: BuildVersion::current(),
        version_info: crate::node::VersionInfo::current(),
    };
    let record = ClusterRecord {
        format_version: CLUSTER_RECORD_FORMAT_VERSION,
        cluster_id: identity.cluster_id,
        created_at: wall_clock_now(),
        members: vec![self_descriptor],
        database_group: DatabaseGroupDescriptor {
            database_id: DatabaseId::from_bytes(mint_id(csprng)?),
            raft_group_id: RaftGroupId::from_bytes(mint_id(csprng)?),
            voter_ids: vec![identity.node_id],
        },
    };
    write_trust(node_data, &request.trust)?;
    write_cluster_record(node_data, &record)?;
    Ok(InitReport { identity, record })
}

/// `cluster join`: validate an invite and provision this node for the
/// invited cluster.
///
/// Fails closed with [`ClusterError::ClusterIdentityMismatch`] when a
/// persisted identity binds the node to a different cluster (S2A-001), and
/// with [`ClusterError::AlreadyBootstrapped`] when the node already ran init
/// or join.
pub fn cluster_join(
    node_data: &Path,
    invite: &JoinInvite,
    csprng: Csprng<'_>,
) -> Result<JoinReport, ClusterError> {
    validate_invite(invite)?;
    let _lock = BootstrapLock::acquire(node_data)?;
    if load_cluster_record(node_data)?.is_some() || load_join_record(node_data)?.is_some() {
        return Err(ClusterError::AlreadyBootstrapped {
            cluster_id: NodeIdentity::load(node_data)?
                .map(|identity| identity.cluster_id)
                .unwrap_or(ClusterId::ZERO),
        });
    }
    let identity = NodeIdentity::provision(node_data, invite.cluster_id, csprng)?;
    let record = JoinRecord {
        format_version: JOIN_RECORD_FORMAT_VERSION,
        cluster_id: invite.cluster_id,
        member_endpoints: invite.member_endpoints.clone(),
        joined_at: wall_clock_now(),
    };
    write_trust(node_data, &invite.trust)?;
    let meta_dir = cluster_meta_dir(node_data);
    write_meta_atomic(
        &meta_dir,
        JOIN_RECORD_FILENAME,
        &encode_json(JOIN_RECORD_FILENAME, &record)?,
    )?;
    Ok(JoinReport { identity, record })
}

/// `cluster status`: identity, membership, and group descriptors.
///
/// A node that minted an identity but never ran init/join reports empty
/// membership; a directory without any identity fails with
/// [`ClusterError::NotInitialized`].
pub fn cluster_status(node_data: &Path) -> Result<ClusterStatus, ClusterError> {
    let identity = NodeIdentity::load(node_data)?.ok_or(ClusterError::NotInitialized)?;
    let record = load_cluster_record(node_data)?;
    let join = load_join_record(node_data)?;
    let trust = load_trust(node_data)?;
    let membership = record
        .as_ref()
        .map(|r| r.members.clone())
        .unwrap_or_default();
    let member_endpoints = record
        .as_ref()
        .map(|r| r.members.iter().map(|m| m.rpc_address.clone()).collect())
        .or_else(|| join.as_ref().map(|j| j.member_endpoints.clone()))
        .unwrap_or_default();
    Ok(ClusterStatus {
        identity,
        membership,
        member_endpoints,
        database_group: record.map(|r| r.database_group),
        trust: trust.as_ref().map(TrustSummary::from),
    })
}

/// `node drain`: move a member from `Up` to `Draining` in the persisted
/// membership record.
pub fn node_drain(node_data: &Path, node_id: NodeId) -> Result<NodeDescriptor, ClusterError> {
    let _lock = BootstrapLock::acquire(node_data)?;
    let mut record = load_cluster_record(node_data)?.ok_or(ClusterError::NotInitialized)?;
    let member = record
        .members
        .iter_mut()
        .find(|member| member.node_id == node_id)
        .ok_or(ClusterError::NodeNotFound { node: node_id })?;
    if member.state != NodeState::Up {
        return Err(ClusterError::InvalidNodeStateTransition {
            node: node_id,
            from: member.state,
            to: NodeState::Draining,
        });
    }
    member.state = NodeState::Draining;
    let updated = member.clone();
    write_cluster_record(node_data, &record)?;
    Ok(updated)
}

/// The token `node remove` requires as explicit confirmation: SHA-256 over a
/// domain separator and the cluster/node identifiers, hex-encoded. An
/// operator obtains it out of band (the CLI prints it) and pastes it back.
pub fn removal_confirmation_token(cluster_id: ClusterId, node_id: NodeId) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"mongreldb/node-remove/v1");
    hasher.update(cluster_id.as_bytes());
    hasher.update(node_id.as_bytes());
    let digest: [u8; 32] = hasher.finalize().into();
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// `node remove`: move a member to `Decommissioned` in the persisted
/// membership record. Requires the [`removal_confirmation_token`] as explicit
/// confirmation; permitted from `Up`, `Draining`, or `Down`.
pub fn node_remove(
    node_data: &Path,
    node_id: NodeId,
    confirmation_token: &str,
) -> Result<NodeDescriptor, ClusterError> {
    let _lock = BootstrapLock::acquire(node_data)?;
    let mut record = load_cluster_record(node_data)?.ok_or(ClusterError::NotInitialized)?;
    if confirmation_token != removal_confirmation_token(record.cluster_id, node_id) {
        return Err(ClusterError::InvalidConfirmationToken);
    }
    let member = record
        .members
        .iter_mut()
        .find(|member| member.node_id == node_id)
        .ok_or(ClusterError::NodeNotFound { node: node_id })?;
    if !matches!(
        member.state,
        NodeState::Up | NodeState::Draining | NodeState::Down
    ) {
        return Err(ClusterError::InvalidNodeStateTransition {
            node: node_id,
            from: member.state,
            to: NodeState::Decommissioned,
        });
    }
    member.state = NodeState::Decommissioned;
    let updated = member.clone();
    write_cluster_record(node_data, &record)?;
    Ok(updated)
}

fn validate_invite(invite: &JoinInvite) -> Result<(), ClusterError> {
    if invite.cluster_id == ClusterId::ZERO {
        return Err(ClusterError::InvalidInvite(
            "cluster id is the reserved zero value",
        ));
    }
    if invite.member_endpoints.is_empty() {
        return Err(ClusterError::InvalidInvite("no member endpoints supplied"));
    }
    if invite
        .member_endpoints
        .iter()
        .any(|endpoint| endpoint.trim().is_empty())
    {
        return Err(ClusterError::InvalidInvite("empty member endpoint"));
    }
    invite.trust.validate()?;
    Ok(())
}

fn check_version(file: &'static str, found: u32, max: u32) -> Result<(), ClusterError> {
    if found == 0 || found > max {
        return Err(ClusterError::UnsupportedFormatVersion {
            file,
            found,
            min: 1,
            max,
        });
    }
    Ok(())
}

fn load_cluster_record(node_data: &Path) -> Result<Option<ClusterRecord>, ClusterError> {
    let path = cluster_meta_dir(node_data).join(CLUSTER_RECORD_FILENAME);
    let Some(bytes) = read_meta_file(&path)? else {
        return Ok(None);
    };
    let record: ClusterRecord = decode_json(CLUSTER_RECORD_FILENAME, &bytes)?;
    check_version(
        CLUSTER_RECORD_FILENAME,
        record.format_version,
        CLUSTER_RECORD_FORMAT_VERSION,
    )?;
    Ok(Some(record))
}

fn write_cluster_record(node_data: &Path, record: &ClusterRecord) -> Result<(), ClusterError> {
    write_meta_atomic(
        &cluster_meta_dir(node_data),
        CLUSTER_RECORD_FILENAME,
        &encode_json(CLUSTER_RECORD_FILENAME, record)?,
    )?;
    Ok(())
}

fn load_join_record(node_data: &Path) -> Result<Option<JoinRecord>, ClusterError> {
    let path = cluster_meta_dir(node_data).join(JOIN_RECORD_FILENAME);
    let Some(bytes) = read_meta_file(&path)? else {
        return Ok(None);
    };
    let record: JoinRecord = decode_json(JOIN_RECORD_FILENAME, &bytes)?;
    check_version(
        JOIN_RECORD_FILENAME,
        record.format_version,
        JOIN_RECORD_FORMAT_VERSION,
    )?;
    Ok(Some(record))
}

/// Versioned on-disk form of the persisted [`TrustConfig`].
///
/// Trust material is persisted so the node runtime can configure TLS at boot
/// without an operator round-trip. The file holds the node private key;
/// operators are expected to keep `cluster-meta/` on owner-only storage, as
/// they already must for the PEM files the material was supplied from.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustEnvelope {
    format_version: u32,
    trust: TrustConfig,
}

fn write_trust(node_data: &Path, trust: &TrustConfig) -> Result<(), ClusterError> {
    let meta_dir = cluster_meta_dir(node_data);
    let bytes = encode_json(
        TRUST_FILENAME,
        &TrustEnvelope {
            format_version: TRUST_FORMAT_VERSION,
            trust: trust.clone(),
        },
    )?;
    write_meta_atomic(&meta_dir, TRUST_FILENAME, &bytes)?;
    Ok(())
}

fn load_trust(node_data: &Path) -> Result<Option<TrustConfig>, ClusterError> {
    let path = cluster_meta_dir(node_data).join(TRUST_FILENAME);
    let Some(bytes) = read_meta_file(&path)? else {
        return Ok(None);
    };
    let envelope: TrustEnvelope = decode_json(TRUST_FILENAME, &bytes)?;
    check_version(
        TRUST_FILENAME,
        envelope.format_version,
        TRUST_FORMAT_VERSION,
    )?;
    Ok(Some(envelope.trust))
}

/// Held for the duration of one bootstrap workflow; released on drop.
/// Serializes concurrent init/join/drain/remove attempts on one directory.
struct BootstrapLock {
    path: PathBuf,
}

impl BootstrapLock {
    fn acquire(node_data: &Path) -> Result<Self, ClusterError> {
        let meta_dir = cluster_meta_dir(node_data);
        fs::create_dir_all(&meta_dir)?;
        let path = meta_dir.join(BOOTSTRAP_LOCK_FILENAME);
        let mut file = match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                return Err(ClusterError::BootstrapInProgress(path));
            }
            Err(error) => return Err(error.into()),
        };
        // The lock is the file's existence; the content is a best-effort
        // marker for operators diagnosing a stale lock after a crash.
        let marker = format!("pid {} at {}\n", std::process::id(), wall_clock_now());
        let _ = file.write_all(marker.as_bytes());
        let _ = file.sync_all();
        Ok(Self { path })
    }
}

impl Drop for BootstrapLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
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

    const CA_PEM: &str = "-----BEGIN CERTIFICATE-----\nY2E=\n-----END CERTIFICATE-----\n";
    const CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nbm9kZQ==\n-----END CERTIFICATE-----\n";
    const KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nc2VjcmV0\n-----END PRIVATE KEY-----\n";

    fn trust(node_ids: Vec<NodeId>) -> TrustConfig {
        TrustConfig::from_pems(
            CA_PEM.to_owned(),
            CERT_PEM.to_owned(),
            KEY_PEM.to_owned(),
            node_ids,
        )
        .unwrap()
    }

    fn init_request(node_id: NodeId) -> InitRequest {
        InitRequest {
            rpc_address: "127.0.0.1:8453".to_owned(),
            locality: "region=test,zone=a".parse().unwrap(),
            capacity: NodeCapacity {
                cpu: 8,
                memory_bytes: 32 * 1024 * 1024 * 1024,
                disk_bytes: 1024 * 1024 * 1024 * 1024,
            },
            trust: trust(vec![node_id]),
        }
    }

    fn invite(cluster_id: ClusterId, node_id: NodeId) -> JoinInvite {
        JoinInvite {
            cluster_id,
            member_endpoints: vec!["127.0.0.1:8453".to_owned()],
            trust: trust(vec![node_id]),
        }
    }

    #[test]
    fn trust_generate_mints_valid_pems() {
        let node = NodeId::new_random();
        let trust = TrustConfig::generate(vec![node]).expect("generate");
        assert!(trust.ca_cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(trust.node_cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(trust.node_key_pem.contains("BEGIN"));
        assert_eq!(trust.allowed_node_ids, vec![node]);
        // Must be loadable as real mTLS material.
        crate::network::TlsConfig::from_trust(&trust).expect("tls from generated trust");
    }

    #[test]
    fn trust_generate_rejects_empty_admission() {
        let error = TrustConfig::generate(vec![]).unwrap_err();
        assert!(
            matches!(error, ClusterError::InvalidTrustMaterial(_)),
            "unexpected: {error}"
        );
    }

    #[test]
    fn trust_validation_rejects_unarmored_or_empty_material() {
        let node = NodeId::new_random();
        for (ca, cert, key, ids) in [
            ("garbage", CERT_PEM, KEY_PEM, vec![node]),
            (CA_PEM, "garbage", KEY_PEM, vec![node]),
            (CA_PEM, CERT_PEM, "garbage", vec![node]),
            (CA_PEM, CERT_PEM, KEY_PEM, vec![]),
        ] {
            let result =
                TrustConfig::from_pems(ca.to_owned(), cert.to_owned(), key.to_owned(), ids);
            assert!(
                matches!(result, Err(ClusterError::InvalidTrustMaterial(_))),
                "unexpected: {result:?}"
            );
        }
    }

    #[test]
    fn trust_debug_never_prints_the_node_key() {
        let config = trust(vec![NodeId::new_random()]);
        let debug = format!("{config:?}");
        assert!(!debug.contains("c2VjcmV0"), "key material leaked: {debug}");
        assert!(debug.contains("<redacted>"));
        let summary = TrustSummary::from(&config);
        assert!(summary.has_node_key);
        assert_eq!(summary.allowed_node_ids, config.allowed_node_ids);
    }

    #[test]
    fn init_creates_identity_record_group_and_trust() {
        let dir = tempfile::tempdir().unwrap();
        let node_id = NodeId::new_random();
        let report = cluster_init(dir.path(), &init_request(node_id), &mut test_csprng()).unwrap();
        assert_ne!(report.identity.cluster_id, ClusterId::ZERO);
        assert_ne!(report.identity.node_id, NodeId::ZERO);
        assert_eq!(report.record.cluster_id, report.identity.cluster_id);
        assert_eq!(report.record.format_version, CLUSTER_RECORD_FORMAT_VERSION);
        assert_eq!(report.record.members.len(), 1);
        let member = &report.record.members[0];
        assert_eq!(member.node_id, report.identity.node_id);
        assert_eq!(member.state, NodeState::Up);
        assert_eq!(member.rpc_address, "127.0.0.1:8453");
        assert_eq!(member.locality.get("region"), Some("test"));
        assert_eq!(member.version.version, env!("CARGO_PKG_VERSION"));
        assert_ne!(report.record.database_group.database_id, DatabaseId::ZERO);
        assert_ne!(
            report.record.database_group.raft_group_id,
            RaftGroupId::ZERO
        );
        assert_eq!(
            report.record.database_group.voter_ids,
            vec![report.identity.node_id]
        );

        let meta = dir.path().join(crate::node::CLUSTER_META_DIR);
        assert!(meta.join(crate::node::IDENTITY_FILENAME).is_file());
        assert!(meta.join(CLUSTER_RECORD_FILENAME).is_file());
        assert!(meta.join(TRUST_FILENAME).is_file());
        // The bootstrap lock is released when the workflow completes.
        assert!(!meta.join(BOOTSTRAP_LOCK_FILENAME).exists());

        let status = cluster_status(dir.path()).unwrap();
        assert_eq!(status.identity, report.identity);
        assert_eq!(status.membership, report.record.members);
        assert_eq!(status.member_endpoints, vec!["127.0.0.1:8453".to_owned()]);
        assert_eq!(status.database_group, Some(report.record.database_group));
        let trust_summary = status.trust.expect("trust persisted");
        assert!(trust_summary.has_node_key);
        assert_eq!(trust_summary.ca_cert_pem, CA_PEM);
    }

    #[test]
    fn init_adopts_an_already_persisted_identity() {
        let dir = tempfile::tempdir().unwrap();
        let identity = NodeIdentity::load_or_create(dir.path(), &mut test_csprng()).unwrap();
        let report = cluster_init(
            dir.path(),
            &init_request(identity.node_id),
            &mut test_csprng(),
        )
        .unwrap();
        assert_eq!(report.identity, identity);
    }

    #[test]
    fn init_twice_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let node_id = NodeId::new_random();
        cluster_init(dir.path(), &init_request(node_id), &mut test_csprng()).unwrap();
        let error =
            cluster_init(dir.path(), &init_request(node_id), &mut test_csprng()).unwrap_err();
        assert!(
            matches!(error, ClusterError::AlreadyBootstrapped { .. }),
            "unexpected: {error}"
        );
    }

    #[test]
    fn init_rejects_invalid_request() {
        let dir = tempfile::tempdir().unwrap();
        let node_id = NodeId::new_random();
        let mut request = init_request(node_id);
        request.rpc_address = "  ".to_owned();
        let error = cluster_init(dir.path(), &request, &mut test_csprng()).unwrap_err();
        assert!(
            matches!(error, ClusterError::InvalidInvite(_)),
            "unexpected: {error}"
        );
        let mut request = init_request(node_id);
        request.trust.allowed_node_ids.clear();
        let error = cluster_init(dir.path(), &request, &mut test_csprng()).unwrap_err();
        assert!(
            matches!(error, ClusterError::InvalidTrustMaterial(_)),
            "unexpected: {error}"
        );
    }

    #[test]
    fn join_persists_identity_trust_and_join_record() {
        let dir = tempfile::tempdir().unwrap();
        let cluster_id = ClusterId::new_random();
        let node_id = NodeId::new_random();
        let report =
            cluster_join(dir.path(), &invite(cluster_id, node_id), &mut test_csprng()).unwrap();
        assert_eq!(report.identity.cluster_id, cluster_id);
        assert_eq!(report.record.cluster_id, cluster_id);
        assert_eq!(
            report.record.member_endpoints,
            vec!["127.0.0.1:8453".to_owned()]
        );
        let status = cluster_status(dir.path()).unwrap();
        assert_eq!(status.identity, report.identity);
        assert!(status.membership.is_empty());
        assert_eq!(status.member_endpoints, vec!["127.0.0.1:8453".to_owned()]);
        assert!(status.database_group.is_none());
        assert!(status.trust.is_some());
    }

    #[test]
    fn join_refuses_a_mismatched_persisted_identity() {
        let dir = tempfile::tempdir().unwrap();
        let cluster_a = ClusterId::new_random();
        let node_id = NodeId::new_random();
        NodeIdentity::provision(dir.path(), cluster_a, &mut test_csprng()).unwrap();
        let cluster_b = ClusterId::new_random();
        let error =
            cluster_join(dir.path(), &invite(cluster_b, node_id), &mut test_csprng()).unwrap_err();
        assert!(
            matches!(
                error,
                ClusterError::ClusterIdentityMismatch { persisted, requested }
                    if persisted == cluster_a && requested == cluster_b
            ),
            "unexpected: {error}"
        );
        // The node remains bound to cluster A and can still join it.
        let report =
            cluster_join(dir.path(), &invite(cluster_a, node_id), &mut test_csprng()).unwrap();
        assert_eq!(report.identity.cluster_id, cluster_a);
    }

    #[test]
    fn join_validates_the_invite() {
        let dir = tempfile::tempdir().unwrap();
        let cluster_id = ClusterId::new_random();
        let node_id = NodeId::new_random();
        for invite in [
            JoinInvite {
                cluster_id: ClusterId::ZERO,
                ..invite(cluster_id, node_id)
            },
            JoinInvite {
                member_endpoints: vec![],
                ..invite(cluster_id, node_id)
            },
            JoinInvite {
                member_endpoints: vec![" ".to_owned()],
                ..invite(cluster_id, node_id)
            },
        ] {
            let error = cluster_join(dir.path(), &invite, &mut test_csprng()).unwrap_err();
            assert!(
                matches!(error, ClusterError::InvalidInvite(_)),
                "unexpected: {error}"
            );
        }
        let mut bad = invite(cluster_id, node_id);
        bad.trust.node_cert_pem = "not pem".to_owned();
        let error = cluster_join(dir.path(), &bad, &mut test_csprng()).unwrap_err();
        assert!(
            matches!(error, ClusterError::InvalidTrustMaterial(_)),
            "unexpected: {error}"
        );
    }

    #[test]
    fn join_twice_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let cluster_id = ClusterId::new_random();
        let node_id = NodeId::new_random();
        cluster_join(dir.path(), &invite(cluster_id, node_id), &mut test_csprng()).unwrap();
        let error =
            cluster_join(dir.path(), &invite(cluster_id, node_id), &mut test_csprng()).unwrap_err();
        assert!(
            matches!(error, ClusterError::AlreadyBootstrapped { .. }),
            "unexpected: {error}"
        );
    }

    #[test]
    fn status_requires_an_identity() {
        let dir = tempfile::tempdir().unwrap();
        let error = cluster_status(dir.path()).unwrap_err();
        assert!(
            matches!(error, ClusterError::NotInitialized),
            "unexpected: {error}"
        );
    }

    #[test]
    fn drain_moves_up_to_draining_and_persists() {
        let dir = tempfile::tempdir().unwrap();
        let node_id = NodeId::new_random();
        let report = cluster_init(dir.path(), &init_request(node_id), &mut test_csprng()).unwrap();
        let self_id = report.identity.node_id;
        let updated = node_drain(dir.path(), self_id).unwrap();
        assert_eq!(updated.state, NodeState::Draining);
        assert_eq!(
            cluster_status(dir.path()).unwrap().membership[0].state,
            NodeState::Draining
        );
        // Draining again is not a legal transition.
        let error = node_drain(dir.path(), self_id).unwrap_err();
        assert!(
            matches!(
                error,
                ClusterError::InvalidNodeStateTransition {
                    from: NodeState::Draining,
                    to: NodeState::Draining,
                    ..
                }
            ),
            "unexpected: {error}"
        );
        // Unknown nodes are reported.
        let error = node_drain(dir.path(), NodeId::new_random()).unwrap_err();
        assert!(
            matches!(error, ClusterError::NodeNotFound { .. }),
            "unexpected: {error}"
        );
    }

    #[test]
    fn remove_requires_the_confirmation_token() {
        let dir = tempfile::tempdir().unwrap();
        let node_id = NodeId::new_random();
        let report = cluster_init(dir.path(), &init_request(node_id), &mut test_csprng()).unwrap();
        let self_id = report.identity.node_id;
        let error = node_remove(dir.path(), self_id, "not-the-token").unwrap_err();
        assert!(
            matches!(error, ClusterError::InvalidConfirmationToken),
            "unexpected: {error}"
        );
        let token = removal_confirmation_token(report.identity.cluster_id, self_id);
        assert_eq!(token.len(), 64);
        let updated = node_remove(dir.path(), self_id, &token).unwrap();
        assert_eq!(updated.state, NodeState::Decommissioned);
        assert_eq!(
            cluster_status(dir.path()).unwrap().membership[0].state,
            NodeState::Decommissioned
        );
        // Removing twice is not a legal transition.
        let error = node_remove(dir.path(), self_id, &token).unwrap_err();
        assert!(
            matches!(
                error,
                ClusterError::InvalidNodeStateTransition {
                    from: NodeState::Decommissioned,
                    to: NodeState::Decommissioned,
                    ..
                }
            ),
            "unexpected: {error}"
        );
    }

    #[test]
    fn drain_and_remove_require_bootstrap() {
        let dir = tempfile::tempdir().unwrap();
        NodeIdentity::load_or_create(dir.path(), &mut test_csprng()).unwrap();
        let node = NodeId::new_random();
        let error = node_drain(dir.path(), node).unwrap_err();
        assert!(
            matches!(error, ClusterError::NotInitialized),
            "unexpected: {error}"
        );
        let error = node_remove(dir.path(), node, "token").unwrap_err();
        assert!(
            matches!(error, ClusterError::NotInitialized),
            "unexpected: {error}"
        );
    }

    #[test]
    fn unknown_cluster_record_version_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let node_id = NodeId::new_random();
        cluster_init(dir.path(), &init_request(node_id), &mut test_csprng()).unwrap();
        let path = dir
            .path()
            .join(crate::node::CLUSTER_META_DIR)
            .join(CLUSTER_RECORD_FILENAME);
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        value["format_version"] = serde_json::json!(42);
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        let error = cluster_status(dir.path()).unwrap_err();
        assert!(
            matches!(
                error,
                ClusterError::UnsupportedFormatVersion {
                    file: CLUSTER_RECORD_FILENAME,
                    found: 42,
                    ..
                }
            ),
            "unexpected: {error}"
        );
    }

    #[test]
    fn concurrent_bootstrap_race_yields_exactly_one_winner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let node_id = NodeId::new_random();
        let barrier = std::sync::Barrier::new(4);
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        cluster_init(&path, &init_request(node_id), &mut test_csprng())
                    })
                })
                .collect();
            let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
            let winners = results.iter().filter(|r| r.is_ok()).count();
            assert_eq!(winners, 1, "exactly one init may win: {results:?}");
            for result in &results {
                if let Err(error) = result {
                    assert!(
                        matches!(
                            error,
                            ClusterError::AlreadyBootstrapped { .. }
                                | ClusterError::BootstrapInProgress(_)
                        ),
                        "unexpected: {error}"
                    );
                }
            }
        });
        // The final durable state is exactly one coherent bootstrap.
        let status = cluster_status(dir.path()).unwrap();
        assert_eq!(status.membership.len(), 1);
        assert_eq!(status.membership[0].state, NodeState::Up);
        assert!(status.database_group.is_some());
    }
}
