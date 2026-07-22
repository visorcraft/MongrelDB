//! Server-hosted [`NodeRuntime`] product path (Stage 2/3).
//!
//! When the operator enables cluster mode (`--cluster-node-data` /
//! `MONGRELDB_CLUSTER_NODE_DATA`), the daemon loads the provisioned
//! [`NodeIdentity`], starts a live [`NodeRuntime`], and wires admin SQL
//! (TRANSFER LEADER / SPLIT TABLET / MERGE TABLETS) through it.
//!
//! Standalone mode (the default) never starts a runtime. Mutating cluster
//! admin commands that need a live group fail closed with
//! `"cluster runtime not running"` so operators are not misled by silent
//! `"accepted"` stubs.
//!
//! # Trust / transport
//!
//! Production builds load mTLS material from the node's persisted
//! `cluster-meta/trust.json` and construct the runtime with
//! [`NodeRuntimeConfig`]'s mTLS security mode. Plaintext cluster transport is
//! refused on the production path. Under `cfg(test)` (and the
//! `dangerous-test-transport` feature on `mongreldb-cluster`),
//! `MONGRELDB_CLUSTER_PLAINTEXT_TEST=1` / `plaintext_test: true` remains a
//! non-production escape hatch for loopback tests.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use mongreldb_cluster::bootstrap::{self, ClusterStatus, TrustConfig};
use mongreldb_cluster::network::{TlsConfig, TransportConfig, TransportSecurity};
use mongreldb_cluster::node::{ClusterError, NodeIdentity};
use mongreldb_cluster::runtime::{
    GroupTiming, MetaMembership, NodeInternalRpcClient, NodeRuntime, NodeRuntimeConfig,
    RuntimeError, RuntimeStatus,
};
use mongreldb_cluster::tablet::Key;
use mongreldb_log::commit_log::ExecutionControl;
use mongreldb_types::ids::{NodeId, TabletId};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex;

/// Operator / test configuration for starting a live cluster node runtime.
#[derive(Clone, Debug)]
pub struct ClusterRuntimeOptions {
    /// Node data root (identity, `cluster-meta/`, tablets, meta group).
    pub node_data: PathBuf,
    /// RPC listen address (`host:port`; port `0` is resolved to a free port).
    pub rpc_listen: String,
    /// Use plaintext transport (test-only; non-production).
    pub plaintext_test: bool,
    /// Install fast raft election timers (tests).
    pub fast_timing: bool,
}

impl ClusterRuntimeOptions {
    /// Build options from an explicit node-data path and optional listen
    /// address, consulting environment variables for the rest.
    ///
    /// - `MONGRELDB_CLUSTER_RPC_LISTEN` — default listen when `rpc_listen` is `None`
    /// - `MONGRELDB_CLUSTER_PLAINTEXT_TEST=1` — plaintext transport + fast timing
    pub fn resolve(node_data: PathBuf, rpc_listen: Option<String>) -> Self {
        let rpc_listen = rpc_listen
            .or_else(|| std::env::var("MONGRELDB_CLUSTER_RPC_LISTEN").ok())
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "127.0.0.1:17443".to_owned());
        let plaintext_test = env_flag_true("MONGRELDB_CLUSTER_PLAINTEXT_TEST");
        Self {
            node_data,
            rpc_listen,
            plaintext_test,
            // Fast timers only in the plaintext test escape hatch so
            // production defaults stay conservative.
            fast_timing: plaintext_test,
        }
    }
}

/// Shared handle to a live [`NodeRuntime`] stored on `AppState`.
///
/// The runtime lives behind a mutex so admin handlers can call async
/// mut methods (`split_tablet` / `merge_tablets`). `shutdown` takes the
/// runtime out once so graceful stop is single-shot even when the handle
/// is cloned across the router and `ServerControl`.
#[derive(Clone)]
pub struct ClusterRuntimeHandle {
    inner: Arc<Mutex<Option<NodeRuntime>>>,
    node_data: PathBuf,
}

/// Failures starting or driving the server-hosted runtime.
#[derive(Debug)]
pub enum ClusterRuntimeError {
    /// Bootstrap / identity layer.
    Cluster(ClusterError),
    /// Node runtime layer.
    Runtime(RuntimeError),
    /// Operator configuration (listen address, trust material, …).
    Config(String),
}

impl std::fmt::Display for ClusterRuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cluster(error) => write!(f, "{error}"),
            Self::Runtime(error) => write!(f, "{error}"),
            Self::Config(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for ClusterRuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Cluster(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::Config(_) => None,
        }
    }
}

impl From<ClusterError> for ClusterRuntimeError {
    fn from(error: ClusterError) -> Self {
        Self::Cluster(error)
    }
}

impl From<RuntimeError> for ClusterRuntimeError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}

impl From<std::io::Error> for ClusterRuntimeError {
    fn from(error: std::io::Error) -> Self {
        Self::Config(format!("cluster runtime I/O: {error}"))
    }
}

impl ClusterRuntimeHandle {
    /// Load identity + bootstrap state from `options.node_data`, start the
    /// runtime, and wrap it. Fails closed when the node has not been
    /// provisioned (`cluster init` / `cluster join`).
    pub async fn start(options: ClusterRuntimeOptions) -> Result<Self, ClusterRuntimeError> {
        let _identity =
            NodeIdentity::load(&options.node_data)?.ok_or(ClusterError::NotInitialized)?;
        let status = bootstrap::cluster_status(&options.node_data)?;
        let listen = resolve_listen_address(&options.rpc_listen)?;
        let security = resolve_security(&options.node_data, options.plaintext_test)?;
        let peers = peers_from_status(&status, &listen);
        let meta = meta_membership_from_status(&status, &listen);

        let config = NodeRuntimeConfig {
            node_data: options.node_data.clone(),
            security,
            transport: transport_config(options.plaintext_test),
            listen_address: listen.clone(),
            rpc_address: Some(listen),
            peers,
            meta,
            timing: options.fast_timing.then(fast_timing),
        };
        let runtime = NodeRuntime::start(config).await?;
        Ok(Self {
            inner: Arc::new(Mutex::new(Some(runtime))),
            node_data: options.node_data,
        })
    }

    /// Node data directory this runtime was started from.
    pub fn node_data(&self) -> &Path {
        &self.node_data
    }

    /// Whether the runtime is still live (not yet shut down).
    pub async fn is_live(&self) -> bool {
        self.inner.lock().await.is_some()
    }

    /// JSON view of [`RuntimeStatus`] for admin status surfaces.
    pub async fn runtime_status_json(&self) -> Result<Value, ClusterRuntimeError> {
        let guard = self.inner.lock().await;
        let runtime = guard
            .as_ref()
            .ok_or_else(|| ClusterRuntimeError::Config("cluster runtime not running".into()))?;
        Ok(runtime_status_to_json(&runtime.status()))
    }

    /// `TRANSFER LEADER <tablet> TO <node>` against a live tablet group.
    pub async fn transfer_leader(
        &self,
        tablet_id: TabletId,
        to: NodeId,
    ) -> Result<Value, ClusterRuntimeError> {
        let guard = self.inner.lock().await;
        let runtime = guard
            .as_ref()
            .ok_or_else(|| ClusterRuntimeError::Config("cluster runtime not running".into()))?;
        let descriptor = runtime.tablet_descriptor(tablet_id).ok_or_else(|| {
            ClusterRuntimeError::Config(format!(
                "this node hosts no live tablet group for tablet {tablet_id}"
            ))
        })?;
        let target = descriptor.replica_on(to).ok_or_else(|| {
            ClusterRuntimeError::Config(format!("node {to} is not a replica of tablet {tablet_id}"))
        })?;
        let group = runtime.tablet_group(tablet_id).ok_or_else(|| {
            ClusterRuntimeError::Config(format!(
                "this node hosts no live tablet group for tablet {tablet_id}"
            ))
        })?;
        group
            .transfer_leader(target.raft_node_id, LEADER_TIMEOUT)
            .await
            .map_err(|error| ClusterRuntimeError::Config(error.to_string()))?;
        Ok(json!({
            "command": "TRANSFER LEADER",
            "tablet_id": tablet_id.to_string(),
            "to": to.to_string(),
            "status": "ok",
            "target_raft_node_id": target.raft_node_id,
        }))
    }

    /// `SPLIT TABLET` against a live runtime (requires meta + hosted tablet).
    pub async fn split_tablet(
        &self,
        tablet_id: TabletId,
        at_key_hex: Option<String>,
    ) -> Result<Value, ClusterRuntimeError> {
        let split_key = match at_key_hex.as_deref() {
            Some(hex) => Some(parse_key_hex(hex)?),
            None => None,
        };
        let mut guard = self.inner.lock().await;
        let runtime = guard
            .as_mut()
            .ok_or_else(|| ClusterRuntimeError::Config("cluster runtime not running".into()))?;
        if runtime.tablet_group(tablet_id).is_none() {
            return Err(ClusterRuntimeError::Config(format!(
                "this node hosts no live tablet group for tablet {tablet_id}"
            )));
        }
        let control = ExecutionControl::default();
        let published = runtime.split_tablet(tablet_id, split_key, &control).await?;
        Ok(json!({
            "command": "SPLIT TABLET",
            "tablet_id": tablet_id.to_string(),
            "at_key_hex": at_key_hex,
            "status": "ok",
            "published": true,
            "left_tablet_id": published.children[0].tablet_id.to_string(),
            "right_tablet_id": published.children[1].tablet_id.to_string(),
        }))
    }

    /// `MERGE TABLETS` against a live runtime (requires meta + hosted pair).
    pub async fn merge_tablets(
        &self,
        left: TabletId,
        right: TabletId,
    ) -> Result<Value, ClusterRuntimeError> {
        let mut guard = self.inner.lock().await;
        let runtime = guard
            .as_mut()
            .ok_or_else(|| ClusterRuntimeError::Config("cluster runtime not running".into()))?;
        for tablet_id in [left, right] {
            if runtime.tablet_group(tablet_id).is_none() {
                return Err(ClusterRuntimeError::Config(format!(
                    "this node hosts no live tablet group for tablet {tablet_id}"
                )));
            }
        }
        let control = ExecutionControl::default();
        let published = runtime.merge_tablets(left, right, &control).await?;
        Ok(json!({
            "command": "MERGE TABLETS",
            "left": left.to_string(),
            "right": right.to_string(),
            "status": "ok",
            "published": true,
            "replacement_tablet_id": published.replacement.tablet_id.to_string(),
        }))
    }

    /// Direct access for tests that need to seed tablets onto the live runtime.
    pub fn runtime_mutex(&self) -> Arc<Mutex<Option<NodeRuntime>>> {
        Arc::clone(&self.inner)
    }

    /// Installs one authenticated node-internal RPC service.
    pub async fn attach_internal_rpc_handler(
        &self,
        service_id: u32,
        handler: Arc<dyn mongreldb_cluster::network::InternalRpcHandler>,
    ) -> Result<(), ClusterRuntimeError> {
        let guard = self.inner.lock().await;
        let runtime = guard
            .as_ref()
            .ok_or_else(|| ClusterRuntimeError::Config("cluster runtime not running".into()))?;
        runtime.attach_internal_rpc_handler(service_id, handler);
        Ok(())
    }

    /// Gets a cloneable client for authenticated internal fan-out.
    pub async fn internal_rpc_client(&self) -> Result<NodeInternalRpcClient, ClusterRuntimeError> {
        let guard = self.inner.lock().await;
        let runtime = guard
            .as_ref()
            .ok_or_else(|| ClusterRuntimeError::Config("cluster runtime not running".into()))?;
        Ok(runtime.internal_rpc_client())
    }

    /// Resolve the applied engine core for one locally hosted tablet.
    ///
    /// Uses `try_lock` so fragment/AI workers never block the async runtime
    /// mutex; contention fails closed with `None` (caller retries / errors).
    pub fn tablet_database_try(
        &self,
        tablet_id: TabletId,
    ) -> Option<std::sync::Arc<mongreldb_core::Database>> {
        let guard = self.inner.try_lock().ok()?;
        let runtime = guard.as_ref()?;
        let sink = runtime.tablet_sink(tablet_id)?;
        let locked = sink.lock().ok()?;
        locked.database()
    }

    /// Hosted tablet ids on this node (tablet-id order), for public data routing.
    pub async fn tablet_ids(&self) -> Result<Vec<TabletId>, ClusterRuntimeError> {
        let guard = self.inner.lock().await;
        let runtime = guard
            .as_ref()
            .ok_or_else(|| ClusterRuntimeError::Config("cluster runtime not running".into()))?;
        Ok(runtime.tablet_ids())
    }

    /// Current applied opaque tablet rows (local replica view) for a hosted tablet.
    pub async fn tablet_rows(
        &self,
        tablet_id: TabletId,
    ) -> Result<std::collections::BTreeMap<Key, Vec<u8>>, ClusterRuntimeError> {
        let guard = self.inner.lock().await;
        let runtime = guard
            .as_ref()
            .ok_or_else(|| ClusterRuntimeError::Config("cluster runtime not running".into()))?;
        Ok(runtime.tablet_rows(tablet_id)?)
    }

    /// Typed user-table rows of a bound hosted tablet.
    pub async fn tablet_typed_rows(
        &self,
        tablet_id: TabletId,
    ) -> Result<mongreldb_consensus::engine_sink::TypedTabletRows, ClusterRuntimeError> {
        let guard = self.inner.lock().await;
        let runtime = guard
            .as_ref()
            .ok_or_else(|| ClusterRuntimeError::Config("cluster runtime not running".into()))?;
        Ok(runtime.tablet_typed_rows(tablet_id)?)
    }

    /// Bind a hosted tablet to a typed user-table schema (P0.3).
    pub async fn bind_tablet_user_table(
        &self,
        tablet_id: TabletId,
        binding: mongreldb_consensus::engine_sink::TabletTableBinding,
    ) -> Result<mongreldb_consensus::engine_sink::TabletTableBinding, ClusterRuntimeError> {
        let guard = self.inner.lock().await;
        let runtime = guard
            .as_ref()
            .ok_or_else(|| ClusterRuntimeError::Config("cluster runtime not running".into()))?;
        Ok(runtime.bind_tablet_user_table(tablet_id, binding)?)
    }

    /// Current typed binding for a hosted tablet, if any.
    pub async fn tablet_table_binding(
        &self,
        tablet_id: TabletId,
    ) -> Result<Option<mongreldb_consensus::engine_sink::TabletTableBinding>, ClusterRuntimeError>
    {
        let guard = self.inner.lock().await;
        let runtime = guard
            .as_ref()
            .ok_or_else(|| ClusterRuntimeError::Config("cluster runtime not running".into()))?;
        Ok(runtime.tablet_table_binding(tablet_id))
    }

    /// Raft-propose upserts into a hosted tablet's opaque MVCC keyspace.
    ///
    /// The local replica must be the group leader; [`ConsensusError::NotLeader`]
    /// surfaces through [`ClusterRuntimeError::Runtime`] with a leader hint.
    pub async fn write_tablet_rows(
        &self,
        tablet_id: TabletId,
        entries: &[(Key, Vec<u8>)],
    ) -> Result<mongreldb_consensus::group::GroupCommitReceipt, ClusterRuntimeError> {
        let guard = self.inner.lock().await;
        let runtime = guard
            .as_ref()
            .ok_or_else(|| ClusterRuntimeError::Config("cluster runtime not running".into()))?;
        let control = ExecutionControl::default();
        Ok(runtime
            .write_tablet_rows(tablet_id, entries, &control)
            .await?)
    }

    /// Raft-propose typed user-table mutations (`COMMAND_TYPE_TABLET_WRITE`).
    pub async fn write_tablet_ops(
        &self,
        tablet_id: TabletId,
        operations: Vec<mongreldb_consensus::engine_sink::TabletWriteOperation>,
    ) -> Result<mongreldb_consensus::group::GroupCommitReceipt, ClusterRuntimeError> {
        let guard = self.inner.lock().await;
        let runtime = guard
            .as_ref()
            .ok_or_else(|| ClusterRuntimeError::Config("cluster runtime not running".into()))?;
        let control = ExecutionControl::default();
        Ok(runtime
            .write_tablet_ops(tablet_id, operations, &control)
            .await?)
    }

    /// Graceful shutdown: stop the runtime once. Additional calls are no-ops.
    pub async fn shutdown(&self) -> Result<(), ClusterRuntimeError> {
        let runtime = {
            let mut guard = self.inner.lock().await;
            guard.take()
        };
        if let Some(runtime) = runtime {
            runtime.shutdown().await?;
        }
        Ok(())
    }
}

const LEADER_TIMEOUT: Duration = Duration::from_secs(15);

fn env_flag_true(name: &str) -> bool {
    match std::env::var(name) {
        Ok(value) => {
            let value = value.trim();
            value == "1" || value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("yes")
        }
        Err(_) => false,
    }
}

/// Bind-and-release when the operator requested port 0 so peer tables carry a
/// concrete address before the runtime starts.
fn resolve_listen_address(listen: &str) -> Result<String, ClusterRuntimeError> {
    let trimmed = listen.trim();
    if trimmed.is_empty() {
        return Err(ClusterRuntimeError::Config(
            "cluster RPC listen address is empty".into(),
        ));
    }
    // Port 0 (or host:0) — allocate a free port up front.
    if trimmed.ends_with(":0") {
        let listener = std::net::TcpListener::bind(trimmed)?;
        return Ok(listener.local_addr()?.to_string());
    }
    Ok(trimmed.to_owned())
}

fn resolve_security(
    node_data: &Path,
    plaintext_test: bool,
) -> Result<TransportSecurity, ClusterRuntimeError> {
    if plaintext_test {
        // Explicit `plaintext_test: true` is the library test API (integration
        // tests + unit tests). Production entry points must not set it:
        // - `NodeRuntimeConfig::production` rejects plaintext (P1.3-T1)
        // - `mongreldb-server` main refuses env-based plaintext unless
        //   `dangerous-test-transport` / cfg(test) (P1.3-T4; see main.rs)
        // Integration tests compile this lib without `cfg(test)`, so the gate
        // cannot rely on cfg alone for the explicit option.
        return Ok(TransportSecurity::PlaintextForTesting);
    }
    let trust = load_persisted_trust(node_data)?.ok_or_else(|| {
        ClusterRuntimeError::Config(
            "cluster trust material missing under cluster-meta/trust.json; \
             run `mongreldb-server cluster init` first (production requires mTLS)"
                .into(),
        )
    })?;
    let tls = TlsConfig::from_trust(&trust).map_err(|error| {
        ClusterRuntimeError::Config(format!(
            "cluster trust PEMs are not usable for mTLS ({error}); \
             supply real certificates from `cluster init` (production requires mTLS)"
        ))
    })?;
    Ok(TransportSecurity::Mtls(tls))
}

/// On-disk envelope written by `bootstrap::write_trust` (private there).
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TrustEnvelope {
    format_version: u32,
    trust: TrustConfig,
}

fn load_persisted_trust(node_data: &Path) -> Result<Option<TrustConfig>, ClusterRuntimeError> {
    let path = node_data.join("cluster-meta").join("trust.json");
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(&path)?;
    let envelope: TrustEnvelope = serde_json::from_slice(&bytes).map_err(|error| {
        ClusterRuntimeError::Config(format!("cluster-meta/trust.json is corrupt: {error}"))
    })?;
    if envelope.format_version != 1 {
        return Err(ClusterRuntimeError::Config(format!(
            "cluster-meta/trust.json format version {} is unsupported",
            envelope.format_version
        )));
    }
    envelope
        .trust
        .validate()
        .map_err(ClusterRuntimeError::from)?;
    Ok(Some(envelope.trust))
}

fn peers_from_status(status: &ClusterStatus, self_listen: &str) -> Vec<(NodeId, String)> {
    if !status.membership.is_empty() {
        return status
            .membership
            .iter()
            .map(|member| {
                let address = if member.node_id == status.identity.node_id {
                    self_listen.to_owned()
                } else {
                    member.rpc_address.clone()
                };
                (member.node_id, address)
            })
            .collect();
    }
    // Join-only or partial bootstrap: at least advertise this node.
    vec![(status.identity.node_id, self_listen.to_owned())]
}

/// First product path: this node hosts meta when it is a database-group
/// voter. Sole-voter init bootstraps the pristine meta group; multi-voter
/// members reopen without re-bootstrap.
fn meta_membership_from_status(
    status: &ClusterStatus,
    self_listen: &str,
) -> Option<MetaMembership> {
    let group = status.database_group.as_ref()?;
    if !group.voter_ids.contains(&status.identity.node_id) {
        return None;
    }
    let bootstrap_voters = if group.voter_ids.len() == 1 {
        Some(vec![(status.identity.node_id, self_listen.to_owned())])
    } else {
        None
    };
    Some(MetaMembership {
        meta_group_id: group.raft_group_id,
        bootstrap_voters,
    })
}

fn transport_config(plaintext_test: bool) -> TransportConfig {
    if plaintext_test {
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
    } else {
        TransportConfig::default()
    }
}

fn fast_timing() -> GroupTiming {
    GroupTiming {
        heartbeat_interval: Duration::from_millis(100),
        election_timeout_min: Duration::from_millis(300),
        election_timeout_max: Duration::from_millis(600),
        install_snapshot_timeout: Duration::from_millis(2_000),
    }
}

fn parse_key_hex(text: &str) -> Result<Key, ClusterRuntimeError> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(ClusterRuntimeError::Config("split key hex is empty".into()));
    }
    if !trimmed.len().is_multiple_of(2) {
        return Err(ClusterRuntimeError::Config(
            "split key hex must have an even number of digits".into(),
        ));
    }
    let mut bytes = Vec::with_capacity(trimmed.len() / 2);
    let chars: Vec<char> = trimmed.chars().collect();
    for chunk in chars.chunks(2) {
        let hi = chunk[0].to_digit(16).ok_or_else(|| {
            ClusterRuntimeError::Config(format!("invalid split key hex `{text}`"))
        })?;
        let lo = chunk[1].to_digit(16).ok_or_else(|| {
            ClusterRuntimeError::Config(format!("invalid split key hex `{text}`"))
        })?;
        bytes.push(((hi << 4) | lo) as u8);
    }
    Ok(Key::from_bytes(bytes))
}

fn runtime_status_to_json(status: &RuntimeStatus) -> Value {
    json!({
        "live": true,
        "node_id": status.identity.node_id.to_string(),
        "cluster_id": status.identity.cluster_id.to_string(),
        "rpc_address": status.rpc_address,
        "meta_present": status.meta.is_some(),
        "meta": status.meta.as_ref().map(|meta| json!({
            "meta_group_id": meta.meta_group_id.to_string(),
            "metadata_version": meta.metadata_version.get(),
            "current_leader": meta.metrics.current_leader,
            "local_raft_node_id": meta.metrics.node_id,
        })),
        "tablet_count": status.tablets.len(),
        "tablets": status.tablets.iter().map(|tablet| json!({
            "tablet_id": tablet.tablet_id.to_string(),
            "raft_group_id": tablet.raft_group_id.to_string(),
            "state": tablet.state.to_string(),
            "replica_count": tablet.replicas.len(),
            "applied_index": tablet.applied.index,
            "current_leader": tablet.metrics.current_leader,
            "local_raft_node_id": tablet.metrics.node_id,
        })).collect::<Vec<_>>(),
    })
}

/// Resolve cluster node-data from CLI / environment. `None` means standalone.
pub fn cluster_node_data_from_env(cli_value: Option<String>) -> Option<PathBuf> {
    cli_value
        .or_else(|| std::env::var("MONGRELDB_CLUSTER_NODE_DATA").ok())
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
}

/// Whether plaintext cluster transport is admitted in this build.
///
/// Production binaries return `false` unless rebuilt with the
/// `dangerous-test-transport` feature. Package unit tests return `true`.
pub fn plaintext_cluster_transport_allowed() -> bool {
    cfg!(any(test, feature = "dangerous-test-transport"))
}

/// Production gate for plaintext cluster transport (P1.3-T4 / P1.3-X2).
///
/// The daemon calls this with [`plaintext_cluster_transport_allowed`] before
/// starting a runtime when `plaintext_test` was requested via env. The pure
/// form takes an explicit `allowed` so product tests can exercise the refuse
/// path without rebuilding under `cfg(test)`.
pub fn admit_plaintext_cluster_transport(
    plaintext_requested: bool,
    allowed: bool,
) -> Result<(), ClusterRuntimeError> {
    if plaintext_requested && !allowed {
        return Err(ClusterRuntimeError::Config(
            "plaintext cluster transport is refused on the production path; \
             configure mTLS via cluster-meta/trust.json (cluster init), or \
             rebuild with the dangerous-test-transport feature for \
             non-production tests only"
                .into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongreldb_cluster::bootstrap::{cluster_init, InitRequest};
    use mongreldb_cluster::node::{Locality, NodeCapacity};
    use tempfile::tempdir;

    const CA_PEM: &str = "-----BEGIN CERTIFICATE-----\nY2E=\n-----END CERTIFICATE-----\n";
    const CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nbm9kZQ==\n-----END CERTIFICATE-----\n";
    const KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nc2VjcmV0\n-----END PRIVATE KEY-----\n";

    fn bootstrap(data: &Path) -> NodeIdentity {
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
            rpc_address: "127.0.0.1:0".to_owned(),
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
    async fn plaintext_start_status_and_missing_tablet_errors() {
        let dir = tempdir().unwrap();
        let identity = bootstrap(dir.path());
        let listen = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().to_string()
        };
        let handle = ClusterRuntimeHandle::start(ClusterRuntimeOptions {
            node_data: dir.path().to_path_buf(),
            rpc_listen: listen.clone(),
            plaintext_test: true,
            fast_timing: true,
        })
        .await
        .expect("runtime starts after cluster init");

        assert!(handle.is_live().await);
        let status = handle.runtime_status_json().await.unwrap();
        assert_eq!(status["live"], true);
        assert_eq!(status["node_id"], identity.node_id.to_string());
        assert_eq!(status["rpc_address"], listen);
        assert_eq!(status["meta_present"], true);
        assert_eq!(status["tablet_count"], 0);

        let missing = TabletId::from_bytes([0xAB; 16]);
        let err = handle
            .transfer_leader(missing, identity.node_id)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("hosts no live tablet"),
            "transfer without tablet must fail closed: {err}"
        );
        let err = handle.split_tablet(missing, None).await.unwrap_err();
        assert!(
            err.to_string().contains("hosts no live tablet"),
            "split without tablet must fail closed: {err}"
        );

        handle.shutdown().await.unwrap();
        assert!(!handle.is_live().await);
    }

    #[tokio::test]
    async fn start_fails_closed_without_cluster_init() {
        let dir = tempdir().unwrap();
        match ClusterRuntimeHandle::start(ClusterRuntimeOptions {
            node_data: dir.path().to_path_buf(),
            rpc_listen: "127.0.0.1:0".into(),
            plaintext_test: true,
            fast_timing: true,
        })
        .await
        {
            Ok(_) => panic!("expected NotInitialized without cluster init"),
            Err(err) => assert!(
                matches!(
                    err,
                    ClusterRuntimeError::Cluster(ClusterError::NotInitialized)
                ) || err
                    .to_string()
                    .to_ascii_lowercase()
                    .contains("not initialized")
                    || err.to_string().contains("NotInitialized"),
                "expected NotInitialized, got {err}"
            ),
        }
    }

    #[test]
    fn parse_key_hex_round_trip() {
        let key = parse_key_hex("0a1b").unwrap();
        assert_eq!(key.as_bytes(), &[0x0a, 0x1b]);
        assert!(parse_key_hex("zz").is_err());
        assert!(parse_key_hex("abc").is_err());
    }

    #[test]
    fn plaintext_gate_open_under_package_tests() {
        // Production binaries compile this false (without the feature).
        // Package tests keep the loopback escape hatch open (P1.3-T4 / X2).
        assert!(plaintext_cluster_transport_allowed());
    }

    /// P1.3-X2: production gate refuses plaintext when the build disallows it.
    #[test]
    fn p13_x2_production_gate_refuses_plaintext_when_disallowed() {
        let err = admit_plaintext_cluster_transport(true, false).expect_err("must refuse");
        assert!(
            err.to_string().contains("plaintext cluster transport is refused"),
            "{err}"
        );
        // Allowed escape hatch (test builds / dangerous-test-transport).
        admit_plaintext_cluster_transport(true, true).expect("allowed when admitted");
        // Production mTLS path does not request plaintext.
        admit_plaintext_cluster_transport(false, false).expect("mTLS path never needs escape hatch");
    }

    /// P1.3-X2: production start path (`plaintext_test: false`) requires usable mTLS.
    #[tokio::test]
    async fn p13_x2_non_plaintext_start_requires_usable_mtls() {
        let dir = tempdir().unwrap();
        bootstrap(dir.path());
        let listen = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().to_string()
        };
        // Dummy bootstrap PEMs are not real X.509; production mTLS path fails closed.
        let err = match ClusterRuntimeHandle::start(ClusterRuntimeOptions {
            node_data: dir.path().to_path_buf(),
            rpc_listen: listen,
            plaintext_test: false,
            fast_timing: true,
        })
        .await
        {
            Ok(handle) => {
                let _ = handle.shutdown().await;
                panic!("dummy trust PEMs must fail mTLS construction");
            }
            Err(error) => error,
        };
        let message = err.to_string().to_ascii_lowercase();
        assert!(
            message.contains("mtls")
                || message.contains("trust")
                || message.contains("certificate")
                || message.contains("pem"),
            "expected mTLS/trust failure, got {err}"
        );
    }
}
