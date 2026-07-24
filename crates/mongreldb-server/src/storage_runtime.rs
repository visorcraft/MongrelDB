//! Authoritative server storage runtime (P0.2 / ADR-0001).
//!
//! The daemon holds exactly one of:
//! - [`ServerStorageRuntime::Standalone`] — ordinary single-node data plane
//!   over a local [`mongreldb_core::Database`]
//! - [`ServerStorageRuntime::Cluster`] — consensus/tablet data plane via
//!   [`crate::cluster_runtime::ClusterRuntimeHandle`]; ordinary public writes
//!   must not use a standalone WAL bypass
//!
//! Dual-root ownership (standalone user database + live cluster runtime as
//! peer data planes) is refused.
//!
//! The standalone types (`StandaloneRuntime`, `ServerStorageRuntime`,
//! `StorageRuntimeError`) are always compiled — the hot SQL path is typed over
//! them. The cluster pieces (`ClusterGatewayRuntime`, the `Cluster` variant,
//! and the cluster accessors) exist only with the `cluster` feature.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use mongreldb_core::Database;
#[cfg(feature = "cluster")]
use mongreldb_query::ai_retrieval::RemoteAiEndpoint;
#[cfg(feature = "cluster")]
use mongreldb_query::distributed::RemoteFragmentEndpoint;

#[cfg(feature = "cluster")]
use crate::cluster_runtime::ClusterRuntimeHandle;

/// Error when code asks the standalone data plane for a cluster process (or
/// the reverse).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageRuntimeError {
    message: String,
}

impl StorageRuntimeError {
    /// Build a fail-closed error.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Stable message for the cluster standalone-bypass refusal.
    pub fn cluster_refuses_standalone_bypass() -> Self {
        Self::new(
            "cluster mode refuses standalone AppState.db data-plane access; \
             public data operations are owned by consensus/tablet state",
        )
    }
}

impl std::fmt::Display for StorageRuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for StorageRuntimeError {}

impl From<StorageRuntimeError> for mongreldb_core::MongrelError {
    fn from(error: StorageRuntimeError) -> Self {
        mongreldb_core::MongrelError::Other(error.message)
    }
}

/// Standalone (single-node server) data plane.
#[derive(Clone)]
pub struct StandaloneRuntime {
    db: Arc<Database>,
}

impl StandaloneRuntime {
    /// Wrap an already-opened local database.
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Local engine handle.
    pub fn db(&self) -> &Arc<Database> {
        &self.db
    }

    /// Durable root path for this standalone core.
    pub fn root(&self) -> &Path {
        self.db.root()
    }
}

/// Cluster gateway / node data plane.
///
/// Holds the live [`ClusterRuntimeHandle`] and optional fragment/AI worker
/// endpoints installed on the authenticated internal RPC transport. Public
/// SQL/Kit/native user writes do not open a peer standalone WAL here.
#[cfg(feature = "cluster")]
#[derive(Clone)]
pub struct ClusterGatewayRuntime {
    handle: ClusterRuntimeHandle,
    fragment_endpoint: Option<Arc<RemoteFragmentEndpoint>>,
    ai_endpoint: Option<Arc<RemoteAiEndpoint>>,
}

#[cfg(feature = "cluster")]
impl ClusterGatewayRuntime {
    /// Wrap a started node runtime (workers may be installed later).
    pub fn new(handle: ClusterRuntimeHandle) -> Self {
        Self {
            handle,
            fragment_endpoint: None,
            ai_endpoint: None,
        }
    }

    /// Wrap a started node runtime that already has workers installed.
    pub fn with_workers(
        handle: ClusterRuntimeHandle,
        fragment_endpoint: Arc<RemoteFragmentEndpoint>,
        ai_endpoint: Arc<RemoteAiEndpoint>,
    ) -> Self {
        Self {
            handle,
            fragment_endpoint: Some(fragment_endpoint),
            ai_endpoint: Some(ai_endpoint),
        }
    }

    /// Live node runtime handle.
    pub fn handle(&self) -> &ClusterRuntimeHandle {
        &self.handle
    }

    /// Node data directory (identity, cluster-meta, tablet roots).
    pub fn node_data(&self) -> &Path {
        self.handle.node_data()
    }

    /// Installed fragment worker endpoint, when production start completed.
    pub fn fragment_endpoint(&self) -> Option<&Arc<RemoteFragmentEndpoint>> {
        self.fragment_endpoint.as_ref()
    }

    /// Installed AI worker endpoint, when production start completed.
    pub fn ai_endpoint(&self) -> Option<&Arc<RemoteAiEndpoint>> {
        self.ai_endpoint.as_ref()
    }

    /// Record worker endpoints after install (tests / staged startup).
    pub fn set_workers(
        &mut self,
        fragment_endpoint: Arc<RemoteFragmentEndpoint>,
        ai_endpoint: Arc<RemoteAiEndpoint>,
    ) {
        self.fragment_endpoint = Some(fragment_endpoint);
        self.ai_endpoint = Some(ai_endpoint);
    }

    /// Whether both production workers are installed.
    pub fn workers_installed(&self) -> bool {
        self.fragment_endpoint.is_some() && self.ai_endpoint.is_some()
    }
}

/// Single authoritative storage runtime for the HTTP/native daemon (P0.2).
#[derive(Clone)]
pub enum ServerStorageRuntime {
    /// Single-node server-owned standalone core.
    Standalone(StandaloneRuntime),
    /// Cluster node: public data owned by consensus/tablet state.
    #[cfg(feature = "cluster")]
    Cluster(ClusterGatewayRuntime),
}

impl ServerStorageRuntime {
    /// Construct the standalone variant.
    pub fn standalone(db: Arc<Database>) -> Self {
        Self::Standalone(StandaloneRuntime::new(db))
    }

    /// Construct the cluster variant (workers optional until installed).
    #[cfg(feature = "cluster")]
    pub fn cluster(handle: ClusterRuntimeHandle) -> Self {
        Self::Cluster(ClusterGatewayRuntime::new(handle))
    }

    /// Construct the cluster variant with workers already installed.
    #[cfg(feature = "cluster")]
    pub fn cluster_with_workers(
        handle: ClusterRuntimeHandle,
        fragment_endpoint: Arc<RemoteFragmentEndpoint>,
        ai_endpoint: Arc<RemoteAiEndpoint>,
    ) -> Self {
        Self::Cluster(ClusterGatewayRuntime::with_workers(
            handle,
            fragment_endpoint,
            ai_endpoint,
        ))
    }

    /// `true` when this process is a cluster node data plane. Always `false`
    /// in builds without the `cluster` feature.
    pub fn is_cluster(&self) -> bool {
        #[cfg(feature = "cluster")]
        {
            matches!(self, Self::Cluster(_))
        }
        #[cfg(not(feature = "cluster"))]
        {
            false
        }
    }

    /// `true` when this process is standalone.
    pub fn is_standalone(&self) -> bool {
        matches!(self, Self::Standalone(_))
    }

    /// Local standalone engine, when this is standalone mode.
    pub fn standalone_db(&self) -> Option<&Arc<Database>> {
        match self {
            Self::Standalone(rt) => Some(rt.db()),
            #[cfg(feature = "cluster")]
            Self::Cluster(_) => None,
        }
    }

    /// Require the standalone engine; fail closed in cluster mode so ordinary
    /// public writes cannot bypass Raft through `AppState.db`.
    pub fn require_standalone_db(&self) -> Result<&Arc<Database>, StorageRuntimeError> {
        self.standalone_db()
            .ok_or_else(StorageRuntimeError::cluster_refuses_standalone_bypass)
    }

    /// Live cluster handle, when this is cluster mode.
    #[cfg(feature = "cluster")]
    pub fn cluster_handle(&self) -> Option<&ClusterRuntimeHandle> {
        match self {
            Self::Cluster(rt) => Some(rt.handle()),
            Self::Standalone(_) => None,
        }
    }

    /// Cluster gateway pieces, when this is cluster mode.
    #[cfg(feature = "cluster")]
    pub fn cluster_gateway(&self) -> Option<&ClusterGatewayRuntime> {
        match self {
            Self::Cluster(rt) => Some(rt),
            Self::Standalone(_) => None,
        }
    }

    /// Mutable cluster gateway (worker install bookkeeping).
    #[cfg(feature = "cluster")]
    pub fn cluster_gateway_mut(&mut self) -> Option<&mut ClusterGatewayRuntime> {
        match self {
            Self::Cluster(rt) => Some(rt),
            Self::Standalone(_) => None,
        }
    }

    /// Durable path used for process-local server state (idempotency, pid
    /// hints). Standalone uses the database root; cluster uses node data.
    pub fn durable_path(&self) -> PathBuf {
        match self {
            Self::Standalone(rt) => rt.root().to_path_buf(),
            #[cfg(feature = "cluster")]
            Self::Cluster(rt) => rt.node_data().to_path_buf(),
        }
    }

    /// Mode name for health / capabilities surfaces.
    pub fn mode_name(&self) -> &'static str {
        match self {
            Self::Standalone(_) => "standalone",
            #[cfg(feature = "cluster")]
            Self::Cluster(_) => "cluster",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn standalone_exposes_db_cluster_refuses() {
        let dir = tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path()).unwrap());
        let standalone = ServerStorageRuntime::standalone(Arc::clone(&db));
        assert!(standalone.is_standalone());
        assert!(!standalone.is_cluster());
        assert!(Arc::ptr_eq(
            standalone.require_standalone_db().unwrap(),
            &db
        ));
        assert_eq!(standalone.mode_name(), "standalone");
    }

    #[test]
    fn cluster_runtime_enum_exists_and_has_no_standalone_db() {
        // Structural: Cluster variant holds gateway pieces without a peer
        // standalone Database field. Construction with a live runtime is
        // covered by cluster_runtime / storage integration tests.
        let dir = tempdir().unwrap();
        let _ = dir;
        let err = StorageRuntimeError::cluster_refuses_standalone_bypass();
        assert!(err.to_string().contains("cluster mode refuses"));
        let converted: mongreldb_core::MongrelError = err.into();
        assert!(converted.to_string().contains("cluster mode refuses"));
    }
}
