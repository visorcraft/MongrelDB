//! Cluster administration HTTP surface (spec section 11.1, S2A-002).
//!
//! The `/admin/cluster/*` endpoints expose the cluster crate's bootstrap and
//! membership workflows over the daemon's authenticated admin channel:
//!
//! ```text
//! GET  /admin/cluster/status        -> bootstrap::cluster_status
//! POST /admin/cluster/node/drain    -> bootstrap::node_drain
//! POST /admin/cluster/node/remove   -> bootstrap::node_remove
//! ```
//!
//! The node data directory is the database directory (`Database::root`); the
//! cluster crate keeps its durable records under `<db-dir>/cluster-meta/`. A
//! server whose database directory carries no cluster identity keeps working
//! exactly as before: the status endpoint reports `"standalone"` mode and the
//! mutating endpoints answer 409 Conflict.
//!
//! The `node remove` confirmation token is never served over HTTP and never
//! written to the audit log: operators obtain it out of band from the CLI
//! (`mongreldb-server node remove --data-dir <dir>` prints it), per the
//! cluster crate's contract. Trust material is equally strict — status
//! responses carry the key-free [`bootstrap::TrustSummary`] view only.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use mongreldb_cluster::bootstrap::{self, ClusterStatus};
use mongreldb_cluster::node::{ClusterError, NodeIdentity, VersionInfo};
use mongreldb_types::ids::NodeId;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{request_owner, require_admin, AppState, OptionalPrincipal};

/// Serialize one status component; these plain-data cluster types always
/// serialize, so a failure here is a bug, not an operator error.
fn component<T: serde::Serialize>(value: &T) -> Value {
    serde_json::to_value(value).expect("cluster status component serialization")
}

/// The key-free `cluster status` JSON view shared by the CLI and
/// `GET /admin/cluster/status`: identity, membership, descriptors, and this
/// binary's version advertisement (spec section 11.8).
pub fn cluster_status_json(status: &ClusterStatus) -> Value {
    let trust = status.trust.as_ref().map(|trust| {
        json!({
            "ca_cert_pem": component(&trust.ca_cert_pem),
            "node_cert_pem": component(&trust.node_cert_pem),
            "allowed_node_ids": component(&trust.allowed_node_ids),
            "has_node_key": trust.has_node_key,
        })
    });
    json!({
        "mode": "cluster",
        "identity": component(&status.identity),
        "membership": component(&status.membership),
        "member_endpoints": component(&status.member_endpoints),
        "database_group": component(&status.database_group),
        "trust": trust,
        "version_info": component(&VersionInfo::current()),
    })
}

/// The `cluster status` JSON view when the database directory holds no
/// cluster identity: the server runs exactly as before, standalone.
pub fn standalone_status_json() -> Value {
    json!({
        "mode": "standalone",
        "detail": "no cluster identity in the database directory; \
                   run `mongreldb-server cluster init` or `mongreldb-server cluster join` \
                   to bootstrap one",
        "version_info": component(&VersionInfo::current()),
    })
}

/// `cluster status` as one JSON report: the cluster view when bootstrapped,
/// the standalone view when no identity exists, or the underlying error
/// (corrupt or unsupported metadata fails closed).
pub fn status_report(node_data: &std::path::Path) -> Result<Value, ClusterError> {
    match bootstrap::cluster_status(node_data) {
        Ok(status) => Ok(cluster_status_json(&status)),
        Err(ClusterError::NotInitialized) => Ok(standalone_status_json()),
        Err(error) => Err(error),
    }
}

/// Map a cluster workflow error onto an HTTP status: bad operator input is
/// 400, a wrong confirmation token is 403, an unknown member is 404, state
/// and bootstrap conflicts are 409, and everything else stays 500 (the same
/// defense-in-depth shape as `status_for_error`).
pub(crate) fn status_for_cluster_error(error: &ClusterError) -> StatusCode {
    match error {
        ClusterError::InvalidInvite(_) | ClusterError::InvalidTrustMaterial(_) => {
            StatusCode::BAD_REQUEST
        }
        ClusterError::InvalidConfirmationToken => StatusCode::FORBIDDEN,
        ClusterError::NodeNotFound { .. } => StatusCode::NOT_FOUND,
        ClusterError::NotInitialized
        | ClusterError::AlreadyBootstrapped { .. }
        | ClusterError::ClusterIdentityMismatch { .. }
        | ClusterError::InvalidNodeStateTransition { .. }
        | ClusterError::BootstrapInProgress(_) => StatusCode::CONFLICT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Map a cluster workflow error onto an HTTP status + JSON error body.
fn cluster_error_response(error: &ClusterError) -> Response {
    (
        status_for_cluster_error(error),
        Json(json!({ "error": error.to_string() })),
    )
        .into_response()
}

/// Resolve the member a drain/remove targets: the explicit `node_id` request
/// field, else this node's own persisted identity. A standalone node has no
/// identity to default to, so the operation conflicts.
fn resolve_target_node(state: &AppState, requested: Option<&str>) -> Result<NodeId, Response> {
    match requested {
        Some(text) => text.parse::<NodeId>().map_err(|error| {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("invalid node_id `{text}`: {error}") })),
            )
                .into_response()
        }),
        None => match NodeIdentity::load(state.db.root()) {
            Ok(Some(identity)) => Ok(identity.node_id),
            Ok(None) => Err((
                StatusCode::CONFLICT,
                Json(json!({
                    "error": "node is standalone; no cluster identity exists \
                              (run `mongreldb-server cluster init` or `cluster join` first)",
                })),
            )
                .into_response()),
            Err(error) => Err(cluster_error_response(&error)),
        },
    }
}

/// `GET /admin/cluster/status` — identity, membership, node descriptors, and
/// version info; reports `standalone` when no cluster identity exists.
pub(crate) async fn status(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
) -> Response {
    if let Err(response) = require_admin(&state, &principal, "admin.cluster.status") {
        return *response;
    }
    match status_report(state.db.root()) {
        Ok(report) => Json(report).into_response(),
        Err(error) => {
            state.audit.record(
                request_owner(&state, &principal),
                "admin.cluster.status.fail",
                error.to_string(),
            );
            cluster_error_response(&error)
        }
    }
}

/// Optional body of `POST /admin/cluster/node/drain`: the member to move from
/// `Up` to `Draining` (defaults to this node's own identity).
#[derive(Deserialize)]
pub(crate) struct NodeDrainRequest {
    #[serde(default)]
    node_id: Option<String>,
}

/// Body of `POST /admin/cluster/node/remove`: the member to decommission and
/// the out-of-band confirmation token (never audited, never echoed back).
#[derive(Deserialize)]
pub(crate) struct NodeRemoveRequest {
    #[serde(default)]
    node_id: Option<String>,
    #[serde(default)]
    confirm_token: Option<String>,
}

/// `POST /admin/cluster/node/drain` — move a member from `Up` to `Draining`
/// in the persisted membership record (audited; defaults to this node).
pub(crate) async fn drain(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    body: Option<Json<NodeDrainRequest>>,
) -> Response {
    let owner = match require_admin(&state, &principal, "admin.cluster.drain") {
        Ok(owner) => owner,
        Err(response) => return *response,
    };
    let requested = body.and_then(|Json(request)| request.node_id);
    let node_id = match resolve_target_node(&state, requested.as_deref()) {
        Ok(node_id) => node_id,
        Err(response) => return response,
    };
    state.audit.record(
        owner.clone(),
        "admin.cluster.drain",
        format!("initiated node_id={node_id}"),
    );
    match bootstrap::node_drain(state.db.root(), node_id) {
        Ok(descriptor) => {
            state.audit.record(
                owner,
                "admin.cluster.drain.ok",
                format!("node_id={node_id} state={}", descriptor.state),
            );
            Json(json!({ "member": component(&descriptor) })).into_response()
        }
        Err(error) => {
            state.audit.record(
                owner,
                "admin.cluster.drain.fail",
                format!("node_id={node_id} {error}"),
            );
            cluster_error_response(&error)
        }
    }
}

/// `POST /admin/cluster/node/remove` — move a member to `Decommissioned` in
/// the persisted membership record. Requires the out-of-band confirmation
/// token in the request body; the token is never written to the audit log
/// and never echoed in responses.
pub(crate) async fn remove(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    body: Option<Json<NodeRemoveRequest>>,
) -> Response {
    let owner = match require_admin(&state, &principal, "admin.cluster.remove") {
        Ok(owner) => owner,
        Err(response) => return *response,
    };
    let (requested, confirm_token) = match body {
        Some(Json(request)) => (request.node_id, request.confirm_token),
        None => (None, None),
    };
    let Some(confirm_token) = confirm_token else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "confirm_token is required; obtain it out of band via \
                          `mongreldb-server node remove --data-dir <dir>`",
            })),
        )
            .into_response();
    };
    let node_id = match resolve_target_node(&state, requested.as_deref()) {
        Ok(node_id) => node_id,
        Err(response) => return response,
    };
    state.audit.record(
        owner.clone(),
        "admin.cluster.remove",
        format!("initiated node_id={node_id}"),
    );
    match bootstrap::node_remove(state.db.root(), node_id, &confirm_token) {
        Ok(descriptor) => {
            state.audit.record(
                owner,
                "admin.cluster.remove.ok",
                format!("node_id={node_id} state={}", descriptor.state),
            );
            Json(json!({ "member": component(&descriptor) })).into_response()
        }
        Err(error) => {
            state.audit.record(
                owner,
                "admin.cluster.remove.fail",
                format!("node_id={node_id} {error}"),
            );
            cluster_error_response(&error)
        }
    }
}
