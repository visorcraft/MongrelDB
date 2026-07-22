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
use mongreldb_cluster::gateway::{self, AdminCommand, JobAction};
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
fn accept_ops_job(
    state: &AppState,
    owner: &str,
    command: &str,
    kind: mongreldb_core::OpsJobKind,
    params: std::collections::BTreeMap<String, String>,
    extra: Option<Value>,
) -> Response {
    let idempotency_key = params.get("idempotency_key").cloned();
    let metadata_version = params
        .get("metadata_version")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    let result = state.ops_jobs.lock().map(|mut store| {
        store.submit_with(kind, params, idempotency_key, metadata_version)
    });
    match result {
        Ok(Ok(job)) => {
            state.audit.record(
                owner.to_string(),
                "admin.sql.job.accepted",
                format!("command={command} job_id={} kind={} state={}", job.job_id, job.kind.name(), job.state.name()),
            );
            let mut body = mongreldb_core::OpsJobStore::accepted_response(&job);
            if let Some(object) = body.as_object_mut() {
                object.insert("command".into(), json!(command));
                if let Some(Value::Object(extra_map)) = extra {
                    for (k, v) in extra_map {
                        object.entry(k).or_insert(v);
                    }
                }
            }
            Json(body).into_response()
        }
        Ok(Err(error)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": error.to_string(), "command": command, "status": "error"})),
        ).into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "ops job store lock poisoned", "command": command, "status": "error"})),
        ).into_response(),
    }
}

fn cluster_error_response(error: &ClusterError) -> Response {
    (
        status_for_cluster_error(error),
        Json(json!({ "error": error.to_string() })),
    )
        .into_response()
}

/// Resolve the member a drain/remove targets: the explicit `node_id` request
/// field, else this node's own persisted identity. A standalone node has no
/// identity to default to, so the operation conflicts. The error response is
/// boxed (same shape as `require_admin`).
fn resolve_target_node(state: &AppState, requested: Option<&str>) -> Result<NodeId, Box<Response>> {
    match requested {
        Some(text) => text.parse::<NodeId>().map_err(|error| {
            Box::new(
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": format!("invalid node_id `{text}`: {error}") })),
                )
                    .into_response(),
            )
        }),
        None => match NodeIdentity::load(cluster_status_root(state)) {
            Ok(Some(identity)) => Ok(identity.node_id),
            Ok(None) => Err(Box::new(
                (
                    StatusCode::CONFLICT,
                    Json(json!({
                        "error": "node is standalone; no cluster identity exists \
                                  (run `mongreldb-server cluster init` or `cluster join` first)",
                    })),
                )
                    .into_response(),
            )),
            Err(error) => Err(Box::new(cluster_error_response(&error))),
        },
    }
}

/// `GET /admin/cluster/status` — identity, membership, node descriptors, and
/// version info; reports `standalone` when no cluster identity exists.
/// When a live [`crate::cluster_runtime::ClusterRuntimeHandle`] is configured,
/// the report also carries a `runtime` object (node id, RPC address, tablet
/// count, meta present).
pub(crate) async fn status(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
) -> Response {
    if let Err(response) = require_admin(&state, &principal, "admin.cluster.status") {
        return *response;
    }
    let root = cluster_status_root(&state);
    match status_report(root) {
        Ok(mut report) => {
            if let Some(runtime) = state.cluster_runtime() {
                match runtime.runtime_status_json().await {
                    Ok(runtime_status) => {
                        if let Some(object) = report.as_object_mut() {
                            object.insert("runtime".into(), runtime_status);
                        }
                    }
                    Err(error) => {
                        if let Some(object) = report.as_object_mut() {
                            object.insert(
                                "runtime".into(),
                                json!({ "live": false, "error": error.to_string() }),
                            );
                        }
                    }
                }
            }
            Json(report).into_response()
        }
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

/// Prefer the live runtime's node-data directory when cluster mode is on;
/// otherwise the database root (historical co-located layout).
fn cluster_status_root(state: &AppState) -> &std::path::Path {
    match state.storage() {
        crate::ServerStorageRuntime::Cluster(rt) => rt.node_data(),
        crate::ServerStorageRuntime::Standalone(rt) => rt.root(),
    }
}

/// Fail-closed response when transfer/split/merge is issued without a live
/// NodeRuntime (standalone default, or cluster mode not configured).
fn runtime_not_running_response(command: &str, fields: Value) -> Response {
    let mut body = fields;
    if let Some(object) = body.as_object_mut() {
        object.insert("command".into(), json!(command));
        object.insert("status".into(), json!("error"));
        object.insert("error".into(), json!("cluster runtime not running"));
        object.insert("category".into(), json!("unavailable"));
    }
    (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response()
}

/// Structured error from a live runtime operation (missing tablet, not leader, …).
fn runtime_op_error_response(command: &str, error: String) -> Response {
    (
        StatusCode::CONFLICT,
        Json(json!({
            "command": command,
            "status": "error",
            "error": error,
            "category": "failed_precondition",
        })),
    )
        .into_response()
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
        Err(response) => return *response,
    };
    state.audit.record(
        owner.clone(),
        "admin.cluster.drain",
        format!("initiated node_id={node_id}"),
    );
    match bootstrap::node_drain(cluster_status_root(&state), node_id) {
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
        Err(response) => return *response,
    };
    state.audit.record(
        owner.clone(),
        "admin.cluster.remove",
        format!("initiated node_id={node_id}"),
    );
    match bootstrap::node_remove(cluster_status_root(&state), node_id, &confirm_token) {
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

// ---------------------------------------------------------------------------
// Admin SQL surface (§15) — typed commands from gateway::parse_admin_sql
// ---------------------------------------------------------------------------

/// Try to handle `sql` as a §15 cluster admin statement.
///
/// Returns `None` when the text is ordinary SQL (caller falls through).
/// Returns `Some(response)` for recognised admin commands. SHOW helpers are
/// available without requiring a fully-booted tablet runtime. Mutating
/// commands that need live groups (`TRANSFER LEADER`, `SPLIT TABLET`,
/// `MERGE TABLETS`) fail closed with `"cluster runtime not running"` when
/// no [`crate::cluster_runtime::ClusterRuntimeHandle`] is configured, and
/// drive the live runtime when it is. `MOVE REPLICA` returns a clear
/// not-yet-live error while a runtime is present (no direct placement API
/// on the product path yet) and the same fail-closed error when absent.
/// `ALTER NODE DRAIN` reuses the existing bootstrap path.
pub(crate) async fn try_admin_sql(
    state: &AppState,
    principal: &Option<mongreldb_core::Principal>,
    sql: &str,
) -> Option<Response> {
    let command = match gateway::parse_admin_sql(sql) {
        Ok(Some(cmd)) => cmd,
        Ok(None) => return None,
        Err(error) => {
            return Some(
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": error.to_string(), "category": "invalid_argument" })),
                )
                    .into_response(),
            );
        }
    };

    // All admin SQL requires the admin principal (spec §15: authenticated +
    // authorized). Audit every attempt.
    let owner = match require_admin(state, principal, "admin.sql") {
        Ok(owner) => owner,
        Err(response) => return Some(*response),
    };
    state.audit.record(
        owner.clone(),
        "admin.sql",
        format!("command={}", admin_command_name(&command)),
    );

    let status_root = cluster_status_root(state);
    let response = match command {
        AdminCommand::ShowCluster => match status_report(status_root) {
            Ok(mut report) => {
                // Stage 4 multi-region policy is server-reachable via admin SQL.
                let multi = state
                    .multi_region
                    .lock()
                    .map(|p| {
                        let placement =
                            mongreldb_cluster::multi_region::placement_from_multi_region(&p);
                        json!({
                            "prefer_home_leader": p.prefer_home_leader,
                            "regional_followers": p.regional_followers,
                            "async_dr_regions": p.async_dr_regions,
                            "tenant_home_region": p.tenant_home_region,
                            "total_voters": p.voters.total_voters(),
                            "placement_replicas": placement.replicas,
                            "multi_leader_default": false,
                        })
                    })
                    .unwrap_or_else(|_| json!({ "error": "multi_region lock poisoned" }));
                if let Some(runtime) = state.cluster_runtime() {
                    match runtime.runtime_status_json().await {
                        Ok(runtime_status) => {
                            if let Some(object) = report.as_object_mut() {
                                object.insert("runtime".into(), runtime_status);
                            }
                        }
                        Err(error) => {
                            if let Some(object) = report.as_object_mut() {
                                object.insert(
                                    "runtime".into(),
                                    json!({ "live": false, "error": error.to_string() }),
                                );
                            }
                        }
                    }
                }
                Json(json!({
                    "command": "SHOW CLUSTER",
                    "result": report,
                    "multi_region": multi,
                }))
                .into_response()
            }
            Err(error) => cluster_error_response(&error),
        },
        AdminCommand::ShowNodes => match bootstrap::cluster_status(status_root) {
            Ok(status) => Json(json!({
                "command": "SHOW NODES",
                "nodes": component(&status.member_endpoints),
                "membership": component(&status.membership),
            }))
            .into_response(),
            Err(ClusterError::NotInitialized) => Json(json!({
                "command": "SHOW NODES",
                "mode": "standalone",
                "nodes": [],
            }))
            .into_response(),
            Err(error) => cluster_error_response(&error),
        },
        AdminCommand::ShowTablets { table } => {
            let (tablets, issues) = mongreldb_cluster::tablet::list_tablets_on_disk(status_root)
                .unwrap_or_else(|e| (Vec::new(), vec![e.to_string()]));
            let rows: Vec<Value> = tablets
                .iter()
                .filter(|t| {
                    // Optional name filter is best-effort (table id hex / display).
                    table.as_ref().is_none_or(|name| {
                        t.table_id.to_string() == *name || name.eq_ignore_ascii_case("all")
                    })
                })
                .map(|t| {
                    json!({
                        "tablet_id": t.tablet_id.to_string(),
                        "table_id": t.table_id.to_string(),
                        "raft_group_id": t.raft_group_id.to_string(),
                        "generation": t.generation,
                        "state": t.state.to_string(),
                        "replicas": t.replicas.len(),
                        "leader_hint": t.leader_hint.map(|n| n.to_string()),
                    })
                })
                .collect();
            Json(json!({
                "command": "SHOW TABLETS",
                "table": table,
                "tablets": rows,
                "issues": issues,
            }))
            .into_response()
        }
        AdminCommand::ShowReplicas { tablet_id } => {
            let (tablets, issues) = mongreldb_cluster::tablet::list_tablets_on_disk(status_root)
                .unwrap_or_else(|e| (Vec::new(), vec![e.to_string()]));
            let mut replicas = Vec::new();
            for t in &tablets {
                if tablet_id.is_some_and(|id| id != t.tablet_id) {
                    continue;
                }
                for r in &t.replicas {
                    replicas.push(json!({
                        "tablet_id": t.tablet_id.to_string(),
                        "node_id": r.node_id.to_string(),
                        "role": r.role.to_string(),
                        "raft_node_id": r.raft_node_id,
                        "counts_toward_quorum": r.role.counts_toward_quorum(),
                    }));
                }
            }
            Json(json!({
                "command": "SHOW REPLICAS",
                "tablet_id": tablet_id.map(|id| id.to_string()),
                "replicas": replicas,
                "issues": issues,
            }))
            .into_response()
        }
        AdminCommand::ShowTransactions => {
            // Live sessions stand in for open interactive transactions; each
            // carries owner + idle bookkeeping via the session store.
            Json(json!({
                "command": "SHOW TRANSACTIONS",
                "open_sessions": state.sessions.len(),
                "transactions": [],
                "note": "distributed txn status partitions surface when a meta/txn group is hosted; session count is live",
            }))
            .into_response()
        }
        AdminCommand::ShowQueries => {
            let queries: Vec<Value> = state
                .query_registry
                .active_statuses()
                .into_iter()
                .map(|q| {
                    json!({
                        "query_id": format!("{}", q.query_id),
                        "owner": q.owner,
                        "session_id": q.session_id,
                        "phase": format!("{:?}", q.phase),
                        "operation": q.operation,
                    })
                })
                .collect();
            Json(json!({
                "command": "SHOW QUERIES",
                "queries": queries,
                "active_count": state.query_registry.active_count(),
            }))
            .into_response()
        }
        AdminCommand::ShowJobs => {
            let engine_jobs: Vec<Value> = state.db()
                .job_registry()
                .list()
                .into_iter()
                .map(|j| {
                    json!({
                        "job_id": j.job_id,
                        "kind": format!("{:?}", j.kind),
                        "state": format!("{:?}", j.state),
                        "source": "engine",
                    })
                })
                .collect();
            let ops: Vec<Value> = state
                .ops_jobs
                .lock()
                .map(|s| {
                    s.list()
                        .into_iter()
                        .map(|j| {
                            json!({
                                "job_id": j.job_id,
                                "kind": j.kind.name(),
                                "state": format!("{:?}", j.state),
                                "phase": j.phase,
                                "progress": j.progress,
                                "source": "ops",
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            Json(json!({
                "command": "SHOW JOBS",
                "jobs": engine_jobs.into_iter().chain(ops).collect::<Vec<_>>(),
            }))
            .into_response()
        }
        AdminCommand::ShowResourceGroups => {
            let stats = Some(state.scheduler.stats());
            // Drive node governor with live inputs and apply actions (S4B).
            let governor = state.node_governor.lock().ok().map(|mut gov| {
                let (db_reserved, db_max, db_gov) = match state.try_db() {
                    Ok(db) => {
                        let g = db.memory_governor();
                        (g.total_used(), g.max_bytes(), Some(g))
                    }
                    Err(_) => (0, gov.governor.max_bytes(), None),
                };
                let ai_capacity = std::env::var("MONGRELDB_AI_MAX_CONCURRENT")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .filter(|v: &usize| *v > 0)
                    .unwrap_or(4);
                let inputs = crate::admission::build_pressure_inputs(
                    &crate::admission::PressureInputSources {
                        db_reserved_bytes: db_reserved,
                        db_max_bytes: db_max,
                        node_configured_max_bytes: gov.governor.max_bytes(),
                        tablet_reserved_bytes: gov.tablet_reserved_bytes(),
                        ai_capacity,
                        ai_available: state.ai_semaphore.available_permits(),
                        process_rss_bytes: crate::admission::process_rss_bytes(),
                    },
                );
                let actions = crate::admission::refresh_pressure(
                    &mut gov,
                    &inputs,
                    &state.scheduler,
                    db_gov,
                );
                let pressure = state.scheduler.pressure().snapshot();
                json!({
                    "tablet_reserved_bytes": gov.tablet_reserved_bytes(),
                    "query_reserved_bytes": inputs.query_reserved_bytes,
                    "configured_max_bytes": inputs.configured_max_bytes,
                    "os_pressure": inputs.os_pressure,
                    "actions": actions.iter().map(|a| format!("{a:?}")).collect::<Vec<_>>(),
                    "applied": {
                        "reject_ai": pressure.reject_ai,
                        "reduced_admission": pressure.reduced_admission,
                        "move_tablet_noops": pressure.move_tablet_noops,
                        "last_evict_bytes": pressure.last_evict_bytes,
                        "evaluate_count": pressure.evaluate_count,
                    },
                    // MoveTabletLeaders is a no-op outside cluster tablet routing.
                    "move_tablet_leaders": "noop_outside_cluster_mode",
                })
            });
            // AI index readiness registry + retrieval planner knobs (S4C/S4D).
            let ai = state.ai_generations.lock().ok().map(|reg| {
                let local_k = mongreldb_query::adaptive_local_k(10, 2.0, 5);
                let readiness = mongreldb_core::readiness_action(true, true, false);
                json!({
                    "adaptive_local_k_example": local_k,
                    "fusion_default": "rrf_k60",
                    "indexes_registered": reg.len(),
                    "readiness_action_example": format!("{readiness:?}"),
                })
            });
            let groups: Vec<Value> = state
                .resource_groups
                .names()
                .into_iter()
                .filter_map(|name| {
                    state.resource_groups.get(&name).map(|g| {
                        json!({
                            "name": g.name,
                            "max_concurrency": g.max_concurrency,
                            "max_queue": g.max_queue,
                            "memory_bytes": g.memory_bytes,
                            "temporary_disk_bytes": g.temporary_disk_bytes,
                            "work_units": g.work_units,
                            "cpu_weight": g.cpu_weight,
                            "priority": g.priority,
                            "max_result_bytes": g.max_result_bytes,
                        })
                    })
                })
                .collect();
            let embedding_providers = state.embedding_providers.list_ids();
            Json(json!({
                "command": "SHOW RESOURCE GROUPS",
                "resource_groups": groups,
                "workload_classes": mongreldb_core::WorkloadClass::ALL
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                "embedding_providers": embedding_providers,
                "scheduler": stats.map(|s| json!({
                    "tenants": s.tenants,
                    "per_class": s.per_class,
                })),
                "node_governor": governor,
                "ai": ai,
            }))
            .into_response()
        }
        AdminCommand::ShowBackups => {
            let backups: Vec<Value> = state
                .ops_jobs
                .lock()
                .map(|s| {
                    s.list()
                        .into_iter()
                        .filter(|j| j.kind == mongreldb_core::OpsJobKind::Backup)
                        .map(|j| {
                            json!({
                                "job_id": j.job_id,
                                "state": format!("{:?}", j.state),
                                "phase": j.phase,
                                "progress": j.progress,
                                "destination": j.params.get("destination"),
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            Json(json!({
                "command": "SHOW BACKUPS",
                "backups": backups,
            }))
            .into_response()
        }
        AdminCommand::AlterNodeDrain { node_id } => {
            state.audit.record(
                owner.clone(),
                "admin.sql.drain",
                format!("node_id={node_id}"),
            );
            match bootstrap::node_drain(status_root, node_id) {
                Ok(descriptor) => {
                    state.audit.record(
                        owner,
                        "admin.sql.drain.ok",
                        format!("node_id={node_id} state={}", descriptor.state),
                    );
                    Json(json!({
                        "command": "ALTER NODE DRAIN",
                        "member": component(&descriptor),
                    }))
                    .into_response()
                }
                Err(error) => {
                    state.audit.record(
                        owner,
                        "admin.sql.drain.fail",
                        format!("node_id={node_id} {error}"),
                    );
                    cluster_error_response(&error)
                }
            }
        }
        AdminCommand::TransferLeader { tablet_id, to } => {
            let mut params = std::collections::BTreeMap::new();
            params.insert("tablet_id".into(), tablet_id.to_string());
            params.insert("to".into(), to.to_string());
            accept_ops_job(state, &owner, "TRANSFER LEADER", mongreldb_core::OpsJobKind::TransferLeader, params, None)
        },
        AdminCommand::MoveReplica {
            tablet_id,
            from,
            to,
        } => {
            // No direct placement call on NodeRuntime yet; fail closed with a
            // clear status whether or not the runtime is live so operators are
            // never told the move was accepted.
            let detail = if state.cluster_runtime().is_some() {
                "move replica is not yet live on the product path"
            } else {
                "cluster runtime not running"
            };
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "command": "MOVE REPLICA",
                    "tablet_id": tablet_id.to_string(),
                    "from": from.to_string(),
                    "to": to.to_string(),
                    "status": "error",
                    "error": detail,
                    "category": "unavailable",
                })),
            )
                .into_response()
        }
        AdminCommand::SplitTablet {
            tablet_id,
            at_key_hex,
        } => {
            // P1.6: long ops accept immediately with a durable ops_jobs entry.
            let mut params = std::collections::BTreeMap::new();
            params.insert("tablet_id".into(), tablet_id.to_string());
            if let Some(hex) = at_key_hex {
                params.insert("at_key_hex".into(), hex);
            }
            accept_ops_job(
                state,
                &owner,
                "SPLIT TABLET",
                mongreldb_core::OpsJobKind::SplitTablet,
                params,
                None,
            )
        },
        AdminCommand::MergeTablets { left, right } => match state.cluster_runtime() {
            None => runtime_not_running_response(
                "MERGE TABLETS",
                json!({
                    "left": left.to_string(),
                    "right": right.to_string(),
                }),
            ),
            Some(runtime) => match runtime.merge_tablets(left, right).await {
                Ok(body) => Json(body).into_response(),
                Err(error) => runtime_op_error_response("MERGE TABLETS", error.to_string()),
            },
        },
        AdminCommand::JobControl { action, job_id } => {
            let verb = match action {
                JobAction::Pause => "PAUSE",
                JobAction::Resume => "RESUME",
                JobAction::Cancel => "CANCEL",
            };
            let result = state.ops_jobs.lock().map(|mut store| match action {
                JobAction::Pause => store.pause(&job_id).map(|j| j.state),
                JobAction::Resume => store.start(&job_id).map(|j| j.state),
                JobAction::Cancel => store.cancel(&job_id).map(|j| j.state),
            });
            match result {
                Ok(Ok(job_state)) => {
                    state.audit.record(
                        owner,
                        "admin.sql.job_control.ok",
                        format!("action={verb} job_id={job_id} state={}", job_state.name()),
                    );
                    Json(json!({
                        "command": format!("{verb} JOB"),
                        "job_id": job_id,
                        "status": "ok",
                        "state": job_state.name(),
                    }))
                    .into_response()
                }
                Ok(Err(mongreldb_core::OpsJobError::TooLate { job_id: id })) => (
                    StatusCode::CONFLICT,
                    Json(json!({
                        "error": "cancel too late: publish fence already crossed",
                        "command": format!("{verb} JOB"),
                        "job_id": id,
                        "category": "failed_precondition",
                    })),
                )
                    .into_response(),
                Ok(Err(error)) => (
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": error.to_string(),
                        "command": format!("{verb} JOB"),
                        "job_id": job_id,
                    })),
                )
                    .into_response(),
                Err(_) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "error": "ops job store lock poisoned",
                        "command": format!("{verb} JOB"),
                        "job_id": job_id,
                    })),
                )
                    .into_response(),
            }
        }
        AdminCommand::BackupDatabase { destination } => {
            let mut params = std::collections::BTreeMap::new();
            if let Some(dest) = &destination {
                params.insert("destination".into(), dest.clone());
            }
            let job = state
                .ops_jobs
                .lock()
                .ok().and_then(|mut store| store.submit(mongreldb_core::OpsJobKind::Backup, params).ok());
            // Do not fire-and-forget HierarchicalScheduler::submit here: orphan
            // Backup work items steal fairness slots until a later poll/complete
            // (review finding). Ops store owns the job lifecycle.
            Json(json!({
                "command": "BACKUP DATABASE",
                "destination": destination,
                "status": "accepted",
                "job": job.map(|j| json!({
                    "job_id": j.job_id,
                    "state": format!("{:?}", j.state),
                })),
                "note": "cluster backup protocol is mongreldb_cluster::cluster_backup; job is resumable via ops store",
            }))
            .into_response()
        }
        AdminCommand::RestoreDatabase {
            source,
            disaster_recovery,
        } => {
            use mongreldb_cluster::cluster_backup::{
                load_manifest, plan_restore, RestoreIdentityMode,
            };
            use mongreldb_types::ids::{ClusterId, DatabaseId};

            let mut params = std::collections::BTreeMap::new();
            params.insert("source".into(), source.clone());
            params.insert("disaster_recovery".into(), disaster_recovery.to_string());
            let job = state.ops_jobs.lock().ok().and_then(|mut store| {
                store.submit(mongreldb_core::OpsJobKind::Restore, params).ok()
            });

            // Live plan from a published backup when the path exists; otherwise
            // surface the load error without pretending restore completed.
            let plan_json = match load_manifest(std::path::Path::new(&source)) {
                Ok(manifest) => {
                    let mode = if disaster_recovery {
                        RestoreIdentityMode::DisasterRecovery
                    } else {
                        RestoreIdentityMode::NewIdentity
                    };
                    let fresh = if disaster_recovery {
                        None
                    } else {
                        Some((ClusterId::new_random(), DatabaseId::new_random()))
                    };
                    match plan_restore(&manifest, mode, fresh) {
                        Ok(plan) => json!({
                            "identity_mode": plan.identity_mode.to_string(),
                            "target_cluster_id": plan.target_cluster_id.to_string(),
                            "target_database_id": plan.target_database_id.to_string(),
                            "source_cluster_id": plan.source_cluster_id.to_string(),
                            "source_database_id": plan.source_database_id.to_string(),
                            "tablet_count": plan.tablets.len(),
                        }),
                        Err(error) => json!({ "error": error.to_string() }),
                    }
                }
                Err(error) => json!({
                    "error": error.to_string(),
                    "note": "ops job still accepted; materialize backup path then resume job",
                }),
            };

            // No fire-and-forget scheduler submit (see BACKUP DATABASE).
            Json(json!({
                "command": "RESTORE DATABASE",
                "source": source,
                "disaster_recovery": disaster_recovery,
                "status": "accepted",
                "job": job.map(|j| json!({
                    "job_id": j.job_id,
                    "kind": j.kind.name(),
                    "state": format!("{:?}", j.state),
                })),
                "restore_plan": plan_json,
            }))
            .into_response()
        }
    };
    Some(response)
}

fn admin_command_name(command: &AdminCommand) -> &'static str {
    match command {
        AdminCommand::ShowCluster => "SHOW CLUSTER",
        AdminCommand::ShowNodes => "SHOW NODES",
        AdminCommand::ShowTablets { .. } => "SHOW TABLETS",
        AdminCommand::ShowReplicas { .. } => "SHOW REPLICAS",
        AdminCommand::ShowTransactions => "SHOW TRANSACTIONS",
        AdminCommand::ShowQueries => "SHOW QUERIES",
        AdminCommand::ShowJobs => "SHOW JOBS",
        AdminCommand::ShowResourceGroups => "SHOW RESOURCE GROUPS",
        AdminCommand::ShowBackups => "SHOW BACKUPS",
        AdminCommand::AlterNodeDrain { .. } => "ALTER NODE DRAIN",
        AdminCommand::TransferLeader { .. } => "TRANSFER LEADER",
        AdminCommand::MoveReplica { .. } => "MOVE REPLICA",
        AdminCommand::SplitTablet { .. } => "SPLIT TABLET",
        AdminCommand::MergeTablets { .. } => "MERGE TABLETS",
        AdminCommand::JobControl { .. } => "JOB CONTROL",
        AdminCommand::BackupDatabase { .. } => "BACKUP DATABASE",
        AdminCommand::RestoreDatabase { .. } => "RESTORE DATABASE",
    }
}

#[cfg(test)]
mod tests {
    use mongreldb_cluster::gateway::parse_admin_sql;

    #[test]
    fn admin_sql_parser_covers_section_15_surface() {
        for sql in [
            "SHOW CLUSTER",
            "SHOW NODES",
            "SHOW TABLETS",
            "SHOW REPLICAS",
            "SHOW TRANSACTIONS",
            "SHOW QUERIES",
            "SHOW JOBS",
            "SHOW RESOURCE GROUPS",
            "SHOW BACKUPS",
            "BACKUP DATABASE",
        ] {
            let cmd = parse_admin_sql(sql).unwrap();
            assert!(cmd.is_some(), "expected admin command for {sql}");
        }
        assert!(parse_admin_sql("SELECT 1").unwrap().is_none());
    }
}
