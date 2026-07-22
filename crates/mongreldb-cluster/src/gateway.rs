//! Cluster query gateway: bind plans to real tablet groups and surface the
//! §15 admin command model (spec sections 12.4, 12.10, 15; Stage 3 residual).
//!
//! This module is pure routing + admin protocol logic. It never opens tablet
//! files from the query path (spec §1 / §12.10: do not bypass tablet routing
//! by opening tablet storage from the gateway). The server adapts these types
//! onto HTTP/SQL sessions and drives [`crate::routing::RoutingCache`] /
//! [`crate::routing::RetryPolicy`] for NotLeader / StaleMetadata retries.
//!
//! # Binding distributed plans
//!
//! [`bind_plan_to_tablets`] takes a tablet layout snapshot (descriptors +
//! meta version) and produces [`BoundFragment`]s that name the concrete
//! tablet group and preferred leader endpoint for each plan fragment. The
//! gateway refuses to bind when the metadata version is stale relative to
//! the request's pin, forcing a refresh+retry through the routing cache.
//!
//! # Admin SQL surface (§15)
//!
//! [`parse_admin_sql`] recognises the cluster admin statements and returns
//! a typed [`AdminCommand`]. Execution (authz, audit, job submission) is
//! the server's responsibility; this module only parses and validates.

use std::collections::BTreeMap;

use mongreldb_types::ids::{
    MetadataVersion, NodeId, QueryId, RaftGroupId, TableId, TabletId, TransactionId,
};
use serde::{Deserialize, Serialize};

use crate::routing::{Endpoint, GroupKey, LeaderHint, RoutingCache, RoutingEntry};
use crate::tablet::{TabletDescriptor, TabletState};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Gateway binding / admin parse errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GatewayError {
    /// The request's pinned metadata version is behind the live layout.
    #[error("stale metadata: request pinned {pinned}, layout is {current}; refresh routing cache")]
    StaleMetadata {
        /// Version the request carried.
        pinned: MetadataVersion,
        /// Version of the layout snapshot.
        current: MetadataVersion,
    },
    /// A fragment named a tablet that is not in the layout.
    #[error("unknown tablet {0} in plan fragment")]
    UnknownTablet(TabletId),
    /// A tablet is not routable (Creating / Retiring / Retired).
    #[error("tablet {tablet_id} is not routable (state {state})")]
    NotRoutable {
        /// Tablet id.
        tablet_id: TabletId,
        /// Current state.
        state: String,
    },
    /// Admin SQL could not be parsed.
    #[error("invalid admin SQL: {0}")]
    InvalidAdminSql(String),
    /// Admin command refused (validation).
    #[error("admin command refused: {0}")]
    AdminRefused(String),
}

// ---------------------------------------------------------------------------
// Plan binding
// ---------------------------------------------------------------------------

/// One plan fragment as the gateway sees it (subset of the query crate's
/// `PlanFragment` so cluster stays free of the query dependency).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayFragment {
    /// Fragment id within the plan.
    pub fragment_id: u32,
    /// Tablets this fragment must run on (empty = all tablets of `table_id`).
    pub tablet_ids: Vec<TabletId>,
    /// Optional table scope when tablet_ids is empty.
    pub table_id: Option<TableId>,
}

/// A distributed plan handle the gateway can bind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayPlan {
    /// Query identity.
    pub query_id: QueryId,
    /// Metadata version the planner pinned.
    pub metadata_version: MetadataVersion,
    /// Fragments to bind.
    pub fragments: Vec<GatewayFragment>,
}

/// Live tablet layout snapshot used for binding.
#[derive(Debug, Clone)]
pub struct TabletLayoutSnapshot {
    /// Meta-plane metadata version of this snapshot.
    pub metadata_version: MetadataVersion,
    /// All known tablet descriptors (keyed by tablet id).
    pub tablets: BTreeMap<TabletId, TabletDescriptor>,
}

impl TabletLayoutSnapshot {
    /// Build from a descriptor list.
    pub fn from_descriptors(
        metadata_version: MetadataVersion,
        descriptors: impl IntoIterator<Item = TabletDescriptor>,
    ) -> Self {
        let mut tablets = BTreeMap::new();
        for d in descriptors {
            tablets.insert(d.tablet_id, d);
        }
        Self {
            metadata_version,
            tablets,
        }
    }

    /// Active (routable) tablets for a table.
    pub fn routable_for_table(&self, table_id: TableId) -> Vec<&TabletDescriptor> {
        self.tablets
            .values()
            .filter(|t| t.table_id == table_id && is_routable(t.state))
            .collect()
    }
}

/// One fragment bound to concrete tablet groups + preferred endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundFragment {
    /// Fragment id.
    pub fragment_id: u32,
    /// Bound targets, ordered by tablet id for determinism.
    pub targets: Vec<BoundTabletTarget>,
}

/// One tablet group target for a bound fragment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundTabletTarget {
    /// Tablet id.
    pub tablet_id: TabletId,
    /// Raft group id.
    pub raft_group_id: RaftGroupId,
    /// Descriptor generation at bind time.
    pub generation: u64,
    /// Preferred leader endpoint address (if known).
    pub preferred_endpoint: Option<Endpoint>,
    /// All replica endpoints.
    pub endpoints: Vec<Endpoint>,
}

/// Fully bound plan ready for dispatch through tablet groups.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundPlan {
    /// Query id.
    pub query_id: QueryId,
    /// Metadata version used for binding.
    pub metadata_version: MetadataVersion,
    /// Bound fragments.
    pub fragments: Vec<BoundFragment>,
}

/// Bind a gateway plan onto real tablet groups.
///
/// Refuses when the plan's pinned metadata version is behind the layout
/// (caller must refresh routing + replan). Never opens tablet files.
pub fn bind_plan_to_tablets(
    plan: &GatewayPlan,
    layout: &TabletLayoutSnapshot,
    routing: &RoutingCache,
    resolve_endpoint: &dyn Fn(NodeId) -> Option<Endpoint>,
) -> Result<BoundPlan, GatewayError> {
    if plan.metadata_version < layout.metadata_version {
        return Err(GatewayError::StaleMetadata {
            pinned: plan.metadata_version,
            current: layout.metadata_version,
        });
    }

    let mut bound_fragments = Vec::with_capacity(plan.fragments.len());
    for frag in &plan.fragments {
        let tablet_ids: Vec<TabletId> = if frag.tablet_ids.is_empty() {
            let table_id = frag.table_id.ok_or_else(|| {
                GatewayError::AdminRefused(
                    "fragment with empty tablet_ids requires table_id".into(),
                )
            })?;
            layout
                .routable_for_table(table_id)
                .into_iter()
                .map(|t| t.tablet_id)
                .collect()
        } else {
            frag.tablet_ids.clone()
        };

        let mut targets = Vec::with_capacity(tablet_ids.len());
        for tablet_id in tablet_ids {
            let desc = layout
                .tablets
                .get(&tablet_id)
                .ok_or(GatewayError::UnknownTablet(tablet_id))?;
            if !is_routable(desc.state) {
                return Err(GatewayError::NotRoutable {
                    tablet_id,
                    state: desc.state.to_string(),
                });
            }
            let endpoints: Vec<Endpoint> = desc
                .replicas
                .iter()
                .filter_map(|r| resolve_endpoint(r.node_id))
                .collect();
            // Prefer routing-cache leader hint, else descriptor leader_hint.
            let preferred = preferred_endpoint(desc, routing, &endpoints);
            // Keep the cache warm for this tablet group.
            let _ = routing.refresh(
                GroupKey::Tablet(tablet_id),
                layout.metadata_version,
                endpoints.clone(),
                desc.leader_hint.map(|leader| LeaderHint {
                    term: 0, // unknown term from descriptor alone
                    leader,
                }),
            );
            targets.push(BoundTabletTarget {
                tablet_id,
                raft_group_id: desc.raft_group_id,
                generation: desc.generation,
                preferred_endpoint: preferred,
                endpoints,
            });
        }
        targets.sort_by_key(|t| t.tablet_id);
        bound_fragments.push(BoundFragment {
            fragment_id: frag.fragment_id,
            targets,
        });
    }

    Ok(BoundPlan {
        query_id: plan.query_id,
        metadata_version: layout.metadata_version,
        fragments: bound_fragments,
    })
}

fn is_routable(state: TabletState) -> bool {
    matches!(
        state,
        TabletState::Active | TabletState::Splitting | TabletState::Merging
    )
}

fn preferred_endpoint(
    desc: &TabletDescriptor,
    routing: &RoutingCache,
    endpoints: &[Endpoint],
) -> Option<Endpoint> {
    if let Some(entry) = routing.get(GroupKey::Tablet(desc.tablet_id)) {
        if let Some(hint) = entry.leader_hint {
            if let Some(ep) = endpoints.iter().find(|e| e.node_id == hint.leader) {
                return Some(ep.clone());
            }
        }
    }
    if let Some(leader) = desc.leader_hint {
        if let Some(ep) = endpoints.iter().find(|e| e.node_id == leader) {
            return Some(ep.clone());
        }
    }
    endpoints.first().cloned()
}

/// Install layout-derived routing entries into the cache (full refresh).
pub fn refresh_routing_from_layout(
    routing: &RoutingCache,
    layout: &TabletLayoutSnapshot,
    resolve_endpoint: &dyn Fn(NodeId) -> Option<Endpoint>,
) -> usize {
    let mut refreshed = 0;
    for desc in layout.tablets.values() {
        if !is_routable(desc.state) {
            continue;
        }
        let endpoints: Vec<Endpoint> = desc
            .replicas
            .iter()
            .filter_map(|r| resolve_endpoint(r.node_id))
            .collect();
        if routing.refresh(
            GroupKey::Tablet(desc.tablet_id),
            layout.metadata_version,
            endpoints,
            desc.leader_hint
                .map(|leader| LeaderHint { term: 0, leader }),
        ) {
            refreshed += 1;
        }
    }
    refreshed
}

// ---------------------------------------------------------------------------
// Admin SQL (§15)
// ---------------------------------------------------------------------------

/// Typed cluster admin commands (spec section 15).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminCommand {
    /// `SHOW CLUSTER`
    ShowCluster,
    /// `SHOW NODES`
    ShowNodes,
    /// `SHOW TABLETS` [FOR TABLE name]
    ShowTablets {
        /// Optional table filter (unresolved name; server binds).
        table: Option<String>,
    },
    /// `SHOW REPLICAS` [FOR TABLET id]
    ShowReplicas {
        /// Optional tablet filter (hex id).
        tablet_id: Option<TabletId>,
    },
    /// `SHOW TRANSACTIONS`
    ShowTransactions,
    /// `SHOW QUERIES`
    ShowQueries,
    /// `SHOW JOBS`
    ShowJobs,
    /// `SHOW RESOURCE GROUPS`
    ShowResourceGroups,
    /// `SHOW BACKUPS`
    ShowBackups,
    /// `ALTER NODE DRAIN` node_id
    AlterNodeDrain {
        /// Node to drain.
        node_id: NodeId,
    },
    /// `TRANSFER LEADER` tablet_id TO node_id
    TransferLeader {
        /// Tablet whose group transfers leadership.
        tablet_id: TabletId,
        /// Target node.
        to: NodeId,
    },
    /// `MOVE REPLICA` tablet_id FROM node_id TO node_id
    MoveReplica {
        /// Tablet.
        tablet_id: TabletId,
        /// Source node.
        from: NodeId,
        /// Target node.
        to: NodeId,
    },
    /// `SPLIT TABLET` tablet_id [AT key_hex]
    SplitTablet {
        /// Tablet to split.
        tablet_id: TabletId,
        /// Optional split key (hex). `None` = automatic.
        at_key_hex: Option<String>,
    },
    /// `MERGE TABLETS` left_id right_id
    MergeTablets {
        /// Lower/left tablet.
        left: TabletId,
        /// Right/adjacent tablet.
        right: TabletId,
    },
    /// `PAUSE JOB` / `RESUME JOB` / `CANCEL JOB` job_id
    JobControl {
        /// Action.
        action: JobAction,
        /// Job id (opaque string).
        job_id: String,
    },
    /// `BACKUP DATABASE` [TO path]
    BackupDatabase {
        /// Optional destination path.
        destination: Option<String>,
    },
    /// `RESTORE DATABASE` FROM path [DISASTER RECOVERY]
    RestoreDatabase {
        /// Backup root path.
        source: String,
        /// Identity mode.
        disaster_recovery: bool,
    },
}

/// Job control verb.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobAction {
    /// Pause a running job.
    Pause,
    /// Resume a paused job.
    Resume,
    /// Cancel a job.
    Cancel,
}

/// Parse one admin SQL statement into a typed command.
///
/// Case-insensitive keywords; identifiers are hex node/tablet ids where
/// required. Non-admin SQL returns `None` so the server can fall through to
/// the ordinary SQL path.
pub fn parse_admin_sql(sql: &str) -> Result<Option<AdminCommand>, GatewayError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let upper = trimmed.to_ascii_uppercase();
    let tokens: Vec<&str> = trimmed.split_whitespace().collect();
    if tokens.is_empty() {
        return Ok(None);
    }

    // SHOW ...
    if upper.starts_with("SHOW ") {
        return parse_show(&tokens);
    }
    // ALTER NODE DRAIN ...
    if upper.starts_with("ALTER NODE DRAIN") {
        return parse_alter_node_drain(&tokens).map(Some);
    }
    if upper.starts_with("TRANSFER LEADER") {
        return parse_transfer_leader(&tokens).map(Some);
    }
    if upper.starts_with("MOVE REPLICA") {
        return parse_move_replica(&tokens).map(Some);
    }
    if upper.starts_with("SPLIT TABLET") {
        return parse_split_tablet(&tokens).map(Some);
    }
    if upper.starts_with("MERGE TABLETS") {
        return parse_merge_tablets(&tokens).map(Some);
    }
    if upper.starts_with("PAUSE JOB")
        || upper.starts_with("RESUME JOB")
        || upper.starts_with("CANCEL JOB")
    {
        return parse_job_control(&tokens).map(Some);
    }
    if upper.starts_with("BACKUP DATABASE") {
        return parse_backup(&tokens).map(Some);
    }
    if upper.starts_with("RESTORE DATABASE") {
        return parse_restore(&tokens).map(Some);
    }
    Ok(None)
}

fn parse_show(tokens: &[&str]) -> Result<Option<AdminCommand>, GatewayError> {
    let kind = tokens
        .get(1)
        .ok_or_else(|| GatewayError::InvalidAdminSql("SHOW requires a target".into()))?
        .to_ascii_uppercase();
    match kind.as_str() {
        "CLUSTER" => Ok(Some(AdminCommand::ShowCluster)),
        "NODES" => Ok(Some(AdminCommand::ShowNodes)),
        "TABLETS" => {
            let table = if tokens.len() >= 4 && tokens[2].eq_ignore_ascii_case("FOR") {
                if !tokens[3].eq_ignore_ascii_case("TABLE") {
                    return Err(GatewayError::InvalidAdminSql(
                        "SHOW TABLETS FOR TABLE <name>".into(),
                    ));
                }
                tokens.get(4).map(|s| (*s).to_owned())
            } else {
                None
            };
            Ok(Some(AdminCommand::ShowTablets { table }))
        }
        "REPLICAS" => {
            let tablet_id = if tokens.len() >= 4 && tokens[2].eq_ignore_ascii_case("FOR") {
                if !tokens[3].eq_ignore_ascii_case("TABLET") {
                    return Err(GatewayError::InvalidAdminSql(
                        "SHOW REPLICAS FOR TABLET <id>".into(),
                    ));
                }
                Some(parse_tablet_id(tokens.get(4).copied().unwrap_or(""))?)
            } else {
                None
            };
            Ok(Some(AdminCommand::ShowReplicas { tablet_id }))
        }
        "TRANSACTIONS" => Ok(Some(AdminCommand::ShowTransactions)),
        "QUERIES" => Ok(Some(AdminCommand::ShowQueries)),
        "JOBS" => Ok(Some(AdminCommand::ShowJobs)),
        "RESOURCE" => {
            if tokens
                .get(2)
                .is_some_and(|t| t.eq_ignore_ascii_case("GROUPS"))
            {
                Ok(Some(AdminCommand::ShowResourceGroups))
            } else {
                Err(GatewayError::InvalidAdminSql("SHOW RESOURCE GROUPS".into()))
            }
        }
        "BACKUPS" => Ok(Some(AdminCommand::ShowBackups)),
        // Not an admin target: ordinary SQL SHOW (SHOW TABLES, SHOW COLUMNS,
        // ...) falls through to the normal SQL path.
        _ => Ok(None),
    }
}

fn parse_alter_node_drain(tokens: &[&str]) -> Result<AdminCommand, GatewayError> {
    // ALTER NODE DRAIN <node_id>
    let id = tokens
        .get(3)
        .ok_or_else(|| GatewayError::InvalidAdminSql("ALTER NODE DRAIN <node_id>".into()))?;
    Ok(AdminCommand::AlterNodeDrain {
        node_id: parse_node_id(id)?,
    })
}

fn parse_transfer_leader(tokens: &[&str]) -> Result<AdminCommand, GatewayError> {
    // TRANSFER LEADER <tablet_id> TO <node_id>
    if tokens.len() < 5 || !tokens[3].eq_ignore_ascii_case("TO") {
        return Err(GatewayError::InvalidAdminSql(
            "TRANSFER LEADER <tablet_id> TO <node_id>".into(),
        ));
    }
    Ok(AdminCommand::TransferLeader {
        tablet_id: parse_tablet_id(tokens[2])?,
        to: parse_node_id(tokens[4])?,
    })
}

fn parse_move_replica(tokens: &[&str]) -> Result<AdminCommand, GatewayError> {
    // MOVE REPLICA <tablet_id> FROM <node> TO <node>
    if tokens.len() < 7
        || !tokens[3].eq_ignore_ascii_case("FROM")
        || !tokens[5].eq_ignore_ascii_case("TO")
    {
        return Err(GatewayError::InvalidAdminSql(
            "MOVE REPLICA <tablet_id> FROM <node_id> TO <node_id>".into(),
        ));
    }
    Ok(AdminCommand::MoveReplica {
        tablet_id: parse_tablet_id(tokens[2])?,
        from: parse_node_id(tokens[4])?,
        to: parse_node_id(tokens[6])?,
    })
}

fn parse_split_tablet(tokens: &[&str]) -> Result<AdminCommand, GatewayError> {
    // SPLIT TABLET <id> [AT <key_hex>]
    let tablet_id = parse_tablet_id(
        tokens
            .get(2)
            .ok_or_else(|| GatewayError::InvalidAdminSql("SPLIT TABLET <id>".into()))?,
    )?;
    let at_key_hex = if tokens.get(3).is_some_and(|t| t.eq_ignore_ascii_case("AT")) {
        Some(
            tokens
                .get(4)
                .ok_or_else(|| GatewayError::InvalidAdminSql("SPLIT TABLET <id> AT <key>".into()))?
                .to_string(),
        )
    } else {
        None
    };
    Ok(AdminCommand::SplitTablet {
        tablet_id,
        at_key_hex,
    })
}

fn parse_merge_tablets(tokens: &[&str]) -> Result<AdminCommand, GatewayError> {
    // MERGE TABLETS <left> <right>
    if tokens.len() < 4 {
        return Err(GatewayError::InvalidAdminSql(
            "MERGE TABLETS <left_id> <right_id>".into(),
        ));
    }
    Ok(AdminCommand::MergeTablets {
        left: parse_tablet_id(tokens[2])?,
        right: parse_tablet_id(tokens[3])?,
    })
}

fn parse_job_control(tokens: &[&str]) -> Result<AdminCommand, GatewayError> {
    let action = match tokens[0].to_ascii_uppercase().as_str() {
        "PAUSE" => JobAction::Pause,
        "RESUME" => JobAction::Resume,
        "CANCEL" => JobAction::Cancel,
        _ => {
            return Err(GatewayError::InvalidAdminSql(
                "PAUSE|RESUME|CANCEL JOB <id>".into(),
            ))
        }
    };
    if !tokens.get(1).is_some_and(|t| t.eq_ignore_ascii_case("JOB")) {
        return Err(GatewayError::InvalidAdminSql(
            "PAUSE|RESUME|CANCEL JOB <id>".into(),
        ));
    }
    let job_id = tokens
        .get(2)
        .ok_or_else(|| GatewayError::InvalidAdminSql("missing job id".into()))?
        .to_string();
    Ok(AdminCommand::JobControl { action, job_id })
}

fn parse_backup(tokens: &[&str]) -> Result<AdminCommand, GatewayError> {
    // BACKUP DATABASE [TO path]
    let destination = if tokens.get(2).is_some_and(|t| t.eq_ignore_ascii_case("TO")) {
        Some(
            tokens
                .get(3)
                .ok_or_else(|| GatewayError::InvalidAdminSql("BACKUP DATABASE TO <path>".into()))?
                .trim_matches('\'')
                .trim_matches('"')
                .to_owned(),
        )
    } else {
        None
    };
    Ok(AdminCommand::BackupDatabase { destination })
}

fn parse_restore(tokens: &[&str]) -> Result<AdminCommand, GatewayError> {
    // RESTORE DATABASE FROM path [DISASTER RECOVERY]
    if tokens.len() < 4 || !tokens[2].eq_ignore_ascii_case("FROM") {
        return Err(GatewayError::InvalidAdminSql(
            "RESTORE DATABASE FROM <path> [DISASTER RECOVERY]".into(),
        ));
    }
    let source = tokens[3].trim_matches('\'').trim_matches('"').to_owned();
    let disaster_recovery = tokens
        .get(4)
        .is_some_and(|t| t.eq_ignore_ascii_case("DISASTER"));
    Ok(AdminCommand::RestoreDatabase {
        source,
        disaster_recovery,
    })
}

fn parse_tablet_id(text: &str) -> Result<TabletId, GatewayError> {
    text.parse()
        .map_err(|e| GatewayError::InvalidAdminSql(format!("invalid tablet id: {e}")))
}

fn parse_node_id(text: &str) -> Result<NodeId, GatewayError> {
    text.parse()
        .map_err(|e| GatewayError::InvalidAdminSql(format!("invalid node id: {e}")))
}

// ---------------------------------------------------------------------------
// Cluster-mode session metadata
// ---------------------------------------------------------------------------

/// Cluster-mode session state the server attaches to a protocol session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterSession {
    /// Session-local routing cache metadata version watermark.
    pub metadata_version: MetadataVersion,
    /// Optional active distributed transaction.
    pub dist_txn_id: Option<TransactionId>,
    /// Preferred read consistency label (opaque to this module).
    pub consistency: String,
}

impl Default for ClusterSession {
    fn default() -> Self {
        Self {
            metadata_version: MetadataVersion::ZERO,
            dist_txn_id: None,
            consistency: "linearizable".into(),
        }
    }
}

/// Install a routing entry helper used by tests and the server.
pub fn routing_entry(
    metadata_version: MetadataVersion,
    endpoints: Vec<Endpoint>,
    leader: Option<NodeId>,
    term: u64,
) -> RoutingEntry {
    RoutingEntry {
        leader_hint: leader.map(|leader| LeaderHint { term, leader }),
        term,
        metadata_version,
        endpoints,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tablet::{ReplicaDescriptor, ReplicaRole, TabletState};
    use mongreldb_types::ids::{ClusterId, NodeId, RaftGroupId, TableId};

    fn tid(n: u8) -> TabletId {
        TabletId::from_bytes({
            let mut b = [0u8; 16];
            b[15] = n;
            b
        })
    }
    fn nid(n: u8) -> NodeId {
        NodeId::from_bytes({
            let mut b = [0u8; 16];
            b[15] = n;
            b
        })
    }
    fn rid(n: u8) -> RaftGroupId {
        RaftGroupId::from_bytes({
            let mut b = [0u8; 16];
            b[15] = n;
            b
        })
    }
    fn qid() -> QueryId {
        QueryId::from_bytes([0xAB; 16])
    }

    fn desc(tablet: u8, table: u64, state: TabletState) -> TabletDescriptor {
        TabletDescriptor {
            tablet_id: tid(tablet),
            table_id: TableId::new(table),
            database_id: mongreldb_types::ids::DatabaseId::ZERO,
            raft_group_id: rid(tablet),
            partition: crate::tablet::PartitionBounds::unbounded(),
            replicas: vec![
                ReplicaDescriptor {
                    node_id: nid(1),
                    role: ReplicaRole::Voter,
                    raft_node_id: 1,
                },
                ReplicaDescriptor {
                    node_id: nid(2),
                    role: ReplicaRole::Voter,
                    raft_node_id: 2,
                },
            ],
            leader_hint: Some(nid(1)),
            generation: 3,
            state,
        }
    }

    fn resolve(node: NodeId) -> Option<Endpoint> {
        Some(Endpoint {
            node_id: node,
            address: format!("127.0.0.1:{}", 8000 + node.as_bytes()[15] as u16),
        })
    }

    #[test]
    fn bind_plan_routes_through_real_tablet_groups() {
        let layout = TabletLayoutSnapshot::from_descriptors(
            MetadataVersion::new(5),
            vec![
                desc(1, 10, TabletState::Active),
                desc(2, 10, TabletState::Active),
            ],
        );
        let routing = RoutingCache::new();
        let plan = GatewayPlan {
            query_id: qid(),
            metadata_version: MetadataVersion::new(5),
            fragments: vec![GatewayFragment {
                fragment_id: 0,
                tablet_ids: vec![],
                table_id: Some(TableId::new(10)),
            }],
        };
        let bound = bind_plan_to_tablets(&plan, &layout, &routing, &resolve).unwrap();
        assert_eq!(bound.fragments.len(), 1);
        assert_eq!(bound.fragments[0].targets.len(), 2);
        assert_eq!(bound.fragments[0].targets[0].tablet_id, tid(1));
        assert_eq!(bound.fragments[0].targets[1].tablet_id, tid(2));
        // Preferred endpoint is the leader.
        assert_eq!(
            bound.fragments[0].targets[0]
                .preferred_endpoint
                .as_ref()
                .map(|e| e.node_id),
            Some(nid(1))
        );
        // Cache warmed.
        assert!(routing.get(GroupKey::Tablet(tid(1))).is_some());
        assert!(!routing.is_stale(GroupKey::Tablet(tid(1)), MetadataVersion::new(5)));
    }

    #[test]
    fn bind_refuses_stale_metadata() {
        let layout = TabletLayoutSnapshot::from_descriptors(
            MetadataVersion::new(9),
            vec![desc(1, 1, TabletState::Active)],
        );
        let plan = GatewayPlan {
            query_id: qid(),
            metadata_version: MetadataVersion::new(3),
            fragments: vec![GatewayFragment {
                fragment_id: 0,
                tablet_ids: vec![tid(1)],
                table_id: None,
            }],
        };
        let err = bind_plan_to_tablets(&plan, &layout, &RoutingCache::new(), &resolve).unwrap_err();
        assert!(matches!(err, GatewayError::StaleMetadata { .. }));
    }

    #[test]
    fn bind_refuses_non_routable_tablet() {
        let layout = TabletLayoutSnapshot::from_descriptors(
            MetadataVersion::new(1),
            vec![desc(1, 1, TabletState::Creating)],
        );
        let plan = GatewayPlan {
            query_id: qid(),
            metadata_version: MetadataVersion::new(1),
            fragments: vec![GatewayFragment {
                fragment_id: 0,
                tablet_ids: vec![tid(1)],
                table_id: None,
            }],
        };
        let err = bind_plan_to_tablets(&plan, &layout, &RoutingCache::new(), &resolve).unwrap_err();
        assert!(matches!(err, GatewayError::NotRoutable { .. }));
    }

    #[test]
    fn parse_show_and_alter_admin_sql() {
        assert_eq!(
            parse_admin_sql("SHOW CLUSTER").unwrap(),
            Some(AdminCommand::ShowCluster)
        );
        // Ordinary SQL SHOW targets are not admin SQL: they fall through to
        // the normal SQL path (DataFusion handles SHOW TABLES).
        assert_eq!(parse_admin_sql("SHOW TABLES").unwrap(), None);
        assert_eq!(
            parse_admin_sql("show nodes;").unwrap(),
            Some(AdminCommand::ShowNodes)
        );
        assert_eq!(
            parse_admin_sql("SHOW TABLETS FOR TABLE orders").unwrap(),
            Some(AdminCommand::ShowTablets {
                table: Some("orders".into())
            })
        );
        assert_eq!(
            parse_admin_sql("SHOW RESOURCE GROUPS").unwrap(),
            Some(AdminCommand::ShowResourceGroups)
        );
        assert_eq!(
            parse_admin_sql("SHOW BACKUPS").unwrap(),
            Some(AdminCommand::ShowBackups)
        );

        let node = nid(7);
        let cmd = parse_admin_sql(&format!("ALTER NODE DRAIN {node}")).unwrap();
        assert_eq!(cmd, Some(AdminCommand::AlterNodeDrain { node_id: node }));

        let t = tid(3);
        let cmd = parse_admin_sql(&format!("TRANSFER LEADER {t} TO {node}")).unwrap();
        assert_eq!(
            cmd,
            Some(AdminCommand::TransferLeader {
                tablet_id: t,
                to: node
            })
        );

        let from = nid(1);
        let to = nid(2);
        let cmd = parse_admin_sql(&format!("MOVE REPLICA {t} FROM {from} TO {to}")).unwrap();
        assert_eq!(
            cmd,
            Some(AdminCommand::MoveReplica {
                tablet_id: t,
                from,
                to
            })
        );

        let cmd = parse_admin_sql(&format!("SPLIT TABLET {t}")).unwrap();
        assert_eq!(
            cmd,
            Some(AdminCommand::SplitTablet {
                tablet_id: t,
                at_key_hex: None
            })
        );

        let left = tid(1);
        let right = tid(2);
        let cmd = parse_admin_sql(&format!("MERGE TABLETS {left} {right}")).unwrap();
        assert_eq!(cmd, Some(AdminCommand::MergeTablets { left, right }));

        assert_eq!(
            parse_admin_sql("PAUSE JOB job-42").unwrap(),
            Some(AdminCommand::JobControl {
                action: JobAction::Pause,
                job_id: "job-42".into()
            })
        );
        assert_eq!(
            parse_admin_sql("BACKUP DATABASE TO '/var/backups/x'").unwrap(),
            Some(AdminCommand::BackupDatabase {
                destination: Some("/var/backups/x".into())
            })
        );
        assert_eq!(
            parse_admin_sql("RESTORE DATABASE FROM '/var/backups/x' DISASTER RECOVERY").unwrap(),
            Some(AdminCommand::RestoreDatabase {
                source: "/var/backups/x".into(),
                disaster_recovery: true
            })
        );

        // Ordinary SQL falls through.
        assert_eq!(parse_admin_sql("SELECT 1").unwrap(), None);
    }

    #[test]
    fn refresh_routing_installs_entries() {
        let layout = TabletLayoutSnapshot::from_descriptors(
            MetadataVersion::new(2),
            vec![
                desc(1, 1, TabletState::Active),
                desc(2, 1, TabletState::Retired),
            ],
        );
        let routing = RoutingCache::new();
        let n = refresh_routing_from_layout(&routing, &layout, &resolve);
        assert_eq!(n, 1); // retired skipped
        assert!(routing.get(GroupKey::Tablet(tid(1))).is_some());
        assert!(routing.get(GroupKey::Tablet(tid(2))).is_none());
    }

    #[test]
    fn _cluster_id_unused_silence() {
        // keep ClusterId import usable if tests expand
        let _ = ClusterId::from_bytes([0; 16]);
    }
}
