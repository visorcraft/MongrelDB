//! Replica placement and rebalancing (spec section 12.7, Stage 3G support).
//!
//! [`PlacementPolicy`] is the declared placement contract of a table:
//! replica count, voter locality constraints, leader preferences, and
//! prohibited nodes. The meta control plane (Stage 3A) will adopt these
//! types as its replicated policy records; they are defined here, beside the
//! engine that enforces them, until that integration lands.
//!
//! The placement engine is deterministic — the same inputs always produce
//! the same decision. [`choose_replicas`] ranks candidate nodes by, in
//! order:
//!
//! 1. eligibility: `Up` nodes that are not prohibited, hold no replica of
//!    the tablet, and satisfy every required voter locality constraint;
//! 2. fewest unsatisfied optional voter constraints;
//! 3. failure-domain spread: fewest replicas (existing plus already chosen)
//!    in the node's failure domain — its `zone` locality tier, falling back
//!    to `region`, falling back to the node itself, so node spread is the
//!    floor ("zone then node");
//! 4. least loaded: the composite [`NodeLoad`] score when loads are
//!    supplied, else zero;
//! 5. [`NodeId`] ascending, the total-order tie-break.
//!
//! [`check_move_safety`] enforces the non-negotiable of spec section 12.7 —
//! never reduce healthy voters below quorum — and [`plan_rebalance`] turns
//! current placements and node loads into an ordered, bounded movement plan
//! (add learner, snapshot/catch up, promote, optional leader transfer,
//! remove old replica).

use std::collections::{BTreeMap, BTreeSet};

use mongreldb_types::ids::{NodeId, TableId, TabletId};
use serde::{Deserialize, Serialize};

use crate::node::{Locality, NodeDescriptor, NodeState};
use crate::tablet::{ReplicaDescriptor, ReplicaRole, TabletDescriptor, TabletState};

// ---------------------------------------------------------------------------
// Policy types (spec section 12.7)
// ---------------------------------------------------------------------------

/// One locality requirement or preference against a node's [`Locality`]
/// tiers (spec section 12.7).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalityConstraint {
    /// Locality tier key (for example `region` or `zone`).
    pub key: String,
    /// Required tier value.
    pub value: String,
    /// `true`: only matching nodes are eligible. `false`: matching nodes are
    /// preferred, non-matching nodes remain eligible.
    pub required: bool,
}

impl LocalityConstraint {
    /// A hard constraint: only matching nodes are eligible.
    pub fn required(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            required: true,
        }
    }

    /// A soft constraint: matching nodes are preferred.
    pub fn preferred(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            required: false,
        }
    }

    /// Whether a node's locality satisfies the constraint.
    pub fn satisfied_by(&self, locality: &Locality) -> bool {
        locality.get(&self.key) == Some(self.value.as_str())
    }
}

/// The placement contract of a table (spec section 12.7).
///
/// Reconciliation note: spec section 12.1 lists placement policies among the
/// meta group's replicated state. The Stage 3A meta control plane adopts
/// this type as the replicated record; it is defined here because the
/// placement engine that enforces it lives in this module.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlacementPolicy {
    /// Total voting replicas per tablet; at least 1.
    pub replicas: u8,
    /// Locality constraints every voter is placed by (required ones filter
    /// eligibility, optional ones steer the ranking).
    pub voter_constraints: Vec<LocalityConstraint>,
    /// Locality preferences steering leadership (see [`choose_leader`]).
    pub leader_preferences: Vec<LocalityConstraint>,
    /// Nodes that must never hold a replica of the table.
    pub prohibited_nodes: Vec<NodeId>,
}

impl Default for PlacementPolicy {
    /// Three replicas, no constraints — the common HA default.
    fn default() -> Self {
        Self {
            replicas: 3,
            voter_constraints: Vec::new(),
            leader_preferences: Vec::new(),
            prohibited_nodes: Vec::new(),
        }
    }
}

/// Why a placement decision or policy is refused. All refusals fail closed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlacementError {
    /// A policy must request at least one replica (spec section 12.7).
    #[error("placement policy must request at least one replica")]
    ZeroReplicas,
    /// A locality constraint with an empty key or value is meaningless.
    #[error("locality constraint has an empty key or value")]
    EmptyConstraint,
    /// The policy cannot be satisfied by the supplied membership.
    #[error("placement policy is infeasible: {0}")]
    Infeasible(String),
    /// A voter change on a group with no voters is meaningless.
    #[error("voter change on an empty group is meaningless")]
    EmptyGroup,
    /// The change would leave fewer healthy voters than the quorum of the
    /// current configuration (spec section 12.7: never reduce healthy voters
    /// below quorum).
    #[error(
        "{change} refused: {remaining} healthy voter(s) would remain below the quorum \
         {quorum} of the current {current_voters}-voter configuration"
    )]
    QuorumViolation {
        /// Voters in the current committed configuration.
        current_voters: u32,
        /// Healthy voters after the change.
        remaining: u32,
        /// Quorum of the current configuration.
        quorum: u32,
        /// The attempted change.
        change: &'static str,
    },
}

/// The quorum of a voter configuration: strict majority.
pub fn quorum_size(voters: u32) -> u32 {
    voters / 2 + 1
}

/// A membership change whose safety [`check_move_safety`] judges.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum VoterChange {
    /// Add a voter (the promote step of the movement protocol).
    AddVoter,
    /// Add a learner; voters are untouched.
    AddLearner,
    /// Promote a learner to voter.
    PromoteLearner,
    /// Demote a voter to learner.
    DemoteVoter,
    /// Remove a voter outright.
    RemoveVoter,
}

impl VoterChange {
    /// Stable name for errors.
    pub fn name(self) -> &'static str {
        match self {
            Self::AddVoter => "add voter",
            Self::AddLearner => "add learner",
            Self::PromoteLearner => "promote learner",
            Self::DemoteVoter => "demote voter",
            Self::RemoveVoter => "remove voter",
        }
    }
}

/// Judges whether a membership change may start, given the
/// `current_voters` of the committed configuration (spec section 12.7:
/// never reduce healthy voters below quorum).
///
/// Growing the group is always safe. Removing or demoting a voter is safe
/// only while the remaining voters still form a quorum of the *current*
/// configuration — so a direct 3→2 reduction passes (2 ≥ 2), while a direct
/// 2→1 reduction is refused (1 < 2): a two-voter group must first grow to
/// three before it may shrink. The movement protocol of [`plan_rebalance`]
/// is add-first (learner, catch up, promote, then remove), so the removal
/// it plans is always judged against the post-promote voter count.
///
/// When health is unknown, pass the configured voter count for both
/// arguments (legacy behaviour). Prefer [`check_move_safety_healthy`] when
/// the runtime reports per-replica reachability (review finding **m10**).
pub fn check_move_safety(current_voters: u32, change: VoterChange) -> Result<(), PlacementError> {
    check_move_safety_healthy(current_voters, current_voters, change)
}

/// Like [`check_move_safety`], but judges removal/demotion against
/// **healthy** voter counts (spec §12.7 / review **m10**).
///
/// - `configured_voters` — voters in the committed configuration.
/// - `healthy_voters` — of those, how many are currently reachable.
///
/// Removal is refused when either (a) the configured post-removal set would
/// fall below the configured quorum, or (b) the healthy post-removal set
/// would fall below the configured quorum (zero failure margin on a
/// degraded group). Growing the group is always allowed so repair can proceed.
pub fn check_move_safety_healthy(
    configured_voters: u32,
    healthy_voters: u32,
    change: VoterChange,
) -> Result<(), PlacementError> {
    if configured_voters == 0 {
        return Err(PlacementError::EmptyGroup);
    }
    let healthy_voters = healthy_voters.min(configured_voters);
    match change {
        VoterChange::AddVoter | VoterChange::AddLearner | VoterChange::PromoteLearner => Ok(()),
        VoterChange::DemoteVoter | VoterChange::RemoveVoter => {
            let remaining_configured = configured_voters - 1;
            let remaining_healthy = healthy_voters.saturating_sub(1);
            let quorum = quorum_size(configured_voters);
            if remaining_configured < quorum || remaining_healthy < quorum {
                return Err(PlacementError::QuorumViolation {
                    current_voters: healthy_voters,
                    remaining: remaining_healthy.min(remaining_configured),
                    quorum,
                    change: change.name(),
                });
            }
            Ok(())
        }
    }
}

/// Validates a placement policy against the current membership (used when
/// the policy is declared or changed — DDL time, not per movement):
///
/// - `replicas >= 1` (spec section 12.7);
/// - no empty locality constraints;
/// - quorum feasibility: at least `replicas` distinct eligible nodes exist
///   (`Up`, not prohibited, satisfying every required voter constraint), so
///   the full voter set can always be placed at once.
pub fn validate_policy(
    policy: &PlacementPolicy,
    nodes: &[NodeDescriptor],
) -> Result<(), PlacementError> {
    if policy.replicas == 0 {
        return Err(PlacementError::ZeroReplicas);
    }
    for constraint in policy
        .voter_constraints
        .iter()
        .chain(&policy.leader_preferences)
    {
        if constraint.key.is_empty() || constraint.value.is_empty() {
            return Err(PlacementError::EmptyConstraint);
        }
    }
    let prohibited: BTreeSet<NodeId> = policy.prohibited_nodes.iter().copied().collect();
    let eligible = nodes
        .iter()
        .filter(|node| node.state == NodeState::Up)
        .filter(|node| !prohibited.contains(&node.node_id))
        .filter(|node| {
            policy
                .voter_constraints
                .iter()
                .filter(|constraint| constraint.required)
                .all(|constraint| constraint.satisfied_by(&node.locality))
        })
        .count();
    if eligible < usize::from(policy.replicas) {
        return Err(PlacementError::Infeasible(format!(
            "policy requests {} replica(s) but only {eligible} eligible node(s) are up, \
             not prohibited, and satisfy the required voter constraints",
            policy.replicas
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Replica selection
// ---------------------------------------------------------------------------

/// The failure domain of a node: its `zone` locality tier, falling back to
/// `region`, falling back to the node itself so node spread is the floor
/// (spec section 12.7: "failure domains"; the zone-then-node order is this
/// module's documented rule).
fn zone_of(node: &NodeDescriptor) -> String {
    if let Some(zone) = node.locality.get("zone") {
        format!("zone:{zone}")
    } else if let Some(region) = node.locality.get("region") {
        format!("region:{region}")
    } else {
        zone_of_id(node.node_id)
    }
}

/// The own-domain fallback for a replica whose node descriptor is unknown.
fn zone_of_id(node_id: NodeId) -> String {
    format!("node:{}", node_id.to_hex())
}

/// Counts existing replicas per failure domain.
fn seed_zone_counts(
    existing: &[ReplicaDescriptor],
    nodes: &[NodeDescriptor],
) -> BTreeMap<String, usize> {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for replica in existing {
        let zone = nodes
            .iter()
            .find(|node| node.node_id == replica.node_id)
            .map_or_else(|| zone_of_id(replica.node_id), zone_of);
        *counts.entry(zone).or_insert(0) += 1;
    }
    counts
}

/// Chooses nodes for new voter replicas of one tablet so the voter total
/// reaches `policy.replicas` (spec section 12.7). Deterministic: the same
/// inputs always produce the same nodes, regardless of the order `nodes`
/// arrives in. Returns the nodes in selection order; fewer than requested
/// when the membership cannot satisfy the policy (callers re-validate with
/// [`validate_policy`]).
///
/// The deterministic ranking is the module-level order: eligibility first
/// (up, not prohibited, no replica yet, required constraints met), then fewest
/// unsatisfied optional constraints, fewest replicas in the failure domain,
/// and [`NodeId`] ascending as the final tie-break. No load signal exists at
/// declaration time, so the least-loaded key is zero for every candidate
/// here; the rebalancer's load-aware selection goes through the same ranking
/// with composite [`NodeLoad`] scores filled in.
pub fn choose_replicas(
    policy: &PlacementPolicy,
    nodes: &[NodeDescriptor],
    existing: &[ReplicaDescriptor],
) -> Vec<NodeId> {
    let existing_voters = existing
        .iter()
        .filter(|replica| replica.role == ReplicaRole::Voter)
        .count();
    let needed = usize::from(policy.replicas).saturating_sub(existing_voters);
    if needed == 0 {
        return Vec::new();
    }
    let excluded: BTreeSet<NodeId> = existing.iter().map(|replica| replica.node_id).collect();
    let mut zone_counts = seed_zone_counts(existing, nodes);
    choose_targets(policy, nodes, &excluded, &mut zone_counts, needed, None)
}

/// The deterministic selection core shared by [`choose_replicas`] and the
/// rebalancer. `zone_counts` tracks placed replicas per failure domain and
/// is updated as nodes are chosen; `load_scores`, when supplied, both fills
/// the least-loaded ranking key and restricts candidates to nodes that have
/// reported loads (fail closed: never place onto a node whose load is
/// unknown).
fn choose_targets(
    policy: &PlacementPolicy,
    nodes: &[NodeDescriptor],
    excluded: &BTreeSet<NodeId>,
    zone_counts: &mut BTreeMap<String, usize>,
    count: usize,
    load_scores: Option<&BTreeMap<NodeId, u64>>,
) -> Vec<NodeId> {
    let prohibited: BTreeSet<NodeId> = policy.prohibited_nodes.iter().copied().collect();
    let mut chosen: Vec<NodeId> = Vec::with_capacity(count);
    for _ in 0..count {
        let candidate = nodes
            .iter()
            .filter(|node| node.state == NodeState::Up)
            .filter(|node| !prohibited.contains(&node.node_id))
            .filter(|node| !excluded.contains(&node.node_id))
            .filter(|node| !chosen.contains(&node.node_id))
            .filter(|node| {
                policy
                    .voter_constraints
                    .iter()
                    .filter(|constraint| constraint.required)
                    .all(|constraint| constraint.satisfied_by(&node.locality))
            })
            .filter(|node| load_scores.is_none_or(|scores| scores.contains_key(&node.node_id)))
            .min_by_key(|node| {
                (
                    policy
                        .voter_constraints
                        .iter()
                        .filter(|constraint| {
                            !constraint.required && !constraint.satisfied_by(&node.locality)
                        })
                        .count(),
                    zone_counts.get(&zone_of(node)).copied().unwrap_or(0),
                    load_scores
                        .and_then(|scores| scores.get(&node.node_id).copied())
                        .unwrap_or(0),
                    node.node_id,
                )
            });
        let Some(node) = candidate else { break };
        *zone_counts.entry(zone_of(node)).or_insert(0) += 1;
        chosen.push(node.node_id);
    }
    chosen
}

/// Picks the preferred leader among `voters` (spec section 12.7 leader
/// preferences): `Up` voters satisfying every required preference, ranked by
/// fewest unsatisfied optional preferences, then [`NodeId`] ascending.
/// `None` when no voter is up or every up voter violates a required
/// preference.
pub fn choose_leader(
    policy: &PlacementPolicy,
    voters: &[NodeId],
    nodes: &[NodeDescriptor],
) -> Option<NodeId> {
    voters
        .iter()
        .filter_map(|voter| nodes.iter().find(|node| node.node_id == *voter))
        .filter(|node| node.state == NodeState::Up)
        .filter(|node| {
            policy
                .leader_preferences
                .iter()
                .filter(|preference| preference.required)
                .all(|preference| preference.satisfied_by(&node.locality))
        })
        .min_by_key(|node| {
            (
                policy
                    .leader_preferences
                    .iter()
                    .filter(|preference| {
                        !preference.required && !preference.satisfied_by(&node.locality)
                    })
                    .count(),
                node.node_id,
            )
        })
        .map(|node| node.node_id)
}

// ---------------------------------------------------------------------------
// Rebalancing (spec section 12.7)
// ---------------------------------------------------------------------------

/// Reported load of one node (spec section 12.7). All counters are `u64`;
/// units are documented per field. The composite [`Self::score`] normalizes
/// each dimension to per-mille of the cluster maximum and sums, so every
/// dimension weighs equally and the arithmetic stays exact and
/// deterministic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeLoad {
    /// The node reporting.
    pub node_id: NodeId,
    /// Bytes of disk in use.
    pub disk_used_bytes: u64,
    /// Write throughput, operations per second.
    pub write_ops: u64,
    /// Read throughput, operations per second.
    pub read_ops: u64,
    /// CPU in use, millicores (1000 = one core).
    pub cpu_millis: u64,
    /// Bytes of memory in use.
    pub memory_used_bytes: u64,
    /// Replicas hosted.
    pub replica_count: u64,
    /// Tablet leaderships held (leader distribution).
    pub leader_count: u64,
    /// Bytes of memory held by AI (ANN/Sparse/MinHash) indexes.
    pub ai_index_memory_bytes: u64,
}

impl Default for NodeLoad {
    /// All-zero load for the reserved zero node id (used for maxima
    /// accumulation and zeroed reports; [`NodeLoad::for_node`] builds real
    /// reports).
    fn default() -> Self {
        Self::for_node(NodeId::ZERO)
    }
}

impl NodeLoad {
    /// A zeroed load report for `node_id`.
    pub fn for_node(node_id: NodeId) -> Self {
        Self {
            node_id,
            disk_used_bytes: 0,
            write_ops: 0,
            read_ops: 0,
            cpu_millis: 0,
            memory_used_bytes: 0,
            replica_count: 0,
            leader_count: 0,
            ai_index_memory_bytes: 0,
        }
    }

    /// Composite load: the sum of each dimension as per-mille of the cluster
    /// maximum (a dimension with no reported maximum anywhere contributes
    /// zero). Exact integer arithmetic; deterministic.
    pub fn score(&self, maxima: &NodeLoad) -> u64 {
        per_mille(self.disk_used_bytes, maxima.disk_used_bytes)
            + per_mille(self.write_ops, maxima.write_ops)
            + per_mille(self.read_ops, maxima.read_ops)
            + per_mille(self.cpu_millis, maxima.cpu_millis)
            + per_mille(self.memory_used_bytes, maxima.memory_used_bytes)
            + per_mille(self.replica_count, maxima.replica_count)
            + per_mille(self.leader_count, maxima.leader_count)
            + per_mille(self.ai_index_memory_bytes, maxima.ai_index_memory_bytes)
    }
}

/// `value` as per-mille of `maximum` (zero when nothing was reported).
fn per_mille(value: u64, maximum: u64) -> u64 {
    value.saturating_mul(1000) / maximum.max(1)
}

/// Fieldwise maxima of the reported loads.
///
/// Callers should pass only `Up` nodes (review **N9**): a dead node with
/// extreme load must not compress every score. [`plan_rebalance`] filters
/// before calling this helper.
fn load_maxima(loads: &[NodeLoad]) -> NodeLoad {
    let mut maxima = NodeLoad::default();
    for load in loads {
        maxima.disk_used_bytes = maxima.disk_used_bytes.max(load.disk_used_bytes);
        maxima.write_ops = maxima.write_ops.max(load.write_ops);
        maxima.read_ops = maxima.read_ops.max(load.read_ops);
        maxima.cpu_millis = maxima.cpu_millis.max(load.cpu_millis);
        maxima.memory_used_bytes = maxima.memory_used_bytes.max(load.memory_used_bytes);
        maxima.replica_count = maxima.replica_count.max(load.replica_count);
        maxima.leader_count = maxima.leader_count.max(load.leader_count);
        maxima.ai_index_memory_bytes = maxima.ai_index_memory_bytes.max(load.ai_index_memory_bytes);
    }
    maxima
}

/// Knobs for [`plan_rebalance`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebalanceConfig {
    /// Maximum tablet movements in one plan; the movement protocol runs one
    /// plan at a time, so this bounds concurrent replica movement.
    pub max_concurrent_moves: usize,
    /// A node is hot when its composite score exceeds the mean score of all
    /// reporting `Up` nodes by more than this factor, in per-mille
    /// (1250 = 1.25x).
    pub hot_threshold_per_mille: u64,
}

impl Default for RebalanceConfig {
    /// One move at a time, hot = above 1.25x the mean.
    fn default() -> Self {
        Self {
            max_concurrent_moves: 1,
            hot_threshold_per_mille: 1250,
        }
    }
}

/// One ordered step of the movement protocol (spec section 12.7).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MovementStep {
    /// Add the target as a learner of the tablet's group.
    AddLearner {
        /// The tablet being moved.
        tablet_id: TabletId,
        /// The node receiving the replica.
        node_id: NodeId,
    },
    /// Snapshot and catch the learner up to the group log.
    CatchUp {
        /// The tablet being moved.
        tablet_id: TabletId,
        /// The node receiving the replica.
        node_id: NodeId,
    },
    /// Promote the caught-up learner to voter.
    PromoteLearner {
        /// The tablet being moved.
        tablet_id: TabletId,
        /// The node receiving the replica.
        node_id: NodeId,
    },
    /// Move leadership off the source (only planned when the source
    /// currently holds it).
    TransferLeadership {
        /// The tablet being moved.
        tablet_id: TabletId,
        /// The current leader.
        from: NodeId,
        /// The new leader.
        to: NodeId,
    },
    /// Remove the source replica. Always last: the replacement voter is
    /// already promoted, so healthy voters never drop below quorum.
    RemoveReplica {
        /// The tablet being moved.
        tablet_id: TabletId,
        /// The node losing the replica.
        node_id: NodeId,
    },
}

/// One tablet movement: source, target, and the ordered protocol steps.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaMove {
    /// The tablet being moved.
    pub tablet_id: TabletId,
    /// The node the replica moves away from.
    pub from: NodeId,
    /// The node the replica moves to.
    pub to: NodeId,
    /// The ordered movement-protocol steps (spec section 12.7).
    pub steps: Vec<MovementStep>,
}

/// An ordered rebalancing plan: at most [`RebalanceConfig::max_concurrent_moves`]
/// tablet movements, each fully described by its protocol steps. Moves are
/// ordered hottest-source first; a tablet appears at most once.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebalancePlan {
    /// The planned movements.
    pub moves: Vec<ReplicaMove>,
}

impl RebalancePlan {
    /// Whether the plan is empty.
    pub fn is_empty(&self) -> bool {
        self.moves.is_empty()
    }
}

/// Generates a deterministic rebalancing plan (spec section 12.7) from the
/// current tablet placements and reported node loads.
///
/// Rules, in order:
///
/// - Only `Up` nodes that reported loads are movement sources, targets, or
///   part of the mean; a node with unknown load is never a target (fail
///   closed).
/// - A node is hot when its composite score exceeds the mean by more than
///   [`RebalanceConfig::hot_threshold_per_mille`]; sources are processed
///   hottest first, ties by [`NodeId`] ascending.
/// - Only [`TabletState::Active`] tablets move (a tablet mid-split or
///   mid-merge is already in flight), at most one movement per tablet, and
///   only voter replicas move off a source.
/// - The target is the deterministic [`choose_targets`] winner under the
///   table's policy, using load scores that already reflect the movements
///   planned so far.
/// - Every movement follows the add-first protocol — add learner, snapshot/
///   catch up, promote, transfer leadership when the source holds it, remove
///   the old replica — and is only planned when the removal step passes
///   [`check_move_safety`] against the post-promote voter count. A
///   single-voter tablet is therefore never auto-rebalanced: moving it would
///   transiently drop healthy voters below the quorum of the two-voter
///   intermediate configuration.
pub fn plan_rebalance(
    tablets: &[TabletDescriptor],
    nodes: &[NodeDescriptor],
    loads: &[NodeLoad],
    policy_for: &dyn Fn(TableId) -> PlacementPolicy,
    config: &RebalanceConfig,
) -> RebalancePlan {
    let mut plan = RebalancePlan::default();
    if config.max_concurrent_moves == 0 {
        return plan;
    }
    // N9: only Up nodes contribute to load maxima (dead extreme load must
    // not compress every score).
    let up_ids: BTreeSet<NodeId> = nodes
        .iter()
        .filter(|n| n.state == NodeState::Up)
        .map(|n| n.node_id)
        .collect();
    let up_loads: Vec<NodeLoad> = loads
        .iter()
        .filter(|l| up_ids.contains(&l.node_id))
        .copied()
        .collect();
    let maxima = load_maxima(&up_loads);
    let mut planned_loads: BTreeMap<NodeId, NodeLoad> =
        up_loads.iter().map(|load| (load.node_id, *load)).collect();
    let current_scores = |planned_loads: &BTreeMap<NodeId, NodeLoad>| -> BTreeMap<NodeId, u64> {
        nodes
            .iter()
            .filter(|node| node.state == NodeState::Up)
            .filter_map(|node| {
                planned_loads
                    .get(&node.node_id)
                    .map(|load| (node.node_id, load.score(&maxima)))
            })
            .collect()
    };
    let scores = current_scores(&planned_loads);
    if scores.len() < 2 {
        return plan;
    }
    let mean = scores.values().sum::<u64>() / scores.len() as u64;
    let mut hot: Vec<(NodeId, u64)> = scores
        .iter()
        .filter(|(_, score)| {
            score.saturating_mul(1000) > mean.saturating_mul(config.hot_threshold_per_mille)
        })
        .map(|(node_id, score)| (*node_id, *score))
        .collect();
    hot.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));

    let mut in_flight: BTreeSet<TabletId> = BTreeSet::new();
    'sources: for (source, _) in hot {
        let mut candidates: Vec<&TabletDescriptor> = tablets
            .iter()
            .filter(|tablet| tablet.state == TabletState::Active)
            .filter(|tablet| !in_flight.contains(&tablet.tablet_id))
            .filter(|tablet| tablet.voters().any(|replica| replica.node_id == source))
            .collect();
        candidates.sort_by_key(|tablet| tablet.tablet_id);
        for tablet in candidates {
            if plan.moves.len() >= config.max_concurrent_moves {
                break 'sources;
            }
            let voters = tablet.voter_count() as u32;
            // The removal runs after the promote, so it is judged against
            // the post-promote configuration.
            if check_move_safety(voters + 1, VoterChange::RemoveVoter).is_err() {
                continue;
            }
            let policy = policy_for(tablet.table_id);
            let excluded: BTreeSet<NodeId> = tablet
                .replicas
                .iter()
                .map(|replica| replica.node_id)
                .collect();
            let mut zone_counts = seed_zone_counts(&tablet.replicas, nodes);
            let target = {
                let scores = current_scores(&planned_loads);
                choose_targets(
                    &policy,
                    nodes,
                    &excluded,
                    &mut zone_counts,
                    1,
                    Some(&scores),
                )
            };
            let Some(target) = target.into_iter().next() else {
                continue;
            };
            let mut steps = vec![
                MovementStep::AddLearner {
                    tablet_id: tablet.tablet_id,
                    node_id: target,
                },
                MovementStep::CatchUp {
                    tablet_id: tablet.tablet_id,
                    node_id: target,
                },
                MovementStep::PromoteLearner {
                    tablet_id: tablet.tablet_id,
                    node_id: target,
                },
            ];
            let transfers_leadership = tablet.leader_hint == Some(source);
            if transfers_leadership {
                steps.push(MovementStep::TransferLeadership {
                    tablet_id: tablet.tablet_id,
                    from: source,
                    to: target,
                });
            }
            steps.push(MovementStep::RemoveReplica {
                tablet_id: tablet.tablet_id,
                node_id: source,
            });
            plan.moves.push(ReplicaMove {
                tablet_id: tablet.tablet_id,
                from: source,
                to: target,
                steps,
            });
            in_flight.insert(tablet.tablet_id);
            if let Some(load) = planned_loads.get_mut(&source) {
                load.replica_count = load.replica_count.saturating_sub(1);
                if transfers_leadership {
                    load.leader_count = load.leader_count.saturating_sub(1);
                }
            }
            if let Some(load) = planned_loads.get_mut(&target) {
                load.replica_count = load.replica_count.saturating_add(1);
                if transfers_leadership {
                    load.leader_count = load.leader_count.saturating_add(1);
                }
            }
        }
    }
    plan
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{BuildVersion, NodeCapacity, VersionInfo};
    use crate::tablet::{PartitionBounds, TabletState};
    use mongreldb_types::ids::RaftGroupId;

    fn node_id(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 16])
    }

    fn tablet_id(byte: u8) -> TabletId {
        TabletId::from_bytes([byte; 16])
    }

    fn node(byte: u8, locality: &str) -> NodeDescriptor {
        NodeDescriptor {
            node_id: node_id(byte),
            rpc_address: format!("127.0.0.1:{}", 8000 + u16::from(byte)),
            locality: locality.parse().unwrap(),
            capacity: NodeCapacity::default(),
            state: NodeState::Up,
            version: BuildVersion::current(),
            version_info: VersionInfo::current(),
        }
    }

    /// Three zones with two nodes each: 1,2 in a; 3,4 in b; 5,6 in c.
    fn zoned_nodes() -> Vec<NodeDescriptor> {
        vec![
            node(1, "region=r1,zone=a"),
            node(2, "region=r1,zone=a"),
            node(3, "region=r1,zone=b"),
            node(4, "region=r1,zone=b"),
            node(5, "region=r2,zone=c"),
            node(6, "region=r2,zone=c"),
        ]
    }

    fn voter(node: NodeId, raft_node_id: u64) -> ReplicaDescriptor {
        ReplicaDescriptor {
            node_id: node,
            role: ReplicaRole::Voter,
            raft_node_id,
        }
    }

    // -- choose_replicas -------------------------------------------------------

    #[test]
    fn choose_replicas_is_deterministic_regardless_of_input_order() {
        let nodes = zoned_nodes();
        let policy = PlacementPolicy::default();
        let first = choose_replicas(&policy, &nodes, &[]);
        let second = choose_replicas(&policy, &nodes, &[]);
        assert_eq!(first, second);
        let mut reversed = nodes.clone();
        reversed.reverse();
        assert_eq!(first, choose_replicas(&policy, &reversed, &[]));
        assert_eq!(first.len(), 3);
        // Distinct nodes, all present in the membership.
        let unique: BTreeSet<_> = first.iter().collect();
        assert_eq!(unique.len(), 3);
    }

    #[test]
    fn choose_replicas_spreads_failure_domains_zone_then_node() {
        let nodes = zoned_nodes();
        let policy = PlacementPolicy::default();
        let chosen = choose_replicas(&policy, &nodes, &[]);
        // Three replicas land in three different zones.
        let zones: BTreeSet<String> = chosen
            .iter()
            .map(|id| zone_of(nodes.iter().find(|n| &n.node_id == id).unwrap()))
            .collect();
        assert_eq!(zones.len(), 3);

        // Four replicas over three zones: at most two per zone, and never
        // two on one node.
        let mut four = policy.clone();
        four.replicas = 4;
        let chosen = choose_replicas(&four, &nodes, &[]);
        assert_eq!(chosen.len(), 4);
        let mut zone_counts: BTreeMap<String, usize> = BTreeMap::new();
        for id in &chosen {
            let zone = zone_of(nodes.iter().find(|n| &n.node_id == id).unwrap());
            *zone_counts.entry(zone).or_insert(0) += 1;
        }
        assert!(zone_counts.values().all(|count| *count <= 2));
    }

    #[test]
    fn choose_replicas_never_picks_prohibited_nodes() {
        let nodes = zoned_nodes();
        // Prohibit the deterministic winners: node 1 and node 3.
        let policy = PlacementPolicy {
            prohibited_nodes: vec![node_id(1), node_id(3)],
            ..PlacementPolicy::default()
        };
        let chosen = choose_replicas(&policy, &nodes, &[]);
        assert_eq!(chosen.len(), 3);
        assert!(!chosen.contains(&node_id(1)));
        assert!(!chosen.contains(&node_id(3)));
    }

    #[test]
    fn choose_replicas_honors_required_and_preferred_locality() {
        let nodes = zoned_nodes();
        // Required: only zone b nodes are eligible.
        let mut policy = PlacementPolicy {
            replicas: 2,
            voter_constraints: vec![LocalityConstraint::required("zone", "b")],
            ..PlacementPolicy::default()
        };
        let chosen = choose_replicas(&policy, &nodes, &[]);
        assert_eq!(chosen.len(), 2);
        assert!(chosen
            .iter()
            .all(|id| matches!(*id, n if n == node_id(3) || n == node_id(4))));

        // Preferred: the zone-a nodes win the soft ranking, but zone-b nodes
        // remain eligible when more replicas are needed.
        policy.voter_constraints = vec![LocalityConstraint::preferred("zone", "a")];
        policy.replicas = 3;
        let chosen = choose_replicas(&policy, &nodes, &[]);
        assert_eq!(chosen.len(), 3);
        assert!(chosen[..2]
            .iter()
            .all(|id| matches!(*id, n if n == node_id(1) || n == node_id(2))));
    }

    #[test]
    fn choose_replicas_complements_existing_replicas() {
        let nodes = zoned_nodes();
        let policy = PlacementPolicy::default();
        // Two voters already placed: only one more is chosen, never on an
        // occupied node, and the zone spread accounts for the existing ones.
        let existing = vec![voter(node_id(1), 1), voter(node_id(3), 2)];
        let chosen = choose_replicas(&policy, &nodes, &existing);
        assert_eq!(chosen.len(), 1);
        assert!(!existing.iter().any(|replica| replica.node_id == chosen[0]));
        // Zones a and b are taken; the third replica must land in zone c.
        assert!(matches!(chosen[0], n if n == node_id(5) || n == node_id(6)));

        // A full voter set needs nothing.
        let full = vec![
            voter(node_id(1), 1),
            voter(node_id(3), 2),
            voter(node_id(5), 3),
        ];
        assert!(choose_replicas(&policy, &nodes, &full).is_empty());
    }

    // -- validate_policy ---------------------------------------------------------

    #[test]
    fn validate_policy_checks_quorum_feasibility() {
        let nodes = zoned_nodes();
        assert!(validate_policy(&PlacementPolicy::default(), &nodes).is_ok());

        let zero = PlacementPolicy {
            replicas: 0,
            ..PlacementPolicy::default()
        };
        assert_eq!(
            validate_policy(&zero, &nodes),
            Err(PlacementError::ZeroReplicas)
        );

        let too_many = PlacementPolicy {
            replicas: 7,
            ..PlacementPolicy::default()
        };
        assert!(matches!(
            validate_policy(&too_many, &nodes),
            Err(PlacementError::Infeasible(_))
        ));

        // Required constraints shrink the eligible set.
        let constrained = PlacementPolicy {
            replicas: 3,
            voter_constraints: vec![LocalityConstraint::required("zone", "a")],
            ..PlacementPolicy::default()
        };
        assert!(matches!(
            validate_policy(&constrained, &nodes),
            Err(PlacementError::Infeasible(_))
        ));

        // Prohibited nodes shrink it too.
        let prohibited = PlacementPolicy {
            replicas: 3,
            prohibited_nodes: (1..=4).map(node_id).collect(),
            ..PlacementPolicy::default()
        };
        assert!(matches!(
            validate_policy(&prohibited, &nodes),
            Err(PlacementError::Infeasible(_))
        ));

        let empty = PlacementPolicy {
            replicas: 2,
            voter_constraints: vec![LocalityConstraint::required("", "a")],
            ..PlacementPolicy::default()
        };
        assert_eq!(
            validate_policy(&empty, &nodes),
            Err(PlacementError::EmptyConstraint)
        );
    }

    // -- check_move_safety ---------------------------------------------------------

    #[test]
    fn move_safety_never_drops_healthy_voters_below_quorum() {
        // Growth is always safe.
        for voters in 1..=5 {
            for change in [
                VoterChange::AddVoter,
                VoterChange::AddLearner,
                VoterChange::PromoteLearner,
            ] {
                assert!(check_move_safety(voters, change).is_ok());
            }
        }
        // Reductions: 5->4, 4->3, 3->2 keep a quorum; 2->1 and 1->0 do not.
        assert!(check_move_safety(5, VoterChange::RemoveVoter).is_ok());
        assert!(check_move_safety(4, VoterChange::RemoveVoter).is_ok());
        assert!(check_move_safety(3, VoterChange::RemoveVoter).is_ok());
        assert!(check_move_safety(3, VoterChange::DemoteVoter).is_ok());
        let error = check_move_safety(2, VoterChange::RemoveVoter).unwrap_err();
        assert_eq!(
            error,
            PlacementError::QuorumViolation {
                current_voters: 2,
                remaining: 1,
                quorum: 2,
                change: "remove voter",
            }
        );
        assert!(check_move_safety(1, VoterChange::RemoveVoter).is_err());
        assert!(check_move_safety(1, VoterChange::DemoteVoter).is_err());
        assert_eq!(
            check_move_safety(0, VoterChange::AddVoter),
            Err(PlacementError::EmptyGroup)
        );
    }

    // -- choose_leader ----------------------------------------------------------------

    #[test]
    fn choose_leader_follows_leader_preferences() {
        let nodes = zoned_nodes();
        let voters = vec![node_id(1), node_id(3), node_id(5)];
        // No preferences: deterministic lowest node id.
        let policy = PlacementPolicy::default();
        assert_eq!(choose_leader(&policy, &voters, &nodes), Some(node_id(1)));

        // Soft preference for zone c steers to the zone-c voter.
        let preferred = PlacementPolicy {
            leader_preferences: vec![LocalityConstraint::preferred("zone", "c")],
            ..PlacementPolicy::default()
        };
        assert_eq!(choose_leader(&preferred, &voters, &nodes), Some(node_id(5)));

        // Required preference excludes every non-matching voter.
        let required = PlacementPolicy {
            leader_preferences: vec![LocalityConstraint::required("zone", "b")],
            ..PlacementPolicy::default()
        };
        assert_eq!(choose_leader(&required, &voters, &nodes), Some(node_id(3)));

        // A required preference no voter satisfies yields no leader pick.
        let impossible = PlacementPolicy {
            leader_preferences: vec![LocalityConstraint::required("zone", "z")],
            ..PlacementPolicy::default()
        };
        assert_eq!(choose_leader(&impossible, &voters, &nodes), None);
    }

    // -- plan_rebalance -----------------------------------------------------------------

    fn tablet(byte: u8, voters: &[u8], leader: Option<u8>) -> TabletDescriptor {
        TabletDescriptor {
            tablet_id: tablet_id(byte),
            table_id: TableId::new(7),
            database_id: mongreldb_types::ids::DatabaseId::ZERO,
            raft_group_id: RaftGroupId::from_bytes([byte.wrapping_add(64); 16]),
            partition: PartitionBounds::unbounded(),
            replicas: voters
                .iter()
                .enumerate()
                .map(|(index, node)| voter(node_id(*node), index as u64 + 1))
                .collect(),
            leader_hint: leader.map(node_id),
            generation: 1,
            state: TabletState::Active,
        }
    }

    fn load(byte: u8, disk: u64, replicas: u64, leaders: u64) -> NodeLoad {
        NodeLoad {
            node_id: node_id(byte),
            disk_used_bytes: disk,
            write_ops: 0,
            read_ops: 0,
            cpu_millis: 0,
            memory_used_bytes: 0,
            replica_count: replicas,
            leader_count: leaders,
            ai_index_memory_bytes: 0,
        }
    }

    fn rebalance_fixture() -> (Vec<TabletDescriptor>, Vec<NodeDescriptor>, Vec<NodeLoad>) {
        let nodes = vec![node(1, ""), node(2, ""), node(3, ""), node(4, "")];
        let tablets = vec![
            tablet(1, &[1, 2, 3], Some(1)),
            tablet(2, &[1, 2, 3], Some(2)),
            // A single-voter tablet: never auto-rebalanced.
            tablet(3, &[1], Some(1)),
        ];
        let loads = vec![
            load(1, 1_000, 6, 2),
            load(2, 10, 1, 1),
            load(3, 10, 1, 0),
            load(4, 10, 0, 0),
        ];
        (tablets, nodes, loads)
    }

    fn default_policy(_: TableId) -> PlacementPolicy {
        PlacementPolicy::default()
    }

    #[test]
    fn rebalancer_moves_replicas_off_a_hot_node_without_dropping_quorum() {
        let (tablets, nodes, loads) = rebalance_fixture();
        let config = RebalanceConfig {
            max_concurrent_moves: 4,
            ..RebalanceConfig::default()
        };
        let plan = plan_rebalance(&tablets, &nodes, &loads, &default_policy, &config);
        // Both three-voter tablets move off node 1; the single-voter tablet
        // is skipped; node 4 is the only eligible target.
        assert_eq!(plan.moves.len(), 2);
        for movement in &plan.moves {
            assert_eq!(movement.from, node_id(1));
            assert_eq!(movement.to, node_id(4));
            assert_eq!(
                movement.steps.first(),
                Some(&MovementStep::AddLearner {
                    tablet_id: movement.tablet_id,
                    node_id: node_id(4),
                })
            );
            assert_eq!(
                movement.steps.last(),
                Some(&MovementStep::RemoveReplica {
                    tablet_id: movement.tablet_id,
                    node_id: node_id(1),
                })
            );
        }

        // Simulate every planned protocol: at every step the healthy voters
        // stay at or above the quorum of the configuration.
        for movement in &plan.moves {
            let tablet = tablets
                .iter()
                .find(|tablet| tablet.tablet_id == movement.tablet_id)
                .unwrap();
            let mut voters: BTreeSet<NodeId> =
                tablet.voters().map(|replica| replica.node_id).collect();
            let mut learners: BTreeSet<NodeId> = BTreeSet::new();
            for step in &movement.steps {
                match step {
                    MovementStep::AddLearner { node_id, .. } => {
                        assert!(learners.insert(*node_id));
                    }
                    MovementStep::CatchUp { .. } => {}
                    MovementStep::PromoteLearner { node_id, .. } => {
                        assert!(learners.remove(node_id));
                        assert!(voters.insert(*node_id));
                    }
                    MovementStep::TransferLeadership { .. } => {}
                    MovementStep::RemoveReplica { node_id, .. } => {
                        assert!(
                            check_move_safety(voters.len() as u32, VoterChange::RemoveVoter)
                                .is_ok()
                        );
                        assert!(voters.remove(node_id));
                    }
                }
                assert!(voters.len() as u32 >= quorum_size(voters.len() as u32));
            }
            assert_eq!(voters.len(), 3);
        }

        // The leader-hint tablet transfers leadership before removal.
        let led = plan
            .moves
            .iter()
            .find(|movement| movement.tablet_id == tablet_id(1))
            .unwrap();
        assert!(led.steps.contains(&MovementStep::TransferLeadership {
            tablet_id: tablet_id(1),
            from: node_id(1),
            to: node_id(4),
        }));
        // The other tablet does not (its leader is node 2).
        let unled = plan
            .moves
            .iter()
            .find(|movement| movement.tablet_id == tablet_id(2))
            .unwrap();
        assert!(!unled
            .steps
            .iter()
            .any(|step| matches!(step, MovementStep::TransferLeadership { .. })));
    }

    #[test]
    fn rebalancer_is_deterministic_and_bounded_by_max_concurrent_moves() {
        let (tablets, nodes, loads) = rebalance_fixture();
        let config = RebalanceConfig {
            max_concurrent_moves: 1,
            ..RebalanceConfig::default()
        };
        let first = plan_rebalance(&tablets, &nodes, &loads, &default_policy, &config);
        let second = plan_rebalance(&tablets, &nodes, &loads, &default_policy, &config);
        assert_eq!(first, second);
        assert_eq!(first.moves.len(), 1);
        // Hottest source first: the move is off node 1.
        assert_eq!(first.moves[0].from, node_id(1));

        // No moves allowed, no moves planned.
        let disabled = RebalanceConfig {
            max_concurrent_moves: 0,
            ..RebalanceConfig::default()
        };
        assert!(plan_rebalance(&tablets, &nodes, &loads, &default_policy, &disabled).is_empty());
    }

    #[test]
    fn rebalancer_leaves_a_balanced_cluster_alone() {
        let nodes = vec![node(1, ""), node(2, ""), node(3, "")];
        let tablets = vec![tablet(1, &[1, 2, 3], Some(1))];
        let loads = vec![load(1, 10, 1, 0), load(2, 10, 1, 0), load(3, 10, 1, 0)];
        let plan = plan_rebalance(
            &tablets,
            &nodes,
            &loads,
            &default_policy,
            &RebalanceConfig::default(),
        );
        assert!(plan.is_empty());
    }

    #[test]
    fn rebalancer_never_targets_unreported_or_prohibited_nodes() {
        let (tablets, nodes, loads) = rebalance_fixture();
        let config = RebalanceConfig {
            max_concurrent_moves: 4,
            ..RebalanceConfig::default()
        };
        // Node 4 is prohibited for the table: the plan must find no target.
        let prohibiting = |_: TableId| PlacementPolicy {
            prohibited_nodes: vec![node_id(4)],
            ..PlacementPolicy::default()
        };
        assert!(plan_rebalance(&tablets, &nodes, &loads, &prohibiting, &config).is_empty());

        // Node 4 reported no loads: it is never a target (fail closed).
        let loads_without_four: Vec<NodeLoad> = loads
            .into_iter()
            .filter(|load| load.node_id != node_id(4))
            .collect();
        assert!(plan_rebalance(
            &tablets,
            &nodes,
            &loads_without_four,
            &default_policy,
            &config
        )
        .is_empty());
    }

    // -- serde -------------------------------------------------------------------

    #[test]
    fn placement_records_round_trip_serde() {
        let policy = PlacementPolicy {
            replicas: 5,
            voter_constraints: vec![LocalityConstraint::required("zone", "a")],
            leader_preferences: vec![LocalityConstraint::preferred("region", "r1")],
            prohibited_nodes: vec![node_id(9)],
        };
        let json = serde_json::to_vec(&policy).unwrap();
        assert_eq!(
            serde_json::from_slice::<PlacementPolicy>(&json).unwrap(),
            policy
        );

        let plan = RebalancePlan {
            moves: vec![ReplicaMove {
                tablet_id: tablet_id(1),
                from: node_id(1),
                to: node_id(4),
                steps: vec![
                    MovementStep::AddLearner {
                        tablet_id: tablet_id(1),
                        node_id: node_id(4),
                    },
                    MovementStep::CatchUp {
                        tablet_id: tablet_id(1),
                        node_id: node_id(4),
                    },
                    MovementStep::PromoteLearner {
                        tablet_id: tablet_id(1),
                        node_id: node_id(4),
                    },
                    MovementStep::RemoveReplica {
                        tablet_id: tablet_id(1),
                        node_id: node_id(1),
                    },
                ],
            }],
        };
        let json = serde_json::to_vec(&plan).unwrap();
        assert_eq!(
            serde_json::from_slice::<RebalancePlan>(&json).unwrap(),
            plan
        );
    }
}
