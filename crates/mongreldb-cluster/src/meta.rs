//! Cluster meta (spec sections 11-12, Stages 2-3).
//!
//! Stage 2H (spec section 11.8, ADR-0010) lands the rolling-upgrade control
//! surface: the cluster feature level and feature registry, the
//! [`FeatureActivation`] record destined to become a replicated catalog
//! command on the meta group, rolling-upgrade planning
//! ([`plan_rolling_upgrade`]), and rollback assessment ([`assess_rollback`]).
//! The full meta control plane — replicated membership, database/tablet
//! descriptors, placement ownership — lands with Stage 3A (spec section
//! 12.1); until then these are library types the node runtime drives.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::NodeId;
use serde::{Deserialize, Serialize};

use crate::node::{Incompatibility, NodeDescriptor, VersionInfo};

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
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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
