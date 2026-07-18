//! Node-level memory governor wiring (spec section 13.2, Stage 4B).
//!
//! Extends the Stage 1E [`MemoryGovernor`] across tablets and queries on one
//! node: aggregates reservations, consumes OS-pressure inputs, and emits
//! leader-move / admission actions when legal load still risks OOM.

use std::collections::BTreeMap;

use mongreldb_types::ids::TabletId;
use serde::{Deserialize, Serialize};

use crate::memory::{EscalationLevel, GovernorStats, MemoryGovernor};

/// Inputs the node governor observes (spec §13.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodePressureInputs {
    /// Physical memory bytes.
    pub physical_memory_bytes: u64,
    /// Configured maximum for the node.
    pub configured_max_bytes: u64,
    /// OS pressure signal (0.0..=1.0).
    pub os_pressure: f64,
    /// Aggregate cache hit rate (0.0..=1.0).
    pub cache_hit_rate: f64,
    /// Sum of query reservations.
    pub query_reserved_bytes: u64,
    /// Compaction backlog bytes.
    pub compaction_backlog_bytes: u64,
    /// Replication backlog bytes.
    pub replication_backlog_bytes: u64,
}

impl Default for NodePressureInputs {
    fn default() -> Self {
        Self {
            physical_memory_bytes: 16 * 1024 * 1024 * 1024,
            configured_max_bytes: 12 * 1024 * 1024 * 1024,
            os_pressure: 0.0,
            cache_hit_rate: 1.0,
            query_reserved_bytes: 0,
            compaction_backlog_bytes: 0,
            replication_backlog_bytes: 0,
        }
    }
}

/// Actions the governor may request (spec §13.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GovernorAction {
    /// Evict reclaimable caches.
    EvictCaches,
    /// Reduce query admission.
    ReduceAdmission,
    /// Spill analytics working sets.
    SpillAnalytics,
    /// Throttle compaction.
    ThrottleCompaction,
    /// Move tablet leaders off this node.
    MoveTabletLeaders {
        /// Suggested tablets to shed (may be empty = any).
        tablets: Vec<TabletId>,
    },
    /// Reject oversized AI requests.
    RejectOversizedAi,
}

/// Node-level governor: one [`MemoryGovernor`] plus tablet accounting.
#[derive(Debug)]
pub struct NodeMemoryGovernor {
    /// Underlying governor.
    pub governor: MemoryGovernor,
    /// Per-tablet reserved bytes.
    tablet_reserved: BTreeMap<TabletId, u64>,
    /// Last computed actions.
    last_actions: Vec<GovernorAction>,
}

impl NodeMemoryGovernor {
    /// Wrap an existing governor.
    pub fn new(governor: MemoryGovernor) -> Self {
        Self {
            governor,
            tablet_reserved: BTreeMap::new(),
            last_actions: Vec::new(),
        }
    }

    /// Record a tablet reservation delta (positive = reserve, negative = release).
    pub fn adjust_tablet(&mut self, tablet: TabletId, delta: i64) {
        let entry = self.tablet_reserved.entry(tablet).or_insert(0);
        if delta >= 0 {
            *entry = entry.saturating_add(delta as u64);
        } else {
            *entry = entry.saturating_sub((-delta) as u64);
        }
        if *entry == 0 {
            self.tablet_reserved.remove(&tablet);
        }
    }

    /// Evaluate inputs and produce ordered actions (escalation ladder).
    pub fn evaluate(&mut self, inputs: &NodePressureInputs) -> Vec<GovernorAction> {
        let stats: GovernorStats = self.governor.stats();
        let used = stats.total_used.saturating_add(inputs.query_reserved_bytes);
        let max = inputs.configured_max_bytes.max(1);
        let pressure = (used as f64 / max as f64).max(inputs.os_pressure);

        let mut actions = Vec::new();
        if pressure >= 0.70 || inputs.os_pressure >= 0.70 {
            actions.push(GovernorAction::EvictCaches);
        }
        if pressure >= 0.80 {
            actions.push(GovernorAction::ReduceAdmission);
            actions.push(GovernorAction::SpillAnalytics);
        }
        if pressure >= 0.85 || inputs.compaction_backlog_bytes > max / 8 {
            actions.push(GovernorAction::ThrottleCompaction);
        }
        if pressure >= 0.90 {
            let tablets: Vec<TabletId> = self.tablet_reserved.keys().copied().collect();
            actions.push(GovernorAction::MoveTabletLeaders { tablets });
            actions.push(GovernorAction::RejectOversizedAi);
        }

        // Escalation level from the inner governor informs the same ladder.
        match self.governor.escalation() {
            EscalationLevel::None => {}
            EscalationLevel::RejectLowPriority => {
                if !actions.contains(&GovernorAction::ReduceAdmission) {
                    actions.push(GovernorAction::ReduceAdmission);
                }
            }
            EscalationLevel::EvictCaches | EscalationLevel::SpillOperators => {
                if !actions.contains(&GovernorAction::EvictCaches) {
                    actions.push(GovernorAction::EvictCaches);
                }
                if !actions.contains(&GovernorAction::SpillAnalytics) {
                    actions.push(GovernorAction::SpillAnalytics);
                }
            }
            EscalationLevel::ThrottleMaintenance => {
                if !actions
                    .iter()
                    .any(|a| matches!(a, GovernorAction::MoveTabletLeaders { .. }))
                {
                    actions.push(GovernorAction::MoveTabletLeaders {
                        tablets: self.tablet_reserved.keys().copied().collect(),
                    });
                }
            }
        }

        self.last_actions = actions.clone();
        actions
    }

    /// Last actions from [`evaluate`].
    pub fn last_actions(&self) -> &[GovernorAction] {
        &self.last_actions
    }

    /// Sum of per-tablet reservations.
    pub fn tablet_reserved_bytes(&self) -> u64 {
        self.tablet_reserved.values().sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::{GovernorConfig, MemoryGovernor};

    fn gov(max: u64) -> NodeMemoryGovernor {
        NodeMemoryGovernor::new(MemoryGovernor::new(GovernorConfig::new(max)).unwrap())
    }

    #[test]
    fn high_pressure_emits_oom_prevention_actions() {
        let mut g = gov(1_000_000);
        let mut inputs = NodePressureInputs {
            configured_max_bytes: 1_000_000,
            query_reserved_bytes: 950_000,
            os_pressure: 0.95,
            ..NodePressureInputs::default()
        };
        let actions = g.evaluate(&inputs);
        assert!(actions
            .iter()
            .any(|a| matches!(a, GovernorAction::EvictCaches)));
        assert!(actions
            .iter()
            .any(|a| matches!(a, GovernorAction::RejectOversizedAi)));
        assert!(actions
            .iter()
            .any(|a| matches!(a, GovernorAction::MoveTabletLeaders { .. })));
        // Legal load still gets prevention actions — not silent OOM.
        inputs.os_pressure = 0.5;
        inputs.query_reserved_bytes = 1000;
        let calm = g.evaluate(&inputs);
        assert!(calm.len() < actions.len());
    }

    #[test]
    fn tablet_accounting() {
        let mut g = gov(10_000_000);
        let t = TabletId::from_bytes([1; 16]);
        g.adjust_tablet(t, 1000);
        assert_eq!(g.tablet_reserved_bytes(), 1000);
        g.adjust_tablet(t, -1000);
        assert_eq!(g.tablet_reserved_bytes(), 0);
    }
}
