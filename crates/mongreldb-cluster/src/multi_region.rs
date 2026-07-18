//! Multi-region placement and read policies (spec section 13.7, Stage 4G).
//!
//! Placement tiers: region / availability zone / rack / node. Default write
//! mode remains one leader per tablet — multi-leader concurrent row writes
//! are never the default. Provides regional RYW, bounded-staleness local
//! reads, async DR replicas, and clock-skew / RTT monitoring hooks.

use std::collections::BTreeMap;
use std::str::FromStr;

use mongreldb_types::ids::NodeId;
use serde::{Deserialize, Serialize};

use crate::node::{Locality, NodeDescriptor, NodeState};
use crate::placement::{PlacementError, PlacementPolicy};

/// Voter-distribution policy across regions (spec §13.7).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VoterDistribution {
    /// All voters in one region (e.g. 3 voters in `us-central`).
    SingleRegion {
        /// Home region.
        region: String,
        /// Voter count.
        voters: u32,
    },
    /// Voters spread across regions (e.g. 5 voters across 3 regions).
    MultiRegion {
        /// region → voter count.
        voters_per_region: BTreeMap<String, u32>,
    },
}

impl VoterDistribution {
    /// Total configured voters.
    pub fn total_voters(&self) -> u32 {
        match self {
            Self::SingleRegion { voters, .. } => *voters,
            Self::MultiRegion { voters_per_region } => voters_per_region.values().sum(),
        }
    }
}

/// Multi-region placement policy for one table/database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiRegionPolicy {
    /// How voters are distributed.
    pub voters: VoterDistribution,
    /// Tenant home region (prefer leaders here).
    pub tenant_home_region: Option<String>,
    /// Whether regional follower (non-voter) replicas are desired.
    pub regional_followers: bool,
    /// Async DR regions (apply log, never serve linearizable writes).
    pub async_dr_regions: Vec<String>,
    /// Leader preference: prefer home region when healthy.
    pub prefer_home_leader: bool,
}

impl Default for MultiRegionPolicy {
    fn default() -> Self {
        Self {
            voters: VoterDistribution::SingleRegion {
                region: "default".into(),
                voters: 3,
            },
            tenant_home_region: None,
            regional_followers: false,
            async_dr_regions: Vec::new(),
            prefer_home_leader: true,
        }
    }
}

/// Read policy for multi-region requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MultiRegionReadPolicy {
    /// Linearizable global (must hit leader).
    LinearizableGlobal,
    /// Regional read-your-writes (home region or token-bearing replica).
    RegionalReadYourWrites,
    /// Bounded-staleness local reads.
    BoundedStalenessLocal {
        /// Max lag milliseconds.
        max_lag_ms: u64,
    },
    /// Async DR / eventual.
    AsyncDr,
}

/// Clock-skew / RTT sample for monitoring (spec §13.7).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionHealthSample {
    /// Region name.
    pub region: String,
    /// Observed clock skew micros vs local.
    pub clock_skew_micros: u64,
    /// RTT to region millis.
    pub rtt_ms: u64,
    /// Whether skew exceeds the configured limit.
    pub skew_exceeded: bool,
}

/// Validate a multi-region voter distribution against live nodes.
pub fn validate_multi_region(
    policy: &MultiRegionPolicy,
    nodes: &[NodeDescriptor],
) -> Result<(), PlacementError> {
    let up: Vec<&NodeDescriptor> = nodes.iter().filter(|n| n.state == NodeState::Up).collect();
    match &policy.voters {
        VoterDistribution::SingleRegion { region, voters } => {
            let count = up
                .iter()
                .filter(|n| n.locality.get("region") == Some(region.as_str()))
                .count() as u32;
            if count < *voters {
                return Err(PlacementError::Infeasible(format!(
                    "region {region} has {count} Up nodes, need {voters} voters"
                )));
            }
        }
        VoterDistribution::MultiRegion { voters_per_region } => {
            for (region, need) in voters_per_region {
                let count = up
                    .iter()
                    .filter(|n| n.locality.get("region") == Some(region.as_str()))
                    .count() as u32;
                if count < *need {
                    return Err(PlacementError::Infeasible(format!(
                        "region {region} has {count} Up nodes, need {need} voters"
                    )));
                }
            }
        }
    }
    let _ = policy.prefer_home_leader;
    Ok(())
}

/// Prefer a leader in the tenant home region when healthy.
pub fn prefer_leader(
    policy: &MultiRegionPolicy,
    candidates: &[(NodeId, String)],
) -> Option<NodeId> {
    if !policy.prefer_home_leader {
        return candidates.first().map(|(id, _)| *id);
    }
    if let Some(home) = &policy.tenant_home_region {
        if let Some((id, _)) = candidates.iter().find(|(_, region)| region == home) {
            return Some(*id);
        }
    }
    candidates.first().map(|(id, _)| *id)
}

/// Convert a multi-region policy into a base [`PlacementPolicy`] replica count.
pub fn placement_from_multi_region(policy: &MultiRegionPolicy) -> PlacementPolicy {
    PlacementPolicy {
        replicas: u8::try_from(policy.voters.total_voters().max(1)).unwrap_or(u8::MAX),
        ..Default::default()
    }
}

/// Build a locality string `region=X,zone=Y`.
pub fn locality_of(region: &str, zone: Option<&str>) -> Locality {
    let text = match zone {
        Some(z) => format!("region={region},zone={z}"),
        None => format!("region={region}"),
    };
    Locality::from_str(&text).expect("well-formed locality")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{BuildVersion, NodeCapacity, NodeDescriptor, NodeState, VersionInfo};
    use mongreldb_types::ids::NodeId;

    fn node(byte: u8, region: &str, state: NodeState) -> NodeDescriptor {
        NodeDescriptor {
            node_id: NodeId::from_bytes({
                let mut b = [0u8; 16];
                b[15] = byte;
                b
            }),
            rpc_address: format!("n{byte}:8453"),
            locality: locality_of(region, None),
            capacity: NodeCapacity::default(),
            state,
            version: BuildVersion::current(),
            version_info: VersionInfo::current(),
        }
    }

    #[test]
    fn single_region_requires_enough_up_nodes() {
        let policy = MultiRegionPolicy {
            voters: VoterDistribution::SingleRegion {
                region: "us".into(),
                voters: 3,
            },
            ..MultiRegionPolicy::default()
        };
        let nodes = vec![
            node(1, "us", NodeState::Up),
            node(2, "us", NodeState::Up),
            node(3, "eu", NodeState::Up),
        ];
        assert!(validate_multi_region(&policy, &nodes).is_err());
        let nodes = vec![
            node(1, "us", NodeState::Up),
            node(2, "us", NodeState::Up),
            node(3, "us", NodeState::Up),
        ];
        assert!(validate_multi_region(&policy, &nodes).is_ok());
    }

    #[test]
    fn prefer_home_leader() {
        let policy = MultiRegionPolicy {
            tenant_home_region: Some("eu".into()),
            prefer_home_leader: true,
            ..MultiRegionPolicy::default()
        };
        let a = NodeId::from_bytes([1; 16]);
        let b = NodeId::from_bytes([2; 16]);
        let candidates = vec![(a, "us".into()), (b, "eu".into())];
        assert_eq!(prefer_leader(&policy, &candidates), Some(b));
    }

    #[test]
    fn no_multi_leader_default() {
        let p = MultiRegionPolicy::default();
        assert!(matches!(p.voters, VoterDistribution::SingleRegion { .. }));
        assert_eq!(placement_from_multi_region(&p).replicas, 3);
    }
}
