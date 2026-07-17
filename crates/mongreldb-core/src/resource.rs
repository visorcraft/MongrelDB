//! Workload classes and resource groups (spec section 10.5, S1E-001/S1E-002).
//!
//! Implemented in the Stage 1E wave: every unit of work admitted to the node is
//! classified into a [`WorkloadClass`] (S1E-001) and runs inside a
//! [`ResourceGroup`] (S1E-002) that bounds its concurrency, queue depth,
//! memory, temporary disk, CPU/work, and result size (spec section 4.9 —
//! bounded resources).
//!
//! The resource hierarchy is `node → tenant → resource group → query`, carried
//! as typed handles ([`NodeHandle`], [`TenantHandle`],
//! [`ResourceGroupHandle`], [`QueryHandle`]) so a handle for one level can
//! never be confused with another and every query can walk back up to its
//! node.
//!
//! [`ResourceGroupRegistry`] holds the node-local groups with configured
//! defaults: control and replication groups are pinned with reserved capacity
//! (spec section 13.1 — "control and replication have reserved capacity" — and
//! section 4.9). Groups serialize through serde so they can later be
//! replicated as cluster settings (S1F-001 catalog work).

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use mongreldb_types::ids::{NodeId, QueryId};
use serde::{Deserialize, Serialize};

/// The workload class of a unit of admitted work (spec section 10.5, S1E-001).
///
/// The variant set is exactly the spec's; scheduling and memory-governance
/// policy is derived from it. Serde form is the variant name, stable for
/// cluster-settings replication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum WorkloadClass {
    /// Node control plane (membership, catalog, health). Reserved capacity.
    Control,
    /// Replication traffic (log shipping, follower reads). Reserved capacity.
    Replication,
    /// Foreground point reads/writes.
    Oltp,
    /// Foreground ad-hoc SQL.
    InteractiveSql,
    /// ANN/full-text candidate retrieval and scoring.
    AiRetrieval,
    /// Large scans and aggregations.
    Analytics,
    /// Compaction, GC, index rebuilds. Yields to foreground work (§13.1).
    Maintenance,
    /// Backup and export.
    Backup,
}

impl WorkloadClass {
    /// Every class, in declaration order.
    pub const ALL: [WorkloadClass; 8] = [
        WorkloadClass::Control,
        WorkloadClass::Replication,
        WorkloadClass::Oltp,
        WorkloadClass::InteractiveSql,
        WorkloadClass::AiRetrieval,
        WorkloadClass::Analytics,
        WorkloadClass::Maintenance,
        WorkloadClass::Backup,
    ];

    /// Stable lowercase name (matches the scheduler queue names of §13.1).
    pub fn name(self) -> &'static str {
        match self {
            WorkloadClass::Control => "control",
            WorkloadClass::Replication => "replication",
            WorkloadClass::Oltp => "oltp",
            WorkloadClass::InteractiveSql => "interactive_sql",
            WorkloadClass::AiRetrieval => "ai_retrieval",
            WorkloadClass::Analytics => "analytics",
            WorkloadClass::Maintenance => "maintenance",
            WorkloadClass::Backup => "backup",
        }
    }

    /// Classes whose capacity is reserved and must never be fully starved
    /// (spec §13.1: "control and replication have reserved capacity").
    pub fn has_reserved_capacity(self) -> bool {
        matches!(self, WorkloadClass::Control | WorkloadClass::Replication)
    }

    /// Deferrable classes: the first to be rejected and throttled under
    /// pressure (spec §13.1 maintenance yields to foreground work; §10.5
    /// S1E-003 escalation step 1 rejects new low-priority work).
    pub fn is_low_priority(self) -> bool {
        matches!(
            self,
            WorkloadClass::Analytics | WorkloadClass::Maintenance | WorkloadClass::Backup
        )
    }
}

impl fmt::Display for WorkloadClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Errors of resource-group validation and registry operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResourceError {
    /// The group name was empty.
    #[error("resource group name must not be empty")]
    EmptyName,
    /// `cpu_weight` was zero (§4.9 requires an explicit CPU/work bound).
    #[error("resource group {name:?}: cpu_weight must be nonzero (§4.9 bounded CPU/work)")]
    ZeroCpuWeight {
        /// Offending group name.
        name: String,
    },
    /// `work_units` was zero (§4.9 requires an explicit CPU/work bound).
    #[error("resource group {name:?}: work_units must be nonzero (§4.9 bounded CPU/work)")]
    ZeroWorkUnits {
        /// Offending group name.
        name: String,
    },
    /// A pinned control/replication group was removed or stripped of its
    /// reserved capacity (spec §13.1).
    #[error(
        "resource group {name:?} is pinned: control and replication capacity is reserved (§13.1)"
    )]
    Pinned {
        /// Pinned group name.
        name: String,
    },
    /// No group with that name is registered.
    #[error("resource group {name:?} not found")]
    NotFound {
        /// Missing group name.
        name: String,
    },
}

/// Bounds for one class of admitted work (spec section 10.5, S1E-002).
///
/// The field set is exactly the spec's. `priority` is a `u8`, so the spec's
/// `0..=255` range is enforced by the type itself (and by serde on the
/// cluster-settings path); [`validate`](Self::validate) covers the remaining
/// invariants: non-empty name and nonzero weights.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceGroup {
    /// Unique (per tenant) group name.
    pub name: String,
    /// Maximum concurrently executing queries.
    pub max_concurrency: usize,
    /// Maximum queued queries awaiting a concurrency slot.
    pub max_queue: usize,
    /// Memory budget in bytes (enforced via the memory governor, S1E-003).
    pub memory_bytes: u64,
    /// Temporary-disk budget in bytes (spill manager, S1E-004).
    pub temporary_disk_bytes: u64,
    /// Work-unit budget (weighted CPU/I/O accounting units).
    pub work_units: u64,
    /// CPU scheduling weight (relative share; must be nonzero).
    pub cpu_weight: u32,
    /// Scheduling priority, `0..=255` (higher runs first).
    pub priority: u8,
    /// Maximum bytes of one query's result.
    pub max_result_bytes: u64,
}

impl ResourceGroup {
    /// Checks the invariants not carried by the field types: non-empty name
    /// and nonzero weights (priority's `0..=255` range is the `u8` type).
    pub fn validate(&self) -> Result<(), ResourceError> {
        if self.name.is_empty() {
            return Err(ResourceError::EmptyName);
        }
        if self.cpu_weight == 0 {
            return Err(ResourceError::ZeroCpuWeight {
                name: self.name.clone(),
            });
        }
        if self.work_units == 0 {
            return Err(ResourceError::ZeroWorkUnits {
                name: self.name.clone(),
            });
        }
        Ok(())
    }

    /// The configured default group for a workload class (spec §13.1 queue
    /// set). Control and replication receive generous, always-reserved
    /// capacity; foreground classes outrank analytics, which outranks
    /// maintenance and backup. Operators reconfigure these through the
    /// registry; the numbers are starting points, not policy.
    pub fn for_class(class: WorkloadClass) -> Self {
        let (name, max_concurrency, max_queue, memory_mib, work_units, cpu_weight, priority) =
            match class {
                WorkloadClass::Control => {
                    ("control", 8usize, 64usize, 256u64, 1 << 20, 256u32, 255u8)
                }
                WorkloadClass::Replication => ("replication", 8, 64, 512, 1 << 20, 256, 254),
                WorkloadClass::Oltp => ("oltp", 64, 256, 1024, 1 << 20, 128, 200),
                WorkloadClass::InteractiveSql => {
                    ("interactive_sql", 16, 64, 1024, 1 << 19, 64, 180)
                }
                WorkloadClass::AiRetrieval => ("ai_retrieval", 8, 32, 1024, 1 << 18, 32, 150),
                WorkloadClass::Analytics => ("analytics", 4, 16, 2048, 1 << 17, 16, 100),
                WorkloadClass::Maintenance => ("maintenance", 2, 8, 512, 1 << 16, 8, 50),
                WorkloadClass::Backup => ("backup", 1, 4, 256, 1 << 16, 4, 40),
            };
        Self {
            name: name.to_string(),
            max_concurrency,
            max_queue,
            memory_bytes: memory_mib * 1024 * 1024,
            temporary_disk_bytes: 1024 * 1024 * 1024,
            work_units,
            cpu_weight,
            priority,
            max_result_bytes: 1024 * 1024 * 1024,
        }
    }
}

/// Numeric tenant identifier. The zero value is reserved.
///
/// Mirrors the `id64` style of `mongreldb-types` (numeric IDs allocated
/// through replicated catalog state); it moves there with the cluster id work
/// (spec section 7) once tenants become a catalog object.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TenantId(pub u64);

impl TenantId {
    /// The reserved zero value.
    pub const ZERO: Self = Self(0);

    /// Wraps a raw value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TenantId({})", self.0)
    }
}

impl FromStr for TenantId {
    type Err = std::num::ParseIntError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.parse::<u64>().map(Self)
    }
}

/// Typed handle to the node level of the resource hierarchy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeHandle {
    id: NodeId,
}

impl NodeHandle {
    /// A handle for the node `id`.
    pub fn new(id: NodeId) -> Self {
        Self { id }
    }

    /// The node's cluster-wide identifier.
    pub fn id(self) -> NodeId {
        self.id
    }

    /// Descend to tenant `tenant` on this node.
    pub fn tenant(self, tenant: TenantId) -> TenantHandle {
        TenantHandle {
            node: self,
            id: tenant,
        }
    }
}

/// Typed handle to the tenant level (`node → tenant`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TenantHandle {
    node: NodeHandle,
    id: TenantId,
}

impl TenantHandle {
    /// The node this tenant handle is scoped to.
    pub fn node(self) -> NodeHandle {
        self.node
    }

    /// The tenant identifier.
    pub fn id(self) -> TenantId {
        self.id
    }

    /// Descend to the resource group `name` within this tenant.
    pub fn group(self, name: impl Into<String>) -> ResourceGroupHandle {
        ResourceGroupHandle {
            tenant: self,
            name: name.into(),
        }
    }
}

/// Typed handle to the resource-group level (`node → tenant → group`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceGroupHandle {
    tenant: TenantHandle,
    name: String,
}

impl ResourceGroupHandle {
    /// The tenant this group belongs to.
    pub fn tenant(&self) -> TenantHandle {
        self.tenant
    }

    /// The node this group is scoped to.
    pub fn node(&self) -> NodeHandle {
        self.tenant.node()
    }

    /// The group name (registry lookup key).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Descend to query `id` admitted into this group under `class`.
    pub fn query(&self, id: QueryId, class: WorkloadClass) -> QueryHandle {
        QueryHandle {
            group: self.clone(),
            id,
            class,
        }
    }
}

/// Typed handle to the query level (`node → tenant → group → query`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueryHandle {
    group: ResourceGroupHandle,
    id: QueryId,
    class: WorkloadClass,
}

impl QueryHandle {
    /// The resource group this query runs in.
    pub fn group(&self) -> &ResourceGroupHandle {
        &self.group
    }

    /// The query's cluster-wide identifier.
    pub fn id(&self) -> QueryId {
        self.id
    }

    /// The workload class the query was admitted under.
    pub fn class(&self) -> WorkloadClass {
        self.class
    }

    /// The tenant this query runs under.
    pub fn tenant(&self) -> TenantHandle {
        self.group.tenant()
    }

    /// The node this query runs on.
    pub fn node(&self) -> NodeHandle {
        self.group.node()
    }
}

/// Node-local registry of resource groups (S1E-002), with configured defaults
/// for every workload class (§13.1 queue set).
///
/// The control and replication groups are **pinned**: they cannot be removed
/// and cannot be re-registered without reserved (`max_concurrency > 0` and
/// `memory_bytes > 0`) capacity — spec §13.1 "control and replication have
/// reserved capacity" and §4.9 bounded resources.
///
/// Serde form is a JSON/object map of name → group (sorted by name, so the
/// serialized form is deterministic) for later cluster-settings replication
/// (S1F-001). Deserialization re-asserts the pinned defaults: a replicated
/// settings document that omits or starves the control/replication groups is
/// rejected rather than applied.
pub struct ResourceGroupRegistry {
    groups: parking_lot::RwLock<BTreeMap<String, ResourceGroup>>,
}

/// Group names pinned with reserved capacity (§13.1).
const PINNED_GROUP_NAMES: [&str; 2] = ["control", "replication"];

impl ResourceGroupRegistry {
    /// An empty registry (no defaults). Prefer [`with_defaults`](Self::with_defaults)
    /// for any serving node.
    pub fn new() -> Self {
        Self {
            groups: parking_lot::RwLock::new(BTreeMap::new()),
        }
    }

    /// A registry seeded with the configured default group of every workload
    /// class (§13.1), control and replication pinned with reserved capacity.
    pub fn with_defaults() -> Self {
        let registry = Self::new();
        for class in WorkloadClass::ALL {
            let group = ResourceGroup::for_class(class);
            registry.groups.write().insert(group.name.clone(), group);
        }
        registry
    }

    /// Registers (or replaces) a group after validation. Replacing a pinned
    /// control/replication group is rejected unless the replacement keeps
    /// reserved (`max_concurrency > 0` and `memory_bytes > 0`) capacity.
    pub fn register(&self, group: ResourceGroup) -> Result<(), ResourceError> {
        group.validate()?;
        if PINNED_GROUP_NAMES.contains(&group.name.as_str())
            && (group.max_concurrency == 0 || group.memory_bytes == 0)
        {
            return Err(ResourceError::Pinned {
                name: group.name.clone(),
            });
        }
        self.groups.write().insert(group.name.clone(), group);
        Ok(())
    }

    /// Looks up a group by name.
    pub fn get(&self, name: &str) -> Option<ResourceGroup> {
        self.groups.read().get(name).cloned()
    }

    /// Like [`get`](Self::get), but a typed error for the not-found case.
    pub fn lookup(&self, name: &str) -> Result<ResourceGroup, ResourceError> {
        self.get(name).ok_or_else(|| ResourceError::NotFound {
            name: name.to_string(),
        })
    }

    /// Removes a group by name. Pinned control/replication groups cannot be
    /// removed (§13.1 reserved capacity). Returns `true` if a group was
    /// removed.
    pub fn remove(&self, name: &str) -> Result<bool, ResourceError> {
        if PINNED_GROUP_NAMES.contains(&name) {
            return Err(ResourceError::Pinned {
                name: name.to_string(),
            });
        }
        Ok(self.groups.write().remove(name).is_some())
    }

    /// Sorted names of every registered group.
    pub fn names(&self) -> Vec<String> {
        self.groups.read().keys().cloned().collect()
    }

    /// Number of registered groups.
    pub fn len(&self) -> usize {
        self.groups.read().len()
    }

    /// Whether no groups are registered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for ResourceGroupRegistry {
    fn default() -> Self {
        Self::with_defaults()
    }
}

impl fmt::Debug for ResourceGroupRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResourceGroupRegistry")
            .field("groups", &*self.groups.read())
            .finish()
    }
}

impl Serialize for ResourceGroupRegistry {
    /// Serializes as a name → group map (`BTreeMap` order: deterministic).
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.groups.read().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ResourceGroupRegistry {
    /// Restores a registry from its map form, then re-asserts the pinned
    /// control/replication defaults: a document that omits or starves them is
    /// rejected (reserved capacity is a safety invariant, not a preference).
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let groups = BTreeMap::<String, ResourceGroup>::deserialize(deserializer)?;
        let registry = Self::new();
        for (_, group) in groups {
            registry.register(group).map_err(serde::de::Error::custom)?;
        }
        for pinned in PINNED_GROUP_NAMES {
            if registry.get(pinned).is_none() {
                return Err(serde::de::Error::custom(format!(
                    "pinned resource group {pinned:?} missing (§13.1 reserved capacity)"
                )));
            }
        }
        Ok(registry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_group(name: &str) -> ResourceGroup {
        ResourceGroup {
            name: name.to_string(),
            max_concurrency: 4,
            max_queue: 16,
            memory_bytes: 1 << 20,
            temporary_disk_bytes: 1 << 20,
            work_units: 100,
            cpu_weight: 1,
            priority: 128,
            max_result_bytes: 1 << 20,
        }
    }

    #[test]
    fn workload_class_set_matches_spec() {
        // S1E-001: exactly these eight classes.
        assert_eq!(WorkloadClass::ALL.len(), 8);
        let names: Vec<_> = WorkloadClass::ALL.iter().map(|c| c.name()).collect();
        assert_eq!(
            names,
            vec![
                "control",
                "replication",
                "oltp",
                "interactive_sql",
                "ai_retrieval",
                "analytics",
                "maintenance",
                "backup"
            ]
        );
        // Names are unique.
        let mut deduped = names.clone();
        deduped.sort_unstable();
        deduped.dedup();
        assert_eq!(deduped.len(), 8);
    }

    #[test]
    fn control_and_replication_are_the_reserved_classes() {
        for class in WorkloadClass::ALL {
            assert_eq!(
                class.has_reserved_capacity(),
                matches!(class, WorkloadClass::Control | WorkloadClass::Replication),
                "reserved capacity: {class}"
            );
        }
    }

    #[test]
    fn low_priority_classes_are_deferrable() {
        for class in WorkloadClass::ALL {
            assert_eq!(
                class.is_low_priority(),
                matches!(
                    class,
                    WorkloadClass::Analytics | WorkloadClass::Maintenance | WorkloadClass::Backup
                ),
                "low priority: {class}"
            );
        }
    }

    #[test]
    fn workload_class_serde_round_trip() {
        for class in WorkloadClass::ALL {
            let json = serde_json::to_string(&class).unwrap();
            let back: WorkloadClass = serde_json::from_str(&json).unwrap();
            assert_eq!(back, class);
        }
        assert_eq!(
            serde_json::to_string(&WorkloadClass::AiRetrieval).unwrap(),
            "\"AiRetrieval\""
        );
    }

    #[test]
    fn validation_accepts_a_valid_group() {
        assert!(valid_group("g").validate().is_ok());
        // Priority is u8: 0 and 255 are both valid (range is the type).
        for priority in [0u8, 255] {
            let mut g = valid_group("g");
            g.priority = priority;
            assert!(g.validate().is_ok());
        }
    }

    #[test]
    fn validation_rejects_empty_name() {
        let mut g = valid_group("");
        g.name.clear();
        assert_eq!(g.validate(), Err(ResourceError::EmptyName));
    }

    #[test]
    fn validation_rejects_zero_weights() {
        let mut g = valid_group("g");
        g.cpu_weight = 0;
        assert_eq!(
            g.validate(),
            Err(ResourceError::ZeroCpuWeight { name: "g".into() })
        );
        let mut g = valid_group("g");
        g.work_units = 0;
        assert_eq!(
            g.validate(),
            Err(ResourceError::ZeroWorkUnits { name: "g".into() })
        );
    }

    #[test]
    fn resource_group_serde_round_trip_and_priority_range() {
        let g = valid_group("oltp");
        let json = serde_json::to_string(&g).unwrap();
        let back: ResourceGroup = serde_json::from_str(&json).unwrap();
        assert_eq!(back, g);
        // The u8 type enforces the spec's 0..=255 priority range on the
        // (untrusted) cluster-settings path.
        let too_big = json.replace("\"priority\":128", "\"priority\":256");
        assert!(serde_json::from_str::<ResourceGroup>(&too_big).is_err());
    }

    #[test]
    fn registry_defaults_cover_every_class() {
        let registry = ResourceGroupRegistry::with_defaults();
        assert_eq!(registry.len(), WorkloadClass::ALL.len());
        for class in WorkloadClass::ALL {
            let group = registry
                .get(ResourceGroup::for_class(class).name.as_str())
                .unwrap_or_else(|| panic!("default group for {class}"));
            assert!(group.validate().is_ok());
            assert!(group.max_concurrency > 0);
            assert!(group.memory_bytes > 0);
        }
        assert!(registry.lookup("no-such-group").is_err());
    }

    #[test]
    fn control_and_replication_defaults_have_top_priority() {
        let registry = ResourceGroupRegistry::with_defaults();
        let control = registry.lookup("control").unwrap();
        let replication = registry.lookup("replication").unwrap();
        assert_eq!(control.priority, 255);
        assert_eq!(replication.priority, 254);
        for name in registry.names() {
            if name != "control" && name != "replication" {
                let g = registry.lookup(&name).unwrap();
                assert!(
                    g.priority < replication.priority,
                    "{name} outranks replication"
                );
            }
        }
    }

    #[test]
    fn pinned_groups_cannot_be_removed_or_starved() {
        let registry = ResourceGroupRegistry::with_defaults();
        for pinned in ["control", "replication"] {
            assert_eq!(
                registry.remove(pinned),
                Err(ResourceError::Pinned {
                    name: pinned.into()
                })
            );
            // Re-registration is allowed only while reserved capacity is kept.
            let mut starved = registry.lookup(pinned).unwrap();
            starved.max_concurrency = 0;
            assert_eq!(
                registry.register(starved),
                Err(ResourceError::Pinned {
                    name: pinned.into()
                })
            );
            let mut starved = registry.lookup(pinned).unwrap();
            starved.memory_bytes = 0;
            assert_eq!(
                registry.register(starved),
                Err(ResourceError::Pinned {
                    name: pinned.into()
                })
            );
            // A capacity-preserving replacement is accepted.
            let mut kept = registry.lookup(pinned).unwrap();
            kept.max_concurrency += 1;
            assert!(registry.register(kept.clone()).is_ok());
            assert_eq!(registry.lookup(pinned).unwrap(), kept);
        }
        // A normal group can be removed.
        registry.register(valid_group("scratch")).unwrap();
        assert_eq!(registry.remove("scratch"), Ok(true));
        assert_eq!(registry.remove("scratch"), Ok(false));
    }

    #[test]
    fn register_validates_groups() {
        let registry = ResourceGroupRegistry::new();
        let mut bad = valid_group("bad");
        bad.cpu_weight = 0;
        assert!(matches!(
            registry.register(bad),
            Err(ResourceError::ZeroCpuWeight { .. })
        ));
        assert!(registry.get("bad").is_none());
    }

    #[test]
    fn registry_serde_round_trip_preserves_groups() {
        let registry = ResourceGroupRegistry::with_defaults();
        registry.register(valid_group("tenant_a")).unwrap();
        let json = serde_json::to_string(&registry).unwrap();
        let back: ResourceGroupRegistry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.names(), registry.names());
        assert_eq!(back.lookup("tenant_a").unwrap(), valid_group("tenant_a"));
        // Deterministic serialization (sorted map): same registry, same bytes.
        assert_eq!(serde_json::to_string(&back).unwrap(), json);
    }

    #[test]
    fn registry_deserialization_re_enforces_pinned_defaults() {
        // A replicated settings document missing the pinned groups is rejected.
        let bare = serde_json::json!({ "oltp": valid_group("oltp") });
        assert!(serde_json::from_value::<ResourceGroupRegistry>(bare).is_err());
        // One that starves them is rejected too.
        let mut starved = valid_group("control");
        starved.memory_bytes = 0;
        let doc =
            serde_json::json!({ "control": starved, "replication": valid_group("replication") });
        assert!(serde_json::from_value::<ResourceGroupRegistry>(doc).is_err());
    }

    #[test]
    fn hierarchy_handles_walk_up_the_chain() {
        let node = NodeHandle::new(NodeId::new_random());
        let tenant = node.tenant(TenantId::new(7));
        let group = tenant.group("oltp");
        let query_id = QueryId::new_random();
        let query = group.query(query_id, WorkloadClass::Oltp);

        // Every level reaches back up to the node.
        assert_eq!(tenant.node(), node);
        assert_eq!(tenant.id(), TenantId::new(7));
        assert_eq!(group.tenant(), tenant);
        assert_eq!(group.node(), node);
        assert_eq!(group.name(), "oltp");
        assert_eq!(query.group(), &group);
        assert_eq!(query.id(), query_id);
        assert_eq!(query.class(), WorkloadClass::Oltp);
        assert_eq!(query.tenant(), tenant);
        assert_eq!(query.node(), node);
        assert_eq!(node.id(), node.id());
    }

    #[test]
    fn hierarchy_handles_serde_round_trip() {
        let node = NodeHandle::new(NodeId::from_bytes([0xAB; 16]));
        let query = node
            .tenant(TenantId::new(42))
            .group("analytics")
            .query(QueryId::from_bytes([0xCD; 16]), WorkloadClass::Analytics);
        let json = serde_json::to_string(&query).unwrap();
        let back: QueryHandle = serde_json::from_str(&json).unwrap();
        assert_eq!(back, query);
        assert_eq!(back.node().id(), NodeId::from_bytes([0xAB; 16]));
        assert_eq!(back.tenant().id(), TenantId::new(42));
    }

    #[test]
    fn tenant_id_forms() {
        let id = TenantId::new(123);
        assert_eq!(id.get(), 123);
        assert_eq!(id.to_string(), "123");
        assert_eq!(format!("{id:?}"), "TenantId(123)");
        assert_eq!("123".parse::<TenantId>().unwrap(), id);
        assert_eq!(TenantId::ZERO.get(), 0);
    }
}
