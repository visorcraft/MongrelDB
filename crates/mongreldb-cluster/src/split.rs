//! Safe tablet split protocol (spec section 12.5, Stage 3E).
//!
//! One tablet becomes two adjacent child tablets with zero data loss, zero
//! duplication, and no routing window in which a key is unserved or served by
//! two owners. The module maps the spec's eleven steps onto explicit
//! collaborators:
//!
//! ```text
//!  1. Meta marks the source `Splitting`           (TabletMetaPlane::set_tablet)
//!  2. Choose the split key                        (TabletSplitPlanner: explicit or midpoint)
//!  3. Create child descriptors as learners        (Creating; never routable)
//!  4. Pin the source snapshot at `split_ts`       (TabletKeyspace::pin_snapshot)
//!  5. Build child state from the pinned snapshot  (ChildStateSink staged build)
//!  6. Stream/catch up source deltas               (TabletKeyspace::deltas_after)
//!  7. Routing publication barrier                 (the phase machine: no publish before CaughtUp)
//!  8. Publish children + source retirement        (SplitPublishCommand, ONE meta command)
//!  9. Redirect stale requests                     (check_generation + retry_guidance)
//! 10. Retain the source while old pins remain     (SourceRetentionGuard)
//! 11. Remove source replicas                      (Retired + TabletLayout::teardown)
//! ```
//!
//! # Crash safety
//!
//! Every step is idempotent and the executor persists its progress after each
//! phase in a versioned, checksummed `split.json` inside the source tablet
//! directory (the `tablet.json` envelope format is unchanged — the progress
//! record is a sibling, written with the same atomic idiom). After a crash,
//! [`SplitExecutor::resume`] reloads the record and [`SplitExecutor::run`]
//! continues from the persisted phase; completing (or aborting) the split
//! removes the record with the source directory's teardown.
//!
//! # Abort
//!
//! [`abort_split`] unwinds a split that has not yet published: the
//! never-routable children are removed from the meta plane and torn down,
//! the source is republished `Active` through the state graph's documented
//! `Splitting -> Active` rollback edge, and the progress record is cleared.
//! Every step is idempotent, so a crash mid-abort re-enters safely. Once the
//! atomic publication has landed the split cannot abort — rolling back
//! would double-serve the keyspace — and the driver fails closed.
//!
//! Fault hooks (registered in the `mongreldb-fault` catalog):
//!
//! - `tablet.split.before` / `tablet.split.after` bracket the atomic routing
//!   publication (step 8): a `before` failure leaves the split unpublished,
//!   an `after` failure leaves it published but not yet recorded.
//! - `tablet.split.phase.1` ..= `tablet.split.phase.7` fire after each
//!   phase's progress record is durable, in [`SplitPhase`] declaration order
//!   (`MarkedSplitting` = 1 .. `SourceRetired` = 7).
//!
//! # Generation rules
//!
//! Documented on [`TabletDescriptor`]: with `g` the pre-split generation, the
//! source is marked at `g + 1`, the children are created `Creating` at
//! `g + 1`, and the atomic publication assigns `p = g + 2` to the children
//! (`Active`) and the source (`Retiring`) together. Requests holding `g`
//! against the splitting source classify [`RoutingError::TabletSplit`]; after
//! publication, requests holding less than `p` classify
//! [`RoutingError::TabletMoved`]; [`retry_guidance`] turns both into gateway
//! retry directions.
//!
//! # Seams
//!
//! The cluster crate deliberately does not depend on the storage engine, so
//! the applied keyspace and the child-state build are traits
//! ([`TabletKeyspace`], [`ChildStateSink`]) the runtime wave binds to the
//! engine's snapshot/stream mechanics (the `EngineSnapshot` staging idiom of
//! `mongreldb-consensus`: build beside live state, install atomically, then
//! catch up deltas). [`MapKeyspace`]/[`MapChildSink`] are the in-memory
//! reference implementations; [`InMemoryMetaPlane`] mirrors the meta group's
//! last-writer-wins descriptor semantics (its `publish_split` is the
//! reference for the single raft command the meta wave adopts).

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use mongreldb_types::errors::ErrorCategory;
use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::{MetadataVersion, RaftGroupId, TableId, TabletId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::meta::MetaRejectionReason;
use crate::node::ClusterError;
use crate::tablet::{
    Bound, Key, PartitionBounds, ReplicaDescriptor, ReplicaRole, RoutingError, TabletDescriptor,
    TabletError, TabletLayout, TabletState,
};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// The one error type of the split surface: planning, execution, and resume.
#[derive(Debug, thiserror::Error)]
pub enum SplitError {
    /// Descriptor, layout, or persisted-progress failure. Always fail closed.
    #[error(transparent)]
    Tablet(#[from] TabletError),
    /// An armed fault hook fired (crash-resume tests).
    #[error(transparent)]
    Fault(#[from] mongreldb_fault::Fault),
    /// The meta plane refused a descriptor write or the publication.
    #[error(transparent)]
    MetaPlane(#[from] MetaRejectionReason),
    /// A keyspace or child-sink seam operation failed.
    #[error(transparent)]
    TabletData(#[from] TabletDataError),
    /// The split was initiated against a source that is not serving.
    #[error("source tablet {tablet} is in state {state}, expected Active")]
    SourceNotActive {
        /// The source tablet.
        tablet: TabletId,
        /// Its current state.
        state: TabletState,
    },
    /// A midpoint cannot be derived when either endpoint is unbounded.
    #[error(
        "cannot derive a deterministic midpoint split key from unbounded bounds; \
         supply an explicit split key"
    )]
    UnboundedMidpoint,
    /// No key lies strictly between the two endpoints (immediate successors).
    #[error("no key lies strictly between the bounds' endpoints; the tablet cannot be split")]
    UnsplittableBounds,
    /// The explicit split key does not partition the source into two halves.
    #[error("split key {key} does not partition the source bounds into two non-empty halves")]
    InvalidSplitKey {
        /// The rejected key.
        key: Key,
    },
    /// The plan is structurally inconsistent.
    #[error("invalid split plan: {0}")]
    InvalidPlan(String),
    /// The applied keyspace holds a key outside the source partition (the
    /// children cannot own it; fail closed instead of dropping it).
    #[error("applied key {0} lies outside the source partition")]
    KeyOutsideSource(Key),
    /// Step 10's release condition is not met yet; retry when the pins drain.
    #[error("source tablet {tablet} is retained by {pins} old-generation pin(s)")]
    SourceRetained {
        /// The retained source.
        tablet: TabletId,
        /// Old-generation pins still outstanding.
        pins: usize,
    },
    /// The split already published its routing change (spec section 12.5
    /// step 8); rolling it back would double-serve the keyspace. Only a
    /// split that has not reached [`SplitPhase::Published`] can abort.
    #[error("cannot abort the split of tablet {tablet}: it already reached phase {phase}")]
    CannotAbort {
        /// The source tablet.
        tablet: TabletId,
        /// The phase the split had durably reached.
        phase: SplitPhase,
    },
}

/// The failure surface of the keyspace/sink seams the split and merge
/// executors drive (spec sections 12.5-12.6). The engine binding maps its
/// storage errors onto these; the in-memory reference implementations are
/// infallible except for staging-protocol misuse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TabletDataError {
    /// Reading the applied keyspace failed.
    #[error("keyspace operation failed: {0}")]
    Keyspace(String),
    /// `stage`/`install_staged` ran without a matching `begin_build`.
    #[error("no staged build in progress")]
    NoStagedBuild,
    /// Writing the child state failed.
    #[error("child state sink failed: {0}")]
    Sink(String),
}

// ---------------------------------------------------------------------------
// Split phases (persisted; declaration order frozen, spec section 4.10)
// ---------------------------------------------------------------------------

/// The durably persisted phases of one split (spec section 12.5). The
/// executor resumes from the last persisted phase after a crash.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SplitPhase {
    /// The plan is formed and recorded; no meta-plane change yet.
    Started,
    /// Step 1: the source is marked `Splitting` at `g + 1`.
    MarkedSplitting,
    /// Steps 2-3: split key chosen; child descriptors and layouts created as
    /// `Creating` learners.
    ChildrenCreated,
    /// Step 4: the source snapshot is pinned at `split_ts`.
    SnapshotPinned,
    /// Step 5: child state built from the pinned snapshot.
    ChildrenBuilt,
    /// Steps 6-7: source deltas streamed; the children are caught up, so the
    /// routing publication barrier is satisfied.
    CaughtUp,
    /// Steps 8-9: children `Active` + source `Retiring` published atomically.
    Published,
    /// Steps 10-11: the source is `Retired` and its replicas torn down.
    /// Terminal.
    SourceRetired,
}

impl SplitPhase {
    /// The fault hook fired after this phase's progress record is durable
    /// (`Started` has none: it is the pre-work record). Hook names are
    /// registered in the `mongreldb-fault` catalog.
    pub fn hook_name(self) -> Option<&'static str> {
        Some(match self {
            Self::Started => return None,
            Self::MarkedSplitting => "tablet.split.phase.1",
            Self::ChildrenCreated => "tablet.split.phase.2",
            Self::SnapshotPinned => "tablet.split.phase.3",
            Self::ChildrenBuilt => "tablet.split.phase.4",
            Self::CaughtUp => "tablet.split.phase.5",
            Self::Published => "tablet.split.phase.6",
            Self::SourceRetired => "tablet.split.phase.7",
        })
    }
}

impl fmt::Display for SplitPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Started => "Started",
            Self::MarkedSplitting => "MarkedSplitting",
            Self::ChildrenCreated => "ChildrenCreated",
            Self::SnapshotPinned => "SnapshotPinned",
            Self::ChildrenBuilt => "ChildrenBuilt",
            Self::CaughtUp => "CaughtUp",
            Self::Published => "Published",
            Self::SourceRetired => "SourceRetired",
        };
        f.write_str(name)
    }
}

// ---------------------------------------------------------------------------
// Split key selection (spec section 12.5 step 2)
// ---------------------------------------------------------------------------

/// How the planner chooses the split key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SplitKeySelection {
    /// The deterministic lexicographic midpoint of the source bounds (both
    /// endpoints must be bounded).
    Midpoint,
    /// An operator- or hotspot-chosen key; validated to partition the source
    /// into two non-empty halves.
    Explicit(Key),
}

/// The endpoint key of a bound, if bounded.
fn bound_key(bound: &Bound<Key>) -> Option<&Key> {
    match bound {
        Bound::Unbounded => None,
        Bound::Included(key) | Bound::Excluded(key) => Some(key),
    }
}

/// The deterministic lexicographic midpoint of two ordered keys (`low <
/// high`): a key strictly between them, always the same for the same inputs.
///
/// Returns `None` only when `high` is the immediate successor of `low`
/// (`high == low + [0x00]`), the single case where no key lies between.
///
/// The midpoint ranges over raw encoded bytes; it is a boundary in the
/// tablet [`Key`] space and need not itself decode into typed components —
/// order, not decodability, defines the partition.
pub fn midpoint_key(low: &Key, high: &Key) -> Option<Key> {
    let (low, high) = (low.as_bytes(), high.as_bytes());
    debug_assert!(low < high, "midpoint requires ordered endpoints");
    midpoint_bytes(low, high).map(Key::from_bytes)
}

fn midpoint_bytes(low: &[u8], high: &[u8]) -> Option<Vec<u8>> {
    let mut index = 0;
    while index < low.len() && index < high.len() && low[index] == high[index] {
        index += 1;
    }
    if index == low.len() {
        // `low` is a strict prefix of `high`. `low + [high[index] / 2]` is
        // strictly below `high` (halved byte below, or a prefix of it) and
        // strictly above `low` (an extension) — except when `high` is exactly
        // `low + [0x00]`, the immediate successor.
        if high.len() == index + 1 && high[index] == 0 {
            return None;
        }
        let mut mid = low.to_vec();
        mid.push(high[index] / 2);
        return Some(mid);
    }
    // Both sides differ at `index` (so `low[index] < high[index]`).
    let (low_byte, high_byte) = (low[index], high[index]);
    if high_byte >= low_byte + 2 {
        // Room between the differing bytes: halve it and truncate.
        let mut mid = low[..index].to_vec();
        mid.push(low_byte + (high_byte - low_byte) / 2);
        Some(mid)
    } else {
        // Adjacent byte values: extend below `low`; the differing byte keeps
        // the midpoint strictly below `high`.
        let mut mid = low.to_vec();
        mid.push(0x80);
        Some(mid)
    }
}

/// Resolves a [`SplitKeySelection`] to a concrete, validated split key.
fn choose_split_key(
    bounds: &PartitionBounds,
    selection: &SplitKeySelection,
) -> Result<Key, SplitError> {
    match selection {
        SplitKeySelection::Explicit(key) => {
            if bounds.split_at(key).is_none() {
                return Err(SplitError::InvalidSplitKey { key: key.clone() });
            }
            Ok(key.clone())
        }
        SplitKeySelection::Midpoint => {
            let (Some(low), Some(high)) = (bound_key(&bounds.low), bound_key(&bounds.high)) else {
                return Err(SplitError::UnboundedMidpoint);
            };
            midpoint_key(low, high).ok_or(SplitError::UnsplittableBounds)
        }
    }
}

// ---------------------------------------------------------------------------
// The split plan
// ---------------------------------------------------------------------------

/// Identity and replica allocation for one child tablet, supplied by the
/// caller (the meta wave allocates ids; placement chooses nodes). The
/// planner always creates the child replicas as learners (spec section 12.5
/// step 3); they are promoted to voters by the atomic publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildAllocation {
    /// The child tablet's id (fresh, never reused).
    pub tablet_id: TabletId,
    /// The child tablet's Raft group id (fresh, never reused).
    pub raft_group_id: RaftGroupId,
    /// The nodes the child's replicas start on.
    pub replicas: Vec<ReplicaDescriptor>,
}

/// One child of a split (or the replacement of a merge): bounds plus the
/// on-node [`TabletLayout`] plus the initial (learner) replica set.
#[derive(Clone, Debug)]
pub struct ChildPlan {
    /// The partition the child owns.
    pub bounds: PartitionBounds,
    /// The child's on-node directory layout.
    pub layout: TabletLayout,
    /// The initial replica set (created as learners).
    pub replicas: Vec<ReplicaDescriptor>,
}

impl ChildPlan {
    /// The descriptor of this child at `generation` in `state`, with every
    /// replica assigned `role`.
    pub fn descriptor(
        &self,
        table_id: TableId,
        generation: u64,
        state: TabletState,
        role: ReplicaRole,
    ) -> TabletDescriptor {
        TabletDescriptor {
            tablet_id: self.layout.tablet_id(),
            table_id,
            raft_group_id: self.layout.raft_group_id(),
            partition: self.bounds.clone(),
            replicas: self
                .replicas
                .iter()
                .map(|replica| ReplicaDescriptor { role, ..*replica })
                .collect(),
            leader_hint: None,
            generation,
            state,
        }
    }

    /// The persisted mirror of this plan (see [`ChildProgress`]).
    pub fn progress(&self) -> ChildProgress {
        ChildProgress {
            node_data: self.layout.node_data().to_path_buf(),
            tablet_id: self.layout.tablet_id(),
            raft_group_id: self.layout.raft_group_id(),
            bounds: self.bounds.clone(),
            replicas: self.replicas.clone(),
        }
    }
}

/// The persisted mirror of a [`ChildPlan`] (`TabletLayout` is a runtime
/// shape; the progress record carries the parts needed to rebuild it).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildProgress {
    /// Node data root the child layout lives under.
    pub node_data: PathBuf,
    /// The child tablet id.
    pub tablet_id: TabletId,
    /// The child Raft group id.
    pub raft_group_id: RaftGroupId,
    /// The child's partition bounds.
    pub bounds: PartitionBounds,
    /// The initial (learner) replica set.
    pub replicas: Vec<ReplicaDescriptor>,
}

impl ChildProgress {
    /// Rebuilds the runtime plan.
    pub fn plan(&self) -> ChildPlan {
        ChildPlan {
            bounds: self.bounds.clone(),
            layout: TabletLayout::new(self.node_data.clone(), self.tablet_id, self.raft_group_id),
            replicas: self.replicas.clone(),
        }
    }
}

/// One split: the source (pre-`Splitting`, at its initiation generation `g`),
/// the two children (lower half first), the split key, and the pinned split
/// timestamp.
#[derive(Clone, Debug)]
pub struct SplitPlan {
    /// The source tablet as it was at initiation (`Active`, generation `g`).
    pub source: TabletDescriptor,
    /// The children, lower half first; their bounds meet at `split_key`.
    pub children: [ChildPlan; 2],
    /// The chosen split key.
    pub split_key: Key,
    /// The timestamp the source snapshot is pinned at.
    pub split_ts: HlcTimestamp,
}

impl SplitPlan {
    /// Structural validation: the split key partitions the source exactly
    /// into the child bounds, ids are fresh and distinct, and the
    /// creation-time child descriptors are structurally valid.
    pub fn validate(&self) -> Result<(), SplitError> {
        self.source.validate()?;
        let (lower, upper) = self
            .source
            .partition
            .split_at(&self.split_key)
            .ok_or_else(|| SplitError::InvalidSplitKey {
                key: self.split_key.clone(),
            })?;
        if self.children[0].bounds != lower || self.children[1].bounds != upper {
            return Err(SplitError::InvalidPlan(
                "child bounds are not the source bounds split at the split key".to_owned(),
            ));
        }
        let child_ids = [
            (
                self.children[0].layout.tablet_id(),
                self.children[0].layout.raft_group_id(),
            ),
            (
                self.children[1].layout.tablet_id(),
                self.children[1].layout.raft_group_id(),
            ),
        ];
        if child_ids[0] == child_ids[1] || child_ids[0].0 == child_ids[1].0 {
            return Err(SplitError::InvalidPlan(
                "child tablet and raft group ids must be distinct".to_owned(),
            ));
        }
        if child_ids.iter().any(|ids| ids.0 == self.source.tablet_id) {
            return Err(SplitError::InvalidPlan(
                "child tablet ids must differ from the source's".to_owned(),
            ));
        }
        if child_ids
            .iter()
            .any(|ids| ids.1 == self.source.raft_group_id)
        {
            return Err(SplitError::InvalidPlan(
                "child raft group ids must differ from the source's".to_owned(),
            ));
        }
        for descriptor in self.child_descriptors() {
            descriptor.validate()?;
        }
        Ok(())
    }

    /// The child descriptors as created in step 3: `Creating` at the
    /// inception generation (`g + 1`), every replica a learner. `Creating`
    /// tablets are never routed to (spec section 12.5).
    pub fn child_descriptors(&self) -> [TabletDescriptor; 2] {
        let generation = self
            .source
            .generation
            .checked_add(1)
            .expect("descriptor generation overflows u64");
        self.children.clone().map(|child| {
            child.descriptor(
                self.source.table_id,
                generation,
                TabletState::Creating,
                ReplicaRole::Learner,
            )
        })
    }
}

/// Produces [`SplitPlan`]s (spec section 12.5 steps 2-3).
#[derive(Clone, Debug)]
pub struct TabletSplitPlanner {
    node_data: PathBuf,
}

impl TabletSplitPlanner {
    /// A planner rooted at the node's data directory.
    pub fn new(node_data: impl Into<PathBuf>) -> Self {
        Self {
            node_data: node_data.into(),
        }
    }

    /// Plans the split of `source` (which must be `Active`): chooses the
    /// split key, partitions the bounds, and lays out the two children. The
    /// source itself is untouched — the executor marks it `Splitting`.
    pub fn plan(
        &self,
        source: &TabletDescriptor,
        selection: SplitKeySelection,
        split_ts: HlcTimestamp,
        allocations: [ChildAllocation; 2],
    ) -> Result<SplitPlan, SplitError> {
        if source.state != TabletState::Active {
            return Err(SplitError::SourceNotActive {
                tablet: source.tablet_id,
                state: source.state,
            });
        }
        source.validate()?;
        let split_key = choose_split_key(&source.partition, &selection)?;
        let (lower, upper) =
            source
                .partition
                .split_at(&split_key)
                .ok_or_else(|| SplitError::InvalidSplitKey {
                    key: split_key.clone(),
                })?;
        let [lower_alloc, upper_alloc] = allocations;
        let child = |bounds: PartitionBounds, alloc: ChildAllocation| ChildPlan {
            bounds,
            layout: TabletLayout::new(self.node_data.clone(), alloc.tablet_id, alloc.raft_group_id),
            replicas: alloc.replicas,
        };
        let plan = SplitPlan {
            source: source.clone(),
            children: [child(lower, lower_alloc), child(upper, upper_alloc)],
            split_key,
            split_ts,
        };
        plan.validate()?;
        Ok(plan)
    }
}

// ---------------------------------------------------------------------------
// The atomic routing publication (spec section 12.5 steps 7-8)
// ---------------------------------------------------------------------------

/// The two-phase routing publication of one split, applied by the meta group
/// as ONE command (spec section 12.5 step 8): the children become `Active`
/// and the source becomes `Retiring` atomically at one new generation. Never
/// proposed before the children are caught up (the executor's phase machine
/// is the barrier of step 7).
///
/// This is the shape the meta wave adopts as a meta command; the reference
/// [`TabletMetaPlane::publish_split`] applies its semantics.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SplitPublishCommand {
    /// The source, republished `Retiring` at the publication generation.
    pub source: TabletDescriptor,
    /// The children, published `Active` at the publication generation, lower
    /// half first. Publication promotes the caught-up learners to voters
    /// (the Raft membership change rides the same meta command in the
    /// runtime wave).
    pub children: [TabletDescriptor; 2],
    /// The split key the child bounds meet at.
    pub split_key: Key,
    /// The pinned split timestamp the children were built at.
    pub split_ts: HlcTimestamp,
}

impl SplitPublishCommand {
    /// Builds the publication of `plan`: the source transitions
    /// `Splitting -> Retiring` and the children `Creating -> Active`, all at
    /// the publication generation (`g + 2`, see the generation rules on
    /// [`TabletDescriptor`]).
    pub fn from_plan(plan: &SplitPlan) -> Result<Self, SplitError> {
        let marked = plan.source.published_transition(TabletState::Splitting)?;
        let source = marked.published_transition(TabletState::Retiring)?;
        let mut children = plan.child_descriptors();
        for child in &mut children {
            *child = child.published_transition(TabletState::Active)?;
            for replica in &mut child.replicas {
                replica.role = ReplicaRole::Voter;
            }
        }
        let command = Self {
            source,
            children,
            split_key: plan.split_key.clone(),
            split_ts: plan.split_ts,
        };
        command.validate()?;
        Ok(command)
    }

    /// The generation the publication assigns to all three descriptors.
    pub fn publish_generation(&self) -> u64 {
        self.source.generation
    }

    /// Structural validation of the atomic publication: states and the shared
    /// generation, one table, and child bounds that partition the source
    /// exactly at `split_key`.
    pub fn validate(&self) -> Result<(), SplitError> {
        if self.source.state != TabletState::Retiring {
            return Err(SplitError::InvalidPlan(format!(
                "published source must be Retiring, is {}",
                self.source.state
            )));
        }
        let generation = self.source.generation;
        for child in &self.children {
            if child.state != TabletState::Active {
                return Err(SplitError::InvalidPlan(format!(
                    "published child {} must be Active, is {}",
                    child.tablet_id, child.state
                )));
            }
            if child.generation != generation {
                return Err(SplitError::InvalidPlan(
                    "publication assigns one generation to all descriptors".to_owned(),
                ));
            }
            if child.table_id != self.source.table_id {
                return Err(SplitError::InvalidPlan(
                    "children and source name different tables".to_owned(),
                ));
            }
            child.validate()?;
        }
        self.source.validate()?;
        let (lower, upper) = self
            .source
            .partition
            .split_at(&self.split_key)
            .ok_or_else(|| SplitError::InvalidSplitKey {
                key: self.split_key.clone(),
            })?;
        if self.children[0].partition != lower || self.children[1].partition != upper {
            return Err(SplitError::InvalidPlan(
                "published child bounds are not the source bounds split at the split key"
                    .to_owned(),
            ));
        }
        if self.children[0].tablet_id == self.children[1].tablet_id
            || self
                .children
                .iter()
                .any(|c| c.tablet_id == self.source.tablet_id)
        {
            return Err(SplitError::InvalidPlan(
                "publication tablet ids must be distinct".to_owned(),
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Persisted split progress (crash resume)
// ---------------------------------------------------------------------------

/// Name of the per-source progress record, a sibling of `tablet.json`.
pub const SPLIT_PROGRESS_FILENAME: &str = "split.json";
/// The progress-record format version this build writes.
pub const SPLIT_PROGRESS_FORMAT_VERSION: u32 = 1;
/// The oldest progress-record format version this build accepts.
pub const MIN_SUPPORTED_SPLIT_PROGRESS_FORMAT_VERSION: u32 = 1;

/// The durable progress of one split: everything [`SplitExecutor::resume`]
/// needs to continue idempotently after a crash — the plan (source, split
/// key, split timestamp, child ids and layouts) and the last completed
/// phase.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitProgress {
    /// The source descriptor as it was at initiation (`Active`, `g`).
    pub source: TabletDescriptor,
    /// The chosen split key.
    pub split_key: Key,
    /// The pinned split timestamp.
    pub split_ts: HlcTimestamp,
    /// The child plans, lower half first.
    pub children: [ChildProgress; 2],
    /// The last durably completed phase.
    pub phase: SplitPhase,
}

impl SplitProgress {
    /// Records `plan` at `phase`.
    pub fn from_plan(plan: &SplitPlan, phase: SplitPhase) -> Self {
        Self {
            source: plan.source.clone(),
            split_key: plan.split_key.clone(),
            split_ts: plan.split_ts,
            children: plan.children.clone().map(|child| child.progress()),
            phase,
        }
    }

    /// Rebuilds the runtime plan.
    pub fn plan(&self) -> SplitPlan {
        SplitPlan {
            source: self.source.clone(),
            children: self.children.clone().map(|child| child.plan()),
            split_key: self.split_key.clone(),
            split_ts: self.split_ts,
        }
    }
}

/// The durable progress-record envelope: versioned and checksummed with the
/// same idiom as `tablet.json` (fail closed on torn or foreign files).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SplitProgressFile {
    /// Durable format version; see [`SPLIT_PROGRESS_FORMAT_VERSION`].
    format_version: u32,
    /// Lowercase-hex SHA-256 of the canonical JSON encoding of `progress`.
    checksum: String,
    /// The persisted progress.
    progress: SplitProgress,
}

impl SplitProgressFile {
    fn envelope(progress: &SplitProgress) -> Result<Self, SplitError> {
        Ok(Self {
            format_version: SPLIT_PROGRESS_FORMAT_VERSION,
            checksum: progress_checksum(progress).map_err(meta_io)?,
            progress: progress.clone(),
        })
    }
}

/// SHA-256 of the canonical (compact JSON) encoding of the progress record.
fn progress_checksum(progress: &SplitProgress) -> Result<String, ClusterError> {
    let bytes = serde_json::to_vec(progress).map_err(|error| ClusterError::CorruptMetadata {
        file: SPLIT_PROGRESS_FILENAME,
        detail: format!("encode: {error}"),
    })?;
    Ok(hex_encode(&Sha256::digest(&bytes)))
}

/// Maps a node-layer metadata failure onto the split error surface.
fn meta_io(error: ClusterError) -> SplitError {
    SplitError::Tablet(TabletError::Metadata(error))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

// ---------------------------------------------------------------------------
// The meta-plane seam (steps 1, 3, 8, 11)
// ---------------------------------------------------------------------------

/// The meta control plane's tablet-descriptor surface, as the split and
/// merge executors drive it (spec sections 12.1, 12.5, 12.6). The runtime
/// wave binds this to `MetaGroup::propose`; every method is idempotent so a
/// retried or resumed executor reaches the same state.
pub trait TabletMetaPlane {
    /// Publishes one descriptor, last-writer-wins by `generation`: a higher
    /// generation replaces, an equal generation with identical content is a
    /// no-op, anything else is refused (mirroring
    /// `MetaCommand::SetTabletDescriptor`).
    fn set_tablet(&mut self, descriptor: &TabletDescriptor) -> Result<(), MetaRejectionReason>;

    /// The current descriptor of a tablet (the descriptor authority).
    fn tablet(&self, tablet_id: TabletId) -> Option<TabletDescriptor>;

    /// Removes a descriptor at or above its stored generation (mirroring
    /// `MetaCommand::RemoveTabletDescriptor`).
    fn remove_tablet(
        &mut self,
        tablet_id: TabletId,
        generation: u64,
    ) -> Result<(), MetaRejectionReason>;

    /// Steps 7-8 of spec section 12.5: the children become `Active` and the
    /// source `Retiring` atomically — ONE meta command, never exposed before
    /// catch-up. The default applies the command's descriptors within this
    /// one call; the meta-group binding overrides it with a single raft
    /// proposal carrying the command.
    fn publish_split(&mut self, command: &SplitPublishCommand) -> Result<(), MetaRejectionReason> {
        command
            .validate()
            .map_err(|error| MetaRejectionReason::Invalid {
                reason: error.to_string(),
            })?;
        for child in &command.children {
            self.set_tablet(child)?;
        }
        self.set_tablet(&command.source)
    }
}

/// The reference [`TabletMetaPlane`]: an in-memory descriptor map mirroring
/// the meta group's last-writer-wins apply semantics for
/// `SetTabletDescriptor`/`RemoveTabletDescriptor` (it models the tablet map
/// only — the meta group's table-existence check lives on `MetaState`).
/// Tests drive it directly; it also defines the behavior the meta wave's
/// raft binding must reproduce.
#[derive(Clone, Default)]
pub struct InMemoryMetaPlane {
    tablets: Arc<Mutex<BTreeMap<TabletId, TabletDescriptor>>>,
}

impl InMemoryMetaPlane {
    /// An empty plane.
    pub fn new() -> Self {
        Self::default()
    }

    /// Every descriptor, in tablet-id order (routing assertions).
    pub fn descriptors(&self) -> Vec<TabletDescriptor> {
        self.tablets
            .lock()
            .expect("meta plane lock poisoned")
            .values()
            .cloned()
            .collect()
    }
}

impl TabletMetaPlane for InMemoryMetaPlane {
    fn set_tablet(&mut self, descriptor: &TabletDescriptor) -> Result<(), MetaRejectionReason> {
        descriptor
            .validate()
            .map_err(|error| MetaRejectionReason::Invalid {
                reason: error.to_string(),
            })?;
        let mut tablets = self.tablets.lock().expect("meta plane lock poisoned");
        match tablets.get(&descriptor.tablet_id) {
            Some(existing) => {
                if descriptor.generation > existing.generation {
                    tablets.insert(descriptor.tablet_id, descriptor.clone());
                    Ok(())
                } else if descriptor.generation == existing.generation {
                    if existing == descriptor {
                        Ok(())
                    } else {
                        Err(MetaRejectionReason::Conflict {
                            resource: format!("tablet {}", descriptor.tablet_id),
                            reason: "generation already used for different content".to_owned(),
                        })
                    }
                } else {
                    Err(MetaRejectionReason::StaleWrite {
                        resource: format!("tablet {}", descriptor.tablet_id),
                        current: MetadataVersion(existing.generation),
                        attempted: MetadataVersion(descriptor.generation),
                    })
                }
            }
            None => {
                tablets.insert(descriptor.tablet_id, descriptor.clone());
                Ok(())
            }
        }
    }

    fn tablet(&self, tablet_id: TabletId) -> Option<TabletDescriptor> {
        self.tablets
            .lock()
            .expect("meta plane lock poisoned")
            .get(&tablet_id)
            .cloned()
    }

    fn remove_tablet(
        &mut self,
        tablet_id: TabletId,
        generation: u64,
    ) -> Result<(), MetaRejectionReason> {
        let mut tablets = self.tablets.lock().expect("meta plane lock poisoned");
        match tablets.get(&tablet_id) {
            None => Ok(()),
            Some(existing) => {
                if generation >= existing.generation {
                    tablets.remove(&tablet_id);
                    Ok(())
                } else {
                    Err(MetaRejectionReason::StaleWrite {
                        resource: format!("tablet {tablet_id}"),
                        current: MetadataVersion(existing.generation),
                        attempted: MetadataVersion(generation),
                    })
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The keyspace and child-state seams (steps 4-6)
// ---------------------------------------------------------------------------

/// A pinned source snapshot (spec section 12.5 step 4). The engine binding
/// pins an MVCC snapshot / read generation at `split_ts`; dropping releases
/// it. Re-pinning the same timestamp after a crash is a no-op for the
/// binding (the executor re-pins on resume).
pub trait SnapshotPin: Send {
    /// The timestamp the pin protects.
    fn pinned_at(&self) -> HlcTimestamp;
}

/// The boxed record stream the keyspace seams return (snapshot rows or
/// catch-up deltas, in the documented order).
pub type RecordStream<'a> = Box<dyn Iterator<Item = (Key, Vec<u8>)> + 'a>;

/// The applied keyspace of one tablet replica, as the split (and merge)
/// executors read it. The runtime wave binds this to the tablet's storage
/// core: the snapshot is the engine's snapshot/stream mechanics and the
/// deltas are the post-`split_ts` committed stream.
pub trait TabletKeyspace {
    /// Pins the snapshot at `ts` (step 4). Idempotent: re-pinning the same
    /// timestamp returns an equivalent pin.
    fn pin_snapshot(&mut self, ts: HlcTimestamp) -> Result<Box<dyn SnapshotPin>, TabletDataError>;

    /// The keyspace contents visible at the pinned snapshot `ts`, in key
    /// order.
    fn snapshot_at(&self, ts: HlcTimestamp) -> Result<RecordStream<'_>, TabletDataError>;

    /// Mutations committed after `ts`, oldest first (step 6's catch-up
    /// stream). Multiple versions of one key arrive in commit order.
    fn deltas_after(&self, ts: HlcTimestamp) -> Result<RecordStream<'_>, TabletDataError>;
}

/// The child-state sink (spec section 12.5 step 5), staged like the engine's
/// snapshot install: a build is staged beside any live state
/// (`begin_build`/`stage`) and installed atomically (`install_staged`) —
/// never over live state — after which catch-up deltas apply
/// (`apply_delta`). Restarting a build discards the staged content, so a
/// resumed step 5 is idempotent.
pub trait ChildStateSink {
    /// Starts (or restarts) a staged build, discarding prior staged content.
    fn begin_build(&mut self) -> Result<(), TabletDataError>;

    /// Adds one snapshot record to the staged build.
    fn stage(&mut self, key: &Key, value: &[u8]) -> Result<(), TabletDataError>;

    /// Atomically installs the staged build as the child's applied state.
    fn install_staged(&mut self) -> Result<(), TabletDataError>;

    /// Applies one post-snapshot delta to the installed state (step 6).
    fn apply_delta(&mut self, key: &Key, value: &[u8]) -> Result<(), TabletDataError>;
}

/// The reference [`TabletKeyspace`]: a shared in-memory multi-version map.
/// Versions are kept per key so `snapshot_at`/`deltas_after` split the
/// timeline exactly at the pinned timestamp; `insert` models both the seed
/// data and the writes that keep arriving while a split runs.
#[derive(Clone, Default)]
pub struct MapKeyspace {
    state: Arc<Mutex<MapKeyspaceState>>,
}

#[derive(Default)]
struct MapKeyspaceState {
    /// Version chains per key, ascending timestamps.
    rows: BTreeMap<Key, Vec<(HlcTimestamp, Vec<u8>)>>,
    /// Live snapshot pins.
    pins: usize,
}

impl MapKeyspace {
    /// An empty keyspace.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts one version of `key` at `ts`.
    pub fn insert(&self, key: Key, ts: HlcTimestamp, value: Vec<u8>) {
        let mut state = self.state.lock().expect("keyspace lock poisoned");
        let chain = state.rows.entry(key).or_default();
        chain.push((ts, value));
        chain.sort_by_key(|(version, _)| *version);
    }

    /// Number of live snapshot pins.
    pub fn pin_count(&self) -> usize {
        self.state.lock().expect("keyspace lock poisoned").pins
    }

    /// Every key's newest version at or below `ts` (assertion helper).
    pub fn rows_at(&self, ts: HlcTimestamp) -> BTreeMap<Key, Vec<u8>> {
        let state = self.state.lock().expect("keyspace lock poisoned");
        state
            .rows
            .iter()
            .filter_map(|(key, chain)| {
                let visible = chain.iter().rfind(|(version, _)| *version <= ts)?;
                Some((key.clone(), visible.1.clone()))
            })
            .collect()
    }
}

impl TabletKeyspace for MapKeyspace {
    fn pin_snapshot(&mut self, ts: HlcTimestamp) -> Result<Box<dyn SnapshotPin>, TabletDataError> {
        self.state.lock().expect("keyspace lock poisoned").pins += 1;
        Ok(Box::new(MapSnapshotPin {
            ts,
            state: self.state.clone(),
        }))
    }

    fn snapshot_at(&self, ts: HlcTimestamp) -> Result<RecordStream<'_>, TabletDataError> {
        Ok(Box::new(self.rows_at(ts).into_iter()))
    }

    fn deltas_after(&self, ts: HlcTimestamp) -> Result<RecordStream<'_>, TabletDataError> {
        let state = self.state.lock().expect("keyspace lock poisoned");
        let mut deltas: Vec<(HlcTimestamp, Key, Vec<u8>)> = Vec::new();
        for (key, chain) in &state.rows {
            for (version, value) in chain {
                if *version > ts {
                    deltas.push((*version, key.clone(), value.clone()));
                }
            }
        }
        deltas.sort();
        Ok(Box::new(
            deltas.into_iter().map(|(_, key, value)| (key, value)),
        ))
    }
}

/// A [`MapKeyspace`] snapshot pin; dropping releases it.
struct MapSnapshotPin {
    ts: HlcTimestamp,
    state: Arc<Mutex<MapKeyspaceState>>,
}

impl SnapshotPin for MapSnapshotPin {
    fn pinned_at(&self) -> HlcTimestamp {
        self.ts
    }
}

impl Drop for MapSnapshotPin {
    fn drop(&mut self) {
        self.state.lock().expect("keyspace lock poisoned").pins -= 1;
    }
}

/// The reference [`ChildStateSink`]: a shared in-memory map with staged-build
/// semantics. Clones share the same state, so a "crashed" executor's
/// installed data survives into the resumed one (as a real engine's would).
#[derive(Clone, Default)]
pub struct MapChildSink {
    state: Arc<Mutex<MapChildSinkState>>,
}

#[derive(Default)]
struct MapChildSinkState {
    staged: Option<BTreeMap<Key, Vec<u8>>>,
    installed: BTreeMap<Key, Vec<u8>>,
}

impl MapChildSink {
    /// An empty sink.
    pub fn new() -> Self {
        Self::default()
    }

    /// The installed rows (assertion helper).
    pub fn rows(&self) -> BTreeMap<Key, Vec<u8>> {
        self.state
            .lock()
            .expect("child sink lock poisoned")
            .installed
            .clone()
    }
}

impl ChildStateSink for MapChildSink {
    fn begin_build(&mut self) -> Result<(), TabletDataError> {
        self.state.lock().expect("child sink lock poisoned").staged = Some(BTreeMap::new());
        Ok(())
    }

    fn stage(&mut self, key: &Key, value: &[u8]) -> Result<(), TabletDataError> {
        let mut state = self.state.lock().expect("child sink lock poisoned");
        let staged = state
            .staged
            .as_mut()
            .ok_or(TabletDataError::NoStagedBuild)?;
        staged.insert(key.clone(), value.to_vec());
        Ok(())
    }

    fn install_staged(&mut self) -> Result<(), TabletDataError> {
        let mut state = self.state.lock().expect("child sink lock poisoned");
        let staged = state.staged.take().ok_or(TabletDataError::NoStagedBuild)?;
        state.installed = staged;
        Ok(())
    }

    fn apply_delta(&mut self, key: &Key, value: &[u8]) -> Result<(), TabletDataError> {
        let mut state = self.state.lock().expect("child sink lock poisoned");
        state.installed.insert(key.clone(), value.to_vec());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Source retention (spec section 12.5 step 10)
// ---------------------------------------------------------------------------

/// Tracks the in-flight requests still holding pre-publication generations
/// of a retired-from-routing source tablet (spec section 12.5 step 10). The
/// request path pins the generation it routed with and unpins when done;
/// step 11 proceeds only when no old-generation pins remain.
///
/// The guard is deliberately in-memory: a crash drops every in-flight
/// request with it, so a resumed executor starts from an empty guard. (The
/// snapshot pin of step 4 is separate — see [`TabletKeyspace::pin_snapshot`].)
#[derive(Debug)]
pub struct SourceRetentionGuard {
    source: TabletId,
    retired_generation: u64,
    inner: Mutex<RetentionInner>,
}

#[derive(Debug, Default)]
struct RetentionInner {
    next_pin: u64,
    /// Live pins: pin id -> generation the request routed with.
    pins: BTreeMap<u64, u64>,
}

impl SourceRetentionGuard {
    /// A guard for `source`, retired from routing at `retired_generation`.
    pub fn new(source: TabletId, retired_generation: u64) -> Self {
        Self {
            source,
            retired_generation,
            inner: Mutex::new(RetentionInner::default()),
        }
    }

    /// The guarded source tablet.
    pub fn source(&self) -> TabletId {
        self.source
    }

    /// The generation the source retired from routing at.
    pub fn retired_generation(&self) -> u64 {
        self.retired_generation
    }

    /// Registers one in-flight request routed at `used_generation`; the
    /// returned pin id releases it. Pin ids are never reused within a guard.
    pub fn pin(&self, used_generation: u64) -> u64 {
        let mut inner = self.inner.lock().expect("retention lock poisoned");
        let pin = inner.next_pin;
        inner.next_pin += 1;
        inner.pins.insert(pin, used_generation);
        pin
    }

    /// Releases a pin; `false` when the pin was already released.
    pub fn unpin(&self, pin: u64) -> bool {
        self.inner
            .lock()
            .expect("retention lock poisoned")
            .pins
            .remove(&pin)
            .is_some()
    }

    /// Live pins, any generation.
    pub fn pin_count(&self) -> usize {
        self.inner
            .lock()
            .expect("retention lock poisoned")
            .pins
            .len()
    }

    /// Live pins taken at generations the retirement made stale (below the
    /// retirement publication) — the ones step 10 waits for.
    pub fn old_generation_pins(&self) -> usize {
        self.inner
            .lock()
            .expect("retention lock poisoned")
            .pins
            .values()
            .filter(|generation| **generation < self.retired_generation)
            .count()
    }

    /// Step 10's release condition: no old-generation requests or pins
    /// remain.
    pub fn ready_for_removal(&self) -> bool {
        self.old_generation_pins() == 0
    }
}

// ---------------------------------------------------------------------------
// Stale-request redirection (spec section 12.5 step 9)
// ---------------------------------------------------------------------------

/// What the gateway should do with a request [`check_generation`] rejected
/// (spec sections 12.4, 12.5 step 9). Every direction starts with a routing
/// metadata refresh; the retry itself rides the policy engine of
/// [`crate::routing`] via [`RetryGuidance::failure`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryGuidance {
    /// The target is mid-split: refresh metadata and retry once the split
    /// publishes (the children become routable then).
    AwaitSplitPublish {
        /// The splitting tablet.
        tablet_id: TabletId,
    },
    /// The target retired from routing: refresh metadata and re-resolve —
    /// post-split the key lives on a child, post-merge on the replacement
    /// ([`crate::tablet::find_tablet_for_key`] against the refreshed set).
    RefreshAndReroute {
        /// The retired tablet.
        tablet_id: TabletId,
    },
    /// Other staleness (the replica is behind): refresh metadata and retry.
    RefreshAndRetry {
        /// The tablet the request targeted.
        tablet_id: TabletId,
    },
}

impl RetryGuidance {
    /// The stable error category of the rejection.
    pub fn category(&self) -> ErrorCategory {
        match self {
            Self::AwaitSplitPublish { .. } => ErrorCategory::TabletSplitting,
            Self::RefreshAndReroute { .. } => ErrorCategory::TabletMoved,
            Self::RefreshAndRetry { .. } => ErrorCategory::StaleMetadata,
        }
    }

    /// The failure shape the gateway's [`crate::routing::RetryPolicy`]
    /// decides over (all three categories refresh metadata, then retry safe
    /// operations — spec section 11.7).
    pub fn failure(&self) -> crate::routing::Failure {
        crate::routing::Failure::new(self.category())
    }
}

/// Maps a [`check_generation`] rejection onto gateway retry directions
/// (spec section 12.5 step 9).
///
/// [`check_generation`]: crate::tablet::check_generation
pub fn retry_guidance(error: &RoutingError) -> RetryGuidance {
    match *error {
        RoutingError::TabletSplit { tablet_id, .. } => {
            RetryGuidance::AwaitSplitPublish { tablet_id }
        }
        RoutingError::TabletMoved { tablet_id, .. } => {
            RetryGuidance::RefreshAndReroute { tablet_id }
        }
        RoutingError::StaleMetadata { tablet_id, .. } => {
            RetryGuidance::RefreshAndRetry { tablet_id }
        }
    }
}

// ---------------------------------------------------------------------------
// The split executor
// ---------------------------------------------------------------------------

/// Drives one split through the eleven steps of spec section 12.5,
/// persisting progress after every phase so a crash resumes where it
/// stopped. Construct with [`Self::begin`] (a fresh split) or
/// [`Self::resume`] (after a crash; `None` when no split is in progress),
/// then [`Self::run`] to completion or [`Self::step`] phase by phase.
///
/// `M` is the meta plane, `K` the source keyspace, `S` the child-state sink
/// (one per child, lower half first).
pub struct SplitExecutor<M, K, S> {
    progress: SplitProgress,
    source_layout: TabletLayout,
    meta: M,
    keyspace: K,
    sinks: [S; 2],
    snapshot_pin: Option<Box<dyn SnapshotPin>>,
    retention: Option<SourceRetentionGuard>,
}

impl<M: TabletMetaPlane, K: TabletKeyspace, S: ChildStateSink> SplitExecutor<M, K, S> {
    /// Begins a fresh split: validates the plan and records it durably
    /// (`Started`). The source layout must be this node's live replica of
    /// the source tablet.
    pub fn begin(
        plan: SplitPlan,
        source_layout: TabletLayout,
        meta: M,
        keyspace: K,
        sinks: [S; 2],
    ) -> Result<Self, SplitError> {
        plan.validate()?;
        if plan.source.state != TabletState::Active {
            return Err(SplitError::SourceNotActive {
                tablet: plan.source.tablet_id,
                state: plan.source.state,
            });
        }
        if source_layout.tablet_id() != plan.source.tablet_id
            || source_layout.raft_group_id() != plan.source.raft_group_id
        {
            return Err(TabletError::TabletMismatch {
                path: source_layout.tablet_dir(),
                expected: source_layout.tablet_id(),
                found: plan.source.tablet_id,
                expected_group: source_layout.raft_group_id(),
                found_group: plan.source.raft_group_id,
            }
            .into());
        }
        // The source replica must be a real, complete on-disk tablet.
        source_layout.validate()?;
        let executor = Self {
            progress: SplitProgress::from_plan(&plan, SplitPhase::Started),
            source_layout,
            meta,
            keyspace,
            sinks,
            snapshot_pin: None,
            retention: None,
        };
        executor.persist_progress()?;
        Ok(executor)
    }

    /// Resumes a split after a crash: reloads the persisted progress of the
    /// source tablet (`None` when no split is in progress — including a
    /// split whose final teardown already removed the record).
    pub fn resume(
        source_layout: TabletLayout,
        meta: M,
        keyspace: K,
        sinks: [S; 2],
    ) -> Result<Option<Self>, SplitError> {
        let Some(progress) = load_progress(&source_layout)? else {
            return Ok(None);
        };
        progress.plan().validate()?;
        Ok(Some(Self {
            progress,
            source_layout,
            meta,
            keyspace,
            sinks,
            snapshot_pin: None,
            retention: None,
        }))
    }

    /// The last durably completed phase.
    pub fn phase(&self) -> SplitPhase {
        self.progress.phase
    }

    /// The persisted progress record.
    pub fn progress(&self) -> &SplitProgress {
        &self.progress
    }

    /// The plan the split executes.
    pub fn plan(&self) -> SplitPlan {
        self.progress.plan()
    }

    /// The source retention guard (installed at publication, step 8).
    pub fn retention(&self) -> Option<&SourceRetentionGuard> {
        self.retention.as_ref()
    }

    /// The meta plane.
    pub fn meta(&self) -> &M {
        &self.meta
    }

    /// The source keyspace.
    pub fn keyspace(&self) -> &K {
        &self.keyspace
    }

    /// The child-state sinks, lower half first.
    pub fn sinks(&self) -> &[S; 2] {
        &self.sinks
    }

    /// Executes the next phase, persists it, and fires its fault hook.
    /// Returns the newly completed phase. Idempotent: re-entering after a
    /// failure redoes only the failed phase's work.
    pub fn step(&mut self) -> Result<SplitPhase, SplitError> {
        use SplitPhase::{
            CaughtUp, ChildrenBuilt, ChildrenCreated, MarkedSplitting, Published, SnapshotPinned,
            SourceRetired, Started,
        };
        let next = match self.progress.phase {
            Started => {
                self.mark_source_splitting()?;
                MarkedSplitting
            }
            MarkedSplitting => {
                self.create_children()?;
                ChildrenCreated
            }
            ChildrenCreated => {
                self.pin_source_snapshot()?;
                SnapshotPinned
            }
            SnapshotPinned => {
                self.build_children()?;
                ChildrenBuilt
            }
            ChildrenBuilt => {
                self.catch_up_children()?;
                CaughtUp
            }
            CaughtUp => {
                self.publish_children()?;
                Published
            }
            Published => {
                self.remove_source()?;
                SourceRetired
            }
            SourceRetired => SourceRetired,
        };
        // The terminal phase needs no progress record: the source teardown
        // already removed it with the tablet directory.
        self.progress.phase = next;
        if next != SourceRetired {
            self.persist_progress()?;
        }
        if let Some(hook) = next.hook_name() {
            mongreldb_fault::inject(hook)?;
        }
        Ok(next)
    }

    /// Runs every remaining phase to completion. A [`SplitError::SourceRetained`]
    /// surfaces with the split parked at [`SplitPhase::Published`] — drop the
    /// old-generation pins and call [`Self::run`] (or [`Self::resume`]) again.
    pub fn run(&mut self) -> Result<(), SplitError> {
        while self.progress.phase != SplitPhase::SourceRetired {
            self.step()?;
        }
        Ok(())
    }

    /// Runs until `phase` is complete (test/driver convenience).
    pub fn run_until(&mut self, phase: SplitPhase) -> Result<(), SplitError> {
        while self.progress.phase != phase {
            self.step()?;
        }
        Ok(())
    }

    /// Step 1: the meta group marks the source `Splitting` at `g + 1`; the
    /// local replica metadata follows.
    fn mark_source_splitting(&mut self) -> Result<(), SplitError> {
        let marked = self
            .progress
            .source
            .published_transition(TabletState::Splitting)?;
        self.meta.set_tablet(&marked)?;
        self.source_layout.store_metadata(&marked)?;
        Ok(())
    }

    /// Steps 2-3: the child descriptors are created as `Creating` learners —
    /// never routable — and their on-disk layouts are created.
    fn create_children(&mut self) -> Result<(), SplitError> {
        let plan = self.plan();
        for (descriptor, child) in plan.child_descriptors().iter().zip(plan.children.iter()) {
            child.layout.create(descriptor)?;
            self.meta.set_tablet(descriptor)?;
        }
        Ok(())
    }

    /// Step 4: the source snapshot is pinned at `split_ts`. Re-pinning after
    /// a resume is a no-op for the keyspace binding.
    fn pin_source_snapshot(&mut self) -> Result<(), SplitError> {
        self.ensure_snapshot_pin()
    }

    /// Re-acquires the snapshot pin when the executor does not hold one
    /// (fresh pin at step 4; re-pin after a crash resume).
    fn ensure_snapshot_pin(&mut self) -> Result<(), SplitError> {
        if self.snapshot_pin.is_none() {
            self.snapshot_pin = Some(self.keyspace.pin_snapshot(self.progress.split_ts)?);
        }
        Ok(())
    }

    /// Step 5: the pinned snapshot is partitioned at the split key into the
    /// child sinks (staged build, atomic install).
    fn build_children(&mut self) -> Result<(), SplitError> {
        self.ensure_snapshot_pin()?;
        for sink in &mut self.sinks {
            sink.begin_build()?;
        }
        let plan = self.plan();
        let snapshot = self.keyspace.snapshot_at(self.progress.split_ts)?;
        for (key, value) in snapshot {
            let index = route_child(&plan, &key)?;
            self.sinks[index].stage(&key, &value)?;
        }
        for sink in &mut self.sinks {
            sink.install_staged()?;
        }
        Ok(())
    }

    /// Step 6: the post-`split_ts` deltas are streamed into the caught-up
    /// children.
    fn catch_up_children(&mut self) -> Result<(), SplitError> {
        self.ensure_snapshot_pin()?;
        let plan = self.plan();
        let deltas = self.keyspace.deltas_after(self.progress.split_ts)?;
        for (key, value) in deltas {
            let index = route_child(&plan, &key)?;
            self.sinks[index].apply_delta(&key, &value)?;
        }
        Ok(())
    }

    /// Steps 7-9: the atomic routing publication — children `Active`, source
    /// `Retiring`, one generation — then the stale-request bookkeeping (the
    /// retention guard). The phase machine is the step-7 barrier: this only
    /// runs once the children are caught up.
    fn publish_children(&mut self) -> Result<(), SplitError> {
        let plan = self.plan();
        let command = SplitPublishCommand::from_plan(&plan)?;
        mongreldb_fault::inject("tablet.split.before")?;
        self.meta.publish_split(&command)?;
        mongreldb_fault::inject("tablet.split.after")?;
        // The local replica metadata follows the publication.
        for (descriptor, child) in command.children.iter().zip(plan.children.iter()) {
            child.layout.store_metadata(descriptor)?;
        }
        self.source_layout.store_metadata(&command.source)?;
        self.retention = Some(SourceRetentionGuard::new(
            plan.source.tablet_id,
            command.publish_generation(),
        ));
        // The children are published; the snapshot pin has done its work.
        self.snapshot_pin = None;
        Ok(())
    }

    /// Steps 10-11: once no old-generation pins remain, the source is
    /// published `Retired`, its descriptor removed, and its replicas torn
    /// down (which removes the progress record with the tablet directory).
    fn remove_source(&mut self) -> Result<(), SplitError> {
        let source_id = self.progress.source.tablet_id;
        if let Some(guard) = &self.retention {
            if !guard.ready_for_removal() {
                return Err(SplitError::SourceRetained {
                    tablet: source_id,
                    pins: guard.old_generation_pins(),
                });
            }
        }
        if let Some(current) = self.meta.tablet(source_id) {
            let retired = if current.state == TabletState::Retired {
                current
            } else {
                let retired = current.published_transition(TabletState::Retired)?;
                self.meta.set_tablet(&retired)?;
                retired
            };
            self.meta.remove_tablet(source_id, retired.generation)?;
        }
        // Teardown is last: it removes the progress record with the source
        // tablet directory, so a crash before it resumes cleanly into this
        // idempotent step.
        self.source_layout.teardown()?;
        Ok(())
    }

    /// Persists the progress record atomically into the source tablet
    /// directory.
    fn persist_progress(&self) -> Result<(), SplitError> {
        let file = SplitProgressFile::envelope(&self.progress)?;
        let bytes = crate::node::encode_json(SPLIT_PROGRESS_FILENAME, &file).map_err(meta_io)?;
        crate::node::write_meta_atomic(
            &self.source_layout.tablet_dir(),
            SPLIT_PROGRESS_FILENAME,
            &bytes,
        )
        .map_err(ClusterError::Io)
        .map_err(meta_io)?;
        Ok(())
    }
}

/// The child owning `key`: exactly one half contains every key of the
/// source's partition; anything else fails closed.
fn route_child(plan: &SplitPlan, key: &Key) -> Result<usize, SplitError> {
    if plan.children[0].bounds.contains(key) {
        return Ok(0);
    }
    if plan.children[1].bounds.contains(key) {
        return Ok(1);
    }
    Err(SplitError::KeyOutsideSource(key.clone()))
}

// ---------------------------------------------------------------------------
// Split abort (the Splitting -> Active rollback edge of the state graph)
// ---------------------------------------------------------------------------

/// The outcome of one [`abort_split`] drive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SplitAbortReport {
    /// The split's source tablet.
    pub source: TabletId,
    /// The phase the split had durably reached when the abort began (`None`
    /// when no split was in progress — the abort is then a no-op).
    pub phase: Option<SplitPhase>,
    /// The child tablets removed from the meta plane, lower half first.
    pub children_removed: Vec<TabletId>,
    /// The descriptor the source holds after the abort (`Active`; one
    /// generation above the `Splitting` mark when the abort itself
    /// republished it), `None` when no split was in progress.
    pub source_after: Option<TabletDescriptor>,
}

/// Aborts one in-progress split, unwinding it safely to the pre-split
/// routing: the never-routable children are removed from the meta plane and
/// their local layouts torn down, the source is published back to `Active`,
/// and the persisted progress record is removed.
///
/// Only a split that has not reached [`SplitPhase::Published`] can abort:
/// once the atomic routing publication landed, the children own their
/// halves and rolling back would double-serve the keyspace, so the driver
/// fails closed with [`SplitError::CannotAbort`].
///
/// Every step is idempotent and ordered meta-first, local-second, record
/// last, so a crash mid-abort simply re-enters: meta removals are no-ops
/// for absent descriptors, the source restore is a no-op once the source is
/// `Active` again, child layout teardown is idempotent, and the progress
/// record disappears only once the unwind is complete. The local replica
/// metadata (`tablet.json`) follows the restored descriptor.
pub fn abort_split<M: TabletMetaPlane>(
    source_layout: &TabletLayout,
    meta: &mut M,
) -> Result<SplitAbortReport, SplitError> {
    let Some(progress) = load_progress(source_layout)? else {
        return Ok(SplitAbortReport {
            source: source_layout.tablet_id(),
            phase: None,
            children_removed: Vec::new(),
            source_after: None,
        });
    };
    if progress.phase >= SplitPhase::Published {
        return Err(SplitError::CannotAbort {
            tablet: progress.source.tablet_id,
            phase: progress.phase,
        });
    }
    // 1. Remove the children from the meta plane. They were created
    //    `Creating` and never routed to, so removing them cannot strand a
    //    key range; absent children (a not-yet-run or already-aborted step
    //    2 of the executor) are no-ops.
    let mut children_removed = Vec::new();
    for child in &progress.children {
        if let Some(current) = meta.tablet(child.tablet_id) {
            meta.remove_tablet(child.tablet_id, current.generation)?;
            children_removed.push(child.tablet_id);
        }
    }
    // 2. Restore the source to `Active`. The `Splitting -> Active` edge is
    //    the state graph's documented abort rollback; an already-`Active`
    //    source (a crash after this step) is carried through unchanged, and
    //    any other state means the publication already landed and the abort
    //    raced it — `published_transition` fails closed on the illegal edge.
    let current = meta.tablet(progress.source.tablet_id).ok_or_else(|| {
        SplitError::InvalidPlan(format!(
            "source tablet {} is missing from the meta plane mid-abort",
            progress.source.tablet_id
        ))
    })?;
    let restored = if current.state == TabletState::Active {
        current
    } else {
        let restored = current.published_transition(TabletState::Active)?;
        meta.set_tablet(&restored)?;
        restored
    };
    // 3. The local replica metadata follows the restored descriptor.
    source_layout.store_metadata(&restored)?;
    // 4. Tear down the child layouts (idempotent; never destructive across
    //    identity).
    for child in &progress.children {
        child.plan().layout.teardown()?;
    }
    // 5. Last: drop the persisted progress record. A crash before this
    //    re-enters the abort; every step above is idempotent.
    let record = source_layout.tablet_dir().join(SPLIT_PROGRESS_FILENAME);
    match std::fs::remove_file(&record) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(meta_io(ClusterError::Io(error)));
        }
    }
    Ok(SplitAbortReport {
        source: progress.source.tablet_id,
        phase: Some(progress.phase),
        children_removed,
        source_after: Some(restored),
    })
}

/// Loads and verifies the persisted progress record (`None` when absent).
/// Corrupt, unknown-version, or foreign records fail closed.
fn load_progress(source_layout: &TabletLayout) -> Result<Option<SplitProgress>, SplitError> {
    let path = source_layout.tablet_dir().join(SPLIT_PROGRESS_FILENAME);
    let Some(bytes) = crate::node::read_meta_file(&path).map_err(meta_io)? else {
        return Ok(None);
    };
    let file: SplitProgressFile =
        crate::node::decode_json(SPLIT_PROGRESS_FILENAME, &bytes).map_err(meta_io)?;
    if file.format_version < MIN_SUPPORTED_SPLIT_PROGRESS_FORMAT_VERSION
        || file.format_version > SPLIT_PROGRESS_FORMAT_VERSION
    {
        return Err(meta_io(ClusterError::UnsupportedFormatVersion {
            file: SPLIT_PROGRESS_FILENAME,
            found: file.format_version,
            min: MIN_SUPPORTED_SPLIT_PROGRESS_FORMAT_VERSION,
            max: SPLIT_PROGRESS_FORMAT_VERSION,
        }));
    }
    if file.checksum != progress_checksum(&file.progress).map_err(meta_io)? {
        return Err(meta_io(ClusterError::CorruptMetadata {
            file: SPLIT_PROGRESS_FILENAME,
            detail: "checksum mismatch".to_owned(),
        }));
    }
    let progress = file.progress;
    if progress.source.tablet_id != source_layout.tablet_id()
        || progress.source.raft_group_id != source_layout.raft_group_id()
    {
        return Err(TabletError::TabletMismatch {
            path: source_layout.tablet_dir(),
            expected: source_layout.tablet_id(),
            found: progress.source.tablet_id,
            expected_group: source_layout.raft_group_id(),
            found_group: progress.source.raft_group_id,
        }
        .into());
    }
    Ok(Some(progress))
}

/// Reads the persisted split progress of a source tablet, if any (the node
/// runtime's resume probe). Same fail-closed verification as
/// [`SplitExecutor::resume`].
pub fn split_progress(source_layout: &TabletLayout) -> Result<Option<SplitProgress>, SplitError> {
    load_progress(source_layout)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) static EXECUTOR_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mongreldb_types::ids::NodeId;

    use super::*;
    use crate::routing::{
        GroupKey, OperationDescriptor, RetryAction, RetryPolicy, RetryState, RoutingCache,
    };
    use crate::tablet::{
        check_generation, find_tablet_for_key, tablets_overlapping, KeyValue, RowKeyEncoder,
    };

    fn node(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 16])
    }

    fn tablet_id(byte: u8) -> TabletId {
        TabletId::from_bytes([byte; 16])
    }

    fn group_id(byte: u8) -> RaftGroupId {
        RaftGroupId::from_bytes([byte; 16])
    }

    fn key(bytes: &[u8]) -> Key {
        Key::from_bytes(bytes.to_vec())
    }

    fn text_key(text: &str) -> Key {
        RowKeyEncoder::encode_key(&[KeyValue::Text(text.to_owned())])
    }

    fn ts(micros: u64) -> HlcTimestamp {
        HlcTimestamp {
            physical_micros: micros,
            logical: 0,
            node_tiebreaker: 0,
        }
    }

    fn source_descriptor() -> TabletDescriptor {
        TabletDescriptor {
            tablet_id: tablet_id(1),
            table_id: TableId::new(3),
            raft_group_id: group_id(1),
            partition: PartitionBounds::new(
                Bound::Included(text_key("a")),
                Bound::Excluded(text_key("z")),
            )
            .unwrap(),
            replicas: vec![
                ReplicaDescriptor {
                    node_id: node(1),
                    role: ReplicaRole::Voter,
                    raft_node_id: 11,
                },
                ReplicaDescriptor {
                    node_id: node(2),
                    role: ReplicaRole::Voter,
                    raft_node_id: 12,
                },
            ],
            leader_hint: Some(node(1)),
            generation: 5,
            state: TabletState::Active,
        }
    }

    fn allocation(tablet: u8, group: u8, raft_base: u64) -> ChildAllocation {
        ChildAllocation {
            tablet_id: tablet_id(tablet),
            raft_group_id: group_id(group),
            replicas: vec![
                ReplicaDescriptor {
                    node_id: node(3),
                    role: ReplicaRole::Voter,
                    raft_node_id: raft_base,
                },
                ReplicaDescriptor {
                    node_id: node(4),
                    role: ReplicaRole::Voter,
                    raft_node_id: raft_base + 1,
                },
            ],
        }
    }

    /// Keys below the "m" split point, then at/above it.
    const LOWER_KEYS: [&str; 6] = ["b", "d", "f", "h", "j", "l"];
    const UPPER_KEYS: [&str; 7] = ["m", "o", "q", "s", "u", "w", "y"];

    fn seed_keyspace(keyspace: &MapKeyspace) {
        for name in LOWER_KEYS.into_iter().chain(UPPER_KEYS) {
            keyspace.insert(
                text_key(name),
                ts(100),
                format!("v-{name}@100").into_bytes(),
            );
        }
        // In-flight writes after the split timestamp: two updates and an insert.
        keyspace.insert(text_key("b"), ts(200), b"v-b@200".to_vec());
        keyspace.insert(text_key("y"), ts(200), b"v-y@200".to_vec());
        keyspace.insert(text_key("n"), ts(200), b"v-n@200".to_vec());
    }

    struct SplitFixture {
        _dir: tempfile::TempDir,
        source: TabletDescriptor,
        source_layout: TabletLayout,
        meta: InMemoryMetaPlane,
        keyspace: MapKeyspace,
        sinks: [MapChildSink; 2],
        plan: SplitPlan,
    }

    fn split_fixture() -> SplitFixture {
        let dir = tempfile::tempdir().unwrap();
        let source = source_descriptor();
        let source_layout = TabletLayout::new(dir.path(), source.tablet_id, source.raft_group_id);
        source_layout.create(&source).unwrap();
        let mut meta = InMemoryMetaPlane::new();
        meta.set_tablet(&source).unwrap();
        let keyspace = MapKeyspace::new();
        seed_keyspace(&keyspace);
        let planner = TabletSplitPlanner::new(dir.path());
        let plan = planner
            .plan(
                &source,
                SplitKeySelection::Explicit(text_key("m")),
                ts(150),
                [allocation(2, 2, 21), allocation(3, 3, 31)],
            )
            .unwrap();
        SplitFixture {
            _dir: dir,
            source,
            source_layout,
            meta,
            keyspace,
            sinks: [MapChildSink::new(), MapChildSink::new()],
            plan,
        }
    }

    type TestExecutor = SplitExecutor<InMemoryMetaPlane, MapKeyspace, MapChildSink>;

    fn begin_executor(fixture: &SplitFixture) -> TestExecutor {
        SplitExecutor::begin(
            fixture.plan.clone(),
            fixture.source_layout.clone(),
            fixture.meta.clone(),
            fixture.keyspace.clone(),
            fixture.sinks.clone(),
        )
        .unwrap()
    }

    fn assert_split_completed(fixture: &SplitFixture) {
        // The source descriptor is removed; the children are Active at the
        // publication generation with promoted (voter) replicas.
        assert!(fixture.meta.tablet(fixture.source.tablet_id).is_none());
        for (index, id) in [tablet_id(2), tablet_id(3)].into_iter().enumerate() {
            let child = fixture.meta.tablet(id).unwrap();
            assert_eq!(child.state, TabletState::Active);
            assert_eq!(child.generation, 7);
            assert!(child
                .replicas
                .iter()
                .all(|replica| replica.role == ReplicaRole::Voter));
            assert_eq!(child.partition, fixture.plan.children[index].bounds);
            // The local replica metadata followed the publication.
            assert_eq!(
                fixture.plan.children[index].layout.load_metadata().unwrap(),
                child
            );
        }
        // The source replica is torn down and the progress record is gone.
        assert!(!fixture.source_layout.tablet_dir().exists());
        assert!(!fixture.source_layout.group_dir().exists());
        assert!(!fixture
            .source_layout
            .tablet_dir()
            .join(SPLIT_PROGRESS_FILENAME)
            .exists());
        // Zero loss, zero duplication: the children partition the keyspace.
        let lower_rows = fixture.sinks[0].rows();
        let upper_rows = fixture.sinks[1].rows();
        assert!(lower_rows.keys().all(|key| *key < text_key("m")));
        assert!(upper_rows.keys().all(|key| *key >= text_key("m")));
        let mut union = lower_rows.clone();
        for (key, value) in &upper_rows {
            assert!(
                union.insert(key.clone(), value.clone()).is_none(),
                "duplicate key {key} across the split boundary"
            );
        }
        assert_eq!(union, fixture.keyspace.rows_at(ts(u64::MAX)));
    }

    // -- split key selection -------------------------------------------------

    #[test]
    fn midpoint_key_is_deterministic_and_strictly_between() {
        let cases: &[(&[u8], &[u8])] = &[
            (b"a", b"z"),
            (b"aa", b"ab"),
            (b"a", b"a\x01"),
            (b"m", b"n"),
            (b"\x00", b"\xff"),
            (b"abc", b"abd"),
            (b"a", b"aa"),
            (b"", b"\x01"),
        ];
        for (low, high) in cases {
            let mid = midpoint_key(&key(low), &key(high)).unwrap();
            assert!(key(low) < mid, "midpoint {mid} not above {low:?}");
            assert!(mid < key(high), "midpoint {mid} not below {high:?}");
            // Deterministic: same inputs, same midpoint.
            assert_eq!(mid, midpoint_key(&key(low), &key(high)).unwrap());
        }
        // The immediate successor has no midpoint.
        assert_eq!(midpoint_key(&key(b"a"), &key(b"a\x00")), None);
        // Fair halving where byte room allows.
        assert_eq!(midpoint_key(&key(b"a"), &key(b"z")).unwrap(), key(b"m"));
        assert_eq!(
            midpoint_key(&key(b"\x10"), &key(b"\x20")).unwrap(),
            key(b"\x18")
        );
    }

    #[test]
    fn planner_chooses_and_validates_split_keys() {
        let dir = tempfile::tempdir().unwrap();
        let source = source_descriptor();
        let planner = TabletSplitPlanner::new(dir.path());

        // Midpoint over bounded endpoints is deterministic.
        let plan = planner
            .plan(
                &source,
                SplitKeySelection::Midpoint,
                ts(150),
                [allocation(2, 2, 21), allocation(3, 3, 31)],
            )
            .unwrap();
        let (expected_lower, expected_upper) = source.partition.split_at(&plan.split_key).unwrap();
        assert_eq!(plan.children[0].bounds, expected_lower);
        assert_eq!(plan.children[1].bounds, expected_upper);
        assert!(plan.children[0]
            .bounds
            .meets_start_of(&plan.children[1].bounds));

        // Midpoint is unavailable over unbounded endpoints.
        let mut unbounded = source.clone();
        unbounded.partition = PartitionBounds::unbounded();
        assert!(matches!(
            planner.plan(
                &unbounded,
                SplitKeySelection::Midpoint,
                ts(150),
                [allocation(2, 2, 21), allocation(3, 3, 31)],
            ),
            Err(SplitError::UnboundedMidpoint)
        ));

        // Explicit keys outside or at the edge of the bounds are rejected.
        for bad in ["a", "z", "0"] {
            assert!(matches!(
                planner.plan(
                    &source,
                    SplitKeySelection::Explicit(text_key(bad)),
                    ts(150),
                    [allocation(2, 2, 21), allocation(3, 3, 31)],
                ),
                Err(SplitError::InvalidSplitKey { .. })
            ));
        }

        // A non-Active source cannot be planned around.
        let mut splitting = source.clone();
        splitting.state = TabletState::Splitting;
        assert!(matches!(
            planner.plan(
                &splitting,
                SplitKeySelection::Explicit(text_key("m")),
                ts(150),
                [allocation(2, 2, 21), allocation(3, 3, 31)],
            ),
            Err(SplitError::SourceNotActive {
                state: TabletState::Splitting,
                ..
            })
        ));

        // Colliding child ids fail closed.
        let mut colliding = source.clone();
        let error = planner
            .plan(
                &colliding,
                SplitKeySelection::Explicit(text_key("m")),
                ts(150),
                [allocation(2, 2, 21), allocation(2, 3, 31)],
            )
            .unwrap_err();
        assert!(matches!(error, SplitError::InvalidPlan(_)));
        colliding = source.clone();
        let error = planner
            .plan(
                &colliding,
                SplitKeySelection::Explicit(text_key("m")),
                ts(150),
                [allocation(1, 2, 21), allocation(3, 3, 31)],
            )
            .unwrap_err();
        assert!(matches!(error, SplitError::InvalidPlan(_)));
    }

    #[test]
    fn child_descriptors_are_creating_learners_at_the_inception_generation() {
        let dir = tempfile::tempdir().unwrap();
        let planner = TabletSplitPlanner::new(dir.path());
        let plan = planner
            .plan(
                &source_descriptor(),
                SplitKeySelection::Explicit(text_key("m")),
                ts(150),
                [allocation(2, 2, 21), allocation(3, 3, 31)],
            )
            .unwrap();
        let children = plan.child_descriptors();
        assert_eq!(children[0].state, TabletState::Creating);
        assert_eq!(children[0].generation, 6); // source g=5, inception g+1
        assert!(children
            .iter()
            .flat_map(|child| child.replicas.iter())
            .all(|replica| replica.role == ReplicaRole::Learner));
        for child in &children {
            child.validate().unwrap();
        }
        // Creating children are never routable, even though they overlap.
        assert!(!children.iter().any(|child| child.state.is_routable()));
    }

    // -- the atomic publication ----------------------------------------------

    #[test]
    fn publish_command_flips_three_descriptors_at_one_generation() {
        let dir = tempfile::tempdir().unwrap();
        let planner = TabletSplitPlanner::new(dir.path());
        let plan = planner
            .plan(
                &source_descriptor(),
                SplitKeySelection::Explicit(text_key("m")),
                ts(150),
                [allocation(2, 2, 21), allocation(3, 3, 31)],
            )
            .unwrap();
        let command = SplitPublishCommand::from_plan(&plan).unwrap();
        assert_eq!(command.publish_generation(), 7); // g=5 -> marked 6 -> publish 7
        assert_eq!(command.source.state, TabletState::Retiring);
        assert_eq!(command.source.generation, 7);
        for child in &command.children {
            assert_eq!(child.state, TabletState::Active);
            assert_eq!(child.generation, 7);
            assert!(child
                .replicas
                .iter()
                .all(|replica| replica.role == ReplicaRole::Voter));
        }
        // The shape survives serde (the meta wave journals it as one command).
        let bytes = serde_json::to_vec(&command).unwrap();
        let back: SplitPublishCommand = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, command);

        // A tampered shape fails validation: wrong state.
        let mut wrong_state = command.clone();
        wrong_state.children[0].state = TabletState::Creating;
        assert!(wrong_state.validate().is_err());
        // Skewed generations.
        let mut skewed = command.clone();
        skewed.children[0].generation = 8;
        assert!(skewed.validate().is_err());
        // Bounds that do not partition the source.
        let mut wrong_bounds = command.clone();
        wrong_bounds.children[0].partition = PartitionBounds::unbounded();
        assert!(wrong_bounds.validate().is_err());
        // A duplicate tablet id.
        let mut duplicate = command.clone();
        duplicate.children[1].tablet_id = duplicate.children[0].tablet_id;
        assert!(duplicate.validate().is_err());
    }

    // -- the full split -------------------------------------------------------

    #[test]
    fn full_split_partitions_the_keyspace_and_flips_routing_atomically() {
        let _lock = EXECUTOR_TEST_LOCK.lock().unwrap();
        let fixture = split_fixture();
        let table = fixture.source.table_id;
        let mut executor = begin_executor(&fixture);
        assert_eq!(executor.phase(), SplitPhase::Started);

        // Step 1: the source is marked Splitting at g+1 and stays routable.
        assert_eq!(executor.step().unwrap(), SplitPhase::MarkedSplitting);
        let marked = fixture.meta.tablet(fixture.source.tablet_id).unwrap();
        assert_eq!(marked.state, TabletState::Splitting);
        assert_eq!(marked.generation, 6);
        let error = check_generation(&marked, 5).unwrap_err();
        assert!(matches!(error, RoutingError::TabletSplit { .. }));
        assert!(matches!(
            retry_guidance(&error),
            RetryGuidance::AwaitSplitPublish { tablet_id } if tablet_id == fixture.source.tablet_id
        ));
        let tablets = fixture.meta.descriptors();
        for name in ["b", "l", "m", "y"] {
            assert_eq!(
                find_tablet_for_key(&tablets, table, &text_key(name))
                    .unwrap()
                    .tablet_id,
                fixture.source.tablet_id,
                "key {name} left the source during the split"
            );
        }

        // Steps 2-3: the children exist as Creating learners — never routable.
        assert_eq!(executor.step().unwrap(), SplitPhase::ChildrenCreated);
        for id in [tablet_id(2), tablet_id(3)] {
            let child = fixture.meta.tablet(id).unwrap();
            assert_eq!(child.state, TabletState::Creating);
            assert_eq!(child.generation, 6);
            assert!(child
                .replicas
                .iter()
                .all(|replica| replica.role == ReplicaRole::Learner));
        }
        let tablets = fixture.meta.descriptors();
        for name in ["b", "y"] {
            assert_eq!(
                find_tablet_for_key(&tablets, table, &text_key(name))
                    .unwrap()
                    .tablet_id,
                fixture.source.tablet_id,
                "Creating child exposed key {name} before catch-up"
            );
        }
        for child in &fixture.plan.children {
            assert_eq!(
                child.layout.load_metadata().unwrap().state,
                TabletState::Creating
            );
        }

        // Step 4: the snapshot pin is held.
        assert_eq!(executor.step().unwrap(), SplitPhase::SnapshotPinned);
        assert_eq!(fixture.keyspace.pin_count(), 1);
        assert_eq!(executor.phase(), SplitPhase::SnapshotPinned);

        // Step 5: the snapshot at ts 150 is partitioned; post-ts writes are
        // not yet visible in the children.
        assert_eq!(executor.step().unwrap(), SplitPhase::ChildrenBuilt);
        assert_eq!(fixture.sinks[0].rows().len(), LOWER_KEYS.len());
        assert_eq!(fixture.sinks[1].rows().len(), UPPER_KEYS.len());
        assert_eq!(
            fixture.sinks[1].rows().get(&text_key("y")),
            Some(&b"v-y@100".to_vec()),
            "post-split write leaked into the pinned snapshot"
        );

        // Step 6: catch-up applies the post-ts deltas to the right halves.
        assert_eq!(executor.step().unwrap(), SplitPhase::CaughtUp);
        assert_eq!(
            fixture.sinks[0].rows().get(&text_key("b")),
            Some(&b"v-b@200".to_vec())
        );
        assert_eq!(
            fixture.sinks[1].rows().get(&text_key("y")),
            Some(&b"v-y@200".to_vec())
        );
        assert_eq!(
            fixture.sinks[1].rows().get(&text_key("n")),
            Some(&b"v-n@200".to_vec())
        );

        // Steps 7-8: the atomic publication flips routing; the pin releases.
        assert_eq!(executor.step().unwrap(), SplitPhase::Published);
        assert_eq!(fixture.keyspace.pin_count(), 0);
        let retiring = fixture.meta.tablet(fixture.source.tablet_id).unwrap();
        assert_eq!(retiring.state, TabletState::Retiring);
        assert_eq!(retiring.generation, 7);
        let tablets = fixture.meta.descriptors();
        assert_eq!(
            find_tablet_for_key(&tablets, table, &text_key("b"))
                .unwrap()
                .tablet_id,
            tablet_id(2)
        );
        assert_eq!(
            find_tablet_for_key(&tablets, table, &text_key("y"))
                .unwrap()
                .tablet_id,
            tablet_id(3)
        );
        // The split key itself belongs to the upper half.
        assert_eq!(
            find_tablet_for_key(&tablets, table, &text_key("m"))
                .unwrap()
                .tablet_id,
            tablet_id(3)
        );
        // Range queries fan out over exactly the two children, in order.
        let overlapping = tablets_overlapping(&tablets, table, &PartitionBounds::unbounded());
        assert_eq!(
            overlapping
                .iter()
                .map(|tablet| tablet.tablet_id)
                .collect::<Vec<_>>(),
            vec![tablet_id(2), tablet_id(3)]
        );
        // Stale requests against the retired source reroute to the children.
        let error = check_generation(&retiring, 5).unwrap_err();
        assert!(matches!(error, RoutingError::TabletMoved { .. }));
        assert!(matches!(
            retry_guidance(&error),
            RetryGuidance::RefreshAndReroute { .. }
        ));
        // A request at the publication generation passes the children.
        for id in [tablet_id(2), tablet_id(3)] {
            assert!(check_generation(&fixture.meta.tablet(id).unwrap(), 7).is_ok());
        }

        // Steps 10-11: retention gates removal until old pins drain.
        let pin = executor.retention().unwrap().pin(5);
        assert!(matches!(
            executor.step(),
            Err(SplitError::SourceRetained { pins: 1, .. })
        ));
        assert_eq!(executor.phase(), SplitPhase::Published);
        // A new-generation pin does not block removal.
        let fresh = executor.retention().unwrap().pin(7);
        assert!(executor.retention().unwrap().unpin(pin));
        assert_eq!(executor.step().unwrap(), SplitPhase::SourceRetired);
        assert!(executor.retention().unwrap().unpin(fresh));
        assert_split_completed(&fixture);
        // Terminal: further runs are no-ops; nothing is left to resume.
        executor.run().unwrap();
        assert!(SplitExecutor::resume(
            fixture.source_layout.clone(),
            fixture.meta.clone(),
            fixture.keyspace.clone(),
            fixture.sinks.clone(),
        )
        .unwrap()
        .is_none());
    }

    // -- crash-resume at every durable boundary --------------------------------

    #[test]
    fn split_resumes_after_a_crash_at_every_durable_boundary() {
        let _lock = EXECUTOR_TEST_LOCK.lock().unwrap();
        let hooks = [
            "tablet.split.phase.1",
            "tablet.split.phase.2",
            "tablet.split.phase.3",
            "tablet.split.phase.4",
            "tablet.split.phase.5",
            "tablet.split.phase.6",
            "tablet.split.phase.7",
            "tablet.split.before",
            "tablet.split.after",
        ];
        for hook in hooks {
            let fixture = split_fixture();
            let mut executor = begin_executor(&fixture);
            {
                let _guard =
                    mongreldb_fault::ScopedGuard::limited(hook, mongreldb_fault::Action::Fail, 1);
                assert!(
                    matches!(executor.run(), Err(SplitError::Fault(_))),
                    "hook {hook} did not fire"
                );
            }
            // The "crash": the executor is dropped mid-flight.
            drop(executor);
            let resumed = SplitExecutor::resume(
                fixture.source_layout.clone(),
                fixture.meta.clone(),
                fixture.keyspace.clone(),
                fixture.sinks.clone(),
            )
            .unwrap();
            if hook == "tablet.split.phase.7" {
                // The final phase completes the split and removes the progress
                // record with the source directory: nothing to resume.
                assert!(resumed.is_none(), "hook {hook}");
            } else {
                resumed
                    .unwrap()
                    .run()
                    .unwrap_or_else(|error| panic!("resume after {hook} failed: {error}"));
            }
            assert_split_completed(&fixture);
        }
    }

    #[test]
    fn resume_replays_an_interrupted_step_idempotently() {
        let _lock = EXECUTOR_TEST_LOCK.lock().unwrap();
        let fixture = split_fixture();
        let mut executor = begin_executor(&fixture);
        executor.run_until(SplitPhase::ChildrenCreated).unwrap();
        // Crash before the pin phase; the snapshot pin is re-acquired on resume.
        drop(executor);
        let mut resumed = SplitExecutor::resume(
            fixture.source_layout.clone(),
            fixture.meta.clone(),
            fixture.keyspace.clone(),
            fixture.sinks.clone(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(resumed.phase(), SplitPhase::ChildrenCreated);
        resumed.step().unwrap();
        assert_eq!(resumed.phase(), SplitPhase::SnapshotPinned);
        assert_eq!(fixture.keyspace.pin_count(), 1);
        resumed.run().unwrap();
        assert_split_completed(&fixture);
    }

    // -- retention guard -------------------------------------------------------

    #[test]
    fn retention_guard_tracks_old_generation_pins() {
        let guard = SourceRetentionGuard::new(tablet_id(1), 7);
        assert!(guard.ready_for_removal());
        assert_eq!(guard.source(), tablet_id(1));
        assert_eq!(guard.retired_generation(), 7);

        let stale = guard.pin(5);
        let boundary = guard.pin(7); // the retirement generation is not "old"
        assert_eq!(guard.pin_count(), 2);
        assert_eq!(guard.old_generation_pins(), 1);
        assert!(!guard.ready_for_removal());

        assert!(guard.unpin(stale));
        assert!(guard.ready_for_removal());
        assert!(guard.unpin(boundary));
        assert!(!guard.unpin(boundary), "double release must not succeed");
        assert_eq!(guard.pin_count(), 0);
    }

    // -- stale-request classification and retry guidance ------------------------

    #[test]
    fn stale_requests_classify_and_guidance_feeds_the_retry_policy() {
        let mut descriptor = source_descriptor();
        // A match passes.
        assert!(check_generation(&descriptor, 5).is_ok());

        // Splitting source: TabletSplit -> AwaitSplitPublish.
        descriptor.state = TabletState::Splitting;
        descriptor.generation = 6;
        let error = check_generation(&descriptor, 5).unwrap_err();
        assert_eq!(
            error,
            RoutingError::TabletSplit {
                tablet_id: tablet_id(1),
                used_generation: 5,
                current_generation: 6,
            }
        );
        let guidance = retry_guidance(&error);
        assert_eq!(guidance.category(), ErrorCategory::TabletSplitting);

        // Retiring source: TabletMoved -> RefreshAndReroute.
        descriptor.state = TabletState::Retiring;
        descriptor.generation = 7;
        let error = check_generation(&descriptor, 5).unwrap_err();
        assert!(matches!(error, RoutingError::TabletMoved { .. }));
        let guidance = retry_guidance(&error);
        assert_eq!(guidance.category(), ErrorCategory::TabletMoved);

        // A child at the publication generation serving an older request:
        // plain stale metadata.
        descriptor.state = TabletState::Active;
        let error = check_generation(&descriptor, 5).unwrap_err();
        assert!(matches!(error, RoutingError::StaleMetadata { .. }));
        let guidance = retry_guidance(&error);
        assert_eq!(guidance.category(), ErrorCategory::StaleMetadata);

        // The guidance drives the gateway retry policy end to end: every
        // split-related category refreshes metadata, then retries a safe op.
        let policy = RetryPolicy::default();
        let cache = RoutingCache::new();
        let operation = OperationDescriptor {
            idempotent: true,
            idempotency_key: None,
            read_only: true,
            deadline: Duration::from_secs(30),
            max_attempts: 3,
        };
        for error in [
            RoutingError::TabletSplit {
                tablet_id: tablet_id(1),
                used_generation: 5,
                current_generation: 6,
            },
            RoutingError::TabletMoved {
                tablet_id: tablet_id(1),
                used_generation: 5,
                current_generation: 7,
            },
            RoutingError::StaleMetadata {
                tablet_id: tablet_id(1),
                used_generation: 5,
                current_generation: 7,
            },
        ] {
            let mut state = RetryState::default();
            let action = policy.decide(
                GroupKey::Tablet(tablet_id(1)),
                &operation,
                &mut state,
                &retry_guidance(&error).failure(),
                &cache,
                Duration::ZERO,
            );
            assert!(
                matches!(action, RetryAction::RefreshMetadata { .. }),
                "{error} did not map onto a metadata refresh"
            );
        }
    }

    // -- progress record durability ----------------------------------------------

    #[test]
    fn progress_record_round_trips_and_fails_closed() {
        let _lock = EXECUTOR_TEST_LOCK.lock().unwrap();
        let fixture = split_fixture();
        let executor = begin_executor(&fixture);
        let path = fixture
            .source_layout
            .tablet_dir()
            .join(SPLIT_PROGRESS_FILENAME);
        assert!(path.is_file());
        drop(executor);

        // A corrupt payload fails closed.
        std::fs::write(&path, b"{ not json").unwrap();
        assert!(matches!(
            SplitExecutor::resume(
                fixture.source_layout.clone(),
                fixture.meta.clone(),
                fixture.keyspace.clone(),
                fixture.sinks.clone(),
            ),
            Err(SplitError::Tablet(TabletError::Metadata(
                ClusterError::CorruptMetadata { .. }
            )))
        ));

        // An unknown format version fails closed.
        let executor = begin_executor(&fixture);
        drop(executor);
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        value["format_version"] = serde_json::json!(99);
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(matches!(
            SplitExecutor::resume(
                fixture.source_layout.clone(),
                fixture.meta.clone(),
                fixture.keyspace.clone(),
                fixture.sinks.clone(),
            ),
            Err(SplitError::Tablet(TabletError::Metadata(
                ClusterError::UnsupportedFormatVersion { found: 99, .. }
            )))
        ));

        // A tampered payload breaks the checksum.
        let executor = begin_executor(&fixture);
        drop(executor);
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        value["progress"]["split_ts"]["physical_micros"] = serde_json::json!(999);
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(matches!(
            SplitExecutor::resume(
                fixture.source_layout.clone(),
                fixture.meta.clone(),
                fixture.keyspace.clone(),
                fixture.sinks.clone(),
            ),
            Err(SplitError::Tablet(TabletError::Metadata(
                ClusterError::CorruptMetadata { .. }
            )))
        ));
    }

    #[test]
    fn begin_rejects_a_mismatched_source_layout_and_missing_replica() {
        let _lock = EXECUTOR_TEST_LOCK.lock().unwrap();
        let fixture = split_fixture();
        // A layout for a different tablet fails closed.
        let foreign = TabletLayout::new(
            fixture._dir.path(),
            tablet_id(9),
            fixture.source.raft_group_id,
        );
        assert!(matches!(
            SplitExecutor::begin(
                fixture.plan.clone(),
                foreign,
                fixture.meta.clone(),
                fixture.keyspace.clone(),
                fixture.sinks.clone(),
            ),
            Err(SplitError::Tablet(TabletError::TabletMismatch { .. }))
        ));
        // A layout with no on-disk replica fails closed.
        let other_dir = tempfile::tempdir().unwrap();
        let missing = TabletLayout::new(
            other_dir.path(),
            fixture.source.tablet_id,
            fixture.source.raft_group_id,
        );
        assert!(matches!(
            SplitExecutor::begin(
                fixture.plan.clone(),
                missing,
                fixture.meta.clone(),
                fixture.keyspace.clone(),
                fixture.sinks.clone(),
            ),
            Err(SplitError::Tablet(TabletError::MissingMetadata(_)))
        ));
    }

    // -- abort driver -----------------------------------------------------------

    #[test]
    fn abort_before_publish_restores_the_source_and_removes_the_children() {
        let _lock = EXECUTOR_TEST_LOCK.lock().unwrap();
        let fixture = split_fixture();
        let mut executor = begin_executor(&fixture);
        executor.run_until(SplitPhase::SnapshotPinned).unwrap();
        drop(executor);
        // Pre-abort state: the source is Splitting at g + 1, the children
        // exist as Creating learners, and the progress record is durable.
        let marked = fixture.meta.tablet(fixture.source.tablet_id).unwrap();
        assert_eq!(marked.state, TabletState::Splitting);
        assert_eq!(marked.generation, 6);
        for id in [tablet_id(2), tablet_id(3)] {
            assert!(fixture.meta.tablet(id).is_some());
        }
        assert!(fixture
            .source_layout
            .tablet_dir()
            .join(SPLIT_PROGRESS_FILENAME)
            .is_file());

        let mut meta = fixture.meta.clone();
        let report = abort_split(&fixture.source_layout, &mut meta).unwrap();
        assert_eq!(report.source, fixture.source.tablet_id);
        assert_eq!(report.phase, Some(SplitPhase::SnapshotPinned));
        assert_eq!(report.children_removed, vec![tablet_id(2), tablet_id(3)]);
        // The source is Active again at one generation above the mark; the
        // local replica metadata follows.
        let restored = report.source_after.unwrap();
        assert_eq!(restored.state, TabletState::Active);
        assert_eq!(restored.generation, 7);
        assert_eq!(
            meta.tablet(fixture.source.tablet_id),
            Some(restored.clone())
        );
        assert_eq!(fixture.source_layout.load_metadata().unwrap(), restored);
        // The children are gone from the meta plane and their layouts are
        // torn down; the progress record is removed.
        for (index, id) in [tablet_id(2), tablet_id(3)].into_iter().enumerate() {
            assert!(meta.tablet(id).is_none());
            assert!(!fixture.plan.children[index].layout.tablet_dir().exists());
        }
        assert!(!fixture
            .source_layout
            .tablet_dir()
            .join(SPLIT_PROGRESS_FILENAME)
            .exists());
        // The source keyspace was never touched by the abort.
        assert_eq!(fixture.keyspace.rows_at(ts(u64::MAX)).len(), 14);
    }

    #[test]
    fn abort_is_idempotent_across_a_mid_abort_crash() {
        let _lock = EXECUTOR_TEST_LOCK.lock().unwrap();
        let fixture = split_fixture();
        let mut executor = begin_executor(&fixture);
        executor.run_until(SplitPhase::ChildrenCreated).unwrap();
        drop(executor);

        let mut meta = fixture.meta.clone();
        let first = abort_split(&fixture.source_layout, &mut meta).unwrap();
        assert_eq!(first.phase, Some(SplitPhase::ChildrenCreated));
        // A second drive finds no progress record and does nothing.
        let second = abort_split(&fixture.source_layout, &mut meta).unwrap();
        assert_eq!(second.phase, None);
        assert!(second.children_removed.is_empty());
        assert!(second.source_after.is_none());
        // The meta plane still holds exactly the restored source.
        let restored = meta.tablet(fixture.source.tablet_id).unwrap();
        assert_eq!(restored.state, TabletState::Active);
        assert_eq!(restored.generation, 7);
    }

    #[test]
    fn abort_at_started_unwinds_before_any_meta_write() {
        let _lock = EXECUTOR_TEST_LOCK.lock().unwrap();
        let fixture = split_fixture();
        let executor = begin_executor(&fixture);
        drop(executor); // phase == Started: no meta-plane change yet

        let mut meta = fixture.meta.clone();
        let report = abort_split(&fixture.source_layout, &mut meta).unwrap();
        assert_eq!(report.phase, Some(SplitPhase::Started));
        assert!(report.children_removed.is_empty());
        // The source is still at its initiation descriptor (untouched).
        let restored = report.source_after.unwrap();
        assert_eq!(restored, fixture.source);
        assert!(!fixture
            .source_layout
            .tablet_dir()
            .join(SPLIT_PROGRESS_FILENAME)
            .exists());
    }

    #[test]
    fn abort_after_publish_fails_closed() {
        let _lock = EXECUTOR_TEST_LOCK.lock().unwrap();
        let fixture = split_fixture();
        let mut executor = begin_executor(&fixture);
        executor.run_until(SplitPhase::Published).unwrap();
        drop(executor);

        let mut meta = fixture.meta.clone();
        let error = abort_split(&fixture.source_layout, &mut meta).unwrap_err();
        assert!(matches!(
            error,
            SplitError::CannotAbort { tablet, phase }
                if tablet == fixture.source.tablet_id && phase == SplitPhase::Published
        ));
        // The published routing is untouched: children Active, source Retiring.
        assert_eq!(
            meta.tablet(fixture.source.tablet_id).unwrap().state,
            TabletState::Retiring
        );
        for id in [tablet_id(2), tablet_id(3)] {
            assert_eq!(meta.tablet(id).unwrap().state, TabletState::Active);
        }
    }

    #[test]
    fn the_source_can_split_again_after_an_abort() {
        let _lock = EXECUTOR_TEST_LOCK.lock().unwrap();
        let fixture = split_fixture();
        let mut executor = begin_executor(&fixture);
        executor.run_until(SplitPhase::ChildrenCreated).unwrap();
        drop(executor);
        let mut meta = fixture.meta.clone();
        abort_split(&fixture.source_layout, &mut meta).unwrap();

        // A fresh split of the restored source plans and runs to completion
        // from the post-abort generation.
        let restored = meta.tablet(fixture.source.tablet_id).unwrap();
        let planner = TabletSplitPlanner::new(fixture._dir.path());
        let plan = planner
            .plan(
                &restored,
                SplitKeySelection::Explicit(text_key("n")),
                ts(300),
                [allocation(4, 4, 41), allocation(5, 5, 51)],
            )
            .unwrap();
        let sinks = [MapChildSink::new(), MapChildSink::new()];
        let mut executor = SplitExecutor::begin(
            plan,
            fixture.source_layout.clone(),
            meta.clone(),
            fixture.keyspace.clone(),
            sinks.clone(),
        )
        .unwrap();
        executor.run().unwrap();
        assert!(meta.tablet(fixture.source.tablet_id).is_none());
        let mut union = sinks[0].rows();
        union.extend(sinks[1].rows());
        assert_eq!(union, fixture.keyspace.rows_at(ts(u64::MAX)));
    }
}
