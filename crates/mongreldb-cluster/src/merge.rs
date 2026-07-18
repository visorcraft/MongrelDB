//! Safe tablet merge protocol (spec section 12.6, Stage 3F).
//!
//! Two adjacent, compatible tablets become one replacement tablet with zero
//! data loss, zero duplication, and no routing window in which a key is
//! unserved or served by two owners. Merge is the mirror of split (spec
//! section 12.5) and reuses its seams and mechanics:
//!
//! ```text
//! 1. Validate the pair (MergeRejection per violated requirement):
//!    same table/schema, adjacent ranges, compatible placement,
//!    no conflicting schema job, combined size under threshold
//! 2. Meta marks both sources `Merging`            (TabletMetaPlane::set_tablet)
//! 3. Create the replacement descriptor as learners (Creating; never routable)
//! 4. Pin both source snapshots at `merge_ts`       (TabletKeyspace::pin_snapshot)
//! 5. Build the replacement state (staged build, atomic install)
//! 6. Stream/catch up both sources' deltas
//! 7. Routing publication barrier (the phase machine)
//! 8. Publish replacement Active + sources Retiring atomically
//!    (MergePublishCommand, ONE meta command)
//! 9. Redirect stale requests (check_generation + crate::split::retry_guidance)
//! 10. Retain sources while old-generation pins remain (SourceRetentionGuard)
//! 11. Remove source replicas (Retired + TabletLayout::teardown)
//! ```
//!
//! The replacement is the spec's *hidden replacement*: the sources keep
//! serving (`Merging` is routable) until the atomic publication flips
//! routing to the `Active` replacement in one meta command.
//!
//! # Crash safety
//!
//! Identical to split: every step is idempotent and progress persists after
//! each phase in a versioned, checksummed `merge.json` inside the *lower*
//! source's tablet directory. [`MergeExecutor::resume`] reloads it and
//! continues from the persisted phase; completion removes the record with
//! the lower source's teardown. Fault hooks (registered in the
//! `mongreldb-fault` catalog): `tablet.merge.before` / `tablet.merge.after`
//! bracket the atomic publication, and `tablet.merge.phase.1` ..=
//! `tablet.merge.phase.7` fire after each phase's durable record
//! ([`MergePhase`] declaration order).
//!
//! # Generation rules
//!
//! With `g1`, `g2` the pre-merge source generations and
//! `m = max(g1, g2)`: the sources are marked `Merging` at `g1 + 1` and
//! `g2 + 1`; the replacement is created `Creating` at `m + 1`; the atomic
//! publication assigns one command-wide generation `p = m + 2` to the
//! replacement (`Active`) and both sources (`Retiring`) — a source whose
//! generation lags jumps to `p` (a generation is a version, not a count of
//! the tablet's own publications). Source removal publishes
//! `Retiring -> Retired` at `p + 1` before deletion. Stale requests
//! classify as on split: `TabletMoved` against a `Retiring` source,
//! `StaleMetadata` against the `Active` replacement.

use std::fmt;
use std::path::PathBuf;

use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::{NodeId, SchemaVersion, TableId, TabletId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::meta::MetaRejectionReason;
use crate::node::ClusterError;
use crate::split::{
    ChildAllocation, ChildPlan, ChildProgress, ChildStateSink, SnapshotPin, SourceRetentionGuard,
    TabletDataError, TabletKeyspace, TabletMetaPlane,
};
use crate::tablet::{Key, ReplicaRole, TabletDescriptor, TabletError, TabletLayout, TabletState};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Why a merge pair was refused (spec section 12.6): one typed rejection per
/// requirement.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MergeRejection {
    /// The sources partition different tables.
    #[error("tablets belong to different tables ({first_table} vs {second_table})")]
    DifferentTables {
        /// The first source's table.
        first_table: TableId,
        /// The second source's table.
        second_table: TableId,
    },
    /// The sources serve different schema versions.
    #[error("schema versions differ on table {table}: {first} vs {second}")]
    SchemaMismatch {
        /// The shared table.
        table: TableId,
        /// The first source's schema version.
        first: SchemaVersion,
        /// The second source's schema version.
        second: SchemaVersion,
    },
    /// The source ranges overlap or leave a gap.
    #[error("tablets {first} and {second} are not adjacent")]
    NotAdjacent {
        /// One source.
        first: TabletId,
        /// The other source.
        second: TabletId,
    },
    /// The sources place replicas on different node sets, so no single
    /// existing placement can host the merged tablet (replica movement is
    /// the placement wave's protocol, not merge's).
    #[error("placement is incompatible: {first_nodes:?} vs {second_nodes:?}")]
    IncompatiblePlacement {
        /// The first source's replica nodes, sorted.
        first_nodes: Vec<NodeId>,
        /// The second source's replica nodes, sorted.
        second_nodes: Vec<NodeId>,
    },
    /// A schema job is in flight on the table; merging under it would race
    /// the job's tablet scan.
    #[error("schema job {job_id} is active on table {table}")]
    ConflictingSchemaJob {
        /// The table with the active job.
        table: TableId,
        /// The conflicting job.
        job_id: u64,
    },
    /// The merged tablet would exceed the configured size threshold.
    #[error(
        "combined size {combined_bytes} bytes exceeds the merge threshold {threshold_bytes} bytes"
    )]
    CombinedSizeExceedsThreshold {
        /// `first_size_bytes + second_size_bytes`.
        combined_bytes: u64,
        /// The configured threshold.
        threshold_bytes: u64,
    },
    /// A source is not serving (mid-split, mid-merge, or retired).
    #[error("source tablet {tablet} is in state {state}, expected Active")]
    InvalidSourceState {
        /// The offending source.
        tablet: TabletId,
        /// Its current state.
        state: TabletState,
    },
}

/// The one error type of the merge surface: validation, planning, execution,
/// and resume.
#[derive(Debug, thiserror::Error)]
pub enum MergeError {
    /// Descriptor, layout, or persisted-progress failure. Always fail closed.
    #[error(transparent)]
    Tablet(#[from] TabletError),
    /// An armed fault hook fired (crash-resume tests).
    #[error(transparent)]
    Fault(#[from] mongreldb_fault::Fault),
    /// The meta plane refused a descriptor write or the publication.
    #[error(transparent)]
    MetaPlane(#[from] MetaRejectionReason),
    /// A keyspace or replacement-sink seam operation failed.
    #[error(transparent)]
    TabletData(#[from] TabletDataError),
    /// The pair violated a merge requirement.
    #[error(transparent)]
    Rejected(#[from] MergeRejection),
    /// The plan is structurally inconsistent.
    #[error("invalid merge plan: {0}")]
    InvalidPlan(String),
    /// A source keyspace holds a key outside its own partition (fail closed
    /// instead of dropping or misplacing it).
    #[error("applied key {0} lies outside its source partition")]
    KeyOutsideSource(Key),
    /// A source still has old-generation pins; retry when they drain.
    #[error("source tablet {tablet} is retained by {pins} old-generation pin(s)")]
    SourceRetained {
        /// The retained source.
        tablet: TabletId,
        /// Old-generation pins still outstanding.
        pins: usize,
    },
}

// ---------------------------------------------------------------------------
// Merge phases (persisted; declaration order frozen, spec section 4.10)
// ---------------------------------------------------------------------------

/// The durably persisted phases of one merge (spec section 12.6). The
/// executor resumes from the last persisted phase after a crash.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum MergePhase {
    /// The plan is formed and recorded; no meta-plane change yet.
    Started,
    /// Both sources are marked `Merging` (each at its own `g + 1`).
    MarkedMerging,
    /// The replacement descriptor and layout are created as `Creating`
    /// learners.
    ReplacementCreated,
    /// Both source snapshots are pinned at `merge_ts`.
    SnapshotsPinned,
    /// The replacement state is built from the pinned snapshots.
    ReplacementBuilt,
    /// Both sources' deltas are streamed; the replacement is caught up, so
    /// the routing publication barrier is satisfied.
    CaughtUp,
    /// Replacement `Active` + sources `Retiring` published atomically.
    Published,
    /// Both sources are `Retired` and their replicas torn down. Terminal.
    SourcesRetired,
}

impl MergePhase {
    /// The fault hook fired after this phase's progress record is durable
    /// (`Started` has none). Hook names are registered in the
    /// `mongreldb-fault` catalog.
    pub fn hook_name(self) -> Option<&'static str> {
        Some(match self {
            Self::Started => return None,
            Self::MarkedMerging => "tablet.merge.phase.1",
            Self::ReplacementCreated => "tablet.merge.phase.2",
            Self::SnapshotsPinned => "tablet.merge.phase.3",
            Self::ReplacementBuilt => "tablet.merge.phase.4",
            Self::CaughtUp => "tablet.merge.phase.5",
            Self::Published => "tablet.merge.phase.6",
            Self::SourcesRetired => "tablet.merge.phase.7",
        })
    }
}

impl fmt::Display for MergePhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Started => "Started",
            Self::MarkedMerging => "MarkedMerging",
            Self::ReplacementCreated => "ReplacementCreated",
            Self::SnapshotsPinned => "SnapshotsPinned",
            Self::ReplacementBuilt => "ReplacementBuilt",
            Self::CaughtUp => "CaughtUp",
            Self::Published => "Published",
            Self::SourcesRetired => "SourcesRetired",
        };
        f.write_str(name)
    }
}

// ---------------------------------------------------------------------------
// The merge plan
// ---------------------------------------------------------------------------

/// Everything merge validation needs about the candidate pair (spec section
/// 12.6's requirement list).
#[derive(Clone, Debug)]
pub struct MergeInputs {
    /// One source tablet (`Active`).
    pub first: TabletDescriptor,
    /// The other source tablet (`Active`).
    pub second: TabletDescriptor,
    /// The schema version the first source serves.
    pub first_schema: SchemaVersion,
    /// The schema version the second source serves.
    pub second_schema: SchemaVersion,
    /// The id of a schema job currently active on the table, if any (the
    /// meta plane resolves its job registry down to this fact).
    pub active_schema_job: Option<u64>,
    /// The first source's applied size in bytes.
    pub first_size_bytes: u64,
    /// The second source's applied size in bytes.
    pub second_size_bytes: u64,
    /// The configured merge threshold: the merged tablet's combined size
    /// must not exceed it.
    pub max_merged_size_bytes: u64,
}

/// Validates a merge candidate against every requirement of spec section
/// 12.6, returning the sources ordered lower-half-first.
fn validate_merge_inputs(inputs: &MergeInputs) -> Result<[TabletDescriptor; 2], MergeRejection> {
    let (first, second) = (&inputs.first, &inputs.second);
    for source in [first, second] {
        if source.state != TabletState::Active {
            return Err(MergeRejection::InvalidSourceState {
                tablet: source.tablet_id,
                state: source.state,
            });
        }
    }
    if first.table_id != second.table_id {
        return Err(MergeRejection::DifferentTables {
            first_table: first.table_id,
            second_table: second.table_id,
        });
    }
    if inputs.first_schema != inputs.second_schema {
        return Err(MergeRejection::SchemaMismatch {
            table: first.table_id,
            first: inputs.first_schema,
            second: inputs.second_schema,
        });
    }
    let ordered: [&TabletDescriptor; 2] = if first.partition.meets_start_of(&second.partition) {
        [first, second]
    } else if second.partition.meets_start_of(&first.partition) {
        [second, first]
    } else {
        return Err(MergeRejection::NotAdjacent {
            first: first.tablet_id,
            second: second.tablet_id,
        });
    };
    let nodes = |descriptor: &TabletDescriptor| -> Vec<NodeId> {
        let mut nodes: Vec<NodeId> = descriptor
            .replicas
            .iter()
            .map(|replica| replica.node_id)
            .collect();
        nodes.sort();
        nodes
    };
    let (first_nodes, second_nodes) = (nodes(first), nodes(second));
    if first_nodes != second_nodes {
        return Err(MergeRejection::IncompatiblePlacement {
            first_nodes,
            second_nodes,
        });
    }
    if let Some(job_id) = inputs.active_schema_job {
        return Err(MergeRejection::ConflictingSchemaJob {
            table: first.table_id,
            job_id,
        });
    }
    let combined = inputs
        .first_size_bytes
        .checked_add(inputs.second_size_bytes)
        .ok_or(MergeRejection::CombinedSizeExceedsThreshold {
            combined_bytes: u64::MAX,
            threshold_bytes: inputs.max_merged_size_bytes,
        })?;
    if combined > inputs.max_merged_size_bytes {
        return Err(MergeRejection::CombinedSizeExceedsThreshold {
            combined_bytes: combined,
            threshold_bytes: inputs.max_merged_size_bytes,
        });
    }
    Ok([ordered[0].clone(), ordered[1].clone()])
}

/// One merge: the two sources (lower half first, as initiated) and the
/// hidden replacement.
#[derive(Clone, Debug)]
pub struct MergePlan {
    /// The sources as they were at initiation (`Active`, generations
    /// `g1`, `g2`), lower half first.
    pub sources: [TabletDescriptor; 2],
    /// The replacement tablet: the union bounds, its layout, its initial
    /// (learner) replica set.
    pub replacement: ChildPlan,
    /// The timestamp both source snapshots are pinned at.
    pub merge_ts: HlcTimestamp,
}

impl MergePlan {
    /// Structural validation: adjacent ordered sources, the replacement
    /// covering exactly their union, fresh distinct ids, and a structurally
    /// valid creation-time replacement descriptor.
    pub fn validate(&self) -> Result<(), MergeError> {
        for source in &self.sources {
            source.validate()?;
        }
        if !self.sources[0]
            .partition
            .meets_start_of(&self.sources[1].partition)
        {
            return Err(MergeError::InvalidPlan(
                "merge sources are not ordered adjacent halves".to_owned(),
            ));
        }
        let union = self.sources[0]
            .partition
            .union_adjacent(&self.sources[1].partition)
            .ok_or_else(|| MergeError::InvalidPlan("source bounds are not adjacent".to_owned()))?;
        if self.replacement.bounds != union {
            return Err(MergeError::InvalidPlan(
                "replacement bounds are not the union of the source bounds".to_owned(),
            ));
        }
        if self.sources[0].tablet_id == self.sources[1].tablet_id
            || self
                .sources
                .iter()
                .any(|source| source.tablet_id == self.replacement.layout.tablet_id())
            || self.sources[0].raft_group_id == self.sources[1].raft_group_id
            || self
                .sources
                .iter()
                .any(|source| source.raft_group_id == self.replacement.layout.raft_group_id())
        {
            return Err(MergeError::InvalidPlan(
                "source and replacement tablet/raft-group ids must be distinct".to_owned(),
            ));
        }
        self.replacement_descriptor().validate()?;
        Ok(())
    }

    /// The replacement descriptor as created: `Creating` at the inception
    /// generation (`max(g1, g2) + 1`), every replica a learner. `Creating`
    /// tablets are never routed to (spec section 12.5).
    pub fn replacement_descriptor(&self) -> TabletDescriptor {
        let generation = self
            .sources
            .iter()
            .map(|source| source.generation)
            .max()
            .expect("two sources")
            .checked_add(1)
            .expect("descriptor generation overflows u64");
        self.replacement.descriptor(
            self.sources[0].table_id,
            generation,
            TabletState::Creating,
            ReplicaRole::Learner,
        )
    }

    /// The command-wide publication generation (`max(g1, g2) + 2`).
    pub fn publish_generation(&self) -> u64 {
        self.sources
            .iter()
            .map(|source| source.generation)
            .max()
            .expect("two sources")
            .checked_add(2)
            .expect("descriptor generation overflows u64")
    }
}

/// Produces [`MergePlan`]s (spec section 12.6).
#[derive(Clone, Debug)]
pub struct MergePlanner {
    node_data: PathBuf,
}

impl MergePlanner {
    /// A planner rooted at the node's data directory.
    pub fn new(node_data: impl Into<PathBuf>) -> Self {
        Self {
            node_data: node_data.into(),
        }
    }

    /// Validates the candidate pair (typed [`MergeRejection`]s per violated
    /// requirement) and plans the hidden replacement.
    pub fn plan(
        &self,
        inputs: MergeInputs,
        merge_ts: HlcTimestamp,
        allocation: ChildAllocation,
    ) -> Result<MergePlan, MergeError> {
        inputs.first.validate()?;
        inputs.second.validate()?;
        let sources = validate_merge_inputs(&inputs)?;
        let bounds = sources[0]
            .partition
            .union_adjacent(&sources[1].partition)
            .ok_or_else(|| MergeError::InvalidPlan("source bounds are not adjacent".to_owned()))?;
        let plan = MergePlan {
            sources,
            replacement: ChildPlan {
                bounds,
                layout: TabletLayout::new(
                    self.node_data.clone(),
                    allocation.tablet_id,
                    allocation.raft_group_id,
                ),
                replicas: allocation.replicas,
            },
            merge_ts,
        };
        plan.validate()?;
        Ok(plan)
    }
}

// ---------------------------------------------------------------------------
// The atomic routing publication
// ---------------------------------------------------------------------------

/// The routing publication of one merge, applied by the meta group as ONE
/// command (spec section 12.6): the replacement becomes `Active` and both
/// sources become `Retiring` atomically at one new generation. Never
/// proposed before the replacement is caught up (the executor's phase
/// machine is the barrier).
///
/// This is the shape the meta wave adopts as a meta command; the reference
/// [`MergeMetaPlane::publish_merge`] applies its semantics.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MergePublishCommand {
    /// The sources, republished `Retiring` at the publication generation,
    /// lower half first.
    pub sources: [TabletDescriptor; 2],
    /// The replacement, published `Active` at the publication generation.
    /// Publication promotes the caught-up learners to voters (the Raft
    /// membership change rides the same meta command in the runtime wave).
    pub replacement: TabletDescriptor,
    /// The timestamp both source snapshots were pinned at.
    pub merge_ts: HlcTimestamp,
}

impl MergePublishCommand {
    /// Builds the publication of `plan` at the command-wide publication
    /// generation (see the module's generation rules).
    pub fn from_plan(plan: &MergePlan) -> Result<Self, MergeError> {
        let publish_generation = plan.publish_generation();
        let mut sources = plan.sources.clone();
        for source in &mut sources {
            let marked = source.published_transition(TabletState::Merging)?;
            let mut retiring = marked.published_transition(TabletState::Retiring)?;
            // One command-wide generation for all three descriptors.
            retiring.generation = publish_generation;
            *source = retiring;
        }
        let mut replacement = plan
            .replacement_descriptor()
            .published_transition(TabletState::Active)?;
        debug_assert_eq!(replacement.generation, publish_generation);
        for replica in &mut replacement.replicas {
            replica.role = ReplicaRole::Voter;
        }
        let command = Self {
            sources,
            replacement,
            merge_ts: plan.merge_ts,
        };
        command.validate()?;
        Ok(command)
    }

    /// The generation the publication assigns to all three descriptors.
    pub fn publish_generation(&self) -> u64 {
        self.replacement.generation
    }

    /// Structural validation of the atomic publication: states and the
    /// shared generation, one table, and replacement bounds covering exactly
    /// the union of the source bounds.
    pub fn validate(&self) -> Result<(), MergeError> {
        if self.replacement.state != TabletState::Active {
            return Err(MergeError::InvalidPlan(format!(
                "published replacement must be Active, is {}",
                self.replacement.state
            )));
        }
        let generation = self.replacement.generation;
        for source in &self.sources {
            if source.state != TabletState::Retiring {
                return Err(MergeError::InvalidPlan(format!(
                    "published source {} must be Retiring, is {}",
                    source.tablet_id, source.state
                )));
            }
            if source.generation != generation {
                return Err(MergeError::InvalidPlan(
                    "publication assigns one generation to all descriptors".to_owned(),
                ));
            }
            if source.table_id != self.replacement.table_id {
                return Err(MergeError::InvalidPlan(
                    "sources and replacement name different tables".to_owned(),
                ));
            }
            source.validate()?;
        }
        self.replacement.validate()?;
        let union = self.sources[0]
            .partition
            .union_adjacent(&self.sources[1].partition)
            .ok_or_else(|| MergeError::InvalidPlan("source bounds are not adjacent".to_owned()))?;
        if self.replacement.partition != union {
            return Err(MergeError::InvalidPlan(
                "published replacement bounds are not the union of the source bounds".to_owned(),
            ));
        }
        if self.sources[0].tablet_id == self.sources[1].tablet_id
            || self
                .sources
                .iter()
                .any(|source| source.tablet_id == self.replacement.tablet_id)
        {
            return Err(MergeError::InvalidPlan(
                "publication tablet ids must be distinct".to_owned(),
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Persisted merge progress (crash resume)
// ---------------------------------------------------------------------------

/// Name of the progress record, inside the *lower* source's tablet directory.
pub const MERGE_PROGRESS_FILENAME: &str = "merge.json";
/// The progress-record format version this build writes.
pub const MERGE_PROGRESS_FORMAT_VERSION: u32 = 1;
/// The oldest progress-record format version this build accepts.
pub const MIN_SUPPORTED_MERGE_PROGRESS_FORMAT_VERSION: u32 = 1;

/// The durable progress of one merge: everything [`MergeExecutor::resume`]
/// needs to continue idempotently after a crash.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeProgress {
    /// The source descriptors as they were at initiation (`Active`), lower
    /// half first.
    pub sources: [TabletDescriptor; 2],
    /// The replacement plan.
    pub replacement: ChildProgress,
    /// The timestamp both source snapshots are pinned at.
    pub merge_ts: HlcTimestamp,
    /// The last durably completed phase.
    pub phase: MergePhase,
}

impl MergeProgress {
    /// Records `plan` at `phase`.
    pub fn from_plan(plan: &MergePlan, phase: MergePhase) -> Self {
        Self {
            sources: plan.sources.clone(),
            replacement: plan.replacement.progress(),
            merge_ts: plan.merge_ts,
            phase,
        }
    }

    /// Rebuilds the runtime plan.
    pub fn plan(&self) -> MergePlan {
        MergePlan {
            sources: self.sources.clone(),
            replacement: self.replacement.plan(),
            merge_ts: self.merge_ts,
        }
    }
}

/// The durable progress-record envelope: versioned and checksummed with the
/// same idiom as `tablet.json` (fail closed on torn or foreign files).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MergeProgressFile {
    /// Durable format version; see [`MERGE_PROGRESS_FORMAT_VERSION`].
    format_version: u32,
    /// Lowercase-hex SHA-256 of the canonical JSON encoding of `progress`.
    checksum: String,
    /// The persisted progress.
    progress: MergeProgress,
}

impl MergeProgressFile {
    fn envelope(progress: &MergeProgress) -> Result<Self, MergeError> {
        Ok(Self {
            format_version: MERGE_PROGRESS_FORMAT_VERSION,
            checksum: progress_checksum(progress).map_err(meta_io)?,
            progress: progress.clone(),
        })
    }
}

/// SHA-256 of the canonical (compact JSON) encoding of the progress record.
fn progress_checksum(progress: &MergeProgress) -> Result<String, ClusterError> {
    let bytes = serde_json::to_vec(progress).map_err(|error| ClusterError::CorruptMetadata {
        file: MERGE_PROGRESS_FILENAME,
        detail: format!("encode: {error}"),
    })?;
    Ok(hex_encode(&Sha256::digest(&bytes)))
}

/// Maps a node-layer metadata failure onto the merge error surface.
fn meta_io(error: ClusterError) -> MergeError {
    MergeError::Tablet(TabletError::Metadata(error))
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
// The meta-plane seam
// ---------------------------------------------------------------------------

/// The merge half of the meta-plane seam (see [`TabletMetaPlane`]).
pub trait MergeMetaPlane: TabletMetaPlane {
    /// The atomic routing publication of spec section 12.6: the replacement
    /// becomes `Active` and the sources `Retiring` — ONE meta command, never
    /// exposed before catch-up. The default applies the command's
    /// descriptors within this one call; the meta-group binding overrides it
    /// with a single raft proposal carrying the command.
    fn publish_merge(&mut self, command: &MergePublishCommand) -> Result<(), MetaRejectionReason> {
        command
            .validate()
            .map_err(|error| MetaRejectionReason::Invalid {
                reason: error.to_string(),
            })?;
        self.set_tablet(&command.replacement)?;
        for source in &command.sources {
            self.set_tablet(source)?;
        }
        Ok(())
    }
}

impl MergeMetaPlane for crate::split::InMemoryMetaPlane {}

// ---------------------------------------------------------------------------
// The merge executor
// ---------------------------------------------------------------------------

/// Drives one merge through the steps of spec section 12.6, persisting
/// progress after every phase so a crash resumes where it stopped.
/// Construct with [`Self::begin`] (a fresh merge) or [`Self::resume`] (after
/// a crash; `None` when no merge is in progress), then [`Self::run`] to
/// completion or [`Self::step`] phase by phase.
///
/// `M` is the meta plane, `K` the source keyspaces (ordered like the
/// sources), `S` the replacement-state sink.
pub struct MergeExecutor<M, K, S> {
    progress: MergeProgress,
    source_layouts: [TabletLayout; 2],
    meta: M,
    keyspaces: [K; 2],
    sink: S,
    snapshot_pins: [Option<Box<dyn SnapshotPin>>; 2],
    retention: Option<[SourceRetentionGuard; 2]>,
}

impl<M: MergeMetaPlane, K: TabletKeyspace, S: ChildStateSink> MergeExecutor<M, K, S> {
    /// Begins a fresh merge: validates the plan and records it durably
    /// (`Started`). The source layouts must be this node's live replicas of
    /// the two source tablets, ordered lower half first.
    pub fn begin(
        plan: MergePlan,
        source_layouts: [TabletLayout; 2],
        meta: M,
        keyspaces: [K; 2],
        sink: S,
    ) -> Result<Self, MergeError> {
        plan.validate()?;
        for (source, layout) in plan.sources.iter().zip(source_layouts.iter()) {
            if source.state != TabletState::Active {
                return Err(MergeRejection::InvalidSourceState {
                    tablet: source.tablet_id,
                    state: source.state,
                }
                .into());
            }
            if layout.tablet_id() != source.tablet_id
                || layout.raft_group_id() != source.raft_group_id
            {
                return Err(TabletError::TabletMismatch {
                    path: layout.tablet_dir(),
                    expected: layout.tablet_id(),
                    found: source.tablet_id,
                    expected_group: layout.raft_group_id(),
                    found_group: source.raft_group_id,
                }
                .into());
            }
            layout.validate()?;
        }
        let executor = Self {
            progress: MergeProgress::from_plan(&plan, MergePhase::Started),
            source_layouts,
            meta,
            keyspaces,
            sink,
            snapshot_pins: [None, None],
            retention: None,
        };
        executor.persist_progress()?;
        Ok(executor)
    }

    /// Resumes a merge after a crash: reloads the persisted progress from the
    /// lower source's tablet directory (`None` when no merge is in
    /// progress — including one whose final teardown already removed the
    /// record).
    pub fn resume(
        source_layouts: [TabletLayout; 2],
        meta: M,
        keyspaces: [K; 2],
        sink: S,
    ) -> Result<Option<Self>, MergeError> {
        let Some(progress) = load_progress(&source_layouts[0])? else {
            return Ok(None);
        };
        for (source, layout) in progress.sources.iter().zip(source_layouts.iter()) {
            if source.tablet_id != layout.tablet_id()
                || source.raft_group_id != layout.raft_group_id()
            {
                return Err(TabletError::TabletMismatch {
                    path: layout.tablet_dir(),
                    expected: layout.tablet_id(),
                    found: source.tablet_id,
                    expected_group: layout.raft_group_id(),
                    found_group: source.raft_group_id,
                }
                .into());
            }
        }
        progress.plan().validate()?;
        Ok(Some(Self {
            progress,
            source_layouts,
            meta,
            keyspaces,
            sink,
            snapshot_pins: [None, None],
            retention: None,
        }))
    }

    /// The last durably completed phase.
    pub fn phase(&self) -> MergePhase {
        self.progress.phase
    }

    /// The persisted progress record.
    pub fn progress(&self) -> &MergeProgress {
        &self.progress
    }

    /// The plan the merge executes.
    pub fn plan(&self) -> MergePlan {
        self.progress.plan()
    }

    /// The source retention guards (installed at publication).
    pub fn retention(&self) -> Option<&[SourceRetentionGuard; 2]> {
        self.retention.as_ref()
    }

    /// The meta plane.
    pub fn meta(&self) -> &M {
        &self.meta
    }

    /// The source keyspaces, ordered like the sources.
    pub fn keyspaces(&self) -> &[K; 2] {
        &self.keyspaces
    }

    /// The replacement-state sink.
    pub fn sink(&self) -> &S {
        &self.sink
    }

    /// Executes the next phase, persists it, and fires its fault hook.
    /// Returns the newly completed phase. Idempotent: re-entering after a
    /// failure redoes only the failed phase's work.
    pub fn step(&mut self) -> Result<MergePhase, MergeError> {
        use MergePhase::{
            CaughtUp, MarkedMerging, Published, ReplacementBuilt, ReplacementCreated,
            SnapshotsPinned, SourcesRetired, Started,
        };
        let next = match self.progress.phase {
            Started => {
                self.mark_sources_merging()?;
                MarkedMerging
            }
            MarkedMerging => {
                self.create_replacement()?;
                ReplacementCreated
            }
            ReplacementCreated => {
                self.pin_source_snapshots()?;
                SnapshotsPinned
            }
            SnapshotsPinned => {
                self.build_replacement()?;
                ReplacementBuilt
            }
            ReplacementBuilt => {
                self.catch_up_replacement()?;
                CaughtUp
            }
            CaughtUp => {
                self.publish_replacement()?;
                Published
            }
            Published => {
                self.remove_sources()?;
                SourcesRetired
            }
            SourcesRetired => SourcesRetired,
        };
        // The terminal phase needs no progress record: the lower source's
        // teardown already removed it with the tablet directory.
        self.progress.phase = next;
        if next != SourcesRetired {
            self.persist_progress()?;
        }
        if let Some(hook) = next.hook_name() {
            mongreldb_fault::inject(hook)?;
        }
        Ok(next)
    }

    /// Runs every remaining phase to completion. A
    /// [`MergeError::SourceRetained`] surfaces with the merge parked at
    /// [`MergePhase::Published`] — drop the old-generation pins and call
    /// [`Self::run`] (or [`Self::resume`]) again.
    pub fn run(&mut self) -> Result<(), MergeError> {
        while self.progress.phase != MergePhase::SourcesRetired {
            self.step()?;
        }
        Ok(())
    }

    /// Runs until `phase` is complete (test/driver convenience).
    pub fn run_until(&mut self, phase: MergePhase) -> Result<(), MergeError> {
        while self.progress.phase != phase {
            self.step()?;
        }
        Ok(())
    }

    /// Both sources are marked `Merging` (each at its own `g + 1`); local
    /// replica metadata follows.
    fn mark_sources_merging(&mut self) -> Result<(), MergeError> {
        for (source, layout) in self.progress.sources.iter().zip(self.source_layouts.iter()) {
            let marked = source.published_transition(TabletState::Merging)?;
            self.meta.set_tablet(&marked)?;
            layout.store_metadata(&marked)?;
        }
        // Borrow of `self.progress` ends; re-borrow mutably below is fine.
        Ok(())
    }

    /// The replacement descriptor is created as `Creating` learners — never
    /// routable — and its on-disk layout is created.
    fn create_replacement(&mut self) -> Result<(), MergeError> {
        let plan = self.plan();
        let descriptor = plan.replacement_descriptor();
        plan.replacement.layout.create(&descriptor)?;
        self.meta.set_tablet(&descriptor)?;
        Ok(())
    }

    /// Both source snapshots are pinned at `merge_ts` (re-pinned after a
    /// crash resume).
    fn pin_source_snapshots(&mut self) -> Result<(), MergeError> {
        self.ensure_snapshot_pins()
    }

    /// Re-acquires any snapshot pin the executor does not hold.
    fn ensure_snapshot_pins(&mut self) -> Result<(), MergeError> {
        for index in 0..2 {
            if self.snapshot_pins[index].is_none() {
                self.snapshot_pins[index] =
                    Some(self.keyspaces[index].pin_snapshot(self.progress.merge_ts)?);
            }
        }
        Ok(())
    }

    /// The pinned snapshots are unioned into the replacement sink (staged
    /// build, atomic install). Adjacent ranges are disjoint, so every key
    /// appears exactly once; a key outside its source's partition fails
    /// closed as corrupt.
    fn build_replacement(&mut self) -> Result<(), MergeError> {
        self.ensure_snapshot_pins()?;
        self.sink.begin_build()?;
        for index in 0..2 {
            let snapshot = self.keyspaces[index].snapshot_at(self.progress.merge_ts)?;
            for (key, value) in snapshot {
                if !self.progress.sources[index].partition.contains(&key) {
                    return Err(MergeError::KeyOutsideSource(key));
                }
                self.sink.stage(&key, &value)?;
            }
        }
        self.sink.install_staged()?;
        Ok(())
    }

    /// The post-`merge_ts` deltas of both sources stream into the
    /// replacement.
    fn catch_up_replacement(&mut self) -> Result<(), MergeError> {
        self.ensure_snapshot_pins()?;
        for index in 0..2 {
            let deltas = self.keyspaces[index].deltas_after(self.progress.merge_ts)?;
            for (key, value) in deltas {
                if !self.progress.sources[index].partition.contains(&key) {
                    return Err(MergeError::KeyOutsideSource(key));
                }
                self.sink.apply_delta(&key, &value)?;
            }
        }
        Ok(())
    }

    /// The atomic routing publication — replacement `Active`, sources
    /// `Retiring`, one generation — then the retention bookkeeping. The
    /// phase machine is the publication barrier.
    fn publish_replacement(&mut self) -> Result<(), MergeError> {
        let plan = self.plan();
        let command = MergePublishCommand::from_plan(&plan)?;
        mongreldb_fault::inject("tablet.merge.before")?;
        self.meta.publish_merge(&command)?;
        mongreldb_fault::inject("tablet.merge.after")?;
        // The local replica metadata follows the publication.
        plan.replacement
            .layout
            .store_metadata(&command.replacement)?;
        for (descriptor, layout) in command.sources.iter().zip(self.source_layouts.iter()) {
            layout.store_metadata(descriptor)?;
        }
        self.retention = Some(
            command
                .sources
                .clone()
                .map(|source| SourceRetentionGuard::new(source.tablet_id, source.generation)),
        );
        // The replacement is published; the snapshot pins have done their work.
        self.snapshot_pins = [None, None];
        Ok(())
    }

    /// Once neither source has old-generation pins, both are published
    /// `Retired`, their descriptors removed, and their replicas torn down
    /// (the lower source last: its directory holds the progress record).
    fn remove_sources(&mut self) -> Result<(), MergeError> {
        if let Some(guards) = &self.retention {
            for guard in guards {
                if !guard.ready_for_removal() {
                    return Err(MergeError::SourceRetained {
                        tablet: guard.source(),
                        pins: guard.old_generation_pins(),
                    });
                }
            }
        }
        for index in 0..2 {
            let source_id = self.progress.sources[index].tablet_id;
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
        }
        self.source_layouts[1].teardown()?;
        self.source_layouts[0].teardown()?;
        Ok(())
    }

    /// Persists the progress record atomically into the lower source's
    /// tablet directory.
    fn persist_progress(&self) -> Result<(), MergeError> {
        let file = MergeProgressFile::envelope(&self.progress)?;
        let bytes = crate::node::encode_json(MERGE_PROGRESS_FILENAME, &file).map_err(meta_io)?;
        crate::node::write_meta_atomic(
            &self.source_layouts[0].tablet_dir(),
            MERGE_PROGRESS_FILENAME,
            &bytes,
        )
        .map_err(ClusterError::Io)
        .map_err(meta_io)?;
        Ok(())
    }
}

/// Loads and verifies the persisted progress record (`None` when absent).
/// Corrupt, unknown-version, or foreign records fail closed.
fn load_progress(lower_layout: &TabletLayout) -> Result<Option<MergeProgress>, MergeError> {
    let path = lower_layout.tablet_dir().join(MERGE_PROGRESS_FILENAME);
    let Some(bytes) = crate::node::read_meta_file(&path).map_err(meta_io)? else {
        return Ok(None);
    };
    let file: MergeProgressFile =
        crate::node::decode_json(MERGE_PROGRESS_FILENAME, &bytes).map_err(meta_io)?;
    if file.format_version < MIN_SUPPORTED_MERGE_PROGRESS_FORMAT_VERSION
        || file.format_version > MERGE_PROGRESS_FORMAT_VERSION
    {
        return Err(meta_io(ClusterError::UnsupportedFormatVersion {
            file: MERGE_PROGRESS_FILENAME,
            found: file.format_version,
            min: MIN_SUPPORTED_MERGE_PROGRESS_FORMAT_VERSION,
            max: MERGE_PROGRESS_FORMAT_VERSION,
        }));
    }
    if file.checksum != progress_checksum(&file.progress).map_err(meta_io)? {
        return Err(meta_io(ClusterError::CorruptMetadata {
            file: MERGE_PROGRESS_FILENAME,
            detail: "checksum mismatch".to_owned(),
        }));
    }
    let progress = file.progress;
    if progress.sources[0].tablet_id != lower_layout.tablet_id()
        || progress.sources[0].raft_group_id != lower_layout.raft_group_id()
    {
        return Err(TabletError::TabletMismatch {
            path: lower_layout.tablet_dir(),
            expected: lower_layout.tablet_id(),
            found: progress.sources[0].tablet_id,
            expected_group: lower_layout.raft_group_id(),
            found_group: progress.sources[0].raft_group_id,
        }
        .into());
    }
    Ok(Some(progress))
}

/// Reads the persisted merge progress of the *lower* source tablet, if any
/// (the node runtime's resume probe). Same fail-closed verification as
/// [`MergeExecutor::resume`].
pub fn merge_progress(lower_layout: &TabletLayout) -> Result<Option<MergeProgress>, MergeError> {
    load_progress(lower_layout)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use mongreldb_types::ids::RaftGroupId;

    use super::*;
    use crate::split::{
        retry_guidance, ChildAllocation, InMemoryMetaPlane, MapChildSink, MapKeyspace,
        RetryGuidance, EXECUTOR_TEST_LOCK,
    };
    use crate::tablet::{
        check_generation, find_tablet_for_key, tablets_overlapping, Bound, KeyValue,
        PartitionBounds, ReplicaDescriptor, RoutingError, RowKeyEncoder,
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

    fn voters() -> Vec<ReplicaDescriptor> {
        vec![
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
        ]
    }

    fn source(
        tablet: u8,
        group: u8,
        low: Bound<Key>,
        high: Bound<Key>,
        generation: u64,
    ) -> TabletDescriptor {
        TabletDescriptor {
            tablet_id: tablet_id(tablet),
            table_id: TableId::new(3),
            database_id: mongreldb_types::ids::DatabaseId::ZERO,
            raft_group_id: group_id(group),
            partition: PartitionBounds::new(low, high).unwrap(),
            replicas: voters(),
            leader_hint: Some(node(1)),
            generation,
            state: TabletState::Active,
        }
    }

    /// The lower half [a, m) at generation 4 and the upper half [m, z) at
    /// generation 6.
    fn source_pair() -> (TabletDescriptor, TabletDescriptor) {
        (
            source(
                11,
                11,
                Bound::Included(text_key("a")),
                Bound::Excluded(text_key("m")),
                4,
            ),
            source(
                12,
                12,
                Bound::Included(text_key("m")),
                Bound::Excluded(text_key("z")),
                6,
            ),
        )
    }

    fn inputs(first: TabletDescriptor, second: TabletDescriptor) -> MergeInputs {
        MergeInputs {
            first,
            second,
            first_schema: SchemaVersion::new(9),
            second_schema: SchemaVersion::new(9),
            active_schema_job: None,
            first_size_bytes: 1_000,
            second_size_bytes: 2_000,
            max_merged_size_bytes: 1 << 20,
        }
    }

    fn replacement_allocation() -> ChildAllocation {
        ChildAllocation {
            tablet_id: tablet_id(13),
            raft_group_id: group_id(13),
            replicas: vec![
                ReplicaDescriptor {
                    node_id: node(3),
                    role: ReplicaRole::Voter,
                    raft_node_id: 41,
                },
                ReplicaDescriptor {
                    node_id: node(4),
                    role: ReplicaRole::Voter,
                    raft_node_id: 42,
                },
            ],
        }
    }

    // -- merge validation (spec 12.6 requirements) -----------------------------

    #[test]
    fn merge_validation_rejects_each_violated_requirement() {
        let dir = tempfile::tempdir().unwrap();
        let planner = MergePlanner::new(dir.path());
        let (left, right) = source_pair();
        let plan = |inputs: MergeInputs| planner.plan(inputs, ts(150), replacement_allocation());

        // The happy path validates and orders the sources lower-first.
        let merged = plan(inputs(left.clone(), right.clone())).unwrap();
        assert_eq!(merged.sources[0].tablet_id, left.tablet_id);
        assert_eq!(merged.sources[1].tablet_id, right.tablet_id);
        assert_eq!(
            merged.replacement.bounds,
            PartitionBounds::new(
                Bound::Included(text_key("a")),
                Bound::Excluded(text_key("z"))
            )
            .unwrap()
        );
        // Input order does not matter.
        let swapped = plan(inputs(right.clone(), left.clone())).unwrap();
        assert_eq!(swapped.sources[0].tablet_id, left.tablet_id);

        // Different tables.
        let mut foreign = right.clone();
        foreign.table_id = TableId::new(4);
        assert!(matches!(
            plan(inputs(left.clone(), foreign)),
            Err(MergeError::Rejected(MergeRejection::DifferentTables {
                first_table,
                second_table,
            })) if first_table == TableId::new(3) && second_table == TableId::new(4)
        ));

        // Schema mismatch.
        let mut mismatched = inputs(left.clone(), right.clone());
        mismatched.second_schema = SchemaVersion::new(10);
        assert!(matches!(
            plan(mismatched),
            Err(MergeError::Rejected(MergeRejection::SchemaMismatch {
                table,
                first,
                second,
            })) if table == TableId::new(3)
                && first == SchemaVersion::new(9)
                && second == SchemaVersion::new(10)
        ));

        // Non-adjacent ranges (a gap), and overlapping ranges.
        let gapped = source(
            14,
            14,
            Bound::Included(text_key("n")),
            Bound::Excluded(text_key("z")),
            6,
        );
        assert!(matches!(
            plan(inputs(left.clone(), gapped)),
            Err(MergeError::Rejected(MergeRejection::NotAdjacent { .. }))
        ));
        let overlapping = source(
            14,
            14,
            Bound::Included(text_key("l")),
            Bound::Excluded(text_key("z")),
            6,
        );
        assert!(matches!(
            plan(inputs(left.clone(), overlapping)),
            Err(MergeError::Rejected(MergeRejection::NotAdjacent { .. }))
        ));

        // Incompatible placement: different replica node sets.
        let mut elsewhere = right.clone();
        elsewhere.replicas[0].node_id = node(9);
        elsewhere.leader_hint = Some(node(9));
        assert!(matches!(
            plan(inputs(left.clone(), elsewhere)),
            Err(MergeError::Rejected(
                MergeRejection::IncompatiblePlacement { .. }
            ))
        ));

        // A conflicting schema job.
        let mut with_job = inputs(left.clone(), right.clone());
        with_job.active_schema_job = Some(77);
        assert!(matches!(
            plan(with_job),
            Err(MergeError::Rejected(MergeRejection::ConflictingSchemaJob {
                table,
                job_id: 77,
            })) if table == TableId::new(3)
        ));

        // Combined size over the threshold (and overflow-safe).
        let mut too_big = inputs(left.clone(), right.clone());
        too_big.max_merged_size_bytes = 2_999;
        assert!(matches!(
            plan(too_big),
            Err(MergeError::Rejected(
                MergeRejection::CombinedSizeExceedsThreshold {
                    combined_bytes: 3_000,
                    threshold_bytes: 2_999,
                }
            ))
        ));
        let mut overflowing = inputs(left.clone(), right.clone());
        overflowing.first_size_bytes = u64::MAX;
        assert!(matches!(
            plan(overflowing),
            Err(MergeError::Rejected(
                MergeRejection::CombinedSizeExceedsThreshold { .. }
            ))
        ));

        // A source that is not Active.
        let mut splitting = left.clone();
        splitting.state = TabletState::Splitting;
        assert!(matches!(
            plan(inputs(splitting, right.clone())),
            Err(MergeError::Rejected(MergeRejection::InvalidSourceState {
                state: TabletState::Splitting,
                ..
            }))
        ));

        // Replacement ids colliding with a source fail closed.
        let mut colliding = replacement_allocation();
        colliding.tablet_id = left.tablet_id;
        assert!(matches!(
            planner.plan(inputs(left.clone(), right.clone()), ts(150), colliding),
            Err(MergeError::InvalidPlan(_))
        ));
    }

    #[test]
    fn merge_publish_command_flips_three_descriptors_at_one_generation() {
        let dir = tempfile::tempdir().unwrap();
        let planner = MergePlanner::new(dir.path());
        let (left, right) = source_pair();
        let plan = planner
            .plan(inputs(left, right), ts(150), replacement_allocation())
            .unwrap();
        // m = max(4, 6) = 6: replacement Creating at 7, publication at 8.
        assert_eq!(plan.replacement_descriptor().generation, 7);
        let command = MergePublishCommand::from_plan(&plan).unwrap();
        assert_eq!(command.publish_generation(), 8);
        assert_eq!(command.replacement.state, TabletState::Active);
        assert!(command
            .replacement
            .replicas
            .iter()
            .all(|replica| replica.role == ReplicaRole::Voter));
        for (source, original) in command.sources.iter().zip(plan.sources.iter()) {
            assert_eq!(source.state, TabletState::Retiring);
            assert_eq!(source.generation, 8, "source at g={}", original.generation);
        }
        // The shape survives serde (the meta wave journals it as one command).
        let bytes = serde_json::to_vec(&command).unwrap();
        let back: MergePublishCommand = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, command);

        // Tampered shapes fail validation.
        let mut wrong_state = command.clone();
        wrong_state.replacement.state = TabletState::Creating;
        assert!(wrong_state.validate().is_err());
        let mut skewed = command.clone();
        skewed.sources[0].generation = 9;
        assert!(skewed.validate().is_err());
        let mut wrong_bounds = command.clone();
        wrong_bounds.replacement.partition =
            PartitionBounds::new(Bound::Unbounded, Bound::Unbounded).unwrap();
        assert!(wrong_bounds.validate().is_err());
    }

    // -- the full merge ---------------------------------------------------------

    struct MergeFixture {
        _dir: tempfile::TempDir,
        sources: [TabletDescriptor; 2],
        source_layouts: [TabletLayout; 2],
        meta: InMemoryMetaPlane,
        keyspaces: [MapKeyspace; 2],
        sink: MapChildSink,
        plan: MergePlan,
    }

    const LOWER_KEYS: [&str; 6] = ["b", "d", "f", "h", "j", "l"];
    const UPPER_KEYS: [&str; 7] = ["m", "o", "q", "s", "u", "w", "y"];

    fn merge_fixture() -> MergeFixture {
        let dir = tempfile::tempdir().unwrap();
        let (left, right) = source_pair();
        let mut meta = InMemoryMetaPlane::new();
        meta.set_tablet(&left).unwrap();
        meta.set_tablet(&right).unwrap();

        let left_layout = TabletLayout::new(dir.path(), left.tablet_id, left.raft_group_id);
        left_layout.create(&left).unwrap();
        let right_layout = TabletLayout::new(dir.path(), right.tablet_id, right.raft_group_id);
        right_layout.create(&right).unwrap();

        let left_keyspace = MapKeyspace::new();
        for name in LOWER_KEYS {
            left_keyspace.insert(
                text_key(name),
                ts(100),
                format!("v-{name}@100").into_bytes(),
            );
        }
        left_keyspace.insert(text_key("d"), ts(200), b"v-d@200".to_vec());
        let right_keyspace = MapKeyspace::new();
        for name in UPPER_KEYS {
            right_keyspace.insert(
                text_key(name),
                ts(100),
                format!("v-{name}@100").into_bytes(),
            );
        }
        right_keyspace.insert(text_key("x"), ts(200), b"v-x@200".to_vec());

        let planner = MergePlanner::new(dir.path());
        let plan = planner
            .plan(
                inputs(left.clone(), right.clone()),
                ts(150),
                replacement_allocation(),
            )
            .unwrap();
        MergeFixture {
            _dir: dir,
            sources: [left, right],
            source_layouts: [left_layout, right_layout],
            meta,
            keyspaces: [left_keyspace, right_keyspace],
            sink: MapChildSink::new(),
            plan,
        }
    }

    type TestExecutor = MergeExecutor<InMemoryMetaPlane, MapKeyspace, MapChildSink>;

    fn begin_executor(fixture: &MergeFixture) -> TestExecutor {
        MergeExecutor::begin(
            fixture.plan.clone(),
            fixture.source_layouts.clone(),
            fixture.meta.clone(),
            fixture.keyspaces.clone(),
            fixture.sink.clone(),
        )
        .unwrap()
    }

    fn assert_merge_completed(fixture: &MergeFixture) {
        // Both source descriptors are removed; the replacement is Active at
        // the publication generation with promoted (voter) replicas.
        for source in &fixture.sources {
            assert!(fixture.meta.tablet(source.tablet_id).is_none());
        }
        let replacement = fixture.meta.tablet(tablet_id(13)).unwrap();
        assert_eq!(replacement.state, TabletState::Active);
        assert_eq!(replacement.generation, 8);
        assert!(replacement
            .replicas
            .iter()
            .all(|replica| replica.role == ReplicaRole::Voter));
        assert_eq!(replacement.partition, fixture.plan.replacement.bounds);
        assert_eq!(
            fixture.plan.replacement.layout.load_metadata().unwrap(),
            replacement
        );
        // Both source replicas are torn down and the progress record is gone.
        for layout in &fixture.source_layouts {
            assert!(!layout.tablet_dir().exists());
            assert!(!layout.group_dir().exists());
        }
        assert!(!fixture.source_layouts[0]
            .tablet_dir()
            .join(MERGE_PROGRESS_FILENAME)
            .exists());
        // Zero loss, zero duplication: the replacement holds both keyspaces.
        let rows = fixture.sink.rows();
        let mut expected = fixture.keyspaces[0].rows_at(ts(u64::MAX));
        expected.extend(fixture.keyspaces[1].rows_at(ts(u64::MAX)));
        assert_eq!(rows, expected);
    }

    #[test]
    fn full_merge_replaces_adjacent_tablets_with_zero_loss() {
        let _lock = EXECUTOR_TEST_LOCK.lock().unwrap();
        let fixture = merge_fixture();
        let table = TableId::new(3);
        let mut executor = begin_executor(&fixture);
        assert_eq!(executor.phase(), MergePhase::Started);

        // Both sources are marked Merging (each at its own g+1) and keep serving.
        assert_eq!(executor.step().unwrap(), MergePhase::MarkedMerging);
        let marked_left = fixture.meta.tablet(tablet_id(11)).unwrap();
        let marked_right = fixture.meta.tablet(tablet_id(12)).unwrap();
        assert_eq!(marked_left.state, TabletState::Merging);
        assert_eq!(marked_left.generation, 5);
        assert_eq!(marked_right.state, TabletState::Merging);
        assert_eq!(marked_right.generation, 7);
        // A stale generation against a Merging source is plain stale metadata:
        // the source still owns its range until publication.
        let error = check_generation(&marked_left, 4).unwrap_err();
        assert!(matches!(error, RoutingError::StaleMetadata { .. }));
        let tablets = fixture.meta.descriptors();
        assert_eq!(
            find_tablet_for_key(&tablets, table, &text_key("b"))
                .unwrap()
                .tablet_id,
            tablet_id(11)
        );
        assert_eq!(
            find_tablet_for_key(&tablets, table, &text_key("y"))
                .unwrap()
                .tablet_id,
            tablet_id(12)
        );

        // The hidden replacement exists as Creating learners — never routable.
        assert_eq!(executor.step().unwrap(), MergePhase::ReplacementCreated);
        let creating = fixture.meta.tablet(tablet_id(13)).unwrap();
        assert_eq!(creating.state, TabletState::Creating);
        assert_eq!(creating.generation, 7);
        assert!(creating
            .replicas
            .iter()
            .all(|replica| replica.role == ReplicaRole::Learner));
        let tablets = fixture.meta.descriptors();
        assert_eq!(
            find_tablet_for_key(&tablets, table, &text_key("b"))
                .unwrap()
                .tablet_id,
            tablet_id(11),
            "hidden replacement exposed before catch-up"
        );

        // Both snapshots pinned; the build unions them; deltas catch up.
        assert_eq!(executor.step().unwrap(), MergePhase::SnapshotsPinned);
        assert_eq!(fixture.keyspaces[0].pin_count(), 1);
        assert_eq!(fixture.keyspaces[1].pin_count(), 1);
        assert_eq!(executor.step().unwrap(), MergePhase::ReplacementBuilt);
        assert_eq!(
            fixture.sink.rows().len(),
            LOWER_KEYS.len() + UPPER_KEYS.len()
        );
        assert_eq!(
            fixture.sink.rows().get(&text_key("d")),
            Some(&b"v-d@100".to_vec()),
            "post-merge write leaked into the pinned snapshot"
        );
        assert_eq!(executor.step().unwrap(), MergePhase::CaughtUp);
        assert_eq!(
            fixture.sink.rows().get(&text_key("d")),
            Some(&b"v-d@200".to_vec())
        );
        assert_eq!(
            fixture.sink.rows().get(&text_key("x")),
            Some(&b"v-x@200".to_vec())
        );

        // The atomic publication flips routing to the replacement.
        assert_eq!(executor.step().unwrap(), MergePhase::Published);
        assert_eq!(fixture.keyspaces[0].pin_count(), 0);
        assert_eq!(fixture.keyspaces[1].pin_count(), 0);
        for id in [tablet_id(11), tablet_id(12)] {
            let retiring = fixture.meta.tablet(id).unwrap();
            assert_eq!(retiring.state, TabletState::Retiring);
            assert_eq!(retiring.generation, 8);
        }
        let tablets = fixture.meta.descriptors();
        for name in ["b", "m", "y"] {
            assert_eq!(
                find_tablet_for_key(&tablets, table, &text_key(name))
                    .unwrap()
                    .tablet_id,
                tablet_id(13),
                "key {name} did not reroute to the replacement"
            );
        }
        let overlapping = tablets_overlapping(&tablets, table, &PartitionBounds::unbounded());
        assert_eq!(
            overlapping
                .iter()
                .map(|tablet| tablet.tablet_id)
                .collect::<Vec<_>>(),
            vec![tablet_id(13)]
        );
        // Stale requests against the retired sources reroute.
        let retiring = fixture.meta.tablet(tablet_id(11)).unwrap();
        let error = check_generation(&retiring, 4).unwrap_err();
        assert!(matches!(error, RoutingError::TabletMoved { .. }));
        assert!(matches!(
            retry_guidance(&error),
            RetryGuidance::RefreshAndReroute { .. }
        ));
        assert!(check_generation(&fixture.meta.tablet(tablet_id(13)).unwrap(), 8).is_ok());

        // Retention gates removal of both sources until old pins drain.
        let pin_left = executor.retention().unwrap()[0].pin(4);
        assert!(matches!(
            executor.step(),
            Err(MergeError::SourceRetained { pins: 1, .. })
        ));
        assert_eq!(executor.phase(), MergePhase::Published);
        assert!(executor.retention().unwrap()[0].unpin(pin_left));
        let pin_right = executor.retention().unwrap()[1].pin(6);
        assert!(matches!(
            executor.step(),
            Err(MergeError::SourceRetained { pins: 1, .. })
        ));
        assert!(executor.retention().unwrap()[1].unpin(pin_right));
        assert_eq!(executor.step().unwrap(), MergePhase::SourcesRetired);
        assert_merge_completed(&fixture);
        executor.run().unwrap();
        assert!(MergeExecutor::resume(
            fixture.source_layouts.clone(),
            fixture.meta.clone(),
            fixture.keyspaces.clone(),
            fixture.sink.clone(),
        )
        .unwrap()
        .is_none());
    }

    // -- crash-resume at every durable boundary --------------------------------

    #[test]
    fn merge_resumes_after_a_crash_at_every_durable_boundary() {
        let _lock = EXECUTOR_TEST_LOCK.lock().unwrap();
        let hooks = [
            "tablet.merge.phase.1",
            "tablet.merge.phase.2",
            "tablet.merge.phase.3",
            "tablet.merge.phase.4",
            "tablet.merge.phase.5",
            "tablet.merge.phase.6",
            "tablet.merge.phase.7",
            "tablet.merge.before",
            "tablet.merge.after",
        ];
        for hook in hooks {
            let fixture = merge_fixture();
            let mut executor = begin_executor(&fixture);
            {
                let _guard =
                    mongreldb_fault::ScopedGuard::limited(hook, mongreldb_fault::Action::Fail, 1);
                assert!(
                    matches!(executor.run(), Err(MergeError::Fault(_))),
                    "hook {hook} did not fire"
                );
            }
            drop(executor);
            let resumed = MergeExecutor::resume(
                fixture.source_layouts.clone(),
                fixture.meta.clone(),
                fixture.keyspaces.clone(),
                fixture.sink.clone(),
            )
            .unwrap();
            if hook == "tablet.merge.phase.7" {
                assert!(resumed.is_none(), "hook {hook}");
            } else {
                resumed
                    .unwrap()
                    .run()
                    .unwrap_or_else(|error| panic!("resume after {hook} failed: {error}"));
            }
            assert_merge_completed(&fixture);
        }
    }

    #[test]
    fn merge_build_fails_closed_on_out_of_range_keys() {
        let _lock = EXECUTOR_TEST_LOCK.lock().unwrap();
        // A key outside its source partition fails the build closed, in
        // either source (adjacent ranges are disjoint, so a foreign key can
        // never land silently in the union).
        for index in 0..2 {
            let fixture = merge_fixture();
            fixture.keyspaces[index].insert(text_key("zz-outside"), ts(120), b"rogue".to_vec());
            let mut executor = begin_executor(&fixture);
            assert!(
                matches!(
                    executor.run_until(MergePhase::ReplacementBuilt),
                    Err(MergeError::KeyOutsideSource(_))
                ),
                "source {index} accepted an out-of-range key"
            );
        }
    }

    #[test]
    fn merge_progress_record_fails_closed_on_corruption() {
        let _lock = EXECUTOR_TEST_LOCK.lock().unwrap();
        let fixture = merge_fixture();
        let executor = begin_executor(&fixture);
        drop(executor);
        let path = fixture.source_layouts[0]
            .tablet_dir()
            .join(MERGE_PROGRESS_FILENAME);
        std::fs::write(&path, b"{ not json").unwrap();
        assert!(matches!(
            MergeExecutor::resume(
                fixture.source_layouts.clone(),
                fixture.meta.clone(),
                fixture.keyspaces.clone(),
                fixture.sink.clone(),
            ),
            Err(MergeError::Tablet(TabletError::Metadata(
                ClusterError::CorruptMetadata { .. }
            )))
        ));
        // The upper source's directory never holds the record.
        assert!(MergeExecutor::resume(
            [
                fixture.source_layouts[1].clone(),
                fixture.source_layouts[1].clone(),
            ],
            fixture.meta.clone(),
            fixture.keyspaces.clone(),
            fixture.sink.clone(),
        )
        .unwrap()
        .is_none());
    }
}
