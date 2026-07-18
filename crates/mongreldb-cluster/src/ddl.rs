//! Distributed DDL and online index jobs (spec section 12.11, Stage 3K).
//!
//! One replicated [`DdlJobRecord`] drives one schema element through the
//! spec's DDL phases — `Pending -> WriteOnly -> Backfilling -> Validating ->
//! Public` for additions, `Public -> Dropping` for drops — with the
//! transition graph enforced at apply time in the replicated [`DdlJobStore`].
//! The new-index protocol maps the spec's steps onto explicit collaborators:
//!
//! ```text
//!  1. Replicate definition as WriteOnly      (DdlCommand::SubmitJob + AdvancePhase)
//!  2. New writes maintain the hidden index   (ApplySideIndexMaintainer seam)
//!  3. Backfill every tablet at pinned snapshot (per-tablet backfill drivers,
//!     resumable through DdlJobRecord::tablet_progress)
//!  4. Catch up deltas                        (committed writes from the pinned
//!     snapshot forward, swept until drained)
//!  5. Validate counts/checksums              (TabletValidationReport per
//!     tablet, JobValidationReport aggregate)
//!  6. Publish Public atomically              (DdlCommand::PublishJob — ONE
//!     store command; planner-visible from that metadata version)
//!  7. Old generation reclaimed later         (DdlCommand::ReclaimIndex, gated
//!     on no readers below the retirement metadata version)
//! ```
//!
//! # Reconciliation notes
//!
//! - The job's administrative lifecycle reuses [`crate::meta::SchemaJobState`]
//!   verbatim (submitted `Pending`, `Running/Paused/Cancelling/RollingBack`
//!   driven, terminal `Succeeded/Failed`; the transition graph is the core job
//!   registry's, mirrored by `crate::meta`). The schema-element protocol phase
//!   ([`DdlPhase`]) is the second, orthogonal axis the spec's section 12.11
//!   declares.
//! - [`DdlJobRecord`] is the DDL-native superset of
//!   [`crate::meta::SchemaJobRecord`]: it adds the phase axis, the replicated
//!   [`DdlDefinition`], the pinned `source_schema_version`, and the per-tablet
//!   progress map. [`DdlJobKind::AddIndex`] corresponds to
//!   `SchemaJobKind::IndexBuild`, [`DdlJobKind::AlterSchema`] to
//!   `ColumnBackfill`/`SchemaValidation`; `DropIndex` has no core mirror.
//! - [`DdlJobStore`] is the deterministic, serde-versioned state-machine
//!   section the meta group replicates for distributed DDL (same apply idiom
//!   as [`crate::meta::MetaState`]: the metadata version ticks once per
//!   applied command, refusals are journaled, never faulted). This wave keeps
//!   it a sibling of `MetaState` — `crate::meta`'s format v2 command enum is
//!   frozen by the parallel Stage 3 waves — and the integration wave binds
//!   [`DdlCommand`] onto meta-group proposals. The table anchors
//!   ([`TableAnchor`]) shadow `MetaState::tables`' last-writer-wins
//!   `schema_version` semantics so stale-schema enforcement is deterministic
//!   inside this store.
//!
//! # Write-path contract (spec step 2)
//!
//! While an index record of a table sits in `WriteOnly..=Public`, the tablet
//! apply path dual-maintains it: every committed row mutation additionally
//! lands in the hidden (or public) index generation through
//! [`ApplySideIndexMaintainer`]. The handoff with the backfill driver is
//! timestamp-based: writes committed at or below a tablet's catch-up
//! watermark are covered by the delta sweep (they ride
//! [`BackfillKeyspace::deltas_after`]); writes above it are covered by the
//! maintainer. Both paths are idempotent set/remove operations, so a write
//! covered twice converges. A write that arrives before the hidden generation
//! is installed is skipped by the maintainer and covered by the sweep — the
//! pinned snapshot plus the delta stream always bound it. The engine hook
//! lands with the server wave; [`InMemoryTabletIndexes`] is the reference
//! implementation.
//!
//! # Crash safety and resume
//!
//! The job record in the replicated store is the only progress authority:
//! every per-tablet stage transition is an applied command before the driver
//! moves on. A driver crash (or a pause) leaves the record behind; a new
//! [`DdlDriver`] over the same store resumes, skips tablets already
//! `CaughtUp`/`Validated`, and restarts an interrupted tablet's build from
//! scratch — `HiddenIndexSink::begin_build` discards staged content, the same
//! idiom as the split executor's child-state sink. Resume granularity is one
//! tablet.
//!
//! # Schema versions and stale-schema retry
//!
//! Every job pins the `source_schema_version` it was submitted against
//! (spec section 12.11: the schema version rides every transaction and
//! plan). Submission and the atomic publish both check it against the table
//! anchor; a mismatch refuses with
//! [`DdlRejection::SchemaVersionMismatch`], which [`DdlError::category`]
//! maps onto [`ErrorCategory::SchemaVersionMismatch`] (retry class
//! `AfterMetadataRefresh`): the caller refreshes schema metadata and
//! resubmits.
//!
//! # Scope of this wave
//!
//! The engine binding (real index-key extraction, tablet-core snapshots and
//! committed-log delta streams) lands with the server wave, as does the
//! meta-group raft binding of [`DdlCommand`]. Split/merge topology changes
//! mid-job are reconciled by the driver re-enumerating the tablet set each
//! step ([`DdlCommand::ForgetTabletProgress`] drops departed tablets); an
//! index rebuild that retires a previous public generation is a follow-up —
//! this wave refuses duplicate index names, so the reclamation pass is
//! exercised through drops and cancellation.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mongreldb_types::errors::ErrorCategory;
use mongreldb_types::hlc::{ClockSkewError, HlcClock, HlcTimestamp};
use mongreldb_types::ids::{DatabaseId, MetadataVersion, SchemaVersion, TableId, TabletId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::meta::{MetaRejectionReason, SchemaJobState};
use crate::split::{RecordStream, SnapshotPin, TabletDataError};
use crate::tablet::Key;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Snapshot format version of [`DdlJobStore`]; bumped on any layout change
/// (spec section 4.10).
pub const DDL_STORE_FORMAT_VERSION: u32 = 1;
/// Oldest store format version this build accepts.
pub const MIN_SUPPORTED_DDL_STORE_FORMAT_VERSION: u32 = 1;
/// Bound on the journaled command refusals (mirrors
/// `crate::meta::META_REJECTION_LIMIT`).
pub const DDL_REJECTION_LIMIT: usize = 256;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Why a [`DdlCommand`] was refused at apply. Refusals are deterministic
/// (every replica reaches the same conclusion from the same state) and never
/// fault the state machine: they are journaled in [`DdlJobStore::rejections`]
/// and reported to the proposer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
pub enum DdlRejection {
    /// A generic guard reused from the meta control plane's taxonomy
    /// (`StaleWrite`, `Conflict`, `NotFound`, `Invalid`).
    #[error(transparent)]
    Meta(#[from] MetaRejectionReason),
    /// The requested phase edge does not exist in the [`DdlPhase`] graph.
    #[error("illegal DDL phase transition {from} -> {to} for job {job_id}")]
    IllegalPhaseTransition {
        /// The job whose phase was to move.
        job_id: u64,
        /// Current phase.
        from: DdlPhase,
        /// Refused target phase.
        to: DdlPhase,
    },
    /// The job is not `Running`, so the driver must not advance it.
    #[error("DDL job {job_id} is {state:?}, not Running")]
    JobNotRunning {
        /// The job.
        job_id: u64,
        /// Its current administrative state.
        state: SchemaJobState,
    },
    /// The job's pinned schema version no longer matches the table's current
    /// schema version (spec section 12.11: a stale schema returns structured
    /// retry). Maps onto [`ErrorCategory::SchemaVersionMismatch`].
    #[error(
        "schema version mismatch on table {table_id}: job pinned {expected}, \
         table is now {found}; refresh schema metadata and resubmit"
    )]
    SchemaVersionMismatch {
        /// The table.
        table_id: TableId,
        /// The schema version the job pinned at submission.
        expected: SchemaVersion,
        /// The table's current schema version.
        found: SchemaVersion,
    },
    /// A per-tablet progress update would move the durable cursor backwards.
    #[error("tablet {tablet} progress regressed for job {job_id}: {reason}")]
    TabletProgressRegression {
        /// The job.
        job_id: u64,
        /// The tablet.
        tablet: TabletId,
        /// Why the update was refused.
        reason: String,
    },
    /// Publish was attempted before every registered tablet validated.
    #[error("DDL job {job_id} cannot publish: tablets pending validation: {pending:?}")]
    ValidationIncomplete {
        /// The job.
        job_id: u64,
        /// Tablets not yet `Validated`.
        pending: Vec<TabletId>,
    },
    /// A reclamation was attempted while readers below the retirement version
    /// may still hold plans over the retired generation.
    #[error(
        "index `{index_name}` on table {table_id} cannot be reclaimed: oldest reader \
         pins metadata version {oldest_reader:?}, retirement requires {required}"
    )]
    ReclaimBlocked {
        /// The table.
        table_id: TableId,
        /// The index.
        index_name: String,
        /// Oldest live reader pin, when any.
        oldest_reader: Option<MetadataVersion>,
        /// The retirement version every reader must have reached.
        required: MetadataVersion,
    },
}

/// The driver-side failure surface: store refusals, seam errors, and job
/// outcomes the caller must observe (validation failure, cancellation).
#[derive(Debug, thiserror::Error)]
pub enum DdlError {
    /// The replicated store refused a command.
    #[error(transparent)]
    Rejection(#[from] DdlRejection),
    /// A tablet keyspace/sink seam operation failed.
    #[error(transparent)]
    TabletData(#[from] TabletDataError),
    /// The HLC clock refused timestamp allocation.
    #[error(transparent)]
    Clock(#[from] ClockSkewError),
    /// Counts/checksums of the built generation did not match a from-scratch
    /// build; the job was rolled back and the hidden generation dropped.
    #[error("DDL job {job_id} failed validation: {reason}")]
    ValidationFailed {
        /// The failed job.
        job_id: u64,
        /// Which tablet mismatched and how.
        reason: String,
    },
    /// The job was cancelled; the unwind already ran.
    #[error("DDL job {job_id} cancelled")]
    Cancelled {
        /// The cancelled job.
        job_id: u64,
    },
}

impl DdlError {
    /// The stable error taxonomy mapping (spec section 9.7). A stale schema
    /// maps onto [`ErrorCategory::SchemaVersionMismatch`] so the gateway
    /// issues a structured metadata-refresh retry.
    pub fn category(&self) -> ErrorCategory {
        match self {
            Self::Rejection(DdlRejection::SchemaVersionMismatch { .. }) => {
                ErrorCategory::SchemaVersionMismatch
            }
            Self::Rejection(DdlRejection::Meta(MetaRejectionReason::StaleWrite { .. })) => {
                ErrorCategory::StaleMetadata
            }
            Self::Cancelled { .. } => ErrorCategory::Cancelled,
            Self::Rejection(_)
            | Self::TabletData(_)
            | Self::Clock(_)
            | Self::ValidationFailed { .. } => ErrorCategory::ResourceExhausted,
        }
    }
}

// ---------------------------------------------------------------------------
// DDL phases (spec section 12.11, exact)
// ---------------------------------------------------------------------------

/// The replicated protocol phase of one schema element (spec section 12.11).
/// Serde encodes variants by name; names are part of the durable contract and
/// must never change or be reused (spec section 4.10).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DdlPhase {
    /// Definition submitted; not yet replicated for the write path.
    Pending,
    /// New writes maintain the hidden definition; nothing planner-visible.
    WriteOnly,
    /// Per-tablet backfill at the pinned snapshot plus delta catch-up.
    Backfilling,
    /// Counts/checksums validated per tablet before publication.
    Validating,
    /// Planner-visible from the publication metadata version.
    Public,
    /// Being dropped: writes stopped maintaining it, the planner stopped
    /// using it; the generation is reclaimed once readers drain.
    Dropping,
}

impl DdlPhase {
    /// Every phase in protocol order (spec section 12.11).
    pub const ALL: [DdlPhase; 6] = [
        DdlPhase::Pending,
        DdlPhase::WriteOnly,
        DdlPhase::Backfilling,
        DdlPhase::Validating,
        DdlPhase::Public,
        DdlPhase::Dropping,
    ];

    /// Whether the `self -> next` edge exists in the spec's graph. This is
    /// the single enforcement point: every store mutation checks it before
    /// applying a phase move. Cancellation and validation failure do not move
    /// phases — they unwind through the administrative state graph and remove
    /// the unpublished record instead.
    pub fn can_transition(self, next: Self) -> bool {
        use DdlPhase::{Backfilling, Dropping, Pending, Public, Validating, WriteOnly};
        matches!(
            (self, next),
            (Pending, WriteOnly)
                | (WriteOnly, Backfilling)
                | (Backfilling, Validating)
                | (Validating, Public)
                | (Public, Dropping)
        )
    }
}

impl fmt::Display for DdlPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Pending => "Pending",
            Self::WriteOnly => "WriteOnly",
            Self::Backfilling => "Backfilling",
            Self::Validating => "Validating",
            Self::Public => "Public",
            Self::Dropping => "Dropping",
        };
        f.write_str(name)
    }
}

// ---------------------------------------------------------------------------
// Job kinds and definitions
// ---------------------------------------------------------------------------

/// The Stage 3K DDL job kinds (spec section 12.11).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DdlJobKind {
    /// Online secondary-index build (the spec's worked example).
    AddIndex,
    /// Online index removal.
    DropIndex,
    /// Schema alteration (rides the same phase machine; publish advances the
    /// table's schema version).
    AlterSchema,
}

/// The replicated DDL definition of one job. The engine-opaque payloads ride
/// JSON documents, mirroring `crate::meta::TableSchemaRecord`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DdlDefinition {
    /// [`DdlJobKind::AddIndex`]: the new index's name and definition document.
    AddIndex {
        /// Unique index name within the table.
        index_name: String,
        /// Engine-opaque index definition (kind, columns, options).
        spec: serde_json::Value,
    },
    /// [`DdlJobKind::DropIndex`]: the index to remove.
    DropIndex {
        /// The index name; must exist and be `Public`.
        index_name: String,
    },
    /// [`DdlJobKind::AlterSchema`]: the replacement schema document.
    AlterSchema {
        /// Engine-opaque target schema; published at `source + 1`.
        target: serde_json::Value,
    },
}

impl DdlDefinition {
    /// The index name this definition carries, for the index kinds.
    pub fn index_name(&self) -> Option<&str> {
        match self {
            Self::AddIndex { index_name, .. } | Self::DropIndex { index_name } => Some(index_name),
            Self::AlterSchema { .. } => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Per-tablet progress (resumable)
// ---------------------------------------------------------------------------

/// One tablet's position inside a job's `Backfilling`/`Validating` phases.
/// Declaration order is the progress order; a later stage never moves back.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TabletDdlStage {
    /// Registered for the job; the build has not started (or was restarted).
    Pending,
    /// The build started; staged content may exist beside live state.
    Backfilling,
    /// Snapshot content installed and deltas swept through
    /// [`TabletDdlProgress::caught_up_through`].
    CaughtUp,
    /// Counts/checksums validated for this tablet.
    Validated,
}

impl fmt::Display for TabletDdlStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Pending => "Pending",
            Self::Backfilling => "Backfilling",
            Self::CaughtUp => "CaughtUp",
            Self::Validated => "Validated",
        };
        f.write_str(name)
    }
}

/// The durable per-tablet progress cursor of one job. A driver restart
/// resumes from this map alone.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TabletDdlProgress {
    /// The tablet.
    pub tablet_id: TabletId,
    /// Furthest stage reached.
    pub stage: TabletDdlStage,
    /// Rows scanned from the pinned snapshot (observability).
    pub rows_scanned: u64,
    /// Catch-up watermark: every committed write at or below this timestamp
    /// is reflected in the tablet's built generation.
    pub caught_up_through: Option<HlcTimestamp>,
    /// The tablet's validation report, once produced.
    pub validation: Option<TabletValidationReport>,
}

impl TabletDdlProgress {
    /// A freshly registered tablet.
    pub fn pending(tablet_id: TabletId) -> Self {
        Self {
            tablet_id,
            stage: TabletDdlStage::Pending,
            rows_scanned: 0,
            caught_up_through: None,
            validation: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Validation reports (spec step 5)
// ---------------------------------------------------------------------------

/// One tablet's typed validation report: the built generation's count and
/// checksum against a from-scratch build at the same watermark.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TabletValidationReport {
    /// The tablet.
    pub tablet_id: TabletId,
    /// The watermark both sides were compared at.
    pub watermark: HlcTimestamp,
    /// From-scratch row/entry count.
    pub expected_rows: u64,
    /// Built-generation entry count.
    pub actual_rows: u64,
    /// SHA-256 of the from-scratch entry set (sorted key||entry stream).
    pub expected_checksum: [u8; 32],
    /// SHA-256 of the built generation.
    pub actual_checksum: [u8; 32],
}

impl TabletValidationReport {
    /// The tablet passes when counts and checksums both match.
    pub fn passed(&self) -> bool {
        self.expected_rows == self.actual_rows && self.expected_checksum == self.actual_checksum
    }
}

/// The aggregate validation report of one job (spec step 5: per tablet plus
/// aggregate).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobValidationReport {
    /// The job.
    pub job_id: u64,
    /// Every tablet's report, in tablet-id order.
    pub tablets: Vec<TabletValidationReport>,
    /// Sum of expected rows.
    pub total_expected: u64,
    /// Sum of built rows.
    pub total_actual: u64,
    /// Every tablet passed.
    pub passed: bool,
}

/// The deterministic checksum over one built (or from-scratch) entry set:
/// SHA-256 over the sorted key||entry byte stream.
pub fn generation_checksum(entries: &BTreeMap<Key, Vec<u8>>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for (key, entry) in entries {
        hasher.update(key.as_bytes());
        hasher.update(entry);
    }
    hasher.finalize().into()
}

// ---------------------------------------------------------------------------
// Replicated records
// ---------------------------------------------------------------------------

/// One replicated DDL job record (spec section 12.11). Lives in
/// [`DdlJobStore::jobs`]; the per-tablet progress map is the resume
/// authority.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DdlJobRecord {
    /// Job identifier (allocated by the proposer; never reused).
    pub job_id: u64,
    /// Database owning the job's target.
    pub database_id: DatabaseId,
    /// Table the job operates on.
    pub table_id: TableId,
    /// Job kind.
    pub kind: DdlJobKind,
    /// Administrative lifecycle (`crate::meta::SchemaJobState`, reused).
    pub state: SchemaJobState,
    /// Schema-element protocol phase (spec section 12.11).
    pub phase: DdlPhase,
    /// The replicated definition.
    pub definition: DdlDefinition,
    /// The table schema version the job was submitted against; checked again
    /// at publish (spec section 12.11 "schema version is included in every
    /// transaction and plan").
    pub source_schema_version: SchemaVersion,
    /// Submission timestamp stamped by the proposer.
    pub created_at: HlcTimestamp,
    /// Timestamp of the last applied mutation.
    pub updated_at: HlcTimestamp,
    /// The job-wide backfill pin, stamped when entering `Backfilling`
    /// (spec step 3: every tablet backfills at this pinned snapshot).
    pub pinned_snapshot: Option<HlcTimestamp>,
    /// Per-tablet resumable progress.
    #[serde(with = "tablet_progress_map")]
    pub tablet_progress: BTreeMap<TabletId, TabletDdlProgress>,
    /// Failure/cancellation detail for terminal states.
    pub error: Option<String>,
    /// Store [`MetadataVersion`] of the last modification.
    #[serde(default = "zero_metadata_version")]
    pub metadata_version: MetadataVersion,
}

fn zero_metadata_version() -> MetadataVersion {
    MetadataVersion::ZERO
}

/// One replicated index record: the schema element a job drives, outliving
/// the job itself. Planner visibility is a pure function of this record and a
/// metadata version (spec step 6).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DdlIndexRecord {
    /// Table owning the index.
    pub table_id: TableId,
    /// Index name (unique within the table).
    pub index_name: String,
    /// Engine-opaque index definition document.
    pub definition: serde_json::Value,
    /// Element protocol phase (mirrors the owning job's phase while the job
    /// runs; `Public`/`Dropping` persist after it terminates).
    pub phase: DdlPhase,
    /// The job that last drove the record.
    pub job_id: u64,
    /// Creation timestamp.
    pub created_at: HlcTimestamp,
    /// The metadata version the index became planner-visible at
    /// (`planner_visible_at` includes the record at or above this version).
    pub publication_version: Option<MetadataVersion>,
    /// Publication timestamp.
    pub published_at: Option<HlcTimestamp>,
    /// The metadata version the index entered `Dropping` at (planners at or
    /// above it stop using the index; reclamation waits on reader pins below
    /// it).
    pub dropping_since: Option<MetadataVersion>,
    /// Store [`MetadataVersion`] of the last modification.
    #[serde(default = "zero_metadata_version")]
    pub metadata_version: MetadataVersion,
}

/// The replicated table anchor this module's stale-schema checks run against.
/// Shadows `crate::meta::MetaState::tables` (same last-writer-wins
/// `schema_version` semantics); the integration wave feeds it from
/// `MetaCommand::SetTableSchema` applies.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableAnchor {
    /// The table.
    pub table_id: TableId,
    /// Database owning the table.
    pub database_id: DatabaseId,
    /// Current schema version (never reused, never lowered).
    pub schema_version: SchemaVersion,
    /// Current schema document (opaque to the meta group).
    pub schema: serde_json::Value,
    /// Store [`MetadataVersion`] of the last modification.
    #[serde(default = "zero_metadata_version")]
    pub metadata_version: MetadataVersion,
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// One replicated DDL control-plane command (spec section 12.11). Every
/// variant is deterministic and idempotent at apply: records are versioned
/// and guards reject stale or conflicting writes with a typed
/// [`DdlRejection`] instead of faulting the state machine (the
/// `crate::meta::MetaCommand` idiom).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DdlCommand {
    /// Registers (or advances) a table anchor. Last-writer-wins by
    /// `schema_version`, mirroring `MetaCommand::SetTableSchema`.
    RegisterTable {
        /// The anchor to apply.
        anchor: TableAnchor,
    },
    /// Submits a job (starts `SchemaJobState::Pending`; `DdlPhase::Pending`,
    /// or `DdlPhase::Public` for a drop, whose element already is public).
    SubmitJob {
        /// The job record.
        job: DdlJobRecord,
    },
    /// Advances one job along the [`DdlPhase`] graph. `Public` for an
    /// addition is never reached here — publication rides [`Self::PublishJob`]
    /// so validation completeness and the schema-version recheck gate the one
    /// atomic command. `pinned_snapshot` is required for `Backfilling`.
    AdvancePhase {
        /// The job.
        job_id: u64,
        /// Target phase.
        to: DdlPhase,
        /// The job-wide backfill pin (required when `to` is `Backfilling`).
        pinned_snapshot: Option<HlcTimestamp>,
        /// Optimistic-concurrency token over the job record.
        expected_version: Option<MetadataVersion>,
    },
    /// Registers or advances one tablet's durable progress cursor. Updates
    /// are monotonic: the stage never moves backwards and `rows_scanned`
    /// never shrinks within one stage.
    UpdateTabletProgress {
        /// The job.
        job_id: u64,
        /// The new cursor.
        progress: TabletDdlProgress,
    },
    /// Drops the progress cursor of a tablet that left the table's topology
    /// (split/merge reconciliation; only while the job still drives tablets).
    ForgetTabletProgress {
        /// The job.
        job_id: u64,
        /// The departed tablet.
        tablet_id: TabletId,
    },
    /// Records one tablet's validation report; a passing report moves the
    /// tablet to `Validated`.
    ReportTabletValidation {
        /// The job.
        job_id: u64,
        /// The report.
        report: TabletValidationReport,
    },
    /// The atomic publication (spec step 6): validates completeness and the
    /// pinned schema version, flips the element to `Public` stamped with this
    /// command's metadata version, and succeeds the job — one command.
    PublishJob {
        /// The job.
        job_id: u64,
        /// Publication timestamp stamped by the proposer.
        published_at: HlcTimestamp,
    },
    /// Moves the administrative lifecycle (`pause`/`resume`/`cancel`/
    /// `admit`/`succeed`/`fail`), graph-enforced.
    SetJobState {
        /// The job.
        job_id: u64,
        /// Target state.
        state: SchemaJobState,
        /// Update timestamp stamped by the proposer.
        updated_at: HlcTimestamp,
        /// Failure/cancellation detail.
        error: Option<String>,
        /// Optimistic-concurrency token over the job record.
        expected_version: Option<MetadataVersion>,
    },
    /// Unwinds an unpublished index record (cancel/validation-failure
    /// rollback). Refuses to remove a `Public` record — fail closed.
    RemoveIndexRecord {
        /// The job owning the record.
        job_id: u64,
    },
    /// The reclamation pass (spec step 7): removes a `Dropping` index record
    /// once no live reader pins a metadata version below its retirement
    /// version.
    ReclaimIndex {
        /// The table.
        table_id: TableId,
        /// The index.
        index_name: String,
    },
    /// Pins a reader at a metadata version (its plans may reference every
    /// element visible there). A reader's pin only moves forward.
    PinReader {
        /// Reader identifier.
        reader_id: u64,
        /// The pinned metadata version.
        version: MetadataVersion,
    },
    /// Releases one reader pin.
    ReleaseReader {
        /// Reader identifier.
        reader_id: u64,
    },
}

// ---------------------------------------------------------------------------
// The replicated store
// ---------------------------------------------------------------------------

/// One journaled refusal: the refused command's id and the typed reason.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DdlStoreRejection {
    /// Leader-assigned id of the refused command (`None` when it carried
    /// none; always `Some` through the raft binding).
    pub command_id: Option<[u8; 16]>,
    /// Why it was refused.
    pub reason: DdlRejection,
}

/// The replicated DDL control-plane state (spec section 12.11): the section
/// of meta state the DDL jobs, index records, table anchors, and reader pins
/// live in. Deterministic — every replica applies the same commands in the
/// same order and reaches byte-identical state; versioned — `metadata_version`
/// ticks once per applied command (accepted or refused — a refusal appends to
/// the rejection journal, which is itself state), giving readers and the
/// reclamation gate a monotonic watermark.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct DdlJobStore {
    /// Snapshot format version; see [`DDL_STORE_FORMAT_VERSION`].
    pub format_version: u32,
    /// Monotonic per-applied-command version.
    pub metadata_version: MetadataVersion,
    /// Next job id to allocate (strictly greater than every live id).
    pub next_job_id: u64,
    /// Table anchors (stale-schema authority of this store).
    pub tables: BTreeMap<TableId, TableAnchor>,
    /// DDL jobs by id.
    pub jobs: BTreeMap<u64, DdlJobRecord>,
    /// Index records by (table, name).
    #[serde(with = "index_record_map")]
    pub indexes: BTreeMap<(TableId, String), DdlIndexRecord>,
    /// Live reader pins (reader id -> pinned metadata version).
    pub reader_pins: BTreeMap<u64, MetadataVersion>,
    /// Bounded journal of refused commands, oldest first
    /// ([`DDL_REJECTION_LIMIT`]).
    pub rejections: VecDeque<DdlStoreRejection>,
}

impl Default for DdlJobStore {
    fn default() -> Self {
        Self {
            format_version: DDL_STORE_FORMAT_VERSION,
            metadata_version: MetadataVersion::ZERO,
            next_job_id: 1,
            tables: BTreeMap::new(),
            jobs: BTreeMap::new(),
            indexes: BTreeMap::new(),
            reader_pins: BTreeMap::new(),
            rejections: VecDeque::new(),
        }
    }
}

/// JSON-safe serde for the index-record map: `(TableId, String)` tuple keys
/// are not JSON object keys, so the map is encoded as a sequence of
/// `(table, name, record)` triples (and decoded back). Deterministic in
/// `BTreeMap` order.
mod index_record_map {
    use super::{DdlIndexRecord, TableId};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;

    pub fn serialize<S>(
        map: &BTreeMap<(TableId, String), DdlIndexRecord>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let triples: Vec<(TableId, String, DdlIndexRecord)> = map
            .iter()
            .map(|((table, name), record)| (*table, name.clone(), record.clone()))
            .collect();
        triples.serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<(TableId, String), DdlIndexRecord>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let triples: Vec<(TableId, String, DdlIndexRecord)> = Vec::deserialize(deserializer)?;
        Ok(triples
            .into_iter()
            .map(|(table, name, record)| ((table, name), record))
            .collect())
    }
}

/// JSON-safe serde for the per-tablet progress map: `TabletId` is a 16-byte
/// newtype, not a JSON object key, so the map is encoded as a sequence of
/// `(tablet, progress)` pairs (and decoded back). Deterministic in
/// `BTreeMap` order.
mod tablet_progress_map {
    use super::{TabletDdlProgress, TabletId};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;

    pub fn serialize<S>(
        map: &BTreeMap<TabletId, TabletDdlProgress>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let pairs: Vec<(TabletId, TabletDdlProgress)> = map
            .iter()
            .map(|(tablet, progress)| (*tablet, progress.clone()))
            .collect();
        pairs.serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<TabletId, TabletDdlProgress>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let pairs: Vec<(TabletId, TabletDdlProgress)> = Vec::deserialize(deserializer)?;
        Ok(pairs.into_iter().collect())
    }
}

impl DdlJobStore {
    /// Applies one committed command. `metadata_version` ticks first (every
    /// applied command moves the watermark); a refusal journals the reason
    /// and leaves the records untouched (the `crate::meta::MetaState::apply`
    /// idiom).
    pub fn apply(
        &mut self,
        command: &DdlCommand,
        command_id: Option<[u8; 16]>,
        commit_ts: HlcTimestamp,
    ) -> Result<(), DdlRejection> {
        self.metadata_version = MetadataVersion(self.metadata_version.get() + 1);
        let version = self.metadata_version;
        let result = self.dispatch(command, commit_ts, version);
        if let Err(reason) = &result {
            self.rejections.push_back(DdlStoreRejection {
                command_id,
                reason: reason.clone(),
            });
            while self.rejections.len() > DDL_REJECTION_LIMIT {
                self.rejections.pop_front();
            }
        }
        result
    }

    fn dispatch(
        &mut self,
        command: &DdlCommand,
        commit_ts: HlcTimestamp,
        version: MetadataVersion,
    ) -> Result<(), DdlRejection> {
        match command {
            DdlCommand::RegisterTable { anchor } => self.apply_register_table(anchor, version),
            DdlCommand::SubmitJob { job } => self.apply_submit_job(job, commit_ts, version),
            DdlCommand::AdvancePhase {
                job_id,
                to,
                pinned_snapshot,
                expected_version,
            } => self.apply_advance_phase(
                *job_id,
                *to,
                *pinned_snapshot,
                *expected_version,
                commit_ts,
                version,
            ),
            DdlCommand::UpdateTabletProgress { job_id, progress } => {
                self.apply_update_tablet_progress(*job_id, progress, commit_ts, version)
            }
            DdlCommand::ForgetTabletProgress { job_id, tablet_id } => {
                self.apply_forget_tablet_progress(*job_id, *tablet_id, commit_ts, version)
            }
            DdlCommand::ReportTabletValidation { job_id, report } => {
                self.apply_report_tablet_validation(*job_id, report, commit_ts, version)
            }
            DdlCommand::PublishJob {
                job_id,
                published_at,
            } => self.apply_publish_job(*job_id, *published_at, commit_ts, version),
            DdlCommand::SetJobState {
                job_id,
                state,
                updated_at,
                error,
                expected_version,
            } => self.apply_set_job_state(
                *job_id,
                *state,
                *updated_at,
                error,
                *expected_version,
                version,
            ),
            DdlCommand::RemoveIndexRecord { job_id } => {
                self.apply_remove_index_record(*job_id, version)
            }
            DdlCommand::ReclaimIndex {
                table_id,
                index_name,
            } => self.apply_reclaim_index(*table_id, index_name, version),
            DdlCommand::PinReader { reader_id, version } => {
                self.apply_pin_reader(*reader_id, *version)
            }
            DdlCommand::ReleaseReader { reader_id } => {
                self.reader_pins.remove(reader_id);
                Ok(())
            }
        }
    }

    fn apply_register_table(
        &mut self,
        anchor: &TableAnchor,
        version: MetadataVersion,
    ) -> Result<(), DdlRejection> {
        if anchor.table_id == TableId::ZERO {
            return Err(MetaRejectionReason::Invalid {
                reason: "reserved zero table id".to_owned(),
            }
            .into());
        }
        if anchor.schema_version == SchemaVersion::ZERO {
            return Err(MetaRejectionReason::Invalid {
                reason: "reserved zero schema version".to_owned(),
            }
            .into());
        }
        match self.tables.get(&anchor.table_id) {
            Some(existing) => {
                if anchor.schema_version > existing.schema_version {
                    let mut anchor = anchor.clone();
                    anchor.metadata_version = version;
                    self.tables.insert(anchor.table_id, anchor);
                    Ok(())
                } else if anchor.schema_version == existing.schema_version {
                    if existing.database_id == anchor.database_id
                        && existing.schema == anchor.schema
                    {
                        Ok(())
                    } else {
                        Err(MetaRejectionReason::Conflict {
                            resource: format!("table {}", anchor.table_id),
                            reason: "schema version already used for different content".to_owned(),
                        }
                        .into())
                    }
                } else {
                    Err(MetaRejectionReason::StaleWrite {
                        resource: format!("table {}", anchor.table_id),
                        current: MetadataVersion(existing.schema_version.get()),
                        attempted: MetadataVersion(anchor.schema_version.get()),
                    }
                    .into())
                }
            }
            None => {
                let mut anchor = anchor.clone();
                anchor.metadata_version = version;
                self.tables.insert(anchor.table_id, anchor);
                Ok(())
            }
        }
    }

    fn apply_submit_job(
        &mut self,
        job: &DdlJobRecord,
        commit_ts: HlcTimestamp,
        version: MetadataVersion,
    ) -> Result<(), DdlRejection> {
        if job.job_id == 0 {
            return Err(MetaRejectionReason::Invalid {
                reason: "reserved zero job id".to_owned(),
            }
            .into());
        }
        // Idempotent replay: an identical record under an existing id is a
        // no-op; a different one conflicts (the `SubmitSchemaJob` idiom).
        if let Some(existing) = self.jobs.get(&job.job_id) {
            let mut comparable = existing.clone();
            comparable.metadata_version = job.metadata_version;
            return if comparable == *job {
                Ok(())
            } else {
                Err(MetaRejectionReason::Conflict {
                    resource: format!("DDL job {}", job.job_id),
                    reason: "job id already exists with different content".to_owned(),
                }
                .into())
            };
        }
        let anchor = self
            .tables
            .get(&job.table_id)
            .ok_or(MetaRejectionReason::NotFound {
                resource: format!("table {}", job.table_id),
            })?;
        if anchor.database_id != job.database_id {
            return Err(MetaRejectionReason::Conflict {
                resource: format!("table {}", job.table_id),
                reason: "table belongs to a different database".to_owned(),
            }
            .into());
        }
        // Stale schema at submission: structured retry (spec section 12.11).
        if anchor.schema_version != job.source_schema_version {
            return Err(DdlRejection::SchemaVersionMismatch {
                table_id: job.table_id,
                expected: job.source_schema_version,
                found: anchor.schema_version,
            });
        }
        if job.state != SchemaJobState::Pending {
            return Err(MetaRejectionReason::Invalid {
                reason: "submitted jobs start Pending".to_owned(),
            }
            .into());
        }
        let expected_phase = match job.kind {
            DdlJobKind::AddIndex | DdlJobKind::AlterSchema => DdlPhase::Pending,
            DdlJobKind::DropIndex => DdlPhase::Public,
        };
        if job.phase != expected_phase {
            return Err(MetaRejectionReason::Invalid {
                reason: format!(
                    "{:?} jobs start in phase {expected_phase}, not {}",
                    job.kind, job.phase
                ),
            }
            .into());
        }
        // One non-terminal DDL job per table (serialization keeps the
        // per-tablet progress maps deterministic; relaxing this is a
        // documented follow-up).
        if let Some(active) = self
            .jobs
            .values()
            .find(|existing| existing.table_id == job.table_id && !existing.state.is_terminal())
        {
            return Err(MetaRejectionReason::Conflict {
                resource: format!("table {}", job.table_id),
                reason: format!("table already has active DDL job {}", active.job_id),
            }
            .into());
        }
        match &job.definition {
            DdlDefinition::AddIndex { index_name, spec } => {
                if index_name.trim().is_empty() {
                    return Err(MetaRejectionReason::Invalid {
                        reason: "index name is empty".to_owned(),
                    }
                    .into());
                }
                let key = (job.table_id, index_name.clone());
                if self.indexes.contains_key(&key) {
                    return Err(MetaRejectionReason::Conflict {
                        resource: format!("index `{index_name}` on table {}", job.table_id),
                        reason: "index name already exists".to_owned(),
                    }
                    .into());
                }
                self.indexes.insert(
                    key,
                    DdlIndexRecord {
                        table_id: job.table_id,
                        index_name: index_name.clone(),
                        definition: spec.clone(),
                        phase: DdlPhase::Pending,
                        job_id: job.job_id,
                        created_at: commit_ts,
                        publication_version: None,
                        published_at: None,
                        dropping_since: None,
                        metadata_version: version,
                    },
                );
            }
            DdlDefinition::DropIndex { index_name } => {
                let key = (job.table_id, index_name.clone());
                match self.indexes.get(&key) {
                    None => {
                        return Err(MetaRejectionReason::NotFound {
                            resource: format!("index `{index_name}` on table {}", job.table_id),
                        }
                        .into());
                    }
                    Some(record) if record.phase != DdlPhase::Public => {
                        return Err(MetaRejectionReason::Conflict {
                            resource: format!("index `{index_name}` on table {}", job.table_id),
                            reason: format!("index is {}, not Public", record.phase),
                        }
                        .into());
                    }
                    Some(_) => {}
                }
            }
            DdlDefinition::AlterSchema { .. } => {}
        }
        let mut job = job.clone();
        job.metadata_version = version;
        job.updated_at = commit_ts;
        self.next_job_id = self.next_job_id.max(job.job_id + 1);
        self.jobs.insert(job.job_id, job);
        Ok(())
    }

    fn apply_advance_phase(
        &mut self,
        job_id: u64,
        to: DdlPhase,
        pinned_snapshot: Option<HlcTimestamp>,
        expected_version: Option<MetadataVersion>,
        commit_ts: HlcTimestamp,
        version: MetadataVersion,
    ) -> Result<(), DdlRejection> {
        let job = self
            .jobs
            .get(&job_id)
            .ok_or(MetaRejectionReason::NotFound {
                resource: format!("DDL job {job_id}"),
            })?;
        if let Some(expected) = expected_version {
            if expected != job.metadata_version {
                return Err(MetaRejectionReason::StaleWrite {
                    resource: format!("DDL job {job_id}"),
                    current: job.metadata_version,
                    attempted: expected,
                }
                .into());
            }
        }
        if job.state.is_terminal() {
            return Err(MetaRejectionReason::Conflict {
                resource: format!("DDL job {job_id}"),
                reason: format!("terminal state {:?}", job.state),
            }
            .into());
        }
        if job.state != SchemaJobState::Running {
            return Err(DdlRejection::JobNotRunning {
                job_id,
                state: job.state,
            });
        }
        // Idempotent replay of an already-applied advance.
        if job.phase == to {
            if to == DdlPhase::Backfilling && job.pinned_snapshot != pinned_snapshot {
                return Err(MetaRejectionReason::Conflict {
                    resource: format!("DDL job {job_id}"),
                    reason: "phase already reached with a different pinned snapshot".to_owned(),
                }
                .into());
            }
            return Ok(());
        }
        if !job.phase.can_transition(to) {
            return Err(DdlRejection::IllegalPhaseTransition {
                job_id,
                from: job.phase,
                to,
            });
        }
        // Addition kinds publish only through PublishJob, so the atomic
        // command carries the completeness and schema-version gates.
        if to == DdlPhase::Public && job.kind != DdlJobKind::DropIndex {
            return Err(MetaRejectionReason::Invalid {
                reason: "Public is reached through PublishJob, never AdvancePhase".to_owned(),
            }
            .into());
        }
        if to == DdlPhase::Backfilling && pinned_snapshot.is_none() {
            return Err(MetaRejectionReason::Invalid {
                reason: "Backfilling requires the job-wide pinned snapshot".to_owned(),
            }
            .into());
        }
        let job = self.jobs.get_mut(&job_id).expect("job existence checked");
        job.phase = to;
        if to == DdlPhase::Backfilling {
            job.pinned_snapshot = pinned_snapshot;
        }
        job.updated_at = commit_ts;
        job.metadata_version = version;
        // Index records mirror the element phase while a job drives them.
        if let Some(index_name) = job.definition.index_name() {
            let key = (job.table_id, index_name.to_owned());
            if let Some(record) = self.indexes.get_mut(&key) {
                record.phase = to;
                if to == DdlPhase::Dropping {
                    record.dropping_since = Some(version);
                }
                record.metadata_version = version;
            }
        }
        Ok(())
    }

    fn apply_update_tablet_progress(
        &mut self,
        job_id: u64,
        progress: &TabletDdlProgress,
        commit_ts: HlcTimestamp,
        version: MetadataVersion,
    ) -> Result<(), DdlRejection> {
        let job = self
            .jobs
            .get_mut(&job_id)
            .ok_or(MetaRejectionReason::NotFound {
                resource: format!("DDL job {job_id}"),
            })?;
        if job.phase != DdlPhase::Backfilling {
            // Replay tolerance: once the job moved on, only dominated
            // (already-covered) updates remain legal.
            let dominated = job
                .tablet_progress
                .get(&progress.tablet_id)
                .is_some_and(|existing| {
                    existing.stage >= progress.stage
                        && existing.rows_scanned >= progress.rows_scanned
                });
            if dominated {
                return Ok(());
            }
            return Err(MetaRejectionReason::Conflict {
                resource: format!("DDL job {job_id}"),
                reason: format!("job is {}, not Backfilling", job.phase),
            }
            .into());
        }
        if let Some(existing) = job.tablet_progress.get(&progress.tablet_id) {
            if progress.stage < existing.stage {
                return Err(DdlRejection::TabletProgressRegression {
                    job_id,
                    tablet: progress.tablet_id,
                    reason: format!("stage {} -> {}", existing.stage, progress.stage),
                });
            }
            if progress.stage == existing.stage && progress.rows_scanned < existing.rows_scanned {
                return Err(DdlRejection::TabletProgressRegression {
                    job_id,
                    tablet: progress.tablet_id,
                    reason: format!(
                        "rows_scanned {} -> {}",
                        existing.rows_scanned, progress.rows_scanned
                    ),
                });
            }
        }
        job.tablet_progress
            .insert(progress.tablet_id, progress.clone());
        job.updated_at = commit_ts;
        job.metadata_version = version;
        Ok(())
    }

    fn apply_forget_tablet_progress(
        &mut self,
        job_id: u64,
        tablet_id: TabletId,
        commit_ts: HlcTimestamp,
        version: MetadataVersion,
    ) -> Result<(), DdlRejection> {
        let job = self
            .jobs
            .get_mut(&job_id)
            .ok_or(MetaRejectionReason::NotFound {
                resource: format!("DDL job {job_id}"),
            })?;
        if !matches!(job.phase, DdlPhase::Backfilling | DdlPhase::Validating) {
            return Err(MetaRejectionReason::Conflict {
                resource: format!("DDL job {job_id}"),
                reason: format!(
                    "job is {}; tablet cursors are only mutable while driving",
                    job.phase
                ),
            }
            .into());
        }
        if job.tablet_progress.remove(&tablet_id).is_some() {
            job.updated_at = commit_ts;
            job.metadata_version = version;
        }
        Ok(())
    }

    fn apply_report_tablet_validation(
        &mut self,
        job_id: u64,
        report: &TabletValidationReport,
        commit_ts: HlcTimestamp,
        version: MetadataVersion,
    ) -> Result<(), DdlRejection> {
        let job = self
            .jobs
            .get_mut(&job_id)
            .ok_or(MetaRejectionReason::NotFound {
                resource: format!("DDL job {job_id}"),
            })?;
        if job.phase != DdlPhase::Validating {
            return Err(MetaRejectionReason::Conflict {
                resource: format!("DDL job {job_id}"),
                reason: format!("job is {}, not Validating", job.phase),
            }
            .into());
        }
        let progress = job.tablet_progress.get_mut(&report.tablet_id).ok_or(
            MetaRejectionReason::NotFound {
                resource: format!("tablet {} progress of job {job_id}", report.tablet_id),
            },
        )?;
        if progress.stage < TabletDdlStage::CaughtUp {
            return Err(DdlRejection::TabletProgressRegression {
                job_id,
                tablet: report.tablet_id,
                reason: "validation reported before catch-up completed".to_owned(),
            });
        }
        if progress.stage == TabletDdlStage::Validated
            && progress.validation.as_ref() == Some(report)
        {
            return Ok(());
        }
        progress.validation = Some(report.clone());
        progress.caught_up_through = Some(report.watermark);
        if report.passed() {
            progress.stage = TabletDdlStage::Validated;
        }
        job.updated_at = commit_ts;
        job.metadata_version = version;
        Ok(())
    }

    fn apply_publish_job(
        &mut self,
        job_id: u64,
        published_at: HlcTimestamp,
        commit_ts: HlcTimestamp,
        version: MetadataVersion,
    ) -> Result<(), DdlRejection> {
        let job = self
            .jobs
            .get(&job_id)
            .ok_or(MetaRejectionReason::NotFound {
                resource: format!("DDL job {job_id}"),
            })?;
        if job.state == SchemaJobState::Succeeded
            && job.phase == DdlPhase::Public
            && job.kind != DdlJobKind::DropIndex
        {
            return Ok(()); // idempotent replay of the publication
        }
        if job.kind == DdlJobKind::DropIndex {
            return Err(MetaRejectionReason::Invalid {
                reason: "drop jobs ride AdvancePhase(Public -> Dropping), never PublishJob"
                    .to_owned(),
            }
            .into());
        }
        if job.state != SchemaJobState::Running {
            return Err(DdlRejection::JobNotRunning {
                job_id,
                state: job.state,
            });
        }
        if job.phase != DdlPhase::Validating {
            return Err(DdlRejection::IllegalPhaseTransition {
                job_id,
                from: job.phase,
                to: DdlPhase::Public,
            });
        }
        // Stale-schema recheck (spec section 12.11: a stale schema returns
        // structured retry): the table must not have moved since submission.
        let anchor = self
            .tables
            .get(&job.table_id)
            .ok_or(MetaRejectionReason::NotFound {
                resource: format!("table {}", job.table_id),
            })?;
        if anchor.schema_version != job.source_schema_version {
            return Err(DdlRejection::SchemaVersionMismatch {
                table_id: job.table_id,
                expected: job.source_schema_version,
                found: anchor.schema_version,
            });
        }
        let pending: Vec<TabletId> = job
            .tablet_progress
            .values()
            .filter(|progress| {
                progress.stage != TabletDdlStage::Validated
                    || progress
                        .validation
                        .as_ref()
                        .is_none_or(|report| !report.passed())
            })
            .map(|progress| progress.tablet_id)
            .collect();
        if job.tablet_progress.is_empty() || !pending.is_empty() {
            return Err(DdlRejection::ValidationIncomplete { job_id, pending });
        }
        match &job.definition {
            DdlDefinition::AddIndex { index_name, .. } => {
                let key = (job.table_id, index_name.clone());
                let record = self
                    .indexes
                    .get_mut(&key)
                    .ok_or(MetaRejectionReason::NotFound {
                        resource: format!("index `{index_name}` on table {}", job.table_id),
                    })?;
                record.phase = DdlPhase::Public;
                record.publication_version = Some(version);
                record.published_at = Some(published_at);
                record.metadata_version = version;
            }
            DdlDefinition::AlterSchema { target } => {
                let anchor = self
                    .tables
                    .get_mut(&job.table_id)
                    .expect("anchor existence checked");
                anchor.schema_version = SchemaVersion(job.source_schema_version.get() + 1);
                anchor.schema = target.clone();
                anchor.metadata_version = version;
            }
            DdlDefinition::DropIndex { .. } => unreachable!("drop kind refused above"),
        }
        let job = self.jobs.get_mut(&job_id).expect("job existence checked");
        job.phase = DdlPhase::Public;
        job.state = SchemaJobState::Succeeded;
        job.updated_at = commit_ts;
        job.metadata_version = version;
        Ok(())
    }

    fn apply_set_job_state(
        &mut self,
        job_id: u64,
        state: SchemaJobState,
        updated_at: HlcTimestamp,
        error: &Option<String>,
        expected_version: Option<MetadataVersion>,
        version: MetadataVersion,
    ) -> Result<(), DdlRejection> {
        let record = self
            .jobs
            .get_mut(&job_id)
            .ok_or(MetaRejectionReason::NotFound {
                resource: format!("DDL job {job_id}"),
            })?;
        if let Some(expected) = expected_version {
            if expected != record.metadata_version {
                return Err(MetaRejectionReason::StaleWrite {
                    resource: format!("DDL job {job_id}"),
                    current: record.metadata_version,
                    attempted: expected,
                }
                .into());
            }
        }
        if record.state.is_terminal() {
            return Err(MetaRejectionReason::Conflict {
                resource: format!("DDL job {job_id}"),
                reason: format!("terminal state {:?}", record.state),
            }
            .into());
        }
        if record.state != state && !record.state.can_transition(state) {
            return Err(MetaRejectionReason::Conflict {
                resource: format!("DDL job {job_id}"),
                reason: format!("illegal transition {:?} -> {:?}", record.state, state),
            }
            .into());
        }
        if record.state == state && record.error == *error {
            return Ok(());
        }
        record.state = state;
        record.updated_at = updated_at;
        record.error = error.clone();
        record.metadata_version = version;
        Ok(())
    }

    fn apply_remove_index_record(
        &mut self,
        job_id: u64,
        version: MetadataVersion,
    ) -> Result<(), DdlRejection> {
        let job = self
            .jobs
            .get(&job_id)
            .ok_or(MetaRejectionReason::NotFound {
                resource: format!("DDL job {job_id}"),
            })?;
        if !matches!(
            job.state,
            SchemaJobState::RollingBack | SchemaJobState::Failed
        ) {
            return Err(MetaRejectionReason::Conflict {
                resource: format!("DDL job {job_id}"),
                reason: "index records are removed only during rollback".to_owned(),
            }
            .into());
        }
        let Some(index_name) = job.definition.index_name() else {
            return Ok(());
        };
        let key = (job.table_id, index_name.to_owned());
        match self.indexes.get(&key) {
            None => Ok(()),
            Some(record) if record.phase == DdlPhase::Public => {
                Err(MetaRejectionReason::Conflict {
                    resource: format!("index `{index_name}` on table {}", job.table_id),
                    reason: "refusing to remove a published index".to_owned(),
                }
                .into())
            }
            Some(_) => {
                self.indexes.remove(&key);
                let job = self.jobs.get_mut(&job_id).expect("job existence checked");
                job.metadata_version = version;
                Ok(())
            }
        }
    }

    fn apply_reclaim_index(
        &mut self,
        table_id: TableId,
        index_name: &str,
        _version: MetadataVersion,
    ) -> Result<(), DdlRejection> {
        let key = (table_id, index_name.to_owned());
        let Some(record) = self.indexes.get(&key) else {
            return Ok(()); // already reclaimed (idempotent)
        };
        if record.phase != DdlPhase::Dropping {
            return Err(MetaRejectionReason::Conflict {
                resource: format!("index `{index_name}` on table {table_id}"),
                reason: format!("index is {}, not Dropping", record.phase),
            }
            .into());
        }
        let required = record
            .dropping_since
            .expect("Dropping records carry dropping_since");
        let oldest_reader = self.oldest_reader_version();
        if oldest_reader.is_some_and(|oldest| oldest < required) {
            return Err(DdlRejection::ReclaimBlocked {
                table_id,
                index_name: index_name.to_owned(),
                oldest_reader,
                required,
            });
        }
        self.indexes.remove(&key);
        Ok(())
    }

    fn apply_pin_reader(
        &mut self,
        reader_id: u64,
        version: MetadataVersion,
    ) -> Result<(), DdlRejection> {
        if reader_id == 0 {
            return Err(MetaRejectionReason::Invalid {
                reason: "reserved zero reader id".to_owned(),
            }
            .into());
        }
        if let Some(existing) = self.reader_pins.get(&reader_id) {
            if version < *existing {
                return Err(MetaRejectionReason::Conflict {
                    resource: format!("reader {reader_id}"),
                    reason: format!("reader pins only move forward ({} -> {version})", *existing),
                }
                .into());
            }
        }
        self.reader_pins.insert(reader_id, version);
        Ok(())
    }

    // -- Queries ------------------------------------------------------------

    /// One job record.
    pub fn job(&self, job_id: u64) -> Option<&DdlJobRecord> {
        self.jobs.get(&job_id)
    }

    /// One index record.
    pub fn index(&self, table_id: TableId, index_name: &str) -> Option<&DdlIndexRecord> {
        self.indexes.get(&(table_id, index_name.to_owned()))
    }

    /// One table anchor.
    pub fn table_anchor(&self, table_id: TableId) -> Option<&TableAnchor> {
        self.tables.get(&table_id)
    }

    /// The oldest live reader pin, the reclamation gate's watermark.
    pub fn oldest_reader_version(&self) -> Option<MetadataVersion> {
        self.reader_pins.values().copied().min()
    }

    /// The indexes a planner at metadata version `version` may use (spec
    /// step 6): published at or below `version`, and not already `Dropping`
    /// at `version`. The atomic publication flips visibility at exactly one
    /// metadata version.
    pub fn planner_visible_at(
        &self,
        table_id: TableId,
        version: MetadataVersion,
    ) -> Vec<DdlIndexRecord> {
        self.indexes
            .values()
            .filter(|record| {
                record.table_id == table_id
                    && record
                        .publication_version
                        .is_some_and(|publication| publication <= version)
                    && record
                        .dropping_since
                        .is_none_or(|dropping| version < dropping)
            })
            .cloned()
            .collect()
    }

    /// The indexes the planner of the current metadata version may use.
    pub fn planner_visible(&self, table_id: TableId) -> Vec<DdlIndexRecord> {
        self.planner_visible_at(table_id, self.metadata_version)
    }

    /// The index definitions a tablet apply path dual-maintains for
    /// committed writes (spec step 2): every record in `WriteOnly`,
    /// `Backfilling`, `Validating`, or `Public` — hidden from planners but
    /// maintained from `WriteOnly` on. Deterministic name order.
    pub fn write_maintained(&self, table_id: TableId) -> Vec<DdlIndexRecord> {
        self.indexes
            .values()
            .filter(|record| {
                record.table_id == table_id
                    && matches!(
                        record.phase,
                        DdlPhase::WriteOnly
                            | DdlPhase::Backfilling
                            | DdlPhase::Validating
                            | DdlPhase::Public
                    )
            })
            .cloned()
            .collect()
    }
}

// ---------------------------------------------------------------------------
// The tablet seams (spec steps 2-5)
// ---------------------------------------------------------------------------

/// The boxed stream of timestamped committed mutations
/// ([`BackfillKeyspace::deltas_after`]). Unlike the split executor's delta
/// stream, the DDL catch-up needs commit timestamps to define its watermark:
/// `None` values are deletes (tombstones).
pub type DeltaStream<'a> = Box<dyn Iterator<Item = (HlcTimestamp, Key, Option<Vec<u8>>)> + 'a>;

/// The applied keyspace of one tablet replica, as the DDL backfill driver
/// reads it (spec steps 3-5). The engine binding lands with the server wave:
/// the snapshot is the engine's MVCC snapshot mechanics and the deltas are
/// the committed-log stream from the pinned snapshot forward. All methods
/// take `&self`; implementors synchronize internally (the in-memory
/// reference does), so one provider hands out independent handles.
pub trait BackfillKeyspace {
    /// Pins the snapshot at `ts` (spec step 3). Idempotent: re-pinning the
    /// same timestamp returns an equivalent pin; dropping releases it.
    fn pin_snapshot(&self, ts: HlcTimestamp) -> Result<Box<dyn SnapshotPin>, TabletDataError>;

    /// The keyspace contents visible at `ts`, in key order (tombstoned keys
    /// absent).
    fn snapshot_at(&self, ts: HlcTimestamp) -> Result<RecordStream<'_>, TabletDataError>;

    /// Mutations committed after `ts`, oldest first (spec step 4's catch-up
    /// stream). Multiple versions of one key arrive in commit order; a `None`
    /// value is a delete.
    fn deltas_after(&self, ts: HlcTimestamp) -> Result<DeltaStream<'_>, TabletDataError>;
}

/// The hidden-index build sink of one tablet (spec steps 3-4), staged like
/// the engine's snapshot-install idiom: a build is staged beside live state
/// (`begin_build`/`stage_entry`) and installed atomically
/// (`install_staged`), after which catch-up deltas and dual-maintained writes
/// apply (`apply_delta`). Restarting a build discards the staged content, so
/// a resumed tablet build is idempotent.
pub trait HiddenIndexSink {
    /// Starts (or restarts) a staged build of `index`, discarding prior
    /// staged content for it.
    fn begin_build(&mut self, index: &str) -> Result<(), TabletDataError>;

    /// Adds one snapshot entry to the staged build.
    fn stage_entry(&mut self, index: &str, key: &Key, entry: &[u8]) -> Result<(), TabletDataError>;

    /// Atomically installs the staged build as the tablet's hidden
    /// generation of `index`.
    fn install_staged(&mut self, index: &str) -> Result<(), TabletDataError>;

    /// Applies one committed mutation to the installed generation (spec
    /// steps 2 and 4): `Some(entry)` sets, `None` removes. Idempotent —
    /// catch-up sweeps and the apply-path maintainer may both cover one
    /// write.
    fn apply_delta(
        &mut self,
        index: &str,
        key: &Key,
        entry: Option<&[u8]>,
    ) -> Result<(), TabletDataError>;

    /// Drops the generation (and any staged build) of `index`. Idempotent:
    /// rollback and reclamation call this on every tablet of the table.
    fn drop_generation(&mut self, index: &str) -> Result<(), TabletDataError>;

    /// The installed generation's entries (validation and tests).
    fn generation_entries(&self, index: &str) -> Result<BTreeMap<Key, Vec<u8>>, TabletDataError>;

    /// The installed generation's entry count.
    fn entry_count(&self, index: &str) -> Result<u64, TabletDataError> {
        Ok(self.generation_entries(index)?.len() as u64)
    }
}

/// The tablet apply path's dual-maintenance seam (spec step 2). While an
/// index record of the tablet's table sits in `WriteOnly..=Public`, every
/// committed row mutation additionally lands in the hidden (or public) index
/// generation. The engine binding hooks this into the tablet's apply loop;
/// it lands with the server wave.
pub trait ApplySideIndexMaintainer {
    /// Refreshes the maintainer's view: the names of every index the table
    /// currently maintains (`DdlJobStore::write_maintained`). The apply path
    /// calls this whenever its applied metadata view changes.
    fn sync_definitions(&mut self, maintained: Vec<String>) -> Result<(), TabletDataError>;

    /// Dual-maintains one committed row mutation (`None` = delete) into
    /// every installed generation of the synced definitions. A mutation
    /// arriving before its generation is installed is skipped here and
    /// covered by the catch-up sweep instead (the documented handoff).
    fn apply_committed_write(
        &mut self,
        key: &Key,
        row: Option<&[u8]>,
    ) -> Result<(), TabletDataError>;
}

/// The tablet topology and seam registry the [`DdlDriver`] drives. Handles
/// are independent (interior synchronization), so the driver can hold a
/// keyspace and a sink of one tablet at once.
pub trait DdlTabletProvider {
    /// The table's current tablets, in tablet-id order (deterministic).
    fn tablets_of(&self, table: TableId) -> Vec<TabletId>;

    /// The backfill read seam of one tablet.
    fn keyspace(&self, tablet: TabletId) -> Result<Box<dyn BackfillKeyspace>, TabletDataError>;

    /// The hidden-index build seam of one tablet.
    fn sink(&self, tablet: TabletId) -> Result<Box<dyn HiddenIndexSink>, TabletDataError>;

    /// The apply-path dual-maintenance seam of one tablet.
    fn maintainer(
        &self,
        tablet: TabletId,
    ) -> Result<Box<dyn ApplySideIndexMaintainer>, TabletDataError>;
}

/// Derives one index entry from one row (the engine binding supplies real
/// index-key extraction; tests ride [`identity_projection`]).
pub type IndexProjection = Arc<dyn Fn(&DdlIndexRecord, &Key, &[u8]) -> Vec<u8> + Send + Sync>;

/// The default projection: the entry is the row's value bytes (the hidden
/// index is a full row copy). Both the backfill path and the from-scratch
/// comparison use it, so validation compares the two paths' convergence.
pub fn identity_projection() -> IndexProjection {
    Arc::new(|_index, _key, value| value.to_vec())
}

// ---------------------------------------------------------------------------
// In-memory reference implementations
// ---------------------------------------------------------------------------

/// The in-memory version chain behind [`InMemoryDdlKeyspace`]: per key, the
/// committed versions (timestamp, put-or-tombstone) in ascending order.
type VersionedRows = BTreeMap<Key, Vec<(HlcTimestamp, Option<Vec<u8>>)>>;

/// The reference [`BackfillKeyspace`]: a shared in-memory multi-version map
/// with tombstones. Versions are kept per key so `snapshot_at` /
/// `deltas_after` split the timeline exactly at a timestamp; clones share
/// state, so a "crashed" driver's writes survive into the resumed one.
#[derive(Clone, Default)]
pub struct InMemoryDdlKeyspace {
    state: Arc<Mutex<VersionedRows>>,
}

impl InMemoryDdlKeyspace {
    /// An empty keyspace.
    pub fn new() -> Self {
        Self::default()
    }

    /// Commits one put of `key` at `ts`.
    pub fn insert(&self, key: Key, ts: HlcTimestamp, value: Vec<u8>) {
        let mut rows = self.state.lock().expect("keyspace lock poisoned");
        let chain = rows.entry(key).or_default();
        chain.push((ts, Some(value)));
        chain.sort_by_key(|(version, _)| *version);
    }

    /// Commits one delete of `key` at `ts`.
    pub fn delete(&self, key: Key, ts: HlcTimestamp) {
        let mut rows = self.state.lock().expect("keyspace lock poisoned");
        let chain = rows.entry(key).or_default();
        chain.push((ts, None));
        chain.sort_by_key(|(version, _)| *version);
    }

    /// Every key's newest live value at or below `ts` (assertion helper).
    pub fn rows_at(&self, ts: HlcTimestamp) -> BTreeMap<Key, Vec<u8>> {
        let rows = self.state.lock().expect("keyspace lock poisoned");
        rows.iter()
            .filter_map(|(key, chain)| {
                let (_, value) = chain.iter().rfind(|(version, _)| *version <= ts)?;
                value.clone().map(|value| (key.clone(), value))
            })
            .collect()
    }
}

impl BackfillKeyspace for InMemoryDdlKeyspace {
    fn pin_snapshot(&self, ts: HlcTimestamp) -> Result<Box<dyn SnapshotPin>, TabletDataError> {
        Ok(Box::new(DdlSnapshotPin { ts }))
    }

    fn snapshot_at(&self, ts: HlcTimestamp) -> Result<RecordStream<'_>, TabletDataError> {
        Ok(Box::new(self.rows_at(ts).into_iter()))
    }

    fn deltas_after(&self, ts: HlcTimestamp) -> Result<DeltaStream<'_>, TabletDataError> {
        let rows = self.state.lock().expect("keyspace lock poisoned");
        let mut deltas: Vec<(HlcTimestamp, Key, Option<Vec<u8>>)> = Vec::new();
        for (key, chain) in rows.iter() {
            for (version, value) in chain {
                if *version > ts {
                    deltas.push((*version, key.clone(), value.clone()));
                }
            }
        }
        deltas.sort_by(|(left_ts, left_key, _), (right_ts, right_key, _)| {
            (left_ts, left_key).cmp(&(right_ts, right_key))
        });
        Ok(Box::new(deltas.into_iter()))
    }
}

/// A [`InMemoryDdlKeyspace`] snapshot pin. The in-memory keyspace keeps its
/// whole version chain, so the pin is a pure timestamp record (a real engine
/// releases the pinned read generation on drop).
struct DdlSnapshotPin {
    ts: HlcTimestamp,
}

impl SnapshotPin for DdlSnapshotPin {
    fn pinned_at(&self) -> HlcTimestamp {
        self.ts
    }
}

/// The reference [`HiddenIndexSink`] + [`ApplySideIndexMaintainer`]: per
/// tablet, staged builds beside installed generations, with the apply-path
/// dual maintenance writing into installed generations only. Clones share
/// state (the same idiom as `MapChildSink`).
#[derive(Clone, Default)]
pub struct InMemoryTabletIndexes {
    state: Arc<Mutex<IndexesState>>,
}

#[derive(Default)]
struct IndexesState {
    /// Synced maintained definition names (`sync_definitions`).
    maintained: Vec<String>,
    /// Staged builds by index name (beside live state).
    staged: BTreeMap<String, BTreeMap<Key, Vec<u8>>>,
    /// Installed generations by index name.
    generations: BTreeMap<String, BTreeMap<Key, Vec<u8>>>,
    /// Total `begin_build` calls (restart observability for tests).
    begin_builds: usize,
}

impl InMemoryTabletIndexes {
    /// An empty tablet index set.
    pub fn new() -> Self {
        Self::default()
    }

    /// The installed entries of one generation, empty when absent (assertion
    /// helper).
    pub fn entries(&self, index: &str) -> BTreeMap<Key, Vec<u8>> {
        self.state
            .lock()
            .expect("indexes lock poisoned")
            .generations
            .get(index)
            .cloned()
            .unwrap_or_default()
    }

    /// How often a build was (re)started (assertion helper for resume
    /// tests: an already-finished tablet is never rebuilt).
    pub fn begin_build_count(&self) -> usize {
        self.state
            .lock()
            .expect("indexes lock poisoned")
            .begin_builds
    }

    /// Whether `index` has an installed generation.
    pub fn is_installed(&self, index: &str) -> bool {
        self.state
            .lock()
            .expect("indexes lock poisoned")
            .generations
            .contains_key(index)
    }

    /// The currently synced maintained definition names.
    pub fn maintained(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("indexes lock poisoned")
            .maintained
            .clone()
    }

    /// Seeds staged content without installing it (simulates a driver crash
    /// mid-build; the resumed driver's `begin_build` must discard it).
    #[cfg(test)]
    pub fn seed_staged(&self, index: &str, key: Key, entry: Vec<u8>) {
        self.state
            .lock()
            .expect("indexes lock poisoned")
            .staged
            .entry(index.to_owned())
            .or_default()
            .insert(key, entry);
    }
}

impl HiddenIndexSink for InMemoryTabletIndexes {
    fn begin_build(&mut self, index: &str) -> Result<(), TabletDataError> {
        let mut state = self.state.lock().expect("indexes lock poisoned");
        state.staged.insert(index.to_owned(), BTreeMap::new());
        state.begin_builds += 1;
        Ok(())
    }

    fn stage_entry(&mut self, index: &str, key: &Key, entry: &[u8]) -> Result<(), TabletDataError> {
        let mut state = self.state.lock().expect("indexes lock poisoned");
        let Some(staged) = state.staged.get_mut(index) else {
            return Err(TabletDataError::NoStagedBuild);
        };
        staged.insert(key.clone(), entry.to_vec());
        Ok(())
    }

    fn install_staged(&mut self, index: &str) -> Result<(), TabletDataError> {
        let mut state = self.state.lock().expect("indexes lock poisoned");
        let Some(staged) = state.staged.remove(index) else {
            return Err(TabletDataError::NoStagedBuild);
        };
        state.generations.insert(index.to_owned(), staged);
        Ok(())
    }

    fn apply_delta(
        &mut self,
        index: &str,
        key: &Key,
        entry: Option<&[u8]>,
    ) -> Result<(), TabletDataError> {
        let mut state = self.state.lock().expect("indexes lock poisoned");
        let Some(generation) = state.generations.get_mut(index) else {
            return Err(TabletDataError::Sink(format!(
                "generation `{index}` is not installed"
            )));
        };
        match entry {
            Some(entry) => {
                generation.insert(key.clone(), entry.to_vec());
            }
            None => {
                generation.remove(key);
            }
        }
        Ok(())
    }

    fn drop_generation(&mut self, index: &str) -> Result<(), TabletDataError> {
        let mut state = self.state.lock().expect("indexes lock poisoned");
        state.generations.remove(index);
        state.staged.remove(index);
        Ok(())
    }

    fn generation_entries(&self, index: &str) -> Result<BTreeMap<Key, Vec<u8>>, TabletDataError> {
        Ok(self.entries(index))
    }
}

impl ApplySideIndexMaintainer for InMemoryTabletIndexes {
    fn sync_definitions(&mut self, maintained: Vec<String>) -> Result<(), TabletDataError> {
        self.state.lock().expect("indexes lock poisoned").maintained = maintained;
        Ok(())
    }

    fn apply_committed_write(
        &mut self,
        key: &Key,
        row: Option<&[u8]>,
    ) -> Result<(), TabletDataError> {
        let mut state = self.state.lock().expect("indexes lock poisoned");
        for index in state.maintained.clone() {
            // Writes before the generation install are covered by the
            // catch-up sweep (the documented handoff), never lost.
            if let Some(generation) = state.generations.get_mut(&index) {
                match row {
                    Some(row) => {
                        generation.insert(key.clone(), row.to_vec());
                    }
                    None => {
                        generation.remove(key);
                    }
                }
            }
        }
        Ok(())
    }
}

/// One in-memory tablet: its keyspace plus its hidden-index state.
struct InMemoryTablet {
    table_id: TableId,
    keyspace: InMemoryDdlKeyspace,
    indexes: InMemoryTabletIndexes,
}

/// The reference [`DdlTabletProvider`]: a shared in-memory tablet set.
/// `commit_write` models the tablet apply path (spec step 2): the mutation
/// lands in the applied keyspace AND flows through the dual-maintenance seam
/// in one call.
#[derive(Clone, Default)]
pub struct InMemoryDdlTablets {
    tablets: Arc<Mutex<BTreeMap<TabletId, InMemoryTablet>>>,
}

impl InMemoryDdlTablets {
    /// An empty tablet set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one tablet of `table`.
    pub fn add_tablet(&self, tablet: TabletId, table: TableId) {
        self.tablets
            .lock()
            .expect("tablets lock poisoned")
            .entry(tablet)
            .or_insert_with(|| InMemoryTablet {
                table_id: table,
                keyspace: InMemoryDdlKeyspace::new(),
                indexes: InMemoryTabletIndexes::new(),
            });
    }

    /// Removes one tablet (simulates a split/merge topology change; the
    /// driver forgets its progress cursor on the next step).
    #[cfg(test)]
    pub fn remove_tablet(&self, tablet: TabletId) {
        self.tablets
            .lock()
            .expect("tablets lock poisoned")
            .remove(&tablet);
    }

    /// The keyspace handle of one tablet (test seeding).
    pub fn keyspace_handle(&self, tablet: TabletId) -> InMemoryDdlKeyspace {
        self.tablets
            .lock()
            .expect("tablets lock poisoned")
            .get(&tablet)
            .expect("unknown tablet")
            .keyspace
            .clone()
    }

    /// The index-state handle of one tablet (test assertions).
    pub fn indexes_handle(&self, tablet: TabletId) -> InMemoryTabletIndexes {
        self.tablets
            .lock()
            .expect("tablets lock poisoned")
            .get(&tablet)
            .expect("unknown tablet")
            .indexes
            .clone()
    }

    /// Models one committed write through the tablet apply path: the row
    /// mutation lands in the applied keyspace and is dual-maintained into
    /// every installed hidden/public generation (spec step 2). `None` deletes.
    pub fn commit_write(
        &self,
        tablet: TabletId,
        key: Key,
        ts: HlcTimestamp,
        row: Option<Vec<u8>>,
    ) -> Result<(), TabletDataError> {
        let (keyspace, mut indexes) = {
            let tablets = self.tablets.lock().expect("tablets lock poisoned");
            let tablet = tablets.get(&tablet).expect("unknown tablet");
            (tablet.keyspace.clone(), tablet.indexes.clone())
        };
        match row {
            Some(row) => {
                keyspace.insert(key.clone(), ts, row.clone());
                indexes.apply_committed_write(&key, Some(&row))
            }
            None => {
                keyspace.delete(key.clone(), ts);
                indexes.apply_committed_write(&key, None)
            }
        }
    }
}

impl DdlTabletProvider for InMemoryDdlTablets {
    fn tablets_of(&self, table: TableId) -> Vec<TabletId> {
        self.tablets
            .lock()
            .expect("tablets lock poisoned")
            .iter()
            .filter(|(_, tablet)| tablet.table_id == table)
            .map(|(tablet_id, _)| *tablet_id)
            .collect()
    }

    fn keyspace(&self, tablet: TabletId) -> Result<Box<dyn BackfillKeyspace>, TabletDataError> {
        let tablets = self.tablets.lock().expect("tablets lock poisoned");
        let Some(tablet) = tablets.get(&tablet) else {
            return Err(TabletDataError::Keyspace(format!(
                "unknown tablet {tablet}"
            )));
        };
        Ok(Box::new(tablet.keyspace.clone()))
    }

    fn sink(&self, tablet: TabletId) -> Result<Box<dyn HiddenIndexSink>, TabletDataError> {
        let tablets = self.tablets.lock().expect("tablets lock poisoned");
        let Some(tablet) = tablets.get(&tablet) else {
            return Err(TabletDataError::Sink(format!("unknown tablet {tablet}")));
        };
        Ok(Box::new(tablet.indexes.clone()))
    }

    fn maintainer(
        &self,
        tablet: TabletId,
    ) -> Result<Box<dyn ApplySideIndexMaintainer>, TabletDataError> {
        let tablets = self.tablets.lock().expect("tablets lock poisoned");
        let Some(tablet) = tablets.get(&tablet) else {
            return Err(TabletDataError::Sink(format!("unknown tablet {tablet}")));
        };
        Ok(Box::new(tablet.indexes.clone()))
    }
}

// ---------------------------------------------------------------------------
// The driver
// ---------------------------------------------------------------------------

/// The outcome of one [`DdlDriver::drive_job`] call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DriveOutcome {
    /// The job reached `Succeeded` (`Public` for additions, `Dropping` for
    /// drops).
    Completed,
    /// The job is paused; a later drive resumes from the durable cursors.
    Parked,
    /// The job was cancelled (or crashed mid-rollback) and the unwind ran:
    /// hidden generations dropped, unpublished records removed.
    RolledBack,
}

/// One atomic step of [`DdlDriver::step_job`]; tests drive jobs stepwise to
/// interleave writes, pauses, and crashes between protocol actions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DriveStep {
    /// `Pending -> Running`.
    Admitted,
    /// One [`DdlPhase`] edge applied.
    PhaseAdvanced {
        /// Previous phase.
        from: DdlPhase,
        /// New phase.
        to: DdlPhase,
    },
    /// One tablet's snapshot backfill + catch-up completed and was recorded.
    TabletBackfilled {
        /// The tablet.
        tablet: TabletId,
    },
    /// One tablet validated.
    TabletValidated {
        /// The tablet.
        tablet: TabletId,
    },
    /// The atomic publication landed at this metadata version.
    Published {
        /// The publication metadata version.
        version: MetadataVersion,
    },
    /// The job is paused.
    Parked,
    /// The job's rollback ran.
    RolledBack,
    /// The job is in a terminal state.
    Terminal,
}

/// The outcome of one [`DdlDriver::reclaim_index`] call (spec step 7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReclaimOutcome {
    /// The record was removed; per-tablet generations were dropped.
    Reclaimed,
    /// Readers below the retirement version still pin the metadata; retry
    /// once they drain.
    Blocked {
        /// Oldest live reader pin, when any.
        oldest_reader: Option<MetadataVersion>,
        /// The retirement version every reader must have reached.
        required: MetadataVersion,
    },
}

/// The operator-facing status of one job (spec section 12.11 admin surface).
#[derive(Clone, Debug, PartialEq)]
pub struct DdlJobStatus {
    /// The replicated record.
    pub record: DdlJobRecord,
    /// Tablets with a registered progress cursor.
    pub tablets_registered: usize,
    /// Tablets that completed snapshot backfill + catch-up.
    pub tablets_caught_up: usize,
    /// Tablets that validated.
    pub tablets_validated: usize,
    /// Total rows scanned from pinned snapshots.
    pub rows_backfilled: u64,
}

/// The distributed DDL driver (spec section 12.11): submits jobs, drives
/// them through the phase machine against the replicated [`DdlJobStore`],
/// and runs the per-tablet backfill/catch-up/validation through the tablet
/// seams. One driver per cluster admin surface; the store's applied commands
/// are the only progress authority, so a replacement driver over the same
/// store resumes exactly.
pub struct DdlDriver<P: DdlTabletProvider> {
    store: Arc<Mutex<DdlJobStore>>,
    provider: P,
    clock: HlcClock,
    projection: IndexProjection,
}

impl<P: DdlTabletProvider> DdlDriver<P> {
    /// A driver over `store` and `provider` with the [`identity_projection`].
    pub fn new(store: Arc<Mutex<DdlJobStore>>, provider: P) -> Self {
        Self::with_projection(store, provider, identity_projection())
    }

    /// A driver with an explicit row-to-entry projection.
    pub fn with_projection(
        store: Arc<Mutex<DdlJobStore>>,
        provider: P,
        projection: IndexProjection,
    ) -> Self {
        Self {
            store,
            provider,
            clock: HlcClock::new(0, Duration::from_secs(30)),
            projection,
        }
    }

    /// The shared store handle (tests and the raft binding).
    pub fn store_handle(&self) -> Arc<Mutex<DdlJobStore>> {
        self.store.clone()
    }

    // -- Admin surface: submissions ----------------------------------------

    /// Submits an online index build (spec steps 1-6). The job pins
    /// `source_schema_version`; a stale schema is refused with the structured
    /// [`DdlRejection::SchemaVersionMismatch`] retry.
    pub fn submit_add_index(
        &self,
        database_id: DatabaseId,
        table_id: TableId,
        index_name: impl Into<String>,
        spec: serde_json::Value,
        source_schema_version: SchemaVersion,
    ) -> Result<u64, DdlError> {
        let created_at = self.clock.now()?;
        let job = DdlJobRecord {
            job_id: self.peek_next_job_id(),
            database_id,
            table_id,
            kind: DdlJobKind::AddIndex,
            state: SchemaJobState::Pending,
            phase: DdlPhase::Pending,
            definition: DdlDefinition::AddIndex {
                index_name: index_name.into(),
                spec,
            },
            source_schema_version,
            created_at,
            updated_at: created_at,
            pinned_snapshot: None,
            tablet_progress: BTreeMap::new(),
            error: None,
            metadata_version: MetadataVersion::ZERO,
        };
        self.apply(DdlCommand::SubmitJob { job: job.clone() })?;
        Ok(job.job_id)
    }

    /// Submits an online index drop (`Public -> Dropping -> reclaim`).
    pub fn submit_drop_index(
        &self,
        database_id: DatabaseId,
        table_id: TableId,
        index_name: impl Into<String>,
        source_schema_version: SchemaVersion,
    ) -> Result<u64, DdlError> {
        let created_at = self.clock.now()?;
        let job = DdlJobRecord {
            job_id: self.peek_next_job_id(),
            database_id,
            table_id,
            kind: DdlJobKind::DropIndex,
            state: SchemaJobState::Pending,
            phase: DdlPhase::Public,
            definition: DdlDefinition::DropIndex {
                index_name: index_name.into(),
            },
            source_schema_version,
            created_at,
            updated_at: created_at,
            pinned_snapshot: None,
            tablet_progress: BTreeMap::new(),
            error: None,
            metadata_version: MetadataVersion::ZERO,
        };
        self.apply(DdlCommand::SubmitJob { job: job.clone() })?;
        Ok(job.job_id)
    }

    /// Submits a schema alteration. Publication advances the table anchor to
    /// `source_schema_version + 1` and swaps the schema document atomically.
    pub fn submit_alter_schema(
        &self,
        database_id: DatabaseId,
        table_id: TableId,
        target: serde_json::Value,
        source_schema_version: SchemaVersion,
    ) -> Result<u64, DdlError> {
        let created_at = self.clock.now()?;
        let job = DdlJobRecord {
            job_id: self.peek_next_job_id(),
            database_id,
            table_id,
            kind: DdlJobKind::AlterSchema,
            state: SchemaJobState::Pending,
            phase: DdlPhase::Pending,
            definition: DdlDefinition::AlterSchema { target },
            source_schema_version,
            created_at,
            updated_at: created_at,
            pinned_snapshot: None,
            tablet_progress: BTreeMap::new(),
            error: None,
            metadata_version: MetadataVersion::ZERO,
        };
        self.apply(DdlCommand::SubmitJob { job: job.clone() })?;
        Ok(job.job_id)
    }

    // -- Admin surface: job control -----------------------------------------

    /// Parks a `Running` job at the next tablet boundary (graph-enforced).
    /// Refuses from every other state (a double pause is an operator error,
    /// not an idempotent replay).
    pub fn pause_job(&self, job_id: u64) -> Result<(), DdlError> {
        let record = self.job_record(job_id)?;
        if record.state != SchemaJobState::Running {
            return Err(DdlError::Rejection(DdlRejection::JobNotRunning {
                job_id,
                state: record.state,
            }));
        }
        self.apply(DdlCommand::SetJobState {
            job_id,
            state: SchemaJobState::Paused,
            updated_at: self.clock.now()?,
            error: None,
            expected_version: Some(record.metadata_version),
        })
    }

    /// Requeues a `Paused` job; the next drive admits and resumes it from the
    /// durable per-tablet cursors. Refuses from every other state.
    pub fn resume_job(&self, job_id: u64) -> Result<(), DdlError> {
        let record = self.job_record(job_id)?;
        if record.state != SchemaJobState::Paused {
            return Err(DdlError::Rejection(DdlRejection::Meta(
                MetaRejectionReason::Conflict {
                    resource: format!("DDL job {job_id}"),
                    reason: format!("cannot resume from {:?}", record.state),
                },
            )));
        }
        self.apply(DdlCommand::SetJobState {
            job_id,
            state: SchemaJobState::Pending,
            updated_at: self.clock.now()?,
            error: None,
            expected_version: Some(record.metadata_version),
        })
    }

    /// Requests cancellation. The unwind (hidden generations dropped,
    /// unpublished records removed) runs on the next drive step. Terminal
    /// jobs refuse cancellation.
    pub fn cancel_job(&self, job_id: u64) -> Result<(), DdlError> {
        let record = self.job_record(job_id)?;
        if record.state.is_terminal() {
            return Err(DdlError::Rejection(DdlRejection::Meta(
                MetaRejectionReason::Conflict {
                    resource: format!("DDL job {job_id}"),
                    reason: format!("terminal state {:?}", record.state),
                },
            )));
        }
        self.apply(DdlCommand::SetJobState {
            job_id,
            state: SchemaJobState::Cancelling,
            updated_at: self.clock.now()?,
            error: None,
            expected_version: Some(record.metadata_version),
        })
    }

    /// The operator-facing status of one job.
    pub fn job_status(&self, job_id: u64) -> Option<DdlJobStatus> {
        let record = self
            .store
            .lock()
            .expect("store lock poisoned")
            .job(job_id)?
            .clone();
        let tablets_registered = record.tablet_progress.len();
        let tablets_caught_up = record
            .tablet_progress
            .values()
            .filter(|progress| progress.stage >= TabletDdlStage::CaughtUp)
            .count();
        let tablets_validated = record
            .tablet_progress
            .values()
            .filter(|progress| progress.stage == TabletDdlStage::Validated)
            .count();
        let rows_backfilled = record
            .tablet_progress
            .values()
            .map(|progress| progress.rows_scanned)
            .sum();
        Some(DdlJobStatus {
            record,
            tablets_registered,
            tablets_caught_up,
            tablets_validated,
            rows_backfilled,
        })
    }

    /// The aggregate validation report of one job (spec step 5), once every
    /// registered tablet reported.
    pub fn validation_report(&self, job_id: u64) -> Option<JobValidationReport> {
        let record = self
            .store
            .lock()
            .expect("store lock poisoned")
            .job(job_id)?
            .clone();
        let tablets: Vec<TabletValidationReport> = record
            .tablet_progress
            .values()
            .filter_map(|progress| progress.validation.clone())
            .collect();
        if tablets.is_empty() {
            return None;
        }
        let total_expected = tablets.iter().map(|report| report.expected_rows).sum();
        let total_actual = tablets.iter().map(|report| report.actual_rows).sum();
        let passed = tablets.iter().all(TabletValidationReport::passed);
        Some(JobValidationReport {
            job_id,
            tablets,
            total_expected,
            total_actual,
            passed,
        })
    }

    // -- Admin surface: reclamation (spec step 7) ---------------------------

    /// Pins a reader at a metadata version (its plans may reference every
    /// element visible there). The gateway wave owns real reader tracking.
    pub fn pin_reader(&self, reader_id: u64, version: MetadataVersion) -> Result<(), DdlError> {
        self.apply(DdlCommand::PinReader { reader_id, version })
    }

    /// Releases one reader pin.
    pub fn release_reader(&self, reader_id: u64) -> Result<(), DdlError> {
        self.apply(DdlCommand::ReleaseReader { reader_id })
    }

    /// Runs the reclamation pass for one dropped index: removes the record
    /// once no live reader pins a metadata version below its retirement
    /// version, and drops the per-tablet generations.
    pub fn reclaim_index(
        &self,
        table_id: TableId,
        index_name: &str,
    ) -> Result<ReclaimOutcome, DdlError> {
        match self.apply(DdlCommand::ReclaimIndex {
            table_id,
            index_name: index_name.to_owned(),
        }) {
            Ok(()) => {
                for tablet in self.provider.tablets_of(table_id) {
                    self.provider.sink(tablet)?.drop_generation(index_name)?;
                }
                Ok(ReclaimOutcome::Reclaimed)
            }
            Err(DdlError::Rejection(DdlRejection::ReclaimBlocked {
                oldest_reader,
                required,
                ..
            })) => Ok(ReclaimOutcome::Blocked {
                oldest_reader,
                required,
            }),
            Err(error) => Err(error),
        }
    }

    // -- Driving ------------------------------------------------------------

    /// Drives one job until it terminates, parks, or fails. Resumable: every
    /// step is an applied store command, so a fresh driver over the same
    /// store continues exactly where a crashed one stopped.
    pub fn drive_job(&self, job_id: u64) -> Result<DriveOutcome, DdlError> {
        loop {
            match self.step_job(job_id)? {
                DriveStep::Parked => return Ok(DriveOutcome::Parked),
                DriveStep::RolledBack => return Ok(DriveOutcome::RolledBack),
                DriveStep::Terminal => {
                    let state = self.job_record(job_id)?.state;
                    return Ok(match state {
                        SchemaJobState::Succeeded => DriveOutcome::Completed,
                        _ => DriveOutcome::RolledBack,
                    });
                }
                DriveStep::Admitted
                | DriveStep::PhaseAdvanced { .. }
                | DriveStep::TabletBackfilled { .. }
                | DriveStep::TabletValidated { .. }
                | DriveStep::Published { .. } => {}
            }
        }
    }

    /// Performs one atomic protocol action of one job. Every mutating step
    /// is exactly one applied store command (plus the seam work it records).
    fn step_job(&self, job_id: u64) -> Result<DriveStep, DdlError> {
        let job = self.job_record(job_id)?;
        match job.state {
            SchemaJobState::Pending => {
                self.set_state(job_id, SchemaJobState::Running, None)?;
                return Ok(DriveStep::Admitted);
            }
            SchemaJobState::Paused => return Ok(DriveStep::Parked),
            SchemaJobState::Cancelling | SchemaJobState::RollingBack => {
                self.rollback(&job, "cancelled by operator")?;
                return Ok(DriveStep::RolledBack);
            }
            SchemaJobState::Failed | SchemaJobState::Succeeded => {
                return Ok(DriveStep::Terminal);
            }
            SchemaJobState::Running => {}
        }
        match job.phase {
            DdlPhase::Pending => {
                // Spec step 1: the definition is replicated WriteOnly; from
                // here the tablet apply path dual-maintains it (step 2).
                self.advance_phase(job_id, DdlPhase::WriteOnly, None)?;
                self.sync_maintainers(job.table_id)?;
                Ok(DriveStep::PhaseAdvanced {
                    from: DdlPhase::Pending,
                    to: DdlPhase::WriteOnly,
                })
            }
            DdlPhase::WriteOnly => {
                // Spec step 3: stamp the job-wide pinned snapshot.
                let pin = self.clock.now()?;
                self.advance_phase(job_id, DdlPhase::Backfilling, Some(pin))?;
                Ok(DriveStep::PhaseAdvanced {
                    from: DdlPhase::WriteOnly,
                    to: DdlPhase::Backfilling,
                })
            }
            DdlPhase::Backfilling => {
                let tablets = self.provider.tablets_of(job.table_id);
                for tablet in &tablets {
                    if !job.tablet_progress.contains_key(tablet) {
                        self.update_progress(job_id, TabletDdlProgress::pending(*tablet))?;
                    }
                }
                let mut job = self.job_record(job_id)?;
                let departed: Vec<TabletId> = job
                    .tablet_progress
                    .keys()
                    .filter(|tablet| !tablets.contains(tablet))
                    .copied()
                    .collect();
                for tablet in departed {
                    self.apply(DdlCommand::ForgetTabletProgress {
                        job_id,
                        tablet_id: tablet,
                    })?;
                }
                job = self.job_record(job_id)?;
                let next = tablets.iter().copied().find(|tablet| {
                    job.tablet_progress
                        .get(tablet)
                        .is_none_or(|progress| progress.stage < TabletDdlStage::CaughtUp)
                });
                match next {
                    Some(tablet) => {
                        let progress = job.tablet_progress.get(&tablet);
                        if progress.is_none_or(|p| p.stage < TabletDdlStage::Backfilling) {
                            let mut marker = TabletDdlProgress::pending(tablet);
                            marker.stage = TabletDdlStage::Backfilling;
                            self.update_progress(job_id, marker)?;
                            job = self.job_record(job_id)?;
                        }
                        let progress = self.backfill_tablet(&job, tablet)?;
                        self.update_progress(job_id, progress)?;
                        Ok(DriveStep::TabletBackfilled { tablet })
                    }
                    None => {
                        self.advance_phase(job_id, DdlPhase::Validating, None)?;
                        Ok(DriveStep::PhaseAdvanced {
                            from: DdlPhase::Backfilling,
                            to: DdlPhase::Validating,
                        })
                    }
                }
            }
            DdlPhase::Validating => {
                // Spec step 5: validate every backfilled tablet (the progress
                // map is the tablet set the job backfilled; tablets appearing
                // mid-Validating are the split/merge follow-up's concern).
                let next = job
                    .tablet_progress
                    .values()
                    .find(|progress| progress.stage < TabletDdlStage::Validated)
                    .map(|progress| progress.tablet_id);
                match next {
                    Some(tablet) => {
                        let report = self.validate_tablet(&job, tablet)?;
                        let passed = report.passed();
                        self.apply(DdlCommand::ReportTabletValidation {
                            job_id,
                            report: report.clone(),
                        })?;
                        if !passed {
                            let reason = format!(
                                "tablet {tablet}: expected {} rows, built {} rows",
                                report.expected_rows, report.actual_rows
                            );
                            self.rollback(&job, &reason)?;
                            return Err(DdlError::ValidationFailed { job_id, reason });
                        }
                        Ok(DriveStep::TabletValidated { tablet })
                    }
                    None => {
                        // Spec step 6: ONE atomic command flips the element
                        // Public at this metadata version.
                        let published_at = self.clock.now()?;
                        match self.apply(DdlCommand::PublishJob {
                            job_id,
                            published_at,
                        }) {
                            Ok(()) => Ok(DriveStep::Published {
                                version: self
                                    .store
                                    .lock()
                                    .expect("store lock poisoned")
                                    .metadata_version,
                            }),
                            Err(DdlError::Rejection(
                                reason @ DdlRejection::SchemaVersionMismatch { .. },
                            )) => {
                                self.rollback(&job, &reason.to_string())?;
                                Err(DdlError::Rejection(reason))
                            }
                            Err(error) => Err(error),
                        }
                    }
                }
            }
            DdlPhase::Public => match job.kind {
                // Spec drop path: one command flips the element Dropping —
                // writes stop maintaining it, planners stop using it.
                DdlJobKind::DropIndex => {
                    self.advance_phase(job_id, DdlPhase::Dropping, None)?;
                    self.sync_maintainers(job.table_id)?;
                    Ok(DriveStep::PhaseAdvanced {
                        from: DdlPhase::Public,
                        to: DdlPhase::Dropping,
                    })
                }
                DdlJobKind::AddIndex | DdlJobKind::AlterSchema => Ok(DriveStep::Terminal),
            },
            DdlPhase::Dropping => {
                self.set_state(job_id, SchemaJobState::Succeeded, None)?;
                Ok(DriveStep::Terminal)
            }
        }
    }

    /// Spec steps 3-4 for one tablet: build the hidden generation from the
    /// pinned snapshot, install it, then sweep committed deltas forward until
    /// the stream drains. Returns the durable cursor.
    fn backfill_tablet(
        &self,
        job: &DdlJobRecord,
        tablet: TabletId,
    ) -> Result<TabletDdlProgress, DdlError> {
        let pin = job
            .pinned_snapshot
            .ok_or(DdlRejection::Meta(MetaRejectionReason::Invalid {
                reason: format!("job {} entered Backfilling without a pin", job.job_id),
            }))?;
        let keyspace = self.provider.keyspace(tablet)?;
        let _pin = keyspace.pin_snapshot(pin)?;
        let mut rows_scanned = 0_u64;
        match &job.definition {
            DdlDefinition::AddIndex { index_name, .. } => {
                let mut sink = self.provider.sink(tablet)?;
                let record = self.index_record(job, index_name)?;
                sink.begin_build(index_name)?;
                for (key, value) in keyspace.snapshot_at(pin)? {
                    let entry = (self.projection)(&record, &key, &value);
                    sink.stage_entry(index_name, &key, &entry)?;
                    rows_scanned += 1;
                }
                sink.install_staged(index_name)?;
            }
            DdlDefinition::AlterSchema { .. } => {
                // The engine's column backfill lands with the server wave;
                // this wave scans the pinned snapshot per tablet so the phase
                // machine and resume cursors carry real per-tablet progress.
                for (_key, _value) in keyspace.snapshot_at(pin)? {
                    rows_scanned += 1;
                }
            }
            DdlDefinition::DropIndex { .. } => {
                return Err(DdlRejection::Meta(MetaRejectionReason::Invalid {
                    reason: "drop jobs never backfill".to_owned(),
                })
                .into());
            }
        }
        let watermark = self.catch_up_tablet(job, tablet, pin)?;
        Ok(TabletDdlProgress {
            tablet_id: tablet,
            stage: TabletDdlStage::CaughtUp,
            rows_scanned,
            caught_up_through: Some(watermark),
            validation: None,
        })
    }

    /// Spec step 4: sweeps committed deltas from `from` forward until a sweep
    /// applies nothing. Each non-empty sweep strictly advances the watermark,
    /// so the loop terminates; writes racing the sweep are picked up by the
    /// next one (or by the apply-path maintainer past the final watermark).
    fn catch_up_tablet(
        &self,
        job: &DdlJobRecord,
        tablet: TabletId,
        from: HlcTimestamp,
    ) -> Result<HlcTimestamp, DdlError> {
        let keyspace = self.provider.keyspace(tablet)?;
        let mut sink = match &job.definition {
            DdlDefinition::AddIndex { index_name, .. } => {
                Some((self.provider.sink(tablet)?, index_name.clone()))
            }
            _ => None,
        };
        let mut watermark = from;
        loop {
            let mut saw_any = false;
            let mut max_ts = watermark;
            for (ts, key, value) in keyspace.deltas_after(watermark)? {
                saw_any = true;
                max_ts = max_ts.max(ts);
                if let Some((sink, index_name)) = &mut sink {
                    let entry = match &value {
                        Some(value) => {
                            let record = self.index_record(job, index_name)?;
                            Some((self.projection)(&record, &key, value))
                        }
                        None => None,
                    };
                    sink.apply_delta(index_name, &key, entry.as_deref())?;
                }
            }
            if !saw_any {
                return Ok(watermark);
            }
            watermark = max_ts;
        }
    }

    /// Spec step 5 for one tablet: a final catch-up sweep, then the built
    /// generation against a from-scratch build at the same watermark.
    fn validate_tablet(
        &self,
        job: &DdlJobRecord,
        tablet: TabletId,
    ) -> Result<TabletValidationReport, DdlError> {
        let pin = job.pinned_snapshot.expect("Validating implies a pin");
        let from = job
            .tablet_progress
            .get(&tablet)
            .and_then(|progress| progress.caught_up_through)
            .unwrap_or(pin);
        let watermark = self.catch_up_tablet(job, tablet, from)?;
        let keyspace = self.provider.keyspace(tablet)?;
        let expected: BTreeMap<Key, Vec<u8>> = match &job.definition {
            DdlDefinition::AddIndex { index_name, .. } => {
                let record = self.index_record(job, index_name)?;
                keyspace
                    .snapshot_at(watermark)?
                    .map(|(key, value)| {
                        let entry = (self.projection)(&record, &key, &value);
                        (key, entry)
                    })
                    .collect()
            }
            // AlterSchema validates scan completeness at the watermark until
            // the engine's column backfill lands (documented module scope).
            _ => keyspace.snapshot_at(watermark)?.collect(),
        };
        let actual = match &job.definition {
            DdlDefinition::AddIndex { index_name, .. } => {
                self.provider.sink(tablet)?.generation_entries(index_name)?
            }
            _ => expected.clone(),
        };
        Ok(TabletValidationReport {
            tablet_id: tablet,
            watermark,
            expected_rows: expected.len() as u64,
            actual_rows: actual.len() as u64,
            expected_checksum: generation_checksum(&expected),
            actual_checksum: generation_checksum(&actual),
        })
    }

    /// The unwind for cancellation and validation failure: drop the hidden
    /// generations on every tablet, remove the unpublished index record, and
    /// fail the job. Idempotent; a `RollingBack` job (crash mid-rollback)
    /// re-enters here.
    fn rollback(&self, job: &DdlJobRecord, reason: &str) -> Result<(), DdlError> {
        let current = self.job_record(job.job_id)?;
        match current.state {
            SchemaJobState::Failed => return Ok(()),
            SchemaJobState::Succeeded => {
                return Err(DdlRejection::Meta(MetaRejectionReason::Conflict {
                    resource: format!("DDL job {}", job.job_id),
                    reason: "cannot roll back a succeeded job".to_owned(),
                })
                .into());
            }
            SchemaJobState::Running | SchemaJobState::Cancelling => {
                self.set_state(
                    job.job_id,
                    SchemaJobState::RollingBack,
                    Some(reason.to_owned()),
                )?;
            }
            SchemaJobState::Pending | SchemaJobState::Paused | SchemaJobState::RollingBack => {}
        }
        if let DdlDefinition::AddIndex { index_name, .. } = &job.definition {
            for tablet in self.provider.tablets_of(job.table_id) {
                self.provider.sink(tablet)?.drop_generation(index_name)?;
            }
            self.apply(DdlCommand::RemoveIndexRecord { job_id: job.job_id })?;
            self.sync_maintainers(job.table_id)?;
        }
        self.set_state(job.job_id, SchemaJobState::Failed, Some(reason.to_owned()))
    }

    // -- Store helpers --------------------------------------------------------

    fn apply(&self, command: DdlCommand) -> Result<(), DdlError> {
        let commit_ts = self.clock.now()?;
        self.store
            .lock()
            .expect("store lock poisoned")
            .apply(&command, None, commit_ts)
            .map_err(DdlError::from)
    }

    fn job_record(&self, job_id: u64) -> Result<DdlJobRecord, DdlError> {
        self.store
            .lock()
            .expect("store lock poisoned")
            .job(job_id)
            .cloned()
            .ok_or_else(|| {
                DdlRejection::Meta(MetaRejectionReason::NotFound {
                    resource: format!("DDL job {job_id}"),
                })
                .into()
            })
    }

    fn index_record(
        &self,
        job: &DdlJobRecord,
        index_name: &str,
    ) -> Result<DdlIndexRecord, DdlError> {
        self.store
            .lock()
            .expect("store lock poisoned")
            .index(job.table_id, index_name)
            .cloned()
            .ok_or_else(|| {
                DdlRejection::Meta(MetaRejectionReason::NotFound {
                    resource: format!("index `{index_name}` on table {}", job.table_id),
                })
                .into()
            })
    }

    fn peek_next_job_id(&self) -> u64 {
        self.store.lock().expect("store lock poisoned").next_job_id
    }

    fn set_state(
        &self,
        job_id: u64,
        state: SchemaJobState,
        error: Option<String>,
    ) -> Result<(), DdlError> {
        let updated_at = self.clock.now()?;
        self.apply(DdlCommand::SetJobState {
            job_id,
            state,
            updated_at,
            error,
            expected_version: None,
        })
    }

    fn advance_phase(
        &self,
        job_id: u64,
        to: DdlPhase,
        pinned_snapshot: Option<HlcTimestamp>,
    ) -> Result<(), DdlError> {
        self.apply(DdlCommand::AdvancePhase {
            job_id,
            to,
            pinned_snapshot,
            expected_version: None,
        })
    }

    fn update_progress(&self, job_id: u64, progress: TabletDdlProgress) -> Result<(), DdlError> {
        self.apply(DdlCommand::UpdateTabletProgress { job_id, progress })
    }

    /// Pushes the table's maintained definition set to every tablet's
    /// apply-path maintainer (spec step 2's "tablet apply sees WriteOnly
    /// index defs for its table").
    fn sync_maintainers(&self, table_id: TableId) -> Result<(), DdlError> {
        let names: Vec<String> = self
            .store
            .lock()
            .expect("store lock poisoned")
            .write_maintained(table_id)
            .iter()
            .map(|record| record.index_name.clone())
            .collect();
        for tablet in self.provider.tablets_of(table_id) {
            self.provider
                .maintainer(tablet)?
                .sync_definitions(names.clone())?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE: u64 = 1;

    fn ts(micros: u64) -> HlcTimestamp {
        HlcTimestamp {
            physical_micros: micros,
            logical: 0,
            node_tiebreaker: 0,
        }
    }

    /// A commit timestamp `n` micros after the job's pinned snapshot.
    fn after(pin: HlcTimestamp, n: u64) -> HlcTimestamp {
        HlcTimestamp {
            physical_micros: pin.physical_micros + n,
            logical: 0,
            node_tiebreaker: 0,
        }
    }

    fn tablet(n: u8) -> TabletId {
        TabletId::from_bytes([n; 16])
    }

    fn database() -> DatabaseId {
        DatabaseId::from_bytes([7; 16])
    }

    fn table() -> TableId {
        TableId(TABLE)
    }

    fn key(n: u64) -> Key {
        Key::from_bytes(n.to_be_bytes().to_vec())
    }

    fn value(n: u64) -> Vec<u8> {
        format!("value-{n}").into_bytes()
    }

    fn spec() -> serde_json::Value {
        serde_json::json!({"kind": "Bitmap", "columns": [1]})
    }

    struct Fixture {
        store: Arc<Mutex<DdlJobStore>>,
        tablets: InMemoryDdlTablets,
        driver: DdlDriver<InMemoryDdlTablets>,
    }

    impl Fixture {
        /// A store with table 1 anchored at schema version 1, `tablet_count`
        /// tablets each seeded with `rows_per_tablet` rows at ts(1..=rows).
        fn new(tablet_count: u8, rows_per_tablet: u64) -> Self {
            let store = Arc::new(Mutex::new(DdlJobStore::default()));
            let tablets = InMemoryDdlTablets::new();
            for n in 1..=tablet_count {
                tablets.add_tablet(tablet(n), table());
                let keyspace = tablets.keyspace_handle(tablet(n));
                for row in 1..=rows_per_tablet {
                    keyspace.insert(key(row), ts(row), value(row));
                }
            }
            let driver = DdlDriver::new(store.clone(), tablets.clone());
            driver
                .apply(DdlCommand::RegisterTable {
                    anchor: TableAnchor {
                        table_id: table(),
                        database_id: database(),
                        schema_version: SchemaVersion(1),
                        schema: serde_json::json!({"columns": ["id", "v"]}),
                        metadata_version: MetadataVersion::ZERO,
                    },
                })
                .expect("register table");
            Self {
                store,
                tablets,
                driver,
            }
        }

        fn store(&self) -> std::sync::MutexGuard<'_, DdlJobStore> {
            self.store.lock().expect("store lock poisoned")
        }

        fn submit_add(&self, index_name: &str) -> Result<u64, DdlError> {
            self.driver
                .submit_add_index(database(), table(), index_name, spec(), SchemaVersion(1))
        }

        fn pin_of(&self, job_id: u64) -> HlcTimestamp {
            self.store()
                .job(job_id)
                .and_then(|job| job.pinned_snapshot)
                .expect("job is pinned")
        }

        /// Drives the job until `step` returns true, returning the steps it
        /// took (fails after a bound of steps to surface livelocks).
        fn drive_until(
            &self,
            job_id: u64,
            mut stop: impl FnMut(DriveStep) -> bool,
        ) -> Vec<DriveStep> {
            let mut steps = Vec::new();
            for _ in 0..64 {
                let step = self.driver.step_job(job_id).expect("step");
                steps.push(step);
                if stop(step) || matches!(step, DriveStep::Terminal | DriveStep::Parked) {
                    return steps;
                }
            }
            panic!("job {job_id} did not reach the expected step");
        }

        fn drive_to_backfilled(&self, job_id: u64, count: usize) {
            let mut backfilled = 0_usize;
            self.drive_until(job_id, |step| {
                if matches!(step, DriveStep::TabletBackfilled { .. }) {
                    backfilled += 1;
                }
                backfilled == count
            });
        }
    }

    #[test]
    fn add_index_full_lifecycle_over_three_tablets() {
        let fixture = Fixture::new(3, 5);
        let job_id = fixture.submit_add("idx_a").expect("submit");

        // Spec step 1-2: the definition is replicated WriteOnly — maintained
        // by the write path, hidden from planners.
        let steps = fixture.drive_until(job_id, |step| {
            matches!(
                step,
                DriveStep::PhaseAdvanced {
                    from: DdlPhase::Pending,
                    to: DdlPhase::WriteOnly,
                }
            )
        });
        assert_eq!(steps.first(), Some(&DriveStep::Admitted));
        assert_eq!(fixture.store().write_maintained(table()).len(), 1);
        assert!(fixture.store().planner_visible(table()).is_empty());
        for n in 1..=3 {
            assert_eq!(
                fixture.tablets.indexes_handle(tablet(n)).maintained(),
                ["idx_a"]
            );
        }

        // Spec step 3: the job-wide snapshot is pinned.
        fixture.drive_until(job_id, |step| {
            matches!(
                step,
                DriveStep::PhaseAdvanced {
                    from: DdlPhase::WriteOnly,
                    to: DdlPhase::Backfilling,
                }
            )
        });
        let pin = fixture.pin_of(job_id);

        // Writes keep arriving. One lands before tablet 1's generation
        // install (the catch-up sweep must cover it); one lands after (the
        // apply-path maintainer must cover it); one delete exercises
        // tombstones on both paths.
        fixture
            .tablets
            .commit_write(tablet(1), key(100), after(pin, 1), Some(value(100)))
            .expect("write 100");
        fixture.drive_to_backfilled(job_id, 1);
        assert!(fixture
            .tablets
            .indexes_handle(tablet(1))
            .is_installed("idx_a"));
        fixture
            .tablets
            .commit_write(tablet(1), key(102), after(pin, 3), Some(value(102)))
            .expect("write 102");
        // Dual maintenance applied write 102 immediately.
        assert_eq!(
            fixture
                .tablets
                .indexes_handle(tablet(1))
                .entries("idx_a")
                .len(),
            7,
        );
        fixture
            .tablets
            .commit_write(tablet(2), key(101), after(pin, 2), Some(value(101)))
            .expect("write 101");
        fixture
            .tablets
            .commit_write(tablet(1), key(1), after(pin, 4), None)
            .expect("delete 1");
        fixture.drive_to_backfilled(job_id, 2);

        // Spec step 5-6: validation, then the atomic publication.
        let steps = fixture.drive_until(job_id, |step| matches!(step, DriveStep::Published { .. }));
        let publication_version = steps.iter().find_map(|step| match step {
            DriveStep::Published { version } => Some(*version),
            _ => None,
        });
        let publication_version = publication_version.expect("published");
        let store = fixture.store();
        let job = store.job(job_id).expect("job");
        assert_eq!(job.state, SchemaJobState::Succeeded);
        assert_eq!(job.phase, DdlPhase::Public);
        let record = store.index(table(), "idx_a").expect("index record");
        assert_eq!(record.phase, DdlPhase::Public);
        assert_eq!(record.publication_version, Some(publication_version));
        // The planner flips at exactly one metadata version.
        assert!(store
            .planner_visible_at(table(), MetadataVersion(publication_version.get() - 1))
            .is_empty());
        assert_eq!(
            store.planner_visible_at(table(), publication_version).len(),
            1
        );
        drop(store);

        // Spec step 5's aggregate report: per-tablet counts and checksums of
        // backfill+catch-up equal a from-scratch build.
        let report = fixture
            .driver
            .validation_report(job_id)
            .expect("validation report");
        assert!(report.passed);
        assert_eq!(report.tablets.len(), 3);
        assert_eq!(report.total_expected, report.total_actual);
        let per_tablet: BTreeMap<TabletId, &TabletValidationReport> = report
            .tablets
            .iter()
            .map(|tablet_report| (tablet_report.tablet_id, tablet_report))
            .collect();
        assert_eq!(per_tablet[&tablet(1)].expected_rows, 6); // 5 seeded - 1 deleted + 2 written
        assert_eq!(per_tablet[&tablet(2)].expected_rows, 6);
        assert_eq!(per_tablet[&tablet(3)].expected_rows, 5);

        // Direct content equality on tablet 1: built generation == projected
        // from-scratch rows at the validation watermark.
        let watermark = per_tablet[&tablet(1)].watermark;
        let expected = fixture
            .tablets
            .keyspace_handle(tablet(1))
            .rows_at(watermark);
        let actual = fixture.tablets.indexes_handle(tablet(1)).entries("idx_a");
        assert_eq!(actual, expected);

        // Status reporting aggregates the durable cursors.
        let status = fixture.driver.job_status(job_id).expect("status");
        assert_eq!(status.tablets_registered, 3);
        assert_eq!(status.tablets_caught_up, 3);
        assert_eq!(status.tablets_validated, 3);
        assert_eq!(status.rows_backfilled, 15);
    }

    #[test]
    fn catch_up_covers_pre_install_writes_and_tombstones() {
        let fixture = Fixture::new(1, 3);
        let job_id = fixture.submit_add("idx_t").expect("submit");
        fixture.drive_until(job_id, |step| {
            matches!(
                step,
                DriveStep::PhaseAdvanced {
                    from: DdlPhase::WriteOnly,
                    to: DdlPhase::Backfilling,
                }
            )
        });
        let pin = fixture.pin_of(job_id);
        // Write-then-delete a fresh key and delete a seeded key, all before
        // the generation is installed: the maintainer skips them, so the
        // delta sweep must cover them exactly once in effect.
        fixture
            .tablets
            .commit_write(tablet(1), key(50), after(pin, 1), Some(value(50)))
            .expect("write 50");
        fixture
            .tablets
            .commit_write(tablet(1), key(50), after(pin, 2), None)
            .expect("delete 50");
        fixture
            .tablets
            .commit_write(tablet(1), key(2), after(pin, 3), None)
            .expect("delete 2");
        assert_eq!(
            fixture.driver.drive_job(job_id).expect("drive"),
            DriveOutcome::Completed
        );

        let report = fixture.driver.validation_report(job_id).expect("report");
        assert!(report.passed);
        assert_eq!(report.total_expected, 2); // keys 1 and 3 survive
        let actual = fixture.tablets.indexes_handle(tablet(1)).entries("idx_t");
        assert_eq!(actual.len(), 2);
        assert!(!actual.contains_key(&key(2)));
        assert!(!actual.contains_key(&key(50)));
    }

    #[test]
    fn atomic_publish_flips_planner_visibility_at_one_metadata_version() {
        let fixture = Fixture::new(1, 2);
        let job_id = fixture.submit_add("idx_v").expect("submit");
        let before = fixture.store().metadata_version;
        assert_eq!(
            fixture.driver.drive_job(job_id).expect("drive"),
            DriveOutcome::Completed
        );
        let store = fixture.store();
        let publication = store
            .index(table(), "idx_v")
            .and_then(|record| record.publication_version)
            .expect("publication version");
        assert!(publication > before);
        assert!(
            store
                .planner_visible_at(table(), MetadataVersion(publication.get() - 1))
                .is_empty(),
            "hidden before the publication version"
        );
        let visible = store.planner_visible_at(table(), publication);
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].index_name, "idx_v");
    }

    #[test]
    fn stale_schema_returns_structured_retry() {
        let fixture = Fixture::new(1, 2);
        let job_id = fixture.submit_add("idx_s").expect("submit");
        // Drive to the validation boundary, then move the table's schema
        // (models a concurrent alter landing mid-job).
        fixture.drive_until(job_id, |step| {
            matches!(
                step,
                DriveStep::PhaseAdvanced {
                    from: DdlPhase::Backfilling,
                    to: DdlPhase::Validating,
                }
            )
        });
        fixture
            .driver
            .apply(DdlCommand::RegisterTable {
                anchor: TableAnchor {
                    table_id: table(),
                    database_id: database(),
                    schema_version: SchemaVersion(2),
                    schema: serde_json::json!({"columns": ["id", "v", "w"]}),
                    metadata_version: MetadataVersion::ZERO,
                },
            })
            .expect("schema bump");
        let error = fixture
            .driver
            .drive_job(job_id)
            .expect_err("publish must refuse the stale schema");
        assert_eq!(error.category(), ErrorCategory::SchemaVersionMismatch);
        match &error {
            DdlError::Rejection(DdlRejection::SchemaVersionMismatch {
                table_id,
                expected,
                found,
            }) => {
                assert_eq!(*table_id, table());
                assert_eq!(*expected, SchemaVersion(1));
                assert_eq!(*found, SchemaVersion(2));
            }
            other => panic!("expected SchemaVersionMismatch, got {other:?}"),
        }
        // The failed job unwound: hidden generation dropped, record removed.
        let store = fixture.store();
        let job = store.job(job_id).expect("job");
        assert_eq!(job.state, SchemaJobState::Failed);
        assert!(store.index(table(), "idx_s").is_none());
        drop(store);
        assert!(!fixture
            .tablets
            .indexes_handle(tablet(1))
            .is_installed("idx_s"));

        // Submission against the moved schema fails at submit time too; the
        // refreshed resubmission succeeds.
        let stale = fixture.driver.submit_add_index(
            database(),
            table(),
            "idx_s2",
            spec(),
            SchemaVersion(1),
        );
        assert!(matches!(
            stale,
            Err(DdlError::Rejection(
                DdlRejection::SchemaVersionMismatch { .. }
            ))
        ));
        let job_id = fixture
            .driver
            .submit_add_index(database(), table(), "idx_s2", spec(), SchemaVersion(2))
            .expect("resubmit at the current schema version");
        assert_eq!(
            fixture.driver.drive_job(job_id).expect("drive"),
            DriveOutcome::Completed
        );
    }

    #[test]
    fn pause_resume_mid_backfill_resumes_exactly() {
        let fixture = Fixture::new(3, 4);
        let job_id = fixture.submit_add("idx_p").expect("submit");
        fixture.drive_to_backfilled(job_id, 1);
        fixture.driver.pause_job(job_id).expect("pause");
        assert!(
            fixture.driver.pause_job(job_id).is_err(),
            "double pause is refused"
        );
        assert_eq!(
            fixture.driver.drive_job(job_id).expect("drive"),
            DriveOutcome::Parked
        );
        assert_eq!(
            fixture
                .tablets
                .indexes_handle(tablet(1))
                .begin_build_count(),
            1
        );
        assert_eq!(
            fixture
                .tablets
                .indexes_handle(tablet(2))
                .begin_build_count(),
            0
        );

        fixture.driver.resume_job(job_id).expect("resume");
        assert_eq!(
            fixture.driver.drive_job(job_id).expect("drive"),
            DriveOutcome::Completed
        );
        // Tablet 1 was never rebuilt; the others built exactly once.
        assert_eq!(
            fixture
                .tablets
                .indexes_handle(tablet(1))
                .begin_build_count(),
            1
        );
        assert_eq!(
            fixture
                .tablets
                .indexes_handle(tablet(2))
                .begin_build_count(),
            1
        );
        assert_eq!(
            fixture
                .tablets
                .indexes_handle(tablet(3))
                .begin_build_count(),
            1
        );
        assert!(
            fixture
                .driver
                .validation_report(job_id)
                .expect("report")
                .passed
        );
        assert!(
            fixture.driver.resume_job(job_id).is_err(),
            "resume of a terminal job is refused"
        );
    }

    #[test]
    fn cancel_unwinds_the_hidden_generation_and_leaves_the_table_unaffected() {
        let fixture = Fixture::new(3, 4);
        let job_id = fixture.submit_add("idx_c").expect("submit");
        fixture.drive_to_backfilled(job_id, 1);
        assert!(fixture
            .tablets
            .indexes_handle(tablet(1))
            .is_installed("idx_c"));

        fixture.driver.cancel_job(job_id).expect("cancel");
        assert_eq!(
            fixture.driver.drive_job(job_id).expect("drive"),
            DriveOutcome::RolledBack
        );
        let store = fixture.store();
        let job = store.job(job_id).expect("job");
        assert_eq!(job.state, SchemaJobState::Failed);
        assert_eq!(job.error.as_deref(), Some("cancelled by operator"));
        assert!(store.index(table(), "idx_c").is_none());
        assert!(store.planner_visible(table()).is_empty());
        assert!(store.write_maintained(table()).is_empty());
        // The table is unaffected: anchor and rows untouched.
        assert_eq!(
            store.table_anchor(table()).expect("anchor").schema_version,
            SchemaVersion(1)
        );
        drop(store);
        for n in 1..=3 {
            let indexes = fixture.tablets.indexes_handle(tablet(n));
            assert!(!indexes.is_installed("idx_c"));
            assert_eq!(indexes.entries("idx_c").len(), 0);
            assert_eq!(indexes.maintained(), Vec::<String>::new());
        }
        assert_eq!(
            fixture
                .tablets
                .keyspace_handle(tablet(1))
                .rows_at(ts(u64::MAX))
                .len(),
            4
        );

        // Cancelling a terminal job is refused; a fresh job on the table is
        // admitted after the unwind.
        let second = fixture.submit_add("idx_c2").expect("submit after unwind");
        assert_eq!(
            fixture.driver.drive_job(second).expect("drive"),
            DriveOutcome::Completed
        );
        assert!(fixture.driver.cancel_job(second).is_err());
    }

    #[test]
    fn drop_index_lifecycle_reclaims_after_readers_drain() {
        let fixture = Fixture::new(3, 3);
        let add = fixture.submit_add("idx_d").expect("submit add");
        assert_eq!(
            fixture.driver.drive_job(add).expect("drive"),
            DriveOutcome::Completed
        );
        let pin = fixture.pin_of(add);
        // Public indexes stay maintained.
        fixture
            .tablets
            .commit_write(tablet(1), key(40), after(pin, 1), Some(value(40)))
            .expect("maintained write");
        assert_eq!(
            fixture
                .tablets
                .indexes_handle(tablet(1))
                .entries("idx_d")
                .len(),
            4
        );
        let pre_drop_version = fixture.store().metadata_version;

        let drop = fixture
            .driver
            .submit_drop_index(database(), table(), "idx_d", SchemaVersion(1))
            .expect("submit drop");
        assert_eq!(
            fixture.driver.drive_job(drop).expect("drive"),
            DriveOutcome::Completed
        );
        let dropping_since = {
            let store = fixture.store();
            let record = store.index(table(), "idx_d").expect("record");
            assert_eq!(record.phase, DdlPhase::Dropping);
            assert_eq!(
                store.job(drop).expect("job").state,
                SchemaJobState::Succeeded
            );
            // The planner stopped using it; an older plan still sees it.
            assert!(store.planner_visible(table()).is_empty());
            assert_eq!(store.planner_visible_at(table(), pre_drop_version).len(), 1);
            record.dropping_since.expect("dropping version")
        };
        assert!(dropping_since > pre_drop_version);
        // Writes stopped maintaining it.
        assert_eq!(
            fixture.tablets.indexes_handle(tablet(1)).maintained(),
            Vec::<String>::new()
        );
        fixture
            .tablets
            .commit_write(tablet(1), key(41), after(pin, 2), Some(value(41)))
            .expect("write after drop");
        assert_eq!(
            fixture
                .tablets
                .indexes_handle(tablet(1))
                .entries("idx_d")
                .len(),
            4
        );

        // Reclamation is gated on readers below the retirement version.
        fixture
            .driver
            .pin_reader(1, pre_drop_version)
            .expect("pin reader");
        let blocked = fixture
            .driver
            .reclaim_index(table(), "idx_d")
            .expect("reclaim attempt");
        assert_eq!(
            blocked,
            ReclaimOutcome::Blocked {
                oldest_reader: Some(pre_drop_version),
                required: dropping_since,
            }
        );
        let forward_version = fixture.store().metadata_version;
        fixture
            .driver
            .pin_reader(1, forward_version)
            .expect("reader moves forward");
        assert_eq!(
            fixture
                .driver
                .reclaim_index(table(), "idx_d")
                .expect("reclaim"),
            ReclaimOutcome::Reclaimed
        );
        assert!(fixture.store().index(table(), "idx_d").is_none());
        for n in 1..=3 {
            assert!(!fixture
                .tablets
                .indexes_handle(tablet(n))
                .is_installed("idx_d"));
        }
        // A backward reader pin is refused.
        fixture
            .driver
            .pin_reader(2, dropping_since)
            .expect("pin reader 2");
        assert!(fixture.driver.pin_reader(2, pre_drop_version).is_err());
    }

    #[test]
    fn per_tablet_progress_survives_a_driver_crash() {
        let fixture = Fixture::new(3, 4);
        let job_id = fixture.submit_add("idx_x").expect("submit");
        fixture.drive_to_backfilled(job_id, 1);
        let pin = fixture.pin_of(job_id);
        let crashed = fixture.driver;
        // The crashed driver also left an interrupted staged build behind on
        // tablet 2; the resumed driver must discard it via begin_build.
        fixture.tablets.indexes_handle(tablet(2)).seed_staged(
            "idx_x",
            key(999),
            b"garbage".to_vec(),
        );
        // A committed write lands while no driver runs.
        fixture
            .tablets
            .commit_write(tablet(2), key(60), after(pin, 1), Some(value(60)))
            .expect("write during outage");
        drop(crashed);

        let resumed = DdlDriver::new(fixture.store.clone(), fixture.tablets.clone());
        assert_eq!(
            resumed.drive_job(job_id).expect("drive"),
            DriveOutcome::Completed
        );
        // Tablet 1's durable cursor skipped its rebuild; tablet 2's staged
        // garbage was discarded and rebuilt correctly.
        assert_eq!(
            fixture
                .tablets
                .indexes_handle(tablet(1))
                .begin_build_count(),
            1
        );
        assert_eq!(
            fixture
                .tablets
                .indexes_handle(tablet(2))
                .begin_build_count(),
            1
        );
        assert!(!fixture
            .tablets
            .indexes_handle(tablet(2))
            .entries("idx_x")
            .contains_key(&key(999)));
        assert_eq!(
            fixture
                .tablets
                .indexes_handle(tablet(2))
                .entries("idx_x")
                .len(),
            5
        );
        assert!(resumed.validation_report(job_id).expect("report").passed);
    }

    #[test]
    fn phase_graph_is_enforced() {
        let edges: [(DdlPhase, DdlPhase); 5] = [
            (DdlPhase::Pending, DdlPhase::WriteOnly),
            (DdlPhase::WriteOnly, DdlPhase::Backfilling),
            (DdlPhase::Backfilling, DdlPhase::Validating),
            (DdlPhase::Validating, DdlPhase::Public),
            (DdlPhase::Public, DdlPhase::Dropping),
        ];
        for from in DdlPhase::ALL {
            for to in DdlPhase::ALL {
                assert_eq!(
                    from.can_transition(to),
                    edges.contains(&(from, to)),
                    "{from} -> {to}",
                );
            }
        }

        // The store refuses off-graph and bypass-PublishJob moves.
        let fixture = Fixture::new(1, 1);
        let job_id = fixture.submit_add("idx_g").expect("submit");
        fixture.drive_until(job_id, |step| matches!(step, DriveStep::Admitted));
        let skipped = fixture
            .driver
            .advance_phase(job_id, DdlPhase::Validating, None);
        assert!(matches!(
            skipped,
            Err(DdlError::Rejection(DdlRejection::IllegalPhaseTransition {
                from: DdlPhase::Pending,
                to: DdlPhase::Validating,
                ..
            }))
        ));
        fixture.drive_to_backfilled(job_id, 1);
        fixture.drive_until(job_id, |step| {
            matches!(
                step,
                DriveStep::PhaseAdvanced {
                    from: DdlPhase::Backfilling,
                    to: DdlPhase::Validating,
                }
            )
        });
        let bypass = fixture.driver.advance_phase(job_id, DdlPhase::Public, None);
        assert!(
            matches!(
                bypass,
                Err(DdlError::Rejection(DdlRejection::Meta(
                    MetaRejectionReason::Invalid { .. }
                )))
            ),
            "Public rides PublishJob: {bypass:?}",
        );
    }

    #[test]
    fn admin_state_graph_is_enforced() {
        let fixture = Fixture::new(1, 1);
        let job_id = fixture.submit_add("idx_a").expect("submit");
        assert!(
            fixture.driver.pause_job(job_id).is_err(),
            "Pending cannot pause"
        );
        assert!(
            fixture.driver.resume_job(job_id).is_err(),
            "Pending cannot resume"
        );
        fixture
            .driver
            .cancel_job(job_id)
            .expect("cancel from Pending");
        assert_eq!(
            fixture.driver.drive_job(job_id).expect("drive"),
            DriveOutcome::RolledBack
        );
        let job = fixture.store().job(job_id).expect("job").clone();
        assert_eq!(job.state, SchemaJobState::Failed);
        assert!(
            fixture.driver.cancel_job(job_id).is_err(),
            "terminal jobs refuse cancel"
        );
    }

    #[test]
    fn alter_schema_publishes_the_next_schema_version_atomically() {
        let fixture = Fixture::new(2, 3);
        let target = serde_json::json!({"columns": ["id", "v", "w"]});
        let job_id = fixture
            .driver
            .submit_alter_schema(database(), table(), target.clone(), SchemaVersion(1))
            .expect("submit alter");
        assert_eq!(
            fixture.driver.drive_job(job_id).expect("drive"),
            DriveOutcome::Completed
        );
        let store = fixture.store();
        let anchor = store.table_anchor(table()).expect("anchor");
        assert_eq!(anchor.schema_version, SchemaVersion(2));
        assert_eq!(anchor.schema, target);
        let job = store.job(job_id).expect("job");
        assert_eq!(job.state, SchemaJobState::Succeeded);
        assert_eq!(job.phase, DdlPhase::Public);
        assert_eq!(job.tablet_progress.len(), 2);
        drop(store);

        // The old schema version is now stale for new jobs; the new one works.
        let stale = fixture.submit_add("idx_old");
        assert!(matches!(
            stale,
            Err(DdlError::Rejection(
                DdlRejection::SchemaVersionMismatch { .. }
            ))
        ));
        let fresh = fixture
            .driver
            .submit_add_index(database(), table(), "idx_new", spec(), SchemaVersion(2))
            .expect("submit at v2");
        assert_eq!(
            fixture.driver.drive_job(fresh).expect("drive"),
            DriveOutcome::Completed
        );
    }

    #[test]
    fn topology_change_mid_backfill_is_reconciled() {
        let fixture = Fixture::new(3, 3);
        let job_id = fixture.submit_add("idx_t").expect("submit");
        fixture.drive_to_backfilled(job_id, 1);
        // A split adds tablet 4; a merge retires tablet 3.
        fixture.tablets.add_tablet(tablet(4), table());
        let keyspace = fixture.tablets.keyspace_handle(tablet(4));
        for row in 1..=3 {
            keyspace.insert(key(row + 100), ts(row), value(row + 100));
        }
        fixture.tablets.remove_tablet(tablet(3));
        assert_eq!(
            fixture.driver.drive_job(job_id).expect("drive"),
            DriveOutcome::Completed
        );
        let status = fixture.driver.job_status(job_id).expect("status");
        assert_eq!(status.tablets_registered, 3);
        assert!(status.record.tablet_progress.contains_key(&tablet(4)));
        assert!(!status.record.tablet_progress.contains_key(&tablet(3)));
        assert!(
            fixture
                .driver
                .validation_report(job_id)
                .expect("report")
                .passed
        );
    }

    #[test]
    fn conflicting_and_duplicate_submissions_are_refused() {
        let fixture = Fixture::new(1, 2);
        let job_id = fixture.submit_add("idx_1").expect("submit");
        // One active job per table.
        let concurrent = fixture.submit_add("idx_2");
        assert!(matches!(
            concurrent,
            Err(DdlError::Rejection(DdlRejection::Meta(
                MetaRejectionReason::Conflict { .. }
            )))
        ));
        assert_eq!(
            fixture.driver.drive_job(job_id).expect("drive"),
            DriveOutcome::Completed
        );
        // A completed index name cannot be re-added.
        let duplicate = fixture.submit_add("idx_1");
        assert!(matches!(
            duplicate,
            Err(DdlError::Rejection(DdlRejection::Meta(
                MetaRejectionReason::Conflict { .. }
            )))
        ));
        // Dropping a missing index fails closed.
        let missing =
            fixture
                .driver
                .submit_drop_index(database(), table(), "idx_missing", SchemaVersion(1));
        assert!(matches!(
            missing,
            Err(DdlError::Rejection(DdlRejection::Meta(
                MetaRejectionReason::NotFound { .. }
            )))
        ));
        // Identical command replay is idempotent (raft retry).
        let job = fixture.store().job(job_id).expect("job").clone();
        fixture
            .driver
            .apply(DdlCommand::SubmitJob { job })
            .expect("identical replay is a no-op");
        assert_eq!(fixture.store().jobs.len(), 1);
    }

    #[test]
    fn store_roundtrips_through_json() {
        let fixture = Fixture::new(2, 3);
        let job_id = fixture.submit_add("idx_j").expect("submit");
        fixture.drive_to_backfilled(job_id, 1);
        let snapshot = {
            let store = fixture.store();
            serde_json::to_string(&*store).expect("encode")
        };
        let decoded: DdlJobStore = serde_json::from_str(&snapshot).expect("decode");
        assert_eq!(decoded, *fixture.store());
    }

    #[test]
    fn command_roundtrips_through_json() {
        let job = DdlJobRecord {
            job_id: 9,
            database_id: database(),
            table_id: table(),
            kind: DdlJobKind::AddIndex,
            state: SchemaJobState::Pending,
            phase: DdlPhase::Pending,
            definition: DdlDefinition::AddIndex {
                index_name: "idx_serde".to_owned(),
                spec: spec(),
            },
            source_schema_version: SchemaVersion(1),
            created_at: ts(10),
            updated_at: ts(10),
            pinned_snapshot: None,
            tablet_progress: BTreeMap::new(),
            error: None,
            metadata_version: MetadataVersion::ZERO,
        };
        let command = DdlCommand::SubmitJob { job };
        let encoded = serde_json::to_string(&command).expect("encode");
        let decoded: DdlCommand = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(decoded, command);
    }
}
