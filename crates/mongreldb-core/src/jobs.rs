//! Persistent online jobs (spec section 10.6, S1F-002/S1F-003).
//!
//! This module delivers the Stage 1F job framework: the S1F-002 job state
//! machine with a persisted registry, and the S1F-003 build-and-publish
//! driver ([`run_build_publish`]) as a reusable protocol for every
//! [`JobKind`] (index builds, backfills, validation, …).
//!
//! **Design (landed):** the framework supports synchronous drivers and
//! surface-owned executors. Online index DDL persists `Pending`, returns its
//! id, and drives this protocol on a named background thread. Other job kinds
//! may still be advanced synchronously by their owning surface. Durable phase
//! checkpoints make either model reconstructible after crash or pause.
//!
//! # State machine (S1F-002)
//!
//! [`JobState`] is exactly the spec's seven states. Legal transitions are
//! enforced by [`JobState::can_transition`]; every registry mutation goes
//! through it and illegal transitions return [`JobError::IllegalTransition`]:
//!
//! ```text
//! Pending     -> Running       (admission, concurrency-bounded)
//! Pending     -> Cancelling    (cancel before start)
//! Running     -> Paused        (operator pause; takes effect at phase boundary)
//! Running     -> Cancelling    (operator cancel; cooperative)
//! Running     -> RollingBack   (a build phase failed)
//! Running     -> Succeeded     (all phases complete)
//! Paused      -> Pending       (resume: requeue for the next drive)
//! Paused      -> Cancelling    (cancel a parked job)
//! Cancelling  -> RollingBack   (worker observed the cancel, cleaning up)
//! Cancelling  -> Failed        (cancel completed without rollback work)
//! RollingBack -> Failed        (rollback finished; terminal)
//! ```
//!
//! `Succeeded` and `Failed` are terminal. There is no in-place retry edge:
//! a failed job is resubmitted as a new job id.
//!
//! # Persistence
//!
//! The registry is mirrored to a sibling `JOBS` file next to `CATALOG`,
//! written through [`crate::durable_file::DurableRoot::write_atomic`] — the
//! same temp-write + fsync + atomic rename + parent-dir fsync path the
//! catalog checkpoint uses (review fix #19), so a crash never leaves a
//! half-linked registry. The frame mirrors `catalog.rs`: an 8-byte magic, a
//! SHA-256 integrity tag over the body (or AES-256-GCM via the database
//! `meta_dek` when the `encryption` feature is active), and a versioned
//! serde_json envelope. `catalog.rs` is not modified; the jobs file is an
//! independent sibling checkpoint.
//!
//! Two deliberate deviations from `catalog.rs`'s read path, both failing
//! closed (the catalog predates the jobs file and keeps its lenient
//! tamper-means-absent behavior for legacy opens; a jobs registry that
//! silently vanishes could orphan schema state, so corruption is an error):
//!
//! - a checksum/authentication mismatch is [`JobError::Storage`], not `None`;
//! - a missing file alone means "no jobs yet" and opens empty.
//!
//! Every state mutation rewrites the file before returning, so the durable
//! record is never behind the in-memory one (mutations are applied to a
//! clone, persisted, then swapped in).
//!
//! # Crash recovery
//!
//! Recovery runs at [`JobRegistry::open`] and is persisted immediately:
//!
//! - `Running -> Paused`: no worker survives a process crash. The job parks
//!   with its last durable checkpoint; [`JobRegistry::resume`] requeues it
//!   and the next [`run_build_publish`] drive resumes from that checkpoint.
//! - `Cancelling -> Failed`: the crash itself completed the cancellation.
//! - `RollingBack -> Failed`: the crash interrupted rollback; the recorded
//!   error is annotated. Unpublished generations orphaned by an interrupted
//!   rollback are reclaimed by the publish/GC paths of the driving surface.
//!
//! `Paused`, `Pending`, `Succeeded`, and `Failed` records reopen unchanged.
//!
//! # Cooperative cancellation and pause
//!
//! Every job owns a [`CancellationToken`]. `cancel()` on a `Running` job
//! moves it to `Cancelling` and sets the token; the running job observes the
//! token (via [`JobContext::check_cancelled`]) or the next phase-boundary
//! state check, rolls back, and lands in `Failed`. Cancelling a `Pending` or
//! `Paused` job has no live worker to notify, so the registry completes the
//! cancellation synchronously (`Pending|Paused -> Cancelling -> Failed`).
//! `pause()` moves `Running -> Paused`; the drive finishes any in-flight
//! phase, persists that phase's checkpoint (progress is monotonic), and
//! parks at the next phase boundary. [`JobRegistry::resume`] refuses to
//! requeue a job whose previous drive is still draining in this process
//! ([`JobError::DriveActive`]).
//!
//! # Build-and-publish driver (S1F-003)
//!
//! [`run_build_publish`] drives a [`BuildPublishJob`] through the spec's
//! seven phases in order: record pending definition, pin snapshot, build
//! hidden generation, catch up committed deltas, validate, publish
//! atomically, release old generation after pins drop. After every completed
//! phase the driver persists a checkpoint (completed-phase count plus the
//! job's opaque [`BuildPublishJob::checkpoint_state`]); on resume, completed
//! phases are skipped.
//!
//! Phase contract:
//!
//! - Phases must be idempotent. A fault or crash between phase completion
//!   and checkpoint persistence re-runs the phase on resume.
//! - Publish must be atomic: the hidden generation becomes visible in one
//!   operation, so re-running any pre-publish phase is harmless.
//! - A phase returning an error moves the job `Running -> RollingBack ->
//!   Failed` and invokes [`BuildPublishJob::rollback`] between the two.
//! - An injected fault (see below) parks the job as `Paused` instead:
//!   transient environmental failures are resumable, while errors the phase
//!   itself reports are job-logic failures. A durable-write error aborts the
//!   drive with [`JobError::Storage`]/[`JobError::Io`]; crash recovery on the
//!   next open then parks the job (`Running -> Paused`) for resume.
//!
//! # Fault-injection hooks (documented FND-006 extension)
//!
//! The driver fires `job.<phase>.before` / `job.<phase>.after` at every phase
//! boundary, extending the section 9.6 catalog:
//!
//! - `job.record_pending.before` / `job.record_pending.after`
//! - `job.pin_snapshot.before` / `job.pin_snapshot.after`
//! - `job.build_hidden.before` / `job.build_hidden.after`
//! - `job.catch_up.before` / `job.catch_up.after`
//! - `job.validate.before` / `job.validate.after`
//! - `job.publish.before` / `job.publish.after`
//! - `job.release_old.before` / `job.release_old.after`
//!
//! `before` fires before the phase body runs (a failure skips the phase and
//! parks the job); `after` fires after the phase body succeeded but before
//! its checkpoint is durable (a failure re-runs the phase on resume).

use parking_lot::{Condvar, Mutex};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::catalog::META_DEK_LEN;
use crate::durable_file::DurableRoot;
use crate::error::MongrelError;

/// The sibling-of-`CATALOG` file mirroring the job registry.
pub const JOBS_FILENAME: &str = "JOBS";
const MAGIC: &[u8; 8] = b"MONGRJOB";
const JOBS_FORMAT_VERSION: u16 = 1;
/// Upper bound on one registry file, mirroring the catalog's 64 MiB cap
/// (spec section 4.9: every resource is bounded).
const MAX_JOBS_BYTES: u64 = 64 * 1024 * 1024;
/// Upper bound on one job's opaque checkpoint payload.
const MAX_CHECKPOINT_BYTES: usize = 1024 * 1024;
/// Upper bound on one job's durable, driver-defined submission payload.
const MAX_DEFINITION_BYTES: usize = 1024 * 1024;
/// Default bound on concurrently active (worker-holding) jobs.
pub const DEFAULT_MAX_CONCURRENT_JOBS: usize = 2;

/// Persistent job states (spec S1F-002, exact).
///
/// Serde encodes variants by name; names are part of the durable contract
/// and must never change or be reused (spec section 4.10).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobState {
    /// Submitted, waiting for admission.
    Pending,
    /// Admitted; a worker is actively driving it.
    Running,
    /// Parked by an operator pause or by crash recovery; resumes from the
    /// last durable checkpoint.
    Paused,
    /// Cancellation requested; the live worker has not finished rolling back.
    Cancelling,
    /// Terminal: every phase completed and published.
    Succeeded,
    /// Terminal: failed or cancelled; `error` on the record says why.
    Failed,
    /// A phase failed or a cancel was observed; rollback is in progress.
    RollingBack,
}

impl JobState {
    /// Every legal state, for exhaustive graph tests.
    #[cfg(test)]
    pub(crate) const ALL: [JobState; 7] = [
        JobState::Pending,
        JobState::Running,
        JobState::Paused,
        JobState::Cancelling,
        JobState::Succeeded,
        JobState::Failed,
        JobState::RollingBack,
    ];

    /// Terminal states have no outgoing edges.
    pub fn is_terminal(self) -> bool {
        matches!(self, JobState::Succeeded | JobState::Failed)
    }

    /// Whether the `self -> next` edge exists in the documented graph
    /// (module-level docs). This is the single enforcement point: every
    /// registry mutation checks it before applying a transition.
    pub fn can_transition(self, next: JobState) -> bool {
        use JobState::{Cancelling, Failed, Paused, Pending, RollingBack, Running, Succeeded};
        matches!(
            (self, next),
            (Pending, Running)
                | (Pending, Cancelling)
                | (Running, Paused)
                | (Running, Cancelling)
                | (Running, RollingBack)
                | (Running, Succeeded)
                | (Paused, Pending)
                | (Paused, Cancelling)
                | (Cancelling, RollingBack)
                | (Cancelling, Failed)
                | (RollingBack, Failed)
        )
    }
}

/// The S1F-002 job kinds (spec section 10.6). The framework is kind-agnostic
/// beyond light target validation in [`JobRegistry::submit`]; each kind is
/// driven through [`run_build_publish`] (or an equivalent phase driver) by
/// the calling surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobKind {
    /// Online secondary-index build (the S1F-003 reference case).
    IndexBuild,
    /// Backfill of a newly added or altered column.
    ColumnBackfill,
    /// Validation of existing rows against a new schema constraint.
    SchemaValidation,
    /// Rebuild of a materialized view's physical table.
    MaterializedViewRebuild,
    /// Rotation of encryption key hierarchy material.
    KeyRotation,
    /// Bulk import too large for the interactive write path.
    LargeImport,
}

/// Table/index identifiers a job operates on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobTarget {
    /// Name of the table the job operates on.
    pub table: String,
    /// Index (or materialized-view) name for kinds that have one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
}

/// Job progress: a normalized fraction plus the raw units it derives from.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobProgress {
    /// Completion in `[0.0, 1.0]`.
    pub fraction: f64,
    /// Work units completed.
    pub done: u64,
    /// Total work units; `0` means "not yet measurable" (fraction leads).
    pub total: u64,
}

impl Default for JobProgress {
    fn default() -> Self {
        Self {
            fraction: 0.0,
            done: 0,
            total: 0,
        }
    }
}

impl JobProgress {
    /// Validated constructor: `fraction` must lie in `[0.0, 1.0]` (NaN is
    /// rejected), and a nonzero `total` must cover `done`.
    pub fn new(fraction: f64, done: u64, total: u64) -> Result<Self, JobError> {
        if !(0.0..=1.0).contains(&fraction) {
            return Err(JobError::InvalidProgress(format!(
                "fraction {fraction} is outside [0.0, 1.0]"
            )));
        }
        if total > 0 && done > total {
            return Err(JobError::InvalidProgress(format!(
                "done {done} exceeds total {total}"
            )));
        }
        Ok(Self {
            fraction,
            done,
            total,
        })
    }
}

/// One persisted job record. `created_at_micros`/`updated_at_micros` are
/// wall-clock Unix microseconds for operator diagnostics — job metadata, not
/// MVCC visibility timestamps (those are the HLC authority's domain).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobRecord {
    /// Registry-allocated id; never reused within a database (spec section 7).
    pub job_id: u64,
    pub kind: JobKind,
    pub state: JobState,
    pub target: JobTarget,
    /// Complete versioned driver definition needed to reconstruct this job
    /// after restart. Generic jobs may omit it; resumable production drivers
    /// should not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition: Option<Vec<u8>>,
    pub progress: JobProgress,
    /// Wall-clock creation time, microseconds since the Unix epoch.
    pub created_at_micros: u64,
    /// Wall-clock time of the last durable mutation, same units.
    pub updated_at_micros: u64,
    /// Terminal failure detail (also set for cancellations).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Opaque resume-after-restart payload written by the active driver.
    /// Cleared on success; retained after failure for diagnostics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<Vec<u8>>,
}

/// Typed errors of the jobs framework. `From<JobError> for MongrelError`
/// maps them onto the engine error taxonomy for callers.
#[derive(Debug, thiserror::Error)]
pub enum JobError {
    /// No record with this id exists.
    #[error("job {job_id} not found")]
    NotFound {
        /// The unknown id.
        job_id: u64,
    },
    /// A transition the documented graph (module docs) does not allow.
    #[error("illegal job state transition from {from:?} to {to:?}")]
    IllegalTransition {
        /// Current state.
        from: JobState,
        /// Attempted target state.
        to: JobState,
    },
    /// A mutation that requires the job to be in a specific state.
    #[error("job {job_id} is {actual:?}, expected {expected:?}")]
    UnexpectedState {
        /// The job.
        job_id: u64,
        /// Required state.
        expected: JobState,
        /// Observed state.
        actual: JobState,
    },
    /// Admission would exceed the configured active-job bound.
    #[error("concurrent job limit reached: {active} active of {limit} allowed")]
    ConcurrencyLimit {
        /// Currently active jobs.
        active: usize,
        /// Configured bound.
        limit: usize,
    },
    /// A resume was attempted while a drive is still draining in this
    /// process; wait for it to park before requeuing the job.
    #[error("job {job_id} still has a live drive in this process")]
    DriveActive {
        /// The busy job.
        job_id: u64,
    },
    /// Progress values failed validation.
    #[error("invalid job progress: {0}")]
    InvalidProgress(String),
    /// The job's cancellation token fired (cooperative cancellation).
    #[error("job cancelled")]
    Cancelled,
    /// A build phase reported a job-logic failure.
    #[error("job phase failed: {0}")]
    Phase(String),
    /// A build phase was denied a bounded resource reservation.
    #[error("job resource limit exceeded for {resource}: requested {requested}, limit {limit}")]
    ResourceLimitExceeded {
        resource: &'static str,
        requested: usize,
        limit: usize,
    },
    /// A named fault-injection hook fired at a phase boundary.
    #[error("injected fault: {0}")]
    InjectedFault(String),
    /// The checkpoint payload exceeds [`MAX_CHECKPOINT_BYTES`].
    #[error("job checkpoint of {bytes} bytes exceeds the {limit}-byte limit")]
    CheckpointTooLarge {
        /// Attempted size.
        bytes: usize,
        /// Configured bound.
        limit: usize,
    },
    /// Durable registry file errors: corruption, tampering, unsupported
    /// format version, or serialization failure. Always fail-closed.
    #[error("job registry storage: {0}")]
    Storage(String),
    /// Filesystem failure on the registry file.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// A state waiter reached its deadline before the requested state.
    #[error("timed out waiting for job {job_id} to become terminal")]
    WaitTimeout { job_id: u64 },
}

impl From<JobError> for MongrelError {
    fn from(error: JobError) -> Self {
        match error {
            JobError::NotFound { job_id } => MongrelError::NotFound(format!("job {job_id}")),
            JobError::Cancelled => MongrelError::Cancelled,
            JobError::ConcurrencyLimit { active, limit } => MongrelError::ResourceLimitExceeded {
                resource: "concurrent jobs",
                requested: active,
                limit,
            },
            JobError::InvalidProgress(message) => MongrelError::InvalidArgument(message),
            JobError::ResourceLimitExceeded {
                resource,
                requested,
                limit,
            } => MongrelError::ResourceLimitExceeded {
                resource,
                requested,
                limit,
            },
            JobError::Io(error) => MongrelError::Io(error),
            JobError::WaitTimeout { .. } => MongrelError::DeadlineExceeded,
            other => MongrelError::Other(other.to_string()),
        }
    }
}

/// Cooperative cancellation handle for one job. Clones share the flag.
///
/// Tokens are process-local: they are not persisted. A crash parks the job
/// (see module docs), so a cancel intent must be re-issued after restart.
#[derive(Debug, Clone, Default)]
pub struct CancellationToken {
    flag: Arc<AtomicBool>,
}

impl CancellationToken {
    /// Whether cancellation was requested.
    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }

    /// Request cancellation. Idempotent.
    pub fn cancel(&self) {
        self.flag.store(true, Ordering::Release);
    }

    /// [`JobError::Cancelled`] when cancelled, `Ok(())` otherwise. Job phases
    /// call this between work batches.
    pub fn check(&self) -> Result<(), JobError> {
        if self.is_cancelled() {
            Err(JobError::Cancelled)
        } else {
            Ok(())
        }
    }
}

/// The S1F-003 build-and-publish phases, in protocol order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildPhase {
    /// 1. Record the pending definition (e.g. hidden index definition).
    RecordPending,
    /// 2. Pin a snapshot so the build sees a stable row set.
    PinSnapshot,
    /// 3. Build the hidden generation from the pinned snapshot.
    BuildHidden,
    /// 4. Catch up deltas committed since the snapshot was pinned.
    CatchUp,
    /// 5. Validate the built generation (counts, hashes, constraints).
    Validate,
    /// 6. Publish the new generation atomically.
    Publish,
    /// 7. Release the old generation once no snapshot pins it.
    ReleaseOld,
}

impl BuildPhase {
    /// Every phase in protocol order.
    pub const ALL: [BuildPhase; 7] = [
        BuildPhase::RecordPending,
        BuildPhase::PinSnapshot,
        BuildPhase::BuildHidden,
        BuildPhase::CatchUp,
        BuildPhase::Validate,
        BuildPhase::Publish,
        BuildPhase::ReleaseOld,
    ];

    /// Stable lowercase label (used in hook names and diagnostics).
    pub fn label(self) -> &'static str {
        match self {
            BuildPhase::RecordPending => "record_pending",
            BuildPhase::PinSnapshot => "pin_snapshot",
            BuildPhase::BuildHidden => "build_hidden",
            BuildPhase::CatchUp => "catch_up",
            BuildPhase::Validate => "validate",
            BuildPhase::Publish => "publish",
            BuildPhase::ReleaseOld => "release_old",
        }
    }

    /// The `job.<phase>.before` hook fired before the phase body runs.
    pub fn before_hook(self) -> &'static str {
        match self {
            BuildPhase::RecordPending => "job.record_pending.before",
            BuildPhase::PinSnapshot => "job.pin_snapshot.before",
            BuildPhase::BuildHidden => "job.build_hidden.before",
            BuildPhase::CatchUp => "job.catch_up.before",
            BuildPhase::Validate => "job.validate.before",
            BuildPhase::Publish => "job.publish.before",
            BuildPhase::ReleaseOld => "job.release_old.before",
        }
    }

    /// The `job.<phase>.after` hook fired after the phase body succeeded and
    /// before its checkpoint is durable.
    pub fn after_hook(self) -> &'static str {
        match self {
            BuildPhase::RecordPending => "job.record_pending.after",
            BuildPhase::PinSnapshot => "job.pin_snapshot.after",
            BuildPhase::BuildHidden => "job.build_hidden.after",
            BuildPhase::CatchUp => "job.catch_up.after",
            BuildPhase::Validate => "job.validate.after",
            BuildPhase::Publish => "job.publish.after",
            BuildPhase::ReleaseOld => "job.release_old.after",
        }
    }

    fn invoke<J: BuildPublishJob + ?Sized>(
        self,
        job: &mut J,
        context: &JobContext,
    ) -> Result<(), JobError> {
        match self {
            BuildPhase::RecordPending => job.record_pending(context),
            BuildPhase::PinSnapshot => job.pin_snapshot(context),
            BuildPhase::BuildHidden => job.build_hidden(context),
            BuildPhase::CatchUp => job.catch_up(context),
            BuildPhase::Validate => job.validate(context),
            BuildPhase::Publish => job.publish(context),
            BuildPhase::ReleaseOld => job.release_old(context),
        }
    }
}

/// A build-and-publish job (spec S1F-003). Implementors perform the actual
/// work per phase; [`run_build_publish`] sequences, checkpoints, and
/// recovers. All methods default to no-ops so kinds that skip a phase (a
/// large import has no old generation to release) implement only what they
/// need. Phases must be idempotent — see the module-level phase contract.
pub trait BuildPublishJob {
    /// Opaque implementor state persisted inside the job checkpoint after
    /// every completed phase (bounded by [`MAX_CHECKPOINT_BYTES`]).
    fn checkpoint_state(&self) -> Vec<u8> {
        Vec::new()
    }

    /// Restore state previously returned by [`Self::checkpoint_state`]
    /// during a resume. Called once before the first uncompleted phase runs.
    fn restore_checkpoint(&mut self, _state: &[u8]) -> Result<(), JobError> {
        Ok(())
    }

    /// Phase 1: record the pending definition (S1F-003 step 1).
    fn record_pending(&mut self, _context: &JobContext) -> Result<(), JobError> {
        Ok(())
    }

    /// Phase 2: pin a snapshot for a stable build view (S1F-003 step 2).
    fn pin_snapshot(&mut self, _context: &JobContext) -> Result<(), JobError> {
        Ok(())
    }

    /// Phase 3: build the hidden generation from the pinned snapshot
    /// (S1F-003 step 3). Nothing built here is visible to readers yet.
    fn build_hidden(&mut self, _context: &JobContext) -> Result<(), JobError> {
        Ok(())
    }

    /// Phase 4: fold in deltas committed after the snapshot pin (S1F-003
    /// step 4), so the generation is current at publish time.
    fn catch_up(&mut self, _context: &JobContext) -> Result<(), JobError> {
        Ok(())
    }

    /// Phase 5: validate the generation before it becomes visible (S1F-003
    /// step 5).
    fn validate(&mut self, _context: &JobContext) -> Result<(), JobError> {
        Ok(())
    }

    /// Phase 6: publish the generation atomically (S1F-003 step 6). After
    /// this phase the new generation is authoritative for new readers.
    fn publish(&mut self, _context: &JobContext) -> Result<(), JobError> {
        Ok(())
    }

    /// Phase 7: release the old generation after all snapshot pins on it
    /// drop (S1F-003 step 7).
    fn release_old(&mut self, _context: &JobContext) -> Result<(), JobError> {
        Ok(())
    }

    /// Best-effort cleanup of unpublished state, invoked between
    /// `RollingBack` and `Failed` when a phase errors or a cancel lands.
    /// A failure here is recorded on the job record, not propagated.
    fn rollback(&mut self) -> Result<(), JobError> {
        Ok(())
    }
}

/// The handle a running [`BuildPublishJob`] phase receives: job identity,
/// the cooperative cancellation token, and progress reporting back into the
/// durable record.
pub struct JobContext<'a> {
    registry: &'a JobRegistry,
    job_id: u64,
    token: CancellationToken,
}

impl JobContext<'_> {
    /// The id of the job being driven.
    pub fn job_id(&self) -> u64 {
        self.job_id
    }

    /// The job's cancellation token.
    pub fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// `Err(JobError::Cancelled)` once the operator cancelled the job.
    pub fn check_cancelled(&self) -> Result<(), JobError> {
        self.token.check()
    }

    /// Persist unit-based progress (`fraction` is derived as `done/total`).
    /// Requires the job to be `Running`; a pause landing mid-phase surfaces
    /// here as [`JobError::UnexpectedState`] so the phase can stop early.
    pub fn report_progress(&self, done: u64, total: u64) -> Result<(), JobError> {
        if total == 0 {
            return Err(JobError::InvalidProgress(
                "unit progress requires a nonzero total".to_string(),
            ));
        }
        let fraction = done as f64 / total as f64;
        self.registry
            .update_progress(self.job_id, JobProgress::new(fraction, done, total)?)
    }
}

/// The durable registry file body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JobsSnapshot {
    /// Next job id to allocate; strictly greater than every live id.
    next_job_id: u64,
    jobs: Vec<JobRecord>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JobsEnvelope {
    format_version: u16,
    registry: JobsSnapshot,
}

#[derive(Debug, Clone, Default)]
struct RegistryInner {
    next_job_id: u64,
    jobs: BTreeMap<u64, JobRecord>,
}

/// The persistent job registry: state machine, durability, admission, and
/// cooperative-cancellation tokens for every job in one database.
///
/// The registry takes no lock of its own: the database directory's
/// `_meta/.lock` (held by the owning `Database`) already rejects concurrent
/// independent handles, and `Database::open` constructs exactly one registry
/// per storage core.
pub struct JobRegistry {
    root: DurableRoot,
    meta_dek: Option<[u8; META_DEK_LEN]>,
    inner: Mutex<RegistryInner>,
    tokens: Mutex<HashMap<u64, CancellationToken>>,
    /// Process-local set of jobs with a live [`run_build_publish`] drive.
    /// `resume()` consults it so a paused job cannot be requeued while its
    /// previous drive is still draining toward the park point.
    active_drives: Mutex<HashSet<u64>>,
    state_changed: Condvar,
    max_concurrent_jobs: usize,
}

impl std::fmt::Debug for JobRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JobRegistry")
            .field("root", &self.root)
            .field("max_concurrent_jobs", &self.max_concurrent_jobs)
            .finish_non_exhaustive()
    }
}

impl JobRegistry {
    /// Open (or create) the registry stored in the database directory `dir`.
    /// Applies crash recovery (module docs) and persists it before returning.
    ///
    /// `meta_dek` mirrors the catalog: `Some` seals the file with the
    /// database metadata key, `None` writes the integrity-tagged plaintext
    /// frame. Without the `encryption` feature, `Some` is rejected.
    pub fn open(dir: &Path, meta_dek: Option<&[u8; META_DEK_LEN]>) -> Result<Self, JobError> {
        let root = DurableRoot::open(dir)?;
        let mut inner = match read_durable(&root, meta_dek)? {
            Some(snapshot) => validate_snapshot(snapshot)?,
            None => RegistryInner {
                next_job_id: 1,
                jobs: BTreeMap::new(),
            },
        };
        let recovered = recover_after_crash(&mut inner);
        let registry = Self {
            root,
            meta_dek: meta_dek.copied(),
            inner: Mutex::new(inner),
            tokens: Mutex::new(HashMap::new()),
            active_drives: Mutex::new(HashSet::new()),
            state_changed: Condvar::new(),
            max_concurrent_jobs: DEFAULT_MAX_CONCURRENT_JOBS,
        };
        if recovered {
            registry.persist_locked(&registry.inner.lock())?;
        }
        Ok(registry)
    }

    /// Builder: override the active-job bound (minimum 1).
    pub fn with_max_concurrent_jobs(mut self, limit: usize) -> Self {
        self.max_concurrent_jobs = limit.max(1);
        self
    }

    /// Submit a new job in `Pending` and persist it before returning its id.
    pub fn submit(&self, kind: JobKind, target: JobTarget) -> Result<u64, JobError> {
        self.submit_with_definition(kind, target, None)
    }

    /// Submit a new job with the complete versioned driver definition.
    ///
    /// The payload is persisted in the initial `Pending` record, before the
    /// id is returned, so a restart can reconstruct work that never began.
    pub fn submit_with_definition(
        &self,
        kind: JobKind,
        target: JobTarget,
        definition: Option<Vec<u8>>,
    ) -> Result<u64, JobError> {
        if target.table.is_empty() {
            return Err(JobError::InvalidProgress(
                "job target table must not be empty".to_string(),
            ));
        }
        if kind == JobKind::IndexBuild && target.index.is_none() {
            return Err(JobError::InvalidProgress(
                "an index-build job requires a target index".to_string(),
            ));
        }
        if definition
            .as_ref()
            .is_some_and(|payload| payload.len() > MAX_DEFINITION_BYTES)
        {
            return Err(JobError::CheckpointTooLarge {
                bytes: definition.as_ref().map_or(0, Vec::len),
                limit: MAX_DEFINITION_BYTES,
            });
        }
        let mut next = self.inner.lock().clone();
        let now = unix_micros();
        let job_id = next.next_job_id;
        next.next_job_id = next
            .next_job_id
            .checked_add(1)
            .ok_or_else(|| JobError::Storage("job id space exhausted".to_string()))?;
        next.jobs.insert(
            job_id,
            JobRecord {
                job_id,
                kind,
                state: JobState::Pending,
                target,
                definition,
                progress: JobProgress::default(),
                created_at_micros: now,
                updated_at_micros: now,
                error: None,
                checkpoint: None,
            },
        );
        self.persist_and_swap(next)?;
        self.tokens
            .lock()
            .insert(job_id, CancellationToken::default());
        Ok(job_id)
    }

    /// A snapshot of one record, if it exists.
    pub fn get(&self, job_id: u64) -> Option<JobRecord> {
        self.inner.lock().jobs.get(&job_id).cloned()
    }

    /// Every record, ordered by job id.
    pub fn list(&self) -> Vec<JobRecord> {
        self.inner.lock().jobs.values().cloned().collect()
    }

    /// Wait until one job reaches a terminal state without polling sleeps.
    pub fn wait_terminal(
        &self,
        job_id: u64,
        timeout: std::time::Duration,
    ) -> Result<JobRecord, JobError> {
        let deadline = std::time::Instant::now() + timeout;
        let mut inner = self.inner.lock();
        loop {
            let record = inner
                .jobs
                .get(&job_id)
                .ok_or(JobError::NotFound { job_id })?;
            if record.state.is_terminal() {
                return Ok(record.clone());
            }
            if self
                .state_changed
                .wait_until(&mut inner, deadline)
                .timed_out()
            {
                return Err(JobError::WaitTimeout { job_id });
            }
        }
    }

    /// The job's cooperative cancellation token, if the job exists.
    pub fn cancellation_token(&self, job_id: u64) -> Option<CancellationToken> {
        if !self.inner.lock().jobs.contains_key(&job_id) {
            return None;
        }
        Some(self.tokens.lock().entry(job_id).or_default().clone())
    }

    /// Park a running job (`Running -> Paused`). The live drive finishes any
    /// in-flight phase, persists its checkpoint, and stops at the next phase
    /// boundary.
    pub fn pause(&self, job_id: u64) -> Result<(), JobError> {
        self.transition(job_id, JobState::Paused)
    }

    /// Requeue a parked job (`Paused -> Pending`). The next
    /// [`run_build_publish`] drive admits it and resumes from its checkpoint.
    /// Refused with [`JobError::DriveActive`] while the previous drive is
    /// still draining in this process — join it first.
    pub fn resume(&self, job_id: u64) -> Result<(), JobError> {
        if self.active_drives.lock().contains(&job_id) {
            return Err(JobError::DriveActive { job_id });
        }
        self.transition(job_id, JobState::Pending)
    }

    /// Cancel a job.
    ///
    /// - `Pending`/`Paused`: no worker is live, so the registry completes
    ///   the cancellation synchronously (`-> Cancelling -> Failed`).
    /// - `Running`: moves to `Cancelling` and sets the token; the live drive
    ///   finishes the cancellation cooperatively (`-> RollingBack ->
    ///   Failed`).
    /// - `Cancelling`/`RollingBack`: already heading to `Failed`; a no-op.
    /// - Terminal states: [`JobError::IllegalTransition`].
    pub fn cancel(&self, job_id: u64) -> Result<(), JobError> {
        let state = self.get(job_id).ok_or(JobError::NotFound { job_id })?.state;
        match state {
            JobState::Pending | JobState::Paused => {
                let mut next = self.inner.lock().clone();
                let record = next.jobs.get_mut(&job_id).expect("record checked above");
                apply_transition(record, JobState::Cancelling)?;
                apply_transition(record, JobState::Failed)?;
                record.error = Some(match state {
                    JobState::Pending => "cancelled before the job started".to_string(),
                    _ => "cancelled while the job was paused".to_string(),
                });
                self.persist_and_swap(next)?;
                self.tokens.lock().entry(job_id).or_default().cancel();
                Ok(())
            }
            JobState::Running => {
                self.transition(job_id, JobState::Cancelling)?;
                self.tokens.lock().entry(job_id).or_default().cancel();
                Ok(())
            }
            JobState::Cancelling | JobState::RollingBack => Ok(()),
            JobState::Succeeded | JobState::Failed => Err(JobError::IllegalTransition {
                from: state,
                to: JobState::Cancelling,
            }),
        }
    }

    /// Persist a new progress value for a running job.
    fn update_progress(&self, job_id: u64, progress: JobProgress) -> Result<(), JobError> {
        let mut next = self.inner.lock().clone();
        let record = next
            .jobs
            .get_mut(&job_id)
            .ok_or(JobError::NotFound { job_id })?;
        if record.state != JobState::Running {
            return Err(JobError::UnexpectedState {
                job_id,
                expected: JobState::Running,
                actual: record.state,
            });
        }
        record.progress = progress;
        record.updated_at_micros = unix_micros();
        self.persist_and_swap(next)
    }

    /// Apply a legal transition and persist it.
    fn transition(&self, job_id: u64, to: JobState) -> Result<(), JobError> {
        let mut next = self.inner.lock().clone();
        let record = next
            .jobs
            .get_mut(&job_id)
            .ok_or(JobError::NotFound { job_id })?;
        apply_transition(record, to)?;
        self.persist_and_swap(next)
    }

    /// `Pending -> Running`, enforcing the active-job bound.
    fn admit(&self, job_id: u64) -> Result<(), JobError> {
        let mut next = self.inner.lock().clone();
        let active = next
            .jobs
            .values()
            .filter(|record| {
                matches!(
                    record.state,
                    JobState::Running | JobState::Cancelling | JobState::RollingBack
                )
            })
            .count();
        let record = next
            .jobs
            .get(&job_id)
            .ok_or(JobError::NotFound { job_id })?;
        if record.state != JobState::Pending {
            return Err(JobError::IllegalTransition {
                from: record.state,
                to: JobState::Running,
            });
        }
        if active >= self.max_concurrent_jobs {
            return Err(JobError::ConcurrencyLimit {
                active,
                limit: self.max_concurrent_jobs,
            });
        }
        let record = next.jobs.get_mut(&job_id).expect("record checked above");
        apply_transition(record, JobState::Running)?;
        self.persist_and_swap(next)
    }

    /// Persist the driver checkpoint and phase-derived progress. Allowed in
    /// `Running` and in `Paused`: an operator pause can land while the phase
    /// whose checkpoint is being saved was in flight, and the completed
    /// phase's checkpoint must still be durable before the drive parks.
    fn save_checkpoint(
        &self,
        job_id: u64,
        checkpoint: Vec<u8>,
        progress: JobProgress,
    ) -> Result<(), JobError> {
        if checkpoint.len() > MAX_CHECKPOINT_BYTES {
            return Err(JobError::CheckpointTooLarge {
                bytes: checkpoint.len(),
                limit: MAX_CHECKPOINT_BYTES,
            });
        }
        let mut next = self.inner.lock().clone();
        let record = next
            .jobs
            .get_mut(&job_id)
            .ok_or(JobError::NotFound { job_id })?;
        if !matches!(record.state, JobState::Running | JobState::Paused) {
            return Err(JobError::UnexpectedState {
                job_id,
                expected: JobState::Running,
                actual: record.state,
            });
        }
        record.checkpoint = Some(checkpoint);
        record.progress = progress;
        record.updated_at_micros = unix_micros();
        self.persist_and_swap(next)
    }

    /// `Running -> Succeeded`: clears the checkpoint (no resume remains) and
    /// pins progress at 100%.
    fn complete(&self, job_id: u64) -> Result<(), JobError> {
        let mut next = self.inner.lock().clone();
        let record = next
            .jobs
            .get_mut(&job_id)
            .ok_or(JobError::NotFound { job_id })?;
        apply_transition(record, JobState::Succeeded)?;
        record.checkpoint = None;
        record.progress.fraction = 1.0;
        self.persist_and_swap(next)
    }

    /// `Running|Cancelling -> RollingBack`.
    fn begin_rollback(&self, job_id: u64) -> Result<(), JobError> {
        self.transition(job_id, JobState::RollingBack)
    }

    /// `RollingBack -> Failed`, recording the terminal error.
    fn fail(&self, job_id: u64, error: String) -> Result<(), JobError> {
        let mut next = self.inner.lock().clone();
        let record = next
            .jobs
            .get_mut(&job_id)
            .ok_or(JobError::NotFound { job_id })?;
        apply_transition(record, JobState::Failed)?;
        record.error = Some(error);
        self.persist_and_swap(next)
    }

    /// Persist `next`, then swap it in as the live state. On a persistence
    /// error the in-memory state is untouched, so memory never runs ahead of
    /// the durable file.
    fn persist_and_swap(&self, next: RegistryInner) -> Result<(), JobError> {
        self.persist_locked(&next)?;
        *self.inner.lock() = next;
        self.state_changed.notify_all();
        Ok(())
    }

    fn persist_locked(&self, inner: &RegistryInner) -> Result<(), JobError> {
        let snapshot = JobsSnapshot {
            next_job_id: inner.next_job_id,
            jobs: inner.jobs.values().cloned().collect(),
        };
        write_durable(&self.root, &snapshot, self.meta_dek.as_ref())
    }
}

/// Drive one job through the full S1F-003 build-and-publish protocol.
///
/// The record must be `Pending`: [`JobRegistry::submit`] leaves it there,
/// and a `Paused` job is requeued with [`JobRegistry::resume`] first. When
/// the record carries a checkpoint (a resume), `job` is restored from it and
/// completed phases are skipped; otherwise all seven phases run in order.
///
/// Returns `Ok(())` when every phase completed (the record is then
/// `Succeeded`) or when an operator pause parked the job mid-run (the record
/// is then `Paused` and a later drive resumes it). Phase errors and
/// cancellation surface as `Err` after the record lands in `Failed`;
/// injected boundary faults surface as [`JobError::InjectedFault`] with the
/// record parked in `Paused`.
pub fn run_build_publish<J: BuildPublishJob + ?Sized>(
    registry: &JobRegistry,
    job_id: u64,
    job: &mut J,
) -> Result<(), JobError> {
    let record = registry.get(job_id).ok_or(JobError::NotFound { job_id })?;
    if record.state != JobState::Pending {
        return Err(JobError::IllegalTransition {
            from: record.state,
            to: JobState::Running,
        });
    }
    // Restore before admission: a corrupt checkpoint leaves the job queued
    // rather than half-admitted.
    let mut completed_phases = 0_usize;
    if let Some(checkpoint) = &record.checkpoint {
        let decoded = decode_build_checkpoint(checkpoint)?;
        job.restore_checkpoint(&decoded.state)?;
        completed_phases = usize::from(decoded.completed_phases);
    }
    registry.admit(job_id)?;
    // From here until the guard drops the job has a live drive; `resume()`
    // refuses to requeue it.
    let _drive = DriveGuard { registry, job_id };
    let token = registry
        .cancellation_token(job_id)
        .ok_or(JobError::NotFound { job_id })?;
    let context = JobContext {
        registry,
        job_id,
        token,
    };

    for (index, phase) in BuildPhase::ALL
        .iter()
        .copied()
        .enumerate()
        .skip(completed_phases)
    {
        // Cooperative stops, checked at every phase boundary: an operator
        // pause parks the drive (checkpoint durable), an operator cancel
        // rolls the job back.
        let state = registry
            .get(job_id)
            .ok_or(JobError::NotFound { job_id })?
            .state;
        match state {
            JobState::Running => {}
            JobState::Paused => return Ok(()),
            JobState::Cancelling => {
                return rollback_and_fail(registry, job_id, job, JobError::Cancelled);
            }
            state => {
                return Err(JobError::IllegalTransition {
                    from: state,
                    to: JobState::Running,
                });
            }
        }
        if let Err(fault) = mongreldb_fault::inject(phase.before_hook()) {
            return park_on_fault(registry, job_id, job, fault);
        }
        let outcome = phase.invoke(job, &context);
        if let Err(fault) = mongreldb_fault::inject(phase.after_hook()) {
            return park_on_fault(registry, job_id, job, fault);
        }
        if let Err(error) = outcome {
            return rollback_and_fail(registry, job_id, job, error);
        }
        completed_phases = index + 1;
        let checkpoint = encode_build_checkpoint(completed_phases as u8, &job.checkpoint_state())?;
        let total = BuildPhase::ALL.len() as u64;
        let done = completed_phases as u64;
        let progress = JobProgress::new(done as f64 / total as f64, done, total)?;
        // An operator pause/cancel may have landed while the phase ran. The
        // completed phase's checkpoint is saved either way (progress is
        // monotonic and phases are idempotent); the drive then parks or
        // rolls back instead of starting the next phase.
        let state = registry
            .get(job_id)
            .ok_or(JobError::NotFound { job_id })?
            .state;
        match state {
            JobState::Running => registry.save_checkpoint(job_id, checkpoint, progress)?,
            JobState::Paused => {
                registry.save_checkpoint(job_id, checkpoint, progress)?;
                return Ok(());
            }
            JobState::Cancelling => {
                return rollback_and_fail(registry, job_id, job, JobError::Cancelled);
            }
            state => {
                return Err(JobError::IllegalTransition {
                    from: state,
                    to: JobState::Running,
                });
            }
        }
    }
    match registry.complete(job_id) {
        Ok(()) => Ok(()),
        Err(JobError::IllegalTransition { from, .. }) => match from {
            // A last-instant operator pause wins; the next drive finishes the
            // already-checkpointed job. A last-instant cancel rolls back.
            JobState::Paused => Ok(()),
            JobState::Cancelling => rollback_and_fail(registry, job_id, job, JobError::Cancelled),
            state => Err(JobError::IllegalTransition {
                from: state,
                to: JobState::Succeeded,
            }),
        },
        Err(error) => Err(error),
    }
}

/// An injected boundary fault is transient: park the job (its last
/// checkpoint is durable) and report the fault. If the operator cancelled
/// concurrently, cancellation wins and the job rolls back instead.
fn park_on_fault<J: BuildPublishJob + ?Sized>(
    registry: &JobRegistry,
    job_id: u64,
    job: &mut J,
    fault: mongreldb_fault::Fault,
) -> Result<(), JobError> {
    match registry.pause(job_id) {
        Ok(()) => Err(JobError::InjectedFault(fault.to_string())),
        Err(JobError::IllegalTransition {
            from: JobState::Cancelling,
            ..
        }) => rollback_and_fail(registry, job_id, job, JobError::Cancelled),
        Err(error) => Err(error),
    }
}

/// Shared failure path: `Running|Cancelling -> RollingBack -> Failed` with
/// the job's `rollback()` in between. The original error is returned to the
/// caller; a rollback failure is recorded on the record, not propagated.
fn rollback_and_fail<J: BuildPublishJob + ?Sized>(
    registry: &JobRegistry,
    job_id: u64,
    job: &mut J,
    error: JobError,
) -> Result<(), JobError> {
    match registry.begin_rollback(job_id) {
        Ok(()) => {}
        // An operator pause/cancel landed concurrently and wins: the job is
        // no longer running, so there is nothing to roll back on this drive.
        // The failing phase re-runs (idempotency contract) on the next one.
        Err(JobError::IllegalTransition { .. }) => return Err(error),
        Err(storage) => return Err(storage),
    }
    let message = match job.rollback() {
        Ok(()) => error.to_string(),
        Err(rollback_error) => format!("{error}; rollback also failed: {rollback_error}"),
    };
    registry.fail(job_id, message)?;
    Err(error)
}

/// RAII marker for a live [`run_build_publish`] drive: removes the job from
/// the registry's active-drive set on every exit path so [`JobRegistry::resume`]
/// can reject a requeue while the previous drive is still draining.
struct DriveGuard<'a> {
    registry: &'a JobRegistry,
    job_id: u64,
}

impl Drop for DriveGuard<'_> {
    fn drop(&mut self) {
        self.registry.active_drives.lock().remove(&self.job_id);
    }
}

/// The driver-owned payload inside [`JobRecord::checkpoint`]: how many
/// leading phases completed, plus the implementor's opaque state.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BuildPublishCheckpoint {
    completed_phases: u8,
    state: Vec<u8>,
}

fn encode_build_checkpoint(completed_phases: u8, state: &[u8]) -> Result<Vec<u8>, JobError> {
    serde_json::to_vec(&BuildPublishCheckpoint {
        completed_phases,
        state: state.to_vec(),
    })
    .map_err(|error| JobError::Storage(format!("job checkpoint serialize: {error}")))
}

fn decode_build_checkpoint(bytes: &[u8]) -> Result<BuildPublishCheckpoint, JobError> {
    let checkpoint: BuildPublishCheckpoint = serde_json::from_slice(bytes)
        .map_err(|error| JobError::Storage(format!("job checkpoint deserialize: {error}")))?;
    if usize::from(checkpoint.completed_phases) > BuildPhase::ALL.len() {
        return Err(JobError::Storage(format!(
            "job checkpoint claims {} completed phases, only {} exist",
            checkpoint.completed_phases,
            BuildPhase::ALL.len()
        )));
    }
    Ok(checkpoint)
}

fn apply_transition(record: &mut JobRecord, to: JobState) -> Result<(), JobError> {
    if !record.state.can_transition(to) {
        return Err(JobError::IllegalTransition {
            from: record.state,
            to,
        });
    }
    record.state = to;
    record.updated_at_micros = unix_micros();
    Ok(())
}

/// Crash-recovery mapping (module docs). Returns whether anything changed.
fn recover_after_crash(inner: &mut RegistryInner) -> bool {
    let now = unix_micros();
    let mut changed = false;
    for record in inner.jobs.values_mut() {
        match record.state {
            JobState::Running => {
                // The worker died with the process; park the job on its last
                // durable checkpoint for an operator-driven resume.
                record.state = JobState::Paused;
                record.updated_at_micros = now;
                changed = true;
            }
            JobState::Cancelling => {
                // The crash itself completed the cancellation.
                record.state = JobState::Failed;
                record.error =
                    Some("cancelled (process restarted while the job was cancelling)".to_string());
                record.updated_at_micros = now;
                changed = true;
            }
            JobState::RollingBack => {
                record.state = JobState::Failed;
                const NOTE: &str = "rollback interrupted by process restart";
                record.error = Some(match record.error.take() {
                    Some(error) => format!("{error}; {NOTE}"),
                    None => NOTE.to_string(),
                });
                record.updated_at_micros = now;
                changed = true;
            }
            JobState::Pending | JobState::Paused | JobState::Succeeded | JobState::Failed => {}
        }
    }
    changed
}

/// Fail-closed validation of the decoded file body: ids are unique and the
/// allocator can never reissue one (spec section 7).
fn validate_snapshot(snapshot: JobsSnapshot) -> Result<RegistryInner, JobError> {
    let mut jobs = BTreeMap::new();
    for record in snapshot.jobs {
        if record
            .definition
            .as_ref()
            .is_some_and(|payload| payload.len() > MAX_DEFINITION_BYTES)
        {
            return Err(JobError::Storage(format!(
                "job {} definition is {} bytes, limit is {}",
                record.job_id,
                record.definition.as_ref().map_or(0, Vec::len),
                MAX_DEFINITION_BYTES
            )));
        }
        if jobs.insert(record.job_id, record).is_some() {
            return Err(JobError::Storage(
                "duplicate job id in registry file".to_string(),
            ));
        }
    }
    let max_id = jobs.keys().next_back().copied().unwrap_or(0);
    if snapshot.next_job_id <= max_id {
        return Err(JobError::Storage(format!(
            "registry allocator at {} would reissue job id {max_id}",
            snapshot.next_job_id
        )));
    }
    Ok(RegistryInner {
        next_job_id: snapshot.next_job_id.max(1),
        jobs,
    })
}

fn encode(snapshot: &JobsSnapshot) -> Result<Vec<u8>, JobError> {
    serde_json::to_vec(&JobsEnvelope {
        format_version: JOBS_FORMAT_VERSION,
        registry: snapshot.clone(),
    })
    .map_err(|error| JobError::Storage(format!("job registry serialize: {error}")))
}

fn decode(body: &[u8]) -> Result<JobsSnapshot, JobError> {
    let envelope: JobsEnvelope = serde_json::from_slice(body)
        .map_err(|error| JobError::Storage(format!("job registry deserialize: {error}")))?;
    if envelope.format_version != JOBS_FORMAT_VERSION {
        return Err(JobError::Storage(format!(
            "unsupported job registry format version {}",
            envelope.format_version
        )));
    }
    Ok(envelope.registry)
}

fn plaintext_frame(body: &[u8]) -> Vec<u8> {
    let hash = Sha256::digest(body);
    let mut out = Vec::with_capacity(body.len() + 8 + 32);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&hash);
    out.extend_from_slice(body);
    out
}

fn seal(body: &[u8], meta_dek: Option<&[u8; META_DEK_LEN]>) -> Result<Vec<u8>, JobError> {
    match meta_dek {
        Some(dek) => crate::encryption::encrypt_blob(dek, body)
            .map_err(|error| JobError::Storage(format!("job registry seal: {error}"))),
        None => Ok(plaintext_frame(body)),
    }
}

fn open_payload(
    bytes: &[u8],
    meta_dek: Option<&[u8; META_DEK_LEN]>,
) -> Result<JobsSnapshot, JobError> {
    match meta_dek {
        // Fail closed: an unauthenticated registry is an error, never
        // "no jobs" (see module docs for the catalog deviation).
        Some(dek) => {
            let body = crate::encryption::decrypt_blob(dek, bytes).map_err(|_| {
                JobError::Storage(
                    "job registry authentication failed (wrong key or tampered)".to_string(),
                )
            })?;
            decode(&body)
        }
        None => parse_plaintext(bytes),
    }
}

/// Write the registry file through the catalog's checksum + atomic-rename
/// path (temp write, fsync, rename, parent-dir fsync).
fn write_durable(
    root: &DurableRoot,
    snapshot: &JobsSnapshot,
    meta_dek: Option<&[u8; META_DEK_LEN]>,
) -> Result<(), JobError> {
    let body = encode(snapshot)?;
    let payload = seal(&body, meta_dek)?;
    root.write_atomic(JOBS_FILENAME, &payload)?;
    Ok(())
}

/// Read the registry file. `Ok(None)` means no file exists yet; any present
/// but unverifiable content is an error (fail closed, module docs).
fn read_durable(
    root: &DurableRoot,
    meta_dek: Option<&[u8; META_DEK_LEN]>,
) -> Result<Option<JobsSnapshot>, JobError> {
    let file = match root.open_regular(JOBS_FILENAME) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let length = file.metadata()?.len();
    if length > MAX_JOBS_BYTES {
        return Err(JobError::Storage(format!(
            "job registry of {length} bytes exceeds the {MAX_JOBS_BYTES}-byte limit"
        )));
    }
    let mut bytes = Vec::with_capacity(length as usize);
    file.take(MAX_JOBS_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length {
        return Err(JobError::Storage(
            "job registry length changed while reading".to_string(),
        ));
    }
    open_payload(&bytes, meta_dek).map(Some)
}

fn parse_plaintext(bytes: &[u8]) -> Result<JobsSnapshot, JobError> {
    if bytes.len() < 8 + 32 || &bytes[..8] != MAGIC {
        return Err(JobError::Storage(
            "job registry magic mismatch (corrupt or sealed with a key)".to_string(),
        ));
    }
    let (tag, body) = bytes[8..].split_at(32);
    let calc = Sha256::digest(body);
    if tag != calc.as_slice() {
        return Err(JobError::Storage(
            "job registry checksum mismatch (tampered or torn)".to_string(),
        ));
    }
    decode(body)
}

/// Wall-clock microseconds since the Unix epoch (saturating). Job metadata
/// only; never an MVCC/visibility timestamp.
fn unix_micros() -> u64 {
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_micros())
        .unwrap_or(0);
    u64::try_from(micros).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_temp() -> (tempfile::TempDir, JobRegistry) {
        let dir = tempfile::tempdir().unwrap();
        let registry = JobRegistry::open(dir.path(), None).unwrap();
        (dir, registry)
    }

    fn target() -> JobTarget {
        JobTarget {
            table: "items".to_string(),
            index: Some("items_idx".to_string()),
        }
    }

    #[test]
    fn transition_graph_matches_the_documented_edges() {
        let legal: [(JobState, JobState); 11] = [
            (JobState::Pending, JobState::Running),
            (JobState::Pending, JobState::Cancelling),
            (JobState::Running, JobState::Paused),
            (JobState::Running, JobState::Cancelling),
            (JobState::Running, JobState::RollingBack),
            (JobState::Running, JobState::Succeeded),
            (JobState::Paused, JobState::Pending),
            (JobState::Paused, JobState::Cancelling),
            (JobState::Cancelling, JobState::RollingBack),
            (JobState::Cancelling, JobState::Failed),
            (JobState::RollingBack, JobState::Failed),
        ];
        for from in JobState::ALL {
            for to in JobState::ALL {
                let expected = legal.contains(&(from, to));
                assert_eq!(from.can_transition(to), expected, "edge {from:?} -> {to:?}");
            }
        }
        for terminal in [JobState::Succeeded, JobState::Failed] {
            assert!(terminal.is_terminal());
            assert!(JobState::ALL
                .iter()
                .all(|&next| !terminal.can_transition(next)));
        }
    }

    #[test]
    fn progress_validation_rejects_out_of_range_values() {
        assert!(JobProgress::new(0.5, 1, 2).is_ok());
        assert!(JobProgress::new(0.0, 0, 0).is_ok());
        assert!(JobProgress::new(1.0, 7, 7).is_ok());
        assert!(JobProgress::new(f64::NAN, 0, 0).is_err());
        assert!(JobProgress::new(-0.1, 0, 0).is_err());
        assert!(JobProgress::new(1.1, 0, 0).is_err());
        assert!(JobProgress::new(0.5, 3, 2).is_err());
    }

    #[test]
    fn persistence_round_trip_preserves_records_and_allocator() {
        let dir = tempfile::tempdir().unwrap();
        let first = JobRegistry::open(dir.path(), None).unwrap();
        let a = first.submit(JobKind::IndexBuild, target()).unwrap();
        let b = first
            .submit(
                JobKind::LargeImport,
                JobTarget {
                    table: "bulk".to_string(),
                    index: None,
                },
            )
            .unwrap();
        assert_eq!((a, b), (1, 2));
        first.admit(a).unwrap();
        first
            .save_checkpoint(
                a,
                b"opaque-resume-state".to_vec(),
                JobProgress::new(0.5, 4, 8).unwrap(),
            )
            .unwrap();
        drop(first);

        let reopened = JobRegistry::open(dir.path(), None).unwrap();
        // Recovery maps Running -> Paused (covered exhaustively below), so
        // compare the fields the mapping does not touch.
        let record = reopened.get(a).unwrap();
        assert_eq!(record.state, JobState::Paused);
        assert_eq!(record.kind, JobKind::IndexBuild);
        assert_eq!(record.target, target());
        assert_eq!(record.progress, JobProgress::new(0.5, 4, 8).unwrap());
        assert_eq!(
            record.checkpoint.as_deref(),
            Some(b"opaque-resume-state".as_slice())
        );
        assert!(record.error.is_none());
        assert!(record.updated_at_micros >= record.created_at_micros);
        assert_eq!(reopened.get(b).unwrap().state, JobState::Pending);
        // Ids are never reused across reopen.
        let c = reopened
            .submit(
                JobKind::KeyRotation,
                JobTarget {
                    table: "items".to_string(),
                    index: None,
                },
            )
            .unwrap();
        assert_eq!(c, 3);
        assert_eq!(reopened.list().len(), 3);
    }

    #[test]
    fn crash_recovery_maps_active_states_to_safe_ones() {
        let dir = tempfile::tempdir().unwrap();
        let registry = JobRegistry::open(dir.path(), None)
            .unwrap()
            .with_max_concurrent_jobs(8);
        let running = registry.submit(JobKind::IndexBuild, target()).unwrap();
        let cancelling = registry.submit(JobKind::IndexBuild, target()).unwrap();
        let rolling_back = registry.submit(JobKind::IndexBuild, target()).unwrap();
        let pending = registry.submit(JobKind::IndexBuild, target()).unwrap();
        let paused = registry.submit(JobKind::IndexBuild, target()).unwrap();
        let succeeded = registry.submit(JobKind::IndexBuild, target()).unwrap();
        let failed = registry.submit(JobKind::IndexBuild, target()).unwrap();

        registry.admit(running).unwrap();
        registry
            .save_checkpoint(
                running,
                b"resume-bytes".to_vec(),
                JobProgress::new(0.25, 1, 4).unwrap(),
            )
            .unwrap();
        registry.admit(cancelling).unwrap();
        registry.cancel(cancelling).unwrap();
        registry.admit(rolling_back).unwrap();
        registry.begin_rollback(rolling_back).unwrap();
        registry.admit(paused).unwrap();
        registry.pause(paused).unwrap();
        registry.admit(succeeded).unwrap();
        registry.complete(succeeded).unwrap();
        registry.admit(failed).unwrap();
        registry.begin_rollback(failed).unwrap();
        registry.fail(failed, "boom".to_string()).unwrap();
        drop(registry);

        let recovered = JobRegistry::open(dir.path(), None).unwrap();
        let running = recovered.get(running).unwrap();
        assert_eq!(running.state, JobState::Paused);
        assert_eq!(
            running.checkpoint.as_deref(),
            Some(b"resume-bytes".as_slice())
        );
        assert_eq!(running.progress, JobProgress::new(0.25, 1, 4).unwrap());

        let cancelling = recovered.get(cancelling).unwrap();
        assert_eq!(cancelling.state, JobState::Failed);
        assert!(cancelling.error.unwrap().contains("cancelled"));

        let rolling_back = recovered.get(rolling_back).unwrap();
        assert_eq!(rolling_back.state, JobState::Failed);
        assert!(rolling_back.error.unwrap().contains("rollback interrupted"));

        assert_eq!(recovered.get(pending).unwrap().state, JobState::Pending);
        assert_eq!(recovered.get(paused).unwrap().state, JobState::Paused);
        assert_eq!(recovered.get(succeeded).unwrap().state, JobState::Succeeded);
        let failed = recovered.get(failed).unwrap();
        assert_eq!(failed.state, JobState::Failed);
        assert_eq!(failed.error.as_deref(), Some("boom"));

        // Recovery was persisted: a second reopen is a no-op.
        drop(recovered);
        let again = JobRegistry::open(dir.path(), None).unwrap();
        assert_eq!(again.list().len(), 7);
        assert!(again.list().iter().all(|record| !matches!(
            record.state,
            JobState::Running | JobState::Cancelling | JobState::RollingBack
        )));
    }

    #[test]
    fn admission_enforces_the_concurrency_bound() {
        let (_dir, registry) = open_temp();
        let registry = registry.with_max_concurrent_jobs(1);
        let a = registry.submit(JobKind::IndexBuild, target()).unwrap();
        let b = registry.submit(JobKind::IndexBuild, target()).unwrap();
        registry.admit(a).unwrap();
        let error = registry.admit(b).unwrap_err();
        assert!(
            matches!(
                error,
                JobError::ConcurrencyLimit {
                    active: 1,
                    limit: 1
                }
            ),
            "expected ConcurrencyLimit, got {error:?}"
        );
        // A failed admission leaves the job queued.
        assert_eq!(registry.get(b).unwrap().state, JobState::Pending);
        registry.complete(a).unwrap();
        registry.admit(b).unwrap();
    }

    #[test]
    fn pause_resume_cancel_follow_the_graph() {
        let (_dir, registry) = open_temp();
        let job = registry.submit(JobKind::IndexBuild, target()).unwrap();
        // Pause requires a live worker.
        assert!(matches!(
            registry.pause(job),
            Err(JobError::IllegalTransition {
                from: JobState::Pending,
                to: JobState::Paused
            })
        ));
        registry.admit(job).unwrap();
        registry.pause(job).unwrap();
        assert_eq!(registry.get(job).unwrap().state, JobState::Paused);
        registry.resume(job).unwrap();
        assert_eq!(registry.get(job).unwrap().state, JobState::Pending);
        registry.admit(job).unwrap();
        registry.cancel(job).unwrap();
        assert_eq!(registry.get(job).unwrap().state, JobState::Cancelling);
        assert!(registry.cancellation_token(job).unwrap().is_cancelled());
        // Cancelling is idempotent.
        registry.cancel(job).unwrap();
        registry.begin_rollback(job).unwrap();
        registry.fail(job, "cancelled".to_string()).unwrap();
        // Terminal states reject every operator verb.
        assert!(matches!(
            registry.cancel(job),
            Err(JobError::IllegalTransition {
                from: JobState::Failed,
                to: JobState::Cancelling
            })
        ));
        assert!(matches!(
            registry.resume(job),
            Err(JobError::IllegalTransition {
                from: JobState::Failed,
                to: JobState::Pending
            })
        ));
    }

    #[test]
    fn cancel_without_a_worker_completes_synchronously() {
        let (_dir, registry) = open_temp();
        let queued = registry.submit(JobKind::IndexBuild, target()).unwrap();
        registry.cancel(queued).unwrap();
        let record = registry.get(queued).unwrap();
        assert_eq!(record.state, JobState::Failed);
        assert_eq!(
            record.error.as_deref(),
            Some("cancelled before the job started")
        );
        assert!(registry.cancellation_token(queued).unwrap().is_cancelled());

        let parked = registry.submit(JobKind::IndexBuild, target()).unwrap();
        registry.admit(parked).unwrap();
        registry.pause(parked).unwrap();
        registry.cancel(parked).unwrap();
        let record = registry.get(parked).unwrap();
        assert_eq!(record.state, JobState::Failed);
        assert_eq!(
            record.error.as_deref(),
            Some("cancelled while the job was paused")
        );
    }

    #[test]
    fn missing_file_opens_empty_and_file_is_created_on_first_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let registry = JobRegistry::open(dir.path(), None).unwrap();
        assert!(registry.list().is_empty());
        assert!(!dir.path().join(JOBS_FILENAME).exists());
        registry.submit(JobKind::IndexBuild, target()).unwrap();
        assert!(dir.path().join(JOBS_FILENAME).exists());
    }

    #[test]
    fn tampered_file_fails_closed() {
        // Corrupt body byte with intact magic+length: checksum fires.
        let dir = tempfile::tempdir().unwrap();
        let registry = JobRegistry::open(dir.path(), None).unwrap();
        registry.submit(JobKind::IndexBuild, target()).unwrap();
        drop(registry);
        let path = dir.path().join(JOBS_FILENAME);
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        std::fs::write(&path, &bytes).unwrap();
        let error = JobRegistry::open(dir.path(), None).unwrap_err();
        assert!(
            matches!(&error, JobError::Storage(message) if message.contains("checksum")),
            "expected checksum failure, got {error:?}"
        );

        // Wrong magic (e.g. a sealed file opened without the key): error.
        bytes[..8].copy_from_slice(b"NOTAJOB!");
        std::fs::write(&path, &bytes).unwrap();
        let error = JobRegistry::open(dir.path(), None).unwrap_err();
        assert!(
            matches!(&error, JobError::Storage(message) if message.contains("magic")),
            "expected magic failure, got {error:?}"
        );

        // Truncated below the frame header: error, never "empty registry".
        std::fs::write(&path, b"MON").unwrap();
        assert!(matches!(
            JobRegistry::open(dir.path(), None),
            Err(JobError::Storage(_))
        ));
    }

    #[test]
    fn unsupported_format_version_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let body = serde_json::to_vec(&serde_json::json!({
            "format_version": 99,
            "registry": { "next_job_id": 1, "jobs": [] }
        }))
        .unwrap();
        let payload = plaintext_frame(&body);
        std::fs::write(dir.path().join(JOBS_FILENAME), payload).unwrap();
        let error = JobRegistry::open(dir.path(), None).unwrap_err();
        assert!(
            matches!(&error, JobError::Storage(message) if message.contains("version 99")),
            "expected version failure, got {error:?}"
        );
    }

    #[test]
    fn allocator_inconsistency_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let body = serde_json::to_vec(&serde_json::json!({
            "format_version": 1,
            "registry": {
                "next_job_id": 1,
                "jobs": [{
                    "job_id": 1,
                    "kind": "IndexBuild",
                    "state": "Pending",
                    "target": { "table": "items" },
                    "progress": { "fraction": 0.0, "done": 0, "total": 0 },
                    "created_at_micros": 1,
                    "updated_at_micros": 1
                }]
            }
        }))
        .unwrap();
        std::fs::write(dir.path().join(JOBS_FILENAME), plaintext_frame(&body)).unwrap();
        assert!(matches!(
            JobRegistry::open(dir.path(), None),
            Err(JobError::Storage(_))
        ));
    }

    #[test]
    fn resume_rejects_a_job_with_a_live_drive() {
        let (_dir, registry) = open_temp();
        let job = registry.submit(JobKind::IndexBuild, target()).unwrap();
        registry.admit(job).unwrap();
        registry.pause(job).unwrap();
        // Simulate a drive still draining toward the park point.
        registry.active_drives.lock().insert(job);
        assert!(matches!(
            registry.resume(job),
            Err(JobError::DriveActive { job_id }) if job_id == job
        ));
        assert_eq!(registry.get(job).unwrap().state, JobState::Paused);
        registry.active_drives.lock().remove(&job);
        registry.resume(job).unwrap();
        assert_eq!(registry.get(job).unwrap().state, JobState::Pending);
    }

    #[test]
    fn checkpoint_size_is_bounded() {
        let (_dir, registry) = open_temp();
        let job = registry.submit(JobKind::IndexBuild, target()).unwrap();
        registry.admit(job).unwrap();
        let oversized = vec![0_u8; MAX_CHECKPOINT_BYTES + 1];
        assert!(matches!(
            registry.save_checkpoint(job, oversized, JobProgress::default()),
            Err(JobError::CheckpointTooLarge { .. })
        ));
    }

    #[test]
    fn job_error_maps_onto_the_engine_error() {
        assert!(matches!(
            MongrelError::from(JobError::Cancelled),
            MongrelError::Cancelled
        ));
        assert!(matches!(
            MongrelError::from(JobError::NotFound { job_id: 7 }),
            MongrelError::NotFound(_)
        ));
        assert!(matches!(
            MongrelError::from(JobError::ConcurrencyLimit {
                active: 3,
                limit: 2
            }),
            MongrelError::ResourceLimitExceeded { .. }
        ));
        assert!(matches!(
            MongrelError::from(JobError::InjectedFault("x".to_string())),
            MongrelError::Other(_)
        ));
    }

    #[test]
    fn record_serde_uses_stable_text_encoding() {
        let (_dir, registry) = open_temp();
        let job = registry
            .submit(JobKind::MaterializedViewRebuild, target())
            .unwrap();
        registry.admit(job).unwrap();
        let record = registry.get(job).unwrap();
        let json = serde_json::to_string(&record).unwrap();
        assert!(json.contains("\"kind\":\"MaterializedViewRebuild\""));
        assert!(json.contains("\"state\":\"Running\""));
        let decoded: JobRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, record);
        // Enum serde guards the durable contract: unknown variants fail.
        let unknown = json.replace("\"Running\"", "\"Napping\"");
        assert!(serde_json::from_str::<JobRecord>(&unknown).is_err());
    }

    #[test]
    fn checkpoint_codec_round_trip_and_bounds() {
        let bytes = encode_build_checkpoint(3, b"impl-state").unwrap();
        let decoded = decode_build_checkpoint(&bytes).unwrap();
        assert_eq!(decoded.completed_phases, 3);
        assert_eq!(decoded.state, b"impl-state");
        let corrupt = encode_build_checkpoint(8, b"").unwrap();
        assert!(matches!(
            decode_build_checkpoint(&corrupt),
            Err(JobError::Storage(_))
        ));
    }
}
