//! Stage 1F integration tests (spec section 10.6, S1F-002/S1F-003): the
//! build-and-publish driver end to end — full lifecycle, cooperative
//! pause/cancel, injected phase-boundary faults with safe resume, the
//! concurrency bound, and crash recovery across a real tempdir `Database`
//! reopen.
//!
//! The fault registry is process-global and every `run_build_publish` drive
//! evaluates the `job.*` hooks, so all of these tests serialize on
//! `TEST_LOCK`: one test's armed hook must never fire on another test's
//! drive. Thread coordination uses barriers/condvars, never sleeps.

use mongreldb_core::jobs::{
    run_build_publish, BuildPhase, BuildPublishJob, JobContext, JobError, JobKind, JobProgress,
    JobRegistry, JobState, JobTarget, JOBS_FILENAME,
};
use mongreldb_core::Database;
use mongreldb_fault::{Action, ScopedGuard};
use std::sync::{Arc, Condvar, Mutex};

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn target() -> JobTarget {
    JobTarget {
        table: "items".to_string(),
        index: Some("items_idx".to_string()),
    }
}

/// Deterministic two-thread rendezvous: the runner thread blocks inside a
/// fault-hook callback until the test thread releases it.
#[derive(Clone, Default)]
struct Gate {
    inner: Arc<(Mutex<GateState>, Condvar)>,
}

#[derive(Default)]
struct GateState {
    entered: bool,
    released: bool,
}

impl Gate {
    /// Runner side: record arrival, then block until released.
    fn block_until_released(&self) {
        let (lock, cvar) = &*self.inner;
        let mut state = lock.lock().unwrap();
        state.entered = true;
        cvar.notify_all();
        while !state.released {
            state = cvar.wait(state).unwrap();
        }
    }

    /// Test side: wait until the runner arrived.
    fn wait_entered(&self) {
        let (lock, cvar) = &*self.inner;
        let mut state = lock.lock().unwrap();
        while !state.entered {
            state = cvar.wait(state).unwrap();
        }
    }

    /// Test side: let the runner continue.
    fn release(&self) {
        let (lock, cvar) = &*self.inner;
        let mut state = lock.lock().unwrap();
        state.released = true;
        cvar.notify_all();
    }
}

/// Observable side effects shared by every mock instance of one test, so a
/// resumed run (a *fresh* `MockBuild`, like a post-restart worker) provably
/// continues the same logical job.
#[derive(Default)]
struct MockState {
    invocations: Vec<&'static str>,
    restores: Vec<Vec<u8>>,
    rollbacks: usize,
}

type SharedMock = Arc<Mutex<MockState>>;

struct MockBuild {
    shared: SharedMock,
    fail_validate: bool,
}

impl MockBuild {
    fn new(shared: &SharedMock) -> Self {
        Self {
            shared: Arc::clone(shared),
            fail_validate: false,
        }
    }

    fn log(&mut self, phase: &'static str) -> Result<(), JobError> {
        self.shared.lock().unwrap().invocations.push(phase);
        Ok(())
    }
}

impl BuildPublishJob for MockBuild {
    fn checkpoint_state(&self) -> Vec<u8> {
        let completed = self.shared.lock().unwrap().invocations.len() as u64;
        completed.to_le_bytes().to_vec()
    }

    fn restore_checkpoint(&mut self, state: &[u8]) -> Result<(), JobError> {
        self.shared.lock().unwrap().restores.push(state.to_vec());
        Ok(())
    }

    fn record_pending(&mut self, _context: &JobContext) -> Result<(), JobError> {
        self.log("record_pending")
    }

    fn pin_snapshot(&mut self, _context: &JobContext) -> Result<(), JobError> {
        self.log("pin_snapshot")
    }

    fn build_hidden(&mut self, _context: &JobContext) -> Result<(), JobError> {
        self.log("build_hidden")
    }

    fn catch_up(&mut self, context: &JobContext) -> Result<(), JobError> {
        // Cooperative cancellation inside a long phase.
        context.check_cancelled()?;
        self.log("catch_up")
    }

    fn validate(&mut self, _context: &JobContext) -> Result<(), JobError> {
        if self.fail_validate {
            return Err(JobError::Phase(
                "validate: generation checksum mismatch".to_string(),
            ));
        }
        self.log("validate")
    }

    fn publish(&mut self, _context: &JobContext) -> Result<(), JobError> {
        self.log("publish")
    }

    fn release_old(&mut self, _context: &JobContext) -> Result<(), JobError> {
        self.log("release_old")
    }

    fn rollback(&mut self) -> Result<(), JobError> {
        self.shared.lock().unwrap().rollbacks += 1;
        Ok(())
    }
}

fn open_registry(dir: &std::path::Path) -> JobRegistry {
    JobRegistry::open(dir, None).unwrap()
}

#[test]
fn build_publish_full_lifecycle() {
    let _permit = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let registry = open_registry(dir.path());
    let shared = SharedMock::default();

    let job = registry.submit(JobKind::IndexBuild, target()).unwrap();
    let mut mock = MockBuild::new(&shared);
    run_build_publish(&registry, job, &mut mock).unwrap();

    let record = registry.get(job).unwrap();
    assert_eq!(record.state, JobState::Succeeded);
    assert_eq!(record.progress, JobProgress::new(1.0, 7, 7).unwrap());
    assert!(record.checkpoint.is_none(), "success clears the checkpoint");
    assert!(record.error.is_none());
    let state = shared.lock().unwrap();
    assert_eq!(
        state.invocations,
        vec![
            "record_pending",
            "pin_snapshot",
            "build_hidden",
            "catch_up",
            "validate",
            "publish",
            "release_old"
        ],
        "phases run exactly once, in protocol order"
    );
    assert!(state.restores.is_empty(), "a fresh run has no checkpoint");
    assert_eq!(state.rollbacks, 0);
    // Terminal states reject the operator verbs.
    assert!(matches!(
        registry.cancel(job),
        Err(JobError::IllegalTransition { .. })
    ));
}

#[test]
fn pause_parks_at_phase_boundary_and_resume_completes() {
    let _permit = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(open_registry(dir.path()));
    let shared = SharedMock::default();

    let job = registry.submit(JobKind::IndexBuild, target()).unwrap();
    let gate = Gate::default();
    let callback_gate = gate.clone();
    let _guard = ScopedGuard::limited(
        BuildPhase::BuildHidden.before_hook(),
        Action::Callback(Arc::new(move |_| callback_gate.block_until_released())),
        1,
    );

    let runner = {
        let registry = Arc::clone(&registry);
        let shared = Arc::clone(&shared);
        std::thread::spawn(move || {
            let mut mock = MockBuild::new(&shared);
            run_build_publish(&registry, job, &mut mock)
        })
    };
    gate.wait_entered();
    registry.pause(job).unwrap();
    gate.release();
    runner.join().unwrap().unwrap();

    let record = registry.get(job).unwrap();
    assert_eq!(
        record.state,
        JobState::Paused,
        "the drive parks at the boundary"
    );
    assert_eq!(record.progress, JobProgress::new(3.0 / 7.0, 3, 7).unwrap());
    assert!(record.checkpoint.is_some());
    assert_eq!(
        shared.lock().unwrap().invocations,
        vec!["record_pending", "pin_snapshot", "build_hidden"]
    );

    // Resume with a fresh worker (restore path), like the scheduler would.
    registry.resume(job).unwrap();
    assert_eq!(registry.get(job).unwrap().state, JobState::Pending);
    let mut resumed = MockBuild::new(&shared);
    run_build_publish(&registry, job, &mut resumed).unwrap();

    let record = registry.get(job).unwrap();
    assert_eq!(record.state, JobState::Succeeded);
    let state = shared.lock().unwrap();
    assert_eq!(
        state.invocations,
        vec![
            "record_pending",
            "pin_snapshot",
            "build_hidden",
            "catch_up",
            "validate",
            "publish",
            "release_old"
        ],
        "completed phases are not re-run after a pause/resume"
    );
    assert_eq!(state.restores.len(), 1, "the resumed worker restored once");
    assert_eq!(
        state.restores[0],
        3_u64.to_le_bytes().to_vec(),
        "the restored state is the checkpoint the first worker wrote"
    );
}

#[test]
fn cooperative_cancel_rolls_back_to_failed() {
    let _permit = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(open_registry(dir.path()));
    let shared = SharedMock::default();

    let job = registry.submit(JobKind::IndexBuild, target()).unwrap();
    let gate = Gate::default();
    let callback_gate = gate.clone();
    let _guard = ScopedGuard::limited(
        BuildPhase::CatchUp.before_hook(),
        Action::Callback(Arc::new(move |_| callback_gate.block_until_released())),
        1,
    );

    let runner = {
        let registry = Arc::clone(&registry);
        let shared = Arc::clone(&shared);
        std::thread::spawn(move || {
            let mut mock = MockBuild::new(&shared);
            run_build_publish(&registry, job, &mut mock)
        })
    };
    gate.wait_entered();
    registry.cancel(job).unwrap();
    assert_eq!(registry.get(job).unwrap().state, JobState::Cancelling);
    assert!(registry.cancellation_token(job).unwrap().is_cancelled());
    gate.release();
    let outcome = runner.join().unwrap();
    assert!(
        matches!(outcome, Err(JobError::Cancelled)),
        "the drive reports the cancellation, got {outcome:?}"
    );

    let record = registry.get(job).unwrap();
    assert_eq!(record.state, JobState::Failed);
    assert!(
        record.error.as_deref().unwrap().contains("cancelled"),
        "the terminal error records the cancel: {:?}",
        record.error
    );
    let state = shared.lock().unwrap();
    assert_eq!(
        state.invocations,
        vec!["record_pending", "pin_snapshot", "build_hidden"],
        "catch_up observed the token before doing any work"
    );
    assert_eq!(
        state.rollbacks, 1,
        "rollback ran between RollingBack and Failed"
    );
}

#[test]
fn injected_fault_at_phase_boundary_parks_then_resume_completes() {
    let _permit = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let registry = open_registry(dir.path());
    let shared = SharedMock::default();

    let job = registry.submit(JobKind::IndexBuild, target()).unwrap();
    let guard = ScopedGuard::limited(BuildPhase::CatchUp.after_hook(), Action::Fail, 1);
    let mut mock = MockBuild::new(&shared);
    let outcome = run_build_publish(&registry, job, &mut mock);
    assert!(
        matches!(&outcome, Err(JobError::InjectedFault(message)) if message.contains("job.catch_up.after")),
        "the injected fault surfaces, got {outcome:?}"
    );
    drop(guard);

    // Transient faults park the job; the failing phase is NOT checkpointed.
    let record = registry.get(job).unwrap();
    assert_eq!(record.state, JobState::Paused);
    assert_eq!(
        record.progress,
        JobProgress::new(3.0 / 7.0, 3, 7).unwrap(),
        "only record_pending..build_hidden are durable"
    );
    assert_eq!(
        shared.lock().unwrap().invocations,
        vec!["record_pending", "pin_snapshot", "build_hidden", "catch_up"]
    );

    // Resume with a fresh worker: catch_up re-runs (the idempotency
    // contract), the earlier phases are skipped, and the job completes.
    registry.resume(job).unwrap();
    let mut resumed = MockBuild::new(&shared);
    run_build_publish(&registry, job, &mut resumed).unwrap();

    let record = registry.get(job).unwrap();
    assert_eq!(record.state, JobState::Succeeded);
    assert_eq!(record.progress, JobProgress::new(1.0, 7, 7).unwrap());
    let state = shared.lock().unwrap();
    assert_eq!(
        state.invocations,
        vec![
            "record_pending",
            "pin_snapshot",
            "build_hidden",
            "catch_up",
            "catch_up",
            "validate",
            "publish",
            "release_old"
        ],
        "resume re-runs only the phase whose checkpoint never landed"
    );
    assert_eq!(state.restores.len(), 1);
    assert_eq!(state.rollbacks, 0, "transient faults never roll back");
}

#[test]
fn phase_error_rolls_back_and_records_failure() {
    let _permit = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let registry = open_registry(dir.path());
    let shared = SharedMock::default();

    let job = registry.submit(JobKind::IndexBuild, target()).unwrap();
    let mut mock = MockBuild::new(&shared);
    mock.fail_validate = true;
    let outcome = run_build_publish(&registry, job, &mut mock);
    assert!(
        matches!(&outcome, Err(JobError::Phase(message)) if message.contains("checksum mismatch")),
        "the phase error surfaces, got {outcome:?}"
    );

    let record = registry.get(job).unwrap();
    assert_eq!(record.state, JobState::Failed);
    assert!(
        record
            .error
            .as_deref()
            .unwrap()
            .contains("checksum mismatch"),
        "the terminal error is the phase's: {:?}",
        record.error
    );
    assert!(
        record.checkpoint.is_some(),
        "the checkpoint is retained for diagnostics after a failure"
    );
    assert_eq!(shared.lock().unwrap().rollbacks, 1);
    // A failed job cannot be resumed or cancelled; it is resubmitted anew.
    assert!(matches!(
        registry.resume(job),
        Err(JobError::IllegalTransition {
            from: JobState::Failed,
            to: JobState::Pending
        })
    ));
    assert!(matches!(
        registry.cancel(job),
        Err(JobError::IllegalTransition {
            from: JobState::Failed,
            to: JobState::Cancelling
        })
    ));
}

#[test]
fn concurrency_bound_rejects_second_admission() {
    let _permit = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(open_registry(dir.path()).with_max_concurrent_jobs(1));
    let shared = SharedMock::default();

    let first = registry.submit(JobKind::IndexBuild, target()).unwrap();
    let second = registry.submit(JobKind::IndexBuild, target()).unwrap();

    let gate = Gate::default();
    let callback_gate = gate.clone();
    let _guard = ScopedGuard::limited(
        BuildPhase::RecordPending.before_hook(),
        Action::Callback(Arc::new(move |_| callback_gate.block_until_released())),
        1,
    );
    let runner = {
        let registry = Arc::clone(&registry);
        let shared = Arc::clone(&shared);
        std::thread::spawn(move || {
            let mut mock = MockBuild::new(&shared);
            run_build_publish(&registry, first, &mut mock)
        })
    };
    // The first job is admitted (holds the single slot) and parked inside
    // its first phase.
    gate.wait_entered();

    let mut mock = MockBuild::new(&shared);
    let outcome = run_build_publish(&registry, second, &mut mock);
    assert!(
        matches!(
            outcome,
            Err(JobError::ConcurrencyLimit {
                active: 1,
                limit: 1
            })
        ),
        "the second admission is rejected, got {outcome:?}"
    );
    assert_eq!(
        registry.get(second).unwrap().state,
        JobState::Pending,
        "a rejected admission leaves the job queued"
    );

    gate.release();
    runner.join().unwrap().unwrap();
    assert_eq!(registry.get(first).unwrap().state, JobState::Succeeded);

    // With the slot free, the queued job admits and completes.
    let mut mock = MockBuild::new(&shared);
    run_build_publish(&registry, second, &mut mock).unwrap();
    assert_eq!(registry.get(second).unwrap().state, JobState::Succeeded);
}

#[test]
fn database_reopen_recovers_and_resumes_jobs() {
    let _permit = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("db");
    let shared = SharedMock::default();

    let job;
    {
        let database = Database::create(&root).unwrap();
        let registry = Arc::new(open_registry(database.root()));
        job = registry.submit(JobKind::IndexBuild, target()).unwrap();

        // Park the job mid-build with a durable checkpoint, then drop
        // everything: the simulated crash.
        let gate = Gate::default();
        let callback_gate = gate.clone();
        let _guard = ScopedGuard::limited(
            BuildPhase::BuildHidden.before_hook(),
            Action::Callback(Arc::new(move |_| callback_gate.block_until_released())),
            1,
        );
        let runner = {
            let registry = Arc::clone(&registry);
            let shared = Arc::clone(&shared);
            std::thread::spawn(move || {
                let mut mock = MockBuild::new(&shared);
                run_build_publish(&registry, job, &mut mock)
            })
        };
        gate.wait_entered();
        registry.pause(job).unwrap();
        gate.release();
        runner.join().unwrap().unwrap();
        assert_eq!(registry.get(job).unwrap().state, JobState::Paused);
    }

    // Reopen the database directory; the registry file sits next to CATALOG.
    assert!(root.join("CATALOG").exists());
    assert!(root.join(JOBS_FILENAME).exists());
    let database = Database::open(&root).unwrap();
    let registry = open_registry(database.root());
    let record = registry.get(job).unwrap();
    assert_eq!(record.state, JobState::Paused);
    assert_eq!(record.kind, JobKind::IndexBuild);
    assert_eq!(record.target, target());
    assert_eq!(record.progress, JobProgress::new(3.0 / 7.0, 3, 7).unwrap());
    assert!(record.checkpoint.is_some());

    // A fresh worker resumes the recovered job to completion.
    registry.resume(job).unwrap();
    let mut resumed = MockBuild::new(&shared);
    run_build_publish(&registry, job, &mut resumed).unwrap();
    let record = registry.get(job).unwrap();
    assert_eq!(record.state, JobState::Succeeded);
    assert_eq!(
        shared.lock().unwrap().invocations,
        vec![
            "record_pending",
            "pin_snapshot",
            "build_hidden",
            "catch_up",
            "validate",
            "publish",
            "release_old"
        ],
        "recovery resumed from the checkpoint, not from scratch"
    );

    // Job ids are never reused across reopen (spec section 7).
    let next = registry
        .submit(
            JobKind::ColumnBackfill,
            JobTarget {
                table: "items".to_string(),
                index: None,
            },
        )
        .unwrap();
    assert_eq!(next, job + 1);
    drop(registry);
    drop(database);

    // Terminal records survive yet another reopen, checkpoint-free.
    let database = Database::open(&root).unwrap();
    let registry = open_registry(database.root());
    let record = registry.get(job).unwrap();
    assert_eq!(record.state, JobState::Succeeded);
    assert_eq!(record.progress, JobProgress::new(1.0, 7, 7).unwrap());
    assert!(record.checkpoint.is_none());
    assert_eq!(registry.get(next).unwrap().state, JobState::Pending);
}
