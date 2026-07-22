//! Persistent async embedding generation jobs, provider readiness (P1.5), and
//! re-embedding build-and-publish (P0.7-T5).
//!
//! Kept as a sibling of [`crate::embedding`] so product-path job types are not
//! entangled with ongoing provider/semantic-identity refactors.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::embedding::{
    EmbeddingError, EmbeddingGenerationStatus, EmbeddingProviderRef, EmbeddingProviderRegistry,
    ProviderHealth,
};

/// Persistent async embedding generation job (P1.5-T1).
///
/// Survives restart via [`crate::jobs::JobRegistry`] when submitted with
/// [`crate::jobs::JobKind::EmbeddingGeneration`] and this payload as the
/// definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddingJob {
    pub job_id: u64,
    pub table_id: u64,
    pub row_id: u64,
    pub column_id: u16,
    pub source_fingerprint: [u8; 32],
    pub semantic_model_fingerprint: [u8; 32],
    pub status: EmbeddingGenerationStatus,
    pub attempts: u32,
    /// Unix micros of the next retry, if Retrying.
    pub next_retry: Option<u64>,
    pub last_error: Option<String>,
}

impl EmbeddingJob {
    pub fn new_pending(
        table_id: u64,
        row_id: u64,
        column_id: u16,
        source_fingerprint: [u8; 32],
        semantic_model_fingerprint: [u8; 32],
    ) -> Self {
        Self {
            job_id: 0,
            table_id,
            row_id,
            column_id,
            source_fingerprint,
            semantic_model_fingerprint,
            status: EmbeddingGenerationStatus::Pending,
            attempts: 0,
            next_retry: None,
            last_error: None,
        }
    }

    pub fn to_definition_bytes(&self) -> Result<Vec<u8>, EmbeddingError> {
        serde_json::to_vec(self)
            .map_err(|error| EmbeddingError::Execution(format!("serialize EmbeddingJob: {error}")))
    }

    pub fn from_definition_bytes(bytes: &[u8]) -> Result<Self, EmbeddingError> {
        serde_json::from_slice(bytes).map_err(|error| {
            EmbeddingError::Execution(format!("deserialize EmbeddingJob: {error}"))
        })
    }

    /// Whether a worker may still drive this job (pending or scheduled retry).
    pub fn is_resumable(&self) -> bool {
        match self.status {
            EmbeddingGenerationStatus::Pending => true,
            EmbeddingGenerationStatus::Failed => self.next_retry.is_some(),
            EmbeddingGenerationStatus::Ready => false,
        }
    }

    /// Invalidate a stale job when the source row fingerprint changed (P1.5-X3).
    ///
    /// Returns `true` when the job was invalidated. Ready jobs with a matching
    /// fingerprint are left alone so a successful result is not discarded.
    pub fn invalidate_if_source_changed(&mut self, current_source_fingerprint: [u8; 32]) -> bool {
        if self.source_fingerprint == current_source_fingerprint {
            return false;
        }
        if matches!(self.status, EmbeddingGenerationStatus::Ready) {
            // Already published against the old fingerprint — mark Failed so
            // ANN eligibility drops and a new job can be submitted.
            self.status = EmbeddingGenerationStatus::Failed;
            self.next_retry = None;
            self.last_error = Some("source row changed; embedding job invalidated".into());
            return true;
        }
        self.status = EmbeddingGenerationStatus::Failed;
        self.next_retry = None;
        self.last_error = Some("source row changed; embedding job invalidated".into());
        true
    }

    /// Record a provider failure and schedule a retry without producing a vector.
    pub fn schedule_retry(&mut self, error: impl Into<String>, next_retry_unix_micros: u64) {
        self.attempts = self.attempts.saturating_add(1);
        self.status = EmbeddingGenerationStatus::Failed;
        self.next_retry = Some(next_retry_unix_micros);
        self.last_error = Some(error.into());
    }

    /// Complete the job with a ready vector. Idempotent: a second call with the
    /// same source fingerprint is a no-op and does not increment attempts
    /// (P1.5-X4: retry does not duplicate updates).
    pub fn complete_ready(&mut self, source_fingerprint: [u8; 32]) -> Result<bool, EmbeddingError> {
        if self.source_fingerprint != source_fingerprint {
            return Err(EmbeddingError::Execution(
                "complete_ready source fingerprint does not match job".into(),
            ));
        }
        if matches!(self.status, EmbeddingGenerationStatus::Ready) {
            // Already applied — do not re-apply.
            return Ok(false);
        }
        self.status = EmbeddingGenerationStatus::Ready;
        self.next_retry = None;
        self.last_error = None;
        self.attempts = self.attempts.saturating_add(1);
        Ok(true)
    }

    /// Resume after restart: Pending/Failed-with-retry jobs remain workable.
    pub fn resume_after_restart(&mut self) -> Result<(), EmbeddingError> {
        if matches!(self.status, EmbeddingGenerationStatus::Ready) {
            return Ok(());
        }
        if !self.is_resumable() {
            return Err(EmbeddingError::Execution(format!(
                "embedding job {} is not resumable (status={:?}, next_retry={:?})",
                self.job_id, self.status, self.next_retry
            )));
        }
        // Re-queue as Pending so a worker picks it up; preserve attempts/error.
        self.status = EmbeddingGenerationStatus::Pending;
        Ok(())
    }
}

/// Whether a generated-embedding status may participate in ANN (P1.5-T3).
pub fn embedding_status_is_ann_eligible(status: EmbeddingGenerationStatus) -> bool {
    matches!(status, EmbeddingGenerationStatus::Ready)
}

// ---------------------------------------------------------------------------
// Re-embedding build-and-publish (P0.7-T5)
// ---------------------------------------------------------------------------

/// Lifecycle of a re-embedding build-and-publish job.
///
/// Flow: `Pending` → `Running` (bounded batches) → `Ready` (validated hidden
/// generation) → atomic [`ReEmbeddingCoordinator::publish_reembedding`] which
/// bumps the publish fence. Failures land in `Failed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReEmbeddingState {
    Pending,
    Running,
    /// Hidden generation is complete and validated; not yet published.
    Ready,
    Failed,
}

/// One published (or about-to-publish) embedding generation for a column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddingGenerationSlot {
    /// Monotonic generation id within the coordinator.
    pub generation_id: u64,
    /// Cryptographic semantic identity of every vector in this generation.
    pub semantic_identity: EmbeddingProviderRef,
    /// Publish fence value that installed this slot as active (`0` while hidden).
    pub publish_fence: u64,
}

/// Persistent re-embedding job definition (P0.7-T5).
///
/// Builds a hidden vector+ANN generation under a new semantic identity, then
/// atomically publishes it. Survives interruption via `batch_cursor` resume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReEmbeddingJob {
    pub job_id: u64,
    pub table_id: u64,
    pub column_id: u16,
    /// Identity currently serving queries (swapped out on publish).
    pub source_identity: EmbeddingProviderRef,
    /// Identity of the hidden generation being built.
    pub target_identity: EmbeddingProviderRef,
    pub hidden_generation_id: u64,
    pub source_generation_id: u64,
    pub state: ReEmbeddingState,
    /// Monotonic fence observed/updated at publish (atomic publish marker).
    pub publish_fence: u64,
    /// Next row offset for bounded resumable batches.
    pub batch_cursor: u64,
    pub rows_total: u64,
    pub rows_done: u64,
    pub last_error: Option<String>,
    /// Snapshot pins holding the retired generation (stub: decremented on retire).
    pub pins_remaining: u32,
}

impl ReEmbeddingJob {
    pub fn to_definition_bytes(&self) -> Result<Vec<u8>, EmbeddingError> {
        serde_json::to_vec(self).map_err(|error| {
            EmbeddingError::Execution(format!("serialize ReEmbeddingJob: {error}"))
        })
    }

    pub fn from_definition_bytes(bytes: &[u8]) -> Result<Self, EmbeddingError> {
        serde_json::from_slice(bytes).map_err(|error| {
            EmbeddingError::Execution(format!("deserialize ReEmbeddingJob: {error}"))
        })
    }
}

/// In-memory re-embedding coordinator: hidden build + atomic publish fence.
///
/// Minimal product-path workflow for P0.7-T5. Storage wiring for actual vector
/// materialization is deferred; this enforces identity, generation, fence, and
/// resume invariants that retrieval and operators rely on.
#[derive(Debug)]
pub struct ReEmbeddingCoordinator {
    next_job_id: AtomicU64,
    next_generation_id: AtomicU64,
    publish_fence: AtomicU64,
    inner: Mutex<ReEmbeddingInner>,
}

#[derive(Debug, Default)]
struct ReEmbeddingInner {
    /// Active published generation per `(table_id, column_id)`.
    active: BTreeMap<(u64, u16), EmbeddingGenerationSlot>,
    /// Hidden generation being built (at most one per column).
    hidden: BTreeMap<(u64, u16), EmbeddingGenerationSlot>,
    /// Retired generations waiting for pin expiry.
    retired: BTreeMap<(u64, u16), EmbeddingGenerationSlot>,
    jobs: BTreeMap<u64, ReEmbeddingJob>,
}

impl Default for ReEmbeddingCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl ReEmbeddingCoordinator {
    pub fn new() -> Self {
        Self {
            next_job_id: AtomicU64::new(1),
            next_generation_id: AtomicU64::new(1),
            publish_fence: AtomicU64::new(0),
            inner: Mutex::new(ReEmbeddingInner::default()),
        }
    }

    /// Seed or replace the active published generation (bootstrap / tests).
    pub fn install_active(
        &self,
        table_id: u64,
        column_id: u16,
        identity: EmbeddingProviderRef,
    ) -> EmbeddingGenerationSlot {
        let generation_id = self.next_generation_id.fetch_add(1, Ordering::SeqCst);
        let fence = self.publish_fence.load(Ordering::SeqCst);
        let slot = EmbeddingGenerationSlot {
            generation_id,
            semantic_identity: identity,
            publish_fence: fence,
        };
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .active
            .insert((table_id, column_id), slot.clone());
        slot
    }

    pub fn active_slot(&self, table_id: u64, column_id: u16) -> Option<EmbeddingGenerationSlot> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .active
            .get(&(table_id, column_id))
            .cloned()
    }

    pub fn hidden_slot(&self, table_id: u64, column_id: u16) -> Option<EmbeddingGenerationSlot> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .hidden
            .get(&(table_id, column_id))
            .cloned()
    }

    pub fn publish_fence(&self) -> u64 {
        self.publish_fence.load(Ordering::SeqCst)
    }

    pub fn get_job(&self, job_id: u64) -> Option<ReEmbeddingJob> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .jobs
            .get(&job_id)
            .cloned()
    }

    /// Start a re-embedding job: records a hidden generation under `target`.
    ///
    /// Fails if `target` has the same cryptographic fingerprint as the active
    /// source, or if a re-embedding is already in flight for the column.
    pub fn start_reembedding(
        &self,
        table_id: u64,
        column_id: u16,
        target_identity: EmbeddingProviderRef,
        rows_total: u64,
    ) -> Result<ReEmbeddingJob, EmbeddingError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let key = (table_id, column_id);
        if inner.hidden.contains_key(&key) {
            return Err(EmbeddingError::Execution(format!(
                "re-embedding already in progress for table {table_id} column {column_id}"
            )));
        }
        let source = inner.active.get(&key).cloned().ok_or_else(|| {
            EmbeddingError::Execution(format!(
                "no active embedding generation for table {table_id} column {column_id}"
            ))
        })?;
        if source.semantic_identity.fingerprint_sha256() == target_identity.fingerprint_sha256() {
            return Err(EmbeddingError::Execution(
                "re-embedding target semantic identity must differ from active source".into(),
            ));
        }
        let hidden_generation_id = self.next_generation_id.fetch_add(1, Ordering::SeqCst);
        let job_id = self.next_job_id.fetch_add(1, Ordering::SeqCst);
        let job = ReEmbeddingJob {
            job_id,
            table_id,
            column_id,
            source_identity: source.semantic_identity.clone(),
            target_identity: target_identity.clone(),
            hidden_generation_id,
            source_generation_id: source.generation_id,
            state: ReEmbeddingState::Pending,
            publish_fence: self.publish_fence.load(Ordering::SeqCst),
            batch_cursor: 0,
            rows_total,
            rows_done: 0,
            last_error: None,
            pins_remaining: 0,
        };
        inner.hidden.insert(
            key,
            EmbeddingGenerationSlot {
                generation_id: hidden_generation_id,
                semantic_identity: target_identity,
                publish_fence: 0,
            },
        );
        inner.jobs.insert(job_id, job.clone());
        Ok(job)
    }

    /// Advance a re-embedding job by up to `batch_size` rows (resumable).
    ///
    /// Transitions `Pending`/`Running` → `Running`, and → `Ready` when all rows
    /// are processed. Re-running after interrupt resumes from `batch_cursor`.
    pub fn build_reembedding_batch(
        &self,
        job_id: u64,
        batch_size: u64,
    ) -> Result<ReEmbeddingJob, EmbeddingError> {
        if batch_size == 0 {
            return Err(EmbeddingError::Execution(
                "re-embedding batch_size must be > 0".into(),
            ));
        }
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let job = inner.jobs.get_mut(&job_id).ok_or_else(|| {
            EmbeddingError::Execution(format!("re-embedding job {job_id} not found"))
        })?;
        match job.state {
            ReEmbeddingState::Pending | ReEmbeddingState::Running => {}
            ReEmbeddingState::Ready => return Ok(job.clone()),
            ReEmbeddingState::Failed => {
                return Err(EmbeddingError::Execution(format!(
                    "re-embedding job {job_id} is Failed: {}",
                    job.last_error.as_deref().unwrap_or("unknown")
                )));
            }
        }
        job.state = ReEmbeddingState::Running;
        let remaining = job.rows_total.saturating_sub(job.batch_cursor);
        let step = remaining.min(batch_size);
        job.batch_cursor = job.batch_cursor.saturating_add(step);
        job.rows_done = job.batch_cursor;
        if job.batch_cursor >= job.rows_total {
            job.state = ReEmbeddingState::Ready;
        }
        Ok(job.clone())
    }

    /// Mark a job failed (e.g. validation / smoke failure). Drops the hidden slot.
    pub fn fail_reembedding(
        &self,
        job_id: u64,
        reason: impl Into<String>,
    ) -> Result<ReEmbeddingJob, EmbeddingError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let (key, hidden_generation_id, finished) = {
            let job = inner.jobs.get_mut(&job_id).ok_or_else(|| {
                EmbeddingError::Execution(format!("re-embedding job {job_id} not found"))
            })?;
            if matches!(job.state, ReEmbeddingState::Ready) && job.publish_fence > 0 {
                // Already published under a prior fence — do not tear down active.
                return Err(EmbeddingError::Execution(format!(
                    "re-embedding job {job_id} already published"
                )));
            }
            let key = (job.table_id, job.column_id);
            let hidden_generation_id = job.hidden_generation_id;
            job.state = ReEmbeddingState::Failed;
            job.last_error = Some(reason.into());
            (key, hidden_generation_id, job.clone())
        };
        // Only drop hidden if it still matches this job's generation.
        if inner
            .hidden
            .get(&key)
            .is_some_and(|h| h.generation_id == hidden_generation_id)
        {
            inner.hidden.remove(&key);
        }
        Ok(finished)
    }

    /// Atomically publish a `Ready` re-embedding: swap active generation and
    /// bump the global publish fence in one critical section.
    pub fn publish_reembedding(&self, job_id: u64) -> Result<ReEmbeddingJob, EmbeddingError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let job = inner.jobs.get(&job_id).cloned().ok_or_else(|| {
            EmbeddingError::Execution(format!("re-embedding job {job_id} not found"))
        })?;
        if job.state != ReEmbeddingState::Ready {
            return Err(EmbeddingError::Execution(format!(
                "re-embedding job {job_id} is {:?}, expected Ready",
                job.state
            )));
        }
        let key = (job.table_id, job.column_id);
        let hidden = inner.hidden.remove(&key).ok_or_else(|| {
            EmbeddingError::Execution(format!(
                "missing hidden generation for re-embedding job {job_id}"
            ))
        })?;
        if hidden.generation_id != job.hidden_generation_id
            || hidden.semantic_identity.fingerprint_sha256()
                != job.target_identity.fingerprint_sha256()
        {
            // Restore hidden on mismatch so the job can be retried/inspected.
            inner.hidden.insert(key, hidden);
            return Err(EmbeddingError::Execution(
                "hidden generation identity/id mismatch at publish".into(),
            ));
        }
        let new_fence = self.publish_fence.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(old) = inner.active.remove(&key) {
            // Pins hold the retired generation until expire (stub default 1).
            let mut retired = old;
            retired.publish_fence = new_fence;
            inner.retired.insert(key, retired);
        }
        let published = EmbeddingGenerationSlot {
            generation_id: hidden.generation_id,
            semantic_identity: hidden.semantic_identity,
            publish_fence: new_fence,
        };
        inner.active.insert(key, published);
        let job = inner.jobs.get_mut(&job_id).expect("job exists");
        job.publish_fence = new_fence;
        // Keep state Ready but fence non-zero signals published; pins_remaining
        // tracks retirement of the previous generation.
        job.pins_remaining = 1;
        Ok(job.clone())
    }

    /// Drop retired generation when pins expire (stub: one call clears pins).
    pub fn retire_old_after_pins(
        &self,
        table_id: u64,
        column_id: u16,
    ) -> Result<Option<EmbeddingGenerationSlot>, EmbeddingError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let key = (table_id, column_id);
        // Clear pin counts on any published jobs for this column.
        for job in inner.jobs.values_mut() {
            if job.table_id == table_id && job.column_id == column_id && job.pins_remaining > 0 {
                job.pins_remaining = 0;
            }
        }
        Ok(inner.retired.remove(&key))
    }

    /// Fail-closed query gate: the caller's model identity must match the
    /// active published generation (P0.7-X5: old model cannot search new index).
    pub fn require_active_identity(
        &self,
        table_id: u64,
        column_id: u16,
        query_identity: &EmbeddingProviderRef,
    ) -> Result<EmbeddingGenerationSlot, EmbeddingError> {
        let active = self
            .active_slot(table_id, column_id)
            .ok_or(EmbeddingError::NoActiveAnnIdentity(column_id))?;
        if active.semantic_identity.fingerprint_sha256() != query_identity.fingerprint_sha256() {
            return Err(EmbeddingError::AnnSemanticIdentityMismatch {
                column_id,
                expected: active.semantic_identity.fingerprint_sha256(),
                got: query_identity.fingerprint_sha256(),
            });
        }
        Ok(active)
    }

    /// Whether a generation id is the currently published active generation.
    pub fn is_active_generation(&self, table_id: u64, column_id: u16, generation_id: u64) -> bool {
        self.active_slot(table_id, column_id)
            .is_some_and(|s| s.generation_id == generation_id)
    }

    /// Source rows changed while a hidden generation was building (P0.7-X8).
    ///
    /// Extends `rows_total` so subsequent [`Self::build_reembedding_batch`]
    /// calls catch up on the additional source updates before Ready. Safe to
    /// call after Ready only when not yet published (resets to Running).
    pub fn note_source_updates_during_backfill(
        &self,
        job_id: u64,
        additional_rows: u64,
    ) -> Result<ReEmbeddingJob, EmbeddingError> {
        if additional_rows == 0 {
            return self.get_job(job_id).ok_or_else(|| {
                EmbeddingError::Execution(format!("re-embedding job {job_id} not found"))
            });
        }
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let job = inner.jobs.get_mut(&job_id).ok_or_else(|| {
            EmbeddingError::Execution(format!("re-embedding job {job_id} not found"))
        })?;
        match job.state {
            ReEmbeddingState::Pending | ReEmbeddingState::Running => {}
            ReEmbeddingState::Ready => {
                // Catch-up before publish: reopen the build window.
                if job.publish_fence > 0 {
                    return Err(EmbeddingError::Execution(format!(
                        "re-embedding job {job_id} already published; cannot catch up"
                    )));
                }
                job.state = ReEmbeddingState::Running;
            }
            ReEmbeddingState::Failed => {
                return Err(EmbeddingError::Execution(format!(
                    "re-embedding job {job_id} is Failed"
                )));
            }
        }
        job.rows_total = job.rows_total.saturating_add(additional_rows);
        Ok(job.clone())
    }
}

/// Aggregate readiness of embedding providers required by schemas (P1.5-T2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderReadiness {
    Ready,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderReadinessEntry {
    pub provider_id: String,
    pub required: bool,
    pub readiness: ProviderReadiness,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderReadinessReport {
    pub overall: ProviderReadiness,
    pub providers: Vec<ProviderReadinessEntry>,
}

impl ProviderReadinessReport {
    pub fn allows_generated_writes(&self) -> bool {
        !matches!(self.overall, ProviderReadiness::Unavailable)
            && self.providers.iter().all(|entry| {
                !entry.required || !matches!(entry.readiness, ProviderReadiness::Unavailable)
            })
    }
}

/// Enumerate required provider ids and compare against the process registry.
/// Missing required providers fail closed (P1.5-T2 / X5).
pub fn check_provider_readiness(
    registry: &EmbeddingProviderRegistry,
    required_provider_ids: &[String],
) -> ProviderReadinessReport {
    let mut providers = Vec::new();
    let mut any_unavailable = false;
    let mut any_degraded = false;
    for provider_id in required_provider_ids {
        match registry.status(provider_id) {
            Some(status) => {
                let readiness = match status.health {
                    ProviderHealth::Ready => ProviderReadiness::Ready,
                    ProviderHealth::Degraded => {
                        any_degraded = true;
                        ProviderReadiness::Degraded
                    }
                    ProviderHealth::Unavailable => {
                        any_unavailable = true;
                        ProviderReadiness::Unavailable
                    }
                };
                providers.push(ProviderReadinessEntry {
                    provider_id: provider_id.clone(),
                    required: true,
                    readiness,
                    detail: format!("{:?}", status.health),
                });
            }
            None => {
                any_unavailable = true;
                providers.push(ProviderReadinessEntry {
                    provider_id: provider_id.clone(),
                    required: true,
                    readiness: ProviderReadiness::Unavailable,
                    detail: "provider not registered".into(),
                });
            }
        }
    }
    let overall = if any_unavailable {
        ProviderReadiness::Unavailable
    } else if any_degraded {
        ProviderReadiness::Degraded
    } else {
        ProviderReadiness::Ready
    };
    ProviderReadinessReport { overall, providers }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embedding::{
        EmbeddingNormalization, EmbeddingProvider, EmbeddingProviderRegistry, FixedVectorProvider,
    };
    use std::sync::Arc;

    fn identity(version: &str, artifact: u8) -> EmbeddingProviderRef {
        FixedVectorProvider::new(
            "text-test",
            "length-and-sum",
            version,
            EmbeddingNormalization::None,
            vec![0.0, f32::from(artifact)],
        )
        .semantic_identity()
    }

    #[test]
    fn reembedding_publish_is_atomic_and_swaps_identity() {
        let coord = ReEmbeddingCoordinator::new();
        let source = identity("1", 0xAA);
        let target = identity("2", 0xBB);
        assert_ne!(source.fingerprint_sha256(), target.fingerprint_sha256());
        let active = coord.install_active(1, 3, source.clone());
        assert_eq!(active.semantic_identity, source);

        let job = coord.start_reembedding(1, 3, target.clone(), 10).unwrap();
        assert_eq!(job.state, ReEmbeddingState::Pending);
        assert_eq!(job.source_generation_id, active.generation_id);
        assert_ne!(job.hidden_generation_id, active.generation_id);

        // Bounded batches + interrupt/resume.
        let mid = coord.build_reembedding_batch(job.job_id, 4).unwrap();
        assert_eq!(mid.state, ReEmbeddingState::Running);
        assert_eq!(mid.batch_cursor, 4);
        let resumed = coord.build_reembedding_batch(job.job_id, 100).unwrap();
        assert_eq!(resumed.state, ReEmbeddingState::Ready);
        assert_eq!(resumed.rows_done, 10);

        let fence_before = coord.publish_fence();
        let published = coord.publish_reembedding(job.job_id).unwrap();
        assert!(published.publish_fence > fence_before);
        assert_eq!(coord.publish_fence(), published.publish_fence);

        let new_active = coord.active_slot(1, 3).unwrap();
        assert_eq!(new_active.semantic_identity, target);
        assert_eq!(new_active.generation_id, job.hidden_generation_id);
        assert_eq!(new_active.publish_fence, published.publish_fence);
        assert!(coord.hidden_slot(1, 3).is_none());

        // Old model cannot search new index.
        let err = coord.require_active_identity(1, 3, &source).unwrap_err();
        assert!(matches!(
            err,
            EmbeddingError::AnnSemanticIdentityMismatch { .. }
        ));
        coord.require_active_identity(1, 3, &target).unwrap();

        let retired = coord.retire_old_after_pins(1, 3).unwrap();
        assert_eq!(retired.unwrap().semantic_identity, source);
        assert_eq!(coord.get_job(job.job_id).unwrap().pins_remaining, 0);
    }

    #[test]
    fn reembedding_rejects_same_identity_and_double_start() {
        let coord = ReEmbeddingCoordinator::new();
        let source = identity("1", 0xAA);
        coord.install_active(1, 3, source.clone());
        let err = coord
            .start_reembedding(1, 3, source.clone(), 1)
            .unwrap_err();
        assert!(err.to_string().contains("must differ"));

        let target = identity("2", 0xBB);
        coord.start_reembedding(1, 3, target.clone(), 1).unwrap();
        let err = coord.start_reembedding(1, 3, target, 1).unwrap_err();
        assert!(err.to_string().contains("already in progress"));
    }

    #[test]
    fn reembedding_job_definition_round_trips() {
        let coord = ReEmbeddingCoordinator::new();
        let source = identity("1", 0xAA);
        let target = identity("2", 0xBB);
        coord.install_active(9, 7, source);
        let job = coord.start_reembedding(9, 7, target, 3).unwrap();
        let bytes = job.to_definition_bytes().unwrap();
        let restored = ReEmbeddingJob::from_definition_bytes(&bytes).unwrap();
        assert_eq!(restored, job);
    }

    #[test]
    fn pending_embedding_job_persists_in_job_registry() {
        let dir = tempfile::tempdir().unwrap();
        let registry = crate::jobs::JobRegistry::open(dir.path(), None).unwrap();
        let job = EmbeddingJob::new_pending(1, 42, 7, [9u8; 32], [8u8; 32]);
        let def = job.to_definition_bytes().unwrap();
        let job_id = registry
            .submit_with_definition(
                crate::jobs::JobKind::EmbeddingGeneration,
                crate::jobs::JobTarget {
                    table: "docs".into(),
                    index: None,
                },
                Some(def),
            )
            .unwrap();
        drop(registry);

        let reopened = crate::jobs::JobRegistry::open(dir.path(), None).unwrap();
        let record = reopened.get(job_id).expect("job survives restart");
        assert_eq!(record.state, crate::jobs::JobState::Pending);
        assert_eq!(record.kind, crate::jobs::JobKind::EmbeddingGeneration);
        let restored =
            EmbeddingJob::from_definition_bytes(record.definition.as_ref().unwrap()).unwrap();
        assert_eq!(restored.row_id, 42);
        assert_eq!(restored.column_id, 7);
        assert_eq!(restored.status, EmbeddingGenerationStatus::Pending);
        assert!(!embedding_status_is_ann_eligible(restored.status));
    }

    #[test]
    fn missing_provider_fails_closed_and_health_visible() {
        let registry = EmbeddingProviderRegistry::new();
        let report = check_provider_readiness(&registry, &["missing-prov".into()]);
        assert_eq!(report.overall, ProviderReadiness::Unavailable);
        assert!(!report.allows_generated_writes());

        registry
            .register_new(Arc::new(FixedVectorProvider::new(
                "local-test",
                "fixed",
                "1",
                EmbeddingNormalization::L2,
                vec![0.0, 1.0],
            )))
            .unwrap();
        let ok = check_provider_readiness(&registry, &["local-test".into()]);
        assert_eq!(ok.overall, ProviderReadiness::Ready);
        assert!(ok.allows_generated_writes());
    }

    #[test]
    fn ann_excludes_non_ready_status() {
        assert!(embedding_status_is_ann_eligible(
            EmbeddingGenerationStatus::Ready
        ));
        assert!(!embedding_status_is_ann_eligible(
            EmbeddingGenerationStatus::Pending
        ));
        assert!(!embedding_status_is_ann_eligible(
            EmbeddingGenerationStatus::Failed
        ));
    }

    // P0.7-X8: source updates during backfill catch up before publish.
    #[test]
    fn reembedding_source_updates_during_backfill_catch_up() {
        let coord = ReEmbeddingCoordinator::new();
        let source = identity("1", 0xAA);
        let target = identity("2", 0xBB);
        coord.install_active(1, 3, source);
        let job = coord.start_reembedding(1, 3, target, 4).unwrap();
        let mid = coord.build_reembedding_batch(job.job_id, 4).unwrap();
        assert_eq!(mid.state, ReEmbeddingState::Ready);
        assert_eq!(mid.rows_done, 4);

        // Two source rows changed while the hidden gen finished building.
        let caught = coord
            .note_source_updates_during_backfill(job.job_id, 2)
            .unwrap();
        assert_eq!(caught.state, ReEmbeddingState::Running);
        assert_eq!(caught.rows_total, 6);
        assert_eq!(caught.batch_cursor, 4);

        let finished = coord.build_reembedding_batch(job.job_id, 10).unwrap();
        assert_eq!(finished.state, ReEmbeddingState::Ready);
        assert_eq!(finished.rows_done, 6);
        coord.publish_reembedding(job.job_id).unwrap();
    }

    // P1.5-X2: worker restart resumes a pending embedding job.
    #[test]
    fn embedding_job_worker_restart_resumes() {
        let dir = tempfile::tempdir().unwrap();
        let registry = crate::jobs::JobRegistry::open(dir.path(), None).unwrap();
        let mut job = EmbeddingJob::new_pending(1, 7, 3, [1u8; 32], [2u8; 32]);
        let def = job.to_definition_bytes().unwrap();
        let job_id = registry
            .submit_with_definition(
                crate::jobs::JobKind::EmbeddingGeneration,
                crate::jobs::JobTarget {
                    table: "docs".into(),
                    index: None,
                },
                Some(def),
            )
            .unwrap();
        job.job_id = job_id;
        drop(registry);

        // Restart: reopen registry and resume definition.
        let reopened = crate::jobs::JobRegistry::open(dir.path(), None).unwrap();
        let record = reopened.get(job_id).expect("job survives restart");
        let mut restored =
            EmbeddingJob::from_definition_bytes(record.definition.as_ref().unwrap()).unwrap();
        restored.job_id = job_id;
        assert!(restored.is_resumable());
        restored.resume_after_restart().unwrap();
        assert_eq!(restored.status, EmbeddingGenerationStatus::Pending);
        let applied = restored.complete_ready([1u8; 32]).unwrap();
        assert!(applied);
        assert_eq!(restored.status, EmbeddingGenerationStatus::Ready);
        assert!(embedding_status_is_ann_eligible(restored.status));
    }

    // P1.5-X3: source row changes invalidate a stale job.
    #[test]
    fn embedding_job_source_change_invalidates() {
        let mut job = EmbeddingJob::new_pending(1, 1, 1, [0xAA; 32], [0xBB; 32]);
        assert!(!job.invalidate_if_source_changed([0xAA; 32]));
        assert!(job.invalidate_if_source_changed([0xCC; 32]));
        assert_eq!(job.status, EmbeddingGenerationStatus::Failed);
        assert!(job.next_retry.is_none());
        assert!(job
            .last_error
            .as_deref()
            .unwrap_or("")
            .contains("source row changed"));
        assert!(!job.is_resumable());
    }

    // P1.5-X4: retry/complete does not duplicate updates.
    #[test]
    fn embedding_job_retry_does_not_duplicate() {
        let mut job = EmbeddingJob::new_pending(1, 1, 1, [1u8; 32], [2u8; 32]);
        job.schedule_retry("provider timeout", 100);
        assert_eq!(job.attempts, 1);
        assert!(job.is_resumable());
        job.resume_after_restart().unwrap();
        assert!(job.complete_ready([1u8; 32]).unwrap());
        assert_eq!(job.attempts, 2);
        // Second complete is a no-op (no duplicate vector publish).
        assert!(!job.complete_ready([1u8; 32]).unwrap());
        assert_eq!(job.attempts, 2);
        assert_eq!(job.status, EmbeddingGenerationStatus::Ready);
    }
}
