//! Online operations as persistent resumable jobs (P1.6).
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum OpsJobKind {
    TransferLeader, MoveReplica, SplitTablet, MergeTablets, Backup, Restore,
    IndexBuild, SchemaBackfill, KeyRotation, RollingUpgrade, NodeReplacement,
    TabletMovement, OnlineIndexBuild, RestoreVerification, StatsCollection, LeaderRebalance,
}
impl OpsJobKind {
    pub const PRODUCT: [OpsJobKind; 9] = [
        Self::TransferLeader, Self::MoveReplica, Self::SplitTablet, Self::MergeTablets,
        Self::Backup, Self::Restore, Self::IndexBuild, Self::SchemaBackfill, Self::KeyRotation,
    ];
    pub const ALL: [OpsJobKind; 16] = [
        Self::TransferLeader, Self::MoveReplica, Self::SplitTablet, Self::MergeTablets,
        Self::Backup, Self::Restore, Self::IndexBuild, Self::SchemaBackfill, Self::KeyRotation,
        Self::RollingUpgrade, Self::NodeReplacement, Self::TabletMovement, Self::OnlineIndexBuild,
        Self::RestoreVerification, Self::StatsCollection, Self::LeaderRebalance,
    ];
    pub fn name(self) -> &'static str {
        match self {
            Self::TransferLeader => "transfer_leader",
            Self::MoveReplica | Self::TabletMovement => "move_replica",
            Self::SplitTablet => "split_tablet",
            Self::MergeTablets => "merge_tablets",
            Self::Backup => "backup",
            Self::Restore | Self::RestoreVerification => "restore",
            Self::IndexBuild | Self::OnlineIndexBuild => "index_build",
            Self::SchemaBackfill => "schema_backfill",
            Self::KeyRotation => "key_rotation",
            Self::RollingUpgrade => "rolling_upgrade",
            Self::NodeReplacement => "node_replacement",
            Self::StatsCollection => "stats_collection",
            Self::LeaderRebalance => "leader_rebalance",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpsJobState { Pending, Running, Paused, Cancelling, Succeeded, Failed, Cancelled }
impl OpsJobState {
    pub fn name(self) -> &'static str {
        match self {
            Self::Pending => "pending", Self::Running => "running", Self::Paused => "paused",
            Self::Cancelling => "cancelling", Self::Succeeded => "succeeded",
            Self::Failed => "failed", Self::Cancelled => "cancelled",
        }
    }
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpsJob {
    pub job_id: String, pub kind: OpsJobKind, pub state: OpsJobState, pub phase: u32,
    pub progress: String, pub params: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default)] pub metadata_version: u64,
    #[serde(default)] pub publish_fenced: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OpsJobsFile {
    version: u32, next: u64, jobs: BTreeMap<String, OpsJob>,
    #[serde(default)] idempotency: BTreeMap<String, String>,
}
#[derive(Debug, Clone)]
pub struct OpsJobStore {
    jobs: BTreeMap<String, OpsJob>, idempotency: BTreeMap<String, String>,
    next: u64, path: Option<PathBuf>,
}
impl Default for OpsJobStore { fn default() -> Self { Self::new() } }
impl OpsJobStore {
    pub fn new() -> Self { Self { jobs: BTreeMap::new(), idempotency: BTreeMap::new(), next: 0, path: None } }
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, OpsJobError> {
        let path = dir.as_ref().join("OPS_JOBS");
        if !path.exists() {
            let store = Self { jobs: BTreeMap::new(), idempotency: BTreeMap::new(), next: 0, path: Some(path) };
            store.persist()?; return Ok(store);
        }
        let bytes = std::fs::read(&path).map_err(|e| OpsJobError::Storage(e.to_string()))?;
        let mut file: OpsJobsFile = serde_json::from_slice(&bytes).map_err(|e| OpsJobError::Storage(e.to_string()))?;
        for job in file.jobs.values_mut() {
            if job.state == OpsJobState::Running {
                job.state = OpsJobState::Pending;
                job.progress = format!("resumed after restart at phase {}", job.phase);
            } else if job.state == OpsJobState::Cancelling {
                job.state = OpsJobState::Cancelled;
                job.progress = "cancelled during restart".into();
            }
        }
        Ok(Self { jobs: file.jobs, idempotency: file.idempotency, next: file.next, path: Some(path) })
    }
    fn persist(&self) -> Result<(), OpsJobError> {
        let Some(path) = &self.path else { return Ok(()) };
        let file = OpsJobsFile { version: 1, next: self.next, jobs: self.jobs.clone(), idempotency: self.idempotency.clone() };
        let bytes = serde_json::to_vec_pretty(&file).map_err(|e| OpsJobError::Storage(e.to_string()))?;
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &bytes).map_err(|e| OpsJobError::Storage(e.to_string()))?;
        std::fs::rename(&tmp, path).map_err(|e| OpsJobError::Storage(e.to_string()))?;
        Ok(())
    }
    pub fn submit(&mut self, kind: OpsJobKind, params: BTreeMap<String, String>) -> Result<OpsJob, OpsJobError> {
        self.submit_with(kind, params, None, 0)
    }
    pub fn submit_with(&mut self, kind: OpsJobKind, params: BTreeMap<String, String>, idempotency_key: Option<String>, metadata_version: u64) -> Result<OpsJob, OpsJobError> {
        if let Some(key) = idempotency_key.as_ref() {
            if let Some(existing_id) = self.idempotency.get(key) {
                if let Some(job) = self.jobs.get(existing_id) { return Ok(job.clone()); }
            }
        }
        self.next += 1;
        let job_id = format!("ops-{}-{}", kind.name(), self.next);
        let job = OpsJob {
            job_id: job_id.clone(), kind, state: OpsJobState::Pending, phase: 0,
            progress: "submitted".into(), params, idempotency_key: idempotency_key.clone(),
            metadata_version, publish_fenced: false, error: None,
        };
        if let Some(key) = idempotency_key { self.idempotency.insert(key, job_id.clone()); }
        self.jobs.insert(job_id, job.clone());
        self.persist()?;
        Ok(job)
    }
    pub fn accepted_response(job: &OpsJob) -> serde_json::Value {
        serde_json::json!({"status":"accepted","job_id":job.job_id,"state":job.state.name(),"metadata_version":job.metadata_version,"kind":job.kind.name()})
    }
    pub fn start(&mut self, job_id: &str) -> Result<&OpsJob, OpsJobError> {
        let job = self.jobs.get_mut(job_id).ok_or(OpsJobError::NotFound)?;
        match job.state {
            OpsJobState::Pending | OpsJobState::Paused => {
                job.state = OpsJobState::Running; job.progress = "running".into();
                self.persist()?; Ok(self.jobs.get(job_id).expect("present"))
            }
            other => Err(OpsJobError::InvalidTransition { from: format!("{other:?}"), to: "Running".into() }),
        }
    }
    pub fn advance_phase(&mut self, job_id: &str, phase: u32, progress: impl Into<String>) -> Result<&OpsJob, OpsJobError> {
        let job = self.jobs.get_mut(job_id).ok_or(OpsJobError::NotFound)?;
        if job.state != OpsJobState::Running { return Err(OpsJobError::InvalidTransition { from: format!("{:?}", job.state), to: "advance_phase".into() }); }
        job.phase = phase; job.progress = progress.into(); self.persist()?; Ok(self.jobs.get(job_id).expect("present"))
    }
    pub fn mark_publish_fence(&mut self, job_id: &str) -> Result<&OpsJob, OpsJobError> {
        let job = self.jobs.get_mut(job_id).ok_or(OpsJobError::NotFound)?;
        if job.state != OpsJobState::Running { return Err(OpsJobError::InvalidTransition { from: format!("{:?}", job.state), to: "publish_fence".into() }); }
        job.publish_fenced = true; job.progress = "publish fence".into(); self.persist()?; Ok(self.jobs.get(job_id).expect("present"))
    }
    pub fn succeed(&mut self, job_id: &str) -> Result<&OpsJob, OpsJobError> {
        let job = self.jobs.get_mut(job_id).ok_or(OpsJobError::NotFound)?;
        if job.state != OpsJobState::Running { return Err(OpsJobError::InvalidTransition { from: format!("{:?}", job.state), to: "Succeeded".into() }); }
        job.state = OpsJobState::Succeeded; job.progress = "succeeded".into(); self.persist()?; Ok(self.jobs.get(job_id).expect("present"))
    }
    pub fn fail(&mut self, job_id: &str, error: impl Into<String>) -> Result<&OpsJob, OpsJobError> {
        let job = self.jobs.get_mut(job_id).ok_or(OpsJobError::NotFound)?;
        if job.state.is_terminal() { return Err(OpsJobError::InvalidTransition { from: format!("{:?}", job.state), to: "Failed".into() }); }
        job.state = OpsJobState::Failed; job.error = Some(error.into()); job.progress = "failed".into(); self.persist()?; Ok(self.jobs.get(job_id).expect("present"))
    }
    pub fn pause(&mut self, job_id: &str) -> Result<&OpsJob, OpsJobError> {
        let job = self.jobs.get_mut(job_id).ok_or(OpsJobError::NotFound)?;
        if job.state != OpsJobState::Running { return Err(OpsJobError::InvalidTransition { from: format!("{:?}", job.state), to: "Paused".into() }); }
        job.state = OpsJobState::Paused; self.persist()?; Ok(self.jobs.get(job_id).expect("present"))
    }
    pub fn cancel(&mut self, job_id: &str) -> Result<&OpsJob, OpsJobError> {
        let job = self.jobs.get_mut(job_id).ok_or(OpsJobError::NotFound)?;
        if job.state.is_terminal() { return Err(OpsJobError::InvalidTransition { from: format!("{:?}", job.state), to: "Cancelled".into() }); }
        if job.publish_fenced { return Err(OpsJobError::TooLate { job_id: job_id.to_string() }); }
        job.state = OpsJobState::Cancelled; job.progress = "cancelled before publish".into(); self.persist()?; Ok(self.jobs.get(job_id).expect("present"))
    }
    pub fn get(&self, job_id: &str) -> Option<&OpsJob> { self.jobs.get(job_id) }
    pub fn list(&self) -> Vec<&OpsJob> { self.jobs.values().collect() }
}
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OpsJobError {
    #[error("ops job not found")] NotFound,
    #[error("invalid transition from {from} to {to}")] InvalidTransition { from: String, to: String },
    #[error("ops job {job_id} already crossed publish fence; cancel too late")] TooLate { job_id: String },
    #[error("ops job storage: {0}")] Storage(String),
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn backup_job_is_resumable_across_phases() {
        let mut store = OpsJobStore::new();
        let job = store.submit(OpsJobKind::Backup, BTreeMap::new()).unwrap();
        let id = job.job_id.clone();
        store.start(&id).unwrap(); store.advance_phase(&id, 2, "snap").unwrap();
        store.pause(&id).unwrap(); store.start(&id).unwrap();
        assert_eq!(store.get(&id).unwrap().phase, 2);
        store.succeed(&id).unwrap();
    }
    #[test] fn product_ops_kinds_exist() { assert_eq!(OpsJobKind::PRODUCT.len(), 9); }
    #[test] fn restart_resumes_running_as_pending() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = OpsJobStore::open(dir.path()).unwrap();
        let job = store.submit_with(OpsJobKind::SplitTablet, BTreeMap::new(), Some("k".into()), 42).unwrap();
        let id = job.job_id.clone(); store.start(&id).unwrap(); store.advance_phase(&id, 3, "x").unwrap(); drop(store);
        let reopened = OpsJobStore::open(dir.path()).unwrap();
        let job = reopened.get(&id).unwrap();
        assert_eq!(job.state, OpsJobState::Pending); assert_eq!(job.phase, 3); assert_eq!(job.metadata_version, 42);
    }
    #[test] fn idempotent_key_returns_same_job() {
        let mut store = OpsJobStore::new();
        let a = store.submit_with(OpsJobKind::TransferLeader, BTreeMap::new(), Some("k".into()), 1).unwrap();
        let b = store.submit_with(OpsJobKind::TransferLeader, BTreeMap::new(), Some("k".into()), 1).unwrap();
        assert_eq!(a.job_id, b.job_id);
    }
    #[test] fn cancel_before_publish_and_too_late_after() {
        let mut store = OpsJobStore::new();
        let job = store.submit(OpsJobKind::MergeTablets, BTreeMap::new()).unwrap();
        let id = job.job_id.clone(); store.start(&id).unwrap(); store.cancel(&id).unwrap();
        assert_eq!(store.get(&id).unwrap().state, OpsJobState::Cancelled);
        let job2 = store.submit(OpsJobKind::MergeTablets, BTreeMap::new()).unwrap();
        let id2 = job2.job_id.clone(); store.start(&id2).unwrap(); store.mark_publish_fence(&id2).unwrap();
        assert!(matches!(store.cancel(&id2), Err(OpsJobError::TooLate { .. })));
    }
    #[test] fn accepted_response_shape() {
        let mut store = OpsJobStore::new();
        let job = store.submit_with(OpsJobKind::Backup, BTreeMap::new(), None, 7).unwrap();
        let body = OpsJobStore::accepted_response(&job);
        assert_eq!(body["status"], "accepted"); assert_eq!(body["state"], "pending"); assert_eq!(body["metadata_version"], 7);
    }

    /// P1.6-X3: TransferLeader / SplitTablet accept returns job_id; cancel before publish.
    #[test]
    fn transfer_and_split_accept_return_job_id_and_cancel_before_publish() {
        let mut store = OpsJobStore::new();
        let transfer = store
            .submit_with(
                OpsJobKind::TransferLeader,
                BTreeMap::from([
                    ("tablet_id".into(), "t1".into()),
                    ("to".into(), "n2".into()),
                ]),
                None,
                3,
            )
            .unwrap();
        let body = OpsJobStore::accepted_response(&transfer);
        assert_eq!(body["status"], "accepted");
        assert_eq!(body["state"], "pending");
        assert!(body["job_id"].as_str().unwrap().starts_with("ops-transfer_leader-"));
        assert_eq!(body["kind"], "transfer_leader");

        let split = store
            .submit(OpsJobKind::SplitTablet, BTreeMap::from([
                ("tablet_id".into(), "t1".into()),
                ("at_key_hex".into(), "00ff".into()),
            ]))
            .unwrap();
        let split_body = OpsJobStore::accepted_response(&split);
        assert_eq!(split_body["status"], "accepted");
        assert!(split_body["job_id"].as_str().unwrap().starts_with("ops-split_tablet-"));

        let id = split.job_id.clone();
        store.start(&id).unwrap();
        store.cancel(&id).unwrap();
        assert_eq!(store.get(&id).unwrap().state, OpsJobState::Cancelled);
        assert_eq!(
            store.get(&id).unwrap().progress,
            "cancelled before publish"
        );
    }

    /// P1.6-X4: cancel after publish fence returns TooLate (product store used by admin SQL).
    #[test]
    fn p16_x4_cancel_after_publish_fence_is_too_late() {
        let mut store = OpsJobStore::new();
        let job = store
            .submit(OpsJobKind::SplitTablet, BTreeMap::from([
                ("tablet_id".into(), "t9".into()),
            ]))
            .unwrap();
        let id = job.job_id.clone();
        store.start(&id).unwrap();
        store.advance_phase(&id, 2, "copying ranges").unwrap();
        store.mark_publish_fence(&id).unwrap();
        assert!(store.get(&id).unwrap().publish_fenced);
        assert_eq!(store.get(&id).unwrap().progress, "publish fence");
        let err = store.cancel(&id).expect_err("must be too late after fence");
        assert!(matches!(err, OpsJobError::TooLate { .. }), "{err:?}");
        // Job remains running/fenced — not silently cancelled.
        assert_eq!(store.get(&id).unwrap().state, OpsJobState::Running);
        assert!(store.get(&id).unwrap().publish_fenced);
    }

    /// P1.6-X5: progress phases and terminal errors are visible via get/list.
    #[test]
    fn p16_x5_progress_and_errors_visible() {
        let mut store = OpsJobStore::new();
        let job = store
            .submit_with(OpsJobKind::Backup, BTreeMap::new(), None, 11)
            .unwrap();
        let id = job.job_id.clone();
        assert_eq!(store.get(&id).unwrap().progress, "submitted");
        store.start(&id).unwrap();
        assert_eq!(store.get(&id).unwrap().progress, "running");
        store.advance_phase(&id, 1, "snapshot 40%").unwrap();
        assert_eq!(store.get(&id).unwrap().phase, 1);
        assert_eq!(store.get(&id).unwrap().progress, "snapshot 40%");
        // list surfaces the same progress for operators / SHOW JOBS.
        let listed = store.list();
        assert!(listed.iter().any(|j| j.job_id == id && j.progress == "snapshot 40%"));
        store.fail(&id, "disk full").unwrap();
        let failed = store.get(&id).unwrap();
        assert_eq!(failed.state, OpsJobState::Failed);
        assert_eq!(failed.error.as_deref(), Some("disk full"));
        assert_eq!(failed.progress, "failed");
    }
}
