//! Online operations as persistent resumable jobs (spec section 14.5, Stage 5E).
//!
//! Every long operation — rolling upgrades, node replacement, tablet movement,
//! index builds, backfills, backup, restore verification, key rotation, stats,
//! leader rebalancing — is modeled as a durable job record with a resumable
//! phase machine.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Kind of long-running online operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum OpsJobKind {
    /// Rolling upgrade of node binaries.
    RollingUpgrade,
    /// Node replacement.
    NodeReplacement,
    /// Tablet replica movement.
    TabletMovement,
    /// Online index build.
    OnlineIndexBuild,
    /// Schema backfill.
    SchemaBackfill,
    /// Cluster/database backup.
    Backup,
    /// Restore verification.
    RestoreVerification,
    /// Encryption key rotation.
    KeyRotation,
    /// Statistics collection.
    StatsCollection,
    /// Leader rebalancing.
    LeaderRebalance,
}

impl OpsJobKind {
    /// All kinds.
    pub const ALL: [OpsJobKind; 10] = [
        Self::RollingUpgrade,
        Self::NodeReplacement,
        Self::TabletMovement,
        Self::OnlineIndexBuild,
        Self::SchemaBackfill,
        Self::Backup,
        Self::RestoreVerification,
        Self::KeyRotation,
        Self::StatsCollection,
        Self::LeaderRebalance,
    ];

    /// Stable name.
    pub fn name(self) -> &'static str {
        match self {
            Self::RollingUpgrade => "rolling_upgrade",
            Self::NodeReplacement => "node_replacement",
            Self::TabletMovement => "tablet_movement",
            Self::OnlineIndexBuild => "online_index_build",
            Self::SchemaBackfill => "schema_backfill",
            Self::Backup => "backup",
            Self::RestoreVerification => "restore_verification",
            Self::KeyRotation => "key_rotation",
            Self::StatsCollection => "stats_collection",
            Self::LeaderRebalance => "leader_rebalance",
        }
    }
}

/// Job lifecycle state (mirrors S1F job graph subset).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpsJobState {
    /// Accepted, not yet running.
    Pending,
    /// Actively executing.
    Running,
    /// Cooperatively paused.
    Paused,
    /// Cancellation requested.
    Cancelling,
    /// Finished successfully.
    Succeeded,
    /// Finished with failure.
    Failed,
}

/// One persistent ops job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpsJob {
    /// Job id.
    pub job_id: String,
    /// Kind.
    pub kind: OpsJobKind,
    /// State.
    pub state: OpsJobState,
    /// Phase index within the kind's protocol (0-based).
    pub phase: u32,
    /// Human progress message.
    pub progress: String,
    /// Opaque parameters (JSON object as string map).
    pub params: BTreeMap<String, String>,
}

/// In-memory job store (durable binding uses JobRegistry / meta jobs).
#[derive(Debug, Default, Clone)]
pub struct OpsJobStore {
    jobs: BTreeMap<String, OpsJob>,
    next: u64,
}

impl OpsJobStore {
    /// Empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Submit a new job in Pending.
    pub fn submit(&mut self, kind: OpsJobKind, params: BTreeMap<String, String>) -> OpsJob {
        self.next += 1;
        let job_id = format!("ops-{}-{}", kind.name(), self.next);
        let job = OpsJob {
            job_id: job_id.clone(),
            kind,
            state: OpsJobState::Pending,
            phase: 0,
            progress: "submitted".into(),
            params,
        };
        self.jobs.insert(job_id, job.clone());
        job
    }

    /// Transition Pending → Running (or resume Paused → Running).
    pub fn start(&mut self, job_id: &str) -> Result<&OpsJob, OpsJobError> {
        let job = self.jobs.get_mut(job_id).ok_or(OpsJobError::NotFound)?;
        match job.state {
            OpsJobState::Pending | OpsJobState::Paused => {
                job.state = OpsJobState::Running;
                job.progress = "running".into();
                Ok(job)
            }
            other => Err(OpsJobError::InvalidTransition {
                from: format!("{other:?}"),
                to: "Running".into(),
            }),
        }
    }

    /// Advance phase while Running (crash-resume friendly).
    pub fn advance_phase(
        &mut self,
        job_id: &str,
        phase: u32,
        progress: impl Into<String>,
    ) -> Result<&OpsJob, OpsJobError> {
        let job = self.jobs.get_mut(job_id).ok_or(OpsJobError::NotFound)?;
        if job.state != OpsJobState::Running {
            return Err(OpsJobError::InvalidTransition {
                from: format!("{:?}", job.state),
                to: "advance_phase".into(),
            });
        }
        job.phase = phase;
        job.progress = progress.into();
        Ok(job)
    }

    /// Mark succeeded.
    pub fn succeed(&mut self, job_id: &str) -> Result<&OpsJob, OpsJobError> {
        let job = self.jobs.get_mut(job_id).ok_or(OpsJobError::NotFound)?;
        if job.state != OpsJobState::Running {
            return Err(OpsJobError::InvalidTransition {
                from: format!("{:?}", job.state),
                to: "Succeeded".into(),
            });
        }
        job.state = OpsJobState::Succeeded;
        job.progress = "succeeded".into();
        Ok(job)
    }

    /// Pause a running job.
    pub fn pause(&mut self, job_id: &str) -> Result<&OpsJob, OpsJobError> {
        let job = self.jobs.get_mut(job_id).ok_or(OpsJobError::NotFound)?;
        if job.state != OpsJobState::Running {
            return Err(OpsJobError::InvalidTransition {
                from: format!("{:?}", job.state),
                to: "Paused".into(),
            });
        }
        job.state = OpsJobState::Paused;
        Ok(job)
    }

    /// Lookup.
    pub fn get(&self, job_id: &str) -> Option<&OpsJob> {
        self.jobs.get(job_id)
    }

    /// List all jobs.
    pub fn list(&self) -> Vec<&OpsJob> {
        self.jobs.values().collect()
    }
}

/// Ops job errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OpsJobError {
    /// Unknown job.
    #[error("ops job not found")]
    NotFound,
    /// Illegal state transition.
    #[error("invalid transition from {from} to {to}")]
    InvalidTransition {
        /// From state.
        from: String,
        /// To state/action.
        to: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backup_job_is_resumable_across_phases() {
        let mut store = OpsJobStore::new();
        let job = store.submit(OpsJobKind::Backup, BTreeMap::new());
        let id = job.job_id.clone();
        store.start(&id).unwrap();
        store.advance_phase(&id, 1, "pin meta").unwrap();
        store.advance_phase(&id, 2, "tablet snapshots").unwrap();
        // Crash simulation: pause then resume.
        store.pause(&id).unwrap();
        store.start(&id).unwrap();
        assert_eq!(store.get(&id).unwrap().phase, 2);
        store.advance_phase(&id, 6, "publish manifest").unwrap();
        store.succeed(&id).unwrap();
        assert_eq!(store.get(&id).unwrap().state, OpsJobState::Succeeded);
    }

    #[test]
    fn all_spec_ops_kinds_exist() {
        assert_eq!(OpsJobKind::ALL.len(), 10);
        assert!(OpsJobKind::ALL.iter().any(|k| k.name() == "key_rotation"));
    }
}
