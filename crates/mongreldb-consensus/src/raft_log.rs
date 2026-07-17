//! `RaftCommitLog`: the replicated-mode [`CommitLog`] (spec sections 4.4,
//! 6.2, 11.2–11.3; ADR-0002, ADR-0004).
//!
//! In replicated mode the committed consensus log is authoritative — there is
//! no second log that could independently declare a transaction committed
//! (spec section 4.4). `propose` carries a `CommandEnvelope` through the
//! group: the leader assigns term/index (the raft log id), the commit
//! timestamp (stamped before replication), and the command id (the
//! envelope's), waits for quorum commit **and** local apply, and returns an
//! irrevocable [`CommitReceipt`].
//!
//! # Durability levels (spec section 11.3)
//!
//! [`DurabilityLevel::Quorum`] is the default and the only level this wave
//! acknowledges: a quorum-acknowledged write has RPO 0 below quorum loss.
//! [`DurabilityLevel::LeaderDisk`] is an *optional* lower guarantee owned by
//! the Stage 2C write protocol (openraft's public API has no
//! "appended-to-leader-log" mid-point callback, so an honest implementation
//! lands with S2C); selecting it here reports
//! [`LogError::Unsupported`]. Memory-only acknowledged writes are never
//! implemented (spec section 11.3).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use mongreldb_log::commit_log::{
    CommitLog, CommitReceipt, CommittedEntry, DurabilityLevel, ExecutionControl, LogError,
    LogPosition, LogSnapshot,
};
use mongreldb_log::envelope::CommandEnvelope;
use mongreldb_types::ids::TransactionId;

use crate::error::ConsensusError;
use crate::group::{ConsensusGroup, ConsensusSnapshot};
use crate::identity::CommandKind;
use crate::network::RaftTransport;

/// The replicated [`CommitLog`] over one [`ConsensusGroup`].
pub struct RaftCommitLog<T: RaftTransport> {
    group: Arc<ConsensusGroup<T>>,
    durability: DurabilityLevel,
    closed: AtomicBool,
}

impl<T: RaftTransport> RaftCommitLog<T> {
    /// Creates the commit log over `group` with quorum durability (the
    /// replicated default).
    pub fn new(group: Arc<ConsensusGroup<T>>) -> Self {
        Self::with_durability(group, DurabilityLevel::Quorum)
    }

    /// Creates the commit log with an explicit durability preference. Note
    /// [`DurabilityLevel::LeaderDisk`] is not acknowledged by this wave (see
    /// module docs).
    pub fn with_durability(group: Arc<ConsensusGroup<T>>, durability: DurabilityLevel) -> Self {
        RaftCommitLog {
            group,
            durability,
            closed: AtomicBool::new(false),
        }
    }

    /// The underlying group (membership, snapshots, read barrier, metrics).
    pub fn group(&self) -> &Arc<ConsensusGroup<T>> {
        &self.group
    }

    /// Shuts the log and the group down gracefully.
    pub async fn shutdown(&self) -> Result<(), LogError> {
        self.closed.store(true, Ordering::Release);
        self.group.shutdown().await.map_err(map_error)
    }
}

fn map_error(err: ConsensusError) -> LogError {
    match err {
        ConsensusError::Cancelled => LogError::Cancelled,
        ConsensusError::DeadlineExceeded => LogError::DeadlineExceeded,
        ConsensusError::Closed => LogError::Closed,
        ConsensusError::Envelope(e) => LogError::Envelope(e),
        ConsensusError::Unsupported(op) => LogError::Unsupported(op),
        // The leader hint rides in the message until the Stage 2C write
        // protocol extends the taxonomy with a routed variant.
        other => LogError::Internal(other.to_string()),
    }
}

impl<T: RaftTransport> CommitLog for RaftCommitLog<T> {
    /// Proposes the command as a transaction command and waits for quorum
    /// commit + local apply. The receipt is irrevocable (spec section 4.7).
    fn propose(
        &self,
        command: CommandEnvelope,
        control: &ExecutionControl,
    ) -> Result<CommitReceipt, LogError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(LogError::Closed);
        }
        if self.durability == DurabilityLevel::LeaderDisk {
            return Err(LogError::Unsupported(
                "LeaderDisk durability lands with the Stage 2C write protocol (spec 11.3)",
            ));
        }
        let command_id = command.command_id;
        // The CommitLog trait is synchronous; the group API is async. Drive
        // the future on the caller's runtime context.
        let receipt = block_on_group(self.group.clone(), |group| async move {
            group.propose(CommandKind::Transaction, command, control).await
        })
        .map_err(map_error)?;
        Ok(CommitReceipt {
            transaction_id: TransactionId::from_bytes(command_id),
            commit_ts: receipt.commit_ts,
            log_position: receipt.position,
            durability: DurabilityLevel::Quorum,
        })
    }

    fn read_committed(
        &self,
        after: LogPosition,
        limit: usize,
    ) -> Result<Vec<CommittedEntry>, LogError> {
        self.group.read_committed(after, limit).map_err(map_error)
    }

    fn applied_position(&self) -> LogPosition {
        self.group.applied_position()
    }

    fn create_snapshot(&self) -> Result<LogSnapshot, LogError> {
        let snapshot: ConsensusSnapshot = block_on_group(self.group.clone(), |group| async move {
            group.snapshot().await
        })
        .map_err(map_error)?;
        Ok(LogSnapshot {
            position: snapshot.position,
            commit_ts: snapshot.commit_ts,
            data: snapshot.data,
        })
    }

    fn install_snapshot(&self, snapshot: LogSnapshot) -> Result<(), LogError> {
        self.group
            .install_snapshot(&ConsensusSnapshot {
                position: snapshot.position,
                commit_ts: snapshot.commit_ts,
                data: snapshot.data,
            })
            .map_err(map_error)
    }
}

/// Drives a group future to completion from the synchronous [`CommitLog`]
/// interface. Inside a tokio runtime this uses `block_in_place` so the
/// current-thread scheduler keeps running the raft tasks; outside a runtime
/// it builds a private current-thread runtime.
fn block_on_group<T, F, Fut, R>(group: Arc<ConsensusGroup<T>>, f: F) -> Result<R, ConsensusError>
where
    T: RaftTransport,
    F: FnOnce(Arc<ConsensusGroup<T>>) -> Fut,
    Fut: std::future::Future<Output = Result<R, ConsensusError>>,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(f(group))),
        Err(_) => {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| ConsensusError::Raft(format!("building runtime: {e}")))?;
            runtime.block_on(f(group))
        }
    }
}

#[cfg(test)]
mod tests {
    // Group-level coverage of RaftCommitLog lives in tests/cluster.rs (it
    // needs a running raft group); see single_node_commit_log_round_trip.
}
