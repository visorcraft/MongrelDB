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
//! [`DurabilityLevel::Quorum`] is the default: a quorum-acknowledged write
//! has RPO 0 below quorum loss. [`DurabilityLevel::LeaderDisk`] (S2C) is the
//! optional lower guarantee: the receipt is issued once the entry is fsynced
//! on the leader's local log, **before** quorum commit. Honesty rules:
//!
//! - the receipt is issued strictly after the segment fsync covering the
//!   entry (a crash before that fsync never acknowledges);
//! - the receipt is NOT a commit declaration — visibility still gates on
//!   quorum commit + apply (spec section 4.4), and a LeaderDisk-acknowledged
//!   entry can be truncated on leader loss (RPO > 0, the documented
//!   trade-off). The entry still commits normally in the background;
//! - leadership is enforced by the raft propose path; waiters are keyed
//!   only to commands proposed in the current term.
//!
//! Memory-only acknowledged writes are never implemented (spec section
//! 11.3). [`DurabilityLevel::GroupCommit`] is the standalone-mode level and
//! is rejected here.

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
use crate::read::SessionToken;

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

    /// Creates the commit log with an explicit durability preference. The
    /// replicated default is [`DurabilityLevel::Quorum`];
    /// [`DurabilityLevel::LeaderDisk`] acknowledges at leader-local fsync
    /// (see module docs for the honesty contract).
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

    /// Builds the Stage 2D session token for a receipt this log issued
    /// (read-your-writes consistency, spec section 11.4): the group id, the
    /// committed log index, and the receipt's commit timestamp.
    pub fn session_token(&self, receipt: &CommitReceipt) -> SessionToken {
        SessionToken {
            group_id: self.group.group_id().to_owned(),
            commit_index: receipt.log_position.index,
            commit_ts: receipt.commit_ts,
        }
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
        // Routed NotLeader (S2C): the hint rides along for the gateway's
        // retry (spec section 11.7).
        ConsensusError::NotLeader { leader } => LogError::NotLeader {
            leader_hint: leader.map(|id| id.to_string()),
        },
        other => LogError::Internal(other.to_string()),
    }
}

impl<T: RaftTransport> CommitLog for RaftCommitLog<T> {
    /// Proposes the command as a transaction command. With
    /// [`DurabilityLevel::Quorum`] (default) this waits for quorum commit +
    /// local apply and the receipt is irrevocable (spec section 4.7). With
    /// [`DurabilityLevel::LeaderDisk`] the receipt is issued once the entry
    /// is fsynced on the leader's local log — before quorum commit (see the
    /// module docs for the honesty contract; it is NOT a commit
    /// declaration).
    fn propose(
        &self,
        command: CommandEnvelope,
        control: &ExecutionControl,
    ) -> Result<CommitReceipt, LogError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(LogError::Closed);
        }
        let command_id = command.command_id;
        match self.durability {
            DurabilityLevel::Quorum => {
                // The CommitLog trait is synchronous; the group API is async.
                // Drive the future on the caller's runtime context.
                let receipt = block_on_group(self.group.clone(), |group| async move {
                    group
                        .propose(CommandKind::Transaction, command, control)
                        .await
                })
                .map_err(map_error)?;
                Ok(CommitReceipt {
                    transaction_id: TransactionId::from_bytes(command_id),
                    commit_ts: receipt.commit_ts,
                    log_position: receipt.position,
                    durability: DurabilityLevel::Quorum,
                })
            }
            DurabilityLevel::LeaderDisk => {
                let receipt = block_on_group(self.group.clone(), |group| async move {
                    group
                        .propose_leader_durable(CommandKind::Transaction, command, control)
                        .await
                })
                .map_err(map_error)?;
                Ok(CommitReceipt {
                    transaction_id: TransactionId::from_bytes(command_id),
                    commit_ts: receipt.commit_ts,
                    log_position: receipt.position,
                    // Quorum commit landing before the fsync signal upgrades
                    // the receipt (never weaker than asked).
                    durability: if receipt.quorum_committed {
                        DurabilityLevel::Quorum
                    } else {
                        DurabilityLevel::LeaderDisk
                    },
                })
            }
            DurabilityLevel::GroupCommit => Err(LogError::Unsupported(
                "GroupCommit is the standalone-mode durability level (spec 11.3)",
            )),
        }
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
        let snapshot: ConsensusSnapshot =
            block_on_group(
                self.group.clone(),
                |group| async move { group.snapshot().await },
            )
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
