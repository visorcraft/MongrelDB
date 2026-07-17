//! Commit-log abstraction (spec sections 6.2 and 9.4, FND-004).
//!
//! [`CommitLog`] is the single authority through which commands become
//! committed. Standalone mode has one implementation wrapping the shared WAL
//! group commit (in `mongreldb-core`); replicated mode implements the same
//! contract over Raft (Stage 2). The storage apply path receives only
//! committed commands.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::TransactionId;

use crate::envelope::{CommandEnvelope, EnvelopeError};

/// A position in one commit log. `term` is zero for the standalone log.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct LogPosition {
    /// Consensus term; zero in standalone mode.
    pub term: u64,
    /// Monotonic log index (the standalone epoch).
    pub index: u64,
}

impl LogPosition {
    /// The position before any entry.
    pub const ZERO: Self = Self { term: 0, index: 0 };
}

/// One committed log entry returned by [`CommitLog::read_committed`].
#[derive(Debug, Clone)]
pub struct CommittedEntry {
    /// Position of this entry in the log.
    pub position: LogPosition,
    /// Commit timestamp assigned by the log authority.
    pub commit_ts: HlcTimestamp,
    /// The committed command.
    pub envelope: CommandEnvelope,
}

/// A point-in-time image of applied state at a log boundary.
#[derive(Debug, Clone)]
pub struct LogSnapshot {
    /// Last log position included in the snapshot.
    pub position: LogPosition,
    /// Commit timestamp of `position`.
    pub commit_ts: HlcTimestamp,
    /// Opaque snapshot payload defined by the implementation.
    pub data: Vec<u8>,
}

/// Durability guarantee attached to a committed command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum DurabilityLevel {
    /// Standalone shared-WAL group commit (fsync before acknowledge).
    GroupCommit,
    /// Replicated: leader-local disk only (optional lower guarantee).
    LeaderDisk,
    /// Replicated: persisted by a quorum (the replicated default, RPO 0).
    Quorum,
}

/// Proof that a command crossed its durable commit fence (spec S1B-004).
///
/// Once a receipt exists, the caller is never told the write rolled back.
#[derive(Debug, Clone)]
pub struct CommitReceipt {
    /// Transaction this command committed.
    pub transaction_id: TransactionId,
    /// Commit timestamp assigned by the log authority.
    pub commit_ts: HlcTimestamp,
    /// Position of the committed entry.
    pub log_position: LogPosition,
    /// Durability level that was satisfied.
    pub durability: DurabilityLevel,
}

/// Deadline and cancellation for log operations.
///
/// This is a deliberately minimal mirror of `mongreldb_core`'s
/// `execution::ExecutionControl`; the core type cannot move below the runtime
/// crate in the dependency graph, so the core adapter converts (see
/// `docs/architecture/adr/0002`).
#[derive(Debug, Clone, Default)]
pub struct ExecutionControl {
    /// Optional absolute deadline; queue wait counts toward it.
    pub deadline: Option<Instant>,
    /// Cooperative cancellation flag.
    pub cancellation: Option<Arc<AtomicBool>>,
}

impl ExecutionControl {
    /// Returns an error if the operation is cancelled or past its deadline.
    pub fn check(&self) -> Result<(), LogError> {
        if let Some(flag) = &self.cancellation {
            if flag.load(Ordering::Relaxed) {
                return Err(LogError::Cancelled);
            }
        }
        if let Some(deadline) = self.deadline {
            if Instant::now() >= deadline {
                return Err(LogError::DeadlineExceeded);
            }
        }
        Ok(())
    }
}

/// Errors produced by a [`CommitLog`].
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    /// The command envelope was malformed or unverifiable.
    #[error(transparent)]
    Envelope(#[from] EnvelopeError),
    /// The operation was cancelled.
    #[error("operation cancelled")]
    Cancelled,
    /// The operation's deadline expired.
    #[error("deadline exceeded")]
    DeadlineExceeded,
    /// The log is closed and rejects new proposals.
    #[error("commit log is closed")]
    Closed,
    /// The receiving replica is not the leader for the consensus group
    /// (spec section 11.7). Retryable with the returned leader hint.
    ///
    /// Category mapping: this variant carries
    /// `mongreldb_types::errors::ErrorCategory::NotLeader` semantics.
    /// No `LogError` → `ErrorCategory` bridge exists yet (the engine
    /// commit path does not convert `LogError` today); the bridge lands
    /// with the Stage 2G gateway/routing wave, which consumes the hint.
    #[error("not the leader (current leader: {leader_hint:?})")]
    NotLeader {
        /// The group's current leader hint when known (text form of the
        /// leader identity, e.g. its raft node id); `None` when the group
        /// has no known leader.
        leader_hint: Option<String>,
    },
    /// The implementation does not provide this operation.
    #[error("unsupported operation: {0}")]
    Unsupported(&'static str),
    /// Any other log failure.
    #[error("commit log failure: {0}")]
    Internal(String),
}

/// The single authority through which commands become committed.
pub trait CommitLog: Send + Sync {
    /// Proposes one command and waits until its durability policy is
    /// satisfied. A returned [`CommitReceipt`] is irrevocable.
    fn propose(
        &self,
        command: CommandEnvelope,
        control: &ExecutionControl,
    ) -> Result<CommitReceipt, LogError>;

    /// Reads committed entries strictly after `after`, in log order.
    fn read_committed(
        &self,
        after: LogPosition,
        limit: usize,
    ) -> Result<Vec<CommittedEntry>, LogError>;

    /// The highest position the local state machine has applied.
    fn applied_position(&self) -> LogPosition;

    /// Captures applied state through the current applied position.
    fn create_snapshot(&self) -> Result<LogSnapshot, LogError>;

    /// Replaces applied state with a snapshot taken at a log boundary.
    fn install_snapshot(&self, snapshot: LogSnapshot) -> Result<(), LogError>;
}
