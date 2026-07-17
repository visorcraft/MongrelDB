//! Adapter error taxonomy for the consensus group boundary.
//!
//! [`ConsensusError`] is the error surface of [`crate::group::ConsensusGroup`].
//! `RaftCommitLog` (ADR-0002) maps it onto `mongreldb_log`'s `LogError` at the
//! `CommitLog` boundary; richer leader routing lands with the Stage 2C write
//! protocol.

use mongreldb_log::envelope::EnvelopeError;

use crate::identity::RaftNodeId;
use crate::network::TransportError;
use crate::state_machine::StateMachineError;
use crate::storage::StoreError;

/// Errors produced by consensus group operations.
#[derive(Debug, thiserror::Error)]
pub enum ConsensusError {
    /// The local node is not the leader; `leader` carries the current leader
    /// hint when the group knows one.
    #[error("not the leader (current leader: {leader:?})")]
    NotLeader {
        /// The node's current belief about the leader, if any.
        leader: Option<RaftNodeId>,
    },
    /// The group is shut down and rejects new work.
    #[error("consensus group is closed")]
    Closed,
    /// The operation was cancelled.
    #[error("operation cancelled")]
    Cancelled,
    /// The operation's deadline expired.
    #[error("deadline exceeded")]
    DeadlineExceeded,
    /// The leader's HLC clock could not stamp a commit timestamp.
    #[error("commit timestamp clock failure: {0}")]
    Clock(String),
    /// The command envelope was malformed or unverifiable.
    #[error(transparent)]
    Envelope(#[from] EnvelopeError),
    /// Durable log/hard-state storage failure.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Apply state machine failure.
    #[error(transparent)]
    StateMachine(#[from] StateMachineError),
    /// Transport-level failure.
    #[error(transparent)]
    Transport(#[from] TransportError),
    /// Any other openraft failure (election, replication, fatal).
    #[error("raft failure: {0}")]
    Raft(String),
    /// The operation is not implemented by this adapter wave.
    #[error("unsupported operation: {0}")]
    Unsupported(&'static str),
    /// The request was malformed for the group's current state.
    #[error("invalid request: {0}")]
    InvalidRequest(String),
}
