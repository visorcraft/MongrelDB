//! Consensus identity and the replicated command payload (spec sections 7 and
//! 11.2, S2B-003).
//!
//! openraft identifies nodes by a small ordered `NodeId`; this crate uses
//! `u64` ([`RaftNodeId`]). MongrelDB's durable [`NodeId`] is a random 128-bit
//! identifier, so the adapter projects it deterministically onto a raft id:
//! the **first eight bytes, little-endian**. The projection is one-way: the
//! full 128-bit `NodeId` lives in the cluster membership directory owned by
//! `mongreldb-cluster` (Stage 2A), never inside raft state. Collisions of the
//! 64-bit projection are rejected at cluster bootstrap by that layer; the
//! consensus adapter treats raft ids as opaque.
//!
//! [`ReplicatedCommand`] is the `AppData` of the raft log (S2B-003). The
//! leader assigns the term and log index (the raft `LogId` of the appended
//! entry), the commit timestamp (stamped into the payload before proposal so
//! every replica applies the identical value deterministically), and the
//! command ID (the [`CommandEnvelope`]'s `command_id`, assigned when the
//! command is constructed, used for idempotent apply per S2B-004).

use mongreldb_log::commit_log::LogPosition;
use mongreldb_log::envelope::CommandEnvelope;
use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::NodeId;

// Referenced by the `declare_raft_types!` expansion (default `SnapshotData`).
use std::io::Cursor;

/// Node identifier type used inside the raft group.
///
/// See the module documentation for the deterministic mapping from
/// [`mongreldb_types::ids::NodeId`].
pub type RaftNodeId = u64;

/// Deterministic projection of a durable 128-bit [`NodeId`] onto the raft
/// node id space: the first eight bytes interpreted little-endian. Pure and
/// total; the inverse mapping is owned by the cluster membership directory,
/// not by this crate.
pub fn raft_node_id(id: &NodeId) -> RaftNodeId {
    u64::from_le_bytes(id.as_bytes()[..8].try_into().expect("16-byte id"))
}

/// Which subsystem a [`ReplicatedCommand`] targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CommandKind {
    /// Transactional (row data) command.
    Transaction,
    /// Catalog mutation command.
    Catalog,
    /// Node/group maintenance command.
    Maintenance,
}

/// Transaction payload: a versioned command envelope plus the leader-assigned
/// commit timestamp (S2B-003).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TransactionCommand {
    /// The versioned, checksummed command; `command_id` is the idempotent-apply
    /// identifier assigned by the leader when the command was constructed.
    pub envelope: CommandEnvelope,
    /// Leader-assigned commit timestamp, identical on every replica.
    pub commit_ts: HlcTimestamp,
}

/// Catalog payload; see [`TransactionCommand`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CatalogCommand {
    /// The versioned, checksummed command.
    pub envelope: CommandEnvelope,
    /// Leader-assigned commit timestamp, identical on every replica.
    pub commit_ts: HlcTimestamp,
}

/// Maintenance payload; see [`TransactionCommand`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MaintenanceCommand {
    /// The versioned, checksummed command.
    pub envelope: CommandEnvelope,
    /// Leader-assigned commit timestamp, identical on every replica.
    pub commit_ts: HlcTimestamp,
}

/// The raft log's application payload (S2B-003).
///
/// `Noop` carries no command; it is used to establish leadership and advance
/// the commit index without touching applied state.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReplicatedCommand {
    /// Transaction command.
    Transaction(TransactionCommand),
    /// Catalog command.
    Catalog(CatalogCommand),
    /// Maintenance command.
    Maintenance(MaintenanceCommand),
    /// No-operation barrier command.
    Noop,
}

impl ReplicatedCommand {
    /// Builds a command of `kind` carrying `envelope`, stamped by the leader
    /// with `commit_ts`.
    pub fn new(kind: CommandKind, envelope: CommandEnvelope, commit_ts: HlcTimestamp) -> Self {
        match kind {
            CommandKind::Transaction => Self::Transaction(TransactionCommand { envelope, commit_ts }),
            CommandKind::Catalog => Self::Catalog(CatalogCommand { envelope, commit_ts }),
            CommandKind::Maintenance => Self::Maintenance(MaintenanceCommand { envelope, commit_ts }),
        }
    }

    /// The leader-assigned command id, or `None` for `Noop`.
    pub fn command_id(&self) -> Option<[u8; 16]> {
        self.envelope().map(|envelope| envelope.command_id)
    }

    /// The leader-assigned commit timestamp, or `None` for `Noop`.
    pub fn commit_ts(&self) -> Option<HlcTimestamp> {
        match self {
            Self::Transaction(command) => Some(command.commit_ts),
            Self::Catalog(command) => Some(command.commit_ts),
            Self::Maintenance(command) => Some(command.commit_ts),
            Self::Noop => None,
        }
    }

    /// The carried command envelope, or `None` for `Noop`.
    pub fn envelope(&self) -> Option<&CommandEnvelope> {
        match self {
            Self::Transaction(command) => Some(&command.envelope),
            Self::Catalog(command) => Some(&command.envelope),
            Self::Maintenance(command) => Some(&command.envelope),
            Self::Noop => None,
        }
    }

    /// The subsystem this command targets, or `None` for `Noop`.
    pub fn kind(&self) -> Option<CommandKind> {
        match self {
            Self::Transaction(_) => Some(CommandKind::Transaction),
            Self::Catalog(_) => Some(CommandKind::Catalog),
            Self::Maintenance(_) => Some(CommandKind::Maintenance),
            Self::Noop => None,
        }
    }
}

/// Per-entry response returned by the apply state machine (openraft `R`).
///
/// Applying a command whose id was already recorded yields `duplicate: true`
/// and does not dispatch to the [`crate::state_machine::ApplySink`] again
/// (S2B-004 idempotent apply).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ApplyResponse {
    /// Position of the applied entry in the raft log.
    pub position: LogPosition,
    /// Leader-assigned command id, if the entry carried a command.
    pub command_id: Option<[u8; 16]>,
    /// Leader-assigned commit timestamp, if the entry carried a command.
    pub commit_ts: Option<HlcTimestamp>,
    /// `true` when the command id was already applied and the entry was
    /// skipped without re-dispatching.
    pub duplicate: bool,
}

openraft::declare_raft_types!(
    /// MongrelDB's openraft type configuration (ADR-0004).
    ///
    /// `NodeId` is the projected [`RaftNodeId`]; `Node` is openraft's
    /// `BasicNode` (network address); entries are openraft's default
    /// `Entry<MongrelRaftConfig>`; snapshots stream as in-memory cursors; the
    /// runtime is tokio.
    pub MongrelRaftConfig:
        D = ReplicatedCommand,
        R = ApplyResponse,
        NodeId = RaftNodeId,
        Node = openraft::BasicNode,
);

/// The running raft node handle.
pub type MongrelRaft = openraft::Raft<MongrelRaftConfig>;
/// A raft log entry of the MongrelDB configuration.
pub type RaftLogEntry = openraft::Entry<MongrelRaftConfig>;
/// A raft log id (term + index) of the MongrelDB configuration.
pub type RaftLogId = openraft::LogId<RaftNodeId>;
/// A raft vote of the MongrelDB configuration.
pub type RaftVote = openraft::Vote<RaftNodeId>;
/// The effective membership (joint or uniform) of the group.
pub type RaftMembership = openraft::Membership<RaftNodeId, openraft::BasicNode>;
/// The membership as persisted by the state machine.
pub type RaftStoredMembership = openraft::StoredMembership<RaftNodeId, openraft::BasicNode>;
/// openraft snapshot metadata of the MongrelDB configuration.
pub type RaftSnapshotMeta = openraft::SnapshotMeta<RaftNodeId, openraft::BasicNode>;
/// openraft storage error of the MongrelDB configuration.
pub type RaftStorageError = openraft::StorageError<RaftNodeId>;

/// Bridges an openraft log id to the commit-log [`LogPosition`]
/// (`term`/`index`; S2B-003's leader-assigned term and log index).
pub fn log_position_of(log_id: &RaftLogId) -> LogPosition {
    LogPosition {
        term: log_id.leader_id.term,
        index: log_id.index,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_node_id_projection_is_deterministic() {
        let id = NodeId::from_bytes([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
        ]);
        assert_eq!(raft_node_id(&id), u64::from_le_bytes([1, 2, 3, 4, 5, 6, 7, 8]));
        assert_eq!(raft_node_id(&id), raft_node_id(&id));
        assert_ne!(raft_node_id(&NodeId::ZERO), raft_node_id(&id));
    }

    #[test]
    fn replicated_command_accessors() {
        let envelope = CommandEnvelope::new(1, [7u8; 16], b"payload".to_vec());
        let commit_ts = HlcTimestamp {
            physical_micros: 42,
            logical: 1,
            node_tiebreaker: 2,
        };
        let command = ReplicatedCommand::new(CommandKind::Transaction, envelope, commit_ts);
        assert_eq!(command.command_id(), Some([7u8; 16]));
        assert_eq!(command.commit_ts(), Some(commit_ts));
        assert_eq!(command.kind(), Some(CommandKind::Transaction));
        assert!(command.envelope().is_some());

        let noop = ReplicatedCommand::Noop;
        assert_eq!(noop.command_id(), None);
        assert_eq!(noop.commit_ts(), None);
        assert_eq!(noop.kind(), None);
        assert!(noop.envelope().is_none());
    }

    #[test]
    fn replicated_command_serde_round_trip() {
        let envelope = CommandEnvelope::new(3, [9u8; 16], vec![1, 2, 3]);
        let command = ReplicatedCommand::new(
            CommandKind::Catalog,
            envelope,
            HlcTimestamp {
                physical_micros: 7,
                logical: 0,
                node_tiebreaker: 0,
            },
        );
        let bytes = bincode::serialize(&command).unwrap();
        assert_eq!(bincode::deserialize::<ReplicatedCommand>(&bytes).unwrap(), command);
        let json = serde_json::to_string(&command).unwrap();
        assert_eq!(
            serde_json::from_str::<ReplicatedCommand>(&json).unwrap(),
            command
        );
    }
}
