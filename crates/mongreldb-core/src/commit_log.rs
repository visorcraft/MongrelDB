//! Standalone [`mongreldb_log::CommitLog`] adapter (spec section 9.4, FND-004).
//!
//! Implemented in the Stage 0 foundation wave: wraps the shared WAL and group
//! commit so the transaction commit path proposes versioned command envelopes
//! through the `CommitLog` interface, and the apply path observes only
//! committed commands.
//!
//! ## Stage 0 wiring choice
//!
//! The commit sequencer (`Database::commit_transaction_with_external_states_inner`
//! and the DDL commit paths) keeps the existing v4 WAL record format: a full
//! envelope dual-write would change on-disk bytes and break the "current
//! database format opens unchanged" Stage 0 gate. Instead this adapter **owns**
//! the append + group-commit durability steps those paths already performed —
//! [`Self::append_transaction`] writes the transaction command's records and
//! its commit marker, [`Self::seal_transaction`] drives group commit and issues
//! the [`CommitReceipt`] — and `Database` gates `publish_in_order` (reader
//! visibility) on that receipt, so spec section 9.4's critical rule ("the
//! storage apply path receives only committed commands") is structurally true.
//!
//! Generic commands proposed through [`CommitLog::propose`] are persisted as
//! [`DdlOp::Command`] records carrying one encoded
//! [`mongreldb_log::CommandEnvelope`]; [`CommitLog::read_committed`] replays
//! them with the existing WAL reader.

use std::path::PathBuf;
use std::sync::Arc;

use mongreldb_log::{
    CommandEnvelope, CommitLog, CommitReceipt, CommittedEntry, DurabilityLevel, LogError,
    LogPosition, LogSnapshot,
};
use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::TransactionId;
use parking_lot::Mutex;
use zeroize::Zeroizing;

use crate::epoch::{Epoch, EpochAuthority};
use crate::txn::GroupCommit;
use crate::wal::{AddedRun, DdlOp, Op, SharedWal};
use crate::MongrelError;

/// Reserved [`CommandEnvelope::command_type`] for the versioned transaction
/// command produced by the commit sequencer (spec section 9.3, FND-003).
pub const COMMAND_TYPE_TRANSACTION: u32 = 1;

/// Converts core's rich [`crate::ExecutionControl`] into the log crate's
/// minimal mirror (see `docs/architecture/adr/0002`).
///
/// The deadline maps directly. Core's cancellation hierarchy (parent/child
/// reasons, first-event-wins ordering) is not representable as one shared
/// atomic flag, so `cancellation` is left `None`: callers enforce cancellation
/// through `ExecutionControl::checkpoint` before proposing, exactly as the
/// commit sequencer already does.
pub fn to_log_control(control: &crate::ExecutionControl) -> mongreldb_log::ExecutionControl {
    mongreldb_log::ExecutionControl {
        deadline: control.deadline(),
        cancellation: None,
    }
}

/// Maps an injected fault at a durability boundary to the closest existing
/// engine error (spec section 9.6, FND-006). Every production failure at the
/// WAL append, fsync, and commit-publication boundaries reaches the engine as
/// an I/O error, so `MongrelError::Io` makes injected faults indistinguishable
/// from real device failures: they exercise the same poison and
/// unknown-outcome paths without a new error variant.
pub(crate) fn fault_as_io(fault: mongreldb_fault::Fault) -> MongrelError {
    MongrelError::Io(std::io::Error::other(fault))
}

/// Maps a per-open WAL transaction id onto a [`TransactionId`]. The mapping is
/// injective within one open generation; the full 128-bit transaction
/// identifier lands with the cluster id work (spec section 7).
fn transaction_id_from_txn(txn_id: u64) -> TransactionId {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&txn_id.to_le_bytes());
    TransactionId::from_bytes(bytes)
}

/// Interim HLC source for standalone commit receipts (spec section 8.1):
/// system-clock microseconds with a Mutex-guarded last value so timestamps
/// never regress, with the logical counter absorbing same-microsecond commits.
/// The full `HlcClock` (send/receive causal rules, drift bounds, node ids)
/// lands with the types crate's clock work; on a single node, physical time
/// plus this tiebreak is sufficient for a monotonic commit timestamp.
#[derive(Debug, Default)]
struct SystemHlcClock {
    last: Mutex<(u64, u32)>,
}

impl SystemHlcClock {
    fn now(&self) -> HlcTimestamp {
        let micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_micros() as u64)
            .unwrap_or(0);
        let mut last = self.last.lock();
        if micros > last.0 {
            *last = (micros, 0);
        } else {
            last.1 = last.1.saturating_add(1);
        }
        HlcTimestamp {
            physical_micros: last.0,
            logical: last.1,
            node_tiebreaker: 0,
        }
    }
}

/// The standalone commit log (spec section 9.4): one shared WAL + one
/// group-commit coordinator are the single authority through which commands
/// become committed. `term` is always 0; the log index is the commit epoch.
pub struct StandaloneCommitLog {
    wal: Arc<Mutex<SharedWal>>,
    group: Arc<GroupCommit>,
    epoch: Arc<EpochAuthority>,
    /// Serializes [`CommitLog::propose`] against the database commit sequencer
    /// so a proposed command cannot move the assigned-epoch counter underneath
    /// an in-flight commit.
    commit_lock: Arc<Mutex<()>>,
    /// Shared per-open transaction-id allocator (same one `Database` uses).
    txn_ids: Arc<Mutex<u64>>,
    /// Database root, for WAL replay in [`CommitLog::read_committed`].
    root: PathBuf,
    /// WAL DEK for replaying encrypted segments; `None` for plaintext.
    wal_dek: Option<Zeroizing<[u8; 32]>>,
    clock: SystemHlcClock,
}

impl StandaloneCommitLog {
    pub(crate) fn new(
        wal: Arc<Mutex<SharedWal>>,
        group: Arc<GroupCommit>,
        epoch: Arc<EpochAuthority>,
        commit_lock: Arc<Mutex<()>>,
        txn_ids: Arc<Mutex<u64>>,
        root: PathBuf,
        wal_dek: Option<Zeroizing<[u8; 32]>>,
    ) -> Self {
        Self {
            wal,
            group,
            epoch,
            commit_lock,
            txn_ids,
            root,
            wal_dek,
            clock: SystemHlcClock::default(),
        }
    }

    /// Owns the WAL append step of the commit sequencer (spec section 9.4):
    /// writes the transaction command's records followed by its commit marker
    /// and returns the commit record's WAL sequence. The caller holds the
    /// database commit lock and the WAL lock; no fsync happens here.
    pub(crate) fn append_transaction(
        &self,
        wal: &mut SharedWal,
        txn_id: u64,
        epoch: Epoch,
        records: Vec<(u64, Op)>,
        added_runs: &[AddedRun],
    ) -> Result<u64, LogError> {
        for (table_id, op) in records {
            wal.append(txn_id, table_id, op)
                .map_err(|error| LogError::Internal(error.to_string()))?;
        }
        wal.append_commit(txn_id, epoch, added_runs)
            .map_err(|error| LogError::Internal(error.to_string()))
    }

    /// Owns the group-commit durability step of the commit sequencer: blocks
    /// until `commit_seq` is durable (one leader fsync serves the batch) and
    /// issues the irrevocable receipt that gates visibility publication.
    pub(crate) fn seal_transaction(
        &self,
        txn_id: u64,
        epoch: Epoch,
        commit_seq: u64,
    ) -> Result<CommitReceipt, LogError> {
        self.group
            .await_durable(&self.wal, commit_seq)
            .map_err(|error| LogError::Internal(error.to_string()))?;
        Ok(CommitReceipt {
            transaction_id: transaction_id_from_txn(txn_id),
            commit_ts: self.clock.now(),
            log_position: LogPosition {
                term: 0,
                index: epoch.0,
            },
            durability: DurabilityLevel::GroupCommit,
        })
    }
}

impl CommitLog for StandaloneCommitLog {
    /// Persists one command envelope as a committed WAL transaction
    /// ([`DdlOp::Command`] record + commit marker) and waits for group-commit
    /// durability. The assigned epoch is published once durable; on any error
    /// the ticket is abandoned so the visibility watermark never stalls behind
    /// an epoch hole.
    fn propose(
        &self,
        command: CommandEnvelope,
        control: &mongreldb_log::ExecutionControl,
    ) -> Result<CommitReceipt, LogError> {
        command.verify()?;
        control.check()?;
        let _commit = self.commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let result = (|| {
            let txn_id = crate::txn::allocate_txn_id(&self.txn_ids)
                .map_err(|error| LogError::Internal(error.to_string()))?;
            let record = Op::Ddl(DdlOp::Command {
                payload: command.encode(),
            });
            let commit_seq = {
                let mut wal = self.wal.lock();
                wal.append(txn_id, crate::database::WAL_TABLE_ID, record)
                    .and_then(|_| wal.append_commit(txn_id, epoch, &[]))
                    .map_err(|error| LogError::Internal(error.to_string()))?
            };
            let receipt = self.seal_transaction(txn_id, epoch, commit_seq)?;
            mongreldb_fault::inject("commit.publish.before")
                .map_err(|fault| LogError::Internal(fault.to_string()))?;
            self.epoch.publish_in_order(epoch);
            mongreldb_fault::inject("commit.publish.after")
                .map_err(|fault| LogError::Internal(fault.to_string()))?;
            Ok(receipt)
        })();
        if result.is_err() {
            // A failed proposal commits no data: abandon the assigned ticket so
            // later publishes are not gated on an epoch hole.
            self.epoch.abandon(epoch);
        }
        result
    }

    /// Replays committed command envelopes with `position.index > after.index`
    /// in commit order, using the existing WAL reader. Only transactions sealed
    /// by a durable `TxnCommit` marker are returned, and replay is constrained
    /// to the authenticated durable WAL head, so unacknowledged appends never
    /// surface. `after.term` is ignored: the standalone log has a single term.
    fn read_committed(
        &self,
        after: LogPosition,
        limit: usize,
    ) -> Result<Vec<CommittedEntry>, LogError> {
        // Serialize against segment rotation, GC, and WAL-head rewrites.
        let _wal = self.wal.lock();
        let records = SharedWal::replay_with_dek(&self.root, self.wal_dek.as_ref())
            .map_err(|error| LogError::Internal(error.to_string()))?;
        let mut commits = std::collections::HashMap::new();
        let mut timestamps = std::collections::HashMap::new();
        for record in &records {
            match record.op {
                Op::TxnCommit { epoch, .. } => {
                    commits.insert(record.txn_id, epoch);
                }
                Op::CommitTimestamp { unix_nanos } => {
                    timestamps.insert(record.txn_id, unix_nanos);
                }
                _ => {}
            }
        }
        let mut entries = Vec::new();
        for record in &records {
            if entries.len() >= limit {
                break;
            }
            let Op::Ddl(DdlOp::Command { payload }) = &record.op else {
                continue;
            };
            let Some(&epoch) = commits.get(&record.txn_id) else {
                continue;
            };
            if epoch <= after.index {
                continue;
            }
            let envelope = CommandEnvelope::decode(payload)?;
            let physical_micros = timestamps.get(&record.txn_id).copied().unwrap_or(0) / 1_000;
            entries.push(CommittedEntry {
                position: LogPosition {
                    term: 0,
                    index: epoch,
                },
                commit_ts: HlcTimestamp {
                    physical_micros,
                    logical: 0,
                    node_tiebreaker: 0,
                },
                envelope,
            });
        }
        Ok(entries)
    }

    /// The highest WAL sequence made durable by group commit. In standalone
    /// mode the local state machine applies everything the WAL makes durable.
    ///
    /// Note the Stage 0 units: receipt positions use the commit epoch while
    /// this watermark uses the WAL record sequence (`durable_seq`), which
    /// advances by several records per commit. Stage 2 unifies both behind one
    /// replicated log index.
    fn applied_position(&self) -> LogPosition {
        LogPosition {
            term: 0,
            index: self.wal.lock().durable_seq(),
        }
    }

    /// Unsupported in Stage 0: replicated log-snapshot boundaries arrive with
    /// the consensus adapter in Stage 2 (spec section 9.4). The standalone
    /// database's checkpoint/backup machinery already covers local images.
    fn create_snapshot(&self) -> Result<LogSnapshot, LogError> {
        Err(LogError::Unsupported(
            "standalone commit log does not create log snapshots; replicated snapshot boundaries arrive in Stage 2",
        ))
    }

    /// Unsupported in Stage 0: see [`CommitLog::create_snapshot`].
    fn install_snapshot(&self, _snapshot: LogSnapshot) -> Result<(), LogError> {
        Err(LogError::Unsupported(
            "standalone commit log does not install log snapshots; replicated snapshot boundaries arrive in Stage 2",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn system_hlc_clock_is_monotonic() {
        let clock = SystemHlcClock::default();
        let mut previous = clock.now();
        for _ in 0..1_000 {
            let next = clock.now();
            assert!(next > previous, "{next} must exceed {previous}");
            previous = next;
        }
    }

    #[test]
    fn to_log_control_maps_deadline_and_leaves_cancellation_to_checkpoint() {
        let control = crate::ExecutionControl::new(None);
        let converted = to_log_control(&control);
        assert!(converted.deadline.is_none());
        assert!(converted.cancellation.is_none());
        assert!(converted.check().is_ok());

        let deadline = Instant::now() + Duration::from_secs(60);
        let control = crate::ExecutionControl::new(Some(deadline));
        let converted = to_log_control(&control);
        assert_eq!(converted.deadline, Some(deadline));

        let expired = crate::ExecutionControl::new(Some(Instant::now()));
        let converted = to_log_control(&expired);
        assert!(matches!(converted.check(), Err(LogError::DeadlineExceeded)));
    }

    #[test]
    fn transaction_id_mapping_is_injective() {
        assert_ne!(transaction_id_from_txn(1), transaction_id_from_txn(2));
        assert_eq!(
            transaction_id_from_txn(7),
            transaction_id_from_txn(7),
            "mapping must be deterministic"
        );
    }
}
