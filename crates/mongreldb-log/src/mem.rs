//! In-memory [`CommitLog`] implementation (spec section 9.4, FND-004).
//!
//! [`InMemoryCommitLog`] is a thread-safe, fully in-memory commit log for
//! tests, embedded single-process use, and deterministic simulation. It keeps
//! every committed entry in memory and assigns strictly increasing positions:
//! `term` is always zero and `index` starts at one, so [`LogPosition::ZERO`]
//! precedes every entry. The log applies each command the moment it commits,
//! so [`CommitLog::applied_position`] advances as entries are proposed.
//! Receipts report [`DurabilityLevel::GroupCommit`] only nominally — nothing
//! is ever fsynced.
//!
//! [`CommitLog::create_snapshot`] captures the committed entries through the
//! applied position in a versioned, self-describing byte format, and
//! [`CommitLog::install_snapshot`] replaces the log state with them, so a
//! snapshot taken on one log installs cleanly into another. The in-memory
//! snapshot retains full entries (no compaction) to keep behavior observable
//! in tests.

use core::fmt;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::TransactionId;

use crate::commit_log::{
    CommitLog, CommitReceipt, CommittedEntry, DurabilityLevel, ExecutionControl, LogError,
    LogPosition, LogSnapshot,
};
use crate::envelope::CommandEnvelope;

/// An injectable source of commit timestamps for [`InMemoryCommitLog`].
///
/// The source is called once per accepted proposal, under the log lock, and
/// should be non-decreasing so commit timestamps order with log positions.
pub type TimestampSource = Box<dyn FnMut() -> HlcTimestamp + Send>;

const SNAPSHOT_MAGIC: [u8; 8] = *b"MLOGSNAP";
const SNAPSHOT_FORMAT_VERSION: u32 = 1;

struct State {
    entries: Vec<CommittedEntry>,
    next_index: u64,
    timestamp_source: TimestampSource,
}

/// Thread-safe in-memory [`CommitLog`]; see the module documentation.
pub struct InMemoryCommitLog {
    state: Mutex<State>,
}

impl InMemoryCommitLog {
    /// Creates an empty log whose commit timestamps come from the system
    /// clock in microseconds (`logical` and `node_tiebreaker` are zero).
    pub fn new() -> Self {
        Self::with_timestamp_source(Box::new(system_time_micros))
    }

    /// Creates an empty log with an injectable commit-timestamp source.
    pub fn with_timestamp_source(timestamp_source: TimestampSource) -> Self {
        Self {
            state: Mutex::new(State {
                entries: Vec::new(),
                next_index: 1,
                timestamp_source,
            }),
        }
    }
}

impl Default for InMemoryCommitLog {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for InMemoryCommitLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("InMemoryCommitLog");
        match self.state.lock() {
            Ok(state) => d
                .field("entries", &state.entries.len())
                .field("next_index", &state.next_index)
                .finish(),
            Err(_) => d.finish_non_exhaustive(),
        }
    }
}

impl CommitLog for InMemoryCommitLog {
    /// Verifies the envelope, honors cancellation/deadline, then commits and
    /// applies the command in one step. The receipt's transaction id is
    /// derived from the envelope's command id (the idempotent-apply
    /// identifier).
    fn propose(
        &self,
        command: CommandEnvelope,
        control: &ExecutionControl,
    ) -> Result<CommitReceipt, LogError> {
        control.check()?;
        command.verify()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| LogError::Internal("in-memory commit log lock poisoned".to_owned()))?;
        let position = LogPosition {
            term: 0,
            index: state.next_index,
        };
        state.next_index += 1;
        let commit_ts = (state.timestamp_source)();
        let command_id = command.command_id;
        state.entries.push(CommittedEntry {
            position,
            commit_ts,
            envelope: command,
        });
        Ok(CommitReceipt {
            transaction_id: TransactionId::from_bytes(command_id),
            commit_ts,
            log_position: position,
            durability: DurabilityLevel::GroupCommit,
        })
    }

    fn read_committed(
        &self,
        after: LogPosition,
        limit: usize,
    ) -> Result<Vec<CommittedEntry>, LogError> {
        let state = self
            .state
            .lock()
            .map_err(|_| LogError::Internal("in-memory commit log lock poisoned".to_owned()))?;
        Ok(state
            .entries
            .iter()
            .filter(|entry| entry.position > after)
            .take(limit)
            .cloned()
            .collect())
    }

    fn applied_position(&self) -> LogPosition {
        self.state
            .lock()
            .expect("in-memory commit log lock poisoned")
            .entries
            .last()
            .map_or(LogPosition::ZERO, |entry| entry.position)
    }

    fn create_snapshot(&self) -> Result<LogSnapshot, LogError> {
        let state = self
            .state
            .lock()
            .map_err(|_| LogError::Internal("in-memory commit log lock poisoned".to_owned()))?;
        let (position, commit_ts) = state
            .entries
            .last()
            .map_or((LogPosition::ZERO, HlcTimestamp::ZERO), |entry| {
                (entry.position, entry.commit_ts)
            });
        Ok(LogSnapshot {
            position,
            commit_ts,
            data: encode_snapshot(&state.entries),
        })
    }

    fn install_snapshot(&self, snapshot: LogSnapshot) -> Result<(), LogError> {
        let entries = decode_snapshot(&snapshot.data)?;
        match entries.last() {
            Some(last) if last.position != snapshot.position => {
                return Err(LogError::Internal(
                    "in-memory snapshot position does not match its last entry".to_owned(),
                ));
            }
            None if snapshot.position != LogPosition::ZERO => {
                return Err(LogError::Internal(
                    "in-memory snapshot carries no entries but a nonzero position".to_owned(),
                ));
            }
            _ => {}
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| LogError::Internal("in-memory commit log lock poisoned".to_owned()))?;
        state.entries = entries;
        state.next_index = snapshot.position.index + 1;
        Ok(())
    }
}

/// Commit timestamp from the system clock in microseconds since the Unix
/// epoch; the default [`TimestampSource`].
fn system_time_micros() -> HlcTimestamp {
    let physical_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as u64);
    HlcTimestamp {
        physical_micros,
        logical: 0,
        node_tiebreaker: 0,
    }
}

/// Serializes committed entries into the versioned, deterministic snapshot
/// byte format. Every envelope uses its canonical encoding, so the checksum
/// is re-verified on decode.
fn encode_snapshot(entries: &[CommittedEntry]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&SNAPSHOT_MAGIC);
    out.extend_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for entry in entries {
        out.extend_from_slice(&entry.position.term.to_le_bytes());
        out.extend_from_slice(&entry.position.index.to_le_bytes());
        out.extend_from_slice(&entry.commit_ts.physical_micros.to_le_bytes());
        out.extend_from_slice(&entry.commit_ts.logical.to_le_bytes());
        out.extend_from_slice(&entry.commit_ts.node_tiebreaker.to_le_bytes());
        let envelope = entry.envelope.encode();
        out.extend_from_slice(&(envelope.len() as u32).to_le_bytes());
        out.extend_from_slice(&envelope);
    }
    out
}

fn malformed(reason: &str) -> LogError {
    LogError::Internal(format!("malformed in-memory snapshot: {reason}"))
}

/// Byte cursor over snapshot data that fails closed on truncation.
struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], LogError> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| malformed("truncated"))?;
        let slice = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(slice)
    }

    fn read_u32(&mut self) -> Result<u32, LogError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("slice len"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64, LogError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("slice len"),
        ))
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }
}

/// Parses snapshot data produced by [`encode_snapshot`], verifying every
/// envelope and rejecting truncated or trailing bytes.
fn decode_snapshot(data: &[u8]) -> Result<Vec<CommittedEntry>, LogError> {
    let mut cursor = Cursor {
        bytes: data,
        offset: 0,
    };
    if cursor.take(SNAPSHOT_MAGIC.len())? != SNAPSHOT_MAGIC.as_slice() {
        return Err(malformed("bad magic"));
    }
    let format_version = cursor.read_u32()?;
    if format_version != SNAPSHOT_FORMAT_VERSION {
        return Err(malformed("unsupported format version"));
    }
    let entry_count = cursor.read_u64()?;
    let mut entries = Vec::with_capacity(entry_count.min(1024) as usize);
    for _ in 0..entry_count {
        let term = cursor.read_u64()?;
        let index = cursor.read_u64()?;
        let physical_micros = cursor.read_u64()?;
        let logical = cursor.read_u32()?;
        let node_tiebreaker = cursor.read_u32()?;
        let envelope_len = cursor.read_u32()? as usize;
        let envelope = CommandEnvelope::decode(cursor.take(envelope_len)?)?;
        entries.push(CommittedEntry {
            position: LogPosition { term, index },
            commit_ts: HlcTimestamp {
                physical_micros,
                logical,
                node_tiebreaker,
            },
            envelope,
        });
    }
    if cursor.remaining() != 0 {
        return Err(malformed("trailing bytes"));
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(id: u8) -> CommandEnvelope {
        CommandEnvelope::new(1, [id; 16], vec![id; 8])
    }

    #[test]
    fn empty_log_starts_at_zero() {
        let log = InMemoryCommitLog::new();
        assert_eq!(log.applied_position(), LogPosition::ZERO);
        assert!(log
            .read_committed(LogPosition::ZERO, 10)
            .unwrap()
            .is_empty());
        let snapshot = log.create_snapshot().unwrap();
        assert_eq!(snapshot.position, LogPosition::ZERO);
        assert_eq!(snapshot.commit_ts, HlcTimestamp::ZERO);
    }

    #[test]
    fn malformed_snapshot_data_is_rejected() {
        let log = InMemoryCommitLog::new();
        let snapshot = LogSnapshot {
            position: LogPosition::ZERO,
            commit_ts: HlcTimestamp::ZERO,
            data: b"garbage".to_vec(),
        };
        assert!(matches!(
            log.install_snapshot(snapshot),
            Err(LogError::Internal(_))
        ));
    }

    #[test]
    fn snapshot_position_mismatch_is_rejected() {
        let source = InMemoryCommitLog::new();
        source
            .propose(envelope(1), &ExecutionControl::default())
            .unwrap();
        let mut snapshot = source.create_snapshot().unwrap();
        snapshot.position = LogPosition { term: 0, index: 99 };
        let log = InMemoryCommitLog::new();
        assert!(matches!(
            log.install_snapshot(snapshot),
            Err(LogError::Internal(_))
        ));
        assert_eq!(log.applied_position(), LogPosition::ZERO);
    }
}
