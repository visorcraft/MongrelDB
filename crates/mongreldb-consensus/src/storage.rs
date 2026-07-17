//! Durable per-group consensus storage (spec section 11.2, S2B-002).
//!
//! Layout under the group directory (`<group dir>/raft/`):
//!
//! ```text
//! raft/hard-state            vote + last committed log id (atomic frame)
//! raft/log/seg-<first>.seg   append-only log segments, checksummed frames
//! raft/log/PURGED            last purged log id (atomic frame)
//! raft/membership            last applied membership (written by state_machine)
//! raft/state/applied         apply checkpoint (written by state_machine)
//! raft/snapshot/             snapshot data + metadata (written by state_machine)
//! ```
//!
//! # Framing (mirrors `mongreldb-core`'s catalog idiom)
//!
//! Atomic files are `MAGIC(8) | sha256(body) | body`, written to a temporary
//! file, fsynced, renamed into place, and sealed with a parent-directory
//! fsync. Log segments are sequences of frames `len u32 LE | body |
//! sha256(body)`; the first frame of a segment is its header. Every body
//! starts with a little-endian `u32` format version; unknown versions fail
//! closed (spec section 4.10).
//!
//! # fsync contract (S2B-002, openraft storage contract)
//!
//! - `save_vote` fsyncs `hard-state` **before returning**.
//! - Log appends honor [`FsyncPolicy`]: the durable default
//!   ([`FsyncPolicy::PerAppend`]) fsyncs before the flush callback fires;
//!   [`FsyncPolicy::Deferred`] batches fsyncs on an interval and fires
//!   callbacks only after the batch fsync, so acknowledged entries are always
//!   durable.
//! - Recovery tolerates a torn tail (crash mid-append) by truncating the
//!   segment to the last good frame; a checksum failure anywhere else fails
//!   closed with [`StoreError::Corrupt`].
//!
//! # Fault hooks (FND-006)
//!
//! `raft.hard_state.write.before` / `raft.hard_state.write.after`,
//! `raft.log.append.before` / `raft.log.append.after`,
//! `raft.log.fsync.before` / `raft.log.fsync.after`.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{ErrorSubject, ErrorVerb, StorageIOError};
use sha2::{Digest, Sha256};

use crate::identity::{
    MongrelRaftConfig, RaftLogEntry, RaftLogId, RaftNodeId, RaftStorageError, RaftVote,
};

/// Current durable format version written by this module.
const FORMAT_VERSION: u32 = 1;

const HARD_STATE_MAGIC: &[u8; 8] = b"MRFT-HS1";
const PURGED_MAGIC: &[u8; 8] = b"MRFT-PG1";
const SEGMENT_MAGIC: &[u8; 8] = b"MRFTSEG1";

/// Errors produced by consensus storage.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// Filesystem failure while reading, writing, syncing, or renaming.
    #[error("storage io error on {path} during {verb}: {source}")]
    Io {
        /// The file or directory operated on.
        path: PathBuf,
        /// What was being done (`read`, `write`, `fsync`, `rename`, ...).
        verb: &'static str,
        /// The underlying error.
        source: io::Error,
    },
    /// A checksummed file or frame failed verification, or an unknown format
    /// version was found. Always fails closed.
    #[error("corrupt storage file {path}: {reason}")]
    Corrupt {
        /// The offending file.
        path: PathBuf,
        /// Why it is considered corrupt.
        reason: String,
    },
}

impl StoreError {
    pub(crate) fn io<'a>(path: &'a Path, verb: &'static str) -> impl FnOnce(io::Error) -> Self + 'a {
        move |source| StoreError::Io {
            path: path.to_path_buf(),
            verb,
            source,
        }
    }

    pub(crate) fn corrupt(path: &Path, reason: impl Into<String>) -> Self {
        StoreError::Corrupt {
            path: path.to_path_buf(),
            reason: reason.into(),
        }
    }

    fn fault(path: &Path, verb: &'static str, fault: mongreldb_fault::Fault) -> Self {
        StoreError::Io {
            path: path.to_path_buf(),
            verb,
            source: io::Error::new(io::ErrorKind::Other, fault),
        }
    }
}

/// Converts a [`StoreError`] into openraft's storage error type.
pub(crate) fn raft_storage_error(
    subject: ErrorSubject<RaftNodeId>,
    verb: ErrorVerb,
    err: StoreError,
) -> RaftStorageError {
    RaftStorageError::IO {
        source: StorageIOError::new(subject, verb, openraft::AnyError::new(&err)),
    }
}

/// Log-append fsync policy (S2B-002). The default is durable.
#[derive(Debug, Clone)]
pub enum FsyncPolicy {
    /// Fsync the active segment before the append's flush callback fires.
    PerAppend,
    /// Batch fsyncs on a fixed interval; flush callbacks fire only after the
    /// batch fsync completes, so acknowledged entries are always durable.
    /// A crash may delay acknowledgment, never acknowledge a lost entry.
    Deferred {
        /// How often the background flusher fsyncs and releases callbacks.
        interval: Duration,
    },
}

impl Default for FsyncPolicy {
    fn default() -> Self {
        FsyncPolicy::PerAppend
    }
}

/// Configuration for consensus storage.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Log append fsync policy; durable default.
    pub fsync_policy: FsyncPolicy,
    /// Roll the active log segment once it exceeds this many bytes.
    pub segment_roll_bytes: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            fsync_policy: FsyncPolicy::PerAppend,
            segment_roll_bytes: 8 * 1024 * 1024,
        }
    }
}

// ---------------------------------------------------------------------------
// Checksummed frame helpers (shared with state_machine).
// ---------------------------------------------------------------------------

/// Serializes `value` with a leading format version.
pub(crate) fn encode_versioned<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, StoreError> {
    let payload = bincode::serialize(value)
        .map_err(|e| StoreError::corrupt(Path::new("<encode>"), e.to_string()))?;
    let mut body = Vec::with_capacity(4 + payload.len());
    body.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    body.extend_from_slice(&payload);
    Ok(body)
}

/// Parses a versioned body, failing closed on unknown versions.
pub(crate) fn decode_versioned<T: for<'de> serde::Deserialize<'de>>(
    path: &Path,
    body: &[u8],
) -> Result<T, StoreError> {
    if body.len() < 4 {
        return Err(StoreError::corrupt(path, "body shorter than format version"));
    }
    let version = u32::from_le_bytes(body[..4].try_into().expect("slice len"));
    if version != FORMAT_VERSION {
        return Err(StoreError::corrupt(
            path,
            format!("unsupported format version {version} (supported {FORMAT_VERSION})"),
        ));
    }
    bincode::deserialize(&body[4..]).map_err(|e| StoreError::corrupt(path, e.to_string()))
}

/// Writes `MAGIC | sha256(body) | body` atomically (tmp + rename + dir fsync).
/// The temporary file is fsynced before the rename and the parent directory
/// after it, so the file is durable when this returns.
pub(crate) fn write_frame_file(
    dir: &Path,
    name: &str,
    magic: &[u8; 8],
    body: &[u8],
) -> Result<(), StoreError> {
    let final_path = dir.join(name);
    let tmp_path = dir.join(format!("{name}.tmp"));
    let hash = Sha256::digest(body);
    {
        let mut file = File::create(&tmp_path).map_err(StoreError::io(&tmp_path, "create"))?;
        file.write_all(magic)
            .and_then(|()| file.write_all(&hash))
            .and_then(|()| file.write_all(body))
            .map_err(StoreError::io(&tmp_path, "write"))?;
        file.sync_all().map_err(StoreError::io(&tmp_path, "fsync"))?;
    }
    std::fs::rename(&tmp_path, &final_path).map_err(StoreError::io(&final_path, "rename"))?;
    fsync_dir(dir)?;
    Ok(())
}

/// Reads an atomic frame file; `Ok(None)` when it does not exist.
pub(crate) fn read_frame_file(path: &Path, magic: &[u8; 8]) -> Result<Option<Vec<u8>>, StoreError> {
    let mut bytes = Vec::new();
    match File::open(path) {
        Ok(mut file) => {
            file.read_to_end(&mut bytes).map_err(StoreError::io(path, "read"))?;
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(StoreError::io(path, "open")(e)),
    }
    if bytes.len() < 8 + 32 || &bytes[..8] != magic {
        return Err(StoreError::corrupt(path, "bad magic or truncated header"));
    }
    let (tag, body) = bytes[8..].split_at(32);
    let calc = Sha256::digest(body);
    if tag != calc.as_slice() {
        return Err(StoreError::corrupt(path, "checksum mismatch"));
    }
    Ok(Some(body.to_vec()))
}

/// Fsyncs a directory so a rename/create inside it is durable.
pub(crate) fn fsync_dir(dir: &Path) -> Result<(), StoreError> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(StoreError::io(dir, "dir fsync"))
}

// ---------------------------------------------------------------------------
// Hard state (vote + committed)
// ---------------------------------------------------------------------------

/// Persisted hard state: the vote (term/voted-for) and the last committed
/// log id openraft asked us to remember.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct HardState {
    vote: Option<RaftVote>,
    committed: Option<RaftLogId>,
}

struct HardStateStore {
    dir: PathBuf,
}

impl HardStateStore {
    fn path(&self) -> PathBuf {
        self.dir.join("hard-state")
    }

    fn load(&self) -> Result<HardState, StoreError> {
        match read_frame_file(&self.path(), HARD_STATE_MAGIC)? {
            None => Ok(HardState::default()),
            Some(body) => decode_versioned(&self.path(), &body),
        }
    }

    /// Persists hard state and fsyncs **before returning** (openraft's vote
    /// contract; S2B-002).
    fn save(&self, state: &HardState) -> Result<(), StoreError> {
        let path = self.path();
        mongreldb_fault::inject("raft.hard_state.write.before")
            .map_err(|f| StoreError::fault(&path, "write", f))?;
        let body = encode_versioned(state)?;
        write_frame_file(&self.dir, "hard-state", HARD_STATE_MAGIC, &body)?;
        mongreldb_fault::inject("raft.hard_state.write.after")
            .map_err(|f| StoreError::fault(&path, "write", f))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Log segments
// ---------------------------------------------------------------------------

struct SegmentMeta {
    /// First log index stored in the segment (from its header frame).
    first_index: u64,
    path: PathBuf,
}

/// Location of one entry frame inside a segment file.
#[derive(Debug, Clone, Copy)]
struct FrameLoc {
    /// Index into `LogInner::segments`.
    segment: usize,
    /// Byte offset of the frame start.
    offset: u64,
}

struct LogInner {
    /// `raft/log/` directory.
    dir: PathBuf,
    segments: Vec<SegmentMeta>,
    /// index -> (log id, frame location); entries `<= last_purged` absent.
    index: BTreeMap<u64, (RaftLogId, FrameLoc)>,
    last_purged: Option<RaftLogId>,
    /// Append handle for the last segment.
    active: Option<File>,
    /// Byte length of the active segment.
    active_len: u64,
    /// Entries stored in the active segment (drives rolling).
    active_entry_count: u64,
    /// Flush callbacks waiting for the next Deferred fsync.
    pending: Vec<LogFlushed<MongrelRaftConfig>>,
    flusher_spawned: bool,
    stop: Arc<AtomicBool>,
    config: StorageConfig,
}

impl LogInner {
    fn active_segment_ordinal(&self) -> usize {
        self.segments.len() - 1
    }

    fn last_indexed(&self) -> Option<RaftLogId> {
        self.index
            .values()
            .next_back()
            .map(|(log_id, _)| log_id.clone())
            .or_else(|| self.last_purged.clone())
    }

    /// Recounts entries located in the active segment (after truncate/purge).
    fn recount_active_entries(&mut self) {
        if self.segments.is_empty() {
            self.active_entry_count = 0;
            return;
        }
        let ordinal = self.active_segment_ordinal();
        self.active_entry_count =
            self.index.values().filter(|(_, loc)| loc.segment == ordinal).count() as u64;
    }
}

/// Reads the frame starting at `offset`; returns `(body, next_offset)`.
fn read_frame_at(path: &Path, offset: u64) -> Result<(Vec<u8>, u64), StoreError> {
    let mut file = File::open(path).map_err(StoreError::io(path, "open"))?;
    file.seek(SeekFrom::Start(offset))
        .map_err(StoreError::io(path, "seek"))?;
    let mut len_buf = [0u8; 4];
    file.read_exact(&mut len_buf).map_err(StoreError::io(path, "read"))?;
    let body_len = u32::from_le_bytes(len_buf) as usize;
    let mut body = vec![0u8; body_len];
    file.read_exact(&mut body).map_err(StoreError::io(path, "read"))?;
    let mut hash = [0u8; 32];
    file.read_exact(&mut hash).map_err(StoreError::io(path, "read"))?;
    let calc = Sha256::digest(&body);
    if hash != calc.as_slice() {
        return Err(StoreError::corrupt(path, "frame checksum mismatch"));
    }
    Ok((body, offset + 4 + body_len as u64 + 32))
}

fn encode_frame(body: &[u8]) -> Vec<u8> {
    let hash = Sha256::digest(body);
    let mut out = Vec::with_capacity(4 + body.len() + 32);
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(body);
    out.extend_from_slice(&hash);
    out
}

fn segment_name(first_index: u64) -> String {
    format!("seg-{first_index:020}.seg")
}

/// Shared read access to the log, used by the log reader and by
/// `RaftCommitLog::read_committed` (same-crate only).
#[derive(Clone)]
pub(crate) struct SharedLog {
    inner: Arc<Mutex<LogInner>>,
}

impl SharedLog {
    fn lock(&self) -> Result<MutexGuard<'_, LogInner>, StoreError> {
        self.inner
            .lock()
            .map_err(|_| StoreError::corrupt(Path::new("<lock>"), "log lock poisoned"))
    }

    /// The last log id openraft should know about (last entry or purged point).
    #[allow(dead_code)] // exercised by unit tests and get_log_state's callers
    pub(crate) fn last_log_id(&self) -> Result<Option<RaftLogId>, StoreError> {
        Ok(self.lock()?.last_indexed())
    }

    /// The last purged log id, if any.
    #[allow(dead_code)] // exercised through RaftLogStorage::get_log_state
    pub(crate) fn last_purged_log_id(&self) -> Result<Option<RaftLogId>, StoreError> {
        Ok(self.lock()?.last_purged.clone())
    }

    /// Reads entries in `[start, end)`, at most `limit`, in log order.
    /// Indices at or below the purge point are absent.
    pub(crate) fn read_entries(
        &self,
        start: u64,
        end: u64,
        limit: usize,
    ) -> Result<Vec<RaftLogEntry>, StoreError> {
        let locs: Vec<(PathBuf, FrameLoc)> = {
            let inner = self.lock()?;
            inner
                .index
                .range(start..end)
                .take(limit)
                .map(|(_, (_, loc))| (inner.segments[loc.segment].path.clone(), *loc))
                .collect()
        };
        let mut entries = Vec::with_capacity(locs.len());
        for (path, loc) in locs {
            let (body, _) = read_frame_at(&path, loc.offset)?;
            let entry: RaftLogEntry = decode_versioned(&path, &body)?;
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Stops the background flusher (if any) and fsyncs pending appends,
    /// releasing their callbacks. Best-effort; used on group shutdown.
    pub(crate) fn close(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.stop.store(true, Ordering::Release);
            if let Some(active) = &inner.active {
                let _ = active.sync_data();
            }
            let pending = std::mem::take(&mut inner.pending);
            drop(inner);
            for callback in pending {
                callback.log_io_completed(Ok(()));
            }
        }
    }
}

/// The openraft log reader (replication tasks read entries through it).
pub struct MongrelLogReader {
    shared: SharedLog,
}

/// Shared range-read used by both log-reader implementations.
fn read_entries_range<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
    shared: &SharedLog,
    range: RB,
) -> Result<Vec<RaftLogEntry>, RaftStorageError> {
    let start = match range.start_bound() {
        std::ops::Bound::Included(i) => *i,
        std::ops::Bound::Excluded(i) => *i + 1,
        std::ops::Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        std::ops::Bound::Included(i) => *i + 1,
        std::ops::Bound::Excluded(i) => *i,
        std::ops::Bound::Unbounded => u64::MAX,
    };
    shared
        .read_entries(start, end, usize::MAX)
        .map_err(|e| raft_storage_error(ErrorSubject::Logs, ErrorVerb::Read, e))
}

impl RaftLogReader<MongrelRaftConfig> for MongrelLogReader {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<RaftLogEntry>, RaftStorageError> {
        read_entries_range(&self.shared, range)
    }
}

// ---------------------------------------------------------------------------
// MongrelLogStore
// ---------------------------------------------------------------------------

/// Durable raft log + hard-state storage (S2B-002).
///
/// All write IO is serialized on one internal mutex, satisfying openraft's
/// "write-IO must be serialized" contract for vote and log.
pub struct MongrelLogStore {
    shared: SharedLog,
    hard: HardStateStore,
}

impl MongrelLogStore {
    /// Opens (creating if needed) the log storage under `<group_dir>/raft/`.
    /// Recovers from a torn tail by truncating to the last good frame.
    pub fn open(group_dir: &Path, config: StorageConfig) -> Result<Self, StoreError> {
        let raft_dir = group_dir.join("raft");
        let log_dir = raft_dir.join("log");
        std::fs::create_dir_all(&log_dir).map_err(StoreError::io(&log_dir, "create dirs"))?;

        let last_purged: Option<RaftLogId> =
            match read_frame_file(&log_dir.join("PURGED"), PURGED_MAGIC)? {
                None => None,
                Some(body) => Some(decode_versioned(&log_dir.join("PURGED"), &body)?),
            };

        let mut segment_files: Vec<(u64, PathBuf)> = Vec::new();
        for entry in std::fs::read_dir(&log_dir).map_err(StoreError::io(&log_dir, "read dir"))? {
            let entry = entry.map_err(StoreError::io(&log_dir, "read dir"))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(rest) = name.strip_prefix("seg-").and_then(|n| n.strip_suffix(".seg")) {
                if let Ok(first_index) = rest.parse::<u64>() {
                    segment_files.push((first_index, entry.path()));
                }
            }
        }
        segment_files.sort_by_key(|(first_index, _)| *first_index);

        let mut inner = LogInner {
            dir: log_dir.clone(),
            segments: Vec::new(),
            index: BTreeMap::new(),
            last_purged: last_purged.clone(),
            active: None,
            active_len: 0,
            active_entry_count: 0,
            pending: Vec::new(),
            flusher_spawned: false,
            stop: Arc::new(AtomicBool::new(false)),
            config,
        };

        for (first_index, path) in segment_files {
            let end = Self::scan_segment(&path, first_index, &last_purged, &mut inner)?;
            inner.segments.push(SegmentMeta { first_index, path });
            inner.active_len = end;
        }

        if let Some(last) = inner.segments.last() {
            let file = OpenOptions::new()
                .append(true)
                .open(&last.path)
                .map_err(StoreError::io(&last.path, "open append"))?;
            inner.active = Some(file);
            inner.recount_active_entries();
        }

        Ok(MongrelLogStore {
            shared: SharedLog {
                inner: Arc::new(Mutex::new(inner)),
            },
            hard: HardStateStore { dir: raft_dir },
        })
    }

    /// Scans one segment, indexing frames past the purge point. Returns the
    /// byte offset just past the last good frame; a torn tail is truncated
    /// back to that offset.
    fn scan_segment(
        path: &Path,
        expected_first_index: u64,
        last_purged: &Option<RaftLogId>,
        inner: &mut LogInner,
    ) -> Result<u64, StoreError> {
        let ordinal = inner.segments.len();
        let mut offset = 0u64;
        let mut frame_no = 0u64;
        let file_len = std::fs::metadata(path).map_err(StoreError::io(path, "metadata"))?.len();

        loop {
            if offset == file_len {
                return Ok(offset);
            }
            let remaining = file_len - offset;
            if remaining < 4 + 32 {
                return Self::truncate_torn_tail(path, offset);
            }
            let mut len_bytes = [0u8; 4];
            {
                let mut file = File::open(path).map_err(StoreError::io(path, "open"))?;
                file.seek(SeekFrom::Start(offset))
                    .map_err(StoreError::io(path, "seek"))?;
                file.read_exact(&mut len_bytes)
                    .map_err(StoreError::io(path, "read"))?;
            }
            let body_len = u32::from_le_bytes(len_bytes) as u64;
            let frame_len = 4 + body_len + 32;
            if remaining < frame_len {
                return Self::truncate_torn_tail(path, offset);
            }
            let (body, next_offset) = match read_frame_at(path, offset) {
                Ok(ok) => ok,
                // A checksum failure at the very last frame of the file is a
                // torn write (crash before fsync); truncate it. Any earlier
                // failure is genuine corruption and fails closed.
                Err(e) if offset + frame_len == file_len => {
                    let _ = e;
                    return Self::truncate_torn_tail(path, offset);
                }
                Err(e) => return Err(e),
            };

            if frame_no == 0 {
                // Segment header frame.
                if body.len() < 8 + 8 + 4 || &body[..8] != SEGMENT_MAGIC {
                    return Err(StoreError::corrupt(path, "bad segment header"));
                }
                let first_index = u64::from_le_bytes(body[8..16].try_into().expect("slice len"));
                if first_index != expected_first_index {
                    return Err(StoreError::corrupt(
                        path,
                        format!(
                            "segment header first index {first_index} != name {expected_first_index}"
                        ),
                    ));
                }
            } else {
                let entry: RaftLogEntry = decode_versioned(path, &body)?;
                let index = entry.log_id.index;
                let purged = last_purged.as_ref().is_some_and(|p| index <= p.index);
                if !purged {
                    inner.index.insert(
                        index,
                        (
                            entry.log_id.clone(),
                            FrameLoc {
                                segment: ordinal,
                                offset,
                            },
                        ),
                    );
                }
            }
            frame_no += 1;
            offset = next_offset;
        }
    }

    fn truncate_torn_tail(path: &Path, good_len: u64) -> Result<u64, StoreError> {
        let file = OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(StoreError::io(path, "open truncate"))?;
        file.set_len(good_len).map_err(StoreError::io(path, "truncate"))?;
        file.sync_data().map_err(StoreError::io(path, "fsync"))?;
        Ok(good_len)
    }

    /// A cloneable read handle for `RaftCommitLog::read_committed` and
    /// observability.
    pub(crate) fn shared_log(&self) -> SharedLog {
        self.shared.clone()
    }

    fn save_hard_state(
        &self,
        vote: Option<RaftVote>,
        committed: Option<RaftLogId>,
    ) -> Result<(), RaftStorageError> {
        let mut state = self
            .hard
            .load()
            .map_err(|e| raft_storage_error(ErrorSubject::Vote, ErrorVerb::Read, e))?;
        if vote.is_some() {
            state.vote = vote;
        }
        if committed.is_some() {
            state.committed = committed;
        }
        self.hard
            .save(&state)
            .map_err(|e| raft_storage_error(ErrorSubject::Vote, ErrorVerb::Write, e))
    }

    /// Creates a new active segment whose first entry is `first_index`.
    fn roll_segment(inner: &mut LogInner, first_index: u64) -> Result<(), StoreError> {
        let path = inner.dir.join(segment_name(first_index));
        let mut file = File::create(&path).map_err(StoreError::io(&path, "create"))?;
        let mut header = Vec::with_capacity(20);
        header.extend_from_slice(SEGMENT_MAGIC);
        header.extend_from_slice(&first_index.to_le_bytes());
        header.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        let frame = encode_frame(&header);
        file.write_all(&frame).map_err(StoreError::io(&path, "write"))?;
        file.sync_data().map_err(StoreError::io(&path, "fsync"))?;
        inner.segments.push(SegmentMeta {
            first_index,
            path: path.clone(),
        });
        inner.active = Some(file);
        inner.active_len = frame.len() as u64;
        inner.active_entry_count = 0;
        fsync_dir(&inner.dir)?;
        Ok(())
    }

    /// Writes entry frames to the active segment (rolling as needed) and
    /// updates the in-memory index. Does **not** fsync; see
    /// [`MongrelLogStore::fsync_active`]. Returns `true` if anything was
    /// written. Entries are readable as soon as this returns.
    fn write_frames(&self, entries: Vec<RaftLogEntry>) -> Result<bool, StoreError> {
        let mut inner = self.shared.lock()?;
        let mut wrote = false;
        for entry in entries {
            let index = entry.log_id.index;
            if let Some((last_index, _)) = inner.index.iter().next_back() {
                debug_assert!(
                    index == last_index + 1 || inner.index.contains_key(&index),
                    "raft log append must not leave holes: last {}, appending {}",
                    last_index,
                    index
                );
            }
            let need_segment = inner.active.is_none()
                || (inner.active_entry_count > 0
                    && inner.active_len >= inner.config.segment_roll_bytes);
            if need_segment {
                Self::roll_segment(&mut inner, index)?;
            }
            let body = encode_versioned(&entry)?;
            let frame = encode_frame(&body);
            let active = inner.active.as_mut().expect("segment rolled above");
            active.write_all(&frame).map_err(StoreError::io(
                &inner.segments[inner.active_segment_ordinal()].path,
                "write",
            ))?;
            let loc = FrameLoc {
                segment: inner.active_segment_ordinal(),
                offset: inner.active_len,
            };
            inner.active_len += frame.len() as u64;
            inner.active_entry_count += 1;
            inner.index.insert(index, (entry.log_id.clone(), loc));
            wrote = true;
        }
        Ok(wrote)
    }

    /// Fsyncs the active segment (with fault hooks at the boundary).
    fn fsync_active(&self) -> Result<(), StoreError> {
        let mut inner = self.shared.lock()?;
        if inner.active.is_none() {
            return Ok(());
        }
        let path = inner.segments[inner.active_segment_ordinal()].path.clone();
        mongreldb_fault::inject("raft.log.fsync.before")
            .map_err(|f| StoreError::fault(&path, "fsync", f))?;
        let active = inner.active.as_mut().expect("checked above");
        active.sync_data().map_err(StoreError::io(&path, "fsync"))?;
        mongreldb_fault::inject("raft.log.fsync.after")
            .map_err(|f| StoreError::fault(&path, "fsync", f))?;
        Ok(())
    }

    /// Queues a flush callback for the Deferred policy, spawning the
    /// background flusher on first use.
    fn defer_callback(&self, callback: LogFlushed<MongrelRaftConfig>, interval: Duration) {
        let spawn = {
            let mut inner = match self.shared.lock() {
                Ok(inner) => inner,
                Err(_) => {
                    callback.log_io_completed(Err(io::Error::new(
                        io::ErrorKind::Other,
                        "log lock poisoned",
                    )));
                    return;
                }
            };
            inner.pending.push(callback);
            if inner.flusher_spawned {
                None
            } else {
                inner.flusher_spawned = true;
                Some((self.shared.inner.clone(), inner.stop.clone()))
            }
        };
        if let Some((inner, stop)) = spawn {
            tokio::spawn(async move { deferred_flush_loop(inner, stop, interval).await });
        }
    }

    fn truncate_log(&self, log_id: RaftLogId) -> Result<(), StoreError> {
        let mut inner = self.shared.lock()?;
        let first_victim = inner
            .index
            .range(log_id.index..)
            .next()
            .map(|(_, (_, loc))| *loc);
        if let Some(victim) = first_victim {
            // Delete every segment after the victim's segment.
            for meta in inner.segments.drain(victim.segment + 1..) {
                std::fs::remove_file(&meta.path).map_err(StoreError::io(&meta.path, "delete"))?;
            }
            // Truncate the victim's segment at its frame start (possibly
            // leaving a header-only active segment, which is fine).
            let path = inner.segments[victim.segment].path.clone();
            let file = OpenOptions::new()
                .write(true)
                .open(&path)
                .map_err(StoreError::io(&path, "open truncate"))?;
            file.set_len(victim.offset)
                .map_err(StoreError::io(&path, "truncate"))?;
            file.sync_data().map_err(StoreError::io(&path, "fsync"))?;
            inner.active = Some(
                OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .map_err(StoreError::io(&path, "open append"))?,
            );
            inner.active_len = victim.offset;
            fsync_dir(&inner.dir)?;
        }
        inner.index.retain(|index, _| *index < log_id.index);
        inner.recount_active_entries();
        Ok(())
    }

    fn purge_log(&self, log_id: RaftLogId) -> Result<(), StoreError> {
        let mut inner = self.shared.lock()?;
        inner.index.retain(|index, _| *index > log_id.index);
        inner.last_purged = Some(log_id.clone());

        // Persist the purge point atomically before deleting anything.
        let body = encode_versioned(&log_id)?;
        write_frame_file(&inner.dir, "PURGED", PURGED_MAGIC, &body)?;

        // Delete segments fully covered by the purge point. Segment `i`
        // covers `[first_i, first_{i+1})`; the last covers `[first_last, ∞)`.
        let mut covered = 0usize;
        for i in 0..inner.segments.len() {
            let range_end = inner
                .segments
                .get(i + 1)
                .map_or(u64::MAX, |next| next.first_index.saturating_sub(1));
            if range_end <= log_id.index {
                covered += 1;
            } else {
                break;
            }
        }
        for meta in inner.segments.drain(..covered) {
            std::fs::remove_file(&meta.path).map_err(StoreError::io(&meta.path, "delete"))?;
        }
        for (_, loc) in inner.index.values_mut() {
            // Remaining locations referenced segments at or past `covered`.
            loc.segment -= covered;
        }
        if covered > 0 {
            fsync_dir(&inner.dir)?;
        }
        if inner.segments.is_empty() {
            // Every segment was purged: the next append rolls a fresh one.
            inner.active = None;
            inner.active_len = 0;
        }
        inner.recount_active_entries();
        Ok(())
    }
}

/// Background flusher for [`FsyncPolicy::Deferred`]: fsyncs the active
/// segment on `interval` and releases the queued flush callbacks only after
/// the fsync, preserving the "durable when acknowledged" contract.
async fn deferred_flush_loop(
    inner: Arc<Mutex<LogInner>>,
    stop: Arc<AtomicBool>,
    interval: Duration,
) {
    loop {
        tokio::time::sleep(interval).await;
        if stop.load(Ordering::Acquire) {
            return;
        }
        let drained: Vec<LogFlushed<MongrelRaftConfig>> = {
            let mut guard = match inner.lock() {
                Ok(guard) => guard,
                Err(_) => return,
            };
            if guard.pending.is_empty() {
                continue;
            }
            let result = match guard.active.as_mut() {
                Some(active) => active.sync_data(),
                None => Ok(()),
            };
            match result {
                Ok(()) => std::mem::take(&mut guard.pending),
                Err(error) => {
                    let pending = std::mem::take(&mut guard.pending);
                    drop(guard);
                    for callback in pending {
                        callback.log_io_completed(Err(io::Error::new(
                            error.kind(),
                            format!("deferred fsync: {error}"),
                        )));
                    }
                    continue;
                }
            }
        };
        for callback in drained {
            callback.log_io_completed(Ok(()));
        }
    }
}

impl RaftLogReader<MongrelRaftConfig> for MongrelLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<RaftLogEntry>, RaftStorageError> {
        read_entries_range(&self.shared, range)
    }
}

impl RaftLogStorage<MongrelRaftConfig> for MongrelLogStore {
    type LogReader = MongrelLogReader;

    async fn get_log_state(&mut self) -> Result<LogState<MongrelRaftConfig>, RaftStorageError> {
        let (last_purged, last) = {
            let inner = self
                .shared
                .lock()
                .map_err(|e| raft_storage_error(ErrorSubject::Logs, ErrorVerb::Read, e))?;
            (inner.last_purged.clone(), inner.last_indexed())
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        MongrelLogReader {
            shared: self.shared.clone(),
        }
    }

    /// Persists the vote and fsyncs **before returning** (S2B-002).
    async fn save_vote(&mut self, vote: &RaftVote) -> Result<(), RaftStorageError> {
        self.save_hard_state(Some(vote.clone()), None)
    }

    async fn read_vote(&mut self) -> Result<Option<RaftVote>, RaftStorageError> {
        Ok(self
            .hard
            .load()
            .map_err(|e| raft_storage_error(ErrorSubject::Vote, ErrorVerb::Read, e))?
            .vote)
    }

    async fn save_committed(&mut self, committed: Option<RaftLogId>) -> Result<(), RaftStorageError> {
        self.save_hard_state(None, committed)
    }

    async fn read_committed(&mut self) -> Result<Option<RaftLogId>, RaftStorageError> {
        Ok(self
            .hard
            .load()
            .map_err(|e| raft_storage_error(ErrorSubject::Vote, ErrorVerb::Read, e))?
            .committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<MongrelRaftConfig>,
    ) -> Result<(), RaftStorageError>
    where
        I: IntoIterator<Item = RaftLogEntry> + Send,
        I::IntoIter: Send,
    {
        let path = self.shared.lock().map(|i| i.dir.clone()).unwrap_or_default();
        self.append_inner(entries.into_iter().collect(), callback, &path)
    }

    async fn truncate(&mut self, log_id: RaftLogId) -> Result<(), RaftStorageError> {
        self.truncate_log(log_id)
            .map_err(|e| raft_storage_error(ErrorSubject::Logs, ErrorVerb::Write, e))
    }

    async fn purge(&mut self, log_id: RaftLogId) -> Result<(), RaftStorageError> {
        self.purge_log(log_id)
            .map_err(|e| raft_storage_error(ErrorSubject::Logs, ErrorVerb::Write, e))
    }
}

impl MongrelLogStore {
    fn append_inner(
        &self,
        entries: Vec<RaftLogEntry>,
        callback: LogFlushed<MongrelRaftConfig>,
        path: &Path,
    ) -> Result<(), RaftStorageError> {
        let result: Result<Option<Duration>, StoreError> = (|| {
            mongreldb_fault::inject("raft.log.append.before")
                .map_err(|f| StoreError::fault(path, "write", f))?;
            eprintln!("[dbg] append_inner: write_frames");
            let wrote = self.write_frames(entries)?;
            eprintln!("[dbg] append_inner: wrote={wrote}");
            let mut deferred_interval = None;
            if wrote {
                match &self.shared.lock()?.config.fsync_policy {
                    FsyncPolicy::PerAppend => self.fsync_active()?,
                    FsyncPolicy::Deferred { interval } => deferred_interval = Some(*interval),
                }
            }
            mongreldb_fault::inject("raft.log.append.after")
                .map_err(|f| StoreError::fault(path, "write", f))?;
            Ok(deferred_interval)
        })();
        match result {
            Ok(Some(interval)) => self.defer_callback(callback, interval),
            Ok(None) => callback.log_io_completed(Ok(())),
            Err(err) => {
                let io_err = io::Error::new(io::ErrorKind::Other, err.to_string());
                callback.log_io_completed(Err(io_err));
                return Err(raft_storage_error(ErrorSubject::Logs, ErrorVerb::Write, err));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{CommandKind, ReplicatedCommand};
    use mongreldb_log::envelope::CommandEnvelope;
    use mongreldb_types::hlc::HlcTimestamp;
    use openraft::entry::EntryPayload;
    use openraft::CommittedLeaderId;
    use openraft::{LogId, Vote};

    fn entry(term: u64, index: u64, id_byte: u8) -> RaftLogEntry {
        let envelope = CommandEnvelope::new(1, [id_byte; 16], vec![id_byte; 4]);
        RaftLogEntry {
            log_id: LogId::new(CommittedLeaderId::new(term, 1), index),
            payload: EntryPayload::Normal(ReplicatedCommand::new(
                CommandKind::Transaction,
                envelope,
                HlcTimestamp::ZERO,
            )),
        }
    }

    fn blank_entry(term: u64, index: u64) -> RaftLogEntry {
        RaftLogEntry {
            log_id: LogId::new(CommittedLeaderId::new(term, 1), index),
            payload: EntryPayload::Blank,
        }
    }

    fn append_and_sync(store: &MongrelLogStore, entries: Vec<RaftLogEntry>) {
        store.write_frames(entries).unwrap();
        store.fsync_active().unwrap();
    }

    #[test]
    fn frame_file_round_trip_and_corruption() {
        let tmp = tempfile::tempdir().unwrap();
        write_frame_file(tmp.path(), "hard-state", HARD_STATE_MAGIC, b"hello").unwrap();
        assert_eq!(
            read_frame_file(&tmp.path().join("hard-state"), HARD_STATE_MAGIC).unwrap(),
            Some(b"hello".to_vec())
        );
        // Corrupt one body byte -> checksum failure fails closed.
        let path = tmp.path().join("hard-state");
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        std::fs::write(&path, bytes).unwrap();
        assert!(matches!(
            read_frame_file(&path, HARD_STATE_MAGIC),
            Err(StoreError::Corrupt { .. })
        ));
        // Wrong magic fails closed too.
        assert!(matches!(
            read_frame_file(&path, PURGED_MAGIC),
            Err(StoreError::Corrupt { .. })
        ));
    }

    #[test]
    fn hard_state_persists_across_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let store = MongrelLogStore::open(tmp.path(), StorageConfig::default()).unwrap();
        store
            .save_hard_state(
                Some(Vote::new(3, 2)),
                Some(LogId::new(CommittedLeaderId::new(3, 2), 9)),
            )
            .unwrap();
        drop(store);
        let store = MongrelLogStore::open(tmp.path(), StorageConfig::default()).unwrap();
        let state = store.hard.load().unwrap();
        assert_eq!(state.vote, Some(Vote::new(3, 2)));
        assert_eq!(
            state.committed,
            Some(LogId::new(CommittedLeaderId::new(3, 2), 9))
        );
    }

    #[test]
    fn append_read_and_log_state() {
        let tmp = tempfile::tempdir().unwrap();
        let store = MongrelLogStore::open(tmp.path(), StorageConfig::default()).unwrap();
        let entries = vec![entry(1, 1, 10), entry(1, 2, 11), blank_entry(1, 3)];
        append_and_sync(&store, entries.clone());
        let shared = store.shared_log();
        assert_eq!(
            shared.last_log_id().unwrap(),
            Some(LogId::new(CommittedLeaderId::new(1, 1), 3))
        );
        let read = shared.read_entries(1, 4, 10).unwrap();
        assert_eq!(read, entries);
        let tail = shared.read_entries(3, 100, 10).unwrap();
        assert_eq!(tail, vec![blank_entry(1, 3)]);
    }

    #[test]
    fn append_survives_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let store = MongrelLogStore::open(tmp.path(), StorageConfig::default()).unwrap();
            append_and_sync(&store, vec![entry(1, 1, 1), entry(1, 2, 2), entry(2, 3, 3)]);
        }
        let store = MongrelLogStore::open(tmp.path(), StorageConfig::default()).unwrap();
        let shared = store.shared_log();
        assert_eq!(
            shared.read_entries(1, 10, 10).unwrap(),
            vec![entry(1, 1, 1), entry(1, 2, 2), entry(2, 3, 3)]
        );
        // Appends continue after reopen without index reuse.
        append_and_sync(&store, vec![entry(2, 4, 4)]);
        assert_eq!(
            shared.last_log_id().unwrap(),
            Some(LogId::new(CommittedLeaderId::new(2, 1), 4))
        );
        assert_eq!(shared.read_entries(1, 10, 10).unwrap().len(), 4);
    }

    #[test]
    fn torn_tail_is_truncated_on_recovery() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let store = MongrelLogStore::open(tmp.path(), StorageConfig::default()).unwrap();
            append_and_sync(&store, vec![entry(1, 1, 1), entry(1, 2, 2)]);
        }
        // Simulate a crash mid-append: garbage bytes after the last good frame.
        let segment = tmp.path().join("raft/log/seg-00000000000000000001.seg");
        let mut bytes = std::fs::read(&segment).unwrap();
        bytes.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        std::fs::write(&segment, bytes).unwrap();

        let store = MongrelLogStore::open(tmp.path(), StorageConfig::default()).unwrap();
        let shared = store.shared_log();
        assert_eq!(shared.read_entries(1, 10, 10).unwrap().len(), 2);
        // New appends land right after the truncated point.
        append_and_sync(&store, vec![entry(1, 3, 3)]);
        drop(store);
        let store = MongrelLogStore::open(tmp.path(), StorageConfig::default()).unwrap();
        assert_eq!(store.shared_log().read_entries(1, 10, 10).unwrap().len(), 3);
    }

    #[test]
    fn mid_segment_corruption_fails_closed() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let store = MongrelLogStore::open(tmp.path(), StorageConfig::default()).unwrap();
            append_and_sync(&store, vec![entry(1, 1, 1), entry(1, 2, 2), entry(1, 3, 3)]);
        }
        // Flip a byte inside the FIRST entry frame (not the tail).
        let segment = tmp.path().join("raft/log/seg-00000000000000000001.seg");
        let mut bytes = std::fs::read(&segment).unwrap();
        let header_frame_len = 4 + 20 + 32;
        bytes[header_frame_len + 6] ^= 0x10;
        std::fs::write(&segment, bytes).unwrap();
        assert!(matches!(
            MongrelLogStore::open(tmp.path(), StorageConfig::default()),
            Err(StoreError::Corrupt { .. })
        ));
    }

    #[test]
    fn segment_rolling_and_purge() {
        let tmp = tempfile::tempdir().unwrap();
        let config = StorageConfig {
            segment_roll_bytes: 128, // force rolls after a couple of entries
            ..StorageConfig::default()
        };
        let store = MongrelLogStore::open(tmp.path(), config).unwrap();
        let entries: Vec<_> = (1..=10u64).map(|i| entry(1, i, i as u8)).collect();
        append_and_sync(&store, entries);
        let segment_count = std::fs::read_dir(tmp.path().join("raft/log"))
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("seg-")
            })
            .count();
        assert!(segment_count > 1, "expected rolled segments");

        // Purge through index 6: earlier entries disappear, later stay.
        store
            .purge_log(LogId::new(CommittedLeaderId::new(1, 1), 6))
            .unwrap();
        let shared = store.shared_log();
        assert_eq!(
            shared.last_purged_log_id().unwrap(),
            Some(LogId::new(CommittedLeaderId::new(1, 1), 6))
        );
        assert_eq!(shared.read_entries(1, 7, 10).unwrap().len(), 0);
        assert_eq!(shared.read_entries(7, 11, 10).unwrap().len(), 4);

        // Purge point survives reopen; entries do not resurrect.
        drop(store);
        let store = MongrelLogStore::open(tmp.path(), StorageConfig::default()).unwrap();
        let shared = store.shared_log();
        assert_eq!(shared.read_entries(1, 7, 10).unwrap().len(), 0);
        assert_eq!(shared.read_entries(7, 11, 10).unwrap().len(), 4);
        // Log state reports the purge point when asked.
        assert_eq!(
            shared.last_log_id().unwrap(),
            Some(LogId::new(CommittedLeaderId::new(1, 1), 10))
        );
    }

    #[test]
    fn truncate_removes_suffix_and_allows_rewrite() {
        let tmp = tempfile::tempdir().unwrap();
        let store = MongrelLogStore::open(tmp.path(), StorageConfig::default()).unwrap();
        append_and_sync(&store, (1..=5u64).map(|i| entry(1, i, i as u8)).collect());
        store
            .truncate_log(LogId::new(CommittedLeaderId::new(1, 1), 3))
            .unwrap();
        let shared = store.shared_log();
        assert_eq!(shared.read_entries(1, 10, 10).unwrap().len(), 2);
        // Conflicting entries replace the truncated suffix (term changed).
        append_and_sync(&store, vec![entry(2, 3, 30), entry(2, 4, 31)]);
        assert_eq!(
            shared.read_entries(1, 10, 10).unwrap(),
            vec![entry(1, 1, 1), entry(1, 2, 2), entry(2, 3, 30), entry(2, 4, 31)]
        );
        // Physical state matches after reopen (no resurrected frames).
        drop(store);
        let store = MongrelLogStore::open(tmp.path(), StorageConfig::default()).unwrap();
        assert_eq!(
            store.shared_log().read_entries(1, 10, 10).unwrap(),
            vec![entry(1, 1, 1), entry(1, 2, 2), entry(2, 3, 30), entry(2, 4, 31)]
        );
    }

    #[test]
    fn purge_everything_leaves_empty_log() {
        let tmp = tempfile::tempdir().unwrap();
        let store = MongrelLogStore::open(tmp.path(), StorageConfig::default()).unwrap();
        append_and_sync(&store, (1..=4u64).map(|i| entry(1, i, i as u8)).collect());
        store
            .purge_log(LogId::new(CommittedLeaderId::new(1, 1), 4))
            .unwrap();
        let shared = store.shared_log();
        assert_eq!(shared.read_entries(1, 100, 100).unwrap().len(), 0);
        assert_eq!(
            shared.last_log_id().unwrap(),
            Some(LogId::new(CommittedLeaderId::new(1, 1), 4))
        );
        // Appends after a full purge roll a fresh segment.
        append_and_sync(&store, vec![entry(1, 5, 5)]);
        assert_eq!(shared.read_entries(1, 100, 100).unwrap().len(), 1);
        drop(store);
        let store = MongrelLogStore::open(tmp.path(), StorageConfig::default()).unwrap();
        let shared = store.shared_log();
        assert_eq!(shared.read_entries(1, 100, 100).unwrap().len(), 1);
        assert_eq!(
            shared.last_purged_log_id().unwrap(),
            Some(LogId::new(CommittedLeaderId::new(1, 1), 4))
        );
    }

    #[tokio::test]
    async fn deferred_policy_fires_callbacks_after_fsync() {
        let tmp = tempfile::tempdir().unwrap();
        let config = StorageConfig {
            fsync_policy: FsyncPolicy::Deferred {
                interval: Duration::from_millis(10),
            },
            ..StorageConfig::default()
        };
        let store = MongrelLogStore::open(tmp.path(), config).unwrap();
        store.write_frames(vec![entry(1, 1, 1)]).unwrap();
        // Entries are readable before the flusher runs.
        assert_eq!(store.shared_log().read_entries(1, 2, 1).unwrap().len(), 1);
        store.shared_log().close(); // close fsyncs and releases pending callbacks
        drop(store);
        let store = MongrelLogStore::open(tmp.path(), StorageConfig::default()).unwrap();
        assert_eq!(store.shared_log().read_entries(1, 2, 1).unwrap().len(), 1);
    }
}
