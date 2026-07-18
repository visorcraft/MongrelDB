//! Spill manager for query execution (spec section 10.5, S1E-004).
//!
//! Implemented in the Stage 1E wave: when the node
//! [`MemoryGovernor`](crate::memory::MemoryGovernor) enters escalation step 3,
//! spill-eligible operators move working memory to disk through this manager.
//! Per the spec, spill files are:
//!
//! - **Query-ID namespaced** — every [`QueryId`] spills under its own
//!   subdirectory `temp/spill/q-<hex>/` of the database root, so one query's
//!   files never interleave with another's and cancellation removes exactly
//!   one directory.
//! - **Checksummed** — every frame carries a CRC32C (the same Castagnoli CRC
//!   as the WAL) over its kind, sequence, and stored payload; the sealing
//!   trailer carries a SHA-256 over all plaintext data payloads plus the
//!   frame count, verified on read.
//! - **Bounded** — a per-query cap (fed from
//!   [`ResourceGroup::temporary_disk_bytes`]) and a node-global cap, enforced
//!   with the memory governor's add-then-validate-rollback protocol; overflow
//!   is the typed [`SpillError::BudgetExceeded`].
//! - **Deleted on success/error/cancel/startup cleanup** — the [`SpillHandle`]
//!   RAII guard deletes its file on drop, an unfinished [`SpillWriter`]
//!   deletes its partial file on drop (the error/cancel path), a dropped
//!   [`SpillSession`] removes the whole per-query directory, and
//!   [`SpillManager::open`] sweeps every stale entry left by a prior process
//!   run (spill files never outlive the process that created them).
//! - **Encrypted when database encryption is enabled** — with a meta DEK
//!   present ([`crate::encryption::meta_dek_for`]) every frame payload is
//!   sealed through the page-cipher stack (`encrypt_blob`: AES-256-GCM with a
//!   fresh random nonce per frame), mirroring the catalog/PITR seal idiom;
//!   otherwise frames are stored plaintext.
//!
//! Layout of one spill file:
//!
//! ```text
//! [magic: 8B "MDBSPILL"][version: u16][enc flag: u8][reserved: u8]
//! frame*: [payload len: u32][crc32c: u32][kind: u8][seq: u64][payload]
//! trailer frame (kind = 1): [data frames: u64][data bytes: u64][sha256: 32B]
//! ```
//!
//! The on-disk tree lives under a descriptor-pinned
//! [`DurableRoot`](crate::durable_file::DurableRoot); every file operation is
//! descriptor-relative and `temp/spill` itself is created lazily on first
//! use. Open one `SpillManager` per database: opening a second manager on the
//! same root sweeps the first manager's files, exactly like a restart.

use std::fmt;
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crc::{Crc, CRC_32_ISCSI};
use mongreldb_types::ids::QueryId;
use sha2::{Digest, Sha256};

use crate::durable_file::DurableRoot;
use crate::encryption::DEK_LEN;
use crate::resource::ResourceGroup;
use crate::MongrelError;

/// Spill tree location relative to the database root.
const SPILL_DIR_REL: &str = "temp/spill";
/// File magic (8 bytes, the [`MongrelError::MagicMismatch`] idiom).
const SPILL_MAGIC: &[u8; 8] = b"MDBSPILL";
/// On-disk format version this build reads and writes.
const FORMAT_VERSION: u16 = 1;
/// Encryption flag stored in the header: frames are plaintext.
const ENC_PLAINTEXT: u8 = 0;
/// Encryption flag stored in the header: frames are AES-256-GCM sealed.
const ENC_AES_GCM: u8 = 1;
/// Header length: magic + version + enc flag + reserved.
const HEADER_LEN: usize = 8 + 2 + 1 + 1;
/// Frame head length: payload len + crc + kind + seq.
const FRAME_HEAD_LEN: usize = 4 + 4 + 1 + 8;
/// Kind byte of a data frame.
const FRAME_DATA: u8 = 0;
/// Kind byte of the sealing trailer frame.
const FRAME_TRAILER: u8 = 1;
/// Trailer payload length: frame count + byte count + SHA-256.
const TRAILER_LEN: usize = 8 + 8 + 32;
/// Maximum bytes of one frame's plaintext payload (bounds reader allocation
/// on corrupt input, mirroring the PITR chunk cap).
const MAX_FRAME_PAYLOAD: u64 = 64 * 1024 * 1024;

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

/// Errors of spill configuration, I/O, budget enforcement, and verification.
#[derive(Debug, thiserror::Error)]
pub enum SpillError {
    /// Filesystem failure on the spill tree.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// The [`SpillConfig`] failed validation.
    #[error("invalid spill configuration: {0}")]
    InvalidConfig(&'static str),
    /// A frame append would exceed the per-query or the node-global spill
    /// budget (S1E-004 "bounded").
    #[error(
        "spill budget exceeded for query {query_id}: requested {requested} bytes \
         ({query_remaining} per-query, {global_remaining} global remaining)"
    )]
    BudgetExceeded {
        /// The query that hit its bound.
        query_id: QueryId,
        /// Bytes the frame needed.
        requested: u64,
        /// Per-query budget remaining before the attempt.
        query_remaining: u64,
        /// Node-global budget remaining before the attempt.
        global_remaining: u64,
    },
    /// One frame's plaintext payload exceeded [`MAX_FRAME_PAYLOAD`].
    #[error("spill frame of {bytes} bytes exceeds the {limit}-byte frame limit")]
    FrameTooLarge {
        /// Attempted payload size.
        bytes: u64,
        /// Configured bound.
        limit: u64,
    },
    /// A frame's stored CRC32C did not match its bytes.
    #[error("checksum mismatch for {context}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        /// Which frame/file failed verification.
        context: String,
        /// Stored checksum.
        expected: u32,
        /// Computed checksum.
        actual: u32,
    },
    /// Structural corruption: bad magic, version, framing, sequence gap, or
    /// a trailer that does not match the streamed frames. Always fail-closed.
    #[error("corrupt spill file: {0}")]
    Corrupt(String),
    /// An encrypted spill file was opened by a manager without a meta DEK.
    #[error("encrypted spill file requires the database encryption key")]
    EncryptionRequired,
    /// A meta DEK was configured (encrypted path).
    #[error("spill encryption requires the `encryption` feature")]
    EncryptionDisabled,
    /// Sealing a frame failed.
    #[error("spill encryption error: {0}")]
    Encryption(String),
    /// Opening a sealed frame failed (wrong key or tampering).
    #[error("spill decryption error: {0}")]
    Decryption(String),
}

impl From<SpillError> for MongrelError {
    fn from(error: SpillError) -> Self {
        match error {
            SpillError::Io(error) => MongrelError::Io(error),
            SpillError::InvalidConfig(message) => MongrelError::InvalidArgument(message.into()),
            SpillError::BudgetExceeded {
                requested,
                query_remaining,
                global_remaining,
                ..
            } => MongrelError::ResourceLimitExceeded {
                resource: "spill temporary disk",
                requested: usize::try_from(requested).unwrap_or(usize::MAX),
                limit: usize::try_from(
                    requested.saturating_add(query_remaining.min(global_remaining)),
                )
                .unwrap_or(usize::MAX),
            },
            SpillError::FrameTooLarge { bytes, limit } => MongrelError::ResourceLimitExceeded {
                resource: "spill frame",
                requested: usize::try_from(bytes).unwrap_or(usize::MAX),
                limit: usize::try_from(limit).unwrap_or(usize::MAX),
            },
            SpillError::ChecksumMismatch {
                context,
                expected,
                actual,
            } => MongrelError::ChecksumMismatch {
                expected: u64::from(expected),
                actual: u64::from(actual),
                context,
            },
            SpillError::Corrupt(message) => {
                MongrelError::Other(format!("corrupt spill file: {message}"))
            }
            SpillError::EncryptionRequired | SpillError::EncryptionDisabled => {
                MongrelError::EncryptionDisabled
            }
            SpillError::Encryption(message) => MongrelError::Encryption(message),
            SpillError::Decryption(message) => MongrelError::Decryption(message),
        }
    }
}

/// Node-global spill configuration (S1E-004). The per-query bound is supplied
/// per session from [`ResourceGroup::temporary_disk_bytes`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpillConfig {
    /// Total bytes of live spill files across every query on this node.
    pub global_bytes: u64,
}

impl SpillConfig {
    /// A config with the given node-global cap.
    pub fn new(global_bytes: u64) -> Self {
        Self { global_bytes }
    }

    fn validate(&self) -> Result<(), SpillError> {
        if self.global_bytes == 0 {
            return Err(SpillError::InvalidConfig(
                "global spill budget must be nonzero",
            ));
        }
        Ok(())
    }
}

/// A point-in-time snapshot of spill-manager state (telemetry and tests).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillStats {
    /// Cumulative stored bytes written (headers, frames, trailers).
    pub bytes_written: u64,
    /// Cumulative stored bytes read back.
    pub bytes_read: u64,
    /// Finished spill files currently live (held by a [`SpillHandle`]).
    pub files_live: u64,
    /// Live bytes currently charged against the global budget.
    pub global_used: u64,
    /// Configured node-global budget.
    pub global_budget_bytes: u64,
    /// Global budget remaining (`global_budget_bytes - global_used`).
    pub budget_remaining: u64,
}

struct ManagerInner {
    db_root: DurableRoot,
    /// Lazily created pinned `temp/spill` root (`None` until the first spill
    /// file is created; the startup sweep never creates it).
    spill_root: parking_lot::Mutex<Option<DurableRoot>>,
    config: SpillConfig,
    meta_dek: Option<[u8; DEK_LEN]>,
    global_used: AtomicU64,
    bytes_written: AtomicU64,
    bytes_read: AtomicU64,
    files_live: AtomicU64,
}

impl ManagerInner {
    /// The pinned `temp/spill` root, creating it (and `temp/`) on first use.
    fn spill_root(&self) -> Result<DurableRoot, SpillError> {
        let mut guard = self.spill_root.lock();
        if let Some(root) = guard.as_ref() {
            return Ok(root.try_clone()?);
        }
        let root = self.db_root.create_directory_all_pinned(SPILL_DIR_REL)?;
        *guard = Some(root.try_clone()?);
        Ok(root)
    }

    /// The pinned `temp/spill` root only if it already exists (never
    /// creates). Used by cleanup paths, which must not resurrect the tree.
    fn spill_root_if_created(&self) -> Option<DurableRoot> {
        self.spill_root
            .lock()
            .as_ref()
            .and_then(|root| root.try_clone().ok())
    }

    /// Removes every stale entry a prior process run left in `temp/spill`.
    /// Spill files are process-local by construction (live files are held by
    /// handles), so everything present at open is garbage.
    fn sweep_stale(&self) -> Result<(), SpillError> {
        match self.db_root.entry_exists(SPILL_DIR_REL) {
            Ok(true) => {}
            // No `temp/spill` (or no `temp` yet): nothing to sweep. The tree
            // is created lazily on the first spill.
            Ok(false) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(SpillError::Io(error)),
        }
        let root = self.db_root.open_directory(SPILL_DIR_REL)?;
        // `io_path` is the descriptor-pinned operational path of `root`, so
        // the enumeration itself cannot wander off the pinned directory.
        for entry in std::fs::read_dir(root.io_path()?)? {
            let entry = entry?;
            let name = entry.file_name();
            if entry.file_type()?.is_dir() {
                root.remove_directory_all(Path::new(&name))?;
            } else {
                root.remove_file(Path::new(&name))?;
            }
        }
        Ok(())
    }

    fn header(&self) -> [u8; HEADER_LEN] {
        let mut header = [0u8; HEADER_LEN];
        header[..8].copy_from_slice(SPILL_MAGIC);
        header[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        header[10] = if self.meta_dek.is_some() {
            ENC_AES_GCM
        } else {
            ENC_PLAINTEXT
        };
        header
    }

    /// Charges `bytes` against the per-query and global budgets with the
    /// governor's add-then-validate-rollback protocol, so a granted set never
    /// exceeds either bound.
    fn try_charge(&self, session: &SessionInner, bytes: u64) -> Result<(), SpillError> {
        let new_query = session.used.fetch_add(bytes, Ordering::Relaxed) + bytes;
        let new_global = self.global_used.fetch_add(bytes, Ordering::Relaxed) + bytes;
        if new_query <= session.cap && new_global <= self.config.global_bytes {
            Ok(())
        } else {
            session.used.fetch_sub(bytes, Ordering::Relaxed);
            self.global_used.fetch_sub(bytes, Ordering::Relaxed);
            Err(SpillError::BudgetExceeded {
                query_id: session.query_id,
                requested: bytes,
                query_remaining: session.cap.saturating_sub(new_query - bytes),
                global_remaining: self.config.global_bytes.saturating_sub(new_global - bytes),
            })
        }
    }

    /// Releases a previous charge (exact inverse of [`try_charge`](Self::try_charge)).
    fn release(&self, session: &SessionInner, bytes: u64) {
        session.used.fetch_sub(bytes, Ordering::Relaxed);
        self.global_used.fetch_sub(bytes, Ordering::Relaxed);
    }
}

/// The node-level spill manager (S1E-004). Cheap to clone (one `Arc`);
/// thread-safe; budget accounting is lock-free.
pub struct SpillManager {
    inner: Arc<ManagerInner>,
}

impl Clone for SpillManager {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl fmt::Debug for SpillManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpillManager")
            .field("global_budget_bytes", &self.inner.config.global_bytes)
            .field(
                "global_used",
                &self.inner.global_used.load(Ordering::Relaxed),
            )
            .field("files_live", &self.inner.files_live.load(Ordering::Relaxed))
            .field("encrypted", &self.inner.meta_dek.is_some())
            .finish()
    }
}

impl SpillManager {
    /// Opens the spill manager on a database root, sweeping every stale entry
    /// a prior process run left in `temp/spill` (S1E-004 startup cleanup).
    /// The `temp/spill` tree itself is created lazily on the first spill.
    ///
    /// `meta_dek` is the database meta DEK
    /// ([`crate::encryption::meta_dek_for`]): `Some` seals every frame with
    /// AES-256-GCM, `None` stores frames plaintext. Without the `encryption`
    /// feature a `Some` DEK is rejected (fail closed).
    pub fn open(
        db_root: &DurableRoot,
        config: SpillConfig,
        meta_dek: Option<[u8; DEK_LEN]>,
    ) -> Result<Self, SpillError> {
        config.validate()?;
        let manager = Self {
            inner: Arc::new(ManagerInner {
                db_root: db_root.try_clone()?,
                spill_root: parking_lot::Mutex::new(None),
                config,
                meta_dek,
                global_used: AtomicU64::new(0),
                bytes_written: AtomicU64::new(0),
                bytes_read: AtomicU64::new(0),
                files_live: AtomicU64::new(0),
            }),
        };
        manager.inner.sweep_stale()?;
        Ok(manager)
    }

    /// The manager's configuration.
    pub fn config(&self) -> &SpillConfig {
        &self.inner.config
    }

    /// Starts a spill session for one query with an explicit per-query cap.
    /// One session per `query_id` at a time: sessions share the per-query
    /// directory name, and chunk creation is exclusive.
    pub fn begin_query(
        &self,
        query_id: QueryId,
        per_query_bytes: u64,
    ) -> Result<SpillSession, SpillError> {
        Ok(SpillSession {
            inner: Arc::new(SessionInner {
                manager: self.clone(),
                query_id,
                dir_name: format!("q-{}", query_id.to_hex()),
                cap: per_query_bytes,
                used: AtomicU64::new(0),
                next_chunk: AtomicU64::new(0),
            }),
        })
    }

    /// Starts a spill session for one query admitted into `group`, with the
    /// per-query cap fed from [`ResourceGroup::temporary_disk_bytes`]
    /// (S1E-002/S1E-004).
    pub fn begin_query_in_group(
        &self,
        query_id: QueryId,
        group: &ResourceGroup,
    ) -> Result<SpillSession, SpillError> {
        self.begin_query(query_id, group.temporary_disk_bytes)
    }

    /// A point-in-time snapshot of spill state.
    pub fn stats(&self) -> SpillStats {
        let global_used = self.inner.global_used.load(Ordering::Relaxed);
        SpillStats {
            bytes_written: self.inner.bytes_written.load(Ordering::Relaxed),
            bytes_read: self.inner.bytes_read.load(Ordering::Relaxed),
            files_live: self.inner.files_live.load(Ordering::Relaxed),
            global_used,
            global_budget_bytes: self.inner.config.global_bytes,
            budget_remaining: self.inner.config.global_bytes.saturating_sub(global_used),
        }
    }
}

struct SessionInner {
    manager: SpillManager,
    query_id: QueryId,
    /// Per-query directory name under `temp/spill` (`q-<hex>`).
    dir_name: String,
    cap: u64,
    used: AtomicU64,
    next_chunk: AtomicU64,
}

impl SessionInner {
    /// The pinned per-query directory, creating it on first use.
    fn query_dir(&self) -> Result<DurableRoot, SpillError> {
        Ok(self
            .manager
            .inner
            .spill_root()?
            .create_directory_all_pinned(&self.dir_name)?)
    }
}

impl Drop for SessionInner {
    /// Removes the whole per-query directory (S1E-004 cleanup on
    /// success/error/cancel). Never creates the tree to delete it.
    fn drop(&mut self) {
        if let Some(root) = self.manager.inner.spill_root_if_created() {
            let _ = root.remove_directory_all(Path::new(&self.dir_name));
        }
    }
}

/// A query's spill session: namespaced, budgeted factory of spill files.
/// Dropping the session removes the whole per-query directory.
pub struct SpillSession {
    inner: Arc<SessionInner>,
}

impl fmt::Debug for SpillSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpillSession")
            .field("query_id", &self.inner.query_id)
            .field("cap", &self.inner.cap)
            .field("used", &self.inner.used.load(Ordering::Relaxed))
            .finish()
    }
}

impl SpillSession {
    /// The query this session spills for.
    pub fn query_id(&self) -> QueryId {
        self.inner.query_id
    }

    /// The per-query spill cap in bytes.
    pub fn cap(&self) -> u64 {
        self.inner.cap
    }

    /// Live bytes currently charged to this query.
    pub fn used(&self) -> u64 {
        self.inner.used.load(Ordering::Relaxed)
    }

    /// Per-query budget remaining.
    pub fn budget_remaining(&self) -> u64 {
        self.inner.cap.saturating_sub(self.used())
    }

    /// Creates the next spill file of this query and returns its writer.
    /// Dropping the writer before [`SpillWriter::finish`] deletes the partial
    /// file (the error/cancel path).
    pub fn new_writer(&self) -> Result<SpillWriter, SpillError> {
        let seq = self.inner.next_chunk.fetch_add(1, Ordering::Relaxed);
        let name = format!("chunk-{seq:06}.spill");
        let dir = self.inner.query_dir()?;
        let manager = &self.inner.manager;
        manager.inner.try_charge(&self.inner, HEADER_LEN as u64)?;
        let result = (|| {
            let mut file = dir.create_regular_new(&name)?;
            file.write_all(&manager.inner.header())?;
            io::Result::Ok(file)
        })();
        match result {
            Ok(file) => {
                manager
                    .inner
                    .bytes_written
                    .fetch_add(HEADER_LEN as u64, Ordering::Relaxed);
                Ok(SpillWriter {
                    inner: Some(WriterInner {
                        session: Arc::clone(&self.inner),
                        dir,
                        name,
                        file,
                        bytes_on_disk: HEADER_LEN as u64,
                        data_frames: 0,
                        data_bytes: 0,
                        digest: Sha256::new(),
                        next_seq: 0,
                    }),
                })
            }
            Err(error) => {
                manager.inner.release(&self.inner, HEADER_LEN as u64);
                Err(SpillError::Io(error))
            }
        }
    }
}

struct WriterInner {
    session: Arc<SessionInner>,
    /// Pinned per-query directory (descriptor-relative delete on abort).
    dir: DurableRoot,
    name: String,
    file: std::fs::File,
    bytes_on_disk: u64,
    data_frames: u64,
    data_bytes: u64,
    digest: Sha256,
    next_seq: u64,
}

impl WriterInner {
    /// Seals (when encrypted), frames, charges, and appends one frame.
    fn write_frame(&mut self, kind: u8, payload: &[u8]) -> Result<(), SpillError> {
        if payload.len() as u64 > MAX_FRAME_PAYLOAD {
            return Err(SpillError::FrameTooLarge {
                bytes: payload.len() as u64,
                limit: MAX_FRAME_PAYLOAD,
            });
        }
        let stored = seal_payload(self.session.manager.inner.meta_dek.as_ref(), payload)?;
        let seq = self.next_seq;
        let mut digest = CRC32C.digest();
        digest.update(&[kind]);
        digest.update(&seq.to_le_bytes());
        digest.update(&stored);
        let crc = digest.finalize();
        let frame_bytes = FRAME_HEAD_LEN as u64 + stored.len() as u64;
        let manager = &self.session.manager;
        manager.inner.try_charge(&self.session, frame_bytes)?;
        let result = (|| {
            self.file.write_all(&(stored.len() as u32).to_le_bytes())?;
            self.file.write_all(&crc.to_le_bytes())?;
            self.file.write_all(&[kind])?;
            self.file.write_all(&seq.to_le_bytes())?;
            self.file.write_all(&stored)
        })();
        match result {
            Ok(()) => {
                self.next_seq += 1;
                self.bytes_on_disk += frame_bytes;
                manager
                    .inner
                    .bytes_written
                    .fetch_add(frame_bytes, Ordering::Relaxed);
                Ok(())
            }
            Err(error) => {
                manager.inner.release(&self.session, frame_bytes);
                Err(SpillError::Io(error))
            }
        }
    }

    /// Writes the sealing trailer (frame count, byte count, SHA-256 over all
    /// plaintext data payloads), then fsyncs file and directory.
    fn write_trailer_and_sync(&mut self) -> Result<(), SpillError> {
        let hash: [u8; 32] = self.digest.clone().finalize().into();
        let mut trailer = Vec::with_capacity(TRAILER_LEN);
        trailer.extend_from_slice(&self.data_frames.to_le_bytes());
        trailer.extend_from_slice(&self.data_bytes.to_le_bytes());
        trailer.extend_from_slice(&hash);
        self.write_frame(FRAME_TRAILER, &trailer)?;
        self.file.sync_all()?;
        self.dir.sync_entry_parent(Path::new(&self.name))?;
        Ok(())
    }
}

/// Deletes the partial file of an unfinished writer and releases its budget
/// (the error/cancel path; S1E-004 "deleted on error/cancel").
fn abort_writer(inner: WriterInner) {
    let WriterInner {
        session,
        dir,
        name,
        file,
        bytes_on_disk,
        ..
    } = inner;
    drop(file);
    let _ = dir.remove_file(Path::new(&name));
    session.manager.inner.release(&session, bytes_on_disk);
}

/// Streaming writer of one spill file. Append data frames with
/// [`append`](Self::append); seal and fsync with [`finish`](Self::finish).
/// Dropping an unfinished writer deletes the partial file.
pub struct SpillWriter {
    inner: Option<WriterInner>,
}

impl fmt::Debug for SpillWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpillWriter")
            .field("name", &self.inner.as_ref().map(|inner| &inner.name))
            .field(
                "bytes_on_disk",
                &self.inner.as_ref().map(|inner| inner.bytes_on_disk),
            )
            .finish()
    }
}

impl SpillWriter {
    /// Appends one data frame. `payload` is opaque to the manager (an
    /// operator's serialized run); it is checksummed and, when the database
    /// is encrypted, sealed before it touches disk.
    pub fn append(&mut self, payload: &[u8]) -> Result<(), SpillError> {
        let inner = self
            .inner
            .as_mut()
            .expect("spill writer is live until finish");
        inner.write_frame(FRAME_DATA, payload)?;
        inner.digest.update(payload);
        inner.data_frames += 1;
        inner.data_bytes += payload.len() as u64;
        Ok(())
    }

    /// Stored bytes written so far (header and frames, post-seal).
    pub fn bytes_on_disk(&self) -> u64 {
        self.inner.as_ref().map_or(0, |inner| inner.bytes_on_disk)
    }

    /// Seals the file with its checksum trailer, fsyncs it, and returns the
    /// RAII handle. On error the partial file is deleted, exactly as if the
    /// writer had been dropped.
    pub fn finish(mut self) -> Result<SpillHandle, SpillError> {
        let mut inner = self
            .inner
            .take()
            .expect("spill writer is live until finish");
        if let Err(error) = inner.write_trailer_and_sync() {
            abort_writer(inner);
            return Err(error);
        }
        inner
            .session
            .manager
            .inner
            .files_live
            .fetch_add(1, Ordering::Relaxed);
        let WriterInner {
            session,
            dir,
            name,
            bytes_on_disk,
            data_frames,
            ..
        } = inner;
        Ok(SpillHandle {
            inner: Some(HandleInner {
                session,
                dir,
                name,
                bytes_on_disk,
                data_frames,
            }),
        })
    }

    /// Abandons the file: deletes the partial write and releases its budget.
    /// Equivalent to dropping the writer, but reports I/O failures.
    pub fn abort(mut self) -> Result<(), SpillError> {
        let inner = self
            .inner
            .take()
            .expect("spill writer is live until finish");
        let WriterInner {
            session,
            dir,
            name,
            file,
            bytes_on_disk,
            ..
        } = inner;
        drop(file);
        dir.remove_file(Path::new(&name))?;
        session.manager.inner.release(&session, bytes_on_disk);
        Ok(())
    }
}

impl Drop for SpillWriter {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            abort_writer(inner);
        }
    }
}

struct HandleInner {
    session: Arc<SessionInner>,
    dir: DurableRoot,
    name: String,
    bytes_on_disk: u64,
    data_frames: u64,
}

/// Deletes the finished file and releases its budget and live-file count.
fn delete_file(inner: HandleInner) -> Result<(), SpillError> {
    inner.dir.remove_file(Path::new(&inner.name))?;
    inner
        .session
        .manager
        .inner
        .release(&inner.session, inner.bytes_on_disk);
    inner
        .session
        .manager
        .inner
        .files_live
        .fetch_sub(1, Ordering::Relaxed);
    Ok(())
}

/// RAII guard for one sealed spill file: dropping it deletes the file and
/// releases its budget (S1E-004 "deleted on success"). Open a verify-on-read
/// [`SpillReader`] with [`reader`](Self::reader).
#[must_use = "a spill handle deletes its file on drop"]
pub struct SpillHandle {
    inner: Option<HandleInner>,
}

impl fmt::Debug for SpillHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpillHandle")
            .field("name", &self.inner.as_ref().map(|inner| &inner.name))
            .field(
                "bytes_on_disk",
                &self.inner.as_ref().map(|inner| inner.bytes_on_disk),
            )
            .finish()
    }
}

impl SpillHandle {
    /// The query whose session created this file.
    pub fn query_id(&self) -> QueryId {
        self.inner
            .as_ref()
            .expect("spill handle is live")
            .session
            .query_id
    }

    /// Stored file size in bytes (header, frames, trailer).
    pub fn bytes_on_disk(&self) -> u64 {
        self.inner.as_ref().map_or(0, |inner| inner.bytes_on_disk)
    }

    /// Number of data frames sealed into the file.
    pub fn frames(&self) -> u64 {
        self.inner.as_ref().map_or(0, |inner| inner.data_frames)
    }

    /// Opens a streaming, verify-on-read reader over the file.
    pub fn reader(&self) -> Result<SpillReader, SpillError> {
        let inner = self.inner.as_ref().expect("spill handle is live");
        let file = inner.dir.open_regular(Path::new(&inner.name))?;
        reader_from(file, &inner.session.manager)
    }

    /// Deletes the file now, releasing its budget. Equivalent to dropping the
    /// handle, but reports I/O failures.
    pub fn delete(mut self) -> Result<(), SpillError> {
        let inner = self.inner.take().expect("spill handle is live");
        delete_file(inner)
    }
}

impl Drop for SpillHandle {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            let _ = delete_file(inner);
        }
    }
}

/// Parses and validates a spill file header and builds the reader (shared by
/// [`SpillHandle::reader`] and tests). Fails closed on bad magic, an
/// unsupported version, an unknown encryption flag, or a DEK mismatch with
/// the manager (mirroring the PITR chunk envelope rules).
fn reader_from(mut file: std::fs::File, manager: &SpillManager) -> Result<SpillReader, SpillError> {
    let mut header = [0u8; HEADER_LEN];
    file.read_exact(&mut header).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            SpillError::Corrupt("spill file is shorter than its header".into())
        } else {
            SpillError::Io(error)
        }
    })?;
    manager
        .inner
        .bytes_read
        .fetch_add(HEADER_LEN as u64, Ordering::Relaxed);
    if header[..8] != SPILL_MAGIC[..] {
        return Err(SpillError::Corrupt("bad spill magic".into()));
    }
    let version = u16::from_le_bytes(header[8..10].try_into().expect("slice length"));
    if version != FORMAT_VERSION {
        return Err(SpillError::Corrupt(format!(
            "unsupported spill format version {version}"
        )));
    }
    let dek = match (header[10], manager.inner.meta_dek.as_ref()) {
        (ENC_PLAINTEXT, None) => None,
        (ENC_AES_GCM, Some(dek)) => Some(*dek),
        (ENC_AES_GCM, None) => return Err(SpillError::EncryptionRequired),
        (ENC_PLAINTEXT, Some(_)) => {
            return Err(SpillError::Corrupt(
                "plaintext spill file opened with an encryption key".into(),
            ))
        }
        (other, _) => {
            return Err(SpillError::Corrupt(format!(
                "unknown spill encryption flag {other}"
            )))
        }
    };
    Ok(SpillReader {
        file,
        manager: manager.clone(),
        dek,
        next_seq: 0,
        data_frames: 0,
        data_bytes: 0,
        digest: Sha256::new(),
        done: false,
    })
}

/// Streaming, verify-on-read reader of one sealed spill file. Every frame's
/// CRC32C is verified before its payload is returned (and decrypted, when
/// the file is sealed); the closing trailer re-checks the frame count, byte
/// count, and the SHA-256 over all data payloads. Any failure is terminal.
pub struct SpillReader {
    file: std::fs::File,
    manager: SpillManager,
    dek: Option<[u8; DEK_LEN]>,
    next_seq: u64,
    data_frames: u64,
    data_bytes: u64,
    digest: Sha256,
    done: bool,
}

impl fmt::Debug for SpillReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpillReader")
            .field("next_seq", &self.next_seq)
            .field("data_frames", &self.data_frames)
            .field("done", &self.done)
            .finish()
    }
}

impl SpillReader {
    /// Reads the next data frame, or `None` once the sealing trailer has been
    /// verified. Errors are terminal: after one, the reader yields no more
    /// frames.
    pub fn next_frame(&mut self) -> Result<Option<Vec<u8>>, SpillError> {
        if self.done {
            return Ok(None);
        }
        match self.next_frame_inner() {
            Ok(frame) => Ok(frame),
            Err(error) => {
                self.done = true;
                Err(error)
            }
        }
    }

    fn next_frame_inner(&mut self) -> Result<Option<Vec<u8>>, SpillError> {
        let mut head = [0u8; FRAME_HEAD_LEN];
        self.file.read_exact(&mut head).map_err(|error| {
            if error.kind() == io::ErrorKind::UnexpectedEof {
                SpillError::Corrupt("truncated spill file: missing trailer".into())
            } else {
                SpillError::Io(error)
            }
        })?;
        let len = u64::from(u32::from_le_bytes(
            head[0..4].try_into().expect("slice length"),
        ));
        let expected_crc = u32::from_le_bytes(head[4..8].try_into().expect("slice length"));
        let kind = head[8];
        let seq = u64::from_le_bytes(head[9..17].try_into().expect("slice length"));
        if len > MAX_FRAME_PAYLOAD {
            return Err(SpillError::Corrupt(format!(
                "spill frame of {len} bytes exceeds the {MAX_FRAME_PAYLOAD}-byte limit"
            )));
        }
        if seq != self.next_seq {
            return Err(SpillError::Corrupt(format!(
                "spill frame sequence gap: expected {}, found {seq}",
                self.next_seq
            )));
        }
        let mut stored = vec![0u8; len as usize];
        self.file.read_exact(&mut stored).map_err(|error| {
            if error.kind() == io::ErrorKind::UnexpectedEof {
                SpillError::Corrupt("truncated spill frame payload".into())
            } else {
                SpillError::Io(error)
            }
        })?;
        let mut digest = CRC32C.digest();
        digest.update(&[kind]);
        digest.update(&seq.to_le_bytes());
        digest.update(&stored);
        let actual_crc = digest.finalize();
        if actual_crc != expected_crc {
            return Err(SpillError::ChecksumMismatch {
                context: format!("spill frame {seq}"),
                expected: expected_crc,
                actual: actual_crc,
            });
        }
        self.manager
            .inner
            .bytes_read
            .fetch_add(FRAME_HEAD_LEN as u64 + len, Ordering::Relaxed);
        let plaintext = open_payload(self.dek.as_ref(), &stored)?;
        self.next_seq += 1;
        match kind {
            FRAME_DATA => {
                self.digest.update(&plaintext);
                self.data_frames += 1;
                self.data_bytes += plaintext.len() as u64;
                Ok(Some(plaintext))
            }
            FRAME_TRAILER => {
                if plaintext.len() != TRAILER_LEN {
                    return Err(SpillError::Corrupt(
                        "spill trailer has the wrong length".into(),
                    ));
                }
                let frames = u64::from_le_bytes(plaintext[0..8].try_into().expect("slice length"));
                let bytes = u64::from_le_bytes(plaintext[8..16].try_into().expect("slice length"));
                let hash: [u8; 32] = plaintext[16..48].try_into().expect("slice length");
                let actual_hash: [u8; 32] = self.digest.clone().finalize().into();
                if frames != self.data_frames || bytes != self.data_bytes || hash != actual_hash {
                    return Err(SpillError::Corrupt(
                        "spill trailer does not match the streamed frames".into(),
                    ));
                }
                self.done = true;
                Ok(None)
            }
            other => Err(SpillError::Corrupt(format!(
                "unknown spill frame kind {other}"
            ))),
        }
    }
}

impl Iterator for SpillReader {
    type Item = Result<Vec<u8>, SpillError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_frame() {
            Ok(Some(frame)) => Some(Ok(frame)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    }
}

/// Maps the crypto stack's `MongrelError` onto the typed spill errors.
fn map_crypto(error: MongrelError) -> SpillError {
    match error {
        MongrelError::Encryption(message) => SpillError::Encryption(message),
        MongrelError::Decryption(message) => SpillError::Decryption(message),
        other => SpillError::Corrupt(other.to_string()),
    }
}

/// Seals one frame's plaintext payload with the page-cipher stack when a meta
/// DEK is present (fresh random nonce per frame, the `encrypt_blob` idiom);
/// passes plaintext through otherwise.
fn seal_payload(dek: Option<&[u8; DEK_LEN]>, plaintext: &[u8]) -> Result<Vec<u8>, SpillError> {
    match dek {
        Some(dek) => crate::encryption::encrypt_blob(dek, plaintext).map_err(map_crypto),
        None => Ok(plaintext.to_vec()),
    }
}

/// Fail-closed stub: [`SpillManager::open`] rejects a DEK without the
/// `encryption` feature, so this is unreachable in practice.

/// Inverse of [`seal_payload`]: authenticates and opens a sealed frame.
fn open_payload(dek: Option<&[u8; DEK_LEN]>, stored: &[u8]) -> Result<Vec<u8>, SpillError> {
    match dek {
        Some(dek) => crate::encryption::decrypt_blob(dek, stored).map_err(map_crypto),
        None => Ok(stored.to_vec()),
    }
}

/// Fail-closed stub — see [`seal_payload`].

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom};
    use std::path::PathBuf;

    fn manager(dir: &tempfile::TempDir, global_bytes: u64) -> SpillManager {
        let root = DurableRoot::open(dir.path()).unwrap();
        SpillManager::open(&root, SpillConfig::new(global_bytes), None).unwrap()
    }

    fn query_dir(dir: &tempfile::TempDir, query_id: QueryId) -> PathBuf {
        dir.path()
            .join("temp")
            .join("spill")
            .join(format!("q-{}", query_id.to_hex()))
    }

    fn only_file(dir: &tempfile::TempDir, query_id: QueryId) -> PathBuf {
        let entries: Vec<_> = std::fs::read_dir(query_dir(dir, query_id))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(entries.len(), 1, "expected exactly one spill file");
        entries[0].clone()
    }

    /// Serializes one frame exactly as `WriterInner::write_frame` does
    /// (plaintext mode), for crafting corrupt files.
    fn crafted_frame(kind: u8, seq: u64, payload: &[u8]) -> Vec<u8> {
        let mut digest = CRC32C.digest();
        digest.update(&[kind]);
        digest.update(&seq.to_le_bytes());
        digest.update(payload);
        let crc = digest.finalize();
        let mut out = Vec::with_capacity(FRAME_HEAD_LEN + payload.len());
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc.to_le_bytes());
        out.push(kind);
        out.extend_from_slice(&seq.to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn crafted_header(enc: u8) -> Vec<u8> {
        let mut header = vec![0u8; HEADER_LEN];
        header[..8].copy_from_slice(SPILL_MAGIC);
        header[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        header[10] = enc;
        header
    }

    #[test]
    fn frame_round_trip_streams_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 1 << 20);
        let session = manager.begin_query(QueryId::new_random(), 1 << 20).unwrap();
        let payloads: Vec<Vec<u8>> = vec![
            b"first".to_vec(),
            Vec::new(), // empty frames are legal
            vec![0xAB; 1000],
            b"last".to_vec(),
        ];
        let mut writer = session.new_writer().unwrap();
        for payload in &payloads {
            writer.append(payload).unwrap();
        }
        let handle = writer.finish().unwrap();
        assert_eq!(handle.frames(), 4);
        assert_eq!(handle.query_id(), session.query_id());
        assert_eq!(handle.bytes_on_disk(), session.used());

        let frames: Vec<Vec<u8>> = handle.reader().unwrap().collect::<Result<_, _>>().unwrap();
        assert_eq!(frames, payloads);

        let stats = manager.stats();
        assert_eq!(stats.files_live, 1);
        assert_eq!(stats.bytes_written, handle.bytes_on_disk());
        assert_eq!(stats.bytes_read, handle.bytes_on_disk());
        assert_eq!(stats.global_used, handle.bytes_on_disk());
        assert_eq!(stats.budget_remaining, (1 << 20) - handle.bytes_on_disk());
    }

    #[test]
    fn large_multi_chunk_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 1 << 24);
        let session = manager.begin_query(QueryId::new_random(), 1 << 24).unwrap();
        // Two chunk files, three frames of mixed large sizes each.
        let mut handles = Vec::new();
        let mut expected = Vec::new();
        for chunk in 0..2u8 {
            let mut writer = session.new_writer().unwrap();
            let mut payloads = Vec::new();
            for (index, len) in [(1usize << 20), (1 << 20) + 7, 333_333].iter().enumerate() {
                let payload = vec![chunk * 16 + index as u8; *len];
                writer.append(&payload).unwrap();
                payloads.push(payload);
            }
            expected.push(payloads);
            handles.push(writer.finish().unwrap());
        }
        assert_eq!(manager.stats().files_live, 2);
        for (handle, payloads) in handles.iter().zip(expected.iter()) {
            let frames: Vec<Vec<u8>> = handle.reader().unwrap().collect::<Result<_, _>>().unwrap();
            assert_eq!(&frames, payloads);
        }
    }

    #[test]
    fn checksum_mismatch_is_detected_on_read() {
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 1 << 20);
        let query_id = QueryId::new_random();
        let session = manager.begin_query(query_id, 1 << 20).unwrap();
        let mut writer = session.new_writer().unwrap();
        writer.append(&vec![0x11; 256]).unwrap();
        writer.append(b"second").unwrap();
        let handle = writer.finish().unwrap();

        // Flip one byte in the first frame's payload (header 12 + head 17).
        let path = only_file(&dir, query_id);
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start((HEADER_LEN + FRAME_HEAD_LEN) as u64))
            .unwrap();
        file.write_all(&[0x99]).unwrap();
        drop(file);

        let mut reader = handle.reader().unwrap();
        let error = reader.next_frame().unwrap_err();
        assert!(
            matches!(error, SpillError::ChecksumMismatch { .. }),
            "expected ChecksumMismatch, got {error:?}"
        );
        // The failure is terminal: no further frames are yielded.
        assert!(reader.next_frame().unwrap().is_none());
    }

    #[test]
    fn trailer_mismatch_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 1 << 20);
        // A crafted file: one valid data frame, then a trailer claiming two
        // frames — all CRCs valid, only the trailer semantics are wrong.
        let mut bytes = crafted_header(ENC_PLAINTEXT);
        bytes.extend_from_slice(&crafted_frame(FRAME_DATA, 0, b"abc"));
        let mut trailer = Vec::with_capacity(TRAILER_LEN);
        trailer.extend_from_slice(&2u64.to_le_bytes()); // wrong frame count
        trailer.extend_from_slice(&3u64.to_le_bytes());
        trailer.extend_from_slice(&[0u8; 32]);
        bytes.extend_from_slice(&crafted_frame(FRAME_TRAILER, 1, &trailer));
        let path = dir.path().join("crafted.spill");
        std::fs::write(&path, &bytes).unwrap();

        let file = std::fs::File::open(&path).unwrap();
        let mut reader = reader_from(file, &manager).unwrap();
        assert_eq!(reader.next_frame().unwrap(), Some(b"abc".to_vec()));
        let error = reader.next_frame().unwrap_err();
        assert!(
            matches!(error, SpillError::Corrupt(_)),
            "expected Corrupt, got {error:?}"
        );
    }

    #[test]
    fn bad_magic_and_version_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 1 << 20);
        for (label, mut header) in [
            ("magic", crafted_header(ENC_PLAINTEXT)),
            ("version", crafted_header(ENC_PLAINTEXT)),
            ("enc flag", crafted_header(ENC_PLAINTEXT)),
        ] {
            match label {
                "magic" => header[0] ^= 0xFF,
                "version" => header[8..10].copy_from_slice(&99u16.to_le_bytes()),
                _ => header[10] = 77,
            }
            let path = dir.path().join(format!("{label}.spill"));
            std::fs::write(&path, &header).unwrap();
            let file = std::fs::File::open(&path).unwrap();
            let error = reader_from(file, &manager).unwrap_err();
            assert!(
                matches!(error, SpillError::Corrupt(_)),
                "{label}: expected Corrupt, got {error:?}"
            );
        }
    }

    #[test]
    fn per_query_budget_is_enforced() {
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 1 << 20);
        let query_id = QueryId::new_random();
        let session = manager.begin_query(query_id, 100).unwrap();
        let mut writer = session.new_writer().unwrap(); // header: 12 bytes
        writer.append(&[0u8; 50]).unwrap(); // frame: 17 + 50 = 67 → 79 used
        assert_eq!(session.used(), 79);
        assert_eq!(session.budget_remaining(), 21);
        let error = writer.append(&[0u8; 50]).unwrap_err();
        assert!(
            matches!(
                error,
                SpillError::BudgetExceeded {
                    query_id: id,
                    requested,
                    query_remaining: 21,
                    ..
                } if id == query_id && requested == 67
            ),
            "expected BudgetExceeded, got {error:?}"
        );
        // The failed frame charged nothing; a fitting frame still lands.
        assert_eq!(session.used(), 79);
        writer.append(&[0u8; 4]).unwrap(); // 17 + 4 = 21 → exactly the cap
        assert_eq!(session.used(), 100);
        assert_eq!(session.budget_remaining(), 0);
        let handle = writer.finish().unwrap_err();
        // Even the trailer no longer fits.
        assert!(matches!(handle, SpillError::BudgetExceeded { .. }));
    }

    #[test]
    fn global_budget_is_enforced_across_queries() {
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 200);
        let a = manager.begin_query(QueryId::new_random(), 1 << 20).unwrap();
        let b = manager.begin_query(QueryId::new_random(), 1 << 20).unwrap();
        let mut writer_a = a.new_writer().unwrap();
        let mut writer_b = b.new_writer().unwrap();
        // Headers: 2 × 12 = 24 bytes of the global budget.
        writer_a.append(&[0u8; 100]).unwrap(); // +117 → 141 global
        let error = writer_b.append(&[0u8; 100]).unwrap_err(); // +117 > 200
        assert!(
            matches!(
                error,
                SpillError::BudgetExceeded {
                    global_remaining: 59,
                    ..
                }
            ),
            "expected global BudgetExceeded, got {error:?}"
        );
        assert_eq!(manager.stats().global_used, 141);
        // Releasing the first query's file re-opens the global budget.
        drop(writer_a);
        assert_eq!(manager.stats().global_used, 12);
        writer_b.append(&[0u8; 100]).unwrap();
    }

    #[test]
    fn unfinished_writer_drop_deletes_file_and_releases_budget() {
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 1 << 20);
        let query_id = QueryId::new_random();
        let session = manager.begin_query(query_id, 1 << 20).unwrap();
        let mut writer = session.new_writer().unwrap();
        writer.append(&[0u8; 100]).unwrap();
        let path = only_file(&dir, query_id);
        assert!(path.exists());
        let used = session.used();
        assert!(used > 0);
        drop(writer); // the cancel path
        assert!(!path.exists(), "partial spill file must be deleted");
        assert_eq!(session.used(), 0);
        assert_eq!(manager.stats().global_used, 0);
        assert_eq!(manager.stats().files_live, 0);
    }

    #[test]
    fn explicit_abort_deletes_and_reports() {
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 1 << 20);
        let query_id = QueryId::new_random();
        let session = manager.begin_query(query_id, 1 << 20).unwrap();
        let mut writer = session.new_writer().unwrap();
        writer.append(&[0u8; 64]).unwrap();
        let path = only_file(&dir, query_id);
        writer.abort().unwrap();
        assert!(!path.exists());
        assert_eq!(session.used(), 0);
        assert_eq!(manager.stats().global_used, 0);
    }

    #[test]
    fn finished_handle_drop_and_delete_release_everything() {
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 1 << 20);
        let query_id = QueryId::new_random();
        let session = manager.begin_query(query_id, 1 << 20).unwrap();

        let mut writer = session.new_writer().unwrap();
        writer.append(b"payload").unwrap();
        let handle = writer.finish().unwrap();
        let path = only_file(&dir, query_id);
        assert_eq!(manager.stats().files_live, 1);
        let used = session.used();
        drop(handle);
        assert!(!path.exists(), "sealed spill file must be deleted on drop");
        assert_eq!(session.used(), 0);
        assert_eq!(manager.stats().files_live, 0);
        assert_eq!(manager.stats().global_used, 0);

        // Explicit delete reports and releases the same way.
        let mut writer = session.new_writer().unwrap();
        writer.append(b"payload").unwrap();
        let handle = writer.finish().unwrap();
        assert_eq!(session.used(), used);
        handle.delete().unwrap();
        assert_eq!(session.used(), 0);
        assert_eq!(manager.stats().files_live, 0);
    }

    #[test]
    fn session_drop_removes_the_query_directory() {
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 1 << 20);
        let query_id = QueryId::new_random();
        let session = manager.begin_query(query_id, 1 << 20).unwrap();
        let mut writer = session.new_writer().unwrap();
        writer.append(b"x").unwrap();
        drop(writer);
        assert!(query_dir(&dir, query_id).exists());
        drop(session);
        assert!(
            !query_dir(&dir, query_id).exists(),
            "session drop must remove the per-query directory"
        );
        // A session that never spilled leaves nothing behind either.
        let quiet = manager.begin_query(QueryId::new_random(), 1 << 20).unwrap();
        drop(quiet);
        assert!(
            !dir.path().join("temp").join("spill").exists() || {
                std::fs::read_dir(dir.path().join("temp").join("spill"))
                    .unwrap()
                    .next()
                    .is_none()
            }
        );
    }

    #[test]
    fn open_sweeps_stale_entries_from_prior_runs() {
        let dir = tempfile::tempdir().unwrap();
        let query_id = QueryId::new_random();
        let stale_path;
        let first = manager(&dir, 1 << 20);
        let handle;
        {
            let session = first.begin_query(query_id, 1 << 20).unwrap();
            let mut writer = session.new_writer().unwrap();
            writer.append(b"from a previous process").unwrap();
            handle = writer.finish().unwrap();
            stale_path = only_file(&dir, query_id);
            // A stray file directly in the spill root must be swept too.
            std::fs::write(dir.path().join("temp").join("spill").join("stray"), b"x").unwrap();
            // Simulate a crash: the session and handle leak away.
            std::mem::forget(session);
        }
        assert!(stale_path.exists());
        assert_eq!(first.stats().global_used, handle.bytes_on_disk());

        // The next process opens its manager: everything stale is swept.
        let second = manager(&dir, 1 << 20);
        assert!(
            !stale_path.exists(),
            "startup sweep must remove stale files"
        );
        assert!(!dir.path().join("temp").join("spill").join("stray").exists());
        assert!(!query_dir(&dir, query_id).exists());
        assert_eq!(second.stats().global_used, 0);
        assert_eq!(second.stats().files_live, 0);

        // The leaked handle's drop is still safe (no double delete) and its
        // accounting unwinds against the first manager.
        drop(handle);
        assert_eq!(first.stats().global_used, 0);
    }

    #[test]
    fn frame_larger_than_the_limit_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, u64::MAX);
        let session = manager
            .begin_query(QueryId::new_random(), u64::MAX)
            .unwrap();
        let mut writer = session.new_writer().unwrap();
        let huge = vec![0u8; MAX_FRAME_PAYLOAD as usize + 1];
        let error = writer.append(&huge).unwrap_err();
        assert!(
            matches!(error, SpillError::FrameTooLarge { .. }),
            "expected FrameTooLarge, got {error:?}"
        );
    }

    #[test]
    fn invalid_config_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let root = DurableRoot::open(dir.path()).unwrap();
        let error = SpillManager::open(&root, SpillConfig::new(0), None).unwrap_err();
        assert!(matches!(error, SpillError::InvalidConfig(_)));
    }

    #[test]
    fn spill_error_maps_to_mongrel_error() {
        let io: MongrelError = SpillError::Io(io::Error::other("x")).into();
        assert!(matches!(io, MongrelError::Io(_)));
        let budget: MongrelError = SpillError::BudgetExceeded {
            query_id: QueryId::new_random(),
            requested: 10,
            query_remaining: 5,
            global_remaining: 90,
        }
        .into();
        assert!(matches!(budget, MongrelError::ResourceLimitExceeded { .. }));
        let checksum: MongrelError = SpillError::ChecksumMismatch {
            context: "frame".into(),
            expected: 1,
            actual: 2,
        }
        .into();
        assert!(matches!(checksum, MongrelError::ChecksumMismatch { .. }));
        let corrupt: MongrelError = SpillError::Corrupt("bad".into()).into();
        assert!(matches!(corrupt, MongrelError::Other(_)));
    }

    mod encrypted {
        use super::*;
        use crate::encryption::{meta_dek_for, Kek, SALT_LEN};

        fn encrypted_manager(dir: &tempfile::TempDir, dek: [u8; DEK_LEN]) -> SpillManager {
            let root = DurableRoot::open(dir.path()).unwrap();
            SpillManager::open(&root, SpillConfig::new(1 << 20), Some(dek)).unwrap()
        }

        fn test_dek(passphrase: &str) -> [u8; DEK_LEN] {
            let salt = [7u8; SALT_LEN];
            let kek = Kek::derive(passphrase, &salt).unwrap();
            meta_dek_for(Some(&kek)).unwrap()
        }

        #[test]
        fn encrypted_round_trip_seals_every_frame_on_disk() {
            let dir = tempfile::tempdir().unwrap();
            let manager = encrypted_manager(&dir, test_dek("pw"));
            let query_id = QueryId::new_random();
            let session = manager.begin_query(query_id, 1 << 20).unwrap();
            let marker = b"highly-recognizable-plaintext-marker";
            let mut writer = session.new_writer().unwrap();
            writer.append(marker).unwrap();
            writer.append(&vec![0x5A; 4096]).unwrap();
            let handle = writer.finish().unwrap();

            // Nothing on disk is the plaintext: marker and payload absent.
            let raw = std::fs::read(only_file(&dir, query_id)).unwrap();
            assert_eq!(raw[10], ENC_AES_GCM);
            assert!(!raw
                .windows(marker.len())
                .any(|window| window == marker.as_slice()));
            assert!(!raw.windows(64).any(|window| window == [0x5A; 64]));

            let frames: Vec<Vec<u8>> = handle.reader().unwrap().collect::<Result<_, _>>().unwrap();
            assert_eq!(frames, vec![marker.to_vec(), vec![0x5A; 4096]]);
        }

        #[test]
        fn encrypted_file_requires_the_key_and_detects_tampering() {
            let dir = tempfile::tempdir().unwrap();
            // Every manager is opened before any spill file exists: opening a
            // manager sweeps stale entries, so opening one after the file
            // lands would delete it (that behavior has its own test).
            let plaintext_manager = manager(&dir, 1 << 20);
            let wrong = encrypted_manager(&dir, test_dek("wrong"));
            let manager = encrypted_manager(&dir, test_dek("pw"));
            let query_id = QueryId::new_random();
            let session = manager.begin_query(query_id, 1 << 20).unwrap();
            let mut writer = session.new_writer().unwrap();
            writer.append(b"secret").unwrap();
            let handle = writer.finish().unwrap();
            let path = only_file(&dir, query_id);
            // A manager without the DEK refuses the encrypted file.
            let error =
                reader_from(std::fs::File::open(&path).unwrap(), &plaintext_manager).unwrap_err();
            assert!(
                matches!(error, SpillError::EncryptionRequired),
                "expected EncryptionRequired, got {error:?}"
            );

            // A wrong DEK fails at the GCM tag (decryption), not at parse.
            let mut reader = reader_from(std::fs::File::open(&path).unwrap(), &wrong).unwrap();
            let error = reader.next_frame().unwrap_err();
            assert!(
                matches!(error, SpillError::Decryption(_)),
                "expected Decryption, got {error:?}"
            );

            // The right key still reads.
            let frames: Vec<Vec<u8>> = handle.reader().unwrap().collect::<Result<_, _>>().unwrap();
            assert_eq!(frames, vec![b"secret".to_vec()]);
        }
    }
}
