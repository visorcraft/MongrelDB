//! Persistent result-cache publication (TODO §2).
//!
//! Goal: result-cache inserts must do **no** serialization, encryption, write,
//! sync, or rename on the query thread. The query thread enqueues a cheap
//! `PersistableEntry` (Arc-shared rows + scalars) onto a bounded coalescing
//! pending map; a background worker drains the map and performs the actual
//! disk write through a [`PersistentCacheIo`] abstraction.
//!
//! Layout: frame format + encryption helpers first, then the writer queue,
//! then the `PersistentCacheIo` trait + production `RealPersistentCacheIo`,
//! then the test module, then the worker spawn function. Clippy's
//! `items_after_test_module` lint flags this ordering; we keep it
//! intentionally because tests sit alongside the writer queue they cover
//! and the worker lives at the bottom because it depends on the writer.
//!
//! Frame format (stable across versions, written in little-endian order):
//!
//! ```text
//! [magic: 4B = b"MLCP"]
//! [format_version: u16 LE]
//! [reserved: u16 LE = 0]
//! [table_id: u64 LE]
//! [schema_id: u64 LE]
//! [run_generation: u64 LE]
//! [cache_key: u64 LE]
//! [entry_generation: u64 LE]
//! [payload_len: u32 LE]
//! [payload: u8 × payload_len]            # bincode(SerializedEntry) or
//!                                        #   encrypted bincode(...)
//! [crc32c: u32 LE]                       # over the prefix + payload_len + payload
//! ```
//!
//! A loader rejects frames whose magic, version, table_id, schema_id, or
//! run_generation does not match the open table, or whose CRC does not match
//! the stored bytes. Stale `cache_key`s (already invalidated) are also
//! rejected on reopen by the worker, which compares its queued entry's
//! per-key generation against the latest clear generation captured on enqueue.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crc::{Crc, CRC_32_ISCSI};

use crate::encryption::{AesCipher, Cipher};
use crate::engine::{LookupMetrics, LookupMetricsSnapshot};

/// Magic identifying a persistent-cache frame: "MLCP" (MongreLDb Cache Page).
pub const FRAME_MAGIC: [u8; 4] = *b"MLCP";
/// On-disk format version. Bumped on any layout change.
pub const FRAME_FORMAT_VERSION: u16 = 1;
/// Reserved key under which the `Clear` op is enqueued. Cannot collide with
/// any real cache key (`u64::MAX` is reserved as the per-op sentinel).
pub(crate) const CLEAR_KEY: u64 = u64::MAX;
/// Length of the fixed prefix before `payload_len`.
const FRAME_PREFIX_LEN: usize = 4 + 2 + 2 + 8 + 8 + 8 + 8 + 8;
/// Length of the fixed trailer after `payload`.
const FRAME_TRAILER_LEN: usize = 4;
/// Total frame header + checksum = prefix + payload_len + trailer.
const FRAME_HEADER_LEN: usize = FRAME_PREFIX_LEN + 4;
const FRAME_FULL_OVERHEAD: usize = FRAME_HEADER_LEN + FRAME_TRAILER_LEN;
/// AES-GCM nonce length.
const NONCE_LEN: usize = 12;
const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

/// Header of a persisted frame. Read from disk and re-validated before any
/// payload is decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedHeader {
    pub format_version: u16,
    pub table_id: u64,
    pub schema_id: u64,
    pub run_generation: u64,
    pub cache_key: u64,
    pub entry_generation: u64,
    pub payload_len: u32,
}

/// Decoded frame ready for the table-side deserializer.
#[derive(Debug, Clone)]
pub struct PersistedFrame {
    pub header: PersistedHeader,
    /// Plaintext or ciphertext payload bytes (decryption is the caller's job).
    pub payload: Vec<u8>,
}

/// Encode a `PersistedFrame` into a self-describing, checksummed byte buffer.
/// The CRC32C is computed over the fixed prefix and the payload bytes.
pub fn encode_frame(frame: &PersistedFrame) -> Vec<u8> {
    let mut out = Vec::with_capacity(FRAME_FULL_OVERHEAD + frame.payload.len());
    out.extend_from_slice(&FRAME_MAGIC);
    out.extend_from_slice(&frame.header.format_version.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // reserved
    out.extend_from_slice(&frame.header.table_id.to_le_bytes());
    out.extend_from_slice(&frame.header.schema_id.to_le_bytes());
    out.extend_from_slice(&frame.header.run_generation.to_le_bytes());
    out.extend_from_slice(&frame.header.cache_key.to_le_bytes());
    out.extend_from_slice(&frame.header.entry_generation.to_le_bytes());
    out.extend_from_slice(&frame.header.payload_len.to_le_bytes());
    out.extend_from_slice(&frame.payload);
    let crc = CRC32C.checksum(&out[0..FRAME_PREFIX_LEN + 4 + frame.payload.len()]);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// Decode a frame buffer. Returns `None` on any structural or CRC failure.
pub fn decode_frame(bytes: &[u8]) -> Option<PersistedFrame> {
    if bytes.len() < FRAME_FULL_OVERHEAD {
        return None;
    }
    // The body we checksum is everything except the trailing 4-byte CRC.
    let body_len = bytes.len() - FRAME_TRAILER_LEN;
    let stored_crc = u32::from_le_bytes(bytes[body_len..].try_into().ok()?);
    let actual_crc = CRC32C.checksum(&bytes[..body_len]);
    if actual_crc != stored_crc {
        return None;
    }
    let mut p = &bytes[..body_len];
    let mut magic = [0u8; 4];
    magic.copy_from_slice(&p[..4]);
    if magic != FRAME_MAGIC {
        return None;
    }
    p = &p[4..];
    let format_version = u16::from_le_bytes(p[..2].try_into().ok()?);
    p = &p[2..];
    let _reserved = u16::from_le_bytes(p[..2].try_into().ok()?);
    p = &p[2..];
    let table_id = u64::from_le_bytes(p[..8].try_into().ok()?);
    p = &p[8..];
    let schema_id = u64::from_le_bytes(p[..8].try_into().ok()?);
    p = &p[8..];
    let run_generation = u64::from_le_bytes(p[..8].try_into().ok()?);
    p = &p[8..];
    let cache_key = u64::from_le_bytes(p[..8].try_into().ok()?);
    p = &p[8..];
    let entry_generation = u64::from_le_bytes(p[..8].try_into().ok()?);
    p = &p[8..];
    let payload_len = u32::from_le_bytes(p[..4].try_into().ok()?) as usize;
    p = &p[4..];
    if p.len() < payload_len {
        return None;
    }
    let payload = p[..payload_len].to_vec();
    Some(PersistedFrame {
        header: PersistedHeader {
            format_version,
            table_id,
            schema_id,
            run_generation,
            cache_key,
            entry_generation,
            payload_len: payload_len as u32,
        },
        payload,
    })
}

/// Quick header-only peek: returns `Some(PersistedHeader)` if the buffer is
/// structurally valid and the magic/version match, without verifying the CRC
/// or copying the payload. The CRC is still verified — a fast reject path
/// would re-parse the entire buffer, so we simply delegate to [`decode_frame`].
pub fn read_header_only(bytes: &[u8]) -> Option<PersistedHeader> {
    decode_frame(bytes).map(|f| f.header)
}

// ============================================================================
// Persistent cache identity + shared encode/decode helpers
// ============================================================================

/// Stable identity bound to a persisted cache frame (REM-002 §18.1). The
/// loader compares the on-disk frame header against this identity and rejects
/// the entry on any mismatch. The `logical_generation` is the durable table
/// epoch (`Table::current_epoch().0`) — never a process-local counter — so a
/// valid frame from an older logical state cannot be served after a commit
/// has advanced the epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistentCacheIdentity {
    pub table_id: u64,
    pub schema_id: u64,
    pub logical_generation: u64,
}

/// Context passed to [`encode_persisted_entry`] and used by the worker to
/// validate the on-disk frame matches what the queue said should be written.
#[derive(Debug, Clone, Copy)]
pub struct PersistContext {
    pub identity: PersistentCacheIdentity,
    pub key: u64,
    pub entry_generation: u64,
}

/// Error returned by [`encode_persisted_entry`] when the payload cannot be
/// framed (e.g. encryption failure). The caller treats this as a transient
/// per-op failure and increments the persist error counter.
#[derive(Debug)]
pub struct CachePersistError {
    pub message: String,
}

impl std::fmt::Display for CachePersistError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CachePersistError {}

/// Why a persisted frame was rejected by [`decode_persisted_entry`]. The
/// caller maps each variant to a rejection metric / log line.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)] // some variants are reserved for future strictness
pub enum CacheLoadRejection {
    /// File does not start with the MLCP magic — legacy unframed data.
    LegacyUnframed,
    /// File is too small to contain a valid header / payload / trailer.
    Truncated,
    /// Stored CRC does not match the body bytes.
    BadCrc,
    /// Format version is not one we can read.
    UnsupportedVersion(u16),
    /// Table identity does not match the open table.
    TableIdMismatch { expected: u64, found: u64 },
    /// Schema identity does not match the open table.
    SchemaIdMismatch { expected: u64, found: u64 },
    /// Logical generation is older than the open table's current epoch.
    GenerationMismatch { expected: u64, found: u64 },
    /// The cache key embedded in the header does not match the on-disk file
    /// name (e.g. a file got renamed but its header still references the
    /// old key — the safest response is to reject the whole entry).
    KeyMismatch { expected: u64, found: u64 },
    /// File claimed an encrypted payload but a cipher was not provided.
    MissingCipher,
    /// Cipher was provided but the GCM tag check failed (wrong key, bit rot).
    DecryptionFailed,
    /// Frame decoded cleanly but the inner payload did not deserialize.
    PayloadInvalid,
}

impl std::fmt::Display for CacheLoadRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LegacyUnframed => f.write_str("legacy unframed data"),
            Self::Truncated => f.write_str("truncated frame"),
            Self::BadCrc => f.write_str("bad crc"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported format version {v}"),
            Self::TableIdMismatch { expected, found } => {
                write!(f, "table id mismatch: expected {expected}, found {found}")
            }
            Self::SchemaIdMismatch { expected, found } => {
                write!(f, "schema id mismatch: expected {expected}, found {found}")
            }
            Self::GenerationMismatch { expected, found } => write!(
                f,
                "logical generation mismatch: expected {expected}, found {found}"
            ),
            Self::KeyMismatch { expected, found } => {
                write!(f, "cache key mismatch: expected {expected}, found {found}")
            }
            Self::MissingCipher => f.write_str("cipher required for encrypted frame"),
            Self::DecryptionFailed => f.write_str("decryption failed"),
            Self::PayloadInvalid => f.write_str("payload deserialize failed"),
        }
    }
}

impl std::error::Error for CacheLoadRejection {}

/// Frame an already-bincode-serialized payload using the MLCP layout. Both
/// the async worker and the synchronous fallback call this — the on-disk
/// format is unified across both paths (REM-002 §18.2). The payload is
/// encrypted (or left as plaintext) before framing; the magic / version /
/// table / schema / generation / key / payload_len / CRC32C layout is the
/// same for both encrypted and plaintext frames.
pub fn encode_persisted_entry(
    context: PersistContext,
    payload: &[u8],
    cipher: Option<&AesCipher>,
) -> Result<Vec<u8>, CachePersistError> {
    let inner = encrypt_payload(cipher, payload).map_err(|e| CachePersistError {
        message: format!("encrypt_persisted_entry: {:?}", e),
    })?;
    let frame = PersistedFrame {
        header: PersistedHeader {
            format_version: FRAME_FORMAT_VERSION,
            table_id: context.identity.table_id,
            schema_id: context.identity.schema_id,
            run_generation: context.identity.logical_generation,
            cache_key: context.key,
            entry_generation: context.entry_generation,
            payload_len: inner.len() as u32,
        },
        payload: inner,
    };
    Ok(encode_frame(&frame))
}

/// Decode + validate a frame produced by [`encode_persisted_entry`]. On
/// rejection, the caller removes the on-disk file (the cache is disposable).
/// On success, the plaintext payload is returned so the engine can perform
/// the bincode-specific `SerializedEntry` deserialize.
pub fn decode_persisted_entry(
    expected: PersistentCacheIdentity,
    expected_key: u64,
    bytes: &[u8],
    cipher: Option<&AesCipher>,
) -> Result<Vec<u8>, CacheLoadRejection> {
    // Legacy unframed files: anything that does not start with MLCP is
    // treated as legacy data. The safe policy is to delete and recompute
    // (REM-002 §18.5).
    if bytes.len() < FRAME_MAGIC.len() || bytes[..FRAME_MAGIC.len()] != FRAME_MAGIC {
        return Err(CacheLoadRejection::LegacyUnframed);
    }
    let frame = decode_frame(bytes).ok_or(CacheLoadRejection::BadCrc)?;
    if frame.header.format_version != FRAME_FORMAT_VERSION {
        return Err(CacheLoadRejection::UnsupportedVersion(
            frame.header.format_version,
        ));
    }
    if frame.header.table_id != expected.table_id {
        return Err(CacheLoadRejection::TableIdMismatch {
            expected: expected.table_id,
            found: frame.header.table_id,
        });
    }
    if frame.header.schema_id != expected.schema_id {
        return Err(CacheLoadRejection::SchemaIdMismatch {
            expected: expected.schema_id,
            found: frame.header.schema_id,
        });
    }
    if frame.header.run_generation < expected.logical_generation {
        // Strict monotonic: a frame at an older logical generation cannot
        // be served. Anything from the same or newer generation is accepted;
        // a newer generation can only appear via a fresh write through the
        // shared encoder.
        return Err(CacheLoadRejection::GenerationMismatch {
            expected: expected.logical_generation,
            found: frame.header.run_generation,
        });
    }
    if frame.header.cache_key != expected_key {
        return Err(CacheLoadRejection::KeyMismatch {
            expected: expected_key,
            found: frame.header.cache_key,
        });
    }
    let plaintext = decrypt_payload(cipher, &frame.payload).map_err(|_e| {
        // The encrypt path reports `IoError`, but on the load path any
        // encryption error is treated as a key/auth failure (the file is
        // untrusted). Convert the error to a typed rejection so the loader
        // can return a clean variant.
        CacheLoadRejection::DecryptionFailed
    })?;
    plaintext.ok_or(CacheLoadRejection::DecryptionFailed)
}

// ============================================================================
// Pending-op map (writer queue)
// ============================================================================

/// A pending operation for one cache key. Later writes supersede earlier
/// writes; a `Remove` supersedes a `Store`. `Clear` invalidates every older
/// op under the same generation.
#[derive(Debug)]
pub enum PendingCacheOp {
    Store(PersistableEntry),
    Remove,
    /// Clear the on-disk cache entirely. The worker only needs to know this
    /// is a clear (the actual file deletion is performed by `io.clear()`).
    Clear,
}

/// Cheap-to-share body for a queued store. The query thread constructs this
/// with raw data (Arc-shared rows, columns, footprint) plus a lazy
/// [`PersistableEntry::payload_factory`]. The factory captures the raw data
/// and produces the bincode-serialized bytes only when the worker invokes
/// it — keeping the query thread free of both serialization and I/O.
pub struct PersistableEntry {
    pub key: u64,
    pub table_id: u64,
    pub schema_id: u64,
    pub run_generation: u64,
    pub entry_generation: u64,
    /// Approximate in-memory bytes; used for queue accounting.
    pub bytes: usize,
    /// Lazy bincode payload producer. Invoked exactly once by the worker
    /// thread; the query thread never executes it. Returns `None` if
    /// bincode encoding fails.
    pub payload_factory: Box<dyn FnOnce() -> Option<Vec<u8>> + Send + 'static>,
}

impl std::fmt::Debug for PersistableEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistableEntry")
            .field("key", &self.key)
            .field("table_id", &self.table_id)
            .field("schema_id", &self.schema_id)
            .field("run_generation", &self.run_generation)
            .field("entry_generation", &self.entry_generation)
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

/// State of the pending-operation map. The worker publishes stores only when
/// the queued op's per-key generation still matches the latest generation seen
/// for that key — a newer invalidate cannot be silently overwritten.
#[derive(Debug)]
pub struct PendingCacheState {
    pub clear_generation: u64,
    pub next_key_generation: u64,
    /// Per-key, monotonically increasing generation. A `Remove` resets a key
    /// to the same generation as a fresh `Store` would receive, so any prior
    /// `Store` is observed as stale.
    pub key_generations: HashMap<u64, u64>,
    pub operations: HashMap<u64, PendingCacheOp>,
    /// Per-op byte estimate: sum of `entry.bytes` for `Store`s. Used for
    /// the `max_pending_bytes` capacity check; checked/saturating on every
    /// transition.
    pub approx_bytes: usize,
}

impl Default for PendingCacheState {
    fn default() -> Self {
        Self {
            clear_generation: 0,
            next_key_generation: 1,
            key_generations: HashMap::new(),
            operations: HashMap::new(),
            approx_bytes: 0,
        }
    }
}

/// Capacity knobs for the writer. Held behind `Arc` so the worker can read
/// without taking the queue lock.
#[derive(Debug, Clone)]
pub struct WriterLimits {
    pub max_pending_keys: usize,
    pub max_pending_bytes: usize,
}

impl Default for WriterLimits {
    fn default() -> Self {
        Self {
            max_pending_keys: 16_384,
            max_pending_bytes: 64 * 1024 * 1024,
        }
    }
}

struct Inner {
    state: PendingCacheState,
    limits: WriterLimits,
    shutdown: bool,
}

/// Summary returned by the worker for one drained op. The writer uses it to
/// maintain the stale-store / errors / abandoned counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOutcome {
    StorePublished,
    StoreStale,
    StoreErrored,
    RemoveApplied,
    RemoveErrored,
    ClearApplied,
    ClearErrored,
    Abandoned,
}

/// A queued op + the key/generation snapshot the worker must verify before
/// publishing. The worker re-reads the queue's per-key generation under the
/// lock and rejects the op if it has been superseded.
#[derive(Debug)]
pub struct DrainedOp {
    pub key: u64,
    pub op: PendingCacheOp,
    pub key_generation: u64,
    pub clear_generation: u64,
}

/// The PersistentResultCacheWriter. The query thread calls
/// [`PersistentResultCacheWriter::enqueue_store`] / [`enqueue_remove`] /
/// `enqueue_clear`; the worker calls [`drain_one`] in a loop and re-validates
/// the op before publishing.
pub struct PersistentResultCacheWriter {
    inner: Mutex<Inner>,
    cond: Condvar,
    metrics: LookupMetrics,
    enqueued_total: AtomicU64,
    coalesced_total: AtomicU64,
    dropped_total: AtomicU64,
    remove_total: AtomicU64,
    stale_total: AtomicU64,
    errors_total: AtomicU64,
    abandoned_total: AtomicU64,
    queue_depth: AtomicU64,
    /// Number of I/O operations the worker is currently processing. The
    /// worker increments this counter before invoking `io.write_atomic` /
    /// `io.remove` / `io.clear` and decrements after the call returns. A
    /// drain helper that observes `queue_depth == 0` should also wait for
    /// this counter to drop to 0 before declaring the writer idle — the
    /// "queue empty but write still in flight" window is what the
    /// `flush_persistent_cache(deadline)` test relies on.
    writes_in_flight: AtomicU64,
}

impl PersistentResultCacheWriter {
    pub fn new(metrics: LookupMetrics, limits: WriterLimits) -> Self {
        Self {
            inner: Mutex::new(Inner {
                state: PendingCacheState::default(),
                limits,
                shutdown: false,
            }),
            cond: Condvar::new(),
            metrics,
            enqueued_total: AtomicU64::new(0),
            coalesced_total: AtomicU64::new(0),
            dropped_total: AtomicU64::new(0),
            remove_total: AtomicU64::new(0),
            stale_total: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            abandoned_total: AtomicU64::new(0),
            queue_depth: AtomicU64::new(0),
            writes_in_flight: AtomicU64::new(0),
        }
    }

    /// Enqueue a `Store` op. Coalesces with a previous `Store` for the same
    /// key (the new entry supersedes the old one — old bytes are subtracted
    /// from the queue's `approx_bytes` first). Drops the op and increments
    /// the dropped counter when the queue is full.
    pub fn enqueue_store(&self, entry: PersistableEntry) {
        let mut guard = self.inner.lock().expect("writer mutex poisoned");
        let key = entry.key;
        let bytes = entry.bytes;
        // Per spec §9.4: coalescing the SAME key never adds a key. Check
        // existing membership before applying the key-count cap.
        let already_present = guard.state.operations.contains_key(&key);
        let would_grow = !already_present;
        let key_cap = guard.limits.max_pending_keys;
        let byte_cap = guard.limits.max_pending_bytes;
        if (would_grow && guard.state.operations.len() >= key_cap)
            || guard.state.approx_bytes.saturating_add(bytes) > byte_cap
        {
            self.dropped_total.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .result_cache_persist_dropped_store_total
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        // Per-op decision: a pending Remove wins over a new Store, but the
        // caller's intent is preserved by leaving the Remove queued. The new
        // Store is dropped silently (counted as dropped).
        if let Some(PendingCacheOp::Remove) = guard.state.operations.get(&key) {
            self.dropped_total.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .result_cache_persist_dropped_store_total
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        if let Some(PendingCacheOp::Store(prev)) = guard.state.operations.get(&key) {
            // Store→Store: subtract prev's bytes, count as coalesced.
            guard.state.approx_bytes = guard.state.approx_bytes.saturating_sub(prev.bytes);
            self.coalesced_total.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .result_cache_persist_coalesced_total
                .fetch_add(1, Ordering::Relaxed);
        } else {
            // Fresh key: register its generation.
            let gen = guard.state.next_key_generation;
            guard.state.next_key_generation = guard.state.next_key_generation.wrapping_add(1);
            guard.state.key_generations.insert(key, gen);
        }
        guard
            .state
            .operations
            .insert(key, PendingCacheOp::Store(entry));
        guard.state.approx_bytes = guard.state.approx_bytes.saturating_add(bytes);
        let depth = guard.state.operations.len() as u64;
        drop(guard);
        self.queue_depth.store(depth, Ordering::Relaxed);
        self.enqueued_total.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .result_cache_persist_enqueued_total
            .fetch_add(1, Ordering::Relaxed);
        self.cond.notify_one();
    }

    /// Enqueue a `Remove` op. Always supersedes any pending `Store` for the
    /// same key (its bytes are subtracted so the queue can free capacity
    /// before the worker actually deletes the file).
    pub fn enqueue_remove(&self, key: u64) {
        let mut guard = self.inner.lock().expect("writer mutex poisoned");
        let new_gen = guard.state.next_key_generation;
        guard.state.next_key_generation = guard.state.next_key_generation.wrapping_add(1);
        guard.state.key_generations.insert(key, new_gen);
        // Subtract the bytes of any pending Store this Remove supersedes.
        if let Some(PendingCacheOp::Store(prev)) = guard.state.operations.get(&key) {
            guard.state.approx_bytes = guard.state.approx_bytes.saturating_sub(prev.bytes);
        }
        guard.state.operations.insert(key, PendingCacheOp::Remove);
        let depth = guard.state.operations.len() as u64;
        drop(guard);
        self.remove_total.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .result_cache_persist_remove_total
            .fetch_add(1, Ordering::Relaxed);
        self.queue_depth.store(depth, Ordering::Relaxed);
        self.cond.notify_one();
    }

    /// Advance the global clear generation and enqueue a `Clear` op so the
    /// worker performs the actual file deletion. The in-memory pending map is
    /// also dropped because every existing op is now stale.
    pub fn enqueue_clear(&self) {
        let mut guard = self.inner.lock().expect("writer mutex poisoned");
        guard.state.clear_generation = guard.state.next_key_generation;
        guard.state.next_key_generation = guard.state.next_key_generation.wrapping_add(1);
        // Subtract the bytes of every Store before clearing the map.
        let mut to_sub = 0usize;
        for entry in guard.state.operations.values() {
            if let PendingCacheOp::Store(s) = entry {
                to_sub = to_sub.saturating_add(s.bytes);
            }
        }
        guard.state.approx_bytes = guard.state.approx_bytes.saturating_sub(to_sub);
        guard.state.operations.clear();
        guard.state.key_generations.clear();
        // Insert the Clear op under a reserved key (`u64::MAX`) so the worker
        // picks it up next. The key space is the cache key space, so this
        // cannot collide with a real cache key.
        guard
            .state
            .operations
            .insert(CLEAR_KEY, PendingCacheOp::Clear);
        let depth = guard.state.operations.len() as u64;
        drop(guard);
        self.queue_depth.store(depth, Ordering::Relaxed);
        self.cond.notify_all();
    }

    /// Pop the next pending op, waiting on the condvar until one is queued
    /// or shutdown is requested. Returns `None` only after shutdown AND the
    /// queue is empty — the worker drains remaining ops before exit.
    pub fn drain_one(&self) -> Option<DrainedOp> {
        let mut guard = self.inner.lock().expect("writer mutex poisoned");
        loop {
            if let Some(next_key) = guard.state.operations.keys().next().copied() {
                let op = guard
                    .state
                    .operations
                    .remove(&next_key)
                    .expect("just observed");
                let key_gen = guard
                    .state
                    .key_generations
                    .get(&next_key)
                    .copied()
                    .unwrap_or(0);
                // Subtract the queued op's contribution to the byte total.
                if let PendingCacheOp::Store(s) = &op {
                    guard.state.approx_bytes = guard.state.approx_bytes.saturating_sub(s.bytes);
                }
                // Drop the per-key generation entry — any new op will allocate a
                // fresh generation on insert. This means a later fresh `Store`
                // for the same key cannot collide with a stale op.
                guard.state.key_generations.remove(&next_key);
                let clear_gen = guard.state.clear_generation;
                let depth = guard.state.operations.len() as u64;
                drop(guard);
                self.queue_depth.store(depth, Ordering::Relaxed);
                return Some(DrainedOp {
                    key: next_key,
                    op,
                    key_generation: key_gen,
                    clear_generation: clear_gen,
                });
            }
            if guard.shutdown {
                return None;
            }
            guard = self.cond.wait(guard).expect("condvar wait poisoned");
        }
    }

    /// Non-blocking variant: returns `Some(op)` if any op is queued, else
    /// `None`. Used by `flush_persistent_cache(deadline)` to drain the queue
    /// under a deadline.
    pub fn try_drain_one(&self) -> Option<DrainedOp> {
        let mut guard = self.inner.lock().expect("writer mutex poisoned");
        let next_key = {
            let mut iter = guard.state.operations.iter();
            let (k, _) = iter.next()?;
            *k
        };
        let op = guard
            .state
            .operations
            .remove(&next_key)
            .expect("just observed");
        let key_gen = guard
            .state
            .key_generations
            .get(&next_key)
            .copied()
            .unwrap_or(0);
        if let PendingCacheOp::Store(s) = &op {
            guard.state.approx_bytes = guard.state.approx_bytes.saturating_sub(s.bytes);
        }
        guard.state.key_generations.remove(&next_key);
        let clear_gen = guard.state.clear_generation;
        let depth = guard.state.operations.len() as u64;
        drop(guard);
        self.queue_depth.store(depth, Ordering::Relaxed);
        Some(DrainedOp {
            key: next_key,
            op,
            key_generation: key_gen,
            clear_generation: clear_gen,
        })
    }

    /// Mark the writer as shutdown. The worker drains remaining ops until
    /// the queue is empty or the optional deadline expires; anything still
    /// queued at that point is counted as abandoned.
    pub fn shutdown(&self) {
        let mut guard = self.inner.lock().expect("writer mutex poisoned");
        guard.shutdown = true;
        drop(guard);
        self.cond.notify_all();
    }

    /// Count the ops still queued at the moment of the call. Used to compute
    /// the abandoned count on shutdown.
    pub fn pending_count(&self) -> u64 {
        self.queue_depth.load(Ordering::Relaxed)
    }

    /// Drain every remaining op synchronously, accumulating their per-op
    /// byte estimates into `abandoned_bytes` and counting each as abandoned.
    /// Returns the number of abandoned ops. Used by
    /// `shutdown_persistent_cache(deadline)` to clear the queue on close.
    pub fn drain_all_as_abandoned(&self) -> u64 {
        let mut guard = self.inner.lock().expect("writer mutex poisoned");
        let mut n = 0u64;
        let mut bytes = 0usize;
        for (_, op) in guard.state.operations.drain() {
            if let PendingCacheOp::Store(s) = op {
                bytes = bytes.saturating_add(s.bytes);
            }
            n += 1;
        }
        guard.state.key_generations.clear();
        guard.state.approx_bytes = 0;
        drop(guard);
        if n > 0 {
            self.abandoned_total.fetch_add(n, Ordering::Relaxed);
            self.metrics
                .result_cache_persist_shutdown_abandoned_total
                .fetch_add(n, Ordering::Relaxed);
            let _ = bytes; // currently unused; reserved for future accounting
        }
        self.queue_depth.store(0, Ordering::Relaxed);
        self.cond.notify_all();
        n
    }

    pub fn record_outcome(&self, outcome: DrainOutcome) {
        match outcome {
            DrainOutcome::StorePublished => {}
            DrainOutcome::RemoveApplied => {}
            DrainOutcome::ClearApplied => {}
            DrainOutcome::StoreStale => {
                self.stale_total.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .result_cache_persist_stale_store_skipped_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            DrainOutcome::StoreErrored
            | DrainOutcome::RemoveErrored
            | DrainOutcome::ClearErrored => {
                self.errors_total.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .result_cache_persist_errors_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            DrainOutcome::Abandoned => {
                self.abandoned_total.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .result_cache_persist_shutdown_abandoned_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn enqueued_total(&self) -> u64 {
        self.enqueued_total.load(Ordering::Relaxed)
    }
    pub fn coalesced_total(&self) -> u64 {
        self.coalesced_total.load(Ordering::Relaxed)
    }
    pub fn dropped_total(&self) -> u64 {
        self.dropped_total.load(Ordering::Relaxed)
    }
    pub fn remove_total(&self) -> u64 {
        self.remove_total.load(Ordering::Relaxed)
    }
    pub fn stale_total(&self) -> u64 {
        self.stale_total.load(Ordering::Relaxed)
    }
    pub fn errors_total(&self) -> u64 {
        self.errors_total.load(Ordering::Relaxed)
    }
    pub fn abandoned_total(&self) -> u64 {
        self.abandoned_total.load(Ordering::Relaxed)
    }
    pub fn queue_depth(&self) -> usize {
        self.queue_depth.load(Ordering::Relaxed) as usize
    }
    /// Number of I/O operations the worker has not yet completed.
    pub fn writes_in_flight(&self) -> u64 {
        self.writes_in_flight.load(Ordering::Relaxed)
    }

    /// Point-in-time copy of the persistent-cache publish counters. Exposed
    /// so external integration tests can assert on the writer's bookkeeping
    /// without going through `Table::lookup_metrics_snapshot`.
    pub fn persist_snapshot(&self) -> LookupMetricsSnapshot {
        LookupMetricsSnapshot {
            result_cache_persist_enqueued_total: self.enqueued_total.load(Ordering::Relaxed),
            result_cache_persist_coalesced_total: self.coalesced_total.load(Ordering::Relaxed),
            result_cache_persist_dropped_store_total: self.dropped_total.load(Ordering::Relaxed),
            result_cache_persist_remove_total: self.remove_total.load(Ordering::Relaxed),
            result_cache_persist_stale_store_skipped_total: self
                .stale_total
                .load(Ordering::Relaxed),
            result_cache_persist_errors_total: self.errors_total.load(Ordering::Relaxed),
            result_cache_persist_shutdown_abandoned_total: self
                .abandoned_total
                .load(Ordering::Relaxed),
            result_cache_persist_queue_depth: self.queue_depth.load(Ordering::Relaxed),
            ..LookupMetricsSnapshot::default()
        }
    }

    /// Construct a writer with a fresh [`LookupMetrics`] (the canonical
    /// metrics type is `pub(crate)` and not constructible from external
    /// integration tests).
    pub fn for_test(limits: WriterLimits) -> Self {
        Self::new(LookupMetrics::default(), limits)
    }

    /// Bump the writer's per-key generation for `key` and return the new
    /// generation. Used by the cache when it allocates a fresh entry
    /// generation just before enqueueing, so a subsequent invalidation
    /// (Remove / Clear) is observed as newer.
    pub fn bump_persist_generation(&self, key: u64) -> u64 {
        let mut guard = self.inner.lock().expect("writer mutex poisoned");
        let gen = guard.state.next_key_generation;
        guard.state.next_key_generation = guard.state.next_key_generation.wrapping_add(1);
        guard.state.key_generations.insert(key, gen);
        gen
    }
}

// ============================================================================
// PersistentCacheIo trait
// ============================================================================

/// I/O abstraction the worker thread uses to publish and reload persistent
/// cache frames. Production code uses [`RealPersistentCacheIo`]; tests inject
/// recording/blocking/failing variants.
pub trait PersistentCacheIo: Send + Sync {
    /// Atomically write `frame` for `key` (write to temp + fsync + rename +
    /// dir-sync). Errors are reported as `IoError`.
    fn write_atomic(&self, key: u64, frame: &[u8]) -> Result<(), IoError>;
    /// Remove the persistent frame for `key`, if any. Missing files are OK.
    fn remove(&self, key: u64) -> Result<(), IoError>;
    /// Delete every persistent frame in this cache. Used by `clear`.
    fn clear(&self) -> Result<(), IoError>;
    /// Read the persistent frame for `key`, if any. Missing files return
    /// `Ok(None)` so the loader can ignore absent entries.
    fn load(&self, key: u64) -> Result<Option<Vec<u8>>, IoError>;
    /// Best-effort: check whether the on-disk frame for `key` exists.
    fn exists(&self, key: u64) -> bool;
}

/// Errors raised by [`PersistentCacheIo`]. Stored as opaque strings so the
/// worker can record them on its metrics without converting every I/O error
/// into a `MongrelError`.
#[derive(Debug, Clone)]
pub struct IoError {
    pub kind: IoErrorKind,
    pub message: String,
}

impl IoError {
    pub fn new(kind: IoErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

/// Distinguishes "retryable" from "fatal" I/O errors. Currently a marker;
/// future changes may add fallback paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoErrorKind {
    Other,
}

/// Real implementation of [`PersistentCacheIo`] writing to a temp directory.
/// Each call performs temp write + fsync + rename + parent dir sync, which
/// is the same crash-safe contract that production code already requires.
pub struct RealPersistentCacheIo {
    dir: PathBuf,
}

impl RealPersistentCacheIo {
    pub fn new(dir: PathBuf) -> std::io::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }
    fn final_path(&self, key: u64) -> PathBuf {
        self.dir.join(format!("{key:016x}.bin"))
    }
    fn temp_path(&self, key: u64) -> PathBuf {
        self.dir.join(format!("{key:016x}.bin.tmp"))
    }
}

impl PersistentCacheIo for RealPersistentCacheIo {
    fn write_atomic(&self, key: u64, frame: &[u8]) -> Result<(), IoError> {
        let final_path = self.final_path(key);
        let tmp_path = self.temp_path(key);
        // Remove any orphan temp file from a prior failed write.
        let _ = std::fs::remove_file(&tmp_path);
        {
            let mut f = std::fs::File::create(&tmp_path).map_err(|e| {
                IoError::new(
                    IoErrorKind::Other,
                    format!("create {}: {e}", tmp_path.display()),
                )
            })?;
            f.write_all(frame)
                .map_err(|e| IoError::new(IoErrorKind::Other, format!("write tmp: {e}")))?;
            f.flush()
                .map_err(|e| IoError::new(IoErrorKind::Other, format!("flush tmp: {e}")))?;
            f.sync_all()
                .map_err(|e| IoError::new(IoErrorKind::Other, format!("fsync tmp: {e}")))?;
        }
        std::fs::rename(&tmp_path, &final_path).map_err(|e| {
            // Best effort: clean up the temp file before reporting the error.
            let _ = std::fs::remove_file(&tmp_path);
            IoError::new(
                IoErrorKind::Other,
                format!(
                    "rename {} -> {}: {e}",
                    tmp_path.display(),
                    final_path.display()
                ),
            )
        })?;
        // Durability: fsync the parent dir so the rename is durable.
        if let Ok(dir) = std::fs::File::open(&self.dir) {
            let _ = dir.sync_all();
        }
        Ok(())
    }
    fn remove(&self, key: u64) -> Result<(), IoError> {
        let path = self.final_path(key);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(IoError::new(
                IoErrorKind::Other,
                format!("remove {}: {e}", path.display()),
            )),
        }
    }
    fn clear(&self) -> Result<(), IoError> {
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                return Err(IoError::new(
                    IoErrorKind::Other,
                    format!("read_dir {}: {e}", self.dir.display()),
                ))
            }
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("bin") {
                if let Err(e) = std::fs::remove_file(&path) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        return Err(IoError::new(
                            IoErrorKind::Other,
                            format!("remove {}: {e}", path.display()),
                        ));
                    }
                }
            }
        }
        Ok(())
    }
    fn load(&self, key: u64) -> Result<Option<Vec<u8>>, IoError> {
        let path = self.final_path(key);
        match std::fs::read(&path) {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(IoError::new(
                IoErrorKind::Other,
                format!("read {}: {e}", path.display()),
            )),
        }
    }
    fn exists(&self, key: u64) -> bool {
        self.final_path(key).exists()
    }
}

/// Helper used by the worker to encrypt a payload before it is written.
/// Returns the plaintext bytes untouched when no DEK is present.
pub fn encrypt_payload(cipher: Option<&AesCipher>, plaintext: &[u8]) -> Result<Vec<u8>, IoError> {
    let Some(cipher) = cipher else {
        return Ok(plaintext.to_vec());
    };
    let mut nonce = [0u8; NONCE_LEN];
    crate::encryption::fill_random(&mut nonce)
        .map_err(|e| IoError::new(IoErrorKind::Other, format!("fill_random nonce: {e}")))?;
    let ct = cipher
        .encrypt_page(&nonce, plaintext)
        .map_err(|e| IoError::new(IoErrorKind::Other, format!("aes encrypt: {e}")))?;
    let mut out = Vec::with_capacity(NONCE_LEN + ct.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    Ok(out)
}

/// Inverse of [`encrypt_payload`]: returns `None` on key mismatch or tag
/// failure.
pub fn decrypt_payload(
    cipher: Option<&AesCipher>,
    bytes: &[u8],
) -> Result<Option<Vec<u8>>, IoError> {
    let Some(cipher) = cipher else {
        return Ok(Some(bytes.to_vec()));
    };
    if bytes.len() < NONCE_LEN {
        return Ok(None);
    }
    let nonce: [u8; NONCE_LEN] = bytes[..NONCE_LEN].try_into().expect("checked above");
    match cipher.decrypt_page(&nonce, &bytes[NONCE_LEN..]) {
        Ok(plaintext) => Ok(Some(plaintext)),
        Err(_) => Ok(None),
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;

    fn entry(key: u64, bytes: usize) -> PersistableEntry {
        PersistableEntry {
            key,
            table_id: 0,
            schema_id: 0,
            run_generation: 0,
            entry_generation: 0,
            bytes,
            payload_factory: Box::new(|| Some(Vec::new())),
        }
    }

    #[test]
    fn frame_round_trip() {
        let frame = PersistedFrame {
            header: PersistedHeader {
                format_version: FRAME_FORMAT_VERSION,
                table_id: 7,
                schema_id: 9,
                run_generation: 11,
                cache_key: 13,
                entry_generation: 17,
                payload_len: 5,
            },
            payload: b"hello".to_vec(),
        };
        let encoded = encode_frame(&frame);
        let decoded = decode_frame(&encoded).expect("round-trip");
        assert_eq!(decoded.header, frame.header);
        assert_eq!(decoded.payload, frame.payload);
    }

    #[test]
    fn frame_rejects_bad_crc() {
        let frame = PersistedFrame {
            header: PersistedHeader {
                format_version: FRAME_FORMAT_VERSION,
                table_id: 1,
                schema_id: 1,
                run_generation: 1,
                cache_key: 1,
                entry_generation: 1,
                payload_len: 4,
            },
            payload: b"test".to_vec(),
        };
        let mut encoded = encode_frame(&frame);
        let last = encoded.len() - 1;
        encoded[last] ^= 0xFF;
        assert!(decode_frame(&encoded).is_none());
    }

    #[test]
    fn coalescing_replaces_earlier_store() {
        let m = LookupMetrics::default();
        let w = PersistentResultCacheWriter::new(m, WriterLimits::default());
        w.enqueue_store(entry(1, 100));
        w.enqueue_store(entry(1, 200));
        assert_eq!(w.coalesced_total(), 1);
        assert_eq!(w.queue_depth(), 1);
    }

    #[test]
    fn coalescing_does_not_grow_key_count() {
        // Even when max_pending_keys == 1, the second enqueue for the same
        // key must be coalesced, not dropped.
        let m = LookupMetrics::default();
        let w = PersistentResultCacheWriter::new(
            m,
            WriterLimits {
                max_pending_keys: 1,
                max_pending_bytes: 1024 * 1024,
            },
        );
        w.enqueue_store(entry(7, 100));
        w.enqueue_store(entry(7, 200));
        assert_eq!(w.coalesced_total(), 1);
        assert_eq!(w.dropped_total(), 0);
        assert_eq!(w.queue_depth(), 1);
    }

    #[test]
    fn remove_supersedes_pending_store() {
        let m = LookupMetrics::default();
        let w = PersistentResultCacheWriter::new(m, WriterLimits::default());
        w.enqueue_store(entry(1, 100));
        w.enqueue_remove(1);
        // A subsequent Store against a pending Remove is dropped.
        w.enqueue_store(entry(1, 100));
        assert_eq!(w.dropped_total(), 1);
        assert_eq!(w.queue_depth(), 1);
    }

    #[test]
    fn clear_invalidates_every_queued_op() {
        let m = LookupMetrics::default();
        let w = PersistentResultCacheWriter::new(m, WriterLimits::default());
        w.enqueue_store(entry(1, 100));
        w.enqueue_store(entry(2, 100));
        w.enqueue_clear();
        // The pending Stores are dropped; a single Clear op is left for the
        // worker to consume (the worker will issue `io.clear()`).
        assert_eq!(w.queue_depth(), 1);
        let drained = w.drain_one().expect("drain clear");
        assert!(matches!(drained.op, PendingCacheOp::Clear));
        assert_eq!(w.queue_depth(), 0);
    }

    #[test]
    fn capacity_overflow_drops_new_key() {
        let m = LookupMetrics::default();
        let w = PersistentResultCacheWriter::new(
            m,
            WriterLimits {
                max_pending_keys: 1,
                max_pending_bytes: 1024,
            },
        );
        w.enqueue_store(entry(1, 100));
        w.enqueue_store(entry(2, 100));
        assert_eq!(w.dropped_total(), 1);
        assert_eq!(w.queue_depth(), 1);
    }

    #[test]
    fn byte_accounting_exact() {
        let m = LookupMetrics::default();
        let w = PersistentResultCacheWriter::new(
            m,
            WriterLimits {
                max_pending_keys: 64,
                max_pending_bytes: 1000,
            },
        );
        w.enqueue_store(entry(1, 400));
        w.enqueue_store(entry(2, 400));
        // The third enqueue for a new key would push bytes past the cap.
        w.enqueue_store(entry(3, 400));
        assert_eq!(w.dropped_total(), 1);
        assert_eq!(w.queue_depth(), 2);

        // Replace store-1 with a 200-byte Store; the queue byte total drops
        // from 800 to 600. Now enqueueing a new 400-byte Store fits.
        w.enqueue_store(entry(1, 200));
        w.enqueue_store(entry(3, 400));
        assert_eq!(w.dropped_total(), 1);

        // After drain, the remaining entry must keep the queue under cap and
        // allow a new 400-byte Store.
        let m2 = LookupMetrics::default();
        let w2 = PersistentResultCacheWriter::new(
            m2,
            WriterLimits {
                max_pending_keys: 64,
                max_pending_bytes: 1000,
            },
        );
        w2.enqueue_store(entry(1, 400));
        w2.enqueue_store(entry(2, 400));
        // Drain one of the two keys; the remaining entry must keep the
        // queue under cap and allow a new 400-byte Store.
        let _ = w2.drain_one();
        w2.enqueue_store(entry(3, 400));
        assert_eq!(w2.dropped_total(), 0);
    }

    #[test]
    fn shutdown_drains_pending_ops() {
        let m = LookupMetrics::default();
        let w = PersistentResultCacheWriter::new(m, WriterLimits::default());
        w.enqueue_store(entry(1, 100));
        w.enqueue_store(entry(2, 100));
        w.shutdown();
        assert!(w.drain_one().is_some());
        assert!(w.drain_one().is_some());
        assert!(w.drain_one().is_none());
    }

    #[test]
    fn drain_all_as_abandoned_clears_queue() {
        let m = LookupMetrics::default();
        let w = PersistentResultCacheWriter::new(m, WriterLimits::default());
        w.enqueue_store(entry(1, 100));
        w.enqueue_store(entry(2, 100));
        w.enqueue_store(entry(3, 100));
        let n = w.drain_all_as_abandoned();
        assert_eq!(n, 3);
        assert_eq!(w.queue_depth(), 0);
        assert_eq!(w.abandoned_total(), 3);
    }
}

/// Test-only: read a path under a [`RealPersistentCacheIo`] for assertion
/// purposes. The path uses a hex key naming scheme identical to the I/O
/// implementation, so the same `load`/`write_atomic` round-trips apply.
pub fn real_io_final_path(dir: &Path, key: u64) -> PathBuf {
    dir.join(format!("{key:016x}.bin"))
}

// ============================================================================
// Worker
// ============================================================================

/// Configuration for spawning a [`spawn_persistent_cache_worker`].
pub struct WorkerConfig {
    /// Writer to drain. Must outlive the worker.
    pub writer: Arc<PersistentResultCacheWriter>,
    /// I/O backend. The worker uses `write_atomic` / `remove` / `clear` /
    /// `load`. Must outlive the worker.
    pub io: Arc<dyn PersistentCacheIo>,
    /// Optional encryption cipher. When `Some`, the worker encrypts every
    /// payload before framing; when `None`, the payload is written in
    /// plaintext.
    pub cipher: Option<Arc<AesCipher>>,
    /// Stale-check guard. The worker re-checks that the queued entry's
    /// `(key, key_generation, clear_generation)` is still the latest snapshot
    /// before publishing. When the guard reports a stale match, the op is
    /// skipped and counted as `StoreStale`.
    pub staleness: Arc<dyn StalenessGuard>,
    /// Maximum stale-checks to perform per op. Prevents unbounded work on a
    /// single contended key.
    pub max_staleness_retries: u32,
    /// Optional completion signal. When `Some`, the worker sends `()`
    /// immediately before exiting. The receiver (held by the engine) uses
    /// `recv_timeout(remaining_deadline)` to wait for a bounded shutdown.
    /// Send errors (receiver dropped) are ignored.
    pub completion: Option<mpsc::Sender<()>>,
}

/// Hook for the worker to validate that a drained op is still current. The
/// production implementation reads the writer's per-key generation under the
/// queue lock; tests can use a custom implementation to simulate races.
pub trait StalenessGuard: Send + Sync {
    /// Return `true` when `(key, key_generation)` is still the latest known
    /// generation for `key` AND `clear_generation` matches the current
    /// global clear generation.
    fn is_current(&self, key: u64, key_generation: u64, clear_generation: u64) -> bool;
}

/// A [`StalenessGuard`] backed by the writer's own queue. The implementation
/// locks the queue, checks the per-key generation and the clear generation,
/// and returns whether the op is still the latest. Cheap when uncontended.
pub struct WriterStalenessGuard {
    writer: Arc<PersistentResultCacheWriter>,
}

impl WriterStalenessGuard {
    pub fn new(writer: Arc<PersistentResultCacheWriter>) -> Self {
        Self { writer }
    }
}

impl StalenessGuard for WriterStalenessGuard {
    fn is_current(&self, key: u64, key_generation: u64, clear_generation: u64) -> bool {
        let guard = self.writer.inner.lock().expect("writer mutex poisoned");
        if guard.state.clear_generation != clear_generation {
            return false;
        }
        match guard.state.key_generations.get(&key) {
            Some(g) => *g == key_generation,
            None => true, // no later op for this key, original is still current
        }
    }
}

/// Spawn the persistent-cache publication worker. The worker thread exits
/// when the writer is shut down AND the queue is empty.
pub fn spawn_persistent_cache_worker(config: WorkerConfig) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || run_persistent_cache_worker(config))
}

fn run_persistent_cache_worker(config: WorkerConfig) {
    let WorkerConfig {
        writer,
        io,
        cipher,
        staleness,
        max_staleness_retries,
        completion,
    } = config;
    loop {
        let drained = match writer.drain_one() {
            Some(d) => d,
            None => {
                // shutdown + queue empty — emit the completion signal so the
                // engine's deadline-bounded shutdown can return promptly.
                if let Some(tx) = completion {
                    let _ = tx.send(());
                }
                return;
            }
        };
        // Re-validate staleness after the lock is released. We give the
        // queue a brief moment to settle, but bounded by `max_staleness_retries`
        // to prevent unbounded spinning under heavy contention.
        let mut attempts = 0u32;
        let is_current = loop {
            if staleness.is_current(
                drained.key,
                drained.key_generation,
                drained.clear_generation,
            ) {
                break true;
            }
            if attempts >= max_staleness_retries {
                break false;
            }
            attempts += 1;
            std::thread::yield_now();
        };
        if !is_current {
            if matches!(drained.op, PendingCacheOp::Store(_)) {
                writer.record_outcome(DrainOutcome::StoreStale);
            }
            // Removes and Clears are non-staleable for the purposes of
            // generation mismatch: a newer Remove wins anyway.
            continue;
        }
        // Final staleness check just before the publish point. Closes the
        // race where `enqueue_clear` (or a per-key invalidate) advances the
        // writer's generation after the top-of-loop check but before the
        // I/O. With a locked final check, a generation bump that happens
        // between the two checks will either:
        //   * happen before the second check — and abort this write, or
        //   * happen after the second check — and the write is allowed
        //     because the user's intent was captured before the shutdown.
        // The I/O itself is uninterruptible; an in-flight `write_atomic`
        // either completes or fails.
        if !staleness.is_current(
            drained.key,
            drained.key_generation,
            drained.clear_generation,
        ) {
            if matches!(drained.op, PendingCacheOp::Store(_)) {
                writer.record_outcome(DrainOutcome::StoreStale);
            }
            continue;
        }
        match drained.op {
            PendingCacheOp::Store(entry) => {
                writer.writes_in_flight.fetch_add(1, Ordering::Relaxed);
                let payload = match (entry.payload_factory)() {
                    Some(p) => p,
                    None => {
                        writer.record_outcome(DrainOutcome::StoreErrored);
                        writer.writes_in_flight.fetch_sub(1, Ordering::Relaxed);
                        continue;
                    }
                };
                let context = PersistContext {
                    identity: PersistentCacheIdentity {
                        table_id: entry.table_id,
                        schema_id: entry.schema_id,
                        logical_generation: entry.run_generation,
                    },
                    key: entry.key,
                    entry_generation: entry.entry_generation,
                };
                let bytes = match encode_persisted_entry(context, &payload, cipher.as_deref()) {
                    Ok(b) => b,
                    Err(_) => {
                        writer.record_outcome(DrainOutcome::StoreErrored);
                        writer.writes_in_flight.fetch_sub(1, Ordering::Relaxed);
                        continue;
                    }
                };
                let outcome = match io.write_atomic(entry.key, &bytes) {
                    Ok(()) => DrainOutcome::StorePublished,
                    Err(_) => DrainOutcome::StoreErrored,
                };
                writer.record_outcome(outcome);
                writer.writes_in_flight.fetch_sub(1, Ordering::Relaxed);
            }
            PendingCacheOp::Remove => {
                writer.writes_in_flight.fetch_add(1, Ordering::Relaxed);
                let outcome = match io.remove(drained.key) {
                    Ok(()) => DrainOutcome::RemoveApplied,
                    Err(_) => DrainOutcome::RemoveErrored,
                };
                writer.record_outcome(outcome);
                writer.writes_in_flight.fetch_sub(1, Ordering::Relaxed);
            }
            PendingCacheOp::Clear => {
                writer.writes_in_flight.fetch_add(1, Ordering::Relaxed);
                let outcome = match io.clear() {
                    Ok(()) => DrainOutcome::ClearApplied,
                    Err(_) => DrainOutcome::ClearErrored,
                };
                writer.record_outcome(outcome);
                writer.writes_in_flight.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

/// Try to receive a worker completion signal with a bounded timeout. Used by
/// `shutdown_persistent_cache(deadline)` so the engine returns even if the
/// worker is blocked in filesystem I/O. Returns `true` if the signal was
/// received (worker has exited), `false` on timeout.
pub fn recv_completion_with_timeout(rx: &mpsc::Receiver<()>, timeout: Duration) -> bool {
    matches!(rx.recv_timeout(timeout), Ok(()))
}
