//! The engine tying the write and read paths together.
//!
//! Sub-ms writes: [`Table::put`] appends to the WAL **without fsyncing**, upserts
//! the skip-list memtable, and updates the in-memory HOT index + secondary
//! indexes. A batch-driven [`Table::commit`] does the group `fsync` and bumps the
//! epoch. [`Table::flush`] commits, drains the memtable into an immutable sorted
//! run, and rotates the WAL. Reads merge versions across the live memtable and
//! all sorted runs ([`Table::get`], [`Table::visible_rows`]).

use crate::columnar;
use crate::cursor::NativePageCursor;
use crate::encryption::Kek;
use crate::encryption::DEK_LEN;
use crate::epoch::{Epoch, EpochAuthority, Snapshot};
use crate::global_idx;
use crate::index::{AnnIndex, BitmapIndex, ColumnLearnedRange, FmIndex, HotIndex, SparseIndex};
use crate::manifest::{self, Manifest, RunRef};
use crate::memtable::{Memtable, Row, Value};
use crate::mutable_run::MutableRun;
use crate::row_id_set::RowIdSet;
use crate::rowid::{RowId, RowIdAllocator};
use crate::schema::{AlterColumn, ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use crate::sorted_run::{RunReader, RunWriter};
use crate::txn::{GroupCommit, OwnedRow};
use crate::wal::{Op, SharedWal, Wal};
use crate::{MongrelError, Result};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use zeroize::Zeroizing;

pub const WAL_DIR: &str = "_wal";
pub const RUNS_DIR: &str = "_runs";
pub const CACHE_DIR: &str = "_cache";
pub const META_DIR: &str = "_meta";
pub const RCACHE_DIR: &str = "_rcache";
pub const KEYS_FILENAME: &str = "keys";
pub const SCHEMA_FILENAME: &str = "schema.json";
const DEFAULT_SYNC_BYTE_THRESHOLD: u64 = 0; // manual commit only (pure group commit)
pub(crate) const PAGE_CACHE_CAPACITY: u64 = 64 * 1024 * 1024; // 64 MiB shared page cache
pub(crate) const DECODED_CACHE_CAPACITY: u64 = 64 * 1024 * 1024; // 64 MiB shared decoded-page cache (Phase 15.4)
/// Default byte watermark at which the PMA mutable-run tier spills to an
/// immutable `.sr` sorted run (Phase 11.1). Coalesces many small flushes into
/// one larger run so the read path merges fewer readers.
const DEFAULT_MUTABLE_RUN_SPILL_BYTES: u64 = 8 * 1024 * 1024;

/// Engine-managed `AUTO_INCREMENT` counter state for a table (present iff the
/// schema declares an `AUTO_INCREMENT` primary key).
///
/// `next` is the next value to hand out (1-based, monotonic, never reused). It
/// is `0` while *unseeded* — the counter has never been advanced (fresh table or
/// a legacy manifest predating `auto_inc_next`). When `seeded` is `false` the
/// first allocation scans `max(PK)` over all visible rows so the counter never
/// collides with pre-existing rows; a value of `0` after seeding never happens
/// (ids are never 0). The manifest persists `next` only when `seeded`, so a
/// reopen that reads `auto_inc_next > 0` is authoritative.
///
/// `seeded == false` but `next > 0` is a transient recovery-only state: WAL
/// replay may bump `next` past replayed ids without marking it seeded, so the
/// scan still runs to cover rows that were already flushed to sorted runs.
#[derive(Clone, Copy, Debug)]
struct AutoIncState {
    column_id: u16,
    next: i64,
    seeded: bool,
}

type FilledAutoIncRow = (Vec<(u16, Value)>, Option<i64>);

/// Resolve the auto-increment column (if any) from a schema into initial
/// counter state. Always called after [`crate::schema::Schema::validate_auto_increment`].
fn resolve_auto_inc(schema: &Schema) -> Option<AutoIncState> {
    schema.auto_increment_column().map(|c| AutoIncState {
        column_id: c.id,
        next: 0,
        seeded: false,
    })
}

/// An open MongrelDB table.
pub struct Table {
    dir: PathBuf,
    table_id: u64,
    wal: WalSink,
    memtable: Memtable,
    /// PMA-backed mutable-run LSM tier (Phase 11.1). A flush drains the
    /// memtable into this in-memory sorted tier instead of immediately writing
    /// a `.sr` run; once it crosses `mutable_run_spill_bytes` it spills to an
    /// immutable run. Purely in-memory — rebuilt from WAL replay on reopen.
    mutable_run: MutableRun,
    /// Byte watermark controlling when `mutable_run` spills to a sorted run.
    mutable_run_spill_bytes: u64,
    /// Zstd compression level for compaction output (Phase 18.1: default 3;
    /// higher = better ratio but slower compaction).
    compaction_zstd_level: i32,
    allocator: RowIdAllocator,
    epoch: Arc<EpochAuthority>,
    /// Manifest-endorsed epoch at open; used to seed the (shared) epoch
    /// authority on a fresh open. Updated whenever the manifest is persisted.
    persisted_epoch: u64,
    schema: Schema,
    hot: HotIndex,
    /// Table Key-Encryption Key (Argon2id+HKDF from the passphrase). Each run
    /// stores a fresh DEK wrapped by this KEK (see §7). `None` when plaintext.
    kek: Option<Arc<Kek>>,
    /// Per-column indexable-encryption keys + scheme (Phase 10.2) for every
    /// ENCRYPTED_INDEXABLE column, derived deterministically from the KEK so
    /// tokens are identical across runs. Empty when the table is plaintext.
    column_keys: HashMap<u16, ([u8; 32], u8)>,
    run_refs: Vec<RunRef>,
    /// Runs superseded by compaction, kept on disk for snapshot retention until
    /// `gc()` reaps them (spec §6.4). Persisted in the manifest (`retiring`).
    retiring: Vec<crate::manifest::RetiredRun>,
    next_run_id: u64,
    sync_byte_threshold: u64,
    /// Next transaction id to assign to a single-table auto-commit txn
    /// (`put`/`delete` then `commit`). 0 is reserved for [`wal::SYSTEM_TXN_ID`].
    /// The Database transaction layer (P2.5) assigns these globally; the
    /// single-table path uses this local counter.
    current_txn_id: u64,
    bitmap: HashMap<u16, BitmapIndex>,
    ann: HashMap<u16, AnnIndex>,
    fm: HashMap<u16, FmIndex>,
    sparse: HashMap<u16, SparseIndex>,
    /// Per-column learned (PGM) range indexes for `IndexKind::LearnedRange`
    /// columns, built from the single sorted run.
    learned_range: HashMap<u16, ColumnLearnedRange>,
    /// Reverse primary-key map for HOT cleanup on row-id deletes.
    pk_by_row: HashMap<RowId, Vec<u8>>,
    /// Refcounted pinned read snapshots (epoch → count); compaction must not GC
    /// versions an active snapshot still needs.
    pinned: BTreeMap<Epoch, usize>,
    /// Live (non-deleted) row count — maintained incrementally for O(1)
    /// `Table::count()` without a scan.
    pub(crate) live_count: u64,
    /// Uniform reservoir sample of row ids for approximate analytics
    /// (Phase 8.2). Maintained incrementally on insert; repopulated on open.
    reservoir: crate::reservoir::Reservoir,
    /// True once any row has been deleted. The incremental aggregate cache
    /// (Phase 8.3) is only valid for append-only tables, so a single delete
    /// permanently disables incremental maintenance for this table.
    had_deletes: bool,
    /// Incremental aggregate cache (Phase 8.3): caller-supplied key → the
    /// mergeable aggregate state, the row-id watermark it covers, and the
    /// epoch. A re-query after more inserts processes only the delta and merges.
    agg_cache: HashMap<u64, CachedAgg>,
    /// The manifest epoch the on-disk `_idx/global.idx` checkpoint covers (0 if
    /// there is no checkpoint). Updated by [`Table::checkpoint_indexes`]; persisted
    /// in the manifest so reopen loads the checkpoint instead of rebuilding.
    global_idx_epoch: u64,
    /// False when the live in-memory indexes are known to be incomplete (e.g.
    /// after [`Table::bulk_load_columns`], which bypasses per-row indexing). A
    /// flush in that state must NOT checkpoint; reopen rebuilds complete indexes
    /// from the runs and resets this to true.
    indexes_complete: bool,
    /// Highest epoch whose data is durable in a sorted run (spec §7.1). Recovery
    /// skips replaying WAL records whose commit epoch is `<= flushed_epoch`.
    flushed_epoch: u64,
    /// Shared, MVCC content-addressed page cache (Phase 9.2). Fed by every
    /// `RunReader::read_page` so all readers share raw (decrypted) page bytes.
    page_cache: Arc<parking_lot::Mutex<crate::cache::PageCache>>,
    /// Global snapshot-retention registry shared across all tables in a
    /// `Database`. Single-table direct opens get a private one.
    snapshots: Arc<crate::retention::SnapshotRegistry>,
    /// Cross-table commit serializer (see [`SharedCtx::commit_lock`]).
    commit_lock: Arc<parking_lot::Mutex<()>>,
    /// Shared decoded-page cache (Phase 15.4): the post-decompress/decrypt typed
    /// page, so repeat scans skip decode. Keyed by `(run_id, column_id, page)`.
    decoded_cache: Arc<parking_lot::Mutex<crate::cache::DecodedPageCache>>,
    /// Table-level result cache (Phase 19.1): `canonical_query_key(conditions,
    /// projection, epoch)` → the survivor columns as typed `NativeColumn`s. Shared
    /// by the native `Condition` API and (via `query_cached`) the tool-call path,
    /// which previously had no caching (only the SQL `MongrelSession` cache did).
    /// Hardening (c): epoch is no longer in the key; instead, a `commit()`
    /// invalidates only entries whose footprint or condition-columns intersect
    /// the committed mutations, tracked in `pending_delete_rids` and
    /// `pending_put_cols`.
    result_cache: Arc<parking_lot::Mutex<ResultCache>>,
    /// WAL DEK (for frame-level encryption). None for plaintext tables.
    wal_dek: Option<Zeroizing<[u8; DEK_LEN]>>,
    /// RowIds deleted since the last `commit()` — used by fine-grained cache
    /// invalidation to check footprint intersection.
    pending_delete_rids: roaring::RoaringBitmap,
    /// Column IDs touched by `put`/`put_batch` since the last `commit()` — used
    /// by conservative insert-newly-matches invalidation.
    pending_put_cols: std::collections::HashSet<u16>,
    /// B1/B2: rows staged by `put`/`put_batch` on a mounted (shared-WAL) table
    /// but not yet applied to the memtable. They are re-stamped to the real
    /// assigned epoch in `commit` (never a speculative `visible+1`), so a
    /// concurrent reader can never observe them before their commit epoch.
    /// Always empty on a standalone (private-WAL) table, which applies inline.
    pending_rows: Vec<Row>,
    pending_rows_auto_inc: Vec<bool>,
    /// B1/B2: tombstones staged on a mounted table, applied at the assigned
    /// epoch in `commit` (mirror of `pending_rows`).
    pending_dels: Vec<RowId>,
    /// B1/B2: truncate staged on a mounted table, applied at the assigned epoch
    /// in `commit`; standalone tables also defer the physical clear until after
    /// the private WAL is fsynced.
    pending_truncate: Option<Epoch>,
    /// Engine-managed `AUTO_INCREMENT` counter (`None` for tables without an
    /// auto-increment primary key). See [`AutoIncState`].
    auto_inc: Option<AutoIncState>,
}

// `Table` is `Sync`: every field is either plain data, an `Arc`, a `Vec`/`HashMap`
// of `Sync` data, or a thread-safe interior-mutability cell (`parking_lot::Mutex`,
// `crossbeam`/`epoch` Arc-shared caches). The only `RefCell`-based type was
// `FmIndex` (lazy rebuild of the BWT), which now uses a `Mutex`, so a `&Table`
// can be safely shared across read threads (concurrent mutation still requires
// the caller's `Mutex<Table>`).
const _: () = {
    const fn assert_sync<T: ?Sized + Sync>() {}
    assert_sync::<Table>();
};

/// A cached query result — either survivor `Row`s (the tool-call/`query` path)
/// or typed survivor columns (the pushdown/`query_columns_native` path). One
/// canonical key maps to exactly one variant (a `query` with no projection vs a
/// `query_columns_native` with a specific projection produce different keys), so
/// there is no representation collision.
enum CachedData {
    Rows(Arc<Vec<Row>>),
    Columns(Arc<Vec<(u16, columnar::NativeColumn)>>),
}

impl CachedData {
    fn approx_bytes(&self) -> u64 {
        match self {
            CachedData::Rows(r) => r.iter().map(|r| r.estimated_bytes()).sum::<u64>(),
            CachedData::Columns(c) => c
                .iter()
                .map(|(_, c)| c.approx_bytes())
                .sum::<u64>()
                .saturating_add(c.len() as u64 * 16),
        }
    }
}

/// A cached entry carrying the survivor `RowId` **footprint** (for precise
/// delete-based invalidation) and the condition column IDs (for conservative
/// insert-based invalidation). Hardening (c).
struct CachedEntry {
    data: CachedData,
    footprint: roaring::RoaringBitmap,
    condition_cols: Vec<u16>,
}

/// Size-bounded **access-order LRU** result cache (Phase 19.1 + hardening (a)).
/// Every `get_*` promotes the key to the back (most-recently-used); eviction
/// pops from the front (least-recently-used) — a true LRU, not FIFO.
///
/// Hardening (b): an optional on-disk persistent tier (`dir = Some(_)`). On a
/// memory miss, the cache tries disk before falling through to re-resolution.
/// On `insert`, the entry is also written to disk atomically (write + fsync +
/// rename). On `invalidate`/`clear`, the matching disk files are deleted. On
/// `Table::open`, existing disk entries are pre-loaded so fine-grained invalidation
/// resumes across restart.
struct ResultCache {
    entries: std::collections::HashMap<u64, CachedEntry>,
    order: std::collections::VecDeque<u64>,
    bytes: u64,
    max_bytes: u64,
    dir: Option<std::path::PathBuf>,
    #[allow(dead_code)]
    cache_dek: Option<Zeroizing<[u8; DEK_LEN]>>,
}

/// Serialised form of a [`CachedEntry`] for the persistent on-disk tier (b).
#[derive(serde::Serialize, serde::Deserialize)]
struct SerializedEntry {
    condition_cols: Vec<u16>,
    footprint_bits: Vec<u32>,
    data: SerializedData,
}

#[derive(serde::Serialize, serde::Deserialize)]
enum SerializedData {
    Rows(Vec<Row>),
    Columns(Vec<(u16, columnar::NativeColumn)>),
}

impl SerializedEntry {
    fn from_entry(entry: &CachedEntry) -> Self {
        let footprint_bits: Vec<u32> = entry.footprint.iter().collect();
        let data = match &entry.data {
            CachedData::Rows(r) => SerializedData::Rows((**r).clone()),
            CachedData::Columns(c) => SerializedData::Columns((**c).clone()),
        };
        Self {
            condition_cols: entry.condition_cols.clone(),
            footprint_bits,
            data,
        }
    }

    fn into_entry(self) -> Option<CachedEntry> {
        let footprint: roaring::RoaringBitmap = self.footprint_bits.into_iter().collect();
        let data = match self.data {
            SerializedData::Rows(r) => CachedData::Rows(Arc::new(r)),
            SerializedData::Columns(c) => {
                // Validate deserialized columns (hardening (b)): reject corrupt
                // data instead of panicking on access.
                if !c.iter().all(|(_, col)| col.validate()) {
                    return None;
                }
                CachedData::Columns(Arc::new(c))
            }
        };
        Some(CachedEntry {
            data,
            footprint,
            condition_cols: self.condition_cols,
        })
    }
}

impl ResultCache {
    const DEFAULT_MAX_BYTES: u64 = 256 * 1024 * 1024;

    fn new() -> Self {
        Self::with_max_bytes(Self::DEFAULT_MAX_BYTES)
    }

    fn with_max_bytes(max_bytes: u64) -> Self {
        Self {
            entries: std::collections::HashMap::new(),
            order: std::collections::VecDeque::new(),
            bytes: 0,
            max_bytes,
            dir: None,
            cache_dek: None,
        }
    }

    fn with_dir(mut self, dir: std::path::PathBuf) -> Self {
        let _ = std::fs::create_dir_all(&dir);
        self.dir = Some(dir);
        self
    }

    fn with_cache_dek(mut self, dek: Option<Zeroizing<[u8; DEK_LEN]>>) -> Self {
        self.cache_dek = dek;
        self
    }

    fn disk_path(&self, key: u64) -> Option<std::path::PathBuf> {
        self.dir.as_ref().map(|d| d.join(format!("{key:016x}.bin")))
    }

    /// Atomically write `entry` to disk (write + rename). Best-effort: silently
    /// ignores I/O errors (the in-memory cache is authoritative; the cache is
    /// disposable — missing/stale files fall through to re-resolution).
    fn store_to_disk(&self, key: u64, entry: &CachedEntry) {
        let Some(path) = self.disk_path(key) else {
            return;
        };
        let serialized = match bincode::serialize(&SerializedEntry::from_entry(entry)) {
            Ok(s) => s,
            Err(_) => return,
        };
        // Encrypt if a cache DEK is present.
        let on_disk = if let Some(dek) = &self.cache_dek {
            match self.encrypt_cache(&serialized, dek) {
                Some(b) => b,
                None => return,
            }
        } else {
            serialized
        };
        let tmp = path.with_extension("tmp");
        use std::io::Write;
        let write = || -> std::io::Result<()> {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&on_disk)?;
            f.flush()?;
            Ok(())
        };
        if write().is_err() {
            let _ = std::fs::remove_file(&tmp);
            return;
        }
        let _ = std::fs::rename(&tmp, &path);
    }

    /// Try loading `key` from disk. Returns `None` on miss or error.
    fn load_from_disk(&self, key: u64) -> Option<CachedEntry> {
        let path = self.disk_path(key)?;
        let bytes = std::fs::read(&path).ok()?;
        let plaintext = if let Some(dek) = &self.cache_dek {
            self.decrypt_cache(&bytes, dek)?
        } else {
            bytes
        };
        let serialized: SerializedEntry = bincode::deserialize(&plaintext).ok()?;
        serialized.into_entry()
    }

    /// Delete the on-disk file for `key` if it exists. Best-effort.
    fn remove_from_disk(&self, key: u64) {
        if let Some(path) = self.disk_path(key) {
            let _ = std::fs::remove_file(&path);
        }
    }

    /// Encrypt cache data: `[nonce: 12B][ciphertext + GCM tag]`.
    #[cfg(feature = "encryption")]
    fn encrypt_cache(&self, plaintext: &[u8], dek: &Zeroizing<[u8; DEK_LEN]>) -> Option<Vec<u8>> {
        use crate::encryption::Cipher;
        let cipher = crate::encryption::AesCipher::new(&dek[..]).ok()?;
        let mut nonce = [0u8; 12];
        crate::encryption::fill_random(&mut nonce);
        let ct = cipher.encrypt_page(&nonce, plaintext).ok()?;
        let mut out = Vec::with_capacity(12 + ct.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ct);
        Some(out)
    }

    #[cfg(not(feature = "encryption"))]
    fn encrypt_cache(&self, _plaintext: &[u8], _dek: &Zeroizing<[u8; DEK_LEN]>) -> Option<Vec<u8>> {
        None
    }

    /// Decrypt cache data: reads nonce from first 12 bytes.
    #[cfg(feature = "encryption")]
    fn decrypt_cache(&self, bytes: &[u8], dek: &Zeroizing<[u8; DEK_LEN]>) -> Option<Vec<u8>> {
        use crate::encryption::Cipher;
        if bytes.len() < 28 {
            return None;
        }
        let cipher = crate::encryption::AesCipher::new(&dek[..]).ok()?;
        let nonce: [u8; 12] = bytes[..12].try_into().ok()?;
        let ct = &bytes[12..];
        cipher.decrypt_page(&nonce, ct).ok()
    }

    #[cfg(not(feature = "encryption"))]
    fn decrypt_cache(&self, _bytes: &[u8], _dek: &Zeroizing<[u8; DEK_LEN]>) -> Option<Vec<u8>> {
        None
    }

    /// Scan the cache directory and pre-load all entries into memory. Called
    /// once on `Table::open`. Best-effort: corrupt/unreadable files are deleted.
    fn load_persistent(&mut self) {
        let Some(dir) = self.dir.as_ref().cloned() else {
            return;
        };
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            // Clean up orphan .tmp files from crashed store_to_disk calls.
            if path.extension().and_then(|e| e.to_str()) == Some("tmp") {
                let _ = std::fs::remove_file(&path);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("bin") {
                continue;
            }
            let stem = match path.file_stem().and_then(|s| s.to_str()) {
                Some(s) => s,
                None => continue,
            };
            let key = match u64::from_str_radix(stem, 16) {
                Ok(k) => k,
                Err(_) => continue,
            };
            let bytes = match std::fs::read(&path) {
                Ok(b) => b,
                Err(_) => continue,
            };
            // Decrypt if cache DEK is present.
            let plaintext = if let Some(dek) = &self.cache_dek {
                match self.decrypt_cache(&bytes, dek) {
                    Some(p) => p,
                    None => {
                        let _ = std::fs::remove_file(&path);
                        continue;
                    }
                }
            } else {
                bytes
            };
            match bincode::deserialize::<SerializedEntry>(&plaintext) {
                Ok(serialized) => {
                    if let Some(entry) = serialized.into_entry() {
                        self.bytes = self.bytes.saturating_add(entry.data.approx_bytes());
                        self.entries.insert(key, entry);
                        self.order.push_back(key);
                    } else {
                        let _ = std::fs::remove_file(&path);
                    }
                }
                Err(_) => {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
        self.evict();
    }

    fn set_max_bytes(&mut self, max_bytes: u64) {
        self.max_bytes = max_bytes;
        self.evict();
    }

    /// Promote `key` to most-recently-used position (back of the deque).
    fn touch(&mut self, key: u64) {
        self.order.retain(|k| *k != key);
        self.order.push_back(key);
    }

    fn get_rows(&mut self, key: u64) -> Option<Arc<Vec<Row>>> {
        let res = self.entries.get(&key).and_then(|e| match &e.data {
            CachedData::Rows(r) => Some(r.clone()),
            CachedData::Columns(_) => None,
        });
        if res.is_some() {
            self.touch(key);
            return res;
        }
        // Memory miss → try the persistent tier (b).
        if let Some(entry) = self.load_from_disk(key) {
            let res = match &entry.data {
                CachedData::Rows(r) => Some(r.clone()),
                CachedData::Columns(_) => None,
            };
            if res.is_some() {
                let approx = entry.data.approx_bytes();
                self.bytes = self.bytes.saturating_add(approx);
                self.entries.insert(key, entry);
                self.order.push_back(key);
                self.evict();
                return res;
            }
        }
        None
    }

    fn get_columns(&mut self, key: u64) -> Option<Arc<Vec<(u16, columnar::NativeColumn)>>> {
        let res = self.entries.get(&key).and_then(|e| match &e.data {
            CachedData::Columns(c) => Some(c.clone()),
            CachedData::Rows(_) => None,
        });
        if res.is_some() {
            self.touch(key);
            return res;
        }
        // Memory miss → try the persistent tier (b).
        if let Some(entry) = self.load_from_disk(key) {
            let res = match &entry.data {
                CachedData::Columns(c) => Some(c.clone()),
                CachedData::Rows(_) => None,
            };
            if res.is_some() {
                let approx = entry.data.approx_bytes();
                self.bytes = self.bytes.saturating_add(approx);
                self.entries.insert(key, entry);
                self.order.push_back(key);
                self.evict();
                return res;
            }
        }
        None
    }

    fn insert(&mut self, key: u64, entry: CachedEntry) {
        let approx = entry.data.approx_bytes();
        if self.entries.remove(&key).is_some() {
            self.order.retain(|k| *k != key);
            self.bytes = self.entries.values().map(|e| e.data.approx_bytes()).sum();
        }
        // Write to the persistent tier (b) before memory insert.
        self.store_to_disk(key, &entry);
        self.bytes = self.bytes.saturating_add(approx);
        self.entries.insert(key, entry);
        self.order.push_back(key);
        self.evict();
    }

    /// Fine-grained invalidation (hardening (c)). Drop only entries that are
    /// actually affected by the committed mutations:
    /// - **Delete path**: if `delete_rids` intersects an entry's footprint, a
    ///   survivor was deleted → stale. If the footprint is empty (multi-run or
    ///   non-empty memtable — we couldn't resolve it), **any** delete
    ///   conservatively invalidates the entry (correctness over precision).
    /// - **Insert path**: if `put_cols` intersects an entry's `condition_cols`,
    ///   a newly-inserted row might match the query → conservatively stale.
    fn invalidate(
        &mut self,
        delete_rids: &roaring::RoaringBitmap,
        put_cols: &std::collections::HashSet<u16>,
    ) {
        if self.entries.is_empty() {
            return;
        }
        let has_deletes = !delete_rids.is_empty();
        let to_remove: std::collections::HashSet<u64> = self
            .entries
            .iter()
            .filter(|(_, e)| {
                let delete_hit = if e.footprint.is_empty() {
                    has_deletes
                } else {
                    e.footprint.intersection_len(delete_rids) > 0
                };
                let col_hit = e.condition_cols.iter().any(|c| put_cols.contains(c));
                delete_hit || col_hit
            })
            .map(|(&k, _)| k)
            .collect();
        for key in &to_remove {
            if let Some(e) = self.entries.remove(key) {
                self.bytes = self.bytes.saturating_sub(e.data.approx_bytes());
            }
            self.remove_from_disk(*key);
        }
        if !to_remove.is_empty() {
            self.order.retain(|k| !to_remove.contains(k));
        }
    }

    fn clear(&mut self) {
        // Delete all persistent files (b).
        if let Some(dir) = &self.dir {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|e| e.to_str()) == Some("bin") {
                        let _ = std::fs::remove_file(&path);
                    }
                }
            }
        }
        self.entries.clear();
        self.order.clear();
        self.bytes = 0;
    }

    fn evict(&mut self) {
        while self.bytes > self.max_bytes {
            let Some(k) = self.order.pop_front() else {
                break;
            };
            if let Some(e) = self.entries.remove(&k) {
                self.bytes = self.bytes.saturating_sub(e.data.approx_bytes());
                // Also delete the disk file (hardening (b)): an evicted entry's
                // disk file must not survive, or invalidate() — which only scans
                // in-memory entries — would miss it and allow a stale disk hit.
                self.remove_from_disk(k);
            }
        }
    }
}

/// Derive per-column indexable-encryption keys (Phase 10.2) for every
/// ENCRYPTED_INDEXABLE column from the KEK. Scheme is `OPE_RANGE` if the column
/// has a `LearnedRange` index, else `HMAC_EQ` (equality). Keys are derived
/// deterministically from the KEK so tokens are stable across runs. Empty when
/// the table is plaintext (no KEK).
/// Derive WAL and cache DEKs from the KEK (None when no encryption).
type DekaOpt = Option<Zeroizing<[u8; DEK_LEN]>>;

fn derive_subkeys(kek: Option<&Kek>, _table_id: u64) -> (DekaOpt, DekaOpt) {
    let _ = kek;
    #[cfg(feature = "encryption")]
    {
        if let Some(k) = kek {
            return (
                Some(k.derive_table_wal_key(_table_id)),
                Some(k.derive_cache_key()),
            );
        }
    }
    (None, None)
}

/// Create a boxed cipher from a DEK (encryption feature only).
#[cfg(feature = "encryption")]
fn make_cipher(dek: &Zeroizing<[u8; DEK_LEN]>) -> Box<dyn crate::encryption::Cipher> {
    Box::new(crate::encryption::AesCipher::new(&dek[..]).expect("DEK is 32 bytes"))
}

#[cfg(not(feature = "encryption"))]
fn make_cipher(_dek: &Zeroizing<[u8; DEK_LEN]>) -> Box<dyn crate::encryption::Cipher> {
    Box::new(crate::encryption::PlaintextCipher)
}

fn build_column_keys(kek: Option<&Kek>, schema: &Schema) -> HashMap<u16, ([u8; 32], u8)> {
    let Some(kek) = kek else {
        return HashMap::new();
    };
    #[cfg(feature = "encryption")]
    {
        use crate::encryption::{SCHEME_HMAC_EQ, SCHEME_OPE_RANGE};
        schema
            .columns
            .iter()
            .filter(|c| c.flags.contains(ColumnFlags::ENCRYPTED_INDEXABLE))
            .map(|c| {
                let scheme = if schema
                    .indexes
                    .iter()
                    .any(|i| i.column_id == c.id && i.kind == IndexKind::LearnedRange)
                {
                    SCHEME_OPE_RANGE
                } else {
                    SCHEME_HMAC_EQ
                };
                let key: [u8; 32] = *kek.derive_column_key(c.id);
                (c.id, (key, scheme))
            })
            .collect()
    }
    #[cfg(not(feature = "encryption"))]
    {
        let _ = (kek, schema);
        HashMap::new()
    }
}

/// Shared services injected into every `Table` owned by a `Database`: one epoch
/// authority (single commit clock), one raw-page cache, one decoded-page cache,
/// one snapshot-retention registry, and the DB-wide KEK. A directly-opened
/// single table builds a private `SharedCtx` of its own.
pub(crate) struct SharedCtx {
    pub epoch: Arc<EpochAuthority>,
    pub page_cache: Arc<parking_lot::Mutex<crate::cache::PageCache>>,
    pub decoded_cache: Arc<parking_lot::Mutex<crate::cache::DecodedPageCache>>,
    pub snapshots: Arc<crate::retention::SnapshotRegistry>,
    pub kek: Option<Arc<Kek>>,
    /// Serializes the commit critical section across all tables sharing this
    /// context so the dual-counter's in-order-publish invariant holds: the
    /// assigned ticket is reserved, the WAL fsynced, the manifest persisted,
    /// and `visible` published as one atomic unit. P3 replaces this with the
    /// bounded validate-first sequencer + group commit (overlapping fsync).
    pub commit_lock: Arc<parking_lot::Mutex<()>>,
    /// B1: when `Some`, the table is mounted in a `Database` and routes every
    /// write through the one shared WAL (no private `_wal/` dir is created).
    /// `None` for a directly-opened standalone table, which keeps a private WAL.
    pub shared: Option<SharedWalCtx>,
}

/// Handles a mounted table needs to write to the database's single shared WAL
/// (B1): the WAL itself, the group-commit coordinator + poison flag (so a
/// single-table commit honors the same durability/§9.3e semantics as a cross-
/// table txn), and the shared txn-id allocator (so auto-commit ids never alias
/// cross-table ones in the merged log).
#[derive(Clone)]
pub(crate) struct SharedWalCtx {
    pub wal: Arc<parking_lot::Mutex<SharedWal>>,
    pub group: Arc<GroupCommit>,
    pub poisoned: Arc<AtomicBool>,
    pub txn_ids: Arc<parking_lot::Mutex<u64>>,
}

/// Where a table's WAL records go. A standalone table owns a `Private` WAL; a
/// `Database`-mounted table writes to the one `Shared` WAL (B1).
enum WalSink {
    Private(Wal),
    Shared(SharedWalCtx),
}

impl SharedCtx {
    /// Build a fresh private (standalone) context. `cache_dir = Some(_)` enables
    /// on-disk page cache persistence (single-table direct open); `None` keeps
    /// it in-memory (shared across tables in a `Database`).
    pub(crate) fn new(kek: Option<Arc<Kek>>, cache_dir: Option<PathBuf>) -> Self {
        let mut cache = crate::cache::PageCache::new(PAGE_CACHE_CAPACITY);
        if let Some(d) = cache_dir {
            cache = cache.with_persistence(d);
        }
        Self {
            epoch: Arc::new(EpochAuthority::new(0)),
            page_cache: Arc::new(parking_lot::Mutex::new(cache)),
            decoded_cache: Arc::new(parking_lot::Mutex::new(
                crate::cache::DecodedPageCache::new(DECODED_CACHE_CAPACITY),
            )),
            snapshots: Arc::new(crate::retention::SnapshotRegistry::new()),
            kek,
            commit_lock: Arc::new(parking_lot::Mutex::new(())),
            shared: None,
        }
    }
}

impl Table {
    pub fn create(dir: impl AsRef<Path>, schema: Schema, table_id: u64) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let ctx = SharedCtx::new(None, Some(dir.join(CACHE_DIR)));
        Self::create_in(&dir, schema, table_id, ctx)
    }

    /// Create a new encrypted table, deriving the table Key-Encryption Key
    /// (KEK) from `passphrase` via Argon2id + HKDF (§7). A fresh random salt is
    /// generated and persisted under `_meta/keys` so the same passphrase
    /// recreates the KEK on reopen. Each run gets its own wrapped DEK.
    ///
    /// **Scope (§7):** encryption is *page-granular* — only sorted-run page
    /// payloads are encrypted. The live WAL (`_wal/`) holds rows as plaintext
    /// between `put` and `flush`; call `flush()` (which rotates the WAL) before
    /// treating sensitive data as fully at-rest-protected. Full WAL encryption
    /// is deferred.
    #[cfg(feature = "encryption")]
    pub fn create_encrypted(
        dir: impl AsRef<Path>,
        schema: Schema,
        table_id: u64,
        passphrase: &str,
    ) -> Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir.join(META_DIR))?;
        let salt = crate::encryption::random_salt();
        std::fs::write(dir.join(META_DIR).join(KEYS_FILENAME), salt)?;
        let kek: Arc<Kek> = Arc::new(Kek::derive(passphrase, &salt)?);
        let ctx = SharedCtx::new(Some(kek), Some(dir.to_path_buf().join(CACHE_DIR)));
        Self::create_in(dir, schema, table_id, ctx)
    }

    /// Create a new encrypted table using a raw key (e.g. from a key file)
    /// instead of a passphrase. Skips Argon2id — the key must already be
    /// high-entropy (>= 32 bytes of random data). ~0.1ms vs ~50ms for the
    /// passphrase path.
    #[cfg(feature = "encryption")]
    pub fn create_with_key(
        dir: impl AsRef<Path>,
        schema: Schema,
        table_id: u64,
        key: &[u8],
    ) -> Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir.join(META_DIR))?;
        let salt = crate::encryption::random_salt();
        std::fs::write(dir.join(META_DIR).join(KEYS_FILENAME), salt)?;
        let kek: Arc<Kek> = Arc::new(Kek::from_raw_key(key, &salt)?);
        let ctx = SharedCtx::new(Some(kek), Some(dir.to_path_buf().join(CACHE_DIR)));
        Self::create_in(dir, schema, table_id, ctx)
    }

    /// Open an existing encrypted table using a raw key.
    #[cfg(feature = "encryption")]
    pub fn open_with_key(dir: impl AsRef<Path>, key: &[u8]) -> Result<Self> {
        let dir = dir.as_ref();
        let salt_path = dir.join(META_DIR).join(KEYS_FILENAME);
        let salt_bytes = std::fs::read(&salt_path).map_err(|e| {
            MongrelError::NotFound(format!(
                "encryption salt file {:?}: {e} (table not encrypted, or corrupted)",
                salt_path
            ))
        })?;
        if salt_bytes.len() != crate::encryption::SALT_LEN {
            return Err(MongrelError::InvalidArgument(format!(
                "salt file is {} bytes, expected {}",
                salt_bytes.len(),
                crate::encryption::SALT_LEN
            )));
        }
        let mut salt = [0u8; crate::encryption::SALT_LEN];
        salt.copy_from_slice(&salt_bytes);
        let kek = Arc::new(Kek::from_raw_key(key, &salt)?);
        let ctx = SharedCtx::new(Some(kek), Some(dir.to_path_buf().join(CACHE_DIR)));
        Self::open_in(dir, ctx)
    }

    pub(crate) fn create_in(
        dir: impl AsRef<Path>,
        schema: Schema,
        table_id: u64,
        ctx: SharedCtx,
    ) -> Result<Self> {
        schema.validate_auto_increment()?;
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(dir.join(RUNS_DIR))?;
        write_schema(&dir, &schema)?;
        let (wal_dek, cache_dek) = derive_subkeys(ctx.kek.as_deref(), table_id);
        // B1: a mounted table routes writes through the shared WAL and never
        // creates its own `_wal/` dir. A standalone table owns a private WAL.
        let (wal, current_txn_id) = match ctx.shared.clone() {
            Some(s) => (WalSink::Shared(s), 0),
            None => {
                std::fs::create_dir_all(dir.join(WAL_DIR))?;
                let mut w = if let Some(ref dk) = wal_dek {
                    Wal::create_with_cipher(
                        dir.join(WAL_DIR).join("seg-000000.wal"),
                        Epoch(0),
                        Some(make_cipher(dk)),
                        0,
                    )?
                } else {
                    Wal::create(dir.join(WAL_DIR).join("seg-000000.wal"), Epoch(0))?
                };
                w.set_sync_byte_threshold(DEFAULT_SYNC_BYTE_THRESHOLD);
                (WalSink::Private(w), 1)
            }
        };
        let mut manifest = Manifest::new(table_id, schema.schema_id);
        // Seal the create-time manifest with the meta DEK so an encrypted table
        // reopens even if no write/flush ever re-persists it (otherwise the
        // reopen's encrypted manifest read fails to authenticate a plaintext
        // blob — see `manifest_meta_dek`).
        let manifest_meta_dek = crate::encryption::meta_dek_for(ctx.kek.as_deref());
        manifest::write_atomic(&dir, &mut manifest, manifest_meta_dek.as_ref())?;
        let (bitmap, ann, fm, sparse) = empty_indexes(&schema);
        let column_keys = build_column_keys(ctx.kek.as_deref(), &schema);
        let auto_inc = resolve_auto_inc(&schema);
        let rcache_dir = dir.join(RCACHE_DIR);
        Ok(Self {
            dir,
            table_id,
            wal,
            memtable: Memtable::new(),
            mutable_run: MutableRun::new(),
            mutable_run_spill_bytes: DEFAULT_MUTABLE_RUN_SPILL_BYTES,
            compaction_zstd_level: 3,
            allocator: RowIdAllocator::new(0),
            epoch: ctx.epoch,
            persisted_epoch: 0,
            schema,
            hot: HotIndex::new(),
            kek: ctx.kek,
            column_keys,
            run_refs: Vec::new(),
            retiring: Vec::new(),
            next_run_id: 1,
            sync_byte_threshold: DEFAULT_SYNC_BYTE_THRESHOLD,
            current_txn_id,
            bitmap,
            ann,
            fm,
            sparse,
            learned_range: HashMap::new(),
            pk_by_row: HashMap::new(),
            pinned: BTreeMap::new(),
            live_count: 0,
            reservoir: crate::reservoir::Reservoir::default(),
            had_deletes: false,
            agg_cache: HashMap::new(),
            global_idx_epoch: 0,
            indexes_complete: true,
            flushed_epoch: 0,
            page_cache: ctx.page_cache,
            decoded_cache: ctx.decoded_cache,
            snapshots: ctx.snapshots,
            commit_lock: ctx.commit_lock,
            result_cache: Arc::new(parking_lot::Mutex::new(
                ResultCache::new()
                    .with_dir(rcache_dir)
                    .with_cache_dek(cache_dek.clone()),
            )),
            pending_delete_rids: roaring::RoaringBitmap::new(),
            pending_put_cols: std::collections::HashSet::new(),
            pending_rows: Vec::new(),
            pending_rows_auto_inc: Vec::new(),
            pending_dels: Vec::new(),
            pending_truncate: None,
            wal_dek,
            auto_inc,
        })
    }

    /// Open an existing table: load the manifest, replay the active WAL segment
    /// into the memtable, and rebuild the HOT + secondary indexes from the runs
    /// and replayed rows.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        let ctx = SharedCtx::new(None, Some(dir.to_path_buf().join(CACHE_DIR)));
        Self::open_in(dir, ctx)
    }

    /// Open an existing encrypted table. `passphrase` must match the one used at
    /// create time (combined with the persisted salt to re-derive the KEK).
    #[cfg(feature = "encryption")]
    pub fn open_encrypted(dir: impl AsRef<Path>, passphrase: &str) -> Result<Self> {
        let dir = dir.as_ref();
        let salt_path = dir.join(META_DIR).join(KEYS_FILENAME);
        let salt_bytes = std::fs::read(&salt_path).map_err(|e| {
            MongrelError::NotFound(format!(
                "encryption salt file {:?}: {e} (table not encrypted, or corrupted)",
                salt_path
            ))
        })?;
        let salt_len = crate::encryption::SALT_LEN;
        if salt_bytes.len() != salt_len {
            return Err(MongrelError::InvalidArgument(format!(
                "encryption salt is {} bytes, expected {salt_len}",
                salt_bytes.len()
            )));
        }
        let mut salt = [0u8; 16];
        salt.copy_from_slice(&salt_bytes);
        let kek: Arc<Kek> = Arc::new(Kek::derive(passphrase, &salt)?);
        let ctx = SharedCtx::new(Some(kek), Some(dir.to_path_buf().join(CACHE_DIR)));
        let t = Self::open_in(dir, ctx)?;
        Ok(t)
    }

    pub(crate) fn open_in(dir: impl AsRef<Path>, ctx: SharedCtx) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let manifest_meta_dek = crate::encryption::meta_dek_for(ctx.kek.as_deref());
        let manifest = manifest::read(&dir, manifest_meta_dek.as_ref())?;
        let schema: Schema = read_schema(&dir)?;
        let replay_epoch = Epoch(manifest.current_epoch);
        let (wal_dek, cache_dek) = derive_subkeys(ctx.kek.as_deref(), manifest.table_id);
        // B1: a mounted table has no private WAL — its committed records live in
        // the shared WAL and are replayed by `Database::recover_shared_wal`. A
        // standalone table replays + reopens its own `_wal/` segment here.
        let (wal, replayed, current_txn_id) = match ctx.shared.clone() {
            Some(s) => (WalSink::Shared(s), Vec::new(), 0),
            None => {
                let active = latest_wal_segment(&dir.join(WAL_DIR))?;
                // Replay BEFORE truncating: `Wal::create` would erase the segment.
                let replayed = match &active {
                    Some(path) => {
                        let cipher = wal_dek.as_ref().map(|dk| make_cipher(dk));
                        crate::wal::replay_with_cipher(path, cipher)?
                    }
                    None => Vec::new(),
                };
                let mut w = match &active {
                    Some(path) => Wal::create_with_cipher(
                        path,
                        replay_epoch,
                        wal_dek.as_ref().map(|dk| make_cipher(dk)),
                        0,
                    )?,
                    None => Wal::create_with_cipher(
                        dir.join(WAL_DIR).join("seg-000000.wal"),
                        replay_epoch,
                        wal_dek.as_ref().map(|dk| make_cipher(dk)),
                        0,
                    )?,
                };
                w.set_sync_byte_threshold(DEFAULT_SYNC_BYTE_THRESHOLD);
                (WalSink::Private(w), replayed, 1)
            }
        };

        let mut memtable = Memtable::new();
        let mut allocator = RowIdAllocator::new(manifest.next_row_id);
        let persisted_epoch = manifest.current_epoch;
        // Seed the auto-increment counter from the manifest. `auto_inc_next == 0`
        // means unseeded (fresh table, or a legacy manifest migrated forward) —
        // the first allocation scans `max(PK)` to avoid colliding with existing
        // rows. WAL replay (below) and `recover_apply` additionally bump `next`
        // past replayed ids without marking it seeded, so the scan still covers
        // any rows that were already flushed to sorted runs.
        let mut auto_inc = resolve_auto_inc(&schema).map(|mut s| {
            s.next = manifest.auto_inc_next;
            s.seeded = manifest.auto_inc_next > 0;
            s
        });

        // 1. Replay is two-phase and TxnCommit-gated: data records (Put/Delete)
        //    are staged per `txn_id` and only applied when a durable
        //    `TxnCommit{epoch}` for that txn is seen. Uncommitted / aborted /
        //    torn-tail txns are discarded. Indexing happens AFTER loading any
        //    checkpoint / run data (below) so the newer replayed versions
        //    overwrite the older run versions in the HOT index.
        let mut staged_puts: HashMap<u64, Vec<Row>> = HashMap::new();
        let mut staged_deletes: HashMap<u64, Vec<RowId>> = HashMap::new();
        let mut replayed_puts: std::collections::BTreeMap<Epoch, Vec<Row>> =
            std::collections::BTreeMap::new();
        let mut replayed_deletes: Vec<(RowId, Epoch)> = Vec::new();
        let mut saw_delete = false;
        for record in replayed {
            let txn_id = record.txn_id;
            match record.op {
                Op::Put { rows, .. } => {
                    let rows: Vec<Row> = bincode::deserialize(&rows)?;
                    for row in &rows {
                        allocator.advance_to(row.row_id);
                        if let Some(ai) = auto_inc.as_mut() {
                            if let Some(Value::Int64(n)) = row.columns.get(&ai.column_id) {
                                if *n + 1 > ai.next {
                                    ai.next = *n + 1;
                                }
                            }
                        }
                    }
                    staged_puts.entry(txn_id).or_default().extend(rows);
                }
                Op::Delete { row_ids, .. } => {
                    staged_deletes.entry(txn_id).or_default().extend(row_ids);
                }
                Op::TxnCommit { epoch, .. } => {
                    let commit_epoch = Epoch(epoch);
                    if let Some(puts) = staged_puts.remove(&txn_id) {
                        for row in &puts {
                            memtable.upsert(row.clone());
                        }
                        replayed_puts.entry(commit_epoch).or_default().extend(puts);
                    }
                    if let Some(dels) = staged_deletes.remove(&txn_id) {
                        saw_delete = true;
                        for rid in dels {
                            memtable.tombstone(rid, commit_epoch);
                            replayed_deletes.push((rid, commit_epoch));
                        }
                    }
                }
                Op::TxnAbort => {
                    staged_puts.remove(&txn_id);
                    staged_deletes.remove(&txn_id);
                }
                Op::TruncateTable { .. } | Op::Flush { .. } | Op::Ddl(_) => {}
            }
        }

        let rcache_dir = dir.join(RCACHE_DIR);
        let column_keys = build_column_keys(ctx.kek.as_deref(), &schema);
        let mut db = Self {
            dir,
            table_id: manifest.table_id,
            wal,
            memtable,
            mutable_run: MutableRun::new(),
            mutable_run_spill_bytes: DEFAULT_MUTABLE_RUN_SPILL_BYTES,
            compaction_zstd_level: 3,
            allocator,
            epoch: ctx.epoch,
            persisted_epoch,
            schema,
            hot: HotIndex::new(),
            kek: ctx.kek,
            column_keys,
            run_refs: manifest.runs.clone(),
            retiring: manifest.retiring.clone(),
            next_run_id: manifest
                .runs
                .iter()
                .map(|r| r.run_id as u64 + 1)
                .max()
                .unwrap_or(1),
            sync_byte_threshold: DEFAULT_SYNC_BYTE_THRESHOLD,
            current_txn_id,
            bitmap: HashMap::new(),
            ann: HashMap::new(),
            fm: HashMap::new(),
            sparse: HashMap::new(),
            learned_range: HashMap::new(),
            pk_by_row: HashMap::new(),
            pinned: BTreeMap::new(),
            live_count: manifest.live_count,
            reservoir: crate::reservoir::Reservoir::default(),
            had_deletes: saw_delete,
            agg_cache: HashMap::new(),
            global_idx_epoch: manifest.global_idx_epoch,
            indexes_complete: true,
            flushed_epoch: manifest.flushed_epoch,
            page_cache: ctx.page_cache,
            decoded_cache: ctx.decoded_cache,
            snapshots: ctx.snapshots,
            commit_lock: ctx.commit_lock,
            result_cache: Arc::new(parking_lot::Mutex::new(
                ResultCache::new()
                    .with_dir(rcache_dir)
                    .with_cache_dek(cache_dek.clone()),
            )),
            pending_delete_rids: roaring::RoaringBitmap::new(),
            pending_put_cols: std::collections::HashSet::new(),
            pending_rows: Vec::new(),
            pending_rows_auto_inc: Vec::new(),
            pending_dels: Vec::new(),
            pending_truncate: None,
            wal_dek,
            auto_inc,
        };

        // Advance the (possibly shared) epoch authority to this table's manifest
        // epoch so rebuild/index reads below observe the recovered watermark.
        db.epoch.advance_recovered(Epoch(db.persisted_epoch));

        // 2. Fast path: load the persisted global-index checkpoint (Phase 9.1).
        //    Valid only when its embedded epoch matches the manifest-endorsed
        //    `global_idx_epoch` and every run was created at or before it, so the
        //    checkpoint covers all run data. Otherwise rebuild from the runs.
        let checkpoint = global_idx::read(&db.dir, db.idx_dek().as_deref())?;
        let checkpoint_valid = checkpoint.as_ref().is_some_and(|c| {
            c.epoch_built == manifest.global_idx_epoch
                && manifest.global_idx_epoch > 0
                && manifest
                    .runs
                    .iter()
                    .all(|r| r.epoch_created <= manifest.global_idx_epoch)
        });
        if let Some(loaded) = checkpoint {
            if checkpoint_valid {
                db.hot = loaded.hot;
                db.bitmap = loaded.bitmap;
                db.ann = loaded.ann;
                db.fm = loaded.fm;
                db.sparse = loaded.sparse;
                db.learned_range = loaded.learned_range;
                db.refresh_pk_by_row_from_hot();
            }
        }
        if !checkpoint_valid {
            let (bitmap, ann, fm, sparse) = empty_indexes(&db.schema);
            db.bitmap = bitmap;
            db.ann = ann;
            db.fm = fm;
            db.sparse = sparse;
            db.rebuild_indexes_from_runs()?;
            db.build_learned_ranges()?;
        }

        // 3. Index the replayed WAL rows on top so updates overwrite. Within a
        //    single transaction epoch duplicate PKs are upserted: only the last
        //    winner is indexed, losers are tombstoned in the already-replayed
        //    memtable.
        for (epoch, group) in replayed_puts {
            let (losers, winner_pks) = db.partition_pk_winners(&group);
            for (key, &row_id) in &winner_pks {
                if let Some(old_rid) = db.hot.get(key) {
                    if old_rid != row_id {
                        db.tombstone_row(old_rid, epoch, false);
                    }
                }
            }
            for &loser_rid in &losers {
                db.tombstone_row(loser_rid, epoch, false);
            }
            for (key, row_id) in winner_pks {
                db.insert_hot_pk(key, row_id);
            }
            if db.schema.primary_key().is_none() {
                for r in &group {
                    db.hot.insert(r.row_id.0.to_be_bytes().to_vec(), r.row_id);
                }
            }
            for r in &group {
                if !losers.contains(&r.row_id) {
                    db.index_row(r);
                }
            }
        }
        // Apply replayed deletes after the puts: a delete targets a specific row
        // id and only removes the HOT entry if it still points to that id, so a
        // newer upsert for the same PK is not accidentally erased.
        for (rid, epoch) in &replayed_deletes {
            db.remove_hot_for_row(*rid, *epoch);
        }

        let _ = db.rebuild_reservoir();
        // Load the persistent result-cache tier (hardening (b)) so fine-grained
        // invalidation resumes across restart.
        db.result_cache.lock().load_persistent();
        Ok(db)
    }

    /// Repopulate the reservoir sample from all visible rows (used on open so a
    /// reopened table has an analytics sample without further inserts).
    fn rebuild_reservoir(&mut self) -> Result<()> {
        let snap = self.snapshot();
        let rows = self.visible_rows(snap)?;
        self.reservoir.reset();
        for r in rows {
            self.reservoir.offer(r.row_id.0);
        }
        Ok(())
    }

    fn rebuild_indexes_from_runs(&mut self) -> Result<()> {
        self.hot = HotIndex::new();
        self.pk_by_row.clear();
        let (bitmap, ann, fm, sparse) = empty_indexes(&self.schema);
        self.bitmap = bitmap;
        self.ann = ann;
        self.fm = fm;
        self.sparse = sparse;
        let snapshot = Epoch(u64::MAX);
        for rr in self.run_refs.clone() {
            let mut reader = self.open_reader(rr.run_id)?;
            for row in reader.visible_rows(snapshot)? {
                let tok_row = self.tokenized_for_indexes(&row);
                index_into(
                    &self.schema,
                    &tok_row,
                    &mut self.hot,
                    &mut self.bitmap,
                    &mut self.ann,
                    &mut self.fm,
                    &mut self.sparse,
                );
            }
        }
        for row in self.mutable_run.visible_versions(snapshot) {
            if row.deleted {
                self.remove_hot_for_row(row.row_id, snapshot);
            } else {
                self.index_row(&row);
            }
        }
        for row in self.memtable.visible_versions(snapshot) {
            if row.deleted {
                self.remove_hot_for_row(row.row_id, snapshot);
            } else {
                self.index_row(&row);
            }
        }
        self.refresh_pk_by_row_from_hot();
        Ok(())
    }

    fn refresh_pk_by_row_from_hot(&mut self) {
        self.pk_by_row.clear();
        if self.schema.primary_key().is_none() {
            return;
        }
        for (key, row_id) in self.hot.entries() {
            self.pk_by_row.insert(row_id, key);
        }
    }

    fn insert_hot_pk(&mut self, key: Vec<u8>, row_id: RowId) {
        if self.schema.primary_key().is_some() {
            self.pk_by_row.insert(row_id, key.clone());
        }
        self.hot.insert(key, row_id);
    }

    /// (Re)build per-column learned (PGM) range indexes for `LearnedRange`
    /// columns from the single sorted run. Serves `Condition::Range` sub-linearly
    /// on the fast path; no-op when there isn't exactly one run.
    pub(crate) fn build_learned_ranges(&mut self) -> Result<()> {
        self.learned_range.clear();
        if self.run_refs.len() != 1 {
            return Ok(());
        }
        let cols: Vec<u16> = self
            .schema
            .indexes
            .iter()
            .filter(|i| i.kind == IndexKind::LearnedRange)
            .map(|i| i.column_id)
            .collect();
        if cols.is_empty() {
            return Ok(());
        }
        let mut reader = self.open_reader(self.run_refs[0].run_id)?;
        let row_ids: Vec<u64> = match reader.column_native(crate::sorted_run::SYS_ROW_ID)? {
            columnar::NativeColumn::Int64 { data, .. } => data.iter().map(|x| *x as u64).collect(),
            _ => return Ok(()),
        };
        for cid in cols {
            let ty = self
                .schema
                .columns
                .iter()
                .find(|c| c.id == cid)
                .map(|c| c.ty)
                .unwrap_or(TypeId::Int64);
            match ty {
                TypeId::Int64 | TypeId::TimestampNanos | TypeId::Date32 => {
                    if let columnar::NativeColumn::Int64 { data, .. } = reader.column_native(cid)? {
                        let pairs: Vec<(i64, u64)> = data
                            .iter()
                            .zip(row_ids.iter())
                            .map(|(v, r)| (*v, *r))
                            .collect();
                        self.learned_range
                            .insert(cid, ColumnLearnedRange::build_i64(&pairs));
                    }
                }
                TypeId::Float64 => {
                    if let columnar::NativeColumn::Float64 { data, .. } =
                        reader.column_native(cid)?
                    {
                        let pairs: Vec<(f64, u64)> = data
                            .iter()
                            .zip(row_ids.iter())
                            .map(|(v, r)| (*v, *r))
                            .collect();
                        self.learned_range
                            .insert(cid, ColumnLearnedRange::build_f64(&pairs));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Phase 14.7: if the live indexes are known incomplete (after a bulk
    /// ingest that deferred index building), rebuild them from the runs now.
    /// Called lazily by `query` / `query_columns_native` / `flush`.
    fn ensure_indexes_complete(&mut self) -> Result<()> {
        if self.indexes_complete {
            crate::trace::QueryTrace::record(|t| {
                t.index_rebuild = crate::trace::IndexRebuild::AlreadyComplete;
            });
            return Ok(());
        }
        crate::trace::QueryTrace::record(|t| {
            t.index_rebuild = crate::trace::IndexRebuild::Rebuilt;
        });
        self.rebuild_indexes_from_runs()?;
        self.build_learned_ranges()?;
        self.indexes_complete = true;
        let epoch = self.current_epoch();
        self.checkpoint_indexes(epoch);
        Ok(())
    }

    fn pending_epoch(&self) -> Epoch {
        Epoch(self.epoch.visible().0 + 1)
    }

    /// True when this table is mounted in a `Database` (writes route through the
    /// shared WAL).
    fn is_shared(&self) -> bool {
        matches!(self.wal, WalSink::Shared(_))
    }

    /// Return the current auto-commit txn id, allocating a fresh one from the
    /// shared allocator on a mounted table when a new span starts (sentinel 0).
    /// A standalone table uses its private monotonic counter (never 0).
    fn ensure_txn_id(&mut self) -> u64 {
        if self.current_txn_id == 0 {
            let id = match &self.wal {
                WalSink::Shared(s) => {
                    let mut g = s.txn_ids.lock();
                    let v = *g;
                    *g = g.wrapping_add(1);
                    v
                }
                WalSink::Private(_) => 1,
            };
            self.current_txn_id = id;
        }
        self.current_txn_id
    }

    /// Append a data record (`Put`/`Delete`) for the current auto-commit txn to
    /// whichever WAL backs this table.
    fn wal_append_data(&mut self, op: Op) -> Result<()> {
        let txn_id = self.ensure_txn_id();
        let table_id = self.table_id;
        match &mut self.wal {
            WalSink::Private(w) => {
                w.append_txn(txn_id, op)?;
            }
            WalSink::Shared(s) => {
                s.wal.lock().append(txn_id, table_id, op)?;
            }
        }
        Ok(())
    }

    /// Upsert a row. Allocates a [`RowId`], appends a (non-fsynced) WAL record,
    /// and updates the memtable + indexes. Returns the new row id. Durability
    /// arrives at the next [`Table::commit`] (or [`Table::flush`]).
    ///
    /// For an `AUTO_INCREMENT` primary key, omit the column (or pass
    /// [`Value::Null`]) and the engine assigns the next counter value; use
    /// [`Table::put_returning`] to learn that assigned value.
    pub fn put(&mut self, columns: Vec<(u16, Value)>) -> Result<RowId> {
        Ok(self.put_returning(columns)?.0)
    }

    /// Like [`Table::put`] but also returns the engine-assigned `AUTO_INCREMENT`
    /// value (`Some` only when the column was omitted/null and the engine filled
    /// it; `None` when the table has no auto-increment column or the caller
    /// supplied an explicit value).
    pub fn put_returning(
        &mut self,
        mut columns: Vec<(u16, Value)>,
    ) -> Result<(RowId, Option<i64>)> {
        let assigned = self.fill_auto_inc(&mut columns)?;
        let mut col_map = std::collections::HashMap::with_capacity(columns.len());
        for (c, v) in &columns {
            col_map.insert(*c, v.clone());
        }
        self.schema.validate_not_null(&col_map)?;
        let row_id = self.allocator.alloc();
        let epoch = self.pending_epoch();
        let mut row = Row::new(row_id, epoch);
        for (col_id, val) in columns {
            row.columns.insert(col_id, val);
        }
        self.commit_rows(vec![row], assigned.is_some())?;
        Ok((row_id, assigned))
    }

    /// Bulk upsert: many rows under a single WAL record + one index pass. Far
    /// cheaper than `put` in a loop for batch ingest.
    pub fn put_batch(&mut self, batch: Vec<Vec<(u16, Value)>>) -> Result<Vec<RowId>> {
        Ok(self
            .put_batch_returning(batch)?
            .into_iter()
            .map(|(r, _)| r)
            .collect())
    }

    /// Like [`Table::put_batch`] but each entry is paired with the engine-
    /// assigned `AUTO_INCREMENT` value (`Some` only when filled by the engine).
    pub fn put_batch_returning(
        &mut self,
        batch: Vec<Vec<(u16, Value)>>,
    ) -> Result<Vec<(RowId, Option<i64>)>> {
        let mut filled: Vec<FilledAutoIncRow> = Vec::with_capacity(batch.len());
        for mut cols in batch {
            let assigned = self.fill_auto_inc(&mut cols)?;
            filled.push((cols, assigned));
        }
        for (cols, _) in &filled {
            let mut col_map = std::collections::HashMap::with_capacity(cols.len());
            for (c, v) in cols {
                col_map.insert(*c, v.clone());
            }
            self.schema.validate_not_null(&col_map)?;
        }
        let epoch = self.pending_epoch();
        let mut rows = Vec::with_capacity(filled.len());
        let mut ids = Vec::with_capacity(filled.len());
        for (cols, assigned) in filled {
            let row_id = self.allocator.alloc();
            let mut row = Row::new(row_id, epoch);
            for (c, v) in cols {
                row.columns.insert(c, v);
            }
            ids.push((row_id, assigned));
            rows.push(row);
        }
        let all_auto_generated = ids.iter().all(|(_, assigned)| assigned.is_some());
        self.commit_rows(rows, all_auto_generated)?;
        Ok(ids)
    }

    /// Fill the `AUTO_INCREMENT` column for an upcoming row. When the column is
    /// omitted or [`Value::Null`] the next counter value is allocated and the
    /// cell is appended/replaced in `columns`; an explicit `Int64` is honored
    /// and advances the counter past it. Returns `Some(value)` when the engine
    /// allocated (so the caller can surface it), `None` otherwise.
    pub fn fill_auto_inc(&mut self, columns: &mut Vec<(u16, Value)>) -> Result<Option<i64>> {
        let Some(cid) = self.auto_inc.as_ref().map(|a| a.column_id) else {
            return Ok(None);
        };
        let pos = columns.iter().position(|(c, _)| *c == cid);
        let assigned = match pos {
            Some(i) => match &columns[i].1 {
                Value::Null => {
                    let next = self.alloc_auto_inc_value()?;
                    columns[i].1 = Value::Int64(next);
                    Some(next)
                }
                Value::Int64(n) => {
                    self.advance_auto_inc_past(*n)?;
                    None
                }
                other => {
                    return Err(MongrelError::InvalidArgument(format!(
                        "AUTO_INCREMENT column {cid} must be Int64 or NULL, got {:?}",
                        other
                    )))
                }
            },
            None => {
                let next = self.alloc_auto_inc_value()?;
                columns.push((cid, Value::Int64(next)));
                Some(next)
            }
        };
        Ok(assigned)
    }

    /// Allocate the next identity value, seeding the counter first if needed.
    fn alloc_auto_inc_value(&mut self) -> Result<i64> {
        self.ensure_auto_inc_seeded()?;
        // Borrow checker: re-read after the mutable `ensure` call returns.
        let ai = self.auto_inc.as_mut().expect("auto-inc column present");
        let v = ai.next;
        ai.next = ai.next.saturating_add(1);
        Ok(v)
    }

    /// Advance the counter past an explicit id, seeding first if needed so a
    /// pre-existing higher id elsewhere is never ignored.
    fn advance_auto_inc_past(&mut self, used: i64) -> Result<()> {
        self.ensure_auto_inc_seeded()?;
        let ai = self.auto_inc.as_mut().expect("auto-inc column present");
        let floor = used.saturating_add(1).max(1);
        if ai.next < floor {
            ai.next = floor;
        }
        Ok(())
    }

    /// Seed the counter on first use by scanning `max(PK)` over all visible
    /// rows, so an upgraded table (legacy client-assigned ids, or a manifest
    /// migrated from `auto_inc_next == 0`) never hands out a colliding id.
    /// Idempotent: a no-op once seeded.
    fn ensure_auto_inc_seeded(&mut self) -> Result<()> {
        let needs_seed = match self.auto_inc {
            Some(ai) => !ai.seeded,
            None => return Ok(()),
        };
        if !needs_seed {
            return Ok(());
        }
        if self.seed_empty_auto_inc() {
            return Ok(());
        }
        let cid = self
            .auto_inc
            .as_ref()
            .expect("auto-inc column present")
            .column_id;
        let max = self.scan_max_int64(cid)?;
        let ai = self.auto_inc.as_mut().expect("auto-inc column present");
        let floor = max.saturating_add(1).max(1);
        if ai.next < floor {
            ai.next = floor;
        }
        ai.seeded = true;
        Ok(())
    }

    fn alloc_auto_inc_range(&mut self, n: usize) -> Result<Option<i64>> {
        if n == 0 || self.auto_inc.is_none() {
            return Ok(None);
        }
        self.ensure_auto_inc_seeded()?;
        let ai = self.auto_inc.as_mut().expect("auto-inc column present");
        let start = ai.next;
        ai.next = ai.next.saturating_add(n as i64);
        Ok(Some(start))
    }

    /// One-time `max(Int64 column)` over all MVCC-visible rows. Used to seed the
    /// auto-increment counter. Runs at most once per table (the manifest then
    /// checkpoints the seeded counter).
    fn scan_max_int64(&mut self, column_id: u16) -> Result<i64> {
        let mut max: i64 = 0;
        for r in self.memtable.visible_versions(Epoch(u64::MAX)) {
            if let Some(Value::Int64(n)) = r.columns.get(&column_id) {
                if *n > max {
                    max = *n;
                }
            }
        }
        for r in self.mutable_run.visible_versions(Epoch(u64::MAX)) {
            if let Some(Value::Int64(n)) = r.columns.get(&column_id) {
                if *n > max {
                    max = *n;
                }
            }
        }
        for rr in self.run_refs.clone() {
            let reader = self.open_reader(rr.run_id)?;
            if let Some(stats) = reader.column_page_stats(column_id) {
                for s in stats {
                    if let Some(n) = crate::sorted_run::be_i64(s.max.as_deref()) {
                        if n > max {
                            max = n;
                        }
                    }
                }
            } else if reader.has_column(column_id) {
                if let columnar::NativeColumn::Int64 { data, validity } =
                    reader.column_native_shared(column_id)?
                {
                    for (i, n) in data.iter().enumerate() {
                        if (validity.is_empty() || columnar::validity_bit(&validity, i)) && *n > max
                        {
                            max = *n;
                        }
                    }
                }
            }
        }
        Ok(max)
    }

    fn seed_empty_auto_inc(&mut self) -> bool {
        let Some(ai) = self.auto_inc.as_mut() else {
            return false;
        };
        if ai.seeded || self.live_count != 0 {
            return false;
        }
        if ai.next < 1 {
            ai.next = 1;
        }
        ai.seeded = true;
        true
    }

    fn advance_auto_inc_from_native_columns(
        &mut self,
        columns: &[(u16, columnar::NativeColumn)],
        n: usize,
        live_before: u64,
    ) -> Result<()> {
        let Some(ai) = self.auto_inc.as_mut() else {
            return Ok(());
        };
        let Some((_, col)) = columns.iter().find(|(cid, _)| *cid == ai.column_id) else {
            return Ok(());
        };
        let columnar::NativeColumn::Int64 { data, validity } = col else {
            return Err(MongrelError::InvalidArgument(format!(
                "AUTO_INCREMENT column {} must be Int64",
                ai.column_id
            )));
        };
        let max = if native_int64_strictly_increasing(col, n) {
            data.get(n.saturating_sub(1)).copied()
        } else {
            data.iter()
                .take(n)
                .enumerate()
                .filter_map(|(i, v)| {
                    if validity.is_empty() || columnar::validity_bit(validity, i) {
                        Some(*v)
                    } else {
                        None
                    }
                })
                .max()
        };
        if let Some(max) = max {
            let floor = max.saturating_add(1).max(1);
            if ai.next < floor {
                ai.next = floor;
            }
            if ai.seeded || live_before == 0 {
                ai.seeded = true;
            }
        }
        Ok(())
    }

    fn fill_auto_inc_native_columns(
        &mut self,
        columns: &mut Vec<(u16, columnar::NativeColumn)>,
        n: usize,
    ) -> Result<()> {
        let Some(cid) = self.auto_inc.as_ref().map(|a| a.column_id) else {
            return Ok(());
        };
        let Some(pos) = columns.iter().position(|(id, _)| *id == cid) else {
            if let Some(start) = self.alloc_auto_inc_range(n)? {
                columns.push((
                    cid,
                    columnar::NativeColumn::Int64 {
                        data: (start..start.saturating_add(n as i64)).collect(),
                        validity: vec![0xFF; n.div_ceil(8)],
                    },
                ));
            }
            return Ok(());
        };

        let columnar::NativeColumn::Int64 { data, validity } = &mut columns[pos].1 else {
            return Err(MongrelError::InvalidArgument(format!(
                "AUTO_INCREMENT column {cid} must be Int64"
            )));
        };
        if data.len() < n {
            return Err(MongrelError::InvalidArgument(format!(
                "AUTO_INCREMENT column {cid} has {} rows, expected {n}",
                data.len()
            )));
        }
        if columnar::all_non_null(validity, n) {
            return Ok(());
        }
        if validity.iter().all(|b| *b == 0) {
            if let Some(start) = self.alloc_auto_inc_range(n)? {
                for (i, slot) in data.iter_mut().take(n).enumerate() {
                    *slot = start.saturating_add(i as i64);
                }
                *validity = vec![0xFF; n.div_ceil(8)];
            }
            return Ok(());
        }

        let new_validity = vec![0xFF; data.len().div_ceil(8)];
        for (i, slot) in data.iter_mut().enumerate().take(n) {
            if columnar::validity_bit(validity, i) {
                self.advance_auto_inc_past(*slot)?;
            } else {
                *slot = self.alloc_auto_inc_value()?;
            }
        }
        *validity = new_validity;
        Ok(())
    }

    /// Reserve (but do not insert) the next `AUTO_INCREMENT` value, advancing
    /// the in-memory counter. Returns `None` when the table has no
    /// auto-increment column.
    ///
    /// This is the escape hatch for callers that stage the row with an explicit
    /// id inside a cross-table [`crate::Transaction`] — where the engine cannot
    /// fill the column on the `put` path (the row id + cells are only assembled
    /// at commit). Unlike the old Kit `__kit_sequences` sequence row, the
    /// reservation is a pure in-memory counter bump: no hot row, no second
    /// commit. It becomes durable when a row carrying the reserved id commits
    /// (the counter is checkpointed to the manifest in the same commit); an
    /// aborted reservation simply leaves a gap, which the never-reuse rule
    /// permits.
    pub fn reserve_auto_inc(&mut self) -> Result<Option<i64>> {
        if self.auto_inc.is_none() {
            return Ok(None);
        }
        Ok(Some(self.alloc_auto_inc_value()?))
    }

    /// Append `rows` under one WAL record. On a standalone table they are folded
    /// into the memtable + indexes immediately (single clock — no speculative-
    /// epoch hazard). On a mounted table (B1/B2) they are staged in
    /// `pending_rows` and applied at the real assigned epoch in `commit`, so a
    /// concurrent reader can never see them before their commit epoch.
    fn commit_rows(&mut self, rows: Vec<Row>, auto_inc_generated: bool) -> Result<()> {
        let payload = bincode::serialize(&rows)?;
        self.wal_append_data(Op::Put {
            table_id: self.table_id,
            rows: payload,
        })?;
        if self.is_shared() {
            self.pending_rows_auto_inc
                .extend(std::iter::repeat(auto_inc_generated).take(rows.len()));
            self.pending_rows.extend(rows);
        } else {
            self.apply_put_rows_inner(rows, !auto_inc_generated)?;
        }
        Ok(())
    }

    /// Apply already-durable put rows to the memtable + indexes + allocator +
    /// live count WITHOUT appending to the per-table WAL (the WAL — shared or
    /// per-table — is the caller's responsibility). Used by the cross-table
    /// `Transaction` commit path (P2.5) after it has written the shared WAL.
    pub(crate) fn apply_put_rows(&mut self, rows: Vec<Row>) -> Result<()> {
        self.apply_put_rows_inner(rows, true)
    }

    fn apply_put_rows_inner(&mut self, rows: Vec<Row>, check_existing_pk: bool) -> Result<()> {
        if check_existing_pk {
            self.ensure_indexes_complete()?;
        }
        let n = rows.len();
        // Track mutated columns for fine-grained cache invalidation (c).
        for r in &rows {
            for &cid in r.columns.keys() {
                self.pending_put_cols.insert(cid);
            }
        }
        let (losers, winner_pks) = self.partition_pk_winners(&rows);
        let epoch = rows.first().map(|r| r.committed_epoch).unwrap_or(Epoch(0));
        // Tombstone any pre-existing row that owns the same PK as a winner.
        if check_existing_pk {
            for (key, &row_id) in &winner_pks {
                if let Some(old_rid) = self.hot.get(key) {
                    if old_rid != row_id {
                        self.tombstone_row(old_rid, epoch, true);
                    }
                }
            }
        }
        // Insert the winners into HOT.
        for (key, row_id) in winner_pks {
            self.insert_hot_pk(key, row_id);
        }
        if self.schema.primary_key().is_none() {
            for r in &rows {
                self.hot.insert(r.row_id.0.to_be_bytes().to_vec(), r.row_id);
            }
        }
        // Index, sample, and materialize only the surviving rows.
        for r in &rows {
            if !losers.contains(&r.row_id) {
                self.index_row(r);
            }
        }
        for r in &rows {
            if !losers.contains(&r.row_id) {
                self.reservoir.offer(r.row_id.0);
            }
        }
        for r in rows {
            if !losers.contains(&r.row_id) {
                self.memtable.upsert(r);
            }
        }
        self.live_count = self.live_count.saturating_add((n - losers.len()) as u64);
        Ok(())
    }

    /// Allocate a fresh row id (advancing the table's allocator). Used by the
    /// cross-table `Transaction` to assign ids before sealing a row.
    pub(crate) fn alloc_row_id(&mut self) -> RowId {
        self.allocator.alloc()
    }

    /// Apply the metadata for rows that were spilled to a linked uniform-epoch
    /// run (P3.4): update the HOT + secondary indexes, the reservoir, the
    /// allocator high-water mark, and `live_count` — but **do NOT** insert the
    /// rows into the memtable. The rows are served from the linked run (which the
    /// scan/merge path reads at the run's commit epoch), so materializing them in
    /// the memtable too would defeat the point of spilling (peak memory stays
    /// bounded). Caller must have linked the run before reads can resolve indexes.
    pub(crate) fn apply_run_metadata(&mut self, rows: &[Row]) -> Result<()> {
        self.ensure_indexes_complete()?;
        let n = rows.len();
        for r in rows {
            for &cid in r.columns.keys() {
                self.pending_put_cols.insert(cid);
            }
        }
        let (losers, winner_pks) = self.partition_pk_winners(rows);
        let epoch = rows.first().map(|r| r.committed_epoch).unwrap_or(Epoch(0));
        // Tombstone pre-existing rows that conflict with winners.
        for (key, &row_id) in &winner_pks {
            if let Some(old_rid) = self.hot.get(key) {
                if old_rid != row_id {
                    self.tombstone_row(old_rid, epoch, true);
                }
            }
        }
        // Hide duplicate-PK rows inside this uniform-epoch run by tombstoning
        // their row ids in the memtable overlay (the overlay wins over the run).
        for &loser_rid in &losers {
            self.tombstone_row(loser_rid, epoch, false);
        }
        // Insert the winners into HOT.
        for (key, row_id) in winner_pks {
            self.insert_hot_pk(key, row_id);
        }
        if self.schema.primary_key().is_none() {
            for r in rows {
                self.hot.insert(r.row_id.0.to_be_bytes().to_vec(), r.row_id);
            }
        }
        for r in rows {
            self.allocator.advance_to(r.row_id);
            if !losers.contains(&r.row_id) {
                self.index_row(r);
            }
        }
        for r in rows {
            if !losers.contains(&r.row_id) {
                self.reservoir.offer(r.row_id.0);
            }
        }
        self.live_count = self.live_count.saturating_add((n - losers.len()) as u64);
        Ok(())
    }

    /// Apply already-committed puts + tombstones during shared-WAL recovery
    /// (spec §15 pass 2). Advances the allocator, upserts/tombstones the
    /// memtable, and indexes the rows — but does NOT touch `live_count` (the
    /// manifest is authoritative) and does NOT append to the WAL.
    pub(crate) fn recover_apply(
        &mut self,
        rows: Vec<Row>,
        deletes: Vec<(RowId, Epoch)>,
    ) -> Result<()> {
        // Rows from different transactions have different epochs and can be
        // upserted sequentially. Rows inside one transaction share an epoch, so
        // duplicate PKs within that transaction must keep only the last winner.
        let mut by_epoch: std::collections::BTreeMap<Epoch, Vec<Row>> =
            std::collections::BTreeMap::new();
        for row in rows {
            self.allocator.advance_to(row.row_id);
            // Mirror the row-id advance for the AUTO_INCREMENT counter: WAL
            // replay must not hand out an id a recovered row already claimed.
            // `seeded` is intentionally left untouched so a still-unseeded
            // counter still scans `max(PK)` to cover already-flushed rows.
            if let Some(ai) = self.auto_inc.as_mut() {
                if let Some(Value::Int64(n)) = row.columns.get(&ai.column_id) {
                    if *n + 1 > ai.next {
                        ai.next = *n + 1;
                    }
                }
            }
            by_epoch.entry(row.committed_epoch).or_default().push(row);
        }
        for (epoch, group) in by_epoch {
            let (losers, winner_pks) = self.partition_pk_winners(&group);
            // Tombstone pre-existing PK owners.
            for (key, &row_id) in &winner_pks {
                if let Some(old_rid) = self.hot.get(key) {
                    if old_rid != row_id {
                        self.tombstone_row(old_rid, epoch, false);
                    }
                }
            }
            for (key, row_id) in winner_pks {
                self.insert_hot_pk(key, row_id);
            }
            if self.schema.primary_key().is_none() {
                for r in &group {
                    self.hot.insert(r.row_id.0.to_be_bytes().to_vec(), r.row_id);
                }
            }
            for r in &group {
                if !losers.contains(&r.row_id) {
                    self.memtable.upsert(r.clone());
                    self.index_row(r);
                }
            }
        }
        for (rid, epoch) in deletes {
            self.memtable.tombstone(rid, epoch);
            self.remove_hot_for_row(rid, epoch);
        }
        let _ = self.rebuild_reservoir();
        Ok(())
    }

    /// Highest epoch whose data is durable in a sorted run (spec §7.1).
    pub(crate) fn flushed_epoch(&self) -> u64 {
        self.flushed_epoch
    }

    /// Validate that `cells` satisfy the schema's NOT NULL constraints.
    pub(crate) fn validate_cells_not_null(&self, cells: &[(u16, Value)]) -> Result<()> {
        let mut col_map = std::collections::HashMap::with_capacity(cells.len());
        for (c, v) in cells {
            col_map.insert(*c, v.clone());
        }
        self.schema.validate_not_null(&col_map)
    }

    /// Column-major NOT NULL validation for the bulk-load paths. Every schema
    /// column that is not marked NULLABLE must be present in `columns` and have
    /// no null validity bits over its first `n` rows.
    fn validate_columns_not_null(
        &self,
        columns: &[(u16, columnar::NativeColumn)],
        n: usize,
    ) -> Result<()> {
        let by_id: HashMap<u16, &columnar::NativeColumn> =
            columns.iter().map(|(id, c)| (*id, c)).collect();
        for col in &self.schema.columns {
            if col.flags.contains(ColumnFlags::NULLABLE) {
                continue;
            }
            match by_id.get(&col.id) {
                None => {
                    return Err(MongrelError::InvalidArgument(format!(
                        "column '{}' ({}) is NOT NULL but was omitted from the bulk load",
                        col.name, col.id
                    )));
                }
                Some(c) => {
                    if c.null_count(n) != 0 {
                        return Err(MongrelError::InvalidArgument(format!(
                            "column '{}' ({}) is NOT NULL but the bulk load contains nulls",
                            col.name, col.id
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// For a bulk-loaded batch, compute the row indices that survive primary-
    /// key upsert: for each PK value the last occurrence wins, earlier
    /// duplicates are dropped. Rows with a null PK value are always kept. Returns
    /// `None` when there is no primary key or no compaction is needed.
    fn bulk_pk_winner_indices(
        &self,
        columns: &[(u16, columnar::NativeColumn)],
        n: usize,
    ) -> Option<Vec<usize>> {
        let pk_col = self.schema.primary_key()?;
        let pk_id = pk_col.id;
        let pk_ty = pk_col.ty;
        let by_id: HashMap<u16, &columnar::NativeColumn> =
            columns.iter().map(|(id, c)| (*id, c)).collect();
        let pk_native = by_id.get(&pk_id)?;
        if native_int64_strictly_increasing(pk_native, n) {
            return None;
        }
        // key -> index of the last row that carried that PK value.
        let mut last: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut null_pk_rows: Vec<usize> = Vec::new();
        for i in 0..n {
            match bulk_index_key(&self.column_keys, pk_id, pk_ty, pk_native, i) {
                Some(key) => {
                    last.insert(key, i);
                }
                None => null_pk_rows.push(i),
            }
        }
        let mut winners: HashSet<usize> = last.values().copied().collect();
        for i in null_pk_rows {
            winners.insert(i);
        }
        Some((0..n).filter(|i| winners.contains(i)).collect())
    }

    /// Logically delete `row_id` (effective at the next commit).
    pub fn delete(&mut self, row_id: RowId) -> Result<()> {
        let epoch = self.pending_epoch();
        self.wal_append_data(Op::Delete {
            table_id: self.table_id,
            row_ids: vec![row_id],
        })?;
        if self.is_shared() {
            self.pending_dels.push(row_id);
        } else {
            self.apply_delete(row_id, epoch);
        }
        Ok(())
    }

    pub fn delete_returning(&mut self, row_id: RowId) -> Result<Option<OwnedRow>> {
        let pre = self.get(row_id, self.snapshot());
        self.delete(row_id)?;
        Ok(pre.map(|row| {
            let mut columns: Vec<_> = row.columns.into_iter().collect();
            columns.sort_by_key(|(id, _)| *id);
            OwnedRow { columns }
        }))
    }

    /// Durably remove every row in the table once the current write span commits.
    pub fn truncate(&mut self) -> Result<()> {
        let epoch = self.pending_epoch();
        self.wal_append_data(Op::TruncateTable {
            table_id: self.table_id,
        })?;
        self.pending_rows.clear();
        self.pending_rows_auto_inc.clear();
        self.pending_dels.clear();
        self.pending_truncate = Some(epoch);
        Ok(())
    }

    /// Apply an already-durable truncate without appending to the WAL.
    pub(crate) fn apply_truncate(&mut self, _epoch: Epoch) -> Result<()> {
        for rr in std::mem::take(&mut self.run_refs) {
            let _ = std::fs::remove_file(self.run_path(rr.run_id as u64));
        }
        for r in std::mem::take(&mut self.retiring) {
            let _ = std::fs::remove_file(self.run_path(r.run_id as u64));
        }
        self.memtable = Memtable::new();
        self.mutable_run = MutableRun::new();
        self.hot = HotIndex::new();
        let (bitmap, ann, fm, sparse) = empty_indexes(&self.schema);
        self.bitmap = bitmap;
        self.ann = ann;
        self.fm = fm;
        self.sparse = sparse;
        self.learned_range.clear();
        self.pk_by_row.clear();
        self.live_count = 0;
        self.reservoir = crate::reservoir::Reservoir::default();
        self.had_deletes = true;
        self.agg_cache.clear();
        self.global_idx_epoch = 0;
        self.indexes_complete = true;
        self.pending_delete_rids.clear();
        self.pending_put_cols.clear();
        self.pending_rows.clear();
        self.pending_rows_auto_inc.clear();
        self.pending_dels.clear();
        self.clear_result_cache();
        self.invalidate_index_checkpoint();
        Ok(())
    }

    /// Apply a tombstone (already-durable on the WAL) at `epoch` without
    /// appending to the per-table WAL. Used by the cross-table `Transaction`.
    pub(crate) fn apply_delete(&mut self, row_id: RowId, epoch: Epoch) {
        self.remove_hot_for_row(row_id, epoch);
        self.tombstone_row(row_id, epoch, true);
    }

    /// Tombstone `row_id` at `epoch`. When `adjust_live_count` is true the
    /// table's `live_count` is decremented (used on the live write path); during
    /// recovery the manifest is authoritative so the flag is false.
    fn tombstone_row(&mut self, row_id: RowId, epoch: Epoch, adjust_live_count: bool) {
        let tombstone = Row {
            row_id,
            committed_epoch: epoch,
            columns: std::collections::HashMap::new(),
            deleted: true,
        };
        self.memtable.upsert(tombstone);
        self.pk_by_row.remove(&row_id);
        if adjust_live_count {
            self.live_count = self.live_count.saturating_sub(1);
        }
        // Track for fine-grained cache invalidation (c).
        self.pending_delete_rids.insert(row_id.0 as u32);
        // A delete makes the incremental aggregate cache (row-id watermark
        // delta) unsafe — permanently disable it for this table.
        self.had_deletes = true;
        self.agg_cache.clear();
    }

    /// If `row_id` has a primary-key value and the HOT index currently maps
    /// that PK to this row id, remove the entry. Keeps the PK→RowId mapping
    /// consistent after deletes and before upserts.
    fn remove_hot_for_row(&mut self, row_id: RowId, epoch: Epoch) {
        let Some(pk_col) = self.schema.primary_key() else {
            return;
        };
        if let Some(key) = self.pk_by_row.remove(&row_id) {
            if self.hot.get(&key) == Some(row_id) {
                self.hot.remove(&key);
            }
            return;
        }
        if !self.indexes_complete {
            return;
        }
        // Use get_version (not get) so older visible versions can still reveal
        // the primary-key value that HOT needs to clean up.
        let pk_val = self
            .memtable
            .get_version(row_id, epoch)
            .and_then(|(_, r)| r.columns.get(&pk_col.id).cloned())
            .or_else(|| {
                self.mutable_run
                    .get_version(row_id, epoch)
                    .filter(|(_, r)| !r.deleted)
                    .and_then(|(_, r)| r.columns.get(&pk_col.id).cloned())
            })
            .or_else(|| {
                self.run_refs.iter().find_map(|rr| {
                    let mut reader = self.open_reader(rr.run_id).ok()?;
                    let (_, r) = reader.get_version(row_id, epoch).ok()??;
                    if r.deleted {
                        return None;
                    }
                    r.columns.get(&pk_col.id).cloned()
                })
            });
        if let Some(pk_val) = pk_val {
            let key = self.index_lookup_key(pk_col.id, &pk_val);
            if self.hot.get(&key) == Some(row_id) {
                self.hot.remove(&key);
            }
        }
    }

    /// For a batch of rows that share the same commit epoch, decide which rows
    /// win for each primary-key value. Returns the set of "loser" row ids that
    /// must be skipped/overwritten, and a map from PK lookup key to the winning
    /// row id. Rows without a PK value are always winners.
    fn partition_pk_winners(
        &self,
        rows: &[Row],
    ) -> (
        std::collections::HashSet<RowId>,
        std::collections::HashMap<Vec<u8>, RowId>,
    ) {
        let mut losers = std::collections::HashSet::new();
        let Some(pk_col) = self.schema.primary_key() else {
            return (losers, std::collections::HashMap::new());
        };
        let pk_id = pk_col.id;
        let mut winners: std::collections::HashMap<Vec<u8>, RowId> =
            std::collections::HashMap::new();
        for r in rows {
            let Some(pk_val) = r.columns.get(&pk_id) else {
                continue;
            };
            let key = self.index_lookup_key(pk_id, pk_val);
            if let Some(&old_rid) = winners.get(&key) {
                losers.insert(old_rid);
            }
            winners.insert(key, r.row_id);
        }
        (losers, winners)
    }

    fn index_row(&mut self, row: &Row) {
        if row.deleted {
            return;
        }
        let effective_row = self.tokenized_for_indexes(row);
        index_into(
            &self.schema,
            &effective_row,
            &mut self.hot,
            &mut self.bitmap,
            &mut self.ann,
            &mut self.fm,
            &mut self.sparse,
        );
    }

    /// Produce the row view that indexes should see. For ENCRYPTED_INDEXABLE
    /// equality (HMAC-eq) columns the plaintext value is replaced by its token,
    /// so the bitmap/HOT indexes store tokens. OPE-range columns keep their raw
    /// value (their range index is rebuilt from runs over plaintext). Plaintext
    /// tables return the row unchanged.
    fn tokenized_for_indexes(&self, row: &Row) -> Row {
        if self.column_keys.is_empty() {
            return row.clone();
        }
        #[cfg(feature = "encryption")]
        {
            use crate::encryption::SCHEME_HMAC_EQ;
            let mut tok = row.clone();
            for (&cid, &(_, scheme)) in &self.column_keys {
                if scheme != SCHEME_HMAC_EQ {
                    continue;
                }
                if let Some(v) = tok.columns.get(&cid).cloned() {
                    if let Some(t) = self.tokenize_value(cid, &v) {
                        tok.columns.insert(cid, t);
                    }
                }
            }
            tok
        }
        #[cfg(not(feature = "encryption"))]
        {
            row.clone()
        }
    }

    /// Group-commit: make all pending writes durable, advance the epoch so they
    /// become visible, and persist the manifest. Dispatches on the WAL sink: a
    /// standalone table fsyncs its private WAL; a mounted table seals into the
    /// shared WAL and defers the fsync to the group-commit coordinator (B1).
    pub fn commit(&mut self) -> Result<Epoch> {
        if self.is_shared() {
            self.commit_shared()
        } else {
            self.commit_private()
        }
    }

    /// Standalone commit: fsync the private WAL under the commit lock.
    fn commit_private(&mut self) -> Result<Epoch> {
        // Serialize the assign→fsync→publish critical section across all tables
        // sharing the epoch authority so `visible` is published strictly in
        // assigned order (the dual-counter invariant).
        let commit_lock = Arc::clone(&self.commit_lock);
        let _g = commit_lock.lock();
        let new_epoch = self.epoch.bump_assigned();
        let txn_id = self.current_txn_id;
        // Seal the staged records under a TxnCommit marker carrying the commit
        // epoch, then a single group fsync. Recovery applies only records whose
        // txn has a durable TxnCommit (uncommitted/torn tails are discarded).
        match &mut self.wal {
            WalSink::Private(w) => {
                w.append_txn(
                    txn_id,
                    Op::TxnCommit {
                        epoch: new_epoch.0,
                        added_runs: Vec::new(),
                    },
                )?;
                w.sync()?;
            }
            WalSink::Shared(_) => unreachable!("commit_private on a shared sink"),
        }
        // The truncate record is now durable; apply the physical clear.
        if let Some(epoch) = self.pending_truncate.take() {
            self.apply_truncate(epoch)?;
        }
        self.invalidate_pending_cache();
        self.persist_manifest(new_epoch)?;
        // Publish through the shared in-order gate so a `Table::commit` can never
        // advance the watermark past an in-flight cross-table transaction's
        // lower assigned epoch whose writes are not yet applied (spec §9.3e).
        self.epoch.publish_in_order(new_epoch);
        self.current_txn_id += 1;
        Ok(new_epoch)
    }

    /// Mounted commit (B1/B2): mirror the cross-table sequencer. Seal a
    /// `TxnCommit` into the shared WAL under the WAL lock (assigning the epoch in
    /// WAL-append order), make it durable via the group-commit coordinator (one
    /// leader fsync for the whole batch), then apply the staged rows at the
    /// assigned epoch and publish in order. Honors the shared poison flag.
    fn commit_shared(&mut self) -> Result<Epoch> {
        use std::sync::atomic::Ordering;
        let s = match &self.wal {
            WalSink::Shared(s) => s.clone(),
            WalSink::Private(_) => unreachable!("commit_shared on a private sink"),
        };
        if s.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }
        // Serialize the whole single-table commit critical section (assign →
        // durable → publish) under the shared commit lock so concurrent
        // `Table::commit`s publish strictly in assigned order and each returns
        // only once its epoch is visible (read-your-writes after commit). The
        // fsync still defers to the group-commit coordinator, which can batch a
        // held commit with concurrent cross-table `transaction()` committers.
        let commit_lock = Arc::clone(&self.commit_lock);
        let _g = commit_lock.lock();
        // Always seal a txn (allocating an id if this span had no writes) so the
        // epoch advances monotonically like the standalone path.
        let txn_id = self.ensure_txn_id();
        let (new_epoch, commit_seq) = {
            let mut wal = s.wal.lock();
            let new_epoch = self.epoch.bump_assigned();
            let seq = wal.append_commit(txn_id, new_epoch, &[])?;
            (new_epoch, seq)
        };
        s.group
            .await_durable(&s.wal, commit_seq)
            .inspect_err(|_| s.poisoned.store(true, Ordering::Relaxed))?;

        // Apply staged truncate/rows/tombstones at the real assigned epoch (B2): nothing
        // was stamped speculatively, and nothing is visible until publish below.
        if self.pending_truncate.take().is_some() {
            self.apply_truncate(new_epoch)?;
        }
        let mut rows = std::mem::take(&mut self.pending_rows);
        if !rows.is_empty() {
            for r in &mut rows {
                r.committed_epoch = new_epoch;
            }
            let auto_inc_flags = std::mem::take(&mut self.pending_rows_auto_inc);
            let all_auto_generated =
                auto_inc_flags.len() == rows.len() && auto_inc_flags.iter().all(|b| *b);
            self.apply_put_rows_inner(rows, !all_auto_generated)?;
        } else {
            self.pending_rows_auto_inc.clear();
        }
        let dels = std::mem::take(&mut self.pending_dels);
        for rid in dels {
            self.apply_delete(rid, new_epoch);
        }

        self.invalidate_pending_cache();
        self.persist_manifest(new_epoch)?;
        self.epoch.publish_in_order(new_epoch);
        // Next auto-commit span allocates a fresh shared txn id.
        self.current_txn_id = 0;
        Ok(new_epoch)
    }

    /// Commit, then drain the memtable into the mutable-run LSM tier (Phase
    /// 11.1). The tier absorbs flushes in place and only spills to an immutable
    /// `.sr` sorted run once it crosses the spill watermark — coalescing many
    /// small flushes into fewer, larger runs. While the tier holds un-spilled
    /// data the WAL is **not** rotated: the Flush marker / WAL rotation is
    /// deferred until the data is durably in a run, so crash recovery replays
    /// those rows back into the memtable (the tier rebuilds from replay).
    pub fn flush(&mut self) -> Result<Epoch> {
        self.ensure_indexes_complete()?;
        let epoch = self.commit()?;
        let rows = self.memtable.drain_sorted();
        if !rows.is_empty() {
            self.mutable_run.insert_many(rows);
        }
        if self.mutable_run.approx_bytes() >= self.mutable_run_spill_bytes {
            self.spill_mutable_run(epoch)?;
            // The tier is now empty and its data is durably in a run → safe to
            // mark the WAL flushed (and, for a private WAL, rotate to a fresh
            // segment so the flushed records aren't replayed).
            self.mark_flushed(epoch)?;
            self.persist_manifest(epoch)?;
            self.build_learned_ranges()?;
            // Memtable is drained and runs are stable → checkpoint the indexes so
            // the next open skips the full run scan (Phase 9.1).
            self.checkpoint_indexes(epoch);
        }
        // else: data coalesced in the in-memory tier; the WAL still covers it
        // and the manifest epoch was already persisted by `commit`.
        Ok(epoch)
    }

    /// Mark `epoch` as flushed: append a `Flush` marker to the WAL, advance
    /// `flushed_epoch`, and — for a private WAL only — rotate to a fresh segment
    /// so the now-durable-in-a-run records are not replayed. A mounted table's
    /// shared WAL is never rotated per-table; recovery skips its already-flushed
    /// records via the manifest `flushed_epoch` gate, and segment GC (B3c) reaps
    /// them once every table has flushed past them.
    fn mark_flushed(&mut self, epoch: Epoch) -> Result<()> {
        let op = Op::Flush {
            table_id: self.table_id,
            flushed_epoch: epoch.0,
        };
        match &mut self.wal {
            WalSink::Private(w) => {
                w.append_system(op)?;
                w.sync()?;
            }
            WalSink::Shared(s) => {
                // Informational in the shared log (recovery gates on the manifest
                // `flushed_epoch`); not separately fsynced — the run + manifest
                // are the durability point and the underlying rows were already
                // fsynced at their commit.
                s.wal.lock().append_system(op)?;
            }
        }
        self.flushed_epoch = epoch.0;
        if matches!(self.wal, WalSink::Private(_)) {
            self.rotate_wal(epoch)?;
        }
        Ok(())
    }

    /// Spill the mutable-run tier to a new immutable level-0 sorted run. The
    /// caller owns the Flush-marker / WAL-rotation / manifest steps (only valid
    /// once all in-flight data is in runs). No-op when the tier is empty.
    fn spill_mutable_run(&mut self, epoch: Epoch) -> Result<()> {
        let rows = self.mutable_run.drain_sorted();
        if rows.is_empty() {
            return Ok(());
        }
        let run_id = self.next_run_id;
        self.next_run_id += 1;
        let path = self.run_path(run_id);
        let mut writer = RunWriter::new(&self.schema, run_id as u128, epoch, 0);
        if let Some(kek) = &self.kek {
            writer = writer.with_encryption(kek.as_ref(), self.indexable_column_specs());
        }
        let header = writer.write(&path, &rows)?;
        self.run_refs.push(RunRef {
            run_id: run_id as u128,
            level: 0,
            epoch_created: epoch.0,
            row_count: header.row_count,
        });
        Ok(())
    }

    /// Tune the mutable-run spill watermark (bytes). A smaller threshold spills
    /// sooner (more, smaller runs — closer to the pre-Phase-11.1 behavior); a
    /// larger one coalesces more flushes in memory.
    pub fn set_mutable_run_spill_bytes(&mut self, bytes: u64) {
        self.mutable_run_spill_bytes = bytes.max(1);
    }

    /// Set the zstd compression level for compaction output (Phase 18.1).
    /// Default 3; higher values give better compression ratio at the cost of
    /// slower compaction.
    pub fn set_compaction_zstd_level(&mut self, level: i32) {
        self.compaction_zstd_level = level;
    }

    /// Set the result-cache byte budget (Phase 19.1 hardening (a)). Entries are
    /// evicted in access-order LRU past this limit. Takes effect immediately
    /// (may evict entries if the new limit is smaller than the current footprint).
    pub fn set_result_cache_max_bytes(&mut self, max_bytes: u64) {
        self.result_cache.lock().set_max_bytes(max_bytes);
    }

    /// Drop every cached result (used by compaction, schema evolution, and bulk
    /// load — paths that change run layout or data without going through the
    /// fine-grained `pending_*` tracking).
    pub(crate) fn clear_result_cache(&mut self) {
        self.result_cache.lock().clear();
    }

    /// Number of versions currently held in the mutable-run tier.
    pub fn mutable_run_len(&self) -> usize {
        self.mutable_run.len()
    }

    /// Drain every version from the mutable-run tier (ascending `(RowId,
    /// Epoch)` order). Used by compaction to fold the tier into its merge.
    pub(crate) fn drain_mutable_run(&mut self) -> Vec<Row> {
        self.mutable_run.drain_sorted()
    }

    /// Bulk-load: write `batch` directly to a new sorted run, bypassing the WAL
    /// and the memtable entirely (no per-row bincode, no skip-list inserts). The
    /// run + a rotated WAL + the manifest are fsynced once — the fast ingest
    /// path for large analytical loads. Indexes are still maintained.
    pub fn bulk_load(&mut self, batch: Vec<Vec<(u16, Value)>>) -> Result<Epoch> {
        let epoch = self.commit()?;
        let n = batch.len();
        if n == 0 {
            return Ok(epoch);
        }
        let live_before = self.live_count;
        // Spill any pending mutable-run data first: bulk_load writes a Flush
        // marker + rotates the WAL below, which is only safe once all in-flight
        // data is durably in a run.
        self.spill_mutable_run(epoch)?;
        let eager_index_build = self.indexes_complete
            && self.run_refs.is_empty()
            && self.memtable.is_empty()
            && self.mutable_run.is_empty();
        // Phase 14.7: route the legacy Value API through the same parallel
        // encode + typed batch-index path as `bulk_load_columns`. Transpose the
        // row-major sparse batch → column-major typed columns (in parallel),
        // then `write_native` + `index_columns_bulk`, instead of per-row
        // `Row { HashMap }` + `index_into` + the sequential `Value` writer.
        let mut user_columns: Vec<(u16, columnar::NativeColumn)> = {
            use rayon::prelude::*;
            use std::collections::HashMap;
            let mut by_col: HashMap<u16, Vec<Value>> = HashMap::new();
            for cdef in &self.schema.columns {
                by_col.insert(cdef.id, vec![Value::Null; n]);
            }
            for (i, row) in batch.iter().enumerate() {
                for (id, v) in row {
                    if let Some(col) = by_col.get_mut(id) {
                        col[i] = v.clone();
                    }
                }
            }
            self.schema
                .columns
                .par_iter()
                .map(|cdef| {
                    let vals = by_col.get(&cdef.id).map(|v| v.as_slice()).unwrap_or(&[]);
                    (cdef.id, columnar::values_to_native(cdef.ty, vals))
                })
                .collect::<Vec<_>>()
        };
        // Enforce NOT NULL constraints and primary-key upsert semantics before
        // any row id is allocated or bytes hit the run file. Losers of a
        // duplicate primary key are dropped from the encoded run entirely so
        // the dedup survives reopen (no ephemeral memtable tombstone).
        self.fill_auto_inc_native_columns(&mut user_columns, n)?;
        self.validate_columns_not_null(&user_columns, n)?;
        let winner_idx = self
            .bulk_pk_winner_indices(&user_columns, n)
            .and_then(|idx| if idx.len() == n { None } else { Some(idx) });
        let (write_columns, write_n): (Vec<(u16, columnar::NativeColumn)>, usize) =
            match winner_idx.as_deref() {
                Some(idx) => {
                    let compacted = user_columns
                        .iter()
                        .map(|(id, c)| (*id, c.gather(idx)))
                        .collect();
                    (compacted, idx.len())
                }
                None => (std::mem::take(&mut user_columns), n),
            };
        self.advance_auto_inc_from_native_columns(&write_columns, write_n, live_before)?;
        let first = self.allocator.alloc_range(write_n as u64).0;
        for rid in first..first + write_n as u64 {
            self.reservoir.offer(rid);
        }
        let run_id = self.next_run_id;
        self.next_run_id += 1;
        let path = self.run_path(run_id);
        let mut writer = RunWriter::new(&self.schema, run_id as u128, epoch, 0)
            .clean(true)
            .with_lz4()
            .with_native_endian();
        if let Some(kek) = &self.kek {
            writer = writer.with_encryption(kek.as_ref(), self.indexable_column_specs());
        }
        let header = writer.write_native(&path, &write_columns, write_n, first)?;
        self.run_refs.push(RunRef {
            run_id: run_id as u128,
            level: 0,
            epoch_created: epoch.0,
            row_count: header.row_count,
        });
        self.live_count = self.live_count.saturating_add(write_n as u64);
        if eager_index_build {
            let row_ids: Vec<u64> = (first..first + write_n as u64).collect();
            self.index_columns_bulk(&write_columns, &row_ids);
            self.indexes_complete = true;
            self.build_learned_ranges()?;
        } else {
            self.indexes_complete = false;
        }
        self.mark_flushed(epoch)?;
        self.persist_manifest(epoch)?;
        if eager_index_build {
            self.checkpoint_indexes(epoch);
        }
        self.clear_result_cache();
        Ok(epoch)
    }

    /// Rotate the private WAL to a fresh segment. Only valid for a standalone
    /// table — a mounted table never rotates the shared WAL per-table.
    fn rotate_wal(&mut self, epoch: Epoch) -> Result<()> {
        let segment = next_wal_segment(&self.dir.join(WAL_DIR))?;
        let cipher = self.wal_dek.as_ref().map(|dk| make_cipher(dk));
        // The segment number (from the filename) namespaces nonces under the
        // constant WAL DEK — pass it through to the writer.
        let segment_no = segment
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.strip_prefix("seg-"))
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let mut wal = Wal::create_with_cipher(segment, epoch, cipher, segment_no)?;
        wal.set_sync_byte_threshold(self.sync_byte_threshold);
        wal.sync()?;
        self.wal = WalSink::Private(wal);
        Ok(())
    }

    /// Fine-grained result-cache invalidation (hardening (c)): drop only
    /// entries whose footprint intersects a deleted RowId or whose
    /// condition-columns intersect a mutated column, then clear the pending
    /// sets. Called by `commit` and the cross-table transaction path.
    pub(crate) fn invalidate_pending_cache(&mut self) {
        self.result_cache
            .lock()
            .invalidate(&self.pending_delete_rids, &self.pending_put_cols);
        self.pending_delete_rids.clear();
        self.pending_put_cols.clear();
    }

    pub(crate) fn persist_manifest(&self, epoch: Epoch) -> Result<()> {
        let mut m = Manifest::new(self.table_id, self.schema.schema_id);
        m.current_epoch = epoch.0;
        m.next_row_id = self.allocator.current().0;
        m.runs = self.run_refs.clone();
        m.live_count = self.live_count;
        m.global_idx_epoch = self.global_idx_epoch;
        m.flushed_epoch = self.flushed_epoch;
        m.retiring = self.retiring.clone();
        // Persist the authoritative counter only when seeded; otherwise write 0
        // so the next open still scans `max(PK)` on first use (an unseeded
        // lower bound from WAL replay is not safe to trust across a flush).
        m.auto_inc_next = match self.auto_inc {
            Some(ai) if ai.seeded => ai.next,
            _ => 0,
        };
        let meta_dek = self.manifest_meta_dek();
        manifest::write_atomic(&self.dir, &mut m, meta_dek.as_ref())?;
        Ok(())
    }

    /// Checkpoint the in-memory secondary indexes to `_idx/global.idx` and stamp
    /// the manifest's `global_idx_epoch` (Phase 9.1). Call after the runs are
    /// stable and the memtable is drained (flush/bulk-load/compact) so the
    /// checkpoint exactly matches the run data; subsequent [`Table::open`] loads it
    /// directly instead of scanning every run.
    pub(crate) fn checkpoint_indexes(&mut self, epoch: Epoch) {
        // Never persist an incomplete index set (e.g. after bulk_load_columns,
        // which bypasses per-row indexing) — reopen rebuilds from the runs.
        if !self.indexes_complete {
            return;
        }
        let snap = global_idx::IndexSnapshot {
            hot: &self.hot,
            bitmap: &self.bitmap,
            ann: &self.ann,
            fm: &self.fm,
            sparse: &self.sparse,
            learned_range: &self.learned_range,
        };
        // Best-effort: a failed checkpoint just means the next open rebuilds.
        let idx_dek = self.idx_dek();
        if global_idx::write_atomic(&self.dir, self.table_id, epoch.0, snap, idx_dek.as_deref())
            .is_ok()
        {
            self.global_idx_epoch = epoch.0;
            let _ = self.persist_manifest(epoch);
        }
    }

    /// Drop any on-disk index checkpoint so the next open rebuilds from runs
    /// (used when the live indexes are known stale, e.g. compaction to empty).
    pub(crate) fn invalidate_index_checkpoint(&mut self) {
        self.global_idx_epoch = 0;
        global_idx::remove(&self.dir);
        let _ = self.persist_manifest(self.epoch.visible());
    }

    /// Read the row at `row_id` visible to `snapshot`, merging the newest
    /// version across the memtable and all sorted runs.
    pub fn get(&self, row_id: RowId, snapshot: Snapshot) -> Option<Row> {
        let mut best: Option<(Epoch, Row)> = self.memtable.get_version(row_id, snapshot.epoch);
        if let Some((epoch, row)) = self.mutable_run.get_version(row_id, snapshot.epoch) {
            if best.as_ref().map(|(be, _)| epoch > *be).unwrap_or(true) {
                best = Some((epoch, row));
            }
        }
        for rr in &self.run_refs {
            let Ok(mut reader) = self.open_reader(rr.run_id) else {
                continue;
            };
            let Ok(Some((epoch, row))) = reader.get_version(row_id, snapshot.epoch) else {
                continue;
            };
            if best.as_ref().map(|(be, _)| epoch > *be).unwrap_or(true) {
                best = Some((epoch, row));
            }
        }
        match best {
            Some((_, r)) if r.deleted => None,
            Some((_, r)) => Some(r),
            None => None,
        }
    }

    /// All rows visible at `snapshot` (newest version per `RowId`, tombstones
    /// dropped), merged across the memtable, the mutable-run tier, and all
    /// runs. Ascending `RowId`.
    pub fn visible_rows(&self, snapshot: Snapshot) -> Result<Vec<Row>> {
        let mut best: HashMap<u64, (Epoch, Row)> = HashMap::new();
        let mut fold = |row: Row| {
            best.entry(row.row_id.0)
                .and_modify(|e| {
                    if row.committed_epoch > e.0 {
                        *e = (row.committed_epoch, row.clone());
                    }
                })
                .or_insert_with(|| (row.committed_epoch, row));
        };
        for row in self.memtable.visible_versions(snapshot.epoch) {
            fold(row);
        }
        for row in self.mutable_run.visible_versions(snapshot.epoch) {
            fold(row);
        }
        for rr in &self.run_refs {
            let mut reader = self.open_reader(rr.run_id)?;
            for row in reader.visible_versions(snapshot.epoch)? {
                fold(row);
            }
        }
        let mut out: Vec<Row> = best
            .into_values()
            .filter_map(|(_, r)| if r.deleted { None } else { Some(r) })
            .collect();
        out.sort_by_key(|r| r.row_id);
        Ok(out)
    }

    /// Visible data as columns (column_id → values) rather than rows — the
    /// vectorized scan path. Fast path: when the memtable is empty and there is
    /// exactly one run (the common post-flush analytical case), it computes the
    /// visible index set once and gathers each column, with **no per-row
    /// `HashMap`/`Row` materialization**. Falls back to [`Self::visible_rows`]
    /// pivoted to columns when the memtable is live or runs overlap.
    pub fn visible_columns(&self, snapshot: Snapshot) -> Result<Vec<(u16, Vec<Value>)>> {
        if self.memtable.is_empty() && self.mutable_run.is_empty() && self.run_refs.len() == 1 {
            let rr = self.run_refs[0].clone();
            let mut reader = self.open_reader(rr.run_id)?;
            let idxs = reader.visible_indices(snapshot.epoch)?;
            let mut cols = Vec::with_capacity(self.schema.columns.len());
            for cdef in &self.schema.columns {
                cols.push((cdef.id, reader.gather_column(cdef.id, &idxs)?));
            }
            return Ok(cols);
        }
        // Fallback: row merge, then pivot to columns.
        let rows = self.visible_rows(snapshot)?;
        let mut cols: Vec<(u16, Vec<Value>)> = self
            .schema
            .columns
            .iter()
            .map(|c| (c.id, Vec::with_capacity(rows.len())))
            .collect();
        for r in &rows {
            for (cid, vec) in cols.iter_mut() {
                vec.push(r.columns.get(cid).cloned().unwrap_or(Value::Null));
            }
        }
        Ok(cols)
    }

    /// Resolve a primary-key value to a row id (latest version).
    pub fn lookup_pk(&self, key: &[u8]) -> Option<RowId> {
        self.hot.get(key)
    }

    /// Run a conjunctive query over the shared row-id space: each condition
    /// yields a candidate row-id set, the sets are intersected, and the
    /// survivors are materialized at the current snapshot. This is the AI-native
    /// "compose primitives" surface (`semsearch ∩ fm_contains ∩ cat_in`).
    pub fn query(&mut self, q: &crate::query::Query) -> Result<Vec<Row>> {
        self.ensure_indexes_complete()?;
        let snapshot = self.snapshot();
        crate::trace::QueryTrace::record(|t| {
            t.run_count = self.run_refs.len();
            t.memtable_rows = self.memtable.len();
            t.mutable_run_rows = self.mutable_run.len();
        });
        // A conjunction with no predicates matches every visible row (the
        // documented "Empty ⇒ all rows" contract); `intersect_sets` of zero
        // sets would otherwise wrongly yield the empty set.
        if q.conditions.is_empty() {
            crate::trace::QueryTrace::record(|t| {
                t.scan_mode = crate::trace::ScanMode::Materialized;
                t.row_materialized = true;
            });
            return self.visible_rows(snapshot);
        }
        crate::trace::QueryTrace::record(|t| {
            t.conditions_pushed = q.conditions.len();
            t.scan_mode = crate::trace::ScanMode::Materialized;
            t.row_materialized = true;
        });
        let mut sets: Vec<RowIdSet> = Vec::with_capacity(q.conditions.len());
        for c in &q.conditions {
            sets.push(self.resolve_condition(c, snapshot)?);
        }
        let rids = RowIdSet::intersect_many(sets).into_sorted_vec();
        self.rows_for_rids(&rids, snapshot)
    }

    /// Materialize the MVCC-visible, non-deleted rows for `rids` at `snapshot`,
    /// preserving the input order. Rows whose newest visible version is a
    /// tombstone, or that no longer exist, are omitted. Shared by index-served
    /// [`query`] and the Phase 8.1 FK-join path.
    pub fn rows_for_rids(&self, rids: &[u64], snapshot: Snapshot) -> Result<Vec<Row>> {
        use std::collections::HashMap;
        let mut rows = Vec::with_capacity(rids.len());
        // Overlay (memtable + mutable-run) newest visible version per rid —
        // these shadow any stale version stored in a run. A rid may have an
        // older version in the mutable-run tier and a newer one in the memtable
        // (an update after a flush), so keep the **newest by epoch** across both
        // tiers, not whichever is inserted last.
        let mut overlay: HashMap<u64, Row> = HashMap::new();
        let fold_newest = |row: Row, overlay: &mut HashMap<u64, Row>| {
            overlay
                .entry(row.row_id.0)
                .and_modify(|e| {
                    if row.committed_epoch > e.committed_epoch {
                        *e = row.clone();
                    }
                })
                .or_insert(row);
        };
        for row in self.memtable.visible_versions(snapshot.epoch) {
            fold_newest(row, &mut overlay);
        }
        for row in self.mutable_run.visible_versions(snapshot.epoch) {
            fold_newest(row, &mut overlay);
        }
        if self.run_refs.len() == 1 {
            // Phase 16.3b: decode the system columns ONCE (via the clean-run-
            // shortcut visibility pass) and binary-search each requested rid,
            // instead of `get_version`-per-rid which re-decoded + cloned the
            // full system columns on every call (the ~350 ms native-query tax).
            // Phase 16.3b finish: batch the survivor positions into ONE
            // `materialize_batch` call so user columns are decoded once each via
            // the typed, page-cached path (not a per-rid `Vec<Value>` decode +
            // `.cloned()`).
            let mut reader = self.open_reader(self.run_refs[0].run_id)?;
            let (positions, vis_rids) = reader.visible_positions_with_rids(snapshot.epoch)?;
            // First pass: classify each input rid (overlay / run position /
            // not-found), recording the run positions to fetch in input order.
            enum Src {
                Overlay,
                Run,
            }
            let mut plan: Vec<Src> = Vec::with_capacity(rids.len());
            let mut fetch: Vec<usize> = Vec::with_capacity(rids.len());
            for rid in rids {
                if overlay.contains_key(rid) {
                    plan.push(Src::Overlay);
                    continue;
                }
                match vis_rids.binary_search(&(*rid as i64)) {
                    Ok(i) => {
                        plan.push(Src::Run);
                        fetch.push(positions[i]);
                    }
                    Err(_) => { /* not found — omitted from output */ }
                }
            }
            let fetched = reader.materialize_batch(&fetch)?;
            let mut fetched_iter = fetched.into_iter();
            for (rid, src) in rids.iter().zip(plan) {
                match src {
                    Src::Overlay => {
                        if let Some(r) = overlay.get(rid) {
                            if !r.deleted {
                                rows.push(r.clone());
                            }
                        }
                    }
                    Src::Run => {
                        if let Some(row) = fetched_iter.next() {
                            if !row.deleted {
                                rows.push(row);
                            }
                        }
                    }
                }
            }
            return Ok(rows);
        }
        // Multi-run: one reader per run; newest visible version across all runs
        // + the overlay. (Per-rid `get_version` here is unavoidable without a
        // cross-run merge, but multi-run is the uncommon cold case.)
        let mut readers: Vec<_> = self
            .run_refs
            .iter()
            .map(|rr| self.open_reader(rr.run_id))
            .collect::<Result<Vec<_>>>()?;
        for rid in rids {
            if let Some(r) = overlay.get(rid) {
                if !r.deleted {
                    rows.push(r.clone());
                }
                continue;
            }
            let mut best: Option<(Epoch, Row)> = None;
            for reader in readers.iter_mut() {
                if let Ok(Some((epoch, row))) = reader.get_version(RowId(*rid), snapshot.epoch) {
                    if best.as_ref().map(|(be, _)| epoch > *be).unwrap_or(true) {
                        best = Some((epoch, row));
                    }
                }
            }
            if let Some((_, r)) = best {
                if !r.deleted {
                    rows.push(r);
                }
            }
        }
        Ok(rows)
    }

    /// Resolve the referencing (FK) side of a primary-key ↔ foreign-key join as
    /// a row-id set (Phase 8.1): union the roaring-bitmap entries of
    /// `fk_column_id` for every value in `pk_values` — the surviving
    /// primary-key values — then intersect with `fk_conditions`, i.e. any
    /// FK-side predicates (`ann_search ∩ fm_contains`, bitmap equality, range,
    /// …). Returns the survivor row-ids ascending. Requires a bitmap index on
    /// `fk_column_id`; returns an empty set when there is none.
    /// Whether live indexes are complete (Phase 14.7 + 17.2: the broadcast
    /// join path checks this before using the HOT index).
    pub fn indexes_complete(&self) -> bool {
        self.indexes_complete
    }

    /// Phase 17.2: broadcast join — return the distinct values in this table's
    /// bitmap index for `column_id` that also exist as a key in `pk_db`'s HOT
    /// index. Avoids loading the entire PK table when the FK column has low
    /// cardinality. Returns `None` if no bitmap index exists for the column.
    pub fn broadcast_join_values(&self, column_id: u16, pk_db: &Table) -> Option<Vec<Vec<u8>>> {
        let b = self.bitmap.get(&column_id)?;
        let result: Vec<Vec<u8>> = b
            .keys()
            .into_iter()
            .filter(|k| pk_db.hot.get(k.as_slice()).is_some())
            .cloned()
            .collect();
        Some(result)
    }

    pub fn fk_join_row_ids(
        &self,
        fk_column_id: u16,
        pk_values: &[Vec<u8>],
        fk_conditions: &[crate::query::Condition],
        snapshot: Snapshot,
    ) -> Result<Vec<u64>> {
        let Some(b) = self.bitmap.get(&fk_column_id) else {
            return Ok(Vec::new());
        };
        let mut join_set = {
            let mut acc = roaring::RoaringBitmap::new();
            for v in pk_values {
                acc |= b.get(v);
            }
            RowIdSet::from_roaring(acc)
        };
        if !fk_conditions.is_empty() {
            let mut sets: Vec<RowIdSet> = Vec::with_capacity(fk_conditions.len() + 1);
            sets.push(join_set);
            for c in fk_conditions {
                sets.push(self.resolve_condition(c, snapshot)?);
            }
            join_set = RowIdSet::intersect_many(sets);
        }
        Ok(join_set.into_sorted_vec())
    }

    /// Like [`fk_join_row_ids`] but returns only the **cardinality** of the FK
    /// survivor set — without materializing or sorting it. For a bare
    /// `COUNT(*)` join with no FK-side filter this is O(1) on the bitmap union
    /// (Phase 17.4): the prior path built a `HashSet<u64>` + `Vec<u64>` +
    /// `sort_unstable` over up to N rows only to read `.len()`.
    pub fn fk_join_count(
        &self,
        fk_column_id: u16,
        pk_values: &[Vec<u8>],
        fk_conditions: &[crate::query::Condition],
        snapshot: Snapshot,
    ) -> Result<u64> {
        let Some(b) = self.bitmap.get(&fk_column_id) else {
            return Ok(0);
        };
        let mut acc = roaring::RoaringBitmap::new();
        for v in pk_values {
            acc |= b.get(v);
        }
        if fk_conditions.is_empty() {
            return Ok(acc.len());
        }
        let mut sets: Vec<RowIdSet> = Vec::with_capacity(fk_conditions.len() + 1);
        sets.push(RowIdSet::from_roaring(acc));
        for c in fk_conditions {
            sets.push(self.resolve_condition(c, snapshot)?);
        }
        Ok(RowIdSet::intersect_many(sets).len() as u64)
    }

    /// Resolve a single condition to its row-id set. Index-served conditions use
    /// the in-memory indexes; `Range`/`RangeF64` prefer the learned (PGM) index
    /// or the reader's page-index-skipping path on the single-run fast path, and
    /// only fall back to a `visible_rows` scan off the fast path (multi-run).
    fn resolve_condition(
        &self,
        c: &crate::query::Condition,
        snapshot: Snapshot,
    ) -> Result<RowIdSet> {
        use crate::query::Condition;
        Ok(match c {
            Condition::Pk(key) => {
                let lookup = self
                    .schema
                    .primary_key()
                    .map(|pk| self.index_lookup_key_bytes(pk.id, key))
                    .unwrap_or_else(|| key.clone());
                self.hot
                    .get(&lookup)
                    .map(|r| RowIdSet::one(r.0))
                    .unwrap_or_else(RowIdSet::empty)
            }
            Condition::BitmapEq { column_id, value } => {
                let lookup = self.index_lookup_key_bytes(*column_id, value);
                self.bitmap
                    .get(column_id)
                    .map(|b| RowIdSet::from_roaring(b.get(&lookup)))
                    .unwrap_or_else(RowIdSet::empty)
            }
            Condition::BitmapIn { column_id, values } => {
                let bm = self.bitmap.get(column_id);
                let mut acc = roaring::RoaringBitmap::new();
                if let Some(b) = bm {
                    for v in values {
                        let lookup = self.index_lookup_key_bytes(*column_id, v);
                        acc |= b.get(&lookup);
                    }
                }
                RowIdSet::from_roaring(acc)
            }
            Condition::FmContains { column_id, pattern } => self
                .fm
                .get(column_id)
                .map(|f| {
                    RowIdSet::from_unsorted(f.locate(pattern).into_iter().map(|r| r.0).collect())
                })
                .unwrap_or_else(RowIdSet::empty),
            Condition::FmContainsAll {
                column_id,
                patterns,
            } => {
                // Multi-segment intersection (Priority 12): resolve each segment
                // via FM and intersect — much tighter than the single longest.
                if let Some(f) = self.fm.get(column_id) {
                    let sets: Vec<RowIdSet> = patterns
                        .iter()
                        .map(|pat| {
                            RowIdSet::from_unsorted(
                                f.locate(pat).into_iter().map(|r| r.0).collect(),
                            )
                        })
                        .collect();
                    RowIdSet::intersect_many(sets)
                } else {
                    RowIdSet::empty()
                }
            }
            Condition::Ann {
                column_id,
                query,
                k,
            } => self
                .ann
                .get(column_id)
                .map(|a| {
                    RowIdSet::from_unsorted(
                        a.search(query, *k).into_iter().map(|(r, _)| r.0).collect(),
                    )
                })
                .unwrap_or_else(RowIdSet::empty),
            Condition::SparseMatch {
                column_id,
                query,
                k,
            } => self
                .sparse
                .get(column_id)
                .map(|s| {
                    RowIdSet::from_unsorted(
                        s.search(query, *k).into_iter().map(|(r, _)| r.0).collect(),
                    )
                })
                .unwrap_or_else(RowIdSet::empty),
            Condition::Range { column_id, lo, hi } => {
                // Build the candidate set from the durable tier — the learned
                // index (built from sorted runs) or a single page-pruned run —
                // then merge the memtable/mutable-run overlay. An overlay row
                // supersedes its run version (it may have been updated out of
                // range or deleted), so overlay rids are dropped from the run
                // set and re-evaluated from the overlay directly. Without this
                // merge, rows still in the memtable are invisible to a ranged
                // read whenever a LearnedRange index is present.
                let mut set = if let Some(li) = self.learned_range.get(column_id) {
                    RowIdSet::from_unsorted(li.range(*lo, *hi).into_iter().collect())
                } else if self.run_refs.len() == 1 {
                    let mut r = self.open_reader(self.run_refs[0].run_id)?;
                    r.range_row_id_set_i64(*column_id, *lo, *hi)?
                } else {
                    return self.range_scan_i64(*column_id, *lo, *hi, snapshot);
                };
                set.remove_many(self.overlay_rid_set(snapshot));
                self.range_scan_overlay_i64(&mut set, *column_id, *lo, *hi, snapshot);
                set
            }
            Condition::RangeF64 {
                column_id,
                lo,
                lo_inclusive,
                hi,
                hi_inclusive,
            } => {
                // See the `Range` arm: merge the overlay over the durable
                // candidate set so memtable/mutable-run rows are visible.
                let mut set = if let Some(li) = self.learned_range.get(column_id) {
                    RowIdSet::from_unsorted(
                        li.range_f64(*lo, *lo_inclusive, *hi, *hi_inclusive)
                            .into_iter()
                            .collect(),
                    )
                } else if self.run_refs.len() == 1 {
                    let mut r = self.open_reader(self.run_refs[0].run_id)?;
                    r.range_row_id_set_f64(*column_id, *lo, *lo_inclusive, *hi, *hi_inclusive)?
                } else {
                    return self.range_scan_f64(
                        *column_id,
                        *lo,
                        *lo_inclusive,
                        *hi,
                        *hi_inclusive,
                        snapshot,
                    );
                };
                set.remove_many(self.overlay_rid_set(snapshot));
                self.range_scan_overlay_f64(
                    &mut set,
                    *column_id,
                    *lo,
                    *lo_inclusive,
                    *hi,
                    *hi_inclusive,
                    snapshot,
                );
                set
            }
            Condition::IsNull { column_id } => {
                let mut set = if self.run_refs.len() == 1 {
                    let mut r = self.open_reader(self.run_refs[0].run_id)?;
                    r.null_row_id_set(*column_id, true)?
                } else {
                    return self.null_scan(*column_id, true, snapshot);
                };
                set.remove_many(self.overlay_rid_set(snapshot));
                self.null_scan_overlay(&mut set, *column_id, true, snapshot);
                set
            }
            Condition::IsNotNull { column_id } => {
                let mut set = if self.run_refs.len() == 1 {
                    let mut r = self.open_reader(self.run_refs[0].run_id)?;
                    r.null_row_id_set(*column_id, false)?
                } else {
                    return self.null_scan(*column_id, false, snapshot);
                };
                set.remove_many(self.overlay_rid_set(snapshot));
                self.null_scan_overlay(&mut set, *column_id, false, snapshot);
                set
            }
        })
    }

    /// Vectorized range scan for Int64 columns (Phase 13.2 / 16.3). Resolves the
    /// survivor set via the reader's **page-pruned** path — pages whose `[min,max]`
    /// excludes `[lo,hi]` are never decoded — restricted to MVCC-visible rows.
    /// This is layout-independent: correct under any memtable / multi-run state,
    /// so it is always safe to call (no "single clean run" gate). Overlay rows
    /// (memtable / mutable-run) are excluded from the run portion and checked
    /// directly via [`Self::range_scan_overlay_i64`].
    fn range_scan_i64(
        &self,
        column_id: u16,
        lo: i64,
        hi: i64,
        snapshot: Snapshot,
    ) -> Result<RowIdSet> {
        let mut row_ids = Vec::new();
        let overlay_rids = self.overlay_rid_set(snapshot);
        for rr in &self.run_refs {
            let mut reader = self.open_reader(rr.run_id)?;
            let matched = reader.range_row_ids_visible_i64(column_id, lo, hi, snapshot.epoch)?;
            for rid in matched {
                if !overlay_rids.contains(&rid) {
                    row_ids.push(rid);
                }
            }
        }
        let mut s = RowIdSet::from_unsorted(row_ids);
        self.range_scan_overlay_i64(&mut s, column_id, lo, hi, snapshot);
        Ok(s)
    }

    /// Float64 analogue of [`Self::range_scan_i64`] with per-bound inclusivity
    /// (Phase 13.2 / 16.3).
    fn range_scan_f64(
        &self,
        column_id: u16,
        lo: f64,
        lo_inclusive: bool,
        hi: f64,
        hi_inclusive: bool,
        snapshot: Snapshot,
    ) -> Result<RowIdSet> {
        let mut row_ids = Vec::new();
        let overlay_rids = self.overlay_rid_set(snapshot);
        for rr in &self.run_refs {
            let mut reader = self.open_reader(rr.run_id)?;
            let matched = reader.range_row_ids_visible_f64(
                column_id,
                lo,
                lo_inclusive,
                hi,
                hi_inclusive,
                snapshot.epoch,
            )?;
            for rid in matched {
                if !overlay_rids.contains(&rid) {
                    row_ids.push(rid);
                }
            }
        }
        let mut s = RowIdSet::from_unsorted(row_ids);
        self.range_scan_overlay_f64(
            &mut s,
            column_id,
            lo,
            lo_inclusive,
            hi,
            hi_inclusive,
            snapshot,
        );
        Ok(s)
    }

    /// Collect the set of row-ids visible in the memtable / mutable-run overlay.
    fn overlay_rid_set(&self, snapshot: Snapshot) -> HashSet<u64> {
        let mut s = HashSet::new();
        for row in self.memtable.visible_versions(snapshot.epoch) {
            s.insert(row.row_id.0);
        }
        for row in self.mutable_run.visible_versions(snapshot.epoch) {
            s.insert(row.row_id.0);
        }
        s
    }

    fn range_scan_overlay_i64(
        &self,
        s: &mut RowIdSet,
        column_id: u16,
        lo: i64,
        hi: i64,
        snapshot: Snapshot,
    ) {
        // Collapse both overlay tiers to the newest visible version per row id
        // (the memtable supersedes the mutable run) before range-checking, so a
        // stale in-range mutable-run version cannot shadow a newer out-of-range
        // memtable version of the same row.
        let mut newest: HashMap<u64, &Row> = HashMap::new();
        let mutable = self.mutable_run.visible_versions(snapshot.epoch);
        let memtable = self.memtable.visible_versions(snapshot.epoch);
        for r in &mutable {
            newest.entry(r.row_id.0).or_insert(r);
        }
        for r in &memtable {
            newest.insert(r.row_id.0, r);
        }
        for row in newest.values() {
            if !row.deleted {
                if let Some(Value::Int64(v)) = row.columns.get(&column_id) {
                    if *v >= lo && *v <= hi {
                        s.insert(row.row_id.0);
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn range_scan_overlay_f64(
        &self,
        s: &mut RowIdSet,
        column_id: u16,
        lo: f64,
        lo_inclusive: bool,
        hi: f64,
        hi_inclusive: bool,
        snapshot: Snapshot,
    ) {
        // See `range_scan_overlay_i64`: dedup to the newest version per row id
        // across the memtable + mutable run before range-checking.
        let mut newest: HashMap<u64, &Row> = HashMap::new();
        let mutable = self.mutable_run.visible_versions(snapshot.epoch);
        let memtable = self.memtable.visible_versions(snapshot.epoch);
        for r in &mutable {
            newest.entry(r.row_id.0).or_insert(r);
        }
        for r in &memtable {
            newest.insert(r.row_id.0, r);
        }
        for row in newest.values() {
            if !row.deleted {
                if let Some(Value::Float64(v)) = row.columns.get(&column_id) {
                    let ok_lo = if lo_inclusive { *v >= lo } else { *v > lo };
                    let ok_hi = if hi_inclusive { *v <= hi } else { *v < hi };
                    if ok_lo && ok_hi {
                        s.insert(row.row_id.0);
                    }
                }
            }
        }
    }

    /// Multi-run fallback for `IS NULL` / `IS NOT NULL`. Calls each run's
    /// MVCC-aware null scan and merges with the overlay.
    fn null_scan(&self, column_id: u16, want_nulls: bool, snapshot: Snapshot) -> Result<RowIdSet> {
        let mut row_ids = Vec::new();
        let overlay_rids = self.overlay_rid_set(snapshot);
        for rr in &self.run_refs {
            let mut reader = self.open_reader(rr.run_id)?;
            let matched = reader.null_row_ids_visible(column_id, want_nulls, snapshot.epoch)?;
            for rid in matched {
                if !overlay_rids.contains(&rid) {
                    row_ids.push(rid);
                }
            }
        }
        let mut s = RowIdSet::from_unsorted(row_ids);
        self.null_scan_overlay(&mut s, column_id, want_nulls, snapshot);
        Ok(s)
    }

    /// Merge overlay rows for `IS NULL` / `IS NOT NULL`. An overlay row
    /// supersedes its run version, so overlay rids are removed from the run
    /// set and re-evaluated from the overlay values directly.
    fn null_scan_overlay(
        &self,
        s: &mut RowIdSet,
        column_id: u16,
        want_nulls: bool,
        snapshot: Snapshot,
    ) {
        let mut newest: HashMap<u64, &Row> = HashMap::new();
        let mutable = self.mutable_run.visible_versions(snapshot.epoch);
        let memtable = self.memtable.visible_versions(snapshot.epoch);
        for r in &mutable {
            newest.entry(r.row_id.0).or_insert(r);
        }
        for r in &memtable {
            newest.insert(r.row_id.0, r);
        }
        for row in newest.values() {
            if row.deleted {
                continue;
            }
            let is_null = !row.columns.contains_key(&column_id)
                || matches!(row.columns.get(&column_id), Some(Value::Null) | None);
            if is_null == want_nulls {
                s.insert(row.row_id.0);
            }
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot::at(self.epoch.visible())
    }

    /// Pin the current epoch as a read snapshot; compaction will preserve the
    /// versions it needs until [`Table::unpin_snapshot`] is called.
    pub fn pin_snapshot(&mut self) -> Snapshot {
        let e = self.epoch.visible();
        *self.pinned.entry(e).or_insert(0) += 1;
        Snapshot::at(e)
    }

    /// Release a pinned snapshot.
    pub fn unpin_snapshot(&mut self, snap: Snapshot) {
        if let Some(count) = self.pinned.get_mut(&snap.epoch) {
            *count -= 1;
            if *count == 0 {
                self.pinned.remove(&snap.epoch);
            }
        }
    }

    /// Oldest pinned snapshot epoch, or `None` if no snapshot is active.
    /// Lowest snapshot epoch that compaction must preserve a version for, or
    /// `None` when no reader is pinned anywhere. Considers BOTH the single-table
    /// local pin set (`self.pinned`, used by the standalone `pin_snapshot` API)
    /// AND the shared `Database` snapshot registry (`db.snapshot()` readers) —
    /// otherwise a multi-table reader's version could be dropped by a compaction
    /// triggered on its table (the registry-gated reaper would then keep the
    /// old run *files*, but readers only scan the merged run, so the version
    /// would still be lost).
    pub(crate) fn min_active_snapshot(&self) -> Option<Epoch> {
        let local = self.pinned.keys().next().copied();
        let global = self.snapshots.min_pinned();
        match (local, global) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, b) => b,
        }
    }

    pub fn current_epoch(&self) -> Epoch {
        self.epoch.visible()
    }

    pub fn memtable_len(&self) -> usize {
        self.memtable.len()
    }

    /// Live (non-deleted) row count, O(1) from a manifest-maintained counter —
    /// the metadata `COUNT(*)` fast path (no scan).
    pub fn count(&self) -> u64 {
        self.live_count
    }

    /// Count rows matching an index-backed conjunctive predicate without
    /// materializing projected columns. Returns `None` when a condition cannot
    /// be served by the native predicate resolver.
    pub fn count_conditions(
        &mut self,
        conditions: &[crate::query::Condition],
        snapshot: Snapshot,
    ) -> Result<Option<u64>> {
        use crate::query::Condition;
        if conditions.is_empty() {
            return Ok(Some(self.live_count));
        }
        let served = |c: &Condition| {
            matches!(
                c,
                Condition::Pk(_)
                    | Condition::BitmapEq { .. }
                    | Condition::BitmapIn { .. }
                    | Condition::FmContains { .. }
                    | Condition::FmContainsAll { .. }
                    | Condition::Ann { .. }
                    | Condition::Range { .. }
                    | Condition::RangeF64 { .. }
                    | Condition::SparseMatch { .. }
                    | Condition::IsNull { .. }
                    | Condition::IsNotNull { .. }
            )
        };
        if !conditions.iter().all(served) {
            return Ok(None);
        }
        self.ensure_indexes_complete()?;
        let mut sets = Vec::with_capacity(conditions.len());
        for condition in conditions {
            sets.push(self.resolve_condition(condition, snapshot)?);
        }
        let count = RowIdSet::intersect_many(sets).len() as u64;
        crate::trace::QueryTrace::record(|t| {
            t.scan_mode = crate::trace::ScanMode::CountSurvivors;
            t.survivor_count = Some(count as usize);
            t.conditions_pushed = conditions.len();
        });
        Ok(Some(count))
    }

    /// Bulk-load typed columns straight to a new run — the fast ingest path.
    /// Bypasses the WAL, the memtable, and the `Value` enum entirely; writes one
    /// compressed run (delta for sorted Int64, dictionary for low-card Bytes)
    /// with **LZ4** (Phase 15.3 — fast decode for scan-heavy analytical runs),
    /// rotates the WAL, and persists the manifest in a single fsync group.
    /// Indexes are bulk-built from the typed columns (Phase 14.2).
    pub fn bulk_load_columns(
        &mut self,
        user_columns: Vec<(u16, columnar::NativeColumn)>,
    ) -> Result<Epoch> {
        self.bulk_load_columns_with(user_columns, 3, false, true)
    }

    /// Maximal-throughput bulk ingest (Phase 14.4): skip zstd entirely and write
    /// raw `ALGO_PLAIN` pages. ~3–4× the encode throughput of
    /// [`Self::bulk_load_columns`] at ~3–4× the on-disk size — the right choice
    /// when ingest latency dominates and a background compaction will re-compress
    /// later. Indexing, WAL rotation, and the manifest are identical to
    /// [`Self::bulk_load_columns`].
    pub fn bulk_load_fast(
        &mut self,
        user_columns: Vec<(u16, columnar::NativeColumn)>,
    ) -> Result<Epoch> {
        self.bulk_load_columns_with(user_columns, -1, true, false)
    }

    fn bulk_load_columns_with(
        &mut self,
        mut user_columns: Vec<(u16, columnar::NativeColumn)>,
        zstd_level: i32,
        force_plain: bool,
        lz4: bool,
    ) -> Result<Epoch> {
        let epoch = self.commit()?;
        let n = user_columns.first().map(|(_, c)| c.len()).unwrap_or(0);
        if n == 0 {
            return Ok(epoch);
        }
        let live_before = self.live_count;
        // Spill pending mutable-run data before the Flush marker + WAL rotation.
        self.spill_mutable_run(epoch)?;
        let eager_index_build = self.indexes_complete
            && self.run_refs.is_empty()
            && self.memtable.is_empty()
            && self.mutable_run.is_empty();
        // Enforce NOT NULL constraints and primary-key upsert semantics before
        // any row id is allocated or bytes hit the run file.
        self.fill_auto_inc_native_columns(&mut user_columns, n)?;
        self.validate_columns_not_null(&user_columns, n)?;
        let winner_idx = self
            .bulk_pk_winner_indices(&user_columns, n)
            .and_then(|idx| if idx.len() == n { None } else { Some(idx) });
        let (write_columns, write_n): (Vec<(u16, columnar::NativeColumn)>, usize) =
            match winner_idx.as_deref() {
                Some(idx) => {
                    let compacted = user_columns
                        .iter()
                        .map(|(id, c)| (*id, c.gather(idx)))
                        .collect();
                    (compacted, idx.len())
                }
                None => (user_columns, n),
            };
        self.advance_auto_inc_from_native_columns(&write_columns, write_n, live_before)?;
        let first = self.allocator.alloc_range(write_n as u64).0;
        for rid in first..first + write_n as u64 {
            self.reservoir.offer(rid);
        }
        let run_id = self.next_run_id;
        self.next_run_id += 1;
        let path = self.run_path(run_id);
        let mut writer =
            RunWriter::new(&self.schema, run_id as u128, epoch, 0).with_native_endian();
        if force_plain {
            writer = writer.with_plain();
        } else if lz4 {
            // Phase 15.3: bulk-loaded analytical runs are scan-heavy, so encode
            // them with LZ4 (3–5× faster decode, ~10% worse ratio than zstd).
            writer = writer.with_lz4();
        } else {
            writer = writer.with_zstd_level(zstd_level);
        }
        if let Some(kek) = &self.kek {
            writer = writer.with_encryption(kek.as_ref(), self.indexable_column_specs());
        }
        let header = writer.write_native(&path, &write_columns, write_n, first)?;
        self.run_refs.push(RunRef {
            run_id: run_id as u128,
            level: 0,
            epoch_created: epoch.0,
            row_count: header.row_count,
        });
        self.live_count = self.live_count.saturating_add(write_n as u64);
        if eager_index_build {
            let row_ids: Vec<u64> = (first..first + write_n as u64).collect();
            self.index_columns_bulk(&write_columns, &row_ids);
            self.indexes_complete = true;
            self.build_learned_ranges()?;
        } else {
            // Phase 14.7: defer index building off the ingest critical path for
            // non-empty tables where cross-run PK/update semantics must be
            // reconstructed from durable state.
            self.indexes_complete = false;
        }
        self.mark_flushed(epoch)?;
        self.persist_manifest(epoch)?;
        if eager_index_build {
            self.checkpoint_indexes(epoch);
        }
        self.clear_result_cache();
        Ok(epoch)
    }

    /// Bulk-build the live in-memory indexes (HOT/bitmap/FM/sparse) straight
    /// from typed columns — the deferred batch-indexing path (Phase 14.2).
    ///
    /// Replaces the per-row `index_into` loop: no `Row`, no per-row
    /// `HashMap<u16, Value>`, no `Value` enum. Index keys are computed directly
    /// from the typed buffers via [`columnar::encode_key_native`], tokenized for
    /// `ENCRYPTED_INDEXABLE` columns the same way `index_into` on a tokenized
    /// row would. FM is appended dirty and rebuilt once on the next query; the
    /// others are populated in a single typed pass. Entries are merged into the
    /// existing indexes so this is correct under multi-run loads and partial
    /// reindexes.
    ///
    /// `row_ids[i]` is the `RowId` of element `i` of every column. ANN
    /// (`IndexKind::Ann`) is intentionally skipped: the native codec carries no
    /// embeddings, so an `Embedding` column can never reach this path (a native
    /// bulk load of an embedding schema fails at encode). LearnedRange is built
    /// separately from the runs by [`Self::build_learned_ranges`].
    fn index_columns_bulk(&mut self, columns: &[(u16, columnar::NativeColumn)], row_ids: &[u64]) {
        let n = row_ids.len();
        if n == 0 {
            return;
        }
        let by_id: std::collections::HashMap<u16, &columnar::NativeColumn> =
            columns.iter().map(|(id, c)| (*id, c)).collect();
        let ty_of: std::collections::HashMap<u16, TypeId> =
            self.schema.columns.iter().map(|c| (c.id, c.ty)).collect();
        let pk_id = self.schema.primary_key().map(|c| c.id);

        for (i, &rid) in row_ids.iter().enumerate() {
            let row_id = RowId(rid);
            if let Some(pid) = pk_id {
                if let Some(col) = by_id.get(&pid) {
                    let ty = ty_of.get(&pid).copied().unwrap_or(TypeId::Int64);
                    if let Some(key) = bulk_index_key(&self.column_keys, pid, ty, col, i) {
                        self.insert_hot_pk(key, row_id);
                    }
                }
            }
            for idef in &self.schema.indexes {
                let Some(col) = by_id.get(&idef.column_id) else {
                    continue;
                };
                let ty = ty_of.get(&idef.column_id).copied().unwrap_or(TypeId::Int64);
                match idef.kind {
                    IndexKind::Bitmap => {
                        if let Some(b) = self.bitmap.get_mut(&idef.column_id) {
                            if let Some(key) =
                                bulk_index_key(&self.column_keys, idef.column_id, ty, col, i)
                            {
                                b.insert(key, row_id);
                            }
                        }
                    }
                    IndexKind::FmIndex => {
                        if let Some(f) = self.fm.get_mut(&idef.column_id) {
                            if let Some(bytes) = columnar::native_bytes_at(col, i) {
                                f.insert(bytes.to_vec(), row_id);
                            }
                        }
                    }
                    IndexKind::Sparse => {
                        if let Some(s) = self.sparse.get_mut(&idef.column_id) {
                            if let Some(bytes) = columnar::native_bytes_at(col, i) {
                                if let Ok(terms) = bincode::deserialize::<Vec<(u32, f32)>>(bytes) {
                                    s.insert(&terms, row_id);
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    /// no `Value`). Fast path: empty memtable + single run decodes columns
    /// directly and gathers visible indices; falls back to the `Value` path
    /// pivoted to native columns otherwise. `projection` (a set of column ids)
    /// limits decoding to the requested columns — `None` ⇒ all user columns.
    pub fn visible_columns_native(
        &self,
        snapshot: Snapshot,
        projection: Option<&[u16]>,
    ) -> Result<Vec<(u16, columnar::NativeColumn)>> {
        let wanted: Vec<u16> = match projection {
            Some(p) => p.to_vec(),
            None => self.schema.columns.iter().map(|c| c.id).collect(),
        };
        if self.memtable.is_empty() && self.mutable_run.is_empty() && self.run_refs.len() == 1 {
            let rr = self.run_refs[0].clone();
            let mut reader = self.open_reader(rr.run_id)?;
            let idxs = reader.visible_indices_native(snapshot.epoch)?;
            let all_visible = idxs.len() == reader.row_count();
            // Phase 15.1: decode every requested column in parallel when the
            // reader is mmap-backed. Each column already parallel-decodes its
            // own pages, so a wide table saturates the pool via nested rayon
            // without oversubscribing (work-stealing handles it). Falls back to
            // the sequential `&mut` path when mmap is unavailable.
            if reader.has_mmap() {
                use rayon::prelude::*;
                // Pre-resolve the requested ids that exist in the schema (don't
                // capture `self` inside the rayon closure).
                let valid: Vec<u16> = wanted
                    .iter()
                    .filter(|cid| self.schema.columns.iter().any(|c| c.id == **cid))
                    .copied()
                    .collect();
                // Decode concurrently; `collect` preserves `valid` order.
                let decoded: Vec<(u16, columnar::NativeColumn)> = valid
                    .par_iter()
                    .filter_map(|cid| {
                        reader
                            .column_native_shared(*cid)
                            .ok()
                            .map(|col| (*cid, col))
                    })
                    .collect();
                let cols = decoded
                    .into_iter()
                    .map(|(id, col)| (id, if all_visible { col } else { col.gather(&idxs) }))
                    .collect();
                return Ok(cols);
            }
            let mut cols = Vec::with_capacity(wanted.len());
            for cid in &wanted {
                let cdef = match self.schema.columns.iter().find(|c| c.id == *cid) {
                    Some(c) => c,
                    None => continue,
                };
                let col = reader.column_native(cdef.id)?;
                cols.push((cdef.id, if all_visible { col } else { col.gather(&idxs) }));
            }
            return Ok(cols);
        }
        let vcols = self.visible_columns(snapshot)?;
        let want_set: std::collections::HashSet<u16> = wanted.iter().copied().collect();
        let out: Vec<(u16, columnar::NativeColumn)> = vcols
            .into_iter()
            .filter(|(id, _)| want_set.contains(id))
            .map(|(id, vals)| {
                let ty = self
                    .schema
                    .columns
                    .iter()
                    .find(|c| c.id == id)
                    .map(|c| c.ty)
                    .unwrap_or(TypeId::Bytes);
                (id, columnar::values_to_native(ty, &vals))
            })
            .collect();
        Ok(out)
    }

    pub fn run_count(&self) -> usize {
        self.run_refs.len()
    }

    /// Whether the memtable is empty (no unflushed puts).
    pub fn memtable_is_empty(&self) -> bool {
        self.memtable.is_empty()
    }

    /// Cumulative raw-page-cache hit/miss counts (Priority 14: hit visibility).
    /// Useful for confirming a repeat scan is served from cache or measuring a
    /// query's locality after [`reset_page_cache_stats`](Self::reset_page_cache_stats).
    pub fn page_cache_stats(&self) -> crate::cache::CacheStats {
        self.page_cache.lock().stats()
    }

    /// Zero the raw-page-cache hit/miss counters.
    pub fn reset_page_cache_stats(&self) {
        self.page_cache.lock().reset_stats();
    }

    /// The run IDs in level order (Phase 15.5: used by the Arrow IPC shadow to
    /// key shadow files and detect stale shadows).
    pub fn run_ids(&self) -> Vec<u128> {
        self.run_refs.iter().map(|r| r.run_id).collect()
    }

    /// Whether the single run (if exactly one) is clean — i.e. has
    /// `RUN_FLAG_CLEAN` set (Phase 15.5: the shadow is zero-copy only for clean
    /// runs).
    pub fn single_run_is_clean(&self) -> bool {
        if self.run_refs.len() != 1 {
            return false;
        }
        self.open_reader(self.run_refs[0].run_id)
            .map(|r| r.is_clean())
            .unwrap_or(false)
    }

    /// Best-effort resolve of the survivor RowId set for fine-grained cache
    /// invalidation (hardening (c)). On the single-run fast path, opens a reader
    /// and calls `resolve_survivor_rids`. On the multi-run/memtable path,
    /// returns an empty bitmap — conservative (condition_cols still catches
    /// column mutations, and deletes are caught by the epoch-free design falling
    /// through to the multi-run path which re-resolves).
    fn resolve_footprint(
        &self,
        conditions: &[crate::query::Condition],
        _snapshot: Snapshot,
    ) -> roaring::RoaringBitmap {
        if !self.memtable.is_empty() || !self.mutable_run.is_empty() {
            return roaring::RoaringBitmap::new();
        }
        if self.run_refs.is_empty() {
            return roaring::RoaringBitmap::new();
        }
        // Try the single-run fast path.
        if self.run_refs.len() == 1 {
            if let Ok(mut reader) = self.open_reader(self.run_refs[0].run_id) {
                if let Ok(rids) = self.resolve_survivor_rids(conditions, &mut reader) {
                    return rids.to_roaring_lossy();
                }
            }
        }
        roaring::RoaringBitmap::new()
    }

    /// Phase 19.1 + hardening (c): a cached form of
    /// [`Table::query_columns_native`]. The cache key embeds the snapshot epoch
    /// so two queries at different pinned snapshots never share an entry;
    /// invalidation is fine-grained — a `commit()` drops only entries whose
    /// footprint intersects a deleted RowId or whose condition-columns intersect
    /// a mutated column. On a miss the underlying `query_columns_native` runs and
    /// the result is cached as typed `NativeColumn`s. Returns `None` exactly when
    /// the non-cached path would (conditions not pushdown-served). Strictly
    /// additive — callers wanting fresh results keep using
    /// `query_columns_native`.
    pub fn query_columns_native_cached(
        &mut self,
        conditions: &[crate::query::Condition],
        projection: Option<&[u16]>,
        snapshot: Snapshot,
    ) -> Result<Option<Vec<(u16, columnar::NativeColumn)>>> {
        if conditions.is_empty() {
            return self.query_columns_native(conditions, projection, snapshot);
        }
        // The snapshot epoch is part of the key so two queries with identical
        // conditions/projection but pinned at different snapshots never share a
        // cached result (MVCC isolation for the explicit-snapshot API).
        let key = crate::query::canonical_query_key(conditions, projection, snapshot.epoch.0);
        if let Some(hit) = self.result_cache.lock().get_columns(key) {
            crate::trace::QueryTrace::record(|t| {
                t.result_cache_hit = true;
                t.scan_mode = crate::trace::ScanMode::NativePushdown;
            });
            return Ok(Some((*hit).clone()));
        }
        let res = self.query_columns_native(conditions, projection, snapshot)?;
        if let Some(cols) = &res {
            let footprint = self.resolve_footprint(conditions, snapshot);
            let condition_cols = crate::query::condition_columns(conditions);
            self.result_cache.lock().insert(
                key,
                CachedEntry {
                    data: CachedData::Columns(Arc::new(cols.clone())),
                    footprint,
                    condition_cols,
                },
            );
        }
        Ok(res)
    }

    /// Phase 19.1 + hardening (c): a cached form of [`Table::query`]. The cache key
    /// is epoch-independent; invalidation is fine-grained (see
    /// [`Table::query_columns_native_cached`]). On a hit returns the cached rows (no
    /// re-resolve, no re-decode).
    pub fn query_cached(&mut self, q: &crate::query::Query) -> Result<Vec<Row>> {
        if q.conditions.is_empty() {
            return self.query(q);
        }
        let key = crate::query::canonical_query_key(&q.conditions, None, 0);
        if let Some(hit) = self.result_cache.lock().get_rows(key) {
            crate::trace::QueryTrace::record(|t| {
                t.result_cache_hit = true;
                t.scan_mode = crate::trace::ScanMode::Materialized;
            });
            return Ok((*hit).clone());
        }
        let rows = self.query(q)?;
        let footprint = rows.iter().map(|r| r.row_id.0 as u32).collect();
        let condition_cols = crate::query::condition_columns(&q.conditions);
        self.result_cache.lock().insert(
            key,
            CachedEntry {
                data: CachedData::Rows(Arc::new(rows.clone())),
                footprint,
                condition_cols,
            },
        );
        Ok(rows)
    }

    // -----------------------------------------------------------------------
    // Traced query wrappers (OPTIMIZATIONS.md Priority 0 / 16).
    //
    // Each `_traced` method runs its underlying query inside a
    // [`crate::trace::QueryTrace::capture`] scope and returns the result
    // alongside the captured path trace. The trace records which physical path
    // served the query (cursor / pushdown / materialized / count-shortcut),
    // whether indexes were rebuilt, whether the result cache hit, overlay size,
    // survivor count, and the fast row-id map usage. Recording is zero-cost
    // when no `_traced` method is on the call stack (the plain methods are
    // unchanged).
    // -----------------------------------------------------------------------

    /// [`Self::query_columns_native`] with a captured [`crate::trace::QueryTrace`].
    #[allow(clippy::type_complexity)]
    pub fn query_columns_native_traced(
        &mut self,
        conditions: &[crate::query::Condition],
        projection: Option<&[u16]>,
        snapshot: Snapshot,
    ) -> Result<(
        Option<Vec<(u16, columnar::NativeColumn)>>,
        crate::trace::QueryTrace,
    )> {
        let (result, trace) = crate::trace::QueryTrace::capture(|| {
            self.query_columns_native(conditions, projection, snapshot)
        });
        Ok((result?, trace))
    }

    /// [`Self::query_columns_native_cached`] with a captured
    /// [`crate::trace::QueryTrace`] (records result-cache hits too).
    #[allow(clippy::type_complexity)]
    pub fn query_columns_native_cached_traced(
        &mut self,
        conditions: &[crate::query::Condition],
        projection: Option<&[u16]>,
        snapshot: Snapshot,
    ) -> Result<(
        Option<Vec<(u16, columnar::NativeColumn)>>,
        crate::trace::QueryTrace,
    )> {
        let (result, trace) = crate::trace::QueryTrace::capture(|| {
            self.query_columns_native_cached(conditions, projection, snapshot)
        });
        Ok((result?, trace))
    }

    /// [`Self::native_page_cursor`] with a captured [`crate::trace::QueryTrace`].
    pub fn native_page_cursor_traced(
        &self,
        snapshot: Snapshot,
        projection: Vec<(u16, TypeId)>,
        conditions: &[crate::query::Condition],
    ) -> Result<(Option<NativePageCursor>, crate::trace::QueryTrace)> {
        let (result, trace) = crate::trace::QueryTrace::capture(|| {
            self.native_page_cursor(snapshot, projection, conditions)
        });
        Ok((result?, trace))
    }

    /// [`Self::native_multi_run_cursor`] with a captured [`crate::trace::QueryTrace`].
    pub fn native_multi_run_cursor_traced(
        &self,
        snapshot: Snapshot,
        projection: Vec<(u16, TypeId)>,
        conditions: &[crate::query::Condition],
    ) -> Result<(
        Option<crate::cursor::MultiRunCursor>,
        crate::trace::QueryTrace,
    )> {
        let (result, trace) = crate::trace::QueryTrace::capture(|| {
            self.native_multi_run_cursor(snapshot, projection, conditions)
        });
        Ok((result?, trace))
    }

    /// [`Self::count_conditions`] with a captured [`crate::trace::QueryTrace`].
    pub fn count_conditions_traced(
        &mut self,
        conditions: &[crate::query::Condition],
        snapshot: Snapshot,
    ) -> Result<(Option<u64>, crate::trace::QueryTrace)> {
        let (result, trace) =
            crate::trace::QueryTrace::capture(|| self.count_conditions(conditions, snapshot));
        Ok((result?, trace))
    }

    /// [`Self::query`] with a captured [`crate::trace::QueryTrace`].
    pub fn query_traced(
        &mut self,
        q: &crate::query::Query,
    ) -> Result<(Vec<Row>, crate::trace::QueryTrace)> {
        let (result, trace) = crate::trace::QueryTrace::capture(|| self.query(q));
        Ok((result?, trace))
    }

    /// Predicate pushdown: resolve `conditions` via indexes to find the matching
    /// row-id set, then decode only those rows' columns — not the whole table.
    /// Returns `None` if the conditions can't be served by indexes (caller falls
    /// back to a full scan). This is the fast path for `WHERE col = 'value'`.
    pub fn query_columns_native(
        &mut self,
        conditions: &[crate::query::Condition],
        projection: Option<&[u16]>,
        snapshot: Snapshot,
    ) -> Result<Option<Vec<(u16, columnar::NativeColumn)>>> {
        use crate::query::Condition;
        if conditions.is_empty() {
            return Ok(None);
        }
        self.ensure_indexes_complete()?;

        // Only these conditions are pushdown-served. Range/RangeF64 need a
        // column read on the single-run fast path; off it they fall back to a
        // visible-rows scan via `resolve_condition` (still correct for any
        // layout, just not page-pruned).
        let served = |c: &Condition| {
            matches!(
                c,
                Condition::Pk(_)
                    | Condition::BitmapEq { .. }
                    | Condition::BitmapIn { .. }
                    | Condition::FmContains { .. }
                    | Condition::FmContainsAll { .. }
                    | Condition::Ann { .. }
                    | Condition::Range { .. }
                    | Condition::RangeF64 { .. }
                    | Condition::SparseMatch { .. }
                    | Condition::IsNull { .. }
                    | Condition::IsNotNull { .. }
            )
        };
        if !conditions.iter().all(served) {
            return Ok(None);
        }
        let fast_path =
            self.memtable.is_empty() && self.mutable_run.is_empty() && self.run_refs.len() == 1;
        crate::trace::QueryTrace::record(|t| {
            t.run_count = self.run_refs.len();
            t.memtable_rows = self.memtable.len();
            t.mutable_run_rows = self.mutable_run.len();
            t.conditions_pushed = conditions.len();
            t.learned_range_used = conditions.iter().any(|c| match c {
                Condition::Range { column_id, .. } | Condition::RangeF64 { column_id, .. } => {
                    self.learned_range.contains_key(column_id)
                }
                _ => false,
            });
        });
        // Build column list (projected or all user columns) + projection pairs.
        let col_ids: Vec<u16> = projection
            .map(|p| p.to_vec())
            .unwrap_or_else(|| self.schema.columns.iter().map(|c| c.id).collect());
        let proj_pairs: Vec<(u16, TypeId)> = col_ids
            .iter()
            .map(|&cid| {
                let ty = self
                    .schema
                    .columns
                    .iter()
                    .find(|c| c.id == cid)
                    .map(|c| c.ty)
                    .unwrap_or(TypeId::Bytes);
                (cid, ty)
            })
            .collect();

        // -----------------------------------------------------------------------
        // Fast path: single run, empty memtable/mutable-run → resolve survivors,
        // binary-search positions, gather only the projected columns from one
        // reader. This is the fastest pushdown path (no cursor overhead).
        // -----------------------------------------------------------------------
        if fast_path {
            // A Range/RangeF64 needs a column read *unless* its column has a
            // learned (PGM) range index, in which case it's served in-memory.
            let needs_column = conditions.iter().any(|c| match c {
                Condition::Range { column_id, .. } => !self.learned_range.contains_key(column_id),
                Condition::RangeF64 { column_id, .. } => {
                    !self.learned_range.contains_key(column_id)
                }
                _ => false,
            });
            let mut reader_opt: Option<RunReader> = if needs_column {
                Some(self.open_reader(self.run_refs[0].run_id)?)
            } else {
                None
            };
            let mut sets: Vec<RowIdSet> = Vec::new();
            for c in conditions {
                let s = match c {
                    Condition::Range { column_id, lo, hi }
                        if !self.learned_range.contains_key(column_id) =>
                    {
                        if reader_opt.is_none() {
                            reader_opt = Some(self.open_reader(self.run_refs[0].run_id)?);
                        }
                        reader_opt
                            .as_mut()
                            .expect("reader opened for range")
                            .range_row_id_set_i64(*column_id, *lo, *hi)?
                    }
                    Condition::RangeF64 {
                        column_id,
                        lo,
                        lo_inclusive,
                        hi,
                        hi_inclusive,
                    } if !self.learned_range.contains_key(column_id) => {
                        if reader_opt.is_none() {
                            reader_opt = Some(self.open_reader(self.run_refs[0].run_id)?);
                        }
                        reader_opt
                            .as_mut()
                            .expect("reader opened for range")
                            .range_row_id_set_f64(
                                *column_id,
                                *lo,
                                *lo_inclusive,
                                *hi,
                                *hi_inclusive,
                            )?
                    }
                    _ => self.resolve_condition(c, snapshot)?,
                };
                sets.push(s);
            }
            let candidates = RowIdSet::intersect_many(sets);
            crate::trace::QueryTrace::record(|t| {
                t.survivor_count = Some(candidates.len());
            });
            if candidates.is_empty() {
                let cols: Vec<(u16, columnar::NativeColumn)> = col_ids
                    .iter()
                    .map(|&id| {
                        (
                            id,
                            columnar::null_native(
                                proj_pairs
                                    .iter()
                                    .find(|(c, _)| c == &id)
                                    .map(|(_, t)| *t)
                                    .unwrap_or(TypeId::Bytes),
                                0,
                            ),
                        )
                    })
                    .collect();
                return Ok(Some(cols));
            }
            let mut reader = match reader_opt.take() {
                Some(r) => r,
                None => self.open_reader(self.run_refs[0].run_id)?,
            };
            let candidate_ids = candidates.into_sorted_vec();
            let (positions, fast_rid) = if let Some(positions) =
                reader.positions_for_row_ids_fast(&candidate_ids)
            {
                (positions, true)
            } else {
                let col = reader.column_native(crate::sorted_run::SYS_ROW_ID)?;
                match col {
                    columnar::NativeColumn::Int64 { data, .. } => {
                        let mut p: Vec<usize> = candidate_ids
                            .iter()
                            .filter_map(|rid| data.binary_search(&(*rid as i64)).ok())
                            .collect();
                        p.sort_unstable();
                        (p, false)
                    }
                    _ => return Err(MongrelError::InvalidArgument("sys row_id not int64".into())),
                }
            };
            crate::trace::QueryTrace::record(|t| {
                t.scan_mode = crate::trace::ScanMode::NativePushdown;
                t.fast_row_id_map = fast_rid;
            });
            let mut cols = Vec::with_capacity(col_ids.len());
            for cid in &col_ids {
                let col = reader.column_native(*cid)?;
                cols.push((*cid, col.gather(&positions)));
            }
            return Ok(Some(cols));
        }

        // -----------------------------------------------------------------------
        // Non-fast path (multi-run / non-empty overlay). Route through the
        // columnar cursor (OPTIMIZATIONS.md Priority 1 + 4): the cursor builder
        // resolves MVCC, predicates, and overlay internally in batch, then
        // streams projected columns page-by-page. This avoids the per-rid
        // `rows_for_rids` `get_version`-across-all-runs cost that made multi-run
        // pushdown ~1000× slower than the single-run fast path.
        //
        // The cursor handles both single-run-with-overlay (`native_page_cursor`)
        // and multi-run (`native_multi_run_cursor`) layouts. The empty-table
        // (no runs, memtable-only) edge case falls through to `rows_for_rids`.
        // -----------------------------------------------------------------------
        if !self.run_refs.is_empty() {
            use crate::cursor::{drain_cursor_to_columns, Cursor};
            let remaining: usize;
            let mut cursor: Box<dyn crate::cursor::Cursor> = if self.run_refs.len() == 1 {
                let c = self
                    .native_page_cursor(snapshot, proj_pairs.clone(), conditions)?
                    .expect("single-run cursor should build when run_refs.len() == 1");
                remaining = c.remaining_rows();
                Box::new(c)
            } else {
                let c = self
                    .native_multi_run_cursor(snapshot, proj_pairs.clone(), conditions)?
                    .expect("multi-run cursor should build when run_refs.len() >= 1");
                remaining = c.remaining_rows();
                Box::new(c)
            };
            crate::trace::QueryTrace::record(|t| {
                if t.survivor_count.is_none() {
                    t.survivor_count = Some(remaining);
                }
            });
            let cols = drain_cursor_to_columns(cursor.as_mut(), &proj_pairs)?;
            return Ok(Some(cols));
        }

        // Empty-table fallback (no sorted runs, memtable/mutable-run only): the
        // cursor builders return `None` for `run_refs.is_empty()`, so resolve
        // from overlay indexes and materialize via `rows_for_rids`. This is the
        // rare edge case (fresh table with only `put`s, no `flush`/`bulk_load`).
        crate::trace::QueryTrace::record(|t| {
            t.scan_mode = crate::trace::ScanMode::Materialized;
            t.row_materialized = true;
        });
        let mut sets: Vec<RowIdSet> = Vec::with_capacity(conditions.len());
        for c in conditions {
            sets.push(self.resolve_condition(c, snapshot)?);
        }
        let rids = RowIdSet::intersect_many(sets).into_sorted_vec();
        let rows = self.rows_for_rids(&rids, snapshot)?;
        let mut cols: Vec<(u16, columnar::NativeColumn)> = Vec::with_capacity(col_ids.len());
        for (cid, ty) in &proj_pairs {
            let vals: Vec<Value> = rows
                .iter()
                .map(|r| r.columns.get(cid).cloned().unwrap_or(Value::Null))
                .collect();
            cols.push((*cid, columnar::values_to_native(*ty, &vals)));
        }
        Ok(Some(cols))
    }

    /// Build a lazy, page-aware [`NativePageCursor`] for the single-run fast
    /// path. MVCC visibility and predicate survivor resolution are settled up
    /// front (so they see the live indexes under the DB lock); the cursor then
    /// owns the reader and decodes only the projected columns of pages that
    /// contain survivors, lazily. This is the fused-predicate + page-skip +
    /// late-materialization scan.
    ///
    /// Phase 13.1: the memtable / mutable-run overlay is now handled. Rows with
    /// a newer version in the overlay are excluded from the run's page plans
    /// (their run version is stale); the overlay rows are pre-materialized and
    /// appended as a final batch via [`NativePageCursor::new_with_overlay`].
    ///
    /// Returns `None` only for multiple sorted runs; the caller falls back to
    /// the materialize-then-stream scan for that layout.
    pub fn native_page_cursor(
        &self,
        snapshot: Snapshot,
        projection: Vec<(u16, TypeId)>,
        conditions: &[crate::query::Condition],
    ) -> Result<Option<NativePageCursor>> {
        use crate::cursor::build_page_plans;
        if self.run_refs.len() != 1 {
            return Ok(None);
        }
        let mut reader = self.open_reader(self.run_refs[0].run_id)?;
        let (positions, rids) = reader.visible_positions_with_rids(snapshot.epoch)?;

        // Collect overlay rows from memtable + mutable_run (visible, newest
        // version per row). These shadow any stale version in the run.
        let overlay_rids: HashSet<u64> = {
            let mut s = HashSet::new();
            for row in self.memtable.visible_versions(snapshot.epoch) {
                s.insert(row.row_id.0);
            }
            for row in self.mutable_run.visible_versions(snapshot.epoch) {
                s.insert(row.row_id.0);
            }
            s
        };

        // Resolve survivor rids via indexes (covers overlay rows for index-
        // served conditions: PK, bitmap, FM, ANN, sparse — all maintained on
        // every put).
        let survivors = if conditions.is_empty() {
            None
        } else {
            Some(self.resolve_survivor_rids(conditions, &mut reader)?)
        };

        // Exclude overlay rids from the run portion: their version in the run
        // is stale (updated/deleted in the overlay) or they don't exist in the
        // run (new inserts). When there are conditions, we remove overlay rids
        // from the survivor set. When there are no conditions, we synthesize a
        // survivor set = (all visible run rids) − (overlay rids) so the stale
        // run rows are pruned.
        let run_survivors: Option<RowIdSet> = if overlay_rids.is_empty() {
            survivors.clone()
        } else if let Some(s) = &survivors {
            let mut run_set = s.clone();
            run_set.remove_many(overlay_rids.iter().copied());
            Some(run_set)
        } else {
            Some(RowIdSet::from_unsorted(
                rids.iter()
                    .map(|&r| r as u64)
                    .filter(|r| !overlay_rids.contains(r))
                    .collect(),
            ))
        };

        let overlay_rows = if overlay_rids.is_empty() {
            Vec::new()
        } else {
            let bound = Self::overlay_materialization_bound(conditions, &survivors);
            self.overlay_visible_rows(snapshot, bound)
        };

        // Build page plans for the run portion.
        let plans = if positions.is_empty() {
            Vec::new()
        } else {
            let page_rows = reader.page_row_counts(crate::sorted_run::SYS_ROW_ID)?;
            build_page_plans(&positions, &rids, &page_rows, run_survivors.as_ref())
        };

        // Filter and materialize the overlay.
        let overlay = if overlay_rows.is_empty() {
            None
        } else {
            let filtered =
                self.filter_overlay_rows(overlay_rows, conditions, survivors.as_ref(), snapshot)?;
            if filtered.is_empty() {
                None
            } else {
                Some(self.materialize_overlay(&filtered, &projection))
            }
        };

        let overlay_row_count = overlay
            .as_ref()
            .map(|c| c.first().map(|c| c.len()).unwrap_or(0))
            .unwrap_or(0);
        crate::trace::QueryTrace::record(|t| {
            t.scan_mode = crate::trace::ScanMode::NativePageCursor;
            t.run_count = self.run_refs.len();
            t.memtable_rows = self.memtable.len();
            t.mutable_run_rows = self.mutable_run.len();
            t.overlay_rows = overlay_row_count;
            t.conditions_pushed = conditions.len();
            t.pages_decoded = plans
                .iter()
                .map(|p| p.positions.len())
                .sum::<usize>()
                .min(1);
        });

        Ok(Some(NativePageCursor::new_with_overlay(
            reader, projection, plans, overlay,
        )))
    }
    /// Generalizes [`Self::native_page_cursor`] (single-run) to arbitrary run
    /// counts via a k-way merge by `RowId`. Cross-run MVCC resolution (newest
    /// visible version per `RowId`) and predicate survivor resolution are settled
    /// up front from the cheap system columns + global indexes; the cursor then
    /// lazily decodes the projected data columns of just the pages that own
    /// survivors, each page at most once. The memtable / mutable-run overlay is
    /// materialized and yielded as a final batch (mirroring the single-run path).
    ///
    /// Returns `None` only when there are no runs at all (caller falls back).
    #[allow(clippy::type_complexity)]
    pub fn native_multi_run_cursor(
        &self,
        snapshot: Snapshot,
        projection: Vec<(u16, TypeId)>,
        conditions: &[crate::query::Condition],
    ) -> Result<Option<crate::cursor::MultiRunCursor>> {
        use crate::cursor::{MultiRunCursor, RunStream};
        use crate::sorted_run::SYS_ROW_ID;
        use std::collections::{BinaryHeap, HashMap, HashSet};
        if self.run_refs.is_empty() {
            return Ok(None);
        }

        // Open each run once; read its system columns + page layout.
        let mut run_meta: Vec<(RunReader, Vec<i64>, Vec<i64>, Vec<u8>, Vec<usize>)> =
            Vec::with_capacity(self.run_refs.len());
        for rr in &self.run_refs {
            let mut reader = self.open_reader(rr.run_id)?;
            let (rids, eps, del) = reader.system_columns_native()?;
            let page_rows = reader.page_row_counts(SYS_ROW_ID)?;
            run_meta.push((reader, rids, eps, del, page_rows));
        }

        // Global cross-run newest-version resolution: rid -> (epoch, run_idx,
        // position, deleted). Mirrors `visible_rows`, tracking which run owns
        // the newest MVCC-visible version.
        let mut best: HashMap<u64, (u64, usize, usize, bool)> = HashMap::new();
        for (run_idx, (_, rids, eps, del, _)) in run_meta.iter().enumerate() {
            for i in 0..rids.len() {
                let rid = rids[i] as u64;
                let e = eps[i] as u64;
                if e > snapshot.epoch.0 {
                    continue;
                }
                let is_del = del[i] != 0;
                best.entry(rid)
                    .and_modify(|cur| {
                        if e > cur.0 {
                            *cur = (e, run_idx, i, is_del);
                        }
                    })
                    .or_insert((e, run_idx, i, is_del));
            }
        }

        // Overlay rids (memtable + mutable-run) shadow every run version.
        let overlay_rids: HashSet<u64> = {
            let mut s = HashSet::new();
            for row in self.memtable.visible_versions(snapshot.epoch) {
                s.insert(row.row_id.0);
            }
            for row in self.mutable_run.visible_versions(snapshot.epoch) {
                s.insert(row.row_id.0);
            }
            s
        };

        // Predicate survivors (global, layout-independent).
        let survivors: Option<RowIdSet> = if conditions.is_empty() {
            None
        } else {
            let mut sets: Vec<RowIdSet> = Vec::with_capacity(conditions.len());
            for c in conditions {
                sets.push(self.resolve_condition(c, snapshot)?);
            }
            Some(RowIdSet::intersect_many(sets))
        };

        // Per-run owned survivors: (rid, position), ascending by rid. A row is
        // owned by the run holding its newest visible version, is not deleted,
        // is not shadowed by the overlay, and satisfies the predicate.
        let mut per_run: Vec<Vec<(u64, usize)>> = vec![Vec::new(); run_meta.len()];
        for (rid, (_, run_idx, pos, deleted)) in &best {
            if *deleted {
                continue;
            }
            if overlay_rids.contains(rid) {
                continue;
            }
            if let Some(s) = &survivors {
                if !s.contains(*rid) {
                    continue;
                }
            }
            per_run[*run_idx].push((*rid, *pos));
        }
        for v in per_run.iter_mut() {
            v.sort_unstable_by_key(|&(rid, _)| rid);
        }

        // Build the merge streams: map each owned position to (page_seq, within).
        let mut streams = Vec::with_capacity(run_meta.len());
        let mut heap: BinaryHeap<std::cmp::Reverse<(u64, usize)>> = BinaryHeap::new();
        let mut total = 0usize;
        for (run_idx, (reader, _, _, _, page_rows)) in run_meta.into_iter().enumerate() {
            let mut starts = Vec::with_capacity(page_rows.len());
            let mut acc = 0usize;
            for &r in &page_rows {
                starts.push(acc);
                acc += r;
            }
            let mut survivors_vec: Vec<(u64, usize, usize)> =
                Vec::with_capacity(per_run[run_idx].len());
            for &(rid, pos) in &per_run[run_idx] {
                let page_seq = match starts.partition_point(|&s| s <= pos) {
                    0 => continue,
                    p => p - 1,
                };
                let within = pos - starts[page_seq];
                survivors_vec.push((rid, page_seq, within));
            }
            total += survivors_vec.len();
            if let Some(&(rid, _, _)) = survivors_vec.first() {
                heap.push(std::cmp::Reverse((rid, run_idx)));
            }
            streams.push(RunStream::new(reader, survivors_vec, page_rows));
        }

        // Materialize the overlay (filtered + projected), yielded as the final batch.
        let overlay_rows = if overlay_rids.is_empty() {
            Vec::new()
        } else {
            let bound = Self::overlay_materialization_bound(conditions, &survivors);
            self.overlay_visible_rows(snapshot, bound)
        };
        let overlay = if overlay_rows.is_empty() {
            None
        } else {
            let filtered =
                self.filter_overlay_rows(overlay_rows, conditions, survivors.as_ref(), snapshot)?;
            if filtered.is_empty() {
                None
            } else {
                Some(self.materialize_overlay(&filtered, &projection))
            }
        };

        let overlay_row_count = overlay
            .as_ref()
            .map(|c| c.first().map(|c| c.len()).unwrap_or(0))
            .unwrap_or(0);
        crate::trace::QueryTrace::record(|t| {
            t.scan_mode = crate::trace::ScanMode::MultiRunCursor;
            t.run_count = self.run_refs.len();
            t.memtable_rows = self.memtable.len();
            t.mutable_run_rows = self.mutable_run.len();
            t.overlay_rows = overlay_row_count;
            t.conditions_pushed = conditions.len();
            t.survivor_count = Some(total);
        });

        Ok(Some(MultiRunCursor::new(
            streams, projection, heap, total, overlay,
        )))
    }

    /// Collect visible, non-deleted overlay rows from the memtable and mutable-
    /// run tier at `snapshot`. These are the rows whose data lives only in the
    /// in-memory buffers (not yet in a sorted run), or that shadow a stale
    /// version in the run.
    /// The survivor set that bounds overlay materialization (Priority 2), or
    /// `None` when overlay rows must be fully materialized — i.e. there is a
    /// `Range`/`RangeF64` residual, for which the index-served survivor set does
    /// not cover matching overlay rows (those are evaluated downstream). This
    /// mirrors the `all_index_served` branch of
    /// [`filter_overlay_rows`](Self::filter_overlay_rows), so bounding here is
    /// result-preserving.
    fn overlay_materialization_bound<'a>(
        conditions: &[crate::query::Condition],
        survivors: &'a Option<RowIdSet>,
    ) -> Option<&'a RowIdSet> {
        use crate::query::Condition;
        let has_range = conditions
            .iter()
            .any(|c| matches!(c, Condition::Range { .. } | Condition::RangeF64 { .. }));
        if has_range {
            None
        } else {
            survivors.as_ref()
        }
    }

    /// Materialize the visible overlay rows (memtable + mutable-run, newest
    /// version per row, non-deleted).
    ///
    /// Priority 2 (selective overlay probing): when `bound` is `Some`, only rows
    /// whose id is in it are materialized. The caller passes the index-resolved
    /// survivor set as `bound` exactly when every condition is index-served — in
    /// which case [`filter_overlay_rows`](Self::filter_overlay_rows) would discard
    /// any non-survivor overlay row anyway, so this prunes the materialization
    /// without changing the result. With a Range/RangeF64 residual the survivor
    /// set is incomplete for overlay rows, so the caller passes `None` (full
    /// materialization) and the range is re-evaluated downstream.
    fn overlay_visible_rows(&self, snapshot: Snapshot, bound: Option<&RowIdSet>) -> Vec<Row> {
        let mut best: HashMap<u64, (Epoch, Row)> = HashMap::new();
        let mut fold = |row: Row| {
            if let Some(b) = bound {
                if !b.contains(row.row_id.0) {
                    return;
                }
            }
            best.entry(row.row_id.0)
                .and_modify(|(be, br)| {
                    if row.committed_epoch > *be {
                        *be = row.committed_epoch;
                        *br = row.clone();
                    }
                })
                .or_insert_with(|| (row.committed_epoch, row));
        };
        for row in self.memtable.visible_versions(snapshot.epoch) {
            fold(row);
        }
        for row in self.mutable_run.visible_versions(snapshot.epoch) {
            fold(row);
        }
        let mut out: Vec<Row> = best
            .into_values()
            .filter_map(|(_, r)| if r.deleted { None } else { Some(r) })
            .collect();
        out.sort_by_key(|r| r.row_id);
        out
    }

    /// Filter overlay rows against the conjunctive predicate. Range / RangeF64
    /// are evaluated directly (the reader-served survivor set misses overlay
    /// rows). All other conditions are index-served (indexes maintained on
    /// every `put`) so the intersected `survivors` set includes overlay rows
    /// that match — but ONLY when every condition is index-served. When there
    /// is a mix, we compute per-condition index sets for non-range conditions
    /// and evaluate range conditions directly, so the intersection is correct.
    fn filter_overlay_rows(
        &self,
        rows: Vec<Row>,
        conditions: &[crate::query::Condition],
        survivors: Option<&RowIdSet>,
        snapshot: Snapshot,
    ) -> Result<Vec<Row>> {
        if conditions.is_empty() {
            return Ok(rows);
        }
        use crate::query::Condition;
        // Determine whether every condition is index-served (survivors set is
        // then complete for overlay rows). If so, a simple membership check
        // suffices and is cheapest.
        let all_index_served = !conditions
            .iter()
            .any(|c| matches!(c, Condition::Range { .. } | Condition::RangeF64 { .. }));
        if all_index_served {
            return Ok(rows
                .into_iter()
                .filter(|r| survivors.map_or(true, |s| s.contains(r.row_id.0)))
                .collect());
        }
        // Mixed: compute per-condition index sets for non-range conditions, and
        // evaluate range conditions directly on column values.
        let mut per_cond_sets: Vec<RowIdSet> = Vec::with_capacity(conditions.len());
        for c in conditions {
            let s = match c {
                Condition::Range { .. } | Condition::RangeF64 { .. } => RowIdSet::empty(),
                _ => self.resolve_condition(c, snapshot)?,
            };
            per_cond_sets.push(s);
        }
        Ok(rows
            .into_iter()
            .filter(|row| {
                conditions.iter().enumerate().all(|(i, c)| match c {
                    Condition::Range { column_id, lo, hi } => {
                        matches!(row.columns.get(column_id), Some(Value::Int64(v)) if *v >= *lo && *v <= *hi)
                    }
                    Condition::RangeF64 { column_id, lo, lo_inclusive, hi, hi_inclusive } => {
                        match row.columns.get(column_id) {
                            Some(Value::Float64(v)) => {
                                let lo_ok = if *lo_inclusive { *v >= *lo } else { *v > *lo };
                                let hi_ok = if *hi_inclusive { *v <= *hi } else { *v < *hi };
                                lo_ok && hi_ok
                            }
                            _ => false,
                        }
                    }
                    _ => per_cond_sets[i].contains(row.row_id.0),
                })
            })
            .collect())
    }

    /// Materialize overlay rows into typed `NativeColumn`s for the cursor's
    /// final batch.
    fn materialize_overlay(
        &self,
        rows: &[Row],
        projection: &[(u16, TypeId)],
    ) -> Vec<columnar::NativeColumn> {
        if projection.is_empty() {
            return vec![columnar::null_native(TypeId::Int64, rows.len())];
        }
        let mut cols = Vec::with_capacity(projection.len());
        for (cid, ty) in projection {
            let vals: Vec<Value> = rows
                .iter()
                .map(|r| r.columns.get(cid).cloned().unwrap_or(Value::Null))
                .collect();
            cols.push(columnar::values_to_native(*ty, &vals));
        }
        cols
    }

    /// Resolve a conjunctive predicate to its surviving `RowId` set on the
    /// single-run fast path: each condition becomes a `RowId` set via the
    /// in-memory indexes or the reader's page-pruned range scan, then they are
    /// intersected. Mirrors the resolution inside [`Self::query_columns_native`].
    fn resolve_survivor_rids(
        &self,
        conditions: &[crate::query::Condition],
        reader: &mut RunReader,
    ) -> Result<RowIdSet> {
        use crate::query::Condition;
        let mut sets: Vec<RowIdSet> = Vec::new();
        for c in conditions {
            let s: RowIdSet = match c {
                Condition::Pk(key) => {
                    let lookup = self
                        .schema
                        .primary_key()
                        .map(|pk| self.index_lookup_key_bytes(pk.id, key))
                        .unwrap_or_else(|| key.clone());
                    self.hot
                        .get(&lookup)
                        .map(|r| RowIdSet::one(r.0))
                        .unwrap_or_else(RowIdSet::empty)
                }
                Condition::BitmapEq { column_id, value } => {
                    let lookup = self.index_lookup_key_bytes(*column_id, value);
                    self.bitmap
                        .get(column_id)
                        .map(|b| RowIdSet::from_roaring(b.get(&lookup)))
                        .unwrap_or_else(RowIdSet::empty)
                }
                Condition::BitmapIn { column_id, values } => {
                    let bm = self.bitmap.get(column_id);
                    let mut acc = roaring::RoaringBitmap::new();
                    if let Some(b) = bm {
                        for v in values {
                            let lookup = self.index_lookup_key_bytes(*column_id, v);
                            acc |= b.get(&lookup);
                        }
                    }
                    RowIdSet::from_roaring(acc)
                }
                Condition::FmContains { column_id, pattern } => self
                    .fm
                    .get(column_id)
                    .map(|f| {
                        RowIdSet::from_unsorted(
                            f.locate(pattern).into_iter().map(|r| r.0).collect(),
                        )
                    })
                    .unwrap_or_else(RowIdSet::empty),
                Condition::FmContainsAll {
                    column_id,
                    patterns,
                } => {
                    if let Some(f) = self.fm.get(column_id) {
                        let sets: Vec<RowIdSet> = patterns
                            .iter()
                            .map(|pat| {
                                RowIdSet::from_unsorted(
                                    f.locate(pat).into_iter().map(|r| r.0).collect(),
                                )
                            })
                            .collect();
                        RowIdSet::intersect_many(sets)
                    } else {
                        RowIdSet::empty()
                    }
                }
                Condition::Ann {
                    column_id,
                    query,
                    k,
                } => self
                    .ann
                    .get(column_id)
                    .map(|a| {
                        RowIdSet::from_unsorted(
                            a.search(query, *k).into_iter().map(|(r, _)| r.0).collect(),
                        )
                    })
                    .unwrap_or_else(RowIdSet::empty),
                Condition::SparseMatch {
                    column_id,
                    query,
                    k,
                } => self
                    .sparse
                    .get(column_id)
                    .map(|s| {
                        RowIdSet::from_unsorted(
                            s.search(query, *k).into_iter().map(|(r, _)| r.0).collect(),
                        )
                    })
                    .unwrap_or_else(RowIdSet::empty),
                Condition::Range { column_id, lo, hi } => {
                    if let Some(li) = self.learned_range.get(column_id) {
                        RowIdSet::from_unsorted(li.range(*lo, *hi).into_iter().collect())
                    } else {
                        reader.range_row_id_set_i64(*column_id, *lo, *hi)?
                    }
                }
                Condition::RangeF64 {
                    column_id,
                    lo,
                    lo_inclusive,
                    hi,
                    hi_inclusive,
                } => {
                    if let Some(li) = self.learned_range.get(column_id) {
                        RowIdSet::from_unsorted(
                            li.range_f64(*lo, *lo_inclusive, *hi, *hi_inclusive)
                                .into_iter()
                                .collect(),
                        )
                    } else {
                        reader.range_row_id_set_f64(
                            *column_id,
                            *lo,
                            *lo_inclusive,
                            *hi,
                            *hi_inclusive,
                        )?
                    }
                }
                Condition::IsNull { column_id } => reader.null_row_id_set(*column_id, true)?,
                Condition::IsNotNull { column_id } => reader.null_row_id_set(*column_id, false)?,
            };
            sets.push(s);
        }
        Ok(RowIdSet::intersect_many(sets))
    }

    /// Native vectorized aggregate over a (possibly filtered) column on the
    /// single-run fast path (Phase 7.2). Resolves survivors via the same
    /// page-pruned cursor as the scan, then accumulates the aggregate in one
    /// pass over the typed buffer — no `Value`, no Arrow `RecordBatch`.
    ///
    /// `column` is `None` for `COUNT(*)`. Returns `Ok(None)` when the fast path
    /// does not apply (multi-run / non-empty memtable); the caller scans.
    pub fn aggregate_native(
        &self,
        snapshot: Snapshot,
        column: Option<u16>,
        conditions: &[crate::query::Condition],
        agg: NativeAgg,
    ) -> Result<Option<NativeAggResult>> {
        if self.run_refs.len() != 1 {
            return Ok(None);
        }
        // Phase 7.1: with no WHERE, MIN/MAX/COUNT(col) come straight from page
        // min/max/null_count — no column decode at all.
        if conditions.is_empty() {
            if let Some(res) = self.aggregate_from_stats(snapshot, column, agg)? {
                return Ok(Some(res));
            }
        }
        // COUNT(*) needs no decode — the survivor count comes from the cursor's
        // precomputed page plans. (COUNT(col) would have to exclude nulls; defer
        // to the caller's scan path by returning None for it.)
        if matches!(agg, NativeAgg::Count) {
            if column.is_some() {
                return Ok(None);
            }
            let n = match self.native_page_cursor(snapshot, Vec::new(), conditions)? {
                Some(cursor) => cursor.remaining_rows(),
                None => return Ok(None),
            };
            return Ok(Some(NativeAggResult::Count(n as u64)));
        }
        let cid = match column {
            Some(c) => c,
            None => return Ok(None),
        };
        let ty = self.column_type(cid);
        let cursor = match self.native_page_cursor(snapshot, vec![(cid, ty)], conditions)? {
            Some(c) => c,
            None => return Ok(None),
        };
        match ty {
            TypeId::Int64 | TypeId::TimestampNanos | TypeId::Date32 => {
                let (count, sum, mn, mx) = accumulate_int(cursor)?;
                Ok(Some(pack_int(agg, count, sum, mn, mx)))
            }
            TypeId::Float64 => {
                let (count, sum, mn, mx) = accumulate_float(cursor)?;
                Ok(Some(pack_float(agg, count, sum, mn, mx)))
            }
            _ => Ok(None),
        }
    }

    /// Phase 7.1 metadata fast path: answer an unfiltered `MIN`/`MAX`/`COUNT(col)`
    /// straight from page `min`/`max`/`null_count` — no column decode. Returns
    /// `None` (caller decodes) for `COUNT(*)`/`SUM`/`AVG`, when exact stats are
    /// unavailable (multi-version run; [`Table::exact_column_stats`] gates this),
    /// or for a column whose stats omit `min`/`max` while it still holds values
    /// (e.g. an encrypted column) — returning `NULL` there would be a wrong
    /// answer, so we fall back to decoding.
    fn aggregate_from_stats(
        &self,
        snapshot: Snapshot,
        column: Option<u16>,
        agg: NativeAgg,
    ) -> Result<Option<NativeAggResult>> {
        let cid = match (agg, column) {
            (NativeAgg::Count | NativeAgg::Min | NativeAgg::Max, Some(c)) => c,
            _ => return Ok(None), // COUNT(*), SUM, AVG: not served from page stats
        };
        let Some(stats) = self.exact_column_stats(snapshot, &[cid])? else {
            return Ok(None);
        };
        let Some(cs) = stats.get(&cid) else {
            return Ok(None);
        };
        match agg {
            // COUNT(col) excludes NULLs: live rows minus the column's null count.
            NativeAgg::Count => Ok(Some(NativeAggResult::Count(
                self.live_count.saturating_sub(cs.null_count),
            ))),
            NativeAgg::Min | NativeAgg::Max => {
                let bound = if agg == NativeAgg::Min {
                    &cs.min
                } else {
                    &cs.max
                };
                match bound {
                    Some(Value::Int64(x)) => Ok(Some(NativeAggResult::Int(*x))),
                    Some(Value::Float64(x)) => Ok(Some(NativeAggResult::Float(*x))),
                    Some(_) => Ok(None), // unexpected stat type ⇒ decode
                    // No bound: a genuine SQL NULL only when the column is wholly
                    // null. Otherwise the stats are simply unavailable (encrypted),
                    // so decode for a correct answer.
                    None if cs.null_count >= self.live_count => Ok(Some(NativeAggResult::Null)),
                    None => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    /// Phase 7.1c: exact `COUNT(DISTINCT col)` from the bitmap index's partition
    /// cardinality — the number of distinct indexed values — with no scan. Each
    /// distinct value is one bitmap key; under the insert-only invariant (empty
    /// overlay, single run, `live_count == row_count`) every key has at least one
    /// live row, so the key count is exact. `NULL` is excluded from
    /// `COUNT(DISTINCT)`, so a null key (from an explicit `Value::Null` put) is
    /// discounted. Returns `None` (caller scans) without a bitmap index on the
    /// column or when the invariant does not hold.
    pub fn count_distinct_from_bitmap(&self, column_id: u16) -> Result<Option<u64>> {
        if !(self.memtable.is_empty() && self.mutable_run.is_empty() && self.run_refs.len() == 1) {
            return Ok(None);
        }
        let reader = self.open_reader(self.run_refs[0].run_id)?;
        if self.live_count != reader.row_count() as u64 {
            return Ok(None);
        }
        let Some(bm) = self.bitmap.get(&column_id) else {
            return Ok(None); // no bitmap index ⇒ let the caller scan
        };
        let mut distinct = bm.value_count() as u64;
        // A null key (explicit `Value::Null`) is indexed but excluded from
        // COUNT(DISTINCT). (Schema-evolution-absent columns are never indexed.)
        if !bm.get(&Value::Null.encode_key()).is_empty() {
            distinct = distinct.saturating_sub(1);
        }
        Ok(Some(distinct))
    }

    /// Incremental aggregate over the live table (Phase 8.3). For an append-only
    /// table, a warm cache entry (same `cache_key`) lets the result be refreshed
    /// by aggregating **only the newly inserted rows** (row-id watermark delta)
    /// and merging, instead of a full recompute. The caller supplies a stable
    /// `cache_key` (e.g. a hash of the SQL + projection); distinct queries must
    /// use distinct keys.
    ///
    /// Returns [`IncrementalAggResult`] with the merged state and whether the
    /// delta path was taken. A single `delete` (ever) disables the incremental
    /// path for the table, so correctness never relies on append-only behavior
    /// that deletes invalidate.
    pub fn aggregate_incremental(
        &mut self,
        cache_key: u64,
        conditions: &[crate::query::Condition],
        column: Option<u16>,
        agg: NativeAgg,
    ) -> Result<IncrementalAggResult> {
        let snap = self.snapshot();
        let cur_wm = self.allocator.current().0;
        let cur_epoch = snap.epoch.0;
        // The watermark equals the committed row count only when the memtable is
        // empty (every allocated row id is durably in a run). With pending
        // (uncommitted) writes the allocator is ahead of the visible set, so the
        // delta range would silently skip just-committed rows — disable the
        // incremental path entirely in that case. The mutable-run tier holding
        // un-spilled data also disables it (those rows aren't in a run yet).
        let incremental_ok =
            !self.had_deletes && self.memtable.is_empty() && self.mutable_run.is_empty();

        // Incremental path: append-only, no pending writes, warm cache, advanced
        // epoch.
        if incremental_ok {
            if let Some(cached) = self.agg_cache.get(&cache_key).cloned() {
                if cached.epoch == cur_epoch {
                    return Ok(IncrementalAggResult {
                        state: cached.state,
                        incremental: true,
                        delta_rows: 0,
                    });
                }
                if cached.epoch < cur_epoch && cached.watermark <= cur_wm {
                    let delta_rids: Vec<u64> = (cached.watermark..cur_wm).collect();
                    let delta_rows = self.rows_for_rids(&delta_rids, snap)?;
                    let index_sets = self.resolve_index_conditions(conditions, snap)?;
                    let delta_state = agg_state_from_rows(
                        &delta_rows,
                        conditions,
                        &index_sets,
                        column,
                        agg,
                        &self.schema,
                    )?;
                    let merged = cached.state.merge(delta_state);
                    let delta_n = delta_rids.len() as u64;
                    self.agg_cache.insert(
                        cache_key,
                        CachedAgg {
                            state: merged.clone(),
                            watermark: cur_wm,
                            epoch: cur_epoch,
                        },
                    );
                    return Ok(IncrementalAggResult {
                        state: merged,
                        incremental: true,
                        delta_rows: delta_n,
                    });
                }
            }
        }

        // Cold path. For Count/Sum/Min/Max the fast vectorized cursor produces a
        // directly-seedable state; for Avg it returns only the mean (losing the
        // sum+count needed to merge a future delta), so Avg falls back to a
        // visible-rows scan that captures both.
        let cursor_ok =
            self.memtable.is_empty() && self.mutable_run.is_empty() && self.run_refs.len() == 1;
        let state = if cursor_ok && agg != NativeAgg::Avg {
            match self.aggregate_native(snap, column, conditions, agg)? {
                Some(result) => {
                    AggState::from_native(result, agg, column.map(|c| self.column_type(c)))
                }
                None => self.agg_state_full_scan(conditions, column, agg, snap)?,
            }
        } else {
            self.agg_state_full_scan(conditions, column, agg, snap)?
        };
        // Seed only when the watermark is meaningful (no pending writes).
        if incremental_ok {
            self.agg_cache.insert(
                cache_key,
                CachedAgg {
                    state: state.clone(),
                    watermark: cur_wm,
                    epoch: cur_epoch,
                },
            );
        }
        Ok(IncrementalAggResult {
            state,
            incremental: false,
            delta_rows: 0,
        })
    }

    /// Full visible-rows scan → [`AggState`] (cold path; captures sum+count for
    /// correct Avg seeding).
    fn agg_state_full_scan(
        &self,
        conditions: &[crate::query::Condition],
        column: Option<u16>,
        agg: NativeAgg,
        snap: Snapshot,
    ) -> Result<AggState> {
        let rows = self.visible_rows(snap)?;
        let index_sets = self.resolve_index_conditions(conditions, snap)?;
        agg_state_from_rows(&rows, conditions, &index_sets, column, agg, &self.schema)
    }

    /// Resolve only the index-defined conditions (`Ann`/`SparseMatch`) to row-id
    /// sets for membership testing during row-wise aggregation.
    fn resolve_index_conditions(
        &self,
        conditions: &[crate::query::Condition],
        snapshot: Snapshot,
    ) -> Result<Vec<RowIdSet>> {
        use crate::query::Condition;
        let mut sets = Vec::new();
        for c in conditions {
            if matches!(c, Condition::Ann { .. } | Condition::SparseMatch { .. }) {
                sets.push(self.resolve_condition(c, snapshot)?);
            }
        }
        Ok(sets)
    }

    fn column_type(&self, cid: u16) -> TypeId {
        self.schema
            .columns
            .iter()
            .find(|c| c.id == cid)
            .map(|c| c.ty)
            .unwrap_or(TypeId::Bytes)
    }

    /// Approximate `COUNT`/`SUM`/`AVG` over a filtered set, computed from the
    /// in-memory reservoir sample (Phase 8.2). Returns a point estimate plus a
    /// normal-theory confidence interval at the supplied z-score (1.96 ≈ 95 %).
    ///
    /// The WHERE predicates are evaluated **exactly** on each sampled row (so
    /// LIKE/FM and equality/range contribute no index bias); `Ann`/`SparseMatch`
    /// are index-defined and resolved once to a row-id set that sampled rows are
    /// tested against. `Ok(None)` when there is no usable sample.
    pub fn approx_aggregate(
        &self,
        conditions: &[crate::query::Condition],
        column: Option<u16>,
        agg: ApproxAgg,
        z: f64,
    ) -> Result<Option<ApproxResult>> {
        use crate::query::Condition;
        let snapshot = self.snapshot();
        let n_pop = self.live_count;
        let sample_rids: Vec<u64> = self.reservoir.row_ids().to_vec();
        if sample_rids.is_empty() {
            return Ok(None);
        }
        // Materialize the live, non-deleted sampled rows.
        let live_sample = self.rows_for_rids(&sample_rids, snapshot)?;
        let s = live_sample.len();
        if s == 0 {
            return Ok(None);
        }

        // Pre-resolve Ann/Sparse conditions (index-defined predicates) to row-id
        // sets; the per-row predicates below are evaluated exactly.
        let mut index_sets: Vec<RowIdSet> = Vec::new();
        for c in conditions {
            if matches!(c, Condition::Ann { .. } | Condition::SparseMatch { .. }) {
                index_sets.push(self.resolve_condition(c, snapshot)?);
            }
        }

        // For Sum/Avg, gather the numeric column value of each passing row.
        let cid = match (agg, column) {
            (ApproxAgg::Count, _) => None,
            (_, Some(c)) => Some(c),
            _ => return Ok(None),
        };
        let mut passing_vals: Vec<f64> = Vec::with_capacity(s);
        for r in &live_sample {
            // Exact per-row predicate evaluation.
            if !conditions
                .iter()
                .all(|c| condition_matches_row(c, r, &self.schema))
            {
                continue;
            }
            // Ann/Sparse membership.
            if !index_sets.iter().all(|set| set.contains(r.row_id.0)) {
                continue;
            }
            if let Some(cid) = cid {
                if let Some(v) = as_f64(r.columns.get(&cid)) {
                    passing_vals.push(v);
                } // nulls ⇒ excluded (matching SQL AVG/SUM null semantics)
            } else {
                passing_vals.push(0.0); // placeholder for COUNT
            }
        }
        let m = passing_vals.len();

        let (point, half) = match agg {
            ApproxAgg::Count => {
                // Proportion estimate scaled to the population.
                let p = m as f64 / s as f64;
                let point = n_pop as f64 * p;
                let var = if s > 1 {
                    n_pop as f64 * n_pop as f64 * p * (1.0 - p) / s as f64
                        * (1.0 - s as f64 / n_pop as f64).max(0.0)
                } else {
                    0.0
                };
                (point, z * var.sqrt())
            }
            ApproxAgg::Sum => {
                // Horvitz–Thompson: each sampled row represents n_pop/s rows.
                let y: Vec<f64> = live_sample
                    .iter()
                    .map(|r| {
                        let passes_row = conditions
                            .iter()
                            .all(|c| condition_matches_row(c, r, &self.schema))
                            && index_sets.iter().all(|set| set.contains(r.row_id.0));
                        if passes_row {
                            cid.and_then(|c| as_f64(r.columns.get(&c))).unwrap_or(0.0)
                        } else {
                            0.0
                        }
                    })
                    .collect();
                let mean_y = y.iter().sum::<f64>() / s as f64;
                let point = n_pop as f64 * mean_y;
                let var = if s > 1 {
                    let ss: f64 = y.iter().map(|v| (v - mean_y).powi(2)).sum();
                    let var_y = ss / (s - 1) as f64;
                    n_pop as f64 * n_pop as f64 * var_y / s as f64
                        * (1.0 - s as f64 / n_pop as f64).max(0.0)
                } else {
                    0.0
                };
                (point, z * var.sqrt())
            }
            ApproxAgg::Avg => {
                if m == 0 {
                    return Ok(Some(ApproxResult {
                        point: 0.0,
                        ci_low: 0.0,
                        ci_high: 0.0,
                        n_population: n_pop,
                        n_sample_live: s,
                        n_passing: 0,
                    }));
                }
                let mean = passing_vals.iter().sum::<f64>() / m as f64;
                let half = if m > 1 {
                    let ss: f64 = passing_vals.iter().map(|v| (v - mean).powi(2)).sum();
                    let sd = (ss / (m - 1) as f64).sqrt();
                    let fpc = (1.0 - s as f64 / n_pop as f64).max(0.0);
                    z * sd / (m as f64).sqrt() * fpc.sqrt()
                } else {
                    0.0
                };
                (mean, half)
            }
        };

        Ok(Some(ApproxResult {
            point,
            ci_low: point - half,
            ci_high: point + half,
            n_population: n_pop,
            n_sample_live: s,
            n_passing: m,
        }))
    }

    /// Exact per-column statistics for the analytical aggregate fast path
    /// (Phase 7.1: `MIN`/`MAX`/`COUNT(col)` from page stats). Returns `None`
    /// unless the table is effectively insert-only at `snapshot` — empty
    /// memtable, a single sorted run, and `live_count == run.row_count()` — so
    /// the run's page `min`/`max`/`null_count` are exact (no tombstoned or
    /// superseded versions skew them). Under deletes/updates the caller falls
    /// back to scanning.
    pub fn exact_column_stats(
        &self,
        _snapshot: Snapshot,
        projection: &[u16],
    ) -> Result<Option<HashMap<u16, ColumnStat>>> {
        if !(self.memtable.is_empty() && self.mutable_run.is_empty() && self.run_refs.len() == 1) {
            return Ok(None);
        }
        let reader = self.open_reader(self.run_refs[0].run_id)?;
        if self.live_count != reader.row_count() as u64 {
            return Ok(None);
        }
        let mut out = HashMap::new();
        for &cid in projection {
            let cdef = match self.schema.columns.iter().find(|c| c.id == cid) {
                Some(c) => c,
                None => continue,
            };
            // Absent column (schema evolution) ⇒ all rows null.
            let Some(stats) = reader.column_page_stats(cid) else {
                out.insert(
                    cid,
                    ColumnStat {
                        min: None,
                        max: None,
                        null_count: self.live_count,
                    },
                );
                continue;
            };
            let stat = match cdef.ty {
                TypeId::Int64 | TypeId::TimestampNanos | TypeId::Date32 => {
                    agg_int(stats, crate::sorted_run::be_i64).map(|(mn, mx, n)| ColumnStat {
                        min: mn.map(Value::Int64),
                        max: mx.map(Value::Int64),
                        null_count: n,
                    })
                }
                TypeId::Float64 => {
                    agg_float(stats, crate::sorted_run::be_f64).map(|(mn, mx, n)| ColumnStat {
                        min: mn.map(Value::Float64),
                        max: mx.map(Value::Float64),
                        null_count: n,
                    })
                }
                _ => None,
            };
            if let Some(s) = stat {
                out.insert(cid, s);
            }
        }
        Ok(Some(out))
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub(crate) fn prepare_alter_column(
        &mut self,
        column_name: &str,
        change: &AlterColumn,
    ) -> Result<ColumnDef> {
        if !self.pending_rows.is_empty() || !self.pending_dels.is_empty() {
            return Err(MongrelError::InvalidArgument(
                "ALTER COLUMN requires committing staged writes first".into(),
            ));
        }
        let old = self
            .schema
            .columns
            .iter()
            .find(|c| c.name == column_name)
            .cloned()
            .ok_or_else(|| MongrelError::Schema(format!("unknown column {column_name}")))?;
        let mut next = old.clone();

        if let Some(name) = &change.name {
            let trimmed = name.trim();
            if trimmed.is_empty() {
                return Err(MongrelError::InvalidArgument(
                    "ALTER COLUMN name must not be empty".into(),
                ));
            }
            if trimmed != old.name && self.schema.columns.iter().any(|c| c.name == trimmed) {
                return Err(MongrelError::Schema(format!(
                    "column {trimmed} already exists"
                )));
            }
            next.name = trimmed.to_string();
        }

        if let Some(ty) = change.ty {
            next.ty = ty;
        }
        if let Some(flags) = change.flags {
            validate_alter_column_flags(old.flags, flags)?;
            next.flags = flags;
        }

        validate_alter_column_type(&self.schema, &old, &next, self.has_stored_versions())?;
        if old.flags.contains(ColumnFlags::NULLABLE)
            && !next.flags.contains(ColumnFlags::NULLABLE)
            && self.column_has_nulls(old.id)?
        {
            return Err(MongrelError::InvalidArgument(format!(
                "column '{}' contains NULL values",
                old.name
            )));
        }
        Ok(next)
    }

    pub(crate) fn apply_altered_column(&mut self, column: ColumnDef) -> Result<()> {
        let idx = self
            .schema
            .columns
            .iter()
            .position(|c| c.id == column.id)
            .ok_or_else(|| MongrelError::Schema(format!("unknown column {}", column.id)))?;
        if self.schema.columns[idx] == column {
            return Ok(());
        }
        self.schema.columns[idx] = column;
        self.schema.schema_id = self.schema.schema_id.saturating_add(1);
        self.schema.validate_auto_increment()?;
        self.auto_inc = resolve_auto_inc(&self.schema);
        self.column_keys = build_column_keys(self.kek.as_deref(), &self.schema);
        write_schema(&self.dir, &self.schema)?;
        self.clear_result_cache();
        let _ = std::fs::remove_dir_all(self.dir.join("_shadow"));
        self.persist_manifest(self.current_epoch())?;
        Ok(())
    }

    pub fn alter_column(&mut self, column_name: &str, change: AlterColumn) -> Result<ColumnDef> {
        let column = self.prepare_alter_column(column_name, &change)?;
        self.apply_altered_column(column.clone())?;
        Ok(column)
    }

    fn column_has_nulls(&mut self, column_id: u16) -> Result<bool> {
        if self.live_count == 0 {
            return Ok(false);
        }
        let snap = self.snapshot();
        let columns = self.visible_columns_native(snap, Some(&[column_id]))?;
        Ok(columns
            .first()
            .map(|(_, col)| col.null_count(col.len()) != 0)
            .unwrap_or(true))
    }

    fn has_stored_versions(&self) -> bool {
        !self.memtable.is_empty()
            || !self.mutable_run.is_empty()
            || self.run_refs.iter().any(|r| r.row_count > 0)
            || !self.retiring.is_empty()
    }

    /// Add a column to the schema (schema evolution). Existing runs simply read
    /// back as null for the new column until re-written. Persists the new schema
    /// and manifest. The caller supplies the full [`ColumnFlags`] so migrations
    /// can add `PRIMARY KEY` / `AUTO_INCREMENT` columns correctly.
    pub fn add_column(&mut self, name: &str, ty: TypeId, flags: ColumnFlags) -> Result<u16> {
        if self.schema.columns.iter().any(|c| c.name == name) {
            return Err(MongrelError::Schema(format!(
                "column {name} already exists"
            )));
        }
        let id = self.schema.columns.iter().map(|c| c.id).max().unwrap_or(0) + 1;
        self.schema.columns.push(ColumnDef {
            id,
            name: name.to_string(),
            ty,
            flags,
        });
        self.schema.schema_id = self.schema.schema_id.saturating_add(1);
        self.schema.validate_auto_increment()?;
        if flags.contains(ColumnFlags::AUTO_INCREMENT) {
            self.auto_inc = resolve_auto_inc(&self.schema);
        }
        write_schema(&self.dir, &self.schema)?;
        self.clear_result_cache();
        // Phase 15.5: invalidate Arrow IPC shadows (schema changed).
        let _ = std::fs::remove_dir_all(self.dir.join("_shadow"));
        self.persist_manifest(self.current_epoch())?;
        Ok(id)
    }

    /// Declare a `LearnedRange` (PGM) index on an existing numeric column and
    /// build it immediately from the current sorted run (Phase 13.3). After
    /// this, `Condition::Range` / `Condition::RangeF64` on that column resolve
    /// survivors sub-linearly (O(log segments + log ε)) instead of scanning the
    /// full column.
    ///
    /// Requires exactly one sorted run (call after `flush`). The index is
    /// rebuilt automatically on subsequent flushes.
    pub fn add_learned_range_index(&mut self, column_name: &str) -> Result<()> {
        let cid = self
            .schema
            .columns
            .iter()
            .find(|c| c.name == column_name)
            .map(|c| c.id)
            .ok_or_else(|| MongrelError::Schema(format!("unknown column {column_name}")))?;
        let ty = self
            .schema
            .columns
            .iter()
            .find(|c| c.id == cid)
            .map(|c| c.ty)
            .unwrap_or(TypeId::Int64);
        if !matches!(
            ty,
            TypeId::Int64 | TypeId::Float64 | TypeId::TimestampNanos | TypeId::Date32
        ) {
            return Err(MongrelError::Schema(format!(
                "LearnedRange requires a numeric column; {column_name} is {ty:?}"
            )));
        }
        if self
            .schema
            .indexes
            .iter()
            .any(|i| i.column_id == cid && i.kind == IndexKind::LearnedRange)
        {
            return Ok(()); // already declared
        }
        self.schema.indexes.push(IndexDef {
            name: format!("{}_learned_range", column_name),
            column_id: cid,
            kind: IndexKind::LearnedRange,
        });
        self.schema.schema_id = self.schema.schema_id.saturating_add(1);
        write_schema(&self.dir, &self.schema)?;
        self.build_learned_ranges()?;
        Ok(())
    }

    /// Tuning knob for the WAL auto-sync threshold. A no-op on a mounted table
    /// (the shared WAL's durability is governed by the group-commit coordinator).
    pub fn set_sync_byte_threshold(&mut self, threshold: u64) {
        self.sync_byte_threshold = threshold;
        if let WalSink::Private(w) = &mut self.wal {
            w.set_sync_byte_threshold(threshold);
        }
    }

    /// Flush all live page-cache entries to the persistent `_cache/` backing
    /// directory (best-effort). Useful before a clean shutdown so hot pages
    /// survive restart.
    pub fn page_cache_flush(&self) {
        self.page_cache.lock().flush_to_disk();
    }

    /// Number of entries currently in the shared page cache (diagnostic).
    pub fn page_cache_len(&self) -> usize {
        self.page_cache.lock().len()
    }

    /// Number of entries currently in the shared decoded-page cache (Phase
    /// 15.4 diagnostic).
    pub fn decoded_cache_len(&self) -> usize {
        self.decoded_cache.lock().len()
    }

    /// Drain the live memtable (prototype/testing helper used by the flush path
    /// demos). Prefer [`Table::flush`] for the durable path.
    pub fn drain_memtable_sorted(&mut self) -> Vec<Row> {
        self.memtable.drain_sorted()
    }

    pub(crate) fn run_path(&self, run_id: u64) -> PathBuf {
        self.dir.join(RUNS_DIR).join(format!("r-{run_id}.sr"))
    }

    pub(crate) fn table_dir(&self) -> &Path {
        &self.dir
    }

    pub(crate) fn schema_ref(&self) -> &crate::schema::Schema {
        &self.schema
    }

    pub(crate) fn alloc_run_id(&mut self) -> u64 {
        let id = self.next_run_id;
        self.next_run_id += 1;
        id
    }

    pub(crate) fn link_run(&mut self, run_ref: crate::manifest::RunRef) {
        self.run_refs.push(run_ref);
    }

    /// Link a spilled run found during shared-WAL recovery (spec §8.5).
    /// **Idempotent**: if the run is already in the manifest (the publish phase
    /// persisted it before the crash, or this is a clean reopen with the
    /// `TxnCommit` still in the WAL) this is a no-op returning `false`, so the
    /// caller never double-links or double-counts. Otherwise — a crash *after*
    /// the commit fsync but *before* publish persisted the manifest — the run is
    /// Enqueue a compaction-superseded run for retention-gated deletion (spec
    /// §6.4). The file stays on disk until [`Self::reap_retiring`] removes it
    /// once `min_active_snapshot` has advanced past `retire_epoch`.
    pub(crate) fn retire_run(&mut self, run_id: u128, retire_epoch: u64) {
        self.retiring.push(crate::manifest::RetiredRun {
            run_id,
            retire_epoch,
        });
    }

    /// Physically delete retired run files whose `retire_epoch` no pinned reader
    /// can still need (`min_active >= retire_epoch`), drop them from the queue,
    /// and persist the manifest if anything changed. Returns the count reaped.
    pub(crate) fn reap_retiring(&mut self, min_active: Epoch) -> Result<usize> {
        if self.retiring.is_empty() {
            return Ok(0);
        }
        let mut reaped = 0;
        let mut kept: Vec<crate::manifest::RetiredRun> = Vec::new();
        // Delete-then-persist is crash-idempotent: if we crash after unlinking
        // some files but before persisting, the manifest still lists them in
        // `retiring`; the next `reap_retiring` re-issues `remove_file` (the
        // error is ignored) and `check()` excludes `retiring` ids from orphan
        // detection, so the lingering entries are harmless until then.
        for r in std::mem::take(&mut self.retiring) {
            if min_active.0 >= r.retire_epoch {
                let _ = std::fs::remove_file(self.run_path(r.run_id as u64));
                reaped += 1;
            } else {
                kept.push(r);
            }
        }
        self.retiring = kept;
        if reaped > 0 {
            self.persist_manifest(self.current_epoch())?;
        }
        Ok(reaped)
    }

    pub(crate) fn recover_spilled_run(&mut self, run_ref: crate::manifest::RunRef) -> bool {
        if self.run_refs.iter().any(|r| r.run_id == run_ref.run_id) {
            return false;
        }
        self.live_count = self.live_count.saturating_add(run_ref.row_count);
        self.run_refs.push(run_ref);
        self.indexes_complete = false;
        true
    }

    pub(crate) fn kek_ref(&self) -> Option<&Arc<Kek>> {
        self.kek.as_ref()
    }

    pub(crate) fn open_reader(&self, run_id: u128) -> Result<RunReader> {
        let mut reader = RunReader::open_with_cache(
            self.dir.join(RUNS_DIR).join(format!("r-{run_id}.sr")),
            self.schema.clone(),
            self.kek.clone(),
            Some(self.page_cache.clone()),
            Some(self.decoded_cache.clone()),
            self.table_id,
        )?;
        // Overlay the real commit epoch for uniform-epoch (large-txn spill) runs:
        // their stored `_epoch` is a placeholder; the manifest RunRef carries the
        // assigned epoch. A no-op for ordinary runs.
        if let Some(rr) = self.run_refs.iter().find(|r| r.run_id == run_id) {
            reader.set_uniform_epoch(Epoch(rr.epoch_created));
        }
        Ok(reader)
    }

    pub(crate) fn run_refs(&self) -> &[RunRef] {
        &self.run_refs
    }

    pub(crate) fn runs_dir(&self) -> PathBuf {
        self.dir.join(RUNS_DIR)
    }

    pub(crate) fn wal_dir(&self) -> PathBuf {
        self.dir.join(WAL_DIR)
    }

    pub(crate) fn set_run_refs(&mut self, refs: Vec<RunRef>) {
        self.run_refs = refs;
    }

    pub(crate) fn next_run_id(&self) -> u64 {
        self.next_run_id
    }

    pub(crate) fn compaction_zstd_level(&self) -> i32 {
        self.compaction_zstd_level
    }

    pub(crate) fn bump_next_run_id(&mut self) {
        self.next_run_id += 1;
    }

    pub(crate) fn kek(&self) -> Option<Arc<Kek>> {
        self.kek.clone()
    }

    /// The index-checkpoint DEK (KEK-derived) for encrypted tables; `None` for
    /// plaintext tables. The checkpoint embeds index keys / PGM segment values
    /// derived from user data, so an encrypted table must encrypt it at rest.
    #[cfg(feature = "encryption")]
    fn idx_dek(&self) -> Option<Zeroizing<[u8; DEK_LEN]>> {
        self.kek.as_ref().map(|k| k.derive_idx_key())
    }

    #[cfg(not(feature = "encryption"))]
    fn idx_dek(&self) -> Option<Zeroizing<[u8; DEK_LEN]>> {
        None
    }

    /// Manifest (and other DB-wide metadata) meta DEK, derived from the KEK so
    /// the on-disk manifest is encrypted + authenticated at rest for encrypted
    /// tables. `None` for plaintext.
    #[cfg(feature = "encryption")]
    fn manifest_meta_dek(&self) -> Option<[u8; DEK_LEN]> {
        self.kek.as_ref().map(|k| *k.derive_meta_key())
    }

    #[cfg(not(feature = "encryption"))]
    fn manifest_meta_dek(&self) -> Option<[u8; DEK_LEN]> {
        None
    }

    /// `(column_id, scheme)` for every ENCRYPTED_INDEXABLE column — passed to
    /// the run writer so each run's descriptor records the column keys.
    pub(crate) fn indexable_column_specs(&self) -> Vec<(u16, u8)> {
        self.column_keys
            .iter()
            .map(|(&id, &(_, scheme))| (id, scheme))
            .collect()
    }

    /// Tokenize a value for an ENCRYPTED_INDEXABLE column (HMAC-eq or OPE-range,
    /// per the column's scheme). Returns `None` for plaintext columns. Indexes
    /// over such columns store tokens, and queries tokenize literals the same
    /// way — so lookups never decrypt the stored (encrypted) page payloads.
    #[cfg(feature = "encryption")]
    fn tokenize_value(&self, column_id: u16, v: &Value) -> Option<Value> {
        self.tokenize_value_enc(column_id, v)
    }

    #[cfg(feature = "encryption")]
    fn tokenize_value_enc(&self, column_id: u16, v: &Value) -> Option<Value> {
        use crate::encryption::{hmac_token, ope_token_f64, ope_token_i64, SCHEME_HMAC_EQ};
        let (key, scheme) = self.column_keys.get(&column_id)?;
        let token: Vec<u8> = match (*scheme, v) {
            (SCHEME_HMAC_EQ, _) => hmac_token(key, &v.encode_key()).to_vec(),
            (_, Value::Int64(x)) => ope_token_i64(key, *x).to_vec(),
            (_, Value::Float64(x)) => ope_token_f64(key, *x).to_vec(),
            _ => hmac_token(key, &v.encode_key()).to_vec(),
        };
        Some(Value::Bytes(token))
    }

    /// Encoded index key for a `Value`, tokenized for HMAC-eq columns.
    fn index_lookup_key(&self, column_id: u16, v: &Value) -> Vec<u8> {
        self.index_lookup_key_bytes(column_id, &v.encode_key())
    }

    /// Tokenize an already-encoded lookup key (equality queries pass the
    /// encoded search value; HMAC-eq columns wrap it under the column key).
    fn index_lookup_key_bytes(&self, column_id: u16, encoded: &[u8]) -> Vec<u8> {
        #[cfg(feature = "encryption")]
        {
            use crate::encryption::{hmac_token, SCHEME_HMAC_EQ};
            if let Some((key, scheme)) = self.column_keys.get(&column_id) {
                if *scheme == SCHEME_HMAC_EQ {
                    return hmac_token(key, encoded).to_vec();
                }
            }
        }
        let _ = column_id;
        encoded.to_vec()
    }
}

fn native_int64_strictly_increasing(col: &columnar::NativeColumn, n: usize) -> bool {
    let columnar::NativeColumn::Int64 { data, validity } = col else {
        return false;
    };
    if data.len() < n || !columnar::all_non_null(validity, n) {
        return false;
    }
    data.iter()
        .take(n)
        .zip(data.iter().skip(1))
        .all(|(a, b)| a < b)
}

/// Exact aggregate of a column's page stats into a min/max/null_count triple
/// (Phase 7.1). Only meaningful when the owning table is insert-only, which
/// [`Table::exact_column_stats`] gates on.
#[derive(Debug, Clone)]
pub struct ColumnStat {
    pub min: Option<Value>,
    pub max: Option<Value>,
    pub null_count: u64,
}

/// A supported native aggregate (Phase 7.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeAgg {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// The typed result of a [`NativeAgg`] over a column.
#[derive(Debug, Clone)]
pub enum NativeAggResult {
    Count(u64),
    Int(i64),
    Float(f64),
    /// No non-null inputs (SUM/MIN/MAX/AVG over zero rows ⇒ SQL NULL).
    Null,
}

/// A supported approximate aggregate over the reservoir sample (Phase 8.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApproxAgg {
    Count,
    Sum,
    Avg,
}

/// Point estimate with a normal-theory confidence interval from the reservoir
/// sample (Phase 8.2). `ci_low`/`ci_high` bracket `point` at the requested
/// z-score; the interval has zero width when the sample equals the whole table.
#[derive(Debug, Clone)]
pub struct ApproxResult {
    /// Point estimate of the aggregate.
    pub point: f64,
    /// Lower bound (`point − z·SE`).
    pub ci_low: f64,
    /// Upper bound (`point + z·SE`).
    pub ci_high: f64,
    /// Live population size (the table's `count()`).
    pub n_population: u64,
    /// Live rows in the sample (`≤` reservoir capacity).
    pub n_sample_live: usize,
    /// Sampled rows passing the WHERE predicate.
    pub n_passing: usize,
}

/// A mergeable running aggregate state (Phase 8.3). Two states over disjoint
/// row sets `merge` into the state over their union, so a cached analytical
/// aggregate can be updated by merging in only the delta (newly inserted rows)
/// instead of a full recompute.
#[derive(Debug, Clone, PartialEq)]
pub enum AggState {
    /// `COUNT(*)` or `COUNT(col)` over `n` matching rows.
    Count(u64),
    /// Int64 `SUM`: running `i128` sum + non-null count.
    SumI {
        sum: i128,
        count: u64,
    },
    /// Float64 `SUM`: running `f64` sum + non-null count.
    SumF {
        sum: f64,
        count: u64,
    },
    /// Int64 `AVG`: running `i128` sum + non-null count (avg = sum/count).
    AvgI {
        sum: i128,
        count: u64,
    },
    /// Float64 `AVG`: running `f64` sum + non-null count.
    AvgF {
        sum: f64,
        count: u64,
    },
    /// Int64 `MIN`/`MAX`.
    MinI(i64),
    MaxI(i64),
    /// Float64 `MIN`/`MAX`.
    MinF(f64),
    MaxF(f64),
    /// No matching rows observed yet.
    Empty,
}

impl AggState {
    /// Combine two states over disjoint row sets into the state over the union.
    pub fn merge(self, other: AggState) -> AggState {
        use AggState::*;
        match (self, other) {
            (Empty, x) | (x, Empty) => x,
            (Count(a), Count(b)) => Count(a + b),
            (SumI { sum: sa, count: ca }, SumI { sum: sb, count: cb }) => SumI {
                sum: sa + sb,
                count: ca + cb,
            },
            (SumF { sum: sa, count: ca }, SumF { sum: sb, count: cb }) => SumF {
                sum: sa + sb,
                count: ca + cb,
            },
            (AvgI { sum: sa, count: ca }, AvgI { sum: sb, count: cb }) => AvgI {
                sum: sa + sb,
                count: ca + cb,
            },
            (AvgF { sum: sa, count: ca }, AvgF { sum: sb, count: cb }) => AvgF {
                sum: sa + sb,
                count: ca + cb,
            },
            (MinI(a), MinI(b)) => MinI(a.min(b)),
            (MaxI(a), MaxI(b)) => MaxI(a.max(b)),
            (MinF(a), MinF(b)) => MinF(a.min(b)),
            (MaxF(a), MaxF(b)) => MaxF(a.max(b)),
            _ => Empty, // mismatched kinds — shouldn't happen (same query)
        }
    }

    /// The scalar point value (`f64`), or `None` when there were no inputs.
    pub fn point(&self) -> Option<f64> {
        match self {
            AggState::Count(n) => Some(*n as f64),
            AggState::SumI { sum, .. } => Some(*sum as f64),
            AggState::SumF { sum, .. } => Some(*sum),
            AggState::AvgI { sum, count } if *count > 0 => Some(*sum as f64 / *count as f64),
            AggState::AvgF { sum, count } if *count > 0 => Some(*sum / *count as f64),
            AggState::MinI(n) => Some(*n as f64),
            AggState::MaxI(n) => Some(*n as f64),
            AggState::MinF(n) => Some(*n),
            AggState::MaxF(n) => Some(*n),
            AggState::AvgI { .. } | AggState::AvgF { .. } | AggState::Empty => None,
        }
    }

    /// Convert a vectorized [`NativeAggResult`] (from the cursor path) into a
    /// mergeable [`AggState`], so the incremental cache can be seeded from the
    /// fast cold path. `ty` is the column's type (`None` for COUNT(*)).
    pub fn from_native(result: NativeAggResult, agg: NativeAgg, ty: Option<TypeId>) -> Self {
        let is_float = matches!(ty, Some(TypeId::Float64));
        match (agg, result) {
            (NativeAgg::Count, NativeAggResult::Count(n)) => AggState::Count(n),
            (NativeAgg::Sum, NativeAggResult::Int(x)) => AggState::SumI {
                sum: x as i128,
                count: 1, // count unknown from NativeAggResult; use sentinel
            },
            (NativeAgg::Sum, NativeAggResult::Float(x)) => AggState::SumF { sum: x, count: 1 },
            (NativeAgg::Avg, NativeAggResult::Float(x)) => AggState::AvgF { sum: x, count: 1 },
            (NativeAgg::Min, NativeAggResult::Int(x)) => AggState::MinI(x),
            (NativeAgg::Max, NativeAggResult::Int(x)) => AggState::MaxI(x),
            (NativeAgg::Min, NativeAggResult::Float(x)) => AggState::MinF(x),
            (NativeAgg::Max, NativeAggResult::Float(x)) => AggState::MaxF(x),
            (NativeAgg::Count, _) => AggState::Empty,
            (_, NativeAggResult::Null) => AggState::Empty,
            _ => {
                let _ = is_float;
                AggState::Empty
            }
        }
    }
}

/// A cached incremental aggregate (Phase 8.3): the mergeable state, the row-id
/// watermark it covers (rows `[0, watermark)`), and the snapshot epoch.
#[derive(Debug, Clone)]
pub struct CachedAgg {
    pub state: AggState,
    pub watermark: u64,
    pub epoch: u64,
}

/// Outcome of [`Table::aggregate_incremental`].
#[derive(Debug, Clone)]
pub struct IncrementalAggResult {
    /// The aggregate state covering all rows at the current epoch.
    pub state: AggState,
    /// `true` when produced by merging only the delta (new rows); `false` when
    /// a full recompute was required (cold cache, deletes, or same epoch).
    pub incremental: bool,
    /// Rows processed in the delta pass (`0` for a full recompute).
    pub delta_rows: u64,
}

/// Compute a mergeable [`AggState`] over `rows` that pass every per-row
/// `conditions` conjunct (and whose row id is in every pre-resolved
/// `index_sets`). Shared by the cold (full) and warm (delta) incremental paths.
fn agg_state_from_rows(
    rows: &[Row],
    conditions: &[crate::query::Condition],
    index_sets: &[RowIdSet],
    column: Option<u16>,
    agg: NativeAgg,
    schema: &Schema,
) -> Result<AggState> {
    let mut count: u64 = 0;
    let mut sum_i: i128 = 0;
    let mut sum_f: f64 = 0.0;
    let mut mn_i: i64 = i64::MAX;
    let mut mx_i: i64 = i64::MIN;
    let mut mn_f: f64 = f64::INFINITY;
    let mut mx_f: f64 = f64::NEG_INFINITY;
    let mut saw_int = false;
    let mut saw_float = false;
    for r in rows {
        if !conditions
            .iter()
            .all(|c| condition_matches_row(c, r, schema))
        {
            continue;
        }
        if !index_sets.iter().all(|s| s.contains(r.row_id.0)) {
            continue;
        }
        match agg {
            NativeAgg::Count => match column {
                // COUNT(*) counts every passing row.
                None => count += 1,
                // COUNT(col) excludes NULLs — explicit `Value::Null` and a column
                // absent from the row (schema evolution) are both NULL.
                Some(cid) => match r.columns.get(&cid) {
                    None | Some(Value::Null) => {}
                    Some(_) => count += 1,
                },
            },
            _ => match column.and_then(|cid| r.columns.get(&cid)) {
                Some(Value::Int64(n)) => {
                    count += 1;
                    sum_i += *n as i128;
                    mn_i = mn_i.min(*n);
                    mx_i = mx_i.max(*n);
                    saw_int = true;
                }
                Some(Value::Float64(f)) => {
                    count += 1;
                    sum_f += f;
                    mn_f = mn_f.min(*f);
                    mx_f = mx_f.max(*f);
                    saw_float = true;
                }
                _ => {}
            },
        }
    }
    Ok(match agg {
        NativeAgg::Count => {
            if count == 0 {
                AggState::Empty
            } else {
                AggState::Count(count)
            }
        }
        NativeAgg::Sum => {
            if count == 0 {
                AggState::Empty
            } else if saw_int {
                AggState::SumI { sum: sum_i, count }
            } else {
                AggState::SumF { sum: sum_f, count }
            }
        }
        NativeAgg::Avg => {
            if count == 0 {
                AggState::Empty
            } else if saw_int {
                AggState::AvgI { sum: sum_i, count }
            } else {
                AggState::AvgF { sum: sum_f, count }
            }
        }
        NativeAgg::Min => {
            if !saw_int && !saw_float {
                AggState::Empty
            } else if saw_int {
                AggState::MinI(mn_i)
            } else {
                AggState::MinF(mn_f)
            }
        }
        NativeAgg::Max => {
            if !saw_int && !saw_float {
                AggState::Empty
            } else if saw_int {
                AggState::MaxI(mx_i)
            } else {
                AggState::MaxF(mx_f)
            }
        }
    })
}

/// Evaluate an index-served [`Condition`] exactly against a materialized row.
/// `Ann`/`SparseMatch` (index-defined) always pass here; callers test those via a
/// pre-resolved row-id set.
fn condition_matches_row(c: &crate::query::Condition, row: &Row, schema: &Schema) -> bool {
    use crate::query::Condition;
    match c {
        Condition::Pk(key) => match schema.primary_key() {
            Some(pk) => row
                .columns
                .get(&pk.id)
                .map(|v| v.encode_key() == *key)
                .unwrap_or(false),
            None => false,
        },
        Condition::BitmapEq { column_id, value } => row
            .columns
            .get(column_id)
            .map(|v| v.encode_key() == *value)
            .unwrap_or(false),
        Condition::BitmapIn { column_id, values } => {
            let key = row.columns.get(column_id).map(|v| v.encode_key());
            match key {
                Some(k) => values.contains(&k),
                None => false,
            }
        }
        Condition::Range { column_id, lo, hi } => match row.columns.get(column_id) {
            Some(Value::Int64(n)) => *n >= *lo && *n <= *hi,
            _ => false,
        },
        Condition::RangeF64 {
            column_id,
            lo,
            lo_inclusive,
            hi,
            hi_inclusive,
        } => match row.columns.get(column_id) {
            Some(Value::Float64(n)) => {
                let lo_ok = if *lo_inclusive { *n >= *lo } else { *n > *lo };
                let hi_ok = if *hi_inclusive { *n <= *hi } else { *n < *hi };
                lo_ok && hi_ok
            }
            _ => false,
        },
        Condition::FmContains { column_id, pattern } => match row.columns.get(column_id) {
            Some(Value::Bytes(b)) => {
                !pattern.is_empty() && b.windows(pattern.len()).any(|w| w == &pattern[..])
            }
            _ => false,
        },
        Condition::FmContainsAll {
            column_id,
            patterns,
        } => match row.columns.get(column_id) {
            Some(Value::Bytes(b)) => patterns
                .iter()
                .all(|pat| !pat.is_empty() && b.windows(pat.len()).any(|w| w == &pat[..])),
            _ => false,
        },
        Condition::Ann { .. } | Condition::SparseMatch { .. } => true,
        Condition::IsNull { column_id } => {
            matches!(row.columns.get(column_id), Some(Value::Null) | None)
        }
        Condition::IsNotNull { column_id } => {
            !matches!(row.columns.get(column_id), Some(Value::Null) | None)
        }
    }
}

/// Coerce a cell to `f64` for Sum/Avg (Int64/Float64 only).
fn as_f64(v: Option<&Value>) -> Option<f64> {
    match v {
        Some(Value::Int64(n)) => Some(*n as f64),
        Some(Value::Float64(f)) => Some(*f),
        _ => None,
    }
}

/// One-pass vectorized accumulation of `(non-null count, sum, min, max)` over an
/// Int64 column streamed through `cursor`. The inner loop over a contiguous
/// `&[i64]` autovectorizes (SIMD) for the all-non-null prefix.
fn accumulate_int(mut cursor: crate::cursor::NativePageCursor) -> Result<(u64, i128, i64, i64)> {
    let mut count: u64 = 0;
    let mut sum: i128 = 0;
    let mut mn: i64 = i64::MAX;
    let mut mx: i64 = i64::MIN;
    while let Some(cols) = cursor.next_batch()? {
        if let Some(crate::columnar::NativeColumn::Int64 { data, validity }) = cols.first() {
            if crate::columnar::all_non_null(validity, data.len()) {
                // All-non-null: vectorized sum/min/max with no per-element branch.
                count += data.len() as u64;
                sum += data.iter().map(|&v| v as i128).sum::<i128>();
                mn = mn.min(*data.iter().min().unwrap_or(&mn));
                mx = mx.max(*data.iter().max().unwrap_or(&mx));
            } else {
                for (i, &v) in data.iter().enumerate() {
                    if crate::columnar::validity_bit(validity, i) {
                        count += 1;
                        sum += v as i128;
                        mn = mn.min(v);
                        mx = mx.max(v);
                    }
                }
            }
        }
    }
    Ok((count, sum, mn, mx))
}

/// f64 analogue of [`accumulate_int`].
fn accumulate_float(mut cursor: crate::cursor::NativePageCursor) -> Result<(u64, f64, f64, f64)> {
    let mut count: u64 = 0;
    let mut sum: f64 = 0.0;
    let mut mn: f64 = f64::INFINITY;
    let mut mx: f64 = f64::NEG_INFINITY;
    while let Some(cols) = cursor.next_batch()? {
        if let Some(crate::columnar::NativeColumn::Float64 { data, validity }) = cols.first() {
            if crate::columnar::all_non_null(validity, data.len()) {
                count += data.len() as u64;
                sum += data.iter().sum::<f64>();
                mn = mn.min(data.iter().copied().fold(f64::INFINITY, f64::min));
                mx = mx.max(data.iter().copied().fold(f64::NEG_INFINITY, f64::max));
            } else {
                for (i, &v) in data.iter().enumerate() {
                    if crate::columnar::validity_bit(validity, i) {
                        count += 1;
                        sum += v;
                        mn = mn.min(v);
                        mx = mx.max(v);
                    }
                }
            }
        }
    }
    Ok((count, sum, mn, mx))
}

fn pack_int(agg: NativeAgg, count: u64, sum: i128, mn: i64, mx: i64) -> NativeAggResult {
    if count == 0 && !matches!(agg, NativeAgg::Count) {
        return NativeAggResult::Null;
    }
    match agg {
        NativeAgg::Count => NativeAggResult::Count(count),
        // i64 overflow on Sum ⇒ SQL NULL (DataFusion errors on overflow; null is
        // a safe, non-misleading fallback rather than a saturated wrong value).
        NativeAgg::Sum => match sum.try_into() {
            Ok(v) => NativeAggResult::Int(v),
            Err(_) => NativeAggResult::Null,
        },
        NativeAgg::Min => NativeAggResult::Int(mn),
        NativeAgg::Max => NativeAggResult::Int(mx),
        NativeAgg::Avg => NativeAggResult::Float((sum as f64) / (count as f64)),
    }
}

fn pack_float(agg: NativeAgg, count: u64, sum: f64, mn: f64, mx: f64) -> NativeAggResult {
    if count == 0 && !matches!(agg, NativeAgg::Count) {
        return NativeAggResult::Null;
    }
    match agg {
        NativeAgg::Count => NativeAggResult::Count(count),
        NativeAgg::Sum => NativeAggResult::Float(sum),
        NativeAgg::Min => NativeAggResult::Float(mn),
        NativeAgg::Max => NativeAggResult::Float(mx),
        NativeAgg::Avg => NativeAggResult::Float(sum / (count as f64)),
    }
}

/// Aggregate per-page `min`/`max`/`null_count` into a column-wide i64 triple.
/// Returns `None` if no page contributes a non-null min/max (all-null column).
fn agg_int(
    stats: &[crate::page::PageStat],
    decode: fn(Option<&[u8]>) -> Option<i64>,
) -> Option<(Option<i64>, Option<i64>, u64)> {
    let (mut mn, mut mx, mut nulls) = (i64::MAX, i64::MIN, 0u64);
    let mut any = false;
    for s in stats {
        if let Some(v) = decode(s.min.as_deref()) {
            mn = mn.min(v);
            any = true;
        }
        if let Some(v) = decode(s.max.as_deref()) {
            mx = mx.max(v);
            any = true;
        }
        nulls += s.null_count;
    }
    any.then_some((Some(mn), Some(mx), nulls))
}

/// f64 analogue of [`agg_int`] (compares as f64, not as bit patterns).
fn agg_float(
    stats: &[crate::page::PageStat],
    decode: fn(Option<&[u8]>) -> Option<f64>,
) -> Option<(Option<f64>, Option<f64>, u64)> {
    let (mut mn, mut mx, mut nulls) = (f64::INFINITY, f64::NEG_INFINITY, 0u64);
    let mut any = false;
    for s in stats {
        if let Some(v) = decode(s.min.as_deref()) {
            mn = mn.min(v);
            any = true;
        }
        if let Some(v) = decode(s.max.as_deref()) {
            mx = mx.max(v);
            any = true;
        }
        nulls += s.null_count;
    }
    any.then_some((Some(mn), Some(mx), nulls))
}

/// The four maintained secondary-index maps, keyed by column id.
type SecondaryIndexes = (
    HashMap<u16, BitmapIndex>,
    HashMap<u16, AnnIndex>,
    HashMap<u16, FmIndex>,
    HashMap<u16, SparseIndex>,
);

fn empty_indexes(schema: &Schema) -> SecondaryIndexes {
    let mut bitmap = HashMap::new();
    let mut ann = HashMap::new();
    let mut fm = HashMap::new();
    let mut sparse = HashMap::new();
    for idef in &schema.indexes {
        match idef.kind {
            IndexKind::Bitmap => {
                bitmap.insert(idef.column_id, BitmapIndex::new());
            }
            IndexKind::Ann => {
                let dim = schema
                    .columns
                    .iter()
                    .find(|c| c.id == idef.column_id)
                    .and_then(|c| match c.ty {
                        TypeId::Embedding { dim } => Some(dim as usize),
                        _ => None,
                    })
                    .unwrap_or(0);
                ann.insert(idef.column_id, AnnIndex::new(dim));
            }
            IndexKind::FmIndex => {
                fm.insert(idef.column_id, FmIndex::new());
            }
            IndexKind::Sparse => {
                sparse.insert(idef.column_id, SparseIndex::new());
            }
            _ => {}
        }
    }
    (bitmap, ann, fm, sparse)
}

const ALTER_COLUMN_PROTECTED_FLAGS: u32 = ColumnFlags::PRIMARY_KEY
    | ColumnFlags::AUTO_INCREMENT
    | ColumnFlags::ENCRYPTED
    | ColumnFlags::ENCRYPTED_INDEXABLE
    | ColumnFlags::EMBEDDING_BINARY_QUANTIZED;

fn validate_alter_column_flags(old: ColumnFlags, new: ColumnFlags) -> Result<()> {
    if (old.bits() ^ new.bits()) & ALTER_COLUMN_PROTECTED_FLAGS != 0 {
        return Err(MongrelError::Schema(
            "ALTER COLUMN may only change NULLABLE; primary key, auto-increment, encryption, and embedding flags are immutable".into(),
        ));
    }
    Ok(())
}

fn validate_alter_column_type(
    schema: &Schema,
    old: &ColumnDef,
    next: &ColumnDef,
    has_stored_versions: bool,
) -> Result<()> {
    if old.ty == next.ty {
        return Ok(());
    }
    if schema.indexes.iter().any(|i| i.column_id == old.id) {
        return Err(MongrelError::Schema(format!(
            "ALTER COLUMN TYPE is not supported for indexed column '{}'",
            old.name
        )));
    }
    if !has_stored_versions || storage_compatible_type_change(old.ty, next.ty) {
        return Ok(());
    }
    Err(MongrelError::Schema(format!(
        "ALTER COLUMN TYPE from {:?} to {:?} requires an empty column or a representation-compatible type",
        old.ty, next.ty
    )))
}

fn storage_compatible_type_change(old: TypeId, new: TypeId) -> bool {
    matches!(
        (old, new),
        (TypeId::Int64, TypeId::TimestampNanos) | (TypeId::TimestampNanos, TypeId::Int64)
    )
}

fn index_into(
    schema: &Schema,
    row: &Row,
    hot: &mut HotIndex,
    bitmap: &mut HashMap<u16, BitmapIndex>,
    ann: &mut HashMap<u16, AnnIndex>,
    fm: &mut HashMap<u16, FmIndex>,
    sparse: &mut HashMap<u16, SparseIndex>,
) {
    for idef in &schema.indexes {
        let Some(val) = row.columns.get(&idef.column_id) else {
            continue;
        };
        match idef.kind {
            IndexKind::Bitmap => {
                if let Some(b) = bitmap.get_mut(&idef.column_id) {
                    b.insert(val.encode_key(), row.row_id);
                }
            }
            IndexKind::Ann => {
                if let (Some(a), Value::Embedding(v)) = (ann.get_mut(&idef.column_id), val) {
                    a.insert(v, row.row_id);
                }
            }
            IndexKind::FmIndex => {
                if let (Some(f), Value::Bytes(b)) = (fm.get_mut(&idef.column_id), val) {
                    f.insert(b.clone(), row.row_id);
                }
            }
            IndexKind::Sparse => {
                if let (Some(s), Value::Bytes(b)) = (sparse.get_mut(&idef.column_id), val) {
                    // A sparse vector is stored as a bincode'd `Vec<(u32, f32)>`
                    // in a Bytes column (SPLADE weights in, retrieval out).
                    if let Ok(terms) = bincode::deserialize::<Vec<(u32, f32)>>(b) {
                        s.insert(&terms, row.row_id);
                    }
                }
            }
            _ => {}
        }
    }
    if let Some(pk_col) = schema.primary_key() {
        if let Some(pk_val) = row.columns.get(&pk_col.id) {
            hot.insert(pk_val.encode_key(), row.row_id);
        }
    }
}

/// Per-element index key for the typed bulk-index path (Phase 14.2): mirrors
/// `index_into` on a `tokenized_for_indexes(row)` — encodes the element the way
/// [`Value::encode_key`] would, then applies the column's
/// `ENCRYPTED_INDEXABLE` tokenization (HMAC-eq / OPE) so bitmap/HOT keys match
/// what the incremental path stores. Returns `None` for null slots.
#[allow(dead_code)]
fn bulk_index_key(
    column_keys: &HashMap<u16, ([u8; 32], u8)>,
    column_id: u16,
    ty: TypeId,
    col: &columnar::NativeColumn,
    i: usize,
) -> Option<Vec<u8>> {
    let encoded = columnar::encode_key_native(ty, col, i)?;
    #[cfg(feature = "encryption")]
    {
        use crate::encryption::{hmac_token, ope_token_f64, ope_token_i64, SCHEME_HMAC_EQ};
        if let Some((key, scheme)) = column_keys.get(&column_id) {
            return Some(match (*scheme, col) {
                (SCHEME_HMAC_EQ, _) => hmac_token(key, &encoded).to_vec(),
                (_, columnar::NativeColumn::Int64 { data, .. }) => {
                    ope_token_i64(key, data[i]).to_vec()
                }
                (_, columnar::NativeColumn::Float64 { data, .. }) => {
                    ope_token_f64(key, data[i]).to_vec()
                }
                _ => hmac_token(key, &encoded).to_vec(),
            });
        }
    }
    #[cfg(not(feature = "encryption"))]
    {
        let _ = (column_id, column_keys, col);
    }
    Some(encoded)
}

pub(crate) fn write_schema(dir: &Path, schema: &Schema) -> Result<()> {
    let json = serde_json::to_string_pretty(schema)
        .map_err(|e| MongrelError::Schema(format!("encode schema: {e}")))?;
    std::fs::write(dir.join(SCHEMA_FILENAME), json)?;
    Ok(())
}

fn read_schema(dir: &Path) -> Result<Schema> {
    serde_json::from_str(&std::fs::read_to_string(dir.join(SCHEMA_FILENAME))?)
        .map_err(|e| MongrelError::Schema(format!("decode schema: {e}")))
}

fn next_wal_segment(wal_dir: &Path) -> Result<PathBuf> {
    Ok(wal_dir.join(format!("seg-{:06}.wal", next_wal_number(wal_dir)?)))
}

fn latest_wal_segment(wal_dir: &Path) -> Result<Option<PathBuf>> {
    let n = list_wal_numbers(wal_dir)?;
    Ok(n.map(|max| wal_dir.join(format!("seg-{max:06}.wal"))))
}

fn next_wal_number(wal_dir: &Path) -> Result<u32> {
    Ok(list_wal_numbers(wal_dir)?.map(|m| m + 1).unwrap_or(0))
}

fn list_wal_numbers(wal_dir: &Path) -> Result<Option<u32>> {
    let _ = std::fs::create_dir_all(wal_dir);
    let mut max_n = None;
    for entry in std::fs::read_dir(wal_dir)? {
        let entry = entry?;
        let fname = entry.file_name();
        let Some(s) = fname.to_str() else {
            continue;
        };
        let Some(stripped) = s.strip_prefix("seg-") else {
            continue;
        };
        let Some(stripped) = stripped.strip_suffix(".wal") else {
            continue;
        };
        if let Ok(n) = stripped.parse::<u32>() {
            max_n = Some(max_n.map(|m: u32| m.max(n)).unwrap_or(n));
        }
    }
    Ok(max_n)
}
