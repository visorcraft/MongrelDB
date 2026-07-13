//! Multi-table `Database` container (spec §5, §6, §10).
//!
//! Owns the shared services — catalog, dual-counter epoch authority, shared
//! raw/decoded page caches, snapshot-retention registry, and the DB-wide KEK —
//! and mounts per-table [`Table`] engines under `tables/<id>/` that borrow them.
//! P1 scope: per-table WALs remain (collapsed into one shared WAL in P2); the
//! win here is one consistent commit clock across tables and one reopen path.

use crate::catalog::{self, Catalog, CatalogEntry, TableState, META_DEK_LEN};
use crate::engine::{SharedCtx, Table};
use crate::epoch::{Epoch, EpochAuthority, Snapshot};
use crate::error::{MongrelError, Result};
use crate::external_table::ExternalTableEntry;
use crate::memtable::Value;
use crate::procedure::{
    ProcedureCallOutput, ProcedureCallResult, ProcedureCallRow, ProcedureCondition, ProcedureEntry,
    ProcedureStep, ProcedureValue, StoredProcedure,
};
use crate::retention::{OwnedSnapshotGuard, SnapshotGuard, SnapshotRegistry};
use crate::rowid::RowId;
use crate::schema::{AlterColumn, ColumnDef, Schema, TypeId};
use crate::trigger::{
    StoredTrigger, TriggerCondition, TriggerConfig, TriggerEntry, TriggerEvent, TriggerExpr,
    TriggerRaiseAction, TriggerStep, TriggerTarget, TriggerTiming, TriggerValue,
};
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize};
use std::sync::Arc;

pub const TABLES_DIR: &str = "tables";
pub const VTAB_DIR: &str = "_vtab";
pub const META_DIR: &str = "_meta";
pub const KEYS_FILENAME: &str = "keys";
pub const HISTORY_RETENTION_FILENAME: &str = "history_retention";

/// Sentinel `table_id` for `CheckIssue`s that concern the shared WAL rather
/// than any table. `u64::MAX` is never allocated to a real table (the catalog
/// mints ids from 0 upward), so [`Database::doctor`] can safely skip them.
pub const WAL_TABLE_ID: u64 = u64::MAX;
/// Sentinel `table_id` for `CheckIssue`s that concern external-table module
/// state instead of an ordinary table.
pub const EXTERNAL_TABLE_ID: u64 = u64::MAX - 1;

fn current_unix_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn read_history_retention(root: &Path, current_epoch: Epoch) -> Result<(u64, Epoch)> {
    let path = root.join(META_DIR).join(HISTORY_RETENTION_FILENAME);
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((0, current_epoch));
        }
        Err(error) => return Err(error.into()),
    };
    let mut fields = text.split_whitespace();
    let epochs = fields
        .next()
        .ok_or_else(|| MongrelError::Other("history retention file is empty".into()))?
        .parse::<u64>()
        .map_err(|error| MongrelError::Other(format!("history retention epochs: {error}")))?;
    let start = fields
        .next()
        .unwrap_or("0")
        .parse::<u64>()
        .map_err(|error| MongrelError::Other(format!("history retention start: {error}")))?;
    Ok((epochs, Epoch(start)))
}

fn write_history_retention(root: &Path, epochs: u64, start: Epoch) -> Result<()> {
    let meta = root.join(META_DIR);
    std::fs::create_dir_all(&meta)?;
    let path = meta.join(HISTORY_RETENTION_FILENAME);
    let tmp = meta.join(format!("{HISTORY_RETENTION_FILENAME}.tmp"));
    {
        let mut file = std::fs::File::create(&tmp)?;
        writeln!(file, "{epochs} {}", start.0)?;
        file.sync_all()?;
    }
    std::fs::rename(tmp, path)?;
    if let Ok(dir) = std::fs::File::open(meta) {
        let _ = dir.sync_all();
    }
    Ok(())
}

fn prepare_backup_destination(
    source: &Path,
    destination: &Path,
) -> Result<(PathBuf, PathBuf, PathBuf)> {
    let source = source.canonicalize()?;
    if destination.exists() {
        return Err(MongrelError::Conflict(format!(
            "backup destination already exists: {}",
            destination.display()
        )));
    }
    let name = destination
        .file_name()
        .ok_or_else(|| MongrelError::InvalidArgument("invalid backup destination".into()))?;
    let requested_parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(requested_parent)?;
    let parent = requested_parent.canonicalize()?;
    if parent.starts_with(&source) {
        return Err(MongrelError::InvalidArgument(
            "backup destination must not be inside the source database".into(),
        ));
    }
    let destination = parent.join(name);
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let stage = parent.join(format!(
        ".{}.backup-stage-{}-{nonce}",
        name.to_string_lossy(),
        std::process::id()
    ));
    if stage.exists() {
        return Err(MongrelError::Conflict(format!(
            "backup staging path already exists: {}",
            stage.display()
        )));
    }
    Ok((destination, parent, stage))
}

fn copy_backup_boundary(
    source_root: &Path,
    destination_root: &Path,
    deferred_runs: &HashSet<PathBuf>,
    copied: &mut Vec<PathBuf>,
) -> Result<()> {
    fn visit(
        source_root: &Path,
        source: &Path,
        destination_root: &Path,
        deferred_runs: &HashSet<PathBuf>,
        copied: &mut Vec<PathBuf>,
    ) -> Result<()> {
        let mut entries = std::fs::read_dir(source)?.collect::<std::io::Result<Vec<_>>>()?;
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let path = entry.path();
            let relative = path
                .strip_prefix(source_root)
                .map_err(|error| MongrelError::Other(format!("backup path: {error}")))?;
            if backup_path_excluded(relative) {
                continue;
            }
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                return Err(MongrelError::InvalidArgument(format!(
                    "backup refuses symlink {}",
                    path.display()
                )));
            }
            if file_type.is_dir() {
                std::fs::create_dir_all(destination_root.join(relative))?;
                visit(source_root, &path, destination_root, deferred_runs, copied)?;
            } else if file_type.is_file() {
                if deferred_runs.contains(relative) {
                    continue;
                }
                if relative
                    .parent()
                    .and_then(Path::file_name)
                    .is_some_and(|parent| parent == "_runs")
                    && relative
                        .extension()
                        .is_some_and(|extension| extension == "sr")
                {
                    continue;
                }
                crate::backup::copy_file_synced(&path, &destination_root.join(relative))?;
                copied.push(relative.to_path_buf());
            }
        }
        Ok(())
    }

    std::fs::create_dir_all(destination_root)?;
    visit(
        source_root,
        source_root,
        destination_root,
        deferred_runs,
        copied,
    )
}

fn backup_path_excluded(relative: &Path) -> bool {
    relative == Path::new("_meta/.lock")
        || relative == Path::new("_meta/replica")
        || relative == Path::new("_meta/repl_epoch")
        || relative == Path::new(crate::backup::BACKUP_MANIFEST_PATH)
        || relative.components().any(|component| {
            matches!(component, std::path::Component::Normal(name) if name == "_cache" || name == "_txn" || name == "backup-pins")
        })
}

#[derive(Debug, Clone)]
pub enum ExternalTriggerWrite {
    Insert {
        table: String,
        cells: Vec<(u16, Value)>,
    },
    UpdateByPk {
        table: String,
        pk: Value,
        cells: Vec<(u16, Value)>,
    },
    DeleteByPk {
        table: String,
        pk: Value,
    },
}

impl ExternalTriggerWrite {
    fn table(&self) -> &str {
        match self {
            Self::Insert { table, .. }
            | Self::UpdateByPk { table, .. }
            | Self::DeleteByPk { table, .. } => table,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExternalTriggerBaseWrite {
    Put {
        table: String,
        cells: Vec<(u16, Value)>,
    },
    Delete {
        table: String,
        row_id: RowId,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExternalTriggerWriteResult {
    pub state: Vec<u8>,
    pub base_writes: Vec<ExternalTriggerBaseWrite>,
}

impl ExternalTriggerWriteResult {
    pub fn new(state: Vec<u8>) -> Self {
        Self {
            state,
            base_writes: Vec::new(),
        }
    }
}

pub trait ExternalTriggerBridge {
    fn apply_trigger_external_write(
        &self,
        entry: &ExternalTableEntry,
        base_state: Vec<u8>,
        op: ExternalTriggerWrite,
    ) -> Result<ExternalTriggerWriteResult>;
}

/// A pending uniform-epoch run written during a large transaction (spec §8.5).
struct SpilledRun {
    table_id: u64,
    run_id: u128,
    pending_path: PathBuf,
    rows: Vec<crate::memtable::Row>,
    row_count: u64,
    min_rid: u64,
    max_rid: u64,
}

#[derive(Debug, Clone)]
struct TriggerRowImage {
    columns: HashMap<u16, Value>,
}

impl TriggerRowImage {
    fn from_row(row: crate::memtable::Row) -> Self {
        Self {
            columns: row.columns,
        }
    }

    fn from_cells(cells: &[(u16, Value)]) -> Self {
        Self {
            columns: cells.iter().cloned().collect(),
        }
    }
}

#[derive(Debug, Clone)]
struct WriteEvent {
    table: String,
    kind: TriggerEvent,
    old: Option<TriggerRowImage>,
    new: Option<TriggerRowImage>,
    changed_columns: Vec<u16>,
    op_indices: Vec<usize>,
    put_idx: Option<usize>,
    trigger_stack: Vec<String>,
}

#[derive(Default)]
struct TriggerExpansion {
    before: Vec<(u64, crate::txn::Staged)>,
    before_stacks: Vec<Vec<String>>,
    before_external: Vec<ExternalTriggerWrite>,
    after: Vec<(u64, crate::txn::Staged)>,
    after_stacks: Vec<Vec<String>>,
    after_external: Vec<ExternalTriggerWrite>,
    ignored_indices: std::collections::BTreeSet<usize>,
}

struct TriggerProgramOutput<'a> {
    added: &'a mut Vec<(u64, crate::txn::Staged)>,
    added_stacks: &'a mut Vec<Vec<String>>,
    added_external: &'a mut Vec<ExternalTriggerWrite>,
    ignored_indices: &'a mut std::collections::BTreeSet<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TriggerProgramOutcome {
    Continue,
    Ignore,
}

/// An integrity issue found by [`Database::check`] (spec §16).
#[derive(Debug, Clone)]
pub struct CheckIssue {
    pub table_id: u64,
    pub table_name: String,
    pub severity: String,
    pub description: String,
}

/// One optimistic authorization snapshot for a complete scored read.
#[derive(Debug, Clone)]
pub struct AuthorizedReadSnapshot {
    pub table: String,
    pub table_snapshot: Snapshot,
    pub data_generation: u64,
    pub security_version: u64,
    pub allowed_row_ids: Option<HashSet<RowId>>,
}

type RlsCacheKey = (String, u64, u64, String);

/// Runtime statistics for the byte-bounded RLS candidate cache.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RlsCacheStats {
    pub entries: usize,
    pub bytes: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub build_nanos: u64,
    pub rows_evaluated: u64,
}

const RLS_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

#[derive(Default)]
struct RlsCache {
    entries: HashMap<RlsCacheKey, (Arc<HashSet<RowId>>, usize)>,
    lru: VecDeque<RlsCacheKey>,
    bytes: usize,
    hits: u64,
    misses: u64,
    evictions: u64,
    build_nanos: u64,
    rows_evaluated: u64,
}

impl RlsCache {
    fn get(&mut self, key: &RlsCacheKey) -> Option<Arc<HashSet<RowId>>> {
        let value = self.entries.get(key).map(|(value, _)| Arc::clone(value));
        if value.is_some() {
            self.hits = self.hits.saturating_add(1);
            self.touch(key);
        } else {
            self.misses = self.misses.saturating_add(1);
        }
        value
    }

    fn insert(&mut self, key: RlsCacheKey, value: Arc<HashSet<RowId>>) {
        let bytes = key
            .0
            .len()
            .saturating_add(key.3.len())
            .saturating_add(
                value
                    .capacity()
                    .saturating_mul(std::mem::size_of::<RowId>() * 3),
            )
            .saturating_add(std::mem::size_of::<RlsCacheKey>());
        if bytes > RLS_CACHE_MAX_BYTES {
            return;
        }
        if let Some((_, old_bytes)) = self.entries.remove(&key) {
            self.bytes = self.bytes.saturating_sub(old_bytes);
        }
        self.lru.retain(|candidate| candidate != &key);
        while self.bytes.saturating_add(bytes) > RLS_CACHE_MAX_BYTES {
            let Some(oldest) = self.lru.pop_front() else {
                break;
            };
            if let Some((_, old_bytes)) = self.entries.remove(&oldest) {
                self.bytes = self.bytes.saturating_sub(old_bytes);
                self.evictions = self.evictions.saturating_add(1);
            }
        }
        self.bytes = self.bytes.saturating_add(bytes);
        self.lru.push_back(key.clone());
        self.entries.insert(key, (value, bytes));
    }

    fn touch(&mut self, key: &RlsCacheKey) {
        self.lru.retain(|candidate| candidate != key);
        self.lru.push_back(key.clone());
    }

    fn stats(&self) -> RlsCacheStats {
        RlsCacheStats {
            entries: self.entries.len(),
            bytes: self.bytes,
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
            build_nanos: self.build_nanos,
            rows_evaluated: self.rows_evaluated,
        }
    }
}

/// A handle to a live table inside a [`Database`]. Writes take the inner lock
/// (P1); P3.3 replaces this with lock-free `ArcSwap` reads + a publish lock for
/// writes.
pub type TableHandle = Arc<Mutex<Table>>;

/// Knobs for [`Database::open_with_options`].
///
/// All fields default to the same values the convenience
/// [`Database::open`] / [`Database::open_encrypted`] / etc. constructors use,
/// so `OpenOptions::default()` round-trips the historical behavior exactly.
#[derive(Clone, Debug, Default)]
pub struct OpenOptions {
    /// Maximum time, in milliseconds, to wait for the cross-process database
    /// lock (`_meta/.lock`) before failing the open with `MongrelError::Io`.
    ///
    /// `0` (the default) preserves the historical fail-fast semantics: a
    /// single `try_lock_exclusive` call, no retry, no sleep. SQLite-style
    /// `busy_timeout` semantics kick in once this is non-zero — the open
    /// sleeps with progressively wider backoff (1ms → 10ms → 50ms) until
    /// either the lock is acquired or `lock_timeout_ms` elapses, at which
    /// point the open returns the same `Io(WouldBlock)` error the fail-fast
    /// path would.
    ///
    /// Only the cross-process lock is affected. Mounted tables, page-cache
    /// misses, and WAL appends already serialize through in-process locks
    /// that handle their own contention.
    pub lock_timeout_ms: u32,
}

impl OpenOptions {
    /// Set [`OpenOptions::lock_timeout_ms`]. `0` keeps the fail-fast default;
    /// SQLite-style applications typically pick 1_000 – 5_000ms.
    pub fn with_lock_timeout_ms(mut self, ms: u32) -> Self {
        self.lock_timeout_ms = ms;
        self
    }
}

/// A multi-table database: one catalog, one epoch clock, shared caches, a
/// shared WAL, and a live map of name → `Arc<Table>`.
pub struct Database {
    root: PathBuf,
    /// Set by `_meta/replica`; user writes are rejected on follower copies.
    read_only: bool,
    catalog: RwLock<Catalog>,
    rls_cache: Mutex<RlsCache>,
    epoch: Arc<EpochAuthority>,
    snapshots: Arc<SnapshotRegistry>,
    page_cache: Arc<crate::cache::Sharded<crate::cache::PageCache>>,
    decoded_cache: Arc<crate::cache::Sharded<crate::cache::DecodedPageCache>>,
    commit_lock: Arc<Mutex<()>>,
    /// One shared WAL multiplexing every table's records (spec §7.2). Owned
    /// behind a `Mutex` so the transaction layer can append + group-sync. Shared
    /// (via `Arc`) with every mounted `Table` so single-table `put`/`commit`
    /// writes also land in this one WAL (B1 — one WAL per database).
    shared_wal: Arc<Mutex<crate::wal::SharedWal>>,
    /// Monotonic per-open transaction-id counter. Scoped by `open_generation`
    /// in P2.7; here it just needs to be unique within an open. Shared with
    /// mounted tables so their auto-commit txn ids never alias cross-table ones.
    next_txn_id: Arc<Mutex<u64>>,
    tables: RwLock<HashMap<u64, TableHandle>>,
    kek: Option<Arc<crate::encryption::Kek>>,
    /// Serializes DDL (create/drop table); data commits serialize through
    /// `commit_lock` shared via `SharedCtx`.
    ddl_lock: Mutex<()>,
    meta_dek: Option<[u8; META_DEK_LEN]>,
    /// P3.4: when staged bytes per table exceed this, write a uniform-epoch
    /// pending run to `_txn/<txn_id>/` instead of streaming Put records (§8.5).
    spill_threshold: std::sync::atomic::AtomicU64,
    /// P3.1: write-key → commit_epoch for first-committer-wins conflict
    /// detection (spec §9.2).
    conflicts: crate::txn::ConflictIndex,
    /// P3.1: min read_epoch of all in-flight txns, drives conflict-index
    /// pruning (spec §9.2, review fix #12).
    active_txns: crate::txn::ActiveTxns,
    /// P3.2: set on fsync error — all subsequent writes fail fast (spec §9.3e).
    /// Shared with mounted tables so a single-table commit also honors poison.
    poisoned: Arc<std::sync::atomic::AtomicBool>,
    /// P3.2: group-commit coordinator. The sequencer appends under the WAL lock
    /// but defers the fsync to one leader here, so concurrent commits share a
    /// single fsync (spec §9.3). Shared with mounted tables.
    group: Arc<crate::txn::GroupCommit>,
    /// P3.6: txn ids currently spilling into `_txn/<id>/`. GC never deletes a
    /// live spill's pending dir (review fix #14, spec §6.4).
    active_spills: Arc<crate::retention::ActiveSpills>,
    /// A write lock captures a consistent bootstrap image; transaction commits
    /// hold a read lock across spill preparation, WAL append, and publish.
    replication_barrier: parking_lot::RwLock<()>,
    /// Number of rotated WAL segments retained for lagging followers.
    replication_wal_retention_segments: AtomicUsize,
    /// Live immutable run files being copied by online backups. Compaction may
    /// retire these runs, but GC cannot unlink them until the backup guard
    /// drops after the atomic install.
    backup_pins: Mutex<HashMap<(u64, u128), usize>>,
    /// Test-only barrier invoked after a transaction writes its spill runs but
    /// before the sequencer/publish, so tests can race `gc()` against an
    /// in-flight spill. `None` in production.
    #[doc(hidden)]
    spill_hook: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    /// Test seam after a backup boundary is captured and before pinned runs are
    /// copied. Lets tests compact+GC the source at the worst possible moment.
    #[doc(hidden)]
    backup_hook: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    trigger_recursive: AtomicBool,
    trigger_max_depth: AtomicU32,
    trigger_max_loop_iterations: AtomicU32,
    /// Exclusive cross-process lock held for the database's lifetime to prevent
    /// two processes from opening the same directory concurrently.
    _lock: Option<std::fs::File>,
    /// Lightweight channel for ephemeral SQL NOTIFY messages. Durable row CDC
    /// is reconstructed from the WAL by [`Database::change_events_since`].
    notify: tokio::sync::broadcast::Sender<ChangeEvent>,
    /// Commit-time wake-up for durable CDC consumers. Payloads are reconstructed
    /// from the WAL, so lagged receivers lose only a wake-up, never data.
    change_wake: tokio::sync::broadcast::Sender<()>,
    /// The authenticated principal for this handle. `None` on databases
    /// opened without credentials (the default — `require_auth = false`),
    /// `Some` on credentialed opens. Consulted by every enforcement point
    /// when the catalog's `require_auth` flag is set. Behind an `RwLock`
    /// because the access pattern is read-heavy: every `require()` call
    /// reads the principal, while writes happen only at open, `enable_auth`,
    /// and `refresh_principal`. This matches the engine's existing use of
    /// `RwLock` for `catalog` and `tables`.
    /// See `docs/15-credential-enforcement.md`.
    principal: RwLock<Option<crate::auth::Principal>>,
    /// Shared, cloneable handle to the auth state (require_auth flag from the
    /// catalog + the principal). Cloned into every mounted `Table` so the
    /// Table layer can enforce permissions without holding a reference back
    /// to `Database` (which would create a cycle). `AuthState` is already
    /// cheaply cloneable (inner `Arc`), so no outer `Arc` is needed.
    auth_state: crate::auth_state::AuthState,
}

/// RAII guard that ensures an assigned epoch is resolved (published or
/// abandoned) on every code path, including early `?` returns.
///
/// On drop, if not disarmed, calls [`EpochAuthority::abandon`] — the operation
/// failed, so the epoch must not become visible to readers. On success, the
/// caller calls [`EpochAuthority::publish_in_order`] and then
/// [`Self::disarm`] to prevent the abandon.
///
/// This is the root-cause fix for the epoch-hole bug: previously, if a DDL or
/// auth operation failed after `bump_assigned` but before `advance_visible`,
/// the epoch was never published, permanently blocking the in-order watermark
/// and making all subsequent queries return empty results.
struct EpochGuard<'a> {
    authority: &'a EpochAuthority,
    epoch: Epoch,
    armed: bool,
}

struct BackupRunPins<'a> {
    pins: &'a Mutex<HashMap<(u64, u128), usize>>,
    runs: Vec<(u64, u128)>,
}

struct BackupFilePins {
    root: PathBuf,
}

impl Drop for BackupFilePins {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

impl Drop for BackupRunPins<'_> {
    fn drop(&mut self) {
        let mut pins = self.pins.lock();
        for run in &self.runs {
            if let Some(count) = pins.get_mut(run) {
                *count -= 1;
                if *count == 0 {
                    pins.remove(run);
                }
            }
        }
    }
}

impl<'a> EpochGuard<'a> {
    fn new(authority: &'a EpochAuthority, epoch: Epoch) -> Self {
        Self {
            authority,
            epoch,
            armed: true,
        }
    }

    /// Mark the epoch as successfully published. Call this after
    /// `publish_in_order` to prevent the guard from abandoning the epoch.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for EpochGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.authority.abandon(self.epoch);
        }
    }
}

/// A durable data-change event reconstructed from committed WAL records, or an
/// ephemeral SQL `NOTIFY` event when `id` is `None`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChangeEvent {
    pub id: Option<String>,
    pub channel: String,
    pub table_id: Option<u64>,
    pub table: String,
    pub op: String,
    pub epoch: u64,
    pub txn_id: Option<u64>,
    pub message: Option<String>,
    pub data: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct CdcBatch {
    pub events: Vec<ChangeEvent>,
    pub current_epoch: u64,
    pub earliest_epoch: Option<u64>,
    pub gap: bool,
}

/// Manual `Debug` for `Database` — surfaces the diagnostics-relevant fields
/// (root, epoch, table count, encryption/auth state) without requiring every
/// internal type (Table, GroupCommit, broadcast sender, etc.) to impl Debug.
/// The raw field types carry locks, trait objects, and channels that have no
/// useful `Debug` output, so a hand-written impl is clearer than peppering
/// `#[allow(dead_code)]` skip attributes across two dozen fields.
impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cat = self.catalog.read();
        let principal_guard = self.principal.read();
        let principal: &str = principal_guard
            .as_ref()
            .map(|p| p.username.as_str())
            .unwrap_or("<none>");
        f.debug_struct("Database")
            .field("root", &self.root)
            .field("db_epoch", &cat.db_epoch)
            .field("open_generation", &"sidecar")
            .field("tables", &cat.tables.len())
            .field("visible_epoch", &self.epoch.visible().0)
            .field("encrypted", &self.kek.is_some())
            .field("require_auth", &cat.require_auth)
            .field("principal", &principal)
            .finish()
    }
}

impl Database {
    /// Create a fresh plaintext database at `root`.
    pub fn create(root: impl AsRef<Path>) -> Result<Self> {
        Self::create_inner(root, None)
    }

    /// Create a fresh encrypted database, deriving the DB-wide KEK from a
    /// passphrase (Argon2id + HKDF). The salt is persisted at `_meta/keys`.
    #[cfg(feature = "encryption")]
    pub fn create_encrypted(root: impl AsRef<Path>, passphrase: &str) -> Result<Self> {
        let root = root.as_ref();
        Self::reject_existing_database(root)?;
        std::fs::create_dir_all(root)?;
        std::fs::create_dir_all(root.join(META_DIR))?;
        let salt = crate::encryption::random_salt();
        std::fs::write(root.join(META_DIR).join(KEYS_FILENAME), salt)?;
        let kek = Arc::new(crate::encryption::Kek::derive(passphrase, &salt)?);
        Self::create_inner(root, Some(kek))
    }

    /// Create a fresh encrypted database, deriving the DB-wide KEK from a raw
    /// high-entropy key via HKDF. The salt is persisted at `_meta/keys`.
    #[cfg(feature = "encryption")]
    pub fn create_with_key(root: impl AsRef<Path>, key: &[u8]) -> Result<Self> {
        let root = root.as_ref();
        Self::reject_existing_database(root)?;
        std::fs::create_dir_all(root)?;
        std::fs::create_dir_all(root.join(META_DIR))?;
        let salt = crate::encryption::random_salt();
        std::fs::write(root.join(META_DIR).join(KEYS_FILENAME), salt)?;
        let kek = Arc::new(crate::encryption::Kek::from_raw_key(key, &salt)?);
        Self::create_inner(root, Some(kek))
    }

    fn create_inner(
        root: impl AsRef<Path>,
        kek: Option<Arc<crate::encryption::Kek>>,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        Self::reject_existing_database(&root)?;
        std::fs::create_dir_all(&root)?;
        std::fs::create_dir_all(root.join(TABLES_DIR))?;
        let meta_dek = crate::encryption::meta_dek_for(kek.as_deref());
        let cat = Catalog::empty();
        catalog::write_atomic(&root, &cat, meta_dek.as_ref())?;
        Self::finish_open(root, cat, kek, meta_dek, false, None, 0)
    }

    /// Open an existing plaintext database.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        Self::open_inner(root, None, None)
    }

    /// Open an existing encrypted database with a passphrase.
    #[cfg(feature = "encryption")]
    pub fn open_encrypted(root: impl AsRef<Path>, passphrase: &str) -> Result<Self> {
        let root = root.as_ref();
        let salt_bytes = std::fs::read(root.join(META_DIR).join(KEYS_FILENAME))
            .map_err(|e| MongrelError::NotFound(format!("encryption salt file: {e}")))?;
        let mut salt = [0u8; crate::encryption::SALT_LEN];
        salt.copy_from_slice(&salt_bytes);
        let kek = Arc::new(crate::encryption::Kek::derive(passphrase, &salt)?);
        Self::open_inner(root, Some(kek), None)
    }

    /// Open an existing encrypted database with a configurable cross-process
    /// lock timeout. Mirrors [`open_with_options`](Self::open_with_options).
    #[cfg(feature = "encryption")]
    pub fn open_encrypted_with_options(
        root: impl AsRef<Path>,
        passphrase: &str,
        options: OpenOptions,
    ) -> Result<Self> {
        let root = root.as_ref();
        let salt_bytes = std::fs::read(root.join(META_DIR).join(KEYS_FILENAME))
            .map_err(|e| MongrelError::NotFound(format!("encryption salt file: {e}")))?;
        let mut salt = [0u8; crate::encryption::SALT_LEN];
        salt.copy_from_slice(&salt_bytes);
        let kek = Arc::new(crate::encryption::Kek::derive(passphrase, &salt)?);
        Self::open_inner_with_lock_timeout(root, Some(kek), None, options.lock_timeout_ms)
    }

    /// Open an existing encrypted database using a raw high-entropy key.
    #[cfg(feature = "encryption")]
    pub fn open_with_key(root: impl AsRef<Path>, key: &[u8]) -> Result<Self> {
        let root = root.as_ref();
        let salt_path = root.join(META_DIR).join(KEYS_FILENAME);
        let salt_bytes = std::fs::read(&salt_path).map_err(|e| {
            MongrelError::NotFound(format!(
                "encryption salt file {:?}: {e} (database not encrypted, or corrupted)",
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
        let kek = Arc::new(crate::encryption::Kek::from_raw_key(key, &salt)?);
        Self::open_inner(root, Some(kek), None)
    }

    /// Open an existing plaintext database that has `require_auth = true`,
    /// verifying the supplied credentials up front and caching the resolved
    /// [`Principal`] on the returned handle. Every subsequent operation will
    /// be checked against that principal.
    ///
    /// Returns [`MongrelError::AuthNotRequired`] if the database does not have
    /// `require_auth` enabled — callers must pick the matching constructor for
    /// the database's mode. Returns [`MongrelError::InvalidCredentials`] on a
    /// bad username/password.
    ///
    /// See `docs/15-credential-enforcement.md`.
    pub fn open_with_credentials(
        root: impl AsRef<Path>,
        username: &str,
        password: &str,
    ) -> Result<Self> {
        Self::open_inner_with_credentials(root, None, username, password)
    }

    /// Open with credentials and a configurable cross-process lock timeout.
    /// Mirrors [`open_with_options`](Self::open_with_options) for the
    /// credentialed path.
    pub fn open_with_credentials_and_options(
        root: impl AsRef<Path>,
        username: &str,
        password: &str,
        options: OpenOptions,
    ) -> Result<Self> {
        Self::open_inner_with_credentials_and_lock_timeout(
            root,
            None,
            username,
            password,
            options.lock_timeout_ms,
        )
    }

    /// Open an existing encrypted database that has `require_auth = true`,
    /// combining the encryption passphrase flow with credential verification.
    #[cfg(feature = "encryption")]
    pub fn open_encrypted_with_credentials(
        root: impl AsRef<Path>,
        passphrase: &str,
        username: &str,
        password: &str,
    ) -> Result<Self> {
        let root = root.as_ref();
        let salt_bytes = std::fs::read(root.join(META_DIR).join(KEYS_FILENAME))
            .map_err(|e| MongrelError::NotFound(format!("encryption salt file: {e}")))?;
        let mut salt = [0u8; crate::encryption::SALT_LEN];
        salt.copy_from_slice(&salt_bytes);
        let kek = Arc::new(crate::encryption::Kek::derive(passphrase, &salt)?);
        Self::open_inner_with_credentials(root, Some(kek), username, password)
    }

    /// Open an encrypted + credentialed database with a configurable
    /// cross-process lock timeout. Mirrors
    /// [`open_encrypted_with_options`](Self::open_encrypted_with_options).
    #[cfg(feature = "encryption")]
    pub fn open_encrypted_with_credentials_and_options(
        root: impl AsRef<Path>,
        passphrase: &str,
        username: &str,
        password: &str,
        options: OpenOptions,
    ) -> Result<Self> {
        let root = root.as_ref();
        let salt_bytes = std::fs::read(root.join(META_DIR).join(KEYS_FILENAME))
            .map_err(|e| MongrelError::NotFound(format!("encryption salt file: {e}")))?;
        let mut salt = [0u8; crate::encryption::SALT_LEN];
        salt.copy_from_slice(&salt_bytes);
        let kek = Arc::new(crate::encryption::Kek::derive(passphrase, &salt)?);
        Self::open_inner_with_credentials_and_lock_timeout(
            root,
            Some(kek),
            username,
            password,
            options.lock_timeout_ms,
        )
    }

    /// Open an existing database with non-default [`OpenOptions`].
    ///
    /// Use this when you need cross-process lock retries (`lock_timeout_ms`)
    /// rather than the fail-fast default. The other open constructors keep
    /// their previous defaults; use their `*_with_options` variants when they
    /// need the same timeout behavior.
    pub fn open_with_options(root: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        // No encryption, no auth; encrypted and credentialed paths have their
        // own `*_with_options` constructors.
        Self::open_inner_with_lock_timeout(root, None, None, options.lock_timeout_ms)
    }

    fn open_inner_with_lock_timeout(
        root: impl AsRef<Path>,
        kek: Option<Arc<crate::encryption::Kek>>,
        _meta_dek_override: Option<[u8; META_DEK_LEN]>,
        lock_timeout_ms: u32,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let meta_dek = crate::encryption::meta_dek_for(kek.as_deref());
        let cat = catalog::read(&root, meta_dek.as_ref())?
            .ok_or_else(|| MongrelError::NotFound(format!("no catalog found at {:?}", root)))?;
        Self::finish_open(root, cat, kek, meta_dek, true, None, lock_timeout_ms)
    }

    /// Shared credentialed-open inner: read the catalog, verify the database
    /// requires auth, verify the password, resolve the principal, and pass
    /// everything to `finish_open` in one shot. This avoids the chicken-and-egg
    /// problem where `finish_open`'s fail-closed check (`require_auth &&
    /// principal.is_none()`) would fire before a post-open `authenticate()`
    /// could supply the principal.
    fn open_inner_with_credentials(
        root: impl AsRef<Path>,
        kek: Option<Arc<crate::encryption::Kek>>,
        username: &str,
        password: &str,
    ) -> Result<Self> {
        Self::open_inner_with_credentials_and_lock_timeout(root, kek, username, password, 0)
    }

    /// Credentialed-open with an explicit cross-process lock timeout. The
    /// timeout is opt-in: callers that don't pass `OpenOptions` keep the
    /// historical fail-fast behavior via the wrapper above.
    fn open_inner_with_credentials_and_lock_timeout(
        root: impl AsRef<Path>,
        kek: Option<Arc<crate::encryption::Kek>>,
        username: &str,
        password: &str,
        lock_timeout_ms: u32,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let meta_dek = crate::encryption::meta_dek_for(kek.as_deref());
        let cat = catalog::read(&root, meta_dek.as_ref())?
            .ok_or_else(|| MongrelError::NotFound(format!("no catalog found at {:?}", root)))?;

        // Fail early if the database is not in require_auth mode — the caller
        // picked the wrong constructor.
        if !cat.require_auth {
            return Err(MongrelError::AuthNotRequired);
        }

        // Verify credentials against the on-disk catalog before constructing
        // the full Database handle. This reads users/hashes directly from the
        // loaded catalog rather than going through the Database::verify_user
        // method (which requires a constructed Database).
        let user = cat
            .users
            .iter()
            .find(|u| u.username == username)
            .filter(|u| !u.password_hash.is_empty())
            .ok_or_else(|| MongrelError::InvalidCredentials {
                username: username.to_string(),
            })?;
        let password_ok = crate::auth::verify_password(password, &user.password_hash)
            .map_err(MongrelError::Other)?;
        if !password_ok {
            return Err(MongrelError::InvalidCredentials {
                username: username.to_string(),
            });
        }

        // Resolve the principal from the catalog (roles + permissions).
        let principal =
            Self::resolve_principal_from_catalog(&cat, &user.username).ok_or_else(|| {
                MongrelError::InvalidCredentials {
                    username: username.to_string(),
                }
            })?;

        Self::finish_open(
            root,
            cat,
            kek,
            meta_dek,
            true,
            Some(principal),
            lock_timeout_ms,
        )
    }

    /// Create a fresh plaintext database with `require_auth = true` and a
    /// single admin user. The returned handle is already authenticated as
    /// that admin — every subsequent operation is checked against the admin
    /// principal (which bypasses all permission checks via `is_admin`).
    ///
    /// This is the bootstrap path: there is no window where the database
    /// requires auth but has no users.
    ///
    /// See `docs/15-credential-enforcement.md`.
    pub fn create_with_credentials(
        root: impl AsRef<Path>,
        admin_username: &str,
        admin_password: &str,
    ) -> Result<Self> {
        Self::create_inner_with_credentials(root, None, admin_username, admin_password)
    }

    /// Create a fresh encrypted database with `require_auth = true` and a
    /// single admin user. Composes encryption-at-rest with credential
    /// enforcement.
    #[cfg(feature = "encryption")]
    pub fn create_encrypted_with_credentials(
        root: impl AsRef<Path>,
        passphrase: &str,
        admin_username: &str,
        admin_password: &str,
    ) -> Result<Self> {
        let root = root.as_ref();
        Self::reject_existing_database(root)?;
        std::fs::create_dir_all(root)?;
        std::fs::create_dir_all(root.join(META_DIR))?;
        let salt = crate::encryption::random_salt();
        std::fs::write(root.join(META_DIR).join(KEYS_FILENAME), salt)?;
        let kek = Arc::new(crate::encryption::Kek::derive(passphrase, &salt)?);
        Self::create_inner_with_credentials(root, Some(kek), admin_username, admin_password)
    }

    fn create_inner_with_credentials(
        root: impl AsRef<Path>,
        kek: Option<Arc<crate::encryption::Kek>>,
        admin_username: &str,
        admin_password: &str,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        Self::reject_existing_database(&root)?;
        std::fs::create_dir_all(&root)?;
        std::fs::create_dir_all(root.join(TABLES_DIR))?;
        let meta_dek = crate::encryption::meta_dek_for(kek.as_deref());

        // Build the initial catalog with require_auth = true and one admin user.
        let password_hash =
            crate::auth::hash_password(admin_password).map_err(MongrelError::Other)?;
        let mut cat = Catalog::empty();
        cat.require_auth = true;
        cat.next_user_id = 1;
        cat.users.push(crate::auth::UserEntry {
            id: 1,
            username: admin_username.to_string(),
            password_hash,
            roles: Vec::new(),
            is_admin: true,
            created_epoch: 0,
        });
        catalog::write_atomic(&root, &cat, meta_dek.as_ref())?;

        // The handle is constructed already authenticated as the admin user
        // it just created — no separate verify step needed.
        let admin_principal = crate::auth::Principal {
            username: admin_username.to_string(),
            is_admin: true,
            roles: Vec::new(),
            permissions: Vec::new(),
        };
        Self::finish_open(root, cat, kek, meta_dek, false, Some(admin_principal), 0)
    }

    fn reject_existing_database(root: &Path) -> Result<()> {
        // Refuse to overwrite an existing database. If CATALOG exists, the
        // directory already contains a real database; replacing it destroys data.
        if root.join(catalog::CATALOG_FILENAME).exists() {
            return Err(MongrelError::InvalidArgument(format!(
                "database already exists at {}; use Database::open() to open it, \
                 or remove the directory first",
                root.display()
            )));
        }
        Ok(())
    }

    fn open_inner(
        root: impl AsRef<Path>,
        kek: Option<Arc<crate::encryption::Kek>>,
        _meta_dek_override: Option<[u8; META_DEK_LEN]>,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let meta_dek = crate::encryption::meta_dek_for(kek.as_deref());
        let cat = catalog::read(&root, meta_dek.as_ref())?
            .ok_or_else(|| MongrelError::NotFound(format!("no catalog found at {:?}", root)))?;
        Self::finish_open(root, cat, kek, meta_dek, true, None, 0)
    }

    /// Acquire an exclusive advisory lock on `f`, retrying on `EAGAIN`/`EWOULDBLOCK`
    /// until `timeout_ms` elapses, mirroring SQLite's `busy_timeout` semantics.
    ///
    /// `timeout_ms == 0` is the fail-fast path: a single `try_lock_exclusive` call,
    /// no retry, no sleep. Existing open paths rely on that fail-fast default for
    /// backwards compatibility — opt in with `OpenOptions::lock_timeout_ms`.
    ///
    /// Backoff schedule: 1ms → 10ms → 50ms → 50ms → ... until `timeout_ms`.
    /// Total elapsed (not just sleep time) is bounded by `timeout_ms`, so the
    /// caller never blocks past its budget even at the tail of a busy lock
    /// holder's lock-window.
    fn fs_lock_exclusive(f: &std::fs::File, timeout_ms: u32) -> std::io::Result<()> {
        use fs2::FileExt;
        if timeout_ms == 0 {
            return f.try_lock_exclusive();
        }
        // Per-call deadline so a single stray 50ms sleep can't overshoot the budget.
        let deadline =
            std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms as u64);
        let mut next_sleep = std::time::Duration::from_millis(1);
        loop {
            match f.try_lock_exclusive() {
                Ok(()) => return Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    let now = std::time::Instant::now();
                    if now >= deadline {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::WouldBlock,
                            format!("could not acquire database lock within {timeout_ms}ms"),
                        ));
                    }
                    let remaining = deadline - now;
                    let sleep = next_sleep.min(remaining);
                    std::thread::sleep(sleep);
                    // Cap the per-iteration sleep so a single back-off step
                    // never overshoots the remaining budget.
                    next_sleep = next_sleep
                        .saturating_mul(10)
                        .min(std::time::Duration::from_millis(50));
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn finish_open(
        root: PathBuf,
        cat: Catalog,
        kek: Option<Arc<crate::encryption::Kek>>,
        meta_dek: Option<[u8; META_DEK_LEN]>,
        existing: bool,
        principal: Option<crate::auth::Principal>,
        lock_timeout_ms: u32,
    ) -> Result<Self> {
        let read_only = existing && root.join(META_DIR).join("replica").exists();
        // Acquire an exclusive cross-process lock on the database directory.
        // This prevents two *processes* from opening the same DB simultaneously
        // (which would corrupt data). Multiple opens within the *same* process
        // are allowed (they share memory via Arc) — so we track locked paths in
        // a process-global set and skip re-locking if already held.
        std::fs::create_dir_all(root.join("_meta")).ok();
        let lock_path = root.join("_meta").join(".lock");
        let canonical = lock_path.canonicalize().unwrap_or(lock_path.clone());
        let lock_file = {
            static LOCKED_PATHS: std::sync::OnceLock<
                std::sync::Mutex<std::collections::HashSet<PathBuf>>,
            > = std::sync::OnceLock::new();
            let locked = LOCKED_PATHS
                .get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()));
            let mut guard = locked.lock().unwrap();
            if guard.contains(&canonical) {
                // Already locked by this process — allow the re-open.
                None
            } else {
                let f = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .open(&lock_path)?;
                Self::fs_lock_exclusive(&f, lock_timeout_ms).map_err(|e| {
                    MongrelError::Io(std::io::Error::other(format!(
                        "database at {} is locked by another process: {e}",
                        root.display()
                    )))
                })?;
                guard.insert(canonical.clone());
                Some(f)
            }
        };
        if lock_file.is_some() {
            let stale_backup_pins = root.join(META_DIR).join("backup-pins");
            if stale_backup_pins.exists() {
                std::fs::remove_dir_all(stale_backup_pins)?;
            }
        }

        let epoch = Arc::new(EpochAuthority::new(cat.db_epoch));
        let snapshots = Arc::new(SnapshotRegistry::new());
        let (history_epochs, history_start) = read_history_retention(&root, Epoch(cat.db_epoch))?;
        snapshots.configure_history(history_epochs, history_start);
        let page_cache = Arc::new(crate::cache::Sharded::new(
            crate::cache::CACHE_SHARDS,
            || {
                crate::cache::PageCache::new(
                    crate::engine::PAGE_CACHE_CAPACITY / crate::cache::CACHE_SHARDS as u64,
                )
            },
        ));
        let decoded_cache = Arc::new(crate::cache::Sharded::new(
            crate::cache::CACHE_SHARDS,
            || {
                crate::cache::DecodedPageCache::new(
                    crate::engine::DECODED_CACHE_CAPACITY / crate::cache::CACHE_SHARDS as u64,
                )
            },
        ));
        let commit_lock = Arc::new(Mutex::new(()));
        let wal_dek = crate::encryption::wal_dek_for(kek.as_deref());
        let shared_wal = Arc::new(Mutex::new(if existing {
            crate::wal::SharedWal::open(&root, Epoch(cat.db_epoch), wal_dek.clone())?
        } else {
            crate::wal::SharedWal::create_with_dek(&root, Epoch(cat.db_epoch), wal_dek.clone())?
        }));
        // Shared write-path state handed to every mounted table so single-table
        // `put`/`commit` writes route through the one shared WAL, the one group-
        // commit coordinator, and the one poison flag (B1).
        let poisoned = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let group = Arc::new(crate::txn::GroupCommit::new(
            shared_wal.lock().durable_seq(),
        ));
        let (change_wake, _change_rx) = tokio::sync::broadcast::channel(256);
        // Final base value is set after the open-generation bump below; tables
        // only draw ids once the user issues a write (post-open), so the
        // placeholder is never observed.
        let txn_ids = Arc::new(Mutex::new(1u64));

        // Recover DDL from the shared WAL BEFORE opening tables (spec §15,
        // review fix #16). A crash between WAL fsync and the catalog
        // checkpoint leaves committed DDL durable in the WAL but absent from
        // the on-disk catalog; replay it here so the table-mounting loop and
        // data-record recovery see a correct catalog.
        let mut cat = cat;
        if existing {
            recover_ddl_from_wal(&root, &mut cat, meta_dek.as_ref(), wal_dek.as_ref())?;
        }

        // Build the shared auth state early — it's cloned into every mounted
        // Table's SharedCtx so the Table layer can enforce permissions without
        // a reference back to Database. The `require_auth` flag is mirrored
        // from the catalog; `enable_auth` / `refresh_principal` update it live.
        let auth_state = crate::auth_state::AuthState::new(cat.require_auth, principal.clone());
        let auth_checker: Option<Arc<dyn crate::auth_state::TableAuthChecker>> = Some(Arc::new(
            crate::auth_state::DefaultTableAuthChecker::new(auth_state.clone()),
        ));

        // Open every live table against the shared context. Mounted tables have
        // no private WAL (B1) — `open_in` just loads the manifest/runs and
        // advances the shared epoch authority to its manifest epoch, so the
        // final shared watermark is the max across all tables. All of a mounted
        // table's committed records are replayed below from the shared WAL.
        let mut tables: HashMap<u64, TableHandle> = HashMap::new();
        for entry in &cat.tables {
            if !matches!(entry.state, TableState::Live) {
                continue;
            }
            let tdir = root.join(TABLES_DIR).join(entry.table_id.to_string());
            let ctx = SharedCtx {
                epoch: Arc::clone(&epoch),
                page_cache: Arc::clone(&page_cache),
                decoded_cache: Arc::clone(&decoded_cache),
                snapshots: Arc::clone(&snapshots),
                kek: kek.clone(),
                commit_lock: Arc::clone(&commit_lock),
                shared: Some(crate::engine::SharedWalCtx {
                    wal: Arc::clone(&shared_wal),
                    group: Arc::clone(&group),
                    poisoned: Arc::clone(&poisoned),
                    txn_ids: Arc::clone(&txn_ids),
                    change_wake: change_wake.clone(),
                }),
                table_name: Some(entry.name.clone()),
                auth: auth_checker.clone(),
                read_only,
            };
            let t = Table::open_in(&tdir, ctx)?;
            tables.insert(entry.table_id, Arc::new(Mutex::new(t)));
        }

        // Recover transaction writes from the shared WAL (spec §15). This is the
        // single durability source for mounted tables: it applies every committed
        // record — both single-table `Table::commit` writes and cross-table
        // transactions — gated by each table's `flushed_epoch` (records already
        // durable in a run are not re-applied).
        if existing {
            recover_shared_wal(&root, &tables, &epoch, wal_dek.as_ref())?;
            // P3.4: sweep stale `_txn/<txn_id>/` dirs left by aborted/crashed
            // large transactions (spec §8.5, review fix #14).
            sweep_pending_txn_dirs(&root, &cat);
        }

        // Bump `open_generation` on every open and scope transaction ids by it
        // (`txn_id = (generation << 32) | counter`), so ids never alias across
        // reopens (review fix #11). Persist the bumped generation to a sidecar
        // file (`_meta/generation`) rather than CATALOG, so CATALOG stays
        // byte-stable across bare opens for content-addressed storage.
        let open_generation = if existing {
            let meta_dir = root.join(META_DIR);
            let gen = catalog::read_generation(&meta_dir);
            let bumped = gen.wrapping_add(1);
            catalog::write_generation(&meta_dir, bumped)?;
            bumped
        } else {
            0
        };
        let next_txn_id = (open_generation << 32) | 1;
        // Seed the shared txn-id allocator now that the generation is final.
        *txn_ids.lock() = next_txn_id;

        // Fail-closed: an existing database with `require_auth = true` must be
        // opened with credentials (a non-None principal). The credentialed
        // constructors pass the principal through finish_open; the plain
        // open/open_encrypted paths pass None and are rejected here. A brand-
        // new database (`existing = false`) never has require_auth set yet
        // (create_with_credentials sets it in the catalog before construction
        // AND passes the principal), so the check only gates the reopen path.
        if existing && cat.require_auth && principal.is_none() {
            return Err(MongrelError::AuthRequired);
        }

        Ok(Self {
            root,
            read_only,
            catalog: RwLock::new(cat),
            rls_cache: Mutex::new(RlsCache::default()),
            epoch,
            snapshots,
            page_cache,
            decoded_cache,
            commit_lock,
            shared_wal,
            next_txn_id: txn_ids,
            tables: RwLock::new(tables),
            kek,
            ddl_lock: Mutex::new(()),
            meta_dek,
            conflicts: crate::txn::ConflictIndex::new(),
            active_txns: crate::txn::ActiveTxns::new(),
            poisoned,
            group,
            spill_threshold: std::sync::atomic::AtomicU64::new(64 * 1024 * 1024),
            active_spills: Arc::new(crate::retention::ActiveSpills::new()),
            replication_barrier: parking_lot::RwLock::new(()),
            replication_wal_retention_segments: AtomicUsize::new(0),
            backup_pins: Mutex::new(HashMap::new()),
            spill_hook: Mutex::new(None),
            backup_hook: Mutex::new(None),
            trigger_recursive: AtomicBool::new(TriggerConfig::default().recursive_triggers),
            trigger_max_depth: AtomicU32::new(TriggerConfig::default().max_depth),
            trigger_max_loop_iterations: AtomicU32::new(
                TriggerConfig::default().max_loop_iterations,
            ),
            _lock: lock_file,
            notify: {
                let (tx, _rx) = tokio::sync::broadcast::channel(256);
                tx
            },
            change_wake,
            principal: RwLock::new(principal),
            auth_state,
        })
    }

    /// The current reader-visible epoch.
    pub fn visible_epoch(&self) -> Epoch {
        self.epoch.visible()
    }

    /// Clone the in-memory catalog (for diagnostics / tests).
    pub fn catalog_snapshot(&self) -> Catalog {
        self.catalog.read().clone()
    }

    pub fn materialized_view(&self, name: &str) -> Option<crate::catalog::MaterializedViewEntry> {
        self.catalog
            .read()
            .materialized_views
            .iter()
            .find(|definition| definition.name == name)
            .cloned()
    }

    pub fn materialized_views(&self) -> Vec<crate::catalog::MaterializedViewEntry> {
        self.catalog.read().materialized_views.clone()
    }

    pub fn security_catalog(&self) -> crate::security::SecurityCatalog {
        self.catalog.read().security.clone()
    }

    pub fn security_active_for(&self, table: &str) -> bool {
        self.catalog.read().security.table_has_security(table)
    }

    /// Persist a complete validated RLS/masking catalog through the WAL.
    pub fn set_security_catalog(&self, security: crate::security::SecurityCatalog) -> Result<()> {
        self.set_security_catalog_as(security, None)
    }

    /// Persist security policy changes on behalf of an explicit request principal.
    pub fn set_security_catalog_as(
        &self,
        security: crate::security::SecurityCatalog,
        principal: Option<&crate::auth::Principal>,
    ) -> Result<()> {
        use crate::wal::DdlOp;
        use std::sync::atomic::Ordering;

        self.require_for(principal, &crate::auth::Permission::Admin)?;
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }
        let _ddl = self.ddl_lock.lock();
        validate_security_catalog(&self.catalog.read(), &security)?;
        let payload = DdlOp::encode_security(&security)?;
        let _commit = self.commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let txn_id = self.alloc_txn_id();
        let commit_seq = {
            let mut wal = self.shared_wal.lock();
            wal.append(
                txn_id,
                WAL_TABLE_ID,
                crate::wal::Op::Ddl(DdlOp::SetSecurityCatalog {
                    security_json: payload,
                }),
            )?;
            wal.append_commit(txn_id, epoch, &[])?
        };
        self.group
            .await_durable(&self.shared_wal, commit_seq)
            .inspect_err(|_| {
                self.poisoned.store(true, Ordering::Relaxed);
            })?;
        {
            let mut catalog = self.catalog.write();
            catalog.security = security;
            catalog.security_version = catalog.security_version.wrapping_add(1);
            catalog.db_epoch = catalog.db_epoch.max(epoch.0);
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.epoch.publish_in_order(epoch);
        epoch_guard.disarm();
        Ok(())
    }

    pub fn require_for(
        &self,
        principal: Option<&crate::auth::Principal>,
        permission: &crate::auth::Permission,
    ) -> Result<()> {
        let Some(principal) = principal else {
            return self.require(permission);
        };
        if principal.has_permission(permission) {
            Ok(())
        } else {
            Err(MongrelError::PermissionDenied {
                required: permission.clone(),
                principal: principal.username.clone(),
            })
        }
    }

    pub fn principal_snapshot(&self) -> Option<crate::auth::Principal> {
        self.principal.read().clone()
    }

    pub fn require_columns_for(
        &self,
        table: &str,
        operation: crate::auth::ColumnOperation,
        column_ids: &[u16],
        principal: Option<&crate::auth::Principal>,
    ) -> Result<()> {
        let schema = self
            .catalog
            .read()
            .live(table)
            .ok_or_else(|| MongrelError::NotFound(format!("table {table:?} not found")))?
            .schema
            .clone();
        let cached = self.principal.read().clone();
        let principal = principal.or(cached.as_ref());
        let Some(principal) = principal else {
            let permission = match operation {
                crate::auth::ColumnOperation::Select => crate::auth::Permission::Select {
                    table: table.to_string(),
                },
                crate::auth::ColumnOperation::Insert => crate::auth::Permission::Insert {
                    table: table.to_string(),
                },
                crate::auth::ColumnOperation::Update => crate::auth::Permission::Update {
                    table: table.to_string(),
                },
            };
            return self.require(&permission);
        };
        match principal.column_access(table, operation) {
            crate::auth::ColumnAccess::All => Ok(()),
            crate::auth::ColumnAccess::Columns(allowed) => {
                let denied = column_ids.iter().find_map(|column_id| {
                    schema
                        .columns
                        .iter()
                        .find(|column| column.id == *column_id)
                        .filter(|column| !allowed.contains(&column.name))
                });
                if denied.is_none() {
                    Ok(())
                } else {
                    Err(MongrelError::PermissionDenied {
                        required: match operation {
                            crate::auth::ColumnOperation::Select => {
                                crate::auth::Permission::SelectColumns {
                                    table: table.to_string(),
                                    columns: denied
                                        .into_iter()
                                        .map(|column| column.name.clone())
                                        .collect(),
                                }
                            }
                            crate::auth::ColumnOperation::Insert => {
                                crate::auth::Permission::InsertColumns {
                                    table: table.to_string(),
                                    columns: denied
                                        .into_iter()
                                        .map(|column| column.name.clone())
                                        .collect(),
                                }
                            }
                            crate::auth::ColumnOperation::Update => {
                                crate::auth::Permission::UpdateColumns {
                                    table: table.to_string(),
                                    columns: denied
                                        .into_iter()
                                        .map(|column| column.name.clone())
                                        .collect(),
                                }
                            }
                        },
                        principal: principal.username.clone(),
                    })
                }
            }
            crate::auth::ColumnAccess::Denied => Err(MongrelError::PermissionDenied {
                required: match operation {
                    crate::auth::ColumnOperation::Select => crate::auth::Permission::Select {
                        table: table.to_string(),
                    },
                    crate::auth::ColumnOperation::Insert => crate::auth::Permission::Insert {
                        table: table.to_string(),
                    },
                    crate::auth::ColumnOperation::Update => crate::auth::Permission::Update {
                        table: table.to_string(),
                    },
                },
                principal: principal.username.clone(),
            }),
        }
    }

    pub fn select_column_ids_for(
        &self,
        table: &str,
        principal: Option<&crate::auth::Principal>,
    ) -> Result<Vec<u16>> {
        let catalog = self.catalog.read();
        let columns = catalog
            .live(table)
            .ok_or_else(|| MongrelError::NotFound(format!("table {table:?} not found")))?
            .schema
            .columns
            .iter()
            .map(|column| (column.id, column.name.clone()))
            .collect::<Vec<_>>();
        drop(catalog);
        let cached_principal = self.principal.read().clone();
        let principal = principal.or(cached_principal.as_ref());
        let Some(principal) = principal else {
            self.require(&crate::auth::Permission::Select {
                table: table.to_string(),
            })?;
            return Ok(columns.iter().map(|(id, _)| *id).collect());
        };
        match principal.column_access(table, crate::auth::ColumnOperation::Select) {
            crate::auth::ColumnAccess::All => Ok(columns.iter().map(|(id, _)| *id).collect()),
            crate::auth::ColumnAccess::Columns(allowed) => Ok(columns
                .iter()
                .filter(|(_, name)| allowed.contains(name))
                .map(|(id, _)| *id)
                .collect()),
            crate::auth::ColumnAccess::Denied => Err(MongrelError::PermissionDenied {
                required: crate::auth::Permission::Select {
                    table: table.to_string(),
                },
                principal: principal.username.clone(),
            }),
        }
    }

    pub fn secure_rows_for(
        &self,
        table: &str,
        rows: Vec<crate::memtable::Row>,
        principal: Option<&crate::auth::Principal>,
    ) -> Result<Vec<crate::memtable::Row>> {
        self.secure_rows_for_with_context(table, rows, principal, None)
    }

    pub fn secure_rows_for_with_context(
        &self,
        table: &str,
        rows: Vec<crate::memtable::Row>,
        principal: Option<&crate::auth::Principal>,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Vec<crate::memtable::Row>> {
        let security = self.catalog.read().security.clone();
        if !security.table_has_security(table) {
            return Ok(rows);
        }
        let owned;
        let principal = match principal {
            Some(principal) => principal,
            None => {
                owned = self
                    .principal
                    .read()
                    .clone()
                    .ok_or(MongrelError::AuthRequired)?;
                &owned
            }
        };
        let mut output = Vec::new();
        for mut row in rows {
            if let Some(context) = context {
                context.consume(1)?;
            }
            if security.row_allowed(
                table,
                crate::security::PolicyCommand::Select,
                &row,
                principal,
                false,
            ) {
                security.apply_masks(table, &mut row, principal);
                output.push(row);
            }
        }
        Ok(output)
    }

    /// Apply column masks to already RLS-authorized scored hits without a
    /// second row gather or policy evaluation.
    pub fn mask_search_hits_for(
        &self,
        table: &str,
        hits: &mut [crate::query::SearchHit],
        principal: Option<&crate::auth::Principal>,
    ) -> Result<()> {
        let security = self.catalog.read().security.clone();
        if !security.table_has_security(table) {
            return Ok(());
        }
        let owned;
        let principal = match principal {
            Some(principal) => principal,
            None => {
                owned = self.principal.read().clone();
                let Some(principal) = owned.as_ref() else {
                    return Ok(());
                };
                principal
            }
        };
        for hit in hits {
            security.apply_masks_to_cells(table, &mut hit.cells, principal);
        }
        Ok(())
    }

    /// Apply masks to rows already admitted by candidate-aware RLS.
    pub fn mask_rows_for(
        &self,
        table: &str,
        rows: &mut [crate::memtable::Row],
        principal: Option<&crate::auth::Principal>,
    ) -> Result<()> {
        let security = self.catalog.read().security.clone();
        if !security.table_has_security(table) {
            return Ok(());
        }
        let owned;
        let principal = match principal {
            Some(principal) => principal,
            None => {
                owned = self
                    .principal
                    .read()
                    .clone()
                    .ok_or(MongrelError::AuthRequired)?;
                &owned
            }
        };
        for row in rows {
            security.apply_masks(table, row, principal);
        }
        Ok(())
    }

    /// Row IDs allowed to enter scored ranking. `None` means no RLS filter.
    pub fn authorized_candidate_ids_for(
        &self,
        table: &str,
        principal: Option<&crate::auth::Principal>,
    ) -> Result<Option<std::collections::HashSet<RowId>>> {
        Ok(self
            .authorized_read_snapshot(table, principal)?
            .allowed_row_ids)
    }

    fn allowed_row_ids_locked(
        &self,
        table_name: &str,
        table: &Table,
        table_snapshot: Snapshot,
        security: &crate::security::SecurityCatalog,
        security_version: u64,
        principal: Option<&crate::auth::Principal>,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Option<Arc<HashSet<RowId>>>> {
        if !security.rls_enabled(table_name) {
            return Ok(None);
        }
        let authorization_started = std::time::Instant::now();
        let principal = principal.ok_or(MongrelError::AuthRequired)?;
        let mut roles = principal.roles.clone();
        roles.sort_unstable();
        let principal_key = format!("{}:{}:{roles:?}", principal.username, principal.is_admin);
        let cache_key = (
            table_name.to_string(),
            table.data_generation(),
            security_version,
            principal_key,
        );
        if let Some(allowed) = self.rls_cache.lock().get(&cache_key) {
            crate::trace::QueryTrace::record(|trace| {
                trace.rls_cache_hit = true;
                trace.authorization_nanos = trace
                    .authorization_nanos
                    .saturating_add(authorization_started.elapsed().as_nanos() as u64);
            });
            return Ok(Some(allowed));
        }
        if let Some(context) = context {
            context.checkpoint()?;
        }
        // ponytail: full RLS universe scan; replace with policy-column candidate checks if RLS search throughput matters.
        let started = std::time::Instant::now();
        let rows = table.visible_rows(table_snapshot)?;
        let rows_evaluated = rows.len() as u64;
        let mut allowed = HashSet::new();
        for chunk in rows.chunks(256) {
            if let Some(context) = context {
                context.consume(chunk.len())?;
            }
            allowed.extend(chunk.iter().filter_map(|row| {
                security
                    .row_allowed(
                        table_name,
                        crate::security::PolicyCommand::Select,
                        row,
                        principal,
                        false,
                    )
                    .then_some(row.row_id)
            }));
        }
        let allowed = Arc::new(allowed);
        let mut cache = self.rls_cache.lock();
        cache.build_nanos = cache
            .build_nanos
            .saturating_add(started.elapsed().as_nanos() as u64);
        cache.rows_evaluated = cache.rows_evaluated.saturating_add(rows_evaluated);
        cache.insert(cache_key, Arc::clone(&allowed));
        crate::trace::QueryTrace::record(|trace| {
            trace.rls_rows_evaluated = trace
                .rls_rows_evaluated
                .saturating_add(rows_evaluated as usize);
            trace.authorization_nanos = trace
                .authorization_nanos
                .saturating_add(authorization_started.elapsed().as_nanos() as u64);
        });
        Ok(Some(allowed))
    }

    fn principal_for_authorized_read(
        &self,
        catalog: &Catalog,
        principal: Option<&crate::auth::Principal>,
        catalog_bound: bool,
    ) -> Result<Option<crate::auth::Principal>> {
        let principal = principal.cloned().or_else(|| self.principal.read().clone());
        let Some(principal) = principal else {
            return Ok(None);
        };
        if catalog_bound
            || catalog
                .users
                .iter()
                .any(|user| user.username == principal.username)
        {
            return Self::resolve_principal_from_catalog(catalog, &principal.username)
                .map(Some)
                .ok_or(MongrelError::AuthRequired);
        }
        Ok(Some(principal))
    }

    /// Run authorization, candidate generation, ranking, and materialization
    /// while holding one table generation. Security changes cause a bounded
    /// retry before any result is published.
    pub fn with_authorized_read<T, F>(
        &self,
        table_name: &str,
        principal: Option<&crate::auth::Principal>,
        catalog_bound: bool,
        read: F,
    ) -> Result<T>
    where
        F: FnMut(
            &mut Table,
            Snapshot,
            Option<&HashSet<RowId>>,
            Option<&crate::auth::Principal>,
        ) -> Result<T>,
    {
        self.with_authorized_read_context(table_name, principal, catalog_bound, None, read)
    }

    pub fn with_authorized_read_context<T, F>(
        &self,
        table_name: &str,
        principal: Option<&crate::auth::Principal>,
        catalog_bound: bool,
        context: Option<&crate::query::AiExecutionContext>,
        mut read: F,
    ) -> Result<T>
    where
        F: FnMut(
            &mut Table,
            Snapshot,
            Option<&HashSet<RowId>>,
            Option<&crate::auth::Principal>,
        ) -> Result<T>,
    {
        if principal.is_none() && self.principal.read().is_some() {
            self.refresh_principal()?;
        }
        const RETRIES: usize = 3;
        let handle = self.table(table_name)?;
        for attempt in 0..RETRIES {
            crate::trace::QueryTrace::record(|trace| {
                trace.authorization_retries = attempt;
            });
            let (security, security_version, effective_principal) = {
                let catalog = self.catalog.read();
                (
                    catalog.security.clone(),
                    catalog.security_version,
                    self.principal_for_authorized_read(&catalog, principal, catalog_bound)?,
                )
            };
            let result = {
                let mut table = handle.lock();
                let snapshot = table.snapshot();
                let allowed = self.allowed_row_ids_locked(
                    table_name,
                    &table,
                    snapshot,
                    &security,
                    security_version,
                    effective_principal.as_ref(),
                    context,
                )?;
                read(
                    &mut table,
                    snapshot,
                    allowed.as_deref(),
                    effective_principal.as_ref(),
                )?
            };
            if self.catalog.read().security_version == security_version {
                return Ok(result);
            }
            if attempt + 1 == RETRIES {
                return Err(MongrelError::Conflict(
                    "security policy changed during scored read".into(),
                ));
            }
        }
        unreachable!()
    }

    /// Scored-read authorization that evaluates RLS only for approximate
    /// candidates. This avoids a full-table policy scan on cache misses while
    /// preserving one table generation and security-version retry.
    pub fn with_authorized_scored_read_context<T, F>(
        &self,
        table_name: &str,
        principal: Option<&crate::auth::Principal>,
        catalog_bound: bool,
        context: Option<&crate::query::AiExecutionContext>,
        mut read: F,
    ) -> Result<T>
    where
        F: FnMut(
            &mut Table,
            Snapshot,
            Option<&crate::security::CandidateAuthorization<'_>>,
            Option<&crate::auth::Principal>,
        ) -> Result<T>,
    {
        if principal.is_none() && self.principal.read().is_some() {
            self.refresh_principal()?;
        }
        const RETRIES: usize = 3;
        let handle = self.table(table_name)?;
        for attempt in 0..RETRIES {
            if let Some(context) = context {
                context.checkpoint()?;
            }
            crate::trace::QueryTrace::record(|trace| {
                trace.authorization_retries = attempt;
            });
            let (security, security_version, effective_principal) = {
                let catalog = self.catalog.read();
                (
                    catalog.security.clone(),
                    catalog.security_version,
                    self.principal_for_authorized_read(&catalog, principal, catalog_bound)?,
                )
            };
            let result = {
                let mut table = handle.lock();
                let snapshot = table.snapshot();
                let candidate_authorization = if security.rls_enabled(table_name) {
                    Some(crate::security::CandidateAuthorization {
                        table: table_name,
                        security: &security,
                        principal: effective_principal
                            .as_ref()
                            .ok_or(MongrelError::AuthRequired)?,
                    })
                } else {
                    None
                };
                read(
                    &mut table,
                    snapshot,
                    candidate_authorization.as_ref(),
                    effective_principal.as_ref(),
                )?
            };
            if self.catalog.read().security_version == security_version {
                return Ok(result);
            }
            if attempt + 1 == RETRIES {
                return Err(MongrelError::Conflict(
                    "security policy changed during scored read".into(),
                ));
            }
        }
        unreachable!()
    }

    /// Execute a native conjunctive read with the database principal's row
    /// policy, column grants, and masks applied. Raw [`Table`] methods remain
    /// policy-unaware; language bindings must use this boundary for reads.
    pub fn query_for_current_principal(
        &self,
        table_name: &str,
        query: &crate::query::Query,
        projection: Option<&[u16]>,
    ) -> Result<Vec<crate::memtable::Row>> {
        let condition_columns = crate::query::condition_columns(&query.conditions);
        self.with_authorized_read(
            table_name,
            None,
            true,
            |table, snapshot, allowed, principal| {
                let allowed_columns = self.select_column_ids_for(table_name, principal)?;
                self.require_columns_for(
                    table_name,
                    crate::auth::ColumnOperation::Select,
                    &condition_columns,
                    principal,
                )?;
                if let Some(projection) = projection {
                    self.require_columns_for(
                        table_name,
                        crate::auth::ColumnOperation::Select,
                        projection,
                        principal,
                    )?;
                }
                let mut rows = table.query_at_with_allowed(query, snapshot, allowed)?;
                let projection =
                    projection.map(|columns| columns.iter().copied().collect::<HashSet<_>>());
                for row in &mut rows {
                    row.columns.retain(|column, _| {
                        allowed_columns.contains(column)
                            && projection
                                .as_ref()
                                .map_or(true, |projection| projection.contains(column))
                    });
                }
                self.secure_rows_for(table_name, rows, principal)
            },
        )
    }

    /// Read one row with the database principal's row policy, column grants,
    /// and masks applied.
    pub fn get_for_current_principal(
        &self,
        table_name: &str,
        row_id: RowId,
    ) -> Result<Option<crate::memtable::Row>> {
        self.with_authorized_read(
            table_name,
            None,
            true,
            |table, snapshot, allowed, principal| {
                let allowed_columns = self.select_column_ids_for(table_name, principal)?;
                let Some(row) = table.get(row_id, snapshot) else {
                    return Ok(None);
                };
                if allowed.is_some_and(|allowed| !allowed.contains(&row.row_id)) {
                    return Ok(None);
                }
                let mut rows = self.secure_rows_for(table_name, vec![row], principal)?;
                if let Some(row) = rows.first_mut() {
                    row.columns
                        .retain(|column, _| allowed_columns.contains(column));
                }
                Ok(rows.pop())
            },
        )
    }

    /// Run exact ANN reranking over only rows authorized for this database
    /// handle. The embedding column still requires normal column access.
    pub fn ann_rerank_for_current_principal(
        &self,
        table_name: &str,
        request: &crate::query::AnnRerankRequest,
    ) -> Result<Vec<crate::query::AnnRerankHit>> {
        self.with_authorized_scored_read_context(
            table_name,
            None,
            true,
            None,
            |table, snapshot, authorization, principal| {
                self.require_columns_for(
                    table_name,
                    crate::auth::ColumnOperation::Select,
                    &[request.column_id],
                    principal,
                )?;
                table.ann_rerank_at_with_candidate_authorization_and_context(
                    request,
                    snapshot,
                    authorization,
                    None,
                )
            },
        )
    }

    /// Capture one table snapshot and the security version used to authorize it.
    /// The caller must validate the returned version before publishing results.
    pub fn authorized_read_snapshot(
        &self,
        table: &str,
        principal: Option<&crate::auth::Principal>,
    ) -> Result<AuthorizedReadSnapshot> {
        let (security, security_version, effective_principal) = {
            let catalog = self.catalog.read();
            (
                catalog.security.clone(),
                catalog.security_version,
                self.principal_for_authorized_read(&catalog, principal, false)?,
            )
        };
        let handle = self.table(table)?;
        let (table_snapshot, data_generation, allowed_row_ids) = {
            let table_handle = handle.lock();
            let table_snapshot = table_handle.snapshot();
            let data_generation = table_handle.data_generation();
            let allowed = self.allowed_row_ids_locked(
                table,
                &table_handle,
                table_snapshot,
                &security,
                security_version,
                effective_principal.as_ref(),
                None,
            )?;
            (
                table_snapshot,
                data_generation,
                allowed.map(|allowed| (*allowed).clone()),
            )
        };
        Ok(AuthorizedReadSnapshot {
            table: table.to_string(),
            table_snapshot,
            data_generation,
            security_version,
            allowed_row_ids,
        })
    }

    pub fn authorized_read_snapshot_valid(&self, snapshot: &AuthorizedReadSnapshot) -> bool {
        if self.catalog.read().security_version != snapshot.security_version {
            return false;
        }
        self.table(&snapshot.table)
            .ok()
            .is_some_and(|table| table.lock().data_generation() == snapshot.data_generation)
    }

    pub fn rls_cache_stats(&self) -> RlsCacheStats {
        self.rls_cache.lock().stats()
    }

    /// Read visible rows with column authorization, RLS, and masks applied.
    pub fn rows_for(
        &self,
        table: &str,
        principal: Option<&crate::auth::Principal>,
    ) -> Result<Vec<crate::memtable::Row>> {
        if principal.is_none() && self.principal.read().is_some() {
            self.refresh_principal()?;
        }
        let allowed = self.select_column_ids_for(table, principal)?;
        let handle = self.table(table)?;
        let rows = {
            let table = handle.lock();
            table.visible_rows(table.snapshot())?
        };
        let mut rows = self.secure_rows_for(table, rows, principal)?;
        for row in &mut rows {
            row.columns.retain(|column, _| allowed.contains(column));
        }
        Ok(rows)
    }

    /// Count rows visible to a principal without bypassing RLS.
    pub fn count_for(
        &self,
        table: &str,
        principal: Option<&crate::auth::Principal>,
    ) -> Result<u64> {
        if principal.is_none() && self.principal.read().is_some() {
            self.refresh_principal()?;
        }
        self.select_column_ids_for(table, principal)?;
        if self.security_active_for(table) {
            Ok(self.rows_for(table, principal)?.len() as u64)
        } else {
            Ok(self.table(table)?.lock().count())
        }
    }

    /// Authorize and write one native-API row for an explicit principal.
    pub fn put_for(
        &self,
        table: &str,
        mut cells: Vec<(u16, crate::memtable::Value)>,
        principal: Option<&crate::auth::Principal>,
    ) -> Result<RowId> {
        let columns = cells.iter().map(|(column, _)| *column).collect::<Vec<_>>();
        self.require_columns_for(
            table,
            crate::auth::ColumnOperation::Insert,
            &columns,
            principal,
        )?;
        let handle = self.table(table)?;
        let mut table_handle = handle.lock();
        table_handle.fill_auto_inc(&mut cells)?;
        table_handle.apply_defaults(&mut cells)?;
        let mut row = crate::memtable::Row::new(RowId(0), self.epoch.visible());
        row.columns.extend(cells.iter().cloned());
        self.check_row_policy_for(
            table,
            crate::security::PolicyCommand::Insert,
            &row,
            true,
            principal,
        )?;
        table_handle.put(cells)
    }

    pub fn check_row_policy_for(
        &self,
        table: &str,
        command: crate::security::PolicyCommand,
        row: &crate::memtable::Row,
        check_new: bool,
        principal: Option<&crate::auth::Principal>,
    ) -> Result<()> {
        let security = self.catalog.read().security.clone();
        if !security.rls_enabled(table) {
            return Ok(());
        }
        let cached = self.principal.read().clone();
        let principal = principal
            .or(cached.as_ref())
            .ok_or(MongrelError::AuthRequired)?;
        if security.row_allowed(table, command, row, principal, check_new) {
            return Ok(());
        }
        let required = match command {
            crate::security::PolicyCommand::Insert => crate::auth::Permission::Insert {
                table: table.to_string(),
            },
            crate::security::PolicyCommand::Update => crate::auth::Permission::Update {
                table: table.to_string(),
            },
            crate::security::PolicyCommand::Select => crate::auth::Permission::Select {
                table: table.to_string(),
            },
            crate::security::PolicyCommand::Delete | crate::security::PolicyCommand::All => {
                crate::auth::Permission::Delete {
                    table: table.to_string(),
                }
            }
        };
        Err(MongrelError::PermissionDenied {
            required,
            principal: principal.username.clone(),
        })
    }

    /// Durably create or replace a materialized-view definition after its
    /// physical table has been populated.
    pub fn set_materialized_view(
        &self,
        definition: crate::catalog::MaterializedViewEntry,
    ) -> Result<()> {
        use crate::wal::DdlOp;
        use std::sync::atomic::Ordering;

        self.require(&crate::auth::Permission::Ddl)?;
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }
        if definition.name.is_empty() || definition.query.trim().is_empty() {
            return Err(MongrelError::InvalidArgument(
                "materialized view name and query must not be empty".into(),
            ));
        }

        let _ddl = self.ddl_lock.lock();
        let table_id = self
            .catalog
            .read()
            .live(&definition.name)
            .ok_or_else(|| {
                MongrelError::NotFound(format!(
                    "materialized view table {:?} not found",
                    definition.name
                ))
            })?
            .table_id;
        let definition_json = DdlOp::encode_materialized_view(&definition)?;
        let _commit = self.commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let txn_id = self.alloc_txn_id();
        let commit_seq = {
            let mut wal = self.shared_wal.lock();
            wal.append(
                txn_id,
                table_id,
                crate::wal::Op::Ddl(DdlOp::SetMaterializedView {
                    name: definition.name.clone(),
                    definition_json,
                }),
            )?;
            wal.append_commit(txn_id, epoch, &[])?
        };
        self.group
            .await_durable(&self.shared_wal, commit_seq)
            .inspect_err(|_| {
                self.poisoned.store(true, Ordering::Relaxed);
            })?;

        {
            let mut catalog = self.catalog.write();
            if let Some(existing) = catalog
                .materialized_views
                .iter_mut()
                .find(|existing| existing.name == definition.name)
            {
                *existing = definition;
            } else {
                catalog.materialized_views.push(definition);
            }
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.epoch.publish_in_order(epoch);
        epoch_guard.disarm();
        Ok(())
    }

    /// The filesystem root this database was opened/created at.
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn is_read_only_replica(&self) -> bool {
        self.read_only
    }

    pub fn set_replication_wal_retention_segments(&self, segments: usize) {
        self.replication_wal_retention_segments
            .store(segments, std::sync::atomic::Ordering::Relaxed);
    }

    /// Capture a consistent bootstrap image. DDL, transaction spill/publish,
    /// direct table commits, compaction, and WAL append are quiesced while the
    /// file image is read. WAL records newer than manifests remain sufficient
    /// for recovery, so no flush or compaction is required.
    pub fn replication_snapshot(&self) -> Result<crate::replication::ReplicationSnapshot> {
        let _barrier = self.replication_barrier.write();
        let _ddl = self.ddl_lock.lock();
        let mut handles: Vec<_> = self
            .tables
            .read()
            .iter()
            .map(|(id, handle)| (*id, Arc::clone(handle)))
            .collect();
        handles.sort_by_key(|(id, _)| *id);
        let _table_guards: Vec<_> = handles.iter().map(|(_, handle)| handle.lock()).collect();
        let _commit = self.commit_lock.lock();
        let mut wal = self.shared_wal.lock();
        wal.group_sync()?;
        let wal_dek = crate::encryption::wal_dek_for(self.kek.as_deref());
        let records = crate::wal::SharedWal::replay_with_dek(&self.root, wal_dek.as_ref())?;
        let epoch = records
            .iter()
            .filter_map(|record| match &record.op {
                crate::wal::Op::TxnCommit { epoch, .. } => Some(*epoch),
                _ => None,
            })
            .max()
            .unwrap_or(0)
            .max(self.visible_epoch().0);
        let files = crate::replication::capture_files(&self.root)?;
        drop(wal);
        Ok(crate::replication::ReplicationSnapshot::new(epoch, files))
    }

    /// Create an online, directly-openable backup at `destination`.
    ///
    /// The short boundary phase quiesces commits/DDL, syncs the WAL, copies
    /// mutable metadata, and pins the exact immutable runs named by the copied
    /// manifests. Writers resume while those runs stream into a sibling staging
    /// directory. A checksummed backup manifest is written last, then the stage
    /// is atomically renamed into place.
    pub fn hot_backup(&self, destination: impl AsRef<Path>) -> Result<crate::backup::BackupReport> {
        self.require(&crate::auth::Permission::Ddl)?;
        let (destination, parent, stage) =
            prepare_backup_destination(&self.root, destination.as_ref())?;
        std::fs::create_dir(&stage)?;

        let outcome = (|| {
            let barrier = self.replication_barrier.write();
            let ddl = self.ddl_lock.lock();
            let mut handles: Vec<_> = self
                .tables
                .read()
                .iter()
                .map(|(id, handle)| (*id, Arc::clone(handle)))
                .collect();
            handles.sort_by_key(|(id, _)| *id);
            let table_guards: Vec<_> = handles.iter().map(|(_, handle)| handle.lock()).collect();
            let commit = self.commit_lock.lock();
            let mut wal = self.shared_wal.lock();
            wal.group_sync()?;
            let epoch = self.visible_epoch().0;

            let pin_nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let file_pin_root = self
                .root
                .join(META_DIR)
                .join("backup-pins")
                .join(format!("{}-{pin_nonce}", std::process::id()));
            std::fs::create_dir_all(&file_pin_root)?;
            let _file_pins = BackupFilePins {
                root: file_pin_root.clone(),
            };
            let mut run_files = Vec::new();
            for (index, (table_id, _)) in handles.iter().enumerate() {
                let table = &table_guards[index];
                for run in table.run_refs() {
                    let source = table.runs_dir().join(format!("r-{}.sr", run.run_id));
                    let relative = source
                        .strip_prefix(&self.root)
                        .map_err(|error| MongrelError::Other(format!("backup run path: {error}")))?
                        .to_path_buf();
                    let pinned = file_pin_root.join(format!("{table_id}-{}.sr", run.run_id));
                    if std::fs::hard_link(&source, &pinned).is_err() {
                        crate::backup::copy_file_synced(&source, &pinned)?;
                    }
                    run_files.push(((*table_id, run.run_id), pinned, relative));
                }
            }
            std::fs::File::open(&file_pin_root)?.sync_all()?;
            let run_keys: Vec<_> = run_files.iter().map(|(key, _, _)| *key).collect();
            {
                let mut pins = self.backup_pins.lock();
                for key in &run_keys {
                    *pins.entry(*key).or_insert(0) += 1;
                }
            }
            let _run_pins = BackupRunPins {
                pins: &self.backup_pins,
                runs: run_keys,
            };
            let deferred: HashSet<_> = run_files
                .iter()
                .map(|(_, _, relative)| relative.clone())
                .collect();
            let mut copied = Vec::new();
            copy_backup_boundary(&self.root, &stage, &deferred, &mut copied)?;

            drop(wal);
            drop(commit);
            drop(table_guards);
            drop(ddl);
            drop(barrier);

            if let Some(hook) = self.backup_hook.lock().as_ref() {
                hook();
            }
            for (_, source, relative) in run_files {
                crate::backup::copy_file_synced(&source, &stage.join(&relative))?;
                copied.push(relative);
            }

            let manifest = crate::backup::BackupManifest::create(&stage, epoch, &copied)?;
            manifest.write(&stage)?;
            crate::backup::sync_directories(&stage)?;
            if destination.exists() {
                return Err(MongrelError::Conflict(format!(
                    "backup destination already exists: {}",
                    destination.display()
                )));
            }
            std::fs::rename(&stage, &destination)?;
            std::fs::File::open(&parent)?.sync_all()?;
            Ok(crate::backup::BackupReport {
                destination,
                epoch,
                files: manifest.files.len(),
                bytes: manifest.total_bytes(),
            })
        })();

        if outcome.is_err() && stage.exists() {
            let _ = std::fs::remove_dir_all(&stage);
        }
        outcome
    }

    /// Return complete committed transactions after `since_epoch`. A gap or a
    /// transaction backed by a spilled run requires a fresh bootstrap image.
    pub fn replication_batch_since(
        &self,
        since_epoch: u64,
    ) -> Result<crate::replication::ReplicationBatch> {
        use crate::wal::Op;

        let mut wal = self.shared_wal.lock();
        wal.group_sync()?;
        let wal_dek = crate::encryption::wal_dek_for(self.kek.as_deref());
        let records = crate::wal::SharedWal::replay_with_dek(&self.root, wal_dek.as_ref())?;
        drop(wal);

        let commits: HashMap<u64, u64> = records
            .iter()
            .filter_map(|record| match &record.op {
                Op::TxnCommit { epoch, .. } => Some((record.txn_id, *epoch)),
                _ => None,
            })
            .collect();
        let earliest_epoch = commits.values().copied().min();
        let current_epoch = commits
            .values()
            .copied()
            .max()
            .unwrap_or(0)
            .max(self.visible_epoch().0);
        let selected: HashSet<u64> = commits
            .iter()
            .filter_map(|(txn_id, epoch)| (*epoch > since_epoch).then_some(*txn_id))
            .collect();
        let retention_gap = since_epoch < current_epoch
            && earliest_epoch.map_or(true, |epoch| epoch > since_epoch.saturating_add(1));
        let spilled = records.iter().any(|record| {
            selected.contains(&record.txn_id)
                && matches!(
                    &record.op,
                    Op::TxnCommit { added_runs, .. } if !added_runs.is_empty()
                )
        });
        let records = records
            .into_iter()
            .filter(|record| record.txn_id != crate::wal::SYSTEM_TXN_ID)
            .filter(|record| selected.contains(&record.txn_id))
            .collect();
        Ok(crate::replication::ReplicationBatch {
            current_epoch,
            earliest_epoch,
            requires_snapshot: retention_gap || spilled,
            records,
        })
    }

    /// Durably append a leader batch to a follower's local WAL. The caller
    /// must drop and reopen this handle to run ordinary WAL recovery before it
    /// advances `_meta/repl_epoch`.
    pub fn append_replication_batch(&self, records: &[crate::wal::Record]) -> Result<u64> {
        use crate::wal::Op;

        if !self.read_only {
            return Err(MongrelError::InvalidArgument(
                "replication batches may only target a marked replica".into(),
            ));
        }
        let current = crate::replication::replica_epoch(&self.root)?;
        let mut commits = HashMap::new();
        let mut commit_timestamps = HashMap::new();
        for record in records {
            match &record.op {
                Op::TxnCommit { epoch, added_runs } => {
                    if !added_runs.is_empty() {
                        return Err(MongrelError::Conflict(
                            "replication snapshot required for spilled-run transaction".into(),
                        ));
                    }
                    if commits.insert(record.txn_id, *epoch).is_some() {
                        return Err(MongrelError::InvalidArgument(format!(
                            "duplicate commit for replication transaction {}",
                            record.txn_id
                        )));
                    }
                }
                Op::CommitTimestamp { unix_nanos } => {
                    commit_timestamps.insert(record.txn_id, *unix_nanos);
                }
                _ => {}
            }
        }
        for record in records {
            if record.txn_id != crate::wal::SYSTEM_TXN_ID
                && !matches!(&record.op, Op::TxnAbort)
                && !commits.contains_key(&record.txn_id)
            {
                return Err(MongrelError::InvalidArgument(format!(
                    "incomplete replication transaction {}",
                    record.txn_id
                )));
            }
        }
        let target_epoch = commits
            .values()
            .copied()
            .filter(|epoch| *epoch > current)
            .max()
            .unwrap_or(current);
        let mut selected: HashSet<u64> = commits
            .iter()
            .filter_map(|(txn_id, epoch)| (*epoch > current).then_some(*txn_id))
            .collect();
        if selected.is_empty() {
            return Ok(current);
        }
        let mut wal = self.shared_wal.lock();
        wal.group_sync()?;
        let wal_dek = crate::encryption::wal_dek_for(self.kek.as_deref());
        let existing: HashSet<(u64, u64)> =
            crate::wal::SharedWal::replay_with_dek(&self.root, wal_dek.as_ref())?
                .into_iter()
                .filter_map(|record| match record.op {
                    Op::TxnCommit { epoch, .. } => Some((record.txn_id, epoch)),
                    _ => None,
                })
                .collect();
        selected.retain(|txn_id| {
            commits
                .get(txn_id)
                .is_some_and(|epoch| !existing.contains(&(*txn_id, *epoch)))
        });
        for record in records {
            if !selected.contains(&record.txn_id) {
                continue;
            }
            match &record.op {
                Op::TxnCommit { epoch, added_runs } => {
                    let timestamp = commit_timestamps
                        .get(&record.txn_id)
                        .copied()
                        .unwrap_or_else(current_unix_nanos);
                    wal.append_commit_at(record.txn_id, Epoch(*epoch), added_runs, timestamp)?;
                }
                Op::TxnAbort | Op::Flush { .. } | Op::CommitTimestamp { .. } => {}
                op => {
                    wal.append(record.txn_id, 0, op.clone())?;
                }
            }
        }
        if !selected.is_empty() {
            wal.group_sync()?;
        }
        Ok(target_epoch)
    }

    /// Resolve a table name → id (live tables only). pub(crate) so the
    /// transaction layer can stage by name.
    pub fn table_id(&self, name: &str) -> Result<u64> {
        let cat = self.catalog.read();
        cat.live(name)
            .map(|e| e.table_id)
            .ok_or_else(|| MongrelError::NotFound(format!("table {name:?} not found")))
    }

    pub fn procedures(&self) -> Vec<StoredProcedure> {
        self.catalog
            .read()
            .procedures
            .iter()
            .map(|p| p.procedure.clone())
            .collect()
    }

    pub fn procedure(&self, name: &str) -> Option<StoredProcedure> {
        self.catalog
            .read()
            .procedures
            .iter()
            .find(|p| p.procedure.name == name)
            .map(|p| p.procedure.clone())
    }

    pub fn create_procedure(&self, mut procedure: StoredProcedure) -> Result<StoredProcedure> {
        self.require(&crate::auth::Permission::Ddl)?;
        let _g = self.ddl_lock.lock();
        procedure.validate()?;
        self.validate_procedure_references(&procedure)?;
        {
            let cat = self.catalog.read();
            if cat
                .procedures
                .iter()
                .any(|p| p.procedure.name == procedure.name)
            {
                return Err(MongrelError::InvalidArgument(format!(
                    "procedure {:?} already exists",
                    procedure.name
                )));
            }
        }
        let commit_lock = Arc::clone(&self.commit_lock);
        let _c = commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        procedure.created_epoch = epoch.0;
        procedure.updated_epoch = epoch.0;
        {
            let mut cat = self.catalog.write();
            cat.procedures.push(ProcedureEntry::from(procedure.clone()));
            cat.db_epoch = epoch.0;
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(procedure)
    }

    pub fn create_or_replace_procedure(
        &self,
        procedure: StoredProcedure,
    ) -> Result<StoredProcedure> {
        let _g = self.ddl_lock.lock();
        procedure.validate()?;
        self.validate_procedure_references(&procedure)?;
        let commit_lock = Arc::clone(&self.commit_lock);
        let _c = commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let replaced = {
            let mut cat = self.catalog.write();
            let next = match cat
                .procedures
                .iter()
                .position(|p| p.procedure.name == procedure.name)
            {
                Some(idx) => {
                    let next = cat.procedures[idx]
                        .procedure
                        .replaced(procedure.clone(), epoch.0)?;
                    cat.procedures[idx] = ProcedureEntry::from(next.clone());
                    next
                }
                None => {
                    let mut next = procedure;
                    next.created_epoch = epoch.0;
                    next.updated_epoch = epoch.0;
                    cat.procedures.push(ProcedureEntry::from(next.clone()));
                    next
                }
            };
            cat.db_epoch = epoch.0;
            next
        };
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(replaced)
    }

    pub fn drop_procedure(&self, name: &str) -> Result<()> {
        self.require(&crate::auth::Permission::Ddl)?;
        let _g = self.ddl_lock.lock();
        let commit_lock = Arc::clone(&self.commit_lock);
        let _c = commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        {
            let mut cat = self.catalog.write();
            let before = cat.procedures.len();
            cat.procedures.retain(|p| p.procedure.name != name);
            if cat.procedures.len() == before {
                return Err(MongrelError::NotFound(format!(
                    "procedure {name:?} not found"
                )));
            }
            cat.db_epoch = epoch.0;
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(())
    }

    // ── User / role / credentials management ─────────────────────────────

    /// List all catalog users (password hashes included — callers should not
    /// serialize them externally).
    pub fn users(&self) -> Vec<crate::auth::UserEntry> {
        self.catalog.read().users.clone()
    }

    /// List all catalog roles.
    pub fn roles(&self) -> Vec<crate::auth::RoleEntry> {
        self.catalog.read().roles.clone()
    }

    /// Create a new user with an Argon2id-hashed password.
    pub fn create_user(&self, username: &str, password: &str) -> Result<crate::auth::UserEntry> {
        self.require(&crate::auth::Permission::Admin)?;
        let hash = crate::auth::hash_password(password).map_err(MongrelError::Other)?;
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let id = {
            let mut cat = self.catalog.write();
            if cat.users.iter().any(|u| u.username == username) {
                return Err(MongrelError::InvalidArgument(format!(
                    "user {username:?} already exists"
                )));
            }
            cat.next_user_id += 1;
            let id = cat.next_user_id;
            let entry = crate::auth::UserEntry {
                id,
                username: username.into(),
                password_hash: hash,
                roles: Vec::new(),
                is_admin: false,
                created_epoch: epoch.0,
            };
            cat.users.push(entry.clone());
            cat.security_version = cat.security_version.wrapping_add(1);
            cat.db_epoch = epoch.0;
            entry
        };
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(id)
    }

    /// Drop a user by username.
    pub fn drop_user(&self, username: &str) -> Result<()> {
        self.require(&crate::auth::Permission::Admin)?;
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        {
            let mut cat = self.catalog.write();
            let before = cat.users.len();
            cat.users.retain(|u| u.username != username);
            if cat.users.len() == before {
                return Err(MongrelError::NotFound(format!(
                    "user {username:?} not found"
                )));
            }
            cat.security_version = cat.security_version.wrapping_add(1);
            cat.db_epoch = epoch.0;
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(())
    }

    /// Change a user's password.
    pub fn alter_user_password(&self, username: &str, new_password: &str) -> Result<()> {
        self.require(&crate::auth::Permission::Admin)?;
        let hash = crate::auth::hash_password(new_password).map_err(MongrelError::Other)?;
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        {
            let mut cat = self.catalog.write();
            let user = cat
                .users
                .iter_mut()
                .find(|u| u.username == username)
                .ok_or_else(|| MongrelError::NotFound(format!("user {username:?} not found")))?;
            user.password_hash = hash;
            cat.security_version = cat.security_version.wrapping_add(1);
            cat.db_epoch = epoch.0;
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(())
    }

    /// Verify credentials. Returns `Some(entry)` on success, `None` on
    /// mismatch, `Err` on engine error.
    pub fn verify_user(
        &self,
        username: &str,
        password: &str,
    ) -> Result<Option<crate::auth::UserEntry>> {
        let cat = self.catalog.read();
        let Some(user) = cat.users.iter().find(|u| u.username == username) else {
            return Ok(None);
        };
        if user.password_hash.is_empty() {
            return Ok(None);
        }
        let ok = crate::auth::verify_password(password, &user.password_hash)
            .map_err(MongrelError::Other)?;
        if ok {
            Ok(Some(user.clone()))
        } else {
            Ok(None)
        }
    }

    /// Grant admin privileges to a user (bypasses all permission checks).
    pub fn set_user_admin(&self, username: &str, is_admin: bool) -> Result<()> {
        self.require(&crate::auth::Permission::Admin)?;
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        {
            let mut cat = self.catalog.write();
            let user = cat
                .users
                .iter_mut()
                .find(|u| u.username == username)
                .ok_or_else(|| MongrelError::NotFound(format!("user {username:?} not found")))?;
            user.is_admin = is_admin;
            cat.security_version = cat.security_version.wrapping_add(1);
            cat.db_epoch = epoch.0;
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(())
    }

    /// Create a new role.
    pub fn create_role(&self, name: &str) -> Result<crate::auth::RoleEntry> {
        self.require(&crate::auth::Permission::Admin)?;
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let entry = {
            let mut cat = self.catalog.write();
            if cat.roles.iter().any(|r| r.name == name) {
                return Err(MongrelError::InvalidArgument(format!(
                    "role {name:?} already exists"
                )));
            }
            let entry = crate::auth::RoleEntry {
                name: name.into(),
                permissions: Vec::new(),
                created_epoch: epoch.0,
            };
            cat.roles.push(entry.clone());
            cat.security_version = cat.security_version.wrapping_add(1);
            cat.db_epoch = epoch.0;
            entry
        };
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(entry)
    }

    /// Drop a role by name.
    pub fn drop_role(&self, name: &str) -> Result<()> {
        self.require(&crate::auth::Permission::Admin)?;
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        {
            let mut cat = self.catalog.write();
            let before = cat.roles.len();
            cat.roles.retain(|r| r.name != name);
            if cat.roles.len() == before {
                return Err(MongrelError::NotFound(format!("role {name:?} not found")));
            }
            // Remove the role from all users.
            for user in &mut cat.users {
                user.roles.retain(|r| r != name);
            }
            cat.security_version = cat.security_version.wrapping_add(1);
            cat.db_epoch = epoch.0;
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(())
    }

    /// Grant a role to a user.
    pub fn grant_role(&self, username: &str, role_name: &str) -> Result<()> {
        self.require(&crate::auth::Permission::Admin)?;
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        {
            let mut cat = self.catalog.write();
            if !cat.roles.iter().any(|r| r.name == role_name) {
                return Err(MongrelError::NotFound(format!(
                    "role {role_name:?} not found"
                )));
            }
            let user = cat
                .users
                .iter_mut()
                .find(|u| u.username == username)
                .ok_or_else(|| MongrelError::NotFound(format!("user {username:?} not found")))?;
            if !user.roles.contains(&role_name.to_string()) {
                user.roles.push(role_name.into());
            }
            cat.security_version = cat.security_version.wrapping_add(1);
            cat.db_epoch = epoch.0;
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(())
    }

    /// Revoke a role from a user.
    pub fn revoke_role(&self, username: &str, role_name: &str) -> Result<()> {
        self.require(&crate::auth::Permission::Admin)?;
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        {
            let mut cat = self.catalog.write();
            let user = cat
                .users
                .iter_mut()
                .find(|u| u.username == username)
                .ok_or_else(|| MongrelError::NotFound(format!("user {username:?} not found")))?;
            user.roles.retain(|r| r != role_name);
            cat.security_version = cat.security_version.wrapping_add(1);
            cat.db_epoch = epoch.0;
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(())
    }

    /// Grant a permission to a role.
    pub fn grant_permission(
        &self,
        role_name: &str,
        permission: crate::auth::Permission,
    ) -> Result<()> {
        self.require(&crate::auth::Permission::Admin)?;
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        {
            let mut cat = self.catalog.write();
            let role = cat
                .roles
                .iter_mut()
                .find(|r| r.name == role_name)
                .ok_or_else(|| MongrelError::NotFound(format!("role {role_name:?} not found")))?;
            merge_permission(&mut role.permissions, permission);
            cat.security_version = cat.security_version.wrapping_add(1);
            cat.db_epoch = epoch.0;
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(())
    }

    /// Revoke a permission from a role.
    pub fn revoke_permission(
        &self,
        role_name: &str,
        permission: crate::auth::Permission,
    ) -> Result<()> {
        self.require(&crate::auth::Permission::Admin)?;
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        {
            let mut cat = self.catalog.write();
            let role = cat
                .roles
                .iter_mut()
                .find(|r| r.name == role_name)
                .ok_or_else(|| MongrelError::NotFound(format!("role {role_name:?} not found")))?;
            revoke_permission_from(&mut role.permissions, &permission);
            cat.security_version = cat.security_version.wrapping_add(1);
            cat.db_epoch = epoch.0;
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(())
    }

    /// Resolve a user into a [`crate::auth::Principal`] by collecting all
    /// permissions from their roles. Returns `None` if the user doesn't exist.
    pub fn resolve_principal(&self, username: &str) -> Option<crate::auth::Principal> {
        let cat = self.catalog.read();
        Self::resolve_principal_from_catalog(&cat, username)
    }

    /// Resolve a username to a [`Principal`] directly from a catalog snapshot,
    /// without needing a constructed `Database`. Used by the credentialed open
    /// path (which must verify credentials before the `Database` exists) and
    /// by [`resolve_principal`](Self::resolve_principal).
    fn resolve_principal_from_catalog(
        cat: &Catalog,
        username: &str,
    ) -> Option<crate::auth::Principal> {
        let user = cat.users.iter().find(|u| u.username == username)?;
        let mut permissions = Vec::new();
        for role_name in &user.roles {
            if let Some(role) = cat.roles.iter().find(|r| &r.name == role_name) {
                permissions.extend(role.permissions.iter().cloned());
            }
        }
        Some(crate::auth::Principal {
            username: user.username.clone(),
            is_admin: user.is_admin,
            roles: user.roles.clone(),
            permissions,
        })
    }

    /// Check whether a user has a specific permission (via their roles).
    pub fn check_permission(&self, username: &str, permission: &crate::auth::Permission) -> bool {
        match self.resolve_principal(username) {
            Some(p) => p.has_permission(permission),
            None => false,
        }
    }

    /// Returns `true` if this database's catalog has `require_auth = true`.
    /// When true, every operation consults the cached [`Principal`] via
    /// [`require`](Self::require).
    pub fn require_auth_enabled(&self) -> bool {
        self.catalog.read().require_auth
    }

    /// A snapshot of the cached principal for this handle, if any. `None` for
    /// databases opened without credentials (the default). Returns a clone
    /// because the principal lives behind an `RwLock`.
    pub fn principal(&self) -> Option<crate::auth::Principal> {
        self.principal.read().clone()
    }

    /// Build a `TableAuthChecker` from the current auth state. Used when
    /// mounting a new table (`create_table`) so the table inherits the
    /// database's enforcement configuration. The checker reads the live
    /// `require_auth` flag and cached principal, so changes via `enable_auth`
    /// / `refresh_principal` propagate to already-mounted tables.
    fn table_auth_checker(&self) -> Option<Arc<dyn crate::auth_state::TableAuthChecker>> {
        Some(Arc::new(crate::auth_state::DefaultTableAuthChecker::new(
            self.auth_state.clone(),
        )))
    }

    /// Re-resolve the cached principal from the on-disk catalog. Long-lived
    /// handles (e.g. a daemon) call this after a `REVOKE` or role change —
    /// possibly made by a different handle to the same database — to pick up
    /// the new effective permissions without re-verifying the password.
    ///
    /// This reloads the catalog from disk first, so changes committed by other
    /// handles (or other processes) are visible. The username is taken from
    /// the existing cached principal; if the user has since been dropped,
    /// returns [`MongrelError::InvalidCredentials`].
    ///
    /// No-op (returns `Ok(())`) on a credentialless database, or on a
    /// credentialed database whose cached principal is `None`.
    pub fn refresh_principal(&self) -> Result<()> {
        let username = match self.principal.read().clone() {
            Some(p) => p.username,
            None => return Ok(()),
        };
        // Reload the catalog from disk so role/permission changes made by
        // other handles (or processes) are reflected. The in-memory catalog
        // is only updated by mutations on *this* handle.
        let cat = catalog::read(&self.root, self.meta_dek.as_ref())?
            .ok_or_else(|| MongrelError::NotFound("catalog vanished during refresh".into()))?;
        // Swap in the reloaded catalog so subsequent operations on this handle
        // also see the updated permissions/roles.
        *self.catalog.write() = cat.clone();
        match Self::resolve_principal_from_catalog(&cat, &username) {
            Some(p) => {
                *self.principal.write() = Some(p.clone());
                // Update the shared auth state so mounted Tables see the new
                // permissions immediately (Tables read from AuthState, not from
                // self.principal).
                self.auth_state.set_principal(Some(p));
                Ok(())
            }
            None => Err(MongrelError::InvalidCredentials { username }),
        }
    }

    /// Convert a credentialless database to a credentialed one: create the
    /// first admin user, set `require_auth = true`, and cache the admin
    /// principal on this handle so subsequent operations on the same handle
    /// continue to work. After this call, the database can only be reopened
    /// via `open_with_credentials` / `open_encrypted_with_credentials`.
    ///
    /// Refuses if the database already has `require_auth = true`. This is
    /// the conversion path for existing databases; for fresh databases,
    /// `create_with_credentials` sets everything up atomically.
    ///
    /// See `docs/15-credential-enforcement.md`.
    pub fn enable_auth(&self, admin_username: &str, admin_password: &str) -> Result<()> {
        let password_hash =
            crate::auth::hash_password(admin_password).map_err(MongrelError::Other)?;
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        {
            let mut cat = self.catalog.write();
            if cat.require_auth {
                return Err(MongrelError::InvalidArgument(
                    "database already has require_auth enabled".into(),
                ));
            }
            // Reject a duplicate username so the bootstrap doesn't silently
            // shadow an existing user.
            if cat.users.iter().any(|u| u.username == admin_username) {
                return Err(MongrelError::InvalidArgument(format!(
                    "user {admin_username:?} already exists"
                )));
            }
            cat.next_user_id = cat.next_user_id.max(1);
            let id = cat.next_user_id;
            cat.next_user_id += 1;
            cat.users.push(crate::auth::UserEntry {
                id,
                username: admin_username.to_string(),
                password_hash,
                roles: Vec::new(),
                is_admin: true,
                created_epoch: epoch.0,
            });
            cat.require_auth = true;
            cat.security_version = cat.security_version.wrapping_add(1);
            cat.db_epoch = epoch.0;
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        // Cache the admin principal on this handle + update the shared auth
        // state so mounted tables start enforcing immediately.
        *self.principal.write() = Some(crate::auth::Principal {
            username: admin_username.to_string(),
            is_admin: true,
            roles: Vec::new(),
            permissions: Vec::new(),
        });
        self.auth_state.set_require_auth(true);
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(())
    }

    /// Disable `require_auth` on this database, reverting it to credentialless
    /// mode. This is the **recovery** path — it requires the handle to already
    /// be open (and therefore already authenticated if `require_auth` was on).
    ///
    /// After this call, the database can be reopened with plain
    /// [`open`](Self::open) / [`open_encrypted`](Self::open_encrypted) without
    /// credentials. All existing users and roles are preserved in the catalog
    /// (so `require_auth` can be re-enabled without recreating them), but they
    /// are no longer consulted for enforcement.
    ///
    /// For true **offline** recovery (when credentials are lost and no
    /// authenticated handle is available), the caller opens the database
    /// directly via the catalog file (filesystem access required) and calls
    /// this method — see the CLI's `auth disable-offline` command.
    ///
    /// See `docs/15-credential-enforcement.md` §4.7.
    pub fn disable_auth(&self) -> Result<()> {
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        {
            let mut cat = self.catalog.write();
            if !cat.require_auth {
                return Err(MongrelError::InvalidArgument(
                    "database does not have require_auth enabled".into(),
                ));
            }
            cat.require_auth = false;
            cat.security_version = cat.security_version.wrapping_add(1);
            cat.db_epoch = epoch.0;
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        // Clear the cached principal — enforcement is now off.
        *self.principal.write() = None;
        // Update the shared auth state so mounted tables also stop enforcing.
        self.auth_state.set_require_auth(false);
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(())
    }

    /// Enforcement check: if the catalog has `require_auth = true`, verify
    /// that the cached principal satisfies `perm`. Called by every
    /// enforcement point (DDL, admin, maintenance, and — in Phase 2 —
    /// Table/Transaction/MongrelSession operations).
    ///
    /// On a credentialless database this is a no-op (`Ok(())`).
    pub fn require(&self, perm: &crate::auth::Permission) -> Result<()> {
        if self.read_only && !matches!(perm, crate::auth::Permission::Select { .. }) {
            return Err(MongrelError::ReadOnlyReplica);
        }
        if !self.catalog.read().require_auth {
            return Ok(());
        }
        let guard = self.principal.read();
        let p = guard.as_ref().ok_or(MongrelError::AuthRequired)?;
        if p.has_permission(perm) {
            Ok(())
        } else {
            Err(MongrelError::PermissionDenied {
                required: perm.clone(),
                principal: p.username.clone(),
            })
        }
    }

    /// Convenience: enforce a table-level permission (`Select`/`Insert`/
    /// `Update`/`Delete`) by table name. Used by the Transaction layer and
    /// other callers that know the operation kind + table name but don't want
    /// to construct the full `Permission` enum value themselves.
    pub fn require_table(
        &self,
        table: &str,
        perm: crate::auth_state::RequiredPermission,
    ) -> Result<()> {
        self.require(&perm.into_permission(table))
    }

    pub fn triggers(&self) -> Vec<StoredTrigger> {
        self.catalog
            .read()
            .triggers
            .iter()
            .map(|t| t.trigger.clone())
            .collect()
    }

    pub fn trigger(&self, name: &str) -> Option<StoredTrigger> {
        self.catalog
            .read()
            .triggers
            .iter()
            .find(|t| t.trigger.name == name)
            .map(|t| t.trigger.clone())
    }

    pub fn create_trigger(&self, mut trigger: StoredTrigger) -> Result<StoredTrigger> {
        self.require(&crate::auth::Permission::Ddl)?;
        let _g = self.ddl_lock.lock();
        trigger.validate()?;
        self.validate_trigger_references(&trigger)?;
        {
            let cat = self.catalog.read();
            if cat.triggers.iter().any(|t| t.trigger.name == trigger.name) {
                return Err(MongrelError::InvalidArgument(format!(
                    "trigger {:?} already exists",
                    trigger.name
                )));
            }
        }
        let commit_lock = Arc::clone(&self.commit_lock);
        let _c = commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        trigger.created_epoch = epoch.0;
        trigger.updated_epoch = epoch.0;
        {
            let mut cat = self.catalog.write();
            cat.triggers.push(TriggerEntry::from(trigger.clone()));
            cat.db_epoch = epoch.0;
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(trigger)
    }

    pub fn create_or_replace_trigger(&self, trigger: StoredTrigger) -> Result<StoredTrigger> {
        let _g = self.ddl_lock.lock();
        trigger.validate()?;
        self.validate_trigger_references(&trigger)?;
        let commit_lock = Arc::clone(&self.commit_lock);
        let _c = commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let replaced = {
            let mut cat = self.catalog.write();
            let next = match cat
                .triggers
                .iter()
                .position(|t| t.trigger.name == trigger.name)
            {
                Some(idx) => {
                    let next = cat.triggers[idx]
                        .trigger
                        .replaced(trigger.clone(), epoch.0)?;
                    cat.triggers[idx] = TriggerEntry::from(next.clone());
                    next
                }
                None => {
                    let mut next = trigger;
                    next.created_epoch = epoch.0;
                    next.updated_epoch = epoch.0;
                    cat.triggers.push(TriggerEntry::from(next.clone()));
                    next
                }
            };
            cat.db_epoch = epoch.0;
            next
        };
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(replaced)
    }

    pub fn drop_trigger(&self, name: &str) -> Result<()> {
        self.require(&crate::auth::Permission::Ddl)?;
        let _g = self.ddl_lock.lock();
        let commit_lock = Arc::clone(&self.commit_lock);
        let _c = commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        {
            let mut cat = self.catalog.write();
            let before = cat.triggers.len();
            cat.triggers.retain(|t| t.trigger.name != name);
            if cat.triggers.len() == before {
                return Err(MongrelError::NotFound(format!(
                    "trigger {name:?} not found"
                )));
            }
            cat.db_epoch = epoch.0;
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(())
    }

    pub fn external_tables(&self) -> Vec<ExternalTableEntry> {
        self.catalog.read().external_tables.clone()
    }

    pub fn external_table(&self, name: &str) -> Option<ExternalTableEntry> {
        self.catalog
            .read()
            .external_tables
            .iter()
            .find(|t| t.name == name)
            .cloned()
    }

    pub fn create_external_table(
        &self,
        mut entry: ExternalTableEntry,
    ) -> Result<ExternalTableEntry> {
        self.require(&crate::auth::Permission::Ddl)?;
        let _g = self.ddl_lock.lock();
        entry.validate()?;
        {
            let cat = self.catalog.read();
            if cat.live(&entry.name).is_some()
                || cat.external_tables.iter().any(|t| t.name == entry.name)
            {
                return Err(MongrelError::InvalidArgument(format!(
                    "table {:?} already exists",
                    entry.name
                )));
            }
        }
        let commit_lock = Arc::clone(&self.commit_lock);
        let _c = commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        entry.created_epoch = epoch.0;
        {
            let mut cat = self.catalog.write();
            cat.external_tables.push(entry.clone());
            cat.db_epoch = epoch.0;
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(entry)
    }

    pub fn drop_external_table(&self, name: &str) -> Result<()> {
        self.require(&crate::auth::Permission::Ddl)?;
        let _g = self.ddl_lock.lock();
        let commit_lock = Arc::clone(&self.commit_lock);
        let _c = commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        {
            let mut cat = self.catalog.write();
            let before = cat.external_tables.len();
            cat.external_tables.retain(|t| t.name != name);
            if cat.external_tables.len() == before {
                return Err(MongrelError::NotFound(format!(
                    "external table {name:?} not found"
                )));
            }
            cat.db_epoch = epoch.0;
        }
        let state_dir = self.root.join(VTAB_DIR).join(name);
        if state_dir.exists() {
            std::fs::remove_dir_all(state_dir)?;
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(())
    }

    pub fn commit_external_table_state(&self, name: &str, state: &[u8]) -> Result<Epoch> {
        let txn_id = self.alloc_txn_id();
        self.commit_transaction_with_external_states(
            txn_id,
            self.epoch.visible(),
            Vec::new(),
            vec![(name.to_string(), state.to_vec())],
            Vec::new(),
            None,
            None,
        )
    }

    pub fn trigger_config(&self) -> TriggerConfig {
        use std::sync::atomic::Ordering;
        TriggerConfig {
            recursive_triggers: self.trigger_recursive.load(Ordering::Relaxed),
            max_depth: self.trigger_max_depth.load(Ordering::Relaxed),
            max_loop_iterations: self.trigger_max_loop_iterations.load(Ordering::Relaxed),
        }
    }

    pub fn set_trigger_config(&self, config: TriggerConfig) -> Result<()> {
        use std::sync::atomic::Ordering;
        if config.max_depth == 0 {
            return Err(MongrelError::InvalidArgument(
                "trigger max_depth must be greater than 0".into(),
            ));
        }
        self.trigger_recursive
            .store(config.recursive_triggers, Ordering::Relaxed);
        self.trigger_max_depth
            .store(config.max_depth, Ordering::Relaxed);
        self.trigger_max_loop_iterations
            .store(config.max_loop_iterations, Ordering::Relaxed);
        Ok(())
    }

    pub fn set_recursive_triggers(&self, recursive: bool) {
        use std::sync::atomic::Ordering;
        self.trigger_recursive.store(recursive, Ordering::Relaxed);
    }

    /// Subscribe to ephemeral SQL NOTIFY messages. Durable row changes use
    /// [`Self::change_events_since`], with [`Self::subscribe_change_commits`]
    /// as a low-latency wake-up.
    pub fn subscribe_changes(&self) -> tokio::sync::broadcast::Receiver<ChangeEvent> {
        self.notify.subscribe()
    }

    pub fn subscribe_change_commits(&self) -> tokio::sync::broadcast::Receiver<()> {
        self.change_wake.subscribe()
    }

    /// Reconstruct committed row changes from the retained shared WAL. Event
    /// ids are stable `<commit_epoch>:<operation_index>` pairs. A caller that
    /// resumes before the oldest retained commit receives `gap = true` and
    /// must rebootstrap instead of silently skipping changes.
    pub fn change_events_since(&self, last_event_id: Option<&str>) -> Result<CdcBatch> {
        use crate::wal::Op;

        let resume = match last_event_id {
            Some(id) => {
                let (epoch, index) = id.split_once(':').ok_or_else(|| {
                    MongrelError::InvalidArgument(format!(
                        "invalid CDC event id {id:?}; expected <epoch>:<index>"
                    ))
                })?;
                Some((
                    epoch.parse::<u64>().map_err(|error| {
                        MongrelError::InvalidArgument(format!("invalid CDC epoch: {error}"))
                    })?,
                    index.parse::<u32>().map_err(|error| {
                        MongrelError::InvalidArgument(format!("invalid CDC index: {error}"))
                    })?,
                ))
            }
            None => None,
        };

        let mut wal = self.shared_wal.lock();
        wal.group_sync()?;
        let wal_dek = crate::encryption::wal_dek_for(self.kek.as_deref());
        let records = crate::wal::SharedWal::replay_with_dek(&self.root, wal_dek.as_ref())?;
        drop(wal);

        let commits: HashMap<u64, (u64, Vec<crate::wal::AddedRun>)> = records
            .iter()
            .filter_map(|record| match &record.op {
                Op::TxnCommit { epoch, added_runs } => {
                    Some((record.txn_id, (*epoch, added_runs.clone())))
                }
                _ => None,
            })
            .collect();
        let earliest_epoch = commits.values().map(|(epoch, _)| *epoch).min();
        let current_epoch = self.epoch.visible().0;
        let gap = resume.is_some_and(|(epoch, _)| {
            epoch < current_epoch
                && earliest_epoch.map_or(true, |earliest| earliest > epoch.saturating_add(1))
        });
        if gap {
            return Ok(CdcBatch {
                events: Vec::new(),
                current_epoch,
                earliest_epoch,
                gap: true,
            });
        }

        let table_names: HashMap<u64, String> = self
            .catalog
            .read()
            .tables
            .iter()
            .map(|entry| (entry.table_id, entry.name.clone()))
            .collect();
        let before_images: HashMap<(u64, u64, u64), crate::memtable::Row> = records
            .iter()
            .filter_map(|record| {
                if !commits.contains_key(&record.txn_id) {
                    return None;
                }
                let Op::BeforeImage {
                    table_id,
                    row_id,
                    row,
                } = &record.op
                else {
                    return None;
                };
                bincode::deserialize(row)
                    .ok()
                    .map(|before| ((record.txn_id, *table_id, row_id.0), before))
            })
            .collect();
        let mut operation_indices: HashMap<u64, u32> = HashMap::new();
        let mut events = Vec::new();
        for record in &records {
            let Some((commit_epoch, _)) = commits.get(&record.txn_id) else {
                continue;
            };
            let event = match &record.op {
                Op::Put { table_id, rows } => {
                    let rows: Vec<crate::memtable::Row> = bincode::deserialize(rows)?;
                    let data = serde_json::to_value(rows)
                        .map_err(|error| MongrelError::Other(format!("CDC JSON: {error}")))?;
                    Some((*table_id, "put", data))
                }
                Op::Delete { table_id, row_ids } => {
                    let before = row_ids
                        .iter()
                        .filter_map(|row_id| {
                            before_images
                                .get(&(record.txn_id, *table_id, row_id.0))
                                .cloned()
                        })
                        .collect::<Vec<_>>();
                    Some((
                        *table_id,
                        "delete",
                        serde_json::json!({
                            "row_ids": row_ids.iter().map(|row_id| row_id.0).collect::<Vec<_>>(),
                            "before": before,
                        }),
                    ))
                }
                Op::TruncateTable { table_id } => {
                    Some((*table_id, "truncate", serde_json::Value::Null))
                }
                _ => None,
            };
            if let Some((table_id, op, data)) = event {
                let index = operation_indices.entry(record.txn_id).or_insert(0);
                let event_position = (*commit_epoch, *index);
                *index = index.saturating_add(1);
                if resume.is_some_and(|position| event_position <= position) {
                    continue;
                }
                events.push(ChangeEvent {
                    id: Some(format!("{}:{}", event_position.0, event_position.1)),
                    channel: "changes".into(),
                    table_id: Some(table_id),
                    table: table_names.get(&table_id).cloned().unwrap_or_default(),
                    op: op.into(),
                    epoch: *commit_epoch,
                    txn_id: Some(record.txn_id),
                    message: None,
                    data: Some(data),
                });
            }
            if let Op::TxnCommit { added_runs, .. } = &record.op {
                for run in added_runs {
                    let index = operation_indices.entry(record.txn_id).or_insert(0);
                    let event_position = (*commit_epoch, *index);
                    *index = index.saturating_add(1);
                    if resume.is_some_and(|position| event_position <= position) {
                        continue;
                    }
                    let handle = self.tables.read().get(&run.table_id).cloned();
                    let rows = handle.and_then(|handle| {
                        let table = handle.lock();
                        let mut reader = table.open_reader(run.run_id).ok()?;
                        let mut rows = reader.all_rows().ok()?;
                        for row in &mut rows {
                            row.committed_epoch = Epoch(*commit_epoch);
                        }
                        Some(rows)
                    });
                    let Some(rows) = rows else {
                        // Spilled transactions keep row payloads in an immutable
                        // run instead of duplicating them in the WAL. If that run
                        // was already compacted/reaped, resuming cannot provide a
                        // complete row image and must fail closed.
                        return Ok(CdcBatch {
                            events: Vec::new(),
                            current_epoch,
                            earliest_epoch,
                            gap: true,
                        });
                    };
                    events.push(ChangeEvent {
                        id: Some(format!("{}:{}", event_position.0, event_position.1)),
                        channel: "changes".into(),
                        table_id: Some(run.table_id),
                        table: table_names.get(&run.table_id).cloned().unwrap_or_default(),
                        op: "put_run".into(),
                        epoch: *commit_epoch,
                        txn_id: Some(record.txn_id),
                        message: None,
                        data: Some(serde_json::json!({
                            "run_id": run.run_id.to_string(),
                            "row_count": run.row_count,
                            "min_row_id": run.min_row_id,
                            "max_row_id": run.max_row_id,
                            "rows": rows,
                        })),
                    });
                }
            }
        }
        Ok(CdcBatch {
            events,
            current_epoch,
            earliest_epoch,
            gap: false,
        })
    }

    /// Publish a notification message on a named channel. Reaches all active
    /// subscribers (daemon `/events`, application listeners).
    pub fn notify(&self, channel: &str, message: Option<String>) {
        let _ = self.notify.send(ChangeEvent {
            id: None,
            channel: channel.to_string(),
            table_id: None,
            table: String::new(),
            op: "notify".into(),
            epoch: self.epoch.visible().0,
            txn_id: None,
            message,
            data: None,
        });
    }

    pub fn call_procedure(
        &self,
        name: &str,
        args: HashMap<String, crate::Value>,
    ) -> Result<ProcedureCallResult> {
        self.call_procedure_as(name, args, None)
    }

    pub fn call_procedure_as(
        &self,
        name: &str,
        args: HashMap<String, crate::Value>,
        principal: Option<&crate::auth::Principal>,
    ) -> Result<ProcedureCallResult> {
        // v1 requires ALL to call procedures on a require_auth database; a
        // finer SECURITY DEFINER-style marker is a future extension (spec §9
        // decision 1).
        self.require_for(principal, &crate::auth::Permission::All)?;
        let procedure = self
            .procedure(name)
            .ok_or_else(|| MongrelError::NotFound(format!("procedure {name:?} not found")))?;
        let args = bind_procedure_args(&procedure, args)?;
        let has_writes = procedure.body.steps.iter().any(ProcedureStep::is_write);
        let mut outputs: HashMap<String, ProcedureCallOutput> = HashMap::new();
        if has_writes {
            let mut tx = self.begin_as(principal.cloned());
            let run = (|| {
                for step in &procedure.body.steps {
                    let output = self.execute_procedure_step(
                        step,
                        &args,
                        &outputs,
                        Some(&mut tx),
                        principal,
                    )?;
                    outputs.insert(step.id().to_string(), output);
                }
                eval_return_output(&procedure.body.return_value, &args, &outputs)
            })();
            match run {
                Ok(output) => {
                    let epoch = tx.commit()?.0;
                    Ok(ProcedureCallResult {
                        epoch: Some(epoch),
                        output,
                    })
                }
                Err(e) => {
                    tx.rollback();
                    Err(e)
                }
            }
        } else {
            for step in &procedure.body.steps {
                let output = self.execute_procedure_step(step, &args, &outputs, None, principal)?;
                outputs.insert(step.id().to_string(), output);
            }
            Ok(ProcedureCallResult {
                epoch: None,
                output: eval_return_output(&procedure.body.return_value, &args, &outputs)?,
            })
        }
    }

    fn execute_procedure_step(
        &self,
        step: &ProcedureStep,
        args: &HashMap<String, crate::Value>,
        outputs: &HashMap<String, ProcedureCallOutput>,
        tx: Option<&mut crate::txn::Transaction<'_>>,
        principal: Option<&crate::auth::Principal>,
    ) -> Result<ProcedureCallOutput> {
        match step {
            ProcedureStep::NativeQuery {
                table,
                conditions,
                projection,
                limit,
                ..
            } => {
                let mut q = crate::Query::new();
                for condition in conditions {
                    q = q.and(eval_condition(condition, args, outputs)?);
                }
                let handle = self.table(table)?;
                let rows = handle.lock().query(&q)?;
                let mut rows = self.secure_rows_for(table, rows, principal)?;
                if let Some(limit) = limit {
                    rows.truncate(*limit);
                }
                let projection = projection.as_ref();
                Ok(ProcedureCallOutput::Rows(
                    rows.into_iter()
                        .map(|row| ProcedureCallRow {
                            row_id: Some(row.row_id),
                            columns: match projection {
                                Some(ids) => row
                                    .columns
                                    .into_iter()
                                    .filter(|(id, _)| ids.contains(id))
                                    .collect(),
                                None => row.columns,
                            },
                        })
                        .collect(),
                ))
            }
            ProcedureStep::Put {
                table,
                cells,
                returning,
                ..
            } => {
                let tx = tx.ok_or_else(|| {
                    MongrelError::InvalidArgument(
                        "write procedure step requires a transaction".into(),
                    )
                })?;
                let cells = eval_cells(cells, args, outputs)?;
                if *returning {
                    let out = tx.put_returning(table, cells)?;
                    Ok(ProcedureCallOutput::Row(ProcedureCallRow {
                        row_id: None,
                        columns: out.row.columns.into_iter().collect(),
                    }))
                } else {
                    tx.put(table, cells)?;
                    Ok(ProcedureCallOutput::Null)
                }
            }
            ProcedureStep::Upsert {
                table,
                cells,
                update_cells,
                returning,
                ..
            } => {
                let tx = tx.ok_or_else(|| {
                    MongrelError::InvalidArgument(
                        "write procedure step requires a transaction".into(),
                    )
                })?;
                let cells = eval_cells(cells, args, outputs)?;
                let action = match update_cells {
                    Some(update_cells) => {
                        crate::UpsertAction::DoUpdate(eval_cells(update_cells, args, outputs)?)
                    }
                    None => crate::UpsertAction::DoNothing,
                };
                let out = tx.upsert(table, cells, action)?;
                if *returning {
                    Ok(ProcedureCallOutput::Row(ProcedureCallRow {
                        row_id: None,
                        columns: out.row.columns.into_iter().collect(),
                    }))
                } else {
                    Ok(ProcedureCallOutput::Null)
                }
            }
            ProcedureStep::DeleteByPk { table, pk, .. } => {
                let tx = tx.ok_or_else(|| {
                    MongrelError::InvalidArgument(
                        "write procedure step requires a transaction".into(),
                    )
                })?;
                let pk = eval_value(pk, args, outputs)?;
                let handle = self.table(table)?;
                let row_id = handle.lock().lookup_pk(&pk.encode_key()).ok_or_else(|| {
                    MongrelError::NotFound("procedure delete_by_pk target not found".into())
                })?;
                tx.delete(table, row_id)?;
                Ok(ProcedureCallOutput::Scalar(crate::Value::Bool(true)))
            }
            ProcedureStep::DeleteRows { .. } => Err(MongrelError::InvalidArgument(
                "DeleteRows procedure step is not supported by the core executor yet".into(),
            )),
            ProcedureStep::SqlQuery { .. } => Err(MongrelError::InvalidArgument(
                "SqlQuery procedure step must be executed by mongreldb-query".into(),
            )),
        }
    }

    fn validate_procedure_references(&self, procedure: &StoredProcedure) -> Result<()> {
        let cat = self.catalog.read();
        for step in &procedure.body.steps {
            let Some(table_name) = step.table() else {
                continue;
            };
            let schema = &cat
                .live(table_name)
                .ok_or_else(|| {
                    MongrelError::InvalidArgument(format!(
                        "procedure {:?} references unknown table {table_name:?}",
                        procedure.name
                    ))
                })?
                .schema;
            match step {
                ProcedureStep::NativeQuery {
                    conditions,
                    projection,
                    ..
                } => {
                    for condition in conditions {
                        validate_condition_columns(condition, schema)?;
                    }
                    if let Some(projection) = projection {
                        for id in projection {
                            validate_column_id(*id, schema)?;
                        }
                    }
                }
                ProcedureStep::Put { cells, .. } => {
                    for cell in cells {
                        validate_column_id(cell.column_id, schema)?;
                    }
                }
                ProcedureStep::Upsert {
                    cells,
                    update_cells,
                    ..
                } => {
                    for cell in cells {
                        validate_column_id(cell.column_id, schema)?;
                    }
                    if let Some(update_cells) = update_cells {
                        for cell in update_cells {
                            validate_column_id(cell.column_id, schema)?;
                        }
                    }
                }
                ProcedureStep::DeleteByPk { .. } => {
                    if schema.primary_key().is_none() {
                        return Err(MongrelError::InvalidArgument(format!(
                            "procedure {:?} references DeleteByPk on table {table_name:?} without a primary key",
                            procedure.name
                        )));
                    }
                }
                ProcedureStep::DeleteRows { .. } | ProcedureStep::SqlQuery { .. } => {}
            }
        }
        Ok(())
    }

    fn validate_trigger_references(&self, trigger: &StoredTrigger) -> Result<()> {
        let cat = self.catalog.read();
        let target_schema = match &trigger.target {
            TriggerTarget::Table(target_name) => cat
                .live(target_name)
                .ok_or_else(|| {
                    MongrelError::InvalidArgument(format!(
                        "trigger {:?} references unknown target table {target_name:?}",
                        trigger.name
                    ))
                })?
                .schema
                .clone(),
            TriggerTarget::View(_) => Schema {
                columns: trigger.target_columns.clone(),
                ..Schema::default()
            },
        };
        for col in &trigger.update_of {
            if target_schema.column(col).is_none() {
                return Err(MongrelError::InvalidArgument(format!(
                    "trigger {:?} UPDATE OF references unknown column {col:?}",
                    trigger.name
                )));
            }
        }
        if let Some(expr) = &trigger.when {
            validate_trigger_expr(expr, &target_schema, trigger.event)?;
        }
        let mut select_schemas: HashMap<String, &Schema> = HashMap::new();
        for step in &trigger.program.steps {
            if matches!(step, TriggerStep::SetNew { .. }) && trigger.timing != TriggerTiming::Before
            {
                return Err(MongrelError::InvalidArgument(
                    "SetNew trigger steps are only valid in BEFORE triggers".into(),
                ));
            }
            validate_trigger_step(
                step,
                &cat,
                &target_schema,
                trigger.event,
                &mut select_schemas,
            )?;
        }
        Ok(())
    }

    /// Begin a new transaction reading at the current visible epoch.
    pub fn begin(&self) -> crate::txn::Transaction<'_> {
        self.begin_with_isolation(crate::txn::IsolationLevel::default())
    }

    pub fn begin_as(
        &self,
        principal: Option<crate::auth::Principal>,
    ) -> crate::txn::Transaction<'_> {
        let txn_id = self.alloc_txn_id();
        let read = Snapshot::at(self.epoch.visible());
        crate::txn::Transaction::new(self, txn_id, read).with_principal(principal)
    }

    /// Begin a transaction with a specific isolation level.
    pub fn begin_with_isolation(
        &self,
        level: crate::txn::IsolationLevel,
    ) -> crate::txn::Transaction<'_> {
        let txn_id = self.alloc_txn_id();
        let epoch = match level {
            crate::txn::IsolationLevel::ReadCommitted => self.epoch.visible(),
            _ => self.epoch.visible(),
        };
        let read = Snapshot::at(epoch);
        crate::txn::Transaction::new(self, txn_id, read)
    }

    /// Begin a transaction whose trigger programs may route external-table DML
    /// through an application/query-layer module bridge.
    pub fn begin_with_external_trigger_bridge<'a>(
        &'a self,
        bridge: &'a dyn ExternalTriggerBridge,
    ) -> crate::txn::Transaction<'a> {
        let txn_id = self.alloc_txn_id();
        let read = Snapshot::at(self.epoch.visible());
        crate::txn::Transaction::new(self, txn_id, read).with_external_trigger_bridge(bridge)
    }

    pub fn begin_with_external_trigger_bridge_as<'a>(
        &'a self,
        bridge: &'a dyn ExternalTriggerBridge,
        principal: Option<crate::auth::Principal>,
    ) -> crate::txn::Transaction<'a> {
        let txn_id = self.alloc_txn_id();
        let read = Snapshot::at(self.epoch.visible());
        crate::txn::Transaction::new(self, txn_id, read)
            .with_external_trigger_bridge(bridge)
            .with_principal(principal)
    }

    /// Run `f` in a transaction; commit on `Ok`, rollback on `Err`.
    pub fn transaction<T>(
        &self,
        f: impl FnOnce(&mut crate::txn::Transaction) -> Result<T>,
    ) -> Result<T> {
        let mut tx = self.begin();
        match f(&mut tx) {
            Ok(out) => {
                tx.commit()?;
                Ok(out)
            }
            Err(e) => {
                tx.rollback();
                Err(e)
            }
        }
    }

    /// Run `f` in a transaction with an external-trigger bridge; commit on
    /// `Ok`, rollback on `Err`.
    pub fn transaction_with_external_trigger_bridge<'a, T>(
        &'a self,
        bridge: &'a dyn ExternalTriggerBridge,
        f: impl FnOnce(&mut crate::txn::Transaction) -> Result<T>,
    ) -> Result<T> {
        let mut tx = self.begin_with_external_trigger_bridge(bridge);
        match f(&mut tx) {
            Ok(out) => {
                tx.commit()?;
                Ok(out)
            }
            Err(e) => {
                tx.rollback();
                Err(e)
            }
        }
    }

    pub fn transaction_with_external_trigger_bridge_as<'a, T>(
        &'a self,
        bridge: &'a dyn ExternalTriggerBridge,
        principal: Option<crate::auth::Principal>,
        f: impl FnOnce(&mut crate::txn::Transaction) -> Result<T>,
    ) -> Result<T> {
        let mut tx = self.begin_with_external_trigger_bridge_as(bridge, principal);
        match f(&mut tx) {
            Ok(output) => {
                tx.commit()?;
                Ok(output)
            }
            Err(error) => {
                tx.rollback();
                Err(error)
            }
        }
    }

    /// Register a txn in `ActiveTxns` (spec §9.2, review fix #12). Called from
    /// `Transaction::new` so registration happens **before** any read.
    pub(crate) fn register_active(&self, epoch: Epoch) -> crate::txn::ActiveTxnGuard<'_> {
        self.active_txns.register(epoch)
    }

    fn fill_auto_increment_for_staging(
        &self,
        staging: &mut [(u64, crate::txn::Staged)],
    ) -> Result<()> {
        let tables = self.tables.read();
        for (table_id, staged) in staging {
            if let crate::txn::Staged::Put(cells) = staged {
                if let Some(handle) = tables.get(table_id) {
                    let mut t = handle.lock();
                    t.fill_auto_inc(cells)?;
                }
            }
        }
        Ok(())
    }

    fn expand_table_triggers(
        &self,
        staging: &mut Vec<(u64, crate::txn::Staged)>,
        read_epoch: Epoch,
        external_trigger_bridge: Option<&dyn ExternalTriggerBridge>,
        external_states: &mut Vec<(String, Vec<u8>)>,
    ) -> Result<()> {
        let mut external_writes = Vec::new();
        let config = self.trigger_config();
        if config.recursive_triggers {
            let chunk = std::mem::take(staging);
            let stacks = vec![Vec::new(); chunk.len()];
            *staging = self.expand_trigger_chunk(
                chunk,
                stacks,
                read_epoch,
                0,
                config.max_depth,
                &mut external_writes,
                &config,
            )?;
            self.apply_external_trigger_writes(
                external_writes,
                external_trigger_bridge,
                external_states,
                staging,
            )?;
            return Ok(());
        }

        let mut expansion = self.expand_table_triggers_once(staging, read_epoch, None, &config)?;
        if !expansion.before.is_empty() {
            let mut final_staging = expansion.before;
            final_staging.extend(filter_ignored_staging(
                std::mem::take(staging),
                &expansion.ignored_indices,
            ));
            *staging = final_staging;
        } else if !expansion.ignored_indices.is_empty() {
            *staging = filter_ignored_staging(std::mem::take(staging), &expansion.ignored_indices);
        }
        staging.append(&mut expansion.after);
        external_writes.append(&mut expansion.before_external);
        external_writes.append(&mut expansion.after_external);
        self.apply_external_trigger_writes(
            external_writes,
            external_trigger_bridge,
            external_states,
            staging,
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn expand_trigger_chunk(
        &self,
        mut chunk: Vec<(u64, crate::txn::Staged)>,
        stacks: Vec<Vec<String>>,
        read_epoch: Epoch,
        depth: u32,
        max_depth: u32,
        external_writes: &mut Vec<ExternalTriggerWrite>,
        config: &TriggerConfig,
    ) -> Result<Vec<(u64, crate::txn::Staged)>> {
        if chunk.is_empty() {
            return Ok(Vec::new());
        }
        self.fill_auto_increment_for_staging(&mut chunk)?;
        let expansion =
            self.expand_table_triggers_once(&mut chunk, read_epoch, Some(&stacks), config)?;
        if depth >= max_depth && (!expansion.before.is_empty() || !expansion.after.is_empty()) {
            let stack = expansion
                .before_stacks
                .first()
                .or_else(|| expansion.after_stacks.first())
                .cloned()
                .unwrap_or_default();
            return Err(MongrelError::Conflict(format!(
                "trigger recursion exceeded max depth {max_depth}; trigger stack: {}",
                Self::format_trigger_stack(&stack)
            )));
        }

        let mut out = Vec::new();
        external_writes.extend(expansion.before_external);
        out.extend(self.expand_trigger_chunk(
            expansion.before,
            expansion.before_stacks,
            read_epoch,
            depth + 1,
            max_depth,
            external_writes,
            config,
        )?);
        out.extend(filter_ignored_staging(chunk, &expansion.ignored_indices));
        external_writes.extend(expansion.after_external);
        out.extend(self.expand_trigger_chunk(
            expansion.after,
            expansion.after_stacks,
            read_epoch,
            depth + 1,
            max_depth,
            external_writes,
            config,
        )?);
        Ok(out)
    }

    fn apply_external_trigger_writes(
        &self,
        writes: Vec<ExternalTriggerWrite>,
        bridge: Option<&dyn ExternalTriggerBridge>,
        external_states: &mut Vec<(String, Vec<u8>)>,
        staging: &mut Vec<(u64, crate::txn::Staged)>,
    ) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }
        let bridge = bridge.ok_or_else(|| {
            MongrelError::InvalidArgument(
                "trigger program wrote an external table, but this transaction has no external trigger bridge".into(),
            )
        })?;
        for write in writes {
            let table = write.table().to_string();
            let entry = self.external_table(&table).ok_or_else(|| {
                MongrelError::NotFound(format!("external table {table:?} not found"))
            })?;
            let base_state = current_external_state_bytes(&self.root, external_states, &table)?;
            let result = bridge.apply_trigger_external_write(&entry, base_state, write)?;
            external_states.push((table, result.state));
            for base_write in result.base_writes {
                match base_write {
                    ExternalTriggerBaseWrite::Put { table, cells } => {
                        let table_id = self.table_id(&table)?;
                        staging.push((table_id, crate::txn::Staged::Put(cells)));
                    }
                    ExternalTriggerBaseWrite::Delete { table, row_id } => {
                        let table_id = self.table_id(&table)?;
                        staging.push((table_id, crate::txn::Staged::Delete(row_id)));
                    }
                }
            }
        }
        dedup_external_states_in_place(external_states);
        Ok(())
    }

    fn expand_table_triggers_once(
        &self,
        staging: &mut Vec<(u64, crate::txn::Staged)>,
        read_epoch: Epoch,
        trigger_stacks: Option<&[Vec<String>]>,
        config: &TriggerConfig,
    ) -> Result<TriggerExpansion> {
        let triggers: Vec<StoredTrigger> = self
            .catalog
            .read()
            .triggers
            .iter()
            .filter(|entry| {
                entry.trigger.enabled
                    && matches!(
                        entry.trigger.timing,
                        TriggerTiming::Before | TriggerTiming::After
                    )
                    && matches!(entry.trigger.target, TriggerTarget::Table(_))
            })
            .map(|entry| entry.trigger.clone())
            .collect();
        if triggers.is_empty() || staging.is_empty() {
            return Ok(TriggerExpansion::default());
        }

        let before_triggers = triggers
            .iter()
            .filter(|trigger| trigger.timing == TriggerTiming::Before)
            .cloned()
            .collect::<Vec<_>>();
        let after_triggers = triggers
            .iter()
            .filter(|trigger| trigger.timing == TriggerTiming::After)
            .cloned()
            .collect::<Vec<_>>();

        let mut before_added = Vec::new();
        let mut before_stacks = Vec::new();
        let mut before_external = Vec::new();
        let mut ignored_indices = std::collections::BTreeSet::new();
        if !before_triggers.is_empty() {
            let before_events =
                self.trigger_events_for_staging(staging, read_epoch, trigger_stacks)?;
            let mut out = TriggerProgramOutput {
                added: &mut before_added,
                added_stacks: &mut before_stacks,
                added_external: &mut before_external,
                ignored_indices: &mut ignored_indices,
            };
            self.execute_triggers_for_events(
                &before_triggers,
                &before_events,
                Some(staging),
                &mut out,
                config,
                read_epoch,
            )?;
        }

        let after_events = if after_triggers.is_empty() {
            Vec::new()
        } else {
            self.trigger_events_for_staging(staging, read_epoch, trigger_stacks)?
                .into_iter()
                .filter(|event| {
                    !event
                        .op_indices
                        .iter()
                        .any(|idx| ignored_indices.contains(idx))
                })
                .collect()
        };

        let mut after_added = Vec::new();
        let mut after_stacks = Vec::new();
        let mut after_external = Vec::new();
        let mut out = TriggerProgramOutput {
            added: &mut after_added,
            added_stacks: &mut after_stacks,
            added_external: &mut after_external,
            ignored_indices: &mut ignored_indices,
        };
        self.execute_triggers_for_events(
            &after_triggers,
            &after_events,
            None,
            &mut out,
            config,
            read_epoch,
        )?;
        Ok(TriggerExpansion {
            before: before_added,
            before_stacks,
            before_external,
            after: after_added,
            after_stacks,
            after_external,
            ignored_indices,
        })
    }

    fn execute_triggers_for_events(
        &self,
        triggers: &[StoredTrigger],
        events: &[WriteEvent],
        mut staging: Option<&mut Vec<(u64, crate::txn::Staged)>>,
        out: &mut TriggerProgramOutput<'_>,
        config: &TriggerConfig,
        read_epoch: Epoch,
    ) -> Result<()> {
        for event in events {
            for trigger in triggers {
                if event
                    .op_indices
                    .iter()
                    .any(|idx| out.ignored_indices.contains(idx))
                {
                    break;
                }
                let matches = {
                    let cat = self.catalog.read();
                    trigger_matches_event(trigger, event, &cat)?
                };
                if !matches {
                    continue;
                }
                if let Some(when) = &trigger.when {
                    if !eval_trigger_expr(when, event)? {
                        continue;
                    }
                }
                let trigger_stack = Self::trigger_stack_with(&event.trigger_stack, &trigger.name);
                if event.trigger_stack.iter().any(|name| name == &trigger.name) {
                    return Err(MongrelError::Conflict(format!(
                        "trigger recursion cycle detected; trigger stack: {}",
                        Self::format_trigger_stack(&trigger_stack)
                    )));
                }
                let outcome = match staging.as_mut() {
                    Some(staging) => self.execute_trigger_program(
                        trigger,
                        event,
                        Some(&mut **staging),
                        out,
                        &trigger_stack,
                        config,
                        read_epoch,
                    )?,
                    None => self.execute_trigger_program(
                        trigger,
                        event,
                        None,
                        out,
                        &trigger_stack,
                        config,
                        read_epoch,
                    )?,
                };
                if outcome == TriggerProgramOutcome::Ignore {
                    out.ignored_indices.extend(event.op_indices.iter().copied());
                    break;
                }
            }
        }
        Ok(())
    }

    fn trigger_events_for_staging(
        &self,
        staging: &[(u64, crate::txn::Staged)],
        read_epoch: Epoch,
        trigger_stacks: Option<&[Vec<String>]>,
    ) -> Result<Vec<WriteEvent>> {
        use crate::txn::Staged;
        use std::collections::{HashMap, VecDeque};

        let snapshot = Snapshot::at(read_epoch);
        let cat = self.catalog.read();
        let mut table_names = HashMap::new();
        let mut table_schemas = HashMap::new();
        for entry in cat
            .tables
            .iter()
            .filter(|entry| matches!(entry.state, TableState::Live))
        {
            table_names.insert(entry.table_id, entry.name.clone());
            table_schemas.insert(entry.table_id, entry.schema.clone());
        }
        drop(cat);

        let mut old_rows: HashMap<usize, TriggerRowImage> = HashMap::new();
        let mut delete_by_key: HashMap<(u64, Vec<u8>), VecDeque<usize>> = HashMap::new();
        let mut put_by_key: HashMap<(u64, Vec<u8>), VecDeque<usize>> = HashMap::new();

        for (idx, (table_id, staged)) in staging.iter().enumerate() {
            let Some(schema) = table_schemas.get(table_id) else {
                continue;
            };
            let Some(pk) = schema.primary_key() else {
                continue;
            };
            match staged {
                Staged::Delete(row_id) => {
                    let handle = self.table_by_id(*table_id)?;
                    let Some(row) = handle.lock().get(*row_id, snapshot) else {
                        continue;
                    };
                    let Some(pk_value) = row.columns.get(&pk.id) else {
                        continue;
                    };
                    old_rows.insert(idx, TriggerRowImage::from_row(row.clone()));
                    delete_by_key
                        .entry((*table_id, pk_value.encode_key()))
                        .or_default()
                        .push_back(idx);
                }
                Staged::Put(cells) => {
                    if let Some((_, value)) = cells.iter().find(|(id, _)| *id == pk.id) {
                        put_by_key
                            .entry((*table_id, value.encode_key()))
                            .or_default()
                            .push_back(idx);
                    }
                }
                Staged::Update(row_id, _) => {
                    let handle = self.table_by_id(*table_id)?;
                    let row = handle.lock().get(*row_id, snapshot);
                    if let Some(row) = row {
                        old_rows.insert(idx, TriggerRowImage::from_row(row));
                    }
                }
                Staged::Truncate => {}
            }
        }

        let mut paired_delete = std::collections::HashSet::new();
        let mut paired_put = std::collections::HashSet::new();
        let mut events = Vec::new();

        for (key, deletes) in delete_by_key.iter_mut() {
            let Some(puts) = put_by_key.get_mut(key) else {
                continue;
            };
            while let (Some(delete_idx), Some(put_idx)) = (deletes.pop_front(), puts.pop_front()) {
                paired_delete.insert(delete_idx);
                paired_put.insert(put_idx);
                let (table_id, _) = &staging[put_idx];
                let Some(table_name) = table_names.get(table_id).cloned() else {
                    continue;
                };
                let old = old_rows.get(&delete_idx).cloned();
                let new = match &staging[put_idx].1 {
                    Staged::Put(cells) => Some(TriggerRowImage::from_cells(cells)),
                    _ => None,
                };
                let changed_columns = changed_columns(old.as_ref(), new.as_ref());
                events.push(WriteEvent {
                    table: table_name,
                    kind: TriggerEvent::Update,
                    old,
                    new,
                    changed_columns,
                    op_indices: vec![delete_idx, put_idx],
                    put_idx: Some(put_idx),
                    trigger_stack: Self::trigger_stack_for_indices(
                        trigger_stacks,
                        &[delete_idx, put_idx],
                    ),
                });
            }
        }

        for (idx, (table_id, staged)) in staging.iter().enumerate() {
            let Some(table_name) = table_names.get(table_id).cloned() else {
                continue;
            };
            match staged {
                Staged::Put(cells) if !paired_put.contains(&idx) => {
                    let new = Some(TriggerRowImage::from_cells(cells));
                    let changed_columns = cells.iter().map(|(id, _)| *id).collect();
                    events.push(WriteEvent {
                        table: table_name,
                        kind: TriggerEvent::Insert,
                        old: None,
                        new,
                        changed_columns,
                        op_indices: vec![idx],
                        put_idx: Some(idx),
                        trigger_stack: Self::trigger_stack_for_indices(trigger_stacks, &[idx]),
                    });
                }
                Staged::Delete(row_id) if !paired_delete.contains(&idx) => {
                    let old = match old_rows.get(&idx).cloned() {
                        Some(old) => Some(old),
                        None => {
                            let handle = self.table_by_id(*table_id)?;
                            let row = handle.lock().get(*row_id, snapshot);
                            row.map(TriggerRowImage::from_row)
                        }
                    };
                    let Some(old) = old else {
                        continue;
                    };
                    let changed_columns = old.columns.keys().copied().collect();
                    events.push(WriteEvent {
                        table: table_name,
                        kind: TriggerEvent::Delete,
                        old: Some(old),
                        new: None,
                        changed_columns,
                        op_indices: vec![idx],
                        put_idx: None,
                        trigger_stack: Self::trigger_stack_for_indices(trigger_stacks, &[idx]),
                    });
                }
                Staged::Update(_, cells) => {
                    let old = old_rows.get(&idx).cloned();
                    let new = Some(TriggerRowImage::from_cells(cells));
                    let changed_columns = changed_columns(old.as_ref(), new.as_ref());
                    events.push(WriteEvent {
                        table: table_name,
                        kind: TriggerEvent::Update,
                        old,
                        new,
                        changed_columns,
                        op_indices: vec![idx],
                        put_idx: Some(idx),
                        trigger_stack: Self::trigger_stack_for_indices(trigger_stacks, &[idx]),
                    });
                }
                Staged::Truncate => {}
                _ => {}
            }
        }

        Ok(events)
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_trigger_program(
        &self,
        trigger: &StoredTrigger,
        event: &WriteEvent,
        staging: Option<&mut Vec<(u64, crate::txn::Staged)>>,
        out: &mut TriggerProgramOutput<'_>,
        trigger_stack: &[String],
        config: &TriggerConfig,
        read_epoch: Epoch,
    ) -> Result<TriggerProgramOutcome> {
        let mut event = event.clone();
        let mut select_results: HashMap<String, Vec<TriggerRowImage>> = HashMap::new();
        self.execute_trigger_steps(
            trigger,
            &trigger.program.steps,
            &mut event,
            staging,
            out,
            trigger_stack,
            config,
            &mut select_results,
            0,
            None,
            read_epoch,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_trigger_steps(
        &self,
        trigger: &StoredTrigger,
        steps: &[TriggerStep],
        event: &mut WriteEvent,
        mut staging: Option<&mut Vec<(u64, crate::txn::Staged)>>,
        out: &mut TriggerProgramOutput<'_>,
        trigger_stack: &[String],
        config: &TriggerConfig,
        select_results: &mut HashMap<String, Vec<TriggerRowImage>>,
        depth: u32,
        selected: Option<&TriggerRowImage>,
        read_epoch: Epoch,
    ) -> Result<TriggerProgramOutcome> {
        let _ = depth;
        for step in steps {
            match step {
                TriggerStep::SetNew { cells } => {
                    if trigger.timing != TriggerTiming::Before {
                        return Err(MongrelError::InvalidArgument(
                            "SetNew trigger step is only valid in BEFORE triggers".into(),
                        ));
                    }
                    let put_idx = event.put_idx.ok_or_else(|| {
                        MongrelError::InvalidArgument(
                            "SetNew trigger step requires INSERT or UPDATE NEW row".into(),
                        )
                    })?;
                    let staging = staging.as_deref_mut().ok_or_else(|| {
                        MongrelError::InvalidArgument(
                            "SetNew trigger step requires mutable trigger staging".into(),
                        )
                    })?;
                    let row_cells = match staging.get_mut(put_idx).map(|(_, op)| op) {
                        Some(crate::txn::Staged::Put(cells))
                        | Some(crate::txn::Staged::Update(_, cells)) => cells,
                        _ => {
                            return Err(MongrelError::InvalidArgument(
                                "SetNew trigger step target row is not mutable".into(),
                            ))
                        }
                    };
                    for (column_id, value) in eval_trigger_cells(cells, event, selected)? {
                        row_cells.retain(|(id, _)| *id != column_id);
                        row_cells.push((column_id, value.clone()));
                        if let Some(new) = &mut event.new {
                            new.columns.insert(column_id, value);
                        }
                    }
                    row_cells.sort_by_key(|(id, _)| *id);
                }
                TriggerStep::Insert { table, cells } => {
                    let cells = eval_trigger_cells(cells, event, selected)?;
                    if let Ok(table_id) = self.table_id(table) {
                        out.added.push((table_id, crate::txn::Staged::Put(cells)));
                        out.added_stacks.push(trigger_stack.to_vec());
                    } else if self.external_table(table).is_some() {
                        out.added_external.push(ExternalTriggerWrite::Insert {
                            table: table.clone(),
                            cells,
                        });
                    } else {
                        return Err(MongrelError::NotFound(format!(
                            "trigger {:?} insert target {table:?} not found",
                            trigger.name
                        )));
                    }
                }
                TriggerStep::UpdateByPk { table, pk, cells } => {
                    let pk = eval_trigger_value(pk, event, selected)?;
                    let cells = eval_trigger_cells(cells, event, selected)?;
                    if self.external_table(table).is_some() {
                        out.added_external.push(ExternalTriggerWrite::UpdateByPk {
                            table: table.clone(),
                            pk,
                            cells,
                        });
                    } else {
                        let row_id = self
                            .table(table)?
                            .lock()
                            .lookup_pk(&pk.encode_key())
                            .ok_or_else(|| {
                                MongrelError::NotFound(format!(
                                    "trigger {:?} update target not found",
                                    trigger.name
                                ))
                            })?;
                        let handle = self.table(table)?;
                        let snapshot = Snapshot::at(self.epoch.visible());
                        let old = handle.lock().get(row_id, snapshot).ok_or_else(|| {
                            MongrelError::NotFound(format!(
                                "trigger {:?} update target not visible",
                                trigger.name
                            ))
                        })?;
                        let mut merged = old.columns;
                        for (column_id, value) in cells {
                            merged.insert(column_id, value);
                        }
                        out.added.push((
                            self.table_id(table)?,
                            crate::txn::Staged::Update(row_id, merged.into_iter().collect()),
                        ));
                        out.added_stacks.push(trigger_stack.to_vec());
                    }
                }
                TriggerStep::DeleteByPk { table, pk } => {
                    let pk = eval_trigger_value(pk, event, selected)?;
                    if self.external_table(table).is_some() {
                        out.added_external.push(ExternalTriggerWrite::DeleteByPk {
                            table: table.clone(),
                            pk,
                        });
                    } else {
                        let row_id = self
                            .table(table)?
                            .lock()
                            .lookup_pk(&pk.encode_key())
                            .ok_or_else(|| {
                                MongrelError::NotFound(format!(
                                    "trigger {:?} delete target not found",
                                    trigger.name
                                ))
                            })?;
                        out.added
                            .push((self.table_id(table)?, crate::txn::Staged::Delete(row_id)));
                        out.added_stacks.push(trigger_stack.to_vec());
                    }
                }
                TriggerStep::Select {
                    id,
                    table,
                    conditions,
                } => {
                    let schema = self.table(table)?.lock().schema().clone();
                    let snapshot = Snapshot::at(read_epoch);
                    let rows = self.table(table)?.lock().visible_rows(snapshot)?;
                    let mut matched = Vec::new();
                    for row in rows {
                        let image = TriggerRowImage::from_row(row);
                        let passes = conditions
                            .iter()
                            .map(|cond| eval_trigger_condition(cond, event, &image, &schema))
                            .collect::<Result<Vec<_>>>()?
                            .into_iter()
                            .all(|b| b);
                        if passes {
                            matched.push(image);
                        }
                    }
                    if let Some(pk) = schema.primary_key() {
                        matched.sort_by(|a, b| {
                            let av = a.columns.get(&pk.id).unwrap_or(&Value::Null);
                            let bv = b.columns.get(&pk.id).unwrap_or(&Value::Null);
                            value_order(av, bv).unwrap_or(std::cmp::Ordering::Equal)
                        });
                    }
                    select_results.insert(id.clone(), matched);
                }
                TriggerStep::Foreach { id, steps } => {
                    let rows = select_results.get(id).ok_or_else(|| {
                        MongrelError::InvalidArgument(format!(
                            "trigger {:?} foreach references unknown select id {id:?}",
                            trigger.name
                        ))
                    })?;
                    if rows.len() > config.max_loop_iterations as usize {
                        return Err(MongrelError::InvalidArgument(format!(
                            "trigger {:?} foreach exceeded max_loop_iterations ({})",
                            trigger.name, config.max_loop_iterations
                        )));
                    }
                    for row in rows.clone() {
                        let result = self.execute_trigger_steps(
                            trigger,
                            steps,
                            event,
                            staging.as_deref_mut(),
                            out,
                            trigger_stack,
                            config,
                            select_results,
                            depth + 1,
                            Some(&row),
                            read_epoch,
                        )?;
                        if result == TriggerProgramOutcome::Ignore {
                            return Ok(TriggerProgramOutcome::Ignore);
                        }
                    }
                }
                TriggerStep::DeleteWhere { table, conditions } => {
                    let schema = self.table(table)?.lock().schema().clone();
                    let snapshot = Snapshot::at(read_epoch);
                    let rows = self.table(table)?.lock().visible_rows(snapshot)?;
                    let table_id = self.table_id(table)?;
                    let mut to_delete = Vec::new();
                    for row in rows {
                        let image = TriggerRowImage::from_row(row.clone());
                        let passes = conditions
                            .iter()
                            .map(|cond| eval_trigger_condition(cond, event, &image, &schema))
                            .collect::<Result<Vec<_>>>()?
                            .into_iter()
                            .all(|b| b);
                        if passes {
                            to_delete.push((table_id, row.row_id));
                        }
                    }
                    for (table_id, row_id) in to_delete {
                        out.added
                            .push((table_id, crate::txn::Staged::Delete(row_id)));
                        out.added_stacks.push(trigger_stack.to_vec());
                    }
                }
                TriggerStep::UpdateWhere {
                    table,
                    conditions,
                    cells,
                } => {
                    let schema = self.table(table)?.lock().schema().clone();
                    let snapshot = Snapshot::at(read_epoch);
                    let rows = self.table(table)?.lock().visible_rows(snapshot)?;
                    let table_id = self.table_id(table)?;
                    let mut to_update = Vec::new();
                    for row in rows {
                        let image = TriggerRowImage::from_row(row.clone());
                        let passes = conditions
                            .iter()
                            .map(|cond| eval_trigger_condition(cond, event, &image, &schema))
                            .collect::<Result<Vec<_>>>()?
                            .into_iter()
                            .all(|b| b);
                        if passes {
                            let new_cells = cells
                                .iter()
                                .map(|cell| {
                                    Ok((
                                        cell.column_id,
                                        eval_trigger_value(&cell.value, event, Some(&image))?,
                                    ))
                                })
                                .collect::<Result<Vec<_>>>()?;
                            let mut merged = row.columns.clone();
                            for (column_id, value) in new_cells {
                                merged.insert(column_id, value);
                            }
                            to_update.push((table_id, row.row_id, merged));
                        }
                    }
                    for (table_id, row_id, merged) in to_update {
                        out.added.push((
                            table_id,
                            crate::txn::Staged::Update(row_id, merged.into_iter().collect()),
                        ));
                        out.added_stacks.push(trigger_stack.to_vec());
                    }
                }
                TriggerStep::Raise { action, message } => match action {
                    TriggerRaiseAction::Ignore => return Ok(TriggerProgramOutcome::Ignore),
                    TriggerRaiseAction::Abort
                    | TriggerRaiseAction::Fail
                    | TriggerRaiseAction::Rollback => {
                        let message = eval_trigger_value(message, event, selected)?;
                        return Err(MongrelError::Conflict(format!(
                            "trigger {:?} raised: {}; trigger stack: {}",
                            trigger.name,
                            trigger_message(message),
                            Self::format_trigger_stack(trigger_stack)
                        )));
                    }
                },
            }
        }
        Ok(TriggerProgramOutcome::Continue)
    }

    fn trigger_stack_for_indices(stacks: Option<&[Vec<String>]>, indices: &[usize]) -> Vec<String> {
        let Some(stacks) = stacks else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for idx in indices {
            let Some(stack) = stacks.get(*idx) else {
                continue;
            };
            for name in stack {
                if !out.iter().any(|existing| existing == name) {
                    out.push(name.clone());
                }
            }
        }
        out
    }

    fn trigger_stack_with(stack: &[String], trigger_name: &str) -> Vec<String> {
        let mut out = stack.to_vec();
        out.push(trigger_name.to_string());
        out
    }

    fn format_trigger_stack(stack: &[String]) -> String {
        if stack.is_empty() {
            "<root>".into()
        } else {
            stack.join(" -> ")
        }
    }

    /// Authoritatively validate every declared constraint on the staged write
    /// set under the transaction's read snapshot, AND expand ON DELETE CASCADE /
    /// SET NULL actions into explicit child ops. Called from
    /// [`Self::commit_transaction`] outside the WAL mutex. Returns the first
    /// violation as an `Err`, aborting the commit atomically. This is the
    /// server-side authority point: concurrent remote writers that each pass
    /// their own client-side checks still cannot both commit a violating batch.
    ///
    /// Scope: CHECK (full, three-valued), UNIQUE beyond the PK (existence scan +
    /// intra-transaction dedup; concurrent-txn races are additionally caught by
    /// `WriteKey::Unique`), and FK insert-side parent existence + ON DELETE
    /// {RESTRICT, CASCADE, SET NULL}. CASCADE appends child deletes (transitive
    /// fixpoint); SET NULL appends child updates (FK columns nulled). Truncate is
    /// RESTRICT-only (cascade-truncate is unsupported).
    fn validate_constraints(
        &self,
        staging: &mut Vec<(u64, crate::txn::Staged)>,
        read_epoch: Epoch,
    ) -> Result<()> {
        use crate::constraint::{encode_composite_key, validate_checks, FkAction};
        use crate::memtable::Row;
        use crate::txn::Staged;
        use std::collections::HashSet;

        let snapshot = Snapshot::at(read_epoch);
        let cat = self.catalog.read();

        // Collect live (id, name, constraints-bearing?) for staged tables.
        let live: Vec<(u64, &str, &crate::schema::Schema)> = cat
            .tables
            .iter()
            .filter(|e| matches!(e.state, TableState::Live))
            .map(|e| (e.table_id, e.name.as_str(), &e.schema))
            .collect();

        // Fast path: bail if no live table declares any constraints at all.
        let any_constraints = live.iter().any(|(_, _, s)| !s.constraints.is_empty());
        if !any_constraints {
            return Ok(());
        }

        // Lazily-loaded visible rows per table, shared across checks.
        let mut rows_cache: HashMap<u64, Vec<Row>> = HashMap::new();
        let mut load_rows = |table_id: u64| -> Result<Vec<Row>> {
            if let Some(r) = rows_cache.get(&table_id) {
                return Ok(r.clone());
            }
            let handle = self.table_by_id(table_id)?;
            let rows = handle.lock().visible_rows(snapshot)?;
            rows_cache.insert(table_id, rows.clone());
            Ok(rows)
        };

        // ── Phase A1: expand ON UPDATE CASCADE / SET NULL while updates still
        // carry an explicit old RowId + full new image. This makes action choice
        // reliable even when the referenced key itself changes; a delete+put
        // heuristic cannot distinguish that from unrelated operations.
        let mut processed_updates = HashSet::new();
        type PendingUpdate = (usize, u64, crate::rowid::RowId, Vec<(u16, Value)>);
        loop {
            let updates: Vec<PendingUpdate> = staging
                .iter()
                .enumerate()
                .filter_map(|(index, (table_id, op))| match op {
                    Staged::Update(row_id, cells) if !processed_updates.contains(&index) => {
                        Some((index, *table_id, *row_id, cells.clone()))
                    }
                    _ => None,
                })
                .collect();
            if updates.is_empty() {
                break;
            }
            let mut new_ops = Vec::new();
            for (index, table_id, row_id, new_cells) in updates {
                processed_updates.insert(index);
                let Some(tname) = live
                    .iter()
                    .find(|(id, _, _)| *id == table_id)
                    .map(|(_, name, _)| *name)
                else {
                    continue;
                };
                let Some(old_row) = self.table_by_id(table_id)?.lock().get(row_id, snapshot) else {
                    continue;
                };
                let new_map: HashMap<u16, Value> = new_cells.iter().cloned().collect();
                for (child_id, _child_name, child_schema) in &live {
                    for fk in &child_schema.constraints.foreign_keys {
                        if fk.ref_table != tname {
                            continue;
                        }
                        let Some(old_key) = encode_composite_key(&fk.ref_columns, &old_row.columns)
                        else {
                            continue;
                        };
                        if encode_composite_key(&fk.ref_columns, &new_map).as_deref()
                            == Some(old_key.as_slice())
                        {
                            continue;
                        }
                        if fk.on_update == FkAction::Restrict {
                            continue;
                        }
                        let child_rows = load_rows(*child_id)?;
                        for child in child_rows {
                            if encode_composite_key(&fk.columns, &child.columns).as_deref()
                                != Some(old_key.as_slice())
                            {
                                continue;
                            }
                            if staging.iter().any(|(id, op)| {
                                *id == *child_id
                                    && matches!(op, Staged::Delete(id) if *id == child.row_id)
                            }) {
                                continue;
                            }
                            let mut cells: Vec<(u16, Value)> = child
                                .columns
                                .iter()
                                .map(|(column_id, value)| (*column_id, value.clone()))
                                .collect();
                            for (child_column, parent_column) in
                                fk.columns.iter().zip(&fk.ref_columns)
                            {
                                cells.retain(|(column_id, _)| column_id != child_column);
                                let value = match fk.on_update {
                                    FkAction::Cascade => {
                                        new_map.get(parent_column).cloned().unwrap_or(Value::Null)
                                    }
                                    FkAction::SetNull => Value::Null,
                                    FkAction::Restrict => unreachable!(),
                                };
                                cells.push((*child_column, value));
                            }
                            cells.sort_by_key(|(column_id, _)| *column_id);
                            if let Some(existing_index) = staging.iter().position(|(id, op)| {
                                *id == *child_id
                                    && matches!(op, Staged::Update(id, _) if *id == child.row_id)
                            }) {
                                if let Staged::Update(_, existing) = &mut staging[existing_index].1
                                {
                                    if *existing != cells {
                                        *existing = cells;
                                        processed_updates.remove(&existing_index);
                                    }
                                }
                            } else {
                                new_ops.push((*child_id, Staged::Update(child.row_id, cells)));
                            }
                        }
                    }
                }
            }
            staging.extend(new_ops);
        }

        // ── Phase A2: expand ON DELETE CASCADE / SET NULL into explicit child
        // ops (transitive fixpoint). RESTRICT is not expanded here — it is
        // enforced as a violation in Phase B. `cascaded` records every delete
        // we have already expanded so a self-referential CASCADE FK cannot loop.
        let mut cascaded: HashSet<(u64, u64)> = HashSet::new();
        loop {
            let mut new_ops: Vec<(u64, Staged)> = Vec::new();
            let deletes: Vec<(u64, crate::rowid::RowId)> = staging
                .iter()
                .filter_map(|(t, op)| match op {
                    Staged::Delete(rid) => Some((*t, *rid)),
                    _ => None,
                })
                .collect();
            for (table_id, rid) in deletes {
                if !cascaded.insert((table_id, rid.0)) {
                    continue;
                }
                let Some(tname) = live
                    .iter()
                    .find(|(t, _, _)| *t == table_id)
                    .map(|(_, n, _)| *n)
                else {
                    continue;
                };
                let parent_handle = self.table_by_id(table_id)?;
                let Some(parent_row) = parent_handle.lock().get(rid, snapshot) else {
                    continue;
                };
                for (child_id, _child_name, child_schema) in &live {
                    for fk in &child_schema.constraints.foreign_keys {
                        if fk.ref_table != tname {
                            continue;
                        }
                        let Some(parent_key) =
                            encode_composite_key(&fk.ref_columns, &parent_row.columns)
                        else {
                            continue;
                        };
                        // Suppress ON DELETE cascade/set-null when this "delete"
                        // is actually half of an UPDATE encoded as Delete(old)+
                        // Put(new): if a staged Put in the SAME table still
                        // provides the referenced parent key, the parent still
                        // exists (its non-key columns changed) and the children
                        // must be left alone. A genuine delete, or an update
                        // that CHANGES the referenced key, has no preserving Put
                        // → cascade fires as before.
                        let key_preserved = staging.iter().any(|(t, op)| {
                            if *t != table_id {
                                return false;
                            }
                            let Staged::Put(cells) = op else {
                                return false;
                            };
                            let map: HashMap<u16, crate::memtable::Value> =
                                cells.iter().cloned().collect();
                            encode_composite_key(&fk.ref_columns, &map).as_deref()
                                == Some(parent_key.as_slice())
                        });
                        if key_preserved {
                            continue;
                        }
                        match fk.on_delete {
                            FkAction::Restrict => continue,
                            FkAction::Cascade => {
                                let child_rows = load_rows(*child_id)?;
                                for cr in &child_rows {
                                    if !cascaded.contains(&(*child_id, cr.row_id.0))
                                        && encode_composite_key(&fk.columns, &cr.columns).as_deref()
                                            == Some(parent_key.as_slice())
                                    {
                                        new_ops.push((*child_id, Staged::Delete(cr.row_id)));
                                    }
                                }
                            }
                            FkAction::SetNull => {
                                let child_rows = load_rows(*child_id)?;
                                for cr in &child_rows {
                                    if !cascaded.contains(&(*child_id, cr.row_id.0))
                                        && encode_composite_key(&fk.columns, &cr.columns).as_deref()
                                            == Some(parent_key.as_slice())
                                    {
                                        // Re-emit the child row with the FK
                                        // columns set to NULL (delete + put).
                                        let mut cells: Vec<(u16, crate::memtable::Value)> = cr
                                            .columns
                                            .iter()
                                            .map(|(k, v)| (*k, v.clone()))
                                            .collect();
                                        for cid in &fk.columns {
                                            cells.retain(|(k, _)| k != cid);
                                            cells.push((*cid, crate::memtable::Value::Null));
                                        }
                                        new_ops.push((*child_id, Staged::Update(cr.row_id, cells)));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if new_ops.is_empty() {
                break;
            }
            staging.extend(new_ops);
        }

        // Rows staged for deletion in THIS transaction (now including cascaded
        // deletes). Used to exclude the old version of an updated row from
        // unique-existence scans.
        let staged_deletes: HashSet<(u64, u64)> = staging
            .iter()
            .filter_map(|(t, op)| match op {
                Staged::Delete(rid) | Staged::Update(rid, _) => Some((*t, rid.0)),
                _ => None,
            })
            .collect();

        // Intra-transaction unique-key dedup: (table_id, uc_id, key).
        let mut seen_unique: HashSet<(u64, u16, Vec<u8>)> = HashSet::new();

        // ── Phase B: validate the fully-expanded staging set.
        for (table_id, op) in staging.iter() {
            let Some((_, tname, schema)) = live.iter().find(|(t, _, _)| t == table_id).copied()
            else {
                continue;
            };
            let cells_map: HashMap<u16, crate::memtable::Value>;
            match op {
                Staged::Put(cells) | Staged::Update(_, cells) => {
                    cells_map = cells.iter().cloned().collect();

                    // CHECK constraints.
                    if !schema.constraints.checks.is_empty() {
                        validate_checks(&schema.constraints.checks, &cells_map)?;
                    }

                    // UNIQUE (non-PK) constraints.
                    for uc in &schema.constraints.uniques {
                        let Some(key) = encode_composite_key(&uc.columns, &cells_map) else {
                            continue; // NULL in a constrained column → skip (SQL).
                        };
                        let marker = (*table_id, uc.id, key.clone());
                        if !seen_unique.insert(marker) {
                            return Err(MongrelError::Conflict(format!(
                                "UNIQUE constraint '{}' on table '{tname}' violated within batch",
                                uc.name
                            )));
                        }
                        let rows = load_rows(*table_id)?;
                        for r in &rows {
                            // Skip rows this same transaction is deleting (the
                            // old version of an updated/cascade-deleted row).
                            if staged_deletes.contains(&(*table_id, r.row_id.0)) {
                                continue;
                            }
                            if let Some(theirs) = encode_composite_key(&uc.columns, &r.columns) {
                                if theirs == key {
                                    return Err(MongrelError::Conflict(format!(
                                        "UNIQUE constraint '{}' on table '{tname}' violated",
                                        uc.name
                                    )));
                                }
                            }
                        }
                    }

                    // FK insert-side: parent must exist.
                    for fk in &schema.constraints.foreign_keys {
                        let Some(child_key) = encode_composite_key(&fk.columns, &cells_map) else {
                            continue; // NULL FK component → not checked (SQL).
                        };
                        let Some(parent_id) = cat
                            .tables
                            .iter()
                            .find(|t| t.name == fk.ref_table)
                            .map(|t| t.table_id)
                        else {
                            return Err(MongrelError::InvalidArgument(format!(
                                "FOREIGN KEY '{}' references unknown table '{}'",
                                fk.name, fk.ref_table
                            )));
                        };
                        let parent_rows = load_rows(parent_id)?;
                        let mut found = false;
                        for r in &parent_rows {
                            if staged_deletes.contains(&(parent_id, r.row_id.0)) {
                                continue;
                            }
                            if let Some(pkey) = encode_composite_key(&fk.ref_columns, &r.columns) {
                                if pkey == child_key {
                                    found = true;
                                    break;
                                }
                            }
                        }
                        // Final-write-set FK validation: a parent inserted in
                        // THIS transaction also satisfies the FK. This enables
                        // atomic parent+child batches and cyclical/mutual FK
                        // inserts within a single transaction — the child sees
                        // the staged parent put even though it is not committed
                        // yet.
                        if !found {
                            for (st_table, st_op) in staging.iter() {
                                if *st_table != parent_id {
                                    continue;
                                }
                                if let Staged::Put(pcells) | Staged::Update(_, pcells) = st_op {
                                    let pmap: HashMap<u16, crate::memtable::Value> =
                                        pcells.iter().cloned().collect();
                                    if let Some(pkey) = encode_composite_key(&fk.ref_columns, &pmap)
                                    {
                                        if pkey == child_key {
                                            found = true;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        if !found {
                            return Err(MongrelError::Conflict(format!(
                                "FOREIGN KEY '{}' on table '{tname}' has no matching parent in '{}'",
                                fk.name, fk.ref_table
                            )));
                        }
                    }

                    // Parent-side ON UPDATE RESTRICT. CASCADE/SET NULL were
                    // expanded in Phase A; here the final child write set is
                    // known, so a child explicitly moved/deleted by this same
                    // transaction does not cause a false violation.
                    if let Staged::Update(row_id, _) = op {
                        let parent_handle = self.table_by_id(*table_id)?;
                        let Some(old_parent) = parent_handle.lock().get(*row_id, snapshot) else {
                            continue;
                        };
                        for (child_id, child_name, child_schema) in &live {
                            for fk in &child_schema.constraints.foreign_keys {
                                if fk.ref_table != tname || fk.on_update != FkAction::Restrict {
                                    continue;
                                }
                                let Some(old_key) =
                                    encode_composite_key(&fk.ref_columns, &old_parent.columns)
                                else {
                                    continue;
                                };
                                if encode_composite_key(&fk.ref_columns, &cells_map).as_deref()
                                    == Some(old_key.as_slice())
                                {
                                    continue;
                                }
                                for child in load_rows(*child_id)? {
                                    if encode_composite_key(&fk.columns, &child.columns).as_deref()
                                        != Some(old_key.as_slice())
                                    {
                                        continue;
                                    }
                                    let replacement = staging.iter().find_map(|(id, op)| {
                                        if *id != *child_id {
                                            return None;
                                        }
                                        match op {
                                            Staged::Delete(id) if *id == child.row_id => Some(None),
                                            Staged::Update(id, cells) if *id == child.row_id => {
                                                let map: HashMap<u16, Value> =
                                                    cells.iter().cloned().collect();
                                                Some(encode_composite_key(&fk.columns, &map))
                                            }
                                            _ => None,
                                        }
                                    });
                                    if replacement.is_some_and(|key| {
                                        key.as_deref() != Some(old_key.as_slice())
                                    }) {
                                        continue;
                                    }
                                    return Err(MongrelError::Conflict(format!(
                                        "FOREIGN KEY '{}' on table '{child_name}' restricts update (parent key referenced)",
                                        fk.name
                                    )));
                                }
                            }
                        }
                    }
                }
                Staged::Delete(rid) => {
                    // FK ON DELETE RESTRICT: a child row (whose FK action is
                    // RESTRICT) referencing this parent blocks the delete.
                    // CASCADE/SET NULL children were expanded in Phase A.
                    let parent_handle = self.table_by_id(*table_id)?;
                    let Some(parent_row) = parent_handle.lock().get(*rid, snapshot) else {
                        continue;
                    };
                    for (child_id, child_name, child_schema) in &live {
                        for fk in &child_schema.constraints.foreign_keys {
                            if fk.ref_table != tname || fk.on_delete != FkAction::Restrict {
                                continue;
                            }
                            let Some(parent_key) =
                                encode_composite_key(&fk.ref_columns, &parent_row.columns)
                            else {
                                continue;
                            };
                            let child_rows = load_rows(*child_id)?;
                            for r in &child_rows {
                                // A child already being deleted by this txn
                                // (cascade/inline) is not a restrict violation.
                                if staged_deletes.contains(&(*child_id, r.row_id.0)) {
                                    continue;
                                }
                                if let Some(ck) = encode_composite_key(&fk.columns, &r.columns) {
                                    if ck == parent_key {
                                        return Err(MongrelError::Conflict(format!(
                                            "FOREIGN KEY '{}' on table '{child_name}' restricts delete (parent referenced)",
                                            fk.name
                                        )));
                                    }
                                }
                            }
                        }
                    }
                }
                Staged::Truncate => {
                    // Truncate is RESTRICT-only: reject if any child references
                    // this table (any FK action), since cascade-truncate is
                    // unsupported.
                    for (child_id, child_name, child_schema) in &live {
                        for fk in &child_schema.constraints.foreign_keys {
                            if fk.ref_table != tname {
                                continue;
                            }
                            let child_rows = load_rows(*child_id)?;
                            if child_rows
                                .iter()
                                .any(|r| encode_composite_key(&fk.columns, &r.columns).is_some())
                            {
                                return Err(MongrelError::Conflict(format!(
                                    "FOREIGN KEY '{}' on table '{child_name}' restricts truncate of '{tname}'",
                                    fk.name
                                )));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_security_writes(
        &self,
        staging: &[(u64, crate::txn::Staged)],
        read_epoch: Epoch,
        explicit_principal: Option<&crate::auth::Principal>,
    ) -> Result<()> {
        use crate::security::PolicyCommand;
        use crate::txn::Staged;

        let catalog = self.catalog.read();
        if catalog.security.rls_tables.is_empty() {
            return Ok(());
        }
        let security = catalog.security.clone();
        let table_names = catalog
            .tables
            .iter()
            .filter(|entry| matches!(entry.state, TableState::Live))
            .map(|entry| (entry.table_id, entry.name.clone()))
            .collect::<HashMap<_, _>>();
        drop(catalog);
        if !staging.iter().any(|(table_id, _)| {
            table_names
                .get(table_id)
                .is_some_and(|table| security.rls_enabled(table))
        }) {
            return Ok(());
        }
        let cached = self.principal.read().clone();
        let principal = explicit_principal
            .or(cached.as_ref())
            .ok_or(MongrelError::AuthRequired)?;

        for (table_id, operation) in staging {
            let Some(table) = table_names.get(table_id) else {
                continue;
            };
            if !security.rls_enabled(table) || principal.is_admin {
                continue;
            }
            let denied = |command| MongrelError::PermissionDenied {
                required: match command {
                    PolicyCommand::Insert => crate::auth::Permission::Insert {
                        table: table.clone(),
                    },
                    PolicyCommand::Update => crate::auth::Permission::Update {
                        table: table.clone(),
                    },
                    PolicyCommand::Delete | PolicyCommand::All | PolicyCommand::Select => {
                        crate::auth::Permission::Delete {
                            table: table.clone(),
                        }
                    }
                },
                principal: principal.username.clone(),
            };
            match operation {
                Staged::Put(cells) => {
                    let mut row = crate::memtable::Row::new(RowId(0), Epoch(read_epoch.0));
                    row.columns.extend(cells.iter().cloned());
                    if !security.row_allowed(table, PolicyCommand::Insert, &row, principal, true) {
                        return Err(denied(PolicyCommand::Insert));
                    }
                }
                Staged::Update(row_id, cells) => {
                    let old = self
                        .table_by_id(*table_id)?
                        .lock()
                        .get(*row_id, Snapshot::at(read_epoch))
                        .ok_or_else(|| {
                            MongrelError::NotFound(format!("row {} not found", row_id.0))
                        })?;
                    if !security.row_allowed(table, PolicyCommand::Update, &old, principal, false) {
                        return Err(denied(PolicyCommand::Update));
                    }
                    let mut new = crate::memtable::Row::new(*row_id, Epoch(read_epoch.0));
                    new.columns.extend(cells.iter().cloned());
                    if !security.row_allowed(table, PolicyCommand::Update, &new, principal, true) {
                        return Err(denied(PolicyCommand::Update));
                    }
                }
                Staged::Delete(row_id) => {
                    let old = self
                        .table_by_id(*table_id)?
                        .lock()
                        .get(*row_id, Snapshot::at(read_epoch))
                        .ok_or_else(|| {
                            MongrelError::NotFound(format!("row {} not found", row_id.0))
                        })?;
                    if !security.row_allowed(table, PolicyCommand::Delete, &old, principal, false) {
                        return Err(denied(PolicyCommand::Delete));
                    }
                }
                Staged::Truncate => return Err(denied(PolicyCommand::Delete)),
            }
        }
        Ok(())
    }

    /// Seal a transaction (spec §9.3):
    /// 1. Prepare — derive write keys, allocate row ids (brief table locks).
    /// 2. Sequencer — validate-first under the WAL mutex; abort on conflict
    ///    with no epoch consumed; assign epoch, append data records + TxnCommit,
    ///    group-sync, record conflict keys.
    /// 3. Publish — apply to tables, advance visible in-order.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn commit_transaction_with_external_states(
        &self,
        txn_id: u64,
        read_epoch: Epoch,
        mut staging: Vec<(u64, crate::txn::Staged)>,
        external_states: Vec<(String, Vec<u8>)>,
        materialized_view_updates: Vec<crate::catalog::MaterializedViewEntry>,
        security_principal: Option<crate::auth::Principal>,
        external_trigger_bridge: Option<&dyn ExternalTriggerBridge>,
    ) -> Result<Epoch> {
        use crate::memtable::Row;
        use crate::txn::{Staged, StagedOp, WriteKey};
        use crate::wal::Op;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        use std::sync::atomic::Ordering;

        if self.read_only {
            return Err(MongrelError::ReadOnlyReplica);
        }
        let _replication_guard = self.replication_barrier.read();
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }
        let mut external_states = dedup_external_states(external_states);
        if !external_states.is_empty() {
            let cat = self.catalog.read();
            for (name, _) in &external_states {
                if !cat.external_tables.iter().any(|entry| entry.name == *name) {
                    return Err(MongrelError::NotFound(format!(
                        "external table {name:?} not found"
                    )));
                }
            }
        }
        let prepared_materialized_views = {
            let mut deduplicated = HashMap::new();
            for definition in materialized_view_updates {
                if definition.name.is_empty() || definition.query.trim().is_empty() {
                    return Err(MongrelError::InvalidArgument(
                        "materialized view name and query must not be empty".into(),
                    ));
                }
                deduplicated.insert(definition.name.clone(), definition);
            }
            let catalog = self.catalog.read();
            let mut prepared = Vec::with_capacity(deduplicated.len());
            for definition in deduplicated.into_values() {
                let table_id = catalog
                    .live(&definition.name)
                    .ok_or_else(|| {
                        MongrelError::NotFound(format!(
                            "materialized view table {:?} not found",
                            definition.name
                        ))
                    })?
                    .table_id;
                prepared.push((table_id, definition));
            }
            prepared.sort_by(|left, right| left.1.name.cmp(&right.1.name));
            prepared
        };

        // ── 1. Prepare: fill generated values, expand triggers, validate, then
        // derive write keys from the final atomic write set.
        self.fill_auto_increment_for_staging(&mut staging)?;
        self.expand_table_triggers(
            &mut staging,
            read_epoch,
            external_trigger_bridge,
            &mut external_states,
        )?;
        self.fill_auto_increment_for_staging(&mut staging)?;
        external_states = dedup_external_states(external_states);

        // Validate declarative constraints (unique / FK / check) under the read
        // snapshot, outside the WAL mutex. Trigger-produced writes are included
        // here, so the batch either satisfies every declared constraint or is
        // rejected atomically.
        self.validate_constraints(&mut staging, read_epoch)?;
        self.validate_security_writes(&staging, read_epoch, security_principal.as_ref())?;
        let mut normalized = Vec::with_capacity(staging.len() * 2);
        for (table_id, op) in staging {
            match op {
                crate::txn::Staged::Update(row_id, cells) => {
                    normalized.push((table_id, crate::txn::Staged::Delete(row_id)));
                    normalized.push((table_id, crate::txn::Staged::Put(cells)));
                }
                op => normalized.push((table_id, op)),
            }
        }
        staging = normalized;
        let has_changes = !staging.is_empty()
            || !external_states.is_empty()
            || !prepared_materialized_views.is_empty();
        let truncated_tables: HashSet<u64> = staging
            .iter()
            .filter_map(|(table_id, op)| matches!(op, Staged::Truncate).then_some(*table_id))
            .collect();

        let write_keys = {
            let cat = self.catalog.read();
            let mut keys: Vec<WriteKey> = Vec::new();
            for (table_id, staged) in &staging {
                match staged {
                    Staged::Put(cells) => {
                        if let Some(entry) = cat.tables.iter().find(|t| t.table_id == *table_id) {
                            for col in &entry.schema.columns {
                                if col.flags.contains(crate::schema::ColumnFlags::PRIMARY_KEY) {
                                    if let Some((_, val)) =
                                        cells.iter().find(|(id, _)| *id == col.id)
                                    {
                                        let mut h = DefaultHasher::new();
                                        val.encode_key().hash(&mut h);
                                        keys.push(WriteKey::Unique {
                                            table_id: *table_id,
                                            index_id: 0,
                                            key_hash: h.finish(),
                                        });
                                    }
                                }
                            }
                            // Declared non-PK unique constraints register a
                            // `WriteKey::Unique` (namespace-separated from the
                            // PK's index_id==0 by setting the high bit) so two
                            // concurrent transactions inserting the same key
                            // cannot both commit. Rows with any NULL constrained
                            // column are skipped (SQL semantics).
                            for uc in &entry.schema.constraints.uniques {
                                if let Some(key_bytes) = crate::constraint::encode_composite_key(
                                    &uc.columns,
                                    &cells.iter().cloned().collect(),
                                ) {
                                    let mut h = DefaultHasher::new();
                                    key_bytes.hash(&mut h);
                                    keys.push(WriteKey::Unique {
                                        table_id: *table_id,
                                        index_id: uc.id | 0x8000,
                                        key_hash: h.finish(),
                                    });
                                }
                            }
                        }
                    }
                    Staged::Delete(rid) => keys.push(WriteKey::Row {
                        table_id: *table_id,
                        row_id: rid.0,
                    }),
                    Staged::Truncate => keys.push(WriteKey::Table {
                        table_id: *table_id,
                    }),
                    Staged::Update(_, _) => unreachable!("updates normalized before prepare"),
                }
            }
            for (name, _) in &external_states {
                let mut h = DefaultHasher::new();
                name.hash(&mut h);
                keys.push(WriteKey::Unique {
                    table_id: EXTERNAL_TABLE_ID,
                    index_id: 0,
                    key_hash: h.finish(),
                });
            }
            keys
        };

        // Opportunistic pruning.
        let min_active = self.active_txns.min_read_epoch();
        if min_active < u64::MAX {
            self.conflicts.prune_below(Epoch(min_active));
        }

        // ── 1a. Pre-validate the full write-set OUTSIDE the sequencer (spec
        // §8.5, review fix #17). Snapshot the conflict-index version so the
        // sequencer only re-checks if new commits arrived in the interim.
        if self.conflicts.conflicts(&write_keys, read_epoch) {
            return Err(MongrelError::Conflict(
                "write-write conflict (pre-validate, first-committer-wins)".into(),
            ));
        }
        let pre_validate_version = self.conflicts.version();

        // ── 1b. Spill: if a table's staged puts exceed the threshold, write a
        // uniform-epoch pending run (spec §8.5). Rows in the run are NOT
        // streamed as Put records; they are linked at publish time.
        let mut spilled: Vec<SpilledRun> = Vec::new();
        let mut spilled_tables: std::collections::HashSet<u64> = std::collections::HashSet::new();
        // Protect this txn's `_txn/<id>/` dir from a concurrent `gc()` for as long
        // as the spill runs are live (registered on first spill, dropped at the
        // end of this function on commit/abort/error).
        let mut spill_guard: Option<crate::retention::SpillGuard> = None;
        {
            let mut table_bytes: HashMap<u64, usize> = HashMap::new();
            for (table_id, staged) in &staging {
                if let Staged::Put(cells) = staged {
                    *table_bytes.entry(*table_id).or_default() += cells.len() * 16;
                }
            }
            let tables = self.tables.read();
            for (&table_id, &bytes) in &table_bytes {
                if bytes as u64
                    <= self
                        .spill_threshold
                        .load(std::sync::atomic::Ordering::Relaxed)
                {
                    continue;
                }
                let Some(handle) = tables.get(&table_id) else {
                    continue;
                };
                spill_guard.get_or_insert_with(|| self.active_spills.register(txn_id));
                let mut t = handle.lock();
                let tdir = t.table_dir().to_path_buf();
                let txn_dir = tdir.join("_txn").join(txn_id.to_string());
                std::fs::create_dir_all(&txn_dir)?;
                let run_id = t.alloc_run_id() as u128;
                let pending_path = txn_dir.join(format!("r-{run_id}.sr"));

                let mut rows: Vec<Row> = Vec::new();
                for (tid, staged) in &staging {
                    if *tid != table_id {
                        continue;
                    }
                    if let Staged::Put(cells) = staged {
                        t.validate_cells_not_null(cells)?;
                        let row_id = t.alloc_row_id();
                        let mut row = Row::new(row_id, Epoch(0));
                        for (c, v) in cells {
                            row.columns.insert(*c, v.clone());
                        }
                        rows.push(row);
                    }
                }
                let schema = t.schema_ref().clone();
                let kek = t.kek_ref().cloned();
                let specs = t.indexable_column_specs();
                drop(t);

                let mut writer = crate::sorted_run::RunWriter::new(&schema, run_id, Epoch(0), 0)
                    .uniform_epoch(true);
                if let Some(ref kek) = kek {
                    writer = writer.with_encryption(kek.as_ref(), specs);
                }
                let header = writer.write(&pending_path, &rows)?;
                let row_count = header.row_count;
                let min_rid = rows.first().map(|r| r.row_id.0).unwrap_or(0);
                let max_rid = rows.last().map(|r| r.row_id.0).unwrap_or(0);

                spilled.push(SpilledRun {
                    table_id,
                    run_id,
                    pending_path,
                    rows,
                    row_count,
                    min_rid,
                    max_rid,
                });
                spilled_tables.insert(table_id);
            }
        }

        // Test seam: let a test race `gc()` against this in-flight spill.
        if spill_guard.is_some() {
            if let Some(hook) = self.spill_hook.lock().as_ref() {
                hook();
            }
        }

        // ── 1c. Pre-build non-spilled put rows OUTSIDE the WAL critical section.
        // Allocating row ids + building the rows here (lock order: table handle →
        // nothing) means the sequencer never locks a table handle while holding
        // the shared-WAL mutex. That matters because `Table::commit`/`flush` lock
        // the table handle THEN the shared WAL; if the sequencer did the reverse
        // (WAL then handle) the two paths would deadlock (review fix: B1).
        // Aligned 1:1 with `staging`; `None` for deletes and spilled puts.
        // Row ids are allocated here, before the sequencer's delta conflict
        // re-check, so a losing txn leaks the ids it reserved — harmless, the
        // u64 row-id space is monotonic and gaps are expected (spills do the same).
        let mut prebuilt: Vec<Option<Row>> = Vec::with_capacity(staging.len());
        let mut delete_images: Vec<Option<Row>> = Vec::with_capacity(staging.len());
        {
            let tables = self.tables.read();
            for (table_id, staged) in &staging {
                match staged {
                    Staged::Put(cells) if !spilled_tables.contains(table_id) => {
                        let handle = tables.get(table_id).ok_or_else(|| {
                            MongrelError::NotFound(format!("table {table_id} not mounted"))
                        })?;
                        let mut t = handle.lock();
                        t.validate_cells_not_null(cells)?;
                        let row_id = t.alloc_row_id();
                        drop(t);
                        let mut row = Row::new(row_id, Epoch(0));
                        for (c, v) in cells {
                            row.columns.insert(*c, v.clone());
                        }
                        prebuilt.push(Some(row));
                        delete_images.push(None);
                    }
                    Staged::Delete(row_id) => {
                        let before = tables.get(table_id).and_then(|handle| {
                            handle.lock().get(*row_id, Snapshot::at(read_epoch))
                        });
                        prebuilt.push(None);
                        delete_images.push(before);
                    }
                    Staged::Put(_) | Staged::Truncate => {
                        prebuilt.push(None);
                        delete_images.push(None);
                    }
                    Staged::Update(_, _) => unreachable!("updates normalized before prepare"),
                }
            }
        }

        let mut prepared_external = Vec::with_capacity(external_states.len());
        for (name, state) in &external_states {
            let pending = prepare_external_state_file(&self.root, name, state, txn_id)?;
            prepared_external.push((name.clone(), state.clone(), pending));
        }

        // ── 2. Sequencer: validate-first → assign → append → sync → record ──
        let added_runs: Vec<crate::wal::AddedRun> = spilled
            .iter()
            .map(|s| crate::wal::AddedRun {
                table_id: s.table_id,
                run_id: s.run_id,
                row_count: s.row_count,
                level: 0,
                min_row_id: s.min_rid,
                max_row_id: s.max_rid,
                content_hash: [0u8; 32],
            })
            .collect();
        let (new_epoch, mut _epoch_guard, applies, committed_materialized_views, commit_seq) = {
            let mut wal = self.shared_wal.lock();

            // Re-check only if the conflict index advanced since pre-validation
            // (bounded delta — spec §8.5, review fix #17). If the version is
            // unchanged, the pre-check result is still valid and the sequencer
            // does O(1) work regardless of write-set size.
            if self.conflicts.version() != pre_validate_version
                && self.conflicts.conflicts(&write_keys, read_epoch)
            {
                // Abort: this txn assigned no epoch yet, so drop the quarantined
                // spill runs we wrote during prepare instead of leaking them in
                // `_txn/` until the next GC/reopen sweep.
                drop(wal);
                for s in &spilled {
                    if let Some(parent) = s.pending_path.parent() {
                        let _ = std::fs::remove_dir_all(parent);
                    }
                }
                for (_, _, pending) in &prepared_external {
                    let _ = std::fs::remove_file(pending);
                }
                return Err(MongrelError::Conflict(
                    "write-write conflict (sequencer delta re-check)".into(),
                ));
            }

            let new_epoch = self.epoch.bump_assigned();
            let _epoch_guard = EpochGuard::new(self.epoch.as_ref(), new_epoch);
            let mut applies: Vec<(u64, Vec<StagedOp>)> = Vec::new();
            let mut committed_materialized_views = Vec::new();

            for (idx, (table_id, staged)) in staging.iter().enumerate() {
                // Skip puts for tables that were spilled — their data is in a
                // pending run, not in streamed Put records.
                if spilled_tables.contains(table_id) && matches!(staged, Staged::Put(_)) {
                    continue;
                }
                let mut ops = Vec::new();
                match staged {
                    Staged::Put(_) => {
                        // Stamp the pre-built row at the real assigned epoch.
                        let mut row = prebuilt[idx].take().expect("prebuilt put row");
                        row.committed_epoch = new_epoch;
                        let payload = bincode::serialize(&vec![row.clone()])
                            .map_err(|e| MongrelError::Other(format!("row serialize: {e}")))?;
                        wal.append(
                            txn_id,
                            *table_id,
                            Op::Put {
                                table_id: *table_id,
                                rows: payload,
                            },
                        )?;
                        ops.push(StagedOp::Put(row));
                    }
                    Staged::Delete(rid) => {
                        if let Some(before) = &delete_images[idx] {
                            wal.append(
                                txn_id,
                                *table_id,
                                Op::BeforeImage {
                                    table_id: *table_id,
                                    row_id: *rid,
                                    row: bincode::serialize(before).map_err(|error| {
                                        MongrelError::Other(format!(
                                            "before-image serialize: {error}"
                                        ))
                                    })?,
                                },
                            )?;
                        }
                        wal.append(
                            txn_id,
                            *table_id,
                            Op::Delete {
                                table_id: *table_id,
                                row_ids: vec![*rid],
                            },
                        )?;
                        ops.push(StagedOp::Delete(*rid));
                    }
                    Staged::Truncate => {
                        wal.append(
                            txn_id,
                            *table_id,
                            Op::TruncateTable {
                                table_id: *table_id,
                            },
                        )?;
                        ops.push(StagedOp::Truncate);
                    }
                    Staged::Update(_, _) => unreachable!("updates normalized before sequencer"),
                }
                applies.push((*table_id, ops));
            }

            for (name, state, _) in &prepared_external {
                wal.append(
                    txn_id,
                    EXTERNAL_TABLE_ID,
                    Op::ExternalTableState {
                        name: name.clone(),
                        state: state.clone(),
                    },
                )?;
            }

            for (table_id, definition) in &prepared_materialized_views {
                let mut definition = definition.clone();
                definition.last_refresh_epoch = new_epoch.0;
                wal.append(
                    txn_id,
                    *table_id,
                    Op::Ddl(crate::wal::DdlOp::SetMaterializedView {
                        name: definition.name.clone(),
                        definition_json: crate::wal::DdlOp::encode_materialized_view(&definition)?,
                    }),
                )?;
                committed_materialized_views.push(definition);
            }

            let commit_seq = wal.append_commit(txn_id, new_epoch, &added_runs)?;

            // Record the conflict + assign the epoch under the WAL lock so commit
            // order == WAL append order, but DO NOT fsync here (P3.2): the fsync
            // moves out of this critical section to the group-commit coordinator
            // so concurrent committers share a single leader fsync.
            self.conflicts.record(&write_keys, new_epoch);
            (
                new_epoch,
                _epoch_guard,
                applies,
                committed_materialized_views,
                commit_seq,
            )
        };

        // ── 2b. Durability: one leader fsync serves this whole batch (P3.2). ──
        self.group
            .await_durable(&self.shared_wal, commit_seq)
            .inspect_err(|_| {
                self.poisoned.store(true, Ordering::Relaxed);
            })?;

        // ── 3. Publish: apply non-spilled ops + link spilled runs ──
        {
            let tables = self.tables.read();
            // Apply truncates/deletes before linking spilled replacement rows.
            // This makes TRUNCATE + INSERT a single atomic replace even when
            // the insert side exceeds the spill threshold.
            for (table_id, ops) in applies {
                if let Some(handle) = tables.get(&table_id) {
                    let mut t = handle.lock();
                    for op in ops {
                        match op {
                            StagedOp::Put(row) => t.apply_put_rows(vec![row])?,
                            StagedOp::Delete(rid) => t.apply_delete(rid, new_epoch),
                            StagedOp::Truncate => t.apply_truncate(new_epoch)?,
                        }
                    }
                    t.invalidate_pending_cache();
                    t.persist_manifest(new_epoch)?;
                }
            }
            for s in &spilled {
                if let Some(handle) = tables.get(&s.table_id) {
                    let mut t = handle.lock();
                    let dest = t.run_path(s.run_id as u64);
                    std::fs::rename(&s.pending_path, &dest)?;
                    if let Some(parent) = s.pending_path.parent() {
                        let _ = std::fs::remove_dir_all(parent);
                    }
                    t.link_run(crate::manifest::RunRef {
                        run_id: s.run_id,
                        level: 0,
                        epoch_created: new_epoch.0,
                        row_count: s.row_count,
                    });
                    t.apply_run_metadata(&s.rows)?;
                    if truncated_tables.contains(&s.table_id) {
                        // TRUNCATE + spilled puts fully describe this table at
                        // the commit epoch. Endorse the epoch so clean-reopen
                        // recovery does not replay the truncate over the
                        // already-linked replacement run.
                        t.set_flushed_epoch(new_epoch);
                    }
                    t.invalidate_pending_cache();
                    t.persist_manifest(new_epoch)?;
                }
            }
        }
        for (name, _, pending) in &prepared_external {
            publish_external_state_file(&self.root, name, pending)?;
        }
        if !committed_materialized_views.is_empty() {
            {
                let mut catalog = self.catalog.write();
                for definition in committed_materialized_views {
                    if let Some(existing) = catalog
                        .materialized_views
                        .iter_mut()
                        .find(|existing| existing.name == definition.name)
                    {
                        *existing = definition;
                    } else {
                        catalog.materialized_views.push(definition);
                    }
                }
                catalog.db_epoch = catalog.db_epoch.max(new_epoch.0);
            }
            catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        }

        self.epoch.publish_in_order(new_epoch);
        if has_changes {
            let _ = self.change_wake.send(());
        }
        _epoch_guard.disarm();
        Ok(new_epoch)
    }

    /// Register a read snapshot at the current visible epoch and return it with
    /// a guard that retains it for GC until dropped.
    pub fn snapshot(&self) -> (Snapshot, SnapshotGuard<'_>) {
        let e = self.epoch.visible();
        let g = self.snapshots.register(e);
        (Snapshot::at(e), g)
    }

    /// Owned (clonable-handle) variant of [`Self::snapshot`] for cross-thread
    /// retention.
    pub fn snapshot_owned(&self) -> (Snapshot, OwnedSnapshotGuard) {
        let e = self.epoch.visible();
        let g = self.snapshots.register_owned(e);
        (Snapshot::at(e), g)
    }

    /// Configure a rolling history window measured in prior commit epochs.
    /// The first enable starts at the current epoch because earlier versions
    /// may already have been compacted. Increasing the window likewise cannot
    /// recreate history that fell outside the previous guarantee.
    pub fn set_history_retention_epochs(&self, epochs: u64) -> Result<()> {
        let _guard = self.ddl_lock.lock();
        let current = self.epoch.visible();
        let (old_epochs, old_start) = self.snapshots.history_config();
        let earliest_already_guaranteed = if old_epochs == 0 {
            current
        } else {
            Epoch(old_start.0.max(current.0.saturating_sub(old_epochs)))
        };
        let start = if epochs == 0 {
            current
        } else {
            earliest_already_guaranteed
        };
        write_history_retention(&self.root, epochs, start)?;
        self.snapshots.configure_history(epochs, start);
        Ok(())
    }

    pub fn history_retention_epochs(&self) -> u64 {
        self.snapshots.history_config().0
    }

    pub fn earliest_retained_epoch(&self) -> Epoch {
        let current = self.epoch.visible();
        self.snapshots.history_floor(current).unwrap_or(current)
    }

    /// Pin a guaranteed historical epoch for the lifetime of the returned
    /// guard. Rejects future epochs and epochs outside the configured window.
    pub fn snapshot_at_owned(&self, epoch: Epoch) -> Result<(Snapshot, OwnedSnapshotGuard)> {
        let current = self.epoch.visible();
        if epoch > current {
            return Err(MongrelError::InvalidArgument(format!(
                "epoch {} is in the future; current epoch is {}",
                epoch.0, current.0
            )));
        }
        let earliest = self.earliest_retained_epoch();
        if epoch < earliest {
            return Err(MongrelError::InvalidArgument(format!(
                "epoch {} is no longer retained; earliest available epoch is {}",
                epoch.0, earliest.0
            )));
        }
        let guard = self.snapshots.register_owned(epoch);
        Ok((Snapshot::at(epoch), guard))
    }

    /// Names of all live tables.
    pub fn table_names(&self) -> Vec<String> {
        self.catalog
            .read()
            .tables
            .iter()
            .filter(|t| matches!(t.state, TableState::Live))
            .map(|t| t.name.clone())
            .collect()
    }

    /// Best-effort flush-on-close (§4.4): force-flush every mounted table
    /// that has pending writes to a `.sr` sorted run, so WAL segments can be
    /// reaped on the next open. Call this as the last action before a
    /// short-lived process (CLI, one-shot script) exits. The daemon does not
    /// need this — its background auto-compactor handles run management.
    pub fn close(&self) -> Result<()> {
        for name in self.table_names() {
            if let Ok(handle) = self.table(&name) {
                if let Err(e) = handle.lock().close() {
                    eprintln!("[close] flush failed for {name}: {e}");
                }
            }
        }
        Ok(())
    }

    /// Compact every mounted table: merge all sorted runs into one clean run
    /// so query cost stays flat (single-run fast path) instead of growing
    /// with run count. Tables with < 2 runs are skipped unless TTL has expired
    /// rows to reclaim. Each table
    /// is locked individually for its own compaction; snapshot retention is
    /// honored by `Table::compact`. Returns `(tables_compacted, tables_skipped)`.
    pub fn compact(&self) -> Result<(usize, usize)> {
        self.require(&crate::auth::Permission::Ddl)?;
        let mut compacted = 0;
        let mut skipped = 0;
        for name in self.table_names() {
            let Ok(handle) = self.table(&name) else {
                continue;
            };
            {
                let mut t = handle.lock();
                let before = t.run_count();
                if before < 2 && !t.should_compact() {
                    skipped += 1;
                    continue;
                }
                match t.compact() {
                    Ok(()) => {
                        let after = t.run_count();
                        compacted += 1;
                        eprintln!("[compact] {name}: {before} -> {after} runs");
                    }
                    Err(e) => {
                        eprintln!("[compact] {name}: compaction failed: {e}");
                        skipped += 1;
                    }
                }
            }
        }
        Ok((compacted, skipped))
    }

    /// Compact a single table by name. Returns `Ok(true)` if it was
    /// compacted, `Ok(false)` if skipped (< 2 runs).
    pub fn compact_table(&self, name: &str) -> Result<bool> {
        self.require(&crate::auth::Permission::Ddl)?;
        let handle = self.table(name)?;
        let mut t = handle.lock();
        let before = t.run_count();
        if before < 2 {
            return Ok(false);
        }
        t.compact()?;
        Ok(t.run_count() < before)
    }

    /// Look up a live table by name.
    pub fn table(&self, name: &str) -> Result<TableHandle> {
        let cat = self.catalog.read();
        let entry = cat
            .live(name)
            .ok_or_else(|| MongrelError::NotFound(format!("table {name:?} not found")))?;
        let id = entry.table_id;
        drop(cat);
        self.tables
            .read()
            .get(&id)
            .cloned()
            .ok_or_else(|| MongrelError::NotFound(format!("table {name:?} not mounted")))
    }

    /// Whether any mounted table has wall-clock TTL retention. SQL sessions
    /// use this to avoid epoch-keyed result caches that can outlive a cutoff.
    pub fn has_ttl_tables(&self) -> bool {
        self.tables
            .read()
            .values()
            .any(|table| table.lock().ttl().is_some())
    }

    /// Resolve a live table id → mounted handle (used by the constraint
    /// validation pass and other id-qualified internal paths).
    fn table_by_id(&self, id: u64) -> Result<TableHandle> {
        self.tables
            .read()
            .get(&id)
            .cloned()
            .ok_or_else(|| MongrelError::NotFound(format!("table id {id} not mounted")))
    }

    /// Create a new table. The DDL is first logged to the shared WAL
    /// (`Op::Ddl(CreateTable)` + `TxnCommit`) and group-synced so it is durable
    /// BEFORE the in-memory catalog and table map are mutated; the catalog
    /// checkpoint is rewritten afterwards (spec §15, review fix #16). A reopen
    /// that sees a stale catalog still recovers the table by replaying the Ddl.
    pub fn create_table(&self, name: &str, schema: Schema) -> Result<u64> {
        use crate::wal::DdlOp;
        use std::sync::atomic::Ordering;

        self.require(&crate::auth::Permission::Ddl)?;
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }

        let _g = self.ddl_lock.lock();
        {
            let cat = self.catalog.read();
            if cat.live(name).is_some() {
                return Err(MongrelError::InvalidArgument(format!(
                    "table {name:?} already exists"
                )));
            }
        }

        // Allocate id + epoch + txn id under the commit lock so the DDL commit
        // is serialized with data commits (in-order publish).
        let commit_lock = Arc::clone(&self.commit_lock);
        let _c = commit_lock.lock();
        let table_id = {
            let mut cat = self.catalog.write();
            let id = cat.next_table_id;
            cat.next_table_id += 1;
            id
        };
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let txn_id = self.alloc_txn_id();

        // Stamp the schema_id with the unique table_id so every table in the
        // database has a distinct schema_id (caller-provided values are
        // ignored to prevent collisions).
        let mut schema = schema;
        schema.schema_id = table_id;
        // Defense in depth: reject an invalid schema BEFORE any durable
        // side-effect. `Table::create_in` re-validates, but by then the DDL has
        // already been appended to the shared WAL; a failing create_in would
        // leave a dangling entry that `recover_ddl_from_wal` replays without
        // re-validating, corrupting the catalog on reopen. Validating here
        // keeps the WAL free of schemas that can never be opened.
        schema.validate_auto_increment()?;
        schema.validate_defaults()?;
        schema.validate_ai()?;
        for index in &schema.indexes {
            index.validate_options()?;
        }
        for constraint in &schema.constraints.checks {
            constraint.expr.validate()?;
        }

        // 1. Log the DDL + commit marker to the shared WAL, then make it durable
        //    via the group-commit coordinator (no fsync under the WAL lock — P3.2).
        let schema_json = DdlOp::encode_schema(&schema)?;
        let commit_seq = {
            let mut wal = self.shared_wal.lock();
            wal.append(
                txn_id,
                table_id,
                crate::wal::Op::Ddl(DdlOp::CreateTable {
                    table_id,
                    name: name.to_string(),
                    schema_json,
                }),
            )?;
            wal.append_commit(txn_id, epoch, &[])?
        };
        self.group
            .await_durable(&self.shared_wal, commit_seq)
            .inspect_err(|_| {
                self.poisoned.store(true, Ordering::Relaxed);
            })?;

        // 2. Create the on-disk table dir + manifest.
        let tdir = self.root.join(TABLES_DIR).join(table_id.to_string());
        std::fs::create_dir_all(&tdir)?;
        let ctx = SharedCtx {
            epoch: Arc::clone(&self.epoch),
            page_cache: Arc::clone(&self.page_cache),
            decoded_cache: Arc::clone(&self.decoded_cache),
            snapshots: Arc::clone(&self.snapshots),
            kek: self.kek.clone(),
            commit_lock: Arc::clone(&self.commit_lock),
            shared: Some(crate::engine::SharedWalCtx {
                wal: Arc::clone(&self.shared_wal),
                group: Arc::clone(&self.group),
                poisoned: Arc::clone(&self.poisoned),
                txn_ids: Arc::clone(&self.next_txn_id),
                change_wake: self.change_wake.clone(),
            }),
            table_name: Some(name.to_string()),
            auth: self.table_auth_checker(),
            read_only: self.read_only,
        };
        let table = Table::create_in(&tdir, schema.clone(), table_id, ctx)?;

        // 3. Mutate the in-memory catalog + mount the table, then rewrite the
        //    catalog checkpoint (lazy: outside the WAL critical section).
        {
            let mut cat = self.catalog.write();
            cat.tables.push(CatalogEntry {
                table_id,
                name: name.to_string(),
                schema,
                state: TableState::Live,
                created_epoch: epoch.0,
            });
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.tables
            .write()
            .insert(table_id, Arc::new(Mutex::new(table)));

        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(table_id)
    }

    /// Logically drop a table, logging the DDL through the shared WAL first.
    pub fn drop_table(&self, name: &str) -> Result<()> {
        use crate::wal::DdlOp;
        use std::sync::atomic::Ordering;

        self.require(&crate::auth::Permission::Ddl)?;
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }

        let _g = self.ddl_lock.lock();
        let table_id = {
            let cat = self.catalog.read();
            cat.live(name)
                .ok_or_else(|| MongrelError::NotFound(format!("table {name:?} not found")))?
                .table_id
        };

        let commit_lock = Arc::clone(&self.commit_lock);
        let _c = commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let txn_id = self.alloc_txn_id();
        let commit_seq = {
            let mut wal = self.shared_wal.lock();
            wal.append(
                txn_id,
                table_id,
                crate::wal::Op::Ddl(DdlOp::DropTable { table_id }),
            )?;
            wal.append_commit(txn_id, epoch, &[])?
        };
        self.group
            .await_durable(&self.shared_wal, commit_seq)
            .inspect_err(|_| {
                self.poisoned.store(true, Ordering::Relaxed);
            })?;

        {
            let mut cat = self.catalog.write();
            let entry = cat
                .tables
                .iter_mut()
                .find(|t| t.table_id == table_id)
                .ok_or_else(|| MongrelError::NotFound(format!("table {name:?} not found")))?;
            entry.state = TableState::Dropped { at_epoch: epoch.0 };
            cat.triggers.retain(|trigger| {
                !matches!(
                    &trigger.trigger.target,
                    TriggerTarget::Table(target) if target == name
                )
            });
            cat.materialized_views
                .retain(|definition| definition.name != name);
            cat.security.rls_tables.retain(|table| table != name);
            cat.security.policies.retain(|policy| policy.table != name);
            cat.security.masks.retain(|mask| mask.table != name);
            for role in &mut cat.roles {
                role.permissions
                    .retain(|permission| permission_table(permission) != Some(name));
            }
            cat.security_version = cat.security_version.wrapping_add(1);
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.tables.write().remove(&table_id);

        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(())
    }

    /// Rename a live table. `name` must exist and `new_name` must not collide
    /// with any live table; both checks run under `ddl_lock` so they are atomic
    /// with the rename and with concurrent `create_table` existence checks (no
    /// TOCTOU window). A no-op rename (`name == new_name`) succeeds without
    /// side-effects. The rename is logged to the shared WAL as
    /// `DdlOp::RenameTable` and recovered on reopen; the `table_id`, schema,
    /// and on-disk layout are unchanged (the table is keyed by `table_id`, so
    /// the in-memory object does not move — only the catalog name changes).
    pub fn rename_table(&self, name: &str, new_name: &str) -> Result<()> {
        use crate::wal::DdlOp;
        use std::sync::atomic::Ordering;

        self.require(&crate::auth::Permission::Ddl)?;
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }

        // A no-op rename short-circuits before any locking, so it can never
        // trip the "target already exists" check (the source *is* that name).
        if name == new_name {
            return Ok(());
        }
        if new_name.is_empty() {
            return Err(MongrelError::InvalidArgument(
                "rename_table: new name must not be empty".into(),
            ));
        }

        let _g = self.ddl_lock.lock();
        let table_id = {
            let cat = self.catalog.read();
            let src = cat
                .live(name)
                .ok_or_else(|| MongrelError::NotFound(format!("table {name:?} not found")))?;
            // Target must be free. Checked under ddl_lock, which every other
            // DDL (create/rename/drop) also holds, so a concurrent operation
            // cannot claim `new_name` between this check and the catalog write.
            if cat.live(new_name).is_some() {
                return Err(MongrelError::InvalidArgument(format!(
                    "rename_table: a table named {new_name:?} already exists"
                )));
            }
            src.table_id
        };

        let commit_lock = Arc::clone(&self.commit_lock);
        let _c = commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let txn_id = self.alloc_txn_id();
        let commit_seq = {
            let mut wal = self.shared_wal.lock();
            wal.append(
                txn_id,
                table_id,
                crate::wal::Op::Ddl(DdlOp::RenameTable {
                    table_id,
                    new_name: new_name.to_string(),
                }),
            )?;
            wal.append_commit(txn_id, epoch, &[])?
        };
        self.group
            .await_durable(&self.shared_wal, commit_seq)
            .inspect_err(|_| {
                self.poisoned.store(true, Ordering::Relaxed);
            })?;

        {
            let mut cat = self.catalog.write();
            let entry = cat
                .tables
                .iter_mut()
                .find(|t| t.table_id == table_id)
                .ok_or_else(|| MongrelError::NotFound(format!("table {name:?} not found")))?;
            entry.name = new_name.to_string();
            for trigger in &mut cat.triggers {
                if matches!(
                    &trigger.trigger.target,
                    TriggerTarget::Table(target) if target == name
                ) {
                    trigger.trigger = trigger.trigger.retarget_table(new_name, epoch.0)?;
                }
            }
            if let Some(definition) = cat
                .materialized_views
                .iter_mut()
                .find(|definition| definition.name == name)
            {
                definition.name = new_name.to_string();
            }
            for table in &mut cat.security.rls_tables {
                if table == name {
                    *table = new_name.to_string();
                }
            }
            for policy in &mut cat.security.policies {
                if policy.table == name {
                    policy.table = new_name.to_string();
                }
            }
            for mask in &mut cat.security.masks {
                if mask.table == name {
                    mask.table = new_name.to_string();
                }
            }
            for role in &mut cat.roles {
                for permission in &mut role.permissions {
                    rename_permission_table(permission, name, new_name);
                }
            }
            cat.security_version = cat.security_version.wrapping_add(1);
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        // The in-memory table object is keyed by table_id, not name, so it does
        // not move and live TableHandles remain valid.
        if let Some(table) = self.tables.read().get(&table_id) {
            table.lock().set_catalog_name(new_name.to_string());
        }
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(())
    }

    pub fn alter_column(
        &self,
        table_name: &str,
        column_name: &str,
        change: AlterColumn,
    ) -> Result<ColumnDef> {
        use crate::wal::DdlOp;
        use std::sync::atomic::Ordering;

        self.require(&crate::auth::Permission::Ddl)?;
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }

        let _g = self.ddl_lock.lock();
        let table_id = {
            let cat = self.catalog.read();
            cat.live(table_name)
                .ok_or_else(|| MongrelError::NotFound(format!("table {table_name:?} not found")))?
                .table_id
        };
        let handle =
            self.tables.read().get(&table_id).cloned().ok_or_else(|| {
                MongrelError::NotFound(format!("table {table_name:?} not mounted"))
            })?;

        // Legitimate online-ALTER slice: when nullable -> NOT NULL has a
        // declared default, backfill existing NULL/absent cells as one durable
        // transaction before logging the metadata change. A crash between the
        // two commits leaves a harmless nullable-but-filled column; retry is
        // idempotent because only remaining NULLs are touched.
        let backfill = {
            let table = handle.lock();
            let old = table
                .schema()
                .column(column_name)
                .cloned()
                .ok_or_else(|| MongrelError::Schema(format!("unknown column {column_name}")))?;
            let next_flags = change.flags.unwrap_or(old.flags);
            if old.flags.contains(crate::schema::ColumnFlags::NULLABLE)
                && !next_flags.contains(crate::schema::ColumnFlags::NULLABLE)
                && old.default_value.is_some()
            {
                let snapshot = Snapshot::at(self.epoch.visible());
                let mut updates = Vec::new();
                for row in table.visible_rows(snapshot)? {
                    if row
                        .columns
                        .get(&old.id)
                        .is_some_and(|value| !matches!(value, Value::Null))
                    {
                        continue;
                    }
                    let mut cells: Vec<(u16, Value)> = row.columns.into_iter().collect();
                    table.apply_defaults(&mut cells)?;
                    updates.push((table_id, crate::txn::Staged::Update(row.row_id, cells)));
                }
                updates
            } else {
                Vec::new()
            }
        };
        if !backfill.is_empty() {
            self.commit_transaction_with_external_states(
                self.alloc_txn_id(),
                self.epoch.visible(),
                backfill,
                Vec::new(),
                Vec::new(),
                None,
                None,
            )?;
        }
        let mut table = handle.lock();
        let column = table.prepare_alter_column(column_name, &change)?;
        let renamed_column = (column.name != column_name).then(|| column.name.clone());
        if table
            .schema()
            .columns
            .iter()
            .find(|c| c.id == column.id)
            .is_some_and(|c| c == &column)
        {
            return Ok(column);
        }

        let commit_lock = Arc::clone(&self.commit_lock);
        let _c = commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let txn_id = self.alloc_txn_id();
        let column_json = DdlOp::encode_column(&column)?;
        let commit_seq = {
            let mut wal = self.shared_wal.lock();
            wal.append(
                txn_id,
                table_id,
                crate::wal::Op::Ddl(DdlOp::AlterTable {
                    table_id,
                    column_json,
                }),
            )?;
            wal.append_commit(txn_id, epoch, &[])?
        };
        self.group
            .await_durable(&self.shared_wal, commit_seq)
            .inspect_err(|_| {
                self.poisoned.store(true, Ordering::Relaxed);
            })?;

        table.apply_altered_column(column.clone())?;
        let schema = table.schema().clone();
        drop(table);

        {
            let mut cat = self.catalog.write();
            let entry = cat
                .tables
                .iter_mut()
                .find(|t| t.table_id == table_id)
                .ok_or_else(|| MongrelError::NotFound(format!("table {table_name:?} not found")))?;
            entry.schema = schema;
            if let Some(new_column_name) = renamed_column {
                for trigger in &mut cat.triggers {
                    if matches!(
                        &trigger.trigger.target,
                        TriggerTarget::Table(target) if target == table_name
                    ) {
                        trigger.trigger = trigger.trigger.renamed_update_column(
                            column_name,
                            new_column_name.clone(),
                            epoch.0,
                        )?;
                    }
                }
                for role in &mut cat.roles {
                    for permission in &mut role.permissions {
                        rename_permission_column(
                            permission,
                            table_name,
                            column_name,
                            &new_column_name,
                        );
                    }
                }
                cat.security_version = cat.security_version.wrapping_add(1);
            }
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;

        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(column)
    }

    /// Set a timestamp-column TTL policy and WAL-log it for crash recovery and
    /// replication. Duration is in nanoseconds.
    pub fn set_table_ttl(
        &self,
        table_name: &str,
        column_name: &str,
        duration_nanos: u64,
    ) -> Result<crate::manifest::TtlPolicy> {
        let policy = self.replace_table_ttl(table_name, Some((column_name, duration_nanos)))?;
        Ok(policy.expect("set TTL produces a policy"))
    }

    pub fn clear_table_ttl(&self, table_name: &str) -> Result<()> {
        self.replace_table_ttl(table_name, None)?;
        Ok(())
    }

    fn replace_table_ttl(
        &self,
        table_name: &str,
        requested: Option<(&str, u64)>,
    ) -> Result<Option<crate::manifest::TtlPolicy>> {
        use crate::wal::DdlOp;
        use std::sync::atomic::Ordering;

        self.require(&crate::auth::Permission::Ddl)?;
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }

        let _g = self.ddl_lock.lock();
        let table_id = {
            let cat = self.catalog.read();
            cat.live(table_name)
                .ok_or_else(|| MongrelError::NotFound(format!("table {table_name:?} not found")))?
                .table_id
        };
        let handle =
            self.tables.read().get(&table_id).cloned().ok_or_else(|| {
                MongrelError::NotFound(format!("table {table_name:?} not mounted"))
            })?;
        let mut table = handle.lock();
        let policy = match requested {
            Some((column, duration)) => Some(table.prepare_ttl_policy(column, duration)?),
            None => None,
        };
        if table.ttl() == policy {
            return Ok(policy);
        }

        let commit_lock = Arc::clone(&self.commit_lock);
        let _c = commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let txn_id = self.alloc_txn_id();
        let policy_json = DdlOp::encode_ttl(policy)?;
        let commit_seq = {
            let mut wal = self.shared_wal.lock();
            wal.append(
                txn_id,
                table_id,
                crate::wal::Op::Ddl(DdlOp::SetTtl {
                    table_id,
                    policy_json,
                }),
            )?;
            wal.append_commit(txn_id, epoch, &[])?
        };
        self.group
            .await_durable(&self.shared_wal, commit_seq)
            .inspect_err(|_| {
                self.poisoned.store(true, Ordering::Relaxed);
            })?;

        table.apply_ttl_policy_at(policy, epoch)?;
        self.epoch.publish_in_order(epoch);
        _epoch_guard.disarm();
        Ok(policy)
    }

    /// Retention-gated garbage collection (spec §6.4, §7.4, §16). Deletes:
    /// - Dropped-table subdirs whose `at_epoch < min_active_snapshot`.
    /// - Stale `_txn/` dirs (aborted/crashed large-txn pending runs).
    ///
    /// Returns the number of items reclaimed.
    pub fn gc(&self) -> Result<usize> {
        self.require(&crate::auth::Permission::Ddl)?;
        let min_active = self.snapshots.min_active(self.epoch.visible()).0;
        let mut reclaimed = 0;

        // Reclaim dropped-table dirs where no pinned snapshot still needs them.
        let cat = self.catalog.read();
        for entry in &cat.tables {
            if let TableState::Dropped { at_epoch } = entry.state {
                if at_epoch <= min_active {
                    let tdir = self.root.join(TABLES_DIR).join(entry.table_id.to_string());
                    if tdir.exists() {
                        std::fs::remove_dir_all(&tdir)?;
                        reclaimed += 1;
                    }
                }
            }
        }
        drop(cat);

        // Sweep stale _txn/<id>/ dirs on remaining live tables — but NEVER an
        // in-flight spill's dir (deleting it would lose the pending run and fail
        // the commit, review fix #14). Each `_txn/` subdir is named by its txn id;
        // skip any id still registered in `active_spills`.
        let cat = self.catalog.read();
        for entry in &cat.tables {
            if !matches!(entry.state, TableState::Live) {
                continue;
            }
            let txn_dir = self
                .root
                .join(TABLES_DIR)
                .join(entry.table_id.to_string())
                .join("_txn");
            if !txn_dir.exists() {
                continue;
            }
            for sub in std::fs::read_dir(&txn_dir)? {
                let sub = sub?;
                let name = sub.file_name();
                let Some(name) = name.to_str() else { continue };
                // A non-numeric entry can't belong to a live txn — sweep it.
                let is_active = name
                    .parse::<u64>()
                    .map(|id| self.active_spills.is_active(id))
                    .unwrap_or(false);
                if is_active {
                    continue;
                }
                std::fs::remove_dir_all(sub.path())?;
                reclaimed += 1;
            }
        }
        drop(cat);

        let external_names = {
            let cat = self.catalog.read();
            cat.external_tables
                .iter()
                .map(|entry| entry.name.clone())
                .collect::<std::collections::HashSet<_>>()
        };
        let vtab_dir = self.root.join(VTAB_DIR);
        if vtab_dir.exists() {
            for entry in std::fs::read_dir(&vtab_dir)? {
                let entry = entry?;
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                if external_names.contains(name) {
                    continue;
                }
                let path = entry.path();
                if path.is_dir() {
                    std::fs::remove_dir_all(path)?;
                } else {
                    std::fs::remove_file(path)?;
                }
                reclaimed += 1;
            }
        }

        // Reap compaction-superseded runs whose retire epoch no pinned snapshot
        // can still need (spec §6.4). Each table deletes its own retired files
        // gated on `min_active` and persists its manifest.
        let tables = self.tables.read();
        for (table_id, handle) in tables.iter() {
            let backup_pinned: HashSet<u128> = self
                .backup_pins
                .lock()
                .keys()
                .filter_map(|(pinned_table, run_id)| {
                    (*pinned_table == *table_id).then_some(*run_id)
                })
                .collect();
            reclaimed += handle
                .lock()
                .reap_retiring(Epoch(min_active), &backup_pinned)?;
        }

        // WAL-segment GC (spec §6.4/§16). `SharedWal::open` mints a fresh active
        // segment on every reopen without truncating the prior ones, so rotated
        // segments accumulate. Once every live table's committed data is durable
        // in runs (no in-memory rows) and no in-flight spill is open, all rotated
        // (non-active) segments are redundant for recovery and safe to delete —
        // an in-flight txn only ever appends to the active segment, which is
        // never deleted.
        let all_durable = self.active_spills.is_idle()
            && tables.values().all(|h| {
                let g = h.lock();
                g.memtable_len() == 0 && g.mutable_run_len() == 0
            });
        drop(tables);
        if all_durable {
            let retain = self
                .replication_wal_retention_segments
                .load(std::sync::atomic::Ordering::Relaxed);
            reclaimed += self
                .shared_wal
                .lock()
                .gc_segments_retain_recent(u64::MAX, retain)?;
        }

        Ok(reclaimed)
    }

    /// Produce a deterministic-stable byte image of the database directory.
    ///
    /// After `checkpoint()`:
    ///   - All pending writes are flushed to sorted runs (no memtable data).
    ///   - Each table is compacted to a single sorted run (no run fragmentation).
    ///   - All non-active WAL segments are deleted (data is durable in runs).
    ///   - The active WAL segment is rotated to a fresh empty segment.
    ///   - Dropped-table directories are removed.
    ///   - All manifests + catalog are persisted.
    ///
    /// The resulting directory is byte-stable: `git add` captures a snapshot
    /// that `git checkout` restores deterministically. No stale WAL tail bytes,
    /// no unbounded segment growth, no mutable-run spill files.
    ///
    /// This is the engine primitive behind `mongreldb snapshot <dir>` (CLI).
    /// It does NOT clear the exclusive lock — the caller still owns the
    /// database handle.
    pub fn checkpoint(&self) -> Result<()> {
        // 1. Force-flush every table's pending writes to sorted runs.
        self.close()?;

        // 2. Compact every table to a single run (merge all fragments).
        let _ = self.compact()?;

        // 3. GC everything: dropped-table dirs, stale _txn dirs, retired runs,
        //    and all WAL segments whose data is now durable in runs.
        self.gc()?;

        // 4. Reap ALL WAL segments (all data is durable in runs after flush +
        //    compact). Delete every segment file, then the reopen creates a
        //    fresh empty one via SharedWal::open. We can't use gc_segments alone
        //    because it skips the active segment — and leaving a stale active
        //    segment with pre-checkpoint tail bytes causes a magic-mismatch or
        //    truncated-read panic on reopen.
        {
            let wal = self.shared_wal.lock();
            let active = wal.active_segment_no();
            drop(wal);
            // Remove every segment file including the active one.
            let wal_dir = self.root.join("_wal");
            if wal_dir.exists() {
                for entry in std::fs::read_dir(&wal_dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.extension().is_some_and(|ext| ext == "wal") {
                        let _ = std::fs::remove_file(&path);
                    }
                }
            }
            let _ = active; // tracked for debugging
        }

        // 5. Persist the catalog with the bumped next_segment_no.
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;

        // 6. Persist every table's manifest (force_flush/compact already did
        //    this, but a final pass ensures consistency after WAL rotation).
        let tables = self.tables.read();
        let visible = self.epoch.visible();
        for handle in tables.values() {
            handle.lock().persist_manifest(visible)?;
        }

        Ok(())
    }
    fn alloc_txn_id(&self) -> u64 {
        let mut g = self.next_txn_id.lock();
        let id = *g;
        *g = g.wrapping_add(1);
        id
    }

    /// Set the per-table spill threshold (bytes). When a transaction's staged
    /// bytes for a single table exceed this, the rows are written as a
    /// uniform-epoch pending run instead of streamed Put records (spec §8.5).
    pub fn set_spill_threshold(&self, bytes: u64) {
        self.spill_threshold
            .store(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Test-only: install a hook invoked after a transaction writes its spill
    /// runs but before the sequencer, so a test can race `gc()` against an
    /// in-flight spill. Not part of the stable API.
    #[doc(hidden)]
    pub fn __set_spill_hook(&self, f: impl Fn() + Send + Sync + 'static) {
        *self.spill_hook.lock() = Some(Box::new(f));
    }

    /// Test-only: pause an online backup after its consistent boundary is
    /// captured but before the pinned immutable runs are copied.
    #[doc(hidden)]
    pub fn __set_backup_hook(&self, f: impl Fn() + Send + Sync + 'static) {
        *self.backup_hook.lock() = Some(Box::new(f));
    }

    /// Number of WAL fsyncs issued so far (test/diagnostic). With group commit
    /// this stays well below the number of committed transactions when commits
    /// are concurrent (one leader fsync covers a whole batch — spec §9.3).
    #[doc(hidden)]
    pub fn __wal_group_sync_count(&self) -> u64 {
        self.shared_wal.lock().group_sync_count()
    }

    /// Force the poisoned state (test-only) to verify the §9.3e fail-fast
    /// contract that an fsync error would trigger in production.
    #[doc(hidden)]
    pub fn __poison(&self) {
        self.poisoned
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Verify multi-table integrity (spec §16). For every live table this:
    /// authenticates the manifest; opens each `RunRef`'s file through
    /// [`RunReader`](crate::sorted_run::RunReader), which verifies the run footer
    /// checksum and — for encrypted DBs — the keyed run-metadata MAC; checks each
    /// run's physical row count against its `RunRef`; flags `RunRef`s whose file
    /// is missing (dangling) and `.sr` files on disk that no `RunRef` references
    /// (orphan); and verifies `flushed_epoch <= current_epoch`. Returns the list
    /// of issues found (empty = healthy). Orphans are `warning`-severity; all
    /// other findings are `error`-severity (so [`Self::doctor`] quarantines them).
    ///
    /// Cost: O(total run bytes) — the footer checksum is verified over each run's
    /// full body, so this is an integrity tool, not a hot path.
    pub fn check(&self) -> Vec<CheckIssue> {
        let mut issues = Vec::new();
        let cat = self.catalog.read();
        let manifest_meta_dek = crate::encryption::meta_dek_for(self.kek.as_deref());
        for entry in &cat.tables {
            if !matches!(entry.state, TableState::Live) {
                continue;
            }
            let tdir = self.root.join(TABLES_DIR).join(entry.table_id.to_string());
            let mut err = |sev: &str, desc: String| {
                issues.push(CheckIssue {
                    table_id: entry.table_id,
                    table_name: entry.name.clone(),
                    severity: sev.into(),
                    description: desc,
                });
            };
            let m = match crate::manifest::read(&tdir, manifest_meta_dek.as_ref()) {
                Ok(m) => m,
                Err(e) => {
                    err("error", format!("manifest read failed: {e}"));
                    continue;
                }
            };
            if m.flushed_epoch > m.current_epoch {
                err(
                    "error",
                    format!(
                        "flushed_epoch {} exceeds current_epoch {} (impossible)",
                        m.flushed_epoch, m.current_epoch
                    ),
                );
            }

            let runs_dir = tdir.join(crate::engine::RUNS_DIR);
            let mut referenced: std::collections::HashSet<u128> = std::collections::HashSet::new();
            for rr in &m.runs {
                referenced.insert(rr.run_id);
                let run_path = runs_dir.join(format!("r-{}.sr", rr.run_id));
                if !run_path.exists() {
                    err("error", format!("missing run file: r-{}.sr", rr.run_id));
                    continue;
                }
                match crate::sorted_run::RunReader::open(
                    &run_path,
                    entry.schema.clone(),
                    self.kek.clone(),
                ) {
                    Ok(reader) => {
                        if reader.row_count() as u64 != rr.row_count {
                            err(
                                "error",
                                format!(
                                    "run r-{} row count mismatch: manifest {} vs run {}",
                                    rr.run_id,
                                    rr.row_count,
                                    reader.row_count()
                                ),
                            );
                        }
                    }
                    Err(e) => {
                        err(
                            "error",
                            format!("run r-{} integrity check failed: {e}", rr.run_id),
                        );
                    }
                }
            }

            // Compaction-superseded runs awaiting retention-gated deletion are
            // tracked in `retiring`; their files are expected on disk, so they
            // are not orphans.
            for r in &m.retiring {
                referenced.insert(r.run_id);
            }

            // Orphan `.sr` files present on disk but absent from the manifest.
            if let Ok(rd) = std::fs::read_dir(&runs_dir) {
                for ent in rd.flatten() {
                    let p = ent.path();
                    if p.extension().and_then(|s| s.to_str()) != Some("sr") {
                        continue;
                    }
                    let run_id = p
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .and_then(|s| s.strip_prefix("r-"))
                        .and_then(|s| s.parse::<u128>().ok());
                    if let Some(id) = run_id {
                        if !referenced.contains(&id) {
                            err(
                                "warning",
                                format!("orphan run file r-{id}.sr not referenced by the manifest"),
                            );
                        }
                    }
                }
            }
        }

        let external_names = cat
            .external_tables
            .iter()
            .map(|entry| entry.name.clone())
            .collect::<std::collections::HashSet<_>>();
        let vtab_dir = self.root.join(VTAB_DIR);
        if let Ok(entries) = std::fs::read_dir(&vtab_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                if !external_names.contains(name) {
                    issues.push(CheckIssue {
                        table_id: EXTERNAL_TABLE_ID,
                        table_name: name.to_string(),
                        severity: "warning".into(),
                        description: format!(
                            "orphan external table state entry {:?} not referenced by the catalog",
                            entry.path()
                        ),
                    });
                }
            }
        }

        // WAL retention / integrity invariant (spec §16): every on-disk WAL
        // segment must open (header magic + version, and the frame cipher must
        // be derivable for an encrypted WAL). A segment that won't open is
        // corrupt or truncated and would break crash recovery. `table_id` is
        // the reserved `WAL_TABLE_ID` sentinel (u64::MAX) so [`Self::doctor`]
        // never confuses a WAL issue with a real table.
        for (seg, msg) in self.shared_wal.lock().verify_segments() {
            issues.push(CheckIssue {
                table_id: WAL_TABLE_ID,
                table_name: "<wal>".into(),
                severity: "error".into(),
                description: format!("WAL segment seg-{seg:06}.wal failed integrity check: {msg}"),
            });
        }
        issues
    }

    /// Quarantine unreadable tables (spec §16). Moves corrupt table dirs to
    /// `_quarantine/<table_id>/`, marks them dropped in the catalog, and
    /// unmounts them from the live table map so the DB still opens.
    pub fn doctor(&self) -> Result<Vec<u64>> {
        // Hold the DDL lock for the whole operation to prevent concurrent
        // create_table/drop_table from racing the catalog/dir mutation.
        let _ddl = self.ddl_lock.lock();
        let issues = self.check();
        // A corrupt WAL segment is reported as an error but is NOT a table
        // problem — quarantining an innocent table cannot fix it (and the first
        // real table is id 0, so the WAL sentinel WAL_TABLE_ID = u64::MAX keeps
        // them disjoint). The admin must address WAL corruption manually.
        let bad_tables: std::collections::HashSet<u64> = issues
            .iter()
            .filter(|i| {
                i.severity == "error"
                    && i.table_id != WAL_TABLE_ID
                    && i.table_id != EXTERNAL_TABLE_ID
            })
            .map(|i| i.table_id)
            .collect();
        if bad_tables.is_empty() {
            return Ok(Vec::new());
        }

        let qdir = self.root.join("_quarantine");
        std::fs::create_dir_all(&qdir)?;
        let mut quarantined = Vec::new();
        for &table_id in &bad_tables {
            let tdir = self.root.join(TABLES_DIR).join(table_id.to_string());
            if tdir.exists() {
                let dest = qdir.join(table_id.to_string());
                std::fs::rename(&tdir, &dest)?;
            }
            {
                let mut cat = self.catalog.write();
                if let Some(entry) = cat.tables.iter_mut().find(|t| t.table_id == table_id) {
                    entry.state = TableState::Dropped {
                        at_epoch: self.epoch.visible().0,
                    };
                }
            }
            // Unmount the live handle so no further access reaches the moved dir.
            self.tables.write().remove(&table_id);
            quarantined.push(table_id);
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        Ok(quarantined)
    }

    /// The DB-wide KEK (if encrypted).
    #[allow(dead_code)]
    pub(crate) fn kek(&self) -> Option<&Arc<crate::encryption::Kek>> {
        self.kek.as_ref()
    }

    /// Shared epoch authority (used by the transaction layer in P2).
    #[allow(dead_code)]
    pub(crate) fn epoch_authority(&self) -> &Arc<EpochAuthority> {
        &self.epoch
    }

    /// Shared snapshot registry (used by GC in P3.6).
    #[allow(dead_code)]
    pub(crate) fn snapshots(&self) -> &Arc<SnapshotRegistry> {
        &self.snapshots
    }
}

fn external_state_dir(root: &Path, name: &str) -> PathBuf {
    root.join(VTAB_DIR).join(name)
}

fn filter_ignored_staging(
    staging: Vec<(u64, crate::txn::Staged)>,
    ignored_indices: &std::collections::BTreeSet<usize>,
) -> Vec<(u64, crate::txn::Staged)> {
    if ignored_indices.is_empty() {
        return staging;
    }
    staging
        .into_iter()
        .enumerate()
        .filter_map(|(idx, staged)| (!ignored_indices.contains(&idx)).then_some(staged))
        .collect()
}

fn external_state_file(root: &Path, name: &str) -> PathBuf {
    external_state_dir(root, name).join("state.json")
}

fn read_external_state_file(root: &Path, name: &str) -> Result<Vec<u8>> {
    let path = external_state_file(root, name);
    match std::fs::read(path) {
        Ok(bytes) => Ok(bytes),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e.into()),
    }
}

fn current_external_state_bytes(
    root: &Path,
    external_states: &[(String, Vec<u8>)],
    name: &str,
) -> Result<Vec<u8>> {
    for (table, state) in external_states.iter().rev() {
        if table == name {
            return Ok(state.clone());
        }
    }
    read_external_state_file(root, name)
}

fn dedup_external_states(external_states: Vec<(String, Vec<u8>)>) -> Vec<(String, Vec<u8>)> {
    let mut out = external_states;
    dedup_external_states_in_place(&mut out);
    out
}

fn dedup_external_states_in_place(external_states: &mut Vec<(String, Vec<u8>)>) {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::with_capacity(external_states.len());
    for (name, state) in std::mem::take(external_states).into_iter().rev() {
        if seen.insert(name.clone()) {
            out.push((name, state));
        }
    }
    out.reverse();
    *external_states = out;
}

fn prepare_external_state_file(
    root: &Path,
    name: &str,
    state: &[u8],
    txn_id: u64,
) -> Result<PathBuf> {
    let dir = external_state_dir(root, name);
    std::fs::create_dir_all(&dir)?;
    let pending = dir.join(format!("state.json.{txn_id}.tmp"));
    {
        let mut file = std::fs::File::create(&pending)?;
        file.write_all(state)?;
        file.sync_all()?;
    }
    Ok(pending)
}

fn publish_external_state_file(root: &Path, name: &str, pending: &Path) -> Result<()> {
    let path = external_state_file(root, name);
    std::fs::rename(pending, &path)?;
    if let Ok(dir) = std::fs::File::open(external_state_dir(root, name)) {
        let _ = dir.sync_all();
    }
    Ok(())
}

fn write_external_state_file(root: &Path, name: &str, state: &[u8]) -> Result<()> {
    let pending = prepare_external_state_file(root, name, state, 0)?;
    publish_external_state_file(root, name, &pending)
}

/// Two-pass, `flushed_epoch`-gated recovery of the shared WAL (spec §15).
///
/// Pass 1 scans every `TxnCommit` marker and records `txn_id → commit_epoch`
/// (the per-txn outcome; aborted / in-flight / torn-tail txns are absent). Pass
/// 2 applies each committed data record (Put/Delete) to its table at the commit
/// epoch, skipping records whose `commit_epoch <= table.flushed_epoch` (already
/// durable in a sorted run). Finally the shared epoch authority is raised to the
/// max committed epoch so the next commit continues monotonically.
fn recover_shared_wal(
    root: &Path,
    tables: &HashMap<u64, TableHandle>,
    epoch: &EpochAuthority,
    wal_dek: Option<&zeroize::Zeroizing<[u8; 32]>>,
) -> Result<()> {
    use crate::memtable::Row;
    use crate::rowid::RowId;
    use crate::wal::{Op, SharedWal};

    let records = SharedWal::replay_with_dek(root, wal_dek)?;

    // Pass 1: committed-txn outcomes + collect spilled-run info.
    let mut committed: HashMap<u64, u64> = HashMap::new();
    let mut spilled_to_link: Vec<(
        u64, /*txn_id*/
        u64, /*epoch*/
        Vec<crate::wal::AddedRun>,
    )> = Vec::new();
    for r in &records {
        if let Op::TxnCommit {
            epoch: ce,
            ref added_runs,
        } = r.op
        {
            committed.insert(r.txn_id, ce);
            if !added_runs.is_empty() {
                spilled_to_link.push((r.txn_id, ce, added_runs.clone()));
            }
        }
    }
    let truncated_transactions: HashSet<(u64, u64)> = records
        .iter()
        .filter_map(|record| {
            committed.get(&record.txn_id)?;
            match record.op {
                Op::TruncateTable { table_id } => Some((record.txn_id, table_id)),
                _ => None,
            }
        })
        .collect();

    // Pass 2: stage data per table, gated by flushed_epoch.
    type TableStage = (Vec<Row>, Vec<(RowId, Epoch)>, Option<Epoch>, Epoch);
    let mut stage: HashMap<u64, TableStage> = HashMap::new();
    let mut max_epoch = epoch.visible().0;
    for r in records {
        let Some(&ce) = committed.get(&r.txn_id) else {
            continue; // aborted / in-flight — discard
        };
        let commit_epoch = Epoch(ce);
        max_epoch = max_epoch.max(ce);
        match r.op {
            Op::Put { table_id, rows } => {
                // Skip if this table already flushed past the commit epoch.
                let skip = tables
                    .get(&table_id)
                    .map(|h| h.lock().flushed_epoch() >= ce)
                    .unwrap_or(true);
                if skip {
                    continue;
                }
                let rows: Vec<Row> = match bincode::deserialize(&rows) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                // Re-stamp each row at the txn commit epoch (rows are pre-stamped
                // at pending_epoch which equals the commit epoch, but be robust).
                let rows: Vec<Row> = rows
                    .into_iter()
                    .map(|mut row| {
                        row.committed_epoch = commit_epoch;
                        row
                    })
                    .collect();
                let entry = stage
                    .entry(table_id)
                    .or_insert_with(|| (Vec::new(), Vec::new(), None, commit_epoch));
                entry.0.extend(rows);
                entry.3 = commit_epoch;
            }
            Op::Delete { table_id, row_ids } => {
                let skip = tables
                    .get(&table_id)
                    .map(|h| h.lock().flushed_epoch() >= ce)
                    .unwrap_or(true);
                if skip {
                    continue;
                }
                let dels = row_ids.into_iter().map(|rid| (rid, commit_epoch));
                let entry = stage
                    .entry(table_id)
                    .or_insert_with(|| (Vec::new(), Vec::new(), None, commit_epoch));
                entry.1.extend(dels);
                entry.3 = commit_epoch;
            }
            Op::TruncateTable { table_id } => {
                let skip = tables
                    .get(&table_id)
                    .map(|h| h.lock().flushed_epoch() >= ce)
                    .unwrap_or(true);
                if skip {
                    continue;
                }
                stage.insert(
                    table_id,
                    (Vec::new(), Vec::new(), Some(commit_epoch), commit_epoch),
                );
            }
            Op::ExternalTableState { name, state } => {
                write_external_state_file(root, &name, &state)?;
            }
            Op::Flush { .. }
            | Op::TxnCommit { .. }
            | Op::TxnAbort
            | Op::Ddl(_)
            | Op::BeforeImage { .. }
            | Op::CommitTimestamp { .. } => {}
        }
    }
    for (table_id, (rows, deletes, truncate_epoch, table_epoch)) in stage {
        let Some(handle) = tables.get(&table_id) else {
            continue;
        };
        let mut t = handle.lock();
        if let Some(epoch) = truncate_epoch {
            t.apply_truncate(epoch)?;
        }
        t.recover_apply(rows, deletes)?;
        // The WAL can be newer than the copied/persisted manifest after a
        // crash or replication apply. Rebuild O(1) count metadata from the
        // recovered state before endorsing the commit epoch in the manifest.
        let rows = t.visible_rows(Snapshot::at(Epoch(u64::MAX)))?;
        t.live_count = rows.len() as u64;
        t.persist_manifest(table_epoch)?;
    }

    // Pass 3: link spilled runs from committed txns (spec §8.5). A crash
    // between TxnCommit sync and the publish phase leaves the run in
    // `_txn/<txn_id>/`. Move it to `_runs/` and add the RunRef.
    for (txn_id, ce, added_runs) in &spilled_to_link {
        for ar in added_runs {
            let Some(handle) = tables.get(&ar.table_id) else {
                continue;
            };
            let mut t = handle.lock();
            let dest = t.run_path(ar.run_id as u64);
            if !dest.exists() {
                let pending = root
                    .join(TABLES_DIR)
                    .join(ar.table_id.to_string())
                    .join("_txn")
                    .join(txn_id.to_string())
                    .join(format!("r-{}.sr", ar.run_id));
                if pending.exists() {
                    if let Some(parent) = pending.parent() {
                        std::fs::rename(&pending, &dest)?;
                        let _ = std::fs::remove_dir_all(parent);
                    }
                }
            }
            // Only link a run whose file is actually present, and never re-link
            // one the publish phase already persisted into the manifest (which is
            // the common clean-reopen case, since the `TxnCommit` lives in the WAL
            // until segment GC). `recover_spilled_run` is idempotent + reconciles
            // `live_count`/indexes only when the run is genuinely new.
            if t.run_path(ar.run_id as u64).exists() {
                let linked = t.recover_spilled_run(crate::manifest::RunRef {
                    run_id: ar.run_id,
                    level: ar.level,
                    epoch_created: *ce,
                    row_count: ar.row_count,
                });
                let replaced = truncated_transactions.contains(&(*txn_id, ar.table_id));
                if replaced {
                    t.set_flushed_epoch(Epoch(*ce));
                }
                if linked || replaced {
                    t.persist_manifest(Epoch(*ce))?;
                }
            }
        }
    }

    epoch.advance_recovered(Epoch(max_epoch));
    Ok(())
}

fn validate_condition_columns(condition: &ProcedureCondition, schema: &Schema) -> Result<()> {
    match condition {
        ProcedureCondition::Pk { .. } => {
            if schema.primary_key().is_none() {
                return Err(MongrelError::InvalidArgument(
                    "procedure condition Pk references a table without a primary key".into(),
                ));
            }
        }
        ProcedureCondition::BitmapEq { column_id, .. }
        | ProcedureCondition::BitmapIn { column_id, .. }
        | ProcedureCondition::Range { column_id, .. }
        | ProcedureCondition::RangeF64 { column_id, .. }
        | ProcedureCondition::IsNull { column_id }
        | ProcedureCondition::IsNotNull { column_id }
        | ProcedureCondition::FmContains { column_id, .. } => {
            validate_column_id(*column_id, schema)?;
        }
    }
    Ok(())
}

fn bind_procedure_args(
    procedure: &StoredProcedure,
    mut args: HashMap<String, crate::Value>,
) -> Result<HashMap<String, crate::Value>> {
    let mut out = HashMap::new();
    for param in &procedure.params {
        let value = match args.remove(&param.name) {
            Some(value) => value,
            None => param.default.clone().ok_or_else(|| {
                MongrelError::InvalidArgument(format!(
                    "missing required procedure parameter {:?}",
                    param.name
                ))
            })?,
        };
        if !param.nullable && matches!(value, crate::Value::Null) {
            return Err(MongrelError::InvalidArgument(format!(
                "procedure parameter {:?} must not be NULL",
                param.name
            )));
        }
        if !matches!(value, crate::Value::Null) && !value_matches_type(&value, param.ty.clone()) {
            return Err(MongrelError::InvalidArgument(format!(
                "procedure parameter {:?} has wrong type",
                param.name
            )));
        }
        out.insert(param.name.clone(), value);
    }
    if let Some(extra) = args.keys().next() {
        return Err(MongrelError::InvalidArgument(format!(
            "unknown procedure parameter {extra:?}"
        )));
    }
    Ok(out)
}

fn value_matches_type(value: &crate::Value, ty: crate::TypeId) -> bool {
    matches!(
        (value, ty),
        (crate::Value::Bool(_), crate::TypeId::Bool)
            | (crate::Value::Int64(_), crate::TypeId::Int8)
            | (crate::Value::Int64(_), crate::TypeId::Int16)
            | (crate::Value::Int64(_), crate::TypeId::Int32)
            | (crate::Value::Int64(_), crate::TypeId::Int64)
            | (crate::Value::Int64(_), crate::TypeId::UInt8)
            | (crate::Value::Int64(_), crate::TypeId::UInt16)
            | (crate::Value::Int64(_), crate::TypeId::UInt32)
            | (crate::Value::Int64(_), crate::TypeId::UInt64)
            | (crate::Value::Int64(_), crate::TypeId::TimestampNanos)
            | (crate::Value::Int64(_), crate::TypeId::Date32)
            | (crate::Value::Float64(_), crate::TypeId::Float32)
            | (crate::Value::Float64(_), crate::TypeId::Float64)
            | (crate::Value::Bytes(_), crate::TypeId::Bytes)
            | (crate::Value::Embedding(_), crate::TypeId::Embedding { .. })
    )
}

fn eval_cells(
    cells: &[crate::procedure::ProcedureCell],
    args: &HashMap<String, crate::Value>,
    outputs: &HashMap<String, ProcedureCallOutput>,
) -> Result<Vec<(u16, crate::Value)>> {
    cells
        .iter()
        .map(|cell| Ok((cell.column_id, eval_value(&cell.value, args, outputs)?)))
        .collect()
}

fn eval_condition(
    condition: &ProcedureCondition,
    args: &HashMap<String, crate::Value>,
    outputs: &HashMap<String, ProcedureCallOutput>,
) -> Result<crate::Condition> {
    Ok(match condition {
        ProcedureCondition::Pk { value } => {
            crate::Condition::Pk(eval_value(value, args, outputs)?.encode_key())
        }
        ProcedureCondition::BitmapEq { column_id, value } => crate::Condition::BitmapEq {
            column_id: *column_id,
            value: eval_value(value, args, outputs)?.encode_key(),
        },
        ProcedureCondition::BitmapIn { column_id, values } => crate::Condition::BitmapIn {
            column_id: *column_id,
            values: values
                .iter()
                .map(|value| Ok(eval_value(value, args, outputs)?.encode_key()))
                .collect::<Result<Vec<_>>>()?,
        },
        ProcedureCondition::Range { column_id, lo, hi } => crate::Condition::Range {
            column_id: *column_id,
            lo: expect_i64(eval_value(lo, args, outputs)?)?,
            hi: expect_i64(eval_value(hi, args, outputs)?)?,
        },
        ProcedureCondition::RangeF64 {
            column_id,
            lo,
            lo_inclusive,
            hi,
            hi_inclusive,
        } => crate::Condition::RangeF64 {
            column_id: *column_id,
            lo: expect_f64(eval_value(lo, args, outputs)?)?,
            lo_inclusive: *lo_inclusive,
            hi: expect_f64(eval_value(hi, args, outputs)?)?,
            hi_inclusive: *hi_inclusive,
        },
        ProcedureCondition::IsNull { column_id } => crate::Condition::IsNull {
            column_id: *column_id,
        },
        ProcedureCondition::IsNotNull { column_id } => crate::Condition::IsNotNull {
            column_id: *column_id,
        },
        ProcedureCondition::FmContains { column_id, pattern } => crate::Condition::FmContains {
            column_id: *column_id,
            pattern: expect_bytes(eval_value(pattern, args, outputs)?)?,
        },
    })
}

fn eval_value(
    value: &ProcedureValue,
    args: &HashMap<String, crate::Value>,
    outputs: &HashMap<String, ProcedureCallOutput>,
) -> Result<crate::Value> {
    match value {
        ProcedureValue::Literal(value) => Ok(value.clone()),
        ProcedureValue::Param(name) => args.get(name).cloned().ok_or_else(|| {
            MongrelError::InvalidArgument(format!("unknown procedure parameter {name:?}"))
        }),
        ProcedureValue::StepScalar(id) => match outputs.get(id) {
            Some(ProcedureCallOutput::Scalar(value)) => Ok(value.clone()),
            _ => Err(MongrelError::InvalidArgument(format!(
                "procedure step {id:?} did not return a scalar"
            ))),
        },
        ProcedureValue::StepRows(_) | ProcedureValue::StepRow(_) => {
            Err(MongrelError::InvalidArgument(
                "row-valued procedure reference cannot be used as a scalar".into(),
            ))
        }
        ProcedureValue::Object(_) | ProcedureValue::Array(_) => Err(MongrelError::InvalidArgument(
            "structured procedure value cannot be used as a scalar cell".into(),
        )),
    }
}

fn eval_return_output(
    value: &ProcedureValue,
    args: &HashMap<String, crate::Value>,
    outputs: &HashMap<String, ProcedureCallOutput>,
) -> Result<ProcedureCallOutput> {
    match value {
        ProcedureValue::Literal(value) => Ok(ProcedureCallOutput::Scalar(value.clone())),
        ProcedureValue::Param(name) => Ok(ProcedureCallOutput::Scalar(
            args.get(name).cloned().ok_or_else(|| {
                MongrelError::InvalidArgument(format!("unknown procedure parameter {name:?}"))
            })?,
        )),
        ProcedureValue::StepRows(id)
        | ProcedureValue::StepRow(id)
        | ProcedureValue::StepScalar(id) => outputs.get(id).cloned().ok_or_else(|| {
            MongrelError::InvalidArgument(format!("unknown procedure step output {id:?}"))
        }),
        ProcedureValue::Object(fields) => {
            let mut out = Vec::with_capacity(fields.len());
            for (name, value) in fields {
                out.push((name.clone(), eval_return_output(value, args, outputs)?));
            }
            Ok(ProcedureCallOutput::Object(out))
        }
        ProcedureValue::Array(values) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                out.push(eval_return_output(value, args, outputs)?);
            }
            Ok(ProcedureCallOutput::Array(out))
        }
    }
}

fn expect_i64(value: crate::Value) -> Result<i64> {
    match value {
        crate::Value::Int64(value) => Ok(value),
        _ => Err(MongrelError::InvalidArgument(
            "procedure value must be Int64".into(),
        )),
    }
}

fn expect_f64(value: crate::Value) -> Result<f64> {
    match value {
        crate::Value::Float64(value) => Ok(value),
        _ => Err(MongrelError::InvalidArgument(
            "procedure value must be Float64".into(),
        )),
    }
}

fn expect_bytes(value: crate::Value) -> Result<Vec<u8>> {
    match value {
        crate::Value::Bytes(value) => Ok(value),
        _ => Err(MongrelError::InvalidArgument(
            "procedure value must be Bytes".into(),
        )),
    }
}

fn validate_column_id(column_id: u16, schema: &Schema) -> Result<()> {
    if schema.columns.iter().any(|c| c.id == column_id) {
        Ok(())
    } else {
        Err(MongrelError::InvalidArgument(format!(
            "unknown column id {column_id}"
        )))
    }
}

fn trigger_matches_event(
    trigger: &StoredTrigger,
    event: &WriteEvent,
    cat: &Catalog,
) -> Result<bool> {
    if trigger.event != event.kind {
        return Ok(false);
    }
    let TriggerTarget::Table(target) = &trigger.target else {
        return Ok(false);
    };
    if target != &event.table {
        return Ok(false);
    }
    if trigger.event == TriggerEvent::Update && !trigger.update_of.is_empty() {
        let schema = &cat
            .live(target)
            .ok_or_else(|| {
                MongrelError::InvalidArgument(format!(
                    "trigger {:?} references unknown table {target:?}",
                    trigger.name
                ))
            })?
            .schema;
        let mut watched = Vec::with_capacity(trigger.update_of.len());
        for name in &trigger.update_of {
            let col = schema.column(name).ok_or_else(|| {
                MongrelError::InvalidArgument(format!(
                    "trigger {:?} references unknown UPDATE OF column {name:?}",
                    trigger.name
                ))
            })?;
            watched.push(col.id);
        }
        if !event
            .changed_columns
            .iter()
            .any(|column_id| watched.contains(column_id))
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn changed_columns(old: Option<&TriggerRowImage>, new: Option<&TriggerRowImage>) -> Vec<u16> {
    let mut ids = std::collections::BTreeSet::new();
    if let Some(old) = old {
        ids.extend(old.columns.keys().copied());
    }
    if let Some(new) = new {
        ids.extend(new.columns.keys().copied());
    }
    ids.into_iter()
        .filter(|id| {
            old.and_then(|row| row.columns.get(id)) != new.and_then(|row| row.columns.get(id))
        })
        .collect()
}

fn eval_trigger_cells(
    cells: &[crate::trigger::TriggerCell],
    event: &WriteEvent,
    selected: Option<&TriggerRowImage>,
) -> Result<Vec<(u16, Value)>> {
    cells
        .iter()
        .map(|cell| {
            Ok((
                cell.column_id,
                eval_trigger_value(&cell.value, event, selected)?,
            ))
        })
        .collect()
}

fn eval_trigger_expr(expr: &TriggerExpr, event: &WriteEvent) -> Result<bool> {
    match expr {
        TriggerExpr::Value(value) => match eval_trigger_value(value, event, None)? {
            Value::Bool(value) => Ok(value),
            Value::Null => Ok(false),
            other => Err(MongrelError::InvalidArgument(format!(
                "trigger WHEN value must be boolean, got {other:?}"
            ))),
        },
        TriggerExpr::Eq { left, right } => Ok(values_equal(
            &eval_trigger_value(left, event, None)?,
            &eval_trigger_value(right, event, None)?,
        )),
        TriggerExpr::NotEq { left, right } => Ok(!values_equal(
            &eval_trigger_value(left, event, None)?,
            &eval_trigger_value(right, event, None)?,
        )),
        TriggerExpr::Lt { left, right } => match value_order(
            &eval_trigger_value(left, event, None)?,
            &eval_trigger_value(right, event, None)?,
        ) {
            Some(ordering) => Ok(ordering == std::cmp::Ordering::Less),
            None => Ok(false),
        },
        TriggerExpr::Lte { left, right } => match value_order(
            &eval_trigger_value(left, event, None)?,
            &eval_trigger_value(right, event, None)?,
        ) {
            Some(ordering) => Ok(ordering != std::cmp::Ordering::Greater),
            None => Ok(false),
        },
        TriggerExpr::Gt { left, right } => match value_order(
            &eval_trigger_value(left, event, None)?,
            &eval_trigger_value(right, event, None)?,
        ) {
            Some(ordering) => Ok(ordering == std::cmp::Ordering::Greater),
            None => Ok(false),
        },
        TriggerExpr::Gte { left, right } => match value_order(
            &eval_trigger_value(left, event, None)?,
            &eval_trigger_value(right, event, None)?,
        ) {
            Some(ordering) => Ok(ordering != std::cmp::Ordering::Less),
            None => Ok(false),
        },
        TriggerExpr::IsNull(value) => Ok(matches!(
            eval_trigger_value(value, event, None)?,
            Value::Null
        )),
        TriggerExpr::IsNotNull(value) => Ok(!matches!(
            eval_trigger_value(value, event, None)?,
            Value::Null
        )),
        TriggerExpr::And { left, right } => {
            if !eval_trigger_expr(left, event)? {
                Ok(false)
            } else {
                Ok(eval_trigger_expr(right, event)?)
            }
        }
        TriggerExpr::Or { left, right } => {
            if eval_trigger_expr(left, event)? {
                Ok(true)
            } else {
                Ok(eval_trigger_expr(right, event)?)
            }
        }
        TriggerExpr::Not(expr) => Ok(!eval_trigger_expr(expr, event)?),
    }
}

fn eval_trigger_condition(
    condition: &TriggerCondition,
    event: &WriteEvent,
    selected: &TriggerRowImage,
    schema: &Schema,
) -> Result<bool> {
    match condition {
        TriggerCondition::Pk { value } => {
            let pk = schema.primary_key().ok_or_else(|| {
                MongrelError::InvalidArgument(
                    "trigger condition Pk references a table without a primary key".into(),
                )
            })?;
            let lhs = eval_trigger_value(value, event, Some(selected))?;
            Ok(values_equal(
                &lhs,
                selected.columns.get(&pk.id).unwrap_or(&Value::Null),
            ))
        }
        TriggerCondition::Eq { column_id, value } => Ok(values_equal(
            selected.columns.get(column_id).unwrap_or(&Value::Null),
            &eval_trigger_value(value, event, Some(selected))?,
        )),
        TriggerCondition::NotEq { column_id, value } => Ok(!values_equal(
            selected.columns.get(column_id).unwrap_or(&Value::Null),
            &eval_trigger_value(value, event, Some(selected))?,
        )),
        TriggerCondition::Lt { column_id, value } => match value_order(
            selected.columns.get(column_id).unwrap_or(&Value::Null),
            &eval_trigger_value(value, event, Some(selected))?,
        ) {
            Some(ordering) => Ok(ordering == std::cmp::Ordering::Less),
            None => Ok(false),
        },
        TriggerCondition::Lte { column_id, value } => match value_order(
            selected.columns.get(column_id).unwrap_or(&Value::Null),
            &eval_trigger_value(value, event, Some(selected))?,
        ) {
            Some(ordering) => Ok(ordering != std::cmp::Ordering::Greater),
            None => Ok(false),
        },
        TriggerCondition::Gt { column_id, value } => match value_order(
            selected.columns.get(column_id).unwrap_or(&Value::Null),
            &eval_trigger_value(value, event, Some(selected))?,
        ) {
            Some(ordering) => Ok(ordering == std::cmp::Ordering::Greater),
            None => Ok(false),
        },
        TriggerCondition::Gte { column_id, value } => match value_order(
            selected.columns.get(column_id).unwrap_or(&Value::Null),
            &eval_trigger_value(value, event, Some(selected))?,
        ) {
            Some(ordering) => Ok(ordering != std::cmp::Ordering::Less),
            None => Ok(false),
        },
        TriggerCondition::IsNull { column_id } => Ok(matches!(
            selected.columns.get(column_id),
            None | Some(Value::Null)
        )),
        TriggerCondition::IsNotNull { column_id } => Ok(!matches!(
            selected.columns.get(column_id),
            None | Some(Value::Null)
        )),
        TriggerCondition::And { left, right } => {
            if !eval_trigger_condition(left, event, selected, schema)? {
                Ok(false)
            } else {
                Ok(eval_trigger_condition(right, event, selected, schema)?)
            }
        }
        TriggerCondition::Or { left, right } => {
            if eval_trigger_condition(left, event, selected, schema)? {
                Ok(true)
            } else {
                Ok(eval_trigger_condition(right, event, selected, schema)?)
            }
        }
        TriggerCondition::Not(condition) => {
            Ok(!eval_trigger_condition(condition, event, selected, schema)?)
        }
    }
}

fn eval_trigger_value(
    value: &TriggerValue,
    event: &WriteEvent,
    selected: Option<&TriggerRowImage>,
) -> Result<Value> {
    match value {
        TriggerValue::Literal(value) => Ok(value.clone()),
        TriggerValue::NewColumn(column_id) => event
            .new
            .as_ref()
            .and_then(|row| row.columns.get(column_id))
            .cloned()
            .ok_or_else(|| MongrelError::InvalidArgument("NEW column is not available".into())),
        TriggerValue::OldColumn(column_id) => event
            .old
            .as_ref()
            .and_then(|row| row.columns.get(column_id))
            .cloned()
            .ok_or_else(|| MongrelError::InvalidArgument("OLD column is not available".into())),
        TriggerValue::SelectedColumn(column_id) => selected
            .and_then(|row| row.columns.get(column_id))
            .cloned()
            .ok_or_else(|| {
                MongrelError::InvalidArgument("SELECTED column is not available".into())
            }),
    }
}

fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Int64(a), Value::Int64(b)) => a == b,
        (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
        (Value::Bytes(a), Value::Bytes(b)) => a == b,
        (Value::Embedding(a), Value::Embedding(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|(a, b)| a.to_bits() == b.to_bits())
        }
        _ => false,
    }
}

fn value_order(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => None,
        (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
        (Value::Int64(a), Value::Int64(b)) => Some(a.cmp(b)),
        // Cross-type Int64/Float64 comparison coerces the integer to f64.
        // This matches the spec but can lose precision for i64 values above 2^53.
        (Value::Int64(a), Value::Float64(b)) => {
            let af = *a as f64;
            Some(af.total_cmp(b))
        }
        // Cross-type Int64/Float64 comparison coerces the integer to f64.
        // This matches the spec but can lose precision for i64 values above 2^53.
        (Value::Float64(a), Value::Int64(b)) => {
            let bf = *b as f64;
            Some(a.total_cmp(&bf))
        }
        (Value::Float64(a), Value::Float64(b)) => Some(a.total_cmp(b)),
        (Value::Bytes(a), Value::Bytes(b)) => Some(a.cmp(b)),
        (Value::Embedding(_), Value::Embedding(_)) => None,
        _ => None,
    }
}

fn trigger_message(value: Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::Bool(value) => value.to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Float64(value) => value.to_string(),
        Value::Bytes(value) => String::from_utf8_lossy(&value).into_owned(),
        Value::Embedding(value) => format!("{value:?}"),
        Value::Decimal(value) => value.to_string(),
        Value::Interval {
            months,
            days,
            nanos,
        } => format!("{months}m {days}d {nanos}ns"),
        Value::Uuid(b) => b.iter().map(|x| format!("{x:02x}")).collect(),
        Value::Json(b) => String::from_utf8_lossy(&b).into_owned(),
    }
}

fn validate_trigger_step<'a>(
    step: &TriggerStep,
    cat: &'a Catalog,
    target_schema: &Schema,
    event: TriggerEvent,
    select_schemas: &mut HashMap<String, &'a Schema>,
) -> Result<()> {
    match step {
        TriggerStep::SetNew { cells } => {
            if event == TriggerEvent::Delete {
                return Err(MongrelError::InvalidArgument(
                    "SetNew trigger step is not valid for DELETE triggers".into(),
                ));
            }
            for cell in cells {
                validate_column_id(cell.column_id, target_schema)?;
                validate_trigger_value(&cell.value, target_schema, event)?;
            }
        }
        TriggerStep::Insert { table, cells } => {
            let schema = trigger_write_schema(cat, table, "insert")?;
            for cell in cells {
                validate_column_id(cell.column_id, schema)?;
                validate_trigger_value(&cell.value, target_schema, event)?;
            }
        }
        TriggerStep::UpdateByPk { table, pk, cells } => {
            let schema = trigger_write_schema(cat, table, "update")?;
            if schema.primary_key().is_none() {
                return Err(MongrelError::InvalidArgument(format!(
                    "trigger update_by_pk references table {table:?} without a primary key"
                )));
            }
            validate_trigger_value(pk, target_schema, event)?;
            for cell in cells {
                validate_column_id(cell.column_id, schema)?;
                validate_trigger_value(&cell.value, target_schema, event)?;
            }
        }
        TriggerStep::DeleteByPk { table, pk } => {
            let schema = trigger_write_schema(cat, table, "delete")?;
            if schema.primary_key().is_none() {
                return Err(MongrelError::InvalidArgument(format!(
                    "trigger delete_by_pk references table {table:?} without a primary key"
                )));
            }
            validate_trigger_value(pk, target_schema, event)?;
        }
        TriggerStep::Select {
            id,
            table,
            conditions,
        } => {
            let schema = trigger_read_schema(cat, table)?;
            for condition in conditions {
                validate_trigger_condition(condition, schema, target_schema, event)?;
            }
            if select_schemas.contains_key(id) {
                return Err(MongrelError::InvalidArgument(format!(
                    "duplicate select id {id:?} in trigger program"
                )));
            }
            select_schemas.insert(id.clone(), schema);
        }
        TriggerStep::Foreach { id, steps } => {
            if !select_schemas.contains_key(id) {
                return Err(MongrelError::InvalidArgument(format!(
                    "foreach references unknown select id {id:?}"
                )));
            }
            let mut inner_select_schemas = select_schemas.clone();
            for step in steps {
                validate_trigger_step(step, cat, target_schema, event, &mut inner_select_schemas)?;
            }
        }
        TriggerStep::DeleteWhere { table, conditions } => {
            let schema = trigger_write_schema(cat, table, "delete")?;
            for condition in conditions {
                validate_trigger_condition(condition, schema, target_schema, event)?;
            }
        }
        TriggerStep::UpdateWhere {
            table,
            conditions,
            cells,
        } => {
            let schema = trigger_write_schema(cat, table, "update")?;
            for condition in conditions {
                validate_trigger_condition(condition, schema, target_schema, event)?;
            }
            for cell in cells {
                validate_column_id(cell.column_id, schema)?;
                validate_trigger_value(&cell.value, target_schema, event)?;
            }
        }
        TriggerStep::Raise { message, .. } => {
            validate_trigger_value(message, target_schema, event)?
        }
    }
    Ok(())
}

fn trigger_write_schema<'a>(cat: &'a Catalog, table: &str, op: &str) -> Result<&'a Schema> {
    if let Some(entry) = cat.live(table) {
        return Ok(&entry.schema);
    }
    if let Some(entry) = cat.external_tables.iter().find(|entry| entry.name == table) {
        let allowed = match op {
            "insert" => entry.capabilities.writable || entry.capabilities.insert_only,
            "update" | "delete" => entry.capabilities.writable,
            _ => false,
        };
        if !allowed {
            return Err(MongrelError::InvalidArgument(format!(
                "trigger {op} references external table {table:?}, but module {:?} is not writable for that operation",
                entry.module
            )));
        }
        if !entry.capabilities.transaction_safe {
            return Err(MongrelError::InvalidArgument(format!(
                "trigger {op} references external table {table:?}, but module {:?} is not transaction-safe",
                entry.module
            )));
        }
        return Ok(&entry.declared_schema);
    }
    Err(MongrelError::InvalidArgument(format!(
        "trigger references unknown table {table:?}"
    )))
}

fn trigger_read_schema<'a>(cat: &'a Catalog, table: &str) -> Result<&'a Schema> {
    if let Some(entry) = cat.live(table) {
        return Ok(&entry.schema);
    }
    if let Some(entry) = cat.external_tables.iter().find(|entry| entry.name == table) {
        if entry.capabilities.trigger_safe {
            return Ok(&entry.declared_schema);
        }
        return Err(MongrelError::InvalidArgument(format!(
            "trigger reads external table {table:?}, but module {:?} is not trigger-safe",
            entry.module
        )));
    }
    Err(MongrelError::InvalidArgument(format!(
        "trigger references unknown table {table:?}"
    )))
}

fn validate_trigger_condition(
    condition: &TriggerCondition,
    schema: &Schema,
    target_schema: &Schema,
    event: TriggerEvent,
) -> Result<()> {
    match condition {
        TriggerCondition::Pk { value } => {
            if schema.primary_key().is_none() {
                return Err(MongrelError::InvalidArgument(
                    "trigger condition Pk references a table without a primary key".into(),
                ));
            }
            validate_trigger_value(value, target_schema, event)
        }
        TriggerCondition::Eq { column_id, value }
        | TriggerCondition::NotEq { column_id, value }
        | TriggerCondition::Lt { column_id, value }
        | TriggerCondition::Lte { column_id, value }
        | TriggerCondition::Gt { column_id, value }
        | TriggerCondition::Gte { column_id, value } => {
            validate_column_id(*column_id, schema)?;
            validate_trigger_value(value, target_schema, event)
        }
        TriggerCondition::IsNull { column_id } | TriggerCondition::IsNotNull { column_id } => {
            validate_column_id(*column_id, schema)
        }
        TriggerCondition::And { left, right } | TriggerCondition::Or { left, right } => {
            validate_trigger_condition(left, schema, target_schema, event)?;
            validate_trigger_condition(right, schema, target_schema, event)
        }
        TriggerCondition::Not(condition) => {
            validate_trigger_condition(condition, schema, target_schema, event)
        }
    }
}

fn validate_trigger_expr(expr: &TriggerExpr, schema: &Schema, event: TriggerEvent) -> Result<()> {
    match expr {
        TriggerExpr::Value(value) | TriggerExpr::IsNull(value) | TriggerExpr::IsNotNull(value) => {
            validate_trigger_value(value, schema, event)
        }
        TriggerExpr::Eq { left, right }
        | TriggerExpr::NotEq { left, right }
        | TriggerExpr::Lt { left, right }
        | TriggerExpr::Lte { left, right }
        | TriggerExpr::Gt { left, right }
        | TriggerExpr::Gte { left, right } => {
            validate_trigger_value(left, schema, event)?;
            validate_trigger_value(right, schema, event)
        }
        TriggerExpr::And { left, right } | TriggerExpr::Or { left, right } => {
            validate_trigger_expr(left, schema, event)?;
            validate_trigger_expr(right, schema, event)
        }
        TriggerExpr::Not(expr) => validate_trigger_expr(expr, schema, event),
    }
}

fn validate_trigger_value(
    value: &TriggerValue,
    schema: &Schema,
    event: TriggerEvent,
) -> Result<()> {
    match value {
        TriggerValue::Literal(_) => Ok(()),
        TriggerValue::NewColumn(id) => {
            if event == TriggerEvent::Delete {
                return Err(MongrelError::InvalidArgument(
                    "DELETE triggers cannot reference NEW".into(),
                ));
            }
            validate_column_id(*id, schema)
        }
        TriggerValue::OldColumn(id) => {
            if event == TriggerEvent::Insert {
                return Err(MongrelError::InvalidArgument(
                    "INSERT triggers cannot reference OLD".into(),
                ));
            }
            validate_column_id(*id, schema)
        }
        // SELECTED column references are only meaningful inside a foreach loop.
        // Strict loop-scope validation is deferred to runtime; the executor raises
        // an error if a selected row is not available.
        TriggerValue::SelectedColumn(_) => Ok(()),
    }
}

/// Replay committed `Op::Ddl` records from the shared WAL into the catalog
/// (spec §15, review fix #16). A crash between WAL group-sync and the catalog
/// checkpoint leaves DDL durable in the WAL but absent from the on-disk
/// catalog. This pass closes that window by reconstructing missing entries
/// (and marking committed drops) before tables are mounted.
fn recover_ddl_from_wal(
    root: &Path,
    cat: &mut Catalog,
    meta_dek: Option<&[u8; META_DEK_LEN]>,
    wal_dek: Option<&zeroize::Zeroizing<[u8; 32]>>,
) -> Result<()> {
    use crate::wal::{DdlOp, Op, SharedWal};

    let records = match SharedWal::replay_with_dek(root, wal_dek) {
        Ok(r) => r,
        Err(_) => return Ok(()),
    };

    let mut committed: HashMap<u64, u64> = HashMap::new();
    for r in &records {
        if let Op::TxnCommit { epoch: ce, .. } = r.op {
            committed.insert(r.txn_id, ce);
        }
    }

    let mut changed = false;
    for r in records {
        let Some(&ce) = committed.get(&r.txn_id) else {
            continue;
        };
        match r.op {
            Op::Ddl(DdlOp::CreateTable {
                table_id,
                ref name,
                ref schema_json,
            }) => {
                if cat.tables.iter().any(|t| t.table_id == table_id) {
                    continue;
                }
                let schema = DdlOp::decode_schema(schema_json)?;
                let tdir = root.join(TABLES_DIR).join(table_id.to_string());
                if !tdir.exists() {
                    std::fs::create_dir_all(tdir.join(crate::engine::WAL_DIR))?;
                    std::fs::create_dir_all(tdir.join(crate::engine::RUNS_DIR))?;
                    crate::engine::write_schema(&tdir, &schema)?;
                    // The DB-wide meta DEK is also the per-table manifest meta
                    // DEK (both derive from the KEK via `derive_meta_key`), so a
                    // reconstructed manifest must be sealed with it — otherwise
                    // the follow-up `Table::open_in` cannot authenticate it on an
                    // encrypted DB and the table becomes permanently unopenable.
                    let mut m = crate::manifest::Manifest::new(table_id, schema.schema_id);
                    crate::manifest::write_atomic(&tdir, &mut m, meta_dek)?;
                }
                cat.tables.push(CatalogEntry {
                    table_id,
                    name: name.clone(),
                    schema,
                    state: TableState::Live,
                    created_epoch: ce,
                });
                cat.next_table_id = cat.next_table_id.max(table_id + 1);
                changed = true;
            }
            Op::Ddl(DdlOp::DropTable { table_id }) => {
                let mut dropped_name = None;
                if let Some(entry) = cat.tables.iter_mut().find(|t| t.table_id == table_id) {
                    dropped_name = Some(entry.name.clone());
                    if matches!(entry.state, TableState::Live) {
                        entry.state = TableState::Dropped { at_epoch: ce };
                        changed = true;
                    }
                }
                if let Some(name) = dropped_name {
                    let before = cat.materialized_views.len();
                    cat.materialized_views
                        .retain(|definition| definition.name != name);
                    changed |= before != cat.materialized_views.len();
                    cat.security.rls_tables.retain(|table| table != &name);
                    cat.security.policies.retain(|policy| policy.table != name);
                    cat.security.masks.retain(|mask| mask.table != name);
                    for role in &mut cat.roles {
                        role.permissions
                            .retain(|permission| permission_table(permission) != Some(&name));
                    }
                    cat.security_version = cat.security_version.wrapping_add(1);
                }
            }
            Op::Ddl(DdlOp::RenameTable {
                table_id,
                ref new_name,
            }) => {
                let mut old_name = None;
                if let Some(entry) = cat.tables.iter_mut().find(|t| t.table_id == table_id) {
                    if entry.name != *new_name {
                        old_name = Some(entry.name.clone());
                        entry.name = new_name.clone();
                        changed = true;
                    }
                }
                if let Some(old_name) = old_name {
                    if let Some(definition) = cat
                        .materialized_views
                        .iter_mut()
                        .find(|definition| definition.name == old_name)
                    {
                        definition.name = new_name.clone();
                    }
                    for table in &mut cat.security.rls_tables {
                        if *table == old_name {
                            *table = new_name.clone();
                        }
                    }
                    for policy in &mut cat.security.policies {
                        if policy.table == old_name {
                            policy.table = new_name.clone();
                        }
                    }
                    for mask in &mut cat.security.masks {
                        if mask.table == old_name {
                            mask.table = new_name.clone();
                        }
                    }
                    for role in &mut cat.roles {
                        for permission in &mut role.permissions {
                            rename_permission_table(permission, &old_name, new_name);
                        }
                    }
                    cat.security_version = cat.security_version.wrapping_add(1);
                }
                // If the entry is absent, its CreateTable was already
                // checkpointed carrying the post-rename name, so there is
                // nothing to apply — a no-op, not an error.
            }
            Op::Ddl(DdlOp::AlterTable {
                table_id,
                ref column_json,
            }) => {
                let column = DdlOp::decode_column(column_json)?;
                let mut renamed = None;
                if let Some(entry) = cat.tables.iter_mut().find(|t| t.table_id == table_id) {
                    renamed = entry
                        .schema
                        .columns
                        .iter()
                        .find(|existing| existing.id == column.id && existing.name != column.name)
                        .map(|existing| {
                            (
                                entry.name.clone(),
                                existing.name.clone(),
                                column.name.clone(),
                            )
                        });
                    if apply_recovered_column_def(&mut entry.schema, column) {
                        let tdir = root.join(TABLES_DIR).join(table_id.to_string());
                        if tdir.exists() {
                            crate::engine::write_schema(&tdir, &entry.schema)?;
                        }
                        changed = true;
                    }
                }
                if let Some((table, old_name, new_name)) = renamed {
                    for role in &mut cat.roles {
                        for permission in &mut role.permissions {
                            rename_permission_column(permission, &table, &old_name, &new_name);
                        }
                    }
                    cat.security_version = cat.security_version.wrapping_add(1);
                }
            }
            Op::Ddl(DdlOp::SetTtl {
                table_id,
                ref policy_json,
            }) => {
                let policy = DdlOp::decode_ttl(policy_json)?;
                if let Some(policy) = policy {
                    let valid = cat
                        .tables
                        .iter()
                        .find(|entry| entry.table_id == table_id)
                        .and_then(|entry| {
                            entry
                                .schema
                                .columns
                                .iter()
                                .find(|column| column.id == policy.column_id)
                        })
                        .is_some_and(|column| {
                            column.ty == TypeId::TimestampNanos
                                && policy.duration_nanos > 0
                                && policy.duration_nanos <= i64::MAX as u64
                        });
                    if !valid {
                        return Err(MongrelError::Schema(format!(
                            "invalid recovered TTL policy for table id {table_id}"
                        )));
                    }
                }
                let tdir = root.join(TABLES_DIR).join(table_id.to_string());
                if tdir.exists() {
                    let mut manifest = crate::manifest::read(&tdir, meta_dek)?;
                    if manifest.ttl != policy || manifest.current_epoch < ce {
                        manifest.ttl = policy;
                        manifest.current_epoch = manifest.current_epoch.max(ce);
                        crate::manifest::write_atomic(&tdir, &mut manifest, meta_dek)?;
                    }
                }
            }
            Op::Ddl(DdlOp::SetMaterializedView {
                ref name,
                ref definition_json,
            }) => {
                let definition = DdlOp::decode_materialized_view(definition_json)?;
                if definition.name != *name {
                    return Err(MongrelError::Schema(format!(
                        "materialized view WAL name mismatch: {name:?}"
                    )));
                }
                if cat.live(name).is_some() {
                    if let Some(existing) = cat
                        .materialized_views
                        .iter_mut()
                        .find(|existing| existing.name == *name)
                    {
                        if *existing != definition {
                            *existing = definition;
                            changed = true;
                        }
                    } else {
                        cat.materialized_views.push(definition);
                        changed = true;
                    }
                }
            }
            Op::Ddl(DdlOp::SetSecurityCatalog { ref security_json }) => {
                let security = DdlOp::decode_security(security_json)?;
                validate_security_catalog(cat, &security)?;
                if cat.security != security {
                    cat.security = security;
                    cat.security_version = cat.security_version.wrapping_add(1);
                    changed = true;
                }
            }
            _ => {}
        }
    }

    if changed {
        catalog::write_atomic(root, cat, meta_dek)?;
    }
    Ok(())
}

fn apply_recovered_column_def(schema: &mut Schema, column: ColumnDef) -> bool {
    match schema.columns.iter_mut().find(|c| c.id == column.id) {
        Some(existing) if *existing == column => false,
        Some(existing) => {
            *existing = column;
            schema.schema_id = schema.schema_id.saturating_add(1);
            true
        }
        None => {
            schema.columns.push(column);
            schema.schema_id = schema.schema_id.saturating_add(1);
            true
        }
    }
}

fn permission_table(permission: &crate::auth::Permission) -> Option<&str> {
    use crate::auth::Permission;
    match permission {
        Permission::Select { table }
        | Permission::Insert { table }
        | Permission::Update { table }
        | Permission::Delete { table }
        | Permission::SelectColumns { table, .. }
        | Permission::InsertColumns { table, .. }
        | Permission::UpdateColumns { table, .. } => Some(table),
        Permission::All | Permission::Ddl | Permission::Admin => None,
    }
}

fn rename_permission_table(permission: &mut crate::auth::Permission, old: &str, new: &str) {
    use crate::auth::Permission;
    let table = match permission {
        Permission::Select { table }
        | Permission::Insert { table }
        | Permission::Update { table }
        | Permission::Delete { table }
        | Permission::SelectColumns { table, .. }
        | Permission::InsertColumns { table, .. }
        | Permission::UpdateColumns { table, .. } => Some(table),
        Permission::All | Permission::Ddl | Permission::Admin => None,
    };
    if let Some(table) = table.filter(|table| table.as_str() == old) {
        *table = new.to_string();
    }
}

fn rename_permission_column(
    permission: &mut crate::auth::Permission,
    target_table: &str,
    old: &str,
    new: &str,
) {
    use crate::auth::Permission;
    let columns = match permission {
        Permission::SelectColumns { table, columns }
        | Permission::InsertColumns { table, columns }
        | Permission::UpdateColumns { table, columns }
            if table == target_table =>
        {
            Some(columns)
        }
        _ => None,
    };
    if let Some(column) = columns
        .into_iter()
        .flatten()
        .find(|column| column.as_str() == old)
    {
        *column = new.to_string();
    }
}

fn merge_permission(
    permissions: &mut Vec<crate::auth::Permission>,
    permission: crate::auth::Permission,
) {
    use crate::auth::Permission;
    let (kind, table, mut columns) = match permission {
        Permission::SelectColumns { table, columns } => (0, table, columns),
        Permission::InsertColumns { table, columns } => (1, table, columns),
        Permission::UpdateColumns { table, columns } => (2, table, columns),
        permission if !permissions.contains(&permission) => {
            permissions.push(permission);
            return;
        }
        _ => return,
    };
    for permission in permissions.iter_mut() {
        let existing = match permission {
            Permission::SelectColumns {
                table: existing_table,
                columns,
            } if kind == 0 && existing_table == &table => Some(columns),
            Permission::InsertColumns {
                table: existing_table,
                columns,
            } if kind == 1 && existing_table == &table => Some(columns),
            Permission::UpdateColumns {
                table: existing_table,
                columns,
            } if kind == 2 && existing_table == &table => Some(columns),
            _ => None,
        };
        if let Some(existing) = existing {
            existing.append(&mut columns);
            existing.sort();
            existing.dedup();
            return;
        }
    }
    columns.sort();
    columns.dedup();
    permissions.push(match kind {
        0 => Permission::SelectColumns { table, columns },
        1 => Permission::InsertColumns { table, columns },
        2 => Permission::UpdateColumns { table, columns },
        _ => unreachable!(),
    });
}

fn revoke_permission_from(
    permissions: &mut Vec<crate::auth::Permission>,
    revoked: &crate::auth::Permission,
) {
    use crate::auth::Permission;
    let revoked_columns = match revoked {
        Permission::SelectColumns { table, columns } => Some((0, table, columns)),
        Permission::InsertColumns { table, columns } => Some((1, table, columns)),
        Permission::UpdateColumns { table, columns } => Some((2, table, columns)),
        _ => None,
    };
    let Some((kind, table, columns)) = revoked_columns else {
        permissions.retain(|permission| permission != revoked);
        return;
    };
    for permission in permissions.iter_mut() {
        let current = match permission {
            Permission::SelectColumns {
                table: current_table,
                columns,
            } if kind == 0 && current_table == table => Some(columns),
            Permission::InsertColumns {
                table: current_table,
                columns,
            } if kind == 1 && current_table == table => Some(columns),
            Permission::UpdateColumns {
                table: current_table,
                columns,
            } if kind == 2 && current_table == table => Some(columns),
            _ => None,
        };
        if let Some(current) = current {
            current.retain(|column| !columns.contains(column));
        }
    }
    permissions.retain(|permission| match permission {
        Permission::SelectColumns { columns, .. }
        | Permission::InsertColumns { columns, .. }
        | Permission::UpdateColumns { columns, .. } => !columns.is_empty(),
        _ => true,
    });
}

fn validate_security_catalog(
    catalog: &Catalog,
    security: &crate::security::SecurityCatalog,
) -> Result<()> {
    let mut policy_names = HashSet::new();
    for table in &security.rls_tables {
        if catalog.live(table).is_none() {
            return Err(MongrelError::NotFound(format!(
                "RLS table {table:?} not found"
            )));
        }
    }
    for policy in &security.policies {
        if !policy_names.insert((policy.table.clone(), policy.name.clone())) {
            return Err(MongrelError::InvalidArgument(format!(
                "duplicate policy {:?} on {:?}",
                policy.name, policy.table
            )));
        }
        let schema = &catalog
            .live(&policy.table)
            .ok_or_else(|| {
                MongrelError::NotFound(format!("policy table {:?} not found", policy.table))
            })?
            .schema;
        if let Some(expression) = &policy.using {
            validate_security_expression(expression, schema)?;
        }
        if let Some(expression) = &policy.with_check {
            validate_security_expression(expression, schema)?;
        }
    }
    let mut mask_names = HashSet::new();
    for mask in &security.masks {
        if !mask_names.insert((mask.table.clone(), mask.name.clone())) {
            return Err(MongrelError::InvalidArgument(format!(
                "duplicate mask {:?} on {:?}",
                mask.name, mask.table
            )));
        }
        let column = catalog
            .live(&mask.table)
            .and_then(|entry| {
                entry
                    .schema
                    .columns
                    .iter()
                    .find(|column| column.id == mask.column)
            })
            .ok_or_else(|| {
                MongrelError::NotFound(format!(
                    "mask column {} on {:?} not found",
                    mask.column, mask.table
                ))
            })?;
        if matches!(
            mask.strategy,
            crate::security::MaskStrategy::Redact { .. } | crate::security::MaskStrategy::Sha256
        ) && !matches!(column.ty, TypeId::Bytes | TypeId::Enum { .. })
        {
            return Err(MongrelError::InvalidArgument(format!(
                "mask {:?} requires a string/bytes column",
                mask.name
            )));
        }
    }
    Ok(())
}

fn validate_security_expression(
    expression: &crate::security::SecurityExpr,
    schema: &Schema,
) -> Result<()> {
    use crate::security::SecurityExpr;
    match expression {
        SecurityExpr::True => Ok(()),
        SecurityExpr::ColumnEqCurrentUser { column }
        | SecurityExpr::ColumnEqValue { column, .. } => {
            if schema
                .columns
                .iter()
                .any(|candidate| candidate.id == *column)
            {
                Ok(())
            } else {
                Err(MongrelError::InvalidArgument(format!(
                    "security expression references unknown column id {column}"
                )))
            }
        }
        SecurityExpr::And { left, right } | SecurityExpr::Or { left, right } => {
            validate_security_expression(left, schema)?;
            validate_security_expression(right, schema)
        }
        SecurityExpr::Not { expression } => validate_security_expression(expression, schema),
    }
}

/// Sweep stale `_txn/<txn_id>/` dirs from every table (spec §8.5, review fix
/// #14). These dirs hold pending uniform-epoch runs from large transactions
/// that were aborted or crashed before commit. On open, all such dirs are safe
/// to remove — committed txns moved their runs to `_runs/` at publish time.
fn sweep_pending_txn_dirs(root: &Path, cat: &Catalog) {
    for entry in &cat.tables {
        let txn_dir = root
            .join(TABLES_DIR)
            .join(entry.table_id.to_string())
            .join("_txn");
        if txn_dir.exists() {
            let _ = std::fs::remove_dir_all(&txn_dir);
        }
    }
}

#[cfg(test)]
mod trigger_engine_tests {
    use super::*;

    fn event_with(new_cells: &[(u16, Value)], old_cells: &[(u16, Value)]) -> WriteEvent {
        WriteEvent {
            table: "test".into(),
            kind: TriggerEvent::Insert,
            new: Some(TriggerRowImage {
                columns: new_cells.iter().cloned().collect(),
            }),
            old: Some(TriggerRowImage {
                columns: old_cells.iter().cloned().collect(),
            }),
            changed_columns: Vec::new(),
            op_indices: Vec::new(),
            put_idx: None,
            trigger_stack: Vec::new(),
        }
    }

    fn event_insert(new_cells: &[(u16, Value)]) -> WriteEvent {
        WriteEvent {
            table: "test".into(),
            kind: TriggerEvent::Insert,
            new: Some(TriggerRowImage {
                columns: new_cells.iter().cloned().collect(),
            }),
            old: None,
            changed_columns: Vec::new(),
            op_indices: Vec::new(),
            put_idx: None,
            trigger_stack: Vec::new(),
        }
    }

    #[test]
    fn value_order_int64_vs_float64() {
        assert_eq!(
            value_order(&Value::Int64(5), &Value::Float64(5.0)),
            Some(std::cmp::Ordering::Equal)
        );
        assert_eq!(
            value_order(&Value::Int64(5), &Value::Float64(3.0)),
            Some(std::cmp::Ordering::Greater)
        );
        assert_eq!(
            value_order(&Value::Int64(2), &Value::Float64(3.0)),
            Some(std::cmp::Ordering::Less)
        );
    }

    #[test]
    fn value_order_null_returns_none() {
        assert_eq!(value_order(&Value::Int64(5), &Value::Null), None);
        assert_eq!(value_order(&Value::Null, &Value::Int64(5)), None);
        assert_eq!(value_order(&Value::Null, &Value::Null), None);
    }

    #[test]
    fn value_order_cross_group_returns_none() {
        assert_eq!(
            value_order(&Value::Int64(5), &Value::Bytes(b"x".to_vec())),
            None
        );
        assert_eq!(value_order(&Value::Bool(true), &Value::Int64(1)), None);
        assert_eq!(
            value_order(
                &Value::Embedding(vec![1.0, 2.0]),
                &Value::Embedding(vec![1.0, 2.0])
            ),
            None
        );
    }

    #[test]
    fn eval_trigger_expr_ranges_and_booleans() {
        let expr = TriggerExpr::And {
            left: Box::new(TriggerExpr::Gt {
                left: TriggerValue::NewColumn(1),
                right: TriggerValue::Literal(Value::Int64(0)),
            }),
            right: Box::new(TriggerExpr::Lte {
                left: TriggerValue::NewColumn(1),
                right: TriggerValue::Literal(Value::Int64(100)),
            }),
        };
        assert!(eval_trigger_expr(&expr, &event_insert(&[(1, Value::Int64(50))])).unwrap());
        assert!(!eval_trigger_expr(&expr, &event_insert(&[(1, Value::Int64(200))])).unwrap());
        assert!(!eval_trigger_expr(&expr, &event_insert(&[(1, Value::Null)])).unwrap());

        let or_expr = TriggerExpr::Or {
            left: Box::new(TriggerExpr::Lt {
                left: TriggerValue::NewColumn(1),
                right: TriggerValue::Literal(Value::Int64(0)),
            }),
            right: Box::new(TriggerExpr::Not(Box::new(TriggerExpr::IsNull(
                TriggerValue::OldColumn(2),
            )))),
        };
        assert!(eval_trigger_expr(
            &or_expr,
            &event_with(&[(1, Value::Int64(5))], &[(2, Value::Int64(99))])
        )
        .unwrap());
        assert!(!eval_trigger_expr(
            &or_expr,
            &event_with(&[(1, Value::Int64(5))], &[(2, Value::Null)])
        )
        .unwrap());

        assert!(eval_trigger_expr(
            &TriggerExpr::Value(TriggerValue::Literal(Value::Bool(true))),
            &event_insert(&[])
        )
        .unwrap());
        assert!(!eval_trigger_expr(
            &TriggerExpr::Value(TriggerValue::Literal(Value::Bool(false))),
            &event_insert(&[])
        )
        .unwrap());
        assert!(!eval_trigger_expr(
            &TriggerExpr::Value(TriggerValue::Literal(Value::Null)),
            &event_insert(&[])
        )
        .unwrap());
    }
}
