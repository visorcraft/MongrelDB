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
use crate::retention::{OwnedSnapshotGuard, SnapshotGuard, SnapshotRegistry};
use crate::schema::Schema;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const TABLES_DIR: &str = "tables";
pub const META_DIR: &str = "_meta";
pub const KEYS_FILENAME: &str = "keys";

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

/// An integrity issue found by [`Database::check`] (spec §16).
#[derive(Debug, Clone)]
pub struct CheckIssue {
    pub table_id: u64,
    pub table_name: String,
    pub severity: String,
    pub description: String,
}

/// A handle to a live table inside a [`Database`]. Writes take the inner lock
/// (P1); P3.3 replaces this with lock-free `ArcSwap` reads + a publish lock for
/// writes.
pub type TableHandle = Arc<Mutex<Table>>;

/// A multi-table database: one catalog, one epoch clock, shared caches, a
/// shared WAL, and a live map of name → `Arc<Table>`.
pub struct Database {
    root: PathBuf,
    catalog: RwLock<Catalog>,
    epoch: Arc<EpochAuthority>,
    snapshots: Arc<SnapshotRegistry>,
    page_cache: Arc<parking_lot::Mutex<crate::cache::PageCache>>,
    decoded_cache: Arc<parking_lot::Mutex<crate::cache::DecodedPageCache>>,
    commit_lock: Arc<Mutex<()>>,
    /// One shared WAL multiplexing every table's records (spec §7.2). Owned
    /// behind a `Mutex` so the transaction layer can append + group-sync.
    shared_wal: Mutex<crate::wal::SharedWal>,
    /// Monotonic per-open transaction-id counter. Scoped by `open_generation`
    /// in P2.7; here it just needs to be unique within an open.
    next_txn_id: Mutex<u64>,
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
    poisoned: std::sync::atomic::AtomicBool,
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
        std::fs::create_dir_all(root)?;
        std::fs::create_dir_all(root.join(META_DIR))?;
        let salt = crate::encryption::random_salt();
        std::fs::write(root.join(META_DIR).join(KEYS_FILENAME), salt)?;
        let kek = Arc::new(crate::encryption::Kek::derive(passphrase, &salt)?);
        Self::create_inner(root, Some(kek))
    }

    fn create_inner(
        root: impl AsRef<Path>,
        kek: Option<Arc<crate::encryption::Kek>>,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        std::fs::create_dir_all(root.join(TABLES_DIR))?;
        let meta_dek = crate::encryption::meta_dek_for(kek.as_deref());
        let cat = Catalog::empty();
        catalog::write_atomic(&root, &cat, meta_dek.as_ref())?;
        Self::finish_open(root, cat, kek, meta_dek, false)
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

    fn open_inner(
        root: impl AsRef<Path>,
        kek: Option<Arc<crate::encryption::Kek>>,
        _meta_dek_override: Option<[u8; META_DEK_LEN]>,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let meta_dek = crate::encryption::meta_dek_for(kek.as_deref());
        let cat = catalog::read(&root, meta_dek.as_ref())?
            .ok_or_else(|| MongrelError::NotFound(format!("no catalog found at {:?}", root)))?;
        Self::finish_open(root, cat, kek, meta_dek, true)
    }

    fn finish_open(
        root: PathBuf,
        cat: Catalog,
        kek: Option<Arc<crate::encryption::Kek>>,
        meta_dek: Option<[u8; META_DEK_LEN]>,
        existing: bool,
    ) -> Result<Self> {
        let epoch = Arc::new(EpochAuthority::new(cat.db_epoch));
        let snapshots = Arc::new(SnapshotRegistry::new());
        let page_cache = Arc::new(parking_lot::Mutex::new(crate::cache::PageCache::new(
            crate::engine::PAGE_CACHE_CAPACITY,
        )));
        let decoded_cache = Arc::new(parking_lot::Mutex::new(
            crate::cache::DecodedPageCache::new(crate::engine::DECODED_CACHE_CAPACITY),
        ));
        let commit_lock = Arc::new(Mutex::new(()));
        let wal_dek = crate::encryption::wal_dek_for(kek.as_deref());
        let shared_wal = Mutex::new(if existing {
            crate::wal::SharedWal::open(&root, Epoch(cat.db_epoch), wal_dek.clone())?
        } else {
            crate::wal::SharedWal::create_with_dek(&root, Epoch(cat.db_epoch), wal_dek.clone())?
        });

        // Recover DDL from the shared WAL BEFORE opening tables (spec §15,
        // review fix #16). A crash between WAL fsync and the catalog
        // checkpoint leaves committed DDL durable in the WAL but absent from
        // the on-disk catalog; replay it here so the table-mounting loop and
        // data-record recovery see a correct catalog.
        let mut cat = cat;
        if existing {
            recover_ddl_from_wal(&root, &mut cat, meta_dek.as_ref(), wal_dek.as_ref())?;
        }

        // Open every live table against the shared context. Each `open_in`
        // replays that table's PER-TABLE WAL (Table::put-style writes) and
        // advances the shared epoch authority to its manifest epoch, so the
        // final shared watermark is the max across all tables.
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
            };
            let t = Table::open_in(&tdir, ctx)?;
            tables.insert(entry.table_id, Arc::new(Mutex::new(t)));
        }

        // Recover transaction writes from the shared WAL (spec §15). The per-
        // table WALs above already replayed `Table::put` writes; this pass
        // applies committed cross-table transactions, gated by each table's
        // `flushed_epoch` (records already durable in a run are not re-applied).
        if existing {
            recover_shared_wal(&root, &tables, &epoch, wal_dek.as_ref())?;
            // P3.4: sweep stale `_txn/<txn_id>/` dirs left by aborted/crashed
            // large transactions (spec §8.5, review fix #14).
            sweep_pending_txn_dirs(&root, &cat);
        }

        // Bump `open_generation` on every open and scope transaction ids by it
        // (`txn_id = (generation << 32) | counter`), so ids never alias across
        // reopens (review fix #11). Persist the bumped generation to the catalog.
        if existing {
            cat.open_generation = cat.open_generation.wrapping_add(1);
            catalog::write_atomic(&root, &cat, meta_dek.as_ref())?;
        }
        let next_txn_id = (cat.open_generation << 32) | 1;

        Ok(Self {
            root,
            catalog: RwLock::new(cat),
            epoch,
            snapshots,
            page_cache,
            decoded_cache,
            commit_lock,
            shared_wal,
            next_txn_id: Mutex::new(next_txn_id),
            tables: RwLock::new(tables),
            kek,
            ddl_lock: Mutex::new(()),
            meta_dek,
            conflicts: crate::txn::ConflictIndex::new(),
            active_txns: crate::txn::ActiveTxns::new(),
            poisoned: std::sync::atomic::AtomicBool::new(false),
            spill_threshold: std::sync::atomic::AtomicU64::new(64 * 1024 * 1024),
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

    /// Resolve a table name → id (live tables only). pub(crate) so the
    /// transaction layer can stage by name.
    pub fn table_id(&self, name: &str) -> Result<u64> {
        let cat = self.catalog.read();
        cat.live(name)
            .map(|e| e.table_id)
            .ok_or_else(|| MongrelError::NotFound(format!("table {name:?} not found")))
    }

    /// Begin a new transaction reading at the current visible epoch.
    pub fn begin(&self) -> crate::txn::Transaction<'_> {
        let txn_id = self.alloc_txn_id();
        let read = Snapshot::at(self.epoch.visible());
        crate::txn::Transaction::new(self, txn_id, read)
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

    /// Register a txn in `ActiveTxns` (spec §9.2, review fix #12). Called from
    /// `Transaction::new` so registration happens **before** any read.
    pub(crate) fn register_active(&self, epoch: Epoch) -> crate::txn::ActiveTxnGuard<'_> {
        self.active_txns.register(epoch)
    }

    /// Seal a transaction (spec §9.3):
    /// 1. Prepare — derive write keys, allocate row ids (brief table locks).
    /// 2. Sequencer — validate-first under the WAL mutex; abort on conflict
    ///    with no epoch consumed; assign epoch, append data records + TxnCommit,
    ///    group-sync, record conflict keys.
    /// 3. Publish — apply to tables, advance visible in-order.
    pub(crate) fn commit_transaction(
        &self,
        txn_id: u64,
        read_epoch: Epoch,
        staging: Vec<(u64, crate::txn::Staged)>,
    ) -> Result<Epoch> {
        use crate::memtable::Row;
        use crate::txn::{Staged, StagedOp, WriteKey};
        use crate::wal::Op;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        use std::sync::atomic::Ordering;

        if self.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }

        // ── 1. Prepare: derive write keys + allocate row ids ──
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
                        }
                    }
                    Staged::Delete(rid) => keys.push(WriteKey::Row {
                        table_id: *table_id,
                        row_id: rid.0,
                    }),
                }
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

                let mut writer = crate::sorted_run::RunWriter::new(&schema, run_id, Epoch(0), 0);
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
        let (new_epoch, applies) = {
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
                return Err(MongrelError::Conflict(
                    "write-write conflict (sequencer delta re-check)".into(),
                ));
            }

            let new_epoch = self.epoch.bump_assigned();
            let tables = self.tables.read();
            let mut applies: Vec<(u64, Vec<StagedOp>)> = Vec::new();

            for (table_id, staged) in &staging {
                // Skip puts for tables that were spilled — their data is in a
                // pending run, not in streamed Put records.
                if spilled_tables.contains(table_id) && matches!(staged, Staged::Put(_)) {
                    continue;
                }
                let handle = tables.get(table_id).ok_or_else(|| {
                    MongrelError::NotFound(format!("table {table_id} not mounted"))
                })?;
                let mut t = handle.lock();
                let mut ops = Vec::new();
                match staged {
                    Staged::Put(cells) => {
                        let row_id = t.alloc_row_id();
                        let mut row = Row::new(row_id, new_epoch);
                        for (c, v) in cells {
                            row.columns.insert(*c, v.clone());
                        }
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
                }
                applies.push((*table_id, ops));
            }

            wal.append_commit(txn_id, new_epoch, &added_runs)?;
            wal.group_sync().inspect_err(|_| {
                self.poisoned.store(true, Ordering::Relaxed);
            })?;

            self.conflicts.record(&write_keys, new_epoch);
            (new_epoch, applies)
        };

        // ── 3. Publish: link spilled runs + apply non-spilled ops ──
        {
            let tables = self.tables.read();
            // Link spilled runs first.
            for s in &spilled {
                if let Some(handle) = tables.get(&s.table_id) {
                    let mut t = handle.lock();
                    let dest = t.run_path(s.run_id as u64);
                    std::fs::rename(&s.pending_path, &dest)?;
                    // Clean up the now-empty `_txn/<txn_id>/` dir.
                    if let Some(parent) = s.pending_path.parent() {
                        let _ = std::fs::remove_dir_all(parent);
                    }
                    t.link_run(crate::manifest::RunRef {
                        run_id: s.run_id,
                        level: 0,
                        epoch_created: new_epoch.0,
                        row_count: s.row_count,
                    });
                    // Apply the run's rows to indexes + memtable + live_count.
                    // The merge logic in `visible_rows` deduplicates by row_id,
                    // so having rows in both a run and the memtable is safe.
                    let mut rows = s.rows.clone();
                    for r in rows.iter_mut() {
                        r.committed_epoch = new_epoch;
                    }
                    t.apply_put_rows(rows);
                    t.invalidate_pending_cache();
                    t.persist_manifest(new_epoch)?;
                }
            }
            // Apply non-spilled ops.
            for (table_id, ops) in applies {
                if let Some(handle) = tables.get(&table_id) {
                    let mut t = handle.lock();
                    for op in ops {
                        match op {
                            StagedOp::Put(row) => t.apply_put_rows(vec![row]),
                            StagedOp::Delete(rid) => t.apply_delete(rid, new_epoch),
                        }
                    }
                    t.invalidate_pending_cache();
                    t.persist_manifest(new_epoch)?;
                }
            }
        }

        self.advance_visible(new_epoch);
        Ok(new_epoch)
    }

    /// Advance `visible` in-order: epoch E becomes visible only once E and all
    /// prior unpublished epochs have finished publishing (spec §9.3e). The
    /// in-order gate lives on the shared [`EpochAuthority`] so this path, the
    /// single-table `Table::commit` path, and DDL all share one watermark and
    /// can never publish out of assigned order under concurrency.
    fn advance_visible(&self, published: Epoch) {
        self.epoch.publish_in_order(published);
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

    /// Create a new table. The DDL is first logged to the shared WAL
    /// (`Op::Ddl(CreateTable)` + `TxnCommit`) and group-synced so it is durable
    /// BEFORE the in-memory catalog and table map are mutated; the catalog
    /// checkpoint is rewritten afterwards (spec §15, review fix #16). A reopen
    /// that sees a stale catalog still recovers the table by replaying the Ddl.
    pub fn create_table(&self, name: &str, schema: Schema) -> Result<u64> {
        use crate::wal::DdlOp;

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
        let txn_id = self.alloc_txn_id();

        // Stamp the schema_id with the unique table_id so every table in the
        // database has a distinct schema_id (caller-provided values are
        // ignored to prevent collisions).
        let mut schema = schema;
        schema.schema_id = table_id;

        // 1. Log the DDL + commit marker to the shared WAL and fsync (durable).
        let schema_json = DdlOp::encode_schema(&schema)?;
        {
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
            wal.append_commit(txn_id, epoch, &[])?;
            wal.group_sync()?;
        }

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

        self.advance_visible(epoch);
        Ok(table_id)
    }

    /// Logically drop a table, logging the DDL through the shared WAL first.
    pub fn drop_table(&self, name: &str) -> Result<()> {
        use crate::wal::DdlOp;

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
        let txn_id = self.alloc_txn_id();
        {
            let mut wal = self.shared_wal.lock();
            wal.append(
                txn_id,
                table_id,
                crate::wal::Op::Ddl(DdlOp::DropTable { table_id }),
            )?;
            wal.append_commit(txn_id, epoch, &[])?;
            wal.group_sync()?;
        }

        {
            let mut cat = self.catalog.write();
            let entry = cat
                .tables
                .iter_mut()
                .find(|t| t.table_id == table_id)
                .ok_or_else(|| MongrelError::NotFound(format!("table {name:?} not found")))?;
            entry.state = TableState::Dropped { at_epoch: epoch.0 };
        }
        catalog::write_atomic(&self.root, &self.catalog.read(), self.meta_dek.as_ref())?;
        self.tables.write().remove(&table_id);

        self.advance_visible(epoch);
        Ok(())
    }

    /// Retention-gated garbage collection (spec §6.4, §7.4, §16). Deletes:
    /// - Dropped-table subdirs whose `at_epoch < min_active_snapshot`.
    /// - Stale `_txn/` dirs (aborted/crashed large-txn pending runs).
    ///
    /// Returns the number of items reclaimed.
    pub fn gc(&self) -> Result<usize> {
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

        // Sweep stale _txn/ dirs on remaining live tables.
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
            if txn_dir.exists() {
                std::fs::remove_dir_all(&txn_dir)?;
                reclaimed += 1;
            }
        }

        Ok(reclaimed)
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

    /// Verify multi-table integrity (spec §16). Checks that every live table's
    /// manifest is readable and every `RunRef`'s file exists, has the correct
    /// magic header, and has a well-formed footer (not truncated). Returns a
    /// list of issues found (empty = healthy).
    pub fn check(&self) -> Vec<CheckIssue> {
        let mut issues = Vec::new();
        let cat = self.catalog.read();
        for entry in &cat.tables {
            if !matches!(entry.state, TableState::Live) {
                continue;
            }
            let tdir = self.root.join(TABLES_DIR).join(entry.table_id.to_string());
            let manifest_meta_dek = crate::encryption::meta_dek_for(self.kek.as_deref());
            match crate::manifest::read(&tdir, manifest_meta_dek.as_ref()) {
                Ok(m) => {
                    for rr in &m.runs {
                        let run_path = tdir
                            .join(crate::engine::RUNS_DIR)
                            .join(format!("r-{}.sr", rr.run_id));
                        match verify_run_file(&run_path) {
                            Ok(()) => {}
                            Err(reason) => {
                                issues.push(CheckIssue {
                                    table_id: entry.table_id,
                                    table_name: entry.name.clone(),
                                    severity: "error".into(),
                                    description: reason,
                                });
                            }
                        }
                    }
                }
                Err(e) => {
                    issues.push(CheckIssue {
                        table_id: entry.table_id,
                        table_name: entry.name.clone(),
                        severity: "error".into(),
                        description: format!("manifest read failed: {e}"),
                    });
                }
            }
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
        let bad_tables: std::collections::HashSet<u64> = issues
            .iter()
            .filter(|i| i.severity == "error")
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

    // Pass 2: stage data per table, gated by flushed_epoch.
    type TableStage = (Vec<Row>, Vec<(RowId, Epoch)>);
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
                stage.entry(table_id).or_default().0.extend(rows);
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
                stage.entry(table_id).or_default().1.extend(dels);
            }
            _ => {}
        }
    }

    for (table_id, (rows, deletes)) in stage {
        let Some(handle) = tables.get(&table_id) else {
            continue;
        };
        let mut t = handle.lock();
        t.recover_apply(rows, deletes)?;
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
            t.link_run(crate::manifest::RunRef {
                run_id: ar.run_id,
                level: ar.level,
                epoch_created: *ce,
                row_count: ar.row_count,
            });
        }
    }

    epoch.advance_recovered(Epoch(max_epoch));
    Ok(())
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
                if let Some(entry) = cat.tables.iter_mut().find(|t| t.table_id == table_id) {
                    if matches!(entry.state, TableState::Live) {
                        entry.state = TableState::Dropped { at_epoch: ce };
                        changed = true;
                    }
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

/// Verify a `.sr` run file is non-truncated and has valid magic + footer.
/// Returns `Ok(())` on success, `Err(reason_string)` on any integrity issue.
fn verify_run_file(path: &Path) -> std::result::Result<(), String> {
    use std::io::{Read, Seek, SeekFrom};

    let metadata = if path.exists() {
        std::fs::metadata(path).map_err(|e| format!("run file stat failed: {e}"))?
    } else {
        return Err(format!(
            "missing run file: {}",
            path.file_name().unwrap_or_default().to_string_lossy()
        ));
    };
    let size = metadata.len() as usize;
    const MIN_RUN_SIZE: usize = 8 + 48;
    if size < MIN_RUN_SIZE {
        return Err(format!(
            "run file too small ({} bytes, expected ≥ {})",
            size, MIN_RUN_SIZE
        ));
    }

    let mut file = std::fs::File::open(path).map_err(|e| format!("run file open failed: {e}"))?;

    let mut magic = [0u8; 8];
    file.read_exact(&mut magic)
        .map_err(|e| format!("run header read failed: {e}"))?;
    if magic != crate::sorted_run::RUN_MAGIC {
        return Err(format!(
            "run header magic mismatch: expected {:?}, got {:?}",
            crate::sorted_run::RUN_MAGIC,
            magic
        ));
    }

    let tail_len = size.min(80);
    let mut tail = vec![0u8; tail_len];
    file.seek(SeekFrom::End(-(tail_len as i64)))
        .map_err(|e| format!("run seek failed: {e}"))?;
    file.read_exact(&mut tail)
        .map_err(|e| format!("run footer read failed: {e}"))?;
    let magic_found = tail.windows(8).any(|w| w == crate::sorted_run::RUN_MAGIC);
    if !magic_found {
        return Err("run footer magic not found (truncated or corrupt)".into());
    }

    Ok(())
}
