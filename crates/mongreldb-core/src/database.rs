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
    /// P3.1: write-key → commit_epoch for first-committer-wins conflict
    /// detection (spec §9.2).
    conflicts: crate::txn::ConflictIndex,
    /// P3.1: min read_epoch of all in-flight txns, drives conflict-index
    /// pruning (spec §9.2, review fix #12).
    active_txns: crate::txn::ActiveTxns,
    /// P3.2: epochs that have published but not yet been absorbed into the
    /// in-order `visible` watermark.
    pending_visible: Mutex<std::collections::BTreeSet<u64>>,
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
            pending_visible: Mutex::new(std::collections::BTreeSet::new()),
            poisoned: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// The current reader-visible epoch.
    pub fn visible_epoch(&self) -> Epoch {
        self.epoch.visible()
    }

    /// Resolve a table name → id (live tables only). pub(crate) so the
    /// transaction layer can stage by name.
    pub(crate) fn table_id(&self, name: &str) -> Result<u64> {
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

        // ── 2. Sequencer: validate-first → assign → append → sync → record ──
        let (new_epoch, applies) = {
            let mut wal = self.shared_wal.lock();

            // Validate first — abort on conflict, no epoch consumed.
            if self.conflicts.conflicts(&write_keys, read_epoch) {
                return Err(MongrelError::Conflict(
                    "write-write conflict (first-committer-wins)".into(),
                ));
            }

            let new_epoch = self.epoch.bump_assigned();
            let tables = self.tables.read();
            let mut applies: Vec<(u64, Vec<StagedOp>)> = Vec::new();

            for (table_id, staged) in &staging {
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

            wal.append_commit(txn_id, new_epoch, &[])?;
            wal.group_sync().inspect_err(|_| {
                self.poisoned.store(true, Ordering::Relaxed);
            })?;

            self.conflicts.record(&write_keys, new_epoch);
            (new_epoch, applies)
        };

        // ── 3. Publish: apply to tables, advance visible in-order ──
        {
            let tables = self.tables.read();
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
    /// prior unpublished epochs have finished publishing (spec §9.3e).
    fn advance_visible(&self, published: Epoch) {
        let mut pending = self.pending_visible.lock();
        pending.insert(published.0);
        let mut vis = self.epoch.visible().0;
        while pending.remove(&(vis + 1)) {
            vis += 1;
        }
        if vis > self.epoch.visible().0 {
            self.epoch.publish_visible(Epoch(vis));
        }
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

        self.epoch.publish_visible(epoch);
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

        self.epoch.publish_visible(epoch);
        Ok(())
    }

    /// Allocate the next generation-scoped transaction id.
    fn alloc_txn_id(&self) -> u64 {
        let mut g = self.next_txn_id.lock();
        let id = *g;
        *g = g.wrapping_add(1);
        id
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

    // Pass 1: committed-txn outcomes.
    let mut committed: HashMap<u64, u64> = HashMap::new();
    for r in &records {
        if let Op::TxnCommit { epoch: ce, .. } = r.op {
            committed.insert(r.txn_id, ce);
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
                    let manifest_meta_dek = crate::encryption::meta_dek_for(
                        // We don't have the kek here, but the manifest must be
                        // writable. Reconstruct from meta_dek is not possible;
                        // for encrypted DBs the table dir always exists (created
                        // by create_in before crash), so this path is plaintext-only.
                        None,
                    );
                    let mut m = crate::manifest::Manifest::new(table_id, schema.schema_id);
                    crate::manifest::write_atomic(&tdir, &mut m, manifest_meta_dek.as_ref())?;
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
