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

/// A multi-table database: one catalog, one epoch clock, shared caches, and a
/// live map of name → `Arc<Table>`.
pub struct Database {
    root: PathBuf,
    catalog: RwLock<Catalog>,
    epoch: Arc<EpochAuthority>,
    snapshots: Arc<SnapshotRegistry>,
    page_cache: Arc<parking_lot::Mutex<crate::cache::PageCache>>,
    decoded_cache: Arc<parking_lot::Mutex<crate::cache::DecodedPageCache>>,
    commit_lock: Arc<Mutex<()>>,
    tables: RwLock<HashMap<u64, TableHandle>>,
    kek: Option<Arc<crate::encryption::Kek>>,
    /// Serializes DDL (create/drop table); data commits serialize through
    /// `commit_lock` shared via `SharedCtx`.
    ddl_lock: Mutex<()>,
    meta_dek: Option<[u8; META_DEK_LEN]>,
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
        Self::finish_open(root, cat, kek, meta_dek)
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
        Self::finish_open(root, cat, kek, meta_dek)
    }

    fn finish_open(
        root: PathBuf,
        cat: Catalog,
        kek: Option<Arc<crate::encryption::Kek>>,
        meta_dek: Option<[u8; META_DEK_LEN]>,
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

        // Open every live table against the shared context. Each `open_in`
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

        Ok(Self {
            root,
            catalog: RwLock::new(cat),
            epoch,
            snapshots,
            page_cache,
            decoded_cache,
            commit_lock,
            tables: RwLock::new(tables),
            kek,
            ddl_lock: Mutex::new(()),
            meta_dek,
        })
    }

    /// The current reader-visible epoch.
    pub fn visible_epoch(&self) -> Epoch {
        self.epoch.visible()
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

    /// Create a new table. Allocates an id, writes the schema + empty manifest,
    /// appends a catalog entry, and mounts the table.
    pub fn create_table(&self, name: &str, schema: Schema) -> Result<u64> {
        let _g = self.ddl_lock.lock();
        {
            let cat = self.catalog.read();
            if cat.live(name).is_some() {
                return Err(MongrelError::InvalidArgument(format!(
                    "table {name:?} already exists"
                )));
            }
        }
        let mut cat = self.catalog.write();
        let table_id = cat.next_table_id;
        cat.next_table_id += 1;
        let created_epoch = self.epoch.visible().0;
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
        cat.tables.push(CatalogEntry {
            table_id,
            name: name.to_string(),
            schema,
            state: TableState::Live,
            created_epoch,
        });
        catalog::write_atomic(&self.root, &cat, self.meta_dek.as_ref())?;
        self.tables
            .write()
            .insert(table_id, Arc::new(Mutex::new(table)));
        Ok(table_id)
    }

    /// Logically drop a table. Its rows become unqueryable immediately; the
    /// physical subdir is reaped later by the retention-gated GC (P3.6).
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let _g = self.ddl_lock.lock();
        let mut cat = self.catalog.write();
        let entry = cat
            .tables
            .iter_mut()
            .find(|t| t.name == name && matches!(t.state, TableState::Live))
            .ok_or_else(|| MongrelError::NotFound(format!("table {name:?} not found")))?;
        let id = entry.table_id;
        entry.state = TableState::Dropped {
            at_epoch: self.epoch.visible().0,
        };
        catalog::write_atomic(&self.root, &cat, self.meta_dek.as_ref())?;
        drop(cat);
        self.tables.write().remove(&id);
        Ok(())
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
