//! Multi-table `Database` container (spec §5, §6, §10).
//!
//! Owns the shared services — catalog, dual-counter epoch authority, shared
//! raw/decoded page caches, snapshot-retention registry, and the DB-wide KEK —
//! and mounts per-table [`Table`] engines under `tables/<id>/` that borrow them.
//! P1 scope: per-table WALs remain (collapsed into one shared WAL in P2); the
//! win here is one consistent commit clock across tables and one reopen path.

use crate::catalog::{self, Catalog, CatalogEntry, TableState, META_DEK_LEN};
use crate::engine::{SharedCtx, Table};
use crate::epoch::{Epoch, EpochAuthority, EpochGuard, MaintenanceReceipt, Snapshot};
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
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

pub const TABLES_DIR: &str = "tables";
pub const VTAB_DIR: &str = "_vtab";
pub const META_DIR: &str = "_meta";
pub const KEYS_FILENAME: &str = "keys";
pub const HISTORY_RETENTION_FILENAME: &str = "history_retention";
pub const CTAS_BUILD_TABLE_PREFIX: &str = "__mongreldb_ctas_build_";

/// Sentinel `table_id` for `CheckIssue`s that concern the shared WAL rather
/// than any table. `u64::MAX` is never allocated to a real table (the catalog
/// mints ids from 0 upward), so [`Database::doctor`] can safely skip them.
pub const WAL_TABLE_ID: u64 = u64::MAX;
/// Sentinel `table_id` for `CheckIssue`s that concern external-table module
/// state instead of an ordinary table.
pub const EXTERNAL_TABLE_ID: u64 = u64::MAX - 1;

fn advance_security_version(catalog: &mut Catalog) -> Result<()> {
    catalog.security_version = catalog.security_version.checked_add(1).ok_or_else(|| {
        MongrelError::Conflict("security catalog version space is exhausted".into())
    })?;
    Ok(())
}

type OpenLeaseId = u64;

static DATABASE_OPEN_WAIT_COUNT: AtomicU64 = AtomicU64::new(0);
static DATABASE_OPEN_FAILURE_COUNT: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DatabaseOpenMetrics {
    pub lock_waits: u64,
    pub failures: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum DatabaseOpenKey {
    IntendedPath(PathBuf),
    FileIdentity(crate::durable_file::DurableFileIdentity),
}

#[derive(Debug)]
enum ProcessOpenState {
    Opening { lease_id: OpenLeaseId },
    Open { lease_id: OpenLeaseId },
    Closing { lease_id: OpenLeaseId },
}

impl ProcessOpenState {
    fn lease_id(&self) -> OpenLeaseId {
        match self {
            Self::Opening { lease_id } | Self::Open { lease_id } | Self::Closing { lease_id } => {
                *lease_id
            }
        }
    }
}

#[derive(Default)]
struct ProcessOpenRegistry {
    next_lease_id: OpenLeaseId,
    entries: HashMap<DatabaseOpenKey, ProcessOpenState>,
}

fn process_open_registry() -> &'static Mutex<ProcessOpenRegistry> {
    static REGISTRY: std::sync::OnceLock<Mutex<ProcessOpenRegistry>> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(ProcessOpenRegistry::default()))
}

fn same_process_locked(path: &Path) -> MongrelError {
    MongrelError::DatabaseLocked {
        path: path.to_path_buf(),
        message: "database is already open in this process; reuse the existing Arc<Database>"
            .into(),
    }
}

struct OpenReservation {
    lease_id: OpenLeaseId,
    keys: Vec<DatabaseOpenKey>,
    committed: bool,
}

impl OpenReservation {
    fn acquire(key: DatabaseOpenKey, display_path: &Path) -> Result<Self> {
        let mut registry = process_open_registry().lock();
        if registry.entries.contains_key(&key) {
            DATABASE_OPEN_FAILURE_COUNT.fetch_add(1, Ordering::Relaxed);
            return Err(same_process_locked(display_path));
        }
        registry.next_lease_id = registry.next_lease_id.checked_add(1).ok_or_else(|| {
            MongrelError::Full("process database-open lease namespace exhausted".into())
        })?;
        let lease_id = registry.next_lease_id;
        registry
            .entries
            .insert(key.clone(), ProcessOpenState::Opening { lease_id });
        Ok(Self {
            lease_id,
            keys: vec![key],
            committed: false,
        })
    }

    fn into_lease(
        mut self,
        bootstrap_file: std::fs::File,
        canonical_path: PathBuf,
    ) -> ExclusiveDatabaseLease {
        self.committed = true;
        ExclusiveDatabaseLease {
            lease_id: self.lease_id,
            keys: std::mem::take(&mut self.keys),
            bootstrap_file,
            legacy_file: None,
            canonical_path,
            durable_root: None,
            owner_pid: std::process::id(),
            opened: false,
        }
    }
}

impl Drop for OpenReservation {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        DATABASE_OPEN_FAILURE_COUNT.fetch_add(1, Ordering::Relaxed);
        let mut registry = process_open_registry().lock();
        for key in &self.keys {
            if registry
                .entries
                .get(key)
                .is_some_and(|state| state.lease_id() == self.lease_id)
            {
                registry.entries.remove(key);
            }
        }
    }
}

struct ExclusiveDatabaseLease {
    lease_id: OpenLeaseId,
    keys: Vec<DatabaseOpenKey>,
    bootstrap_file: std::fs::File,
    legacy_file: Option<std::fs::File>,
    canonical_path: PathBuf,
    durable_root: Option<Arc<crate::durable_file::DurableRoot>>,
    owner_pid: u32,
    opened: bool,
}

impl ExclusiveDatabaseLease {
    fn claim_root_identity(&mut self, root: &crate::durable_file::DurableRoot) -> Result<()> {
        let key = DatabaseOpenKey::FileIdentity(root.file_identity()?);
        if self.keys.contains(&key) {
            return Ok(());
        }
        let mut registry = process_open_registry().lock();
        if registry.entries.contains_key(&key) {
            return Err(same_process_locked(&self.canonical_path));
        }
        registry.entries.insert(
            key.clone(),
            ProcessOpenState::Opening {
                lease_id: self.lease_id,
            },
        );
        self.keys.push(key);
        Ok(())
    }

    fn mark_open(&mut self) -> Result<()> {
        let mut registry = process_open_registry().lock();
        if self.keys.iter().any(|key| {
            registry
                .entries
                .get(key)
                .is_none_or(|state| state.lease_id() != self.lease_id)
        }) {
            return Err(MongrelError::Conflict(
                "database-open reservation changed during initialization".into(),
            ));
        }
        for key in &self.keys {
            registry.entries.insert(
                key.clone(),
                ProcessOpenState::Open {
                    lease_id: self.lease_id,
                },
            );
        }
        self.opened = true;
        Ok(())
    }
}

impl Drop for ExclusiveDatabaseLease {
    fn drop(&mut self) {
        if std::process::id() != self.owner_pid {
            return;
        }
        if !self.opened {
            DATABASE_OPEN_FAILURE_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        {
            let mut registry = process_open_registry().lock();
            for key in &self.keys {
                if registry
                    .entries
                    .get(key)
                    .is_some_and(|state| state.lease_id() == self.lease_id)
                {
                    registry.entries.insert(
                        key.clone(),
                        ProcessOpenState::Closing {
                            lease_id: self.lease_id,
                        },
                    );
                }
            }
        }
        if let Some(file) = &self.legacy_file {
            let _ = fs2::FileExt::unlock(file);
        }
        let _ = fs2::FileExt::unlock(&self.bootstrap_file);
        let mut registry = process_open_registry().lock();
        for key in &self.keys {
            if registry
                .entries
                .get(key)
                .is_some_and(|state| state.lease_id() == self.lease_id)
            {
                registry.entries.remove(key);
            }
        }
    }
}

fn commit_prepare_checkpoint(
    control: Option<&crate::ExecutionControl>,
    index: usize,
) -> Result<()> {
    if index.is_multiple_of(256) {
        if let Some(control) = control {
            control.checkpoint()?;
        }
    }
    Ok(())
}

fn finish_controlled_commit_attempt(
    result: Result<Epoch>,
    after_commit: &mut Option<&mut dyn FnMut(Option<Epoch>) -> Result<()>>,
) -> Result<Epoch> {
    let Some(after_commit) = after_commit.as_mut() else {
        return result;
    };
    match result {
        Ok(epoch) => match (**after_commit)(Some(epoch)) {
            Ok(()) => Ok(epoch),
            Err(error) => Err(MongrelError::DurableCommit {
                epoch: epoch.0,
                message: error.to_string(),
            }),
        },
        Err(MongrelError::DurableCommit { epoch, message }) => {
            let callback_error = (**after_commit)(Some(Epoch(epoch))).err();
            Err(MongrelError::DurableCommit {
                epoch,
                message: callback_error
                    .map(|error| format!("{message}; commit callback: {error}"))
                    .unwrap_or(message),
            })
        }
        Err(error) => match (**after_commit)(None) {
            Ok(()) => Err(error),
            Err(callback_error) => Err(MongrelError::Other(format!(
                "{error}; commit callback: {callback_error}"
            ))),
        },
    }
}

fn current_unix_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[cfg(feature = "encryption")]
fn read_encryption_salt(
    root: &crate::durable_file::DurableRoot,
) -> Result<[u8; crate::encryption::SALT_LEN]> {
    let mut file = root
        .open_regular(Path::new(META_DIR).join(KEYS_FILENAME))
        .map_err(|error| MongrelError::NotFound(format!("encryption salt file: {error}")))?;
    let length = file.metadata()?.len();
    if length != crate::encryption::SALT_LEN as u64 {
        return Err(MongrelError::Encryption(format!(
            "invalid encryption salt length: got {length}, expected {}",
            crate::encryption::SALT_LEN
        )));
    }
    let mut salt = [0_u8; crate::encryption::SALT_LEN];
    file.read_exact(&mut salt)?;
    Ok(salt)
}

fn incremental_aggregate_cache_key(
    table: &str,
    conditions: &[crate::query::Condition],
    column: Option<u16>,
    agg: crate::engine::NativeAgg,
    principal: Option<&crate::auth::Principal>,
    security_version: u64,
) -> u64 {
    use std::hash::{Hash, Hasher};
    let projection = column.as_ref().map(std::slice::from_ref);
    let query_key = crate::query::canonical_query_key(conditions, projection, security_version);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    table.hash(&mut hasher);
    query_key.hash(&mut hasher);
    match agg {
        crate::engine::NativeAgg::Count => 0u8,
        crate::engine::NativeAgg::Sum => 1,
        crate::engine::NativeAgg::Min => 2,
        crate::engine::NativeAgg::Max => 3,
        crate::engine::NativeAgg::Avg => 4,
    }
    .hash(&mut hasher);
    if let Some(principal) = principal {
        principal.user_id.hash(&mut hasher);
        principal.created_epoch.hash(&mut hasher);
        principal.username.hash(&mut hasher);
        principal.is_admin.hash(&mut hasher);
        let mut roles = principal.roles.clone();
        roles.sort_unstable();
        roles.hash(&mut hasher);
    }
    hasher.finish()
}

fn read_history_retention(
    root: &crate::durable_file::DurableRoot,
    current_epoch: Epoch,
) -> Result<(u64, Epoch)> {
    const MAX_HISTORY_RETENTION_BYTES: u64 = 128;
    let file = match root.open_regular(Path::new(META_DIR).join(HISTORY_RETENTION_FILENAME)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((0, current_epoch));
        }
        Err(error) => return Err(error.into()),
    };
    let length = file.metadata()?.len();
    if length > MAX_HISTORY_RETENTION_BYTES {
        return Err(MongrelError::ResourceLimitExceeded {
            resource: "history retention bytes",
            requested: usize::try_from(length).unwrap_or(usize::MAX),
            limit: MAX_HISTORY_RETENTION_BYTES as usize,
        });
    }
    let mut bytes = Vec::with_capacity(length as usize);
    file.take(MAX_HISTORY_RETENTION_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length {
        return Err(MongrelError::Other(
            "history retention length changed while reading".into(),
        ));
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|error| MongrelError::Other(format!("history retention encoding: {error}")))?;
    let mut fields = text.split_whitespace();
    let epochs = fields
        .next()
        .ok_or_else(|| MongrelError::Other("history retention file is empty".into()))?
        .parse::<u64>()
        .map_err(|error| MongrelError::Other(format!("history retention epochs: {error}")))?;
    let start = fields
        .next()
        .ok_or_else(|| MongrelError::Other("history retention start is missing".into()))?
        .parse::<u64>()
        .map_err(|error| MongrelError::Other(format!("history retention start: {error}")))?;
    if fields.next().is_some() || start > current_epoch.0 {
        return Err(MongrelError::Other(
            "history retention file has trailing fields or a future start epoch".into(),
        ));
    }
    Ok((epochs, Epoch(start)))
}

fn write_history_retention<F>(
    root: &Path,
    epochs: u64,
    start: Epoch,
    after_publish: F,
) -> Result<()>
where
    F: FnOnce(),
{
    let meta = root.join(META_DIR);
    let path = meta.join(HISTORY_RETENTION_FILENAME);
    let bytes = format!("{epochs} {}\n", start.0);
    crate::durable_file::write_atomic_with_after(&path, bytes.as_bytes(), after_publish)?;
    Ok(())
}

struct PreparedBackupDestination {
    parent: crate::durable_file::DurableRoot,
    destination_name: std::ffi::OsString,
    destination_path: PathBuf,
    stage_name: std::ffi::OsString,
    stage: Option<Box<crate::durable_file::DurableRoot>>,
}

fn prepare_backup_destination(
    source: &Path,
    destination: &Path,
) -> Result<PreparedBackupDestination> {
    let destination_name = destination
        .file_name()
        .ok_or_else(|| MongrelError::InvalidArgument("invalid backup destination".into()))?
        .to_os_string();
    let requested_parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    crate::durable_file::create_directory_all(requested_parent)?;
    let parent = crate::durable_file::DurableRoot::open(requested_parent)?;
    prepare_backup_destination_in(source, &parent, &destination_name)
}

fn prepare_backup_destination_in(
    source: &Path,
    parent: &crate::durable_file::DurableRoot,
    destination_name: &std::ffi::OsStr,
) -> Result<PreparedBackupDestination> {
    let source = source.canonicalize()?;
    if parent.canonical_path().starts_with(&source) {
        return Err(MongrelError::InvalidArgument(
            "backup destination must not be inside the source database".into(),
        ));
    }
    if parent.entry_exists(Path::new(&destination_name))? {
        return Err(MongrelError::Conflict(format!(
            "backup destination already exists: {}",
            parent.canonical_path().join(destination_name).display()
        )));
    }
    let mut stage_name = None;
    for _ in 0..128 {
        let mut nonce = [0_u8; 8];
        crate::encryption::fill_random(&mut nonce)?;
        let suffix = nonce
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let name = std::ffi::OsString::from(format!(
            ".{}.backup-stage-{}-{suffix}",
            destination_name.to_string_lossy(),
            std::process::id()
        ));
        match parent.create_directory_new(Path::new(&name)) {
            Ok(()) => {
                stage_name = Some(name);
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    let stage_name = stage_name
        .ok_or_else(|| MongrelError::Conflict("could not allocate backup staging path".into()))?;
    let stage = parent.open_directory(Path::new(&stage_name))?;
    Ok(PreparedBackupDestination {
        destination_path: parent.canonical_path().join(destination_name),
        destination_name: destination_name.to_os_string(),
        stage_name,
        stage: Some(Box::new(stage)),
        parent: parent.try_clone()?,
    })
}

fn copy_backup_boundary(
    source_root: &Path,
    destination_root: &crate::durable_file::DurableRoot,
    deferred_runs: &HashSet<PathBuf>,
    copied: &mut Vec<PathBuf>,
    control: Option<&crate::ExecutionControl>,
) -> Result<()> {
    let mut visited = 0;
    crate::durable_file::walk_regular_files_nofollow(
        source_root,
        |relative, is_directory| {
            if visited % 256 == 0 {
                if let Some(control) = control {
                    control.checkpoint()?;
                }
            }
            visited += 1;
            if backup_path_excluded(relative) {
                return Ok(false);
            }
            if is_directory {
                return Ok(true);
            }
            if deferred_runs.contains(relative) {
                return Ok(false);
            }
            Ok(!(relative
                .parent()
                .and_then(Path::file_name)
                .is_some_and(|parent| parent == "_runs")
                && relative
                    .extension()
                    .is_some_and(|extension| extension == "sr")))
        },
        |relative| {
            destination_root.create_directory_all(relative)?;
            Ok(())
        },
        |relative, source| {
            destination_root.copy_new_from(relative, source)?;
            copied.push(relative.to_path_buf());
            Ok(())
        },
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

pub trait ExternalTriggerBridge: Send + Sync {
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
    final_path: PathBuf,
    rows: Vec<crate::memtable::Row>,
    row_count: u64,
    min_rid: u64,
    max_rid: u64,
    content_hash: [u8; 32],
}

const SPILLED_WAL_PAYLOAD_MAX_BYTES: usize = 24 * 1024 * 1024;
const SPILLED_WAL_TOTAL_MAX_BYTES: usize = 256 * 1024 * 1024;

fn encode_spilled_row_chunks(
    rows: &[crate::memtable::Row],
    total_bytes: &mut usize,
    total_limit: usize,
    control: Option<&crate::ExecutionControl>,
) -> Result<Vec<Vec<u8>>> {
    let mut output = Vec::new();
    let mut start = 0;
    while start < rows.len() {
        // Bincode's sequence length prefix is a u64 with the workspace's
        // fixed-int options. `serialized_size` computes exact row sizes
        // without first allocating one transaction-sized buffer.
        let mut estimated_bytes = std::mem::size_of::<u64>();
        let mut end = start;
        while end < rows.len() {
            if end % 256 == 0 {
                if let Some(control) = control {
                    control.checkpoint()?;
                }
            }
            let row_bytes =
                usize::try_from(bincode::serialized_size(&rows[end])?).map_err(|_| {
                    MongrelError::ResourceLimitExceeded {
                        resource: "spilled WAL row bytes",
                        requested: usize::MAX,
                        limit: SPILLED_WAL_PAYLOAD_MAX_BYTES,
                    }
                })?;
            let next_bytes = estimated_bytes.checked_add(row_bytes).ok_or(
                MongrelError::ResourceLimitExceeded {
                    resource: "spilled WAL row bytes",
                    requested: usize::MAX,
                    limit: SPILLED_WAL_PAYLOAD_MAX_BYTES,
                },
            )?;
            if next_bytes > SPILLED_WAL_PAYLOAD_MAX_BYTES {
                break;
            }
            estimated_bytes = next_bytes;
            end += 1;
        }
        if end == start {
            return Err(MongrelError::ResourceLimitExceeded {
                resource: "spilled WAL row bytes",
                requested: estimated_bytes.saturating_add(1),
                limit: SPILLED_WAL_PAYLOAD_MAX_BYTES,
            });
        }
        let payload = bincode::serialize(&rows[start..end])?;
        if payload.len() > SPILLED_WAL_PAYLOAD_MAX_BYTES {
            return Err(MongrelError::ResourceLimitExceeded {
                resource: "spilled WAL row bytes",
                requested: payload.len(),
                limit: SPILLED_WAL_PAYLOAD_MAX_BYTES,
            });
        }
        let requested = total_bytes.checked_add(payload.len()).unwrap_or(usize::MAX);
        if requested > total_limit {
            return Err(MongrelError::ResourceLimitExceeded {
                resource: "spilled WAL transaction bytes",
                requested,
                limit: total_limit,
            });
        }
        *total_bytes = requested;
        output.push(payload);
        start = end;
    }
    Ok(output)
}

#[cfg(test)]
mod spilled_wal_encoding_tests {
    use super::*;

    #[test]
    fn logical_spill_payload_has_a_total_bound() {
        let rows = (0..4)
            .map(|row_id| crate::memtable::Row {
                row_id: crate::rowid::RowId(row_id),
                committed_epoch: Epoch::ZERO,
                columns: [(1, Value::Bytes(vec![0; 64]))].into_iter().collect(),
                deleted: false,
            })
            .collect::<Vec<_>>();
        let mut total = 0;
        let error = encode_spilled_row_chunks(&rows, &mut total, 32, None).unwrap_err();
        assert!(matches!(
            error,
            MongrelError::ResourceLimitExceeded {
                resource: "spilled WAL transaction bytes",
                ..
            }
        ));
    }
}

/// Move spill files to their final names before the WAL commit. Dropping this
/// guard restores pending names while commit is still known not to have begun.
/// It is disarmed immediately before the first WAL append, where the outcome
/// can become ambiguous and recovery may need the final names.
struct PreparedRunLinks {
    links: Vec<(PathBuf, PathBuf)>,
    armed: bool,
}

impl PreparedRunLinks {
    fn prepare(spilled: &[SpilledRun]) -> Result<Self> {
        let mut guard = Self {
            links: Vec::with_capacity(spilled.len()),
            armed: true,
        };
        for run in spilled {
            crate::durable_file::rename(&run.pending_path, &run.final_path)?;
            guard
                .links
                .push((run.pending_path.clone(), run.final_path.clone()));
        }
        Ok(guard)
    }

    fn disarm(&mut self) {
        self.armed = false;
        for (pending, _) in &self.links {
            if let Some(parent) = pending.parent() {
                let _ = std::fs::remove_dir_all(parent);
            }
        }
    }
}

impl Drop for PreparedRunLinks {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        for (pending, final_path) in self.links.iter().rev() {
            let _ = std::fs::rename(final_path, pending);
        }
    }
}

struct TableApplyBatch {
    table_id: u64,
    handle: TableHandle,
    ops: Vec<crate::txn::StagedOp>,
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

#[derive(Clone, PartialEq)]
struct TriggerCatalogBinding {
    triggers: Vec<TriggerEntry>,
    tables: Vec<(String, u64, u64)>,
    external_tables: Vec<(String, u64, u64)>,
}

fn trigger_catalog_binding(catalog: &Catalog) -> Option<TriggerCatalogBinding> {
    let mut triggers = catalog
        .triggers
        .iter()
        .filter(|entry| entry.trigger.enabled)
        .cloned()
        .collect::<Vec<_>>();
    if triggers.is_empty() {
        return None;
    }
    triggers.sort_by(|left, right| left.trigger.name.cmp(&right.trigger.name));
    let mut tables = catalog
        .tables
        .iter()
        .filter(|entry| matches!(entry.state, TableState::Live))
        .map(|entry| (entry.name.clone(), entry.table_id, entry.schema.schema_id))
        .collect::<Vec<_>>();
    tables.sort_unstable();
    let mut external_tables = catalog
        .external_tables
        .iter()
        .map(|entry| {
            (
                entry.name.clone(),
                entry.created_epoch,
                entry.declared_schema.schema_id,
            )
        })
        .collect::<Vec<_>>();
    external_tables.sort_unstable();
    Some(TriggerCatalogBinding {
        triggers,
        tables,
        external_tables,
    })
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

/// Exact table/security generation used by one successful authorized read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorizedReadStamp {
    pub table_id: u64,
    pub schema_id: u64,
    pub data_generation: u64,
    pub security_version: u64,
    pub snapshot: Snapshot,
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
const CDC_MAX_WAL_RECORDS: usize = 1_000_000;
const CDC_MAX_WAL_REPLAY_BYTES: usize = 256 * 1024 * 1024;
const CDC_MAX_EVENTS: usize = 100_000;
const CDC_MAX_ROWS: usize = 1_000_000;
const CDC_MAX_INLINE_PAYLOAD_BYTES: usize = 32 * 1024 * 1024;
const CDC_MAX_RETAINED_BYTES: usize = 256 * 1024 * 1024;

fn charge_cdc_bytes(total: &mut usize, amount: usize, resource: &'static str) -> Result<()> {
    let requested = total.saturating_add(amount);
    if requested > CDC_MAX_RETAINED_BYTES {
        return Err(MongrelError::ResourceLimitExceeded {
            resource,
            requested,
            limit: CDC_MAX_RETAINED_BYTES,
        });
    }
    *total = requested;
    Ok(())
}

fn cdc_row_storage_bytes(row: &crate::memtable::Row) -> usize {
    usize::try_from(row.estimated_bytes())
        .unwrap_or(usize::MAX)
        .saturating_add(std::mem::size_of::<crate::memtable::Row>())
}

fn cdc_row_json_bytes(row: &crate::memtable::Row) -> usize {
    let value_slot = std::mem::size_of::<serde_json::Value>();
    row.columns.values().fold(512_usize, |bytes, value| {
        let values = match value {
            Value::Bytes(values) => values.len(),
            Value::Json(values) => values.len(),
            Value::Embedding(values) => values.len(),
            _ => 1,
        };
        bytes.saturating_add(values.saturating_mul(value_slot))
    })
}

fn cdc_rows_json_bytes(rows: &[crate::memtable::Row]) -> usize {
    rows.iter().fold(0_usize, |bytes, row| {
        bytes.saturating_add(cdc_row_json_bytes(row))
    })
}

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

/// Mounted table with immutable, structurally shared scored-read generations.
#[derive(Clone)]
pub struct TableHandle {
    inner: TableHandleInner,
    generation_metrics: Arc<TableGenerationMetrics>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TableGenerationStats {
    pub active_read_generations: usize,
    pub max_live_read_generations: usize,
    pub cow_clone_count: u64,
    pub cow_clone_nanos: u64,
    pub estimated_cow_clone_bytes: u64,
    pub writer_wait_nanos: u64,
}

#[derive(Default)]
#[doc(hidden)]
pub struct TableGenerationMetrics {
    active_read_generations: AtomicUsize,
    max_live_read_generations: AtomicUsize,
    cow_clone_count: AtomicU64,
    cow_clone_nanos: AtomicU64,
    estimated_cow_clone_bytes: AtomicU64,
    writer_wait_nanos: AtomicU64,
}

impl TableGenerationMetrics {
    fn activate(self: &Arc<Self>, table: Table) -> Arc<TableReadGeneration> {
        let active = self.active_read_generations.fetch_add(1, Ordering::Relaxed) + 1;
        self.max_live_read_generations
            .fetch_max(active, Ordering::Relaxed);
        Arc::new(TableReadGeneration {
            table,
            metrics: Arc::clone(self),
        })
    }

    fn stats(&self) -> TableGenerationStats {
        TableGenerationStats {
            active_read_generations: self.active_read_generations.load(Ordering::Relaxed),
            max_live_read_generations: self.max_live_read_generations.load(Ordering::Relaxed),
            cow_clone_count: self.cow_clone_count.load(Ordering::Relaxed),
            cow_clone_nanos: self.cow_clone_nanos.load(Ordering::Relaxed),
            estimated_cow_clone_bytes: self.estimated_cow_clone_bytes.load(Ordering::Relaxed),
            writer_wait_nanos: self.writer_wait_nanos.load(Ordering::Relaxed),
        }
    }
}

/// Immutable, structurally shared snapshot used by scored readers.
pub struct TableReadGeneration {
    table: Table,
    metrics: Arc<TableGenerationMetrics>,
}

impl std::ops::Deref for TableReadGeneration {
    type Target = Table;

    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

impl Drop for TableReadGeneration {
    fn drop(&mut self) {
        self.metrics
            .active_read_generations
            .fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
enum TableHandleInner {
    CopyOnWrite(Arc<RwLock<Arc<Table>>>),
    Direct(Arc<Mutex<Table>>),
}

pub enum TableGuard<'a> {
    CopyOnWrite {
        table: parking_lot::RwLockWriteGuard<'a, Arc<Table>>,
        metrics: Arc<TableGenerationMetrics>,
    },
    Direct {
        table: parking_lot::MutexGuard<'a, Table>,
    },
}

impl TableHandle {
    fn new(table: Table) -> Self {
        Self {
            inner: TableHandleInner::CopyOnWrite(Arc::new(RwLock::new(Arc::new(table)))),
            generation_metrics: Arc::new(TableGenerationMetrics::default()),
        }
    }

    pub fn from_table(table: Table) -> Self {
        Self::new(table)
    }

    pub fn lock(&self) -> TableGuard<'_> {
        let started = std::time::Instant::now();
        let guard = match &self.inner {
            TableHandleInner::CopyOnWrite(table) => TableGuard::CopyOnWrite {
                table: table.write(),
                metrics: Arc::clone(&self.generation_metrics),
            },
            TableHandleInner::Direct(table) => TableGuard::Direct {
                table: table.lock(),
            },
        };
        self.generation_metrics.writer_wait_nanos.fetch_add(
            started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        guard
    }

    fn try_lock_for(&self, timeout: std::time::Duration) -> Option<TableGuard<'_>> {
        let started = std::time::Instant::now();
        let guard = match &self.inner {
            TableHandleInner::CopyOnWrite(table) => {
                table
                    .try_write_for(timeout)
                    .map(|table| TableGuard::CopyOnWrite {
                        table,
                        metrics: Arc::clone(&self.generation_metrics),
                    })
            }
            TableHandleInner::Direct(table) => table
                .try_lock_for(timeout)
                .map(|table| TableGuard::Direct { table }),
        };
        self.generation_metrics.writer_wait_nanos.fetch_add(
            started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        guard
    }

    pub fn ptr_eq(&self, other: &Self) -> bool {
        match (&self.inner, &other.inner) {
            (TableHandleInner::CopyOnWrite(left), TableHandleInner::CopyOnWrite(right)) => {
                Arc::ptr_eq(left, right)
            }
            (TableHandleInner::Direct(left), TableHandleInner::Direct(right)) => {
                Arc::ptr_eq(left, right)
            }
            _ => false,
        }
    }

    pub fn read_generation_with_context(
        &self,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<(Arc<TableReadGeneration>, Snapshot)> {
        let mut table = if let Some(context) = context {
            loop {
                context.checkpoint()?;
                let wait = context
                    .remaining_duration()
                    .unwrap_or(std::time::Duration::from_millis(5))
                    .min(std::time::Duration::from_millis(5));
                if let Some(table) = self.try_lock_for(wait) {
                    break table;
                }
            }
        } else {
            self.lock()
        };
        let snapshot = table.snapshot();
        let generation = table.clone_read_generation()?;
        Ok((self.generation_metrics.activate(generation), snapshot))
    }

    pub fn generation_stats(&self) -> TableGenerationStats {
        self.generation_metrics.stats()
    }
}

impl From<Arc<Mutex<Table>>> for TableHandle {
    fn from(table: Arc<Mutex<Table>>) -> Self {
        Self {
            inner: TableHandleInner::Direct(table),
            generation_metrics: Arc::new(TableGenerationMetrics::default()),
        }
    }
}

impl std::ops::Deref for TableGuard<'_> {
    type Target = Table;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::CopyOnWrite { table, .. } => table.as_ref(),
            Self::Direct { table } => table,
        }
    }
}

impl std::ops::DerefMut for TableGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::CopyOnWrite { table, metrics } => {
                if Arc::strong_count(table) > 1 || Arc::weak_count(table) > 0 {
                    let estimated_bytes = table.estimated_clone_bytes();
                    let started = std::time::Instant::now();
                    let table = Arc::make_mut(table);
                    metrics.cow_clone_count.fetch_add(1, Ordering::Relaxed);
                    metrics.cow_clone_nanos.fetch_add(
                        started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
                        Ordering::Relaxed,
                    );
                    metrics
                        .estimated_cow_clone_bytes
                        .fetch_add(estimated_bytes, Ordering::Relaxed);
                    table
                } else {
                    Arc::make_mut(table)
                }
            }
            Self::Direct { table } => table,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReadAuthorization {
    pub operation: crate::auth::ColumnOperation,
    pub columns: Vec<u16>,
    pub permissions: Vec<crate::auth::Permission>,
}

#[derive(Default, Debug)]
struct TableWritePermissionNeeds {
    insert: bool,
    insert_columns: Vec<u16>,
    update: bool,
    update_columns: Vec<u16>,
    delete: bool,
    truncate: bool,
}

#[cfg(test)]
thread_local! {
    static WRITE_PERMISSION_DECISIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static AUTO_INCREMENT_TABLE_LOCKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static PREBUILD_TABLE_LOCKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static PUBLISH_TABLE_LOCKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static COMMIT_MANIFEST_WRITES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static TABLE_PERMISSION_DECISIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn summarize_write_permissions(
    staging: &[(u64, crate::txn::Staged)],
) -> HashMap<u64, TableWritePermissionNeeds> {
    use crate::txn::Staged;

    let mut needs = HashMap::<u64, TableWritePermissionNeeds>::new();
    for (table_id, operation) in staging {
        let table = needs.entry(*table_id).or_default();
        match operation {
            Staged::Put(cells) => {
                table.insert = true;
                table
                    .insert_columns
                    .extend(cells.iter().map(|(column, _)| *column));
            }
            Staged::Update {
                changed_columns, ..
            } => {
                table.update = true;
                table.update_columns.extend(changed_columns);
            }
            Staged::Delete(_) => table.delete = true,
            Staged::Truncate => table.truncate = true,
        }
    }
    for table in needs.values_mut() {
        table.insert_columns.sort_unstable();
        table.insert_columns.dedup();
        table.update_columns.sort_unstable();
        table.update_columns.dedup();
    }
    needs
}

struct SecurityCoordinator {
    /// Lock order: security gate -> commit lock -> shared WAL -> table locks.
    gate: RwLock<()>,
    version: AtomicU64,
}

fn security_coordinator(root: &Path, version: u64) -> Arc<SecurityCoordinator> {
    static COORDINATORS: std::sync::OnceLock<
        Mutex<HashMap<PathBuf, std::sync::Weak<SecurityCoordinator>>>,
    > = std::sync::OnceLock::new();

    let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let mut coordinators = COORDINATORS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock();
    coordinators.retain(|_, coordinator| coordinator.strong_count() > 0);
    if let Some(coordinator) = coordinators.get(&root).and_then(std::sync::Weak::upgrade) {
        return coordinator;
    }
    let coordinator = Arc::new(SecurityCoordinator {
        gate: RwLock::new(()),
        version: AtomicU64::new(version),
    });
    coordinators.insert(root, Arc::downgrade(&coordinator));
    coordinator
}

pub fn lock_table_with_context<'a>(
    handle: &'a TableHandle,
    context: Option<&crate::query::AiExecutionContext>,
) -> Result<TableGuard<'a>> {
    let Some(context) = context else {
        return Ok(handle.lock());
    };
    loop {
        context.checkpoint()?;
        let wait = context
            .remaining_duration()
            .unwrap_or(std::time::Duration::from_millis(5))
            .min(std::time::Duration::from_millis(5));
        if let Some(guard) = handle.try_lock_for(wait) {
            return Ok(guard);
        }
    }
}

/// Knobs for [`Database::open_with_options`].
///
/// All fields default to the same values the convenience
/// [`Database::open`] / [`Database::open_encrypted`] / etc. constructors use,
/// so `OpenOptions::default()` round-trips the historical behavior exactly.
#[derive(Clone, Debug, Default)]
pub struct OpenOptions {
    /// Maximum time, in milliseconds, to wait for the cross-process database
    /// lock (`_meta/.lock`) before failing with `MongrelError::DatabaseLocked`.
    ///
    /// `0` (the default) preserves the historical fail-fast semantics: a
    /// single `try_lock_exclusive` call, no retry, no sleep. SQLite-style
    /// `busy_timeout` semantics kick in once this is non-zero — the open
    /// sleeps with progressively wider backoff (1ms → 10ms → 50ms) until
    /// either the lock is acquired or `lock_timeout_ms` elapses, at which
    /// point the open returns the same typed lock error as the fail-fast path.
    ///
    /// Only the cross-process lock is affected. Mounted tables, page-cache
    /// misses, and WAL appends already serialize through in-process locks
    /// that handle their own contention. A second independent open in the
    /// same process always returns `DatabaseLocked` immediately; share the
    /// existing `Arc<Database>` instead.
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
    durable_root: Arc<crate::durable_file::DurableRoot>,
    /// Set by `_meta/replica`; user writes are rejected on follower copies.
    read_only: bool,
    catalog: RwLock<Catalog>,
    security_coordinator: Arc<SecurityCoordinator>,
    security_catalog_disk_reads: AtomicU64,
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
    /// Live immutable run files used by online backups or scored read
    /// generations. GC cannot unlink them until every owning guard drops.
    backup_pins: Arc<Mutex<HashMap<(u64, u128), usize>>>,
    /// Test-only barrier invoked after a transaction writes its spill runs but
    /// before the sequencer/publish, so tests can race `gc()` against an
    /// in-flight spill. `None` in production.
    #[doc(hidden)]
    spill_hook: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    /// Test seam after the security read gate is held and before WAL append.
    #[doc(hidden)]
    security_commit_hook: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    /// Test seam after transaction preparation and before catalog generation
    /// validation under the commit sequencer.
    #[doc(hidden)]
    catalog_commit_hook: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    /// Test seam after a backup boundary is captured and before pinned runs are
    /// copied. Lets tests compact+GC the source at the worst possible moment.
    #[doc(hidden)]
    backup_hook: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    replication_hook: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    trigger_recursive: AtomicBool,
    trigger_max_depth: AtomicU32,
    trigger_max_loop_iterations: AtomicU32,
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
    /// Final field so every storage resource drops before the exclusive lease.
    _lock: Option<ExclusiveDatabaseLease>,
}

struct RunPins {
    pins: Arc<Mutex<HashMap<(u64, u128), usize>>>,
    runs: Vec<(u64, u128)>,
}

struct BackupFilePins {
    root: PathBuf,
}

struct PendingTableDir {
    path: PathBuf,
    armed: bool,
}

impl PendingTableDir {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingTableDir {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

impl Drop for BackupFilePins {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

impl Drop for RunPins {
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
    pub fn open_metrics() -> DatabaseOpenMetrics {
        DatabaseOpenMetrics {
            lock_waits: DATABASE_OPEN_WAIT_COUNT.load(Ordering::Relaxed),
            failures: DATABASE_OPEN_FAILURE_COUNT.load(Ordering::Relaxed),
        }
    }

    fn ensure_owner_process(&self) -> Result<()> {
        let current_pid = std::process::id();
        let owner_pid = self
            ._lock
            .as_ref()
            .map(|lease| lease.owner_pid)
            .unwrap_or(current_pid);
        if current_pid == owner_pid {
            Ok(())
        } else {
            Err(MongrelError::ForkedProcess {
                owner_pid,
                current_pid,
            })
        }
    }

    /// Explicitly close the final shared database owner.
    pub fn shutdown(self: Arc<Self>) -> Result<()> {
        match Arc::try_unwrap(self) {
            Ok(database) => {
                database.ensure_owner_process()?;
                drop(database);
                Ok(())
            }
            Err(database) => Err(MongrelError::DatabaseBusy {
                strong_handles: Arc::strong_count(&database),
            }),
        }
    }

    fn canonical_lock_target(root: &Path) -> std::io::Result<(PathBuf, PathBuf)> {
        if let Ok(canonical) = root.canonicalize() {
            let lock_dir = canonical.parent().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "database root must have a parent directory",
                )
            })?;
            return Ok((canonical.clone(), lock_dir.to_path_buf()));
        }

        let absolute = if root.is_absolute() {
            root.to_path_buf()
        } else {
            std::env::current_dir()?.join(root)
        };
        let mut cursor = absolute.as_path();
        let mut suffix = Vec::new();
        while !cursor.exists() {
            let name = cursor.file_name().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("no existing ancestor for database root {}", root.display()),
                )
            })?;
            suffix.push(name.to_os_string());
            cursor = cursor.parent().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("no existing ancestor for database root {}", root.display()),
                )
            })?;
        }
        let lock_dir = cursor.canonicalize()?;
        let mut canonical = lock_dir.clone();
        for component in suffix.iter().rev() {
            canonical.push(component);
        }
        Ok((canonical, lock_dir))
    }

    fn acquire_database_lock(root: &Path, timeout_ms: u32) -> Result<ExclusiveDatabaseLease> {
        use std::hash::{Hash, Hasher};

        let (canonical_path, lock_dir) = Self::canonical_lock_target(root)?;
        let reservation =
            OpenReservation::acquire(DatabaseOpenKey::IntendedPath(canonical_path.clone()), root)?;

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        canonical_path.hash(&mut hasher);
        let lock_path = lock_dir.join(format!(".mongreldb-{:016x}.lock", hasher.finish()));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(lock_path)?;
        if let Err(error) = Self::fs_lock_exclusive(&file, timeout_ms) {
            return Err(MongrelError::DatabaseLocked {
                path: root.to_path_buf(),
                message: error.to_string(),
            });
        }
        Ok(reservation.into_lease(file, canonical_path))
    }

    fn acquire_legacy_database_lock(
        lock: &mut ExclusiveDatabaseLease,
        root: &Path,
        timeout_ms: u32,
    ) -> Result<()> {
        let durable_root = lock
            .durable_root
            .as_ref()
            .ok_or_else(|| MongrelError::Other("database root descriptor was not pinned".into()))?;
        let file = durable_root.open_lock_file(Path::new(META_DIR).join(".lock"))?;
        if let Err(error) = Self::fs_lock_exclusive(&file, timeout_ms) {
            return Err(MongrelError::DatabaseLocked {
                path: root.to_path_buf(),
                message: error.to_string(),
            });
        }
        lock.legacy_file = Some(file);
        Ok(())
    }

    fn pin_or_create_database_root(path: &Path) -> Result<crate::durable_file::DurableRoot> {
        if path.exists() {
            return crate::durable_file::DurableRoot::open(path).map_err(Into::into);
        }
        let mut ancestor = path;
        while !ancestor.exists() {
            ancestor = ancestor.parent().ok_or_else(|| {
                MongrelError::NotFound(format!(
                    "no existing ancestor for database root {}",
                    path.display()
                ))
            })?;
        }
        let relative = path.strip_prefix(ancestor).map_err(|error| {
            MongrelError::InvalidArgument(format!("invalid database root: {error}"))
        })?;
        crate::durable_file::DurableRoot::open(ancestor)?
            .create_directory_all_pinned(relative)
            .map_err(Into::into)
    }

    fn begin_create(root: impl AsRef<Path>) -> Result<(PathBuf, ExclusiveDatabaseLease)> {
        let requested_root = root.as_ref();
        let mut lock = Self::acquire_database_lock(requested_root, 0)?;
        let root = lock.canonical_path.clone();
        Self::reject_existing_database(&root)?;
        let durable_root = Arc::new(Self::pin_or_create_database_root(&root)?);
        if durable_root.canonical_path() != lock.canonical_path {
            return Err(MongrelError::Conflict(
                "database root changed while it was being created".into(),
            ));
        }
        lock.claim_root_identity(&durable_root)?;
        durable_root.create_directory_all(META_DIR)?;
        lock.durable_root = Some(durable_root);
        let io_root = lock
            .durable_root
            .as_ref()
            .ok_or_else(|| MongrelError::Other("database root descriptor was not pinned".into()))?
            .io_path()?;
        Self::acquire_legacy_database_lock(&mut lock, &io_root, 0)?;
        Self::reject_existing_database(&io_root)?;
        Ok((io_root, lock))
    }

    fn begin_open(
        root: impl AsRef<Path>,
        lock_timeout_ms: u32,
    ) -> Result<(PathBuf, ExclusiveDatabaseLease)> {
        let root = root.as_ref();
        let canonical_root = root.canonicalize().map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                MongrelError::NotFound(format!("database root {}: {error}", root.display()))
            } else {
                error.into()
            }
        })?;
        let durable_root = crate::durable_file::DurableRoot::open(&canonical_root)?;
        Self::begin_open_durable(durable_root, lock_timeout_ms)
    }

    fn begin_open_durable(
        durable_root: crate::durable_file::DurableRoot,
        lock_timeout_ms: u32,
    ) -> Result<(PathBuf, ExclusiveDatabaseLease)> {
        let io_root = durable_root.io_path()?;
        let current_root = io_root.canonicalize()?;
        let mut lock = Self::acquire_database_lock(&current_root, lock_timeout_ms)?;
        lock.claim_root_identity(&durable_root)?;
        lock.durable_root = Some(Arc::new(durable_root));
        let io_root = lock
            .durable_root
            .as_ref()
            .ok_or_else(|| MongrelError::Other("database root descriptor was not pinned".into()))?
            .io_path()?;
        if lock
            .durable_root
            .as_ref()
            .ok_or_else(|| MongrelError::Other("database root descriptor was not pinned".into()))?
            .open_directory(META_DIR)
            .is_err()
        {
            return Err(MongrelError::NotFound(format!(
                "no database metadata found at {:?}",
                current_root
            )));
        }
        Self::acquire_legacy_database_lock(&mut lock, &io_root, lock_timeout_ms)?;
        Ok((io_root, lock))
    }

    /// Create a fresh plaintext database at `root`.
    pub fn create(root: impl AsRef<Path>) -> Result<Self> {
        let (root, lock) = Self::begin_create(root)?;
        Self::create_inner(root, None, lock)
    }

    /// Create a fresh encrypted database, deriving the DB-wide KEK from a
    /// passphrase (Argon2id + HKDF). The salt is persisted at `_meta/keys`.
    #[cfg(feature = "encryption")]
    pub fn create_encrypted(root: impl AsRef<Path>, passphrase: &str) -> Result<Self> {
        let (root, lock) = Self::begin_create(root)?;
        let salt = crate::encryption::random_salt()?;
        crate::durable_file::write_atomic(&root.join(META_DIR).join(KEYS_FILENAME), &salt)?;
        let kek = Arc::new(crate::encryption::Kek::derive(passphrase, &salt)?);
        Self::create_inner(root, Some(kek), lock)
    }

    /// Create a fresh encrypted database, deriving the DB-wide KEK from a raw
    /// high-entropy key via HKDF. The salt is persisted at `_meta/keys`.
    #[cfg(feature = "encryption")]
    pub fn create_with_key(root: impl AsRef<Path>, key: &[u8]) -> Result<Self> {
        let (root, lock) = Self::begin_create(root)?;
        let salt = crate::encryption::random_salt()?;
        crate::durable_file::write_atomic(&root.join(META_DIR).join(KEYS_FILENAME), &salt)?;
        let kek = Arc::new(crate::encryption::Kek::from_raw_key(key, &salt)?);
        Self::create_inner(root, Some(kek), lock)
    }

    fn create_inner(
        root: PathBuf,
        kek: Option<Arc<crate::encryption::Kek>>,
        lock: ExclusiveDatabaseLease,
    ) -> Result<Self> {
        crate::durable_file::create_directory_all(&root.join(TABLES_DIR))?;
        let meta_dek = crate::encryption::meta_dek_for(kek.as_deref());
        let cat = Catalog::empty();
        catalog::write_atomic(&root, &cat, meta_dek.as_ref())?;
        Self::finish_open(root, cat, kek, meta_dek, false, None, None, None, lock)
    }

    /// Open an existing plaintext database.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        Self::open_inner(root, None, None)
    }

    /// Open an existing encrypted database with a passphrase.
    #[cfg(feature = "encryption")]
    pub fn open_encrypted(root: impl AsRef<Path>, passphrase: &str) -> Result<Self> {
        let (root, lock) = Self::begin_open(root, 0)?;
        let salt = read_encryption_salt(lock.durable_root.as_deref().ok_or_else(|| {
            MongrelError::Other("database root descriptor was not pinned".into())
        })?)?;
        let kek = Arc::new(crate::encryption::Kek::derive(passphrase, &salt)?);
        Self::open_inner_locked(root, Some(kek), lock)
    }

    /// Open an existing encrypted database with a configurable cross-process
    /// lock timeout. Mirrors [`open_with_options`](Self::open_with_options).
    #[cfg(feature = "encryption")]
    pub fn open_encrypted_with_options(
        root: impl AsRef<Path>,
        passphrase: &str,
        options: OpenOptions,
    ) -> Result<Self> {
        let (root, lock) = Self::begin_open(root, options.lock_timeout_ms)?;
        let salt = read_encryption_salt(lock.durable_root.as_deref().ok_or_else(|| {
            MongrelError::Other("database root descriptor was not pinned".into())
        })?)?;
        let kek = Arc::new(crate::encryption::Kek::derive(passphrase, &salt)?);
        Self::open_inner_locked(root, Some(kek), lock)
    }

    /// Open an existing encrypted database using a raw high-entropy key.
    #[cfg(feature = "encryption")]
    pub fn open_with_key(root: impl AsRef<Path>, key: &[u8]) -> Result<Self> {
        let (root, lock) = Self::begin_open(root, 0)?;
        let salt = read_encryption_salt(lock.durable_root.as_deref().ok_or_else(|| {
            MongrelError::Other("database root descriptor was not pinned".into())
        })?)?;
        let kek = Arc::new(crate::encryption::Kek::from_raw_key(key, &salt)?);
        Self::open_inner_locked(root, Some(kek), lock)
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
        let (root, lock) = Self::begin_open(root, 0)?;
        let salt = read_encryption_salt(lock.durable_root.as_deref().ok_or_else(|| {
            MongrelError::Other("database root descriptor was not pinned".into())
        })?)?;
        let kek = Arc::new(crate::encryption::Kek::derive(passphrase, &salt)?);
        Self::open_inner_with_credentials_locked(root, Some(kek), username, password, lock)
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
        let (root, lock) = Self::begin_open(root, options.lock_timeout_ms)?;
        let salt = read_encryption_salt(lock.durable_root.as_deref().ok_or_else(|| {
            MongrelError::Other("database root descriptor was not pinned".into())
        })?)?;
        let kek = Arc::new(crate::encryption::Kek::derive(passphrase, &salt)?);
        Self::open_inner_with_credentials_locked(root, Some(kek), username, password, lock)
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
        let (root, lock) = Self::begin_open(root, lock_timeout_ms)?;
        Self::open_inner_locked(root, kek, lock)
    }

    fn open_inner_locked(
        root: PathBuf,
        kek: Option<Arc<crate::encryption::Kek>>,
        lock: ExclusiveDatabaseLease,
    ) -> Result<Self> {
        let meta_dek = crate::encryption::meta_dek_for(kek.as_deref());
        let mut cat = catalog::read_durable(
            lock.durable_root.as_deref().ok_or_else(|| {
                MongrelError::Other("database root descriptor was not pinned".into())
            })?,
            meta_dek.as_ref(),
        )?
        .ok_or_else(|| MongrelError::NotFound(format!("no catalog found at {:?}", root)))?;
        let recovery_checkpoint = cat.clone();

        // CATALOG is only a checkpoint. Authentication must use the
        // authoritative catalog after committed WAL DDL/security replay.
        let wal_dek = crate::encryption::wal_dek_for(kek.as_deref());
        let recovery_records = crate::wal::SharedWal::replay_durable_with_dek(
            lock.durable_root.as_deref().ok_or_else(|| {
                MongrelError::Other("database root descriptor was not pinned".into())
            })?,
            wal_dek.as_ref(),
        )?;
        recover_ddl_from_records(
            &root,
            Some(lock.durable_root.as_deref().ok_or_else(|| {
                MongrelError::Other("database root descriptor was not pinned".into())
            })?),
            &mut cat,
            meta_dek.as_ref(),
            false,
            None,
            &recovery_records,
        )?;
        Self::finish_open(
            root,
            cat,
            kek,
            meta_dek,
            true,
            Some(recovery_checkpoint),
            Some(recovery_records),
            None,
            lock,
        )
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
        let (root, lock) = Self::begin_open(root, lock_timeout_ms)?;
        Self::open_inner_with_credentials_locked(root, kek, username, password, lock)
    }

    fn open_inner_with_credentials_locked(
        root: PathBuf,
        kek: Option<Arc<crate::encryption::Kek>>,
        username: &str,
        password: &str,
        lock: ExclusiveDatabaseLease,
    ) -> Result<Self> {
        let meta_dek = crate::encryption::meta_dek_for(kek.as_deref());
        let mut cat = catalog::read_durable(
            lock.durable_root.as_deref().ok_or_else(|| {
                MongrelError::Other("database root descriptor was not pinned".into())
            })?,
            meta_dek.as_ref(),
        )?
        .ok_or_else(|| MongrelError::NotFound(format!("no catalog found at {:?}", root)))?;
        let recovery_checkpoint = cat.clone();

        // Never verify against a stale checkpoint. A committed password,
        // user, role, or auth-mode change in WAL is authoritative.
        let wal_dek = crate::encryption::wal_dek_for(kek.as_deref());
        let recovery_records = crate::wal::SharedWal::replay_durable_with_dek(
            lock.durable_root.as_deref().ok_or_else(|| {
                MongrelError::Other("database root descriptor was not pinned".into())
            })?,
            wal_dek.as_ref(),
        )?;
        recover_ddl_from_records(
            &root,
            Some(lock.durable_root.as_deref().ok_or_else(|| {
                MongrelError::Other("database root descriptor was not pinned".into())
            })?),
            &mut cat,
            meta_dek.as_ref(),
            false,
            None,
            &recovery_records,
        )?;

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
            Some(recovery_checkpoint),
            Some(recovery_records),
            Some(principal),
            lock,
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
        let (root, lock) = Self::begin_create(root)?;
        Self::create_inner_with_credentials(root, None, admin_username, admin_password, lock)
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
        let (root, lock) = Self::begin_create(root)?;
        let salt = crate::encryption::random_salt()?;
        crate::durable_file::write_atomic(&root.join(META_DIR).join(KEYS_FILENAME), &salt)?;
        let kek = Arc::new(crate::encryption::Kek::derive(passphrase, &salt)?);
        Self::create_inner_with_credentials(root, Some(kek), admin_username, admin_password, lock)
    }

    fn create_inner_with_credentials(
        root: PathBuf,
        kek: Option<Arc<crate::encryption::Kek>>,
        admin_username: &str,
        admin_password: &str,
        lock: ExclusiveDatabaseLease,
    ) -> Result<Self> {
        crate::durable_file::create_directory_all(&root.join(TABLES_DIR))?;
        let meta_dek = crate::encryption::meta_dek_for(kek.as_deref());

        // Build the initial catalog with require_auth = true and one admin user.
        let password_hash =
            crate::auth::hash_password(admin_password).map_err(MongrelError::Other)?;
        let mut cat = Catalog::empty();
        cat.require_auth = true;
        cat.next_user_id = 2;
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
            user_id: 1,
            created_epoch: 0,
            username: admin_username.to_string(),
            is_admin: true,
            roles: Vec::new(),
            permissions: Vec::new(),
        };
        Self::finish_open(
            root,
            cat,
            kek,
            meta_dek,
            false,
            None,
            None,
            Some(admin_principal),
            lock,
        )
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
        Self::open_inner_with_lock_timeout(root, kek, None, 0)
    }

    /// Internal recovery open for a staging directory explicitly marked as a
    /// read-only replica. It bypasses user authentication only so PITR can
    /// replay auth-mode and password transitions; it is not public API.
    pub(crate) fn open_replica_recovery_durable(
        root: &crate::durable_file::DurableRoot,
    ) -> Result<Self> {
        let (root, lock) = Self::begin_open_durable(root.try_clone()?, 0)?;
        Self::open_replica_recovery_inner(root, None, lock)
    }

    #[cfg(feature = "encryption")]
    pub(crate) fn open_encrypted_replica_recovery_durable(
        root: &crate::durable_file::DurableRoot,
        passphrase: &str,
    ) -> Result<Self> {
        let (root_path, lock) = Self::begin_open_durable(root.try_clone()?, 0)?;
        let salt = read_encryption_salt(root)?;
        let kek = Arc::new(crate::encryption::Kek::derive(passphrase, &salt)?);
        Self::open_replica_recovery_inner(root_path, Some(kek), lock)
    }

    fn open_replica_recovery_inner(
        root: PathBuf,
        kek: Option<Arc<crate::encryption::Kek>>,
        lock: ExclusiveDatabaseLease,
    ) -> Result<Self> {
        if !root.join(META_DIR).join("replica").is_file() {
            return Err(MongrelError::InvalidArgument(
                "recovery auth bypass requires a marked replica staging directory".into(),
            ));
        }
        let meta_dek = crate::encryption::meta_dek_for(kek.as_deref());
        let mut cat = catalog::read_durable(
            lock.durable_root.as_deref().ok_or_else(|| {
                MongrelError::Other("database root descriptor was not pinned".into())
            })?,
            meta_dek.as_ref(),
        )?
        .ok_or_else(|| MongrelError::NotFound(format!("no catalog found at {:?}", root)))?;
        let recovery_checkpoint = cat.clone();
        let wal_dek = crate::encryption::wal_dek_for(kek.as_deref());
        let recovery_records = crate::wal::SharedWal::replay_durable_with_dek(
            lock.durable_root.as_deref().ok_or_else(|| {
                MongrelError::Other("database root descriptor was not pinned".into())
            })?,
            wal_dek.as_ref(),
        )?;
        recover_ddl_from_records(
            &root,
            Some(lock.durable_root.as_deref().ok_or_else(|| {
                MongrelError::Other("database root descriptor was not pinned".into())
            })?),
            &mut cat,
            meta_dek.as_ref(),
            false,
            None,
            &recovery_records,
        )?;
        let principal = if cat.require_auth {
            cat.users
                .iter()
                .find(|user| user.is_admin)
                .and_then(|user| Self::resolve_principal_from_catalog(&cat, &user.username))
                .ok_or_else(|| {
                    MongrelError::Schema(
                        "authenticated replica catalog has no recoverable admin".into(),
                    )
                })?
                .into()
        } else {
            None
        };
        Self::finish_open(
            root,
            cat,
            kek,
            meta_dek,
            true,
            Some(recovery_checkpoint),
            Some(recovery_records),
            principal,
            lock,
        )
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
        let mut recorded_wait = false;
        loop {
            match f.try_lock_exclusive() {
                Ok(()) => return Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if !recorded_wait {
                        DATABASE_OPEN_WAIT_COUNT.fetch_add(1, Ordering::Relaxed);
                        recorded_wait = true;
                    }
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

    #[allow(clippy::too_many_arguments)]
    fn finish_open(
        root: PathBuf,
        cat: Catalog,
        kek: Option<Arc<crate::encryption::Kek>>,
        meta_dek: Option<[u8; META_DEK_LEN]>,
        existing: bool,
        recovery_checkpoint: Option<Catalog>,
        recovery_records: Option<Vec<crate::wal::Record>>,
        principal: Option<crate::auth::Principal>,
        lock: ExclusiveDatabaseLease,
    ) -> Result<Self> {
        let durable_root = Arc::clone(lock.durable_root.as_ref().ok_or_else(|| {
            MongrelError::Other("database root descriptor was not pinned".into())
        })?);
        let read_only = if existing {
            match durable_root.open_regular(Path::new(META_DIR).join("replica")) {
                Ok(_) => true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                Err(error) => return Err(error.into()),
            }
        } else {
            false
        };
        let recovered_catalog = cat;
        let mut cat = recovered_catalog.clone();
        let abandoned = if existing && !read_only {
            let abandoned = cat
                .tables
                .iter()
                .filter(|entry| matches!(entry.state, TableState::Building { .. }))
                .map(|entry| entry.table_id)
                .collect::<Vec<_>>();
            for entry in &mut cat.tables {
                if abandoned.contains(&entry.table_id) {
                    entry.state = TableState::Dropped {
                        at_epoch: cat.db_epoch,
                    };
                }
            }
            abandoned
        } else {
            Vec::new()
        };
        let wal_dek = crate::encryption::wal_dek_for(kek.as_deref());
        let recovery_records = match (existing, recovery_records) {
            (true, Some(records)) => records,
            (true, None) => {
                return Err(MongrelError::Other(
                    "existing open has no validated WAL recovery plan".into(),
                ))
            }
            (false, _) => Vec::new(),
        };
        let (history_epochs, history_start) =
            read_history_retention(&durable_root, Epoch(cat.db_epoch))?;
        let open_generation = if existing {
            let checkpoint = recovery_checkpoint.as_ref().ok_or_else(|| {
                MongrelError::Other("existing open has no catalog recovery checkpoint".into())
            })?;
            let recovered_table_ids = cat
                .tables
                .iter()
                .filter(|entry| {
                    checkpoint
                        .tables
                        .iter()
                        .all(|checkpoint| checkpoint.table_id != entry.table_id)
                })
                .map(|entry| entry.table_id)
                .collect::<HashSet<_>>();
            let reconciled_table_ids = cat
                .tables
                .iter()
                .filter(|entry| {
                    checkpoint
                        .tables
                        .iter()
                        .find(|checkpoint| checkpoint.table_id == entry.table_id)
                        .is_some_and(|checkpoint| {
                            crate::wal::DdlOp::encode_schema(&checkpoint.schema).ok()
                                != crate::wal::DdlOp::encode_schema(&entry.schema).ok()
                        })
                })
                .map(|entry| entry.table_id)
                .collect::<HashSet<_>>();
            validate_shared_wal_recovery_plan(
                &durable_root,
                &cat,
                &recovered_table_ids,
                &reconciled_table_ids,
                meta_dek.as_ref(),
                kek.clone(),
                &recovery_records,
            )?;
            let retained_generation = recovery_records
                .iter()
                .filter(|record| record.txn_id != crate::wal::SYSTEM_TXN_ID)
                .map(|record| record.txn_id >> 32)
                .max()
                .unwrap_or(0);
            let head_generation =
                crate::wal::SharedWal::durable_open_generation(&durable_root, wal_dek.as_ref())?;
            let durable_floor = match head_generation {
                Some(head) if retained_generation > head => {
                    return Err(MongrelError::CorruptWal {
                        offset: retained_generation,
                        reason: format!(
                            "retained transaction generation {retained_generation} exceeds WAL head generation {head}"
                        ),
                    })
                }
                Some(head) => head,
                None => retained_generation,
            };
            let stored = catalog::read_generation(&durable_root)?;
            if stored.is_some_and(|generation| generation < durable_floor) {
                return Err(MongrelError::Other(format!(
                    "open-generation {stored:?} precedes durable WAL generation {durable_floor}"
                )));
            }
            let bumped = stored
                .unwrap_or(durable_floor)
                .max(durable_floor)
                .checked_add(1)
                .ok_or_else(|| MongrelError::Full("open-generation namespace exhausted".into()))?;
            if bumped > u32::MAX as u64 {
                return Err(MongrelError::Full(
                    "open-generation namespace exhausted".into(),
                ));
            }
            bumped
        } else {
            0
        };
        let principal = if cat.require_auth {
            let supplied = principal.as_ref().ok_or(MongrelError::AuthRequired)?;
            Some(
                Self::resolve_bound_principal_from_catalog(&cat, supplied)
                    .ok_or(MongrelError::AuthRequired)?,
            )
        } else {
            principal
        };
        let mut table_roots = HashMap::<u64, Arc<crate::durable_file::DurableRoot>>::new();
        if existing {
            for entry in &cat.tables {
                if !matches!(entry.state, TableState::Live) {
                    continue;
                }
                match durable_root
                    .open_directory(Path::new(TABLES_DIR).join(entry.table_id.to_string()))
                {
                    Ok(root) => {
                        table_roots.insert(entry.table_id, Arc::new(root));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }

        // No database-tree mutation occurs above this point. DDL, row payloads,
        // immutable runs, auth state, retention, and generation state have all
        // been validated against the authoritative recovered catalog.
        if existing {
            let mut applied = recovery_checkpoint.ok_or_else(|| {
                MongrelError::Other("existing open has no catalog recovery checkpoint".into())
            })?;
            recover_ddl_from_records(
                &root,
                Some(&durable_root),
                &mut applied,
                meta_dek.as_ref(),
                true,
                Some(&table_roots),
                &recovery_records,
            )?;
            let catalog_value = |catalog: &Catalog| {
                serde_json::to_value(catalog)
                    .map_err(|error| MongrelError::Other(format!("catalog compare: {error}")))
            };
            if catalog_value(&applied)? != catalog_value(&recovered_catalog)? {
                return Err(MongrelError::CorruptWal {
                    offset: 0,
                    reason: "validated and applied DDL recovery plans differ".into(),
                });
            }
            if catalog_value(&cat)? != catalog_value(&applied)? {
                catalog::write_atomic(&root, &cat, meta_dek.as_ref())?;
            }
            validate_catalog_table_storage(&durable_root, &cat, meta_dek.as_ref())?;
            if !read_only {
                sweep_unreferenced_table_dirs(&root, &cat)?;
            }
            match durable_root.remove_directory_all(Path::new(META_DIR).join("backup-pins")) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }

        let epoch = Arc::new(EpochAuthority::new(cat.db_epoch));
        let snapshots = Arc::new(SnapshotRegistry::new());
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
        let shared_wal = Arc::new(Mutex::new(if existing {
            crate::wal::SharedWal::open_durable_root_validated(
                Arc::clone(&durable_root),
                Epoch(cat.db_epoch),
                wal_dek.clone(),
                Some(&recovery_records),
            )?
        } else {
            crate::wal::SharedWal::create_with_durable_root(
                Arc::clone(&durable_root),
                Epoch(cat.db_epoch),
                wal_dek.clone(),
            )?
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
        let _ = abandoned;

        // Build the shared auth state early — it's cloned into every mounted
        // Table's SharedCtx so the Table layer can enforce permissions without
        // a reference back to Database. The `require_auth` flag is mirrored
        // from the catalog; `enable_auth` / `refresh_principal` update it live.
        let auth_state = crate::auth_state::AuthState::new(cat.require_auth, principal.clone());
        let security_coordinator = security_coordinator(&root, cat.security_version);
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
            let table_root = match table_roots.remove(&entry.table_id) {
                Some(root) => root,
                None => Arc::new(
                    durable_root
                        .open_directory(Path::new(TABLES_DIR).join(entry.table_id.to_string()))?,
                ),
            };
            let tdir = table_root.io_path()?;
            let ctx = SharedCtx {
                root_guard: Some(table_root),
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
            tables.insert(entry.table_id, TableHandle::new(t));
        }

        // Recover transaction writes from the shared WAL (spec §15). This is the
        // single durability source for mounted tables: it applies every committed
        // record — both single-table `Table::commit` writes and cross-table
        // transactions — gated by each table's `flushed_epoch` (records already
        // durable in a run are not re-applied).
        if existing {
            recover_shared_wal(&durable_root, &tables, &cat, &epoch, &recovery_records)?;
            reconcile_recovered_table_metadata(&tables, epoch.visible())?;
            if read_only {
                crate::replication::reconcile_replica_epoch_durable(
                    &durable_root,
                    epoch.visible().0,
                )?;
            }
            // P3.4: sweep stale `_txn/<txn_id>/` dirs left by aborted/crashed
            // large transactions (spec §8.5, review fix #14).
            sweep_pending_txn_dirs(&root, &cat);
        }

        // Persist only after all semantic recovery and table mounting succeeds.
        catalog::write_generation(&durable_root, open_generation)?;
        shared_wal.lock().seal_open_generation(open_generation)?;
        crate::replication::replication_identity_durable(&durable_root)?;
        let next_txn_id = (open_generation << 32) | 1;
        // Seed the shared txn-id allocator now that the generation is final.
        *txn_ids.lock() = next_txn_id;
        let mut lock = lock;
        lock.mark_open()?;

        Ok(Self {
            root,
            durable_root,
            read_only,
            catalog: RwLock::new(cat),
            security_coordinator,
            security_catalog_disk_reads: AtomicU64::new(0),
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
            backup_pins: Arc::new(Mutex::new(HashMap::new())),
            spill_hook: Mutex::new(None),
            security_commit_hook: Mutex::new(None),
            catalog_commit_hook: Mutex::new(None),
            backup_hook: Mutex::new(None),
            replication_hook: Mutex::new(None),
            trigger_recursive: AtomicBool::new(TriggerConfig::default().recursive_triggers),
            trigger_max_depth: AtomicU32::new(TriggerConfig::default().max_depth),
            trigger_max_loop_iterations: AtomicU32::new(
                TriggerConfig::default().max_loop_iterations,
            ),
            notify: {
                let (tx, _rx) = tokio::sync::broadcast::channel(256);
                tx
            },
            change_wake,
            principal: RwLock::new(principal),
            auth_state,
            _lock: Some(lock),
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

    /// Read SQLite-compatible application metadata persisted in the catalog.
    pub fn sql_pragma_i64(&self, key: &str) -> Result<Option<i64>> {
        let catalog = self.catalog.read();
        match key {
            "user_version" => Ok(catalog.user_version),
            "application_id" => Ok(catalog.application_id),
            _ => Err(MongrelError::InvalidArgument(format!(
                "unsupported persistent SQL pragma {key:?}"
            ))),
        }
    }

    /// Persist SQLite-compatible application metadata and return its exact
    /// publication epoch. An unchanged value performs no durable write.
    pub fn set_sql_pragma_i64_with_epoch(&self, key: &str, value: i64) -> Result<Option<Epoch>> {
        self.set_sql_pragma_i64_with_epoch_inner(key, value, None)
    }

    pub fn set_sql_pragma_i64_with_epoch_controlled<F>(
        &self,
        key: &str,
        value: i64,
        mut before_commit: F,
    ) -> Result<Option<Epoch>>
    where
        F: FnMut() -> Result<()>,
    {
        self.set_sql_pragma_i64_with_epoch_inner(key, value, Some(&mut before_commit))
    }

    fn set_sql_pragma_i64_with_epoch_inner(
        &self,
        key: &str,
        value: i64,
        before_commit: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<Option<Epoch>> {
        use crate::wal::DdlOp;

        self.require(&crate::auth::Permission::Ddl)?;
        if self.read_only {
            return Err(MongrelError::ReadOnlyReplica);
        }
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }
        let _ddl = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Ddl)?;
        let mut next_catalog = self.catalog.read().clone();
        let target = match key {
            "user_version" => &mut next_catalog.user_version,
            "application_id" => &mut next_catalog.application_id,
            _ => {
                return Err(MongrelError::InvalidArgument(format!(
                    "unsupported persistent SQL pragma {key:?}"
                )))
            }
        };
        if *target == Some(value) {
            return Ok(None);
        }
        *target = Some(value);

        let _commit = self.commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let txn_id = self.alloc_txn_id()?;
        next_catalog.db_epoch = next_catalog.db_epoch.max(epoch.0);
        let commit_seq = {
            let mut wal = self.shared_wal.lock();
            if let Some(before_commit) = before_commit {
                before_commit()?;
            }
            let append: Result<u64> = (|| {
                wal.append(
                    txn_id,
                    WAL_TABLE_ID,
                    crate::wal::Op::Ddl(DdlOp::SetSqlPragma {
                        key: key.to_string(),
                        value,
                    }),
                )?;
                append_catalog_snapshot(&mut wal, txn_id, &next_catalog)?;
                wal.append_commit(txn_id, epoch, &[])
            })();
            append.map_err(|error| self.commit_outcome_unknown(epoch, error))?
        };
        self.await_durable_commit(commit_seq, epoch)?;
        let checkpoint = self.checkpoint_catalog_after_durable(next_catalog);
        self.finish_durable_publish(epoch, &mut epoch_guard, checkpoint)?;
        Ok(Some(epoch))
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

    fn refresh_security_catalog_if_stale(&self, expected_version: u64) -> Result<()> {
        if self.catalog.read().security_version == expected_version {
            return Ok(());
        }
        self.security_catalog_disk_reads
            .fetch_add(1, Ordering::Relaxed);
        let fresh = catalog::read_durable(&self.durable_root, self.meta_dek.as_ref())?
            .ok_or_else(|| MongrelError::NotFound("catalog vanished during write".into()))?;
        let principal = self.principal.read().clone();
        let principal = if fresh.require_auth {
            principal
                .as_ref()
                .and_then(|principal| Self::resolve_bound_principal_from_catalog(&fresh, principal))
        } else {
            principal
        };
        self.auth_state.set_require_auth(fresh.require_auth);
        *self.catalog.write() = fresh;
        *self.principal.write() = principal.clone();
        self.auth_state.set_principal(principal);
        Ok(())
    }

    fn security_write(&self) -> Result<parking_lot::RwLockWriteGuard<'_, ()>> {
        let guard = self.security_coordinator.gate.write();
        let version = self.security_coordinator.version.load(Ordering::Acquire);
        self.refresh_security_catalog_if_stale(version)?;
        Ok(guard)
    }

    /// Commit an exact catalog image through the shared WAL, then checkpoint it.
    /// The WAL image is the authoritative PITR and replication delta; CATALOG is
    /// only its restart checkpoint.
    fn publish_catalog_candidate(
        &self,
        catalog: Catalog,
        epoch: Epoch,
        epoch_guard: &mut EpochGuard<'_>,
        before_publish: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<()> {
        self.publish_catalog_candidate_with_prelude(
            catalog,
            epoch,
            epoch_guard,
            before_publish,
            Vec::new(),
        )
    }

    fn publish_catalog_candidate_with_prelude(
        &self,
        catalog: Catalog,
        epoch: Epoch,
        epoch_guard: &mut EpochGuard<'_>,
        mut before_publish: Option<&mut dyn FnMut() -> Result<()>>,
        prelude: Vec<(u64, crate::wal::Op)>,
    ) -> Result<()> {
        use crate::wal::DdlOp;

        if self.read_only {
            return Err(MongrelError::ReadOnlyReplica);
        }
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }
        if let Some(before_publish) = before_publish.as_mut() {
            (**before_publish)()?;
        }
        if catalog.db_epoch != epoch.0 {
            return Err(MongrelError::InvalidArgument(format!(
                "catalog epoch {} does not match commit epoch {}",
                catalog.db_epoch, epoch.0
            )));
        }
        {
            let current = self.catalog.read();
            validate_catalog_transition(&current, &catalog)?;
        }
        validate_recovered_catalog(&catalog)?;
        let catalog_json = DdlOp::encode_catalog(&catalog)?;
        let txn_id = self.alloc_txn_id()?;
        let commit_seq = {
            let mut wal = self.shared_wal.lock();
            let append: Result<u64> = (|| {
                for (table_id, op) in prelude {
                    wal.append(txn_id, table_id, op)?;
                }
                wal.append(
                    txn_id,
                    WAL_TABLE_ID,
                    crate::wal::Op::Ddl(DdlOp::CatalogSnapshot { catalog_json }),
                )?;
                wal.append_commit(txn_id, epoch, &[])
            })();
            append.map_err(|error| self.commit_outcome_unknown(epoch, error))?
        };
        self.await_durable_commit(commit_seq, epoch)?;
        let checkpoint = self.checkpoint_catalog_after_durable(catalog);
        self.finish_durable_publish(epoch, epoch_guard, checkpoint)
    }

    /// A WAL commit is already durable. Publish the matching catalog in memory
    /// even when its checkpoint rewrite fails; recovery can rebuild the file,
    /// while the live handle must never continue with pre-commit metadata.
    fn checkpoint_catalog_after_durable(&self, catalog: Catalog) -> Result<()> {
        let checkpoint = catalog::write_atomic(&self.root, &catalog, self.meta_dek.as_ref());
        let version = catalog.security_version;
        let principal = self.principal.read().clone();
        let principal = if catalog.require_auth {
            principal.as_ref().and_then(|principal| {
                Self::resolve_bound_principal_from_catalog(&catalog, principal)
            })
        } else {
            principal
        };
        *self.catalog.write() = catalog;
        self.security_coordinator
            .version
            .store(version, Ordering::Release);
        self.auth_state
            .set_require_auth(self.catalog.read().require_auth);
        *self.principal.write() = principal.clone();
        self.auth_state.set_principal(principal);
        checkpoint
    }

    fn finish_durable_publish(
        &self,
        epoch: Epoch,
        epoch_guard: &mut EpochGuard<'_>,
        post_step: Result<()>,
    ) -> Result<()> {
        self.epoch.publish_in_order(epoch);
        epoch_guard.disarm();
        match post_step {
            Ok(()) => Ok(()),
            Err(error) => {
                self.poisoned.store(true, Ordering::Relaxed);
                Err(MongrelError::DurableCommit {
                    epoch: epoch.0,
                    message: error.to_string(),
                })
            }
        }
    }

    /// Wait for a commit marker to reach stable storage. A failed append/fsync
    /// acknowledgement is ambiguous, so poison the live handle and preserve
    /// the assigned epoch in a structured unknown-outcome error.
    fn await_durable_commit(&self, commit_seq: u64, epoch: Epoch) -> Result<()> {
        match self.group.await_durable(&self.shared_wal, commit_seq) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.poisoned.store(true, Ordering::Relaxed);
                Err(MongrelError::CommitOutcomeUnknown {
                    epoch: epoch.0,
                    message: error.to_string(),
                })
            }
        }
    }

    fn commit_outcome_unknown(&self, epoch: Epoch, error: impl std::fmt::Display) -> MongrelError {
        self.poisoned.store(true, Ordering::Relaxed);
        MongrelError::CommitOutcomeUnknown {
            epoch: epoch.0,
            message: error.to_string(),
        }
    }

    /// Persist a complete validated RLS/masking catalog through the WAL.
    pub fn set_security_catalog(&self, security: crate::security::SecurityCatalog) -> Result<()> {
        self.set_security_catalog_as_with_epoch(security, None)
            .map(|_| ())
    }

    /// Persist security policy changes on behalf of an explicit request principal.
    pub fn set_security_catalog_as(
        &self,
        security: crate::security::SecurityCatalog,
        principal: Option<&crate::auth::Principal>,
    ) -> Result<()> {
        self.set_security_catalog_as_with_epoch(security, principal)
            .map(|_| ())
    }

    /// Persist security policy changes and return the exact publication epoch.
    pub fn set_security_catalog_as_with_epoch(
        &self,
        security: crate::security::SecurityCatalog,
        principal: Option<&crate::auth::Principal>,
    ) -> Result<Epoch> {
        self.set_security_catalog_as_with_epoch_inner(security, principal, None)
    }

    /// Persist security policy changes, entering the commit fence immediately
    /// before the first WAL record can become visible to recovery.
    pub fn set_security_catalog_as_with_epoch_controlled<F>(
        &self,
        security: crate::security::SecurityCatalog,
        principal: Option<&crate::auth::Principal>,
        mut before_commit: F,
    ) -> Result<Epoch>
    where
        F: FnMut() -> Result<()>,
    {
        self.set_security_catalog_as_with_epoch_inner(security, principal, Some(&mut before_commit))
    }

    fn set_security_catalog_as_with_epoch_inner(
        &self,
        security: crate::security::SecurityCatalog,
        principal: Option<&crate::auth::Principal>,
        before_commit: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<Epoch> {
        use crate::wal::DdlOp;
        use std::sync::atomic::Ordering;

        self.require_for(principal, &crate::auth::Permission::Admin)?;
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }
        let _ddl = self.ddl_lock.lock();
        // DDL serializes first; write-path order after that is security gate ->
        // commit lock -> shared WAL.
        let _security_write = self.security_write()?;
        self.require_for(principal, &crate::auth::Permission::Admin)?;
        let mut next_catalog = self.catalog.read().clone();
        validate_security_catalog(&next_catalog, &security)?;
        let payload = DdlOp::encode_security(&security)?;
        let _commit = self.commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let txn_id = self.alloc_txn_id()?;
        next_catalog.security = security;
        advance_security_version(&mut next_catalog)?;
        next_catalog.db_epoch = next_catalog.db_epoch.max(epoch.0);
        let commit_seq = {
            let mut wal = self.shared_wal.lock();
            if let Some(before_commit) = before_commit {
                before_commit()?;
            }
            let append: Result<u64> = (|| {
                wal.append(
                    txn_id,
                    WAL_TABLE_ID,
                    crate::wal::Op::Ddl(DdlOp::SetSecurityCatalog {
                        security_json: payload,
                    }),
                )?;
                append_catalog_snapshot(&mut wal, txn_id, &next_catalog)?;
                wal.append_commit(txn_id, epoch, &[])
            })();
            append.map_err(|error| self.commit_outcome_unknown(epoch, error))?
        };
        self.await_durable_commit(commit_seq, epoch)?;
        let checkpoint = self.checkpoint_catalog_after_durable(next_catalog);
        self.finish_durable_publish(epoch, &mut epoch_guard, checkpoint)?;
        Ok(epoch)
    }

    pub fn require_for(
        &self,
        principal: Option<&crate::auth::Principal>,
        permission: &crate::auth::Permission,
    ) -> Result<()> {
        let Some(principal) = principal else {
            return self.require(permission);
        };
        let resolved;
        let principal = if self.auth_state.require_auth() || principal.user_id != 0 {
            resolved = Self::resolve_bound_principal_from_catalog(&self.catalog.read(), principal)
                .ok_or(MongrelError::AuthRequired)?;
            &resolved
        } else {
            principal
        };
        #[cfg(test)]
        TABLE_PERMISSION_DECISIONS.with(|decisions| decisions.set(decisions.get() + 1));
        if principal.has_permission(permission) {
            Ok(())
        } else {
            Err(MongrelError::PermissionDenied {
                required: permission.clone(),
                principal: principal.username.clone(),
            })
        }
    }

    /// Recheck the exact operation principal while the caller holds the
    /// security gate. This deliberately performs no refresh or nested gate
    /// acquisition.
    fn require_exact_principal_current(
        &self,
        principal: Option<&crate::auth::Principal>,
        permission: &crate::auth::Permission,
    ) -> Result<()> {
        let catalog = self.catalog.read();
        if !catalog.require_auth {
            return Ok(());
        }
        let supplied = principal.ok_or(MongrelError::AuthRequired)?;
        let current = Self::resolve_bound_principal_from_catalog(&catalog, supplied)
            .ok_or(MongrelError::AuthRequired)?;
        if current.has_permission(permission) {
            Ok(())
        } else {
            Err(MongrelError::PermissionDenied {
                required: permission.clone(),
                principal: current.username,
            })
        }
    }

    pub(crate) fn with_exact_principal_current<T, F>(
        &self,
        principal: Option<&crate::auth::Principal>,
        permission: &crate::auth::Permission,
        operation: F,
    ) -> Result<T>
    where
        F: FnOnce() -> Result<T>,
    {
        let _security = self.security_coordinator.gate.read();
        self.require_exact_principal_current(principal, permission)?;
        operation()
    }

    pub fn principal_snapshot(&self) -> Option<crate::auth::Principal> {
        self.principal.read().clone()
    }

    #[cfg(test)]
    pub(crate) fn set_cached_principal_for_test(&self, principal: Option<crate::auth::Principal>) {
        *self.principal.write() = principal.clone();
        self.auth_state.set_principal(principal);
    }

    pub fn require_columns_for(
        &self,
        table: &str,
        operation: crate::auth::ColumnOperation,
        column_ids: &[u16],
        principal: Option<&crate::auth::Principal>,
    ) -> Result<()> {
        if principal.is_none() && !self.auth_state.require_auth() {
            return Ok(());
        }
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
        let catalog = self.catalog.read();
        let resolved;
        let principal = if catalog.require_auth || principal.user_id != 0 {
            resolved = Self::resolve_bound_principal_from_catalog(&catalog, principal)
                .ok_or(MongrelError::AuthRequired)?;
            &resolved
        } else {
            principal
        };
        let schema = &catalog
            .live(table)
            .ok_or_else(|| MongrelError::NotFound(format!("table {table:?} not found")))?
            .schema;
        Self::require_columns_for_principal(table, schema, operation, column_ids, principal)
    }

    fn require_columns_for_principal(
        table: &str,
        schema: &Schema,
        operation: crate::auth::ColumnOperation,
        column_ids: &[u16],
        principal: &crate::auth::Principal,
    ) -> Result<()> {
        #[cfg(test)]
        WRITE_PERMISSION_DECISIONS.with(|decisions| decisions.set(decisions.get() + 1));
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
        let principal = self.principal_for_authorized_read(&catalog, principal, false)?;
        drop(catalog);
        let Some(principal) = principal.as_ref() else {
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
        let (security, principal) = {
            let catalog = self.catalog.read();
            (
                catalog.security.clone(),
                self.principal_for_authorized_read(&catalog, principal, false)?,
            )
        };
        if !security.table_has_security(table) {
            return Ok(rows);
        }
        let principal = principal.as_ref().ok_or(MongrelError::AuthRequired)?;
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
        let (security, principal) = {
            let catalog = self.catalog.read();
            (
                catalog.security.clone(),
                self.principal_for_authorized_read(&catalog, principal, false)?,
            )
        };
        if !security.table_has_security(table) {
            return Ok(());
        }
        let principal = principal.as_ref().ok_or(MongrelError::AuthRequired)?;
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
        let (security, principal) = {
            let catalog = self.catalog.read();
            (
                catalog.security.clone(),
                self.principal_for_authorized_read(&catalog, principal, false)?,
            )
        };
        if !security.table_has_security(table) {
            return Ok(());
        }
        let principal = principal.as_ref().ok_or(MongrelError::AuthRequired)?;
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
        security_state: (&crate::security::SecurityCatalog, u64),
        principal: Option<&crate::auth::Principal>,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Option<Arc<HashSet<RowId>>>> {
        let (security, security_version) = security_state;
        if !security.rls_enabled(table_name) {
            return Ok(None);
        }
        let authorization_started = std::time::Instant::now();
        let principal = principal.ok_or(MongrelError::AuthRequired)?;
        let mut roles = principal.roles.clone();
        roles.sort_unstable();
        let principal_key = format!(
            "{}:{}:{}:{}:{roles:?}",
            principal.user_id, principal.created_epoch, principal.username, principal.is_admin
        );
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
        if catalog.require_auth || catalog_bound || principal.user_id != 0 {
            return Self::resolve_bound_principal_from_catalog(catalog, &principal)
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
        self.with_authorized_read_context(
            table_name,
            principal,
            catalog_bound,
            None,
            None,
            None,
            read,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_authorized_read_context<T, F>(
        &self,
        table_name: &str,
        principal: Option<&crate::auth::Principal>,
        catalog_bound: bool,
        authorization: Option<&ReadAuthorization>,
        context: Option<&crate::query::AiExecutionContext>,
        snapshot_override: Option<Snapshot>,
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
        self.with_authorized_read_context_stamped(
            table_name,
            principal,
            catalog_bound,
            authorization,
            context,
            snapshot_override,
            read,
        )
        .map(|(result, _)| result)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_authorized_read_context_stamped<T, F>(
        &self,
        table_name: &str,
        principal: Option<&crate::auth::Principal>,
        catalog_bound: bool,
        authorization: Option<&ReadAuthorization>,
        context: Option<&crate::query::AiExecutionContext>,
        snapshot_override: Option<Snapshot>,
        mut read: F,
    ) -> Result<(T, AuthorizedReadStamp)>
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
            if let Some(authorization) = authorization {
                for permission in &authorization.permissions {
                    self.require_for(effective_principal.as_ref(), permission)?;
                }
                self.require_columns_for(
                    table_name,
                    authorization.operation,
                    &authorization.columns,
                    effective_principal.as_ref(),
                )?;
            }
            let result = {
                let mut table = lock_table_with_context(&handle, context)?;
                let snapshot = snapshot_override.unwrap_or_else(|| table.snapshot());
                let allowed = self.allowed_row_ids_locked(
                    table_name,
                    &table,
                    snapshot,
                    (&security, security_version),
                    effective_principal.as_ref(),
                    context,
                )?;
                let stamp = AuthorizedReadStamp {
                    table_id: table.table_id(),
                    schema_id: table.schema().schema_id,
                    data_generation: table.data_generation(),
                    security_version,
                    snapshot,
                };
                let result = read(
                    &mut table,
                    snapshot,
                    allowed.as_deref(),
                    effective_principal.as_ref(),
                )?;
                (result, stamp)
            };
            if let Some(context) = context {
                context.checkpoint()?;
            }
            if self.catalog.read().security_version == security_version {
                return Ok(result);
            }
            if attempt + 1 == RETRIES {
                return Err(MongrelError::Conflict(
                    "security policy changed during scored read".into(),
                ));
            }
        }
        Err(MongrelError::Conflict(
            "authorization retry loop exhausted".into(),
        ))
    }

    fn with_authorized_aggregate_table<T, F>(
        &self,
        table_name: &str,
        columns: &[u16],
        principal: Option<&crate::auth::Principal>,
        catalog_bound: bool,
        allow_table_security: bool,
        mut aggregate: F,
    ) -> Result<T>
    where
        F: FnMut(
            &mut Table,
            Option<&crate::security::CandidateAuthorization<'_>>,
            Option<&crate::auth::Principal>,
            u64,
        ) -> Result<T>,
    {
        if principal.is_none() && self.principal.read().is_some() {
            self.refresh_principal()?;
        }
        const RETRIES: usize = 3;
        let handle = self.table(table_name)?;
        for attempt in 0..RETRIES {
            let (security, security_version, effective_principal) = {
                let catalog = self.catalog.read();
                (
                    catalog.security.clone(),
                    catalog.security_version,
                    self.principal_for_authorized_read(&catalog, principal, catalog_bound)?,
                )
            };
            self.require_columns_for(
                table_name,
                crate::auth::ColumnOperation::Select,
                columns,
                effective_principal.as_ref(),
            )?;
            if !allow_table_security && security.table_has_security(table_name) {
                return Err(MongrelError::InvalidArgument(
                    "incremental aggregate is unsupported while RLS or column masks are active"
                        .into(),
                ));
            }
            let result = {
                let mut table = handle.lock();
                let authorization = if security.rls_enabled(table_name) {
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
                aggregate(
                    &mut table,
                    authorization.as_ref(),
                    effective_principal.as_ref(),
                    security_version,
                )?
            };
            if self.catalog.read().security_version == security_version {
                return Ok(result);
            }
            if attempt + 1 == RETRIES {
                return Err(MongrelError::Conflict(
                    "security policy changed during aggregate read".into(),
                ));
            }
        }
        Err(MongrelError::Conflict(
            "aggregate authorization retry loop exhausted".into(),
        ))
    }

    /// Scored-read authorization that evaluates RLS only for approximate
    /// candidates. This avoids a full-table policy scan on cache misses while
    /// preserving one table generation and security-version retry.
    pub fn with_authorized_scored_read_context<T, F>(
        &self,
        table_name: &str,
        principal: Option<&crate::auth::Principal>,
        catalog_bound: bool,
        authorization: Option<&ReadAuthorization>,
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
        self.with_authorized_scored_read_context_at(
            table_name,
            principal,
            catalog_bound,
            authorization,
            context,
            None,
            |table, snapshot, authorization, principal| {
                let mut table = table.clone();
                read(&mut table, snapshot, authorization, principal)
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_authorized_scored_read_context_at<T, F>(
        &self,
        table_name: &str,
        principal: Option<&crate::auth::Principal>,
        catalog_bound: bool,
        authorization: Option<&ReadAuthorization>,
        context: Option<&crate::query::AiExecutionContext>,
        snapshot_override: Option<Snapshot>,
        read: F,
    ) -> Result<T>
    where
        F: FnMut(
            &Table,
            Snapshot,
            Option<&crate::security::CandidateAuthorization<'_>>,
            Option<&crate::auth::Principal>,
        ) -> Result<T>,
    {
        self.with_authorized_scored_read_context_at_stamped(
            table_name,
            principal,
            catalog_bound,
            authorization,
            context,
            snapshot_override,
            read,
        )
        .map(|(result, _)| result)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_authorized_scored_read_context_at_stamped<T, F>(
        &self,
        table_name: &str,
        principal: Option<&crate::auth::Principal>,
        catalog_bound: bool,
        authorization: Option<&ReadAuthorization>,
        context: Option<&crate::query::AiExecutionContext>,
        snapshot_override: Option<Snapshot>,
        mut read: F,
    ) -> Result<(T, AuthorizedReadStamp)>
    where
        F: FnMut(
            &Table,
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
            if let Some(authorization) = authorization {
                for permission in &authorization.permissions {
                    self.require_for(effective_principal.as_ref(), permission)?;
                }
                self.require_columns_for(
                    table_name,
                    authorization.operation,
                    &authorization.columns,
                    effective_principal.as_ref(),
                )?;
            }
            let result = {
                let (table, snapshot, _snapshot_guard, _run_pins) =
                    self.scored_read_generation(&handle, context, snapshot_override)?;
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
                let stamp = AuthorizedReadStamp {
                    table_id: table.table_id(),
                    schema_id: table.schema().schema_id,
                    data_generation: table.data_generation(),
                    security_version,
                    snapshot,
                };
                let result = read(
                    table.as_ref(),
                    snapshot,
                    candidate_authorization.as_ref(),
                    effective_principal.as_ref(),
                )?;
                (result, stamp)
            };
            if let Some(context) = context {
                context.checkpoint()?;
            }
            if self.catalog.read().security_version == security_version {
                return Ok(result);
            }
            if attempt + 1 == RETRIES {
                return Err(MongrelError::Conflict(
                    "security policy changed during scored read".into(),
                ));
            }
        }
        Err(MongrelError::Conflict(
            "scored-read authorization retry loop exhausted".into(),
        ))
    }

    fn scored_read_generation(
        &self,
        handle: &TableHandle,
        context: Option<&crate::query::AiExecutionContext>,
        snapshot_override: Option<Snapshot>,
    ) -> Result<(
        Arc<TableReadGeneration>,
        Snapshot,
        crate::retention::OwnedSnapshotGuard,
        RunPins,
    )> {
        let mut table = if let Some(context) = context {
            loop {
                context.checkpoint()?;
                let wait = context
                    .remaining_duration()
                    .unwrap_or(std::time::Duration::from_millis(5))
                    .min(std::time::Duration::from_millis(5));
                if let Some(table) = handle.try_lock_for(wait) {
                    break table;
                }
            }
        } else {
            handle.lock()
        };
        let (snapshot, snapshot_guard) = if let Some(snapshot) = snapshot_override {
            self.snapshot_at_owned(snapshot.epoch)?
        } else {
            let snapshot = table.snapshot();
            let guard = self.snapshots.register_owned(snapshot.epoch);
            (snapshot, guard)
        };
        let table_id = table.table_id();
        let run_keys: Vec<_> = table
            .active_run_ids()
            .map(|run_id| (table_id, run_id))
            .collect();
        let generation = handle
            .generation_metrics
            .activate(table.clone_read_generation()?);
        let run_pins = self.pin_runs(&run_keys);
        Ok((generation, snapshot, snapshot_guard, run_pins))
    }

    fn pin_runs(&self, runs: &[(u64, u128)]) -> RunPins {
        let mut pins = self.backup_pins.lock();
        for run in runs {
            *pins.entry(*run).or_insert(0) += 1;
        }
        drop(pins);
        RunPins {
            pins: Arc::clone(&self.backup_pins),
            runs: runs.to_vec(),
        }
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
                                .is_none_or(|projection| projection.contains(column))
                    });
                }
                self.secure_rows_for(table_name, rows, principal)
            },
        )
    }

    /// Execute a secured native read with cooperative cancellation across
    /// authorization, candidate generation, materialization, masking, and
    /// projection.
    pub fn query_for_current_principal_controlled(
        &self,
        table_name: &str,
        query: &crate::query::Query,
        projection: Option<&[u16]>,
        control: &crate::ExecutionControl,
    ) -> Result<Vec<crate::memtable::Row>> {
        self.query_for_principal_controlled(table_name, query, projection, None, true, control)
    }

    fn query_for_principal_controlled(
        &self,
        table_name: &str,
        query: &crate::query::Query,
        projection: Option<&[u16]>,
        principal: Option<&crate::auth::Principal>,
        catalog_bound: bool,
        control: &crate::ExecutionControl,
    ) -> Result<Vec<crate::memtable::Row>> {
        control.checkpoint()?;
        let context = crate::query::AiExecutionContext::with_control(
            control.clone(),
            usize::MAX,
            crate::query::MAX_FUSED_CANDIDATES,
        );
        let condition_columns = crate::query::condition_columns(&query.conditions);
        self.with_authorized_read_context(
            table_name,
            principal,
            catalog_bound,
            None,
            Some(&context),
            None,
            |table, snapshot, allowed, principal| {
                control.checkpoint()?;
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
                let rows =
                    table.query_at_with_allowed_controlled(query, snapshot, allowed, control)?;
                let projection =
                    projection.map(|columns| columns.iter().copied().collect::<HashSet<_>>());
                let mut projected = Vec::with_capacity(rows.len());
                for (index, mut row) in rows.into_iter().enumerate() {
                    if index & 255 == 0 {
                        control.checkpoint()?;
                    }
                    row.columns.retain(|column, _| {
                        allowed_columns.contains(column)
                            && projection
                                .as_ref()
                                .is_none_or(|projection| projection.contains(column))
                    });
                    projected.push(row);
                }
                self.secure_rows_for_with_context(table_name, projected, principal, Some(&context))
            },
        )
    }

    /// Reservoir aggregate with column grants, RLS, masks, and security-version
    /// retry applied at the database boundary.
    pub fn approx_aggregate_for_current_principal(
        &self,
        table_name: &str,
        conditions: &[crate::query::Condition],
        column: Option<u16>,
        agg: crate::engine::ApproxAgg,
        z: f64,
    ) -> Result<Option<crate::engine::ApproxResult>> {
        if !z.is_finite() || z <= 0.0 {
            return Err(MongrelError::InvalidArgument(
                "z must be finite and > 0".into(),
            ));
        }
        let mut columns = crate::query::condition_columns(conditions);
        columns.extend(column);
        columns.sort_unstable();
        columns.dedup();
        self.with_authorized_aggregate_table(
            table_name,
            &columns,
            None,
            true,
            true,
            |table, authorization, _, _| {
                table.approx_aggregate_with_candidate_authorization(
                    conditions,
                    column,
                    agg,
                    z,
                    authorization,
                )
            },
        )
    }

    /// Incremental aggregate over an append-only table. Active RLS or masks are
    /// rejected because the table-global delta cache cannot safely represent a
    /// secured row universe.
    pub fn incremental_aggregate_for_current_principal(
        &self,
        table_name: &str,
        conditions: &[crate::query::Condition],
        column: Option<u16>,
        agg: crate::engine::NativeAgg,
    ) -> Result<crate::engine::IncrementalAggResult> {
        self.incremental_aggregate_for_principal(table_name, conditions, column, agg, None, true)
    }

    /// Incremental aggregate using an explicit request principal. A
    /// catalog-bound principal is re-resolved on every retry so live grants,
    /// revocations, RLS, and masks cannot reuse a stale cache entry.
    pub fn incremental_aggregate_for_principal(
        &self,
        table_name: &str,
        conditions: &[crate::query::Condition],
        column: Option<u16>,
        agg: crate::engine::NativeAgg,
        principal: Option<&crate::auth::Principal>,
        catalog_bound: bool,
    ) -> Result<crate::engine::IncrementalAggResult> {
        let mut columns = crate::query::condition_columns(conditions);
        columns.extend(column);
        columns.sort_unstable();
        columns.dedup();
        self.with_authorized_aggregate_table(
            table_name,
            &columns,
            principal,
            catalog_bound,
            false,
            |table, _, principal, security_version| {
                let cache_key = incremental_aggregate_cache_key(
                    table_name,
                    conditions,
                    column,
                    agg,
                    principal,
                    security_version,
                );
                table.aggregate_incremental(cache_key, conditions, column, agg)
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
        self.with_authorized_scored_read_context_at(
            table_name,
            None,
            true,
            Some(&ReadAuthorization {
                operation: crate::auth::ColumnOperation::Select,
                columns: vec![request.column_id],
                permissions: Vec::new(),
            }),
            None,
            None,
            |table, snapshot, authorization, principal| {
                self.require_columns_for(
                    table_name,
                    crate::auth::ColumnOperation::Select,
                    &[request.column_id],
                    principal,
                )?;
                table.ann_rerank_at_with_candidate_authorization_on_generation(
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
                (&security, security_version),
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

    /// Historical rows use the current principal and security catalog against
    /// the row values visible at the requested snapshot.
    pub fn rows_at_epoch_for_current_principal(
        &self,
        table_name: &str,
        snapshot: Snapshot,
    ) -> Result<Vec<crate::memtable::Row>> {
        self.with_authorized_read_context(
            table_name,
            None,
            true,
            Some(&ReadAuthorization {
                operation: crate::auth::ColumnOperation::Select,
                columns: Vec::new(),
                permissions: Vec::new(),
            }),
            None,
            Some(snapshot),
            |table, snapshot, allowed, principal| {
                let allowed_columns = self.select_column_ids_for(table_name, principal)?;
                let mut rows = table.visible_rows(snapshot)?;
                if let Some(allowed) = allowed {
                    rows.retain(|row| allowed.contains(&row.row_id));
                }
                rows = self.secure_rows_for(table_name, rows, principal)?;
                for row in &mut rows {
                    row.columns
                        .retain(|column, _| allowed_columns.contains(column));
                }
                Ok(rows)
            },
        )
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
        self.set_materialized_view_with_epoch(definition)
            .map(|_| ())
    }

    /// Durably create or replace a materialized-view definition and return its epoch.
    pub fn set_materialized_view_with_epoch(
        &self,
        definition: crate::catalog::MaterializedViewEntry,
    ) -> Result<Epoch> {
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
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Ddl)?;
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
        let txn_id = self.alloc_txn_id()?;
        let mut next_catalog = self.catalog.read().clone();
        if let Some(existing) = next_catalog
            .materialized_views
            .iter_mut()
            .find(|existing| existing.name == definition.name)
        {
            *existing = definition.clone();
        } else {
            next_catalog.materialized_views.push(definition.clone());
        }
        next_catalog.db_epoch = next_catalog.db_epoch.max(epoch.0);
        let commit_seq = {
            let mut wal = self.shared_wal.lock();
            let append: Result<u64> = (|| {
                wal.append(
                    txn_id,
                    table_id,
                    crate::wal::Op::Ddl(DdlOp::SetMaterializedView {
                        name: definition.name.clone(),
                        definition_json,
                    }),
                )?;
                append_catalog_snapshot(&mut wal, txn_id, &next_catalog)?;
                wal.append_commit(txn_id, epoch, &[])
            })();
            append.map_err(|error| self.commit_outcome_unknown(epoch, error))?
        };
        self.await_durable_commit(commit_seq, epoch)?;

        let checkpoint = self.checkpoint_catalog_after_durable(next_catalog);
        self.finish_durable_publish(epoch, &mut epoch_guard, checkpoint)?;
        Ok(epoch)
    }

    /// The filesystem root this database was opened/created at.
    pub fn root(&self) -> &Path {
        self.durable_root.canonical_path()
    }

    /// Open a descriptor-pinned view of this database root for durable
    /// extension state such as server idempotency receipts.
    pub fn durable_root(&self) -> Arc<crate::durable_file::DurableRoot> {
        Arc::clone(&self.durable_root)
    }

    /// Domain-separated authentication key for server idempotency state.
    /// Encrypted databases derive it from the in-memory KEK. Plain databases
    /// return `None`; their server persists a random key under the pinned root.
    #[cfg(feature = "encryption")]
    pub fn derive_server_idempotency_key(&self) -> Option<zeroize::Zeroizing<[u8; 32]>> {
        self.kek
            .as_deref()
            .map(|kek| kek.derive_subkey(b"mongreldb/server/idempotency/v1"))
    }

    #[cfg(not(feature = "encryption"))]
    pub fn derive_server_idempotency_key(&self) -> Option<zeroize::Zeroizing<[u8; 32]>> {
        None
    }

    pub fn is_read_only_replica(&self) -> bool {
        self.read_only
    }

    /// Reject reads whose backing state may require WAL recovery after a
    /// post-commit publication failure. Ordinary table/catalog state is made
    /// coherent before poison; file-backed external modules use this gate.
    pub fn ensure_consistent_read(&self) -> Result<()> {
        self.ensure_owner_process()?;
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by post-commit failure; reopen required".into(),
            ));
        }
        Ok(())
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
        let admin = crate::auth::Permission::Admin;
        self.require(&admin)?;
        let operation_principal = self.principal_snapshot();
        let _barrier = self.replication_barrier.write();
        let _ddl = self.ddl_lock.lock();
        let _security = self.security_coordinator.gate.read();
        self.require_exact_principal_current(operation_principal.as_ref(), &admin)?;
        let mut handles: Vec<_> = self
            .tables
            .read()
            .iter()
            .map(|(id, handle)| (*id, handle.clone()))
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
            .max(self.epoch.committed().0);
        let files = crate::replication::capture_files(&self.root)?;
        let source_id = crate::replication::replication_identity_durable(&self.durable_root)?;
        drop(wal);
        Ok(crate::replication::ReplicationSnapshot::new(
            source_id, epoch, files,
        ))
    }

    /// Create an online, directly-openable backup at `destination`.
    ///
    /// The short boundary phase quiesces commits/DDL, syncs the WAL, copies
    /// mutable metadata, and pins the exact immutable runs named by the copied
    /// manifests. Writers resume while those runs stream into a sibling staging
    /// directory. A checksummed backup manifest is written last, then the stage
    /// is atomically renamed into place.
    pub fn hot_backup(&self, destination: impl AsRef<Path>) -> Result<crate::backup::BackupReport> {
        let control = crate::ExecutionControl::new(None);
        self.hot_backup_controlled(destination, &control, || true)
    }

    pub(crate) fn hot_backup_to_durable_child(
        &self,
        parent: &crate::durable_file::DurableRoot,
        child: &Path,
        control: &crate::ExecutionControl,
    ) -> Result<crate::backup::BackupReport> {
        let mut components = child.components();
        if !matches!(components.next(), Some(std::path::Component::Normal(_)))
            || components.next().is_some()
        {
            return Err(MongrelError::InvalidArgument(
                "durable backup child must be one relative path component".into(),
            ));
        }
        let destination_name = child.file_name().ok_or_else(|| {
            MongrelError::InvalidArgument("durable backup child has no filename".into())
        })?;
        let prepared = prepare_backup_destination_in(&self.root, parent, destination_name)?;
        self.hot_backup_prepared(prepared, control, || true)
    }

    /// Build a backup cooperatively, then invoke `before_publish` immediately
    /// before the staging directory is atomically renamed into place.
    #[doc(hidden)]
    pub fn hot_backup_controlled<F>(
        &self,
        destination: impl AsRef<Path>,
        control: &crate::ExecutionControl,
        before_publish: F,
    ) -> Result<crate::backup::BackupReport>
    where
        F: FnOnce() -> bool,
    {
        let prepared = prepare_backup_destination(&self.root, destination.as_ref())?;
        self.hot_backup_prepared(prepared, control, before_publish)
    }

    fn hot_backup_prepared<F>(
        &self,
        mut prepared: PreparedBackupDestination,
        control: &crate::ExecutionControl,
        before_publish: F,
    ) -> Result<crate::backup::BackupReport>
    where
        F: FnOnce() -> bool,
    {
        let admin = crate::auth::Permission::Admin;
        self.require(&admin)?;
        let operation_principal = self.principal_snapshot();
        control.checkpoint()?;
        let destination = prepared.destination_path.clone();
        let mut before_publish = Some(before_publish);

        let outcome = (|| {
            control.checkpoint()?;
            let barrier = self.replication_barrier.write();
            let ddl = self.ddl_lock.lock();
            let security = self.security_coordinator.gate.read();
            self.require_exact_principal_current(operation_principal.as_ref(), &admin)?;
            let mut handles: Vec<_> = self
                .tables
                .read()
                .iter()
                .map(|(id, handle)| (*id, handle.clone()))
                .collect();
            handles.sort_by_key(|(id, _)| *id);
            let table_guards: Vec<_> = handles.iter().map(|(_, handle)| handle.lock()).collect();
            let commit = self.commit_lock.lock();
            let mut wal = self.shared_wal.lock();
            wal.group_sync()?;
            let epoch = self.epoch.committed().0;
            let boundary_unix_nanos = current_unix_nanos();

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
                if index % 256 == 0 {
                    control.checkpoint()?;
                }
                let table = &table_guards[index];
                for (run_index, run) in table.run_refs().iter().enumerate() {
                    if run_index % 256 == 0 {
                        control.checkpoint()?;
                    }
                    let source = table.run_path(run.run_id as u64);
                    let relative = Path::new(TABLES_DIR)
                        .join(table_id.to_string())
                        .join(crate::engine::RUNS_DIR)
                        .join(format!("r-{}.sr", run.run_id));
                    let pinned = file_pin_root.join(format!("{table_id}-{}.sr", run.run_id));
                    if std::fs::hard_link(&source, &pinned).is_err() {
                        crate::backup::copy_file_synced(&source, &pinned)?;
                    }
                    run_files.push(((*table_id, run.run_id), pinned, relative));
                }
            }
            crate::durable_file::sync_directory(&file_pin_root)?;
            let run_keys: Vec<_> = run_files.iter().map(|(key, _, _)| *key).collect();
            {
                let mut pins = self.backup_pins.lock();
                for key in &run_keys {
                    *pins.entry(*key).or_insert(0) += 1;
                }
            }
            let _run_pins = RunPins {
                pins: Arc::clone(&self.backup_pins),
                runs: run_keys,
            };
            let deferred: HashSet<_> = run_files
                .iter()
                .map(|(_, _, relative)| relative.clone())
                .collect();
            let mut copied = Vec::new();
            copy_backup_boundary(
                &self.root,
                prepared.stage.as_deref().ok_or_else(|| {
                    MongrelError::Other("backup staging root was already released".into())
                })?,
                &deferred,
                &mut copied,
                Some(control),
            )?;

            drop(wal);
            drop(commit);
            drop(table_guards);
            drop(security);
            drop(ddl);
            drop(barrier);

            if let Some(hook) = self.backup_hook.lock().as_ref() {
                hook();
            }
            for (index, (_, source, relative)) in run_files.into_iter().enumerate() {
                if index % 256 == 0 {
                    control.checkpoint()?;
                }
                let mut source = crate::durable_file::open_regular_nofollow(&source)?;
                prepared
                    .stage
                    .as_deref()
                    .ok_or_else(|| {
                        MongrelError::Other("backup staging root was already released".into())
                    })?
                    .copy_new_from(&relative, &mut source)?;
                copied.push(relative);
            }

            let manifest = crate::backup::BackupManifest::create_controlled_durable(
                prepared.stage.as_deref().ok_or_else(|| {
                    MongrelError::Other("backup staging root was already released".into())
                })?,
                epoch,
                &copied,
                control,
            )?;
            manifest.write_to_durable(prepared.stage.as_deref().ok_or_else(|| {
                MongrelError::Other("backup staging root was already released".into())
            })?)?;
            control.checkpoint()?;
            let publish = before_publish.take().ok_or_else(|| {
                MongrelError::Other("backup publication callback already consumed".into())
            })?;
            if !publish() {
                return Err(MongrelError::Cancelled);
            }
            let final_security = self.security_coordinator.gate.read();
            self.require_exact_principal_current(operation_principal.as_ref(), &admin)?;
            // Windows pins directories without delete sharing. Release the
            // stage handle before renaming that directory, while the parent
            // remains descriptor-pinned for the no-replace publication.
            drop(prepared.stage.take().ok_or_else(|| {
                MongrelError::Other("backup staging root was already released".into())
            })?);
            let published = std::cell::Cell::new(false);
            if let Err(error) = prepared.parent.rename_directory_new_with_after(
                Path::new(&prepared.stage_name),
                &prepared.parent,
                Path::new(&prepared.destination_name),
                || published.set(true),
            ) {
                if published.get() {
                    return Err(MongrelError::CommitOutcomeUnknown {
                        epoch,
                        message: format!("backup publication was not durable: {error}"),
                    });
                }
                return Err(error.into());
            }
            drop(final_security);
            Ok(crate::backup::BackupReport {
                destination,
                epoch,
                boundary_unix_nanos,
                files: manifest.files.len(),
                bytes: manifest.total_bytes(),
            })
        })();

        if outcome.is_err() {
            drop(prepared.stage.take());
            let _ = prepared
                .parent
                .remove_directory_all(Path::new(&prepared.stage_name));
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

        let admin = crate::auth::Permission::Admin;
        self.require(&admin)?;
        let operation_principal = self.principal_snapshot();

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
            .max(self.epoch.committed().0);
        let selected: HashSet<u64> = commits
            .iter()
            .filter_map(|(txn_id, epoch)| (*epoch > since_epoch).then_some(*txn_id))
            .collect();
        let retention_gap = since_epoch < current_epoch
            && since_epoch < crate::replication::replication_wal_floor(&self.root)?;
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
        let source_id = crate::replication::replication_identity_durable(&self.durable_root)?;
        let batch = crate::replication::ReplicationBatch::complete_for_source(
            source_id,
            since_epoch,
            current_epoch,
            earliest_epoch,
            retention_gap,
            spilled,
            records,
        )?;
        if let Some(hook) = self.replication_hook.lock().as_ref() {
            hook();
        }
        let _security = self.security_coordinator.gate.read();
        self.require_exact_principal_current(operation_principal.as_ref(), &admin)?;
        Ok(batch)
    }

    /// Durably append a leader batch to a follower's local WAL and checkpoint
    /// its catalog metadata. Security changes apply to this live handle before
    /// success returns. The caller must reopen to mount new table state.
    pub fn append_replication_batch(
        &self,
        batch: &crate::replication::ReplicationBatch,
    ) -> Result<u64> {
        use crate::wal::Op;

        if !self.read_only {
            return Err(MongrelError::InvalidArgument(
                "replication batches may only target a marked replica".into(),
            ));
        }
        let current = crate::replication::replica_epoch(&self.root)?;
        if batch.is_source_bound() {
            let source_id = crate::replication::replica_source_id_durable(&self.durable_root)?;
            if batch.source_id != source_id {
                return Err(MongrelError::Conflict(
                    "replication batch source does not match follower binding".into(),
                ));
            }
        }
        if batch.requires_snapshot {
            return Err(MongrelError::Conflict(
                "replication snapshot required for this batch".into(),
            ));
        }
        batch.validate_proof()?;
        if batch.from_epoch != current {
            if batch.from_epoch < current && batch.current_epoch == current {
                let wal_dek = crate::encryption::wal_dek_for(self.kek.as_deref());
                let _wal = self.shared_wal.lock();
                let existing: HashSet<(u64, u64)> =
                    crate::wal::SharedWal::replay_with_dek(&self.root, wal_dek.as_ref())?
                        .into_iter()
                        .filter_map(|record| match record.op {
                            Op::TxnCommit { epoch, .. } => Some((record.txn_id, epoch)),
                            _ => None,
                        })
                        .collect();
                let already_applied = batch.records.iter().all(|record| match &record.op {
                    Op::TxnCommit { epoch, .. } => existing.contains(&(record.txn_id, *epoch)),
                    _ => true,
                });
                if already_applied {
                    return Ok(current);
                }
            }
            return Err(MongrelError::Conflict(format!(
                "replication batch starts at epoch {}, follower is at epoch {current}",
                batch.from_epoch
            )));
        }
        if batch.current_epoch < current {
            return Err(MongrelError::InvalidArgument(format!(
                "replication batch current epoch {} precedes follower epoch {current}",
                batch.current_epoch
            )));
        }
        let records = &batch.records;
        let mut commits = HashMap::new();
        let mut commit_epochs = HashSet::new();
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
                    if *epoch <= current || *epoch > batch.current_epoch {
                        return Err(MongrelError::InvalidArgument(format!(
                            "replication commit epoch {epoch} is outside ({current}, {}]",
                            batch.current_epoch
                        )));
                    }
                    if !commit_epochs.insert(*epoch) {
                        return Err(MongrelError::InvalidArgument(format!(
                            "duplicate replication commit epoch {epoch}"
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
            if record.txn_id == crate::wal::SYSTEM_TXN_ID
                || matches!(&record.op, Op::TxnAbort | Op::Flush { .. })
            {
                return Err(MongrelError::InvalidArgument(
                    "replication batch contains a non-committed record".into(),
                ));
            }
            if !commits.contains_key(&record.txn_id) {
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
        if target_epoch != batch.current_epoch {
            return Err(MongrelError::InvalidArgument(format!(
                "replication batch ends at epoch {target_epoch}, expected {}",
                batch.current_epoch
            )));
        }
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
        drop(wal);

        // Auth mode is selected before `finish_open` replays the WAL. Make the
        // catalog transition durable and publish security state to this live
        // handle before reporting success.
        let mut recovered_catalog = self.catalog.read().clone();
        if let Err(error) = recover_ddl_from_wal(
            &self.root,
            Some(&self.durable_root),
            &mut recovered_catalog,
            self.meta_dek.as_ref(),
            wal_dek.as_ref(),
            true,
            None,
        ) {
            return Err(MongrelError::DurableCommit {
                epoch: target_epoch,
                message: format!(
                    "replication WAL is durable but catalog checkpoint failed: {error}"
                ),
            });
        }
        let _security = self.security_coordinator.gate.write();
        let old_security_version = self.catalog.read().security_version;
        let security_changed = old_security_version != recovered_catalog.security_version
            || self.catalog.read().require_auth != recovered_catalog.require_auth;
        let require_auth = recovered_catalog.require_auth;
        let principal = if security_changed {
            None
        } else {
            self.principal.read().as_ref().and_then(|principal| {
                Self::resolve_bound_principal_from_catalog(&recovered_catalog, principal)
            })
        };
        if require_auth {
            self.auth_state.set_require_auth(true);
        }
        self.auth_state.set_principal(principal.clone());
        *self.principal.write() = principal;
        let security_version = recovered_catalog.security_version;
        *self.catalog.write() = recovered_catalog;
        self.security_coordinator
            .version
            .store(security_version, Ordering::Release);
        if !require_auth {
            self.auth_state.set_require_auth(false);
        }
        if let Err(error) =
            crate::replication::reconcile_replica_epoch_durable(&self.durable_root, target_epoch)
        {
            return Err(MongrelError::DurableCommit {
                epoch: target_epoch,
                message: format!(
                    "replication WAL and catalog are durable but follower watermark failed: {error}"
                ),
            });
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

    /// Return the stable table id and current schema generation from one
    /// catalog snapshot. Callers can bind retries to this identity so a table
    /// dropped and recreated under the same name is never mistaken for the
    /// original resource.
    pub fn table_identity(&self, name: &str) -> Result<(u64, u64)> {
        let catalog = self.catalog.read();
        catalog
            .live(name)
            .map(|entry| (entry.table_id, entry.schema.schema_id))
            .ok_or_else(|| MongrelError::NotFound(format!("table {name:?} not found")))
    }

    pub(crate) fn building_table_id(&self, name: &str) -> Result<u64> {
        self.catalog
            .read()
            .building(name)
            .map(|entry| entry.table_id)
            .ok_or_else(|| MongrelError::NotFound(format!("building table {name:?} not found")))
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

    pub fn create_procedure(&self, procedure: StoredProcedure) -> Result<StoredProcedure> {
        self.create_procedure_inner(procedure, None)
    }

    pub fn create_procedure_controlled<F>(
        &self,
        procedure: StoredProcedure,
        mut before_publish: F,
    ) -> Result<StoredProcedure>
    where
        F: FnMut() -> Result<()>,
    {
        self.create_procedure_inner(procedure, Some(&mut before_publish))
    }

    fn create_procedure_inner(
        &self,
        mut procedure: StoredProcedure,
        before_publish: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<StoredProcedure> {
        self.require(&crate::auth::Permission::Ddl)?;
        let _g = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Ddl)?;
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
        let mut next_catalog = self.catalog.read().clone();
        next_catalog
            .procedures
            .push(ProcedureEntry::from(procedure.clone()));
        next_catalog.db_epoch = epoch.0;
        self.publish_catalog_candidate(next_catalog, epoch, &mut _epoch_guard, before_publish)?;
        Ok(procedure)
    }

    pub fn create_or_replace_procedure(
        &self,
        procedure: StoredProcedure,
    ) -> Result<StoredProcedure> {
        self.create_or_replace_procedure_inner(procedure, None)
    }

    pub fn create_or_replace_procedure_controlled<F>(
        &self,
        procedure: StoredProcedure,
        mut before_publish: F,
    ) -> Result<StoredProcedure>
    where
        F: FnMut() -> Result<()>,
    {
        self.create_or_replace_procedure_inner(procedure, Some(&mut before_publish))
    }

    fn create_or_replace_procedure_inner(
        &self,
        procedure: StoredProcedure,
        before_publish: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<StoredProcedure> {
        self.require(&crate::auth::Permission::Ddl)?;
        let _g = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Ddl)?;
        procedure.validate()?;
        self.validate_procedure_references(&procedure)?;
        let commit_lock = Arc::clone(&self.commit_lock);
        let _c = commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let mut next_catalog = self.catalog.read().clone();
        let replaced = {
            let next = match next_catalog
                .procedures
                .iter()
                .position(|p| p.procedure.name == procedure.name)
            {
                Some(idx) => {
                    let next = next_catalog.procedures[idx]
                        .procedure
                        .replaced(procedure.clone(), epoch.0)?;
                    next_catalog.procedures[idx] = ProcedureEntry::from(next.clone());
                    next
                }
                None => {
                    let mut next = procedure;
                    next.created_epoch = epoch.0;
                    next.updated_epoch = epoch.0;
                    next_catalog
                        .procedures
                        .push(ProcedureEntry::from(next.clone()));
                    next
                }
            };
            next_catalog.db_epoch = epoch.0;
            next
        };
        self.publish_catalog_candidate(next_catalog, epoch, &mut _epoch_guard, before_publish)?;
        Ok(replaced)
    }

    pub fn drop_procedure(&self, name: &str) -> Result<()> {
        self.drop_procedure_with_epoch(name).map(|_| ())
    }

    pub fn drop_procedure_with_epoch(&self, name: &str) -> Result<Epoch> {
        self.drop_procedure_with_epoch_inner(name, None)
    }

    pub fn drop_procedure_with_epoch_controlled<F>(
        &self,
        name: &str,
        mut before_publish: F,
    ) -> Result<Epoch>
    where
        F: FnMut() -> Result<()>,
    {
        self.drop_procedure_with_epoch_inner(name, Some(&mut before_publish))
    }

    fn drop_procedure_with_epoch_inner(
        &self,
        name: &str,
        before_publish: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<Epoch> {
        self.require(&crate::auth::Permission::Ddl)?;
        let _g = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Ddl)?;
        let commit_lock = Arc::clone(&self.commit_lock);
        let _c = commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let mut next_catalog = self.catalog.read().clone();
        let before = next_catalog.procedures.len();
        next_catalog.procedures.retain(|p| p.procedure.name != name);
        if next_catalog.procedures.len() == before {
            return Err(MongrelError::NotFound(format!(
                "procedure {name:?} not found"
            )));
        }
        next_catalog.db_epoch = epoch.0;
        self.publish_catalog_candidate(next_catalog, epoch, &mut _epoch_guard, before_publish)?;
        Ok(epoch)
    }

    // ── User / role / credentials management ─────────────────────────────

    /// List all catalog users (password hashes included — callers should not
    /// serialize them externally).
    pub fn users(&self) -> Vec<crate::auth::UserEntry> {
        self.catalog.read().users.clone()
    }

    /// Resolve only the stable, non-secret identity fields needed to scope
    /// request receipts. Password hashes never leave the catalog lock.
    pub fn user_identity(&self, username: &str) -> Option<(u64, u64)> {
        self.catalog
            .read()
            .users
            .iter()
            .find(|user| user.username == username)
            .map(|user| (user.id, user.created_epoch))
    }

    /// Current catalog authorization generation. Retry bindings can include
    /// this value to fail closed after roles, grants, or row policies change.
    pub fn security_version(&self) -> u64 {
        self.catalog.read().security_version
    }

    /// List all catalog roles.
    pub fn roles(&self) -> Vec<crate::auth::RoleEntry> {
        self.catalog.read().roles.clone()
    }

    /// Create a new user with an Argon2id-hashed password.
    pub fn create_user(&self, username: &str, password: &str) -> Result<crate::auth::UserEntry> {
        self.require(&crate::auth::Permission::Admin)?;
        let hash = crate::auth::hash_password(password).map_err(MongrelError::Other)?;
        self.create_user_with_password_hash(username, hash)
    }

    /// Create a user from a password hash prepared before a commit fence.
    pub fn create_user_with_password_hash(
        &self,
        username: &str,
        hash: String,
    ) -> Result<crate::auth::UserEntry> {
        self.create_user_with_password_hash_inner(username, hash, None)
    }

    pub fn create_user_with_password_hash_controlled<F>(
        &self,
        username: &str,
        hash: String,
        mut before_publish: F,
    ) -> Result<crate::auth::UserEntry>
    where
        F: FnMut() -> Result<()>,
    {
        self.create_user_with_password_hash_inner(username, hash, Some(&mut before_publish))
    }

    fn create_user_with_password_hash_inner(
        &self,
        username: &str,
        hash: String,
        before_publish: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<crate::auth::UserEntry> {
        self.require(&crate::auth::Permission::Admin)?;
        let _ddl = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Admin)?;
        let _commit = self.commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let mut next_catalog = self.catalog.read().clone();
        if next_catalog.users.iter().any(|u| u.username == username) {
            return Err(MongrelError::InvalidArgument(format!(
                "user {username:?} already exists"
            )));
        }
        next_catalog.next_user_id = next_catalog.next_user_id.max(1);
        let id = next_catalog.next_user_id;
        next_catalog.next_user_id = id
            .checked_add(1)
            .ok_or_else(|| MongrelError::Full("user-id namespace exhausted".into()))?;
        let entry = crate::auth::UserEntry {
            id,
            username: username.into(),
            password_hash: hash,
            roles: Vec::new(),
            is_admin: false,
            created_epoch: epoch.0,
        };
        next_catalog.users.push(entry.clone());
        advance_security_version(&mut next_catalog)?;
        next_catalog.db_epoch = epoch.0;
        self.publish_catalog_candidate(next_catalog, epoch, &mut _epoch_guard, before_publish)?;
        Ok(entry)
    }

    /// Drop a user by username.
    pub fn drop_user(&self, username: &str) -> Result<()> {
        self.drop_user_with_epoch(username).map(|_| ())
    }

    pub fn drop_user_with_epoch(&self, username: &str) -> Result<Epoch> {
        self.drop_user_with_epoch_inner(username, None)
    }

    pub fn drop_user_with_epoch_controlled<F>(
        &self,
        username: &str,
        mut before_publish: F,
    ) -> Result<Epoch>
    where
        F: FnMut() -> Result<()>,
    {
        self.drop_user_with_epoch_inner(username, Some(&mut before_publish))
    }

    fn drop_user_with_epoch_inner(
        &self,
        username: &str,
        before_publish: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<Epoch> {
        self.require(&crate::auth::Permission::Admin)?;
        let _ddl = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Admin)?;
        let _commit = self.commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let mut next_catalog = self.catalog.read().clone();
        let before = next_catalog.users.len();
        next_catalog.users.retain(|u| u.username != username);
        if next_catalog.users.len() == before {
            return Err(MongrelError::NotFound(format!(
                "user {username:?} not found"
            )));
        }
        advance_security_version(&mut next_catalog)?;
        next_catalog.db_epoch = epoch.0;
        self.publish_catalog_candidate(next_catalog, epoch, &mut _epoch_guard, before_publish)?;
        Ok(epoch)
    }

    /// Change a user's password.
    pub fn alter_user_password(&self, username: &str, new_password: &str) -> Result<()> {
        self.alter_user_password_with_epoch(username, new_password)
            .map(|_| ())
    }

    pub fn alter_user_password_with_epoch(
        &self,
        username: &str,
        new_password: &str,
    ) -> Result<Epoch> {
        self.require(&crate::auth::Permission::Admin)?;
        let hash = crate::auth::hash_password(new_password).map_err(MongrelError::Other)?;
        self.alter_user_password_hash_with_epoch(username, hash)
    }

    pub fn alter_user_password_hash_with_epoch(
        &self,
        username: &str,
        hash: String,
    ) -> Result<Epoch> {
        self.alter_user_password_hash_with_epoch_inner(username, hash, None)
    }

    pub fn alter_user_password_hash_with_epoch_controlled<F>(
        &self,
        username: &str,
        hash: String,
        mut before_publish: F,
    ) -> Result<Epoch>
    where
        F: FnMut() -> Result<()>,
    {
        self.alter_user_password_hash_with_epoch_inner(username, hash, Some(&mut before_publish))
    }

    fn alter_user_password_hash_with_epoch_inner(
        &self,
        username: &str,
        hash: String,
        before_publish: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<Epoch> {
        self.require(&crate::auth::Permission::Admin)?;
        let _ddl = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Admin)?;
        let _commit = self.commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let mut next_catalog = self.catalog.read().clone();
        let user = next_catalog
            .users
            .iter_mut()
            .find(|u| u.username == username)
            .ok_or_else(|| MongrelError::NotFound(format!("user {username:?} not found")))?;
        user.password_hash = hash;
        advance_security_version(&mut next_catalog)?;
        next_catalog.db_epoch = epoch.0;
        self.publish_catalog_candidate(next_catalog, epoch, &mut _epoch_guard, before_publish)?;
        Ok(epoch)
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

    /// Authenticate and resolve one immutable principal from the same catalog
    /// snapshot. Username reuse cannot bridge the password check and principal
    /// resolution.
    pub fn authenticate_principal(
        &self,
        username: &str,
        password: &str,
    ) -> Result<Option<crate::auth::Principal>> {
        self.authenticate_principal_inner(username, password, || {})
    }

    fn authenticate_principal_inner<F>(
        &self,
        username: &str,
        password: &str,
        after_verify: F,
    ) -> Result<Option<crate::auth::Principal>>
    where
        F: FnOnce(),
    {
        let catalog = self.catalog.read();
        let Some(user) = catalog.users.iter().find(|user| user.username == username) else {
            return Ok(None);
        };
        if user.password_hash.is_empty()
            || !crate::auth::verify_password(password, &user.password_hash)
                .map_err(MongrelError::Other)?
        {
            return Ok(None);
        }
        after_verify();
        Ok(Self::resolve_user_principal_from_catalog(&catalog, user))
    }

    /// Grant admin privileges to a user (bypasses all permission checks).
    pub fn set_user_admin(&self, username: &str, is_admin: bool) -> Result<()> {
        self.set_user_admin_with_epoch(username, is_admin)
            .map(|_| ())
    }

    pub fn set_user_admin_with_epoch(
        &self,
        username: &str,
        is_admin: bool,
    ) -> Result<Option<Epoch>> {
        self.set_user_admin_with_epoch_inner(username, is_admin, None)
    }

    pub fn set_user_admin_with_epoch_controlled<F>(
        &self,
        username: &str,
        is_admin: bool,
        mut before_publish: F,
    ) -> Result<Option<Epoch>>
    where
        F: FnMut() -> Result<()>,
    {
        self.set_user_admin_with_epoch_inner(username, is_admin, Some(&mut before_publish))
    }

    fn set_user_admin_with_epoch_inner(
        &self,
        username: &str,
        is_admin: bool,
        before_publish: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<Option<Epoch>> {
        self.require(&crate::auth::Permission::Admin)?;
        let _ddl = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Admin)?;
        let _commit = self.commit_lock.lock();
        let mut next_catalog = self.catalog.read().clone();
        let user = next_catalog
            .users
            .iter_mut()
            .find(|u| u.username == username)
            .ok_or_else(|| MongrelError::NotFound(format!("user {username:?} not found")))?;
        if user.is_admin == is_admin {
            return Ok(None);
        }
        user.is_admin = is_admin;
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        advance_security_version(&mut next_catalog)?;
        next_catalog.db_epoch = epoch.0;
        self.publish_catalog_candidate(next_catalog, epoch, &mut _epoch_guard, before_publish)?;
        Ok(Some(epoch))
    }

    /// Create a new role.
    pub fn create_role(&self, name: &str) -> Result<crate::auth::RoleEntry> {
        self.create_role_inner(name, None)
    }

    pub fn create_role_controlled<F>(
        &self,
        name: &str,
        mut before_publish: F,
    ) -> Result<crate::auth::RoleEntry>
    where
        F: FnMut() -> Result<()>,
    {
        self.create_role_inner(name, Some(&mut before_publish))
    }

    fn create_role_inner(
        &self,
        name: &str,
        before_publish: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<crate::auth::RoleEntry> {
        self.require(&crate::auth::Permission::Admin)?;
        let _ddl = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Admin)?;
        let _commit = self.commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let mut next_catalog = self.catalog.read().clone();
        if next_catalog.roles.iter().any(|r| r.name == name) {
            return Err(MongrelError::InvalidArgument(format!(
                "role {name:?} already exists"
            )));
        }
        let entry = crate::auth::RoleEntry {
            name: name.into(),
            permissions: Vec::new(),
            created_epoch: epoch.0,
        };
        next_catalog.roles.push(entry.clone());
        advance_security_version(&mut next_catalog)?;
        next_catalog.db_epoch = epoch.0;
        self.publish_catalog_candidate(next_catalog, epoch, &mut _epoch_guard, before_publish)?;
        Ok(entry)
    }

    /// Drop a role by name.
    pub fn drop_role(&self, name: &str) -> Result<()> {
        self.drop_role_with_epoch(name).map(|_| ())
    }

    pub fn drop_role_with_epoch(&self, name: &str) -> Result<Epoch> {
        self.drop_role_with_epoch_inner(name, None)
    }

    pub fn drop_role_with_epoch_controlled<F>(
        &self,
        name: &str,
        mut before_publish: F,
    ) -> Result<Epoch>
    where
        F: FnMut() -> Result<()>,
    {
        self.drop_role_with_epoch_inner(name, Some(&mut before_publish))
    }

    fn drop_role_with_epoch_inner(
        &self,
        name: &str,
        before_publish: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<Epoch> {
        self.require(&crate::auth::Permission::Admin)?;
        let _ddl = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Admin)?;
        let _commit = self.commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let mut next_catalog = self.catalog.read().clone();
        let before = next_catalog.roles.len();
        next_catalog.roles.retain(|r| r.name != name);
        if next_catalog.roles.len() == before {
            return Err(MongrelError::NotFound(format!("role {name:?} not found")));
        }
        for user in &mut next_catalog.users {
            user.roles.retain(|r| r != name);
        }
        advance_security_version(&mut next_catalog)?;
        next_catalog.db_epoch = epoch.0;
        self.publish_catalog_candidate(next_catalog, epoch, &mut _epoch_guard, before_publish)?;
        Ok(epoch)
    }

    /// Grant a role to a user.
    pub fn grant_role(&self, username: &str, role_name: &str) -> Result<()> {
        self.grant_role_with_epoch(username, role_name).map(|_| ())
    }

    pub fn grant_role_with_epoch(&self, username: &str, role_name: &str) -> Result<Option<Epoch>> {
        self.grant_role_with_epoch_inner(username, role_name, None)
    }

    pub fn grant_role_with_epoch_controlled<F>(
        &self,
        username: &str,
        role_name: &str,
        mut before_publish: F,
    ) -> Result<Option<Epoch>>
    where
        F: FnMut() -> Result<()>,
    {
        self.grant_role_with_epoch_inner(username, role_name, Some(&mut before_publish))
    }

    fn grant_role_with_epoch_inner(
        &self,
        username: &str,
        role_name: &str,
        before_publish: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<Option<Epoch>> {
        self.require(&crate::auth::Permission::Admin)?;
        let _ddl = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Admin)?;
        let _commit = self.commit_lock.lock();
        let mut next_catalog = self.catalog.read().clone();
        if !next_catalog.roles.iter().any(|r| r.name == role_name) {
            return Err(MongrelError::NotFound(format!(
                "role {role_name:?} not found"
            )));
        }
        let user = next_catalog
            .users
            .iter_mut()
            .find(|u| u.username == username)
            .ok_or_else(|| MongrelError::NotFound(format!("user {username:?} not found")))?;
        if user.roles.iter().any(|role| role == role_name) {
            return Ok(None);
        }
        user.roles.push(role_name.into());
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        advance_security_version(&mut next_catalog)?;
        next_catalog.db_epoch = epoch.0;
        self.publish_catalog_candidate(next_catalog, epoch, &mut _epoch_guard, before_publish)?;
        Ok(Some(epoch))
    }

    /// Revoke a role from a user.
    pub fn revoke_role(&self, username: &str, role_name: &str) -> Result<()> {
        self.revoke_role_with_epoch(username, role_name).map(|_| ())
    }

    pub fn revoke_role_with_epoch(&self, username: &str, role_name: &str) -> Result<Option<Epoch>> {
        self.revoke_role_with_epoch_inner(username, role_name, None)
    }

    pub fn revoke_role_with_epoch_controlled<F>(
        &self,
        username: &str,
        role_name: &str,
        mut before_publish: F,
    ) -> Result<Option<Epoch>>
    where
        F: FnMut() -> Result<()>,
    {
        self.revoke_role_with_epoch_inner(username, role_name, Some(&mut before_publish))
    }

    fn revoke_role_with_epoch_inner(
        &self,
        username: &str,
        role_name: &str,
        before_publish: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<Option<Epoch>> {
        self.require(&crate::auth::Permission::Admin)?;
        let _ddl = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Admin)?;
        let _commit = self.commit_lock.lock();
        let mut next_catalog = self.catalog.read().clone();
        let user = next_catalog
            .users
            .iter_mut()
            .find(|u| u.username == username)
            .ok_or_else(|| MongrelError::NotFound(format!("user {username:?} not found")))?;
        let before = user.roles.len();
        user.roles.retain(|r| r != role_name);
        if user.roles.len() == before {
            return Ok(None);
        }
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        advance_security_version(&mut next_catalog)?;
        next_catalog.db_epoch = epoch.0;
        self.publish_catalog_candidate(next_catalog, epoch, &mut _epoch_guard, before_publish)?;
        Ok(Some(epoch))
    }

    /// Grant a permission to a role.
    pub fn grant_permission(
        &self,
        role_name: &str,
        permission: crate::auth::Permission,
    ) -> Result<()> {
        self.grant_permission_with_epoch(role_name, permission)
            .map(|_| ())
    }

    pub fn grant_permission_with_epoch(
        &self,
        role_name: &str,
        permission: crate::auth::Permission,
    ) -> Result<Option<Epoch>> {
        self.grant_permission_with_epoch_inner(role_name, permission, None)
    }

    pub fn grant_permission_with_epoch_controlled<F>(
        &self,
        role_name: &str,
        permission: crate::auth::Permission,
        mut before_publish: F,
    ) -> Result<Option<Epoch>>
    where
        F: FnMut() -> Result<()>,
    {
        self.grant_permission_with_epoch_inner(role_name, permission, Some(&mut before_publish))
    }

    fn grant_permission_with_epoch_inner(
        &self,
        role_name: &str,
        permission: crate::auth::Permission,
        before_publish: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<Option<Epoch>> {
        self.require(&crate::auth::Permission::Admin)?;
        let _ddl = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Admin)?;
        let _commit = self.commit_lock.lock();
        let mut next_catalog = self.catalog.read().clone();
        let role = next_catalog
            .roles
            .iter_mut()
            .find(|r| r.name == role_name)
            .ok_or_else(|| MongrelError::NotFound(format!("role {role_name:?} not found")))?;
        let before = role.permissions.clone();
        merge_permission(&mut role.permissions, permission);
        if role.permissions == before {
            return Ok(None);
        }
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        advance_security_version(&mut next_catalog)?;
        next_catalog.db_epoch = epoch.0;
        self.publish_catalog_candidate(next_catalog, epoch, &mut _epoch_guard, before_publish)?;
        Ok(Some(epoch))
    }

    /// Revoke a permission from a role.
    pub fn revoke_permission(
        &self,
        role_name: &str,
        permission: crate::auth::Permission,
    ) -> Result<()> {
        self.revoke_permission_with_epoch(role_name, permission)
            .map(|_| ())
    }

    pub fn revoke_permission_with_epoch(
        &self,
        role_name: &str,
        permission: crate::auth::Permission,
    ) -> Result<Option<Epoch>> {
        self.revoke_permission_with_epoch_inner(role_name, permission, None)
    }

    pub fn revoke_permission_with_epoch_controlled<F>(
        &self,
        role_name: &str,
        permission: crate::auth::Permission,
        mut before_publish: F,
    ) -> Result<Option<Epoch>>
    where
        F: FnMut() -> Result<()>,
    {
        self.revoke_permission_with_epoch_inner(role_name, permission, Some(&mut before_publish))
    }

    fn revoke_permission_with_epoch_inner(
        &self,
        role_name: &str,
        permission: crate::auth::Permission,
        before_publish: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<Option<Epoch>> {
        self.require(&crate::auth::Permission::Admin)?;
        let _ddl = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Admin)?;
        let _commit = self.commit_lock.lock();
        let mut next_catalog = self.catalog.read().clone();
        let role = next_catalog
            .roles
            .iter_mut()
            .find(|r| r.name == role_name)
            .ok_or_else(|| MongrelError::NotFound(format!("role {role_name:?} not found")))?;
        let before = role.permissions.clone();
        revoke_permission_from(&mut role.permissions, &permission);
        if role.permissions == before {
            return Ok(None);
        }
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        advance_security_version(&mut next_catalog)?;
        next_catalog.db_epoch = epoch.0;
        self.publish_catalog_candidate(next_catalog, epoch, &mut _epoch_guard, before_publish)?;
        Ok(Some(epoch))
    }

    /// Resolve a user into a [`crate::auth::Principal`] by collecting all
    /// permissions from their roles. Returns `None` if the user doesn't exist.
    pub fn resolve_principal(&self, username: &str) -> Option<crate::auth::Principal> {
        let cat = self.catalog.read();
        Self::resolve_principal_from_catalog(&cat, username)
    }

    /// Re-resolve only when the immutable user identity still exists. This is
    /// the server/session validation path; username reuse never matches.
    pub fn resolve_current_principal(
        &self,
        principal: &crate::auth::Principal,
    ) -> Option<crate::auth::Principal> {
        Self::resolve_bound_principal_from_catalog(&self.catalog.read(), principal)
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
        Self::resolve_user_principal_from_catalog(cat, user)
    }

    fn resolve_bound_principal_from_catalog(
        cat: &Catalog,
        principal: &crate::auth::Principal,
    ) -> Option<crate::auth::Principal> {
        let user = cat.users.iter().find(|user| {
            user.id == principal.user_id
                && user.created_epoch == principal.created_epoch
                && user.username == principal.username
        })?;
        Self::resolve_user_principal_from_catalog(cat, user)
    }

    fn resolve_user_principal_from_catalog(
        cat: &Catalog,
        user: &crate::auth::UserEntry,
    ) -> Option<crate::auth::Principal> {
        let mut permissions = Vec::new();
        for role_name in &user.roles {
            if let Some(role) = cat.roles.iter().find(|r| &r.name == role_name) {
                permissions.extend(role.permissions.iter().cloned());
            }
        }
        Some(crate::auth::Principal {
            user_id: user.id,
            created_epoch: user.created_epoch,
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

    /// Re-resolve the cached principal from the shared current catalog.
    /// Long-lived
    /// handles (e.g. a daemon) call this after a `REVOKE` or role change —
    /// possibly made by a different handle to the same database — to pick up
    /// the new effective permissions without re-verifying the password.
    ///
    /// The process-wide security version reloads from disk only when another
    /// handle published a newer catalog. The username is taken from
    /// the existing cached principal; if the user has since been dropped,
    /// returns [`MongrelError::InvalidCredentials`].
    ///
    /// No-op (returns `Ok(())`) on a credentialless database, or on a
    /// credentialed database whose cached principal is `None`.
    pub fn refresh_principal(&self) -> Result<()> {
        let previous = match self.principal.read().clone() {
            Some(principal) => principal,
            None => return Ok(()),
        };
        let observed_version = self.security_coordinator.version.load(Ordering::Acquire);
        self.refresh_security_catalog_if_stale(observed_version)?;
        let cat = self.catalog.read();
        match Self::resolve_bound_principal_from_catalog(&cat, &previous) {
            Some(p) => {
                *self.principal.write() = Some(p.clone());
                // Update the shared auth state so mounted Tables see the new
                // permissions immediately (Tables read from AuthState, not from
                // self.principal).
                self.auth_state.set_principal(Some(p));
                Ok(())
            }
            None => Err(MongrelError::InvalidCredentials {
                username: previous.username,
            }),
        }
    }

    /// Number of security-catalog disk reloads performed by this open handle.
    /// Initial open reads are excluded.
    pub fn security_catalog_disk_read_count(&self) -> u64 {
        self.security_catalog_disk_reads.load(Ordering::Relaxed)
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
        let _ddl = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        let _commit = self.commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let mut next_catalog = self.catalog.read().clone();
        if next_catalog.require_auth {
            return Err(MongrelError::InvalidArgument(
                "database already has require_auth enabled".into(),
            ));
        }
        if next_catalog
            .users
            .iter()
            .any(|u| u.username == admin_username)
        {
            return Err(MongrelError::InvalidArgument(format!(
                "user {admin_username:?} already exists"
            )));
        }
        next_catalog.next_user_id = next_catalog.next_user_id.max(1);
        let id = next_catalog.next_user_id;
        next_catalog.next_user_id = id
            .checked_add(1)
            .ok_or_else(|| MongrelError::Full("user-id namespace exhausted".into()))?;
        next_catalog.users.push(crate::auth::UserEntry {
            id,
            username: admin_username.to_string(),
            password_hash,
            roles: Vec::new(),
            is_admin: true,
            created_epoch: epoch.0,
        });
        next_catalog.require_auth = true;
        advance_security_version(&mut next_catalog)?;
        next_catalog.db_epoch = epoch.0;
        let publish = self.publish_catalog_candidate(next_catalog, epoch, &mut _epoch_guard, None);
        // Cache the admin principal on this handle + update the shared auth
        // state whenever rename published, even if directory fsync was
        // inconclusive.
        if publish.is_ok() || matches!(&publish, Err(MongrelError::CommitOutcomeUnknown { .. })) {
            let principal = crate::auth::Principal {
                user_id: id,
                created_epoch: epoch.0,
                username: admin_username.to_string(),
                is_admin: true,
                roles: Vec::new(),
                permissions: Vec::new(),
            };
            *self.principal.write() = Some(principal.clone());
            self.auth_state.set_principal(Some(principal));
        }
        publish
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
        let _ddl = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        let _commit = self.commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let mut next_catalog = self.catalog.read().clone();
        if !next_catalog.require_auth {
            return Err(MongrelError::InvalidArgument(
                "database does not have require_auth enabled".into(),
            ));
        }
        next_catalog.require_auth = false;
        advance_security_version(&mut next_catalog)?;
        next_catalog.db_epoch = epoch.0;
        let publish = self.publish_catalog_candidate(next_catalog, epoch, &mut _epoch_guard, None);
        // Clear the cached principal — enforcement is now off.
        if publish.is_ok() || matches!(&publish, Err(MongrelError::CommitOutcomeUnknown { .. })) {
            *self.principal.write() = None;
        }
        publish
    }

    /// Enforcement check: if the catalog has `require_auth = true`, verify
    /// that the cached principal satisfies `perm`. Called by every
    /// enforcement point (DDL, admin, maintenance, and — in Phase 2 —
    /// Table/Transaction/MongrelSession operations).
    ///
    /// On a credentialless database this is a no-op (`Ok(())`).
    pub fn require(&self, perm: &crate::auth::Permission) -> Result<()> {
        self.ensure_owner_process()?;
        if self.read_only && !matches!(perm, crate::auth::Permission::Select { .. }) {
            return Err(MongrelError::ReadOnlyReplica);
        }
        if self.principal.read().is_some() {
            self.refresh_principal().map_err(|error| match error {
                MongrelError::InvalidCredentials { .. } => MongrelError::AuthRequired,
                error => error,
            })?;
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

    pub fn create_trigger(&self, trigger: StoredTrigger) -> Result<StoredTrigger> {
        self.create_trigger_inner(trigger, None, None)
    }

    pub fn create_trigger_controlled<F>(
        &self,
        trigger: StoredTrigger,
        mut before_publish: F,
    ) -> Result<StoredTrigger>
    where
        F: FnMut() -> Result<()>,
    {
        self.create_trigger_inner(trigger, None, Some(&mut before_publish))
    }

    pub fn create_trigger_as_controlled<F>(
        &self,
        trigger: StoredTrigger,
        principal: Option<&crate::auth::Principal>,
        mut before_publish: F,
    ) -> Result<StoredTrigger>
    where
        F: FnMut() -> Result<()>,
    {
        self.create_trigger_inner(trigger, principal, Some(&mut before_publish))
    }

    fn create_trigger_inner(
        &self,
        mut trigger: StoredTrigger,
        principal: Option<&crate::auth::Principal>,
        before_publish: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<StoredTrigger> {
        self.require_for(principal, &crate::auth::Permission::Ddl)?;
        let _g = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require_for(principal, &crate::auth::Permission::Ddl)?;
        trigger.validate()?;
        self.validate_trigger_references(&trigger)
            .map_err(trigger_validation_error)?;
        {
            let cat = self.catalog.read();
            if cat.triggers.iter().any(|t| t.trigger.name == trigger.name) {
                return Err(MongrelError::TriggerValidation(format!(
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
        let mut next_catalog = self.catalog.read().clone();
        next_catalog
            .triggers
            .push(TriggerEntry::from(trigger.clone()));
        next_catalog.db_epoch = epoch.0;
        self.publish_catalog_candidate(next_catalog, epoch, &mut _epoch_guard, before_publish)?;
        Ok(trigger)
    }

    pub fn create_or_replace_trigger(&self, trigger: StoredTrigger) -> Result<StoredTrigger> {
        self.create_or_replace_trigger_inner(trigger, None, None)
    }

    pub fn create_or_replace_trigger_controlled<F>(
        &self,
        trigger: StoredTrigger,
        mut before_publish: F,
    ) -> Result<StoredTrigger>
    where
        F: FnMut() -> Result<()>,
    {
        self.create_or_replace_trigger_inner(trigger, None, Some(&mut before_publish))
    }

    pub fn create_or_replace_trigger_as_controlled<F>(
        &self,
        trigger: StoredTrigger,
        principal: Option<&crate::auth::Principal>,
        mut before_publish: F,
    ) -> Result<StoredTrigger>
    where
        F: FnMut() -> Result<()>,
    {
        self.create_or_replace_trigger_inner(trigger, principal, Some(&mut before_publish))
    }

    fn create_or_replace_trigger_inner(
        &self,
        trigger: StoredTrigger,
        principal: Option<&crate::auth::Principal>,
        before_publish: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<StoredTrigger> {
        self.require_for(principal, &crate::auth::Permission::Ddl)?;
        let _g = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require_for(principal, &crate::auth::Permission::Ddl)?;
        trigger.validate()?;
        self.validate_trigger_references(&trigger)
            .map_err(trigger_validation_error)?;
        let commit_lock = Arc::clone(&self.commit_lock);
        let _c = commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let mut next_catalog = self.catalog.read().clone();
        let replaced = {
            let next = match next_catalog
                .triggers
                .iter()
                .position(|t| t.trigger.name == trigger.name)
            {
                Some(idx) => {
                    let next = next_catalog.triggers[idx]
                        .trigger
                        .replaced(trigger.clone(), epoch.0)?;
                    next_catalog.triggers[idx] = TriggerEntry::from(next.clone());
                    next
                }
                None => {
                    let mut next = trigger;
                    next.created_epoch = epoch.0;
                    next.updated_epoch = epoch.0;
                    next_catalog.triggers.push(TriggerEntry::from(next.clone()));
                    next
                }
            };
            next_catalog.db_epoch = epoch.0;
            next
        };
        self.publish_catalog_candidate(next_catalog, epoch, &mut _epoch_guard, before_publish)?;
        Ok(replaced)
    }

    pub fn drop_trigger(&self, name: &str) -> Result<()> {
        self.drop_trigger_with_epoch(name).map(|_| ())
    }

    /// Drop one trigger and return the exact catalog publication epoch.
    pub fn drop_trigger_with_epoch(&self, name: &str) -> Result<Epoch> {
        self.drop_triggers_with_epoch(&[name.to_string()])
    }

    pub fn drop_trigger_with_epoch_controlled<F>(
        &self,
        name: &str,
        before_publish: F,
    ) -> Result<Epoch>
    where
        F: FnMut() -> Result<()>,
    {
        self.drop_triggers_with_epoch_controlled(&[name.to_string()], before_publish)
    }

    /// Atomically drop several triggers in one catalog publication.
    pub fn drop_triggers_with_epoch(&self, names: &[String]) -> Result<Epoch> {
        self.drop_triggers_with_epoch_inner(names, None, None)
    }

    pub fn drop_triggers_with_epoch_controlled<F>(
        &self,
        names: &[String],
        mut before_publish: F,
    ) -> Result<Epoch>
    where
        F: FnMut() -> Result<()>,
    {
        self.drop_triggers_with_epoch_inner(names, None, Some(&mut before_publish))
    }

    pub fn drop_triggers_with_epoch_as_controlled<F>(
        &self,
        names: &[String],
        principal: Option<&crate::auth::Principal>,
        mut before_publish: F,
    ) -> Result<Epoch>
    where
        F: FnMut() -> Result<()>,
    {
        self.drop_triggers_with_epoch_inner(names, principal, Some(&mut before_publish))
    }

    fn drop_triggers_with_epoch_inner(
        &self,
        names: &[String],
        principal: Option<&crate::auth::Principal>,
        before_publish: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<Epoch> {
        self.require_for(principal, &crate::auth::Permission::Ddl)?;
        if names.is_empty() {
            return Err(MongrelError::InvalidArgument(
                "at least one trigger name is required".into(),
            ));
        }
        let _g = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require_for(principal, &crate::auth::Permission::Ddl)?;
        {
            let cat = self.catalog.read();
            for name in names {
                if !cat.triggers.iter().any(|t| t.trigger.name == *name) {
                    return Err(MongrelError::NotFound(format!(
                        "trigger {name:?} not found"
                    )));
                }
            }
        }
        let commit_lock = Arc::clone(&self.commit_lock);
        let _c = commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let mut next_catalog = self.catalog.read().clone();
        next_catalog
            .triggers
            .retain(|trigger| !names.contains(&trigger.trigger.name));
        next_catalog.db_epoch = epoch.0;
        self.publish_catalog_candidate(next_catalog, epoch, &mut _epoch_guard, before_publish)?;
        Ok(epoch)
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

    pub fn create_external_table(&self, entry: ExternalTableEntry) -> Result<ExternalTableEntry> {
        self.create_external_table_inner(entry, None)
    }

    pub fn create_external_table_controlled<F>(
        &self,
        entry: ExternalTableEntry,
        mut before_publish: F,
    ) -> Result<ExternalTableEntry>
    where
        F: FnMut() -> Result<()>,
    {
        self.create_external_table_inner(entry, Some(&mut before_publish))
    }

    fn create_external_table_inner(
        &self,
        mut entry: ExternalTableEntry,
        before_publish: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<ExternalTableEntry> {
        self.require(&crate::auth::Permission::Ddl)?;
        let _g = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Ddl)?;
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
        // A prior durable drop may have left connector state behind if its
        // cleanup failed or the process crashed. Never let a new table with
        // the same name inherit that stale state.
        crate::durable_file::create_directory(&self.root.join(VTAB_DIR))?;
        crate::durable_file::remove_directory_all(&self.root.join(VTAB_DIR).join(&entry.name))?;
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        entry.created_epoch = epoch.0;
        let mut next_catalog = self.catalog.read().clone();
        next_catalog.external_tables.push(entry.clone());
        next_catalog.db_epoch = epoch.0;
        self.publish_catalog_candidate_with_prelude(
            next_catalog,
            epoch,
            &mut _epoch_guard,
            before_publish,
            vec![(
                EXTERNAL_TABLE_ID,
                crate::wal::Op::Ddl(crate::wal::DdlOp::ResetExternalTableState {
                    name: entry.name.clone(),
                    generation_epoch: epoch.0,
                }),
            )],
        )?;
        Ok(entry)
    }

    pub fn drop_external_table(&self, name: &str) -> Result<()> {
        self.drop_external_table_with_epoch(name).map(|_| ())
    }

    /// Drop an external table and return the exact catalog publication epoch.
    pub fn drop_external_table_with_epoch(&self, name: &str) -> Result<Epoch> {
        self.drop_external_table_with_epoch_inner(name, None)
    }

    pub fn drop_external_table_with_epoch_controlled<F>(
        &self,
        name: &str,
        mut before_publish: F,
    ) -> Result<Epoch>
    where
        F: FnMut() -> Result<()>,
    {
        self.drop_external_table_with_epoch_inner(name, Some(&mut before_publish))
    }

    fn drop_external_table_with_epoch_inner(
        &self,
        name: &str,
        before_publish: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<Epoch> {
        self.require(&crate::auth::Permission::Ddl)?;
        let _g = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Ddl)?;
        let commit_lock = Arc::clone(&self.commit_lock);
        let _c = commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let mut next_catalog = self.catalog.read().clone();
        let before = next_catalog.external_tables.len();
        next_catalog.external_tables.retain(|t| t.name != name);
        if next_catalog.external_tables.len() == before {
            return Err(MongrelError::NotFound(format!(
                "external table {name:?} not found"
            )));
        }
        next_catalog.db_epoch = epoch.0;
        self.publish_catalog_candidate_with_prelude(
            next_catalog,
            epoch,
            &mut _epoch_guard,
            before_publish,
            vec![(
                EXTERNAL_TABLE_ID,
                crate::wal::Op::Ddl(crate::wal::DdlOp::ResetExternalTableState {
                    name: name.to_string(),
                    generation_epoch: epoch.0,
                }),
            )],
        )?;
        let state_dir = self.root.join(VTAB_DIR).join(name);
        if let Err(error) = crate::durable_file::remove_directory_all(&state_dir) {
            return Err(MongrelError::DurableCommit {
                epoch: epoch.0,
                message: format!(
                    "external table was dropped but connector-state cleanup failed: {error}"
                ),
            });
        }
        Ok(epoch)
    }

    pub fn commit_external_table_state(&self, name: &str, state: &[u8]) -> Result<Epoch> {
        let txn_id = self.alloc_txn_id()?;
        let (principal, catalog_bound) = self.transaction_principal_snapshot();
        self.commit_transaction_with_external_states(
            txn_id,
            self.epoch.visible(),
            Vec::new(),
            vec![(name.to_string(), state.to_vec())],
            Vec::new(),
            principal,
            catalog_bound,
            None,
        )
        .map(|(epoch, _)| epoch)
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
        let control = crate::ExecutionControl::new(None);
        self.change_events_since_controlled(last_event_id, &control)
    }

    /// Reconstruct committed changes with cooperative cancellation and bounds.
    pub fn change_events_since_controlled(
        &self,
        last_event_id: Option<&str>,
        control: &crate::ExecutionControl,
    ) -> Result<CdcBatch> {
        use crate::wal::Op;

        control.checkpoint()?;
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
        let records = crate::wal::SharedWal::replay_with_dek_controlled(
            &self.root,
            wal_dek.as_ref(),
            control,
            CDC_MAX_WAL_RECORDS,
            CDC_MAX_WAL_REPLAY_BYTES,
        )?;
        drop(wal);
        control.checkpoint()?;

        let mut commits: HashMap<u64, (u64, Vec<crate::wal::AddedRun>)> = HashMap::new();
        let mut spilled_payloads: HashMap<(u64, u64), Vec<&[u8]>> = HashMap::new();
        for (index, record) in records.iter().enumerate() {
            if index % 256 == 0 {
                control.checkpoint()?;
            }
            if let Op::TxnCommit { epoch, added_runs } = &record.op {
                commits.insert(record.txn_id, (*epoch, added_runs.clone()));
            }
            if let Op::SpilledRows { table_id, rows } = &record.op {
                spilled_payloads
                    .entry((record.txn_id, *table_id))
                    .or_default()
                    .push(rows);
            }
        }
        let earliest_epoch = commits.values().map(|(epoch, _)| *epoch).min();
        let current_epoch = self.epoch.committed().0;
        let retention_floor = crate::replication::replication_wal_floor(&self.root)?;
        let gap = resume.is_some_and(|(epoch, _)| {
            retention_floor != 0 && epoch <= retention_floor && epoch <= current_epoch
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
        let mut before_images: HashMap<(u64, u64, u64), crate::memtable::Row> = HashMap::new();
        let mut retained_bytes = 0_usize;
        for (index, record) in records.iter().enumerate() {
            if index % 256 == 0 {
                control.checkpoint()?;
            }
            if !commits.contains_key(&record.txn_id) {
                continue;
            }
            let Op::BeforeImage {
                table_id,
                row_id,
                row,
            } = &record.op
            else {
                continue;
            };
            if row.len() > CDC_MAX_INLINE_PAYLOAD_BYTES {
                return Err(MongrelError::ResourceLimitExceeded {
                    resource: "CDC before-image bytes",
                    requested: row.len(),
                    limit: CDC_MAX_INLINE_PAYLOAD_BYTES,
                });
            }
            let before: crate::memtable::Row = bincode::deserialize(row)?;
            if before_images.len() >= CDC_MAX_ROWS {
                return Err(MongrelError::ResourceLimitExceeded {
                    resource: "CDC before-image rows",
                    requested: before_images.len().saturating_add(1),
                    limit: CDC_MAX_ROWS,
                });
            }
            charge_cdc_bytes(
                &mut retained_bytes,
                cdc_row_storage_bytes(&before),
                "CDC retained bytes",
            )?;
            before_images.insert((record.txn_id, *table_id, row_id.0), before);
        }
        let mut operation_indices: HashMap<u64, u32> = HashMap::new();
        let mut events = Vec::new();
        let mut decoded_rows = before_images.len();
        for (record_index, record) in records.iter().enumerate() {
            if record_index % 256 == 0 {
                control.checkpoint()?;
            }
            let Some((commit_epoch, _)) = commits.get(&record.txn_id) else {
                continue;
            };
            let event = match &record.op {
                Op::Put { table_id, rows } => {
                    if rows.len() > CDC_MAX_INLINE_PAYLOAD_BYTES {
                        return Err(MongrelError::ResourceLimitExceeded {
                            resource: "CDC inline row bytes",
                            requested: rows.len(),
                            limit: CDC_MAX_INLINE_PAYLOAD_BYTES,
                        });
                    }
                    let rows: Vec<crate::memtable::Row> = bincode::deserialize(rows)?;
                    decoded_rows = decoded_rows.saturating_add(rows.len());
                    if decoded_rows > CDC_MAX_ROWS {
                        return Err(MongrelError::ResourceLimitExceeded {
                            resource: "CDC decoded rows",
                            requested: decoded_rows,
                            limit: CDC_MAX_ROWS,
                        });
                    }
                    let event_bytes = cdc_rows_json_bytes(&rows).saturating_add(512);
                    let mut peak_bytes = retained_bytes;
                    charge_cdc_bytes(&mut peak_bytes, event_bytes, "CDC retained event bytes")?;
                    let data = serde_json::to_value(rows)
                        .map_err(|error| MongrelError::Other(format!("CDC JSON: {error}")))?;
                    Some((*table_id, "put", data, event_bytes))
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
                    let event_bytes = cdc_rows_json_bytes(&before)
                        .saturating_add(
                            row_ids
                                .len()
                                .saturating_mul(std::mem::size_of::<serde_json::Value>()),
                        )
                        .saturating_add(512);
                    let mut peak_bytes = retained_bytes;
                    charge_cdc_bytes(&mut peak_bytes, event_bytes, "CDC retained event bytes")?;
                    Some((
                        *table_id,
                        "delete",
                        serde_json::json!({
                            "row_ids": row_ids.iter().map(|row_id| row_id.0).collect::<Vec<_>>(),
                            "before": before,
                        }),
                        event_bytes,
                    ))
                }
                Op::TruncateTable { table_id } => {
                    Some((*table_id, "truncate", serde_json::Value::Null, 512))
                }
                _ => None,
            };
            if let Some((table_id, op, data, event_bytes)) = event {
                let index = operation_indices.entry(record.txn_id).or_insert(0);
                let event_position = (*commit_epoch, *index);
                *index = index.saturating_add(1);
                if resume.is_some_and(|position| event_position <= position) {
                    continue;
                }
                if events.len() >= CDC_MAX_EVENTS {
                    return Err(MongrelError::ResourceLimitExceeded {
                        resource: "CDC events",
                        requested: events.len().saturating_add(1),
                        limit: CDC_MAX_EVENTS,
                    });
                }
                charge_cdc_bytes(&mut retained_bytes, event_bytes, "CDC retained event bytes")?;
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
                    control.checkpoint()?;
                    let index = operation_indices.entry(record.txn_id).or_insert(0);
                    let event_position = (*commit_epoch, *index);
                    *index = index.saturating_add(1);
                    if resume.is_some_and(|position| event_position <= position) {
                        continue;
                    }
                    let mut rows = if let Some(payloads) =
                        spilled_payloads.get(&(record.txn_id, run.table_id))
                    {
                        let mut rows = Vec::new();
                        for payload in payloads {
                            control.checkpoint()?;
                            if payload.len() > CDC_MAX_INLINE_PAYLOAD_BYTES {
                                return Err(MongrelError::ResourceLimitExceeded {
                                    resource: "CDC spilled row bytes",
                                    requested: payload.len(),
                                    limit: CDC_MAX_INLINE_PAYLOAD_BYTES,
                                });
                            }
                            let chunk: Vec<crate::memtable::Row> = bincode::deserialize(payload)?;
                            if decoded_rows
                                .saturating_add(rows.len())
                                .saturating_add(chunk.len())
                                > CDC_MAX_ROWS
                            {
                                return Err(MongrelError::ResourceLimitExceeded {
                                    resource: "CDC decoded rows",
                                    requested: decoded_rows
                                        .saturating_add(rows.len())
                                        .saturating_add(chunk.len()),
                                    limit: CDC_MAX_ROWS,
                                });
                            }
                            rows.extend(chunk);
                        }
                        rows
                    } else {
                        let Some(handle) = self.tables.read().get(&run.table_id).cloned() else {
                            return Ok(CdcBatch {
                                events: Vec::new(),
                                current_epoch,
                                earliest_epoch,
                                gap: true,
                            });
                        };
                        let table = handle.lock();
                        let mut reader = match table.open_reader(run.run_id) {
                            Ok(reader) => reader,
                            Err(_) => {
                                return Ok(CdcBatch {
                                    events: Vec::new(),
                                    current_epoch,
                                    earliest_epoch,
                                    gap: true,
                                })
                            }
                        };
                        let remaining = CDC_MAX_ROWS.saturating_sub(decoded_rows);
                        let rows = reader.all_rows_controlled(control, remaining)?;
                        drop(reader);
                        drop(table);
                        rows
                    };
                    for row in &mut rows {
                        row.committed_epoch = Epoch(*commit_epoch);
                    }
                    decoded_rows = decoded_rows.saturating_add(rows.len());
                    let event_bytes = cdc_rows_json_bytes(&rows).saturating_add(768);
                    charge_cdc_bytes(&mut retained_bytes, event_bytes, "CDC retained event bytes")?;
                    if events.len() >= CDC_MAX_EVENTS {
                        return Err(MongrelError::ResourceLimitExceeded {
                            resource: "CDC events",
                            requested: events.len().saturating_add(1),
                            limit: CDC_MAX_EVENTS,
                        });
                    }
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
        control.checkpoint()?;
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
        let control = crate::ExecutionControl::new(None);
        self.call_procedure_as_controlled(name, args, principal, &control, || true)
    }

    /// Execute only the exact procedure revision previously authorized by the
    /// caller. A dropped or replaced definition fails closed.
    #[doc(hidden)]
    pub fn call_procedure_as_bound(
        &self,
        expected: &StoredProcedure,
        args: HashMap<String, crate::Value>,
        principal: Option<&crate::auth::Principal>,
    ) -> Result<ProcedureCallResult> {
        self.require_for(principal, &crate::auth::Permission::All)?;
        let procedure = self.procedure(&expected.name).ok_or_else(|| {
            MongrelError::NotFound(format!("procedure {:?} not found", expected.name))
        })?;
        if &procedure != expected {
            return Err(MongrelError::Conflict(format!(
                "procedure {:?} changed after request authorization",
                expected.name
            )));
        }
        let control = crate::ExecutionControl::new(None);
        self.execute_procedure_as_controlled(procedure, args, principal, &control, || true)
    }

    /// Execute a procedure with cooperative cancellation during preparation.
    /// `before_commit` runs after every procedure step has succeeded and
    /// immediately before a write procedure commits. Returning `false` aborts
    /// the transaction without publishing it.
    #[doc(hidden)]
    pub fn call_procedure_as_controlled<F>(
        &self,
        name: &str,
        args: HashMap<String, crate::Value>,
        principal: Option<&crate::auth::Principal>,
        control: &crate::ExecutionControl,
        before_commit: F,
    ) -> Result<ProcedureCallResult>
    where
        F: FnOnce() -> bool,
    {
        // v1 requires ALL to call procedures on a require_auth database; a
        // finer SECURITY DEFINER-style marker is a future extension (spec §9
        // decision 1).
        self.require_for(principal, &crate::auth::Permission::All)?;
        let procedure = self
            .procedure(name)
            .ok_or_else(|| MongrelError::NotFound(format!("procedure {name:?} not found")))?;
        self.execute_procedure_as_controlled(procedure, args, principal, control, before_commit)
    }

    fn execute_procedure_as_controlled<F>(
        &self,
        procedure: StoredProcedure,
        args: HashMap<String, crate::Value>,
        principal: Option<&crate::auth::Principal>,
        control: &crate::ExecutionControl,
        before_commit: F,
    ) -> Result<ProcedureCallResult>
    where
        F: FnOnce() -> bool,
    {
        let args = bind_procedure_args(&procedure, args)?;
        let has_writes = procedure.body.steps.iter().any(ProcedureStep::is_write);
        let mut outputs: HashMap<String, ProcedureCallOutput> = HashMap::new();
        if has_writes {
            let mut tx = self.begin_as(principal.cloned());
            let run = (|| {
                for (step_index, step) in procedure.body.steps.iter().enumerate() {
                    if step_index % 256 == 0 {
                        control.checkpoint()?;
                    }
                    let output = self.execute_procedure_step(
                        step,
                        &args,
                        &outputs,
                        Some(&mut tx),
                        principal,
                        Some(control),
                    )?;
                    outputs.insert(step.id().to_string(), output);
                }
                control.checkpoint()?;
                eval_return_output(&procedure.body.return_value, &args, &outputs)
            })();
            match run {
                Ok(output) => {
                    control.checkpoint()?;
                    if !before_commit() {
                        tx.rollback();
                        return Err(MongrelError::Cancelled);
                    }
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
            for (step_index, step) in procedure.body.steps.iter().enumerate() {
                if step_index % 256 == 0 {
                    control.checkpoint()?;
                }
                let output = self.execute_procedure_step(
                    step,
                    &args,
                    &outputs,
                    None,
                    principal,
                    Some(control),
                )?;
                outputs.insert(step.id().to_string(), output);
            }
            control.checkpoint()?;
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
        control: Option<&crate::ExecutionControl>,
    ) -> Result<ProcedureCallOutput> {
        if let Some(control) = control {
            control.checkpoint()?;
        }
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
                let fallback_control = crate::ExecutionControl::new(None);
                let query_control = control.unwrap_or(&fallback_control);
                let mut rows = self.query_for_principal_controlled(
                    table,
                    &q,
                    projection.as_deref(),
                    principal,
                    false,
                    query_control,
                )?;
                if let Some(limit) = limit {
                    rows.truncate(*limit);
                }
                let mut output = Vec::with_capacity(rows.len());
                for (row_index, row) in rows.into_iter().enumerate() {
                    if row_index % 256 == 0 {
                        if let Some(control) = control {
                            control.checkpoint()?;
                        }
                    }
                    output.push(ProcedureCallRow {
                        row_id: Some(row.row_id),
                        columns: row.columns,
                    });
                }
                Ok(ProcedureCallOutput::Rows(output))
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

    fn transaction_principal_snapshot(&self) -> (Option<crate::auth::Principal>, bool) {
        let principal = self.principal.read().clone();
        let catalog_bound = principal.as_ref().is_some_and(|principal| {
            let catalog = self.catalog.read();
            catalog.require_auth || principal.user_id != 0
        });
        (principal, catalog_bound)
    }

    pub fn begin_as(
        &self,
        principal: Option<crate::auth::Principal>,
    ) -> crate::txn::Transaction<'_> {
        let catalog_bound = principal.as_ref().is_some_and(|principal| {
            let catalog = self.catalog.read();
            catalog.require_auth || principal.user_id != 0
        });
        let txn_id = self.alloc_txn_id();
        let read = Snapshot::at(self.epoch.visible());
        crate::txn::Transaction::new(self, txn_id, read).with_principal(principal, catalog_bound)
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
        let (principal, catalog_bound) = self.transaction_principal_snapshot();
        crate::txn::Transaction::new(self, txn_id, read).with_principal(principal, catalog_bound)
    }

    /// Begin a transaction whose trigger programs may route external-table DML
    /// through an application/query-layer module bridge.
    pub fn begin_with_external_trigger_bridge<'a>(
        &'a self,
        bridge: &'a dyn ExternalTriggerBridge,
    ) -> crate::txn::Transaction<'a> {
        let txn_id = self.alloc_txn_id();
        let read = Snapshot::at(self.epoch.visible());
        let (principal, catalog_bound) = self.transaction_principal_snapshot();
        crate::txn::Transaction::new(self, txn_id, read)
            .with_external_trigger_bridge(bridge)
            .with_principal(principal, catalog_bound)
    }

    pub fn begin_with_external_trigger_bridge_as<'a>(
        &'a self,
        bridge: &'a dyn ExternalTriggerBridge,
        principal: Option<crate::auth::Principal>,
    ) -> crate::txn::Transaction<'a> {
        let catalog_bound = principal.as_ref().is_some_and(|principal| {
            let catalog = self.catalog.read();
            catalog.require_auth || principal.user_id != 0
        });
        let txn_id = self.alloc_txn_id();
        let read = Snapshot::at(self.epoch.visible());
        crate::txn::Transaction::new(self, txn_id, read)
            .with_external_trigger_bridge(bridge)
            .with_principal(principal, catalog_bound)
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

    pub fn transaction_with_row_ids<T>(
        &self,
        f: impl FnOnce(&mut crate::txn::Transaction) -> Result<T>,
    ) -> Result<(T, Vec<RowId>)> {
        let mut tx = self.begin();
        match f(&mut tx) {
            Ok(output) => {
                let (_, row_ids) = tx.commit_with_row_ids()?;
                Ok((output, row_ids))
            }
            Err(error) => {
                tx.rollback();
                Err(error)
            }
        }
    }

    pub fn transaction_for_current_principal<T>(
        &self,
        f: impl FnOnce(&mut crate::txn::Transaction) -> Result<T>,
    ) -> Result<T> {
        if self.principal.read().is_some() {
            self.refresh_principal()?;
        }
        let mut transaction = self.begin_as(self.principal.read().clone());
        match f(&mut transaction) {
            Ok(output) => {
                transaction.commit()?;
                Ok(output)
            }
            Err(error) => {
                transaction.rollback();
                Err(error)
            }
        }
    }

    pub fn transaction_for_current_principal_with_epoch<T>(
        &self,
        f: impl FnOnce(&mut crate::txn::Transaction) -> Result<T>,
    ) -> Result<(Epoch, T)> {
        if self.principal.read().is_some() {
            self.refresh_principal()?;
        }
        let mut transaction = self.begin_as(self.principal.read().clone());
        match f(&mut transaction) {
            Ok(output) => {
                let epoch = transaction.commit()?;
                Ok((epoch, output))
            }
            Err(error) => {
                transaction.rollback();
                Err(error)
            }
        }
    }

    pub fn transaction_with_row_ids_for_current_principal<T>(
        &self,
        f: impl FnOnce(&mut crate::txn::Transaction) -> Result<T>,
    ) -> Result<(T, Vec<RowId>)> {
        if self.principal.read().is_some() {
            self.refresh_principal()?;
        }
        let mut transaction = self.begin_as(self.principal.read().clone());
        match f(&mut transaction) {
            Ok(output) => {
                let (_, row_ids) = transaction.commit_with_row_ids()?;
                Ok((output, row_ids))
            }
            Err(error) => {
                transaction.rollback();
                Err(error)
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
        control: Option<&crate::ExecutionControl>,
    ) -> Result<()> {
        let mut puts_by_table: HashMap<u64, Vec<usize>> = HashMap::new();
        for (index, (table_id, staged)) in staging.iter().enumerate() {
            commit_prepare_checkpoint(control, index)?;
            if matches!(staged, crate::txn::Staged::Put(_)) {
                puts_by_table.entry(*table_id).or_default().push(index);
            }
        }

        let tables = self.tables.read();
        for (table_index, (table_id, indexes)) in puts_by_table.into_iter().enumerate() {
            commit_prepare_checkpoint(control, table_index)?;
            if let Some(handle) = tables.get(&table_id) {
                #[cfg(test)]
                AUTO_INCREMENT_TABLE_LOCKS.with(|count| count.set(count.get() + 1));
                let mut t = handle.lock();
                for (fill_index, index) in indexes.into_iter().enumerate() {
                    commit_prepare_checkpoint(control, fill_index)?;
                    if let crate::txn::Staged::Put(cells) = &mut staging[index].1 {
                        t.fill_auto_inc(cells)?;
                    }
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
        control: Option<&crate::ExecutionControl>,
    ) -> Result<()> {
        commit_prepare_checkpoint(control, 0)?;
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
                control,
            )?;
            self.apply_external_trigger_writes(
                external_writes,
                external_trigger_bridge,
                external_states,
                staging,
                control,
            )?;
            return Ok(());
        }

        let mut expansion =
            self.expand_table_triggers_once(staging, read_epoch, None, &config, control)?;
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
            control,
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
        control: Option<&crate::ExecutionControl>,
    ) -> Result<Vec<(u64, crate::txn::Staged)>> {
        if chunk.is_empty() {
            return Ok(Vec::new());
        }
        commit_prepare_checkpoint(control, 0)?;
        self.fill_auto_increment_for_staging(&mut chunk, control)?;
        let expansion = self.expand_table_triggers_once(
            &mut chunk,
            read_epoch,
            Some(&stacks),
            config,
            control,
        )?;
        if depth >= max_depth && (!expansion.before.is_empty() || !expansion.after.is_empty()) {
            let stack = expansion
                .before_stacks
                .first()
                .or_else(|| expansion.after_stacks.first())
                .cloned()
                .unwrap_or_default();
            return Err(MongrelError::TriggerValidation(format!(
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
            control,
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
            control,
        )?);
        Ok(out)
    }

    fn apply_external_trigger_writes(
        &self,
        writes: Vec<ExternalTriggerWrite>,
        bridge: Option<&dyn ExternalTriggerBridge>,
        external_states: &mut Vec<(String, Vec<u8>)>,
        staging: &mut Vec<(u64, crate::txn::Staged)>,
        control: Option<&crate::ExecutionControl>,
    ) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }
        let bridge = bridge.ok_or_else(|| {
            MongrelError::TriggerValidation(
                "trigger program wrote an external table, but this transaction has no external trigger bridge".into(),
            )
        })?;
        for (write_index, write) in writes.into_iter().enumerate() {
            commit_prepare_checkpoint(control, write_index)?;
            let table = write.table().to_string();
            let entry = self.external_table(&table).ok_or_else(|| {
                MongrelError::NotFound(format!("external table {table:?} not found"))
            })?;
            let base_state = current_external_state_bytes(&self.root, external_states, &table)?;
            let result = bridge.apply_trigger_external_write(&entry, base_state, write)?;
            external_states.push((table, result.state));
            for (base_index, base_write) in result.base_writes.into_iter().enumerate() {
                commit_prepare_checkpoint(control, base_index)?;
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
        control: Option<&crate::ExecutionControl>,
    ) -> Result<TriggerExpansion> {
        commit_prepare_checkpoint(control, 0)?;
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
                self.trigger_events_for_staging(staging, read_epoch, trigger_stacks, control)?;
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
                control,
            )?;
        }

        let after_events = if after_triggers.is_empty() {
            Vec::new()
        } else {
            self.trigger_events_for_staging(staging, read_epoch, trigger_stacks, control)?
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
            control,
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

    #[allow(clippy::too_many_arguments)]
    fn execute_triggers_for_events(
        &self,
        triggers: &[StoredTrigger],
        events: &[WriteEvent],
        mut staging: Option<&mut Vec<(u64, crate::txn::Staged)>>,
        out: &mut TriggerProgramOutput<'_>,
        config: &TriggerConfig,
        read_epoch: Epoch,
        control: Option<&crate::ExecutionControl>,
    ) -> Result<()> {
        let mut checkpoint_index = 0_usize;
        for event in events {
            for trigger in triggers {
                commit_prepare_checkpoint(control, checkpoint_index)?;
                checkpoint_index += 1;
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
                    return Err(MongrelError::TriggerValidation(format!(
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
                        control,
                    )?,
                    None => self.execute_trigger_program(
                        trigger,
                        event,
                        None,
                        out,
                        &trigger_stack,
                        config,
                        read_epoch,
                        control,
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
        control: Option<&crate::ExecutionControl>,
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
            commit_prepare_checkpoint(control, idx)?;
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
                Staged::Update { row_id, .. } => {
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

        for (pair_index, (key, deletes)) in delete_by_key.iter_mut().enumerate() {
            commit_prepare_checkpoint(control, pair_index)?;
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
            commit_prepare_checkpoint(control, idx)?;
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
                Staged::Update { new_row: cells, .. } => {
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
        control: Option<&crate::ExecutionControl>,
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
            control,
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
        control: Option<&crate::ExecutionControl>,
    ) -> Result<TriggerProgramOutcome> {
        let _ = depth;
        for (step_index, step) in steps.iter().enumerate() {
            commit_prepare_checkpoint(control, step_index)?;
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
                    let mut update_changed_columns = None;
                    let row_cells = match staging.get_mut(put_idx).map(|(_, op)| op) {
                        Some(crate::txn::Staged::Put(cells)) => cells,
                        Some(crate::txn::Staged::Update {
                            new_row,
                            changed_columns,
                            ..
                        }) => {
                            update_changed_columns = Some(changed_columns);
                            new_row
                        }
                        _ => {
                            return Err(MongrelError::InvalidArgument(
                                "SetNew trigger step target row is not mutable".into(),
                            ))
                        }
                    };
                    for (column_id, value) in eval_trigger_cells(cells, event, selected)? {
                        row_cells.retain(|(id, _)| *id != column_id);
                        row_cells.push((column_id, value.clone()));
                        if let Some(changed_columns) = &mut update_changed_columns {
                            changed_columns.push(column_id);
                        }
                        if let Some(new) = &mut event.new {
                            new.columns.insert(column_id, value);
                        }
                    }
                    row_cells.sort_by_key(|(id, _)| *id);
                    if let Some(changed_columns) = update_changed_columns {
                        changed_columns.sort_unstable();
                        changed_columns.dedup();
                    }
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
                        let mut changed_columns = cells
                            .iter()
                            .map(|(column_id, _)| *column_id)
                            .collect::<Vec<_>>();
                        changed_columns.sort_unstable();
                        changed_columns.dedup();
                        let mut merged = old.columns;
                        for (column_id, value) in cells {
                            merged.insert(column_id, value);
                        }
                        out.added.push((
                            self.table_id(table)?,
                            crate::txn::Staged::Update {
                                row_id,
                                new_row: merged.into_iter().collect(),
                                changed_columns,
                            },
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
                    let handle = self.table(table)?;
                    let rows = match control {
                        Some(control) => {
                            handle.lock().visible_rows_controlled(snapshot, control)?
                        }
                        None => handle.lock().visible_rows(snapshot)?,
                    };
                    let mut matched = Vec::new();
                    for (row_index, row) in rows.into_iter().enumerate() {
                        commit_prepare_checkpoint(control, row_index)?;
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
                    for (row_index, row) in rows.clone().into_iter().enumerate() {
                        commit_prepare_checkpoint(control, row_index)?;
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
                            control,
                        )?;
                        if result == TriggerProgramOutcome::Ignore {
                            return Ok(TriggerProgramOutcome::Ignore);
                        }
                    }
                }
                TriggerStep::DeleteWhere { table, conditions } => {
                    let schema = self.table(table)?.lock().schema().clone();
                    let snapshot = Snapshot::at(read_epoch);
                    let handle = self.table(table)?;
                    let rows = match control {
                        Some(control) => {
                            handle.lock().visible_rows_controlled(snapshot, control)?
                        }
                        None => handle.lock().visible_rows(snapshot)?,
                    };
                    let table_id = self.table_id(table)?;
                    let mut to_delete = Vec::new();
                    for (row_index, row) in rows.into_iter().enumerate() {
                        commit_prepare_checkpoint(control, row_index)?;
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
                    for (row_index, (table_id, row_id)) in to_delete.into_iter().enumerate() {
                        commit_prepare_checkpoint(control, row_index)?;
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
                    let handle = self.table(table)?;
                    let rows = match control {
                        Some(control) => {
                            handle.lock().visible_rows_controlled(snapshot, control)?
                        }
                        None => handle.lock().visible_rows(snapshot)?,
                    };
                    let table_id = self.table_id(table)?;
                    let mut changed_columns =
                        cells.iter().map(|cell| cell.column_id).collect::<Vec<_>>();
                    changed_columns.sort_unstable();
                    changed_columns.dedup();
                    let mut to_update = Vec::new();
                    for (row_index, row) in rows.into_iter().enumerate() {
                        commit_prepare_checkpoint(control, row_index)?;
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
                    for (row_index, (table_id, row_id, merged)) in to_update.into_iter().enumerate()
                    {
                        commit_prepare_checkpoint(control, row_index)?;
                        out.added.push((
                            table_id,
                            crate::txn::Staged::Update {
                                row_id,
                                new_row: merged.into_iter().collect(),
                                changed_columns: changed_columns.clone(),
                            },
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
                        return Err(MongrelError::TriggerValidation(format!(
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
        control: Option<&crate::ExecutionControl>,
    ) -> Result<()> {
        use crate::constraint::{encode_composite_key, validate_checks, FkAction};
        use crate::memtable::Row;
        use crate::txn::Staged;
        use std::collections::HashSet;

        commit_prepare_checkpoint(control, 0)?;
        let snapshot = Snapshot::at(read_epoch);
        let cat = self.catalog.read();

        // Collect live (id, name, constraints-bearing?) for staged tables.
        let live: Vec<(u64, &str, &crate::schema::Schema)> = cat
            .tables
            .iter()
            .filter(|entry| matches!(entry.state, TableState::Live | TableState::Building { .. }))
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
            let rows = match control {
                Some(control) => handle.lock().visible_rows_controlled(snapshot, control)?,
                None => handle.lock().visible_rows(snapshot)?,
            };
            rows_cache.insert(table_id, rows.clone());
            Ok(rows)
        };

        // ── Phase A1: expand ON UPDATE CASCADE / SET NULL while updates still
        // carry an explicit old RowId + full new image. This makes action choice
        // reliable even when the referenced key itself changes; a delete+put
        // heuristic cannot distinguish that from unrelated operations.
        let mut processed_updates = HashSet::new();
        type PendingUpdate = (usize, u64, crate::rowid::RowId, Vec<(u16, Value)>);
        let mut update_pass = 0_usize;
        loop {
            commit_prepare_checkpoint(control, update_pass)?;
            update_pass += 1;
            let updates: Vec<PendingUpdate> = staging
                .iter()
                .enumerate()
                .filter_map(|(index, (table_id, op))| match op {
                    Staged::Update {
                        row_id,
                        new_row: cells,
                        ..
                    } if !processed_updates.contains(&index) => {
                        Some((index, *table_id, *row_id, cells.clone()))
                    }
                    _ => None,
                })
                .collect();
            if updates.is_empty() {
                break;
            }
            let mut new_ops = Vec::new();
            for (update_index, (index, table_id, row_id, new_cells)) in
                updates.into_iter().enumerate()
            {
                commit_prepare_checkpoint(control, update_index)?;
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
                        for (child_index, child) in child_rows.into_iter().enumerate() {
                            commit_prepare_checkpoint(control, child_index)?;
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
                                    FkAction::Restrict => {
                                        return Err(MongrelError::Other(
                                            "restricted foreign-key update reached cascade preparation"
                                                .into(),
                                        ));
                                    }
                                };
                                cells.push((*child_column, value));
                            }
                            cells.sort_by_key(|(column_id, _)| *column_id);
                            if let Some(existing_index) = staging.iter().position(|(id, op)| {
                                *id == *child_id
                                    && matches!(op, Staged::Update { row_id, .. } if *row_id == child.row_id)
                            }) {
                                if let Staged::Update {
                                    new_row: existing,
                                    changed_columns,
                                    ..
                                } = &mut staging[existing_index].1 {
                                    changed_columns.extend(fk.columns.iter().copied());
                                    changed_columns.sort_unstable();
                                    changed_columns.dedup();
                                    if *existing != cells {
                                        *existing = cells;
                                        processed_updates.remove(&existing_index);
                                    }
                                }
                            } else {
                                new_ops.push((
                                    *child_id,
                                    Staged::Update {
                                        row_id: child.row_id,
                                        new_row: cells,
                                        changed_columns: fk.columns.clone(),
                                    },
                                ));
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
        let mut cascade_pass = 0_usize;
        loop {
            commit_prepare_checkpoint(control, cascade_pass)?;
            cascade_pass += 1;
            let mut new_ops: Vec<(u64, Staged)> = Vec::new();
            let deletes: Vec<(u64, crate::rowid::RowId)> = staging
                .iter()
                .filter_map(|(t, op)| match op {
                    Staged::Delete(rid) => Some((*t, *rid)),
                    _ => None,
                })
                .collect();
            for (delete_index, (table_id, rid)) in deletes.into_iter().enumerate() {
                commit_prepare_checkpoint(control, delete_index)?;
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
                                for (child_index, cr) in child_rows.iter().enumerate() {
                                    commit_prepare_checkpoint(control, child_index)?;
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
                                for (child_index, cr) in child_rows.iter().enumerate() {
                                    commit_prepare_checkpoint(control, child_index)?;
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
                                        new_ops.push((
                                            *child_id,
                                            Staged::Update {
                                                row_id: cr.row_id,
                                                new_row: cells,
                                                changed_columns: fk.columns.clone(),
                                            },
                                        ));
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
                Staged::Delete(rid) | Staged::Update { row_id: rid, .. } => Some((*t, rid.0)),
                _ => None,
            })
            .collect();

        // Intra-transaction unique-key dedup: (table_id, uc_id, key).
        let mut seen_unique: HashSet<(u64, u16, Vec<u8>)> = HashSet::new();

        // ── Phase B: validate the fully-expanded staging set.
        for (operation_index, (table_id, op)) in staging.iter().enumerate() {
            commit_prepare_checkpoint(control, operation_index)?;
            let Some((_, tname, schema)) = live.iter().find(|(t, _, _)| t == table_id).copied()
            else {
                continue;
            };
            let cells_map: HashMap<u16, crate::memtable::Value>;
            match op {
                Staged::Put(cells) | Staged::Update { new_row: cells, .. } => {
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
                        for (row_index, r) in rows.iter().enumerate() {
                            commit_prepare_checkpoint(control, row_index)?;
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
                        for (row_index, r) in parent_rows.iter().enumerate() {
                            commit_prepare_checkpoint(control, row_index)?;
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
                            for (staged_index, (st_table, st_op)) in staging.iter().enumerate() {
                                commit_prepare_checkpoint(control, staged_index)?;
                                if *st_table != parent_id {
                                    continue;
                                }
                                if let Staged::Put(pcells)
                                | Staged::Update {
                                    new_row: pcells, ..
                                } = st_op
                                {
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
                    if let Staged::Update { row_id, .. } = op {
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
                                for (child_index, child) in
                                    load_rows(*child_id)?.into_iter().enumerate()
                                {
                                    commit_prepare_checkpoint(control, child_index)?;
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
                                            Staged::Update {
                                                row_id,
                                                new_row: cells,
                                                ..
                                            } if *row_id == child.row_id => {
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
                            for (row_index, r) in child_rows.iter().enumerate() {
                                commit_prepare_checkpoint(control, row_index)?;
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

    fn validate_write_permissions(
        &self,
        staging: &[(u64, crate::txn::Staged)],
        principal: Option<&crate::auth::Principal>,
        control: Option<&crate::ExecutionControl>,
    ) -> Result<()> {
        commit_prepare_checkpoint(control, 0)?;
        if principal.is_none() && !self.auth_state.require_auth() {
            return Ok(());
        }
        let principal = principal.ok_or(MongrelError::AuthRequired)?;
        let needs = summarize_write_permissions(staging);
        let catalog = self.catalog.read();

        if needs.values().any(|need| need.truncate) {
            self.require_for(Some(principal), &crate::auth::Permission::Admin)?;
        }
        for (need_index, (table_id, need)) in needs.into_iter().enumerate() {
            commit_prepare_checkpoint(control, need_index)?;
            let entry = catalog
                .tables
                .iter()
                .find(|entry| {
                    entry.table_id == table_id
                        && matches!(entry.state, TableState::Live | TableState::Building { .. })
                })
                .ok_or_else(|| {
                    MongrelError::NotFound(format!(
                        "live table {table_id} not found during write validation"
                    ))
                })?;
            if matches!(entry.state, TableState::Building { .. }) {
                self.require_for(Some(principal), &crate::auth::Permission::Ddl)?;
                continue;
            }
            if need.insert {
                Self::require_columns_for_principal(
                    &entry.name,
                    &entry.schema,
                    crate::auth::ColumnOperation::Insert,
                    &need.insert_columns,
                    principal,
                )?;
            }
            if need.update {
                Self::require_columns_for_principal(
                    &entry.name,
                    &entry.schema,
                    crate::auth::ColumnOperation::Update,
                    &need.update_columns,
                    principal,
                )?;
            }
            if need.delete {
                self.require_for(
                    Some(principal),
                    &crate::auth::Permission::Delete {
                        table: entry.name.clone(),
                    },
                )?;
            }
        }
        Ok(())
    }

    fn validate_security_writes(
        &self,
        staging: &[(u64, crate::txn::Staged)],
        read_epoch: Epoch,
        explicit_principal: Option<&crate::auth::Principal>,
        control: Option<&crate::ExecutionControl>,
    ) -> Result<()> {
        commit_prepare_checkpoint(control, 0)?;
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
        let principal = explicit_principal.ok_or(MongrelError::AuthRequired)?;

        for (operation_index, (table_id, operation)) in staging.iter().enumerate() {
            commit_prepare_checkpoint(control, operation_index)?;
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
                Staged::Update {
                    row_id,
                    new_row: cells,
                    ..
                } => {
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
        staging: Vec<(u64, crate::txn::Staged)>,
        external_states: Vec<(String, Vec<u8>)>,
        materialized_view_updates: Vec<crate::catalog::MaterializedViewEntry>,
        security_principal: Option<crate::auth::Principal>,
        principal_catalog_bound: bool,
        external_trigger_bridge: Option<&dyn ExternalTriggerBridge>,
    ) -> Result<(Epoch, Vec<RowId>)> {
        self.commit_transaction_with_external_states_inner(
            txn_id,
            read_epoch,
            staging,
            external_states,
            materialized_view_updates,
            security_principal,
            principal_catalog_bound,
            external_trigger_bridge,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn commit_transaction_with_external_states_controlled(
        &self,
        txn_id: u64,
        read_epoch: Epoch,
        staging: Vec<(u64, crate::txn::Staged)>,
        external_states: Vec<(String, Vec<u8>)>,
        materialized_view_updates: Vec<crate::catalog::MaterializedViewEntry>,
        security_principal: Option<crate::auth::Principal>,
        principal_catalog_bound: bool,
        external_trigger_bridge: Option<&dyn ExternalTriggerBridge>,
        control: &crate::ExecutionControl,
        before_commit: &mut dyn FnMut() -> Result<()>,
    ) -> Result<(Epoch, Vec<RowId>)> {
        self.commit_transaction_with_external_states_inner(
            txn_id,
            read_epoch,
            staging,
            external_states,
            materialized_view_updates,
            security_principal,
            principal_catalog_bound,
            external_trigger_bridge,
            Some(control),
            Some(before_commit),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_transaction_with_external_states_inner(
        &self,
        txn_id: u64,
        read_epoch: Epoch,
        mut staging: Vec<(u64, crate::txn::Staged)>,
        external_states: Vec<(String, Vec<u8>)>,
        materialized_view_updates: Vec<crate::catalog::MaterializedViewEntry>,
        mut security_principal: Option<crate::auth::Principal>,
        principal_catalog_bound: bool,
        external_trigger_bridge: Option<&dyn ExternalTriggerBridge>,
        control: Option<&crate::ExecutionControl>,
        mut before_commit: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<(Epoch, Vec<RowId>)> {
        use crate::memtable::Row;
        use crate::txn::{Staged, StagedOp, WriteKey};
        use crate::wal::Op;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        use std::sync::atomic::Ordering;

        if txn_id == crate::wal::SYSTEM_TXN_ID {
            return Err(MongrelError::Full(
                "per-open transaction id namespace exhausted; reopen the database".into(),
            ));
        }
        if self.read_only {
            return Err(MongrelError::ReadOnlyReplica);
        }
        commit_prepare_checkpoint(control, 0)?;
        let observed_security_version = self.security_coordinator.version.load(Ordering::Acquire);
        self.refresh_security_catalog_if_stale(observed_security_version)?;
        let trigger_binding = trigger_catalog_binding(&self.catalog.read());
        if self.auth_state.require_auth() && security_principal.is_none() {
            return Err(MongrelError::AuthRequired);
        }
        {
            let catalog = self.catalog.read();
            if catalog.require_auth
                || principal_catalog_bound
                || security_principal
                    .as_ref()
                    .is_some_and(|principal| principal.user_id != 0)
            {
                let principal = security_principal
                    .as_ref()
                    .ok_or(MongrelError::AuthRequired)?;
                security_principal =
                    Self::resolve_bound_principal_from_catalog(&catalog, principal);
                if security_principal.is_none() {
                    return Err(MongrelError::AuthRequired);
                }
            }
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
            for (definition_index, definition) in materialized_view_updates.into_iter().enumerate()
            {
                commit_prepare_checkpoint(control, definition_index)?;
                if definition.name.is_empty() || definition.query.trim().is_empty() {
                    return Err(MongrelError::InvalidArgument(
                        "materialized view name and query must not be empty".into(),
                    ));
                }
                deduplicated.insert(definition.name.clone(), definition);
            }
            let catalog = self.catalog.read();
            let mut prepared = Vec::with_capacity(deduplicated.len());
            for (definition_index, definition) in deduplicated.into_values().enumerate() {
                commit_prepare_checkpoint(control, definition_index)?;
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
        self.fill_auto_increment_for_staging(&mut staging, control)?;
        self.expand_table_triggers(
            &mut staging,
            read_epoch,
            external_trigger_bridge,
            &mut external_states,
            control,
        )?;
        self.fill_auto_increment_for_staging(&mut staging, control)?;
        external_states = dedup_external_states(external_states);
        let expected_external_generations = {
            let catalog = self.catalog.read();
            let mut generations = HashMap::with_capacity(external_states.len());
            for (name, _) in &external_states {
                let entry = catalog
                    .external_tables
                    .iter()
                    .find(|entry| entry.name == *name)
                    .ok_or_else(|| {
                        MongrelError::NotFound(format!("external table {name:?} not found"))
                    })?;
                generations.insert(name.clone(), entry.created_epoch);
            }
            generations
        };

        // Validate declarative constraints (unique / FK / check) under the read
        // snapshot, outside the WAL mutex. Trigger-produced writes are included
        // here, so the batch either satisfies every declared constraint or is
        // rejected atomically.
        self.validate_constraints(&mut staging, read_epoch, control)?;
        self.validate_write_permissions(&staging, security_principal.as_ref(), control)?;
        self.validate_security_writes(&staging, read_epoch, security_principal.as_ref(), control)?;
        let mut normalized = Vec::with_capacity(staging.len() * 2);
        for (staged_index, (table_id, op)) in staging.into_iter().enumerate() {
            commit_prepare_checkpoint(control, staged_index)?;
            match op {
                crate::txn::Staged::Update {
                    row_id,
                    new_row: cells,
                    ..
                } => {
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
            for (staged_index, (table_id, staged)) in staging.iter().enumerate() {
                commit_prepare_checkpoint(control, staged_index)?;
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
                    Staged::Update { .. } => {
                        return Err(MongrelError::Other(
                            "transaction contains an unnormalized update during preparation".into(),
                        ));
                    }
                }
            }
            for (external_index, (name, _)) in external_states.iter().enumerate() {
                commit_prepare_checkpoint(control, external_index)?;
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
            let mut table_bytes: HashMap<u64, u64> = HashMap::new();
            let mut put_indexes: HashMap<u64, Vec<usize>> = HashMap::new();
            for (staged_index, (table_id, staged)) in staging.iter().enumerate() {
                commit_prepare_checkpoint(control, staged_index)?;
                if let Staged::Put(cells) = staged {
                    let bytes = cells.iter().fold(32_u64, |bytes, (_, value)| {
                        bytes.saturating_add(value.estimated_bytes())
                    });
                    let table_bytes = table_bytes.entry(*table_id).or_default();
                    *table_bytes = table_bytes.saturating_add(bytes);
                    put_indexes.entry(*table_id).or_default().push(staged_index);
                }
            }
            let tables = self.tables.read();
            for (table_index, (&table_id, &bytes)) in table_bytes.iter().enumerate() {
                commit_prepare_checkpoint(control, table_index)?;
                if bytes
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
                let run_id = t.alloc_run_id()? as u128;
                let pending_path = txn_dir.join(format!("r-{run_id}.sr"));
                let final_path = t.run_path(run_id as u64);

                let mut rows: Vec<Row> = Vec::new();
                for (put_index, staged_index) in put_indexes[&table_id].iter().enumerate() {
                    commit_prepare_checkpoint(control, put_index)?;
                    let Staged::Put(cells) = &mut staging[*staged_index].1 else {
                        return Err(MongrelError::Other(
                            "transaction put index no longer references a put".into(),
                        ));
                    };
                    t.validate_cells_not_null(cells)?;
                    let row_id = t.alloc_row_id()?;
                    let mut row = Row::new(row_id, Epoch(0));
                    row.columns.extend(std::mem::take(cells));
                    rows.push(row);
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
                commit_prepare_checkpoint(control, 0)?;
                let header = writer.write(&pending_path, &rows)?;
                commit_prepare_checkpoint(control, 0)?;
                let row_count = header.row_count;
                let min_rid = rows.first().map(|r| r.row_id.0).unwrap_or(0);
                let max_rid = rows.last().map(|r| r.row_id.0).unwrap_or(0);

                spilled.push(SpilledRun {
                    table_id,
                    run_id,
                    pending_path,
                    final_path,
                    rows,
                    row_count,
                    min_rid,
                    max_rid,
                    content_hash: header.content_hash,
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
        let mut prebuilt: Vec<Option<Row>> = std::iter::repeat_with(|| None)
            .take(staging.len())
            .collect();
        let mut delete_images: Vec<Option<Row>> = std::iter::repeat_with(|| None)
            .take(staging.len())
            .collect();
        {
            let mut indexes_by_table: HashMap<u64, Vec<usize>> = HashMap::new();
            for (index, (table_id, staged)) in staging.iter().enumerate() {
                commit_prepare_checkpoint(control, index)?;
                if matches!(staged, Staged::Delete(_))
                    || matches!(staged, Staged::Put(_) if !spilled_tables.contains(table_id))
                {
                    indexes_by_table.entry(*table_id).or_default().push(index);
                }
            }
            let tables = self.tables.read();
            for (table_index, (table_id, indexes)) in indexes_by_table.into_iter().enumerate() {
                commit_prepare_checkpoint(control, table_index)?;
                let handle = tables.get(&table_id).ok_or_else(|| {
                    MongrelError::NotFound(format!("table {table_id} not mounted"))
                })?;
                #[cfg(test)]
                PREBUILD_TABLE_LOCKS.with(|count| count.set(count.get() + 1));
                let mut t = handle.lock();
                for (prepare_index, index) in indexes.into_iter().enumerate() {
                    commit_prepare_checkpoint(control, prepare_index)?;
                    match &staging[index].1 {
                        Staged::Put(cells) if !spilled_tables.contains(&table_id) => {
                            t.validate_cells_not_null(cells)?;
                            let mut row = Row::new(t.alloc_row_id()?, Epoch(0));
                            for (column, value) in cells {
                                row.columns.insert(*column, value.clone());
                            }
                            prebuilt[index] = Some(row);
                        }
                        Staged::Delete(row_id) => {
                            delete_images[index] = t.get(*row_id, Snapshot::at(read_epoch));
                        }
                        Staged::Put(_) | Staged::Truncate => {}
                        Staged::Update { .. } => {
                            return Err(MongrelError::Other(
                                "transaction contains an unnormalized update during row preparation"
                                    .into(),
                            ));
                        }
                    }
                }
            }
        }

        // Finish every fallible index read before the commit marker can become
        // durable. Post-durable row/run metadata application is then entirely
        // in-memory and cannot stop halfway through a multi-table publish.
        let prepared_table_handles = {
            let table_ids: HashSet<u64> = staging.iter().map(|(table_id, _)| *table_id).collect();
            let put_table_ids: HashSet<u64> = staging
                .iter()
                .filter_map(|(table_id, staged)| {
                    matches!(staged, Staged::Put(_)).then_some(*table_id)
                })
                .collect();
            let tables = self.tables.read();
            let mut handles = HashMap::with_capacity(table_ids.len());
            for (table_index, table_id) in table_ids.into_iter().enumerate() {
                commit_prepare_checkpoint(control, table_index)?;
                let handle = tables.get(&table_id).ok_or_else(|| {
                    MongrelError::NotFound(format!("table {table_id} not mounted"))
                })?;
                if put_table_ids.contains(&table_id) {
                    match control {
                        Some(control) => {
                            handle.lock().prepare_durable_publish_controlled(control)?
                        }
                        None => handle.lock().prepare_durable_publish()?,
                    }
                }
                handles.insert(table_id, handle.clone());
            }
            handles
        };

        // Link large-transaction spill files before WAL durability. The guard
        // restores their pending names on every error before WAL append begins;
        // publication only attaches already-present files in memory.
        let mut prepared_run_links = PreparedRunLinks::prepare(&spilled)?;

        let mut spilled_row_ids: HashMap<u64, VecDeque<RowId>> = spilled
            .iter()
            .map(|run| {
                (
                    run.table_id,
                    run.rows.iter().map(|row| row.row_id).collect(),
                )
            })
            .collect();
        let committed_row_ids = staging
            .iter()
            .enumerate()
            .filter_map(|(index, (table_id, staged))| {
                if !matches!(staged, Staged::Put(_)) {
                    return None;
                }
                prebuilt[index].as_ref().map(|row| row.row_id).or_else(|| {
                    spilled_row_ids
                        .get_mut(table_id)
                        .and_then(VecDeque::pop_front)
                })
            })
            .collect();

        let mut prepared_external = Vec::with_capacity(external_states.len());
        for (external_index, (name, state)) in external_states.iter().enumerate() {
            commit_prepare_checkpoint(control, external_index)?;
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
                content_hash: s.content_hash,
            })
            .collect();
        if let Some(hook) = self.catalog_commit_hook.lock().as_ref() {
            hook();
        }
        // Lock order: security gate -> commit lock -> shared WAL -> table locks.
        // Security mutations cannot overtake an authorized commit before its
        // commit marker is durable.
        let security_guard = self.security_coordinator.gate.read();
        if self.security_coordinator.version.load(Ordering::Acquire) != observed_security_version {
            return Err(MongrelError::Conflict(
                "security policy changed during write".into(),
            ));
        }
        if spill_guard.is_some() {
            if let Some(hook) = self.security_commit_hook.lock().as_ref() {
                hook();
            }
        }
        let commit_guard = self.commit_lock.lock();
        let catalog_generation_result = (|| {
            {
                let catalog = self.catalog.read();
                for table_id in prepared_table_handles.keys() {
                    let is_current = catalog.tables.iter().any(|entry| {
                        entry.table_id == *table_id
                            && matches!(entry.state, TableState::Live | TableState::Building { .. })
                    });
                    if !is_current {
                        return Err(MongrelError::Conflict(format!(
                            "table {table_id} changed during transaction preparation"
                        )));
                    }
                }
                for (name, created_epoch) in &expected_external_generations {
                    let current = catalog
                        .external_tables
                        .iter()
                        .find(|entry| entry.name == *name)
                        .map(|entry| entry.created_epoch);
                    if current != Some(*created_epoch) {
                        return Err(MongrelError::Conflict(format!(
                            "external table {name:?} changed during transaction preparation"
                        )));
                    }
                }
                for (table_id, definition) in &prepared_materialized_views {
                    let current = catalog.live(&definition.name).map(|entry| entry.table_id);
                    if current != Some(*table_id) {
                        return Err(MongrelError::Conflict(format!(
                            "materialized view {:?} changed during transaction preparation",
                            definition.name
                        )));
                    }
                }
                if trigger_catalog_binding(&catalog) != trigger_binding {
                    return Err(MongrelError::Conflict(
                        "trigger or referenced table generation changed during transaction preparation"
                            .into(),
                    ));
                }
            }
            let tables = self.tables.read();
            for (table_id, prepared) in &prepared_table_handles {
                if !tables
                    .get(table_id)
                    .is_some_and(|current| current.ptr_eq(prepared))
                {
                    return Err(MongrelError::Conflict(format!(
                        "table {table_id} mount changed during transaction preparation"
                    )));
                }
            }
            Ok(())
        })();
        if let Err(error) = catalog_generation_result {
            drop(commit_guard);
            for (_, _, pending) in &prepared_external {
                let _ = std::fs::remove_file(pending);
            }
            return Err(error);
        }
        // The commit lock keeps the next epoch stable while logical spill
        // records are serialized. Build them before taking the shared WAL
        // lock, and cap their aggregate memory/WAL footprint.
        let new_epoch = self.epoch.assigned().next();
        let mut spilled_wal_bytes = 0;
        let mut spilled_wal_records = Vec::<(u64, Op)>::new();
        let spill_prepare = (|| {
            for run in &mut spilled {
                for row in &mut run.rows {
                    row.committed_epoch = new_epoch;
                }
                for rows in encode_spilled_row_chunks(
                    &run.rows,
                    &mut spilled_wal_bytes,
                    SPILLED_WAL_TOTAL_MAX_BYTES,
                    control,
                )? {
                    spilled_wal_records.push((
                        run.table_id,
                        Op::SpilledRows {
                            table_id: run.table_id,
                            rows,
                        },
                    ));
                }
            }
            Result::<()>::Ok(())
        })();
        if let Err(error) = spill_prepare {
            for (_, _, pending) in &prepared_external {
                let _ = std::fs::remove_file(pending);
            }
            return Err(error);
        }
        let (new_epoch, mut _epoch_guard, applies, committed_materialized_views, commit_seq) = {
            let mut wal = self.shared_wal.lock();

            // Re-check only if the conflict index advanced since pre-validation
            // (bounded delta — spec §8.5, review fix #17). If the version is
            // unchanged, the pre-check result is still valid and the sequencer
            // does O(1) work regardless of write-set size.
            if self.conflicts.version() != pre_validate_version
                && self.conflicts.conflicts(&write_keys, read_epoch)
            {
                // Abort: this txn assigned no epoch yet. The prepared-run guard
                // restores final run names to their pending paths on return.
                drop(wal);
                for (_, _, pending) in &prepared_external {
                    let _ = std::fs::remove_file(pending);
                }
                return Err(MongrelError::Conflict(
                    "write-write conflict (sequencer delta re-check)".into(),
                ));
            }

            if let Some(control) = control {
                if let Err(error) = control.checkpoint() {
                    drop(wal);
                    for (_, _, pending) in &prepared_external {
                        let _ = std::fs::remove_file(pending);
                    }
                    return Err(error);
                }
            }
            let mut applies = Vec::<TableApplyBatch>::new();
            let mut apply_indexes = HashMap::<u64, usize>::new();
            let mut committed_materialized_views = Vec::new();
            let mut wal_records = spilled_wal_records;

            let mut index = 0;
            while index < staging.len() {
                let table_id = staging[index].0;
                let handle = prepared_table_handles
                    .get(&table_id)
                    .cloned()
                    .ok_or_else(|| {
                        MongrelError::NotFound(format!("table {table_id} not prepared"))
                    })?;
                let batch_index = *apply_indexes.entry(table_id).or_insert_with(|| {
                    let index = applies.len();
                    applies.push(TableApplyBatch {
                        table_id,
                        handle,
                        ops: Vec::new(),
                    });
                    index
                });

                // Skip puts for tables that were spilled — their data is in a
                // pending run, not in streamed Put records.
                if spilled_tables.contains(&table_id) && matches!(&staging[index].1, Staged::Put(_))
                {
                    index += 1;
                    continue;
                }

                match &staging[index].1 {
                    Staged::Put(_) => {
                        let mut rows = Vec::new();
                        while index < staging.len()
                            && staging[index].0 == table_id
                            && matches!(&staging[index].1, Staged::Put(_))
                        {
                            let mut row = prebuilt[index].take().ok_or_else(|| {
                                MongrelError::Other(
                                    "transaction prepare lost a prebuilt put row".into(),
                                )
                            })?;
                            row.committed_epoch = new_epoch;
                            rows.push(row);
                            index += 1;
                        }
                        let payload = bincode::serialize(&rows)
                            .map_err(|e| MongrelError::Other(format!("row serialize: {e}")))?;
                        wal_records.push((
                            table_id,
                            Op::Put {
                                table_id,
                                rows: payload,
                            },
                        ));
                        applies[batch_index].ops.push(StagedOp::Put(rows));
                    }
                    Staged::Delete(_) => {
                        let mut row_ids = Vec::new();
                        while index < staging.len()
                            && staging[index].0 == table_id
                            && matches!(&staging[index].1, Staged::Delete(_))
                        {
                            let Staged::Delete(row_id) = &staging[index].1 else {
                                return Err(MongrelError::Other(
                                    "transaction delete batch changed during WAL preparation"
                                        .into(),
                                ));
                            };
                            if let Some(before) = &delete_images[index] {
                                wal_records.push((
                                    table_id,
                                    Op::BeforeImage {
                                        table_id,
                                        row_id: *row_id,
                                        row: bincode::serialize(before).map_err(|error| {
                                            MongrelError::Other(format!(
                                                "before-image serialize: {error}"
                                            ))
                                        })?,
                                    },
                                ));
                            }
                            row_ids.push(*row_id);
                            index += 1;
                        }
                        wal_records.push((
                            table_id,
                            Op::Delete {
                                table_id,
                                row_ids: row_ids.clone(),
                            },
                        ));
                        applies[batch_index].ops.push(StagedOp::Delete(row_ids));
                    }
                    Staged::Truncate => {
                        wal_records.push((table_id, Op::TruncateTable { table_id }));
                        applies[batch_index].ops.push(StagedOp::Truncate);
                        index += 1;
                    }
                    Staged::Update { .. } => {
                        return Err(MongrelError::Other(
                            "transaction contains an unnormalized update at the sequencer".into(),
                        ));
                    }
                }
            }

            for (name, state, _) in &prepared_external {
                wal_records.push((
                    EXTERNAL_TABLE_ID,
                    Op::ExternalTableState {
                        name: name.clone(),
                        state: state.clone(),
                    },
                ));
            }

            for (table_id, definition) in &prepared_materialized_views {
                let mut definition = definition.clone();
                definition.last_refresh_epoch = new_epoch.0;
                wal_records.push((
                    *table_id,
                    Op::Ddl(crate::wal::DdlOp::SetMaterializedView {
                        name: definition.name.clone(),
                        definition_json: crate::wal::DdlOp::encode_materialized_view(&definition)?,
                    }),
                ));
                committed_materialized_views.push(definition);
            }
            if !committed_materialized_views.is_empty() {
                let mut next_catalog = self.catalog.read().clone();
                for definition in &committed_materialized_views {
                    if let Some(existing) = next_catalog
                        .materialized_views
                        .iter_mut()
                        .find(|existing| existing.name == definition.name)
                    {
                        *existing = definition.clone();
                    } else {
                        next_catalog.materialized_views.push(definition.clone());
                    }
                }
                next_catalog.db_epoch = next_catalog.db_epoch.max(new_epoch.0);
                wal_records.push((
                    WAL_TABLE_ID,
                    Op::Ddl(crate::wal::DdlOp::CatalogSnapshot {
                        catalog_json: crate::wal::DdlOp::encode_catalog(&next_catalog)?,
                    }),
                ));
            }

            if let Some(control) = control {
                if let Err(error) = control.checkpoint() {
                    drop(wal);
                    for (_, _, pending) in &prepared_external {
                        let _ = std::fs::remove_file(pending);
                    }
                    return Err(error);
                }
            }
            if let Some(before_commit) = before_commit.as_mut() {
                if let Err(error) = before_commit() {
                    drop(wal);
                    for (_, _, pending) in &prepared_external {
                        let _ = std::fs::remove_file(pending);
                    }
                    return Err(error);
                }
            }

            let assigned_epoch = self.epoch.bump_assigned();
            let _epoch_guard = EpochGuard::new(self.epoch.as_ref(), assigned_epoch);
            if assigned_epoch != new_epoch {
                for (_, _, pending) in &prepared_external {
                    let _ = std::fs::remove_file(pending);
                }
                return Err(MongrelError::Conflict(
                    "commit epoch changed while sequencer lock was held".into(),
                ));
            }

            // From this point the outcome can become ambiguous. Keep prepared
            // spill files at the final names referenced by a possibly durable
            // commit marker; orphan cleanup is safe when the append did fail.
            prepared_run_links.disarm();

            let append: Result<u64> = (|| {
                for (table_id, op) in wal_records {
                    wal.append(txn_id, table_id, op)?;
                }
                wal.append_commit(txn_id, new_epoch, &added_runs)
            })();
            let commit_seq =
                append.map_err(|error| self.commit_outcome_unknown(new_epoch, error))?;

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
        drop(commit_guard);

        // ── 2b. Durability: one leader fsync serves this whole batch (P3.2). ──
        self.await_durable_commit(commit_seq, new_epoch)?;
        drop(security_guard);

        // ── 3. Publish: apply non-spilled ops + link spilled runs ──
        let publish_result: Result<()> = {
            let mut first_error = None;
            let mut spilled_by_table: HashMap<u64, Vec<&SpilledRun>> = HashMap::new();
            for run in &spilled {
                spilled_by_table.entry(run.table_id).or_default().push(run);
            }
            let mut modified_tables = Vec::with_capacity(applies.len());
            // Apply every table completely before any fallible manifest write.
            // The visible epoch remains unchanged until all tables are coherent.
            for batch in applies {
                #[cfg(test)]
                PUBLISH_TABLE_LOCKS.with(|count| count.set(count.get() + 1));
                let mut t = batch.handle.lock();
                for op in batch.ops {
                    match op {
                        StagedOp::Put(rows) => t.apply_put_rows_prepared(rows),
                        StagedOp::Delete(row_ids) => {
                            for row_id in row_ids {
                                t.apply_delete(row_id, new_epoch);
                            }
                        }
                        StagedOp::Truncate => t.apply_truncate(new_epoch),
                    }
                }
                if let Some(runs) = spilled_by_table.remove(&batch.table_id) {
                    for run in runs {
                        t.link_run(crate::manifest::RunRef {
                            run_id: run.run_id,
                            level: 0,
                            epoch_created: new_epoch.0,
                            row_count: run.row_count,
                        });
                        t.apply_run_metadata_prepared(&run.rows)?;
                        if truncated_tables.contains(&batch.table_id) {
                            // TRUNCATE + spilled puts fully describe this table at
                            // the commit epoch. Endorse the epoch so clean-reopen
                            // recovery does not replay the truncate over the
                            // already-linked replacement run.
                            t.set_flushed_epoch(new_epoch);
                        }
                    }
                }
                t.invalidate_pending_cache();
                drop(t);
                modified_tables.push(batch.handle);
            }

            // Checkpoint only after every live table carries the durable state.
            // Continue after one checkpoint failure so runtime publication stays
            // all-or-nothing; WAL recovery repairs failed files on reopen.
            for handle in modified_tables {
                #[cfg(test)]
                COMMIT_MANIFEST_WRITES.with(|count| count.set(count.get() + 1));
                if let Err(error) = handle.lock().persist_manifest(new_epoch) {
                    first_error.get_or_insert(error);
                }
            }
            for (name, _, pending) in &prepared_external {
                if let Err(error) = publish_external_state_file(&self.root, name, pending) {
                    first_error.get_or_insert(error);
                }
            }
            if !committed_materialized_views.is_empty() {
                let mut next_catalog = self.catalog.read().clone();
                for definition in committed_materialized_views {
                    if let Some(existing) = next_catalog
                        .materialized_views
                        .iter_mut()
                        .find(|existing| existing.name == definition.name)
                    {
                        *existing = definition;
                    } else {
                        next_catalog.materialized_views.push(definition);
                    }
                }
                next_catalog.db_epoch = next_catalog.db_epoch.max(new_epoch.0);
                if let Err(error) = self.checkpoint_catalog_after_durable(next_catalog) {
                    first_error.get_or_insert(error);
                }
            }
            match first_error {
                Some(error) => Err(error),
                None => Ok(()),
            }
        };

        if has_changes {
            let _ = self.change_wake.send(());
        }
        self.finish_durable_publish(new_epoch, &mut _epoch_guard, publish_result)?;
        Ok((new_epoch, committed_row_ids))
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
        let published = std::cell::Cell::new(false);
        let result = write_history_retention(&self.root, epochs, start, || {
            self.snapshots.configure_history(epochs, start);
            published.set(true);
        });
        match result {
            Err(error) if published.get() => Err(MongrelError::CommitOutcomeUnknown {
                epoch: current.0,
                message: format!("history-retention publication was not durable: {error}"),
            }),
            result => result,
        }
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
        self.ensure_owner_process()?;
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
    pub(crate) fn table_by_id(&self, id: u64) -> Result<TableHandle> {
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
        if name.starts_with(CTAS_BUILD_TABLE_PREFIX) {
            return Err(MongrelError::InvalidArgument(format!(
                "table names beginning with {CTAS_BUILD_TABLE_PREFIX:?} are reserved"
            )));
        }
        self.create_table_with_state(name, schema, TableState::Live)
    }

    /// Create a durable but non-queryable CTAS build table.
    #[doc(hidden)]
    pub fn create_building_table(
        &self,
        build_name: &str,
        intended_name: &str,
        query_id: &str,
        schema: Schema,
    ) -> Result<u64> {
        if !build_name.starts_with(CTAS_BUILD_TABLE_PREFIX)
            || intended_name.is_empty()
            || intended_name.starts_with(CTAS_BUILD_TABLE_PREFIX)
            || query_id.is_empty()
        {
            return Err(MongrelError::InvalidArgument(
                "invalid CTAS building-table identity".into(),
            ));
        }
        self.create_table_with_state(
            build_name,
            schema,
            TableState::Building {
                intended_name: intended_name.to_string(),
                query_id: query_id.to_string(),
                created_at_unix_nanos: current_unix_nanos(),
                replaces_table_id: None,
            },
        )
    }

    /// Create a hidden schema-rebuild table while the intended target remains
    /// live. Publication later validates that the same target is still live.
    #[doc(hidden)]
    pub fn create_rebuilding_table(
        &self,
        build_name: &str,
        intended_name: &str,
        query_id: &str,
        schema: Schema,
    ) -> Result<u64> {
        if !build_name.starts_with(CTAS_BUILD_TABLE_PREFIX)
            || intended_name.is_empty()
            || intended_name.starts_with(CTAS_BUILD_TABLE_PREFIX)
            || query_id.is_empty()
        {
            return Err(MongrelError::InvalidArgument(
                "invalid rebuilding-table identity".into(),
            ));
        }
        let replaces_table_id = self
            .catalog
            .read()
            .live(intended_name)
            .ok_or_else(|| MongrelError::NotFound(format!("table {intended_name:?} not found")))?
            .table_id;
        self.create_table_with_state(
            build_name,
            schema,
            TableState::Building {
                intended_name: intended_name.to_string(),
                query_id: query_id.to_string(),
                created_at_unix_nanos: current_unix_nanos(),
                replaces_table_id: Some(replaces_table_id),
            },
        )
    }

    fn create_table_with_state(
        &self,
        name: &str,
        schema: Schema,
        state: TableState,
    ) -> Result<u64> {
        use crate::wal::DdlOp;
        use std::sync::atomic::Ordering;

        self.require(&crate::auth::Permission::Ddl)?;
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }

        let _g = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Ddl)?;
        {
            let cat = self.catalog.read();
            match &state {
                TableState::Live => {
                    if cat.live(name).is_some() || cat.building_for(name).is_some() {
                        return Err(MongrelError::InvalidArgument(format!(
                            "table {name:?} already exists or is being built"
                        )));
                    }
                }
                TableState::Building {
                    intended_name,
                    replaces_table_id,
                    ..
                } => {
                    let target_matches = match replaces_table_id {
                        Some(table_id) => cat
                            .live(intended_name)
                            .is_some_and(|entry| entry.table_id == *table_id),
                        None => cat.live(intended_name).is_none(),
                    };
                    if !target_matches || cat.building_for(intended_name).is_some() {
                        return Err(MongrelError::InvalidArgument(format!(
                            "table {intended_name:?} changed or is already being built"
                        )));
                    }
                    if cat.building(name).is_some() {
                        return Err(MongrelError::InvalidArgument(format!(
                            "building table {name:?} already exists"
                        )));
                    }
                }
                TableState::Dropped { .. } => {
                    return Err(MongrelError::InvalidArgument(
                        "cannot create a dropped table".into(),
                    ));
                }
            }
        }

        // Allocate id + epoch + txn id under the commit lock so the DDL commit
        // is serialized with data commits (in-order publish).
        let commit_lock = Arc::clone(&self.commit_lock);
        let _c = commit_lock.lock();
        let table_id = {
            let mut cat = self.catalog.write();
            let id = cat.next_table_id;
            cat.next_table_id = id
                .checked_add(1)
                .ok_or_else(|| MongrelError::InvalidArgument("table id space exhausted".into()))?;
            Result::<u64>::Ok(id)
        }?;
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let txn_id = self.alloc_txn_id()?;

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

        // Build the complete mounted table before its DDL can become durable.
        // Any failure removes the unpublished directory and abandons the epoch.
        let table_relative = Path::new(TABLES_DIR).join(table_id.to_string());
        let canonical_tdir = self.root.join(&table_relative);
        let table_root = Arc::new(
            self.durable_root
                .create_directory_all_pinned(&table_relative)?,
        );
        let tdir = table_root.io_path()?;
        let mut pending_table_dir = PendingTableDir::new(canonical_tdir);
        let ctx = SharedCtx {
            root_guard: Some(table_root),
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

        // 1. Log the DDL + commit marker to the shared WAL, then make it durable
        //    via the group-commit coordinator (no fsync under the WAL lock — P3.2).
        let schema_json = DdlOp::encode_schema(&schema)?;
        let ddl = match &state {
            TableState::Live => DdlOp::CreateTable {
                table_id,
                name: name.to_string(),
                schema_json,
            },
            TableState::Building {
                intended_name,
                query_id,
                created_at_unix_nanos,
                replaces_table_id,
            } => match replaces_table_id {
                Some(replaces_table_id) => DdlOp::CreateRebuildingTable {
                    table_id,
                    build_name: name.to_string(),
                    intended_name: intended_name.clone(),
                    query_id: query_id.clone(),
                    created_at_unix_nanos: *created_at_unix_nanos,
                    replaces_table_id: *replaces_table_id,
                    schema_json,
                },
                None => DdlOp::CreateBuildingTable {
                    table_id,
                    build_name: name.to_string(),
                    intended_name: intended_name.clone(),
                    query_id: query_id.clone(),
                    created_at_unix_nanos: *created_at_unix_nanos,
                    schema_json,
                },
            },
            TableState::Dropped { .. } => {
                return Err(MongrelError::InvalidArgument(
                    "cannot create a table in dropped state".into(),
                ));
            }
        };
        let mut next_catalog = self.catalog.read().clone();
        next_catalog.tables.push(CatalogEntry {
            table_id,
            name: name.to_string(),
            schema: schema.clone(),
            state: state.clone(),
            created_epoch: epoch.0,
        });
        next_catalog.db_epoch = next_catalog.db_epoch.max(epoch.0);
        let commit_seq = {
            let mut wal = self.shared_wal.lock();
            let append: Result<u64> = (|| {
                wal.append(txn_id, table_id, crate::wal::Op::Ddl(ddl))?;
                append_catalog_snapshot(&mut wal, txn_id, &next_catalog)?;
                wal.append_commit(txn_id, epoch, &[])
            })();
            append.map_err(|error| self.commit_outcome_unknown(epoch, error))?
        };
        self.await_durable_commit(commit_seq, epoch)?;
        pending_table_dir.disarm();

        // Publish the mounted table and catalog in memory even if the catalog
        // checkpoint fails after the WAL commit.
        self.tables
            .write()
            .insert(table_id, TableHandle::new(table));
        let checkpoint = self.checkpoint_catalog_after_durable(next_catalog);
        self.finish_durable_publish(epoch, &mut _epoch_guard, checkpoint)?;
        Ok(table_id)
    }

    /// Logically drop a table, logging the DDL through the shared WAL first.
    pub fn drop_table(&self, name: &str) -> Result<()> {
        self.drop_table_with_epoch(name).map(|_| ())
    }

    /// Logically drop a table and return the exact publication epoch.
    pub fn drop_table_with_epoch(&self, name: &str) -> Result<Epoch> {
        self.drop_table_with_state(name, false, None)
    }

    pub fn drop_table_with_epoch_controlled<F>(
        &self,
        name: &str,
        mut before_commit: F,
    ) -> Result<Epoch>
    where
        F: FnMut() -> Result<()>,
    {
        self.drop_table_with_state(name, false, Some(&mut before_commit))
    }

    /// Discard an unpublished CTAS build.
    #[doc(hidden)]
    pub fn discard_building_table(&self, name: &str) -> Result<()> {
        if !name.starts_with(CTAS_BUILD_TABLE_PREFIX) {
            return Err(MongrelError::InvalidArgument(
                "not a CTAS building table".into(),
            ));
        }
        self.drop_table_with_state(name, true, None).map(|_| ())
    }

    fn drop_table_with_state(
        &self,
        name: &str,
        building: bool,
        before_commit: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<Epoch> {
        use crate::wal::DdlOp;
        use std::sync::atomic::Ordering;

        self.require(&crate::auth::Permission::Ddl)?;
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }

        let _g = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Ddl)?;
        let table_id = {
            let cat = self.catalog.read();
            if building {
                cat.building(name)
            } else {
                cat.live(name)
            }
            .ok_or_else(|| MongrelError::NotFound(format!("table {name:?} not found")))?
            .table_id
        };

        let commit_lock = Arc::clone(&self.commit_lock);
        let _c = commit_lock.lock();
        let epoch = self.epoch.bump_assigned();
        let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
        let txn_id = self.alloc_txn_id()?;
        let mut next_catalog = self.catalog.read().clone();
        let entry = next_catalog
            .tables
            .iter_mut()
            .find(|t| t.table_id == table_id)
            .ok_or_else(|| MongrelError::NotFound(format!("table {name:?} not found")))?;
        entry.state = TableState::Dropped { at_epoch: epoch.0 };
        next_catalog.triggers.retain(|trigger| {
            !matches!(
                &trigger.trigger.target,
                TriggerTarget::Table(target) if target == name
            )
        });
        next_catalog
            .materialized_views
            .retain(|definition| definition.name != name);
        next_catalog
            .security
            .rls_tables
            .retain(|table| table != name);
        next_catalog
            .security
            .policies
            .retain(|policy| policy.table != name);
        next_catalog
            .security
            .masks
            .retain(|mask| mask.table != name);
        for role in &mut next_catalog.roles {
            role.permissions
                .retain(|permission| permission_table(permission) != Some(name));
        }
        advance_security_version(&mut next_catalog)?;
        next_catalog.db_epoch = next_catalog.db_epoch.max(epoch.0);
        let commit_seq = {
            let mut wal = self.shared_wal.lock();
            if let Some(before_commit) = before_commit {
                before_commit()?;
            }
            let append: Result<u64> = (|| {
                wal.append(
                    txn_id,
                    table_id,
                    crate::wal::Op::Ddl(DdlOp::DropTable { table_id }),
                )?;
                append_catalog_snapshot(&mut wal, txn_id, &next_catalog)?;
                wal.append_commit(txn_id, epoch, &[])
            })();
            append.map_err(|error| self.commit_outcome_unknown(epoch, error))?
        };
        self.await_durable_commit(commit_seq, epoch)?;

        let checkpoint = self.checkpoint_catalog_after_durable(next_catalog);
        self.tables.write().remove(&table_id);
        self.finish_durable_publish(epoch, &mut _epoch_guard, checkpoint)?;
        Ok(epoch)
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
        self.rename_table_with_epoch(name, new_name).map(|_| ())
    }

    /// Rename a table and return its exact publication epoch.
    pub fn rename_table_with_epoch(&self, name: &str, new_name: &str) -> Result<Epoch> {
        self.rename_table_with_epoch_inner(name, new_name, None)
    }

    pub fn rename_table_with_epoch_controlled<F>(
        &self,
        name: &str,
        new_name: &str,
        mut before_commit: F,
    ) -> Result<Epoch>
    where
        F: FnMut() -> Result<()>,
    {
        self.rename_table_with_epoch_inner(name, new_name, Some(&mut before_commit))
    }

    fn rename_table_with_epoch_inner(
        &self,
        name: &str,
        new_name: &str,
        before_commit: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<Epoch> {
        if name.starts_with(CTAS_BUILD_TABLE_PREFIX)
            || new_name.starts_with(CTAS_BUILD_TABLE_PREFIX)
        {
            return Err(MongrelError::InvalidArgument(
                "the CTAS building-table namespace is reserved".into(),
            ));
        }
        self.rename_table_with_state(name, new_name, false, None, before_commit)
    }

    /// Atomically publish a hidden CTAS build under its intended live name.
    #[doc(hidden)]
    pub fn publish_building_table(&self, build_name: &str, new_name: &str) -> Result<Epoch> {
        self.publish_building_table_inner(build_name, new_name, None)
    }

    #[doc(hidden)]
    pub fn publish_building_table_controlled<F>(
        &self,
        build_name: &str,
        new_name: &str,
        mut before_commit: F,
    ) -> Result<Epoch>
    where
        F: FnMut() -> Result<()>,
    {
        self.publish_building_table_inner(build_name, new_name, Some(&mut before_commit))
    }

    fn publish_building_table_inner(
        &self,
        build_name: &str,
        new_name: &str,
        before_commit: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<Epoch> {
        if !build_name.starts_with(CTAS_BUILD_TABLE_PREFIX)
            || new_name.starts_with(CTAS_BUILD_TABLE_PREFIX)
        {
            return Err(MongrelError::InvalidArgument(
                "invalid CTAS publish identity".into(),
            ));
        }
        self.rename_table_with_state(build_name, new_name, true, None, before_commit)
    }

    /// Atomically publish a hidden build and its materialized-view definition.
    #[doc(hidden)]
    pub fn publish_materialized_building_table(
        &self,
        build_name: &str,
        new_name: &str,
        definition: crate::catalog::MaterializedViewEntry,
    ) -> Result<Epoch> {
        self.publish_materialized_building_table_inner(build_name, new_name, definition, None)
    }

    #[doc(hidden)]
    pub fn publish_materialized_building_table_controlled<F>(
        &self,
        build_name: &str,
        new_name: &str,
        definition: crate::catalog::MaterializedViewEntry,
        mut before_commit: F,
    ) -> Result<Epoch>
    where
        F: FnMut() -> Result<()>,
    {
        self.publish_materialized_building_table_inner(
            build_name,
            new_name,
            definition,
            Some(&mut before_commit),
        )
    }

    fn publish_materialized_building_table_inner(
        &self,
        build_name: &str,
        new_name: &str,
        definition: crate::catalog::MaterializedViewEntry,
        before_commit: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<Epoch> {
        if definition.name != new_name || definition.query.trim().is_empty() {
            return Err(MongrelError::InvalidArgument(
                "invalid materialized-view publication".into(),
            ));
        }
        self.rename_table_with_state(build_name, new_name, true, Some(definition), before_commit)
    }

    /// Atomically replace a still-live table with its completed hidden rebuild.
    #[doc(hidden)]
    pub fn publish_rebuilding_table(&self, build_name: &str, new_name: &str) -> Result<Epoch> {
        self.publish_rebuilding_table_inner(build_name, new_name, None, None)
    }

    #[doc(hidden)]
    pub fn publish_rebuilding_table_controlled<F>(
        &self,
        build_name: &str,
        new_name: &str,
        mut before_commit: F,
    ) -> Result<Epoch>
    where
        F: FnMut() -> Result<()>,
    {
        self.publish_rebuilding_table_inner(build_name, new_name, None, Some(&mut before_commit))
    }

    /// Atomically replace a live materialized-view table and its definition.
    #[doc(hidden)]
    pub fn publish_materialized_rebuilding_table_controlled<F>(
        &self,
        build_name: &str,
        new_name: &str,
        definition: crate::catalog::MaterializedViewEntry,
        mut before_commit: F,
    ) -> Result<Epoch>
    where
        F: FnMut() -> Result<()>,
    {
        self.publish_rebuilding_table_inner(
            build_name,
            new_name,
            Some(definition),
            Some(&mut before_commit),
        )
    }

    fn publish_rebuilding_table_inner(
        &self,
        build_name: &str,
        new_name: &str,
        mut materialized_view: Option<crate::catalog::MaterializedViewEntry>,
        before_commit: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<Epoch> {
        use crate::wal::DdlOp;

        if !build_name.starts_with(CTAS_BUILD_TABLE_PREFIX)
            || new_name.is_empty()
            || new_name.starts_with(CTAS_BUILD_TABLE_PREFIX)
        {
            return Err(MongrelError::InvalidArgument(
                "invalid rebuilding-table publish identity".into(),
            ));
        }
        if materialized_view.as_ref().is_some_and(|definition| {
            definition.name != new_name || definition.query.trim().is_empty()
        }) {
            return Err(MongrelError::InvalidArgument(
                "invalid materialized-view replacement".into(),
            ));
        }
        self.require(&crate::auth::Permission::Ddl)?;
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }

        let _ddl = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        let (table_id, replaced_table_id) = {
            let catalog = self.catalog.read();
            let build = catalog.building(build_name).ok_or_else(|| {
                MongrelError::NotFound(format!("building table {build_name:?} not found"))
            })?;
            let replaced_table_id = match &build.state {
                TableState::Building {
                    intended_name,
                    replaces_table_id: Some(replaced_table_id),
                    ..
                } if intended_name == new_name => *replaced_table_id,
                _ => {
                    return Err(MongrelError::InvalidArgument(format!(
                        "building table {build_name:?} is not a replacement for {new_name:?}"
                    )))
                }
            };
            if catalog
                .live(new_name)
                .is_none_or(|entry| entry.table_id != replaced_table_id)
            {
                return Err(MongrelError::Conflict(format!(
                    "table {new_name:?} changed while its replacement was built"
                )));
            }
            (build.table_id, replaced_table_id)
        };

        let _commit = self.commit_lock.lock();
        let epoch = self.epoch.assigned().next();
        let txn_id = self.alloc_txn_id()?;
        let mut next_catalog = self.catalog.read().clone();
        apply_rebuilding_publish(
            &mut next_catalog,
            table_id,
            replaced_table_id,
            new_name,
            epoch.0,
        )?;
        if let Some(definition) = materialized_view.as_mut() {
            definition.last_refresh_epoch = epoch.0;
        }
        let materialized_view_json = materialized_view
            .as_ref()
            .map(DdlOp::encode_materialized_view)
            .transpose()?;
        if let Some(definition) = materialized_view {
            if let Some(existing) = next_catalog
                .materialized_views
                .iter_mut()
                .find(|existing| existing.name == definition.name)
            {
                *existing = definition;
            } else {
                next_catalog.materialized_views.push(definition);
            }
        }
        next_catalog.db_epoch = next_catalog.db_epoch.max(epoch.0);
        if let Some(before_commit) = before_commit {
            before_commit()?;
        }
        let assigned_epoch = self.epoch.bump_assigned();
        let mut epoch_guard = EpochGuard::new(self.epoch.as_ref(), assigned_epoch);
        if assigned_epoch != epoch {
            return Err(MongrelError::Conflict(
                "commit epoch changed while sequencer lock was held".into(),
            ));
        }
        let commit_seq = {
            let mut wal = self.shared_wal.lock();
            let append: Result<u64> = (|| {
                wal.append(
                    txn_id,
                    table_id,
                    crate::wal::Op::Ddl(DdlOp::ReplaceBuildingTable {
                        table_id,
                        replaced_table_id,
                        new_name: new_name.to_string(),
                    }),
                )?;
                if let Some(definition_json) = materialized_view_json {
                    wal.append(
                        txn_id,
                        table_id,
                        crate::wal::Op::Ddl(DdlOp::SetMaterializedView {
                            name: new_name.to_string(),
                            definition_json,
                        }),
                    )?;
                }
                append_catalog_snapshot(&mut wal, txn_id, &next_catalog)?;
                wal.append_commit(txn_id, epoch, &[])
            })();
            append.map_err(|error| self.commit_outcome_unknown(epoch, error))?
        };
        self.await_durable_commit(commit_seq, epoch)?;

        let checkpoint = self.checkpoint_catalog_after_durable(next_catalog);
        self.tables.write().remove(&replaced_table_id);
        if let Some(table) = self.tables.read().get(&table_id) {
            table.lock().set_catalog_name(new_name.to_string());
        }
        self.finish_durable_publish(epoch, &mut epoch_guard, checkpoint)?;
        Ok(epoch)
    }

    fn rename_table_with_state(
        &self,
        name: &str,
        new_name: &str,
        building: bool,
        mut materialized_view: Option<crate::catalog::MaterializedViewEntry>,
        before_commit: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<Epoch> {
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
            return Ok(self.visible_epoch());
        }
        if new_name.is_empty() {
            return Err(MongrelError::InvalidArgument(
                "rename_table: new name must not be empty".into(),
            ));
        }

        let _g = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Ddl)?;
        let table_id = {
            let cat = self.catalog.read();
            let src = if building {
                cat.building(name)
            } else {
                cat.live(name)
            }
            .ok_or_else(|| MongrelError::NotFound(format!("table {name:?} not found")))?;
            if building
                && !matches!(
                    &src.state,
                    TableState::Building { intended_name, .. } if intended_name == new_name
                )
            {
                return Err(MongrelError::InvalidArgument(format!(
                    "building table {name:?} is not reserved for {new_name:?}"
                )));
            }
            // Target must be free. Checked under ddl_lock, which every other
            // DDL (create/rename/drop) also holds, so a concurrent operation
            // cannot claim `new_name` between this check and the catalog write.
            if cat.live(new_name).is_some() || (!building && cat.building_for(new_name).is_some()) {
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
        let txn_id = self.alloc_txn_id()?;
        if let Some(definition) = materialized_view.as_mut() {
            definition.last_refresh_epoch = epoch.0;
        }
        let materialized_view_json = materialized_view
            .as_ref()
            .map(DdlOp::encode_materialized_view)
            .transpose()?;
        let mut next_catalog = self.catalog.read().clone();
        let entry = next_catalog
            .tables
            .iter_mut()
            .find(|t| t.table_id == table_id)
            .ok_or_else(|| MongrelError::NotFound(format!("table {name:?} not found")))?;
        entry.name = new_name.to_string();
        if building {
            entry.state = TableState::Live;
        }
        for trigger in &mut next_catalog.triggers {
            if matches!(
                &trigger.trigger.target,
                TriggerTarget::Table(target) if target == name
            ) {
                trigger.trigger = trigger.trigger.retarget_table(new_name, epoch.0)?;
            }
        }
        if let Some(definition) = next_catalog
            .materialized_views
            .iter_mut()
            .find(|definition| definition.name == name)
        {
            definition.name = new_name.to_string();
        }
        if let Some(definition) = materialized_view.take() {
            next_catalog.materialized_views.push(definition);
        }
        next_catalog.db_epoch = next_catalog.db_epoch.max(epoch.0);
        for table in &mut next_catalog.security.rls_tables {
            if table == name {
                *table = new_name.to_string();
            }
        }
        for policy in &mut next_catalog.security.policies {
            if policy.table == name {
                policy.table = new_name.to_string();
            }
        }
        for mask in &mut next_catalog.security.masks {
            if mask.table == name {
                mask.table = new_name.to_string();
            }
        }
        for role in &mut next_catalog.roles {
            for permission in &mut role.permissions {
                rename_permission_table(permission, name, new_name);
            }
        }
        advance_security_version(&mut next_catalog)?;
        let ddl = if building {
            DdlOp::PublishBuildingTable {
                table_id,
                new_name: new_name.to_string(),
            }
        } else {
            DdlOp::RenameTable {
                table_id,
                new_name: new_name.to_string(),
            }
        };
        let commit_seq = {
            let mut wal = self.shared_wal.lock();
            if let Some(before_commit) = before_commit {
                before_commit()?;
            }
            let append: Result<u64> = (|| {
                wal.append(txn_id, table_id, crate::wal::Op::Ddl(ddl))?;
                if let Some(definition_json) = materialized_view_json {
                    wal.append(
                        txn_id,
                        table_id,
                        crate::wal::Op::Ddl(DdlOp::SetMaterializedView {
                            name: new_name.to_string(),
                            definition_json,
                        }),
                    )?;
                }
                append_catalog_snapshot(&mut wal, txn_id, &next_catalog)?;
                wal.append_commit(txn_id, epoch, &[])
            })();
            append.map_err(|error| self.commit_outcome_unknown(epoch, error))?
        };
        self.await_durable_commit(commit_seq, epoch)?;

        let checkpoint = self.checkpoint_catalog_after_durable(next_catalog);
        // The in-memory table object is keyed by table_id, not name, so it does
        // not move and live TableHandles remain valid.
        if let Some(table) = self.tables.read().get(&table_id) {
            table.lock().set_catalog_name(new_name.to_string());
        }
        self.finish_durable_publish(epoch, &mut _epoch_guard, checkpoint)?;
        Ok(epoch)
    }

    pub fn alter_column(
        &self,
        table_name: &str,
        column_name: &str,
        change: AlterColumn,
    ) -> Result<ColumnDef> {
        self.alter_column_with_epoch(table_name, column_name, change)
            .map(|(column, _)| column)
    }

    pub fn alter_column_with_epoch(
        &self,
        table_name: &str,
        column_name: &str,
        change: AlterColumn,
    ) -> Result<(ColumnDef, Option<Epoch>)> {
        self.alter_column_with_epoch_inner(table_name, column_name, change, None, None, None)
    }

    /// Cooperatively prepare an ALTER and fence each durable commit separately.
    /// `after_commit(Some(epoch))` follows an exact durable outcome;
    /// `after_commit(None)` follows an uncertain WAL attempt. It is called once
    /// for every successful `before_commit` callback.
    pub fn alter_column_with_epoch_controlled<B, A>(
        &self,
        table_name: &str,
        column_name: &str,
        change: AlterColumn,
        control: &crate::ExecutionControl,
        mut before_commit: B,
        mut after_commit: A,
    ) -> Result<(ColumnDef, Option<Epoch>)>
    where
        B: FnMut() -> Result<()>,
        A: FnMut(Option<Epoch>) -> Result<()>,
    {
        self.alter_column_with_epoch_inner(
            table_name,
            column_name,
            change,
            Some(control),
            Some(&mut before_commit),
            Some(&mut after_commit),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn alter_column_with_epoch_inner(
        &self,
        table_name: &str,
        column_name: &str,
        change: AlterColumn,
        control: Option<&crate::ExecutionControl>,
        mut before_commit: Option<&mut dyn FnMut() -> Result<()>>,
        mut after_commit: Option<&mut dyn FnMut(Option<Epoch>) -> Result<()>>,
    ) -> Result<(ColumnDef, Option<Epoch>)> {
        use crate::wal::DdlOp;
        use std::sync::atomic::Ordering;

        self.require(&crate::auth::Permission::Ddl)?;
        commit_prepare_checkpoint(control, 0)?;
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
                let rows = match control {
                    Some(control) => table.visible_rows_controlled(snapshot, control)?,
                    None => table.visible_rows(snapshot)?,
                };
                for (row_index, row) in rows.into_iter().enumerate() {
                    commit_prepare_checkpoint(control, row_index)?;
                    if row
                        .columns
                        .get(&old.id)
                        .is_some_and(|value| !matches!(value, Value::Null))
                    {
                        continue;
                    }
                    let mut cells: Vec<(u16, Value)> = row.columns.into_iter().collect();
                    table.apply_defaults(&mut cells)?;
                    updates.push((
                        table_id,
                        crate::txn::Staged::Update {
                            row_id: row.row_id,
                            new_row: cells,
                            changed_columns: vec![old.id],
                        },
                    ));
                }
                updates
            } else {
                Vec::new()
            }
        };
        let durable_epoch = std::cell::Cell::new(None);
        let backfill_epoch = if backfill.is_empty() {
            None
        } else {
            let (principal, catalog_bound) = self.transaction_principal_snapshot();
            let txn_id = self.alloc_txn_id()?;
            let mut entered_fence = false;
            let commit_result = match (control, before_commit.as_deref_mut()) {
                (Some(control), Some(before_commit)) => self
                    .commit_transaction_with_external_states_controlled(
                        txn_id,
                        self.epoch.visible(),
                        backfill,
                        Vec::new(),
                        Vec::new(),
                        principal.clone(),
                        catalog_bound,
                        None,
                        control,
                        &mut || {
                            before_commit()?;
                            entered_fence = true;
                            Ok(())
                        },
                    )
                    .map(|(epoch, _)| epoch),
                _ => self
                    .commit_transaction_with_external_states(
                        txn_id,
                        self.epoch.visible(),
                        backfill,
                        Vec::new(),
                        Vec::new(),
                        principal,
                        catalog_bound,
                        None,
                    )
                    .map(|(epoch, _)| epoch),
            };
            let commit_result = if entered_fence {
                finish_controlled_commit_attempt(commit_result, &mut after_commit)
            } else {
                commit_result
            };
            match &commit_result {
                Ok(epoch) => durable_epoch.set(Some(*epoch)),
                Err(MongrelError::DurableCommit { epoch, .. }) => {
                    durable_epoch.set(Some(Epoch(*epoch)));
                }
                Err(_) => {}
            }
            Some(commit_result?)
        };
        let result: Result<(ColumnDef, Option<Epoch>)> = (|| {
            let _security_write = self.security_write()?;
            self.require(&crate::auth::Permission::Ddl)?;
            if self
                .catalog
                .read()
                .live(table_name)
                .is_none_or(|entry| entry.table_id != table_id)
            {
                return Err(MongrelError::Conflict(format!(
                    "table {table_name:?} changed during ALTER"
                )));
            }
            let mut table = handle.lock();
            let (column, prepared_schema) = table.prepare_alter_column(column_name, &change)?;
            let renamed_column = (column.name != column_name).then(|| column.name.clone());
            let Some(prepared_schema) = prepared_schema else {
                return Ok((column, backfill_epoch));
            };

            let commit_lock = Arc::clone(&self.commit_lock);
            let _c = commit_lock.lock();
            let epoch = self.epoch.bump_assigned();
            let mut _epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
            let txn_id = self.alloc_txn_id()?;
            let column_json = DdlOp::encode_column(&column)?;
            let mut next_catalog = self.catalog.read().clone();
            let catalog_entry_index = next_catalog
                .tables
                .iter()
                .position(|entry| entry.table_id == table_id)
                .ok_or_else(|| MongrelError::NotFound(format!("table {table_name:?} not found")))?;
            if let Some(new_column_name) = &renamed_column {
                for (trigger_index, trigger) in next_catalog.triggers.iter_mut().enumerate() {
                    commit_prepare_checkpoint(control, trigger_index)?;
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
                for (role_index, role) in next_catalog.roles.iter_mut().enumerate() {
                    commit_prepare_checkpoint(control, role_index)?;
                    for (permission_index, permission) in role.permissions.iter_mut().enumerate() {
                        commit_prepare_checkpoint(control, permission_index)?;
                        rename_permission_column(
                            permission,
                            table_name,
                            column_name,
                            new_column_name,
                        );
                    }
                }
                advance_security_version(&mut next_catalog)?;
            }
            next_catalog.tables[catalog_entry_index].schema = prepared_schema.clone();
            next_catalog.db_epoch = next_catalog.db_epoch.max(epoch.0);
            commit_prepare_checkpoint(control, 0)?;
            let mut entered_fence = false;
            if let Some(before_commit) = before_commit.as_deref_mut() {
                before_commit()?;
                entered_fence = true;
            }
            let commit_result: Result<Epoch> = (|| {
                let commit_seq = {
                    let mut wal = self.shared_wal.lock();
                    let append: Result<u64> = (|| {
                        wal.append(
                            txn_id,
                            table_id,
                            crate::wal::Op::Ddl(DdlOp::AlterTable {
                                table_id,
                                column_json,
                            }),
                        )?;
                        append_catalog_snapshot(&mut wal, txn_id, &next_catalog)?;
                        wal.append_commit(txn_id, epoch, &[])
                    })();
                    append.map_err(|error| self.commit_outcome_unknown(epoch, error))?
                };
                self.await_durable_commit(commit_seq, epoch)?;
                durable_epoch.set(Some(epoch));

                table.apply_altered_schema_prepared(prepared_schema);
                let schema = table.schema().clone();
                let table_checkpoint = table.checkpoint_altered_schema();
                drop(table);
                next_catalog.tables[catalog_entry_index].schema = schema;
                next_catalog.db_epoch = next_catalog.db_epoch.max(epoch.0);
                let catalog_result =
                    catalog::write_atomic(&self.root, &next_catalog, self.meta_dek.as_ref());
                let security_version = next_catalog.security_version;
                *self.catalog.write() = next_catalog;
                if renamed_column.is_some() {
                    self.security_coordinator
                        .version
                        .store(security_version, Ordering::Release);
                }
                self.epoch.publish_in_order(epoch);
                _epoch_guard.disarm();
                if let Err(error) = table_checkpoint.and(catalog_result) {
                    self.poisoned.store(true, Ordering::Relaxed);
                    return Err(MongrelError::DurableCommit {
                        epoch: epoch.0,
                        message: error.to_string(),
                    });
                }
                Ok(epoch)
            })();
            let commit_result = if entered_fence {
                finish_controlled_commit_attempt(commit_result, &mut after_commit)
            } else {
                commit_result
            };
            let epoch = commit_result?;
            Ok((column, Some(epoch)))
        })();
        result.map_err(|error| match (durable_epoch.get(), error) {
            (_, error @ MongrelError::DurableCommit { .. }) => error,
            (Some(epoch), error) => MongrelError::DurableCommit {
                epoch: epoch.0,
                message: error.to_string(),
            },
            (None, error) => error,
        })
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
        policy.ok_or_else(|| MongrelError::Other("set TTL produced no policy".into()))
    }

    /// Set TTL metadata on a hidden build before it is published.
    #[doc(hidden)]
    pub fn set_building_table_ttl(
        &self,
        table_name: &str,
        column_name: &str,
        duration_nanos: u64,
    ) -> Result<crate::manifest::TtlPolicy> {
        let policy = self.replace_table_ttl_with_state(
            table_name,
            Some((column_name, duration_nanos)),
            true,
        )?;
        policy
            .ok_or_else(|| MongrelError::Other("set building-table TTL produced no policy".into()))
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
        self.replace_table_ttl_with_state(table_name, requested, false)
    }

    fn replace_table_ttl_with_state(
        &self,
        table_name: &str,
        requested: Option<(&str, u64)>,
        building: bool,
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
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Ddl)?;
        let table_id = {
            let cat = self.catalog.read();
            if building {
                cat.building(table_name)
            } else {
                cat.live(table_name)
            }
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
        let txn_id = self.alloc_txn_id()?;
        let policy_json = DdlOp::encode_ttl(policy)?;
        let mut next_catalog = self.catalog.read().clone();
        next_catalog.db_epoch = next_catalog.db_epoch.max(epoch.0);
        let commit_seq = {
            let mut wal = self.shared_wal.lock();
            let append: Result<u64> = (|| {
                wal.append(
                    txn_id,
                    table_id,
                    crate::wal::Op::Ddl(DdlOp::SetTtl {
                        table_id,
                        policy_json,
                    }),
                )?;
                append_catalog_snapshot(&mut wal, txn_id, &next_catalog)?;
                wal.append_commit(txn_id, epoch, &[])
            })();
            append.map_err(|error| self.commit_outcome_unknown(epoch, error))?
        };
        self.await_durable_commit(commit_seq, epoch)?;

        let mut publish_error = table.apply_ttl_policy_at(policy, epoch).err();
        drop(table);
        if let Err(error) = self.checkpoint_catalog_after_durable(next_catalog) {
            publish_error.get_or_insert(error);
        }
        self.finish_durable_publish(epoch, &mut _epoch_guard, publish_error.map_or(Ok(()), Err))?;
        Ok(policy)
    }

    /// Retention-gated garbage collection (spec §6.4, §7.4, §16). Deletes:
    /// - Dropped-table subdirs whose `at_epoch < min_active_snapshot`.
    /// - Stale `_txn/` dirs (aborted/crashed large-txn pending runs).
    ///
    /// Returns the number of items reclaimed.
    pub fn gc(&self) -> Result<usize> {
        let control = crate::ExecutionControl::new(None);
        self.gc_controlled(&control, || true)
    }

    /// Discover reclaimable state cooperatively, then cross one publication
    /// boundary immediately before the first irreversible deletion.
    #[doc(hidden)]
    pub fn gc_controlled<F>(
        &self,
        control: &crate::ExecutionControl,
        before_publish: F,
    ) -> Result<usize>
    where
        F: FnOnce() -> bool,
    {
        self.gc_controlled_with_receipt(control, before_publish)
            .map(|(reclaimed, _)| reclaimed)
    }

    /// Discover reclaimable state from one exact catalog/epoch snapshot, then
    /// return that snapshot if an irreversible deletion was attempted.
    #[doc(hidden)]
    pub fn gc_controlled_with_receipt<F>(
        &self,
        control: &crate::ExecutionControl,
        before_publish: F,
    ) -> Result<(usize, Option<MaintenanceReceipt>)>
    where
        F: FnOnce() -> bool,
    {
        enum Candidate {
            Directory(PathBuf),
            File(PathBuf),
        }

        self.require(&crate::auth::Permission::Ddl)?;
        let _ddl = self.ddl_lock.lock();
        self.require(&crate::auth::Permission::Ddl)?;
        control.checkpoint()?;
        let maintenance_epoch = self.epoch.visible();
        let min_active = self.snapshots.min_active(maintenance_epoch).0;
        let mut candidates = Vec::new();

        // Reclaim dropped-table dirs where no pinned snapshot still needs them.
        let cat = self.catalog.read();
        for (entry_index, entry) in cat.tables.iter().enumerate() {
            if entry_index % 256 == 0 {
                control.checkpoint()?;
            }
            if let TableState::Dropped { at_epoch } = entry.state {
                if at_epoch <= min_active {
                    let tdir = self.root.join(TABLES_DIR).join(entry.table_id.to_string());
                    if tdir.exists() {
                        candidates.push(Candidate::Directory(tdir));
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
        for (entry_index, entry) in cat.tables.iter().enumerate() {
            if entry_index % 256 == 0 {
                control.checkpoint()?;
            }
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
            for (sub_index, sub) in std::fs::read_dir(&txn_dir)?.enumerate() {
                if sub_index % 256 == 0 {
                    control.checkpoint()?;
                }
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
                candidates.push(Candidate::Directory(sub.path()));
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
            for (entry_index, entry) in std::fs::read_dir(&vtab_dir)?.enumerate() {
                if entry_index % 256 == 0 {
                    control.checkpoint()?;
                }
                let entry = entry?;
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                if external_names.contains(name) {
                    continue;
                }
                let path = entry.path();
                if path.is_dir() {
                    candidates.push(Candidate::Directory(path));
                } else {
                    candidates.push(Candidate::File(path));
                }
            }
        }

        // Reap compaction-superseded runs whose retire epoch no pinned snapshot
        // can still need (spec §6.4). Each table deletes its own retired files
        // gated on `min_active` and persists its manifest.
        let tables = self
            .tables
            .read()
            .iter()
            .map(|(table_id, handle)| (*table_id, handle.clone()))
            .collect::<Vec<_>>();
        let mut retiring = Vec::new();
        for (table_index, (table_id, handle)) in tables.iter().enumerate() {
            if table_index % 256 == 0 {
                control.checkpoint()?;
            }
            let backup_pinned: HashSet<u128> = self
                .backup_pins
                .lock()
                .keys()
                .filter_map(|(pinned_table, run_id)| {
                    (*pinned_table == *table_id).then_some(*run_id)
                })
                .collect();
            if handle
                .lock()
                .has_reapable_retiring(Epoch(min_active), &backup_pinned)
            {
                retiring.push((handle.clone(), backup_pinned));
            }
        }

        // WAL-segment GC (spec §6.4/§16). `SharedWal::open` mints a fresh active
        // segment on every reopen without truncating the prior ones, so rotated
        // segments accumulate. Once every live table's committed data is durable
        // in runs (no in-memory rows) and no in-flight spill is open, all rotated
        // (non-active) segments are redundant for recovery and safe to delete —
        // an in-flight txn only ever appends to the active segment, which is
        // never deleted.
        let all_durable = self.active_spills.is_idle()
            && tables.iter().all(|(_, handle)| {
                let g = handle.lock();
                g.memtable_len() == 0 && g.mutable_run_len() == 0
            });
        let retain = self
            .replication_wal_retention_segments
            .load(std::sync::atomic::Ordering::Relaxed);
        let reap_wal = all_durable
            && self
                .shared_wal
                .lock()
                .has_gc_segments_retain_recent(retain)?;

        if candidates.is_empty() && retiring.is_empty() && !reap_wal {
            return Ok((0, None));
        }
        control.checkpoint()?;
        if !before_publish() {
            return Err(MongrelError::Cancelled);
        }

        let mut reclaimed = 0;
        for candidate in candidates {
            match candidate {
                Candidate::Directory(path) => std::fs::remove_dir_all(path)?,
                Candidate::File(path) => std::fs::remove_file(path)?,
            }
            reclaimed += 1;
        }
        for (handle, backup_pinned) in retiring {
            reclaimed += handle
                .lock()
                .reap_retiring(Epoch(min_active), &backup_pinned)?;
        }
        if reap_wal {
            reclaimed += self
                .shared_wal
                .lock()
                .gc_segments_retain_recent(u64::MAX, retain)?;
        }

        Ok((
            reclaimed,
            Some(MaintenanceReceipt {
                epoch: maintenance_epoch,
            }),
        ))
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
        self.checkpoint_controlled(|| Ok(()))
    }

    /// Strict checkpoint with a deterministic test hook after every table is
    /// flushed/compacted but before WAL replacement.
    #[doc(hidden)]
    pub fn checkpoint_controlled<F>(&self, before_wal_reset: F) -> Result<()>
    where
        F: FnOnce() -> Result<()>,
    {
        self.require(&crate::auth::Permission::Ddl)?;
        // Block cross-table commits and DDL for the full operation. Locking all
        // mounted handles also excludes direct `Table` commits, which do not
        // enter the database replication barrier.
        let _replication = self.replication_barrier.write();
        let _ddl = self.ddl_lock.lock();
        let _security = self.security_coordinator.gate.read();
        self.require(&crate::auth::Permission::Ddl)?;

        let mut handles = self
            .tables
            .read()
            .iter()
            .map(|(table_id, handle)| (*table_id, handle.clone()))
            .collect::<Vec<_>>();
        handles.sort_by_key(|(table_id, _)| *table_id);
        let mut tables = handles
            .iter()
            .map(|(table_id, handle)| (*table_id, handle.lock()))
            .collect::<Vec<_>>();

        // Strict flush. Any error leaves the old WAL recovery source intact.
        for (_, table) in &mut tables {
            if table.has_pending_writes() || table.memtable_len() > 0 || table.mutable_run_len() > 0
            {
                table.force_flush()?;
            }
        }

        // Strict compaction. Checkpoint never reports a stable image after a
        // skipped failure.
        for (_, table) in &mut tables {
            if table.run_count() >= 2 || table.should_compact() {
                table.compact()?;
            }
        }

        before_wal_reset()?;

        // Reap table-local retired runs while every table remains quiesced.
        let maintenance_epoch = self.epoch.visible();
        let min_active = self.snapshots.min_active(maintenance_epoch);
        for (table_id, table) in &mut tables {
            let backup_pinned: HashSet<u128> = self
                .backup_pins
                .lock()
                .keys()
                .filter_map(|(pinned_table, run_id)| {
                    (*pinned_table == *table_id).then_some(*run_id)
                })
                .collect();
            table.reap_retiring(min_active, &backup_pinned)?;
        }

        // Publish a fresh synced active WAL, then durably reap every older
        // segment. This point is reached only after every strict flush succeeds.
        self.shared_wal.lock().reset_after_checkpoint()?;

        // Remove catalog-unreachable directories and stale transaction state.
        let catalog_snapshot = self.catalog.read().clone();
        for entry in &catalog_snapshot.tables {
            if matches!(entry.state, TableState::Dropped { at_epoch } if at_epoch <= min_active.0) {
                crate::durable_file::remove_directory_all(
                    &self.root.join(TABLES_DIR).join(entry.table_id.to_string()),
                )?;
            }
            if !matches!(entry.state, TableState::Live) {
                continue;
            }
            let transaction_dir = self
                .root
                .join(TABLES_DIR)
                .join(entry.table_id.to_string())
                .join("_txn");
            if transaction_dir.is_dir() {
                for child in std::fs::read_dir(&transaction_dir)? {
                    let child = child?;
                    let active = child
                        .file_name()
                        .to_str()
                        .and_then(|name| name.parse::<u64>().ok())
                        .is_some_and(|txn_id| self.active_spills.is_active(txn_id));
                    if !active {
                        crate::durable_file::remove_directory_all(&child.path())?;
                    }
                }
            }
        }
        let external_names = catalog_snapshot
            .external_tables
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<HashSet<_>>();
        let external_root = self.root.join(VTAB_DIR);
        if external_root.is_dir() {
            for entry in std::fs::read_dir(&external_root)? {
                let entry = entry?;
                let name = entry.file_name();
                if name
                    .to_str()
                    .is_some_and(|name| external_names.contains(name))
                {
                    continue;
                }
                if entry.file_type()?.is_dir() {
                    crate::durable_file::remove_directory_all(&entry.path())?;
                } else {
                    std::fs::remove_file(entry.path())?;
                    crate::durable_file::sync_directory(&external_root)?;
                }
            }
        }

        // Final authoritative metadata checkpoint while all writers remain
        // excluded.
        catalog::write_atomic(&self.root, &catalog_snapshot, self.meta_dek.as_ref())?;
        let visible = self.epoch.visible();
        for (_, table) in &tables {
            table.persist_manifest(visible)?;
        }

        Ok(())
    }
    fn alloc_txn_id(&self) -> Result<u64> {
        self.ensure_owner_process()?;
        crate::txn::allocate_txn_id(&self.next_txn_id)
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

    /// Test-only: install a hook invoked while a spilled commit holds the
    /// security read gate and before it appends to the WAL.
    #[doc(hidden)]
    pub fn __set_security_commit_hook(&self, f: impl Fn() + Send + Sync + 'static) {
        *self.security_commit_hook.lock() = Some(Box::new(f));
    }

    /// Test-only: install a hook after transaction preparation and before the
    /// commit sequencer validates catalog generations.
    #[doc(hidden)]
    pub fn __set_catalog_commit_hook(&self, f: impl Fn() + Send + Sync + 'static) {
        *self.catalog_commit_hook.lock() = Some(Box::new(f));
    }

    /// Test-only: pause an online backup after its consistent boundary is
    /// captured but before the pinned immutable runs are copied.
    #[doc(hidden)]
    pub fn __set_backup_hook(&self, f: impl Fn() + Send + Sync + 'static) {
        *self.backup_hook.lock() = Some(Box::new(f));
    }

    /// Test-only: pause WAL extraction before its final principal recheck.
    #[doc(hidden)]
    pub fn __set_replication_hook(&self, f: impl Fn() + Send + Sync + 'static) {
        *self.replication_hook.lock() = Some(Box::new(f));
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
        match self.check_inner(None) {
            Ok(issues) => issues,
            Err(error) => vec![CheckIssue {
                table_id: WAL_TABLE_ID,
                table_name: "shared WAL".into(),
                severity: "error".into(),
                description: error.to_string(),
            }],
        }
    }

    /// Integrity check with cooperative cancellation between tables and runs.
    #[doc(hidden)]
    pub fn check_controlled(&self, control: &crate::ExecutionControl) -> Result<Vec<CheckIssue>> {
        self.check_inner(Some(control))
    }

    fn check_inner(&self, control: Option<&crate::ExecutionControl>) -> Result<Vec<CheckIssue>> {
        let mut issues = Vec::new();
        let cat = self.catalog.read();
        let manifest_meta_dek = crate::encryption::meta_dek_for(self.kek.as_deref());
        for (table_index, entry) in cat.tables.iter().enumerate() {
            if table_index % 256 == 0 {
                if let Some(control) = control {
                    control.checkpoint()?;
                }
            }
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
            for (run_index, rr) in m.runs.iter().enumerate() {
                if run_index % 256 == 0 {
                    if let Some(control) = control {
                        control.checkpoint()?;
                    }
                }
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
                for (entry_index, ent) in rd.flatten().enumerate() {
                    if entry_index % 256 == 0 {
                        if let Some(control) = control {
                            control.checkpoint()?;
                        }
                    }
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
            for (entry_index, entry) in entries.flatten().enumerate() {
                if entry_index % 256 == 0 {
                    if let Some(control) = control {
                        control.checkpoint()?;
                    }
                }
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
        if let Some(control) = control {
            control.checkpoint()?;
        }
        for (seg, msg) in self.shared_wal.lock().verify_segments() {
            issues.push(CheckIssue {
                table_id: WAL_TABLE_ID,
                table_name: "<wal>".into(),
                severity: "error".into(),
                description: format!("WAL segment seg-{seg:06}.wal failed integrity check: {msg}"),
            });
        }
        Ok(issues)
    }

    /// Quarantine unreadable tables (spec §16). Moves corrupt table dirs to
    /// `_quarantine/<table_id>/`, marks them dropped in the catalog, and
    /// unmounts them from the live table map so the DB still opens.
    pub fn doctor(&self) -> Result<Vec<u64>> {
        let control = crate::ExecutionControl::new(None);
        self.doctor_controlled(&control, || true)
    }

    /// Check cancellably, then fence immediately before the first quarantine
    /// mutation. Returning `false` from `before_publish` leaves the database
    /// untouched.
    #[doc(hidden)]
    pub fn doctor_controlled<F>(
        &self,
        control: &crate::ExecutionControl,
        before_publish: F,
    ) -> Result<Vec<u64>>
    where
        F: FnOnce() -> bool,
    {
        self.doctor_controlled_with_receipt(control, before_publish)
            .map(|(quarantined, _)| quarantined)
    }

    /// Check cancellably and return the exact catalog epoch used for a
    /// quarantine publication. No receipt is returned when nothing changes.
    #[doc(hidden)]
    pub fn doctor_controlled_with_receipt<F>(
        &self,
        control: &crate::ExecutionControl,
        before_publish: F,
    ) -> Result<(Vec<u64>, Option<MaintenanceReceipt>)>
    where
        F: FnOnce() -> bool,
    {
        // Hold the DDL lock for the whole operation to prevent concurrent
        // create_table/drop_table from racing the catalog/dir mutation.
        let _ddl = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        let issues = self.check_inner(Some(control))?;
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
            return Ok((Vec::new(), None));
        }
        let _commit = self.commit_lock.lock();
        control.checkpoint()?;
        if !before_publish() {
            return Err(MongrelError::Cancelled);
        }
        let maintenance_epoch = self.epoch.bump_assigned();
        let mut epoch_guard = EpochGuard::new(self.epoch.as_ref(), maintenance_epoch);

        let qdir = self.root.join("_quarantine");
        crate::durable_file::create_directory(&qdir)?;
        let mut bad_tables = bad_tables.into_iter().collect::<Vec<_>>();
        bad_tables.sort_unstable();

        // Quiesce every mounted target before catalog publication. Existing
        // handle clones are marked unavailable in the publication callback so
        // they cannot append to the shared WAL after their catalog entry drops.
        let mut handles = self
            .tables
            .read()
            .iter()
            .filter(|(table_id, _)| bad_tables.binary_search(table_id).is_ok())
            .map(|(table_id, handle)| (*table_id, handle.clone()))
            .collect::<Vec<_>>();
        handles.sort_by_key(|(table_id, _)| *table_id);
        let mut table_guards = handles
            .iter()
            .map(|(table_id, handle)| (*table_id, handle.lock()))
            .collect::<Vec<_>>();

        let mut next_catalog = self.catalog.read().clone();
        for table_id in &bad_tables {
            if let Some(entry) = next_catalog
                .tables
                .iter_mut()
                .find(|entry| entry.table_id == *table_id)
            {
                entry.state = TableState::Dropped {
                    at_epoch: maintenance_epoch.0,
                };
            }
        }
        next_catalog.db_epoch = next_catalog.db_epoch.max(maintenance_epoch.0);

        let txn_id = self.alloc_txn_id()?;
        let commit_seq = {
            let mut wal = self.shared_wal.lock();
            let append: Result<u64> = (|| {
                for table_id in &bad_tables {
                    wal.append(
                        txn_id,
                        *table_id,
                        crate::wal::Op::Ddl(crate::wal::DdlOp::DropTable {
                            table_id: *table_id,
                        }),
                    )?;
                }
                append_catalog_snapshot(&mut wal, txn_id, &next_catalog)?;
                wal.append_commit(txn_id, maintenance_epoch, &[])
            })();
            append.map_err(|error| self.commit_outcome_unknown(maintenance_epoch, error))?
        };
        self.await_durable_commit(commit_seq, maintenance_epoch)?;
        for (_, table) in &mut table_guards {
            table.mark_unavailable_after_quarantine();
        }
        {
            let mut live_tables = self.tables.write();
            for table_id in &bad_tables {
                live_tables.remove(table_id);
            }
        }
        let checkpoint = self.checkpoint_catalog_after_durable(next_catalog);
        self.finish_durable_publish(maintenance_epoch, &mut epoch_guard, checkpoint)?;

        // The catalog drop is durable. Directory placement is secondary but
        // still uses a write-through rename. A failure reports the known
        // catalog outcome and leaves a harmless orphan under `tables/`.
        for table_id in &bad_tables {
            let source = self.root.join(TABLES_DIR).join(table_id.to_string());
            if source.exists() {
                let destination = qdir.join(table_id.to_string());
                if let Err(error) = crate::durable_file::rename(&source, &destination) {
                    return Err(MongrelError::DurableCommit {
                        epoch: maintenance_epoch.0,
                        message: format!(
                            "DOCTOR dropped table {table_id} but quarantine move failed: {error}"
                        ),
                    });
                }
            }
        }
        Ok((
            bad_tables,
            Some(MaintenanceReceipt {
                epoch: maintenance_epoch,
            }),
        ))
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

fn append_catalog_snapshot(
    wal: &mut crate::wal::SharedWal,
    txn_id: u64,
    catalog: &Catalog,
) -> Result<()> {
    let catalog_json = crate::wal::DdlOp::encode_catalog(catalog)?;
    wal.append(
        txn_id,
        WAL_TABLE_ID,
        crate::wal::Op::Ddl(crate::wal::DdlOp::CatalogSnapshot { catalog_json }),
    )?;
    Ok(())
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
    crate::durable_file::create_directory(&root.join(VTAB_DIR))?;
    let dir = external_state_dir(root, name);
    crate::durable_file::create_directory(&dir)?;
    let pending = dir.join(format!("state.json.{txn_id}.tmp"));
    {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&pending)?;
        file.write_all(state)?;
        file.sync_all()?;
    }
    Ok(pending)
}

fn publish_external_state_file(root: &Path, name: &str, pending: &Path) -> Result<()> {
    let path = external_state_file(root, name);
    crate::durable_file::replace(pending, &path)?;
    Ok(())
}

fn write_external_state_file(
    durable: &crate::durable_file::DurableRoot,
    name: &str,
    state: &[u8],
) -> Result<()> {
    let directory = Path::new(VTAB_DIR).join(name);
    durable.create_directory_all(&directory)?;
    durable.write_atomic(directory.join("state.json"), state)?;
    Ok(())
}

fn validate_recovered_data_table(
    catalog: &Catalog,
    tables: &HashMap<u64, TableHandle>,
    table_id: u64,
    commit_epoch: u64,
    offset: u64,
) -> Result<bool> {
    let entry = catalog
        .tables
        .iter()
        .find(|entry| entry.table_id == table_id)
        .ok_or_else(|| MongrelError::CorruptWal {
            offset,
            reason: format!("committed record references unknown table {table_id}"),
        })?;
    if commit_epoch < entry.created_epoch {
        return Err(MongrelError::CorruptWal {
            offset,
            reason: format!(
                "table {table_id} record epoch {commit_epoch} precedes creation epoch {}",
                entry.created_epoch
            ),
        });
    }
    match entry.state {
        TableState::Dropped { at_epoch } => {
            // Abandoned hidden builds are marked dropped at the last durable
            // boundary during open, so their final build commit may equal the
            // cleanup epoch. Ordinary table drops consume a new epoch and must
            // remain strictly later than every data commit.
            let abandoned_build_boundary =
                entry.name.starts_with(CTAS_BUILD_TABLE_PREFIX) && commit_epoch == at_epoch;
            if commit_epoch >= at_epoch && !abandoned_build_boundary {
                Err(MongrelError::CorruptWal {
                    offset,
                    reason: format!(
                        "table {table_id} record epoch {commit_epoch} is not before drop epoch {at_epoch}"
                    ),
                })
            } else {
                Ok(false)
            }
        }
        TableState::Live | TableState::Building { .. } => {
            if tables.contains_key(&table_id) {
                Ok(true)
            } else {
                Err(MongrelError::CorruptWal {
                    offset,
                    reason: format!("live table {table_id} has no mounted recovery handle"),
                })
            }
        }
    }
}

type RecoveryTableStage = (
    Vec<crate::memtable::Row>,
    Vec<(crate::rowid::RowId, Epoch)>,
    Option<Epoch>,
    Epoch,
);

#[derive(Clone)]
struct RecoveryValidationTable {
    schema: Schema,
    flushed_epoch: u64,
}

fn validate_shared_wal_recovery_plan(
    durable_root: &crate::durable_file::DurableRoot,
    catalog: &Catalog,
    recovered_table_ids: &HashSet<u64>,
    reconciled_table_ids: &HashSet<u64>,
    meta_dek: Option<&[u8; META_DEK_LEN]>,
    kek: Option<Arc<crate::encryption::Kek>>,
    records: &[crate::wal::Record],
) -> Result<()> {
    use crate::wal::{DdlOp, Op};

    let mut tables = HashMap::<u64, RecoveryValidationTable>::new();
    for entry in &catalog.tables {
        if !matches!(entry.state, TableState::Live) {
            continue;
        }
        let relative_dir = Path::new(TABLES_DIR).join(entry.table_id.to_string());
        let manifest = match crate::manifest::read_durable(durable_root, &relative_dir, meta_dek) {
            Ok(manifest) => Some(manifest),
            Err(MongrelError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let flushed_epoch = if let Some(manifest) = manifest {
            if manifest.table_id != entry.table_id {
                return Err(MongrelError::Conflict(format!(
                    "catalog table {} storage identity mismatch",
                    entry.table_id
                )));
            }
            if (manifest.schema_id != entry.schema.schema_id
                && !reconciled_table_ids.contains(&entry.table_id))
                || manifest.flushed_epoch > manifest.current_epoch
                || manifest.global_idx_epoch > manifest.current_epoch
                || manifest.next_row_id == u64::MAX
                || manifest.auto_inc_next < 0
                || manifest.auto_inc_next == i64::MAX
                || (entry.schema.auto_increment_column().is_none() && manifest.auto_inc_next != 0)
            {
                return Err(MongrelError::InvalidArgument(format!(
                    "table {} manifest counters or schema identity are invalid",
                    entry.table_id
                )));
            }
            #[cfg(feature = "encryption")]
            let idx_dek = kek.as_ref().map(|key| key.derive_idx_key());
            #[cfg(not(feature = "encryption"))]
            let idx_dek: Option<zeroize::Zeroizing<[u8; 32]>> = None;
            crate::global_idx::read_durable_for(
                durable_root,
                &relative_dir,
                entry.table_id,
                &entry.schema,
                idx_dek.as_deref(),
            )?;
            let mut run_ids = HashSet::new();
            let mut maximum_row_id = None::<u64>;
            for run in &manifest.runs {
                if run.run_id >= u64::MAX as u128
                    || run.epoch_created > manifest.current_epoch
                    || !run_ids.insert(run.run_id)
                {
                    return Err(MongrelError::InvalidArgument(format!(
                        "table {} manifest contains an invalid or duplicate run id",
                        entry.table_id
                    )));
                }
                let relative = relative_dir
                    .join(crate::engine::RUNS_DIR)
                    .join(format!("r-{}.sr", run.run_id as u64));
                let file = durable_root.open_regular(&relative)?;
                let mut reader = crate::sorted_run::RunReader::open_file(
                    file,
                    entry.schema.clone(),
                    kek.clone(),
                )?;
                let header = reader.header();
                if header.run_id != run.run_id
                    || header.level != run.level
                    || header.row_count != run.row_count
                    || !header.is_uniform_epoch() && header.epoch_created != run.epoch_created
                    || header.is_uniform_epoch() && header.epoch_created != 0
                    || header.schema_id > entry.schema.schema_id
                {
                    return Err(MongrelError::InvalidArgument(format!(
                        "table {} run {} differs from its manifest: header=(id {}, level {}, rows {}, epoch {}, schema {}), manifest=(id {}, level {}, rows {}, epoch {}, schema <= {})",
                        entry.table_id,
                        run.run_id,
                        header.run_id,
                        header.level,
                        header.row_count,
                        header.epoch_created,
                        header.schema_id,
                        run.run_id,
                        run.level,
                        run.row_count,
                        run.epoch_created,
                        entry.schema.schema_id,
                    )));
                }
                if header.row_count != 0 {
                    maximum_row_id = Some(
                        maximum_row_id
                            .map_or(header.max_row_id, |value| value.max(header.max_row_id)),
                    );
                }
                reader.validate_all_pages()?;
            }
            if maximum_row_id.is_some_and(|maximum| manifest.next_row_id <= maximum) {
                return Err(MongrelError::InvalidArgument(format!(
                    "table {} next_row_id does not advance beyond persisted rows",
                    entry.table_id
                )));
            }
            for run in &manifest.retiring {
                if run.run_id >= u64::MAX as u128
                    || run.retire_epoch > manifest.current_epoch
                    || !run_ids.insert(run.run_id)
                {
                    return Err(MongrelError::InvalidArgument(format!(
                        "table {} manifest contains an invalid or aliased retired run",
                        entry.table_id
                    )));
                }
            }
            manifest.flushed_epoch
        } else {
            if !recovered_table_ids.contains(&entry.table_id) {
                return Err(MongrelError::NotFound(format!(
                    "live table {} manifest is missing",
                    entry.table_id
                )));
            }
            0
        };
        tables.insert(
            entry.table_id,
            RecoveryValidationTable {
                schema: entry.schema.clone(),
                flushed_epoch,
            },
        );
    }

    let committed = records
        .iter()
        .filter_map(|record| match record.op {
            Op::TxnCommit { epoch, .. } => Some((record.txn_id, epoch)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();
    let mut run_ids = HashSet::new();
    let mut recovered_row_ids = HashMap::<u64, HashSet<u64>>::new();
    for record in records {
        let Some(&commit_epoch) = committed.get(&record.txn_id) else {
            continue;
        };
        match &record.op {
            Op::Put { table_id, rows } => {
                let table = validate_recovery_data_table_plan(
                    catalog,
                    &tables,
                    *table_id,
                    commit_epoch,
                    record.seq.0,
                )?;
                let decoded: Vec<crate::memtable::Row> =
                    bincode::deserialize(rows).map_err(|error| MongrelError::CorruptWal {
                        offset: record.seq.0,
                        reason: format!(
                            "committed Put payload for transaction {} could not be decoded: {error}",
                            record.txn_id
                        ),
                    })?;
                if let Some(table) = table {
                    for row in &decoded {
                        if !recovered_row_ids
                            .entry(*table_id)
                            .or_default()
                            .insert(row.row_id.0)
                        {
                            return Err(MongrelError::CorruptWal {
                                offset: record.seq.0,
                                reason: format!(
                                    "committed WAL repeats recovered row id {} for table {table_id}",
                                    row.row_id.0
                                ),
                            });
                        }
                        validate_recovered_row(&table.schema, row)?;
                    }
                }
            }
            Op::Delete { table_id, .. } | Op::TruncateTable { table_id } => {
                validate_recovery_data_table_plan(
                    catalog,
                    &tables,
                    *table_id,
                    commit_epoch,
                    record.seq.0,
                )?;
            }
            Op::ExternalTableState { name, .. } => validate_recovered_external_name(name)?,
            Op::Ddl(DdlOp::ResetExternalTableState {
                name,
                generation_epoch,
            }) => {
                if *generation_epoch != commit_epoch {
                    return Err(MongrelError::CorruptWal {
                        offset: record.seq.0,
                        reason: format!(
                            "external state reset epoch {generation_epoch} does not match WAL commit epoch {commit_epoch}"
                        ),
                    });
                }
                validate_recovered_external_name(name)?;
            }
            Op::TxnCommit { added_runs, .. } => {
                for added in added_runs {
                    let Some(table) = validate_recovery_data_table_plan(
                        catalog,
                        &tables,
                        added.table_id,
                        commit_epoch,
                        record.seq.0,
                    )?
                    else {
                        continue;
                    };
                    if added.run_id >= u64::MAX as u128
                        || !run_ids.insert((added.table_id, added.run_id))
                    {
                        return Err(MongrelError::CorruptWal {
                            offset: record.seq.0,
                            reason: format!(
                                "duplicate or invalid recovered run {} for table {}",
                                added.run_id, added.table_id
                            ),
                        });
                    }
                    if commit_epoch <= table.flushed_epoch {
                        continue;
                    }
                    validate_planned_spilled_run(
                        durable_root,
                        record.txn_id,
                        commit_epoch,
                        added,
                        &table.schema,
                        kek.clone(),
                    )?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_recovery_data_table_plan<'a>(
    catalog: &Catalog,
    tables: &'a HashMap<u64, RecoveryValidationTable>,
    table_id: u64,
    commit_epoch: u64,
    offset: u64,
) -> Result<Option<&'a RecoveryValidationTable>> {
    let entry = catalog
        .tables
        .iter()
        .find(|entry| entry.table_id == table_id)
        .ok_or_else(|| MongrelError::CorruptWal {
            offset,
            reason: format!("committed record references unknown table {table_id}"),
        })?;
    if commit_epoch < entry.created_epoch {
        return Err(MongrelError::CorruptWal {
            offset,
            reason: format!(
                "table {table_id} record epoch {commit_epoch} precedes creation epoch {}",
                entry.created_epoch
            ),
        });
    }
    match entry.state {
        TableState::Dropped { at_epoch } => {
            let abandoned =
                entry.name.starts_with(CTAS_BUILD_TABLE_PREFIX) && commit_epoch == at_epoch;
            if commit_epoch >= at_epoch && !abandoned {
                return Err(MongrelError::CorruptWal {
                    offset,
                    reason: format!(
                        "table {table_id} record epoch {commit_epoch} is not before drop epoch {at_epoch}"
                    ),
                });
            }
            Ok(None)
        }
        TableState::Live => {
            tables
                .get(&table_id)
                .map(Some)
                .ok_or_else(|| MongrelError::CorruptWal {
                    offset,
                    reason: format!("live table {table_id} has no recovery plan"),
                })
        }
        TableState::Building { .. } => Err(MongrelError::CorruptWal {
            offset,
            reason: format!("building table {table_id} was not normalized before recovery"),
        }),
    }
}

fn validate_planned_spilled_run(
    root: &crate::durable_file::DurableRoot,
    txn_id: u64,
    commit_epoch: u64,
    added: &crate::wal::AddedRun,
    schema: &Schema,
    kek: Option<Arc<crate::encryption::Kek>>,
) -> Result<()> {
    let table = Path::new(TABLES_DIR).join(added.table_id.to_string());
    let destination = table
        .join(crate::engine::RUNS_DIR)
        .join(format!("r-{}.sr", added.run_id as u64));
    let pending = table
        .join("_txn")
        .join(txn_id.to_string())
        .join(format!("r-{}.sr", added.run_id as u64));
    let file = match root.open_regular(&destination) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            root.open_regular(&pending).map_err(|pending_error| {
                if pending_error.kind() == std::io::ErrorKind::NotFound {
                    MongrelError::CorruptWal {
                        offset: commit_epoch,
                        reason: format!(
                            "committed spilled run {} for transaction {txn_id} is missing",
                            added.run_id
                        ),
                    }
                } else {
                    pending_error.into()
                }
            })?
        }
        Err(error) => return Err(error.into()),
    };
    let mut reader = crate::sorted_run::RunReader::open_file(file, schema.clone(), kek)?;
    let header = reader.header();
    if header.run_id != added.run_id
        || header.content_hash != added.content_hash
        || header.row_count != added.row_count
        || header.level != added.level
        || header.min_row_id != added.min_row_id
        || header.max_row_id != added.max_row_id
        || header.schema_id != schema.schema_id
        || !header.is_uniform_epoch()
        || header.epoch_created != 0
    {
        return Err(MongrelError::CorruptWal {
            offset: commit_epoch,
            reason: format!(
                "committed spilled run {} metadata differs from WAL",
                added.run_id
            ),
        });
    }
    reader.validate_all_pages()?;
    Ok(())
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
    durable_root: &crate::durable_file::DurableRoot,
    tables: &HashMap<u64, TableHandle>,
    catalog: &Catalog,
    epoch: &EpochAuthority,
    records: &[crate::wal::Record],
) -> Result<()> {
    use crate::memtable::Row;
    use crate::wal::{DdlOp, Op};

    // Pass 1: committed-txn outcomes + collect spilled-run info.
    let mut committed: HashMap<u64, u64> = HashMap::new();
    let mut spilled_to_link: Vec<(
        u64, /*txn_id*/
        u64, /*epoch*/
        Vec<crate::wal::AddedRun>,
    )> = Vec::new();
    for r in records {
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
    for record in records {
        let Some(&commit_epoch) = committed.get(&record.txn_id) else {
            continue;
        };
        match &record.op {
            Op::Put { table_id, .. }
            | Op::Delete { table_id, .. }
            | Op::TruncateTable { table_id } => {
                validate_recovered_data_table(
                    catalog,
                    tables,
                    *table_id,
                    commit_epoch,
                    record.seq.0,
                )?;
            }
            Op::TxnCommit { added_runs, .. } => {
                for run in added_runs {
                    validate_recovered_data_table(
                        catalog,
                        tables,
                        run.table_id,
                        commit_epoch,
                        record.seq.0,
                    )?;
                }
            }
            _ => {}
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
    enum ExternalRecoveryAction {
        Write { name: String, state: Vec<u8> },
        Reset { name: String },
    }
    let mut stage: HashMap<u64, RecoveryTableStage> = HashMap::new();
    let mut external_actions = Vec::new();
    let mut max_epoch = epoch.visible().0;
    for r in records.iter().cloned() {
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
                let rows: Vec<Row> = bincode::deserialize(&rows).map_err(|error| {
                    MongrelError::CorruptWal {
                        offset: r.seq.0,
                        reason: format!(
                            "committed Put payload for transaction {} could not be decoded: {error}",
                            r.txn_id
                        ),
                    }
                })?;
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
                let current_generation = catalog
                    .external_tables
                    .iter()
                    .find(|entry| entry.name == name)
                    .map(|entry| entry.created_epoch);
                if current_generation.is_some_and(|created_epoch| ce >= created_epoch) {
                    validate_recovered_external_name(&name)?;
                    external_actions.push(ExternalRecoveryAction::Write { name, state });
                }
            }
            Op::Ddl(DdlOp::ResetExternalTableState {
                name,
                generation_epoch,
            }) => {
                if generation_epoch != ce {
                    return Err(MongrelError::CorruptWal {
                        offset: r.seq.0,
                        reason: format!(
                        "external state reset epoch {generation_epoch} does not match WAL commit epoch {ce}"
                    ),
                    });
                }
                validate_recovered_external_name(&name)?;
                external_actions.push(ExternalRecoveryAction::Reset { name });
            }
            Op::Flush { .. }
            | Op::TxnCommit { .. }
            | Op::TxnAbort
            | Op::Ddl(_)
            | Op::BeforeImage { .. }
            | Op::CommitTimestamp { .. }
            | Op::SpilledRows { .. } => {}
        }
    }
    for (_, commit_epoch, added_runs) in &mut spilled_to_link {
        added_runs.retain(|added| {
            tables
                .get(&added.table_id)
                .is_some_and(|table| table.lock().flushed_epoch() < *commit_epoch)
        });
    }
    spilled_to_link.retain(|(_, _, added_runs)| !added_runs.is_empty());
    validate_recovery_table_stages(tables, &stage)?;
    validate_recovery_spilled_runs(durable_root, tables, &spilled_to_link)?;

    // All WAL payloads, catalog generations, table stages, and immutable run
    // identities have now been validated. Only this application phase mutates
    // the database tree.
    for action in external_actions {
        match action {
            ExternalRecoveryAction::Write { name, state } => {
                write_external_state_file(durable_root, &name, &state)?;
            }
            ExternalRecoveryAction::Reset { name } => {
                durable_root.create_directory_all(VTAB_DIR)?;
                durable_root.remove_directory_all(Path::new(VTAB_DIR).join(name))?;
            }
        }
    }
    for (table_id, (rows, deletes, truncate_epoch, table_epoch)) in stage {
        let Some(handle) = tables.get(&table_id) else {
            continue;
        };
        let mut t = handle.lock();
        if let Some(epoch) = truncate_epoch {
            t.apply_truncate(epoch);
        }
        t.recover_apply(rows, deletes)?;
        // The WAL can be newer than the copied/persisted manifest after a
        // crash or replication apply. Rebuild O(1) count metadata from the
        // recovered state before endorsing the commit epoch in the manifest.
        let rows = t.visible_rows(Snapshot::at(Epoch(u64::MAX)))?;
        t.live_count = rows.len() as u64;
        // Recovery can replay older row commits while a newer spilled run is
        // already linked by the copied manifest. Never move that manifest's
        // epoch behind its existing run references.
        t.persist_manifest(table_epoch.max(epoch.visible()))?;
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
            let table_dir = Path::new(TABLES_DIR).join(ar.table_id.to_string());
            let destination = table_dir
                .join(crate::engine::RUNS_DIR)
                .join(format!("r-{}.sr", ar.run_id));
            match durable_root.open_regular(&destination) {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    let pending = table_dir
                        .join("_txn")
                        .join(txn_id.to_string())
                        .join(format!("r-{}.sr", ar.run_id));
                    durable_root.rename_file_new(&pending, &destination)?;
                }
                Err(error) => return Err(error.into()),
            }
            // Only link a run whose file is actually present, and never re-link
            // one the publish phase already persisted into the manifest (which is
            // the common clean-reopen case, since the `TxnCommit` lives in the WAL
            // until segment GC). `recover_spilled_run` is idempotent + reconciles
            // `live_count`/indexes only when the run is genuinely new.
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
                t.persist_manifest(Epoch(*ce).max(epoch.visible()))?;
            }
        }
    }

    epoch.advance_recovered(Epoch(max_epoch));
    Ok(())
}

fn reconcile_recovered_table_metadata(
    tables: &HashMap<u64, TableHandle>,
    epoch: Epoch,
) -> Result<()> {
    let mut table_ids = tables.keys().copied().collect::<Vec<_>>();
    table_ids.sort_unstable();
    let mut plans = Vec::with_capacity(table_ids.len());
    for table_id in &table_ids {
        let handle = tables.get(table_id).ok_or_else(|| {
            MongrelError::Other(format!("mounted table {table_id} vanished during recovery"))
        })?;
        plans.push((*table_id, handle.lock().plan_recovered_metadata()?));
    }
    // Every table's data and metadata have been decoded successfully. Publish
    // repairs only after the complete database-wide plan is known valid.
    for (table_id, plan) in plans {
        let handle = tables.get(&table_id).ok_or_else(|| {
            MongrelError::Other(format!("mounted table {table_id} vanished during recovery"))
        })?;
        handle.lock().apply_recovered_metadata(plan, epoch)?;
    }
    Ok(())
}

fn validate_recovered_external_name(name: &str) -> Result<()> {
    if name.is_empty()
        || !name.chars().all(|character| {
            character.is_ascii_alphanumeric() || character == '_' || character == '-'
        })
    {
        return Err(MongrelError::CorruptWal {
            offset: 0,
            reason: format!("unsafe recovered external-table name {name:?}"),
        });
    }
    Ok(())
}

fn validate_recovery_table_stages(
    tables: &HashMap<u64, TableHandle>,
    stages: &HashMap<u64, RecoveryTableStage>,
) -> Result<()> {
    for (table_id, (rows, _, _, _)) in stages {
        let handle = tables
            .get(table_id)
            .ok_or_else(|| MongrelError::CorruptWal {
                offset: *table_id,
                reason: format!("recovery stage references unmounted table {table_id}"),
            })?;
        let table = handle.lock();
        // Force all existing immutable runs through their integrity/decode path
        // before any other table manifest can be changed.
        table.visible_rows(Snapshot::at(Epoch(u64::MAX)))?;
        for row in rows {
            validate_recovered_row(table.schema(), row)?;
        }
    }
    Ok(())
}

fn validate_recovered_row(schema: &Schema, row: &crate::memtable::Row) -> Result<()> {
    if row.deleted || row.row_id.0 == u64::MAX {
        return Err(MongrelError::CorruptWal {
            offset: row.row_id.0,
            reason: "committed Put payload contains a tombstone or exhausted row id".into(),
        });
    }
    let cells = row
        .columns
        .iter()
        .map(|(column, value)| (*column, value.clone()))
        .collect::<Vec<_>>();
    schema
        .validate_persisted_values(&cells)
        .map_err(|error| MongrelError::CorruptWal {
            offset: row.row_id.0,
            reason: format!("recovered row violates table schema: {error}"),
        })?;
    if schema.auto_increment_column().is_some_and(|column| {
        matches!(row.columns.get(&column.id), Some(Value::Int64(value)) if *value == i64::MAX)
    }) {
        return Err(MongrelError::CorruptWal {
            offset: row.row_id.0,
            reason: "recovered AUTO_INCREMENT value exhausts i64".into(),
        });
    }
    Ok(())
}

fn validate_recovery_spilled_runs(
    root: &crate::durable_file::DurableRoot,
    tables: &HashMap<u64, TableHandle>,
    spilled: &[(u64, u64, Vec<crate::wal::AddedRun>)],
) -> Result<()> {
    let mut identities = HashSet::new();
    for (txn_id, commit_epoch, added_runs) in spilled {
        for added in added_runs {
            if added.run_id >= u64::MAX as u128 {
                return Err(MongrelError::CorruptWal {
                    offset: *commit_epoch,
                    reason: format!(
                        "recovered run id {} exceeds the on-disk namespace",
                        added.run_id
                    ),
                });
            }
            let Some(handle) = tables.get(&added.table_id) else {
                continue;
            };
            if !identities.insert((added.table_id, added.run_id)) {
                return Err(MongrelError::CorruptWal {
                    offset: *commit_epoch,
                    reason: format!(
                        "duplicate recovered run {} for table {}",
                        added.run_id, added.table_id
                    ),
                });
            }
            let table = handle.lock();
            validate_planned_spilled_run(
                root,
                *txn_id,
                *commit_epoch,
                added,
                table.schema(),
                table.kek(),
            )?;
        }
    }
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

fn trigger_validation_error(error: MongrelError) -> MongrelError {
    match error {
        MongrelError::TriggerValidation(_) => error,
        MongrelError::InvalidArgument(message)
        | MongrelError::Conflict(message)
        | MongrelError::NotFound(message) => MongrelError::TriggerValidation(message),
        error => error,
    }
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
    durable_root: Option<&crate::durable_file::DurableRoot>,
    target_catalog: &mut Catalog,
    meta_dek: Option<&[u8; META_DEK_LEN]>,
    wal_dek: Option<&zeroize::Zeroizing<[u8; 32]>>,
    apply: bool,
    table_roots: Option<&HashMap<u64, Arc<crate::durable_file::DurableRoot>>>,
) -> Result<()> {
    use crate::wal::SharedWal;
    let records = match durable_root {
        Some(root) => SharedWal::replay_durable_with_dek(root, wal_dek)?,
        None => SharedWal::replay_with_dek(root, wal_dek)?,
    };
    recover_ddl_from_records(
        root,
        durable_root,
        target_catalog,
        meta_dek,
        apply,
        table_roots,
        &records,
    )
}

fn recover_ddl_from_records(
    root: &Path,
    durable_root: Option<&crate::durable_file::DurableRoot>,
    target_catalog: &mut Catalog,
    meta_dek: Option<&[u8; META_DEK_LEN]>,
    apply: bool,
    table_roots: Option<&HashMap<u64, Arc<crate::durable_file::DurableRoot>>>,
    records: &[crate::wal::Record],
) -> Result<()> {
    use crate::wal::{DdlOp, Op};

    let original_catalog = target_catalog.clone();
    let mut recovered_catalog = original_catalog.clone();
    let cat = &mut recovered_catalog;
    let mut created_table_ids = HashSet::<u64>::new();
    let mut ttl_updates = HashMap::<u64, (Option<crate::manifest::TtlPolicy>, u64)>::new();

    let mut committed: HashMap<u64, u64> = HashMap::new();
    for r in records {
        if let Op::TxnCommit { epoch: ce, .. } = r.op {
            committed.insert(r.txn_id, ce);
        }
    }
    let catalog_snapshot_txns = records
        .iter()
        .filter_map(|record| {
            (committed.contains_key(&record.txn_id)
                && matches!(&record.op, Op::Ddl(DdlOp::CatalogSnapshot { .. })))
            .then_some(record.txn_id)
        })
        .collect::<HashSet<_>>();

    let mut changed = false;
    let mut applied_catalog_epoch = cat.db_epoch;
    let max_committed_epoch = committed.values().copied().max().unwrap_or(cat.db_epoch);
    for r in records.iter().cloned() {
        let Some(&ce) = committed.get(&r.txn_id) else {
            continue;
        };
        let txn_id = r.txn_id;
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
                validate_recovered_schema(&schema)?;
                created_table_ids.insert(table_id);
                cat.tables.push(CatalogEntry {
                    table_id,
                    name: name.clone(),
                    schema,
                    state: TableState::Live,
                    created_epoch: ce,
                });
                cat.next_table_id =
                    cat.next_table_id
                        .max(table_id.checked_add(1).ok_or_else(|| {
                            MongrelError::Full("table id namespace exhausted".into())
                        })?);
                changed = true;
            }
            Op::Ddl(DdlOp::CreateBuildingTable {
                table_id,
                ref build_name,
                ref intended_name,
                ref query_id,
                created_at_unix_nanos,
                ref schema_json,
            }) => {
                if cat.tables.iter().any(|table| table.table_id == table_id) {
                    continue;
                }
                let schema = DdlOp::decode_schema(schema_json)?;
                validate_recovered_schema(&schema)?;
                created_table_ids.insert(table_id);
                cat.tables.push(CatalogEntry {
                    table_id,
                    name: build_name.clone(),
                    schema,
                    state: TableState::Building {
                        intended_name: intended_name.clone(),
                        query_id: query_id.clone(),
                        created_at_unix_nanos,
                        replaces_table_id: None,
                    },
                    created_epoch: ce,
                });
                cat.next_table_id =
                    cat.next_table_id
                        .max(table_id.checked_add(1).ok_or_else(|| {
                            MongrelError::Full("table id namespace exhausted".into())
                        })?);
                changed = true;
            }
            Op::Ddl(DdlOp::CreateRebuildingTable {
                table_id,
                ref build_name,
                ref intended_name,
                ref query_id,
                created_at_unix_nanos,
                replaces_table_id,
                ref schema_json,
            }) => {
                if cat.tables.iter().any(|table| table.table_id == table_id) {
                    continue;
                }
                let schema = DdlOp::decode_schema(schema_json)?;
                validate_recovered_schema(&schema)?;
                created_table_ids.insert(table_id);
                cat.tables.push(CatalogEntry {
                    table_id,
                    name: build_name.clone(),
                    schema,
                    state: TableState::Building {
                        intended_name: intended_name.clone(),
                        query_id: query_id.clone(),
                        created_at_unix_nanos,
                        replaces_table_id: Some(replaces_table_id),
                    },
                    created_epoch: ce,
                });
                cat.next_table_id =
                    cat.next_table_id
                        .max(table_id.checked_add(1).ok_or_else(|| {
                            MongrelError::Full("table id namespace exhausted".into())
                        })?);
                changed = true;
            }
            Op::Ddl(DdlOp::DropTable { table_id }) => {
                let mut dropped_name = None;
                if let Some(entry) = cat.tables.iter_mut().find(|t| t.table_id == table_id) {
                    if matches!(entry.state, TableState::Live | TableState::Building { .. }) {
                        dropped_name = Some(entry.name.clone());
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
                    if !catalog_snapshot_txns.contains(&txn_id) {
                        advance_security_version(cat)?;
                    }
                }
            }
            Op::Ddl(DdlOp::PublishBuildingTable {
                table_id,
                ref new_name,
            }) => {
                if let Some(entry) = cat
                    .tables
                    .iter_mut()
                    .find(|table| table.table_id == table_id)
                {
                    if entry.name != *new_name || !matches!(entry.state, TableState::Live) {
                        entry.name = new_name.clone();
                        entry.state = TableState::Live;
                        changed = true;
                    }
                }
            }
            Op::Ddl(DdlOp::ReplaceBuildingTable {
                table_id,
                replaced_table_id,
                ref new_name,
            }) => {
                changed |=
                    apply_rebuilding_publish(cat, table_id, replaced_table_id, new_name, ce)?;
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
                    if !catalog_snapshot_txns.contains(&txn_id) {
                        advance_security_version(cat)?;
                    }
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
                    if apply_recovered_column_def(&mut entry.schema, column)? {
                        validate_recovered_schema(&entry.schema)?;
                        changed = true;
                    }
                }
                if let Some((table, old_name, new_name)) = renamed {
                    for role in &mut cat.roles {
                        for permission in &mut role.permissions {
                            rename_permission_column(permission, &table, &old_name, &new_name);
                        }
                    }
                    if !catalog_snapshot_txns.contains(&txn_id) {
                        advance_security_version(cat)?;
                    }
                }
            }
            Op::Ddl(DdlOp::SetTtl {
                table_id,
                ref policy_json,
            }) => {
                let policy = DdlOp::decode_ttl(policy_json)?;
                let entry = cat
                    .tables
                    .iter()
                    .find(|entry| entry.table_id == table_id)
                    .ok_or_else(|| {
                        MongrelError::Schema(format!(
                            "recovered TTL references unknown table id {table_id}"
                        ))
                    })?;
                if let Some(policy) = policy {
                    let valid = entry
                        .schema
                        .columns
                        .iter()
                        .find(|column| column.id == policy.column_id)
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
                ttl_updates.insert(table_id, (policy, ce));
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
                    if !catalog_snapshot_txns.contains(&txn_id) {
                        advance_security_version(cat)?;
                    }
                    changed = true;
                }
            }
            Op::Ddl(DdlOp::SetSqlPragma { ref key, value }) => {
                let target = match key.as_str() {
                    "user_version" => &mut cat.user_version,
                    "application_id" => &mut cat.application_id,
                    _ => {
                        return Err(MongrelError::InvalidArgument(format!(
                            "unsupported recovered SQL pragma {key:?}"
                        )))
                    }
                };
                if *target != Some(value) {
                    *target = Some(value);
                    cat.db_epoch = cat.db_epoch.max(ce);
                    changed = true;
                }
            }
            Op::Ddl(DdlOp::CatalogSnapshot { ref catalog_json }) => {
                if ce <= applied_catalog_epoch {
                    continue;
                }
                let snapshot = DdlOp::decode_catalog(catalog_json)?;
                if snapshot.db_epoch != ce {
                    return Err(MongrelError::Schema(format!(
                        "catalog snapshot epoch {} does not match WAL commit epoch {ce}",
                        snapshot.db_epoch
                    )));
                }
                validate_recovered_catalog(&snapshot)?;
                validate_catalog_transition(cat, &snapshot)?;
                *cat = snapshot;
                applied_catalog_epoch = ce;
                changed = true;
            }
            _ => {}
        }
    }

    if cat.db_epoch < max_committed_epoch {
        cat.db_epoch = max_committed_epoch;
        changed = true;
    }
    changed |= repair_catalog_allocator_counters(cat)?;

    validate_recovered_catalog(cat)?;
    let storage_reconciliation = validate_recovered_storage_plan(
        root,
        durable_root,
        cat,
        &created_table_ids,
        &ttl_updates,
        meta_dek,
    )?;

    let needs_storage_apply = !storage_reconciliation.is_empty() || !ttl_updates.is_empty();
    if apply && (changed || needs_storage_apply) {
        for table_id in storage_reconciliation {
            let entry = cat
                .tables
                .iter()
                .find(|entry| entry.table_id == table_id)
                .ok_or_else(|| MongrelError::CorruptWal {
                    offset: table_id,
                    reason: "recovery storage plan lost its catalog table".into(),
                })?;
            ensure_recovered_table_storage(
                table_roots
                    .and_then(|roots| roots.get(&table_id))
                    .map(Arc::as_ref),
                durable_root,
                &root.join(TABLES_DIR).join(table_id.to_string()),
                table_id,
                &entry.schema,
                meta_dek,
            )?;
        }
        for (table_id, (policy, ttl_epoch)) in ttl_updates {
            let Some(entry) = cat.tables.iter().find(|entry| {
                entry.table_id == table_id
                    && matches!(entry.state, TableState::Live | TableState::Building { .. })
            }) else {
                continue;
            };
            let table_root = if let Some(root) = table_roots.and_then(|roots| roots.get(&table_id))
            {
                root.try_clone()?
            } else if let Some(root) = durable_root {
                root.open_directory(Path::new(TABLES_DIR).join(table_id.to_string()))?
            } else {
                crate::durable_file::DurableRoot::open(
                    root.join(TABLES_DIR).join(table_id.to_string()),
                )?
            };
            let table_dir = table_root.io_path()?;
            let mut manifest = crate::manifest::read_durable(&table_root, "", meta_dek)?;
            if manifest.ttl != policy || manifest.current_epoch < ttl_epoch {
                manifest.ttl = policy;
                manifest.current_epoch = manifest.current_epoch.max(ttl_epoch);
                manifest.schema_id = entry.schema.schema_id;
                crate::manifest::write_atomic(&table_dir, &mut manifest, meta_dek)?;
            }
        }
        if changed {
            match durable_root {
                Some(root) => catalog::write_durable(root, cat, meta_dek)?,
                None => catalog::write_atomic(root, cat, meta_dek)?,
            }
        }
    }
    *target_catalog = recovered_catalog;
    Ok(())
}

fn ensure_recovered_table_storage(
    pinned_table: Option<&crate::durable_file::DurableRoot>,
    durable_root: Option<&crate::durable_file::DurableRoot>,
    fallback_table_dir: &Path,
    table_id: u64,
    schema: &Schema,
    meta_dek: Option<&[u8; META_DEK_LEN]>,
) -> Result<()> {
    let table_root = if let Some(root) = pinned_table {
        root.try_clone()?
    } else if let Some(root) = durable_root {
        let relative = Path::new(TABLES_DIR).join(table_id.to_string());
        match root.open_directory(&relative) {
            Ok(table) => table,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                root.create_directory_all_pinned(relative)?
            }
            Err(error) => return Err(error.into()),
        }
    } else {
        crate::durable_file::create_directory_all(fallback_table_dir)?;
        crate::durable_file::DurableRoot::open(fallback_table_dir)?
    };
    let table_dir = table_root.io_path()?;
    let mut existing_manifest = match crate::manifest::read_durable(&table_root, "", meta_dek) {
        Ok(manifest) => {
            if manifest.table_id != table_id {
                return Err(MongrelError::Conflict(format!(
                    "recovered table directory id mismatch: expected {table_id}, found {}",
                    manifest.table_id
                )));
            }
            Some(manifest)
        }
        Err(MongrelError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };

    table_root.create_directory_all(crate::engine::WAL_DIR)?;
    table_root.create_directory_all(crate::engine::RUNS_DIR)?;
    crate::engine::write_schema(&table_dir, schema)?;

    if let Some(mut manifest) = existing_manifest.take() {
        if manifest.schema_id != schema.schema_id {
            manifest.schema_id = schema.schema_id;
            crate::manifest::write_atomic(&table_dir, &mut manifest, meta_dek)?;
        }
    } else {
        // The DB-wide meta DEK is also the per-table manifest meta DEK.
        let mut manifest = crate::manifest::Manifest::new(table_id, schema.schema_id);
        crate::manifest::write_atomic(&table_dir, &mut manifest, meta_dek)?;
    }
    Ok(())
}

fn validate_recovered_schema(schema: &Schema) -> Result<()> {
    schema.validate_auto_increment()?;
    schema.validate_defaults()?;
    schema.validate_ai()?;
    let mut column_ids = HashSet::new();
    let mut column_names = HashSet::new();
    for column in &schema.columns {
        if !column_ids.insert(column.id) || !column_names.insert(column.name.as_str()) {
            return Err(MongrelError::Schema(
                "recovered schema contains duplicate columns".into(),
            ));
        }
        match &column.ty {
            TypeId::Decimal128 { precision, scale }
                if *precision == 0 || *precision > 38 || scale.unsigned_abs() > *precision =>
            {
                return Err(MongrelError::Schema(format!(
                    "column {:?} has invalid decimal precision or scale",
                    column.name
                )));
            }
            TypeId::Enum { variants }
                if variants.is_empty()
                    || variants.iter().any(String::is_empty)
                    || variants.iter().collect::<HashSet<_>>().len() != variants.len() =>
            {
                return Err(MongrelError::Schema(format!(
                    "column {:?} has invalid enum variants",
                    column.name
                )));
            }
            _ => {}
        }
    }
    let mut index_names = HashSet::new();
    for index in &schema.indexes {
        index.validate_options()?;
        if index.name.is_empty()
            || !index_names.insert(index.name.as_str())
            || schema
                .columns
                .iter()
                .all(|column| column.id != index.column_id)
        {
            return Err(MongrelError::Schema(format!(
                "recovered index {:?} references missing column {}",
                index.name, index.column_id
            )));
        }
    }
    let mut colocated = HashSet::new();
    for group in &schema.colocation {
        if group.is_empty()
            || group.iter().any(|id| !column_ids.contains(id))
            || group.iter().any(|id| !colocated.insert(*id))
        {
            return Err(MongrelError::Schema(
                "recovered schema contains invalid column co-location groups".into(),
            ));
        }
    }

    let mut constraint_ids = HashSet::new();
    let mut constraint_names = HashSet::<String>::new();
    let mut validate_constraint_identity = |id: u16, name: &str| -> Result<()> {
        if name.is_empty()
            || !constraint_ids.insert(id)
            || !constraint_names.insert(name.to_owned())
        {
            return Err(MongrelError::Schema(
                "recovered schema contains duplicate or empty constraint identities".into(),
            ));
        }
        Ok(())
    };
    for unique in &schema.constraints.uniques {
        validate_constraint_identity(unique.id, &unique.name)?;
        if unique.columns.is_empty()
            || unique.columns.iter().any(|id| !column_ids.contains(id))
            || unique.columns.iter().collect::<HashSet<_>>().len() != unique.columns.len()
        {
            return Err(MongrelError::Schema(format!(
                "unique constraint {:?} has invalid columns",
                unique.name
            )));
        }
    }
    for foreign_key in &schema.constraints.foreign_keys {
        validate_constraint_identity(foreign_key.id, &foreign_key.name)?;
        if foreign_key.ref_table.is_empty()
            || foreign_key.columns.is_empty()
            || foreign_key.columns.len() != foreign_key.ref_columns.len()
            || foreign_key
                .columns
                .iter()
                .any(|id| !column_ids.contains(id))
            || foreign_key.columns.iter().collect::<HashSet<_>>().len() != foreign_key.columns.len()
            || foreign_key.ref_columns.iter().collect::<HashSet<_>>().len()
                != foreign_key.ref_columns.len()
        {
            return Err(MongrelError::Schema(format!(
                "foreign key {:?} has invalid columns",
                foreign_key.name
            )));
        }
        if (matches!(foreign_key.on_delete, crate::constraint::FkAction::SetNull)
            || matches!(foreign_key.on_update, crate::constraint::FkAction::SetNull))
            && foreign_key.columns.iter().any(|id| {
                schema
                    .columns
                    .iter()
                    .find(|column| column.id == *id)
                    .is_none_or(|column| {
                        !column.flags.contains(crate::schema::ColumnFlags::NULLABLE)
                    })
            })
        {
            return Err(MongrelError::Schema(format!(
                "foreign key {:?} uses SET NULL on a non-nullable column",
                foreign_key.name
            )));
        }
    }
    for check in &schema.constraints.checks {
        validate_constraint_identity(check.id, &check.name)?;
        check.expr.validate()?;
        validate_check_columns(&check.expr, &column_ids)?;
    }
    Ok(())
}

fn validate_check_columns(
    expression: &crate::constraint::CheckExpr,
    column_ids: &HashSet<u16>,
) -> Result<()> {
    use crate::constraint::CheckExpr;
    match expression {
        CheckExpr::Col(id) | CheckExpr::IsNull(id) | CheckExpr::IsNotNull(id) => {
            if column_ids.contains(id) {
                Ok(())
            } else {
                Err(MongrelError::Schema(format!(
                    "check constraint references unknown column {id}"
                )))
            }
        }
        CheckExpr::Regex { col, .. } => {
            if column_ids.contains(col) {
                Ok(())
            } else {
                Err(MongrelError::Schema(format!(
                    "check constraint references unknown column {col}"
                )))
            }
        }
        CheckExpr::Add(left, right)
        | CheckExpr::Sub(left, right)
        | CheckExpr::Mul(left, right)
        | CheckExpr::Div(left, right)
        | CheckExpr::Mod(left, right)
        | CheckExpr::Eq(left, right)
        | CheckExpr::Ne(left, right)
        | CheckExpr::Lt(left, right)
        | CheckExpr::Le(left, right)
        | CheckExpr::Gt(left, right)
        | CheckExpr::Ge(left, right)
        | CheckExpr::And(left, right)
        | CheckExpr::Or(left, right) => {
            validate_check_columns(left, column_ids)?;
            validate_check_columns(right, column_ids)
        }
        CheckExpr::Not(inner) => validate_check_columns(inner, column_ids),
        CheckExpr::True | CheckExpr::Lit(_) => Ok(()),
    }
}

fn validate_catalog_transition(current: &Catalog, next: &Catalog) -> Result<()> {
    for (name, prior, candidate) in [
        ("db_epoch", current.db_epoch, next.db_epoch),
        ("next_table_id", current.next_table_id, next.next_table_id),
        (
            "next_segment_no",
            current.next_segment_no,
            next.next_segment_no,
        ),
        ("next_user_id", current.next_user_id, next.next_user_id),
        (
            "security_version",
            current.security_version,
            next.security_version,
        ),
    ] {
        if candidate < prior {
            return Err(MongrelError::Schema(format!(
                "catalog snapshot rolls back {name} from {prior} to {candidate}"
            )));
        }
    }
    for prior in &current.tables {
        let Some(candidate) = next
            .tables
            .iter()
            .find(|entry| entry.table_id == prior.table_id)
        else {
            return Err(MongrelError::Schema(format!(
                "catalog snapshot removes table identity {}",
                prior.table_id
            )));
        };
        if candidate.created_epoch != prior.created_epoch
            || candidate.schema.schema_id < prior.schema.schema_id
            || matches!(prior.state, TableState::Dropped { .. })
                && !matches!(candidate.state, TableState::Dropped { .. })
        {
            return Err(MongrelError::Schema(format!(
                "catalog snapshot rolls back table identity {}",
                prior.table_id
            )));
        }
    }
    for prior in &current.users {
        if let Some(candidate) = next.users.iter().find(|user| user.id == prior.id) {
            if candidate.username != prior.username
                || candidate.created_epoch != prior.created_epoch
            {
                return Err(MongrelError::Schema(format!(
                    "catalog snapshot reuses user identity {}",
                    prior.id
                )));
            }
        }
    }
    Ok(())
}

fn validate_recovered_catalog(catalog: &Catalog) -> Result<()> {
    let mut table_ids = HashSet::new();
    let mut active_names = HashSet::new();
    let mut max_table_id = None::<u64>;
    for entry in &catalog.tables {
        if !table_ids.insert(entry.table_id) {
            return Err(MongrelError::Schema(format!(
                "catalog contains duplicate table id {}",
                entry.table_id
            )));
        }
        max_table_id = Some(max_table_id.map_or(entry.table_id, |value| value.max(entry.table_id)));
        if entry.name.is_empty() || entry.created_epoch > catalog.db_epoch {
            return Err(MongrelError::Schema(format!(
                "catalog table {} has invalid name or creation epoch",
                entry.table_id
            )));
        }
        validate_recovered_schema(&entry.schema)?;
        match &entry.state {
            TableState::Live => {
                if !active_names.insert(entry.name.as_str()) {
                    return Err(MongrelError::Schema(format!(
                        "catalog contains duplicate active table name {:?}",
                        entry.name
                    )));
                }
            }
            TableState::Dropped { at_epoch } => {
                if *at_epoch < entry.created_epoch || *at_epoch > catalog.db_epoch {
                    return Err(MongrelError::Schema(format!(
                        "catalog table {} has invalid drop epoch {at_epoch}",
                        entry.table_id
                    )));
                }
            }
            TableState::Building {
                intended_name,
                query_id,
                replaces_table_id,
                ..
            } => {
                if intended_name.is_empty() || query_id.is_empty() {
                    return Err(MongrelError::Schema(format!(
                        "building table {} has empty identity fields",
                        entry.table_id
                    )));
                }
                if !active_names.insert(entry.name.as_str()) {
                    return Err(MongrelError::Schema(format!(
                        "catalog contains duplicate active/building table name {:?}",
                        entry.name
                    )));
                }
                if replaces_table_id.is_some_and(|id| id == entry.table_id) {
                    return Err(MongrelError::Schema(
                        "building table cannot replace itself".into(),
                    ));
                }
            }
        }
    }
    if let Some(maximum) = max_table_id {
        let required = maximum
            .checked_add(1)
            .ok_or_else(|| MongrelError::Full("table id namespace exhausted".into()))?;
        if catalog.next_table_id < required {
            return Err(MongrelError::Schema(format!(
                "catalog next_table_id {} precedes required {required}",
                catalog.next_table_id
            )));
        }
    }
    for entry in &catalog.tables {
        if let TableState::Building {
            replaces_table_id: Some(replaced),
            ..
        } = entry.state
        {
            if !table_ids.contains(&replaced) {
                return Err(MongrelError::Schema(format!(
                    "building table {} replaces unknown table {replaced}",
                    entry.table_id
                )));
            }
        }
    }
    for entry in &catalog.tables {
        if matches!(entry.state, TableState::Live | TableState::Building { .. }) {
            validate_foreign_key_targets(catalog, &entry.schema)?;
        }
    }

    let mut external_names = HashSet::new();
    for entry in &catalog.external_tables {
        entry.validate()?;
        validate_recovered_schema(&entry.declared_schema)?;
        if !entry.declared_schema.constraints.is_empty() {
            return Err(MongrelError::Schema(format!(
                "external table {:?} cannot carry engine-enforced constraints",
                entry.name
            )));
        }
        if entry.created_epoch > catalog.db_epoch
            || !external_names.insert(entry.name.as_str())
            || active_names.contains(entry.name.as_str())
        {
            return Err(MongrelError::Schema(format!(
                "invalid or duplicate external table {:?}",
                entry.name
            )));
        }
    }

    let mut procedure_names = HashSet::new();
    for entry in &catalog.procedures {
        entry.procedure.validate()?;
        if entry.procedure.created_epoch > entry.procedure.updated_epoch
            || entry.procedure.updated_epoch > catalog.db_epoch
            || !procedure_names.insert(entry.procedure.name.as_str())
        {
            return Err(MongrelError::Schema(format!(
                "invalid or duplicate procedure {:?}",
                entry.procedure.name
            )));
        }
        validate_recovered_procedure_references(catalog, &entry.procedure)?;
    }

    let mut trigger_names = HashSet::new();
    for entry in &catalog.triggers {
        entry.trigger.validate()?;
        if entry.trigger.created_epoch > entry.trigger.updated_epoch
            || entry.trigger.updated_epoch > catalog.db_epoch
            || !trigger_names.insert(entry.trigger.name.as_str())
        {
            return Err(MongrelError::Schema(format!(
                "invalid or duplicate trigger {:?}",
                entry.trigger.name
            )));
        }
        validate_recovered_trigger_references(catalog, &entry.trigger)?;
    }

    let mut views = HashSet::new();
    for view in &catalog.materialized_views {
        let target = catalog.live(&view.name).ok_or_else(|| {
            MongrelError::Schema(format!(
                "materialized view {:?} has no live table",
                view.name
            ))
        })?;
        if view.name.is_empty()
            || view.query.trim().is_empty()
            || view.last_refresh_epoch > catalog.db_epoch
            || !views.insert(view.name.as_str())
        {
            return Err(MongrelError::Schema(format!(
                "materialized view {:?} has no unique live table",
                view.name
            )));
        }
        if let Some(incremental) = &view.incremental {
            let source = catalog.live(&incremental.source_table).ok_or_else(|| {
                MongrelError::Schema(format!(
                    "materialized view {:?} references missing source {:?}",
                    view.name, incremental.source_table
                ))
            })?;
            if source.table_id != incremental.source_table_id
                || source
                    .schema
                    .columns
                    .iter()
                    .all(|column| column.id != incremental.group_column)
            {
                return Err(MongrelError::Schema(format!(
                    "materialized view {:?} has invalid incremental source",
                    view.name
                )));
            }
            let target_ids = target
                .schema
                .columns
                .iter()
                .map(|column| column.id)
                .collect::<HashSet<_>>();
            let mut output_ids = HashSet::new();
            let count_outputs = incremental
                .outputs
                .iter()
                .filter(|output| {
                    matches!(output.kind, crate::catalog::IncrementalAggregateKind::Count)
                })
                .count();
            if incremental.checkpoint_event_id.is_empty()
                || !target_ids.contains(&incremental.group_output_column)
                || !target_ids.contains(&incremental.count_output_column)
                || incremental.outputs.is_empty()
                || count_outputs != 1
                || incremental.outputs.iter().any(|output| {
                    !target_ids.contains(&output.output_column)
                        || output.output_column == incremental.group_output_column
                        || !output_ids.insert(output.output_column)
                        || matches!(output.kind, crate::catalog::IncrementalAggregateKind::Count)
                            && output.output_column != incremental.count_output_column
                        || match output.kind {
                            crate::catalog::IncrementalAggregateKind::Sum { source_column } => {
                                source
                                    .schema
                                    .columns
                                    .iter()
                                    .all(|column| column.id != source_column)
                            }
                            crate::catalog::IncrementalAggregateKind::Count => false,
                        }
                })
            {
                return Err(MongrelError::Schema(format!(
                    "materialized view {:?} has invalid incremental outputs",
                    view.name
                )));
            }
        }
    }

    validate_security_catalog(catalog, &catalog.security)?;
    validate_recovered_auth_catalog(catalog)?;
    Ok(())
}

fn repair_catalog_allocator_counters(catalog: &mut Catalog) -> Result<bool> {
    let mut changed = false;
    if let Some(maximum) = catalog.tables.iter().map(|entry| entry.table_id).max() {
        let required = maximum
            .checked_add(1)
            .ok_or_else(|| MongrelError::Full("table id namespace exhausted".into()))?;
        if catalog.next_table_id < required {
            catalog.next_table_id = required;
            changed = true;
        }
    }
    if let Some(maximum) = catalog.users.iter().map(|user| user.id).max() {
        let required = maximum
            .checked_add(1)
            .ok_or_else(|| MongrelError::Full("user id namespace exhausted".into()))?;
        if catalog.next_user_id < required {
            catalog.next_user_id = required;
            changed = true;
        }
    }
    Ok(changed)
}

fn validate_foreign_key_targets(catalog: &Catalog, schema: &Schema) -> Result<()> {
    for foreign_key in &schema.constraints.foreign_keys {
        let parent = catalog.live(&foreign_key.ref_table).ok_or_else(|| {
            MongrelError::Schema(format!(
                "foreign key {:?} references unknown live table {:?}",
                foreign_key.name, foreign_key.ref_table
            ))
        })?;
        let referenced_unique = parent
            .schema
            .constraints
            .uniques
            .iter()
            .any(|unique| unique.columns == foreign_key.ref_columns)
            || foreign_key.ref_columns.len() == 1
                && parent
                    .schema
                    .primary_key()
                    .is_some_and(|column| column.id == foreign_key.ref_columns[0]);
        if !referenced_unique {
            return Err(MongrelError::Schema(format!(
                "foreign key {:?} does not reference a unique key",
                foreign_key.name
            )));
        }
        for (local_id, parent_id) in foreign_key.columns.iter().zip(&foreign_key.ref_columns) {
            let local = schema.columns.iter().find(|column| column.id == *local_id);
            let referenced = parent
                .schema
                .columns
                .iter()
                .find(|column| column.id == *parent_id);
            if local
                .zip(referenced)
                .is_none_or(|(local, referenced)| local.ty != referenced.ty)
            {
                return Err(MongrelError::Schema(format!(
                    "foreign key {:?} has missing or incompatible columns",
                    foreign_key.name
                )));
            }
        }
    }
    Ok(())
}

fn validate_recovered_procedure_references(
    catalog: &Catalog,
    procedure: &StoredProcedure,
) -> Result<()> {
    for step in &procedure.body.steps {
        let Some(table_name) = step.table() else {
            continue;
        };
        let schema = &catalog
            .live(table_name)
            .ok_or_else(|| {
                MongrelError::Schema(format!(
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
                for id in projection.iter().flatten() {
                    validate_column_id(*id, schema)?;
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
                for cell in cells.iter().chain(update_cells.iter().flatten()) {
                    validate_column_id(cell.column_id, schema)?;
                }
            }
            ProcedureStep::DeleteByPk { .. } if schema.primary_key().is_none() => {
                return Err(MongrelError::Schema(format!(
                    "procedure {:?} deletes by primary key on table without one",
                    procedure.name
                )));
            }
            ProcedureStep::DeleteByPk { .. }
            | ProcedureStep::DeleteRows { .. }
            | ProcedureStep::SqlQuery { .. } => {}
        }
    }
    Ok(())
}

fn validate_recovered_trigger_references(catalog: &Catalog, trigger: &StoredTrigger) -> Result<()> {
    let target_schema = match &trigger.target {
        TriggerTarget::Table(name) => catalog
            .live(name)
            .ok_or_else(|| {
                MongrelError::Schema(format!(
                    "trigger {:?} references unknown table {name:?}",
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
    for column in &trigger.update_of {
        if target_schema.column(column).is_none() {
            return Err(MongrelError::Schema(format!(
                "trigger {:?} references unknown UPDATE OF column {column:?}",
                trigger.name
            )));
        }
    }
    if let Some(expr) = &trigger.when {
        validate_trigger_expr(expr, &target_schema, trigger.event)?;
    }
    let mut selects = HashMap::new();
    for step in &trigger.program.steps {
        if matches!(step, TriggerStep::SetNew { .. }) && trigger.timing != TriggerTiming::Before {
            return Err(MongrelError::Schema(
                "SetNew is only valid in BEFORE triggers".into(),
            ));
        }
        validate_trigger_step(step, catalog, &target_schema, trigger.event, &mut selects)?;
    }
    Ok(())
}

fn validate_recovered_auth_catalog(catalog: &Catalog) -> Result<()> {
    let mut role_names = HashSet::new();
    for role in &catalog.roles {
        if role.name.is_empty()
            || role.created_epoch > catalog.db_epoch
            || !role_names.insert(role.name.as_str())
        {
            return Err(MongrelError::Schema(format!(
                "invalid or duplicate role {:?}",
                role.name
            )));
        }
        for permission in &role.permissions {
            if let Some(table) = permission_table(permission) {
                let schema = catalog
                    .live(table)
                    .map(|entry| &entry.schema)
                    .or_else(|| {
                        catalog
                            .external_tables
                            .iter()
                            .find(|entry| entry.name == table)
                            .map(|entry| &entry.declared_schema)
                    })
                    .ok_or_else(|| {
                        MongrelError::Schema(format!(
                            "role {:?} references unknown table {table:?}",
                            role.name
                        ))
                    })?;
                let columns = match permission {
                    crate::auth::Permission::SelectColumns { columns, .. }
                    | crate::auth::Permission::InsertColumns { columns, .. }
                    | crate::auth::Permission::UpdateColumns { columns, .. } => Some(columns),
                    _ => None,
                };
                if columns.is_some_and(|columns| {
                    columns.is_empty()
                        || columns.iter().any(|column| schema.column(column).is_none())
                }) {
                    return Err(MongrelError::Schema(format!(
                        "role {:?} contains invalid column permissions",
                        role.name
                    )));
                }
            }
        }
    }
    let mut user_ids = HashSet::new();
    let mut usernames = HashSet::new();
    let mut maximum_user_id = 0;
    for user in &catalog.users {
        maximum_user_id = maximum_user_id.max(user.id);
        if user.id == 0
            || user.username.is_empty()
            || user.password_hash.is_empty()
            || user.created_epoch > catalog.db_epoch
            || !user_ids.insert(user.id)
            || !usernames.insert(user.username.as_str())
            || user
                .roles
                .iter()
                .any(|role| !role_names.contains(role.as_str()))
        {
            return Err(MongrelError::Schema(format!(
                "invalid or duplicate user {:?}",
                user.username
            )));
        }
    }
    if !catalog.users.is_empty() && catalog.next_user_id <= maximum_user_id {
        return Err(MongrelError::Schema(
            "catalog next_user_id does not advance beyond existing user ids".into(),
        ));
    }
    if catalog.require_auth && !catalog.users.iter().any(|user| user.is_admin) {
        return Err(MongrelError::Schema(
            "authenticated catalog has no administrator".into(),
        ));
    }
    Ok(())
}

fn validate_recovered_storage_plan(
    root: &Path,
    durable_root: Option<&crate::durable_file::DurableRoot>,
    catalog: &Catalog,
    created_table_ids: &HashSet<u64>,
    ttl_updates: &HashMap<u64, (Option<crate::manifest::TtlPolicy>, u64)>,
    meta_dek: Option<&[u8; META_DEK_LEN]>,
) -> Result<Vec<u64>> {
    const MAX_SCHEMA_BYTES: u64 = 16 * 1024 * 1024;
    let mut reconcile = Vec::new();
    for entry in &catalog.tables {
        if !matches!(entry.state, TableState::Live | TableState::Building { .. }) {
            continue;
        }
        let relative_dir = Path::new(TABLES_DIR).join(entry.table_id.to_string());
        let table_dir = root.join(TABLES_DIR).join(entry.table_id.to_string());
        let table_exists = match durable_root {
            Some(root) => match root.open_directory(&relative_dir) {
                Ok(_) => true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                Err(error) => return Err(error.into()),
            },
            None => table_dir.is_dir(),
        };
        if !table_exists {
            if created_table_ids.contains(&entry.table_id) {
                reconcile.push(entry.table_id);
                continue;
            }
            return Err(MongrelError::NotFound(format!(
                "catalog table {} storage is missing",
                entry.table_id
            )));
        }
        let manifest_result = match durable_root {
            Some(root) => crate::manifest::read_durable(root, &relative_dir, meta_dek),
            None => crate::manifest::read(&table_dir, meta_dek),
        };
        let manifest = match manifest_result {
            Ok(manifest) => manifest,
            Err(MongrelError::Io(error))
                if created_table_ids.contains(&entry.table_id)
                    && error.kind() == std::io::ErrorKind::NotFound =>
            {
                reconcile.push(entry.table_id);
                continue;
            }
            Err(error) => return Err(error),
        };
        if manifest.table_id != entry.table_id {
            return Err(MongrelError::Conflict(format!(
                "catalog table {} storage identity mismatch",
                entry.table_id
            )));
        }
        let schema_result = match durable_root {
            Some(root) => root
                .open_regular(relative_dir.join(crate::engine::SCHEMA_FILENAME))
                .map_err(MongrelError::from),
            None => crate::durable_file::open_regular_nofollow(
                &table_dir.join(crate::engine::SCHEMA_FILENAME),
            ),
        };
        let file = match schema_result {
            Ok(file) => file,
            Err(MongrelError::Io(error))
                if created_table_ids.contains(&entry.table_id)
                    && error.kind() == std::io::ErrorKind::NotFound =>
            {
                reconcile.push(entry.table_id);
                continue;
            }
            Err(error) => return Err(error),
        };
        let length = file.metadata()?.len();
        if length > MAX_SCHEMA_BYTES {
            return Err(MongrelError::ResourceLimitExceeded {
                resource: "recovered schema bytes",
                requested: usize::try_from(length).unwrap_or(usize::MAX),
                limit: MAX_SCHEMA_BYTES as usize,
            });
        }
        let disk_schema: Schema = serde_json::from_reader(file.take(MAX_SCHEMA_BYTES + 1))
            .map_err(|error| MongrelError::Schema(format!("decode recovered schema: {error}")))?;
        if manifest.schema_id != entry.schema.schema_id
            || crate::wal::DdlOp::encode_schema(&disk_schema)?
                != crate::wal::DdlOp::encode_schema(&entry.schema)?
        {
            reconcile.push(entry.table_id);
        }
    }
    for table_id in ttl_updates.keys() {
        if !catalog.tables.iter().any(|entry| {
            entry.table_id == *table_id
                && matches!(entry.state, TableState::Live | TableState::Building { .. })
        }) {
            continue;
        }
        let relative_dir = Path::new(TABLES_DIR).join(table_id.to_string());
        let table_exists = match durable_root {
            Some(root) => match root.open_directory(&relative_dir) {
                Ok(_) => true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                Err(error) => return Err(error.into()),
            },
            None => root.join(&relative_dir).is_dir(),
        };
        if !table_exists && !created_table_ids.contains(table_id) {
            return Err(MongrelError::NotFound(format!(
                "TTL recovery table {table_id} storage is missing"
            )));
        }
    }
    reconcile.sort_unstable();
    reconcile.dedup();
    Ok(reconcile)
}

fn validate_catalog_table_storage(
    root: &crate::durable_file::DurableRoot,
    catalog: &Catalog,
    meta_dek: Option<&[u8; META_DEK_LEN]>,
) -> Result<()> {
    for entry in &catalog.tables {
        if !matches!(entry.state, TableState::Live | TableState::Building { .. }) {
            continue;
        }
        let table_dir = Path::new(TABLES_DIR).join(entry.table_id.to_string());
        let manifest = crate::manifest::read_durable(root, &table_dir, meta_dek)?;
        if manifest.table_id != entry.table_id || manifest.schema_id != entry.schema.schema_id {
            return Err(MongrelError::Conflict(format!(
                "catalog table {} storage identity mismatch",
                entry.table_id
            )));
        }
        root.open_regular(table_dir.join(crate::engine::SCHEMA_FILENAME))?;
    }
    Ok(())
}

fn apply_recovered_column_def(schema: &mut Schema, column: ColumnDef) -> Result<bool> {
    match schema.columns.iter_mut().find(|c| c.id == column.id) {
        Some(existing) if *existing == column => Ok(false),
        Some(existing) => {
            *existing = column;
            schema.schema_id = schema
                .schema_id
                .checked_add(1)
                .ok_or_else(|| MongrelError::Schema("schema id space exhausted".into()))?;
            Ok(true)
        }
        None => {
            schema.columns.push(column);
            schema.schema_id = schema
                .schema_id
                .checked_add(1)
                .ok_or_else(|| MongrelError::Schema("schema id space exhausted".into()))?;
            Ok(true)
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

fn apply_rebuilding_publish(
    catalog: &mut Catalog,
    table_id: u64,
    replaced_table_id: u64,
    new_name: &str,
    epoch: u64,
) -> Result<bool> {
    let already_published = catalog.tables.iter().any(|entry| {
        entry.table_id == table_id
            && entry.name == new_name
            && matches!(entry.state, TableState::Live)
    }) && catalog.tables.iter().any(|entry| {
        entry.table_id == replaced_table_id && matches!(entry.state, TableState::Dropped { .. })
    });
    if already_published {
        return Ok(false);
    }
    let schema = catalog
        .tables
        .iter()
        .find(|entry| entry.table_id == table_id)
        .ok_or_else(|| MongrelError::NotFound(format!("table id {table_id} not found")))?
        .schema
        .clone();
    let replaced = catalog
        .tables
        .iter_mut()
        .find(|entry| entry.table_id == replaced_table_id)
        .ok_or_else(|| MongrelError::NotFound(format!("table id {replaced_table_id} not found")))?;
    replaced.state = TableState::Dropped { at_epoch: epoch };
    let replacement = catalog
        .tables
        .iter_mut()
        .find(|entry| entry.table_id == table_id)
        .ok_or_else(|| MongrelError::NotFound(format!("table id {table_id} not found")))?;
    replacement.name = new_name.to_string();
    replacement.state = TableState::Live;

    for role in &mut catalog.roles {
        role.permissions.retain_mut(|permission| {
            retain_rebuilt_permission_columns(permission, new_name, &schema)
        });
    }
    for definition in &mut catalog.materialized_views {
        if let Some(incremental) = definition.incremental.as_mut() {
            if incremental.source_table == new_name
                && incremental.source_table_id == replaced_table_id
            {
                incremental.source_table_id = table_id;
            }
        }
    }
    advance_security_version(catalog)?;
    Ok(true)
}

fn retain_rebuilt_permission_columns(
    permission: &mut crate::auth::Permission,
    target_table: &str,
    schema: &Schema,
) -> bool {
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
    if let Some(columns) = columns {
        columns.retain(|column| schema.column(column).is_some());
        !columns.is_empty()
    } else {
        true
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
    let permission = if kind == 0 {
        Permission::SelectColumns { table, columns }
    } else if kind == 1 {
        Permission::InsertColumns { table, columns }
    } else {
        Permission::UpdateColumns { table, columns }
    };
    permissions.push(permission);
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

/// Remove canonical numeric table directories that no catalog generation owns.
fn sweep_unreferenced_table_dirs(root: &Path, cat: &Catalog) -> Result<()> {
    let referenced = cat
        .tables
        .iter()
        .filter(|entry| matches!(entry.state, TableState::Live | TableState::Building { .. }))
        .map(|entry| entry.table_id)
        .collect::<HashSet<_>>();
    let tables_dir = root.join(TABLES_DIR);
    let entries = match std::fs::read_dir(&tables_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        let Ok(table_id) = name.parse::<u64>() else {
            continue;
        };
        if name != table_id.to_string() {
            continue;
        }
        if !referenced.contains(&table_id) {
            crate::durable_file::remove_directory_all(&entry.path())?;
        }
    }
    Ok(())
}

/// Sweep stale `_txn/<txn_id>/` dirs from every table (spec §8.5, review fix
/// #14). These dirs hold pending uniform-epoch runs from large transactions
/// that were aborted or crashed before commit. On open, all such dirs are safe
/// to remove because committed txns moved their runs to `_runs/` at publish.
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
mod write_permission_tests {
    use super::*;
    use crate::txn::Staged;

    struct NoopExternalBridge;

    impl ExternalTriggerBridge for NoopExternalBridge {
        fn apply_trigger_external_write(
            &self,
            _entry: &ExternalTableEntry,
            base_state: Vec<u8>,
            _op: ExternalTriggerWrite,
        ) -> Result<ExternalTriggerWriteResult> {
            Ok(ExternalTriggerWriteResult::new(base_state))
        }
    }

    fn assert_txn_namespace_full<T>(result: Result<T>) {
        assert!(matches!(result, Err(MongrelError::Full(_))));
    }

    #[test]
    fn every_begin_api_preserves_transaction_id_exhaustion_without_wal_mutation() {
        let directory = tempfile::tempdir().unwrap();
        let database = Database::create(directory.path()).unwrap();
        let generation = (*database.next_txn_id.lock() >> 32).saturating_add(1);
        *database.next_txn_id.lock() = generation << 32;
        let before = crate::wal::SharedWal::replay(directory.path())
            .unwrap()
            .len();
        let bridge = NoopExternalBridge;

        assert_txn_namespace_full(database.begin().commit());
        assert_txn_namespace_full(database.begin_as(None).commit_with_row_ids());
        assert_txn_namespace_full(
            database
                .begin_with_isolation(crate::txn::IsolationLevel::Serializable)
                .commit(),
        );
        assert_txn_namespace_full(
            database
                .begin_with_external_trigger_bridge(&bridge)
                .commit(),
        );
        assert_txn_namespace_full(
            database
                .begin_with_external_trigger_bridge_as(&bridge, None)
                .commit_controlled(&crate::ExecutionControl::new(None), || Ok(())),
        );

        assert_eq!(
            crate::wal::SharedWal::replay(directory.path())
                .unwrap()
                .len(),
            before
        );
        drop(database);
        Database::open(directory.path()).unwrap();
    }

    #[test]
    fn recovered_storage_identity_mismatch_does_not_mutate_directory() {
        let directory = tempfile::tempdir().unwrap();
        let table_dir = directory.path().join("7");
        crate::durable_file::create_directory_all(&table_dir).unwrap();
        let original_schema = test_schema();
        crate::engine::write_schema(&table_dir, &original_schema).unwrap();
        let mut manifest = crate::manifest::Manifest::new(8, original_schema.schema_id);
        crate::manifest::write_atomic(&table_dir, &mut manifest, None).unwrap();
        let schema_path = table_dir.join(crate::engine::SCHEMA_FILENAME);
        let original_bytes = std::fs::read(&schema_path).unwrap();

        let mut replacement_schema = original_schema;
        replacement_schema.schema_id += 1;
        assert!(matches!(
            ensure_recovered_table_storage(None, None, &table_dir, 7, &replacement_schema, None,),
            Err(MongrelError::Conflict(_))
        ));

        assert_eq!(std::fs::read(schema_path).unwrap(), original_bytes);
        assert!(!table_dir.join(crate::engine::WAL_DIR).exists());
        assert!(!table_dir.join(crate::engine::RUNS_DIR).exists());
        assert_eq!(crate::manifest::read(&table_dir, None).unwrap().table_id, 8);
    }

    #[test]
    fn catalog_table_missing_storage_fails_without_recreating_it() {
        let directory = tempfile::tempdir().unwrap();
        let table_dir = {
            let database = Database::create(directory.path()).unwrap();
            database.create_table("docs", test_schema()).unwrap();
            directory
                .path()
                .join(TABLES_DIR)
                .join(database.table_id("docs").unwrap().to_string())
        };
        std::fs::remove_dir_all(&table_dir).unwrap();

        assert!(matches!(
            Database::open(directory.path()),
            Err(MongrelError::NotFound(_))
        ));
        assert!(!table_dir.exists());
    }

    #[test]
    fn authentication_and_principal_resolution_share_one_catalog_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let database = std::sync::Arc::new(
            Database::create_with_credentials(directory.path(), "admin", "admin-password").unwrap(),
        );
        database.create_user("alice", "old-password").unwrap();
        let old_identity = database.user_identity("alice").unwrap();
        let (verified_tx, verified_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let (mutation_started_tx, mutation_started_rx) = std::sync::mpsc::channel();
        let (mutation_done_tx, mutation_done_rx) = std::sync::mpsc::channel();

        std::thread::scope(|scope| {
            let authenticate = {
                let database = std::sync::Arc::clone(&database);
                scope.spawn(move || {
                    database.authenticate_principal_inner("alice", "old-password", || {
                        verified_tx.send(()).unwrap();
                        resume_rx.recv().unwrap();
                    })
                })
            };
            verified_rx.recv().unwrap();
            let mutate = {
                let database = std::sync::Arc::clone(&database);
                scope.spawn(move || {
                    mutation_started_tx.send(()).unwrap();
                    database.drop_user("alice").unwrap();
                    database.create_user("alice", "new-password").unwrap();
                    mutation_done_tx.send(()).unwrap();
                })
            };
            mutation_started_rx.recv().unwrap();
            assert!(mutation_done_rx
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err());
            resume_tx.send(()).unwrap();
            let principal = authenticate.join().unwrap().unwrap().unwrap();
            assert_eq!((principal.user_id, principal.created_epoch), old_identity);
            mutate.join().unwrap();
        });

        assert_ne!(database.user_identity("alice").unwrap(), old_identity);
        assert!(database
            .authenticate_principal("alice", "old-password")
            .unwrap()
            .is_none());
        assert!(database
            .authenticate_principal("alice", "new-password")
            .unwrap()
            .is_some());
    }

    #[test]
    fn homogeneous_batch_summarizes_to_one_permission_decision() {
        let staging = (0..10_050)
            .map(|_| {
                (
                    7,
                    Staged::Put(vec![(2, Value::Int64(2)), (1, Value::Int64(1))]),
                )
            })
            .collect::<Vec<_>>();

        let needs = summarize_write_permissions(&staging);
        let table = needs.get(&7).unwrap();
        assert_eq!(needs.len(), 1);
        assert!(table.insert);
        assert_eq!(table.insert_columns, [1, 2]);
        assert!(!table.update);
        assert!(!table.delete);
        assert!(!table.truncate);
    }

    #[test]
    fn mixed_writes_union_columns_and_preserve_empty_operations() {
        let staging = vec![
            (7, Staged::Put(vec![(2, Value::Int64(2))])),
            (7, Staged::Put(vec![(1, Value::Int64(1))])),
            (
                7,
                Staged::Update {
                    row_id: RowId(1),
                    new_row: vec![(1, Value::Int64(1)), (2, Value::Int64(2))],
                    changed_columns: vec![2],
                },
            ),
            (7, Staged::Delete(RowId(2))),
            (8, Staged::Truncate),
        ];

        let needs = summarize_write_permissions(&staging);
        let table = needs.get(&7).unwrap();
        assert_eq!(table.insert_columns, [1, 2]);
        assert!(table.update);
        assert_eq!(table.update_columns, [2]);
        assert!(table.delete);
        assert!(needs.get(&8).unwrap().truncate);
    }

    #[test]
    fn final_permission_decisions_do_not_scale_with_rows() {
        let credentialless_dir = tempfile::tempdir().unwrap();
        let credentialless = Database::create(credentialless_dir.path()).unwrap();
        credentialless.create_table("docs", test_schema()).unwrap();
        WRITE_PERMISSION_DECISIONS.with(|decisions| decisions.set(0));
        credentialless
            .validate_write_permissions(&puts(credentialless.table_id("docs").unwrap()), None, None)
            .unwrap();
        WRITE_PERMISSION_DECISIONS.with(|decisions| assert_eq!(decisions.get(), 0));

        let authenticated_dir = tempfile::tempdir().unwrap();
        let authenticated =
            Database::create_with_credentials(authenticated_dir.path(), "admin", "admin-password")
                .unwrap();
        authenticated.create_table("docs", test_schema()).unwrap();
        let admin = authenticated.resolve_principal("admin").unwrap();
        WRITE_PERMISSION_DECISIONS.with(|decisions| decisions.set(0));
        authenticated
            .validate_write_permissions(
                &puts(authenticated.table_id("docs").unwrap()),
                Some(&admin),
                None,
            )
            .unwrap();
        WRITE_PERMISSION_DECISIONS.with(|decisions| assert_eq!(decisions.get(), 1));
    }

    #[test]
    fn delete_batch_checks_permission_once_when_staged_and_once_when_committed() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create_with_credentials(dir.path(), "admin", "admin-password").unwrap();
        db.create_table("docs", test_schema()).unwrap();
        let admin = db.resolve_principal("admin").unwrap();
        TABLE_PERMISSION_DECISIONS.with(|decisions| decisions.set(0));

        let mut transaction = db.begin_as(Some(admin));
        transaction
            .delete_batch("docs", (0..100).map(RowId).collect())
            .unwrap();
        transaction.commit().unwrap();

        TABLE_PERMISSION_DECISIONS.with(|decisions| assert_eq!(decisions.get(), 2));
    }

    #[test]
    fn truncate_validation_checks_admin_once_for_all_tables() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create_with_credentials(dir.path(), "admin", "admin-password").unwrap();
        db.create_table("first", test_schema()).unwrap();
        db.create_table("second", test_schema()).unwrap();
        let admin = db.resolve_principal("admin").unwrap();
        let staging = vec![
            (db.table_id("first").unwrap(), Staged::Truncate),
            (db.table_id("second").unwrap(), Staged::Truncate),
        ];

        TABLE_PERMISSION_DECISIONS.with(|decisions| decisions.set(0));
        db.validate_write_permissions(&staging, Some(&admin), None)
            .unwrap();
        TABLE_PERMISSION_DECISIONS.with(|decisions| assert_eq!(decisions.get(), 1));
    }

    #[test]
    fn one_table_commit_batches_structural_work() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path()).unwrap();
        db.create_table("docs", test_schema()).unwrap();
        let table_id = db.table_id("docs").unwrap();

        AUTO_INCREMENT_TABLE_LOCKS.with(|count| count.set(0));
        PREBUILD_TABLE_LOCKS.with(|count| count.set(0));
        PUBLISH_TABLE_LOCKS.with(|count| count.set(0));
        COMMIT_MANIFEST_WRITES.with(|count| count.set(0));
        db.transaction(|transaction| {
            for id in 0..100 {
                transaction.put("docs", vec![(1, Value::Int64(id))])?;
            }
            Ok(())
        })
        .unwrap();

        AUTO_INCREMENT_TABLE_LOCKS.with(|count| assert_eq!(count.get(), 2));
        PREBUILD_TABLE_LOCKS.with(|count| assert_eq!(count.get(), 1));
        PUBLISH_TABLE_LOCKS.with(|count| assert_eq!(count.get(), 1));
        COMMIT_MANIFEST_WRITES.with(|count| assert_eq!(count.get(), 1));

        let puts = crate::wal::SharedWal::replay(dir.path())
            .unwrap()
            .into_iter()
            .filter_map(|record| match record.op {
                crate::wal::Op::Put { table_id: id, rows } if id == table_id => Some(
                    bincode::deserialize::<Vec<crate::memtable::Row>>(&rows)
                        .unwrap()
                        .len(),
                ),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(puts, [100]);

        let row_ids = db
            .table("docs")
            .unwrap()
            .lock()
            .visible_rows(db.snapshot().0)
            .unwrap()
            .into_iter()
            .take(2)
            .map(|row| row.row_id)
            .collect::<Vec<_>>();
        PREBUILD_TABLE_LOCKS.with(|count| count.set(0));
        PUBLISH_TABLE_LOCKS.with(|count| count.set(0));
        COMMIT_MANIFEST_WRITES.with(|count| count.set(0));
        db.transaction(|transaction| {
            for row_id in row_ids {
                transaction.delete("docs", row_id)?;
            }
            Ok(())
        })
        .unwrap();
        PREBUILD_TABLE_LOCKS.with(|count| assert_eq!(count.get(), 1));
        PUBLISH_TABLE_LOCKS.with(|count| assert_eq!(count.get(), 1));
        COMMIT_MANIFEST_WRITES.with(|count| assert_eq!(count.get(), 1));

        let deletes = crate::wal::SharedWal::replay(dir.path())
            .unwrap()
            .into_iter()
            .filter_map(|record| match record.op {
                crate::wal::Op::Delete {
                    table_id: id,
                    row_ids,
                } if id == table_id => Some(row_ids.len()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(deletes, [2]);
    }

    fn puts(table_id: u64) -> Vec<(u64, Staged)> {
        (0..10_050)
            .map(|id| (table_id, Staged::Put(vec![(1, Value::Int64(id))])))
            .collect()
    }

    fn test_schema() -> Schema {
        Schema {
            columns: vec![ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: crate::schema::ColumnFlags::empty()
                    .with(crate::schema::ColumnFlags::PRIMARY_KEY),
                default_value: None,
            }],
            ..Schema::default()
        }
    }
}

#[cfg(test)]
mod cdc_bounds_tests {
    use super::*;

    #[test]
    fn retained_byte_limit_rejects_without_allocating_payload() {
        let mut retained = 0;
        let error = charge_cdc_bytes(
            &mut retained,
            CDC_MAX_RETAINED_BYTES.saturating_add(1),
            "CDC retained bytes",
        )
        .unwrap_err();
        assert!(matches!(
            error,
            MongrelError::ResourceLimitExceeded {
                resource: "CDC retained bytes",
                ..
            }
        ));
    }

    #[test]
    fn row_json_estimate_accounts_for_byte_array_expansion() {
        let row = crate::memtable::Row::new(RowId(1), Epoch(1))
            .with_column(1, Value::Bytes(vec![0; 1024]));
        assert!(cdc_row_json_bytes(&row) >= 1024 * std::mem::size_of::<serde_json::Value>());
    }
}

#[cfg(test)]
mod generation_metrics_tests {
    use super::*;
    use crate::schema::{ColumnDef, ColumnFlags, Schema, TypeId};

    #[test]
    fn legacy_cow_fallback_is_measured() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::create(
            dir.path(),
            Schema {
                columns: vec![ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                    default_value: None,
                }],
                ..Schema::default()
            },
            1,
        )
        .unwrap();
        let handle = TableHandle::from_table(table);
        let held = match &handle.inner {
            TableHandleInner::CopyOnWrite(slot) => Arc::clone(&slot.read()),
            TableHandleInner::Direct(_) => unreachable!(),
        };

        handle.lock().set_sync_byte_threshold(1);

        let stats = handle.generation_stats();
        assert_eq!(stats.cow_clone_count, 1);
        assert!(stats.estimated_cow_clone_bytes > 0);
        drop(held);
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
