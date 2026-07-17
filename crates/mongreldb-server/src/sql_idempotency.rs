use fs2::FileExt;
use hmac::Mac as _;
use mongreldb_core::durable_file::DurableRoot;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use zeroize::Zeroizing;

// v5 replaces the forgeable v4 checksum with a keyed MAC. Older entries are
// deliberately unreadable and remain fail-closed outcome-unknown markers.
const ENTRY_VERSION: u8 = 5;
const LOCK_STRIPES: usize = 64;
pub(crate) const CAPACITY_LOCK_FILE: &str = ".capacity.lock";
pub(crate) const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;
const INTEGRITY_KEY_FILE: &str = "_meta/server-idempotency.key";
const MAX_ENTRY_BYTES: u64 = 16 * 1024 * 1024;
const SQL_INTENT_MAC_DOMAIN: &[u8] = b"mongreldb/server/sql-idempotency/intent/v5\0";
const SQL_RECEIPT_MAC_DOMAIN: &[u8] = b"mongreldb/server/sql-idempotency/receipt/v5\0";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SqlIdempotencyBinding {
    pub(crate) sql_fingerprint: [u8; 32],
    pub(crate) parameter_hash: [u8; 32],
    pub(crate) request_semantics_hash: [u8; 32],
    pub(crate) session_semantics_hash: [u8; 32],
    /// Server-enforced key/receipt retention policy. A retry under a different
    /// expiry policy is not the same idempotent request.
    pub(crate) expires_after_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SqlReceiptOutcome {
    pub(crate) committed: bool,
    pub(crate) committed_statements: usize,
    pub(crate) last_commit_epoch: Option<u64>,
    pub(crate) last_commit_epoch_text: Option<String>,
    pub(crate) first_commit_statement_index: Option<usize>,
    pub(crate) last_commit_statement_index: Option<usize>,
    pub(crate) completed_statements: usize,
    pub(crate) statement_index: usize,
    pub(crate) serialization: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SqlReceiptTerminalError {
    pub(crate) code: String,
    pub(crate) category: String,
}

/// The core commit log's irrevocable receipt for one committed idempotent
/// write (spec §10.2 S1B-005), recorded through the durable `TXN_IDEMPOTENCY`
/// ledger and echoed back additively in receipt responses. Contains no secret
/// material: ids, the HLC commit timestamp, the log position, and the
/// durability level only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SqlCommitReceipt {
    /// 128-bit transaction id, canonical lowercase hex.
    pub(crate) transaction_id: String,
    pub(crate) commit_ts_physical_micros: u64,
    pub(crate) commit_ts_logical: u32,
    pub(crate) commit_ts_node_tiebreaker: u32,
    pub(crate) log_term: u64,
    pub(crate) log_index: u64,
    pub(crate) durability: String,
}

impl SqlCommitReceipt {
    pub(crate) fn from_core(receipt: &mongreldb_log::CommitReceipt) -> Self {
        let durability = match receipt.durability {
            mongreldb_log::DurabilityLevel::GroupCommit => "group_commit",
            mongreldb_log::DurabilityLevel::LeaderDisk => "leader_disk",
            mongreldb_log::DurabilityLevel::Quorum => "quorum",
        };
        Self {
            transaction_id: receipt.transaction_id.to_hex(),
            commit_ts_physical_micros: receipt.commit_ts.physical_micros,
            commit_ts_logical: receipt.commit_ts.logical,
            commit_ts_node_tiebreaker: receipt.commit_ts.node_tiebreaker,
            log_term: receipt.log_position.term,
            log_index: receipt.log_position.index,
            durability: durability.to_owned(),
        }
    }

    /// The receipt's HLC commit timestamp (the read-your-writes token type of
    /// the canonical session record, S1D-004).
    pub(crate) fn commit_ts(&self) -> mongreldb_types::hlc::HlcTimestamp {
        mongreldb_types::hlc::HlcTimestamp {
            physical_micros: self.commit_ts_physical_micros,
            logical: self.commit_ts_logical,
            node_tiebreaker: self.commit_ts_node_tiebreaker,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SqlDurableReceipt {
    pub(crate) original_query_id: String,
    pub(crate) status: String,
    pub(crate) server_state: String,
    pub(crate) cancellation_reason: String,
    pub(crate) outcome: SqlReceiptOutcome,
    pub(crate) terminal_error: Option<SqlReceiptTerminalError>,
    /// Additive (format-v5 compatible): present for receipts recorded after
    /// the core-ledger unification landed. `skip_serializing_if` keeps the
    /// byte form of pre-unification receipts identical so their
    /// authentication tags still verify.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) commit_receipt: Option<SqlCommitReceipt>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedIntent {
    version: u8,
    scope_hash: [u8; 32],
    owner_hash: [u8; 32],
    created_at_ms: u64,
    binding: SqlIdempotencyBinding,
    authentication: [u8; 32],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedReceipt {
    version: u8,
    scope_hash: [u8; 32],
    owner_hash: [u8; 32],
    expires_at_ms: u64,
    binding: SqlIdempotencyBinding,
    receipt: SqlDurableReceipt,
    authentication: [u8; 32],
}

pub(crate) enum BeginResult {
    Replay {
        receipt: SqlDurableReceipt,
        expires_at_ms: u64,
    },
    Execute(SqlIdempotencyExecution),
    Mismatch,
    /// A durable intent exists without a durable receipt. The previous process
    /// may have committed, so retrying the write would be unsafe.
    Indeterminate {
        created_at_ms: Option<u64>,
    },
    Full,
    Unavailable,
}

pub(crate) struct SqlIdempotencyStore {
    files: StoreFiles,
    integrity: Option<Arc<IdempotencyIntegrity>>,
    ttl: Duration,
    max_entries: usize,
    max_entries_per_owner: usize,
    synchronization: Arc<StoreSynchronization>,
    available: AtomicBool,
}

pub(crate) struct StoreSynchronization {
    locks: Vec<Arc<AsyncMutex<()>>>,
    capacity_lock: AsyncMutex<()>,
    active_scopes: Mutex<HashMap<String, [u8; 32]>>,
}

impl StoreSynchronization {
    pub(crate) async fn lock_scope(&self, scope_hash: [u8; 32]) -> OwnedMutexGuard<()> {
        Arc::clone(&self.locks[usize::from(scope_hash[0]) % LOCK_STRIPES])
            .lock_owned()
            .await
    }

    pub(crate) async fn lock_capacity(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.capacity_lock.lock().await
    }
}

static STORE_SYNCHRONIZATION: OnceLock<Mutex<HashMap<PathBuf, Weak<StoreSynchronization>>>> =
    OnceLock::new();

enum ExistingEntry {
    Replay(Box<PersistedReceipt>),
    Intent(Box<PersistedIntent>),
    Corrupt,
    None,
}

#[derive(Clone)]
pub(crate) struct StoreFiles {
    root: Arc<DurableRoot>,
    directory: PathBuf,
}

impl StoreFiles {
    pub(crate) fn new(root: Arc<DurableRoot>, directory: impl Into<PathBuf>) -> Self {
        let directory = directory.into();
        Self { root, directory }
    }

    pub(crate) fn ensure_directory(&self) -> io::Result<()> {
        match self.root.create_directory_all(&self.directory) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                self.root.open_directory(&self.directory).map(|_| ())
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn path(&self) -> PathBuf {
        self.root.canonical_path().join(&self.directory)
    }

    fn relative(&self, name: impl AsRef<Path>) -> PathBuf {
        self.directory.join(name)
    }

    pub(crate) fn read(&self, name: impl AsRef<Path>) -> io::Result<Vec<u8>> {
        let file = self.root.open_regular(self.relative(name))?;
        let length = file.metadata()?.len();
        if length > MAX_ENTRY_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "idempotency entry exceeds maximum size",
            ));
        }
        let mut bytes = Vec::with_capacity(length as usize);
        file.take(MAX_ENTRY_BYTES.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_ENTRY_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "idempotency entry exceeds maximum size",
            ));
        }
        Ok(bytes)
    }

    pub(crate) fn write_new(&self, name: impl AsRef<Path>, bytes: &[u8]) -> io::Result<()> {
        self.root.write_new(self.relative(name), bytes)
    }

    pub(crate) fn write_atomic(&self, name: impl AsRef<Path>, bytes: &[u8]) -> io::Result<()> {
        self.root.write_atomic(self.relative(name), bytes)
    }

    fn open_lock(&self, name: impl AsRef<Path>) -> io::Result<std::fs::File> {
        self.root.open_lock_file(self.relative(name))
    }

    pub(crate) fn remove(&self, name: impl AsRef<Path>) -> io::Result<()> {
        self.root.remove_file(self.relative(name))
    }

    pub(crate) fn list(&self) -> io::Result<Vec<std::ffi::OsString>> {
        self.root.list_regular_files(&self.directory)
    }

    #[cfg(test)]
    pub(crate) fn absolute(&self, name: impl AsRef<Path>) -> PathBuf {
        self.path().join(name)
    }
}

pub(crate) struct IdempotencyIntegrity {
    key: Zeroizing<[u8; 32]>,
}

impl IdempotencyIntegrity {
    pub(crate) fn for_database(
        database: &mongreldb_core::Database,
    ) -> io::Result<(Arc<DurableRoot>, Arc<Self>)> {
        let root = database.durable_root();
        let key = match database.derive_server_idempotency_key() {
            Some(key) => key,
            None => load_or_create_plaintext_integrity_key(&root)?,
        };
        Ok((root, Arc::new(Self { key })))
    }

    #[cfg(test)]
    pub(crate) fn for_test_root(root: &Arc<DurableRoot>) -> Option<Arc<Self>> {
        load_or_create_plaintext_integrity_key(root)
            .ok()
            .map(|key| Arc::new(Self { key }))
    }

    pub(crate) fn authenticate(&self, domain: &[u8], bytes: &[u8]) -> [u8; 32] {
        let mut mac = <hmac::Hmac<Sha256> as hmac::Mac>::new_from_slice(self.key.as_ref())
            .expect("HMAC accepts a 32-byte key");
        mac.update(domain);
        mac.update(bytes);
        mac.finalize().into_bytes().into()
    }

    pub(crate) fn verify(&self, domain: &[u8], bytes: &[u8], tag: &[u8; 32]) -> bool {
        let mut mac = <hmac::Hmac<Sha256> as hmac::Mac>::new_from_slice(self.key.as_ref())
            .expect("HMAC accepts a 32-byte key");
        mac.update(domain);
        mac.update(bytes);
        mac.verify_slice(tag).is_ok()
    }
}

fn load_or_create_plaintext_integrity_key(root: &DurableRoot) -> io::Result<Zeroizing<[u8; 32]>> {
    match root.create_directory_all("_meta") {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            root.open_directory("_meta")?;
        }
        Err(error) => return Err(error),
    }
    match read_integrity_key(root) {
        Ok(key) => return Ok(key),
        Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
        Err(_) => {}
    }
    let mut key = Zeroizing::new([0u8; 32]);
    mongreldb_core::encryption::fill_random(key.as_mut())
        .map_err(|error| io::Error::other(error.to_string()))?;
    match root.write_new(INTEGRITY_KEY_FILE, key.as_ref()) {
        Ok(()) => Ok(key),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => read_integrity_key(root),
        Err(error) => Err(error),
    }
}

fn read_integrity_key(root: &DurableRoot) -> io::Result<Zeroizing<[u8; 32]>> {
    let mut file = root.open_regular(INTEGRITY_KEY_FILE)?;
    let mut key = Zeroizing::new([0u8; 32]);
    file.read_exact(key.as_mut())?;
    let mut trailing = [0u8; 1];
    if file.read(&mut trailing)? != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid server idempotency integrity key length",
        ));
    }
    Ok(key)
}

impl SqlIdempotencyStore {
    #[cfg(test)]
    pub(crate) fn new(root: &Path, ttl: Duration, max_entries: usize) -> Self {
        let root = Arc::new(DurableRoot::open(root).expect("temporary test root must open"));
        let integrity = load_or_create_plaintext_integrity_key(&root)
            .ok()
            .map(|key| Arc::new(IdempotencyIntegrity { key }));
        Self::new_with_integrity(root, integrity, ttl, max_entries)
    }

    pub(crate) fn new_with_integrity(
        root: Arc<DurableRoot>,
        integrity: Option<Arc<IdempotencyIntegrity>>,
        ttl: Duration,
        max_entries: usize,
    ) -> Self {
        let files = StoreFiles::new(root, "_sql_idempotency");
        let available = integrity.is_some() && files.ensure_directory().is_ok();
        let synchronization = synchronization_for(&files.path());
        Self {
            files,
            integrity,
            ttl,
            max_entries: max_entries.max(1),
            max_entries_per_owner: (max_entries.max(1) / 4).max(1),
            synchronization,
            available: AtomicBool::new(available),
        }
    }

    pub(crate) fn validate_key(key: &str) -> Result<(), &'static str> {
        if key.is_empty() {
            return Err("idempotency_key must not be empty");
        }
        if key.len() > MAX_IDEMPOTENCY_KEY_BYTES {
            return Err("idempotency_key must not exceed 256 bytes");
        }
        Ok(())
    }

    pub(crate) fn expires_after_ms(&self) -> u64 {
        duration_ms(self.ttl)
    }

    pub(crate) async fn begin(
        self: &Arc<Self>,
        owner: &str,
        key: &str,
        binding: SqlIdempotencyBinding,
    ) -> BeginResult {
        if !self.available.load(Ordering::Acquire) {
            if self.integrity.is_none() || self.files.ensure_directory().is_err() {
                return BeginResult::Unavailable;
            }
            self.available.store(true, Ordering::Release);
        }
        let scope_hash = scoped_hash(owner, key);
        let scope = hex(&scope_hash);
        let guard =
            Arc::clone(&self.synchronization.locks[usize::from(scope_hash[0]) % LOCK_STRIPES])
                .lock_owned()
                .await;
        let owner_hash = hash(owner.as_bytes());
        match self.read_entry(&scope, scope_hash, owner_hash) {
            ExistingEntry::Replay(entry) => {
                return if entry.binding == binding {
                    BeginResult::Replay {
                        receipt: entry.receipt,
                        expires_at_ms: entry.expires_at_ms,
                    }
                } else {
                    BeginResult::Mismatch
                };
            }
            ExistingEntry::Intent(intent) => {
                return if intent.binding == binding {
                    BeginResult::Indeterminate {
                        created_at_ms: Some(intent.created_at_ms),
                    }
                } else {
                    BeginResult::Mismatch
                };
            }
            ExistingEntry::Corrupt => {
                return BeginResult::Indeterminate {
                    created_at_ms: None,
                };
            }
            ExistingEntry::None => {}
        }
        let _capacity_guard = self.synchronization.capacity_lock.lock().await;
        let Ok(_capacity_file_guard) = CapacityFileGuard::acquire(&self.files).await else {
            return BeginResult::Unavailable;
        };
        self.prune_expired_receipts();
        match self.read_entry(&scope, scope_hash, owner_hash) {
            ExistingEntry::Replay(entry) => {
                return if entry.binding == binding {
                    BeginResult::Replay {
                        receipt: entry.receipt,
                        expires_at_ms: entry.expires_at_ms,
                    }
                } else {
                    BeginResult::Mismatch
                };
            }
            ExistingEntry::Intent(intent) => {
                return if intent.binding == binding {
                    BeginResult::Indeterminate {
                        created_at_ms: Some(intent.created_at_ms),
                    }
                } else {
                    BeginResult::Mismatch
                };
            }
            ExistingEntry::Corrupt => {
                return BeginResult::Indeterminate {
                    created_at_ms: None,
                };
            }
            ExistingEntry::None => {}
        }
        let Some(usage) = self.capacity_usage(owner_hash) else {
            return BeginResult::Full;
        };
        if usage.total >= self.max_entries || usage.owner >= self.max_entries_per_owner {
            return BeginResult::Full;
        }
        let created_at_ms = now_ms();
        let mut intent = PersistedIntent {
            version: ENTRY_VERSION,
            scope_hash,
            owner_hash,
            created_at_ms,
            binding: binding.clone(),
            authentication: [0; 32],
        };
        let Some(integrity) = self.integrity.as_deref() else {
            return BeginResult::Unavailable;
        };
        let Ok(authentication) = intent_authentication(integrity, &intent) else {
            return BeginResult::Unavailable;
        };
        intent.authentication = authentication;
        match persist_claim(&self.files, self.intent_name(&scope), &intent) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                return match self.read_entry(&scope, scope_hash, owner_hash) {
                    ExistingEntry::Replay(entry) if entry.binding == binding => {
                        BeginResult::Replay {
                            receipt: entry.receipt,
                            expires_at_ms: entry.expires_at_ms,
                        }
                    }
                    ExistingEntry::Intent(intent) if intent.binding == binding => {
                        BeginResult::Indeterminate {
                            created_at_ms: Some(intent.created_at_ms),
                        }
                    }
                    ExistingEntry::Replay(_) | ExistingEntry::Intent(_) => BeginResult::Mismatch,
                    ExistingEntry::Corrupt | ExistingEntry::None => BeginResult::Indeterminate {
                        created_at_ms: None,
                    },
                };
            }
            Err(_) => return BeginResult::Unavailable,
        }
        self.synchronization
            .active_scopes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(scope.clone(), owner_hash);
        BeginResult::Execute(SqlIdempotencyExecution {
            store: Arc::clone(self),
            scope,
            scope_hash,
            owner_hash,
            owner: owner.to_owned(),
            key: key.to_owned(),
            binding,
            _guard: guard,
        })
    }

    fn read_entry(
        &self,
        scope: &str,
        expected_scope: [u8; 32],
        expected_owner: [u8; 32],
    ) -> ExistingEntry {
        let receipt_name = self.receipt_name(scope);
        match self.files.read(&receipt_name) {
            Ok(bytes) => {
                let Ok(receipt) = serde_json::from_slice::<PersistedReceipt>(&bytes) else {
                    return ExistingEntry::Corrupt;
                };
                if receipt.version != ENTRY_VERSION
                    || receipt.scope_hash != expected_scope
                    || receipt.owner_hash != expected_owner
                    || !self.receipt_authentication_matches(&receipt)
                {
                    return ExistingEntry::Corrupt;
                }
                if receipt.expires_at_ms > now_ms() {
                    return ExistingEntry::Replay(Box::new(receipt));
                }
                // A surviving intent is the durable ambiguity marker. Never delete
                // it while expiring a receipt: it may belong to a newer execution,
                // and deleting it can turn an uncertain committed write into a
                // replayable request after restart.
                if remove_durable(&self.files, &receipt_name).is_err() {
                    return ExistingEntry::Corrupt;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => return ExistingEntry::Corrupt,
        }
        let intent_name = self.intent_name(scope);
        let bytes = match self.files.read(&intent_name) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return ExistingEntry::None,
            Err(_) => return ExistingEntry::Corrupt,
        };
        let Ok(intent) = serde_json::from_slice::<PersistedIntent>(&bytes) else {
            return ExistingEntry::Corrupt;
        };
        if intent.version != ENTRY_VERSION
            || intent.scope_hash != expected_scope
            || intent.owner_hash != expected_owner
            || !self.intent_authentication_matches(&intent)
        {
            return ExistingEntry::Corrupt;
        }
        ExistingEntry::Intent(Box::new(intent))
    }

    fn prune_expired_receipts(&self) {
        let Ok(entries) = self.files.list() else {
            return;
        };
        for name in entries {
            let Some(_scope) = scope_from_path(Path::new(&name), ".receipt.json") else {
                continue;
            };
            let expired = self
                .files
                .read(Path::new(&name))
                .ok()
                .and_then(|bytes| serde_json::from_slice::<PersistedReceipt>(&bytes).ok())
                .is_some_and(|receipt| {
                    receipt.version == ENTRY_VERSION
                        && self.receipt_authentication_matches(&receipt)
                        && receipt.expires_at_ms <= now_ms()
                });
            if expired {
                let _ = remove_durable(&self.files, Path::new(&name));
            }
        }
    }

    /// Count every persisted scope. Unknown/corrupt directory entries consume
    /// global capacity so damaged state cannot create an unbounded fail-open
    /// retry surface. Valid entries also count against their owner's quota.
    fn capacity_usage(&self, requested_owner: [u8; 32]) -> Option<CapacityUsage> {
        let active = self
            .synchronization
            .active_scopes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut scopes = active
            .iter()
            .map(|(scope, owner)| (scope.clone(), Some(*owner)))
            .collect::<HashMap<_, _>>();
        let entries = self.files.list().ok()?;
        let mut unknown_entries = 0_usize;
        for name in entries {
            let path = Path::new(&name);
            if path
                .file_name()
                .is_some_and(|name| name == CAPACITY_LOCK_FILE)
            {
                continue;
            }
            let (scope, owner) = if let Some(scope) = scope_from_path(path, ".intent.json") {
                let owner = self
                    .files
                    .read(path)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<PersistedIntent>(&bytes).ok())
                    .and_then(|entry| {
                        (entry.version == ENTRY_VERSION
                            && hex(&entry.scope_hash) == scope
                            && self.intent_authentication_matches(&entry))
                        .then_some(entry.owner_hash)
                    });
                (scope, owner)
            } else if let Some(scope) = scope_from_path(path, ".receipt.json") {
                let owner = self
                    .files
                    .read(path)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<PersistedReceipt>(&bytes).ok())
                    .and_then(|entry| {
                        (entry.version == ENTRY_VERSION
                            && hex(&entry.scope_hash) == scope
                            && self.receipt_authentication_matches(&entry))
                        .then_some(entry.owner_hash)
                    });
                (scope, owner)
            } else {
                unknown_entries = unknown_entries.saturating_add(1);
                continue;
            };
            scopes
                .entry(scope.to_owned())
                .and_modify(|current| {
                    if *current != owner {
                        *current = None;
                    }
                })
                .or_insert(owner);
        }
        Some(CapacityUsage {
            total: scopes.len().saturating_add(unknown_entries),
            owner: scopes
                .values()
                .filter(|owner| **owner == Some(requested_owner))
                .count(),
        })
    }

    fn intent_name(&self, scope: &str) -> String {
        format!("{scope}.intent.json")
    }

    fn receipt_name(&self, scope: &str) -> String {
        format!("{scope}.receipt.json")
    }

    #[cfg(test)]
    fn intent_path(&self, scope: &str) -> PathBuf {
        self.files.absolute(self.intent_name(scope))
    }

    #[cfg(test)]
    fn receipt_path(&self, scope: &str) -> PathBuf {
        self.files.absolute(self.receipt_name(scope))
    }

    fn intent_authentication_matches(&self, intent: &PersistedIntent) -> bool {
        self.integrity.as_deref().is_some_and(|integrity| {
            intent_authentication_bytes(intent).is_ok_and(|bytes| {
                integrity.verify(SQL_INTENT_MAC_DOMAIN, &bytes, &intent.authentication)
            })
        })
    }

    fn receipt_authentication_matches(&self, receipt: &PersistedReceipt) -> bool {
        self.integrity.as_deref().is_some_and(|integrity| {
            receipt_authentication_bytes(receipt).is_ok_and(|bytes| {
                integrity.verify(SQL_RECEIPT_MAC_DOMAIN, &bytes, &receipt.authentication)
            })
        })
    }
}

struct CapacityUsage {
    total: usize,
    owner: usize,
}

pub(crate) struct SqlIdempotencyExecution {
    store: Arc<SqlIdempotencyStore>,
    scope: String,
    scope_hash: [u8; 32],
    owner_hash: [u8; 32],
    owner: String,
    key: String,
    binding: SqlIdempotencyBinding,
    _guard: OwnedMutexGuard<()>,
}

impl SqlIdempotencyExecution {
    pub(crate) fn owner(&self) -> &str {
        &self.owner
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn binding(&self) -> &SqlIdempotencyBinding {
        &self.binding
    }

    pub(crate) fn ttl(&self) -> Duration {
        Duration::from_millis(self.store.expires_after_ms())
    }

    pub(crate) fn commit(self, receipt: SqlDurableReceipt) -> (u64, bool) {
        let expires_at_ms = now_ms().saturating_add(duration_ms(self.store.ttl));
        let mut entry = PersistedReceipt {
            version: ENTRY_VERSION,
            scope_hash: self.scope_hash,
            owner_hash: self.owner_hash,
            expires_at_ms,
            binding: self.binding.clone(),
            receipt,
            authentication: [0; 32],
        };
        let Some(integrity) = self.store.integrity.as_deref() else {
            return (expires_at_ms, false);
        };
        let Ok(authentication) = receipt_authentication(integrity, &entry) else {
            return (expires_at_ms, false);
        };
        entry.authentication = authentication;
        let persisted = persist_new(
            &self.store.files,
            self.store.receipt_name(&self.scope),
            &entry,
        )
        .is_ok();
        if persisted {
            let _ = remove_durable(&self.store.files, self.store.intent_name(&self.scope));
        }
        (expires_at_ms, persisted)
    }

    /// Remove an intent only after the registry proves the request reached a
    /// terminal state without any durable commit.
    pub(crate) fn abort(self) {
        let _ = remove_durable(&self.store.files, self.store.intent_name(&self.scope));
    }
}

impl Drop for SqlIdempotencyExecution {
    fn drop(&mut self) {
        self.store
            .synchronization
            .active_scopes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.scope);
    }
}

pub(crate) fn synchronization_for(dir: &Path) -> Arc<StoreSynchronization> {
    let key = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    let registry = STORE_SYNCHRONIZATION.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(synchronization) = registry.get(&key).and_then(Weak::upgrade) {
        return synchronization;
    }
    registry.retain(|_, synchronization| synchronization.strong_count() != 0);
    let synchronization = Arc::new(StoreSynchronization {
        locks: (0..LOCK_STRIPES)
            .map(|_| Arc::new(AsyncMutex::new(())))
            .collect(),
        capacity_lock: AsyncMutex::new(()),
        active_scopes: Mutex::new(HashMap::new()),
    });
    registry.insert(key, Arc::downgrade(&synchronization));
    synchronization
}

#[derive(Serialize)]
struct IntentAuthentication<'a> {
    version: u8,
    scope_hash: &'a [u8; 32],
    owner_hash: &'a [u8; 32],
    created_at_ms: u64,
    binding: &'a SqlIdempotencyBinding,
}

#[derive(Serialize)]
struct ReceiptAuthentication<'a> {
    version: u8,
    scope_hash: &'a [u8; 32],
    owner_hash: &'a [u8; 32],
    expires_at_ms: u64,
    binding: &'a SqlIdempotencyBinding,
    receipt: &'a SqlDurableReceipt,
}

fn intent_authentication_bytes(intent: &PersistedIntent) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&IntentAuthentication {
        version: intent.version,
        scope_hash: &intent.scope_hash,
        owner_hash: &intent.owner_hash,
        created_at_ms: intent.created_at_ms,
        binding: &intent.binding,
    })
}

fn receipt_authentication_bytes(receipt: &PersistedReceipt) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&ReceiptAuthentication {
        version: receipt.version,
        scope_hash: &receipt.scope_hash,
        owner_hash: &receipt.owner_hash,
        expires_at_ms: receipt.expires_at_ms,
        binding: &receipt.binding,
        receipt: &receipt.receipt,
    })
}

fn intent_authentication(
    integrity: &IdempotencyIntegrity,
    intent: &PersistedIntent,
) -> Result<[u8; 32], serde_json::Error> {
    intent_authentication_bytes(intent)
        .map(|bytes| integrity.authenticate(SQL_INTENT_MAC_DOMAIN, &bytes))
}

fn receipt_authentication(
    integrity: &IdempotencyIntegrity,
    receipt: &PersistedReceipt,
) -> Result<[u8; 32], serde_json::Error> {
    receipt_authentication_bytes(receipt)
        .map(|bytes| integrity.authenticate(SQL_RECEIPT_MAC_DOMAIN, &bytes))
}

pub(crate) struct CapacityFileGuard(std::fs::File);

impl CapacityFileGuard {
    pub(crate) async fn acquire(files: &StoreFiles) -> io::Result<Self> {
        let file = files.open_lock(CAPACITY_LOCK_FILE)?;
        loop {
            match file.try_lock_exclusive() {
                Ok(()) => return Ok(Self(file)),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }
}

impl Drop for CapacityFileGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

pub(crate) fn persist_claim(
    files: &StoreFiles,
    name: impl AsRef<Path>,
    value: &impl Serialize,
) -> io::Result<()> {
    let bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    files.write_new(name, &bytes)
}

pub(crate) fn persist_new(
    files: &StoreFiles,
    name: impl AsRef<Path>,
    value: &impl Serialize,
) -> io::Result<()> {
    let bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    files.write_atomic(name, &bytes)
}

pub(crate) fn remove_durable(files: &StoreFiles, name: impl AsRef<Path>) -> io::Result<()> {
    let name = name.as_ref();
    #[cfg(test)]
    if REMOVE_FAILURE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_deref()
        == Some(files.absolute(name).as_path())
    {
        return Err(io::Error::other("injected durable removal failure"));
    }
    files.remove(name)
}

#[cfg(test)]
static REMOVE_FAILURE: Mutex<Option<PathBuf>> = Mutex::new(None);

pub(crate) fn scope_from_path<'a>(path: &'a Path, suffix: &str) -> Option<&'a str> {
    let name = path.file_name()?.to_str()?;
    let scope = name.strip_suffix(suffix)?;
    (scope.len() == 64 && scope.bytes().all(|byte| byte.is_ascii_hexdigit())).then_some(scope)
}

fn scoped_hash(owner: &str, key: &str) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"mongreldb-sql-idempotency-v2\0");
    digest.update((owner.len() as u64).to_le_bytes());
    digest.update(owner.as_bytes());
    digest.update((key.len() as u64).to_le_bytes());
    digest.update(key.as_bytes());
    digest.finalize().into()
}

pub(crate) fn hash(value: &[u8]) -> [u8; 32] {
    Sha256::digest(value).into()
}

pub(crate) fn hex(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

pub(crate) fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

/// Namespace separating `/sql` idempotency keys from any other user of the
/// core `TXN_IDEMPOTENCY` ledger (Kit/native paths use their own).
const SQL_LEDGER_KEY_NAMESPACE: &str = "sql:";
const SQL_LEDGER_FINGERPRINT_DOMAIN: &[u8] =
    b"mongreldb/server/sql-idempotency/ledger-fingerprint/v1\0";

/// The S1B-005 request fingerprint the core ledger binds an idempotency key
/// to: a domain-separated fold of the full `/sql` request binding (SQL
/// fingerprint, parameters, request and session semantics, expiry policy).
fn ledger_fingerprint(binding: &SqlIdempotencyBinding) -> u64 {
    let mut digest = Sha256::new();
    digest.update(SQL_LEDGER_FINGERPRINT_DOMAIN);
    digest.update(binding.sql_fingerprint);
    digest.update(binding.parameter_hash);
    digest.update(binding.request_semantics_hash);
    digest.update(binding.session_semantics_hash);
    digest.update(binding.expires_after_ms.to_le_bytes());
    let output: [u8; 32] = digest.finalize().into();
    u64::from_le_bytes(output[..8].try_into().expect("8 of 32 digest bytes"))
}

/// Record one committed idempotent `/sql` write in the core's durable
/// `TXN_IDEMPOTENCY` ledger (spec §10.2 S1B-005), making the
/// key → original-`CommitReceipt` binding survive restart under the commit
/// log's authority — the same record an embedded
/// `Transaction::commit_idempotent` caller gets. Returns the ledger's receipt
/// (the original one on an identical replay) for additive surfacing in the
/// HTTP receipt, or `None` when the ledger record could not be completed; the
/// HTTP store remains the wire authority in that case (a warning is logged —
/// it never silently changes the response contract).
///
/// The binding's expiry policy is passed as the ledger TTL so both stores
/// retire the key together. A same-fingerprint replay (possible only when the
/// HTTP store lost state the ledger kept) still yields the original receipt,
/// which is exactly the ledger's truth; a fingerprint conflict is logged and
/// yields `None` — the durable write's known-committed outcome is never
/// reclassified by the bookkeeping path.
pub(crate) fn record_core_idempotency_commit(
    db: &Arc<mongreldb_core::Database>,
    owner: &str,
    key: &str,
    binding: &SqlIdempotencyBinding,
    ttl: Duration,
) -> Option<SqlCommitReceipt> {
    let request = mongreldb_core::txn::IdempotencyRequest {
        key: format!("{SQL_LEDGER_KEY_NAMESPACE}{key}"),
        owner: owner.to_owned(),
        fingerprint: ledger_fingerprint(binding),
        ttl: Some(ttl),
    };
    let transaction = db.begin();
    let state = transaction.state_handle();
    match transaction.commit_idempotent(request) {
        Ok(_epoch) => match state.state() {
            mongreldb_core::txn::TransactionState::Committed(receipt) => {
                Some(SqlCommitReceipt::from_core(&receipt))
            }
            other => {
                eprintln!("[idempotency] core ledger commit ended in unexpected state {other:?}");
                None
            }
        },
        Err(error) => {
            eprintln!("[idempotency] core ledger record for /sql write failed: {error}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::process::{Child, Command, Stdio};
    use std::time::Instant;
    use tempfile::tempdir;

    const CHILD_ROOT: &str = "MONGRELDB_IDEMPOTENCY_TEST_ROOT";
    const CHILD_KEY: &str = "MONGRELDB_IDEMPOTENCY_TEST_KEY";
    const CHILD_MAX_ENTRIES: &str = "MONGRELDB_IDEMPOTENCY_TEST_MAX_ENTRIES";
    const CHILD_READY: &str = "MONGRELDB_IDEMPOTENCY_TEST_READY";
    const CHILD_GO: &str = "MONGRELDB_IDEMPOTENCY_TEST_GO";
    const CHILD_OUTCOME: &str = "MONGRELDB_IDEMPOTENCY_TEST_OUTCOME";

    fn binding(value: u8) -> SqlIdempotencyBinding {
        SqlIdempotencyBinding {
            sql_fingerprint: [value; 32],
            parameter_hash: [2; 32],
            request_semantics_hash: [3; 32],
            session_semantics_hash: [4; 32],
            expires_after_ms: 60_000,
        }
    }

    fn receipt() -> SqlDurableReceipt {
        SqlDurableReceipt {
            original_query_id: "00112233445566778899aabbccddeeff".into(),
            status: "committed".into(),
            server_state: "completed".into(),
            cancellation_reason: "none".into(),
            outcome: SqlReceiptOutcome {
                committed: true,
                committed_statements: 1,
                last_commit_epoch: Some(7),
                last_commit_epoch_text: Some("7".into()),
                first_commit_statement_index: Some(0),
                last_commit_statement_index: Some(0),
                completed_statements: 1,
                statement_index: 0,
                serialization: "succeeded".into(),
            },
            terminal_error: None,
            commit_receipt: None,
        }
    }

    #[test]
    fn cross_process_claim_helper() {
        let Some(root) = std::env::var_os(CHILD_ROOT).map(PathBuf::from) else {
            return;
        };
        let key = std::env::var(CHILD_KEY).unwrap();
        let max_entries = std::env::var(CHILD_MAX_ENTRIES).unwrap().parse().unwrap();
        let ready = PathBuf::from(std::env::var_os(CHILD_READY).unwrap());
        let go = PathBuf::from(std::env::var_os(CHILD_GO).unwrap());
        let outcome = PathBuf::from(std::env::var_os(CHILD_OUTCOME).unwrap());
        let store = Arc::new(SqlIdempotencyStore::new(
            &root,
            Duration::from_secs(60),
            max_entries,
        ));
        std::fs::write(ready, b"ready").unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !go.exists() {
            assert!(Instant::now() < deadline, "parent did not release child");
            std::thread::sleep(Duration::from_millis(1));
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let (label, execution) = match runtime.block_on(store.begin("alice", &key, binding(1))) {
            BeginResult::Execute(execution) => ("execute", Some(execution)),
            BeginResult::Indeterminate { .. } => ("indeterminate", None),
            BeginResult::Full => ("full", None),
            BeginResult::Replay { .. } => ("replay", None),
            BeginResult::Mismatch => ("mismatch", None),
            BeginResult::Unavailable => ("unavailable", None),
        };
        std::fs::write(outcome, label).unwrap();
        drop(execution);
    }

    fn wait_for_children(children: &mut [Child], paths: &[PathBuf]) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !paths.iter().all(|path| path.exists()) {
            for child in children.iter_mut() {
                assert_eq!(child.try_wait().unwrap(), None, "child exited before ready");
            }
            assert!(Instant::now() < deadline, "children did not become ready");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn cross_process_outcomes(keys: [&str; 2], max_entries: usize) -> Vec<String> {
        let directory = tempdir().unwrap();
        let go = directory.path().join("go");
        let executable = std::env::current_exe().unwrap();
        let mut ready_paths = Vec::new();
        let mut outcome_paths = Vec::new();
        let mut children = Vec::new();
        for (index, key) in keys.into_iter().enumerate() {
            let ready = directory.path().join(format!("ready-{index}"));
            let outcome = directory.path().join(format!("outcome-{index}"));
            let child = Command::new(&executable)
                .arg("cross_process_claim_helper")
                .arg("--nocapture")
                .arg("--test-threads=1")
                .env(CHILD_ROOT, directory.path())
                .env(CHILD_KEY, key)
                .env(CHILD_MAX_ENTRIES, max_entries.to_string())
                .env(CHILD_READY, &ready)
                .env(CHILD_GO, &go)
                .env(CHILD_OUTCOME, &outcome)
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            ready_paths.push(ready);
            outcome_paths.push(outcome);
            children.push(child);
        }
        wait_for_children(&mut children, &ready_paths);
        std::fs::write(go, b"go").unwrap();
        for child in children {
            let output = child.wait_with_output().unwrap();
            assert!(
                output.status.success(),
                "child failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        outcome_paths
            .iter()
            .map(|path| std::fs::read_to_string(path).unwrap())
            .collect()
    }

    #[test]
    fn cross_process_claim_and_capacity_are_atomic() {
        let mut same_key = cross_process_outcomes(["same", "same"], 8);
        same_key.sort();
        assert_eq!(same_key, ["execute", "indeterminate"]);

        let mut capacity = cross_process_outcomes(["one", "two"], 1);
        capacity.sort();
        assert_eq!(capacity, ["execute", "full"]);
    }

    #[tokio::test]
    async fn replay_is_owner_bound_and_mismatched_reuse_is_rejected() {
        let directory = tempdir().unwrap();
        let store = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_secs(60),
            8,
        ));
        let BeginResult::Execute(execution) = store.begin("alice", "same", binding(1)).await else {
            panic!("first request must execute");
        };
        execution.commit(receipt());
        assert!(matches!(
            store.begin("alice", "same", binding(1)).await,
            BeginResult::Replay { .. }
        ));
        assert!(matches!(
            store.begin("alice", "same", binding(9)).await,
            BeginResult::Mismatch
        ));
        let mut different_expiry = binding(1);
        different_expiry.expires_after_ms += 1;
        assert!(matches!(
            store.begin("alice", "same", different_expiry).await,
            BeginResult::Mismatch
        ));
        assert!(matches!(
            store.begin("bob", "same", binding(1)).await,
            BeginResult::Execute(_)
        ));
    }

    #[tokio::test]
    async fn crash_after_intent_never_reexecutes_write() {
        let directory = tempdir().unwrap();
        let store = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_millis(20),
            8,
        ));
        let BeginResult::Execute(execution) = store.begin("alice", "key", binding(1)).await else {
            panic!("first request must execute");
        };
        drop(execution);
        let restarted = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_millis(20),
            8,
        ));
        assert!(matches!(
            restarted.begin("alice", "key", binding(1)).await,
            BeginResult::Indeterminate { .. }
        ));
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(matches!(
            restarted.begin("alice", "key", binding(1)).await,
            BeginResult::Indeterminate { .. }
        ));
    }

    #[tokio::test]
    async fn unavailable_store_directory_fails_closed() {
        let directory = tempdir().unwrap();
        std::fs::write(directory.path().join("_sql_idempotency"), b"blocked").unwrap();
        let store = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_secs(60),
            8,
        ));
        assert!(matches!(
            store.begin("alice", "key", binding(1)).await,
            BeginResult::Unavailable
        ));
    }

    #[tokio::test]
    async fn proven_abort_allows_retry() {
        let directory = tempdir().unwrap();
        let store = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_secs(60),
            8,
        ));
        let BeginResult::Execute(execution) = store.begin("alice", "key", binding(1)).await else {
            panic!("first request must execute");
        };
        execution.abort();
        assert!(matches!(
            store.begin("alice", "key", binding(1)).await,
            BeginResult::Execute(_)
        ));
    }

    #[tokio::test]
    async fn expired_committed_key_can_be_reused() {
        let directory = tempdir().unwrap();
        let store = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_millis(250),
            8,
        ));
        let BeginResult::Execute(execution) = store.begin("alice", "key", binding(1)).await else {
            panic!("first request must execute");
        };
        let (_, persisted) = execution.commit(receipt());
        assert!(persisted);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(matches!(
            store.begin("alice", "key", binding(2)).await,
            BeginResult::Execute(_)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn expired_receipt_cleanup_never_deletes_surviving_intent() {
        let directory = tempdir().unwrap();
        let store = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_millis(20),
            8,
        ));
        let request_binding = binding(1);
        let BeginResult::Execute(execution) =
            store.begin("alice", "key", request_binding.clone()).await
        else {
            panic!("first request must execute");
        };
        let (_, persisted) = execution.commit(receipt());
        assert!(persisted);

        // Model a crash seam where receipt publication succeeded but removing
        // the durable intent did not. This ambiguity marker must outlive
        // receipt expiry and every pruning pass.
        let scope_hash = scoped_hash("alice", "key");
        let scope = hex(&scope_hash);
        let intent_path = store.intent_path(&scope);
        let mut intent = PersistedIntent {
            version: ENTRY_VERSION,
            scope_hash,
            owner_hash: hash(b"alice"),
            created_at_ms: now_ms(),
            binding: request_binding.clone(),
            authentication: [0; 32],
        };
        intent.authentication =
            intent_authentication(store.integrity.as_deref().unwrap(), &intent).unwrap();
        persist_new(&store.files, store.intent_name(&scope), &intent).unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;

        let receipt_path = store.receipt_path(&scope);
        *REMOVE_FAILURE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(receipt_path.clone());
        assert!(matches!(
            store.begin("alice", "key", request_binding.clone()).await,
            BeginResult::Indeterminate {
                created_at_ms: None
            }
        ));
        assert!(intent_path.exists());

        *REMOVE_FAILURE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        store.prune_expired_receipts();
        assert!(!receipt_path.exists());
        assert!(intent_path.exists());
        assert!(matches!(
            store.begin("alice", "key", request_binding).await,
            BeginResult::Indeterminate {
                created_at_ms: Some(_)
            }
        ));
    }

    #[tokio::test]
    async fn receipt_ttl_starts_after_execution_finishes() {
        let directory = tempdir().unwrap();
        let ttl = Duration::from_secs(60);
        let store = Arc::new(SqlIdempotencyStore::new(directory.path(), ttl, 8));
        let BeginResult::Execute(execution) = store.begin("alice", "key", binding(1)).await else {
            panic!("first request must execute");
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        let commit_started_at_ms = now_ms();
        let (expires_at_ms, persisted) = execution.commit(receipt());
        assert!(persisted);
        assert!(expires_at_ms >= commit_started_at_ms.saturating_add(duration_ms(ttl)));
        assert!(matches!(
            store.begin("alice", "key", binding(1)).await,
            BeginResult::Replay { .. }
        ));
    }

    #[tokio::test]
    async fn store_capacity_counts_durable_intent() {
        let directory = tempdir().unwrap();
        let store = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_secs(60),
            1,
        ));
        let BeginResult::Execute(_first) = store.begin("alice", "one", binding(1)).await else {
            panic!("first request must reserve capacity");
        };
        assert!(matches!(
            store.begin("alice", "two", binding(2)).await,
            BeginResult::Full
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stores_for_same_root_cannot_overbook_capacity() {
        let directory = tempdir().unwrap();
        let first = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_secs(60),
            1,
        ));
        let second = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_secs(60),
            1,
        ));
        let (first, second) = tokio::join!(
            first.begin("alice", "one", binding(1)),
            second.begin("alice", "two", binding(2)),
        );
        let outcomes = [first, second];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, BeginResult::Execute(_)))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, BeginResult::Full))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn valid_json_receipt_corruption_is_never_pruned_or_reexecuted() {
        let directory = tempdir().unwrap();
        let store = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_secs(60),
            8,
        ));
        let BeginResult::Execute(execution) = store.begin("alice", "key", binding(1)).await else {
            panic!("first request must execute");
        };
        let (_, persisted) = execution.commit(receipt());
        assert!(persisted);

        let scope = hex(&scoped_hash("alice", "key"));
        let receipt_path = store.receipt_path(&scope);
        let mut corrupted: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&receipt_path).unwrap()).unwrap();
        corrupted["expires_at_ms"] = serde_json::Value::from(0);
        std::fs::write(&receipt_path, serde_json::to_vec(&corrupted).unwrap()).unwrap();

        store.prune_expired_receipts();
        assert!(receipt_path.exists());
        assert!(matches!(
            store.begin("alice", "key", binding(1)).await,
            BeginResult::Indeterminate {
                created_at_ms: None
            }
        ));
    }

    #[tokio::test]
    async fn forged_valid_json_intent_fails_closed() {
        let directory = tempdir().unwrap();
        let store = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_secs(60),
            8,
        ));
        let BeginResult::Execute(execution) = store.begin("alice", "key", binding(1)).await else {
            panic!("first request must execute");
        };
        drop(execution);

        let scope = hex(&scoped_hash("alice", "key"));
        let intent_path = store.intent_path(&scope);
        let mut forged: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&intent_path).unwrap()).unwrap();
        forged["created_at_ms"] = serde_json::Value::from(0);
        std::fs::write(&intent_path, serde_json::to_vec(&forged).unwrap()).unwrap();

        assert!(matches!(
            store.begin("alice", "key", binding(1)).await,
            BeginResult::Indeterminate {
                created_at_ms: None
            }
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinked_receipt_intent_and_capacity_lock_fail_closed() {
        use std::os::unix::fs::symlink;

        let directory = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let store = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_secs(60),
            8,
        ));

        let BeginResult::Execute(execution) = store.begin("alice", "receipt", binding(1)).await
        else {
            panic!("receipt request must execute");
        };
        let (_, persisted) = execution.commit(receipt());
        assert!(persisted);
        let receipt_scope = hex(&scoped_hash("alice", "receipt"));
        let receipt_path = store.receipt_path(&receipt_scope);
        let outside_receipt = outside.path().join("receipt.json");
        std::fs::rename(&receipt_path, &outside_receipt).unwrap();
        symlink(&outside_receipt, &receipt_path).unwrap();
        assert!(matches!(
            store.begin("alice", "receipt", binding(1)).await,
            BeginResult::Indeterminate {
                created_at_ms: None
            }
        ));

        let intent_root = tempdir().unwrap();
        let intent_store = Arc::new(SqlIdempotencyStore::new(
            intent_root.path(),
            Duration::from_secs(60),
            8,
        ));
        let BeginResult::Execute(execution) =
            intent_store.begin("alice", "intent", binding(2)).await
        else {
            panic!("intent request must execute");
        };
        drop(execution);
        let intent_scope = hex(&scoped_hash("alice", "intent"));
        let intent_path = intent_store.intent_path(&intent_scope);
        let outside_intent = outside.path().join("intent.json");
        std::fs::rename(&intent_path, &outside_intent).unwrap();
        symlink(&outside_intent, &intent_path).unwrap();
        assert!(matches!(
            intent_store.begin("alice", "intent", binding(2)).await,
            BeginResult::Indeterminate {
                created_at_ms: None
            }
        ));

        let lock_root = tempdir().unwrap();
        let lock_outside = tempdir().unwrap();
        let lock_store = Arc::new(SqlIdempotencyStore::new(
            lock_root.path(),
            Duration::from_secs(60),
            8,
        ));
        let outside_lock = lock_outside.path().join("capacity.lock");
        std::fs::write(&outside_lock, b"outside").unwrap();
        symlink(&outside_lock, lock_store.files.absolute(CAPACITY_LOCK_FILE)).unwrap();
        assert!(matches!(
            lock_store.begin("alice", "key", binding(3)).await,
            BeginResult::Unavailable
        ));
    }

    #[tokio::test]
    async fn legacy_fixed_temp_cannot_block_or_replace_receipt() {
        let directory = tempdir().unwrap();
        let store = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_secs(60),
            8,
        ));
        let BeginResult::Execute(execution) = store.begin("alice", "key", binding(1)).await else {
            panic!("first request must execute");
        };
        let scope = hex(&scoped_hash("alice", "key"));
        let old_fixed_temp = store.files.absolute(format!("{scope}.receipt.json.tmp"));
        std::fs::write(&old_fixed_temp, b"attacker-controlled").unwrap();

        let (_, persisted) = execution.commit(receipt());
        assert!(persisted);
        assert_eq!(
            std::fs::read(old_fixed_temp).unwrap(),
            b"attacker-controlled"
        );
        let restarted = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_secs(60),
            8,
        ));
        assert!(matches!(
            restarted.begin("alice", "key", binding(1)).await,
            BeginResult::Replay { receipt, .. }
                if receipt.original_query_id == "00112233445566778899aabbccddeeff"
        ));
    }

    #[tokio::test]
    async fn changed_plaintext_integrity_key_never_forges_replay() {
        let directory = tempdir().unwrap();
        let store = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_secs(60),
            8,
        ));
        let BeginResult::Execute(execution) = store.begin("alice", "key", binding(1)).await else {
            panic!("first request must execute");
        };
        let (_, persisted) = execution.commit(receipt());
        assert!(persisted);
        drop(store);
        std::fs::write(directory.path().join(INTEGRITY_KEY_FILE), [0x5au8; 32]).unwrap();

        let restarted = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_secs(60),
            8,
        ));
        assert!(matches!(
            restarted.begin("alice", "key", binding(1)).await,
            BeginResult::Indeterminate {
                created_at_ms: None
            }
        ));
    }

    #[tokio::test]
    async fn renamed_root_stays_descriptor_pinned() {
        let parent = tempdir().unwrap();
        let original = parent.path().join("database");
        let moved = parent.path().join("moved-database");
        std::fs::create_dir(&original).unwrap();
        let store = Arc::new(SqlIdempotencyStore::new(
            &original,
            Duration::from_secs(60),
            8,
        ));
        std::fs::rename(&original, &moved).unwrap();
        std::fs::create_dir(&original).unwrap();

        let BeginResult::Execute(_execution) = store.begin("alice", "key", binding(1)).await else {
            panic!("pinned store must remain usable");
        };
        let scope = hex(&scoped_hash("alice", "key"));
        assert!(moved
            .join("_sql_idempotency")
            .join(store.intent_name(&scope))
            .is_file());
        assert!(!original.join("_sql_idempotency").exists());
    }

    #[tokio::test]
    async fn abandoned_intent_preserves_unknown_key_and_consumes_capacity() {
        let directory = tempdir().unwrap();
        let store = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_secs(60),
            1,
        ));
        let BeginResult::Execute(first) = store.begin("alice", "one", binding(1)).await else {
            panic!("first request must reserve capacity");
        };
        drop(first);

        let restarted = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_secs(60),
            1,
        ));
        assert!(matches!(
            restarted.begin("alice", "two", binding(2)).await,
            BeginResult::Full
        ));
        assert!(matches!(
            restarted.begin("alice", "one", binding(1)).await,
            BeginResult::Indeterminate {
                created_at_ms: Some(_)
            }
        ));
        assert!(matches!(
            restarted.begin("alice", "one", binding(9)).await,
            BeginResult::Mismatch
        ));
    }

    #[tokio::test]
    async fn one_owner_cannot_exhaust_global_capacity() {
        let directory = tempdir().unwrap();
        let store = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_secs(60),
            8,
        ));
        for (key, value) in [("one", 1), ("two", 2)] {
            let BeginResult::Execute(execution) = store.begin("alice", key, binding(value)).await
            else {
                panic!("owner quota must admit first two scopes");
            };
            drop(execution);
        }
        assert!(matches!(
            store.begin("alice", "three", binding(3)).await,
            BeginResult::Full
        ));
        assert!(matches!(
            store.begin("bob", "one", binding(4)).await,
            BeginResult::Execute(_)
        ));
    }

    #[tokio::test]
    async fn corrupt_directory_entry_consumes_global_capacity() {
        let directory = tempdir().unwrap();
        let store = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_secs(60),
            1,
        ));
        std::fs::write(store.files.absolute("damaged-entry"), b"corrupt").unwrap();

        assert!(matches!(
            store.begin("alice", "one", binding(1)).await,
            BeginResult::Full
        ));
    }

    #[test]
    fn concurrent_distinct_scopes_cannot_overbook_capacity() {
        let directory = tempdir().unwrap();
        let store = Arc::new(SqlIdempotencyStore::new(
            directory.path(),
            Duration::from_secs(60),
            1,
        ));
        let mut stripes = HashSet::new();
        let mut keys = Vec::new();
        for index in 0..1_000 {
            let key = format!("key-{index}");
            let stripe = usize::from(scoped_hash("alice", &key)[0]) % LOCK_STRIPES;
            if stripes.insert(stripe) {
                keys.push(key);
            }
            if keys.len() == 8 {
                break;
            }
        }
        assert_eq!(keys.len(), 8);
        let barrier = Arc::new(std::sync::Barrier::new(keys.len()));
        let release = Arc::new(std::sync::Barrier::new(keys.len()));
        let threads: Vec<_> = keys
            .into_iter()
            .enumerate()
            .map(|(index, key)| {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                let release = Arc::clone(&release);
                std::thread::spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    barrier.wait();
                    let (outcome, execution) =
                        match runtime.block_on(store.begin("alice", &key, binding(index as u8))) {
                            BeginResult::Execute(execution) => ("execute", Some(execution)),
                            BeginResult::Full => ("full", None),
                            _ => ("unexpected", None),
                        };
                    release.wait();
                    drop(execution);
                    outcome
                })
            })
            .collect();
        let outcomes: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == "execute")
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == "full")
                .count(),
            7
        );
    }
}
