//! Typed Kit-aware server endpoints that sit on top of the engine's
//! transactional commit path. These give remote clients an authoritative
//! surface (validation + constraints executed server-side inside one core
//! transaction) rather than SQL passthrough.
//!
//! Routes:
//!   GET  /kit/schema            → all tables' schema/constraint metadata
//!   GET  /kit/schema/{table}    → one table's metadata (404 if absent)
//!   POST /kit/txn               → typed atomic batch (see [`KitTxnRequest`])
//!
//! Enforcement: every `/kit/txn` runs inside [`Database::transaction`], so the
//! engine's declarative constraints (unique / FK / check) are enforced
//! authoritatively at commit — concurrent conflicting writers cannot both
//! commit, and a violating batch is rejected atomically (no partial commit).

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use hmac::{Hmac, Mac};
use mongreldb_core::constraint::TableConstraints;
use mongreldb_core::embedding::EmbeddingSource;
use mongreldb_core::query::{
    AnnCandidateDistance, AnnRerankRequest, Condition, Fusion, NamedRetriever, Query, Retriever,
    RetrieverScore, SearchRequest, SetMember, SetSimilarityRequest, VectorMetric,
};
use mongreldb_core::schema::{
    ColumnDef, ColumnFlags, DefaultExpr, IndexDef, IndexKind, Schema, TypeId,
};
use mongreldb_core::txn::{UpsertAction, UpsertActionKind};
use mongreldb_core::{MongrelError, RowId, Value};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Jval};
use sha2::{Digest, Sha256};

use crate::json_to_value;
use crate::{request_principal, validate_table_name, AppState, OptionalPrincipal};

const DEFAULT_AI_DEADLINE_MS: u64 = 30_000;
const MAX_AI_DEADLINE_MS: u64 = 60_000;
const DEFAULT_AI_WORK: usize = 1_000_000;
const MAX_AI_WORK: usize = 1_000_000;
const AI_CANCELLATION_GRACE: std::time::Duration = std::time::Duration::from_millis(100);

fn max_ai_fused_candidates() -> usize {
    std::env::var("MONGRELDB_AI_MAX_FUSED_CANDIDATES")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value: &usize| *value > 0 && *value <= mongreldb_core::query::MAX_FUSED_CANDIDATES)
        .unwrap_or(mongreldb_core::query::MAX_FUSED_CANDIDATES)
}

fn ai_execution_options(
    deadline_ms: Option<u64>,
    max_work: Option<usize>,
) -> Result<
    (
        std::time::Duration,
        mongreldb_core::query::AiExecutionContext,
    ),
    MongrelError,
> {
    let deadline_ms = deadline_ms.unwrap_or(DEFAULT_AI_DEADLINE_MS);
    if deadline_ms == 0 || deadline_ms > MAX_AI_DEADLINE_MS {
        return Err(MongrelError::InvalidArgument(format!(
            "deadline_ms must be between 1 and {MAX_AI_DEADLINE_MS}"
        )));
    }
    let max_work = max_work.unwrap_or(DEFAULT_AI_WORK);
    if max_work == 0 || max_work > MAX_AI_WORK {
        return Err(MongrelError::InvalidArgument(format!(
            "max_work must be between 1 and {MAX_AI_WORK}"
        )));
    }
    let duration = std::time::Duration::from_millis(deadline_ms);
    Ok((
        duration,
        mongreldb_core::query::AiExecutionContext::with_limits(
            duration,
            max_work,
            max_ai_fused_candidates(),
        ),
    ))
}

struct CancelOnDrop(Option<mongreldb_core::query::AiExecutionContext>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(context) = &self.0 {
            context.cancel();
        }
    }
}

async fn run_ai<T, F>(
    state: Arc<AppState>,
    timeout: std::time::Duration,
    context: mongreldb_core::query::AiExecutionContext,
    work: F,
) -> Result<T, MongrelError>
where
    T: Send + 'static,
    F: FnOnce(&mongreldb_core::query::AiExecutionContext) -> Result<T, MongrelError>
        + Send
        + 'static,
{
    // S4B: evaluate node pressure and refuse AI work under RejectOversizedAi.
    crate::refresh_node_pressure(&state);
    let started = std::time::Instant::now();
    let permit = tokio::time::timeout(timeout, state.ai_semaphore.clone().acquire_owned())
        .await
        .map_err(|_| MongrelError::DeadlineExceeded)?
        .map_err(|_| MongrelError::Cancelled)?;
    let remaining = timeout.saturating_sub(started.elapsed());
    if remaining.is_zero() {
        return Err(MongrelError::DeadlineExceeded);
    }
    let class = mongreldb_core::WorkloadClass::AiRetrieval;
    let priority = crate::admission::priority_for_class(&state.resource_groups, class);
    let scheduled = tokio::time::timeout(
        remaining,
        state.scheduler.submit_and_wait(
            crate::admission::AdmitRequest {
                tenant: "default",
                class,
                priority,
                deadline: Some(remaining),
                query_id: Some(mongreldb_types::ids::QueryId::new_random()),
                tag: "kit-ai",
            },
            std::future::pending(),
        ),
    )
    .await
    .map_err(|_| MongrelError::DeadlineExceeded)?
    .map_err(crate::admission::admit_error_to_core)?;
    let remaining = timeout.saturating_sub(started.elapsed());
    if remaining.is_zero() {
        return Err(MongrelError::DeadlineExceeded);
    }
    let worker_context = context.clone();
    let mut cancel = CancelOnDrop(Some(context.clone()));
    let mut task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let _scheduled = scheduled;
        work(&worker_context)
    });
    let result = match tokio::time::timeout(remaining, &mut task).await {
        Ok(result) => {
            result.map_err(|error| MongrelError::Other(format!("AI worker failed: {error}")))?
        }
        Err(_) => {
            context.cancel();
            if tokio::time::timeout(AI_CANCELLATION_GRACE, &mut task)
                .await
                .is_err()
            {
                eprintln!("AI worker exceeded cancellation grace");
            }
            Err(MongrelError::DeadlineExceeded)
        }
    };
    cancel.0 = None;
    result
}

#[allow(clippy::too_many_arguments)]
fn retry_authorized_context<T, F>(
    state: &AppState,
    table: &str,
    principal: Option<&mongreldb_core::Principal>,
    required_columns: &[u16],
    required_permissions: &[mongreldb_core::Permission],
    context: &mongreldb_core::query::AiExecutionContext,
    snapshot_override: Option<mongreldb_core::Snapshot>,
    read: F,
) -> Result<T, MongrelError>
where
    F: FnMut(
        &mongreldb_core::Table,
        mongreldb_core::Snapshot,
        Option<&mongreldb_core::security::CandidateAuthorization<'_>>,
        Option<&mongreldb_core::Principal>,
    ) -> Result<T, MongrelError>,
{
    retry_authorized_context_stamped(
        state,
        table,
        principal,
        required_columns,
        required_permissions,
        context,
        snapshot_override,
        read,
    )
    .map(|(result, _)| result)
}

#[allow(clippy::too_many_arguments)]
fn retry_authorized_context_stamped<T, F>(
    state: &AppState,
    table: &str,
    principal: Option<&mongreldb_core::Principal>,
    required_columns: &[u16],
    required_permissions: &[mongreldb_core::Permission],
    context: &mongreldb_core::query::AiExecutionContext,
    snapshot_override: Option<mongreldb_core::Snapshot>,
    read: F,
) -> Result<(T, mongreldb_core::AuthorizedReadStamp), MongrelError>
where
    F: FnMut(
        &mongreldb_core::Table,
        mongreldb_core::Snapshot,
        Option<&mongreldb_core::security::CandidateAuthorization<'_>>,
        Option<&mongreldb_core::Principal>,
    ) -> Result<T, MongrelError>,
{
    let catalog_bound = principal
        .is_some_and(|principal| state.db().resolve_principal(&principal.username).is_some());
    state.db().with_authorized_scored_read_context_at_stamped(
        table,
        principal,
        catalog_bound,
        Some(&mongreldb_core::ReadAuthorization {
            operation: mongreldb_core::ColumnOperation::Select,
            columns: required_columns.to_vec(),
            permissions: required_permissions.to_vec(),
        }),
        Some(context),
        snapshot_override,
        read,
    )
}

// v3 replaces the forgeable v2 checksum with a keyed MAC. Older entries are
// deliberately unreadable and remain fail-closed outcome-unknown markers.
const IDEMPOTENCY_ENTRY_VERSION: u8 = 3;
const KIT_INTENT_MAC_DOMAIN: &[u8] = b"mongreldb/server/kit-idempotency/intent/v3\0";
const KIT_RECEIPT_MAC_DOMAIN: &[u8] = b"mongreldb/server/kit-idempotency/receipt/v3\0";

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct IdempotencyResponse {
    status: u16,
    body: Jval,
}

impl IdempotencyResponse {
    fn new(status: StatusCode, body: Jval) -> Self {
        Self {
            status: status.as_u16(),
            body,
        }
    }

    fn into_response(self) -> Response {
        (
            StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Json(self.body),
        )
            .into_response()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IdempotencyBinding {
    owner_hash: [u8; 32],
    key_hash: [u8; 32],
    operation_hash: [u8; 32],
    payload_hash: [u8; 32],
}

#[derive(Serialize, Deserialize)]
struct PersistedIntent {
    version: u8,
    scope_hash: [u8; 32],
    created_at_ms: u64,
    binding: IdempotencyBinding,
    authentication: [u8; 32],
}

#[derive(Serialize, Deserialize)]
struct PersistedReceipt {
    version: u8,
    scope_hash: [u8; 32],
    expires_at_ms: u64,
    binding: IdempotencyBinding,
    response: IdempotencyResponse,
    authentication: [u8; 32],
}

enum ExistingIdempotencyEntry {
    Replay(IdempotencyResponse),
    Intent,
    Expired,
    Mismatch,
    Corrupt,
    None,
}

enum IdempotencyBegin<'a> {
    Replay(IdempotencyResponse),
    Execute(IdempotencyExecution<'a>),
    Mismatch,
    Indeterminate,
    Full,
    Unavailable,
}

struct IdempotencyExecution<'a> {
    store: &'a IdempotencyStore,
    scope: String,
    scope_hash: [u8; 32],
    binding: IdempotencyBinding,
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

/// Durable non-SQL write idempotency. The intent is synced before execution;
/// the exact response is synced after commit. A surviving or corrupt intent
/// fails closed because the earlier write may have committed.
pub struct IdempotencyStore {
    files: crate::sql_idempotency::StoreFiles,
    integrity: Option<Arc<crate::sql_idempotency::IdempotencyIntegrity>>,
    synchronization: Arc<crate::sql_idempotency::StoreSynchronization>,
    available: AtomicBool,
    ttl: std::time::Duration,
    max_entries: usize,
    max_entries_per_owner: usize,
}

impl IdempotencyStore {
    #[cfg(test)]
    pub fn new(root: &std::path::Path) -> Self {
        let root = Arc::new(
            mongreldb_core::durable_file::DurableRoot::open(root)
                .expect("temporary test root must open"),
        );
        let integrity = crate::sql_idempotency::IdempotencyIntegrity::for_test_root(&root);
        Self::new_with_integrity(
            root,
            integrity,
            crate::default_sql_idempotency_ttl(),
            crate::default_sql_idempotency_max_entries(),
        )
    }

    pub(crate) fn new_with_integrity(
        root: Arc<mongreldb_core::durable_file::DurableRoot>,
        integrity: Option<Arc<crate::sql_idempotency::IdempotencyIntegrity>>,
        ttl: std::time::Duration,
        max_entries: usize,
    ) -> Self {
        Self::from_parts(root, integrity, ttl, max_entries)
    }

    #[cfg(test)]
    fn new_with_limits(
        root: &std::path::Path,
        ttl: std::time::Duration,
        max_entries: usize,
    ) -> Self {
        let root = Arc::new(
            mongreldb_core::durable_file::DurableRoot::open(root)
                .expect("temporary test root must open"),
        );
        let integrity = crate::sql_idempotency::IdempotencyIntegrity::for_test_root(&root);
        Self::from_parts(root, integrity, ttl, max_entries)
    }

    fn from_parts(
        root: Arc<mongreldb_core::durable_file::DurableRoot>,
        integrity: Option<Arc<crate::sql_idempotency::IdempotencyIntegrity>>,
        ttl: std::time::Duration,
        max_entries: usize,
    ) -> Self {
        let files = crate::sql_idempotency::StoreFiles::new(root, "_idem");
        let available = integrity.is_some() && files.ensure_directory().is_ok();
        let synchronization = crate::sql_idempotency::synchronization_for(&files.path());
        let max_entries = max_entries.max(1);
        Self {
            files,
            integrity,
            synchronization,
            available: AtomicBool::new(available),
            ttl,
            max_entries,
            max_entries_per_owner: (max_entries / 4).max(1),
        }
    }

    async fn begin(
        &self,
        owner: &str,
        key: &str,
        operation: &str,
        payload: &[u8],
    ) -> IdempotencyBegin<'_> {
        if !self.available.load(Ordering::Acquire) {
            if self.integrity.is_none() || self.files.ensure_directory().is_err() {
                return IdempotencyBegin::Unavailable;
            }
            self.available.store(true, Ordering::Release);
        }
        let scope_hash = idempotency_scope_hash(owner, key);
        let scope = crate::sql_idempotency::hex(&scope_hash);
        let binding = IdempotencyBinding {
            owner_hash: crate::sql_idempotency::hash(owner.as_bytes()),
            key_hash: crate::sql_idempotency::hash(key.as_bytes()),
            operation_hash: crate::sql_idempotency::hash(operation.as_bytes()),
            payload_hash: crate::sql_idempotency::hash(payload),
        };
        let guard = self.synchronization.lock_scope(scope_hash).await;
        match self.read_entry(&scope, scope_hash, &binding) {
            ExistingIdempotencyEntry::Replay(response) => {
                return IdempotencyBegin::Replay(response)
            }
            ExistingIdempotencyEntry::Intent | ExistingIdempotencyEntry::Corrupt => {
                return IdempotencyBegin::Indeterminate
            }
            ExistingIdempotencyEntry::Mismatch => return IdempotencyBegin::Mismatch,
            ExistingIdempotencyEntry::Expired | ExistingIdempotencyEntry::None => {}
        }
        let _capacity_guard = self.synchronization.lock_capacity().await;
        let Ok(_capacity_file_guard) =
            crate::sql_idempotency::CapacityFileGuard::acquire(&self.files).await
        else {
            return IdempotencyBegin::Unavailable;
        };
        self.prune_expired_receipts();
        match self.read_entry(&scope, scope_hash, &binding) {
            ExistingIdempotencyEntry::Replay(response) => {
                return IdempotencyBegin::Replay(response)
            }
            ExistingIdempotencyEntry::Mismatch => return IdempotencyBegin::Mismatch,
            ExistingIdempotencyEntry::Intent
            | ExistingIdempotencyEntry::Expired
            | ExistingIdempotencyEntry::Corrupt => return IdempotencyBegin::Indeterminate,
            ExistingIdempotencyEntry::None => {}
        }
        if self.legacy_entry_exists() {
            return IdempotencyBegin::Indeterminate;
        }
        let Some(usage) = self.capacity_usage(binding.owner_hash) else {
            return IdempotencyBegin::Full;
        };
        if usage.total >= self.max_entries || usage.owner >= self.max_entries_per_owner {
            return IdempotencyBegin::Full;
        }
        let mut intent = PersistedIntent {
            version: IDEMPOTENCY_ENTRY_VERSION,
            scope_hash,
            created_at_ms: crate::sql_idempotency::now_ms(),
            binding: binding.clone(),
            authentication: [0; 32],
        };
        let Some(integrity) = self.integrity.as_deref() else {
            return IdempotencyBegin::Unavailable;
        };
        let Ok(authentication) = intent_authentication(integrity, &intent) else {
            return IdempotencyBegin::Unavailable;
        };
        intent.authentication = authentication;
        match crate::sql_idempotency::persist_claim(&self.files, self.intent_name(&scope), &intent)
        {
            Ok(()) => IdempotencyBegin::Execute(IdempotencyExecution {
                store: self,
                scope,
                scope_hash,
                binding,
                _guard: guard,
            }),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                match self.read_entry(&scope, scope_hash, &binding) {
                    ExistingIdempotencyEntry::Replay(response) => {
                        IdempotencyBegin::Replay(response)
                    }
                    ExistingIdempotencyEntry::Mismatch => IdempotencyBegin::Mismatch,
                    ExistingIdempotencyEntry::Intent
                    | ExistingIdempotencyEntry::Expired
                    | ExistingIdempotencyEntry::Corrupt
                    | ExistingIdempotencyEntry::None => IdempotencyBegin::Indeterminate,
                }
            }
            Err(_) => IdempotencyBegin::Unavailable,
        }
    }

    fn read_entry(
        &self,
        scope: &str,
        expected_scope: [u8; 32],
        expected_binding: &IdempotencyBinding,
    ) -> ExistingIdempotencyEntry {
        let receipt_name = self.receipt_name(scope);
        match self.files.read(&receipt_name) {
            Ok(bytes) => {
                let Ok(receipt) = serde_json::from_slice::<PersistedReceipt>(&bytes) else {
                    return ExistingIdempotencyEntry::Corrupt;
                };
                if receipt.version != IDEMPOTENCY_ENTRY_VERSION
                    || receipt.scope_hash != expected_scope
                    || StatusCode::from_u16(receipt.response.status).is_err()
                    || !self.receipt_authentication_matches(&receipt)
                {
                    return ExistingIdempotencyEntry::Corrupt;
                }
                if receipt.expires_at_ms <= crate::sql_idempotency::now_ms() {
                    return ExistingIdempotencyEntry::Expired;
                }
                return if receipt.binding == *expected_binding {
                    ExistingIdempotencyEntry::Replay(receipt.response)
                } else {
                    ExistingIdempotencyEntry::Mismatch
                };
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => return ExistingIdempotencyEntry::Corrupt,
        }
        let intent_name = self.intent_name(scope);
        let bytes = match self.files.read(&intent_name) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return ExistingIdempotencyEntry::None
            }
            Err(_) => return ExistingIdempotencyEntry::Corrupt,
        };
        let Ok(intent) = serde_json::from_slice::<PersistedIntent>(&bytes) else {
            return ExistingIdempotencyEntry::Corrupt;
        };
        if intent.version != IDEMPOTENCY_ENTRY_VERSION
            || intent.scope_hash != expected_scope
            || !self.intent_authentication_matches(&intent)
        {
            return ExistingIdempotencyEntry::Corrupt;
        }
        if intent.binding == *expected_binding {
            ExistingIdempotencyEntry::Intent
        } else {
            ExistingIdempotencyEntry::Mismatch
        }
    }

    fn prune_expired_receipts(&self) {
        let Ok(entries) = self.files.list() else {
            return;
        };
        for name in entries {
            let path = std::path::Path::new(&name);
            if crate::sql_idempotency::scope_from_path(path, ".receipt.json").is_none() {
                continue;
            }
            let expired = self
                .files
                .read(path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<PersistedReceipt>(&bytes).ok())
                .is_some_and(|receipt| {
                    receipt.version == IDEMPOTENCY_ENTRY_VERSION
                        && self.receipt_authentication_matches(&receipt)
                        && receipt.expires_at_ms <= crate::sql_idempotency::now_ms()
                });
            if expired {
                let _ = crate::sql_idempotency::remove_durable(&self.files, path);
            }
        }
    }

    fn capacity_usage(&self, requested_owner: [u8; 32]) -> Option<IdempotencyCapacityUsage> {
        let entries = self.files.list().ok()?;
        let mut scopes = HashMap::<String, Option<[u8; 32]>>::new();
        let mut unknown_entries = 0_usize;
        for name in entries {
            let path = std::path::Path::new(&name);
            if path
                .file_name()
                .is_some_and(|name| name == crate::sql_idempotency::CAPACITY_LOCK_FILE)
            {
                continue;
            }
            let (scope, owner) = if let Some(scope) =
                crate::sql_idempotency::scope_from_path(path, ".intent.json")
            {
                let owner = self
                    .files
                    .read(path)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<PersistedIntent>(&bytes).ok())
                    .and_then(|intent| {
                        (intent.version == IDEMPOTENCY_ENTRY_VERSION
                            && crate::sql_idempotency::hex(&intent.scope_hash) == scope
                            && self.intent_authentication_matches(&intent))
                        .then_some(intent.binding.owner_hash)
                    });
                (scope, owner)
            } else if let Some(scope) =
                crate::sql_idempotency::scope_from_path(path, ".receipt.json")
            {
                let owner = self
                    .files
                    .read(path)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<PersistedReceipt>(&bytes).ok())
                    .and_then(|receipt| {
                        (receipt.version == IDEMPOTENCY_ENTRY_VERSION
                            && crate::sql_idempotency::hex(&receipt.scope_hash) == scope
                            && self.receipt_authentication_matches(&receipt))
                        .then_some(receipt.binding.owner_hash)
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
        Some(IdempotencyCapacityUsage {
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
    fn intent_path(&self, scope: &str) -> std::path::PathBuf {
        self.files.absolute(self.intent_name(scope))
    }

    #[cfg(test)]
    fn receipt_path(&self, scope: &str) -> std::path::PathBuf {
        self.files.absolute(self.receipt_name(scope))
    }

    fn intent_authentication_matches(&self, intent: &PersistedIntent) -> bool {
        self.integrity.as_deref().is_some_and(|integrity| {
            intent_authentication_bytes(intent).is_ok_and(|bytes| {
                integrity.verify(KIT_INTENT_MAC_DOMAIN, &bytes, &intent.authentication)
            })
        })
    }

    fn receipt_authentication_matches(&self, receipt: &PersistedReceipt) -> bool {
        self.integrity.as_deref().is_some_and(|integrity| {
            receipt_authentication_bytes(receipt).is_ok_and(|bytes| {
                integrity.verify(KIT_RECEIPT_MAC_DOMAIN, &bytes, &receipt.authentication)
            })
        })
    }

    fn legacy_entry_exists(&self) -> bool {
        // The former DefaultHasher filenames carried no key or binding, and
        // DefaultHasher is not stable across toolchains. Any surviving legacy
        // receipt therefore blocks new keyed writes until an operator removes
        // it after the old retry window.
        let Ok(entries) = self.files.list() else {
            return true;
        };
        for entry in entries {
            let Some(name) = entry.to_str() else {
                continue;
            };
            let legacy_hash = name
                .strip_suffix(".json")
                .or_else(|| name.strip_suffix(".json.tmp"));
            if legacy_hash.is_some_and(|value| {
                value.len() == 16 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
            }) {
                return true;
            }
        }
        false
    }
}

struct IdempotencyCapacityUsage {
    total: usize,
    owner: usize,
}

impl IdempotencyExecution<'_> {
    fn commit(self, response: IdempotencyResponse) -> bool {
        let mut receipt = PersistedReceipt {
            version: IDEMPOTENCY_ENTRY_VERSION,
            scope_hash: self.scope_hash,
            expires_at_ms: crate::sql_idempotency::now_ms()
                .saturating_add(crate::sql_idempotency::duration_ms(self.store.ttl)),
            binding: self.binding,
            response,
            authentication: [0; 32],
        };
        let Some(integrity) = self.store.integrity.as_deref() else {
            return false;
        };
        let Ok(authentication) = receipt_authentication(integrity, &receipt) else {
            return false;
        };
        receipt.authentication = authentication;
        let persisted = crate::sql_idempotency::persist_new(
            &self.store.files,
            self.store.receipt_name(&self.scope),
            &receipt,
        )
        .is_ok();
        if persisted {
            let _ = crate::sql_idempotency::remove_durable(
                &self.store.files,
                self.store.intent_name(&self.scope),
            );
        }
        persisted
    }

    fn abort(self) {
        let _ = crate::sql_idempotency::remove_durable(
            &self.store.files,
            self.store.intent_name(&self.scope),
        );
    }
}

#[derive(Serialize)]
struct IntentAuthentication<'a> {
    version: u8,
    scope_hash: &'a [u8; 32],
    created_at_ms: u64,
    binding: &'a IdempotencyBinding,
}

#[derive(Serialize)]
struct ReceiptAuthentication<'a> {
    version: u8,
    scope_hash: &'a [u8; 32],
    expires_at_ms: u64,
    binding: &'a IdempotencyBinding,
    response: &'a IdempotencyResponse,
}

fn intent_authentication_bytes(intent: &PersistedIntent) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&IntentAuthentication {
        version: intent.version,
        scope_hash: &intent.scope_hash,
        created_at_ms: intent.created_at_ms,
        binding: &intent.binding,
    })
}

fn receipt_authentication_bytes(receipt: &PersistedReceipt) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&ReceiptAuthentication {
        version: receipt.version,
        scope_hash: &receipt.scope_hash,
        expires_at_ms: receipt.expires_at_ms,
        binding: &receipt.binding,
        response: &receipt.response,
    })
}

fn intent_authentication(
    integrity: &crate::sql_idempotency::IdempotencyIntegrity,
    intent: &PersistedIntent,
) -> Result<[u8; 32], serde_json::Error> {
    intent_authentication_bytes(intent)
        .map(|bytes| integrity.authenticate(KIT_INTENT_MAC_DOMAIN, &bytes))
}

fn receipt_authentication(
    integrity: &crate::sql_idempotency::IdempotencyIntegrity,
    receipt: &PersistedReceipt,
) -> Result<[u8; 32], serde_json::Error> {
    receipt_authentication_bytes(receipt)
        .map(|bytes| integrity.authenticate(KIT_RECEIPT_MAC_DOMAIN, &bytes))
}

fn idempotency_scope_hash(owner: &str, key: &str) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"mongreldb-server-idempotency-v2\0");
    digest.update((owner.len() as u64).to_le_bytes());
    digest.update(owner.as_bytes());
    digest.update((key.len() as u64).to_le_bytes());
    digest.update(key.as_bytes());
    digest.finalize().into()
}

pub(crate) fn idempotency_owner(
    state: &AppState,
    authenticated_user: Option<&mongreldb_core::Principal>,
) -> std::result::Result<String, Box<Response>> {
    if let Some(principal) = authenticated_user {
        state
            .db()
            .resolve_current_principal(principal)
            .ok_or_else(|| Box::new(kit_core_error(&MongrelError::AuthRequired)))?;
        return Ok(format!(
            "user:{}:{}",
            principal.user_id, principal.created_epoch
        ));
    }
    if let Some(token) = state.auth_token.as_deref() {
        if state.db().require_auth_enabled()
            && !state
                .db()
                .principal_snapshot()
                .and_then(|principal| state.db().resolve_current_principal(&principal))
                .is_some_and(|principal| principal.is_admin)
        {
            return Err(Box::new(kit_core_error(&MongrelError::AuthRequired)));
        }
        let mut digest = Sha256::new();
        digest.update(b"mongreldb-server-bearer-owner-v1\0");
        digest.update((token.len() as u64).to_le_bytes());
        digest.update(token.as_bytes());
        return Ok(format!(
            "bearer:{}",
            crate::sql_idempotency::hex(&digest.finalize())
        ));
    }
    if state.user_auth || state.db().require_auth_enabled() {
        return Err(Box::new(kit_core_error(&MongrelError::AuthRequired)));
    }
    Ok("anonymous".into())
}

pub(crate) enum IdempotentJsonFailure {
    Safe(Box<Response>),
    Committed(IdempotencyResponse),
    OutcomeUnknown { epoch: Option<u64>, message: String },
}

impl IdempotentJsonFailure {
    pub(crate) fn safe(response: Response) -> Self {
        Self::Safe(Box::new(response))
    }

    fn into_response(self) -> Response {
        match self {
            Self::Safe(response) => *response,
            Self::Committed(response) => response.into_response(),
            Self::OutcomeUnknown { epoch, message } => idempotency_outcome_unknown(epoch, &message),
        }
    }
}

pub(crate) fn idempotent_core_failure(
    error: MongrelError,
    status: StatusCode,
    code: &str,
) -> IdempotentJsonFailure {
    match error {
        MongrelError::DurableCommit { epoch, message } => {
            IdempotentJsonFailure::Committed(IdempotencyResponse::new(
                StatusCode::CONFLICT,
                json!({
                    "status": "committed",
                    "committed": true,
                    "epoch": epoch,
                    "epoch_text": epoch.to_string(),
                    "retryable": false,
                    "error": { "code": "COMMIT_OUTCOME", "message": message }
                }),
            ))
        }
        MongrelError::CommitOutcomeUnknown { epoch, message } => {
            IdempotentJsonFailure::OutcomeUnknown {
                epoch: Some(epoch),
                message,
            }
        }
        error => IdempotentJsonFailure::safe(
            (
                status,
                Json(json!({
                    "status": "aborted",
                    "committed": false,
                    "retryable": false,
                    "error": { "code": code, "message": error.to_string() }
                })),
            )
                .into_response(),
        ),
    }
}

pub(crate) async fn idempotent_json<T, F>(
    state: &Arc<AppState>,
    owner: &str,
    operation: &str,
    key: Option<&str>,
    payload: &T,
    execute: F,
) -> Response
where
    T: Serialize + ?Sized,
    F: FnOnce() -> Result<Jval, IdempotentJsonFailure>,
{
    idempotent_json_validated(state, owner, operation, key, payload, execute, |_| Ok(())).await
}

pub(crate) async fn idempotent_json_validated<T, F, V>(
    state: &Arc<AppState>,
    owner: &str,
    operation: &str,
    key: Option<&str>,
    payload: &T,
    execute: F,
    validate_replay: V,
) -> Response
where
    T: Serialize + ?Sized,
    F: FnOnce() -> Result<Jval, IdempotentJsonFailure>,
    V: FnOnce(&Jval) -> Result<(), Box<Response>>,
{
    let Some(key) = key else {
        return match execute() {
            Ok(value) => Json(value).into_response(),
            Err(failure) => failure.into_response(),
        };
    };
    if let Err(message) = crate::sql_idempotency::SqlIdempotencyStore::validate_key(key) {
        return idempotency_error(StatusCode::BAD_REQUEST, "INVALID_IDEMPOTENCY_KEY", message);
    }
    let payload = match serde_json::to_vec(payload) {
        Ok(payload) => payload,
        Err(_) => {
            return idempotency_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "IDEMPOTENCY_STORE_UNAVAILABLE",
                "failed to serialize idempotency request binding",
            )
        }
    };
    match state.idem.begin(owner, key, operation, &payload).await {
        IdempotencyBegin::Replay(response) => match validate_replay(&response.body) {
            Ok(()) => response.into_response(),
            Err(response) => *response,
        },
        IdempotencyBegin::Execute(execution) => finish_idempotent_execution(execution, execute()),
        IdempotencyBegin::Mismatch => idempotency_error(
            StatusCode::CONFLICT,
            "IDEMPOTENCY_KEY_REUSE_MISMATCH",
            "idempotency key was already used with a different operation or payload",
        ),
        IdempotencyBegin::Indeterminate => idempotency_outcome_unknown(
            None,
            "a durable write intent exists without a durable receipt; the operation was not re-executed",
        ),
        IdempotencyBegin::Full => idempotency_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "IDEMPOTENCY_STORE_FULL",
            "idempotency receipt store is full",
        ),
        IdempotencyBegin::Unavailable => idempotency_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "IDEMPOTENCY_STORE_UNAVAILABLE",
            "could not durably reserve the idempotency key",
        ),
    }
}

fn finish_idempotent_execution(
    execution: IdempotencyExecution<'_>,
    result: Result<Jval, IdempotentJsonFailure>,
) -> Response {
    match result {
        Ok(body) => {
            let response = IdempotencyResponse::new(StatusCode::OK, body);
            if execution.commit(response.clone()) {
                response.into_response()
            } else {
                idempotency_outcome_unknown(
                    None,
                    "the operation completed but its durable idempotency receipt could not be published; it will not be re-executed",
                )
            }
        }
        Err(IdempotentJsonFailure::Safe(response)) => {
            execution.abort();
            *response
        }
        Err(IdempotentJsonFailure::Committed(response)) => {
            if execution.commit(response.clone()) {
                response.into_response()
            } else {
                idempotency_outcome_unknown(
                    None,
                    "the operation committed but its durable idempotency receipt could not be published; it will not be re-executed",
                )
            }
        }
        Err(IdempotentJsonFailure::OutcomeUnknown { epoch, message }) => {
            drop(execution);
            idempotency_outcome_unknown(epoch, &message)
        }
    }
}

fn idempotency_error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(json!({
            "status": "aborted",
            "committed": false,
            "retryable": matches!(code, "IDEMPOTENCY_STORE_FULL" | "IDEMPOTENCY_STORE_UNAVAILABLE"),
            "error": { "code": code, "message": message }
        })),
    )
        .into_response()
}

fn idempotency_outcome_unknown(epoch: Option<u64>, message: &str) -> Response {
    (
        StatusCode::CONFLICT,
        Json(json!({
            "status": "outcome_unknown",
            "committed": Jval::Null,
            "epoch": epoch,
            "epoch_text": epoch.map(|epoch| epoch.to_string()),
            "retryable": false,
            "error": { "code": "QUERY_OUTCOME_UNKNOWN", "message": message }
        })),
    )
        .into_response()
}

/// Preserve durable-write outcome information for non-idempotent HTTP routes.
/// Safe pre-commit errors stay with each route's existing response contract.
pub(crate) fn durable_core_error_response(error: &MongrelError) -> Option<Response> {
    match error {
        MongrelError::DurableCommit { epoch, message } => Some(
            (
                StatusCode::CONFLICT,
                Json(json!({
                    "status": "committed",
                    "committed": true,
                    "epoch": epoch,
                    "epoch_text": epoch.to_string(),
                    "retryable": false,
                    "error": { "code": "COMMIT_OUTCOME", "message": message }
                })),
            )
                .into_response(),
        ),
        MongrelError::CommitOutcomeUnknown { epoch, message } => {
            Some(idempotency_outcome_unknown(Some(*epoch), message))
        }
        _ => None,
    }
}

// ── Request models ──────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct KitTxnRequest {
    #[serde(default)]
    pub idempotency_key: Option<String>,
    pub ops: Vec<KitOp>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct KitTxnTableBinding {
    table_id: u64,
    schema_id: u64,
}

struct KitTxnPreflight {
    tables: Vec<KitTxnTableBinding>,
    security_version: u64,
}

#[derive(Serialize)]
struct KitTxnIdempotencyPayload<'a> {
    ops: &'a [KitOp],
    tables: &'a [KitTxnTableBinding],
    security_version: u64,
}

/// One typed operation in a `/kit/txn` batch (externally tagged: `{"put": …}`).
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KitOp {
    Put {
        table: String,
        /// Flat `[col_id, val, col_id, val, …]` cells.
        cells: Vec<Jval>,
        #[serde(default)]
        returning: bool,
    },
    Upsert {
        table: String,
        cells: Vec<Jval>,
        /// Cells applied on conflict (absent ⇒ DO NOTHING).
        update_cells: Option<Vec<Jval>>,
        #[serde(default)]
        returning: bool,
    },
    Delete {
        table: String,
        row_id: u64,
    },
    DeleteByPk {
        table: String,
        pk: Jval,
    },
}

// ── Response models ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KitTxnResponse {
    pub status: String,
    pub epoch: u64,
    pub epoch_text: String,
    pub results: Vec<KitOpResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum KitOpResult {
    Put {
        /// The engine allocates physical row ids at commit, so this is `None`
        /// for batch puts. The returned `row` carries the PK (and any auto_inc),
        /// which is how typed clients identify a logical row.
        row_id: Option<String>,
        auto_inc: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        row: Option<Vec<Jval>>,
    },
    Upsert {
        action: String,
        auto_inc: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        row: Option<Vec<Jval>>,
    },
    Deleted,
    NotFound,
}

/// Typed error envelope returned on a rejected batch.
#[derive(Debug, Serialize)]
pub struct KitErrorEnvelope {
    pub status: String,
    pub error: KitError,
}

#[derive(Debug, Serialize)]
pub struct KitError {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub op_index: Option<usize>,
}

impl KitError {
    fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
            op_index: None,
        }
    }
    fn with_op(mut self, idx: usize) -> Self {
        self.op_index = Some(idx);
        self
    }
}

/// Map an engine error from the commit path to a typed error code.
pub fn error_code(e: &MongrelError) -> &'static str {
    match e {
        MongrelError::TriggerValidation(_) => "TRIGGER_VALIDATION",
        MongrelError::Conflict(_) => {
            let m = format!("{e}");
            if m.contains("UNIQUE") {
                "UNIQUE_VIOLATION"
            } else if m.contains("FOREIGN KEY") {
                "FK_VIOLATION"
            } else {
                "CONFLICT"
            }
        }
        MongrelError::InvalidArgument(_) => {
            let m = format!("{e}");
            if m.contains("CHECK constraint") {
                "CHECK_VIOLATION"
            } else {
                "BAD_REQUEST"
            }
        }
        MongrelError::NotFound(_) => "NOT_FOUND",
        MongrelError::Deadlock { .. } => "DEADLOCK",
        MongrelError::SerializationFailure { .. } => "SERIALIZATION_FAILURE",
        MongrelError::CursorStale(_) => "CURSOR_STALE",
        MongrelError::CursorExpired => "CURSOR_EXPIRED",
        _ => "INTERNAL",
    }
}

// ── Metadata handlers ───────────────────────────────────────────────────────

pub async fn schema_all(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
) -> Response {
    let principal = request_principal(&state, &principal);
    let names = state.db().table_names();
    let mut tables = serde_json::Map::new();
    for name in &names {
        if let Ok(schema) = visible_schema(&state, name, principal.as_ref()) {
            tables.insert(name.clone(), schema_descriptor(&schema));
        }
    }
    Json(json!({ "tables": serde_json::Value::Object(tables) })).into_response()
}

pub async fn schema_one(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(table): Path<String>,
) -> Response {
    let principal = request_principal(&state, &principal);
    match visible_schema(&state, &table, principal.as_ref()) {
        Ok(schema) => Json(schema_descriptor(&schema)).into_response(),
        Err(error) => kit_core_error(&error),
    }
}

fn visible_schema(
    state: &AppState,
    table: &str,
    principal: Option<&mongreldb_core::Principal>,
) -> mongreldb_core::Result<Schema> {
    let allowed = state.db().select_column_ids_for(table, principal)?;
    let mut schema = state.db().table(table)?.lock().schema().clone();
    let restricted = allowed.len() != schema.columns.len();
    schema.columns.retain(|column| allowed.contains(&column.id));
    schema
        .indexes
        .retain(|index| allowed.contains(&index.column_id));
    schema
        .constraints
        .uniques
        .retain(|unique| unique.columns.iter().all(|column| allowed.contains(column)));
    schema.constraints.foreign_keys.retain(|foreign_key| {
        !restricted
            && foreign_key
                .columns
                .iter()
                .all(|column| allowed.contains(column))
    });
    if restricted {
        schema.constraints.checks.clear();
    }
    Ok(schema)
}

fn schema_descriptor(schema: &Schema) -> Jval {
    let columns: Vec<Jval> = schema
        .columns
        .iter()
        .map(|c| {
            json!({
                "id": c.id,
                "name": c.name,
                "ty": type_name(&c.ty),
                "primary_key": c.flags.contains(ColumnFlags::PRIMARY_KEY),
                "nullable": c.flags.contains(ColumnFlags::NULLABLE),
                "auto_increment": c.flags.contains(ColumnFlags::AUTO_INCREMENT),
                "embedding_source": c.embedding_source,
            })
        })
        .collect();
    let uniques: Vec<Jval> = schema
        .constraints
        .uniques
        .iter()
        .map(|u| json!({ "id": u.id, "name": u.name, "columns": u.columns }))
        .collect();
    let fks: Vec<Jval> = schema
        .constraints
        .foreign_keys
        .iter()
        .map(|f| {
            json!({
                "id": f.id,
                "name": f.name,
                "columns": f.columns,
                "ref_table": f.ref_table,
                "ref_columns": f.ref_columns,
                "on_delete": format!("{:?}", f.on_delete).to_lowercase(),
                "on_update": format!("{:?}", f.on_update).to_lowercase(),
            })
        })
        .collect();
    let checks: Vec<Jval> = schema
        .constraints
        .checks
        .iter()
        .map(|c| json!({ "id": c.id, "name": c.name }))
        .collect();
    let indexes: Vec<Jval> = schema
        .indexes
        .iter()
        .map(|index| {
            json!({
                "name": index.name,
                "column_id": index.column_id,
                "kind": index_kind_name(index.kind),
                "predicate": index.predicate,
                "options": index.options,
            })
        })
        .collect();
    json!({
        "schema_id": schema.schema_id,
        "columns": columns,
        "indexes": indexes,
        "constraints": { "uniques": uniques, "foreign_keys": fks, "checks": checks },
    })
}

fn index_kind_name(kind: IndexKind) -> &'static str {
    match kind {
        IndexKind::Bitmap => "bitmap",
        IndexKind::FmIndex => "fm_index",
        IndexKind::Ann => "ann",
        IndexKind::LearnedRange => "learned_range",
        IndexKind::MinHash => "minhash",
        IndexKind::Sparse => "sparse",
    }
}

fn type_name(ty: &mongreldb_core::schema::TypeId) -> &'static str {
    use mongreldb_core::schema::TypeId::*;
    match ty {
        Bool => "bool",
        Int8 => "int8",
        Int16 => "int16",
        Int32 => "int32",
        Int64 => "int64",
        UInt8 => "uint8",
        UInt16 => "uint16",
        UInt32 => "uint32",
        UInt64 => "uint64",
        Float32 => "float32",
        Float64 => "float64",
        TimestampNanos => "timestamp_nanos",
        Date32 => "date32",
        Bytes => "bytes",
        Embedding { .. } => "embedding",
        Date64 => "date64",
        Time64 => "time64",
        Interval => "interval",
        Decimal128 { .. } => "decimal128",
        Uuid => "uuid",
        Json => "json",
        Array { .. } => "array",
        Enum { .. } => "enum",
    }
}

fn parse_type_name(s: &str) -> std::result::Result<TypeId, String> {
    use mongreldb_core::schema::TypeId::*;
    Ok(match s {
        "bool" => Bool,
        "int8" | "i8" => Int8,
        "int16" | "i16" => Int16,
        "int32" | "i32" => Int32,
        "int64" | "i64" | "bigint" => Int64,
        "uint8" | "u8" => UInt8,
        "uint16" | "u16" => UInt16,
        "uint32" | "u32" => UInt32,
        "uint64" | "u64" => UInt64,
        "float32" | "f32" => Float32,
        "float64" | "f64" | "double" => Float64,
        "timestamp_nanos" | "timestamp" => TimestampNanos,
        "date32" | "date" => Date32,
        "bytes" | "varchar" | "text" | "string" => Bytes,
        // embedding(N) or embedding<N> — fixed-dimension vector column.
        other if other.starts_with("embedding") => {
            let rest = other.trim_start_matches("embedding").trim();
            let dim_str = rest
                .trim_start_matches(['(', '<'])
                .trim_end_matches([')', '>'])
                .trim();
            let dim: u32 = dim_str.parse().map_err(|_| {
                format!("invalid embedding dimension '{dim_str}'; expected embedding(<dim>)")
            })?;
            Embedding { dim }
        }
        other => return Err(format!("unknown type: {other}")),
    })
}

// ── Typed DDL: POST /kit/create_table ───────────────────────────────────────
//
// A constraint-aware table creator: the full ColumnFlags surface (nullable /
// primary_key / auto_increment / encrypted / encrypted_indexable) plus the
// engine's declarative TableConstraints (unique / FK / check). This lets a
// remote client self-provision a constraint-bearing table entirely over HTTP —
// the legacy `/tables` route only maps `primary_key`.

#[derive(Debug, Deserialize)]
pub struct KitCreateTableRequest {
    pub name: String,
    pub columns: Vec<KitColumnDef>,
    #[serde(default)]
    pub indexes: Vec<KitIndexDef>,
    #[serde(default)]
    pub constraints: TableConstraints,
}

#[derive(Debug, Deserialize)]
pub struct KitIndexDef {
    pub name: String,
    pub column_id: u16,
    pub kind: String,
    #[serde(default)]
    pub predicate: Option<String>,
    #[serde(default)]
    pub options: mongreldb_core::schema::IndexOptions,
}

fn kit_index_kind(kind: &str) -> std::result::Result<IndexKind, String> {
    match kind {
        "bitmap" => Ok(IndexKind::Bitmap),
        "fm" | "fm_index" => Ok(IndexKind::FmIndex),
        "ann" | "hnsw" => Ok(IndexKind::Ann),
        "learned_range" | "range" => Ok(IndexKind::LearnedRange),
        "minhash" | "lsh" => Ok(IndexKind::MinHash),
        "sparse" => Ok(IndexKind::Sparse),
        _ => Err(format!("unknown index kind: {kind}")),
    }
}

fn validate_index_type(kind: IndexKind, ty: &TypeId) -> bool {
    match kind {
        IndexKind::Ann => matches!(ty, TypeId::Embedding { .. }),
        IndexKind::Sparse | IndexKind::MinHash | IndexKind::FmIndex => {
            matches!(ty, TypeId::Bytes)
        }
        IndexKind::LearnedRange => matches!(
            ty,
            TypeId::Int8
                | TypeId::Int16
                | TypeId::Int32
                | TypeId::Int64
                | TypeId::UInt8
                | TypeId::UInt16
                | TypeId::UInt32
                | TypeId::UInt64
                | TypeId::Float32
                | TypeId::Float64
                | TypeId::TimestampNanos
                | TypeId::Date32
                | TypeId::Date64
                | TypeId::Time64
        ),
        IndexKind::Bitmap => !matches!(ty, TypeId::Embedding { .. }),
    }
}

#[derive(Debug, Deserialize)]
pub struct KitColumnDef {
    pub id: u16,
    pub name: String,
    pub ty: String,
    #[serde(default)]
    pub primary_key: bool,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub auto_increment: bool,
    #[serde(default)]
    pub encrypted: bool,
    #[serde(default)]
    pub encrypted_indexable: bool,
    #[serde(default)]
    pub enum_variants: Option<Vec<String>>,
    // `default_expr` accepts the dynamic `now` / `uuid` discriminators.
    #[serde(default)]
    pub default_expr: Option<String>,
    // `default_value` accepts a static JSON scalar, including explicit null.
    #[serde(default)]
    pub default_value: KitStaticDefault,
    #[serde(default)]
    pub embedding_source: Option<EmbeddingSource>,
}

/// Presence-aware default value. `None` means the key was omitted; `Some(Null)`
/// means the caller explicitly requested a static JSON null default.
#[derive(Debug, Default)]
pub struct KitStaticDefault(pub Option<Jval>);

impl<'de> Deserialize<'de> for KitStaticDefault {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self(Some(Jval::deserialize(deserializer)?)))
    }
}

/// Convert a KitColumnDef's default fields into an engine DefaultExpr.
fn kit_default_expr(
    c: &KitColumnDef,
    ty: &TypeId,
) -> std::result::Result<Option<DefaultExpr>, Box<Response>> {
    if let Some(expr) = c.default_expr.as_deref() {
        return match expr {
            "now" => Ok(Some(DefaultExpr::Now)),
            "uuid" => Ok(Some(DefaultExpr::Uuid)),
            other => Err(Box::new(
                (
                    StatusCode::BAD_REQUEST,
                    Json(KitErrorEnvelope {
                        status: "aborted".into(),
                        error: KitError::new(
                            "BAD_REQUEST",
                            format!("unknown default_expr \"{other}\""),
                        ),
                    }),
                )
                    .into_response(),
            )),
        };
    }
    let Some(value) = c.default_value.0.as_ref() else {
        return Ok(None);
    };
    if let (Jval::String(value), TypeId::Enum { variants }) = (value, ty) {
        if !variants.iter().any(|variant| variant == value) {
            return Err(Box::new(
                (
                    StatusCode::BAD_REQUEST,
                    Json(KitErrorEnvelope {
                        status: "aborted".into(),
                        error: KitError::new(
                            "BAD_REQUEST",
                            format!("default enum value \"{value}\" is not declared"),
                        ),
                    }),
                )
                    .into_response(),
            ));
        }
    }
    Ok(Some(DefaultExpr::Static(json_to_value(value, ty))))
}

pub async fn kit_create_table(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(req): Json<KitCreateTableRequest>,
) -> Response {
    if let Some(response) = crate::require_writes_open(&state) {
        return response;
    }
    if let Err(error) = state.db().require_for(
        request_principal(&state, &principal).as_ref(),
        &mongreldb_core::Permission::Ddl,
    ) {
        return kit_core_error(&error);
    }
    if let Err(msg) = validate_table_name(&req.name) {
        return (
            StatusCode::BAD_REQUEST,
            Json(KitErrorEnvelope {
                status: "aborted".into(),
                error: KitError::new("BAD_REQUEST", msg),
            }),
        )
            .into_response();
    }
    let mut columns = Vec::with_capacity(req.columns.len());
    for c in &req.columns {
        let ty = if c.ty == "enum" {
            let variants = c.enum_variants.clone().unwrap_or_default();
            if variants.is_empty() {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(KitErrorEnvelope {
                        status: "aborted".into(),
                        error: KitError::new(
                            "BAD_REQUEST",
                            "enum column requires non-empty enum_variants",
                        ),
                    }),
                )
                    .into_response();
            }
            TypeId::Enum {
                variants: variants.into(),
            }
        } else {
            match parse_type_name(&c.ty) {
                Ok(t) => t,
                Err(msg) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(KitErrorEnvelope {
                            status: "aborted".into(),
                            error: KitError::new("BAD_REQUEST", msg),
                        }),
                    )
                        .into_response();
                }
            }
        };
        let mut flags = ColumnFlags::empty();
        if c.primary_key {
            flags = flags.with(ColumnFlags::PRIMARY_KEY);
        }
        if c.nullable {
            flags = flags.with(ColumnFlags::NULLABLE);
        }
        if c.auto_increment {
            flags = flags.with(ColumnFlags::AUTO_INCREMENT);
        }
        if c.encrypted {
            flags = flags.with(ColumnFlags::ENCRYPTED);
        }
        if c.encrypted_indexable {
            flags = flags.with(ColumnFlags::ENCRYPTED_INDEXABLE);
        }
        columns.push(ColumnDef {
            id: c.id,
            name: c.name.clone(),
            ty: ty.clone(),
            flags,
            default_value: match kit_default_expr(c, &ty) {
                Ok(v) => v,
                Err(resp) => return *resp,
            },
            embedding_source: c.embedding_source.clone(),
        });
    }
    let mut names = std::collections::HashSet::new();
    let mut indexes = Vec::with_capacity(req.indexes.len());
    for index in &req.indexes {
        if !names.insert(&index.name) {
            return kit_bad_request(format!("duplicate index name: {}", index.name));
        }
        let Some(column) = columns.iter().find(|column| column.id == index.column_id) else {
            return kit_bad_request(format!(
                "index {} references unknown column {}",
                index.name, index.column_id
            ));
        };
        let kind = match kit_index_kind(&index.kind) {
            Ok(kind) => kind,
            Err(message) => return kit_bad_request(message),
        };
        if !validate_index_type(kind, &column.ty) {
            return kit_bad_request(format!(
                "index {} kind {} is incompatible with column {} type {}",
                index.name,
                index.kind,
                index.column_id,
                type_name(&column.ty)
            ));
        }
        indexes.push(IndexDef {
            name: index.name.clone(),
            column_id: index.column_id,
            kind,
            predicate: index.predicate.clone(),
            options: index.options.clone(),
        });
    }
    let schema = Schema {
        schema_id: 0,
        columns,
        indexes,
        colocation: vec![],
        constraints: req.constraints,
        clustered: false,
    };
    match state.db().create_table(&req.name, schema) {
        Ok(id) => Json(json!({
            "table_id": id,
            "table_id_text": id.to_string()
        }))
        .into_response(),
        Err(error) => durable_core_error_response(&error).unwrap_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(KitErrorEnvelope {
                    status: "aborted".into(),
                    error: KitError::new("BAD_REQUEST", format!("{error}")),
                }),
            )
                .into_response()
        }),
    }
}

fn kit_bad_request(message: String) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(KitErrorEnvelope {
            status: "aborted".into(),
            error: KitError::new("BAD_REQUEST", message),
        }),
    )
        .into_response()
}

fn kit_core_error(error: &MongrelError) -> Response {
    if let Some(response) = durable_core_error_response(error) {
        return response;
    }
    let (status, code) = match error {
        MongrelError::InvalidArgument(_)
        | MongrelError::Schema(_)
        | MongrelError::ColumnNotFound(_) => (StatusCode::BAD_REQUEST, "BAD_REQUEST"),
        MongrelError::AuthRequired | MongrelError::InvalidCredentials { .. } => {
            (StatusCode::UNAUTHORIZED, "AUTH_REQUIRED")
        }
        MongrelError::PermissionDenied { .. } => (StatusCode::FORBIDDEN, "PERMISSION_DENIED"),
        MongrelError::NotFound(_) => (StatusCode::NOT_FOUND, "NOT_FOUND"),
        MongrelError::Conflict(_) => (StatusCode::CONFLICT, "CONFLICT"),
        MongrelError::Deadlock { .. } => (StatusCode::CONFLICT, "DEADLOCK"),
        MongrelError::SerializationFailure { .. } => {
            (StatusCode::CONFLICT, "SERIALIZATION_FAILURE")
        }
        MongrelError::CursorStale(_) => (StatusCode::CONFLICT, "CURSOR_STALE"),
        MongrelError::CursorExpired => (StatusCode::GONE, "CURSOR_EXPIRED"),
        MongrelError::DeadlineExceeded => (StatusCode::GATEWAY_TIMEOUT, "DEADLINE_EXCEEDED"),
        MongrelError::WorkBudgetExceeded => (StatusCode::TOO_MANY_REQUESTS, "WORK_BUDGET_EXCEEDED"),
        MongrelError::Cancelled => (super::client_closed_request_status(), "CANCELLED"),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL"),
    };
    (
        status,
        Json(KitErrorEnvelope {
            status: "aborted".into(),
            error: KitError::new(code, error.to_string()),
        }),
    )
        .into_response()
}

// ── Typed transaction handler ───────────────────────────────────────────────

pub async fn kit_txn(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(req): Json<KitTxnRequest>,
) -> Response {
    // P0.2: cluster mode routes simple put batches through tablet Raft; never
    // open standalone AppState.db for ordinary Kit writes.
    if state.is_cluster_mode() {
        if let Some(response) = crate::cluster_data_plane::try_kit_txn(&state, &req).await {
            return response;
        }
        if let Some(response) = crate::refuse_cluster_standalone_data_plane(&state) {
            return response;
        }
    }
    if let Some(response) = crate::require_writes_open(&state) {
        return response;
    }
    let effective_principal = request_principal(&state, &principal);
    let preflight = match preflight_kit_txn(&state, &req, effective_principal.as_ref()) {
        Ok(preflight) => preflight,
        Err(response) => return *response,
    };
    let owner = match idempotency_owner(&state, principal.as_ref()) {
        Ok(owner) => owner,
        Err(response) => return *response,
    };
    let payload = KitTxnIdempotencyPayload {
        ops: &req.ops,
        tables: &preflight.tables,
        security_version: preflight.security_version,
    };
    idempotent_json(
        &state,
        &owner,
        "kit:txn",
        req.idempotency_key.as_deref(),
        &payload,
        || match execute_kit_txn(&state, &req, effective_principal, &preflight.tables) {
            Ok(response) => {
                serde_json::to_value(response).map_err(|_| IdempotentJsonFailure::OutcomeUnknown {
                    epoch: None,
                    message: "the transaction committed but its response could not be serialized"
                        .into(),
                })
            }
            Err(failure) => Err(failure),
        },
    )
    .await
}

// ── Native typed query endpoint (/kit/query) ────────────────────────────────
//
// A row-ID- and typed-cell-returning native query over the engine's `Condition`
// primitives (PK / bitmap equality / range / FM / null tests). This is the
// native counterpart to SQL reads: it returns physical row ids (SQL hides
// them). Conditions intersect in the row-id space; only survivors decode.

#[derive(Debug, Deserialize)]
pub struct KitQueryRequest {
    pub table: String,
    #[serde(default)]
    pub conditions: Vec<JsonCondition>,
    /// Projected column ids. Omit / empty ⇒ all columns.
    #[serde(default)]
    pub projection: Option<Vec<u16>>,
    /// Cap on the number of returned rows (after intersection).
    #[serde(default)]
    pub limit: Option<usize>,
    /// Number of matching rows to skip before applying `limit`.
    #[serde(default)]
    pub offset: usize,
    /// Signed, generation-bound continuation token returned by a previous page.
    #[serde(default)]
    pub cursor: Option<String>,
}

/// A condition over the row-id space, mirroring `mongreldb_core::query::Condition`
/// in a JSON-friendly, externally-tagged shape.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonCondition {
    Pk {
        value: Jval,
    },
    BitmapEq {
        column_id: u16,
        value: Jval,
    },
    BitmapIn {
        column_id: u16,
        values: Vec<Jval>,
    },
    Range {
        column_id: u16,
        lo: i64,
        hi: i64,
    },
    RangeF64 {
        column_id: u16,
        lo: f64,
        lo_inclusive: bool,
        hi: f64,
        hi_inclusive: bool,
    },
    IsNull {
        column_id: u16,
    },
    IsNotNull {
        column_id: u16,
    },
    FmContains {
        column_id: u16,
        pattern: String,
    },
    FmContainsAll {
        column_id: u16,
        patterns: Vec<String>,
    },
    Ann {
        column_id: u16,
        query: Vec<f32>,
        k: usize,
    },
    SparseMatch {
        column_id: u16,
        query: Vec<(u32, f32)>,
        k: usize,
    },
    #[serde(rename = "minhash_similar", alias = "min_hash_similar")]
    MinHashSimilar {
        column_id: u16,
        query: Vec<u64>,
        k: usize,
    },
    #[serde(rename = "minhash_similar_members")]
    MinHashSimilarMembers {
        column_id: u16,
        members: Vec<Jval>,
        k: usize,
    },
}

#[derive(Debug, Serialize)]
pub struct KitQueryResponse {
    pub rows: Vec<KitRow>,
    pub truncated: bool,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct KitRow {
    pub row_id: String,
    pub cells: Vec<Jval>,
}

const CURSOR_TTL_NANOS: i64 = 5 * 60 * 1_000_000_000;
const CURSOR_CLOCK_SKEW_NANOS: i64 = 5 * 1_000_000_000;

#[derive(Debug, Clone, Copy)]
struct KitCursorBinding {
    returned_count: u64,
    table_id: u64,
    schema_id: u64,
    data_generation: u64,
    security_version: u64,
    query_time_nanos: i64,
    issued_at_nanos: i64,
    expires_at_nanos: i64,
    principal_hash: [u8; 32],
    request_hash: [u8; 32],
}

#[derive(Debug, Clone, Copy)]
struct KitQueryCursor {
    epoch: u64,
    row_id: u64,
    binding: KitCursorBinding,
}

fn cursor_now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

fn cursor_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[(byte >> 4) as usize]));
        out.push(char::from(HEX[(byte & 0x0f) as usize]));
    }
    out
}

fn cursor_unhex<const N: usize>(value: &str) -> Option<[u8; N]> {
    if value.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(out)
}

fn cursor_principal_hash(principal: Option<&mongreldb_core::Principal>) -> [u8; 32] {
    let mut hash = Sha256::new();
    match principal {
        Some(principal) => {
            hash.update([1]);
            hash.update((principal.username.len() as u64).to_le_bytes());
            hash.update(principal.username.as_bytes());
        }
        None => hash.update([0]),
    }
    hash.finalize().into()
}

fn cursor_sign(key: &[u8; 32], payload: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts 32-byte keys");
    mac.update(payload.as_bytes());
    cursor_hex(&mac.finalize().into_bytes())
}

fn cursor_verified_payload<'a>(
    value: &'a str,
    key: &[u8; 32],
    kind: &str,
) -> Result<&'a str, MongrelError> {
    let invalid = || MongrelError::InvalidArgument(format!("invalid {kind} cursor"));
    let (payload, tag) = value.rsplit_once(':').ok_or_else(&invalid)?;
    let tag = cursor_unhex::<32>(tag).ok_or_else(&invalid)?;
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts 32-byte keys");
    mac.update(payload.as_bytes());
    mac.verify_slice(&tag).map_err(|_| invalid())?;
    Ok(payload)
}

fn cursor_binding_fields(binding: KitCursorBinding) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}:{}:{}:{}:{}",
        binding.returned_count,
        binding.table_id,
        binding.schema_id,
        binding.data_generation,
        binding.security_version,
        binding.query_time_nanos,
        binding.issued_at_nanos,
        binding.expires_at_nanos,
        cursor_hex(&binding.principal_hash),
        cursor_hex(&binding.request_hash),
    )
}

fn parse_cursor_binding(
    parts: &[&str],
    start: usize,
    kind: &str,
) -> Result<KitCursorBinding, MongrelError> {
    let invalid = || MongrelError::InvalidArgument(format!("invalid {kind} cursor"));
    if parts.len() != start + 10 {
        return Err(invalid());
    }
    let binding = KitCursorBinding {
        returned_count: parts[start].parse().map_err(|_| invalid())?,
        table_id: parts[start + 1].parse().map_err(|_| invalid())?,
        schema_id: parts[start + 2].parse().map_err(|_| invalid())?,
        data_generation: parts[start + 3].parse().map_err(|_| invalid())?,
        security_version: parts[start + 4].parse().map_err(|_| invalid())?,
        query_time_nanos: parts[start + 5].parse().map_err(|_| invalid())?,
        issued_at_nanos: parts[start + 6].parse().map_err(|_| invalid())?,
        expires_at_nanos: parts[start + 7].parse().map_err(|_| invalid())?,
        principal_hash: cursor_unhex(parts[start + 8]).ok_or_else(&invalid)?,
        request_hash: cursor_unhex(parts[start + 9]).ok_or_else(&invalid)?,
    };
    let lifetime = binding
        .expires_at_nanos
        .checked_sub(binding.issued_at_nanos)
        .ok_or_else(&invalid)?;
    let now = cursor_now_nanos();
    if lifetime <= 0
        || lifetime > CURSOR_TTL_NANOS
        || binding.issued_at_nanos > now.saturating_add(CURSOR_CLOCK_SKEW_NANOS)
    {
        return Err(invalid());
    }
    if now > binding.expires_at_nanos {
        return Err(MongrelError::CursorExpired);
    }
    Ok(binding)
}

fn new_cursor_binding(
    stamp: mongreldb_core::AuthorizedReadStamp,
    principal: Option<&mongreldb_core::Principal>,
    request_hash: [u8; 32],
    query_time_nanos: i64,
    returned_count: u64,
) -> KitCursorBinding {
    let issued_at_nanos = cursor_now_nanos();
    KitCursorBinding {
        returned_count,
        table_id: stamp.table_id,
        schema_id: stamp.schema_id,
        data_generation: stamp.data_generation,
        security_version: stamp.security_version,
        query_time_nanos,
        issued_at_nanos,
        expires_at_nanos: issued_at_nanos.saturating_add(CURSOR_TTL_NANOS),
        principal_hash: cursor_principal_hash(principal),
        request_hash,
    }
}

fn validate_cursor_identity(
    binding: KitCursorBinding,
    principal: Option<&mongreldb_core::Principal>,
    request_hash: [u8; 32],
) -> Result<(), MongrelError> {
    if binding.request_hash != request_hash {
        return Err(MongrelError::InvalidArgument(
            "cursor does not match request".into(),
        ));
    }
    if binding.principal_hash != cursor_principal_hash(principal) {
        return Err(MongrelError::CursorStale("cursor principal changed".into()));
    }
    Ok(())
}

fn validate_cursor_stamp(
    binding: KitCursorBinding,
    epoch: u64,
    stamp: mongreldb_core::AuthorizedReadStamp,
) -> Result<(), MongrelError> {
    if binding.table_id != stamp.table_id
        || binding.schema_id != stamp.schema_id
        || binding.data_generation != stamp.data_generation
        || binding.security_version != stamp.security_version
        || epoch != stamp.snapshot.epoch.0
    {
        return Err(MongrelError::CursorStale(
            "table, schema, index, or security generation changed".into(),
        ));
    }
    Ok(())
}

fn parse_kit_query_cursor(value: &str, key: &[u8; 32]) -> Result<KitQueryCursor, MongrelError> {
    if !value.starts_with("q2:") {
        return Err(MongrelError::CursorStale(
            "unsupported query cursor version".into(),
        ));
    }
    let payload = cursor_verified_payload(value, key, "query")?;
    let parts: Vec<_> = payload.split(':').collect();
    let invalid = || MongrelError::InvalidArgument("invalid query cursor".into());
    if parts.first() != Some(&"q2") {
        return Err(invalid());
    }
    Ok(KitQueryCursor {
        epoch: parts
            .get(1)
            .ok_or_else(&invalid)?
            .parse()
            .map_err(|_| invalid())?,
        row_id: parts
            .get(2)
            .ok_or_else(&invalid)?
            .parse()
            .map_err(|_| invalid())?,
        binding: parse_cursor_binding(&parts, 3, "query")?,
    })
}

fn format_kit_query_cursor(cursor: KitQueryCursor, key: &[u8; 32]) -> String {
    let payload = format!(
        "q2:{}:{}:{}",
        cursor.epoch,
        cursor.row_id,
        cursor_binding_fields(cursor.binding)
    );
    format!("{payload}:{}", cursor_sign(key, &payload))
}

#[derive(Debug, Deserialize)]
pub struct KitRetrieveRequest {
    pub table: String,
    pub retriever: JsonRetriever,
    #[serde(default)]
    pub deadline_ms: Option<u64>,
    #[serde(default)]
    pub max_work: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct KitAnnRerankRequest {
    pub table: String,
    pub column_id: u16,
    pub query: Vec<f32>,
    pub candidate_k: usize,
    pub limit: usize,
    pub metric: KitVectorMetric,
    #[serde(default)]
    pub deadline_ms: Option<u64>,
    #[serde(default)]
    pub max_work: Option<usize>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KitVectorMetric {
    Cosine,
    DotProduct,
    Euclidean,
}

impl From<KitVectorMetric> for VectorMetric {
    fn from(metric: KitVectorMetric) -> Self {
        match metric {
            KitVectorMetric::Cosine => Self::Cosine,
            KitVectorMetric::DotProduct => Self::DotProduct,
            KitVectorMetric::Euclidean => Self::Euclidean,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonRetriever {
    Ann {
        column_id: u16,
        query: Vec<f32>,
        k: usize,
    },
    Sparse {
        column_id: u16,
        query: Vec<(u32, f32)>,
        k: usize,
    },
    MinHash {
        column_id: u16,
        members: Vec<Jval>,
        k: usize,
    },
}

impl JsonRetriever {
    pub(crate) fn column_id(&self) -> u16 {
        match self {
            Self::Ann { column_id, .. }
            | Self::Sparse { column_id, .. }
            | Self::MinHash { column_id, .. } => *column_id,
        }
    }

    fn to_core(&self) -> Result<Retriever, String> {
        Ok(match self {
            Self::Ann {
                column_id,
                query,
                k,
            } => Retriever::Ann {
                column_id: *column_id,
                query: query.clone(),
                k: *k,
            },
            Self::Sparse {
                column_id,
                query,
                k,
            } => Retriever::Sparse {
                column_id: *column_id,
                query: query.clone(),
                k: *k,
            },
            Self::MinHash {
                column_id,
                members,
                k,
            } => Retriever::MinHash {
                column_id: *column_id,
                members: members.iter().map(set_member).collect::<Result<_, _>>()?,
                k: *k,
            },
        })
    }
}

fn set_member(value: &Jval) -> Result<SetMember, String> {
    match value {
        Jval::String(value) => Ok(SetMember::String(value.clone())),
        Jval::Number(value) => Ok(SetMember::Number(value.clone())),
        Jval::Bool(value) => Ok(SetMember::Boolean(*value)),
        _ => Err("set member must be a string, number, or boolean".into()),
    }
}

fn retriever_score_json(score: RetrieverScore) -> Jval {
    match score {
        RetrieverScore::AnnHammingDistance(value) => {
            json!({"kind":"ann_hamming_distance","value":value})
        }
        RetrieverScore::AnnCosineDistance(value) => {
            json!({"kind":"ann_cosine_distance","value":value})
        }
        RetrieverScore::SparseDotProduct(value) => {
            json!({"kind":"sparse_dot_product","value":value})
        }
        RetrieverScore::MinHashEstimatedJaccard(value) => {
            json!({"kind":"minhash_estimated_jaccard","value":value})
        }
    }
}

fn ann_candidate_distance_json(distance: AnnCandidateDistance) -> Jval {
    match distance {
        AnnCandidateDistance::Hamming(value) => json!({"kind":"hamming","value":value}),
        AnnCandidateDistance::Cosine(value) => json!({"kind":"cosine","value":value}),
    }
}

pub async fn kit_ai_metrics(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
) -> Response {
    let principal = request_principal(&state, &principal);
    if let Err(error) = state
        .db()
        .require_for(principal.as_ref(), &mongreldb_core::Permission::Admin)
    {
        return kit_core_error(&error);
    }
    let stats = state.db().rls_cache_stats();
    Json(json!({
        "rls_cache": {
            "entries": stats.entries,
            "bytes": stats.bytes,
            "hits": stats.hits,
            "misses": stats.misses,
            "evictions": stats.evictions,
            "build_nanos": stats.build_nanos,
            "rows_evaluated": stats.rows_evaluated,
        }
    }))
    .into_response()
}

/// Kit text → embed → ANN retrieve under the active semantic identity (P0.7).
///
/// Public wire for language clients that cannot call core `retrieve_text`
/// in-process. Request body:
/// `{ "table", "embedding_column", "text", "k"?: number, "deadline_ms"?, "max_work"? }`.
#[derive(Debug, Deserialize)]
pub struct KitRetrieveTextRequest {
    pub table: String,
    pub embedding_column: u16,
    pub text: String,
    #[serde(default)]
    pub k: Option<usize>,
    #[serde(default)]
    pub deadline_ms: Option<u64>,
    #[serde(default)]
    pub max_work: Option<usize>,
}

pub async fn kit_retrieve_text(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(req): Json<KitRetrieveTextRequest>,
) -> Response {
    let (timeout, context) = match ai_execution_options(req.deadline_ms, req.max_work) {
        Ok(options) => options,
        Err(error) => return kit_core_error(&error),
    };
    let principal = request_principal(&state, &principal);
    let column_id = req.embedding_column;
    if let Err(error) = state.db().require_columns_for(
        &req.table,
        mongreldb_core::ColumnOperation::Select,
        &[column_id],
        principal.as_ref(),
    ) {
        return kit_core_error(&error);
    }
    let k = req.k.unwrap_or(10);
    if k == 0 {
        return kit_bad_request("k must be greater than zero".into());
    }
    let table_name = req.table;
    let text = req.text;
    let worker_state = Arc::clone(&state);
    let result = run_ai(state, timeout, context, move |_context| {
        worker_state.db().retrieve_text_for_principal(
            &table_name,
            column_id,
            &text,
            mongreldb_core::embedding::TextSearchOptions::new(k),
            principal.as_ref(),
        )
    })
    .await;
    match result {
        Ok(retrieved) => {
            let provenance = &retrieved.provenance;
            let identity = &provenance.semantic_identity;
            Json(json!({
                "hits": retrieved.hits.into_iter().map(|hit| json!({
                    "row_id": hit.row_id.0.to_string(),
                    "rank": hit.rank,
                    "score": retriever_score_json(hit.score),
                })).collect::<Vec<_>>(),
                "provenance": {
                    "embedding_column": provenance.embedding_column,
                    "provider_registry_generation": provenance.provider_registry_generation,
                    "query_source_fingerprint": cursor_hex(&provenance.query_source_fingerprint),
                    "semantic_identity": {
                        "provider_id": identity.provider_id,
                        "provider_version": identity.provider_version,
                        "model_id": identity.model_id,
                        "model_version": identity.model_version,
                        "model_artifact_sha256": cursor_hex(&identity.model_artifact_sha256),
                        "tokenizer_sha256": cursor_hex(&identity.tokenizer_sha256),
                        "preprocessing_sha256": cursor_hex(&identity.preprocessing_sha256),
                        "dimension": identity.dimension,
                        "normalization": format!("{:?}", identity.normalization),
                    },
                },
            }))
            .into_response()
        }
        Err(error) => kit_core_error(&error),
    }
}

pub async fn kit_retrieve(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(req): Json<KitRetrieveRequest>,
) -> Response {
    let (timeout, context) = match ai_execution_options(req.deadline_ms, req.max_work) {
        Ok(options) => options,
        Err(error) => return kit_core_error(&error),
    };
    let principal = request_principal(&state, &principal);
    let column_id = req.retriever.column_id();
    if let Err(error) = state.db().require_columns_for(
        &req.table,
        mongreldb_core::ColumnOperation::Select,
        &[column_id],
        principal.as_ref(),
    ) {
        return kit_core_error(&error);
    }
    let retriever = match req.retriever.to_core() {
        Ok(retriever) => retriever,
        Err(message) => return kit_bad_request(message),
    };
    let table_name = req.table;
    let worker_state = Arc::clone(&state);
    let result = run_ai(state, timeout, context, move |context| {
        retry_authorized_context(
            &worker_state,
            &table_name,
            principal.as_ref(),
            &[column_id],
            &[],
            context,
            None,
            |table, snapshot, authorization, _| {
                table.retrieve_at_with_candidate_authorization_on_generation(
                    &retriever,
                    snapshot,
                    authorization,
                    Some(context),
                )
            },
        )
    })
    .await;
    match result {
        Ok(hits) => Json(json!({
            "hits": hits.into_iter().map(|hit| json!({
                "row_id": hit.row_id.0.to_string(),
                "rank": hit.rank,
                "score": retriever_score_json(hit.score)
            })).collect::<Vec<_>>()
        }))
        .into_response(),
        Err(error) => kit_core_error(&error),
    }
}

pub async fn kit_ann_rerank(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(req): Json<KitAnnRerankRequest>,
) -> Response {
    let (timeout, context) = match ai_execution_options(req.deadline_ms, req.max_work) {
        Ok(options) => options,
        Err(error) => return kit_core_error(&error),
    };
    let principal = request_principal(&state, &principal);
    if let Err(error) = state.db().require_columns_for(
        &req.table,
        mongreldb_core::ColumnOperation::Select,
        &[req.column_id],
        principal.as_ref(),
    ) {
        return kit_core_error(&error);
    }
    let request = AnnRerankRequest {
        column_id: req.column_id,
        query: req.query,
        candidate_k: req.candidate_k,
        limit: req.limit,
        metric: req.metric.into(),
    };
    let table_name = req.table;
    let worker_state = Arc::clone(&state);
    match run_ai(state, timeout, context, move |context| {
        retry_authorized_context(
            &worker_state,
            &table_name,
            principal.as_ref(),
            &[request.column_id],
            &[],
            context,
            None,
            |table, snapshot, authorization, _| {
                table.ann_rerank_at_with_candidate_authorization_on_generation(
                    &request,
                    snapshot,
                    authorization,
                    Some(context),
                )
            },
        )
    })
    .await
    {
        Ok(hits) => Json(json!({
            "hits": hits.into_iter().map(|hit| json!({
                "row_id": hit.row_id.0.to_string(),
                "candidate_distance": ann_candidate_distance_json(hit.candidate_distance),
                "exact_score": hit.exact_score,
            })).collect::<Vec<_>>()
        }))
        .into_response(),
        Err(error) => kit_core_error(&error),
    }
}

#[derive(Debug, Deserialize)]
pub struct KitSearchRequest {
    pub table: String,
    #[serde(default)]
    pub must: Vec<JsonCondition>,
    pub retrievers: Vec<KitNamedRetriever>,
    pub fusion: KitFusion,
    #[serde(default)]
    pub rerank: Option<KitRerank>,
    pub limit: usize,
    #[serde(default)]
    pub projection: Option<Vec<u16>>,
    #[serde(default)]
    pub deadline_ms: Option<u64>,
    #[serde(default)]
    pub max_work: Option<usize>,
    #[serde(default)]
    pub explain: bool,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Clone, Copy)]
struct KitSearchCursor {
    epoch: u64,
    final_score: f64,
    row_id: u64,
    binding: KitCursorBinding,
}

fn parse_kit_search_cursor(value: &str, key: &[u8; 32]) -> Result<KitSearchCursor, MongrelError> {
    if !value.starts_with("s2:") {
        return Err(MongrelError::CursorStale(
            "unsupported search cursor version".into(),
        ));
    }
    let payload = cursor_verified_payload(value, key, "search")?;
    let invalid = || MongrelError::InvalidArgument("invalid search cursor".into());
    let parts: Vec<_> = payload.split(':').collect();
    if parts.first() != Some(&"s2") {
        return Err(invalid());
    }
    let epoch = parts
        .get(1)
        .ok_or_else(&invalid)?
        .parse()
        .map_err(|_| invalid())?;
    let score_bits =
        u64::from_str_radix(parts.get(2).ok_or_else(&invalid)?, 16).map_err(|_| invalid())?;
    let row_id = parts
        .get(3)
        .ok_or_else(&invalid)?
        .parse()
        .map_err(|_| invalid())?;
    let final_score = f64::from_bits(score_bits);
    if !final_score.is_finite() {
        return Err(invalid());
    }
    Ok(KitSearchCursor {
        epoch,
        final_score,
        row_id,
        binding: parse_cursor_binding(&parts, 4, "search")?,
    })
}

fn format_kit_search_cursor(cursor: KitSearchCursor, key: &[u8; 32]) -> String {
    let payload = format!(
        "s2:{}:{:016x}:{}:{}",
        cursor.epoch,
        cursor.final_score.to_bits(),
        cursor.row_id,
        cursor_binding_fields(cursor.binding)
    );
    format!("{payload}:{}", cursor_sign(key, &payload))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KitRerank {
    ExactVector {
        embedding_column: u16,
        query: Vec<f32>,
        metric: KitVectorMetric,
        candidate_limit: usize,
        #[serde(default = "default_retriever_weight")]
        weight: f64,
    },
}

#[derive(Debug, Deserialize)]
pub struct KitNamedRetriever {
    pub name: String,
    #[serde(default = "default_retriever_weight")]
    pub weight: f64,
    #[serde(flatten)]
    pub retriever: JsonRetriever,
}

fn default_retriever_weight() -> f64 {
    1.0
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KitFusion {
    ReciprocalRank { constant: u32 },
}

fn execute_kit_search(
    state: &AppState,
    principal: Option<&mongreldb_core::Principal>,
    req: &KitSearchRequest,
    context: &mongreldb_core::query::AiExecutionContext,
) -> Result<Jval, MongrelError> {
    let cursor_mac_key = state.cursor_mac_key.get()?;
    let handle = state.db().table(&req.table)?;
    let schema = mongreldb_core::lock_table_with_context(&handle, Some(context))?
        .schema()
        .clone();
    let mut required = req
        .projection
        .clone()
        .unwrap_or_else(|| schema.columns.iter().map(|column| column.id).collect());
    for condition in &req.must {
        match condition {
            JsonCondition::Pk { .. } => {
                if let Some(primary_key) = schema.primary_key() {
                    required.push(primary_key.id);
                }
            }
            JsonCondition::BitmapEq { column_id, .. }
            | JsonCondition::BitmapIn { column_id, .. }
            | JsonCondition::Range { column_id, .. }
            | JsonCondition::RangeF64 { column_id, .. }
            | JsonCondition::IsNull { column_id }
            | JsonCondition::IsNotNull { column_id }
            | JsonCondition::FmContains { column_id, .. }
            | JsonCondition::FmContainsAll { column_id, .. }
            | JsonCondition::Ann { column_id, .. }
            | JsonCondition::SparseMatch { column_id, .. }
            | JsonCondition::MinHashSimilar { column_id, .. }
            | JsonCondition::MinHashSimilarMembers { column_id, .. } => required.push(*column_id),
        }
    }
    required.extend(
        req.retrievers
            .iter()
            .map(|retriever| retriever.retriever.column_id()),
    );
    if let Some(KitRerank::ExactVector {
        embedding_column, ..
    }) = &req.rerank
    {
        required.push(*embedding_column);
    }
    required.sort_unstable();
    required.dedup();
    state.db().require_columns_for(
        &req.table,
        mongreldb_core::ColumnOperation::Select,
        &required,
        principal,
    )?;
    let must = req
        .must
        .iter()
        .map(|condition| parse_condition(condition, &schema))
        .collect::<Result<Vec<_>, _>>()
        .map_err(MongrelError::InvalidArgument)?;
    let retrievers = req
        .retrievers
        .iter()
        .map(|retriever| {
            Ok(NamedRetriever {
                name: retriever.name.clone(),
                weight: retriever.weight,
                retriever: retriever
                    .retriever
                    .to_core()
                    .map_err(MongrelError::InvalidArgument)?,
            })
        })
        .collect::<Result<Vec<_>, MongrelError>>()?;
    let projection_work = req
        .projection
        .as_ref()
        .map_or(schema.columns.len(), Vec::len);
    let mut estimated_work = retrievers
        .iter()
        .filter(|named| named.weight != 0.0)
        .try_fold(
            req.must
                .len()
                .checked_add(projection_work)
                .ok_or(MongrelError::WorkBudgetExceeded)?,
            |total, named| {
                let k = match &named.retriever {
                    Retriever::Ann { k, .. }
                    | Retriever::Sparse { k, .. }
                    | Retriever::MinHash { k, .. } => *k,
                };
                total.checked_add(k).ok_or(MongrelError::WorkBudgetExceeded)
            },
        )?;
    if let Some(KitRerank::ExactVector {
        candidate_limit, ..
    }) = &req.rerank
    {
        estimated_work = estimated_work
            .checked_add(*candidate_limit)
            .ok_or(MongrelError::WorkBudgetExceeded)?;
    }
    if estimated_work > context.work_limit() {
        return Err(MongrelError::WorkBudgetExceeded);
    }
    let fusion = match &req.fusion {
        KitFusion::ReciprocalRank { constant } => Fusion::ReciprocalRank {
            constant: *constant,
        },
    };
    let request = SearchRequest {
        must,
        retrievers,
        fusion,
        rerank: req.rerank.as_ref().map(|rerank| match rerank {
            KitRerank::ExactVector {
                embedding_column,
                query,
                metric,
                candidate_limit,
                weight,
            } => mongreldb_core::query::Rerank::ExactVector {
                embedding_column: *embedding_column,
                query: query.clone(),
                metric: (*metric).into(),
                candidate_limit: *candidate_limit,
                weight: *weight,
            },
        }),
        limit: req.limit,
        projection: req.projection.clone(),
    };
    let request_hash = mongreldb_core::query::canonical_search_cursor_hash(&req.table, &request);
    let cursor = req
        .cursor
        .as_deref()
        .map(|value| parse_kit_search_cursor(value, &cursor_mac_key))
        .transpose()?;
    if let Some(cursor) = cursor {
        validate_cursor_identity(cursor.binding, principal, request_hash)?;
    }
    let epoch = cursor
        .map(|cursor| cursor.epoch)
        .unwrap_or_else(|| state.db().visible_epoch().0);
    let (snapshot, _snapshot_guard) = state.db().snapshot_at_owned(mongreldb_core::Epoch(epoch))?;
    let search_after = cursor
        .map(|cursor| {
            Ok::<_, MongrelError>(mongreldb_core::query::SearchAfter {
                final_score: cursor.final_score,
                row_id: RowId(cursor.row_id),
                returned_count: usize::try_from(cursor.binding.returned_count)
                    .map_err(|_| MongrelError::InvalidArgument("invalid search cursor".into()))?,
            })
        })
        .transpose()?;
    let read_context = cursor.map_or_else(
        || context.clone(),
        |cursor| context.with_query_time_nanos(cursor.binding.query_time_nanos),
    );
    let (result, trace) = mongreldb_core::trace::QueryTrace::capture(|| {
        retry_authorized_context_stamped(
            state,
            &req.table,
            principal,
            &required,
            if req.explain {
                &[mongreldb_core::Permission::Admin]
            } else {
                &[]
            },
            &read_context,
            Some(snapshot),
            |table, snapshot, authorization, effective_principal| {
                let mut hits = table.search_at_with_candidate_authorization_on_generation_after(
                    &request,
                    snapshot,
                    authorization,
                    Some(&read_context),
                    search_after,
                )?;
                state
                    .db()
                    .mask_search_hits_for(&req.table, &mut hits, effective_principal)?;
                Ok(hits)
            },
        )
    });
    let (hits, stamp) = result?;
    if let Some(cursor) = cursor {
        validate_cursor_stamp(cursor.binding, epoch, stamp)?;
    }
    read_context.checkpoint()?;
    let hit_count = u64::try_from(hits.len())
        .map_err(|_| MongrelError::InvalidArgument("search result count overflow".into()))?;
    let next_cursor = if hits.len() == req.limit {
        hits.last().map(|hit| {
            let binding = match cursor {
                Some(cursor) => KitCursorBinding {
                    returned_count: cursor.binding.returned_count.saturating_add(hit_count),
                    ..cursor.binding
                },
                None => new_cursor_binding(
                    stamp,
                    principal,
                    request_hash,
                    read_context.query_time_nanos(),
                    hit_count,
                ),
            };
            format_kit_search_cursor(
                KitSearchCursor {
                    epoch,
                    final_score: hit.final_score,
                    row_id: hit.row_id.0,
                    binding,
                },
                &cursor_mac_key,
            )
        })
    } else {
        None
    };
    let mut response = json!({
        "next_cursor": next_cursor,
        "hits": hits.into_iter().map(|hit| json!({
            "row_id": hit.row_id.0.to_string(),
            "cells": hit.cells.into_iter().flat_map(|(column_id, value)| [json!(column_id), value_to_json(&value)]).collect::<Vec<_>>(),
            "components": hit.components.into_iter().map(|component| json!({
                "retriever_name": component.retriever_name,
                "rank": component.rank,
                "raw_score": retriever_score_json(component.raw_score),
                "contribution": component.contribution,
            })).collect::<Vec<_>>(),
            "fused_score": hit.fused_score,
            "exact_rerank_score": hit.exact_rerank_score,
            "final_score": hit.final_score,
            "final_rank": hit.final_rank,
        })).collect::<Vec<_>>()
    });
    if req.explain {
        response["trace"] = json!({
            "authorization_nanos": trace.authorization_nanos,
            "rls_cache_hit": trace.rls_cache_hit,
            "rls_rows_evaluated": trace.rls_rows_evaluated,
            "rls_policy_columns_decoded": trace.rls_policy_columns_decoded,
            "authorization_retries": trace.authorization_retries,
            "hard_filter_nanos": trace.hard_filter_nanos,
            "ann_algorithm": trace.ann_algorithm.map(|algorithm| match algorithm {
                mongreldb_core::schema::AnnAlgorithm::Hnsw => "hnsw",
                mongreldb_core::schema::AnnAlgorithm::DiskAnn => "diskann",
                mongreldb_core::schema::AnnAlgorithm::Ivf => "ivf",
            }),
            "ann_quantization": trace.ann_quantization.map(|quantization| match quantization {
                mongreldb_core::schema::AnnQuantization::BinarySign => "binary_sign",
                mongreldb_core::schema::AnnQuantization::Dense => "dense",
                mongreldb_core::schema::AnnQuantization::Product { .. } => "product",
            }),
            "ann_backend": trace.ann_backend,
            "ann_candidate_nanos": trace.ann_candidate_nanos,
            "ann_candidate_cap_hit": trace.ann_candidate_cap_hit,
            "sparse_candidate_nanos": trace.sparse_candidate_nanos,
            "minhash_candidate_nanos": trace.minhash_candidate_nanos,
            "candidate_count": trace.candidate_count,
            "union_size": trace.union_size,
            "fusion_nanos": trace.fusion_nanos,
            "projection_nanos": trace.projection_nanos,
            "projection_rows": trace.projection_rows,
            "projection_cells": trace.projection_cells,
            "work_consumed": trace.work_consumed,
            "total_nanos": trace.total_nanos,
        });
    }
    Ok(response)
}

pub async fn kit_search(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(req): Json<KitSearchRequest>,
) -> Response {
    // P0.2 / P0.8: cluster mode serves tablet_rows with hybrid fusion when
    // multiple retrievers are present (fuse_distributed_hits → merge_hybrid_contributions).
    // Never open standalone AppState.db for Kit search.
    if state.is_cluster_mode() {
        if let Some(response) = crate::cluster_data_plane::try_kit_search(&state, &req).await {
            return response;
        }
        if let Some(response) = crate::refuse_cluster_standalone_data_plane(&state) {
            return response;
        }
    }
    let principal = request_principal(&state, &principal);
    if req.explain {
        if let Err(error) = state
            .db()
            .require_for(principal.as_ref(), &mongreldb_core::Permission::Admin)
        {
            return kit_core_error(&error);
        }
    }
    let (timeout, context) = match ai_execution_options(req.deadline_ms, req.max_work) {
        Ok(options) => options,
        Err(error) => return kit_core_error(&error),
    };
    let worker_state = Arc::clone(&state);
    match run_ai(state, timeout, context, move |context| {
        execute_kit_search(&worker_state, principal.as_ref(), &req, context)
    })
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => kit_core_error(&error),
    }
}

#[derive(Debug, Deserialize)]
pub struct KitSetSimilarityRequest {
    pub table: String,
    pub column_id: u16,
    pub members: Vec<Jval>,
    pub candidate_k: usize,
    pub min_jaccard: f32,
    pub limit: usize,
    #[serde(default)]
    pub deadline_ms: Option<u64>,
    #[serde(default)]
    pub max_work: Option<usize>,
}

pub async fn kit_set_similarity(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(req): Json<KitSetSimilarityRequest>,
) -> Response {
    let (timeout, context) = match ai_execution_options(req.deadline_ms, req.max_work) {
        Ok(options) => options,
        Err(error) => return kit_core_error(&error),
    };
    let principal = request_principal(&state, &principal);
    if let Err(error) = state.db().require_columns_for(
        &req.table,
        mongreldb_core::ColumnOperation::Select,
        &[req.column_id],
        principal.as_ref(),
    ) {
        return kit_core_error(&error);
    }
    let members = match req.members.iter().map(set_member).collect::<Result<_, _>>() {
        Ok(members) => members,
        Err(message) => return kit_bad_request(message),
    };
    let request = SetSimilarityRequest {
        column_id: req.column_id,
        members,
        candidate_k: req.candidate_k,
        min_jaccard: req.min_jaccard,
        limit: req.limit,
    };
    let table_name = req.table;
    let worker_state = Arc::clone(&state);
    let result = run_ai(state, timeout, context, move |context| {
        retry_authorized_context(
            &worker_state,
            &table_name,
            principal.as_ref(),
            &[request.column_id],
            &[],
            context,
            None,
            |table, snapshot, authorization, _| {
                table.set_similarity_at_with_candidate_authorization_on_generation(
                    &request,
                    snapshot,
                    authorization,
                    Some(context),
                )
            },
        )
    })
    .await;
    match result {
        Ok(hits) => Json(json!({
            "hits": hits.into_iter().map(|hit| json!({
                "row_id": hit.row_id.0.to_string(),
                "estimated_jaccard": hit.estimated_jaccard,
                "exact_jaccard": hit.exact_jaccard,
            })).collect::<Vec<_>>()
        }))
        .into_response(),
        Err(error) => kit_core_error(&error),
    }
}

pub async fn kit_query(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(req): Json<KitQueryRequest>,
) -> Response {
    if req.cursor.is_some() && req.offset != 0 {
        return kit_bad_request("offset cannot be combined with cursor".into());
    }
    if req.conditions.iter().any(|condition| {
        matches!(
            condition,
            JsonCondition::Ann { .. }
                | JsonCondition::SparseMatch { .. }
                | JsonCondition::MinHashSimilar { .. }
                | JsonCondition::MinHashSimilarMembers { .. }
        )
    }) {
        return kit_bad_request(
            "ranked AI conditions are not available on /kit/query; use /kit/retrieve or /kit/search"
                .into(),
        );
    }
    if req.offset > mongreldb_core::query::MAX_QUERY_OFFSET {
        return kit_bad_request(format!(
            "offset exceeds {}",
            mongreldb_core::query::MAX_QUERY_OFFSET
        ));
    }
    let cursor_mac_key = match state.cursor_mac_key.get() {
        Ok(key) => key,
        Err(error) => return kit_core_error(&error),
    };
    let principal = request_principal(&state, &principal);
    let handle = match state.db().table(&req.table) {
        Ok(h) => h,
        Err(error) => return kit_core_error(&error),
    };
    let schema = handle.lock().schema().clone();
    let allowed = match state
        .db()
        .select_column_ids_for(&req.table, principal.as_ref())
    {
        Ok(allowed) => allowed,
        Err(error) => {
            return kit_core_error(&error);
        }
    };
    let projection_ids = req
        .projection
        .as_ref()
        .filter(|projection| !projection.is_empty())
        .cloned()
        .unwrap_or_else(|| allowed.clone());
    if projection_ids.len() > mongreldb_core::query::MAX_PROJECTION_COLUMNS {
        return kit_bad_request(format!(
            "projection exceeds {} columns",
            mongreldb_core::query::MAX_PROJECTION_COLUMNS
        ));
    }
    let mut required = projection_ids.clone();
    for condition in &req.conditions {
        match condition {
            JsonCondition::Pk { .. } => {
                if let Some(primary_key) = schema.primary_key() {
                    required.push(primary_key.id);
                }
            }
            JsonCondition::BitmapEq { column_id, .. }
            | JsonCondition::BitmapIn { column_id, .. }
            | JsonCondition::Range { column_id, .. }
            | JsonCondition::RangeF64 { column_id, .. }
            | JsonCondition::IsNull { column_id }
            | JsonCondition::IsNotNull { column_id }
            | JsonCondition::FmContains { column_id, .. }
            | JsonCondition::FmContainsAll { column_id, .. }
            | JsonCondition::Ann { column_id, .. }
            | JsonCondition::SparseMatch { column_id, .. }
            | JsonCondition::MinHashSimilar { column_id, .. }
            | JsonCondition::MinHashSimilarMembers { column_id, .. } => required.push(*column_id),
        }
    }
    required.sort_unstable();
    required.dedup();
    if let Err(error) = state.db().require_columns_for(
        &req.table,
        mongreldb_core::ColumnOperation::Select,
        &required,
        principal.as_ref(),
    ) {
        return kit_core_error(&error);
    }

    // Translate JSON conditions → engine Conditions.
    let conditions = match req
        .conditions
        .iter()
        .map(|condition| parse_condition(condition, &schema))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(conditions) => conditions,
        Err(message) => return kit_bad_request(message),
    };
    let limit = req.limit.unwrap_or(mongreldb_core::query::MAX_FINAL_LIMIT);
    if limit == 0 || limit > mongreldb_core::query::MAX_FINAL_LIMIT {
        return kit_bad_request(format!(
            "limit must be between 1 and {}",
            mongreldb_core::query::MAX_FINAL_LIMIT
        ));
    }
    let request_hash = mongreldb_core::query::canonical_query_cursor_hash(
        &req.table,
        &conditions,
        Some(&projection_ids),
    );
    let cursor = match req
        .cursor
        .as_deref()
        .map(|value| parse_kit_query_cursor(value, &cursor_mac_key))
        .transpose()
    {
        Ok(cursor) => cursor,
        Err(error) => return kit_core_error(&error),
    };
    if let Some(cursor) = cursor {
        if let Err(error) =
            validate_cursor_identity(cursor.binding, principal.as_ref(), request_hash)
        {
            return kit_core_error(&error);
        }
    }
    let epoch = cursor
        .map(|cursor| cursor.epoch)
        .unwrap_or_else(|| state.db().visible_epoch().0);
    let (snapshot, _snapshot_guard) =
        match state.db().snapshot_at_owned(mongreldb_core::Epoch(epoch)) {
            Ok(snapshot) => snapshot,
            Err(error) => return kit_core_error(&error),
        };
    let fetch_limit = limit
        .saturating_add(1)
        .min(mongreldb_core::query::MAX_FINAL_LIMIT);
    let q = Query {
        conditions,
        limit: Some(fetch_limit),
        offset: req.offset,
    };
    let after_row_id = cursor.map(|cursor| RowId(cursor.row_id));
    let query_time_nanos =
        cursor.map_or_else(cursor_now_nanos, |cursor| cursor.binding.query_time_nanos);

    let projection = projection_ids
        .into_iter()
        .collect::<std::collections::HashSet<_>>();
    let principal_catalog_bound = principal
        .as_ref()
        .is_some_and(|principal| state.db().resolve_principal(&principal.username).is_some());
    let (rows, stamp) = match state.db().with_authorized_read_context_stamped(
        &req.table,
        principal.as_ref(),
        principal_catalog_bound,
        Some(&mongreldb_core::ReadAuthorization {
            operation: mongreldb_core::ColumnOperation::Select,
            columns: required,
            permissions: Vec::new(),
        }),
        None,
        Some(snapshot),
        |table, snapshot, allowed, effective_principal| {
            let rows = table.query_at_with_allowed_after_at_time(
                &q,
                snapshot,
                allowed,
                after_row_id,
                query_time_nanos,
            )?;
            state
                .db()
                .secure_rows_for(&req.table, rows, effective_principal)
        },
    ) {
        Ok(rows) => rows,
        Err(error) => return kit_core_error(&error),
    };
    if let Some(cursor) = cursor {
        if let Err(error) = validate_cursor_stamp(cursor.binding, epoch, stamp) {
            return kit_core_error(&error);
        }
    }
    let truncated = rows.len() > limit
        || (limit == mongreldb_core::query::MAX_FINAL_LIMIT && rows.len() == limit);
    let mut out: Vec<KitRow> = Vec::with_capacity(rows.len().min(limit));
    let mut last_row_id = None;
    for r in rows.into_iter().take(limit) {
        last_row_id = Some(r.row_id);
        let cells: Vec<Jval> = schema
            .columns
            .iter()
            .filter(|column| projection.contains(&column.id))
            .filter_map(|column| {
                r.columns
                    .get(&column.id)
                    .map(|value| vec![json!(column.id), value_to_json(value)])
            })
            .flatten()
            .collect();
        out.push(KitRow {
            row_id: r.row_id.0.to_string(),
            cells,
        });
    }
    let next_cursor = if truncated {
        last_row_id.map(|row_id| {
            let returned_count = u64::try_from(out.len()).unwrap_or(u64::MAX);
            let binding = match cursor {
                Some(cursor) => KitCursorBinding {
                    returned_count: cursor.binding.returned_count.saturating_add(returned_count),
                    ..cursor.binding
                },
                None => new_cursor_binding(
                    stamp,
                    principal.as_ref(),
                    request_hash,
                    query_time_nanos,
                    returned_count,
                ),
            };
            format_kit_query_cursor(
                KitQueryCursor {
                    epoch,
                    row_id: row_id.0,
                    binding,
                },
                &cursor_mac_key,
            )
        })
    } else {
        None
    };
    Json(KitQueryResponse {
        rows: out,
        truncated,
        next_cursor,
    })
    .into_response()
}

fn parse_condition(c: &JsonCondition, schema: &Schema) -> std::result::Result<Condition, String> {
    Ok(match c {
        JsonCondition::Pk { value } => {
            let pk = schema.primary_key().ok_or("table has no primary key")?;
            Condition::Pk(json_to_value(value, &pk.ty).encode_key())
        }
        JsonCondition::BitmapEq { column_id, value } => {
            let ty = col_type(schema, *column_id)?;
            Condition::BitmapEq {
                column_id: *column_id,
                value: json_to_value(value, &ty).encode_key(),
            }
        }
        JsonCondition::BitmapIn { column_id, values } => {
            let ty = col_type(schema, *column_id)?;
            Condition::BitmapIn {
                column_id: *column_id,
                values: values
                    .iter()
                    .map(|v| json_to_value(v, &ty).encode_key())
                    .collect(),
            }
        }
        JsonCondition::Range { column_id, lo, hi } => Condition::Range {
            column_id: *column_id,
            lo: *lo,
            hi: *hi,
        },
        JsonCondition::RangeF64 {
            column_id,
            lo,
            lo_inclusive,
            hi,
            hi_inclusive,
        } => Condition::RangeF64 {
            column_id: *column_id,
            lo: *lo,
            lo_inclusive: *lo_inclusive,
            hi: *hi,
            hi_inclusive: *hi_inclusive,
        },
        JsonCondition::IsNull { column_id } => Condition::IsNull {
            column_id: *column_id,
        },
        JsonCondition::IsNotNull { column_id } => Condition::IsNotNull {
            column_id: *column_id,
        },
        JsonCondition::FmContains { column_id, pattern } => Condition::FmContains {
            column_id: *column_id,
            pattern: pattern.as_bytes().to_vec(),
        },
        JsonCondition::FmContainsAll {
            column_id,
            patterns,
        } => Condition::FmContainsAll {
            column_id: *column_id,
            patterns: patterns.iter().map(|s| s.as_bytes().to_vec()).collect(),
        },
        JsonCondition::Ann {
            column_id,
            query,
            k,
        } => Condition::Ann {
            column_id: *column_id,
            query: query.clone(),
            k: *k,
        },
        JsonCondition::SparseMatch {
            column_id,
            query,
            k,
        } => Condition::SparseMatch {
            column_id: *column_id,
            query: query.clone(),
            k: *k,
        },
        JsonCondition::MinHashSimilar {
            column_id,
            query,
            k,
        } => Condition::MinHashSimilar {
            column_id: *column_id,
            query: query.clone(),
            k: *k,
        },
        JsonCondition::MinHashSimilarMembers {
            column_id,
            members,
            k,
        } => Condition::MinHashSimilar {
            column_id: *column_id,
            query: members
                .iter()
                .map(mongreldb_core::index::minhash_member_hash_v1)
                .collect::<Result<Vec<_>, _>>()
                .map_err(str::to_string)?,
            k: *k,
        },
    })
}

fn col_type(
    schema: &Schema,
    column_id: u16,
) -> std::result::Result<mongreldb_core::schema::TypeId, String> {
    schema
        .columns
        .iter()
        .find(|c| c.id == column_id)
        .map(|c| c.ty.clone())
        .ok_or_else(|| format!("unknown column id {column_id}"))
}

fn preflight_kit_txn(
    state: &AppState,
    req: &KitTxnRequest,
    principal: Option<&mongreldb_core::Principal>,
) -> Result<KitTxnPreflight, Box<Response>> {
    for _ in 0..3 {
        let security_version = state.db().security_version();
        let tables = preflight_kit_txn_once(state, req, principal)?;
        if state.db().security_version() == security_version {
            return Ok(KitTxnPreflight {
                tables,
                security_version,
            });
        }
    }
    Err(Box::new(kit_core_error(&MongrelError::Conflict(
        "authorization changed during transaction preflight".into(),
    ))))
}

fn preflight_kit_txn_once(
    state: &AppState,
    req: &KitTxnRequest,
    principal: Option<&mongreldb_core::Principal>,
) -> Result<Vec<KitTxnTableBinding>, Box<Response>> {
    let mut bindings = Vec::with_capacity(req.ops.len());
    for (index, op) in req.ops.iter().enumerate() {
        let table = match op {
            KitOp::Put { table, .. }
            | KitOp::Upsert { table, .. }
            | KitOp::Delete { table, .. }
            | KitOp::DeleteByPk { table, .. } => table,
        };
        let mut stable = None;
        for _ in 0..3 {
            let (binding, schema) = current_kit_table(state, table)
                .map_err(|error| Box::new(kit_core_error(&error)))?;
            match op {
                KitOp::Put {
                    cells, returning, ..
                } => {
                    let cells = parse_cells(cells, &schema)
                        .map_err(|message| Box::new(op_error_msg(index, "BAD_REQUEST", message)))?;
                    let columns = cells.iter().map(|(column, _)| *column).collect::<Vec<_>>();
                    state
                        .db()
                        .require_columns_for(
                            table,
                            mongreldb_core::ColumnOperation::Insert,
                            &columns,
                            principal,
                        )
                        .map_err(|error| Box::new(kit_core_error(&error)))?;
                    if *returning {
                        require_returning_columns(state, table, &schema, principal)?;
                    }
                }
                KitOp::Upsert {
                    cells,
                    update_cells,
                    returning,
                    ..
                } => {
                    let cells = parse_cells(cells, &schema)
                        .map_err(|message| Box::new(op_error_msg(index, "BAD_REQUEST", message)))?;
                    let columns = cells.iter().map(|(column, _)| *column).collect::<Vec<_>>();
                    state
                        .db()
                        .require_columns_for(
                            table,
                            mongreldb_core::ColumnOperation::Insert,
                            &columns,
                            principal,
                        )
                        .map_err(|error| Box::new(kit_core_error(&error)))?;
                    if let Some(update_cells) = update_cells {
                        let update_cells =
                            parse_cells(update_cells, &schema).map_err(|message| {
                                Box::new(op_error_msg(index, "BAD_REQUEST", message))
                            })?;
                        let columns = update_cells
                            .iter()
                            .map(|(column, _)| *column)
                            .collect::<Vec<_>>();
                        state
                            .db()
                            .require_columns_for(
                                table,
                                mongreldb_core::ColumnOperation::Update,
                                &columns,
                                principal,
                            )
                            .map_err(|error| Box::new(kit_core_error(&error)))?;
                    }
                    if *returning {
                        require_returning_columns(state, table, &schema, principal)?;
                    }
                }
                KitOp::Delete { .. } => state
                    .db()
                    .require_for(
                        principal,
                        &mongreldb_core::Permission::Delete {
                            table: table.clone(),
                        },
                    )
                    .map_err(|error| Box::new(kit_core_error(&error)))?,
                KitOp::DeleteByPk { pk, .. } => {
                    pk_value(pk, &schema)
                        .map_err(|message| Box::new(op_error_msg(index, "BAD_REQUEST", message)))?;
                    state
                        .db()
                        .require_for(
                            principal,
                            &mongreldb_core::Permission::Delete {
                                table: table.clone(),
                            },
                        )
                        .map_err(|error| Box::new(kit_core_error(&error)))?;
                }
            }
            if state.db().table_identity(table).ok() == Some((binding.table_id, binding.schema_id))
            {
                stable = Some(binding);
                break;
            }
        }
        bindings.push(stable.ok_or_else(|| {
            Box::new(kit_core_error(&MongrelError::Conflict(format!(
                "table {table:?} changed during request authorization"
            ))))
        })?);
    }
    Ok(bindings)
}

fn require_returning_columns(
    state: &AppState,
    table: &str,
    schema: &Schema,
    principal: Option<&mongreldb_core::Principal>,
) -> Result<(), Box<Response>> {
    let columns = schema
        .columns
        .iter()
        .map(|column| column.id)
        .collect::<Vec<_>>();
    state
        .db()
        .require_columns_for(
            table,
            mongreldb_core::ColumnOperation::Select,
            &columns,
            principal,
        )
        .map_err(|error| Box::new(kit_core_error(&error)))
}

fn current_kit_table(
    state: &AppState,
    table: &str,
) -> mongreldb_core::Result<(KitTxnTableBinding, Schema)> {
    for _ in 0..3 {
        let (table_id, schema_id) = state.db().table_identity(table)?;
        let schema = state.db().table(table)?.lock().schema().clone();
        if schema.schema_id == schema_id
            && state.db().table_identity(table).ok() == Some((table_id, schema_id))
        {
            return Ok((
                KitTxnTableBinding {
                    table_id,
                    schema_id,
                },
                schema,
            ));
        }
    }
    Err(MongrelError::Conflict(format!(
        "table {table:?} changed during request authorization"
    )))
}

fn execute_kit_txn(
    state: &AppState,
    req: &KitTxnRequest,
    principal: Option<mongreldb_core::Principal>,
    bindings: &[KitTxnTableBinding],
) -> Result<KitTxnResponse, IdempotentJsonFailure> {
    // 1. Structural pre-validation: resolve each op against the live schemas and
    //    parse cells into typed Values. This gives per-op error attribution
    //    (op_index) for malformed input BEFORE consuming an epoch.
    enum Action {
        Put {
            table: String,
            cells: Vec<(u16, Value)>,
        },
        Upsert {
            table: String,
            cells: Vec<(u16, Value)>,
            update_cells: Option<Vec<(u16, Value)>>,
        },
        Delete {
            table: String,
            row_id: RowId,
        },
        DeleteByPk {
            table: String,
            key: Value,
        },
    }
    struct Parsed {
        returning: bool,
        binding: KitTxnTableBinding,
        action: Action,
    }

    if bindings.len() != req.ops.len() {
        return Err(IdempotentJsonFailure::OutcomeUnknown {
            epoch: None,
            message: "transaction resource binding was incomplete".into(),
        });
    }
    let mut parsed: Vec<Parsed> = Vec::with_capacity(req.ops.len());
    for (i, op) in req.ops.iter().enumerate() {
        let binding = &bindings[i];
        match op {
            KitOp::Put {
                table,
                cells,
                returning,
            } => {
                let schema = lookup_bound_schema(state, table, binding)
                    .map_err(|e| idempotent_op_failure(i, e))?;
                let cells = parse_cells(cells, &schema)
                    .map_err(|m| IdempotentJsonFailure::safe(op_error_msg(i, "BAD_REQUEST", m)))?;
                parsed.push(Parsed {
                    returning: *returning,
                    binding: binding.clone(),
                    action: Action::Put {
                        table: table.clone(),
                        cells,
                    },
                });
            }
            KitOp::Upsert {
                table,
                cells,
                update_cells,
                returning,
            } => {
                let schema = lookup_bound_schema(state, table, binding)
                    .map_err(|e| idempotent_op_failure(i, e))?;
                let cells = parse_cells(cells, &schema)
                    .map_err(|m| IdempotentJsonFailure::safe(op_error_msg(i, "BAD_REQUEST", m)))?;
                let upd = match update_cells {
                    Some(uc) => Some(parse_cells(uc, &schema).map_err(|m| {
                        IdempotentJsonFailure::safe(op_error_msg(i, "BAD_REQUEST", m))
                    })?),
                    None => None,
                };
                parsed.push(Parsed {
                    returning: *returning,
                    binding: binding.clone(),
                    action: Action::Upsert {
                        table: table.clone(),
                        cells,
                        update_cells: upd,
                    },
                });
            }
            KitOp::Delete { table, row_id } => {
                lookup_bound_schema(state, table, binding)
                    .map_err(|e| idempotent_op_failure(i, e))?;
                parsed.push(Parsed {
                    returning: false,
                    binding: binding.clone(),
                    action: Action::Delete {
                        table: table.clone(),
                        row_id: RowId(*row_id),
                    },
                });
            }
            KitOp::DeleteByPk { table, pk } => {
                let schema = lookup_bound_schema(state, table, binding)
                    .map_err(|e| idempotent_op_failure(i, e))?;
                let key = pk_value(pk, &schema)
                    .map_err(|m| IdempotentJsonFailure::safe(op_error_msg(i, "BAD_REQUEST", m)))?;
                parsed.push(Parsed {
                    returning: false,
                    binding: binding.clone(),
                    action: Action::DeleteByPk {
                        table: table.clone(),
                        key,
                    },
                });
            }
        }
    }

    // 2. Execute the whole batch inside ONE core transaction. Constraint
    //    enforcement (unique / FK / check) is authoritative at commit; any
    //    violation aborts the entire batch atomically (no partial commit).
    let db = Arc::clone(state.db());
    let mut transaction = db.begin_as(principal);
    let outcome: mongreldb_core::Result<Vec<KitOpResult>> = (|| {
        let mut results: Vec<KitOpResult> = Vec::with_capacity(parsed.len());
        for p in &parsed {
            match &p.action {
                Action::Put { table, cells } => {
                    let pr = transaction.put_returning_bound(
                        table,
                        p.binding.table_id,
                        p.binding.schema_id,
                        cells.clone(),
                    )?;
                    results.push(KitOpResult::Put {
                        row_id: None,
                        auto_inc: pr.auto_inc,
                        row: if p.returning {
                            Some(row_to_json(&pr.row))
                        } else {
                            None
                        },
                    });
                }
                Action::Upsert {
                    table,
                    cells,
                    update_cells,
                } => {
                    let action = match update_cells {
                        Some(uc) => UpsertAction::DoUpdate(uc.clone()),
                        None => UpsertAction::DoNothing,
                    };
                    let ur = transaction.upsert_bound(
                        table,
                        p.binding.table_id,
                        p.binding.schema_id,
                        cells.clone(),
                        action,
                    )?;
                    let action_str = match ur.action {
                        UpsertActionKind::Inserted => "inserted",
                        UpsertActionKind::Updated => "updated",
                        UpsertActionKind::Unchanged => "unchanged",
                    };
                    results.push(KitOpResult::Upsert {
                        action: action_str.to_string(),
                        auto_inc: ur.auto_inc,
                        row: if p.returning {
                            Some(row_to_json(&ur.row))
                        } else {
                            None
                        },
                    });
                }
                Action::Delete { table, row_id } => {
                    transaction.delete_bound(
                        table,
                        p.binding.table_id,
                        p.binding.schema_id,
                        *row_id,
                    )?;
                    results.push(KitOpResult::Deleted);
                }
                Action::DeleteByPk { table, key } => {
                    if transaction.delete_by_pk_bound(
                        table,
                        p.binding.table_id,
                        p.binding.schema_id,
                        key,
                    )? {
                        results.push(KitOpResult::Deleted);
                    } else {
                        results.push(KitOpResult::NotFound);
                    }
                }
            }
        }
        Ok(results)
    })();

    let results = match outcome {
        Ok(r) => r,
        Err(error) => return Err(idempotent_txn_failure(error)),
    };

    for (operation, binding) in req.ops.iter().zip(bindings) {
        let table = match operation {
            KitOp::Put { table, .. }
            | KitOp::Upsert { table, .. }
            | KitOp::Delete { table, .. }
            | KitOp::DeleteByPk { table, .. } => table,
        };
        if state.db().table_identity(table).ok() != Some((binding.table_id, binding.schema_id)) {
            return Err(idempotent_txn_failure(MongrelError::Conflict(format!(
                "table {table:?} changed before transaction commit"
            ))));
        }
    }

    match transaction.commit() {
        Ok(epoch) => Ok(KitTxnResponse {
            status: "committed".into(),
            epoch: epoch.0,
            epoch_text: epoch.0.to_string(),
            results,
        }),
        Err(MongrelError::DurableCommit { epoch, message }) => {
            Err(IdempotentJsonFailure::Committed(IdempotencyResponse::new(
                StatusCode::CONFLICT,
                json!({
                    "status": "committed",
                    "committed": true,
                    "epoch": epoch,
                    "epoch_text": epoch.to_string(),
                    "results": results,
                    "retryable": false,
                    "error": { "code": "COMMIT_OUTCOME", "message": message }
                }),
            )))
        }
        Err(MongrelError::CommitOutcomeUnknown { epoch, message }) => {
            Err(IdempotentJsonFailure::OutcomeUnknown {
                epoch: Some(epoch),
                message,
            })
        }
        Err(error) => Err(idempotent_txn_failure(error)),
    }
}

// Helpers ---------------------------------------------------------------------

fn idempotent_txn_failure(error: MongrelError) -> IdempotentJsonFailure {
    match error {
        error
        @ (MongrelError::DurableCommit { .. } | MongrelError::CommitOutcomeUnknown { .. }) => {
            idempotent_core_failure(error, StatusCode::CONFLICT, "COMMIT_OUTCOME")
        }
        error => {
            let code = error_code(&error);
            let status = match crate::status_for_error(&error) {
                StatusCode::INTERNAL_SERVER_ERROR => StatusCode::CONFLICT,
                status => status,
            };
            IdempotentJsonFailure::safe(
                (
                    status,
                    Json(KitErrorEnvelope {
                        status: "aborted".into(),
                        error: KitError::new(code, error.to_string()),
                    }),
                )
                    .into_response(),
            )
        }
    }
}

fn idempotent_op_failure(index: usize, error: MongrelError) -> IdempotentJsonFailure {
    match error {
        error
        @ (MongrelError::DurableCommit { .. } | MongrelError::CommitOutcomeUnknown { .. }) => {
            idempotent_core_failure(error, StatusCode::CONFLICT, "COMMIT_OUTCOME")
        }
        error => IdempotentJsonFailure::safe(op_error(index, error)),
    }
}

fn op_error(i: usize, e: MongrelError) -> Response {
    let code = error_code(&e);
    (
        StatusCode::BAD_REQUEST,
        Json(KitErrorEnvelope {
            status: "aborted".into(),
            error: KitError::new(code, format!("{e}")).with_op(i),
        }),
    )
        .into_response()
}

fn op_error_msg(i: usize, code: &str, msg: String) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(KitErrorEnvelope {
            status: "aborted".into(),
            error: KitError::new(code, msg).with_op(i),
        }),
    )
        .into_response()
}

fn lookup_bound_schema(
    state: &AppState,
    table: &str,
    expected: &KitTxnTableBinding,
) -> std::result::Result<Schema, MongrelError> {
    let (binding, schema) = current_kit_table(state, table)?;
    if &binding != expected {
        return Err(MongrelError::Conflict(format!(
            "table {table:?} changed after request authorization"
        )));
    }
    Ok(schema)
}

/// Parse a flat `[col_id, val, …]` cell array against a schema.
fn parse_cells(row: &[Jval], schema: &Schema) -> std::result::Result<Vec<(u16, Value)>, String> {
    #[allow(clippy::manual_is_multiple_of)]
    if row.len() % 2 != 0 {
        return Err("cells must be an even-length [col_id, value, …] array".into());
    }
    let mut out = Vec::with_capacity(row.len() / 2);
    for chunk in row.chunks(2) {
        let raw_col_id = chunk[0]
            .as_u64()
            .ok_or("column id must be a non-negative integer")?;
        let col_id = u16::try_from(raw_col_id).map_err(|_| "column id must fit u16")?;
        let column = schema
            .columns
            .iter()
            .find(|c| c.id == col_id)
            .ok_or_else(|| format!("unknown column id {col_id}"))?;
        out.push((
            col_id,
            kit_value(&chunk[1], column, &schema.indexes)
                .map_err(|message| format!("column {col_id}: {message}"))?,
        ));
    }
    Ok(out)
}

fn kit_value(value: &Jval, column: &ColumnDef, indexes: &[IndexDef]) -> Result<Value, String> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    match &column.ty {
        TypeId::Embedding { dim } => {
            let values = value
                .as_array()
                .ok_or("embedding must be a numeric array")?;
            if values.len() != *dim as usize {
                return Err(format!(
                    "embedding dimension must be {dim}, got {}",
                    values.len()
                ));
            }
            Ok(values
                .iter()
                .map(|value| {
                    let value = value.as_f64().ok_or("embedding value must be numeric")?;
                    let value = value as f32;
                    value
                        .is_finite()
                        .then_some(value)
                        .ok_or("embedding value must be finite f32")
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Embedding)?)
        }
        TypeId::Bytes
            if indexes
                .iter()
                .any(|index| index.column_id == column.id && index.kind == IndexKind::Sparse) =>
        {
            let pairs = value
                .as_array()
                .ok_or("sparse vector must be an array of [token_id, weight]")?;
            let mut terms = std::collections::BTreeMap::<u32, f32>::new();
            for pair in pairs {
                let pair = pair
                    .as_array()
                    .filter(|pair| pair.len() == 2)
                    .ok_or("sparse term must be [token_id, weight]")?;
                let token = pair[0]
                    .as_u64()
                    .and_then(|token| u32::try_from(token).ok())
                    .ok_or("sparse token_id must fit u32")?;
                let weight = pair[1].as_f64().ok_or("sparse weight must be numeric")? as f32;
                if !weight.is_finite() {
                    return Err("sparse weight must be finite f32".into());
                }
                *terms.entry(token).or_default() += weight;
            }
            if terms.values().any(|weight| !weight.is_finite()) {
                return Err("summed sparse weight must be finite f32".into());
            }
            bincode::serialize(&terms.into_iter().collect::<Vec<_>>())
                .map(Value::Bytes)
                .map_err(|error| error.to_string())
        }
        TypeId::Bytes
            if indexes
                .iter()
                .any(|index| index.column_id == column.id && index.kind == IndexKind::MinHash) =>
        {
            let members = value.as_array().ok_or("set must be an array")?;
            if members
                .iter()
                .any(|member| !matches!(member, Jval::String(_) | Jval::Number(_) | Jval::Bool(_)))
            {
                return Err("set members must be strings, numbers, or booleans".into());
            }
            serde_json::to_vec(members)
                .map(Value::Bytes)
                .map_err(|error| error.to_string())
        }
        _ => Ok(json_to_value(value, &column.ty)),
    }
}

/// Coerce a PK JSON value against the table's declared primary-key column.
fn pk_value(pk: &Jval, schema: &Schema) -> std::result::Result<Value, String> {
    let pk_col = schema
        .primary_key()
        .ok_or("table has no primary_key column")?;
    Ok(json_to_value(pk, &pk_col.ty))
}

fn row_to_json(row: &mongreldb_core::txn::OwnedRow) -> Vec<Jval> {
    let mut out: Vec<Jval> = Vec::with_capacity(row.columns.len() * 2);
    for (id, v) in &row.columns {
        out.push(json!(id));
        out.push(value_to_json(v));
    }
    out
}

pub(crate) fn value_to_json(v: &Value) -> Jval {
    match v {
        Value::Int64(n) => json!(n),
        Value::Float64(f) => serde_json::Number::from_f64(*f)
            .map(Jval::Number)
            .unwrap_or(Jval::Null),
        Value::Bytes(b) => Jval::String(String::from_utf8_lossy(b).into_owned()),
        Value::Bool(b) => Jval::Bool(*b),
        Value::Null => Jval::Null,
        Value::Embedding(v) => {
            let arr: Vec<Jval> = v
                .iter()
                .map(|x| {
                    serde_json::Number::from_f64(*x as f64)
                        .map(Jval::Number)
                        .unwrap_or(Jval::Null)
                })
                .collect();
            Jval::Array(arr)
        }
        Value::GeneratedEmbedding(value) => {
            let arr: Vec<Jval> = value
                .vector
                .iter()
                .map(|x| {
                    serde_json::Number::from_f64(*x as f64)
                        .map(Jval::Number)
                        .unwrap_or(Jval::Null)
                })
                .collect();
            Jval::Array(arr)
        }
        Value::Decimal(d) => Jval::String(d.to_string()),
        Value::Uuid(_) | Value::Json(_) => Jval::Null,
        Value::Interval { .. } => Jval::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ann_candidate_distance_json_uses_tagged_kinds() {
        let hamming = ann_candidate_distance_json(AnnCandidateDistance::Hamming(7));
        assert_eq!(hamming["kind"], "hamming");
        assert_eq!(hamming["value"], 7);
        let cosine = ann_candidate_distance_json(AnnCandidateDistance::Cosine(0.125));
        assert_eq!(cosine["kind"], "cosine");
        assert!((cosine["value"].as_f64().unwrap() - 0.125).abs() < 1e-6);
        // Dense cosine must never be encoded under the hamming name.
        assert_ne!(cosine["kind"], "hamming");
    }

    #[test]
    fn retriever_score_json_keeps_binary_and_dense_distinct() {
        let hamming = retriever_score_json(RetrieverScore::AnnHammingDistance(2));
        assert_eq!(hamming["kind"], "ann_hamming_distance");
        assert_eq!(hamming["value"], 2);
        let cosine = retriever_score_json(RetrieverScore::AnnCosineDistance(0.5));
        assert_eq!(cosine["kind"], "ann_cosine_distance");
        assert!((cosine["value"].as_f64().unwrap() - 0.5).abs() < 1e-6);
    }

    #[test]
    fn trigger_error_code_uses_typed_variant_only() {
        assert_eq!(
            error_code(&MongrelError::TriggerValidation("invalid".into())),
            "TRIGGER_VALIDATION"
        );
        assert_eq!(
            error_code(&MongrelError::Conflict(
                "ordinary conflict mentioning trigger text".into()
            )),
            "CONFLICT"
        );
        assert_eq!(
            error_code(&MongrelError::InvalidArgument(
                "external trigger bridge is unrelated text".into()
            )),
            "BAD_REQUEST"
        );
    }

    #[tokio::test]
    async fn deadlock_and_serialization_failure_are_409_with_typed_codes() {
        for (error, code) in [
            (
                MongrelError::Deadlock {
                    victim: 7,
                    cycle: "7 → 3 → 7".into(),
                },
                "DEADLOCK",
            ),
            (
                MongrelError::SerializationFailure {
                    message: "ssi certification failed".into(),
                },
                "SERIALIZATION_FAILURE",
            ),
        ] {
            assert_eq!(error_code(&error), code);
            let (status, body) = response_json(kit_core_error(&error)).await;
            assert_eq!(status, StatusCode::CONFLICT, "{error}");
            assert_eq!(body["status"], "aborted", "{body}");
            assert_eq!(body["error"]["code"], code, "{body}");
        }
    }

    async fn response_json(response: Response) -> (StatusCode, Jval) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn idempotency_intent_survives_panic_and_fails_closed_after_restart() {
        let directory = tempfile::tempdir().unwrap();
        let store = IdempotencyStore::new(directory.path());
        let execution = match store
            .begin("owner", "key", "operation", br#"{"value":1}"#)
            .await
        {
            IdempotencyBegin::Execute(execution) => execution,
            _ => panic!("expected fresh execution"),
        };
        std::fs::write(directory.path().join("committed-side-effect"), b"once").unwrap();
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _execution = execution;
            panic!("process stopped after commit and before receipt publication");
        }))
        .is_err());

        let restarted = IdempotencyStore::new(directory.path());
        assert!(matches!(
            restarted
                .begin("owner", "key", "operation", br#"{"value":1}"#)
                .await,
            IdempotencyBegin::Indeterminate
        ));
        assert_eq!(
            std::fs::read(directory.path().join("committed-side-effect")).unwrap(),
            b"once"
        );
    }

    #[tokio::test]
    async fn idempotency_receipt_binding_and_checksum_survive_restart() {
        let directory = tempfile::tempdir().unwrap();
        let store = IdempotencyStore::new(directory.path());
        let execution = match store.begin("owner", "key", "operation", b"payload").await {
            IdempotencyBegin::Execute(execution) => execution,
            _ => panic!("expected fresh execution"),
        };
        assert!(execution.commit(IdempotencyResponse::new(
            StatusCode::CREATED,
            json!({ "status": "ok", "epoch": 7 }),
        )));

        let restarted = IdempotencyStore::new(directory.path());
        assert!(matches!(
            restarted.begin("owner", "key", "operation", b"payload").await,
            IdempotencyBegin::Replay(response)
                if response.status == StatusCode::CREATED.as_u16()
                    && response.body["epoch"] == 7
        ));
        assert!(matches!(
            restarted.begin("owner", "key", "other", b"payload").await,
            IdempotencyBegin::Mismatch
        ));
        assert!(matches!(
            restarted
                .begin("other", "key", "operation", b"payload")
                .await,
            IdempotencyBegin::Execute(_)
        ));

        let scope = crate::sql_idempotency::hex(&idempotency_scope_hash("owner", "key"));
        let receipt_path = restarted.receipt_path(&scope);
        let mut receipt: Jval =
            serde_json::from_slice(&std::fs::read(&receipt_path).unwrap()).unwrap();
        receipt["response"]["body"]["epoch"] = json!(8);
        std::fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
        let restarted = IdempotencyStore::new(directory.path());
        assert!(matches!(
            restarted
                .begin("owner", "key", "operation", b"payload")
                .await,
            IdempotencyBegin::Indeterminate
        ));
    }

    #[tokio::test]
    async fn forged_valid_json_intent_fails_closed() {
        let directory = tempfile::tempdir().unwrap();
        let store = IdempotencyStore::new(directory.path());
        let execution = match store.begin("owner", "key", "operation", b"payload").await {
            IdempotencyBegin::Execute(execution) => execution,
            _ => panic!("expected fresh execution"),
        };
        drop(execution);
        let scope = crate::sql_idempotency::hex(&idempotency_scope_hash("owner", "key"));
        let intent_path = store.intent_path(&scope);
        let mut forged: Jval =
            serde_json::from_slice(&std::fs::read(&intent_path).unwrap()).unwrap();
        forged["created_at_ms"] = json!(0);
        std::fs::write(&intent_path, serde_json::to_vec(&forged).unwrap()).unwrap();

        assert!(matches!(
            store.begin("owner", "key", "operation", b"payload").await,
            IdempotencyBegin::Indeterminate
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinked_entries_and_capacity_lock_fail_closed() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let store = IdempotencyStore::new(directory.path());
        let execution = match store
            .begin("owner", "receipt", "operation", b"payload")
            .await
        {
            IdempotencyBegin::Execute(execution) => execution,
            _ => panic!("expected fresh execution"),
        };
        assert!(execution.commit(IdempotencyResponse::new(
            StatusCode::OK,
            json!({ "status": "ok" }),
        )));
        let scope = crate::sql_idempotency::hex(&idempotency_scope_hash("owner", "receipt"));
        let receipt_path = store.receipt_path(&scope);
        let outside_receipt = outside.path().join("receipt.json");
        std::fs::rename(&receipt_path, &outside_receipt).unwrap();
        symlink(&outside_receipt, &receipt_path).unwrap();
        assert!(matches!(
            store
                .begin("owner", "receipt", "operation", b"payload")
                .await,
            IdempotencyBegin::Indeterminate
        ));

        let intent_root = tempfile::tempdir().unwrap();
        let intent_store = IdempotencyStore::new(intent_root.path());
        let execution = match intent_store
            .begin("owner", "intent", "operation", b"payload")
            .await
        {
            IdempotencyBegin::Execute(execution) => execution,
            _ => panic!("expected fresh execution"),
        };
        drop(execution);
        let scope = crate::sql_idempotency::hex(&idempotency_scope_hash("owner", "intent"));
        let intent_path = intent_store.intent_path(&scope);
        let outside_intent = outside.path().join("intent.json");
        std::fs::rename(&intent_path, &outside_intent).unwrap();
        symlink(&outside_intent, &intent_path).unwrap();
        assert!(matches!(
            intent_store
                .begin("owner", "intent", "operation", b"payload")
                .await,
            IdempotencyBegin::Indeterminate
        ));

        let lock_root = tempfile::tempdir().unwrap();
        let lock_outside = tempfile::tempdir().unwrap();
        let lock_store = IdempotencyStore::new(lock_root.path());
        let outside_lock = lock_outside.path().join("capacity.lock");
        std::fs::write(&outside_lock, b"outside").unwrap();
        symlink(
            &outside_lock,
            lock_store
                .files
                .absolute(crate::sql_idempotency::CAPACITY_LOCK_FILE),
        )
        .unwrap();
        assert!(matches!(
            lock_store
                .begin("owner", "key", "operation", b"payload")
                .await,
            IdempotencyBegin::Unavailable
        ));
    }

    #[tokio::test]
    async fn legacy_fixed_temp_cannot_block_or_replace_receipt() {
        let directory = tempfile::tempdir().unwrap();
        let store = IdempotencyStore::new(directory.path());
        let execution = match store.begin("owner", "key", "operation", b"payload").await {
            IdempotencyBegin::Execute(execution) => execution,
            _ => panic!("expected fresh execution"),
        };
        let scope = crate::sql_idempotency::hex(&idempotency_scope_hash("owner", "key"));
        let old_fixed_temp = store.files.absolute(format!("{scope}.receipt.json.tmp"));
        std::fs::write(&old_fixed_temp, b"attacker-controlled").unwrap();
        assert!(execution.commit(IdempotencyResponse::new(
            StatusCode::CREATED,
            json!({ "status": "ok", "epoch": 7 }),
        )));
        assert_eq!(
            std::fs::read(old_fixed_temp).unwrap(),
            b"attacker-controlled"
        );

        let restarted = IdempotencyStore::new(directory.path());
        assert!(matches!(
            restarted.begin("owner", "key", "operation", b"payload").await,
            IdempotencyBegin::Replay(response)
                if response.status == StatusCode::CREATED.as_u16()
                    && response.body["epoch"] == 7
        ));
    }

    #[tokio::test]
    async fn renamed_root_stays_descriptor_pinned() {
        let parent = tempfile::tempdir().unwrap();
        let original = parent.path().join("database");
        let moved = parent.path().join("moved-database");
        std::fs::create_dir(&original).unwrap();
        let store = IdempotencyStore::new(&original);
        std::fs::rename(&original, &moved).unwrap();
        std::fs::create_dir(&original).unwrap();

        assert!(matches!(
            store.begin("owner", "key", "operation", b"payload").await,
            IdempotencyBegin::Execute(_)
        ));
        let scope = crate::sql_idempotency::hex(&idempotency_scope_hash("owner", "key"));
        assert!(moved
            .join("_idem")
            .join(store.intent_name(&scope))
            .is_file());
        assert!(!original.join("_idem").exists());
    }

    #[tokio::test]
    async fn durable_commit_failure_is_published_and_replayed_exactly_after_restart() {
        const EXACT_EPOCH: u64 = 9_007_199_254_740_993;
        let directory = tempfile::tempdir().unwrap();
        let store = IdempotencyStore::new(directory.path());
        let execution = match store.begin("owner", "key", "operation", b"payload").await {
            IdempotencyBegin::Execute(execution) => execution,
            _ => panic!("expected fresh execution"),
        };
        let side_effect = directory.path().join("committed-side-effect");
        std::fs::write(&side_effect, b"once").unwrap();

        let response = finish_idempotent_execution(
            execution,
            Err(idempotent_core_failure(
                MongrelError::DurableCommit {
                    epoch: EXACT_EPOCH,
                    message: "post-commit publication failed".into(),
                },
                StatusCode::BAD_REQUEST,
                "IGNORED_PRECOMMIT_CODE",
            )),
        );
        let (status, body) = response_json(response).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["status"], "committed");
        assert_eq!(body["committed"], true);
        assert_eq!(body["epoch"], EXACT_EPOCH);
        assert_eq!(body["epoch_text"], EXACT_EPOCH.to_string());
        assert_eq!(body["error"]["code"], "COMMIT_OUTCOME");

        let restarted = IdempotencyStore::new(directory.path());
        let replay = match restarted
            .begin("owner", "key", "operation", b"payload")
            .await
        {
            IdempotencyBegin::Replay(response) => response.into_response(),
            _ => panic!("committed error must replay instead of execute"),
        };
        let (replay_status, replay_body) = response_json(replay).await;
        assert_eq!(replay_status, status);
        assert_eq!(replay_body, body);
        assert_eq!(std::fs::read(side_effect).unwrap(), b"once");
    }

    #[tokio::test]
    async fn unknown_commit_outcome_retains_intent_and_never_reexecutes_after_restart() {
        const EXACT_EPOCH: u64 = 9_007_199_254_740_993;
        let directory = tempfile::tempdir().unwrap();
        let store = IdempotencyStore::new(directory.path());
        let execution = match store.begin("owner", "key", "operation", b"payload").await {
            IdempotencyBegin::Execute(execution) => execution,
            _ => panic!("expected fresh execution"),
        };
        let side_effect = directory.path().join("possible-side-effect");
        std::fs::write(&side_effect, b"once").unwrap();

        let response = finish_idempotent_execution(
            execution,
            Err(idempotent_core_failure(
                MongrelError::CommitOutcomeUnknown {
                    epoch: EXACT_EPOCH,
                    message: "commit publication outcome is unknown".into(),
                },
                StatusCode::BAD_REQUEST,
                "IGNORED_PRECOMMIT_CODE",
            )),
        );
        let (status, body) = response_json(response).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["status"], "outcome_unknown");
        assert_eq!(body["committed"], Jval::Null);
        assert_eq!(body["epoch"], EXACT_EPOCH);
        assert_eq!(body["epoch_text"], EXACT_EPOCH.to_string());

        let restarted = IdempotencyStore::new(directory.path());
        assert!(matches!(
            restarted
                .begin("owner", "key", "operation", b"payload")
                .await,
            IdempotencyBegin::Indeterminate
        ));
        assert_eq!(std::fs::read(side_effect).unwrap(), b"once");
    }

    #[tokio::test]
    async fn idempotency_receipt_publish_failure_retains_intent() {
        let directory = tempfile::tempdir().unwrap();
        let store = IdempotencyStore::new(directory.path());
        let execution = match store.begin("owner", "key", "operation", b"payload").await {
            IdempotencyBegin::Execute(execution) => execution,
            _ => panic!("expected fresh execution"),
        };
        std::fs::create_dir(store.receipt_path(&execution.scope)).unwrap();
        assert!(!execution.commit(IdempotencyResponse::new(
            StatusCode::OK,
            json!({ "status": "ok" }),
        )));
        let restarted = IdempotencyStore::new(directory.path());
        assert!(matches!(
            restarted
                .begin("owner", "key", "operation", b"payload")
                .await,
            IdempotencyBegin::Indeterminate
        ));
    }

    #[tokio::test]
    async fn idempotency_capacity_is_bounded_and_intents_never_expire() {
        let directory = tempfile::tempdir().unwrap();
        let store = IdempotencyStore::new_with_limits(
            directory.path(),
            std::time::Duration::from_millis(1),
            2,
        );
        assert!(matches!(
            store.begin("owner", "key-1", "operation", b"one").await,
            IdempotencyBegin::Execute(_)
        ));
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        assert!(matches!(
            store.begin("owner", "key-2", "operation", b"two").await,
            IdempotencyBegin::Full
        ));
        assert!(matches!(
            store.begin("other", "key-2", "operation", b"two").await,
            IdempotencyBegin::Execute(_)
        ));
        assert!(matches!(
            store.begin("third", "key-3", "operation", b"three").await,
            IdempotencyBegin::Full
        ));
    }

    #[tokio::test]
    async fn expired_receipts_are_pruned_under_capacity_lock() {
        let directory = tempfile::tempdir().unwrap();
        let store = IdempotencyStore::new_with_limits(
            directory.path(),
            std::time::Duration::from_millis(1),
            1,
        );
        let execution = match store.begin("owner", "old", "operation", b"old").await {
            IdempotencyBegin::Execute(execution) => execution,
            _ => panic!("expected fresh execution"),
        };
        assert!(execution.commit(IdempotencyResponse::new(
            StatusCode::OK,
            json!({ "status": "ok" }),
        )));
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        assert!(matches!(
            store.begin("owner", "new", "operation", b"new").await,
            IdempotencyBegin::Execute(_)
        ));
    }

    #[tokio::test]
    async fn legacy_unbound_receipts_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let store = IdempotencyStore::new(directory.path());
        std::fs::write(
            store.files.absolute("0123456789abcdef.json"),
            br#"{"status":"ok"}"#,
        )
        .unwrap();
        assert!(matches!(
            store
                .begin("unrelated-owner", "new-key", "operation", b"payload")
                .await,
            IdempotencyBegin::Indeterminate
        ));
    }

    fn column(id: u16, ty: TypeId) -> ColumnDef {
        ColumnDef {
            id,
            name: format!("c{id}"),
            ty,
            flags: ColumnFlags::empty(),
            default_value: None,
            embedding_source: None,
        }
    }

    #[test]
    fn ai_wire_values_are_strict_and_canonical() {
        let embedding = column(1, TypeId::Embedding { dim: 2 });
        assert_eq!(
            kit_value(&json!([1, -1]), &embedding, &[]).unwrap(),
            Value::Embedding(vec![1.0, -1.0])
        );
        assert!(kit_value(&json!([1]), &embedding, &[])
            .unwrap_err()
            .contains("dimension"));

        let sparse = column(2, TypeId::Bytes);
        let sparse_index = IndexDef {
            name: "sparse".into(),
            column_id: 2,
            kind: IndexKind::Sparse,
            predicate: None,
            options: Default::default(),
        };
        let Value::Bytes(encoded) = kit_value(
            &json!([[2, 1.0], [1, 2.0], [2, 3.0]]),
            &sparse,
            &[sparse_index],
        )
        .unwrap() else {
            panic!("expected bytes")
        };
        assert_eq!(
            bincode::deserialize::<Vec<(u32, f32)>>(&encoded).unwrap(),
            vec![(1, 2.0), (2, 4.0)]
        );

        let set = column(3, TypeId::Bytes);
        let set_index = IndexDef {
            name: "set".into(),
            column_id: 3,
            kind: IndexKind::MinHash,
            predicate: None,
            options: Default::default(),
        };
        assert_eq!(
            kit_value(
                &json!(["a", 1, true]),
                &set,
                std::slice::from_ref(&set_index),
            )
            .unwrap(),
            Value::Bytes(br#"["a",1,true]"#.to_vec())
        );
        assert!(kit_value(&json!([{"bad": true}]), &set, &[set_index]).is_err());
    }

    #[test]
    fn cursor_v2_rejects_tampering_expiry_and_other_server_keys() {
        fn tamper_part(token: &str, index: usize, replacement: &str) -> String {
            let mut parts = token.split(':').collect::<Vec<_>>();
            parts[index] = replacement;
            parts.join(":")
        }

        let key = [7; 32];
        let stamp = mongreldb_core::AuthorizedReadStamp {
            table_id: 11,
            schema_id: 12,
            data_generation: 13,
            security_version: 14,
            snapshot: mongreldb_core::Snapshot::at(mongreldb_core::Epoch(15)),
        };
        let binding = new_cursor_binding(stamp, None, [16; 32], 17, 1);
        let token = format_kit_query_cursor(
            KitQueryCursor {
                epoch: 15,
                row_id: 18,
                binding,
            },
            &key,
        );
        assert_eq!(parse_kit_query_cursor(&token, &key).unwrap().row_id, 18);
        for tampered in [
            tamper_part(&token, 1, "19"),
            tamper_part(&token, 2, "20"),
            tamper_part(&token, 12, &cursor_hex(&[17; 32])),
        ] {
            assert!(matches!(
                parse_kit_query_cursor(&tampered, &key),
                Err(MongrelError::InvalidArgument(_))
            ));
        }

        let search_token = format_kit_search_cursor(
            KitSearchCursor {
                epoch: 15,
                final_score: 0.75,
                row_id: 18,
                binding,
            },
            &key,
        );
        for tampered in [
            tamper_part(&search_token, 1, "19"),
            tamper_part(&search_token, 2, "3fe0000000000000"),
            tamper_part(&search_token, 3, "20"),
            tamper_part(&search_token, 13, &cursor_hex(&[17; 32])),
        ] {
            assert!(matches!(
                parse_kit_search_cursor(&tampered, &key),
                Err(MongrelError::InvalidArgument(_))
            ));
        }
        assert!(matches!(
            parse_kit_query_cursor(
                &format_kit_query_cursor(
                    KitQueryCursor {
                        epoch: 15,
                        row_id: 18,
                        binding,
                    },
                    &key,
                ),
                &[8; 32]
            ),
            Err(MongrelError::InvalidArgument(_))
        ));

        let now = cursor_now_nanos();
        let expired = KitCursorBinding {
            issued_at_nanos: now - CURSOR_TTL_NANOS,
            expires_at_nanos: now - 1,
            ..binding
        };
        let expired = format_kit_query_cursor(
            KitQueryCursor {
                epoch: 15,
                row_id: 18,
                binding: expired,
            },
            &key,
        );
        assert!(matches!(
            parse_kit_query_cursor(&expired, &key),
            Err(MongrelError::CursorExpired)
        ));
        let expired_search = format_kit_search_cursor(
            KitSearchCursor {
                epoch: 15,
                final_score: 0.75,
                row_id: 18,
                binding: KitCursorBinding {
                    issued_at_nanos: now - CURSOR_TTL_NANOS,
                    expires_at_nanos: now - 1,
                    ..binding
                },
            },
            &key,
        );
        assert!(matches!(
            parse_kit_search_cursor(&expired_search, &key),
            Err(MongrelError::CursorExpired)
        ));
        assert!(matches!(
            parse_kit_query_cursor("q1:old", &key),
            Err(MongrelError::CursorStale(_))
        ));
        assert!(matches!(
            parse_kit_search_cursor("s1:old", &key),
            Err(MongrelError::CursorStale(_))
        ));
    }
}
