//! Server-side session store enabling cross-request interactive transactions
//! over the daemon (PLAN.md Phase 6 #10), aligned with the canonical session
//! model of `mongreldb-protocol` (spec section 10.4, S1D-004).
//!
//! Each session holds a long-lived [`MongrelSession`] whose `sql_txn` staging
//! survives across `run()` calls. Because `BEGIN`/`COMMIT` only stage ops
//! logically (the core `Transaction` is opened at `COMMIT`, not `BEGIN`), an
//! idle session with an open transaction does **not** pin an MVCC epoch — so
//! abandoned sessions cost only the staged-ops memory until the idle reaper
//! evicts them.
//!
//! ## Canonical session record (S1D-004)
//!
//! Every entry also carries the protocol crate's [`Session`] record: principal
//! identity, current database, transaction state, prepared statements,
//! session settings, read-your-writes token, and last activity. The record is
//! data only — sessions stay lightweight and own no storage; the
//! [`MongrelSession`] remains the execution handle the record keys off. The
//! record is updated best-effort by request handlers
//! ([`SessionEntry::sync_record_after_request`]) and read back for
//! diagnostics/tests via [`SessionEntry::protocol_record`].
//!
//! ## Safety rails
//! - **Auth-bound ownership**: a session is owned by the principal that created
//!   it; lookups by a different principal return `None` (treated as 404 to
//!   avoid confirming a session's existence to a non-owner).
//! - **Per-session serialization**: a `tokio::sync::Mutex` guards each session
//!   so two concurrent requests on the same token cannot interleave a
//!   `BEGIN`/`INSERT`/`COMMIT` sequence.
//! - **Bounded capacity**: `max_sessions` rejects new sessions with 503 once
//!   full.
//! - **Idle reaper**: [`SessionStore::sweep_idle`] drops sessions whose
//!   `last_used` exceeds the configured timeout, discarding any staged
//!   transaction (effective rollback).

use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mongreldb_protocol::prepared::{PreparedStatementBinding, StatementId};
use mongreldb_protocol::request::{AuthenticatedIdentity, IsolationLevel, SessionId};
use mongreldb_protocol::session::{Session, TransactionState};
use mongreldb_query::MongrelSession;
use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::{DatabaseId, TransactionId};

/// One pooled session plus its ownership and liveness metadata.
pub struct SessionEntry {
    /// The live query session — reused across requests via `X-Session-ID`.
    /// Behind a lock so the S1D-005 replan path can swap in a freshly opened
    /// session (current catalog registrations) while a request holds the
    /// per-session lock.
    session: std::sync::RwLock<Arc<MongrelSession>>,
    /// Principal that created the session (`username`, `"token"`, or
    /// `"anonymous"`). Requests from any other principal are rejected.
    pub owner: String,
    last_used: std::sync::Mutex<Instant>,
    /// Held for the duration of a request to serialize per-session access.
    pub lock: tokio::sync::Mutex<()>,
    /// Set when the session is closed/evicted. `get()` rejects closed entries,
    /// and request handlers re-check it after acquiring the lock so an in-flight
    /// request that obtained an `Arc` just before eviction aborts rather than
    /// committing into a closed session.
    closed: AtomicBool,
    /// The canonical S1D-004 session record: protocol-facing identity,
    /// transaction state, prepared-statement bindings, settings,
    /// read-your-writes token, and last-activity timestamp. Data only — the
    /// execution handle above remains the owner of execution state.
    record: std::sync::Mutex<Session>,
    /// Server-side prepared-statement name → protocol statement id. The plan
    /// itself lives in the `MongrelSession`; the binding record it is
    /// validated against lives in `record.prepared_statements` (S1D-005).
    prepared_names: std::sync::Mutex<BTreeMap<String, StatementId>>,
}

impl SessionEntry {
    /// The live query session handle, cloned per use. Callers that must
    /// observe a replan-driven session swap re-read it through this accessor.
    pub fn session(&self) -> Arc<MongrelSession> {
        self.session
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    /// Swap in a freshly opened query session (S1D-005): the old session's
    /// DataFusion registrations snapshot the catalog at open, so replanning a
    /// prepared statement after a cross-session catalog change requires a
    /// session built against the current catalog. Callers hold the
    /// per-session lock, so no request is mid-flight on the old session, and
    /// must only swap when no transaction is staged on the old one.
    pub(crate) fn replace_session(&self, session: MongrelSession) {
        *self
            .session
            .write()
            .unwrap_or_else(|error| error.into_inner()) = Arc::new(session);
    }

    pub(crate) fn touch(&self) {
        if let Ok(mut t) = self.last_used.lock() {
            *t = Instant::now();
        }
        if let Ok(mut record) = self.record.lock() {
            record.last_activity_unix_micros = now_unix_micros();
        }
    }

    /// Whether this entry has been closed/evicted and must reject new work.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn mark_closed(&self) {
        self.closed.store(true, Ordering::Release);
    }

    fn idle_for_at_least(&self, timeout: Duration) -> bool {
        self.last_used
            .lock()
            .map(|t| t.elapsed() >= timeout)
            .unwrap_or(false)
    }

    /// A consistent copy of the canonical S1D-004 session record
    /// (diagnostics and tests).
    pub fn protocol_record(&self) -> Session {
        self.record
            .lock()
            .map(|record| record.clone())
            .unwrap_or_else(|error| error.into_inner().clone())
    }

    /// Refresh the canonical record after a request completed on this
    /// session: last activity, transaction state (derived from the session's
    /// staged-ops staging), and the read-your-writes token when the request
    /// durably committed. Callers hold the per-session lock, so the record
    /// cannot race another request on this session.
    ///
    /// `commit` is `Some` exactly when the request committed. The token is
    /// the literal HLC commit-timestamp lineage of the write when one is
    /// known: the core commit log's `CommitReceipt.commit_ts` recorded for an
    /// idempotent commit (the S1B-005 idempotency path), else the exact
    /// commit timestamp the query layer sourced into the durable outcome,
    /// else the per-open epoch→commit-ts ledger
    /// (`Database::commit_ts_for_epoch`). When none of those resolve it is
    /// the node's HLC timestamp captured at a fresh `begin` after the commit
    /// became visible — the single HLC authority orders it after the write's
    /// commit timestamp (spec §8.2), so any later read at this token observes
    /// the session's own write.
    pub(crate) fn sync_record_after_request(&self, commit: Option<HlcTimestamp>) {
        let Ok(mut record) = self.record.lock() else {
            return;
        };
        record.last_activity_unix_micros = now_unix_micros();
        let staging = self.session().staged_sql_operation_count().is_some();
        match (&record.transaction_state, staging) {
            (TransactionState::Idle, true) => {
                record.transaction_state = TransactionState::Active {
                    transaction_id: TransactionId::new_random(),
                    isolation: IsolationLevel::Snapshot,
                };
            }
            (TransactionState::Active { .. }, false) => {
                record.transaction_state = TransactionState::Idle;
            }
            _ => {}
        }
        if let Some(commit_ts) = commit {
            record.read_your_writes_token = Some(commit_ts);
        }
    }

    /// Allocate the next session-scoped prepared-statement id. Callers hold
    /// the per-session lock, so id allocation cannot race.
    pub(crate) fn allocate_statement_id(&self) -> StatementId {
        let record = self
            .record
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let next = record
            .prepared_statements
            .keys()
            .next_back()
            .map_or(1, |id| id.get().saturating_add(1));
        StatementId::new(next)
    }

    /// The binding recorded for a server-side statement name, if the name was
    /// prepared through the registry-tracked prepare endpoint.
    pub(crate) fn prepared_binding(
        &self,
        name: &str,
    ) -> Option<(StatementId, PreparedStatementBinding)> {
        let names = self
            .prepared_names
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let statement_id = names.get(name).copied()?;
        let record = self
            .record
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        record
            .prepared_statements
            .get(&statement_id)
            .cloned()
            .map(|binding| (statement_id, binding))
    }

    /// Record (or replace) a prepared-statement binding under a server-side
    /// statement name (S1D-005).
    pub(crate) fn insert_prepared_binding(&self, name: String, binding: PreparedStatementBinding) {
        self.prepared_names
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(name, binding.statement_id);
        self.record
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .prepared_statements
            .insert(binding.statement_id, binding);
    }

    /// Drop a prepared-statement binding by server-side name (DEALLOCATE).
    pub(crate) fn remove_prepared_binding(&self, name: &str) -> Option<StatementId> {
        let statement_id = self
            .prepared_names
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(name)?;
        self.record
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .prepared_statements
            .remove(&statement_id);
        Some(statement_id)
    }

    /// Number of registry-tracked prepared statements on this session.
    #[cfg(test)]
    pub(crate) fn prepared_statement_count(&self) -> usize {
        self.record
            .lock()
            .map(|record| record.prepared_statements.len())
            .unwrap_or(0)
    }
}

/// Wall-clock microseconds since the Unix epoch — the same time base as the
/// canonical model's `deadline_unix_micros` / `last_activity_unix_micros`.
pub(crate) fn now_unix_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(u128::from(u64::MAX)) as u64
}

/// Token-keyed pool of live sessions. Threaded through `AppState` as
/// `Arc<SessionStore>` so the idle reaper (a detached thread) shares the same
/// map as request handlers.
pub struct SessionStore {
    sessions: std::sync::Mutex<HashMap<String, Arc<SessionEntry>>>,
    max_sessions: usize,
    idle_timeout: Duration,
    /// Logical database the sessions of this store resolve against
    /// (S1D-004). Process-local: sessions are in-memory and die with the
    /// process, so the id is drawn at store construction. Catalog-allocated
    /// cluster-wide database ids land with the distributed waves.
    database_id: DatabaseId,
}

impl SessionStore {
    /// New store allowing up to `max_sessions` concurrent sessions, each evicted
    /// after `idle_timeout` of inactivity.
    pub fn new(max_sessions: usize, idle_timeout: Duration) -> Self {
        Self::new_with_database_id(max_sessions, idle_timeout, DatabaseId::new_random())
    }

    /// [`Self::new`] with an explicit logical database id stamped onto every
    /// session record.
    pub fn new_with_database_id(
        max_sessions: usize,
        idle_timeout: Duration,
        database_id: DatabaseId,
    ) -> Self {
        Self {
            sessions: std::sync::Mutex::new(HashMap::new()),
            // Always allow at least one session so the feature is usable when
            // a caller passes a zero/negative-feeling cap.
            max_sessions: max_sessions.max(1),
            idle_timeout,
            database_id,
        }
    }

    /// The logical database id stamped onto this store's session records.
    pub fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Register a new session under a fresh opaque token. Returns the token, or
    /// `None` if the store is at capacity (caller maps this to HTTP 503).
    ///
    /// The session record carries [`AuthenticatedIdentity::Credentialless`];
    /// authenticated daemons use [`Self::create_with_identity`].
    pub fn create(&self, session: MongrelSession, owner: String) -> Option<String> {
        self.create_with_identity(session, owner, AuthenticatedIdentity::Credentialless)
    }

    /// [`Self::create`] with the authenticated identity the session acts as
    /// (S1D-004): fixed at session open and carried by the canonical record.
    pub fn create_with_identity(
        &self,
        session: MongrelSession,
        owner: String,
        principal: AuthenticatedIdentity,
    ) -> Option<String> {
        let mut guard = self.sessions.lock().ok()?;
        if guard.len() >= self.max_sessions {
            return None;
        }
        let token = random_token()?;
        // `random_token` emits exactly 32 lowercase hex digits, the canonical
        // `SessionId` text form, so this parse cannot fail.
        let session_id = SessionId::from_str(&token).unwrap_or(SessionId::ZERO);
        let record = Session::new(session_id, principal, self.database_id, now_unix_micros());
        guard.insert(
            token.clone(),
            Arc::new(SessionEntry {
                session: std::sync::RwLock::new(Arc::new(session)),
                owner,
                last_used: std::sync::Mutex::new(Instant::now()),
                lock: tokio::sync::Mutex::new(()),
                closed: AtomicBool::new(false),
                record: std::sync::Mutex::new(record),
                prepared_names: std::sync::Mutex::new(BTreeMap::new()),
            }),
        );
        Some(token)
    }

    /// Look up a session by token, verifying the caller owns it and the session
    /// is not closed. Returns a cloned `Arc<SessionEntry>` (the store lock is
    /// released immediately; the caller holds the entry's per-session lock for
    /// the request duration, and must re-check `is_closed()` after locking).
    /// Returns `None` for an unknown token, an ownership mismatch, or a closed
    /// session.
    pub fn get(&self, token: &str, owner: &str) -> Option<Arc<SessionEntry>> {
        let guard = self.sessions.lock().ok()?;
        let entry = guard.get(token)?;
        if entry.owner != owner || entry.is_closed() {
            return None;
        }
        Some(Arc::clone(entry))
    }

    /// Remove and drop a session, marking it closed first so any request that
    /// already holds an `Arc` aborts after acquiring the lock. Returns whether a
    /// session was removed. Rejects non-owners.
    pub fn close(&self, token: &str, owner: &str) -> bool {
        self.take_for_close(token, owner).is_some()
    }

    /// Mark closing and remove from new lookups while returning the live entry
    /// so the caller can cancel its queries and wait a bounded grace period.
    pub(crate) fn take_for_close(&self, token: &str, owner: &str) -> Option<Arc<SessionEntry>> {
        if let Ok(mut guard) = self.sessions.lock() {
            if let Some(entry) = guard.get(token) {
                if entry.owner != owner {
                    return None;
                }
                entry.mark_closed();
            }
            return guard.remove(token);
        }
        None
    }

    /// Evict every session idle for longer than the configured timeout, skipping
    /// any session with an in-flight request (per-session lock held). Called
    /// periodically by [`spawn_session_reaper`]. Returns the count evicted.
    pub fn sweep_idle(&self) -> usize {
        let Ok(mut guard) = self.sessions.lock() else {
            return 0;
        };
        let timeout = self.idle_timeout;
        // Collect tokens to evict: idle AND not currently in-flight (its
        // per-session lock is acquirable). Holding the store lock prevents new
        // lookups while we decide; try_lock fails exactly when a request holds
        // or is acquiring the session lock.
        let to_evict: Vec<String> = guard
            .iter()
            .filter_map(|(token, entry)| {
                if entry.idle_for_at_least(timeout)
                    && entry.session().query_registry().active_for_session(token) == 0
                    && entry.lock.try_lock().is_ok()
                {
                    Some(token.clone())
                } else {
                    None
                }
            })
            .collect();
        let count = to_evict.len();
        for token in &to_evict {
            if let Some(entry) = guard.remove(token) {
                entry.mark_closed();
            }
        }
        count
    }

    /// Current number of live sessions (diagnostics / tests).
    pub fn len(&self) -> usize {
        self.sessions.lock().map(|g| g.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Mark every session closed and remove it from the pool during shutdown.
    pub(crate) fn close_all(&self) {
        if let Ok(mut guard) = self.sessions.lock() {
            for entry in guard.values() {
                entry.mark_closed();
            }
            guard.clear();
        }
    }
}

/// Background idle-session reaper. Sweeps every 30 s, evicting sessions whose
/// `last_used` exceeds the configured timeout. Errors are logged and never
/// abort the sweep. Mirrors `spawn_auto_compactor`'s pattern.
pub fn spawn_session_reaper(store: Arc<SessionStore>) {
    std::thread::Builder::new()
        .name("mongreldb-session-reaper".into())
        .spawn(move || loop {
            std::thread::sleep(Duration::from_secs(30));
            let evicted = store.sweep_idle();
            if evicted > 0 {
                eprintln!("[session-reaper] evicted {evicted} idle session(s)");
            }
        })
        .expect("spawn session-reaper");
}

/// Generate an opaque, cryptographically-random session token. Reads 16 bytes
/// from `/dev/urandom` (Linux/macOS) and hex-encodes them. Session tokens are
/// bearer capabilities, so there is NO predictable fallback: if the OS RNG is
/// unavailable, this returns `None` and the caller rejects session creation
/// (HTTP 503) rather than handing out a guessable token.
fn random_token() -> Option<String> {
    let bytes = read_urandom(16)?;
    let mut n = 0u128;
    for &b in &bytes {
        n = (n << 8) | b as u128;
    }
    Some(format!("{n:032x}"))
}

fn read_urandom(n: usize) -> Option<Vec<u8>> {
    use std::io::Read;
    let mut f = std::fs::File::open("/dev/urandom").ok()?;
    let mut buf = vec![0u8; n];
    f.read_exact(&mut buf).ok()?;
    Some(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongreldb_core::Database;
    use mongreldb_query::{RegisteredQueryGuard, SqlQueryOptions};
    use tempfile::tempdir;

    fn make_session() -> MongrelSession {
        let dir = tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path()).unwrap());
        // Keep the TempDir alive for the session's lifetime by leaking it; tests
        // are short-lived and the OS reclaims on exit.
        std::mem::forget(dir);
        MongrelSession::open(db).unwrap()
    }

    #[test]
    fn create_and_get_roundtrip() {
        let store = SessionStore::new(8, Duration::from_secs(60));
        let token = store.create(make_session(), "alice".into()).unwrap();
        assert!(store.get(&token, "alice").is_some());
        // Wrong owner → None (ownership enforced).
        assert!(store.get(&token, "eve").is_none());
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn close_removes_session() {
        let store = SessionStore::new(8, Duration::from_secs(60));
        let token = store.create(make_session(), "alice".into()).unwrap();
        assert!(store.close(&token, "alice"));
        assert!(store.get(&token, "alice").is_none());
        assert!(store.is_empty());
        // Non-owner cannot close.
        let t2 = store.create(make_session(), "bob".into()).unwrap();
        assert!(!store.close(&t2, "alice"));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn capacity_limit_rejects_new_sessions() {
        let store = SessionStore::new(1, Duration::from_secs(60));
        assert!(store.create(make_session(), "a".into()).is_some());
        // At capacity → None.
        assert!(store.create(make_session(), "b".into()).is_none());
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn sweep_idle_evicts_stale_sessions() {
        let store = SessionStore::new(8, Duration::from_millis(1));
        let token = store.create(make_session(), "alice".into()).unwrap();
        assert_eq!(store.len(), 1);
        // Sleep past the idle timeout.
        std::thread::sleep(Duration::from_millis(20));
        let evicted = store.sweep_idle();
        assert_eq!(evicted, 1);
        assert!(store.get(&token, "alice").is_none());
        assert!(store.is_empty());
    }

    #[test]
    fn sweep_idle_keeps_active_queries() {
        let store = SessionStore::new(8, Duration::from_millis(1));
        let token = store.create(make_session(), "alice".into()).unwrap();
        let entry = store.get(&token, "alice").unwrap();
        let query = entry
            .session()
            .register_query(SqlQueryOptions {
                session_id: Some(token.clone()),
                ..SqlQueryOptions::default()
            })
            .unwrap();
        let query = RegisteredQueryGuard::new(query);
        std::thread::sleep(Duration::from_millis(20));

        assert_eq!(store.sweep_idle(), 0);
        assert!(store.get(&token, "alice").is_some());

        drop(query);
        assert_eq!(store.sweep_idle(), 1);
        assert!(store.is_empty());
    }

    #[test]
    fn protocol_record_carries_identity_database_and_session_id() {
        let database_id = DatabaseId::new_random();
        let store = SessionStore::new_with_database_id(8, Duration::from_secs(60), database_id);
        let identity = AuthenticatedIdentity::CatalogUser {
            username: "alice".to_owned(),
            user_id: 42,
            created_version: 7,
        };
        let token = store
            .create_with_identity(make_session(), "alice".into(), identity.clone())
            .unwrap();
        let entry = store.get(&token, "alice").unwrap();
        let record = entry.protocol_record();
        assert_eq!(record.session_id, SessionId::from_str(&token).unwrap());
        assert_eq!(record.principal, identity);
        assert_eq!(record.current_database, database_id);
        assert_eq!(record.transaction_state, TransactionState::Idle);
        assert!(record.prepared_statements.is_empty());
        assert!(record.settings.is_empty());
        assert_eq!(record.read_your_writes_token, None);
        assert!(record.last_activity_unix_micros > 0);
        assert_eq!(store.database_id(), database_id);
    }

    #[test]
    fn sync_record_tracks_commit_and_activity() {
        let store = SessionStore::new(8, Duration::from_secs(60));
        let token = store.create(make_session(), "alice".into()).unwrap();
        let entry = store.get(&token, "alice").unwrap();
        let before = entry.protocol_record().last_activity_unix_micros;
        std::thread::sleep(Duration::from_millis(2));

        let commit_ts = HlcTimestamp {
            physical_micros: now_unix_micros().saturating_sub(1_000),
            logical: 3,
            node_tiebreaker: 0,
        };
        entry.sync_record_after_request(Some(commit_ts));
        let record = entry.protocol_record();
        assert!(record.last_activity_unix_micros >= before);
        assert_eq!(
            record.read_your_writes_token,
            Some(commit_ts),
            "a committed request must advance the read-your-writes token to the commit timestamp"
        );

        // Without a staged transaction the state stays Idle.
        assert_eq!(record.transaction_state, TransactionState::Idle);

        // A non-committed request never moves the token.
        entry.sync_record_after_request(None);
        assert_eq!(
            entry.protocol_record().read_your_writes_token,
            Some(commit_ts)
        );
    }

    #[test]
    fn prepared_bindings_are_tracked_by_name_and_id() {
        let store = SessionStore::new(8, Duration::from_secs(60));
        let token = store.create(make_session(), "alice".into()).unwrap();
        let entry = store.get(&token, "alice").unwrap();

        let first = entry.allocate_statement_id();
        assert_eq!(first, StatementId::new(1));
        let mut binding = PreparedStatementBinding {
            statement_id: first,
            sql: "SELECT 1".to_owned(),
            parameter_types: vec![],
            catalog_version: mongreldb_types::ids::MetadataVersion::new(3),
            schema_versions: BTreeMap::new(),
            feature_set: Default::default(),
        };
        entry.insert_prepared_binding("stmt_a".to_owned(), binding.clone());
        assert_eq!(entry.prepared_statement_count(), 1);
        assert_eq!(
            entry.prepared_binding("stmt_a"),
            Some((first, binding.clone()))
        );

        // Ids allocate monotonically from the greatest live id.
        let second = entry.allocate_statement_id();
        assert_eq!(second, StatementId::new(2));
        binding.statement_id = second;
        entry.insert_prepared_binding("stmt_b".to_owned(), binding);

        assert_eq!(entry.remove_prepared_binding("stmt_a"), Some(first));
        assert_eq!(entry.prepared_binding("stmt_a"), None);
        assert_eq!(entry.prepared_statement_count(), 1);
        assert_eq!(entry.protocol_record().prepared_statements.len(), 1);
        // Removing an unknown name is a no-op.
        assert_eq!(entry.remove_prepared_binding("stmt_a"), None);
    }
}
