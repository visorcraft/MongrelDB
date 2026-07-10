//! Server-side session store enabling cross-request interactive transactions
//! over the daemon (PLAN.md Phase 6 #10).
//!
//! Each session holds a long-lived [`MongrelSession`] whose `sql_txn` staging
//! survives across `run()` calls. Because `BEGIN`/`COMMIT` only stage ops
//! logically (the core `Transaction` is opened at `COMMIT`, not `BEGIN`), an
//! idle session with an open transaction does **not** pin an MVCC epoch — so
//! abandoned sessions cost only the staged-ops memory until the idle reaper
//! evicts them.
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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mongreldb_query::MongrelSession;

/// One pooled session plus its ownership and liveness metadata.
pub struct SessionEntry {
    /// The live query session — reused across requests via `X-Session-ID`.
    pub session: Arc<MongrelSession>,
    /// Principal that created the session (`username`, `"token"`, or
    /// `"anonymous"`). Requests from any other principal are rejected.
    pub owner: String,
    last_used: std::sync::Mutex<Instant>,
    /// Held for the duration of a request to serialize per-session access.
    pub lock: tokio::sync::Mutex<()>,
}

impl SessionEntry {
    pub(crate) fn touch(&self) {
        if let Ok(mut t) = self.last_used.lock() {
            *t = Instant::now();
        }
    }

    fn idle_for_at_least(&self, timeout: Duration) -> bool {
        self.last_used
            .lock()
            .map(|t| t.elapsed() >= timeout)
            .unwrap_or(false)
    }
}

/// Token-keyed pool of live sessions. Threaded through `AppState` as
/// `Arc<SessionStore>` so the idle reaper (a detached thread) shares the same
/// map as request handlers.
pub struct SessionStore {
    sessions: std::sync::Mutex<HashMap<String, Arc<SessionEntry>>>,
    max_sessions: usize,
    idle_timeout: Duration,
}

impl SessionStore {
    /// New store allowing up to `max_sessions` concurrent sessions, each evicted
    /// after `idle_timeout` of inactivity.
    pub fn new(max_sessions: usize, idle_timeout: Duration) -> Self {
        Self {
            sessions: std::sync::Mutex::new(HashMap::new()),
            // Always allow at least one session so the feature is usable when
            // a caller passes a zero/negative-feeling cap.
            max_sessions: max_sessions.max(1),
            idle_timeout,
        }
    }

    /// Register a new session under a fresh opaque token. Returns the token, or
    /// `None` if the store is at capacity (caller maps this to HTTP 503).
    pub fn create(&self, session: MongrelSession, owner: String) -> Option<String> {
        let mut guard = self.sessions.lock().ok()?;
        if guard.len() >= self.max_sessions {
            return None;
        }
        let token = random_token()?;
        guard.insert(
            token.clone(),
            Arc::new(SessionEntry {
                session: Arc::new(session),
                owner,
                last_used: std::sync::Mutex::new(Instant::now()),
                lock: tokio::sync::Mutex::new(()),
            }),
        );
        Some(token)
    }

    /// Look up a session by token, verifying the caller owns it. Returns a
    /// cloned `Arc<SessionEntry>` (the store lock is released immediately; the
    /// caller holds the entry's per-session lock for the request duration).
    /// Returns `None` for an unknown token OR an ownership mismatch.
    pub fn get(&self, token: &str, owner: &str) -> Option<Arc<SessionEntry>> {
        let guard = self.sessions.lock().ok()?;
        let entry = guard.get(token)?;
        if entry.owner != owner {
            return None;
        }
        Some(Arc::clone(entry))
    }

    /// Remove and drop a session (any staged transaction is discarded). Returns
    /// whether a session was removed. Rejects non-owners.
    pub fn close(&self, token: &str, owner: &str) -> bool {
        if let Ok(mut guard) = self.sessions.lock() {
            if let Some(entry) = guard.get(token) {
                if entry.owner != owner {
                    return false;
                }
            }
            return guard.remove(token).is_some();
        }
        false
    }

    /// Evict every session idle for longer than the configured timeout. Called
    /// periodically by [`spawn_session_reaper`]. Returns the count evicted.
    pub fn sweep_idle(&self) -> usize {
        let Ok(mut guard) = self.sessions.lock() else {
            return 0;
        };
        let timeout = self.idle_timeout;
        let before = guard.len();
        guard.retain(|_, entry| !entry.idle_for_at_least(timeout));
        before - guard.len()
    }

    /// Current number of live sessions (diagnostics / tests).
    pub fn len(&self) -> usize {
        self.sessions.lock().map(|g| g.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

/// Generate an opaque, hard-to-guess session token. Reads 16 bytes from
/// `/dev/urandom` (Linux/macOS) and hex-encodes them; falls back to a
/// time+counter-derived value only if the OS source is unavailable.
fn random_token() -> Option<String> {
    if let Some(bytes) = read_urandom(16) {
        let mut n = 0u128;
        for &b in &bytes {
            n = (n << 8) | b as u128;
        }
        return Some(format!("{n:032x}"));
    }
    // Fallback: less secure but still unique within a process lifetime.
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let c = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mixed = nanos ^ (c as u128);
    Some(format!("{mixed:032x}"))
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
}
