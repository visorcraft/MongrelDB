//! Security audit logging — records actor-aware events (authentication,
//! DDL/privilege changes) for operational and compliance visibility.
//!
//! This is the v1 surface called out in PLAN.md Phase 6 (#19): start with auth
//! and authorization and DDL events. The store is an **in-memory ring buffer**
//! mirrored to stderr. It is deliberately NOT claimed as tamper-evident: a
//! mutable buffer or the CRC-protected WAL cannot be called tamper-proof
//! without a cryptographic chain, a deletion/retention policy, and an external
//! sink (see PLAN.md #19 key-risk). Durable/attested audit is a follow-up.

use std::collections::VecDeque;
use std::sync::Mutex;

/// One auditable event.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AuditEvent {
    /// UNIX epoch nanoseconds at which the event was recorded.
    pub ts_unix_nanos: u128,
    /// The actor the event is attributed to (`"<username>"`, `"token"`, or
    /// `"anonymous"` when unauthenticated/no principal was resolved).
    pub principal: String,
    /// Coarse event class: `"login.ok"`, `"login.fail"`, `"ddl"`,
    /// `"grant"`, `"revoke"`, ...
    pub action: String,
    /// Free-form detail (a DDL snippet, the remote credential, ...). Keep short.
    pub detail: String,
}

/// Bounded in-memory audit log. Events are also echoed to stderr so an external
/// log collector (journald, Docker logs, a sidecar) captures them durably
/// regardless of the ring buffer's eviction.
pub struct AuditLog {
    buf: Mutex<VecDeque<AuditEvent>>,
    cap: usize,
}

impl AuditLog {
    /// New ring buffer retaining the last `cap` events.
    pub fn new(cap: usize) -> Self {
        Self {
            buf: Mutex::new(VecDeque::with_capacity(cap.min(4096))),
            cap: cap.max(1),
        }
    }

    /// Record an event. Evicts the oldest entry once the ring is full so the
    /// buffer's memory is bounded.
    pub fn record(
        &self,
        principal: impl Into<String>,
        action: impl Into<String>,
        detail: impl Into<String>,
    ) {
        let event = AuditEvent {
            ts_unix_nanos: now_unix_nanos(),
            principal: principal.into(),
            action: action.into(),
            detail: detail.into(),
        };
        // Echo to stderr for external collection (best-effort; ignore errors).
        eprintln!(
            "[audit] {} {} \u{2014} {}",
            event.principal, event.action, event.detail
        );
        let mut guard = self.buf.lock().expect("audit log not poisoned");
        if guard.len() >= self.cap {
            guard.pop_front();
        }
        guard.push_back(event);
    }

    /// Snapshot of the retained events, oldest-first.
    pub fn recent(&self) -> Vec<AuditEvent> {
        self.buf
            .lock()
            .expect("audit log not poisoned")
            .iter()
            .cloned()
            .collect()
    }
}

fn now_unix_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// Whether a SQL string is a DDL or privilege statement that should be audited.
/// Conservative keyword prefix check (case-insensitive); false positives only
/// mean an extra audit line, never a missed one.
pub fn is_audited_sql(sql: &str) -> bool {
    let s = sql.trim_start();
    let lower = s.to_ascii_lowercase();
    lower.starts_with("create ")
        || lower.starts_with("drop ")
        || lower.starts_with("alter ")
        || lower.starts_with("grant ")
        || lower.starts_with("revoke ")
        || lower.starts_with("truncate ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_buffer_evicts_oldest_when_full() {
        let log = AuditLog::new(2);
        log.record("alice", "login.ok", "first");
        log.record("alice", "login.ok", "second");
        log.record("bob", "login.ok", "third");
        let recent = log.recent();
        assert_eq!(recent.len(), 2, "capacity 2 keeps only the last 2");
        assert_eq!(recent[0].detail, "second");
        assert_eq!(recent[1].detail, "third");
        assert_eq!(recent[1].principal, "bob");
    }

    #[test]
    fn record_populates_timestamp_and_fields() {
        let log = AuditLog::new(8);
        log.record("carol", "ddl", "CREATE TABLE t");
        let e = &log.recent()[0];
        assert_eq!(e.principal, "carol");
        assert_eq!(e.action, "ddl");
        assert_eq!(e.detail, "CREATE TABLE t");
        assert!(e.ts_unix_nanos > 0, "timestamp should be set");
    }

    #[test]
    fn is_audited_sql_matches_ddl_and_privilege_prefixes() {
        assert!(is_audited_sql("CREATE TABLE t (id int)"));
        assert!(is_audited_sql("  drop index i"));
        assert!(is_audited_sql("ALTER TABLE t ADD COLUMN c"));
        assert!(is_audited_sql("GRANT SELECT ON t TO r"));
        assert!(is_audited_sql("REVOKE ALL FROM u"));
        assert!(is_audited_sql("truncate t"));
        // Non-DDL is not audited.
        assert!(!is_audited_sql("SELECT * FROM t"));
        assert!(!is_audited_sql("INSERT INTO t VALUES (1)"));
    }
}
