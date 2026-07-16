//! Lightweight Prometheus-compatible metrics + slow-query instrumentation.
//!
//! No external metrics crate — counters are plain `AtomicU64`s and the
//! `/metrics` handler emits the [Prometheus text exposition format][1] by hand.
//! This keeps the daemon's dependency surface minimal and matches the
//! "sub-ms writes" ethos: the counters are bumped with `Relaxed` ordering (no
//! global fence on the hot path).
//!
//! [1]: https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md

use std::sync::atomic::{AtomicU64, Ordering};

/// Daemon-wide counters, bumped from every instrumented HTTP handler.
///
/// Stored as `Arc<Metrics>` inside `AppState` so all handlers share one set.
/// Every field is a monotonic counter unless noted (gauges are snapshot at
/// scrape time).
#[derive(Default)]
pub struct Metrics {
    /// Total `/sql` requests received (before execution).
    pub sql_queries: AtomicU64,
    /// `/sql` requests that returned an error.
    pub sql_errors: AtomicU64,
    /// `/sql` requests slower than the configured slow-query threshold.
    pub slow_queries: AtomicU64,
    /// Total single-row `PUT /tables/{name}/put` calls.
    pub puts: AtomicU64,
    /// Total `POST /tables/{name}/commit` calls.
    pub commits: AtomicU64,
    /// Total `POST /txn` atomic batch calls.
    pub txns: AtomicU64,
    pub sql_cancel_requests: AtomicU64,
    pub sql_cancelled: AtomicU64,
    pub sql_cancelled_by_reason: [AtomicU64; 6],
    pub sql_deadline_exceeded: AtomicU64,
    pub sql_commit_cancel_winner_cancel: AtomicU64,
    pub sql_commit_cancel_winner_commit: AtomicU64,
    pub sql_stuck_after_cancel: AtomicU64,
    pub sql_output_bytes: AtomicU64,
    pub sql_cancel_latency_micros: AtomicU64,
    pub sql_cancel_latency_count: AtomicU64,
}

impl Metrics {
    /// Bump `sql_queries`.
    #[inline]
    pub fn inc_sql_queries(&self) {
        self.sql_queries.fetch_add(1, Ordering::Relaxed);
    }

    /// Bump `sql_errors`.
    #[inline]
    pub fn inc_sql_errors(&self) {
        self.sql_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Bump `slow_queries`.
    #[inline]
    pub fn inc_slow_queries(&self) {
        self.slow_queries.fetch_add(1, Ordering::Relaxed);
    }

    /// Bump `puts`.
    #[inline]
    pub fn inc_puts(&self) {
        self.puts.fetch_add(1, Ordering::Relaxed);
    }

    /// Bump `commits`.
    #[inline]
    pub fn inc_commits(&self) {
        self.commits.fetch_add(1, Ordering::Relaxed);
    }

    /// Bump `txns`.
    #[inline]
    pub fn inc_txns(&self) {
        self.txns.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_sql_cancel_requests(&self) {
        self.sql_cancel_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_sql_cancelled(&self, reason: mongreldb_core::CancellationReason) {
        self.sql_cancelled.fetch_add(1, Ordering::Relaxed);
        self.sql_cancelled_by_reason[reason as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_sql_deadline_exceeded(&self) {
        self.sql_deadline_exceeded.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_sql_commit_cancel_winner_cancel(&self) {
        self.sql_commit_cancel_winner_cancel
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_sql_commit_cancel_winner_commit(&self) {
        self.sql_commit_cancel_winner_commit
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_sql_stuck_after_cancel(&self, count: usize) {
        self.sql_stuck_after_cancel
            .fetch_add(count.min(u64::MAX as usize) as u64, Ordering::Relaxed);
    }

    pub fn add_sql_output_bytes(&self, bytes: usize) {
        self.sql_output_bytes
            .fetch_add(bytes.min(u64::MAX as usize) as u64, Ordering::Relaxed);
    }

    pub fn observe_sql_cancel_latency(&self, latency: std::time::Duration) {
        self.sql_cancel_latency_micros.fetch_add(
            latency.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        self.sql_cancel_latency_count
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Render the current counter values as a Prometheus text-format body.
    ///
    /// `table_count` is passed in as a gauge (it is read off the live
    /// `Database` at scrape time rather than maintained as a counter).
    pub fn prometheus_text(
        &self,
        table_count: usize,
        registry: mongreldb_query::QueryRegistryStats,
        pre_cancel: (usize, usize),
    ) -> String {
        let (pre_cancel_entries, pre_cancel_bytes) = pre_cancel;
        let sql_queries = self.sql_queries.load(Ordering::Relaxed);
        let sql_errors = self.sql_errors.load(Ordering::Relaxed);
        let slow_queries = self.slow_queries.load(Ordering::Relaxed);
        let puts = self.puts.load(Ordering::Relaxed);
        let commits = self.commits.load(Ordering::Relaxed);
        let txns = self.txns.load(Ordering::Relaxed);
        let cancel_requests = self.sql_cancel_requests.load(Ordering::Relaxed);
        let cancelled = self.sql_cancelled.load(Ordering::Relaxed);
        let cancelled_by_reason = self
            .sql_cancelled_by_reason
            .each_ref()
            .map(|counter| counter.load(Ordering::Relaxed));
        let deadline_exceeded = self.sql_deadline_exceeded.load(Ordering::Relaxed);
        let race_cancel = self.sql_commit_cancel_winner_cancel.load(Ordering::Relaxed);
        let race_commit = self.sql_commit_cancel_winner_commit.load(Ordering::Relaxed);
        let stuck = self.sql_stuck_after_cancel.load(Ordering::Relaxed);
        let output_bytes = self.sql_output_bytes.load(Ordering::Relaxed);
        let cancel_latency_micros = self.sql_cancel_latency_micros.load(Ordering::Relaxed);
        let cancel_latency_count = self.sql_cancel_latency_count.load(Ordering::Relaxed);

        let mut out = String::with_capacity(1024);
        // mongreldb_sql_queries_total
        out.push_str("# HELP mongreldb_sql_queries_total Total /sql requests received.\n");
        out.push_str("# TYPE mongreldb_sql_queries_total counter\n");
        out.push_str(&format!("mongreldb_sql_queries_total {sql_queries}\n\n"));
        out.push_str("# TYPE mongreldb_sql_active_queries gauge\n");
        out.push_str(&format!(
            "mongreldb_sql_active_queries {}\n\n",
            registry.active
        ));
        out.push_str("# TYPE mongreldb_sql_queued_queries gauge\n");
        out.push_str(&format!(
            "mongreldb_sql_queued_queries {}\n\n",
            registry.queued
        ));
        out.push_str("# TYPE mongreldb_sql_cancel_requests_total counter\n");
        out.push_str(&format!(
            "mongreldb_sql_cancel_requests_total {cancel_requests}\n\n"
        ));
        out.push_str("# TYPE mongreldb_sql_cancelled_total counter\n");
        out.push_str(&format!("mongreldb_sql_cancelled_total {cancelled}\n"));
        for (index, reason) in [
            "none",
            "client_request",
            "deadline",
            "client_disconnected",
            "session_closed",
            "server_shutdown",
        ]
        .into_iter()
        .enumerate()
        {
            out.push_str(&format!(
                "mongreldb_sql_cancelled_total{{reason=\"{reason}\"}} {}\n",
                cancelled_by_reason[index]
            ));
        }
        out.push('\n');
        out.push_str("# TYPE mongreldb_sql_cancel_latency_seconds summary\n");
        out.push_str(&format!(
            "mongreldb_sql_cancel_latency_seconds_sum {}\n",
            cancel_latency_micros as f64 / 1_000_000.0
        ));
        out.push_str(&format!(
            "mongreldb_sql_cancel_latency_seconds_count {cancel_latency_count}\n\n"
        ));
        out.push_str("# TYPE mongreldb_sql_deadline_exceeded_total counter\n");
        out.push_str(&format!(
            "mongreldb_sql_deadline_exceeded_total {deadline_exceeded}\n\n"
        ));
        out.push_str("# TYPE mongreldb_sql_commit_cancel_races_total counter\n");
        out.push_str(&format!(
            "mongreldb_sql_commit_cancel_races_total{{winner=\"cancel\"}} {race_cancel}\n"
        ));
        out.push_str(&format!(
            "mongreldb_sql_commit_cancel_races_total{{winner=\"commit\"}} {race_commit}\n\n"
        ));
        out.push_str("# TYPE mongreldb_sql_registry_entries gauge\n");
        out.push_str(&format!(
            "mongreldb_sql_registry_entries {}\n\n",
            registry.active + registry.detailed + registry.compact
        ));
        out.push_str("# TYPE mongreldb_sql_registry_bytes gauge\n");
        out.push_str(&format!(
            "mongreldb_sql_registry_bytes {}\n\n",
            registry.detailed_bytes + registry.compact_bytes
        ));
        out.push_str("# TYPE mongreldb_sql_registry_active gauge\n");
        out.push_str(&format!(
            "mongreldb_sql_registry_active {}\n",
            registry.active
        ));
        out.push_str("# TYPE mongreldb_sql_registry_queued gauge\n");
        out.push_str(&format!(
            "mongreldb_sql_registry_queued {}\n",
            registry.queued
        ));
        out.push_str("# TYPE mongreldb_sql_registry_detailed gauge\n");
        out.push_str(&format!(
            "mongreldb_sql_registry_detailed {}\n",
            registry.detailed
        ));
        out.push_str("# TYPE mongreldb_sql_registry_compact gauge\n");
        out.push_str(&format!(
            "mongreldb_sql_registry_compact {}\n",
            registry.compact
        ));
        out.push_str("# TYPE mongreldb_sql_registry_detailed_bytes gauge\n");
        out.push_str(&format!(
            "mongreldb_sql_registry_detailed_bytes {}\n",
            registry.detailed_bytes
        ));
        out.push_str("# TYPE mongreldb_sql_registry_compact_bytes gauge\n");
        out.push_str(&format!(
            "mongreldb_sql_registry_compact_bytes {}\n",
            registry.compact_bytes
        ));
        out.push_str("# TYPE mongreldb_sql_registry_demotions_total counter\n");
        out.push_str(&format!(
            "mongreldb_sql_registry_demotions_total {}\n",
            registry.demotions
        ));
        out.push_str("# TYPE mongreldb_sql_registry_compact_evictions_total counter\n");
        out.push_str(&format!(
            "mongreldb_sql_registry_compact_evictions_total {}\n",
            registry.compact_evictions
        ));
        out.push_str("# TYPE mongreldb_sql_registry_rejections_total counter\n");
        out.push_str(&format!(
            "mongreldb_sql_registry_rejections_total{{reason=\"active_limit\"}} {}\n",
            registry.active_rejections
        ));
        out.push_str("# TYPE mongreldb_sql_registry_oldest_compact_age_seconds gauge\n");
        out.push_str(&format!(
            "mongreldb_sql_registry_oldest_compact_age_seconds {}\n\n",
            registry.oldest_compact_age.as_secs_f64()
        ));
        out.push_str("# TYPE mongreldb_sql_pre_cancel_entries gauge\n");
        out.push_str(&format!(
            "mongreldb_sql_pre_cancel_entries {pre_cancel_entries}\n\n"
        ));
        out.push_str("# TYPE mongreldb_sql_pre_cancel_bytes gauge\n");
        out.push_str(&format!(
            "mongreldb_sql_pre_cancel_bytes {pre_cancel_bytes}\n\n"
        ));
        out.push_str("# TYPE mongreldb_sql_stuck_after_cancel_total counter\n");
        out.push_str(&format!(
            "mongreldb_sql_stuck_after_cancel_total {stuck}\n\n"
        ));
        out.push_str("# TYPE mongreldb_sql_output_bytes counter\n");
        out.push_str(&format!("mongreldb_sql_output_bytes {output_bytes}\n\n"));
        // mongreldb_sql_errors_total
        out.push_str("# HELP mongreldb_sql_errors_total /sql requests that returned an error.\n");
        out.push_str("# TYPE mongreldb_sql_errors_total counter\n");
        out.push_str(&format!("mongreldb_sql_errors_total {sql_errors}\n\n"));
        // mongreldb_slow_queries_total
        out.push_str(
            "# HELP mongreldb_slow_queries_total /sql requests above the slow-query threshold.\n",
        );
        out.push_str("# TYPE mongreldb_slow_queries_total counter\n");
        out.push_str(&format!("mongreldb_slow_queries_total {slow_queries}\n\n"));
        // mongreldb_puts_total
        out.push_str("# HELP mongreldb_puts_total Single-row put requests.\n");
        out.push_str("# TYPE mongreldb_puts_total counter\n");
        out.push_str(&format!("mongreldb_puts_total {puts}\n\n"));
        // mongreldb_commits_total
        out.push_str("# HELP mongreldb_commits_total Explicit table commit requests.\n");
        out.push_str("# TYPE mongreldb_commits_total counter\n");
        out.push_str(&format!("mongreldb_commits_total {commits}\n\n"));
        // mongreldb_txns_total
        out.push_str("# HELP mongreldb_txns_total Atomic /txn batch requests.\n");
        out.push_str("# TYPE mongreldb_txns_total counter\n");
        out.push_str(&format!("mongreldb_txns_total {txns}\n\n"));
        // mongreldb_tables (gauge)
        out.push_str("# HELP mongreldb_tables Current number of tables.\n");
        out.push_str("# TYPE mongreldb_tables gauge\n");
        out.push_str(&format!("mongreldb_tables {table_count}\n"));
        out
    }
}

/// Read the slow-query threshold from the `MONGRELBL_SLOW_QUERY_MS` env var,
/// defaulting to 100 ms. Returns the threshold as a `Duration`.
pub fn slow_query_threshold() -> std::time::Duration {
    const DEFAULT_MS: u64 = 100;
    let ms = std::env::var("MONGRELBL_SLOW_QUERY_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_MS);
    std::time::Duration::from_millis(ms.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_text_contains_all_series_and_types() {
        let m = Metrics::default();
        m.inc_sql_queries();
        m.inc_sql_queries();
        m.inc_sql_errors();
        m.inc_slow_queries();
        m.inc_puts();
        m.inc_commits();
        m.inc_txns();
        let body = m.prometheus_text(
            3,
            mongreldb_query::QueryRegistryStats {
                active: 1,
                queued: 1,
                detailed: 1,
                detailed_bytes: 512,
                ..Default::default()
            },
            (1, 128),
        );
        // HELP/TYPE lines present for every series.
        assert!(body.contains("# TYPE mongreldb_sql_queries_total counter"));
        assert!(body.contains("# TYPE mongreldb_sql_errors_total counter"));
        assert!(body.contains("# TYPE mongreldb_slow_queries_total counter"));
        assert!(body.contains("# TYPE mongreldb_puts_total counter"));
        assert!(body.contains("# TYPE mongreldb_commits_total counter"));
        assert!(body.contains("# TYPE mongreldb_txns_total counter"));
        assert!(body.contains("# TYPE mongreldb_tables gauge"));
        assert!(body.contains("mongreldb_sql_pre_cancel_entries 1"));
        assert!(body.contains("mongreldb_sql_pre_cancel_bytes 128"));
        // Counter values reflect the bumps.
        assert!(body.contains("mongreldb_sql_queries_total 2"));
        assert!(body.contains("mongreldb_sql_errors_total 1"));
        assert!(body.contains("mongreldb_slow_queries_total 1"));
        assert!(body.contains("mongreldb_puts_total 1"));
        assert!(body.contains("mongreldb_commits_total 1"));
        assert!(body.contains("mongreldb_txns_total 1"));
        // Gauge value.
        assert!(body.contains("mongreldb_tables 3"));
    }

    #[test]
    fn default_threshold_is_100ms() {
        // Only assert the default when the env var is not set in the test
        // environment; if a developer sets it, respect their value.
        if std::env::var("MONGRELBL_SLOW_QUERY_MS").is_err() {
            assert_eq!(
                slow_query_threshold(),
                std::time::Duration::from_millis(100)
            );
        }
    }
}
