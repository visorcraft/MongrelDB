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

    /// Render the current counter values as a Prometheus text-format body.
    ///
    /// `table_count` is passed in as a gauge (it is read off the live
    /// `Database` at scrape time rather than maintained as a counter).
    pub fn prometheus_text(&self, table_count: usize) -> String {
        let sql_queries = self.sql_queries.load(Ordering::Relaxed);
        let sql_errors = self.sql_errors.load(Ordering::Relaxed);
        let slow_queries = self.slow_queries.load(Ordering::Relaxed);
        let puts = self.puts.load(Ordering::Relaxed);
        let commits = self.commits.load(Ordering::Relaxed);
        let txns = self.txns.load(Ordering::Relaxed);

        let mut out = String::with_capacity(1024);
        // mongreldb_sql_queries_total
        out.push_str("# HELP mongreldb_sql_queries_total Total /sql requests received.\n");
        out.push_str("# TYPE mongreldb_sql_queries_total counter\n");
        out.push_str(&format!("mongreldb_sql_queries_total {sql_queries}\n\n"));
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
        let body = m.prometheus_text(3);
        // HELP/TYPE lines present for every series.
        assert!(body.contains("# TYPE mongreldb_sql_queries_total counter"));
        assert!(body.contains("# TYPE mongreldb_sql_errors_total counter"));
        assert!(body.contains("# TYPE mongreldb_slow_queries_total counter"));
        assert!(body.contains("# TYPE mongreldb_puts_total counter"));
        assert!(body.contains("# TYPE mongreldb_commits_total counter"));
        assert!(body.contains("# TYPE mongreldb_txns_total counter"));
        assert!(body.contains("# TYPE mongreldb_tables gauge"));
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
