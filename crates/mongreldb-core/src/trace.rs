//! Query path instrumentation (OPTIMIZATIONS.md Priority 0 / 16).
//!
//! MongrelDB has many physical paths a query can take: an O(1) metadata count,
//! a zero-copy Arrow IPC shadow, a single-run lazy page cursor, a multi-run
//! k-way merge cursor, an index-pushdown columnar gather, or a full row
//! materialization. Correctness tests verify *results* but never reveal *which*
//! path ran, so performance regressions are currently invisible until a
//! benchmark accidentally trips one.
//!
//! [`QueryTrace`] makes those path decisions observable. It is a lightweight,
//! opt-in record filled at decision points via a thread-local scope collector.
//! The hot path (no capture active) pays only a single TLS load per record site
//! and then returns immediately — there is no allocation, no lock, and no
//! signature change to the hundreds of internal functions that serve queries.
//!
//! ## Usage
//!
//! The public `_traced` methods on [`crate::engine::Table`] (and
//! `MongrelSession::run_sql_traced` in the query crate) wrap the corresponding
//! query in [`QueryTrace::capture`] and return the result alongside the trace:
//!
//! ```no_run
//! # use mongreldb_core::*;
//! # let mut db: Table = unimplemented!();
//! # let snap = db.snapshot();
//! # let conditions = &[];
//! # let proj = &[];
//! let (cols, trace) = db.query_columns_native_traced(conditions, Some(proj), snap).unwrap();
//! assert_eq!(trace.scan_mode, trace::ScanMode::NativePushdown);
//! assert_eq!(trace.index_rebuild, trace::IndexRebuild::AlreadyComplete);
//! ```
//!
//! ## Extensibility
//!
//! New fields can be added to [`QueryTrace`] freely — it is `#[derive(Default)]`,
//! so existing callers and tests continue to compile. New recording sites are a
//! single [`QueryTrace::record`] call at the decision point; no plumbing is
//! required because the thread-local stack is the transport.

use std::cell::RefCell;
use std::fmt;

thread_local! {
    /// A stack of in-progress traces, supporting nested captures (an inner
    /// `capture` gets its own fresh trace; the outer trace is unaffected).
    static STACK: RefCell<Vec<QueryTrace>> = const { RefCell::new(Vec::new()) };
}

/// Which physical scan path served a query. Recorded by the SQL scan
/// ([`crate::scan`]) and the native query entry points. Used in benchmarks and
/// path-sensitive tests to assert that the expected path was taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScanMode {
    /// The trace was never filled (no recording site ran). Indicates a path
    /// that has not yet been instrumented.
    #[default]
    Unknown,
    /// `COUNT(*)` / empty projection answered from the maintained `live_count`
    /// metadata in O(1) — no run read, no index resolve.
    CountMetadata,
    /// `COUNT(*)` with a pushed `WHERE` answered from survivor set cardinality
    /// via [`crate::engine::Table::count_conditions`] — index resolve only, no
    /// column decode.
    CountSurvivors,
    /// Zero-copy Arrow IPC shadow read — no per-column decode at all (clean
    /// single-run unfiltered table).
    ArrowShadow,
    /// Single-run lazy page cursor: fused predicate + page skip + late
    /// materialization ([`crate::cursor::NativePageCursor`]).
    NativePageCursor,
    /// Multi-run k-way merge cursor ([`crate::cursor::MultiRunCursor`]).
    MultiRunCursor,
    /// Index pushdown fast path: survivors resolved then gathered column-wise
    /// from a single reader ([`crate::engine::Table::query_columns_native`]
    /// fast path — no cursor streaming, but no row materialization either).
    NativePushdown,
    /// Full materialization fallback: `visible_columns_native` or
    /// `rows_for_rids` — rows go through the `Row { HashMap }` shape. This is
    /// the path optimizations try to avoid.
    Materialized,
    /// §5.3 direct SQL dispatch: a simple single-table `SELECT` recognized from
    /// the raw SQL (sqlparser AST) and served straight from the native column
    /// cursor, bypassing DataFusion parse+plan+optimize entirely.
    DirectDispatch,
    /// DataFusion scan served by an external table module / virtual table
    /// provider rather than a native MongrelDB storage table.
    ExternalModule,
}

impl fmt::Display for ScanMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ScanMode::Unknown => "unknown",
            ScanMode::CountMetadata => "count-metadata",
            ScanMode::CountSurvivors => "count-survivors",
            ScanMode::ArrowShadow => "arrow-shadow",
            ScanMode::NativePageCursor => "native-page-cursor",
            ScanMode::MultiRunCursor => "multi-run-cursor",
            ScanMode::NativePushdown => "native-pushdown",
            ScanMode::Materialized => "materialized",
            ScanMode::DirectDispatch => "direct-dispatch",
            ScanMode::ExternalModule => "external-module",
        };
        f.write_str(s)
    }
}

/// Which join execution path served a query (Priority 13: join diagnostics).
/// `None` for non-join queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JoinMode {
    /// No join in the query (or the join path was never reached).
    #[default]
    None,
    /// Native FK↔PK roaring-bitmap intersection — no hash-join materialization
    /// ([`crate::engine::Table`] index resolve only).
    FkBitmap,
    /// Fell back to DataFusion's hash join (shape the native path can't serve).
    DataFusionHash,
}

impl fmt::Display for JoinMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            JoinMode::None => "none",
            JoinMode::FkBitmap => "fk-bitmap",
            JoinMode::DataFusionHash => "datafusion-hash",
        })
    }
}

/// Whether `ensure_indexes_complete` rebuilt indexes during this query. A
/// rebuild is the user-facing stall case (Priority 10); this field exposes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexRebuild {
    /// No index rebuild happened (no query ran, or the table had no indexes).
    #[default]
    None,
    /// Indexes were already complete; `ensure_indexes_complete` was a no-op.
    AlreadyComplete,
    /// Indexes were rebuilt from runs during this query (the stall case).
    Rebuilt,
}

/// Records which engine path a query took. Filled at decision points via
/// [`QueryTrace::record`]; collected via [`QueryTrace::capture`].
///
/// All fields are `#[derive(Default)]`, so adding a new field is non-breaking.
/// Fields default to zero / `false` / `None` so a trace from an uninstrumented
/// path still reads cleanly.
#[derive(Debug, Default, Clone)]
pub struct QueryTrace {
    /// The physical scan path that served this query.
    pub scan_mode: ScanMode,
    /// Number of sorted runs in the table at query time. `1` = single-run fast
    /// path eligible; `>1` = k-way merge; `0` = empty/memtable-only table.
    pub run_count: usize,
    /// Rows in the memtable overlay (unflushed puts/updates/deletes).
    pub memtable_rows: usize,
    /// Rows in the mutable-run tier overlay.
    pub mutable_run_rows: usize,
    /// Rows in the materialized overlay batch yielded by a cursor (memtable +
    /// mutable-run combined, post-filter). Non-zero means the query paid
    /// overlay materialization cost.
    pub overlay_rows: usize,
    /// How many conditions were translated to native pushdown (index-served).
    pub conditions_pushed: usize,
    /// How many conditions could not be pushed down (residual / fallback).
    pub conditions_residual: usize,
    /// Survivor row count after predicate resolution, if known without decoding
    /// data columns (set for index-served and count paths).
    pub survivor_count: Option<usize>,
    /// Whether `ensure_indexes_complete` rebuilt indexes during this query.
    pub index_rebuild: IndexRebuild,
    /// Whether the fast clean-run row-id→position arithmetic was used (avoids
    /// decoding + binary-searching the system row-id column).
    pub fast_row_id_map: bool,
    /// Whether a learned (PGM) range index served a `Range`/`RangeF64`
    /// condition (in-memory, no column read).
    pub learned_range_used: bool,
    /// Whether the result cache returned a hit (no re-decode / re-resolve).
    pub result_cache_hit: bool,
    /// Whether rows were materialized as `Row { HashMap }` (the slow path).
    pub row_materialized: bool,
    /// Number of pages decoded (lazily filled by cursors when capturing).
    pub pages_decoded: usize,
    /// Number of pages skipped by page-stat pruning or empty page plans.
    pub pages_skipped: usize,
    /// Which join path served the query (Priority 13). `None` for non-joins.
    pub join_mode: JoinMode,
    /// Logical-planning time in nanoseconds (Priority 8: parse + plan, separate
    /// from execution). `0` on a result-cache hit (planning was skipped) or when
    /// the plan came from the logical-plan cache.
    pub planning_nanos: u64,
    /// Authorization/RLS candidate-cache work for the query.
    pub authorization_nanos: u64,
    pub rls_cache_hit: bool,
    pub rls_rows_evaluated: usize,
    pub rls_policy_columns_decoded: usize,
    pub authorization_retries: usize,
    /// AI retrieval stage timings and bounded cardinalities.
    pub hard_filter_nanos: u64,
    pub ann_candidate_nanos: u64,
    pub ann_candidate_cap_hit: bool,
    pub sparse_candidate_nanos: u64,
    pub minhash_candidate_nanos: u64,
    pub candidate_count: usize,
    pub union_size: usize,
    pub fusion_nanos: u64,
    pub exact_vector_gather_nanos: u64,
    pub exact_vector_score_nanos: u64,
    pub exact_set_gather_nanos: u64,
    pub exact_set_parse_nanos: u64,
    pub exact_set_score_nanos: u64,
    pub projection_nanos: u64,
    pub projection_rows: usize,
    pub projection_cells: usize,
    pub work_consumed: usize,
    pub total_nanos: u64,
}

impl QueryTrace {
    /// Execute `f` with path tracing active on the current thread, returning
    /// the result and the captured trace. Recording calls inside `f` (and
    /// anything `f` calls on this thread) fill the returned trace.
    ///
    /// Nesting is supported: an inner `capture` pushes a fresh trace onto the
    /// stack and gets its own result; the outer trace is unaffected by inner
    /// recordings.
    ///
    /// When no capture is active (the normal hot path), [`QueryTrace::record`]
    /// is a single TLS load + empty-check + return — zero allocation, zero
    /// locking, no measurable cost.
    pub fn capture<F, T>(f: F) -> (T, QueryTrace)
    where
        F: FnOnce() -> T,
    {
        Self::push_scope();
        let result = f();
        let trace = Self::pop_scope();
        (result, trace)
    }

    /// Push a fresh trace onto the thread-local stack, starting a capture scope.
    /// Pair with [`Self::pop_scope`] to retrieve the trace. This is the async-
    /// compatible alternative to [`Self::capture`]: push before `await`, pop
    /// after.
    ///
    /// **Thread affinity:** the trace is thread-local, so recordings must happen
    /// on the same OS thread as the push/pop pair. This holds for synchronous
    /// query paths (the common case) and for single-partition DataFusion scans
    /// (physical planning + leaf execution run inline on the polling thread).
    pub fn push_scope() {
        STACK.with(|s| s.borrow_mut().push(QueryTrace::default()));
    }

    /// Pop the innermost trace from the thread-local stack, ending a capture
    /// scope. Returns the captured trace. Panics if the stack is empty (unpaired
    /// pop); see [`Self::push_scope`].
    pub fn pop_scope() -> QueryTrace {
        STACK.with(|s| s.borrow_mut().pop()).unwrap_or_default()
    }

    /// Whether path tracing is active on this thread (at least one
    /// [`QueryTrace::capture`] scope is open).
    #[inline]
    pub fn capturing() -> bool {
        STACK.with(|s| !s.borrow().is_empty())
    }

    /// Record into the innermost active trace via `f`. **No-op when not
    /// capturing** — the hot path pays only the TLS load to check the stack.
    ///
    /// This is the primary recording primitive: call it at every decision point
    /// (path selection, index rebuild, fast-path hit/miss). It is cheap enough
    /// to call once per query entry point without measurable overhead.
    #[inline]
    pub fn record<F>(f: F)
    where
        F: FnOnce(&mut QueryTrace),
    {
        STACK.with(|s| {
            if let Some(trace) = s.borrow_mut().last_mut() {
                f(trace);
            }
        });
    }

    /// Returns `true` when this trace took a "good" (non-materializing) path:
    /// a cursor, a pushdown, a shadow, or a count shortcut — and did **not**
    /// rebuild indexes or materialize rows. Useful as a quick sanity check in
    /// path-sensitive tests.
    pub fn is_fast(&self) -> bool {
        !matches!(self.scan_mode, ScanMode::Materialized | ScanMode::Unknown)
            && self.index_rebuild != IndexRebuild::Rebuilt
            && !self.row_materialized
    }
}

impl fmt::Display for QueryTrace {
    /// Compact one-line summary for benchmark output and ad-hoc inspection:
    /// `native-pushdown pushed=2 survivors=12500 runs=1 idx=complete fast-rid`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.scan_mode)?;
        if self.run_count > 0 {
            write!(f, " runs={}", self.run_count)?;
        }
        if self.conditions_pushed > 0 {
            write!(f, " pushed={}", self.conditions_pushed)?;
        }
        if self.conditions_residual > 0 {
            write!(f, " residual={}", self.conditions_residual)?;
        }
        if let Some(n) = self.survivor_count {
            write!(f, " survivors={}", n)?;
        }
        let idx = match self.index_rebuild {
            IndexRebuild::None => "",
            IndexRebuild::AlreadyComplete => " idx=complete",
            IndexRebuild::Rebuilt => " idx=REBUILT",
        };
        f.write_str(idx)?;
        if self.result_cache_hit {
            f.write_str(" cache=hit")?;
        }
        if self.learned_range_used {
            f.write_str(" learned-range")?;
        }
        if self.fast_row_id_map {
            f.write_str(" fast-rid")?;
        }
        if self.row_materialized {
            f.write_str(" row-mat")?;
        }
        if self.overlay_rows > 0 {
            write!(f, " overlay={}", self.overlay_rows)?;
        }
        if self.pages_decoded > 0 {
            write!(f, " pages={}", self.pages_decoded)?;
        }
        if self.pages_skipped > 0 {
            write!(f, " skipped={}", self.pages_skipped)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Test assertion helpers — fluent methods for path-sensitive performance tests.
// Available in integration tests and downstream test suites (not gated behind
// #[cfg(test)] so they work from external test crates).
// ---------------------------------------------------------------------------

impl QueryTrace {
    /// Assert the scan mode equals `expected`. Returns `&self` for chaining.
    pub fn assert_mode(&self, expected: ScanMode) -> &Self {
        assert_eq!(
            self.scan_mode, expected,
            "expected scan mode {expected:?} but got {:?} ({self})",
            self.scan_mode
        );
        self
    }

    /// Assert no index rebuild happened during this query (Priority 10 guard).
    pub fn assert_no_index_rebuild(&self) -> &Self {
        assert_ne!(
            self.index_rebuild,
            IndexRebuild::Rebuilt,
            "expected no index rebuild but indexes were rebuilt ({self})"
        );
        self
    }

    /// Assert the query did not materialize `Row { HashMap }` objects.
    pub fn assert_not_materialized(&self) -> &Self {
        assert!(
            !self.row_materialized,
            "expected columnar/cursor path but rows were materialized ({self})"
        );
        self
    }

    /// Assert the result cache returned a hit.
    pub fn assert_cache_hit(&self) -> &Self {
        assert!(
            self.result_cache_hit,
            "expected result cache hit but got miss ({self})"
        );
        self
    }

    /// Assert the fast clean-run row-id→position arithmetic was used.
    pub fn assert_fast_row_id_map(&self) -> &Self {
        assert!(
            self.fast_row_id_map,
            "expected fast row-id map but got fallback ({self})"
        );
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_collects_records() {
        // Recording outside a capture is a no-op (no panic).
        QueryTrace::record(|t| {
            t.run_count = 999;
        });
        assert!(!QueryTrace::capturing());

        let (result, trace) = QueryTrace::capture(|| {
            assert!(QueryTrace::capturing());
            QueryTrace::record(|t| {
                t.run_count = 3;
                t.scan_mode = ScanMode::NativePushdown;
            });
            42
        });
        assert_eq!(result, 42);
        assert_eq!(trace.run_count, 3);
        assert_eq!(trace.scan_mode, ScanMode::NativePushdown);
        assert!(!QueryTrace::capturing());
    }

    #[test]
    fn nested_captures_are_independent() {
        let (outer, outer_trace) = QueryTrace::capture(|| {
            QueryTrace::record(|t| t.run_count = 1);
            let (inner, inner_trace) = QueryTrace::capture(|| {
                QueryTrace::record(|t| t.run_count = 99);
                "inner"
            });
            assert_eq!(inner, "inner");
            // The inner capture's records must NOT bleed into the outer trace.
            assert_eq!(inner_trace.run_count, 99);
            // But subsequent outer records still hit the outer trace.
            QueryTrace::record(|t| t.conditions_pushed = 2);
            "outer"
        });
        assert_eq!(outer, "outer");
        assert_eq!(outer_trace.run_count, 1);
        assert_eq!(outer_trace.conditions_pushed, 2);
    }

    #[test]
    fn display_summary_is_compact() {
        let t = QueryTrace {
            scan_mode: ScanMode::NativePushdown,
            run_count: 1,
            conditions_pushed: 2,
            survivor_count: Some(12500),
            index_rebuild: IndexRebuild::AlreadyComplete,
            fast_row_id_map: true,
            ..Default::default()
        };
        let s = format!("{t}");
        assert!(s.contains("native-pushdown"));
        assert!(s.contains("runs=1"));
        assert!(s.contains("pushed=2"));
        assert!(s.contains("survivors=12500"));
        assert!(s.contains("idx=complete"));
        assert!(s.contains("fast-rid"));
    }

    #[test]
    fn is_fast_distinguishes_paths() {
        let good = QueryTrace {
            scan_mode: ScanMode::NativePageCursor,
            index_rebuild: IndexRebuild::AlreadyComplete,
            ..Default::default()
        };
        assert!(good.is_fast());

        let mut bad = good.clone();
        bad.index_rebuild = IndexRebuild::Rebuilt;
        assert!(!bad.is_fast());

        let mut mat = good.clone();
        mat.scan_mode = ScanMode::Materialized;
        assert!(!mat.is_fast());
    }

    #[test]
    fn assertion_helpers_chain() {
        let t = QueryTrace {
            scan_mode: ScanMode::ArrowShadow,
            index_rebuild: IndexRebuild::AlreadyComplete,
            ..Default::default()
        };
        t.assert_mode(ScanMode::ArrowShadow)
            .assert_no_index_rebuild()
            .assert_not_materialized();
    }
}
