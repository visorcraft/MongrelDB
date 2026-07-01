# MongrelDB Optimization Backlog

This file is a working backlog of high-probability performance routes. It is
intentionally biased toward areas where the current code already shows a
fallback, broad materialization, extra hashing, or a cold-path surprise. Each
item should be treated as a hypothesis until a benchmark proves it.

## Research Update - 2026-06-29

Local research against the current `crates/` implementation found that parts of
this backlog have already moved forward: multi-run cursors, SQL `IN` pushdown,
native aggregate interception, join `COUNT(*)`, residual LIKE pre-filtering, and
fixed-width Arrow owned conversion are partially implemented.

The recurring unresolved cost pattern was:

1. candidate sets were frequently converted to `HashSet<u64>`;
2. clean runs still decoded or binary-searched `_row_id` in hot paths;
3. overlay and multi-run paths did broad upfront MVCC work before selectivity or
   `LIMIT` could help;
4. lazy index completion after bulk load was still a user-facing stall;
5. path selection was hard to assert or benchmark.

Focused benchmark command:

```bash
cargo bench -p mongreldb-core --bench filtered_query
cargo bench -p mongreldb-core --bench path_matrix
```

## Status Update - 2026-07-01

Several backlog items have since landed and are now reflected below (moved into
"Implemented" or annotated on their priority). In particular: overlay
materialization is now bounded to the survivor set (`9d368e1`); `COUNT(DISTINCT
col)` is served from bitmap cardinality (`3fd142c`, `count_distinct_from_bitmap`);
`MIN`/`MAX`/`COUNT(col)` are answered from page stats and native aggregates run
over any run layout via `scan_cursor` (`3d2669e`, `284f3d2`,
`aggregate_from_stats`/`aggregate_native`); the raw-page cache exposes hit/miss
counters (`5257d74`); `IS NULL`/`IS NOT NULL` push down as page-stat-aware
`Condition::IsNull`/`IsNotNull`; multi-segment `LIKE` intersects FM lookups via
`Condition::FmContainsAll`; and the Kit accelerates FK equality joins with an
indexed `BitmapIn` probe. The remaining open items are the ones still marked ⬜ /
"open" further down.

## Implemented

The following items from the "Suggested First Pass" and revised priorities are
done:

1. **RowIdSet representation** (Priority 2) — `crates/mongreldb-core/src/row_id_set.rs`.
   Bitmap/range/PK/ANN/FM results stay in their native representation until an
   API boundary forces materialization.
2. **Clean-run row-id-to-position arithmetic** (Priority 3) — `positions_for_row_ids_fast`
   bypasses SYS_ROW_ID decode for contiguous-row-id runs.
3. **Filtered COUNT(\*) from survivor cardinality** (Priority 7 partial) —
   `Table::count_conditions` resolves index survivor cardinality without column
   decode (~2.5 µs for 50 k survivors on 1 M rows).
4. **Path instrumentation and benchmark matrix** (Priority 0 + 16) —
   `crates/mongreldb-core/src/trace.rs`: `QueryTrace` with thread-local scope
   collector, `_traced` API variants, fluent test assertions, and
   `benches/path_matrix.rs` (path-aware benchmark printing traces).
5. **Columnar cursor fallback for `query_columns_native`** (Priority 1 + 4) —
   the non-fast-path no longer falls to per-rid `rows_for_rids`. It routes
   through `drain_cursor_to_columns`. Multi-run pushdown: ~191 s → ~111 ms.
   Dirty single-run overlay: ~46 ms → ~32 ms.
6. **Eager index build for empty bulk loads** (Priority 10 partial) — fresh
   empty-table bulk loads build/checkpoint indexes eagerly; deferred rebuild
   preserved for non-empty loads.
7. **Kit Priority 1: Rust Kit Predicate Pushdown** —
   `mongreldb_kit/crates/mongreldb-kit/src/pushdown.rs`: `Expr`→`Condition`
   translator. `get_by_pk_internal` and FK `parent_exists` go from O(N) scan to
   O(1) HOT probe. `run_select`/`run_aggregate` push translatable filters to
   core; unsupported expressions fall back to full scan.
8. **Overlay materialization bounded to the survivor set** (Priority 2/5) —
   `9d368e1`: selective queries no longer materialize every overlay row up front.
9. **Native aggregates from page stats over any run layout** (Priority 7) —
   `aggregate_from_stats` answers `MIN`/`MAX`/`COUNT(col)` from page bounds/null
   counts, and `aggregate_native` runs over any run layout via `scan_cursor`
   (`3d2669e`, `284f3d2`).
10. **`COUNT(DISTINCT col)` from bitmap cardinality** (Priority 7) — `3fd142c`,
    `count_distinct_from_bitmap`.
11. **`IS NULL` / `IS NOT NULL` pushdown** (Priority 6/11) — page-stat-aware
    `Condition::IsNull` / `IsNotNull` (skips pages by null count).
12. **Multi-segment `LIKE` via FM intersection** (Priority 12) —
    `Condition::FmContainsAll` intersects per-segment FM lookups.
13. **Raw-page cache hit/miss counters** (Priority 14) — `5257d74`.
14. **Kit FK-join acceleration** (Priority 13, partial) — eq-joins probe the
    right table with an indexed `BitmapIn` over distinct left keys.

The revised core priority order:

1. ✅ path trace and benchmark matrix;
2. ✅ selective overlay probing (survivor-bounded overlay materialization);
3. ✅ multi-run/page-plan selectivity (cursor fallback closes the big gap);
4. 🔶 SQL filter coverage and LIKE selectivity (IS NULL + multi-segment LIKE
   done; OR→BitmapIn, anchored LIKE, trigram indexes open);
5. 🔶 join diagnostics and columnar output (Kit FK-join + Arrow `executeArrow`
   done; native semi/anti + both-side-filtered joins open);
6. 🔶 cache contention and query-cost compaction (hit/miss counters done; cache
   sharding + compaction-as-optimization open);
7. ⬜ physical-plan/direct SQL dispatch and broader Arrow shadowing.

---

## Todo - MongrelDB Kit

MongrelDB Kit is mostly a schema/query/constraint layer, so its performance work
should target bridge overhead and places where it hides native engine fast paths.

### Kit Priority 0: Cross-Language Benchmarks

**Route:** add a benchmark suite that exercises the same workload through Rust
Kit, TypeScript Kit, Python Kit, and direct MongrelDB core/NAPI calls.

### Kit Priority 1: Rust Kit Predicate Pushdown — ✅ DONE

### Kit Priority 2: TypeScript Predicate Collapsing

**Route:** compile a whole TypeScript predicate tree into one native condition
list when possible. `AND` of pushable leaves → one `table.query(conditions)`.
`IN` → one `BitmapIn` call. Filtered `count()` → native count.

### Kit Priority 3: TypeScript Row Conversion Overhead

**Route:** `rowFromRowJs` does `cells.find()` per schema column (O(cols × cells)
per row). Build a `Map<number, Cell>` once per row or return cells pre-sorted.

### Kit Priority 4: Python Bridge Without JSON Strings — ⬜ OPEN (regressed slightly)

**Route:** return Python objects directly from PyO3 instead of `json.dumps` →
`json.loads` per row. Convert `serde_json::Value` to `PyObject` directly.

**Note (2026-07-01):** row reads already build `PyDict`s directly (no JSON), but
several newer analytics surfaces (`approx_aggregate`, `incremental_aggregate`,
`explain`, `set_similarity`) return a JSON string that the facade `json.loads`.
Those are low call-rate so the cost is negligible, but the direct
`serde_json::Value` → `PyObject` conversion this priority wants would also cover
them.

### Kit Priority 5: Joins And Grouping

**Route:** push base-side filters before join materialization. Use FK/PK
equality metadata to query only matching children/parents.

### Kit Priority 6: Bulk Write Crossings And Guard Checks

**Route:** batch unique guard lookups by owner/key. Batch FK parent existence
checks for repeated parent ids. Add packed guard-row put/delete operations.

---

## Remaining Core Priorities

### Priority 5: Overlay-Aware Querying Without Full Overlay Materialization — 🔶 PARTIAL

**Done:** overlay materialization is now bounded to the survivor set (`9d368e1`).
**Route (remaining):** lightweight overlay indexes / reuse HOT/bitmap entries with
an overlay row-id filter for the still-broad cases.

### Priority 6: Push More SQL Filters Into Native Conditions — 🔶 PARTIAL

**Done:** `IS NULL`/`IS NOT NULL` push down (page-stat-aware).
**Route (remaining):** OR-of-equalities → `BitmapIn`, boolean equality,
date/timestamp range literals, cast canonicalization.

### Priority 7: Native Count/Aggregate From Survivor Cardinality (extended) — ✅ DONE

`COUNT(*) WHERE bitmap/range/pk`, `COUNT(col)` from page stats/null counts,
`MIN`/`MAX` from page bounds (`aggregate_from_stats`), and `COUNT(DISTINCT
low-card)` from the bitmap partition (`count_distinct_from_bitmap`) all landed.
Native aggregates run over any run layout via `scan_cursor` (`aggregate_native`).

### Priority 8: Physical Plan Cache And Fast SQL Dispatch

**Route:** cache optimized physical plans keyed by normalized SQL + schema
epoch. Add a direct SQL-shape recognizer for single-table projection/filter/
limit that invokes the cursor directly. Track planning time separately.

### Priority 9: Arrow Conversion And Zero-Copy Surfaces

**Route:** use Arrow buffers directly for owned `Vec<i64>`/`Vec<f64>`. Add
background shadow refresh for compacted/multi-run snapshots. Add partial shadow
reads for projected columns.

### Priority 11: Page-Level Predicate Plans For More Conditions — 🔶 PARTIAL

**Done:** page-level null-count handling for `IS NULL`/`IS NOT NULL`.
**Route (remaining):** page plans from stats for range-only scans; fuse
bitmap+range at the page-plan level.

### Priority 12: LIKE/FM Residual Reduction — 🔶 PARTIAL

**Done:** multiple literal segments intersected via `Condition::FmContainsAll`.
**Route (remaining):** prefix/suffix-aware filters for anchored `LIKE 'abc%'` /
`LIKE '%abc'`; trigram/n-gram indexes for short patterns.

### Priority 13: Join Fast-Path Coverage — 🔶 PARTIAL

**Done:** FK/PK equality joins accelerated with an indexed `BitmapIn` probe
(Kit); FK/PK `COUNT(*)` shapes.
**Route (remaining):** projected joins, filtered joins on both sides,
semi/anti-joins via bitmap/FK indexes, join-shape explain diagnostics.

### Priority 14: Cache Contention And Cache Hit Visibility — 🔶 PARTIAL

**Done:** raw-page cache hit/miss counters (`5257d74`).
**Route (remaining):** try-lock-miss counters; shard caches by run/page hash;
per-query local decoded cache for hot projected pages.

### Priority 15: Compaction Policy As A Query Optimization

**Route:** track query penalties from run count and overlay size. Trigger
background compaction when queries repeatedly hit multi-run fallback. Preserve
learned range indexes and Arrow shadows across compaction.
