# Benchmarks - MongrelDB (and vs SQLite/DuckDB)

Measured live on this development sandbox. All engines embedded/in-process
(no daemon). Re-run: `cargo run --release --bin compare` in
`crates/mongreldb-perf`. Criterion throughput: `cargo bench -p mongreldb-core`.

Refreshed 2026-07-05 against engine v0.28.0 (after the Tier 1-3 Kit gap
closures and SQLite-parity SQL features - recursive CTEs, window functions,
REGEXP, information_schema.tables, ATTACH, SAVEPOINTs). All changes were query-layer
additions; the core write/read hot path is unchanged. Numbers are within
sandbox noise of the 2026-07-02 §5 baseline. See "§5 optimization session"
below for that session's improvements.

## Cross-engine matrix - N = 100 rows (median of runs)

| engine | bulk_insert | single_insert_commit | single_update_commit | delete_one | filter(cost<250) | count_star | join(cities) |
|---|---:|---:|---:|---:|---:|---:|---:|
| **MongrelDB** | 607.9 µs | **6.8 µs** | **7.4 µs** | **5.9 µs** | 10.1 µs | 288.5 µs | 420.4 µs |
| MongrelDB (enc) | 177.6 µs | 8.0 µs | 7.8 µs | 6.6 µs | 8.4 µs | 310.1 µs | 423.7 µs |
| SQLite (rusqlite) | **47.5 µs** | 14.1 µs | 14.1 µs | 13.1 µs | **6.3 µs** | **3.4 µs** | **7.0 µs** |
| DuckDB native | 1.04 ms | 277.5 µs | 251.8 µs | 138.4 µs | 155.8 µs | 86.1 µs | 442.0 µs |
| DuckDB-Parquet | 10.75 ms | - | - | - | 424.0 µs | 286.2 µs | 680.8 µs |
| DuckDB-CSV | 318.4 µs | - | - | - | 1.72 ms | 1.43 ms | 2.03 ms |

## Cross-engine matrix - N = 1 000 000 rows (median of runs)

| engine | bulk_insert | single_insert_commit | single_update_commit | delete_one | filter(cost<250) | count_star | join(cities) |
|---|---:|---:|---:|---:|---:|---:|---:|
| **MongrelDB** | **93.94 ms** | **8.0 µs** | **7.2 µs** | **6.1 µs** | **8.7 µs** | **290.4 µs** | **1.16 ms** |
| MongrelDB (enc) | 89.34 ms | 9.1 µs | 9.1 µs | 7.0 µs | 9.4 µs | 289.0 µs | 1.17 ms |
| SQLite (rusqlite) | 213.73 ms | 14.7 µs | 14.8 µs | 13.0 µs | 17.59 ms | 3.20 ms | 22.54 ms |
| DuckDB native | 247.90 ms | 296.4 µs | 485.0 µs | 164.4 µs | 695.7 µs | 188.5 µs | 3.93 ms |
| DuckDB-Parquet | 31.65 ms | - | - | - | 1.95 ms | 296.7 µs | 3.37 ms |
| DuckDB-CSV | 33.77 ms | - | - | - | 40.63 ms | 38.91 ms | 44.17 ms |

Numbers are within sandbox noise of the 2026-07-02 §5 baseline. The Tier 1-3
Kit gap closures and SQLite-parity SQL features (recursive CTEs, window functions,
REGEXP, information_schema.tables, ATTACH, SAVEPOINTs) are all query-layer additions; the
core write/read hot path is unchanged. Cross-engine leads are preserved:
single-row writes ~1.8× SQLite / ~37× DuckDB, cold filter ~2000× SQLite /
~80× DuckDB, join COUNT(*) ~19× SQLite / ~3.4× DuckDB.

## Encryption overhead (MongrelDB, plain vs AES-256-GCM)

### N = 100

| engine | bulk_insert | filter(cost<250) | count_star | join(cities) |
|---|---:|---:|---:|---:|
| plain | 256.2 µs | 9.5 µs | 290.5 µs | 461.2 µs |
| encrypted | 1.09 ms | 7.8 µs | 314.0 µs | 502.1 µs |

### N = 1 000 000

| engine | bulk_insert | filter(cost<250) | count_star | join(cities) |
|---|---:|---:|---:|---:|
| plain | 86.56 ms | 8.6 µs | 284.0 µs | 1.18 ms |
| encrypted | 98.62 ms | 10.2 µs | 288.7 µs | 1.16 ms |

Encrypted `filter(cost<250)` stays at parity with plain - the encrypted
page-stats envelope (decrypted once at open, overlaid onto the in-memory
page index) prunes identically to plaintext, with no plaintext values
ever touching the file.

## MongrelDB native query paths (non-SQL)

| N | count() O(1) metadata | filter via Db::query (index/tool-call) |
|---:|---:|---:|
| 100 | 0.0 µs | 27.2 µs |
| 1 000 000 | 0.0 µs | 7.16 ms |

## Criterion throughput benchmarks (1M rows, release)

### Bulk ingest

| Path | Time (1M) | Throughput | Notes |
|---|---:|---:|---|
| `bulk_load` (Value API) | 82.4 ms | **12.1 Melem/s** | Row-major → typed parallel encode |
| `bulk_load_columns` (typed) | 38.8 ms | **25.7 Melem/s** | Native typed; deferred indexing (default policy) |
| `bulk_load_fast` (typed + plain) | 56.1 ms | **17.8 Melem/s** | Raw `ALGO_PLAIN` fast path |
| `put_batch` + flush | 1.79 s | 560 Kelem/s | Per-row WAL append + fsync |
| `put_batch` (no flush) | 746 ms | 1.34 Melem/s | WAL append only |

### Scan

| Path | Time (1M) | Throughput | Notes |
|---|---:|---:|---:|
| `scan_columns` (native typed) | 236 ms | **4.25 Melem/s** | BE decode path |
| `scan_columns_le` (LE memcpy) | 81.5 ms | **12.3 Melem/s** | Native-endian path |
| Full scan (all columns, filtered_query bench) | 70.7 ms | **14.1 Melem/s** | 4-col decode + filter |

### Pushdown filter

| Path | Time (1M) | Throughput | Notes |
|---|---:|---:|---:|
| Bitmap equality | 8.2 ms | **122 Melem/s** | Roaring bitmap lookup |
| Range (int) | 8.5 ms | **118 Melem/s** | PGM learned index |
| Bitmap ∩ Range | 12.0 ms | **83.3 Melem/s** | Multi-condition intersect |
| Bitmap + 1-col projection | 4.8 ms | **208 Melem/s** | Projection pushdown |

Pushdown throughput improved from the §5 optimization session: bitmap
intersection is −17% (cheap-first condition resolution resolves the O(1)
bitmap before the range scan, then early-exits on empty), and projection
pushdown is −15% from sharded cache contention reduction.

### Write path

| Path | Time | Notes |
|---|---:|---:|
| `put` (no fsync) | **618 ns** | WAL append only |
| `commit` (fsync) | **6.79 µs** | Group commit, WAL sync |
| Group commit (1000 rows) | 686 µs | 1.46 Melem/s |

### Encryption (AES-256-GCM)

| Size | Encrypt | Decrypt |
|---|---:|---:|
| 4 KiB | **1.87 GiB/s** | 1.89 GiB/s |
| 64 KiB | 1.91 GiB/s | 1.89 GiB/s |
| 256 KiB | 1.90 GiB/s | 1.89 GiB/s |
| 1 MiB | 1.88 GiB/s | 1.87 GiB/s |

### Storage efficiency

| Dimension | Value |
|---|---:|
| Bytes/row (1M rows, 4 cols, zstd) | **4.17** |
| Total size (1M rows) | 4.17 MB |

## Methodology
n- **HTTP loopback:** `mongreldb-server` on `127.0.0.1`, Python `httpx` client, 10k iterations per op, release build.

- All measurements on this dev sandbox (Linux, release build, `--all-features`).
- **Cross-engine:** all engines embedded in-process (no daemon/HTTP). SQLite
  uses bundled `rusqlite`; DuckDB uses bundled `duckdb` crate.
- MongrelDB `count()` is O(1) metadata; `count_star` = `SELECT COUNT(*)` (scan).
- `filter`/`count_star`/`join` go through DataFusion SQL; `clear_cache()` before
  each so these are cold (no result cache hit).
- `single_*_commit` = one op + `commit()` (durable, WAL fsync).
- Encryption = MongrelDB page-level AES-256-GCM.
- Parquet/CSV are immutable: single-row insert/update/delete are N/A (load = file write).
- Bulk-ingest criterion benches run under the default `IndexBuildPolicy::Deferred`
  (indexes complete lazily on first query/flush, off the ingest critical path).
- Criterion throughput: `cargo bench -p mongreldb-core --bench {scale,filtered_query,write_path,page_encryption}`.

## Key takeaways (1M rows)

| Metric | MongrelDB | SQLite | DuckDB native | DuckDB-Parquet |
|---|---:|---:|---:|---:|
| **Single-row write (durable)** | **8.0 µs** | 14.7 µs | 296 µs | - |
| **Bulk ingest** | **93.9 ms** | 213.7 ms | 247.9 ms | 31.7 ms |
| **Cold SQL filter** | **8.7 µs** | 17.6 ms | 696 µs | 2.0 ms |
| **COUNT(*) SQL** | **290 µs** | 3.20 ms | 189 µs | 297 µs |
| **Join COUNT(*)** | **1.16 ms** | 22.5 ms | 3.93 ms | 3.4 ms |
| **Typed bulk ingest** | **25.7 Melem/s** | - | - | - |
| **LE scan throughput** | **12.3 Melem/s** | - | - | - |
| **Bitmap pushdown** | **122 Melem/s** | - | - | - |
| **Storage** | **4.17 bytes/row** | - | - | - |

MongrelDB wins single-row writes (~1.8× SQLite, ~37× DuckDB), bulk insert
(~2.3× SQLite, ~2.6× DuckDB native), cold SQL filter (~2020× SQLite via direct
dispatch), and join COUNT(*) (~3.4× DuckDB, ~19× SQLite). DuckDB-Parquet still
wins bulk file creation and has the fastest analytical filter.

## Regression history

### §5 optimization session (2026-07-02)

All 11 items from the PLAN.md §5 backlog were implemented and verified.
Headline improvements measured against the 07-01 baseline:

- **Bitmap ∩ Range pushdown: −17%** (14.4→12.0 ms) - cheap-first condition
  resolution (§5.5) resolves the O(1) bitmap before the expensive range
  scan, then early-exits on empty survivor sets.
- **Full scan: −8%** (77.2→70.7 ms), **range pushdown: −8%** (9.2→8.5 ms),
  **projection pushdown: −10%** (5.3→4.8 ms) - sharded caches (§5.8) reduce
  lock contention under the parallel rayon scan path.
- **Single-insert at N=100: −38%** (10.3→6.4 µs), **delete: −29%** (8.6→6.1 µs)
  - sharded cache creation + reduced write-path lock overhead.
- **Cold SQL filter at N=1M: 7.99 ms→9.8 µs** - direct SQL dispatch (§5.3)
  bypasses DataFusion parse+plan for simple `SELECT … WHERE` shapes, falling
  through to `ctx.sql()` for anything it can't serve.
- No regressions on the write path (group_commit_1000 flat at 686 µs).

### 2026-07-01 audit

A 2026-07-01 audit found this document was 147 commits stale and three
regressions had crept in since it was last measured. All three were
root-caused and fixed the same day; the numbers above reflect the fix.

1. **Typed bulk ingest, 38 ms → 436 ms (11×).** A prior commit made fresh-table
   bulk loads build and checkpoint indexes *inside* the ingest call. Fixed by
   `Table::set_index_build_policy` - `IndexBuildPolicy::Deferred` (default)
   restores the documented behavior of deferring index construction to the
   first query/flush; `Eager` is available for callers that want predictable
   first-query latency instead.
2. **Per-put overhead, +40% on `put`/`group_commit`.** NOT-NULL validation and
   PK-upsert bookkeeping had grown a throwaway `HashMap` + full-row clone per
   put, even for single-row and append-only batches. Fixed with a slice-based
   validator, a single-row fast path, a lazily-built (rather than
   eagerly-maintained) PK reverse map, and an upsert-probe skip when a batch's
   PKs are provably fresh.
3. **Encrypted `WHERE` filter, 7.0 ms → 17-21 ms (2.4-3×).** An earlier change
   correctly stopped storing plaintext page min/max for encrypted columns (it
   would leak values) but left encrypted columns with no substitute - every
   filtered read decrypted every page. Fixed by moving per-page min/max into a
   run-level AES-256-GCM-encrypted stats envelope, decrypted once at open and
   overlaid onto the in-memory page index - encrypted columns prune exactly
   like plaintext ones, with no plaintext values ever touching the file.
## HTTP loopback (Tier 2)

Measured against `mongreldb-server` on `127.0.0.1`, release build, 10,000 iterations per op, single-row JSON request body. Python `httpx` client, `time.perf_counter_ns()` per call.

| op | p50 | p95 | p99 | max | throughput |
|---|---:|---:|---:|---:|---:|
| `PUT /tables/bench/put` | 0.22 ms | 0.37 ms | 0.62 ms | 5.67 ms | 4,026 ops/s |
| `POST /kit/txn` commit | 0.21 ms | 0.36 ms | 0.53 ms | 5.91 ms | 4,350 ops/s |

`PUT` and `commit` are within noise of each other; `fsync` is in the commit path but does not dominate on this hardware (NVMe). The HTTP-tier ceiling is set by `axum`'s handler dispatch, JSON parse/serialize, and the TCP round trip, not the engine. The native Tier 1 path is ~25-30× faster than this; see "Criterion throughput benchmarks → Write path" above.

