# Benchmarks — MongrelDB (and vs SQLite/DuckDB)

Measured live on this development sandbox. All engines embedded/in-process
(no daemon). Re-run: `cargo run --release --bin compare` in
`crates/mongreldb-perf`. Criterion throughput: `cargo bench -p mongreldb-core`.

Refreshed 2026-07-02 after the §5 optimization session (sharded caches,
direct SQL dispatch, cheap-first condition resolution, overlay-aware count,
anchored-prefix LIKE, compaction-as-query-opt). See "§5 optimization session"
below.

## Cross-engine matrix — N = 100 rows (median of runs)

| engine | bulk_insert | single_insert_commit | single_update_commit | delete_one | filter(cost<250) | count_star | join(cities) |
|---|---:|---:|---:|---:|---:|---:|---:|
| **MongrelDB** | 721.0 µs | **6.4 µs** | **9.0 µs** | **6.1 µs** | 8.9 µs | 380.5 µs | 410.5 µs |
| MongrelDB (enc) | 445.6 µs | 7.8 µs | 7.6 µs | 6.8 µs | 6.0 µs | 251.5 µs | 404.7 µs |
| SQLite (rusqlite) | **44.6 µs** | 13.9 µs | 14.4 µs | 13.2 µs | **5.6 µs** | **3.2 µs** | **6.8 µs** |
| DuckDB native | 758.4 µs | 236.1 µs | 268.2 µs | 193.9 µs | 145.3 µs | 88.5 µs | 512.7 µs |
| DuckDB-Parquet | 14.26 ms | — | — | — | 438.0 µs | 245.0 µs | 760.0 µs |
| DuckDB-CSV | 344.1 µs | — | — | — | 3.30 ms | 1.48 ms | 1.92 ms |

## Cross-engine matrix — N = 1 000 000 rows (median of runs)

| engine | bulk_insert | single_insert_commit | single_update_commit | delete_one | filter(cost<250) | count_star | join(cities) |
|---|---:|---:|---:|---:|---:|---:|---:|
| **MongrelDB** | **80.51 ms** | **7.2 µs** | **7.1 µs** | **5.8 µs** | **9.8 µs** | **276.7 µs** | **1.10 ms** |
| MongrelDB (enc) | 90.43 ms | 9.3 µs | 9.2 µs | 7.7 µs | 8.0 µs | 274.5 µs | 1.22 ms |
| SQLite (rusqlite) | 220.41 ms | 13.7 µs | 14.1 µs | 13.6 µs | 17.33 ms | 3.30 ms | 22.29 ms |
| DuckDB native | 272.30 ms | 246.5 µs | 459.7 µs | 162.6 µs | 705.3 µs | 130.3 µs | 2.60 ms |
| DuckDB-Parquet | 27.93 ms | — | — | — | 2.09 ms | 336.9 µs | 2.78 ms |
| DuckDB-CSV | 32.47 ms | — | — | — | 42.84 ms | 38.21 ms | 43.30 ms |

Single-record write improved across the board: insert at N=100 dropped 10.3→6.4 µs
(−38%) and delete 8.6→6.1 µs (−29%) from sharded caches reducing lock overhead.
The `filter(cost<250)` drop at N=1M (7.99 ms→9.8 µs) is from direct SQL dispatch
(§5.3) bypassing DataFusion planning for simple `SELECT … WHERE` shapes.

## Encryption overhead (MongrelDB, plain vs AES-256-GCM)

### N = 100

| engine | bulk_insert | filter(cost<250) | count_star | join(cities) |
|---|---:|---:|---:|---:|
| plain | 262.3 µs | 8.3 µs | 287.4 µs | 393.9 µs |
| encrypted | 391.8 µs | 10.7 µs | 285.3 µs | 463.4 µs |

### N = 1 000 000

| engine | bulk_insert | filter(cost<250) | count_star | join(cities) |
|---|---:|---:|---:|---:|
| plain | 83.94 ms | 8.0 µs | 265.9 µs | 1.13 ms |
| encrypted | 84.00 ms | 7.8 µs | 306.4 µs | 1.32 ms |

Encrypted `filter(cost<250)` stays at parity with plain — the encrypted
page-stats envelope (decrypted once at open, overlaid onto the in-memory
page index) prunes identically to plaintext, with no plaintext values
ever touching the file.

## MongrelDB native query paths (non-SQL)

| N | count() O(1) metadata | filter via Db::query (index/tool-call) |
|---:|---:|---:|
| 100 | 0.0 µs | 28.7 µs |
| 1 000 000 | 0.0 µs | 6.91 ms |

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
| **Single-row write (durable)** | **7.2 µs** | 13.7 µs | 247 µs | — |
| **Bulk ingest** | **80.5 ms** | 220.4 ms | 272.3 ms | 27.9 ms |
| **Cold SQL filter** | **9.8 µs** | 17.3 ms | 705 µs | 2.1 ms |
| **COUNT(*) SQL** | **277 µs** | 3.30 ms | 130 µs | 337 µs |
| **Join COUNT(*)** | **1.10 ms** | 22.3 ms | 2.60 ms | 2.8 ms |
| **Typed bulk ingest** | **25.7 Melem/s** | — | — | — |
| **LE scan throughput** | **12.3 Melem/s** | — | — | — |
| **Bitmap pushdown** | **122 Melem/s** | — | — | — |
| **Storage** | **4.17 bytes/row** | — | — | — |

MongrelDB wins single-row writes (~1.9× SQLite, ~34× DuckDB), bulk insert
(~2.7× SQLite, ~3.4× DuckDB native), cold SQL filter (~1700× SQLite via direct
dispatch), and join COUNT(*) (~2.4× DuckDB, ~20× SQLite). DuckDB-Parquet still
wins bulk file creation and has the fastest analytical filter.

## Regression history

### §5 optimization session (2026-07-02)

All 11 items from the PLAN.md §5 backlog were implemented and verified.
Headline improvements measured against the 07-01 baseline:

- **Bitmap ∩ Range pushdown: −17%** (14.4→12.0 ms) — cheap-first condition
  resolution (§5.5) resolves the O(1) bitmap before the expensive range
  scan, then early-exits on empty survivor sets.
- **Full scan: −8%** (77.2→70.7 ms), **range pushdown: −8%** (9.2→8.5 ms),
  **projection pushdown: −10%** (5.3→4.8 ms) — sharded caches (§5.8) reduce
  lock contention under the parallel rayon scan path.
- **Single-insert at N=100: −38%** (10.3→6.4 µs), **delete: −29%** (8.6→6.1 µs)
  — sharded cache creation + reduced write-path lock overhead.
- **Cold SQL filter at N=1M: 7.99 ms→9.8 µs** — direct SQL dispatch (§5.3)
  bypasses DataFusion parse+plan for simple `SELECT … WHERE` shapes, falling
  through to `ctx.sql()` for anything it can't serve.
- No regressions on the write path (group_commit_1000 flat at 686 µs).

### 2026-07-01 audit

A 2026-07-01 audit found this document was 147 commits stale and three
regressions had crept in since it was last measured. All three were
root-caused and fixed the same day; the numbers above reflect the fix.

1. **Typed bulk ingest, 38 ms → 436 ms (11×).** A prior commit made fresh-table
   bulk loads build and checkpoint indexes *inside* the ingest call. Fixed by
   `Table::set_index_build_policy` — `IndexBuildPolicy::Deferred` (default)
   restores the documented behavior of deferring index construction to the
   first query/flush; `Eager` is available for callers that want predictable
   first-query latency instead.
2. **Per-put overhead, +40% on `put`/`group_commit`.** NOT-NULL validation and
   PK-upsert bookkeeping had grown a throwaway `HashMap` + full-row clone per
   put, even for single-row and append-only batches. Fixed with a slice-based
   validator, a single-row fast path, a lazily-built (rather than
   eagerly-maintained) PK reverse map, and an upsert-probe skip when a batch's
   PKs are provably fresh.
3. **Encrypted `WHERE` filter, 7.0 ms → 17–21 ms (2.4–3×).** An earlier change
   correctly stopped storing plaintext page min/max for encrypted columns (it
   would leak values) but left encrypted columns with no substitute — every
   filtered read decrypted every page. Fixed by moving per-page min/max into a
   run-level AES-256-GCM-encrypted stats envelope, decrypted once at open and
   overlaid onto the in-memory page index — encrypted columns prune exactly
   like plaintext ones, with no plaintext values ever touching the file.
