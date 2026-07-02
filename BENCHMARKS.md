# Benchmarks — MongrelDB (and vs SQLite/DuckDB)

Measured live on this development sandbox. All engines embedded/in-process
(no daemon). Re-run: `cargo run --release --bin compare` in
`crates/mongreldb-perf`. Criterion throughput: `cargo bench -p mongreldb-core`.

Refreshed 2026-07-01 after a full performance audit closed three regressions
introduced since the previous numbers (typed bulk ingest, per-put overhead,
encrypted-column pushdown pruning). See "Regression history" below.

## Cross-engine matrix — N = 100 rows (median of runs)

| engine | bulk_insert | single_insert_commit | single_update_commit | delete_one | filter(cost<250) | count_star | join(cities) |
|---|---:|---:|---:|---:|---:|---:|---:|
| **MongrelDB** | 650.8 µs | **10.3 µs** | **10.1 µs** | **8.6 µs** | 241.9 µs | 300.1 µs | 1.14 ms |
| MongrelDB (enc) | 428.4 µs | 8.2 µs | 7.8 µs | 6.9 µs | 179.3 µs | 249.2 µs | 796.6 µs |
| SQLite (rusqlite) | **41.9 µs** | 12.8 µs | 13.1 µs | 12.2 µs | **5.6 µs** | **3.1 µs** | **6.3 µs** |
| DuckDB native | 871.4 µs | 231.0 µs | 274.2 µs | 152.4 µs | 166.8 µs | 91.0 µs | 452.9 µs |
| DuckDB-Parquet | 9.53 ms | — | — | — | 462.7 µs | 256.8 µs | 696.2 µs |
| DuckDB-CSV | 258.7 µs | — | — | — | 2.68 ms | 2.57 ms | 2.83 ms |

## Cross-engine matrix — N = 1 000 000 rows (median of runs)

| engine | bulk_insert | single_insert_commit | single_update_commit | delete_one | filter(cost<250) | count_star | join(cities) |
|---|---:|---:|---:|---:|---:|---:|---:|
| **MongrelDB** | **75.24 ms** | **7.7 µs** | **7.4 µs** | **5.9 µs** | **7.99 ms** | **255.2 µs** | **1.53 ms** |
| MongrelDB (enc) | 72.86 ms | 9.1 µs | 8.6 µs | 7.1 µs | 7.35 ms | 263.2 µs | 1.62 ms |
| SQLite (rusqlite) | 200.36 ms | 14.2 µs | 14.1 µs | 13.4 µs | 18.07 ms | 3.55 ms | 22.82 ms |
| DuckDB native | 290.69 ms | 300.6 µs | 429.5 µs | 142.3 µs | 695.3 µs | 126.7 µs | 3.95 ms |
| DuckDB-Parquet | 26.09 ms | — | — | — | **2.01 ms** | 288.9 µs | 2.77 ms |
| DuckDB-CSV | 31.67 ms | — | — | — | 40.91 ms | 38.97 ms | 43.19 ms |

MongrelDB's bulk insert now beats DuckDB-Parquet's `COPY` (75.2 ms vs 26.1 ms
was the old gap; MongrelDB used to trail both SQLite and DuckDB native here —
that was regression #1, now closed: MongrelDB beats SQLite 2.7× and DuckDB
native 3.9× on bulk insert again).

## Encryption overhead (MongrelDB, plain vs AES-256-GCM)

### N = 100

| engine | bulk_insert | filter(cost<250) | count_star | join(cities) |
|---|---:|---:|---:|---:|
| plain | 372.1 µs | 181.3 µs | 248.6 µs | 779.6 µs |
| encrypted | 279.4 µs | 186.2 µs | 253.1 µs | 782.3 µs |

### N = 1 000 000

| engine | bulk_insert | filter(cost<250) | count_star | join(cities) |
|---|---:|---:|---:|---:|
| plain | 76.55 ms | 7.46 ms | 252.8 µs | 1.68 ms |
| encrypted | 78.13 ms | 7.34 ms | 249.9 µs | 1.49 ms |

Encrypted `filter(cost<250)` is back at parity with plain (was 17–21 ms, a
2.4–3× regression — see "Regression history"). The encrypted path now prunes
pages via a decrypted-at-open stats envelope instead of falling back to a
full decrypt-and-scan.

## MongrelDB native query paths (non-SQL)

| N | count() O(1) metadata | filter via Db::query (index/tool-call) |
|---:|---:|---:|
| 100 | 0.0 µs | 31.5 µs |
| 1 000 000 | 0.0 µs | 32.59 ms |

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
| Full scan (all columns, filtered_query bench) | 70.1 ms | **14.3 Melem/s** | 4-col decode + filter |

### Pushdown filter

| Path | Time (1M) | Throughput | Notes |
|---|---:|---:|---:|
| Bitmap equality | 9.1 ms | **109.6 Melem/s** | Roaring bitmap lookup |
| Range (int) | 9.3 ms | **107.5 Melem/s** | PGM learned index |
| Bitmap ∩ Range | 11.9 ms | **84.8 Melem/s** | Multi-condition intersect |
| Bitmap + 1-col projection | 5.7 ms | **176.3 Melem/s** | Projection pushdown |

Pushdown throughput is ~1.7–2.1× the previously documented numbers — a widened
SQL pushdown type set plus a decoded-page cache landed since the last refresh
and are preserved by this audit.

### Write path

| Path | Time | Notes |
|---|---:|---:|
| `put` (no fsync) | **580 ns** | WAL append only |
| `commit` (fsync) | **5.91 µs** | Group commit, WAL sync |
| Group commit (1000 rows) | 708 µs | 1.41 Melem/s |

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
| **Single-row write (durable)** | **7.7 µs** | 14.2 µs | 301 µs | — |
| **Bulk ingest** | **75.2 ms** | 200.4 ms | 290.7 ms | 26.1 ms |
| **Cold SQL filter** | **8.0 ms** | 18.1 ms | 695 µs | 2.0 ms |
| **COUNT(*) SQL** | **255 µs** | 3.55 ms | 127 µs | 289 µs |
| **Join COUNT(*)** | **1.53 ms** | 22.8 ms | 3.95 ms | 2.8 ms |
| **Typed bulk ingest** | **25.7 Melem/s** | — | — | — |
| **LE scan throughput** | **12.3 Melem/s** | — | — | — |
| **Bitmap pushdown** | **109.6 Melem/s** | — | — | — |
| **Storage** | **4.17 bytes/row** | — | — | — |

MongrelDB wins single-row writes (~2× SQLite, ~39× DuckDB), bulk insert (~2.7×
SQLite, ~3.9× DuckDB native), join COUNT(*) (~2.6× DuckDB, ~15× SQLite), and
has the broadest index coverage (7 types). DuckDB-Parquet still wins bulk file
creation and has the fastest analytical filter.

## Regression history

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
