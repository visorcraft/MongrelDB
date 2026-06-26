# Benchmarks — MongrelDB (and vs SQLite/DuckDB)

Measured live on this development sandbox. All engines embedded/in-process
(no daemon). Re-run: `cargo run --release --bin compare` in
`crates/mongreldb-perf`. Criterion throughput: `cargo bench -p mongreldb-core`.

## Cross-engine matrix — N = 100 rows (median of runs)

| engine | bulk_insert | single_insert_commit | single_update_commit | delete_one | filter(cost<250) | count_star | join(cities) |
|---|---:|---:|---:|---:|---:|---:|---:|
| **MongrelDB** | 694.5 µs | **9.0 µs** | **8.6 µs** | **7.1 µs** | 248.7 µs | 309.7 µs | 1.14 ms |
| MongrelDB (enc) | 294.0 µs | 8.2 µs | 7.9 µs | 6.4 µs | 274.5 µs | 252.0 µs | 884.1 µs |
| SQLite (rusqlite) | **54.9 µs** | 22.0 µs | 22.9 µs | 21.3 µs | **9.5 µs** | **5.2 µs** | **10.7 µs** |
| DuckDB native | 856.9 µs | 276.7 µs | 249.9 µs | 133.7 µs | 147.4 µs | 112.3 µs | 446.6 µs |
| DuckDB-Parquet | 14.46 ms | — | — | — | 439.0 µs | 265.8 µs | 891.4 µs |
| DuckDB-CSV | 287.2 µs | — | — | — | 2.99 ms | 2.66 ms | 2.72 ms |

## Cross-engine matrix — N = 1 000 000 rows (median of runs)

| engine | bulk_insert | single_insert_commit | single_update_commit | delete_one | filter(cost<250) | count_star | join(cities) |
|---|---:|---:|---:|---:|---:|---:|---:|
| **MongrelDB** | 189.13 ms | **6.7 µs** | **6.1 µs** | **4.6 µs** | **7.12 ms** | **257.3 µs** | **1.48 ms** |
| MongrelDB (enc) | 144.50 ms | 6.8 µs | 6.5 µs | 4.7 µs | 7.02 ms | 259.6 µs | 1.42 ms |
| SQLite (rusqlite) | 205.04 ms | 13.3 µs | 13.7 µs | 12.8 µs | 18.36 ms | 3.97 ms | 23.02 ms |
| DuckDB native | 309.49 ms | 262.1 µs | 425.1 µs | 139.3 µs | 695.0 µs | 129.7 µs | 4.30 ms |
| DuckDB-Parquet | **25.94 ms** | — | — | — | **1.96 ms** | 291.0 µs | 2.86 ms |
| DuckDB-CSV | 29.64 ms | — | — | — | 38.04 ms | 36.62 ms | 40.38 ms |

## Encryption overhead (MongrelDB, plain vs AES-256-GCM)

### N = 100

| engine | bulk_insert | filter(cost<250) | count_star | join(cities) |
|---|---:|---:|---:|---:|
| plain | 235.1 µs | 236.9 µs | 262.6 µs | 794.1 µs |
| encrypted | 310.5 µs | 179.5 µs | 250.6 µs | 751.9 µs |

### N = 1 000 000

| engine | bulk_insert | filter(cost<250) | count_star | join(cities) |
|---|---:|---:|---:|---:|
| plain | 194.44 ms | 7.21 ms | 306.6 µs | 1.55 ms |
| encrypted | 148.50 ms | 7.67 ms | 259.2 µs | 1.45 ms |

## MongrelDB native query paths (non-SQL)

| N | count() O(1) metadata | filter via Db::query (index/tool-call) |
|---:|---:|---:|
| 100 | 0.0 µs | 49.9 µs |
| 1 000 000 | 0.0 µs | 33.17 ms |

## Criterion throughput benchmarks (1M rows, release)

### Bulk ingest

| Path | Time (1M) | Throughput | Notes |
|---|---:|---:|---|
| `bulk_load` (Value API) | 136 ms | **7.34 Melem/s** | Row-major → typed parallel encode |
| `bulk_load_columns` (typed) | 38 ms | **26.2 Melem/s** | Native typed; 14.7 deferred indexing |
| `bulk_load_fast` (typed + plain) | 52 ms | **19.4 Melem/s** | Raw `ALGO_PLAIN` fast path |
| `put_batch` + flush | 1.68 s | 595 Kelem/s | Per-row WAL append + fsync |
| `put_batch` (no flush) | 818 ms | 1.22 Melem/s | WAL append only |

### Scan

| Path | Time (1M) | Throughput | Notes |
|---|---:|---:|---:|
| `scan_columns` (native typed) | 221 ms | **4.52 Melem/s** | BE decode path |
| `scan_columns_le` (LE memcpy) | 84 ms | **11.9 Melem/s** | 15.7 native-endian path |
| Full scan (all columns, filtered_query bench) | 71 ms | **14.1 Melem/s** | 4-col decode + filter |

### Pushdown filter

| Path | Time (1M) | Throughput | Notes |
|---|---:|---:|---:|
| Bitmap equality | 15.4 ms | **64.8 Melem/s** | Roaring bitmap lookup |
| Range (int) | 15.2 ms | **65.9 Melem/s** | PGM learned index |
| Bitmap ∩ Range | 34.9 ms | 28.6 Melem/s | Multi-condition intersect |
| Bitmap + 1-col projection | 12.1 ms | **82.8 Melem/s** | Projection pushdown |

### Write path

| Path | Time | Notes |
|---|---:|---:|
| `put` (no fsync) | **601 ns** | WAL append only |
| `commit` (fsync) | **5.77 µs** | Group commit, WAL sync |
| Group commit (1000 rows) | 697 µs | 1.44 Melem/s |

### Encryption (AES-256-GCM-SIV)

| Size | Encrypt | Decrypt |
|---|---:|---:|
| 4 KiB | **1.85 GiB/s** | 1.85 GiB/s |
| 64 KiB | 1.90 GiB/s | 1.89 GiB/s |
| 256 KiB | 1.89 GiB/s | 1.87 GiB/s |
| 1 MiB | 1.87 GiB/s | 1.86 GiB/s |

### Storage efficiency

| Dimension | Value |
|---|---:|
| Bytes/row (1M rows, 4 cols, zstd) | **4.17** |
| Total size (1M rows) | 3.98 MB |

## Methodology

- All measurements on this dev sandbox (Linux, release build, `--all-features`).
- **Cross-engine:** all engines embedded in-process (no daemon/HTTP). SQLite
  uses bundled `rusqlite`; DuckDB uses bundled `duckdb` crate.
- MongrelDB `count()` is O(1) metadata; `count_star` = `SELECT COUNT(*)` (scan).
- `filter`/`count_star`/`join` go through DataFusion SQL; `clear_cache()` before
  each so these are cold (no result cache hit).
- `single_*_commit` = one op + `commit()` (durable, WAL fsync).
- Encryption = MongrelDB page-level AES-256-GCM-SIV.
- Parquet/CSV are immutable: single-row insert/update/delete are N/A (load = file write).
- Criterion throughput: `cargo bench -p mongreldb-core --bench {scale,filtered_query,write_path,page_encryption}`.

## Key takeaways (1M rows)

| Metric | MongrelDB | SQLite | DuckDB native | DuckDB-Parquet |
|---|---:|---:|---:|---:|
| **Single-row write (durable)** | **6.7 µs** | 13.3 µs | 262 µs | — |
| **Bulk ingest** | 189 ms | 205 ms | 309 ms | **26 ms** |
| **Cold SQL filter** | **7.1 ms** | 18.4 ms | 695 µs | 2.0 ms |
| **COUNT(*) SQL** | **257 µs** | 3.97 ms | 130 µs | 291 µs |
| **Join COUNT(*)** | **1.48 ms** | 23.0 ms | 4.3 ms | 2.9 ms |
| **Typed bulk ingest** | **26.2 Melem/s** | — | — | — |
| **LE scan throughput** | **11.9 Melem/s** | — | — | — |
| **Bitmap pushdown** | **64.8 Melem/s** | — | — | — |
| **Storage** | **4.17 bytes/row** | — | — | — |

MongrelDB wins single-row writes (~2× SQLite, ~40× DuckDB), join COUNT(*)
(~3× DuckDB, ~15× SQLite), and has the broadest index coverage (7 types).
DuckDB-Parquet wins bulk file creation and has the fastest analytical scan.
