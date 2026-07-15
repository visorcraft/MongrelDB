# MongrelDB benchmarks

Latest available local measurements, collected 2026-07-14 and 2026-07-15 from
release builds on Linux x86-64 with an Intel Core Ultra 9 386H, 62 GiB RAM, and
rustc 1.96.1. Times are Criterion central estimates unless marked as medians.
These are engineering measurements from one machine, not cross-machine product
guarantees.

## One-million-row scale

| Operation | Time | Throughput |
|---|---:|---:|
| Batch ingest plus commit | 854.47 ms | 1.1703 M rows/s |
| Batch ingest plus flush | 2.0855 s | 479.51 K rows/s |
| Bulk load | 110.65 ms | 9.0373 M rows/s |
| Typed bulk load | 58.471 ms | 17.102 M rows/s |
| Fast bulk load | 88.120 ms | 11.348 M rows/s |
| Generic full scan | 254.17 ms | 3.9343 M rows/s |
| Typed full scan | 83.707 ms | 11.946 M rows/s |

## One-million-row queries

| Operation | Time |
|---|---:|
| Full scan, all columns | 67.807 ms |
| Bitmap equality | 8.0387 ms |
| Integer range | 8.8231 ms |
| Bitmap and range intersection | 14.224 ms |
| One-column bitmap projection | 6.5335 ms |
| Primary-key lookup | 7.5735 ms |
| Count 50,000 survivors | 2.1712 µs |
| Dirty-table bitmap equality | 29.751 ms |
| Multi-run bitmap equality, native | 90.599 ms |
| Multi-run bitmap equality, cursor | 103.48 ms |

## Writes

| Operation | Time | Throughput |
|---|---:|---:|
| Put without fsync | 4.4828 µs | |
| Commit with fsync | 4.6721 ms | |
| 1,000 puts plus commit | 7.7071 ms | 129.75 K rows/s |
| Durable update on a 100-row flushed table | 4.2844 ms | |
| Update without fsync on a 100-row flushed table | 10.362 µs | |
| Authenticated 10,000-row batch | 15 ms | 0 security catalog disk reads |

## Read generations and paging

The creation result is the median of ten independent Criterion processes.
The overlap result is the median of five runs holding 32 generations while
committing 100 writes against a one-million-row table.

| Measurement | Result |
|---|---:|
| Read-generation creation | 1.46335 µs |
| Overlap commit p50 | 4.215 ms |
| Overlap commit p95 | 8.127 ms |
| Overlap commit p99 | 8.826 ms |
| Overlap peak RSS | 1,168,683,008 bytes |
| Whole-table copy-on-write clones | 0 |
| Maximum live generations | 32 |
| Live generations after cursor drop | 0 |
| MongrelDB Kit 10,050-row paging test | 182 ms |
| MongrelDB Kit TypeScript suite | 8.99 s, 308 passed |

## SQL cancellation

| Measurement | Result |
|---|---:|
| Controlled point query | 2.1795 to 2.2743 µs |
| Controlled 100k scan | 27.314 to 27.662 ms |
| Accepted cancellation to scan completion | 85.665 to 93.687 µs |
| Accepted cancellation to queued completion | 3.9783 to 4.0002 µs |

Criterion reported no statistically significant regression for these four
cancellation measurements.

## Commands

```bash
cargo bench -p mongreldb-core --bench read_generation -- --noplot
cargo bench -p mongreldb-core --bench write_path -- --noplot
cargo bench -p mongreldb-core --bench scale -- --noplot
cargo bench -p mongreldb-core --bench filtered_query -- --noplot
cargo bench -p mongreldb-core --bench path_matrix -- --noplot
cargo bench -p mongreldb-core --bench trips -- --noplot
cargo bench --manifest-path crates/mongreldb-query/Cargo.toml \
  --bench sql_cancellation
```

AI retrieval has a separate reproducible harness and enforced thresholds in
[`docs/ai/benchmark-methodology.md`](docs/ai/benchmark-methodology.md).
