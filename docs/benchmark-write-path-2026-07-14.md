# Core write-path and read-generation baseline, 2026-07-14

Benchmark code: `0e422f7463afd1c345dfead8efd62d6c0b5f7c70`

Environment:

- Linux 7.2.0-rc3-1-cachyos-rc, x86-64
- Intel Core Ultra 9 386H, 16 cores
- 62 GiB RAM
- rustc 1.96.1
- release profile, default features
- Criterion 0.5, sequential benchmark processes

Run commands:

```bash
cargo bench -p mongreldb-core --bench read_generation create -- --noplot
cargo bench -p mongreldb-core --bench read_generation -- --noplot
cargo bench -p mongreldb-core --bench write_path -- --noplot
cargo bench -p mongreldb-core --bench scale -- --noplot
cargo bench -p mongreldb-core --bench filtered_query -- --noplot
cargo bench -p mongreldb-core --bench path_matrix -- --noplot
cargo bench -p mongreldb-core --bench trips -- --noplot
```

## Tracked comparisons

| Benchmark | Earlier result | Current result | Change |
|---|---:|---:|---:|
| 10,050-row Kit paging test, 0.53.2 | 29.286 s | 183 ms warm median | 160x faster |
| 10,050-row Kit paging test, regressed 0.53.3 | 31.965 to 32.222 s | 183 ms warm median | at least 174x faster |
| Packed delete, isolated | about 2.9 s | 106 ms warm median | 27x faster |
| Full TypeScript suite | 39.53 to 40.81 s, 2 failures | 10.45 s, 300 passed | 3.8x faster |
| 1M read-generation creation, first post-P5 run | 1.5043 us | 1.45410 us, 10-run median | 3.3% faster |
| 1M live-generation writer | 105.61 ms | 85.973 ms | no statistically significant change |
| 1M benchmark peak RSS | 588820 kB | 589208 kB | 0.07% higher |
| Whole-table COW clones | 0 | 0 | unchanged |

The 1M benchmark was added in P5. No pre-P1 measurement exists for it. The
earlier 1M values above are from the first post-P5 run.

## 1M read-generation creation, 10 runs

Each value is Criterion's central estimate from one independent process. The
tracked number is the median of those ten estimates.

| Run | Estimate |
|---:|---:|
| 1 | 1.4701 us |
| 2 | 1.4585 us |
| 3 | 1.4464 us |
| 4 | 1.4695 us |
| 5 | 1.4471 us |
| 6 | 1.3587 us |
| 7 | 1.4585 us |
| 8 | 1.4507 us |
| 9 | 1.4465 us |
| 10 | 1.4575 us |
| **Median** | **1.45410 us** |

A preceding single rerun measured 1.6246 us. The 10-run median is 10.5% lower,
so 1.6246 us is not used as the tracked baseline.

Read-generation creation publishes and counts an immutable generation. It does
not scan all one million rows.

## Current 1M scale baseline

| Operation | Central estimate | Throughput |
|---|---:|---:|
| Batch ingest plus commit | 854.47 ms | 1.1703 M rows/s |
| Batch ingest plus flush | 2.0855 s | 479.51 K rows/s |
| Bulk load | 110.65 ms | 9.0373 M rows/s |
| Typed bulk load | 58.471 ms | 17.102 M rows/s |
| Fast bulk load | 88.120 ms | 11.348 M rows/s |
| Generic full scan | 254.17 ms | 3.9343 M rows/s |
| Typed full scan | 83.707 ms | 11.946 M rows/s |

## Current 1M query baseline

| Operation | Central estimate |
|---|---:|
| Full scan, all columns | 67.807 ms |
| Bitmap equality | 8.0387 ms |
| Integer range | 8.8231 ms |
| Bitmap and range intersection | 14.224 ms |
| One-column bitmap projection | 6.5335 ms |
| Primary-key lookup | 7.5735 ms |
| Count 50,000 survivors | 2.1712 us |
| Dirty-table bitmap equality | 29.751 ms |
| Multi-run bitmap equality, native | 90.599 ms |
| Multi-run bitmap equality, cursor | 103.48 ms |

## Current write baseline

| Operation | Central estimate | Throughput |
|---|---:|---:|
| Put without fsync | 4.4828 us | |
| Commit with fsync | 4.6721 ms | |
| 1,000 puts plus commit | 7.7071 ms | 129.75 K rows/s |
| Single durable update on a 100-row flushed table | 4.2844 ms | |
| Single update without fsync on a 100-row flushed table | 10.362 us | |

No earlier recorded values exist for the scale, query, or write microbenchmarks.
These measurements establish their comparison baseline.

## Bounded 1M cursor-generation overlap

`read_generation_characterization` held 32 successive immutable read
generations while committing 100 single-row writes against a 1,000,000-row
table. The enforced thresholds are in `docs/read-generation-thresholds.json`.

| Metric | Previous | Current | Threshold |
|---|---:|---:|---:|
| Commit p50 | 4.057 ms | 4.076 ms | |
| Commit p95 | 8.394 ms | 8.840 ms | |
| Commit p99 | 9.081 ms | 9.259 ms | 500 ms maximum |
| Peak RSS | 1,168,375,808 bytes | 1,169,690,624 bytes | 1,610,612,736 bytes maximum |
| Whole-table COW clones | 0 | 0 | 0 |
| Estimated COW clone bytes | 0 | 0 | 0 |
| Maximum live generations | 32 | 32 | 32 |
| Live generations after cursor drop | 0 | 0 | 0 |

The nightly 1M workflow runs the characterization and
`scripts/validate-read-generation-characterization.py`. This makes commit p99,
peak RSS, clone amplification, and bounded cursor lifetime release artifacts
instead of advisory local numbers.

## Authenticated 10k-row batch

The catalog-bound write characterization runs with an unchanged process-wide
security version:

```bash
cargo run -p mongreldb-core --release --example authenticated_batch_bench
```

| Metric | Current |
|---|---:|
| Rows committed | 10,000 |
| Commit latency | 16 ms |
| Security catalog disk reads | 0 |

The counter excludes the initial database open. A security-version change
causes exactly one reload on the stale handle; unchanged authenticated commits
reuse the in-memory catalog.
