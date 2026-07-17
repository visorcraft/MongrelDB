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

## Stage 1 qualification

Qualification evidence for the Stage 1 gate (spec §10 "Stage 1 gate"), in the
sense of §21: these are starting qualification targets from one machine, not
marketing promises. Collected 2026-07-17 from release builds on the machine
described above (Linux 7.2.0-rc3-1-cachyos-rc, database tempdirs on
/dev/nvme2n1p2 NVMe), branch `architecture_expansion`, working tree (not a
tagged commit). The one-million-row overlapping read/write gate, the
1,000-concurrent-session gate, and the warm loopback point-query p95 baseline
are covered; the remaining gate items are covered by the correctness suites
(crash/restart survival, isolation anomalies, cancellation, backup/restore,
PITR, AI qualification).

Overlapping read/write, 4 writers + 4 readers on one `Database` (100-row
commit batches; readers point-read a never-updated seeded half; zero errors
in both runs):

| Measurement | CI default (100,000 rows) | Gate scale (1,000,000 rows) |
|---|---:|---:|
| Commits | 500 | 5,000 |
| Commit p50 | 21.417 ms | 33.012 ms |
| Commit p99 | 141.078 ms | 176.885 ms |
| Commit p99 bound asserted | 500 ms | 500 ms |
| Overlapping point reads | 1,279,152 | 15,810,096 |
| Peak RSS (VmHWM) | 58,482,688 B | 506,540,032 B |
| Peak RSS bound asserted | 741,670,912 B | 2,584,870,912 B |
| Wall time (workload phase) | 4.459 s | 59.915 s |

The RSS bound is 512 MiB process base + 2 KiB/row, calibrated from the
one-million-row `read_generation` overlap peak (1,168,683,008 B, above) with
~1.8x per-row headroom; both runs finish far under it. Commit latency here is
group-commit fsync latency under four concurrent committers with readers
pinning snapshots, so it is not comparable to the single-committer "Commit
with fsync" figure above; the asserted 500 ms p99 bound (env-tunable, see
below) exists to catch stalls, not to state an SLO.

Warm embedded point query (deterministic 100,000-row dataset, warm cache,
10,000 queries, each a full begin/get/rollback round trip):

| Measurement | Result |
|---|---:|
| Point-query p50 | 1.037 µs |
| Point-query p95 | 1.434 µs |
| Point-query p99 | 1.933 µs |

Warm loopback HTTP point query (deterministic 10,000-row dataset seeded over
the HTTP API, warm read path, 1,000 sequential
`SELECT id FROM items WHERE id = ?` queries on one session, client-observed
full-round-trip latency; release mode on the Intel Core Ultra 9 386H
described above):

| Measurement | Result |
|---|---:|
| Point-query p50 | 1.287 ms |
| Point-query p95 | 2.070 ms |
| Point-query p99 | 2.396 ms |
| p95 tripwire asserted | 250 ms |

1,000 concurrent HTTP sessions against the daemon router on loopback (one
durable `BEGIN`/`INSERT`/`COMMIT` write plus three `SELECT 1` reads per
session; 6,000 statements; in-flight requests bounded at 256):

| Measurement | Result |
|---|---:|
| Sessions live at peak | 1,000 (store cap enforced: 1,001st open → 503) |
| Failed requests | 0 |
| Sessions after close | 0 |
| Wall time | 5.557 s |
| Peak RSS (VmHWM) | 259,309,568 B |
| RSS no-OOM tripwire asserted | 4,294,967,296 B |

Warm loopback HTTP point query against the daemon router (deterministic
10,000-row dataset seeded over the HTTP API, untimed warm pass, then 1,000
sequential `SELECT id FROM items WHERE id = ?` point queries on one session;
client-observed latency spans the full 127.0.0.1 round trip including the
response body; same release-mode 2026-07-17 machine as above):

| Measurement | Result |
|---|---:|
| Point-query p50 | 1.892 ms |
| Point-query p95 | 2.472 ms |
| Point-query p99 | 3.008 ms |
| p95 tripwire asserted | 250 ms |

Unlike the embedded figure above (in-process begin/get/rollback), this number
includes HTTP request handling, session lookup, SQL planning, and JSON
serialization on every query — it is the gate's "point query over a warm
local network" baseline.

Qualification runs are tests (not Criterion benches), release mode, with
scale knobs documented in the test headers:

```bash
cargo test -p mongreldb-core --test qualification --release -- --nocapture
MONGRELDB_QUAL_ROWS=1000000 \
  cargo test -p mongreldb-core --test qualification --release -- --nocapture
(cd crates/mongreldb-server && \
  cargo test --release --test scale_test -- --nocapture)
```

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
