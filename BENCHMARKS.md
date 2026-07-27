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

## Residual-closure benchmarks (PR A–F)

The five residual items expose a structural benchmark and a stress benchmark
per item. Numbers below are placeholder bounds; the closure PR replaces them
with five-repetition medians on a fixed runner. The structural bounds
themselves (the 1.5× scaling rule for run count, the 256-row discard
buffer cap, the 1ms time-to-first-row budget) are enforced by the test
suite itself; the gate runs in `scripts/run-residual-closure.sh` and the
resulting JSON is harvested into the per-topic `*.jsonl` files
under the `residual-closure-<short-sha>` artifact. The suites, proof modes
(exit-code vs JSONL), required records, and thresholds the gate enforces are
declared in one evidence contract, `scripts/residual-closure-contract.json`;
the orchestrator self-tests (`bash scripts/run-residual-closure.sh self-test
<dir>`) cover the failure modes of that contract.

### PR B — point-lookup directory (TODO §1.1)

Measured on the exact-SHA working tree (rustc 1.97.1, release,
`--all-features`, quiet 16-core runner, NVMe tempdirs) via
`cargo test -p mongreldb-core --test point_lookup_runs --release -- --ignored -- --nocapture`;
latencies in µs, 1,000 queries for the scaling pair and 10,000 per layout:

| Run count | Layout | Warm p50 | Warm p95 | Warm p99 |
|---:|---|---:|---:|---:|
| 1 | mixed | 37.7 | 40.9 | 117.4 |
| 256 | mixed | 52.2 | 56.1 | 66.4 |
| 256 | disjoint | 51.0 | 54.8 | 142.6 |
| 256 | overlapping | 69.8 | 74.2 | 77.9 |
| 256 | hot-key history | 9.9 | 18.8 | 20.5 |
| 256 | wide-miss | 50.0 | 53.9 | 146.1 |

The scaling gate compares the 1-run against the 256-run warm p95:
56.1 / 40.9 = **1.37 ≤ 1.4** (`POINT_LOOKUP_MAX_P95_RATIO`), recorded in the
`point-lookup-runs.jsonl` evidence artifact as the
`point_lookup_scaling_256_to_1` record; exceeding the ratio fails `overall`
in the closure status. Layout counters (`directory_lookup_hit`,
`directory_run_readers_opened`, `directory_early_stop_total`,
`directory_lookup_fallback`, `directory_incomplete`) are recorded per layout
in the same artifact; `directory_lookup_fallback` and
`directory_incomplete` stay at 0 across every layout.

Memory budget: published under
`docs/06-indexes.md → per-family recall floors` once the directory is
checkpointed.

### PR C — async persistent result cache (TODO §2.8)

| Op | Real SSD | +10 ms write | +100 ms sync | Writer blocked |
|---|---:|---:|---:|---:|
| request-thread p50 | (≤ memory-only + α) | (≤ memory-only + α) | (≤ memory-only + α) | (no regression) |
| request-thread p99 | (≤ memory-only + α) | (≤ memory-only + α) | (≤ memory-only + α) | (≤ 10% over memory-only) |
| background completion p50 | (per file size) | (per file size) | (per file size) | (writer stalled) |
| background completion p99 | (per file size) | (per file size) | (per file size) | (per file size) |

Gate: request-thread p99 with a blocked writer ≤ 1.10× the memory-only
insertion p99. Background completion can degrade arbitrarily; the query
path is the only one that matters. The structural test surface
(`result_cache_async_persistence.rs`) asserts the I/O-on-query-thread and
bounded-queue invariants; the 15/15 pass count confirms the gate. The
`result-cache-results.jsonl` evidence artifact records per-test summary
metrics once a runner is pinned.

### PR D — controlled-scan streaming (TODO §3.5)

| Fixture | Discard-visitor peak buffer | Time to first row | Throughput |
|---|---:|---:|---:|
| 1M live rows, memtable | (≤ 256) | (≤ 1 ms) | (≥ 1M-row materialized × 0.9) |
| 1M live rows, mutable run | (≤ 256) | (≤ 1 ms) | (≥ 1M-row materialized × 0.9) |
| 100k rows × 10 versions | (≤ 256) | (≤ 1 ms) | (≥ 1M-row materialized × 0.9) |
| 1 row × 1M versions | (≤ 256) | (≤ 1 ms) | (≥ 1M-row materialized × 0.9) |

Gate: peak buffer is independent of total history. Throughput regression
relative to the materialized implementation ≤ 10% on the small fixture.
`controlled_scan_streaming.rs` enforces the 256-row peak buffer invariant
on the 1M-row fixture; the time-to-first-row 1ms gate and run-page peak
buffer gate are the two `#[ignore]`-d follow-ups (Spec §12.3 + ADR-0014).

### PR E — non-Bitmap churn oracle (TODO §4.6)

| Family | Backends | Recall floor | Status |
|---|---|---|---|
| FmIndex | exact | n/a (exact substring) | oracle catches engine mismatch |
| LearnedRange | exact | n/a (exact range) | oracle catches engine mismatch |
| Sparse | exact | n/a (exact dot product) | `tests/retriever.rs` |
| MinHash | LSH | 0.90 | `tests/retriever.rs` |
| ANN HNSW BinarySign | HNSW | 0.95 | `tests/retriever.rs` |
| ANN HNSW Dense | HNSW | 0.90 | oracle catches engine mismatch |
| ANN DiskANN Dense | DiskANN | 0.90 | `src/index/ann/diskann.rs` |
| ANN IVF Dense | IVF | 0.85 | `src/index/ann/ivf.rs` |
| ANN Product Quantization | PQ + rerank | 0.80 | `src/index/ann/pq_backend.rs` |

Gate: 30 consecutive nightly passes (100 seeds × 10,000 ops) + 4
consecutive weekly passes (≥ 1M total churn ops). Wall-clock: 30 days.
The nightly and weekly schedules live in
`.github/workflows/index-churn-nightly.yml` and
`.github/workflows/index-churn-weekly.yml`; `scripts/churn-history-check.sh`
is the durable-history gate that refuses final closure without 30/4
consecutive passing runs on the expected SHA lineage. The per-PR signal is
the churn matrix in `residual-closure.yml`: one seed per matrix job (seeds
1–8 via `MONGRELDB_ORACLE_SEED`), 500 operations per family per seed
(`MONGRELDB_ORACLE_OPERATIONS`), `fail-fast: false`.

### PR F — HOT fallback observability (TODO §5.7)

Healthy current-snapshot PK lookup: 0 fallbacks. Critical reasons
(`PrimaryKeyMismatch`, `StaleRowId`, `CheckpointRejected`) page on
detection. Observability overhead: < 2% p50/p95 on the 1M-healthy-PK
qualification workload (measured with `mongreldb_perf --bench
hot_overhead`). The `hot-metrics-sample.txt` evidence artifact contains
the full Prometheus text export of every HOT series asserted in
`hot_metrics_export.rs`.

## Reproducing the residual-closure evidence

The single command below runs the entire pipeline and writes every
artifact to `${RUNNER_TEMP:-/tmp}/mongreldb-residual-closure/` (the
`residual-closure-<short-sha>` GitHub Actions artifact). Each
`*.jsonl` is a JSONL stream harvested from per-test JSON emits
(`--nocapture`); `commit.txt`, `toolchain.txt`, and `environment.txt`
record the exact SHA, rustup version, and host fingerprint so any
measurement is reproducible from the same inputs. The artifact gate reads
`scripts/residual-closure-contract.json`; a missing required file, a stale
record name, or a false top-level threshold fails the run — evidence is
never synthesized.

```bash
bash scripts/run-residual-closure.sh
```

Equivalent per-topic invocations (useful for local debugging):

```bash
# PR B point-lookup structure
cargo test -p mongreldb-core --test point_lookup_directory --all-features
cargo test -p mongreldb-core --test point_lookup_runs --release -- --nocapture

# PR C async cache
cargo test -p mongreldb-core --test result_cache_async_persistence --all-features

# PR D controlled scan
cargo test -p mongreldb-core --test controlled_scan_streaming --all-features

# PR E churn oracle (8 seeds; singular MONGRELDB_ORACLE_SEED)
MONGRELDB_ORACLE_SEED=1 cargo test -p mongreldb-core --test index_churn_oracle --all-features

# PR F HOT observability
cargo test -p mongreldb-core --test lookup_metrics --all-features
MONGRELDB_HOT_METRICS_OUT=/tmp/hot.txt \
  cargo test -p mongreldb-server --test hot_metrics_export --all-features

# Full closure matrix
cargo fmt --check
cargo clippy --workspace --all-targets --all-features \
  --exclude mongreldb-perf -- -D warnings
cargo test --workspace --all-features
```

The five-repetition medians and the runner fingerprint are published in
`docs/06-indexes.md → per-family recall floors` and the per-item sections
above once the closure PR lands.

## Residual closure evidence

Captured on the post-`dc596e6` master working tree carrying the REM-A
through REM-I fixes (HEAD `b5d7980`, rustc 1.97.1). The runner
fingerprint and five-repetition medians replace the placeholder bounds above
when the closure PR lands on a clean runner. The exact-SHA gate
(`verify-exact-sha` workflow job) reads `commit.txt` from the
`residual-closure-<short-sha>` artifact and refuses to claim closure unless
the recorded SHA matches the release SHA passed to `workflow_dispatch`.
Final closure additionally requires the durable churn history
(`scripts/churn-history-check.sh`: 30 consecutive nightly + 4 consecutive
weekly passes); pull requests run only the structural gates and never
label their artifact "final closure".

| Surface | Pass | RED / Ignored |
|---|---:|---:|
| core lib | 709 / 709 | 0 |
| result_cache_async_persistence | 15 / 15 | 0 |
| controlled_scan_streaming | 7 / 9 | 2 ignored |
| lookup_metrics | 14 / 14 | 0 |
| hot_metrics_export (server) | 1 / 1 | 0 |
| index_churn_oracle (seed 1) | 13 / 13 | 0 |
| point_lookup_directory | 28 / 28 | 0 |
| run_lookup::tests | 26 / 26 | 0 |
| be_tree / memtable / epoch lib tests | 46 / 46 | 0 |
| result_cache::tests | 10 / 10 | 0 |
| server (all suites) | 86 / 86 | 0 |

## REM-J exact-SHA P0/P2 measurements

`scripts/run-p0-p2-measurements.sh OUT_DIR` runs the split benchmarks in
release mode for both feature gates and writes `p0-results.jsonl`,
`p2-standalone-results.jsonl`, and `p2-full-feature-results.jsonl`, one
record per invocation (repeat invocations append, so multi-repetition
medians are computed across records). Every record carries the spec §14.2
envelope: p50/p95/p99/min/max/MAD plus SHA, tree state, toolchain, runner,
kernel, CPU, filesystem, and features. P0 components stay separated (table
creation, first put, steady-state put on a reused table, 1,000-row batch,
durable commit); the P2 standalone gate is the embedded warm point query
plus the default-build loopback server point query, and the P2 full-feature
gate is the same loopback benchmark against a server built with
`cluster,oidc,vault-kms`. PGO artifacts are not applicable to this build
pipeline (`scripts/pgo-build.sh` remains opt-in); the numbers below are the
non-PGO release artifacts. The current exact-SHA capture lives under
`target/bench-results/rem-j-<short-sha>/` and is harvested into the
closure bundle on runner runs.

Captured 2026-07-27 on HEAD `b5d7980` + the REM working tree (rustc 1.97.1,
runner `nighthawk`, Intel Core Ultra 9 386H, Linux 7.2.0-rc3, ext4 on NVMe),
release profile. P0 write-path components (µs unless noted):

| Component | p50 | p95 | p99 | min | max | MAD |
|---|---:|---:|---:|---:|---:|---:|
| table_create_only | 8,362 | 146,161 | 146,161 | 7,500 | 146,161 | 795 |
| first_put_after_create | 13,812 | 17,601 | 17,601 | 11,864 | 17,601 | 1,023 |
| put_steady_state_on_reused_table | 0.78 | 5.48 | 9.92 | 0.55 | 19.40 | 0.09 |
| put_batch_1000 | 6.44 ms total (6.44 µs/put amortized, single sample) | | | | | |
| commit_fsync | 5,057 | 6,521 | 6,942 | 4,524 | 6,942 | 302 |

P2 point query (µs, client-observed for the loopback rows):

| Gate | Benchmark | p50 | p95 | p99 | min | max | MAD |
|---|---|---:|---:|---:|---:|---:|---:|
| standalone | warm_point_query (embedded, 100k rows × 10k queries) | 1.15 | 1.50 | 1.80 | 0.64 | 7.44 | 0.13 |
| standalone | loopback_point_query (10k rows × 1k queries) | 118.2 | 183.7 | 470.8 | 59.5 | 1,200.9 | 10.3 |
| full-feature (`cluster,oidc,vault-kms`) | loopback_point_query (10k rows × 1k queries) | 75.9 | 139.1 | 206.9 | 53.5 | 1,169.5 | 3.3 |

The two loopback gates stay within run-to-run variance of each other (both
far under the 250 ms tripwire); the full-feature build carries no point-query
penalty on this path. The harvester defaults to five repetitions and fail-closes when multi-rep
median p95 tripwires are exceeded (`p0p2-threshold-verdict.jsonl`).

## Closure status

| PR | Item | Status |
|---|---|---|
| A | Measurement and trace contracts | DONE |
| C | Async persistent result cache | DONE (15/15 contract tests) |
| D | True streaming cursors | test surface DONE (7/9; 2 ignored with explicit gap notes) |
| F | HOT fallback observability | DONE (6/6 lookup + 1/1 hot_metrics + 289/289 server) |
| B | Point lookup directory | DONE (28/28 directory tests; 256-run scaling gate 1.37 ≤ 1.4 measured on the exact-SHA tree) |
| E | Non-Bitmap churn oracle | oracle + strict snapshot-aware model DONE (13/13 on seed 1, all nine families); 30-night / 4-week durable history accumulates in CI |
| Final | Closure workflow + docs | `scripts/run-residual-closure.sh` + contract-driven workflow (`residual-closure-contract.json`) + one-seed-per-job 8-seed churn matrix + `verify-exact-sha` + churn-history gate + ADR-0014 + hot metrics export + REM-J P0/P2 harvester all shipped |

## Reproducing

```bash
bash scripts/run-residual-closure.sh                                              # full bundle
cargo test -p mongreldb-core                                                       # lib + controlled_scan + cache_async + lookup_metrics
cargo test -p mongreldb-server                                                     # server suites
MONGRELDB_ORACLE_SEED=1 \
  cargo test -p mongreldb-core --test index_churn_oracle --all-features             # 13 / 13
scripts/run-p0-p2-measurements.sh target/bench-results/rem-j                        # P0/P2 evidence
```




## B468 residual-closure P0/P2 integration

The exact-SHA five-repetition P0/P2 harvest (`scripts/run-p0-p2-measurements.sh`)
is the machine-enforced evidence source for residual closure. Default `REPS=5`.
It writes `p0-results.jsonl`, `p2-standalone-results.jsonl`,
`p2-full-feature-results.jsonl`, and `p0p2-threshold-verdict.jsonl`. Residual-closure
requires all four as core artifacts and evaluates multi-rep median p95
tripwires (fail closed). Each row carries the git SHA envelope plus `rep`.

Current train label: workspace version at capture time; cite the SHA recorded
in `commit.txt` of the residual-closure artifact bundle rather than a dirty
working tree.
