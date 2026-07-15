# SQL Cancellation Qualification, 2026-07-14

The latest benchmark was run from `aa55cbb27c392ed3466c2f1321aaa7d1cd098900`
with only the queued-latency benchmark itself uncommitted.

## Environment

```text
CPU: Intel Core Ultra 9 386H, 16 cores, 1 thread/core
OS: Linux 7.2.0-rc3-1-cachyos-rc x86_64
Rust: rustc 1.96.1 (31fca3adb 2026-06-26)
Profile: Cargo bench optimized
Dataset: 100,000 Int64 rows
Criterion mode: --quick characterization
```

## Results

Command:

```sh
cargo bench --manifest-path crates/mongreldb-query/Cargo.toml \
  --bench sql_cancellation -- --quick
```

| Measurement | Quick interval |
|---|---:|
| Controlled `SELECT 1` | 2.1082 to 2.1108 microseconds |
| Controlled DataFusion 100k scan and expression aggregate | 27.018 to 27.345 milliseconds |
| Cancel accepted to scan worker finished | 86.625 to 96.768 microseconds |
| Cancel accepted to queued worker finished | 4.1239 to 4.3059 microseconds |

The scan cancellation interval is below the initial 100 ms native scan p95
target by over three orders of magnitude. Queued cancellation is below the
initial 20 ms target by over three orders of magnitude. Criterion reported no
statistically significant change from the prior quick characterization for
the first three measurements. This quick run is a local
characterization, not a statistically complete release artifact. Release CI
should run Criterion normally and retain `target/criterion` output.

The benchmark clears result and plan caches before scan measurements and uses
`sum(id * id)` to prevent the native precomputed aggregate fast path from
turning the scan into a cache/metadata lookup. Cancellation pauses at the
deterministic `BeforeScanBatch` hook, requests cancellation, releases the
worker, and measures until the worker has finished.

## Correctness gates exercised

```text
query registry and strict query IDs
queued cancellation and queued deadline
managed stream drop cleanup
fresh controls for prepared-plan reuse
autocommit cancel before commit fence
commit wins cancel race
explicit transaction savepoint restore and aborted state
buffered serialization cancellation
Arrow stream disconnect cancellation
session close cancellation
idle-reaper active-query protection
prepared planning and execution cancellation
graceful shutdown cancellation
commit-fence HTTP outcome and durable status
owner/admin/cross-user query-control security
NAPI, C ABI, Rust HTTP client, Kit Rust, TypeScript, Python, and CLI surfaces
```

The complete release gate remains:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```
