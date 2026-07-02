<p align="center">
  <img src="assets/mongrel.png" alt="MongrelDB logo" width="250" />
</p>

<h1 align="center">MongrelDB</h1>

<p align="center">
  <b>A log-structured columnar database for sub-millisecond writes, learned indexes, and AI-native access.</b>
  <br />
  Custom <code>.sr</code> columnar format · Bε-tree memtable · WAL with group commit · eight index kinds · hybrid pushdown · MVCC snapshots · page-level encryption · declarative constraints · DataFusion SQL · NAPI addon
</p>

<p align="center">
  <a href="https://github.com/visorcraft/MongrelDB/releases/latest"><img src="https://img.shields.io/github/v/release/visorcraft/MongrelDB?sort=semver" alt="Latest release" /></a>
  <img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg" alt="License: MIT OR Apache-2.0" />
  <img src="https://img.shields.io/badge/built%20with-Rust-000000?logo=rust&amp;logoColor=white" alt="Built with Rust" />
  <img src="https://img.shields.io/badge/engine-DataFusion%2054-4B8BBE" alt="DataFusion 54" />
  <img src="https://img.shields.io/badge/platform-Linux%20%7C%20macOS%20%7C%20Windows-333333?logo=linux&amp;logoColor=white" alt="Platform: Linux, macOS, Windows" />
</p>

---

## What is MongrelDB?

MongrelDB is an embedded, single-node database engine optimized for
**operational workloads** — sub-millisecond single-row writes and updates on a
custom columnar format, with a rich index set designed for AI-native access
patterns. **New to MongrelDB? Start with the [docs](docs/).**

The write path is an LSM/Bε-tree: an append-only WAL with group commit feeds a
Bε-tree memtable keyed by `(RowId, Epoch)`, which flushes to immutable sorted
runs (`.sr` PAX columnar pages). Single-row durable update: **~6 µs**.

The read path merges memtable + sorted runs under MVCC snapshot isolation. Eight
index kinds — all resolving through a shared `RowId` space — enable hybrid
queries that no single traditional index can serve:

| Index | Type | Use case |
|---|---|---|
| **HOT** | Height-optimized trie | Primary-key point lookup |
| **Bitmap** | Roaring bitmap | Equality on low-cardinality columns |
| **PGM** | Learned (shrinking-cone, ε-bounded) | Range queries |
| **FM-index** | BWT + wavelet tree | Substring containment |
| **HNSW** | Hierarchical navigable small world | Approximate nearest neighbor (recall@10 ≥ 0.90) |
| **PMA** | Packed memory array | Cache-oblivious mutable sorted runs |
| **Sparse** | Inverted token lists | SPLADE-style learned-sparse retrieval (top-k by sparse dot product) |
| **MinHash** | LSH set-similarity | AI dedup/join primitives |

## Performance profile

Measured on 1M rows, dev sandbox (full results in [`BENCHMARKS.md`](BENCHMARKS.md)):

| Metric | Value |
|---|---:|
| Single-row durable write (`put` + `commit`) | **7.7 µs** |
| Single-row durable update | **7.4 µs** |
| `put` (no fsync) | **580 ns** |
| `commit` (fsync, group commit) | **5.9 µs** |
| Bulk ingest (typed `bulk_load_columns`) | **25.7 Melem/s** (38.8 ms) |
| Bulk ingest (Value API `bulk_load`) | **12.1 Melem/s** (82.4 ms) |
| Full columnar scan (LE native-endian) | **12.3 Melem/s** (81.5 ms) |
| Full scan (all columns) | **14.3 Melem/s** (70.1 ms) |
| Bitmap-equality pushdown | **109.6 Melem/s** (9.1 ms) |
| Range pushdown (PGM learned index) | **107.5 Melem/s** (9.3 ms) |
| 1-column projection pushdown | **176.3 Melem/s** (5.7 ms) |
| Cold SQL filter (`WHERE cost < 250`) | **8.0 ms** |
| Cold SQL `COUNT(*)` | **255 µs** |
| Cold SQL join `COUNT(*)` | **1.53 ms** |
| Warm result-cache hit (any query) | **0.1–0.3 µs** |
| Storage | **4.17 bytes/row** (4.17 MB / 1M rows) |
| `COUNT(*)` metadata | **0 µs** (O(1)) |
| AES-256-GCM encrypt/decrypt | **~1.88 GiB/s** |

**Cross-engine (1M rows):** single-row writes **2× faster than SQLite, 39× faster than DuckDB**.
Bulk insert **2.7× faster than SQLite, 3.9× faster than DuckDB native**. Join
`COUNT(*)` **2.6× faster than DuckDB, 15× faster than SQLite**.

## Architecture

- **Format:** Custom `.sr` sorted-run files (PAX columnar pages with
  self-describing encoding-byte prefix). Adaptive per-column encoding: Delta
  for sorted integers, Dictionary for low-cardinality strings, Zstd for
  high-cardinality data, plaintext passthrough. Int/float buffer↔bytes codec is
  vectorized (`bytemuck`); sorted runs are memory-mapped (no per-page read
  syscalls).
- **Write path:** WAL (group commit) → Bε-tree memtable → immutable sorted
  runs. Write amplification approaches O(1).
- **Read path:** Memtable merge + sorted-run scan. MVCC: readers pin
  `Snapshot { epoch }`, see only `committed_epoch <= snapshot.epoch`.
- **Predicate pushdown:** `WHERE col = lit`, `col <,>,<=,>=,BETWEEN`, and
  `col LIKE '%p%'` translate to index-backed conditions (Bitmap/PK, Range,
  FM-index) resolved to a row-id set; `ann_search(col, '[..]', k)` is a UDF that
  resolves via HNSW. Conditions intersect in the shared `RowId` space, then only
  matching rows + requested columns are decoded.
- **Projection pushdown:** only the columns the query asks for are decoded.
- **Page index:** columns are split into 65 536-row pages with populated
  `PageStat` min/max; the reader skips pages whose `[min,max]` excludes the
  predicate during filtered scans (Parquet-style pruning). Encrypted columns
  keep their min/max out of the cleartext directory (it would leak values);
  the bounds travel in a per-run AES-256-GCM stats envelope decrypted once at
  open, so encrypted columns prune identically to plaintext ones.
- **Multi-table:** a `Database` hosts many named tables under a shared WAL;
  distinct tables register on one DataFusion context for joins.
- **Constraints:** opt-in per-table declarative unique, foreign-key (with
  `RESTRICT`/`CASCADE`/`SET NULL` on delete), and CHECK constraints, enforced
  inside the core transaction path — no application-side validation required.
- **Arrow bridge:** Constructs `Int64Array`/`Float64Array` directly from typed
  buffers (one memcpy, no per-element builder) for the all-non-null case.
- **Compaction:** Merges sorted runs with snapshot retention (readers pinning
  old epochs still see consistent data).
- **Encryption:** Optional page-level AES-256-GCM (`encryption` feature).
  See [Encryption](#encryption) below.
- **Result cache:** Fine-grained invalidation (footprint + condition-column
  based, not coarse epoch wipe). Persistent on-disk tier (`_rcache/`). Wired
  into SQL scan + NAPI query + native Condition API. Warm cache hits return
  pre-computed Arrow batches in ~0.1 µs.
- **Arrow IPC shadow:** Zero-copy read cache for clean single-run tables
  (`_shadow/`). Lazy-written on first scan, zero-copy RecordBatch on
  subsequent scans.
- **Schema evolution:** `add_column` adds a nullable column; old runs read it
  as null.
- **Daemon:** Optional `mongreldb-server` HTTP daemon (axum/tokio) keeps a
  multi-table `Database` warm for multi-process access, over SQL/native routes
  and a typed Kit API (`/kit/schema`, `/kit/txn`, `/kit/query`,
  `/kit/create_table`). `mongreldb-client` + NAPI `RemoteDatabase` connect to it.
- **GC / check / doctor:** Orphan run + stale WAL + stale shadow cleanup;
  footer checksum verification; best-effort repair.

## Encryption

MongrelDB supports optional page-level encryption via AES-256-GCM (enabled
with the `encryption` feature). The **secret is a passphrase or a raw key
file** — there is no KMS integration or environment-variable mechanism.

### Key hierarchy

```
passphrase + salt (16-byte random, in _meta/keys)   |   raw key file (≥32 bytes)
  │                                                      │
  ▼  Argon2id (19 MiB, t=2) + HKDF-SHA256               ▼  HKDF-SHA256 only
  └─────────────► KEK (256-bit, table-level, never persisted) ◄───────────┘
        │
        ├──► per-run DEK (random; AES-256-GCM-wrapped in the run descriptor) → page payloads
        ├──► WAL key              → WAL frame AEAD (_wal/)
        ├──► result-cache key     → _rcache/ AEAD
        ├──► index-checkpoint key → _idx/global.idx AEAD
        ├──► run-metadata MAC key → HMAC over each run's header + dir + descriptor
        └──► per-column key (HKDF "mongreldb/colkey/" + column_id)   [ENCRYPTED_INDEXABLE]
               ├──► HMAC-SHA256          → deterministic equality tokens
               └──► order-preserving enc → non-linear range tokens
```

All key material in memory is wrapped in `Zeroizing` and wiped on drop.

### Usage

```rust
// Create — generates a random salt, persists it to _meta/keys
let db = Table::create_encrypted(dir, schema, 1, "my-passphrase")?;

// Open — reads the salt, re-derives the same KEK
let db = Table::open_encrypted(dir, "my-passphrase")?;
```

```sh
# Build with encryption support
cargo build --release --features encryption
```

### What is and isn't encrypted

| Component | Encrypted? |
|---|---|
| Sorted-run page payloads (`.sr`) | **Yes** (AES-256-GCM per page) |
| WAL segments (`_wal/`) | **Yes** (frame-level AES-256-GCM) |
| Result cache (`_rcache/`) | **Yes** (AES-256-GCM) |
| Index checkpoint (`_idx/global.idx`) | **Yes** (AES-256-GCM) |
| Per-page min/max zone maps | **Yes** — per-run encrypted stats envelope (page pruning without plaintext bounds) |
| Run header / directory | No — but **authenticated** by a required keyed HMAC (tamper-evident) |
| Manifest / schema | No |

Tampering an encrypted run's cleartext metadata (offsets, page stats, structure)
is caught on open by the run-metadata MAC; page payloads are authenticated per
page by AES-256-GCM.

### Key files

In addition to the passphrase API, you can use a raw key file:

```rust
let key = std::fs::read("my.key")?;  // 32+ bytes of random data
let db = Table::create_with_key(dir, schema, 1, &key)?;
let db = Table::open_with_key(dir, &key)?;
```

Generate a key with `openssl rand 32 > my.key`. The raw key path skips
Argon2id (~0.1ms vs ~50ms for passphrases).

### Performance overhead

~1.87 GiB/s encrypt/decrypt throughput (AES-256-GCM, hardware-accelerated).
In practice, encryption adds negligible latency to bulk ingest and queries
(measured at <5% overhead on 1M-row workloads).

## Node.js addon

MongrelDB also ships as a native NAPI addon (`crates/mongreldb-node`) — the
**better-sqlite3 model**: in-process, no HTTP hop, so the sub-ms write latency
isn't lost to a round-trip. It exposes a **typed object/method API** (not SQL)
with a hybrid `query` that composes ANN, FM, bitmap equality/IN, and range
conditions in a single row-id-space intersection. TypeScript types are generated
at build time, and row ids / counts / epochs cross the FFI as lossless `BigInt`:

```sh
cd crates/mongreldb-node && npm install && npm run build   # release NAPI addon + typings
```

A `smoke.mjs` exercises put/get/count and a hybrid query against the live addon.
Create/open a `Database`, create tables with `createTable`, then operate through
`db.table(name)`. The table handle exposes `put`, `putBatch`, `bulkLoadTyped`,
`query`, `queryArrow`, `count`, and `countWhere`; Promise variants are available
for blocking read/write methods. `RemoteDatabase` routes to a
`mongreldb-server` daemon for multi-process cache sharing.

## Benchmarks

`crates/mongreldb-perf` is a standalone harness comparing MongrelDB (plain +
encrypted) to SQLite and DuckDB (native / Parquet / CSV) at 100 and 1M rows.
Measured results and analysis live in [`BENCHMARKS.md`](BENCHMARKS.md). Summary:
MongrelDB wins single-row writes (**7.7 µs** vs SQLite 14.2 µs, DuckDB 301 µs),
bulk insert (**75.2 ms** vs SQLite 200.4 ms, DuckDB native 290.7 ms), join
`COUNT(*)` (**1.53 ms** vs DuckDB 3.95 ms, SQLite 22.8 ms), and O(1)
`count()`. DuckDB-Parquet wins bulk file creation (26 ms via `COPY`) and has
the fastest analytical filter. Warm result-cache hits are sub-µs across all
queries.

## Setup

**Prerequisites:** Rust ≥ 1.80 (Node.js ≥ 16 for the addon).

```sh
git clone https://github.com/visorcraft/MongrelDB.git
cd MongrelDB
cargo build --release
```

## Development

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features

# Individual crates
cargo test -p mongreldb-core --all-features   # core tests
cargo test -p mongreldb-query                 # SQL/frontend tests
cargo bench -p mongreldb-core --bench filtered_query
cargo run -p mongreldb-core --example hybrid_query --release   # hybrid-query demo
```

## Project layout

```text
crates/mongreldb-core/    WAL, memtable, Bε-tree, sorted runs (mmap'd), vectorized
                          columnar codec, eight index kinds (HOT/Bitmap/PGM/FM/HNSW/
                          PMA/Sparse/MinHash), page stats, encryption, constraints,
                          compaction, GC, check/doctor
crates/mongreldb-query/   DataFusion 54 SQL + Arrow frontend (predicate/projection
                          pushdown, multi-table joins, ann_search/sparse_match UDF,
                          result cache, Arrow IPC shadow, materialized views)
crates/mongreldb-node/    NAPI addon (typed object API; built via `napi`)
crates/mongreldb-server/  HTTP daemon (axum/tokio; SQL + native query + typed Kit API)
crates/mongreldb-client/  typed HTTP client for the daemon (SQL/native + Kit API)
crates/mongreldb-perf/    cross-engine benchmark vs SQLite/DuckDB (standalone)
crates/mongreldb-core/examples/hybrid_query.rs
                          runnable ann ∩ fm ∩ bitmap hybrid-query demo
BENCHMARKS.md             measured cross-engine performance matrix
```

## License

MongrelDB is dual-licensed under MIT or Apache-2.0.
