<p align="center">
  <img src="assets/mongrel.png" alt="MongrelDB logo" width="250" />
</p>

<h1 align="center">MongrelDB</h1>

<p align="center">
  <b>A log-structured columnar database for sub-millisecond writes, learned indexes, and AI-native access.</b>
  <br />
  Custom <code>.sr</code> columnar format · Bε-tree memtable · WAL with group commit · eight secondary index kinds · exact ANN reranking · multi-index intersection · MVCC snapshots · page-level encryption · declarative constraints · user/role auth · credential enforcement · replication · change data capture · DataFusion SQL · recursive CTEs · window functions · CREATE TABLE AS SELECT · materialized views · multi-statement SQL · FTS ranking · NAPI addon
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
**operational workloads** - sub-millisecond single-row writes and updates on a
custom columnar format, with a rich index set designed for AI-native access
patterns. **New to MongrelDB? Start with the [docs](docs/).**

The write path is an LSM/Bε-tree: an append-only WAL with group commit feeds a
Bε-tree memtable keyed by `(RowId, Epoch)`, which flushes to immutable sorted
runs (`.sr` PAX columnar pages). Single-row durable update: **~7 µs**.

The read path merges memtable + sorted runs under MVCC snapshot isolation. Six
user-creatable secondary index kinds resolve through a shared `RowId` space.
Native conditions compose as strict intersections:

| Index | Type | Use case |
|---|---|---|
| **Bitmap** | Roaring bitmap | Equality on low-cardinality columns |
| **PGM** | Learned (shrinking-cone, ε-bounded) | Range queries |
| **FM-index** | BWT + wavelet tree | Substring containment |
| **ANN** | Binary-quantized HNSW, Hamming distance | Approximate nearest neighbor candidates |
| **Sparse** | Inverted token lists | SPLADE-style learned-sparse retrieval (top-k by sparse dot product) |
| **MinHash** | LSH set-similarity | AI dedup/join primitives |

HNSW implements `Ann`; PGM implements `LearnedRange`. PMA is an internal
mutable-run tier. The implicit primary-key surface currently uses a `BTreeMap`
stand-in rather than a completed HOT trie.

## AI-native retrieval

MongrelDB keeps operational data and model-derived representations in the
same transactional row. Dense embeddings, learned-sparse vectors, lexical
content, set fingerprints, metadata, and timestamps can each use a specialized
index while resolving through one shared RowId space.

This supports:

- RAG retrieval with dense ANN, sparse retrieval, lexical constraints, and
  metadata filters;
- ingestion-time near-duplicate detection with MinHash/LSH plus exact Jaccard
  verification;
- agent memory retrieval by meaning, entities, recency, tenant, and memory type;
- prompt caching and training-data deduplication;
- local and embedded AI applications without a separate vector service.

| Signal | Index | Typical role |
|---|---|---|
| Dense embedding | HNSW | Semantic candidate generation |
| Sparse vector | Inverted sparse index | Rare-term and learned lexical relevance |
| Text | FM-index | Exact substring constraints |
| Token/entity/shingle set | MinHash/LSH | Near-duplicate and set-overlap candidates |
| Tenant/category/status | Roaring bitmap | Access control and metadata filters |
| Time/score | PGM learned range | Recency and numeric constraints |

Native conditions currently compose as conjunctive candidate-set filters.
ANN, Sparse, and MinHash calculate scores internally, but `Query` returns only
surviving rows. MinHash returns approximate LSH candidates; exact verification
cannot recover an LSH miss. The scored `Retriever` API and `/kit/retrieve`
preserve raw scores; `/kit/set_similarity` adds exact Jaccard verification.
`SearchRequest` and `/kit/search` apply hard filters first, union named
retrievers, fuse ranks with deterministic RRF, and return component plus fused
scores. An optional post-fusion exact-vector stage reranks a bounded candidate
window while preserving the original fused score, exact score, final score,
and final rank. Filtered retrieval evaluates RLS only for new approximate
candidates and adaptively over-fetches, so a highly selective policy may
honestly return fewer than `k` hits.
SQL exposes projected scored table functions: `ann_search_scored`,
`sparse_search_scored`, `minhash_search_scored`, `set_similarity_scored`, and
`hybrid_search_scored`. `ann_search_exact` and the matching Rust, Kit HTTP,
NAPI, C FFI, and Rust client APIs rerank binary-HNSW candidates from stored
full-precision vectors using cosine similarity, dot product, or L2 distance.

Every public AI surface rejects non-finite or oversized input. Shared ceilings
include 10,000 final hits, 100,000 candidates per retriever, 32 retrievers, 256
hard conditions, 65,536 sparse terms or set members, and 4,096 projected
columns. Hybrid unions stop at 250,000 candidates and materialize only ranked
result windows. Every scored Kit endpoint accepts `deadline_ms` and `max_work`,
runs off Tokio workers, cooperatively cancels engine work, and shares a bounded
concurrency semaphore. Configure lower server ceilings with
`MONGRELDB_AI_MAX_FUSED_CANDIDATES` and `MONGRELDB_AI_MAX_CONCURRENT`.
Scored SQL uses the same controls through `MONGRELDB_SQL_AI_TIMEOUT_MS`,
`MONGRELDB_SQL_AI_MAX_WORK`, `MONGRELDB_SQL_AI_MAX_FUSED_CANDIDATES`, and
`MONGRELDB_SQL_AI_MAX_CONCURRENT`.

Index options preserve existing defaults when omitted. `CREATE INDEX ... WITH
(...)` and Kit schema definitions can tune ANN `m`, `ef_construction`, and
`ef_search`; MinHash `permutations` and `bands`; and learned-range `epsilon`.
The HNSW index remains binary-sign quantized. Exact reranking reads stored
full-precision vectors, but full-precision and product-quantized ANN indexes are
not implemented.

## Performance profile

Measured on 1M rows, dev sandbox (full results in [`BENCHMARKS.md`](BENCHMARKS.md)):

| Metric | Value |
|---|---:|
| Single-row durable write (`put` + `commit`) | **8.0 µs** |
| Single-row durable update | **7.2 µs** |
| `put` (no fsync) | **618 ns** |
| `commit` (fsync, group commit) | **6.79 µs** |
| Bulk ingest (typed `bulk_load_columns`) | **25.7 Melem/s** (38.8 ms) |
| Bulk ingest (Value API `bulk_load`) | **12.1 Melem/s** (82.4 ms) |
| Full columnar scan (LE native-endian) | **12.3 Melem/s** (81.5 ms) |
| Full scan (all columns) | **14.1 Melem/s** (70.7 ms) |
| Bitmap-equality pushdown | **122 Melem/s** (8.2 ms) |
| Range pushdown (PGM learned index) | **118 Melem/s** (8.5 ms) |
| 1-column projection pushdown | **208 Melem/s** (4.8 ms) |
| Cold SQL filter (`WHERE cost < 250`) | **8.7 µs** |
| Cold SQL `COUNT(*)` | **290 µs** |
| Cold SQL join `COUNT(*)` | **1.16 ms** |
| Warm result-cache hit (any query) | **0.1-0.3 µs** |
| Storage | **4.17 bytes/row** (4.17 MB / 1M rows) |
| `COUNT(*)` metadata | **0 µs** (O(1)) |
| AES-256-GCM encrypt/decrypt | **~1.88 GiB/s** |

**Cross-engine (1M rows, latest):** single-row writes **1.8× faster than SQLite, 37× faster than DuckDB**.
Bulk insert **2.3× faster than SQLite, 2.6× faster than DuckDB native**. Join
`COUNT(*)` **3.4× faster than DuckDB, 19× faster than SQLite**.

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
- **SQL surface:** DataFusion 54 with `WITH RECURSIVE` CTEs, window functions
  (`OVER`/`PARTITION BY`), `CREATE TABLE AS SELECT`, session-scoped
  `CREATE VIEW` plus `CREATE MATERIALIZED VIEW`, multi-statement execution in a
  single `run()` call, and an FTS ranking UDF
  (`mongreldb_fts_rank`) alongside the `fts_docs` virtual table.
- **Constraints:** opt-in per-table declarative unique, foreign-key (with
  `RESTRICT`/`CASCADE`/`SET NULL` on delete), and CHECK constraints, enforced
  inside the core transaction path - no application-side validation required.
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
  and a typed Kit API (`/kit/create_table`, `/kit/schema`, `/kit/txn`,
  `/kit/query`, `/kit/retrieve`, `/kit/set_similarity`, `/kit/search`).
  `mongreldb-client` + NAPI `RemoteDatabase` connect to it.

AI retrieval availability:

| Surface | Boolean Query | Scored Retriever | Exact ANN rerank | Exact Set | Hybrid Search |
|---|---:|---:|---:|---:|---:|
| Rust core | Yes | Yes | Yes | Yes | Yes |
| Kit HTTP | Yes | Yes | Yes | Yes | Yes |
| SQL | Yes | Yes | Yes | Yes | Yes |
| NAPI embedded | Yes | No typed helper | Yes | No typed helper | No typed helper |
| C FFI | Yes | No typed helper | Yes | No typed helper | No typed helper |
| Rust HTTP client | Yes | Yes | Yes | Yes | Yes |

Credentialed NAPI and C FFI typed reads use the same row-level security and
column-mask checks as the core API. Raw `Table` access remains an explicit
in-process, policy-unaware engine API. `MongrelClient` and
`AsyncMongrelClient` support Bearer and HTTP Basic auth builders.
  Use `--daemon` to run in the background, or deploy with
  systemd/Docker/supervisord for auto-restart. See
  [Daemon Mode](docs/08-daemon.md#running-as-a-daemon---daemon-mode) for details.
  Notable flags: `--daemon` (background + PID file), `--pidfile <path>`,
  `--port <n>`, `--auth-token`/`--auth-users` (auth), `--max-connections <n>`,
  `--max-sessions <n>`, `--session-idle-timeout <s>`, and `--passphrase <key>`
  (page-level encryption).
  MVCC history retention defaults to 1024 epochs, can be set at startup with
  `MONGRELDB_HISTORY_RETENTION_EPOCHS`, and can be inspected or changed by an
  administrator through `GET`/`PUT /history/retention`. Both endpoints require
  `ADMIN` permission. `GET` returns
  `{"history_retention_epochs": <u64>, "earliest_retained_epoch": <u64>}`;
  `PUT` accepts `{"history_retention_epochs": <u64>}` and returns the same
  shape using the post-update values. Increasing `history_retention_epochs`
  cannot restore history that has already been pruned, so
  `earliest_retained_epoch` never moves backward.
- **Authentication:** `CREATE USER` / `CREATE ROLE` / `GRANT` / `REVOKE` with
  Argon2id password hashing. Daemon supports Bearer token (`--auth-token`) and
  HTTP Basic auth (`--auth-users`). **Credential enforcement**
  (`require_auth`) makes permissions required at the storage layer - every open
  and operation is checked against the authenticated principal. Connection
  pooling via `--max-connections`. See
  [Credential Enforcement](docs/15-credential-enforcement.md).
- **Replication:** `GET /wal/stream` streams committed WAL records; the
  `ReplicationFollower` client applies them to a local copy for read scaling.
- **Change data capture:** `NOTIFY` / `LISTEN` SQL commands + `GET /events`
  SSE endpoint for real-time change notifications.
- **GC / check / doctor:** Orphan run + stale WAL + stale shadow cleanup;
  footer checksum verification; best-effort repair.

## Encryption

MongrelDB supports optional page-level encryption via AES-256-GCM (enabled
with the `encryption` feature). The **secret is a passphrase or a raw key
file** - there is no KMS integration or environment-variable mechanism.

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
// Create - generates a random salt, persists it to _meta/keys
let db = Table::create_encrypted(dir, schema, 1, "my-passphrase")?;

// Open - reads the salt, re-derives the same KEK
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
| Per-page min/max zone maps | **Yes** - per-run encrypted stats envelope (page pruning without plaintext bounds) |
| Run header / directory | No - but **authenticated** by a required keyed HMAC (tamper-evident) |
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

## Language Clients

MongrelDB supports **34 languages** across two integration tiers:

- **Tier 1 (Embedded):** The engine runs in-process via native bindings. No daemon, zero serialization overhead.
- **Tier 2 (HTTP):** A pure-language HTTP client connects to a running `mongreldb-server` daemon. No native dependencies.

**Tier 1 (Embedded)** - 9 languages with in-process native bindings:

| Language | Binding | Repository | Install |
|---|---|---|---|
| **C** | Native (C ABI) + HTTP | [MongrelDB-C](https://github.com/visorcraft/MongrelDB-C) | CMake (links libcurl) or prebuilt `libmongreldb` |
| **C++** | Native (C ABI) + HTTP | [MongrelDB-CPP](https://github.com/visorcraft/MongrelDB-CPP) | CMake (header-only, links libcurl) or prebuilt `libmongreldb` |
| **C#/.NET** | Native (P/Invoke) + HTTP | [MongrelDB-DotNet](https://github.com/visorcraft/MongrelDB-DotNet) | `dotnet add package Visorcraft.MongrelDB.Native` (or `Visorcraft.MongrelDB` for HTTP) |
| **Java** | Native (JNI) + HTTP | [MongrelDB-Java](https://github.com/visorcraft/MongrelDB-Java) | Maven/Gradle + `libmongreldb_jni` |
| **Kotlin** | Native (JNI) + HTTP | [MongrelDB-Kotlin](https://github.com/visorcraft/MongrelDB-Kotlin) | Gradle + `libmongreldb_jni` |
| **Python** | Native (PyO3) | [MongrelDB Kit](https://github.com/visorcraft/MongrelDB-Kit) | `pip install mongreldb-kit` |
| **Rust** | Native (Direct) | [MongrelDB](https://github.com/visorcraft/MongrelDB) | `cargo add mongreldb-core` |
| **Scala** | Native (JNI) + HTTP | [MongrelDB-Scala](https://github.com/visorcraft/MongrelDB-Scala) | sbt + `libmongreldb_jni` |
| **TypeScript** | Native (NAPI) | [MongrelDB Kit](https://github.com/visorcraft/MongrelDB-Kit) | `npm install @visorcraft/mongreldb-kit` |

**Tier 2 (HTTP)** - 25 languages with pure HTTP clients:

| Language | HTTP library | Repository | Install |
|---|---|---|---|
| **Clojure** | `clj-http` | [MongrelDB-Clojure](https://github.com/visorcraft/MongrelDB-Clojure) | deps.edn / Leiningen |
| **Crystal** | `HTTP::Client` | [MongrelDB-Crystal](https://github.com/visorcraft/MongrelDB-Crystal) | `shards add mongreldb` |
| **D** | `requests` | [MongrelDB-D](https://github.com/visorcraft/MongrelDB-D) | `dub add mongreldb` |
| **Dart** | `http` | [MongrelDB-Dart](https://github.com/visorcraft/MongrelDB-Dart) | `dart pub add mongreldb` |
| **Elixir** | `Req` | [MongrelDB-Elixir](https://github.com/visorcraft/MongrelDB-Elixir) | `{:mongreldb, "~> 0.1"}` in `mix.exs` |
| **Erlang** | `httpc` | [MongrelDB-Erlang](https://github.com/visorcraft/MongrelDB-Erlang) | rebar3 |
| **F#** | `HttpClient` | [MongrelDB-FSharp](https://github.com/visorcraft/MongrelDB-FSharp) | `dotnet add reference` |
| **Fortran** | `curl` | [MongrelDB-Fortran](https://github.com/visorcraft/MongrelDB-Fortran) | fpm |
| **Gleam** | `gleam_http` | [MongrelDB-Gleam](https://github.com/visorcraft/MongrelDB-Gleam) | `gleam add mongreldb` |
| **Go** | `net/http` | [MongrelDB-Go](https://github.com/visorcraft/MongrelDB-Go) | `go get github.com/visorcraft/mongreldb-go` |
| **Julia** | `HTTP.jl` | [MongrelDB-Julia](https://github.com/visorcraft/MongrelDB-Julia) | `] add MongrelDB` |
| **Lua** | `lua-curl` | [MongrelDB-Lua](https://github.com/visorcraft/MongrelDB-Lua) | `luarocks install mongreldb` |
| **Mojo** | `http` | [MongrelDB-Mojo](https://github.com/visorcraft/MongrelDB-Mojo) | `magic add mongreldb` |
| **Nim** | `HttpClient` | [MongrelDB-Nim](https://github.com/visorcraft/MongrelDB-Nim) | `nimble install mongreldb` |
| **Objective-C** | `NSURLSession` | [MongrelDB-ObjC](https://github.com/visorcraft/MongrelDB-ObjC) | CMake (links Foundation) |
| **Odin** | `net/http` | [MongrelDB-Odin](https://github.com/visorcraft/MongrelDB-Odin) | `odin build` |
| **Perl** | `HTTP::Tiny` | [MongrelDB-Perl](https://github.com/visorcraft/MongrelDB-Perl) | `cpanm MongrelDB` |
| **PHP** | `cURL` | [MongrelDB-PHP](https://github.com/visorcraft/MongrelDB-PHP) | `composer require visorcraft/mongreldb-php` |
| **PowerShell** | `Invoke-RestMethod` | [MongrelDB-Powershell](https://github.com/visorcraft/MongrelDB-Powershell) | `Import-Module mongreldb` |
| **R** | `libcurl` | [MongrelDB-R](https://github.com/visorcraft/MongrelDB-R) | `install.packages("mongreldb")` |
| **Ruby** | `net/http` | [MongrelDB-Ruby](https://github.com/visorcraft/MongrelDB-Ruby) | `gem install mongreldb` |
| **Swift** | `URLSession` | [MongrelDB-Swift](https://github.com/visorcraft/MongrelDB-Swift) | Swift Package Manager |
| **Tcl** | `http` | [MongrelDB-Tcl](https://github.com/visorcraft/MongrelDB-Tcl) | `package require mongreldb` |
| **V** | `net.http` | [MongrelDB-V](https://github.com/visorcraft/MongrelDB-V) | `v install` |
| **Zig** | `std.http` | [MongrelDB-Zig](https://github.com/visorcraft/MongrelDB-Zig) | `zig fetch` |

The **[C ABI](crates/mongreldb-ffi)** (`mongreldb-ffi`) provides a stable C interface over the engine core: opaque handles, typed queries, transactions, auth, **SQL execution** (DataFusion, returns Arrow IPC), and **migration planning/checksums** (JSON in/out, language-neutral). A second FFI crate, **[mongreldb-kit-ffi](crates/mongreldb-kit-ffi)**, adds the Kit layer (schema model, full migration runner, query builder execution) as `libmongreldb_kit`. A third binding crate, **[mongreldb-jni](crates/mongreldb-jni)**, provides a JNI shim (`libmongreldb_jni`) for Java, Kotlin, and Scala. The C and C++ clients bundle both C ABI headers for direct native embedding.

### Native libraries (prebuilt)

Prebuilt `libmongreldb` (core engine), `libmongreldb_kit` (Kit layer), and `libmongreldb_jni` (JVM shim) are attached to every release for six platform targets:

| Platform | C/C++ archives | JVM JAR |
|---|---|---|
| Linux x64 (glibc) | `mongreldb-native-linux-x64-gnu.tar.gz` + `mongreldb-kit-native-linux-x64-gnu.tar.gz` | `mongreldb-jni-0.52.2-linux-x64.jar` |
| Linux x64 (musl) | `mongreldb-native-linux-x64-musl.tar.gz` + `mongreldb-kit-native-linux-x64-musl.tar.gz` | `mongreldb-jni-0.52.2-linux-x64-musl.jar` |
| Linux arm64 (glibc) | `mongreldb-native-linux-arm64-gnu.tar.gz` + `mongreldb-kit-native-linux-arm64-gnu.tar.gz` | `mongreldb-jni-0.52.2-linux-arm64.jar` |
| macOS arm64 | `mongreldb-native-darwin-arm64.tar.gz` + `mongreldb-kit-native-darwin-arm64.tar.gz` | `mongreldb-jni-0.52.2-darwin-arm64.jar` |
| macOS x64 | `mongreldb-native-darwin-x64.tar.gz` + `mongreldb-kit-native-darwin-x64.tar.gz` | `mongreldb-jni-0.52.2-darwin-x64.jar` |
| Windows x64 | `mongreldb-native-windows-x64.zip` + `mongreldb-kit-native-windows-x64.zip` | `mongreldb-jni-0.52.2-windows-x64.jar` |

A fat JAR (`mongreldb-jni-0.52.2.jar`) with all platforms bundled is also published. Each C/C++ archive contains `lib/` (shared + static libraries) and `include/` (the C header). Download from the [releases page](https://github.com/visorcraft/MongrelDB/releases). See the C, C++, .NET, Java, Kotlin, and Scala client READMEs for linking instructions.

## Node.js addon

MongrelDB also ships as a native NAPI addon (`crates/mongreldb-node`) - the
**better-sqlite3 model**: in-process, no HTTP hop, so the sub-ms write latency
isn't lost to a round-trip. It exposes both a **typed object/method API** and a
**full SQL surface**: the hybrid `query` composes ANN, FM, bitmap equality/IN,
range, null, and `BytesPrefix` conditions in a single row-id-space intersection,
while `db.sql(sql)` runs cross-table SQL (DataFusion) and returns Arrow IPC.
TypeScript types are generated at build time, and row ids / counts / epochs cross
the FFI as lossless `BigInt`:

```sh
cd crates/mongreldb-node && npm install && npm run build   # release NAPI addon + typings
```

A `smoke.mjs` exercises put/get/count and a hybrid query against the live addon.
Create/open a `Database`, create tables with `createTable`, then operate through
`db.table(name)`. The table handle exposes `put`, `putBatch`, `bulkLoadTyped`,
`query`, `queryArrow`, `count`, and `countWhere`; Promise variants are available
for blocking read/write methods. The `Database` also exposes `sql(sql)` (returns
Arrow IPC bytes), `createTable`/`dropTable`/`renameTable`, and procedure/trigger
management. `RemoteDatabase` routes to a `mongreldb-server` daemon for
multi-process cache sharing.

The addon's `Database` holds a long-lived SQL session for the database's
lifetime, so session-scoped objects - views (`CREATE VIEW`), prepared
statements, and the result cache - persist across `sql()` calls. Reopening the
database starts a fresh session (re-apply any view-defining migrations then).

## Benchmarks

`crates/mongreldb-perf` is a standalone harness comparing MongrelDB (plain +
encrypted) to SQLite and DuckDB (native / Parquet / CSV) at 100 and 1M rows.
Measured results and analysis live in [`BENCHMARKS.md`](BENCHMARKS.md). Summary:
MongrelDB wins single-row writes (**8.0 µs** vs SQLite 14.7 µs, DuckDB 296 µs),
bulk insert (**93.9 ms** vs SQLite 213.7 ms, DuckDB native 247.9 ms), join
`COUNT(*)` (**1.16 ms** vs DuckDB 3.93 ms, SQLite 22.5 ms), and O(1)
`count()`. DuckDB-Parquet wins bulk file creation (31.65 ms via `COPY`) and has
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
                          columnar codec, six secondary index kinds (Bitmap/PGM/FM/
                          ANN/Sparse/MinHash), page stats, encryption, constraints,
                          compaction, GC, check/doctor
crates/mongreldb-query/   DataFusion 54 SQL + Arrow frontend (predicate/projection
                          pushdown, multi-table joins, ann_search/sparse_match UDF,
                          result cache, Arrow IPC shadow, materialized views)
crates/mongreldb-node/    NAPI addon (typed object API; built via `napi`)
crates/mongreldb-server/  HTTP daemon (axum/tokio; SQL + native query + typed Kit API)
crates/mongreldb-client/  typed HTTP client for the daemon (SQL/native + Kit API)
crates/mongreldb-ffi/     C ABI over the engine core (SQL, migrations, foundation for native bindings)
crates/mongreldb-kit-ffi/ C ABI over MongrelDB Kit (schema model, migration runner, query builder)
crates/mongreldb-jni/     JNI shim for the JVM (Java, Kotlin, Scala)
crates/mongreldb-perf/    cross-engine benchmark vs SQLite/DuckDB (standalone)
crates/mongreldb-core/examples/hybrid_query.rs
                          runnable ann ∩ fm ∩ bitmap hybrid-query demo
BENCHMARKS.md             measured cross-engine performance matrix
```

## License

MongrelDB is dual-licensed under MIT or Apache-2.0.
