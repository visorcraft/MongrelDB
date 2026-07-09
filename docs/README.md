# MongrelDB Documentation

Welcome! MongrelDB is a fast embedded database for applications that need
both quick single-row writes and rich search capabilities — text search,
vector similarity, range queries, and more.

## Start Here

**New to MongrelDB?** Read these in order:

1. **[Getting Started](01-getting-started.md)** — install, create your first
   database, write and read data
2. **[Rust Quick Start](02-rust-quickstart.md)** — the full Rust API (writes,
   reads, batch insert, bulk load, transactions)
3. **[Node.js Quick Start](03-nodejs-quickstart.md)** — same for JavaScript /
   TypeScript (sync + async, batch, Arrow results)

## Topics

4. **[SQL Queries](04-sql-queries.md)** — running SQL with the DataFusion
   engine, WHERE pushdown, result caching, materialized views
5. **[Native Queries](05-native-queries.md)** — the Condition API: composing
   bitmap + range + text + vector searches in a single call
6. **[Indexes](06-indexes.md)** — the seven index types explained, and when
   to use each
7. **[Encryption](07-encryption.md)** — protecting data at rest with
   AES-256-GCM and a passphrase
8. **[Daemon Mode](08-daemon.md)** — running `mongreldb-server` for
   multi-process access over HTTP
9. **[Maintenance & Operations](09-maintenance.md)** — compaction,
   flush-on-close, cron jobs, GC
10. **[Stored Procedures](10-stored-procedures.md)** — catalog-backed routines
    callable from SQL, HTTP, NAPI, and Kit clients
11. **[Extended SQL Functions](11-extended-sql-functions.md)** — built-in
    date/time, JSON, string, math, and custom function hooks
12. **[Operational SQL Commands](12-operational-sql-commands.md)** —
    PRAGMA introspection, ANALYZE, REINDEX, VACUUM, and EXPLAIN QUERY PLAN
13. **[Trigger Programs & External Table Modules](13-triggers-and-external-table-modules.md)** —
    architecture spec for triggers and external table modules
14. **[Users, Roles & Permissions](14-auth.md)** — catalog-stored users with
    Argon2id password hashing, roles, `GRANT`/`REVOKE`, and daemon (HTTP Basic +
    Bearer token) authentication
15. **[Credential Enforcement](15-credential-enforcement.md)** — opt-in
    `require_auth` storage-layer enforcement: credentialed open/create
    constructors, the full enforcement matrix, composition with encryption,
    and offline recovery

## Quick Reference

### Performance (1M rows)

| Operation | Speed |
|---|---:|
| Single-row write (durable) | **8.0 µs** |
| Bulk ingest (typed) | **25.7 Melem/s** (38.8 ms) |
| Columnar scan | **12.3 Melem/s** (81.5 ms) |
| Bitmap equality lookup | **122 Melem/s** (8.2 ms) |
| Warm cache hit | **0.1–0.3 µs** |
| Storage | 4.17 bytes/row |

### Supported Languages

| Language | Package | Status |
|---|---|---|
| Rust | `mongreldb-core` + `mongreldb-query` | Full API |
| Node.js / TypeScript | `mongreldb-node` (NAPI addon) | Full API |
| HTTP (any language) | `mongreldb-server` daemon | SQL + native query |

### Data Types

| MongrelDB type | Rust | JavaScript |
|---|---|---|
| `Int64` | `i64` | `BigInt` |
| `Float64` | `f64` | `Number` |
| `Bool` | `bool` | `boolean` |
| `Decimal128` | `i128` | `string` |
| `TimestampNanos` | `i64` | `BigInt` |
| `Date32`/`Date64` | `i32`/`i64` | `Number` |
| `Time64` | `i64` | `BigInt` |
| `Interval` | `{months,days,nanos}` | `{months,days,nanos}` |
| `Uuid` | `[u8; 16]` | `string` |
| `Json` | `Vec<u8>` | `string` |
| `Array` | `Vec<u8>` (JSON) | `unknown[]` |
| `Bytes` (text/binary) | `Vec<u8>` | `Buffer` / `string` |
| `Embedding` | `Vec<f32>` | `Float32Array` |

## Repository

- **GitHub:** [github.com/visorcraft/MongrelDB](https://github.com/visorcraft/MongrelDB)
- **License:** MIT OR Apache-2.0
- **Benchmarks:** [BENCHMARKS.md](../BENCHMARKS.md)
