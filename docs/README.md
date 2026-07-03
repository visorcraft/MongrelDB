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

## Quick Reference

### Performance (1M rows)

| Operation | Speed |
|---|---:|
| Single-row write (durable) | 6.7 µs |
| Bulk ingest (typed) | 26.2M rows/sec |
| Columnar scan | 11.9M rows/sec |
| Bitmap equality lookup | 64.8M rows/sec |
| Warm cache hit | 0.1 µs |
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
| `Bytes` (text/binary) | `Vec<u8>` | `Buffer` / `string` |
| `Bool` | `bool` | `boolean` |
| `Embedding` (fixed-size f32 vector) | `Vec<f32>` | `Float32Array` |

## Repository

- **GitHub:** [github.com/visorcraft/MongrelDB](https://github.com/visorcraft/MongrelDB)
- **License:** MIT OR Apache-2.0
- **Benchmarks:** [BENCHMARKS.md](../BENCHMARKS.md)
