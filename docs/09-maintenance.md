# Maintenance & Operations

MongrelDB is log-structured: writes append to a WAL, then flush to
immutable `.sr` (sorted run) files. Over time, frequent writes create
multiple runs per table - which is fine (readers merge them under MVCC),
but query latency is best when each table has one clean run. Compaction
merges all runs back into one.

## Compaction

### When to compact

| Pattern | Runs accumulate? | Action |
|---|---|---|
| Daemon (long-lived) | Auto-compacted every 30s | None - the background sweep handles it |
| CLI / embedded (open, write, close) | One tiny run per invocation | Compact periodically (cron) or on startup |
| Bulk load + occasional updates | Few runs | Compact after a burst of updates |

A table with **8+ runs** triggers the daemon's automatic compaction
(`AUTO_COMPACT_RUN_THRESHOLD`). You can check the run count at any time:

```rust
let run_count = db.table("events")?.lock().run_count();
```

### Daemon auto-compaction

If you're running `mongreldb-server`, a background thread sweeps every
table every 30 seconds and compacts any with 8+ runs. No configuration
needed - this is always on.

### Manual compaction

#### CLI

```sh
mongreldb-kit-cli compact /path/to/db
# → compacted 3 table(s), skipped 1
```

This opens the database, compacts every table, and exits. Safe to run
at any time - readers pin their own snapshot and are unaffected.

#### HTTP (daemon)

```sh
# All tables
curl -X POST http://127.0.0.1:8453/compact
# → {"status":"ok","compacted":3,"skipped":1}

# Single table
curl -X POST http://127.0.0.1:8453/tables/events/compact
# → {"status":"compacted","table":"events"}
```

#### Rust

```rust
use mongreldb_core::Database;

let db = Database::open("/path/to/db")?;

// All tables
let (compacted, skipped) = db.compact()?;

// Single table
let did_compact = db.compact_table("events")?;
```

#### Node.js / TypeScript

```typescript
const stats = db.compactAll();
// → { compacted: 3, skipped: 1 }

db.compactTable("events");
// → true (or false if skipped)
```

#### Python

```python
compacted, skipped = db.compact_all()
# → (3, 1)

db.compact_table("events")
# → True (or False if skipped)
```

### Cron job

For non-daemon deployments (CLI / embedded processes that open, write,
close), schedule a periodic compaction:

```sh
# /etc/cron.d/mongreldb-compact
0 2 * * * appuser mongreldb-kit-cli compact /var/lib/myapp/db
```

Or against a running daemon:

```sh
0 2 * * * appuser curl -sS -X POST http://127.0.0.1:8453/compact
```

### What compaction does

1. Reads all live rows across every sorted run (honoring MVCC - readers
   with pinned snapshots are unaffected).
2. Writes a single new clean `.sr` run.
3. Atomically swaps the table's run list to the new run.
4. Old runs become eligible for GC (space is reclaimed on the next
   `gc()` pass).

Compaction is **crash-safe**: the old runs stay on disk until the new one
is fsync'd and the manifest is persisted. A crash mid-compaction leaves
the pre-compaction state intact.

## Flush-on-close

For short-lived processes (CLI invocations, one-shot scripts), MongrelDB
provides an explicit close that force-flushes pending writes to a `.sr`
run before exit. This keeps WAL segments bounded - without it, each
process leaves unflushed WAL data that accumulates across invocations.

The CLI calls `close()` automatically after every write command. In
application code:

```rust
// Rust (Kit)
db.close()?;  // force-flush + exit

// Rust (core)
db.close()?;  // same - sweeps all tables
```

```typescript
// The CLI handles this; for long-running Node processes it's unnecessary
// (the daemon auto-compactor covers run management).
```

```python
# Python (Kit)
db.close()
```

## GC

Orphaned runs (left after compaction or table drops) and stale WAL
segments are reclaimed by `gc()`. The daemon runs this periodically; for
embedded/CLI usage, call it after compaction or on startup:

```sh
mongreldb-kit-cli doctor /path/to/db  # checks integrity + runs GC
```

```rust
let reclaimed = db.gc()?;
```
