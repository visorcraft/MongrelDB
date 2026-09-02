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

Daemon compaction does not reset the WAL. For a bounded recovery log on an
embedded or CLI deployment, checkpoint instead (the daemon must be stopped
so this process can take the lock):

```sh
0 3 * * * appuser mongreldb-kit checkpoint /var/lib/myapp/db
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

## Flush, VACUUM, and checkpoint

These three operations sound similar and are not interchangeable. WAL
garbage collection only drops a segment when every table's memtable **and**
mutable-run tier are empty. `flush()` moves the memtable into the mutable
run; that is durable, but the WAL still has to keep the records until the
mutable run spills to an immutable `.sr` file.

| Operation | What it does | Shrinks the WAL? |
|---|---|---|
| `flush()` / `Table::flush` | Commits pending writes and moves the memtable into the mutable-run tier | Only if every table also has an empty mutable run |
| `VACUUM` / `compact()` + `gc()` | Merges sorted runs, then reaps orphan files | Same rule: leftover mutable runs keep WAL segments alive |
| `PRAGMA wal_checkpoint` | Flushes live tables, then runs GC. Returns a SQLite-shaped `busy` / `log` / `checkpointed` row | Same as flush + GC; does **not** rotate the active segment |
| `Database::checkpoint()` | Force-flushes memtable **and** mutable run, compacts, reaps retired runs, then replaces the WAL with a fresh empty active segment | Yes. This is the operation that bounds recovery |

Use compaction to keep query latency flat. Use `checkpoint()` when the
on-disk WAL has grown and you want the next open to replay only a small
active segment.

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

Close is not a substitute for `checkpoint()`. Close flushes; checkpoint
also compacts and drops rotated WAL segments.

## Checkpoint

`Database::checkpoint()` (engine 0.64.18+) produces a stable on-disk image:

1. Force-flush every table, including the mutable-run tier, to durable `.sr`
   runs.
2. Compact any table that still has more than one run.
3. Reap retired runs that no snapshot still pins.
4. Publish a fresh empty active WAL and delete every older segment.
5. Remove dropped-table directories that are past the retention horizon.

The call takes the DDL lock and the replication barrier for the duration.
Credential-enforced databases require the `Ddl` permission. A crash in the
middle leaves the previous WAL intact; the new empty segment is published
only after every flush succeeds.

```rust
use mongreldb_core::Database;

let db = Database::open("/path/to/db")?;
db.checkpoint()?;
```

```typescript
// Node.js engine addon (@visorcraft/mongreldb)
db.checkpoint();

// Kit TypeScript (@visorcraft/mongreldb-kit), requires engine 0.64.18+
db.checkpoint();
```

```python
# Kit Python
db.checkpoint()
```

```sh
# Kit CLI
mongreldb-kit checkpoint /path/to/db
```

There is no HTTP `/checkpoint` route on `mongreldb-server`. Stop the daemon
and run checkpoint against the data directory, or call it from an embedded
handle that owns the lock. `PRAGMA wal_checkpoint` over SQL is the weaker
flush-then-GC path described above, not this WAL reset.

After checkpoint the directory is byte-stable enough for a filesystem copy
or `mongreldb snapshot`. It does not release the exclusive lock; drop or
`shutdown()` the handle when you are done.

## WAL recovery

On open, MongrelDB streams WAL records with incremental sequence and framing
checks. It does **not** load the whole log into one `Vec`. Recovery is
unlimited by default: a large WAL takes time and memory proportional to
replay, but open no longer fails just because the log is hundreds of
megabytes or millions of records.

A per-frame 64 MiB cap still treats a garbage length prefix as a torn write.
That bound is about corrupt frames, not total log size.

To fail closed on a runaway log (shared hosts, untrusted directories), set
optional caps. `0` and unset mean unlimited. An explicit `OpenOptions` value
overrides the environment.

```sh
# Fail open if the durable WAL is larger than 2 GiB, or has more than
# 10 million records. Either variable can be set alone.
export MONGRELDB_MAX_RECOVERY_WAL_BYTES=2147483648
export MONGRELDB_MAX_RECOVERY_WAL_RECORDS=10000000
```

```rust
use mongreldb_core::{Database, OpenOptions};

let opts = OpenOptions::default()
    .with_max_recovery_wal_bytes(2 * 1024 * 1024 * 1024)
    .with_max_recovery_wal_records(10_000_000);
let db = Database::open_with_options("/path/to/db", opts)?;
```

A cap that is exceeded returns `MongrelError::ResourceLimitExceeded` with
`resource` equal to `"WAL recovery bytes"` or `"WAL recovery records"`.
Shrink the log with `checkpoint()` (from a process that can still open the
database, or after raising/removing the cap for one open), then keep
checkpoint in the maintenance schedule so the cap is not a surprise later.

Kit and Node.js opens honor the environment variables. Kit `OpenOptions`
currently exposes lock timeout only; use the env vars or the engine
`OpenOptions` above.

Startup WAL replay is not point-in-time recovery. PITR archives are a
separate backup format; see [Point-in-Time Recovery](17-point-in-time-recovery.md).

## GC

Orphaned runs (left after compaction or table drops) and stale WAL
segments are reclaimed by `gc()`. The daemon runs this periodically; for
embedded/CLI usage, call it after compaction or on startup:

```sh
mongreldb-kit doctor /path/to/db  # checks integrity + runs GC
```

```rust
let reclaimed = db.gc()?;
```

`gc()` cannot drop WAL segments that are still the recovery source for
in-memory or mutable-run data. If `gc()` returns and the `wal/` directory
is still large, run `checkpoint()`.
