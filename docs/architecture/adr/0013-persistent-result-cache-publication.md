# ADR 0013 — Persistent Result-Cache Publication

**Status:** Accepted
**Date:** 2026-07-25
**Supersedes:** ad-hoc comment in `crates/mongreldb-core/src/engine.rs:1522-1530` ("writes
the temp file, `flush()`, `rename` — fsync is the [write+rename] pair")
**Closes:** TODO §2.1 (Residual Closure, baseline `ca3d0b2`)

## Decision

The persistent result cache is **disposable best-effort** state, not crash-durable
state. The query thread never waits for persistent publication. Persistent files
serve only as a "warm" cache that can survive a process restart; their loss is
recoverable by recomputation, and a stale file is strictly worse than a missing
one.

The persistence path therefore satisfies the following contract:

1. The in-memory tier is the authoritative source for the current process.
2. Persistent files are written by a single background worker per database.
3. The query thread enqueues an `Arc`-shared `PersistableEntry` (no serialization,
   no encryption, no `write`, no `flush`, no `rename`) and returns immediately.
4. Background worker: serialize → optionally encrypt → write temp → `flush` →
   `rename` → optional directory sync → notify caller only via metrics.
5. Persistent state must be invalidated aggressively on schema, run/index
   generation, clear, or per-key invalidation. No stale file can be observed
   after invalidation.
6. Errors degrade to a recompute on the next query, never to a query failure.

## Why not durable

A persistent result cache is a write-amplification target on every result cache
insert. Making it durable (fsync the data, fsync the directory, replay the WAL
until published) buys at most a faster warm path on restart at the cost of:

- a per-insert synchronous write on the query thread (or a WAL entry for every
  result, which is even more expensive), and
- a shutdown path that must wait for the queue to drain before the process can
  exit, which materially delays restart.

The persistent cache is an optimization that recovers the cost of an unprimed
cold query. For the workloads that benefit (large analytical results reused
across queries), losing a few entries on unclean shutdown is far cheaper than
gating every result-insert on durable storage.

## Why not a thread per query

A thread per query is unbounded and a single hot key can fan out into thousands
of useless writes. The bounded coalescing pending map collapses repeated stores
for the same key, and a `Remove` supersedes a pending `Store`. Overload drops
`Store` only and never drops `Remove` or `Clear` semantics.

## Stale-resurrection prevention

Every persisted record carries a generation tuple:

```
(table_id, schema_id, run_generation, entry_generation, key, checksum)
```

On load, the worker rejects any record whose `(table_id, schema_id, run_generation)`
does not match the active table. The worker also re-checks the generation
*before publishing* so an older queued store cannot overwrite a newer
invalidate.

Per-key invalidation advances the key's `entry_generation`, enqueues a `Remove`,
and any older pending `Store` for the same key is discarded.

`Clear` advances the global `clear_generation` and invalidates every queued op
older than the clear.

## Shutdown semantics

`Database::close` calls `flush_persistent_cache(deadline)` and then
`shutdown_persistent_cache(deadline)`. The worker drains remaining ops until the
queue is empty or the deadline expires. The default deadline is 250 ms; if it
expires, the worker marks the abandoned stores and exits. Persistent cache is
disposable, so the loss is acceptable.

Worker teardown is safe during panic/unwind via a `Drop` guard that calls
`shutdown` and joins the thread.

## Metrics

```
result_cache_persist_enqueued_total
result_cache_persist_coalesced_total
result_cache_persist_dropped_store_total
result_cache_persist_remove_total
result_cache_persist_stale_store_skipped_total
result_cache_persist_errors_total
result_cache_persist_shutdown_abandoned_total
result_cache_persist_queue_depth
```

The deprecated `result_cache_persistent_write_us` is renamed to
`result_cache_persist_serialize_us` and recorded on the worker thread, not the
query thread.

## Consequences

- Restart reuse is delayed only by the shutdown deadline (250 ms by default),
  not by the publication latency.
- Persistent files are not fsync'd; a power loss can leave orphan `.tmp` files
  that the next open sweeps.
- Coalescing can drop a `Store` that would have been useful; the dropped-store
  counter exposes this.

## Rejected alternatives

- **Synchronous `write` + `flush` + `rename` on the query thread.** Adds 1-10 ms
  per insert depending on storage. Rejected.
- **WAL-anchored persistence.** Adds a WAL entry per result-cache insert. The
  result cache is not authoritative; the WAL is. Rejected.
- **fsync the data file and the parent directory.** Doubles the cost of the
  background write. Rejected; the persistent cache is disposable.
- **Per-query thread.** Unbounded concurrency. Rejected.
