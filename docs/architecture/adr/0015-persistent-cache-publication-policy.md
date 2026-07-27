# ADR 0015 — Persistent Result-Cache Publication Failure Policy and Generation Identity

**Status:** Accepted
**Date:** 2026-07-26
**Amends:** [0013](0013-persistent-result-cache-publication.md) (removes the implicit
synchronous-fallback reading; keeps every other invariant)
**Closes:** REM-D (query-thread synchronous persistent-cache fallback) and REM-E
(future-generation cache frames accepted) from the dc596e6 remaining-issues audit

## Context

ADR-0013 established that the persistent result cache is disposable optimization
state and that the query thread never waits for publication. The implementation,
however, kept an implicit `Option<writer>` on `ResultCache`: when the background
writer was absent — worker spawn failure, worker I/O-setup failure, or post-shutdown
operation — `persist_entry` fell back to a **synchronous** `store_to_disk` on the
query thread (bincode serialize → optional encrypt → temp write → fsync → rename →
directory sync). That reintroduced exactly the 1–10 ms query-path stall ADR-0013
rejected, at the least predictable moments (degraded startup, shutdown overlap).

Separately, the frame loader rejected only frames *older* than the table's logical
generation (`frame_generation < expected`). A frame with a *future* generation was
accepted. Future frames exist after backup restore, PITR restore, manual `_rcache`
copies, and partial filesystem rollbacks; such a frame can contain rows produced by
commits that do not exist in the restored table, so serving it returns data from the
future relative to the table.

## Decision

1. **Explicit publication state.** `ResultCache` carries a
   `PersistentPublicationState` — `Async(Arc<PersistentResultCacheWriter>)` or
   `Disabled(PersistenceDisabledReason)` with reasons `NoDirectory`,
   `WorkerSpawnFailed`, `WorkerShutdown`, `QueueUnavailable` — replacing the implicit
   `Option<writer>` semantics.

2. **No query-thread synchronous publication, ever.** When publication is
   `Disabled`, `persist_entry` keeps the in-memory entry, skips the persistent
   write, increments `result_cache_persist_skipped_total`, and records
   `result_cache_persist_skipped` / `result_cache_persist_skip_reason` on the query
   trace. The in-memory tier remains fully functional; the loss of the durable copy
   degrades to a recompute on the next miss. `query_cached` and
   `query_columns_native_cached` contain no disk-write call.

3. **A single synchronous publisher remains for maintenance.**
   `persist_entry_synchronously_for_maintenance` (same MLCP frame format, same
   encoder) is kept for migration tools, explicit administrative flushes
   (`Table::_persist_cached_entry_synchronously_for_maintenance`), and focused
   format-parity tests. It is never called from the query path.

4. **Exact logical-generation identity on load.** The loader accepts a frame only
   when `frame.header.run_generation == expected.logical_generation`. Older frames
   are stale; future frames indicate a restore/rollback anomaly. Both are rejected
   with `CacheLoadRejection::GenerationMismatch` and removed best-effort; the
   rejection never fails the query and never increments the disk-hit counter.
   Accepting a future generation requires a separately designed restore protocol.

5. **Observability.** Four counters, exposed through `LookupMetricsSnapshot`, the
   server's Prometheus `/metrics` exposition, and the query trace:
   - `result_cache_persist_unavailable_total` — transitions of publication to a
     `Disabled` state (spawn failure, worker shutdown);
   - `result_cache_persist_skipped_total` — query-path persist operations skipped
     because publication was disabled;
   - `result_cache_worker_spawn_failures_total` — worker spawn / I/O-setup failures;
   - `result_cache_worker_shutdown_total` — worker shutdowns while the cache was live.

## Alternatives Considered

- **Keep the synchronous fallback but bound it (size cap, timeout).** Still puts
  filesystem latency on the query thread exactly when the system is degraded.
  Rejected; the persistent cache is disposable and a recompute is the correct
  degradation.
- **Fail table open when the worker cannot start.** A cache-tier problem would
  become an availability problem. Rejected; the cache is an optimization, not
  authoritative state.
- **Accept future generations and trust invalidation.** Invalidation generations
  are process-local; after a restore they cannot recognize rows from abandoned
  commits. Rejected; exact identity is the only sound rule without a restore
  protocol that explicitly carries cache frames forward.
- **Bumping the frame format version to signal strictness.** Unnecessary: the
  strict check is purely loader-side policy over the unchanged MLCP layout, and
  every previously-written frame remains valid under it (writers always stamped
  the current generation, so equality held for all legitimate frames).

## Consequences

- Query latency is insulated from cache-tier failure modes in both directions:
  a missing worker costs nothing per query, and a stalled worker already cost
  nothing (ADR-0013).
- A table whose worker failed to start silently loses warm restart reuse until
  restarted; the spawn-failure and skip counters make this visible instead of
  silent.
- After any restore/rollback that rewinds the table epoch, all pre-restore cache
  frames are rejected and deleted on first open — the intended behavior, at the
  price of a cold cache (recompute), which is the safe default.
- `Database::close` (worker shutdown) now transitions publication to
  `Disabled(WorkerShutdown)`; any post-shutdown insert keeps only its in-memory
  entry. Tests that relied on the post-shutdown synchronous write must use the
  maintenance API instead.

## Migration

No on-disk format change: the MLCP frame layout is untouched, and every frame
written before this change satisfies exact-generation equality (writers stamped
the current table generation). Older directories benefit from the stricter loader
automatically; previously-accepted future frames are deleted on first open.
Operational dashboards should adopt the four new counters; the pre-existing
`result_cache_persist_*` series are unchanged.

## Reversal Strategy

The state enum is additive: restoring the fallback would mean re-adding a
synchronous branch in `persist_entry` (a localized, reviewable change), but the
exact-generation check must not be relaxed without a designed restore protocol,
since doing so re-opens a wrong-result path after restores. Reverting the metrics
is a pure observability change with no data-format impact.
