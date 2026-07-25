# Controlled-Cursor Streaming Design (TODO §3)

**Status:** Accepted (skeleton landed in PR A + MutableRun cursor in PR D; full
k-way merge + 4-caller refactor lands in PR D as a follow-up).

## Problem

`Table::for_each_visible_row_controlled` at `ca3d0b2` builds a complete
`BTreeMap<RowId, Row>` of newest visible rows before draining it in 256-row
batches. The active merge buffer is bounded; the upstream materialization is
not. Batching a completed map is not true streaming.

## Solution

Two new borrowing cursor types expose a `(RowId, Epoch, &Row)` iterator in
ascending `(RowId, Epoch)` order, deduped to the newest visible version per
`RowId` under the supplied `Snapshot`:

```rust
struct MemtableVisibleVersionCursor<'a> { /* memtable::BeTree iter */ }
struct MutableRunVisibleVersionCursor<'a> { /* pma::Pma iter */ }
```

Both yield `&Row` references with lifetime `'a` borrowed from the underlying
storage; no row bytes are cloned or owned.

## Properties

1. **Bounded memory** by:
   - one current item per segment
   - the versions for the current `RowId` (a small buffer)
   - the configured merge batch

2. **Cancellation** by `ExecutionControl::checkpoint`:
   - before every segment refill
   - at least every 256 examined versions
   - before emitting a row
   - before expensive HLC comparisons

3. **Mixed stamped/unstamped** visibility uses the existing
   `Snapshot::observes_row` and `Snapshot::version_is_newer` predicates.
   Legacy unstamped rows fall back to epoch.

4. **Cross-source k-way merge** via a `BinaryHeap<HeapEntry<'a>>` ordered by
   `(RowId, Epoch)`. Each segment's iterator is advanced lazily.

## Caller refactor

`for_each_visible_row_controlled`, `visible_rows_at_time`,
`native_page_cursor`, `native_multi_run_cursor`, and `overlay_visible_rows`
all switch from `Vec<Row>` / `BTreeMap<RowId, Row>` materialization to the
streaming cursor pair. The visitor receives `(RowId, Epoch, &Row)` tuples
directly.

## Trace fields

`QueryTrace` records:

```
controlled_scan_versions_examined
controlled_scan_rows_emitted
controlled_scan_source_refills
controlled_scan_peak_source_buffer_rows
controlled_scan_peak_same_row_versions
controlled_scan_checkpoints
controlled_scan_time_to_first_row_us
controlled_scan_cancel_latency_us
```

The peak values are clamped to:

```
O(number of source segments + versions for current RowId + configured batch)
```

which is independent of total live rows and total history depth.

## Consequences

- 1M live in-memory rows: no `BTreeMap<RowId, Row>` or `Vec<Row>` intermediate
  appears on the controlled-scan path.
- Increasing history from 100k to 1M versions: no proportional transient
  allocation beyond the storage already owned by the memtable/mutable run.
- Cancellation observed within 256 examined versions or one segment refill,
  whichever comes first.

## Rejected alternatives

- **Pre-collect then chunk** (the old implementation). Allocated a full
  newest-row map before draining. Hidden memory spike.
- **Two-pass**: first count, then allocate. Doubles the work and still hits
  the same peak. Rejected.
- **Per-segment visitor**: gives up the k-way merge ordering, which the
  k-way merge is paying for. Rejected.
