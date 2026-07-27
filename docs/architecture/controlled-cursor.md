# Controlled-Cursor Streaming Design (TODO §3)

**Status:** Accepted. The memtable leg is a true lazy cursor (REM-C): the
per-segment Bε-tree streams are lazy ordered cursors with bounded,
per-child buffer groups — no tree-wide collection or sort exists anywhere
on the controlled-scan path.

## Problem

`Table::for_each_visible_row_controlled` at `ca3d0b2` builds a complete
`BTreeMap<RowId, Row>` of newest visible rows before draining it in 256-row
batches. The active merge buffer is bounded; the upstream materialization is
not. Batching a completed map is not true streaming.

REM-C found the same flaw one level down: the memtable segment stream
(`LeafVersions::new`) walked the complete Bε-tree, pushed every leaf row and
every buffered message into one `Vec<Cow<Row>>`, sorted it, and only then
emitted the first row — O(N log N) constructor time and O(N) cursor-owned
memory for N memtable versions, with no cancellation during construction.

## Solution

Two borrowing cursor levels expose newest-visible versions in ascending
order under the supplied `Snapshot`:

```rust
struct BeTreeVersionCursor<'a> { /* lazy ordered Bε-tree cursor */ }
struct MemtableVisibleVersionCursor<'a> { /* k-way merge over segments */ }
struct MutableRunVisibleVersionCursor<'a> { /* pma::Pma iter */ }
```

`BeTreeVersionCursor` exploits the Bε-tree's ordered, non-overlapping child
key ranges. Each internal node assigns every buffered message to the one
child that owns its key (`child_index(keys, message.key())`), sorts only
that bounded per-child group (bounded by `BUFFER_CAP`), and two-way merges
the group against the live child cursor. Children are processed left to
right; a new child cursor is constructed only after the previous key range
is exhausted. No tree-wide vector or sort is ever built, so the first row
is available after one root→leaf descent.

`MemtableVisibleVersionCursor` k-way merges the per-segment
`BeTreeVersionCursor` streams: one heap head per segment, pop the lowest
`RowId`, gather every head sharing it, emit the newest visible version
(`Snapshot::version_is_newer`) exactly once per `RowId`. Heap heads are
pulled lazily on the first advance, so cursor construction is O(segments)
and a pre-cancelled control is observed before any version streams.

## Equal-key determinism

The full emission order is `(RowId, Epoch, source_priority, sequence)`:

- `source_priority`: `0` for a buffered message, `1` for a leaf row. On an
  exact `(RowId, Epoch)` tie the buffered message — the more recent
  mutation, since buffer contents postdate the leaves below the node — is
  emitted before the leaf-resident row.
- `sequence`: the message's insertion index within its node buffer, so ties
  inside one buffer resolve in insertion order.

This matches the pre-cursor stable-sort behavior and never depends on heap
addresses or timing: repeated scans over the same tree yield identical
streams.

## Instrumentation

`BeTreeVersionCursorStats` (per cursor node, aggregated over the live chain;
finished subtrees fold into their parent):

```
active_frames / peak_active_frames          live frames on the root→leaf path
buffered_messages_owned (+ peak)            bounded by height × BUFFER_CAP
total_versions_precollected                 always 0 (regression tripwire)
versions_examined                           emissions per cursor node
checkpoints                                 ExecutionControl checkpoints made
```

## Properties

1. **Bounded memory** by:
   - tree depth × node buffer capacity (per-segment Bε-tree cursor)
   - one current item per segment
   - the versions for the current `RowId` (a small buffer)
   - the configured merge batch

2. **Cancellation** by `ExecutionControl::checkpoint`:
   - before descending into a new child (which sorts that node's buffer
     groups)
   - every 256 versions emitted per cursor frame
   - every 256 gathered heads inside a large same-`RowId` group
   - before every segment refill
   - before emitting a row

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
- The memtable segment stream no longer collects or sorts every version:
  `total_versions_precollected == 0`, and the first row streams after one
  root→leaf descent rather than a full tree walk plus sort.
- Increasing history from 100k to 1M versions: no proportional transient
  allocation beyond the storage already owned by the memtable/mutable run.
- Cancellation observed within 256 examined versions or one segment refill,
  whichever comes first — including during tree traversal and large
  same-`RowId` gathers.

## Rejected alternatives

- **Pre-collect then chunk** (the old implementation). Allocated a full
  newest-row map before draining. Hidden memory spike.
- **Collect-and-sort per segment** (the pre-REM-C `LeafVersions`). Hid a
  second full metadata structure proportional to memtable history inside an
  apparently streaming cursor. Replaced by the lazy per-child merge.
- **Two-pass**: first count, then allocate. Doubles the work and still hits
  the same peak. Rejected.
- **Per-segment visitor**: gives up the k-way merge ordering, which the
  k-way merge is paying for. Rejected.
