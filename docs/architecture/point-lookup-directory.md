# Point-Lookup Directory Design

**Status:** Accepted (skeleton landed in PR A; full rebuild/checkpoint lands in PR B)
**Closes:** TODO §1.2-1.4 (Residual Closure, baseline `ca3d0b2`)

## Problem

`Table::get` at `ca3d0b2` iterates every `run_ref` and skips a run only when
`run_row_id_ranges` proves the `RowId` is outside its header min/max range. With
wide or overlapping ranges, the lookup opens every candidate run — O(active
runs) reader opens.

## Solution

A `RunLookupDirectory` keyed by `RowId` that stores a newest-first list of
`RunLocator` per row. Each `RunLocator` carries enough metadata to conservatively
decide whether a run can hold a version visible to a supplied `Snapshot`,
without opening the run.

```rust
struct RunLocator {
    run_id: u128,
    min_epoch: Epoch,
    max_epoch: Epoch,
    min_hlc: Option<HlcTimestamp>,
    max_hlc: Option<HlcTimestamp>,
    contains_unstamped_versions: bool,
}
```

Lookup flow:

1. consult `RunLookupDirectory::locate(row_id)` for the candidate set;
2. filter locators against the supplied `Snapshot` using conservative
   `Snapshot::observes_row` and `Snapshot::version_is_newer` predicates;
3. order by newest commit authority;
4. open only the surviving locators;
5. stop as soon as the best materialized version is provably newer than every
   remaining locator's upper bound.

## Property: derived, never authoritative

The directory is **derived** from the sorted-run system columns and shadowed by
the run-set fingerprint. Missing, stale, corrupt, or incomplete state must
fall back to the existing range-scan path:

- `RunLookupDirectory::locate` returns `None` → fall back;
- on-disk fingerprint does not match the manifest's active run set → fall back;
- any IO error reading the directory → fall back, increment metric.

The `directory_complete` flag is set on the `QueryTrace` only when the
directory served the lookup; the legacy path leaves it `false`.

## On-disk shape

Delta-encoded `RowId` keys (varint). For each key, a newest-first list of
`RunLocator` *ordinals* (which decode to `RunRef` via the manifest), not 128-bit
`run_id`s. Fingerprint of the active run-set + schema/index generation is
written alongside and re-validated on reopen.

The rebuild-and-publish ordering is:

1. construct the replacement directory state in memory;
2. publish the manifest;
3. atomically publish the derived checkpoint (temp → rename → optional dir sync).

A crash between manifest and directory publication leaves the old directory
valid; the next open rebuilds from the new manifest.

## Bounded run growth

The directory removes dependence on unrelated runs, but a single hot row can
still accumulate many version-bearing locators. We define:

- `MAX_L0_OVERLAPPING_RUNS`: a configurable cap (default 64).
- trigger compaction before the threshold is exceeded.
- enforce non-overlapping `RowId` ranges for levels above L0.
- invariant test: a table cannot remain indefinitely above the L0 overlap threshold.
- hot-key collapse test: old version locators collapse after retention pins advance.
- pinned snapshots retain all needed historical locators until their pins are released.

## Metrics

```
directory_lookup_hit
directory_lookup_fallback
directory_incomplete
directory_run_readers_opened
directory_early_stop_total
```

Tracing: `directory_complete`, `directory_candidates`, `run_range_rejects`,
`membership_filter_rejects`, `run_readers_opened`, `early_stop`,
`point_cache_hits`.

## Compatibility

`run_row_id_ranges` is preserved as a cheap fallback and sanity check. The
existing `get_run_opened` / `get_run_skipped` counters remain in their current
shape so comparability with prior benchmarks is preserved.
