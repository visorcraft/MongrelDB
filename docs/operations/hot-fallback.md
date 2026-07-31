# HOT Fallback Runbook

**Status:** Shipped (PR F, baseline `ca3d0b2`)
**Owners:** storage / observability

## What a HOT lookup is

MongrelDB serves primary-key lookups through a `Height-Optimized Trie` (HOT)
that maps the PK value to a `RowId`. A **hit** is when the HOT map returns a
`RowId` whose materialized row matches the PK and is visible, live, and
TTL-valid at the calling snapshot.

A **fallback** is everything else: the lookup falls through to the slower
overlay + sorted-run path. In a healthy current-snapshot workload, the fallback
rate should be 0. Any non-zero fallback is a regression.

## Reason taxonomy

Each fallback is tagged with exactly one stable reason:

| Reason | When it fires | Severity |
|---|---|---|
| `missing_mapping` | The HOT map has no entry for the PK. | **Investigation** — usually a missing index. |
| `stale_row_id` | The mapped `RowId` exists but belongs to a superseded version. | **Critical** — possible index corruption. |
| `invisible_at_snapshot` | The mapped row is invisible at the calling snapshot. | Expected for historical snapshots. |
| `historical_snapshot` | The query is explicitly bound to a historical snapshot. | Expected. |
| `tombstone` | The mapped row was deleted. | **Investigation** — stale HOT entry. |
| `ttl_expired` | The mapped row's TTL elapsed. | Expected for TTL workloads. |
| `primary_key_mismatch` | The materialized row's PK does not match the requested PK. | **Critical** — definite index corruption. |
| `index_incomplete` | Indexes were not yet built when the query fired. | **Investigation** — long-running rebuild. |
| `checkpoint_rejected` | The global HOT index checkpoint was rejected on reopen. | **Critical** — automatic rebuild required. |

## Inspecting counters

Read counters at the `/metrics` endpoint:

```bash
curl -s http://daemon:8080/metrics | grep -E '^(hot_lookup|hot_fallback|hot_mapping|hot_checkpoint)'
```

Healthy state on a current-snapshot workload:

```
hot_lookup_total{outcome="hit"} 1000000
hot_lookup_total{outcome="fallback"} 0
hot_fallback_total{reason="historical_snapshot"} 5   # admittance test
hot_fallback_total{reason="ttl_expired"} 12          # admittance test
```

Anything else is unexpected.

## What to do

| Symptom | Action |
|---|---|
| `hot_fallback_total{reason="primary_key_mismatch"}` > 0 | Page on-call. Run `rebuild_indexes` immediately. Do not delete the data directory first. |
| `hot_fallback_total{reason="stale_row_id"}` > 0 | Page on-call. Run `rebuild_indexes`. Compare HOT mapping with materialized PK in a trace. |
| `hot_fallback_total{reason="checkpoint_rejected"}` > 0 | Run `rebuild_indexes`. If rejected again, page on-call — the checkpoint file may be corrupt. |
| `hot_fallback_total{reason="index_incomplete"}` > 0 long-running | Check `hot_mapping_rebuild_total` and the rebuild log. Long-running rebuilds usually mean a large orphan catalog. |
| `hot_fallback_total{reason="missing_mapping"}` climbing | Check the index schema for the table. The HOT index may have been dropped. |
| Fallback rate > 0.1% for 5 minutes on current-snapshot traffic | Page on-call. This is the dashboard alert. |

## Commands

```bash
# Inspect table metrics (per-table LookupMetricsSnapshot as Prometheus text)
mongreldb-server table-metrics <db_dir>

# Run rebuild_indexes on every live table
mongreldb-server table-rebuild-indexes <db_dir>

# Capture a traced PK query (per-table lookup metrics snapshot)
mongreldb-server trace-query <db_dir>

# Check global index checkpoint state (mapping_rebuild vs checkpoint_rejected counters)
mongreldb-server index-checkpoint-state <db_dir>

# Compare HOT mapping with materialized PK
mongreldb-server index-hot-compare <db_dir> <table> --pk=<pk>
```

## Capturing a trace

A traced PK query reports `hot_lookup_attempted`, `hot_lookup_hit`,
`hot_fallback_reason`, and the fallback work counters:

```bash
mongreldb-server trace-query <db_dir>
```

A healthy hit:

```
hot_lookup_attempted=true hot_lookup_hit=true hot_lookup_nanos=420
```

A fallback with full work:

```
hot_lookup_attempted=true hot_lookup_hit=false
hot_fallback_reason=tombstone
hot_fallback_overlay_versions=2 hot_fallback_runs_considered=3
hot_fallback_runs_opened=1 hot_fallback_pages_decoded=4
hot_fallback_rows_materialized=1 hot_fallback_nanos=28000
```

## When fallback preserves correctness vs when it indicates corruption

- **Preserves correctness, harms latency:** `historical_snapshot`,
  `invisible_at_snapshot`, `ttl_expired`, `tombstone`, `missing_mapping`,
  `index_incomplete`. The fallback path produces the same row as the HOT path
  would have, but slower.
- **Indicates corruption:** `primary_key_mismatch`, `stale_row_id`,
  `checkpoint_rejected`. The HOT map is wrong. Rebuild.
