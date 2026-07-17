# ADR-0006: Tablet Partitioning Model

- Status: Accepted
- Date: 2026-07-17
- Spec references: §12.1 (Stage 3A meta control plane), §12.2 (Stage 3B partitioning), §12.5/§12.6 (tablet split/merge)

## Context

Stage 3 partitions one logical database across independently replicated
tablets (spec §12). Every routing, split/merge, placement, and distributed
transaction decision in stages 3–4 depends on how table rows map to tablets,
so the partitioning model must be decided before any durable tablet format is
written.

Facts of the current (v0.59.0) storage engine that constrain the model:

- **Shared `RowId` space.** `crates/mongreldb-core/src/rowid.rs` defines
  `RowId(pub u64)`, allocated monotonically per table by `RowIdAllocator`,
  never reused. Every index of a table (primary HOT, learned PGM, secondary
  bitmaps, ANN, FM-index) resolves to or from `RowId`, and multi-condition
  queries intersect in that single id space.
- **WITHOUT ROWID derivation.** Clustered (WITHOUT ROWID) tables derive
  `RowId` from a single-column primary key with FNV-1a 64-bit
  (`derive_clustered_row_id`, `crates/mongreldb-core/src/engine.rs:3025`),
  clamped to a minimum of 1. The derivation is deterministic across runs and
  processes.
- **No partitioning metadata exists.** The catalog today records tables,
  columns, indexes, users, and grants — nothing about partition keys or
  tablet bounds.
- **The meta control plane** (spec §12.1) owns `TabletDescriptor` records
  carrying `partition: PartitionBounds`, so partition identity is cluster
  metadata, not local storage state.

The model must preserve the index-intersection invariant (conditions
intersect in `RowId` space) while allowing a table to span many tablets, each
of which owns an independent slice of the table's rows.

## Decision

Adopt **tablet partitioning with a declared partition key per table**, using
the four partitioning strategies of spec §12.2:

```rust
enum Partitioning {
    Hash {
        columns: Vec<ColumnId>,
        buckets: u32,
    },
    Range {
        columns: Vec<ColumnId>,
        splits: Vec<Key>,
    },
    Tenant {
        tenant_column: ColumnId,
        buckets_per_tenant: u32,
    },
    TimeRange {
        timestamp_column: ColumnId,
        interval: TimeInterval,
    },
}
```

Rules (normative, from spec §12.2):

1. **Every table has a declared partition key.** There is no unpartitioned
   distributed table. A table that declares nothing gets an automatic default
   (see rule 4), never an implicit "whole table on one tablet forever"
   passthrough.
2. **The primary key should include the partition key.** This is the fast
   path: point reads and writes route to exactly one tablet from the key
   alone. Primary keys that omit the partition key are permitted but take the
   slow path (fan-out lookup or a global index), and the planner must surface
   that cost.
3. **Colocation.** Related tables may declare colocation, which constrains
   the placement layer (spec §12.7) to keep matching partitions of the
   colocated tables on the same nodes so joins and multi-table transactions
   stay tablet-local where possible.
4. **Automatic defaults are visible in schema metadata.** When MongrelDB
   chooses a default partitioning (for example hash on the primary key with a
   default bucket count), the chosen `Partitioning` value — not the fact that
   it was defaulted — is written into the table's catalog entry and returned
   by schema inspection. Users must be able to see and reason about the
   effective partitioning; hidden defaults are prohibited.

RowId implications (decided here because §12.2 leaves them open):

5. **`RowId` becomes tablet-scoped.** Each tablet owns its rows and its own
   `RowId` allocation for non-clustered rows; the single-id-space
   intersection invariant of `rowid.rs` is preserved *within a tablet*, which
   is the only scope where local index generations (spec §12.3) intersect
   conditions. Cross-tablet query fragments combine results by key/tuple, not
   by intersecting remote `RowId` bitmaps.
6. **Global row identity is `(TabletId, RowId)`.** Where a stage-3+ component
   needs a globally unique row reference (CDC, distributed constraints,
   backup/PITR), it uses the tablet-qualified pair. `RowId` alone is never
   interpreted outside its owning tablet.
7. **WITHOUT ROWID stays FNV-1a derived.** Clustered tables keep the
   deterministic PK-derived `RowId`; because the derivation is pure, the same
   primary key yields the same `RowId` on any replica or after a tablet
   split, which keeps clustered rows stable across topology changes. The
   partition key of a clustered table is derived from its primary key.

Tablet bounds are stored in `TabletDescriptor.partition` (spec §12.1) and
changed only through the meta Raft group; split (§12.5) and merge (§12.6)
operate on these bounds and never on ad-hoc local state.

## Alternatives Considered

- **Hash-only partitioning.** Simple and uniform, but destroys range scans
  and time-ordered locality, which are core MongrelDB workloads (sorted runs,
  time-series and audit-style tables). Rejected as the sole strategy; kept as
  one of the four variants and the usual default.
- **Range-only partitioning.** Preserves scan locality but requires accurate
  split keys up front and hot-spots on monotonically increasing keys (the
  classic time-series write hotspot). Rejected as the sole strategy.
- **Single global `RowId` space across tablets** (e.g., interleaved id ranges
  or a global allocator). Would preserve today's invariant verbatim, but a
  global allocator is a throughput bottleneck and a availability liability,
  and pre-assigned ranges break under split/merge. Rejected in favor of
  tablet-scoped ids with `(TabletId, RowId)` global identity.
- **Composite 128-bit row identifiers embedding the partition.** Makes every
  id globally unique without qualification, but doubles the id width in every
  index, bitmap, and WAL record, and still does not survive merge (partition
  identity baked into the id goes stale). Rejected.
- **Application-level sharding (no declared keys).** Pushes routing into
  every client and makes colocation, rebalancing, and distributed SQL
  impossible to plan. Violates §12.2 rule 1. Rejected.
- **Directory-based partitioning (per-row lookup table).** Maximally flexible
  but turns the meta group into a per-row metadata hotspot and defeats
  key-based routing. Rejected.

## Consequences

Positive:

- Routing, split/merge, rebalancing, and distributed SQL planning all read
  one declarative model from the catalog; no hidden per-table behavior.
- The four variants cover the dominant workloads: uniform spread (Hash),
  ordered scans (Range), multi-tenant isolation (Tenant), and time-series
  rollover (TimeRange).
- Visible defaults keep `CREATE TABLE` simple for new users while remaining
  inspectable and reversible-by-redeclaration for operators.
- Colocation gives the join planner and 2PC (ADR-0007) a locality lever
  without promising single-tablet transactions.
- Tablet-scoped `RowId` preserves the existing engine invariants verbatim
  inside each tablet: memtable, sorted runs, and all six index kinds keep
  intersecting in one id space with no changes to their local algorithms.

Negative / costs:

- Cross-tablet secondary indexes and global unique constraints become
  distributed problems (spec §12.9), no longer local bitmap lookups.
- Primary keys that omit the partition key pay fan-out or global-index cost;
  this must be documented and surfaced by the planner, or users will build
  slow tables by accident.
- `RowId` is no longer meaningful across a whole table: any external system
  (CDC consumers, backups, client-side caches) that today stores bare
  `RowId` values must move to `(TabletId, RowId)`.
- Tenant and TimeRange partitioning couple the catalog to tenant lifecycles
  and wall-clock intervals; both need background jobs (bucket allocation,
  interval rollover) that must be failure-atomic.

## Migration

1. Pre-Stage-3 releases remain single-tablet-per-table internally: the
   catalog gains the `Partitioning` field early, populated with the visible
   automatic default, while data layout is unchanged. This lets schema
   metadata, clients, and tests stabilize before any data moves.
2. Existing v0.59.0 databases adopt their default partitioning at open/upgrade
   time through the normal catalog migration path; the adopted value is
   written into schema metadata (rule 4) and is inspectable.
3. When Stage 3 lands, tables gain additional tablets through split
   (§12.5); no rewrite of existing sorted runs is required because `RowId`
   is tablet-scoped and the initial tablet's ids remain valid within it.
4. WITHOUT ROWID tables require no id migration: FNV-1a derivation is
   topology-independent.
5. External consumers of bare `RowId` (CDC records, backups) are versioned to
   carry `(TabletId, RowId)` before the first multi-tablet table is allowed.

## Reversal Strategy

- **Before any table gains a second tablet:** reversal is a catalog change —
  drop the `Partitioning` metadata and the (still single) tablet mapping.
  No user data moves.
- **After splits exist:** reversal requires merge (§12.6) back to one tablet
  per table. Merge is a supported, tested operation in the target
  architecture, so reversal is operationally expensive but not destructive;
  `(TabletId, RowId)` references collapse back to the surviving tablet's ids.
- **Not reversible:** external systems that consumed tablet-qualified CDC or
  backup formats cannot un-learn the qualification. The model's durable
  formats (catalog `Partitioning`, `TabletDescriptor.partition`,
  tablet-qualified row references) are therefore gated behind the FND-001
  definition of done: senior-maintainer approval of this ADR before the
  formats merge.
