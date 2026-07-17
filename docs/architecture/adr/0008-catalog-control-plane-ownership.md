# ADR-0008: Catalog/Control-Plane Ownership

- Status: Accepted
- Date: 2026-07-17
- Spec references: §9.1 (FND-001), §12.1 (Stage 3A meta control plane), §10.6
  (Stage 1F catalog and online jobs), §4.6 (security invariant), §4.2/§4.4
  (ownership and durability authority)

## Context

Today MongrelDB is a single-node engine. Its catalog is persisted as a
checkpoint file at `<root>/CATALOG` (`crates/mongreldb-core/src/catalog.rs`,
`CATALOG_FORMAT_VERSION = 1`), rewritten atomically on DDL. The catalog already
carries security metadata: `users` with Argon2id password hashes, `roles`,
`require_auth`, and a `security_version` counter (catalog.rs:123-138). Two
facts already foreshadow the target design:

- Every catalog mutation is committed through the shared WAL as a versioned
  operation, and each DDL commit appends `DdlOp::CatalogSnapshot` — the exact
  post-commit catalog image — so recovery, PITR, and logical replication
  preserve all derived catalog mutations
  (`crates/mongreldb-core/src/wal.rs:172`).
- Open/restart already treats CATALOG as a cache: "CATALOG is only a
  checkpoint. Authentication must use the authoritative catalog after
  committed WAL DDL/security replay" (`database.rs:2044`), and "The WAL image
  is the authoritative PITR and replication delta; CATALOG is only its restart
  checkpoint" (`database.rs:2967`). Security revocation already propagates
  across open handles without reopening the storage core via the process-wide
  `SecurityCoordinator` gate and `security_version` refresh
  (`database.rs:1350`, `database.rs:2936`).

Cluster stages need metadata that a per-node file cannot authoritatively hold:
cluster membership, node descriptors and locality, database descriptors, table
schemas, tablet descriptors, replica placement and placement policies,
schema/index jobs, transaction status partitions, cluster settings, and
feature flags (§12.1). A local file provides no total order across nodes, no
linearizable metadata reads, and no way to make revocation of privileges
effective cluster-wide at a defined commit point. The invariants require one
committed log order per ownership domain (§4.2), a single durability authority
(§4.4), and authorization evaluated against current *replicated* security
metadata with revocation effective without reopening the storage core (§4.6).
§10.6 (S1F-001) states the rule directly: the current catalog file becomes a
checkpoint, not the sole authority.

## Decision

1. A dedicated meta Raft group owns the control plane. Its committed log is
   the sole authority for:

   - cluster membership
   - node descriptors and locality
   - database descriptors
   - table schemas
   - tablet descriptors
   - replica placement
   - placement policies
   - schema/index jobs
   - transaction status partitions
   - cluster settings
   - feature flags

   It does not contain user row data (§12.1).

2. All catalog mutations are expressed as versioned commands (FND-003
   `CommandEnvelope`, crates/mongreldb-log) proposed through the FND-004
   `CommitLog` abstraction and applied only after commit. The catalog state
   machine covers databases, tables/columns, indexes, users/roles/grants,
   RLS/masks, triggers/procedures, materialized views, resource groups, and
   job definitions (§10.6 S1F-001). Commands are idempotent via `command_id`.

3. The local `CATALOG` file is demoted — formally, not just in recovery — to a
   checkpoint/cache of applied catalog state. It is never consulted as the
   authority; it exists so a node can restart without replaying the entire
   meta log and so single-node modes keep working. Its encrypted, versioned,
   atomically-renamed on-disk format is unchanged.

4. Security metadata (users, roles, grants, RLS/masks, `require_auth`,
   `security_version`) is replicated through the same committed log as the
   rest of the catalog. Every node applies committed security commands to its
   in-memory authorization view and bumps `security_version`; revocation takes
   effect at apply time on every live handle without reopening the storage
   core (§4.6), generalizing today's `SecurityCoordinator` gate/version
   mechanism from process-shared to cluster-replicated.

5. The meta Raft group is separate from the tablet Raft groups. Metadata
   throughput, failover, and snapshots are decoupled from data groups, and the
   meta group can be bootstrapped before any tablet group exists.

6. Schema/index jobs are meta-group state with persistent states (`Pending`,
   `Running`, `Paused`, `Cancelling`, `Succeeded`, `Failed`, `RollingBack`)
   and follow the build-and-publish pattern: record pending definition, pin a
   snapshot, build hidden, catch up committed deltas, validate, publish
   atomically, release the old generation after pins drop (§10.6 S1F-002,
   S1F-003).

## Alternatives Considered

1. **Keep per-node CATALOG files coordinated by gossip or two-phase commit.**
   Rejected: no single committed order for metadata, metadata split-brain
   under partitions (violates §4.2 and §4.4), and no defined point at which a
   revocation is effective cluster-wide.

2. **Store control-plane state in a special system tablet replicated by an
   ordinary data Raft group.** Rejected: bootstrap circularity (placement
   metadata would be needed to locate the tablet that stores placement
   metadata), metadata availability coupled to data-group failover, and
   metadata write amplification. §12.1 prescribes a dedicated meta group.

3. **Delegate the control plane to an external consensus store (e.g. etcd).**
   Rejected: a second durability authority outside MongrelDB's commit log
   (violates §4.4's single-authority rule), an extra operational dependency,
   and a second system to back up, restore, and keep version-compatible.

4. **Raft only security metadata; keep the local file authoritative for
   schema.** Rejected: two sources of truth for the catalog, with schema and
   security state able to diverge; contradicts "checkpoint, not the sole
   authority" (§10.6).

## Consequences

Positive:

- One linearizable authority for all cluster metadata; DDL, placement, and
  feature-flag changes are totally ordered with respect to each other.
- Revocation of users/roles/grants is effective at commit/apply on every node
  without reopening storage cores (§4.6).
- Online schema jobs gain persistent, replicated state machines, so index
  builds and backfills survive node failure and restart.
- Restart semantics are uniform across modes: load checkpoint, replay
  committed log since the checkpoint, serve — the rule single-node recovery
  already follows today.

Negative / costs:

- The meta group is an availability dependency for DDL and for metadata cache
  misses; data-plane reads and writes continue on cached descriptors but a
  partitioned meta group blocks schema change and membership change.
- Cluster bootstrap must create the meta group before any tablet group.
- The meta log needs snapshots and compaction, and every catalog mutation
  must be re-expressed as a versioned command (the S1F-001 migration).
- Metadata read-your-writes and linearizable metadata reads must be
  implemented on top of the meta group (leader read or read-index), not
  assumed.

## Migration

1. **Stage 1F (§10.6, single node):** move logical catalog mutations into
   versioned commands flowing through the standalone commit log; the CATALOG
   file becomes checkpoint-only. This is behavior-preserving because the WAL
   `CatalogSnapshot` image is already the authoritative PITR/replication
   delta today (database.rs:2967), and open already rebuilds authorization
   state from committed WAL replay (database.rs:2044).
2. **Stage 2:** the same catalog commands flow through `RaftCommitLog` for
   the single-shard replica set; security-version propagation replaces the
   process-local coordinator between replicas.
3. **Stage 3A (§12.1):** bootstrap the dedicated meta Raft group; seed it
   from the existing catalog plus node/database descriptors; tablet groups
   register with it. The local CATALOG file remains the restart checkpoint of
   applied meta state on each node.

## Reversal Strategy

- Before Stage 3, reversal means disabling meta-group writes and continuing on
  the standalone commit-log path; the command and checkpoint formats are
  unchanged, so single-node operation is unaffected.
- After the meta group owns metadata, a node can always rebuild a standalone
  catalog by exporting applied meta state into the existing CATALOG checkpoint
  format and opening it single-node. Because the meta group never contains
  user row data (§12.1), reversal cannot lose row data; the worst case is
  recomputing placement.
- The versioned, encrypted CATALOG checkpoint writer is kept permanently in
  every mode, so downgrades to single-node or pre-cluster binaries always find
  a readable checkpoint. Format evolution follows ADR-0010, so a checkpoint
  written before any feature activation remains readable by the previous
  binary.
