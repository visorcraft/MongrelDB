# Sharded Cluster: Tablets, Meta Control Plane, and Placement

Stage 3 of the [architecture program](18-architecture-foundations.md)
partitions one logical database across independently replicated **tablets**:
a dedicated meta Raft group owns the control plane, every table declares a
partition key, tablets carry the data, and a placement engine keeps replicas
balanced and quorum-safe. This page tours the machinery in
`mongreldb-cluster` and the Stage 3 gate surfaces (meta, tablets, dist txn,
gateway, cluster backup). The Stage 2 replication machinery it builds on
(one Raft group per database, durability levels, read consistency, failover)
is described in [Replicated High Availability](20-replicated-ha.md); the
single-node subsystems in
[Single-Node Subsystems](19-single-node-subsystems.md).

The normative decisions live in three ADRs:
[ADR-0006](architecture/adr/0006-tablet-partitioning-model.md) (tablet
partitioning model), [ADR-0007](architecture/adr/0007-distributed-transaction-protocol.md)
(distributed transaction protocol), and
[ADR-0008](architecture/adr/0008-catalog-control-plane-ownership.md)
(catalog/control-plane ownership).

## Meta control plane (spec §12.1)

A **dedicated meta Raft group** owns the cluster's control-plane state. It is
a `ConsensusGroup` like any other (same durable storage, same idempotent
apply), wrapped by `MetaGroup` (`mongreldb-cluster/src/meta.rs`) with a
`MetaApplySink` whose applied state is `MetaState`. Per spec §12.1 it owns
exactly:

- cluster membership (registered node descriptors and locality),
- database descriptors,
- table schemas (opaque schema documents + monotonic `schema_version`),
- tablet descriptors,
- replica placement (per raft group) and named placement policies,
- schema/index jobs (persistent state machine: `Pending`, `Running`,
  `Paused`, `Cancelling`, `Succeeded`, `Failed`, `RollingBack`),
- transaction status partitions (which raft group coordinates a shard of the
  distributed-transaction records),
- cluster settings (see below),
- feature flags (the cluster feature level and activation records, ADR-0010).

**It never contains user row data.** The rule is enforced in code: a
`Transaction`-kind command applied to the meta group fails closed, and only
`Catalog` envelopes stamped `COMMAND_TYPE_META_COMMAND` (discriminant `4`)
are accepted. Every mutating operation is a `MetaCommand` proposed through
`MetaGroup::propose` at quorum durability and applied deterministically on
every replica.

The apply path is **total**: a command that conflicts with state (stale
versions, missing references, illegal transitions, denied settings) is never
a raft error — it is refused with a typed `MetaRejectionReason`, journaled in
the bounded rejection journal (`META_REJECTION_LIMIT = 256`), and surfaced to
the proposer. Records carry last-writer-wins versions: `schema_version` for
schemas, `generation` for tablet descriptors, and a per-record
`metadata_version` that doubles as the optimistic-concurrency token of the
CAS-guarded `UpdateNodeState` / `UpdateSchemaJob` commands. Two operators
racing a read-modify-write on one record do not corrupt each other — the
higher version wins and the loser is refused — but note the model *is*
last-writer-wins: a stale-read rewrite that carries the newer version wins
silently, so admin tooling should always re-read before rewriting.

The sink checkpoints the full state to
`<node-data>/groups/<meta-group-id>/raft/state/meta-state.json` after every
entry (atomic temp-write + rename + directory fsync), so a restart replays
nothing, and the same record travels inside raft snapshots for follower
catch-up. State and command payloads are serde-versioned (current format
version 2); version-1 payloads from the first meta build still decode and
migrate at read time, and unknown versions fail closed.

### Cluster settings and the secrets denylist

`MetaCommand::SetClusterSetting` mutates the dynamic cluster settings
(spec §16.2). The typed keys are `history_retention_epochs`,
`backup.enabled`, `backup.interval_seconds`, `backup.retention_count`,
`default_consistency`, `ai.max_concurrent_requests`, `ai.max_memory_bytes`,
`jobs.max_concurrent`, and the dynamic shape `resource_groups.<name>` (a
`null` value removes the group). Unknown keys and wrongly-typed values are
refused.

**Secrets are never plaintext cluster settings.** A key containing
`secret`, `password`, `passwd`, `private_key`, `api_key`, `token`, or
`credential` (case-insensitive) is refused before any other check — the
denylist fires even for otherwise-unknown keys. TLS private keys, backup
credentials, and encryption-key material live in static node configuration
or the engine's key hierarchy, never in replicated meta state.

### Meta group membership workflows

`MetaGroup` drives the spec §12.7 movement protocol for its own membership:
`add_member` adds a learner, waits until it is line-rate, promotes it to
voter through joint consensus, then registers its node descriptor in
replicated state; `remove_member` applies `RemoveNode` first (refused while
any tablet or placement still references the node), then removes the voter —
leadership must be transferred off the node first, and removing the current
leader fails closed. Bootstrapping rejects collisions of the deterministic
64-bit raft-id projection of the 128-bit `NodeId`s, so two distinct nodes
can never alias inside a raft group.

## Tablet model (spec §12.1–12.4)

A tablet is one independently replicated key range of one table. Its
descriptor (`mongreldb-cluster/src/tablet.rs`) mirrors spec §12.1:

```text
TabletDescriptor {
    tablet_id, table_id, raft_group_id,
    partition: PartitionBounds,      // typed key range, low → high
    replicas: Vec<ReplicaDescriptor>,// distinct nodes; each carries its per-group raft_node_id
    leader_hint: Option<NodeId>,
    generation: u64,                 // bumped by every atomic publication
    state: TabletState,
}
```

The lifecycle graph is enforced in code (`Creating → Active | Retired`,
`Active → Splitting | Merging | Retiring`, `Splitting | Merging → Active |
Retiring`, `Retiring → Retired`; `Retired` is terminal and ids are never
reused). Only `Active`, `Splitting`, and `Merging` tablets are routable: a
split/merge source stays authoritative until the atomic publication flips it
to `Retiring`, and `Creating` tablets are never exposed before catch-up.
Structural validation (reserved ids, well-formed bounds, distinct replica
nodes and raft ids, leader hint must name a replica, at least one voter
outside `Creating`) runs on every persisted descriptor.

### Partitioning (spec §12.2, ADR-0006)

Every table has a declared partition key; there is no unpartitioned
distributed table. Four strategies exist (`Partitioning`):

- **`Hash { columns, buckets }`** — `fnv1a64(encoded key) % buckets`;
  tablets own contiguous runs of buckets.
- **`Range { columns, splits }`** — lexicographic ranges over the encoded
  key; `splits.len() + 1` partitions cover the key space.
- **`Tenant { tenant_column, buckets_per_tenant }`** — one bucket space per
  tenant, so tenants spread evenly and can be isolated onto dedicated
  tablets.
- **`TimeRange { timestamp_column, interval }`** — fixed-width time buckets
  (`timestamp / interval`); pre-epoch timestamps fail closed.

Partition keys are encoded with the order-preserving `RowKeyEncoder`
(sign-flipped big-endian integers, prefix-free escaped strings, cross-type
tag order), so typed ranges map onto plain byte ranges. `Partitioning::route`
maps a key to its slot, and `routed_key` produces the canonical byte address
the tablet bounds range over. The per-table `TablePartitioningRecord` keeps
the chosen partitioning visible in schema metadata — including when it was
derived automatically (`PartitioningOrigin::AutomaticDefault`), never
hidden — and an optional `ColocatedWith` declaration constrains placement to
keep matching partitions of related tables on the same nodes.

### Routing (spec §12.4)

Pure selection helpers turn the meta plane's descriptors into routes:
`find_tablet_for_key` routes point reads/writes directly,
`tablets_overlapping` fans a range query out to every overlapping tablet in
deterministic order, and `check_generation` verifies the generation a request
was routed with, classifying a mismatch as `TabletMoved`, `TabletSplit`, or
`StaleMetadata` (each mapping to its stable `ErrorCategory`). The gateway
refreshes metadata and retries safe operations on all three.

### On-node layout and ownership (spec §12.3)

```text
node-data/
  cluster-meta/                          identity, cluster/trust/join records
  tablets/<tablet-id>/{state,runs,indexes,temp}   + tablet.json
  groups/<raft-group-id>/raft                     log, vote, checkpoints, snapshots
```

`tablet.json` is versioned and checksummed; loading fails closed on a
missing, corrupt, unknown-version, or wrong-tablet file, and `create` is
idempotent but never silently repurposes a directory (`MetadataConflict`).
One tablet storage core is owned by one node process:
`TabletOwnershipRegistry` enforces the in-process half on canonical paths,
mirroring the Stage 1 shared-core registry; the storage core's file lease
(`_meta/.lock`) remains the cross-process half. Opening rules for replica
directories — including which open modes a `ClusterReplica` root accepts —
are documented in [Single-Node Subsystems](19-single-node-subsystems.md)
("Storage-mode marker").

## Placement and rebalancing (spec §12.7)

A `PlacementPolicy` declares, per table: `replicas` (voter count),
`voter_constraints` (hard/soft locality requirements),
`leader_preferences`, and `prohibited_nodes`. `validate_policy` refuses
unsatisfiable policies at declaration time (zero replicas, empty
constraints, or fewer eligible nodes than requested replicas).

The placement engine is deterministic — the same inputs always produce the
same decision, and input node order never matters:

1. **Eligibility** — `Up` nodes, not prohibited, holding no replica of the
   tablet, satisfying every required voter constraint; when load signals
   exist, only nodes that reported load (fail closed).
2. Fewest unsatisfied *optional* voter constraints.
3. **Failure-domain spread** — fewest replicas in the node's zone (falling
   back to region, then to the node itself, so node spread is the floor).
4. Least loaded by the composite `NodeLoad` score (disk, write/read
   throughput, CPU, memory, replica and leader counts, AI index memory —
   each normalized per-mille of the cluster maximum and summed).
5. `NodeId` ascending, the total-order tie-break.

`choose_leader` ranks `Up` voters by fewest unsatisfied optional leader
preferences (required ones filter), then `NodeId` ascending.
`check_move_safety` enforces the non-negotiable — **never reduce healthy
voters below quorum** (strict majority of the current configuration): a
direct 3→2 reduction passes (2 ≥ quorum(3)), a direct 2→1 reduction is
refused (1 < quorum(2)), so a two-voter group must grow before it may
shrink.

`plan_rebalance` turns placements plus reported loads into an ordered,
bounded plan: a node is hot when its composite score exceeds the mean by
more than `hot_threshold_per_mille` (default 1.25×), moves are capped by
`max_concurrent_moves` (default 1), only `Active` tablets move, and every
movement follows the add-first protocol — add learner, snapshot/catch up,
promote, transfer leadership when the source holds it, remove the old
replica last — judged against the post-promote configuration. A
single-voter tablet is therefore never auto-rebalanced. An idle or evenly
loaded cluster yields an empty plan.

## Cluster transport: mTLS (spec §14.3 direction)

The networked raft transport (`mongreldb-cluster/src/network.rs`) carries
the three raft RPCs plus an election trigger over TCP: one
`mongreldb-protocol` envelope frame per direction on a dedicated connection,
bounded payload (default 16 MiB), version and CRC checked, unknown message
types and dispatch failures answered with a distinct error frame — never
something parseable as a raft answer. Connect retries use bounded
exponential backoff; every read, write, and handshake is time-bounded;
inbound connections are capped (default 256, excess closed immediately).

Production deployments must run `TransportSecurity::Mtls`:

- TLS 1.3 only, both peers authenticate with certificates chaining to the
  cluster CA.
- A node certificate binds its durable 128-bit `NodeId` as the subject CN
  and a SAN dNSName of the form
  `node-<lowercase hex NodeId>.mongreldb.cluster`. The client passes that
  name as the TLS server name, so the server's identity is verified against
  the peer directory entry; the server verifies the client chain and then
  requires the certificate's CN or SAN to name an admitted node
  (`TrustConfig::allowed_node_ids`).
- Session resumption keeps the binding: rustls restores the client
  certificate chain from the ticket, so the identity check fires on resumed
  sessions too. Certificate expiry is enforced at the handshake; rotating
  certificates currently means restarting the transport.
- There is no plaintext negotiation to downgrade into: the security mode is
  static configuration per node, and a plaintext peer fails the handshake.
  `PlaintextForTesting` exists for loopback tests only — an unauthenticated
  raft port lets any process inject votes and entries into the group.

Trust material is PEM (CA + node cert + key) loaded from a trust directory
(`ca-cert.pem`, `node-cert.pem`, `node-key.pem`) and persisted to
`<db-dir>/cluster-meta/trust.json`; keep that directory on owner-only
storage. Operators may supply PEMs via `TrustConfig::from_pems`, or mint a
bootstrap CA and first node cert with `TrustConfig::generate`. Live rotation
uses `NodeRuntime::reload_trust` / `TcpTransport::reload_security` without a
full restart. The checked-in test fixtures under
`crates/mongreldb-cluster/tests/fixtures/` document the exact certificate
shape (EC P-256, `basicConstraints=CA:FALSE`, EKU serverAuth+clientAuth)
and regenerate with `./regenerate.sh` (OpenSSL 3.x); they are test-only
material.

## Operating a cluster

The operator surface landed in `mongreldb-server`:

- **CLI** — `mongreldb-server cluster init|join|status` and
  `mongreldb-server node drain|remove` bootstrap clusters and drive
  membership transitions; `node remove` requires a confirmation token the
  CLI prints on a tokenless first run.
- **Admin endpoints** — `GET /admin/cluster/status`,
  `POST /admin/cluster/node/drain`, `POST /admin/cluster/node/remove` on the
  authenticated daemon.

Flags, request/response shapes, the token flow, and audit behavior are in
[Daemon Mode](08-daemon.md#cluster-administration).

## Node runtime (spec §6.6, integration)

`NodeRuntime` (`mongreldb-cluster/src/runtime.rs`) is the library form of a
running cluster node. Starting one loads the persisted `NodeIdentity`,
builds the `TcpTransport` client and `TransportServer` listener and feeds
the membership directory into the peer table, starts the meta group when
the node is a meta member, and reopens every tablet group found under
`tablets/<tablet-id>/tablet.json` — one `ConsensusGroup` per group, each
registered in the transport's dispatch registry under its meta-allocated
`raft_node_id` (never the node-id projection, which the meta group already
occupies). Shutdown drains the listener first, then stops every group
(fsync, raft stop), then releases the tablet ownership guards.

Each tablet group binds a `ClusterReplica` core. Database id resolution
(`resolve_tablet_database_id`): non-zero `TabletDescriptor.database_id` when
the publisher stamped it into tablet metadata, else a deterministic
raft-group-derived id (stable across restarts and identical on every
replica without consulting meta — non-meta nodes would otherwise diverge).

## Tablet split and merge (spec §12.5/§12.6)

The split and merge executors (`split.rs`, `merge.rs`) implement the spec's
safe protocols as explicit phase machines over seams (`TabletMetaPlane`,
`TabletKeyspace`, `ChildStateSink`) the runtime binds to the engine:

- **Split.** Meta marks the source `Splitting`; children are created as
  learners (`Creating`, never routed); the source snapshot is pinned at the
  split timestamp; child state is built staged and caught up from source
  deltas; a publication barrier blocks until caught up; one atomic meta
  command publishes the children `Active` and the source `Retiring`
  together; stale requests are redirected by generation checks; the source
  is retained until no old-generation pins remain, then removed
  (`Retired` + layout teardown).
- **Merge** mirrors it for two adjacent, compatible tablets (same
  table/schema, adjacent ranges, compatible placement, no conflicting
  schema job, combined size under threshold): both sources keep serving
  (`Merging` is routable) until one atomic command publishes the hidden
  `Active` replacement and retires both sources.

Every phase is idempotent and persisted in a versioned, checksummed
progress record (`split.json` / `merge.json` in the tablet directory), so a
crashed executor resumes from the durable phase; fault hooks bracket the
atomic publication and fire after each phase. The generation rules (source
marked at `g + 1`, publication at `g + 2`, retirement at `p + 1`; for
merge, `m = max` of the two source generations) keep every stale request
classifiable as `TabletSplit`/`TabletMoved`/`StaleMetadata`.

## Distributed transactions (groundwork)

`mongreldb-cluster/src/dist_txn.rs` carries the spec §12.8 two-phase-commit
protocol: a replicated coordinator record per transaction (a terminal
decision is final), durable per-participant write intents, deterministic
command ids so client retries converge, commit timestamps strictly greater
than every observed timestamp, heartbeat-expiry push rules (never abort on
suspicion), and lazy orphan-intent resolution. The protocol state machines
and group flows are unit- and group-tested (idempotent replays, prepare
conflicts, coordinator failover recovery, expiry pushes, orphan sweeps),
and the engine binding materializes committed intents into MVCC row
versions via `apply_replicated_records` at `commit_ts`. Server gateway
routing of multi-tablet writes goes through tablet groups via
`RoutingCache`/`RetryPolicy` (never a file-open bypass). Multi-node
operation uses `NodeRuntime` (identity, mTLS transport, meta + tablet
group lifecycle) and §15 admin SQL for drain, leader transfer, movement,
split/merge, and backup/restore.

## Landed Stage 3 residual (S3L + gateway)

- **Cluster backup/PITR (§12.12).** `mongreldb-cluster::cluster_backup` —
  multi-tablet manifest, pin-meta → tablet snapshots → log archive →
  validate → **publish manifest last**, `BackupSource` trait (cluster stays
  core-free), `verify_backup`, restore plan with new cluster/database
  identity unless disaster recovery, fault hooks `cluster.backup.*`.
- **Server gateway + §15 admin SQL.** `mongreldb-cluster::gateway` binds
  distributed plan fragments to real tablet groups through
  `RoutingCache`/`RetryPolicy` (never opens tablet files from the query
  path). `SHOW CLUSTER/NODES/TABLETS/...`, `ALTER NODE DRAIN`,
  `TRANSFER LEADER`, `MOVE REPLICA`, `SPLIT TABLET`, `MERGE TABLETS`,
  `BACKUP/RESTORE DATABASE` parse into typed commands; the server
  intercepts them on the SQL path with admin authz + audit.
- **Global constraints, distributed SQL groundwork, distributed DDL,** and
  **intent→MVCC binding** for 2PC have also landed in prior Stage 3 waves
  (`global_constraints.rs`, `query::distributed`, `ddl.rs`, `dist_txn.rs`).

## Stage 4 / 5 surfaces (workload separation + production ops)

- Hierarchical scheduler (`mongreldb-core::scheduler`), node memory governor
  (`node_governor`), AI index generations (`ai_generation`), distributed AI
  retrieval merge (`mongreldb-query::ai_retrieval`), specialized replica
  roles, multi-region placement (`multi_region`), MySQL migrate path
  (`migrate_mysql`), security hardening (SCRAM/JWT/service tokens/KMS
  redaction), and online ops jobs (`ops_jobs`).
