# Single-Node Subsystems

Stage 1 of the [architecture program](18-architecture-foundations.md) builds
the production single-node server machinery inside `mongreldb-core`. This
page tours the subsystems that have landed, shows how to observe them, and
states plainly what is and is not wired into the engine yet. None of them
change the on-disk database format: existing databases open unchanged.

## Resource groups and the memory governor (S1E)

Every unit of admitted work is classified into one of eight `WorkloadClass`
values (`control`, `replication`, `oltp`, `interactive_sql`, `ai_retrieval`,
`analytics`, `maintenance`, `backup`) and belongs in a `ResourceGroup` that
bounds its concurrency, queue depth, memory, temporary disk, work units, CPU
weight, priority (0-255), and result size.
`ResourceGroupRegistry::with_defaults()` seeds one configured group per
class; the `control` and `replication` groups are pinned — they cannot be
removed and cannot be re-registered without reserved capacity. Groups
serialize deterministically (sorted by name) so they can later replicate as
cluster settings.

The node-level `MemoryGovernor` owns nine memory pools (`MemoryClass`: page
cache, decoded cache, query execution, result buffering, AI candidates,
compaction, replication, backup, network buffers). A subsystem reserves bytes
with `try_reserve(bytes, class)` and holds the returned `Reservation` guard;
dropping the guard releases the bytes. `pressure()` is the fraction of the
configured maximum in use; as it rises the governor escalates in a fixed
order, each level with its own threshold and a hysteresis band so the level
does not flap around a boundary:

1. `RejectLowPriority` — new compaction/backup reservations are rejected.
2. `EvictCaches` — registered reclaimable caches are driven through
   `evict_reclaimable`; the page caches implement `Reclaimable`, and
   `PageCache::with_governor(governor, class)` attaches one.
3. `SpillOperators` — `spill_trigger()` reports the level and
   `request_spill_grant` turns it into a typed grant that releases memory
   reservation bytes in exchange for moving operator working memory to
   disk; the disk side is the spill manager (next section).
4. `ThrottleMaintenance` — maintenance work yields to foreground work.

At every level the reserved floor holds: replication and network-buffer
(control-plane) memory is never fully starved. Observe the governor with
`stats()` — `GovernorStats` reports per-class usage, pressure, the current
escalation level, and granted/rejected reservation counters.

**Wired:** `Database::open` constructs exactly one governor per core
(`OpenOptions::memory_budget_bytes`, default `DEFAULT_MEMORY_BUDGET_BYTES`
= 1 GiB — a reservation cap, not a preallocation) and exposes it through
`db.memory_governor()`. Both page caches reserve under it and register as
reclaimable, so escalation step 2 drives real cache eviction.
`ResourceGroupRegistry` remains integrator-constructed: the core does not
own one yet, and admission is not yet classified into groups on engine
paths.

## Spill manager (S1E-004)

When the governor enters escalation step 3, spill-eligible operators move
working memory to disk through the `SpillManager`. `Database::open`
constructs one per core, rooted at `<db-root>/temp/spill`
(`OpenOptions::temp_disk_budget_bytes`, default
`DEFAULT_TEMP_DISK_BUDGET_BYTES` = 4 GiB of live spill files), and exposes
it through `db.spill_manager()`. Query engines open a per-query
`SpillSession` with `begin_query(query_id, cap_bytes)` or
`begin_query_in_group(query_id, group)` — the per-query cap comes from
`ResourceGroup::temporary_disk_bytes`.

Spill files carry the spec's four properties:

- **Query-ID namespaced** — every query spills under its own
  `temp/spill/q-<hex>/` subdirectory, so cancellation removes exactly one
  directory.
- **Checksummed** — every frame carries a CRC32C (the WAL's Castagnoli
  CRC); the sealing trailer carries a SHA-256 over all plaintext payloads
  plus the frame count, verified on read.
- **Bounded** — the per-query cap and the node-global cap are enforced
  with the governor's add-then-validate-rollback protocol; overflow is the
  typed `SpillError::BudgetExceeded`.
- **Encrypted when database encryption is enabled** — with a meta DEK
  present, every frame payload is sealed AES-256-GCM with a fresh random
  nonce; otherwise frames are plaintext.

Cleanup is total: the `SpillHandle` RAII guard deletes its file on drop,
an unfinished `SpillWriter` deletes its partial file, a dropped
`SpillSession` removes the whole per-query directory, and
`SpillManager::open` sweeps every stale entry a prior process run left
behind — spill files never outlive the process that created them. The
governor's `request_spill_grant` is the memory-side counterpart: an
eligible reservation trades bytes for the grant before the operator
writes through `SpillWriter::append`, `finish`es into a handle, and reads
back through `SpillReader`.

**Not wired yet:** no query operator spills today — the SQL engine does
not open spill sessions. Wiring spill-eligible operators (sorts, joins,
aggregations) onto escalation step 3 is follow-up work.

## Persistent online jobs (S1F-002/S1F-003)

The `JobRegistry` tracks long-running schema/data jobs — index builds, column
backfills, schema validation, materialized-view rebuilds, key rotation, large
imports — through the seven `JobState` values: `Pending`, `Running`,
`Paused`, `Cancelling`, `RollingBack`, and the terminal `Succeeded` and
`Failed`. Legal transitions are enforced by `JobState::can_transition`;
illegal ones fail with `JobError::IllegalTransition`.

The registry is mirrored to a `JOBS` file next to `CATALOG` on every state
mutation, written through the same temp-write + fsync + atomic-rename +
parent-dir-fsync path the catalog checkpoint uses (an 8-byte magic, a SHA-256
integrity tag over the body — or AES-256-GCM under the `encryption` feature —
and a versioned JSON envelope). Crash recovery runs at `JobRegistry::open`:
`Running` jobs park as `Paused` with their last durable checkpoint, and
interrupted cancels and rollbacks land in `Failed`.

Operators observe and steer jobs through `submit` / `get` / `list` / `pause`
/ `resume` / `cancel`; `cancellation_token(job_id)` exposes the cooperative
cancel signal. `run_build_publish(registry, job_id, job)` drives a
`BuildPublishJob` through the seven phases in order — record pending
definition, pin snapshot, build hidden generation, catch up committed deltas,
validate, publish atomically, release old generation — persisting a
checkpoint after each, so a resumed drive skips completed phases. Phases must
be idempotent and publish must be atomic. Fault-injection hooks
(`job.<phase>.before` / `job.<phase>.after`) fire at every phase boundary.

**Wired:** `Database::open` opens exactly one registry per core (the
sibling `JOBS` file persists across reopen) and exposes it through
`db.job_registry()`; crash recovery runs inside that open. Still not
wired: no executor threads ship — a caller drives a job synchronously
through `run_build_publish`. Wiring concrete job kinds (index builds
first) into engine paths is the next wave's work.

## Versioned catalog commands (S1F-001)

Every logical catalog mutation is expressed as a versioned `CatalogCommand`
wrapped in a `CatalogCommandRecord`: an explicit encoding `version`
(`CATALOG_COMMAND_FORMAT_VERSION = 1`; unknown versions fail closed on
decode) plus a monotonic `catalog_version` assigned on apply. The `CATALOG`
file is demoted from sole authority to a checkpoint with **no on-disk format
change** — the bounded retained command history (`COMMAND_HISTORY_LIMIT =
256`) rides the existing checkpoint and the `CatalogSnapshot` WAL payload.

`Catalog::apply_command` validates, applies, bumps `catalog_version`, and
appends the record; application is deterministic, and replaying the same
record against the same catalog version is an idempotent no-op.
`apply_command_and_checkpoint` additionally rewrites the checkpoint through
the existing atomic write path. Observe state with `catalog_version()` and
`commands_since(version)` — the latter returns a strict suffix when the
bounded history has compacted past `version`.

`required_permission(command)` documents the permission each command needs:
`Ddl` for table/column/index, trigger/procedure, and materialized-view
commands; `Admin` for user/role/grant/revoke, security-policy,
resource-group, and job-definition commands. **No enforcement change yet:**
the legacy `Database` entry points keep their existing gates; a later wave
routes mutations through command objects and checks `required_permission`
against the caller's principal.

## Lock manager (S1B-003)

`LockManager` provides key and predicate locking with deadlock detection: two
modes (`LockMode::Shared` and `LockMode::Exclusive` — deliberately no Update
mode; conversion deadlocks are resolved by the detector instead) over four
`LockKey` families: `Row` (one physical row), `Key` (primary-key or
unique-constraint bytes), `Range` (serializable predicate protection), and
`Barrier` (`schema_barrier()`, `sequence_barrier(name)`).

Grants are strict FIFO per key: a reader arriving behind a queued writer
never barges ahead of it. The wait-for graph is rebuilt on every enqueue and
grant; cycles are found deterministically and the victim is chosen
deterministically — lowest explicit priority first, then the youngest
transaction (largest `u64` ID). The victim's `acquire` fails with
`LockError::Deadlock`, bridged to `MongrelError::Deadlock` (victim and
cycle preserved) so callers see the precise `ErrorCategory::Deadlock`
(taxonomy category 9) with the same retry-the-whole-transaction discipline
as a write conflict. Waits honor a deadline
(`LockError::DeadlineExceeded`) and cooperative cancellation
(`LockError::Cancelled`). A transaction holds at most one in-flight
`acquire`, re-acquisition is re-entrant, and `release_all(txn_id)` runs
exactly once when the transaction ends.

**Wired:** `Database::open` constructs the manager (`db.lock_manager()`),
and the engine paths acquire through it. Every commit, abort, and rollback
funnels into `release_txn_locks`. DML commits hold the schema barrier
Shared; DDL operations take it Exclusive for the duration of the entry
point, so schema changes exclude concurrent DML and one another. The
commit path claims primary-key and unique-constraint keys Exclusive,
takes FK parent-protection locks while checking foreign keys, and
serializes auto-increment fills on per-table sequence barriers. A
deadlock victim's error surfaces at the SQL boundary as
`MongrelError::Deadlock`. Still not wired: SQL `FOR UPDATE` does not
acquire through the manager, and serializable predicate protection does
not yet take `Range` locks.

## Version-retention pins (S1C-004)

A historical version may be reclaimed only when it is older than the
oldest pin of **every** source. The six `PinSource` values:
`TransactionSnapshot` (oldest active MVCC reader, projected from the
`SnapshotRegistry`), `HistoryRetention` (the configured rolling window,
also projected), `BackupPitr`, `Replication`, `ReadGeneration`
(cursors and immutable read generations), and `OnlineIndexBuild`.

Every mounted table owns a `PinRegistry`; subsystems that still read old
versions register a `PinGuard` (cheap, deregisters on drop). GC folds the
registry into the table's version floor: `gc_versions` reaps a retiring
run only when its retire epoch is at or below the oldest pin of every
source, so no reader can lose versions it still needs. Observe the live
set with `Database::version_pins_report()` — one `PinsReport` per table,
one `PinInfo` per active source (oldest epoch, when the oldest pin was
taken, live pin count).

**Wired:** the per-table registries, the GC floor, and the diagnostics
report all operate today. Follow-up: the `Database`-level backup
run-file pins (`backup_pins`) are still consulted separately by the
GC/checkpoint paths; merging them into the `PinRegistry` is documented
follow-up work.

## Still to land

- `ResourceGroupRegistry` construction inside `Database::open` and
  workload classification on engine admission paths.
- Spill-eligible query operators (sorts, joins, aggregations) wired onto
  governor escalation step 3 through `db.spill_manager()`.
- A jobs executor and concrete `BuildPublishJob` kinds (index builds
  first) driven from engine paths.
- SQL `FOR UPDATE` lock acquisition and serializable `Range` predicate
  locks.
- Credentialed handle attaches and per-handle read-only enforcement.
- Routing catalog mutations through `CatalogCommand` objects and checking
  `required_permission` against the caller's principal.
- Merging the `Database`-level backup run-file pins (`backup_pins`) into
  the per-table `PinRegistry`.
