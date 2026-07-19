# Architecture Foundations

MongrelDB implements the "Best Practical Architecture" program: the same
codebase supports embedded exclusive/shared-handle use, a production
single-node server, replicated HA, and a sharded tablet cluster with
workload-isolated AI/analytics execution. The design is recorded as ten
[Architecture Decision Records](architecture/adr/README.md) (ADRs). This page
summarizes the Stage 0 foundation contracts that remain visible on every
public API (commit log, taxonomy, build identity, storage ownership).

**Integrated stages (see
[implementation status](architecture/implementation-status.md) for exact-SHA
qualification):**

| Stage | Doc | Surface |
|---|---|---|
| 0 foundations | this page | commit log, taxonomy, modes, ADRs |
| 1 single-node | [19](19-single-node-subsystems.md) | resource groups, spill, locks, jobs |
| 2 HA | [20](20-replicated-ha.md) | Raft per database, mTLS, snapshots |
| 3 sharded | [21](21-sharded-cluster.md) | meta, tablets, dist txn, gateway |
| 4 AI/workload | [22](22-embeddings-and-retrieval.md) | scheduler, generated embeddings, ANN/sparse, multi-region |
| 5 ops | [23](23-native-rpc-and-mysql-compatibility.md) | native RPC, security, MySQL migration/wire, executable certification |

Embedded single-node and the HTTP daemon remain first-class; they are no
longer the only runtime modes.

## Commit visibility goes through a commit log

Every commit now reaches durability through the `CommitLog` trait
(`mongreldb-log` crate). The standalone implementation wraps the existing
shared-WAL group commit, and the engine publishes reader visibility only after
the returned `CommitReceipt` exists — the storage apply path receives only
committed commands.

This is an internal contract change, not an API change: the on-disk format is
unchanged, existing databases open unchanged, and `put`/`commit` behave exactly
as before.

## Stable error taxonomy

`MongrelError::category()` maps every engine error onto one of twenty stable
`ErrorCategory` values from the `mongreldb-types` crate. Each category has a
numeric code (`code()`, 1–20) that is never reused and a retry class
(`retry_class()`):

```rust
use mongreldb_types::errors::ErrorCategory;

match error.category() {
    ErrorCategory::TransactionConflict => { /* retry the whole transaction */ }
    category if category.is_retryable() => { /* refresh metadata or back off */ }
    _ => { /* not retryable: surface the error */ }
}
```

Key programmatic handling off the category or its code, never the message text
— messages are diagnostic and may change between releases. Categories a plain
retry cannot fix (`DeadlineExceeded`, `Cancelled`, `Unauthenticated`,
`PermissionDenied`, `ClusterVersionMismatch`) report `is_retryable() == false`.
`CommitOutcomeUnknown` is never blindly retried: replay only with a durable
idempotency key.

The full `MongrelError` variant and message remain available in-process. The
taxonomy is deliberately coarser; it is the cross-language contract the Node,
C FFI, and JNI bindings will map.

## Build information

`mongreldb_core::build_info()` reports the exact build identity:

```rust
let info = mongreldb_core::build_info();
println!(
    "engine {} (artifact {}, git {}, {}, {})",
    info.engine_version,
    info.artifact_version,
    info.mongreldb_git_sha,
    info.target_triple,
    info.build_profile,
);
```

The git SHA comes from `MONGRELDB_GIT_SHA` at build time, falling back to
`git rev-parse HEAD` and then to the packaged `.cargo_vcs_info.json`. Quote
`build_info()` output when reporting issues.

## Component version check

Every first-party crate — engine, bindings, and the foundation crates — moves
on one version train. CI enforces this with:

```sh
python3 scripts/verify-component-versions
```

The script fails when any `crates/*/Cargo.toml` or any resolved first-party
package drifts from the workspace version. Run it before cutting a release or
after adding a crate.

## Shared handles (Stage 1A)

`DatabaseManager::global().open_shared(path, OpenIdentity)` attaches a
lightweight `DatabaseHandle` to the one process-local `DatabaseCore` for a
durable root ([ADR-0001](architecture/adr/0001-storage-ownership-and-modes.md)):

- **One core per root.** The manager keys cores by `DatabaseFileIdentity` —
  the durable device/inode identity of the pinned root directory, never the
  path text — so path aliases and renamed parents collapse onto one core.
  Recovery, WAL opening, open-generation advancement, and table mounting run
  exactly once, on the first attach; concurrent attaches wait on that one
  initialization instead of racing a second open.
- **Handles are cheap and capability-bounded.** Dropping a handle has no
  storage side effects; storage closes when the last core reference drops.
  Handles do not `Deref` to `Database` and never expose raw table handles.
  `shares_core_with` verifies shared ownership without exposing the core.
- **Per-handle identity.** Every handle carries a `HandleIdentity`
  (`handle.identity()`) and a `HandleAccess` (`handle.access()`); the core
  never stores one mutable "current principal". `OpenIdentity` supports
  credentialless, catalog credentials, and scoped service principals.
  Catalog roles and permissions resolve live on every sensitive operation;
  dropped users and revoked roles affect existing handles immediately.
- **Read-only is enforced.** `open_shared_with_access` binds
  `HandleAccess::read_only()` to the handle. Every DML, DDL, catalog mutation,
  shutdown, and other write exposed by the handle returns `ReadOnlyHandle`.
  Queries continue through the principal-aware RLS and masking path.
- **Exclusivity holds both ways.** A shared core takes the same lease an
  exclusive `Database::open` takes: `Database::open` on the same root fails
  with `DatabaseLocked` while shared handles exist, and `open_shared` fails
  the same way while an exclusive owner holds the root.
- **Lifecycle is explicit.** `handle.lifecycle_state()` reports the core's
  `LifecycleState` (`Opening`, `Open`, `Draining`, `Closing`, `Closed`,
  `Poisoned`), and `handle.operation_guard()` admits one operation (rejected
  once the core leaves `Open`). `handle.shutdown(drain_deadline)` rejects new
  operations, drains in-flight ones within the deadline, syncs durable state,
  releases the file lock, and marks the core `Closed` — every handle then
  rejects further operations, and a later attach re-initializes a fresh core.
  `Database::shutdown()` on a shared facade is rejected with
  `MongrelError::Conflict`; use the handle's `shutdown`.

The exclusive model is unchanged alongside this: `Database::open` still owns
its root alone, sharing one `Arc<Database>` across workers remains valid, and
its `Database::shutdown()` still closes the final owner, failing with
`DatabaseBusy { strong_handles }` while other strong handles exist.

## Test infrastructure (engine developers)

Two Stage 0 crates serve engine testing rather than applications:

- `mongreldb-fault`: named fault-injection hooks at durable boundaries (WAL
  append/fsync, commit publish, catalog publish, snapshot install, index
  publish). Hooks are disabled by default and cost one atomic load when
  disarmed; tests arm hooks and synchronize on barriers, never sleeps.
- `mongreldb-sim`: a seeded deterministic simulator (virtual clock, network,
  and disk) for reproducible consensus and distributed-transaction tests in
  later stages.
