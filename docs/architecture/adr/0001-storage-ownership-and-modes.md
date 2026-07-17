# ADR-0001: Storage Ownership and Modes

- Status: Accepted
- Date: 2026-07-16
- Spec references: sections 2 (product modes), 4.1 (local ownership), 4.3
  (file ownership), 5.3 (storage mode marker), 6.10 (dependency direction)

## Context

MongrelDB today is an embedded engine: `Database::open` takes exclusive
ownership of a database directory inside the calling process. Ownership is
already enforced structurally in `mongreldb-core`:

- Cross-process exclusion uses `fs2` exclusive file locks. Open acquires a
  sibling lock file `.mongreldb-<path-hash>.lock` plus the legacy
  `_meta/.lock` inside the database root
  (`crates/mongreldb-core/src/database.rs`, `acquire_database_lock` ~line 1716
  and `acquire_legacy_database_lock` ~line 1740). Contention fails with the
  typed `MongrelError::DatabaseLocked`; `OpenOptions::lock_timeout_ms`
  (~line 1418) optionally turns the fail-fast check into SQLite-style
  bounded waiting.
- In-process exclusion uses a process-local open registry
  (`process_open_registry`, `OpenReservation` in `database.rs`): a second
  independent open of the same canonical path in the same process returns
  `DatabaseLocked` immediately, and callers are directed to share the
  existing `Arc<Database>` instead.
- A durable `_meta/replica` marker already exists and flips a `Database`
  into read-only follower behavior (`database.rs` ~line 1435), so the
  codebase has precedent for a durable per-root mode marker.

The target architecture (spec section 2) keeps this embedded mode but adds a
shared-handle embedded mode, a single-node server mode, and two cluster
scale levels (replicated HA, sharded). The non-negotiable invariants demand
that for one durable root there is exactly one process, one storage core,
one active log writer, one epoch/timestamp allocator, one transaction-ID
authority, one catalog state, and one close lifecycle (section 4.1), and
that different processes MUST NOT write the same database directory
(section 4.3). Section 2.6 names the prohibited deployment: a desktop
process and `mongreldb-server` opening the same directory directly at the
same time.

## Decision

Adopt one-process-one-storage-core ownership as the permanent storage
ownership model, with exactly three supported local access modes and one
prohibited co-open:

1. **Embedded exclusive mode.** One process owns the directory, one storage
   core is created, a second independent open of the same root is rejected,
   and the application shares `Arc<DatabaseCore>` or lightweight handles.
   This remains first-class for desktop apps, CLIs, tests, local AI tools,
   and edge deployments.
2. **Embedded shared-handle mode.** A process-local manager
   (`DatabaseManager::global().open_shared(...)`) returns handles that all
   reference the exact same process-local `DatabaseCore`. Recovery, WAL
   opening, open-generation advancement, and table mounting happen once.
   Each handle may carry its own principal and read-only restriction;
   dropping one handle does not close storage while another exists.
3. **Server-owned standalone mode.** Only the `mongreldb-server` process
   opens the files; every other client connects through the network
   protocol, SQL, or Kit/native APIs.

The prohibited co-open of spec section 2.6 stays prohibited forever: two
independent processes — embedded or server — never open the same directory
for write. Multi-node concurrency is provided by replication to separate
local directories, never by shared file access (section 4.3).

The mode of a durable root is recorded in a durable storage-mode marker
(section 5.3):

```rust
enum StorageMode {
    Standalone,
    ServerOwnedStandalone,
    ClusterReplica {
        cluster_id: ClusterId,
        node_id: NodeId,
        database_id: DatabaseId,
    },
}
```

Rules:

- `Standalone` may be opened embedded (exclusive or shared-handle).
- `ServerOwnedStandalone` is functionally standalone, but the server owns
  the lock; embedded opens of such a root are rejected with a clear error.
- `ClusterReplica` may be opened only by the cluster node runtime, which
  binds the directory to the recorded `cluster_id`/`node_id`/`database_id`.
- A backup validator may open any mode read-only through a special offline
  validation API.

## Alternatives Considered

- **Multi-process writers coordinated through the files** (file locks,
  shared page cache on a network filesystem). Rejected: violates section
  4.3 outright, cannot give one epoch/timestamp allocator or one commit
  authority (section 4.1), and network filesystems are explicitly ruled out
  as a coordination mechanism.
- **Server-only deployment, dropping embedded.** Rejected: embedded is a
  first-class product mode (section 2.1) and the existing user base runs
  it; the server is instead layered on the same single storage core.
- **Single global mode with no durable marker.** Rejected: without a
  durable marker, a directory created for a cluster replica could be opened
  embedded and fork the replicated log; the marker is what makes the
  section 2.6 prohibition enforceable across process restarts.

## Consequences

- The existing `fs2` lock + process-open-registry enforcement remains the
  mechanism; no new inter-process coordination is introduced.
- Handle identity (principal, read-only restriction) lives on the handle,
  never inside shared table state, matching section 4.6.
- The durable `StorageMode` marker becomes part of the root format and
  therefore a versioned durable format subject to section 4.10 fail-closed
  rules.
- Stage 1A (`DatabaseCore`/`DatabaseHandle` split, process-local
  shared-core registry, lifecycle) implements the shared-handle mode;
  Stage 2 implements `ClusterReplica` opens through the node runtime.
- Operational tooling must respect the marker: an embedded CLI pointed at a
  `ServerOwnedStandalone` or `ClusterReplica` root fails fast instead of
  mounting.

## Migration

- Existing databases have no marker; absence means `Standalone`, preserving
  every current embedded deployment with no conversion step.
- The marker is written when a root is first opened by a server in
  server-owned mode or when a cluster node adopts a directory as a
  `ClusterReplica`; writes go through the same durable, checksummed root
  metadata path as `_meta` files today.
- No in-place conversion from standalone to cluster replica is performed
  for the first cluster release (spec section 5.2); moving to replicated
  mode means bootstrap of a fresh replica directory plus restore/catch-up.

## Reversal Strategy

- The marker is additive metadata. Reverting to the pre-marker engine
  ignores it; `Standalone` roots behave exactly as today, and removing a
  marker file (or rewriting it to `Standalone`) with the database closed
  restores embedded-openable state.
- The ownership enforcement itself (fs2 locks, in-process registry) is
  unchanged from the current release, so nothing about the reversal
  weakens the section 2.6 prohibition.
