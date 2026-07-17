# ADR-0002: Commit-Log Authority

- Status: Accepted
- Date: 2026-07-16
- Spec references: sections 4.4 (durability authority), 4.7 (cancellation
  and outcomes), 4.8 (derived indexes), 6.2 (`mongreldb-log`), 6.10
  (dependency direction), 9.4 (FND-004)

## Context

Durability in MongrelDB today funnels through one choke point: the shared
WAL. `SharedWal` (`crates/mongreldb-core/src/wal.rs` ~line 1279) multiplexes
every table's records onto one active segment, and
`SharedWal::group_sync` (~line 1898) is documented and implemented as "the
single durability point for every concurrent appender since the last
`group_sync`" — one `flush() + sync_all()` plus a WAL-head write covers all
buffered committers (group commit). Committed state is stamped with the
monotonic `Epoch(u64)` (`crates/mongreldb-core/src/epoch.rs` line 14).

The target architecture adds a replicated mode in which the committed
consensus log — not the local WAL — is authoritative. Spec section 4.4 is
absolute: standalone mode has the standalone commit log as authority,
replicated mode has the committed consensus log as authority, and there
MUST NOT be two independent logs that can each declare a transaction
committed. Sorted runs and indexes are applied state and may be rebuilt.
Section 9.4 (FND-004) therefore defines a `CommitLog` abstraction that both
modes implement, with the critical rule that the storage apply path
receives only committed commands.

## Decision

There is exactly one authoritative commit log per storage core, and all
commits reach it through the `CommitLog` trait defined in
`crates/mongreldb-log/src/commit_log.rs`:

```rust
pub trait CommitLog: Send + Sync {
    fn propose(
        &self,
        command: CommandEnvelope,
        control: &ExecutionControl,
    ) -> Result<CommitReceipt, LogError>;

    fn read_committed(
        &self,
        after: LogPosition,
        limit: usize,
    ) -> Result<Vec<CommittedEntry>, LogError>;

    fn applied_position(&self) -> LogPosition;

    fn create_snapshot(&self) -> Result<LogSnapshot, LogError>;

    fn install_snapshot(&self, snapshot: LogSnapshot) -> Result<(), LogError>;
}
```

- `StandaloneCommitLog` ships first: it wraps the existing `SharedWal`
  group commit (the FND-004 adapter lives in
  `crates/mongreldb-core/src/commit_log.rs`), so standalone durability
  semantics — fsync-before-acknowledge at `group_sync` — are preserved
  unchanged. `LogPosition.term` is zero in standalone mode and
  `LogPosition.index` carries the standalone sequence/epoch.
- `RaftCommitLog` implements the same contract in Stage 2 over the
  consensus adapter (see ADR-0004). Switching modes swaps the `CommitLog`
  implementation; the storage apply path below it is untouched.
- The apply path receives only committed commands. A returned
  `CommitReceipt` is irrevocable: once a write crosses its durable commit
  fence, the caller is never told it rolled back (section 4.7).
- Sorted runs, memtable flushes, and every derived index (ANN, Sparse,
  MinHash, FM, bitmap, range) are rebuildable applied state, never commit
  authority (sections 4.4, 4.8). Losing them costs performance or
  approximate recall, never authoritative rows.

**Intentional deviation from the spec text of section 9.4:** the trait's
`control` parameter is `mongreldb_log::ExecutionControl`, a deliberately
minimal deadline-plus-cancellation type (`Option<Instant>` deadline,
`Option<Arc<AtomicBool>>` cancellation), not `mongreldb_core`'s
`execution::ExecutionControl`. The core type is a rich hierarchical
control (parent/child cancellation states, serialized cancellation
reasons, `tokio::sync::Notify` wakeups, parking-lot state —
`crates/mongreldb-core/src/execution.rs` line 74) that lives at the
runtime layer and cannot move below core in the dependency graph without
dragging tokio and the runtime into the log crate, violating section
6.10's direction (`log` sits below `runtime`). The core adapter converts
its hierarchical `ExecutionControl` into the minimal log-level one at the
propose boundary: the tightest deadline is preserved, and cancellation is
bridged onto the shared flag. This mirrors the deviation already
documented in `crates/mongreldb-log/src/commit_log.rs` (~lines 82–94).

## Alternatives Considered

- **Keep the WAL as the only interface, no abstraction.** Rejected: a
  replicated mode would then bolt consensus onto the side, producing
  exactly the two independent commit authorities section 4.4 forbids.
- **Two parallel logs (local WAL plus Raft log), both allowed to commit.**
  Rejected: violates section 4.4 verbatim; failover could acknowledge
  divergent commits.
- **Move core's `execution::ExecutionControl` down into `mongreldb-log`.**
  Rejected: it would pull tokio, parking_lot, and runtime-layer semantics
  into the lowest-level crate, inverting the section 6.10 dependency
  direction; the minimal mirror carries everything the log contract
  actually needs (deadline, cancellation).

## Consequences

- The commit path is re-plumbed once (FND-004) and never again: Stage 2
  replication, Stage 3 sharding, CDC, and backup/PITR all consume the same
  `read_committed`/`applied_position` contract.
- `LogPosition` gives every consumer a total order with an explicit term,
  which replicated mode needs and standalone mode trivially provides.
- Snapshot boundaries (`create_snapshot`/`install_snapshot`) become a log
  concept, aligning Stage 2E snapshot catch-up with standalone log
  truncation.
- The dual `ExecutionControl` types must be kept in sync deliberately; the
  conversion is the single documented seam.

## Migration

- Standalone deployments keep their current on-disk WAL; the
  `StandaloneCommitLog` adapter wraps `SharedWal` in place, so no data
  migration is required.
- Internally, call sites that today append to `SharedWal` directly move
  behind `CommitLog::propose` as FND-004 lands; group-commit timing and
  fsync behavior are unchanged.
- Replicated mode is a new deployment mode (Stage 2), not a conversion:
  per spec section 5.2 there is no in-place magic conversion for the first
  cluster release.

## Reversal Strategy

- The adapter is a thin seam: reverting means deleting
  `StandaloneCommitLog` and letting the transaction layer call
  `SharedWal::group_sync` directly again, which is exactly the current
  code path. No on-disk format changes are introduced by this decision, so
  reversal costs only the adapter code.
- The `mongreldb-log` crate's public API (envelope + trait) is frozen at
  Stage 0; reversal would instead supersede this ADR with a new one rather
  than silently mutating the trait.
