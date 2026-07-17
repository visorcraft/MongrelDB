# ADR-0004: Consensus Library Selection

- Status: Accepted
- Date: 2026-07-16
- Spec references: sections 4.2 (cluster ownership), 6.5
  (`mongreldb-consensus`), 6.10 (dependency direction), 11.2 (Stage 2B,
  S2B-001 through S2B-004), 11.4 (reads), 11.6 (membership and failover)

## Context

Stage 2 introduces the replicated high-availability mode (spec section
2.4): one logical database per Raft group, one committed log order, at
most one effective leader per term (section 4.2). The consensus layer must
provide, per S2B-001:

```text
pre-vote
leader election
log replication
joint-consensus membership changes
snapshots
read index
leadership transfer
persistent hard state
```

Spec section 6.5 is categorical: `mongreldb-consensus` owns the *adapter*
to a mature Raft implementation and **must not implement Raft from
scratch**. MongrelDB implements the storage/state-machine adapter, not
the consensus algorithm itself (S2B-001). The adapter bridges the chosen
library to the `CommitLog` trait (ADR-0002) via `RaftCommitLog`, persists
per-group state (`raft/hard-state`, log segments, snapshot metadata,
membership — S2B-002), proposes `ReplicatedCommand`s (S2B-003), and runs
a deterministic, idempotent apply state machine (S2B-004). Reads need a
read index for linearizable queries and leadership transfer for planned
failover and rolling upgrade (sections 11.4, 11.8).

## Decision

Use **[openraft](https://github.com/databendlabs/openraft)** as the Raft
implementation, wrapped by the `mongreldb-consensus` adapter crate.

Rationale against the S2B-001 requirement list:

- **Mature and async-native.** openraft is a production-used Raft
  implementation (originating from the Databend project) built on
  `async`/`.await` and tokio, which matches MongrelDB's runtime; proposal,
  replication, and apply are ordinary async calls rather than a
  hand-driven state-machine loop.
- **Full feature coverage.** It provides pre-vote, leader election, log
  replication, joint-consensus membership changes, snapshot install and
  replication, read index (linearizable read barriers), leadership
  transfer, and persisted hard state — every S2B-001 line item is a
  library feature, not code we must write.
- **Trait-based integration boundary.** Storage and the state machine are
  plugged in through its storage traits and the network through its
  network traits, which maps directly onto our intended adapter:
  `RaftCommitLog` (ADR-0002) implements `CommitLog` by proposing
  `CommandEnvelope` payloads into the openraft group, and the apply
  state machine (S2B-004) records last applied term/index, command ID,
  and commit timestamp for idempotent replay.

MongrelDB implements only the storage/state-machine adapter, per section
6.5. The dependency on openraft is confined to `mongreldb-consensus`;
lower crates (`log`, `storage`, `runtime`) never import it, preserving
the section 6.10 dependency direction.

## Alternatives Considered

- **raft-rs** (the etcd-raft port used by TiKV). Rejected for this
  program: it is proven in production, but its API is a lower-level,
  `Ready`-driven design — the host must continuously drive ready/advance
  processing, persistence, and message pumping by hand, which recreates a
  large slice of the consensus runtime we are explicitly trying not to
  own. Its async and membership ergonomics (joint consensus arrived later
  and remains more manual) fit S2B-001's checklist less directly than
  openraft's built-ins.
- **Hand-rolled Raft.** Rejected unconditionally: spec section 6.5 makes
  this a MUST NOT. Correct joint consensus, pre-vote, and snapshot
  interaction are among the easiest things in systems engineering to get
  subtly wrong, and our effort belongs in the adapter, state machine, and
  product modes.

## Consequences

- `mongreldb-consensus` gains an external dependency; it is the only
  crate that does, and its version is pinned and reviewed like every
  other engine dependency.
- We inherit openraft's correctness surface (elections, membership,
  snapshot protocol) and its upgrade cadence; tracking upstream releases
  becomes routine maintenance.
- The adapter must map our versioned `CommandEnvelope` (FND-003) and
  HLC commit timestamps (ADR-0003) onto openraft entries, and persist
  hard state/log/snapshots checksummed and fsynced per the library's
  storage contract (S2B-002).
- Deterministic simulation (FND-005) and fault injection (FND-006) must
  wrap the adapter's network and storage traits, not the library
  internals.
- If openraft's development stalls, the cost of switching is bounded by
  the adapter boundary — see Reversal Strategy.

## Migration

- No existing deployment changes: Stage 2 is a new mode, and per spec
  section 5.2 there is no in-place conversion of standalone directories
  into cluster replicas for the first cluster release.
- Standalone mode never links `mongreldb-consensus`; the
  `StandaloneCommitLog` path (ADR-0002) is untouched.
- New replicated deployments bootstrap per sections 2.4 and 11.1–11.2:
  fresh per-node local directories, cluster bootstrap with joint
  consensus for every later membership change.

## Reversal Strategy

- The decision is isolated behind two seams: the `CommitLog` trait
  (ADR-0002) above and openraft's storage/network traits below. Swapping
  libraries means re-implementing `RaftCommitLog` and the
  storage/state-machine adapter against a different Raft crate; neither
  the standalone engine nor the apply path changes.
- Before the first cluster release ships durable `raft/` state, reversal
  costs only adapter code. After durable replicated formats exist,
  reversal additionally requires a log/hard-state migration or a
  snapshot-based rebuild of each replica (install snapshot from a
  replacement group), which is operationally supported by Stage 2E
  snapshot catch-up.
