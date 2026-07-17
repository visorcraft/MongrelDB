# Replicated High Availability

Stage 2 of the [architecture program](18-architecture-foundations.md) turns
one logical database into one consensus-replicated group: one Raft group per
database, one committed log order, at most one effective leader per term.
This page tours the machinery that has landed in `mongreldb-consensus` and
`mongreldb-cluster`, reports measured failover behavior, and states plainly
what is still in progress. The Stage 1 single-node subsystems are described
in [Single-Node Subsystems](19-single-node-subsystems.md).

Nothing here changes the standalone on-disk database format: existing
databases open unchanged, and the engine still runs on the standalone
commit log until the integration wave binds the replicated one.

## Consensus architecture

Per [ADR-0004](architecture/adr/0004-consensus-library-selection.md),
MongrelDB wraps a mature Raft implementation —
[openraft](https://github.com/databendlabs/openraft) 0.9 — and implements
only the storage, state-machine, and network adapter, never the consensus
algorithm itself. Pre-vote, leader election, log replication,
joint-consensus membership changes, snapshots, read index, leadership
transfer, and persistent hard state are all library features.

The adapter pieces (`mongreldb-consensus`):

- **`ConsensusGroup`** — one member of a group: proposals, the linearizable
  read barrier, snapshots, membership changes, best-effort leadership
  transfer, and observability metrics.
- **`ReplicatedCommand`** (S2B-003) — the log payload: `Transaction`,
  `Catalog`, `Maintenance`, or `Noop`. The leader assigns the term and log
  index (the raft log id), stamps the commit timestamp from the group HLC
  clock *before* replication so every replica applies the identical value,
  and carries the envelope's command id.
- **Idempotent apply state machine** (S2B-004) — persists the last applied
  term/index, command id, and commit timestamp plus a bounded recent-id set
  (checkpointed every apply batch and carried inside snapshots). Replays
  and client retries are recognized and skipped; the engine's durable
  idempotency ledger backstops the sink-first checkpoint window.
- **`RaftCommitLog`** — the replicated `CommitLog` implementation
  ([ADR-0002](architecture/adr/0002-commit-log-authority.md)): the
  committed consensus log is the single commit authority; there is no
  second log that could independently declare a transaction committed.

### Per-group durable storage (S2B-002)

Each group member keeps its own local directory:

```text
raft/hard-state            vote + last committed log id (atomic frame)
raft/log/seg-<first>.seg   append-only log segments, checksummed frames
raft/log/PURGED            last purged log id (atomic frame)
raft/membership            last applied membership
raft/state/applied         apply checkpoint (with the idempotency set)
raft/snapshot/             snapshot data frames + CURRENT metadata
```

Atomic files are `MAGIC | sha256(body) | body`, written to a temporary
file, fsynced, renamed into place, and sealed with a parent-directory
fsync; log segments are length-prefixed frames with per-frame SHA-256.
`save_vote` fsyncs before returning; log appends fsync per append (the
durable default) or batched on an interval, but in every case the flush
callback fires only after the fsync, so acknowledged entries are always
durable. Recovery truncates a torn tail and fails closed on any other
checksum violation.

## Write protocol and durability levels

A client write follows the spec section 11.3 path: find the leader,
validate, propose through Raft, persist on a quorum, commit, apply in
order, and return the commit receipt. Two durability levels exist
(spec 11.3):

- **`Quorum` (default)** — the receipt is issued after quorum persistence,
  commit, and local apply. A quorum-acknowledged write has **RPO 0** for
  failures below quorum loss.
- **`LeaderDisk` (optional lower guarantee)** — the receipt is issued once
  the entry is fsynced on the leader's local log, *before* quorum commit.
  The honesty contract: a crash before that fsync never acknowledges; the
  receipt is **not** a commit declaration (visibility still gates on quorum
  commit + apply), and a LeaderDisk-acknowledged entry **can be truncated
  on leader loss — RPO > 0**. If quorum commit lands before the fsync
  signal, the receipt is upgraded to quorum strength. Choose this level
  only for data whose loss on failover is acceptable.

Memory-only acknowledged writes are never implemented, and the
standalone-mode `GroupCommit` level is rejected in replicated mode.

## Read consistency levels

All five spec section 11.4 levels are implemented
(`mongreldb_consensus::read::ReadConsistency`). A read barrier answers *up
to which applied position may this node serve the read?* and returns a
`ReadWatermark` (applied position + last applied commit timestamp); the
caller serves its read at or below it.

- **`Linearizable`** — leader read-index: the leader confirms leadership
  with a quorum, then waits until the read position is applied. Never
  served by an unconfirmed leader; followers answer `NotLeader` with the
  leader hint.
- **`ReadYourWrites { token }`** — `RaftCommitLog::session_token(receipt)`
  issues a `SessionToken` (group id, commit index, commit timestamp) for
  every committed write; any replica presenting the token waits until its
  applied watermark reaches that position.
- **`Snapshot { timestamp }`** — waits until the applied watermark's commit
  timestamp covers the requested timestamp.
- **`BoundedStaleness { max_lag_ms }`** — serves only if the applied commit
  timestamp lags the node's HLC clock by at most the bound, else
  `StalenessExceeded` (the caller picks another replica).
- **`Eventual`** — serves the current local applied watermark immediately.

Barrier errors are typed for routing (spec 11.7): `NotLeader` carries the
leader hint, `LeaderUnknown` asks for rediscovery, and
`StalenessExceeded`/`DeadlineExceeded` are retry decisions for the gateway.

## Snapshots and catch-up

A consensus snapshot carries the apply checkpoint (last applied term/index,
command id, commit timestamp, idempotency set), the last applied
membership, and the apply sink's payload — framed and SHA-256 checksummed
under a deterministic, content-derived snapshot id so identical snapshots
install under identical names everywhere. Snapshots are built by the
group's snapshot policy (`snapshot_policy_logs`) or forced with
`ConsensusGroup::snapshot()`, and the log is compacted behind them
(`max_in_snapshot_log_to_keep`).

A follower that falls further behind than `replication_lag_threshold` is
brought up to date by snapshot install instead of log replication.
Installation is staged: download, verify hashes, replace applied state
atomically, update the last-applied metadata, then resume log apply —
never directly over live state. `RaftCommitLog::create_snapshot` /
`install_snapshot` expose the same round trip at the `CommitLog` surface.

## Membership changes

Membership changes use joint consensus (openraft's two-step
`change_membership`). Node add follows the spec section 11.6 order:
`add_learner` (blocking until line-rate), catch-up verification, then
`promote` to voter. Node remove (`remove`) drops the voter through joint
consensus; transfer leadership first when removing the leader. The durable
128-bit `NodeId` lives in the cluster membership directory owned by
`mongreldb-cluster`; the consensus adapter projects it deterministically
onto the raft `u64` id space.

## Leadership transfer

openraft 0.9 has no dedicated transfer RPC, so transfer is orchestrated as
a best-effort handoff: the old leader suppresses its own candidacy, the
transport asks the target to start an election, and the target wins the
next round once the old leader's lease lapses. It is used for planned
failover and rolling upgrades ([ADR-0010](architecture/adr/0010-rolling-upgrade-compatibility.md)),
never required for correctness — an individual attempt can lose the
election race and is retried by the caller.

## Failover behavior (measured)

The spec section 11.6 target: **leader failure detected and a new leader
available within 10 seconds, p95**, on a reference one-AZ network.

The qualification suite (`crates/mongreldb-consensus/tests/qualification.rs`)
measures detection-plus-availability over 20 leader kills on a three-node
cluster — from the kill until the survivors agree on a new leader *and* a
client write commits through it. Measured on the qualification runs for
this page (in-process transport, test timing config of 50 ms heartbeats
and 150–300 ms election timeouts):

```text
failover p50 529 ms, p95 666 ms over 20 leader kills (target: p95 < 10 s)
```

The test asserts p95 < 10 s as a regression tripwire — with roughly 15×
margin at test timings. Production defaults (200 ms heartbeats, 600–1200 ms
election timeouts) land somewhat higher but still far below the target.
This is a test-harness measurement on one machine, not a deployment
benchmark.

After failover, no acknowledged quorum write is lost: the qualification
suite kills a follower, confirms quorum-acknowledged writes keep
committing, restarts the node, and verifies catch-up with every
acknowledged write present. It likewise partitions a follower past log
truncation and verifies snapshot catch-up with no missing committed
entries.

## Chaos qualification

`crates/mongreldb-consensus/tests/chaos.rs` runs the section 11 gate as a
randomized-but-seeded scenario matrix: deterministic scenario order derived
from a seed constant (`MONGREL_CHAOS_SEED` overrides it; the seed is
printed at the start of every run and inside every failure message). Each
of 21 rounds applies one of leader crash, follower crash, minority
partition, majority partition, heal, membership add/promote/remove, or
leadership transfer while a sequential client proposes monotonically
numbered commands with idempotent retries. After every round, on the
healed and converged cluster, the suite asserts:

- at most one effective leader per term (metrics observations accumulated
  over the whole run);
- no committed entry ever lost or reordered on any node — committed
  streams agree exactly, first occurrences are monotone, and every
  acknowledged receipt is present at its recorded position;
- no split-brain commit — proposals attempted on the quorum-less side of a
  partition are rejected and never appear in any committed stream
  post-heal;
- state-machine applied sequences are identical on all live nodes.

## Request routing and retries

`mongreldb-cluster` (Stage 2A/2G) holds the client-facing pieces: durable
node identity and cluster bootstrap records (cluster id, initial
membership, trust configuration), a `RoutingCache` (leader hint, term,
metadata version, endpoint list per group), and a `RetryPolicy`
implementing the spec section 11.7 discipline — on `NotLeader` use the
returned hint, refresh metadata when stale, retry idempotent reads, and
retry writes only with an idempotency key or an unambiguous not-proposed
status. An ambiguous write is never replayed automatically without a
durable idempotency key.

## Still to land

- **A real RPC transport.** Today the only transport is the in-memory one
  used by tests and deterministic simulation; the networked transport
  (with TLS from the cluster trust configuration) is a later Stage 2 wave.
- **Live engine integration.** The engine `ApplySink` binding has landed
  (`engine_sink.rs`): committed `ReplicatedCommand`s apply to a
  `ClusterReplica`-marked core through the same WAL-recovery apply path the
  engine already used. Still open: the server wiring that runs one group
  per database in production, switching those cores from the default
  `StandaloneCommitLog` to `RaftCommitLog`, and an online `hot_backup`
  against a live replica core — every replica open is read-only and
  `hot_backup` requires `Admin`, so replica backups currently stage offline
  from a quiesced root (see the gate table).
- **Cluster CLI.** The bootstrap records and join flow exist in
  `mongreldb-cluster`; the `mongreldb cluster init/join/status`,
  `node drain`, and `node remove` commands are not wired yet.
- **Rolling upgrades.** Version advertising, feature levels, and the
  upgrade-planning/rollback-assessment machinery (ADR-0010) have landed in
  `mongreldb-cluster::meta` with unit tests; the executed N→N+1 upgrade
  choreography waits on the real transport.

## Stage 2 gate status

From the spec section 11 gate list:

| Gate item | Status |
|---|---|
| Three-node cluster passes deterministic partition tests | **Pass** — `tests/cluster.rs` partition scenarios and the seeded `tests/chaos.rs` matrix |
| No split-brain commit under any simulated partition | **Pass** — minority/majority partition rounds assert quorum-less proposals never appear post-heal |
| Quorum writes survive one-node loss | **Pass** — `tests/qualification.rs::quorum_acknowledged_writes_survive_one_node_loss` |
| Snapshot catch-up works after log truncation | **Pass** — `tests/qualification.rs::snapshot_catch_up_after_log_truncation` (and the cluster-suite variant) |
| Leader transfer preserves availability | **Pass** — transfer rounds in the chaos matrix keep the client committing; best-effort orchestration with retries |
| Linearizable read tests pass | **Pass** — read-index barrier on a confirmed leader; unconfirmed leaders and followers never serve it |
| Read-your-writes tests pass | **Pass** — receipt-position wait and the `SessionToken`-carried `ReadConsistency::ReadYourWrites` barrier |
| Rolling upgrade N→N+1 and rollback-before-feature-activation | **Deferred (execution)** — planning is landed and unit-tested (`mongreldb-cluster/src/meta.rs`: followers-first/leader-last ordering, compatibility verification, binary rollback until first feature activation); the executed upgrade still needs the networked transport (ADR-0010) |
| Backup from a follower is valid | **Pass** — `mongreldb-core/tests/stage2_gate.rs::backup_from_a_follower_is_valid_and_matches_the_leader`: a quiesced follower's applied root stages into a backup that `verify_backup` + `validate_restore` accept, whose manifest file hashes equal the leader's backup at the same applied watermark (except the node-local `_meta/replication_id` and `_meta/storage-mode` markers), and which reopens under the replica open rules with the exact applied rows. Live `hot_backup` on a replica core is read-only-rejected this wave (see above) |
| Current AI and SQL results match standalone behavior at the same snapshot | **Pass** — `mongreldb-core/tests/stage2_gate.rs::ai_and_sql_results_match_standalone_at_the_same_snapshot`: standalone and `ClusterReplica` engines seeded with identical data agree at the same committed watermark on full row content and commit epochs, ANN/Sparse/MinHash top-k `(id, score)` sequences, bitmap/FM query id sets, and COUNT/SUM/MIN/MAX/AVG values. One documented shape difference: replica state is overlay-only this wave (replicated spilled-run commits fail closed until Stage 2C spill translation), so the `aggregate_native` fast path declines on a replica and SQL aggregates serve from the scan fallback — the test asserts the standalone native results equal the replica fallback values |
