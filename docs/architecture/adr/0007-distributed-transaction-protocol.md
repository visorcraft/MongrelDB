# ADR-0007: Distributed Transaction Protocol

- Status: Accepted
- Date: 2026-07-17
- Spec references: §8 (MVCC timestamp model), §12.8 (Stage 3H distributed transaction protocol), §10.2 (Stage 1B single-node transaction model)

## Context

Once tables are partitioned across tablets (ADR-0006), a single transaction
can write rows owned by different Raft groups. MongrelDB then needs an atomic
commit protocol that preserves the engine's MVCC guarantees (Snapshot,
ReadCommitted, Serializable isolation; spec §4.5, §10.2) across tablet
boundaries, survives coordinator and participant failures, and gives clients
an unambiguous outcome.

Constraints that shape the choice:

- **HLC timestamps are the commit currency.** The target MVCC model (spec §8)
  stamps row versions with `HlcTimestamp` (`physical_micros`, `logical`,
  `node_tiebreaker`, lexicographic ordering), already scaffolded in
  `crates/mongreldb-types/src/hlc.rs`. Clock rules require that commit
  timestamps exceed every participant read/write timestamp (§8.2) and that a
  received timestamp advances the local clock.
- **Every tablet is already replicated.** Writes reach durability through the
  tablet's Raft group (spec §12.3), so the protocol's records can ride the
  same durability machinery instead of inventing a second one.
- **Ambiguous outcomes must be resolvable.** Clients retry through transport
  failure; the protocol must let a retry discover the true outcome rather
  than guess (spec §12.8 "Client outcome", and §4.7 cancellation/outcomes).
- **Fail-closed is a security boundary.** Recovery may never resolve an
  intent to the wrong decision; aborting a live transaction is only allowed
  under documented rules.

The decision covers coordinator selection, the commit record, prepare and
decision flows, timestamp assignment, and orphan-intent recovery.

## Decision

Adopt **replicated two-phase commit (2PC) with durable write intents and
replicated transaction-status records**, per spec §12.8.

1. **Deterministic coordinator choice.** The transaction coordinator is
   chosen deterministically as either:
   - the **home tablet of the first write** of the transaction, or
   - the **transaction-status shard derived from the transaction ID**.

   Both options give every node the same answer for the same transaction
   without consultation, which is what makes decentralized recovery possible.
   The choice between the two is a Stage 3H implementation detail; the
   deterministic property is the decision.

2. **Replicated coordinator record.** The coordinator's transaction-status
   record is itself replicated through its Raft group, with states:

   ```rust
   enum DistributedTxnState {
       Pending,
       Preparing,
       Committed { commit_ts: HlcTimestamp },
       Aborted { reason: AbortReason },
   }
   ```

   A leadership change on the coordinator's Raft group does not lose the
   record; the new leader continues the protocol from the durable state.

3. **Prepare (phase 1).** For each participant tablet, the coordinator:
   1. validates schema and authorization versions,
   2. checks conflicts and locks,
   3. persists the transaction's **write intents** through the participant's
      Raft group,
   4. receives back a prepare timestamp and a prepare token.

   Intents are durable before the prepare response is sent, so a prepared
   participant can never lose the evidence of an in-flight transaction.

4. **Decision (phase 2).** When all participants have prepared, the
   coordinator:
   1. chooses `commit_ts` **strictly greater than every observed
      timestamp** — all prepare timestamps and all participant read/write
      timestamps seen by the transaction — using the HLC clock rule
      `next_after(minimum)` (spec §8.2),
   2. persists `Committed { commit_ts }` through its Raft group,
   3. returns to the client **only after the decision is durable**.

   Participants resolve their intents (materialize versions at `commit_ts`)
   using the durable decision, either eagerly or lazily on first contact.

5. **Abort.** The coordinator persists `Aborted { reason }`; participants
   remove intents. Any abort is itself a durable, replicated record, never an
   in-memory hint.

6. **Orphan-intent recovery.** Any node that encounters an intent from an
   unknown transaction may query the coordinator record. Recovery follows
   exactly:
   - **check the transaction record** — a `Committed` or `Aborted` record
     resolves the intent immediately in that direction;
   - **push an expired pending transaction** — a recovery agent may try to
     move a `Pending` transaction forward (heartbeat/timeout expiry) by
     CAS-ing the record, never by assuming;
   - **abort only under documented timeout/priority rules** — an intent may
     be aborted unilaterally only when the transaction's coordinator record
     shows an expired `Pending` state under the documented timeout, or when
     the recovering transaction holds documented priority (for example to
     break deadlock); the abort still goes through the replicated record.

   Recovery never resolves an intent without reading or writing the
   replicated record. That is the fail-closed property.

7. **Client outcome.** A committed client receives: transaction ID, commit
   timestamp, participant set, and the durability level achieved. An
   ambiguous transport failure (the commit response was lost) is resolved by
   re-querying with the transaction ID / idempotency key — the same
   idempotency discipline as the single-node model (spec §10.2 S1B-005) and
   the protocol layer (ADR-0005).

Interaction with MVCC: single-tablet transactions continue to commit through
the Stage 1B protocol (§10.2) without 2PC overhead; the distributed protocol
engages only when the write set spans more than one tablet. Isolation-level
semantics (Snapshot / ReadCommitted / Serializable) are unchanged — 2PC
changes *where* the commit decision lives, not *what* visibility means.

## Alternatives Considered

- **Percolator-style commit (TiKV/Google Percolator).** Transactions write
  locks/values per key and commit by flipping a single primary cell; there is
  no coordinator record, and recovery resolves via the primary. It avoids
  coordinator hot-spots and gives good single-region latency, but: commit
  status is scattered across data cells rather than one authoritative record,
  making the §4.7 "durable outcome" contract and client-outcome query harder;
  orphan-lock cleanup is GC-coupled and historically subtle (lost primary
  writes, async-commit edge cases); and the model assumes timestamp oracle
  (TSO) service for commit timestamps, adding a global allocation dependency
  MongrelDB's HLC model deliberately avoids (§8). Rejected; the explicit
  replicated transaction record is a better match for our invariants, though
  we adopt Percolator's *push expired pending transaction* recovery move
  (decision 6).

- **XA / classic 2PC with an external transaction manager.** XA standardizes
  prepare/commit/rollback across resource managers and is what JDBC-era
  tooling expects. It was rejected because XA assumes a trusted, single,
  usually unreplicated coordinator; the in-doubt window after prepare blocks
  participants indefinitely on coordinator loss; and the specification has no
  answer for deterministic decentralized recovery. Our design keeps 2PC's
  shape but fixes XA's two fatal flaws directly: the coordinator record is
  Raft-replicated (coordinator failover continues the protocol), and any node
  may recover via the deterministic record location instead of waiting on a
  human to resolve in-doubt transactions. XA remains a possible *adapter* at
  the JDBC boundary, never the internal protocol.

- **Single-decree Paxos / one-shot consensus per transaction.** Elegant, but
  forces every multi-tablet transaction through a fresh consensus instance
  with no natural place for intent durability or push-recovery. Rejected.

- **Serializable snapshot isolation (SSI) across tablets without 2PC.** SSI
  handles *isolation* (detecting dangerous structures), not *atomicity* of a
  multi-group commit; it composes with, and does not replace, an atomic
  commit protocol. Rejected as a substitute; Serializable isolation remains
  enforced per §10.2 on top of this protocol.

- **Deterministic transaction scheduling (Calvin-style).** Removes commit
  coordination by pre-ordering transactions, but requires knowing read/write
  sets up front, which is incompatible with interactive SQL sessions.
  Rejected.

## Consequences

Positive:

- Atomic multi-tablet commits with a single authoritative, replicated record
  of the outcome — no in-doubt ambiguity that survives failover.
- Commit timestamps are HLC values strictly after all participant
  timestamps, so snapshot reads cluster-wide see a consistent, causally
  ordered commit without a global timestamp service (§8.2 rules hold by
  construction).
- Deterministic coordinator location plus the documented recovery moves make
  orphan intents self-healing; any node can resolve any intent.
- Client outcomes are explicit (txn ID, commit_ts, participants, durability)
  and ambiguous retries resolve idempotently.
- Single-tablet transactions keep the cheap Stage 1B path; the protocol's
  cost is paid only by genuinely distributed writes.

Negative / costs:

- Two extra durable writes on the commit path (intent persistence at
  participants, decision persistence at the coordinator) add latency versus
  single-tablet commit; cross-tablet transactions are strictly slower and
  the SQL layer should steer hot transactions toward colocation (ADR-0006).
- Prepared-but-undecided intents hold conflicts/locks until recovery
  resolves them; the documented timeout/priority rules are a liveness
  tuning surface that operators must understand.
- The transaction-status shards become new system tablets with their own
  placement, backup, and upgrade story (spec §12.1 lists them as meta
  control-plane property).
- Lazy intent resolution puts cleanup work on the read path of later
  transactions; unlucky readers pay the resolution cost and must push rather
  than assume.

## Migration

1. Stages 0–2 ship the HLC timestamp model (§8) and the single-node
   transaction/idempotency machinery (§10.2) first; `HlcTimestamp` is
   already scaffolded in `crates/mongreldb-types`. No distributed code paths
   activate before Stage 3.
2. The transaction-status record format and the intent record format are
   durable cluster formats: they are encoded in the versioned
   `CommandEnvelope` (spec §9.3, implemented in `crates/mongreldb-log`) with
   schema-evolution-safe payloads, field numbers never reused, and unknown
   required versions failing closed (ADR-0005).
3. When Stage 3H activates, transactions that touch one tablet commit through
   the existing protocol unchanged; only multi-tablet write sets engage 2PC.
   There is no data migration of user rows.
4. Rollout is feature-flagged per cluster (meta control plane owns feature
   flags, §12.1); a cluster with the flag off rejects multi-tablet
   transactions with a structured, non-retryable error rather than silently
   degrading atomicity.
5. Testing gate before enablement: deterministic simulation (spec §9.5,
   FND-005) of coordinator loss, participant loss, and ambiguous client
   retry, plus fault injection (§9.6) at every durable-write point of the
   protocol.

## Reversal Strategy

- **Before the feature flag is ever enabled:** full reversal by deleting the
  protocol code; nothing durable references it.
- **After enablement, with the flag off again:** no new multi-tablet
  transactions start; outstanding ones drain. Transaction records and
  resolved intents remain in the replicated logs and are readable by any
  build that supports their envelope `format_version` — reversal never
  requires rewriting history.
- **Data-format reversal:** because records are envelope-versioned and
  fail-closed, a future replacement protocol can introduce a new record
  format under a new version while old builds simply refuse it. What cannot
  be reversed is the *commit_ts* history of committed transactions: HLC
  commit timestamps are permanent MVCC facts. That is inherent to any
  timestamp-ordered MVCC and is why this ADR requires senior-maintainer
  approval before the durable formats merge (spec §9.1 definition of done).
