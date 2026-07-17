# ADR-0010: Rolling Upgrade Compatibility

- Status: Accepted
- Date: 2026-07-17
- Spec references: §9.1 (FND-001), §11.8 (Stage 2H rolling upgrades), §17
  (upgrade and compatibility policy), §4.10 (versioning invariant), §9.3
  (FND-003 versioned command envelope)

## Context

Once MongrelDB runs as a replicated cluster (Stage 2 and beyond), taking the
whole cluster down to upgrade is not acceptable: upgrades must roll node by
node while the cluster keeps committing. That means mixed-version clusters —
binaries at version N and N-1 — must interoperate correctly for the duration
of an upgrade, and it must always be possible to abandon an upgrade safely.

The invariants set the ground rules (§4.10): every durable and network format
has a versioned envelope; unknown required fields or incompatible versions
fail closed; field numbers and enum values are never reused; rolling upgrade
compatibility is tested before release. §17 fixes the support window —
current version N and previous version N-1 — and its rules, and §11.8 defines
what nodes advertise and the order of operations.

Stage 0 already landed the foundation: the FND-003 `CommandEnvelope`
(`crates/mongreldb-log/src/envelope.rs`) carries `format_version` with an
explicit `MIN_SUPPORTED_FORMAT_VERSION`, a `command_type` discriminant whose
numbers are never reused, a schema-evolution-safe payload, and a SHA-256 over
version, type, length, and payload; decoding fails closed on unknown versions,
truncation, trailing bytes, or checksum mismatch. Legacy formats predate this
discipline (e.g. the CATALOG checkpoint at `CATALOG_FORMAT_VERSION = 1` and
the bincode WAL op enums) and must be brought under the same min/max-version
regime as they are migrated to versioned commands (ADR-0008).

## Decision

1. **Support window.** A cluster may run binaries at version N and N-1 during
   a rolling upgrade (§17). Skipping a minor version in one step is not
   supported; multi-version moves stop at each intermediate version, or use
   the restore-based path.

2. **Versioned envelopes everywhere.** Every durable format (commit log
   entries, snapshots, catalog checkpoints, sorted-run/index metadata) and
   every network format (protocol messages, replication streams) is wrapped in
   a versioned envelope. New formats use the FND-003 `CommandEnvelope` or an
   equivalent header with explicit format min/max ranges. Payloads use a
   schema-evolution-safe encoding in which field numbers and enum values are
   never reused, unknown optional fields are preserved or ignored safely, and
   unknown required fields or incompatible versions fail closed (§4.10,
   §9.3).

3. **Compatibility rules (§17).**

   - N nodes understand N-1 log entries.
   - N-1 nodes ignore optional N fields.
   - New required commands are not emitted until feature activation.
   - Snapshot format supports the previous reader during the rollback window.
   - On-disk downgrade is not implied; rollback after feature activation is
     restore-based (restore from a pre-upgrade backup/snapshot).
   - Cluster feature level is separate from binary version.

4. **Feature activation is a replicated catalog command** (§11.8) applied
   through the meta control plane (ADR-0008). A feature may be activated only
   after every voter in the cluster supports it (§11.8 step 5). Before
   activation, an N binary must behave byte-compatibly with N-1 on every
   shared format: it emits nothing an N-1 node must understand but cannot.

5. **Node advertisement.** Each node advertises (§11.8) its binary version,
   protocol min/max, log format min/max, snapshot format min/max, and feature
   set. These are exchanged at handshake/join and recorded in the node
   descriptor (`NodeDescriptor.version`, §12.1), so compatibility is verified
   before a node participates and before activation is proposed.

6. **Upgrade order (§11.8).**

   1. Verify compatibility.
   2. Upgrade followers one at a time.
   3. Transfer leadership.
   4. Upgrade the former leader.
   5. Enable new features only after every voter supports them.

7. **Rollback windows.** Between the first binary upgrade and feature
   activation, binary rollback is always supported: no required N-only command
   has been emitted, and snapshots are still written in a format the previous
   reader accepts. Feature activation ends the rollback window.

8. **Release gate.** Rolling upgrade N→N+1 and rollback-before-feature-
   activation must pass in deterministic simulation before any release
   (Stage 2 gate, §11.8; §4.10). Compatibility failures fail closed, never
   silently degrade.

## Alternatives Considered

1. **Big-bang upgrades** (stop all nodes, upgrade, restart). Rejected:
   violates availability goals; §17 mandates the N/N-1 rolling window.

2. **Unbounded version skew** (any N interoperates with any M). Rejected: the
   test matrix grows without bound and failure modes become implicit. Fixed
   min/max ranges in every envelope give explicit, fail-closed behavior
   instead.

3. **Infer feature enablement from local binary version at startup.**
   Rejected: a new binary may run in a mixed cluster indefinitely; enablement
   is a cluster-wide, quorum-ordered decision (a replicated catalog command),
   not a local inference from version strings.

4. **Guarantee on-disk downgrade for all formats.** Rejected: it would double
   format-engineering and validation cost for a path that backups already
   provide. §17 documents restore-based rollback instead, and the rollback
   window keeps the previous reader working until activation.

5. **Per-format ad-hoc version checks.** Rejected: inconsistent fail-open
   risks; one envelope discipline (FND-003) applied uniformly is auditable and
   testable.

## Consequences

Positive:

- No-downtime upgrades with an explicit, testable compatibility contract.
- Feature level decoupled from binary version: operators can canary binaries
  without enabling features, and activate features deliberately at quorum.
- Fail-closed envelopes prevent silent corruption when versions genuinely
  cannot interoperate.
- Two-phase rollout (deploy, then activate) makes every risky format change
  reversible until a deliberate, logged activation decision.

Negative / costs:

- Every new required command or format change ships at least one release
  before it can be activated; feature work lands dark first.
- Until activation, changes to shared payloads are additive-optional only —
  no required fields, no enum-discriminant reuse.
- Snapshot writers must keep emitting previous-reader-compatible snapshots
  for the whole rollback window.
- CI must include mixed-version clusters and rollback-before-activation runs
  (Stage 2 gate; chaos/upgrade suites in Stage 5F, §14.6).

## Migration

1. **Stage 0 (done):** FND-003 lands `CommandEnvelope` with min/max format
   versions and fail-closed decoding in `mongreldb-log`; FND-004 routes all
   persisted commands through it.
2. **Stage 1:** catalog mutations become versioned commands (S1F-001,
   ADR-0008); legacy formats (WAL ops, CATALOG checkpoint at
   `CATALOG_FORMAT_VERSION = 1`) are documented with explicit accepted
   format-version ranges and fail closed outside them.
3. **Stage 2H (§11.8):** node advertisement, join-time compatibility
   verification, the upgrade order above, and the replicated
   feature-activation command. The Stage 2 gate requires passing N→N+1
   upgrade and rollback-before-activation tests.
4. **Stage 3+:** the feature-activation command rides the meta Raft group
   (ADR-0008) and is itself a versioned command, so the activation mechanism
   is governed by these same rules.

## Reversal Strategy

- **Before feature activation (the tested path):** replace the N binary with
  the N-1 binary node by node, in reverse upgrade order (former leader last).
  All durable state remains N-1-readable because no required N-only command
  was ever emitted and snapshots were written in the previous-reader format.
  The Stage 2 gate exercises exactly this rollback.
- **After feature activation:** binary downgrade alone is insufficient and is
  not supported. Rollback is restore-based (§17): restore from a
  backup/snapshot taken before activation, then replay the committed log up
  to a pre-activation fence. This is documented as the only supported
  post-activation path.
- **Feature level never auto-lowers.** Lowering the cluster feature level
  requires the same restore-based path; there is no in-place "un-activate"
  command.
