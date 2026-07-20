# Architecture Decision Records

This directory holds the Architecture Decision Records (ADRs) for the MongrelDB
"Best Practical Architecture" program, as required by spec section 9.1
(FND-001). All senior maintainers must approve these ADRs before durable
cluster formats are merged.

## Index

| ADR | Title | Status |
| --- | ----- | ------ |
| [0001](0001-storage-ownership-and-modes.md) | Storage ownership and embedded/server modes | Accepted |
| [0002](0002-commit-log-authority.md) | Commit-log authority | Accepted |
| [0003](0003-mvcc-timestamp-format.md) | MVCC timestamp format | Accepted |
| [0004](0004-consensus-library-selection.md) | Consensus library selection | Accepted |
| [0005](0005-network-protocol-and-serialization.md) | Network protocol and serialization | Accepted |
| [0006](0006-tablet-partitioning-model.md) | Tablet partitioning model | Accepted |
| [0007](0007-distributed-transaction-protocol.md) | Distributed transaction protocol | Accepted |
| [0008](0008-catalog-control-plane-ownership.md) | Catalog/control-plane ownership | Accepted |
| [0009](0009-ai-index-replication-and-rebuild-policy.md) | AI index replication/rebuild policy | Accepted |
| [0010](0010-rolling-upgrade-compatibility.md) | Rolling upgrade compatibility | Accepted |
| [0011](0011-release-workspace-and-lock-boundaries.md) | Release workspace and lock boundaries | Accepted |

## Format

Every ADR uses the following sections, in this order (spec section 9.1):

1. **Context** — the forces, constraints, and current code that shape the
   decision, with references to the normative spec and verified source
   locations.
2. **Decision** — the change being made, stated in active voice.
3. **Alternatives Considered** — the credible options that were rejected, and
   why.
4. **Consequences** — what becomes easier or harder, including follow-up work
   the decision creates.
5. **Migration** — how existing data, deployments, and code move from the old
   behavior to the decided one.
6. **Reversal Strategy** — how the decision can be undone if it proves wrong,
   and what that undo costs.

Header block: `Status` (Proposed / Accepted / Superseded by ADR-NNNN), `Date`,
and `Spec references` pointing at the normative spec sections the ADR grounds
in.
