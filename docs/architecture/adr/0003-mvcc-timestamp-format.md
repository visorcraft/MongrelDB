# ADR-0003: MVCC Timestamp Format

- Status: Accepted
- Date: 2026-07-16
- Spec references: sections 4.5 (MVCC), 4.10 (versioning), 8.1 (target
  timestamp), 8.2 (clock rules), 8.3 (row version), 8.4 (legacy migration)

## Context

MVCC today is stamped with `Epoch(pub u64)`
(`crates/mongreldb-core/src/epoch.rs` line 14): a monotonically increasing
commit number bumped once per committed transaction by an atomic
`EpochClock`. Readers pin a `Snapshot { epoch }` and observe exactly the
versions with `committed_epoch <= snapshot.epoch`. This gives
single-node correctness-by-construction — the same epoch tags cache
entries and WAL records — and the three isolation levels (`Snapshot`,
`ReadCommitted`, `Serializable`, `crates/mongreldb-core/src/txn.rs`
~line 1328) are all derived from it.

A bare epoch counter cannot serve the replicated and sharded modes: it
carries no physical time (so no bounded-staleness reads, no
time-travel-by-wall-clock, no skew monitoring), and it cannot order
commits across nodes without a single allocator. Spec section 8 defines
the target model: a hybrid logical clock with lexicographic ordering,
explicit clock rules, and a disciplined migration path from the legacy
epoch format.

## Decision

Adopt `HlcTimestamp` as the target commit/visibility timestamp, stored
alongside the legacy `Epoch(u64)` during migration:

```rust
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct HlcTimestamp {
    pub physical_micros: u64,
    pub logical: u32,
    pub node_tiebreaker: u32,
}
```

Ordering is lexicographic by `(physical_micros, logical,
node_tiebreaker)`; the type is seeded in
`crates/mongreldb-types/src/hlc.rs` with derived `Ord` implementing that
order via field declaration order.

- **Clock rules (section 8.2).** A node clock exposes `now()`,
  `observe(remote)`, and `next_after(minimum)`. Physical time may move
  backward; HLC ordering may not. A received timestamp advances the local
  clock. Commit timestamps are greater than every participant read/write
  timestamp. Clock skew is monitored, and excessive skew rejects
  leadership or timestamp allocation.
- **Row versions (section 8.3).** Committed row versions carry an
  immutable `commit_ts: HlcTimestamp`; visibility keeps the current
  semantics (a version is visible iff its commit timestamp is at or below
  the snapshot's), per section 4.5. Tombstone versions remain an allowed
  implementation of deletes provided visibility semantics match.
- **Migration representation (section 8.4).** Stored row versions use the
  explicit, seeded `StoredVersionStamp` enum in
  `crates/mongreldb-types/src/hlc.rs`:

  ```rust
  enum StoredVersionStamp {
      LegacyEpoch(u64),
      Hlc(HlcTimestamp),
  }
  ```

  The database durably stores two fields: `mvcc_format_version` and
  `migration_watermark`. All `LegacyEpoch` values sort before the
  migration watermark; new HLC values sort at or after it. A compaction
  rewrites legacy versions to the HLC representation. The format is
  always explicit — it is **never inferred from byte length**, matching
  the section 4.10 rule that durable formats are versioned envelopes that
  fail closed.
- **Coexistence.** `Epoch(u64)` remains the standalone commit number and
  cache-invalidation tag while the migration watermark is unset; the WAL
  epoch and `LogPosition.index` continue to use it (see ADR-0002). HLC
  becomes the stamp that replication, cross-node snapshots, and
  bounded-staleness reads compare.

## Alternatives Considered

- **Keep `Epoch(u64)` forever.** Rejected: no physical time, no cross-node
  ordering without a single allocator, and no skew detection — it cannot
  express the section 2.4/2.5 modes' read-consistency levels.
- **Raw wall-clock micros.** Rejected: clock skew and NTP steps would
  violate the monotonicity MVCC visibility depends on; the logical
  component exists precisely to keep ordering when physical time stalls or
  regresses.
- **Lamport-only logical clock (no physical component).** Rejected:
  loses wall-clock semantics needed for bounded-staleness reads and
  operational time travel.
- **Infer the stored format from byte length** (8 bytes = epoch,
  16 bytes = HLC). Rejected explicitly by spec section 8.4 and by section
  4.10's fail-closed versioning; a future third representation or a
  corrupted length field would silently misorder versions.

## Consequences

- Every durable location that stores a commit stamp gains an explicit
  versioned representation (`StoredVersionStamp`), and comparisons during
  migration are two-mode: legacy-before-watermark, HLC-at-or-after.
- `HlcTimestamp` is 16 bytes versus the epoch's 8, doubling per-version
  stamp storage until compaction rewrites legacy versions; the lexicographic
  layout keeps comparison cheap and order-preserving for byte-encoded keys.
- Commit allocation gains clock-skew monitoring and a failure mode
  (excessive skew rejects timestamp allocation) that single-node Epoch
  never had; this is deliberate fail-closed behavior.
- Cache invalidation keyed on `Epoch` (see `epoch.rs`) is unaffected
  during migration; HLC keys take over only for replicated visibility.

## Migration

1. Write `mvcc_format_version = 1` (or the current legacy value) and an
   unset `migration_watermark` into durable root metadata on first open
   after upgrade; absence of both means fully legacy.
2. Begin stamping new commits with HLC at or above the watermark once the
   watermark is set; legacy versions continue to compare below it.
3. Compactions rewrite `LegacyEpoch` stamps to their watermark-mapped HLC
   representation; when no legacy stamps remain, `mvcc_format_version`
   advances and the two-mode comparison path can retire.
4. Throughout, format is read only from the explicit enum tag and the two
   durable fields — never from byte length.

## Reversal Strategy

- Until the watermark is set, the system is byte-identical to today:
  HLC types are additive in `mongreldb-types`, and `Epoch(u64)` remains
  authoritative. Reversal is deleting the unused types.
- After the watermark is set but before compaction completes, reversal
  means freezing HLC allocation and continuing to read both stamps —
  possible because the explicit `StoredVersionStamp` tag preserves the
  original legacy values losslessly.
- After legacy stamps are fully rewritten, reversal requires a new
  migration ADR mapping HLC stamps back to epochs; the watermark mapping
  makes that mechanical but it is not free, so crossing the watermark is
  the point of no cheap return.
