# MULTITABLE.md — Level 2: native multi-table database

**Status:** Draft / proposed (rev 2 — hardened by an adversarial peer-review pass).
**Created:** 2026-06-26. **Scope:** core engine (`mongreldb-core`), query layer
(`mongreldb-query`), Node addon (`mongreldb-node`), daemon + client
(`mongreldb-server`/`mongreldb-client`). **Supersedes:** the "register N `Db`
handles on a `MongrelSession`" pattern as the *primary* multi-table story (it
remains available as a thin compatibility shim).

> **`(review fix #N)` annotations** throughout this document trace each correctness
> fix back to a finding from the rev-1 adversarial review (concurrency/MVCC/recovery
> /durability/encryption). They mark the subtle invariants an implementer must not
> regress; do not remove them when implementing.

This is the **Level 2** design referenced in agent discussion: one database that
owns *many* tables under a **single shared WAL**, a **single epoch clock**, a
**catalog**, **atomic cross-table transactions**, and **consistent cross-table
snapshots** — replacing today's "one `Db` = one table = one directory = one WAL
fd" model. The on-disk WAL record format is already table-aware
(`Op::Put/Delete/TruncateTable` carry `table_id`, `wal.rs:36-52`), so the bulk of
the work is **ownership, control flow, recovery, and concurrency**, not the log
byte format.

Because **MongrelDB has no production users yet**, this spec takes a **clean
break**: no on-disk migration path, no dual-mode compatibility. Old single-table
directories are not auto-upgraded (an optional one-shot `import` tool is listed
under Non-goals/Future).

---

## 1. Goals / Non-goals

### 1.1 Goals
1. **One database, many tables.** A `Database` opens a root directory and manages
   N tables created/dropped at runtime via a catalog (DDL).
2. **One WAL, one fd.** All tables share a single append-only WAL. Open-database
   fd count is O(1) in tables (one WAL writer + transient run readers), not O(N).
3. **Atomic cross-table transactions.** A transaction spanning multiple tables
   commits all-or-nothing under one epoch.
4. **Genuinely concurrent writers.** Multiple transactions prepare, fsync (group
   commit), and publish in parallel; the only serialized step is a microsecond
   commit-sequencer critical section. Same-key write–write conflicts abort one
   side (retryable); disjoint writers never block each other.
5. **Unbounded transaction size.** A transaction larger than memory spills its
   staged writes to disk (per-txn runs / prepared WAL records) — there is **no
   hard size cap**.
6. **Consistent cross-table snapshots.** A reader (incl. a multi-table SQL join)
   observes a single point-in-time across *all* tables — no torn reads, and never
   a partially-published transaction.
7. **Shared caches with a global budget.** One page cache / decoded-page cache for
   the whole database, keyed by `(table_id, run_id, column, page)`.
8. **One key, many tables.** A single passphrase / key file derives a
   database-level KEK; per-table and per-run subkeys descend from it.
9. **Native NAPI multi-table API.** `mongreldb-node` exposes a `Database` with
   table handles, cross-table SQL, and cross-table transactions — not one JS
   `Database` object per table.
10. **No regression** to single-row write latency, scan throughput, or the
   security guarantees added recently (per-page AEAD, run-metadata MAC, encrypted
   WAL/result-cache/index-checkpoint).

### 1.2 Isolation level (in scope, stated precisely)
The engine provides **snapshot isolation (SI)** under **genuinely concurrent
writers** (§9), with **first-committer-wins** write–write conflict detection:
- Concurrent transactions whose write sets are disjoint all commit and never block
  each other.
- Two concurrent transactions that both write the same `(table_id, row_id)` →
  whichever validates second aborts with a **retryable** `Conflict` error (the
  common single-row, distinct-key write path therefore never conflicts).
- Full **serializable** (SSI, read-write anti-dependency tracking) is *not*
  provided; callers needing it use coarser write keys or app-level coordination.
  This is a stated isolation level, not a deferral — every Level-2 capability
  (concurrency, atomicity, large transactions) is fully specified here.

### 1.3 Non-goals
- Foreign-key / referential-integrity constraints, cascades, triggers.
- Distributed / multi-node, replication, sharding.
- Cross-database queries (joins across two separate `Database` roots).
- A `.sr`/WAL byte-format migration from the old single-table layout (an optional
  offline `import-table` utility is Future Work — tooling only; it gates no
  Level-2 capability).

---

## 2. Current architecture (what's coupled to one table)

Evidence from the tree, so the redesign is grounded:

| Concern | Today (per-table) | Reference |
|---|---|---|
| Open unit | `Db` = "An open MongrelDB table" | `engine.rs:45` |
| Directory | flat root: `_wal/ _runs/ _cache/ _rcache/ _idx/ _meta/ _mf schema.json` | `engine.rs:30-36`, `manifest.rs:15`, `global_idx.rs:30` |
| WAL | per-table `Wal` (one `BufWriter<File>` fd) | `wal.rs` |
| WAL records | **already carry `table_id`** (discarded on replay) | `wal.rs:36-52`, `engine.rs:857` |
| Epoch clock | per-table `EpochClock` (atomic u64) | `epoch.rs:44` |
| Manifest | per-table (`table_id, current_epoch, next_row_id, runs, live_count, …`) | `manifest.rs:26` |
| RowId allocator | per-table | `engine.rs` |
| Indexes + checkpoint | per-table; `_idx/global.idx` | `global_idx.rs` |
| Caches | per-table `page_cache`/`decoded_cache`/`result_cache`, each its own budget | `engine.rs:767-777` |
| Encryption | per-table KEK (passphrase + per-table salt in `_meta/keys`) | `engine.rs:654-708` |
| Multi-table (read) | `MongrelSession.tables: HashMap<String, Arc<Mutex<Db>>>` + `register_db` | `lib.rs:780/855` |
| Cross-table "snapshot" | **faked**: `combined_epoch` hashes each table's epoch (mul-31) for cache-keying only | `lib.rs:962` |

The `combined_epoch` hash is the clearest tell that cross-table consistency does
not exist today; Level 2 replaces it with a real database epoch.

---

## 3. Design overview

### 3.1 Type model
Introduce a container and **rename the per-table engine**:

- `Database` — owns the root dir, the **shared WAL**, the **single `EpochClock`**,
  the **catalog**, the **shared caches**, and the **DB KEK**. Holds
  `tables: RwLock<HashMap<TableId, Arc<Table>>>`.
- `Table` (renamed from `Db`) — owns **only table-local** state: `schema`,
  `memtable`, `mutable_run`, the index set, `run_refs`, per-table
  `RowIdAllocator`, per-table `live_count`, per-table column keys. It **borrows**
  shared services from its `Database` (WAL, clock, caches, KEK) rather than
  owning them. A `Table` is never opened standalone.
- `TableId(u64)` — stable per table for the database's lifetime; assigned by the
  catalog, never reused after drop (tombstoned).
- `Snapshot { epoch }` — unchanged shape (`epoch.rs:23`) but now a **database-wide**
  point-in-time: valid across every table.
- `Transaction<'db>` — a write handle accumulating ops across tables, committed
  atomically.
- `TableHandle` — a cheap `Arc<Table>` + back-reference to `Database`, the public
  read/write entry point per table.

> **Naming note.** `Db` → `Table` is a large mechanical rename across all crates.
> It is conceptually correct (a `Database` *contains* `Table`s) and avoids the
> confusion of a type named `Db` that means "one table". Do it as the first,
> isolated commit (§17).

### 3.2 Shared vs. table-local (authoritative split)
```
Database (shared)                         Table (per-table, under Database)
─────────────────                         ────────────────────────────────
root dir + catalog                        schema (+ schema_id)
EpochClock (one)                          memtable (Bε-tree skip-list)
SharedWal (one fd)                        mutable_run (PMA tier)
PageCache / DecodedPageCache (one each)   indexes: HOT/Bitmap/PGM/FM/HNSW/PMA/Sparse
ResultCache (DB-epoch keyed)              run_refs: Vec<RunRef>
KEK (one, from one passphrase/key)        RowIdAllocator
WAL-DEK / cache-DEK / idx-DEK             live_count
                                          column_keys (ENCRYPTED_INDEXABLE)
                                          flushed_epoch watermark
```

### 3.3 What stays the same
- `.sr` sorted-run format, the columnar codec, the seven index types, page stats,
  per-page AEAD + nonce derivation, the **run-metadata MAC**, the encrypted index
  checkpoint, and per-table reader/writer code paths are unchanged. Runs stay
  under a per-table subdir; only their *containing* services move up to `Database`.
- The WAL **frame** format (len/crc/seq/payload, optional `[nonce][GCM]`) is
  unchanged. Recovery semantics (torn-tail = clean EOF) are extended to
  transaction granularity (§8.4), not replaced.

---

## 4. On-disk layout

```
<root>/
  CATALOG                  # database catalog (atomic; see §7). Root of truth.
  _meta/
    keys                   # 16-byte DB-level Argon2id salt (encryption only)
  _wal/                    # SHARED WAL segments: seg-NNNNNN.wal (records carry table_id)
  _cache/                  # OPTIONAL shared persistent page cache (ciphertext pages)
  _rcache/                 # OPTIONAL shared result cache (encrypted blobs)
  tables/
    <table_id>/            # one subdir per live table, named by numeric TableId
      manifest             # per-table run set + live_count + flushed_epoch (atomic, enc+auth)
      schema.json          # per-table schema (also mirrored in CATALOG for fast open)
      _runs/               # r-<run_id>.sr immutable sorted runs
      _idx/global.idx      # per-table index checkpoint (encrypted if DB encrypted)
```

Rationale:
- **Shared `_wal/` at root** is the heart of Level 2 (one fd, one log, one
  recovery stream, atomic multi-table groups).
- **Per-table subdirs** keep runs/indexes/manifest isolated, so per-table compaction,
  GC, and `read_*` code paths stay byte-for-byte as they are today (only the base
  path changes from `<db>/_runs` to `<db>/tables/<id>/_runs`).
- The catalog mirrors each schema so `Database::open` does not need to stat every
  subdir before knowing the table set; the per-table `schema.json` remains the
  authority consumed by the per-table reader/writer.

---

## 5. Catalog (`CATALOG`)

The catalog is a **checkpoint, not the durability authority** (review fix #16).
DDL is logged to the WAL as ordinary transactions (§7.1) and is durable there; the
`CATALOG` file is a periodically-rewritten snapshot of the table set that lets
`open` skip replaying all historical DDL, exactly as `_idx/global.idx` checkpoints
indexes. Recovery loads the catalog checkpoint, then replays WAL DDL ops with
`epoch > catalog.db_epoch` to bring it current (§15).

### 5.1 Contents
A small file (magic + version + bincode body + SHA-256; for an encrypted DB it is
**encrypted AND authenticated** with the DB metadata DEK — §11, review fix #20):

```rust
struct Catalog {
    magic: [u8; 8],              // b"MONGRCAT"
    format_version: u16,
    db_epoch: u64,               // commit epoch this checkpoint reflects (replay anchor)
    next_table_id: u64,          // monotonic; never reused
    open_generation: u64,        // bumped+fsynced on every Database::open (review fix #11)
    next_segment_no: u64,        // monotonic WAL segment number (review fix #23)
    tables: Vec<CatalogEntry>,
    checksum: [u8; 32],          // over the preceding bytes (pre-encryption)
}
struct CatalogEntry {
    table_id: u64,
    name: String,                // unique, case-sensitive
    schema: Schema,              // mirror of tables/<id>/schema.json
    state: TableState,
    created_epoch: u64,
}
// Dropped tables are retained until no active snapshot can still observe them (§6.4):
enum TableState { Live, Dropped { at_epoch: u64 } }
```

### 5.2 Durability & ordering
- **Crash-atomic file writes** use write-tmp → fsync(tmp) → rename → **fsync(parent
  dir)**. The directory fsync is mandatory and must be added to the catalog,
  per-table manifest, index-checkpoint, and schema-mirror writers — today's
  `manifest::write_atomic` / `global_idx::write_atomic` omit it (review fix #19).
- **Replay anchor:** on open the clock starts at `catalog.db_epoch` and is advanced
  by WAL replay (§15); `db_epoch` is not authoritative on its own.
- **DDL is a WAL transaction** (review fix #16): `create_table`/`drop_table`/
  `alter_table` append a `DdlOp` record + `TxnCommit` through the normal commit path
  (§9.3), getting a `commit_epoch` ordered against data commits. The in-memory
  catalog is updated at **publish** (after the group fsync), and the `CATALOG` file
  is rewritten lazily as a checkpoint — so no fsync happens inside the commit
  sequencer (resolves the §9.5 "no I/O in sequencer" contradiction).
- **`open_generation`** is incremented and fsynced (with `next_segment_no`) as the
  first step of every `Database::open`, *before* any new WAL append, so freshly
  allocated `txn_id`s and segment numbers never collide with values still present in
  un-reclaimed WAL segments (review fixes #11, #23).

---

## 6. Epoch & MVCC (the consistency payoff)

### 6.1 One clock, two counters
`Database` owns the single epoch authority (extends `EpochClock`, `epoch.rs:44`).
Under concurrent committers it exposes **two monotonic values**:
- **`assigned`** — bumped inside the commit sequencer (§9.3); a transaction's
  `commit_epoch` is its `assigned` value and defines the global serialization
  order. Concurrent committers get strictly increasing `assigned` epochs.
- **`visible`** — the reader watermark, advanced **in `assigned` order** only after
  a transaction has fully published (§9.3 step g). A reader's snapshot is the
  current `visible`. Because `visible` never skips ahead of an un-published commit,
  a reader can never observe a partially-applied transaction.

To keep `visible` from stalling, a `commit_epoch` is **only consumed by a
transaction that has passed validation** (validate-before-assign, §9.3b, review fix
#1): an aborted transaction never holds an epoch, so there are no "dead" epochs the
in-order advancer could wait on forever. (A no-op `Completed(epoch)` gap marker is
the belt-and-suspenders fallback if any internal path ever assigns then abandons an
epoch.) `commit()` returns only **after** its `commit_epoch <= visible`, so a
successful commit is both durable *and* visible to subsequent `snapshot()` calls.

With no in-flight concurrency `visible == assigned`; the split exists purely so
concurrent commits publish without exposing torn state. Every run, memtable
version, tombstone, and cache entry across all tables is tagged with the
committing transaction's `commit_epoch`.

### 6.2 Snapshots
`Database::snapshot() -> Snapshot` reads the **`visible`** watermark once. A reader
pins it and passes the **same** `Snapshot` to every table it touches:
```rust
let snap = db.snapshot();
let a = db.table("orders")?.scan(snap, ...);
let b = db.table("items")?.scan(snap, ...);   // same instant as `a`
```
Because all tables share the clock and tag versions with it, the join of `a` and
`b` is a genuine point-in-time view. The `combined_epoch` hash (`lib.rs:962`) is
**deleted**; the result-cache key becomes `(sql, snapshot.epoch)` with a real
database epoch.

### 6.3 Visibility rule (unchanged, now global)
A version is visible to `snap` iff `committed_epoch <= snap.epoch` — exactly
today's rule (`epoch.rs:36`), but the epoch space is now database-wide.

Indexes (HOT/Bitmap/PGM/FM/HNSW/Sparse) are treated as **append-only candidate
supersets**: publish only *adds* entries, never removes them on delete/update (a
tombstone or superseding version handles correctness). Index probes therefore
return a superset of row-ids that is then **visibility-filtered** against the
reader's snapshot — exactly the model the current engine already relies on — so a
concurrent delete at epoch 11 cannot strip an entry a reader pinned at 10 still
needs (review fix #2). `live_count` (the O(1) `COUNT(*)` source, `manifest.rs:35`)
reflects the **latest committed epoch**, not the reader's snapshot — a documented
limitation carried over from today; a snapshot-exact count uses the scan path.

### 6.4 Snapshot retention (global)
Because compaction, `DROP TABLE`, run replacement, and WAL-segment GC all *retire*
data, they must not retire anything an in-flight reader still needs. `Database`
keeps a **global active-snapshot registry** (review fix #3):
- `snapshot()` registers its `epoch` (a refcounted multiset / min-tracker) and a
  `SnapshotGuard` deregisters on drop. `min_active_snapshot` = the lowest live
  registered epoch (or `visible` if none).
- **Nothing physically retired below `min_active_snapshot`.** Compaction keeps
  superseded runs, `DROP` keeps the table subdir + catalog entry (state `Dropped`,
  invisible to new snapshots), and run/segment deletion all wait until
  `min_active_snapshot > retire_epoch`. A background reaper performs the deferred
  deletes once the watermark advances.
- This is the database-wide generalization of today's per-run "compaction with
  snapshot retention."

---

## 7. Shared WAL

### 7.1 Record envelope: `txn_id` routing
With concurrent writers, records from different in-flight transactions
**interleave** in the one log, so each record must say which transaction and which
table it belongs to. The WAL `Record` envelope gains a `txn_id` (the frame already
carries `seq`; CRC now covers `seq ‖ txn_id ‖ payload`):

```rust
// txn_id is `(open_generation:u32, local:u32)` packed into u64 (review fix #11):
// generation-scoped so a fresh counter after restart never aliases an old segment's
// txn_id. txn_id == 0 is reserved for SYSTEM records (Flush) outside any txn (#21).
struct Record { seq: u64, txn_id: u64, op: Op }    // txn_id is new; CRC covers seq‖txn_id‖payload
```

`Op` already carries `table_id` for Put/Delete/TruncateTable. Format changes:
1. **`Op::Flush { table_id, flushed_epoch }`** — a SYSTEM record (`txn_id == 0`,
   excluded from txn bucketing, review fix #21). `flushed_epoch` is the highest
   *commit epoch* whose data for `table_id` is now durably in runs — **not** a raw
   WAL seq (review fix #4).
2. **`Op::TxnCommit { epoch, added_runs: Vec<AddedRun> }`** — finalizes `txn_id`;
   `epoch` is the authoritative `commit_epoch`; `added_runs` carries everything
   recovery needs to link a spilled run **without reading it** (review fixes #7, #8):
   ```rust
   struct AddedRun { table_id: u64, run_id: u128, row_count: u64, level: u8,
                     min_row_id: u64, max_row_id: u64, content_hash: [u8;32] }
   ```
   The run's rows are **not** stamped with a per-row epoch (it isn't known at spill
   time, review fix #7); the run is a *uniform-epoch* run whose visibility epoch is
   `RunRef.commit_epoch`, set from this marker at link time (§8.5).
3. **`Op::TxnAbort`** — marks `txn_id` abandoned so recovery reclaims its prepared
   records + spilled runs eagerly (end-of-log uncommitted txns are discarded anyway).
4. **`Op::Ddl(DdlOp)`** — `CreateTable{table_id,name,schema}` / `DropTable{table_id}`
   / `AlterTable{table_id, change}`, committed as a normal txn (review fix #16); the
   `CATALOG` file is a rebuildable checkpoint of the resulting state (§5).

Per-record epoch fields (e.g. `Delete.epoch`, `wal.rs:44-48`) are dropped — versions
are stamped with the txn's `commit_epoch` from `TxnCommit` at apply.

### 7.2 Write API
```rust
impl SharedWal {
    // Append one prepared record for a txn (bounded-size batches, §9.3a). Returns seq.
    fn append(&mut self, txn_id: u64, table_id: u64, op: Op) -> Result<u64>;
    fn append_commit(&mut self, txn_id: u64, epoch: Epoch, added: &[AddedRun]) -> Result<u64>;
    fn append_abort(&mut self, txn_id: u64) -> Result<()>;
    fn append_system(&mut self, op: Op) -> Result<u64>;            // txn_id = 0 (Flush)
    // Durability: returns the highest seq guaranteed on disk after this call (#15).
    fn group_sync(&mut self) -> Result<u64 /* durable_seq */>;
}
```
- `seq` is the WAL's monotonic counter; `txn_id` is generation-scoped (above);
  `commit_epoch` is separate (assigned in the sequencer, §9.3b).
- The active-segment fd is the single persistent DB fd, shared by all writers. Every
  append is a **bounded buffered memcpy** (large data is appended in bounded batches
  during prepare, never as one GB-sized op, review fix #17), so no single critical
  section does unbounded work.

### 7.3 Encryption: deterministic, collision-free nonces (review fix #23)
The shared WAL is encrypted with one DB WAL-DEK (`KEK.derive_wal_key()`). Because the
DEK is constant for the database's life, a **random** per-segment seed only gives a
birthday-bounded guarantee. Instead the nonce base is **deterministic and unique**:
the 12-byte nonce = `segment_no (8B, the persisted monotonic `next_segment_no`, §5)
‖ frame_counter (4B)`. Segment numbers never repeat (persisted, fsynced before reuse,
survive reopen) and the frame counter is unique within a segment (overflow refused,
forcing rotation), so **no (key, nonce) pair ever repeats** — a hard guarantee, not a
probability. (This supersedes the random-seed scheme; for the greenfield Level-2
format we do it deterministically.)

### 7.4 Flush, watermarks, segment GC (review fixes #4, #5, #9, #18)
- **Generation-sealed flush (#9).** To flush `Table T` it atomically **seals** its
  active memtable — swapping in a fresh empty generation via an atomic pointer — so
  concurrent publishers insert into the new generation while the sealed one is
  immutable. The sealed generation contains exactly the committed rows with
  `commit_epoch <= seal_epoch`. Only sealed generations are flushed to a run.
- **`flushed_epoch` recovery gating (#4).** After the run is durable, `T`'s manifest
  records `flushed_epoch = seal_epoch` (atomic, §5.2). Recovery applies a committed
  txn's record for `T` iff `txn.commit_epoch > T.flushed_epoch` — gating by **commit
  epoch, not raw seq**, so a record streamed early (low seq) but committed *after* a
  flush is still replayed (its `commit_epoch` exceeds `flushed_epoch`).
- **Segment GC (#4, #5).** A segment is reclaimable only when every record in it is
  accounted for: define `min_retained_seq = min(` lowest seq of any **in-flight /
  in-doubt** txn, lowest seq of any **committed-but-not-yet-flushed** txn `)`. A
  segment with `max_seq < min_retained_seq` AND `max_seq` below the snapshot-retention
  bound (§6.4) may be deleted. This never deletes a segment holding prepared records
  of a txn that has not yet durably flushed (closes the #5 data-loss window).
- **Flush lock discipline (#18).** Flush appends its `Op::Flush` and seals the
  generation **without holding any table lock while touching the WAL**; it takes `T`'s
  `publish_lock` only to swap the generation pointer and the `runs` `ArcSwap`. Global
  lock order `WAL/sequencer → publish_lock` is preserved (§9.5).

### 7.5 Rotation
Rotation is database-global (size/age threshold). Each new segment takes the next
`next_segment_no` (persisted) → its deterministic nonce base (§7.3).

---

## 8. Transactions

### 8.1 API (core)
```rust
let mut txn = db.begin();                       // borrows the write path
txn.put("orders", row)?;                        // table-qualified mutations
txn.delete("items", rid)?;
txn.put_batch("audit", rows)?;
let epoch = txn.commit()?;                       // one WAL group + one fsync + one bump
// or: txn.rollback();                           // drop without commit
```
- `db.put(table, ...)` / `db.delete(...)` outside an explicit txn are **implicit
  single-statement transactions** (begin→op→commit), preserving today's
  ergonomics and the ~6 µs single-row path (one op, one fsync).
- A closure helper: `db.transaction(|txn| { ...; Ok(()) })` commits on `Ok`,
  rolls back on `Err`/panic.

### 8.2 Lifecycle (concurrent, optimistic)
Transactions are **prepared concurrently** and finalized in a bounded
commit-sequencer step (§9.3). Full lifecycle:

1. **begin** — allocate a generation-scoped `txn_id` (§7.1); **register in
   `ActiveTxns` before reading** (so the conflict-index pruner can never drop a key
   this txn might need, review fix #12); then capture `read_epoch = visible` (§6.1).
   Create private staging: buffered rows/tombstones, plus the **write-set keys**
   (§8.3). No global locks.
2. **buffer + stream** (concurrent, no global lock) — `put`/`delete` append to
   private staging; reads merge `read_epoch` with staging. As staging grows it
   **spills** (§8.5): row records stream to the WAL in **bounded batches** (each a
   brief WAL-mutex memcpy, never one huge op), and bulk data spills to per-txn runs.
   All of a txn's *data* records are thus appended here, during prepare — **not** in
   the sequencer (review fix #17).
3. **commit — sequencer (bounded; no I/O, no disk reads, review fixes #1, #17):**
   a. **validate first** against the in-memory conflict index: any key written by a
      committer with `commit_epoch > read_epoch` ⇒ **abort** (`Err(Conflict)`,
      retryable). Validation uses pre-materialized in-memory key fingerprints; a huge
      spilled key set is pre-checked outside the sequencer, then only the small
      *delta* since that pre-check is re-validated here.
   b. **only on success, assign** `commit_epoch = clock.bump_assigned()` (so an abort
      consumes no epoch — `visible` can't stall, review fix #1);
   c. append **only** the bounded `TxnCommit { epoch, added_runs }` marker, and record
      this txn's write-set keys → `commit_epoch` in the conflict index;
   d. leave the sequencer.
4. **group fsync** (concurrent) — join the batch; the leader fsyncs and returns a
   `durable_seq`; this txn proceeds only once `durable_seq >= its TxnCommit seq`
   (review fix #15). **Durability point.** An fsync error **poisons** the DB (§9.3e).
5. **publish** (concurrent across tables) — apply staging to each table: lock-free
   memtable insert at `commit_epoch` into the *current generation* (§7.4);
   **append-only** index updates (no deletions, §6.3) and `run_refs`/`live_count`
   swaps under that table's brief `publish_lock`; link spilled runs at
   `commit_epoch` (§8.5).
6. **advance `visible`** — in `assigned` order, once this txn and all lower epochs
   have published (§6.1). `commit()` returns after `commit_epoch <= visible`.

`rollback()`/drop appends `TxnAbort`, drops staging, GCs spilled runs, deregisters
from `ActiveTxns`. **Implicit single-statement writes** run this same path with a
one-key write set; distinct keys never conflict, so the hot single-row path keeps
~6 µs latency and *improves* under load via group-fsync amortization.

### 8.3 Isolation (snapshot isolation, first-committer-wins) — conflict keys
- **Reads:** snapshot isolation, database-wide (§6).
- **Writes:** optimistic; two txns conflict iff their write-sets share a key and the
  later committer's `read_epoch < other.commit_epoch`. First committer wins; loser
  gets retryable `Err(Conflict)`.
- **Write-set keys are broader than `(table_id,row_id)`** (review fix #13) — the set
  is the union of:
  - **row-version keys** `(table_id, row_id)` for updates/deletes of existing rows;
  - **unique keys** `(table_id, unique_index_id, key_hash)` for every PK / UNIQUE
    column an insert or update touches — so two concurrent inserts of the *same PK*
    (which get *different* row_ids) **conflict** and one aborts, preserving
    uniqueness under concurrency;
  - **table-scope keys** `(table_id, TABLE)` taken by `TRUNCATE`/`DROP`/`ALTER` and,
    as a reader-side marker, by any txn writing that table — so DDL conflicts with
    concurrent DML on the same table.
- SSI write-skew is out of scope (§1.2).

### 8.4 Recovery (interleaved, bounded, epoch-ordered)
Recovery must handle interleaved txns **without buffering whole transactions in
memory** (review fix #10) and without `txn_id` aliasing (review fix #11):

1. **Pass 1 (scan markers).** Stream the WAL recording, per `txn_id`: outcome
   (committed `epoch` + `added_runs`, aborted, or absent⇒in-flight) and its seq
   range. `txn_id`s carry the `open_generation` that wrote them, so a restarted
   counter never merges with old segments. This pass holds only small per-txn
   metadata.
2. **Pass 2 (apply committed, epoch-ordered).** Re-stream; for each record whose
   `txn_id` committed, apply it to its table **iff `commit_epoch > table.flushed_epoch`**
   (review fix #4) at `commit_epoch`. Large txns' bulk data is **already on disk** as
   pending runs — recovery just **links** `added_runs` (cheap, no buffering); only
   small streamed row-records are applied directly. If the set of concurrently-open
   committed txns' streamed records ever exceeds a memory bound, recovery spills its
   apply buffers (same mechanism as §8.5). Apply respects global epoch order across
   tables.
3. **Discard** aborted, in-flight-at-EOF, and torn-trailing txns; GC their pending
   runs (review fix #14 quarantine makes this a directory sweep).
4. Set `assigned` and `visible` to the max committed epoch; rebuild the catalog from
   its checkpoint + replayed `Ddl` ops (§5).

**Torn vs. corrupt (review fix #22):** a CRC failure or short read on the **last**
frame is a torn tail → truncate and treat as clean EOF; a CRC failure on a frame
*followed by* a well-formed later frame is interior corruption → hard error. This
requires extending `WalReader` (today any CRC mismatch errors, `wal.rs:340-345`).

### 8.5 Large transactions (unbounded — spill, no cap)
No hard size limit. Three spill mechanisms keep peak memory bounded:

- **Per-txn pending runs (bulk).** When a table's staged bytes cross
  `spill_threshold`, staged rows are written via the bulk run writer into a
  **quarantined** run `tables/<id>/_txn/<txn_id>/r-<run_id>.sr` (review fix #14) —
  fully encoded/compressed/encrypted. GC never touches `_txn/` for a *live* txn_id,
  so a pending run can't be deleted mid-txn. The run is a **uniform-epoch run**: its
  rows carry no per-row epoch (unknown at spill time, review fix #7); the visibility
  epoch comes from the `RunRef.commit_epoch` set at link time. At commit,
  `TxnCommit.added_runs` (typed `AddedRun`, review fix #8) names each run; publish
  atomically moves it into `tables/<id>/_runs/` and adds the `RunRef` at
  `commit_epoch`. Recovery re-links from `added_runs`; abort/crash leaves the run in
  `_txn/` for the directory sweep.
- **Conflict-key spill.** A huge write-set key list spills to a temp sorted set;
  validation pre-checks it outside the sequencer and re-checks only the small recent
  delta inside (§8.2.3a), keeping the sequencer bounded. Validation cost is
  O(write-set) — unavoidable for huge txns, never traded for correctness.
- **Streamed row records.** Non-bulk staged records stream to the WAL in bounded
  batches during prepare; publish/recovery replay them from the log.
- `txn.spill_now()` forces a flush at a natural boundary; otherwise spilling is
  automatic and invisible.

---

## 9. Concurrency model (concurrent writers, MVCC, group commit)

The cardinal rules: **no giant `Mutex<Database>`** (today each table is one
`Arc<Mutex<Db>>`, `lib.rs:780`), and **the only serialized step is a microsecond
commit-sequencer critical section** — preparation, fsync, and publish all run in
parallel.

### 9.1 State & locks
```rust
struct Database {
    epoch: EpochAuthority,            // `assigned` + in-order `visible` watermark (§6.1)
    seq: CommitSequencer,             // bounded critical section: validate+epoch+marker
    group: GroupCommit,               // leader/follower fsync, exposes durable_seq (§9.3e)
    conflicts: ConflictIndex,         // sharded write-key -> commit_epoch (§9.2)
    active: ActiveTxns,               // registered read_epochs; min drives pruning (§9.2)
    snapshots: SnapshotRegistry,      // min_active_snapshot for retention (§6.4)
    generation: u64,                  // this open's txn_id namespace (persisted, §5)
    txn_ctr: AtomicU64,               // local counter; txn_id = (generation, ctr)
    poisoned: AtomicBool,             // set on fsync/IO failure → all writes fail (§9.3e)
    catalog: RwLock<Catalog>,         // in-memory; checkpoint of WAL-logged DDL (§5)
    tables: RwLock<HashMap<TableId, Arc<Table>>>,
    page_cache: Arc<Mutex<PageCache>>,            // shared; internally synchronized
    decoded_cache: Arc<Mutex<DecodedPageCache>>,
    kek: Option<Arc<Kek>>,
}
struct CommitSequencer { wal: Mutex<SharedWal> }  // the one serialized commit point
struct Table {
    schema: ArcSwap<Schema>,          // lock-free read; swapped on ALTER publish
    memtable: ArcSwap<MemGen>,        // active generation; sealed+swapped on flush (§7.4)
    indexes: ArcSwap<IndexSet>,       // append-only superset; swapped on publish (§6.3)
    runs: ArcSwap<Vec<RunRef>>,       // lock-free read; swapped on publish/flush/compact
    publish_lock: Mutex<()>,          // brief: serializes publishes to THIS table only
    allocator: RowIdAllocator,        // atomic
    live_count: AtomicU64,            // latest-committed (not snapshot-exact, §6.3)
    column_keys: HashMap<u16, ([u8;32], u8)>,
    flushed_epoch: AtomicU64,         // highest commit epoch durably in runs (§7.4)
}
```

### 9.2 Conflict index + active-txn registration
- A sharded concurrent map **write-key → commit_epoch** of recent commits, where a
  write-key is a row-version, unique/PK, or table-scope key (§8.3). Validation
  (§8.2.3a) probes it per write-set key.
- **Registration ordering (review fix #12):** `begin` inserts the txn into
  `ActiveTxns` (contributing its `read_epoch` to `min(active read_epoch)`) **before**
  the txn performs any read or could be affected by pruning, and stays until its
  outcome is final. Thus a key written at epoch `K` is never pruned while a txn with
  `read_epoch < K` is live.
- **Pruning:** entries with `commit_epoch < min(active read_epoch)` can never cause a
  future conflict and are dropped opportunistically.

### 9.3 Write path (concurrent, pipelined)
Steps run in parallel across txns; only the sequencer step is serialized.
- **a. prepare (parallel, no global lock):** build/encode rows, compute index +
  conflict keys, stage/spill (§8.5), and **stream all data records to the WAL in
  bounded batches** (each a brief `seq.wal` memcpy). The expensive work is here and
  fully concurrent.
- **b. sequencer (bounded; review fixes #1, #17):** take `seq.wal`; **validate first**
  against the conflict index (in-memory only; huge key sets pre-checked in (a), only
  the recent delta re-checked here); on conflict **abort with no epoch consumed**.
  On success **assign `commit_epoch`**, append **only** the bounded `TxnCommit`
  marker, record write-set keys at `commit_epoch`, release. No fsync, no disk read,
  no GB copy inside — strictly bounded.
- **c. group fsync (parallel; durable-seq contract, review fix #15):** committers
  queue on `GroupCommit`. A batch **closes at a `high_seq`**; the leader issues one
  `fdatasync`, then sets `durable_seq = high_seq`. A committer publishes **only after
  `durable_seq >= its TxnCommit seq`** (never woken early for bytes outside the
  flushed batch). An fsync **error sets `poisoned`** → every in-flight commit returns
  an error and the DB rejects new writes until reopened.
- **d. publish (parallel across tables):** for each table in the write set: lock-free
  insert into the current `MemGen` at `commit_epoch`; take `publish_lock` only to
  `ArcSwap`-swap `indexes` (append-only) / `runs` and bump `live_count`; link spilled
  runs. Different tables publish in parallel; same-table publishes serialize on that
  table's `publish_lock`.
- **e. advance `visible`:** an in-order advancer (min-heap of finished
  `commit_epoch`s) bumps `visible` to `E` once `E` and all `< E` have published
  (aborted epochs don't exist, §6.1) — readers never see a gap or partial txn.

**DDL** is logged as a `Ddl` op committed through the *same* path (validate against
table-scope conflict keys, append `Ddl`+`TxnCommit`, group fsync, publish by mutating
the in-memory catalog + table map). The `CATALOG` file is rewritten as a checkpoint
**outside** the sequencer (review fix #16) — no fsync inside the critical section.

### 9.4 Read path (lock-free, never blocks on writers)
- `db.snapshot()` reads `visible` and registers in `snapshots` (returns a guard, §6.4).
- Reads resolve against `ArcSwap` `runs`/`schema`/`indexes`/`memtable` (lock-free
  loads) + the shared page cache. A reader on table A is never blocked by readers or
  writers of any table, nor by the sequencer.

### 9.5 Lock ordering & liveness
- **Single order:** `seq.wal` → `publish_lock`. `publish_lock` is taken only *after*
  leaving the sequencer, one table at a time, released immediately. **Flush obeys the
  same order** (review fix #18): it appends `Op::Flush` / seals the generation while
  holding *no* table lock, then takes `publish_lock` only to swap the sealed
  generation + `runs`. No path holds a table lock while entering `seq.wal`, so no
  cycle exists.
- **Bounded sequencer:** performs no fsync, no disk read, no unbounded copy (§9.3b) —
  one slow committer cannot stall the pipeline; fsync is batched outside it.
- **Liveness:** conflict-aborted txns retry with a fresh `read_epoch`; the winning
  committer has advanced, so a retry makes progress (no livelock). A poisoned DB fails
  fast rather than hanging.

---

## 10. Shared caches

- **PageCache / DecodedPageCache**: one instance per `Database`, one global byte
  budget. The cache key already hashes `(run_id, column_id, page_seq)`
  (`sorted_run.rs:page_cache_key`); **prepend `table_id`** so keys are unique
  across tables. Run readers receive the shared cache `Arc` from the `Database`
  (today `open_reader` passes the per-`Db` cache, `engine.rs:3669`).
- **ResultCache**: on the `MongrelSession`, keyed by `(sql, db_epoch)` — now a real
  consistent epoch (§6), so a commit on *any* table correctly invalidates cached
  cross-table results without the `combined_epoch` hash.
- **Persistent `_cache/` / `_rcache/`**: shared at the DB root; the page cache file
  names already encode the content/identity hash, so cross-table collisions are
  impossible once `table_id` is in the key.

---

## 11. Encryption (one key, many tables)

The recently-added security model is preserved and lifted to the database scope:

- **One KEK per database.** `Database::create_encrypted(root, passphrase)` writes a
  single 16-byte salt to `<root>/_meta/keys` and derives the KEK via Argon2id +
  HKDF (or HKDF-only from a key file). `Database::open_encrypted` re-derives it.
- **Subkey derivation** (unchanged primitives, DB-scoped):
  - `derive_wal_key()` → the **single shared** WAL DEK (deterministic, collision-free
    nonces from the persisted segment number, §7.3, review fix #23).
  - `derive_cache_key()` → the shared result-cache DEK.
  - `derive_meta_key()` → the DB **metadata** DEK, used to **encrypt AND authenticate
    every metadata file**: the catalog, each per-table manifest, each index
    checkpoint, and the schema mirror. HKDF info is domain-separated per file kind
    and includes `table_id` where per-table (`mongreldb/meta/<kind>/v1` ‖ table_id).
  - `derive_run_mac_key()` → the run-metadata MAC key (per-run HMAC unchanged).
  - per-run DEK (random, wrapped by the KEK) and per-column indexable keys: exactly
    as today, derived inside each `Table`.
- **At rest, encrypted:** run page payloads (per-page AES-256-GCM), shared WAL frames
  (`_wal/`), result cache (`_rcache/`), and **all metadata** — catalog, per-table
  manifests, index checkpoints, schema mirrors (review fix #20: manifests are now
  encrypted+authenticated, not just SHA-256, so an attacker can't edit `run_refs` /
  `flushed_epoch` and recompute an unkeyed hash).
- **Authenticated (cleartext bytes, keyed integrity):** run header/directory via the
  required per-run HMAC; for a plaintext database, metadata files keep the SHA-256
  corruption check (no key to authenticate with — acceptable, no confidentiality
  claim for a plaintext DB).
- All key material remains `Zeroizing`.

> The min/max zone-map suppression for encrypted columns and the non-linear OPE
> are table-local behaviors and carry over unchanged.

---

## 12. Query layer (`mongreldb-query`)

- `MongrelSession::open(Arc<Database>)` replaces `new(Db)` + N×`register_db`. On
  open it reads the catalog and **auto-registers every live table** as a DataFusion
  `MongrelProvider` (the provider gains a `table_id` and reads through the shared
  `Database`/snapshot).
- `run(sql)` takes one `db.snapshot()` and threads it through the whole plan, so
  joins are point-in-time consistent (§6). Result-cache key = `(sql, snapshot.epoch)`.
- DDL via SQL (`CREATE TABLE` / `DROP TABLE`) maps to catalog operations; `register`
  becomes internal.
- `combined_epoch` (`lib.rs:962`) and the "mutate the primary table last / call
  `clear_cache`" footgun documented on `register_db` are **removed** — the shared
  clock makes them obsolete.
- The existing FK-join fast paths and pushdown continue to work per `Table`; they
  now resolve against a consistent multi-table snapshot.

---

## 13. `mongreldb-node` (NAPI) — multi-table addon

### 13.1 Why a redesign
Today the NAPI `Database` (`crates/mongreldb-node/src/lib.rs:284`) is a **single
table** — `new(path, schema, table_id)` wraps one core `Db`; there is no catalog,
no cross-table SQL in-process, and no cross-table transaction. Level 2 makes the
JS `Database` a true multi-table handle.

### 13.2 JS API surface (TypeScript shape generated at build)
```ts
class Database {
  static create(path: string, opts?: { passphrase?: string; keyFile?: string }): Database;
  static open(path: string,   opts?: { passphrase?: string; keyFile?: string }): Database;

  // DDL
  createTable(name: string, schema: SchemaSpec): Table;
  dropTable(name: string): void;
  table(name: string): Table;           // cheap handle (throws if absent)
  tableNames(): string[];

  // Cross-table SQL → Arrow IPC Buffer (existing zero-copy bridge)
  sql(sql: string): Buffer;
  sqlAsync(sql: string): Promise<Buffer>;

  // Cross-table atomic transaction (concurrent; commit may throw ConflictError)
  begin(): Transaction;
  transaction<T>(fn: (txn: Transaction) => T, opts?: { retries?: number }): T; // commit on return,
                                                  // rollback on throw, auto-retry on ConflictError
  snapshotEpoch(): bigint;              // current visible DB epoch
  close(): void;
}

class Table {                            // bound to a Database; not constructible directly
  put(cells: Cell[]): bigint;            // rowId
  putBatch(rows: Cell[][]): bigint[];
  bulkLoadTyped(cols: TypedColumn[]): bigint;
  get(rowId: bigint): Row | null;
  query(conds: ConditionSpec[]): Row[];          // hybrid ann ∩ fm ∩ bitmap …
  queryArrow(conds: ConditionSpec[]): Buffer;
  count(): bigint;
  // async variants: putAsync / getAsync / queryAsync / queryArrowAsync …
}

class Transaction {                      // cross-table; explicit lifecycle
  table(name: string): TxnTable;         // table-scoped mutators within the txn
  commit(): bigint;                      // returns the committed epoch
  rollback(): void;
}
class TxnTable { put(cells: Cell[]): void; putBatch(rows: Cell[][]): void; delete(rowId: bigint): void; }

class WriteBuffer { /* now constructed from a Table, not a Database */ }
class RemoteDatabase { /* §14: table-qualified routes */ }
```

### 13.3 Rust/NAPI implementation notes
- `#[napi] struct Database { inner: Arc<core::Database> }` — `Arc` because `Table`
  and `Transaction` handles share it; the core `Database` is `Send + Sync` (§9), so
  it satisfies NAPI's threadsafe requirements.
- `#[napi] struct Table { db: Arc<core::Database>, table_id: u64 }` — resolves the
  `Arc<core::Table>` on each call (or caches it). Cheap to mint many handles.
- **Transactions across FFI:** explicit `begin/commit/rollback` (a callback-style
  `transaction(fn)` wrapper in `index.js` commits on return / rolls back on a thrown
  JS error, since unwinding a JS throw through NAPI into a Rust drop is fragile).
  A Rust `Transaction` does **not** hold any global lock while open — it only
  accumulates a private write set and (per §9) contends solely in the microsecond
  commit sequencer — so **many JS transactions can be in flight concurrently**.
  `commit()` returns the `commit_epoch`, or throws a typed **`ConflictError`** that
  callers (or the `transaction(fn)` helper, optionally with a retry count) retry.
  A forgotten txn rolls back on `Drop`.
- **Async:** every blocking method keeps its `*Async` Promise variant offloading to
  the NAPI tokio blocking pool (as today, `lib.rs:404-475`). Async transactions are
  now safe (an open txn blocks no one); `commitAsync()` offloads the fsync wait to
  the pool so the JS event loop is never stalled on durability.
- **BigInt** for rowId / count / epoch (lossless u64), unchanged.
- **Arrow:** `sql()` / `queryArrow()` return Arrow-IPC `Buffer`s via the existing
  bridge; cross-table `sql()` runs through the `MongrelSession` over the shared
  `Database`.
- **Build:** unchanged (`napi build --release`); regenerate `index.{js,d.ts}`.
  `smoke.mjs` is extended to: create two tables, insert, run a JOIN, run a
  cross-table transaction, and assert atomic visibility.

### 13.4 Backward shape
The old single-table `new(path, schema, table_id)` is removed (clean break). A JS
convenience `Database.openSingle(path, name)` may wrap a one-table database for
callers that want the old feel; optional.

---

## 14. Daemon + client

- `mongreldb-server` holds **one** `Arc<Database>` + one `MongrelSession`. Routes
  become table-qualified and gain transaction + DDL endpoints:
  - `POST /tables` (create), `DELETE /tables/:name` (drop), `GET /tables` (list)
  - `POST /tables/:name/put`, `/get`, `/query`, `/count`
  - `POST /sql` (cross-table), `POST /txn` (begin→ops→commit in one request body)
- `mongreldb-client` + NAPI `RemoteDatabase` mirror the routes; `RemoteDatabase`
  exposes `.table(name)` and `.sql(...)` and a `/txn` batch.
- Multi-process cache sharing now works at database granularity (one warm
  `Database`), which is strictly better than today's per-table daemon.

---

## 15. Recovery & crash safety (end-to-end)

`Database::open(root)`:
1. **Bump + fsync `open_generation` and `next_segment_no`** in the catalog (review
   fixes #11, #23) — the first durable write, so new txn_ids/segments can't alias
   anything in un-reclaimed segments.
2. Read + decrypt + **authenticate** `CATALOG` (review fix #20). Obtain `db_epoch`,
   the table set + schemas.
3. For each live table: open + authenticate its manifest → `run_refs`, `live_count`,
   `flushed_epoch`; load the encrypted index checkpoint or mark for rebuild; build
   lock-free read structures.
4. Init `assigned` and `visible` to `max(db_epoch, max table.current_epoch)`.
5. **Replay the shared WAL — bounded, two-pass** (§8.4): pass 1 indexes per-`txn_id`
   outcomes (committed `epoch`+`added_runs` / abort / in-flight) holding only small
   metadata; pass 2 applies each committed txn's records to its table **iff
   `commit_epoch > table.flushed_epoch`** at `commit_epoch`, links `added_runs`, and
   replays `Ddl` ops to reconstruct the in-memory catalog. Discard aborted /
   in-flight-at-EOF / torn-trailing txns and sweep their `_txn/<txn_id>/` runs.
   Apply in global epoch order; advance `assigned`/`visible` to the max committed
   epoch.
6. **Do NOT truncate the replayed segments (review fix #6).** Open a **fresh** active
   segment (next `next_segment_no`) for appends and **retain** the replayed segments
   until a flush/checkpoint makes their data durable in runs and segment GC reclaims
   them (§7.4). Truncating-then-rewriting the active segment — as the current
   single-table code does (`engine.rs:831-842`) — risks losing the only durable copy
   if a second crash precedes the next checkpoint; the new scheme never does that.

Crash-safety invariants:
- **All metadata files** (catalog, manifests, checkpoints, schema mirrors) are written
  tmp → fsync(tmp) → rename → **fsync(parent dir)** (review fix #19), and are
  encrypted+authenticated for an encrypted DB (review fix #20).
- **A transaction is durable iff its `TxnCommit` is within `durable_seq`** (one group
  fsync per batch, §9.3c). Any `txn_id` without a durable `TxnCommit` is rolled back
  wholesale — all interleaved records and any spilled per-txn runs.
- **DDL ordering:** create-table appends `Ddl::CreateTable`+`TxnCommit` (durability
  is the WAL); the table subdir + the in-memory catalog entry are materialized at
  publish, and the `CATALOG` checkpoint is rewritten lazily. A crash before the
  checkpoint is covered by WAL replay (step 5). Drop-table tombstones the entry
  (`Dropped{at_epoch}`) and defers physical subdir deletion to the retention reaper
  (§6.4).
- **WAL segment GC** never deletes a segment any live table still needs (§7.4).

---

## 16. GC / check / doctor (extended)

- **GC:**
  - **WAL segments** reclaimed by `min_retained_seq` (in-flight + committed-not-flushed,
    §7.4) **and** the snapshot-retention bound (§6.4) — never delete a segment a
    crash-recovery or live reader could still need.
  - **Pending-run sweep:** `tables/<id>/_txn/<txn_id>/` directories whose `txn_id` is
    not live (aborted, or absent after recovery) are deleted (review fix #14); a live
    txn's quarantine is never touched.
  - **Retired runs / dropped tables:** physical deletion of compaction-superseded
    runs and `Dropped` table subdirs is performed by the **retention reaper** only
    once `min_active_snapshot` passes their retire epoch (§6.4).
- **check:** authenticate the catalog + every manifest (keyed, encrypted DB) or
  verify SHA-256 (plaintext DB); verify every run footer + run-metadata MAC; verify
  `flushed_epoch` ≤ WAL head and that no live segment is below `min_retained_seq`;
  verify no orphan run lacks a `RunRef` and no `RunRef` lacks a run.
- **doctor:** drop corrupt runs (per table); if a table's subdir is unreadable but
  its catalog entry is intact, quarantine the table (`Dropped`) rather than failing
  the whole `open`.

---

## 17. Implementation phasing (isolated, reviewable commits)

Each phase compiles, passes the suite, and is independently revertible. The order
front-loads the rename and the consistency win, then the WAL/txn redesign, then
surfaces.

- **P0 — Rename `Db` → `Table`** across all crates. Pure mechanical; no behavior
  change. Lands first to keep later diffs legible.
- **P1 — `Database` container + catalog + shared clock + retention.** Introduce
  `Database` owning the catalog (checkpoint), the single dual-counter epoch
  authority, the shared caches, and the **`SnapshotRegistry`** (§6.4). Metadata
  files become encrypted+authenticated with directory-fsync (review fixes #19, #20);
  `open_generation`/`next_segment_no` are persisted (#11, #23). Tables still have
  their own WAL here, but **all share the clock + retention**, giving **consistent
  read snapshots** and removing `combined_epoch`. Coherent intermediate (reads
  consistent) but **not** atomic cross-table writes — a stepping stone, not the end.
- **P2 — Shared WAL + atomic transactions + recovery.** Collapse N WALs into one
  shared log; add the generation-scoped `txn_id` envelope (`txn_id=0` system),
  `Op::TxnCommit{added_runs}`/`TxnAbort`/`Ddl`, `Op::Flush{flushed_epoch}`, per-table
  `flushed_epoch` gating, **deterministic per-segment nonce** (#23), the txn API,
  **no-truncate-on-reopen** (#6), the bounded **two-pass** recovery + **torn-vs-
  corrupt** `WalReader` (#10, #22), and **DDL-as-WAL** (#16). Commits still apply
  one-at-a-time (correct, not yet parallel), but the on-disk format + recovery are
  **final**, so P3 adds no format change. **Heaviest, most correctness-critical.**
- **P3 — Concurrent writers + group commit + large-txn spill.** Add the bounded
  validate-first commit sequencer (#1, #17), the conflict index with broadened keys
  (row/unique-PK/table-scope) + `ActiveTxns` registration ordering (#12, #13), the
  group-commit `durable_seq` contract + `poisoned` flag (#15), generation-sealed
  flush + flush lock discipline (#9, #18), the `assigned`/`visible` watermark advance,
  fine-grained `publish_lock` + `ArcSwap` reads, and the §8.5 spill (quarantined
  `_txn/` runs (#14), uniform-epoch runs (#7), `AddedRun` linking (#8), conflict-key
  spill). Delivers genuine concurrency, SI, and unbounded transactions on P2's
  format — no on-disk change.
- **P4 — Query layer** over `Database` (auto-register from catalog, snapshot-
  threaded joins, DDL-via-SQL).
- **P5 — NAPI redesign** (§13): `Database`/`Table`/`Transaction` + `ConflictError`
  + retry, regenerated TS, extended smoke test.
- **P6 — Daemon + client** (§14): table-qualified routes, `/txn`, `/tables`.
- **P7 — GC/check/doctor** extensions (§16) and the optional offline `import-table`
  migration tool.

> P2 and P3 are split only so the durability/recovery core lands and is tested
> before the concurrency machinery rides on top — **both are in scope**; P2 is not
> a shippable end state.

---

## 18. Testing strategy

The redesign touches durability and MVCC, so testing is a first-class deliverable.

- **Cross-table atomicity (crash matrix):** kill the process (a) mid-WAL-group
  before `TxnCommit`, (b) after `TxnCommit` before publish, (c) between the two
  catalog/manifest writes of a DDL, (d) mid-segment-rotation. After each, reopen
  and assert all-or-nothing across every table in the txn. (Extend the existing
  `crash.rs`/`crash_process.rs` harnesses.)
- **Cross-table consistent snapshot:** a reader pinned before a multi-table txn
  sees none of it; a reader after sees all of it; never a mix. Property test with
  interleaved writers/readers.
- **Recovery routing (interleaved):** with concurrent committers, records of txns
  A and B interleave in the log; recovery applies each only on its own `TxnCommit`;
  an uncommitted/aborted interleaved txn (and its spilled runs) is discarded; records
  for table X never land in table Y; `flushed_epoch` gating skips already-flushed commits.
- **Concurrent writers (stress):** many threads commit disjoint-key txns in
  parallel and **all succeed** (no spurious aborts); throughput scales with the
  group-commit batch (fsync count ≪ commit count under load).
- **Write–write conflict (SI):** two concurrent txns writing the same
  `(table,row_id)` → exactly one commits, the other gets `Conflict`; retry succeeds;
  no lost update. Property test with randomized interleavings.
- **Unique/PK conflict (review fix #13):** two concurrent inserts of the *same PK*
  (distinct row_ids) → exactly one commits; the duplicate is rejected — uniqueness
  holds under concurrency. Also: concurrent `DROP`/`TRUNCATE` vs. DML on the same
  table conflicts (table-scope key).
- **Validate-before-assign / watermark liveness (review fix #1):** force aborts
  interleaved with commits; assert `visible` never stalls and equals the max
  published epoch; aborts consume no epoch.
- **Snapshot retention (review fix #3):** a reader pins epoch E, then `DROP TABLE` /
  compaction retire data at >E; the reader still completes its scan correctly; the
  physical delete happens only after the reader's guard drops. Test for tables,
  runs, and WAL segments.
- **Reopen durability (review fix #6):** commit, crash, reopen (replay, no truncate),
  crash again before any checkpoint, reopen → the committed data is still present.
- **`flushed_epoch` gating (review fix #4):** stream a record early (low seq), flush
  the table past that seq, commit the txn afterward, crash, reopen → the row is
  present (gated by epoch, not seq).
- **Group-commit durable-seq (review fix #15):** a committer never publishes before
  its `TxnCommit` seq ≤ `durable_seq`; an injected fsync error poisons the DB and
  fails all in-flight commits (none become visible).
- **txn_id generation (review fix #11):** segments from a prior open contain
  `txn_id`s whose generation differs from the new open's; recovery never merges them.
- **Bounded recovery (review fix #10):** recover a database with a multi-GB committed
  txn (spilled to runs) and many concurrent in-flight txns at crash; assert recovery
  peak memory is bounded (runs are linked, not buffered).
- **Pending-run quarantine GC (review fix #14):** a large txn's `_txn/<id>/` run is
  never deleted while the txn is live; after abort/crash it is swept.
- **Torn vs. corrupt WAL (review fix #22):** a torn trailing frame truncates cleanly;
  interior corruption errors.
- **Deadlock/liveness:** lock order `seq.wal` → `publish_lock` only (incl. flush,
  review fix #18); conflict-retry makes progress (no livelock).
- **Encryption (multi-table):** one passphrase opens all tables; metadata files
  (catalog + manifests + checkpoints) are encrypted AND **tamper-evident** — edit a
  manifest `run_ref`/`flushed_epoch` and reopen ⇒ authentication failure (review fix
  #20); deterministic WAL nonces never repeat across segments/reopens (review fix #23).
- **fd budget:** a database with 50 tables holds exactly one persistent WAL fd at
  rest; assert via `/proc/self/fd` (Linux).
- **NAPI:** `smoke.mjs` — create 2+ tables, insert, JOIN via `sql()`, run a
  cross-table `transaction()` and assert atomic visibility; `*Async` variants;
  BigInt round-trips; `RemoteDatabase` `/txn`.
- **Perf gates:** single-row implicit-txn write stays ~6–7 µs at low concurrency
  and **improves under load** (group-commit amortization); full scan / pushdown
  throughput unchanged; shared page cache hit rate ≥ the per-table baseline at equal
  total budget; conflict-abort rate ≈ 0 for disjoint-key workloads.

---

## 19. Risks & design considerations

These are tuning/correctness considerations, not deferrals — every capability is
specified above.

1. **Commit-sequencer contention.** All commits pass through one microsecond
   critical section (validate + buffered append). It performs no I/O (§9.5), so it
   is not the bottleneck; the group-commit fsync is. **Tune:** batch window /
   max-batch for the group-commit coordinator. Mitigation if ever hot: shard the
   conflict index (already sharded) and keep validation O(write-set).
2. **Conflict-abort rate** under same-key contention. First-committer-wins means hot
   single rows can thrash. **Mitigation:** callers retry (built into
   `transaction(fn)`); document hot-key patterns; aborts cost no fsync (rejected in
   the sequencer before durability).
3. **WAL as a shared hotspot.** All tables funnel through one log. Append is a
   buffered memcpy under the sequencer; fsync is batched. **Tune:** segment size,
   sync byte threshold, group-commit window.
4. **Spill correctness** (per-txn runs, conflict-key spill, streamed records) is the
   subtle large-txn invariant — covered by §18 large-transaction + crash tests.
5. **Per-table `flushed_epoch` + interleaved two-pass recovery** is the subtle
   durability invariant — covered by §18 `flushed_epoch`-gating and recovery tests.
6. **Visibility watermark in-order advance** must not stall if a low-epoch txn is
   slow to publish (it briefly holds back `visible`). **Mitigation:** publish is
   in-memory and fast; large data is spilled to runs and merely *linked* at publish
   (§8.5), so publish never does heavy I/O while others wait on the watermark.
7. **Catalog vs. schema mirror.** The **WAL is the DDL authority**; both `CATALOG`
   and `schema.json` are rebuildable checkpoints (encrypted+authenticated when the
   DB is), refreshed lazily after publish. Recovery reconstructs them from the WAL.
8. **NAPI transaction ergonomics** (throw → rollback, conflict → retry). JS
   `transaction(fn, {retries})` wrapper + `Drop`-rollback; validate no
   double-commit / use-after-commit / forgotten-rollback.

---

## 20. Summary of concrete changes

| Area | Change |
|---|---|
| `engine.rs` | `Db` → `Table` (P0). Split shared services out to new `Database`. Tables become lock-free-read (`ArcSwap` runs/schema) + `publish_lock`. |
| `database.rs` (new) | `Database`, catalog, the dual-counter epoch authority, shared `SharedWal`, shared caches, KEK, `begin/commit/rollback`, `transaction()`, `create_table/drop_table/table/snapshot`. |
| `txn.rs` (new) | `Transaction` (private write set + staging/spill), commit sequencer, conflict index + first-committer-wins validation, group-commit fsync coordinator, `assigned`/`visible` watermark. |
| `catalog.rs` (new) | `CATALOG` checkpoint (rebuildable from WAL `Ddl` ops, not the authority); `open_generation` + `next_segment_no`; encrypted+authenticated for an encrypted DB; **directory-fsync** atomic write. |
| `retention.rs` (new) | `SnapshotRegistry` (`min_active_snapshot`) + the deferred-delete reaper for runs / dropped tables / WAL segments (§6.4). |
| `wal.rs` | `Record` gains generation-scoped `txn_id` (CRC covers it; `txn_id=0`=system); `Op::TxnCommit{epoch, added_runs: Vec<AddedRun>}` + `Op::TxnAbort` + `Op::Ddl`; `Op::Flush{table_id, flushed_epoch}`; drop per-record epoch; **deterministic per-segment nonce** from the persisted segment number; `group_sync`→`durable_seq`; **torn-tail vs interior-corruption** distinction in `WalReader`. |
| `manifest.rs` | Per-table manifest gains `flushed_epoch` (commit-epoch, not seq); atomic `AddedRun` linking; **encrypted+authenticated** (not bare SHA-256) for encrypted DBs; **directory fsync**. |
| `epoch.rs` | Dual-counter authority: `assigned` + in-order `visible` watermark (validate-before-assign, no dead epochs); owned by `Database`. |
| `txn.rs` (new) | also: commit sequencer (bounded, validate-first), conflict index with broadened keys (row/unique-PK/table-scope) + `ActiveTxns` registration ordering + pruning, group-commit `durable_seq` contract + `poisoned` flag, per-txn spill (quarantined `_txn/` runs, conflict-key spill). |
| `global_idx.rs` | Checkpoint encrypted+authenticated under `derive_meta_key()` with per-`table_id` domain separation; directory fsync. |
| `encryption.rs` | No primitive change; KEK is DB-level; add `derive_meta_key()`; per-file/per-table domain separation in info labels. |
| `mongreldb-query` | `MongrelSession::open(Arc<Database>)`, auto-register from catalog, snapshot-threaded joins, drop `combined_epoch`, DDL-via-SQL. |
| `mongreldb-node` | New `Database`/`Table`/`Transaction` NAPI surface (§13); regenerated TS; extended smoke. |
| `mongreldb-server`/`client` | One `Database`; table-qualified + `/txn` + `/tables` routes. |
| tests | New crash/MVCC/concurrency/fd/NAPI suites (§18). |
