# Multi-Table (Level 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn MongrelDB from "one `Db` = one table = one WAL" into a native multi-table `Database` with a single shared WAL, one epoch clock, atomic + concurrent cross-table transactions (snapshot isolation, first-committer-wins), unbounded (spilling) transactions, consistent cross-table snapshots, shared caches, one key hierarchy, and a multi-table NAPI addon.

**Architecture:** A new `Database` owns shared services (catalog, dual-counter epoch authority, shared WAL, shared caches, KEK, snapshot-retention registry, commit sequencer + conflict index + group-commit). The per-table engine (`Db`, renamed `Table`) keeps only table-local state and borrows shared services. Concurrency = optimistic MVCC: prepare in parallel → bounded commit sequencer (validate-first, assign epoch, append commit marker) → group fsync → parallel publish → in-order `visible` watermark.

**Tech Stack:** Rust 2021 (MSRV 1.80), `parking_lot`, `crossbeam-skiplist`, `arc-swap` (new dep), `memmap2`, `bincode`, `crc`, `sha2`, `aes-gcm`/`argon2`/`hkdf`/`hmac`/`zeroize` (encryption feature), `datafusion` 54 / `arrow` 58 (query), `napi`/`napi-derive` 2 (node).

**Companion spec:** `MULTITABLE.md` (repo root). This plan references it by section (e.g. "spec §9.3"). Where the spec gives a multi-page algorithm, the task body gives the exact files, interfaces, the failing test, and a code sketch, and points to the spec section for the full algorithm — do **not** re-derive; read the spec section. `(review fix #N)` tags trace correctness invariants from the spec's peer-review pass; preserve them.

## Global Constraints

- **MSRV 1.80**, edition 2021. `cargo fmt --check` and `cargo clippy --workspace --all-targets --all-features -- -D warnings` must stay green every task.
- **No AI/agent attribution** anywhere (commits, code, comments, test data). Use neutral test names (`qa-*`, `pipeline-test-*`).
- **Clean break:** no on-disk migration from the old single-table layout (no users yet). Do not add back-compat shims for the old format.
- **Encryption parity:** never regress per-page AEAD, the required run-metadata MAC, or encrypted WAL/result-cache/index-checkpoint. New metadata (catalog, manifests) must be encrypted **and authenticated** for encrypted DBs (spec §11, review fix #20).
- **Lock order is global and singular:** `seq.wal` → table `publish_lock`. Never hold a table lock while entering the sequencer (spec §9.5). Every concurrency task must uphold this.
- **No unbounded work in the commit sequencer** (no fsync, no disk read, no GB copy) — spec §9.3b.
- **`mongreldb-node` is not a workspace member** (built via `napi build`); `cargo test --workspace --all-features` must not require the Node toolchain.
- **TDD:** every task writes the failing test first, watches it fail, implements minimally, watches it pass, commits. Commit messages end with no attribution trailer.

## How tasks are sized

Each task is the smallest unit with its own test cycle and a reviewer gate. Phases P0–P7 mirror spec §17 and each ends with working, testable software. **Recommendation (per writing-plans scope check):** treat each phase as a sub-plan checkpoint — review and merge a phase before starting the next. P2 and P3 are the correctness-critical core; budget the most review there.

---

## File Structure

**New files (`crates/mongreldb-core/src/`):**
- `database.rs` — `Database`: root dir, catalog handle, epoch authority, shared WAL, shared caches, KEK, snapshot registry, `create_table`/`drop_table`/`table`/`snapshot`/`begin`/`transaction`.
- `catalog.rs` — `Catalog` checkpoint format + encrypted/authenticated atomic read/write (dir-fsync).
- `txn.rs` — `Transaction`, write-set/staging/spill, `CommitSequencer`, `ConflictIndex`, `ActiveTxns`, `GroupCommit` (`durable_seq`), `assigned`/`visible` watermark advancer.
- `retention.rs` — `SnapshotRegistry` (`min_active_snapshot`) + deferred-delete reaper.

**Modified (`crates/mongreldb-core/src/`):** `engine.rs` (`Db`→`Table`, lock-free reads), `wal.rs` (txn_id envelope, new ops, deterministic nonce, group-sync, torn-vs-corrupt), `manifest.rs` (`flushed_epoch`, enc+auth, dir-fsync), `epoch.rs` (dual counter), `global_idx.rs` (per-table enc+auth checkpoint), `encryption.rs` (`derive_meta_key`), `lib.rs` (exports), `sorted_run.rs` (uniform-epoch run + `RunRef.commit_epoch`).

**Modified (other crates):** `crates/mongreldb-query/src/lib.rs` + `scan.rs`, `crates/mongreldb-node/src/lib.rs` (+ `index.js` wrapper, `smoke.mjs`), `crates/mongreldb-server/src/`, `crates/mongreldb-client/src/`.

**Test files:** per-task `crates/mongreldb-core/tests/*.rs` (new: `database_basic.rs`, `txn_atomic.rs`, `txn_concurrent.rs`, `txn_large.rs`, `recovery_multitable.rs`, `snapshot_retention.rs`, `wal_shared.rs`, `catalog.rs`), plus crate unit tests, plus `crates/mongreldb-node/smoke.mjs`.

---

## Phase P0 — Rename `Db` → `Table`

Pure mechanical rename so later diffs are legible. No behavior change. One task.

### Task P0.1: Rename the `Db` type to `Table` workspace-wide

**Files:**
- Modify: `crates/mongreldb-core/src/engine.rs` (the `pub struct Db` and all `impl Db`), `crates/mongreldb-core/src/lib.rs` (re-export), every `use mongreldb_core::Db` / `Db::` across `mongreldb-core`, `mongreldb-query`, `mongreldb-node`, `mongreldb-server`, `mongreldb-client`, and `tests/`, `benches/`, `examples/`.
- Test: existing suite is the test (behavior-preserving).

**Interfaces:**
- Produces: `pub struct Table` (was `Db`) with identical methods. A temporary `pub type Db = Table;` alias is **not** added (clean break) — update all call sites.

- [ ] **Step 1: Establish the green baseline**

Run: `cargo test --workspace --all-features 2>&1 | tail -5`
Expected: all pass (record the count, e.g. "203 passed").

- [ ] **Step 2: Rename the definition and all references**

Rename `struct Db` → `struct Table`, `impl Db` → `impl Table`, doc comment `/// An open MongrelDB table.` stays. Then mechanically update references:

```bash
cd /work/repos/visorcraft/mongreldb
grep -rl '\bDb\b' crates --include='*.rs' | xargs sed -i 's/\bDb::/Table::/g; s/: Db\b/: Table/g; s/<Db>/<Table>/g; s/\bDb {/Table {/g; s/-> Db\b/-> Table/g; s/struct Db\b/struct Table/g; s/impl Db\b/impl Table/g'
```
Then hand-audit remaining `\bDb\b` (e.g. in strings, `Arc<Mutex<Db>>`, NAPI `struct Database` is unrelated — do not touch it). The NAPI `Database` (`crates/mongreldb-node/src/lib.rs:284`) is a different type; leave it for P5.

- [ ] **Step 3: Update the lib.rs export**

In `crates/mongreldb-core/src/lib.rs`, change `pub use engine::Db;` (or the `Db` in a `pub use engine::{...}` list) to `Table`.

- [ ] **Step 4: Compile + test**

Run: `cargo test --workspace --all-features 2>&1 | tail -5`
Expected: same pass count as Step 1, zero failures.
Run: `cargo clippy --workspace --all-targets --all-features -- -D warnings`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "refactor: rename Db -> Table ahead of multi-table Database"
```

---

## Phase P1 — `Database` container + catalog + shared clock + retention

Introduce `Database` owning the catalog, the dual-counter epoch authority, shared caches, and the snapshot registry. Tables still each own a WAL here (collapsed in P2); the win is **consistent read snapshots** across tables and the removal of `combined_epoch`. Spec §5, §6, §10.

### Task P1.1: Dual-counter epoch authority

**Files:**
- Modify: `crates/mongreldb-core/src/epoch.rs`
- Test: `crates/mongreldb-core/src/epoch.rs` (unit tests in-file)

**Interfaces:**
- Produces:
  ```rust
  pub struct EpochAuthority { assigned: AtomicU64, visible: AtomicU64 }
  impl EpochAuthority {
      pub fn new(start: u64) -> Self;
      pub fn visible(&self) -> Epoch;            // reader watermark
      pub fn bump_assigned(&self) -> Epoch;      // commit-order ticket
      pub fn publish_visible(&self, e: Epoch);   // in-order advance to e (monotonic)
      pub fn set_recovered(&self, e: Epoch);     // both counters = e on open
  }
  ```
  Existing `Epoch`, `Snapshot` unchanged.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn epoch_authority_assigned_and_visible_advance_in_order() {
    let a = EpochAuthority::new(0);
    assert_eq!(a.visible(), Epoch(0));
    let e1 = a.bump_assigned();
    let e2 = a.bump_assigned();
    assert_eq!((e1, e2), (Epoch(1), Epoch(2)));
    // visible only advances when told, monotonically, never backward
    a.publish_visible(Epoch(2));
    assert_eq!(a.visible(), Epoch(2));
    a.publish_visible(Epoch(1)); // stale; must not regress
    assert_eq!(a.visible(), Epoch(2));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p mongreldb-core --lib epoch_authority_assigned_and_visible -- --exact 2>&1 | tail -5`
Expected: FAIL (`EpochAuthority` not found).

- [ ] **Step 3: Implement `EpochAuthority`**

Add the struct above. `bump_assigned` = `fetch_add(1)+1`; `publish_visible(e)` = CAS loop that only raises (`while cur < e { compare_exchange }`); `visible` = load Acquire. Keep `EpochClock` for now (P2 removes per-table clocks).

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p mongreldb-core --lib epoch_authority -- 2>&1 | tail -5`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/mongreldb-core/src/epoch.rs && git commit -m "feat(epoch): dual-counter assigned/visible authority"
```

### Task P1.2: Snapshot-retention registry

**Files:**
- Create: `crates/mongreldb-core/src/retention.rs`
- Modify: `crates/mongreldb-core/src/lib.rs` (`mod retention;`)
- Test: in `retention.rs`

**Interfaces:**
- Produces:
  ```rust
  pub struct SnapshotRegistry { /* parking_lot::Mutex<BTreeMap<u64,u64>> counts + fallback */ }
  pub struct SnapshotGuard<'r> { /* deregisters on Drop */ }
  impl SnapshotRegistry {
      pub fn new() -> Self;
      pub fn register(&self, epoch: Epoch) -> SnapshotGuard<'_>;
      pub fn min_active(&self, visible: Epoch) -> Epoch; // lowest live epoch, else `visible`
  }
  ```

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn retention_tracks_min_active_snapshot() {
    let r = SnapshotRegistry::new();
    assert_eq!(r.min_active(Epoch(10)), Epoch(10)); // none active -> visible
    let g1 = r.register(Epoch(5));
    let g2 = r.register(Epoch(8));
    assert_eq!(r.min_active(Epoch(10)), Epoch(5));
    drop(g1);
    assert_eq!(r.min_active(Epoch(10)), Epoch(8));
    drop(g2);
    assert_eq!(r.min_active(Epoch(10)), Epoch(10));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p mongreldb-core --lib retention_tracks_min_active -- 2>&1 | tail -5`
Expected: FAIL (module/type missing).

- [ ] **Step 3: Implement** a refcounted multiset (`BTreeMap<u64 /*epoch*/, u64 /*count*/>`); `register` increments + returns a guard holding `&self`+epoch; `Drop` decrements and removes at zero; `min_active` returns the first key or `visible`.

- [ ] **Step 4: Run to verify it passes** — Run: `cargo test -p mongreldb-core --lib retention_tracks_min_active -- 2>&1 | tail -5` → PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/mongreldb-core/src/retention.rs crates/mongreldb-core/src/lib.rs && git commit -m "feat(retention): global snapshot registry for deferred deletes"
```

### Task P1.3: Catalog format (encrypted + authenticated, dir-fsync)

**Files:**
- Create: `crates/mongreldb-core/src/catalog.rs`
- Modify: `crates/mongreldb-core/src/lib.rs`, `crates/mongreldb-core/src/encryption.rs` (add `derive_meta_key`)
- Test: `crates/mongreldb-core/tests/catalog.rs`

**Interfaces:**
- Consumes: `encryption::{encrypt_blob, decrypt_blob}` (already exist), new `Kek::derive_meta_key()`.
- Produces (spec §5.1):
  ```rust
  pub struct Catalog { pub db_epoch: u64, pub next_table_id: u64, pub open_generation: u64,
                       pub next_segment_no: u64, pub tables: Vec<CatalogEntry> }
  pub struct CatalogEntry { pub table_id: u64, pub name: String, pub schema: Schema,
                            pub state: TableState, pub created_epoch: u64 }
  pub enum TableState { Live, Dropped { at_epoch: u64 } }
  pub fn write_atomic(dir: &Path, cat: &Catalog, meta_dek: Option<&[u8;32]>) -> Result<()>;
  pub fn read(dir: &Path, meta_dek: Option<&[u8;32]>) -> Result<Option<Catalog>>;
  ```

- [ ] **Step 1: Add `derive_meta_key` (sub-step of this task)**

In `encryption.rs`, mirror `derive_idx_key`: `pub fn derive_meta_key(&self) -> Zeroizing<[u8;32]> { self.derive_subkey(b"mongreldb/meta/v1") }`. Export it.

- [ ] **Step 2: Write the failing test**

```rust
// tests/catalog.rs
use mongreldb_core::catalog::{self, Catalog, CatalogEntry, TableState};
#[test]
fn catalog_roundtrips_plaintext_and_dir_fsync() {
    let dir = tempfile::tempdir().unwrap();
    let cat = Catalog { db_epoch: 7, next_table_id: 3, open_generation: 1, next_segment_no: 4,
        tables: vec![CatalogEntry { table_id: 1, name: "orders".into(),
            schema: sample_schema(), state: TableState::Live, created_epoch: 2 }] };
    catalog::write_atomic(dir.path(), &cat, None).unwrap();
    let got = catalog::read(dir.path(), None).unwrap().unwrap();
    assert_eq!(got.db_epoch, 7);
    assert_eq!(got.tables[0].name, "orders");
}
```
(Add `sample_schema()` helper; reuse the schema shape from `tests/key_hierarchy.rs`.)

- [ ] **Step 3: Run to verify it fails** — `cargo test -p mongreldb-core --test catalog 2>&1 | tail -5` → FAIL (module missing).

- [ ] **Step 4: Implement** `catalog.rs`: bincode body + `MONGRCAT` magic + SHA-256; if `meta_dek` is `Some`, wrap the whole blob via `encrypt_blob`; write tmp → `fsync(tmp)` → rename → **`File::open(dir).sync_all()`** (directory fsync, review fix #19). `read` reverses it, returning `Ok(None)` on missing/auth-failure.

- [ ] **Step 5: Add the encrypted-roundtrip + tamper test**

```rust
#[cfg(feature = "encryption")]
#[test]
fn catalog_encrypted_is_authenticated() {
    let dir = tempfile::tempdir().unwrap();
    let dek = [9u8;32];
    let cat = /* as above */;
    catalog::write_atomic(dir.path(), &cat, Some(&dek)).unwrap();
    // tamper a byte of the file -> read must fail auth (None), not silently parse
    let p = dir.path().join("CATALOG");
    let mut b = std::fs::read(&p).unwrap(); let n=b.len(); b[n/2]^=0xFF; std::fs::write(&p,b).unwrap();
    assert!(catalog::read(dir.path(), Some(&dek)).unwrap().is_none());
}
```

- [ ] **Step 6: Run both tests** — `cargo test -p mongreldb-core --test catalog --features encryption 2>&1 | tail -5` → PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/mongreldb-core/src/catalog.rs crates/mongreldb-core/src/lib.rs crates/mongreldb-core/src/encryption.rs crates/mongreldb-core/tests/catalog.rs && git commit -m "feat(catalog): encrypted+authenticated DB catalog with dir-fsync"
```

### Task P1.4: `Database` skeleton over per-table `Table`s (shared clock + caches + retention)

**Files:**
- Create: `crates/mongreldb-core/src/database.rs`
- Modify: `crates/mongreldb-core/src/lib.rs`, `crates/mongreldb-core/src/engine.rs` (let `Table` accept an injected `Arc<EpochAuthority>`, shared caches, and `Arc<SnapshotRegistry>` instead of creating its own clock/caches)
- Test: `crates/mongreldb-core/tests/database_basic.rs`

**Interfaces:**
- Consumes: `Table` (P0), `EpochAuthority` (P1.1), `SnapshotRegistry` (P1.2), `catalog` (P1.3).
- Produces:
  ```rust
  pub struct Database { /* root, RwLock<Catalog>, Arc<EpochAuthority>, Arc<SnapshotRegistry>,
                          RwLock<HashMap<u64, Arc<Table>>>, shared caches, Option<Arc<Kek>> */ }
  impl Database {
      pub fn create(root: &Path) -> Result<Self>;
      pub fn open(root: &Path) -> Result<Self>;
      #[cfg(feature="encryption")] pub fn create_encrypted(root:&Path, pass:&str)->Result<Self>;
      #[cfg(feature="encryption")] pub fn open_encrypted(root:&Path, pass:&str)->Result<Self>;
      pub fn create_table(&self, name: &str, schema: Schema) -> Result<u64 /*table_id*/>;
      pub fn drop_table(&self, name: &str) -> Result<()>;
      pub fn table(&self, name: &str) -> Result<Arc<Table>>;
      pub fn table_names(&self) -> Vec<String>;
      pub fn snapshot(&self) -> (Snapshot, SnapshotGuard<'_>);
  }
  ```
  `Table` gains: `fn open_in(dir, schema, table_id, shared: SharedCtx) -> Result<Self>` where `SharedCtx { epoch: Arc<EpochAuthority>, page_cache, decoded_cache, snapshots, kek }`.

- [ ] **Step 1: Write the failing test**

```rust
// tests/database_basic.rs
#[test]
fn database_creates_tables_and_shares_one_clock() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let _o = db.create_table("orders", orders_schema()).unwrap();
    let _i = db.create_table("items", items_schema()).unwrap();
    assert_eq!(db.table_names().len(), 2);
    // a put on each table advances the SAME clock (epochs strictly increase across tables)
    let e_pre = db.snapshot().0.epoch;
    db.table("orders").unwrap().put(vec![(1, Value::Int64(1))]).unwrap();
    db.table("orders").unwrap().commit().unwrap();
    db.table("items").unwrap().put(vec![(1, Value::Int64(2))]).unwrap();
    let e_post = db.table("items").unwrap().commit().unwrap();
    assert!(e_post.0 > e_pre.0);
    // reopen sees both tables
    drop(db);
    let db = Database::open(dir.path()).unwrap();
    assert_eq!(db.table_names().len(), 2);
}
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p mongreldb-core --test database_basic 2>&1 | tail -5` → FAIL.

- [ ] **Step 3: Implement `Database`** per spec §5/§6/§10: `create` writes an empty `CATALOG` (+ `_meta/keys` for encrypted); `create_table` allocates `next_table_id`, makes `tables/<id>/`, writes the table's `schema.json` + empty manifest, appends a `CatalogEntry`, rewrites the catalog, and inserts an `Arc<Table>` built with the shared `SharedCtx`; `open` reads the catalog and opens each live table with the shared ctx; `snapshot` reads `epoch.visible()` and registers in `snapshots`. Refactor `Table` construction to take `SharedCtx` (remove its private `EpochClock`/cache creation; use the injected ones). **Per-table WAL stays for now.**

- [ ] **Step 4: Run to verify it passes** — `cargo test -p mongreldb-core --test database_basic 2>&1 | tail -5` → PASS. Also `cargo test --workspace --all-features` stays green.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(database): multi-table container with shared clock, caches, retention"
```

### Task P1.5: Shared page-cache key includes `table_id`

**Files:**
- Modify: `crates/mongreldb-core/src/sorted_run.rs` (`page_cache_key`), `engine.rs` (`open_reader` passes `table_id`)
- Test: `crates/mongreldb-core/src/sorted_run.rs` unit test

**Interfaces:**
- Produces: `pub(crate) fn page_cache_key(table_id: u64, run_id: u128, column_id: u16, page_seq: usize) -> [u8;32]` (gains `table_id`).

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn page_cache_key_distinguishes_tables() {
    let a = page_cache_key(1, 5, 2, 3);
    let b = page_cache_key(2, 5, 2, 3); // same run/col/page, different table
    assert_ne!(a, b);
}
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p mongreldb-core --lib page_cache_key_distinguishes -- 2>&1 | tail -5` → FAIL (arity).

- [ ] **Step 3: Implement** — add `table_id` to the hash input; thread `table_id` through `RunReader` (`open_with_cache`) and `Table::open_reader`.

- [ ] **Step 4: Run to verify it passes** — PASS; `cargo test --workspace --all-features` green.

- [ ] **Step 5: Commit** — `git commit -am "feat(cache): key shared page cache by table_id"`

---

## Phase P2 — Shared WAL + atomic transactions + recovery

The durability core. Collapse per-table WALs into one shared log; final on-disk format. Spec §7, §8.2 (single-applier subset), §8.4, §15. Concurrency (parallel commit) comes in P3 on top of this format.

### Task P2.1: WAL record envelope + new ops (format)

**Files:**
- Modify: `crates/mongreldb-core/src/wal.rs`
- Test: `crates/mongreldb-core/src/wal.rs` unit tests

**Interfaces:**
- Produces (spec §7.1):
  ```rust
  pub struct Record { pub seq: u64, pub txn_id: u64, pub op: Op }  // txn_id new; CRC covers seq‖txn_id‖payload
  pub enum Op {
      Put { table_id: u64, rows: Vec<u8> },
      Delete { table_id: u64, row_ids: Vec<RowId> },               // no per-record epoch (dropped)
      TruncateTable { table_id: u64 },
      Flush { table_id: u64, flushed_epoch: u64 },                 // SYSTEM record (txn_id==0)
      TxnCommit { epoch: u64, added_runs: Vec<AddedRun> },
      TxnAbort,
      Ddl(DdlOp),
  }
  pub struct AddedRun { pub table_id: u64, pub run_id: u128, pub row_count: u64, pub level: u8,
                        pub min_row_id: u64, pub max_row_id: u64, pub content_hash: [u8;32] }
  pub enum DdlOp { CreateTable{table_id:u64,name:String,schema:Schema}, DropTable{table_id:u64},
                   AlterTable{table_id:u64, add_column:(String,TypeId)} }
  pub const SYSTEM_TXN_ID: u64 = 0;
  ```

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn record_roundtrips_with_txn_id_and_commit_marker() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("seg-000000.wal");
    let mut w = Wal::create(&path, Epoch(0)).unwrap();
    w.append_txn(7, Op::Put{table_id:3, rows:vec![1,2,3]}).unwrap();
    w.append_txn(7, Op::TxnCommit{epoch:11, added_runs:vec![]}).unwrap();
    w.sync().unwrap();
    let recs = replay(&path).unwrap();
    assert_eq!(recs[0].txn_id, 7);
    assert!(matches!(recs[0].op, Op::Put{table_id:3,..}));
    assert!(matches!(recs[1].op, Op::TxnCommit{epoch:11,..}));
}
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p mongreldb-core --lib record_roundtrips_with_txn_id -- 2>&1 | tail -5` → FAIL.

- [ ] **Step 3: Implement** — add `txn_id` to `Record`; CRC over `seq‖txn_id‖payload`; new `Op` variants; `append_txn(txn_id, op)` and `append_system(op)` (txn_id=0). Keep frame layout otherwise. Update `WalReader::next_record` to read `txn_id`.

- [ ] **Step 4: Run to verify it passes** — PASS. Update any exhaustive `match Op` arms in `engine.rs` recovery.

- [ ] **Step 5: Commit** — `git commit -am "feat(wal): txn_id envelope + TxnCommit/TxnAbort/Ddl/AddedRun ops"`

### Task P2.2: Deterministic, collision-free WAL nonce (review fix #23)

**Files:** Modify `crates/mongreldb-core/src/wal.rs`. Test in-file (encryption feature).

**Interfaces:** `Wal::create_with_cipher(path, epoch, cipher, segment_no: u64)` gains `segment_no`; nonce = `segment_no(8B) ‖ frame_counter(4B)`.

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(feature="encryption")]
#[test]
fn wal_nonce_is_segment_deterministic() {
    // two segments with different segment_no must never share a frame nonce base
    assert_ne!(frame_nonce_for(5, 0), frame_nonce_for(6, 0));
    assert_ne!(frame_nonce_for(5, 0), frame_nonce_for(5, 1));
}
```
(Expose a `#[cfg(test)] fn frame_nonce_for(segment_no,frame)->[u8;12]` mirroring the writer.)

- [ ] **Step 2: Run to verify it fails** — FAIL (arity/behavior).

- [ ] **Step 3: Implement** — replace the random 8-byte seed with `segment_no.to_be_bytes()[..8] ‖ (frame as u32).to_le_bytes()`; refuse frame overflow (rotate). `segment_no` comes from the catalog's `next_segment_no` (P2.4 wires it).

- [ ] **Step 4: Run to verify it passes** — PASS.

- [ ] **Step 5: Commit** — `git commit -am "feat(wal): deterministic per-segment nonce from persisted segment number"`

### Task P2.3: Torn-tail vs interior corruption in `WalReader` (review fix #22)

**Files:** Modify `crates/mongreldb-core/src/wal.rs`. Test in-file.

**Interfaces:** `replay`/`WalReader` treat a CRC/short failure on the **last** frame as clean EOF (truncate); a CRC failure followed by a valid later frame → `Err(CorruptWal)`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn trailing_torn_is_eof_but_interior_corruption_errors() {
    // (a) good records then a half-written trailing frame -> replay returns the good prefix
    // (b) corrupt an interior frame's CRC, append a valid frame after -> replay errors
    // ... construct both files, assert (a) Ok(prefix), (b) Err(CorruptWal)
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL (interior case currently also treated as EOF, or both error).

- [ ] **Step 3: Implement** — read-ahead one frame: a CRC failure is a torn tail only if no well-formed frame follows; else error. (Two-pass or buffered lookahead per spec §8.4.)

- [ ] **Step 4: Run to verify it passes** — PASS.

- [ ] **Step 5: Commit** — `git commit -am "feat(wal): distinguish trailing torn frame from interior corruption"`

### Task P2.4: `SharedWal` + group-sync API over one fd

**Files:** Create the `SharedWal` wrapper in `wal.rs` (or `database.rs`); Modify `database.rs` to own it; Modify `manifest.rs` for `flushed_epoch`. Test: `crates/mongreldb-core/tests/wal_shared.rs`.

**Interfaces (spec §7.2):**
```rust
impl SharedWal {
    pub fn append(&mut self, txn_id: u64, table_id: u64, op: Op) -> Result<u64>;
    pub fn append_commit(&mut self, txn_id: u64, epoch: Epoch, added: &[AddedRun]) -> Result<u64>;
    pub fn append_abort(&mut self, txn_id: u64) -> Result<()>;
    pub fn append_system(&mut self, op: Op) -> Result<u64>;
    pub fn group_sync(&mut self) -> Result<u64 /*durable_seq*/>;
    pub fn rotate(&mut self, segment_no: u64) -> Result<()>;
}
```
`manifest.rs`: `Manifest.flushed_epoch: u64` (replaces nothing; new field); manifest read/write now `enc+auth` via `meta_dek` + dir-fsync.

- [ ] **Step 1: Write the failing test**

```rust
// tests/wal_shared.rs
#[test]
fn shared_wal_interleaves_two_tables_one_fd() {
    let dir = tempfile::tempdir().unwrap();
    let mut w = SharedWal::create(dir.path(), 0).unwrap();
    w.append(1, 10, Op::Put{table_id:10, rows:vec![1]}).unwrap();
    w.append(2, 20, Op::Put{table_id:20, rows:vec![2]}).unwrap();
    w.append_commit(1, Epoch(1), &[]).unwrap();
    w.append_commit(2, Epoch(2), &[]).unwrap();
    let d = w.group_sync().unwrap();
    assert!(d >= 4);
    let recs = SharedWal::replay(dir.path()).unwrap();
    assert_eq!(recs.iter().filter(|r| matches!(r.op, Op::Put{..})).count(), 2);
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL (type missing).

- [ ] **Step 3: Implement** `SharedWal` as a thin owner of the active `Wal` segment + segment list under `<root>/_wal/`; `group_sync` flushes+fsyncs and returns the highest durable seq; `rotate` opens the next `seg-NNNNNN.wal` with the given `segment_no`. Add `Manifest.flushed_epoch` + switch manifest writer to enc+auth + dir-fsync.

- [ ] **Step 4: Run to verify it passes** — PASS.

- [ ] **Step 5: Commit** — `git commit -am "feat(wal): SharedWal over one fd + flushed_epoch manifest (enc+auth)"`

### Task P2.5: Single-applier transactions on the shared WAL

**Files:** Create `crates/mongreldb-core/src/txn.rs` (`Transaction` + a serial `commit`); Modify `database.rs` (`begin`/`transaction`, route `Table` writes through the shared WAL). Test: `crates/mongreldb-core/tests/txn_atomic.rs`.

**Interfaces (spec §8.2, single-applier subset — parallelism added in P3):**
```rust
pub struct Transaction<'db> { /* db, txn_id, read_epoch, staging per table */ }
impl Database {
    pub fn begin(&self) -> Transaction<'_>;
    pub fn transaction<T>(&self, f: impl FnOnce(&mut Transaction)->Result<T>) -> Result<T>;
}
impl<'db> Transaction<'db> {
    pub fn put(&mut self, table: &str, cells: Vec<(u16,Value)>) -> Result<RowId>;
    pub fn delete(&mut self, table: &str, rid: RowId) -> Result<()>;
    pub fn commit(self) -> Result<Epoch>;
    pub fn rollback(self);
}
```
Implicit `Table::put`+`commit` route through a one-op `Transaction`.

- [ ] **Step 1: Write the failing cross-table atomicity test**

```rust
// tests/txn_atomic.rs
#[test]
fn cross_table_txn_is_all_or_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("a", one_int_schema()).unwrap();
    db.create_table("b", one_int_schema()).unwrap();
    db.transaction(|t| { t.put("a", vec![(1,Value::Int64(1))])?; t.put("b", vec![(1,Value::Int64(2))])?; Ok(()) }).unwrap();
    let snap = db.snapshot().0;
    assert_eq!(db.table("a").unwrap().count(), 1);
    assert_eq!(db.table("b").unwrap().count(), 1);
    // a rolled-back txn writes nothing
    let _ = db.transaction(|t| { t.put("a", vec![(2,Value::Int64(9))])?; Err(MongrelError::Other("boom".into())) });
    assert_eq!(db.table("a").unwrap().count(), 1);
    let _ = snap;
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.

- [ ] **Step 3: Implement** the serial commit (spec §8.2 with a single global commit mutex for now; P3 makes it the bounded sequencer): buffer staging; on commit assign `epoch = epoch.bump_assigned()`, append data records + `TxnCommit` with `txn_id`, `group_sync`, apply staging to each table's memtable/indexes at `epoch`, persist per-table manifest `current_epoch`, `epoch.publish_visible(epoch)`. `txn_id` from a `(generation, counter)` (generation from catalog, P2.7 persists it). Use `SYSTEM_TXN_ID` only for `Flush`.

- [ ] **Step 4: Run to verify it passes** — PASS.

- [ ] **Step 5: Commit** — `git commit -am "feat(txn): atomic cross-table transactions on the shared WAL (serial applier)"`

### Task P2.6: `flushed_epoch`-gated, two-pass, bounded recovery (review fixes #4, #10)

**Files:** Modify `database.rs` (open/recovery), `txn.rs`. Test: `crates/mongreldb-core/tests/recovery_multitable.rs`.

**Interfaces:** `Database::open` performs the spec §15 sequence (two-pass, epoch-ordered, gated by `Table::flushed_epoch`).

- [ ] **Step 1: Write the failing tests**

```rust
// tests/recovery_multitable.rs
#[test]
fn recovery_replays_committed_skips_uncommitted_and_gates_by_flushed_epoch() {
    // 1) commit txns across 2 tables, drop db WITHOUT clean shutdown (simulate via raw reopen)
    // 2) reopen -> all committed rows present, none from a forced torn tail
    // 3) flush table A (flushed_epoch advances), append+commit a later txn to A, reopen
    //    -> the later txn is still applied (gated by epoch, not seq)
}
```
(Use the `crash_process.rs` pattern for an unclean reopen.)

- [ ] **Step 2: Run to verify it fails** — FAIL.

- [ ] **Step 3: Implement** spec §15 steps 1–6: bump+fsync generation/segment_no; read catalog; open tables; **pass 1** (scan markers → per-txn outcome, small metadata); **pass 2** (apply committed where `commit_epoch > table.flushed_epoch`, link `added_runs`, replay `Ddl`); discard aborted/in-flight/torn; **do not truncate** replayed segments — open a fresh segment (review fix #6); set `assigned`/`visible` to max committed.

- [ ] **Step 4: Run to verify it passes** — PASS.

- [ ] **Step 5: Commit** — `git commit -am "feat(recovery): two-pass, epoch-gated, no-truncate multi-table recovery"`

### Task P2.7: Generation-scoped `txn_id` + DDL-as-WAL (review fixes #11, #16)

**Files:** Modify `database.rs` (DDL through the txn path; persist `open_generation`), `txn.rs`, `catalog.rs`. Test: `crates/mongreldb-core/tests/recovery_multitable.rs` (add cases).

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn ddl_is_durable_via_wal_before_catalog_checkpoint() {
    // create table, crash before catalog rewrite (inject) -> reopen replays Ddl -> table exists
}
#[test]
fn txn_ids_do_not_alias_across_reopen() {
    // open gen=1 writes txn_id (1,5); reopen bumps gen=2; new txn (2,1) != any (1,*)
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.

- [ ] **Step 3: Implement** — `create_table`/`drop_table`/`add_column` append `Op::Ddl(..)` + `TxnCommit`, group-sync, then mutate the in-memory catalog + table map at publish; rewrite the `CATALOG` checkpoint lazily (outside any commit critical section). `open_generation` (catalog) is bumped+fsynced first thing in `open`; `txn_id = (generation<<32)|counter`.

- [ ] **Step 4: Run to verify it passes** — PASS; `cargo test --workspace --all-features` green.

- [ ] **Step 5: Commit** — `git commit -am "feat(ddl): WAL-logged DDL + generation-scoped txn ids"`

---

## Phase P3 — Concurrent writers + group commit + large-txn spill

Make commits parallel on P2's final format. Spec §6.1, §8.2, §8.3, §8.5, §9. No on-disk change.

### Task P3.1: Conflict index + `ActiveTxns` (broadened keys, registration ordering)

**Files:** Add to `txn.rs`. Test in `txn.rs` / `tests/txn_concurrent.rs`.

**Interfaces (spec §8.3, §9.2):**
```rust
pub enum WriteKey { Row{table_id:u64,row_id:u64}, Unique{table_id:u64,index_id:u16,key_hash:u64}, Table{table_id:u64} }
pub struct ConflictIndex { /* sharded DashMap<WriteKey, u64 /*commit_epoch*/> */ }
impl ConflictIndex {
    pub fn conflicts(&self, keys: &[WriteKey], read_epoch: Epoch) -> bool;        // any key committed > read_epoch
    pub fn record(&self, keys: &[WriteKey], commit_epoch: Epoch);
    pub fn prune_below(&self, min_active: Epoch);
}
pub struct ActiveTxns { /* min read_epoch tracker */ }
```

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn conflict_index_first_committer_wins_and_prunes_safely() {
    let ci = ConflictIndex::new();
    let k = vec![WriteKey::Row{table_id:1,row_id:7}];
    assert!(!ci.conflicts(&k, Epoch(5)));
    ci.record(&k, Epoch(6));
    assert!(ci.conflicts(&k, Epoch(5)));   // reader at 5 conflicts with commit at 6
    assert!(!ci.conflicts(&k, Epoch(6)));  // reader at 6 does not
    ci.prune_below(Epoch(7));              // no active txn below 7
    assert!(!ci.conflicts(&k, Epoch(5)));  // pruned
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.

- [ ] **Step 3: Implement** — sharded map; `conflicts` returns true if any key's recorded epoch `> read_epoch`; `prune_below` drops entries `< min_active`. Add `WriteKey` derivation for puts (Row + Unique for PK/UNIQUE columns) and DDL/TRUNCATE (Table). `ActiveTxns` registers `read_epoch` on `begin` **before** the first read (review fix #12).

- [ ] **Step 4: Run to verify it passes** — PASS.

- [ ] **Step 5: Commit** — `git commit -am "feat(txn): conflict index + active-txn registration (SI, FCW)"`

### Task P3.2: Bounded commit sequencer (validate-first) + group commit (`durable_seq`, poison)

**Files:** Modify `txn.rs` (`CommitSequencer`, `GroupCommit`), `database.rs`. Test: `tests/txn_concurrent.rs`.

**Interfaces (spec §9.3):** the commit path becomes: prepare (stream records, parallel) → sequencer `{validate-first; on success bump_assigned; append TxnCommit; record keys}` → `GroupCommit` (leader fsync → `durable_seq`; publish only when `durable_seq >= commit_seq`; fsync error sets `poisoned`) → parallel publish → in-order `publish_visible`.

- [ ] **Step 1: Write the failing tests**

```rust
// tests/txn_concurrent.rs
#[test]
fn concurrent_disjoint_writers_all_commit() {
    // N threads each insert distinct PKs across 2 tables; all commit, count == N*?, zero conflicts
}
#[test]
fn same_pk_concurrent_insert_conflicts_exactly_one_wins() {
    // 2 threads insert the SAME pk concurrently; exactly one Ok, one Err(Conflict); retry succeeds; no duplicate
}
#[test]
fn aborted_txn_consumes_no_epoch_and_visible_does_not_stall() {
    // force a conflict abort interleaved with commits; assert visible == max published epoch afterward
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.

- [ ] **Step 3: Implement** spec §9.3: move data-record appends into prepare (bounded batches under the WAL mutex); make the sequencer do only validate-first → assign → append `TxnCommit` → record keys (no fsync/disk/GB-copy); `GroupCommit` batches fsync and exposes `durable_seq`; publish gated on `durable_seq`; `poisoned` AtomicBool on fsync error; in-order `visible` advancer (min-heap of finished epochs). Keep the lock order `seq.wal → publish_lock`.

- [ ] **Step 4: Run to verify it passes** — PASS (run the concurrency tests with `--test-threads` high; add a stress loop).

- [ ] **Step 5: Commit** — `git commit -am "feat(txn): bounded validate-first sequencer + group commit (concurrent SI)"`

### Task P3.3: Lock-free reads (`ArcSwap`) + generation-sealed flush (review fixes #9, #18)

**Files:** Modify `engine.rs` (`Table` fields → `ArcSwap`/`MemGen`, `publish_lock`), flush path. Test: `tests/txn_concurrent.rs`.

**Interfaces (spec §7.4, §9.1):** `Table { memtable: ArcSwap<MemGen>, indexes: ArcSwap<IndexSet>, runs: ArcSwap<Vec<RunRef>>, publish_lock: Mutex<()> , flushed_epoch: AtomicU64 }`. Flush seals (swaps in a fresh `MemGen`) and flushes only the sealed generation.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn flush_under_concurrent_writes_loses_no_rows() {
    // writer thread commits rows continuously; main thread flushes repeatedly;
    // after join, total visible rows == total committed (no missed/duplicated rows)
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL (race / wrong count) or compile error.

- [ ] **Step 3: Implement** — convert `Table` read structures to `arc-swap` (add `arc-swap` to `Cargo.toml`); reads `load()` lock-free; publish swaps under `publish_lock`; flush seals the memtable generation atomically, appends `Op::Flush{flushed_epoch}` (system txn_id=0) **without holding `publish_lock`**, then swaps the sealed gen + `runs` under `publish_lock`, then sets `flushed_epoch`.

- [ ] **Step 4: Run to verify it passes** — PASS (run under `--release` stress + `cargo test`); `cargo clippy` clean.

- [ ] **Step 5: Commit** — `git commit -am "feat(table): lock-free reads + generation-sealed flush"`

### Task P3.4: Unbounded transactions — quarantined per-txn pending runs (review fixes #7, #8, #14)

**Files:** Modify `txn.rs` (spill), `sorted_run.rs` (uniform-epoch run + `RunRef.commit_epoch`), `database.rs` (link at commit/recovery), `retention.rs`/GC (quarantine sweep). Test: `crates/mongreldb-core/tests/txn_large.rs`.

**Interfaces (spec §8.5):** when a table's staged bytes for a txn exceed `spill_threshold`, write a **uniform-epoch** run to `tables/<id>/_txn/<txn_id>/r-<run_id>.sr`; at commit, `TxnCommit.added_runs` names it; publish moves it to `_runs/` + adds `RunRef{commit_epoch}`. `RunRef` gains `commit_epoch: u64`; uniform-epoch runs read visibility from it (not a per-row `_epoch`).

- [ ] **Step 1: Write the failing tests**

```rust
// tests/txn_large.rs
#[test]
fn transaction_larger_than_threshold_spills_and_commits_atomically() {
    // set tiny spill_threshold; insert many rows in one txn; commit;
    // assert all rows visible, peak memory bounded (rows went to a run, not memtable buffer)
}
#[test]
fn aborted_large_txn_leaves_no_visible_rows_and_gc_sweeps_pending_run() {
    // spill then rollback (or simulate crash before TxnCommit); reopen; zero rows; _txn/ swept
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.

- [ ] **Step 3: Implement** — `spill_threshold` config; bulk-write staged rows via the existing run writer into `_txn/<txn_id>/`; mark the run uniform-epoch (header flag) so its visibility epoch is `RunRef.commit_epoch`; at commit move + link with `commit_epoch`; recovery links from `added_runs`; GC sweeps `_txn/<txn_id>/` for dead txn_ids (never live ones).

- [ ] **Step 4: Run to verify it passes** — PASS.

- [ ] **Step 5: Commit** — `git commit -am "feat(txn): unbounded transactions via quarantined uniform-epoch spill runs"`

### Task P3.5: Conflict-key spill + bounded sequencer for huge write sets (review fix #17)

**Files:** Modify `txn.rs`. Test: `tests/txn_large.rs`.

**Interfaces (spec §8.5):** huge write-set keys spill to a temp sorted set; pre-validate outside the sequencer, re-check only the recent delta inside.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn huge_writeset_validation_keeps_sequencer_bounded() {
    // a txn with a very large write set commits; assert the sequencer-held duration
    // (instrumented counter) does not scale with write-set size (only with the delta)
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.

- [ ] **Step 3: Implement** — spill keys past a threshold; pre-validate the full set against the conflict index before entering the sequencer; inside, re-validate only keys recorded since the pre-check snapshot (a small bounded delta).

- [ ] **Step 4: Run to verify it passes** — PASS.

- [ ] **Step 5: Commit** — `git commit -am "feat(txn): conflict-key spill keeps the commit sequencer bounded"`

### Task P3.6: Retention-gated GC for runs, dropped tables, WAL segments (review fixes #3, #4, #5)

**Files:** Modify `retention.rs` (reaper), GC in `database.rs`/`compaction.rs`, `SharedWal` segment GC. Test: `crates/mongreldb-core/tests/snapshot_retention.rs`.

**Interfaces (spec §6.4, §7.4, §16):** the reaper deletes superseded runs / `Dropped` subdirs / WAL segments only when `min_active_snapshot` passes their retire epoch and (for segments) `max_seq < min_retained_seq`.

- [ ] **Step 1: Write the failing tests**

```rust
// tests/snapshot_retention.rs
#[test]
fn pinned_reader_blocks_physical_drop_until_guard_released() {
    // reader pins epoch E (holds SnapshotGuard); DROP table at >E; the reader still scans it OK;
    // after guard drops + reaper runs, subdir is gone
}
#[test]
fn wal_segment_not_gcd_while_in_flight_txn_holds_it() {
    // begin a txn (records in segment S) but don't commit; flush other tables past S;
    // GC must NOT delete S; commit; crash; reopen -> txn recoverable
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.

- [ ] **Step 3: Implement** — compaction keeps superseded runs and enqueues them for the reaper at the compaction epoch; `drop_table` sets `Dropped{at_epoch}` and enqueues the subdir; `SharedWal` computes `min_retained_seq` from in-flight + committed-not-flushed txns; the reaper runs opportunistically gated by `min_active_snapshot`.

- [ ] **Step 4: Run to verify it passes** — PASS.

- [ ] **Step 5: Commit** — `git commit -am "feat(gc): snapshot-retention-gated reaper for runs, dropped tables, WAL segments"`

---

## Phase P4 — Query layer over `Database`

Spec §12. Cross-table SQL/joins on one consistent snapshot.

### Task P4.1: `MongrelSession::open(Arc<Database>)` auto-registering catalog tables

**Files:** Modify `crates/mongreldb-query/src/lib.rs` (`MongrelSession`, `MongrelProvider`), `scan.rs`. Test: `crates/mongreldb-query/tests/*` (new `multi_table_sql.rs`).

**Interfaces:** `MongrelSession::open(db: Arc<Database>) -> Self` registers every live table as a `MongrelProvider{ db, table_id }`; `run(sql)` takes one `db.snapshot()` threaded through the plan; result-cache key `(sql, snapshot.epoch)`; delete `combined_epoch` (`lib.rs:962`).

- [ ] **Step 1: Write the failing test**

```rust
// tests/multi_table_sql.rs
#[tokio::test]
async fn cross_table_join_is_snapshot_consistent() {
    // build a Database with orders+customers, insert, then:
    // SELECT ... FROM orders JOIN customers ON ...  returns expected rows;
    // a concurrent commit to customers after snapshot() is NOT reflected in the in-flight query
}
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p mongreldb-query --test multi_table_sql 2>&1 | tail -5` → FAIL.

- [ ] **Step 3: Implement** — `open(Arc<Database>)`, provider gains `table_id` + reads through `Database` at a fixed snapshot; thread the snapshot; cache key uses `snapshot.epoch`; remove `combined_epoch` and the `register_db` footgun docs. Keep FK-join/pushdown fast paths (they now run per `Table` against a consistent snapshot).

- [ ] **Step 4: Run to verify it passes** — PASS; `cargo test -p mongreldb-query` green.

- [ ] **Step 5: Commit** — `git commit -am "feat(query): MongrelSession over multi-table Database, consistent join snapshot"`

### Task P4.2: DDL via SQL (`CREATE TABLE` / `DROP TABLE`)

**Files:** Modify `crates/mongreldb-query/src/lib.rs`. Test: `tests/multi_table_sql.rs`.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn create_and_drop_table_via_sql() {
    // session.run("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)").await?;
    // insert via session, SELECT works; session.run("DROP TABLE t") -> table gone
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL.

- [ ] **Step 3: Implement** — intercept `CREATE TABLE`/`DROP TABLE` statements, map to `Database::create_table`/`drop_table`, register/deregister the provider.

- [ ] **Step 4: Run to verify it passes** — PASS.

- [ ] **Step 5: Commit** — `git commit -am "feat(query): CREATE/DROP TABLE DDL mapped to the catalog"`

---

## Phase P5 — NAPI redesign (`mongreldb-node`)

Spec §13. Multi-table JS `Database` + `Table` + `Transaction` + `ConflictError` + retry. Built via `napi build` (not a workspace member).

### Task P5.1: `Database` + `Table` NAPI surface

**Files:** Modify `crates/mongreldb-node/src/lib.rs`. Test: `crates/mongreldb-node/smoke.mjs`.

**Interfaces (spec §13.2):** `#[napi] Database { inner: Arc<core::Database> }` with `create`/`open`/`createTable`/`dropTable`/`table`/`tableNames`/`sql`/`close`; `#[napi] Table { db: Arc<core::Database>, table_id: u64 }` with `put`/`putBatch`/`bulkLoadTyped`/`get`/`query`/`queryArrow`/`count` (+ `*Async`). Remove the old single-table `Database::new(path,schema,table_id)`.

- [ ] **Step 1: Write the failing smoke check**

```js
// smoke.mjs (add)
import { Database } from './index.js';
const db = Database.create(tmp);
db.createTable('a', schemaA); db.createTable('b', schemaB);
db.table('a').put(cellsA); db.table('b').put(cellsB);
assert(db.tableNames().length === 2);
const arrow = db.sql('SELECT * FROM a'); assert(arrow.length > 0);
```

- [ ] **Step 2: Build + run to verify it fails**

Run: `cd crates/mongreldb-node && npx napi build --release && node smoke.mjs`
Expected: FAIL (new API absent).

- [ ] **Step 3: Implement** — rewrite the NAPI `Database` to wrap `Arc<core::Database>`; add `Table` handles; cross-table `sql()` via an internal `MongrelSession`; keep `BigInt`/Arrow `Buffer`/`*Async` patterns (`lib.rs:404-475`).

- [ ] **Step 4: Build + run to verify it passes** — `npx napi build --release && node smoke.mjs` → PASS; regenerate `index.{js,d.ts}`.

- [ ] **Step 5: Commit** — `git commit -am "feat(node): multi-table Database + Table NAPI surface"`

### Task P5.2: Cross-table `Transaction` + `ConflictError` + retry wrapper

**Files:** Modify `crates/mongreldb-node/src/lib.rs`, `crates/mongreldb-node/index.js` (JS `transaction(fn,{retries})` wrapper). Test: `smoke.mjs`.

**Interfaces (spec §13.2/§13.3):** `#[napi] Transaction { table(name)->TxnTable, commit()->bigint, rollback() }`, `TxnTable { put/putBatch/delete }`; `Drop`-rollback; `commit` throws `ConflictError`; JS `db.transaction(fn,{retries})` retries on `ConflictError`.

- [ ] **Step 1: Write the failing smoke check**

```js
const epoch = db.transaction((t) => { t.table('a').put(cells1); t.table('b').delete(rid); });
assert(typeof epoch === 'bigint');
// conflict path: two overlapping txns on the same pk -> one throws ConflictError, retry succeeds
```

- [ ] **Step 2: Build + run to verify it fails** — FAIL.

- [ ] **Step 3: Implement** — NAPI `Transaction` holding a core `Transaction` (no global lock while open); `commit` maps `Err(Conflict)` → a JS `ConflictError`; `Drop` rolls back; add the `index.js` `transaction(fn,{retries})` helper.

- [ ] **Step 4: Build + run to verify it passes** — PASS.

- [ ] **Step 5: Commit** — `git commit -am "feat(node): cross-table transactions with ConflictError + retry"`

### Task P5.3: `WriteBuffer` from `Table`; smoke for atomic cross-table visibility

**Files:** Modify `crates/mongreldb-node/src/lib.rs`, `smoke.mjs`.

- [ ] **Step 1: Write the failing smoke check** — construct `WriteBuffer` from a `Table` (not `Database`); assert a cross-table `transaction` is atomically visible (read mid-txn from another handle sees nothing; after commit sees all).
- [ ] **Step 2: Build + run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** — repoint `WriteBuffer::new(table, threshold)`; finalize smoke assertions.
- [ ] **Step 4: Build + run to verify it passes** — `node smoke.mjs` → PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(node): table-scoped WriteBuffer + atomic-visibility smoke"`

---

## Phase P6 — Daemon + client

Spec §14. One warm `Database`; table-qualified + `/txn` + `/tables` routes.

### Task P6.1: `mongreldb-server` over one `Database` with table-qualified routes

**Files:** Modify `crates/mongreldb-server/src/*`. Test: `crates/mongreldb-server/tests/*` (HTTP integration).

**Interfaces:** routes `POST /tables`, `DELETE /tables/:name`, `GET /tables`, `POST /tables/:name/{put,get,query,count}`, `POST /sql`, `POST /txn` (begin→ops→commit in one body).

- [ ] **Step 1: Write the failing test** — spin the server on a temp `Database`; `POST /tables` two tables; `POST /sql` a join; assert rows.
- [ ] **Step 2: Run to verify it fails** — `cargo test -p mongreldb-server 2>&1 | tail -5` → FAIL.
- [ ] **Step 3: Implement** — replace the per-table `Db` state with `Arc<Database>` + `MongrelSession`; add the routes; `/txn` body = a list of table-qualified ops applied in one `transaction`.
- [ ] **Step 4: Run to verify it passes** — PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(server): one Database, table-qualified + /txn + /tables routes"`

### Task P6.2: `mongreldb-client` + NAPI `RemoteDatabase` mirror the routes

**Files:** Modify `crates/mongreldb-client/src/*`, `crates/mongreldb-node/src/lib.rs` (`RemoteDatabase`), `smoke.mjs`.

- [ ] **Step 1: Write the failing test/smoke** — `RemoteDatabase.connect(url).table('a').count()` and `.sql(...)` and a `/txn` batch against a live server.
- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** — `.table(name)`, `.sql(...)`, `/txn` in the client + `RemoteDatabase`.
- [ ] **Step 4: Run to verify it passes** — PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(client): table-qualified RemoteDatabase + /txn"`

---

## Phase P7 — GC/check/doctor + optional import tool

Spec §16. (P3.6 already added retention-gated GC; this finalizes `check`/`doctor` + the offline import tool.)

### Task P7.1: `check`/`doctor` for multi-table integrity

**Files:** Modify the check/doctor entry points in `mongreldb-core`. Test: `crates/mongreldb-core/tests/*` (extend).

**Interfaces:** `check` authenticates the catalog + every manifest (or SHA-256 for plaintext), verifies run footers + run MACs, verifies `flushed_epoch ≤ WAL head` and no live segment below `min_retained_seq`, and that runs ↔ `RunRef`s are consistent; `doctor` quarantines unreadable tables.

- [ ] **Step 1: Write the failing tests** — build a DB, corrupt (a) a manifest byte (encrypted → auth fail), (b) drop a `RunRef`'s file; assert `check` reports each; `doctor` quarantines the bad table and the DB still opens.
- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** — extend check/doctor per spec §16.
- [ ] **Step 4: Run to verify it passes** — PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(check): multi-table integrity check + doctor quarantine"`

### Task P7.2: Offline `import-table` tool (optional, single-table → Database)

**Files:** Create a small bin in `mongreldb-core` or a `tools/` example. Test: round-trip an old-layout single-table dir into a `Database` table.

- [ ] **Step 1: Write the failing test** — produce an old single-table dir (pre-Database), run `import-table`, assert rows appear under a named table in a `Database`.
- [ ] **Step 2: Run to verify it fails** — FAIL.
- [ ] **Step 3: Implement** — read the old runs/manifest, `create_table`, bulk-load the rows.
- [ ] **Step 4: Run to verify it passes** — PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(tools): offline import-table into a multi-table Database"`

---

## Cross-cutting verification (run after each phase)

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test -p mongreldb-core --features encryption          # encryption suite
# node (P5+):
( cd crates/mongreldb-node && npx napi build --release && node smoke.mjs )
```

Phase exit gates (the spec §18 matrix maps to tasks): P2 → crash/atomicity/recovery (`txn_atomic`, `recovery_multitable`); P3 → concurrency/conflict/visibility/large-txn/retention (`txn_concurrent`, `txn_large`, `snapshot_retention`); P3 also re-runs the fd-budget assertion (one WAL fd for N tables); P5 → `smoke.mjs` atomic cross-table visibility.

---

## Self-review checklist (run before handing off)

- [ ] **Spec coverage:** every spec section maps to ≥1 task — §5 Catalog→P1.3/P2.7; §6 epoch/MVCC/retention→P1.1/P1.2/P3.6; §7 WAL→P2.1–P2.4,P3.3; §8 txns→P2.5,P3.1–P3.5; §9 concurrency→P3.1–P3.3; §10 caches→P1.4/P1.5; §11 encryption→P1.3 + carried in each metadata writer; §12 query→P4; §13 node→P5; §14 daemon/client→P6; §15 recovery→P2.6/P2.7; §16 GC/check→P3.6/P7. All 23 review fixes are tagged on their task.
- [ ] **No placeholders:** every code step shows real test/impl code or a precise interface + spec-section pointer (the spec holds the full algorithms — DRY).
- [ ] **Type consistency:** `EpochAuthority`, `WriteKey`, `AddedRun`, `SharedWal`, `Transaction`, `SnapshotRegistry`, `Database`/`Table` signatures match across the tasks that produce/consume them.
