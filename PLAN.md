# PLAN: Declarative and operational follow-up

Status: **partially complete** as of 2026-07-11. All scheduled work is
shipped except Tier-2 Language Client Support, which remains incomplete.

## Shipped

### Declarative constraints

- Engine-enforced `TypeId::Enum { variants }`, including native and bulk write
  paths.
- Engine `CheckExpr::Regex` with cached compilation.
- Engine defaults: `Static`, `Now`, and `Uuid`.
- SQL `CHECK (expr)` lowering, including arithmetic, comparisons, boolean
  expressions, null semantics, and regex operators.
- `ALTER TABLE ... ADD CONSTRAINT ... CHECK` and durable constraint recovery.
- `ALTER TABLE ... ADD COLUMN ... NOT NULL DEFAULT` backfill.

### Kit delegation

The sibling `mongreldb_kit` repository lowers these shapes into engine schema
constraints:

- `enum_values` to `TypeId::Enum` plus a compatibility CHECK for engine 0.46.2.
- `regex` to `CheckExpr::Regex`.
- table `check_constraints` and column `check_expr` to `CheckExpr`.
- `Static`, `Now`, and `Uuid` defaults to `DefaultExpr`.

Kit write validation no longer repeats engine-owned enum, regex, table CHECK,
or column CHECK evaluation. Kit still owns type, null, length, range, sequence,
and named custom-default handling.

### Language clients

README lists 34 supported languages: Rust, TypeScript, and Python are the three
Tier-1 surfaces; 31 Tier-2 repositories provide HTTP clients.

The server-side `GET`/`PUT /history/retention` contract and the presence-aware
Kit default deserialization are implemented and tested in the main repository.
Rust exposes the three retention controls, `mongreldb-client` has dedicated
HTTP tests, `mongreldb-node` has `RemoteDatabase` forwarding tests, and the
Tier-1 TypeScript kit has remote/live retention tests, the full static-default
matrix, and updated docs. The remaining Language Client Support work is:

- **Tier-1 Python:** embedded tests do not cover the cannot-restore-history
  rule; remote tests do not assert the exact PUT body / GET response keys; docs
  are not updated.
- **All 31 Tier-2 repositories remain incomplete:** required API names, focused
  retention wire tests, live `AS OF EPOCH` retention tests, full static-default
  matrices, decoded-JSON wire assertions, and documentation updates are missing
  or incorrect in most languages. PHP and Swift had complete wire coverage for
  prior shapes, but the new retention/default contracts are still not fully
  covered.

### Operational program

- **#1 Time travel:** SQL `AS OF EPOCH`, snapshot pinning, retention-floor
  enforcement, historical full-scan safety, cache isolation, and current-schema
  semantics.
- **#2 CDC:** commit-time row events, stable event IDs, SSE keep-alive,
  `Last-Event-ID` resume, WAL-backed replay, retention-gap errors, and SQL
  `LISTEN` support.
- **#3 TTL:** durable timestamp-column policies, visibility filtering,
  cache-safe reads, WAL/replication recovery, and compaction reclamation.
- **#5 Materialized views:** persistent definitions, atomic full refresh, and
  checkpointed single-table incremental aggregate refresh.
- **#6 Replication:** consistent snapshot bootstrap, complete-transaction WAL
  streaming, retention-gap signaling, durable follower watermarks, idempotent
  retry, encryption/auth coverage, and read-only followers.
- **#7 Backup and PITR:** online checksummed backup with GC pins, encrypted
  reopen coverage, WAL archival, epoch/timestamp cutoffs, checksummed chunks,
  and staged restore.
- **#8 Observability:** Prometheus metrics, slow-query tracing, and cache-safe
  `EXPLAIN ANALYZE`.
- **#9 Online schema slice:** `NOT NULL DEFAULT` backfill.
- **#10 HTTP sessions:** auth binding, serialized access, idle expiry, rollback
  on close, 1024-entry LRU result/plan caches, and a 10,000-operation SQL
  transaction staging cap.
- **#11 Prepared statements:** session-owned prepare/execute/deallocate,
  parameter validation, DDL invalidation, and streaming execution.
- **#12 Streaming:** DataFusion `SendableRecordBatchStream` through Arrow IPC
  and `Body::from_stream`, with cache/native-vector bypass and cancellation by
  body drop.
- **#15 Data security:** row-level policies, column privileges, masking,
  explicit principal propagation, security-aware cache behavior, persistent
  catalog/WAL state, DDL retargeting, and protected admin endpoints.
- **#18 Foreign keys:** final-write-set validation plus `ON UPDATE RESTRICT`,
  `CASCADE`, and `SET NULL` with durable update identity.
- **#19 Audit:** bounded in-memory audit ring, stderr mirror, auth and DDL/
  privilege events, and an Admin-only `/audit` endpoint. No tamper-evident
  claim is made.

## Demand-gated backlog

These are deliberate deferrals, not unfinished current-plan tasks:

- Cursor/keyset pagination. Arrow result streaming is complete.
- Arbitrary `TTL_EXPR` policies.
- SQL wall-clock time travel and historical-schema reconstruction. PITR already
  supports timestamp cutoffs through its durable commit ledger.
- A separate cap for named prepared statements. Session lifetime and caches are
  already bounded.
- ENUM helper SQL functions and `DefaultExpr::Custom`.
- Composite/covering indexes, pending workload evidence beyond bitmap
  intersection and PAX projection.
- Fully concurrent online schema migration, pending a proven need beyond the
  shipped `NOT NULL DEFAULT` slice.
- In-engine Raft. Prefer orchestrator-managed fenced promotion after the
  shipped replication work.
- Statement-level and DDL triggers, pending demand beyond existing row and
  external trigger support.
- PL/pgSQL, pending measured demand and execution-limit requirements.
- Durable or cryptographically chained audit storage, pending a defined
  retention and trust model.

## Not pursued

- **#13 2PC/XA:** conflicts with the embedded, single-node identity. Use CDC,
  outbox, sagas, and idempotent writes.

## Language Client Support

Status: **in progress** as of 2026-07-11. Section 1 (main-repository server
contracts) and Tier-1 TypeScript and Python parity are complete; section 3
(Tier-2 repositories) remains unfinished against the frozen retention and
static-default contracts below.

The README's authoritative matrix contains 34 languages: Rust, TypeScript, and
Python are embedded Tier 1 clients; the other 31 repositories are Tier 2 HTTP
clients. The local `v0.47.1` tag contains history-retention commit `f75a98b`
and static Kit default commit `0f34932`.

Current state:

- Rust already exposes `Database::set_history_retention_epochs`,
  `history_retention_epochs`, and `earliest_retained_epoch`; its persistence and
  retention-floor behavior are covered by
  `crates/mongreldb-core/tests/time_travel.rs`.
- TypeScript embedded support already exposes the three controls through the
  NAPI `Database` and `KitDatabase`. Python embedded support exposes
  `rows_at_epoch`, but not the three retention controls.
- The 31 Tier 2 clients cannot implement the retention API until the
  `mongreldb-server` contract is frozen; that contract is now in place.
- All 34 languages can already express static string, number, and boolean
  defaults. All except Lua can express an explicit JSON-null field. No Tier 2
  repository has one focused wire test covering string, number, boolean, and
  null together. PHP and Swift also lack focused `default_expr` wire coverage.
- `KitColumnDef.default_value` is now deserialized with presence awareness:
  a missing field means no default and an explicit JSON null becomes
  `DefaultExpr::Static(Value::Null)`. The legacy reinterpretation of
  `default_value: "now"` and `"uuid"` has been removed; dynamic defaults must
  use `default_expr`.

### 1. Freeze and implement the server contracts first

**Status: complete.** The main repository implements the frozen contracts and
all required tests pass.

In `crates/mongreldb-server/src/lib.rs`:

- Add `GET /history/retention`. Return exactly
  `{"history_retention_epochs": <u64>, "earliest_retained_epoch": <u64>}`.
- Add `PUT /history/retention` with exactly
  `{"history_retention_epochs": <u64>}`. Call
  `Database::set_history_retention_epochs`, then return the same response shape
  as GET using the post-update values.
- Apply the existing auth middleware and explicitly require
  `Permission::Admin` in both handlers because this is a database-wide durable
  GC/time-travel policy. Use the existing `status_for_error` mapping.
- Reject negative, fractional, non-numeric, and greater-than-`u64` inputs with
  HTTP 400. Preserve the core behavior that increasing retention cannot restore
  already-pruned history.
- Add server integration tests in
  `crates/mongreldb-server/tests/server_test.rs` for GET, PUT, the exact JSON
  keys and integer values, persistence after reopen, and the unchanged
  `earliest_retained_epoch` when a larger window cannot restore lost history.
  Add authenticated non-admin rejection and admin success cases in
  `crates/mongreldb-server/tests/security_test.rs`.
- Document the environment fallback, endpoint, auth requirement, response
  fields, and cannot-restore-history rule in `README.md` and
  `docs/08-daemon.md`.

In `crates/mongreldb-server/src/kit.rs` and
`crates/mongreldb-server/tests/kit_txn_test.rs`:

- Replace `Option<serde_json::Value>` with a presence-aware deserialization
  shape so missing `default_value` means no default while explicit JSON null
  becomes `DefaultExpr::Static(Value::Null)`.
- Make `default_value` exclusively literal. `default_expr` remains the only
  dynamic discriminator and accepts only `"now"` or `"uuid"`. Remove the
  legacy reinterpretation of `default_value: "now"` and `"uuid"`; all current
  clients already have `default_expr`. Call this compatibility change out in
  `docs/08-daemon.md` and the release notes.
- Keep the existing precedence rule for malformed requests containing both
  fields: `default_expr` wins. Every maintained client must serialize exactly
  one field.
- Add one end-to-end create/insert test containing a string, integer, boolean,
  and explicit-null static default plus `default_expr: "now"`. Assert the
  engine schema contains the four matching `DefaultExpr::Static` variants and
  that omitted cells receive the non-null defaults. Add literal `"now"` and
  `"uuid"` string cases so they cannot regress back to dynamic expressions.

In `crates/mongreldb-client/src/lib.rs`, add a shared deserializable
`HistoryRetention` response with `u64` fields and these blocking-client methods:
`set_history_retention_epochs(u64) -> ClientResult<HistoryRetention>`,
`history_retention_epochs() -> ClientResult<u64>`, and
`earliest_retained_epoch() -> ClientResult<u64>`. Cover exact method, path,
request, response, and error propagation in the crate's HTTP client tests.

In `crates/mongreldb-node/src/lib.rs` and
`crates/mongreldb-node/native.d.ts`, expose the same three methods on NAPI
`RemoteDatabase` after the Rust HTTP client exists. Keep the already-shipped
embedded `Database` methods unchanged. Add NAPI remote forwarding tests and
regenerate `native.d.ts` with the normal release build.

### 2. Finish Tier 1 parity

**Status:** Rust, TypeScript, and Python are complete.

- **Rust:** no public API change. Keep the existing core history tests and
  `DefaultExpr::Static` engine tests. The Rust HTTP helper work is listed above.
- **TypeScript:** ✅ complete. Embedded history APIs already exist; the work
  extended `packages/kit/src/db.test.ts` with the full static-default matrix,
  added transport coverage in `packages/kit/src/remote.test.ts`, live coverage
  in `packages/kit/src/live_remote.test.ts`, and updated `README.md`,
  `docs/typescript.md`, and `docs/defaults.md`.
- **Python:** ✅ complete. Embedded history APIs were added to the Rust kit
  and PyO3 surface and exposed from `python/mongreldb_kit/mongreldb_kit/__init__.py`;
  `python/tests/test_basic.py` covers set/get/earliest, retained reads,
  persistence after reopen, and the cannot-restore-history rule. The same three
  HTTP methods were added to `python/mongreldb_kit/mongreldb_kit/remote.py` with
  exact request/response assertions in `python/tests/test_remote.py`. Docs were
  updated in `README.md`, `docs/python.md`, and `docs/defaults.md`.

### 3. Finish all 31 Tier 2 repositories

For every repository below, the retention methods must use the frozen GET/PUT
contract, the client's existing base URL/auth/error transport, and an unsigned
64-bit-capable return type where the language has one. Languages without one
must use their existing lossless epoch representation or reject overflow
instead of rounding. Each focused transport test must assert the exact
HTTP method, `/history/retention` path, PUT body key, GET response keys, and
propagation of a non-2xx response. Each live test must set a window before
writes, read both getters, update a row, and prove an older epoch remains
readable through the client's existing SQL `AS OF EPOCH` surface where that
surface exists.

Every create-table wire test must send distinct columns with
`default_value: "draft"`, `default_value: 7`, `default_value: true`, an explicit
`default_value: null` key, literal `default_value: "now"`, and
`default_expr: "now"`. It must inspect decoded request JSON, not text, and prove
the five literal values preserve their JSON types while `default_expr` remains
separate. Typed clients must emit only `default_expr` when it is set, otherwise
typed `default_value`, then any legacy string field.

| Language/repository | Retention API and source files | Required tests and docs |
|---|---|---|
| C `mongreldb_c` ✅ | Add `mongreldb_set_history_retention_epochs`, `mongreldb_history_retention_epochs`, and `mongreldb_earliest_retained_epoch` to `include/mongreldb.h` and `src/mongreldb.c`. Keep `mongreldb_column.default_value_json`; it already carries typed JSON. | Extend `tests/test_wire_shape.c` with the full default matrix and retention wire contract; add live coverage in `tests/test_mongreldb.c`; update `README.md` and `docs/quickstart.md`. |
| C#/.NET `mongreldb_dotnet` ✅ | Add async `SetHistoryRetentionEpochsAsync`, `HistoryRetentionEpochsAsync`, and `EarliestRetainedEpochAsync` to `src/Client/MongrelDBClient.cs`. Existing `Dictionary<string, object?>` create-table bodies already preserve scalars. | Extend `tests/MongrelDB.Tests/CreateTableWireShapeTests.cs` and `tests/MongrelDB.Tests/LiveTests.cs`; update `README.md` and `docs/quickstart.md`. |
| C++ `mongreldb_cpp` ✅ | Add `set_history_retention_epochs`, `history_retention_epochs`, and `earliest_retained_epoch` to `MongrelDBClient` in `include/mongreldb/mongreldb.hpp` and `include/mongreldb/mongreldb_impl.hpp`. Keep `Column::default_value_json`. | Extend `tests/test_wire_shape.cpp`; add live coverage in `tests/test_mongreldb.cpp`; update `README.md` and `docs/quickstart.md`. |
| Clojure `mongreldb_clojure` | Add `set-history-retention-epochs`, `history-retention-epochs`, and `earliest-retained-epoch` to `src/visorcraft/mongreldb/core.clj`. Column maps already pass typed scalars unchanged. | Extend `tests/visorcraft/mongreldb/live_test.clj` with the full default and retention cases; update `README.md` and `docs/quickstart.md`. |
| Crystal `mongreldb_crystal` ✅ | Add `set_history_retention_epochs`, `history_retention_epochs`, and `earliest_retained_epoch` to `src/mongreldb.cr`. `Column`/`CellValue` in `src/mongreldb/types.cr` already supports the scalar matrix. | Extend `spec/unit_spec.cr` and `spec/live_spec.cr`; update `README.md` and `docs/quickstart.md`. |
| D `mongreldb_d` ✅ | Add `setHistoryRetentionEpochs`, `historyRetentionEpochs`, and `earliestRetainedEpoch` to `source/mongreldb/client.d`. Keep `Column.default_value_json`. | Extend `tests/test_wire_shape.d` and `tests/main.d`; update `README.md` and `docs/quickstart.md`. |
| Dart `mongreldb_dart` ✅ | Add `setHistoryRetentionEpochs`, `historyRetentionEpochs`, and `earliestRetainedEpoch` to `lib/src/mongreldb.dart`; `lib/mongreldb.dart` already exports that surface. Generic maps already preserve scalars. | Extend `test/wire_shape_test.dart` and `test/mongreldb_test.dart`; update `README.md` and `docs/quickstart.md`. |
| Elixir `mongreldb_elixir` | Add `set_history_retention_epochs`, `history_retention_epochs`, and `earliest_retained_epoch` to `lib/mongreldb.ex`. Maps already preserve scalars. | Extend `test/create_table_wire_test.exs` and `test/mongreldb_live_test.exs`; update `README.md` and `docs/quickstart.md`. |
| Erlang `mongreldb_erlang` | Export `set_history_retention_epochs`, `history_retention_epochs`, and `earliest_retained_epoch` from `src/mongreldb.erl`. Maps already preserve scalars. | Extend `test/mongreldb_unit_test.erl` and `test/mongreldb_live_test.erl`; update `README.md` and `docs/quickstart.md`. |
| F# `mongreldb_fsharp` | Add `SetHistoryRetentionEpochs`, `HistoryRetentionEpochs`, and `EarliestRetainedEpoch` to `src/Visorcraft.MongrelDB/Client.fs`. Existing `IDictionary<string,obj>` bodies preserve scalars. | Extend `tests/Visorcraft.MongrelDB.Tests/UnitTests.fs` and `tests/Visorcraft.MongrelDB.Tests/LiveTests.fs`; update `README.md` and `docs/quickstart.md`. |
| Fortran `mongreldb_fortran` | Add type-bound `set_history_retention_epochs`, `history_retention_epochs`, and `earliest_retained_epoch` procedures to `src/mongreldb.f90`; use `src/mongreldb_json.f90` for numeric response parsing. Raw `columns_json` already preserves scalars. | Extend `tests/wire_shape_test.f90` and `tests/live_test.f90`; update `README.md` and `docs/quickstart.md`. |
| Gleam `mongreldb_gleam` | Add `set_history_retention_epochs`, `history_retention_epochs`, and `earliest_retained_epoch` to `src/mongreldb.gleam`. `ColumnWithDefaults.default_value_json` already accepts `json.string/int/bool/null`. | Extend `test/wire_shape_test.gleam`, `test/mongreldb_test.gleam`, and `test/mongreldb_live_test.gleam`; update `README.md` and the stale default examples in `docs/quickstart.md`. |
| Go `mongreldb_go` | Add `SetHistoryRetentionEpochs`, `HistoryRetentionEpochs`, and `EarliestRetainedEpoch` to `client.go`. Keep `Column.DefaultValueJSON`; explicit null remains representable with `json.RawMessage("null")`. | Extend `create_table_wire_shape_test.go`, `httptest_test.go`, and `client_test.go`; update `README.md` and `docs/quickstart.md`. |
| Java `mongreldb_java` | Add `setHistoryRetentionEpochs(long)`, `historyRetentionEpochs()`, and `earliestRetainedEpoch()` to `src/main/java/dev/visorcraft/mongreldb/MongrelDB.java`. Generic maps already preserve scalars. | Extend `src/test/java/dev/visorcraft/mongreldb/MongrelDBWireShapeTest.java` and `src/test/java/dev/visorcraft/mongreldb/MongrelDBLiveTest.java`; update `README.md` and `docs/quickstart.md`. |
| Julia `mongreldb_julia` | Export `setHistoryRetentionEpochs`, `historyRetentionEpochs`, and `earliestRetainedEpoch` from `src/MongrelDB.jl`. `Dict` plus `nothing` already preserves the scalar matrix. | Extend `test/wire_shape_test.jl`, `test/json_test.jl`, and `test/live_test.jl`; update `README.md` and `docs/quickstart.md`. |
| Kotlin `mongreldb_kotlin` | Add `setHistoryRetentionEpochs(Long)`, `historyRetentionEpochs()`, and `earliestRetainedEpoch()` to `src/main/kotlin/dev/visorcraft/mongreldb/MongrelDB.kt`. Generic nullable maps already preserve scalars. | Extend `src/test/kotlin/dev/visorcraft/mongreldb/CreateTableWireShapeTest.kt` and `src/test/kotlin/dev/visorcraft/mongreldb/MongrelDBLiveTest.kt`; update `README.md` and the stale defaults section in `docs/quickstart.md`. |
| Lua `mongreldb_lua` | First add a public JSON-null sentinel and encoder branch in `src/mongreldb/json.lua`; Lua `nil` deletes a table key and cannot represent explicit null. Then add `Client:setHistoryRetentionEpochs`, `Client:historyRetentionEpochs`, and `Client:earliestRetainedEpoch` to `src/mongreldb/init.lua`. | Extend `tests/json_test.lua`, `tests/wire_shape_test.lua`, and `tests/live_test.lua`; update `README.md` and `docs/quickstart.md`. |
| Mojo `mongreldb_mojo` | Add `set_history_retention_epochs`, `history_retention_epochs`, and `earliest_retained_epoch` to `src/mongreldb/mongreldb.mojo`. Python-backed dictionaries already preserve scalars. | Extend `tests/wire_shape_test.mojo` and `tests/live_test.mojo`; update `README.md` and `docs/quickstart.md`. |
| Nim `mongreldb_nim` | Add `setHistoryRetentionEpochs`, `historyRetentionEpochs`, and `earliestRetainedEpoch` to `src/mongreldb.nim`. Keep `Column.defaultValueJson`; explicit null uses `some(newJNull())`. | Extend `tests/test_wire_shape.nim` and `src/mongreldb/tests/live_test.nim`; update `README.md` and the stale defaults section in `docs/quickstart.md`. |
| Objective-C `mongreldb_objc` | Add `setHistoryRetentionEpochs:error:`, `historyRetentionEpochs:`, and `earliestRetainedEpoch:` to `src/MongrelDBClient.h` and `src/MongrelDBClient.m`. `defaultValueJSON` already accepts `NSNull`. | Extend `tests/test_wire_shape.m` and `tests/test_mongreldb.m`; update `README.md` and `docs/quickstart.md`. |
| Odin `mongreldb_odin` | Add `set_history_retention_epochs`, `history_retention_epochs`, and `earliest_retained_epoch` to `mongreldb/mongreldb.odin`. Keep `Column.default_value`, `default_scalar`, and `default_expr`. | Extend `tests/wire_shape_test.odin` and `tests/mongreldb_live_test.odin`; update `README.md` and `docs/quickstart.md`. |
| Perl `mongreldb_perl` | Add `setHistoryRetentionEpochs`, `historyRetentionEpochs`, and `earliestRetainedEpoch` to `lib/MongrelDB.pm`. Generic JSON::PP bodies already preserve scalars. | Extend `t/wire_shape_test.t` and `t/live_test.t`; update `README.md` and `docs/quickstart.md`. |
| PHP `mongreldb_php` | Add `setHistoryRetentionEpochs`, `historyRetentionEpochs`, and `earliestRetainedEpoch` to `src/Database.php`. Arrays and `json_encode` already preserve scalars. | Extend `tests/KitCreateTableConformanceTest.php` with number, boolean, null, literal `"now"`, and `default_expr`; add retention transport coverage to `tests/HttpConformanceTest.php`; update misleading default docs in `README.md` and `docs/quickstart.md`. |
| PowerShell `mongreldb_powershell` | Add `Set-MongrelDBHistoryRetention`, `Get-MongrelDBHistoryRetention`, and `Get-MongrelDBEarliestRetainedEpoch` to `src/MongrelDB.psm1` and export them from `src/MongrelDB.psd1`. Keep `ConvertTo-MongrelDBCreateTableBody`. | Extend `tests/wire_shape_test.ps1` and `tests/live_test.ps1`; update `README.md` and `docs/quickstart.md`. |
| R `mongreldb_r` | Add `mongreldb_set_history_retention`, `mongreldb_history_retention`, and `mongreldb_earliest_retained_epoch` to `R/api.R` and export them from `NAMESPACE`. Existing `encode_payload` preserves scalars. | Extend `tests/testthat/test-create-table-wire-shape.R` and `tests/testthat/test-json.R`; add live behavior to `tests/testthat/test-live.R`; update `README.md` and `docs/quickstart.md`. |
| Ruby `mongreldb_ruby` | Add `set_history_retention_epochs`, `history_retention_epochs`, and `earliest_retained_epoch` to `lib/mongreldb.rb`. Hash/JSON bodies already preserve scalars. | Extend `spec/create_table_wire_shape_spec.rb` and `test/live_test.rb`; update `README.md` and `docs/quickstart.md`. |
| Scala `mongreldb_scala` | Add `setHistoryRetentionEpochs`, `historyRetentionEpochs`, and `earliestRetainedEpoch` to `src/main/scala/dev/visorcraft/mongreldb/MongrelDB.scala`. Existing `Json.scala` supports the scalar matrix. | Extend `src/test/scala/dev/visorcraft/mongreldb/CreateTableWireShapeTest.scala` and `src/test/scala/dev/visorcraft/mongreldb/MongrelDBLiveTest.scala`; update `README.md` and `docs/quickstart.md`. |
| Swift `mongreldb_swift` | Add `setHistoryRetentionEpochs`, `historyRetentionEpochs`, and `earliestRetainedEpoch` to `Sources/MongrelDB/MongrelDBClient.swift`. Existing `[String: Any]` bodies support `NSNull`. | Extend `Tests/MongrelDBTests/CreateTableWireShapeTests.swift` with number, boolean, `NSNull`, literal `"now"`, and `default_expr`; add URLProtocol retention tests there and live behavior in `Tests/MongrelDBTests/MongrelDBLiveTests.swift`; update `README.md` and `docs/quickstart.md`. |
| Tcl `mongreldb_tcl` | Add and namespace-export `setHistoryRetentionEpochs`, `historyRetentionEpochs`, and `earliestRetainedEpoch` in `src/mongreldb.tcl`. Keep `default_value_json` for non-string scalars. | Extend `tests/wire_shape_test.tcl` and `tests/live_test.tcl`; update `README.md` and `docs/quickstart.md`. |
| V `mongreldb_v` | Add `set_history_retention_epochs`, `history_retention_epochs`, and `earliest_retained_epoch` to `mongreldb/mongreldb.v`. Keep `Column.default_value`, `default_scalar`, and `default_expr`. | Extend `tests/wire_shape_test.v` with number and null plus retention coverage; add live behavior to `tests/mongreldb_live_test.v`; update `README.md` and `docs/quickstart.md`. |
| Zig `mongreldb_zig` | Add `setHistoryRetentionEpochs`, `historyRetentionEpochs`, and `earliestRetainedEpoch` to `Client` in `src/mongreldb.zig`. Keep `Column.default_value`, `default_scalar`, and `default_expr`. | Extend `tests/wire_shape.zig` with number and null plus retention coverage; add live behavior to `tests/live_test.zig`; update `README.md` and `docs/quickstart.md`. |

### 4. Completion gates

- Main repository: `cargo fmt --check`, `cargo clippy --workspace --all-targets
  --all-features -- -D warnings`, `cargo test --workspace --all-features`, and
  `cd crates/mongreldb-node && npm run build`.
- `mongreldb_kit`: Rust workspace tests, TypeScript build/typecheck/tests, Python
  tests, and regenerated native artifacts against MongrelDB containing the
  server contract.
- Every Tier 2 repository: its documented format/build/unit gate, the focused
  default/retention transport test, and its live daemon suite against the same
  server build. A missing local toolchain is not a pass; run that gate in CI or
  the repository's supported container before marking the language complete.
- Final audit must report exactly 34/34 language rows, 31/31 Tier 2 retention
  clients, 31/31 full static-default wire matrices, and zero undocumented
  toolchain skips. As of the latest review this final audit has not yet been
  achieved.

## Verification

Current-tree gates run on 2026-07-11:

- Main workspace: format clean; workspace all-feature tests passed.
- NAPI addon: release build passed.
- Rust HTTP client: all-feature tests passed.
- `mongreldb-node`: `npm test` passed.
- `mongreldb_kit`: 169 Rust tests passed.
- TypeScript Kit: build/typecheck clean, 285 tests passed and 9 live tests
  skipped.
- Python Kit: 149 tests passed, including remote and shared conformance.
- Tier-2 gates passed where available: C, C++, Clojure, D, Dart focused wire,
  Elixir, Java, Julia, Kotlin, Lua, Nim, Odin focused wire, Perl, PHP,
  PowerShell container wire, Ruby, Tcl container wire, V focused wire, and Zig.
- R syntax passed; full tests require unavailable `jsonlite` and `testthat`.
- Toolchains unavailable locally: .NET/F#, Crystal, Erlang `rebar3`, Fortran
  `fpm`, Gleam, Go, Mojo, Scala, and Swift.
- Objective-C build is blocked by the legacy runtime rejecting `-fobjc-arc`.
- Dart full shared-daemon rerun had a pre-existing range-state collision after
  its focused wire tests passed. Odin's full wire suite has a pre-existing
  constraint-test leak/hang after its library build and focused tests passed.
- Every Tier-2 repository passes `git diff --check`.

Sections 1 and 2 of Language Client Support are now complete. All 31 Tier-2
repositories still need the retention/default work described in section 3.
