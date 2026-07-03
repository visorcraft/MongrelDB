# Trigger Programs and External Table Modules

This document scopes what it would take to add two SQLite-compatible feature
families to MongrelDB without baking SQLite concepts into the core engine:

- Trigger programs: row-level programs that run around table/view writes.
- External table modules: a module system behind `CREATE VIRTUAL TABLE` and
  eponymous table-valued functions.

The implementation should be modular first. SQL compatibility is one frontend
over engine-level extension points, not the shape of the engine itself.

## Goals

- Preserve MongrelDB's core invariants: WAL durability, MVCC snapshots,
  constraint enforcement, typed Kit API semantics, and index-backed query
  planning.
- Make triggers and table modules available to SQL, HTTP, NAPI, and Rust users
  through shared catalog and execution layers.
- Keep compatibility syntax where it is useful: `CREATE TRIGGER`,
  `DROP TRIGGER`, `CREATE VIRTUAL TABLE ... USING module(...)`, table-valued
  functions, and PRAGMA introspection.
- Avoid a quick SQL-only shim that native writes bypass. If a database has
  cataloged triggers, they must be enforced at the same layer as constraints.
- Make the extension APIs explicit and safe: deterministic execution,
  bounded recursion, capability declarations, typed schemas, and no arbitrary
  dynamic code loading by default.

## Non-Goals

- Do not embed SQLite, expose SQLite VM bytecode, or adopt SQLite's file format.
- Do not enable arbitrary loadable native extensions through SQL. MongrelDB's
  existing `load_extension(...)` policy should remain disabled unless a future
  deployment model deliberately opts in.
- Do not try to exactly preserve SQLite's undefined `BEFORE` trigger edge cases.
  Where SQLite documents undefined behavior, MongrelDB should pick deterministic
  semantics and document the difference.
- Do not special-case FTS or R-Tree directly in the SQL parser. They should be
  built-in modules on the same module API available to future modules.

## SQLite Reference Surface

SQLite trigger behavior to account for:

- `CREATE TRIGGER` supports `BEFORE`, `AFTER`, and `INSTEAD OF`.
- SQLite currently supports row-level triggers, not statement-level triggers.
- Trigger events are `INSERT`, `UPDATE`, and `DELETE`; `UPDATE OF column...`
  narrows an update trigger.
- Trigger `WHEN` clauses and bodies can reference `NEW.column` and `OLD.column`
  according to the event.
- `INSTEAD OF` triggers apply to views.
- Trigger programs can use a `RAISE(...)` function to abort, fail, rollback, or
  ignore.
- Recursive trigger behavior is controlled separately from trigger definition.

SQLite virtual-table behavior to account for:

- A virtual table looks like a SQL table, but module callbacks serve reads and
  writes instead of reading ordinary table storage.
- `CREATE VIRTUAL TABLE name USING module(args...)` creates a module-backed
  table.
- Modules can expose hidden columns and table-valued function style arguments.
- SQLite's module API has an optimization negotiation step (`xBestIndex`) that
  accepts constraints, order requirements, and estimated cost.
- Virtual tables can be read-only or writable, depending on module support.
- FTS5 and R-Tree are modules, not hard-coded SQL grammar.

## Existing MongrelDB Hooks

Relevant current pieces:

- `Database::commit_transaction` is already the authoritative multi-table
  commit path for declarative constraints. Trigger enforcement belongs near
  this layer.
- `crates/mongreldb-query/src/commands.rs` already parses SQL DDL/DML into
  `PendingSqlOp` and routes transactions through `Database`.
- Stored procedures already define a durable, validated, declarative IR for
  read/write routines. Trigger actions should reuse or generalize this rather
  than inventing a second opaque execution language.
- DataFusion integration already uses `TableProvider` for base tables and table
  functions (`json_each`, `json_tree`, etc.). External table modules should
  integrate here.
- The catalog already stores procedures. It should grow versioned entries for
  triggers and external tables.

## Trigger Architecture

### Catalog Model

Add catalog entries under `mongreldb-core`:

```rust
pub struct TriggerEntry {
    pub name: String,
    pub target: TriggerTarget,
    pub timing: TriggerTiming,
    pub event: TriggerEvent,
    pub update_of: Vec<String>,
    pub when: Option<TriggerExpr>,
    pub program: TriggerProgram,
    pub created_epoch: u64,
    pub enabled: bool,
}

pub enum TriggerTarget {
    Table(String),
    View(String),
}

pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}
```

The catalog should maintain:

- trigger name uniqueness within a database namespace;
- target back-references for fast lookup by table/view and event;
- stable creation order, used as trigger firing order;
- dependency metadata for `DROP TABLE`, `DROP VIEW`, `ALTER TABLE RENAME`, and
  `ALTER TABLE RENAME COLUMN`.

### Trigger Program IR

Trigger bodies should compile to an IR, not remain raw SQL strings. Reuse the
stored-procedure model where possible:

```rust
pub struct TriggerProgram {
    pub steps: Vec<TriggerStep>,
}

pub enum TriggerStep {
    Insert { table: String, cells: Vec<TriggerCell>, conflict: TriggerConflict },
    Update { table: String, predicate: TriggerPredicate, cells: Vec<TriggerCell> },
    Delete { table: String, predicate: TriggerPredicate },
    Select { query: TriggerQuery, sink: TriggerSelectSink },
    Raise { action: TriggerRaiseAction, message: TriggerValue },
}
```

`TriggerValue` should support:

- literals;
- `NEW.column` and `OLD.column`;
- input parameters from the outer write context;
- step outputs if we intentionally allow them;
- scalar expressions using the same expression evaluator as CHECK constraints
  and SQL DML predicates.

This points to one shared expression subsystem:

- `ExprIr` for scalar predicates and projections;
- evaluators over `RowImage` (`old`, `new`, table schema);
- function registry hooks for deterministic scalar functions;
- explicit typing and null semantics.

### Firing Pipeline

Triggers must fire from the engine commit path, not only from SQL DML. Proposed
commit pipeline for `Database::commit_transaction`:

1. Normalize staged writes into per-row write intents.
2. Load affected old rows under the transaction read snapshot.
3. Build `WriteEvent` values:

```rust
pub struct WriteEvent {
    pub table: String,
    pub row_id: Option<RowId>,
    pub kind: TriggerEvent,
    pub old: Option<RowImage>,
    pub new: Option<RowImage>,
    pub changed_columns: Vec<u16>,
    pub origin: WriteOrigin,
}
```

4. Run `BEFORE` triggers in creation order.
5. Apply resulting write-intent rewrites to the pending transaction.
6. Expand `AFTER` triggers into additional pending writes.
7. Repeat expansion for triggered writes if recursion is enabled.
8. Validate declarative constraints against the final pending write set.
9. Commit once through the WAL and sequencer.

The expansion should be modeled as a single `TriggerExpansion` loop before the
durable commit:

```text
initial writes
  -> expand BEFORE triggers
  -> expand base writes
  -> expand AFTER triggers
  -> repeat for triggered writes if recursion enabled
  -> validate constraints
  -> durable commit once
```

This preserves atomicity: either the original statement and all triggered work
commit together, or none of it commits.

### Recursion and Reentrancy

Add a `TriggerRuntime`:

```rust
pub struct TriggerRuntime {
    pub recursive_triggers: bool,
    pub max_depth: u32,
    pub stack: Vec<TriggerFrame>,
}
```

Default policy:

- `recursive_triggers = false` initially to match current PRAGMA behavior.
- `max_depth` defaults to a conservative value, e.g. 32.
- self-recursion is blocked unless recursive triggers are enabled.
- cycles across tables are detected and reported with a clear trigger stack.

`PRAGMA recursive_triggers = 1` flips the runtime setting for cataloged
database writes.

### BEFORE, AFTER, and INSTEAD OF Semantics

Recommended phased semantics:

- Phase 1: `AFTER` triggers on ordinary tables.
- Phase 2: deterministic `BEFORE` triggers on ordinary tables.
- Phase 3: `INSTEAD OF` triggers on views.

`AFTER` triggers are safest because the row image is final and they avoid
SQLite's documented `BEFORE` ambiguity.

For `BEFORE` triggers, do not emulate undefined behavior. Define:

- `BEFORE INSERT`: may modify `NEW` through explicit `SET NEW.column = expr`
  IR or by returning a replacement row.
- `BEFORE UPDATE`: may modify `NEW`; may not delete the same row directly from
  within the trigger unless recursion is enabled and the behavior is defined as
  a replacement operation.
- `BEFORE DELETE`: may abort or write other rows; cannot make `OLD` mutable.

SQLite SQL syntax does not have `SET NEW.column`; SQLite users normally perform
side-effecting statements instead. MongrelDB can support SQLite-compatible
side-effecting bodies, but the internal IR should still distinguish row-image
mutation from independent writes.

For `INSTEAD OF` triggers:

- only views are valid targets;
- base write is suppressed;
- trigger program is responsible for translating view writes to base table
  writes;
- `changes()` compatibility should follow the documented SQL layer policy,
  while engine-level write counts should include actual base writes.

### RAISE and Error Mapping

Add `RAISE()` as trigger-only expression support:

- `RAISE(ABORT, msg)` -> constraint-style error, rollback current statement.
- `RAISE(FAIL, msg)` -> statement error after preserving prior row effects only
  if the engine can express SQLite's partial-failure behavior safely; otherwise
  map to `ABORT` and document the stricter atomicity.
- `RAISE(ROLLBACK, msg)` -> transaction rollback.
- `RAISE(IGNORE)` -> skip remaining trigger program and current row operation.

MongrelDB should prefer atomic, predictable behavior over SQLite partial-effect
quirks where they conflict with WAL transaction semantics.

### DDL and Introspection

SQL commands:

- `CREATE TRIGGER [IF NOT EXISTS] name ...`
- `DROP TRIGGER [IF EXISTS] name`
- `PRAGMA recursive_triggers`
- `PRAGMA trigger_list` as a MongrelDB-compatible extension, or expose triggers
  through a future schema catalog view.

Catalog maintenance:

- dropping a target table drops its triggers;
- renaming a table rewrites trigger target metadata;
- renaming a column rewrites `update_of`, `NEW`/`OLD` references, and trigger
  expression IR, or rejects the rename if safe rewrite is not possible.

### API Surface

Rust:

```rust
db.create_trigger(trigger: TriggerEntry) -> Result<TriggerEntry>;
db.drop_trigger(name: &str) -> Result<()>;
db.triggers() -> Vec<TriggerEntry>;
db.set_trigger_config(config: TriggerConfig);
```

HTTP/Kit:

- expose trigger catalog CRUD under explicit admin endpoints;
- include trigger failures in typed Kit error envelopes;
- add idempotency behavior for trigger DDL.

NAPI:

- mirror procedure catalog APIs: `createTrigger`, `dropTrigger`, `triggers`.

### Tests

Minimum tests:

- `AFTER INSERT`, `AFTER UPDATE OF`, `AFTER DELETE` table triggers.
- `WHEN` predicates with `NEW`/`OLD` values.
- trigger write atomicity on success and error.
- interaction with unique/FK/CHECK constraints.
- recursion disabled by default.
- recursion enabled with max-depth protection.
- trigger ordering by creation epoch.
- `DROP TABLE` removes target triggers.
- `ALTER TABLE RENAME` keeps triggers attached.
- SQL, Rust, HTTP, and NAPI write paths all fire cataloged triggers.
- crash recovery: no partially applied trigger writes after restart.

## External Table Module Architecture

Feature name: external table modules. SQL compatibility syntax can continue to
say `CREATE VIRTUAL TABLE`, but the internal abstraction should not assume
SQLite's C API.

### Module Registry

Add a module registry owned by the query/database layer:

```rust
pub trait ExternalTableModule: Send + Sync {
    fn name(&self) -> &str;
    fn capabilities(&self) -> ModuleCapabilities;
    fn create(&self, ctx: ModuleCreateCtx, args: ModuleArgs) -> Result<ExternalTableSpec>;
    fn connect(&self, ctx: ModuleConnectCtx, spec: &ExternalTableSpec) -> Result<Arc<dyn ExternalTable>>;
    fn destroy(&self, ctx: ModuleConnectCtx, spec: &ExternalTableSpec) -> Result<()>;
}

pub trait ExternalTable: Send + Sync {
    fn schema(&self) -> SchemaRef;
    fn plan(&self, request: ExternalPlanRequest) -> Result<ExternalPlan>;
    fn scan(&self, plan: ExternalPlan, snapshot: Snapshot) -> Result<SendableRecordBatchStream>;
    fn write(&self, op: ExternalWriteOp, txn: &mut ExternalTxn) -> Result<ExternalWriteResult>;
}
```

The registry should support:

- built-in modules compiled into MongrelDB;
- application-provided Rust modules registered on a session/database builder;
- daemon-allowed modules configured at startup;
- no arbitrary SQL `load_extension` by default.

### Catalog Model

Add external table catalog entries:

```rust
pub struct ExternalTableEntry {
    pub name: String,
    pub module: String,
    pub args: Vec<ModuleArg>,
    pub declared_schema: ExternalSchema,
    pub hidden_columns: Vec<String>,
    pub options: serde_json::Value,
    pub capabilities: ModuleCapabilities,
    pub created_epoch: u64,
}
```

Persist entries in the same catalog generation as tables/procedures/triggers.
Module-owned durable state should live under:

```text
_vtab/<table_name>/
```

The module owns this directory but must use engine-provided file APIs so
encryption, checksums, backup, GC, and crash recovery remain coherent.

### Planner Contract

SQLite's `xBestIndex` maps naturally to a typed planner negotiation:

```rust
pub struct ExternalPlanRequest {
    pub projected_columns: Vec<ColumnId>,
    pub filters: Vec<ExternalFilter>,
    pub order_by: Vec<ExternalOrder>,
    pub limit: Option<usize>,
}

pub struct ExternalPlan {
    pub accepted_filters: Vec<AcceptedFilter>,
    pub residual_filters_required: bool,
    pub order_satisfied: bool,
    pub estimated_rows: Option<u64>,
    pub estimated_cost: f64,
    pub opaque: Arc<dyn Any + Send + Sync>,
}
```

Rules:

- accepted filters must be marked exact or inexact;
- inexact filters are re-applied by DataFusion;
- modules can require hidden-column arguments for table-valued function style
  calls;
- modules can declare whether output ordering satisfies `ORDER BY`;
- modules can push down `LIMIT`/`OFFSET` only when semantics are exact.

This is the same idea as DataFusion `TableProvider` filter pushdown, but with a
module-owned plan object and richer cost/order metadata. The stable module
surface should use typed `ExternalFilter` values; raw DataFusion expressions are
available only as an escape hatch for module-specific function syntaxes such as
FTS and R-Tree predicates.

### DataFusion Integration

Implement a generic `ExternalTableProvider`:

- translates DataFusion projection/filter/limit requests into
  `ExternalPlanRequest`; order pushdown remains module plan metadata until the
  DataFusion provider API exposes ordering requests in `ScanArgs`;
- exposes module schemas, including hidden columns where appropriate;
- exposes module-owned index metadata through an optional module hook, so
  `PRAGMA index_list` / `index_info` / `index_xinfo` do not need per-module
  command-layer special cases;
- caches immutable module plans when safe;
- emits `TableProviderFilterPushDown::Exact`, `Inexact`, or `Unsupported`;
- records query trace fields like `ScanMode::ExternalModule`.

Existing JSON table functions share the module row builder, and cataloged
`json_each` / `json_tree` variants run through the same external table module
abstraction. New table-valued functions should follow that path instead of
creating a separate virtual-table system.

### Write Semantics

Modules can be:

- read-only;
- insert-only;
- fully writable;
- derived/index modules maintained from base-table changes.

Writable modules need an `ExternalTxn` interface:

```rust
pub trait ExternalTxn {
    fn put_state(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    fn delete_state(&mut self, key: &[u8]) -> Result<()>;
    fn read_state(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn emit_base_write(&mut self, op: PendingWrite) -> Result<()>;
}
```

Durability must be atomic with the surrounding MongrelDB transaction. If module
state is stored outside normal tables, the WAL needs module-state frames or a
transactional state table under the hood.

### Built-In Module Roadmap

Start with modules that exercise the architecture without high write risk:

1. `series`: eponymous-only read-only generator for planner and hidden-column
   arguments.
2. `json_each` / `json_tree`: migrate existing table functions to modules.
3. `dbstat` / `schema_tables`: read-only catalog/statistics modules.
4. `fts`: FTS5-inspired text module backed by MongrelDB FM-index and/or sparse
   retrieval. This should be a module over existing index primitives, not a
   separate text engine bolted into SQL.
5. `rtree`: spatial range module. Initial support can map rectangles to
   multi-column range filters; later support can add a dedicated spatial index.

FTS and R-Tree should validate the extension design, but they should not be the
first implementation target.

### FTS/R-Tree Compatibility Surface

`fts_docs` intentionally implements the common FTS table shape without copying
the FTS5 auxiliary-function surface wholesale:

- supported columns: `doc_id`, `text`, hidden `query`, computed `rank`,
  `snippet`, and `highlight`;
- supported query entry points: `query = '...'`, `table MATCH '...'`,
  `alias MATCH '...'`, and `text MATCH '...'`;
- supported query syntax: terms, implicit `AND`, explicit `OR`, `NOT`, quoted
  phrases, and `*` suffix prefix terms when prefix queries are enabled;
- supported options: `tokenizer=simple|ascii|unicode61`, `prefix` /
  `prefix_queries`, `case_sensitive`, `min_token_len`, and `stopwords` with
  `|` separators;
- intentionally different: ranking/snippet/highlight are module-computed result
  columns, not SQLite FTS5 auxiliary functions such as `bm25(...)`, and advanced
  FTS5 grammar is out of scope for the first module contract.

`rtree_rects` implements the spatial compatibility slice as rectangle overlap:

- supported columns: `id`, `min_x`, `max_x`, `min_y`, `max_y`, plus hidden query
  bounds `query_min_x`, `query_max_x`, `query_min_y`, and `query_max_y`;
- supported query entry points: hidden bound equality constraints or
  `rtree_intersects(min_x, max_x, min_y, max_y, qmin_x, qmax_x, qmin_y, qmax_y)`;
- intentionally different: the module is backed by MongrelDB module-owned
  candidate indexes and exact overlap validation, not SQLite's R-Tree file
  format or extension ABI.

### Security and Deployment

Module loading policy:

- built-ins are always safe to register;
- application modules require explicit Rust/daemon registration;
- no dynamic shared-library loading through SQL by default;
- modules declare whether they are deterministic, read-only, trusted, and
  allowed in triggers.

For daemon deployments:

- config lists enabled modules;
- module state directories are scoped to the database root;
- external filesystem/network access is denied unless the module is explicitly
  trusted and configured.

### DDL and Introspection

SQL:

- `CREATE VIRTUAL TABLE [IF NOT EXISTS] name USING module(args...)`
- `DROP TABLE name` should call module destroy hooks when the table is external.
- eponymous module calls: `SELECT * FROM module(args...)`.

PRAGMAs/catalog:

- `PRAGMA table_list` marks external tables distinctly.
- `PRAGMA table_xinfo` includes hidden columns.
- `PRAGMA module_list` returns registered modules.
- `PRAGMA index_list`, `index_info`, and `index_xinfo` return module-owned
  index metadata only when the module exposes it through `ExternalTableModule`.

### Tests

Minimum tests:

- create/connect/drop external table lifecycle;
- hidden columns and eponymous table calls;
- filter pushdown exact/inexact behavior;
- projection pushdown;
- order and limit pushdown;
- module errors are surfaced as typed query errors;
- read-only modules reject writes;
- writable module state is atomic with database commits;
- crash recovery for module state;
- `PRAGMA table_list`, `table_xinfo`, and `module_list` coverage;
- DataFusion joins between base tables and external tables;
- trigger interaction: whether a module is allowed as trigger target or trigger
  source is capability-gated.

## Interaction Between Triggers and External Modules

The systems should share extension infrastructure:

- catalog storage and versioning;
- expression IR;
- capability flags;
- transaction context;
- security policy;
- SQL DDL interception;
- introspection.

Important rules:

- triggers on ordinary tables can read external tables only if the module is
  marked trigger-safe;
- triggers may write external tables only if the module is writable and
  transaction-safe, and the transaction is run with the external-trigger bridge
  that turns trigger-produced external DML into module `ExternalTxn` state before
  WAL commit;
- external modules should not fire triggers unless they emit base-table writes
  through the normal transaction path;
- derived modules such as FTS should subscribe to base-table changes through a
  future changefeed/index-maintenance hook, not through user-visible triggers.

## Current Implementation

The current implementation covers the first executable slice:

- durable catalog entries for trigger programs and external table modules;
- SQL `CREATE TRIGGER`, `CREATE TRIGGER IF NOT EXISTS`, and `DROP TRIGGER` for
  row-level `BEFORE` and `AFTER` table triggers;
- trigger IR compiled from supported SQL trigger bodies, with `NEW.column` and
  `OLD.column` references;
- deterministic `BEFORE` row replacement through the internal `SetNew` trigger
  IR step for catalog/API callers;
- engine-side trigger expansion in `Database::commit_transaction`, so SQL,
  Rust, HTTP, and NAPI transaction writes share the same enforcement point;
- trigger catalog CRUD over the HTTP daemon, typed Rust client, and NAPI
  local/remote bindings, all normalizing definitions through the core trigger
  validator before storage; HTTP trigger DDL accepts body or `Idempotency-Key`
  keys, the typed Rust client exposes idempotent trigger DDL helpers, remote
  NAPI trigger specs can carry the same key, and successful create/replace/drop
  responses replay through the shared `_idem` store;
- atomic trigger-produced writes before WAL commit and declarative constraint
  validation, with recovery coverage proving committed trigger side effects
  survive reopen, rejected trigger batches leave no partial rows behind, and
  trigger-produced rows participate in unique, foreign-key, and CHECK
  validation before commit; trigger runtime failures raised during `/kit/txn`
  map to typed `TRIGGER_VALIDATION` Kit/client error envelopes;
- bounded recursive trigger execution, disabled by default and enabled through
  `PRAGMA recursive_triggers = 1`, with explicit trigger-stack diagnostics for
  detected cycles and max-depth failures;
- stable trigger firing order by catalog creation position, with replacement
  preserving the original trigger position rather than moving it behind later
  triggers;
- SQL trigger introspection through `PRAGMA trigger_list`;
- trigger dependency maintenance for dropped/renamed tables, dropped session
  views, and renamed `UPDATE OF` columns, including trigger checksum refresh on
  catalog rewrites;
- cataloged `INSTEAD OF INSERT`/`UPDATE`/`DELETE` triggers on session views
  with explicit view column aliases; SQL `UPDATE`/`DELETE` routing materializes
  view rows through DataFusion, so joined and computed view queries can feed
  `OLD`/`NEW` trigger images before translated base-table writes commit
  atomically;
- `RAISE(ABORT|FAIL|ROLLBACK, message)` and `RAISE(IGNORE)` support through
  `SELECT RAISE(...)` trigger body statements; `FAIL` and `ROLLBACK` use
  MongrelDB's stricter atomic abort semantics rather than SQLite-style partial
  row effects, while `RAISE(IGNORE)` preserves any trigger side effects already
  staged, suppresses the current row operation, and skips later triggers for
  that row event;
- `CREATE VIRTUAL TABLE ... USING series(...)`, cataloged external table
  metadata, a session-owned module registry layer, a public
  `ExternalTableModule` / `ExternalTable` API for app-provided modules, a
  trait-backed generic DataFusion provider path, module capability descriptors,
  read-only DML rejection, module lifecycle destroy hooks on `DROP TABLE`,
  typed schema errors for unregistered modules and invalid module arguments,
  module-visible projection pushdown, shared exact `LIMIT` truncation in the
  generic provider scan path, exact simple filter pushdown for cataloged
  `series` through typed `ExternalFilter` values, deterministic read-only
  module plan caching in the generic provider, an enriched `ExternalPlan`
  contract that derives accepted filter
  metadata, residual-filter requirements, order satisfaction, estimates, and an
  opaque extension slot, `ScanMode::ExternalModule` query tracing for cataloged
  external tables and module-backed table-valued functions, `PRAGMA table_list`
  / `table_xinfo` / `module_list` visibility, and the eponymous `series(...)`
  table-valued function;
- app-provided read-only module registration through `MongrelSession`, including
  `open_with_external_modules(...)` for reconnecting cataloged virtual tables
  whose module implementation is supplied by the application, and DataFusion
  joins between base tables and app-provided external tables through the generic
  provider path;
- public app-provided writable module callbacks through `ExternalWriteOp`,
  `ExternalWriteResult`, and `ExternalTxn`, with opaque key/value module state
  committed atomically through the existing shared-WAL external-state frames;
- daemon embedding support for startup module allowlists through
  `build_app_with_external_modules(...)`, so `/sql` sessions can reconnect
  cataloged app-backed virtual tables without enabling SQL dynamic loading;
- cataloged `json_each` / `json_tree` / `jsonb_each` / `jsonb_tree` modules for
  literal JSON arguments, sharing the same row builder as the existing
  eponymous table-valued functions and declaring hidden `json` / `root`
  argument columns;
- read-only `schema_tables` and `dbstat` catalog/stat modules backed by a
  context-bearing module connect path, so modules can serve live database
  metadata without special SQL parser hooks;
- a first writable module, `kv_store`, using the same module registry/provider
  path plus durable `_vtab/<table>/state.json` row state, SQL
  `INSERT`/`UPDATE`/`DELETE` routing, primary-key validation, provider refresh
  after writes, and state reload on reopened sessions;
- shared-WAL `ExternalTableState` frames for opaque module state payloads, with
  recovery replay back into `_vtab/<table>/state.json` before external providers
  reconnect;
- explicit SQL transaction staging for writable external tables, including
  multiple staged module DML statements against the same external table and
  mixed base-table plus module-state commits under one WAL `TxnCommit`;
- first FTS/spatial modules on the shared module API: writable `fts_docs`
  exposes `doc_id`, `text`, hidden `query` token search pushdown, module options
  for tokenizer selection, prefix queries, case sensitivity, minimum token
  length, and stopwords, richer query parsing for `AND`/`OR`/`NOT`, quoted
  phrases, and prefix terms, a sparse-style module-owned inverted token
  accelerator for candidate narrowing before exact evaluation, and
  module-computed `rank` / `snippet` / `highlight` result columns; SQL
  compatibility rewrites common
  `fts_docs MATCH 'query'`,
  `alias MATCH 'query'`, and `text MATCH 'query'` forms into the module's
  hidden query constraint; writable `rtree_rects` exposes rectangle bounds,
  hidden query rectangle bounds, an exact `rtree_intersects(...)` SQL function
  for overlap pushdown, and a module-owned spatial range index that intersects
  bound candidates before exact rectangle validation;
- module-owned index metadata is surfaced through the shared
  `ExternalTableModule::indexes` hook and generic PRAGMA handling, with built-in
  FTS/RTree metadata and app-registered module coverage;
- capability-gated trigger validation for external modules: trigger-safe
  external tables can be referenced by API-level trigger read steps, while
  trigger writes to read-only or non-transaction-safe external tables fail with
  explicit capability errors. Writable transaction-safe external trigger
  targets run through a neutral `ExternalTriggerBridge`: `mongreldb-core`
  collects evaluated trigger DML without depending on query modules, and
  `mongreldb-query` resolves it through the registered `ExternalTableModule`
  into module state plus any emitted base writes before the shared WAL commit;
- `_vtab` lifecycle integration: backup copies module state as part of the
  database root, shared-WAL recovery restores committed module state,
  `check()` warns about orphan external-table state entries, `gc()` reclaims
  orphan `_vtab` entries that no external table catalog entry references, and
  `doctor()` leaves warning-only external-state orphans for `gc()` rather than
  quarantining unrelated base tables.

No FTS/spatial module acceleration item remains intentionally deferred in this
phase; post-spec work is compatibility polish and benchmark-driven index
tuning.

## Implementation Phases

### Phase 1: Shared Extension Foundations

- Add catalog entries for triggers and external tables.
- Add expression IR shared by CHECK constraints, procedure predicates, trigger
  predicates, and module hidden-column constraints.
- Add capability flags and registry types.
- Add test fixtures for catalog durability and schema evolution.

### Phase 2: Table-Level AFTER Triggers

- Parse `CREATE TRIGGER` and `DROP TRIGGER`.
- Compile `WHEN`, `NEW`/`OLD`, and body statements into trigger IR.
- Fire `AFTER INSERT/UPDATE/DELETE` from `Database::commit_transaction`.
- Support `RAISE(ABORT, ...)` and `RAISE(IGNORE)`.
- Keep `recursive_triggers = 0`.

### Phase 3: View Routing and Remaining Trigger Semantics

- Extend SQL syntax/API coverage for `BEFORE` row mutation where needed.
- Add view write routing through `INSTEAD OF` triggers. `UPDATE`/`DELETE` view
  routing now materializes `OLD` rows through DataFusion; `INSERT` keeps using
  the incoming value image directly.
- Recursive trigger diagnostics report explicit trigger stacks for detected
  cycles and max-depth failures.
- Expand SQL and API introspection.

### Phase 4: External Table Registry and Read-Only Modules

- Add `ExternalTableModule` and generic DataFusion provider.
- Implement `series`, `json_each`, `json_tree`, and catalog/stat modules.
- Add `CREATE VIRTUAL TABLE` DDL for read-only modules where it makes sense.
- Expose explicit app-provided read-only module registration on
  `MongrelSession`, including reopen-time registration for persisted virtual
  table catalog entries.

### Phase 5: Writable and Durable Modules

- Add module-state WAL integration. The first durable writable module now
  commits opaque state payloads through the shared WAL, and explicit SQL
  transactions stage external state alongside ordinary base-table writes before
  committing both through one shared-WAL transaction.
- Add writable module API and transaction tests. App-provided modules can now
  implement write callbacks over `ExternalTxn` key/value state while built-ins
  can continue using row-replacement helpers.
- Add backup/GC/check/doctor integration for `_vtab/`.
- Keep external-module writes from trigger programs behind an explicit bridge
  rather than binding `mongreldb-core` directly to query-layer module execution.

### Phase 6: FTS and Spatial Modules

- Build an FTS module over MongrelDB text/sparse primitives. The first
  `fts_docs` module is implemented with token containment through hidden-column
  pushdown, common `MATCH` compatibility rewrites, and module-computed rank,
  snippet, and highlight columns; tokenizer/query options now cover the first
  compatibility slice, and a sparse-style inverted token accelerator narrows
  candidate rows before exact module evaluation.
- Build an R-Tree-style spatial module over range indexes first, then add a
  dedicated spatial index if benchmarks justify it. The first `rtree_rects`
  module is implemented with rectangle overlap through hidden-bound pushdown
  and the `rtree_intersects(...)` SQL function, backed by a module-owned spatial
  range index for candidate narrowing.
- Add compatibility docs for FTS5/R-Tree syntax that is supported versus
  intentionally different.
- The trigger-to-external-module execution bridge receives evaluated trigger
  DML, runs the target module's `ExternalTxn` callback, returns external state
  plus any emitted base writes, and feeds both back into the single WAL
  transaction. Plain core transactions without a bridge reject triggered
  external writes with an explicit bridge-required error.

## Open Design Decisions

- Whether triggers should fire for standalone `Table` handles or only for
  cataloged `Database` tables. Recommendation: only cataloged `Database`
  tables initially, because cross-table atomicity and catalog lookup require
  `Database`.
- Whether `BEFORE` trigger row mutation should be exposed through SQLite-like
  side-effect statements only or through a clearer MongrelDB trigger IR. The IR
  should support mutation explicitly even if SQL syntax compiles from
  side-effecting statements.
- Whether to expose statement-level triggers as a MongrelDB extension. SQLite
  does not support them, so this should wait.
- Whether daemon-hosted app-provided modules should be configured only through a
  typed admin API in addition to the current startup allowlist. In-process
  sessions and embedded daemon routers currently register app modules
  explicitly, while SQL dynamic loading remains disabled.
- How much additional FTS5 syntax to emulate beyond the common `MATCH` query
  surface. Tokenizers and ranking options should go through module options.

## References

- SQLite `CREATE TRIGGER`: https://sqlite.org/lang_createtrigger.html
- SQLite virtual table mechanism: https://sqlite.org/vtab.html
- SQLite `CREATE VIRTUAL TABLE`: https://sqlite.org/lang_createvtab.html
- SQLite FTS5: https://sqlite.org/fts5.html
- SQLite R-Tree: https://sqlite.org/rtree.html
