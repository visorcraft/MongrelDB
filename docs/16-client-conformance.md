# Client Conformance Matrix

Every official MongrelDB language client must verify this behavior matrix
against a running `mongreldb-server` in CI. The matrix defines the minimum
round-trip operations that prove a client is wire-compatible.

Index and AI schema shapes also require exact offline wire tests. Dynamic
clients may use native maps/lists. Typed clients must expose typed structures
or a documented complete-JSON escape hatch.

## Required operations

| # | Operation | What it verifies |
|---|---|---|
| 1 | **Health** | Connect to the daemon and confirm it responds to `/health` |
| 2 | **Create table** | `POST /kit/create_table` with typed columns (int64 PK, varchar, float64) |
| 3 | **Put** | `POST /kit/txn` with a `put` op; verify the daemon commits |
| 4 | **Count** | `GET /tables/{name}/count` returns the correct integer |
| 5 | **Query by PK** | `POST /kit/query` with a `pk` condition; verify the row is returned |
| 6 | **Query by range** | `POST /kit/query` with a `range` condition (lo/hi bounds); verify filtered results |
| 7 | **Upsert** | `POST /kit/txn` with an `upsert` op on an existing PK; verify update, not duplicate |
| 8 | **Transaction** | `POST /kit/txn` with multiple staged ops in one batch; verify atomic commit |
| 9 | **Delete by PK** | `POST /kit/txn` with a `delete_by_pk` op; verify the row is removed |
| 10 | **SQL** | `POST /sql` with `INSERT INTO ...`; verify side effects via count |
| 11 | **Table names** | `GET /tables` returns a list including the created table |
| 12 | **Schema** | `GET /kit/schema/{name}` returns the column descriptors |
| 13 | **Error: not found** | Requesting a nonexistent table returns the typed 404 error |
| 14 | **Idempotency** | `POST /kit/txn` with an `idempotency_key`; retry returns the same result |

## Required index and AI wire shapes

| Surface | Required coverage |
|---|---|
| Schema indexes | `bitmap`, `fm_index`, `ann`, `learned_range`, `minhash`, and `sparse` |
| Index options | Partial `predicate`; ANN `m`, `ef_construction`, `ef_search`, and `quantization`; MinHash `permutations` and `bands`; learned-range `epsilon` |
| Dense ANN | `quantization: "dense"`, which selects full-precision cosine distance |
| Embedding source | Application-supplied vectors plus portable `configured_model` and `generated_column_spec` metadata |
| Complex writes | Dense vector arrays, sparse `[token_id, weight]` pairs, and MinHash member arrays |
| Query conditions | `bitmap_in`, `fm_contains_all`, `ann`, `sparse_match`, `minhash_similar`, and `minhash_similar_members`, in addition to the live baseline above |

Embedding generation is separate from ANN quantization. Any client may supply
vectors from any model. Automatic generated columns resolve the stored
`provider_id` through server configuration. Clients never upload executable
model code to the daemon.

## Test requirements

Each live test suite must:

1. Use a **unique table name** per test (e.g. suffix with a timestamp or UUID)
   so parallel runs don't conflict.
2. **Assert actual values** in query results, not just row counts - a broken
   query that returns all rows should fail the range test.
3. Run against a **fresh daemon** (no pre-existing data) to avoid state
   pollution between tests.
4. Be **skippable** when no daemon is reachable (for local offline runs),
   but CI must always run the full live suite against a real server.

## Shared schema

All live tests use the same column layout so results are comparable across
languages:

| Column ID | Name | Type | Primary key | Nullable |
|---|---|---|---|---|
| 1 | `id` | `int64` | yes | no |
| 2 | `name` / `label` / `customer` | `varchar` | no | no |
| 3 | `amount` / `score` | `float64` | no | no |

## Error contract

The cross-language error contract is the stable `ErrorCategory` taxonomy from
`mongreldb-types`: `MongrelError::category()` maps every engine error onto one
of twenty categories, each with a numeric code (`code()`, 1-20) that is never
reused and a retry class (`retry_class()`). Clients must key error handling
off the category or its code, never the message text - messages are diagnostic
and may change between releases. See
[Architecture Foundations](18-architecture-foundations.md#stable-error-taxonomy)
for the full contract.
