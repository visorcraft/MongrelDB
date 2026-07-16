# Stored Procedures

Stored procedures are durable, database-managed routines stored in the MongrelDB catalog.
Procedure bodies use a declarative JSON IR over existing MongrelDB primitives: native queries,
typed writes, and SQL calls through the query layer. They do not embed arbitrary scripting runtimes.

## SQL

```sql
CREATE PROCEDURE read_users AS JSON '<procedure-json>';
CREATE OR REPLACE PROCEDURE read_users AS JSON '<procedure-json>';
SHOW PROCEDURES;
DESCRIBE PROCEDURE read_users;
CALL read_users(JSON '{"status":"active"}');
DROP PROCEDURE read_users;
```

`CALL` returns one `result_json` UTF-8 column. Read-write procedures commit through the same
transaction path as ordinary writes, so constraints and write-write conflicts are enforced
atomically.

## HTTP

- `GET /procedures`
- `GET /procedures/{name}`
- `POST /procedures`
- `PUT /procedures/{name}`
- `DELETE /procedures/{name}`
- `POST /procedures/{name}/call`
- `POST /kit/procedures/{name}/call`

The Kit call route accepts `{ "args": { ... }, "idempotency_key": "..." }` and returns
`{ "status": "ok", "committed": <bool>, "epoch": <optional>, "epoch_text": <optional>, "result": ... }`.
`committed` is true exactly when both exact epoch fields are present. A keyed call uses
the daemon's durable owner/operation/payload-bound idempotency store. A retry
replays the exact prior HTTP status and body. An uncertain commit retains its
intent and returns `QUERY_OUTCOME_UNKNOWN` without calling the procedure again.
Before receipt lookup, the daemon rechecks current `ALL` permission. The key
also binds the security version and exact procedure creation epoch, update
epoch, version, and checksum. Permission revocation or procedure replacement
cannot reveal or replay a stale result.

## NAPI

`Database` exposes `createProcedure`, `createOrReplaceProcedure`, `dropProcedure`,
`procedures`, `procedure`, `callProcedure`, and `callProcedureAsync`. Procedure specs and args are
JSON-backed for ABI stability.

## Limits

Stored procedures v1 are straight-line routines. They do not support loops, recursion, nested
procedure calls, DDL during execution, or dynamic table/column identifiers.
