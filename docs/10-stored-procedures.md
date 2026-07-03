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
`{ "status": "ok", "epoch": <optional>, "result": ... }`.

## NAPI

`Database` exposes `createProcedure`, `createOrReplaceProcedure`, `dropProcedure`,
`procedures`, `procedure`, `callProcedure`, and `callProcedureAsync`. Procedure specs and args are
JSON-backed for ABI stability.

## Limits

Stored procedures v1 are straight-line routines. They do not support loops, recursion, nested
procedure calls, DDL during execution, or dynamic table/column identifiers.
