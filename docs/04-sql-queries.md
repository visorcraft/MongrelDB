# SQL Queries

MongrelDB includes a SQL frontend powered by [DataFusion](https://arrow.apache.org/datafusion/)
(version 54). You write standard SQL, and MongrelDB translates `WHERE` clauses
into index-backed lookups whenever possible.

## Setting Up a SQL Session

You need both `mongreldb-core` and `mongreldb-query`:

```rust
use mongreldb_core::Db;
use mongreldb_query::MongrelSession;

let mut db = Db::create("./mydb", schema, 1)?;
db.bulk_load(rows)?;

let session = MongrelSession::new(db);

// Register the table under a name so SQL can reference it
session.register("users").await?;
```

For joins, register multiple tables:

```rust
session.register_db("orders", orders_db).await?;
```

## Running SQL

```rust
let batches = session.run("SELECT * FROM users WHERE score > 90").await?;
```

`batches` is a `Vec<RecordBatch>` - Arrow's in-memory columnar format. Each
batch holds up to 65,536 rows.

### Multi-statement execution

Multiple SQL statements separated by semicolons can be executed in a single
`run()` call. Each statement is executed sequentially; the result of the last
statement is returned. DDL/DML statements return empty result sets.

```sql
-- Execute DDL + DML + SELECT in one call.
CREATE TABLE temp AS SELECT * FROM orders WHERE amount > 100;
INSERT INTO temp (id, amount) VALUES (999, 500.0);
SELECT count(*) FROM temp;
```

### SELECT

```sql
-- columns
SELECT id, email FROM users

-- count
SELECT count(*) FROM users

-- with WHERE
SELECT * FROM users WHERE score >= 90.0 AND score < 100.0

-- GROUP BY
SELECT email_domain, count(*) FROM users GROUP BY email_domain

-- ORDER BY
SELECT * FROM users ORDER BY score DESC

-- LIMIT
SELECT * FROM users LIMIT 10

-- JOIN
SELECT u.name, o.total
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE o.total > 100.0
```

## How WHERE Clauses Are Accelerated

MongrelDB inspects your `WHERE` clause and translates it into index lookups
before any data is scanned. This is called **predicate pushdown**:

| SQL pattern | Index used | Behavior |
|---|---|---|
| `col = 'value'` | Bitmap or PK | Exact - returns only matching rows |
| `col IN ('a', 'b', 'c')` | Bitmap union | Exact |
| `col > 5`, `col BETWEEN 1 AND 10` | PGM learned index / page prune | Exact |
| `col LIKE '%text%'` | FM-index | Returns a superset (DataFusion re-checks) |
| `ann_search(col, '[0.1, 0.2, ...]', 10)` | HNSW | Exact (custom UDF) |
| `sparse_match(col, 'query text', 10)` | Sparse inverted index | Exact (custom UDF) |

Conditions that MongrelDB can't push down (complex expressions, OR logic)
are handled by DataFusion's own filter after the scan. This is always correct
- pushdown only makes things faster, never wrong.

## Special UDFs

MongrelDB registers two SQL user-defined functions for AI-native queries:

### ANN Search (Vector Similarity)

```sql
-- Find the 10 rows whose embedding column is closest to the query vector
SELECT * FROM docs WHERE ann_search(embedding, '[0.12, 0.45, 0.78, ...]', 10)
```

### Sparse Match (Text Relevance)

```sql
-- SPLADE-style sparse retrieval
SELECT * FROM docs WHERE sparse_match(content, 'machine learning database', 10)
```

MongrelDB also registers **Extended SQL Functions** for date/time, JSON,
string, math, and application-defined function hooks. See
[Extended SQL Functions](11-extended-sql-functions.md).

For schema introspection, maintenance commands, and planner inspection through
SQL, see [Operational SQL Commands](12-operational-sql-commands.md).

## Result Caching

Repeated SQL queries return instantly from cache. The cache is keyed by
`(SQL string, epoch)`, so any `commit()` that bumps the epoch invalidates
stale results automatically:

```rust
// First run - actually executes the query
let r1 = session.run("SELECT count(*) FROM users WHERE score > 90").await?;
// ~7 ms

// Second run - cache hit, returns pre-computed result
let r2 = session.run("SELECT count(*) FROM users WHERE score > 90").await?;
// ~0.1 µs

// After a write + commit, cache is invalidated
db.put(new_row).unwrap();
db.commit().unwrap();

// Third run - cache miss, re-executes
let r3 = session.run("SELECT count(*) FROM users WHERE score > 90").await?;
// ~7 ms
```

To force a cold run (bypass cache):

```rust
session.clear_cache();
```

## Views (CREATE VIEW)

Register a named SQL query as a view. `SELECT * FROM <view_name>` is
transparently rewritten to run the view's defining SQL. Views can be created
either through the Rust session API or with standard `CREATE VIEW` / `DROP VIEW`
SQL:

```rust
// Rust API:
session.create_view(
    "active_users",
    "SELECT * FROM users WHERE status = 'active'"
);

// This runs the view's SQL:
let batches = session.run("SELECT count(*) FROM active_users").await?;
```

```sql
-- SQL (works in any client that runs SQL: the daemon, the NAPI addon's
-- db.sql(), the Kit's Database::sql(), etc.):
CREATE VIEW active_users AS SELECT id, email FROM users WHERE status = 'active';
SELECT count(*) FROM active_users;
DROP VIEW IF EXISTS active_users;
```

Views are invalidated on commit just like regular cached queries.

> **Views are session-scoped, not persisted.** A view lives in the `MongrelSession`
> that created it - it is not written to the catalog or WAL. The NAPI addon's
> `Database` and the Kit's `Database` cache one session for the database's
> lifetime, so a view created via `db.sql("CREATE VIEW ...")` persists across
> subsequent `sql()` calls on the same handle. Closing and reopening the database
> starts a fresh session (re-apply any view-defining migrations then). The daemon
> opens a fresh session per `/sql` HTTP request, so views do **not** persist
> across daemon calls - define them in a long-lived application process instead.

### Node.js example

```javascript
import { tableFromIPC } from 'apache-arrow';

// Create the view, then query it in a second sql() call - both hit the same
// cached session on the Database handle.
await db.sql('CREATE VIEW vip AS SELECT id, email FROM users WHERE score >= 90');
const vips = tableFromIPC(await db.sql('SELECT * FROM vip ORDER BY id'));
```

## Materialized Views (CREATE MATERIALIZED VIEW)

A materialized view physically materializes its defining query as a real
table - the data is stored, not computed on read. This is useful for
expensive aggregations or joins that are queried frequently.

```sql
-- Create a materialized view that stores a pre-computed summary.
CREATE MATERIALIZED VIEW daily_totals AS
  SELECT date_trunc('day', created_at) AS day, SUM(amount) AS total
  FROM orders GROUP BY day;

-- Query it like a regular table - no recomputation.
SELECT * FROM daily_totals WHERE day > '2026-01-01';
```

Materialized views are physically tables - they occupy storage and support
indexes. The defining SQL is stored so the view can be refreshed
(re-materialized) in a future release. To drop one, use `DROP TABLE` or
`DROP MATERIALIZED VIEW`.

**Views vs. Materialized Views:** regular `CREATE VIEW` stores only the SQL
definition and recomputes on every read (session-scoped); `CREATE MATERIALIZED
VIEW` stores the result rows and reads from the materialized table.

## CREATE TABLE AS SELECT

Create a new table and populate it from a query in one statement. The schema
(column names and types) is inferred from the query result.

```sql
CREATE TABLE high_value_orders AS
  SELECT * FROM orders WHERE amount > 1000;

-- The new table is a regular table - add indexes, query it, etc.
SELECT count(*) FROM high_value_orders;
```

The first column of the query becomes the primary key of the new table; all
other columns are nullable. To customize the schema, create the table
explicitly and `INSERT INTO ... SELECT`.

## Column Statistics

For unfiltered scans of insert-only tables, MongrelDB provides exact per-column
min/max/null_count statistics. DataFusion uses these to answer `MIN(col)`,
`MAX(col)`, and `COUNT(*)` without scanning any data.

## Recursive CTEs (`WITH RECURSIVE`)

MongrelDB supports `WITH RECURSIVE` for tree traversal, graph queries, and
hierarchical aggregation - the full DataFusion 54 recursive-CTE engine:

```sql
WITH RECURSIVE tree AS (
    SELECT id, parent, 0 AS depth FROM nodes WHERE parent IS NULL
    UNION ALL
    SELECT n.id, n.parent, t.depth + 1
    FROM nodes n JOIN tree t ON n.parent = t.id
)
SELECT id, depth FROM tree ORDER BY id;
```

## Window Functions (`OVER` / `PARTITION BY`)

Standard SQL window functions are supported natively via DataFusion - `ROW_NUMBER()`,
`RANK()`, `DENSE_RANK()`, `LAG()`, `LEAD()`, `FIRST_VALUE()`, `LAST_VALUE()`,
`NTILE()`, `PERCENT_RANK()`, `CUME_DIST()`, and aggregate windows (`SUM() OVER`,
`AVG() OVER` with optional `ROWS`/`RANGE` frames):

```sql
SELECT id, category, amount,
       ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS rank,
       SUM(amount) OVER (PARTITION BY category) AS category_total
FROM orders ORDER BY category, rank;
```

## `EXPLAIN` / `EXPLAIN QUERY PLAN`

`EXPLAIN <sql>` and `EXPLAIN QUERY PLAN <sql>` return the DataFusion logical +
physical plan, annotated with MongrelDB-specific scan-mode details (direct
dispatch, index pushdown, external module, result-cache hit):
