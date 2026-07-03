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

`batches` is a `Vec<RecordBatch>` — Arrow's in-memory columnar format. Each
batch holds up to 65,536 rows.

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
| `col = 'value'` | Bitmap or PK | Exact — returns only matching rows |
| `col IN ('a', 'b', 'c')` | Bitmap union | Exact |
| `col > 5`, `col BETWEEN 1 AND 10` | PGM learned index / page prune | Exact |
| `col LIKE '%text%'` | FM-index | Returns a superset (DataFusion re-checks) |
| `ann_search(col, '[0.1, 0.2, ...]', 10)` | HNSW | Exact (custom UDF) |
| `sparse_match(col, 'query text', 10)` | Sparse inverted index | Exact (custom UDF) |

Conditions that MongrelDB can't push down (complex expressions, OR logic)
are handled by DataFusion's own filter after the scan. This is always correct
— pushdown only makes things faster, never wrong.

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

## Result Caching

Repeated SQL queries return instantly from cache. The cache is keyed by
`(SQL string, epoch)`, so any `commit()` that bumps the epoch invalidates
stale results automatically:

```rust
// First run — actually executes the query
let r1 = session.run("SELECT count(*) FROM users WHERE score > 90").await?;
// ~7 ms

// Second run — cache hit, returns pre-computed result
let r2 = session.run("SELECT count(*) FROM users WHERE score > 90").await?;
// ~0.1 µs

// After a write + commit, cache is invalidated
db.put(new_row).unwrap();
db.commit().unwrap();

// Third run — cache miss, re-executes
let r3 = session.run("SELECT count(*) FROM users WHERE score > 90").await?;
// ~7 ms
```

To force a cold run (bypass cache):

```rust
session.clear_cache();
```

## Materialized Views

Register a named SQL query as a view. `SELECT * FROM <view_name>` is
transparently rewritten to run the view's defining SQL:

```rust
session.create_view(
    "active_users",
    "SELECT * FROM users WHERE status = 'active'"
);

// This runs the view's SQL:
let batches = session.run("SELECT count(*) FROM active_users").await?;
```

Views are invalidated on commit just like regular cached queries.

## Column Statistics

For unfiltered scans of insert-only tables, MongrelDB provides exact per-column
min/max/null_count statistics. DataFusion uses these to answer `MIN(col)`,
`MAX(col)`, and `COUNT(*)` without scanning any data.
