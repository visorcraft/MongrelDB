# Extended SQL Functions

MongrelDB registers an Extended SQL Functions layer on every `MongrelSession`.
These functions run inside DataFusion SQL and cover application-oriented
date/time, JSON, string, math, and custom-function use cases.

## Date and Time

```sql
SELECT date('2024-02-03 04:05:06');
SELECT time('2024-02-03 04:05:06');
SELECT datetime('2024-02-03 04:05:06', '+1 day', 'start of day');
SELECT unixepoch('1970-01-02 00:00:00');
SELECT julianday('1970-01-01 00:00:00');
SELECT strftime('%Y/%m/%d %H:%M:%S', '2024-02-03 04:05:06');
SELECT timediff('2024-02-03 04:05:08', '2024-02-03 04:05:06');
```

Supported functions:

| Function | Return | Notes |
|---|---|---|
| `date(value, modifiers...)` | text | `YYYY-MM-DD`; no value means `now` |
| `time(value, modifiers...)` | text | `HH:MM:SS`; no value means `now` |
| `datetime(value, modifiers...)` | text | `YYYY-MM-DD HH:MM:SS`; no value means `now` |
| `julianday(value, modifiers...)` | float | Julian day number |
| `unixepoch(value, modifiers...)` | int | Unix timestamp seconds |
| `strftime(format, value, modifiers...)` | text | Supports `%Y`, `%m`, `%d`, `%H`, `%M`, `%S`, `%f`, `%s`, `%J`, `%%` |
| `timediff(lhs, rhs)` | float | Seconds between timestamps |

Recognized modifiers are `+/-N day`, `+/-N hour`, `+/-N minute`,
`+/-N second`, `start of day`, and `unixepoch`.

Date/time functions are treated as volatile because they can depend on `now`.
Queries that call them bypass the session result cache.

## JSON

```sql
SELECT json_valid('{"a":[1,2]}');
SELECT json_extract('{"a":[1,{"b":"x"}]}', '$.a[1].b');
SELECT json_type('{"a":[1]}', '$.a');
SELECT json_array_length('{"a":[1,2,3]}', '$.a');
SELECT json_array(1, 'x', NULL);
SELECT json_object('a', 1, 'b', 'x');
SELECT json_patch('{"a":1,"b":2}', '{"b":null,"c":3}');
SELECT json_array_insert('[1,3]', '$[1]', 2);
SELECT json_set('{"a":1}', '$.b', 2);
SELECT json_insert('{"a":1}', '$.a', 9);
SELECT json_replace('{"a":1}', '$.a', 9);
SELECT json_remove('{"a":1,"b":2}', '$.a');
SELECT json_error_position('{"a":');
SELECT json_pretty('{"a":[1,2]}');
SELECT json_quote('a''b');
SELECT key, value, type FROM json_each('[10,{"a":1}]');
SELECT fullkey, atom FROM json_tree('{"a":[1,{"b":2}]}');
SELECT atom FROM jsonb_tree('{"a":[1]}') WHERE fullkey = '$.a[0]';
SELECT value FROM series(1, 3);
```

Supported functions:

| Function | Return | Notes |
|---|---|---|
| `json(value)` | text | Parses and normalizes JSON text |
| `json_valid(value)` | int | `1` for valid JSON, `0` otherwise |
| `json_extract(json, path...)` | text | Scalar results return scalar text; object/array results return JSON text |
| `json_type(json, path)` | text | `null`, `true`, `false`, `integer`, `real`, `text`, `array`, or `object` |
| `json_array_length(json, path)` | int | Non-arrays return `0` |
| `json_quote(value)` | text | Converts a SQL value to JSON text |
| `json_array(value...)` | text | Builds a JSON array |
| `json_object(key, value...)` | text | Builds a JSON object from alternating key/value arguments |
| `json_patch(json, patch)` | text | Applies RFC 7396-style merge patch semantics |
| `json_pretty(json)` | text | Pretty-prints normalized JSON |
| `json_error_position(value)` | int | `0` for valid JSON, otherwise the parser error column |
| `json_array_insert(json, path, value...)` | text | Inserts into arrays at numeric path indexes |
| `json_set(json, path, value...)` | text | Sets or creates values |
| `json_insert(json, path, value...)` | text | Inserts only when the path is absent |
| `json_replace(json, path, value...)` | text | Replaces only when the path is present |
| `json_remove(json, path...)` | text | Removes object keys or array indexes |
| `json_each(json, root)` | table | Expands a top-level object or array into rows |
| `json_tree(json, root)` | table | Recursively expands a JSON value into rows with parent ids |
| `jsonb...` aliases | text/table | Compatibility aliases backed by MongrelDB's canonical JSON text model |
| `series(stop)` / `series(start, stop, step)` | table | Generates inclusive integer series rows in a `value` column |

JSON paths support root `$`, object keys with `.key`, and array indexes with
`[0]`, for example `$.items[0].name`. Bracket-quoted object keys are supported
for keys that contain punctuation, for example `$["weird.key"]`.

`json_each` and `json_tree` return `key`, `value`, `type`, `atom`, `id`,
`parent`, `fullkey`, and `path`. The `json` argument and optional `root`
argument must be SQL string literals.

## String and Math

```sql
SELECT instr('abcdef', 'cd');
SELECT quote('a''b');
SELECT hex('Az');
SELECT unhex('417A');
SELECT printf('hi %s %d', 'x', 7);
SELECT format('%s-%d', 'n', 2);
SELECT char(65, 66);
SELECT abs(-7), coalesce(NULL, 5, 3), max(1, 5, 3), min(1, 5, 3);
SELECT concat('a', NULL, 'b'), concat_ws('-', 'a', NULL, 'b');
SELECT like('a_%', 'Abc'), soundex('Robert');
SELECT mod(7, 3);
SELECT pow(2, 3), power(2, 3), sqrt(9), log(2, 8);
SELECT random();
SELECT changes(), total_changes(), last_insert_rowid();
SELECT randomblob(16);
SELECT zeroblob(16);
SELECT glob('a*', 'abcdef');
SELECT length('hello');
SELECT octet_length('hello');
SELECT replace('abcabc', 'ab', 'X');
SELECT round(12.345, 2);
SELECT sign(-9);
SELECT substr('abcdef', 2, 3);
SELECT typeof(1.5);
SELECT unicode('Az'), unistr('A\u0042'), unistr_quote('a\b');
SELECT lower('AZ'), upper('az'), trim('  x  ');
SELECT likely(score > 90), ifnull(NULL, 'fallback'), iif(1, 'yes', 'no');
SELECT mongreldb_compileoption_get(0), mongreldb_compileoption_used('WAL');
```

Supported functions:

| Function | Return | Notes |
|---|---|---|
| `instr(haystack, needle)` | int | 1-based character position, or `0` when absent |
| `quote(value)` | text | SQL literal text |
| `hex(value)` | text | Uppercase hexadecimal |
| `unhex(value)` | text | Hex bytes decoded as UTF-8 |
| `printf(format, args...)` | text | Supports common flags, width, precision, `%s`, `%z`, `%q`, `%Q`, `%d`, `%i`, `%u`, `%x`, `%X`, `%o`, `%c`, `%f`, `%e`, `%E`, `%g`, `%G`, and `%%` |
| `format(format, args...)` | text | Alias for `printf` |
| `char(codepoint...)` | text | Unicode code points |
| `abs(value)` | same numeric family | Absolute value |
| `max(value...)` / `min(value...)` | same as selected input | Scalar multi-argument extrema; aggregate `MAX()`/`MIN()` remain available for one-argument aggregate calls |
| `concat(value...)` | text | Concatenates non-null values |
| `concat_ws(separator, value...)` | text | Concatenates non-null values with a separator; null separator returns `NULL` |
| `like(pattern, value, escape)` | int | Function form of SQL LIKE; optional escape character |
| `mod(lhs, rhs)` | float | Remainder; divide by zero returns `NULL` |
| `pow(lhs, rhs)` / `power(lhs, rhs)` | float | Power |
| `pi()` | float | Mathematical pi |
| `acos`, `acosh`, `asin`, `asinh`, `atan`, `atan2`, `atanh` | float | Trigonometric functions; domain errors return `NULL` |
| `ceil` / `ceiling`, `floor`, `round` | float | Rounding functions |
| `cos`, `cosh`, `sin`, `sinh`, `tan`, `tanh` | float | Trigonometric functions |
| `degrees`, `radians`, `exp`, `ln`, `log`, `log10`, `log2`, `sqrt` | float | Common math functions |
| `random()` | int | Volatile `i64`; bypasses the session result cache |
| `changes()` / `total_changes()` / `last_insert_rowid()` | int | Per-session DML counters for SQL statements routed through `MongrelSession` |
| `randomblob(n)` | bytes | Volatile random bytes; bypasses the session result cache |
| `zeroblob(n)` | bytes | `n` zero bytes |
| `glob(pattern, value)` | int | `*`, `?`, bracket classes, negated classes, and ranges |
| `length(value)` | int | Character length for text, byte length for binary |
| `octet_length(value)` | int | UTF-8 byte length |
| `replace(value, from, to)` | text | String replacement |
| `round(value, digits)` | float | Rounded floating-point value |
| `sign(value)` | int | `-1`, `0`, or `1` |
| `substr(value, start, length)` / `substring(...)` | text | 1-based substring extraction |
| `typeof(value)` | text | `null`, `integer`, `real`, `text`, or `blob` |
| `unicode(value)` | int | First Unicode code point |
| `unistr(value)` / `unistr_quote(value)` | text | Unicode escape decoding and escaped SQL literal helper |
| `lower(value)` / `upper(value)` | text | ASCII/Unicode case conversion |
| `ltrim(value)` / `rtrim(value)` / `trim(value)` | text | Whitespace trimming |
| `likely(value)` / `unlikely(value)` / `likelihood(value, p)` | same as input | Planner hint compatibility; value is returned unchanged |
| `ifnull(lhs, rhs)` | same as selected input | First non-null argument |
| `iif(cond, then, else)` / `if(...)` | same as selected branch | Conditional expression helper |
| `nullif(lhs, rhs)` | same as `lhs` | Returns `NULL` when arguments compare equal |
| `soundex(value)` | text | Four-character Soundex code |
| `mongreldb_compileoption_get(n)` / `mongreldb_compileoption_used(name)` | text/int | Compatibility accessors for MongrelDB SQL capability flags |
| `mongreldb_offset(value)` | int | Returns `NULL`; MongrelDB does not expose page offsets through SQL |
| `mongreldb_version()` / `mongreldb_source_id()` | text | Compatibility accessors returning MongrelDB query-layer version/source text |
| `load_extension(...)` | error | Explicitly disabled |

## Aggregate and Window Functions

MongrelDB uses DataFusion for built-in aggregate and window execution, with
compatibility rewrites for aliases whose surface differs from DataFusion SQL:

| Function | Return | Notes |
|---|---|---|
| `count`, `sum`, `avg`, `min`, `max` | aggregate-dependent | Standard aggregate functions; one-argument `min`/`max` remain aggregates, multi-argument calls are scalar extrema |
| `group_concat(value)` | text | Alias for `string_agg(value, ',')`; skips null values |
| `group_concat(value, separator)` | text | Alias for `string_agg(value, separator)` |
| `string_agg(value, separator)` | text | DataFusion aggregate; exposed through the compatibility profile |
| `total(value)` | float | Alias for a floating-point `sum` that returns `0.0` when no non-null inputs are present |
| `median(value)` | float | Exact median over non-null numeric inputs |
| `percentile(value, p)` | float | Exact continuous percentile; `p` is `0..100` and must be constant within the aggregate |
| `percentile_cont(value, p)` | float | Exact continuous percentile; `p` is `0..1` |
| `percentile_disc(value, p)` | float | Exact discrete percentile; `p` is `0..1` and returns an input value |
| `row_number`, `rank`, `dense_rank`, `lag`, `lead` | window-dependent | Standard window functions through DataFusion |

The aggregate/window parity suite compares grouped aggregates, distinct and
filtered aggregates, `group_concat`, `total`, ranking windows, offset windows,
and running-frame windows against SQLite on a shared corpus. The percentile
family follows SQLite's percentile-extension rules and is covered by direct
expected-value tests because SQLite builds often omit that extension.

## Full-Text Search

| Function | Returns | Description |
|---|---|---|
| `mongreldb_fts_rank(text, query)` | Float64 | BM25-inspired relevance score for `text` against whitespace-tokenized `query`. Higher = more relevant. |

```sql
-- Rank rows by relevance to a query, returning the most relevant first.
SELECT id, title, mongreldb_fts_rank(content, 'database performance') AS score
FROM articles
ORDER BY score DESC
LIMIT 10;
```

For accurate full-text search with a maintained inverted index and global IDF,
use the `fts_docs` virtual table module (see
[Extended SQL & virtual tables](extended-sql-and-virtual-tables.md)).

## Custom Functions

Rust applications can register custom DataFusion functions on a session:

```rust
session.register_scalar_udf(my_scalar_udf);
session.register_aggregate_udf(my_aggregate_udf);
session.register_window_udf(my_window_udf);
```

Each registration clears the session result and plan caches because function
resolution can change query output without advancing the storage epoch.

Built-in DataFusion aggregate and window functions remain available through the
normal SQL planner. MongrelDB's registration hooks are for application-defined
extensions that need to live alongside the built-in function set.

## Full Profile Status

Implemented beyond the MVP: recursive `json_tree`, jsonb compatibility aliases,
bracket-quoted JSON paths, broader `printf`/`format` flags, complete core GLOB
character classes, Unicode case conversion, per-session DML counters, the common
math function set, long-tail core scalar compatibility functions, and
SQLite-reference aggregate/window parity coverage, including exact
percentile-family aggregates. Exact edge-case alignment is covered by focused
function tests plus the SQLite-reference aggregate/window corpus and remains the
compatibility contract for future additions.
