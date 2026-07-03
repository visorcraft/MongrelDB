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
SELECT json_set('{"a":1}', '$.b', 2);
SELECT json_insert('{"a":1}', '$.a', 9);
SELECT json_remove('{"a":1,"b":2}', '$.a');
SELECT json_quote('a''b');
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
| `json_set(json, path, value...)` | text | Sets or creates values |
| `json_insert(json, path, value...)` | text | Inserts only when the path is absent |
| `json_remove(json, path...)` | text | Removes object keys or array indexes |

JSON paths support root `$`, object keys with `.key`, and array indexes with
`[0]`, for example `$.items[0].name`.

## String and Math

```sql
SELECT instr('abcdef', 'cd');
SELECT quote('a''b');
SELECT hex('Az');
SELECT unhex('417A');
SELECT printf('hi %s %d', 'x', 7);
SELECT format('%s-%d', 'n', 2);
SELECT char(65, 66);
SELECT mod(7, 3);
SELECT pow(2, 3);
SELECT random();
```

Supported functions:

| Function | Return | Notes |
|---|---|---|
| `instr(haystack, needle)` | int | 1-based character position, or `0` when absent |
| `quote(value)` | text | SQL literal text |
| `hex(value)` | text | Uppercase hexadecimal |
| `unhex(value)` | text | Hex bytes decoded as UTF-8 |
| `printf(format, args...)` | text | Supports `%s`, `%q`, `%Q`, `%d`, `%i`, `%f`, `%g`, and `%%` |
| `format(format, args...)` | text | Alias for `printf` |
| `char(codepoint...)` | text | Unicode code points |
| `mod(lhs, rhs)` | float | Remainder; divide by zero returns `NULL` |
| `pow(lhs, rhs)` | float | Power |
| `random()` | int | Volatile `i64`; bypasses the session result cache |

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
