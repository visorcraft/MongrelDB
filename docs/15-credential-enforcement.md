# Credential Enforcement (require_auth)

By default, MongrelDB databases are credentialless — you create one and start
reading and writing immediately, like SQLite. **Credential enforcement** is an
opt-in security layer that makes the storage layer require authenticated
credentials for every read, write, DDL, admin, and SQL operation.

When enabled, a database carries `require_auth = true` in its catalog. Every
subsequent open must supply valid username/password credentials, and every
operation is checked against the authenticated principal's permissions. A
stolen database file alone cannot be queried — even with the bytes, an attacker
needs valid credentials to use the MongrelDB API.

> **Not a substitute for encryption.** Credential enforcement is a
> *logical-access* control — it gates the MongrelDB API, not the raw bytes.
> For data-at-rest protection, combine it with MongrelDB's page-level
> AES-256-GCM encryption (`create_encrypted_with_credentials`). See
> [Encryption](07-encryption.md).

## Quick start

### Create a credentialed database

**Rust:**
```rust
use mongreldb_core::Database;

let db = Database::create_with_credentials("./secure_db", "admin", "s3cret-pw")?;
// db is already authenticated as admin — every operation is checked.
db.create_table("orders", schema)?;
```

**Node.js / TypeScript:**
```typescript
import { Database } from '@visorcraft/mongreldb';

const db = Database.createWithCredentials('./secure_db', 'admin', 's3cret-pw');
db.createTable('orders', schema);
```

**Python:**
```python
from mongreldb_kit import Database

db = Database.create_with_credentials('./secure_db', schema, 'admin', 's3cret-pw')
```

**CLI:**
```sh
mongreldb-kit init ./secure_db --require-auth --admin-user admin --admin-password s3cret-pw
```

### Open a credentialed database

**Rust:**
```rust
// Plain open FAILS on a require_auth database:
// Database::open("./secure_db")? → AuthRequired error

let db = Database::open_with_credentials("./secure_db", "admin", "s3cret-pw")?;
```

**Node.js / TypeScript:**
```typescript
const db = Database.openWithCredentials('./secure_db', 'admin', 's3cret-pw');
```

**TypeScript kit (with options):**
```typescript
import { KitDatabase } from '@visorcraft/mongreldb-kit';

const db = KitDatabase.openSync('./secure_db', schema, {
  credentials: { username: 'admin', password: 's3cret-pw' }
});
```

**Python:**
```python
db = Database.open_with_credentials('./secure_db', 'admin', 's3cret-pw')
```

**CLI:**
```sh
mongreldb-kit --user admin --password s3cret-pw check ./secure_db
# or via environment variables:
MONGREL_USER=admin MONGREL_PASSWORD=s3cret-pw mongreldb-kit check ./secure_db
```

### Convert an existing database

If you have an existing credentialless database and want to add enforcement
without recreating it:

```rust
let db = Database::open("./existing_db")?;
db.enable_auth("admin", "s3cret-pw")?;
// The same handle is now authenticated as admin.
// The database can only be reopened via open_with_credentials.
```

**CLI:**
```sh
mongreldb-kit auth enable ./existing_db --admin-user admin --admin-password s3cret-pw
```

### Disable enforcement (recovery)

To revert a credentialed database to credentialless mode:

```rust
let db = Database::open_with_credentials("./secure_db", "admin", "s3cret-pw")?;
db.disable_auth()?;
// The database is now credentialless — plain open works.
```

**CLI:**
```sh
mongreldb-kit auth disable-offline ./secure_db
# WARNING: disabling require_auth on ./secure_db
# This reverts the database to credentialless mode...
# proceed? [y/N] y
# require_auth disabled — database is now credentialless
```

If credentials are lost entirely, the database directory's catalog file can be
edited directly (filesystem access required) — see [Threat model](#threat-model)
below.

## How it works

### The `require_auth` flag

The catalog (the `_meta/CATALOG` blob that stores table schemas, procedures,
triggers, users, and roles) has a `require_auth: bool` field. It defaults to
`false` (backward compatible — old databases open unchanged). When `true`,
every `Database::open` without credentials fails with `AuthRequired`, and every
operation consults the cached principal's permissions.

### The enforcement matrix

When `require_auth` is `true`, every public operation is checked:

| Operation | Required permission |
|---|---|
| `Table::query` / `count` / `scan` | `Select { table }` |
| `Table::put` / `put_batch` | `Insert { table }` |
| `Table::delete` / `truncate` | `Delete { table }` |
| `Transaction::put` / `put_batch` | `Insert { table }` |
| `Transaction::delete` / `delete_many` / `truncate` | `Delete { table }` |
| `Transaction::update_many` | `Update { table }` |
| `Transaction::upsert` (insert) | `Insert { table }` |
| `Transaction::upsert` (update) | `Insert { table }` + `Update { table }` |
| SQL `SELECT` | `Select { table }` |
| SQL `INSERT` | `Insert { table }` |
| SQL `UPDATE` | `Update { table }` + `Select { table }` |
| SQL `DELETE` / `TRUNCATE` | `Delete { table }` + `Select { table }` |
| `create_table` / `drop_table` / `alter` / procedures / triggers | `Ddl` |
| `compact` / `vacuum` / `gc` | `Ddl` |
| `create_user` / `create_role` / `grant` / `revoke` | `Admin` |
| `call_procedure` | `All` (v1; finer control is a future extension) |

**Admin bypass:** a principal with `is_admin = true` short-circuits every
check. The admin user created at `create_with_credentials` / `enable_auth`
time has this flag.

**`All` does not imply `Admin`:** `Permission::All` grants every table-level
and DDL permission but NOT admin (user/role management). Only `is_admin = true`
grants admin. This is a deliberate design decision — see the spec §9.

### The `AuthState` abstraction

The enforcement layer uses an extensible `AuthState` abstraction that bundles
the `require_auth` flag and cached principal into an `Arc`-clonable handle.
This handle is cloned into every mounted `Table`, so the Table layer can
enforce without a reference back to `Database` (avoiding a reference cycle).

The `TableAuthChecker` trait lets the daemon (or any future multi-tenant
layer) provide its own principal source — e.g. one that reads from per-request
state — while the embedded default reads the open-time cached principal.

### Composing with encryption

Credential enforcement and encryption-at-rest are orthogonal:

- **Encryption** protects the bytes on disk (page-level AES-256-GCM).
- **Credential enforcement** protects the logical operations (read/write/DDL).

A database can be both encrypted and credentialed:

```rust
let db = Database::create_encrypted_with_credentials(
    "./secure_db",
    "passphrase",       // encryption
    "admin",            // auth
    "s3cret-pw",
)?;
```

Reopening requires both the passphrase and the credentials:

```rust
let db = Database::open_encrypted_with_credentials(
    "./secure_db",
    "passphrase",
    "admin",
    "s3cret-pw",
)?;
```

## Permissions

See [Users, Roles & Permissions](14-auth.md) for the full permission model.
The key points for enforcement:

| Permission | Satisfies |
|---|---|
| `All` | Every `Select`/`Insert`/`Update`/`Delete`/`Ddl` (but NOT `Admin`) |
| `Admin` | Only `Admin` (user/role management) |
| `Ddl` | Only `Ddl` (schema changes, compaction, procedures, triggers) |
| `Select { table }` | `Select` on that specific table |
| `Insert { table }` | `Insert` on that specific table |
| `Update { table }` | `Update` on that specific table |
| `Delete { table }` | `Delete` on that specific table |

There are no wildcards — `Select { table: "*" }` matches a table literally
named `*`. Grant `All` for full access (minus admin).

## Error types

| Error | HTTP (daemon) | Meaning |
|---|---|---|
| `AuthRequired` | 401 | Database has `require_auth` but was opened without credentials |
| `AuthNotRequired` | 400 | Credentialed constructor used on a credentialless database |
| `InvalidCredentials` | 401 | Wrong username/password |
| `PermissionDenied` | 403 | Principal lacks the required permission |

## Daemon (HTTP) integration

The daemon's HTTP auth middleware (Bearer token or HTTP Basic) runs *before*
the request reaches the storage layer. With credential enforcement, the
storage layer *also* checks permissions — defense in depth:

```sh
# Start the daemon with user auth for a require_auth database.
mongreldb-server ./secure_db 8453 --auth-users

# Each request must carry valid Basic auth:
curl -u alice:alice-pw http://127.0.0.1:8453/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM orders"}'

# If alice lacks Select on orders, the storage layer returns 403 (even though
# the HTTP middleware accepted her credentials).
```

For `require_auth` databases, the daemon must run with `--auth-users` (or
`--auth-users` + `--auth-token`). Token-only mode is insufficient because it
doesn't resolve a catalog `Principal` for per-operation checks.

## Threat model

### What this stops

- An attacker with read access to the database *file path* but **not** the
  credentials cannot query, mutate, or enumerate data through the MongrelDB
  API — even if they copy the bytes. (For encrypted databases, they also
  can't decrypt the bytes without the passphrase.)
- A compromised low-privilege service account cannot escalate beyond its
  granted permissions — the storage layer enforces what the HTTP layer
  asserted.
- Application bugs that forget to check permissions are caught by the storage
  layer anyway.

### What this does NOT stop

- An attacker with raw disk access who parses the catalog/blob format
  directly. Auth enforcement is logical, not cryptographic. Use full-disk
  encryption or HSM-backed keys for that layer.
- An attacker who can write to `_meta/` and call `disable_auth` (or edit the
  catalog file). Filesystem permissions on the database directory are the
  boundary — document this in your operations runbook.
- Brute-force of weak passwords. Argon2id (~50ms/verify) limits online
  guessing to ~20/sec/core. For offline brute-force of a stolen hash, the same
  Argon2id cost applies. v1 does not implement lockout/throttling.

## Offline recovery (lost credentials)

If the admin password is lost:

1. **If you still have filesystem access:** Open the database from a process
   that can read the catalog file, and call `disable_auth()`. The CLI
   provides `auth disable-offline` for this.

2. **If the database is encrypted and you have the passphrase:** Open with
   `open_encrypted` (the passphrase bypasses auth enforcement at the catalog
   level) and call `disable_auth()`.

3. **If all credentials AND the passphrase are lost:** The data is
   unrecoverable (this is the intended behavior for a security feature).

> The offline recovery grants no power an attacker with disk access doesn't
> already have. Auth enforcement is a logical-access control, not a
> cryptographic one.

## API reference

### Rust (`mongreldb-core`)

```rust
// Constructors
Database::create_with_credentials(path, admin_user, admin_pw) -> Result<Self>
Database::open_with_credentials(path, user, pw) -> Result<Self>
Database::create_encrypted_with_credentials(path, passphrase, admin_user, admin_pw) -> Result<Self>
Database::open_encrypted_with_credentials(path, passphrase, user, pw) -> Result<Self>

// Management
Database::enable_auth(&self, admin_user, admin_pw) -> Result<()>
Database::disable_auth(&self) -> Result<()>
Database::require_auth_enabled(&self) -> bool
Database::refresh_principal(&self) -> Result<()>
Database::principal(&self) -> Option<Principal>
```

### TypeScript (`@visorcraft/mongreldb-kit`)

```typescript
KitDatabase.createWithCredentialsSync(path, schema, adminUser, adminPw): KitDatabase
KitDatabase.createEncryptedWithCredentialsSync(path, schema, passphrase, adminUser, adminPw): KitDatabase
KitDatabase.openSync(path, schema, { credentials: { username, password } }): KitDatabase
KitDatabase.openSync(path, schema, {
  encryption: { passphrase },
  credentials: { username, password }
}): KitDatabase

db.enableAuth(adminUser, adminPw): void
db.disableAuth(): void
db.requireAuthEnabled(): boolean
db.refreshPrincipal(): void
```

### Python (`mongreldb-kit`)

```python
Database.create_with_credentials(path, schema, admin_user, admin_pw) -> Database
Database.open_with_credentials(path, username, password) -> Database
Database.create_encrypted_with_credentials(path, schema, passphrase, admin_user, admin_pw) -> Database
Database.open_encrypted_with_credentials(path, passphrase, username, password) -> Database

db.enable_auth(admin_user, admin_pw) -> None
db.disable_auth() -> None
db.require_auth_enabled() -> bool
db.refresh_principal() -> None
```

### CLI (`mongreldb-kit`)

```sh
# Create
mongreldb-kit init <path> --require-auth --admin-user <user> --admin-password <pw>

# Open (global flags)
mongreldb-kit --user <user> --password <pw> <command> <path>
MONGREL_USER=<user> MONGREL_PASSWORD=<pw> mongreldb-kit <command> <path>
echo "<pw>" | mongreldb-kit --user <user> --password-stdin <command> <path>

# Manage
mongreldb-kit auth enable <path> --admin-user <user> --admin-password <pw>
mongreldb-kit auth disable-offline <path> [--passphrase <pw>] [--yes]
```
