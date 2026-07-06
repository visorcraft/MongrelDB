# Users, Roles & Permissions

MongrelDB ships a catalog-stored authentication system: **users** with
Argon2id-hashed passwords, **roles** that group permissions, and a
`GRANT`/`REVOKE` model for table-level access control. The same model is
exposed through four surfaces — the embedded Rust API, the Node.js NAPI
addon, the Python binding, and the HTTP daemon — and through SQL DDL
(`CREATE USER`, `GRANT`, …).

Users and roles live **inside the catalog** alongside tables, procedures, and
triggers. They are invisible to `table_names()` and never collide with your
own table names. Old database files without users deserialize with empty
user/role lists, so enabling auth is always backward-compatible.

> Passwords are hashed with **Argon2id** (19 MiB memory, t=2, p=1 — the same
> OWASP-recommended parameters used for the encryption KEK). Plaintext
> passwords are never stored or logged.

## Concepts

- **User** — an identity with a unique username and an Argon2id password hash.
  An `is_admin` flag short-circuits all permission checks.
- **Role** — a named bundle of permissions. Users belong to zero or more
  roles; their effective permissions are the union of permissions across all
  their roles.
- **Permission** — one of:
  | Permission | Meaning |
  | --- | --- |
  | `All` | Every permission on every table (`GRANT ALL`). |
  | `Admin` | User/role management (`CREATE USER`, `GRANT`, `CREATE ROLE`). |
  | `Ddl` | Schema changes (`CREATE TABLE`, `DROP TABLE`, `ALTER TABLE`). |
  | `Select { table }` | `SELECT` on a specific table. |
  | `Insert { table }` | `INSERT` on a specific table. |
  | `Update { table }` | `UPDATE` on a specific table. |
  | `Delete { table }` | `DELETE` on a specific table. |

  `All` satisfies any required permission; otherwise the permission kinds
  must match exactly (a `Select` does not satisfy an `Insert`). Table names
  are matched literally — `"*"` is a real table name, not a wildcard.

## SQL DDL

The SQL frontend accepts the standard `CREATE USER` / `CREATE ROLE` /
`GRANT` / `REVOKE` vocabulary. Run it from `db.sql(...)`, the daemon `/sql`
endpoint, or the Kit CLI's `sql` command.

```sql
-- Create a login user (password is Argon2id-hashed before storage).
CREATE USER alice WITH PASSWORD 's3cret-pw';

-- Change a password or grant/revoke admin.
ALTER USER alice WITH PASSWORD 'new-pw';
ALTER USER alice ADMIN;

-- Roles bundle permissions.
CREATE ROLE analyst;
GRANT  SELECT ON orders TO analyst;
GRANT  INSERT ON orders TO analyst;
GRANT  ALL    ON audit_log TO analyst;

-- Grant the role to a user.
GRANT analyst TO alice;

-- Show what exists.
SHOW USERS;
SHOW ROLES;

-- Revoke and drop.
REVOKE INSERT ON orders FROM analyst;
REVOKE analyst FROM alice;
DROP ROLE analyst;
DROP USER alice;
```

## Rust (embedded)

```rust
use mongreldb_core::Database;
use mongreldb_core::auth::Permission;

let db = Database::open("./my_database")?;

// Users
db.create_user("alice", "s3cret-pw")?;
db.alter_user_password("alice", "new-pw")?;
assert!(db.verify_user("alice", "new-pw")?.is_some());
db.set_user_admin("alice", true)?;

// Roles + permissions
db.create_role("analyst")?;
db.grant_permission("analyst", Permission::Select { table: "orders".into() })?;
db.grant_permission("analyst", Permission::Insert { table: "orders".into() })?;
db.grant_role("alice", "analyst")?;

// Check access at the application layer.
assert!(db.check_permission("alice", &Permission::Select { table: "orders".into() }));

// Inspect.
for u in db.users() { println!("user: {}", u.username); }
for r in db.roles() { println!("role: {}", r.name); }

// Resolve the full principal (used by the daemon's auth middleware).
let principal = db.resolve_principal("alice").expect("exists");
assert!(principal.has_permission(&Permission::Ddl)); // admin bypass

// Reverse everything.
db.revoke_role("alice", "analyst")?;
db.revoke_permission("analyst", Permission::Insert { table: "orders".into() })?;
db.drop_role("analyst")?;
db.drop_user("alice")?;
```

## Node.js / TypeScript (NAPI addon)

Permission strings use a compact form: `"all"`, `"admin"`, `"ddl"`, or
`"select:table"`, `"insert:table"`, `"update:table"`, `"delete:table"`.

```typescript
import { Database } from '@visorcraft/mongreldb';

const db = Database.open('./my_database');

// Users
db.createUser('alice', 's3cret-pw');
db.alterUserPassword('alice', 'new-pw');
console.log(db.verifyUser('alice', 'new-pw')); // true
db.setUserAdmin('alice', true);
console.log(db.users());                       // ['alice']

// Roles + permissions
db.createRole('analyst');
db.grantPermission('analyst', 'select:orders');
db.grantPermission('analyst', 'insert:orders');
db.grantRole('alice', 'analyst');
console.log(db.roles());                       // ['analyst']

// Reverse
db.revokePermission('analyst', 'insert:orders');
db.revokeRole('alice', 'analyst');
db.dropRole('analyst');
db.dropUser('alice');
```

## Python (maturin extension)

```python
from mongreldb_kit import Database

db = Database.open('./my_database')

# Users
db.create_user('alice', 's3cret-pw')
db.alter_user_password('alice', 'new-pw')
assert db.verify_user('alice', 'new-pw') is True
db.set_user_admin('alice', True)
assert db.users() == ['alice']

# Roles + permissions (same string vocabulary as NAPI)
db.create_role('analyst')
db.grant_permission('analyst', 'select:orders')
db.grant_permission('analyst', 'insert:orders')
db.grant_role('alice', 'analyst')
assert db.roles() == ['analyst']

# Reverse
db.revoke_permission('analyst', 'insert:orders')
db.revoke_role('alice', 'analyst')
db.drop_role('analyst')
db.drop_user('alice')
```

## MongrelDB Kit (Rust / TypeScript / Python)

The Kit's `Database` re-exports `Permission` and forwards every auth method
to the engine, so the snippet above works identically through the Kit's
typed surface. From Rust:

```rust
use mongreldb_kit::{Database, Permission};

let db = Database::open("./my_database")?;
db.create_user("alice", "s3cret-pw")?;
db.grant_permission("analyst", Permission::Select { table: "orders".into() })?;
```

From TypeScript:

```typescript
import { KitDatabase } from '@visorcraft/mongreldb-kit';

const db = KitDatabase.openSync('./data', schema);
db.createUser('alice', 's3cret-pw');
db.grantPermission('analyst', 'select:orders');
```

From Python:

```python
from mongreldb_kit import Database
db = Database.open('./data')
db.create_user('alice', 's3cret-pw')
db.grant_permission('analyst', 'select:orders')
```

## MongrelDB Kit CLI

The `mongreldb-kit` CLI exposes `user` and `role` subcommands that wrap the
same operations:

```sh
# Users
mongreldb-kit user create  ./my_database alice 's3cret-pw'
mongreldb-kit user passwd  ./my_database alice 'new-pw'
mongreldb-kit user verify  ./my_database alice 'new-pw'   # prints ok / invalid
mongreldb-kit user admin   ./my_database alice true       # grant admin
mongreldb-kit user list    ./my_database
mongreldb-kit user drop    ./my_database alice

# Roles + permissions
mongreldb-kit role create ./my_database analyst
mongreldb-kit role allow  ./my_database analyst select:orders
mongreldb-kit role allow  ./my_database analyst insert:orders
mongreldb-kit role grant  ./my_database alice   analyst
mongreldb-kit role list   ./my_database
mongreldb-kit role deny   ./my_database analyst insert:orders
mongreldb-kit role revoke ./my_database alice   analyst
mongreldb-kit role drop   ./my_database analyst
```

## Daemon (HTTP) authentication

`mongreldb-server` supports three auth modes — they can be combined:

1. **Token** (`--auth-token <token>`): every request must carry
   `Authorization: Bearer <token>`.
2. **User** (`--auth-users`): every request must carry
   `Authorization: Basic <base64(user:pass)>` against a catalog user
   (Argon2id-verified). The matching `Principal` is injected into request
   extensions for permission checks.
3. **Both**: token **or** valid user credentials accepted.

```sh
# Start the daemon with both token and user auth.
mongreldb-server ./my_database 8453 --auth-token shared-secret --auth-users
```

Connect with curl using either scheme:

```sh
# Bearer token
curl -H "Authorization: Bearer shared-secret" http://127.0.0.1:8453/health

# Basic auth against a catalog user
curl -u alice:new-pw http://127.0.0.1:8453/health
```

Manage users on a running daemon through the SQL endpoint:

```sh
# Create the first admin user (run before enabling --auth-users in production)
curl -X POST http://127.0.0.1:8453/sql \
  -H "Authorization: Bearer shared-secret" \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE USER alice WITH PASSWORD '\''s3cret-pw'\''; ALTER USER alice ADMIN"}'
```

## Operational notes

- **Bootstrapping the first admin.** A freshly created database has no
  users. Create the first admin either via the embedded API
  (`db.create_user(...)` + `db.set_user_admin(..., true)`), via SQL
  (`CREATE USER …; ALTER USER … ADMIN`), or via the CLI
  (`mongreldb-kit user create … && mongreldb-kit user admin … true`) before
  starting the daemon with `--auth-users`.
- **Lockouts.** Because there is no recovery email flow, treat the first
  admin credentials like a root account. Keep an embedded/CLI path available
  for emergency password resets.
- **Storage.** Users and roles serialize as part of the catalog (the same
  `_meta` blob that stores procedures and triggers); there is no separate
  auth file to back up.
- **Performance.** `verify_user` runs one Argon2id hash comparison (~50 ms
  on typical hardware). The daemon caches the resolved `Principal` for the
  lifetime of the request only — for high-QPS authenticated workloads,
  prefer the Bearer token mode, which is a single string compare.
