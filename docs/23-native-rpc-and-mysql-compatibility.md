# Native RPC and MySQL compatibility

MongrelDB exposes one canonical SQL/session runtime through native gRPC,
HTTP/JSON, Kit, and the MySQL compatibility listener. Native gRPC uses
Protobuf control messages, Arrow IPC record batches, multiplexed HTTP/2, and
TLS 1.3 only.

## Native listener

Start HTTP and native listeners together:

```bash
mongreldb-server /var/lib/mongreldb/app \
  --port 8453 \
  --native-port 8454 \
  --tls-cert /etc/mongreldb/server.pem \
  --tls-key /etc/mongreldb/server-key.pem
```

Use `--tls-client-ca` to require client certificates. Catalog password
authentication uses SCRAM-SHA-256. Native service-token records are loaded
with `--service-tokens`. OIDC requires `--oidc-issuer`,
`--oidc-audience`, and one or more `--oidc-allow-host` values. OIDC discovery,
JWKS, and embedding-provider endpoints must use verified HTTPS.
`--session-idle-timeout` also bounds idle native connections.

The Rust native client is `mongreldb_client::native::NativeClient`. It pools
connections, multiplexes requests, supports sessions and prepared statements,
streams Arrow batches with backpressure, exposes cancel/status operations, and
retries only safe reads or idempotent writes.

## MySQL listener

`mongreldb-mysql-wire` is a TLS 1.3 sidecar over the native client:

```bash
mongreldb-mysql-wire \
  --listen 127.0.0.1:3307 \
  --tls-cert /etc/mongreldb/mysql.pem \
  --tls-key /etc/mongreldb/mysql-key.pem \
  --native-endpoint https://127.0.0.1:8454 \
  --native-domain localhost \
  --native-ca /etc/mongreldb/ca.pem \
  --database-id 00112233445566778899aabbccddeeff \
  --database-name app
```

Plaintext authentication is rejected. The listener uses the
`caching_sha2_password` challenge proof, then opens a normal native session.
Queries, transactions, prepared statements, Arrow result streaming, and
`KILL QUERY` use the canonical runtime.

## Online MySQL migration

`mongreldb-migrate-mysql` connects to a CA-verified TLS MySQL source. It
introspects columns, primary/unique/foreign keys, charsets, collations,
generated columns, and triggers. The runtime performs a consistent snapshot,
stable primary-key cursor copy, row-binlog catch-up, persisted file/position
checkpoints, count/checksum validation, locked cutover, and guarded rollback.

The public entry point is:

```rust
mongreldb_migrate_mysql::migrate(
    &source,
    &native_session,
    &checkpoint_store,
    &options,
    &execution_control,
    publish_cutover,
).await?;
```

The source must enable row binlogs with full row images. The migration
principal needs replication and client access. `MigrationOptions::schema_only`
stops after target DDL. A resumed run validates source identity and schema
hash before using its checkpoint.

## Qualification

CI tests migration against a real TLS MySQL 8.4 container and tests the wire
listener through the external `mysql_async` driver. The clean qualification
job also runs protocol fuzz targets, durable crash tests, and conformance
against the exact packaged server and C ABI. See
[implementation status](architecture/implementation-status.md) for the
exact-SHA evidence state.
