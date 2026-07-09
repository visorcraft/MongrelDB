# Security

This document describes the security properties of MongrelDB and how to
report vulnerabilities.

## Encryption at rest

MongrelDB supports optional page-level encryption using AES-256-GCM
(enabled with the `encryption` Cargo feature). When enabled:

- Sorted-run page payloads (`.sr` files) are encrypted.
- WAL segments (`_wal/`) are encrypted (frame-level AES-256-GCM).
- Result cache files (`_rcache/`) are encrypted.
- Encryption keys are derived from a user-supplied passphrase via
  Argon2id + HKDF-SHA256. The passphrase is the sole secret.
- Key material in memory is wrapped in `Zeroizing` buffers and wiped
  on drop.

### Unencrypted components

- Run headers and structural metadata (needed to open files)
- Manifest, schema files, index checkpoints
- Result cache files (`_rcache/`)
- Arrow IPC shadow files (`_shadow/`)

## Daemon security (mongreldb-server)

The optional HTTP daemon (`mongreldb-server`) has these properties:

- Binds to `127.0.0.1` only - not accessible from other machines.
- **No authentication** - any local process can query, write, or
  delete data.
- No TLS - traffic is plaintext on the loopback interface.
- No rate limiting or request size caps.

For remote access or multi-tenant environments, place a reverse proxy
(nginx, Caddy) in front with TLS termination and authentication. Do
not expose the daemon directly to a network.

## Input validation

- SQL queries are parsed by DataFusion 54, which applies its own input
  validation and parameterization.
- The native Condition API accepts typed parameters (column IDs, value
  bytes, numeric ranges) - no string interpolation, no injection surface.
- Bulk-load paths accept typed buffers (`NativeColumn`) - invalid
  buffer lengths are rejected by the `validate()` method on
  deserialization.

## Dependency security

MongrelDB's direct dependencies:

| Dependency | Version | Role |
|---|---|---|
| `aes-gcm` | 0.10 | AES-256-GCM encryption |
| `argon2` | 0.5 | Passphrase key derivation |
| `zstd` | 0.13 | Column compression |
| `roaring` | 0.10 | Bitmap indexes |
| `crossbeam-skiplist` | 0.1 | Memtable / HOT index |
| `datafusion` | 54 | SQL engine |
| `arrow` | 58 | Columnar in-memory format |

All are widely used, actively maintained, and MIT or Apache-2.0
licensed. Report dependency vulnerabilities through GitHub's Dependabot
alerts or the private vulnerability reporting flow below.

## Reporting a vulnerability

**Do not file a public GitHub issue, discussion, or pull request for
security problems.** Report privately through **GitHub's private
vulnerability reporting**:

1. Go to the repository's **Security** tab.
2. Click **Report a vulnerability**.
3. Fill in the advisory form with the details below.

This keeps the report confidential between you and the maintainers
until a fix is ready. Please include as much as you can:

- a description of the issue and its impact,
- step-by-step reproduction steps,
- the MongrelDB version, OS, and Rust version,
- the relevant configuration, error output, or a proof-of-concept,
- a suggested fix or mitigation, if you have one.

### What to expect

- **Acknowledgement** of your report within a few days.
- An initial assessment and, where confirmed, a remediation plan.
- Progress updates through the private advisory thread until the
  issue is resolved.
- Credit for your responsible disclosure in the advisory, unless you
  prefer to remain anonymous.

We ask that you give us a reasonable opportunity to ship a fix before
any public disclosure.
