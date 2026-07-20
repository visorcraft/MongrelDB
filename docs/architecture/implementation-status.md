# Architecture implementation status

This is the authoritative status for the 0.60.3 architecture audit. A source
module or unit test alone does not make a feature production-ready.

Status meanings:

- **Not Started**: no usable implementation.
- **Scaffolded**: types, helpers, or simulations exist, but no production path.
- **Integrated**: a production path exists, but the release qualification gate
  is incomplete.
- **Qualified**: exact-SHA CI and packaged-artifact evidence passed. This state
  is recorded per R1-R10 task in the generated certification artifact's
  `architecture_tasks` field.

Source rows remain Integrated until evidence exists. The generated manifest
binds this file through `implementation_status_sha256`, so CI fails if the
matrix is incomplete or malformed.

| ID | Status | Source | Tests | CI job | Last qualified SHA | Known limitations |
|---|---|---|---|---|---|---|
| R1 Shared-handle authority | Integrated | `crates/mongreldb-core/src/handle.rs`, `manager.rs`, `database.rs` | `crates/mongreldb-core/tests/shared_handles.rs` | workspace tests |  | Exact-SHA qualification remains required. |
| R2 Native RPC | Integrated | `crates/mongreldb-protocol/proto/`, `src/native_transport.rs`, `crates/mongreldb-server/src/native.rs`, `crates/mongreldb-client/src/native.rs` | `crates/mongreldb-protocol/tests/native_proto.rs`, `native_transport.rs`, `crates/mongreldb-server/tests/native_rpc.rs`, `adapter_parity.rs` | workspace, server, and client tests |  | Exact-SHA packaged qualification remains required. |
| R3 Production security | Integrated | `crates/mongreldb-core/src/security_hardening.rs`, `crates/mongreldb-server/src/oidc.rs`, `native.rs`, `vault_kms.rs` | security module tests, `crates/mongreldb-core/tests/kms_encryption.rs`, and `crates/mongreldb-server/tests/native_rpc.rs` | workspace and server tests |  | Exact-SHA qualification remains required. |
| R4 MySQL migration and wire | Integrated | `crates/mongreldb-migrate-mysql`, `crates/mongreldb-mysql-wire` | real MySQL container and external `mysql_async` client tests | `CI / MySQL wire compatibility`, `Qualification nightly / Real MySQL migration` |  | Exact-SHA qualification remains required. |
| R5 Generated embedding writes | Integrated | `crates/mongreldb-core/src/embedding.rs`, `schema.rs`, `database.rs`, `columnar.rs`; MongrelDB-Kit Rust/TypeScript/Python `GeneratedColumnSpec` schema | `crates/mongreldb-core/tests/generated_embeddings.rs`; MongrelDB-Kit schema and provider tests | workspace tests |  | Synchronous `AbortWrite` is the explicit policy; exact-SHA qualification remains required. |
| R6 Provider hardening | Integrated | `crates/mongreldb-core/src/embedding.rs`, `crates/mongreldb-server/src/remote_embedding.rs` | embedding unit/integration and remote configuration tests | workspace and server tests |  | Exact-SHA qualification remains required. |
| R7 Production certification | Integrated | `crates/mongreldb-core/src/certification.rs`, `fuzz/`, `scripts/generate-certification-manifest.py`, `scripts/qualify-packaged-artifacts.sh` | debug/release workspace tests, macOS/Windows architecture tests, five fuzz targets, kill-at-hook durable crash matrix, packaged server/C ABI conformance | `Qualification nightly / Full tests`, `Qualification nightly / Extended parser fuzz`, `Clean release qualification` |  | No manifest exists until the clean exact-SHA CI job passes. |
| R8 Documentation truth | Integrated | this file, public architecture docs | certification manifest validation | `Clean release qualification` |  | Exact-SHA qualification remains required. |
| R9 Public operational contract | Integrated | public API, operations docs, and this matrix | subsystem and adapter tests | `Clean release qualification` |  | Exact-SHA qualification remains required. |
| R10 Exact-SHA evidence | Integrated | `scripts/generate-certification-manifest.py` | clean artifact conformance | `Clean release qualification` |  | No fresh-checkout, exact-SHA qualification artifact exists yet. |

Public documentation may describe Integrated behavior precisely, but must not
call it Qualified or production-certified. Updating a row to Qualified requires
all referenced tests, the relevant packaged clients, and the release artifact
checks to pass for the same commit SHA.
