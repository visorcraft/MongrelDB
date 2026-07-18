# Architecture implementation status

This is the authoritative status for the 0.60.3 architecture audit. A source
module or unit test alone does not make a feature production-ready.

Status meanings:

- **Not Started**: no usable implementation.
- **Scaffolded**: types, helpers, or simulations exist, but no production path.
- **Integrated**: a production path exists, but the release qualification gate
  is incomplete.
- **Qualified**: exact-SHA CI and packaged-artifact evidence passed.

No item is currently Qualified. `last qualified SHA` stays empty until CI
publishes the required evidence.

| ID | Status | Source | Tests | CI job | Last qualified SHA | Known limitations |
|---|---|---|---|---|---|---|
| R1 Shared-handle authority | Integrated | `crates/mongreldb-core/src/handle.rs`, `manager.rs`, `database.rs` | `crates/mongreldb-core/tests/shared_handles.rs` | workspace tests |  | Packaged-client authority tests remain required. |
| R2 Native RPC | Scaffolded | `crates/mongreldb-protocol/src/` | protocol unit tests | workspace tests |  | No Protobuf schemas, HTTP/2 TLS listener, native client, or real Arrow stream. |
| R3 Production security | Scaffolded | `crates/mongreldb-core/src/security_hardening.rs` | module unit tests | workspace tests |  | SCRAM/JWS/JWKS primitives are not wired into a production listener. No HTTPS OIDC discovery adapter or production KMS provider. Rotation journal is not wired into data re-encryption. |
| R4 MySQL migration and wire | Scaffolded | `crates/mongreldb-core/src/migrate_mysql.rs`, server compatibility handlers | module and handler unit tests | workspace tests |  | No external MySQL source/binlog loop or packet-compatible listener qualification. |
| R5 Generated embedding writes | Integrated | `crates/mongreldb-core/src/embedding.rs`, `schema.rs`, `database.rs` | `crates/mongreldb-core/tests/generated_embeddings.rs` | workspace tests |  | Synchronous `AbortWrite` only. Background pending/ready jobs and per-row generation metadata are not implemented. |
| R6 Provider hardening | Integrated | `crates/mongreldb-core/src/embedding.rs` | embedding unit and integration tests | workspace tests |  | Remote-provider secret, TLS, egress, retry, redaction, and tenant-isolation adapter is not implemented. |
| R7 Production certification | Scaffolded | `crates/mongreldb-core/src/certification.rs` | module unit tests | workspace tests |  | Static inventory is not executable release certification. Fuzz, crash matrix, and packaged-artifact evidence are missing. |
| R8 Documentation truth | Integrated | this file, public architecture docs | documentation review | none |  | CI does not yet validate this table against evidence artifacts. |
| R9 Public operational contract | Scaffolded | public API and operations docs | subsystem tests | workspace tests |  | Stage 4/5 public qualification matrix and packaged-client parity remain incomplete. |
| R10 Exact-SHA evidence | Not Started |  |  |  |  | No fresh-checkout, exact-SHA qualification artifact exists. |

Public documentation may describe Integrated behavior precisely, but must not
call it Qualified or production-certified. Updating a row to Qualified requires
all referenced tests, the relevant packaged clients, and the release artifact
checks to pass for the same commit SHA.
