# ADR-0005: Network Protocol and Serialization

- Status: Accepted
- Date: 2026-07-17
- Spec references: §6.7 (`mongreldb-protocol`), §9.3 (FND-003 versioned command envelope), §10.4 (Stage 1D server protocol and sessions)

## Context

MongrelDB v0.59.0 exposes exactly one wire surface: an HTTP/1.1 + JSON API
served by `crates/mongreldb-server` on axum 0.8 (see
`crates/mongreldb-server/Cargo.toml:20` and `axum::serve` in
`crates/mongreldb-server/src/main.rs`), including the Kit routes in
`crates/mongreldb-server/src/kit.rs`. Every Rust, Node.js, C, and Java client
speaks this surface today.

This surface has structural limits that the target architecture (spec §3,
stages 1–3) cannot live with:

- **No multiplexing.** One in-flight request per HTTP/1.1 connection (or
  connection-pool sprawl) blocks query cancellation, interleaved streaming
  results, and session transactions on a single connection.
- **JSON serialization cost and ambiguity.** JSON is text; it has no 64-bit
  integer fidelity, no zero-copy binary column batches, and per-row field-name
  repetition. Result sets dominate server CPU at scale.
- **No streaming result framing.** Large result sets are buffered or paged
  through request/response cycles instead of flowing as back-pressured
  batches.
- **No structured error contract.** Errors are human-readable strings; clients
  cannot reliably branch on machine-checkable error codes, retryability, or
  failed-constraint identity.
- **Replication needs a native transport.** Stages 2–3 add Raft replication,
  coordinator/participant 2PC traffic, and tablet routing. These are
  high-frequency internal RPCs with strict latency budgets; HTTP/JSON per
  message is not viable.
- **Durability of commands.** Cluster commands must persist inside the
  versioned `CommandEnvelope` (spec §9.3, FND-003) already implemented in
  `crates/mongreldb-log/src/envelope.rs`, whose payload contract requires a
  schema-evolution-safe encoding with a deterministic canonical form.

A decision is needed now because the protocol crate boundary
(`mongreldb-protocol`, spec §6.7) is drawn in Stage 0/1, and every later stage
(consensus, cluster, distributed SQL) builds on this transport.

## Decision

Adopt a **native RPC protocol** as the single primary internal and external
transport, with the existing HTTP/JSON + Kit surface retained as compatibility
adapters.

1. **Transport: TLS 1.3 with HTTP/2 multiplexed streams.** All native RPC
   traffic — client-to-server and node-to-node — runs over TLS 1.3. HTTP/2
   provides multiplexed bidirectional streams on one connection, which carries
   concurrent queries, cancellation, and streaming results without a thread or
   connection per request. The stack must be QUIC-capable at the API boundary
   (spec §6.7 names "HTTP/2 or QUIC-capable RPC"), but QUIC deployment is
   deferred and is not part of this decision.

2. **Frame typing:**
   - **Control frames: Protobuf**, generated with `prost`. Authentication,
     session lifecycle, transaction control, catalog, admin, health, query
     status/cancel, and prepared-statement management are Protobuf messages
     defined in `mongreldb-protocol`.
   - **Result frames: Arrow IPC.** Query result sets flow as Arrow IPC record
     batches on streaming RPCs, matching the existing DataFusion/Arrow query
     frontend (`crates/mongreldb-query`) without a transpose through JSON.

3. **Structured errors.** Every RPC failure carries a machine-checkable
   structure: stable error code (aligned with the Stage 0 error taxonomy,
   spec §9.7), message, retryability class, and optional detail payload (for
   example the violated constraint identity). Stringly-typed errors are
   prohibited on the native protocol.

4. **Canonical request model.** Every protocol adapter — native RPC, HTTP/JSON,
   and Kit — converts inbound traffic into the single `ExecuteRequest` of spec
   §10.4 (S1D-001): `request_id`, `query_id`, optional `session_id`,
   `database_id`, authenticated principal, command, deadline, result limits,
   resource group, and idempotency key. Adapters own no semantics beyond
   encoding conversion.

5. **Service surface.** The native protocol exposes the services of spec
   §10.4 (S1D-003): `AuthService`, `SessionService`, `QueryService`,
   `TransactionService`, `CatalogService`, `AdminService`, `HealthService`,
   with the required methods (`OpenSession`, `CloseSession`, `Prepare`,
   `Execute`, `ExecuteStream`, `CancelQuery`, `GetQueryStatus`, `Begin`,
   `Commit`, `Rollback`, `GetSchema`).

6. **Serialization rules for command payloads** (aligned with the scaffolded
   `CommandEnvelope` in `crates/mongreldb-log`, spec §9.3):
   - Command payloads use a schema-evolution-safe encoding (Protobuf for
     Protobuf-defined commands).
   - **Field numbers are never reused.** Retired fields are reserved.
   - Unknown optional fields are preserved or ignored safely.
   - **Unknown required command versions fail closed.** Decoders reject
     envelope `format_version` values outside the supported range, exactly as
     `CommandEnvelope::verify`/`decode` already do (`UnsupportedVersion`,
     `ChecksumMismatch`, `Truncated`, `TrailingBytes`).
   - Every command has a deterministic canonical encoding; the envelope
     checksum covers format version, command type, payload length, and
     payload.

7. **Connection handling** follows spec §10.4 (S1D-007): asynchronous network
   I/O, never one dedicated OS thread per connection, and hard bounds on
   connections, sessions, in-flight requests, request bytes, result bytes, and
   idle time.

8. **HTTP/JSON and Kit routes remain** as compatibility adapters mapping onto
   the same `ExecuteRequest` model. They are supported surfaces, not legacy
   code paths to delete; they may lag the native protocol in features
   (streaming, cancellation propagation) but never in correctness or
   authorization.

## Alternatives Considered

- **Keep HTTP/JSON as the only protocol.** Zero migration cost, but fails
  every structural requirement above: no multiplexing, no streaming frames,
  JSON CPU cost, no replication-grade internal transport. Rejected.

- **gRPC (tonic) as the RPC framework.** gRPC gives HTTP/2 + Protobuf +
  streaming for free and is the closest off-the-shelf match. It was not
  selected outright because Arrow IPC result frames want explicit control over
  framing and zero-copy buffer layout, and node-to-node Raft/2PC traffic wants
  custom, minimal frames without gRPC's per-call overhead. Protobuf message
  *definitions* remain compatible with a later gRPC mapping if operational
  experience favors it; `prost` is retained as the message compiler either
  way.

- **FlatBuffers or Cap'n Proto for control frames.** Better zero-copy reads
  than Protobuf, but a smaller Rust ecosystem, weaker schema-evolution
  tooling, and a second serialization idiom to govern. Protobuf's field-number
  discipline maps directly onto the envelope rules of §9.3. Rejected.

- **JSON control frames over HTTP/2 (keep axum, add streaming).** Solves
  multiplexing but keeps JSON's numeric-fidelity and CPU problems on the hot
  path and still lacks a canonical binary encoding for durable commands.
  Rejected.

- **QUIC/HTTP/3 now.** §6.7 permits a QUIC-capable RPC, but QUIC adds
  operational surface (UDP load balancing, TLS offload, kernel bypass tuning)
  before replication even exists. Deferred; the protocol design must not
  preclude it.

- **Custom bincode/rmp wire format.** Fast but not schema-evolution-safe and
  not self-describing; §9.3 explicitly prohibits persisting new cluster
  commands as unversioned `bincode` enums. Rejected.

## Consequences

Positive:

- One protocol serves embedded-server clients, inter-node replication, and
  distributed transactions; adapters shrink to pure encoding conversion.
- Arrow IPC result frames eliminate the JSON transpose on the query hot path
  and give clients typed, columnar batches with backpressure.
- Multiplexed streams make cancellation, query status, and session
  transactions first-class instead of bolted-on endpoints.
- Protobuf field-number discipline plus the `CommandEnvelope` fail-closed
  rules make mixed-version clusters and rolling upgrades (spec §11.8) a
  designed property rather than an accident.
- Structured errors give every binding (Rust, Node.js NAPI, C FFI, JNI) one
  machine-checkable error contract.

Negative / costs:

- A second, permanent protocol surface (native RPC alongside HTTP/JSON + Kit)
  must be tested, fuzzed, and versioned. Adapter parity tests become
  mandatory.
- Protobuf schema governance is a new ongoing duty: reserving retired field
  numbers, reviewing `.proto` changes, and enforcing canonical encoding in CI.
- Clients must ship a new transport stack (TLS 1.3, HTTP/2, Protobuf, Arrow
  IPC) in every binding language; FFI bindings need a C-ABI-safe framing
  story.
- TLS 1.3 is mandatory on the native path; deployments that today terminate
  plaintext HTTP must provision certificates for replication and native
  clients.

## Migration

1. Stage 0/1: create `mongreldb-protocol` owning the `.proto` definitions and
   the versioned message/service surface; implement the native listener in
   `mongreldb-server` next to the existing axum 0.8 listener. The HTTP/JSON
   and Kit routes are rewired to the canonical `ExecuteRequest` model
   (S1D-001) so both surfaces share one execution path.
2. The `mongreldb-client` crate gains the native transport behind its existing
   session/transaction API; HTTP/JSON remains the fallback transport until the
   native path is feature-complete.
3. Durable cluster commands adopt the `CommandEnvelope` (already scaffolded in
   `crates/mongreldb-log`) with Protobuf payloads from day one; no unversioned
   command bytes are ever written.
4. Binding crates (node, ffi, kit-ffi, jni) migrate to the native protocol one
   release at a time, keeping their public APIs stable.
5. Feature/correctness gates: adapter parity tests (same request through both
   surfaces yields the same outcome), mixed-version envelope decode tests
   (unknown versions fail closed), and fuzzing of the frame decoder.

## Reversal Strategy

The native protocol is additive; nothing durable depends on it until Stage 2
replication writes envelope-encoded commands. Reversal is therefore possible
in stages:

- **Before Stage 2:** remove the native listener and `mongreldb-protocol`;
  HTTP/JSON + Kit remain the whole surface. No data migration is needed
  because the adapter model keeps storage formats untouched.
- **After durable commands exist:** envelope-decoding is versioned and
  fail-closed, so even a full protocol rollback leaves persisted commands
  readable by any build that supports their `format_version`. A rollback build
  simply refuses newer versions instead of corrupting state.
- **Compatibility floor:** HTTP/JSON + Kit are never removed as part of this
  decision, so clients always have a working surface to fall back to. The
  irreversible step is external operational reliance on native-only features
  (streaming, native cancellation); that reliance, not the code, is what
  cannot be rolled back cheaply.
