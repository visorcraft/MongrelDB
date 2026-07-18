# Protobuf governance

Package `mongreldb.v1` is the native RPC compatibility boundary.

- Never reuse a field number or enum numeric value.
- Reserve removed field numbers and names.
- Add fields with new numbers only.
- Append enum values. Keep zero as `UNSPECIFIED`.
- Breaking service or semantic changes require a new package version.
- Unknown fields must remain tolerated by Protobuf decoders.
- `RequestContext.version` is mandatory at runtime. Unsupported major
  versions fail closed.

`cargo test -p mongreldb-protocol` compiles every schema and generated tonic
client/server stub. CI installs `protoc` before workspace, server, client, and
release builds.
