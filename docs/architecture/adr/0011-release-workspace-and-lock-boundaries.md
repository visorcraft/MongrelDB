# ADR-0011: Release Workspace and Lock Boundaries

- Status: Accepted
- Date: 2026-07-20
- Spec references: release qualification and exact-SHA evidence

## Context

MongrelDB previously treated every binding and daemon crate as an independent
Cargo workspace. That produced many lockfiles for crates that all ship from
one engine source tree and version train. A version verifier could detect
declared-version drift, but could not prove one dependency resolution.

Two adapters are different: `mongreldb-kit-ffi` and `mongreldb-jni` consume
`mongreldb-kit` from the separate MongrelDB-Kit repository. A new engine API
must be published before Kit can publish against it, while those adapters can
only qualify after that Kit release exists. Pulling them into the engine
workspace would create a cross-repository release cycle and make the engine's
pre-publication qualification depend on a package that cannot exist yet.

## Decision

1. Every engine-owned Rust crate is a member of the root workspace and uses
   the root `Cargo.lock`: core, query, protocol, log, consensus, cluster,
   simulator, fault injection, server, client, Node addon, C FFI, MySQL
   migrate/wire, and performance harness.
2. Nested workspace declarations and lockfiles are removed from those crates.
   Root `cargo ... --workspace` gates resolve and check the whole engine train.
3. `mongreldb-kit-ffi` and `mongreldb-jni` remain separate workspaces. Their
   lockfiles are release evidence for the cross-repository adapter phase, not
   alternate engine resolutions.
4. Release order is:

   1. qualify and publish engine crates and native engine artifacts;
   2. update, qualify, and publish MongrelDB-Kit at the same component version;
   3. regenerate the two adapter lockfiles against that exact Kit version;
   4. qualify and publish Kit FFI and JNI artifacts.

5. The release manifest records the root lock hash, both adapter lock hashes,
   the MongrelDB SHA, and the MongrelDB-Kit SHA. Version verification remains
   necessary, but is not accepted as a substitute for these hashes.

## Alternatives Considered

- Keep every crate standalone. Rejected because independent locks allowed
  engine artifacts at one version to resolve different dependency graphs.
- Put all crates in one workspace. Rejected because pre-release engine source
  and the not-yet-published matching Kit package form an unavoidable release
  cycle.
- Move JNI and Kit FFI into the Kit repository immediately. Viable later, but
  rejected for this release because their packaging workflows and public
  artifact locations are already owned here.

## Consequences

- Engine code has one reproducible dependency resolution.
- The only extra lockfiles describe real cross-repository release units.
- NAPI remains in the root Cargo workspace; its JavaScript build and smoke
  gates still run separately because Cargo does not exercise Node packaging.
- Kit FFI/JNI cannot be called qualified before the matching Kit package and
  exact source SHA exist.

## Migration

Remove nested `[workspace]` declarations and `Cargo.lock` files from every
engine-owned crate, add those crates to the root workspace, and update version
scripts to regenerate only the root lock. Keep the two adapter workspaces and
regenerate their locks after the matching Kit package is available.

## Reversal Strategy

An engine crate may become a separate release unit only through another ADR
that names its independent compatibility contract and qualification evidence.
The two adapters may move into the Kit repository; doing so removes their
workspaces and lockfiles here after release workflows and artifact ownership
move atomically.
