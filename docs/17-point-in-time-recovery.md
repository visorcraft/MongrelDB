# Point-in-Time Recovery

MongrelDB point-in-time recovery (PITR) combines a consistent online base
backup with transaction-complete logical WAL chunks. Applications can restore
the latest archived state, a committed epoch, the last commit at or before a
timestamp, an exact transaction ID, or a WAL log position.

## Create and update an archive

```rust
use mongreldb_core::Database;

let database = Database::open("./data")?;
database.create_pitr_archive("./pitr")?;

// Run this periodically while the database remains open.
database.archive_pitr("./pitr")?;
# Ok::<(), mongreldb_core::MongrelError>(())
```

`create_pitr_archive` requires a new destination. It takes an online base
backup and publishes the completed archive atomically. `archive_pitr` appends
every complete commit after the current archive watermark. A WAL retention gap
fails closed. Create a new archive when that happens.

Both operations require the `Admin` permission when credential enforcement is
enabled. MongrelDB rechecks the same immutable user identity and its current
admin status immediately before publishing. Dropping and recreating a username
does not preserve authorization.

## Restore

Restores always use a new destination directory:

```rust
use mongreldb_core::{restore_pitr, PitrCredentials, PitrTarget};

let restored_epoch = restore_pitr(
    "./pitr",
    "./restored",
    PitrTarget::Latest,
    PitrCredentials::None,
)?;

let restored_epoch = restore_pitr(
    "./pitr",
    "./restored-at-epoch",
    PitrTarget::Epoch(42),
    PitrCredentials::None,
)?;

let restored_epoch = restore_pitr(
    "./pitr",
    "./restored-at-time",
    PitrTarget::TimestampNanos(1_750_000_000_000_000_000),
)?;

let restored_epoch = restore_pitr(
    "./pitr",
    "./restored-at-txn",
    PitrTarget::TransactionId(4294967300),
    PitrCredentials::None,
)?;

let restored_epoch = restore_pitr(
    "./pitr",
    "./restored-at-position",
    PitrTarget::LogPosition(1_048_576),
    PitrCredentials::None,
)?;
# Ok::<(), mongreldb_core::MongrelError>(())
```

An epoch target that falls between committed epochs resolves to the preceding
commit. A timestamp target resolves to the last commit whose stored timestamp
is not later than the requested timestamp. A transaction-id target resolves to
the exact commit of that transaction through the archive's commit ledger
(transaction IDs are scoped by the source database's open generation). A
log-position target resolves to the newest commit whose WAL record sequence is
at or below the position; positions inside the base backup resolve to the base
boundary. Both ledger targets fail closed on archives written before the
ledger existed. `mongreldb_core::pitr::restore_pitr_validated` additionally
returns a `RestoreReport` summarizing the post-restore validation pass (run
checksums, catalog load, manifest consistency); the same pass runs inside
every restore and aborts publication on corruption.

For an encrypted database, supply the original database passphrase:

```rust
restore_pitr(
    "./pitr",
    "./restored",
    PitrTarget::Latest,
    PitrCredentials::Encryption("database passphrase"),
)?;
# Ok::<(), mongreldb_core::MongrelError>(())
```

`PitrCredentials::User` and `PitrCredentials::EncryptionAndUser` additionally
verify a user against the final restored catalog before publication. Recovery
itself is an offline filesystem-owner operation and does not need a database
user when that final validation is not requested.

## Archive format and integrity

New archives use PITR format version 2.

- Every chunk has an exact generated filename, contiguous epoch range, record
  count, byte count, commit list, timestamp list, first and last record
  sequence, and SHA-256 checksum. Sequences are strictly increasing within and
  across chunks.
- The manifest records the SHA-256 of the exact bounded base-backup manifest.
  Restore requires its epoch and exact file set to match. Extra unlisted base
  files, including injected WAL files, fail closed.
- Chunk references form a SHA-256 chain rooted at the base backup boundary and
  base-backup manifest checksum.
- Each chunk reference carries a commit ledger parallel to its commit list:
  the committing transaction ID and the WAL record sequence (log position) of
  every commit. Restore cross-checks recorded ledger entries against the chunk
  bodies. The ledger lives only in the JSON manifest; chunk binary layouts are
  unchanged, and archives without it remain restorable at epoch, timestamp,
  and latest targets.
- Restore validates paths, ordering, ranges, counts, checksums, chain links,
  commit markers, transaction completeness, timestamps, and chunk bodies.
- Missing, duplicated, reordered, substituted, truncated, or rewritten chunks
  fail before a restore destination is published.
- Manifests and chunks have bounded sizes. Trailing or malformed binary data is
  rejected.

For encrypted databases, version 2 also provides confidentiality and keyed
authentication:

- Logical WAL chunk payloads use AES-256-GCM with a fresh random nonce per
  chunk.
- Chunk and manifest keys are separately domain-derived from the database key.
- The manifest is authenticated with HMAC-SHA256. It roots the chunk identity,
  range, count, checksum, ordering, previous-chain links, and exact base-backup
  manifest.
- The database passphrase and derived keys are never stored in the PITR
  manifest or chunk files.

A wrong passphrase, modified ciphertext, changed manifest, or chunk-chain
rewrite fails authentication. Plaintext archives retain strict structural
validation, SHA-256 checksums, and chaining, but do not claim keyed
authentication against an attacker who can rewrite the entire archive.

## Version 1 archives

Plaintext version 1 archives remain restorable for compatibility. They are
restore-only. MongrelDB refuses to append to them because mixing old and new
chunk rules would weaken the archive. Create a new version 2 archive to resume
periodic archiving.

Encrypted version 1 archives are refused. Their logical chunks were not
encrypted or keyed-authenticated, so silently migrating them would preserve an
unsafe history. Restore from a separately trusted backup if needed, open the
database, then create a new version 2 PITR archive.

## Failure behavior

Restore stages all work in a temporary sibling directory. It verifies the base
backup, manifest, requested target, chunk chain, chunk bodies, replay result,
and optional final credentials before atomically publishing the destination.
On a normal validation or authentication failure, the requested destination is
not created. A restore destination inside the archive is rejected so staging
cannot recursively copy or modify its own source. A restore never happens in
place over a running database: a destination whose `_meta/.lock` is held by a
live database handle is refused with `DatabaseLocked` before any staging work
begins, and an existing destination of any kind conflicts.

Archive, restore, and staging roots are descriptor-pinned. Manifest, chunk,
lock, copy, cleanup, and publication operations are root-relative and refuse
symlinks, reparse points, and non-regular files. Temporary files and staging
directories use fresh random names, and final directory publication is
no-replace. Renaming or replacing a path after an operation starts cannot
redirect writes into the replacement. A malicious nested symlink makes the
operation fail without writing through it.

Keep PITR archives on storage with appropriate access controls and backups.
Encryption protects archived logical data, but it does not replace filesystem
permissions, retention planning, or independent archive copies. Descriptor
pinning and no-follow traversal prevent path redirection, but they cannot make
a directory safe from an arbitrary process running as the same OS user with
permission to rename and rewrite every entry. Do not allow concurrent writers
other than the archiving daemon.

