//! Point-in-time recovery archives built from an online base backup plus
//! checksummed, transaction-complete logical WAL chunks.

use crate::backup::verify_backup_durable_with_manifest_sha256;
use crate::durable_file::DurableRoot;
use crate::epoch::Epoch;
use crate::wal::{Op, Record};
use crate::{Database, MongrelError, Result};
use bincode::Options as _;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::{Component, Path, PathBuf};

const LEGACY_FORMAT_VERSION: u16 = 1;
const FORMAT_VERSION: u16 = 2;
const MANIFEST_FILE: &str = "pitr.json";
const MAX_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;
const MAX_CHUNK_BYTES: u64 = 1024 * 1024 * 1024;
#[cfg(feature = "encryption")]
const MANIFEST_AUTH_DOMAIN: &[u8] = b"mongreldb/pitr/manifest-auth/v2\0";
const CHAIN_DOMAIN: &[u8] = b"mongreldb/pitr/chunk-chain/v2\0";
const GENESIS_DOMAIN: &[u8] = b"mongreldb/pitr/genesis/v2\0";
#[cfg(feature = "encryption")]
const CHUNK_KEY_DOMAIN: &[u8] = b"mongreldb/pitr/chunk/v2";
#[cfg(feature = "encryption")]
const MANIFEST_KEY_DOMAIN: &[u8] = b"mongreldb/pitr/manifest/v2";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PitrTarget {
    Latest,
    Epoch(u64),
    TimestampNanos(u64),
    /// Restore through the commit of this exact transaction id, resolved
    /// against the archive's commit-point ledger ([`PitrCommitPoint`]).
    /// Transaction ids are scoped by the source database's open generation;
    /// an id the ledger does not record (including archives written before
    /// Stage 1G, whose ledger predates the field) fails closed.
    TransactionId(u64),
    /// Restore through the newest commit whose WAL record sequence (log
    /// position) is at or below this position. Positions before the first
    /// archived commit resolve to the base backup boundary; a position
    /// above every archived commit resolves to [`PitrTarget::Latest`]'s
    /// epoch. An archive whose ledger records no sequences at all (written
    /// before Stage 1G) fails closed.
    LogPosition(u64),
}

#[derive(Clone, Copy)]
pub enum PitrCredentials<'a> {
    /// Filesystem-owner recovery. Authenticated target catalogs are restored
    /// without validating a database user.
    None,
    Encryption(&'a str),
    /// Filesystem-owner recovery plus validation against the final target
    /// catalog. Supplying bad credentials fails before publication.
    User {
        username: &'a str,
        password: &'a str,
    },
    EncryptionAndUser {
        passphrase: &'a str,
        username: &'a str,
        password: &'a str,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PitrCommitPoint {
    pub epoch: u64,
    pub unix_nanos: u64,
}

/// Stage 1G commit-ledger entry: the committing transaction id and the WAL
/// record sequence (log position) of one commit. Stored parallel to
/// [`PitrChunkRef::commits`] in the JSON manifest only — never inside the
/// bincode chunk bodies, whose byte layout is unchanged.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PitrCommitLedgerEntry {
    pub txn_id: u64,
    pub sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PitrChunkRef {
    pub file: String,
    pub from_epoch: u64,
    pub through_epoch: u64,
    pub records: usize,
    pub bytes: u64,
    pub sha256: String,
    pub commits: Vec<PitrCommitPoint>,
    /// Stage 1G commit ledger, parallel to `commits`: transaction id and log
    /// position per commit. Empty means "not recorded" (archives written
    /// before Stage 1G); when present it has exactly `commits.len()` entries.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub commit_ledger: Vec<PitrCommitLedgerEntry>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub first_sequence: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub last_sequence: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub previous_chain_sha256: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub chain_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PitrArchiveManifest {
    pub format_version: u16,
    pub base_epoch: u64,
    pub base_unix_nanos: u64,
    /// SHA-256 of the exact bounded base-backup manifest bytes. Version 2
    /// authentication and chain genesis bind the base file set through this.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub base_backup_sha256: String,
    pub archived_through_epoch: u64,
    pub last_commit_unix_nanos: u64,
    pub chunks: Vec<PitrChunkRef>,
    /// Version 2 chunks are AEAD-encrypted when the source database is encrypted.
    #[serde(default)]
    pub encrypted: bool,
    /// Hash-chain head. The genesis value binds the base backup boundary.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub chain_sha256: String,
    /// HMAC-SHA256 over the canonical manifest, present for encrypted v2 archives.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PitrArchiveReport {
    pub archive: PathBuf,
    pub from_epoch: u64,
    pub through_epoch: u64,
    pub records: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyPitrChunk {
    format_version: u16,
    from_epoch: u64,
    through_epoch: u64,
    records: Vec<Record>,
    commits: Vec<PitrCommitPoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PitrChunkV2 {
    format_version: u16,
    from_epoch: u64,
    through_epoch: u64,
    records: Vec<Record>,
    commits: Vec<PitrCommitPoint>,
    first_sequence: u64,
    last_sequence: u64,
    previous_chain_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PitrChunkEnvelopeV2 {
    format_version: u16,
    encrypted: bool,
    nonce: Option<[u8; 12]>,
    payload: Vec<u8>,
}

struct DecodedPitrChunk {
    from_epoch: u64,
    through_epoch: u64,
    records: Vec<Record>,
    commits: Vec<PitrCommitPoint>,
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    previous_chain_sha256: Option<String>,
}

impl Database {
    /// Initialize a PITR archive with a consistent online base backup.
    pub fn create_pitr_archive(&self, destination: impl AsRef<Path>) -> Result<PitrArchiveReport> {
        self.create_pitr_archive_inner(destination.as_ref(), || Ok(()))
    }

    fn create_pitr_archive_inner<F>(
        &self,
        destination: &Path,
        before_publish: F,
    ) -> Result<PitrArchiveReport>
    where
        F: FnOnce() -> Result<()>,
    {
        let admin = crate::auth::Permission::Admin;
        self.require(&admin)?;
        let operation_principal = self.principal_snapshot();
        let prepared = prepare_destination(destination, "pitr-stage")?;
        let stage = prepared.parent.open_directory(&prepared.stage_name)?;
        let mut before_publish = Some(before_publish);
        let outcome = (|| {
            let backup = self.hot_backup_to_durable_child(
                &stage,
                Path::new("base"),
                &crate::ExecutionControl::new(None),
            )?;
            let base = stage.open_directory("base")?;
            let (verified_backup, base_backup_sha256) =
                verify_backup_durable_with_manifest_sha256(&base)?;
            if verified_backup.epoch != backup.epoch {
                return Err(MongrelError::Other(format!(
                    "PITR base backup epoch changed during creation: expected {}, got {}",
                    backup.epoch, verified_backup.epoch
                )));
            }
            drop(base);
            let manifest = PitrArchiveManifest {
                format_version: FORMAT_VERSION,
                base_epoch: backup.epoch,
                base_unix_nanos: backup.boundary_unix_nanos,
                base_backup_sha256: base_backup_sha256.clone(),
                archived_through_epoch: backup.epoch,
                last_commit_unix_nanos: backup.boundary_unix_nanos,
                chunks: Vec::new(),
                encrypted: self.kek().is_some(),
                chain_sha256: genesis_chain(
                    backup.epoch,
                    backup.boundary_unix_nanos,
                    &base_backup_sha256,
                )?,
                authentication: None,
            };
            write_manifest(&stage, &manifest, self.kek().map(AsRef::as_ref))?;
            let publish = before_publish
                .take()
                .ok_or_else(|| MongrelError::Other("PITR publish hook already consumed".into()))?;
            publish()?;
            drop(stage);
            self.with_exact_principal_current(operation_principal.as_ref(), &admin, || {
                let published = std::cell::Cell::new(false);
                if let Err(error) = prepared.parent.rename_directory_new_with_after(
                    &prepared.stage_name,
                    &prepared.parent,
                    &prepared.destination_name,
                    || published.set(true),
                ) {
                    if published.get() {
                        return Err(MongrelError::CommitOutcomeUnknown {
                            epoch: backup.epoch,
                            message: format!("PITR archive publication was not durable: {error}"),
                        });
                    }
                    if error.kind() == std::io::ErrorKind::AlreadyExists {
                        return Err(MongrelError::Conflict(format!(
                            "PITR archive already exists: {}",
                            prepared.destination.display()
                        )));
                    }
                    return Err(error.into());
                }
                // Keep a practical WAL window for callers that archive periodically.
                self.set_replication_wal_retention_segments(64);
                Ok(PitrArchiveReport {
                    archive: prepared.destination.clone(),
                    from_epoch: backup.epoch,
                    through_epoch: backup.epoch,
                    records: 0,
                })
            })
        })();
        if outcome.is_err() {
            let _ = prepared.parent.remove_directory_all(&prepared.stage_name);
        }
        outcome
    }

    /// Append all complete commits since the archive watermark. Spilled-run
    /// commits are converted to ordinary logical Put records while their run
    /// payload remains available. A retention gap fails closed.
    pub fn archive_pitr(&self, archive: impl AsRef<Path>) -> Result<PitrArchiveReport> {
        self.archive_pitr_inner(archive.as_ref(), || Ok(()))
    }

    fn archive_pitr_inner<F>(&self, archive: &Path, before_publish: F) -> Result<PitrArchiveReport>
    where
        F: FnOnce() -> Result<()>,
    {
        let admin = crate::auth::Permission::Admin;
        self.require(&admin)?;
        let operation_principal = self.principal_snapshot();
        let archive = DurableRoot::open(archive)?;
        let archive_path = archive.canonical_path().to_path_buf();
        let lock = archive.open_lock_file(".archive.lock")?;
        lock.lock_exclusive()?;
        let mut manifest = read_pitr_manifest_from_root(&archive)?;
        validate_archive_key(&manifest, self.kek().map(AsRef::as_ref))?;
        verify_manifest_authentication(&manifest, self.kek().map(AsRef::as_ref))?;
        if manifest.format_version == LEGACY_FORMAT_VERSION {
            return Err(MongrelError::Conflict(
                "legacy PITR archives are restore-only; create a new version 2 archive".into(),
            ));
        }
        let from_epoch = manifest.archived_through_epoch;
        let batch = self.replication_batch_since(from_epoch)?;
        if batch.current_epoch == from_epoch {
            return Ok(PitrArchiveReport {
                archive: archive_path,
                from_epoch,
                through_epoch: from_epoch,
                records: 0,
            });
        }
        if batch.retention_gap {
            return Err(MongrelError::Conflict(format!(
                "PITR WAL retention gap after epoch {from_epoch}; create a new base archive"
            )));
        }

        let mut records = materialize_spilled_records(self, batch.records)?;
        if records.is_empty() {
            return Err(MongrelError::Conflict(
                "PITR source advanced but no complete WAL transactions remain".into(),
            ));
        }
        let minimum_sequence = manifest
            .chunks
            .last()
            .map(|reference| {
                reference.last_sequence.checked_add(1).ok_or_else(|| {
                    MongrelError::Conflict("PITR record sequence space exhausted".into())
                })
            })
            .transpose()?;
        normalize_record_sequences(&mut records, minimum_sequence)?;
        let first_sequence = records
            .first()
            .map(|record| record.seq.0)
            .ok_or_else(|| MongrelError::Conflict("PITR batch has no records".into()))?;
        let last_sequence = records
            .last()
            .map(|record| record.seq.0)
            .ok_or_else(|| MongrelError::Conflict("PITR batch has no records".into()))?;
        let mut timestamps = HashMap::new();
        let mut commit_epochs = Vec::new();
        for record in &records {
            match record.op {
                Op::CommitTimestamp { unix_nanos } => {
                    timestamps.insert(record.txn_id, unix_nanos);
                }
                Op::TxnCommit { epoch, .. } => {
                    commit_epochs.push((record.txn_id, epoch, record.seq.0));
                }
                _ => {}
            }
        }
        commit_epochs.sort_by_key(|(_, epoch, _)| *epoch);
        let archive_time = unix_nanos();
        let mut last_timestamp = manifest.last_commit_unix_nanos;
        let mut commit_ledger = Vec::new();
        let commits = commit_epochs
            .into_iter()
            .map(|(txn_id, epoch, sequence)| {
                let timestamp = timestamps
                    .get(&txn_id)
                    .copied()
                    .unwrap_or(archive_time)
                    .max(last_timestamp);
                last_timestamp = timestamp;
                commit_ledger.push(PitrCommitLedgerEntry { txn_id, sequence });
                PitrCommitPoint {
                    epoch,
                    unix_nanos: timestamp,
                }
            })
            .collect::<Vec<_>>();
        let through_epoch = commits
            .last()
            .map(|commit| commit.epoch)
            .ok_or_else(|| MongrelError::Conflict("PITR batch has no commit marker".into()))?;
        let previous_chain_sha256 = manifest.chain_sha256.clone();
        let chunk = PitrChunkV2 {
            format_version: FORMAT_VERSION,
            from_epoch,
            through_epoch,
            records,
            commits: commits.clone(),
            first_sequence,
            last_sequence,
            previous_chain_sha256: previous_chain_sha256.clone(),
        };
        let file = chunk_file_name(from_epoch, through_epoch);
        let bytes = encode_or_reuse_chunk_v2(
            &archive,
            Path::new(&file),
            &chunk,
            self.kek().map(AsRef::as_ref),
        )?;
        let chunk_sha256 = sha256_bytes(&bytes);
        let chain_sha256 = next_chain(
            &previous_chain_sha256,
            &chunk_sha256,
            from_epoch,
            through_epoch,
            chunk.records.len(),
            first_sequence,
            last_sequence,
        )?;
        before_publish()?;
        self.with_exact_principal_current(operation_principal.as_ref(), &admin, || {
            publish_chunk(&archive, Path::new(&file), &bytes, &chunk_sha256)?;
            manifest.chunks.push(PitrChunkRef {
                file,
                from_epoch,
                through_epoch,
                records: chunk.records.len(),
                bytes: bytes.len() as u64,
                sha256: chunk_sha256,
                commits,
                commit_ledger,
                first_sequence,
                last_sequence,
                previous_chain_sha256,
                chain_sha256: chain_sha256.clone(),
            });
            manifest.archived_through_epoch = through_epoch;
            manifest.last_commit_unix_nanos = last_timestamp;
            manifest.chain_sha256 = chain_sha256;
            let manifest_published = std::cell::Cell::new(false);
            let publication = write_manifest_with_after(
                &archive,
                &manifest,
                self.kek().map(AsRef::as_ref),
                || manifest_published.set(true),
            );
            finish_manifest_publication(publication, manifest_published.get(), through_epoch)?;
            Ok(PitrArchiveReport {
                archive: archive_path,
                from_epoch,
                through_epoch,
                records: chunk.records.len(),
            })
        })
    }
}

pub fn read_pitr_manifest(archive: impl AsRef<Path>) -> Result<PitrArchiveManifest> {
    let archive = DurableRoot::open(archive)?;
    read_pitr_manifest_from_root(&archive)
}

fn read_pitr_manifest_from_root(archive: &DurableRoot) -> Result<PitrArchiveManifest> {
    let source = archive.open_regular(MANIFEST_FILE)?;
    let length = source.metadata()?.len();
    if length > MAX_MANIFEST_BYTES {
        return Err(MongrelError::InvalidArgument(format!(
            "PITR manifest exceeds {MAX_MANIFEST_BYTES} bytes"
        )));
    }
    let mut bytes = Vec::with_capacity(length as usize);
    source
        .take(MAX_MANIFEST_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(MongrelError::InvalidArgument(format!(
            "PITR manifest exceeds {MAX_MANIFEST_BYTES} bytes"
        )));
    }
    let manifest: PitrArchiveManifest = serde_json::from_slice(&bytes)
        .map_err(|error| MongrelError::InvalidArgument(format!("PITR manifest: {error}")))?;
    validate_manifest_structure(&manifest)?;
    Ok(manifest)
}

/// Restore an archive to a new directory at an epoch or timestamp cutoff.
///
/// Restores never happen in place: the destination must not exist, and a
/// destination whose `_meta/.lock` is held by a running database is refused
/// before any staging work begins (spec 10.7).
pub fn restore_pitr(
    archive: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    target: PitrTarget,
    credentials: PitrCredentials<'_>,
) -> Result<u64> {
    restore_pitr_inner(
        archive.as_ref(),
        destination.as_ref(),
        target,
        credentials,
        |_| Ok(()),
    )
    .map(|(epoch, _)| epoch)
}

/// [`restore_pitr`] plus the post-restore validation report (Stage 1G).
pub fn restore_pitr_validated(
    archive: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    target: PitrTarget,
    credentials: PitrCredentials<'_>,
) -> Result<(u64, crate::backup::RestoreReport)> {
    restore_pitr_inner(
        archive.as_ref(),
        destination.as_ref(),
        target,
        credentials,
        |_| Ok(()),
    )
}

/// Refuse a restore destination held by a running database. The destination
/// must not exist at all (enforced by `prepare_destination` and the
/// no-replace publish rename); this guard turns the "exists" case into a
/// precise error when the existing root is locked by a live handle.
fn refuse_locked_destination(destination: &Path) -> Result<()> {
    use fs2::FileExt as _;

    let lock_path = destination.join("_meta").join(".lock");
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    match file.try_lock_exclusive() {
        Ok(()) => Ok(()),
        Err(error) => Err(MongrelError::DatabaseLocked {
            path: destination.to_path_buf(),
            message: format!("PITR never restores in place over a running database: {error}"),
        }),
    }
}

fn restore_pitr_inner<F>(
    archive: &Path,
    destination: &Path,
    target: PitrTarget,
    credentials: PitrCredentials<'_>,
    after_stage_created: F,
) -> Result<(u64, crate::backup::RestoreReport)>
where
    F: FnOnce(&Path) -> Result<()>,
{
    refuse_locked_destination(destination)?;
    let archive = DurableRoot::open(archive)?;
    let manifest = read_pitr_manifest_from_root(&archive)?;
    let base = archive.open_directory("base")?;
    let (backup_manifest, base_backup_sha256) = verify_backup_durable_with_manifest_sha256(&base)?;
    let archive_kek = derive_archive_kek(&archive, &manifest, credentials)?;
    verify_manifest_authentication(&manifest, archive_kek.as_ref())?;
    if backup_manifest.epoch != manifest.base_epoch {
        return invalid_pitr(format!(
            "PITR base backup epoch mismatch: expected {}, got {}",
            manifest.base_epoch, backup_manifest.epoch
        ));
    }
    if manifest.format_version == FORMAT_VERSION
        && base_backup_sha256 != manifest.base_backup_sha256
    {
        return invalid_pitr("PITR base backup manifest does not match the archive manifest");
    }
    let target_epoch = resolve_target_epoch(&manifest, target)?;
    let records = load_records_through(&archive, &manifest, target_epoch, archive_kek.as_ref())?;
    let prepared = prepare_destination(destination, "pitr-restore")?;
    if prepared
        .parent
        .canonical_path()
        .starts_with(archive.canonical_path())
    {
        prepared.parent.remove_directory_all(&prepared.stage_name)?;
        return Err(MongrelError::InvalidArgument(
            "PITR restore destination must not be inside the archive".into(),
        ));
    }
    let stage = prepared.parent.open_directory(&prepared.stage_name)?;
    let stage_path = prepared.parent.canonical_path().join(&prepared.stage_name);
    let mut after_stage_created = Some(after_stage_created);
    let outcome = (|| {
        let hook = after_stage_created
            .take()
            .ok_or_else(|| MongrelError::Other("PITR restore hook already consumed".into()))?;
        hook(&stage_path)?;
        copy_tree(&base, &stage)?;
        // The archive may be updated concurrently after the base was verified
        // above. Verify the exact copied tree before replaying anything so a
        // mixed-generation copy can never become the published destination.
        let (staged_backup_manifest, staged_backup_sha256) =
            verify_backup_durable_with_manifest_sha256(&stage)?;
        if staged_backup_manifest != backup_manifest || staged_backup_sha256 != base_backup_sha256 {
            return invalid_pitr(
                "PITR base backup changed while the restore staging copy was created",
            );
        }
        let meta = stage.create_directory_all_pinned("_meta")?;
        meta.write_atomic("replica", b"PITR restore staging\n")?;
        meta.write_atomic("repl_epoch", manifest.base_epoch.to_string().as_bytes())?;

        if !records.is_empty() {
            let earliest_epoch = records.iter().filter_map(|record| match record.op {
                Op::TxnCommit { epoch, .. } => Some(epoch),
                _ => None,
            });
            let batch = crate::replication::ReplicationBatch::complete(
                manifest.base_epoch,
                target_epoch,
                earliest_epoch.min(),
                false,
                false,
                records,
            )?;
            let replica = open_recovery_staging(&stage, credentials)?;
            replica.append_replication_batch(&batch)?;
            drop(replica);
        }
        let recovered = open_recovery_staging(&stage, credentials)?;
        if recovered.visible_epoch().0 < target_epoch {
            return Err(MongrelError::Other(format!(
                "PITR recovery stopped at epoch {}, expected {target_epoch}",
                recovered.visible_epoch().0
            )));
        }
        validate_target_user_credentials(&recovered, credentials)?;
        drop(recovered);
        let restore_report =
            validate_staged_restore(&stage, &staged_backup_manifest, archive_kek.as_ref())?;
        meta.remove_file("replica")?;
        meta.remove_file("repl_epoch")?;
        drop(meta);
        drop(stage);
        // FND-006: the staged restore is fully recovered; fire before the
        // rename publishes it. The outer error path removes the staging
        // tree, so a fired fault leaves the destination untouched.
        crate::catalog::inject_hook("snapshot.install.before")?;
        let published = std::cell::Cell::new(false);
        if let Err(error) = prepared.parent.rename_directory_new_with_after(
            &prepared.stage_name,
            &prepared.parent,
            &prepared.destination_name,
            || published.set(true),
        ) {
            if published.get() {
                return Err(MongrelError::CommitOutcomeUnknown {
                    epoch: target_epoch,
                    message: format!("PITR restore publication was not durable: {error}"),
                });
            }
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                return Err(MongrelError::Conflict(format!(
                    "PITR destination already exists: {}",
                    prepared.destination.display()
                )));
            }
            return Err(error.into());
        }
        // FND-006: the restore is published; the caller still sees a hook
        // failure as an error even though the destination is complete.
        crate::catalog::inject_hook("snapshot.install.after")?;
        Ok((target_epoch, restore_report))
    })();
    if outcome.is_err() {
        let _ = prepared.parent.remove_directory_all(&prepared.stage_name);
    }
    outcome
}

/// Post-restore validation pass over the recovered staging tree (Stage 1G).
/// WAL replay never rewrites immutable `.sr` payloads, so every base-manifest
/// run still present must match its recorded size and SHA-256 (a mismatch is
/// staging corruption and fails the restore); runs removed by replayed DDL
/// are reported as issues. The replayed catalog must still decode.
fn validate_staged_restore(
    stage: &DurableRoot,
    backup_manifest: &crate::backup::BackupManifest,
    kek: Option<&crate::encryption::Kek>,
) -> Result<crate::backup::RestoreReport> {
    let mut report = crate::backup::RestoreReport {
        manifest_consistent: true,
        ..crate::backup::RestoreReport::default()
    };
    for file in &backup_manifest.files {
        if file
            .path
            .extension()
            .and_then(|extension| extension.to_str())
            != Some("sr")
        {
            continue;
        }
        let mut source = match stage.open_regular(&file.path) {
            Ok(source) => source,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                report.issues.push(format!(
                    "run {} was removed by replayed DDL",
                    file.path.display()
                ));
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        if source.metadata()?.len() != file.bytes {
            return invalid_pitr(format!(
                "restored run {} size mismatch",
                file.path.display()
            ));
        }
        let actual = crate::backup::sha256_open_file_inner(&mut source, None)?;
        if actual != file.sha256 {
            return invalid_pitr(format!(
                "restored run {} checksum mismatch",
                file.path.display()
            ));
        }
        report.files_checked += 1;
        report.files_ok += 1;
        report.bytes_checked += file.bytes;
    }
    let meta_dek = crate::encryption::meta_dek_for(kek);
    match crate::catalog::read_durable(stage, meta_dek.as_ref())? {
        Some(_) => report.catalog_loaded = true,
        None => return invalid_pitr("restored catalog does not decode after replay"),
    }
    Ok(report)
}

fn validate_manifest_structure(manifest: &PitrArchiveManifest) -> Result<()> {
    if !matches!(
        manifest.format_version,
        LEGACY_FORMAT_VERSION | FORMAT_VERSION
    ) {
        return invalid_pitr(format!(
            "unsupported PITR archive version {}",
            manifest.format_version
        ));
    }
    if manifest.archived_through_epoch < manifest.base_epoch {
        return invalid_pitr("PITR archive watermark predates its base backup");
    }
    if manifest.last_commit_unix_nanos < manifest.base_unix_nanos {
        return invalid_pitr("PITR archive timestamp predates its base backup");
    }

    let v2 = manifest.format_version == FORMAT_VERSION;
    let genesis = if v2 {
        validate_sha256(
            &manifest.base_backup_sha256,
            "PITR base backup manifest checksum",
        )?;
        genesis_chain(
            manifest.base_epoch,
            manifest.base_unix_nanos,
            &manifest.base_backup_sha256,
        )?
    } else {
        String::new()
    };
    if v2 {
        validate_sha256(&manifest.chain_sha256, "PITR chain head")?;
        match (&manifest.authentication, manifest.encrypted) {
            (Some(authentication), true) => {
                validate_sha256(authentication, "PITR manifest authentication")?;
            }
            (None, false) => {}
            (None, true) => {
                return invalid_pitr("encrypted PITR manifest lacks authentication");
            }
            (Some(_), false) => {
                return invalid_pitr("plaintext PITR manifest has unexpected authentication");
            }
        }
    } else if manifest.encrypted
        || !manifest.base_backup_sha256.is_empty()
        || !manifest.chain_sha256.is_empty()
        || manifest.authentication.is_some()
    {
        return invalid_pitr("legacy PITR manifest contains version 2 fields");
    }

    let mut expected_from = manifest.base_epoch;
    let mut previous_commit_epoch = manifest.base_epoch;
    let mut previous_timestamp = manifest.base_unix_nanos;
    let mut previous_chain = genesis;
    let mut previous_sequence = None;
    for reference in &manifest.chunks {
        validate_chunk_reference_path(reference)?;
        if reference.from_epoch != expected_from {
            return invalid_pitr(format!(
                "PITR chunk {} is not contiguous: expected from_epoch {expected_from}, got {}",
                reference.file, reference.from_epoch
            ));
        }
        if reference.through_epoch <= reference.from_epoch {
            return invalid_pitr(format!(
                "PITR chunk {} has an empty or reversed epoch range",
                reference.file
            ));
        }
        if reference.records == 0 || reference.bytes == 0 || reference.bytes > MAX_CHUNK_BYTES {
            return invalid_pitr(format!(
                "PITR chunk {} has invalid record or byte counts",
                reference.file
            ));
        }
        validate_sha256(&reference.sha256, "PITR chunk checksum")?;
        if reference.commits.is_empty() {
            return invalid_pitr(format!(
                "PITR chunk {} has no commit points",
                reference.file
            ));
        }
        if !reference.commit_ledger.is_empty()
            && reference.commit_ledger.len() != reference.commits.len()
        {
            return invalid_pitr(format!(
                "PITR chunk {} commit ledger does not match its commit points",
                reference.file
            ));
        }
        for commit in &reference.commits {
            if commit.epoch <= previous_commit_epoch
                || commit.epoch <= reference.from_epoch
                || commit.epoch > reference.through_epoch
            {
                return invalid_pitr(format!(
                    "PITR chunk {} has an invalid commit epoch {}",
                    reference.file, commit.epoch
                ));
            }
            if commit.unix_nanos < previous_timestamp {
                return invalid_pitr(format!(
                    "PITR chunk {} has a decreasing commit timestamp",
                    reference.file
                ));
            }
            previous_commit_epoch = commit.epoch;
            previous_timestamp = commit.unix_nanos;
        }
        if previous_commit_epoch != reference.through_epoch {
            return invalid_pitr(format!(
                "PITR chunk {} does not end at its final commit",
                reference.file
            ));
        }
        if v2 {
            if reference.first_sequence > reference.last_sequence
                || reference
                    .last_sequence
                    .checked_sub(reference.first_sequence)
                    .and_then(|span| span.checked_add(1))
                    != u64::try_from(reference.records).ok()
                || previous_sequence.is_some_and(|previous| reference.first_sequence <= previous)
            {
                return invalid_pitr(format!(
                    "PITR chunk {} has an invalid record sequence range",
                    reference.file
                ));
            }
            validate_sha256(&reference.previous_chain_sha256, "PITR previous chain hash")?;
            validate_sha256(&reference.chain_sha256, "PITR chain hash")?;
            if reference.previous_chain_sha256 != previous_chain {
                return invalid_pitr(format!(
                    "PITR chunk {} breaks the previous-chain link",
                    reference.file
                ));
            }
            let expected_chain = next_chain(
                &previous_chain,
                &reference.sha256,
                reference.from_epoch,
                reference.through_epoch,
                reference.records,
                reference.first_sequence,
                reference.last_sequence,
            )?;
            if reference.chain_sha256 != expected_chain {
                return invalid_pitr(format!(
                    "PITR chunk {} has an invalid chain hash",
                    reference.file
                ));
            }
            previous_chain = expected_chain;
            previous_sequence = Some(reference.last_sequence);
        } else if !reference.previous_chain_sha256.is_empty()
            || !reference.chain_sha256.is_empty()
            || reference.first_sequence != 0
            || reference.last_sequence != 0
            || !reference.commit_ledger.is_empty()
        {
            return invalid_pitr(format!(
                "legacy PITR chunk {} contains version 2 chain fields",
                reference.file
            ));
        }
        expected_from = reference.through_epoch;
    }

    if expected_from != manifest.archived_through_epoch {
        return invalid_pitr("PITR archive watermark does not match its final chunk");
    }
    if previous_timestamp != manifest.last_commit_unix_nanos {
        return invalid_pitr("PITR archive timestamp does not match its final commit");
    }
    if v2 && manifest.chain_sha256 != previous_chain {
        return invalid_pitr("PITR manifest chain head does not match its chunks");
    }
    Ok(())
}

fn validate_chunk_reference_path(reference: &PitrChunkRef) -> Result<()> {
    let path = Path::new(&reference.file);
    if path.components().count() != 1
        || !matches!(path.components().next(), Some(Component::Normal(_)))
        || reference.file != chunk_file_name(reference.from_epoch, reference.through_epoch)
    {
        return invalid_pitr(format!("invalid PITR chunk path {:?}", reference.file));
    }
    Ok(())
}

fn chunk_file_name(from_epoch: u64, through_epoch: u64) -> String {
    format!("wal-{from_epoch:020}-{through_epoch:020}.bin")
}

fn genesis_chain(
    base_epoch: u64,
    base_unix_nanos: u64,
    base_backup_sha256: &str,
) -> Result<String> {
    let base_backup = decode_sha256(base_backup_sha256, "PITR base backup manifest checksum")?;
    let mut hasher = Sha256::new();
    hasher.update(GENESIS_DOMAIN);
    hasher.update(base_epoch.to_be_bytes());
    hasher.update(base_unix_nanos.to_be_bytes());
    hasher.update(base_backup);
    Ok(hex_bytes(&hasher.finalize()))
}

fn next_chain(
    previous_chain_sha256: &str,
    chunk_sha256: &str,
    from_epoch: u64,
    through_epoch: u64,
    records: usize,
    first_sequence: u64,
    last_sequence: u64,
) -> Result<String> {
    let previous = decode_sha256(previous_chain_sha256, "PITR previous chain hash")?;
    let chunk = decode_sha256(chunk_sha256, "PITR chunk checksum")?;
    let records = u64::try_from(records)
        .map_err(|_| MongrelError::InvalidArgument("PITR record count is too large".into()))?;
    let mut hasher = Sha256::new();
    hasher.update(CHAIN_DOMAIN);
    hasher.update(previous);
    hasher.update(chunk);
    hasher.update(from_epoch.to_be_bytes());
    hasher.update(through_epoch.to_be_bytes());
    hasher.update(records.to_be_bytes());
    hasher.update(first_sequence.to_be_bytes());
    hasher.update(last_sequence.to_be_bytes());
    Ok(hex_bytes(&hasher.finalize()))
}

fn encode_chunk_v2(chunk: &PitrChunkV2, kek: Option<&crate::encryption::Kek>) -> Result<Vec<u8>> {
    let plaintext = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .with_limit(MAX_CHUNK_BYTES)
        .serialize(chunk)?;
    let (encrypted, nonce, payload) = match kek {
        Some(kek) => encrypt_chunk_payload(kek, &plaintext)?,
        None => (false, None, plaintext),
    };
    let envelope = PitrChunkEnvelopeV2 {
        format_version: FORMAT_VERSION,
        encrypted,
        nonce,
        payload,
    };
    let bytes = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .with_limit(MAX_CHUNK_BYTES)
        .serialize(&envelope)?;
    if bytes.len() as u64 > MAX_CHUNK_BYTES {
        return invalid_pitr("PITR chunk exceeds maximum size");
    }
    Ok(bytes)
}

fn encode_or_reuse_chunk_v2(
    root: &DurableRoot,
    path: &Path,
    expected: &PitrChunkV2,
    kek: Option<&crate::encryption::Kek>,
) -> Result<Vec<u8>> {
    let source = match root.open_regular(path) {
        Ok(source) => source,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return encode_chunk_v2(expected, kek)
        }
        Err(error) => return Err(error.into()),
    };
    let length = source.metadata()?.len();
    if length == 0 || length > MAX_CHUNK_BYTES {
        return Err(MongrelError::Conflict(format!(
            "PITR orphan chunk {} has an invalid length",
            path.display()
        )));
    }
    let mut bytes = Vec::with_capacity(length as usize);
    source
        .take(MAX_CHUNK_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_CHUNK_BYTES {
        return Err(MongrelError::Conflict(format!(
            "PITR orphan chunk {} exceeds maximum size",
            path.display()
        )));
    }
    let decoded = decode_chunk(FORMAT_VERSION, kek.is_some(), &bytes, kek).map_err(|error| {
        MongrelError::Conflict(format!(
            "PITR orphan chunk {} cannot be reused: {error}",
            path.display()
        ))
    })?;
    let expected_records = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .serialize(&expected.records)?;
    let actual_records = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .serialize(&decoded.records)?;
    if decoded.from_epoch != expected.from_epoch
        || decoded.through_epoch != expected.through_epoch
        || decoded.commits != expected.commits
        || decoded.first_sequence != Some(expected.first_sequence)
        || decoded.last_sequence != Some(expected.last_sequence)
        || decoded.previous_chain_sha256.as_deref() != Some(expected.previous_chain_sha256.as_str())
        || actual_records != expected_records
    {
        return Err(MongrelError::Conflict(format!(
            "PITR orphan chunk {} does not match retry payload",
            path.display()
        )));
    }
    Ok(bytes)
}

#[cfg(feature = "encryption")]
fn encrypt_chunk_payload(
    kek: &crate::encryption::Kek,
    plaintext: &[u8],
) -> Result<(bool, Option<[u8; 12]>, Vec<u8>)> {
    use crate::encryption::Cipher as _;

    let mut nonce = [0u8; 12];
    crate::encryption::fill_random(&mut nonce)?;
    let key = kek.derive_subkey(CHUNK_KEY_DOMAIN);
    let cipher = crate::encryption::AesCipher::new(key.as_ref())?;
    Ok((true, Some(nonce), cipher.encrypt_page(&nonce, plaintext)?))
}

#[cfg(not(feature = "encryption"))]
fn encrypt_chunk_payload(
    _kek: &crate::encryption::Kek,
    _plaintext: &[u8],
) -> Result<(bool, Option<[u8; 12]>, Vec<u8>)> {
    unreachable!("Kek is unconstructable without the encryption feature")
}

fn decode_chunk(
    format_version: u16,
    encrypted: bool,
    bytes: &[u8],
    kek: Option<&crate::encryption::Kek>,
) -> Result<DecodedPitrChunk> {
    if format_version == LEGACY_FORMAT_VERSION {
        let chunk: LegacyPitrChunk = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .reject_trailing_bytes()
            .with_limit(MAX_CHUNK_BYTES)
            .deserialize(bytes)?;
        if chunk.format_version != LEGACY_FORMAT_VERSION {
            return invalid_pitr(format!(
                "unsupported legacy PITR chunk version {}",
                chunk.format_version
            ));
        }
        return Ok(DecodedPitrChunk {
            from_epoch: chunk.from_epoch,
            through_epoch: chunk.through_epoch,
            records: chunk.records,
            commits: chunk.commits,
            first_sequence: None,
            last_sequence: None,
            previous_chain_sha256: None,
        });
    }

    let envelope: PitrChunkEnvelopeV2 = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .with_limit(MAX_CHUNK_BYTES)
        .deserialize(bytes)?;
    if envelope.format_version != FORMAT_VERSION || envelope.encrypted != encrypted {
        return invalid_pitr("PITR chunk envelope does not match its manifest");
    }
    let plaintext = match (envelope.encrypted, envelope.nonce, kek) {
        (false, None, None) => envelope.payload,
        (true, Some(nonce), Some(kek)) => decrypt_chunk_payload(kek, &nonce, &envelope.payload)?,
        (true, None, _) => return invalid_pitr("encrypted PITR chunk lacks a nonce"),
        (true, Some(_), None) => {
            return Err(MongrelError::Encryption(
                "encrypted PITR chunk requires its database passphrase".into(),
            ));
        }
        (false, Some(_), _) => return invalid_pitr("plaintext PITR chunk has a nonce"),
        (false, None, Some(_)) => {
            return invalid_pitr("plaintext PITR chunk was opened with an encryption key");
        }
    };
    let chunk: PitrChunkV2 = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .with_limit(MAX_CHUNK_BYTES)
        .deserialize(&plaintext)?;
    if chunk.format_version != FORMAT_VERSION {
        return invalid_pitr(format!(
            "unsupported PITR chunk version {}",
            chunk.format_version
        ));
    }
    Ok(DecodedPitrChunk {
        from_epoch: chunk.from_epoch,
        through_epoch: chunk.through_epoch,
        records: chunk.records,
        commits: chunk.commits,
        first_sequence: Some(chunk.first_sequence),
        last_sequence: Some(chunk.last_sequence),
        previous_chain_sha256: Some(chunk.previous_chain_sha256),
    })
}

#[cfg(feature = "encryption")]
fn decrypt_chunk_payload(
    kek: &crate::encryption::Kek,
    nonce: &[u8; 12],
    ciphertext: &[u8],
) -> Result<Vec<u8>> {
    use crate::encryption::Cipher as _;

    let key = kek.derive_subkey(CHUNK_KEY_DOMAIN);
    crate::encryption::AesCipher::new(key.as_ref())?.decrypt_page(nonce, ciphertext)
}

#[cfg(not(feature = "encryption"))]
fn decrypt_chunk_payload(
    _kek: &crate::encryption::Kek,
    _nonce: &[u8; 12],
    _ciphertext: &[u8],
) -> Result<Vec<u8>> {
    unreachable!("Kek is unconstructable without the encryption feature")
}

fn validate_chunk(
    reference: &PitrChunkRef,
    chunk: &DecodedPitrChunk,
    format_version: u16,
    preceding_sequence: Option<u64>,
) -> Result<Option<u64>> {
    if chunk.from_epoch != reference.from_epoch
        || chunk.through_epoch != reference.through_epoch
        || chunk.records.len() != reference.records
        || chunk.commits != reference.commits
        || (format_version == FORMAT_VERSION
            && (chunk.first_sequence != Some(reference.first_sequence)
                || chunk.last_sequence != Some(reference.last_sequence)))
    {
        return invalid_pitr(format!(
            "PITR chunk {} body does not match its manifest reference",
            reference.file
        ));
    }
    match (format_version, &chunk.previous_chain_sha256) {
        (FORMAT_VERSION, Some(previous)) if previous == &reference.previous_chain_sha256 => {}
        (LEGACY_FORMAT_VERSION, None) => {}
        _ => {
            return invalid_pitr(format!(
                "PITR chunk {} body has an invalid previous-chain link",
                reference.file
            ));
        }
    }

    let mut seen = HashSet::new();
    let mut committed = HashSet::new();
    let mut commit_epochs = Vec::new();
    let mut commit_txns = Vec::new();
    let mut commit_sequences = Vec::new();
    let mut commit_timestamps = HashMap::new();
    let mut previous_sequence = preceding_sequence.map(Epoch);
    for record in &chunk.records {
        if record.txn_id == crate::wal::SYSTEM_TXN_ID {
            return invalid_pitr(format!(
                "PITR chunk {} contains a system transaction",
                reference.file
            ));
        }
        let invalid_sequence = previous_sequence.is_some_and(|previous: Epoch| {
            if format_version == FORMAT_VERSION {
                record.seq <= previous
            } else {
                record.seq < previous
            }
        });
        if invalid_sequence {
            return invalid_pitr(format!(
                "PITR chunk {} has duplicate or decreasing record sequence numbers",
                reference.file
            ));
        }
        previous_sequence = Some(record.seq);
        if committed.contains(&record.txn_id) {
            return invalid_pitr(format!(
                "PITR chunk {} contains records after a transaction commit",
                reference.file
            ));
        }
        seen.insert(record.txn_id);
        if let Op::CommitTimestamp { unix_nanos } = record.op {
            if commit_timestamps
                .insert(record.txn_id, unix_nanos)
                .is_some()
            {
                return invalid_pitr(format!(
                    "PITR chunk {} contains duplicate commit timestamps",
                    reference.file
                ));
            }
        }
        if let Op::TxnCommit { epoch, .. } = record.op {
            if !committed.insert(record.txn_id) {
                return invalid_pitr(format!(
                    "PITR chunk {} contains a duplicate transaction commit",
                    reference.file
                ));
            }
            commit_epochs.push(epoch);
            commit_txns.push(record.txn_id);
            commit_sequences.push(record.seq.0);
        }
    }
    if seen != committed {
        return invalid_pitr(format!(
            "PITR chunk {} contains an incomplete transaction",
            reference.file
        ));
    }
    let expected_epochs = reference
        .commits
        .iter()
        .map(|commit| commit.epoch)
        .collect::<Vec<_>>();
    if commit_epochs != expected_epochs {
        return invalid_pitr(format!(
            "PITR chunk {} commit markers do not match its manifest",
            reference.file
        ));
    }
    for (index, txn_id) in commit_txns.into_iter().enumerate() {
        if commit_timestamps
            .get(&txn_id)
            .is_some_and(|timestamp| *timestamp != reference.commits[index].unix_nanos)
        {
            return invalid_pitr(format!(
                "PITR chunk {} commit timestamp does not match its body",
                reference.file
            ));
        }
        // The Stage 1G ledger is cross-checked only where recorded; an empty
        // ledger (pre-1G archive) skips the check entirely.
        if let Some(entry) = reference.commit_ledger.get(index) {
            if entry.txn_id != txn_id {
                return invalid_pitr(format!(
                    "PITR chunk {} commit transaction id does not match its body",
                    reference.file
                ));
            }
            if entry.sequence != commit_sequences[index] {
                return invalid_pitr(format!(
                    "PITR chunk {} commit log position does not match its body",
                    reference.file
                ));
            }
        }
    }
    if format_version == FORMAT_VERSION
        && (chunk.records.first().map(|record| record.seq.0) != Some(reference.first_sequence)
            || chunk.records.last().map(|record| record.seq.0) != Some(reference.last_sequence))
    {
        return invalid_pitr(format!(
            "PITR chunk {} record sequence bounds do not match its body",
            reference.file
        ));
    }
    Ok(previous_sequence.map(|sequence| sequence.0))
}

fn validate_archive_key(
    manifest: &PitrArchiveManifest,
    kek: Option<&crate::encryption::Kek>,
) -> Result<()> {
    if manifest.format_version == LEGACY_FORMAT_VERSION && kek.is_some() {
        return Err(MongrelError::Conflict(
            "encrypted legacy PITR archives are unsupported; create a version 2 archive".into(),
        ));
    }
    if manifest.encrypted != kek.is_some() {
        return Err(MongrelError::Conflict(
            "PITR archive encryption does not match the source database".into(),
        ));
    }
    Ok(())
}

fn derive_archive_kek(
    archive: &DurableRoot,
    manifest: &PitrArchiveManifest,
    credentials: PitrCredentials<'_>,
) -> Result<Option<crate::encryption::Kek>> {
    let salt_path = Path::new("base").join("_meta").join("keys");
    let mut salt_file = match archive.open_regular(&salt_path) {
        Ok(file) => Some(file),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let encrypted_base = salt_file.is_some();
    if manifest.format_version == LEGACY_FORMAT_VERSION && encrypted_base {
        return Err(MongrelError::Conflict(
            "encrypted legacy PITR archives cannot be restored safely; create a version 2 archive"
                .into(),
        ));
    }
    if manifest.format_version == FORMAT_VERSION && manifest.encrypted != encrypted_base {
        return invalid_pitr("PITR manifest encryption does not match its base backup");
    }
    if !encrypted_base {
        if matches!(
            credentials,
            PitrCredentials::Encryption(_) | PitrCredentials::EncryptionAndUser { .. }
        ) {
            return invalid_pitr("plaintext PITR archive does not accept encryption credentials");
        }
        return Ok(None);
    }
    let passphrase = match credentials {
        PitrCredentials::Encryption(passphrase)
        | PitrCredentials::EncryptionAndUser { passphrase, .. } => passphrase,
        PitrCredentials::None | PitrCredentials::User { .. } => {
            return Err(MongrelError::Encryption(
                "encrypted PITR archive requires its database passphrase".into(),
            ));
        }
    };
    derive_kek_from_salt(
        salt_file
            .as_mut()
            .ok_or_else(|| MongrelError::Encryption("missing PITR encryption salt".into()))?,
        passphrase,
    )
    .map(Some)
}

#[cfg(feature = "encryption")]
fn derive_kek_from_salt(
    source: &mut std::fs::File,
    passphrase: &str,
) -> Result<crate::encryption::Kek> {
    let mut salt = [0u8; crate::encryption::SALT_LEN];
    source.read_exact(&mut salt)?;
    let mut extra = [0u8; 1];
    if source.read(&mut extra)? != 0 {
        return Err(MongrelError::Encryption(
            "invalid PITR base encryption salt length".into(),
        ));
    }
    crate::encryption::Kek::derive(passphrase, &salt)
}

#[cfg(not(feature = "encryption"))]
fn derive_kek_from_salt(
    _source: &mut std::fs::File,
    _passphrase: &str,
) -> Result<crate::encryption::Kek> {
    Err(MongrelError::Encryption(
        "encryption feature is disabled".into(),
    ))
}

fn verify_manifest_authentication(
    manifest: &PitrArchiveManifest,
    kek: Option<&crate::encryption::Kek>,
) -> Result<()> {
    if manifest.format_version == LEGACY_FORMAT_VERSION || !manifest.encrypted {
        return Ok(());
    }
    verify_manifest_mac(
        manifest,
        kek.ok_or_else(|| {
            MongrelError::Encryption("encrypted PITR archive requires an encryption key".into())
        })?,
    )
}

fn manifest_authentication(
    manifest: &PitrArchiveManifest,
    kek: Option<&crate::encryption::Kek>,
) -> Result<Option<String>> {
    if manifest.format_version == LEGACY_FORMAT_VERSION || !manifest.encrypted {
        return Ok(None);
    }
    sign_manifest_mac(
        manifest,
        kek.ok_or_else(|| {
            MongrelError::Encryption("encrypted PITR archive requires an encryption key".into())
        })?,
    )
    .map(Some)
}

#[cfg(feature = "encryption")]
fn manifest_auth_bytes(manifest: &PitrArchiveManifest) -> Result<Vec<u8>> {
    let mut unsigned = manifest.clone();
    unsigned.authentication = None;
    serde_json::to_vec(&unsigned)
        .map_err(|error| MongrelError::Other(format!("PITR manifest encode: {error}")))
}

#[cfg(feature = "encryption")]
fn sign_manifest_mac(
    manifest: &PitrArchiveManifest,
    kek: &crate::encryption::Kek,
) -> Result<String> {
    use hmac::Mac as _;

    let key = kek.derive_subkey(MANIFEST_KEY_DOMAIN);
    let mut mac = <hmac::Hmac<Sha256> as hmac::Mac>::new_from_slice(key.as_ref())
        .map_err(|error| MongrelError::Encryption(format!("PITR HMAC key: {error}")))?;
    mac.update(MANIFEST_AUTH_DOMAIN);
    mac.update(&manifest_auth_bytes(manifest)?);
    Ok(hex_bytes(&mac.finalize().into_bytes()))
}

#[cfg(not(feature = "encryption"))]
fn sign_manifest_mac(
    _manifest: &PitrArchiveManifest,
    _kek: &crate::encryption::Kek,
) -> Result<String> {
    unreachable!("Kek is unconstructable without the encryption feature")
}

#[cfg(feature = "encryption")]
fn verify_manifest_mac(manifest: &PitrArchiveManifest, kek: &crate::encryption::Kek) -> Result<()> {
    use hmac::Mac as _;

    let authentication = manifest
        .authentication
        .as_deref()
        .ok_or_else(|| MongrelError::InvalidArgument("missing PITR authentication".into()))?;
    let expected = decode_sha256(authentication, "PITR manifest authentication")?;
    let key = kek.derive_subkey(MANIFEST_KEY_DOMAIN);
    let mut mac = <hmac::Hmac<Sha256> as hmac::Mac>::new_from_slice(key.as_ref())
        .map_err(|error| MongrelError::Encryption(format!("PITR HMAC key: {error}")))?;
    mac.update(MANIFEST_AUTH_DOMAIN);
    mac.update(&manifest_auth_bytes(manifest)?);
    mac.verify_slice(&expected)
        .map_err(|_| MongrelError::Decryption("PITR manifest authentication failed".into()))
}

#[cfg(not(feature = "encryption"))]
fn verify_manifest_mac(
    _manifest: &PitrArchiveManifest,
    _kek: &crate::encryption::Kek,
) -> Result<()> {
    unreachable!("Kek is unconstructable without the encryption feature")
}

fn validate_sha256(value: &str, label: &str) -> Result<()> {
    decode_sha256(value, label).map(|_| ())
}

fn decode_sha256(value: &str, label: &str) -> Result<[u8; 32]> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return invalid_pitr(format!("{label} is not lowercase SHA-256 hex"));
    }
    let mut bytes = [0u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]);
    }
    Ok(bytes)
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => unreachable!("validated hex digit"),
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

fn invalid_pitr<T>(message: impl Into<String>) -> Result<T> {
    Err(MongrelError::InvalidArgument(message.into()))
}

fn materialize_spilled_records(db: &Database, records: Vec<Record>) -> Result<Vec<Record>> {
    let table_schemas = db
        .catalog_snapshot()
        .tables
        .into_iter()
        .map(|entry| (entry.table_id, entry.schema))
        .collect::<HashMap<_, _>>();
    let commit_epochs: HashMap<u64, u64> = records
        .iter()
        .filter_map(|record| match record.op {
            Op::TxnCommit { epoch, .. } => Some((record.txn_id, epoch)),
            _ => None,
        })
        .collect();
    let logical_spills = records
        .iter()
        .filter_map(|record| match &record.op {
            Op::SpilledRows { table_id, .. } => Some((record.txn_id, *table_id)),
            _ => None,
        })
        .collect::<HashSet<_>>();
    let mut output = Vec::with_capacity(records.len());
    for record in records {
        match record.op {
            Op::SpilledRows { table_id, rows } => output.push(Record::new(
                record.seq,
                record.txn_id,
                Op::Put { table_id, rows },
            )),
            Op::TxnCommit {
                epoch,
                mut added_runs,
            } => {
                for run in &added_runs {
                    if logical_spills.contains(&(record.txn_id, run.table_id)) {
                        continue;
                    }
                    let schema = table_schemas.get(&run.table_id).ok_or_else(|| {
                        MongrelError::Conflict(format!(
                            "PITR cannot materialize spilled run {} for unavailable table {}",
                            run.run_id, run.table_id
                        ))
                    })?;
                    let run_path = db
                        .root()
                        .join(crate::database::TABLES_DIR)
                        .join(run.table_id.to_string())
                        .join(crate::engine::RUNS_DIR)
                        .join(format!("r-{}.sr", run.run_id));
                    let mut reader = crate::sorted_run::RunReader::open(
                        run_path,
                        schema.clone(),
                        db.kek().cloned(),
                    )?;
                    let mut rows = reader.all_rows()?;
                    for row in &mut rows {
                        row.committed_epoch = Epoch(epoch);
                    }
                    output.push(Record::new(
                        record.seq,
                        record.txn_id,
                        Op::Put {
                            table_id: run.table_id,
                            rows: bincode::serialize(&rows)?,
                        },
                    ));
                }
                added_runs.clear();
                output.push(Record::new(
                    record.seq,
                    record.txn_id,
                    Op::TxnCommit { epoch, added_runs },
                ));
            }
            op => output.push(Record::new(record.seq, record.txn_id, op)),
        }
    }
    let complete: HashSet<u64> = output
        .iter()
        .filter_map(|record| match record.op {
            Op::TxnCommit { .. } => Some(record.txn_id),
            _ => None,
        })
        .collect();
    if commit_epochs
        .keys()
        .any(|txn_id| !complete.contains(txn_id))
    {
        return Err(MongrelError::Conflict(
            "PITR conversion lost a transaction commit".into(),
        ));
    }
    Ok(output)
}

fn normalize_record_sequences(records: &mut [Record], minimum: Option<u64>) -> Result<()> {
    let original = records
        .first()
        .map(|record| record.seq.0)
        .ok_or_else(|| MongrelError::Conflict("PITR batch has no records".into()))?;
    let start = minimum.map_or(original, |minimum| minimum.max(original));
    for (offset, record) in records.iter_mut().enumerate() {
        record.seq = Epoch(
            start
                .checked_add(u64::try_from(offset).map_err(|_| {
                    MongrelError::Conflict("PITR record sequence space exhausted".into())
                })?)
                .ok_or_else(|| {
                    MongrelError::Conflict("PITR record sequence space exhausted".into())
                })?,
        );
    }
    Ok(())
}

fn resolve_target_epoch(manifest: &PitrArchiveManifest, target: PitrTarget) -> Result<u64> {
    match target {
        PitrTarget::Latest => Ok(manifest.archived_through_epoch),
        PitrTarget::Epoch(epoch)
            if epoch >= manifest.base_epoch && epoch <= manifest.archived_through_epoch =>
        {
            Ok(manifest
                .chunks
                .iter()
                .flat_map(|chunk| &chunk.commits)
                .filter(|commit| commit.epoch <= epoch)
                .map(|commit| commit.epoch)
                .max()
                .unwrap_or(manifest.base_epoch))
        }
        PitrTarget::Epoch(epoch) => Err(MongrelError::InvalidArgument(format!(
            "PITR epoch {epoch} outside archive range {}..={}",
            manifest.base_epoch, manifest.archived_through_epoch
        ))),
        PitrTarget::TimestampNanos(timestamp) => {
            if timestamp < manifest.base_unix_nanos {
                return Err(MongrelError::InvalidArgument(
                    "PITR timestamp predates base backup".into(),
                ));
            }
            let mut epoch = manifest.base_epoch;
            for commit in manifest.chunks.iter().flat_map(|chunk| &chunk.commits) {
                if commit.unix_nanos > timestamp {
                    break;
                }
                epoch = commit.epoch;
            }
            Ok(epoch)
        }
        PitrTarget::TransactionId(txn_id) => manifest
            .chunks
            .iter()
            .flat_map(|chunk| chunk.commit_ledger.iter().zip(&chunk.commits))
            .find(|(entry, _)| entry.txn_id == txn_id)
            .map(|(_, commit)| commit.epoch)
            .ok_or_else(|| {
                MongrelError::InvalidArgument(format!(
                    "PITR transaction id {txn_id} is not in the archive commit ledger"
                ))
            }),
        PitrTarget::LogPosition(position) => {
            let mut epoch = None;
            let mut ledger_has_positions = false;
            for chunk in &manifest.chunks {
                for (entry, commit) in chunk.commit_ledger.iter().zip(&chunk.commits) {
                    ledger_has_positions = true;
                    if entry.sequence <= position {
                        epoch = Some(commit.epoch);
                    }
                }
            }
            if !ledger_has_positions {
                return Err(MongrelError::InvalidArgument(
                    "PITR archive commit ledger records no log positions".into(),
                ));
            }
            // A position below the first archived commit lies inside the base
            // backup; commits are atomic, so the base boundary is the answer.
            Ok(epoch.unwrap_or(manifest.base_epoch))
        }
    }
}

fn load_records_through(
    archive: &DurableRoot,
    manifest: &PitrArchiveManifest,
    target_epoch: u64,
    kek: Option<&crate::encryption::Kek>,
) -> Result<Vec<Record>> {
    let mut records = Vec::new();
    let mut previous_sequence = None;
    for reference in &manifest.chunks {
        let source = archive.open_regular(&reference.file)?;
        let length = source.metadata()?.len();
        if length != reference.bytes || length > MAX_CHUNK_BYTES {
            return Err(MongrelError::Other(format!(
                "PITR chunk {} length mismatch",
                reference.file
            )));
        }
        let mut bytes = Vec::with_capacity(length as usize);
        source
            .take(reference.bytes.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 != reference.bytes || sha256_bytes(&bytes) != reference.sha256 {
            return Err(MongrelError::Other(format!(
                "PITR chunk {} checksum mismatch",
                reference.file
            )));
        }
        let chunk = decode_chunk(manifest.format_version, manifest.encrypted, &bytes, kek)?;
        previous_sequence = validate_chunk(
            reference,
            &chunk,
            manifest.format_version,
            previous_sequence,
        )?;
        if reference.from_epoch < target_epoch {
            let selected: HashSet<u64> = chunk
                .records
                .iter()
                .filter_map(|record| match record.op {
                    Op::TxnCommit { epoch, .. } if epoch <= target_epoch => Some(record.txn_id),
                    _ => None,
                })
                .collect();
            records.extend(
                chunk
                    .records
                    .into_iter()
                    .filter(|record| selected.contains(&record.txn_id)),
            );
        }
    }
    Ok(records)
}

fn open_recovery_staging(root: &DurableRoot, credentials: PitrCredentials<'_>) -> Result<Database> {
    match credentials {
        PitrCredentials::None | PitrCredentials::User { .. } => {
            Database::open_replica_recovery_durable(root)
        }
        #[cfg(feature = "encryption")]
        PitrCredentials::Encryption(passphrase) => {
            Database::open_encrypted_replica_recovery_durable(root, passphrase)
        }
        #[cfg(not(feature = "encryption"))]
        PitrCredentials::Encryption(_) => Err(MongrelError::Encryption(
            "encryption feature is disabled".into(),
        )),
        #[cfg(feature = "encryption")]
        PitrCredentials::EncryptionAndUser { passphrase, .. } => {
            Database::open_encrypted_replica_recovery_durable(root, passphrase)
        }
        #[cfg(not(feature = "encryption"))]
        PitrCredentials::EncryptionAndUser { .. } => Err(MongrelError::Encryption(
            "encryption feature is disabled".into(),
        )),
    }
}

fn validate_target_user_credentials(
    database: &Database,
    credentials: PitrCredentials<'_>,
) -> Result<()> {
    let (username, password) = match credentials {
        PitrCredentials::User { username, password }
        | PitrCredentials::EncryptionAndUser {
            username, password, ..
        } => (username, password),
        PitrCredentials::None | PitrCredentials::Encryption(_) => return Ok(()),
    };
    if !database.require_auth_enabled() {
        return Err(MongrelError::AuthNotRequired);
    }
    if database.verify_user(username, password)?.is_some() {
        Ok(())
    } else {
        Err(MongrelError::InvalidCredentials {
            username: username.to_string(),
        })
    }
}

struct PreparedDestination {
    destination: PathBuf,
    parent: DurableRoot,
    destination_name: PathBuf,
    stage_name: PathBuf,
}

fn prepare_destination(path: &Path, label: &str) -> Result<PreparedDestination> {
    let name = path
        .file_name()
        .ok_or_else(|| MongrelError::InvalidArgument("invalid destination".into()))?;
    let requested_parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    crate::durable_file::create_directory_all(requested_parent)?;
    let parent = DurableRoot::open(requested_parent)?;
    let destination_name = PathBuf::from(name);
    if parent.entry_exists(&destination_name)? {
        return Err(MongrelError::Conflict(format!(
            "destination already exists: {}",
            path.display()
        )));
    }
    for _ in 0..128 {
        let mut nonce = [0u8; 12];
        crate::encryption::fill_random(&mut nonce)?;
        let suffix = hex_bytes(&nonce);
        let stage_name = PathBuf::from(format!(
            ".{}.{}-{}-{suffix}",
            name.to_string_lossy(),
            label,
            std::process::id(),
        ));
        match parent.create_directory_new(&stage_name) {
            Ok(()) => {
                return Ok(PreparedDestination {
                    destination: parent.canonical_path().join(&destination_name),
                    parent,
                    destination_name,
                    stage_name,
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err(MongrelError::Conflict(
        "could not allocate PITR staging directory".into(),
    ))
}

fn copy_tree(source: &DurableRoot, destination: &DurableRoot) -> Result<()> {
    source.walk_regular_files(
        |_, _| Ok(true),
        |relative| {
            destination.create_directory_all(relative)?;
            Ok(())
        },
        |relative, source| {
            destination.copy_new_from(relative, source)?;
            Ok(())
        },
    )
}

fn write_manifest(
    root: &DurableRoot,
    manifest: &PitrArchiveManifest,
    kek: Option<&crate::encryption::Kek>,
) -> Result<()> {
    write_manifest_with_after(root, manifest, kek, || {})
}

fn write_manifest_with_after<F>(
    root: &DurableRoot,
    manifest: &PitrArchiveManifest,
    kek: Option<&crate::encryption::Kek>,
    after_publish: F,
) -> Result<()>
where
    F: FnOnce(),
{
    let mut manifest = manifest.clone();
    manifest.authentication = manifest_authentication(&manifest, kek)?;
    validate_manifest_structure(&manifest)?;
    let bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|error| MongrelError::Other(format!("PITR manifest encode: {error}")))?;
    root.write_atomic_with_after(MANIFEST_FILE, &bytes, after_publish)?;
    Ok(())
}

fn finish_manifest_publication(
    result: Result<()>,
    published: bool,
    through_epoch: u64,
) -> Result<()> {
    match result {
        Err(error) if published => Err(MongrelError::CommitOutcomeUnknown {
            epoch: through_epoch,
            message: format!("PITR manifest publication was not durable: {error}"),
        }),
        result => result,
    }
}

fn publish_chunk(root: &DurableRoot, path: &Path, bytes: &[u8], sha256: &str) -> Result<()> {
    match root.write_new(path, bytes) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            verify_existing_chunk(root, path, bytes.len() as u64, sha256)
        }
        Err(error) => Err(error.into()),
    }
}

fn verify_existing_chunk(
    root: &DurableRoot,
    path: &Path,
    expected_bytes: u64,
    expected_sha256: &str,
) -> Result<()> {
    let file = root.open_regular(path)?;
    if file.metadata()?.len() != expected_bytes {
        return Err(MongrelError::Conflict(format!(
            "PITR orphan chunk {} does not match retry payload",
            path.display()
        )));
    }
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut total = 0u64;
    let mut file = file.take(expected_bytes.saturating_add(1));
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        total = total.saturating_add(read as u64);
        hasher.update(&buffer[..read]);
    }
    if total != expected_bytes {
        return Err(MongrelError::Conflict(format!(
            "PITR orphan chunk {} changed while being verified",
            path.display()
        )));
    }
    let actual = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if actual != expected_sha256 {
        return Err(MongrelError::Conflict(format!(
            "PITR orphan chunk {} checksum does not match retry payload",
            path.display()
        )));
    }
    Ok(())
}

fn sha256_bytes(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn unix_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Principal;
    use crate::schema::{ColumnDef, ColumnFlags, Schema, TypeId};

    #[test]
    fn visible_manifest_sync_failure_has_unknown_commit_outcome() {
        let error = finish_manifest_publication(
            Err(MongrelError::Other("parent fsync failed".into())),
            true,
            42,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            MongrelError::CommitOutcomeUnknown { epoch: 42, .. }
        ));

        let error = finish_manifest_publication(
            Err(MongrelError::Other("rename failed".into())),
            false,
            42,
        )
        .unwrap_err();
        assert!(matches!(error, MongrelError::Other(message) if message == "rename failed"));
    }

    fn schema() -> Schema {
        Schema {
            schema_id: 0,
            columns: vec![ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            }],
            indexes: Vec::new(),
            colocation: Vec::new(),
            constraints: Default::default(),
            clustered: false,
        }
    }

    fn sample_manifest() -> PitrArchiveManifest {
        let base_epoch = 10;
        let base_unix_nanos = 100;
        let base_backup_sha256 = "aa".repeat(32);
        let genesis = genesis_chain(base_epoch, base_unix_nanos, &base_backup_sha256).unwrap();
        let first_sha = "11".repeat(32);
        let first_chain = next_chain(&genesis, &first_sha, 10, 12, 4, 20, 23).unwrap();
        let second_sha = "22".repeat(32);
        let second_chain = next_chain(&first_chain, &second_sha, 12, 13, 2, 30, 31).unwrap();
        PitrArchiveManifest {
            format_version: FORMAT_VERSION,
            base_epoch,
            base_unix_nanos,
            base_backup_sha256,
            archived_through_epoch: 13,
            last_commit_unix_nanos: 130,
            chunks: vec![
                PitrChunkRef {
                    file: chunk_file_name(10, 12),
                    from_epoch: 10,
                    through_epoch: 12,
                    records: 4,
                    bytes: 100,
                    sha256: first_sha,
                    commits: vec![
                        PitrCommitPoint {
                            epoch: 11,
                            unix_nanos: 110,
                        },
                        PitrCommitPoint {
                            epoch: 12,
                            unix_nanos: 120,
                        },
                    ],
                    commit_ledger: Vec::new(),
                    first_sequence: 20,
                    last_sequence: 23,
                    previous_chain_sha256: genesis,
                    chain_sha256: first_chain.clone(),
                },
                PitrChunkRef {
                    file: chunk_file_name(12, 13),
                    from_epoch: 12,
                    through_epoch: 13,
                    records: 2,
                    bytes: 100,
                    sha256: second_sha,
                    commits: vec![PitrCommitPoint {
                        epoch: 13,
                        unix_nanos: 130,
                    }],
                    commit_ledger: Vec::new(),
                    first_sequence: 30,
                    last_sequence: 31,
                    previous_chain_sha256: first_chain,
                    chain_sha256: second_chain.clone(),
                },
            ],
            encrypted: false,
            chain_sha256: second_chain,
            authentication: None,
        }
    }

    fn sample_chunk() -> PitrChunkV2 {
        PitrChunkV2 {
            format_version: FORMAT_VERSION,
            from_epoch: 10,
            through_epoch: 11,
            records: vec![
                Record::new(Epoch(20), 7, Op::CommitTimestamp { unix_nanos: 110 }),
                Record::new(
                    Epoch(21),
                    7,
                    Op::TxnCommit {
                        epoch: 11,
                        added_runs: Vec::new(),
                    },
                ),
            ],
            commits: vec![PitrCommitPoint {
                epoch: 11,
                unix_nanos: 110,
            }],
            first_sequence: 20,
            last_sequence: 21,
            previous_chain_sha256: "11".repeat(32),
        }
    }

    #[test]
    fn matching_plaintext_orphan_chunk_is_reused_exactly() {
        let directory = tempfile::tempdir().unwrap();
        let root = DurableRoot::open(directory.path()).unwrap();
        let chunk = sample_chunk();
        let file = Path::new("wal-10-11.bin");
        let encoded = encode_or_reuse_chunk_v2(&root, file, &chunk, None).unwrap();
        root.write_new(file, &encoded).unwrap();

        let reused = encode_or_reuse_chunk_v2(&root, file, &chunk, None).unwrap();

        assert_eq!(reused, encoded);
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn matching_encrypted_orphan_chunk_is_reused_exactly() {
        let directory = tempfile::tempdir().unwrap();
        let root = DurableRoot::open(directory.path()).unwrap();
        let chunk = sample_chunk();
        let file = Path::new("wal-10-11.bin");
        let kek = crate::encryption::Kek::derive("secret", &[7; 16]).unwrap();
        let encoded = encode_or_reuse_chunk_v2(&root, file, &chunk, Some(&kek)).unwrap();
        root.write_new(file, &encoded).unwrap();

        let reused = encode_or_reuse_chunk_v2(&root, file, &chunk, Some(&kek)).unwrap();

        assert_eq!(reused, encoded);
    }

    fn root_and_alice(database: &Database) -> (Principal, Principal) {
        let root = database.principal_snapshot().unwrap();
        database.create_user("alice", "alice-password").unwrap();
        database.set_user_admin("alice", true).unwrap();
        let alice = database.resolve_principal("alice").unwrap();
        (root, alice)
    }

    #[test]
    fn base_archive_rechecks_exact_admin_at_outer_publication() {
        let source = tempfile::tempdir().unwrap();
        let destination_parent = tempfile::tempdir().unwrap();
        let destination = destination_parent.path().join("archive");
        let database =
            Database::create_with_credentials(source.path(), "admin", "admin-password").unwrap();
        database.create_user("rescue", "rescue-password").unwrap();
        database.set_user_admin("rescue", true).unwrap();

        let result = database.create_pitr_archive_inner(&destination, || {
            database.drop_user("admin")?;
            Ok(())
        });

        assert!(
            matches!(result, Err(MongrelError::AuthRequired)),
            "unexpected result: {result:?}"
        );
        assert!(!destination.exists());
    }

    #[test]
    fn incremental_archive_rechecks_exact_admin_at_outer_publication() {
        let source = tempfile::tempdir().unwrap();
        let archive_parent = tempfile::tempdir().unwrap();
        let archive = archive_parent.path().join("archive");
        let database =
            Database::create_with_credentials(source.path(), "admin", "admin-password").unwrap();
        database.create_user("rescue", "rescue-password").unwrap();
        database.set_user_admin("rescue", true).unwrap();
        let base = database.create_pitr_archive(&archive).unwrap();
        database.create_table("items", schema()).unwrap();

        let result = database.archive_pitr_inner(&archive, || {
            database.drop_user("admin")?;
            Ok(())
        });

        assert!(
            matches!(result, Err(MongrelError::AuthRequired)),
            "unexpected result: {result:?}"
        );
        let manifest = read_pitr_manifest(&archive).unwrap();
        assert_eq!(manifest.archived_through_epoch, base.through_epoch);
        assert!(manifest.chunks.is_empty());
    }

    #[test]
    fn manifest_rejects_paths_gaps_duplicates_reordering_and_bad_timestamps() {
        let manifest = sample_manifest();
        validate_manifest_structure(&manifest).unwrap();

        let mut path = manifest.clone();
        path.chunks[0].file = "../escape.bin".into();
        assert!(validate_manifest_structure(&path).is_err());

        let mut missing_base_digest = manifest.clone();
        missing_base_digest.base_backup_sha256.clear();
        assert!(validate_manifest_structure(&missing_base_digest).is_err());

        let mut absolute = manifest.clone();
        absolute.chunks[0].file = "/tmp/escape.bin".into();
        assert!(validate_manifest_structure(&absolute).is_err());

        let mut gap = manifest.clone();
        gap.chunks[1].from_epoch = 11;
        gap.chunks[1].file = chunk_file_name(11, 13);
        assert!(validate_manifest_structure(&gap).is_err());

        let mut duplicate = manifest.clone();
        duplicate.chunks.insert(1, duplicate.chunks[0].clone());
        assert!(validate_manifest_structure(&duplicate).is_err());

        let mut reordered = manifest.clone();
        reordered.chunks.swap(0, 1);
        assert!(validate_manifest_structure(&reordered).is_err());

        let mut duplicate_sequence = manifest.clone();
        duplicate_sequence.chunks[1].first_sequence = duplicate_sequence.chunks[0].last_sequence;
        duplicate_sequence.chunks[1].last_sequence =
            duplicate_sequence.chunks[1].first_sequence + 1;
        assert!(validate_manifest_structure(&duplicate_sequence).is_err());

        let mut timestamp = manifest;
        timestamp.chunks[1].commits[0].unix_nanos = 119;
        timestamp.last_commit_unix_nanos = 119;
        assert!(validate_manifest_structure(&timestamp).is_err());
    }

    #[test]
    fn chunk_body_rejects_count_range_commit_and_sequence_mismatches() {
        let manifest = sample_manifest();
        let reference = &manifest.chunks[1];
        let records = vec![
            Record::new(Epoch(30), 7, Op::CommitTimestamp { unix_nanos: 130 }),
            Record::new(
                Epoch(31),
                7,
                Op::TxnCommit {
                    epoch: 13,
                    added_runs: Vec::new(),
                },
            ),
        ];
        let chunk = DecodedPitrChunk {
            from_epoch: 12,
            through_epoch: 13,
            records,
            commits: reference.commits.clone(),
            first_sequence: Some(reference.first_sequence),
            last_sequence: Some(reference.last_sequence),
            previous_chain_sha256: Some(reference.previous_chain_sha256.clone()),
        };
        validate_chunk(reference, &chunk, FORMAT_VERSION, None).unwrap();

        let mut wrong_count = reference.clone();
        wrong_count.records = 3;
        assert!(validate_chunk(&wrong_count, &chunk, FORMAT_VERSION, None).is_err());

        let mut wrong_range = DecodedPitrChunk {
            from_epoch: 11,
            through_epoch: chunk.through_epoch,
            records: chunk.records.clone(),
            commits: chunk.commits.clone(),
            first_sequence: chunk.first_sequence,
            last_sequence: chunk.last_sequence,
            previous_chain_sha256: chunk.previous_chain_sha256.clone(),
        };
        assert!(validate_chunk(reference, &wrong_range, FORMAT_VERSION, None).is_err());
        wrong_range.from_epoch = chunk.from_epoch;
        wrong_range.records.swap(0, 1);
        assert!(validate_chunk(reference, &wrong_range, FORMAT_VERSION, None).is_err());

        let mut duplicate_sequence = DecodedPitrChunk {
            from_epoch: chunk.from_epoch,
            through_epoch: chunk.through_epoch,
            records: chunk.records.clone(),
            commits: chunk.commits.clone(),
            first_sequence: chunk.first_sequence,
            last_sequence: chunk.last_sequence,
            previous_chain_sha256: chunk.previous_chain_sha256.clone(),
        };
        duplicate_sequence.records[1].seq = duplicate_sequence.records[0].seq;
        assert!(validate_chunk(reference, &duplicate_sequence, FORMAT_VERSION, None).is_err());
        assert!(validate_chunk(reference, &chunk, FORMAT_VERSION, Some(30)).is_err());
    }

    #[test]
    fn base_archive_rejects_drop_and_recreate_of_same_admin_username() {
        let source = tempfile::tempdir().unwrap();
        let destination_parent = tempfile::tempdir().unwrap();
        let destination = destination_parent.path().join("archive");
        let database =
            Database::create_with_credentials(source.path(), "root", "root-password").unwrap();
        let (root, stale_alice) = root_and_alice(&database);
        database.set_cached_principal_for_test(Some(stale_alice.clone()));

        let result = database.create_pitr_archive_inner(&destination, || {
            database.set_cached_principal_for_test(Some(root.clone()));
            database.drop_user("alice")?;
            database.create_user("alice", "replacement-password")?;
            database.set_user_admin("alice", true)?;
            database.set_cached_principal_for_test(Some(stale_alice.clone()));
            Ok(())
        });

        assert!(matches!(result, Err(MongrelError::AuthRequired)));
        assert!(!destination.exists());
    }

    #[test]
    fn incremental_archive_rejects_admin_demotion_at_final_publication() {
        let source = tempfile::tempdir().unwrap();
        let archive_parent = tempfile::tempdir().unwrap();
        let archive = archive_parent.path().join("archive");
        let database =
            Database::create_with_credentials(source.path(), "root", "root-password").unwrap();
        let (root, stale_alice) = root_and_alice(&database);
        database.create_table("items", schema()).unwrap();
        database.create_pitr_archive(&archive).unwrap();
        let mut transaction = database.begin();
        transaction
            .put("items", vec![(1, crate::Value::Int64(1))])
            .unwrap();
        transaction.commit().unwrap();
        let before = read_pitr_manifest(&archive).unwrap();
        database.set_cached_principal_for_test(Some(stale_alice.clone()));

        let result = database.archive_pitr_inner(&archive, || {
            database.set_cached_principal_for_test(Some(root.clone()));
            database.set_user_admin("alice", false)?;
            database.set_cached_principal_for_test(Some(stale_alice.clone()));
            Ok(())
        });

        assert!(matches!(result, Err(MongrelError::PermissionDenied { .. })));
        let after = read_pitr_manifest(&archive).unwrap();
        assert_eq!(after, before);
    }

    #[cfg(unix)]
    #[test]
    fn archive_lock_symlink_is_rejected_without_touching_target() {
        use std::os::unix::fs::symlink;

        let source = tempfile::tempdir().unwrap();
        let archive_parent = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let archive = archive_parent.path().join("archive");
        let database = Database::create(source.path()).unwrap();
        database.create_table("items", schema()).unwrap();
        database.create_pitr_archive(&archive).unwrap();
        let mut transaction = database.begin();
        transaction
            .put("items", vec![(1, crate::Value::Int64(1))])
            .unwrap();
        transaction.commit().unwrap();
        let outside_lock = outside.path().join("lock");
        std::fs::write(&outside_lock, b"outside").unwrap();
        symlink(&outside_lock, archive.join(".archive.lock")).unwrap();

        assert!(database.archive_pitr(&archive).is_err());
        assert_eq!(std::fs::read(outside_lock).unwrap(), b"outside");
        assert!(read_pitr_manifest(&archive).unwrap().chunks.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_chunk_is_not_followed_during_restore() {
        use std::os::unix::fs::symlink;

        let source = tempfile::tempdir().unwrap();
        let archive_parent = tempfile::tempdir().unwrap();
        let restore_parent = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let archive = archive_parent.path().join("archive");
        let destination = restore_parent.path().join("restored");
        let database = Database::create(source.path()).unwrap();
        database.create_table("items", schema()).unwrap();
        database.create_pitr_archive(&archive).unwrap();
        let mut transaction = database.begin();
        transaction
            .put("items", vec![(1, crate::Value::Int64(1))])
            .unwrap();
        transaction.commit().unwrap();
        database.archive_pitr(&archive).unwrap();
        let manifest = read_pitr_manifest(&archive).unwrap();
        let chunk_path = archive.join(&manifest.chunks[0].file);
        let outside_chunk = outside.path().join("chunk.bin");
        std::fs::rename(&chunk_path, &outside_chunk).unwrap();
        let before = std::fs::read(&outside_chunk).unwrap();
        symlink(&outside_chunk, &chunk_path).unwrap();

        assert!(restore_pitr(
            &archive,
            &destination,
            PitrTarget::Latest,
            PitrCredentials::None,
        )
        .is_err());
        assert!(!destination.exists());
        assert_eq!(std::fs::read(outside_chunk).unwrap(), before);
    }

    #[test]
    fn restore_rejects_base_changed_after_initial_verification() {
        let source = tempfile::tempdir().unwrap();
        let archive_parent = tempfile::tempdir().unwrap();
        let restore_parent = tempfile::tempdir().unwrap();
        let archive = archive_parent.path().join("archive");
        let destination = restore_parent.path().join("restored");
        let database = Database::create(source.path()).unwrap();
        database.create_table("items", schema()).unwrap();
        database.create_pitr_archive(&archive).unwrap();
        let base_manifest = crate::backup::verify_backup(archive.join("base")).unwrap();
        let victim = archive.join("base").join(&base_manifest.files[0].path);

        let result = restore_pitr_inner(
            &archive,
            &destination,
            PitrTarget::Latest,
            PitrCredentials::None,
            |_| {
                std::fs::write(&victim, b"changed after verification")?;
                Ok(())
            },
        );

        assert!(result.is_err());
        assert!(!destination.exists());
    }

    #[cfg(unix)]
    #[test]
    fn restore_stage_nested_symlink_cannot_escape() {
        use std::os::unix::fs::symlink;

        let source = tempfile::tempdir().unwrap();
        let archive_parent = tempfile::tempdir().unwrap();
        let restore_parent = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let archive = archive_parent.path().join("archive");
        let destination = restore_parent.path().join("restored");
        let database = Database::create(source.path()).unwrap();
        database.create_table("items", schema()).unwrap();
        database.create_pitr_archive(&archive).unwrap();
        let guard = outside.path().join("guard");
        std::fs::write(&guard, b"unchanged").unwrap();

        let result = restore_pitr_inner(
            &archive,
            &destination,
            PitrTarget::Latest,
            PitrCredentials::None,
            |stage| {
                symlink(outside.path(), stage.join("_meta"))?;
                Ok(())
            },
        );

        assert!(result.is_err());
        assert!(!destination.exists());
        assert_eq!(std::fs::read(&guard).unwrap(), b"unchanged");
        assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 1);
    }

    #[test]
    fn restore_destination_inside_archive_is_rejected_without_staging_debris() {
        let source = tempfile::tempdir().unwrap();
        let archive_parent = tempfile::tempdir().unwrap();
        let archive = archive_parent.path().join("archive");
        let database = Database::create(source.path()).unwrap();
        database.create_table("items", schema()).unwrap();
        database.create_pitr_archive(&archive).unwrap();
        let base = archive.join("base");
        let mut before = std::fs::read_dir(&base)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        before.sort();

        let result = restore_pitr(
            &archive,
            base.join("restored"),
            PitrTarget::Latest,
            PitrCredentials::None,
        );

        assert!(matches!(result, Err(MongrelError::InvalidArgument(_))));
        let mut after = std::fs::read_dir(&base)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        after.sort();
        assert_eq!(after, before);
    }

    #[cfg(unix)]
    #[test]
    fn base_backup_stays_in_pinned_stage_after_parent_rename() {
        let source = tempfile::tempdir().unwrap();
        let parent_root = tempfile::tempdir().unwrap();
        let requested_parent = parent_root.path().join("requested");
        let moved_parent = parent_root.path().join("moved");
        std::fs::create_dir(&requested_parent).unwrap();
        let destination = requested_parent.join("archive");
        let database = Database::create(source.path()).unwrap();
        database.create_table("items", schema()).unwrap();
        let mut transaction = database.begin();
        transaction
            .put("items", vec![(1, crate::Value::Int64(1))])
            .unwrap();
        transaction.commit().unwrap();
        database.checkpoint().unwrap();
        let requested_for_hook = requested_parent.clone();
        let moved_for_hook = moved_parent.clone();
        database.__set_backup_hook(move || {
            std::fs::rename(&requested_for_hook, &moved_for_hook).unwrap();
            std::fs::create_dir(&requested_for_hook).unwrap();
        });

        database.create_pitr_archive(&destination).unwrap();

        assert!(moved_parent.join("archive/base").is_dir());
        assert!(!requested_parent.join("archive").exists());
        read_pitr_manifest(moved_parent.join("archive")).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn archive_publication_stays_in_pinned_parent_after_rename() {
        let source = tempfile::tempdir().unwrap();
        let parent_root = tempfile::tempdir().unwrap();
        let requested_parent = parent_root.path().join("requested");
        let moved_parent = parent_root.path().join("moved");
        std::fs::create_dir(&requested_parent).unwrap();
        let destination = requested_parent.join("archive");
        let database = Database::create(source.path()).unwrap();

        let report = database
            .create_pitr_archive_inner(&destination, || {
                std::fs::rename(&requested_parent, &moved_parent)?;
                std::fs::create_dir(&requested_parent)?;
                Ok(())
            })
            .unwrap();

        assert_eq!(report.archive, destination);
        assert!(moved_parent.join("archive").is_dir());
        assert!(!requested_parent.join("archive").exists());
    }

    #[cfg(unix)]
    #[test]
    fn restore_recovery_stays_in_pinned_stage_after_parent_rename() {
        let source = tempfile::tempdir().unwrap();
        let archive_parent = tempfile::tempdir().unwrap();
        let restore_root = tempfile::tempdir().unwrap();
        let archive = archive_parent.path().join("archive");
        let requested_parent = restore_root.path().join("requested");
        let moved_parent = restore_root.path().join("moved");
        std::fs::create_dir(&requested_parent).unwrap();
        let destination = requested_parent.join("restored");
        let database = Database::create(source.path()).unwrap();
        database.create_table("items", schema()).unwrap();
        database.create_pitr_archive(&archive).unwrap();
        let mut transaction = database.begin();
        transaction
            .put("items", vec![(1, crate::Value::Int64(7))])
            .unwrap();
        transaction.commit().unwrap();
        database.archive_pitr(&archive).unwrap();

        restore_pitr_inner(
            &archive,
            &destination,
            PitrTarget::Latest,
            PitrCredentials::None,
            |_| {
                std::fs::rename(&requested_parent, &moved_parent)?;
                std::fs::create_dir(&requested_parent)?;
                Ok(())
            },
        )
        .unwrap();

        assert!(!destination.exists());
        let restored = Database::open(moved_parent.join("restored")).unwrap();
        assert_eq!(
            restored
                .table("items")
                .unwrap()
                .lock()
                .visible_rows(restored.snapshot().0)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn plaintext_legacy_archive_is_restore_only_but_remains_restorable() {
        let source = tempfile::tempdir().unwrap();
        let archive_parent = tempfile::tempdir().unwrap();
        let restore_parent = tempfile::tempdir().unwrap();
        let archive = archive_parent.path().join("archive");
        let destination = restore_parent.path().join("restored");
        let database = Database::create(source.path()).unwrap();
        database.create_table("items", schema()).unwrap();
        database.create_pitr_archive(&archive).unwrap();
        let mut transaction = database.begin();
        transaction
            .put("items", vec![(1, crate::Value::Int64(7))])
            .unwrap();
        transaction.commit().unwrap();
        database.archive_pitr(&archive).unwrap();

        let mut manifest = read_pitr_manifest(&archive).unwrap();
        let reference = &mut manifest.chunks[0];
        let current_bytes = std::fs::read(archive.join(&reference.file)).unwrap();
        let decoded = decode_chunk(FORMAT_VERSION, false, &current_bytes, None).unwrap();
        let legacy = LegacyPitrChunk {
            format_version: LEGACY_FORMAT_VERSION,
            from_epoch: decoded.from_epoch,
            through_epoch: decoded.through_epoch,
            records: decoded.records,
            commits: decoded.commits,
        };
        let legacy_bytes = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .serialize(&legacy)
            .unwrap();
        std::fs::write(archive.join(&reference.file), &legacy_bytes).unwrap();
        reference.bytes = legacy_bytes.len() as u64;
        reference.sha256 = sha256_bytes(&legacy_bytes);
        reference.first_sequence = 0;
        reference.last_sequence = 0;
        reference.previous_chain_sha256.clear();
        reference.chain_sha256.clear();
        reference.commit_ledger.clear();
        manifest.format_version = LEGACY_FORMAT_VERSION;
        manifest.base_backup_sha256.clear();
        manifest.chain_sha256.clear();
        manifest.authentication = None;
        std::fs::write(
            archive.join(MANIFEST_FILE),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        restore_pitr(
            &archive,
            &destination,
            PitrTarget::Latest,
            PitrCredentials::None,
        )
        .unwrap();
        let restored = Database::open(destination).unwrap();
        assert_eq!(
            restored
                .table("items")
                .unwrap()
                .lock()
                .visible_rows(restored.snapshot().0)
                .unwrap()
                .len(),
            1
        );
        assert!(matches!(
            database.archive_pitr(&archive),
            Err(MongrelError::Conflict(_))
        ));
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn encrypted_chunks_use_distinct_random_nonces() {
        let source = tempfile::tempdir().unwrap();
        let archive_parent = tempfile::tempdir().unwrap();
        let archive = archive_parent.path().join("archive");
        let database = Database::create_encrypted(source.path(), "secret passphrase").unwrap();
        database.create_table("items", schema()).unwrap();
        database.create_pitr_archive(&archive).unwrap();
        for id in [1, 2] {
            let mut transaction = database.begin();
            transaction
                .put("items", vec![(1, crate::Value::Int64(id))])
                .unwrap();
            transaction.commit().unwrap();
            database.archive_pitr(&archive).unwrap();
        }
        let manifest = read_pitr_manifest(&archive).unwrap();
        assert_eq!(manifest.chunks.len(), 2);
        let nonces = manifest
            .chunks
            .iter()
            .map(|reference| {
                let bytes = std::fs::read(archive.join(&reference.file)).unwrap();
                bincode::DefaultOptions::new()
                    .with_fixint_encoding()
                    .reject_trailing_bytes()
                    .deserialize::<PitrChunkEnvelopeV2>(&bytes)
                    .unwrap()
                    .nonce
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_ne!(nonces[0], nonces[1]);
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn encrypted_legacy_archive_is_refused() {
        let source = tempfile::tempdir().unwrap();
        let archive_parent = tempfile::tempdir().unwrap();
        let restore_parent = tempfile::tempdir().unwrap();
        let archive = archive_parent.path().join("archive");
        let destination = restore_parent.path().join("restored");
        let database = Database::create_encrypted(source.path(), "secret passphrase").unwrap();
        database.create_pitr_archive(&archive).unwrap();
        let mut manifest = read_pitr_manifest(&archive).unwrap();
        manifest.format_version = LEGACY_FORMAT_VERSION;
        manifest.encrypted = false;
        manifest.base_backup_sha256.clear();
        manifest.chain_sha256.clear();
        manifest.authentication = None;
        std::fs::write(
            archive.join(MANIFEST_FILE),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        assert!(matches!(
            restore_pitr(
                &archive,
                &destination,
                PitrTarget::Latest,
                PitrCredentials::Encryption("secret passphrase"),
            ),
            Err(MongrelError::Conflict(_))
        ));
        assert!(!destination.exists());
    }

    fn ledger_manifest() -> PitrArchiveManifest {
        let mut manifest = sample_manifest();
        manifest.chunks[0].commit_ledger = vec![
            PitrCommitLedgerEntry {
                txn_id: 5,
                sequence: 21,
            },
            PitrCommitLedgerEntry {
                txn_id: 7,
                sequence: 23,
            },
        ];
        manifest.chunks[1].commit_ledger = vec![PitrCommitLedgerEntry {
            txn_id: 9,
            sequence: 31,
        }];
        manifest
    }

    #[test]
    fn pre_1g_chunk_reference_json_decodes_without_a_ledger() {
        let json = r#"{
            "file": "wal-00000000000000000010-00000000000000000012.bin",
            "from_epoch": 10,
            "through_epoch": 12,
            "records": 4,
            "bytes": 100,
            "sha256": "1111111111111111111111111111111111111111111111111111111111111111",
            "commits": [{"epoch": 11, "unix_nanos": 110}]
        }"#;
        let reference: PitrChunkRef = serde_json::from_str(json).unwrap();
        assert!(reference.commit_ledger.is_empty());
        // Serializing an empty ledger keeps the pre-1G JSON shape.
        assert!(!serde_json::to_string(&reference)
            .unwrap()
            .contains("commit_ledger"));
    }

    #[test]
    fn transaction_id_target_lands_on_the_exact_commit() {
        let manifest = ledger_manifest();
        assert_eq!(
            resolve_target_epoch(&manifest, PitrTarget::TransactionId(5)).unwrap(),
            11
        );
        assert_eq!(
            resolve_target_epoch(&manifest, PitrTarget::TransactionId(9)).unwrap(),
            13
        );
        // Unknown ids fail closed, as does a legacy ledger without entries.
        assert!(resolve_target_epoch(&manifest, PitrTarget::TransactionId(8)).is_err());
        assert!(resolve_target_epoch(&sample_manifest(), PitrTarget::TransactionId(5)).is_err());
    }

    #[test]
    fn log_position_target_resolves_through_the_commit_ledger() {
        let manifest = ledger_manifest();
        assert_eq!(
            resolve_target_epoch(&manifest, PitrTarget::LogPosition(23)).unwrap(),
            12
        );
        // A position between commits resolves to the earlier commit.
        assert_eq!(
            resolve_target_epoch(&manifest, PitrTarget::LogPosition(30)).unwrap(),
            12
        );
        // A position inside the base backup resolves to the base boundary.
        assert_eq!(
            resolve_target_epoch(&manifest, PitrTarget::LogPosition(20)).unwrap(),
            10
        );
        // A position above every commit resolves to the archive watermark.
        assert_eq!(
            resolve_target_epoch(&manifest, PitrTarget::LogPosition(u64::MAX)).unwrap(),
            13
        );
        // A legacy ledger without recorded positions fails closed.
        assert!(resolve_target_epoch(&sample_manifest(), PitrTarget::LogPosition(23)).is_err());
    }

    #[test]
    fn manifest_structure_rejects_a_ledger_length_mismatch() {
        let mut manifest = ledger_manifest();
        validate_manifest_structure(&manifest).unwrap();
        manifest.chunks[0].commit_ledger.pop();
        assert!(validate_manifest_structure(&manifest).is_err());

        let mut legacy = ledger_manifest();
        legacy.format_version = LEGACY_FORMAT_VERSION;
        legacy.base_backup_sha256.clear();
        legacy.chain_sha256.clear();
        for chunk in &mut legacy.chunks {
            chunk.first_sequence = 0;
            chunk.last_sequence = 0;
            chunk.previous_chain_sha256.clear();
            chunk.chain_sha256.clear();
        }
        assert!(validate_manifest_structure(&legacy).is_err());
    }

    #[test]
    fn chunk_body_rejects_commit_ledger_mismatches() {
        let manifest = sample_manifest();
        let mut reference = manifest.chunks[1].clone();
        reference.commit_ledger = vec![PitrCommitLedgerEntry {
            txn_id: 7,
            sequence: 31,
        }];
        let chunk = DecodedPitrChunk {
            from_epoch: 12,
            through_epoch: 13,
            records: vec![
                Record::new(Epoch(30), 7, Op::CommitTimestamp { unix_nanos: 130 }),
                Record::new(
                    Epoch(31),
                    7,
                    Op::TxnCommit {
                        epoch: 13,
                        added_runs: Vec::new(),
                    },
                ),
            ],
            commits: reference.commits.clone(),
            first_sequence: Some(30),
            last_sequence: Some(31),
            previous_chain_sha256: Some(reference.previous_chain_sha256.clone()),
        };
        validate_chunk(&reference, &chunk, FORMAT_VERSION, None).unwrap();

        let mut wrong_txn = reference.clone();
        wrong_txn.commit_ledger[0].txn_id = 8;
        assert!(validate_chunk(&wrong_txn, &chunk, FORMAT_VERSION, None).is_err());

        let mut wrong_sequence = reference.clone();
        wrong_sequence.commit_ledger[0].sequence = 30;
        assert!(validate_chunk(&wrong_sequence, &chunk, FORMAT_VERSION, None).is_err());
    }
}
