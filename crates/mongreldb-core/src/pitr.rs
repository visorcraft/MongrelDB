//! Point-in-time recovery archives built from an online base backup plus
//! checksummed, transaction-complete logical WAL chunks.

use crate::backup::verify_backup;
use crate::catalog::TableState;
use crate::epoch::Epoch;
use crate::wal::{Op, Record};
use crate::{Database, MongrelError, Result};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};

const FORMAT_VERSION: u16 = 1;
const MANIFEST_FILE: &str = "pitr.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PitrTarget {
    Latest,
    Epoch(u64),
    TimestampNanos(u64),
}

#[derive(Clone, Copy)]
pub enum PitrCredentials<'a> {
    None,
    Encryption(&'a str),
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
pub struct PitrCommitPoint {
    pub epoch: u64,
    pub unix_nanos: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PitrChunkRef {
    pub file: String,
    pub from_epoch: u64,
    pub through_epoch: u64,
    pub records: usize,
    pub bytes: u64,
    pub sha256: String,
    pub commits: Vec<PitrCommitPoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PitrArchiveManifest {
    pub format_version: u16,
    pub base_epoch: u64,
    pub base_unix_nanos: u64,
    pub archived_through_epoch: u64,
    pub last_commit_unix_nanos: u64,
    pub chunks: Vec<PitrChunkRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PitrArchiveReport {
    pub archive: PathBuf,
    pub from_epoch: u64,
    pub through_epoch: u64,
    pub records: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PitrChunk {
    format_version: u16,
    from_epoch: u64,
    through_epoch: u64,
    records: Vec<Record>,
    commits: Vec<PitrCommitPoint>,
}

impl Database {
    /// Initialize a PITR archive with a consistent online base backup.
    pub fn create_pitr_archive(&self, destination: impl AsRef<Path>) -> Result<PitrArchiveReport> {
        let destination = destination.as_ref();
        let (destination, parent, stage) = prepare_destination(destination, "pitr-stage")?;
        std::fs::create_dir(&stage)?;
        let outcome = (|| {
            let backup = self.hot_backup(stage.join("base"))?;
            let now = unix_nanos();
            let manifest = PitrArchiveManifest {
                format_version: FORMAT_VERSION,
                base_epoch: backup.epoch,
                base_unix_nanos: now,
                archived_through_epoch: backup.epoch,
                last_commit_unix_nanos: now,
                chunks: Vec::new(),
            };
            write_manifest(&stage, &manifest)?;
            crate::backup::sync_directories(&stage)?;
            if destination.exists() {
                return Err(MongrelError::Conflict(format!(
                    "PITR archive already exists: {}",
                    destination.display()
                )));
            }
            std::fs::rename(&stage, &destination)?;
            sync_dir(&parent)?;
            // Keep a practical WAL window for callers that archive periodically.
            self.set_replication_wal_retention_segments(64);
            Ok(PitrArchiveReport {
                archive: destination,
                from_epoch: backup.epoch,
                through_epoch: backup.epoch,
                records: 0,
            })
        })();
        if outcome.is_err() && stage.exists() {
            let _ = std::fs::remove_dir_all(stage);
        }
        outcome
    }

    /// Append all complete commits since the archive watermark. Spilled-run
    /// commits are converted to ordinary logical Put records while their run
    /// payload remains available. A retention gap fails closed.
    pub fn archive_pitr(&self, archive: impl AsRef<Path>) -> Result<PitrArchiveReport> {
        let archive = archive.as_ref().canonicalize()?;
        let lock_path = archive.join(".archive.lock");
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        lock.lock_exclusive()?;
        let mut manifest = read_pitr_manifest(&archive)?;
        let from_epoch = manifest.archived_through_epoch;
        let batch = self.replication_batch_since(from_epoch)?;
        if batch.current_epoch == from_epoch {
            return Ok(PitrArchiveReport {
                archive,
                from_epoch,
                through_epoch: from_epoch,
                records: 0,
            });
        }
        let has_spilled = batch.records.iter().any(|record| {
            matches!(&record.op, Op::TxnCommit { added_runs, .. } if !added_runs.is_empty())
        });
        if batch.requires_snapshot && !has_spilled {
            return Err(MongrelError::Conflict(format!(
                "PITR WAL retention gap after epoch {from_epoch}; create a new base archive"
            )));
        }
        if batch
            .earliest_epoch
            .is_some_and(|earliest| earliest > from_epoch.saturating_add(1))
        {
            return Err(MongrelError::Conflict(format!(
                "PITR WAL retention gap: earliest retained epoch is {}",
                batch.earliest_epoch.unwrap()
            )));
        }

        let records = materialize_spilled_records(self, batch.records)?;
        if records.is_empty() {
            return Err(MongrelError::Conflict(
                "PITR source advanced but no complete WAL transactions remain".into(),
            ));
        }
        let mut timestamps = HashMap::new();
        let mut commit_epochs = Vec::new();
        for record in &records {
            match record.op {
                Op::CommitTimestamp { unix_nanos } => {
                    timestamps.insert(record.txn_id, unix_nanos);
                }
                Op::TxnCommit { epoch, .. } => commit_epochs.push((record.txn_id, epoch)),
                _ => {}
            }
        }
        commit_epochs.sort_by_key(|(_, epoch)| *epoch);
        let archive_time = unix_nanos();
        let mut last_timestamp = manifest.last_commit_unix_nanos;
        let commits = commit_epochs
            .into_iter()
            .map(|(txn_id, epoch)| {
                let timestamp = timestamps
                    .get(&txn_id)
                    .copied()
                    .unwrap_or(archive_time)
                    .max(last_timestamp);
                last_timestamp = timestamp;
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
        let chunk = PitrChunk {
            format_version: FORMAT_VERSION,
            from_epoch,
            through_epoch,
            records,
            commits: commits.clone(),
        };
        let bytes = bincode::serialize(&chunk)?;
        let file = format!("wal-{from_epoch:020}-{through_epoch:020}.bin");
        let chunk_path = archive.join(&file);
        let mut output = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&chunk_path)?;
        output.write_all(&bytes)?;
        output.sync_all()?;
        manifest.chunks.push(PitrChunkRef {
            file,
            from_epoch,
            through_epoch,
            records: chunk.records.len(),
            bytes: bytes.len() as u64,
            sha256: sha256_bytes(&bytes),
            commits,
        });
        manifest.archived_through_epoch = through_epoch;
        manifest.last_commit_unix_nanos = last_timestamp;
        write_manifest(&archive, &manifest)?;
        sync_dir(&archive)?;
        Ok(PitrArchiveReport {
            archive,
            from_epoch,
            through_epoch,
            records: chunk.records.len(),
        })
    }
}

pub fn read_pitr_manifest(archive: impl AsRef<Path>) -> Result<PitrArchiveManifest> {
    let archive = archive.as_ref();
    let manifest: PitrArchiveManifest =
        serde_json::from_slice(&std::fs::read(archive.join(MANIFEST_FILE))?)
            .map_err(|error| MongrelError::InvalidArgument(format!("PITR manifest: {error}")))?;
    if manifest.format_version != FORMAT_VERSION {
        return Err(MongrelError::InvalidArgument(format!(
            "unsupported PITR archive version {}",
            manifest.format_version
        )));
    }
    Ok(manifest)
}

/// Restore an archive to a new directory at an epoch or timestamp cutoff.
pub fn restore_pitr(
    archive: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    target: PitrTarget,
    credentials: PitrCredentials<'_>,
) -> Result<u64> {
    let archive = archive.as_ref().canonicalize()?;
    let manifest = read_pitr_manifest(&archive)?;
    verify_backup(archive.join("base"))?;
    let target_epoch = resolve_target_epoch(&manifest, target)?;
    let (destination, parent, stage) = prepare_destination(destination.as_ref(), "pitr-restore")?;
    std::fs::create_dir(&stage)?;
    let outcome = (|| {
        copy_tree(&archive.join("base"), &stage)?;
        let meta = stage.join("_meta");
        std::fs::create_dir_all(&meta)?;
        write_synced(&meta.join("replica"), b"PITR restore staging\n")?;
        crate::replication::write_replica_epoch(&stage, manifest.base_epoch)?;

        let records = load_records_through(&archive, &manifest, target_epoch)?;
        if !records.is_empty() {
            let replica = open_with_credentials(&stage, credentials)?;
            let applied = replica.append_replication_batch(&records)?;
            drop(replica);
            let recovered = open_with_credentials(&stage, credentials)?;
            if recovered.visible_epoch().0 < applied || recovered.visible_epoch().0 < target_epoch {
                return Err(MongrelError::Other(format!(
                    "PITR recovery stopped at epoch {}, expected {target_epoch}",
                    recovered.visible_epoch().0
                )));
            }
            drop(recovered);
        }
        let _ = std::fs::remove_file(meta.join("replica"));
        let _ = std::fs::remove_file(meta.join("repl_epoch"));
        sync_dir(&meta)?;
        crate::backup::sync_directories(&stage)?;
        if destination.exists() {
            return Err(MongrelError::Conflict(format!(
                "PITR destination already exists: {}",
                destination.display()
            )));
        }
        std::fs::rename(&stage, &destination)?;
        sync_dir(&parent)?;
        Ok(target_epoch)
    })();
    if outcome.is_err() && stage.exists() {
        let _ = std::fs::remove_dir_all(stage);
    }
    outcome
}

fn materialize_spilled_records(db: &Database, records: Vec<Record>) -> Result<Vec<Record>> {
    let table_names: HashMap<u64, String> = db
        .catalog_snapshot()
        .tables
        .into_iter()
        .filter(|entry| matches!(entry.state, TableState::Live))
        .map(|entry| (entry.table_id, entry.name))
        .collect();
    let commit_epochs: HashMap<u64, u64> = records
        .iter()
        .filter_map(|record| match record.op {
            Op::TxnCommit { epoch, .. } => Some((record.txn_id, epoch)),
            _ => None,
        })
        .collect();
    let mut output = Vec::with_capacity(records.len());
    for record in records {
        let Op::TxnCommit { epoch, added_runs } = &record.op else {
            output.push(record);
            continue;
        };
        for run in added_runs {
            let name = table_names.get(&run.table_id).ok_or_else(|| {
                MongrelError::Conflict(format!(
                    "PITR cannot materialize spilled run {} for unavailable table {}",
                    run.run_id, run.table_id
                ))
            })?;
            let handle = db.table(name)?;
            let table = handle.lock();
            let mut reader = table.open_reader(run.run_id)?;
            let mut rows = reader.all_rows()?;
            for row in &mut rows {
                row.committed_epoch = Epoch(*epoch);
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
        let mut commit = record;
        if let Op::TxnCommit { added_runs, .. } = &mut commit.op {
            added_runs.clear();
        }
        output.push(commit);
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

fn resolve_target_epoch(manifest: &PitrArchiveManifest, target: PitrTarget) -> Result<u64> {
    match target {
        PitrTarget::Latest => Ok(manifest.archived_through_epoch),
        PitrTarget::Epoch(epoch)
            if epoch >= manifest.base_epoch && epoch <= manifest.archived_through_epoch =>
        {
            Ok(epoch)
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
    }
}

fn load_records_through(
    archive: &Path,
    manifest: &PitrArchiveManifest,
    target_epoch: u64,
) -> Result<Vec<Record>> {
    let mut records = Vec::new();
    for reference in &manifest.chunks {
        if reference.from_epoch >= target_epoch {
            break;
        }
        let bytes = std::fs::read(archive.join(&reference.file))?;
        if bytes.len() as u64 != reference.bytes || sha256_bytes(&bytes) != reference.sha256 {
            return Err(MongrelError::Other(format!(
                "PITR chunk {} checksum mismatch",
                reference.file
            )));
        }
        let chunk: PitrChunk = bincode::deserialize(&bytes)?;
        if chunk.format_version != FORMAT_VERSION {
            return Err(MongrelError::InvalidArgument(format!(
                "unsupported PITR chunk version {}",
                chunk.format_version
            )));
        }
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
        if reference.through_epoch >= target_epoch {
            break;
        }
    }
    Ok(records)
}

fn open_with_credentials(path: &Path, credentials: PitrCredentials<'_>) -> Result<Database> {
    match credentials {
        PitrCredentials::None => Database::open(path),
        #[cfg(feature = "encryption")]
        PitrCredentials::Encryption(passphrase) => Database::open_encrypted(path, passphrase),
        #[cfg(not(feature = "encryption"))]
        PitrCredentials::Encryption(_) => Err(MongrelError::Encryption(
            "encryption feature is disabled".into(),
        )),
        PitrCredentials::User { username, password } => {
            Database::open_with_credentials(path, username, password)
        }
        #[cfg(feature = "encryption")]
        PitrCredentials::EncryptionAndUser {
            passphrase,
            username,
            password,
        } => Database::open_encrypted_with_credentials(path, passphrase, username, password),
        #[cfg(not(feature = "encryption"))]
        PitrCredentials::EncryptionAndUser { .. } => Err(MongrelError::Encryption(
            "encryption feature is disabled".into(),
        )),
    }
}

fn prepare_destination(path: &Path, label: &str) -> Result<(PathBuf, PathBuf, PathBuf)> {
    if path.exists() {
        return Err(MongrelError::Conflict(format!(
            "destination already exists: {}",
            path.display()
        )));
    }
    let name = path
        .file_name()
        .ok_or_else(|| MongrelError::InvalidArgument("invalid destination".into()))?;
    let requested_parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(requested_parent)?;
    let parent = requested_parent.canonicalize()?;
    let destination = parent.join(name);
    let stage = parent.join(format!(
        ".{}.{}-{}-{}",
        name.to_string_lossy(),
        label,
        std::process::id(),
        unix_nanos()
    ));
    if stage.exists() {
        return Err(MongrelError::Conflict(
            "PITR staging path already exists".into(),
        ));
    }
    Ok((destination, parent, stage))
}

fn copy_tree(source: &Path, destination: &Path) -> Result<()> {
    std::fs::create_dir_all(destination)?;
    let mut entries = std::fs::read_dir(source)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Err(MongrelError::InvalidArgument(format!(
                "PITR restore refuses symlink {}",
                entry.path().display()
            )));
        }
        let target = destination.join(entry.file_name());
        if file_type.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else if file_type.is_file() {
            crate::backup::copy_file_synced(&entry.path(), &target)?;
        }
    }
    Ok(())
}

fn write_manifest(root: &Path, manifest: &PitrArchiveManifest) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|error| MongrelError::Other(format!("PITR manifest encode: {error}")))?;
    let temporary = root.join(format!(".{MANIFEST_FILE}.tmp"));
    write_synced(&temporary, &bytes)?;
    std::fs::rename(temporary, root.join(MANIFEST_FILE))?;
    sync_dir(root)
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = std::fs::File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn sync_dir(path: &Path) -> Result<()> {
    std::fs::File::open(path)?.sync_all()?;
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
