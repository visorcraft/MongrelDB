//! Online backup manifest and verification.
//!
//! The manifest audit fields (Stage 1G, spec section 10.7) are additive and
//! every one of them defaults, so backups written before Stage 1G still
//! deserialize and validate unchanged: `database_id`/`encryption` decode as
//! `None`, and `catalog_version`, `snapshot_unix_micros`, and
//! `open_generation` decode as `0` ("unknown"). Conversely, pre-1G binaries
//! ignore the new keys because the manifest never denied unknown fields.

use crate::{MongrelError, Result};
use mongreldb_types::ids::DatabaseId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::io::Read;
use std::path::{Component, Path, PathBuf};

pub const BACKUP_FORMAT_VERSION: u16 = 1;
pub const BACKUP_MANIFEST_PATH: &str = "_meta/backup.json";
const MAX_BACKUP_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;
/// Persisted replication identity marker reused as the backup database ID.
const REPLICATION_ID_PATH: &str = "_meta/replication_id";
const REPLICATION_ID_LEN: usize = 32;
/// KEK salt marker whose presence identifies an encrypted database.
const KEYS_PATH: &str = "_meta/keys";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupFile {
    pub path: PathBuf,
    pub bytes: u64,
    pub sha256: String,
}

/// Encryption metadata recorded in a backup manifest (Stage 1G). The KEK
/// salt itself travels inside the backup as the `_meta/keys` file, so only
/// the scheme identifiers live here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupEncryptionMetadata {
    /// KEK derivation scheme for the database passphrase.
    pub kdf: String,
    /// Page/record cipher guarding the backup files.
    pub cipher: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupManifest {
    pub format_version: u16,
    pub epoch: u64,
    pub created_unix_nanos: u64,
    /// Database identity (spec 10.7). Derived from the persisted replication
    /// identity (`_meta/replication_id`, 32 CSPRNG bytes created on first
    /// open): the `DatabaseId` is its first 16 bytes. The derivation is
    /// deterministic, stable across backups of one database, and travels
    /// with the backup because the marker file is part of the copied file
    /// set. `None` only when the source directory lacks the marker (a
    /// database never opened by a replication-aware binary); backup never
    /// invents or persists a fresh identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database_id: Option<DatabaseId>,
    /// Catalog command state-machine version at the backup boundary
    /// (S1F). `0` means "unknown": pre-1G manifests, legacy catalogs, and
    /// encrypted catalogs whose bytes cannot be decoded here without the
    /// database passphrase.
    #[serde(default)]
    pub catalog_version: u64,
    /// Snapshot wall clock in HLC physical units (microseconds since the
    /// UNIX epoch), captured at manifest creation right after the commit
    /// boundary was copied. `0` means "unknown" (pre-1G manifests). The
    /// exact boundary nanoseconds remain available to the caller through
    /// [`BackupReport::boundary_unix_nanos`].
    #[serde(default)]
    pub snapshot_unix_micros: u64,
    /// WAL open generation scoping `epoch`, read from `_meta/generation`.
    /// Together `(epoch, open_generation)` is the log continuation position:
    /// the durable commit watermark plus the generation that scopes the WAL
    /// record sequence it refers to. `0` means "unknown" (pre-1G manifests
    /// or a missing sidecar).
    #[serde(default)]
    pub open_generation: u64,
    /// Encryption metadata; `None` identifies a plaintext backup.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<BackupEncryptionMetadata>,
    pub files: Vec<BackupFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupReport {
    pub destination: PathBuf,
    pub epoch: u64,
    /// Wall-clock time captured under the same commit boundary as `epoch`.
    pub boundary_unix_nanos: u64,
    pub files: usize,
    pub bytes: u64,
}

/// Outcome of a post-restore validation pass (Stage 1G, spec 10.7), in the
/// idiom of [`crate::gc::CheckReport`]/[`crate::gc::DoctorReport`]: soft
/// findings collect in `issues`, hard corruption returns `Err`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RestoreReport {
    /// Manifest-listed files whose size and SHA-256 were re-verified.
    pub files_checked: usize,
    /// Files whose checksum matched the manifest.
    pub files_ok: usize,
    /// Total bytes re-hashed.
    pub bytes_checked: u64,
    /// The catalog decoded. `false` for encrypted trees validated without
    /// the database passphrase (recorded as an issue, not a failure).
    pub catalog_loaded: bool,
    /// Manifest structure, file hashes, and the manifest/file-set equality
    /// all held.
    pub manifest_consistent: bool,
    /// Non-fatal findings.
    pub issues: Vec<String>,
}

/// Post-restore validation pass over a backup or restored database tree:
/// re-verifies the manifest, every listed file size/hash, and the manifest/
/// file-set equality, then loads the catalog. Row counts are intentionally
/// out of scope: opening the tree as a database would take its lock and
/// bump its open generation.
pub fn validate_restore(root: impl AsRef<Path>) -> Result<RestoreReport> {
    let root = crate::durable_file::DurableRoot::open(root)?;
    validate_restore_durable(&root)
}

pub(crate) fn validate_restore_durable(
    root: &crate::durable_file::DurableRoot,
) -> Result<RestoreReport> {
    let (manifest, _) = verify_backup_durable_with_manifest_sha256(root)?;
    let mut report = RestoreReport {
        files_checked: manifest.files.len(),
        files_ok: manifest.files.len(),
        bytes_checked: manifest.total_bytes(),
        manifest_consistent: true,
        ..RestoreReport::default()
    };
    match crate::catalog::read_durable(root, None)? {
        Some(_) => report.catalog_loaded = true,
        None => report
            .issues
            .push("catalog did not decode without a passphrase (encrypted)".into()),
    }
    Ok(report)
}

impl BackupManifest {
    pub(crate) fn create_controlled_durable(
        root: &crate::durable_file::DurableRoot,
        epoch: u64,
        paths: &[PathBuf],
        control: &crate::ExecutionControl,
    ) -> Result<Self> {
        let mut paths = paths.to_vec();
        paths.sort();
        paths.dedup();
        let mut files = Vec::with_capacity(paths.len());
        for (index, path) in paths.into_iter().enumerate() {
            if index % 256 == 0 {
                control.checkpoint()?;
            }
            validate_relative_path(&path)?;
            let mut source = root.open_regular(&path)?;
            let bytes = source.metadata()?.len();
            files.push(BackupFile {
                path,
                bytes,
                sha256: sha256_open_file_inner(&mut source, Some(control))?,
            });
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        Ok(Self {
            format_version: BACKUP_FORMAT_VERSION,
            epoch,
            created_unix_nanos: now.as_nanos() as u64,
            database_id: backup_database_id(root)?,
            catalog_version: backup_catalog_version(root)?,
            snapshot_unix_micros: now.as_micros() as u64,
            open_generation: crate::catalog::read_generation(root)?.unwrap_or(0),
            encryption: backup_encryption_metadata(root)?,
            files,
        })
    }

    pub(crate) fn write_to_durable(&self, root: &crate::durable_file::DurableRoot) -> Result<()> {
        root.create_directory_all("_meta")?;
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|error| MongrelError::Other(format!("backup manifest encode: {error}")))?;
        root.write_new(BACKUP_MANIFEST_PATH, &bytes)?;
        Ok(())
    }

    pub fn total_bytes(&self) -> u64 {
        self.files.iter().map(|file| file.bytes).sum()
    }
}

/// Derive the backup-scoped database ID from the persisted replication
/// identity marker. Read-only: a missing marker degrades to `None` rather
/// than inventing an identity (database identity lifecycle belongs to
/// `database.rs`), while a malformed marker fails closed because the marker
/// is immutable once created.
fn backup_database_id(root: &crate::durable_file::DurableRoot) -> Result<Option<DatabaseId>> {
    let file = match root.open_regular(REPLICATION_ID_PATH) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut bytes = Vec::with_capacity(REPLICATION_ID_LEN);
    file.take(REPLICATION_ID_LEN as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() != REPLICATION_ID_LEN || bytes.iter().all(|byte| *byte == 0) {
        return Err(MongrelError::Other(format!(
            "invalid database replication identity length: got {}, expected {REPLICATION_ID_LEN} nonzero bytes",
            bytes.len()
        )));
    }
    let mut id = [0u8; 16];
    id.copy_from_slice(&bytes[..16]);
    Ok(Some(DatabaseId::from_bytes(id)))
}

/// Catalog version at the backup boundary. Encrypted catalogs cannot be
/// decoded without the database passphrase, which the manifest builder does
/// not hold; those record `0` ("unknown", see [`BackupManifest`]).
fn backup_catalog_version(root: &crate::durable_file::DurableRoot) -> Result<u64> {
    Ok(crate::catalog::read_durable(root, None)?
        .map(|catalog| catalog.catalog_version())
        .unwrap_or(0))
}

fn backup_encryption_metadata(
    root: &crate::durable_file::DurableRoot,
) -> Result<Option<BackupEncryptionMetadata>> {
    match root.open_regular(KEYS_PATH) {
        Ok(_) => Ok(Some(BackupEncryptionMetadata {
            kdf: "argon2id-hkdf-sha256".into(),
            cipher: "aes-256-gcm".into(),
        })),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Verify a backup manifest, every listed file size/hash, and the catalog.
pub fn verify_backup(root: impl AsRef<Path>) -> Result<BackupManifest> {
    let root = crate::durable_file::DurableRoot::open(root)?;
    verify_backup_durable(&root)
}

pub(crate) fn verify_backup_durable(
    root: &crate::durable_file::DurableRoot,
) -> Result<BackupManifest> {
    verify_backup_durable_with_manifest_sha256(root).map(|(manifest, _)| manifest)
}

pub(crate) fn verify_backup_durable_with_manifest_sha256(
    root: &crate::durable_file::DurableRoot,
) -> Result<(BackupManifest, String)> {
    let manifest_bytes = read_backup_manifest_durable(root)?;
    let manifest_sha256 = Sha256::digest(&manifest_bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let manifest: BackupManifest = serde_json::from_slice(&manifest_bytes).map_err(|error| {
        MongrelError::InvalidArgument(format!("invalid backup manifest: {error}"))
    })?;
    if manifest.format_version != BACKUP_FORMAT_VERSION {
        return Err(MongrelError::InvalidArgument(format!(
            "unsupported backup format version {}",
            manifest.format_version
        )));
    }
    if root.open_regular(crate::catalog::CATALOG_FILENAME).is_err() {
        return Err(MongrelError::InvalidArgument(
            "backup has no catalog".into(),
        ));
    }
    let mut expected = HashSet::with_capacity(manifest.files.len() + 1);
    expected.insert(PathBuf::from(BACKUP_MANIFEST_PATH));
    for file in &manifest.files {
        validate_relative_path(&file.path)?;
        if !expected.insert(file.path.clone()) {
            return Err(MongrelError::InvalidArgument(format!(
                "backup manifest lists {} more than once",
                file.path.display()
            )));
        }
        let mut source = root.open_regular(&file.path).map_err(|error| {
            MongrelError::Other(format!("backup file {}: {error}", file.path.display()))
        })?;
        let metadata = source.metadata()?;
        if metadata.len() != file.bytes {
            return Err(MongrelError::Other(format!(
                "backup file {} size mismatch: expected {}, got {}",
                file.path.display(),
                file.bytes,
                metadata.len()
            )));
        }
        let actual = sha256_open_file_inner(&mut source, None)?;
        if actual != file.sha256 {
            return Err(MongrelError::Other(format!(
                "backup file {} checksum mismatch",
                file.path.display()
            )));
        }
    }
    let mut actual = HashSet::new();
    root.walk_regular_files(
        |_, _| Ok(true),
        |_| Ok(()),
        |relative, _| {
            actual.insert(relative.to_path_buf());
            Ok(())
        },
    )?;
    if actual != expected {
        let mut missing = expected.difference(&actual).cloned().collect::<Vec<_>>();
        let mut extra = actual.difference(&expected).cloned().collect::<Vec<_>>();
        missing.sort();
        extra.sort();
        return Err(MongrelError::InvalidArgument(format!(
            "backup file set differs from manifest (missing: {missing:?}; extra: {extra:?})"
        )));
    }
    Ok((manifest, manifest_sha256))
}

fn read_backup_manifest_durable(root: &crate::durable_file::DurableRoot) -> Result<Vec<u8>> {
    let file = root.open_regular(BACKUP_MANIFEST_PATH)?;
    let bytes = file.metadata()?.len();
    if bytes > MAX_BACKUP_MANIFEST_BYTES {
        return Err(MongrelError::ResourceLimitExceeded {
            resource: "backup manifest bytes",
            requested: usize::try_from(bytes).unwrap_or(usize::MAX),
            limit: MAX_BACKUP_MANIFEST_BYTES as usize,
        });
    }
    let mut manifest_bytes = Vec::with_capacity(bytes as usize);
    file.take(MAX_BACKUP_MANIFEST_BYTES + 1)
        .read_to_end(&mut manifest_bytes)?;
    Ok(manifest_bytes)
}

pub(crate) fn copy_file_synced(source: &Path, destination: &Path) -> Result<u64> {
    let mut source = crate::durable_file::open_regular_nofollow(source)?;
    copy_open_file_synced(&mut source, destination)
}

pub(crate) fn copy_open_file_synced(source: &mut std::fs::File, destination: &Path) -> Result<u64> {
    let parent = destination
        .parent()
        .ok_or_else(|| MongrelError::InvalidArgument("invalid backup file path".into()))?;
    std::fs::create_dir_all(parent)?;
    let mut destination = std::fs::File::create(destination)?;
    let bytes = std::io::copy(source, &mut destination)?;
    destination.sync_all()?;
    Ok(bytes)
}

pub(crate) fn sha256_open_file_inner(
    file: &mut std::fs::File,
    control: Option<&crate::ExecutionControl>,
) -> Result<String> {
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        if let Some(control) = control {
            control.checkpoint()?;
        }
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn validate_relative_path(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(MongrelError::InvalidArgument(format!(
            "invalid backup path {}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pre_1g_manifest_json_decodes_with_unknown_audit_fields() {
        let json = r#"{"format_version":1,"epoch":7,"created_unix_nanos":42,"files":[]}"#;
        let manifest: BackupManifest = serde_json::from_str(json).unwrap();
        assert_eq!(manifest.format_version, 1);
        assert_eq!(manifest.epoch, 7);
        assert_eq!(manifest.database_id, None);
        assert_eq!(manifest.catalog_version, 0);
        assert_eq!(manifest.snapshot_unix_micros, 0);
        assert_eq!(manifest.open_generation, 0);
        assert_eq!(manifest.encryption, None);
    }

    #[test]
    fn manifest_serde_round_trip_preserves_audit_fields() {
        let manifest = BackupManifest {
            format_version: BACKUP_FORMAT_VERSION,
            epoch: 9,
            created_unix_nanos: 1_000,
            database_id: Some(DatabaseId::from_bytes([7; 16])),
            catalog_version: 3,
            snapshot_unix_micros: 1,
            open_generation: 2,
            encryption: Some(BackupEncryptionMetadata {
                kdf: "argon2id-hkdf-sha256".into(),
                cipher: "aes-256-gcm".into(),
            }),
            files: vec![BackupFile {
                path: PathBuf::from("CATALOG"),
                bytes: 10,
                sha256: "ab".repeat(32),
            }],
        };
        let bytes = serde_json::to_vec_pretty(&manifest).unwrap();
        let decoded: BackupManifest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded, manifest);
    }

    #[test]
    fn database_id_follows_the_replication_identity_marker() {
        let directory = tempfile::tempdir().unwrap();
        let root = crate::durable_file::DurableRoot::open(directory.path()).unwrap();
        assert_eq!(backup_database_id(&root).unwrap(), None);

        root.create_directory_all("_meta").unwrap();
        let identity = [7u8; REPLICATION_ID_LEN];
        root.write_new(REPLICATION_ID_PATH, &identity).unwrap();
        let id = backup_database_id(&root).unwrap().unwrap();
        assert_eq!(id.as_bytes(), &[7u8; 16]);
        // Stable: a second read derives the same identity.
        assert_eq!(backup_database_id(&root).unwrap(), Some(id));
    }

    #[test]
    fn database_id_fails_closed_on_a_malformed_identity_marker() {
        let directory = tempfile::tempdir().unwrap();
        let root = crate::durable_file::DurableRoot::open(directory.path()).unwrap();
        root.create_directory_all("_meta").unwrap();
        root.write_new(REPLICATION_ID_PATH, &[1, 2, 3]).unwrap();
        assert!(backup_database_id(&root).is_err());

        let zeroed = tempfile::tempdir().unwrap();
        let zeroed = crate::durable_file::DurableRoot::open(zeroed.path()).unwrap();
        zeroed.create_directory_all("_meta").unwrap();
        zeroed
            .write_new(REPLICATION_ID_PATH, &[0u8; REPLICATION_ID_LEN])
            .unwrap();
        assert!(backup_database_id(&zeroed).is_err());
    }
}
