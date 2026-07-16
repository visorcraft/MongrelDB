//! Online backup manifest and verification.

use crate::{MongrelError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::io::Read;
use std::path::{Component, Path, PathBuf};

pub const BACKUP_FORMAT_VERSION: u16 = 1;
pub const BACKUP_MANIFEST_PATH: &str = "_meta/backup.json";
const MAX_BACKUP_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupFile {
    pub path: PathBuf,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupManifest {
    pub format_version: u16,
    pub epoch: u64,
    pub created_unix_nanos: u64,
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
        Ok(Self {
            format_version: BACKUP_FORMAT_VERSION,
            epoch,
            created_unix_nanos: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
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

fn sha256_open_file_inner(
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
