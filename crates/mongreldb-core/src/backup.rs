//! Online backup manifest and verification.

use crate::{MongrelError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::{Component, Path, PathBuf};

pub const BACKUP_FORMAT_VERSION: u16 = 1;
pub const BACKUP_MANIFEST_PATH: &str = "_meta/backup.json";

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
    pub files: usize,
    pub bytes: u64,
}

impl BackupManifest {
    pub(crate) fn create(root: &Path, epoch: u64, paths: &[PathBuf]) -> Result<Self> {
        let mut paths = paths.to_vec();
        paths.sort();
        paths.dedup();
        let mut files = Vec::with_capacity(paths.len());
        for path in paths {
            validate_relative_path(&path)?;
            let absolute = root.join(&path);
            let metadata = std::fs::metadata(&absolute)?;
            if !metadata.is_file() {
                return Err(MongrelError::InvalidArgument(format!(
                    "backup entry is not a file: {}",
                    path.display()
                )));
            }
            files.push(BackupFile {
                path,
                bytes: metadata.len(),
                sha256: sha256_file(&absolute)?,
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

    pub(crate) fn write(&self, root: &Path) -> Result<()> {
        let path = root.join(BACKUP_MANIFEST_PATH);
        let parent = path
            .parent()
            .ok_or_else(|| MongrelError::InvalidArgument("invalid backup manifest path".into()))?;
        std::fs::create_dir_all(parent)?;
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|error| MongrelError::Other(format!("backup manifest encode: {error}")))?;
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)?;
        use std::io::Write;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(())
    }

    pub fn total_bytes(&self) -> u64 {
        self.files.iter().map(|file| file.bytes).sum()
    }
}

/// Verify a backup manifest, every listed file size/hash, and the catalog.
pub fn verify_backup(root: impl AsRef<Path>) -> Result<BackupManifest> {
    let root = root.as_ref();
    let manifest: BackupManifest = serde_json::from_slice(&std::fs::read(
        root.join(BACKUP_MANIFEST_PATH),
    )?)
    .map_err(|error| MongrelError::InvalidArgument(format!("invalid backup manifest: {error}")))?;
    if manifest.format_version != BACKUP_FORMAT_VERSION {
        return Err(MongrelError::InvalidArgument(format!(
            "unsupported backup format version {}",
            manifest.format_version
        )));
    }
    if !root.join(crate::catalog::CATALOG_FILENAME).is_file() {
        return Err(MongrelError::InvalidArgument(
            "backup has no catalog".into(),
        ));
    }
    for file in &manifest.files {
        validate_relative_path(&file.path)?;
        let path = root.join(&file.path);
        let metadata = std::fs::metadata(&path).map_err(|error| {
            MongrelError::Other(format!("backup file {}: {error}", file.path.display()))
        })?;
        if metadata.len() != file.bytes {
            return Err(MongrelError::Other(format!(
                "backup file {} size mismatch: expected {}, got {}",
                file.path.display(),
                file.bytes,
                metadata.len()
            )));
        }
        let actual = sha256_file(&path)?;
        if actual != file.sha256 {
            return Err(MongrelError::Other(format!(
                "backup file {} checksum mismatch",
                file.path.display()
            )));
        }
    }
    Ok(manifest)
}

pub(crate) fn copy_file_synced(source: &Path, destination: &Path) -> Result<u64> {
    let parent = destination
        .parent()
        .ok_or_else(|| MongrelError::InvalidArgument("invalid backup file path".into()))?;
    std::fs::create_dir_all(parent)?;
    let bytes = std::fs::copy(source, destination)?;
    std::fs::File::open(destination)?.sync_all()?;
    Ok(bytes)
}

pub(crate) fn sync_directories(root: &Path) -> Result<()> {
    let mut directories = vec![root.to_path_buf()];
    collect_directories(root, &mut directories)?;
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        std::fs::File::open(directory)?.sync_all()?;
    }
    Ok(())
}

fn collect_directories(directory: &Path, output: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            output.push(entry.path());
            collect_directories(&entry.path(), output)?;
        }
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
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
