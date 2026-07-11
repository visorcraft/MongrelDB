//! Replication bootstrap image and follower metadata.

use crate::{MongrelError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::Write;
use std::path::{Component, Path, PathBuf};

const FORMAT_VERSION: u16 = 1;
const REPLICA_MARKER: &str = "replica";
const REPLICA_EPOCH: &str = "repl_epoch";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReplicationFile {
    path: PathBuf,
    data: Vec<u8>,
}

impl ReplicationFile {
    pub(crate) fn new(path: PathBuf, data: Vec<u8>) -> Self {
        Self { path, data }
    }
}

pub(crate) fn capture_files(root: &Path) -> Result<Vec<ReplicationFile>> {
    let mut files = Vec::new();
    capture_dir(root, root, &mut files)?;
    files.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(files)
}

fn capture_dir(root: &Path, dir: &Path, files: &mut Vec<ReplicationFile>) -> Result<()> {
    let mut entries = std::fs::read_dir(dir)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        let relative = path
            .strip_prefix(root)
            .map_err(|error| MongrelError::Other(error.to_string()))?;
        if relative == Path::new("_meta/.lock")
            || relative == Path::new("_meta/replica")
            || relative == Path::new("_meta/repl_epoch")
            || relative
                .components()
                .any(|component| matches!(component, Component::Normal(name) if name == "_cache"))
        {
            continue;
        }
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Err(MongrelError::InvalidArgument(format!(
                "replication snapshot refuses symlink {}",
                path.display()
            )));
        }
        if file_type.is_dir() {
            capture_dir(root, &path, files)?;
        } else if file_type.is_file() {
            files.push(ReplicationFile::new(
                relative.to_path_buf(),
                std::fs::read(&path)?,
            ));
        }
    }
    Ok(())
}

/// A consistent database-directory image plus the leader commit epoch it
/// covers. The image is opaque to HTTP; encode/decode use the core's versioned
/// bincode envelope so server and client share one format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationSnapshot {
    version: u16,
    epoch: u64,
    files: Vec<ReplicationFile>,
}

/// Complete committed WAL transactions available after a follower epoch.
#[derive(Debug, Clone)]
pub struct ReplicationBatch {
    pub current_epoch: u64,
    pub earliest_epoch: Option<u64>,
    pub requires_snapshot: bool,
    pub records: Vec<crate::wal::Record>,
}

impl ReplicationSnapshot {
    pub(crate) fn new(epoch: u64, files: Vec<ReplicationFile>) -> Self {
        Self {
            version: FORMAT_VERSION,
            epoch,
            files,
        }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let snapshot: Self = bincode::deserialize(bytes)?;
        if snapshot.version != FORMAT_VERSION {
            return Err(MongrelError::InvalidArgument(format!(
                "unsupported replication snapshot version {}",
                snapshot.version
            )));
        }
        Ok(snapshot)
    }

    /// Atomically replace `destination` with this snapshot and mark it as a
    /// read-only replica. Files are first written and fsynced in a sibling
    /// staging directory; an existing destination is retained until install.
    pub fn install(&self, destination: impl AsRef<Path>) -> Result<()> {
        let destination = destination.as_ref();
        let parent = destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        let name = destination
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| MongrelError::InvalidArgument("invalid replica destination".into()))?;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let stage = parent.join(format!(
            ".{name}.replica-stage-{}-{nonce}",
            std::process::id()
        ));
        let backup = parent.join(format!(
            ".{name}.replica-old-{}-{nonce}",
            std::process::id()
        ));

        if stage.exists() || backup.exists() {
            return Err(MongrelError::Conflict(
                "replication staging path already exists".into(),
            ));
        }
        std::fs::create_dir(&stage)?;
        if let Err(error) = self.write_into(&stage) {
            let _ = std::fs::remove_dir_all(&stage);
            return Err(error);
        }

        let had_destination = destination.exists();
        if had_destination {
            std::fs::rename(destination, &backup)?;
        }
        if let Err(error) = std::fs::rename(&stage, destination) {
            if had_destination {
                let _ = std::fs::rename(&backup, destination);
            }
            let _ = std::fs::remove_dir_all(&stage);
            return Err(error.into());
        }
        sync_dir(parent);
        if had_destination {
            std::fs::remove_dir_all(&backup)?;
        }
        Ok(())
    }

    fn write_into(&self, root: &Path) -> Result<()> {
        let mut seen = HashSet::new();
        for file in &self.files {
            validate_relative_path(&file.path)?;
            if !seen.insert(file.path.clone()) {
                return Err(MongrelError::InvalidArgument(format!(
                    "duplicate replication snapshot path {:?}",
                    file.path
                )));
            }
            let path = root.join(&file.path);
            let parent = path.parent().expect("validated file has parent");
            std::fs::create_dir_all(parent)?;
            let mut output = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)?;
            output.write_all(&file.data)?;
            output.sync_all()?;
        }
        if !root.join(crate::catalog::CATALOG_FILENAME).is_file() {
            return Err(MongrelError::InvalidArgument(
                "replication snapshot has no CATALOG".into(),
            ));
        }
        let meta = root.join("_meta");
        std::fs::create_dir_all(&meta)?;
        write_synced(&meta.join(REPLICA_MARKER), b"read-only replica\n")?;
        write_replica_epoch(root, self.epoch)?;
        sync_dir(&meta);
        sync_dir(root);
        Ok(())
    }
}

pub fn is_replica(root: impl AsRef<Path>) -> bool {
    root.as_ref().join("_meta").join(REPLICA_MARKER).is_file()
}

pub fn replica_epoch(root: impl AsRef<Path>) -> Result<u64> {
    let path = root.as_ref().join("_meta").join(REPLICA_EPOCH);
    let value = std::fs::read_to_string(&path)
        .map_err(|error| MongrelError::NotFound(format!("{}: {error}", path.display())))?;
    value.trim().parse().map_err(|error| {
        MongrelError::InvalidArgument(format!(
            "invalid replica epoch in {}: {error}",
            path.display()
        ))
    })
}

pub fn write_replica_epoch(root: impl AsRef<Path>, epoch: u64) -> Result<()> {
    let meta = root.as_ref().join("_meta");
    std::fs::create_dir_all(&meta)?;
    let path = meta.join(REPLICA_EPOCH);
    let temp = meta.join(format!("{REPLICA_EPOCH}.tmp"));
    write_synced(&temp, epoch.to_string().as_bytes())?;
    std::fs::rename(&temp, &path)?;
    sync_dir(&meta);
    Ok(())
}

fn validate_relative_path(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(MongrelError::InvalidArgument(format!(
            "unsafe replication snapshot path {:?}",
            path
        )));
    }
    Ok(())
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = std::fs::File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn sync_dir(path: &Path) {
    if let Ok(dir) = std::fs::File::open(path) {
        let _ = dir.sync_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_install_rejects_path_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = ReplicationSnapshot {
            version: FORMAT_VERSION,
            epoch: 1,
            files: vec![ReplicationFile::new("../escape".into(), vec![1])],
        };
        assert!(snapshot.install(dir.path().join("replica")).is_err());
        assert!(!dir.path().join("escape").exists());
    }
}
