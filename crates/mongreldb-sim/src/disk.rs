//! Virtual durable store with injectable faults (spec section 9.5,
//! FND-005).
//!
//! Each simulated node owns a [`VirtualDisk`]. Bytes pass through two
//! stages: `pending` (written, not yet durable) and `durable` (fsynced).
//! [`VirtualDisk::crash`] models power loss by discarding every pending
//! byte: crash recovery exposes exactly the fsynced prefix, and unsynced
//! data is lost. Tests can inject write failures, fsync failures, and
//! torn (partial) writes.

use std::collections::BTreeMap;

/// The failure surface of a virtual disk operation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DiskError {
    /// An injected failure fired during a write.
    #[error("injected write failure on `{path}`")]
    WriteFailed {
        /// The file being written.
        path: String,
    },
    /// An injected failure fired during an fsync.
    #[error("injected fsync failure on `{path}`")]
    FsyncFailed {
        /// The file being synced.
        path: String,
    },
}

#[derive(Debug, Default)]
struct FileState {
    durable: Vec<u8>,
    pending: Vec<u8>,
}

/// A crashable virtual disk with two-stage durability and fault knobs.
#[derive(Debug, Default)]
pub struct VirtualDisk {
    files: BTreeMap<String, FileState>,
    fail_writes: bool,
    fail_fsyncs: bool,
    fail_next_writes: u32,
    fail_next_fsyncs: u32,
    torn_limit: Option<usize>,
}

impl VirtualDisk {
    /// An empty disk with no faults armed.
    pub fn new() -> Self {
        Self::default()
    }

    /// Fails every write while `enabled` is true.
    pub fn set_write_failures(&mut self, enabled: bool) {
        self.fail_writes = enabled;
    }

    /// Fails every fsync while `enabled` is true.
    pub fn set_fsync_failures(&mut self, enabled: bool) {
        self.fail_fsyncs = enabled;
    }

    /// Fails only the next write.
    pub fn fail_next_write(&mut self) {
        self.fail_next_writes += 1;
    }

    /// Fails only the next fsync.
    pub fn fail_next_fsync(&mut self) {
        self.fail_next_fsyncs += 1;
    }

    /// Truncates every write to at most `limit` bytes (a torn write).
    /// `None` disables truncation.
    pub fn set_torn_write_limit(&mut self, limit: Option<usize>) {
        self.torn_limit = limit;
    }

    /// Appends bytes to the pending stage. Returns the number of bytes
    /// accepted, which a torn write may shorten. The bytes are not
    /// durable until [`VirtualDisk::fsync`].
    pub fn append(&mut self, path: &str, bytes: &[u8]) -> Result<usize, DiskError> {
        if self.fail_next_writes > 0 {
            self.fail_next_writes -= 1;
            return Err(DiskError::WriteFailed {
                path: path.to_string(),
            });
        }
        if self.fail_writes {
            return Err(DiskError::WriteFailed {
                path: path.to_string(),
            });
        }
        let written = self
            .torn_limit
            .map_or(bytes.len(), |limit| bytes.len().min(limit));
        self.files
            .entry(path.to_string())
            .or_default()
            .pending
            .extend_from_slice(&bytes[..written]);
        Ok(written)
    }

    /// Moves all pending bytes of `path` to the durable stage.
    pub fn fsync(&mut self, path: &str) -> Result<(), DiskError> {
        if self.fail_next_fsyncs > 0 {
            self.fail_next_fsyncs -= 1;
            return Err(DiskError::FsyncFailed {
                path: path.to_string(),
            });
        }
        if self.fail_fsyncs {
            return Err(DiskError::FsyncFailed {
                path: path.to_string(),
            });
        }
        let file = self.files.entry(path.to_string()).or_default();
        let pending = std::mem::take(&mut file.pending);
        file.durable.extend_from_slice(&pending);
        Ok(())
    }

    /// Live view: durable bytes followed by not-yet-fsynced bytes.
    pub fn read(&self, path: &str) -> Vec<u8> {
        let mut data = self.read_durable(path);
        if let Some(file) = self.files.get(path) {
            data.extend_from_slice(&file.pending);
        }
        data
    }

    /// Crash-recovery view: exactly the fsynced prefix.
    pub fn read_durable(&self, path: &str) -> Vec<u8> {
        self.files
            .get(path)
            .map_or_else(Vec::new, |file| file.durable.clone())
    }

    /// Number of fsynced bytes for `path`.
    pub fn durable_len(&self, path: &str) -> usize {
        self.files.get(path).map_or(0, |file| file.durable.len())
    }

    /// Number of written-but-not-fsynced bytes for `path`.
    pub fn pending_len(&self, path: &str) -> usize {
        self.files.get(path).map_or(0, |file| file.pending.len())
    }

    /// Simulates power loss: every pending (un-fsynced) byte is lost,
    /// on every file.
    pub fn crash(&mut self) {
        for file in self.files.values_mut() {
            file.pending.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fsync_makes_bytes_durable_and_crash_drops_pending() {
        let mut disk = VirtualDisk::new();
        assert_eq!(disk.append("wal", b"aa").unwrap(), 2);
        assert_eq!(disk.pending_len("wal"), 2);
        assert_eq!(disk.durable_len("wal"), 0);
        disk.fsync("wal").unwrap();
        assert_eq!(disk.append("wal", b"bb").unwrap(), 2);
        assert_eq!(disk.read("wal"), b"aabb");

        disk.crash();
        assert_eq!(disk.read("wal"), b"aa");
        assert_eq!(disk.read_durable("wal"), b"aa");
        assert_eq!(disk.pending_len("wal"), 0);
    }

    #[test]
    fn torn_writes_store_only_a_prefix() {
        let mut disk = VirtualDisk::new();
        disk.set_torn_write_limit(Some(3));
        assert_eq!(disk.append("f", b"abcdef").unwrap(), 3);
        assert_eq!(disk.read("f"), b"abc");

        disk.set_torn_write_limit(None);
        assert_eq!(disk.append("f", b"de").unwrap(), 2);
        assert_eq!(disk.read("f"), b"abcde");
    }

    #[test]
    fn injected_write_failures_fire_and_clear() {
        let mut disk = VirtualDisk::new();
        disk.fail_next_write();
        assert_eq!(
            disk.append("f", b"x"),
            Err(DiskError::WriteFailed {
                path: "f".to_string()
            })
        );
        assert_eq!(disk.append("f", b"x").unwrap(), 1);

        disk.set_write_failures(true);
        assert!(disk.append("f", b"y").is_err());
        disk.set_write_failures(false);
        assert_eq!(disk.append("f", b"y").unwrap(), 1);
        assert_eq!(disk.read("f"), b"xy");
    }

    #[test]
    fn injected_fsync_failures_keep_pending_bytes() {
        let mut disk = VirtualDisk::new();
        disk.set_fsync_failures(true);
        disk.append("f", b"abc").unwrap();
        assert_eq!(
            disk.fsync("f"),
            Err(DiskError::FsyncFailed {
                path: "f".to_string()
            })
        );
        assert_eq!(disk.pending_len("f"), 3);
        assert_eq!(disk.durable_len("f"), 0);

        disk.set_fsync_failures(false);
        disk.fail_next_fsync();
        assert!(disk.fsync("f").is_err());
        disk.fsync("f").unwrap();
        assert_eq!(disk.read_durable("f"), b"abc");
    }
}
