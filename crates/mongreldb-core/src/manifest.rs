//! Manifest — the atomic pointer to the current set of sorted runs.
//!
//! On-disk layout matches `DBPLAN.md` §6.4. A commit writes `_mf.tmp` then
//! `rename(_mf.tmp, _mf)`, which is atomic on POSIX, giving crash-safe commit.

use crate::{MongrelError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

pub const MANIFEST_MAGIC: [u8; 8] = *b"MONGRMFT";
pub const MANIFEST_VERSION: u16 = 1;
pub const MANIFEST_FILENAME: &str = "_mf";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRef {
    pub run_id: u128,
    pub level: u8,
    pub epoch_created: u64,
    pub row_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub magic: [u8; 8],
    pub format_version: u16,
    pub table_id: u64,
    pub current_epoch: u64,
    pub next_row_id: u64,
    pub schema_id: u64,
    pub runs: Vec<RunRef>,
    pub global_idx_epoch: u64,
    /// Live (non-deleted) row count, maintained incrementally so `COUNT(*)` is
    /// O(1) from the manifest without a scan.
    pub live_count: u64,
    pub checksum: [u8; 32],
}

impl Manifest {
    pub fn new(table_id: u64, schema_id: u64) -> Self {
        Self {
            magic: MANIFEST_MAGIC,
            format_version: MANIFEST_VERSION,
            table_id,
            current_epoch: 0,
            next_row_id: 0,
            schema_id,
            runs: Vec::new(),
            global_idx_epoch: 0,
            live_count: 0,
            checksum: [0u8; 32],
        }
    }

    fn compute_checksum(&mut self) {
        self.checksum = [0u8; 32];
        let bytes = bincode::serialize(self).expect("manifest serializable");
        self.checksum = Sha256::digest(&bytes).into();
    }
}

/// Atomically write the manifest to `<dir>/_mf`.
pub fn write_atomic(dir: impl AsRef<Path>, manifest: &mut Manifest) -> Result<()> {
    let dir = dir.as_ref();
    let final_path: PathBuf = dir.join(MANIFEST_FILENAME);
    let tmp_path: PathBuf = dir.join(format!("{MANIFEST_FILENAME}.tmp"));

    manifest.compute_checksum();
    let bytes = bincode::serialize(manifest)?;
    {
        let mut file = fs::File::create(&tmp_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp_path, &final_path)?;
    Ok(())
}

/// Read the manifest from `<dir>/_mf`, verifying magic and checksum.
pub fn read(dir: impl AsRef<Path>) -> Result<Manifest> {
    let path = dir.as_ref().join(MANIFEST_FILENAME);
    let bytes = fs::read(&path)?;
    let manifest: Manifest = bincode::deserialize(&bytes)?;
    if manifest.magic != MANIFEST_MAGIC {
        return Err(MongrelError::MagicMismatch {
            what: "manifest",
            expected: MANIFEST_MAGIC,
            got: manifest.magic,
        });
    }
    // Recompute the checksum (over a copy with checksum zeroed).
    let mut zeroed = manifest.clone();
    zeroed.checksum = [0u8; 32];
    let recomputed: [u8; 32] = Sha256::digest(&bincode::serialize(&zeroed)?).into();
    if recomputed != manifest.checksum {
        return Err(MongrelError::ChecksumMismatch {
            expected: u64::from_be_bytes(manifest.checksum[..8].try_into().unwrap()),
            actual: u64::from_be_bytes(recomputed[..8].try_into().unwrap()),
            context: "manifest".into(),
        });
    }
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn write_then_read_roundtrips() {
        let dir = tempdir().unwrap();
        let mut m = Manifest::new(10, 3);
        m.current_epoch = 9;
        m.next_row_id = 100;
        m.runs.push(RunRef {
            run_id: 0xDEAD,
            level: 0,
            epoch_created: 8,
            row_count: 42,
        });
        write_atomic(dir.path(), &mut m).unwrap();

        let read_back = read(dir.path()).unwrap();
        assert_eq!(read_back.table_id, 10);
        assert_eq!(read_back.current_epoch, 9);
        assert_eq!(read_back.next_row_id, 100);
        assert_eq!(read_back.runs.len(), 1);
        assert_eq!(read_back.runs[0].run_id, 0xDEAD);
    }

    #[test]
    fn detects_tampering() {
        let dir = tempdir().unwrap();
        let mut m = Manifest::new(1, 1);
        m.current_epoch = 5;
        write_atomic(dir.path(), &mut m).unwrap();

        // Corrupt a byte.
        let path = dir.path().join(MANIFEST_FILENAME);
        let mut bytes = fs::read(&path).unwrap();
        bytes[20] ^= 0xFF;
        fs::write(&path, bytes).unwrap();

        let err = read(dir.path()).unwrap_err();
        assert!(
            matches!(
                err,
                MongrelError::ChecksumMismatch { .. } | MongrelError::MagicMismatch { .. }
            ),
            "got {err:?}"
        );
    }
}
