//! Manifest — the atomic pointer to the current set of sorted runs.
//!
//! On-disk layout matches `DBPLAN.md` §6.4. A commit writes `_mf.tmp` then
//! `rename(_mf.tmp, _mf)`, which is atomic on POSIX, giving crash-safe commit.
//! For encrypted DBs the whole blob is AES-256-GCM sealed under the DB-wide
//! meta DEK (confidential + authenticated); for plaintext DBs it carries a
//! SHA-256 integrity tag. Either way the parent directory is fsynced after the
//! rename so the new manifest is durable across a crash (review fix #19).

use crate::encryption::DEK_LEN;
#[cfg(feature = "encryption")]
use crate::encryption::{decrypt_blob, encrypt_blob};
use crate::{MongrelError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

pub const MANIFEST_MAGIC: [u8; 8] = *b"MONGRMFT";
pub const MANIFEST_VERSION: u16 = 1;
pub const MANIFEST_FILENAME: &str = "_mf";
/// 32-byte meta DEK length (matches [`crate::encryption::DEK_LEN`]).
pub const META_DEK_LEN: usize = DEK_LEN;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRef {
    pub run_id: u128,
    pub level: u8,
    pub epoch_created: u64,
    pub row_count: u64,
}

/// A run that compaction superseded but kept on disk for snapshot retention
/// (spec §6.4/§7.4). Its file is physically deleted by `gc()` only once
/// `min_active_snapshot` has advanced past `retire_epoch` — i.e. no pinned
/// reader can still need it. Persisted in the manifest so the reaper survives
/// a reopen (otherwise the file would linger as an orphan).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetiredRun {
    pub run_id: u128,
    /// The compaction epoch at which this run was superseded. Reapable once
    /// `min_active_snapshot > retire_epoch`.
    pub retire_epoch: u64,
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
    /// Highest epoch whose data is durable in a sorted run (spec §7.1). Recovery
    /// may skip replaying WAL records for this table whose commit epoch is
    /// `<= flushed_epoch` (they are already represented by runs).
    #[serde(default)]
    pub flushed_epoch: u64,
    /// Runs superseded by compaction but retained for snapshot retention,
    /// pending physical deletion by `gc()` (spec §6.4). See [`RetiredRun`].
    #[serde(default)]
    pub retiring: Vec<RetiredRun>,
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
            flushed_epoch: 0,
            retiring: Vec::new(),
            checksum: [0u8; 32],
        }
    }

    fn compute_checksum(&mut self) {
        self.checksum = [0u8; 32];
        let bytes = bincode::serialize(self).expect("manifest serializable");
        self.checksum = Sha256::digest(&bytes).into();
    }
}

/// Atomically write the manifest to `<dir>/_mf`. When `meta_dek` is `Some` the
/// blob is AES-256-GCM sealed (confidential + authenticated); otherwise it
/// carries a SHA-256 integrity tag. The parent directory is fsynced after the
/// rename (review fix #19).
pub fn write_atomic(
    dir: impl AsRef<Path>,
    manifest: &mut Manifest,
    meta_dek: Option<&[u8; META_DEK_LEN]>,
) -> Result<()> {
    let dir = dir.as_ref();
    let final_path: PathBuf = dir.join(MANIFEST_FILENAME);
    let tmp_path: PathBuf = dir.join(format!("{MANIFEST_FILENAME}.tmp"));

    manifest.compute_checksum();
    let bytes = bincode::serialize(manifest)?;
    let payload = seal(&bytes, meta_dek)?;
    {
        let mut file = fs::File::create(&tmp_path)?;
        file.write_all(&payload)?;
        file.sync_all()?;
    }
    fs::rename(&tmp_path, &final_path)?;
    if let Ok(d) = fs::File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

/// Read the manifest from `<dir>/_mf`, verifying magic and checksum (plaintext)
/// or the GCM tag (encrypted). `meta_dek` must match the one used at write.
pub fn read(dir: impl AsRef<Path>, meta_dek: Option<&[u8; META_DEK_LEN]>) -> Result<Manifest> {
    let path = dir.as_ref().join(MANIFEST_FILENAME);
    let bytes = fs::read(&path)?;
    let plaintext = open_payload(&bytes, meta_dek)?;
    let manifest: Manifest = bincode::deserialize(&plaintext)?;
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

#[cfg(feature = "encryption")]
fn seal(body: &[u8], meta_dek: Option<&[u8; META_DEK_LEN]>) -> Result<Vec<u8>> {
    match meta_dek {
        Some(dek) => encrypt_blob(dek, body),
        None => Ok(body.to_vec()),
    }
}

#[cfg(not(feature = "encryption"))]
fn seal(body: &[u8], _meta_dek: Option<&[u8; META_DEK_LEN]>) -> Result<Vec<u8>> {
    Ok(body.to_vec())
}

#[cfg(feature = "encryption")]
fn open_payload(bytes: &[u8], meta_dek: Option<&[u8; META_DEK_LEN]>) -> Result<Vec<u8>> {
    match meta_dek {
        Some(dek) => decrypt_blob(dek, bytes),
        None => Ok(bytes.to_vec()),
    }
}

#[cfg(not(feature = "encryption"))]
fn open_payload(bytes: &[u8], _meta_dek: Option<&[u8; META_DEK_LEN]>) -> Result<Vec<u8>> {
    Ok(bytes.to_vec())
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
        m.flushed_epoch = 7;
        m.runs.push(RunRef {
            run_id: 0xDEAD,
            level: 0,
            epoch_created: 8,
            row_count: 42,
        });
        write_atomic(dir.path(), &mut m, None).unwrap();

        let read_back = read(dir.path(), None).unwrap();
        assert_eq!(read_back.table_id, 10);
        assert_eq!(read_back.current_epoch, 9);
        assert_eq!(read_back.next_row_id, 100);
        assert_eq!(read_back.flushed_epoch, 7);
        assert_eq!(read_back.runs.len(), 1);
        assert_eq!(read_back.runs[0].run_id, 0xDEAD);
    }

    #[test]
    fn detects_tampering() {
        let dir = tempdir().unwrap();
        let mut m = Manifest::new(1, 1);
        m.current_epoch = 5;
        write_atomic(dir.path(), &mut m, None).unwrap();

        // Corrupt a byte.
        let path = dir.path().join(MANIFEST_FILENAME);
        let mut bytes = fs::read(&path).unwrap();
        bytes[20] ^= 0xFF;
        fs::write(&path, bytes).unwrap();

        let err = read(dir.path(), None).unwrap_err();
        assert!(
            matches!(
                err,
                MongrelError::ChecksumMismatch { .. } | MongrelError::MagicMismatch { .. }
            ),
            "got {err:?}"
        );
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn encrypted_manifest_roundtrips_and_rejects_wrong_key() {
        let dir = tempdir().unwrap();
        let dek = [42u8; 32];
        let mut m = Manifest::new(2, 9);
        m.current_epoch = 3;
        m.flushed_epoch = 2;
        write_atomic(dir.path(), &mut m, Some(&dek)).unwrap();
        let back = read(dir.path(), Some(&dek)).unwrap();
        assert_eq!(back.current_epoch, 3);
        assert_eq!(back.flushed_epoch, 2);
        // wrong key -> GCM auth failure
        let wrong = [0u8; 32];
        assert!(read(dir.path(), Some(&wrong)).is_err());
    }
}
