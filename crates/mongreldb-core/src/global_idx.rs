//! Global index file — `_idx/global.idx`.
//!
//! On flush/bulk-load the in-memory secondary indexes (HOT, bitmap, FM, ANN,
//! sparse, learned-range) are checkpointed here so [`crate::engine::Table::open`]
//! can load them directly instead of scanning every sorted run. The file is
//! self-describing and integrity-checked:
//!
//! ```text
//!    8   MAGIC = b"MONGRIDX"
//!    ..   bincode(GlobalIdxBody)
//!    8   MAGIC = b"MONGRIDX"
//!   32   SHA-256 over [0..footer)
//! ```
//! Each `GlobalIdxBody` carries the `epoch_built` (the manifest epoch the
//! checkpoint covers) and a list of framed `(kind, column_id, payload)`
//! records. The manifest's `global_idx_epoch` is the authoritative pointer; on
//! open the checkpoint is used only when its embedded `epoch_built` equals the
//! manifest value and every run was created at or before it.

use crate::index::{ColumnLearnedRange, ColumnLearnedRangeSnapshot};
use crate::rowid::RowId;
use crate::{MongrelError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub const IDX_MAGIC: [u8; 8] = *b"MONGRIDX";
pub const IDX_VERSION: u16 = 2;
pub const IDX_DIR: &str = "_idx";
pub const IDX_FILENAME: &str = "global.idx";

// Record kind bytes for framed index payloads.
const K_HOT: u8 = 1;
const K_BITMAP: u8 = 3;
const K_FM: u8 = 4;
const K_ANN: u8 = 5;
const K_SPARSE: u8 = 9;
const K_LEARNED: u8 = 10;
const K_MINHASH: u8 = 11;

/// All in-memory secondary indexes bundled for a single checkpoint write.
pub struct IndexSnapshot<'a> {
    pub hot: &'a crate::index::HotIndex,
    pub bitmap: &'a HashMap<u16, crate::index::BitmapIndex>,
    pub ann: &'a HashMap<u16, crate::index::AnnIndex>,
    pub fm: &'a HashMap<u16, crate::index::FmIndex>,
    pub sparse: &'a HashMap<u16, crate::index::SparseIndex>,
    pub minhash: &'a HashMap<u16, crate::index::MinHashIndex>,
    pub learned_range: &'a HashMap<u16, ColumnLearnedRange>,
}

/// A loaded checkpoint plus the epoch it was built at.
pub struct LoadedIndexes {
    pub epoch_built: u64,
    pub hot: crate::index::HotIndex,
    pub bitmap: HashMap<u16, crate::index::BitmapIndex>,
    pub ann: HashMap<u16, crate::index::AnnIndex>,
    pub fm: HashMap<u16, crate::index::FmIndex>,
    pub sparse: HashMap<u16, crate::index::SparseIndex>,
    pub minhash: HashMap<u16, crate::index::MinHashIndex>,
    pub learned_range: HashMap<u16, ColumnLearnedRange>,
}

#[derive(Serialize, Deserialize)]
struct GlobalIdxBody {
    format_version: u16,
    table_id: u64,
    epoch_built: u64,
    records: Vec<Record>,
}

#[derive(Serialize, Deserialize)]
struct Record {
    kind: u8,
    column_id: u16,
    payload: Vec<u8>,
}

/// Path of the checkpoint file under `dir`.
pub fn path(dir: &Path) -> PathBuf {
    dir.join(IDX_DIR).join(IDX_FILENAME)
}

/// Atomically write the checkpoint: serialize all indexes, write
/// `_idx/global.idx.tmp`, fsync, rename, fsync the dir. The caller is
/// responsible for bumping `manifest.global_idx_epoch` afterwards.
/// Wrap the plaintext checkpoint blob for disk: encrypted (`[nonce][GCM]`) when
/// a DEK is present (the table is encrypted), else returned unchanged. The
/// plaintext checkpoint embeds index keys / PGM segment values derived from
/// user data, so for an encrypted table it must not hit disk in the clear.
fn encode_file(plain: Vec<u8>, dek: Option<&[u8; 32]>) -> Result<Vec<u8>> {
    #[cfg(feature = "encryption")]
    {
        if let Some(k) = dek {
            return crate::encryption::encrypt_blob(k, &plain);
        }
    }
    #[cfg(not(feature = "encryption"))]
    {
        let _ = dek;
    }
    Ok(plain)
}

/// Inverse of [`encode_file`]. Returns `None` when an encrypted checkpoint fails
/// to decrypt (wrong key, tamper, or corruption) so the caller simply rebuilds
/// from the runs.
fn decode_file(raw: Vec<u8>, dek: Option<&[u8; 32]>) -> Option<Vec<u8>> {
    #[cfg(feature = "encryption")]
    {
        if let Some(k) = dek {
            return crate::encryption::decrypt_blob(k, &raw).ok();
        }
    }
    #[cfg(not(feature = "encryption"))]
    {
        let _ = dek;
    }
    Some(raw)
}

pub fn write_atomic(
    dir: &Path,
    table_id: u64,
    epoch_built: u64,
    snap: IndexSnapshot<'_>,
    dek: Option<&[u8; 32]>,
) -> Result<()> {
    let mut records = Vec::new();

    // HOT (kind 1, column_id unused).
    let hot_entries = snap.hot.entries();
    if !hot_entries.is_empty() {
        records.push(Record {
            kind: K_HOT,
            column_id: 0,
            payload: bincode::serialize(&hot_entries)?,
        });
    }

    for (&cid, bm) in snap.bitmap {
        let entries = bm.entries();
        if !entries.is_empty() {
            records.push(Record {
                kind: K_BITMAP,
                column_id: cid,
                payload: bincode::serialize(&entries)?,
            });
        }
    }
    for (&cid, ann) in snap.ann {
        if !ann.is_empty() {
            records.push(Record {
                kind: K_ANN,
                column_id: cid,
                payload: ann.freeze(),
            });
        }
    }
    for (&cid, fm) in snap.fm {
        if fm.doc_count() > 0 {
            let docs = fm.docs();
            records.push(Record {
                kind: K_FM,
                column_id: cid,
                payload: bincode::serialize(&docs)?,
            });
        }
    }
    for (&cid, sp) in snap.sparse {
        if !sp.is_empty() {
            let entries = sp.entries();
            records.push(Record {
                kind: K_SPARSE,
                column_id: cid,
                payload: bincode::serialize(&entries)?,
            });
        }
    }
    for (&cid, mh) in snap.minhash {
        if !mh.is_empty() {
            let entries = mh.entries();
            records.push(Record {
                kind: K_MINHASH,
                column_id: cid,
                payload: bincode::serialize(&entries)?,
            });
        }
    }
    for (&cid, lr) in snap.learned_range {
        let s = lr.snapshot();
        records.push(Record {
            kind: K_LEARNED,
            column_id: cid,
            payload: bincode::serialize(&s)?,
        });
    }

    let body = GlobalIdxBody {
        format_version: IDX_VERSION,
        table_id,
        epoch_built,
        records,
    };
    let body_bytes = bincode::serialize(&body)?;

    let idx_dir = dir.join(IDX_DIR);
    std::fs::create_dir_all(&idx_dir)?;
    let final_path = idx_dir.join(IDX_FILENAME);
    let tmp_path = idx_dir.join(format!("{IDX_FILENAME}.tmp"));

    // MAGIC || body || MAGIC || checksum
    let mut out = Vec::with_capacity(8 + body_bytes.len() + 8 + 32);
    out.extend_from_slice(&IDX_MAGIC);
    out.extend_from_slice(&body_bytes);
    out.extend_from_slice(&IDX_MAGIC);
    let hash: [u8; 32] = Sha256::digest(&out).into();
    out.extend_from_slice(&hash);

    // Encrypt the whole blob at rest for encrypted tables (GCM also authenticates,
    // so the inner SHA-256 becomes a redundant corruption check — kept for format
    // uniformity with the plaintext path).
    let out = encode_file(out, dek)?;

    {
        let mut file = std::fs::File::create(&tmp_path)?;
        use std::io::Write;
        file.write_all(&out)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp_path, &final_path)?;
    Ok(())
}

/// Read and validate the checkpoint. Verifies both MAGIC sentinels and the
/// trailing SHA-256 before deserializing records.
pub fn read(dir: &Path, dek: Option<&[u8; 32]>) -> Result<Option<LoadedIndexes>> {
    let path = path(dir);
    let raw = match std::fs::read(&path) {
        Ok(b) => b,
        Err(_) => return Ok(None),
    };
    // Decrypt first for encrypted tables; a decryption failure (wrong key, tamper,
    // or a pre-encryption checkpoint) → rebuild from runs.
    let bytes = match decode_file(raw, dek) {
        Some(b) => b,
        None => return Ok(None),
    };
    if bytes.len() < 8 + 8 + 32 {
        return Ok(None); // too short to be valid; rebuild instead
    }

    let header_magic = &bytes[..8];
    if header_magic != IDX_MAGIC {
        return Err(MongrelError::MagicMismatch {
            what: "global index",
            expected: IDX_MAGIC,
            got: header_magic.try_into().unwrap_or([0; 8]),
        });
    }
    let footer_start = bytes.len() - 32 - 8;
    let footer_magic = &bytes[footer_start..footer_start + 8];
    if footer_magic != IDX_MAGIC {
        return Err(MongrelError::MagicMismatch {
            what: "global index footer",
            expected: IDX_MAGIC,
            got: footer_magic.try_into().unwrap_or([0; 8]),
        });
    }
    let stored_hash = &bytes[bytes.len() - 32..];
    let recomputed: [u8; 32] = Sha256::digest(&bytes[..bytes.len() - 32]).into();
    if stored_hash != recomputed {
        return Err(MongrelError::ChecksumMismatch {
            expected: u64::from_be_bytes(stored_hash[..8].try_into().unwrap()),
            actual: u64::from_be_bytes(recomputed[..8].try_into().unwrap()),
            context: "global index".into(),
        });
    }

    let body: GlobalIdxBody = bincode::deserialize(&bytes[8..footer_start])?;
    if body.format_version != IDX_VERSION {
        return Ok(None);
    }

    let mut hot = crate::index::HotIndex::new();
    let mut bitmap = HashMap::new();
    let mut ann = HashMap::new();
    let mut fm = HashMap::new();
    let mut sparse = HashMap::new();
    let mut minhash = HashMap::new();
    let mut learned_range = HashMap::new();

    for rec in body.records {
        match rec.kind {
            K_HOT => {
                let entries: Vec<(Vec<u8>, RowId)> = bincode::deserialize(&rec.payload)?;
                hot = crate::index::HotIndex::from_entries(entries);
            }
            K_BITMAP => {
                let entries: Vec<(Vec<u8>, Vec<u8>)> = bincode::deserialize(&rec.payload)?;
                bitmap.insert(
                    rec.column_id,
                    crate::index::BitmapIndex::from_entries(entries)
                        .map_err(|e| MongrelError::Other(e.into()))?,
                );
            }
            K_FM => {
                let docs: Vec<(Vec<u8>, RowId)> = bincode::deserialize(&rec.payload)?;
                fm.insert(rec.column_id, crate::index::FmIndex::from_docs(docs));
            }
            K_ANN => {
                let idx = crate::index::AnnIndex::thaw(&rec.payload)?;
                ann.insert(rec.column_id, idx);
            }
            K_SPARSE => {
                let entries: Vec<(u32, Vec<(RowId, f32)>)> = bincode::deserialize(&rec.payload)?;
                sparse.insert(
                    rec.column_id,
                    crate::index::SparseIndex::from_entries(entries),
                );
            }
            K_MINHASH => {
                let entries: crate::index::minhash::MinHashEntries =
                    bincode::deserialize(&rec.payload)?;
                minhash.insert(
                    rec.column_id,
                    crate::index::MinHashIndex::from_entries(entries),
                );
            }
            K_LEARNED => {
                let snap: ColumnLearnedRangeSnapshot = bincode::deserialize(&rec.payload)?;
                learned_range.insert(rec.column_id, ColumnLearnedRange::from_snapshot(snap));
            }
            _ => { /* unknown kind: ignore (forward-compatible) */ }
        }
    }

    Ok(Some(LoadedIndexes {
        epoch_built: body.epoch_built,
        hot,
        bitmap,
        ann,
        fm,
        sparse,
        minhash,
        learned_range,
    }))
}

/// Remove the checkpoint (e.g. when the manifest no longer endorses it).
pub fn remove(dir: &Path) {
    let _ = std::fs::remove_file(path(dir));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{AnnIndex, BitmapIndex, FmIndex, HotIndex, SparseIndex};
    use crate::rowid::RowId;
    use tempfile::tempdir;

    #[test]
    fn roundtrip_all_index_kinds() {
        let dir = tempdir().unwrap();

        let mut hot = HotIndex::new();
        hot.insert(b"alice".to_vec(), RowId(1));
        hot.insert(b"bob".to_vec(), RowId(2));

        let mut bitmap = HashMap::new();
        let mut bm = BitmapIndex::new();
        bm.insert(b"red".to_vec(), RowId(1));
        bm.insert(b"red".to_vec(), RowId(3));
        bm.insert(b"blue".to_vec(), RowId(5));
        bitmap.insert(7u16, bm);

        let mut fm_map = HashMap::new();
        let mut fm = FmIndex::new();
        fm.insert(b"the quick brown fox".to_vec(), RowId(1));
        fm.insert(b"fox in socks".to_vec(), RowId(2));
        fm_map.insert(9u16, fm);

        let mut ann_map = HashMap::new();
        let mut ann = AnnIndex::new(8);
        ann.insert(&[1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0], RowId(0));
        ann.insert(&[-1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0], RowId(1));
        ann_map.insert(11u16, ann);

        let mut sparse_map = HashMap::new();
        let mut sp = SparseIndex::new();
        sp.insert(&[(1, 2.0), (2, 1.0)], RowId(0));
        sp.insert(&[(1, 1.0)], RowId(1));
        sparse_map.insert(13u16, sp);

        let mut minhash_map = HashMap::new();
        let mut mh = crate::index::MinHashIndex::new();
        let toks = |ts: &[&str]| -> Vec<u64> {
            ts.iter()
                .map(|t| crate::index::minhash_token_hash(t))
                .collect()
        };
        mh.insert(&toks(&["a", "b", "c", "d"]), RowId(0));
        mh.insert(&toks(&["x", "y", "z", "w"]), RowId(1));
        minhash_map.insert(15u16, mh);

        let lr_map = HashMap::<u16, ColumnLearnedRange>::new();

        let snap = IndexSnapshot {
            hot: &hot,
            bitmap: &bitmap,
            ann: &ann_map,
            fm: &fm_map,
            sparse: &sparse_map,
            minhash: &minhash_map,
            learned_range: &lr_map,
        };
        write_atomic(dir.path(), 42, 7, snap, None).unwrap();

        let loaded = read(dir.path(), None).unwrap().expect("checkpoint present");
        assert_eq!(loaded.epoch_built, 7);
        assert_eq!(loaded.hot.get(b"alice"), Some(RowId(1)));
        assert_eq!(loaded.hot.get(b"bob"), Some(RowId(2)));
        let red = loaded.bitmap[&7].get(b"red");
        let red_ids: Vec<u32> = red.iter().collect();
        assert_eq!(red_ids, vec![1, 3]);
        assert_eq!(loaded.fm[&9].locate(b"fox").len(), 2);
        let top = loaded.ann[&11].search(&[1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0], 1);
        assert_eq!(top[0].0, RowId(0));
        let sp_top = loaded.sparse[&13].search(&[(1, 1.0), (2, 1.0)], 2);
        assert_eq!(sp_top[0].0, RowId(0));
        // MinHash survives the checkpoint round-trip: the identical set is found.
        let mh_top = loaded.minhash[&15].search(&toks(&["a", "b", "c", "d"]), 5);
        assert_eq!(mh_top[0].0, RowId(0));
        assert!(mh_top[0].1 > 0.95);
    }

    #[test]
    fn read_returns_none_when_absent() {
        let dir = tempdir().unwrap();
        assert!(read(dir.path(), None).unwrap().is_none());
    }

    #[test]
    fn detects_corruption() {
        let dir = tempdir().unwrap();
        let hot = HotIndex::new();
        let bitmap = HashMap::new();
        let ann = HashMap::new();
        let fm = HashMap::new();
        let sparse = HashMap::new();
        let minhash = HashMap::new();
        let lr = HashMap::new();
        write_atomic(
            dir.path(),
            1,
            1,
            IndexSnapshot {
                hot: &hot,
                bitmap: &bitmap,
                ann: &ann,
                fm: &fm,
                sparse: &sparse,
                minhash: &minhash,
                learned_range: &lr,
            },
            None,
        )
        .unwrap();
        // Flip a body byte (between the two MAGICs).
        let p = path(dir.path());
        let mut bytes = std::fs::read(&p).unwrap();
        bytes[12] ^= 0xFF;
        std::fs::write(&p, bytes).unwrap();
        let res = read(dir.path(), None);
        assert!(
            matches!(res, Err(MongrelError::ChecksumMismatch { .. })),
            "expected checksum mismatch"
        );
    }
}
