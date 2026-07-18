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
use crate::schema::{IndexKind, Schema};
use crate::{MongrelError, Result};
use bincode::Options;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};

pub const IDX_MAGIC: [u8; 8] = *b"MONGRIDX";
pub const IDX_VERSION: u16 = 3;
pub const IDX_DIR: &str = "_idx";
pub const IDX_FILENAME: &str = "global.idx";
const MAX_INDEX_RECORDS: usize = 65_536;
/// Up-front allocation ceiling for a single payload while decoding. The
/// serialized length prefix is attacker-controllable, so it is never
/// preallocated wholesale; the true payload bound is enforced by the input
/// itself (bincode fails at EOF on an over-claimed length), which keeps a
/// corrupt prefix from forcing an allocation beyond the actual file size.
const MAX_PAYLOAD_PREALLOC_BYTES: usize = 16 * 1024 * 1024;

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
    #[serde(deserialize_with = "deserialize_records")]
    records: Vec<Record>,
}

#[derive(Serialize, Deserialize)]
struct Record {
    kind: u8,
    column_id: u16,
    #[serde(deserialize_with = "deserialize_payload")]
    payload: Vec<u8>,
}

fn deserialize_records<'de, D>(deserializer: D) -> std::result::Result<Vec<Record>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Visitor;

    impl<'de> serde::de::Visitor<'de> for Visitor {
        type Value = Vec<Record>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a bounded global-index record list")
        }

        fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let capacity = sequence.size_hint().unwrap_or(0);
            if capacity > MAX_INDEX_RECORDS {
                return Err(serde::de::Error::custom("too many global-index records"));
            }
            let mut records = Vec::with_capacity(capacity);
            while let Some(record) = sequence.next_element()? {
                if records.len() == MAX_INDEX_RECORDS {
                    return Err(serde::de::Error::custom("too many global-index records"));
                }
                records.push(record);
            }
            Ok(records)
        }
    }

    deserializer.deserialize_seq(Visitor)
}

fn deserialize_payload<'de, D>(deserializer: D) -> std::result::Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Visitor;

    impl<'de> serde::de::Visitor<'de> for Visitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a bounded global-index payload")
        }

        fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            // A legitimate payload can far exceed any fixed byte cap (the 1M-row
            // qualification writes minhash payloads > 500 MiB), so there is no
            // fixed payload limit: the preallocation is capped and the input's
            // end enforces the true length (bincode errors on EOF).
            let capacity = sequence.size_hint().unwrap_or(0);
            let mut payload = Vec::with_capacity(capacity.min(MAX_PAYLOAD_PREALLOC_BYTES));
            while let Some(byte) = sequence.next_element()? {
                payload.push(byte);
            }
            Ok(payload)
        }
    }

    deserializer.deserialize_seq(Visitor)
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
    {
        if let Some(k) = dek {
            return crate::encryption::encrypt_blob(k, &plain);
        }
    }
    Ok(plain)
}

/// Inverse of [`encode_file`]. Returns `None` when an encrypted checkpoint fails
/// to decrypt (wrong key, tamper, or corruption) so the caller simply rebuilds
/// from the runs.
fn decode_file(raw: Vec<u8>, dek: Option<&[u8; 32]>) -> Option<Vec<u8>> {
    {
        if let Some(k) = dek {
            return crate::encryption::decrypt_blob(k, &raw).ok();
        }
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
    let table_root = crate::durable_file::DurableRoot::open(dir)?;
    let idx_root = table_root.create_directory_all_pinned(IDX_DIR)?;
    write_atomic_root(&idx_root, table_id, epoch_built, snap, dek)
}

pub(crate) fn write_atomic_root(
    idx_root: &crate::durable_file::DurableRoot,
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
            records.push(Record {
                kind: K_MINHASH,
                column_id: cid,
                payload: bincode::serialize(&mh.snapshot())?,
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

    idx_root.write_atomic(IDX_FILENAME, &out)?;
    Ok(())
}

/// Read and validate the checkpoint. Verifies both MAGIC sentinels and the
/// trailing SHA-256 before deserializing records.
pub fn read(
    dir: &Path,
    expected_table_id: u64,
    schema: &Schema,
    dek: Option<&[u8; 32]>,
) -> Result<Option<LoadedIndexes>> {
    let path = path(dir);
    let file = match crate::durable_file::open_regular_nofollow(&path) {
        Ok(file) => file,
        Err(MongrelError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(None)
        }
        Err(error) => return Err(error),
    };
    read_file(file, expected_table_id, schema, dek)
}

pub(crate) fn read_durable_for(
    root: &crate::durable_file::DurableRoot,
    relative_dir: impl AsRef<Path>,
    expected_table_id: u64,
    schema: &Schema,
    dek: Option<&[u8; 32]>,
) -> Result<Option<LoadedIndexes>> {
    let relative = relative_dir.as_ref().join(IDX_DIR).join(IDX_FILENAME);
    let file = match root.open_regular(relative) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    read_file(file, expected_table_id, schema, dek)
}

pub(crate) fn read_root(
    idx_root: &crate::durable_file::DurableRoot,
    expected_table_id: u64,
    schema: &Schema,
    dek: Option<&[u8; 32]>,
) -> Result<Option<LoadedIndexes>> {
    let file = match idx_root.open_regular(IDX_FILENAME) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    read_file(file, expected_table_id, schema, dek)
}

fn read_file(
    file: std::fs::File,
    expected_table_id: u64,
    schema: &Schema,
    dek: Option<&[u8; 32]>,
) -> Result<Option<LoadedIndexes>> {
    let length = file.metadata()?.len();
    // No fixed file-size cap: checkpoints grow with table size (the 1M-row
    // qualification checkpoint approaches 1 GiB). The read is bounded by the
    // actual file length, and the SHA-256 footer below authenticates the
    // content before any payload is decoded.
    let mut raw = Vec::with_capacity(length as usize);
    file.take(length + 1).read_to_end(&mut raw)?;
    if raw.len() as u64 != length {
        return Ok(None);
    }
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
        return Ok(None);
    }
    let footer_start = bytes.len() - 32 - 8;
    let footer_magic = &bytes[footer_start..footer_start + 8];
    if footer_magic != IDX_MAGIC {
        return Ok(None);
    }
    let stored_hash = &bytes[bytes.len() - 32..];
    let recomputed: [u8; 32] = Sha256::digest(&bytes[..bytes.len() - 32]).into();
    if stored_hash != recomputed {
        return Ok(None);
    }

    let body: GlobalIdxBody = match decode_bounded(&bytes[8..footer_start]) {
        Some(body) => body,
        None => return Ok(None),
    };
    if body.format_version != IDX_VERSION {
        return Ok(None);
    }
    if body.table_id != expected_table_id {
        return Ok(None);
    }

    let mut hot = crate::index::HotIndex::new();
    let mut bitmap = HashMap::new();
    let mut ann = HashMap::new();
    let mut fm = HashMap::new();
    let mut sparse = HashMap::new();
    let mut minhash = HashMap::new();
    let mut learned_range = HashMap::new();
    let mut identities = HashSet::new();

    for rec in body.records {
        if !identities.insert((rec.kind, rec.column_id)) || !record_matches_schema(&rec, schema) {
            return Ok(None);
        }
        match rec.kind {
            K_HOT => {
                let Some(entries) = decode_bounded::<Vec<(Vec<u8>, RowId)>>(&rec.payload) else {
                    return Ok(None);
                };
                hot = crate::index::HotIndex::from_entries(entries);
            }
            K_BITMAP => {
                let Some(entries) = decode_bounded::<Vec<(Vec<u8>, Vec<u8>)>>(&rec.payload) else {
                    return Ok(None);
                };
                let Ok(index) = crate::index::BitmapIndex::from_entries(entries) else {
                    return Ok(None);
                };
                bitmap.insert(rec.column_id, index);
            }
            K_FM => {
                let Some(docs) = decode_bounded::<Vec<(Vec<u8>, RowId)>>(&rec.payload) else {
                    return Ok(None);
                };
                fm.insert(rec.column_id, crate::index::FmIndex::from_docs(docs));
            }
            K_ANN => {
                let Ok(idx) =
                    crate::index::AnnIndex::thaw_bounded(&rec.payload, rec.payload.len() as u64)
                else {
                    return Ok(None);
                };
                ann.insert(rec.column_id, idx);
            }
            K_SPARSE => {
                let Some(entries) = decode_bounded::<Vec<(u32, Vec<(RowId, f32)>)>>(&rec.payload)
                else {
                    return Ok(None);
                };
                sparse.insert(
                    rec.column_id,
                    crate::index::SparseIndex::from_entries(entries),
                );
            }
            K_MINHASH => {
                let Some(snapshot) =
                    decode_bounded::<crate::index::minhash::MinHashSnapshot>(&rec.payload)
                else {
                    return Ok(None);
                };
                minhash.insert(
                    rec.column_id,
                    crate::index::MinHashIndex::from_snapshot(snapshot),
                );
            }
            K_LEARNED => {
                let Some(snap) = decode_bounded::<ColumnLearnedRangeSnapshot>(&rec.payload) else {
                    return Ok(None);
                };
                learned_range.insert(rec.column_id, ColumnLearnedRange::from_snapshot(snap));
            }
            _ => return Ok(None),
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

fn decode_bounded<T>(bytes: &[u8]) -> Option<T>
where
    T: serde::de::DeserializeOwned,
{
    // Deserialization is bounded by the input slice itself: over-reading past
    // the slice errors at EOF, and trailing bytes are rejected. No fixed byte
    // cap — legitimate payloads exceed any small constant at scale.
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .deserialize(bytes)
        .ok()
}

fn record_matches_schema(record: &Record, schema: &Schema) -> bool {
    let expected_kind = match record.kind {
        K_HOT => return record.column_id == 0,
        K_BITMAP => IndexKind::Bitmap,
        K_FM => IndexKind::FmIndex,
        K_ANN => IndexKind::Ann,
        K_SPARSE => IndexKind::Sparse,
        K_LEARNED => IndexKind::LearnedRange,
        K_MINHASH => IndexKind::MinHash,
        _ => return false,
    };
    schema
        .indexes
        .iter()
        .any(|index| index.column_id == record.column_id && index.kind == expected_kind)
}

/// Remove the checkpoint (e.g. when the manifest no longer endorses it).
pub fn remove(dir: &Path) {
    let _ = std::fs::remove_file(path(dir));
}

#[derive(Debug, Clone)]
pub struct IndexRecordSize {
    pub kind: &'static str,
    pub column_id: u16,
    pub payload_bytes: u64,
}

/// Inspect per-index payload sizes in a plaintext checkpoint. Benchmark helper;
/// encrypted checkpoints intentionally return an invalid-magic error.
pub fn plaintext_record_sizes(dir: &Path) -> Result<Vec<IndexRecordSize>> {
    let bytes = std::fs::read(path(dir))?;
    if bytes.len() < 48 || bytes[..8] != IDX_MAGIC {
        return Err(MongrelError::MagicMismatch {
            what: "global index",
            expected: IDX_MAGIC,
            got: bytes
                .get(..8)
                .and_then(|value| value.try_into().ok())
                .unwrap_or([0; 8]),
        });
    }
    let footer_start = bytes.len() - 40;
    let body: GlobalIdxBody = bincode::deserialize(&bytes[8..footer_start])?;
    Ok(body
        .records
        .into_iter()
        .map(|record| IndexRecordSize {
            kind: match record.kind {
                K_HOT => "hot_primary",
                K_BITMAP => "bitmap",
                K_FM => "fm",
                K_ANN => "ann",
                K_SPARSE => "sparse",
                K_LEARNED => "learned_range",
                K_MINHASH => "minhash",
                _ => "unknown",
            },
            column_id: record.column_id,
            payload_bytes: record.payload.len() as u64,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{AnnIndex, BitmapIndex, FmIndex, HotIndex, SparseIndex};
    use crate::rowid::RowId;
    use crate::schema::{IndexDef, IndexOptions};
    use tempfile::tempdir;

    fn indexed_schema() -> Schema {
        Schema {
            indexes: [
                (7, IndexKind::Bitmap),
                (9, IndexKind::FmIndex),
                (11, IndexKind::Ann),
                (13, IndexKind::Sparse),
                (15, IndexKind::MinHash),
            ]
            .into_iter()
            .map(|(column_id, kind)| IndexDef {
                name: format!("idx_{column_id}"),
                column_id,
                kind,
                predicate: None,
                options: IndexOptions::default(),
            })
            .collect(),
            ..Schema::default()
        }
    }

    fn write_body(dir: &Path, body: &GlobalIdxBody) {
        std::fs::create_dir_all(dir.join(IDX_DIR)).unwrap();
        let mut bytes = IDX_MAGIC.to_vec();
        bytes.extend(bincode::serialize(body).unwrap());
        bytes.extend(IDX_MAGIC);
        let hash: [u8; 32] = Sha256::digest(&bytes).into();
        bytes.extend(hash);
        std::fs::write(path(dir), bytes).unwrap();
    }

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
        ann.insert(&[1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0], RowId(0))
            .unwrap();
        ann.insert(&[-1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0], RowId(1))
            .unwrap();
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

        let loaded = read(dir.path(), 42, &indexed_schema(), None)
            .unwrap()
            .expect("checkpoint present");
        assert_eq!(loaded.epoch_built, 7);
        assert_eq!(loaded.hot.get(b"alice"), Some(RowId(1)));
        assert_eq!(loaded.hot.get(b"bob"), Some(RowId(2)));
        let red = loaded.bitmap[&7].get(b"red");
        let red_ids: Vec<u32> = red.iter().collect();
        assert_eq!(red_ids, vec![1, 3]);
        assert_eq!(loaded.fm[&9].locate(b"fox").len(), 2);
        let top = loaded.ann[&11]
            .search(&[1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0], 1)
            .unwrap();
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
        assert!(read(dir.path(), 1, &Schema::default(), None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn old_checkpoint_version_is_rejected_for_rebuild() {
        let dir = tempdir().unwrap();
        let body = GlobalIdxBody {
            format_version: IDX_VERSION - 1,
            table_id: 1,
            epoch_built: 1,
            records: vec![],
        };
        write_body(dir.path(), &body);
        assert!(read(dir.path(), 1, &Schema::default(), None)
            .unwrap()
            .is_none());
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
        assert!(read(dir.path(), 1, &Schema::default(), None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn checkpoint_is_bound_to_table_and_schema() {
        let dir = tempdir().unwrap();
        let hot = HotIndex::new();
        let mut bitmap = HashMap::new();
        let mut index = BitmapIndex::new();
        index.insert(b"value".to_vec(), RowId(1));
        bitmap.insert(7, index);
        write_atomic(
            dir.path(),
            42,
            1,
            IndexSnapshot {
                hot: &hot,
                bitmap: &bitmap,
                ann: &HashMap::new(),
                fm: &HashMap::new(),
                sparse: &HashMap::new(),
                minhash: &HashMap::new(),
                learned_range: &HashMap::new(),
            },
            None,
        )
        .unwrap();

        assert!(read(dir.path(), 41, &indexed_schema(), None)
            .unwrap()
            .is_none());
        assert!(read(dir.path(), 42, &Schema::default(), None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn duplicate_and_unknown_records_trigger_rebuild() {
        let dir = tempdir().unwrap();
        let bitmap_payload = bincode::serialize(&Vec::<(Vec<u8>, Vec<u8>)>::new()).unwrap();
        let mut body = GlobalIdxBody {
            format_version: IDX_VERSION,
            table_id: 1,
            epoch_built: 1,
            records: vec![
                Record {
                    kind: K_BITMAP,
                    column_id: 7,
                    payload: bitmap_payload.clone(),
                },
                Record {
                    kind: K_BITMAP,
                    column_id: 7,
                    payload: bitmap_payload,
                },
            ],
        };
        write_body(dir.path(), &body);
        assert!(read(dir.path(), 1, &indexed_schema(), None)
            .unwrap()
            .is_none());

        body.records = vec![Record {
            kind: 255,
            column_id: 7,
            payload: Vec::new(),
        }];
        write_body(dir.path(), &body);
        assert!(read(dir.path(), 1, &indexed_schema(), None)
            .unwrap()
            .is_none());
    }

    /// Regression test for the chronic AI 1M qualification failure: legitimate
    /// checkpoints at scale far exceed 64 MiB (at 1M rows the minhash payload
    /// is ~528 MiB and the ann payload ~305 MiB). A fixed 64 MiB cap on the
    /// checkpoint read path made every large table discard its index
    /// checkpoint on open and failed the qualification's checkpoint
    /// inspection. Payload bounds are input-derived, never fixed.
    #[test]
    fn large_checkpoint_over_64_mib_roundtrips() {
        let dir = tempdir().unwrap();
        // ~68 MiB hot payload: 1.7M entries x ~40 serialized bytes.
        let entries: Vec<(Vec<u8>, RowId)> = (0u64..1_700_000)
            .map(|i| {
                let mut key = b"large-key-".to_vec();
                key.extend_from_slice(&i.to_le_bytes());
                key.extend_from_slice(&[0u8; 6]); // 24-byte keys
                (key, RowId(i))
            })
            .collect();
        let payload = bincode::serialize(&entries).unwrap();
        assert!(payload.len() as u64 > 64 * 1024 * 1024);
        let body = GlobalIdxBody {
            format_version: IDX_VERSION,
            table_id: 7,
            epoch_built: 3,
            records: vec![Record {
                kind: K_HOT,
                column_id: 0,
                payload,
            }],
        };
        write_body(dir.path(), &body);
        let file_len = std::fs::metadata(path(dir.path())).unwrap().len();
        assert!(file_len > 64 * 1024 * 1024);

        let loaded = read(dir.path(), 7, &indexed_schema(), None)
            .unwrap()
            .expect("large checkpoint loads");
        assert_eq!(loaded.epoch_built, 3);
        let mut probe = b"large-key-".to_vec();
        probe.extend_from_slice(&1_699_999u64.to_le_bytes());
        probe.extend_from_slice(&[0u8; 6]);
        assert_eq!(loaded.hot.get(&probe), Some(RowId(1_699_999)));

        // The benchmark inspection helper lists large payloads too.
        let sizes = plaintext_record_sizes(dir.path()).unwrap();
        assert_eq!(sizes.len(), 1);
        assert_eq!(sizes[0].kind, "hot_primary");
        assert!(sizes[0].payload_bytes > 64 * 1024 * 1024);
    }

    /// A corrupt payload length prefix that over-claims must fail closed at
    /// EOF without attempting the claimed allocation (no abort, no OOM).
    #[test]
    fn over_claimed_payload_length_fails_closed() {
        let dir = tempdir().unwrap();
        let body = GlobalIdxBody {
            format_version: IDX_VERSION,
            table_id: 1,
            epoch_built: 1,
            records: vec![Record {
                kind: K_HOT,
                column_id: 0,
                payload: vec![1, 2, 3],
            }],
        };
        let mut bytes = IDX_MAGIC.to_vec();
        bytes.extend(bincode::serialize(&body).unwrap());
        // bincode (fixint) layout up to the first record's payload length:
        // format_version u16 | table_id u64 | epoch_built u64 | records len
        // u64 | kind u8 | column_id u16 | payload len u64  =>  2+8+8+8+1+2 = 29.
        let payload_len_offset = 2 + 8 + 8 + 8 + 1 + 2;
        bytes[payload_len_offset..payload_len_offset + 8]
            .copy_from_slice(&(1u64 << 40).to_le_bytes());
        bytes.extend(IDX_MAGIC);
        let hash: [u8; 32] = Sha256::digest(&bytes).into();
        bytes.extend(hash);
        std::fs::create_dir_all(dir.path().join(IDX_DIR)).unwrap();
        std::fs::write(path(dir.path()), bytes).unwrap();

        // The hash verifies, but decoding must fail at EOF — returning None
        // for a rebuild, never panicking or allocating the claimed 1 TiB.
        assert!(read(dir.path(), 1, &indexed_schema(), None)
            .unwrap()
            .is_none());
    }
}
