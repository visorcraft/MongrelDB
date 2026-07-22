//! Sorted Run — the immutable columnar unit (`.sr`).
//!
//! On-disk layout: a 256-byte header, a columnar page region (PAX), a column
//! directory, an index trailer, and a checksummed footer. [`RunWriter`] flushes
//! drained memtable rows into encoded columns
//! (system columns `_row_id` / `_epoch` / `_deleted` plus user columns), and
//! [`RunReader`] decodes them back, answering MVCC point lookups and scans.
//!
//! # HLC stamps (P0.5-T3)
//!
//! Sorted runs may carry an optional [`SYS_COMMIT_TS`] system column (16-byte
//! little-endian HLC encoding). Writers emit it whenever any flushed row has a
//! `commit_ts`; readers restore stamps when the column is present and treat a
//! missing column as legacy (`commit_ts: None`). Epoch remains the always-on
//! system column so pre-stamp runs stay readable without a format bump.
//! Full PITR-at-HLC still depends on archive targets (P0.5-X10).

use crate::columnar;
use crate::encryption::{setup_run_encryption, Cipher, Kek, RunEncryption};
use crate::epoch::Epoch;
use crate::error::{MongrelError, Result};
use crate::index::pgm::PgmIndex;
use crate::memtable::{Row, Value};
use crate::page::{Encoding, PageStat};
use crate::row_id_set::RowIdSet;
use crate::rowid::RowId;
use crate::schema::{Schema, TypeId};
use mongreldb_types::hlc::HlcTimestamp;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const RUN_MAGIC: [u8; 8] = *b"MONGRRUN";
pub const RUN_FORMAT_VERSION: u16 = 1;
/// v2 appends `encrypted_stats_offset`/`encrypted_stats_len` to [`RunHeader`].
/// The fields are trailing and the header region is zero-padded, so a run
/// written before v2 deserializes with both fields = 0 ("no encrypted stats
/// section"). Pre-v2 *encrypted* runs are not readable (their MAC covers the
/// shorter v1 header serialization) — recreate them; no released data exists.
pub const RUN_HEADER_VERSION: u16 = 2;
pub const RUN_HEADER_PAD: usize = 256;
const MAX_RUN_PAGE_BYTES: u64 = 64 * 1024 * 1024 + 32;

/// Reserved `(column_id, page_seq)` nonce coordinates for the encrypted
/// page-stats envelope (see [`RunHeader::encrypted_stats_offset`]). Pages per
/// column are capped strictly below `u16::MAX` at encode, so no page nonce can
/// ever collide with this pair under the run's DEK.
const ENC_STATS_NONCE_COLUMN: u16 = u16::MAX;
const ENC_STATS_NONCE_SEQ: u32 = u16::MAX as u32;

/// One page's `(min, max)` bounds as stored in the encrypted stats envelope.
type PageMinMax = (Option<Vec<u8>>, Option<Vec<u8>>);

/// Per-page `(min, max)` bounds of one encrypted column, keyed by column id —
/// the plaintext shape of the encrypted stats envelope. The cleartext column
/// directory must not carry these (they would leak values to anyone holding
/// the file without the key), so they travel AES-256-GCM-encrypted under the
/// run DEK and are overlaid onto the in-memory [`PageStat`]s at open, which
/// restores zone-map page pruning for encrypted columns.
type EncryptedColumnStats = Vec<(u16, Vec<PageMinMax>)>;

/// AES-256-GCM authentication-tag length appended to every ciphertext. Used to
/// precompute on-disk page sizes so the direct-to-mmap writer can lay the whole
/// run out before encrypting (Phase 14.6). Validated against the cipher's real
/// output in [`write_run_mmap`].
const GCM_TAG_LEN: usize = 16;

/// On-disk length of a page payload: ciphertext = plaintext + GCM tag when
/// encrypted, else the plaintext length itself.
fn on_disk_len(page_len: usize, encrypted: bool) -> usize {
    if encrypted {
        page_len + GCM_TAG_LEN
    } else {
        page_len
    }
}

pub const RUN_FLAG_ENCRYPTED: u8 = 1 << 0;
pub const RUN_FLAG_TOMBSTONE_ONLY: u8 = 1 << 1;
/// Run is "clean": exactly one version per `RowId`, no tombstones, and row_ids
/// are strictly ascending. Bulk-loaded and compacted runs are clean by
/// construction. For a clean run, MVCC visibility is trivially *every* position
/// (the newest visible version of a rid is the only version, and it is not
/// deleted), so the visibility pass can skip decoding the epoch/deleted system
/// columns and the group-collapse loop — only the row_id column is needed (for
/// survivor↔position mapping). Old runs lack this bit and default to not-clean
/// (safe fallback to the full pass).
pub const RUN_FLAG_CLEAN: u8 = 1 << 2;
/// Run is "uniform-epoch": every row's commit epoch is the run's commit epoch,
/// which is **not** baked into the file (it is assigned at commit/link time and
/// recorded in the manifest `RunRef.epoch_created`). Spill runs from large
/// transactions are written before the commit epoch is known, so their stored
/// `_epoch` column is a placeholder; the reader must overlay the real epoch from
/// the `RunRef`. The engine calls [`RunReader::set_uniform_epoch`] with that
/// value after opening such a run.
pub const RUN_FLAG_UNIFORM_EPOCH: u8 = 1 << 3;
pub const SORT_KEY_ROW_ID: u16 = 0xFFFF;

/// Reserved column ids for the MVCC system columns, stored in every run.
pub const SYS_ROW_ID: u16 = 0xFFFE;
pub const SYS_EPOCH: u16 = 0xFFFD;
pub const SYS_DELETED: u16 = 0xFFFC;
/// Optional HLC commit stamp (P0.5-T3). Absent on legacy runs → `commit_ts: None`.
///
/// Encoding: 16 little-endian bytes
/// `(physical_micros:u64, logical:u32, node_tiebreaker:u32)`, or `Null` when
/// a particular row version was not stamped.
pub const SYS_COMMIT_TS: u16 = 0xFFFB;

/// Bytes length of the on-disk HLC encoding for [`SYS_COMMIT_TS`].
const COMMIT_TS_BYTES: usize = 16;

/// Encode an optional HLC stamp for the optional [`SYS_COMMIT_TS`] column.
fn encode_commit_ts_value(ts: Option<HlcTimestamp>) -> Value {
    match ts {
        None => Value::Null,
        Some(ts) => {
            let mut buf = [0u8; COMMIT_TS_BYTES];
            buf[0..8].copy_from_slice(&ts.physical_micros.to_le_bytes());
            buf[8..12].copy_from_slice(&ts.logical.to_le_bytes());
            buf[12..16].copy_from_slice(&ts.node_tiebreaker.to_le_bytes());
            Value::Bytes(buf.to_vec())
        }
    }
}

/// Decode a [`SYS_COMMIT_TS`] cell. Malformed/null/absent → `None` (legacy).
fn decode_commit_ts_value(value: Option<&Value>) -> Option<HlcTimestamp> {
    match value {
        Some(Value::Bytes(bytes)) if bytes.len() == COMMIT_TS_BYTES => {
            let physical_micros = u64::from_le_bytes(bytes[0..8].try_into().ok()?);
            let logical = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
            let node_tiebreaker = u32::from_le_bytes(bytes[12..16].try_into().ok()?);
            Some(HlcTimestamp {
                physical_micros,
                logical,
                node_tiebreaker,
            })
        }
        _ => None,
    }
}

/// True when `column_id` is a reserved system column (including optional HLC).
fn is_system_column_id(column_id: u16) -> bool {
    matches!(
        column_id,
        SYS_ROW_ID | SYS_EPOCH | SYS_DELETED | SYS_COMMIT_TS
    )
}

/// Guaranteed positional error of the stored PGM model (the predicted offset is
/// within `± LEARNED_EPSILON` of the true position; a tiny final scan corrects).
const LEARNED_EPSILON: usize = 64;

/// Build the learned-index trailer as a compressed PGM-index over
/// `(row_id, array_index)`. Near-linear row-id sequences collapse to a single
/// segment, so the trailer is typically a few dozen bytes regardless of run size.
fn build_learned_trailer(row_ids: &[Value]) -> Vec<u8> {
    let points: Vec<(u64, usize)> = row_ids
        .iter()
        .enumerate()
        .filter_map(|(i, v)| match v {
            Value::Int64(r) => Some((*r as u64, i)),
            _ => None,
        })
        .collect();
    let pgm = PgmIndex::build(&points, LEARNED_EPSILON);
    bincode::serialize(&pgm).expect("pgm serialize")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunHeader {
    pub magic: [u8; 8],
    pub format_version: u16,
    pub header_layout_version: u16,
    pub run_id: u128,
    pub content_hash: [u8; 32],
    pub schema_id: u64,
    pub epoch_created: u64,
    pub level: u8,
    pub flags: u8,
    pub sort_key_column_id: u16,
    pub row_count: u64,
    pub min_row_id: u64,
    pub max_row_id: u64,
    pub column_count: u64,
    pub column_dir_offset: u64,
    pub index_trailer_offset: u64,
    pub encryption_descriptor_offset: u64,
    pub footer_offset: u64,
    /// Offset/length of the AES-256-GCM-encrypted per-page min/max envelope
    /// for encrypted columns (0/0 = absent; always 0 for plaintext runs and
    /// pre-v2 files, whose zero header padding deserializes to 0 here).
    pub encrypted_stats_offset: u64,
    pub encrypted_stats_len: u64,
}

impl RunHeader {
    pub fn is_encrypted(&self) -> bool {
        self.flags & RUN_FLAG_ENCRYPTED != 0
    }
    pub fn is_clean(&self) -> bool {
        self.flags & RUN_FLAG_CLEAN != 0
    }
    pub fn is_uniform_epoch(&self) -> bool {
        self.flags & RUN_FLAG_UNIFORM_EPOCH != 0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnPageHeader {
    pub column_id: u16,
    pub type_id_tag: u16,
    pub encoding: u8,
    pub flags: u8,
    pub page_count: u32,
    pub page_region_offset: u64,
    pub page_region_len: u64,
    pub page_stats: Vec<PageStat>,
}

impl ColumnPageHeader {
    const PAGE_ENCRYPTED: u8 = 1 << 0;
}

/// Length of the run-metadata HMAC tag appended after the footer of an encrypted
/// run (HMAC-SHA256, see [`crate::encryption::run_metadata_mac`]).
const RUN_MAC_LEN: usize = 32;

/// Compute the run-metadata MAC tag for an encrypted run, or `None` for a
/// plaintext run (or when the `encryption` feature is off, where `enc` is always
/// `None`). MACs `header ‖ dir ‖ descriptor` under the run's KEK-derived key.
fn compute_run_mac(
    enc: Option<&RunEncryption>,
    header_bytes: &[u8],
    dir_bytes: &[u8],
) -> Option<[u8; RUN_MAC_LEN]> {
    {
        if let Some(e) = enc {
            if let Some(mac_key) = &e.mac_key {
                return Some(crate::encryption::run_metadata_mac(
                    mac_key,
                    header_bytes,
                    dir_bytes,
                    &e.descriptor_bytes,
                ));
            }
        }
    }
    None
}

/// A column's pages handed to the low-level writer.
pub struct ColumnPayload {
    pub column_id: u16,
    pub type_id_tag: u16,
    pub encoding: Encoding,
    pub pages: Vec<Vec<u8>>,
    /// Optional value-derived stats per page (parallel to [`Self::pages`]). When
    /// present, [`write_run_with`] fills only the offset/length slots; when a
    /// slot is missing it falls back to an empty stat.
    pub page_stats: Vec<PageStat>,
}

/// Specification handed to [`write_run`] / [`write_run_with`].
pub struct RunSpec<'a> {
    pub run_id: u128,
    pub schema_id: u64,
    pub epoch_created: u64,
    pub level: u8,
    pub flags: u8,
    pub sort_key_column_id: u16,
    pub row_count: u64,
    pub min_row_id: u64,
    pub max_row_id: u64,
    pub columns: &'a [ColumnPayload],
}

/// Write a run with no encryption and no index trailer (back-compat entry point).
pub fn write_run(path: impl AsRef<Path>, spec: &RunSpec) -> Result<RunHeader> {
    write_run_with(path, spec, None, &[], None)
}

/// Write a run, optionally encrypting page payloads with a per-file DEK
/// (wrapped by the table `kek`, see §7) and appending an `index_trailer` blob
/// (used by [`RunWriter`] for the learned index).
///
/// Tries the direct-to-mmap writer (Phase 14.6 — pages are encrypted + placed
/// straight into a memory mapping of the output file, in parallel, with no
/// intermediate whole-file `Vec<u8>`), and falls back to the in-buffer writer
/// only when the mapping itself can't be created (some filesystems/environments
/// reject `mmap`). The two writers produce a byte-identical run.
pub fn write_run_with(
    path: impl AsRef<Path>,
    spec: &RunSpec,
    kek: Option<&Kek>,
    indexable_columns: &[(u16, u8)],
    index_trailer: Option<&[u8]>,
) -> Result<RunHeader> {
    // Assemble per-run encryption material (fresh DEK + nonce prefix + wrapped
    // descriptor) when a KEK is supplied. The page cipher lives only for this
    // write; the wrapped DEK is embedded in the run below.
    let enc: Option<RunEncryption> = match kek {
        Some(k) => Some(setup_run_encryption(k, indexable_columns)?),
        None => None,
    };
    match write_run_mmap(path.as_ref(), spec, enc.as_ref(), index_trailer) {
        Ok(h) => Ok(h),
        Err(e) if is_mmap_unavailable(&e) => write_run_vec(path, spec, enc, index_trailer),
        Err(e) => Err(e),
    }
}

fn write_run_with_file(
    mut file: File,
    spec: &RunSpec,
    kek: Option<&Kek>,
    indexable_columns: &[(u16, u8)],
    index_trailer: Option<&[u8]>,
) -> Result<RunHeader> {
    let enc = match kek {
        Some(key) => Some(setup_run_encryption(key, indexable_columns)?),
        None => None,
    };
    match write_run_mmap_file(&file, spec, enc.as_ref(), index_trailer) {
        Ok(header) => Ok(header),
        Err(error) if is_mmap_unavailable(&error) => {
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            let (bytes, header) = encode_run_vec(spec, enc, index_trailer)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            Ok(header)
        }
        Err(error) => Err(error),
    }
}

/// `true` when `write_run_mmap` could not create the mapping (the only case
/// where we fall back to the in-buffer writer). Any other error — encryption,
/// serialization, a genuine write failure — must surface.
fn is_mmap_unavailable(e: &MongrelError) -> bool {
    matches!(e, MongrelError::InvalidArgument(m) if m.starts_with("__mmap_unavailable__:"))
}

/// Direct-to-mmap run writer (Phases 14.5 + 14.6).
///
/// Lays the whole run out up front (computing each page's on-disk offset/length
/// without encrypting), sizes the file, memory-maps it, then encrypts + copies
/// every page **straight into the mapping** on the rayon pool. Because pages
/// land in the kernel page cache (the mapping) instead of a heap `Vec<u8>`,
/// the kernel can begin writeback of earlier pages while later ones are still
/// being encrypted — CPU encryption overlaps with disk I/O (the double-buffer
/// intent of 14.5). There is no whole-run buffer; peak extra memory is a few
/// transient ciphertext `Vec`s (one per worker thread), not the full run.
/// Direct-to-mmap run writer (Phases 14.5 + 14.6).
///
/// Lays the whole run out ([`plan_run`]), sizes the file, memory-maps it, then
/// delegates to [`place_run`] which encrypts + copies every page **straight into
/// the mapping** on the rayon pool. Because pages land in the kernel page cache
/// (the mapping) instead of a heap `Vec<u8>`, the kernel can begin writeback of
/// earlier pages while later ones are still being encrypted — CPU encryption
/// overlaps with disk I/O (the double-buffer intent of 14.5). There is no
/// whole-run buffer; peak extra memory is a few transient ciphertext `Vec`s
/// (one per worker thread), not the full run.
fn write_run_mmap(
    path: &Path,
    spec: &RunSpec,
    enc: Option<&RunEncryption>,
    index_trailer: Option<&[u8]>,
) -> Result<RunHeader> {
    let file = OpenOptions::new().create_new(true).write(true).open(path)?;
    let result = write_run_mmap_file(&file, spec, enc, index_trailer);
    if result.as_ref().is_err_and(is_mmap_unavailable) {
        drop(file);
        let _ = std::fs::remove_file(path);
    }
    result
}

fn write_run_mmap_file(
    file: &File,
    spec: &RunSpec,
    enc: Option<&RunEncryption>,
    index_trailer: Option<&[u8]>,
) -> Result<RunHeader> {
    let plan = plan_run(spec, enc, index_trailer)?;
    file.set_len(plan.total as u64)?;
    let mut mmap = match unsafe { memmap2::MmapMut::map_mut(file) } {
        Ok(m) => m,
        Err(e) => {
            return Err(MongrelError::InvalidArgument(format!(
                "__mmap_unavailable__: {e}"
            )));
        }
    };
    let header = place_run(spec, enc, index_trailer, &plan, &mut mmap[..])?;
    mmap.flush()?;
    file.sync_all()?;
    Ok(header)
}

/// A precomputed run layout: the page-placement jobs, the serialized column
/// directory (stats already filled), and every on-disk offset/length. The
/// layout is independent of the backing buffer, so [`place_run`] can target a
/// mmap'd file or a plain `Vec` interchangeably — which is what lets the
/// placement path be unit-tested even where file mmap is unavailable.
struct RunPlan {
    jobs: Vec<(usize, usize, u64, usize)>, // (col_idx, page_seq, offset, on_disk_len)
    dir_bytes: Vec<u8>,
    encrypted: bool,
    column_dir_offset: u64,
    index_trailer_offset: u64,
    encryption_descriptor_offset: u64,
    footer_offset: u64,
    total: usize,
    /// Serialized (plaintext) [`EncryptedColumnStats`]; encrypted into the
    /// stats section by the writer. `None` when the run is plaintext or no
    /// encrypted column carries min/max.
    encrypted_stats_plain: Option<Vec<u8>>,
    encrypted_stats_offset: u64,
    encrypted_stats_len: u64,
}

/// Compute the run layout: per-page offset + on-disk length, the filled column
/// directory (serialized), and the dir / trailer / descriptor / footer offsets.
/// Validates the encrypted-page-seq nonce-exhaustion bound.
fn plan_run(
    spec: &RunSpec,
    enc: Option<&RunEncryption>,
    index_trailer: Option<&[u8]>,
) -> Result<RunPlan> {
    let encrypted = enc.is_some();
    let columns = spec.columns;
    let mut jobs: Vec<(usize, usize, u64, usize)> = Vec::new();
    let mut dir: Vec<ColumnPageHeader> = Vec::with_capacity(columns.len());
    let mut enc_stats: EncryptedColumnStats = Vec::new();
    let mut cursor: u64 = RUN_HEADER_PAD as u64;
    for (ci, col) in columns.iter().enumerate() {
        let region_offset = cursor;
        let mut region_len = 0u64;
        let mut stats = Vec::with_capacity(col.pages.len());
        let mut col_minmax: Vec<PageMinMax> = Vec::new();
        for (ps, page) in col.pages.iter().enumerate() {
            // The per-page GCM nonce encodes page_seq in 2 bytes; refuse to
            // silently truncate at 65 535 pages/column (4.29e9 rows), which
            // would otherwise reuse a nonce under the run's DEK. Sequence
            // 0xFFFF itself is reserved for the encrypted-stats envelope.
            if encrypted && ps >= u16::MAX as usize {
                return Err(MongrelError::Full(format!(
                    "column {:#x} exceeds 65534 pages; encrypted-run page-seq nonce space exhausted",
                    col.column_id
                )));
            }
            let odl = on_disk_len(page.len(), encrypted);
            jobs.push((ci, ps, cursor, odl));
            let mut stat = col.page_stats.get(ps).cloned().unwrap_or(PageStat {
                first_row_id: 0,
                last_row_id: 0,
                null_count: 0,
                row_count: 0,
                min: None,
                max: None,
                offset: 0,
                compressed_len: 0,
                uncompressed_len: 0,
            });
            stat.offset = cursor;
            stat.compressed_len = odl as u32;
            stat.uncompressed_len = page.len() as u32;
            // The column directory is serialized in cleartext, so per-page
            // min/max would leak raw plaintext values (literal bytes for `Bytes`
            // columns) of every encrypted page to anyone reading the file
            // without the key. Move them out of the directory and into the
            // DEK-encrypted stats envelope, which the reader overlays back at
            // open — zone-map page pruning works identically to plaintext runs.
            if encrypted {
                col_minmax.push((stat.min.take(), stat.max.take()));
            }
            stats.push(stat);
            cursor += odl as u64;
            region_len += odl as u64;
        }
        if col_minmax
            .iter()
            .any(|(mn, mx)| mn.is_some() || mx.is_some())
        {
            enc_stats.push((col.column_id, col_minmax));
        }
        let page_flags = if encrypted {
            ColumnPageHeader::PAGE_ENCRYPTED
        } else {
            0
        };
        dir.push(ColumnPageHeader {
            column_id: col.column_id,
            type_id_tag: col.type_id_tag,
            encoding: col.encoding as u8,
            flags: page_flags,
            page_count: col.pages.len() as u32,
            page_region_offset: region_offset,
            page_region_len: region_len,
            page_stats: stats,
        });
    }
    let dir_bytes = bincode::serialize(&dir)?;
    let column_dir_offset = cursor;
    cursor += dir_bytes.len() as u64;
    let index_trailer_offset = match index_trailer {
        Some(t) => {
            let off = cursor;
            cursor += t.len() as u64;
            off
        }
        None => 0,
    };
    let encryption_descriptor_offset = match enc {
        Some(e) => {
            let off = cursor;
            cursor += 4 + e.descriptor_bytes.len() as u64;
            off
        }
        None => 0,
    };
    let (encrypted_stats_plain, encrypted_stats_offset, encrypted_stats_len) =
        if encrypted && !enc_stats.is_empty() {
            let plain = bincode::serialize(&enc_stats)?;
            let ct_len = on_disk_len(plain.len(), true) as u64;
            let off = cursor;
            cursor += ct_len;
            (Some(plain), off, ct_len)
        } else {
            (None, 0, 0)
        };
    let footer_offset = cursor;
    // footer = MAGIC(8) + footer_offset(8) + checksum(32); encrypted runs append
    // a 32-byte HMAC tag (RUN_MAC_LEN) authenticating header+dir+descriptor.
    let total = footer_offset as usize + 8 + 8 + 32 + if encrypted { RUN_MAC_LEN } else { 0 };
    Ok(RunPlan {
        jobs,
        dir_bytes,
        encrypted,
        column_dir_offset,
        index_trailer_offset,
        encryption_descriptor_offset,
        footer_offset,
        total,
        encrypted_stats_plain,
        encrypted_stats_offset,
        encrypted_stats_len,
    })
}

/// Place a run into `buf` (which must be exactly [`RunPlan::total`] bytes):
/// encrypt + copy every page into its precomputed slot on the rayon pool, then
/// write the column directory, index trailer, encryption descriptor, header,
/// and checksummed footer. Backed by a mmap'd file in production and by a
/// `Vec<u8>` in tests — the placement logic is identical either way, which is
/// what makes the Phase 14.6 path unit-testable.
fn place_run(
    spec: &RunSpec,
    enc: Option<&RunEncryption>,
    index_trailer: Option<&[u8]>,
    plan: &RunPlan,
    buf: &mut [u8],
) -> Result<RunHeader> {
    use rayon::prelude::*;
    use std::borrow::Cow;
    debug_assert_eq!(
        buf.len(),
        plan.total,
        "place_run: buffer must be exactly plan.total bytes"
    );

    let cipher: Option<&dyn Cipher> = enc.map(|e| e.cipher.as_ref());
    let nonce_prefix = enc.map(|e| e.nonce_prefix);
    let columns = spec.columns;

    // ---- parallel placement: encrypt + copy each page into its slot ----
    // Disjointness is by construction: every job targets [offset, offset+len)
    // and `plan_run` made those ranges non-overlapping. The `SyncPtr` newtype
    // lets the base pointer cross thread boundaries for that purpose.
    struct SyncPtr(*mut u8);
    unsafe impl Send for SyncPtr {}
    unsafe impl Sync for SyncPtr {}
    impl SyncPtr {
        fn get(&self) -> *mut u8 {
            self.0
        }
    }
    let base = SyncPtr(buf.as_mut_ptr());
    plan.jobs
        .par_iter()
        .map(
            |&(ci, ps, offset, odl): &(usize, usize, u64, usize)| -> Result<()> {
                let page = &columns[ci].pages[ps];
                let dst =
                    unsafe { std::slice::from_raw_parts_mut(base.get().add(offset as usize), odl) };
                let bytes: Cow<[u8]> = match cipher {
                    Some(c) => {
                        let nonce =
                            page_nonce(nonce_prefix.unwrap(), columns[ci].column_id, ps as u32);
                        let ct = c.encrypt_page(&nonce, page)?;
                        // Guards the GCM_TAG_LEN assumption: a future cipher with a
                        // different tag size must change `on_disk_len` too.
                        assert_eq!(
                        ct.len(), odl,
                        "ciphertext length {} != predicted {}; GCM tag size assumption is stale",
                        ct.len(), odl
                    );
                        Cow::Owned(ct)
                    }
                    None => {
                        debug_assert_eq!(page.len(), odl);
                        Cow::Borrowed(page.as_slice())
                    }
                };
                dst.copy_from_slice(&bytes);
                Ok(())
            },
        )
        .collect::<Result<()>>()?;

    // ---- content hash over the on-disk page region (column-major, page order) ----
    let content_hash = {
        let mut h = Sha256::new();
        h.update(&buf[RUN_HEADER_PAD..plan.column_dir_offset as usize]);
        h.finalize()
    };

    // ---- dir / trailer / descriptor ----
    let doff = plan.column_dir_offset as usize;
    buf[doff..doff + plan.dir_bytes.len()].copy_from_slice(&plan.dir_bytes);
    if let Some(t) = index_trailer {
        let off = plan.index_trailer_offset as usize;
        buf[off..off + t.len()].copy_from_slice(t);
    }
    if let Some(e) = enc {
        let off = plan.encryption_descriptor_offset as usize;
        buf[off..off + 4].copy_from_slice(&(e.descriptor_bytes.len() as u32).to_le_bytes());
        buf[off + 4..off + 4 + e.descriptor_bytes.len()].copy_from_slice(&e.descriptor_bytes);
    }
    if let (Some(e), Some(plain)) = (enc, plan.encrypted_stats_plain.as_ref()) {
        let nonce = page_nonce(e.nonce_prefix, ENC_STATS_NONCE_COLUMN, ENC_STATS_NONCE_SEQ);
        let ct = e.cipher.encrypt_page(&nonce, plain)?;
        debug_assert_eq!(ct.len() as u64, plan.encrypted_stats_len);
        let off = plan.encrypted_stats_offset as usize;
        buf[off..off + ct.len()].copy_from_slice(&ct);
    }

    // ---- header + footer ----
    let header_flags = if plan.encrypted {
        spec.flags | RUN_FLAG_ENCRYPTED
    } else {
        spec.flags
    };
    let header = RunHeader {
        magic: RUN_MAGIC,
        format_version: RUN_FORMAT_VERSION,
        header_layout_version: RUN_HEADER_VERSION,
        run_id: spec.run_id,
        content_hash: content_hash.into(),
        schema_id: spec.schema_id,
        epoch_created: spec.epoch_created,
        level: spec.level,
        flags: header_flags,
        sort_key_column_id: spec.sort_key_column_id,
        row_count: spec.row_count,
        min_row_id: spec.min_row_id,
        max_row_id: spec.max_row_id,
        column_count: columns.len() as u64,
        column_dir_offset: plan.column_dir_offset,
        index_trailer_offset: plan.index_trailer_offset,
        encryption_descriptor_offset: plan.encryption_descriptor_offset,
        footer_offset: plan.footer_offset,
        encrypted_stats_offset: plan.encrypted_stats_offset,
        encrypted_stats_len: plan.encrypted_stats_len,
    };
    let header_bytes = bincode::serialize(&header)?;
    if header_bytes.len() > RUN_HEADER_PAD {
        return Err(MongrelError::InvalidArgument(format!(
            "run header too large: {} > {RUN_HEADER_PAD}",
            header_bytes.len()
        )));
    }
    buf[..header_bytes.len()].copy_from_slice(&header_bytes);

    let checksum = Sha256::digest(&buf[..plan.footer_offset as usize]);
    let foot = plan.footer_offset as usize;
    buf[foot..foot + 8].copy_from_slice(&RUN_MAGIC);
    buf[foot + 8..foot + 16].copy_from_slice(&plan.footer_offset.to_le_bytes());
    buf[foot + 16..foot + 48].copy_from_slice(&checksum);
    // Encrypted runs: append the keyed metadata MAC over header‖dir‖descriptor.
    if let Some(tag) = compute_run_mac(enc, &header_bytes, &plan.dir_bytes) {
        buf[foot + 48..foot + 48 + RUN_MAC_LEN].copy_from_slice(&tag);
    }
    Ok(header)
}

/// Fallback in-buffer writer (used when the output file can't be mmap'd).
/// Produces a byte-identical run to [`write_run_mmap`].
fn write_run_vec(
    path: impl AsRef<Path>,
    spec: &RunSpec,
    enc: Option<RunEncryption>,
    index_trailer: Option<&[u8]>,
) -> Result<RunHeader> {
    let (buf, header) = encode_run_vec(spec, enc, index_trailer)?;
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(&buf)?;
    file.sync_all()?;
    Ok(header)
}

fn encode_run_vec(
    spec: &RunSpec,
    enc: Option<RunEncryption>,
    index_trailer: Option<&[u8]>,
) -> Result<(Vec<u8>, RunHeader)> {
    let mut buf: Vec<u8> = vec![0; RUN_HEADER_PAD]; // reserve header region
    let mut content_hasher = Sha256::new();
    let mut dir: Vec<ColumnPageHeader> = Vec::with_capacity(spec.columns.len());
    let mut enc_stats: EncryptedColumnStats = Vec::new();

    for col in spec.columns {
        let region_offset = buf.len() as u64;
        let mut region_len = 0u64;
        let mut stats = Vec::with_capacity(col.pages.len());
        let mut col_minmax: Vec<PageMinMax> = Vec::new();
        for (page_seq, page) in col.pages.iter().enumerate() {
            if enc.is_some() && page_seq >= u16::MAX as usize {
                return Err(MongrelError::Full(format!(
                    "column {:#x} exceeds 65534 pages; encrypted-run page-seq nonce space exhausted",
                    col.column_id
                )));
            }
            let on_disk: Vec<u8> = match &enc {
                Some(e) => e.cipher.encrypt_page(
                    &page_nonce(e.nonce_prefix, col.column_id, page_seq as u32),
                    page,
                )?,
                None => page.clone(),
            };
            let offset = buf.len() as u64;
            buf.write_all(&on_disk)?;
            content_hasher.update(&on_disk);
            region_len += on_disk.len() as u64;
            let mut stat = if let Some(s) = col.page_stats.get(page_seq) {
                s.clone()
            } else {
                PageStat {
                    first_row_id: 0,
                    last_row_id: 0,
                    null_count: 0,
                    row_count: 0,
                    min: None,
                    max: None,
                    offset: 0,
                    compressed_len: 0,
                    uncompressed_len: 0,
                }
            };
            stat.offset = offset;
            stat.compressed_len = on_disk.len() as u32;
            stat.uncompressed_len = page.len() as u32;
            // See plan_run: the cleartext directory must not carry plaintext
            // min/max for encrypted columns — they move into the encrypted
            // stats envelope. Keep this byte-identical to plan_run so both
            // writers emit the same run.
            if enc.is_some() {
                col_minmax.push((stat.min.take(), stat.max.take()));
            }
            stats.push(stat);
        }
        if col_minmax
            .iter()
            .any(|(mn, mx)| mn.is_some() || mx.is_some())
        {
            enc_stats.push((col.column_id, col_minmax));
        }
        let page_flags = if enc.is_some() {
            ColumnPageHeader::PAGE_ENCRYPTED
        } else {
            0
        };
        dir.push(ColumnPageHeader {
            column_id: col.column_id,
            type_id_tag: col.type_id_tag,
            encoding: col.encoding as u8,
            flags: page_flags,
            page_count: col.pages.len() as u32,
            page_region_offset: region_offset,
            page_region_len: region_len,
            page_stats: stats,
        });
    }

    let column_dir_offset = buf.len() as u64;
    let dir_bytes = bincode::serialize(&dir)?;
    buf.write_all(&dir_bytes)?;

    let index_trailer_offset = match index_trailer {
        Some(trailer) => {
            let off = buf.len() as u64;
            buf.write_all(trailer)?;
            off
        }
        None => 0,
    };
    let encryption_descriptor_offset = match &enc {
        Some(e) => {
            let off = buf.len() as u64;
            buf.write_all(&(e.descriptor_bytes.len() as u32).to_le_bytes())?;
            buf.write_all(&e.descriptor_bytes)?;
            off
        }
        None => 0,
    };
    let (encrypted_stats_offset, encrypted_stats_len) = match &enc {
        Some(e) if !enc_stats.is_empty() => {
            let plain = bincode::serialize(&enc_stats)?;
            let nonce = page_nonce(e.nonce_prefix, ENC_STATS_NONCE_COLUMN, ENC_STATS_NONCE_SEQ);
            let ct = e.cipher.encrypt_page(&nonce, &plain)?;
            let off = buf.len() as u64;
            let len = ct.len() as u64;
            buf.write_all(&ct)?;
            (off, len)
        }
        _ => (0, 0),
    };
    let footer_offset = buf.len() as u64;

    let header_flags = if enc.is_some() {
        spec.flags | RUN_FLAG_ENCRYPTED
    } else {
        spec.flags
    };
    let header = RunHeader {
        magic: RUN_MAGIC,
        format_version: RUN_FORMAT_VERSION,
        header_layout_version: RUN_HEADER_VERSION,
        run_id: spec.run_id,
        content_hash: content_hasher.finalize().into(),
        schema_id: spec.schema_id,
        epoch_created: spec.epoch_created,
        level: spec.level,
        flags: header_flags,
        sort_key_column_id: spec.sort_key_column_id,
        row_count: spec.row_count,
        min_row_id: spec.min_row_id,
        max_row_id: spec.max_row_id,
        column_count: spec.columns.len() as u64,
        column_dir_offset,
        index_trailer_offset,
        encryption_descriptor_offset,
        footer_offset,
        encrypted_stats_offset,
        encrypted_stats_len,
    };
    let header_bytes = bincode::serialize(&header)?;
    if header_bytes.len() > RUN_HEADER_PAD {
        return Err(MongrelError::InvalidArgument(format!(
            "run header too large: {} > {RUN_HEADER_PAD}",
            header_bytes.len()
        )));
    }
    buf[..header_bytes.len()].copy_from_slice(&header_bytes);

    let checksum = Sha256::digest(&buf[..footer_offset as usize]);
    buf.write_all(&RUN_MAGIC)?;
    buf.write_all(&footer_offset.to_le_bytes())?;
    buf.write_all(&checksum)?;
    // Encrypted runs: append the keyed metadata MAC (byte-identical to the mmap
    // writer). `dir_bytes`/`header_bytes` here are the exact serialized forms.
    if let Some(tag) = compute_run_mac(enc.as_ref(), &header_bytes, &dir_bytes) {
        buf.write_all(&tag)?;
    }

    Ok((buf, header))
}

fn page_nonce(nonce_prefix: [u8; 12], column_id: u16, page_seq: u32) -> [u8; 12] {
    let mut n = nonce_prefix;
    n[8..10].copy_from_slice(&column_id.to_le_bytes());
    n[10..12].copy_from_slice(&(page_seq as u16).to_le_bytes());
    n
}

/// Decrypt the run's per-page min/max envelope ([`EncryptedColumnStats`]) and
/// overlay the bounds onto the in-memory column directory, restoring zone-map
/// page pruning for encrypted columns. Caller must have verified the run
/// metadata MAC first (the envelope's offset/len live in the header); the
/// envelope itself is AES-256-GCM-authenticated, so tampering fails loudly —
/// the same posture as a tampered page payload.
fn overlay_encrypted_stats(
    file: &mut File,
    header: &RunHeader,
    cipher: &dyn Cipher,
    nonce_prefix: [u8; 12],
    dir: &mut [ColumnPageHeader],
) -> Result<()> {
    file.seek(SeekFrom::Start(header.encrypted_stats_offset))?;
    const MAX_ENCRYPTED_STATS_BYTES: u64 = 64 * 1024 * 1024;
    if header.encrypted_stats_len > MAX_ENCRYPTED_STATS_BYTES {
        return Err(MongrelError::InvalidArgument(format!(
            "encrypted run stats length {} exceeds {MAX_ENCRYPTED_STATS_BYTES}",
            header.encrypted_stats_len
        )));
    }
    let mut ct = vec![0u8; header.encrypted_stats_len as usize];
    file.read_exact(&mut ct)?;
    let nonce = page_nonce(nonce_prefix, ENC_STATS_NONCE_COLUMN, ENC_STATS_NONCE_SEQ);
    let plain = cipher.decrypt_page(&nonce, &ct)?;
    let stats: EncryptedColumnStats = bincode::deserialize(&plain)
        .map_err(|e| MongrelError::Encryption(format!("bad encrypted page-stats envelope: {e}")))?;
    for (cid, minmax) in stats {
        let Some(col) = dir.iter_mut().find(|c| c.column_id == cid) else {
            continue;
        };
        for (stat, (mn, mx)) in col.page_stats.iter_mut().zip(minmax) {
            stat.min = mn;
            stat.max = mx;
        }
    }
    Ok(())
}

/// Stable content-address of an immutable run page (the cache key): SHA-256 of
/// `(table_id, run_id, column_id, page_seq)`. Runs are immutable, so this
/// identity is also the page's content address — a rewritten page lives in a
/// different run (different id) and so gets a different key without any
/// invalidation sweep. `table_id` namespaces the shared cache across tables in
/// a `Database` so two tables' identically-numbered runs never collide.
pub(crate) fn page_cache_key(
    table_id: u64,
    run_id: u128,
    column_id: u16,
    page_seq: usize,
) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(table_id.to_be_bytes());
    h.update(run_id.to_be_bytes());
    h.update(column_id.to_be_bytes());
    h.update((page_seq as u64).to_be_bytes());
    let out = h.finalize();
    let mut k = [0u8; 32];
    k.copy_from_slice(&out);
    k
}

/// Decrypt a raw (on-disk) page when the column is encrypted, else pass it
/// through. The shared page cache stores the raw bytes (ciphertext when
/// encrypted), so this runs after every cache hit or disk read.
fn decrypt_or_passthrough(
    cipher: Option<&dyn Cipher>,
    nonce_prefix: [u8; 12],
    column_id: u16,
    page_seq: usize,
    encrypted: bool,
    buf: &[u8],
) -> Result<Vec<u8>> {
    if encrypted {
        match cipher {
            Some(c) => {
                Ok(c.decrypt_page(&page_nonce(nonce_prefix, column_id, page_seq as u32), buf)?)
            }
            None => Err(MongrelError::Decryption(
                "encrypted page but no cipher".into(),
            )),
        }
    } else {
        Ok(buf.to_vec())
    }
}

/// Read just the header and confirm the footer magic, without hashing the
/// body. Cheap: two small fixed-size reads, no allocation proportional to
/// run size. Only safe as a substitute for [`read_header`] when the caller
/// has independent evidence this exact `run_id`'s checksum already verified
/// in this process (see `open_with_cache`'s `verified_runs` cache) — `.sr`
/// runs are immutable once written, so a checksum verified once cannot
/// regress later in the same process lifetime. Still catches gross
/// corruption (truncation, garbled header) via the magic checks and the
/// bincode deserialize.
fn read_header_fast_from_file(file: &mut File) -> Result<RunHeader> {
    file.seek(SeekFrom::Start(0))?;
    let mut header_buf = [0u8; RUN_HEADER_PAD];
    file.read_exact(&mut header_buf)?;
    let header: RunHeader = bincode::deserialize(&header_buf)
        .map_err(|e| MongrelError::InvalidArgument(format!("bad run header: {e}")))?;
    validate_run_header_bytes(&header, &header_buf)?;
    validate_run_layout(&header, file.metadata()?.len())?;
    file.seek(SeekFrom::Start(header.footer_offset))?;
    let mut footer_magic = [0u8; 8];
    file.read_exact(&mut footer_magic)?;
    if footer_magic != RUN_MAGIC {
        return Err(MongrelError::MagicMismatch {
            what: "sorted run footer",
            expected: RUN_MAGIC,
            got: footer_magic,
        });
    }
    Ok(header)
}

/// Read and validate a run header (magic + footer checksum).
pub fn read_header(path: impl AsRef<Path>) -> Result<RunHeader> {
    let mut file = crate::durable_file::open_regular_nofollow(path.as_ref())?;
    read_header_from_file(&mut file)
}

fn validate_run_layout(header: &RunHeader, file_len: u64) -> Result<()> {
    let footer_len = 8u64
        + 8
        + 32
        + if header.is_encrypted() {
            RUN_MAC_LEN as u64
        } else {
            0
        };
    let expected_len = header
        .footer_offset
        .checked_add(footer_len)
        .ok_or_else(|| MongrelError::InvalidArgument("sorted run length overflow".into()))?;
    if header.footer_offset < RUN_HEADER_PAD as u64 || expected_len != file_len {
        return Err(MongrelError::InvalidArgument(format!(
            "invalid sorted run layout: footer={} file={file_len}",
            header.footer_offset
        )));
    }
    let in_body = |offset: u64| {
        offset == 0 || (offset >= RUN_HEADER_PAD as u64 && offset <= header.footer_offset)
    };
    if header.column_dir_offset < RUN_HEADER_PAD as u64
        || header.column_dir_offset > header.footer_offset
        || !in_body(header.index_trailer_offset)
        || !in_body(header.encryption_descriptor_offset)
        || !in_body(header.encrypted_stats_offset)
        || header
            .encrypted_stats_offset
            .checked_add(header.encrypted_stats_len)
            .is_none_or(|end| end > header.footer_offset)
    {
        return Err(MongrelError::InvalidArgument(
            "sorted run metadata offsets are outside the file".into(),
        ));
    }
    Ok(())
}

fn validate_run_header_bytes(header: &RunHeader, bytes: &[u8; RUN_HEADER_PAD]) -> Result<()> {
    if header.magic != RUN_MAGIC {
        return Err(MongrelError::MagicMismatch {
            what: "sorted run",
            expected: RUN_MAGIC,
            got: header.magic,
        });
    }
    const KNOWN_FLAGS: u8 =
        RUN_FLAG_ENCRYPTED | RUN_FLAG_TOMBSTONE_ONLY | RUN_FLAG_CLEAN | RUN_FLAG_UNIFORM_EPOCH;
    if header.format_version != RUN_FORMAT_VERSION
        || header.header_layout_version != RUN_HEADER_VERSION
        || header.flags & !KNOWN_FLAGS != 0
        || (header.row_count == 0 && (header.min_row_id != 0 || header.max_row_id != 0))
        || (header.row_count != 0 && header.min_row_id > header.max_row_id)
    {
        return Err(MongrelError::InvalidArgument(
            "unsupported or invalid sorted run header".into(),
        ));
    }
    let canonical = bincode::serialize(header)?;
    if canonical.len() > RUN_HEADER_PAD
        || bytes[..canonical.len()] != canonical
        || bytes[canonical.len()..].iter().any(|byte| *byte != 0)
    {
        return Err(MongrelError::InvalidArgument(
            "sorted run header has noncanonical bytes or nonzero padding".into(),
        ));
    }
    Ok(())
}

fn read_header_from_file(file: &mut File) -> Result<RunHeader> {
    file.seek(SeekFrom::Start(0))?;
    let mut header_buf = [0u8; RUN_HEADER_PAD];
    file.read_exact(&mut header_buf)?;
    let header: RunHeader = bincode::deserialize(&header_buf)
        .map_err(|e| MongrelError::InvalidArgument(format!("bad run header: {e}")))?;
    validate_run_header_bytes(&header, &header_buf)?;

    validate_run_layout(&header, file.metadata()?.len())?;
    file.seek(SeekFrom::Start(header.footer_offset))?;
    let mut footer = [0u8; 8 + 8 + 32];
    file.read_exact(&mut footer)?;
    if footer[..8] != RUN_MAGIC {
        return Err(MongrelError::MagicMismatch {
            what: "sorted run footer",
            expected: RUN_MAGIC,
            got: footer[..8].try_into().unwrap(),
        });
    }
    let mut hasher = Sha256::new();
    file.seek(SeekFrom::Start(0))?;
    let mut remaining = header.footer_offset;
    let mut buffer = [0u8; 64 * 1024];
    while remaining != 0 {
        let length = usize::try_from(remaining.min(buffer.len() as u64)).unwrap();
        file.read_exact(&mut buffer[..length])?;
        hasher.update(&buffer[..length]);
        remaining -= length as u64;
    }
    let computed: [u8; 32] = hasher.finalize().into();
    let stored: [u8; 32] = footer[16..].try_into().unwrap();
    if computed != stored {
        return Err(MongrelError::ChecksumMismatch {
            expected: u64::from_be_bytes(stored[..8].try_into().unwrap()),
            actual: u64::from_be_bytes(computed[..8].try_into().unwrap()),
            context: "sorted run footer".into(),
        });
    }
    file.seek(SeekFrom::Start(RUN_HEADER_PAD as u64))?;
    let mut remaining = header.column_dir_offset - RUN_HEADER_PAD as u64;
    let mut content = Sha256::new();
    while remaining != 0 {
        let length = usize::try_from(remaining.min(buffer.len() as u64)).unwrap();
        file.read_exact(&mut buffer[..length])?;
        content.update(&buffer[..length]);
        remaining -= length as u64;
    }
    let content_hash: [u8; 32] = content.finalize().into();
    if content_hash != header.content_hash {
        return Err(MongrelError::ChecksumMismatch {
            expected: u64::from_be_bytes(header.content_hash[..8].try_into().unwrap()),
            actual: u64::from_be_bytes(content_hash[..8].try_into().unwrap()),
            context: "sorted run content hash".into(),
        });
    }
    Ok(header)
}

/// Read the column directory.
pub fn read_column_dir(
    path: impl AsRef<Path>,
    header: &RunHeader,
) -> Result<Vec<ColumnPageHeader>> {
    let mut file = crate::durable_file::open_regular_nofollow(path.as_ref())?;
    read_column_dir_from_file(&mut file, header)
}

fn read_column_dir_from_file(file: &mut File, header: &RunHeader) -> Result<Vec<ColumnPageHeader>> {
    file.seek(SeekFrom::Start(header.column_dir_offset))?;
    let end = [
        header.index_trailer_offset,
        header.encryption_descriptor_offset,
        header.encrypted_stats_offset,
        header.footer_offset,
    ]
    .into_iter()
    .filter(|offset| *offset > header.column_dir_offset)
    .min()
    .ok_or_else(|| {
        MongrelError::InvalidArgument("sorted run column directory has no end".into())
    })?;
    let len = end.checked_sub(header.column_dir_offset).ok_or_else(|| {
        MongrelError::InvalidArgument("sorted run column directory offsets are reversed".into())
    })?;
    const MAX_COLUMN_DIR_BYTES: u64 = 64 * 1024 * 1024;
    if len > MAX_COLUMN_DIR_BYTES {
        return Err(MongrelError::InvalidArgument(format!(
            "sorted run column directory length {len} exceeds {MAX_COLUMN_DIR_BYTES}"
        )));
    }
    let mut buf = vec![0u8; len as usize];
    file.read_exact(&mut buf)?;
    let dir: Vec<ColumnPageHeader> = bincode::deserialize(&buf)
        .map_err(|e| MongrelError::InvalidArgument(format!("bad column dir: {e}")))?;
    Ok(dir)
}

fn validate_column_directory_layout(header: &RunHeader, dir: &[ColumnPageHeader]) -> Result<()> {
    if header.column_count != dir.len() as u64 || dir.len() > u16::MAX as usize {
        return Err(MongrelError::InvalidArgument(
            "sorted run column count is invalid".into(),
        ));
    }
    let mut regions = Vec::with_capacity(dir.len());
    let mut ids = std::collections::HashSet::new();
    for column in dir {
        if !ids.insert(column.column_id)
            || column.flags & !ColumnPageHeader::PAGE_ENCRYPTED != 0
            || column.page_count as usize != column.page_stats.len()
        {
            return Err(MongrelError::InvalidArgument(
                "sorted run column directory identity is invalid".into(),
            ));
        }
        let region_end = column
            .page_region_offset
            .checked_add(column.page_region_len)
            .ok_or_else(|| MongrelError::InvalidArgument("run page region overflows".into()))?;
        if column.page_region_offset < RUN_HEADER_PAD as u64
            || region_end > header.column_dir_offset
        {
            return Err(MongrelError::InvalidArgument(
                "sorted run page region is outside its body".into(),
            ));
        }
        let mut cursor = column.page_region_offset;
        let mut rows = 0_u64;
        for stat in &column.page_stats {
            let length = stat.compressed_len as u64;
            let end = stat
                .offset
                .checked_add(length)
                .ok_or_else(|| MongrelError::InvalidArgument("run page offset overflows".into()))?;
            if stat.offset != cursor
                || length == 0
                || length > MAX_RUN_PAGE_BYTES
                || stat.uncompressed_len as u64 > MAX_RUN_PAGE_BYTES
                || stat.row_count as usize > PAGE_ROWS
                || end > region_end
            {
                return Err(MongrelError::InvalidArgument(
                    "sorted run page metadata is outside its region".into(),
                ));
            }
            rows = rows.checked_add(stat.row_count as u64).ok_or_else(|| {
                MongrelError::InvalidArgument("sorted run row count overflows".into())
            })?;
            cursor = end;
        }
        if cursor != region_end || rows != header.row_count {
            return Err(MongrelError::InvalidArgument(
                "sorted run page region length or row count is inconsistent".into(),
            ));
        }
        regions.push((column.page_region_offset, region_end));
    }
    regions.sort_unstable();
    if regions.windows(2).any(|pair| pair[0].1 > pair[1].0) {
        return Err(MongrelError::InvalidArgument(
            "sorted run page regions overlap".into(),
        ));
    }
    Ok(())
}

fn read_encryption_descriptor_bytes_from_file(
    file: &mut File,
    header: &RunHeader,
) -> Result<Vec<u8>> {
    file.seek(SeekFrom::Start(header.encryption_descriptor_offset))?;
    let mut len_buf = [0u8; 4];
    file.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    // The descriptor is tiny (~60 bytes + a few column entries); clamp to a
    // sane ceiling so a corrupt/malicious header can't trigger a multi-GiB
    // allocation. (The footer checksum already guards integrity; this is a
    // defense-in-depth bound on the pre-checksum read.)
    const MAX_DESCRIPTOR_BYTES: usize = 65_536;
    if len > MAX_DESCRIPTOR_BYTES {
        return Err(MongrelError::InvalidArgument(format!(
            "encryption descriptor length {len} exceeds {MAX_DESCRIPTOR_BYTES}"
        )));
    }
    let mut buf = vec![0u8; len];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

/// Authenticate an encrypted run's cleartext metadata (`header ‖ dir ‖
/// descriptor`) against the keyed MAC tag stored after the footer. Run BEFORE
/// any offset/stat from the directory is trusted to drive a read. Errors if the
/// tag is missing (a run written before run-metadata MACs existed) or does not
/// match (tampering, or the wrong key). The on-disk page payloads are AEAD-
/// authenticated separately, so they are not covered here.
fn verify_run_mac(
    file: &mut File,
    header: &RunHeader,
    dir: &[ColumnPageHeader],
    kek: &Kek,
    desc_bytes: &[u8],
) -> Result<()> {
    let header_bytes = bincode::serialize(header)?;
    let dir_bytes = bincode::serialize(dir)?;
    let mac_key = kek.derive_run_mac_key();
    let expected =
        crate::encryption::run_metadata_mac(&mac_key, &header_bytes, &dir_bytes, desc_bytes);
    file.seek(SeekFrom::Start(header.footer_offset + 8 + 8 + 32))?;
    let mut tag = [0u8; RUN_MAC_LEN];
    file.read_exact(&mut tag).map_err(|_| {
        MongrelError::Decryption(
            "encrypted run is missing or truncated its metadata MAC; cannot \
             authenticate metadata"
                .into(),
        )
    })?;
    // Constant-time comparison (no early-exit timing oracle on the tag).
    let mut diff = 0u8;
    for (x, y) in tag.iter().zip(expected.iter()) {
        diff |= x ^ y;
    }
    if diff != 0 {
        return Err(MongrelError::Decryption(
            "run metadata authentication failed — tampered run or wrong key".into(),
        ));
    }
    Ok(())
}

// ============================ high-level writer ============================

/// Builds and writes a sorted run from drained memtable rows.
///
/// `rows` must be sorted ascending by `(row_id, epoch)` (the memtable's natural
/// drain order). System columns `_row_id`, `_epoch`, `_deleted` are always
/// emitted; each user column in `schema` is emitted, with `Null` for rows that
/// don't set it.
pub struct RunWriter<'a> {
    schema: &'a Schema,
    run_id: u128,
    epoch_created: Epoch,
    level: u8,
    kek: Option<&'a Kek>,
    /// `(column_id, scheme)` for each ENCRYPTED_INDEXABLE column — wrapped into
    /// the run's Encryption Descriptor (Phase 10.2).
    indexable_columns: Vec<(u16, u8)>,
    /// Per-page compression policy (Phase 14.4 / 15.3). Default `Zstd(3)`
    /// (compaction); the bulk path uses `Zstd(1)` or `Lz4` (hot, scan-heavy
    /// runs), and `Plain` skips compression entirely (`bulk_load_fast`).
    compress: columnar::Compress,
    /// Whether this run is "clean" (one version per RowId, no tombstones,
    /// ascending row_ids) — written into [`RUN_FLAG_CLEAN`] so readers can skip
    /// the MVCC visibility pass. Set true only by paths that construct clean
    /// system columns by construction (typed bulk load, compaction output).
    clean: bool,
    /// Whether this run's stored `_epoch` column is a placeholder and its real
    /// commit epoch lives in the manifest `RunRef.epoch_created` (set only by the
    /// large-transaction spill path, which writes before the epoch is assigned).
    /// Stamps [`RUN_FLAG_UNIFORM_EPOCH`] so the reader overlays the real epoch.
    uniform_epoch: bool,
    /// Write fixed-width page payloads in little-endian (Phase 15.7) so the
    /// decode path is a memcpy on real (x86/ARM) hardware instead of a
    /// per-element `swap_bytes`. Default **true** for the typed write paths
    /// (`write_native`); the legacy `write` (Value) path keeps big-endian for
    /// back-compat with pre-15.7 runs. The flag is stored per-page (bit 3 of the
    /// algo byte), so a run may freely mix LE and BE pages.
    le: bool,
}

impl<'a> RunWriter<'a> {
    pub fn new(schema: &'a Schema, run_id: u128, epoch_created: Epoch, level: u8) -> Self {
        Self {
            schema,
            run_id,
            epoch_created,
            level,
            kek: None,
            indexable_columns: Vec::new(),
            compress: columnar::Compress::Zstd(3),
            clean: false,
            uniform_epoch: false,
            le: false,
        }
    }

    /// Mark this run as uniform-epoch: its stored `_epoch` column is a
    /// placeholder and the real commit epoch is supplied at read time from the
    /// manifest `RunRef.epoch_created` (see [`RUN_FLAG_UNIFORM_EPOCH`]). Used by
    /// the large-transaction spill path, which writes the run before the commit
    /// epoch is assigned.
    pub fn uniform_epoch(mut self, uniform: bool) -> Self {
        self.uniform_epoch = uniform;
        self
    }

    /// Encrypt this run's pages with a fresh per-file DEK wrapped by `kek`.
    /// `indexable_columns` are the ENCRYPTED_INDEXABLE `(column_id, scheme)`
    /// pairs whose column keys are derived+wrapped into the descriptor.
    pub fn with_encryption(mut self, kek: &'a Kek, indexable_columns: Vec<(u16, u8)>) -> Self {
        self.kek = Some(kek);
        self.indexable_columns = indexable_columns;
        self
    }

    /// Override the zstd level for this run's pages (Phase 14.4). The bulk
    /// ingest path uses level 1; background compaction upgrades cold runs to 3.
    pub fn with_zstd_level(mut self, level: i32) -> Self {
        self.compress = columnar::Compress::Zstd(level);
        self
    }

    /// Compress hot/mutable-run pages with LZ4 (Phase 15.3): 3–5× faster decode
    /// than zstd with ~10% worse ratio. The right default for runs that get
    /// scanned (bulk-loaded analytical runs).
    pub fn with_lz4(mut self) -> Self {
        self.compress = columnar::Compress::Lz4;
        self
    }

    /// Emit raw `ALGO_PLAIN` pages with no compression (Phase 14.4
    /// `bulk_load_fast`): maximal encode throughput at the cost of ~3–4× size.
    pub fn with_plain(mut self) -> Self {
        self.compress = columnar::Compress::Plain;
        self
    }

    /// Mark this run as "clean" (one version per RowId, no tombstones, ascending
    /// row_ids). Only set when the caller constructs the system columns so by
    /// construction (typed bulk load of fresh, contiguous row_ids; compaction
    /// output that has collapsed versions and dropped tombstones). Stamps
    /// [`RUN_FLAG_CLEAN`] into the header so readers skip the MVCC pass.
    pub fn clean(mut self, clean: bool) -> Self {
        self.clean = clean;
        self
    }

    /// Write fixed-width page payloads in little-endian (Phase 15.7): the decode
    /// path becomes a memcpy on little-endian targets. Only effective on the
    /// typed `write_native` path; no-op on big-endian writers (they keep the
    /// portable BE layout, since "native" would not be LE there).
    pub fn with_native_endian(mut self) -> Self {
        if cfg!(target_endian = "little") {
            self.le = true;
        }
        self
    }

    /// Write a run straight from typed columns (no `Value`). `user_columns` are
    /// the schema's user columns as [`NativeColumn`]s; the system columns
    /// (`_row_id`/`_epoch`/`_deleted`) are built from `first_row_id..+n` /
    /// `epoch_created` / all-false. Sorted Int64 columns (always the system
    /// `_row_id`, plus any sorted user Int64) use delta encoding unless
    /// [`RunWriter::with_plain`] forces raw `ALGO_PLAIN` everywhere.
    pub fn write_native(
        self,
        path: impl AsRef<Path>,
        user_columns: &[(u16, columnar::NativeColumn)],
        n: usize,
        first_row_id: u64,
    ) -> Result<RunHeader> {
        self.write_native_target(Some(path.as_ref()), None, user_columns, n, first_row_id)
    }

    pub(crate) fn write_native_file(
        self,
        file: File,
        user_columns: &[(u16, columnar::NativeColumn)],
        n: usize,
        first_row_id: u64,
    ) -> Result<RunHeader> {
        self.write_native_target(None, Some(file), user_columns, n, first_row_id)
    }

    fn write_native_target(
        self,
        path: Option<&Path>,
        file: Option<File>,
        user_columns: &[(u16, columnar::NativeColumn)],
        n: usize,
        first_row_id: u64,
    ) -> Result<RunHeader> {
        use columnar::NativeColumn;
        let row_id_col = NativeColumn::int64_sequence(first_row_id as i64, n);
        let epoch_col = NativeColumn::int64_constant(self.epoch_created.0 as i64, n);
        let deleted_col = NativeColumn::bool_constant(false, n);

        let learned_trailer = build_learned_trailer_native(&row_id_col);

        // All columns split on the same row-position boundaries so a reader can
        // skip whole pages of any column via its [min,max] stat.
        let row_ids = match &row_id_col {
            NativeColumn::Int64 { data, .. } => data.as_slice(),
            _ => &[],
        };
        let bounds = page_bounds(row_ids);
        let compress = self.compress;
        let le = self.le;
        let plain = matches!(compress, columnar::Compress::Plain);
        // In plain mode every column is `Encoding::Plain` (raw); otherwise keep
        // the chosen encoding (Delta for the sorted _row_id, Zstd for the rest).
        let row_id_enc = if plain {
            Encoding::Plain
        } else {
            Encoding::Delta
        };
        let sys_enc = if plain {
            Encoding::Plain
        } else {
            Encoding::Zstd
        };

        let mut columns: Vec<ColumnPayload> = Vec::with_capacity(3 + self.schema.columns.len());
        let (pages, stats) = native_column_pages(
            TypeId::Int64,
            &row_id_col,
            row_id_enc,
            compress,
            le,
            &bounds,
        )?;
        columns.push(ColumnPayload {
            column_id: SYS_ROW_ID,
            type_id_tag: type_tag(&TypeId::Int64),
            encoding: row_id_enc,
            pages,
            page_stats: stats,
        });
        let (pages, stats) =
            native_column_pages(TypeId::Int64, &epoch_col, sys_enc, compress, le, &bounds)?;
        columns.push(ColumnPayload {
            column_id: SYS_EPOCH,
            type_id_tag: type_tag(&TypeId::Int64),
            encoding: sys_enc,
            pages,
            page_stats: stats,
        });
        let (pages, stats) =
            native_column_pages(TypeId::Bool, &deleted_col, sys_enc, compress, le, &bounds)?;
        columns.push(ColumnPayload {
            column_id: SYS_DELETED,
            type_id_tag: type_tag(&TypeId::Bool),
            encoding: sys_enc,
            pages,
            page_stats: stats,
        });
        // Encode all user columns in parallel (Phase 14.3): each column's pages
        // are independent, and the page-level work is itself parallel, so a
        // wide table saturates the pool without oversubscribing (rayon
        // work-steals across the nested parallelism). Order is preserved so the
        // column directory matches `schema.columns` order.
        use rayon::prelude::*;
        let user_cols: Vec<ColumnPayload> = self
            .schema
            .columns
            .par_iter()
            .map(|cdef| -> Result<ColumnPayload> {
                let col = user_columns
                    .iter()
                    .find(|(id, _)| *id == cdef.id)
                    .map(|(_, c)| c)
                    .ok_or_else(|| {
                        MongrelError::ColumnNotFound(format!("user column {}", cdef.id))
                    })?;
                let encoding = if plain {
                    Encoding::Plain
                } else {
                    choose_encoding_native(&cdef.ty, col)
                };
                let (pages, stats) =
                    native_column_pages(cdef.ty.clone(), col, encoding, compress, le, &bounds)?;
                Ok(ColumnPayload {
                    column_id: cdef.id,
                    type_id_tag: type_tag(&cdef.ty),
                    encoding,
                    pages,
                    page_stats: stats,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        columns.extend(user_cols);

        // A uniform-epoch run's `_epoch` column is a placeholder (the real commit
        // epoch is overlaid from the RunRef at read time), so it must NOT be
        // marked clean — the clean fast path skips snapshot gating — and must
        // carry the overlay flag. write_native is not the spill path today, but
        // keep it sound if it ever is.
        let flags = if self.uniform_epoch {
            RUN_FLAG_UNIFORM_EPOCH
        } else {
            RUN_FLAG_CLEAN
        };
        let spec = RunSpec {
            run_id: self.run_id,
            schema_id: self.schema.schema_id,
            epoch_created: self.epoch_created.0,
            level: self.level,
            flags,
            sort_key_column_id: SYS_ROW_ID,
            row_count: n as u64,
            min_row_id: first_row_id,
            max_row_id: first_row_id + n as u64 - 1,
            columns: &columns,
        };
        match file {
            Some(file) => write_run_with_file(
                file,
                &spec,
                self.kek,
                &self.indexable_columns,
                Some(&learned_trailer),
            ),
            None => write_run_with(
                path.ok_or_else(|| {
                    MongrelError::InvalidArgument("sorted run output is missing".into())
                })?,
                &spec,
                self.kek,
                &self.indexable_columns,
                Some(&learned_trailer),
            ),
        }
    }

    pub fn write(self, path: impl AsRef<Path>, rows: &[Row]) -> Result<RunHeader> {
        self.write_target(Some(path.as_ref()), None, rows)
    }

    pub(crate) fn write_file(self, file: File, rows: &[Row]) -> Result<RunHeader> {
        self.write_target(None, Some(file), rows)
    }

    fn write_target(
        self,
        path: Option<&Path>,
        file: Option<File>,
        rows: &[Row],
    ) -> Result<RunHeader> {
        let n = rows.len();
        // System columns.
        let mut row_ids = Vec::with_capacity(n);
        let mut epochs = Vec::with_capacity(n);
        let mut deleted = Vec::with_capacity(n);
        let mut commit_ts_vals = Vec::with_capacity(n);
        // Emit SYS_COMMIT_TS when any version is HLC-stamped (P0.5-T3).
        let has_commit_ts = rows.iter().any(|r| r.commit_ts.is_some());
        for r in rows {
            row_ids.push(Value::Int64(r.row_id.0 as i64));
            epochs.push(Value::Int64(r.committed_epoch.0 as i64));
            deleted.push(Value::Bool(r.deleted));
            if has_commit_ts {
                commit_ts_vals.push(encode_commit_ts_value(r.commit_ts));
            }
        }
        let learned_trailer = build_learned_trailer(&row_ids);
        let (min_rid, max_rid) = row_id_bounds(rows);
        let row_id_i64: Vec<i64> = rows.iter().map(|r| r.row_id.0 as i64).collect();
        let bounds = page_bounds(&row_id_i64);

        let mut columns: Vec<ColumnPayload> =
            Vec::with_capacity(3 + usize::from(has_commit_ts) + self.schema.columns.len());
        let (pages, stats, enc) = value_column_pages(TypeId::Int64, &row_ids, &bounds)?;
        columns.push(ColumnPayload {
            column_id: SYS_ROW_ID,
            type_id_tag: type_tag(&TypeId::Int64),
            encoding: enc,
            pages,
            page_stats: stats,
        });
        let (pages, stats, enc) = value_column_pages(TypeId::Int64, &epochs, &bounds)?;
        columns.push(ColumnPayload {
            column_id: SYS_EPOCH,
            type_id_tag: type_tag(&TypeId::Int64),
            encoding: enc,
            pages,
            page_stats: stats,
        });
        let (pages, stats, enc) = value_column_pages(TypeId::Bool, &deleted, &bounds)?;
        columns.push(ColumnPayload {
            column_id: SYS_DELETED,
            type_id_tag: type_tag(&TypeId::Bool),
            encoding: enc,
            pages,
            page_stats: stats,
        });
        if has_commit_ts {
            let (pages, stats, enc) = value_column_pages(TypeId::Bytes, &commit_ts_vals, &bounds)?;
            columns.push(ColumnPayload {
                column_id: SYS_COMMIT_TS,
                type_id_tag: type_tag(&TypeId::Bytes),
                encoding: enc,
                pages,
                page_stats: stats,
            });
        }
        // User columns — choose an encoding per column from run-time stats.
        for cdef in &self.schema.columns {
            let vals: Vec<Value> = rows
                .iter()
                .map(|r| r.columns.get(&cdef.id).cloned().unwrap_or(Value::Null))
                .collect();
            let (pages, stats, encoding) = value_column_pages(cdef.ty.clone(), &vals, &bounds)?;
            columns.push(ColumnPayload {
                column_id: cdef.id,
                type_id_tag: type_tag(&cdef.ty),
                encoding,
                pages,
                page_stats: stats,
            });
        }

        let spec = RunSpec {
            run_id: self.run_id,
            schema_id: self.schema.schema_id,
            epoch_created: self.epoch_created.0,
            level: self.level,
            flags: {
                let mut f = if self.clean { RUN_FLAG_CLEAN } else { 0 };
                if self.uniform_epoch {
                    f |= RUN_FLAG_UNIFORM_EPOCH;
                }
                f
            },
            sort_key_column_id: SYS_ROW_ID,
            row_count: n as u64,
            min_row_id: min_rid,
            max_row_id: max_rid,
            columns: &columns,
        };
        match file {
            Some(file) => write_run_with_file(
                file,
                &spec,
                self.kek,
                &self.indexable_columns,
                Some(&learned_trailer),
            ),
            None => write_run_with(
                path.ok_or_else(|| {
                    MongrelError::InvalidArgument("sorted run output is missing".into())
                })?,
                &spec,
                self.kek,
                &self.indexable_columns,
                Some(&learned_trailer),
            ),
        }
    }
}

fn type_tag(ty: &TypeId) -> u16 {
    // Informational only; the reader resolves the full type from the schema.
    match ty {
        TypeId::Bool => 1,
        TypeId::Int64 => 8,
        TypeId::Float64 => 9,
        TypeId::Bytes => 12,
        TypeId::Embedding { .. } => 13,
        _ => 0,
    }
}

/// Decode a big-endian i64 from a `PageStat` min/max slot (None if absent or
/// truncated — treated as "all-null page" by the caller).
pub(crate) fn be_i64(b: Option<&[u8]>) -> Option<i64> {
    let b = b?;
    (b.len() == 8).then(|| i64::from_be_bytes(b.try_into().unwrap()))
}

/// Decode a big-endian f64 (stored as `to_bits`) from a stat slot.
pub(crate) fn be_f64(b: Option<&[u8]>) -> Option<f64> {
    let b = b?;
    (b.len() == 8).then(|| f64::from_bits(u64::from_be_bytes(b.try_into().unwrap())))
}

/// Rows per columnar page. Small enough to prune effectively, large enough to
/// keep per-page overhead negligible. ~16 pages per 1M-row column.
const PAGE_ROWS: usize = 65_536;

/// `(start, end, first_row_id, last_row_id)` for each page, derived from the
/// actual row-id column so flush paths with non-contiguous ids stay correct.
fn page_bounds(row_ids: &[i64]) -> Vec<(usize, usize, u64, u64)> {
    let n = row_ids.len();
    if n == 0 {
        return vec![(0, 0, 0, 0)];
    }
    let mut out = Vec::new();
    let mut start = 0;
    while start < n {
        let end = (start + PAGE_ROWS).min(n);
        out.push((start, end, row_ids[start] as u64, row_ids[end - 1] as u64));
        start = end;
    }
    out
}

/// Split a typed column into per-page encoded bytes + value-derived stats.
/// Pages are encoded in parallel across the rayon pool when a column spans more
/// than one page (Phase 14.3) — a 1M-row column has 16 independent encode tasks.
/// `compress` selects the page algorithm (Phase 14.4 / 15.3). Order is preserved
/// so the column directory stays sequential by page_seq.
fn native_column_pages(
    ty: TypeId,
    col: &columnar::NativeColumn,
    encoding: Encoding,
    compress: columnar::Compress,
    le: bool,
    bounds: &[(usize, usize, u64, u64)],
) -> Result<(Vec<Vec<u8>>, Vec<PageStat>)> {
    use rayon::prelude::*;
    let encode_one =
        |&(s, e, frid, lrid): &(usize, usize, u64, u64)| -> Result<(Vec<u8>, PageStat)> {
            let chunk = col.slice_range(s, e);
            let stat = columnar::page_stat_for(ty.clone(), &chunk, frid, lrid);
            let page = columnar::encode_page_native(ty.clone(), &chunk, encoding, compress, le)?;
            Ok((page, stat))
        };
    // Single-page columns skip the thread-pool handshake.
    let pairs: Vec<(Vec<u8>, PageStat)> = if bounds.len() > 1 {
        bounds
            .par_iter()
            .map(encode_one)
            .collect::<Result<Vec<_>>>()?
    } else {
        bounds.iter().map(encode_one).collect::<Result<Vec<_>>>()?
    };
    let (pages, stats) = pairs.into_iter().unzip();
    Ok((pages, stats))
}

/// Split a `Value` column into per-page encoded bytes + stats (flush path).
fn value_column_pages(
    ty: TypeId,
    vals: &[Value],
    bounds: &[(usize, usize, u64, u64)],
) -> Result<(Vec<Vec<u8>>, Vec<PageStat>, Encoding)> {
    let encoding = choose_encoding(&ty, vals);
    let mut pages = Vec::with_capacity(bounds.len());
    let mut stats = Vec::with_capacity(bounds.len());
    for &(s, e, frid, lrid) in bounds {
        let chunk = &vals[s..e];
        pages.push(columnar::encode_page(ty.clone(), chunk, encoding)?);
        let native = columnar::values_to_native(ty.clone(), chunk);
        stats.push(columnar::page_stat_for(ty.clone(), &native, frid, lrid));
    }
    Ok((pages, stats, encoding))
}

/// Pick a page encoding from run-time stats: dictionary for low-cardinality
/// strings, zstd-plain otherwise.
fn choose_encoding(ty: &TypeId, values: &[Value]) -> Encoding {
    use std::collections::HashSet;
    if matches!(ty, TypeId::Bytes) {
        let n = values.len();
        if n > 0 {
            let distinct = values
                .iter()
                .filter(|v| !matches!(v, Value::Null))
                .map(|v| v.encode_key())
                .collect::<HashSet<_>>()
                .len();
            if (distinct as f64 / n as f64) < 0.5 {
                return Encoding::Dictionary;
            }
        }
    }
    Encoding::Zstd
}

/// Encoding choice for the typed (native) path: delta for sorted Int64,
/// dictionary for low-cardinality Bytes, zstd otherwise.
fn choose_encoding_native(ty: &TypeId, col: &columnar::NativeColumn) -> Encoding {
    use std::collections::HashSet;
    match (ty, col) {
        (TypeId::Int64 | TypeId::TimestampNanos, columnar::NativeColumn::Int64 { data, .. }) => {
            if data.windows(2).all(|w| w[0] <= w[1]) {
                Encoding::Delta
            } else {
                Encoding::Zstd
            }
        }
        (
            TypeId::Bytes,
            columnar::NativeColumn::Bytes {
                offsets, values, ..
            },
        ) => {
            let n = offsets.len().saturating_sub(1);
            if n > 0 {
                let distinct: HashSet<&[u8]> = (0..n)
                    .map(|i| &values[offsets[i] as usize..offsets[i + 1] as usize])
                    .collect();
                if (distinct.len() as f64 / n as f64) < 0.5 {
                    return Encoding::Dictionary;
                }
            }
            Encoding::Zstd
        }
        _ => Encoding::Zstd,
    }
}

/// PGM-index trailer built straight from the typed row-id column.
fn build_learned_trailer_native(col: &columnar::NativeColumn) -> Vec<u8> {
    let points: Vec<(u64, usize)> = match col {
        columnar::NativeColumn::Int64 { data, .. } => data
            .iter()
            .enumerate()
            .map(|(i, v)| (*v as u64, i))
            .collect(),
        _ => Vec::new(),
    };
    let pgm = PgmIndex::build(&points, LEARNED_EPSILON);
    bincode::serialize(&pgm).expect("pgm serialize")
}

fn row_id_bounds(rows: &[Row]) -> (u64, u64) {
    match (rows.first(), rows.last()) {
        (Some(f), Some(l)) => (f.row_id.0, l.row_id.0),
        _ => (0, 0),
    }
}

// ============================ high-level reader ============================

/// Reads a sorted run: decodes columns lazily (cached), answers MVCC point
/// lookups via page-pruned `SYS_ROW_ID` bounds, and materializes visible rows
/// for scans.
pub struct RunReader {
    file: File,
    mmap: Option<memmap2::Mmap>,
    header: RunHeader,
    dir: Vec<ColumnPageHeader>,
    schema: Schema,
    /// Owning table id — namespaces the shared page cache across tables.
    table_id: u64,
    /// Per-run page cipher, built from the unwrapped DEK (None when plaintext).
    cipher: Option<Box<dyn Cipher>>,
    /// Per-run nonce prefix (overlaid per page with column_id + page_seq).
    nonce_prefix: [u8; 12],
    col_cache: HashMap<u16, Vec<Value>>,
    /// Shared, MVCC content-addressed page cache (Phase 9.2). Caches raw page
    /// bytes (ciphertext when encrypted) so all readers share decoded/decrypted
    /// pages. `None` only in standalone tests.
    page_cache: Option<Arc<crate::cache::Sharded<crate::cache::PageCache>>>,
    /// Shared decoded-page cache (Phase 15.4): the post-decompress/decrypt typed
    /// page, so a repeat scan skips decode. Keyed by `(run_id, column_id,
    /// page_seq)` identity; `None` in standalone tests.
    decoded_cache: Option<Arc<crate::cache::Sharded<crate::cache::DecodedPageCache>>>,
    /// Uniform-epoch overlay (see [`RUN_FLAG_UNIFORM_EPOCH`]). When `Some`, every
    /// row's commit epoch is taken to be this value instead of the placeholder
    /// stored in the `_epoch` column. Set by [`Self::set_uniform_epoch`].
    epoch_override: Option<Epoch>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RunVisibleVersion {
    pub(crate) row_id: RowId,
    pub(crate) committed_epoch: Epoch,
    pub(crate) deleted: bool,
    page_seq: usize,
    within_page: usize,
}

/// Page-bounded cursor over one run's newest snapshot-visible version per row.
/// Only the three compact system columns and one user-data page are decoded at
/// a time. Full `Vec<Row>` materialization is deliberately avoided.
pub(crate) struct RunVisibleVersionCursor {
    reader: RunReader,
    snapshot: Epoch,
    page_row_counts: Vec<usize>,
    page_seq: usize,
    within_page: usize,
    row_ids: Vec<i64>,
    epochs: Vec<i64>,
    deleted: Vec<u8>,
    lookahead: Option<RunVisibleVersion>,
    materialized_page: Option<usize>,
    materialized_columns: Vec<(u16, columnar::NativeColumn)>,
}

impl RunVisibleVersionCursor {
    fn new(reader: RunReader, snapshot: Epoch) -> Result<Self> {
        let page_row_counts = reader.page_row_counts(SYS_ROW_ID)?;
        Ok(Self {
            reader,
            snapshot,
            page_row_counts,
            page_seq: 0,
            within_page: 0,
            row_ids: Vec::new(),
            epochs: Vec::new(),
            deleted: Vec::new(),
            lookahead: None,
            materialized_page: None,
            materialized_columns: Vec::new(),
        })
    }

    fn load_system_page(&mut self, control: &crate::ExecutionControl) -> Result<bool> {
        while self.page_seq < self.page_row_counts.len() {
            control.checkpoint()?;
            let rows = self.page_row_counts[self.page_seq];
            if rows == 0 {
                self.page_seq += 1;
                continue;
            }
            self.row_ids = match columnar::decode_page_native(
                TypeId::Int64,
                &self.reader.read_page(SYS_ROW_ID, self.page_seq)?,
                rows,
            )? {
                columnar::NativeColumn::Int64 { data, .. } => data,
                _ => return Err(MongrelError::InvalidArgument("sys row_id not int64".into())),
            };
            self.epochs = if let Some(epoch) = self.reader.epoch_override {
                vec![epoch.0 as i64; rows]
            } else {
                match columnar::decode_page_native(
                    TypeId::Int64,
                    &self.reader.read_page(SYS_EPOCH, self.page_seq)?,
                    rows,
                )? {
                    columnar::NativeColumn::Int64 { data, .. } => data,
                    _ => return Err(MongrelError::InvalidArgument("sys epoch not int64".into())),
                }
            };
            self.deleted = match columnar::decode_page_native(
                TypeId::Bool,
                &self.reader.read_page(SYS_DELETED, self.page_seq)?,
                rows,
            )? {
                columnar::NativeColumn::Bool { data, .. } => data,
                _ => return Err(MongrelError::InvalidArgument("sys deleted not bool".into())),
            };
            self.within_page = 0;
            return Ok(true);
        }
        Ok(false)
    }

    fn next_raw(&mut self, control: &crate::ExecutionControl) -> Result<Option<RunVisibleVersion>> {
        if self.within_page >= self.row_ids.len() {
            if !self.row_ids.is_empty() {
                self.page_seq += 1;
                self.row_ids.clear();
                self.epochs.clear();
                self.deleted.clear();
            }
            if !self.load_system_page(control)? {
                return Ok(None);
            }
        }
        if self.within_page.is_multiple_of(256) {
            control.checkpoint()?;
        }
        let position = self.within_page;
        self.within_page += 1;
        Ok(Some(RunVisibleVersion {
            row_id: RowId(self.row_ids[position] as u64),
            committed_epoch: Epoch(self.epochs[position] as u64),
            deleted: self.deleted[position] != 0,
            page_seq: self.page_seq,
            within_page: position,
        }))
    }

    pub(crate) fn next_visible_version(
        &mut self,
        control: &crate::ExecutionControl,
    ) -> Result<Option<RunVisibleVersion>> {
        loop {
            let first = match self.lookahead.take() {
                Some(version) => version,
                None => match self.next_raw(control)? {
                    Some(version) => version,
                    None => return Ok(None),
                },
            };
            let row_id = first.row_id;
            let mut best = (first.committed_epoch <= self.snapshot).then_some(first);
            while let Some(candidate) = self.next_raw(control)? {
                if candidate.row_id != row_id {
                    self.lookahead = Some(candidate);
                    break;
                }
                if candidate.committed_epoch <= self.snapshot
                    && best
                        .is_none_or(|current| candidate.committed_epoch > current.committed_epoch)
                {
                    best = Some(candidate);
                }
            }
            if best.is_some() {
                return Ok(best);
            }
        }
    }

    pub(crate) fn materialize(
        &mut self,
        version: RunVisibleVersion,
        control: &crate::ExecutionControl,
    ) -> Result<Row> {
        if self.materialized_page != Some(version.page_seq) {
            let rows = self.page_row_counts[version.page_seq];
            let columns = self.reader.schema.columns.clone();
            let mut materialized = Vec::with_capacity(columns.len());
            for (index, column) in columns.into_iter().enumerate() {
                if index % 16 == 0 {
                    control.checkpoint()?;
                }
                let native = if self.reader.has_column(column.id) {
                    columnar::decode_page_native(
                        column.ty,
                        &self.reader.read_page(column.id, version.page_seq)?,
                        rows,
                    )?
                } else {
                    columnar::null_native(column.ty, rows)
                };
                materialized.push((column.id, native));
            }
            self.materialized_columns = materialized;
            self.materialized_page = Some(version.page_seq);
        }
        let columns = self
            .materialized_columns
            .iter()
            .map(|(column_id, column)| {
                (
                    *column_id,
                    column.value_at(version.within_page).unwrap_or(Value::Null),
                )
            })
            .collect();
        // Optional HLC stamp (P0.5-T3): load from SYS_COMMIT_TS when present.
        let commit_ts = if self.reader.has_column(SYS_COMMIT_TS) {
            let page = self.reader.read_page(SYS_COMMIT_TS, version.page_seq)?;
            let native = columnar::decode_page_native(
                TypeId::Bytes,
                &page,
                self.page_row_counts[version.page_seq],
            )?;
            decode_commit_ts_value(native.value_at(version.within_page).as_ref())
        } else {
            None
        };
        Ok(Row {
            row_id: version.row_id,
            committed_epoch: version.committed_epoch,
            columns,
            deleted: version.deleted,
            commit_ts,
        })
    }
}

impl RunReader {
    pub fn open(path: impl AsRef<Path>, schema: Schema, kek: Option<Arc<Kek>>) -> Result<Self> {
        Self::open_with_cache(path, schema, kek, None, None, 0, None)
    }

    pub(crate) fn open_file(file: File, schema: Schema, kek: Option<Arc<Kek>>) -> Result<Self> {
        Self::open_file_with_cache(file, schema, kek, None, None, 0, None, None)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_with_cache(
        path: impl AsRef<Path>,
        schema: Schema,
        kek: Option<Arc<Kek>>,
        page_cache: Option<Arc<crate::cache::Sharded<crate::cache::PageCache>>>,
        decoded_cache: Option<Arc<crate::cache::Sharded<crate::cache::DecodedPageCache>>>,
        table_id: u64,
        verified_runs: Option<&parking_lot::Mutex<std::collections::HashSet<u128>>>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = crate::durable_file::open_regular_nofollow(&path)?;
        Self::open_file_with_cache(
            file,
            schema,
            kek,
            page_cache,
            decoded_cache,
            table_id,
            verified_runs,
            Some(path),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_file_with_cache(
        mut file: File,
        schema: Schema,
        kek: Option<Arc<Kek>>,
        page_cache: Option<Arc<crate::cache::Sharded<crate::cache::PageCache>>>,
        decoded_cache: Option<Arc<crate::cache::Sharded<crate::cache::DecodedPageCache>>>,
        table_id: u64,
        verified_runs: Option<&parking_lot::Mutex<std::collections::HashSet<u128>>>,
        path: Option<PathBuf>,
    ) -> Result<Self> {
        let header = match verified_runs {
            Some(cache) => {
                let header = read_header_fast_from_file(&mut file)?;
                if cache.lock().contains(&header.run_id) {
                    header
                } else {
                    let verified = read_header_from_file(&mut file)?;
                    cache.lock().insert(verified.run_id);
                    verified
                }
            }
            None => read_header_from_file(&mut file)?,
        };
        let mut dir = read_column_dir_from_file(&mut file, &header)?;
        validate_column_directory_layout(&header, &dir)?;
        if header.is_encrypted() != kek.is_some() {
            return Err(MongrelError::Encryption(
                "sorted-run encryption mode differs from the database".into(),
            ));
        }
        // Unwrap this run's per-file DEK (stored wrapped in its Encryption
        // Descriptor) using the table KEK, then build the page cipher.
        let (cipher, nonce_prefix): (Option<Box<dyn Cipher>>, [u8; 12]) = if header.is_encrypted() {
            let kek = kek.as_ref().ok_or_else(|| {
                MongrelError::Encryption(
                    "run is encrypted but no key-encryption key was provided".into(),
                )
            })?;
            if header.encryption_descriptor_offset == 0 {
                return Err(MongrelError::Encryption(
                    "encrypted run has no encryption descriptor".into(),
                ));
            }
            let desc_bytes = read_encryption_descriptor_bytes_from_file(&mut file, &header)?;
            // Authenticate the cleartext metadata (header‖dir‖descriptor) under
            // the KEK-derived MAC key BEFORE trusting any offset/stat to drive a
            // read. Required for every encrypted run (no downgrade path: an
            // attacker can neither strip the encryption — pages stay ciphertext —
            // nor forge the tag without the key).
            verify_run_mac(&mut file, &header, &dir, kek, &desc_bytes)?;
            let enc = crate::encryption::build_run_cipher(kek, &desc_bytes)?;
            // With the metadata authenticated, decrypt the per-page min/max
            // envelope (v2 runs) and overlay it so zone-map pruning works on
            // encrypted columns exactly as it does on plaintext ones.
            if header.encrypted_stats_offset != 0 {
                overlay_encrypted_stats(
                    &mut file,
                    &header,
                    enc.cipher.as_ref(),
                    enc.nonce_prefix,
                    &mut dir,
                )?;
            }
            (Some(enc.cipher), enc.nonce_prefix)
        } else {
            (None, [0u8; 12])
        };
        // Best-effort memory map: lets the OS page cache manage I/O and removes
        // per-page seek+read syscalls. Falls back to read() on empty/unmappable
        // files. The `file` handle is kept for the lifetime of the mapping.
        // (Per-column `MADV_WILLNEED` read-ahead is issued from
        // `column_native_shared` — see Phase 15.2 — so a global advice policy is
        // not set here; that would degrade concurrent point lookups.)
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file) }.ok();
        let _ = path;
        Ok(Self {
            file,
            mmap,
            header,
            dir,
            schema,
            table_id,
            cipher,
            nonce_prefix,
            col_cache: HashMap::new(),
            epoch_override: None,
            page_cache,
            decoded_cache,
        })
    }

    pub fn header(&self) -> &RunHeader {
        &self.header
    }

    pub(crate) fn validate_all_pages(&mut self) -> Result<()> {
        let mut columns = std::collections::HashSet::new();
        let row_header = self.find_header(SYS_ROW_ID)?.clone();
        let expected_rows = row_header
            .page_stats
            .iter()
            .map(|stat| stat.row_count)
            .collect::<Vec<_>>();
        let mut row_bounds = Vec::with_capacity(row_header.page_stats.len());
        let mut previous = None::<u64>;
        let mut first = None::<u64>;
        let mut last = None::<u64>;
        let mut counted = 0_u64;
        for (page, stat) in row_header.page_stats.iter().enumerate() {
            let bytes = self.read_page(SYS_ROW_ID, page)?;
            let native =
                columnar::decode_page_native(TypeId::Int64, &bytes, stat.row_count as usize)?;
            if columnar::page_stat_for(TypeId::Int64, &native, 0, 0).null_count != 0 {
                return Err(MongrelError::InvalidArgument(
                    "sorted run row-id page contains nulls".into(),
                ));
            }
            let columnar::NativeColumn::Int64 { data, validity } = native else {
                return Err(MongrelError::InvalidArgument(
                    "sorted run row-id page has the wrong type".into(),
                ));
            };
            let _ = validity;
            if data.is_empty() {
                if self.header.row_count != 0 || stat.row_count != 0 {
                    return Err(MongrelError::InvalidArgument(
                        "sorted run row-id page is unexpectedly empty".into(),
                    ));
                }
                row_bounds.push((0, 0));
                continue;
            }
            if data.iter().any(|value| *value < 0) {
                return Err(MongrelError::InvalidArgument(
                    "sorted run contains a negative row id".into(),
                ));
            }
            let page_first = data[0] as u64;
            let page_last = data[data.len() - 1] as u64;
            for value in data {
                let value = value as u64;
                if previous.is_some_and(|prior| {
                    value < prior || (self.header.is_clean() && value == prior)
                }) {
                    return Err(MongrelError::InvalidArgument(
                        "sorted run row ids are not ordered".into(),
                    ));
                }
                first.get_or_insert(value);
                previous = Some(value);
                last = Some(value);
                counted += 1;
            }
            if stat.first_row_id != page_first || stat.last_row_id != page_last {
                return Err(MongrelError::InvalidArgument(
                    "sorted run row-id page bounds are stale".into(),
                ));
            }
            row_bounds.push((page_first, page_last));
        }
        if counted != self.header.row_count
            || first.unwrap_or(0) != self.header.min_row_id
            || last.unwrap_or(0) != self.header.max_row_id
        {
            return Err(MongrelError::InvalidArgument(
                "sorted run header row bounds differ from its pages".into(),
            ));
        }
        for column in self.dir.clone() {
            if !columns.insert(column.column_id) {
                return Err(MongrelError::InvalidArgument(format!(
                    "sorted run contains duplicate column {}",
                    column.column_id
                )));
            }
            let rows = column
                .page_stats
                .iter()
                .map(|stat| stat.row_count)
                .collect::<Vec<_>>();
            if rows != expected_rows {
                return Err(MongrelError::InvalidArgument(
                    "sorted run columns have inconsistent page row counts".into(),
                ));
            }
            let ty = self.resolve_type(column.column_id);
            if column.type_id_tag != type_tag(&ty)
                || column.encoding > Encoding::Zstd as u8
                || (!is_system_column_id(column.column_id)
                    && self
                        .schema
                        .columns
                        .iter()
                        .all(|item| item.id != column.column_id))
            {
                return Err(MongrelError::InvalidArgument(
                    "sorted run column type or encoding is invalid".into(),
                ));
            }
            for (page, stat) in column.page_stats.iter().enumerate() {
                let bytes = self.read_page(column.column_id, page)?;
                if bytes.len() != stat.uncompressed_len as usize {
                    return Err(MongrelError::InvalidArgument(
                        "sorted run page length differs from its metadata".into(),
                    ));
                }
                let native =
                    match columnar::decode_page_native(ty.clone(), &bytes, stat.row_count as usize)
                    {
                        Ok(native) => native,
                        Err(MongrelError::InvalidArgument(message))
                            if message.starts_with("decode_page_native: unsupported ty") =>
                        {
                            let values =
                                columnar::decode_page(ty.clone(), &bytes, stat.row_count as usize)?;
                            columnar::values_to_native(ty.clone(), &values)
                        }
                        Err(error) => return Err(error),
                    };
                let (first_row_id, last_row_id) =
                    row_bounds.get(page).copied().ok_or_else(|| {
                        MongrelError::InvalidArgument(
                            "sorted run column has more pages than its row-id column".into(),
                        )
                    })?;
                let expected =
                    columnar::page_stat_for(ty.clone(), &native, first_row_id, last_row_id);
                if stat.first_row_id != expected.first_row_id
                    || stat.last_row_id != expected.last_row_id
                    || stat.null_count != expected.null_count
                    || stat.min != expected.min
                    || stat.max != expected.max
                {
                    return Err(MongrelError::InvalidArgument(
                        "sorted run page statistics differ from decoded values".into(),
                    ));
                }
            }
        }
        if !columns.contains(&SYS_ROW_ID)
            || !columns.contains(&SYS_EPOCH)
            || !columns.contains(&SYS_DELETED)
            || expected_rows.into_iter().map(u64::from).sum::<u64>() != self.header.row_count
        {
            return Err(MongrelError::InvalidArgument(
                "sorted run system columns or row count are invalid".into(),
            ));
        }
        if self.header.schema_id == self.schema.schema_id
            && self
                .schema
                .columns
                .iter()
                .any(|column| !columns.contains(&column.id))
        {
            return Err(MongrelError::InvalidArgument(
                "sorted run is missing a column from its declared schema".into(),
            ));
        }

        // Page encodings and statistics are not enough to establish semantic
        // validity.  Reconstruct every materialized row and apply the schema's
        // row-level rules before the run can participate in recovery.
        for row_index in 0..usize::try_from(self.header.row_count).map_err(|_| {
            MongrelError::InvalidArgument("sorted run row count exceeds this platform".into())
        })? {
            let epoch = match self.column(SYS_EPOCH)?.get(row_index) {
                Some(Value::Int64(value)) if *value >= 0 => *value as u64,
                _ => {
                    return Err(MongrelError::InvalidArgument(
                        "sorted run contains an invalid commit epoch".into(),
                    ))
                }
            };
            if epoch > self.header.epoch_created || (self.header.is_uniform_epoch() && epoch != 0) {
                return Err(MongrelError::InvalidArgument(
                    "sorted run commit epoch exceeds its creation epoch".into(),
                ));
            }
            let deleted = match self.column(SYS_DELETED)?.get(row_index) {
                Some(Value::Bool(value)) => *value,
                _ => {
                    return Err(MongrelError::InvalidArgument(
                        "sorted run contains an invalid tombstone marker".into(),
                    ))
                }
            };
            let mut values = Vec::with_capacity(self.schema.columns.len());
            for column in self.schema.columns.clone() {
                let value = if columns.contains(&column.id) {
                    self.column(column.id)?
                        .get(row_index)
                        .cloned()
                        .unwrap_or(Value::Null)
                } else {
                    Value::Null
                };
                values.push((column.id, value));
            }
            if !deleted {
                self.schema
                    .validate_persisted_values(&values)
                    .map_err(|error| {
                        MongrelError::InvalidArgument(format!(
                            "sorted run row violates its schema: {error}"
                        ))
                    })?;
            }
        }
        Ok(())
    }

    /// Overlay the real commit epoch for a uniform-epoch run (see
    /// [`RUN_FLAG_UNIFORM_EPOCH`]). No-op unless the run carries that flag, so it
    /// is always safe for the engine to call with the `RunRef.epoch_created`.
    pub(crate) fn set_uniform_epoch(&mut self, epoch: Epoch) {
        if self.header.is_uniform_epoch() {
            self.epoch_override = Some(epoch);
            // Drop any cached placeholder epoch column so the overlay takes hold.
            self.col_cache.remove(&SYS_EPOCH);
        }
    }

    pub(crate) fn into_visible_version_cursor(
        self,
        snapshot: Epoch,
    ) -> Result<RunVisibleVersionCursor> {
        RunVisibleVersionCursor::new(self, snapshot)
    }

    /// Whether this run is "clean" (one version per RowId, no tombstones,
    /// ascending row_ids) — stamped at write time via [`RUN_FLAG_CLEAN`].
    pub fn is_clean(&self) -> bool {
        self.header.is_clean()
    }

    pub fn row_count(&self) -> usize {
        self.header.row_count as usize
    }

    pub(crate) fn clean_contiguous_row_ids(&self) -> bool {
        let n = self.row_count();
        n > 0
            && self.is_clean()
            && self.epoch_override.is_none()
            && self.header.max_row_id >= self.header.min_row_id
            && self
                .header
                .max_row_id
                .checked_sub(self.header.min_row_id)
                .and_then(|span| span.checked_add(1))
                == Some(n as u64)
    }

    pub(crate) fn position_for_row_id_fast(&self, row_id: u64) -> Option<usize> {
        if !self.clean_contiguous_row_ids()
            || row_id < self.header.min_row_id
            || row_id > self.header.max_row_id
        {
            return None;
        }
        Some((row_id - self.header.min_row_id) as usize)
    }

    pub(crate) fn positions_for_row_ids_fast(&self, row_ids: &[u64]) -> Option<Vec<usize>> {
        if !self.clean_contiguous_row_ids() {
            return None;
        }
        let mut positions = Vec::with_capacity(row_ids.len());
        for &row_id in row_ids {
            if let Some(pos) = self.position_for_row_id_fast(row_id) {
                positions.push(pos);
            }
        }
        positions.sort_unstable();
        Some(positions)
    }

    fn resolve_type(&self, column_id: u16) -> TypeId {
        match column_id {
            SYS_ROW_ID | SYS_EPOCH => TypeId::Int64,
            SYS_DELETED => TypeId::Bool,
            SYS_COMMIT_TS => TypeId::Bytes,
            _ => self
                .schema
                .columns
                .iter()
                .find(|c| c.id == column_id)
                .map(|c| c.ty.clone())
                .unwrap_or(TypeId::Bytes),
        }
    }

    /// Optional HLC stamp at row `index` (P0.5-T3). Missing column → `None`.
    fn commit_ts_at(&mut self, index: usize) -> Result<Option<HlcTimestamp>> {
        if !self.has_column(SYS_COMMIT_TS) {
            return Ok(None);
        }
        Ok(decode_commit_ts_value(
            self.column(SYS_COMMIT_TS)?.get(index),
        ))
    }

    fn find_header(&self, column_id: u16) -> Result<&ColumnPageHeader> {
        self.dir
            .iter()
            .find(|h| h.column_id == column_id)
            .ok_or_else(|| MongrelError::ColumnNotFound(format!("column {column_id}")))
    }

    /// Whether `column_id`'s pages are encrypted. Encrypted runs carry no
    /// cleartext per-page min/max (they would leak plaintext values), so the
    /// range resolvers must NOT prune by stats for such columns — a missing
    /// stat there means "hidden", not "all-null". They fall back to decrypting
    /// and scanning every page.
    fn col_encrypted(&self, column_id: u16) -> bool {
        self.dir
            .iter()
            .find(|h| h.column_id == column_id)
            .map(|h| h.flags & ColumnPageHeader::PAGE_ENCRYPTED != 0)
            .unwrap_or(false)
    }

    pub(crate) fn read_page(&mut self, column_id: u16, page_seq: usize) -> Result<Vec<u8>> {
        let (offset, compressed_len, encrypted) = {
            let ch = self.find_header(column_id)?;
            let stat = ch
                .page_stats
                .get(page_seq)
                .ok_or_else(|| MongrelError::InvalidArgument("page seq out of range".into()))?;
            (
                stat.offset,
                stat.compressed_len,
                ch.flags & ColumnPageHeader::PAGE_ENCRYPTED != 0,
            )
        };
        let end = offset
            .checked_add(compressed_len as u64)
            .ok_or_else(|| MongrelError::InvalidArgument("run page offset overflows".into()))?;
        let start = usize::try_from(offset)
            .map_err(|_| MongrelError::InvalidArgument("run page offset is too large".into()))?;
        let end = usize::try_from(end)
            .map_err(|_| MongrelError::InvalidArgument("run page end is too large".into()))?;
        // Shared cache: serve the raw (on-disk / ciphertext) page bytes if
        // present, so concurrent readers never re-read or re-decrypt a page.
        let key = page_cache_key(self.table_id, self.header.run_id, column_id, page_seq);
        if let Some(cache) = &self.page_cache {
            if let Some(bytes) = cache.lock(&key).get(
                &key,
                crate::epoch::Snapshot::at(crate::epoch::Epoch(u64::MAX)),
            ) {
                return decrypt_or_passthrough(
                    self.cipher.as_deref(),
                    self.nonce_prefix,
                    column_id,
                    page_seq,
                    encrypted,
                    &bytes,
                );
            }
        }
        let buf = match &self.mmap {
            // Slice the mapping — no seek/read syscalls; the OS page cache fills
            // the pages on first touch.
            Some(m) => m
                .get(start..end)
                .ok_or_else(|| {
                    MongrelError::InvalidArgument("run page is outside the mapped file".into())
                })?
                .to_vec(),
            None => {
                self.file.seek(SeekFrom::Start(offset))?;
                let mut buf = vec![0u8; compressed_len as usize];
                self.file.read_exact(&mut buf)?;
                buf
            }
        };
        // Spill the raw bytes into the shared cache (post-read, pre-decrypt).
        if let Some(cache) = &self.page_cache {
            cache.lock(&key).insert(crate::page::CachedPage {
                committed_epoch: crate::epoch::Epoch(self.header.epoch_created),
                content_hash: key,
                bytes: bytes::Bytes::copy_from_slice(&buf),
            });
        }
        decrypt_or_passthrough(
            self.cipher.as_deref(),
            self.nonce_prefix,
            column_id,
            page_seq,
            encrypted,
            &buf,
        )
    }

    /// `&self` version of [`Self::read_page`] restricted to the mmap-backed
    /// path, so page bytes can be read concurrently (rayon) without the
    /// `&mut self` file handle. Decryption (when enabled) uses the `Sync`
    /// cipher and a deterministic per-page nonce, so it is also parallel-safe.
    fn read_page_shared(&self, column_id: u16, page_seq: usize) -> Result<Vec<u8>> {
        let ch = self.find_header(column_id)?;
        let stat = ch
            .page_stats
            .get(page_seq)
            .ok_or_else(|| MongrelError::InvalidArgument("page seq out of range".into()))?;
        let encrypted = ch.flags & ColumnPageHeader::PAGE_ENCRYPTED != 0;
        // Non-blocking probe of the shared cache: never block the rayon pool on
        // a contended lock. On a hit, avoid the mmap slice + decrypt entirely.
        let key = page_cache_key(self.table_id, self.header.run_id, column_id, page_seq);
        if let Some(cache) = &self.page_cache {
            if let Some(guard) = cache.try_lock(&key) {
                if let Some(bytes) = guard.try_get(
                    &key,
                    crate::epoch::Snapshot::at(crate::epoch::Epoch(u64::MAX)),
                ) {
                    return decrypt_or_passthrough(
                        self.cipher.as_deref(),
                        self.nonce_prefix,
                        column_id,
                        page_seq,
                        encrypted,
                        &bytes,
                    );
                }
            }
        }
        let mmap = self.mmap.as_ref().ok_or_else(|| {
            MongrelError::InvalidArgument("parallel page decode requires an mmap-backed run".into())
        })?;
        let end = stat
            .offset
            .checked_add(stat.compressed_len as u64)
            .ok_or_else(|| MongrelError::InvalidArgument("run page offset overflows".into()))?;
        let start = usize::try_from(stat.offset)
            .map_err(|_| MongrelError::InvalidArgument("run page offset is too large".into()))?;
        let end = usize::try_from(end)
            .map_err(|_| MongrelError::InvalidArgument("run page end is too large".into()))?;
        let buf = mmap
            .get(start..end)
            .ok_or_else(|| {
                MongrelError::InvalidArgument("run page is outside the mapped file".into())
            })?
            .to_vec();
        // Opportunistic, non-blocking insert: populate the shared cache so later
        // readers (and encrypted re-reads) skip the mmap slice + decrypt. Never
        // block the rayon pool — if the lock is contended, just skip the insert.
        if let Some(cache) = &self.page_cache {
            if let Some(mut guard) = cache.try_lock(&key) {
                guard.insert(crate::page::CachedPage {
                    committed_epoch: crate::epoch::Epoch(self.header.epoch_created),
                    content_hash: key,
                    bytes: bytes::Bytes::copy_from_slice(&buf),
                });
            }
        }
        decrypt_or_passthrough(
            self.cipher.as_deref(),
            self.nonce_prefix,
            column_id,
            page_seq,
            encrypted,
            &buf,
        )
    }

    /// Decode (and cache) a full column, concatenating all pages.
    fn column(&mut self, column_id: u16) -> Result<&[Value]> {
        // Uniform-epoch overlay: serve the `_epoch` column as a constant of the
        // real commit epoch instead of the placeholder stored on disk.
        if column_id == SYS_EPOCH {
            if let Some(ov) = self.epoch_override {
                if !self.col_cache.contains_key(&column_id) {
                    let n = self.row_count();
                    self.col_cache
                        .insert(column_id, vec![Value::Int64(ov.0 as i64); n]);
                }
                return Ok(self.col_cache.get(&column_id).unwrap().as_slice());
            }
        }
        if !self.col_cache.contains_key(&column_id) {
            let ty = self.resolve_type(column_id);
            let page_rows: Vec<usize> = {
                let ch = self.find_header(column_id)?;
                ch.page_stats.iter().map(|s| s.row_count as usize).collect()
            };
            let mut decoded: Vec<Value> = Vec::with_capacity(self.row_count());
            for (seq, &pr) in page_rows.iter().enumerate() {
                let page = self.read_page(column_id, seq)?;
                decoded.extend(columnar::decode_page(ty.clone(), &page, pr)?);
            }
            self.col_cache.insert(column_id, decoded);
        }
        Ok(self.col_cache.get(&column_id).unwrap().as_slice())
    }

    /// Newest version of `row_id` with `epoch <= snapshot`, including tombstones
    /// (returned as a `Row` with `deleted=true`). `None` if no such version.
    ///
    /// Page-pruned: `SYS_ROW_ID` pages carry exact `first_row_id`/`last_row_id`
    /// bounds (rows are written in ascending `(RowId, Epoch)` order), so this
    /// decodes only the page(s) that can contain `row_id` instead of the whole
    /// column — the old implementation decoded every row's `SYS_ROW_ID` (and
    /// `SYS_EPOCH`) up front, making every single-row point lookup (the common
    /// case for a PK/unique check feeding insert/update/delete) pay a
    /// full-column decode. A row's version group can span at most two adjacent
    /// pages (split at a page boundary mid-group); `candidate_pages` below
    /// collects every page whose bounds include `row_id`, which is normally
    /// one page and two only in that split case.
    pub fn get_version(&mut self, row_id: RowId, snapshot: Epoch) -> Result<Option<(Epoch, Row)>> {
        match self.find_version_page(row_id, snapshot)? {
            None => Ok(None),
            Some((epoch, seq, local_index)) => Ok(Some((
                Epoch(epoch),
                self.materialize_in_page(seq, local_index)?,
            ))),
        }
    }

    /// Page-pruned search for the newest version of `row_id` with `epoch <=
    /// snapshot`: `(epoch, page_seq, local_index)`, or `None` if no such
    /// version exists in this run. Factored out of [`Self::get_version`] so
    /// [`Self::get_version_column`] can reuse the exact same page-finding
    /// logic without re-deriving it.
    fn find_version_page(
        &mut self,
        row_id: RowId,
        snapshot: Epoch,
    ) -> Result<Option<(u64, usize, usize)>> {
        let n = self.row_count();
        if n == 0 {
            return Ok(None);
        }
        let target = row_id.0 as i64;
        let ch = self.find_header(SYS_ROW_ID)?;
        let mut page_start = 0u64;
        let candidate_pages: Vec<(usize, u64)> = ch // (page_seq, row offset of page start)
            .page_stats
            .iter()
            .enumerate()
            .filter_map(|(seq, s)| {
                let start = page_start;
                page_start += s.row_count as u64;
                (s.first_row_id <= row_id.0 && row_id.0 <= s.last_row_id).then_some((seq, start))
            })
            .collect();
        if candidate_pages.is_empty() {
            return Ok(None);
        }
        let ty = self.resolve_type(SYS_ROW_ID);
        let mut best: Option<(u64, usize, usize)> = None; // (epoch, page_seq, local index)
        for (seq, _page_row_start) in candidate_pages {
            let page_rows = self.find_header(SYS_ROW_ID)?.page_stats[seq].row_count as usize;
            let row_ids =
                match self.decode_page_native_cached(ty.clone(), SYS_ROW_ID, seq, page_rows)? {
                    columnar::NativeColumn::Int64 { data, .. } => data,
                    _ => return Err(MongrelError::InvalidArgument("sys row_id not int64".into())),
                };
            let local = match row_ids.binary_search(&target) {
                Ok(i) => i,
                Err(_) => continue,
            };
            let epoch_ty = self.resolve_type(SYS_EPOCH);
            let epochs =
                match self.decode_page_native_cached(epoch_ty, SYS_EPOCH, seq, page_rows)? {
                    columnar::NativeColumn::Int64 { data, .. } => data,
                    _ => return Err(MongrelError::InvalidArgument("sys epoch not int64".into())),
                };
            // `local` is one match; the row-id's full version group is the
            // contiguous run of equal row-ids around it within this page.
            let mut lo = local;
            while lo > 0 && row_ids[lo - 1] == target {
                lo -= 1;
            }
            let mut hi = local;
            while hi + 1 < row_ids.len() && row_ids[hi + 1] == target {
                hi += 1;
            }
            for (i, &epoch) in epochs[lo..=hi].iter().enumerate() {
                let epoch = epoch as u64;
                if epoch <= snapshot.0 && best.map(|(be, ..)| epoch > be).unwrap_or(true) {
                    best = Some((epoch, seq, lo + i));
                }
            }
        }
        Ok(best)
    }

    /// Like [`Self::get_version`], but decodes only `column_id` (plus the
    /// `SYS_DELETED` flag) instead of materializing every schema column via
    /// [`Self::materialize_in_page`]. For a wide schema this avoids paying to
    /// decode every other column's page just to read one value and throw the
    /// rest away — e.g. `Table::remove_hot_for_row`'s PK-only lookup, which
    /// used to pull the whole row (every column, every page) just for the
    /// primary key.
    pub fn get_version_column(
        &mut self,
        row_id: RowId,
        snapshot: Epoch,
        column_id: u16,
    ) -> Result<Option<(Epoch, bool, Option<Value>)>> {
        let Some((epoch, seq, local_index)) = self.find_version_page(row_id, snapshot)? else {
            return Ok(None);
        };
        let page_rows = self.find_header(SYS_ROW_ID)?.page_stats[seq].row_count as usize;
        let page_start: usize = self.find_header(SYS_ROW_ID)?.page_stats[..seq]
            .iter()
            .map(|s| s.row_count as usize)
            .sum();
        let global_index = page_start + local_index;
        let native_at = |slf: &mut Self, cid: u16| -> Result<Option<Value>> {
            if !slf.dir.iter().any(|h| h.column_id == cid) {
                return Ok(None);
            }
            let ty = slf.resolve_type(cid);
            if !matches!(
                ty,
                TypeId::Bool
                    | TypeId::Int8
                    | TypeId::Int16
                    | TypeId::Int32
                    | TypeId::Int64
                    | TypeId::UInt8
                    | TypeId::UInt16
                    | TypeId::UInt32
                    | TypeId::UInt64
                    | TypeId::Float32
                    | TypeId::Float64
                    | TypeId::TimestampNanos
                    | TypeId::Date32
                    | TypeId::Bytes
            ) {
                return Ok(slf.column(cid)?.get(global_index).cloned());
            }
            Ok(slf
                .decode_page_native_cached(ty, cid, seq, page_rows)?
                .value_at(local_index))
        };
        let deleted = matches!(native_at(self, SYS_DELETED)?, Some(Value::Bool(true)));
        let value = native_at(self, column_id)?;
        Ok(Some((Epoch(epoch), deleted, value)))
    }

    /// Newest version epoch and tombstone flag without decoding a user column.
    pub fn get_version_visibility(
        &mut self,
        row_id: RowId,
        snapshot: Epoch,
    ) -> Result<Option<(Epoch, bool)>> {
        let Some((epoch, seq, local_index)) = self.find_version_page(row_id, snapshot)? else {
            return Ok(None);
        };
        let page_rows = self.find_header(SYS_ROW_ID)?.page_stats[seq].row_count as usize;
        let deleted = match self.decode_page_native_cached(
            self.resolve_type(SYS_DELETED),
            SYS_DELETED,
            seq,
            page_rows,
        )? {
            columnar::NativeColumn::Bool { data, .. } => data[local_index] != 0,
            _ => return Err(MongrelError::InvalidArgument("sys deleted not bool".into())),
        };
        Ok(Some((Epoch(epoch), deleted)))
    }

    /// Build a `Row` from page `seq`'s data at `local_index`, decoding only
    /// that one page per column instead of [`Self::materialize`]'s whole-column
    /// `Vec<Value>` decode — used by [`Self::get_version`], which already knows
    /// the exact page from its page-pruned search. Only for scalar types with a
    /// [`columnar::NativeColumn`] representation; any other column (e.g. a
    /// fixed-size `Embedding`, which `NativeColumn` has no variant for) falls
    /// back to the whole-column path for that column specifically, so this is
    /// never wrong, only sometimes not faster.
    fn materialize_in_page(&mut self, seq: usize, local_index: usize) -> Result<Row> {
        let page_rows = self.find_header(SYS_ROW_ID)?.page_stats[seq].row_count as usize;
        let page_start: usize = self.find_header(SYS_ROW_ID)?.page_stats[..seq]
            .iter()
            .map(|s| s.row_count as usize)
            .sum();
        let global_index = page_start + local_index;
        let native_at = |slf: &mut Self, column_id: u16| -> Result<Option<Value>> {
            // Absent column (schema evolution: added after this run was
            // written) reads as null, matching `materialize`'s own guard.
            if !slf.dir.iter().any(|h| h.column_id == column_id) {
                return Ok(None);
            }
            let ty = slf.resolve_type(column_id);
            if !matches!(
                ty,
                TypeId::Bool
                    | TypeId::Int8
                    | TypeId::Int16
                    | TypeId::Int32
                    | TypeId::Int64
                    | TypeId::UInt8
                    | TypeId::UInt16
                    | TypeId::UInt32
                    | TypeId::UInt64
                    | TypeId::Float32
                    | TypeId::Float64
                    | TypeId::TimestampNanos
                    | TypeId::Date32
                    | TypeId::Bytes
            ) {
                // Not a NativeColumn-representable scalar (e.g. Embedding) —
                // fall back to the always-correct whole-column decode.
                return Ok(slf.column(column_id)?.get(global_index).cloned());
            }
            Ok(slf
                .decode_page_native_cached(ty, column_id, seq, page_rows)?
                .value_at(local_index))
        };
        let row_id = RowId(match native_at(self, SYS_ROW_ID)? {
            Some(Value::Int64(x)) => x as u64,
            _ => 0,
        });
        let committed_epoch = Epoch(match native_at(self, SYS_EPOCH)? {
            Some(Value::Int64(x)) => x as u64,
            _ => 0,
        });
        let deleted = matches!(native_at(self, SYS_DELETED)?, Some(Value::Bool(true)));
        let commit_ts = if self.has_column(SYS_COMMIT_TS) {
            decode_commit_ts_value(native_at(self, SYS_COMMIT_TS)?.as_ref())
        } else {
            None
        };
        let col_ids: Vec<u16> = self.schema.columns.iter().map(|c| c.id).collect();
        let mut columns = HashMap::new();
        for id in col_ids {
            columns.insert(id, native_at(self, id)?.unwrap_or(Value::Null));
        }
        Ok(Row {
            row_id,
            committed_epoch,
            columns,
            deleted,
            commit_ts,
        })
    }

    /// Every row in the run (all versions), in `(RowId, Epoch)` order. Used by
    /// compaction, which must see every version to apply snapshot retention.
    pub fn all_rows(&mut self) -> Result<Vec<Row>> {
        let n = self.row_count();
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(self.materialize(i)?);
        }
        Ok(out)
    }

    /// Every version with cooperative cancellation and a hard row bound.
    pub fn all_rows_controlled(
        &mut self,
        control: &crate::ExecutionControl,
        max_rows: usize,
    ) -> Result<Vec<Row>> {
        let n = self.row_count();
        if n > max_rows {
            return Err(MongrelError::ResourceLimitExceeded {
                resource: "controlled run row materialization",
                requested: n,
                limit: max_rows,
            });
        }
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            if i % 256 == 0 {
                control.checkpoint()?;
            }
            out.push(self.materialize(i)?);
        }
        control.checkpoint()?;
        Ok(out)
    }

    /// Indices of the newest non-deleted version per `RowId` visible at
    /// `snapshot`, ascending. This is the columnar scan primitive: compute the
    /// visible set once (one pass over the row-id/epoch/deleted columns), then
    /// [`Self::gather_column`] each user column at these indices — no per-row
    /// `HashMap`/`Row` materialization.
    pub fn visible_indices(&mut self, snapshot: Epoch) -> Result<Vec<usize>> {
        let n = self.row_count();
        if n == 0 {
            return Ok(Vec::new());
        }
        let row_ids = self.column(SYS_ROW_ID)?.to_vec();
        let epochs = self.column(SYS_EPOCH)?.to_vec();
        let deleted = self.column(SYS_DELETED)?.to_vec();
        let mut best: HashMap<u64, (u64, usize)> = HashMap::new();
        for i in 0..n {
            let rid = int_at(&row_ids, i);
            let e = int_at(&epochs, i);
            if e > snapshot.0 {
                continue;
            }
            best.entry(rid)
                .and_modify(|(be, bi)| {
                    if e > *be {
                        *be = e;
                        *bi = i;
                    }
                })
                .or_insert((e, i));
        }
        let mut idxs: Vec<usize> = best.into_values().map(|(_, i)| i).collect();
        idxs.retain(|&i| !bool_at(&deleted, i));
        idxs.sort_unstable();
        Ok(idxs)
    }

    /// Gather `column_id`'s values at the given indices (column cached). Used
    /// with [`Self::visible_indices`] for vectorized scans. A column absent from
    /// this run (e.g. added via schema evolution after the run was written)
    /// yields all-nulls.
    pub fn gather_column(&mut self, column_id: u16, indices: &[usize]) -> Result<Vec<Value>> {
        if !self.dir.iter().any(|h| h.column_id == column_id) {
            return Ok(vec![Value::Null; indices.len()]);
        }
        let col = self.column(column_id)?;
        Ok(indices
            .iter()
            .map(|&i| col.get(i).cloned().unwrap_or(Value::Null))
            .collect())
    }

    /// Decode a column straight to a typed [`NativeColumn`] (no `Value`),
    /// concatenating all pages. A column absent from this run (schema evolution)
    /// yields an all-null column. Pages are decoded in parallel (rayon) when the
    /// run is mmap-backed and has more than one page; otherwise sequentially.
    pub fn column_native(&mut self, column_id: u16) -> Result<columnar::NativeColumn> {
        use rayon::prelude::*;
        if column_id == SYS_EPOCH {
            if let Some(ov) = self.epoch_override {
                return Ok(columnar::NativeColumn::int64_constant(
                    ov.0 as i64,
                    self.row_count(),
                ));
            }
        }
        let ty = self.resolve_type(column_id);
        let n = self.row_count();
        let Some(ch) = self.dir.iter().find(|h| h.column_id == column_id) else {
            return Ok(columnar::null_native(ty, n));
        };
        let page_count = ch.page_count as usize;
        let page_rows: Vec<usize> = ch.page_stats.iter().map(|s| s.row_count as usize).collect();
        if page_count == 0 {
            return Ok(columnar::null_native(ty, n));
        }
        let parts: Vec<columnar::NativeColumn> = if self.mmap.is_some() && page_count > 1 {
            // Parallel decode: each page is an independent region of the shared
            // mmap; the cipher (if any) is Sync. Spread the (CPU-bound) decode
            // across the rayon thread pool.
            let reader: &RunReader = self;
            (0..page_count)
                .into_par_iter()
                .map(|seq| {
                    let raw = reader.read_page_shared(column_id, seq)?;
                    columnar::decode_page_native(ty.clone(), &raw, page_rows[seq])
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            let mut out = Vec::with_capacity(page_count);
            for (seq, &pr) in page_rows.iter().enumerate() {
                let page = self.read_page(column_id, seq)?;
                out.push(columnar::decode_page_native(ty.clone(), &page, pr)?);
            }
            out
        };
        Ok(columnar::NativeColumn::concat(&parts))
    }

    /// Whether this reader is backed by a memory map (the prerequisite for the
    /// `&self` parallel decode paths). Readers on filesystems that reject mmap
    /// fall back to per-page `read()` and `has_mmap()` is false.
    pub fn has_mmap(&self) -> bool {
        self.mmap.is_some()
    }

    /// `&self` variant of [`column_native`] for cross-column parallel scans
    /// (Phase 15.1). Requires the mmap backing (uses [`read_page_shared`], which
    /// is rayon-safe); callers without mmap use the `&mut` [`column_native`].
    /// Pages within the column decode in parallel when there is more than one,
    /// and `MADV_WILLNEED` is hinted up front so the kernel pre-faults the whole
    /// column's byte range (Phase 15.2) before the decode workers touch it.
    pub fn column_native_shared(&self, column_id: u16) -> Result<columnar::NativeColumn> {
        use rayon::prelude::*;
        if column_id == SYS_EPOCH {
            if let Some(ov) = self.epoch_override {
                return Ok(columnar::NativeColumn::int64_constant(
                    ov.0 as i64,
                    self.row_count(),
                ));
            }
        }
        let ty = self.resolve_type(column_id);
        let n = self.row_count();
        let Some(ch) = self.dir.iter().find(|h| h.column_id == column_id) else {
            return Ok(columnar::null_native(ty, n));
        };
        let page_count = ch.page_count as usize;
        let page_rows: Vec<usize> = ch.page_stats.iter().map(|s| s.row_count as usize).collect();
        if page_count == 0 {
            return Ok(columnar::null_native(ty, n));
        }
        // Phase 15.2: best-effort read-ahead. Tell the kernel to page-in the
        // column's full byte range before the workers fan out, overlapping the
        // disk I/O with the upcoming decode CPU.
        #[cfg(unix)]
        {
            if let (Some(m), Some(first)) = (&self.mmap, ch.page_stats.first()) {
                let start = first.offset as usize;
                let end = (ch.page_region_offset as usize) + (ch.page_region_len as usize);
                if end > start {
                    let _ = m.advise_range(memmap2::Advice::WillNeed, start, end - start);
                }
            }
        }
        let run_id = self.header.run_id;
        // Decode in parallel (cache probes use `try_lock` → no worker blocking).
        // Each item is the decoded page plus its key when it was a cache miss
        // (hits return `None` for the key so we don't re-insert/clone them).
        let mut parts_keys: Vec<(columnar::NativeColumn, Option<[u8; 32]>)> = if page_count > 1 {
            (0..page_count)
                .into_par_iter()
                .map(|seq| {
                    self.decode_page_cached(ty.clone(), column_id, seq, page_rows[seq], run_id)
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            vec![self.decode_page_cached(ty, column_id, 0, page_rows[0], run_id)?]
        };
        // Sequentially cache the freshly-decoded pages — no parallel contention
        // on the insert, so every miss is reliably stored for the next scan.
        if let Some(cache) = &self.decoded_cache {
            for (col, key) in parts_keys.iter_mut() {
                if let Some(k) = key.take() {
                    cache.lock(&k).insert(k, Arc::new(col.clone()));
                }
            }
        }
        let parts: Vec<columnar::NativeColumn> = parts_keys.into_iter().map(|(c, _)| c).collect();
        Ok(columnar::NativeColumn::concat(&parts))
    }

    /// Decode one page for the shared scan path, consulting the decoded-page
    /// cache first (Phase 15.4). Returns the decoded page plus `Some(key)` on a
    /// cache miss (so the caller can insert it) or `None` on a hit (already
    /// cached). Cache probes use `try_lock` so rayon workers never block.
    fn decode_page_cached(
        &self,
        ty: TypeId,
        column_id: u16,
        seq: usize,
        nrows: usize,
        run_id: u128,
    ) -> Result<(columnar::NativeColumn, Option<[u8; 32]>)> {
        let key = page_cache_key(self.table_id, run_id, column_id, seq);
        if let Some(cache) = &self.decoded_cache {
            if let Some(g) = cache.try_lock(&key) {
                if let Some(hit) = g.try_get(&key) {
                    return Ok(((*hit).clone(), None));
                }
            }
        }
        let raw = self.read_page_shared(column_id, seq)?;
        let col = columnar::decode_page_native(ty, &raw, nrows)?;
        Ok((col, Some(key)))
    }

    /// [`Self::decode_page_cached`], but for the sequential point-lookup paths
    /// (`find_version_page`, `materialize_in_page`, `get_version_column`) via
    /// [`Self::read_page`] (handles the non-mmap-backed fallback) instead of
    /// [`Self::read_page_shared`] (mmap-only, for the parallel rayon scan
    /// path). No rayon pool to avoid blocking here, so a plain `.lock()` is
    /// fine — unlike the scan path's `try_lock`, this always consults the
    /// cache rather than skipping it under contention. Without this, every
    /// point lookup re-decompressed its page from scratch even when a prior
    /// lookup had just decoded the exact same page (the dominant remaining
    /// cost measured for `remove_hot_for_row`'s on-disk path).
    fn decode_page_native_cached(
        &mut self,
        ty: TypeId,
        column_id: u16,
        seq: usize,
        nrows: usize,
    ) -> Result<columnar::NativeColumn> {
        let key = page_cache_key(self.table_id, self.header.run_id, column_id, seq);
        if let Some(cache) = &self.decoded_cache {
            if let Some(hit) = cache.lock(&key).try_get(&key) {
                return Ok((*hit).clone());
            }
        }
        let raw = self.read_page(column_id, seq)?;
        let col = columnar::decode_page_native(ty, &raw, nrows)?;
        if let Some(cache) = &self.decoded_cache {
            cache
                .lock(&key)
                .insert(key, std::sync::Arc::new(col.clone()));
        }
        Ok(col)
    }

    /// Row ids whose Int64 value is in `[lo, hi]`, **skipping pages whose
    /// `[min,max]` stat excludes the range** (Parquet-style page-index pruning).
    /// Nulls are excluded. Used by `Table::query_columns_native` to serve
    /// `Condition::Range` without decoding every page.
    pub fn range_row_ids_i64(
        &mut self,
        column_id: u16,
        lo: i64,
        hi: i64,
    ) -> Result<std::collections::HashSet<u64>> {
        Ok(self
            .range_row_id_set_i64(column_id, lo, hi)?
            .into_sorted_vec()
            .into_iter()
            .collect())
    }

    pub(crate) fn range_row_id_set_i64(
        &mut self,
        column_id: u16,
        lo: i64,
        hi: i64,
    ) -> Result<RowIdSet> {
        let info: Vec<(Option<i64>, Option<i64>, usize)> =
            match self.dir.iter().find(|h| h.column_id == column_id) {
                Some(ch) => ch
                    .page_stats
                    .iter()
                    .map(|s| {
                        (
                            be_i64(s.min.as_deref()),
                            be_i64(s.max.as_deref()),
                            s.row_count as usize,
                        )
                    })
                    .collect(),
                None => return Ok(RowIdSet::empty()),
            };
        // Encrypted columns are pruneable only when this run carries the
        // decrypted stats envelope (overlaid at open). Without one (a run
        // whose writer recorded no stats at all), a missing min/max means
        // "unknown" — never prune — whereas with the envelope (and always for
        // plaintext runs) a missing min/max means an all-null page.
        let stats_pruneable =
            !self.col_encrypted(column_id) || self.header.encrypted_stats_offset != 0;
        let clean_contiguous = self.clean_contiguous_row_ids();
        let mut out = Vec::new();
        let mut page_start = 0usize;
        for (seq, (mn, mx, nrows)) in info.into_iter().enumerate() {
            let current_page_start = page_start;
            page_start += nrows;
            // Skip pages that cannot contain a match (or are all-null).
            let skip = stats_pruneable
                && match (mn, mx) {
                    (Some(mn), Some(mx)) => mx < lo || mn > hi,
                    _ => true,
                };
            if skip {
                continue;
            }
            let val_page = self.read_page(column_id, seq)?;
            let vals =
                columnar::decode_page_native(self.resolve_type(column_id), &val_page, nrows)?;
            if let columnar::NativeColumn::Int64 { data: v, validity } = vals {
                if clean_contiguous {
                    for (i, val) in v.iter().enumerate() {
                        if columnar::validity_bit(&validity, i) && *val >= lo && *val <= hi {
                            out.push(self.header.min_row_id + current_page_start as u64 + i as u64);
                        }
                    }
                } else {
                    let rid_page = self.read_page(SYS_ROW_ID, seq)?;
                    let rids = columnar::decode_page_native(TypeId::Int64, &rid_page, nrows)?;
                    if let columnar::NativeColumn::Int64 { data: r, .. } = rids {
                        for (i, val) in v.iter().enumerate() {
                            if columnar::validity_bit(&validity, i) && *val >= lo && *val <= hi {
                                out.push(r[i] as u64);
                            }
                        }
                    }
                }
            }
        }
        Ok(RowIdSet::from_unsorted(out))
    }

    /// Float64 analogue of [`Self::range_row_ids_i64`] with per-bound
    /// inclusivity, for `Condition::RangeF64`.
    pub fn range_row_ids_f64(
        &mut self,
        column_id: u16,
        lo: f64,
        lo_inclusive: bool,
        hi: f64,
        hi_inclusive: bool,
    ) -> Result<std::collections::HashSet<u64>> {
        Ok(self
            .range_row_id_set_f64(column_id, lo, lo_inclusive, hi, hi_inclusive)?
            .into_sorted_vec()
            .into_iter()
            .collect())
    }

    pub(crate) fn range_row_id_set_f64(
        &mut self,
        column_id: u16,
        lo: f64,
        lo_inclusive: bool,
        hi: f64,
        hi_inclusive: bool,
    ) -> Result<RowIdSet> {
        let info: Vec<(Option<f64>, Option<f64>, usize)> =
            match self.dir.iter().find(|h| h.column_id == column_id) {
                Some(ch) => ch
                    .page_stats
                    .iter()
                    .map(|s| {
                        (
                            be_f64(s.min.as_deref()),
                            be_f64(s.max.as_deref()),
                            s.row_count as usize,
                        )
                    })
                    .collect(),
                None => return Ok(RowIdSet::empty()),
            };
        // Encrypted columns are pruneable only when this run carries the
        // decrypted stats envelope (overlaid at open). Without one (a run
        // whose writer recorded no stats at all), a missing min/max means
        // "unknown" — never prune — whereas with the envelope (and always for
        // plaintext runs) a missing min/max means an all-null page.
        let stats_pruneable =
            !self.col_encrypted(column_id) || self.header.encrypted_stats_offset != 0;
        let clean_contiguous = self.clean_contiguous_row_ids();
        let mut out = Vec::new();
        let mut page_start = 0usize;
        for (seq, (mn, mx, nrows)) in info.into_iter().enumerate() {
            let current_page_start = page_start;
            page_start += nrows;
            // A page can be dropped iff every value fails the predicate, i.e. the
            // largest fails the lo-test or the smallest fails the hi-test.
            let skip = stats_pruneable
                && match (mn, mx) {
                    (Some(mn), Some(mx)) => {
                        let skip_lo = mx < lo || (!lo_inclusive && mx == lo);
                        let skip_hi = mn > hi || (!hi_inclusive && mn == hi);
                        skip_lo || skip_hi
                    }
                    _ => true,
                };
            if skip {
                continue;
            }
            let val_page = self.read_page(column_id, seq)?;
            let vals = columnar::decode_page_native(TypeId::Float64, &val_page, nrows)?;
            if let columnar::NativeColumn::Float64 { data: v, validity } = vals {
                if clean_contiguous {
                    for (i, val) in v.iter().enumerate() {
                        if !columnar::validity_bit(&validity, i) || val.is_nan() {
                            continue;
                        }
                        let ok_lo = if lo_inclusive { *val >= lo } else { *val > lo };
                        let ok_hi = if hi_inclusive { *val <= hi } else { *val < hi };
                        if ok_lo && ok_hi {
                            out.push(self.header.min_row_id + current_page_start as u64 + i as u64);
                        }
                    }
                } else {
                    let rid_page = self.read_page(SYS_ROW_ID, seq)?;
                    let rids = columnar::decode_page_native(TypeId::Int64, &rid_page, nrows)?;
                    if let columnar::NativeColumn::Int64 { data: r, .. } = rids {
                        for (i, val) in v.iter().enumerate() {
                            if !columnar::validity_bit(&validity, i) || val.is_nan() {
                                continue;
                            }
                            let ok_lo = if lo_inclusive { *val >= lo } else { *val > lo };
                            let ok_hi = if hi_inclusive { *val <= hi } else { *val < hi };
                            if ok_lo && ok_hi {
                                out.push(r[i] as u64);
                            }
                        }
                    }
                }
            }
        }
        Ok(RowIdSet::from_unsorted(out))
    }

    /// Page-pruned row-id set for `IS NULL` / `IS NOT NULL` on `column_id`.
    /// Skips pages whose `null_count` makes a match impossible (no nulls for
    /// `IS NULL`, all-nulls for `IS NOT NULL`), then decodes the validity bitmap
    /// of surviving pages to pinpoint matching rows.
    pub(crate) fn null_row_id_set(&mut self, column_id: u16, want_nulls: bool) -> Result<RowIdSet> {
        let stats: Vec<(usize, usize)> = match self.dir.iter().find(|h| h.column_id == column_id) {
            Some(ch) => ch
                .page_stats
                .iter()
                .map(|s| (s.null_count as usize, s.row_count as usize))
                .collect(),
            None => return Ok(RowIdSet::empty()),
        };
        let ty = self.resolve_type(column_id);
        let clean_contiguous = self.clean_contiguous_row_ids();
        let mut out = Vec::new();
        let mut page_start = 0usize;
        for (seq, (null_count, nrows)) in stats.into_iter().enumerate() {
            let current_page_start = page_start;
            page_start += nrows;
            // Skip pages that cannot match.
            if want_nulls && null_count == 0 {
                continue;
            }
            if !want_nulls && null_count == nrows {
                continue;
            }
            let val_page = self.read_page(column_id, seq)?;
            let col = columnar::decode_page_native(ty.clone(), &val_page, nrows)?;
            let validity = col.validity();
            if clean_contiguous {
                for i in 0..nrows {
                    let is_null = !columnar::validity_bit(validity, i);
                    if is_null == want_nulls {
                        out.push(self.header.min_row_id + current_page_start as u64 + i as u64);
                    }
                }
            } else {
                let rid_page = self.read_page(SYS_ROW_ID, seq)?;
                let rids = columnar::decode_page_native(TypeId::Int64, &rid_page, nrows)?;
                if let columnar::NativeColumn::Int64 { data: r, .. } = rids {
                    for (i, &rid) in r.iter().enumerate().take(nrows) {
                        let is_null = !columnar::validity_bit(validity, i);
                        if is_null == want_nulls {
                            out.push(rid as u64);
                        }
                    }
                }
            }
        }
        Ok(RowIdSet::from_unsorted(out))
    }

    pub fn visible_indices_native(&mut self, snapshot: Epoch) -> Result<Vec<usize>> {
        let n = self.row_count();
        if n == 0 {
            return Ok(Vec::new());
        }
        let (row_ids, epochs, deleted) = self.system_columns_native()?;
        let mut best: HashMap<u64, (u64, usize)> = HashMap::new();
        for i in 0..n {
            let rid = row_ids[i] as u64;
            let e = epochs[i] as u64;
            if e > snapshot.0 {
                continue;
            }
            best.entry(rid)
                .and_modify(|(be, bi)| {
                    if e > *be {
                        *be = e;
                        *bi = i;
                    }
                })
                .or_insert((e, i));
        }
        let mut idxs: Vec<usize> = best.into_values().map(|(_, i)| i).collect();
        idxs.retain(|&i| deleted[i] == 0);
        idxs.sort_unstable();
        Ok(idxs)
    }

    /// Page-pruned, **MVCC-visible** Int64 range resolution (Phase 16.3).
    ///
    /// Like [`Self::range_row_ids_i64`] (skips pages whose `[min,max]` excludes
    /// `[lo, hi]`) but restricts the output to the newest non-deleted version per
    /// `RowId` visible at `snapshot`. This is the layout-independent range
    /// primitive: correct under any memtable / multi-run / deletion-vector state,
    /// so the engine no longer has to fall back to a full-column decode when the
    /// "single clean run" invariant doesn't hold. Nulls are excluded.
    pub fn range_row_ids_visible_i64(
        &mut self,
        column_id: u16,
        lo: i64,
        hi: i64,
        snapshot: Epoch,
    ) -> Result<Vec<u64>> {
        let stats: Vec<(Option<i64>, Option<i64>, usize)> = match self.column_page_stats(column_id)
        {
            Some(s) => s
                .iter()
                .map(|st| {
                    (
                        be_i64(st.min.as_deref()),
                        be_i64(st.max.as_deref()),
                        st.row_count as usize,
                    )
                })
                .collect(),
            None => return Ok(Vec::new()),
        };
        // Encrypted columns are pruneable only when this run carries the
        // decrypted stats envelope (overlaid at open). Without one (a run
        // whose writer recorded no stats at all), a missing min/max means
        // "unknown" — never prune — whereas with the envelope (and always for
        // plaintext runs) a missing min/max means an all-null page.
        let stats_pruneable =
            !self.col_encrypted(column_id) || self.header.encrypted_stats_offset != 0;
        let (positions, rids) = self.visible_positions_with_rids(snapshot)?;
        let mut out: Vec<u64> = Vec::new();
        let mut vis = 0usize;
        let mut page_start = 0usize;
        for (seq, &(mn, mx, nrows)) in stats.iter().enumerate() {
            let page_end = page_start + nrows;
            // A page can be dropped iff every value fails the predicate.
            let skip = stats_pruneable
                && match (mn, mx) {
                    (Some(mn), Some(mx)) => mx < lo || mn > hi,
                    _ => true, // all-null / no stats → nulls never match
                };
            if !skip {
                let val_page = self.read_page(column_id, seq)?;
                let vals = columnar::decode_page_native(TypeId::Int64, &val_page, nrows)?;
                if let columnar::NativeColumn::Int64 { data: v, validity } = vals {
                    while vis < positions.len() && positions[vis] < page_end {
                        let local = positions[vis] - page_start;
                        if columnar::validity_bit(&validity, local)
                            && v[local] >= lo
                            && v[local] <= hi
                        {
                            out.push(rids[vis] as u64);
                        }
                        vis += 1;
                    }
                }
            } else {
                while vis < positions.len() && positions[vis] < page_end {
                    vis += 1;
                }
            }
            page_start = page_end;
        }
        Ok(out)
    }

    /// Float64 analogue of [`Self::range_row_ids_visible_i64`] with per-bound
    /// inclusivity (Phase 16.3).
    pub fn range_row_ids_visible_f64(
        &mut self,
        column_id: u16,
        lo: f64,
        lo_inclusive: bool,
        hi: f64,
        hi_inclusive: bool,
        snapshot: Epoch,
    ) -> Result<Vec<u64>> {
        let stats: Vec<(Option<f64>, Option<f64>, usize)> = match self.column_page_stats(column_id)
        {
            Some(s) => s
                .iter()
                .map(|st| {
                    (
                        be_f64(st.min.as_deref()),
                        be_f64(st.max.as_deref()),
                        st.row_count as usize,
                    )
                })
                .collect(),
            None => return Ok(Vec::new()),
        };
        // Encrypted columns are pruneable only when this run carries the
        // decrypted stats envelope (overlaid at open). Without one (a run
        // whose writer recorded no stats at all), a missing min/max means
        // "unknown" — never prune — whereas with the envelope (and always for
        // plaintext runs) a missing min/max means an all-null page.
        let stats_pruneable =
            !self.col_encrypted(column_id) || self.header.encrypted_stats_offset != 0;
        let (positions, rids) = self.visible_positions_with_rids(snapshot)?;
        let mut out: Vec<u64> = Vec::new();
        let mut vis = 0usize;
        let mut page_start = 0usize;
        for (seq, &(mn, mx, nrows)) in stats.iter().enumerate() {
            let page_end = page_start + nrows;
            let skip = stats_pruneable
                && match (mn, mx) {
                    (Some(mn), Some(mx)) => {
                        let skip_lo = mx < lo || (!lo_inclusive && mx == lo);
                        let skip_hi = mn > hi || (!hi_inclusive && mn == hi);
                        skip_lo || skip_hi
                    }
                    _ => true,
                };
            if !skip {
                let val_page = self.read_page(column_id, seq)?;
                let vals = columnar::decode_page_native(TypeId::Float64, &val_page, nrows)?;
                if let columnar::NativeColumn::Float64 { data: v, validity } = vals {
                    while vis < positions.len() && positions[vis] < page_end {
                        let local = positions[vis] - page_start;
                        if columnar::validity_bit(&validity, local) && !v[local].is_nan() {
                            let val = v[local];
                            let ok_lo = if lo_inclusive { val >= lo } else { val > lo };
                            let ok_hi = if hi_inclusive { val <= hi } else { val < hi };
                            if ok_lo && ok_hi {
                                out.push(rids[vis] as u64);
                            }
                        }
                        vis += 1;
                    }
                }
            } else {
                while vis < positions.len() && positions[vis] < page_end {
                    vis += 1;
                }
            }
            page_start = page_end;
        }
        Ok(out)
    }

    /// MVCC-visible `IS NULL` / `IS NOT NULL` resolution. Follows the same
    /// page-stat-pruned + visible-positions pattern as
    /// [`Self::range_row_ids_visible_i64`], but checks the validity bitmap
    /// instead of a value range. Pages with no nulls (for IS NULL) or all-nulls
    /// (for IS NOT NULL) are skipped.
    pub fn null_row_ids_visible(
        &mut self,
        column_id: u16,
        want_nulls: bool,
        snapshot: Epoch,
    ) -> Result<Vec<u64>> {
        let stats: Vec<(usize, usize)> = match self.column_page_stats(column_id) {
            Some(s) => s
                .iter()
                .map(|st| (st.null_count as usize, st.row_count as usize))
                .collect(),
            None => return Ok(Vec::new()),
        };
        let ty = self.resolve_type(column_id);
        let (positions, rids) = self.visible_positions_with_rids(snapshot)?;
        let mut out: Vec<u64> = Vec::new();
        let mut vis = 0usize;
        let mut page_start = 0usize;
        for (seq, &(null_count, nrows)) in stats.iter().enumerate() {
            let page_end = page_start + nrows;
            let skip = (want_nulls && null_count == 0) || (!want_nulls && null_count == nrows);
            if !skip {
                let val_page = self.read_page(column_id, seq)?;
                let col = columnar::decode_page_native(ty.clone(), &val_page, nrows)?;
                let validity = col.validity();
                while vis < positions.len() && positions[vis] < page_end {
                    let local = positions[vis] - page_start;
                    let is_null = !columnar::validity_bit(validity, local);
                    if is_null == want_nulls {
                        out.push(rids[vis] as u64);
                    }
                    vis += 1;
                }
            } else {
                while vis < positions.len() && positions[vis] < page_end {
                    vis += 1;
                }
            }
            page_start = page_end;
        }
        Ok(out)
    }

    /// tombstones excluded) paired with each position's `RowId`, in one pass.
    /// Used by [`crate::cursor::NativePageCursor`] to map survivors to pages
    /// without re-decoding the system columns.
    pub fn visible_positions_with_rids(
        &mut self,
        snapshot: Epoch,
    ) -> Result<(Vec<usize>, Vec<i64>)> {
        let n = self.row_count();
        if n == 0 {
            return Ok((Vec::new(), Vec::new()));
        }
        // Clean-run fast path (Phase 16.3c): one version per RowId, no
        // tombstones, ascending row_ids ⟹ every position is the newest (and
        // only) visible version. Skip decoding epoch/deleted and the group-
        // collapse loop; just decode row_ids (needed for survivor↔position
        // mapping) and return identity positions [0..n).
        // A uniform-epoch overlay must still gate by snapshot, so skip the clean
        // fast path when an override is active (defensive: spill runs are never
        // written clean).
        if self.is_clean()
            && self.epoch_override.is_none()
            && self.header.epoch_created <= snapshot.0
        {
            let row_ids = match self.column_native_shared(SYS_ROW_ID)? {
                columnar::NativeColumn::Int64 { data, .. } => data,
                _ => return Err(MongrelError::InvalidArgument("sys row_id not int64".into())),
            };
            let positions: Vec<usize> = (0..n).collect();
            return Ok((positions, row_ids));
        }
        let (row_ids, epochs, deleted) = self.system_columns_native()?;
        // Runs are written in `(RowId, Epoch)` ascending order (Bε-tree
        // composite key), so same-rid positions are consecutive with the
        // newest (highest epoch) last. One linear pass keeps the last position
        // per rid with `epoch <= snapshot`, dropping tombstones — no HashMap,
        // no per-row hashing, sequential memory access. (Phase 16.3.)
        //
        // Invariant: every write path (memtable drain, mutable-run spill,
        // bulk_load's sequential alloc) produces runs in this order; if a path
        // ever wrote an unsorted run this would under-count (only consecutive
        // dup groups merge) and must be reverted to the HashMap form.
        let mut idxs: Vec<usize> = Vec::new();
        let mut i = 0;
        while i < n {
            let rid = row_ids[i] as u64;
            // Walk the consecutive rid group; epochs rise within it, so the
            // last position with epoch <= snapshot is the newest visible one.
            let mut best: Option<usize> = None;
            let mut j = i;
            while j < n && row_ids[j] as u64 == rid {
                if epochs[j] as u64 <= snapshot.0 {
                    best = Some(j);
                }
                j += 1;
            }
            if let Some(b) = best {
                if deleted[b] == 0 {
                    idxs.push(b);
                }
            }
            i = j;
        }
        // Groups are processed in rid-ascending = position-ascending order.
        let rids: Vec<i64> = idxs.iter().map(|&k| row_ids[k]).collect();
        Ok((idxs, rids))
    }

    /// Row count of each PAX page of `column_id`, in page order. Every column
    /// in a run shares the same PAX row partition, so this yields the table's
    /// page layout (cumulative sums give page start offsets).
    pub fn page_row_counts(&self, column_id: u16) -> Result<Vec<usize>> {
        Ok(self
            .find_header(column_id)?
            .page_stats
            .iter()
            .map(|s| s.row_count as usize)
            .collect())
    }

    /// Whether this run stores `column_id` (false for a column added via
    /// `add_column` after the run was written — those read as all-null).
    pub fn has_column(&self, column_id: u16) -> bool {
        self.dir.iter().any(|h| h.column_id == column_id)
    }

    /// The per-page [`PageStat`]s for `column_id`, or `None` if the column is
    /// absent from this run (schema evolution). Used to compute exact column
    /// min/max/null_count for the analytical aggregate fast path.
    pub fn column_page_stats(&self, column_id: u16) -> Option<&[crate::page::PageStat]> {
        self.dir
            .iter()
            .find(|h| h.column_id == column_id)
            .map(|ch| ch.page_stats.as_slice())
    }

    /// Decode the system columns once, as typed buffers. Uses the shared
    /// parallel + decoded-page-cached path (`column_native_shared`) so the
    /// row-id/epoch/deleted pages decode concurrently and stay cached across
    /// queries — MVCC visibility resolution is on every scan's hot path.
    pub(crate) fn system_columns_native(&mut self) -> Result<(Vec<i64>, Vec<i64>, Vec<u8>)> {
        let row_ids = match self.column_native_shared(SYS_ROW_ID)? {
            columnar::NativeColumn::Int64 { data, .. } => data,
            _ => return Err(MongrelError::InvalidArgument("sys row_id not int64".into())),
        };
        let epochs = match self.column_native_shared(SYS_EPOCH)? {
            columnar::NativeColumn::Int64 { data, .. } => data,
            _ => return Err(MongrelError::InvalidArgument("sys epoch not int64".into())),
        };
        let deleted = match self.column_native_shared(SYS_DELETED)? {
            columnar::NativeColumn::Bool { data, .. } => data,
            _ => return Err(MongrelError::InvalidArgument("sys deleted not bool".into())),
        };
        Ok((row_ids, epochs, deleted))
    }

    /// Newest visible version per `RowId` at `snapshot`, **including
    /// tombstones** (as `Row`s with `deleted=true`). Ascending `RowId`. Used by
    /// the engine to merge versions across runs and the memtable.
    ///
    /// HLC stamps are restored when [`SYS_COMMIT_TS`] is present (P0.5-T3);
    /// legacy runs without the column materialise `commit_ts: None`. Callers
    /// that hold a full [`crate::epoch::Snapshot`] still apply
    /// [`crate::epoch::Snapshot::observes_row`].
    pub fn visible_versions(&mut self, snapshot: Epoch) -> Result<Vec<Row>> {
        let n = self.row_count();
        if n == 0 {
            return Ok(Vec::new());
        }
        let row_ids = self.column(SYS_ROW_ID)?.to_vec();
        let epochs = self.column(SYS_EPOCH)?.to_vec();
        let mut best: HashMap<u64, (u64, usize)> = HashMap::new();
        for i in 0..n {
            let rid = int_at(&row_ids, i);
            let epoch = int_at(&epochs, i);
            if epoch > snapshot.0 {
                continue;
            }
            best.entry(rid)
                .and_modify(|e| {
                    if epoch > e.0 {
                        *e = (epoch, i);
                    }
                })
                .or_insert((epoch, i));
        }
        let mut picks: Vec<usize> = best.into_values().map(|(_, i)| i).collect();
        picks.sort();
        let mut out = Vec::with_capacity(picks.len());
        for i in picks {
            out.push(self.materialize(i)?);
        }
        Ok(out)
    }

    /// All non-deleted rows visible at `snapshot`. Ascending `RowId`.
    pub fn visible_rows(&mut self, snapshot: Epoch) -> Result<Vec<Row>> {
        Ok(self
            .visible_versions(snapshot)?
            .into_iter()
            .filter(|r| !r.deleted)
            .collect())
    }

    pub(crate) fn materialize(&mut self, index: usize) -> Result<Row> {
        let row_id = RowId(int_at(self.column(SYS_ROW_ID)?, index));
        let epoch = Epoch(int_at(self.column(SYS_EPOCH)?, index));
        let deleted = bool_at(self.column(SYS_DELETED)?, index);
        let commit_ts = self.commit_ts_at(index)?;
        let col_ids: Vec<u16> = self.schema.columns.iter().map(|c| c.id).collect();
        let mut columns = HashMap::new();
        for id in col_ids {
            let val = if self.dir.iter().any(|h| h.column_id == id) {
                self.column(id)?.get(index).cloned().unwrap_or(Value::Null)
            } else {
                // Column added via schema evolution after this run was written.
                Value::Null
            };
            columns.insert(id, val);
        }
        Ok(Row {
            row_id,
            committed_epoch: epoch,
            columns,
            deleted,
            commit_ts,
        })
    }

    /// Batched row materialization (Phase 16.3b finish): decode each system +
    /// user column **once** via the typed, page-cached `column_native` path and
    /// gather only the requested `indices` straight into `Row`s. This replaces
    /// N independent `materialize` calls, each of which rebuilt a full-column
    /// `Vec<Value>` (heap-allocating one `Value` per row) and then `.cloned()`
    /// a single slot. Exact semantics retained: schema-evolved columns absent
    /// from this run read `Value::Null`; deleted rows are still returned (the
    /// caller filters them).
    pub(crate) fn materialize_batch(&mut self, indices: &[usize]) -> Result<Vec<Row>> {
        if indices.is_empty() {
            return Ok(Vec::new());
        }
        use std::collections::HashMap;
        let rid_col = self.column_native_shared(SYS_ROW_ID)?;
        let epoch_col = self.column_native_shared(SYS_EPOCH)?;
        let del_col = self.column_native_shared(SYS_DELETED)?;
        let commit_ts_col = if self.has_column(SYS_COMMIT_TS) {
            Some(self.column_native_shared(SYS_COMMIT_TS)?)
        } else {
            None
        };
        let mut user: HashMap<u16, columnar::NativeColumn> = HashMap::new();
        let present: Vec<u16> = self
            .schema
            .columns
            .iter()
            .map(|c| c.id)
            .filter(|id| self.dir.iter().any(|h| h.column_id == *id))
            .collect();
        for id in present {
            user.insert(id, self.column_native(id)?);
        }
        let i64_at = |col: &columnar::NativeColumn, i: usize| -> i64 {
            match col {
                columnar::NativeColumn::Int64 { data, .. } => data.get(i).copied().unwrap_or(0),
                _ => 0,
            }
        };
        let bool_at_native = |col: &columnar::NativeColumn, i: usize| -> bool {
            match col {
                columnar::NativeColumn::Bool { data, .. } => {
                    data.get(i).copied().map(|b| b != 0).unwrap_or(false)
                }
                _ => false,
            }
        };
        let mut rows = Vec::with_capacity(indices.len());
        for &idx in indices {
            let row_id = RowId(i64_at(&rid_col, idx) as u64);
            let epoch = Epoch(i64_at(&epoch_col, idx) as u64);
            let deleted = bool_at_native(&del_col, idx);
            let commit_ts = commit_ts_col
                .as_ref()
                .and_then(|col| decode_commit_ts_value(col.value_at(idx).as_ref()));
            let mut columns = HashMap::with_capacity(self.schema.columns.len());
            for cdef in self.schema.columns.iter() {
                let val = match user.get(&cdef.id) {
                    Some(col) => col.value_at(idx).unwrap_or(Value::Null),
                    None => Value::Null,
                };
                columns.insert(cdef.id, val);
            }
            rows.push(Row {
                row_id,
                committed_epoch: epoch,
                columns,
                deleted,
                commit_ts,
            });
        }
        Ok(rows)
    }
}

fn int_at(vals: &[Value], i: usize) -> u64 {
    match vals.get(i) {
        Some(Value::Int64(x)) => *x as u64,
        _ => 0,
    }
}

fn bool_at(vals: &[Value], i: usize) -> bool {
    matches!(vals.get(i), Some(Value::Bool(true)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::NativeColumn;
    use crate::memtable::Value;
    use crate::rowid::RowId;
    use crate::schema::{ColumnDef, ColumnFlags};
    use tempfile::tempdir;

    fn schema() -> Schema {
        Schema {
            schema_id: 1,
            columns: vec![
                ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                    default_value: None,
                    embedding_source: None,
                },
                ColumnDef {
                    id: 2,
                    name: "name".into(),
                    ty: TypeId::Bytes,
                    flags: ColumnFlags::empty(),
                    default_value: None,
                    embedding_source: None,
                },
            ],
            indexes: Vec::new(),
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        }
    }

    fn rows() -> Vec<Row> {
        vec![
            Row::new(RowId(1), Epoch(10))
                .with_column(1, Value::Int64(1))
                .with_column(2, Value::Bytes(b"alice".to_vec())),
            Row::new(RowId(2), Epoch(10))
                .with_column(1, Value::Int64(2))
                .with_column(2, Value::Bytes(b"bob".to_vec())),
            Row::new(RowId(3), Epoch(10))
                .with_column(1, Value::Int64(3))
                .with_column(2, Value::Bytes(b"carol".to_vec())),
        ]
    }

    #[test]
    fn flush_then_read_mvcc() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("r-1.sr");
        let header = RunWriter::new(&schema(), 1, Epoch(10), 0)
            .write(&path, &rows())
            .unwrap();
        assert_eq!(header.row_count, 3);

        let mut r = RunReader::open(&path, schema(), None).unwrap();
        // Point lookup.
        let (e, row) = r.get_version(RowId(2), Epoch(20)).unwrap().unwrap();
        assert_eq!(e, Epoch(10));
        assert_eq!(row.row_id, RowId(2));
        assert!(matches!(row.columns.get(&2), Some(Value::Bytes(_))));
        // Missing.
        assert!(r.get_version(RowId(99), Epoch(20)).unwrap().is_none());
        // Scan.
        let all = r.visible_rows(Epoch(20)).unwrap();
        assert_eq!(all.len(), 3);
        // Unstamped flush omits SYS_COMMIT_TS (legacy path).
        assert!(!r.has_column(SYS_COMMIT_TS));
        assert!(all.iter().all(|row| row.commit_ts.is_none()));
    }

    /// ID: P0.5-T3 — flush preserves HLC stamps via optional SYS_COMMIT_TS;
    /// legacy runs without the column materialise commit_ts as None.
    #[test]
    fn p05_t3_sys_commit_ts_preserved_on_flush_and_legacy_is_none() {
        let dir = tempdir().unwrap();
        let stamped_path = dir.path().join("r-hlc.sr");
        let stamp = HlcTimestamp {
            physical_micros: 42_000_000,
            logical: 7,
            node_tiebreaker: 3,
        };
        let stamped = vec![
            Row::new_with_hlc(RowId(1), Epoch(10), stamp)
                .with_column(1, Value::Int64(1))
                .with_column(2, Value::Bytes(b"alice".to_vec())),
            Row::new(RowId(2), Epoch(11)) // mixed: unstamped sibling
                .with_column(1, Value::Int64(2))
                .with_column(2, Value::Bytes(b"bob".to_vec())),
        ];
        RunWriter::new(&schema(), 9, Epoch(11), 0)
            .write(&stamped_path, &stamped)
            .unwrap();
        let mut reader = RunReader::open(&stamped_path, schema(), None).unwrap();
        assert!(
            reader.has_column(SYS_COMMIT_TS),
            "stamped flush must emit optional SYS_COMMIT_TS"
        );
        let (_, row1) = reader
            .get_version(RowId(1), Epoch(20))
            .unwrap()
            .expect("row 1");
        assert_eq!(row1.commit_ts, Some(stamp));
        let (_, row2) = reader
            .get_version(RowId(2), Epoch(20))
            .unwrap()
            .expect("row 2");
        assert_eq!(row2.commit_ts, None, "Null stamp cell stays None");
        let versions = reader.visible_versions(Epoch(20)).unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(
            versions
                .iter()
                .find(|r| r.row_id == RowId(1))
                .and_then(|r| r.commit_ts),
            Some(stamp)
        );

        // Legacy run (no stamps on write) lacks the column → always None.
        let legacy_path = dir.path().join("r-legacy.sr");
        RunWriter::new(&schema(), 10, Epoch(10), 0)
            .write(&legacy_path, &rows())
            .unwrap();
        let mut legacy = RunReader::open(&legacy_path, schema(), None).unwrap();
        assert!(!legacy.has_column(SYS_COMMIT_TS));
        let all = legacy.visible_versions(Epoch(20)).unwrap();
        assert!(all.iter().all(|r| r.commit_ts.is_none()));
    }

    #[test]
    fn visible_version_cursor_preserves_order_snapshot_and_tombstones() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("r-cursor.sr");
        let mut tombstone = Row::new(RowId(2), Epoch(2));
        tombstone.deleted = true;
        let versions = vec![
            Row::new(RowId(1), Epoch(4)).with_column(1, Value::Int64(10)),
            Row::new(RowId(2), Epoch(1)).with_column(1, Value::Int64(2)),
            tombstone,
            Row::new(RowId(3), Epoch(1)).with_column(1, Value::Int64(20)),
            Row::new(RowId(3), Epoch(3)).with_column(1, Value::Int64(23)),
            Row::new(RowId(4), Epoch(4)).with_column(1, Value::Int64(30)),
        ];
        RunWriter::new(&schema(), 7, Epoch(4), 0)
            .write(&path, &versions)
            .unwrap();

        let control = crate::ExecutionControl::new(None);
        let mut cursor = RunReader::open(&path, schema(), None)
            .unwrap()
            .into_visible_version_cursor(Epoch(2))
            .unwrap();
        let first = cursor.next_visible_version(&control).unwrap().unwrap();
        assert_eq!(first.row_id, RowId(2));
        assert!(first.deleted);
        assert_eq!(first.committed_epoch, Epoch(2));
        let second = cursor.next_visible_version(&control).unwrap().unwrap();
        assert_eq!(second.row_id, RowId(3));
        assert_eq!(second.committed_epoch, Epoch(1));
        let row = cursor.materialize(second, &control).unwrap();
        assert_eq!(row.columns.get(&1), Some(&Value::Int64(20)));
        assert!(cursor.next_visible_version(&control).unwrap().is_none());
    }

    #[test]
    fn learned_index_was_stored() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("r-2.sr");
        RunWriter::new(&schema(), 2, Epoch(1), 0)
            .write(&path, &rows())
            .unwrap();
        let header = read_header(&path).unwrap();
        assert!(
            header.index_trailer_offset != 0,
            "trailer should be present"
        );
    }

    #[test]
    fn low_level_container_still_round_trips() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("r-3.sr");
        let cols = vec![ColumnPayload {
            column_id: 1,
            type_id_tag: 8,
            encoding: Encoding::Plain,
            pages: vec![vec![1, 2, 3, 4]],
            page_stats: Vec::new(),
        }];
        let header = write_run(
            &path,
            &RunSpec {
                run_id: 1,
                schema_id: 1,
                epoch_created: 1,
                level: 0,
                flags: 0,
                sort_key_column_id: SORT_KEY_ROW_ID,
                row_count: 1,
                min_row_id: 0,
                max_row_id: 0,
                columns: &cols,
            },
        )
        .unwrap();
        let back = read_header(&path).unwrap();
        assert_eq!(back.content_hash, header.content_hash);
        assert_eq!(read_column_dir(&path, &back).unwrap().len(), 1);
    }

    #[test]
    fn detects_corruption() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("r-4.sr");
        write_run(
            &path,
            &RunSpec {
                run_id: 1,
                schema_id: 1,
                epoch_created: 1,
                level: 0,
                flags: 0,
                sort_key_column_id: SORT_KEY_ROW_ID,
                row_count: 1,
                min_row_id: 0,
                max_row_id: 0,
                columns: &[ColumnPayload {
                    column_id: 1,
                    type_id_tag: 8,
                    encoding: Encoding::Plain,
                    pages: vec![vec![1, 2, 3]],
                    page_stats: Vec::new(),
                }],
            },
        )
        .unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[300] ^= 0xFF;
        std::fs::write(&path, bytes).unwrap();
        let err = read_header(&path).unwrap_err();
        assert!(
            matches!(err, MongrelError::ChecksumMismatch { .. }),
            "got {err:?}"
        );
    }

    /// Phase 14.6: the direct-to-mmap placement logic must produce a byte-
    /// identical run to the in-buffer fallback. We drive `plan_run` + `place_run`
    /// into a plain `Vec<u8>` (so it's testable even where file mmap is denied —
    /// e.g. this sandbox), compare against `write_run_vec`, and additionally
    /// check that `write_run_mmap` agrees wherever a mapping can be created.
    #[test]
    fn mmap_and_vec_writers_are_byte_identical() {
        let dir = tempdir().unwrap();
        let stats = vec![PageStat {
            first_row_id: 0,
            last_row_id: 1,
            null_count: 0,
            row_count: 2,
            min: Some(vec![0]),
            max: Some(vec![9]),
            offset: 0,
            compressed_len: 0,
            uncompressed_len: 0,
        }];
        let spec = RunSpec {
            run_id: 7,
            schema_id: 1,
            epoch_created: 10,
            level: 0,
            flags: 0,
            sort_key_column_id: SORT_KEY_ROW_ID,
            row_count: 2,
            min_row_id: 0,
            max_row_id: 1,
            columns: &[
                ColumnPayload {
                    column_id: 1,
                    type_id_tag: 8,
                    encoding: Encoding::Plain,
                    pages: vec![vec![1, 2, 3, 4], vec![5, 6, 7, 8]],
                    page_stats: stats.clone(),
                },
                ColumnPayload {
                    column_id: 2,
                    type_id_tag: 8,
                    encoding: Encoding::Plain,
                    pages: vec![vec![10, 20], vec![30, 40]],
                    page_stats: stats.clone(),
                },
            ],
        };
        let trailer = b"learned-trailer-bytes";

        // (1) placement path into a Vec-backed buffer.
        let plan = plan_run(&spec, None, Some(trailer)).expect("plan");
        let mut buf = vec![0u8; plan.total];
        let h_place = place_run(&spec, None, Some(trailer), &plan, &mut buf)
            .expect("place_run into Vec succeeds");

        // (2) in-buffer fallback writer to a real file.
        let path_vec = dir.path().join("vec.sr");
        let h_vec = write_run_vec(&path_vec, &spec, None, Some(trailer)).unwrap();
        let bv = std::fs::read(&path_vec).unwrap();

        assert_eq!(h_place.content_hash, h_vec.content_hash);
        assert_eq!(h_place.footer_offset, h_vec.footer_offset);
        assert_eq!(
            buf, bv,
            "place_run and write_run_vec must be byte-identical"
        );
        assert!(read_header(&path_vec).is_ok());

        // (3) the mmap writer, when the FS supports it, must also agree. Where
        // file mmap is denied (some sandboxes), it reports the fallback sentinel
        // and `write_run_with` transparently uses the vec path instead.
        let path_mmap = dir.path().join("mmap.sr");
        match write_run_mmap(&path_mmap, &spec, None, Some(trailer)) {
            Ok(h_mmap) => {
                let bm = std::fs::read(&path_mmap).unwrap();
                assert_eq!(bm, bv, "mmap run must be byte-identical to vec run");
                assert_eq!(h_mmap.content_hash, h_vec.content_hash);
            }
            Err(e) if is_mmap_unavailable(&e) => {
                eprintln!("note: file mmap unavailable here; vec path covered (1)/(2)");
            }
            Err(e) => panic!("unexpected mmap error: {e:?}"),
        }
    }

    /// Phase 15.1: the `&self` parallel-decode path (`column_native_shared`)
    /// must yield the same `NativeColumn` as the `&mut` `column_native` path,
    /// column for column, on a multi-page run. This guards the cross-column
    /// parallel scan used by `visible_columns_native`.
    #[test]
    fn column_native_shared_matches_column_native() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("r.sr");
        // Enough rows to span >1 page (PAGE_ROWS = 65 536) so the parallel page
        // branch runs, plus a partial tail page.
        let n = 65_536 * 2 + 9;
        let id_col = NativeColumn::int64_sequence(1, n);
        let mut offsets = vec![0u32];
        let mut values = Vec::new();
        for i in 0..n {
            values.extend_from_slice(format!("v{}", i % 17).as_bytes());
            offsets.push(values.len() as u32);
        }
        let name_col = NativeColumn::Bytes {
            offsets,
            values,
            validity: vec![0xFF; n.div_ceil(8)],
        };
        let header = RunWriter::new(&schema(), 9, Epoch(5), 0)
            .write_native(&path, &[(1, id_col), (2, name_col)], n, 1)
            .unwrap();

        let mut reader = RunReader::open_with_cache(&path, schema(), None, None, None, 0, None)
            .expect("open reader");
        assert!(reader.has_mmap(), "test env must support read-only mmap");
        assert_eq!(reader.row_count(), header.row_count as usize);

        for cid in [1u16, 2] {
            let a = reader.column_native(cid).expect("column_native");
            let b = reader
                .column_native_shared(cid)
                .expect("column_native_shared");
            assert_eq!(a.len(), b.len(), "len mismatch col {cid}");
            // Byte-level equality of the typed buffers.
            match (&a, &b) {
                (
                    NativeColumn::Int64 {
                        data: da,
                        validity: va,
                    },
                    NativeColumn::Int64 {
                        data: db,
                        validity: vb,
                    },
                ) => {
                    assert_eq!(da, db, "Int64 data col {cid}");
                    assert_eq!(va, vb, "Int64 validity col {cid}");
                }
                (
                    NativeColumn::Bytes {
                        offsets: oa,
                        values: ua,
                        validity: va,
                    },
                    NativeColumn::Bytes {
                        offsets: ob,
                        values: ub,
                        validity: vb,
                    },
                ) => {
                    assert_eq!(oa, ob, "Bytes offsets col {cid}");
                    assert_eq!(ua, ub, "Bytes values col {cid}");
                    assert_eq!(va, vb, "Bytes validity col {cid}");
                }
                _ => panic!("type mismatch col {cid}: {a:?} vs {b:?}"),
            }
        }
    }

    #[test]
    fn page_cache_key_distinguishes_tables() {
        let a = page_cache_key(1, 5, 2, 3);
        let b = page_cache_key(2, 5, 2, 3);
        assert_ne!(a, b, "same run/col/page, different table must differ");
        // same table different run/col/page also differ (sanity)
        assert_ne!(page_cache_key(1, 5, 2, 3), page_cache_key(1, 6, 2, 3));
        assert_ne!(page_cache_key(1, 5, 2, 3), page_cache_key(1, 5, 2, 4));
    }
}
