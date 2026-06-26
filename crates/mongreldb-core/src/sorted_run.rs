//! Sorted Run — the immutable columnar unit (`.sr`).
//!
//! On-disk layout matches `DBPLAN.md` §6.2: a 256-byte header, a columnar page
//! region (PAX), a column directory, an index trailer, and a checksummed
//! footer. [`RunWriter`] flushes drained memtable rows into encoded columns
//! (system columns `_row_id` / `_epoch` / `_deleted` plus user columns), and
//! [`RunReader`] decodes them back, answering MVCC point lookups and scans.

use crate::columnar;
use crate::encryption::{setup_run_encryption, Cipher, Kek, RunEncryption};
use crate::epoch::Epoch;
use crate::error::{MongrelError, Result};
use crate::index::pgm::{LearnedIndex, PgmIndex};
use crate::memtable::{Row, Value};
use crate::page::{Encoding, PageStat};
use crate::rowid::RowId;
use crate::schema::{Schema, TypeId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

pub const RUN_MAGIC: [u8; 8] = *b"MONGRRUN";
pub const RUN_FORMAT_VERSION: u16 = 1;
pub const RUN_HEADER_VERSION: u16 = 1;
pub const RUN_HEADER_PAD: usize = 256;

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
pub const SORT_KEY_ROW_ID: u16 = 0xFFFF;

/// Reserved column ids for the MVCC system columns, stored in every run.
pub const SYS_ROW_ID: u16 = 0xFFFE;
pub const SYS_EPOCH: u16 = 0xFFFD;
pub const SYS_DELETED: u16 = 0xFFFC;

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
}

impl RunHeader {
    pub fn is_encrypted(&self) -> bool {
        self.flags & RUN_FLAG_ENCRYPTED != 0
    }
    pub fn is_clean(&self) -> bool {
        self.flags & RUN_FLAG_CLEAN != 0
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
    let plan = plan_run(spec, enc, index_trailer)?;
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    file.set_len(plan.total as u64)?;
    let mut mmap = match unsafe { memmap2::MmapMut::map_mut(&file) } {
        Ok(m) => m,
        Err(e) => {
            return Err(MongrelError::InvalidArgument(format!(
                "__mmap_unavailable__: {e}"
            )))
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
    let mut cursor: u64 = RUN_HEADER_PAD as u64;
    for (ci, col) in columns.iter().enumerate() {
        let region_offset = cursor;
        let mut region_len = 0u64;
        let mut stats = Vec::with_capacity(col.pages.len());
        for (ps, page) in col.pages.iter().enumerate() {
            // The per-page GCM nonce encodes page_seq in 2 bytes; refuse to
            // silently truncate past 65 535 pages/column (4.29e9 rows), which
            // would otherwise reuse a nonce under the run's DEK.
            if encrypted && ps > u16::MAX as usize {
                return Err(MongrelError::Full(format!(
                    "column {:#x} exceeds 65535 pages; encrypted-run page-seq nonce space exhausted",
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
            stats.push(stat);
            cursor += odl as u64;
            region_len += odl as u64;
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
    let footer_offset = cursor;
    let total = footer_offset as usize + 8 + 8 + 32;
    Ok(RunPlan {
        jobs,
        dir_bytes,
        encrypted,
        column_dir_offset,
        index_trailer_offset,
        encryption_descriptor_offset,
        footer_offset,
        total,
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
    let mut buf: Vec<u8> = vec![0; RUN_HEADER_PAD]; // reserve header region
    let mut content_hasher = Sha256::new();
    let mut dir: Vec<ColumnPageHeader> = Vec::with_capacity(spec.columns.len());

    for col in spec.columns {
        let region_offset = buf.len() as u64;
        let mut region_len = 0u64;
        let mut stats = Vec::with_capacity(col.pages.len());
        for (page_seq, page) in col.pages.iter().enumerate() {
            if enc.is_some() && page_seq > u16::MAX as usize {
                return Err(MongrelError::Full(format!(
                    "column {:#x} exceeds 65535 pages; encrypted-run page-seq nonce space exhausted",
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
            stats.push(stat);
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

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    file.write_all(&buf)?;
    file.sync_all()?;
    Ok(header)
}

fn page_nonce(nonce_prefix: [u8; 12], column_id: u16, page_seq: u32) -> [u8; 12] {
    let mut n = nonce_prefix;
    n[8..10].copy_from_slice(&column_id.to_le_bytes());
    n[10..12].copy_from_slice(&(page_seq as u16).to_le_bytes());
    n
}

/// Stable content-address of an immutable run page (the cache key): SHA-256 of
/// `(run_id, column_id, page_seq)`. Runs are immutable, so this identity is
/// also the page's content address — a rewritten page lives in a different run
/// (different id) and so gets a different key without any invalidation sweep.
pub(crate) fn page_cache_key(run_id: u128, column_id: u16, page_seq: usize) -> [u8; 32] {
    let mut h = Sha256::new();
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

/// Read and validate a run header (magic + footer checksum).
pub fn read_header(path: impl AsRef<Path>) -> Result<RunHeader> {
    let mut file = File::open(path)?;
    let mut header_buf = vec![0u8; RUN_HEADER_PAD];
    file.read_exact(&mut header_buf)?;
    let header: RunHeader = bincode::deserialize(&header_buf)
        .map_err(|e| MongrelError::InvalidArgument(format!("bad run header: {e}")))?;
    if header.magic != RUN_MAGIC {
        return Err(MongrelError::MagicMismatch {
            what: "sorted run",
            expected: RUN_MAGIC,
            got: header.magic,
        });
    }

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
    hasher.update(&header_buf);
    let body_len = header.footer_offset.saturating_sub(RUN_HEADER_PAD as u64);
    file.seek(SeekFrom::Start(RUN_HEADER_PAD as u64))?;
    let mut body = vec![0u8; body_len as usize];
    file.read_exact(&mut body)?;
    hasher.update(&body);
    let computed: [u8; 32] = hasher.finalize().into();
    let stored: [u8; 32] = footer[16..].try_into().unwrap();
    if computed != stored {
        return Err(MongrelError::ChecksumMismatch {
            expected: u64::from_be_bytes(stored[..8].try_into().unwrap()),
            actual: u64::from_be_bytes(computed[..8].try_into().unwrap()),
            context: "sorted run footer".into(),
        });
    }
    Ok(header)
}

/// Read the column directory.
pub fn read_column_dir(
    path: impl AsRef<Path>,
    header: &RunHeader,
) -> Result<Vec<ColumnPageHeader>> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(header.column_dir_offset))?;
    let end = if header.index_trailer_offset != 0 {
        header.index_trailer_offset
    } else {
        header.footer_offset
    };
    let len = end.saturating_sub(header.column_dir_offset);
    let mut buf = vec![0u8; len as usize];
    file.read_exact(&mut buf)?;
    let dir: Vec<ColumnPageHeader> = bincode::deserialize(&buf)
        .map_err(|e| MongrelError::InvalidArgument(format!("bad column dir: {e}")))?;
    Ok(dir)
}

/// Read the raw (bincode) Encryption Descriptor body stored at
/// `header.encryption_descriptor_offset` (a 4-byte length prefix precedes it).
pub(crate) fn read_encryption_descriptor_bytes(
    path: impl AsRef<Path>,
    header: &RunHeader,
) -> Result<Vec<u8>> {
    let mut file = File::open(path)?;
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
            le: false,
        }
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
                    native_column_pages(cdef.ty, col, encoding, compress, le, &bounds)?;
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

        let spec = RunSpec {
            run_id: self.run_id,
            schema_id: self.schema.schema_id,
            epoch_created: self.epoch_created.0,
            level: self.level,
            flags: RUN_FLAG_CLEAN,
            sort_key_column_id: SYS_ROW_ID,
            row_count: n as u64,
            min_row_id: first_row_id,
            max_row_id: first_row_id + n as u64 - 1,
            columns: &columns,
        };
        write_run_with(
            path,
            &spec,
            self.kek,
            &self.indexable_columns,
            Some(&learned_trailer),
        )
    }

    pub fn write(self, path: impl AsRef<Path>, rows: &[Row]) -> Result<RunHeader> {
        let n = rows.len();
        // System columns.
        let mut row_ids = Vec::with_capacity(n);
        let mut epochs = Vec::with_capacity(n);
        let mut deleted = Vec::with_capacity(n);
        for r in rows {
            row_ids.push(Value::Int64(r.row_id.0 as i64));
            epochs.push(Value::Int64(r.committed_epoch.0 as i64));
            deleted.push(Value::Bool(r.deleted));
        }
        let learned_trailer = build_learned_trailer(&row_ids);
        let (min_rid, max_rid) = row_id_bounds(rows);
        let row_id_i64: Vec<i64> = rows.iter().map(|r| r.row_id.0 as i64).collect();
        let bounds = page_bounds(&row_id_i64);

        let mut columns: Vec<ColumnPayload> = Vec::with_capacity(3 + self.schema.columns.len());
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
        // User columns — choose an encoding per column from run-time stats.
        for cdef in &self.schema.columns {
            let vals: Vec<Value> = rows
                .iter()
                .map(|r| r.columns.get(&cdef.id).cloned().unwrap_or(Value::Null))
                .collect();
            let (pages, stats, encoding) = value_column_pages(cdef.ty, &vals, &bounds)?;
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
            flags: if self.clean { RUN_FLAG_CLEAN } else { 0 },
            sort_key_column_id: SYS_ROW_ID,
            row_count: n as u64,
            min_row_id: min_rid,
            max_row_id: max_rid,
            columns: &columns,
        };
        write_run_with(
            path,
            &spec,
            self.kek,
            &self.indexable_columns,
            Some(&learned_trailer),
        )
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
            let stat = columnar::page_stat_for(ty, &chunk, frid, lrid);
            let page = columnar::encode_page_native(ty, &chunk, encoding, compress, le)?;
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
        pages.push(columnar::encode_page(ty, chunk, encoding)?);
        let native = columnar::values_to_native(ty, chunk);
        stats.push(columnar::page_stat_for(ty, &native, frid, lrid));
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
/// lookups via the learned index, and materializes visible rows for scans.
pub struct RunReader {
    file: File,
    mmap: Option<memmap2::Mmap>,
    header: RunHeader,
    dir: Vec<ColumnPageHeader>,
    schema: Schema,
    /// Per-run page cipher, built from the unwrapped DEK (None when plaintext).
    cipher: Option<Box<dyn Cipher>>,
    /// Per-run nonce prefix (overlaid per page with column_id + page_seq).
    nonce_prefix: [u8; 12],
    col_cache: HashMap<u16, Vec<Value>>,
    learned: Option<PgmIndex>,
    /// Shared, MVCC content-addressed page cache (Phase 9.2). Caches raw page
    /// bytes (ciphertext when encrypted) so all readers share decoded/decrypted
    /// pages. `None` only in standalone tests.
    page_cache: Option<Arc<parking_lot::Mutex<crate::cache::PageCache>>>,
    /// Shared decoded-page cache (Phase 15.4): the post-decompress/decrypt typed
    /// page, so a repeat scan skips decode. Keyed by `(run_id, column_id,
    /// page_seq)` identity; `None` in standalone tests.
    decoded_cache: Option<Arc<parking_lot::Mutex<crate::cache::DecodedPageCache>>>,
}

impl RunReader {
    pub fn open(path: impl AsRef<Path>, schema: Schema, kek: Option<Arc<Kek>>) -> Result<Self> {
        Self::open_with_cache(path, schema, kek, None, None)
    }

    pub(crate) fn open_with_cache(
        path: impl AsRef<Path>,
        schema: Schema,
        kek: Option<Arc<Kek>>,
        page_cache: Option<Arc<parking_lot::Mutex<crate::cache::PageCache>>>,
        decoded_cache: Option<Arc<parking_lot::Mutex<crate::cache::DecodedPageCache>>>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let header = read_header(&path)?;
        let dir = read_column_dir(&path, &header)?;
        let learned = read_learned(&path, &header)?;
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
            let desc_bytes = read_encryption_descriptor_bytes(&path, &header)?;
            let enc = crate::encryption::build_run_cipher(kek, &desc_bytes)?;
            (Some(enc.cipher), enc.nonce_prefix)
        } else {
            (None, [0u8; 12])
        };
        // Keep one open handle for all subsequent page reads (avoids a
        // File::open syscall per column).
        let file = File::open(&path)?;
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
            cipher,
            nonce_prefix,
            col_cache: HashMap::new(),
            learned,
            page_cache,
            decoded_cache,
        })
    }

    pub fn header(&self) -> &RunHeader {
        &self.header
    }

    /// Whether this run is "clean" (one version per RowId, no tombstones,
    /// ascending row_ids) — stamped at write time via [`RUN_FLAG_CLEAN`].
    pub fn is_clean(&self) -> bool {
        self.header.is_clean()
    }

    pub fn row_count(&self) -> usize {
        self.header.row_count as usize
    }

    fn resolve_type(&self, column_id: u16) -> TypeId {
        match column_id {
            SYS_ROW_ID | SYS_EPOCH => TypeId::Int64,
            SYS_DELETED => TypeId::Bool,
            _ => self
                .schema
                .columns
                .iter()
                .find(|c| c.id == column_id)
                .map(|c| c.ty)
                .unwrap_or(TypeId::Bytes),
        }
    }

    fn find_header(&self, column_id: u16) -> Result<&ColumnPageHeader> {
        self.dir
            .iter()
            .find(|h| h.column_id == column_id)
            .ok_or_else(|| MongrelError::ColumnNotFound(format!("column {column_id}")))
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
        // Shared cache: serve the raw (on-disk / ciphertext) page bytes if
        // present, so concurrent readers never re-read or re-decrypt a page.
        let key = page_cache_key(self.header.run_id, column_id, page_seq);
        if let Some(cache) = &self.page_cache {
            if let Some(bytes) = cache.lock().get(
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
            Some(m) => m[offset as usize..(offset + compressed_len as u64) as usize].to_vec(),
            None => {
                self.file.seek(SeekFrom::Start(offset))?;
                let mut buf = vec![0u8; compressed_len as usize];
                self.file.read_exact(&mut buf)?;
                buf
            }
        };
        // Spill the raw bytes into the shared cache (post-read, pre-decrypt).
        if let Some(cache) = &self.page_cache {
            cache.lock().insert(crate::page::CachedPage {
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
        let key = page_cache_key(self.header.run_id, column_id, page_seq);
        if let Some(cache) = &self.page_cache {
            if let Some(guard) = cache.try_lock() {
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
        let start = stat.offset as usize;
        let end = (stat.offset + stat.compressed_len as u64) as usize;
        let buf = mmap[start..end].to_vec();
        // Opportunistic, non-blocking insert: populate the shared cache so later
        // readers (and encrypted re-reads) skip the mmap slice + decrypt. Never
        // block the rayon pool — if the lock is contended, just skip the insert.
        if let Some(cache) = &self.page_cache {
            if let Some(mut guard) = cache.try_lock() {
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
        if !self.col_cache.contains_key(&column_id) {
            let ty = self.resolve_type(column_id);
            let page_rows: Vec<usize> = {
                let ch = self.find_header(column_id)?;
                ch.page_stats.iter().map(|s| s.row_count as usize).collect()
            };
            let mut decoded: Vec<Value> = Vec::with_capacity(self.row_count());
            for (seq, &pr) in page_rows.iter().enumerate() {
                let page = self.read_page(column_id, seq)?;
                decoded.extend(columnar::decode_page(ty, &page, pr)?);
            }
            self.col_cache.insert(column_id, decoded);
        }
        Ok(self.col_cache.get(&column_id).unwrap().as_slice())
    }

    /// Newest version of `row_id` with `epoch <= snapshot`, including tombstones
    /// (returned as a `Row` with `deleted=true`). `None` if no such version.
    pub fn get_version(&mut self, row_id: RowId, snapshot: Epoch) -> Result<Option<(Epoch, Row)>> {
        let n = self.row_count();
        if n == 0 {
            return Ok(None);
        }
        let target = row_id.0;
        let row_ids = self.column(SYS_ROW_ID)?.to_vec();
        let window = self.predict_window(target, n);
        // Scan the predicted window; fall back to a full binary search if the
        // learned model missed (e.g. sparse sampling at the tail).
        let mut start = None;
        let mut probe = window;
        if probe.is_empty() {
            probe = 0..n;
        }
        for i in probe.clone() {
            if int_at(&row_ids, i) == target {
                start = Some(i);
                break;
            }
        }
        let mut idx = start;
        if idx.is_none() {
            match row_ids.binary_search_by_key(&target, value_row_id) {
                Ok(i) => idx = Some(i),
                Err(_) => return Ok(None),
            }
        }
        let mut best: Option<(u64, usize)> = None; // (epoch, index)
        let mut i = idx.unwrap();
        while i < n && int_at(&row_ids, i) == target {
            let epoch = int_at(self.column(SYS_EPOCH)?, i);
            if epoch <= snapshot.0 && best.map(|(be, _)| epoch > be).unwrap_or(true) {
                best = Some((epoch, i));
            }
            i += 1;
        }
        match best {
            None => Ok(None),
            Some((epoch, index)) => Ok(Some((Epoch(epoch), self.materialize(index)?))),
        }
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
                    columnar::decode_page_native(ty, &raw, page_rows[seq])
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            let mut out = Vec::with_capacity(page_count);
            for (seq, &pr) in page_rows.iter().enumerate() {
                let page = self.read_page(column_id, seq)?;
                out.push(columnar::decode_page_native(ty, &page, pr)?);
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
        if let (Some(m), Some(first)) = (&self.mmap, ch.page_stats.first()) {
            let start = first.offset as usize;
            let end = (ch.page_region_offset as usize) + (ch.page_region_len as usize);
            if end > start {
                let _ = m.advise_range(memmap2::Advice::WillNeed, start, end - start);
            }
        }
        let run_id = self.header.run_id;
        // Decode in parallel (cache probes use `try_lock` → no worker blocking).
        // Each item is the decoded page plus its key when it was a cache miss
        // (hits return `None` for the key so we don't re-insert/clone them).
        let mut parts_keys: Vec<(columnar::NativeColumn, Option<[u8; 32]>)> = if page_count > 1 {
            (0..page_count)
                .into_par_iter()
                .map(|seq| self.decode_page_cached(ty, column_id, seq, page_rows[seq], run_id))
                .collect::<Result<Vec<_>>>()?
        } else {
            vec![self.decode_page_cached(ty, column_id, 0, page_rows[0], run_id)?]
        };
        // Sequentially cache the freshly-decoded pages — no parallel contention
        // on the insert, so every miss is reliably stored for the next scan.
        if let Some(cache) = &self.decoded_cache {
            let mut g = cache.lock();
            for (col, key) in parts_keys.iter_mut() {
                if let Some(k) = key.take() {
                    g.insert(k, Arc::new(col.clone()));
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
        let key = page_cache_key(run_id, column_id, seq);
        if let Some(cache) = &self.decoded_cache {
            if let Some(g) = cache.try_lock() {
                if let Some(hit) = g.try_get(&key) {
                    return Ok(((*hit).clone(), None));
                }
            }
        }
        let raw = self.read_page_shared(column_id, seq)?;
        let col = columnar::decode_page_native(ty, &raw, nrows)?;
        Ok((col, Some(key)))
    }

    /// Row ids whose Int64 value is in `[lo, hi]`, **skipping pages whose
    /// `[min,max]` stat excludes the range** (Parquet-style page-index pruning).
    /// Nulls are excluded. Used by `Db::query_columns_native` to serve
    /// `Condition::Range` without decoding every page.
    pub fn range_row_ids_i64(
        &mut self,
        column_id: u16,
        lo: i64,
        hi: i64,
    ) -> Result<std::collections::HashSet<u64>> {
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
                None => return Ok(std::collections::HashSet::new()),
            };
        let mut out = std::collections::HashSet::new();
        for (seq, (mn, mx, nrows)) in info.into_iter().enumerate() {
            // Skip pages that cannot contain a match (or are all-null).
            let skip = match (mn, mx) {
                (Some(mn), Some(mx)) => mx < lo || mn > hi,
                _ => true,
            };
            if skip {
                continue;
            }
            let rid_page = self.read_page(SYS_ROW_ID, seq)?;
            let val_page = self.read_page(column_id, seq)?;
            let rids = columnar::decode_page_native(TypeId::Int64, &rid_page, nrows)?;
            let vals =
                columnar::decode_page_native(self.resolve_type(column_id), &val_page, nrows)?;
            if let (
                columnar::NativeColumn::Int64 { data: r, .. },
                columnar::NativeColumn::Int64 { data: v, validity },
            ) = (rids, vals)
            {
                for (i, val) in v.iter().enumerate() {
                    if columnar::validity_bit(&validity, i) && *val >= lo && *val <= hi {
                        out.insert(r[i] as u64);
                    }
                }
            }
        }
        Ok(out)
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
                None => return Ok(std::collections::HashSet::new()),
            };
        let mut out = std::collections::HashSet::new();
        for (seq, (mn, mx, nrows)) in info.into_iter().enumerate() {
            // A page can be dropped iff every value fails the predicate, i.e. the
            // largest fails the lo-test or the smallest fails the hi-test.
            let skip = match (mn, mx) {
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
            let rid_page = self.read_page(SYS_ROW_ID, seq)?;
            let val_page = self.read_page(column_id, seq)?;
            let rids = columnar::decode_page_native(TypeId::Int64, &rid_page, nrows)?;
            let vals = columnar::decode_page_native(TypeId::Float64, &val_page, nrows)?;
            if let (
                columnar::NativeColumn::Int64 { data: r, .. },
                columnar::NativeColumn::Float64 { data: v, validity },
            ) = (rids, vals)
            {
                for (i, val) in v.iter().enumerate() {
                    if !columnar::validity_bit(&validity, i) || val.is_nan() {
                        continue;
                    }
                    let ok_lo = if lo_inclusive { *val >= lo } else { *val > lo };
                    let ok_hi = if hi_inclusive { *val <= hi } else { *val < hi };
                    if ok_lo && ok_hi {
                        out.insert(r[i] as u64);
                    }
                }
            }
        }
        Ok(out)
    }

    /// Visible array indices computed from typed system columns (no `Value`).
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
        let (positions, rids) = self.visible_positions_with_rids(snapshot)?;
        let mut out: Vec<u64> = Vec::new();
        let mut vis = 0usize;
        let mut page_start = 0usize;
        for (seq, &(mn, mx, nrows)) in stats.iter().enumerate() {
            let page_end = page_start + nrows;
            // A page can be dropped iff every value fails the predicate.
            let skip = match (mn, mx) {
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
        let (positions, rids) = self.visible_positions_with_rids(snapshot)?;
        let mut out: Vec<u64> = Vec::new();
        let mut vis = 0usize;
        let mut page_start = 0usize;
        for (seq, &(mn, mx, nrows)) in stats.iter().enumerate() {
            let page_end = page_start + nrows;
            let skip = match (mn, mx) {
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

    /// Visible row positions (latest version per `RowId` at `snapshot`,
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
        if self.is_clean() {
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

    fn predict_window(&self, target: u64, n: usize) -> std::ops::Range<usize> {
        let Some(idx) = &self.learned else {
            return 0..n;
        };
        let (lo, hi) = idx.predict(target);
        let lo = lo.min(n);
        let hi = if hi == usize::MAX { n } else { hi.min(n) };
        lo..hi
    }

    pub(crate) fn materialize(&mut self, index: usize) -> Result<Row> {
        let row_id = RowId(int_at(self.column(SYS_ROW_ID)?, index));
        let epoch = Epoch(int_at(self.column(SYS_EPOCH)?, index));
        let deleted = bool_at(self.column(SYS_DELETED)?, index);
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
            });
        }
        Ok(rows)
    }
}

fn read_learned(path: &Path, header: &RunHeader) -> Result<Option<PgmIndex>> {
    if header.index_trailer_offset == 0 {
        return Ok(None);
    }
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(header.index_trailer_offset))?;
    let len = header
        .footer_offset
        .saturating_sub(header.index_trailer_offset);
    let mut buf = vec![0u8; len as usize];
    file.read_exact(&mut buf)?;
    let pgm: PgmIndex = bincode::deserialize(&buf)
        .map_err(|e| MongrelError::InvalidArgument(format!("bad learned trailer: {e}")))?;
    Ok(Some(pgm))
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

fn value_row_id(v: &Value) -> u64 {
    match v {
        Value::Int64(x) => *x as u64,
        _ => u64::MAX,
    }
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
                },
                ColumnDef {
                    id: 2,
                    name: "name".into(),
                    ty: TypeId::Bytes,
                    flags: ColumnFlags::empty(),
                },
            ],
            indexes: Vec::new(),
            colocation: vec![],
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

        let mut reader =
            RunReader::open_with_cache(&path, schema(), None, None, None).expect("open reader");
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
}
