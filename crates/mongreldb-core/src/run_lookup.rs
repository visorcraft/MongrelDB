//! RowId → run-set lookup directory (TODO §1.2).
//!
//! Derived, rebuildable, **never** authoritative. Missing, stale, corrupt, or
//! incomplete directory state falls back to the existing range-scan path in
//! `Table::get`. The directory is rebuilt from sorted-run system columns and
//! published atomically alongside the manifest, so a torn publication never
//! leaves an inconsistent directory visible.
//!
//! On-disk shape:
//! - delta-encoded `RowId` keys (varint);
//! - for each key, a newest-first list of `RunLocator`s (run_id u128, epoch
//!   min/max, optional HLC min/max, unstamped flag);
//! - a fingerprint of the exact active run-set + schema/index generation that
//!   produced the directory; rejected on reopen if the active manifest
//!   diverges;
//! - a CRC32C footer for corruption detection.
//!
//! Large tables use a sharded checkpoint layout (one file per `RowId` range).
//! The single-file API is retained for tests and small tables; the
//! production open path uses [`RunLookupDirectory::read_checkpoint_sharded`].

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

use crate::epoch::{Epoch, Snapshot};
use crate::manifest::RunRef;
use crate::rowid::RowId;
use crate::{Result, Table};
use crc::{Crc, CRC_32_ISCSI};
use mongreldb_types::hlc::HlcTimestamp;

/// On-disk filename of the single-file directory checkpoint. Production tables
/// use the sharded layout (see `directory_shard_path`).
pub const DIRECTORY_FILENAME: &str = "directory.bin";
/// Prefix for sharded directory files (`directory.shard-<start>.bin`).
pub const DIRECTORY_SHARD_PREFIX: &str = "directory.shard-";
pub const DIRECTORY_SHARD_SUFFIX: &str = ".bin";
/// Default max bytes per shard when publishing a large directory. Keeps the
/// in-memory buffer under 16 MiB so rebuilds/checkpoint publishes never need
/// the entire directory resident at once on a 256-run workload.
pub const DEFAULT_SHARD_BYTES: u64 = 16 * 1024 * 1024;

/// Magic bytes that prefix the on-disk checkpoint (`MLKP` = Mongrel Lookup).
const LOOKUP_MAGIC: [u8; 4] = *b"MLKP";
/// CRC32C (Castagnoli) over the body — same algorithm as the WAL.
const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
const FOOTER_LEN: usize = 4;
const HLC_BYTES: usize = 16;

/// What one run-level posting says about a `RowId`: enough metadata to
/// conservatively decide whether the run can contain a version visible to a
/// supplied snapshot, without opening the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunLocator {
    /// Stable identifier of the run, matched against the manifest's `RunRef`.
    pub run_id: u128,
    pub min_epoch: Epoch,
    pub max_epoch: Epoch,
    /// `Some` when the run carries HLC-stamped rows.
    pub min_hlc: Option<HlcTimestamp>,
    pub max_hlc: Option<HlcTimestamp>,
    /// `true` when the run also contains legacy unstamped (epoch-only) rows.
    pub contains_unstamped_versions: bool,
}

/// A complete, directly-consultable view of the directory for a single
/// `RowId`. Returned by [`RunLookupDirectory::locate`].
#[derive(Debug, Clone, Default)]
pub struct RunLocatorList {
    pub locators: Vec<RunLocator>,
}

impl RunLocatorList {
    pub fn is_empty(&self) -> bool {
        self.locators.is_empty()
    }

    pub fn len(&self) -> usize {
        self.locators.len()
    }
}

/// Per-`RowId` posting list. Backed by a `BTreeMap` for the in-memory
/// representation; the on-disk checkpoint uses delta-encoded `RowId` keys.
#[derive(Debug, Default, Clone)]
pub struct RunLookupDirectory {
    /// `(RowId -> newest-first RunLocator list)`.
    postings: BTreeMap<RowId, RunLocatorList>,
    /// Fingerprint of the active run-set + schema/index generation that
    /// produced this directory. `None` for an in-memory rebuild not yet
    /// checkpointed.
    fingerprint: Option<u64>,
}

impl RunLookupDirectory {
    /// Construct an empty directory with no fingerprint.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Look up the candidate run locators for `row_id`. Returns an empty list
    /// when the row has no postings (caller may treat this as a miss).
    pub fn locate(&self, row_id: RowId) -> &RunLocatorList {
        static EMPTY: RunLocatorList = RunLocatorList {
            locators: Vec::new(),
        };
        self.postings.get(&row_id).unwrap_or(&EMPTY)
    }

    /// Insert or replace a `RunLocator` for `row_id`. Locators are kept
    /// newest-first; the most recent write wins for ties.
    pub fn insert(&mut self, row_id: RowId, locator: RunLocator) {
        let entry = self.postings.entry(row_id).or_default();
        // Newest-first: locate the first locator whose `max_epoch`/`max_hlc`
        // is older than the new one and insert before it. Falls back to
        // append when the new locator is the oldest.
        let pos = entry
            .locators
            .iter()
            .position(|existing| locator_is_newer(locator, *existing));
        match pos {
            Some(idx) => entry.locators.insert(idx, locator),
            None => entry.locators.push(locator),
        }
    }

    /// Drop every locator whose `run_id` no longer exists in the active
    /// manifest. Returns the number of removed postings.
    pub fn retain_active_runs(&mut self, active_runs: &[u128]) -> usize {
        let mut removed = 0;
        for list in self.postings.values_mut() {
            let before = list.locators.len();
            list.locators.retain(|l| active_runs.contains(&l.run_id));
            removed += before - list.locators.len();
        }
        removed
    }

    /// Set the fingerprint of the active run-set + schema/index generation.
    pub fn set_fingerprint(&mut self, fp: u64) {
        self.fingerprint = Some(fp);
    }

    pub fn fingerprint(&self) -> Option<u64> {
        self.fingerprint
    }

    /// Number of `(RowId, posting)` entries. Used for memory/footprint metrics.
    pub fn total_locators(&self) -> usize {
        self.postings.values().map(|l| l.locators.len()).sum()
    }

    /// Number of distinct `RowId` keys.
    pub fn key_count(&self) -> usize {
        self.postings.len()
    }

    /// Maximum number of locators for any single `RowId`.
    pub fn max_locators_per_row(&self) -> usize {
        self.postings
            .values()
            .map(|l| l.locators.len())
            .max()
            .unwrap_or(0)
    }

    /// Average number of locators per `RowId` (0 when the directory is empty).
    pub fn avg_locators_per_row(&self) -> f64 {
        if self.postings.is_empty() {
            return 0.0;
        }
        self.total_locators() as f64 / self.postings.len() as f64
    }

    /// Persist the directory to `path` as a single atomic file (write to temp
    /// sibling, fsync, rename). A `fingerprint` must have been set; without
    /// one the on-disk file is meaningless on reopen. Layout: magic, fingerprint,
    /// key count, delta-encoded `RowId` keys with newest-first per-key locator
    /// lists, then a CRC32C footer.
    pub fn write_checkpoint(&self, path: &Path) -> io::Result<()> {
        let fp = self.fingerprint.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "run-lookup directory has no fingerprint; refusing to checkpoint",
            )
        })?;
        let mut body: Vec<u8> = Vec::new();
        body.extend_from_slice(&LOOKUP_MAGIC);
        body.extend_from_slice(&fp.to_le_bytes());
        let key_count = self.postings.len() as u32;
        body.extend_from_slice(&key_count.to_le_bytes());
        let mut prev: u64 = 0;
        for (row_id, list) in &self.postings {
            let cur = row_id.0;
            let delta = cur.checked_sub(prev).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "run-lookup row_id sequence is not non-decreasing",
                )
            })?;
            encode_varint(delta, &mut body);
            let locator_count = list.locators.len() as u32;
            body.extend_from_slice(&locator_count.to_le_bytes());
            for loc in &list.locators {
                body.extend_from_slice(&loc.run_id.to_le_bytes());
                body.extend_from_slice(&loc.min_epoch.0.to_le_bytes());
                body.extend_from_slice(&loc.max_epoch.0.to_le_bytes());
                write_optional_hlc(loc.min_hlc, &mut body);
                write_optional_hlc(loc.max_hlc, &mut body);
                body.push(if loc.contains_unstamped_versions {
                    1
                } else {
                    0
                });
            }
            prev = cur;
        }
        let crc = CRC32C.checksum(&body);
        body.extend_from_slice(&crc.to_le_bytes());

        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let staging = parent.join(format!(
            ".{}.{}.{}.staging",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("checkpoint"),
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
        ));
        {
            // Best-effort cleanup of any stale staging file left behind by a
            // crashed previous attempt at the same nanos tick. The window is
            // vanishingly small but the truncate avoids an `AlreadyExists`
            // failure that would mask a real I/O error.
            let _ = std::fs::remove_file(&staging);
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&staging)?;
            file.write_all(&body)?;
            file.sync_all()?;
        }
        match std::fs::rename(&staging, path) {
            Ok(()) => {}
            Err(error) => {
                let _ = std::fs::remove_file(&staging);
                return Err(error);
            }
        }
        if let Ok(dir) = OpenOptions::new().read(true).open(parent) {
            let _ = dir.sync_all();
        }
        Ok(())
    }

    /// Read a checkpoint file, validating magic, CRC32C, and that the stored
    /// fingerprint equals the supplied `fingerprint`. Any mismatch is returned
    /// as an `io::Error`; the caller may then fall back to `rebuild_from_runs`.
    pub fn read_checkpoint(path: &Path, fingerprint: u64) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let len = file.metadata()?.len();
        const MAX_CHECKPOINT_BYTES: u64 = 64 * 1024 * 1024;
        if len < 4 + 8 + 4 + FOOTER_LEN as u64 || len > MAX_CHECKPOINT_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "run-lookup checkpoint length is out of range",
            ));
        }
        let mut bytes = Vec::with_capacity(len as usize);
        file.read_to_end(&mut bytes)?;
        let split = bytes.len() - FOOTER_LEN;
        let (body, footer) = bytes.split_at(split);
        let expected = u32::from_le_bytes(footer.try_into().unwrap());
        let actual = CRC32C.checksum(body);
        if expected != actual {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "run-lookup checkpoint CRC mismatch",
            ));
        }
        if body.len() < 4 + 8 + 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "run-lookup checkpoint body too short",
            ));
        }
        if body[..4] != LOOKUP_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "run-lookup checkpoint magic mismatch",
            ));
        }
        let stored_fp = u64::from_le_bytes(body[4..12].try_into().unwrap());
        if stored_fp != fingerprint {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "run-lookup checkpoint fingerprint does not match active run set",
            ));
        }
        let key_count = u32::from_le_bytes(body[12..16].try_into().unwrap());
        let mut cursor = &body[16..];
        let mut postings: BTreeMap<RowId, RunLocatorList> = BTreeMap::new();
        let mut prev: u64 = 0;
        for _ in 0..key_count {
            let (delta, consumed) = decode_varint(cursor)?;
            cursor = &cursor[consumed..];
            let cur = prev.checked_add(delta).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "run-lookup checkpoint varint overflows u64",
                )
            })?;
            if cur < prev {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "run-lookup checkpoint row_id sequence is not monotonic",
                ));
            }
            if cursor.len() < 4 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "run-lookup checkpoint truncates locator count",
                ));
            }
            let locator_count = u32::from_le_bytes(cursor[..4].try_into().unwrap()) as usize;
            cursor = &cursor[4..];
            let mut list = RunLocatorList {
                locators: Vec::with_capacity(locator_count),
            };
            for _ in 0..locator_count {
                // Per-locator fixed prefix: run_id(16) + min_epoch(8) +
                // max_epoch(8) + min_hlc_presence(1) + max_hlc_presence(1)
                // + contains_unstamped(1) = 35. The two 16-byte HLC bodies
                // are checked separately via read_optional_hlc.
                let need = 16 + 8 + 8 + 1 + 1 + 1;
                if cursor.len() < need {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "run-lookup checkpoint truncates locator fields",
                    ));
                }
                let run_id_bytes: [u8; 16] = cursor[..16].try_into().unwrap();
                let run_id = u128::from_le_bytes(run_id_bytes);
                cursor = &cursor[16..];
                let min_epoch = u64::from_le_bytes(cursor[..8].try_into().unwrap());
                cursor = &cursor[8..];
                let max_epoch = u64::from_le_bytes(cursor[..8].try_into().unwrap());
                cursor = &cursor[8..];
                let (min_hlc, used) = read_optional_hlc(cursor)?;
                cursor = &cursor[used..];
                let (max_hlc, used) = read_optional_hlc(cursor)?;
                cursor = &cursor[used..];
                if cursor.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "run-lookup checkpoint truncates unstamped flag",
                    ));
                }
                let contains_unstamped = cursor[0] != 0;
                cursor = &cursor[1..];
                list.locators.push(RunLocator {
                    run_id,
                    min_epoch: Epoch(min_epoch),
                    max_epoch: Epoch(max_epoch),
                    min_hlc,
                    max_hlc,
                    contains_unstamped_versions: contains_unstamped,
                });
            }
            postings.insert(RowId(cur), list);
            prev = cur;
        }
        if !cursor.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "run-lookup checkpoint has trailing bytes after declared key count",
            ));
        }
        Ok(Self {
            postings,
            fingerprint: Some(fingerprint),
        })
    }

    /// Rebuild the directory by scanning each run's system columns. One
    /// `RunLocator` is emitted per `(RowId, run_id)` pair, aggregating
    /// min/max epoch + HLC and the unstamped-row presence flag across every
    /// version that run carries for that row.
    ///
    /// The implementation is the system-column-only iterator
    /// ([`crate::sorted_run::RunReader::for_each_system`]); the full-row
    /// fallback is intentionally absent so directory rebuilds never pay to
    /// materialize user columns. See spec §8.4 step 4.
    pub fn rebuild_from_runs(runs: &[RunRef], table: &Table) -> Result<Self> {
        rebuild_from_runs_system_only(runs, table)
    }

    /// Persist the directory as a sequence of shard files in `base_dir`, one
    /// per `RowId` range, bounded by `max_shard_bytes` each (default
    /// [`DEFAULT_SHARD_BYTES`]). Any previous `directory.shard-*.bin` files
    /// in `base_dir` are unlinked first so a torn-tail publication cannot
    /// leave a stale shard from the previous run.
    pub fn write_checkpoint_sharded(
        &self,
        base_dir: &Path,
        max_shard_bytes: u64,
    ) -> io::Result<()> {
        let _ = self.fingerprint.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "run-lookup directory has no fingerprint; refusing to checkpoint",
            )
        })?;
        // Best-effort: clear any prior shards before publishing new ones.
        clear_shards(base_dir)?;
        let max = max_shard_bytes.max(1024 * 1024); // sanity floor
        let mut iter = self.postings.iter().peekable();
        let mut shard_no: usize = 0;
        while let Some((first_row_id, _)) = iter.peek() {
            shard_no += 1;
            let shard_start = first_row_id.0;
            // The shard spans at least one entry. Determine a clean upper
            // boundary in row_id space by accumulating entries until the
            // projected body length would exceed `max`.
            let mut shard_entries: Vec<(RowId, RunLocatorList)> = Vec::new();
            let mut body_len: usize = 4 + 8 + 4; // magic + fp + key_count
            while let Some((_row_id, list)) = iter.peek() {
                let entry_cost = 1 + 8 // varint delta
                    + 4 // locator count
                    + list.locators.len()
                        * (16 + 8 + 8 + 1 + 1 + 16 + 16 + 1); // worst case per locator
                if !shard_entries.is_empty() && body_len + entry_cost > max as usize {
                    break;
                }
                body_len += entry_cost;
                let (rid, list) = iter.next().unwrap();
                shard_entries.push((*rid, list.clone()));
            }
            let last = shard_entries
                .last()
                .map(|(rid, _)| rid.0)
                .unwrap_or(shard_start);
            let path = if iter.peek().is_some() {
                directory_shard_path(base_dir, shard_start, Some(last))
            } else {
                directory_shard_path(base_dir, shard_start, None)
            };
            write_single_shard(&path, &shard_entries, self.fingerprint.unwrap())?;
        }
        if shard_no == 0 {
            // Empty directory: still emit one zero-row shard so the open path
            // can confirm the checkpoint was published atomically.
            let path = directory_shard_path(base_dir, 0, None);
            write_single_shard(&path, &[], self.fingerprint.unwrap())?;
        }
        // Final best-effort directory sync.
        if let Ok(dir) = OpenOptions::new().read(true).open(base_dir) {
            let _ = dir.sync_all();
        }
        Ok(())
    }

    /// Read the directory from the sharded layout in `base_dir`. Validates
    /// every shard's magic, CRC, and fingerprint independently. Returns the
    /// merged directory; a missing or unreadable shard is propagated as
    /// `io::Error` so the caller can fall back to rebuild-from-runs.
    pub fn read_checkpoint_sharded(base_dir: &Path, fingerprint: u64) -> io::Result<Self> {
        let mut paths: Vec<PathBuf> = Vec::new();
        for entry in std::fs::read_dir(base_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            // Shards use the `directory.shard-` prefix; the last shard is
            // published as `directory.shard-<start>-open` (no `.bin` suffix)
            // so the open boundary is unambiguous. Both layouts are matched
            // here.
            if name.starts_with(DIRECTORY_SHARD_PREFIX)
                && (name.ends_with(DIRECTORY_SHARD_SUFFIX) || name.ends_with("-open"))
            {
                paths.push(entry.path());
            }
        }
        paths.sort();
        if paths.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "no run-lookup directory shards present",
            ));
        }
        let mut merged: BTreeMap<RowId, RunLocatorList> = BTreeMap::new();
        for path in paths {
            let shard = read_single_shard(&path, fingerprint)?;
            for (rid, list) in shard {
                merged.insert(rid, list);
            }
        }
        Ok(Self {
            postings: merged,
            fingerprint: Some(fingerprint),
        })
    }
}

fn clear_shards(base_dir: &Path) -> io::Result<()> {
    if !base_dir.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(base_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with(DIRECTORY_SHARD_PREFIX)
            && (name.ends_with(DIRECTORY_SHARD_SUFFIX) || name.ends_with("-open"))
        {
            let _ = std::fs::remove_file(entry.path());
        }
    }
    Ok(())
}

fn write_single_shard(
    path: &Path,
    entries: &[(RowId, RunLocatorList)],
    fingerprint: u64,
) -> io::Result<()> {
    let mut body: Vec<u8> = Vec::new();
    body.extend_from_slice(&LOOKUP_MAGIC);
    body.extend_from_slice(&fingerprint.to_le_bytes());
    let key_count = entries.len() as u32;
    body.extend_from_slice(&key_count.to_le_bytes());
    let mut prev: u64 = 0;
    for (row_id, list) in entries {
        let cur = row_id.0;
        let delta = cur.checked_sub(prev).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "run-lookup row_id sequence is not non-decreasing",
            )
        })?;
        encode_varint(delta, &mut body);
        let locator_count = list.locators.len() as u32;
        body.extend_from_slice(&locator_count.to_le_bytes());
        for loc in &list.locators {
            body.extend_from_slice(&loc.run_id.to_le_bytes());
            body.extend_from_slice(&loc.min_epoch.0.to_le_bytes());
            body.extend_from_slice(&loc.max_epoch.0.to_le_bytes());
            write_optional_hlc(loc.min_hlc, &mut body);
            write_optional_hlc(loc.max_hlc, &mut body);
            body.push(if loc.contains_unstamped_versions {
                1
            } else {
                0
            });
        }
        prev = cur;
    }
    let crc = CRC32C.checksum(&body);
    body.extend_from_slice(&crc.to_le_bytes());

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let staging = parent.join(format!(
        ".{}.{}.{}.staging",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("shard"),
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    ));
    {
        let _ = std::fs::remove_file(&staging);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&staging)?;
        file.write_all(&body)?;
        file.sync_all()?;
    }
    match std::fs::rename(&staging, path) {
        Ok(()) => {}
        Err(error) => {
            let _ = std::fs::remove_file(&staging);
            return Err(error);
        }
    }
    Ok(())
}

fn read_single_shard(path: &Path, fingerprint: u64) -> io::Result<BTreeMap<RowId, RunLocatorList>> {
    let mut file = File::open(path)?;
    let len = file.metadata()?.len();
    // Shards are bounded by `max_shard_bytes`. A pathologically large shard
    // is treated as corruption so the caller can fall back to rebuild.
    if len < 4 + 8 + 4 + FOOTER_LEN as u64 || len > MAX_SHARD_BYTES * 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "run-lookup shard length is out of range",
        ));
    }
    let mut bytes = Vec::with_capacity(len as usize);
    file.read_to_end(&mut bytes)?;
    let split = bytes.len() - FOOTER_LEN;
    let (body, footer) = bytes.split_at(split);
    let expected = u32::from_le_bytes(footer.try_into().unwrap());
    let actual = CRC32C.checksum(body);
    if expected != actual {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "run-lookup shard CRC mismatch",
        ));
    }
    if body.len() < 4 + 8 + 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "run-lookup shard body too short",
        ));
    }
    if body[..4] != LOOKUP_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "run-lookup shard magic mismatch",
        ));
    }
    let stored_fp = u64::from_le_bytes(body[4..12].try_into().unwrap());
    if stored_fp != fingerprint {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "run-lookup shard fingerprint does not match active run set",
        ));
    }
    let key_count = u32::from_le_bytes(body[12..16].try_into().unwrap());
    let mut cursor = &body[16..];
    let mut postings: BTreeMap<RowId, RunLocatorList> = BTreeMap::new();
    let mut prev: u64 = 0;
    for _ in 0..key_count {
        let (delta, consumed) = decode_varint(cursor)?;
        cursor = &cursor[consumed..];
        let cur = prev.checked_add(delta).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "run-lookup shard varint overflows u64",
            )
        })?;
        if cur < prev {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "run-lookup shard row_id sequence is not monotonic",
            ));
        }
        if cursor.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "run-lookup shard truncates locator count",
            ));
        }
        let locator_count = u32::from_le_bytes(cursor[..4].try_into().unwrap()) as usize;
        cursor = &cursor[4..];
        let mut list = RunLocatorList {
            locators: Vec::with_capacity(locator_count),
        };
        for _ in 0..locator_count {
            let need = 16 + 8 + 8 + 1 + 1 + 1;
            if cursor.len() < need {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "run-lookup shard truncates locator fields",
                ));
            }
            let run_id_bytes: [u8; 16] = cursor[..16].try_into().unwrap();
            let run_id = u128::from_le_bytes(run_id_bytes);
            cursor = &cursor[16..];
            let min_epoch = u64::from_le_bytes(cursor[..8].try_into().unwrap());
            cursor = &cursor[8..];
            let max_epoch = u64::from_le_bytes(cursor[..8].try_into().unwrap());
            cursor = &cursor[8..];
            let (min_hlc, used) = read_optional_hlc(cursor)?;
            cursor = &cursor[used..];
            let (max_hlc, used) = read_optional_hlc(cursor)?;
            cursor = &cursor[used..];
            if cursor.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "run-lookup shard truncates unstamped flag",
                ));
            }
            let contains_unstamped = cursor[0] != 0;
            cursor = &cursor[1..];
            list.locators.push(RunLocator {
                run_id,
                min_epoch: Epoch(min_epoch),
                max_epoch: Epoch(max_epoch),
                min_hlc,
                max_hlc,
                contains_unstamped_versions: contains_unstamped,
            });
        }
        postings.insert(RowId(cur), list);
        prev = cur;
    }
    if !cursor.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "run-lookup shard has trailing bytes after declared key count",
        ));
    }
    Ok(postings)
}

// PathBuf is referenced by `read_checkpoint_sharded`; import for clarity.
use std::path::PathBuf;

fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn decode_varint(input: &[u8]) -> io::Result<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    let mut consumed = 0;
    for byte in input.iter().take(10) {
        consumed += 1;
        let low = (byte & 0x7F) as u64;
        value |= low.checked_shl(shift).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "run-lookup varint overflows u64",
            )
        })?;
        shift += 7;
        if byte & 0x80 == 0 {
            return Ok((value, consumed));
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "run-lookup varint is truncated or too long",
    ))
}

fn write_optional_hlc(ts: Option<HlcTimestamp>, out: &mut Vec<u8>) {
    match ts {
        None => out.push(0),
        Some(ts) => {
            out.push(1);
            out.extend_from_slice(&ts.physical_micros.to_le_bytes());
            out.extend_from_slice(&ts.logical.to_le_bytes());
            out.extend_from_slice(&ts.node_tiebreaker.to_le_bytes());
        }
    }
}

fn read_optional_hlc(input: &[u8]) -> io::Result<(Option<HlcTimestamp>, usize)> {
    if input.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "run-lookup checkpoint truncates hlc presence flag",
        ));
    }
    let present = input[0] != 0;
    if !present {
        return Ok((None, 1));
    }
    if input.len() < 1 + HLC_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "run-lookup checkpoint truncates hlc body",
        ));
    }
    let physical_micros = u64::from_le_bytes(input[1..9].try_into().unwrap());
    let logical = u32::from_le_bytes(input[9..13].try_into().unwrap());
    let node_tiebreaker = u32::from_le_bytes(input[13..17].try_into().unwrap());
    Ok((
        Some(HlcTimestamp {
            physical_micros,
            logical,
            node_tiebreaker,
        }),
        1 + HLC_BYTES,
    ))
}

/// `true` when `newer` is strictly newer than `older` by either epoch or
/// HLC. Ties compare as `false` (preserve existing order).
fn locator_is_newer(newer: RunLocator, older: RunLocator) -> bool {
    if newer.max_epoch > older.max_epoch {
        return true;
    }
    if newer.max_epoch < older.max_epoch {
        return false;
    }
    match (newer.max_hlc, older.max_hlc) {
        (Some(a), Some(b)) => a > b,
        (Some(_), None) => true,
        (None, Some(_)) => false,
        (None, None) => false,
    }
}

impl RunLocator {
    /// `true` when this locator *cannot* contain a version visible to
    /// `snapshot`. Conservative: when in doubt, return `false` so the caller
    /// opens the run. Mirrors the snapshot rules in spec §8.4 step 7:
    ///
    /// - HLC-authoritative snapshot + all-stamped locator → compare HLC
    ///   bounds; the locator must be entirely above the snapshot.
    /// - Epoch-only snapshot + locator without HLC → compare epoch bounds.
    /// - Mixed stamped/unstamped locator → conservative (return `false`).
    /// - Unknown / missing bound → conservative.
    pub fn is_impossible_for(&self, snapshot: Snapshot) -> bool {
        // A run that also carries legacy (unstamped) versions may still host
        // a version that's only visible under the epoch rule. Conservative
        // means we must NOT skip such a run.
        if self.contains_unstamped_versions {
            return false;
        }
        match (snapshot.uses_hlc_authority(), self.max_hlc, self.min_hlc) {
            // HLC-authoritative + locator fully stamped: skip when the locator's
            // minimum HLC already exceeds the snapshot — every version it
            // contains is too new to be visible.
            (true, Some(min_hlc), _) => min_hlc > snapshot.commit_ts,
            // Epoch-only + locator has no HLC at all: skip when the locator's
            // minimum epoch already exceeds the snapshot.
            (false, None, _) => {
                let snap_epoch = snapshot.epoch;
                self.min_epoch > snap_epoch
            }
            // Any other mix is incomparable — stay conservative.
            _ => false,
        }
    }
}

/// Deterministic fingerprint of the run-set + schema/index generations that
/// produced this directory. `format_version` is the directory's own wire
/// version so a future format bump invalidates older checkpoints even if the
/// run set is unchanged. Uses xxh3 (already a workspace dependency) with a
/// fixed seed, so the value is reproducible across processes/restarts.
pub fn compute_fingerprint(
    run_ids_ordered: &[u128],
    schema_id: u64,
    index_generation: u64,
    run_generation: u64,
    format_version: u32,
) -> u64 {
    let mut buf: Vec<u8> = Vec::with_capacity(8 + 8 + 8 + 4 + run_ids_ordered.len() * (16 + 2));
    buf.extend_from_slice(&schema_id.to_le_bytes());
    buf.extend_from_slice(&index_generation.to_le_bytes());
    buf.extend_from_slice(&run_generation.to_le_bytes());
    buf.extend_from_slice(&format_version.to_le_bytes());
    // Include the run level alongside the run id so a compacted-down topology
    // (level 0 → level 1 promotion) doesn't accidentally share a fingerprint
    // with the pre-compaction set.
    for run in run_ids_ordered {
        let run_id_bytes = run.to_le_bytes();
        buf.extend_from_slice(&run_id_bytes);
    }
    // Stable, fixed seed (NOT process-local / time-based).
    xxhash_rust::xxh3::xxh3_64_with_seed(&buf, 0x9E37_79B1_854A_0001)
}

/// Build the file path of a single sharded directory checkpoint for the
/// `RowId` range `[start, end_inclusive]`. `end_inclusive == None` means
/// the trailing shard (last open-ended range).
pub fn directory_shard_path(
    base_dir: &Path,
    start: u64,
    end_inclusive: Option<u64>,
) -> std::path::PathBuf {
    match end_inclusive {
        Some(end) => base_dir.join(format!(
            "{DIRECTORY_SHARD_PREFIX}{start:020}-{end:020}{DIRECTORY_SHARD_SUFFIX}"
        )),
        None => base_dir.join(format!("{DIRECTORY_SHARD_PREFIX}{start:020}-open")),
    }
}

/// Build a `RunLookupDirectory` for a single sorted run without materializing
/// any user columns. Uses [`crate::sorted_run::RunReader::for_each_system`]
/// (a system-column-only iterator) and folds the result into one
/// [`RunLocator`] per `RowId` that the run carries. The caller is expected
/// to call this from inside an `Arc<Table>` or equivalent — `table` is
/// borrowed for `open_reader`.
pub fn rebuild_from_runs_system_only(runs: &[RunRef], table: &Table) -> Result<RunLookupDirectory> {
    let mut dir = RunLookupDirectory::empty();
    for run_ref in runs {
        let mut reader = table.open_reader(run_ref.run_id)?;
        // Per-row agg: min epoch, max epoch, min hlc, max hlc,
        // contains_unstamped.
        type RunRowAgg = (
            Epoch,
            Epoch,
            Option<HlcTimestamp>,
            Option<HlcTimestamp>,
            bool,
        );
        let mut per_row: BTreeMap<RowId, RunRowAgg> = BTreeMap::new();
        reader.for_each_system(|row_id, committed_epoch, commit_ts, deleted| {
            let entry = per_row.entry(row_id).or_insert((
                committed_epoch,
                committed_epoch,
                commit_ts,
                commit_ts,
                commit_ts.is_none(),
            ));
            if committed_epoch < entry.0 {
                entry.0 = committed_epoch;
            }
            if committed_epoch > entry.1 {
                entry.1 = committed_epoch;
            }
            match (commit_ts, entry.2) {
                (Some(ts), Some(prev)) if ts < prev => entry.2 = Some(ts),
                (Some(_), None) => entry.2 = commit_ts,
                _ => {}
            }
            match (commit_ts, entry.3) {
                (Some(ts), Some(prev)) if ts > prev => entry.3 = Some(ts),
                (Some(_), None) => entry.3 = commit_ts,
                _ => {}
            }
            if commit_ts.is_none() {
                entry.4 = true;
            }
            // Tombstones do not change the locator's min/max envelope (they
            // share the row's commit epoch/HLC with the live version that
            // preceded them), so the deleted flag is intentionally ignored
            // for envelope construction. The HOT/manifest already removes
            // tombstones that are superseded by a newer live version.
            let _ = deleted;
            Ok(())
        })?;
        for (row_id, (min_epoch, max_epoch, min_hlc, max_hlc, contains_unstamped)) in per_row {
            dir.insert(
                row_id,
                RunLocator {
                    run_id: run_ref.run_id,
                    min_epoch,
                    max_epoch,
                    min_hlc,
                    max_hlc,
                    contains_unstamped_versions: contains_unstamped,
                },
            );
        }
    }
    Ok(dir)
}

/// Format version of the on-disk directory. Bumped when the wire format
/// changes in a way that older readers cannot safely interpret.
pub const DIRECTORY_FORMAT_VERSION: u32 = 1;
/// Hard upper bound for a single shard, raised from the 64 MiB legacy limit so
/// very large tables (e.g. 256-run benchmarks) can checkpoint without
/// truncation. The reader enforces only a soft sanity check.
pub const MAX_SHARD_BYTES: u64 = 16 * 1024 * 1024;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn hlc_from_phys(physical_micros: u64) -> HlcTimestamp {
        HlcTimestamp {
            physical_micros,
            logical: 0,
            node_tiebreaker: 0,
        }
    }

    fn loc(run_id: u128, epoch: u64, hlc: Option<u64>) -> RunLocator {
        RunLocator {
            run_id,
            min_epoch: Epoch(epoch),
            max_epoch: Epoch(epoch),
            min_hlc: hlc.map(hlc_from_phys),
            max_hlc: hlc.map(hlc_from_phys),
            contains_unstamped_versions: hlc.is_none(),
        }
    }

    #[test]
    fn empty_directory_returns_empty_list() {
        let dir = RunLookupDirectory::empty();
        assert!(dir.locate(RowId(42)).is_empty());
        assert_eq!(dir.key_count(), 0);
    }

    #[test]
    fn insert_orders_locators_newest_first() {
        let mut dir = RunLookupDirectory::empty();
        dir.insert(RowId(1), loc(0xA, 1, None));
        dir.insert(RowId(1), loc(0xB, 3, None));
        dir.insert(RowId(1), loc(0xC, 2, None));
        let list = dir.locate(RowId(1));
        let ids: Vec<u128> = list.locators.iter().map(|l| l.run_id).collect();
        assert_eq!(ids, vec![0xB, 0xC, 0xA]);
    }

    #[test]
    fn retain_active_runs_drops_stale_locators() {
        let mut dir = RunLookupDirectory::empty();
        dir.insert(RowId(1), loc(10, 1, None));
        dir.insert(RowId(1), loc(20, 2, None));
        dir.insert(RowId(2), loc(20, 1, None));
        let removed = dir.retain_active_runs(&[10]);
        assert_eq!(removed, 2);
        assert!(dir.locate(RowId(1)).locators.iter().all(|l| l.run_id == 10));
        assert!(dir.locate(RowId(2)).is_empty());
    }

    #[test]
    fn hlc_tie_break_resolves_above_epoch_only() {
        let mut dir = RunLookupDirectory::empty();
        dir.insert(RowId(1), loc(0xA, 5, None));
        dir.insert(RowId(1), loc(0xB, 5, Some(100)));
        let list = dir.locate(RowId(1));
        assert_eq!(list.locators[0].run_id, 0xB, "HLC newer wins on tie");
    }

    #[test]
    fn memory_metrics_report_postings() {
        let mut dir = RunLookupDirectory::empty();
        dir.insert(RowId(1), loc(1, 1, None));
        dir.insert(RowId(1), loc(2, 2, None));
        dir.insert(RowId(2), loc(1, 1, None));
        assert_eq!(dir.total_locators(), 3);
        assert_eq!(dir.key_count(), 2);
        assert_eq!(dir.max_locators_per_row(), 2);
        assert!((dir.avg_locators_per_row() - 1.5).abs() < 1e-9);
    }

    #[test]
    fn checkpoint_roundtrip_preserves_all_locators() {
        let dir = tempdir().unwrap();
        let mut src = RunLookupDirectory::empty();
        src.set_fingerprint(0x0102_0304_0506_0708);
        src.insert(RowId(7), loc(0x11, 1, None));
        src.insert(RowId(7), loc(0x22, 5, Some(100)));
        src.insert(RowId(8), loc(0x33, 3, Some(50)));
        src.insert(RowId(1_000_000), loc(0x44, 9, None));
        let path = dir.path().join("lookup.ckpt");
        src.write_checkpoint(&path).unwrap();

        let restored = RunLookupDirectory::read_checkpoint(&path, 0x0102_0304_0506_0708).unwrap();
        assert_eq!(restored.fingerprint(), Some(0x0102_0304_0506_0708));
        assert_eq!(restored.key_count(), 3);
        assert_eq!(restored.total_locators(), 4);
        for (row_id, expected) in [
            (RowId(7), vec![0x22, 0x11]),
            (RowId(8), vec![0x33]),
            (RowId(1_000_000), vec![0x44]),
        ] {
            let actual: Vec<u128> = restored
                .locate(row_id)
                .locators
                .iter()
                .map(|l| l.run_id)
                .collect();
            assert_eq!(actual, expected, "row_id {row_id} locator order");
        }
        let list7 = restored.locate(RowId(7));
        assert_eq!(
            list7.locators[0].min_hlc.map(|t| t.physical_micros),
            Some(100)
        );
        assert!(list7.locators[1].contains_unstamped_versions);
    }

    #[test]
    fn read_checkpoint_rejects_mismatched_fingerprint() {
        let dir = tempdir().unwrap();
        let mut src = RunLookupDirectory::empty();
        src.set_fingerprint(42);
        src.insert(RowId(1), loc(7, 1, None));
        let path = dir.path().join("lookup.ckpt");
        src.write_checkpoint(&path).unwrap();
        let err = RunLookupDirectory::read_checkpoint(&path, 43).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("fingerprint"));
    }
}
