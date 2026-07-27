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

/// What a `Table::get` directory consultation concluded with.
///
/// `CompleteMiss` is the authoritative answer "this row has no postings in
/// the directory" — the memtable and mutable-run tiers have already been
/// searched, so an empty complete lookup means no immutable run can host a
/// visible version. The caller opens zero readers.
///
/// `Candidates` means the directory returned at least one locator; the
/// caller walks them with the conservative snapshot filter and the
/// [`RunLocator::can_contain_version_newer_than`] early-stop proof.
///
/// `UnavailableOrStale` means the directory was missing, corrupt, or had a
/// stale fingerprint; the caller falls back to the existing range-scan
/// path. Distinct from `CompleteMiss` because the directory gave no
/// definitive answer and the row *could* still exist in an immutable run.
#[derive(Debug, Clone)]
pub(crate) enum DirectoryLookupDecision {
    CompleteMiss,
    Candidates(Vec<RunLocator>),
    UnavailableOrStale,
}

/// A compact snapshot of the (epoch, hlc) coordinates of a row version. Used
/// by the safe early-stop proof so the caller can ask "can this locator
/// still beat the current winner?" without exposing the full [`crate::memtable::Row`]
/// in the public surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VersionStamp {
    pub epoch: Epoch,
    pub hlc: Option<HlcTimestamp>,
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

    /// Decide how the read path should consult this directory for `row_id`.
    /// Distinguishes a complete directory miss (no postings → open zero
    /// immutable readers) from the absence of any usable directory
    /// (`UnavailableOrStale` → range-scan fallback).
    pub(crate) fn decide(&self, row_id: RowId) -> DirectoryLookupDecision {
        match self.postings.get(&row_id) {
            None => DirectoryLookupDecision::CompleteMiss,
            Some(list) if list.is_empty() => DirectoryLookupDecision::CompleteMiss,
            Some(list) => DirectoryLookupDecision::Candidates(list.locators.clone()),
        }
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
    /// opens the run. The proof is split into the stamped and unstamped
    /// possibilities and never uses positional tuple fields (REM-A):
    ///
    /// - a stamped version may be visible when the locator's **minimum**
    ///   HLC is at or below the snapshot (`min_hlc`, not `max_hlc` — a run
    ///   can span the snapshot and still host an older visible version);
    /// - an unstamped version may be visible when the locator's minimum
    ///   epoch is at or below the snapshot epoch.
    pub fn is_impossible_for(&self, snapshot: Snapshot) -> bool {
        let stamped_possible = self.may_have_visible_stamped_version(snapshot);
        let unstamped_possible = self.may_have_visible_unstamped_version(snapshot);

        !stamped_possible && !unstamped_possible
    }

    /// `true` when a stamped version inside this locator may be visible to
    /// `snapshot`. HLC-authoritative snapshots compare HLC bounds; epoch-only
    /// snapshots compare epoch bounds (stamped rows remain epoch-visible
    /// under the dual-model rule). The locator's `min_epoch` is aggregated
    /// across stamped and unstamped rows, so a false positive (opening a run
    /// that could have been skipped) is possible — that is safe; a false
    /// negative (skipping a run that contains a visible row) is not.
    fn may_have_visible_stamped_version(&self, snapshot: Snapshot) -> bool {
        let Some(min_hlc) = self.min_hlc else {
            return false;
        };

        if snapshot.uses_hlc_authority() {
            min_hlc <= snapshot.commit_ts
        } else {
            self.min_epoch <= snapshot.epoch
        }
    }

    /// `true` when an unstamped version inside this locator may be visible to
    /// `snapshot`. Unstamped visibility is epoch-based under both snapshot
    /// modes.
    fn may_have_visible_unstamped_version(&self, snapshot: Snapshot) -> bool {
        self.contains_unstamped_versions && self.min_epoch <= snapshot.epoch
    }

    /// Safe early-stop proof for `Table::get`. Returns `true` when this
    /// locator **might** still contain a version strictly newer than the
    /// current winner `best` and visible to `snapshot`. Returns `false`
    /// only when the locator provably cannot — the caller may then skip
    /// opening this run and increment the early-stop counter.
    ///
    /// The proof asks the stamped and unstamped questions independently
    /// (REM-B): a locator is skippable only when neither a stamped nor an
    /// unstamped version inside it can beat `best`. The recency rule of
    /// [`Snapshot::version_is_newer`] is honored exactly — HLC recency
    /// applies whenever both candidates are stamped, regardless of whether
    /// the snapshot itself is HLC-authoritative.
    ///
    /// Conservative: when the metadata is too sparse to prove impossibility,
    /// returns `true` so the caller opens the run (correctness wins over the
    /// optimization).
    pub(crate) fn can_contain_version_newer_than(
        &self,
        best: VersionStamp,
        snapshot: Snapshot,
    ) -> bool {
        // Unknown metadata: the locator records no usable HLC envelope and
        // claims no unstamped versions, so neither proof below can establish
        // anything. Contradictory-by-construction metadata must stay
        // conservative — open the run.
        if (self.min_hlc.is_none() || self.max_hlc.is_none()) && !self.contains_unstamped_versions {
            return true;
        }
        self.stamped_version_may_beat(best, snapshot)
            || self.unstamped_version_may_beat(best, snapshot)
    }

    /// `true` when a **stamped** version inside this locator may be visible
    /// to `snapshot` and strictly newer than `best`.
    ///
    /// - Stamped candidate versus stamped best compares HLC — under an
    ///   HLC-authoritative snapshot visibility caps the locator's max HLC at
    ///   `snapshot.commit_ts`; under an epoch-only snapshot visibility is
    ///   epoch-based but recency is still HLC-based, so the proof only
    ///   requires some stamped row to be epoch-visible while the locator's
    ///   max HLC exceeds the best (the locator does not preserve the
    ///   per-version epoch↔HLC correlation, so this is conservative).
    /// - Stamped candidate versus unstamped best compares epoch. Under an
    ///   HLC-authoritative snapshot the candidate is HLC-visible regardless
    ///   of its local epoch, so `max_epoch` is **not** capped by
    ///   `snapshot.epoch`; under an epoch-only snapshot both visibility and
    ///   recency are epoch-based and the cap applies.
    fn stamped_version_may_beat(&self, best: VersionStamp, snapshot: Snapshot) -> bool {
        let (Some(min_hlc), Some(max_hlc)) = (self.min_hlc, self.max_hlc) else {
            return false;
        };

        match best.hlc {
            Some(best_hlc) => {
                if snapshot.uses_hlc_authority() {
                    max_hlc.min(snapshot.commit_ts) > best_hlc
                } else {
                    self.min_epoch <= snapshot.epoch && max_hlc > best_hlc
                }
            }
            None => {
                if snapshot.uses_hlc_authority() {
                    min_hlc <= snapshot.commit_ts && self.max_epoch > best.epoch
                } else {
                    self.max_epoch.min(snapshot.epoch) > best.epoch
                }
            }
        }
    }

    /// `true` when an **unstamped** version inside this locator may be
    /// visible to `snapshot` and strictly newer than `best`. One side of the
    /// comparison is unstamped, so recency uses epoch; unstamped visibility
    /// is also epoch-based under both snapshot modes. `max_epoch` includes
    /// stamped versions too, so this may be a false positive — it cannot
    /// produce a false negative.
    fn unstamped_version_may_beat(&self, best: VersionStamp, snapshot: Snapshot) -> bool {
        self.contains_unstamped_versions && self.max_epoch.min(snapshot.epoch) > best.epoch
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

    fn stamped_loc(run_id: u128, epoch: u64, hlc: u64) -> RunLocator {
        RunLocator {
            run_id,
            min_epoch: Epoch(epoch),
            max_epoch: Epoch(epoch),
            min_hlc: Some(hlc_from_phys(hlc)),
            max_hlc: Some(hlc_from_phys(hlc)),
            contains_unstamped_versions: false,
        }
    }

    fn mixed_loc(run_id: u128, epoch: u64, hlc: u64) -> RunLocator {
        RunLocator {
            run_id,
            min_epoch: Epoch(epoch),
            max_epoch: Epoch(epoch),
            min_hlc: Some(hlc_from_phys(hlc)),
            max_hlc: Some(hlc_from_phys(hlc)),
            contains_unstamped_versions: true,
        }
    }

    #[test]
    fn decide_distinguishes_complete_miss_from_candidates() {
        let mut dir = RunLookupDirectory::empty();
        // No insert for RowId(99): a complete miss.
        match dir.decide(RowId(99)) {
            DirectoryLookupDecision::CompleteMiss => {}
            other => panic!("expected CompleteMiss, got {other:?}"),
        }
        // Empty posting list is also a complete miss (defensive — should
        // not happen in production, but the contract still says miss).
        dir.insert(RowId(7), stamped_loc(1, 1, 100));
        dir.insert(RowId(7), stamped_loc(2, 2, 200));
        match dir.decide(RowId(7)) {
            DirectoryLookupDecision::Candidates(list) => assert_eq!(list.len(), 2),
            other => panic!("expected Candidates, got {other:?}"),
        }
    }

    #[test]
    fn early_stop_epoch_only_proves_no_beat() {
        // Locator max_epoch = 5; best = epoch 10; cannot beat.
        let l = loc(1, 5, None);
        let snap = Snapshot::at(Epoch(20));
        let best = VersionStamp {
            epoch: Epoch(10),
            hlc: None,
        };
        assert!(
            !l.can_contain_version_newer_than(best, snap),
            "max_epoch <= best.epoch must early-stop"
        );
    }

    #[test]
    fn early_stop_epoch_only_proves_beat() {
        // Locator max_epoch = 15; best = epoch 10; visible cap 20.
        let l = loc(1, 15, None);
        let snap = Snapshot::at(Epoch(20));
        let best = VersionStamp {
            epoch: Epoch(10),
            hlc: None,
        };
        assert!(
            l.can_contain_version_newer_than(best, snap),
            "max_epoch > best.epoch must not early-stop"
        );
    }

    #[test]
    fn early_stop_pure_hlc_proves_no_beat() {
        // Locator max HLC = 100; best HLC = 200; snapshot HLC = 1000.
        // Visible cap = min(100, 1000) = 100 <= best.hlc=200 → no beat.
        let l = stamped_loc(1, 5, 100);
        let snap = Snapshot::at_hlc(Epoch(20), hlc_from_phys(1000));
        let best = VersionStamp {
            epoch: Epoch(5),
            hlc: Some(hlc_from_phys(200)),
        };
        assert!(
            !l.can_contain_version_newer_than(best, snap),
            "pure HLC: max visible HLC <= best HLC must early-stop"
        );
    }

    #[test]
    fn early_stop_pure_hlc_proves_beat() {
        // Locator max HLC = 500; best HLC = 200; snapshot HLC = 1000.
        // Visible cap = min(500, 1000) = 500 > best.hlc=200 → beat possible.
        let l = stamped_loc(1, 5, 500);
        let snap = Snapshot::at_hlc(Epoch(20), hlc_from_phys(1000));
        let best = VersionStamp {
            epoch: Epoch(5),
            hlc: Some(hlc_from_phys(200)),
        };
        assert!(
            l.can_contain_version_newer_than(best, snap),
            "pure HLC: max visible HLC > best HLC must not early-stop"
        );
    }

    #[test]
    fn early_stop_pure_hlc_respects_snapshot_cap() {
        // Locator max HLC = 5000; best HLC = 200; snapshot HLC = 100.
        // Visibility caps at snapshot, so locator cannot contain anything
        // strictly newer than best AND visible. Note: this is the case
        // where `is_impossible_for` would skip the run entirely; the early-
        // stop proof must agree when best is already pinned.
        let l = stamped_loc(1, 5, 5000);
        let snap = Snapshot::at_hlc(Epoch(20), hlc_from_phys(100));
        let best = VersionStamp {
            epoch: Epoch(5),
            hlc: Some(hlc_from_phys(200)),
        };
        assert!(
            !l.can_contain_version_newer_than(best, snap),
            "snapshot HLC cap below locator max must early-stop when best > cap"
        );
    }

    #[test]
    fn early_stop_mixed_with_unstamped_falls_back_to_epoch() {
        // Locator has stamped HLC=100 AND unstamped; best has high epoch
        // 200 but no HLC. Mixed comparison falls back to epoch. Even
        // though the locator's max HLC is below any sensible best, the
        // unstamped rows can beat on epoch alone.
        let l = mixed_loc(1, 250, 100);
        let snap = Snapshot::at_hlc(Epoch(300), hlc_from_phys(1000));
        let best = VersionStamp {
            epoch: Epoch(200),
            hlc: None,
        };
        assert!(
            l.can_contain_version_newer_than(best, snap),
            "mixed: unstamped epoch beat path must not early-stop"
        );
        // Inverse: best's epoch exceeds the locator's max.
        let l = mixed_loc(1, 100, 5000);
        let best = VersionStamp {
            epoch: Epoch(200),
            hlc: None,
        };
        assert!(
            !l.can_contain_version_newer_than(best, snap),
            "mixed: max epoch <= best.epoch must early-stop"
        );
    }

    #[test]
    fn early_stop_hlc_snap_with_unstamped_best_uses_epoch_path() {
        // best has no HLC; even under HLC-authoritative snap, comparison
        // falls back to epoch (mixed comparison rule).
        let l = stamped_loc(1, 250, 999);
        let snap = Snapshot::at_hlc(Epoch(300), hlc_from_phys(1000));
        let best = VersionStamp {
            epoch: Epoch(200),
            hlc: None,
        };
        assert!(
            l.can_contain_version_newer_than(best, snap),
            "unstamped best: epoch path wins even under HLC-authority snap"
        );
    }

    #[test]
    fn early_stop_unknown_metadata_stays_conservative() {
        // Spec §36.4: when locator metadata cannot prove a remaining run
        // is unable to beat the winner, open the run. Construct a locator
        // whose epoch bound is *equal* to the best — strict inequality is
        // required to prove the locator cannot beat, so a tie must stay
        // conservative.
        let l = RunLocator {
            run_id: 1,
            min_epoch: Epoch(10),
            max_epoch: Epoch(10),
            min_hlc: Some(hlc_from_phys(50)),
            max_hlc: Some(hlc_from_phys(50)),
            contains_unstamped_versions: false,
        };
        let snap = Snapshot::at_hlc(Epoch(20), hlc_from_phys(100));
        let best = VersionStamp {
            epoch: Epoch(10),
            hlc: Some(hlc_from_phys(50)),
        };
        // Strict equality is not a beat — but the proof is conservative
        // and never asserts a tie is a win.
        assert!(
            !l.can_contain_version_newer_than(best, snap),
            "strict tie (equal epoch+HLC) is not a beat; safe to early-stop"
        );

        // Truly unknown: locator with no HLC bounds AND no
        // contains_unstamped flag — metadata is contradictory and we
        // cannot prove anything in either direction. The proof must stay
        // conservative and keep the run open.
        let l_unknown = RunLocator {
            run_id: 2,
            min_epoch: Epoch(10),
            max_epoch: Epoch(10),
            min_hlc: None,
            max_hlc: None,
            contains_unstamped_versions: false,
        };
        let best_lower = VersionStamp {
            epoch: Epoch(5),
            hlc: Some(hlc_from_phys(50)),
        };
        assert!(
            l_unknown.can_contain_version_newer_than(best_lower, snap),
            "unknown metadata with higher possible epoch must stay conservative"
        );
    }

    // ------------------------------------------------------------------
    // REM-A (spec §5.6): the HLC impossibility proof must use `min_hlc`.
    // ------------------------------------------------------------------

    #[test]
    fn locator_spanning_hlc_snapshot_is_not_impossible() {
        // One run carries an old visible version (HLC 100) and a newer
        // invisible version (HLC 1,000); the snapshot sits between them.
        let locator = RunLocator {
            run_id: 1,
            min_epoch: Epoch(10),
            max_epoch: Epoch(20),
            min_hlc: Some(hlc_from_phys(100)),
            max_hlc: Some(hlc_from_phys(1_000)),
            contains_unstamped_versions: false,
        };
        let snapshot = Snapshot::at_hlc(Epoch(15), hlc_from_phys(500));
        assert!(
            !locator.is_impossible_for(snapshot),
            "min_hlc 100 <= snapshot 500: the run spans the snapshot and must be opened"
        );
    }

    #[test]
    fn locator_entirely_after_hlc_snapshot_is_impossible() {
        let locator = RunLocator {
            run_id: 1,
            min_epoch: Epoch(10),
            max_epoch: Epoch(20),
            min_hlc: Some(hlc_from_phys(600)),
            max_hlc: Some(hlc_from_phys(1_000)),
            contains_unstamped_versions: false,
        };
        let snapshot = Snapshot::at_hlc(Epoch(15), hlc_from_phys(500));
        assert!(
            locator.is_impossible_for(snapshot),
            "min_hlc 600 > snapshot 500: every stamped version is too new"
        );
    }

    #[test]
    fn locator_entirely_before_hlc_snapshot_is_possible() {
        let locator = RunLocator {
            run_id: 1,
            min_epoch: Epoch(10),
            max_epoch: Epoch(20),
            min_hlc: Some(hlc_from_phys(100)),
            max_hlc: Some(hlc_from_phys(200)),
            contains_unstamped_versions: false,
        };
        let snapshot = Snapshot::at_hlc(Epoch(15), hlc_from_phys(500));
        assert!(
            !locator.is_impossible_for(snapshot),
            "locator entirely below the snapshot is obviously possible"
        );
    }

    #[test]
    fn mixed_locator_is_impossible_only_when_both_paths_are_excluded() {
        // Stamped path open (min_hlc visible) even though the epoch bound
        // exceeds the snapshot epoch.
        let stamped_side_open = RunLocator {
            run_id: 1,
            min_epoch: Epoch(60),
            max_epoch: Epoch(70),
            min_hlc: Some(hlc_from_phys(100)),
            max_hlc: Some(hlc_from_phys(900)),
            contains_unstamped_versions: true,
        };
        let snap = Snapshot::at_hlc(Epoch(50), hlc_from_phys(500));
        assert!(
            !stamped_side_open.is_impossible_for(snap),
            "stamped min_hlc 100 <= 500 keeps the mixed locator possible"
        );
        // Unstamped path open (min_epoch visible) even though every HLC is
        // above the snapshot.
        let unstamped_side_open = RunLocator {
            run_id: 2,
            min_epoch: Epoch(40),
            max_epoch: Epoch(70),
            min_hlc: Some(hlc_from_phys(600)),
            max_hlc: Some(hlc_from_phys(900)),
            contains_unstamped_versions: true,
        };
        assert!(
            !unstamped_side_open.is_impossible_for(snap),
            "unstamped min_epoch 40 <= 50 keeps the mixed locator possible"
        );
        // Both paths excluded: min_hlc above the snapshot AND min_epoch
        // above the snapshot epoch.
        let fully_after = RunLocator {
            run_id: 3,
            min_epoch: Epoch(60),
            max_epoch: Epoch(70),
            min_hlc: Some(hlc_from_phys(600)),
            max_hlc: Some(hlc_from_phys(900)),
            contains_unstamped_versions: true,
        };
        assert!(
            fully_after.is_impossible_for(snap),
            "every stamped version is HLC-invisible and every unstamped version is epoch-invisible"
        );
    }

    // ------------------------------------------------------------------
    // REM-B (spec §6.12): authority-matrix tests for the split
    // stamped/unstamped early-stop proof.
    // ------------------------------------------------------------------

    /// Spec §6.5: a mixed locator (unstamped epoch 50 + stamped epoch 40 /
    /// HLC 300) must not be epoch-pruned against a stamped best (epoch 100 /
    /// HLC 200) — the stamped candidate wins by HLC.
    #[test]
    fn mixed_locator_stamped_candidate_can_beat_stamped_best_by_hlc() {
        let locator = RunLocator {
            run_id: 1,
            min_epoch: Epoch(40),
            max_epoch: Epoch(50),
            min_hlc: Some(hlc_from_phys(300)),
            max_hlc: Some(hlc_from_phys(300)),
            contains_unstamped_versions: true,
        };
        let snapshot = Snapshot::at_hlc(Epoch(200), hlc_from_phys(400));
        let best = VersionStamp {
            epoch: Epoch(100),
            hlc: Some(hlc_from_phys(200)),
        };
        assert!(
            locator.can_contain_version_newer_than(best, snapshot),
            "stamped candidate HLC 300 > best HLC 200; the locator must be opened"
        );
    }

    /// Spec §6.6: under an epoch-only snapshot, two stamped candidates are
    /// still ordered by HLC — the epoch-only snapshot must not disable HLC
    /// recency.
    #[test]
    fn epoch_snapshot_stamped_candidate_can_beat_stamped_best_by_hlc() {
        let locator = stamped_loc(1, 50, 300);
        let snapshot = Snapshot::at(Epoch(200));
        let best = VersionStamp {
            epoch: Epoch(100),
            hlc: Some(hlc_from_phys(200)),
        };
        assert!(
            locator.can_contain_version_newer_than(best, snapshot),
            "stamped vs stamped recency is HLC-based even under an epoch-only snapshot"
        );
        // Negative cell: the locator's max HLC does not exceed the best.
        let older = stamped_loc(2, 50, 150);
        assert!(
            !older.can_contain_version_newer_than(best, snapshot),
            "max_hlc 150 <= best HLC 200 and no unstamped rows: provably cannot beat"
        );
        // Negative cell: no stamped row can be epoch-visible.
        let future = RunLocator {
            run_id: 3,
            min_epoch: Epoch(300),
            max_epoch: Epoch(400),
            min_hlc: Some(hlc_from_phys(300)),
            max_hlc: Some(hlc_from_phys(300)),
            contains_unstamped_versions: false,
        };
        assert!(
            !future.can_contain_version_newer_than(best, snapshot),
            "min_epoch 300 > snapshot epoch 200: no stamped row is epoch-visible"
        );
    }

    /// Spec §6.7: a stamped candidate under HLC visibility is visible
    /// regardless of its local epoch, so its epoch must NOT be capped by
    /// `snapshot.epoch` when the best is unstamped.
    #[test]
    fn hlc_snapshot_stamped_candidate_can_beat_unstamped_best_by_epoch() {
        let locator = stamped_loc(1, 500, 200);
        let snapshot = Snapshot::at_hlc(Epoch(300), hlc_from_phys(300));
        let best = VersionStamp {
            epoch: Epoch(400),
            hlc: None,
        };
        assert!(
            locator.can_contain_version_newer_than(best, snapshot),
            "candidate HLC 200 <= snapshot 300 and epoch 500 > best epoch 400: must open"
        );
        // Negative cell: the stamped candidate is HLC-invisible.
        let invisible = stamped_loc(2, 500, 400);
        assert!(
            !invisible.can_contain_version_newer_than(best, snapshot),
            "min_hlc 400 > snapshot 300: no stamped version is visible"
        );
        // Negative cell: visible, but its epoch cannot beat the best.
        let older = stamped_loc(3, 350, 200);
        assert!(
            !older.can_contain_version_newer_than(best, snapshot),
            "max_epoch 350 <= best epoch 400: provably cannot beat"
        );
    }

    /// Unstamped candidates use epoch visibility and epoch recency under
    /// both snapshot modes.
    #[test]
    fn unstamped_candidate_uses_epoch_visibility_under_hlc_snapshot() {
        let locator = loc(1, 250, None); // unstamped-only, epoch 250
        let snapshot = Snapshot::at_hlc(Epoch(300), hlc_from_phys(1_000));
        let best = VersionStamp {
            epoch: Epoch(200),
            hlc: Some(hlc_from_phys(50)),
        };
        assert!(
            locator.can_contain_version_newer_than(best, snapshot),
            "unstamped epoch 250 > best epoch 200 and visible at snapshot epoch 300"
        );
        // Visibility cap: the snapshot epoch hides the whole locator.
        let tight_snap = Snapshot::at_hlc(Epoch(240), hlc_from_phys(1_000));
        let tight_best = VersionStamp {
            epoch: Epoch(240),
            hlc: Some(hlc_from_phys(50)),
        };
        assert!(
            !locator.can_contain_version_newer_than(tight_best, tight_snap),
            "min(250, 240) = 240 <= best epoch 240: no visible unstamped version can beat"
        );
        // Epoch-only snapshot behaves identically for unstamped candidates.
        let epoch_snap = Snapshot::at(Epoch(300));
        assert!(
            locator.can_contain_version_newer_than(best, epoch_snap),
            "unstamped epoch comparison does not depend on the snapshot mode"
        );
    }

    /// Metadata that cannot prove anything in either direction must keep the
    /// run open.
    #[test]
    fn unknown_metadata_is_conservative() {
        let best = VersionStamp {
            epoch: Epoch(5),
            hlc: Some(hlc_from_phys(50)),
        };
        let hlc_snap = Snapshot::at_hlc(Epoch(20), hlc_from_phys(100));
        let epoch_snap = Snapshot::at(Epoch(20));
        // Contradictory: claims fully stamped but records no HLC envelope.
        let no_envelope = RunLocator {
            run_id: 1,
            min_epoch: Epoch(10),
            max_epoch: Epoch(10),
            min_hlc: None,
            max_hlc: None,
            contains_unstamped_versions: false,
        };
        for snap in [hlc_snap, epoch_snap] {
            assert!(
                no_envelope.can_contain_version_newer_than(best, snap),
                "missing HLC envelope without the unstamped flag is unknown: stay conservative"
            );
        }
        // Partial envelope (min without max) is equally unprovable.
        let partial = RunLocator {
            min_hlc: Some(hlc_from_phys(10)),
            ..no_envelope
        };
        assert!(
            partial.can_contain_version_newer_than(best, hlc_snap),
            "a partial HLC envelope cannot prove impossibility: stay conservative"
        );
    }

    /// Strict inequality is required everywhere: equal HLC or equal epoch
    /// never counts as newer.
    #[test]
    fn equal_authority_does_not_count_as_strictly_newer() {
        let hlc_snap = Snapshot::at_hlc(Epoch(20), hlc_from_phys(100));
        // Stamped vs stamped, equal HLC.
        let stamped = stamped_loc(1, 10, 50);
        let stamped_best = VersionStamp {
            epoch: Epoch(10),
            hlc: Some(hlc_from_phys(50)),
        };
        assert!(
            !stamped.can_contain_version_newer_than(stamped_best, hlc_snap),
            "equal HLC is not strictly newer"
        );
        // Unstamped vs unstamped, equal epoch.
        let unstamped = loc(2, 10, None);
        let unstamped_best = VersionStamp {
            epoch: Epoch(10),
            hlc: None,
        };
        assert!(
            !unstamped.can_contain_version_newer_than(unstamped_best, hlc_snap),
            "equal epoch is not strictly newer"
        );
        // Stamped vs unstamped best under an epoch-only snapshot, equal
        // epoch.
        let epoch_snap = Snapshot::at(Epoch(20));
        assert!(
            !stamped.can_contain_version_newer_than(unstamped_best, epoch_snap),
            "equal epoch is not strictly newer on the stamped-vs-unstamped path"
        );
        // Unstamped candidate vs stamped best, equal epoch.
        assert!(
            !unstamped.can_contain_version_newer_than(stamped_best, epoch_snap),
            "equal epoch is not strictly newer on the unstamped path"
        );
    }
}
