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

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

use crate::epoch::Epoch;
use crate::manifest::RunRef;
use crate::rowid::RowId;
use crate::{Result, Table};
use crc::{Crc, CRC_32_ISCSI};
use mongreldb_types::hlc::HlcTimestamp;

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
    pub fn rebuild_from_runs(runs: &[RunRef], table: &Table) -> Result<Self> {
        let mut dir = Self::empty();
        type RunRowAgg = (
            Epoch,
            Epoch,
            Option<HlcTimestamp>,
            Option<HlcTimestamp>,
            bool,
        );
        for run_ref in runs {
            let mut reader = table.open_reader(run_ref.run_id)?;
            let rows = reader.all_rows()?;
            let mut per_row: BTreeMap<RowId, RunRowAgg> = BTreeMap::new();
            for row in rows {
                let entry = per_row.entry(row.row_id).or_insert((
                    row.committed_epoch,
                    row.committed_epoch,
                    row.commit_ts,
                    row.commit_ts,
                    row.commit_ts.is_none(),
                ));
                if row.committed_epoch < entry.0 {
                    entry.0 = row.committed_epoch;
                }
                if row.committed_epoch > entry.1 {
                    entry.1 = row.committed_epoch;
                }
                match (row.commit_ts, entry.2) {
                    (Some(ts), Some(prev)) if ts < prev => entry.2 = Some(ts),
                    (Some(_), None) => entry.2 = row.commit_ts,
                    _ => {}
                }
                match (row.commit_ts, entry.3) {
                    (Some(ts), Some(prev)) if ts > prev => entry.3 = Some(ts),
                    (Some(_), None) => entry.3 = row.commit_ts,
                    _ => {}
                }
                if row.commit_ts.is_none() {
                    entry.4 = true;
                }
            }
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
}

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
        assert_eq!(list7.locators[1].contains_unstamped_versions, true);
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
