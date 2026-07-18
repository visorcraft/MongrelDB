//! Operational tools: garbage collection and integrity check.
//!
//! `gc` removes run files not referenced by the manifest (orphans from aborted
//! flushes / old compaction inputs) and stale WAL segments (all but the active
//! one). `check` re-verifies every referenced run's footer checksum.

use crate::engine::Table;
use crate::sorted_run::read_header;
use crate::Result;
use std::collections::HashSet;

/// Outcome of a [`Table::gc`] pass.
#[derive(Debug, Default, Clone)]
pub struct GcReport {
    pub runs_removed: usize,
    pub wal_segments_removed: usize,
    pub bytes_freed: u64,
}

/// Outcome of a [`Table::gc_versions`] pass (S1C-004).
#[derive(Debug, Clone)]
pub struct GcVersionsReport {
    /// The unified version-retention floor the pass consulted: the oldest
    /// epoch still held by ANY pin source (oldest active transaction
    /// snapshot, configured history retention, and every registered
    /// backup/PITR, replication, cursor/read-generation, and
    /// online-index-build pin).
    pub floor: crate::epoch::Epoch,
    /// Retiring runs physically reaped because their `retire_epoch` was at or
    /// below `floor`.
    pub runs_reaped: usize,
}

/// Outcome of a [`Table::check`] pass.
#[derive(Debug, Default, Clone)]
pub struct CheckReport {
    pub runs_checked: usize,
    pub runs_ok: usize,
    pub issues: Vec<String>,
}

/// Outcome of a [`Table::doctor`] pass (best-effort repair): corrupt runs are
/// dropped from the manifest so the table is usable again, at the cost of the
/// data they held.
#[derive(Debug, Default, Clone)]
pub struct DoctorReport {
    pub runs_dropped: Vec<u128>,
}

impl Table {
    /// Remove orphan run files (not in the manifest) and all but the active WAL
    /// segment. Safe to run any time; readers pin snapshots, not files.
    pub fn gc(&self) -> Result<GcReport> {
        let mut report = GcReport::default();
        let live: HashSet<u128> = self
            .run_refs()
            .iter()
            .map(|r| r.run_id)
            .chain(self.retiring_run_ids())
            .collect();

        for entry in std::fs::read_dir(self.runs_dir())? {
            let entry = entry?;
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Some(id) = name
                .strip_prefix("r-")
                .and_then(|s| s.strip_suffix(".sr"))
                .and_then(|s| s.parse::<u128>().ok())
            else {
                continue;
            };
            if !live.contains(&id) {
                let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                if std::fs::remove_file(&path).is_ok() {
                    report.runs_removed += 1;
                    report.bytes_freed += bytes;
                }
            }
        }

        // WAL: keep only the highest-numbered segment.
        let mut segs: Vec<(u32, std::path::PathBuf)> = Vec::new();
        for entry in std::fs::read_dir(self.wal_dir())? {
            let entry = entry?;
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Some(n) = name
                .strip_prefix("seg-")
                .and_then(|s| s.strip_suffix(".wal"))
                .and_then(|s| s.parse::<u32>().ok())
            else {
                continue;
            };
            segs.push((n, path));
        }
        segs.sort_by_key(|(n, _)| *n);
        if let Some((_, active)) = segs.last() {
            let active = active.clone();
            for (_, path) in segs {
                if path != active {
                    let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                    if std::fs::remove_file(&path).is_ok() {
                        report.wal_segments_removed += 1;
                        report.bytes_freed += bytes;
                    }
                }
            }
        }
        Ok(report)
    }

    /// Reclaim retiring runs gated on the unified version-retention floor
    /// (S1C-004). The floor — [`Table::version_gc_floor`] — is the oldest
    /// epoch held by ANY pin source: the oldest active transaction snapshot,
    /// the configured history-retention window, and every pin registered in
    /// the table's [`crate::retention::PinRegistry`] (backup/PITR,
    /// replication, cursor/read-generation, online-index-build). A retiring
    /// run is reaped only when its `retire_epoch` is at or below that floor,
    /// so no pin source can lose versions it still reads.
    ///
    /// The `Database`-level backup run-file pins (`backup_pins`) are still
    /// consulted by the `Database` GC/checkpoint paths; merging that
    /// mechanism into the [`crate::retention::PinRegistry`] is a documented
    /// follow-up.
    pub fn gc_versions(&mut self) -> Result<GcVersionsReport> {
        let floor = self.version_gc_floor();
        let runs_reaped = self.reap_retiring(floor, &std::collections::HashSet::new())?;
        Ok(GcVersionsReport { floor, runs_reaped })
    }

    /// Verify every manifest-referenced run's footer checksum.
    pub fn check(&self) -> Result<CheckReport> {
        let mut report = CheckReport::default();
        for rr in self.run_refs() {
            report.runs_checked += 1;
            let path = self.run_path(rr.run_id as u64);
            match read_header(&path) {
                Ok(_) => report.runs_ok += 1,
                Err(e) => report.issues.push(format!("run {}: {e}", rr.run_id)),
            }
        }
        Ok(report)
    }

    /// Best-effort repair: drop any manifest-referenced run that fails its
    /// checksum, so the table reopens in a consistent (if lossy) state.
    pub fn doctor(&mut self) -> Result<DoctorReport> {
        let mut report = DoctorReport::default();
        let live: Vec<u128> = self.run_refs().iter().map(|r| r.run_id).collect();
        let mut kept: Vec<crate::manifest::RunRef> = Vec::new();
        for id in live {
            let path = self.run_path(id as u64);
            if read_header(&path).is_ok() {
                if let Some(rr) = self.run_refs().iter().find(|r| r.run_id == id).cloned() {
                    kept.push(rr);
                }
            } else {
                report.runs_dropped.push(id);
            }
        }
        self.set_run_refs(kept);
        self.persist_manifest(self.current_epoch())?;
        // If any run was dropped, the checkpoint now references row-ids whose
        // data is gone → invalidate it so reopen rebuilds from survivors.
        if !report.runs_dropped.is_empty() {
            self.invalidate_index_checkpoint();
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
    use crate::Value;
    use tempfile::tempdir;

    fn schema() -> Schema {
        Schema {
            schema_id: 1,
            columns: vec![ColumnDef {
                id: 1,
                name: "v".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            }],
            indexes: Vec::new(),
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        }
    }

    #[test]
    fn gc_removes_orphan_runs_and_old_wal_segments() {
        let dir = tempdir().unwrap();
        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
        db.set_mutable_run_spill_bytes(1); // spill every flush to a run
        db.put(vec![(1, Value::Int64(1))]).unwrap();
        db.flush().unwrap(); // run 1 + rotated WAL
                             // Orphan run file.
        std::fs::write(dir.path().join("_runs").join("r-777.sr"), b"junk").unwrap();

        let report = db.gc().unwrap();
        assert!(report.runs_removed >= 1, "orphan run removed");
        assert!(!dir.path().join("_runs").join("r-777.sr").exists());

        // check should still pass for the live run.
        let check = db.check().unwrap();
        assert_eq!(check.runs_checked, 1);
        assert!(check.issues.is_empty(), "{:?}", check.issues);
    }

    #[test]
    fn check_flags_a_corrupt_run() {
        let dir = tempdir().unwrap();
        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
        db.set_mutable_run_spill_bytes(1); // spill every flush to a run
        db.put(vec![(1, Value::Int64(1))]).unwrap();
        db.flush().unwrap();
        // Corrupt the live run.
        let live = dir.path().join("_runs").join("r-1.sr");
        let mut bytes = std::fs::read(&live).unwrap();
        bytes[300] ^= 0xFF;
        std::fs::write(&live, bytes).unwrap();

        let check = db.check().unwrap();
        assert_eq!(check.runs_checked, 1);
        assert!(!check.issues.is_empty(), "corruption must be flagged");
    }

    /// S1C-004: every pin source blocks version GC while held; reclamation
    /// proceeds once the pin is released.
    #[test]
    fn gc_versions_blocked_by_each_pin_source_and_reclaims_after_release() {
        use crate::epoch::Epoch;
        use crate::retention::PinSource;

        let dir = tempdir().unwrap();
        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
        db.set_mutable_run_spill_bytes(1);
        db.put(vec![(1, Value::Int64(1))]).unwrap();
        db.flush().unwrap(); // run 1
        db.put(vec![(1, Value::Int64(2))]).unwrap();
        db.flush().unwrap(); // run 2
        db.compact().unwrap(); // merged run 3; runs 1+2 retiring at epoch 2
        assert_eq!(db.run_count(), 1);
        assert_eq!(db.current_epoch(), Epoch(2));

        for source in PinSource::ALL {
            let guard = db.pin_registry().pin(source, Epoch(1));
            assert_eq!(
                db.version_gc_floor(),
                Epoch(1),
                "{source:?} pin must lower the reclamation floor"
            );
            let report = db.gc_versions().unwrap();
            assert_eq!(report.floor, Epoch(1));
            assert_eq!(
                report.runs_reaped, 0,
                "{source:?} pin must block retiring-run reclamation"
            );
            drop(guard);
        }

        // No pins left: the floor returns to the visible epoch (2), which is
        // at the retire epoch — both superseded runs are reclaimed.
        assert_eq!(db.version_gc_floor(), Epoch(2));
        let report = db.gc_versions().unwrap();
        assert_eq!(report.runs_reaped, 2, "all retiring runs reaped");
    }

    /// S1C-004 diagnostics: local snapshot pins project as the transaction
    /// source; registered pins appear with their own source and epoch.
    #[test]
    fn version_pins_report_lists_registered_and_projected_sources() {
        use crate::epoch::Epoch;
        use crate::retention::PinSource;

        let dir = tempdir().unwrap();
        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
        db.put(vec![(1, Value::Int64(1))]).unwrap();
        db.commit().unwrap();
        assert!(db.version_pins_report().is_empty());

        let snapshot = db.pin_snapshot();
        let backup = db.pin_registry().pin(PinSource::BackupPitr, Epoch(0));
        let report = db.version_pins_report();
        assert_eq!(report.len(), 2);
        let transaction = report.get(PinSource::TransactionSnapshot).unwrap();
        assert_eq!(transaction.oldest_epoch, snapshot.epoch);
        assert_eq!(transaction.pin_count, 0, "projection carries no guard");
        let backup_info = report.get(PinSource::BackupPitr).unwrap();
        assert_eq!(backup_info.oldest_epoch, Epoch(0));
        assert_eq!(backup_info.pin_count, 1);
        assert_eq!(report.oldest_epoch(), Some(Epoch(0)));

        drop(backup);
        db.unpin_snapshot(snapshot);
        assert!(db.version_pins_report().is_empty());
    }
}
