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
        let live: HashSet<u128> = self.run_refs().iter().map(|r| r.run_id).collect();

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
}
