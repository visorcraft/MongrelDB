//! Tiered compaction (Phase 5) with snapshot retention.
//!
//! Merges all sorted runs into one, dropping superseded versions and tombstones
//! — but preserving the version each pinned read snapshot still needs. Identical
//! re-encoded pages reuse their content hash, so the page cache keeps hitting.

use crate::engine::Table;
use crate::epoch::{Epoch, MaintenanceReceipt};
use crate::manifest::RunRef;
use crate::memtable::Row;
use crate::sorted_run::RunWriter;
use crate::{ExecutionControl, MongrelError, Result};
use std::collections::HashMap;
use std::path::Path;

impl Table {
    /// Background-compaction run-count threshold (§5.9). When a table accumulates
    /// at least this many sorted runs, every multi-run query pays decode work
    /// proportional to the run count; `maybe_compact` collapses them back to one.
    /// Conservative so a steady write stream doesn't compact too eagerly.
    pub const AUTO_COMPACT_RUN_THRESHOLD: usize = 8;

    /// Whether this table would benefit from compaction right now — the
    /// query-cost signal for §5.9. Pure run-count topology (no per-query
    /// bookkeeping): once runs have accumulated past the threshold, scans and
    /// pushdown queries are paying multi-run fallback cost, so compaction is
    /// worthwhile. A daemon (or any long-lived holder) polls this.
    pub fn should_compact(&self) -> bool {
        self.run_refs().len() >= Self::AUTO_COMPACT_RUN_THRESHOLD
            || (self.ttl().is_some()
                && !self.run_refs().is_empty()
                && self.has_expired_run_rows().unwrap_or(false))
    }

    fn has_expired_run_rows(&self) -> Result<bool> {
        self.has_expired_run_rows_inner(None)
    }

    fn has_expired_run_rows_inner(&self, control: Option<&ExecutionControl>) -> Result<bool> {
        let now_nanos = crate::engine::unix_nanos_now();
        for (run_index, run) in self.run_refs().iter().enumerate() {
            if run_index % 256 == 0 {
                if let Some(control) = control {
                    control.checkpoint()?;
                }
            }
            let mut reader = self.open_reader(run.run_id)?;
            for (row_index, row) in reader.all_rows()?.iter().enumerate() {
                if row_index % 256 == 0 {
                    if let Some(control) = control {
                        control.checkpoint()?;
                    }
                }
                if self.row_expired_at(row, now_nanos) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Compaction as a query optimization (§5.9): if [`should_compact`] reports
    /// that runs have accumulated past the cost threshold, run [`compact`] and
    /// return `true`; otherwise no-op and return `false`. Safe to call
    /// periodically from a background task — [`compact`] is itself a no-op below
    /// two runs and honors snapshot retention. Returns whether a compaction ran.
    pub fn maybe_compact(&mut self) -> Result<bool> {
        if !self.should_compact() {
            return Ok(false);
        }
        self.compact()?;
        Ok(true)
    }

    /// Merge all runs into a single level-1 run, dropping superseded versions
    /// and tombstones — **but preserving** the version each pinned snapshot
    /// still needs. No-op if there are fewer than two runs, unless a one-run
    /// TTL table has expired payloads to reclaim.
    pub fn compact(&mut self) -> Result<()> {
        let control = ExecutionControl::new(None);
        self.compact_controlled(&control, || true).map(|_| ())
    }

    /// Build replacement output cooperatively. Live state is unchanged until
    /// `before_publish` succeeds. No cancellation checkpoint runs after that
    /// manifest-publication boundary.
    #[doc(hidden)]
    pub fn compact_controlled<F>(
        &mut self,
        control: &ExecutionControl,
        before_publish: F,
    ) -> Result<bool>
    where
        F: FnOnce() -> bool,
    {
        self.compact_controlled_with_receipt(control, before_publish)
            .map(|(changed, _)| changed)
    }

    /// Build replacement output cooperatively and return the exact table
    /// snapshot used by the compaction. No receipt is returned for a no-op.
    #[doc(hidden)]
    pub fn compact_controlled_with_receipt<F>(
        &mut self,
        control: &ExecutionControl,
        before_publish: F,
    ) -> Result<(bool, Option<MaintenanceReceipt>)>
    where
        F: FnOnce() -> bool,
    {
        control.checkpoint()?;
        let maintenance_epoch = self.current_epoch();
        let reclaim_ttl = self.ttl().is_some() && self.has_expired_run_rows_inner(Some(control))?;
        if self.run_refs().len() < 2 && !reclaim_ttl {
            return Ok((false, None));
        }
        let min_active = self.min_active_snapshot();
        let old_refs: Vec<RunRef> = self.run_refs().to_vec();
        let now_nanos = crate::engine::unix_nanos_now();
        let mutable_rows = if self.mutable_run_len() > 0 {
            self.snapshot_mutable_run()
        } else {
            Vec::new()
        };

        let mut all: HashMap<u64, Vec<Row>> = HashMap::new();
        let mut scanned = 0_usize;
        for rr in &old_refs {
            control.checkpoint()?;
            let mut reader = self.open_reader(rr.run_id)?;
            for row in reader.all_rows()? {
                if scanned.is_multiple_of(256) {
                    control.checkpoint()?;
                }
                scanned += 1;
                all.entry(row.row_id.0).or_default().push(row);
            }
        }
        for row in mutable_rows {
            if scanned.is_multiple_of(256) {
                control.checkpoint()?;
            }
            scanned += 1;
            all.entry(row.row_id.0).or_default().push(row);
        }

        let mut rows = Vec::new();
        let mut current_live_count = 0u64;
        for (row_index, (_, mut versions)) in all.into_iter().enumerate() {
            if row_index % 256 == 0 {
                control.checkpoint()?;
            }
            versions.sort_by_key(|row| row.committed_epoch);
            let Some(newest) = versions.last() else {
                continue;
            };
            let newest_epoch = newest.committed_epoch;
            if !newest.deleted && !self.row_expired_at(newest, now_nanos) {
                current_live_count += 1;
            }
            for row in select_keep(&versions, min_active) {
                if self.row_expired_at(&row, now_nanos) {
                    if row.committed_epoch == newest_epoch
                        && min_active.is_some_and(|epoch| newest_epoch > epoch)
                    {
                        let mut tombstone = row;
                        tombstone.deleted = true;
                        tombstone.columns.clear();
                        rows.push(tombstone);
                    }
                } else {
                    rows.push(row);
                }
            }
        }
        rows.sort_by_key(|row| (row.row_id, row.committed_epoch));

        let mut staged_run = None;
        if !rows.is_empty() {
            let run_id = self.alloc_run_id()?;
            let final_name = format!("r-{run_id}.sr");
            let stage_name = format!(
                "{final_name}.compact-stage-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            );
            let kek = self.kek();
            let mut writer = RunWriter::new(self.schema(), run_id as u128, maintenance_epoch, 1)
                .clean(min_active.is_none())
                .with_zstd_level(self.compaction_zstd_level());
            if let Some(k) = &kek {
                writer = writer.with_encryption(k.as_ref(), self.indexable_column_specs());
            }
            let header = match self.create_run_entry(Path::new(&stage_name))? {
                Some(file) => writer.write_file(file, &rows),
                None => writer.write(self.runs_dir().join(&stage_name), &rows),
            };
            let header = match header {
                Ok(header) => header,
                Err(error) => {
                    let _ = self.remove_run_entry(Path::new(&stage_name));
                    return Err(error);
                }
            };
            staged_run = Some((
                stage_name,
                final_name,
                RunRef {
                    run_id: run_id as u128,
                    level: 1,
                    epoch_created: header.epoch_created,
                    row_count: header.row_count,
                },
            ));
        }

        if let Err(error) = control.checkpoint() {
            if let Some((stage_name, _, _)) = staged_run {
                let _ = self.remove_run_entry(Path::new(&stage_name));
            }
            return Err(error);
        }
        if !before_publish() {
            if let Some((stage_name, _, _)) = staged_run {
                let _ = self.remove_run_entry(Path::new(&stage_name));
            }
            return Err(MongrelError::Cancelled);
        }

        // Publish the staged file before changing any live topology. A failed
        // rename or directory sync can leave only an unreferenced run file;
        // the old manifest, mutable run, and in-memory refs remain intact.
        let replacement_ref = if let Some((stage_name, final_name, staged_ref)) = staged_run {
            self.publish_run_entry(Path::new(&stage_name), Path::new(&final_name))?;
            Some(staged_ref)
        } else {
            None
        };

        if self.mutable_run_len() > 0 {
            self.drain_mutable_run();
        }
        self.live_count = current_live_count;
        let retire_epoch = maintenance_epoch.0;
        if let Some(replacement_ref) = replacement_ref {
            self.set_run_refs(vec![replacement_ref]);
            for run in &old_refs {
                self.retire_run(run.run_id, retire_epoch);
            }
        } else {
            self.set_run_refs(Vec::new());
            for run in &old_refs {
                self.retire_run(run.run_id, retire_epoch);
            }
        }

        // The new manifest must explicitly reject the old global-index
        // checkpoint.  Compaction does not advance the data epoch, so leaving
        // the old epoch stamped here could make reopen accept indexes for the
        // superseded runs.
        self.prepare_indexes_for_run_replacement();
        if let Err(error) = self.persist_manifest(maintenance_epoch) {
            self.poison_after_maintenance_publish_failure();
            return Err(MongrelError::CommitOutcomeUnknown {
                epoch: maintenance_epoch.0,
                message: format!("compaction manifest publication failed: {error}"),
            });
        }
        self.clear_result_cache();
        self.bump_data_generation();

        // The replacement topology is durable.  Index rebuild failures do not
        // make row data uncertain: keep the indexes marked incomplete so the
        // next indexed operation or reopen rebuilds them from the new runs.
        if let Err(error) = self
            .rebuild_indexes_from_runs()
            .and_then(|_| self.build_learned_ranges())
        {
            return Err(MongrelError::DurableCommit {
                epoch: maintenance_epoch.0,
                message: format!("compaction committed but index rebuild failed: {error}"),
            });
        }
        self.finish_indexes_for_run_replacement();
        self.checkpoint_indexes(maintenance_epoch);
        Ok((
            true,
            Some(MaintenanceReceipt {
                epoch: maintenance_epoch,
            }),
        ))
    }
}

/// Versions to keep for one `RowId` given the oldest queryable snapshot. Every
/// transition at or after the floor is retained, plus the boundary version
/// visible at the floor. With no floor, only the current live version remains.
fn select_keep(vers: &[Row], min_active: Option<Epoch>) -> Vec<Row> {
    let Some(newest) = vers.last().cloned() else {
        return Vec::new();
    };
    match min_active {
        None => {
            if newest.deleted {
                Vec::new()
            } else {
                vec![newest]
            }
        }
        Some(min_e) => {
            let recent_start = vers.partition_point(|row| row.committed_epoch < min_e);
            let mut keep = vers[recent_start..].to_vec();
            if recent_start > 0 && keep.first().is_none_or(|row| row.committed_epoch > min_e) {
                let boundary = vers[recent_start - 1].clone();
                if keep.is_empty() && boundary.deleted {
                    return Vec::new();
                }
                keep.insert(0, boundary);
            }
            keep
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
    use crate::{Database, Snapshot, Value};
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
    fn compaction_merges_runs_and_gcs_tombstoned_row() {
        let dir = tempdir().unwrap();
        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
        // Spill every flush to a run (this test exercises run-level merging).
        db.set_mutable_run_spill_bytes(1);
        let mut ids = Vec::new();
        for i in 1..=5i64 {
            ids.push(db.put(vec![(1, Value::Int64(i))]).unwrap());
        }
        db.flush().unwrap();
        db.delete(ids[2]).unwrap();
        db.flush().unwrap();
        db.put(vec![(1, Value::Int64(60))]).unwrap();
        db.flush().unwrap();
        assert_eq!(db.run_count(), 3);

        db.compact().unwrap();
        assert_eq!(db.run_count(), 1);
        let rows = db.visible_rows(db.snapshot()).unwrap();
        let row_ids: Vec<u64> = rows.iter().map(|r| r.row_id.0).collect();
        assert!(!row_ids.contains(&ids[2].0), "tombstoned row must be GC'd");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn pinned_snapshot_survives_compaction() {
        let dir = tempdir().unwrap();
        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
        let r = db.put(vec![(1, Value::Int64(1))]).unwrap();
        db.flush().unwrap(); // run 1: live version of r

        // Pin a snapshot that sees the live version.
        let pinned = db.pin_snapshot();
        assert_eq!(
            db.get(r, pinned)
                .and_then(|row| row.columns.get(&1).cloned()),
            Some(Value::Int64(1))
        );

        // Delete r, flush a second run, then compact with the pin still active.
        db.delete(r).unwrap();
        db.commit().unwrap();
        db.flush().unwrap(); // run 2: tombstone for r
        db.compact().unwrap(); // merges run1 + run2

        // Pinned snapshot must still see the live version (retention kept it).
        assert_eq!(
            db.get(r, pinned)
                .and_then(|row| row.columns.get(&1).cloned()),
            Some(Value::Int64(1))
        );
        // Current snapshot sees the tombstone → row is gone.
        assert_eq!(
            db.get(r, db.snapshot())
                .and_then(|row| row.columns.get(&1).cloned()),
            None
        );

        // Release the pin; the next compaction may GC the live version.
        db.unpin_snapshot(pinned);
        db.compact().unwrap();
        assert_eq!(
            db.get(r, db.snapshot())
                .and_then(|row| row.columns.get(&1).cloned()),
            None
        );
    }

    #[test]
    fn controlled_compaction_cancel_before_publish_preserves_live_state() {
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path(), schema(), 1).unwrap();
        table.set_mutable_run_spill_bytes(1);
        table.put(vec![(1, Value::Int64(1))]).unwrap();
        table.flush().unwrap();
        table.put(vec![(1, Value::Int64(2))]).unwrap();
        table.flush().unwrap();
        let before_refs: Vec<_> = table
            .run_refs()
            .iter()
            .map(|run| (run.run_id, run.level, run.epoch_created, run.row_count))
            .collect();

        let error = table
            .compact_controlled(&ExecutionControl::new(None), || false)
            .unwrap_err();
        assert!(matches!(error, MongrelError::Cancelled));
        let after_refs: Vec<_> = table
            .run_refs()
            .iter()
            .map(|run| (run.run_id, run.level, run.epoch_created, run.row_count))
            .collect();
        assert_eq!(after_refs, before_refs);
        assert_eq!(table.visible_rows(table.snapshot()).unwrap().len(), 2);
        assert!(std::fs::read_dir(table.runs_dir())
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains("compact-stage")));
    }

    #[test]
    fn compaction_manifest_failure_poisons_standalone_table_until_reopen() {
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path(), schema(), 1).unwrap();
        table.set_mutable_run_spill_bytes(1);
        table.put(vec![(1, Value::Int64(1))]).unwrap();
        table.flush().unwrap();
        table.put(vec![(1, Value::Int64(2))]).unwrap();
        table.flush().unwrap();
        let manifest = dir.path().join(crate::manifest::MANIFEST_FILENAME);
        let saved_manifest = dir.path().join("_mf.saved");
        std::fs::rename(&manifest, &saved_manifest).unwrap();
        std::fs::create_dir(&manifest).unwrap();

        let error = table.compact().unwrap_err();
        assert!(matches!(error, MongrelError::CommitOutcomeUnknown { .. }));
        assert_eq!(table.visible_rows(table.snapshot()).unwrap().len(), 2);
        assert!(table
            .put(vec![(1, Value::Int64(3))])
            .unwrap_err()
            .to_string()
            .contains("reopen required"));

        drop(table);
        std::fs::remove_dir(&manifest).unwrap();
        std::fs::rename(saved_manifest, manifest).unwrap();
        let reopened = Table::open(dir.path()).unwrap();
        assert_eq!(reopened.visible_rows(reopened.snapshot()).unwrap().len(), 2);
    }

    #[test]
    fn compaction_manifest_failure_poisons_mounted_database() {
        let dir = tempdir().unwrap();
        let db = Database::create(dir.path()).unwrap();
        let table_id = db.create_table("items", schema()).unwrap();
        let handle = db.table("items").unwrap();
        {
            let mut table = handle.lock();
            table.set_mutable_run_spill_bytes(1);
            table.put(vec![(1, Value::Int64(1))]).unwrap();
            table.flush().unwrap();
            table.put(vec![(1, Value::Int64(2))]).unwrap();
            table.flush().unwrap();
        }
        let manifest = dir
            .path()
            .join("tables")
            .join(table_id.to_string())
            .join(crate::manifest::MANIFEST_FILENAME);
        let saved_manifest = manifest.with_extension("saved");
        std::fs::rename(&manifest, &saved_manifest).unwrap();
        std::fs::create_dir(&manifest).unwrap();

        let error = handle.lock().compact().unwrap_err();
        assert!(matches!(error, MongrelError::CommitOutcomeUnknown { .. }));
        assert!(db
            .create_table("blocked", schema())
            .unwrap_err()
            .to_string()
            .contains("database poisoned"));
    }

    #[test]
    fn _snapshot_import_used() {
        let _ = Snapshot::at(Epoch(0));
    }
}
