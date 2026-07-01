//! Tiered compaction (Phase 5) with snapshot retention.
//!
//! Merges all sorted runs into one, dropping superseded versions and tombstones
//! — but preserving the version each pinned read snapshot still needs. Identical
//! re-encoded pages reuse their content hash, so the page cache keeps hitting.

use crate::engine::Table;
use crate::epoch::Epoch;
use crate::manifest::RunRef;
use crate::memtable::Row;
use crate::sorted_run::RunWriter;
use crate::Result;
use std::collections::HashMap;

impl Table {
    /// Merge all runs into a single level-1 run, dropping superseded versions
    /// and tombstones — **but preserving** the version each pinned snapshot
    /// still needs. No-op if there are fewer than two runs.
    pub fn compact(&mut self) -> Result<()> {
        if self.run_refs().len() < 2 {
            return Ok(());
        }
        let min_active = self.min_active_snapshot();
        let old_refs: Vec<RunRef> = self.run_refs().to_vec();

        // Fold the mutable-run tier into the compaction input (Phase 11.1) so
        // its rows are merged and rewritten to the output run. Draining here is
        // safe: compaction does not rotate the WAL, so crash recovery replays
        // those rows back into the memtable (the tier rebuilds from replay).
        let mutable_rows = if self.mutable_run_len() > 0 {
            self.drain_mutable_run()
        } else {
            Vec::new()
        };

        // Gather every version of every row across all runs.
        let mut all: HashMap<u64, Vec<Row>> = HashMap::new();
        for rr in &old_refs {
            let mut reader = self.open_reader(rr.run_id)?;
            for row in reader.all_rows()? {
                all.entry(row.row_id.0).or_default().push(row);
            }
        }
        // Merge the mutable-run tier's drained versions on top.
        for row in mutable_rows {
            all.entry(row.row_id.0).or_default().push(row);
        }

        let mut rows: Vec<Row> = Vec::new();
        for (_, mut vers) in all {
            vers.sort_by_key(|r| r.committed_epoch);
            rows.extend(select_keep(&vers, min_active));
        }
        rows.sort_by_key(|r| (r.row_id, r.committed_epoch));

        // Recompute the live-row counter from the merged survivors.
        self.live_count = rows.iter().filter(|r| !r.deleted).count() as u64;

        let retire_epoch = self.current_epoch().0;
        if rows.is_empty() {
            // Point the manifest at the empty run set and enqueue the superseded
            // runs for retention-gated deletion (spec §6.4) — `gc()` deletes them
            // once no pinned snapshot can still need them. Persisting before any
            // unlink also keeps a concurrent `check`/`doctor` from ever seeing a
            // RunRef whose file is already gone.
            self.set_run_refs(Vec::new());
            for rr in &old_refs {
                self.retire_run(rr.run_id, retire_epoch);
            }
            self.persist_manifest(self.current_epoch())?;
            // No live rows remain; the in-memory indexes are stale → drop the
            // checkpoint so reopen rebuilds (empty) instead of loading it.
            self.invalidate_index_checkpoint();
            return Ok(());
        }

        let run_id = self.next_run_id();
        self.bump_next_run_id();
        let path = self.run_path(run_id);
        let kek = self.kek();
        let mut writer = RunWriter::new(self.schema(), run_id as u128, self.current_epoch(), 1)
            .clean(min_active.is_none())
            .with_zstd_level(self.compaction_zstd_level());
        if let Some(k) = &kek {
            writer = writer.with_encryption(k.as_ref(), self.indexable_column_specs());
        }
        let header = writer.write(&path, &rows)?;

        // Point the manifest at the new run and enqueue the superseded runs for
        // retention-gated deletion (spec §6.4): `gc()` deletes their files once
        // `min_active_snapshot` passes this compaction epoch, so a reader pinned
        // below it keeps a consistent on-disk view. Persisting the manifest
        // (with both the new RunRef and the `retiring` queue) BEFORE any unlink
        // also means a concurrent `check`/`doctor` never sees a RunRef whose file
        // is gone, and the retired files are tracked (not orphans) across reopen.
        self.set_run_refs(vec![RunRef {
            run_id: run_id as u128,
            level: 1,
            epoch_created: header.epoch_created,
            row_count: header.row_count,
        }]);
        for rr in &old_refs {
            self.retire_run(rr.run_id, retire_epoch);
        }
        self.persist_manifest(self.current_epoch())?;
        // Compaction yields exactly one run → (re)build the learned-range PGMs
        // so the checkpoint captures them (otherwise reopen would load a
        // checkpoint with empty learned_range and fall back to page-pruned scans).
        self.build_learned_ranges()?;
        self.clear_result_cache();
        self.checkpoint_indexes(self.current_epoch());
        Ok(())
    }
}

/// Versions to keep for one `RowId` given the oldest pinned snapshot. Always
/// keeps the newest version; if snapshots are active, also keeps the newest
/// version `<= min_active` (the one the oldest snapshot sees). Tombstones are
/// only dropped when no snapshot is active.
fn select_keep(vers: &[Row], min_active: Option<Epoch>) -> Vec<Row> {
    let newest = vers.last().expect("at least one version").clone();
    match min_active {
        None => {
            if newest.deleted {
                Vec::new()
            } else {
                vec![newest]
            }
        }
        Some(min_e) => {
            let mut keep = vec![newest.clone()];
            // Newest version visible to the oldest snapshot.
            if let Some(v) = vers.iter().rev().find(|v| v.committed_epoch <= min_e) {
                if v.committed_epoch != newest.committed_epoch {
                    keep.push(v.clone());
                }
            }
            keep
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
    use crate::{Snapshot, Value};
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
    fn _snapshot_import_used() {
        let _ = Snapshot::at(Epoch(0));
    }
}
