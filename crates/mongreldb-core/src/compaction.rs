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
        let now_nanos = crate::engine::unix_nanos_now();
        for run in self.run_refs() {
            let mut reader = self.open_reader(run.run_id)?;
            if reader
                .all_rows()?
                .iter()
                .any(|row| self.row_expired_at(row, now_nanos))
            {
                return Ok(true);
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
        let reclaim_ttl = self.ttl().is_some() && self.has_expired_run_rows()?;
        if self.run_refs().len() < 2 && !reclaim_ttl {
            return Ok(());
        }
        let min_active = self.min_active_snapshot();
        let old_refs: Vec<RunRef> = self.run_refs().to_vec();
        let now_nanos = crate::engine::unix_nanos_now();

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
        let mut expired_reclaimed = false;
        let mut current_live_count = 0u64;
        for (_, mut vers) in all {
            vers.sort_by_key(|r| r.committed_epoch);
            let newest = vers.last().expect("at least one version");
            let newest_epoch = newest.committed_epoch;
            if !newest.deleted && !self.row_expired_at(newest, now_nanos) {
                current_live_count += 1;
            }
            for row in select_keep(&vers, min_active) {
                if self.row_expired_at(&row, now_nanos) {
                    expired_reclaimed = true;
                    // If an older snapshot still needs a pre-expiry version,
                    // keep a payload-free tombstone at the newest epoch. Merely
                    // dropping the expired newest version could resurrect the
                    // older row for current readers after the rewrite.
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
        rows.sort_by_key(|r| (r.row_id, r.committed_epoch));

        // Count logical rows at the current epoch, not retained historical
        // versions (which may leave several non-deleted records per RowId).
        self.live_count = current_live_count;

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
            self.mark_indexes_incomplete();
            self.bump_data_generation();
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
        // Derived indexes must match the compacted live run. Otherwise removed
        // tombstones remain in a newly stamped checkpoint and reappear on open.
        self.rebuild_indexes_from_runs()?;
        // Compaction yields exactly one run → (re)build the learned-range PGMs
        // so the checkpoint captures them (otherwise reopen would load a
        // checkpoint with empty learned_range and fall back to page-pruned scans).
        self.build_learned_ranges()?;
        self.clear_result_cache();
        let _ = expired_reclaimed;
        self.checkpoint_indexes(self.current_epoch());
        self.bump_data_generation();
        Ok(())
    }
}

/// Versions to keep for one `RowId` given the oldest queryable snapshot. Every
/// transition at or after the floor is retained, plus the boundary version
/// visible at the floor. With no floor, only the current live version remains.
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
            let recent_start = vers.partition_point(|row| row.committed_epoch < min_e);
            let mut keep = vers[recent_start..].to_vec();
            if recent_start > 0 && keep.first().map_or(true, |row| row.committed_epoch > min_e) {
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
                default_value: None,
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
    fn _snapshot_import_used() {
        let _ = Snapshot::at(Epoch(0));
    }
}
