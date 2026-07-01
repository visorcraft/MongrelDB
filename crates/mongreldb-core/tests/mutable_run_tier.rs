//! Phase 11.1 — PMA mutable-run LSM tier integration tests.
//!
//! Verifies that `flush()` coalesces memtable drains into the in-memory
//! mutable-run tier (no `.sr` run written until the spill watermark is
//! crossed), that reads still merge the tier with the memtable and runs under
//! MVCC, that crossing the watermark spills a single coalesced run, and that
//! crash recovery rebuilds the tier from WAL replay.

use mongreldb_core::{
    schema::{ColumnDef, ColumnFlags, Schema, TypeId},
    RowId, Snapshot, Table, Value,
};
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
                name: "v".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![],
        colocation: vec![], constraints: Default::default(),
    }
}

fn put(db: &mut Table, id: i64, v: i64) -> RowId {
    db.put(vec![(1, Value::Int64(id)), (2, Value::Int64(v))])
        .unwrap()
}

#[test]
fn flush_coalesces_into_mutable_run_without_writing_a_run() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    // Default 8 MiB spill threshold: a handful of small rows stays in memory.
    let r0 = put(&mut db, 0, 100);
    let r1 = put(&mut db, 1, 101);
    db.flush().unwrap();
    assert_eq!(
        db.run_count(),
        0,
        "no run should be written below the spill watermark"
    );
    assert!(db.memtable_len() == 0, "memtable drained into the tier");
    assert!(
        db.mutable_run_len() >= 2,
        "rows live in the mutable-run tier"
    );

    // Reads still resolve — merged across the tier.
    let snap = db.snapshot();
    assert!(matches!(
        db.get(r0, snap).unwrap().columns.get(&2),
        Some(Value::Int64(100))
    ));
    assert!(matches!(
        db.get(r1, snap).unwrap().columns.get(&2),
        Some(Value::Int64(101))
    ));
    assert_eq!(db.visible_rows(snap).unwrap().len(), 2);
}

#[test]
fn multiple_flushes_coalesce_into_one_run_on_spill() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    // Tiny threshold: each flush spills. Used here to confirm that the spill
    // path still produces a correct, queryable run.
    db.set_mutable_run_spill_bytes(1);
    for i in 0..10 {
        put(&mut db, i, i * 10);
        db.flush().unwrap();
    }
    assert_eq!(db.run_count(), 10);
    let snap = db.snapshot();
    assert_eq!(db.visible_rows(snap).unwrap().len(), 10);

    // Now flip to a large threshold and confirm many flushes coalesce into a
    // single tier (no new runs) until the spill fires.
    db.set_mutable_run_spill_bytes(u64::MAX);
    for i in 10..20 {
        put(&mut db, i, i * 10);
        db.flush().unwrap();
    }
    assert_eq!(
        db.run_count(),
        10,
        "coalesced flushes add no runs below watermark"
    );
    assert!(db.mutable_run_len() >= 10);
    let snap = db.snapshot();
    assert_eq!(
        db.visible_rows(snap).unwrap().len(),
        20,
        "all 20 rows visible"
    );
}

#[test]
fn mvcc_merge_across_memtable_tier_and_run() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    // Row lives in a run.
    db.set_mutable_run_spill_bytes(1);
    let r = put(&mut db, 1, 10);
    db.flush().unwrap(); // → run
                         // Update lands in the tier (coalesced).
    db.set_mutable_run_spill_bytes(u64::MAX);
    db.delete(r).unwrap();
    db.flush().unwrap();
    let r2 = put(&mut db, 2, 20);
    db.flush().unwrap();

    // Current snapshot: r deleted, row 2 live.
    let snap = db.snapshot();
    assert!(
        db.get(r, snap).is_none(),
        "deleted row hidden by the tier's tombstone"
    );
    assert!(db.get(r2, snap).is_some());
    assert!(matches!(
        db.get(r2, snap).unwrap().columns.get(&2),
        Some(Value::Int64(20))
    ));
    assert_eq!(db.visible_rows(snap).unwrap().len(), 1);
}

#[test]
fn reopen_rebuilds_unflushed_tier_from_wal_replay() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut db = Table::create(&path, schema(), 1).unwrap();
        // Default threshold: flush coalesces into the tier, WAL is NOT rotated.
        put(&mut db, 0, 5);
        put(&mut db, 1, 6);
        db.flush().unwrap();
        assert_eq!(db.run_count(), 0, "data is only in the tier + WAL");
    }
    // Reopen: the in-memory tier is empty, but recovery replays the unrotated
    // WAL into the memtable, so the rows are still present.
    let db = Table::open(&path).unwrap();
    assert_eq!(db.run_count(), 0);
    assert_eq!(db.count(), 2);
    let snap = db.snapshot();
    assert_eq!(db.visible_rows(snap).unwrap().len(), 2);
    assert!(matches!(
        db.get(RowId(0), snap).unwrap().columns.get(&2),
        Some(Value::Int64(5))
    ));
    let _ = Snapshot::at(db.current_epoch());
}

/// Regression for Phase 16.3b `rows_for_rids` single-run path: a fresh insert
/// (overlay/memtable-only, not in the run) must resolve from the overlay, and a
/// run-resident rid must resolve from the run via the decode-once + binary-
/// search path. Also pins that a tombstoned overlay rid is dropped.
#[test]
fn rows_for_rids_overlay_shadows_run_and_drops_tombstones() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    // One bulk-loaded run with a known PK (rid 0).
    db.bulk_load(vec![vec![(1, Value::Int64(7)), (2, Value::Int64(70))]])
        .unwrap();
    assert_eq!(db.run_count(), 1);
    let snap = db.snapshot();
    // The bulk-loaded row resolves from the run (not in the overlay).
    let from_run = db.rows_for_rids(&[0], snap).unwrap();
    assert_eq!(from_run.len(), 1);
    assert!(matches!(
        from_run[0].columns.get(&2),
        Some(Value::Int64(70))
    ));

    // A fresh insert lives only in the memtable overlay and must resolve there.
    let r_new = put(&mut db, 9, 99);
    db.commit().unwrap();
    let snap = db.snapshot();
    let from_overlay = db.rows_for_rids(&[r_new.0], snap).unwrap();
    assert_eq!(from_overlay.len(), 1);
    assert!(matches!(
        from_overlay[0].columns.get(&2),
        Some(Value::Int64(99))
    ));

    // Delete the fresh row → tombstone in the overlay → rows_for_rids drops it.
    db.delete(r_new).unwrap();
    db.commit().unwrap();
    let snap = db.snapshot();
    assert_eq!(db.rows_for_rids(&[r_new.0], snap).unwrap().len(), 0);
}
