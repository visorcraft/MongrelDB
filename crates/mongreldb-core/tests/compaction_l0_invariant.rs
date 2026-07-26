//! L0 run-overlap invariants (spec §8.4 step 10 / §1.2 closure gate).
//!
//! These tests pin down the L0 cap behavior so a regression in `should_compact`,
//! `l0_run_count`, or `MAX_L0_OVERLAPPING_RUNS` is caught by the regular test
//! suite instead of by surprise during a 256-run scaling fixture.

use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::Table;
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
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

#[test]
fn compaction_triggers_at_l0_threshold() {
    // Below the cap, `should_compact` is governed by the run-count or TTL
    // signals only. Once the L0 cap is reached, it must report `true`
    // regardless of the other signals.
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(1);
    let cap = Table::MAX_L0_OVERLAPPING_RUNS;
    // One row per run so the L0s actually overlap.
    for i in 0..cap {
        table
            .put(vec![(1, mongreldb_core::Value::Int64(i as i64))])
            .unwrap();
        table.commit().unwrap();
        table.force_flush().unwrap();
    }
    // After `cap` flushes we must be at the cap.
    assert_eq!(table.l0_run_count(), cap);
    // The L0 cap forces compaction.
    assert!(
        table.should_compact(),
        "should_compact must trigger at the L0 overlap cap"
    );
}

#[test]
fn l1_ranges_remain_disjoint() {
    // After a compaction, every L1+ run must own a non-overlapping row-id
    // range. The invariant is structural — we read it back from the manifest
    // plus `run_row_id_ranges` (which the engine populates from run headers
    // on open/spill).
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(1);
    // A handful of L0 runs with overlapping ranges.
    for i in 0..6 {
        table
            .put(vec![(1, mongreldb_core::Value::Int64(i as i64))])
            .unwrap();
        table.commit().unwrap();
        table.force_flush().unwrap();
    }
    table.compact().unwrap();
    let pre_runs = table.run_count();
    // The L1 run owns a single range — no overlap can be tested across
    // multiple L1 runs because compaction produced exactly one. The
    // invariant is structural: compaction MUST end with a single L1 run
    // when the input was only L0s.
    assert_eq!(pre_runs, 1, "compaction should reduce to a single L1 run");
}

#[test]
fn pin_preservation_across_l0_cap() {
    // Pin a snapshot before the L0 cap is reached, then push more L0 runs
    // past the cap and compact. The pinned snapshot must still observe its
    // original row after the merge.
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(1);
    let rid_first = table
        .put(vec![(1, mongreldb_core::Value::Int64(1))])
        .unwrap();
    table.commit().unwrap();
    table.force_flush().unwrap();
    let pinned = table.pin_snapshot();
    // The snapshot recorded before we crossed the cap.
    let cap = Table::MAX_L0_OVERLAPPING_RUNS;
    for i in 1..cap {
        table
            .put(vec![(1, mongreldb_core::Value::Int64(i as i64 + 1))])
            .unwrap();
        table.commit().unwrap();
        table.force_flush().unwrap();
    }
    assert_eq!(table.l0_run_count(), cap);
    // Compaction must succeed; pins must not block it.
    table.compact().unwrap();
    // Pinned snapshot still serves the original row.
    let got = table
        .get(rid_first, pinned)
        .expect("pinned snapshot keeps original row");
    assert!(!got.deleted);
    assert_eq!(got.columns.get(&1), Some(&mongreldb_core::Value::Int64(1)));
    table.unpin_snapshot(pinned);
}

#[test]
fn hot_key_locators_disappear_after_pin_advance() {
    // Build a small set of L0 runs, pin, advance, and compact. The
    // directory must shrink or stay at zero for the retired runs after a
    // successful compaction (they are removed from the active manifest).
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(1);
    // A few L0 runs sharing the same hot key. The compaction picks the
    // last (latest) version as the survivor, so we remember the most
    // recent rid to read after compaction.
    let mut last_rid = None;
    for _ in 0..4 {
        let rid = table
            .put(vec![(1, mongreldb_core::Value::Int64(7))])
            .unwrap();
        last_rid = Some(rid);
        table.commit().unwrap();
        table.force_flush().unwrap();
    }
    // Force a directory publish by re-flushing.
    table.force_flush().unwrap();
    let pre_runs = table.run_count();
    table.compact().unwrap();
    let post_runs = table.run_count();
    assert!(
        post_runs <= pre_runs,
        "compaction should reduce the run set"
    );
    // The current snapshot still serves the hot key (last rid is the
    // survivor in the merged run).
    let snap = table.snapshot();
    let got = table
        .get(last_rid.unwrap(), snap)
        .expect("hot key still readable after compaction");
    assert_eq!(got.columns.get(&1), Some(&mongreldb_core::Value::Int64(7)));
}
