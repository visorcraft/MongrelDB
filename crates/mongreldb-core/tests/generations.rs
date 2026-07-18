//! S1C — immutable read and index generations plus version-GC pins (spec
//! §10.3, S1C-001..004). Covers: readers pinning stable generations under
//! writes, atomic publish swaps, per-source version-GC pins, unified pin
//! diagnostics, ANN base+delta stability through published index generations,
//! and compaction merging deltas into a new base.

use mongreldb_core::epoch::Epoch;
use mongreldb_core::retention::PinSource;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Database, Snapshot, Table, TableHandle, Value};
use std::sync::Arc;
use tempfile::tempdir;

fn pk_schema() -> Schema {
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

/// PK plus a payload column, so re-putting the same PK creates a new,
/// observably different *version* of one row (MVCC), not a new row.
fn versioned_schema() -> Schema {
    let column = |id: u16, name: &str, primary_key: bool| ColumnDef {
        id,
        name: name.into(),
        ty: TypeId::Int64,
        flags: if primary_key {
            ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY)
        } else {
            ColumnFlags::empty().with(ColumnFlags::NULLABLE)
        },
        default_value: None,
        embedding_source: None,
    };
    Schema {
        schema_id: 1,
        columns: vec![column(1, "id", true), column(2, "payload", false)],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn ann_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "embedding".into(),
                ty: TypeId::Embedding { dim: 8 },
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "ann".into(),
            column_id: 2,
            kind: IndexKind::Ann,
            predicate: None,
            options: Default::default(),
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

/// S1C-001: a pinned read generation observes a stable table while the
/// writer keeps committing; a fresh generation sees the new state.
#[test]
fn pinned_read_generations_are_stable_across_writes() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    for v in 0..10 {
        table.put(vec![(1, Value::Int64(v))]).unwrap();
    }
    table.commit().unwrap();
    let handle = TableHandle::from_table(table);

    let (gen1, snap1) = handle.read_generation_with_context(None).unwrap();
    assert_eq!(gen1.visible_rows(snap1).unwrap().len(), 10);

    // Put-heavy workload while the generation is pinned.
    {
        let mut writer = handle.lock();
        for v in 10..210 {
            writer.put(vec![(1, Value::Int64(v))]).unwrap();
        }
        writer.commit().unwrap();
    }

    assert_eq!(
        gen1.visible_rows(snap1).unwrap().len(),
        10,
        "pinned generation must not observe later writes"
    );
    assert_eq!(handle.generation_stats().active_read_generations, 1);

    let (gen2, snap2) = handle.read_generation_with_context(None).unwrap();
    assert_eq!(gen2.visible_rows(snap2).unwrap().len(), 210);
    drop(gen1);
    drop(gen2);
    assert_eq!(handle.generation_stats().active_read_generations, 0);
}

/// S1C-001/S1C-003: tombstones live in the deltas captured at pin time — a
/// generation pinned before a delete still sees the row, one pinned after
/// filters it.
#[test]
fn pinned_generations_filter_tombstones_from_their_own_deltas() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    for v in 0..10 {
        table.put(vec![(1, Value::Int64(v))]).unwrap();
    }
    table.commit().unwrap();
    let handle = TableHandle::from_table(table);

    let (before_delete, snap_before) = handle.read_generation_with_context(None).unwrap();

    {
        let mut writer = handle.lock();
        let row = writer
            .visible_rows(writer.snapshot())
            .unwrap()
            .into_iter()
            .find(|row| row.columns.get(&1) == Some(&Value::Int64(3)))
            .expect("row v=3 exists");
        writer.delete(row.row_id).unwrap();
        writer.commit().unwrap();
    }

    let (after_delete, snap_after) = handle.read_generation_with_context(None).unwrap();
    assert_eq!(before_delete.visible_rows(snap_before).unwrap().len(), 10);
    assert_eq!(after_delete.visible_rows(snap_after).unwrap().len(), 9);
    assert!(
        after_delete
            .visible_rows(snap_after)
            .unwrap()
            .iter()
            .all(|row| row.columns.get(&1) != Some(&Value::Int64(3))),
        "the post-delete generation filters the tombstone"
    );
}

/// S1C-001/S1C-002: publishing swaps the shared cell atomically; a view
/// pinned before the swap is untouched by it.
#[test]
fn atomic_publish_swaps_views_without_disturbing_pinned_readers() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(1);
    for v in 0..5 {
        table.put(vec![(1, Value::Int64(v))]).unwrap();
    }
    table.commit().unwrap();

    let view1 = table.publish_read_generation().unwrap();
    assert_eq!(view1.visible_through(), Epoch(1));
    assert_eq!(view1.indexes().applied_through(), Epoch(1));
    assert_eq!(view1.base_runs().len(), 0, "nothing flushed yet");
    assert_eq!(view1.deltas().hot_len(), 5);
    let pinned = table.published_read_generation();
    assert!(Arc::ptr_eq(&view1, &pinned), "same published cell contents");

    for v in 5..10 {
        table.put(vec![(1, Value::Int64(v))]).unwrap();
    }
    table.flush().unwrap(); // spills run 1 and auto-publishes a fresh view

    let view2 = table.published_read_generation();
    assert!(!Arc::ptr_eq(&view1, &view2), "the swap replaced the cell");
    assert_eq!(view2.visible_through(), Epoch(2));
    assert_eq!(
        view2.base_runs().len(),
        1,
        "flushed run visible in new view"
    );
    // The pinned view is frozen at its publish watermark.
    assert_eq!(view1.visible_through(), Epoch(1));
    assert_eq!(view1.base_runs().len(), 0);
    assert_eq!(view1.deltas().hot_len(), 5);
    assert_eq!(view2.deltas().hot_len(), 10);
}

/// S1C-002 audit: a put-heavy workload with a live pinned generation must
/// not clone the complete table/index set (cow_clone_count tracks exactly
/// those clones at the handle layer).
#[test]
fn writes_do_not_clone_table_while_generations_are_pinned() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    for v in 0..100 {
        table.put(vec![(1, Value::Int64(v))]).unwrap();
    }
    table.commit().unwrap();
    let handle = TableHandle::from_table(table);

    let (generation, _) = handle.read_generation_with_context(None).unwrap();
    let before = handle.generation_stats();
    {
        let mut writer = handle.lock();
        for v in 100..500 {
            writer.put(vec![(1, Value::Int64(v))]).unwrap();
        }
        writer.commit().unwrap();
    }
    let after = handle.generation_stats();
    assert_eq!(
        after.cow_clone_count, before.cow_clone_count,
        "no full-table clone may happen merely because a reader is pinned"
    );
    assert_eq!(
        after.estimated_cow_clone_bytes,
        before.estimated_cow_clone_bytes
    );
    assert_eq!(after.active_read_generations, 1);
    drop(generation);
}

/// S1C-004: a read generation registers a ReadGeneration pin at its birth
/// epoch; compaction preserves the pinned versions until the generation
/// drops, then reclaims them.
#[test]
fn pinned_read_generation_blocks_version_reclamation_until_dropped() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), versioned_schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(1);
    // One row (PK 7), first version at epoch 1.
    table
        .put(vec![(1, Value::Int64(7)), (2, Value::Int64(1))])
        .unwrap();
    table.flush().unwrap(); // epoch 1, run 1
    let handle = TableHandle::from_table(table);

    let (generation, _) = handle.read_generation_with_context(None).unwrap();
    {
        let mut writer = handle.lock();
        // Same PK → a second version of the same row at epoch 2.
        writer
            .put(vec![(1, Value::Int64(7)), (2, Value::Int64(2))])
            .unwrap();
        writer.flush().unwrap(); // epoch 2, run 2
        writer.compact().unwrap(); // pin at epoch 1 → version 1 preserved
        let kept = writer.visible_rows(Snapshot::at(Epoch(1))).unwrap();
        assert_eq!(
            kept.len(),
            1,
            "the pinned epoch-1 version survives compaction"
        );
        assert_eq!(kept[0].columns.get(&2), Some(&Value::Int64(1)));
        assert_eq!(
            writer.version_gc_floor(),
            Epoch(1),
            "the generation pin lowers the reclamation floor"
        );
    }

    drop(generation); // releases the ReadGeneration pin

    let mut writer = handle.lock();
    assert_eq!(
        writer.version_gc_floor(),
        writer.current_epoch(),
        "floor returns to the visible epoch once the pin drops"
    );
    writer
        .put(vec![(1, Value::Int64(7)), (2, Value::Int64(3))])
        .unwrap();
    writer.flush().unwrap();
    writer
        .put(vec![(1, Value::Int64(7)), (2, Value::Int64(4))])
        .unwrap();
    writer.flush().unwrap();
    writer.compact().unwrap(); // no pins → superseded versions reclaimed
    assert!(
        writer
            .visible_rows(Snapshot::at(Epoch(1)))
            .unwrap()
            .is_empty(),
        "epoch-1 version reclaimed after the pin was released"
    );
    let current = writer.visible_rows(writer.snapshot()).unwrap();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].columns.get(&2), Some(&Value::Int64(4)));
}

/// S1C-004: every pin source blocks version reclamation while held and
/// releases it when dropped.
#[test]
fn each_pin_source_blocks_compaction_reclamation() {
    for source in PinSource::ALL {
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path(), versioned_schema(), 1).unwrap();
        table.set_mutable_run_spill_bytes(1);
        table
            .put(vec![(1, Value::Int64(7)), (2, Value::Int64(1))])
            .unwrap();
        table.flush().unwrap(); // epoch 1, run 1

        let guard = table.pin_registry().pin(source, Epoch(1));
        table
            .put(vec![(1, Value::Int64(7)), (2, Value::Int64(2))])
            .unwrap();
        table.flush().unwrap(); // epoch 2, run 2
        table.compact().unwrap();
        let kept = table.visible_rows(Snapshot::at(Epoch(1))).unwrap();
        assert_eq!(
            kept.len(),
            1,
            "{source:?} pin must preserve the epoch-1 version across compaction"
        );
        assert_eq!(kept[0].columns.get(&2), Some(&Value::Int64(1)));
        assert_eq!(table.version_gc_floor(), Epoch(1));
        drop(guard);

        table
            .put(vec![(1, Value::Int64(7)), (2, Value::Int64(3))])
            .unwrap();
        table.flush().unwrap();
        table
            .put(vec![(1, Value::Int64(7)), (2, Value::Int64(4))])
            .unwrap();
        table.flush().unwrap();
        table.compact().unwrap();
        assert!(
            table
                .visible_rows(Snapshot::at(Epoch(1)))
                .unwrap()
                .is_empty(),
            "{source:?} released → epoch-1 version reclaimed"
        );
    }
}

/// S1C-004 diagnostics: the report enumerates every pin source — registered
/// pins plus the transaction-snapshot and history-retention projections.
#[test]
fn version_pins_report_covers_all_six_sources() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema()).unwrap();
    db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(1))])?;
        Ok(())
    })
    .unwrap();
    db.set_history_retention_epochs(8).unwrap();
    let (_snapshot, snapshot_guard) = db.snapshot();

    let handle = db.table("t").unwrap();
    let table = handle.lock();
    let mut guards = Vec::new();
    for source in [
        PinSource::BackupPitr,
        PinSource::Replication,
        PinSource::ReadGeneration,
        PinSource::OnlineIndexBuild,
    ] {
        guards.push(table.pin_registry().pin(source, Epoch(1)));
    }

    let report = table.version_pins_report();
    assert_eq!(report.len(), PinSource::ALL.len());
    for source in PinSource::ALL {
        assert!(
            report.get(source).is_some(),
            "{source:?} must appear in pin diagnostics"
        );
    }
    // Registered pins report their exact epoch and guard count; projected
    // sources (transaction snapshots, history retention) carry no guards.
    for source in [
        PinSource::BackupPitr,
        PinSource::Replication,
        PinSource::ReadGeneration,
        PinSource::OnlineIndexBuild,
    ] {
        let info = report.get(source).unwrap();
        assert_eq!(info.oldest_epoch, Epoch(1), "{source:?} registered epoch");
        assert_eq!(info.pin_count, 1, "{source:?} registered guard");
        assert!(info.held_since.is_some(), "{source:?} guard timestamp");
    }
    assert_eq!(
        report
            .get(PinSource::TransactionSnapshot)
            .unwrap()
            .pin_count,
        0
    );
    assert_eq!(
        report.get(PinSource::HistoryRetention).unwrap().pin_count,
        0
    );
    assert_eq!(report.oldest_epoch(), Some(Epoch(1)));

    drop(guards);
    drop(snapshot_guard);
    let report = table.version_pins_report();
    assert!(
        report.get(PinSource::BackupPitr).is_none(),
        "released pins leave the report"
    );
    assert!(
        report.get(PinSource::HistoryRetention).is_some(),
        "configured history retention stays projected"
    );
}

/// S1C-002/S1C-003: the ANN family in a published index generation is
/// stable across later writes; a fresh view reflects them.
#[test]
fn ann_index_generation_view_is_stable_across_writes() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), ann_schema(), 1).unwrap();
    for id in 0..4 {
        table
            .put(vec![
                (1, Value::Int64(id)),
                (2, Value::Embedding(vec![1.0; 8])),
            ])
            .unwrap();
    }
    table.commit().unwrap();

    let view1 = table.publish_read_generation().unwrap();
    assert_eq!(view1.visible_through(), Epoch(1));
    {
        let ann = view1.indexes().ann().get(2).expect("ann index on column 2");
        assert_eq!(ann.search(&[1.0; 8], 8).unwrap().len(), 4);
        assert_eq!(view1.indexes().ann().applied_through(), Epoch(1));
    }

    for id in 4..8 {
        table
            .put(vec![
                (1, Value::Int64(id)),
                (2, Value::Embedding(vec![-1.0; 8])),
            ])
            .unwrap();
    }
    table.commit().unwrap();
    let view2 = table.publish_read_generation().unwrap();

    // The pinned generation's ANN view is unchanged by the writer's seals.
    let ann1 = view1.indexes().ann().get(2).unwrap();
    assert_eq!(
        ann1.search(&[1.0; 8], 8).unwrap().len(),
        4,
        "pinned ANN generation must not observe later inserts"
    );
    let ann2 = view2.indexes().ann().get(2).unwrap();
    assert_eq!(ann2.search(&[1.0; 8], 8).unwrap().len(), 8);
    assert_eq!(
        ann2.search(&[1.0; 8], 8).unwrap()[0].1,
        0,
        "exact rerank distance"
    );
    assert_eq!(view2.indexes().ann().applied_through(), Epoch(2));
}

/// S1C-003: compaction merges runs (and index deltas) into a new base;
/// ANN recall over the merged base is unchanged.
#[test]
fn compaction_merges_deltas_into_base_and_preserves_ann_recall() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), ann_schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(1);
    for batch in 0..4i64 {
        let sign = if batch % 2 == 0 { 1.0f32 } else { -1.0 };
        for i in 0..4i64 {
            table
                .put(vec![
                    (1, Value::Int64(batch * 4 + i)),
                    (2, Value::Embedding(vec![sign; 8])),
                ])
                .unwrap();
        }
        table.flush().unwrap();
        // Force a seal so the next batch lands in a fresh active delta.
        let _ = table.publish_read_generation().unwrap();
    }
    assert!(table.run_count() >= 4);

    table.compact().unwrap();
    assert_eq!(table.run_count(), 1, "compaction merged every run");

    let view = table.publish_read_generation().unwrap();
    let ann = view.indexes().ann().get(2).unwrap();
    for query in [[1.0f32; 8], [-1.0; 8]] {
        let hits = ann.search(&query, 16).unwrap();
        assert_eq!(hits.len(), 16, "all rows present after compaction");
        let exact = hits.iter().filter(|(_, distance)| *distance == 0).count();
        assert_eq!(exact, 8, "same-sign rows still resolve at distance 0");
    }
}
