//! Core data-integrity tests — try to break MongrelDB through normal API usage.
//!
//! Focus areas: record loss/corruption/hiding, count correctness, reopen
//! durability, schema evolution, partial puts, duplicate PKs, and visibility.

use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{RowId, Table, Value};
use tempfile::tempdir;

fn base_schema() -> Schema {
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
        colocation: vec![],
    }
}

fn schema_with_bitmap() -> Schema {
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
                name: "tag".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 3,
                name: "score".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![IndexDef {
            name: "tag_bm".into(),
            column_id: 2,
            kind: IndexKind::Bitmap,
        }],
        colocation: vec![],
    }
}

fn put2(t: &mut Table, id: i64, v: i64) -> RowId {
    t.put(vec![(1, Value::Int64(id)), (2, Value::Int64(v))])
        .unwrap()
}

fn put3(t: &mut Table, id: i64, tag: &[u8], score: i64) -> RowId {
    t.put(vec![
        (1, Value::Int64(id)),
        (2, Value::Bytes(tag.to_vec())),
        (3, Value::Int64(score)),
    ])
    .unwrap()
}

fn snapshot_row_ids(t: &Table) -> Vec<u64> {
    let snap = t.snapshot();
    t.visible_rows(snap)
        .unwrap()
        .into_iter()
        .map(|r| r.row_id.0)
        .collect()
}

fn snapshot_values(t: &Table) -> Vec<(u64, i64)> {
    let snap = t.snapshot();
    let mut rows: Vec<(u64, i64)> = t
        .visible_rows(snap)
        .unwrap()
        .into_iter()
        .map(|r| {
            let v = match r.columns.get(&1) {
                Some(Value::Int64(n)) => *n,
                _ => panic!("expected int id"),
            };
            (r.row_id.0, v)
        })
        .collect();
    rows.sort_by_key(|x| x.0);
    rows
}

#[test]
fn uncommitted_rows_are_invisible() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();
    put2(&mut t, 1, 10);
    assert_eq!(t.count(), 1, "live_count tracks uncommitted puts");
    assert_eq!(
        snapshot_row_ids(&t).len(),
        0,
        "uncommitted row must not be visible"
    );

    t.commit().unwrap();
    assert_eq!(snapshot_row_ids(&t).len(), 1, "committed row visible");
}

#[test]
fn data_survives_close_open_without_flush() {
    let dir = tempdir().unwrap();
    {
        let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();
        put2(&mut t, 1, 10);
        put2(&mut t, 2, 20);
        t.commit().unwrap();
        assert_eq!(t.count(), 2);
    }
    {
        let t = Table::open(dir.path()).unwrap();
        assert_eq!(t.count(), 2);
        let rows = snapshot_values(&t);
        assert_eq!(rows, vec![(0, 1), (1, 2)]);
    }
}

#[test]
fn data_survives_close_open_with_flush() {
    let dir = tempdir().unwrap();
    {
        let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();
        put2(&mut t, 1, 10);
        put2(&mut t, 2, 20);
        t.commit().unwrap();
        t.flush().unwrap();
    }
    {
        let t = Table::open(dir.path()).unwrap();
        assert_eq!(t.count(), 2);
        let rows = snapshot_values(&t);
        assert_eq!(rows, vec![(0, 1), (1, 2)]);
    }
}

#[test]
fn delete_then_reopen_without_flush_hides_row() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();
    let a = put2(&mut t, 1, 10);
    put2(&mut t, 2, 20);
    t.commit().unwrap();
    t.delete(a).unwrap();
    t.commit().unwrap();
    assert_eq!(t.count(), 1);

    drop(t);
    let t = Table::open(dir.path()).unwrap();
    assert_eq!(t.count(), 1);
    let visible = snapshot_row_ids(&t);
    assert_eq!(visible.len(), 1);
    assert!(
        !visible.contains(&a.0),
        "deleted row must stay hidden after reopen"
    );
}

#[test]
fn delete_then_reopen_after_flush_hides_row() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();
    let a = put2(&mut t, 1, 10);
    put2(&mut t, 2, 20);
    t.commit().unwrap();
    t.delete(a).unwrap();
    t.flush().unwrap();

    drop(t);
    let t = Table::open(dir.path()).unwrap();
    assert_eq!(t.count(), 1);
    let visible = snapshot_row_ids(&t);
    assert_eq!(visible.len(), 1);
    assert!(!visible.contains(&a.0));
}

#[test]
fn count_matches_visible_rows_after_mixed_operations() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();

    assert_eq!(t.count(), 0);
    assert_eq!(t.visible_rows(t.snapshot()).unwrap().len(), 0);

    let a = put2(&mut t, 1, 10);
    let b = put2(&mut t, 2, 20);
    let c = put2(&mut t, 3, 30);
    t.commit().unwrap();
    assert_eq!(t.count(), 3);
    assert_eq!(t.visible_rows(t.snapshot()).unwrap().len(), 3);

    t.delete(b).unwrap();
    t.commit().unwrap();
    assert_eq!(t.count(), 2);
    assert_eq!(t.visible_rows(t.snapshot()).unwrap().len(), 2);

    put2(&mut t, 4, 40);
    t.commit().unwrap();
    assert_eq!(t.count(), 3);
    assert_eq!(t.visible_rows(t.snapshot()).unwrap().len(), 3);

    // Flush and reopen; count should still match scan.
    t.flush().unwrap();
    assert_eq!(
        t.count(),
        t.visible_rows(t.snapshot()).unwrap().len() as u64
    );

    drop(t);
    let t = Table::open(dir.path()).unwrap();
    assert_eq!(t.count(), 3);
    assert_eq!(t.visible_rows(t.snapshot()).unwrap().len(), 3);
    let ids = snapshot_row_ids(&t);
    assert!(ids.contains(&a.0));
    assert!(ids.contains(&c.0));
}

#[test]
fn bulk_load_and_individual_puts_interleaved_survive_reopen() {
    let dir = tempdir().unwrap();
    {
        let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();

        // Bulk load a run.
        let batch: Vec<Vec<(u16, Value)>> = (0..500)
            .map(|i| vec![(1, Value::Int64(i)), (2, Value::Int64(i * 10))])
            .collect();
        t.bulk_load(batch).unwrap();

        // Individual puts after bulk load (land in memtable).
        for i in 500..600 {
            put2(&mut t, i, i * 10);
        }
        t.commit().unwrap();

        // A few more puts without a new flush.
        for i in 600..650 {
            put2(&mut t, i, i * 10);
        }
        t.commit().unwrap();

        assert_eq!(t.count(), 650);
        let rows = snapshot_values(&t);
        assert_eq!(rows.len(), 650);
        // Spot check ordering and id values (second tuple element is column 1).
        assert_eq!(rows[0], (0, 0));
        assert_eq!(rows[499], (499, 499));
        assert_eq!(rows[649], (649, 649));
    }
    {
        let t = Table::open(dir.path()).unwrap();
        assert_eq!(t.count(), 650);
        let rows = snapshot_values(&t);
        assert_eq!(rows.len(), 650);
        assert_eq!(rows[649], (649, 649));
    }
}

#[test]
fn flush_after_interleaved_writes_is_durable() {
    let dir = tempdir().unwrap();
    {
        let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();
        let batch: Vec<Vec<(u16, Value)>> = (0..200)
            .map(|i| vec![(1, Value::Int64(i)), (2, Value::Int64(i * 2))])
            .collect();
        t.bulk_load(batch).unwrap();

        for i in 200..250 {
            put2(&mut t, i, i * 2);
        }
        t.commit().unwrap();
        t.flush().unwrap();
    }
    {
        let t = Table::open(dir.path()).unwrap();
        assert_eq!(t.count(), 250);
        let rows = snapshot_values(&t);
        assert_eq!(rows.len(), 250);
        assert_eq!(rows.last().copied(), Some((249, 249)));
    }
}

#[test]
fn schema_evolution_add_column_reads_null_for_old_rows() {
    let dir = tempdir().unwrap();
    {
        let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();
        put2(&mut t, 1, 10);
        put2(&mut t, 2, 20);
        t.commit().unwrap();
        t.flush().unwrap();

        let new_cid = t.add_column("extra", TypeId::Int64).unwrap();
        assert_eq!(new_cid, 3);

        // New rows can write the new column.
        t.put(vec![
            (1, Value::Int64(3)),
            (2, Value::Int64(30)),
            (3, Value::Int64(300)),
        ])
        .unwrap();
        t.commit().unwrap();
    }
    {
        let t = Table::open(dir.path()).unwrap();
        assert_eq!(t.count(), 3);
        let snap = t.snapshot();
        let cols = t.visible_columns(snap).unwrap();
        let id_col: Vec<Value> = cols.iter().find(|(id, _)| *id == 1).unwrap().1.clone();
        let extra_col: Vec<Value> = cols.iter().find(|(id, _)| *id == 3).unwrap().1.clone();
        assert_eq!(id_col.len(), 3);
        assert_eq!(extra_col.len(), 3);

        // Old rows should read null for column 3; new row reads 300.
        for (i, id_val) in id_col.iter().enumerate() {
            match id_val {
                Value::Int64(1) | Value::Int64(2) => {
                    assert_eq!(
                        extra_col[i],
                        Value::Null,
                        "old row must have null new column"
                    );
                }
                Value::Int64(3) => {
                    assert_eq!(extra_col[i], Value::Int64(300));
                }
                _ => panic!("unexpected row"),
            }
        }
    }
}

#[test]
fn schema_evolution_query_on_new_column_does_not_crash() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();
    put2(&mut t, 1, 10);
    put2(&mut t, 2, 20);
    t.commit().unwrap();
    t.flush().unwrap();

    let _ = t.add_column("extra", TypeId::Int64).unwrap();

    // Range query on the new column should not include old rows (they are null).
    let q = Query::new().and(Condition::Range {
        column_id: 3,
        lo: 0,
        hi: 1000,
    });
    let rows = t.query(&q).unwrap();
    assert_eq!(
        rows.len(),
        0,
        "old rows have null new column, should not match range"
    );
}

#[test]
fn partial_column_puts_store_nulls() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), schema_with_bitmap(), 1).unwrap();

    // Full row.
    put3(&mut t, 1, b"a", 10);
    // Partial row: no score.
    t.put(vec![(1, Value::Int64(2)), (2, Value::Bytes(b"b".to_vec()))])
        .unwrap();
    // Partial row: no tag (column with bitmap index).
    t.put(vec![(1, Value::Int64(3)), (3, Value::Int64(30))])
        .unwrap();
    t.commit().unwrap();

    let snap = t.snapshot();
    let rows = t.visible_rows(snap).unwrap();
    assert_eq!(rows.len(), 3);

    let by_id: std::collections::HashMap<u64, &mongreldb_core::memtable::Row> =
        rows.iter().map(|r| (r.row_id.0, r)).collect();

    let r1 = by_id[&0];
    assert_eq!(r1.columns.get(&2), Some(&Value::Bytes(b"a".to_vec())));
    assert_eq!(r1.columns.get(&3), Some(&Value::Int64(10)));

    let r2 = by_id[&1];
    assert_eq!(r2.columns.get(&2), Some(&Value::Bytes(b"b".to_vec())));
    // Missing columns are absent from the in-memory Row HashMap.
    assert_eq!(r2.columns.get(&3), None);

    let r3 = by_id[&2];
    assert_eq!(r3.columns.get(&2), None);
    assert_eq!(r3.columns.get(&3), Some(&Value::Int64(30)));

    // The higher-level visible_columns API must fill absent columns with Null.
    let cols = t.visible_columns(snap).unwrap();
    let col2: Vec<Value> = cols.iter().find(|(id, _)| *id == 2).unwrap().1.clone();
    let col3: Vec<Value> = cols.iter().find(|(id, _)| *id == 3).unwrap().1.clone();
    assert_eq!(
        col2,
        vec![
            Value::Bytes(b"a".to_vec()),
            Value::Bytes(b"b".to_vec()),
            Value::Null,
        ]
    );
    assert_eq!(col3, vec![Value::Int64(10), Value::Null, Value::Int64(30)]);
}

#[test]
fn duplicate_pk_without_delete_creates_two_visible_rows() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();

    let first = put2(&mut t, 42, 100);
    let second = put2(&mut t, 42, 200); // same PK value
    t.commit().unwrap();

    // The table tracks two live rows internally.
    assert_eq!(t.count(), 2);
    assert_eq!(t.visible_rows(t.snapshot()).unwrap().len(), 2);

    // But the PK index only knows the latest one.
    let pk_lookup = t.lookup_pk(&42i64.to_be_bytes());
    assert_eq!(
        pk_lookup,
        Some(second),
        "PK lookup returns the latest row id"
    );

    // The older row is still visible through scan but not through PK.
    let rows = snapshot_values(&t);
    let ids: Vec<u64> = rows.iter().map(|(rid, _)| *rid).collect();
    assert!(ids.contains(&first.0));
    assert!(ids.contains(&second.0));

    // Reopen should preserve both rows and the latest PK mapping.
    drop(t);
    let t = Table::open(dir.path()).unwrap();
    assert_eq!(t.count(), 2);
    assert_eq!(t.visible_rows(t.snapshot()).unwrap().len(), 2);
    assert_eq!(t.lookup_pk(&42i64.to_be_bytes()), Some(second));
}

#[test]
fn duplicate_pk_query_returns_only_latest() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();

    put2(&mut t, 42, 100);
    put2(&mut t, 42, 200);
    t.commit().unwrap();

    let q = Query::pk(42i64.to_be_bytes().to_vec());
    let rows = t.query(&q).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns.get(&2), Some(&Value::Int64(200)));
}

#[test]
fn visible_rows_respects_snapshot_epoch() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();

    put2(&mut t, 1, 10);
    t.commit().unwrap();
    let snap1 = t.pin_snapshot();

    put2(&mut t, 2, 20);
    t.commit().unwrap();
    let snap2 = t.pin_snapshot();

    put2(&mut t, 3, 30);
    t.commit().unwrap();

    let rows1 = t.visible_rows(snap1).unwrap();
    let rows2 = t.visible_rows(snap2).unwrap();
    let rows_now = t.visible_rows(t.snapshot()).unwrap();

    assert_eq!(rows1.len(), 1, "snapshot1 sees only first row");
    assert_eq!(rows2.len(), 2, "snapshot2 sees first two rows");
    assert_eq!(rows_now.len(), 3, "current snapshot sees all rows");

    t.unpin_snapshot(snap1);
    t.unpin_snapshot(snap2);
}

#[test]
fn row_ids_are_monotonic_after_delete() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();

    let a = put2(&mut t, 1, 10);
    let b = put2(&mut t, 2, 20);
    t.commit().unwrap();
    t.delete(a).unwrap();
    t.commit().unwrap();

    let c = put2(&mut t, 3, 30);
    t.commit().unwrap();

    // Deleted row ids must not be reused.
    assert!(
        c.0 > b.0,
        "new row id must exceed all previously allocated ids"
    );
    assert_eq!(t.count(), 2);
    let ids = snapshot_row_ids(&t);
    assert!(!ids.contains(&a.0));
    assert!(ids.contains(&b.0));
    assert!(ids.contains(&c.0));
}

#[test]
fn update_via_delete_and_put_preserves_single_pk_row() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();

    let old = put2(&mut t, 1, 10);
    t.commit().unwrap();

    t.delete(old).unwrap();
    let new = put2(&mut t, 1, 99);
    t.commit().unwrap();

    assert_eq!(t.count(), 1);
    let rows = snapshot_values(&t);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, new.0);

    drop(t);
    let t = Table::open(dir.path()).unwrap();
    assert_eq!(t.count(), 1);
    assert_eq!(t.lookup_pk(&1i64.to_be_bytes()), Some(new));
}

#[test]
fn reopen_after_add_column_without_flush() {
    let dir = tempdir().unwrap();
    {
        let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();
        put2(&mut t, 1, 10);
        t.commit().unwrap();
        let _ = t.add_column("extra", TypeId::Bytes).unwrap();
        put2(&mut t, 2, 20);
        t.commit().unwrap();
    }
    {
        let t = Table::open(dir.path()).unwrap();
        assert_eq!(t.count(), 2);
        let snap = t.snapshot();
        let cols = t.visible_columns(snap).unwrap();
        let id_col: Vec<Value> = cols.iter().find(|(id, _)| *id == 1).unwrap().1.clone();
        let extra_col: Vec<Value> = cols.iter().find(|(id, _)| *id == 3).unwrap().1.clone();
        assert_eq!(id_col.len(), 2);
        for (i, id_val) in id_col.iter().enumerate() {
            if id_val == &Value::Int64(1) {
                assert_eq!(
                    extra_col[i],
                    Value::Null,
                    "old row must read null new column"
                );
            }
        }
    }
}

#[test]
fn many_small_commits_survive_reopen() {
    let dir = tempdir().unwrap();
    {
        let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();
        for i in 0..100i64 {
            put2(&mut t, i, i * 2);
            t.commit().unwrap();
        }
    }
    {
        let t = Table::open(dir.path()).unwrap();
        assert_eq!(t.count(), 100);
        let rows = snapshot_values(&t);
        assert_eq!(rows.len(), 100);
        for (i, (rid, id)) in rows.iter().enumerate() {
            assert_eq!(*rid, i as u64);
            assert_eq!(*id, i as i64);
        }
    }
}

#[test]
fn put_batch_and_individual_puts_together() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();

    let batch: Vec<Vec<(u16, Value)>> = (0..50)
        .map(|i| vec![(1, Value::Int64(i)), (2, Value::Int64(i))])
        .collect();
    t.put_batch(batch).unwrap();
    put2(&mut t, 50, 50);
    t.commit().unwrap();

    assert_eq!(t.count(), 51);
    let rows = snapshot_values(&t);
    assert_eq!(rows.len(), 51);
    assert_eq!(rows[50], (50, 50));
}

#[test]
fn bulk_load_then_put_then_flush_small_spill_threshold() {
    let dir = tempdir().unwrap();
    {
        let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();
        let batch: Vec<Vec<(u16, Value)>> = (0..100)
            .map(|i| vec![(1, Value::Int64(i)), (2, Value::Int64(i))])
            .collect();
        t.bulk_load(batch).unwrap();

        t.set_mutable_run_spill_bytes(1);
        for i in 100..120 {
            put2(&mut t, i, i);
        }
        t.flush().unwrap();
    }
    {
        let t = Table::open(dir.path()).unwrap();
        assert_eq!(t.count(), 120);
        let rows = snapshot_values(&t);
        assert_eq!(rows.len(), 120);
        assert_eq!(rows[119], (119, 119));
    }
}

#[test]
fn reopen_empty_table_is_valid() {
    let dir = tempdir().unwrap();
    {
        let _ = Table::create(dir.path(), base_schema(), 1).unwrap();
    }
    {
        let t = Table::open(dir.path()).unwrap();
        assert_eq!(t.count(), 0);
        assert_eq!(t.visible_rows(t.snapshot()).unwrap().len(), 0);
    }
}

#[test]
fn add_column_does_not_change_count() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();
    put2(&mut t, 1, 10);
    put2(&mut t, 2, 20);
    t.commit().unwrap();
    assert_eq!(t.count(), 2);

    let _ = t.add_column("extra", TypeId::Int64).unwrap();
    assert_eq!(t.count(), 2, "add_column must not change live row count");
    assert_eq!(t.visible_rows(t.snapshot()).unwrap().len(), 2);

    t.flush().unwrap();
    assert_eq!(t.count(), 2);
}

#[test]
fn partial_put_not_indexed_for_bitmap_equality() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), schema_with_bitmap(), 1).unwrap();

    // Three rows: one with tag="x", one with tag="y", one missing tag.
    put3(&mut t, 1, b"x", 0);
    put3(&mut t, 2, b"y", 0);
    t.put(vec![(1, Value::Int64(3)), (3, Value::Int64(0))])
        .unwrap();
    t.commit().unwrap();

    // Unconditional scan sees all three rows.
    assert_eq!(t.visible_rows(t.snapshot()).unwrap().len(), 3);

    // Bitmap equality on tag="x" should return exactly the first row.
    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"x".to_vec(),
    });
    let rows = t.query(&q).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns.get(&1), Some(&Value::Int64(1)));

    // Bitmap equality on tag="z" returns nothing; the partial-put row is not
    // indexed as "z" (or any tag), so it is excluded.
    let qz = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"z".to_vec(),
    });
    assert_eq!(t.query(&qz).unwrap().len(), 0);
}

#[test]
fn repeated_delete_and_put_cycles_keep_count_correct() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), base_schema(), 1).unwrap();

    let mut handles = Vec::new();
    for i in 0..20i64 {
        let rid = put2(&mut t, i, i * 10);
        handles.push(rid);
    }
    t.commit().unwrap();

    // Delete every other row, then put new rows.
    for (i, rid) in handles.iter().enumerate().step_by(2) {
        t.delete(*rid).unwrap();
        put2(&mut t, 1000 + i as i64, i as i64 * 100);
    }
    t.commit().unwrap();

    assert_eq!(t.count(), 20);
    assert_eq!(t.visible_rows(t.snapshot()).unwrap().len(), 20);

    // Flush and reopen; count must still match scan.
    t.flush().unwrap();
    assert_eq!(
        t.count(),
        t.visible_rows(t.snapshot()).unwrap().len() as u64
    );

    drop(t);
    let t = Table::open(dir.path()).unwrap();
    assert_eq!(t.count(), 20);
    assert_eq!(t.visible_rows(t.snapshot()).unwrap().len(), 20);
}

#[test]
fn query_results_preserved_after_reopen() {
    let dir = tempdir().unwrap();
    {
        let mut t = Table::create(dir.path(), schema_with_bitmap(), 1).unwrap();
        for i in 0..50i64 {
            put3(&mut t, i, b"tok", i);
        }
        t.commit().unwrap();
        t.flush().unwrap();

        let q = Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"tok".to_vec(),
        });
        assert_eq!(t.query(&q).unwrap().len(), 50);
    }
    {
        let mut t = Table::open(dir.path()).unwrap();
        let q = Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"tok".to_vec(),
        });
        let rows = t.query(&q).unwrap();
        assert_eq!(rows.len(), 50, "query result must survive reopen");
        let ids: Vec<i64> = rows
            .iter()
            .map(|r| match r.columns.get(&1) {
                Some(Value::Int64(n)) => *n,
                _ => panic!("expected int"),
            })
            .collect();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..50).collect::<Vec<i64>>());
    }
}
