use mongreldb_core::{
    ColumnDef, ColumnFlags, IndexDef, IndexKind, Query, RowId, Schema, Table, TypeId, Value,
};
use parking_lot::Mutex;
use std::sync::Arc;
use tempfile::tempdir;

fn test_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef {
                id: 0,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 1,
                name: "value".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            },
        ],
        indexes: vec![IndexDef {
            name: "value_bitmap".into(),
            column_id: 1,
            kind: IndexKind::Bitmap,
        }],
        colocation: vec![],
    }
}

fn row(id: i64, value: i64) -> Vec<(u16, Value)> {
    vec![(0, Value::Int64(id)), (1, Value::Int64(value))]
}

fn as_int(v: &Value) -> i64 {
    match v {
        Value::Int64(n) => *n,
        _ => panic!("expected int"),
    }
}

fn row_ids(rows: &[mongreldb_core::memtable::Row]) -> Vec<u64> {
    rows.iter().map(|r| r.row_id.0).collect()
}

#[test]
fn uncommitted_puts_are_hidden() -> mongreldb_core::Result<()> {
    let dir = tempdir()?;
    let mut db = Table::create(&dir, test_schema(), 1)?;
    db.put(row(1, 10))?;
    assert_eq!(db.visible_rows(db.snapshot())?.len(), 0);
    db.commit()?;
    assert_eq!(db.visible_rows(db.snapshot())?.len(), 1);
    Ok(())
}

#[test]
fn pinned_snapshot_hides_later_commits() -> mongreldb_core::Result<()> {
    let dir = tempdir()?;
    let db = Arc::new(Mutex::new(Table::create(&dir, test_schema(), 1)?));

    let snap = {
        let mut g = db.lock();
        for i in 0..100 {
            g.put(row(i, i))?;
        }
        g.commit()?;
        g.pin_snapshot()
    };

    let db2 = db.clone();
    let writer = std::thread::spawn(move || {
        let mut g = db2.lock();
        for i in 100..200 {
            g.put(row(i, i)).unwrap();
        }
        g.commit().unwrap();
    });
    writer.join().unwrap();

    let g = db.lock();
    assert_eq!(g.visible_rows(snap)?.len(), 100);
    assert_eq!(g.count(), 200);
    Ok(())
}

#[test]
fn pinned_snapshot_retains_deleted_rows() -> mongreldb_core::Result<()> {
    let dir = tempdir()?;
    let db = Arc::new(Mutex::new(Table::create(&dir, test_schema(), 1)?));

    let ids: Vec<RowId> = {
        let mut g = db.lock();
        let ids: Vec<RowId> = (0..50).map(|i| g.put(row(i, i)).unwrap()).collect();
        g.commit().unwrap();
        ids
    };

    let snap = {
        let mut g = db.lock();
        g.pin_snapshot()
    };

    {
        let mut g = db.lock();
        for rid in &ids[..25] {
            g.delete(*rid)?;
        }
        g.commit()?;
    }

    let g = db.lock();
    assert_eq!(g.visible_rows(snap)?.len(), 50);
    assert_eq!(g.visible_rows(g.snapshot())?.len(), 25);
    assert_eq!(g.count(), 25);
    Ok(())
}

#[test]
fn concurrent_readers_see_same_pinned_view() -> mongreldb_core::Result<()> {
    let dir = tempdir()?;
    let db = Arc::new(Mutex::new(Table::create(&dir, test_schema(), 1)?));

    {
        let mut g = db.lock();
        for i in 0..200 {
            g.put(row(i, i))?;
        }
        g.commit()?;
    }

    let snap = {
        let mut g = db.lock();
        g.pin_snapshot()
    };

    let expected = {
        let g = db.lock();
        row_ids(&g.visible_rows(snap)?)
    };

    let db_writer = db.clone();
    let writer = std::thread::spawn(move || {
        let mut g = db_writer.lock();
        for i in 200..400 {
            g.put(row(i, i)).unwrap();
        }
        g.commit().unwrap();
    });

    let mut handles = vec![];
    for _ in 0..4 {
        let db_reader = db.clone();
        let exp = expected.clone();
        handles.push(std::thread::spawn(move || {
            let g = db_reader.lock();
            let got = row_ids(&g.visible_rows(snap).unwrap());
            assert_eq!(got, exp, "reader observed a different pinned view");
        }));
    }

    writer.join().unwrap();
    for h in handles {
        h.join().unwrap();
    }
    Ok(())
}

#[test]
fn commit_epochs_are_monotonic_under_contention() -> mongreldb_core::Result<()> {
    let dir = tempdir()?;
    let db = Arc::new(Mutex::new(Table::create(&dir, test_schema(), 1)?));
    let epochs: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

    let mut handles = vec![];
    for t in 0..4 {
        let db = db.clone();
        let epochs = epochs.clone();
        handles.push(std::thread::spawn(move || {
            let mut local = Vec::with_capacity(25);
            for i in 0..25 {
                let mut g = db.lock();
                g.put(row((t * 100 + i) as i64, (t * 100 + i) as i64))
                    .unwrap();
                let e = g.commit().unwrap();
                local.push(e.0);
                let visible = g.current_epoch().0;
                assert!(
                    visible >= e.0,
                    "visible epoch {} regressed below assigned {}",
                    visible,
                    e.0
                );
            }
            epochs.lock().extend(local);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let mut all = epochs.lock().clone();
    all.sort_unstable();
    let final_visible = { db.lock().current_epoch().0 };
    assert_eq!(all.len(), 100);
    assert_eq!(*all.last().unwrap(), final_visible);
    // Under the mutex each commit is serialized, so assigned epochs are unique.
    for w in all.windows(2) {
        assert!(w[1] > w[0], "assigned epochs must be strictly monotonic");
    }
    Ok(())
}

#[test]
fn rowid_never_reused_after_delete() -> mongreldb_core::Result<()> {
    let dir = tempdir()?;
    let mut db = Table::create(&dir, test_schema(), 1)?;

    let ids: Vec<RowId> = (0..5).map(|i| db.put(row(i, i)).unwrap()).collect();
    db.commit()?;

    for rid in &ids[1..4] {
        db.delete(*rid)?;
    }
    db.commit()?;

    let max_old = ids.iter().map(|r| r.0).max().unwrap();
    let new_ids: Vec<RowId> = (0..3).map(|_| db.put(row(100, 100)).unwrap()).collect();
    db.commit()?;

    for rid in &new_ids {
        assert!(
            rid.0 > max_old,
            "reused RowId {} after delete (max old was {})",
            rid.0,
            max_old
        );
    }
    // 5 initial rows minus 3 deleted plus 3 newly inserted = 5 visible rows.
    assert_eq!(db.visible_rows(db.snapshot())?.len(), 5);
    Ok(())
}

#[test]
fn pinned_snapshot_survives_flush() -> mongreldb_core::Result<()> {
    let dir = tempdir()?;
    let mut db = Table::create(&dir, test_schema(), 1)?;
    db.set_mutable_run_spill_bytes(1024);

    for i in 0..500 {
        db.put(row(i, i))?;
    }
    db.commit()?;

    let snap = db.pin_snapshot();

    for i in 0..500 {
        db.put(row(1000 + i, 1000 + i))?;
    }
    db.commit()?;
    let flush_epoch = db.flush()?;
    assert!(flush_epoch.0 >= 2);

    assert_eq!(db.visible_rows(snap)?.len(), 500);
    assert_eq!(db.visible_rows(db.snapshot())?.len(), 1000);
    Ok(())
}

#[test]
fn pinned_snapshot_isolates_updates() -> mongreldb_core::Result<()> {
    let dir = tempdir()?;
    let mut db = Table::create(&dir, test_schema(), 1)?;

    let ids: Vec<RowId> = (1..=10).map(|i| db.put(row(i, i * 10)).unwrap()).collect();
    db.commit()?;

    let snap = db.pin_snapshot();

    for (i, rid) in ids.iter().enumerate() {
        let pk = (i + 1) as i64;
        db.delete(*rid)?;
        db.put(row(pk, pk * 10 + 5))?;
    }
    db.commit()?;

    let old = db.visible_rows(snap)?;
    assert_eq!(old.len(), 10);
    for r in &old {
        let pk = as_int(&r.columns[&0]);
        assert_eq!(r.columns[&1], Value::Int64(pk * 10));
    }

    let new = db.visible_rows(db.snapshot())?;
    assert_eq!(new.len(), 10);
    for r in &new {
        let pk = as_int(&r.columns[&0]);
        assert_eq!(r.columns[&1], Value::Int64(pk * 10 + 5));
    }

    let latest = db
        .lookup_pk(&Value::Int64(5).encode_key())
        .expect("pk lookup after update");
    assert!(
        latest.0 > ids[4].0,
        "pk should point to the newer row id after update"
    );
    Ok(())
}

#[test]
fn interleaved_batch_and_single_puts() -> mongreldb_core::Result<()> {
    let dir = tempdir()?;
    let mut db = Table::create(&dir, test_schema(), 1)?;

    let batch: Vec<Vec<(u16, Value)>> = (0..50).map(|i| row(i, i)).collect();
    db.put_batch(batch)?;
    for i in 50..100 {
        db.put(row(i, i))?;
    }
    db.commit()?;

    assert_eq!(db.count(), 100);
    assert_eq!(db.visible_rows(db.snapshot())?.len(), 100);
    Ok(())
}

#[test]
fn duplicate_pk_creates_multiple_visible_rows() -> mongreldb_core::Result<()> {
    let dir = tempdir()?;
    let mut db = Table::create(&dir, test_schema(), 1)?;

    let _r1 = db.put(row(42, 1))?;
    let r2 = db.put(row(42, 2))?;
    db.commit()?;

    let rows = db.visible_rows(db.snapshot())?;
    assert_eq!(
        rows.len(),
        2,
        "duplicate PK produced {} visible rows",
        rows.len()
    );
    assert_eq!(db.count(), 2);

    let latest = db
        .lookup_pk(&Value::Int64(42).encode_key())
        .expect("pk lookup should find the latest");
    assert_eq!(latest, r2);

    let q = Query::pk(Value::Int64(42).encode_key());
    let qrows = db.query(&q)?;
    assert_eq!(qrows.len(), 1);
    assert_eq!(qrows[0].row_id, r2);
    Ok(())
}

#[test]
fn snapshot_epoch_never_exceeds_visible() -> mongreldb_core::Result<()> {
    let dir = tempdir()?;
    let mut db = Table::create(&dir, test_schema(), 1)?;

    for i in 0..10 {
        db.put(row(i, i))?;
        let e = db.commit()?;
        let snap = db.snapshot();
        assert!(snap.epoch.0 <= e.0);
        assert!(db.current_epoch().0 >= e.0);
    }
    Ok(())
}
