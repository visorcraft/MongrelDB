//! P3.2/P3.3 — concurrent writers, conflict detection, generation-sealed flush.

use mongreldb_core::{schema::*, Database, MongrelError, Value};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;

fn pk_schema(name: &str) -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: name.into(),
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
fn concurrent_disjoint_writers_all_commit() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("a", pk_schema("v")).unwrap();
    db.create_table("b", pk_schema("v")).unwrap();

    let n = 8;
    let per = 50;
    let mut handles = Vec::new();
    for i in 0..n {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for j in 0..per {
                let pk = (i * per + j) as i64;
                let table = if i % 2 == 0 { "a" } else { "b" };
                db.transaction(|t| {
                    t.put(table, vec![(1, Value::Int64(pk))])?;
                    Ok(())
                })
                .unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(db.table("a").unwrap().lock().count(), (n / 2 * per) as u64);
    assert_eq!(db.table("b").unwrap().lock().count(), (n / 2 * per) as u64);
}

#[test]
fn same_pk_concurrent_insert_conflicts_exactly_one_wins() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("t", pk_schema("v")).unwrap();

    // Both writers must take their read snapshot BEFORE either commits, so they
    // genuinely race on the same pre-commit state. The first barrier gates that;
    // the second releases both commits together. (Without the pre-commit-snapshot
    // gate one thread could commit + publish before the other even begins, and
    // the late reader would correctly see the row and not conflict — a real SI
    // outcome, but not the write-write race this test means to exercise.)
    let begun = Arc::new(std::sync::Barrier::new(2));
    let commit = Arc::new(std::sync::Barrier::new(2));
    let mut handles = Vec::new();
    for _ in 0..2 {
        let db = Arc::clone(&db);
        let begun = Arc::clone(&begun);
        let commit = Arc::clone(&commit);
        handles.push(thread::spawn(move || {
            let mut tx = db.begin();
            tx.put("t", vec![(1, Value::Int64(42))]).unwrap();
            begun.wait(); // both have a read snapshot + staged write
            commit.wait(); // race the commits
            tx.commit()
        }));
    }

    let mut ok = 0;
    let mut conflicts = 0;
    for h in handles {
        match h.join().unwrap() {
            Ok(_) => ok += 1,
            Err(MongrelError::Conflict(_)) => conflicts += 1,
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }
    assert_eq!(ok, 1, "exactly one insert must succeed");
    assert_eq!(conflicts, 1, "exactly one must conflict");

    // No duplicate: count is 1.
    assert_eq!(db.table("t").unwrap().lock().count(), 1);

    // Retry succeeds for the loser.
    db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(999))])?;
        Ok(())
    })
    .unwrap();
    // Now 2 rows (42 and 999 — both exist since the second insert has the same
    // PK value but a different row_id; the PK index handles uniqueness at the
    // HOT-index level, but conflict detection only prevents concurrent races).
    // Actually with PRIMARY_KEY, two different values → 2 rows.
    assert_eq!(db.table("t").unwrap().lock().count(), 2);
}

#[test]
fn aborted_txn_consumes_no_epoch_and_visible_does_not_stall() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("t", pk_schema("v")).unwrap();

    // Pre-insert to create a conflict anchor.
    db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(1))])?;
        Ok(())
    })
    .unwrap();
    let after_first = db.visible_epoch().0;

    // Start two txns that both read at the current visible epoch.
    let mut tx1 = db.begin();
    let mut tx2 = db.begin();
    tx1.put("t", vec![(1, Value::Int64(2))]).unwrap();
    tx2.put("t", vec![(1, Value::Int64(2))]).unwrap();

    // Commit tx1 first — succeeds.
    tx1.commit().unwrap();

    // Commit tx2 — must conflict (same PK value, read before tx1's commit).
    let result = tx2.commit();
    assert!(matches!(result, Err(MongrelError::Conflict(_))));

    // The aborted txn consumed no epoch; visible has advanced past after_first.
    let vis = db.visible_epoch().0;
    assert!(
        vis > after_first,
        "visible must not stall after a conflict abort"
    );

    // A subsequent commit still works and visible advances.
    db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(3))])?;
        Ok(())
    })
    .unwrap();
    let vis2 = db.visible_epoch().0;
    assert!(vis2 > vis);
}

#[test]
fn flush_under_concurrent_writes_loses_no_rows() {
    use std::time::Duration;

    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("t", pk_schema("v")).unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));

    let db_w = Arc::clone(&db);
    let stop_w = Arc::clone(&stop);
    let total_w = Arc::clone(&total);
    let writer = thread::spawn(move || {
        let mut i: i64 = 0;
        while !stop_w.load(Ordering::Relaxed) {
            db_w.transaction(|t| {
                t.put("t", vec![(1, Value::Int64(i))])?;
                Ok(())
            })
            .unwrap();
            i += 1;
            total_w.fetch_add(1, Ordering::Relaxed);
        }
    });

    for _ in 0..3 {
        thread::sleep(Duration::from_millis(20));
        let _ = db.table("t").unwrap().lock().flush();
    }

    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    let expected = total.load(Ordering::Relaxed);
    let actual = db.table("t").unwrap().lock().count();
    assert_eq!(actual, expected, "rows lost during concurrent flush");
}

#[test]
fn group_commit_batches_fsyncs_under_concurrency() {
    // P3.2: with real group commit, concurrent committers share a single leader
    // fsync, so the WAL fsync count is strictly below the number of committed
    // transactions. (With the old "fsync under the WAL lock" path every commit
    // would issue its own fsync and the counts would be equal.)
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("t", pk_schema("v")).unwrap();

    let threads = 16u64;
    let per = 20u64;
    let total = threads * per;

    let start = db.__wal_group_sync_count();
    let barrier = Arc::new(std::sync::Barrier::new(threads as usize));
    let mut handles = Vec::new();
    for ti in 0..threads {
        let db = Arc::clone(&db);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            b.wait();
            for j in 0..per {
                let pk = (ti * per + j) as i64;
                db.transaction(|t| {
                    t.put("t", vec![(1, Value::Int64(pk))])?;
                    Ok(())
                })
                .unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let fsyncs = db.__wal_group_sync_count() - start;
    assert_eq!(
        db.table("t").unwrap().lock().count(),
        total,
        "all committed rows must be durable"
    );
    assert!(
        fsyncs < total,
        "group commit must batch: {fsyncs} fsyncs for {total} commits"
    );
}

#[test]
fn single_table_commit_is_not_visible_before_its_commit_epoch() {
    // B2 (subsumed by B1): a mounted table's `put` must not stamp rows at a
    // speculative `visible+1` epoch. Here table A stages a put, then table B's
    // commits advance the shared clock; A's uncommitted row must remain invisible
    // (not counted) until A actually commits — no reader can observe it early.
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("a", pk_schema("v")).unwrap();
    db.create_table("b", pk_schema("v")).unwrap();

    // Stage a put on A but DO NOT commit it.
    {
        let h = db.table("a").unwrap();
        let mut g = h.lock();
        g.put(vec![(1, Value::Int64(100))]).unwrap();
    }

    // Advance the shared clock well past A's would-be speculative epoch.
    for i in 0..5 {
        let h = db.table("b").unwrap();
        let mut g = h.lock();
        g.put(vec![(1, Value::Int64(i))]).unwrap();
        g.commit().unwrap();
    }

    // A's row is still uncommitted → invisible (the old speculative-epoch bug
    // would have made it visible once the shared clock passed visible+1).
    assert_eq!(
        db.table("a").unwrap().lock().count(),
        0,
        "uncommitted row must not be visible before its commit epoch"
    );

    // Commit A; now (and only now) the row is visible.
    db.table("a").unwrap().lock().commit().unwrap();
    assert_eq!(db.table("a").unwrap().lock().count(), 1);
}

#[test]
fn poisoned_db_fails_data_and_ddl_writes_fast() {
    // P3.2 §9.3e: once an fsync error poisons the DB, every subsequent write —
    // including DDL — must fail fast so no later fsync can retroactively make an
    // indeterminate transaction's appended records durable.
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema("v")).unwrap();

    let before = db.__wal_group_sync_count();
    db.__poison();

    let data = db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(1))])?;
        Ok(())
    });
    assert!(matches!(data, Err(MongrelError::Other(_))), "data commit");

    let create = db.create_table("t2", pk_schema("v"));
    assert!(
        matches!(create, Err(MongrelError::Other(_))),
        "create_table"
    );

    let drop = db.drop_table("t");
    assert!(matches!(drop, Err(MongrelError::Other(_))), "drop_table");

    assert_eq!(
        db.__wal_group_sync_count(),
        before,
        "a poisoned DB must issue no further fsyncs"
    );
}
