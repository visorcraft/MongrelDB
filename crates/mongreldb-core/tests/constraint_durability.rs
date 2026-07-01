//! Dedicated concurrent-insert and crash-durability tests for the engine's
//! declarative constraint subsystem (PLAN.md #3). These complement the
//! correctness tests in `constraints.rs` by stressing the constraint validation
//! path under true concurrency (snapshot isolation + write-write conflict
//! detection) and by proving that constraint atomicity survives a crash
//! (recovery via WAL replay).
//!
//! Crash simulation: commit (which group-fsyncs the WAL + manifest), then drop
//! the `Database` handle without a clean shutdown, then reopen and verify.

use mongreldb_core::constraint::{
    CheckConstraint, CheckExpr, FkAction, ForeignKey, TableConstraints, UniqueConstraint,
};
use mongreldb_core::schema::*;
use mongreldb_core::{Database, MongrelError, Value};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::tempdir;

fn col(id: u16, name: &str, ty: TypeId, flags: ColumnFlags) -> ColumnDef {
    ColumnDef {
        id,
        name: name.into(),
        ty,
        flags,
    }
}

/// users(id PK auto-inc, email UNIQUE nullable, age CHECK>=0 nullable).
fn users_schema() -> Schema {
    let mut cons = TableConstraints::default();
    cons.uniques.push(UniqueConstraint {
        id: 1,
        name: "email_unique".into(),
        columns: vec![1],
    });
    cons.checks.push(CheckConstraint {
        id: 2,
        name: "age_nonneg".into(),
        expr: CheckExpr::Or(
            Box::new(CheckExpr::IsNull(2)),
            Box::new(CheckExpr::Ge(
                Box::new(CheckExpr::Col(2)),
                Box::new(CheckExpr::Lit(Value::Int64(0))),
            )),
        ),
    });
    Schema {
        schema_id: 0,
        columns: vec![
            col(
                0,
                "id",
                TypeId::Int64,
                ColumnFlags::empty()
                    .with(ColumnFlags::PRIMARY_KEY)
                    .with(ColumnFlags::AUTO_INCREMENT),
            ),
            col(
                1,
                "email",
                TypeId::Bytes,
                ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            ),
            col(
                2,
                "age",
                TypeId::Int64,
                ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            ),
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: cons,
    }
}

/// orders(oid PK, uid FK→users.id, on_delete = `action`).
fn orders_schema(action: FkAction) -> Schema {
    let mut cons = TableConstraints::default();
    cons.foreign_keys.push(ForeignKey {
        id: 3,
        name: "orders_uid_fk".into(),
        columns: vec![11],
        ref_table: "users".into(),
        ref_columns: vec![0],
        on_delete: action,
    });
    Schema {
        schema_id: 0,
        columns: vec![
            col(
                10,
                "oid",
                TypeId::Int64,
                ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            ),
            col(
                11,
                "uid",
                TypeId::Int64,
                ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            ),
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: cons,
    }
}

// ── Concurrency ─────────────────────────────────────────────────────────────

#[test]
fn concurrent_distinct_unique_keys_all_commit_no_false_conflicts() {
    // Many threads each insert DISTINCT unique-key values. Under correct
    // snapshot isolation every one should commit (the WriteKey::Unique conflict
    // keys are all distinct). A buggy conflict detector would surface false
    // conflicts.
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("users", users_schema()).unwrap();

    let n_threads = 16u64;
    let per_thread = 25u64;
    let total = n_threads * per_thread;
    let barrier = Arc::new(Barrier::new(n_threads as usize));

    let mut handles = Vec::new();
    for t in 0..n_threads {
        let db = Arc::clone(&db);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            b.wait(); // release everyone together to maximize commit contention
            let mut ok = 0u64;
            for j in 0..per_thread {
                let pk = (t * per_thread + j) as i64;
                let email = format!("u{t}_{j}@x");
                let res = db.transaction(|tx| {
                    tx.put(
                        "users",
                        vec![(0, Value::Int64(pk)), (1, Value::Bytes(email.into_bytes()))],
                    )?;
                    Ok(())
                });
                if res.is_ok() {
                    ok += 1;
                }
            }
            ok
        }));
    }
    let committed: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert_eq!(
        committed, total,
        "every distinct-key insert should commit; got {committed}/{total}"
    );
    assert_eq!(db.table("users").unwrap().lock().count(), total);

    // No duplicate emails survived: a post-hoc scan finds no repeated key.
    let snap = db.snapshot().0;
    let rows = db
        .table("users")
        .unwrap()
        .lock()
        .visible_rows(snap)
        .unwrap();
    let mut seen = std::collections::HashSet::new();
    for r in &rows {
        if let Some(Value::Bytes(b)) = r.columns.get(&1) {
            assert!(
                seen.insert(b.clone()),
                "duplicate email survived concurrency"
            );
        }
    }
}

#[test]
fn concurrent_same_unique_key_exactly_one_wins() {
    // Stress: many threads race the SAME unique key. Exactly one commits; the
    // rest get UNIQUE_VIOLATION. The final table holds exactly one row for that
    // key.
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("users", users_schema()).unwrap();

    let n = 24u64;
    let barrier = Arc::new(Barrier::new(n as usize));
    let mut handles = Vec::new();
    for t in 0..n {
        let db = Arc::clone(&db);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            // Each thread uses a distinct PK but the SAME email.
            let res = {
                let mut tx = db.begin();
                tx.put(
                    "users",
                    vec![
                        (0, Value::Int64(1000 + t as i64)),
                        (1, Value::Bytes(b"race@x".to_vec())),
                    ],
                )
                .unwrap();
                b.wait(); // stage before any commit
                tx.commit()
            };
            res
        }));
    }
    let mut ok = 0u64;
    let mut conflicts = 0u64;
    for h in handles {
        match h.join().unwrap() {
            Ok(_) => ok += 1,
            Err(MongrelError::Conflict(_)) => conflicts += 1,
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }
    assert_eq!(ok, 1, "exactly one concurrent unique insert wins");
    assert_eq!(conflicts, n - 1);

    // Exactly one row carries the raced email.
    let snap = db.snapshot().0;
    let rows = db
        .table("users")
        .unwrap()
        .lock()
        .visible_rows(snap)
        .unwrap();
    let with_email = rows
        .iter()
        .filter(|r| r.columns.get(&1) == Some(&Value::Bytes(b"race@x".to_vec())))
        .count();
    assert_eq!(with_email, 1);
}

#[test]
fn concurrent_cascade_deletes_no_orphans_no_corruption() {
    // Multiple parents each with cascade children; concurrent deletes of
    // distinct parents must each cascade cleanly, leaving no orphaned child and
    // no corrupted shared state.
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("users", users_schema()).unwrap();
    db.create_table("orders", orders_schema(FkAction::Cascade))
        .unwrap();

    // Seed 12 users (ids 0..11), each with 2 orders.
    for u in 0..12i64 {
        db.transaction(|t| {
            t.put("users", vec![(0, Value::Int64(u))])?;
            Ok(())
        })
        .unwrap();
        for o in 0..2i64 {
            db.transaction(|t| {
                t.put(
                    "orders",
                    vec![(10, Value::Int64(u * 100 + o)), (11, Value::Int64(u))],
                )?;
                Ok(())
            })
            .unwrap();
        }
    }
    assert_eq!(db.table("orders").unwrap().lock().count(), 24);

    let barrier = Arc::new(Barrier::new(12));
    let mut handles = Vec::new();
    for u in 0..12i64 {
        let db = Arc::clone(&db);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let snap = db.snapshot().0;
            let rid = db
                .table("users")
                .unwrap()
                .lock()
                .visible_rows(snap)
                .unwrap()
                .iter()
                .find(|r| r.columns.get(&0) == Some(&Value::Int64(u)))
                .map(|r| r.row_id)
                .unwrap();
            b.wait();
            db.transaction(move |t| t.delete("users", rid))
        }));
    }
    for h in handles {
        h.join().unwrap().unwrap();
    }

    // Every user + every (cascade) order gone.
    assert_eq!(db.table("users").unwrap().lock().count(), 0);
    assert_eq!(
        db.table("orders").unwrap().lock().count(),
        0,
        "no orphan orders"
    );
}

// ── Crash durability ────────────────────────────────────────────────────────

#[test]
fn committed_valid_batch_survives_reopen_and_constraints_still_enforced() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.create_table("users", users_schema()).unwrap();
        db.transaction(|t| {
            t.put(
                "users",
                vec![(0, Value::Int64(1)), (1, Value::Bytes(b"a@x".to_vec()))],
            )?;
            Ok(())
        })
        .unwrap();
        // db dropped here — simulate crash. The commit group-fsynced the WAL.
    }
    let db = Database::open(&path).unwrap();
    // The committed row survived recovery.
    assert_eq!(db.table("users").unwrap().lock().count(), 1);
    // The unique constraint survived reopen: a duplicate still rejects.
    let r = db.transaction(|t| {
        t.put(
            "users",
            vec![(0, Value::Int64(2)), (1, Value::Bytes(b"a@x".to_vec()))],
        )?;
        Ok(())
    });
    assert!(matches!(r.unwrap_err(), MongrelError::Conflict(_)));
}

#[test]
fn aborted_violating_batch_leaves_no_durable_trace() {
    // A constraint-violating batch aborts BEFORE the sequencer appends any WAL
    // record, so even after a crash+reopen the violating row is absent and a
    // previously-committed valid row is intact. This is the atomicity guarantee
    // that makes remote constraint enforcement safe.
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.create_table("users", users_schema()).unwrap();
        // Valid committed row.
        db.transaction(|t| {
            t.put(
                "users",
                vec![(0, Value::Int64(1)), (1, Value::Bytes(b"good@x".to_vec()))],
            )?;
            Ok(())
        })
        .unwrap();
        // Violating batch: duplicate email → must abort. Nothing is appended.
        let r = db.transaction(|t| {
            t.put(
                "users",
                vec![(0, Value::Int64(2)), (1, Value::Bytes(b"good@x".to_vec()))],
            )?;
            Ok(())
        });
        assert!(matches!(r.unwrap_err(), MongrelError::Conflict(_)));
        // CHECK violation also aborts cleanly mid-batch.
        let r = db.transaction(|t| {
            t.put(
                "users",
                vec![
                    (0, Value::Int64(3)),
                    (1, Value::Bytes(b"bad@x".to_vec())),
                    (2, Value::Int64(-1)),
                ],
            )?;
            Ok(())
        });
        assert!(r.unwrap_err().to_string().contains("age_nonneg"));
    }
    let db = Database::open(&path).unwrap();
    // Only the one valid row is durable; neither violating insert leaked.
    let snap = db.snapshot().0;
    let rows = db
        .table("users")
        .unwrap()
        .lock()
        .visible_rows(snap)
        .unwrap();
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match r.columns.get(&0) {
            Some(Value::Int64(v)) => Some(*v),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![1], "violating inserts left no durable trace");
}

#[test]
fn committed_cascade_survives_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.create_table("users", users_schema()).unwrap();
        db.create_table("orders", orders_schema(FkAction::Cascade))
            .unwrap();
        db.transaction(|t| t.put("users", vec![(0, Value::Int64(1))]))
            .unwrap();
        db.transaction(|t| {
            t.put(
                "orders",
                vec![(10, Value::Int64(50)), (11, Value::Int64(1))],
            )
        })
        .unwrap();
        // Cascade-delete the parent and commit (durable).
        let snap = db.snapshot().0;
        let rid = db
            .table("users")
            .unwrap()
            .lock()
            .visible_rows(snap)
            .unwrap()[0]
            .row_id;
        db.transaction(|t| t.delete("users", rid)).unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(db.table("users").unwrap().lock().count(), 0);
    assert_eq!(
        db.table("orders").unwrap().lock().count(),
        0,
        "cascade delete durable across reopen (no orphan)"
    );
}

#[test]
fn committed_set_null_survives_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.create_table("users", users_schema()).unwrap();
        db.create_table("orders", orders_schema(FkAction::SetNull))
            .unwrap();
        db.transaction(|t| t.put("users", vec![(0, Value::Int64(1))]))
            .unwrap();
        db.transaction(|t| t.put("orders", vec![(10, Value::Int64(7)), (11, Value::Int64(1))]))
            .unwrap();
        let snap = db.snapshot().0;
        let rid = db
            .table("users")
            .unwrap()
            .lock()
            .visible_rows(snap)
            .unwrap()[0]
            .row_id;
        db.transaction(|t| t.delete("users", rid)).unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(db.table("users").unwrap().lock().count(), 0);
    // Child survives with a nulled FK.
    let snap = db.snapshot().0;
    let rows = db
        .table("orders")
        .unwrap()
        .lock()
        .visible_rows(snap)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].columns.get(&11),
        Some(&Value::Null),
        "set-null durable across reopen"
    );
}
