//! Engine-side declarative constraint enforcement (unique / FK / check) on the
//! `Database::transaction` commit path.

use mongreldb_core::constraint::{CheckExpr, FkAction, ForeignKey, TableConstraints, UniqueConstraint};
use mongreldb_core::schema::*;
use mongreldb_core::{Database, MongrelError, RowId, Value};
use std::collections::HashMap;
use tempfile::tempdir;

fn col(id: u16, name: &str, ty: TypeId, flags: ColumnFlags) -> ColumnDef {
    ColumnDef {
        id,
        name: name.into(),
        ty,
        flags,
    }
}

fn users_schema(email_unique: bool, check_age: bool) -> Schema {
    let mut cols = vec![
        col(0, "id", TypeId::Int64, ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY)),
        col(1, "email", TypeId::Bytes, ColumnFlags::empty().with(ColumnFlags::NULLABLE)),
        col(2, "age", TypeId::Int64, ColumnFlags::empty().with(ColumnFlags::NULLABLE)),
    ];
    cols.sort_by_key(|c| c.id);
    let mut cons = TableConstraints::default();
    if email_unique {
        cons.uniques.push(UniqueConstraint {
            id: 1,
            name: "users_email_unique".into(),
            columns: vec![1],
        });
    }
    if check_age {
        cons.checks.push(mongreldb_core::constraint::CheckConstraint {
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
    }
    Schema {
        schema_id: 0,
        columns: cols,
        indexes: vec![],
        colocation: vec![],
        constraints: cons,
    }
}

fn orders_schema_with_fk() -> Schema {
    let cols = vec![
        col(10, "oid", TypeId::Int64, ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY)),
        col(11, "uid", TypeId::Int64, ColumnFlags::empty().with(ColumnFlags::NULLABLE)),
    ];
    let mut cons = TableConstraints::default();
    cons.foreign_keys.push(ForeignKey {
        id: 3,
        name: "orders_user_fk".into(),
        columns: vec![11],
        ref_table: "users".into(),
        ref_columns: vec![0],
        on_delete: FkAction::Restrict,
    });
    Schema {
        schema_id: 0,
        columns: cols,
        indexes: vec![],
        colocation: vec![],
        constraints: cons,
    }
}

fn row(pairs: &[(u16, Value)]) -> Vec<(u16, Value)> {
    pairs.iter().cloned().collect()
}

fn cells_map(pairs: &[(u16, Value)]) -> HashMap<u16, Value> {
    pairs.iter().cloned().collect()
}

#[test]
fn check_constraint_rejects_violating_row() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema(false, true)).unwrap();

    // age >= 0 → ok.
    let r = db.transaction(|t| {
        t.put(
            "users",
            row(&[(0, Value::Int64(1)), (1, Value::Bytes(b"a@x".to_vec())), (2, Value::Int64(30))]),
        )
    });
    assert!(r.is_ok());

    // age < 0 → rejected.
    let r = db.transaction(|t| {
        t.put(
            "users",
            row(&[(0, Value::Int64(2)), (1, Value::Bytes(b"b@x".to_vec())), (2, Value::Int64(-1))]),
        )
    });
    let err = r.unwrap_err();
    assert!(matches!(err, MongrelError::InvalidArgument(_)), "got {err:?}");
    assert!(format!("{err}").contains("age_nonneg"));

    // null age → allowed (OR IsNull branch).
    let r = db.transaction(|t| {
        t.put(
            "users",
            row(&[(0, Value::Int64(3)), (1, Value::Bytes(b"c@x".to_vec())), (2, Value::Null)]),
        )
    });
    assert!(r.is_ok());
}

#[test]
fn unique_constraint_rejects_duplicate() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema(true, false)).unwrap();

    db.transaction(|t| {
        t.put(
            "users",
            row(&[(0, Value::Int64(1)), (1, Value::Bytes(b"a@x".to_vec()))]),
        )
    })
    .unwrap();

    // Same email, different PK → conflict.
    let r = db.transaction(|t| {
        t.put(
            "users",
            row(&[(0, Value::Int64(2)), (1, Value::Bytes(b"a@x".to_vec()))]),
        )
    });
    let err = r.unwrap_err();
    assert!(matches!(err, MongrelError::Conflict(_)), "got {err:?}");
    assert!(format!("{err}").contains("users_email_unique"));

    // Distinct email → ok.
    let r = db.transaction(|t| {
        t.put(
            "users",
            row(&[(0, Value::Int64(3)), (1, Value::Bytes(b"b@x".to_vec()))]),
        )
    });
    assert!(r.is_ok());
}

#[test]
fn unique_null_is_ignored() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema(true, false)).unwrap();
    // Two rows with NULL email both allowed (SQL semantics).
    db.transaction(|t| {
        t.put("users", row(&[(0, Value::Int64(1)), (1, Value::Null)]))
    })
    .unwrap();
    let r = db.transaction(|t| {
        t.put("users", row(&[(0, Value::Int64(2)), (1, Value::Null)]))
    });
    assert!(r.is_ok());
}

#[test]
fn unique_intra_batch_duplicate_rejected() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema(true, false)).unwrap();

    let r = db.transaction(|t| {
        t.put("users", row(&[(0, Value::Int64(1)), (1, Value::Bytes(b"a@x".to_vec()))]))?;
        t.put("users", row(&[(0, Value::Int64(2)), (1, Value::Bytes(b"a@x".to_vec()))]))?;
        Ok(())
    });
    let err = r.unwrap_err();
    assert!(matches!(err, MongrelError::Conflict(_)));
    assert!(format!("{err}").contains("within batch"));
}

#[test]
fn concurrent_unique_only_one_wins() {
    let dir = tempdir().unwrap();
    let db = std::sync::Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("users", users_schema(true, false)).unwrap();

    let mut handles = vec![];
    for i in 0..8 {
        let db = std::sync::Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            db.transaction(|t| {
                t.put(
                    "users",
                    row(&[(0, Value::Int64(100 + i)), (1, Value::Bytes(b"same@x".to_vec()))]),
                )
            })
        }));
    }
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let ok = results.iter().filter(|r| r.is_ok()).count();
    let conflicts = results
        .iter()
        .filter(|r| matches!(r, Err(MongrelError::Conflict(_))))
        .count();
    // Exactly one commits the duplicate email; the rest conflict.
    assert_eq!(ok, 1, "exactly one concurrent unique insert wins; got {results:?}");
    assert_eq!(conflicts, 7);
}

#[test]
fn fk_rejects_orphan_and_restricts_delete() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema(false, false)).unwrap();
    db.create_table("orders", orders_schema_with_fk()).unwrap();

    // order referencing nonexistent user → FK violation.
    let r = db.transaction(|t| {
        t.put("orders", row(&[(10, Value::Int64(1)), (11, Value::Int64(999))]))
    });
    let err = r.unwrap_err();
    assert!(matches!(err, MongrelError::Conflict(_)));
    assert!(format!("{err}").contains("orders_user_fk"));

    // create user + order referencing them.
    db.transaction(|t| {
        t.put("users", row(&[(0, Value::Int64(1)), (1, Value::Bytes(b"u@x".to_vec()))]))
    })
    .unwrap();
    db.transaction(|t| {
        t.put("orders", row(&[(10, Value::Int64(50)), (11, Value::Int64(1))]))
    })
    .unwrap();

    // delete the referenced user → restrict.
    let snap = db.snapshot().0;
    let uid = db
        .table("users")
        .unwrap()
        .lock()
        .visible_rows(snap)
        .unwrap()
        .iter()
        .find(|r| r.columns.get(&0) == Some(&Value::Int64(1)))
        .map(|r| r.row_id)
        .unwrap();
    let r = db.transaction(|t| t.delete("users", uid));
    let err = r.unwrap_err();
    assert!(matches!(err, MongrelError::Conflict(_)));
    assert!(format!("{err}").contains("restricts delete"));

    // FK with NULL uid → not checked.
    let r = db.transaction(|t| {
        t.put("orders", row(&[(10, Value::Int64(60)), (11, Value::Null)]))
    });
    assert!(r.is_ok());
}

#[test]
fn constraints_persist_across_reopen() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("users", users_schema(true, false)).unwrap();
        db.transaction(|t| {
            t.put("users", row(&[(0, Value::Int64(1)), (1, Value::Bytes(b"a@x".to_vec()))]))
        })
        .unwrap();
    }
    // Reopen: the unique constraint must still be enforced.
    let db = Database::open(dir.path()).unwrap();
    let r = db.transaction(|t| {
        t.put("users", row(&[(0, Value::Int64(2)), (1, Value::Bytes(b"a@x".to_vec()))]))
    });
    assert!(matches!(r.unwrap_err(), MongrelError::Conflict(_)));
}

#[test]
fn no_constraints_means_no_overhead_path() {
    // Sanity: a table with empty constraints behaves exactly as before.
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema(false, false)).unwrap();
    db.transaction(|t| {
        t.put("users", row(&[(0, Value::Int64(1)), (1, Value::Bytes(b"a@x".to_vec()))]))
    })
    .unwrap();
    let snap = db.snapshot().0;
    let n = db.table("users").unwrap().lock().visible_rows(snap).unwrap().len();
    assert_eq!(n, 1);
}

// Re-export RowId usage so the import isn't dropped when Value paths change.
#[allow(dead_code)]
fn _rid() -> RowId {
    RowId(0)
}
