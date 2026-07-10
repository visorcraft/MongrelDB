//! Engine-side declarative constraint enforcement (unique / FK / check) on the
//! `Database::transaction` commit path.

use mongreldb_core::constraint::{
    CheckExpr, FkAction, ForeignKey, TableConstraints, UniqueConstraint,
};
use mongreldb_core::schema::*;
use mongreldb_core::{Database, MongrelError, RowId, Value};
use tempfile::tempdir;

fn col(id: u16, name: &str, ty: TypeId, flags: ColumnFlags) -> ColumnDef {
    ColumnDef {
        id,
        name: name.into(),
        ty,
        flags,
        default_value: None,
    }
}

fn users_schema(email_unique: bool, check_age: bool) -> Schema {
    let mut cols = vec![
        col(
            0,
            "id",
            TypeId::Int64,
            ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
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
        cons.checks
            .push(mongreldb_core::constraint::CheckConstraint {
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
        clustered: false,
    }
}

fn orders_schema_with_fk() -> Schema {
    let cols = vec![
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
        clustered: false,
    }
}

fn orders_schema_with_action(action: FkAction) -> Schema {
    let mut s = orders_schema_with_fk();
    s.constraints.foreign_keys[0].on_delete = action;
    s
}

fn row(pairs: &[(u16, Value)]) -> Vec<(u16, Value)> {
    pairs.to_vec()
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
            row(&[
                (0, Value::Int64(1)),
                (1, Value::Bytes(b"a@x".to_vec())),
                (2, Value::Int64(30)),
            ]),
        )
    });
    assert!(r.is_ok());

    // age < 0 → rejected.
    let r = db.transaction(|t| {
        t.put(
            "users",
            row(&[
                (0, Value::Int64(2)),
                (1, Value::Bytes(b"b@x".to_vec())),
                (2, Value::Int64(-1)),
            ]),
        )
    });
    let err = r.unwrap_err();
    assert!(
        matches!(err, MongrelError::InvalidArgument(_)),
        "got {err:?}"
    );
    assert!(format!("{err}").contains("age_nonneg"));

    // null age → allowed (OR IsNull branch).
    let r = db.transaction(|t| {
        t.put(
            "users",
            row(&[
                (0, Value::Int64(3)),
                (1, Value::Bytes(b"c@x".to_vec())),
                (2, Value::Null),
            ]),
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
    db.transaction(|t| t.put("users", row(&[(0, Value::Int64(1)), (1, Value::Null)])))
        .unwrap();
    let r = db.transaction(|t| t.put("users", row(&[(0, Value::Int64(2)), (1, Value::Null)])));
    assert!(r.is_ok());
}

#[test]
fn unique_intra_batch_duplicate_rejected() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema(true, false)).unwrap();

    let r = db.transaction(|t| {
        t.put(
            "users",
            row(&[(0, Value::Int64(1)), (1, Value::Bytes(b"a@x".to_vec()))]),
        )?;
        t.put(
            "users",
            row(&[(0, Value::Int64(2)), (1, Value::Bytes(b"a@x".to_vec()))]),
        )?;
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
                    row(&[
                        (0, Value::Int64(100 + i)),
                        (1, Value::Bytes(b"same@x".to_vec())),
                    ]),
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
    assert_eq!(
        ok, 1,
        "exactly one concurrent unique insert wins; got {results:?}"
    );
    assert_eq!(conflicts, 7);
}

#[test]
fn fk_rejects_orphan_and_restricts_delete() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema(false, false))
        .unwrap();
    db.create_table("orders", orders_schema_with_fk()).unwrap();

    // order referencing nonexistent user → FK violation.
    let r = db.transaction(|t| {
        t.put(
            "orders",
            row(&[(10, Value::Int64(1)), (11, Value::Int64(999))]),
        )
    });
    let err = r.unwrap_err();
    assert!(matches!(err, MongrelError::Conflict(_)));
    assert!(format!("{err}").contains("orders_user_fk"));

    // create user + order referencing them.
    db.transaction(|t| {
        t.put(
            "users",
            row(&[(0, Value::Int64(1)), (1, Value::Bytes(b"u@x".to_vec()))]),
        )
    })
    .unwrap();
    db.transaction(|t| {
        t.put(
            "orders",
            row(&[(10, Value::Int64(50)), (11, Value::Int64(1))]),
        )
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
    let r = db.transaction(|t| t.put("orders", row(&[(10, Value::Int64(60)), (11, Value::Null)])));
    assert!(r.is_ok());
}

#[test]
fn fk_same_txn_parent_and_child_insert() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema(false, false))
        .unwrap();
    db.create_table("orders", orders_schema_with_fk()).unwrap();

    // Single transaction: insert a user AND an order referencing them. The
    // parent is not yet committed when the child's FK is validated, so this
    // requires final-write-set FK validation (the child sees the staged
    // parent put within the same batch).
    let r = db.transaction(|t| {
        t.put(
            "users",
            row(&[(0, Value::Int64(7)), (1, Value::Bytes(b"p@x".to_vec()))]),
        )?;
        t.put(
            "orders",
            row(&[(10, Value::Int64(70)), (11, Value::Int64(7))]),
        )?;
        Ok(())
    });
    assert!(
        r.is_ok(),
        "same-txn parent+child insert should satisfy FK: {:?}",
        r
    );
}

#[test]
fn fk_cyclical_same_txn_inserts() {
    // Two tables that mutually reference each other. Inserting a row in each
    // (each referencing the other) within a single transaction must succeed
    // under final-write-set FK validation.
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();

    // a(id pk, bid -> b.id) ; b(id pk, aid -> a.id)
    let a_schema = {
        let mut cons = TableConstraints::default();
        cons.foreign_keys.push(ForeignKey {
            id: 1,
            name: "a_b_fk".into(),
            columns: vec![11],
            ref_table: "b".into(),
            ref_columns: vec![10],
            on_delete: FkAction::Restrict,
        });
        Schema {
            schema_id: 0,
            columns: vec![
                col(
                    10,
                    "id",
                    TypeId::Int64,
                    ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                ),
                col(
                    11,
                    "bid",
                    TypeId::Int64,
                    ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                ),
            ],
            indexes: vec![],
            colocation: vec![],
            constraints: cons,
            clustered: false,
        }
    };
    let b_schema = {
        let mut cons = TableConstraints::default();
        cons.foreign_keys.push(ForeignKey {
            id: 2,
            name: "b_a_fk".into(),
            columns: vec![11],
            ref_table: "a".into(),
            ref_columns: vec![10],
            on_delete: FkAction::Restrict,
        });
        Schema {
            schema_id: 0,
            columns: vec![
                col(
                    10,
                    "id",
                    TypeId::Int64,
                    ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                ),
                col(
                    11,
                    "aid",
                    TypeId::Int64,
                    ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                ),
            ],
            indexes: vec![],
            colocation: vec![],
            constraints: cons,
            clustered: false,
        }
    };
    db.create_table("a", a_schema).unwrap();
    db.create_table("b", b_schema).unwrap();

    let r = db.transaction(|t| {
        t.put("a", row(&[(10, Value::Int64(1)), (11, Value::Int64(2))]))?;
        t.put("b", row(&[(10, Value::Int64(2)), (11, Value::Int64(1))]))?;
        Ok(())
    });
    assert!(
        r.is_ok(),
        "cyclical same-txn inserts should satisfy FK: {:?}",
        r
    );
}

#[test]
fn fk_cascade_does_not_fire_on_key_preserving_update() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema(false, false))
        .unwrap();
    db.create_table("orders", orders_schema_with_action(FkAction::Cascade))
        .unwrap();

    // A user and a child order referencing them.
    db.transaction(|t| {
        t.put(
            "users",
            row(&[(0, Value::Int64(1)), (1, Value::Bytes(b"a@x".to_vec()))]),
        )?;
        t.put(
            "orders",
            row(&[(10, Value::Int64(50)), (11, Value::Int64(1))]),
        )?;
        Ok::<_, mongreldb_core::MongrelError>(())
    })
    .unwrap();
    assert_eq!(visible_count(&db, "orders"), 1);

    // Simulate a SQL UPDATE of the user's email: delete + re-put with the SAME
    // primary key (id=1). The referenced key is preserved, so ON DELETE
    // CASCADE must NOT delete the child order.
    let user_rid = row_id_of(&db, "users", 0, &Value::Int64(1));
    db.transaction(|t| {
        t.delete("users", user_rid)?;
        t.put(
            "users",
            row(&[(0, Value::Int64(1)), (1, Value::Bytes(b"b@x".to_vec()))]),
        )?;
        Ok::<_, mongreldb_core::MongrelError>(())
    })
    .unwrap();

    assert_eq!(
        visible_count(&db, "orders"),
        1,
        "cascade must not fire when the parent key is preserved by an update"
    );
    assert_eq!(visible_count(&db, "users"), 1);
}

#[test]
fn constraints_persist_across_reopen() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("users", users_schema(true, false)).unwrap();
        db.transaction(|t| {
            t.put(
                "users",
                row(&[(0, Value::Int64(1)), (1, Value::Bytes(b"a@x".to_vec()))]),
            )
        })
        .unwrap();
    }
    // Reopen: the unique constraint must still be enforced.
    let db = Database::open(dir.path()).unwrap();
    let r = db.transaction(|t| {
        t.put(
            "users",
            row(&[(0, Value::Int64(2)), (1, Value::Bytes(b"a@x".to_vec()))]),
        )
    });
    assert!(matches!(r.unwrap_err(), MongrelError::Conflict(_)));
}

#[test]
fn no_constraints_means_no_overhead_path() {
    // Sanity: a table with empty constraints behaves exactly as before.
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema(false, false))
        .unwrap();
    db.transaction(|t| {
        t.put(
            "users",
            row(&[(0, Value::Int64(1)), (1, Value::Bytes(b"a@x".to_vec()))]),
        )
    })
    .unwrap();
    let snap = db.snapshot().0;
    let n = db
        .table("users")
        .unwrap()
        .lock()
        .visible_rows(snap)
        .unwrap()
        .len();
    assert_eq!(n, 1);
}

// Re-export RowId usage so the import isn't dropped when Value paths change.
#[allow(dead_code)]
fn _rid() -> RowId {
    RowId(0)
}

fn visible_count(db: &Database, table: &str) -> usize {
    let snap = db.snapshot().0;
    db.table(table)
        .unwrap()
        .lock()
        .visible_rows(snap)
        .unwrap()
        .len()
}

fn visible_rows_with_uid(db: &Database, table: &str, uid_col: u16) -> Vec<Value> {
    let snap = db.snapshot().0;
    db.table(table)
        .unwrap()
        .lock()
        .visible_rows(snap)
        .unwrap()
        .iter()
        .map(|r| r.columns.get(&uid_col).cloned().unwrap_or(Value::Null))
        .collect()
}

fn row_id_of(db: &Database, table: &str, col: u16, val: &Value) -> RowId {
    let snap = db.snapshot().0;
    db.table(table)
        .unwrap()
        .lock()
        .visible_rows(snap)
        .unwrap()
        .iter()
        .find(|r| r.columns.get(&col) == Some(val))
        .map(|r| r.row_id)
        .unwrap()
}

#[test]
fn fk_cascade_deletes_children() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema(false, false))
        .unwrap();
    db.create_table("orders", orders_schema_with_action(FkAction::Cascade))
        .unwrap();

    db.transaction(|t| t.put("users", row(&[(0, Value::Int64(1))])))
        .unwrap();
    db.transaction(|t| {
        t.put(
            "orders",
            row(&[(10, Value::Int64(100)), (11, Value::Int64(1))]),
        )?;
        t.put(
            "orders",
            row(&[(10, Value::Int64(101)), (11, Value::Int64(1))]),
        )
    })
    .unwrap();
    db.transaction(|t| t.put("users", row(&[(0, Value::Int64(2))])))
        .unwrap();
    db.transaction(|t| {
        t.put(
            "orders",
            row(&[(10, Value::Int64(102)), (11, Value::Int64(2))]),
        )
    })
    .unwrap();

    assert_eq!(visible_count(&db, "orders"), 3);
    // Delete user 1 → both referencing orders cascade-deleted.
    let uid1 = row_id_of(&db, "users", 0, &Value::Int64(1));
    db.transaction(|t| t.delete("users", uid1)).unwrap();
    assert_eq!(visible_count(&db, "users"), 1);
    // Orders 100/101 gone; order 102 (user 2) remains.
    assert_eq!(visible_count(&db, "orders"), 1);
    let snap = db.snapshot().0;
    let remaining = db
        .table("orders")
        .unwrap()
        .lock()
        .visible_rows(snap)
        .unwrap();
    assert_eq!(remaining[0].columns.get(&10), Some(&Value::Int64(102)));
}

#[test]
fn fk_set_null_nulls_child_fk() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema(false, false))
        .unwrap();
    db.create_table("orders", orders_schema_with_action(FkAction::SetNull))
        .unwrap();

    db.transaction(|t| t.put("users", row(&[(0, Value::Int64(1))])))
        .unwrap();
    db.transaction(|t| {
        t.put(
            "orders",
            row(&[(10, Value::Int64(100)), (11, Value::Int64(1))]),
        )
    })
    .unwrap();

    // Delete the parent → the child's FK column becomes NULL (child survives).
    let uid1 = row_id_of(&db, "users", 0, &Value::Int64(1));
    db.transaction(|t| t.delete("users", uid1)).unwrap();
    assert_eq!(visible_count(&db, "users"), 0);
    assert_eq!(
        visible_count(&db, "orders"),
        1,
        "child row survives set-null"
    );
    assert_eq!(
        visible_rows_with_uid(&db, "orders", 11),
        vec![Value::Null],
        "FK column was nulled"
    );
}

#[test]
fn fk_cascade_transitive() {
    // users <- orders (cascade) <- items (cascade): deleting the user cascades
    // through orders to items.
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema(false, false))
        .unwrap();
    db.create_table("orders", orders_schema_with_action(FkAction::Cascade))
        .unwrap();
    // items references orders.oid (col 10).
    let mut items_cons = TableConstraints::default();
    items_cons.foreign_keys.push(ForeignKey {
        id: 4,
        name: "items_order_fk".into(),
        columns: vec![21],
        ref_table: "orders".into(),
        ref_columns: vec![10],
        on_delete: FkAction::Cascade,
    });
    db.create_table(
        "items",
        Schema {
            schema_id: 0,
            columns: vec![
                col(
                    20,
                    "iid",
                    TypeId::Int64,
                    ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                ),
                col(
                    21,
                    "oid",
                    TypeId::Int64,
                    ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                ),
            ],
            indexes: vec![],
            colocation: vec![],
            constraints: items_cons,
            clustered: false,
        },
    )
    .unwrap();

    db.transaction(|t| t.put("users", row(&[(0, Value::Int64(1))])))
        .unwrap();
    db.transaction(|t| {
        t.put(
            "orders",
            row(&[(10, Value::Int64(50)), (11, Value::Int64(1))]),
        )
    })
    .unwrap();
    db.transaction(|t| {
        t.put(
            "items",
            row(&[(20, Value::Int64(9)), (21, Value::Int64(50))]),
        )
    })
    .unwrap();

    assert_eq!(visible_count(&db, "items"), 1);
    let uid1 = row_id_of(&db, "users", 0, &Value::Int64(1));
    db.transaction(|t| t.delete("users", uid1)).unwrap();
    assert_eq!(visible_count(&db, "orders"), 0);
    assert_eq!(
        visible_count(&db, "items"),
        0,
        "transitive cascade reached items"
    );
}

#[test]
fn fk_restrict_still_blocks_when_mixed() {
    // A RESTRICT child still blocks even when a CASCADE child would be cleaned.
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema(false, false))
        .unwrap();
    db.create_table("orders", orders_schema_with_action(FkAction::Cascade))
        .unwrap();
    db.create_table("logs", orders_schema_with_action(FkAction::Restrict))
        .unwrap();

    db.transaction(|t| t.put("users", row(&[(0, Value::Int64(1))])))
        .unwrap();
    db.transaction(|t| {
        t.put(
            "orders",
            row(&[(10, Value::Int64(1)), (11, Value::Int64(1))]),
        )
    })
    .unwrap();
    db.transaction(|t| t.put("logs", row(&[(10, Value::Int64(2)), (11, Value::Int64(1))])))
        .unwrap();

    let uid1 = row_id_of(&db, "users", 0, &Value::Int64(1));
    let r = db.transaction(|t| t.delete("users", uid1));
    let err = r.unwrap_err();
    assert!(matches!(err, MongrelError::Conflict(_)));
    assert!(format!("{err}").contains("restricts delete"));
}
