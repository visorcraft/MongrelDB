use mongreldb_core::constraint::{
    CheckConstraint, CheckExpr, FkAction, ForeignKey, TableConstraints, UniqueConstraint,
};
use mongreldb_core::{
    ColumnDef, ColumnFlags, Database, Epoch, Schema, Snapshot, StoredTrigger, TriggerCell,
    TriggerCondition, TriggerDefinition, TriggerEvent, TriggerExpr, TriggerProgram, TriggerStep,
    TriggerTarget, TriggerTiming, TriggerValue, TypeId, Value,
};
use tempfile::tempdir;

fn col(id: u16, name: &str, ty: TypeId, flags: ColumnFlags) -> ColumnDef {
    ColumnDef {
        id,
        name: name.into(),
        ty,
        flags,
    }
}

fn pk_flags() -> ColumnFlags {
    ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY)
}

fn base_schema() -> Schema {
    Schema {
        schema_id: 0,
        columns: vec![col(1, "id", TypeId::Int64, pk_flags())],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: TableConstraints::default(),
        clustered: false,
    }
}

fn unique_audit_schema() -> Schema {
    let mut constraints = TableConstraints::default();
    constraints.uniques.push(UniqueConstraint {
        id: 1,
        name: "audit_email_unique".into(),
        columns: vec![2],
    });
    Schema {
        schema_id: 0,
        columns: vec![
            col(1, "id", TypeId::Int64, pk_flags()),
            col(2, "email", TypeId::Bytes, ColumnFlags::empty()),
        ],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints,
        clustered: false,
    }
}

fn check_audit_schema() -> Schema {
    let mut constraints = TableConstraints::default();
    constraints.checks.push(CheckConstraint {
        id: 1,
        name: "amount_nonneg".into(),
        expr: CheckExpr::Ge(
            Box::new(CheckExpr::Col(2)),
            Box::new(CheckExpr::Lit(Value::Int64(0))),
        ),
    });
    Schema {
        schema_id: 0,
        columns: vec![
            col(1, "id", TypeId::Int64, pk_flags()),
            col(2, "amount", TypeId::Int64, ColumnFlags::empty()),
        ],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints,
        clustered: false,
    }
}

fn fk_audit_schema() -> Schema {
    let mut constraints = TableConstraints::default();
    constraints.foreign_keys.push(ForeignKey {
        id: 1,
        name: "audit_user_fk".into(),
        columns: vec![2],
        ref_table: "users".into(),
        ref_columns: vec![1],
        on_delete: FkAction::Restrict,
    });
    Schema {
        schema_id: 0,
        columns: vec![
            col(1, "id", TypeId::Int64, pk_flags()),
            col(2, "user_id", TypeId::Int64, ColumnFlags::empty()),
        ],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints,
        clustered: false,
    }
}

fn trigger_inserting_audit(cells: Vec<TriggerCell>) -> StoredTrigger {
    StoredTrigger::new(
        "base_ai",
        TriggerDefinition {
            target: TriggerTarget::Table("base".into()),
            timing: TriggerTiming::After,
            event: TriggerEvent::Insert,
            update_of: Vec::new(),
            target_columns: Vec::new(),
            when: None,
            program: TriggerProgram {
                steps: vec![TriggerStep::Insert {
                    table: "audit".into(),
                    cells,
                }],
            },
        },
        0,
    )
    .unwrap()
}

fn put_base(db: &Database, id: i64) -> mongreldb_core::Result<()> {
    db.transaction(|tx| tx.put("base", vec![(1, Value::Int64(id))]).map(|_| ()))
}

#[test]
fn triggered_unique_violation_aborts_original_write() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("base", base_schema()).unwrap();
    db.create_table("audit", unique_audit_schema()).unwrap();
    db.transaction(|tx| {
        tx.put(
            "audit",
            vec![
                (1, Value::Int64(99)),
                (2, Value::Bytes(b"taken@example.test".to_vec())),
            ],
        )
        .map(|_| ())
    })
    .unwrap();
    db.create_trigger(trigger_inserting_audit(vec![
        TriggerCell {
            column_id: 1,
            value: TriggerValue::NewColumn(1),
        },
        TriggerCell {
            column_id: 2,
            value: TriggerValue::Literal(Value::Bytes(b"taken@example.test".to_vec())),
        },
    ]))
    .unwrap();

    let err = put_base(&db, 1).unwrap_err();
    assert!(err.to_string().contains("audit_email_unique"), "{err}");
    assert_eq!(db.table("base").unwrap().lock().count(), 0);
    assert_eq!(db.table("audit").unwrap().lock().count(), 1);
}

#[test]
fn triggered_check_violation_aborts_original_write() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("base", base_schema()).unwrap();
    db.create_table("audit", check_audit_schema()).unwrap();
    db.create_trigger(trigger_inserting_audit(vec![
        TriggerCell {
            column_id: 1,
            value: TriggerValue::NewColumn(1),
        },
        TriggerCell {
            column_id: 2,
            value: TriggerValue::Literal(Value::Int64(-1)),
        },
    ]))
    .unwrap();

    let err = put_base(&db, 1).unwrap_err();
    assert!(err.to_string().contains("amount_nonneg"), "{err}");
    assert_eq!(db.table("base").unwrap().lock().count(), 0);
    assert_eq!(db.table("audit").unwrap().lock().count(), 0);
}

#[test]
fn triggered_foreign_key_violation_aborts_original_write() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("base", base_schema()).unwrap();
    db.create_table("users", base_schema()).unwrap();
    db.create_table("audit", fk_audit_schema()).unwrap();
    db.create_trigger(trigger_inserting_audit(vec![
        TriggerCell {
            column_id: 1,
            value: TriggerValue::NewColumn(1),
        },
        TriggerCell {
            column_id: 2,
            value: TriggerValue::NewColumn(1),
        },
    ]))
    .unwrap();

    let err = put_base(&db, 7).unwrap_err();
    assert!(err.to_string().contains("audit_user_fk"), "{err}");
    assert_eq!(db.table("base").unwrap().lock().count(), 0);
    assert_eq!(db.table("audit").unwrap().lock().count(), 0);

    db.transaction(|tx| tx.put("users", vec![(1, Value::Int64(7))]).map(|_| ()))
        .unwrap();
    put_base(&db, 7).unwrap();
    assert_eq!(db.table("base").unwrap().lock().count(), 1);
    assert_eq!(db.table("audit").unwrap().lock().count(), 1);
}

mod trigger_expr_serde {
    use mongreldb_core::memtable::Value;
    use mongreldb_core::trigger::*;

    #[test]
    fn trigger_expr_serializes_ranges_and_booleans() {
        let expr = TriggerExpr::And {
            left: Box::new(TriggerExpr::Gt {
                left: TriggerValue::NewColumn(1),
                right: TriggerValue::Literal(Value::Int64(0)),
            }),
            right: Box::new(TriggerExpr::Or {
                left: Box::new(TriggerExpr::Lte {
                    left: TriggerValue::NewColumn(2),
                    right: TriggerValue::Literal(Value::Int64(100)),
                }),
                right: Box::new(TriggerExpr::Not(Box::new(TriggerExpr::IsNull(
                    TriggerValue::OldColumn(3),
                )))),
            }),
        };
        let json = serde_json::to_value(&expr).unwrap();
        let round: TriggerExpr = serde_json::from_value(json).unwrap();
        assert_eq!(expr, round);
    }

    #[test]
    fn trigger_condition_serializes_ranges_and_booleans() {
        let cond = TriggerCondition::And {
            left: Box::new(TriggerCondition::Gt {
                column_id: 1,
                value: TriggerValue::NewColumn(1),
            }),
            right: Box::new(TriggerCondition::Or {
                left: Box::new(TriggerCondition::Lte {
                    column_id: 2,
                    value: TriggerValue::Literal(Value::Int64(100)),
                }),
                right: Box::new(TriggerCondition::Not(Box::new(TriggerCondition::IsNull {
                    column_id: 3,
                }))),
            }),
        };
        let json = serde_json::to_value(&cond).unwrap();
        let round: TriggerCondition = serde_json::from_value(json).unwrap();
        assert_eq!(cond, round);
    }

    #[test]
    fn trigger_step_serializes_new_variants() {
        let program = TriggerProgram {
            steps: vec![
                TriggerStep::Select {
                    id: "children".into(),
                    table: "orders".into(),
                    conditions: vec![TriggerCondition::Eq {
                        column_id: 2,
                        value: TriggerValue::NewColumn(1),
                    }],
                },
                TriggerStep::Foreach {
                    id: "children".into(),
                    steps: vec![TriggerStep::Raise {
                        action: TriggerRaiseAction::Abort,
                        message: TriggerValue::Literal(Value::Bytes(b"found child".to_vec())),
                    }],
                },
                TriggerStep::DeleteWhere {
                    table: "logs".into(),
                    conditions: vec![TriggerCondition::Lt {
                        column_id: 3,
                        value: TriggerValue::NewColumn(4),
                    }],
                },
                TriggerStep::UpdateWhere {
                    table: "orders".into(),
                    conditions: vec![TriggerCondition::Eq {
                        column_id: 2,
                        value: TriggerValue::NewColumn(1),
                    }],
                    cells: vec![TriggerCell {
                        column_id: 5,
                        value: TriggerValue::SelectedColumn(6),
                    }],
                },
            ],
        };
        let json = serde_json::to_value(&program).unwrap();
        let round: TriggerProgram = serde_json::from_value(json).unwrap();
        assert_eq!(program, round);
    }
}

fn parents_schema() -> Schema {
    Schema {
        schema_id: 0,
        columns: vec![col(1, "id", TypeId::Int64, pk_flags())],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: TableConstraints::default(),
        clustered: false,
    }
}

fn parents_with_status_schema() -> Schema {
    Schema {
        schema_id: 0,
        columns: vec![
            col(1, "id", TypeId::Int64, pk_flags()),
            col(2, "status", TypeId::Int64, ColumnFlags::empty()),
        ],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: TableConstraints::default(),
        clustered: false,
    }
}

fn children_schema() -> Schema {
    Schema {
        schema_id: 0,
        columns: vec![
            col(1, "id", TypeId::Int64, pk_flags()),
            col(2, "parent_id", TypeId::Int64, ColumnFlags::empty()),
            col(3, "status", TypeId::Int64, ColumnFlags::empty()),
        ],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: TableConstraints::default(),
        clustered: false,
    }
}

fn logs_schema() -> Schema {
    Schema {
        schema_id: 0,
        columns: vec![
            col(1, "id", TypeId::Int64, pk_flags()),
            col(2, "parent_id", TypeId::Int64, ColumnFlags::empty()),
        ],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: TableConstraints::default(),
        clustered: false,
    }
}

fn nullable_category_schema() -> Schema {
    Schema {
        schema_id: 0,
        columns: vec![
            col(1, "id", TypeId::Int64, pk_flags()),
            col(2, "value", TypeId::Int64, ColumnFlags::empty()),
            col(
                3,
                "category",
                TypeId::Bytes,
                ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            ),
        ],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: TableConstraints::default(),
        clustered: false,
    }
}

#[test]
fn trigger_delete_where_cleans_up_related_rows() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("parents", parents_schema()).unwrap();
    db.create_table("logs", logs_schema()).unwrap();

    let trigger = StoredTrigger::new(
        "parents_ad",
        TriggerDefinition {
            target: TriggerTarget::Table("parents".into()),
            timing: TriggerTiming::After,
            event: TriggerEvent::Delete,
            update_of: Vec::new(),
            target_columns: Vec::new(),
            when: None,
            program: TriggerProgram {
                steps: vec![TriggerStep::DeleteWhere {
                    table: "logs".into(),
                    conditions: vec![TriggerCondition::Eq {
                        column_id: 2,
                        value: TriggerValue::OldColumn(1),
                    }],
                }],
            },
        },
        0,
    )
    .unwrap();
    db.create_trigger(trigger).unwrap();

    db.transaction(|tx| {
        tx.put("parents", vec![(1, Value::Int64(1))]).map(|_| ())?;
        tx.put("parents", vec![(1, Value::Int64(2))]).map(|_| ())?;
        tx.put("logs", vec![(1, Value::Int64(1)), (2, Value::Int64(1))])
            .map(|_| ())?;
        tx.put("logs", vec![(1, Value::Int64(2)), (2, Value::Int64(1))])
            .map(|_| ())?;
        tx.put("logs", vec![(1, Value::Int64(3)), (2, Value::Int64(2))])
            .map(|_| ())?;
        Ok(())
    })
    .unwrap();

    let row_id = {
        let handle = db.table("parents").unwrap();
        let t = handle.lock();
        let rows = t.visible_rows(Snapshot::at(Epoch(u64::MAX))).unwrap();
        rows.iter()
            .find(|r| matches!(r.columns.get(&1), Some(Value::Int64(1))))
            .unwrap()
            .row_id
    };

    db.transaction(|tx| {
        tx.delete("parents", row_id)?;
        Ok(())
    })
    .unwrap();

    assert_eq!(db.table("logs").unwrap().lock().count(), 1);
}

#[test]
fn trigger_update_where_cascades_value() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("parents", parents_with_status_schema())
        .unwrap();
    db.create_table("children", children_schema()).unwrap();

    let trigger = StoredTrigger::new(
        "parents_au",
        TriggerDefinition {
            target: TriggerTarget::Table("parents".into()),
            timing: TriggerTiming::After,
            event: TriggerEvent::Update,
            update_of: Vec::new(),
            target_columns: Vec::new(),
            when: None,
            program: TriggerProgram {
                steps: vec![TriggerStep::UpdateWhere {
                    table: "children".into(),
                    conditions: vec![TriggerCondition::Eq {
                        column_id: 2,
                        value: TriggerValue::OldColumn(1),
                    }],
                    cells: vec![TriggerCell {
                        column_id: 3,
                        value: TriggerValue::NewColumn(2),
                    }],
                }],
            },
        },
        0,
    )
    .unwrap();
    db.create_trigger(trigger).unwrap();

    db.transaction(|tx| {
        tx.put("parents", vec![(1, Value::Int64(1)), (2, Value::Int64(0))])
            .map(|_| ())?;
        tx.put("parents", vec![(1, Value::Int64(2)), (2, Value::Int64(0))])
            .map(|_| ())?;
        tx.put(
            "children",
            vec![
                (1, Value::Int64(1)),
                (2, Value::Int64(1)),
                (3, Value::Int64(0)),
            ],
        )
        .map(|_| ())?;
        tx.put(
            "children",
            vec![
                (1, Value::Int64(2)),
                (2, Value::Int64(1)),
                (3, Value::Int64(0)),
            ],
        )
        .map(|_| ())?;
        tx.put(
            "children",
            vec![
                (1, Value::Int64(3)),
                (2, Value::Int64(2)),
                (3, Value::Int64(0)),
            ],
        )
        .map(|_| ())?;
        Ok(())
    })
    .unwrap();

    db.transaction(|tx| {
        tx.upsert(
            "parents",
            vec![(1, Value::Int64(1)), (2, Value::Int64(1))],
            mongreldb_core::UpsertAction::DoUpdate(vec![(2, Value::Int64(1))]),
        )
        .map(|_| ())
    })
    .unwrap();

    let handle = db.table("children").unwrap();
    let children = handle.lock();
    let rows = children
        .visible_rows(Snapshot::at(Epoch(u64::MAX)))
        .unwrap();
    assert_eq!(rows.len(), 3);
    for row in &rows {
        let parent_id = row.columns.get(&2).unwrap();
        let status = row.columns.get(&3).unwrap();
        if matches!(parent_id, Value::Int64(1)) {
            assert_eq!(status, &Value::Int64(1));
        } else {
            assert_eq!(status, &Value::Int64(0));
        }
    }
}

#[test]
fn trigger_when_clause_ranges_and_booleans() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("scores", nullable_category_schema())
        .unwrap();
    db.create_table("audit", nullable_category_schema())
        .unwrap();

    let trigger = StoredTrigger::new(
        "scores_ai",
        TriggerDefinition {
            target: TriggerTarget::Table("scores".into()),
            timing: TriggerTiming::After,
            event: TriggerEvent::Insert,
            update_of: Vec::new(),
            target_columns: Vec::new(),
            when: Some(TriggerExpr::Or {
                left: Box::new(TriggerExpr::And {
                    left: Box::new(TriggerExpr::Gt {
                        left: TriggerValue::NewColumn(2),
                        right: TriggerValue::Literal(Value::Int64(0)),
                    }),
                    right: Box::new(TriggerExpr::Lte {
                        left: TriggerValue::NewColumn(2),
                        right: TriggerValue::Literal(Value::Int64(100)),
                    }),
                }),
                right: Box::new(TriggerExpr::Not(Box::new(TriggerExpr::IsNull(
                    TriggerValue::NewColumn(3),
                )))),
            }),
            program: TriggerProgram {
                steps: vec![TriggerStep::Insert {
                    table: "audit".into(),
                    cells: vec![
                        TriggerCell {
                            column_id: 1,
                            value: TriggerValue::NewColumn(1),
                        },
                        TriggerCell {
                            column_id: 2,
                            value: TriggerValue::NewColumn(2),
                        },
                        TriggerCell {
                            column_id: 3,
                            value: TriggerValue::NewColumn(3),
                        },
                    ],
                }],
            },
        },
        0,
    )
    .unwrap();
    db.create_trigger(trigger).unwrap();

    db.transaction(|tx| {
        // Matches range: value=50.
        tx.put(
            "scores",
            vec![
                (1, Value::Int64(1)),
                (2, Value::Int64(50)),
                (3, Value::Null),
            ],
        )
        .map(|_| ())?;
        // Matches IS NOT NULL: value=0 but category present.
        tx.put(
            "scores",
            vec![
                (1, Value::Int64(2)),
                (2, Value::Int64(0)),
                (3, Value::Bytes(b"x".to_vec())),
            ],
        )
        .map(|_| ())?;
        // Does not match: value=0 and category null.
        tx.put(
            "scores",
            vec![(1, Value::Int64(3)), (2, Value::Int64(0)), (3, Value::Null)],
        )
        .map(|_| ())?;
        // Does not match: value=200 (out of range) and category null.
        tx.put(
            "scores",
            vec![
                (1, Value::Int64(4)),
                (2, Value::Int64(200)),
                (3, Value::Null),
            ],
        )
        .map(|_| ())?;
        Ok(())
    })
    .unwrap();

    assert_eq!(db.table("audit").unwrap().lock().count(), 2);
}
