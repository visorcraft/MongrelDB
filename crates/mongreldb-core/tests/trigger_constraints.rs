use mongreldb_core::constraint::{
    CheckConstraint, CheckExpr, FkAction, ForeignKey, TableConstraints, UniqueConstraint,
};
use mongreldb_core::{
    ColumnDef, ColumnFlags, Database, Schema, StoredTrigger, TriggerCell, TriggerDefinition,
    TriggerEvent, TriggerProgram, TriggerStep, TriggerTarget, TriggerTiming, TriggerValue, TypeId,
    Value,
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
