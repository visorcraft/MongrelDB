use mongreldb_core::{
    ColumnDef, ColumnFlags, ColumnMask, Database, MaskStrategy, Permission, PolicyCommand,
    Principal, RowPolicy, Schema, SecurityCatalog, SecurityExpr, TypeId, Value,
};

fn schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
            },
            ColumnDef {
                id: 2,
                name: "owner".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
            ColumnDef {
                id: 3,
                name: "secret".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
            ColumnDef {
                id: 4,
                name: "value".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        clustered: true,
        ..Schema::default()
    }
}

fn admin() -> Principal {
    Principal {
        user_id: 0,
        created_epoch: 0,
        username: "admin".into(),
        is_admin: true,
        roles: Vec::new(),
        permissions: Vec::new(),
    }
}

fn user(name: &str) -> Principal {
    Principal {
        user_id: 0,
        created_epoch: 0,
        username: name.into(),
        is_admin: false,
        roles: Vec::new(),
        permissions: vec![
            Permission::SelectColumns {
                table: "docs".into(),
                columns: vec!["id".into(), "owner".into(), "secret".into()],
            },
            Permission::InsertColumns {
                table: "docs".into(),
                columns: vec!["id".into(), "owner".into(), "secret".into(), "value".into()],
            },
            Permission::UpdateColumns {
                table: "docs".into(),
                columns: vec!["value".into()],
            },
            Permission::Delete {
                table: "docs".into(),
            },
        ],
    }
}

fn cells(id: i64, owner: &str, secret: &str, value: i64) -> Vec<(u16, Value)> {
    vec![
        (1, Value::Int64(id)),
        (2, Value::Bytes(owner.as_bytes().to_vec())),
        (3, Value::Bytes(secret.as_bytes().to_vec())),
        (4, Value::Int64(value)),
    ]
}

fn security() -> SecurityCatalog {
    SecurityCatalog {
        rls_tables: vec!["docs".into()],
        policies: vec![RowPolicy {
            name: "owner_only".into(),
            table: "docs".into(),
            command: PolicyCommand::All,
            subjects: vec!["public".into()],
            permissive: true,
            using: Some(SecurityExpr::ColumnEqCurrentUser { column: 2 }),
            with_check: Some(SecurityExpr::ColumnEqCurrentUser { column: 2 }),
        }],
        masks: vec![ColumnMask {
            name: "hide_secret".into(),
            table: "docs".into(),
            column: 3,
            strategy: MaskStrategy::Redact {
                replacement: "***".into(),
            },
            exempt_subjects: vec!["unmasked".into()],
        }],
    }
}

#[test]
fn rls_columns_masks_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("docs", schema()).unwrap();
    let admin = admin();
    let mut transaction = db.begin_as(Some(admin.clone()));
    transaction
        .put("docs", cells(1, "alice", "a-secret", 10))
        .unwrap();
    transaction
        .put("docs", cells(2, "bob", "b-secret", 20))
        .unwrap();
    transaction.commit().unwrap();
    db.set_security_catalog_as(security(), Some(&admin))
        .unwrap();

    let alice = user("alice");
    let rows = db.rows_for("docs", Some(&alice)).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns.get(&1), Some(&Value::Int64(1)));
    assert_eq!(
        rows[0].columns.get(&3),
        Some(&Value::Bytes(b"***".to_vec()))
    );
    assert!(!rows[0].columns.contains_key(&4));
    assert_eq!(db.count_for("docs", Some(&alice)).unwrap(), 1);

    let alice_row_id = db
        .rows_for("docs", Some(&admin))
        .unwrap()
        .into_iter()
        .find(|row| row.columns.get(&1) == Some(&Value::Int64(1)))
        .unwrap()
        .row_id;
    let mut update = db.begin_as(Some(alice.clone()));
    update
        .update_many("docs", vec![(alice_row_id, vec![(4, Value::Int64(11))])])
        .unwrap();
    update.commit().unwrap();

    let current_alice_row = db
        .rows_for("docs", Some(&admin))
        .unwrap()
        .into_iter()
        .find(|row| row.columns.get(&1) == Some(&Value::Int64(1)))
        .unwrap();
    assert_eq!(current_alice_row.columns.get(&4), Some(&Value::Int64(11)));
    assert_eq!(
        current_alice_row.columns.get(&3),
        Some(&Value::Bytes(b"a-secret".to_vec()))
    );

    let mut owner_editor = alice.clone();
    owner_editor.permissions.push(Permission::UpdateColumns {
        table: "docs".into(),
        columns: vec!["owner".into()],
    });
    let mut rls_denied = db.begin_as(Some(owner_editor));
    rls_denied
        .update_many(
            "docs",
            vec![(
                current_alice_row.row_id,
                vec![(2, Value::Bytes(b"bob".to_vec()))],
            )],
        )
        .unwrap();
    assert!(matches!(
        rls_denied.commit(),
        Err(mongreldb_core::MongrelError::PermissionDenied { .. })
    ));

    let mut allowed = db.begin_as(Some(alice.clone()));
    allowed.put("docs", cells(3, "alice", "new", 30)).unwrap();
    allowed.commit().unwrap();

    let mut denied = db.begin_as(Some(alice.clone()));
    denied.put("docs", cells(4, "bob", "stolen", 40)).unwrap();
    assert!(matches!(
        denied.commit(),
        Err(mongreldb_core::MongrelError::PermissionDenied { .. })
    ));

    let bob_row = db
        .rows_for("docs", Some(&admin))
        .unwrap()
        .into_iter()
        .find(|row| row.columns.get(&1) == Some(&Value::Int64(2)))
        .unwrap();
    let mut denied = db.begin_as(Some(alice));
    denied.delete("docs", bob_row.row_id).unwrap();
    assert!(matches!(
        denied.commit(),
        Err(mongreldb_core::MongrelError::PermissionDenied { .. })
    ));

    drop(db);
    let reopened = Database::open(dir.path()).unwrap();
    assert_eq!(reopened.security_catalog(), security());
    assert_eq!(
        reopened.rows_for("docs", Some(&user("bob"))).unwrap().len(),
        1
    );
}
