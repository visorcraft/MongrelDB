use mongreldb_core::query::Retriever;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{
    Database, PolicyCommand, Principal, RowPolicy, SecurityCatalog, SecurityExpr, Value,
};
use std::sync::Arc;

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
                name: "sparse".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "sparse".into(),
            column_id: 3,
            kind: IndexKind::Sparse,
            predicate: None,
            options: Default::default(),
        }],
        ..Schema::default()
    }
}

fn admin() -> Principal {
    Principal {
        username: "admin".into(),
        is_admin: true,
        roles: Vec::new(),
        permissions: Vec::new(),
    }
}

fn alice() -> Principal {
    Principal {
        username: "alice".into(),
        is_admin: false,
        roles: Vec::new(),
        permissions: Vec::new(),
    }
}

fn policy(expression: SecurityExpr) -> SecurityCatalog {
    SecurityCatalog {
        rls_tables: vec!["docs".into()],
        policies: vec![RowPolicy {
            name: "owner_only".into(),
            table: "docs".into(),
            command: PolicyCommand::Select,
            subjects: vec!["public".into()],
            permissive: true,
            using: Some(expression),
            with_check: None,
        }],
        masks: Vec::new(),
    }
}

#[test]
fn stale_authorization_snapshot_is_rejected_before_result_publish() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("docs", schema()).unwrap();
    db.transaction(|transaction| {
        transaction.put(
            "docs",
            vec![
                (1, Value::Int64(1)),
                (2, Value::Bytes(b"alice".to_vec())),
                (
                    3,
                    Value::Bytes(mongreldb_core::query::encode_sparse_vector(&[(1, 1.0)])?),
                ),
            ],
        )?;
        Ok(())
    })
    .unwrap();
    db.set_security_catalog_as(
        policy(SecurityExpr::ColumnEqCurrentUser { column: 2 }),
        Some(&admin()),
    )
    .unwrap();
    let principal = alice();
    let stale = db
        .authorized_read_snapshot("docs", Some(&principal))
        .unwrap();
    assert_eq!(stale.allowed_row_ids.as_ref().unwrap().len(), 1);

    db.set_security_catalog_as(
        policy(SecurityExpr::ColumnEqValue {
            column: 2,
            value: Value::Bytes(b"bob".to_vec()),
        }),
        Some(&admin()),
    )
    .unwrap();
    assert!(!db.authorized_read_snapshot_valid(&stale));

    let handle = db.table("docs").unwrap();
    let stale_hits = handle
        .lock()
        .retrieve_at(
            &Retriever::Sparse {
                column_id: 3,
                query: vec![(1, 1.0)],
                k: 1,
            },
            stale.table_snapshot,
            stale.allowed_row_ids.as_ref(),
        )
        .unwrap();
    assert_eq!(stale_hits.len(), 1);

    let fresh = db
        .authorized_read_snapshot("docs", Some(&principal))
        .unwrap();
    assert!(fresh.allowed_row_ids.as_ref().unwrap().is_empty());
    let fresh_hits = handle
        .lock()
        .retrieve_at(
            &Retriever::Sparse {
                column_id: 3,
                query: vec![(1, 1.0)],
                k: 1,
            },
            fresh.table_snapshot,
            fresh.allowed_row_ids.as_ref(),
        )
        .unwrap();
    assert!(fresh_hits.is_empty());
}

#[test]
fn authorized_read_retries_security_change_and_uses_table_local_cache_generation() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("docs", schema()).unwrap();
    db.create_table("other", schema()).unwrap();
    db.transaction(|transaction| {
        transaction.put(
            "docs",
            vec![
                (1, Value::Int64(1)),
                (2, Value::Bytes(b"alice".to_vec())),
                (
                    3,
                    Value::Bytes(mongreldb_core::query::encode_sparse_vector(&[(1, 1.0)])?),
                ),
            ],
        )?;
        Ok(())
    })
    .unwrap();
    db.set_security_catalog_as(
        policy(SecurityExpr::ColumnEqCurrentUser { column: 2 }),
        Some(&admin()),
    )
    .unwrap();
    let principal = alice();
    db.authorized_read_snapshot("docs", Some(&principal))
        .unwrap();
    db.authorized_read_snapshot("docs", Some(&principal))
        .unwrap();
    let before = db.rls_cache_stats();
    assert_eq!(before.misses, 1);
    assert_eq!(before.hits, 1);

    db.transaction(|transaction| {
        transaction.put(
            "other",
            vec![
                (1, Value::Int64(1)),
                (2, Value::Bytes(b"other".to_vec())),
                (
                    3,
                    Value::Bytes(mongreldb_core::query::encode_sparse_vector(&[(2, 1.0)])?),
                ),
            ],
        )?;
        Ok(())
    })
    .unwrap();
    db.authorized_read_snapshot("docs", Some(&principal))
        .unwrap();
    let after = db.rls_cache_stats();
    assert_eq!(after.misses, before.misses);
    assert_eq!(after.hits, before.hits + 1);
    assert!(after.bytes > 0);

    let calls = std::cell::Cell::new(0usize);
    let rows = db
        .with_authorized_read(
            "docs",
            Some(&principal),
            false,
            |table, snapshot, allowed, _| {
                calls.set(calls.get() + 1);
                if calls.get() == 1 {
                    db.set_security_catalog_as(
                        policy(SecurityExpr::ColumnEqValue {
                            column: 2,
                            value: Value::Bytes(b"bob".to_vec()),
                        }),
                        Some(&admin()),
                    )?;
                }
                table.query_at_with_allowed(&mongreldb_core::Query::new(), snapshot, allowed)
            },
        )
        .unwrap();
    assert_eq!(calls.get(), 2);
    assert!(rows.is_empty());
}
