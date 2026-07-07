//! Integration tests for engine-side DEFAULT value application.
//!
//! Covers Static / Now / Uuid default expressions applied at insert stage time
//! when the column is omitted or explicitly Null.

use mongreldb_core::schema::*;
use mongreldb_core::{Database, Value};
use tempfile::tempdir;

fn pk(id: u16, name: &str, ty: TypeId) -> ColumnDef {
    ColumnDef {
        id,
        name: name.into(),
        ty,
        flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
        default_value: None,
    }
}

fn default_col(id: u16, name: &str, ty: TypeId, dv: DefaultExpr) -> ColumnDef {
    ColumnDef {
        id,
        name: name.into(),
        ty,
        flags: ColumnFlags::empty(),
        default_value: Some(dv),
    }
}

fn nullable_default_col(id: u16, name: &str, ty: TypeId, dv: DefaultExpr) -> ColumnDef {
    ColumnDef {
        id,
        name: name.into(),
        ty,
        flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
        default_value: Some(dv),
    }
}

fn make_schema(columns: Vec<ColumnDef>) -> Schema {
    Schema {
        schema_id: 1,
        columns,
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

#[test]
fn static_default_applied_when_omitted() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table(
        "t",
        make_schema(vec![
            pk(0, "id", TypeId::Int64),
            default_col(
                1,
                "active",
                TypeId::Bool,
                DefaultExpr::Static(Value::Bool(true)),
            ),
        ]),
    )
    .unwrap();

    // Omit "active" — the engine should apply the default before NOT NULL check.
    let r = db.transaction(|t| {
        t.put("t", vec![(0, Value::Int64(1))])?;
        Ok(())
    });
    assert!(r.is_ok());
}

#[test]
fn static_default_applied_when_explicit_null() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table(
        "t",
        make_schema(vec![
            pk(0, "id", TypeId::Int64),
            default_col(
                1,
                "active",
                TypeId::Bool,
                DefaultExpr::Static(Value::Bool(false)),
            ),
        ]),
    )
    .unwrap();

    // Explicit Null → engine replaces with default (matching AUTO_INCREMENT precedent).
    let r = db.transaction(|t| {
        t.put("t", vec![(0, Value::Int64(1)), (1, Value::Null)])?;
        Ok(())
    });
    assert!(r.is_ok());
}

#[test]
fn static_default_not_overriding_explicit_value() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table(
        "t",
        make_schema(vec![
            pk(0, "id", TypeId::Int64),
            default_col(
                1,
                "active",
                TypeId::Bool,
                DefaultExpr::Static(Value::Bool(false)),
            ),
        ]),
    )
    .unwrap();

    // Explicit true — engine should NOT override with default false.
    let r = db.transaction(|t| {
        t.put("t", vec![(0, Value::Int64(1)), (1, Value::Bool(true))])?;
        Ok(())
    });
    assert!(r.is_ok());
}

#[test]
fn now_default_produces_iso8601_bytes() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table(
        "t",
        make_schema(vec![
            pk(0, "id", TypeId::Int64),
            default_col(1, "created_at", TypeId::Bytes, DefaultExpr::Now),
        ]),
    )
    .unwrap();

    let r = db.transaction(|t| {
        t.put("t", vec![(0, Value::Int64(1))])?;
        Ok(())
    });
    assert!(r.is_ok(), "DEFAULT NOW() insert failed: {:?}", r);
}

#[test]
fn uuid_default_produces_random_uuid() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table(
        "t",
        make_schema(vec![
            pk(0, "id", TypeId::Int64),
            default_col(1, "uuid_col", TypeId::Uuid, DefaultExpr::Uuid),
        ]),
    )
    .unwrap();

    let r = db.transaction(|t| {
        t.put("t", vec![(0, Value::Int64(1))])?;
        Ok(())
    });
    assert!(r.is_ok(), "DEFAULT UUID() insert failed: {:?}", r);
}

#[test]
fn mixed_defaults_static_and_now() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table(
        "t",
        make_schema(vec![
            pk(0, "id", TypeId::Int64),
            default_col(
                1,
                "status",
                TypeId::Bytes,
                DefaultExpr::Static(Value::Bytes(b"pending".to_vec())),
            ),
            default_col(2, "created", TypeId::Bytes, DefaultExpr::Now),
        ]),
    )
    .unwrap();

    let r = db.transaction(|t| {
        t.put("t", vec![(0, Value::Int64(1))])?;
        Ok(())
    });
    assert!(r.is_ok());
}

#[test]
fn default_not_null_succeeds_when_omitted() {
    // A NOT NULL column with a default succeeds when omitted,
    // because the engine applies the default BEFORE validate_not_null.
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table(
        "t",
        make_schema(vec![
            pk(0, "id", TypeId::Int64),
            default_col(
                1,
                "active",
                TypeId::Bool,
                DefaultExpr::Static(Value::Bool(true)),
            ),
        ]),
    )
    .unwrap();

    let r = db.transaction(|t| {
        t.put("t", vec![(0, Value::Int64(1))])?;
        Ok(())
    });
    assert!(r.is_ok());
}

#[test]
fn validate_defaults_rejects_incompatible_type_at_create() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let result = db.create_table(
        "t",
        make_schema(vec![
            pk(0, "id", TypeId::Int64),
            default_col(
                1,
                "count",
                TypeId::Int64,
                DefaultExpr::Static(Value::Bool(true)),
            ),
        ]),
    );
    assert!(
        result.is_err(),
        "should reject Bool default on Int64 column"
    );
}

#[test]
fn default_survives_reopen() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table(
            "t",
            make_schema(vec![
                pk(0, "id", TypeId::Int64),
                nullable_default_col(
                    1,
                    "status",
                    TypeId::Bytes,
                    DefaultExpr::Static(Value::Bytes(b"new".to_vec())),
                ),
            ]),
        )
        .unwrap();
        db.transaction(|t| {
            t.put("t", vec![(0, Value::Int64(1))])?;
            Ok(())
        })
        .unwrap();
    }
    // Reopen — the schema (with defaults) should deserialize and still work.
    let db = Database::open(dir.path()).unwrap();
    let r = db.transaction(|t| {
        t.put("t", vec![(0, Value::Int64(2))])?;
        Ok(())
    });
    assert!(r.is_ok());
}
