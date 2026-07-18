//! §5.6: anchored-prefix LIKE via `Condition::BytesPrefix`. On a bitmap-
//! indexed column, `BytesPrefix { prefix }` enumerates bitmap keys that start
//! with the prefix and unions their row-ids — an exact match (no residual),
//! tighter than the FM substring superset (`FmContains`).

use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Condition, Query, Table, Value};
use tempfile::tempdir;

fn schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "name".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "name_bm".into(),
            column_id: 2,
            kind: IndexKind::Bitmap,
            predicate: None,
            options: Default::default(),
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn key(s: &str) -> Vec<u8> {
    Value::Bytes(s.as_bytes().to_vec()).encode_key()
}

#[test]
fn bytes_prefix_matches_exact_prefix_only() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();

    let names = ["apple", "apricot", "banana", "application", "cherry"];
    for (i, n) in names.iter().enumerate() {
        db.put(vec![
            (1, Value::Int64(i as i64)),
            (2, Value::Bytes(n.as_bytes().to_vec())),
        ])
        .unwrap();
    }
    db.flush().unwrap();

    let snap = db.snapshot();
    let cond = Condition::BytesPrefix {
        column_id: 2,
        prefix: key("app"),
    };
    let count = db
        .count_conditions(std::slice::from_ref(&cond), snap)
        .unwrap()
        .unwrap();
    assert_eq!(count, 2, "prefix 'app' matches apple + application");

    let mut rows = db.query(&Query::new().and(cond)).unwrap();
    rows.sort_by_key(|r| match r.columns.get(&1) {
        Some(Value::Int64(v)) => *v,
        _ => i64::MIN,
    });
    let got: Vec<&str> = rows
        .iter()
        .filter_map(|r| match r.columns.get(&2) {
            Some(Value::Bytes(b)) => std::str::from_utf8(b).ok(),
            _ => None,
        })
        .collect();
    assert_eq!(got, vec!["apple", "application"]);
}

#[test]
fn bytes_prefix_no_match_returns_empty() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.put(vec![
        (1, Value::Int64(1)),
        (2, Value::Bytes(b"hello".to_vec())),
    ])
    .unwrap();
    db.flush().unwrap();

    let snap = db.snapshot();
    let cond = Condition::BytesPrefix {
        column_id: 2,
        prefix: key("xyz"),
    };
    let count = db
        .count_conditions(std::slice::from_ref(&cond), snap)
        .unwrap()
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn bytes_prefix_includes_unflushed_overlay_rows() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    // Flushed.
    db.put(vec![
        (1, Value::Int64(1)),
        (2, Value::Bytes(b"apex".to_vec())),
    ])
    .unwrap();
    db.flush().unwrap();
    // Unflushed overlay.
    db.put(vec![
        (1, Value::Int64(2)),
        (2, Value::Bytes(b"apple".to_vec())),
    ])
    .unwrap();
    db.commit().unwrap();

    let snap = db.snapshot();
    let cond = Condition::BytesPrefix {
        column_id: 2,
        prefix: key("ap"),
    };
    let count = db
        .count_conditions(std::slice::from_ref(&cond), snap)
        .unwrap()
        .unwrap();
    assert_eq!(count, 2, "prefix 'ap' spans flushed + overlay");
}
