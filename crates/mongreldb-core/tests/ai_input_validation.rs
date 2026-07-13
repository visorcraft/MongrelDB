use mongreldb_core::query::Retriever;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Database, Table, Value};
use tempfile::tempdir;

fn schema(dim: u32) -> Schema {
    Schema {
        schema_id: 1,
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
                name: "embedding".into(),
                ty: TypeId::Embedding { dim },
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "ann".into(),
            column_id: 2,
            kind: IndexKind::Ann,
            predicate: None,
            options: Default::default(),
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

#[test]
fn malformed_embeddings_return_errors() {
    assert!(Table::create(tempdir().unwrap().path(), schema(0), 1).is_err());
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(8), 1).unwrap();
    for embedding in [
        vec![1.0; 7],
        vec![1.0; 9],
        vec![f32::NAN; 8],
        vec![f32::INFINITY; 8],
    ] {
        assert!(table
            .put(vec![(1, Value::Int64(1)), (2, Value::Embedding(embedding))])
            .is_err());
    }
    assert!(table
        .retrieve(&Retriever::Ann {
            column_id: 2,
            query: vec![1.0; 7],
            k: 1,
        })
        .is_err());
}

#[test]
fn conflicting_representation_indexes_are_rejected() {
    let mut schema = schema(8);
    schema.columns.push(ColumnDef {
        id: 3,
        name: "payload".into(),
        ty: TypeId::Bytes,
        flags: ColumnFlags::empty(),
        default_value: None,
    });
    for (name, kind) in [("sparse", IndexKind::Sparse), ("set", IndexKind::MinHash)] {
        schema.indexes.push(IndexDef {
            name: name.into(),
            column_id: 3,
            kind,
            predicate: None,
            options: Default::default(),
        });
    }
    assert!(Table::create(tempdir().unwrap().path(), schema, 1).is_err());
}

#[test]
fn specialized_columns_require_decodable_value_variants() {
    let nullable = ColumnFlags::empty().with(ColumnFlags::NULLABLE);
    let schema = Schema {
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
                name: "sparse".into(),
                ty: TypeId::Bytes,
                flags: nullable,
                default_value: None,
            },
            ColumnDef {
                id: 3,
                name: "members".into(),
                ty: TypeId::Bytes,
                flags: nullable,
                default_value: None,
            },
            ColumnDef {
                id: 4,
                name: "text".into(),
                ty: TypeId::Bytes,
                flags: nullable,
                default_value: None,
            },
        ],
        indexes: vec![
            IndexDef {
                name: "sparse".into(),
                column_id: 2,
                kind: IndexKind::Sparse,
                predicate: None,
                options: Default::default(),
            },
            IndexDef {
                name: "minhash".into(),
                column_id: 3,
                kind: IndexKind::MinHash,
                predicate: None,
                options: Default::default(),
            },
            IndexDef {
                name: "fm".into(),
                column_id: 4,
                kind: IndexKind::FmIndex,
                predicate: None,
                options: Default::default(),
            },
        ],
        ..Schema::default()
    };
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema, 1).unwrap();
    let base = |id| {
        vec![
            (1, Value::Int64(id)),
            (2, Value::Null),
            (3, Value::Null),
            (4, Value::Null),
        ]
    };
    assert!(table
        .put({
            let mut row = base(1);
            row[1].1 = Value::Int64(7);
            row
        })
        .is_err());
    assert!(table
        .put({
            let mut row = base(2);
            row[1].1 = Value::Bytes(b"not a sparse vector".to_vec());
            row
        })
        .is_err());
    assert!(table
        .put({
            let mut row = base(3);
            row[1].1 = Value::Bool(true);
            row
        })
        .is_err());
    assert!(table
        .put({
            let mut row = base(4);
            row[2].1 = Value::Embedding(vec![1.0]);
            row
        })
        .is_err());
    assert!(table
        .put({
            let mut row = base(5);
            row[2].1 = Value::Int64(7);
            row
        })
        .is_err());
    assert!(table
        .put({
            let mut row = base(6);
            row[3].1 = Value::Bool(true);
            row
        })
        .is_err());
    assert_eq!(table.count(), 0);
    let valid = vec![
        (1, Value::Int64(5)),
        (
            2,
            Value::Bytes(bincode::serialize(&vec![(7u32, 1.0f32)]).unwrap()),
        ),
        (3, Value::Bytes(serde_json::to_vec(&["a"]).unwrap())),
        (4, Value::Bytes(b"text".to_vec())),
    ];
    let row_id = table.put(valid).unwrap();
    table.commit().unwrap();
    drop(table);
    let reopened = Table::open(dir.path()).unwrap();
    assert!(reopened.get(row_id, reopened.snapshot()).is_some());
}

#[test]
fn cross_table_transaction_rejects_specialized_value_before_commit() {
    let schema = Schema {
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
                name: "sparse".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "sparse".into(),
            column_id: 2,
            kind: IndexKind::Sparse,
            predicate: None,
            options: Default::default(),
        }],
        ..Schema::default()
    };
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("a", schema.clone()).unwrap();
    db.create_table("b", schema).unwrap();
    assert!(db
        .transaction(|tx| {
            tx.put(
                "a",
                vec![
                    (1, Value::Int64(1)),
                    (
                        2,
                        Value::Bytes(bincode::serialize(&vec![(7u32, 1.0f32)]).unwrap()),
                    ),
                ],
            )?;
            tx.put("b", vec![(1, Value::Int64(1)), (2, Value::Bool(true))])
        })
        .is_err());
    assert_eq!(db.table("a").unwrap().lock().count(), 0);
    assert_eq!(db.table("b").unwrap().lock().count(), 0);
}
