use mongreldb_core::query::Retriever;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Table, Value};
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
