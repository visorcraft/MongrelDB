use mongreldb_core::query::{
    Condition, Fusion, NamedRetriever, Query, Retriever, SearchRequest, SetMember,
};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Table, Value};
use tempfile::tempdir;

fn schema() -> Schema {
    let column = |id: u16, name: &str, ty: TypeId, primary_key: bool| ColumnDef {
        id,
        name: name.into(),
        ty,
        flags: if primary_key {
            ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY)
        } else {
            ColumnFlags::empty()
        },
        default_value: None,
    };
    Schema {
        schema_id: 1,
        columns: vec![
            column(1, "id", TypeId::Int64, true),
            column(2, "embedding", TypeId::Embedding { dim: 2 }, false),
            column(3, "sparse", TypeId::Bytes, false),
            column(4, "members", TypeId::Bytes, false),
        ],
        indexes: vec![
            IndexDef {
                name: "ann".into(),
                column_id: 2,
                kind: IndexKind::Ann,
                predicate: None,
                options: Default::default(),
            },
            IndexDef {
                name: "sparse".into(),
                column_id: 3,
                kind: IndexKind::Sparse,
                predicate: None,
                options: Default::default(),
            },
            IndexDef {
                name: "minhash".into(),
                column_id: 4,
                kind: IndexKind::MinHash,
                predicate: None,
                options: Default::default(),
            },
        ],
        ..Schema::default()
    }
}

fn row(id: i64, embedding: Vec<f32>, sparse: &[(u32, f32)], members: &[&str]) -> Vec<(u16, Value)> {
    vec![
        (1, Value::Int64(id)),
        (2, Value::Embedding(embedding)),
        (3, Value::Bytes(bincode::serialize(sparse).unwrap())),
        (4, Value::Bytes(serde_json::to_vec(members).unwrap())),
    ]
}

fn ann() -> Retriever {
    Retriever::Ann {
        column_id: 2,
        query: vec![1.0, 1.0],
        k: 1,
    }
}

fn sparse() -> Retriever {
    Retriever::Sparse {
        column_id: 3,
        query: vec![(7, 1.0)],
        k: 1,
    }
}

fn minhash() -> Retriever {
    Retriever::MinHash {
        column_id: 4,
        members: ["a", "b", "c", "d"]
            .into_iter()
            .map(|member| SetMember::String(member.into()))
            .collect(),
        k: 1,
    }
}

#[test]
fn standalone_retrievers_do_not_see_put_before_commit() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    let committed = table
        .put(row(1, vec![-1.0, -1.0], &[(7, 0.1)], &["a", "b", "c", "d"]))
        .unwrap();
    table.commit().unwrap();
    let pending = table
        .put(row(2, vec![1.0, 1.0], &[(7, 10.0)], &["a", "b", "c", "d"]))
        .unwrap();
    let pending_two = table
        .put(row(3, vec![2.0, 2.0], &[(7, 20.0)], &["a", "b", "c", "d"]))
        .unwrap();
    let snapshot = table.snapshot();

    assert!(table.get(pending, snapshot).is_none());
    assert!(table.get(pending_two, snapshot).is_none());
    assert_eq!(table.count(), 1);
    for retriever in [ann(), sparse(), minhash()] {
        let hits = table.retrieve(&retriever).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].row_id, committed);
        assert_eq!(hits[0].rank, 1);
    }

    let ann_condition = Condition::Ann {
        column_id: 2,
        query: vec![1.0, 1.0],
        k: 1,
    };
    assert_eq!(
        table.query(&Query::new().and(ann_condition)).unwrap().len(),
        1
    );
    assert_eq!(
        table
            .count_conditions(
                &[Condition::SparseMatch {
                    column_id: 3,
                    query: vec![(7, 1.0)],
                    k: 1,
                }],
                snapshot,
            )
            .unwrap(),
        Some(1)
    );

    let hybrid = table
        .search(&SearchRequest {
            must: Vec::new(),
            retrievers: vec![NamedRetriever {
                name: "ann".into(),
                weight: 1.0,
                retriever: ann(),
            }],
            fusion: Fusion::ReciprocalRank { constant: 60 },
            limit: 1,
            projection: None,
        })
        .unwrap();
    assert_eq!(hybrid.len(), 1);
    assert_eq!(hybrid[0].row_id, committed);

    drop(table);
    let mut reopened = Table::open(dir.path()).unwrap();
    let reopened_hits = reopened.retrieve(&ann()).unwrap();
    assert_eq!(reopened_hits.len(), 1);
    assert_eq!(reopened_hits[0].row_id, committed);

    let row_ids = reopened
        .put_batch(vec![
            row(4, vec![1.0, 1.0], &[(7, 10.0)], &["a"]),
            row(5, vec![2.0, 2.0], &[(7, 20.0)], &["a"]),
        ])
        .unwrap();
    assert_eq!(reopened.count(), 1);
    reopened.commit().unwrap();
    assert_eq!(reopened.count(), 3);
    for row_id in row_ids {
        assert!(reopened.get(row_id, reopened.snapshot()).is_some());
    }
}
