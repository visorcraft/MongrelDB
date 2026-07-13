use mongreldb_core::query::{
    AnnRerankRequest, Condition, Fusion, NamedRetriever, Retriever, SearchRequest, SetMember,
    VectorMetric, MAX_FINAL_LIMIT, MAX_PROJECTION_COLUMNS, MAX_RETRIEVERS, MAX_RETRIEVER_K,
    MAX_SET_MEMBERS, MAX_SPARSE_TERMS,
};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{MongrelError, Table, Value};

fn table() -> (tempfile::TempDir, Table) {
    let dir = tempfile::tempdir().unwrap();
    let column = |id: u16, name: &str, ty: TypeId, primary: bool| ColumnDef {
        id,
        name: name.into(),
        ty,
        flags: if primary {
            ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY)
        } else {
            ColumnFlags::empty()
        },
        default_value: None,
    };
    let schema = Schema {
        columns: vec![
            column(1, "id", TypeId::Int64, true),
            column(2, "embedding", TypeId::Embedding { dim: 8 }, false),
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
    };
    let mut table = Table::create(dir.path(), schema, 1).unwrap();
    table
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Embedding(vec![1.0; 8])),
            (
                3,
                Value::Bytes(bincode::serialize(&vec![(1u32, f32::MAX)]).unwrap()),
            ),
            (4, Value::Bytes(serde_json::to_vec(&vec!["a"]).unwrap())),
        ])
        .unwrap();
    table.commit().unwrap();
    (dir, table)
}

fn ann(k: usize) -> Retriever {
    Retriever::Ann {
        column_id: 2,
        query: vec![1.0; 8],
        k,
    }
}

fn search(retrievers: Vec<NamedRetriever>, limit: usize) -> SearchRequest {
    SearchRequest {
        must: vec![],
        retrievers,
        fusion: Fusion::ReciprocalRank { constant: 60 },
        limit,
        projection: Some(vec![1]),
    }
}

fn named(name: String, retriever: Retriever) -> NamedRetriever {
    NamedRetriever {
        name,
        weight: 1.0,
        retriever,
    }
}

#[test]
fn public_ai_cardinalities_fail_closed() {
    let (_dir, mut table) = table();
    assert!(matches!(
        table.query(&mongreldb_core::Query::new().with_limit(usize::MAX)),
        Err(MongrelError::InvalidArgument(_))
    ));
    let error = table
        .search(&search(vec![named("ann".into(), ann(1))], usize::MAX))
        .unwrap_err();
    assert!(matches!(error, MongrelError::InvalidArgument(_)));

    assert!(matches!(
        table.retrieve(&ann(MAX_RETRIEVER_K + 1)),
        Err(MongrelError::InvalidArgument(_))
    ));

    let too_many = (0..=MAX_RETRIEVERS)
        .map(|index| named(format!("ann{index}"), ann(1)))
        .collect();
    assert!(matches!(
        table.search(&search(too_many, 1)),
        Err(MongrelError::InvalidArgument(_))
    ));

    assert!(matches!(
        table.retrieve(&Retriever::Sparse {
            column_id: 3,
            query: vec![(1, 1.0); MAX_SPARSE_TERMS + 1],
            k: 1,
        }),
        Err(MongrelError::InvalidArgument(_))
    ));
    assert!(matches!(
        table.retrieve(&Retriever::MinHash {
            column_id: 4,
            members: vec![SetMember::Boolean(true); MAX_SET_MEMBERS + 1],
            k: 1,
        }),
        Err(MongrelError::InvalidArgument(_))
    ));

    let mut request = search(vec![named("ann".into(), ann(1))], 1);
    request.projection = Some(vec![1; MAX_PROJECTION_COLUMNS + 1]);
    assert!(matches!(
        table.search(&request),
        Err(MongrelError::InvalidArgument(_))
    ));

    let mut request = search(vec![named("ann".into(), ann(1))], 1);
    request.must.push(Condition::Ann {
        column_id: 2,
        query: vec![1.0; 8],
        k: 1,
    });
    assert!(matches!(
        table.search(&request),
        Err(MongrelError::InvalidArgument(_))
    ));
}

#[test]
fn ai_scores_remain_finite_or_return_typed_errors() {
    let (_dir, mut table) = table();
    let sparse = table
        .retrieve(&Retriever::Sparse {
            column_id: 3,
            query: vec![(1, f32::MAX)],
            k: 1,
        })
        .unwrap();
    let mongreldb_core::query::RetrieverScore::SparseDotProduct(score) = sparse[0].score else {
        panic!("expected sparse score")
    };
    assert!(score.is_finite());

    let mut request = search(vec![named("ann".into(), ann(1))], MAX_FINAL_LIMIT);
    request.retrievers[0].weight = f64::MAX;
    assert!(matches!(
        table.search(&request),
        Err(MongrelError::InvalidArgument(_))
    ));

    let hits = table
        .ann_rerank(&AnnRerankRequest {
            column_id: 2,
            query: vec![1.0; 8],
            candidate_k: 1,
            limit: 1,
            metric: VectorMetric::Cosine,
        })
        .unwrap();
    assert!(hits[0].exact_score.is_finite());
}
