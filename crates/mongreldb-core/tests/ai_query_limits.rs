use mongreldb_core::query::{
    AiExecutionContext, AnnRerankRequest, Condition, Fusion, NamedRetriever, Retriever,
    SearchRequest, SetMember, VectorMetric, MAX_FINAL_LIMIT, MAX_FUSED_CANDIDATES,
    MAX_PROJECTION_COLUMNS, MAX_RETRIEVERS, MAX_RETRIEVER_K, MAX_SET_MEMBERS, MAX_SPARSE_TERMS,
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
        rerank: None,
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
fn native_query_offset_applies_before_limit() {
    let (_dir, mut table) = table();
    for id in 2..=4 {
        table
            .put(vec![
                (1, Value::Int64(id)),
                (2, Value::Embedding(vec![1.0; 8])),
                (
                    3,
                    Value::Bytes(bincode::serialize(&vec![(id as u32, 1.0)]).unwrap()),
                ),
                (4, Value::Bytes(b"[]".to_vec())),
            ])
            .unwrap();
    }
    table.commit().unwrap();

    let rows = table
        .query(&mongreldb_core::Query::new().with_offset(2).with_limit(1))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[&1], Value::Int64(3));

    let range = Condition::Range {
        column_id: 1,
        lo: 1,
        hi: 4,
    };
    let first_page = table
        .query_cached(
            &mongreldb_core::Query::new()
                .and(range.clone())
                .with_limit(1),
        )
        .unwrap();
    let third_page = table
        .query_cached(
            &mongreldb_core::Query::new()
                .and(range)
                .with_offset(2)
                .with_limit(1),
        )
        .unwrap();
    assert_eq!(first_page[0].columns[&1], Value::Int64(1));
    assert_eq!(third_page[0].columns[&1], Value::Int64(3));
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

#[test]
fn actual_work_budget_and_cancellation_fail_with_typed_errors() {
    let (_dir, mut table) = table();
    let snapshot = table.snapshot();
    let sparse = Retriever::Sparse {
        column_id: 3,
        query: vec![(1, 1.0)],
        k: 1,
    };

    let exhausted = AiExecutionContext::new(None, 0);
    assert!(matches!(
        table.retrieve_at_with_candidate_authorization_and_context(
            &sparse,
            snapshot,
            None,
            Some(&exhausted),
        ),
        Err(MongrelError::WorkBudgetExceeded)
    ));

    let cancelled = AiExecutionContext::new(None, usize::MAX);
    cancelled.cancel();
    assert!(matches!(
        table.search_at_with_candidate_authorization_and_context(
            &search(vec![named("sparse".into(), sparse.clone())], 1),
            snapshot,
            None,
            Some(&cancelled),
        ),
        Err(MongrelError::Cancelled)
    ));
    assert_eq!(table.retrieve(&sparse).unwrap().len(), 1);
}

#[test]
fn fused_union_ceiling_projection_charging_and_zero_weight_are_enforced() {
    let (_dir, mut table) = table();
    table
        .put(vec![
            (1, Value::Int64(2)),
            (2, Value::Embedding(vec![-1.0; 8])),
            (
                3,
                Value::Bytes(bincode::serialize(&vec![(2u32, 1.0f32)]).unwrap()),
            ),
            (4, Value::Bytes(serde_json::to_vec(&vec!["b"]).unwrap())),
        ])
        .unwrap();
    table.commit().unwrap();
    let snapshot = table.snapshot();
    let sparse = |term| Retriever::Sparse {
        column_id: 3,
        query: vec![(term, 1.0)],
        k: 1,
    };

    let ceiling =
        AiExecutionContext::with_limits(std::time::Duration::from_secs(30), usize::MAX, 1);
    assert!(matches!(
        table.search_at_with_candidate_authorization_and_context(
            &search(
                vec![
                    named("first".into(), sparse(1)),
                    named("second".into(), sparse(2)),
                ],
                1,
            ),
            snapshot,
            None,
            Some(&ceiling),
        ),
        Err(MongrelError::WorkBudgetExceeded)
    ));

    let narrow_context = AiExecutionContext::with_limits(
        std::time::Duration::from_secs(30),
        usize::MAX,
        MAX_FUSED_CANDIDATES,
    );
    table
        .search_at_with_candidate_authorization_and_context(
            &search(vec![named("sparse".into(), sparse(1))], 1),
            snapshot,
            None,
            Some(&narrow_context),
        )
        .unwrap();
    let mut full_projection = search(vec![named("sparse".into(), sparse(1))], 1);
    full_projection.projection = None;
    let full_context = AiExecutionContext::with_limits(
        std::time::Duration::from_secs(30),
        usize::MAX,
        MAX_FUSED_CANDIDATES,
    );
    table
        .search_at_with_candidate_authorization_and_context(
            &full_projection,
            snapshot,
            None,
            Some(&full_context),
        )
        .unwrap();
    assert!(full_context.consumed_work() > narrow_context.consumed_work());

    let mut skipped = named("disabled".into(), ann(1));
    skipped.weight = 0.0;
    let zero_context = AiExecutionContext::new(None, 0);
    assert!(table
        .search_at_with_candidate_authorization_and_context(
            &search(vec![skipped], 1),
            snapshot,
            None,
            Some(&zero_context),
        )
        .unwrap()
        .is_empty());
    assert_eq!(zero_context.consumed_work(), 0);
}
