use mongreldb_core::query::{
    AnnCandidateDistance, AnnRerankRequest, Condition, Fusion, NamedRetriever, Query, Retriever,
    RetrieverScore, SearchRequest, SetMember, SetSimilarityRequest, VectorMetric,
};
use mongreldb_core::schema::{
    AnnAlgorithm, AnnOptions, AnnQuantization, ColumnDef, ColumnFlags, IndexDef, IndexKind,
    IndexOptions, Schema, TypeId,
};
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
        embedding_source: None,
    };
    Schema {
        schema_id: 1,
        columns: vec![
            column(1, "id", TypeId::Int64, true),
            column(2, "embedding", TypeId::Embedding { dim: 8 }, false),
            column(3, "sparse", TypeId::Bytes, false),
            column(4, "members", TypeId::Bytes, false),
            ColumnDef {
                id: 5,
                name: "created_at".into(),
                ty: TypeId::TimestampNanos,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
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
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn members(values: &[&str]) -> Value {
    Value::Bytes(serde_json::to_vec(values).unwrap())
}

fn seed(table: &mut Table) {
    for row in [
        vec![
            (1, Value::Int64(1)),
            (2, Value::Embedding(vec![1.0; 8])),
            (
                3,
                Value::Bytes(bincode::serialize(&vec![(1, 2.0f32)]).unwrap()),
            ),
            (4, members(&["a", "b", "c", "d"])),
        ],
        vec![
            (1, Value::Int64(2)),
            (2, Value::Embedding(vec![-1.0; 8])),
            (
                3,
                Value::Bytes(bincode::serialize(&vec![(2, 1.0f32)]).unwrap()),
            ),
            (4, members(&["a", "b", "c", "x"])),
        ],
    ] {
        table.put(row).unwrap();
    }
    table.commit().unwrap();
}

#[test]
fn scored_retrievers_preserve_order_and_reopen() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    seed(&mut table);
    table.flush().unwrap();

    let ann = Retriever::Ann {
        column_id: 2,
        query: vec![1.0; 8],
        k: 2,
    };
    let sparse = Retriever::Sparse {
        column_id: 3,
        query: vec![(1, 1.0)],
        k: 2,
    };
    let minhash = Retriever::MinHash {
        column_id: 4,
        members: ["a", "b", "c", "d"]
            .into_iter()
            .map(|value| SetMember::String(value.into()))
            .collect(),
        k: 2,
    };

    let ann_hits = table.retrieve(&ann).unwrap();
    assert!(matches!(
        ann_hits[0].score,
        RetrieverScore::AnnHammingDistance(0)
    ));
    assert!(ann_hits
        .windows(2)
        .all(|hits| match (hits[0].score, hits[1].score) {
            (RetrieverScore::AnnHammingDistance(a), RetrieverScore::AnnHammingDistance(b)) =>
                a <= b,
            _ => false,
        }));
    let sparse_hits = table.retrieve(&sparse).unwrap();
    assert!(sparse_hits
        .windows(2)
        .all(|hits| match (hits[0].score, hits[1].score) {
            (RetrieverScore::SparseDotProduct(a), RetrieverScore::SparseDotProduct(b)) => a >= b,
            _ => false,
        }));
    let minhash_hits = table.retrieve(&minhash).unwrap();
    assert!(minhash_hits
        .windows(2)
        .all(|hits| match (hits[0].score, hits[1].score) {
            (
                RetrieverScore::MinHashEstimatedJaccard(a),
                RetrieverScore::MinHashEstimatedJaccard(b),
            ) => a >= b,
            _ => false,
        }));

    for (retriever, condition) in [
        (
            ann.clone(),
            Condition::Ann {
                column_id: 2,
                query: vec![1.0; 8],
                k: 2,
            },
        ),
        (
            sparse.clone(),
            Condition::SparseMatch {
                column_id: 3,
                query: vec![(1, 1.0)],
                k: 2,
            },
        ),
        (
            minhash.clone(),
            Condition::MinHashSimilar {
                column_id: 4,
                query: ["a", "b", "c", "d"]
                    .into_iter()
                    .map(mongreldb_core::index::minhash_token_hash)
                    .collect(),
                k: 2,
            },
        ),
    ] {
        let retrieved: std::collections::HashSet<_> = table
            .retrieve(&retriever)
            .unwrap()
            .into_iter()
            .map(|hit| hit.row_id)
            .collect();
        let queried: std::collections::HashSet<_> = table
            .query(&Query::new().and(condition))
            .unwrap()
            .into_iter()
            .map(|row| row.row_id)
            .collect();
        assert_eq!(retrieved, queried);
    }

    table.close().unwrap();
    drop(table);
    let mut reopened = Table::open(dir.path()).unwrap();
    assert_eq!(reopened.retrieve(&ann).unwrap(), ann_hits);
    assert_eq!(reopened.retrieve(&sparse).unwrap(), sparse_hits);
    assert_eq!(reopened.retrieve(&minhash).unwrap(), minhash_hits);
}

#[test]
fn scored_retrievers_validate_input() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    assert!(table
        .retrieve(&Retriever::Ann {
            column_id: 2,
            query: vec![1.0],
            k: 1,
        })
        .unwrap_err()
        .to_string()
        .contains("dimension"));
    assert!(table
        .retrieve(&Retriever::Sparse {
            column_id: 3,
            query: vec![],
            k: 1,
        })
        .is_err());
}

#[test]
fn stale_index_entries_never_consume_top_k() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    seed(&mut table);
    table.flush().unwrap();
    let nearest = table
        .retrieve(&Retriever::Ann {
            column_id: 2,
            query: vec![1.0; 8],
            k: 1,
        })
        .unwrap()[0]
        .row_id;
    table.delete(nearest).unwrap();
    table.commit().unwrap();
    table.flush().unwrap();
    table.compact().unwrap();

    let ann = table
        .retrieve(&Retriever::Ann {
            column_id: 2,
            query: vec![1.0; 8],
            k: 1,
        })
        .unwrap();
    assert_eq!(ann.len(), 1);
    assert_ne!(ann[0].row_id, nearest);
    assert_eq!(ann[0].rank, 1);
    assert!(table
        .retrieve(&Retriever::Sparse {
            column_id: 3,
            query: vec![(1, 1.0)],
            k: 2,
        })
        .unwrap()
        .iter()
        .all(|hit| hit.row_id != nearest));
    assert!(table
        .retrieve(&Retriever::MinHash {
            column_id: 4,
            members: ["a", "b", "c", "d"]
                .into_iter()
                .map(|value| SetMember::String(value.into()))
                .collect(),
            k: 2,
        })
        .unwrap()
        .iter()
        .all(|hit| hit.row_id != nearest));
    table.close().unwrap();
    drop(table);
    let mut reopened = Table::open(dir.path()).unwrap();
    let hits = reopened
        .retrieve(&Retriever::Ann {
            column_id: 2,
            query: vec![1.0; 8],
            k: 2,
        })
        .unwrap();
    assert!(hits.iter().all(|hit| hit.row_id != nearest));
}

#[test]
fn ttl_expired_candidates_never_consume_top_k() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    table.set_ttl("created_at", 1).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    for (id, embedding, created_at) in [
        (1, vec![1.0; 8], now - 1_000_000),
        (2, vec![-1.0; 8], now + 60_000_000_000),
    ] {
        table
            .put(vec![
                (1, Value::Int64(id)),
                (2, Value::Embedding(embedding)),
                (
                    3,
                    Value::Bytes(mongreldb_core::query::encode_sparse_vector(&[(1, 1.0)]).unwrap()),
                ),
                (4, members(&["a", "b"])),
                (5, Value::Int64(created_at)),
            ])
            .unwrap();
    }
    table.commit().unwrap();
    let hits = table
        .retrieve(&Retriever::Ann {
            column_id: 2,
            query: vec![1.0; 8],
            k: 1,
        })
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].rank, 1);
    assert_eq!(
        table.get(hits[0].row_id, table.snapshot()).unwrap().columns[&1],
        Value::Int64(2)
    );
}

#[test]
fn exact_set_similarity_filters_sorts_and_limits() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    seed(&mut table);
    let request = SetSimilarityRequest {
        column_id: 4,
        members: ["a", "b", "c", "d"]
            .into_iter()
            .map(|value| SetMember::String(value.into()))
            .collect(),
        candidate_k: 10,
        min_jaccard: 0.0,
        limit: 10,
    };
    let hits = table.set_similarity(&request).unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].exact_jaccard, 1.0);
    assert_eq!(hits[1].exact_jaccard, 0.6);
    assert_ne!(hits[1].estimated_jaccard, hits[1].exact_jaccard);

    let hits = table
        .set_similarity(&SetSimilarityRequest {
            min_jaccard: 0.7,
            limit: 1,
            ..request.clone()
        })
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].exact_jaccard, 1.0);

    assert!(table
        .set_similarity(&SetSimilarityRequest {
            members: vec![],
            ..request.clone()
        })
        .unwrap()
        .is_empty());
}

#[test]
fn ann_candidates_can_be_exactly_reranked() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    table
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Embedding(vec![1.0; 8])),
            (
                3,
                Value::Bytes(bincode::serialize(&vec![(9u32, 0.0f32)]).unwrap()),
            ),
            (4, members(&[])),
        ])
        .unwrap();
    table
        .put(vec![
            (1, Value::Int64(2)),
            (2, Value::Embedding(vec![2.0; 8])),
            (
                3,
                Value::Bytes(bincode::serialize(&vec![(9u32, 0.0f32)]).unwrap()),
            ),
            (4, members(&[])),
        ])
        .unwrap();
    table.commit().unwrap();
    let hits = table
        .ann_rerank(&AnnRerankRequest {
            column_id: 2,
            query: vec![1.0; 8],
            candidate_k: 2,
            limit: 1,
            metric: VectorMetric::DotProduct,
        })
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].exact_score, 16.0);
    assert!(matches!(
        hits[0].candidate_distance,
        AnnCandidateDistance::Hamming(_)
    ));
}

fn dense_schema() -> Schema {
    let mut schema = schema();
    for index in &mut schema.indexes {
        if index.kind == IndexKind::Ann {
            index.options = IndexOptions {
                ann: Some(AnnOptions {
                    quantization: AnnQuantization::Dense,
                    ..AnnOptions::default()
                }),
                ..IndexOptions::default()
            };
        }
    }
    schema
}

#[test]
fn dense_ann_returns_cosine_scores_and_rerank_candidate() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), dense_schema(), 1).unwrap();
    table
        .put(vec![
            (1, Value::Int64(1)),
            (
                2,
                Value::Embedding(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            ),
            (
                3,
                Value::Bytes(bincode::serialize(&vec![(1u32, 1.0f32)]).unwrap()),
            ),
            (4, members(&["a"])),
        ])
        .unwrap();
    table
        .put(vec![
            (1, Value::Int64(2)),
            (
                2,
                Value::Embedding(vec![0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            ),
            (
                3,
                Value::Bytes(bincode::serialize(&vec![(2u32, 1.0f32)]).unwrap()),
            ),
            (4, members(&["b"])),
        ])
        .unwrap();
    table.commit().unwrap();

    let hits = table
        .retrieve(&Retriever::Ann {
            column_id: 2,
            query: vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            k: 2,
        })
        .unwrap();
    assert_eq!(hits.len(), 2);
    assert!(matches!(
        hits[0].score,
        RetrieverScore::AnnCosineDistance(d) if d.abs() < 1e-5
    ));
    assert!(hits
        .windows(2)
        .all(|window| match (window[0].score, window[1].score) {
            (RetrieverScore::AnnCosineDistance(a), RetrieverScore::AnnCosineDistance(b)) => {
                a.total_cmp(&b) != std::cmp::Ordering::Greater
            }
            _ => false,
        }));
    // Dense public scores must never surface as Hamming.
    assert!(!hits
        .iter()
        .any(|hit| matches!(hit.score, RetrieverScore::AnnHammingDistance(_))));

    let reranked = table
        .ann_rerank(&AnnRerankRequest {
            column_id: 2,
            query: vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            candidate_k: 2,
            limit: 1,
            metric: VectorMetric::Cosine,
        })
        .unwrap();
    assert_eq!(reranked.len(), 1);
    assert!(matches!(
        reranked[0].candidate_distance,
        AnnCandidateDistance::Cosine(d) if d.abs() < 1e-5
    ));
    assert!((reranked[0].exact_score - 1.0).abs() < 1e-5);
}

#[test]
fn hybrid_search_filters_unions_and_fuses_deterministically() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    seed(&mut table);
    let ann = NamedRetriever {
        name: "dense".into(),
        weight: 1.0,
        retriever: Retriever::Ann {
            column_id: 2,
            query: vec![1.0; 8],
            k: 1,
        },
    };
    let sparse = NamedRetriever {
        name: "sparse".into(),
        weight: 1.0,
        retriever: Retriever::Sparse {
            column_id: 3,
            query: vec![(2, 1.0)],
            k: 1,
        },
    };
    let request = SearchRequest {
        must: vec![],
        retrievers: vec![ann.clone(), sparse.clone()],
        fusion: Fusion::ReciprocalRank { constant: 60 },
        rerank: None,
        limit: 10,
        projection: Some(vec![1]),
    };
    let (hits, trace) = mongreldb_core::trace::QueryTrace::capture(|| table.search(&request));
    let hits = hits.unwrap();
    assert_eq!(trace.ann_algorithm, Some(AnnAlgorithm::Hnsw));
    assert_eq!(trace.ann_quantization, Some(AnnQuantization::BinarySign));
    assert_eq!(trace.ann_backend, Some("hnsw"));
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].row_id.0, 0);
    assert_eq!(hits[1].row_id.0, 1);
    assert_eq!(hits[0].fused_score, 1.0 / 61.0);
    assert_eq!(hits[0].cells, vec![(1, Value::Int64(1))]);
    assert_eq!(hits[0].components[0].retriever_name.as_ref(), "dense");
    assert_eq!(hits[1].components[0].retriever_name.as_ref(), "sparse");

    let reversed = table
        .search(&SearchRequest {
            retrievers: vec![sparse, ann],
            ..request.clone()
        })
        .unwrap();
    assert_eq!(reversed, hits);

    let filtered = table
        .search(&SearchRequest {
            must: vec![Condition::Pk(Value::Int64(2).encode_key())],
            retrievers: request.retrievers.clone(),
            ..request.clone()
        })
        .unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].cells, vec![(1, Value::Int64(2))]);

    let duplicate = request.retrievers[0].clone();
    assert!(table
        .search(&SearchRequest {
            retrievers: vec![duplicate.clone(), duplicate],
            ..request
        })
        .unwrap_err()
        .to_string()
        .contains("unique"));
}

#[test]
fn search_projects_small_candidate_set_from_single_run() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    for id in 0..64u32 {
        table
            .put(vec![
                (1, Value::Int64(id as i64)),
                (2, Value::Embedding(vec![1.0; 8])),
                (
                    3,
                    Value::Bytes(bincode::serialize(&vec![(id, 1.0f32)]).unwrap()),
                ),
                (4, members(&[])),
            ])
            .unwrap();
    }
    table.commit().unwrap();
    table.close().unwrap();

    let hits = table
        .search(&SearchRequest {
            must: vec![],
            retrievers: vec![NamedRetriever {
                name: "sparse".into(),
                weight: 1.0,
                retriever: Retriever::Sparse {
                    column_id: 3,
                    query: vec![(37, 1.0)],
                    k: 1,
                },
            }],
            fusion: Fusion::ReciprocalRank { constant: 60 },
            rerank: None,
            limit: 1,
            projection: Some(vec![1]),
        })
        .unwrap();

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].cells, vec![(1, Value::Int64(37))]);
}
