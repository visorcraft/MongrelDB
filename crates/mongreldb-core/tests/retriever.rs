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

/// Churn oracle (senior bar): ANN, Sparse, and MinHash each return **non-empty**
/// top-k that **matches a visible-row brute-force oracle** after multi-cycle
/// delete+insert churn. Empty results or ranking drift fail the test. Stale
/// postings remain append-only; visibility filters must keep recall.
#[test]
fn churn_oracle_topk_matches_visible_brute_force() {
    use mongreldb_core::Query;
    use std::collections::HashSet;

    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();

    // ANN: BinarySign Hamming. Query code 0x55; row i%8==0 is distance 0.
    let query_emb: Vec<f32> = vec![1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0];
    // Sparse: query token 1 — rows with (1, weight) score by weight.
    let query_sparse: Vec<(u32, f32)> = vec![(1, 1.0)];
    // MinHash: query set {a,b,c,d}; rows with that set are exact match (J=1).
    let query_members: Vec<SetMember> = ["a", "b", "c", "d"]
        .into_iter()
        .map(|value| SetMember::String(value.into()))
        .collect();

    let n_live: i64 = 48;
    let k: usize = 3;

    let quantize_sign = |vec: &[f32]| -> u8 {
        let mut bits: u8 = 0;
        for (i, v) in vec.iter().enumerate() {
            if *v > 0.0 {
                bits |= 1 << (i % 8);
            }
        }
        bits
    };

    let jaccard = |a: &HashSet<String>, b: &HashSet<String>| -> f64 {
        if a.is_empty() && b.is_empty() {
            return 1.0;
        }
        let inter = a.intersection(b).count() as f64;
        let union = a.union(b).count() as f64;
        if union == 0.0 {
            0.0
        } else {
            inter / union
        }
    };

    let decode_set = |v: &Value| -> HashSet<String> {
        match v {
            // `members()` stores a JSON string array (see helper above).
            Value::Bytes(b) => serde_json::from_slice::<Vec<String>>(b)
                .unwrap_or_default()
                .into_iter()
                .collect(),
            _ => HashSet::new(),
        }
    };

    let decode_sparse = |v: &Value| -> Vec<(u32, f32)> {
        match v {
            Value::Bytes(b) => bincode::deserialize(b).unwrap_or_default(),
            _ => Vec::new(),
        }
    };

    let sparse_dot = |q: &[(u32, f32)], d: &[(u32, f32)]| -> f32 {
        let mut score = 0.0f32;
        for (qt, qw) in q {
            for (dt, dw) in d {
                if qt == dt {
                    score += qw * dw;
                }
            }
        }
        score
    };

    for i in 0..n_live {
        let embedding: Vec<f32> = (0..8)
            .map(|j| if j == (i % 8) as usize { 1.0 } else { -1.0 })
            .collect();
        // Sparse: half the rows strongly match token 1 with weight decreasing by i.
        let sparse_terms = if i % 2 == 0 {
            vec![(1u32, 10.0 - (i as f32) * 0.01), (2, 0.1)]
        } else {
            vec![(3u32, 1.0), (4, 1.0)]
        };
        // MinHash: every 3rd row is exact query set; others diverge.
        let set_members = if i % 3 == 0 {
            members(&["a", "b", "c", "d"])
        } else if i % 3 == 1 {
            members(&["a", "b", "x", "y"])
        } else {
            members(&["w", "x", "y", "z"])
        };
        table
            .put(vec![
                (1, Value::Int64(10_000 + i)),
                (2, Value::Embedding(embedding)),
                (3, Value::Bytes(bincode::serialize(&sparse_terms).unwrap())),
                (4, set_members),
            ])
            .unwrap();
    }
    table.commit().unwrap();
    table.flush().unwrap();

    let ann_oracle = |table: &mut Table| -> Vec<mongreldb_core::RowId> {
        let qbits = quantize_sign(&query_emb);
        let visible = table.query(&Query::new()).unwrap();
        let mut scored: Vec<(mongreldb_core::RowId, u32)> = visible
            .into_iter()
            .filter_map(|r| match r.columns.get(&2) {
                Some(Value::Embedding(v)) => {
                    let rbits = quantize_sign(v);
                    Some((r.row_id, (qbits ^ rbits).count_ones()))
                }
                _ => None,
            })
            .collect();
        scored.sort_by_key(|&(_, d)| d);
        scored.into_iter().take(k).map(|(id, _)| id).collect()
    };

    let sparse_oracle = |table: &mut Table| -> Vec<mongreldb_core::RowId> {
        let visible = table.query(&Query::new()).unwrap();
        let mut scored: Vec<(mongreldb_core::RowId, f32)> = visible
            .into_iter()
            .filter_map(|r| {
                r.columns
                    .get(&3)
                    .map(|v| (r.row_id, sparse_dot(&query_sparse, &decode_sparse(v))))
            })
            .filter(|(_, s)| *s > 0.0)
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.into_iter().take(k).map(|(id, _)| id).collect()
    };

    let minhash_oracle = |table: &mut Table| -> Vec<mongreldb_core::RowId> {
        let qset: HashSet<String> = ["a", "b", "c", "d"].into_iter().map(String::from).collect();
        let visible = table.query(&Query::new()).unwrap();
        let mut scored: Vec<(mongreldb_core::RowId, f64)> = visible
            .into_iter()
            .filter_map(|r| {
                r.columns
                    .get(&4)
                    .map(|v| (r.row_id, jaccard(&qset, &decode_set(v))))
            })
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.into_iter().take(k).map(|(id, _)| id).collect()
    };

    let cycles = 6;
    for cycle in 0..cycles {
        let ann_o = ann_oracle(&mut table);
        assert_eq!(ann_o.len(), k, "cycle {cycle}: ANN oracle must yield k={k}");
        let sparse_o = sparse_oracle(&mut table);
        assert!(
            !sparse_o.is_empty(),
            "cycle {cycle}: Sparse oracle empty (need positive-score live rows)"
        );
        let minhash_o = minhash_oracle(&mut table);
        assert_eq!(
            minhash_o.len(),
            k,
            "cycle {cycle}: MinHash oracle must yield k={k}"
        );

        let ann_hits = table
            .retrieve(&Retriever::Ann {
                column_id: 2,
                query: query_emb.clone(),
                k,
            })
            .unwrap();
        assert_eq!(
            ann_hits.len(),
            k,
            "cycle {cycle}: ANN returned {} not k={k}",
            ann_hits.len()
        );
        let ann_ids: Vec<_> = ann_hits.iter().map(|h| h.row_id).collect();
        assert_eq!(
            ann_ids, ann_o,
            "cycle {cycle}: ANN top-k {ann_ids:?} != oracle {ann_o:?}"
        );

        let sparse_hits = table
            .retrieve(&Retriever::Sparse {
                column_id: 3,
                query: query_sparse.clone(),
                k,
            })
            .unwrap();
        // Sparse scoring is exact (dot product): require full top-k match.
        assert_eq!(
            sparse_hits.len(),
            sparse_o.len().min(k),
            "cycle {cycle}: Sparse returned {} vs oracle {}",
            sparse_hits.len(),
            sparse_o.len()
        );
        assert!(
            !sparse_hits.is_empty(),
            "cycle {cycle}: Sparse must be non-empty"
        );
        let sparse_ids: Vec<_> = sparse_hits.iter().map(|h| h.row_id).collect();
        assert_eq!(
            sparse_ids,
            sparse_o[..sparse_ids.len()],
            "cycle {cycle}: Sparse full top-k {sparse_ids:?} != oracle {:?}",
            &sparse_o[..sparse_ids.len()]
        );

        let minhash_hits = table
            .retrieve(&Retriever::MinHash {
                column_id: 4,
                members: query_members.clone(),
                k,
            })
            .unwrap();
        assert_eq!(
            minhash_hits.len(),
            k,
            "cycle {cycle}: MinHash returned {} not k={k} (stale budget ate recall)",
            minhash_hits.len()
        );
        // MinHash is approximate LSH: senior bar is full-k of *max-quality*
        // live matches (exact Jaccard of every hit equals the oracle top
        // score), not rid-order identity. With our layout oracle top-k are
        // all J=1 exact-set rows; LSH may pick any of the exact-set rids.
        let qset: HashSet<String> = ["a", "b", "c", "d"].into_iter().map(String::from).collect();
        let oracle_top_j = {
            let visible = table.query(&Query::new()).unwrap();
            let row = visible
                .iter()
                .find(|r| r.row_id == minhash_o[0])
                .expect("oracle top row visible");
            jaccard(&qset, &decode_set(row.columns.get(&4).expect("members")))
        };
        assert!(
            (oracle_top_j - 1.0).abs() < 1e-9,
            "cycle {cycle}: test data expects oracle top Jaccard=1, got {oracle_top_j}"
        );
        for hit in &minhash_hits {
            let row = table
                .get(hit.row_id, mongreldb_core::Snapshot::unbounded())
                .expect("MinHash hit must materialize");
            assert!(
                !row.deleted,
                "cycle {cycle}: MinHash tombstone {}",
                hit.row_id.0
            );
            let j = jaccard(&qset, &decode_set(row.columns.get(&4).expect("members")));
            assert!(
                (j - oracle_top_j).abs() < 1e-9,
                "cycle {cycle}: MinHash hit {} Jaccard {j} < oracle top {oracle_top_j}",
                hit.row_id.0
            );
        }

        // Churn: delete ANN oracle top-1 (and ensure it is removed from sparse/minhash live set).
        table.delete(ann_o[0]).unwrap();
        let new_id = 1_000_000 + cycle as i64;
        table
            .put(vec![
                (1, Value::Int64(new_id)),
                (2, Value::Embedding(vec![-1.0; 8])),
                (
                    3,
                    Value::Bytes(bincode::serialize(&vec![(99_u32, 0.01_f32)]).unwrap()),
                ),
                (4, members(&["z", "y", "x", "w"])),
            ])
            .unwrap();
        table.commit().unwrap();
    }
    table.flush().unwrap();
    table.close().unwrap();
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
