use mongreldb_core::query::{
    Condition, Fusion, NamedRetriever, Retriever, RetrieverScore, SearchRequest, SetMember,
    SetSimilarityRequest,
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
            },
            IndexDef {
                name: "sparse".into(),
                column_id: 3,
                kind: IndexKind::Sparse,
                predicate: None,
            },
            IndexDef {
                name: "minhash".into(),
                column_id: 4,
                kind: IndexKind::MinHash,
                predicate: None,
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
            ..request
        })
        .unwrap()
        .is_empty());
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
        limit: 10,
        projection: Some(vec![1]),
    };
    let hits = table.search(&request).unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].row_id.0, 0);
    assert_eq!(hits[1].row_id.0, 1);
    assert_eq!(hits[0].fused_score, 1.0 / 61.0);
    assert_eq!(hits[0].cells, vec![(1, Value::Int64(1))]);
    assert_eq!(hits[0].components[0].retriever_name, "dense");
    assert_eq!(hits[1].components[0].retriever_name, "sparse");

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
            ..request
        })
        .unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].cells, vec![(1, Value::Int64(2))]);
}
