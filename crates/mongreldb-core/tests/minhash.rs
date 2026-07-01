//! MinHash/LSH set-similarity: a column holds a set as a JSON array; the index
//! builds a signature per row and `Condition::MinHashSimilar` returns the rows
//! sharing an LSH band bucket with the query (a sub-linear candidate set),
//! ranked by estimated Jaccard.

use mongreldb_core::index::minhash_token_hash;
use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Table, Value};
use tempfile::tempdir;

fn set_value(tokens: &[&str]) -> Value {
    // Same representation the Kit stores for a `json`/`text` set column.
    Value::Bytes(serde_json::to_vec(tokens).unwrap())
}

fn query_hashes(tokens: &[&str]) -> Vec<u64> {
    tokens.iter().map(|t| minhash_token_hash(t)).collect()
}

fn schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 2,
                name: "tags".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![IndexDef {
            name: "tags_minhash".into(),
            column_id: 2,
            kind: IndexKind::MinHash,
        }],
        colocation: vec![],
        constraints: Default::default(),
    }
}

const IDENTICAL: &[&str] = &["a", "b", "c", "d", "e", "f", "g", "h"];
const NEAR: &[&str] = &["a", "b", "c", "d", "e", "f", "g", "x"]; // Jaccard 7/9
const DISJOINT: &[&str] = &["p", "q", "r", "s", "t", "u", "v", "w"];

fn ids_of(rows: &[mongreldb_core::memtable::Row]) -> Vec<i64> {
    rows.iter()
        .filter_map(|r| match r.columns.get(&1) {
            Some(Value::Int64(v)) => Some(*v),
            _ => None,
        })
        .collect()
}

#[test]
fn minhash_returns_similar_candidates_and_excludes_disjoint() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.bulk_load(vec![
        vec![(1, Value::Int64(0)), (2, set_value(IDENTICAL))],
        vec![(1, Value::Int64(1)), (2, set_value(NEAR))],
        vec![(1, Value::Int64(2)), (2, set_value(DISJOINT))],
    ])
    .unwrap();

    let q = Query::new().and(Condition::MinHashSimilar {
        column_id: 2,
        query: query_hashes(IDENTICAL),
        k: 10,
    });
    let ids = ids_of(&db.query(&q).unwrap());
    assert!(
        ids.contains(&0),
        "identical set is always a candidate: {ids:?}"
    );
    assert!(ids.contains(&1), "high-Jaccard set is a candidate: {ids:?}");
    assert!(
        !ids.contains(&2),
        "disjoint set is not a candidate: {ids:?}"
    );
}

#[test]
fn minhash_intersects_with_another_condition() {
    let dir = tempdir().unwrap();
    let sc = Schema {
        schema_id: 2,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 2,
                name: "cat".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 3,
                name: "tags".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![
            IndexDef {
                name: "cat_bm".into(),
                column_id: 2,
                kind: IndexKind::Bitmap,
            },
            IndexDef {
                name: "tags_minhash".into(),
                column_id: 3,
                kind: IndexKind::MinHash,
            },
        ],
        colocation: vec![],
        constraints: Default::default(),
    };
    let mut db = Table::create(dir.path(), sc, 1).unwrap();
    db.bulk_load(vec![
        vec![
            (1, Value::Int64(0)),
            (2, Value::Bytes(b"a".to_vec())),
            (3, set_value(IDENTICAL)),
        ],
        vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(b"b".to_vec())),
            (3, set_value(IDENTICAL)),
        ],
    ])
    .unwrap();

    // Similar-to-IDENTICAL ∩ cat="a" ⇒ only doc 0.
    let q = Query::new()
        .and(Condition::MinHashSimilar {
            column_id: 3,
            query: query_hashes(IDENTICAL),
            k: 10,
        })
        .and(Condition::BitmapEq {
            column_id: 2,
            value: b"a".to_vec(),
        });
    assert_eq!(ids_of(&db.query(&q).unwrap()), vec![0]);
}

#[test]
fn minhash_survives_reopen() {
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
        db.bulk_load(vec![
            vec![(1, Value::Int64(0)), (2, set_value(IDENTICAL))],
            vec![(1, Value::Int64(2)), (2, set_value(DISJOINT))],
        ])
        .unwrap();
        db.flush().unwrap();
    }
    // Reopen: the index is restored from the checkpoint (or rebuilt from runs).
    let mut db = Table::open(dir.path()).unwrap();
    let q = Query::new().and(Condition::MinHashSimilar {
        column_id: 2,
        query: query_hashes(IDENTICAL),
        k: 10,
    });
    let ids = ids_of(&db.query(&q).unwrap());
    assert!(
        ids.contains(&0),
        "restored index still finds the match: {ids:?}"
    );
    assert!(!ids.contains(&2), "disjoint still excluded: {ids:?}");
}
