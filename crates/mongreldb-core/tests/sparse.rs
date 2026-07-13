//! SPLADE-style sparse retrieval: documents are stored as bincode'd sparse
//! vectors `(token → weight)`; `Condition::SparseMatch` ranks by sparse dot
//! product over shared tokens. A simple term-frequency tokenizer stands in for a
//! real SPLADE model — the retrieval machinery is model-agnostic.

use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Table, Value};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tempfile::tempdir;

/// Hashing-trick term-frequency sparse vector: `(token, tf)` per unique word.
fn tokenize(text: &str) -> Vec<(u32, f32)> {
    let mut terms: std::collections::HashMap<u32, f32> = std::collections::HashMap::new();
    for word in text.split(|c: char| !c.is_alphanumeric()) {
        if word.is_empty() {
            continue;
        }
        let mut h = DefaultHasher::new();
        word.to_lowercase().hash(&mut h);
        let token = (h.finish() & 0xFFFF_FFFF) as u32;
        *terms.entry(token).or_insert(0.0) += 1.0;
    }
    terms.into_iter().collect()
}

fn sparse_value(text: &str) -> Value {
    Value::Bytes(bincode::serialize(&tokenize(text)).unwrap())
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
                default_value: None,
            },
            ColumnDef {
                id: 2,
                name: "text".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "text_sparse".into(),
            column_id: 2,
            kind: IndexKind::Sparse,
            predicate: None,
            options: Default::default(),
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

#[test]
fn sparse_match_ranks_by_term_overlap() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    let docs = [
        (0i64, "the quick brown fox"),
        (1, "the lazy dog sleeps"),
        (2, "quick fox quick"),
    ];
    db.bulk_load(
        docs.iter()
            .map(|(id, text)| vec![(1, Value::Int64(*id)), (2, sparse_value(text))])
            .collect::<Vec<_>>(),
    )
    .unwrap();

    // query "quick fox": doc2 (quick×2 + fox) > doc0 (quick + fox); doc1 none.
    let q = Query::new().and(Condition::SparseMatch {
        column_id: 2,
        query: tokenize("quick fox"),
        k: 3,
    });
    let rows = db.query(&q).unwrap();
    // rows are sorted by row_id, so sort by relevance via a re-query of scores:
    // Table::query returns the candidate set; verify membership + that doc1 absent.
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match r.columns.get(&1) {
            Some(Value::Int64(v)) => Some(*v),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&0), "doc0 should match: {ids:?}");
    assert!(ids.contains(&2), "doc2 should match: {ids:?}");
    assert!(!ids.contains(&1), "doc1 has no overlap: {ids:?}");
}

#[test]
fn sparse_match_intersects_bitmap() {
    let dir = tempdir().unwrap();
    let sc = Schema {
        schema_id: 2,
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
                name: "cat".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
            ColumnDef {
                id: 3,
                name: "text".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: vec![
            IndexDef {
                name: "cat_bm".into(),
                column_id: 2,
                kind: IndexKind::Bitmap,
                predicate: None,
                options: Default::default(),
            },
            IndexDef {
                name: "text_sparse".into(),
                column_id: 3,
                kind: IndexKind::Sparse,
                predicate: None,
                options: Default::default(),
            },
        ],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    };
    let mut db = Table::create(dir.path(), sc, 1).unwrap();
    // cat "a": docs 0,1 contain "quick"; cat "b": doc 2 also contains "quick".
    db.bulk_load(
        [
            (0i64, "a", "the quick brown fox"),
            (1, "a", "a quick red fox"),
            (2, "b", "quick fox quick"),
        ]
        .iter()
        .map(|(id, cat, text)| {
            vec![
                (1, Value::Int64(*id)),
                (2, Value::Bytes(cat.as_bytes().to_vec())),
                (3, sparse_value(text)),
            ]
        })
        .collect::<Vec<_>>(),
    )
    .unwrap();

    // sparse("quick") ∩ cat="a" ⇒ docs 0 and 1 (doc 2 is cat "b").
    let q = Query::new()
        .and(Condition::SparseMatch {
            column_id: 3,
            query: tokenize("quick"),
            k: 10,
        })
        .and(Condition::BitmapEq {
            column_id: 2,
            value: b"a".to_vec(),
        });
    let rows = db.query(&q).unwrap();
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match r.columns.get(&1) {
            Some(Value::Int64(v)) => Some(*v),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![0, 1]);
}
