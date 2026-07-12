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
                default_value: None,
            },
            ColumnDef {
                id: 2,
                name: "tags".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "tags_minhash".into(),
            column_id: 2,
            kind: IndexKind::MinHash,
            predicate: None,
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
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
                name: "tags".into(),
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
            },
            IndexDef {
                name: "tags_minhash".into(),
                column_id: 3,
                kind: IndexKind::MinHash,
                predicate: None,
            },
        ],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
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

// ponytail: this exists as a one-shot timing probe — demo 2 of the AI
// demo suite. HTTP path has no MinHash DDL (Kit API hardcodes
// `indexes: vec![]`, SQL parser has no "minhash" keyword), so we
// exercise the index in-process. Drop if it ever adds test value.
#[test]
fn minhash_scale_timing() {
    use std::time::Instant;
    let dir = tempdir().unwrap();
    let n: usize = 50_000;
    let sc = Schema {
        schema_id: 99,
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
                name: "tokens".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "tok_minhash".into(),
            column_id: 2,
            kind: IndexKind::MinHash,
            predicate: None,
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    };
    let mut db = Table::create(dir.path(), sc, 99).unwrap();
    let t0 = Instant::now();
    let mut rows: Vec<Vec<(u16, Value)>> = Vec::with_capacity(n);
    // Build a corpus with ~10% near-dup density.
    let mut rng_state: u64 = 0x9E3779B97F4A7C15;
    let mut next_u64 = || {
        rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        rng_state
    };
    let vocab: u64 = 8192;
    let tokens_per_doc: usize = 14;
    for i in 0..n {
        let mut toks: Vec<u64> = Vec::with_capacity(tokens_per_doc);
        for _ in 0..tokens_per_doc {
            toks.push(next_u64() % vocab);
        }
        toks.sort();
        toks.dedup();
        let bytes = serde_json::to_vec(&toks).unwrap();
        rows.push(vec![(1, Value::Int64(i as i64)), (2, Value::Bytes(bytes))]);
    }
    db.bulk_load(rows).unwrap();
    let ingest_ms = t0.elapsed().as_millis();
    eprintln!("\n[demo2] ingested {n} rows in {ingest_ms}ms ({} rows/sec)", (n as u128 * 1000 / (ingest_ms.max(1) as u128)));

    // Pick 50 query docs at random, time MinHashSimilar queries.
    let q_n: usize = 50;
    let k: usize = 50;
    let mut sample_ids: Vec<i64> = (0..n as i64).step_by(n / q_n).collect();
    sample_ids.truncate(q_n);
    let mut times_us: Vec<u128> = Vec::with_capacity(q_n);
    let mut total_pairs: usize = 0;
    for &id in &sample_ids {
        let toks = match db.query(&Query::new().and(Condition::Pk(id.to_be_bytes().to_vec()))).unwrap()
            .into_iter()
            .find_map(|r| match r.columns.get(&2) {
                Some(Value::Bytes(b)) => Some(b.clone()),
                _ => None,
            }) {
            Some(b) => b,
            None => continue,
        };
        let toks_arr: Vec<u64> = serde_json::from_slice(&toks).unwrap();
        let hashes: Vec<u64> = toks_arr
            .iter()
            .map(|t| minhash_token_hash(&t.to_string()))
            .collect();
        let t = Instant::now();
        let res = db.query(&Query::new().and(Condition::MinHashSimilar {
            column_id: 2,
            query: hashes,
            k,
        })).unwrap();
        times_us.push(t.elapsed().as_micros());
        total_pairs += res.len();
    }
    times_us.sort();
    let p = |p: f64| -> u128 {
        let idx = ((times_us.len() - 1) as f64 * p) as usize;
        times_us[idx]
    };
    eprintln!("[demo2] {q_n} queries over {n}-row MinHash index (k={k})");
    eprintln!("[demo2]   min={}µs  p50={}µs  p95={}µs  max={}µs",
              times_us[0], p(0.50), p(0.95), times_us.last().copied().unwrap_or(0));
    eprintln!("[demo2] avg candidates/query = {}/{} = {}",
              total_pairs, q_n, total_pairs / q_n.max(1));
}
