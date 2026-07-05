//! Phase 14.1/14.2 — typed bulk ingest (`bulk_load_columns`) must build the
//! live secondary indexes straight from `NativeColumn`s (no per-row `Value`/
//! `Row`/`HashMap`). Under `IndexBuildPolicy::Eager` the indexes and checkpoint
//! are built inside the load; under the default `Deferred` policy they complete
//! lazily on the first query/flush (Phase 14.7). Exercises PK/HOT, bitmap, FM,
//! and sparse indexes through the typed path, a reopen round-trip, and both
//! policies.

use mongreldb_core::columnar::NativeColumn;
use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{IndexBuildPolicy, Table, Value};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tempfile::tempdir;

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

/// Build an Arrow-style `Bytes` NativeColumn from per-row byte slices.
fn bytes_column(rows: &[Vec<u8>]) -> NativeColumn {
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    let mut values = Vec::new();
    offsets.push(0);
    for r in rows {
        values.extend_from_slice(r);
        offsets.push(values.len() as u32);
    }
    let n = rows.len();
    NativeColumn::Bytes {
        offsets,
        values,
        validity: vec![0xFF; n.div_ceil(8)],
    }
}

fn schema() -> Schema {
    Schema {
        schema_id: 14,
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
                name: "text".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 4,
                name: "sparse".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
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
                name: "text_fm".into(),
                column_id: 3,
                kind: IndexKind::FmIndex,
                predicate: None,
            },
            IndexDef {
                name: "sparse_idx".into(),
                column_id: 4,
                kind: IndexKind::Sparse,
                predicate: None,
            },
        ],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

/// `bulk_load_columns` builds every secondary index from typed columns and
/// serves PK/bitmap/FM/sparse queries in the live session (no reopen).
#[test]
fn typed_bulk_load_builds_all_indexes() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.set_index_build_policy(IndexBuildPolicy::Eager);

    let docs = [
        (0i64, "red", "the quick brown fox", "quick fox"),
        (1, "blue", "the lazy dog", "lazy dog"),
        (2, "red", "quick fox quick", "quick fox quick"),
    ];

    let id_col = NativeColumn::Int64 {
        data: docs.iter().map(|(id, _, _, _)| *id).collect(),
        validity: vec![0xFF; 1],
    };
    let cat_col = bytes_column(
        &docs
            .iter()
            .map(|(_, c, _, _)| c.as_bytes().to_vec())
            .collect::<Vec<_>>(),
    );
    let text_col = bytes_column(
        &docs
            .iter()
            .map(|(_, _, t, _)| t.as_bytes().to_vec())
            .collect::<Vec<_>>(),
    );
    let sparse_col = bytes_column(
        &docs
            .iter()
            .map(|(_, _, _, s)| bincode::serialize(&tokenize(s)).unwrap())
            .collect::<Vec<_>>(),
    );

    db.bulk_load_columns(vec![
        (1, id_col),
        (2, cat_col),
        (3, text_col),
        (4, sparse_col),
    ])
    .unwrap();
    assert_eq!(db.count(), 3);
    assert!(
        db.indexes_complete(),
        "fresh typed bulk load should eagerly build live indexes"
    );
    assert!(
        dir.path().join("_idx/global.idx").exists(),
        "fresh typed bulk load must write the index checkpoint"
    );

    // PK (HOT) built from the typed PK column.
    let q = Query::pk(0i64.to_be_bytes().to_vec());
    assert_eq!(db.query(&q).unwrap().len(), 1);

    // Bitmap: two rows are "red".
    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"red".to_vec(),
    });
    assert_eq!(db.query(&q).unwrap().len(), 2);

    // FM substring: "fox" appears in docs 0 and 2.
    let q = Query::new().and(Condition::FmContains {
        column_id: 3,
        pattern: b"fox".to_vec(),
    });
    let rows = db.query(&q).unwrap();
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match r.columns.get(&1) {
            Some(Value::Int64(v)) => Some(*v),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&0) && ids.contains(&2) && !ids.contains(&1));

    // Sparse: "quick fox" matches docs 0 and 2, not doc 1.
    let q = Query::new().and(Condition::SparseMatch {
        column_id: 4,
        query: tokenize("quick fox"),
        k: 3,
    });
    let rows = db.query(&q).unwrap();
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match r.columns.get(&1) {
            Some(Value::Int64(v)) => Some(*v),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&0) && ids.contains(&2) && !ids.contains(&1));
}

/// Default policy (`Deferred`): a fresh typed bulk load keeps the ingest
/// critical path free of index building — indexes are incomplete right after
/// the load, then complete lazily on the first query, which still answers
/// correctly through every index kind.
#[test]
fn typed_bulk_load_defers_indexes_by_default() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    assert_eq!(db.index_build_policy(), IndexBuildPolicy::Deferred);

    let id_col = NativeColumn::int64_sequence(0, 3);
    let cat_col = bytes_column(&[b"red".to_vec(), b"blue".to_vec(), b"red".to_vec()]);
    let text_col = bytes_column(&[
        b"the quick brown fox".to_vec(),
        b"the lazy dog".to_vec(),
        b"quick fox".to_vec(),
    ]);
    let sparse_col = bytes_column(
        &["quick fox", "lazy dog", "quick fox"]
            .iter()
            .map(|s| bincode::serialize(&tokenize(s)).unwrap())
            .collect::<Vec<_>>(),
    );
    db.bulk_load_columns(vec![
        (1, id_col),
        (2, cat_col),
        (3, text_col),
        (4, sparse_col),
    ])
    .unwrap();

    assert!(
        !db.indexes_complete(),
        "default bulk load must defer index building off the ingest path"
    );
    assert!(
        !dir.path().join("_idx/global.idx").exists(),
        "no index checkpoint may be written while indexes are incomplete"
    );

    // First query triggers the lazy rebuild (Phase 14.7) and serves correctly.
    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"red".to_vec(),
    });
    assert_eq!(db.query(&q).unwrap().len(), 2);
    assert!(
        db.indexes_complete(),
        "first query must complete the deferred indexes"
    );
    assert_eq!(
        db.query(&Query::pk(0i64.to_be_bytes().to_vec()))
            .unwrap()
            .len(),
        1
    );
}

/// The checkpoint written by an `Eager` `bulk_load_columns` reloads on reopen,
/// so all indexes survive without a run scan.
#[test]
fn typed_bulk_load_indexes_survive_reopen() {
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
        db.set_index_build_policy(IndexBuildPolicy::Eager);
        let id_col = NativeColumn::int64_sequence(0, 3);
        let cat_col = bytes_column(&[b"red".to_vec(), b"blue".to_vec(), b"red".to_vec()]);
        let text_col = bytes_column(&[
            b"the quick brown fox".to_vec(),
            b"the lazy dog".to_vec(),
            b"quick fox".to_vec(),
        ]);
        let sparse_col = bytes_column(
            &["quick fox", "lazy dog", "quick fox"]
                .iter()
                .map(|s| bincode::serialize(&tokenize(s)).unwrap())
                .collect::<Vec<_>>(),
        );
        db.bulk_load_columns(vec![
            (1, id_col),
            (2, cat_col),
            (3, text_col),
            (4, sparse_col),
        ])
        .unwrap();
    }
    let mut db = Table::open(dir.path()).unwrap();
    assert_eq!(db.count(), 3);
    // PK served from the reloaded HOT checkpoint.
    assert_eq!(
        db.query(&Query::pk(1i64.to_be_bytes().to_vec()))
            .unwrap()
            .len(),
        1
    );
    // Bitmap served from the reloaded bitmap checkpoint.
    assert_eq!(
        db.query(&Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"red".to_vec(),
        }))
        .unwrap()
        .len(),
        2
    );
}

/// Phase 14.3: parallel column/page encoding must produce a byte-identical,
/// correctly-ordered run. Load a wide multi-page table (4 columns, >1 page each)
/// through the parallel encode path and verify a full native scan round-trips
/// every value and every column keeps its schema order.
#[test]
fn parallel_column_encoding_round_trips() {
    let dir = tempdir().unwrap();
    let sc = Schema {
        schema_id: 15,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 2,
                name: "a".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 3,
                name: "b".into(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 4,
                name: "c".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: Vec::new(),
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    };
    let mut db = Table::create(dir.path(), sc, 1).unwrap();
    // >1 page per column (PAGE_ROWS = 65 536) so native_column_pages takes the
    // rayon branch, and 4 user columns so write_native takes it too.
    let n = 65_536 * 2 + 7;
    let id_col = NativeColumn::int64_sequence(0, n);
    let a_col = NativeColumn::Int64 {
        data: (0..n).map(|i| i as i64 * 3).collect(),
        validity: vec![0xFF; n.div_ceil(8)],
    };
    let b_col = NativeColumn::Float64 {
        data: (0..n).map(|i| i as f64 * 0.5).collect(),
        validity: vec![0xFF; n.div_ceil(8)],
    };
    let c_col = bytes_column(
        &(0..n)
            .map(|i| format!("row{i}").into_bytes())
            .collect::<Vec<_>>(),
    );
    db.bulk_load_columns(vec![(1, id_col), (2, a_col), (3, b_col), (4, c_col)])
        .unwrap();

    let snap = db.snapshot();
    let cols = db.visible_columns_native(snap, None).unwrap();
    // Schema column order is preserved by the parallel encode.
    let order: Vec<u16> = cols.iter().map(|(c, _)| *c).collect();
    assert_eq!(order, vec![1, 2, 3, 4], "column order must match schema");

    let int_data = |cid: u16| -> Vec<i64> {
        match &cols.iter().find(|(c, _)| *c == cid).unwrap().1 {
            NativeColumn::Int64 { data, .. } => data.clone(),
            _ => panic!("col {cid} not Int64"),
        }
    };
    let id = int_data(1);
    let a = int_data(2);
    for i in 0..n {
        assert_eq!(id[i], i as i64, "id row {i}");
        assert_eq!(a[i], i as i64 * 3, "a row {i}");
    }
    match &cols.iter().find(|(c, _)| *c == 3).unwrap().1 {
        NativeColumn::Float64 { data, .. } => {
            for (i, &v) in data.iter().enumerate().take(n) {
                assert_eq!(v, i as f64 * 0.5);
            }
        }
        _ => panic!("col 3 not Float64"),
    }
    match &cols.iter().find(|(c, _)| *c == 4).unwrap().1 {
        NativeColumn::Bytes {
            offsets, values, ..
        } => {
            assert_eq!(offsets.len(), n + 1);
            for i in 0..n {
                let lo = offsets[i] as usize;
                let hi = offsets[i + 1] as usize;
                assert_eq!(&values[lo..hi], format!("row{i}").as_bytes());
            }
        }
        _ => panic!("col 4 not Bytes"),
    }
}

/// Phase 14.4: `bulk_load_fast` writes raw `ALGO_PLAIN` (no-zstd) pages that
/// still round-trip exactly through the reader, stay queryable, and reopen
/// cleanly. The plain run is larger than the zstd run but encodes faster.
#[test]
fn bulk_load_fast_round_trips_plain_pages() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    let docs = [
        (0i64, "red", "the quick brown fox", "quick fox"),
        (1, "blue", "the lazy dog", "lazy dog"),
        (2, "red", "quick fox quick", "quick fox quick"),
    ];
    let id_col = NativeColumn::Int64 {
        data: docs.iter().map(|(id, _, _, _)| *id).collect(),
        validity: vec![0xFF; 1],
    };
    let cat_col = bytes_column(
        &docs
            .iter()
            .map(|(_, c, _, _)| c.as_bytes().to_vec())
            .collect::<Vec<_>>(),
    );
    let text_col = bytes_column(
        &docs
            .iter()
            .map(|(_, _, t, _)| t.as_bytes().to_vec())
            .collect::<Vec<_>>(),
    );
    let sparse_col = bytes_column(
        &docs
            .iter()
            .map(|(_, _, _, s)| bincode::serialize(&tokenize(s)).unwrap())
            .collect::<Vec<_>>(),
    );
    db.bulk_load_fast(vec![
        (1, id_col),
        (2, cat_col),
        (3, text_col),
        (4, sparse_col),
    ])
    .unwrap();
    assert_eq!(db.count(), 3);

    // Plain pages decode back to the right values.
    let snap = db.snapshot();
    let cols = db.visible_columns_native(snap, None).unwrap();
    match &cols.iter().find(|(c, _)| *c == 1).unwrap().1 {
        NativeColumn::Int64 { data, .. } => assert_eq!(data, &vec![0i64, 1, 2]),
        _ => panic!("col 1 not Int64"),
    }

    // Indexes were bulk-built on the plain path too.
    assert_eq!(
        db.query(&Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"red".to_vec(),
        }))
        .unwrap()
        .len(),
        2
    );

    // Reopen reads the plain run + checkpoint.
    drop(db);
    let mut db = Table::open(dir.path()).unwrap();
    assert_eq!(db.count(), 3);
    assert_eq!(
        db.query(&Query::pk(2i64.to_be_bytes().to_vec()))
            .unwrap()
            .len(),
        1
    );
}

/// Phase 15.4: a repeat scan of the same run is served from the decoded-page
/// cache (the post-decompress typed page), so the second scan skips decode.
#[test]
fn decoded_page_cache_populates_on_scan() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    let id_col = NativeColumn::int64_sequence(0, 3);
    let cat_col = bytes_column(&[b"red".to_vec(), b"blue".to_vec(), b"red".to_vec()]);
    let text_col = bytes_column(&[
        b"the quick brown fox".to_vec(),
        b"the lazy dog".to_vec(),
        b"quick fox".to_vec(),
    ]);
    let sparse_col = bytes_column(
        &["quick fox", "lazy dog", "quick fox"]
            .iter()
            .map(|s| bincode::serialize(&tokenize(s)).unwrap())
            .collect::<Vec<_>>(),
    );
    db.bulk_load_columns(vec![
        (1, id_col),
        (2, cat_col),
        (3, text_col),
        (4, sparse_col),
    ])
    .unwrap();
    assert_eq!(db.decoded_cache_len(), 0, "cache empty before any scan");

    // First scan decodes (and caches) every page of every user column.
    let snap = db.snapshot();
    let _ = db.visible_columns_native(snap, None).unwrap();
    assert!(
        db.decoded_cache_len() > 0,
        "decoded cache must populate on first scan"
    );
    let populated = db.decoded_cache_len();

    // Second scan: same result, cache stays populated (pages are reused, not
    // re-added — the count must not grow).
    let cols = db.visible_columns_native(snap, None).unwrap();
    assert_eq!(cols.iter().find(|(c, _)| *c == 1).unwrap().1.len(), 3);
    assert_eq!(
        db.decoded_cache_len(),
        populated,
        "repeat scan reuses entries"
    );
}

/// Phase 14.4: `bulk_load_fast` actually skips compression — its run must be
/// larger than the zstd-compressed `bulk_load_columns` run for compressible data.
#[test]
fn bulk_load_fast_run_is_larger_than_zstd() {
    let sc = Schema {
        schema_id: 16,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
        }],
        indexes: Vec::new(),
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    };
    let mk = |dir| {
        let mut db = Table::create(dir, sc.clone(), 1).unwrap();
        let n = 50_000usize;
        // Highly compressible: a sequential run encodes tiny under delta+zstd.
        let col = NativeColumn::int64_sequence(0, n);
        db.bulk_load_columns(vec![(1, col)]).unwrap();
        db
    };
    let mk_fast = |dir| {
        let mut db = Table::create(dir, sc.clone(), 1).unwrap();
        let n = 50_000usize;
        let col = NativeColumn::int64_sequence(0, n);
        db.bulk_load_fast(vec![(1, col)]).unwrap();
        db
    };

    let d1 = tempdir().unwrap();
    let d2 = tempdir().unwrap();
    let _ = mk(d1.path());
    let _ = mk_fast(d2.path());
    let zstd_size = std::fs::metadata(d1.path().join("_runs").join("r-1.sr"))
        .unwrap()
        .len();
    let plain_size = std::fs::metadata(d2.path().join("_runs").join("r-1.sr"))
        .unwrap()
        .len();
    assert!(
        plain_size > zstd_size,
        "plain run ({plain_size}) must be larger than zstd ({zstd_size}) on compressible data"
    );
}

#[test]
fn double_bulk_load_no_duplicate_indexes() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    let mk = |offset: i64| {
        vec![
            (
                1,
                NativeColumn::Int64 {
                    data: (offset..offset + 3).collect(),
                    validity: vec![0xFF; 1],
                },
            ),
            (
                2,
                bytes_column(&[b"red".to_vec(), b"blue".to_vec(), b"red".to_vec()]),
            ),
            (
                3,
                bytes_column(&[
                    b"the quick brown fox".to_vec(),
                    b"the lazy dog".to_vec(),
                    b"quick fox".to_vec(),
                ]),
            ),
            (
                4,
                bytes_column(
                    &["quick fox", "lazy dog", "quick fox"]
                        .iter()
                        .map(|s| bincode::serialize(&tokenize(s)).unwrap())
                        .collect::<Vec<_>>(),
                ),
            ),
        ]
    };
    db.bulk_load_columns(mk(0)).unwrap();
    let r1 = db
        .query(&Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"red".to_vec(),
        }))
        .unwrap();
    assert_eq!(r1.len(), 2, "first load: 2 red");
    db.bulk_load_columns(mk(3)).unwrap();
    let r2 = db
        .query(&Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"red".to_vec(),
        }))
        .unwrap();
    assert_eq!(
        r2.len(),
        4,
        "no duplicate index entries after double bulk load"
    );
    let r3 = db
        .query(&Query::new().and(Condition::FmContains {
            column_id: 3,
            pattern: b"fox".to_vec(),
        }))
        .unwrap();
    assert_eq!(r3.len(), 4, "FM no duplicates");
}
