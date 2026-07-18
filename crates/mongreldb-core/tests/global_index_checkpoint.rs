//! Phase 9.1 — `_idx/global.idx`: the persisted index checkpoint lets
//! [`Table::open`] load HOT/bitmap/FM/ANN/sparse/learned-range indexes directly
//! instead of scanning every sorted run. These tests exercise the fast path
//! (checkpoint loaded), the WAL-replay-on-top path, the fallback when no
//! checkpoint exists, and the compaction refresh.

use mongreldb_core::columnar::NativeColumn;
use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Table, Value};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tempfile::tempdir;

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
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "color".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 3,
                name: "text".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 4,
                name: "score".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![
            IndexDef {
                name: "color_bm".into(),
                column_id: 2,
                kind: IndexKind::Bitmap,
                predicate: None,
                options: Default::default(),
            },
            IndexDef {
                name: "text_fm".into(),
                column_id: 3,
                kind: IndexKind::FmIndex,
                predicate: None,
                options: Default::default(),
            },
            IndexDef {
                name: "score_lr".into(),
                column_id: 4,
                kind: IndexKind::LearnedRange,
                predicate: None,
                options: Default::default(),
            },
        ],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn enc_i64(v: i64) -> Vec<u8> {
    Value::Int64(v).encode_key()
}

#[test]
fn checkpoint_loaded_on_reopen_all_indexes() {
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
        db.set_mutable_run_spill_bytes(1); // spill so a checkpoint is written
        db.put(vec![
            (1, Value::Int64(10)),
            (2, Value::Bytes(b"red".to_vec())),
            (3, Value::Bytes(b"the quick brown fox".to_vec())),
            (4, Value::Int64(42)),
        ])
        .unwrap();
        db.put(vec![
            (1, Value::Int64(20)),
            (2, Value::Bytes(b"blue".to_vec())),
            (3, Value::Bytes(b"fox in socks".to_vec())),
            (4, Value::Int64(7)),
        ])
        .unwrap();
        db.flush().unwrap();
        assert!(
            dir.path().join("_idx/global.idx").exists(),
            "global.idx checkpoint should exist after flush"
        );
    }
    let mut db = Table::open(dir.path()).unwrap();

    let q = Query::pk(enc_i64(20));
    assert_eq!(db.query(&q).unwrap().len(), 1, "PK (HOT) lookup");

    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"red".to_vec(),
    });
    let red = db.query(&q).unwrap();
    assert_eq!(red.len(), 1);
    assert_eq!(
        red[0].columns.get(&1).and_then(|v| match v {
            Value::Int64(i) => Some(*i),
            _ => None,
        }),
        Some(10)
    );

    let q = Query::new().and(Condition::FmContains {
        column_id: 3,
        pattern: b"fox".to_vec(),
    });
    assert_eq!(db.query(&q).unwrap().len(), 2, "FM substring");

    let q = Query::new().and(Condition::Range {
        column_id: 4,
        lo: 40,
        hi: 50,
    });
    assert_eq!(db.query(&q).unwrap().len(), 1, "learned-range PGM");
}

#[test]
fn wal_replay_indexes_on_top_of_checkpoint() {
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
        db.put(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(b"red".to_vec())),
            (4, Value::Int64(100)),
        ])
        .unwrap();
        db.flush().unwrap();
        db.put(vec![
            (1, Value::Int64(2)),
            (2, Value::Bytes(b"blue".to_vec())),
            (4, Value::Int64(200)),
        ])
        .unwrap();
        db.commit().unwrap();
    }
    let mut db = Table::open(dir.path()).unwrap();
    let q = Query::pk(enc_i64(2));
    assert_eq!(
        db.query(&q).unwrap().len(),
        1,
        "WAL-replayed PK must be indexed"
    );
    let all = Query::new();
    assert_eq!(
        db.query(&all).unwrap().len(),
        2,
        "both flushed + replayed rows visible"
    );
    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"blue".to_vec(),
    });
    assert_eq!(
        db.query(&q).unwrap().len(),
        1,
        "replayed bitmap entry present"
    );
}

#[test]
fn fallback_rebuilds_when_checkpoint_absent() {
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
        db.set_mutable_run_spill_bytes(1); // spill so a checkpoint is written
        db.put(vec![
            (1, Value::Int64(5)),
            (2, Value::Bytes(b"green".to_vec())),
            (4, Value::Int64(9)),
        ])
        .unwrap();
        db.flush().unwrap();
        std::fs::remove_file(dir.path().join("_idx/global.idx")).unwrap();
    }
    let mut db = Table::open(dir.path()).unwrap();
    let q = Query::pk(enc_i64(5));
    assert_eq!(
        db.query(&q).unwrap().len(),
        1,
        "rebuild-from-runs still indexes the PK"
    );
    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"green".to_vec(),
    });
    assert_eq!(db.query(&q).unwrap().len(), 1);
}

#[test]
fn compaction_refreshes_checkpoint() {
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
        db.set_mutable_run_spill_bytes(1); // spill per flush so compaction + checkpoint fire
        db.put(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(b"red".to_vec())),
            (4, Value::Int64(1)),
        ])
        .unwrap();
        db.flush().unwrap();
        db.put(vec![
            (1, Value::Int64(2)),
            (2, Value::Bytes(b"red".to_vec())),
            (4, Value::Int64(2)),
        ])
        .unwrap();
        db.flush().unwrap();
        db.compact().unwrap();
        assert!(
            dir.path().join("_idx/global.idx").exists(),
            "checkpoint refreshed after compaction"
        );
    }
    let mut db = Table::open(dir.path()).unwrap();
    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"red".to_vec(),
    });
    assert_eq!(
        db.query(&q).unwrap().len(),
        2,
        "post-compaction checkpoint serves both rows"
    );
    let q = Query::pk(enc_i64(1));
    assert_eq!(db.query(&q).unwrap().len(), 1);
}

#[test]
fn ann_and_sparse_roundtrip_through_checkpoint() {
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

    let s = Schema {
        schema_id: 2,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "emb".into(),
                ty: TypeId::Embedding { dim: 8 },
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 3,
                name: "doc".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![
            IndexDef {
                name: "emb_ann".into(),
                column_id: 2,
                kind: IndexKind::Ann,
                predicate: None,
                options: Default::default(),
            },
            IndexDef {
                name: "doc_sparse".into(),
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
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create(dir.path(), s, 2).unwrap();
        db.put(vec![
            (1, Value::Int64(0)),
            (
                2,
                Value::Embedding(vec![1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0]),
            ),
            (3, sparse_value("the quick brown fox")),
        ])
        .unwrap();
        db.put(vec![
            (1, Value::Int64(1)),
            (
                2,
                Value::Embedding(vec![-1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0]),
            ),
            (3, sparse_value("lazy dog")),
        ])
        .unwrap();
        db.flush().unwrap();
    }
    let mut db = Table::open(dir.path()).unwrap();

    let q = Query::new().and(Condition::Ann {
        column_id: 2,
        query: vec![1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0],
        k: 1,
    });
    assert_eq!(db.query(&q).unwrap().len(), 1, "ANN from graph checkpoint");

    let q = Query::new().and(Condition::SparseMatch {
        column_id: 3,
        query: tokenize("fox"),
        k: 2,
    });
    assert_eq!(
        db.query(&q).unwrap().len(),
        1,
        "sparse index served from checkpoint"
    );
}

#[test]
fn bulk_load_columns_builds_indexes_and_checkpoints() {
    // Phase 14.2: bulk_load_columns bulk-builds the live indexes straight from
    // the typed columns (no per-row index_into, no Row/Value), writes the
    // checkpoint, and PK lookups work in the live session — no reopen-rebuild
    // needed.
    let s = Schema {
        schema_id: 3,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "score".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "score_lr".into(),
            column_id: 2,
            kind: IndexKind::LearnedRange,
            predicate: None,
            options: Default::default(),
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    };
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create(dir.path(), s, 3).unwrap();
        let ids = NativeColumn::int64_sequence(1, 3);
        let scores = NativeColumn::Int64 {
            data: vec![10, 20, 30],
            validity: vec![0xFF; 3_usize.div_ceil(8)],
        };
        db.bulk_load_columns(vec![(1, ids), (2, scores)]).unwrap();
        assert_eq!(db.count(), 3);
        // The HOT is built from the typed PK column → lookup works immediately.
        let q = Query::pk(enc_i64(2));
        assert_eq!(
            db.query(&q).unwrap().len(),
            1,
            "bulk_load_columns must build the PK index from typed columns"
        );
        // Indexes are complete → checkpoint is written (instant reopen).
        assert!(
            dir.path().join("_idx/global.idx").exists(),
            "bulk_load_columns must checkpoint its bulk-built indexes"
        );
    }
    let mut db = Table::open(dir.path()).unwrap();
    assert_eq!(db.count(), 3, "rows survive reopen");
    let q = Query::pk(enc_i64(2));
    assert_eq!(db.query(&q).unwrap().len(), 1);
}
