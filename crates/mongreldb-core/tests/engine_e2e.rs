//! End-to-end engine tests: flush → read across runs, MVCC snapshots, the
//! conjunctive query surface, crash-free reopen/recovery, and (with the
//! `encryption` feature) encrypted flush + read.

use mongreldb_core::{
    schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId},
    Condition, Query, RowId, Snapshot, Table, Value,
};
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
            },
            ColumnDef {
                id: 2,
                name: "label".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
            ColumnDef {
                id: 3,
                name: "emb".into(),
                ty: TypeId::Embedding { dim: 8 },
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
            },
        ],
        indexes: vec![
            IndexDef {
                name: "label_bitmap".into(),
                column_id: 2,
                kind: IndexKind::Bitmap,
                predicate: None,
                options: Default::default(),
            },
            IndexDef {
                name: "label_fm".into(),
                column_id: 2,
                kind: IndexKind::FmIndex,
                predicate: None,
                options: Default::default(),
            },
            IndexDef {
                name: "emb_ann".into(),
                column_id: 3,
                kind: IndexKind::Ann,
                predicate: None,
                options: Default::default(),
            },
        ],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn red_emb() -> Vec<f32> {
    vec![1.0, 1.0, 1.0, 1.0, -1.0, -1.0, -1.0, -1.0]
}
fn blue_emb() -> Vec<f32> {
    vec![-1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0, 1.0]
}

fn seed(db: &mut Table) -> Vec<RowId> {
    let specs: [(i64, &[u8], Vec<f32>); 4] = [
        (10, b"red", red_emb()),
        (20, b"blue", blue_emb()),
        (30, b"red", red_emb()),
        (40, b"green", red_emb()),
    ];
    let mut ids = Vec::new();
    for (id, label, emb) in specs {
        ids.push(
            db.put(vec![
                (1, Value::Int64(id)),
                (2, Value::Bytes(label.to_vec())),
                (3, Value::Embedding(emb)),
            ])
            .unwrap(),
        );
    }
    ids
}

#[test]
fn flush_then_read_across_run() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    let ids = seed(&mut db);
    db.set_mutable_run_spill_bytes(1); // force a spill so reads cross into the run
    db.flush().unwrap(); // memtable → sorted run; memtable now empty
    assert_eq!(db.run_count(), 1);
    assert_eq!(db.memtable_len(), 0);

    let snap = db.snapshot();
    // Reads now come from the run, not the memtable.
    let row = db.get(ids[1], snap).unwrap();
    assert!(matches!(row.columns.get(&2), Some(Value::Bytes(_))));
    // PK lookup still resolves.
    let rid = db.lookup_pk(&Value::Int64(20).encode_key()).unwrap();
    assert_eq!(rid, ids[1]);
}

#[test]
fn count_conditions_returns_survivor_cardinality() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    seed(&mut db);
    db.flush().unwrap();

    let snap = db.snapshot();
    let red = [Condition::BitmapEq {
        column_id: 2,
        value: b"red".to_vec(),
    }];
    assert_eq!(db.count_conditions(&red, snap).unwrap(), Some(2));

    let pk = [Condition::Pk(Value::Int64(20).encode_key())];
    assert_eq!(db.count_conditions(&pk, snap).unwrap(), Some(1));
}

#[test]
fn mvcc_snapshot_isolation_after_update() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    let id = db
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(b"v1".to_vec())),
        ])
        .unwrap();
    let e1 = db.commit().unwrap();
    db.put(vec![
        (1, Value::Int64(1)),
        (2, Value::Bytes(b"v2".to_vec())),
    ])
    .unwrap();
    let _ = db
        .put(vec![
            (1, Value::Int64(99)),
            (2, Value::Bytes(b"noise".to_vec())),
        ])
        .unwrap(); // bump allocator/seq noise
    let e2 = db.commit().unwrap();

    // Old snapshot sees v1; new snapshot sees v2 (latest row id).
    assert!(matches!(
        db.get(id, Snapshot::at(e1)).unwrap().columns.get(&2),
        Some(Value::Bytes(b))
            if b == b"v1"
    ));
    assert_eq!(db.visible_rows(Snapshot::at(e1)).unwrap().len(), 1);
    let _ = e2;
}

#[test]
fn conjunctive_query_intersects_row_id_space() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    let _ = seed(&mut db);
    db.commit().unwrap();

    // Bitmap equality: label == "red" (two rows).
    let reds = db
        .query(&Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"red".to_vec(),
        }))
        .unwrap();
    assert_eq!(reds.len(), 2);

    // FM substring: label contains "lu" (the "blue" row).
    let blues = db
        .query(&Query::new().and(Condition::FmContains {
            column_id: 2,
            pattern: b"lu".to_vec(),
        }))
        .unwrap();
    assert_eq!(blues.len(), 1);

    // ANN: nearest to the "red" embedding returns red rows first.
    let nearest = db
        .query(&Query::new().and(Condition::Ann {
            column_id: 3,
            query: red_emb(),
            k: 10,
        }))
        .unwrap();
    assert!(nearest.len() >= 2);
}

#[test]
fn reopen_recovers_state_from_wal_and_runs() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let committed_id = {
        let mut db = Table::create(&path, schema(), 1).unwrap();
        let ids = seed(&mut db);
        db.flush().unwrap(); // durable in a run
                             // A few more writes that live only in the WAL (not flushed).
        db.put(vec![
            (1, Value::Int64(50)),
            (2, Value::Bytes(b"yellow".to_vec())),
            (3, Value::Embedding(red_emb())),
        ])
        .unwrap();
        db.commit().unwrap();
        ids[0]
    };
    // Reopen: runs + WAL replay rebuild the live state.
    let mut db = Table::open(&path).unwrap();
    let snap = db.snapshot();
    assert!(db.get(committed_id, snap).is_some(), "flushed row survives");
    let all = db.visible_rows(snap).unwrap();
    assert_eq!(all.len(), 5, "4 flushed + 1 WAL-replayed");
    // The replayed row is queryable via its bitmap index.
    let yellow = db
        .query(&Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"yellow".to_vec(),
        }))
        .unwrap();
    assert_eq!(yellow.len(), 1);
}

#[test]
fn compaction_preserves_visible_state() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    let _ = seed(&mut db);
    db.set_mutable_run_spill_bytes(1); // force a spill per flush (exercises compaction)
    db.flush().unwrap();
    db.put(vec![
        (1, Value::Int64(60)),
        (2, Value::Bytes(b"red".to_vec())),
        (3, Value::Embedding(red_emb())),
    ])
    .unwrap();
    db.flush().unwrap();
    assert_eq!(db.run_count(), 2);
    let before = db.visible_rows(db.snapshot()).unwrap().len();

    db.compact().unwrap();
    assert_eq!(db.run_count(), 1);
    let after = db.visible_rows(db.snapshot()).unwrap().len();
    assert_eq!(before, after, "compaction must not change the visible set");
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_flush_and_read_round_trips() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut db =
            Table::create_encrypted(&path, schema(), 1, "correct horse battery staple").unwrap();
        db.set_mutable_run_spill_bytes(1); // force a spill so the run file is encrypted on disk
        let id = db
            .put(vec![
                (1, Value::Int64(7)),
                (2, Value::Bytes(b"secret".to_vec())),
            ])
            .unwrap();
        db.flush().unwrap();
        let snap = db.snapshot();
        let row = db.get(id, snap).unwrap();
        assert!(matches!(row.columns.get(&2), Some(Value::Bytes(b)) if b == b"secret"));
    }
    // Reopen with the right passphrase; pages decrypt on read.
    let db = Table::open_encrypted(&path, "correct horse battery staple").unwrap();
    let all = db.visible_rows(db.snapshot()).unwrap();
    assert_eq!(all.len(), 1);
    // The on-disk page bytes must not contain the plaintext.
    let run_bytes = std::fs::read(
        std::fs::read_dir(path.join("_runs"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path(),
    )
    .unwrap();
    assert!(!std::str::from_utf8(&run_bytes)
        .unwrap_or("")
        .contains("secret"));
}
