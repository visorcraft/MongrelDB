//! Encryption data-integrity tests.
//!
//! These tests exercise MongrelDB's page-level AES-256-GCM encryption through
//! normal API usage: create/open with passphrase and raw key, wrong-key
//! rejection, reopen after flush, encrypted-indexable columns (bitmap and range
//! queries), result-cache persistence, deterministic tokens across runs, and
//! checks for plaintext leakage or incorrect decryption.
#![cfg(feature = "encryption")]

use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{RowId, Snapshot, Table, Value};
use tempfile::tempdir;

fn schema_with_indexable_bitmap() -> Schema {
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
                name: "category".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty()
                    .with(ColumnFlags::ENCRYPTED)
                    .with(ColumnFlags::ENCRYPTED_INDEXABLE),
            },
            ColumnDef {
                id: 3,
                name: "score".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty()
                    .with(ColumnFlags::ENCRYPTED)
                    .with(ColumnFlags::NULLABLE),
            },
        ],
        indexes: vec![IndexDef {
            name: "category_bm".into(),
            column_id: 2,
            kind: IndexKind::Bitmap,
        }],
        colocation: vec![],
        constraints: Default::default(),
    }
}

fn schema_with_indexable_range() -> Schema {
    Schema {
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
                name: "score".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty()
                    .with(ColumnFlags::ENCRYPTED)
                    .with(ColumnFlags::ENCRYPTED_INDEXABLE),
            },
        ],
        indexes: vec![IndexDef {
            name: "score_lr".into(),
            column_id: 2,
            kind: IndexKind::LearnedRange,
        }],
        colocation: vec![],
        constraints: Default::default(),
    }
}

fn schema_plain_encrypted_mix() -> Schema {
    Schema {
        schema_id: 3,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 2,
                name: "plain_label".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 3,
                name: "secret".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty().with(ColumnFlags::ENCRYPTED),
            },
        ],
        indexes: vec![IndexDef {
            name: "plain_label_bm".into(),
            column_id: 2,
            kind: IndexKind::Bitmap,
        }],
        colocation: vec![],
        constraints: Default::default(),
    }
}

fn row(id: i64, category: &[u8], score: i64) -> Vec<(u16, Value)> {
    vec![
        (1, Value::Int64(id)),
        (2, Value::Bytes(category.to_vec())),
        (3, Value::Int64(score)),
    ]
}

fn row_range(id: i64, score: i64) -> Vec<(u16, Value)> {
    vec![(1, Value::Int64(id)), (2, Value::Int64(score))]
}

fn row_mix(id: i64, plain: &[u8], secret: &[u8]) -> Vec<(u16, Value)> {
    vec![
        (1, Value::Int64(id)),
        (2, Value::Bytes(plain.to_vec())),
        (3, Value::Bytes(secret.to_vec())),
    ]
}

fn seeded_bitmap_table(dir: &std::path::Path, passphrase: &str) -> Table {
    let mut db =
        Table::create_encrypted(dir, schema_with_indexable_bitmap(), 1, passphrase).unwrap();
    for i in 0..100 {
        let cat = if i % 3 == 0 {
            b"alpha".to_vec()
        } else {
            b"beta!".to_vec()
        };
        db.put(row(i as i64, &cat, i as i64)).unwrap();
    }
    db.commit().unwrap();
    db.set_mutable_run_spill_bytes(1);
    db.flush().unwrap();
    db
}

fn seeded_range_table(dir: &std::path::Path, passphrase: &str) -> Table {
    let mut db =
        Table::create_encrypted(dir, schema_with_indexable_range(), 1, passphrase).unwrap();
    for i in 0..100 {
        db.put(row_range(i as i64, i as i64)).unwrap();
    }
    db.commit().unwrap();
    db.set_mutable_run_spill_bytes(1);
    db.flush().unwrap();
    db.add_learned_range_index("score").unwrap();
    db
}

#[test]
fn passphrase_create_open_round_trip() {
    let dir = tempdir().unwrap();
    let committed_id = {
        let mut db = Table::create_encrypted(
            dir.path(),
            schema_with_indexable_bitmap(),
            1,
            "correct horse battery staple",
        )
        .unwrap();
        let id = db.put(row(42, b"secret-category", 7)).unwrap();
        db.commit().unwrap();
        id
    };
    {
        let db = Table::open_encrypted(dir.path(), "correct horse battery staple").unwrap();
        assert_eq!(db.count(), 1);
        let snap = db.snapshot();
        let r = db.get(committed_id, snap).unwrap();
        assert_eq!(
            r.columns.get(&2),
            Some(&Value::Bytes(b"secret-category".to_vec()))
        );
        assert_eq!(r.columns.get(&3), Some(&Value::Int64(7)));
    }
}

#[test]
fn raw_key_create_open_round_trip() {
    let dir = tempdir().unwrap();
    let key: Vec<u8> = (0..64).map(|i| i as u8).collect();
    let committed_id = {
        let mut db =
            Table::create_with_key(dir.path(), schema_with_indexable_bitmap(), 1, &key).unwrap();
        let id = db.put(row(99, b"raw-key-data", 123)).unwrap();
        db.commit().unwrap();
        id
    };
    {
        let db = Table::open_with_key(dir.path(), &key).unwrap();
        assert_eq!(db.count(), 1);
        let r = db.get(committed_id, db.snapshot()).unwrap();
        assert_eq!(
            r.columns.get(&2),
            Some(&Value::Bytes(b"raw-key-data".to_vec()))
        );
    }
}

#[test]
fn wrong_passphrase_rejected() {
    let dir = tempdir().unwrap();
    {
        let mut db =
            Table::create_encrypted(dir.path(), schema_with_indexable_bitmap(), 1, "right")
                .unwrap();
        db.put(row(1, b"x", 1)).unwrap();
        db.commit().unwrap();
    }
    assert!(Table::open_encrypted(dir.path(), "wrong").is_err());
}

#[test]
fn wrong_raw_key_rejected() {
    let dir = tempdir().unwrap();
    let key: Vec<u8> = (0..32).map(|i| i as u8).collect();
    let wrong: Vec<u8> = (1..33).map(|i| i as u8).collect();
    {
        let mut db =
            Table::create_with_key(dir.path(), schema_with_indexable_bitmap(), 1, &key).unwrap();
        db.put(row(1, b"x", 1)).unwrap();
        db.commit().unwrap();
    }
    assert!(Table::open_with_key(dir.path(), &wrong).is_err());
}

#[test]
fn encrypted_table_reopen_after_flush() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut db =
            Table::create_encrypted(&path, schema_with_indexable_bitmap(), 1, "pass").unwrap();
        for i in 0..50 {
            db.put(row(i, b"cat", i)).unwrap();
        }
        db.flush().unwrap();
    }
    {
        let db = Table::open_encrypted(&path, "pass").unwrap();
        assert_eq!(db.count(), 50);
        let all = db.visible_rows(db.snapshot()).unwrap();
        assert_eq!(all.len(), 50);
    }
}

#[test]
fn encrypted_bitmap_query_on_indexable_column() {
    let dir = tempdir().unwrap();
    let mut db = seeded_bitmap_table(dir.path(), "pass");

    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: Value::Bytes(b"alpha".to_vec()).encode_key(),
    });
    let r = db.query(&q).unwrap();
    assert_eq!(r.len(), 34, "34 rows have category alpha");
    for row in &r {
        assert_eq!(row.columns.get(&2), Some(&Value::Bytes(b"alpha".to_vec())));
    }
}

#[test]
fn encrypted_range_query_on_indexable_column() {
    let dir = tempdir().unwrap();
    let mut db = seeded_range_table(dir.path(), "pass");

    let q = Query::new().and(Condition::Range {
        column_id: 2,
        lo: 10,
        hi: 20,
    });
    let r = db.query(&q).unwrap();
    assert_eq!(r.len(), 11, "range [10,20] inclusive has 11 rows");
}

#[test]
fn encrypted_pk_lookup_on_indexable_column() {
    let dir = tempdir().unwrap();
    let mut db =
        Table::create_encrypted(dir.path(), schema_with_indexable_bitmap(), 1, "pass").unwrap();
    let id = db.put(row(12345, b"pk-test", 0)).unwrap();
    db.commit().unwrap();

    let rid = db.lookup_pk(&Value::Int64(12345).encode_key()).unwrap();
    assert_eq!(rid, id);
}

#[test]
fn deterministic_tokens_across_runs() {
    let dir = tempdir().unwrap();
    let mut db =
        Table::create_encrypted(dir.path(), schema_with_indexable_bitmap(), 1, "pass").unwrap();

    // First run.
    for i in 0..50 {
        db.put(row(i, b"shared", i)).unwrap();
    }
    db.set_mutable_run_spill_bytes(1);
    db.flush().unwrap();

    // Second run with the same category value.
    for i in 50..100 {
        db.put(row(i, b"shared", i)).unwrap();
    }
    db.flush().unwrap();

    assert_eq!(db.run_count(), 2);

    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: Value::Bytes(b"shared".to_vec()).encode_key(),
    });
    let r = db.query(&q).unwrap();
    assert_eq!(r.len(), 100, "same token must match rows in both runs");
}

#[test]
fn data_survives_close_open_without_flush() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut db =
            Table::create_encrypted(&path, schema_with_indexable_bitmap(), 1, "pass").unwrap();
        db.put(row(1, b"wal-only", 10)).unwrap();
        db.put(row(2, b"wal-only", 20)).unwrap();
        db.commit().unwrap();
    }
    {
        let db = Table::open_encrypted(&path, "pass").unwrap();
        assert_eq!(db.count(), 2);
        let all = db.visible_rows(db.snapshot()).unwrap();
        assert_eq!(all.len(), 2);
    }
}

#[test]
fn delete_then_reopen_encrypted() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let victim = {
        let mut db =
            Table::create_encrypted(&path, schema_with_indexable_bitmap(), 1, "pass").unwrap();
        let id = db.put(row(7, b"to-delete", 7)).unwrap();
        db.put(row(8, b"keep", 8)).unwrap();
        db.commit().unwrap();
        db.delete(id).unwrap();
        db.commit().unwrap();
        id
    };
    {
        let db = Table::open_encrypted(&path, "pass").unwrap();
        assert_eq!(db.count(), 1);
        let snap = db.snapshot();
        assert!(db.get(victim, snap).is_none());
        let all = db.visible_rows(snap).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(
            all[0].columns.get(&2),
            Some(&Value::Bytes(b"keep".to_vec()))
        );
    }
}

#[test]
fn encrypted_partial_column_put() {
    let dir = tempdir().unwrap();
    let mut db =
        Table::create_encrypted(dir.path(), schema_with_indexable_bitmap(), 1, "pass").unwrap();
    let id = db
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(b"partial".to_vec())),
        ])
        .unwrap();
    db.commit().unwrap();

    let r = db.get(id, db.snapshot()).unwrap();
    assert_eq!(r.columns.get(&2), Some(&Value::Bytes(b"partial".to_vec())));
    assert_eq!(r.columns.get(&3), None);
}

#[test]
fn encrypted_count_matches_visible_rows() {
    let dir = tempdir().unwrap();
    let db = seeded_bitmap_table(dir.path(), "pass");
    let via_count = db.count();
    let via_scan = db.visible_rows(db.snapshot()).unwrap().len() as u64;
    assert_eq!(via_count, via_scan);
    assert_eq!(via_count, 100);
}

#[test]
fn encrypted_schema_evolution() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut db =
            Table::create_encrypted(&path, schema_with_indexable_bitmap(), 1, "pass").unwrap();
        db.put(row(1, b"old", 1)).unwrap();
        db.flush().unwrap();
        let _new_cid = db
            .add_column(
                "new_col",
                TypeId::Int64,
                ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            )
            .unwrap();
        db.put(vec![
            (1, Value::Int64(2)),
            (2, Value::Bytes(b"new".to_vec())),
            (3, Value::Int64(3)),
            (4, Value::Int64(99)),
        ])
        .unwrap();
        db.commit().unwrap();
    }
    {
        let db = Table::open_encrypted(&path, "pass").unwrap();
        let all = db.visible_rows(db.snapshot()).unwrap();
        assert_eq!(all.len(), 2);
    }
}

#[test]
fn encrypted_result_cache_persists_and_decrypts_correctly() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut db =
            Table::create_encrypted(&path, schema_with_indexable_bitmap(), 1, "pass").unwrap();
        for i in 0..100 {
            let cat = if i % 4 == 0 {
                b"alpha".to_vec()
            } else {
                b"beta!".to_vec()
            };
            db.put(row(i as i64, &cat, i as i64)).unwrap();
        }
        db.flush().unwrap();

        let q = Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: Value::Bytes(b"alpha".to_vec()).encode_key(),
        });
        let r0 = db.query_cached(&q).unwrap();
        assert_eq!(r0.len(), 25);
    }
    {
        let mut db = Table::open_encrypted(&path, "pass").unwrap();
        let q = Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: Value::Bytes(b"alpha".to_vec()).encode_key(),
        });
        let r1 = db.query_cached(&q).unwrap();
        assert_eq!(r1.len(), 25, "persistent encrypted cache hit after restart");
        for row in &r1 {
            assert_eq!(row.columns.get(&2), Some(&Value::Bytes(b"alpha".to_vec())));
        }
    }
}

#[test]
fn encrypted_cache_invalidation_on_commit() {
    let dir = tempdir().unwrap();
    let mut db = seeded_bitmap_table(dir.path(), "pass");

    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: Value::Bytes(b"alpha".to_vec()).encode_key(),
    });
    let r0 = db.query_cached(&q).unwrap();
    assert_eq!(r0.len(), 34);

    // Insert a new row matching the cached query; the commit must invalidate.
    db.put(row(1000, b"alpha", 1000)).unwrap();
    db.commit().unwrap();

    let r1 = db.query_cached(&q).unwrap();
    assert_eq!(r1.len(), 35, "cache must reflect the new matching row");
}

#[test]
fn no_plaintext_leakage_in_run_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut db =
            Table::create_encrypted(&path, schema_with_indexable_bitmap(), 1, "pass").unwrap();
        db.put(row(1, b"super-secret-category", 42)).unwrap();
        db.set_mutable_run_spill_bytes(1);
        db.flush().unwrap();
    }

    let runs_dir = path.join("_runs");
    for entry in std::fs::read_dir(&runs_dir).unwrap() {
        let entry = entry.unwrap();
        let bytes = std::fs::read(entry.path()).unwrap();
        let as_str = String::from_utf8_lossy(&bytes);
        assert!(
            !as_str.contains("super-secret-category"),
            "plaintext must not appear in encrypted run file"
        );
    }
}

#[test]
fn encrypted_and_plaintext_columns_coexist() {
    let dir = tempdir().unwrap();
    let mut db =
        Table::create_encrypted(dir.path(), schema_plain_encrypted_mix(), 1, "pass").unwrap();
    db.put(row_mix(1, b"plain", b"cipher")).unwrap();
    db.commit().unwrap();
    db.set_mutable_run_spill_bytes(1);
    db.flush().unwrap();

    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: Value::Bytes(b"plain".to_vec()).encode_key(),
    });
    let r = db.query(&q).unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(
        r[0].columns.get(&3),
        Some(&Value::Bytes(b"cipher".to_vec()))
    );
}

#[test]
fn reopen_after_multiple_flushes() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut db =
            Table::create_encrypted(&path, schema_with_indexable_bitmap(), 1, "pass").unwrap();
        db.set_mutable_run_spill_bytes(1);
        for batch in 0..3 {
            for i in 0..10 {
                db.put(row(batch * 10 + i, b"multi", batch * 10 + i))
                    .unwrap();
            }
            db.flush().unwrap();
        }
    }
    {
        let db = Table::open_encrypted(&path, "pass").unwrap();
        assert_eq!(db.count(), 30);
        let all = db.visible_rows(db.snapshot()).unwrap();
        assert_eq!(all.len(), 30);
    }
}

#[test]
fn encrypted_wal_replay_skips_uncommitted_txn() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut db =
            Table::create_encrypted(&path, schema_with_indexable_bitmap(), 1, "pass").unwrap();
        db.put(row(1, b"committed", 1)).unwrap();
        db.commit().unwrap();
        db.put(row(2, b"uncommitted", 2)).unwrap();
        // Intentionally no commit.
    }
    {
        let db = Table::open_encrypted(&path, "pass").unwrap();
        assert_eq!(db.count(), 1);
    }
}

#[test]
fn encrypted_compaction_preserves_data() {
    let dir = tempdir().unwrap();
    let mut db =
        Table::create_encrypted(dir.path(), schema_with_indexable_bitmap(), 1, "pass").unwrap();
    db.set_mutable_run_spill_bytes(1);
    for i in 0..20 {
        db.put(row(i, b"compact", i)).unwrap();
        db.flush().unwrap();
    }
    let before = db.visible_rows(db.snapshot()).unwrap().len();
    db.compact().unwrap();
    let after = db.visible_rows(db.snapshot()).unwrap().len();
    assert_eq!(before, after);
    assert_eq!(db.count(), 20);
}

#[test]
fn encrypted_snapshot_isolation_across_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let snap_epoch = {
        let mut db =
            Table::create_encrypted(&path, schema_with_indexable_bitmap(), 1, "pass").unwrap();
        db.put(row(1, b"v1", 1)).unwrap();
        let e = db.commit().unwrap();
        db.put(row(1, b"v2", 2)).unwrap();
        db.commit().unwrap();
        e
    };
    {
        let db = Table::open_encrypted(&path, "pass").unwrap();
        let old = db.get(RowId(0), Snapshot::at(snap_epoch));
        // RowId 0 is the first allocated id in a fresh table.
        assert!(old.is_some());
        assert_eq!(
            old.unwrap().columns.get(&2),
            Some(&Value::Bytes(b"v1".to_vec()))
        );
    }
}

#[test]
fn encrypted_query_columns_native_cached_no_leak_on_wrong_key() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let mut db =
            Table::create_encrypted(&path, schema_with_indexable_bitmap(), 1, "pass").unwrap();
        for i in 0..50 {
            db.put(row(i as i64, b"cache", i as i64)).unwrap();
        }
        db.flush().unwrap();
        let q = Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: Value::Bytes(b"cache".to_vec()).encode_key(),
        });
        let _ = db.query_cached(&q).unwrap();
    }
    // Opening with a wrong key must fail; it must not silently return corrupt cache rows.
    assert!(Table::open_encrypted(&path, "wrong").is_err());
}
