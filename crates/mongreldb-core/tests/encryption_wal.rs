//! WAL + cache encryption tests.
#![cfg(feature = "encryption")]

use mongreldb_core::schema::*;
use mongreldb_core::{Table, Value};
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
            },
            ColumnDef {
                id: 2,
                name: "name".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn rows(n: i64) -> Vec<Vec<(u16, Value)>> {
    (0..n)
        .map(|i| {
            vec![
                (1, Value::Int64(i)),
                (2, Value::Bytes(format!("user{i}").into_bytes())),
            ]
        })
        .collect()
}

#[test]
fn encrypted_wal_round_trip() {
    let dir = tempdir().unwrap();
    {
        let mut db =
            Table::create_encrypted(dir.path().join("t"), schema(), 1, "passphrase").unwrap();
        db.bulk_load(rows(100)).unwrap();
        db.put(vec![
            (1, Value::Int64(999)),
            (2, Value::Bytes(b"extra".to_vec())),
        ])
        .unwrap();
        db.commit().unwrap();
    }
    {
        let db = Table::open_encrypted(dir.path().join("t"), "passphrase").unwrap();
        assert_eq!(db.count(), 101);
    }
}

#[test]
fn encrypted_wal_wrong_key_fails() {
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create_encrypted(dir.path().join("t"), schema(), 1, "correct").unwrap();
        db.put(vec![(1, Value::Int64(1)), (2, Value::Bytes(b"a".to_vec()))])
            .unwrap();
        db.commit().unwrap();
    }
    let result = Table::open_encrypted(dir.path().join("t"), "wrong");
    assert!(result.is_err(), "wrong passphrase must fail");
}

#[test]
fn plaintext_wal_still_works() {
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create(dir.path().join("t"), schema(), 1).unwrap();
        db.put(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(b"hello".to_vec())),
        ])
        .unwrap();
        db.commit().unwrap();
    }
    {
        let db = Table::open(dir.path().join("t")).unwrap();
        assert_eq!(db.count(), 1);
    }
}

#[test]
fn raw_key_create_and_open() {
    let dir = tempdir().unwrap();
    let key = (0..32u8).collect::<Vec<u8>>();
    {
        let mut db = Table::create_with_key(dir.path().join("t"), schema(), 1, &key).unwrap();
        db.put(vec![
            (1, Value::Int64(42)),
            (2, Value::Bytes(b"keytest".to_vec())),
        ])
        .unwrap();
        db.commit().unwrap();
    }
    {
        let db = Table::open_with_key(dir.path().join("t"), &key).unwrap();
        assert_eq!(db.count(), 1);
    }
}

#[test]
fn raw_key_wrong_fails() {
    let dir = tempdir().unwrap();
    let key = (0..32u8).collect::<Vec<u8>>();
    let wrong = (1..33u8).collect::<Vec<u8>>();
    {
        let mut db = Table::create_with_key(dir.path().join("t"), schema(), 1, &key).unwrap();
        db.put(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(b"data".to_vec())),
        ])
        .unwrap();
        db.commit().unwrap();
    }
    // Opening with the wrong key fails when trying to decrypt WAL frames.
    let result = Table::open_with_key(dir.path().join("t"), &wrong);
    assert!(
        result.is_err(),
        "wrong key must fail on encrypted WAL replay"
    );
}

#[test]
fn encrypted_cache_survives_restart() {
    use mongreldb_core::query::{Condition, Query};
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create_encrypted(dir.path().join("t"), schema(), 1, "pass").unwrap();
        db.bulk_load(rows(100)).unwrap();
        db.flush().unwrap();
        let q = Query::new().and(Condition::Range {
            column_id: 1,
            lo: 10,
            hi: 20,
        });
        let r = db.query_cached(&q).unwrap();
        assert_eq!(r.len(), 11, "initial query returns 11 rows");
    }
    {
        let mut db = Table::open_encrypted(dir.path().join("t"), "pass").unwrap();
        let q = Query::new().and(Condition::Range {
            column_id: 1,
            lo: 10,
            hi: 20,
        });
        let r = db.query_cached(&q).unwrap();
        assert_eq!(r.len(), 11, "encrypted cache hit after restart");
    }
}
