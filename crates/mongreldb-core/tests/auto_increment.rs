//! Engine-native AUTO_INCREMENT: counter allocation, explicit-id advancement,
//! gap behavior, crash/reopen durability, and seed-from-max on legacy data.

use mongreldb_core::columnar::NativeColumn;
use mongreldb_core::manifest;
use mongreldb_core::schema::*;
use mongreldb_core::{Database, Query, RowId, Table, Value};
use tempfile::tempdir;

fn ai_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty()
                    .with(ColumnFlags::PRIMARY_KEY)
                    .with(ColumnFlags::AUTO_INCREMENT),
            },
            ColumnDef {
                id: 2,
                name: "label".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![],
        colocation: vec![],
    }
}

fn ai_int_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty()
                    .with(ColumnFlags::PRIMARY_KEY)
                    .with(ColumnFlags::AUTO_INCREMENT),
            },
            ColumnDef {
                id: 2,
                name: "value".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![],
        colocation: vec![],
    }
}

fn int_col(data: Vec<i64>) -> NativeColumn {
    let n = data.len();
    NativeColumn::Int64 {
        data,
        validity: vec![0xFF; n.div_ceil(8)],
    }
}

#[test]
fn allocates_monotonic_when_omitted() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), ai_schema(), 1).unwrap();
    let (r1, a1) = t
        .put_returning(vec![(2, Value::Bytes(b"a".to_vec()))])
        .unwrap();
    let (r2, a2) = t
        .put_returning(vec![(2, Value::Bytes(b"b".to_vec()))])
        .unwrap();
    let (r3, a3) = t
        .put_returning(vec![(2, Value::Bytes(b"c".to_vec()))])
        .unwrap();
    assert_eq!(a1, Some(1));
    assert_eq!(a2, Some(2));
    assert_eq!(a3, Some(3));
    assert_ne!(r1, r2);
    assert_ne!(r2, r3);
    t.commit().unwrap();
    // The assigned value must be materialized in the row.
    let snap = t.snapshot();
    let rows = t.visible_rows(snap).unwrap();
    let ids: Vec<i64> = rows
        .iter()
        .map(|r| match r.columns.get(&1) {
            Some(Value::Int64(n)) => *n,
            _ => panic!("expected Int64 id"),
        })
        .collect();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn null_pk_is_treated_as_omit() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), ai_schema(), 1).unwrap();
    let (_, a) = t
        .put_returning(vec![(1, Value::Null), (2, Value::Bytes(b"x".to_vec()))])
        .unwrap();
    assert_eq!(a, Some(1));
}

#[test]
fn explicit_id_advances_counter() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), ai_schema(), 1).unwrap();
    // Explicit id far ahead of the natural sequence.
    let (_, a0) = t
        .put_returning(vec![
            (1, Value::Int64(50)),
            (2, Value::Bytes(b"big".to_vec())),
        ])
        .unwrap();
    assert_eq!(a0, None, "explicit id is not reported as engine-assigned");
    // Next omitted allocation must clear the explicit value (no collision).
    let (_, a1) = t
        .put_returning(vec![(2, Value::Bytes(b"n".to_vec()))])
        .unwrap();
    assert_eq!(a1, Some(51));
}

#[test]
fn explicit_below_current_does_not_rewind() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), ai_schema(), 1).unwrap();
    let _ = t
        .put_returning(vec![(2, Value::Bytes(b"a".to_vec()))])
        .unwrap(); // id 1
    let _ = t
        .put_returning(vec![(2, Value::Bytes(b"b".to_vec()))])
        .unwrap(); // id 2
                   // Explicit id below the current counter is honored but must not rewind it.
    let _ = t
        .put_returning(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(b"dup".to_vec())),
        ])
        .unwrap();
    let (_, a) = t
        .put_returning(vec![(2, Value::Bytes(b"c".to_vec()))])
        .unwrap();
    assert_eq!(a, Some(3));
}

#[test]
fn batch_allocates_per_row() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), ai_schema(), 1).unwrap();
    let out = t
        .put_batch_returning(vec![
            vec![(2, Value::Bytes(b"a".to_vec()))],
            vec![(2, Value::Bytes(b"b".to_vec()))],
            vec![(2, Value::Bytes(b"c".to_vec()))],
        ])
        .unwrap();
    let assigned: Vec<i64> = out.iter().map(|(_, a)| a.unwrap()).collect();
    assert_eq!(assigned, vec![1, 2, 3]);

    // A batch mixing explicit + omitted rows advances the counter correctly.
    let out2 = t
        .put_batch_returning(vec![
            vec![(1, Value::Int64(100)), (2, Value::Bytes(b"x".to_vec()))],
            vec![(2, Value::Bytes(b"y".to_vec()))],
        ])
        .unwrap();
    assert_eq!(out2[0].1, None);
    assert_eq!(out2[1].1, Some(101));
}

#[test]
fn rejects_non_int64_auto_inc_value() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), ai_schema(), 1).unwrap();
    let err = t
        .put_returning(vec![
            (1, Value::Bytes(b"nope".to_vec())),
            (2, Value::Bytes(b"z".to_vec())),
        ])
        .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("auto_increment"),
        "got {err}"
    );
}

#[test]
fn rejects_invalid_auto_inc_schema() {
    let dir = tempdir().unwrap();
    let bad = Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Bytes,
            flags: ColumnFlags::empty()
                .with(ColumnFlags::PRIMARY_KEY)
                .with(ColumnFlags::AUTO_INCREMENT),
        }],
        indexes: vec![],
        colocation: vec![],
    };
    assert!(Table::create(dir.path(), bad, 1).is_err());
}

#[test]
fn counter_survives_reopen() {
    let dir = tempdir().unwrap();
    {
        let mut t = Table::create(dir.path(), ai_schema(), 1).unwrap();
        let _ = t
            .put_returning(vec![(2, Value::Bytes(b"a".to_vec()))])
            .unwrap(); // id 1
        let _ = t
            .put_returning(vec![(2, Value::Bytes(b"b".to_vec()))])
            .unwrap(); // id 2
        t.commit().unwrap();
    }
    // Reopen: the manifest-checkpointed counter continues past 2.
    {
        let mut t = Table::open(dir.path()).unwrap();
        let (_, a) = t
            .put_returning(vec![(2, Value::Bytes(b"c".to_vec()))])
            .unwrap();
        assert_eq!(a, Some(3));
    }
}

#[test]
fn seeds_from_max_on_legacy_data() {
    // A table loaded with explicit ids (e.g. upgraded from client-assigned ids,
    // or bulk-loaded before the counter was ever used) must seed the counter to
    // max(existing) + 1 on the first engine allocation.
    let dir = tempdir().unwrap();
    {
        let mut t = Table::create(dir.path(), ai_schema(), 1).unwrap();
        let batch: Vec<Vec<(u16, Value)>> = (1i64..=50)
            .map(|i| {
                vec![
                    (1, Value::Int64(i)),
                    (2, Value::Bytes(format!("r{i}").into_bytes())),
                ]
            })
            .collect();
        t.bulk_load(batch).unwrap();
        t.flush().unwrap();
    }
    // Reopen (counter is unseeded: manifest auto_inc_next == 0).
    {
        let mut t = Table::open(dir.path()).unwrap();
        let (_, a) = t
            .put_returning(vec![(2, Value::Bytes(b"new".to_vec()))])
            .unwrap();
        assert_eq!(
            a,
            Some(51),
            "must seed to max(existing)+1, not collide at 1"
        );
    }
}

#[test]
fn bulk_load_seeds_counter_when_table_was_empty() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), ai_int_schema(), 1).unwrap();
    t.bulk_load_columns(vec![
        (1, int_col(vec![10, 11, 12])),
        (2, int_col(vec![100, 101, 102])),
    ])
    .unwrap();

    let m = manifest::read(dir.path(), None).unwrap();
    assert_eq!(
        m.auto_inc_next, 13,
        "fresh bulk load should persist the next AUTO_INCREMENT value"
    );

    let (_, assigned) = t.put_returning(vec![(2, Value::Int64(103))]).unwrap();
    assert_eq!(assigned, Some(13));
}

#[test]
fn row_major_bulk_load_seeds_counter_when_table_was_empty() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), ai_int_schema(), 1).unwrap();
    t.bulk_load(vec![
        vec![(1, Value::Int64(20)), (2, Value::Int64(200))],
        vec![(1, Value::Int64(21)), (2, Value::Int64(201))],
    ])
    .unwrap();

    let m = manifest::read(dir.path(), None).unwrap();
    assert_eq!(m.auto_inc_next, 22);

    let (_, assigned) = t.put_returning(vec![(2, Value::Int64(202))]).unwrap();
    assert_eq!(assigned, Some(22));
}

#[test]
fn bulk_load_columns_fills_omitted_auto_inc_pk() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), ai_int_schema(), 1).unwrap();
    t.bulk_load_columns(vec![(2, int_col(vec![100, 101, 102]))])
        .unwrap();

    let rows = t.query(&Query::default()).unwrap();
    let ids: Vec<i64> = rows
        .iter()
        .map(|r| match r.columns.get(&1) {
            Some(Value::Int64(n)) => *n,
            _ => panic!("expected generated id"),
        })
        .collect();
    assert_eq!(ids, vec![1, 2, 3]);

    let m = manifest::read(dir.path(), None).unwrap();
    assert_eq!(m.auto_inc_next, 4);
}

#[test]
fn row_major_bulk_load_fills_omitted_auto_inc_pk() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), ai_int_schema(), 1).unwrap();
    t.bulk_load(vec![
        vec![(2, Value::Int64(100))],
        vec![(2, Value::Int64(101))],
        vec![(2, Value::Int64(102))],
    ])
    .unwrap();

    let rows = t.query(&Query::default()).unwrap();
    let ids: Vec<i64> = rows
        .iter()
        .map(|r| match r.columns.get(&1) {
            Some(Value::Int64(n)) => *n,
            _ => panic!("expected generated id"),
        })
        .collect();
    assert_eq!(ids, vec![1, 2, 3]);

    let m = manifest::read(dir.path(), None).unwrap();
    assert_eq!(m.auto_inc_next, 4);
}

#[test]
fn generated_put_after_lazy_bulk_indexing_remains_queryable() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), ai_int_schema(), 1).unwrap();
    t.bulk_load_columns(vec![
        (1, int_col(vec![1, 2, 3])),
        (2, int_col(vec![10, 20, 30])),
    ])
    .unwrap();

    let (_, assigned) = t.put_returning(vec![(2, Value::Int64(40))]).unwrap();
    assert_eq!(assigned, Some(4));
    t.commit().unwrap();

    let old = t.query(&Query::pk(Value::Int64(1).encode_key())).unwrap();
    assert_eq!(old.len(), 1);
    let new = t.query(&Query::pk(Value::Int64(4).encode_key())).unwrap();
    assert_eq!(new.len(), 1);
    assert_eq!(t.count(), 4);
}

#[test]
fn delete_after_lazy_bulk_indexing_hides_auto_inc_pk() {
    let dir = tempdir().unwrap();
    let mut t = Table::create(dir.path(), ai_int_schema(), 1).unwrap();
    t.bulk_load_columns(vec![
        (1, int_col(vec![1, 2, 3])),
        (2, int_col(vec![10, 20, 30])),
    ])
    .unwrap();

    t.delete(RowId(0)).unwrap();
    t.commit().unwrap();

    let deleted = t.query(&Query::pk(Value::Int64(1).encode_key())).unwrap();
    assert!(
        deleted.is_empty(),
        "deleted AUTO_INCREMENT PK must be hidden"
    );
    let live = t.query(&Query::pk(Value::Int64(2).encode_key())).unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(t.count(), 2);
}

#[test]
fn counter_survives_flush_then_reopen() {
    // After a flush, WAL data moves to a sorted run; the manifest must still
    // report the seeded counter so reopen does not re-seed.
    let dir = tempdir().unwrap();
    {
        let mut t = Table::create(dir.path(), ai_schema(), 1).unwrap();
        for c in ["a", "b", "c"] {
            let _ = t
                .put_returning(vec![(2, Value::Bytes(c.as_bytes().to_vec()))])
                .unwrap();
        }
        t.commit().unwrap();
        t.flush().unwrap();
    }
    {
        let mut t = Table::open(dir.path()).unwrap();
        let (_, a) = t
            .put_returning(vec![(2, Value::Bytes(b"d".to_vec()))])
            .unwrap();
        assert_eq!(a, Some(4));
    }
}

#[test]
fn database_shared_wal_counter_survives_reopen() {
    // The Kit's tables live in a Database (mounted/shared-WAL). Verifies the
    // counter is checkpointed on the mounted commit path and recovered via
    // manifest + shared-WAL replay.
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("things", ai_schema()).unwrap();
        let handle = db.table("things").unwrap();
        {
            let mut g = handle.lock();
            let (_, a1) = g
                .put_returning(vec![(2, Value::Bytes(b"a".to_vec()))])
                .unwrap();
            let (_, a2) = g
                .put_returning(vec![(2, Value::Bytes(b"b".to_vec()))])
                .unwrap();
            assert_eq!((a1, a2), (Some(1), Some(2)));
            g.commit().unwrap();
        }
    }
    {
        let db = Database::open(dir.path()).unwrap();
        let handle = db.table("things").unwrap();
        let mut g = handle.lock();
        let (_, a) = g
            .put_returning(vec![(2, Value::Bytes(b"c".to_vec()))])
            .unwrap();
        g.commit().unwrap();
        assert_eq!(a, Some(3));
    }
}
