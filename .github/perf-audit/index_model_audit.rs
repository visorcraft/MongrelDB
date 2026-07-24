use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Database, Value};
use std::collections::BTreeMap;
use tempfile::tempdir;

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExpectedRow {
    bucket: i64,
    tag: Vec<u8>,
    payload: i64,
}

fn schema() -> Schema {
    Schema {
        schema_id: 991,
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
                name: "bucket".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 3,
                name: "tag".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 4,
                name: "payload".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![
            IndexDef {
                name: "bucket_bitmap".into(),
                column_id: 2,
                kind: IndexKind::Bitmap,
                predicate: None,
                options: Default::default(),
            },
            IndexDef {
                name: "tag_bitmap".into(),
                column_id: 3,
                kind: IndexKind::Bitmap,
                predicate: None,
                options: Default::default(),
            },
        ],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 11
    }
}

fn full_cells(pk: i64, row: &ExpectedRow) -> Vec<(u16, Value)> {
    vec![
        (1, Value::Int64(pk)),
        (2, Value::Int64(row.bucket)),
        (3, Value::Bytes(row.tag.clone())),
        (4, Value::Int64(row.payload)),
    ]
}

fn lookup_pk(db: &Database, pk: i64) -> Option<mongreldb_core::RowId> {
    let handle = db.table("items").unwrap();
    let guard = handle.lock();
    guard.lookup_pk(&Value::Int64(pk).encode_key())
}

fn query_pks(db: &Database, query: Query) -> Vec<i64> {
    let handle = db.table("items").unwrap();
    let mut guard = handle.lock();
    let rows = guard.query(&query).unwrap();
    let mut pks = rows
        .iter()
        .map(|row| match row.columns.get(&1) {
            Some(Value::Int64(pk)) => *pk,
            other => panic!("expected integer primary key, got {other:?}"),
        })
        .collect::<Vec<_>>();
    pks.sort_unstable();
    pks
}

fn expected_pks(
    model: &BTreeMap<i64, ExpectedRow>,
    predicate: impl Fn(&ExpectedRow) -> bool,
) -> Vec<i64> {
    model
        .iter()
        .filter_map(|(pk, row)| predicate(row).then_some(*pk))
        .collect()
}

fn assert_model(db: &Database, model: &BTreeMap<i64, ExpectedRow>) {
    for pk in 0..64_i64 {
        let rows = query_pks(
            db,
            Query::new().and(Condition::Pk(Value::Int64(pk).encode_key())),
        );
        let expected = model.contains_key(&pk).then_some(vec![pk]).unwrap_or_default();
        assert_eq!(rows, expected, "PK query mismatch for {pk}");
        assert_eq!(
            lookup_pk(db, pk).is_some(),
            model.contains_key(&pk),
            "HOT/fallback PK lookup mismatch for {pk}"
        );
    }

    for bucket in 0..8_i64 {
        let expected = expected_pks(model, |row| row.bucket == bucket);
        let bitmap = query_pks(
            db,
            Query::new().and(Condition::BitmapEq {
                column_id: 2,
                value: Value::Int64(bucket).encode_key(),
            }),
        );
        let range = query_pks(
            db,
            Query::new().and(Condition::Range {
                column_id: 2,
                lo: bucket,
                hi: bucket,
            }),
        );
        assert_eq!(bitmap, expected, "bitmap mismatch for bucket {bucket}");
        assert_eq!(range, expected, "RangeInt equality mismatch for bucket {bucket}");
    }

    let expected_range = expected_pks(model, |row| (2..=5).contains(&row.bucket));
    let actual_range = query_pks(
        db,
        Query::new().and(Condition::Range {
            column_id: 2,
            lo: 2,
            hi: 5,
        }),
    );
    assert_eq!(actual_range, expected_range, "multi-value RangeInt mismatch");

    for tag_id in 0..8_i64 {
        let tag = format!("tag-{tag_id}").into_bytes();
        let expected = expected_pks(model, |row| row.tag == tag);
        let actual = query_pks(
            db,
            Query::new().and(Condition::BitmapEq {
                column_id: 3,
                value: tag,
            }),
        );
        assert_eq!(actual, expected, "bytes bitmap mismatch for tag-{tag_id}");
    }
}

#[test]
fn randomized_pk_bitmap_and_range_indexes_match_model() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("items", schema()).unwrap();

    let mut model = BTreeMap::<i64, ExpectedRow>::new();
    let mut rng = Lcg(0xD1B54A32D192ED03);

    for step in 0..1_200_u64 {
        let op = rng.next() % 6;
        let pk = (rng.next() % 64) as i64;
        match op {
            0 | 1 => {
                let row = ExpectedRow {
                    bucket: (rng.next() % 8) as i64,
                    tag: format!("tag-{}", rng.next() % 8).into_bytes(),
                    payload: rng.next() as i64,
                };
                db.transaction(|tx| {
                    tx.put("items", full_cells(pk, &row))?;
                    Ok(())
                })
                .unwrap();
                model.insert(pk, row);
            }
            2 => {
                if let (Some(row_id), Some(row)) = (lookup_pk(&db, pk), model.get_mut(&pk)) {
                    let payload = rng.next() as i64;
                    db.transaction(|tx| {
                        tx.update_many("items", vec![(row_id, vec![(4, Value::Int64(payload))])])?;
                        Ok(())
                    })
                    .unwrap();
                    row.payload = payload;
                }
            }
            3 => {
                if let (Some(row_id), Some(row)) = (lookup_pk(&db, pk), model.get_mut(&pk)) {
                    let bucket = (rng.next() % 8) as i64;
                    let tag = format!("tag-{}", rng.next() % 8).into_bytes();
                    db.transaction(|tx| {
                        tx.update_many(
                            "items",
                            vec![(
                                row_id,
                                vec![
                                    (2, Value::Int64(bucket)),
                                    (3, Value::Bytes(tag.clone())),
                                ],
                            )],
                        )?;
                        Ok(())
                    })
                    .unwrap();
                    row.bucket = bucket;
                    row.tag = tag;
                }
            }
            4 => {
                if let Some(row_id) = lookup_pk(&db, pk) {
                    db.transaction(|tx| {
                        tx.delete("items", row_id)?;
                        Ok(())
                    })
                    .unwrap();
                    model.remove(&pk);
                }
            }
            _ => {
                let row = ExpectedRow {
                    bucket: (rng.next() % 8) as i64,
                    tag: format!("tag-{}", rng.next() % 8).into_bytes(),
                    payload: rng.next() as i64,
                };
                if let Some(row_id) = lookup_pk(&db, pk) {
                    db.transaction(|tx| {
                        tx.delete("items", row_id)?;
                        tx.put("items", full_cells(pk, &row))?;
                        Ok(())
                    })
                    .unwrap();
                } else {
                    db.transaction(|tx| {
                        tx.put("items", full_cells(pk, &row))?;
                        Ok(())
                    })
                    .unwrap();
                }
                model.insert(pk, row);
            }
        }

        if step % 25 == 24 {
            assert_model(&db, &model);
        }
        if step % 300 == 299 {
            db.table("items").unwrap().lock().flush().unwrap();
            assert_model(&db, &model);
        }
        if step % 400 == 399 {
            db.rebuild_indexes("items").unwrap();
            assert_model(&db, &model);
        }
    }

    assert_model(&db, &model);
}
