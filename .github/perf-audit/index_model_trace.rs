use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Database, Value};
use std::collections::{BTreeMap, VecDeque};
use tempfile::tempdir;

#[derive(Clone)]
struct ExpectedRow {
    bucket: i64,
    tag: Vec<u8>,
    payload: i64,
}

fn schema() -> Schema {
    Schema {
        schema_id: 992,
        columns: vec![
            ColumnDef { id: 1, name: "id".into(), ty: TypeId::Int64, flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY), default_value: None, embedding_source: None },
            ColumnDef { id: 2, name: "bucket".into(), ty: TypeId::Int64, flags: ColumnFlags::empty(), default_value: None, embedding_source: None },
            ColumnDef { id: 3, name: "tag".into(), ty: TypeId::Bytes, flags: ColumnFlags::empty(), default_value: None, embedding_source: None },
            ColumnDef { id: 4, name: "payload".into(), ty: TypeId::Int64, flags: ColumnFlags::empty(), default_value: None, embedding_source: None },
        ],
        indexes: vec![
            IndexDef { name: "bucket_bitmap".into(), column_id: 2, kind: IndexKind::Bitmap, predicate: None, options: Default::default() },
            IndexDef { name: "tag_bitmap".into(), column_id: 3, kind: IndexKind::Bitmap, predicate: None, options: Default::default() },
        ],
        ..Schema::default()
    }
}

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 11
    }
}

fn cells(pk: i64, row: &ExpectedRow) -> Vec<(u16, Value)> {
    vec![
        (1, Value::Int64(pk)),
        (2, Value::Int64(row.bucket)),
        (3, Value::Bytes(row.tag.clone())),
        (4, Value::Int64(row.payload)),
    ]
}

fn lookup(db: &Database, pk: i64) -> Option<mongreldb_core::RowId> {
    db.table("items")
        .unwrap()
        .lock()
        .lookup_pk(&Value::Int64(pk).encode_key())
}

fn check(step: u64, db: &Database, model: &BTreeMap<i64, ExpectedRow>, history: &VecDeque<String>) {
    for pk in 0..64_i64 {
        let actual = lookup(db, pk);
        let expected = model.contains_key(&pk);
        if actual.is_some() != expected {
            panic!(
                "first HOT mismatch after step {step}, pk={pk}, actual={actual:?}, expected_present={expected}\n{}",
                history.iter().cloned().collect::<Vec<_>>().join("\n")
            );
        }
    }
}

fn remember(history: &mut VecDeque<String>, value: String) {
    if history.len() == 80 {
        history.pop_front();
    }
    history.push_back(value);
}

#[test]
fn trace_first_stale_hot_transition() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    let mut model = BTreeMap::<i64, ExpectedRow>::new();
    let mut rng = Lcg(0xD1B54A32D192ED03);
    let mut history = VecDeque::new();

    for step in 0..1_200_u64 {
        let op = rng.next() % 6;
        let pk = (rng.next() % 64) as i64;
        let before = lookup(&db, pk);
        match op {
            0 | 1 => {
                let row = ExpectedRow { bucket: (rng.next() % 8) as i64, tag: format!("tag-{}", rng.next() % 8).into_bytes(), payload: rng.next() as i64 };
                db.transaction(|tx| { tx.put("items", cells(pk, &row))?; Ok(()) }).unwrap();
                model.insert(pk, row);
                remember(&mut history, format!("{step}: put pk={pk} before={before:?} after={:?}", lookup(&db, pk)));
            }
            2 => {
                if let (Some(row_id), Some(row)) = (before, model.get_mut(&pk)) {
                    let payload = rng.next() as i64;
                    db.transaction(|tx| { tx.update_many("items", vec![(row_id, vec![(4, Value::Int64(payload))])])?; Ok(()) }).unwrap();
                    row.payload = payload;
                    remember(&mut history, format!("{step}: payload-update pk={pk} old={row_id:?} after={:?}", lookup(&db, pk)));
                } else {
                    remember(&mut history, format!("{step}: payload-skip pk={pk} before={before:?} model={}", model.contains_key(&pk)));
                }
            }
            3 => {
                if let (Some(row_id), Some(row)) = (before, model.get_mut(&pk)) {
                    let bucket = (rng.next() % 8) as i64;
                    let tag = format!("tag-{}", rng.next() % 8).into_bytes();
                    db.transaction(|tx| { tx.update_many("items", vec![(row_id, vec![(2, Value::Int64(bucket)), (3, Value::Bytes(tag.clone()))])])?; Ok(()) }).unwrap();
                    row.bucket = bucket;
                    row.tag = tag;
                    remember(&mut history, format!("{step}: indexed-update pk={pk} old={row_id:?} after={:?}", lookup(&db, pk)));
                } else {
                    remember(&mut history, format!("{step}: indexed-skip pk={pk} before={before:?} model={}", model.contains_key(&pk)));
                }
            }
            4 => {
                if let Some(row_id) = before {
                    db.transaction(|tx| { tx.delete("items", row_id)?; Ok(()) }).unwrap();
                    model.remove(&pk);
                    remember(&mut history, format!("{step}: delete pk={pk} row={row_id:?} after={:?}", lookup(&db, pk)));
                } else {
                    remember(&mut history, format!("{step}: delete-skip pk={pk} model={}", model.contains_key(&pk)));
                }
            }
            _ => {
                let row = ExpectedRow { bucket: (rng.next() % 8) as i64, tag: format!("tag-{}", rng.next() % 8).into_bytes(), payload: rng.next() as i64 };
                if let Some(row_id) = before {
                    db.transaction(|tx| { tx.delete("items", row_id)?; tx.put("items", cells(pk, &row))?; Ok(()) }).unwrap();
                } else {
                    db.transaction(|tx| { tx.put("items", cells(pk, &row))?; Ok(()) }).unwrap();
                }
                model.insert(pk, row);
                remember(&mut history, format!("{step}: delete-put pk={pk} before={before:?} after={:?}", lookup(&db, pk)));
            }
        }
        check(step, &db, &model, &history);
        if step % 300 == 299 {
            db.table("items").unwrap().lock().flush().unwrap();
            remember(&mut history, format!("{step}: flush"));
            check(step, &db, &model, &history);
        }
        if step % 400 == 399 {
            db.rebuild_indexes("items").unwrap();
            remember(&mut history, format!("{step}: rebuild"));
            check(step, &db, &model, &history);
        }
    }
}
